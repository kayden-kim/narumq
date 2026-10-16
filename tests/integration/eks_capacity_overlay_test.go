package integration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

// TestEKSCapacityOverlay checks the capacity fixture's resource and routing boundaries.
// TLS, generated security, loopback JWKS and test-only process control may differ from
// EKS_EC2_HA; the production node configuration and resource limits stay unchanged.
func TestEKSCapacityOverlay(t *testing.T) {
	root := filepath.Join("..", "..", "deploy", "kubernetes")
	read := func(name string) []byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return data
	}
	decode := func(data []byte, name string) map[string]any {
		t.Helper()
		var value map[string]any
		if err := yaml.Unmarshal(data, &value); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		return value
	}
	at := func(value any, keys ...any) any {
		t.Helper()
		for _, key := range keys {
			switch typed := key.(type) {
			case string:
				object, ok := value.(map[string]any)
				if !ok {
					t.Fatalf("%v parent has type %T", key, value)
				}
				value, ok = object[typed]
				if !ok {
					t.Fatalf("missing manifest field %q", typed)
				}
			case int:
				list, ok := value.([]any)
				if !ok || typed >= len(list) {
					t.Fatalf("index %d is not available in %T", typed, value)
				}
				value = list[typed]
			}
		}
		return value
	}
	named := func(value any, name string) map[string]any {
		t.Helper()
		items, _ := value.([]any)
		for _, item := range items {
			entry, _ := item.(map[string]any)
			if entry["name"] == name {
				return entry
			}
		}
		t.Fatalf("manifest list has no entry named %q", name)
		return nil
	}
	assertGuaranteed := func(container map[string]any) {
		t.Helper()
		resources, _ := container["resources"].(map[string]any)
		if !reflect.DeepEqual(resources["requests"], resources["limits"]) || resources["limits"] == nil {
			t.Fatalf("container %v resources requests != limits: %v", container["name"], resources)
		}
	}

	overlay := decode(read("eks-ec2-ha-capacity/kustomization.yaml"), "kustomization")
	if got := at(overlay, "resources"); !reflect.DeepEqual(got, []any{"../eks-ec2-ha"}) {
		t.Fatalf("capacity overlay resources = %v, want only ../eks-ec2-ha (the Job template must not auto-start)", got)
	}
	if got := len(at(overlay, "patches").([]any)); got != 3 {
		t.Fatalf("capacity overlay patches = %d, want 3", got)
	}

	var baseConfigs, patchConfigs eksConfigMap
	if err := yaml.Unmarshal(read("eks-ec2-ha/configmap-cluster.yaml"), &baseConfigs); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(read("eks-ec2-ha-capacity/configmap-cluster-tls-patch.yaml"), &patchConfigs); err != nil {
		t.Fatal(err)
	}
	if len(patchConfigs.Data) != 3 || len(baseConfigs.Data) != 6 {
		t.Fatalf("config key counts base=%d patch=%d, want base 6 (3 node configs + 3 slots) and patch 3", len(baseConfigs.Data), len(patchConfigs.Data))
	}
	for name, baseJSON := range baseConfigs.Data {
		if strings.HasPrefix(name, "slot-") {
			if _, patched := patchConfigs.Data[name]; patched {
				t.Fatalf("capacity patch must not override %s", name)
			}
			continue
		}
		var want, got map[string]any
		if err := json.Unmarshal([]byte(baseJSON), &want); err != nil {
			t.Fatalf("decode base %s: %v", name, err)
		}
		patchedJSON, ok := patchConfigs.Data[name]
		if !ok {
			t.Fatalf("patch is missing %s", name)
		}
		if err := json.Unmarshal([]byte(patchedJSON), &got); err != nil {
			t.Fatalf("decode patched %s: %v", name, err)
		}
		if at(want, "mqtt", "tlsListenAddress") != "" {
			t.Fatalf("default %s must keep the MQTT TLS listener disabled", name)
		}
		mqtt := want["mqtt"].(map[string]any)
		mqtt["tlsListenAddress"] = "0.0.0.0:8883"
		mqtt["securityConfigFile"] = "/capacity-security/security.json"
		mqtt["tls"] = map[string]any{
			"certFile": "/mqtt-tls/tls.crt", "keyFile": "/mqtt-tls/tls.key",
			"caFile": "", "clientAuth": "none", "minVersion": "1.2",
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%s differs from the default node config beyond the MQTT TLS fields\nwant %v\ngot  %v", name, want, got)
		}
	}

	nlb := decode(read("eks-ec2-ha-capacity/service-nlb-tls-patch.yaml"), "nlb patch")
	port := at(nlb, "spec", "ports", 0)
	if at(port, "port") != 8883 || at(port, "protocol") != "TCP" || at(port, "targetPort") != "mqtt-tls" {
		t.Fatalf("NLB patch port = %v, want TCP 8883 -> mqtt-tls", port)
	}

	statefulSet := decode(read("eks-ec2-ha-capacity/statefulset-capacity-patch.yaml"), "statefulset patch")
	pod := at(statefulSet, "spec", "template", "spec")
	jwks := named(at(pod, "containers"), "capacity-jwks")
	assertGuaranteed(jwks)
	listen := false
	for _, arg := range at(jwks, "command").([]any) {
		if arg == "--listen=127.0.0.1:18080" {
			listen = true
		}
	}
	if !listen {
		t.Fatalf("JWKS sidecar must listen on Pod loopback only: %v", jwks["command"])
	}
	securityInit := named(at(pod, "initContainers"), "capacity-security-config")
	assertGuaranteed(securityInit)
	if at(named(at(pod, "containers"), "narumq"), "ports", 0, "containerPort") != 8883 {
		t.Fatal("broker container must expose MQTT TLS 8883")
	}
	capacityCommand, _ := at(named(at(pod, "containers"), "narumq"), "command").([]any)
	if len(capacityCommand) != 3 {
		t.Fatalf("capacity broker command = %v, want [/bin/sh, -c, script]", capacityCommand)
	}
	capacityScript, _ := capacityCommand[2].(string)
	t.Run("slot script", func(t *testing.T) {
		// The fake process wrapper receives the arguments the real wrapper would pass to the broker.
		assertEKSMainSlotBehaviour(t, capacityScript, "/capacity-process/broker-process.sh", func(configs, id string) []string {
			return []string{"run", "/narumq", "-cluster-config", configs + "/node-" + id + ".json"}
		})
	})

	jobTemplate := string(read("eks-ec2-ha-capacity/job-capacity-runner.template.yaml"))
	rendered := regexp.MustCompile(`\$\{[A-Z_]+\}`).ReplaceAllStringFunc(jobTemplate, func(match string) string {
		switch match {
		case "${ACTIVE_DEADLINE_SECONDS}":
			return "7200"
		case "${GENERATOR_CPU}":
			return "3"
		default:
			return "placeholder"
		}
	})
	job := decode([]byte(rendered), "job template")
	jobPod := at(job, "spec", "template", "spec")
	if at(jobPod, "nodeSelector", "narumq.io/generator") != "true" {
		t.Fatal("capacity Job must be pinned to the generator node group")
	}
	toleration := at(jobPod, "tolerations", 0)
	if at(toleration, "key") != "narumq.io/generator" || at(toleration, "effect") != "NoSchedule" {
		t.Fatalf("capacity Job toleration = %v", toleration)
	}
	assertGuaranteed(named(at(jobPod, "containers"), "capacity-runner"))
	for _, volume := range []string{"control", "output"} {
		if _, ok := named(at(jobPod, "volumes"), volume)["emptyDir"]; !ok {
			t.Fatalf("Job volume %s must be an emptyDir", volume)
		}
	}
	if at(named(at(jobPod, "initContainers"), "toolbox"), "restartPolicy") != "Always" {
		t.Fatal("toolbox must be a native sidecar so the Job can finish while artifacts are copied")
	}
}
