package integration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

type eksConfigMap struct {
	Data map[string]string `yaml:"data"`
}

func TestEKSDeployment(t *testing.T) {
	root := filepath.Join("..", "..", "deploy", "kubernetes", "eks-ec2-ha")
	read := func(name string) []byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return data
	}
	manifest := func(name string) map[string]any {
		t.Helper()
		var value map[string]any
		if err := yaml.Unmarshal(read(name), &value); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		return value
	}
	at := func(value any, keys ...string) any {
		t.Helper()
		for _, key := range keys {
			switch object := value.(type) {
			case map[string]any:
				var ok bool
				value, ok = object[key]
				if !ok {
					t.Fatalf("missing manifest field %q", key)
				}
			case []any:
				index, err := strconv.Atoi(key)
				if err != nil || index < 0 || index >= len(object) {
					t.Fatalf("invalid manifest array index %q for %T", key, value)
				}
				value = object[index]
			default:
				t.Fatalf("%s parent has type %T", key, value)
			}
		}
		return value
	}
	wantString := func(value any, want string) {
		t.Helper()
		if got, ok := value.(string); !ok || got != want {
			t.Fatalf("value = %v (%T), want %q", value, value, want)
		}
	}
	wantInt := func(value any, want int) {
		t.Helper()
		if got, ok := value.(int); ok && got == want {
			return
		}
		if got, ok := value.(uint64); ok && got == uint64(want) {
			return
		}
		if got, ok := value.(float64); ok && got == float64(want) {
			return
		}
		t.Fatalf("value = %v (%T), want %d", value, value, want)
	}

	namespace := manifest("namespace.yaml")
	wantString(at(namespace, "kind"), "Namespace")
	wantString(at(namespace, "metadata", "name"), "narumq")

	var configs eksConfigMap
	if err := yaml.Unmarshal(read("configmap-cluster.yaml"), &configs); err != nil {
		t.Fatalf("decode configmap-cluster.yaml: %v", err)
	}
	for index := range 3 {
		name := "node-" + strconv.Itoa(index+1) + ".json"
		var config map[string]any
		if err := json.Unmarshal([]byte(configs.Data[name]), &config); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		if got := at(config, "clusterId"); got != "narumq-cluster-1" {
			t.Fatalf("%s clusterId = %v", name, got)
		}
		wantInt(at(config, "nodeId"), index+1)
		wantString(at(config, "directory"), "/data")
		wantString(at(config, "listenAddress"), "0.0.0.0:7000")
		peer := "narumq-" + strconv.Itoa(index) + ".narumq-headless.narumq.svc.cluster.local"
		wantString(at(config, "advertisedAddress"), peer+":7000")
		wantString(at(config, "localDnsName"), peer)
		wantString(at(config, "peerTLS", "certFile"), "/peer-tls/node.crt")
		wantString(at(config, "peerTLS", "keyFile"), "/peer-tls/node.key")
		wantString(at(config, "peerTLS", "caFile"), "/peer-tls/ca.crt")
		wantString(at(config, "management", "listenAddress"), "0.0.0.0:8080")
		wantString(at(config, "management", "clusterAdminTokenFile"), "/secrets/admin.token")
		apiKeys, ok := at(config, "management", "apiKeys").([]any)
		if !ok || len(apiKeys) != 1 {
			t.Fatalf("%s API keys = %v", name, at(config, "management", "apiKeys"))
		}
		wantString(at(apiKeys[0], "id"), "dashboard")
		wantString(at(apiKeys[0], "keyFile"), "/secrets/dashboard.key")
		roles, ok := at(apiKeys[0], "roles").([]any)
		if !ok || len(roles) != 1 {
			t.Fatalf("%s dashboard roles = %v", name, at(apiKeys[0], "roles"))
		}
		wantString(roles[0], "ReadOnly")
		wantString(at(config, "mqtt", "tcpListenAddress"), "0.0.0.0:1883")
		wantString(at(config, "mqtt", "tlsListenAddress"), "")
		wantString(at(config, "mqtt", "webSocket", "listenAddress"), "0.0.0.0:8083")
		wantString(at(config, "mqtt", "webSocket", "tlsListenAddress"), "")
		wantString(at(config, "mqtt", "securityConfigFile"), "/secrets/security.json")
		members, ok := at(config, "bootstrapMembers").([]any)
		if !ok || len(members) != 3 {
			t.Fatalf("%s bootstrap members = %v", name, at(config, "bootstrapMembers"))
		}
		wantString(at(members[index], "address"), peer+":7000")
	}

	pdb := manifest("pdb.yaml")
	wantString(at(pdb, "kind"), "PodDisruptionBudget")
	wantInt(at(pdb, "spec", "maxUnavailable"), 1)

	headless := manifest("service-headless.yaml")
	wantString(at(headless, "metadata", "name"), "narumq-headless")
	wantString(at(headless, "spec", "clusterIP"), "None")
	wantInt(at(headless, "spec", "ports", "0", "port"), 7000)

	nlb := manifest("service-nlb.yaml")
	wantString(at(nlb, "spec", "type"), "LoadBalancer")
	wantString(at(nlb, "metadata", "annotations", "service.beta.kubernetes.io/aws-load-balancer-type"), "external")
	wantString(at(nlb, "metadata", "annotations", "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type"), "ip")
	wantString(at(nlb, "metadata", "annotations", "service.beta.kubernetes.io/aws-load-balancer-scheme"), "internal")
	ports, ok := at(nlb, "spec", "ports").([]any)
	if !ok || len(ports) != 1 {
		t.Fatalf("NLB ports = %v", at(nlb, "spec", "ports"))
	}
	wantInt(at(ports[0], "port"), 1883)
	for _, port := range ports {
		if at(port, "port") == 8080 {
			t.Fatal("plaintext Management HTTP 8080 must not be exposed through the NLB")
		}
	}
	management := manifest("service-management.yaml")
	wantString(at(management, "metadata", "name"), "narumq-management")
	wantString(at(management, "spec", "type"), "ClusterIP")
	wantInt(at(management, "spec", "ports", "0", "port"), 8080)

	statefulSet := manifest("statefulset.yaml")
	wantString(at(statefulSet, "kind"), "StatefulSet")
	wantInt(at(statefulSet, "spec", "replicas"), 3)
	wantString(at(statefulSet, "spec", "serviceName"), "narumq-headless")
	wantInt(at(statefulSet, "spec", "template", "spec", "terminationGracePeriodSeconds"), 660)
	wantString(at(statefulSet, "spec", "template", "spec", "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "0", "topologyKey"), "kubernetes.io/hostname")
	wantInt(at(statefulSet, "spec", "template", "spec", "topologySpreadConstraints", "0", "maxSkew"), 1)
	wantString(at(statefulSet, "spec", "template", "spec", "topologySpreadConstraints", "0", "topologyKey"), "topology.kubernetes.io/zone")
	wantString(at(statefulSet, "spec", "template", "spec", "topologySpreadConstraints", "0", "whenUnsatisfiable"), "DoNotSchedule")
	container := at(statefulSet, "spec", "template", "spec", "containers", "0")
	wantString(at(container, "command", "2"), "NODE_INDEX=\"$(hostname | awk -F'-' '{print $NF}')\"\nexec /narumq -cluster-config \"/configs/node-$((NODE_INDEX + 1)).json\"\n")
	wantString(at(container, "lifecycle", "preStop", "exec", "command", "2"), "NODE_INDEX=\"$(hostname | awk -F'-' '{print $NF}')\"\nNODE_ID=\"$((NODE_INDEX + 1))\"\nIDEMPOTENCY_KEY=\"prestop-${NODE_ID}-$(cat /proc/sys/kernel/random/uuid)\"\ncurl --fail --silent --show-error --max-time 600 \\\n  -X POST \"http://127.0.0.1:8080/api/v1/nodes/${NODE_ID}/drain\" \\\n  -H \"Authorization: Bearer $(cat /secrets/admin.token)\" \\\n  -H \"Idempotency-Key: ${IDEMPOTENCY_KEY}\" \\\n  -H \"Content-Type: application/json\" \\\n  --data '{}'\n")
	resources := at(container, "resources")
	wantString(at(resources, "requests", "cpu"), "1")
	wantString(at(resources, "requests", "memory"), "4Gi")
	wantString(at(resources, "limits", "cpu"), "1")
	wantString(at(resources, "limits", "memory"), "4Gi")
	for _, path := range []string{"/health/startup", "/health/live", "/health/ready"} {
		found := false
		for _, key := range []string{"startupProbe", "livenessProbe", "readinessProbe"} {
			probe := at(container, key)
			if at(probe, "httpGet", "path") == path && at(probe, "httpGet", "port") == "http-mgmt" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing management probe for %s", path)
		}
	}
	wantString(at(statefulSet, "spec", "volumeClaimTemplates", "0", "spec", "storageClassName"), "gp3")
	wantString(at(statefulSet, "spec", "volumeClaimTemplates", "0", "spec", "resources", "requests", "storage"), "20Gi")
	wantString(at(statefulSet, "spec", "template", "spec", "volumes", "1", "secret", "secretName"), "narumq-cluster-secrets")

	kustomization := manifest("kustomization.yaml")
	kustomResources, ok := at(kustomization, "resources").([]any)
	if !ok || len(kustomResources) != 7 {
		t.Fatalf("Kustomize resources = %v", at(kustomization, "resources"))
	}
	for _, resource := range kustomResources {
		if strings.Contains(resource.(string), "secret.yaml") {
			t.Fatalf("live Secret must not be tracked by Kustomize: %v", kustomResources)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "secret.yaml")); !os.IsNotExist(err) {
		t.Fatalf("secret.yaml must not exist in the package: err=%v", err)
	}
	ignore, err := os.ReadFile(filepath.Join("..", "..", ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []string{".tmp-eks/", "**/*secret.yaml", "!**/*secret.yaml.example", "!**/*secret.template.yaml"} {
		if !strings.Contains(string(ignore), rule) {
			t.Errorf(".gitignore is missing secret exclusion %q", rule)
		}
	}
}
