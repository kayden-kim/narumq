package integration

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"narumq/internal/application/membership"
)

// TestRolloutControllerKindLifecycle is an opt-in producer/consumer acceptance test.
// It requires a disposable Kind cluster named rollout-smoke, a registry endpoint
// reachable from Kind and its host-published HTTP URL, and two broker manifest digests.
// Traceability: SEC-11; docs/contracts/rollout-controller-v1.md §5-§8; docs/specs/drain-and-rolling-update.md §7, §9-§10.
func TestRolloutControllerKindLifecycle(t *testing.T) {
	if os.Getenv("NARUMQ_ROLLOUT_KIND_SMOKE") != "1" {
		t.Skip("set NARUMQ_ROLLOUT_KIND_SMOKE=1 for the real Kubernetes/broker rollout smoke")
	}
	registry := requiredSmokeEnv(t, "NARUMQ_ROLLOUT_REGISTRY")
	hostRegistryURL := requiredSmokeEnv(t, "NARUMQ_ROLLOUT_HOST_REGISTRY")
	oldDigest := requiredSmokeEnv(t, "NARUMQ_ROLLOUT_OLD_DIGEST")
	targetDigest := requiredSmokeEnv(t, "NARUMQ_ROLLOUT_TARGET_DIGEST")
	controllerImage := requiredSmokeEnv(t, "NARUMQ_ROLLOUT_CONTROLLER_IMAGE")
	registryURL := "http://" + registry
	oldImage, targetImage := registry+"/narumq@"+oldDigest, registry+"/narumq@"+targetDigest
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	runKubectl := func(args ...string) ([]byte, error) {
		t.Helper()
		full := append([]string{"--context=kind-rollout-smoke"}, args...)
		command := exec.CommandContext(ctx, "kubectl", full...)
		return command.CombinedOutput()
	}
	kubectl := func(args ...string) []byte {
		t.Helper()
		output, err := runKubectl(args...)
		if err != nil {
			full := append([]string{"--context=kind-rollout-smoke"}, args...)
			t.Fatalf("kubectl %s: %v\n%s", strings.Join(full, " "), err, output)
		}
		return output
	}
	apply := func(value any) {
		t.Helper()
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(ctx, "kubectl", "--context=kind-rollout-smoke", "apply", "-f", "-")
		command.Stdin = strings.NewReader(string(encoded))
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("kubectl apply: %v\n%s", err, output)
		}
	}
	if output := kubectl("get", "namespace", "narumq", "--ignore-not-found", "-o", "name"); len(output) != 0 {
		t.Fatalf("refusing to reuse a non-disposable narumq namespace: %s", output)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cleanupCancel()
		command := exec.CommandContext(cleanupCtx, "kubectl", "--context=kind-rollout-smoke", "get", "customresourcedefinition", "narumqrollouts.narumq.io", "--ignore-not-found", "-o", "name")
		if output, err := command.CombinedOutput(); err != nil {
			t.Errorf("check disposable rollout CRD before cleanup: %v: %s", err, output)
		} else if strings.TrimSpace(string(output)) != "" {
			command = exec.CommandContext(cleanupCtx, "kubectl", "--context=kind-rollout-smoke", "delete", "narumqrollouts.narumq.io", "--all", "-n", "narumq", "--wait=true", "--timeout=90s")
			if output, err := command.CombinedOutput(); err != nil {
				t.Errorf("delete disposable rollout custom resources: %v: %s", err, output)
			}
		}
		command = exec.CommandContext(cleanupCtx, "kubectl", "--context=kind-rollout-smoke", "delete", "namespace", "narumq", "--wait=true", "--timeout=80s")
		if output, err := command.CombinedOutput(); err != nil {
			t.Errorf("delete disposable smoke namespace: %v: %s", err, output)
		}
		for _, args := range [][]string{
			{"delete", "clusterrolebinding", "narumq-rollout-controller-nodes", "--ignore-not-found"},
			{"delete", "clusterrole", "narumq-rollout-controller-nodes", "--ignore-not-found"},
			{"delete", "customresourcedefinition", "narumqrollouts.narumq.io", "--ignore-not-found"},
		} {
			command := exec.CommandContext(cleanupCtx, "kubectl", append([]string{"--context=kind-rollout-smoke"}, args...)...)
			if output, err := command.CombinedOutput(); err != nil {
				t.Errorf("clean up cluster-scoped smoke resource: %v: %s", err, output)
			}
		}
	})
	apply(map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "narumq"}})
	ca := newClusterMembershipProcessCA(t)
	certs := make([]clusterMembershipProcessCertificate, 3)
	members := make([]clusterMembershipProcessMember, 3)
	for index := range certs {
		dns := fmt.Sprintf("narumq-%d.narumq-headless.narumq.svc.cluster.local", index)
		certs[index] = newClusterMembershipProcessCertificate(t, ca, dns)
		members[index] = clusterMembershipProcessMember{MemberID: uint64(index + 1), Address: dns + ":7000", DNSSAN: dns, CertificateSHA256: hex.EncodeToString(certs[index].fingerprint[:])}
	}
	operatorKey := "kind-rollout-operator-key"
	adminToken := "kind-rollout-admin-token"
	configs := map[string]string{}
	for index, certificate := range certs {
		dns := certificate.dnsName
		config := map[string]any{
			"mode": "cluster", "clusterId": "rollout-kind", "nodeId": index + 1, "directory": "/data",
			"listenAddress": "0.0.0.0:7000", "advertisedAddress": dns + ":7000", "localDnsName": dns,
			"clusterProtocolMajor": 2, "clusterProtocolMinor": 0, "stateSchemaMajor": 1, "stateSchemaMinor": 0,
			"role": "bootstrap", "bootstrapMembers": members, "seedAddresses": []string{},
			"peerTLS":    map[string]string{"certFile": "/peer-tls/node.crt", "keyFile": "/peer-tls/node.key", "caFile": "/peer-tls/ca.crt"},
			"management": map[string]any{"listenAddress": "0.0.0.0:8080", "clusterAdminTokenFile": "/secrets/admin.token", "apiKeys": []any{map[string]any{"id": "rollout", "keyFile": "/secrets/operator.key", "roles": []string{"Operator"}}}, "auditDirectory": "/tmp/narumq-management-audit"},
			"mqtt":       map[string]any{"tcpListenAddress": "0.0.0.0:1883", "tlsListenAddress": "", "securityConfigFile": "/secrets/security.json"},
		}
		encoded, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		configs[fmt.Sprintf("node-%d.json", index+1)] = string(encoded)
	}
	apply(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "narumq-cluster-configs", "namespace": "narumq"}, "data": configs})
	peerData := map[string][]byte{"ca.crt": ca.pem}
	for index, certificate := range certs {
		peerData[fmt.Sprintf("node-%d.crt", index+1)] = certificate.certPEM
		peerData[fmt.Sprintf("node-%d.key", index+1)] = certificate.keyPEM
	}
	apply(map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "narumq-peer-tls", "namespace": "narumq"}, "type": "Opaque", "data": peerData})
	security := `{"jwt":{"issuer":"https://issuer.example","audience":"narumq","algorithms":["RS256"],"jwksUrl":"http://127.0.0.1:9/jwks","cacheTtlMillis":300000,"refreshCooldownMillis":30000,"clockSkewMillis":30000,"httpTimeoutMillis":5000},"identities":[{"issuer":"https://issuer.example","subject":"kind-rollout-client","tenantId":"kind-rollout","credentialId":"kind-rollout-client","allowedClientIds":["kind-rollout-client"],"roles":["client"]}],"acl":[]}`
	apply(map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "narumq-management-secrets", "namespace": "narumq"}, "type": "Opaque", "data": map[string][]byte{"admin.token": []byte(adminToken), "operator.key": []byte(operatorKey), "token": []byte(operatorKey), "security.json": []byte(security)}})
	dockerConfig, err := json.Marshal(map[string]any{"auths": map[string]any{registry: map[string]string{"username": "", "password": ""}}})
	if err != nil {
		t.Fatal(err)
	}
	apply(map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "narumq-registry", "namespace": "narumq"}, "type": "kubernetes.io/dockerconfigjson", "data": map[string][]byte{".dockerconfigjson": dockerConfig}})
	labels := map[string]string{"app": "narumq"}
	apply(map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "narumq-headless", "namespace": "narumq"}, "spec": map[string]any{"clusterIP": "None", "publishNotReadyAddresses": true, "selector": labels, "ports": []any{map[string]any{"name": "raft-peer", "port": 7000, "targetPort": 7000}}}})
	apply(map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "narumq-management", "namespace": "narumq"}, "spec": map[string]any{"selector": labels, "ports": []any{map[string]any{"name": "http-mgmt", "port": 8080, "targetPort": "http-mgmt"}}}})
	initContainer := map[string]any{
		"name": "prepare", "image": oldImage,
		"command":         []string{"/bin/sh", "-c", "mkdir -p /data /peer-tls && chown -R 10001:10001 /data && i=${HOSTNAME##*-}; i=$((i+1)); cp /tls/node-${i}.crt /peer-tls/node.crt; cp /tls/node-${i}.key /peer-tls/node.key; cp /tls/ca.crt /peer-tls/ca.crt; chown -R 10001:10001 /peer-tls"},
		"securityContext": map[string]any{"runAsUser": 0},
		"volumeMounts": []any{
			map[string]any{"name": "data", "mountPath": "/data"},
			map[string]any{"name": "peer-tls", "mountPath": "/peer-tls"},
			map[string]any{"name": "tls-source", "mountPath": "/tls", "readOnly": true},
		},
	}
	brokerContainer := map[string]any{
		"name": "narumq", "image": oldImage, "imagePullPolicy": "IfNotPresent",
		"command": []string{"/bin/sh", "-c", "i=${HOSTNAME##*-}; i=$((i+1)); exec /narumq -cluster-config /configs/node-${i}.json"},
		"ports": []any{
			map[string]any{"name": "raft-peer", "containerPort": 7000},
			map[string]any{"name": "http-mgmt", "containerPort": 8080},
			map[string]any{"name": "mqtt-tcp", "containerPort": 1883},
		},
		"readinessProbe": map[string]any{
			"httpGet":       map[string]any{"path": "/health/ready", "port": "http-mgmt"},
			"periodSeconds": 3, "timeoutSeconds": 2, "failureThreshold": 10,
		},
		"volumeMounts": []any{
			map[string]any{"name": "configs", "mountPath": "/configs", "readOnly": true},
			map[string]any{"name": "secrets", "mountPath": "/secrets", "readOnly": true},
			map[string]any{"name": "peer-tls", "mountPath": "/peer-tls", "readOnly": true},
			map[string]any{"name": "data", "mountPath": "/data"},
		},
	}
	podSpec := map[string]any{
		"terminationGracePeriodSeconds": 660,
		"imagePullSecrets":              []any{map[string]any{"name": "narumq-registry"}},
		"initContainers":                []any{initContainer},
		"containers":                    []any{brokerContainer},
		"volumes": []any{
			map[string]any{"name": "configs", "configMap": map[string]any{"name": "narumq-cluster-configs"}},
			map[string]any{"name": "secrets", "secret": map[string]any{"secretName": "narumq-management-secrets"}},
			map[string]any{"name": "tls-source", "secret": map[string]any{"secretName": "narumq-peer-tls"}},
			map[string]any{"name": "peer-tls", "emptyDir": map[string]any{}},
		},
	}
	statefulSet := map[string]any{
		"apiVersion": "apps/v1", "kind": "StatefulSet",
		"metadata": map[string]any{"name": "narumq", "namespace": "narumq"},
		"spec": map[string]any{
			"serviceName": "narumq-headless", "replicas": 3, "podManagementPolicy": "Parallel",
			"updateStrategy": map[string]any{"type": "OnDelete"},
			"selector":       map[string]any{"matchLabels": labels},
			"template":       map[string]any{"metadata": map[string]any{"labels": labels}, "spec": podSpec},
			"volumeClaimTemplates": []any{map[string]any{
				"metadata": map[string]any{"name": "data"},
				"spec":     map[string]any{"accessModes": []string{"ReadWriteOnce"}, "resources": map[string]any{"requests": map[string]any{"storage": "1Gi"}}},
			}},
		},
	}
	apply(statefulSet)
	if output, waitErr := runKubectl("wait", "--for=condition=Ready", "pod", "-l", "app=narumq", "-n", "narumq", "--timeout=90s"); waitErr != nil {
		pods, _ := runKubectl("describe", "pods", "-n", "narumq")
		events, _ := runKubectl("get", "events", "-n", "narumq", "--sort-by=.lastTimestamp")
		logs, _ := runKubectl("logs", "narumq-0", "-n", "narumq", "-c", "narumq", "--tail=100")
		t.Fatalf("three real broker Pods did not become Ready: %v\nwait: %s\npods: %s\nevents: %s\nlogs: %s", waitErr, output, pods, events, logs)
	}
	kubectl("set", "image", "statefulset/narumq", "narumq="+targetImage, "-n", "narumq")
	kubectl("apply", "-f", "../../deploy/kubernetes/rollout-controller/crd.yaml")
	kubectl("apply", "-f", "../../deploy/kubernetes/rollout-controller/rbac.yaml")
	canGet := kubectl("auth", "can-i", "get", "nodes", "--as=system:serviceaccount:narumq:narumq-rollout-controller")
	if !strings.Contains(strings.ToLower(string(canGet)), "yes") {
		t.Fatalf("controller service account cannot GET Nodes: %s", canGet)
	}
	apply(map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "narumq-rollout-audit", "namespace": "narumq"},
		"spec":     map[string]any{"accessModes": []string{"ReadWriteOnce"}, "resources": map[string]any{"requests": map[string]any{"storage": "1Gi"}}},
	})
	controller := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "narumq-rollout-controller", "namespace": "narumq"},
		"spec": map[string]any{
			"replicas": 1, "strategy": map[string]any{"type": "Recreate"},
			"selector": map[string]any{"matchLabels": map[string]string{"app": "narumq-rollout-controller"}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]string{"app": "narumq-rollout-controller"}},
				"spec": map[string]any{
					"serviceAccountName": "narumq-rollout-controller",
					"securityContext":    map[string]any{"runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 10001},
					"containers": []any{map[string]any{
						"name": "controller", "image": controllerImage, "imagePullPolicy": "IfNotPresent",
						"env": []any{
							map[string]any{"name": "POD_NAMESPACE", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.namespace"}}},
							map[string]any{"name": "POD_UID", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.uid"}}},
							map[string]any{"name": "NARUMQ_AUDIT_DIRECTORY", "value": "/var/lib/narumq/audit"},
							map[string]any{"name": "NARUMQ_ALLOWED_OCI_REGISTRIES", "value": registryURL},
						},
						"ports":          []any{map[string]any{"name": "health", "containerPort": 8081}},
						"readinessProbe": map[string]any{"httpGet": map[string]any{"path": "/readyz", "port": "health"}, "periodSeconds": 3},
						"volumeMounts":   []any{map[string]any{"name": "audit", "mountPath": "/var/lib/narumq/audit"}},
					}},
					"volumes": []any{map[string]any{"name": "audit", "persistentVolumeClaim": map[string]any{"claimName": "narumq-rollout-audit"}}},
				},
			},
		},
	}
	apply(controller)
	kubectl("rollout", "status", "deployment/narumq-rollout-controller", "-n", "narumq", "--timeout=3m")

	// Query a real broker API over the in-cluster Service and prove three committed members.
	apiOutput := kubectl("exec", "narumq-0", "-n", "narumq", "-c", "narumq", "--", "/bin/sh", "-c",
		"curl -fsS -H 'X-API-Key: "+operatorKey+"' http://narumq-management:8080/api/v1/cluster/features")
	var featureEnvelope struct {
		Data struct {
			Members []json.RawMessage `json:"members"`
		} `json:"data"`
	}
	if err := json.Unmarshal(apiOutput, &featureEnvelope); err != nil || len(featureEnvelope.Data.Members) != 3 {
		t.Fatalf("broker API did not report three committed voters: err=%v body=%s", err, apiOutput)
	}
	targetConfigDigest := registryConfigDigest(t, hostRegistryURL, targetDigest)
	originalUIDs := smokePodUIDs(t, kubectl)
	// The CR target must equal the digest-bound OCI feature label, which is the canonical build feature set.
	features, err := membership.BuildFeatures("")
	if err != nil {
		t.Fatal(err)
	}
	rolloutResource := func(name, version string) map[string]any {
		return map[string]any{
			"apiVersion": "narumq.io/v1alpha1", "kind": "NaruMQRollout",
			"metadata": map[string]any{"name": name, "namespace": "narumq"},
			"spec": map[string]any{
				"workloadRef":        map[string]any{"name": "narumq"},
				"target":             map[string]any{"image": targetImage, "releaseVersion": version, "features": features},
				"imagePullSecretRef": map[string]any{"name": "narumq-registry"},
				"management":         map[string]any{"endpoint": "http://narumq-management.narumq.svc.cluster.local:8080", "credentialSecretRef": map[string]any{"name": "narumq-management-secrets"}},
				"retryToken":         "first-attempt",
			},
		}
	}
	apply(rolloutResource("rollout-smoke", "0.0.1"))
	operationID := waitForActiveDrain(t, ctx, kubectl, "rollout-smoke")
	controllerPod := strings.TrimSpace(string(kubectl("get", "pods", "-n", "narumq", "-l", "app=narumq-rollout-controller", "-o", "jsonpath={.items[0].metadata.name}")))
	controllerUIDBefore := strings.TrimSpace(string(kubectl("get", "pod", controllerPod, "-n", "narumq", "-o", "jsonpath={.metadata.uid}")))
	kubectl("delete", "pod", controllerPod, "-n", "narumq", "--wait=true", "--timeout=60s")
	kubectl("rollout", "status", "deployment/narumq-rollout-controller", "-n", "narumq", "--timeout=3m")
	controllerUIDAfter := strings.TrimSpace(string(kubectl("get", "pods", "-n", "narumq", "-l", "app=narumq-rollout-controller", "-o", "jsonpath={.items[0].metadata.uid}")))
	if controllerUIDAfter == controllerUIDBefore {
		t.Fatalf("controller Pod UID did not change during active drain restart: %s", controllerUIDAfter)
	}
	var drainEnvelope struct {
		Data struct {
			OperationID string `json:"operationId"`
		} `json:"data"`
	}
	drainOutput := kubectl("exec", "narumq-0", "-n", "narumq", "-c", "narumq", "--", "curl", "-fsS", "-H", "X-API-Key: "+operatorKey, "http://narumq-management:8080/api/v1/nodes/1/drain")
	if err := json.Unmarshal(drainOutput, &drainEnvelope); err != nil || drainEnvelope.Data.OperationID != operationID {
		t.Fatalf("controller restart lost or replaced active broker drain operation: want=%s err=%v body=%s", operationID, err, drainOutput)
	}
	waitForRolloutPhase(t, ctx, kubectl, "rollout-smoke", "Completed")
	updatedUIDs := smokePodUIDs(t, kubectl)
	for ordinal := range 3 {
		name := fmt.Sprintf("narumq-%d", ordinal)
		if originalUIDs[name] == updatedUIDs[name] {
			t.Fatalf("controller completed without replacing %s", name)
		}
		imageID := string(kubectl("get", "pod", name, "-n", "narumq", "-o", "jsonpath={.status.containerStatuses[?(@.name=='narumq')].imageID}"))
		if !strings.Contains(imageID, targetDigest) && !strings.Contains(imageID, targetConfigDigest) {
			t.Fatalf("replacement %s runtime imageID %q is neither target manifest %q nor its verified config %q", name, imageID, targetDigest, targetConfigDigest)
		}
	}
	// A caller-authored version that disagrees with the digest-bound OCI label must not mutate Pods.
	apply(rolloutResource("rollout-denied", "0.0.2"))
	waitForConditionReason(t, ctx, kubectl, "rollout-denied", "preflight_blocked")
	if afterDenied := smokePodUIDs(t, kubectl); !equalSmokeUIDs(updatedUIDs, afterDenied) {
		t.Fatalf("OCI/version preflight denial changed broker Pod identity: before=%v after=%v", updatedUIDs, afterDenied)
	}
	auditReader := map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "rollout-audit-reader", "namespace": "narumq"},
		"spec": map[string]any{
			"restartPolicy": "Never",
			"containers":    []any{map[string]any{"name": "reader", "image": oldImage, "command": []string{"/bin/sh", "-c", "cat /audit/audit.jsonl"}, "volumeMounts": []any{map[string]any{"name": "audit", "mountPath": "/audit", "readOnly": true}}}},
			"volumes":       []any{map[string]any{"name": "audit", "persistentVolumeClaim": map[string]any{"claimName": "narumq-rollout-audit"}}},
		},
	}
	apply(auditReader)
	kubectl("wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/rollout-audit-reader", "-n", "narumq", "--timeout=2m")
	audit := kubectl("logs", "rollout-audit-reader", "-n", "narumq")
	controllerUID := string(kubectl("get", "pods", "-n", "narumq", "-l", "app=narumq-rollout-controller", "-o", "jsonpath={.items[0].metadata.uid}"))
	foundApproval := false
	for _, line := range strings.Split(strings.TrimSpace(string(audit)), "\n") {
		var record struct {
			Event struct {
				Action   string `json:"action"`
				Producer struct {
					InstanceID string `json:"instanceId"`
				} `json:"producer"`
			} `json:"event"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if record.Event.Action == "rollout.pod.delete_approved" {
			foundApproval = true
			if record.Event.Producer.InstanceID != controllerUID {
				t.Fatalf("audit producer instance ID %q does not match controller Pod UID %q", record.Event.Producer.InstanceID, controllerUID)
			}
		}
	}
	if !foundApproval {
		t.Fatalf("durable audit PVC lacks a parseable approved delete event: %s", audit)
	}
	kubectl("delete", "narumqrollouts.narumq.io", "rollout-smoke", "-n", "narumq", "--wait=true", "--timeout=2m")
	kubectl("delete", "narumqrollouts.narumq.io", "rollout-denied", "-n", "narumq", "--wait=true", "--timeout=2m")

}

func smokePodUIDs(t *testing.T, kubectl func(...string) []byte) map[string]string {
	t.Helper()
	output := kubectl("get", "pods", "-n", "narumq", "-l", "app=narumq", "-o", "json")
	var listing struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
				UID  string `json:"uid"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(output, &listing); err != nil {
		t.Fatalf("decode broker Pod listing: %v: %s", err, output)
	}
	if len(listing.Items) != 3 {
		t.Fatalf("broker StatefulSet has %d Pods, want 3", len(listing.Items))
	}
	uids := make(map[string]string, 3)
	for _, item := range listing.Items {
		uids[item.Metadata.Name] = item.Metadata.UID
	}
	return uids
}

func registryConfigDigest(t *testing.T, registryURL, manifestDigest string) string {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, strings.TrimSuffix(registryURL, "/")+"/v2/narumq/manifests/"+manifestDigest, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept", "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("read local registry manifest: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("read local registry manifest: HTTP %s", response.Status)
	}
	var manifest struct {
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
	}
	if err := json.NewDecoder(response.Body).Decode(&manifest); err != nil || !strings.HasPrefix(manifest.Config.Digest, "sha256:") {
		t.Fatalf("decode config descriptor for %s: digest=%q err=%v", manifestDigest, manifest.Config.Digest, err)
	}
	return manifest.Config.Digest
}

func equalSmokeUIDs(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for name, uid := range left {
		if right[name] != uid {
			return false
		}
	}
	return true
}

func waitForActiveDrain(t *testing.T, ctx context.Context, kubectl func(...string) []byte, name string) string {
	t.Helper()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	timeout := time.NewTimer(2 * time.Minute)
	defer timeout.Stop()
	for {
		output := kubectl("get", "narumqrollout", name, "-n", "narumq", "-o", "json")
		var resource struct {
			Status struct {
				Phase           string `json:"phase"`
				DrainOperation  string `json:"drainOperationID"`
				DrainState      string `json:"drainState"`
				DrainSafeToStop *bool  `json:"drainSafeToStop"`
			} `json:"status"`
		}
		if err := json.Unmarshal(output, &resource); err != nil {
			t.Fatalf("decode active drain status: %v: %s", err, output)
		}
		if resource.Status.Phase == "Draining" && resource.Status.DrainOperation != "" &&
			resource.Status.DrainState != "SAFE_TO_STOP" && (resource.Status.DrainSafeToStop == nil || !*resource.Status.DrainSafeToStop) {
			return resource.Status.DrainOperation
		}
		if resource.Status.Phase == "Failed" || resource.Status.Phase == "Completed" {
			t.Fatalf("rollout left active drain before controller restart: %s", output)
		}
		select {
		case <-timeout.C:
			t.Fatalf("timed out waiting for a cancellable active drain before controller restart: %s", output)
		case <-ctx.Done():
			t.Fatalf("smoke context ended waiting for active drain: %s", output)
		case <-ticker.C:
		}
	}
}

func waitForRolloutPhase(t *testing.T, ctx context.Context, kubectl func(...string) []byte, name, phase string) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	timeout := time.NewTimer(5 * time.Minute)
	defer timeout.Stop()
	for {
		output := kubectl("get", "narumqrollout", name, "-n", "narumq", "-o", "json")
		var resource struct {
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		}
		if err := json.Unmarshal(output, &resource); err != nil {
			t.Fatalf("decode rollout %s status: %v: %s", name, err, output)
		}
		if resource.Status.Phase == phase {
			return
		}
		if resource.Status.Phase == "Failed" || resource.Status.Phase == "Aborted" {
			logs := kubectl("logs", "deployment/narumq-rollout-controller", "-n", "narumq", "--tail=100")
			t.Fatalf("rollout %s entered terminal phase %s: %s\ncontroller logs: %s", name, resource.Status.Phase, output, logs)
		}
		select {
		case <-timeout.C:
			logs := kubectl("logs", "deployment/narumq-rollout-controller", "-n", "narumq", "--tail=100")
			events := kubectl("get", "events", "-n", "narumq", "--sort-by=.lastTimestamp")
			t.Fatalf("timed out waiting for rollout %s phase %s; latest status: %s\ncontroller logs: %s\nevents: %s", name, phase, output, logs, events)
		case <-ctx.Done():
			t.Fatalf("smoke context ended waiting for rollout %s phase %s; latest status: %s", name, phase, output)
		case <-ticker.C:
		}
	}
}

func waitForConditionReason(t *testing.T, ctx context.Context, kubectl func(...string) []byte, name, reason string) {
	t.Helper()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		output := kubectl("get", "narumqrollout", name, "-n", "narumq", "-o", "json")
		var resource struct {
			Status struct {
				Conditions []struct {
					Reason string `json:"reason"`
				} `json:"conditions"`
			} `json:"status"`
		}
		if err := json.Unmarshal(output, &resource); err != nil {
			t.Fatalf("decode rollout %s conditions: %v: %s", name, err, output)
		}
		for _, condition := range resource.Status.Conditions {
			if condition.Reason == reason {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for rollout %s condition %s; latest status: %s", name, reason, output)
		case <-ticker.C:
		}
	}
}

func requiredSmokeEnv(t *testing.T, key string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		t.Fatalf("%s is required for the Kind rollout smoke", key)
	}
	return value
}
