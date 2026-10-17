package integration

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
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

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-048, drain-and-rolling-update.md:DRN-SCN-028, drain-and-rolling-update.md:DRN-SCN-029
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
	manifestDocs := func(name string) []map[string]any {
		t.Helper()
		dec := yaml.NewDecoder(bytes.NewReader(read(name)))
		var docs []map[string]any
		for {
			var doc map[string]any
			if err := dec.Decode(&doc); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("decode %s: %v", name, err)
			}
			if len(doc) > 0 {
				docs = append(docs, doc)
			}
		}
		return docs
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
	findNamed := func(items any, field, name string) map[string]any {
		t.Helper()
		values, ok := items.([]any)
		if !ok {
			t.Fatalf("named manifest items = %T, want array", items)
		}
		for _, raw := range values {
			item, ok := raw.(map[string]any)
			if ok && item[field] == name {
				return item
			}
		}
		t.Fatalf("manifest item where %s is %q not found", field, name)
		return nil
	}
	assertNoStaticAWSCredentials := func(pod any) {
		t.Helper()
		podSpec, ok := pod.(map[string]any)
		if !ok {
			t.Fatalf("pod spec = %T, want mapping", pod)
		}
		for _, key := range []string{"initContainers", "containers"} {
			values, ok := podSpec[key].([]any)
			if !ok {
				continue
			}
			for _, raw := range values {
				container, ok := raw.(map[string]any)
				if !ok {
					t.Fatalf("%s container = %T, want mapping", key, raw)
				}
				env, _ := container["env"].([]any)
				for _, rawVariable := range env {
					variable, ok := rawVariable.(map[string]any)
					if !ok {
						t.Fatalf("container environment variable = %T, want mapping", rawVariable)
					}
					name, _ := variable["name"].(string)
					switch name {
					case "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN":
						t.Fatalf("pod must not contain static AWS credential variable %q", name)
					}
				}
			}
		}
	}
	hasMount := func(container map[string]any, volumeName, mountPath string) bool {
		t.Helper()
		mounts, _ := container["volumeMounts"].([]any)
		for _, raw := range mounts {
			mount, ok := raw.(map[string]any)
			if ok && mount["name"] == volumeName && mount["mountPath"] == mountPath {
				return true
			}
		}
		return false
	}

	namespace := manifest("namespace.yaml")
	wantString(at(namespace, "kind"), "Namespace")
	wantString(at(namespace, "metadata", "name"), "narumq")

	var configs eksConfigMap
	if err := yaml.Unmarshal(read("configmap-cluster.yaml"), &configs); err != nil {
		t.Fatalf("decode configmap-cluster.yaml: %v", err)
	}
	for index := range 3 {
		slotName := "slot-" + strconv.Itoa(index)
		slot, ok := configs.Data[slotName]
		if !ok || slot != strconv.Itoa(index+1) {
			t.Fatalf("%s = %q (present=%t), want %q", slotName, slot, ok, strconv.Itoa(index+1))
		}
		memberID, err := strconv.Atoi(slot)
		if err != nil {
			t.Fatalf("%s = %q is not a decimal member ID: %v", slotName, slot, err)
		}
		name := "node-" + slot + ".json"
		rawConfig, ok := configs.Data[name]
		if !ok {
			t.Fatalf("%s references %s, which the ConfigMap does not contain", slotName, name)
		}
		var config map[string]any
		if err := json.Unmarshal([]byte(rawConfig), &config); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		if got := at(config, "clusterId"); got != "narumq-cluster-1" {
			t.Fatalf("%s clusterId = %v", name, got)
		}
		wantInt(at(config, "nodeId"), memberID)
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
	preStop := at(container, "lifecycle", "preStop", "exec", "command")
	preStopCommands, ok := preStop.([]any)
	if !ok || len(preStopCommands) != 3 {
		t.Fatalf("preStop command = %v, want 3 elements [/bin/sh, -c, script]", preStop)
	}
	wantString(preStopCommands[0], "/bin/sh")
	wantString(preStopCommands[1], "-c")
	preStopScript, ok := preStopCommands[2].(string)
	if !ok || len(preStopScript) == 0 {
		t.Fatalf("preStop script = %v, want non-empty string", preStopCommands[2])
	}
	mainCommands, ok := at(container, "command").([]any)
	if !ok || len(mainCommands) != 3 {
		t.Fatalf("main command = %v, want 3 elements [/bin/sh, -c, script]", at(container, "command"))
	}
	wantString(mainCommands[0], "/bin/sh")
	wantString(mainCommands[1], "-c")
	mainScript, ok := mainCommands[2].(string)
	if !ok || len(mainScript) == 0 {
		t.Fatalf("main script = %v, want non-empty string", mainCommands[2])
	}
	// The scripts run in a sandbox that replaces only absolute paths and external programs.
	// A slot value other than ordinal+1 must change the executed config and the drained ID.
	t.Run("slot scripts", func(t *testing.T) {
		assertEKSMainSlotBehaviour(t, mainScript, "/narumq", func(configs, id string) []string {
			return []string{"-cluster-config", configs + "/node-" + id + ".json"}
		})
		assertEKSPreStopSlotBehaviour(t, preStopScript)
	})
	wantString(at(statefulSet, "spec", "volumeClaimTemplates", "0", "spec", "storageClassName"), "gp3")
	wantString(at(statefulSet, "spec", "volumeClaimTemplates", "0", "spec", "resources", "requests", "storage"), "20Gi")
	wantString(at(statefulSet, "spec", "template", "spec", "volumes", "1", "secret", "secretName"), "narumq-cluster-secrets")

	kustomization := manifest("kustomization.yaml")
	kustomResources, ok := at(kustomization, "resources").([]any)
	if !ok || len(kustomResources) != 8 {
		t.Fatalf("Kustomize resources = %v, want 8", at(kustomization, "resources"))
	}
	foundCronJobResource := false
	for _, resource := range kustomResources {
		if resource.(string) == "cronjob-backup.yaml" {
			foundCronJobResource = true
		}
	}
	if !foundCronJobResource {
		t.Fatalf("kustomization.yaml missing cronjob-backup.yaml: %v", kustomResources)
	}

	backupDocs := manifestDocs("cronjob-backup.yaml")
	var backupPVC, backupCronJob map[string]any
	for _, doc := range backupDocs {
		switch doc["kind"] {
		case "PersistentVolumeClaim":
			backupPVC = doc
		case "CronJob":
			backupCronJob = doc
		}
	}
	if backupPVC == nil {
		t.Fatal("missing PersistentVolumeClaim in cronjob-backup.yaml")
	}
	if backupCronJob == nil {
		t.Fatal("missing CronJob in cronjob-backup.yaml")
	}

	wantString(at(backupPVC, "metadata", "name"), "narumq-backup-pvc")
	wantString(at(backupPVC, "metadata", "namespace"), "narumq")
	wantString(at(backupPVC, "spec", "storageClassName"), "gp3")
	wantString(at(backupPVC, "spec", "resources", "requests", "storage"), "20Gi")
	wantString(at(backupPVC, "spec", "accessModes", "0"), "ReadWriteOnce")

	cronPodSpec := at(backupCronJob, "spec", "jobTemplate", "spec", "template", "spec")
	cronPodSpecMap := cronPodSpec.(map[string]any)
	if serviceAccount, exists := cronPodSpecMap["serviceAccountName"]; exists && serviceAccount != "default" {
		t.Fatalf("default backup must use the namespace default ServiceAccount, got %v", serviceAccount)
	}
	if _, exists := cronPodSpecMap["initContainers"]; exists {
		t.Fatal("default backup must not start the S3 uploader sidecar")
	}
	if at(cronPodSpec, "automountServiceAccountToken") != false {
		t.Fatal("backup pod must not mount the default Kubernetes API token")
	}
	wantInt(at(backupCronJob, "spec", "jobTemplate", "spec", "activeDeadlineSeconds"), 3600)
	wantInt(at(backupCronJob, "spec", "jobTemplate", "spec", "backoffLimit"), 2)
	backupContainer := findNamed(at(cronPodSpec, "containers"), "name", "backup")
	wantString(at(backupContainer, "image"), "809446814769.dkr.ecr.ap-northeast-2.amazonaws.com/narumq@sha256:09f77ae1e4d2277fb96192abaafbf3e2d1717683746ff8e6acc17efc9ccfa916")
	wantString(at(backupContainer, "command", "0"), "/bin/sh")
	wantString(at(backupContainer, "command", "1"), "/scripts/backup.sh")
	if !hasMount(backupContainer, "secrets", "/secrets") ||
		!hasMount(backupContainer, "backup-scripts", "/scripts") ||
		!hasMount(backupContainer, "backup-storage", "/backups") {
		t.Fatal("default backup is missing its secret, helper, or durable PVC mount")
	}
	if envFromEntries, ok := backupContainer["envFrom"].([]any); ok {
		for _, raw := range envFromEntries {
			envFrom, ok := raw.(map[string]any)
			if !ok {
				t.Fatalf("backup envFrom entry = %T, want mapping", raw)
			}
			ref, _ := envFrom["configMapRef"].(map[string]any)
			if ref["name"] == "narumq-backup-config" {
				t.Fatal("default backup must not depend on the optional S3 settings ConfigMap")
			}
		}
	}
	backupVolumes := at(cronPodSpec, "volumes")
	wantString(at(findNamed(backupVolumes, "name", "secrets"), "secret", "secretName"), "narumq-cluster-secrets")
	wantInt(at(findNamed(backupVolumes, "name", "secrets"), "secret", "defaultMode"), 288)
	wantString(at(findNamed(backupVolumes, "name", "secrets"), "secret", "items", "0", "key"), "admin.token")
	wantString(at(findNamed(backupVolumes, "name", "backup-storage"), "persistentVolumeClaim", "claimName"), "narumq-backup-pvc")
	wantString(at(findNamed(backupVolumes, "name", "backup-scripts"), "configMap", "name"), "narumq-backup-scripts")
	for _, volume := range backupVolumes.([]any) {
		if volume.(map[string]any)["name"] == "run-state" || volume.(map[string]any)["name"] == "tmp" {
			t.Fatalf("default backup must not mount S3-only volume %v", volume)
		}
	}
	assertNoStaticAWSCredentials(cronPodSpec)

	generator := findNamed(at(kustomization, "configMapGenerator"), "name", "narumq-backup-scripts")
	generatedFiles, ok := at(generator, "files").([]any)
	if !ok || len(generatedFiles) != 1 || generatedFiles[0] != "backup.sh" {
		t.Fatalf("default backup script ConfigMap sources = %v, want only backup.sh", at(generator, "files"))
	}
	if at(kustomization, "generatorOptions", "disableNameSuffixHash") != true {
		t.Fatal("backup helper ConfigMap must retain its stable mounted name")
	}

	s3Overlay := manifest("../eks-ec2-ha-s3/kustomization.yaml")
	s3Resources, ok := at(s3Overlay, "resources").([]any)
	if !ok || len(s3Resources) != 1 || s3Resources[0] != "../eks-ec2-ha" {
		t.Fatalf("S3 overlay resources = %v, want only the default package", at(s3Overlay, "resources"))
	}
	s3Generator := findNamed(at(s3Overlay, "configMapGenerator"), "name", "narumq-backup-scripts")
	wantString(at(s3Generator, "behavior"), "merge")
	s3Files, ok := at(s3Generator, "files").([]any)
	if !ok || len(s3Files) != 2 || s3Files[0] != "s3_uploader.py" || s3Files[1] != "restore-from-s3.py" {
		t.Fatalf("S3 helper ConfigMap sources = %v, want only opt-in helpers", at(s3Generator, "files"))
	}
	s3Patch := manifest("../eks-ec2-ha-s3/cronjob-backup-s3-patch.yaml")
	s3PodSpec := at(s3Patch, "spec", "jobTemplate", "spec", "template", "spec")
	wantString(at(s3PodSpec, "serviceAccountName"), "narumq-backup")
	wantString(at(s3Patch, "spec", "jobTemplate", "spec", "template", "metadata", "annotations", "eks.amazonaws.com/skip-containers"), "backup")
	s3Backup := findNamed(at(s3PodSpec, "containers"), "name", "backup")
	wantString(at(s3Backup, "envFrom", "0", "configMapRef", "name"), "narumq-backup-config")
	wantString(at(findNamed(at(s3Backup, "env"), "name", "BACKUP_RUN_ID"), "valueFrom", "fieldRef", "fieldPath"), "metadata.uid")
	uploader := findNamed(at(s3PodSpec, "initContainers"), "name", "s3-uploader")
	wantString(at(uploader, "restartPolicy"), "Always")
	wantString(at(uploader, "image"), "amazon/aws-cli:2.37.9@sha256:92de75724b6a746951f0e8b915d86bbccd7cb55aff96cd0cb4f7017272160780")
	wantString(at(uploader, "command", "0"), "python3")
	wantString(at(uploader, "command", "1"), "/scripts/s3_uploader.py")
	if !hasMount(s3Backup, "run-state", "/run/narumq-backup") ||
		!hasMount(uploader, "backup-scripts", "/scripts") ||
		!hasMount(uploader, "backup-storage", "/backups") ||
		!hasMount(uploader, "run-state", "/run/narumq-backup") {
		t.Fatal("opt-in backup must share its helpers, durable PVC, and per-run marker volume")
	}
	s3Volumes := at(s3PodSpec, "volumes")
	if _, ok := at(findNamed(s3Volumes, "name", "run-state"), "emptyDir").(map[string]any); !ok {
		t.Fatal("S3 backup run-state markers must be per-pod ephemeral state")
	}
	assertNoStaticAWSCredentials(s3PodSpec)

	listJob := manifest("../eks-ec2-ha-s3/restore-from-s3-list-job.yaml")
	wantString(at(listJob, "kind"), "Job")
	wantString(at(listJob, "metadata", "namespace"), "narumq")
	wantString(at(listJob, "spec", "template", "spec", "serviceAccountName"), "narumq-backup-restore")
	if at(listJob, "spec", "template", "spec", "automountServiceAccountToken") != false {
		t.Fatal("restore listing must not mount the default Kubernetes API token")
	}
	wantInt(at(listJob, "spec", "activeDeadlineSeconds"), 300)
	wantInt(at(listJob, "spec", "backoffLimit"), 0)
	listPodSpec := at(listJob, "spec", "template", "spec")
	listContainer := findNamed(at(listPodSpec, "containers"), "name", "list")
	wantString(at(listContainer, "image"), "amazon/aws-cli:2.37.9@sha256:92de75724b6a746951f0e8b915d86bbccd7cb55aff96cd0cb4f7017272160780")
	wantString(at(listContainer, "command", "0"), "python3")
	wantString(at(listContainer, "command", "1"), "/scripts/restore-from-s3.py")
	wantString(at(listContainer, "args", "0"), "list")
	wantString(at(listContainer, "envFrom", "0", "configMapRef", "name"), "narumq-backup-config")
	if !hasMount(listContainer, "backup-scripts", "/scripts") {
		t.Fatal("restore listing must mount the verified helper script")
	}
	assertNoStaticAWSCredentials(listPodSpec)

	seedJobs := manifestDocs("../eks-ec2-ha-s3/restore-from-s3-seed-jobs.yaml")
	if len(seedJobs) != 3 {
		t.Fatalf("S3 seed Jobs = %d, want one per voter", len(seedJobs))
	}
	for index, seedJob := range seedJobs {
		nodeID := strconv.Itoa(index + 1)
		ordinal := strconv.Itoa(index)
		wantString(at(seedJob, "kind"), "Job")
		wantString(at(seedJob, "metadata", "namespace"), "narumq")
		if generateName, ok := at(seedJob, "metadata", "generateName").(string); !ok || generateName == "" {
			t.Fatalf("restore seed Job %d must use a unique generated name", index+1)
		}
		wantInt(at(seedJob, "spec", "activeDeadlineSeconds"), 3600)
		wantInt(at(seedJob, "spec", "backoffLimit"), 0)
		seedPodSpec := at(seedJob, "spec", "template", "spec")
		wantString(at(seedPodSpec, "serviceAccountName"), "narumq-backup-restore")
		if at(seedPodSpec, "automountServiceAccountToken") != false {
			t.Fatalf("restore seed Job %d must not mount the default Kubernetes API token", index+1)
		}
		wantString(at(seedJob, "spec", "template", "metadata", "annotations", "eks.amazonaws.com/skip-containers"), "restore,prepare-target")
		seedInitContainers, ok := at(seedPodSpec, "initContainers").([]any)
		if !ok || len(seedInitContainers) != 2 {
			t.Fatalf("restore seed Job %d init containers = %v", index+1, at(seedPodSpec, "initContainers"))
		}
		download := findNamed(seedInitContainers, "name", "download-from-s3")
		wantString(at(download, "image"), "amazon/aws-cli:2.37.9@sha256:92de75724b6a746951f0e8b915d86bbccd7cb55aff96cd0cb4f7017272160780")
		wantString(at(download, "command", "0"), "python3")
		wantString(at(download, "command", "1"), "/scripts/restore-from-s3.py")
		wantString(at(download, "args", "0"), "download")
		wantString(at(download, "args", "1"), "$(BACKUP_S3_KEY)")
		wantString(at(download, "args", "2"), "/restore")
		wantString(at(findNamed(at(download, "env"), "name", "BACKUP_S3_KEY"), "valueFrom", "configMapKeyRef", "name"), "narumq-backup-restore-request")
		wantString(at(findNamed(at(download, "env"), "name", "BACKUP_S3_KEY"), "valueFrom", "configMapKeyRef", "key"), "BACKUP_S3_KEY")
		if !hasMount(download, "restore-archive", "/restore") || !hasMount(download, "backup-scripts", "/scripts") {
			t.Fatalf("restore seed Job %d downloader must use an ephemeral archive and the verified helper", index+1)
		}
		preparer := findNamed(seedInitContainers, "name", "prepare-target")
		wantInt(at(preparer, "securityContext", "runAsUser"), 0)
		if !hasMount(preparer, "data", "/volume") {
			t.Fatalf("restore seed Job %d must validate and prepare the PVC before subPath mounts", index+1)
		}
		restore := findNamed(at(seedPodSpec, "containers"), "name", "restore")
		wantString(at(restore, "image"), "809446814769.dkr.ecr.ap-northeast-2.amazonaws.com/narumq@sha256:09f77ae1e4d2277fb96192abaafbf3e2d1717683746ff8e6acc17efc9ccfa916")
		wantString(at(restore, "command", "0"), "/bin/sh")
		restoreCommand, ok := at(restore, "args", "0").(string)
		if !ok || !strings.Contains(restoreCommand, "-cluster-restore") || !strings.Contains(restoreCommand, "/configs/node-"+nodeID+".json") {
			t.Fatalf("restore seed Job %d must pass its voter config to the offline restore CLI", index+1)
		}
		wantString(at(findNamed(at(restore, "volumeMounts"), "name", "data"), "subPath"), "data")
		wantString(at(findNamed(at(seedPodSpec, "volumes"), "name", "data"), "persistentVolumeClaim", "claimName"), "data-narumq-"+ordinal)
		sourceSecrets := findNamed(at(seedPodSpec, "volumes"), "name", "cluster-secrets-source")
		wantString(at(sourceSecrets, "secret", "secretName"), "narumq-cluster-secrets")
		wantInt(at(sourceSecrets, "secret", "defaultMode"), 256)
		secretItems, ok := at(sourceSecrets, "secret", "items").([]any)
		if !ok || len(secretItems) != 3 {
			t.Fatalf("restore seed Job %d projected TLS keys = %v, want CA plus only this voter's certificate and key", index+1, at(sourceSecrets, "secret", "items"))
		}
		expectedTLSKeys := []string{"ca.crt", "node-" + ordinal + ".crt", "node-" + ordinal + ".key"}
		for itemIndex, key := range expectedTLSKeys {
			wantString(at(secretItems, strconv.Itoa(itemIndex), "key"), key)
			wantString(at(secretItems, strconv.Itoa(itemIndex), "path"), key)
		}
		assertNoStaticAWSCredentials(seedPodSpec)
	}
	restoreOverlay := manifest("../eks-ec2-ha-restore-mode/kustomization.yaml")
	wantString(at(restoreOverlay, "kind"), "Kustomization")
	overlayResources, ok := at(restoreOverlay, "resources").([]any)
	if !ok || len(overlayResources) != 2 || overlayResources[0] != "../eks-ec2-ha" || overlayResources[1] != "../eks-ec2-ha-restore-targets" {
		t.Fatalf("restore-mode resources = %v, want the base package plus clean target PVCs", at(restoreOverlay, "resources"))
	}
	statefulSetPatch := findNamed(at(restoreOverlay, "patches"), "path", "statefulset-paused.yaml")
	cronJobPatch := findNamed(at(restoreOverlay, "patches"), "path", "cronjob-suspended.yaml")
	wantString(at(statefulSetPatch, "target", "kind"), "StatefulSet")
	wantString(at(cronJobPatch, "target", "kind"), "CronJob")
	wantInt(at(manifest("../eks-ec2-ha-restore-mode/statefulset-paused.yaml"), "spec", "replicas"), 0)
	if at(manifest("../eks-ec2-ha-restore-mode/cronjob-suspended.yaml"), "spec", "suspend") != true {
		t.Fatal("restore-mode must suspend scheduled backup before offline seed")
	}
	restorePVCs := manifestDocs("../eks-ec2-ha-restore-targets/restore-target-pvcs.yaml")
	if len(restorePVCs) != 3 {
		t.Fatalf("restore target PVCs = %d, want one empty claim per voter", len(restorePVCs))
	}
	for ordinal, pvc := range restorePVCs {
		wantString(at(pvc, "kind"), "PersistentVolumeClaim")
		wantString(at(pvc, "metadata", "name"), "data-narumq-"+strconv.Itoa(ordinal))
		wantString(at(pvc, "spec", "storageClassName"), "gp3")
		wantString(at(pvc, "spec", "resources", "requests", "storage"), "20Gi")
	}
	s3RestoreOverlay := manifest("../eks-ec2-ha-s3-restore-mode/kustomization.yaml")
	s3RestoreResources, ok := at(s3RestoreOverlay, "resources").([]any)
	if !ok || len(s3RestoreResources) != 2 || s3RestoreResources[0] != "../eks-ec2-ha-s3" || s3RestoreResources[1] != "../eks-ec2-ha-restore-targets" {
		t.Fatalf("S3 restore-mode resources = %v, want the S3-enabled package plus clean target PVCs", at(s3RestoreOverlay, "resources"))
	}
	s3StatefulSetPatch := findNamed(at(s3RestoreOverlay, "patches"), "path", "statefulset-paused.yaml")
	s3CronJobPatch := findNamed(at(s3RestoreOverlay, "patches"), "path", "cronjob-suspended.yaml")
	wantString(at(s3StatefulSetPatch, "target", "kind"), "StatefulSet")
	wantString(at(s3CronJobPatch, "target", "kind"), "CronJob")
	wantInt(at(manifest("../eks-ec2-ha-s3-restore-mode/statefulset-paused.yaml"), "spec", "replicas"), 0)
	if at(manifest("../eks-ec2-ha-s3-restore-mode/cronjob-suspended.yaml"), "spec", "suspend") != true {
		t.Fatal("S3 restore-mode must suspend the opt-in scheduled backup before offline seed")
	}
	for _, resource := range kustomResources {
		name, _ := resource.(string)
		if name == "restore-from-s3-list-job.yaml" || name == "restore-from-s3-seed-jobs.yaml" {
			t.Fatalf("on-demand restore Jobs must not be started by the base Kustomize package: %v", kustomResources)
		}
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

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-048
func TestEKSDeploymentS3OptInBoundary(t *testing.T) {
	root := filepath.Join("..", "..", "deploy", "kubernetes")
	readDocs := func(name string) []map[string]any {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		var docs []map[string]any
		for {
			var doc map[string]any
			if err := decoder.Decode(&doc); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("decode %s: %v", name, err)
			}
			if len(doc) > 0 {
				docs = append(docs, doc)
			}
		}
		return docs
	}
	manifest := func(name string) map[string]any {
		t.Helper()
		docs := readDocs(name)
		if len(docs) != 1 {
			t.Fatalf("%s contains %d documents, want one", name, len(docs))
		}
		return docs[0]
	}
	field := func(value any, keys ...string) any {
		t.Helper()
		for _, key := range keys {
			object, ok := value.(map[string]any)
			if !ok {
				t.Fatalf("%s parent has type %T", key, value)
			}
			value, ok = object[key]
			if !ok {
				t.Fatalf("missing manifest field %q", key)
			}
		}
		return value
	}
	stringList := func(value any) []string {
		t.Helper()
		items, ok := value.([]any)
		if !ok {
			t.Fatalf("manifest list has type %T", value)
		}
		result := make([]string, 0, len(items))
		for _, item := range items {
			text, ok := item.(string)
			if !ok {
				t.Fatalf("manifest list item has type %T", item)
			}
			result = append(result, text)
		}
		return result
	}
	wantResources := func(name string, want []string) map[string]any {
		t.Helper()
		kustomization := manifest(name)
		got := stringList(field(kustomization, "resources"))
		if len(got) != len(want) {
			t.Fatalf("%s resources = %v, want %v", name, got, want)
		}
		for index := range want {
			if got[index] != want[index] {
				t.Fatalf("%s resources = %v, want %v", name, got, want)
			}
		}
		return kustomization
	}
	findNamed := func(value any, key, name string) map[string]any {
		t.Helper()
		items, ok := value.([]any)
		if !ok {
			t.Fatalf("manifest list for %s has type %T", name, value)
		}
		for _, item := range items {
			entry, ok := item.(map[string]any)
			if ok && entry[key] == name {
				return entry
			}
		}
		t.Fatalf("manifest list has no %s=%q entry", key, name)
		return nil
	}
	findObject := func(docs []map[string]any, kind, name string) map[string]any {
		t.Helper()
		for _, doc := range docs {
			metadata, _ := doc["metadata"].(map[string]any)
			if doc["kind"] == kind && metadata["name"] == name {
				return doc
			}
		}
		t.Fatalf("manifest documents have no %s %q", kind, name)
		return nil
	}
	hasConfigMapRef := func(container map[string]any, name string) bool {
		t.Helper()
		envFrom, _ := container["envFrom"].([]any)
		for _, raw := range envFrom {
			entry, _ := raw.(map[string]any)
			ref, _ := entry["configMapRef"].(map[string]any)
			if ref["name"] == name {
				return true
			}
		}
		return false
	}

	defaultKustomization := manifest("eks-ec2-ha/kustomization.yaml")
	defaultResources := stringList(field(defaultKustomization, "resources"))
	if len(defaultResources) == 0 {
		t.Fatal("default Kustomize package must include local backup resources")
	}
	for _, resource := range defaultResources {
		for _, doc := range readDocs(filepath.Join("eks-ec2-ha", resource)) {
			metadata, _ := doc["metadata"].(map[string]any)
			switch {
			case doc["kind"] == "ServiceAccount" && metadata["name"] == "narumq-backup":
				t.Fatal("default package must not depend on the Terraform-created S3 writer ServiceAccount")
			case doc["kind"] == "ConfigMap" && metadata["name"] == "narumq-backup-config":
				t.Fatal("default package must not depend on the Terraform-created S3 settings ConfigMap")
			case doc["kind"] == "Job":
				t.Fatal("default package must not include on-demand S3 restore Jobs")
			}
		}
	}
	backupCronJob := findObject(readDocs("eks-ec2-ha/cronjob-backup.yaml"), "CronJob", "narumq-backup")
	defaultPod := field(backupCronJob, "spec", "jobTemplate", "spec", "template", "spec")
	if account, ok := defaultPod.(map[string]any)["serviceAccountName"]; ok && account != "default" {
		t.Fatalf("default backup ServiceAccount = %v, want namespace default", account)
	}
	if _, exists := defaultPod.(map[string]any)["initContainers"]; exists {
		t.Fatal("default backup must not start the S3 uploader sidecar")
	}
	defaultBackup := findNamed(field(defaultPod, "containers"), "name", "backup")
	if hasConfigMapRef(defaultBackup, "narumq-backup-config") {
		t.Fatal("default backup must not reference the optional S3 settings ConfigMap")
	}
	defaultGenerator := findNamed(field(defaultKustomization, "configMapGenerator"), "name", "narumq-backup-scripts")
	defaultFiles := stringList(field(defaultGenerator, "files"))
	if len(defaultFiles) != 1 || defaultFiles[0] != "backup.sh" {
		t.Fatalf("default helper ConfigMap files = %v, want only backup.sh", defaultFiles)
	}

	s3Kustomization := wantResources("eks-ec2-ha-s3/kustomization.yaml", []string{"../eks-ec2-ha"})
	s3Generator := findNamed(field(s3Kustomization, "configMapGenerator"), "name", "narumq-backup-scripts")
	if field(s3Generator, "behavior") != "merge" {
		t.Fatal("S3 helper ConfigMap must merge with the default backup script")
	}
	s3Files := stringList(field(s3Generator, "files"))
	if len(s3Files) != 2 || s3Files[0] != "s3_uploader.py" || s3Files[1] != "restore-from-s3.py" {
		t.Fatalf("S3 helper ConfigMap files = %v, want both S3 helpers", s3Files)
	}
	s3Patch := manifest("eks-ec2-ha-s3/cronjob-backup-s3-patch.yaml")
	s3Pod := field(s3Patch, "spec", "jobTemplate", "spec", "template", "spec")
	if got := field(s3Pod, "serviceAccountName"); got != "narumq-backup" {
		t.Fatalf("S3 backup ServiceAccount = %v, want narumq-backup", got)
	}
	s3Backup := findNamed(field(s3Pod, "containers"), "name", "backup")
	if !hasConfigMapRef(s3Backup, "narumq-backup-config") {
		t.Fatal("S3 backup must reference its Terraform-created settings ConfigMap")
	}

	wantResources("eks-ec2-ha-restore-mode/kustomization.yaml", []string{"../eks-ec2-ha", "../eks-ec2-ha-restore-targets"})
	wantResources("eks-ec2-ha-s3-restore-mode/kustomization.yaml", []string{"../eks-ec2-ha-s3", "../eks-ec2-ha-restore-targets"})
	for _, overlay := range []string{"eks-ec2-ha-restore-mode", "eks-ec2-ha-s3-restore-mode"} {
		statefulSetPatch := manifest(filepath.Join(overlay, "statefulset-paused.yaml"))
		if replicas, ok := field(statefulSetPatch, "spec", "replicas").(int); !ok || replicas != 0 {
			t.Fatalf("%s restore replicas = %v, want zero", overlay, field(statefulSetPatch, "spec", "replicas"))
		}
		cronJobPatch := manifest(filepath.Join(overlay, "cronjob-suspended.yaml"))
		if field(cronJobPatch, "spec", "suspend") != true {
			t.Fatalf("%s restore must suspend the scheduled backup", overlay)
		}
	}
}
