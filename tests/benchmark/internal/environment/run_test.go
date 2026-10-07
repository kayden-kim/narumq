package environment

import (
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func validTargetOptions() TargetOptions {
	return TargetOptions{
		BenchmarkID:          "bp01",
		RunID:                "run-1",
		Target:               "narumq",
		ArtifactDirectory:    "artifacts/narumq",
		ProfilePath:          "profile.json",
		ImageReference:       "narumq:run-1",
		RunnerReference:      "runner:run-1",
		RunnerImageID:        "runner-id",
		RunnerTarget:         "bp01-runner",
		BrokerCPUs:           1,
		BrokerMemoryBytes:    128 << 20,
		GeneratorCPUs:        1,
		GeneratorMemoryBytes: 64 << 20,
		RunnerArgs:           []string{"run", "--profile", "/input/profile.json"},
		Timeout:              time.Minute,
		StartupTimeout:       time.Minute,
	}
}

func TestValidateTargetOptionsRuntimeCollection(t *testing.T) {
	valid := validTargetOptions()
	valid.CollectRuntimeMetrics = true
	valid.RuntimeMetricsInterval = 500 * time.Millisecond
	if err := validateTargetOptions(valid); err != nil {
		t.Fatalf("validateTargetOptions(valid runtime collection) error = %v", err)
	}

	for _, test := range []struct {
		name   string
		modify func(*TargetOptions)
	}{
		{
			name: "missing interval",
			modify: func(options *TargetOptions) {
				options.CollectRuntimeMetrics = true
			},
		},
		{
			name: "emqx collection",
			modify: func(options *TargetOptions) {
				options.Target = "emqx"
				options.CollectRuntimeMetrics = true
				options.RuntimeMetricsInterval = time.Second
			},
		},
		{
			name: "interval without collection",
			modify: func(options *TargetOptions) {
				options.RuntimeMetricsInterval = time.Second
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := validTargetOptions()
			test.modify(&options)
			if err := validateTargetOptions(options); err == nil {
				t.Fatal("validateTargetOptions() accepted invalid runtime collection")
			}
		})
	}
}

func TestBuildBrokerCreateArgsRuntimeCollection(t *testing.T) {
	options := validTargetOptions()
	containerName := "bp01-run-1-narumq"
	disabled := buildBrokerCreateArgs(options, containerName)
	wantDisabled := []string{
		"run", "--detach", "--name", containerName,
		"--network", "none", "--cpus", "1", "--memory", "134217728b",
		"--read-only", "--tmpfs", "/tmp:size=64m",
		options.ImageReference, "-tcp-listen", "127.0.0.1:1883", "-allow-insecure-anonymous-mqtt",
	}
	if !reflect.DeepEqual(disabled, wantDisabled) {
		t.Fatalf("disabled broker argv = %#v, want %#v", disabled, wantDisabled)
	}
	if countToken(disabled, "-allow-insecure-anonymous-mqtt") != 1 {
		t.Fatalf("disabled broker argv = %#v, want one explicit insecure MQTT flag", disabled)
	}

	options.CollectRuntimeMetrics = true
	options.RuntimeMetricsInterval = 500 * time.Millisecond
	enabled := buildBrokerCreateArgs(options, containerName)
	if countToken(enabled, "--mount") != 1 || countToken(enabled, "-runtime-metrics-output") != 1 || countToken(enabled, "-runtime-metrics-interval") != 1 {
		t.Fatalf("enabled broker argv = %#v, want one runtime mount and each runtime flag", enabled)
	}
	if countToken(enabled, "-allow-insecure-anonymous-mqtt") != 1 {
		t.Fatalf("enabled broker argv = %#v, want one explicit insecure MQTT flag", enabled)
	}
	if !containsToken(enabled, "type=bind,source="+options.ArtifactDirectory+",destination=/benchmark-output") || !containsToken(enabled, "/benchmark-output/runtime-metrics.jsonl") || !containsToken(enabled, "500ms") {
		t.Fatalf("enabled broker argv = %#v, missing runtime mount or flags", enabled)
	}

	emqx := validTargetOptions()
	emqx.Target = "emqx"
	emqx.ImageReference = EMQXImageReference
	emqx.RunnerTarget = "bp05-runner"
	emqxArgs := buildBrokerCreateArgs(emqx, "bp01-run-1-emqx")
	joined := strings.Join(emqxArgs, "\x00")
	if strings.Contains(joined, "runtime-metrics") || strings.Contains(joined, "benchmark-output") {
		t.Fatalf("EMQX broker argv = %#v, must not include runtime collection", emqxArgs)
	}
	if countToken(emqxArgs, "-allow-insecure-anonymous-mqtt") != 0 {
		t.Fatalf("EMQX broker argv = %#v, must not include NaruMQ insecure MQTT flag", emqxArgs)
	}
}

func TestNewTargetResultRuntimeMetricsPath(t *testing.T) {
	result := newTargetResult(validTargetOptions())
	if result.RuntimeMetricsPath != filepath.Join("artifacts", "narumq", "runtime-metrics.jsonl") {
		t.Fatalf("NaruMQ RuntimeMetricsPath = %q", result.RuntimeMetricsPath)
	}
	emqxOptions := validTargetOptions()
	emqxOptions.Target = "emqx"
	if result := newTargetResult(emqxOptions); result.RuntimeMetricsPath != "" {
		t.Fatalf("EMQX RuntimeMetricsPath = %q, want empty not-applicable path", result.RuntimeMetricsPath)
	}
}
func TestFinishTargetFailurePreservesAllReasons(t *testing.T) {
	result := finishTargetFailure(newTargetResult(validTargetOptions()), errors.New("runner failed"))
	result = finishTargetFailure(result, errors.New("broker stop failed"))
	if result.Success || !strings.Contains(result.FailureReason, "runner failed") || !strings.Contains(result.FailureReason, "broker stop failed") {
		t.Fatalf("finishTargetFailure() = %#v, want both failure reasons", result)
	}
}

func validClusterOptions() ClusterTargetOptions {
	return ClusterTargetOptions{
		BenchmarkID: "bp03", RunID: "run-1", Target: "narumq",
		ArtifactDirectory: "artifacts/narumq", ProfilePath: "profile.json",
		ImageReference: "narumq:run-1", RunnerReference: "runner:run-1",
		RunnerImageID: "runner-id", RunnerTarget: "bp03-runner",
		NetworkOwnerArgs:            []string{"serve-jwks", "--file", "/config/jwks.json"},
		NetworkOwnerConfigDirectory: "public-jwks",
		RunnerConfigDirectory:       "runner-config",
		Nodes: []ClusterNodeSpec{
			{Name: "node1", DNSName: "node1.bp03.test", ConfigDirectory: "node1", MountConfig: true, DataDestination: "/var/lib/narumq", BrokerArgs: []string{"-cluster-config", "/config/node.json"}},
			{Name: "node2", DNSName: "node2.bp03.test", ConfigDirectory: "node2", DataDestination: "/var/lib/narumq"},
			{Name: "node3", DNSName: "node3.bp03.test", ConfigDirectory: "node3", DataDestination: "/var/lib/narumq"},
		},
		BrokerCPUs: 1, BrokerMemoryBytes: 1 << 30, GeneratorCPUs: 2, GeneratorMemoryBytes: 1 << 29,
		RunnerArgs: []string{"run"}, Timeout: time.Minute, StartupTimeout: time.Minute,
	}
}

func TestValidateClusterTargetOptions(t *testing.T) {
	valid := validClusterOptions()
	if err := validateClusterTargetOptions(valid); err != nil {
		t.Fatalf("valid cluster options: %v", err)
	}
	cases := []struct {
		name   string
		modify func(*ClusterTargetOptions)
	}{
		{"two nodes", func(o *ClusterTargetOptions) { o.Nodes = o.Nodes[:2] }},
		{"duplicate dns names", func(o *ClusterTargetOptions) { o.Nodes[1].DNSName = o.Nodes[0].DNSName }},
		{"wrong runner target", func(o *ClusterTargetOptions) { o.RunnerTarget = "bp01-runner" }},
		{"missing network owner public config", func(o *ClusterTargetOptions) { o.NetworkOwnerConfigDirectory = "" }},
		{"live stats stream with broker restarts", func(o *ClusterTargetOptions) {
			o.LiveStatsStream = true
			o.AllowBrokerRestart = true
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			options := validClusterOptions()
			tc.modify(&options)
			if err := validateClusterTargetOptions(options); err == nil {
				t.Fatal("validateClusterTargetOptions accepted invalid options")
			}
		})
	}
}

func TestBuildClusterBrokerArgs(t *testing.T) {
	options := validClusterOptions()
	node := ClusterNodeSpec{Name: "node2", DNSName: "node2.bp03.test", ConfigDirectory: "node2-config", DataDestination: "/opt/emqx/data", LogDestination: "/opt/emqx/log", BrokerEnv: []string{"EMQX_NODE__NAME=emqx@node2.bp03.test", "EMQX_DURABLE_SESSIONS__ENABLE=false"}}
	options.Target = "emqx"
	options.ImageReference = EMQXImageReference
	emqx := buildClusterBrokerArgs(options, node, "owner")
	if !containsToken(emqx, "type=volume,source=bp03-run-1-emqx-node2-log,destination=/opt/emqx/log") || !containsToken(emqx, "EMQX_NODE__NAME=emqx@node2.bp03.test") || !containsToken(emqx, "EMQX_DURABLE_SESSIONS__ENABLE=false") {
		t.Fatalf("EMQX broker args missing log mount/env: %#v", emqx)
	}

	options.Target = "narumq"
	naruNode := ClusterNodeSpec{Name: "node1", ConfigDirectory: "node1-config", MountConfig: true, DataDestination: "/var/lib/narumq", BrokerArgs: []string{"-cluster-config", "/config/node.json"}}
	naru := buildClusterBrokerArgs(options, naruNode, "owner")
	if !containsToken(naru, "type=bind,source=node1-config,destination=/config,readonly") || !containsToken(naru, "-cluster-config") || !containsToken(naru, "/config/node.json") {
		t.Fatalf("NaruMQ broker args missing config mount/args: %#v", naru)
	}
}

func TestBuildClusterNetworkOwnerArgsUsesPublicConfig(t *testing.T) {
	options := validClusterOptions()
	node := options.Nodes[0]
	owner := buildClusterNetworkOwnerArgs(options, node, "bp03-run-1-narumq-node1-net")
	if !containsToken(owner, "type=bind,source=public-jwks,destination=/config,readonly") {
		t.Fatalf("network owner args missing public config mount: %#v", owner)
	}
	if containsToken(owner, "type=bind,source=node1,destination=/config,readonly") {
		t.Fatalf("network owner args expose node-private config: %#v", owner)
	}
	if !containsToken(owner, "/config/jwks.json") {
		t.Fatalf("network owner args do not serve /config/jwks.json: %#v", owner)
	}
	broker := buildClusterBrokerArgs(options, node, "owner")
	if !containsToken(broker, "type=bind,source=node1,destination=/config,readonly") ||
		containsToken(broker, "type=bind,source=public-jwks,destination=/config,readonly") {
		t.Fatalf("broker config mount changed: %#v", broker)
	}
}
func TestRedactInspectEnvSecrets(t *testing.T) {
	input := []byte(`[{"Id":"container","Config":{"Env":["EMQX_NODE__COOKIE=private-cookie","NARUMQ_ADMIN_TOKEN=private-token","EMQX_DURABLE_SESSIONS__ENABLE=false"]}}]`)
	redacted, err := redactInspectEnvSecrets(input)
	if err != nil {
		t.Fatal(err)
	}
	output := string(redacted)
	for _, value := range []string{"private-cookie", "private-token"} {
		if strings.Contains(output, value) {
			t.Errorf("redacted inspect JSON contains secret value %q", value)
		}
	}
	for _, value := range []string{"EMQX_NODE__COOKIE=REDACTED", "NARUMQ_ADMIN_TOKEN=REDACTED", "EMQX_DURABLE_SESSIONS__ENABLE=false"} {
		if !strings.Contains(output, value) {
			t.Errorf("redacted inspect JSON omits %q", value)
		}
	}
	if _, err := redactInspectEnvSecrets([]byte("{")); err == nil {
		t.Fatal("redactInspectEnvSecrets() accepted malformed JSON")
	}
}

func countToken(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

func containsToken(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
