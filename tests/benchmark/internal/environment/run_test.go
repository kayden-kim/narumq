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
		options.ImageReference, "-tcp-listen", "127.0.0.1:1883",
	}
	if !reflect.DeepEqual(disabled, wantDisabled) {
		t.Fatalf("disabled broker argv = %#v, want %#v", disabled, wantDisabled)
	}

	options.CollectRuntimeMetrics = true
	options.RuntimeMetricsInterval = 500 * time.Millisecond
	enabled := buildBrokerCreateArgs(options, containerName)
	if countToken(enabled, "--mount") != 1 || countToken(enabled, "-runtime-metrics-output") != 1 || countToken(enabled, "-runtime-metrics-interval") != 1 {
		t.Fatalf("enabled broker argv = %#v, want one runtime mount and each runtime flag", enabled)
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
