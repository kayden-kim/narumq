package environment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func RunTarget(ctx context.Context, options TargetOptions) (TargetResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateTargetOptions(options); err != nil {
		return TargetResult{}, err
	}
	result := newTargetResult(options)
	if err := os.MkdirAll(options.ArtifactDirectory, 0o755); err != nil {
		return result, err
	}
	containerName := options.BenchmarkID + "-" + options.RunID + "-" + options.Target
	volumeNames := []string{}
	cleanup := func() {
		_, _ = commandOutput(context.Background(), "", "docker", "rm", "--force", containerName)
		for _, volumeName := range volumeNames {
			_, _ = commandOutput(context.Background(), "", "docker", "volume", "rm", "--force", volumeName)
		}
	}
	defer cleanup()

	if options.Target == "emqx" {
		volumeNames = []string{containerName + "-data", containerName + "-log"}
		for _, volumeName := range volumeNames {
			output, err := commandOutput(ctx, "", "docker", "volume", "create", volumeName)
			if err != nil {
				return finishTargetFailure(result, fmt.Errorf("create volume %s: %w", volumeName, err)), nil
			}
			if strings.TrimSpace(string(output)) == "" {
				return finishTargetFailure(result, fmt.Errorf("create volume %s returned no name", volumeName)), nil
			}
		}
	}
	createArgs := buildBrokerCreateArgs(options, containerName)
	output, err := commandOutput(ctx, "", "docker", createArgs...)
	if err != nil {
		return finishTargetFailure(result, fmt.Errorf("start %s broker: %w", options.Target, err)), nil
	}
	if strings.TrimSpace(string(output)) == "" {
		return finishTargetFailure(result, fmt.Errorf("start %s broker returned no container ID", options.Target)), nil
	}
	if err := waitForContainerRunning(ctx, containerName, options.StartupTimeout); err != nil {
		return finishTargetFailure(result, err), nil
	}
	if err := waitForBrokerReady(ctx, containerName, options.Target, options.StartupTimeout); err != nil {
		return finishTargetFailure(result, err), nil
	}
	if result.ImageID, err = dockerImageID(ctx, "", options.ImageReference); err != nil {
		return finishTargetFailure(result, err), nil
	}
	if result.ImageRepoDigests, err = dockerRepoDigests(ctx, "", options.ImageReference); err != nil {
		return finishTargetFailure(result, err), nil
	}

	statsCancel, statsDone := startStatsSampler(ctx, containerName, result.BrokerStatsPath)
	statsStopped := false
	stopStats := func() error {
		if statsStopped {
			return nil
		}
		statsStopped = true
		statsCancel()
		return <-statsDone
	}
	if options.PreRunSamplingDuration > 0 {
		if err := waitContext(ctx, options.PreRunSamplingDuration); err != nil {
			if statsErr := stopStats(); statsErr != nil {
				result = finishTargetFailure(result, fmt.Errorf("broker stats failed during pre-run shutdown: %w", statsErr))
			}
			return finishTargetFailure(result, fmt.Errorf("pre-run sampling: %w", err)), nil
		}
	}

	runContext, cancel := context.WithTimeout(ctx, options.Timeout)
	runnerArgs := []string{
		"run", "--rm", "--name", containerName + "-runner",
		"--network", "container:" + containerName,
		"--cpus", formatCPU(options.GeneratorCPUs),
		"--memory", strconv.FormatInt(options.GeneratorMemoryBytes, 10) + "b",
		"--read-only", "--tmpfs", "/tmp:size=64m",
		"--mount", "type=bind,source=" + options.ProfilePath + ",destination=/input/profile.json,readonly",
		"--mount", "type=bind,source=" + options.ArtifactDirectory + ",destination=/output",
		options.RunnerReference,
	}
	runnerArgs = append(runnerArgs, options.RunnerArgs...)
	runnerOutput, runnerErr := commandOutput(runContext, "", "docker", runnerArgs...)
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if statsErr := stopStats(); statsErr != nil {
		result = finishTargetFailure(result, fmt.Errorf("broker stats failed: %w", statsErr))
	}
	if sampleErr := appendBrokerStatsSample(shutdownCtx, containerName, result.BrokerStatsPath); sampleErr != nil {
		result = finishTargetFailure(result, fmt.Errorf("final broker stats sample failed: %w", sampleErr))
	}
	if writeErr := os.WriteFile(result.RunnerLogPath, runnerOutput, 0o644); writeErr != nil {
		result = finishTargetFailure(result, fmt.Errorf("write runner log: %w", writeErr))
	}
	if runnerErr != nil {
		result = finishTargetFailure(result, fmt.Errorf("runner failed: %w", runnerErr))
	}
	if _, stopErr := commandOutput(shutdownCtx, "", "docker", "stop", "--time", "10", containerName); stopErr != nil {
		result = finishTargetFailure(result, fmt.Errorf("stop %s broker: %w", options.Target, stopErr))
	}
	if options.CollectRuntimeMetrics {
		info, statErr := os.Stat(result.RuntimeMetricsPath)
		if statErr != nil {
			result = finishTargetFailure(result, fmt.Errorf("read runtime metrics artifact: %w", statErr))
		} else if info.Size() == 0 {
			result = finishTargetFailure(result, errors.New("runtime metrics artifact is empty"))
		}
	}
	if inspect, inspectErr := commandOutput(shutdownCtx, "", "docker", "inspect", containerName); inspectErr == nil {
		if writeErr := os.WriteFile(result.ContainerInspect, inspect, 0o644); writeErr != nil {
			result = finishTargetFailure(result, fmt.Errorf("write container inspect: %w", writeErr))
		}
	} else {
		result = finishTargetFailure(result, fmt.Errorf("inspect broker container: %w", inspectErr))
	}
	if logs, logErr := commandOutput(shutdownCtx, "", "docker", "logs", containerName); logErr == nil {
		if writeErr := os.WriteFile(result.BrokerLogPath, logs, 0o644); writeErr != nil {
			result = finishTargetFailure(result, fmt.Errorf("write broker log: %w", writeErr))
		}
		if options.CollectRuntimeMetrics && strings.Contains(string(logs), "runtime metrics sampler") {
			result = finishTargetFailure(result, errors.New("runtime metrics sampler failed; see broker log"))
		}
	} else {
		result = finishTargetFailure(result, fmt.Errorf("collect broker logs: %w", logErr))
	}
	if result.FailureReason == "" {
		result.Status = "success"
		result.Success = true
	}
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return result, nil
}

func buildBrokerCreateArgs(options TargetOptions, containerName string) []string {
	args := []string{
		"run", "--detach", "--name", containerName,
		"--network", "none",
		"--cpus", formatCPU(options.BrokerCPUs),
		"--memory", strconv.FormatInt(options.BrokerMemoryBytes, 10) + "b",
		"--read-only", "--tmpfs", "/tmp:size=64m",
	}
	if options.Target == "emqx" {
		args = append(args,
			"--mount", "type=volume,source="+containerName+"-data,destination=/opt/emqx/data",
			"--mount", "type=volume,source="+containerName+"-log,destination=/opt/emqx/log",
			"--env", "EMQX_NODE__NAME=emqx@127.0.0.1",
			"--env", "EMQX_NODE__COOKIE="+options.BenchmarkID+"-emqx-cookie",
		)
	}
	if options.Target == "narumq" && options.CollectRuntimeMetrics {
		args = append(args, "--mount", "type=bind,source="+options.ArtifactDirectory+",destination=/benchmark-output")
	}
	args = append(args, options.ImageReference)
	if options.Target == "narumq" {
		args = append(args, "-tcp-listen", "127.0.0.1:1883")
		if options.CollectRuntimeMetrics {
			args = append(args,
				"-runtime-metrics-output", "/benchmark-output/runtime-metrics.jsonl",
				"-runtime-metrics-interval", options.RuntimeMetricsInterval.String(),
			)
		}
	}
	return args
}

func validateTargetOptions(options TargetOptions) error {
	if strings.TrimSpace(options.BenchmarkID) == "" {
		return errors.New("benchmark ID is required")
	}
	if strings.TrimSpace(options.RunID) == "" {
		return errors.New("run ID is required")
	}
	if options.Target != "narumq" && options.Target != "emqx" {
		return fmt.Errorf("unsupported benchmark target %q", options.Target)
	}
	if options.CollectRuntimeMetrics {
		if options.Target != "narumq" {
			return errors.New("runtime metrics collection is supported only for narumq")
		}
		if options.RuntimeMetricsInterval <= 0 {
			return errors.New("runtime metrics interval must be greater than zero")
		}
	} else if options.RuntimeMetricsInterval != 0 {
		return errors.New("runtime metrics interval requires runtime metrics collection")
	}

	if strings.TrimSpace(options.ArtifactDirectory) == "" {
		return errors.New("artifact directory is required")
	}
	if strings.TrimSpace(options.ProfilePath) == "" {
		return errors.New("profile path is required")
	}
	if strings.TrimSpace(options.ImageReference) == "" || strings.TrimSpace(options.RunnerReference) == "" {
		return errors.New("broker and runner image references are required")
	}
	if options.RunnerTarget != "bp01-runner" && options.RunnerTarget != "bp05-runner" {
		return fmt.Errorf("unsupported runner target %q", options.RunnerTarget)
	}
	if options.BrokerCPUs <= 0 || math.IsNaN(options.BrokerCPUs) || math.IsInf(options.BrokerCPUs, 0) || options.BrokerMemoryBytes <= 0 || options.GeneratorCPUs <= 0 || math.IsNaN(options.GeneratorCPUs) || math.IsInf(options.GeneratorCPUs, 0) || options.GeneratorMemoryBytes <= 0 {
		return errors.New("broker and generator resource limits must be positive finite values")
	}
	if len(options.RunnerArgs) == 0 {
		return errors.New("runner arguments are required")
	}
	if options.Timeout <= 0 {
		return errors.New("target timeout must be greater than zero")
	}
	if options.StartupTimeout <= 0 {
		return errors.New("startup timeout must be greater than zero")
	}
	if options.PreRunSamplingDuration < 0 {
		return errors.New("pre-run sampling duration must not be negative")
	}
	return nil
}
func newTargetResult(options TargetOptions) TargetResult {
	runtimeMetricsPath := ""
	if options.Target == "narumq" {
		runtimeMetricsPath = filepath.Join(options.ArtifactDirectory, "runtime-metrics.jsonl")
	}
	return TargetResult{
		Target:             options.Target,
		Status:             "failed",
		StartedAt:          time.Now().UTC().Format(time.RFC3339Nano),
		ArtifactDirectory:  options.ArtifactDirectory,
		SummaryPath:        filepath.Join(options.ArtifactDirectory, "summary.json"),
		SamplesPath:        filepath.Join(options.ArtifactDirectory, "samples.jsonl"),
		RuntimeMetricsPath: runtimeMetricsPath,
		BrokerStatsPath:    filepath.Join(options.ArtifactDirectory, "broker-stats.jsonl"),
		BrokerLogPath:      filepath.Join(options.ArtifactDirectory, "broker.log"),
		RunnerLogPath:      filepath.Join(options.ArtifactDirectory, "runner.log"),
		ContainerInspect:   filepath.Join(options.ArtifactDirectory, "container-inspect.json"),
		ImageReference:     options.ImageReference,
		RunnerImageID:      options.RunnerImageID,
		RunnerTarget:       options.RunnerTarget,
	}
}
func finishTargetFailure(result TargetResult, err error) TargetResult {
	if err != nil {
		if result.FailureReason == "" {
			result.FailureReason = err.Error()
		} else if result.FailureReason != err.Error() {
			result.FailureReason = errors.Join(errors.New(result.FailureReason), err).Error()
		}
	}
	result.Status = "failed"
	result.Success = false
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return result
}

func formatCPU(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitForContainerRunning(ctx context.Context, containerName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		status, err := commandOutput(ctx, "", "docker", "inspect", "--format", "{{.State.Status}}", containerName)
		if err != nil {
			return fmt.Errorf("inspect broker startup: %w", err)
		}
		switch strings.TrimSpace(string(status)) {
		case "running":
			return nil
		case "created", "restarting":
			if time.Now().After(deadline) {
				return errors.New("broker startup timeout")
			}
			if err := waitContext(ctx, 100*time.Millisecond); err != nil {
				return err
			}
		default:
			return fmt.Errorf("broker container entered %q before runner startup", strings.TrimSpace(string(status)))
		}
	}
}

func waitForBrokerReady(ctx context.Context, containerName, target string, timeout time.Duration) error {
	marker := "MQTT TCP listener started"
	if target == "emqx" {
		marker = "EMQX 5.8.8 is running now!"
	}
	deadline := time.Now().Add(timeout)
	for {
		status, inspectErr := commandOutput(ctx, "", "docker", "inspect", "--format", "{{.State.Status}}", containerName)
		if inspectErr != nil {
			return fmt.Errorf("inspect broker readiness: %w", inspectErr)
		}
		statusText := strings.TrimSpace(string(status))
		if statusText != "running" {
			logs, _ := commandOutput(ctx, "", "docker", "logs", containerName)
			return fmt.Errorf("broker entered %q before readiness marker %q: %s", statusText, marker, strings.TrimSpace(string(logs)))
		}
		logs, logsErr := commandOutput(ctx, "", "docker", "logs", containerName)
		if logsErr == nil && strings.Contains(string(logs), marker) {
			return nil
		}
		if time.Now().After(deadline) {
			if logsErr != nil {
				return fmt.Errorf("broker readiness timeout waiting for %q: %w", marker, logsErr)
			}
			return fmt.Errorf("broker readiness timeout waiting for %q", marker)
		}
		if err := waitContext(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
}

func commandText(ctx context.Context, workingDirectory, name string, args ...string) (string, error) {
	output, err := commandOutput(ctx, workingDirectory, name, args...)
	return string(output), err
}

func commandOutput(ctx context.Context, workingDirectory, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	if workingDirectory != "" {
		command.Dir = workingDirectory
	}
	return command.CombinedOutput()
}
func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}
