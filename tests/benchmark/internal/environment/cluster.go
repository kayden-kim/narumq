package environment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ClusterNodeSpec describes one broker and its network namespace owner.
type ClusterNodeSpec struct {
	Name, DNSName, ConfigDirectory string
	MountConfig                    bool
	BrokerArgs, BrokerEnv          []string
	DataDestination                string
	LogDestination, ReadyLogMarker string
}

// ClusterTargetOptions describes a three-node broker cluster and its runner.
type ClusterTargetOptions struct {
	BenchmarkID, RunID, Target, ArtifactDirectory, ProfilePath string
	ImageReference                                             string
	RunnerReference, RunnerImageID, RunnerTarget               string
	NetworkOwnerArgs                                           []string
	NetworkOwnerConfigDirectory                                string
	RunnerConfigDirectory                                      string
	Nodes                                                      []ClusterNodeSpec
	BrokerCPUs, GeneratorCPUs                                  float64
	BrokerMemoryBytes, GeneratorMemoryBytes                    int64
	RunnerArgs                                                 []string
	Timeout, StartupTimeout                                    time.Duration
}

// ClusterNodeResult contains per-node container names and artifact paths.
type ClusterNodeResult struct {
	Name, BrokerContainer, NetworkOwnerContainer                           string
	BrokerStatsPath, NetworkStatsPath, BrokerLogPath, ContainerInspectPath string
}

// ClusterTargetResult is the environment-owned result for one cluster target.
type ClusterTargetResult struct {
	Target, Status                                                       string
	Success                                                              bool
	StartedAt, FinishedAt, ArtifactDirectory, NetworkName, RunnerLogPath string
	ImageReference, ImageID                                              string
	ImageRepoDigests                                                     []string
	RunnerImageID, RunnerTarget, FailureReason                           string
	Nodes                                                                []ClusterNodeResult
}

// RunClusterTarget executes a three-node target. Lifecycle failures are recorded in the result.
func RunClusterTarget(ctx context.Context, options ClusterTargetOptions) (ClusterTargetResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateClusterTargetOptions(options); err != nil {
		return ClusterTargetResult{}, err
	}
	prefix := options.BenchmarkID + "-" + options.RunID + "-" + options.Target
	result := ClusterTargetResult{
		Target:            options.Target,
		Status:            "failed",
		StartedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		ArtifactDirectory: options.ArtifactDirectory,
		NetworkName:       prefix,
		RunnerLogPath:     filepath.Join(options.ArtifactDirectory, "runner.log"),
		ImageReference:    options.ImageReference,
		RunnerImageID:     options.RunnerImageID,
		RunnerTarget:      options.RunnerTarget,
	}
	if err := os.MkdirAll(options.ArtifactDirectory, 0o755); err != nil {
		return finishClusterFailure(result, err), nil
	}
	var containers, volumes []string
	cleanup := func() {
		for index := len(containers) - 1; index >= 0; index-- {
			_, _ = commandOutput(context.Background(), "", "docker", "rm", "--force", containers[index])
		}
		for _, name := range volumes {
			_, _ = commandOutput(context.Background(), "", "docker", "volume", "rm", "--force", name)
		}
		_, _ = commandOutput(context.Background(), "", "docker", "network", "rm", result.NetworkName)
	}
	defer cleanup()
	fail := func(err error) (ClusterTargetResult, error) {
		return finishClusterFailure(result, err), nil
	}
	if _, err := commandOutput(ctx, "", "docker", "network", "create", prefix); err != nil {
		return fail(fmt.Errorf("create cluster network: %w", err))
	}
	result.Nodes = make([]ClusterNodeResult, len(options.Nodes))
	for i, node := range options.Nodes {
		name := prefix + "-" + node.Name
		owner := name + "-net"
		nodeDir := filepath.Join(options.ArtifactDirectory, node.Name)
		if err := os.MkdirAll(nodeDir, 0o755); err != nil {
			return fail(fmt.Errorf("create node artifact directory: %w", err))
		}
		result.Nodes[i] = ClusterNodeResult{Name: node.Name, BrokerContainer: name, NetworkOwnerContainer: owner, BrokerStatsPath: filepath.Join(nodeDir, "broker-stats.jsonl"), NetworkStatsPath: filepath.Join(nodeDir, "network-stats.jsonl"), BrokerLogPath: filepath.Join(nodeDir, "broker.log"), ContainerInspectPath: filepath.Join(nodeDir, "container-inspect.json")}
		for _, volume := range clusterNodeVolumes(prefix, node) {
			output, err := commandOutput(ctx, "", "docker", "volume", "create", volume)
			if err != nil {
				return fail(fmt.Errorf("create volume %s: %w", volume, err))
			}
			if strings.TrimSpace(string(output)) == "" {
				return fail(fmt.Errorf("create volume %s returned no name", volume))
			}
			volumes = append(volumes, volume)
		}
		containers = append(containers, owner)
		ownerArgs := buildClusterNetworkOwnerArgs(options, node, owner)
		if out, err := commandOutput(ctx, "", "docker", ownerArgs...); err != nil {
			return fail(fmt.Errorf("start network owner %s: %w", owner, err))
		} else if strings.TrimSpace(string(out)) == "" {
			return fail(fmt.Errorf("start network owner %s returned no container ID", owner))
		}
		containers = append(containers, name)
		brokerArgs := buildClusterBrokerArgs(options, node, owner)
		if out, err := commandOutput(ctx, "", "docker", brokerArgs...); err != nil {
			return fail(fmt.Errorf("start %s broker: %w", node.Name, err))
		} else if strings.TrimSpace(string(out)) == "" {
			return fail(fmt.Errorf("start %s broker returned no container ID", node.Name))
		}
	}
	for _, node := range result.Nodes {
		for _, container := range []string{node.NetworkOwnerContainer, node.BrokerContainer} {
			if err := waitForContainerRunning(ctx, container, options.StartupTimeout); err != nil {
				return fail(err)
			}
		}
	}
	for i, node := range options.Nodes {
		if node.ReadyLogMarker != "" {
			if err := waitForBrokerReady(ctx, result.Nodes[i].BrokerContainer, node.ReadyLogMarker, options.StartupTimeout); err != nil {
				return fail(err)
			}
		}
	}
	var err error
	if result.ImageID, err = dockerImageID(ctx, "", options.ImageReference); err != nil {
		return fail(err)
	}
	if result.ImageRepoDigests, err = dockerRepoDigests(ctx, "", options.ImageReference); err != nil {
		return fail(err)
	}

	type sampler struct {
		cancel    func()
		done      <-chan error
		container string
		path      string
	}
	samplers := make([]sampler, 0, len(result.Nodes)*2)
	for _, node := range result.Nodes {
		items := []struct {
			container string
			path      string
		}{
			{container: node.BrokerContainer, path: node.BrokerStatsPath},
			{container: node.NetworkOwnerContainer, path: node.NetworkStatsPath},
		}
		for _, item := range items {
			if err := os.MkdirAll(filepath.Dir(item.path), 0o755); err != nil {
				return fail(fmt.Errorf("create stats directory: %w", err))
			}
			cancel, done := startStatsSampler(ctx, item.container, item.path)
			samplers = append(samplers, sampler{
				cancel: cancel, done: done, container: item.container, path: item.path,
			})
		}
	}
	stopSamplers := func(shutdownCtx context.Context) {
		for _, s := range samplers {
			s.cancel()
		}
		for _, s := range samplers {
			if sampleErr := <-s.done; sampleErr != nil {
				result = finishClusterFailure(result, fmt.Errorf("stats sampler %s failed: %w", s.container, sampleErr))
			}
			if sampleErr := appendBrokerStatsSample(shutdownCtx, s.container, s.path); sampleErr != nil {
				result = finishClusterFailure(result, fmt.Errorf("final stats sample %s failed: %w", s.container, sampleErr))
			}
		}
	}
	// Ensure samplers are stopped on every post-startup return path.
	samplersStopped := false
	defer func() {
		if !samplersStopped {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			stopSamplers(shutdownCtx)
		}
	}()

	runnerName := prefix + "-runner"
	containers = append(containers, runnerName)
	runCtx, cancelRun := context.WithTimeout(ctx, options.Timeout)
	runnerArgs := []string{
		"run", "--rm", "--name", runnerName, "--network", prefix,
		"--cpus", formatCPU(options.GeneratorCPUs),
		"--memory", strconv.FormatInt(options.GeneratorMemoryBytes, 10) + "b",
		"--read-only", "--tmpfs", "/tmp:size=64m",
		"--mount", "type=bind,source=" + options.ProfilePath + ",destination=/input/profile.json,readonly",
		"--mount", "type=bind,source=" + options.RunnerConfigDirectory + ",destination=/runner-config,readonly",
		"--mount", "type=bind,source=" + options.ArtifactDirectory + ",destination=/output",
		options.RunnerReference,
	}
	runnerArgs = append(runnerArgs, options.RunnerArgs...)
	runnerOutput, runnerErr := commandOutput(runCtx, "", "docker", runnerArgs...)
	cancelRun()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	stopSamplers(shutdownCtx)
	samplersStopped = true
	if err := os.WriteFile(result.RunnerLogPath, runnerOutput, 0o644); err != nil {
		result = finishClusterFailure(result, fmt.Errorf("write runner log: %w", err))
	}
	if runnerErr != nil {
		result = finishClusterFailure(result, fmt.Errorf("runner failed: %w", runnerErr))
	}
	for _, node := range result.Nodes {
		if _, err := commandOutput(shutdownCtx, "", "docker", "stop", "--time", "10", node.BrokerContainer); err != nil {
			result = finishClusterFailure(result, fmt.Errorf("stop broker %s: %w", node.Name, err))
		}
	}
	for _, node := range result.Nodes {
		if _, err := commandOutput(shutdownCtx, "", "docker", "stop", "--time", "10", node.NetworkOwnerContainer); err != nil {
			result = finishClusterFailure(result, fmt.Errorf("stop network owner %s: %w", node.Name, err))
		}
	}
	for _, node := range result.Nodes {
		data, err := commandOutput(shutdownCtx, "", "docker", "inspect", node.BrokerContainer)
		if err != nil {
			result = finishClusterFailure(result, fmt.Errorf("inspect broker %s: %w", node.Name, err))
		} else if redacted, redactErr := redactInspectEnvSecrets(data); redactErr != nil {
			result = finishClusterFailure(result, fmt.Errorf("redact broker inspect %s: %w", node.Name, redactErr))
		} else if err := os.WriteFile(node.ContainerInspectPath, redacted, 0o644); err != nil {
			result = finishClusterFailure(result, fmt.Errorf("write broker inspect %s: %w", node.Name, err))
		}
		if data, err := commandOutput(shutdownCtx, "", "docker", "logs", node.BrokerContainer); err != nil {
			result = finishClusterFailure(result, fmt.Errorf("collect broker logs %s: %w", node.Name, err))
		} else if err := os.WriteFile(node.BrokerLogPath, data, 0o644); err != nil {
			result = finishClusterFailure(result, fmt.Errorf("write broker logs %s: %w", node.Name, err))
		}
	}
	if result.FailureReason == "" {
		result.Status, result.Success = "success", true
	}
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return result, nil
}

func validateClusterTargetOptions(o ClusterTargetOptions) error {
	required := []struct {
		label string
		value string
	}{
		{label: "benchmark ID", value: o.BenchmarkID},
		{label: "run ID", value: o.RunID},
		{label: "artifact directory", value: o.ArtifactDirectory},
		{label: "profile path", value: o.ProfilePath},
		{label: "broker image reference", value: o.ImageReference},
		{label: "runner image reference", value: o.RunnerReference},
		{label: "runner config directory", value: o.RunnerConfigDirectory},
		{label: "network owner public config directory", value: o.NetworkOwnerConfigDirectory},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s is required", field.label)
		}
	}
	if o.Target != "narumq" && o.Target != "emqx" {
		return fmt.Errorf("unsupported benchmark target %q", o.Target)
	}
	if o.RunnerTarget != "bp03-runner" {
		return fmt.Errorf("unsupported runner target %q", o.RunnerTarget)
	}
	if len(o.Nodes) != 3 {
		return errors.New("exactly three cluster nodes are required")
	}
	names, dns := map[string]bool{}, map[string]bool{}
	for _, n := range o.Nodes {
		if strings.TrimSpace(n.Name) == "" || strings.TrimSpace(n.DNSName) == "" {
			return errors.New("cluster node name and DNS name are required")
		}
		if names[n.Name] {
			return fmt.Errorf("duplicate cluster node name %q", n.Name)
		}
		names[n.Name] = true
		if dns[n.DNSName] {
			return fmt.Errorf("duplicate cluster node DNS name %q", n.DNSName)
		}
		dns[n.DNSName] = true
		if strings.TrimSpace(n.ConfigDirectory) == "" || strings.TrimSpace(n.DataDestination) == "" {
			return fmt.Errorf("node %s config directory and data destination are required", n.Name)
		}
	}
	if o.BrokerCPUs <= 0 || math.IsNaN(o.BrokerCPUs) || math.IsInf(o.BrokerCPUs, 0) ||
		o.GeneratorCPUs <= 0 || math.IsNaN(o.GeneratorCPUs) || math.IsInf(o.GeneratorCPUs, 0) {
		return errors.New("broker and generator CPU limits must be positive finite values")
	}
	if o.BrokerMemoryBytes <= 0 || o.GeneratorMemoryBytes <= 0 {
		return errors.New("broker and generator memory limits must be positive")
	}
	if len(o.NetworkOwnerArgs) == 0 || len(o.RunnerArgs) == 0 {
		return errors.New("runner and network owner arguments are required")
	}
	if o.Timeout <= 0 || o.StartupTimeout <= 0 {
		return errors.New("target and startup timeouts must be greater than zero")
	}
	return nil
}

func buildClusterNetworkOwnerArgs(options ClusterTargetOptions, node ClusterNodeSpec, owner string) []string {
	prefix := options.BenchmarkID + "-" + options.RunID + "-" + options.Target
	args := []string{
		"run", "--detach", "--name", owner,
		"--network", prefix, "--network-alias", node.DNSName, "--hostname", node.DNSName,
		"--cpus", "0.25", "--memory", "67108864b", "--read-only", "--tmpfs", "/tmp:size=16m",
		"--mount", "type=bind,source=" + options.NetworkOwnerConfigDirectory + ",destination=/config,readonly",
		options.RunnerReference,
	}
	return append(args, options.NetworkOwnerArgs...)
}

func clusterNodeVolumes(prefix string, node ClusterNodeSpec) []string {
	volumes := []string{prefix + "-" + node.Name + "-data"}
	if node.LogDestination != "" {
		volumes = append(volumes, prefix+"-"+node.Name+"-log")
	}
	return volumes
}

func buildClusterBrokerArgs(options ClusterTargetOptions, node ClusterNodeSpec, owner string) []string {
	name := options.BenchmarkID + "-" + options.RunID + "-" + options.Target + "-" + node.Name
	args := []string{
		"run", "--detach", "--name", name, "--network", "container:" + owner,
		"--cpus", formatCPU(options.BrokerCPUs),
		"--memory", strconv.FormatInt(options.BrokerMemoryBytes, 10) + "b",
		"--read-only", "--tmpfs", "/tmp:size=64m",
		"--mount", "type=volume,source=" + name + "-data,destination=" + node.DataDestination,
	}
	if node.LogDestination != "" {
		args = append(args, "--mount", "type=volume,source="+name+"-log,destination="+node.LogDestination)
	}
	if node.MountConfig {
		args = append(args, "--mount", "type=bind,source="+node.ConfigDirectory+",destination=/config,readonly")
	}
	for _, env := range node.BrokerEnv {
		args = append(args, "--env", env)
	}
	args = append(args, options.ImageReference)
	args = append(args, node.BrokerArgs...)
	return args
}

func finishClusterFailure(result ClusterTargetResult, err error) ClusterTargetResult {
	if err != nil {
		if result.FailureReason == "" {
			result.FailureReason = err.Error()
		} else if result.FailureReason != err.Error() {
			result.FailureReason = errors.Join(errors.New(result.FailureReason), err).Error()
		}
	}
	result.Status, result.Success = "failed", false
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return result
}

func redactInspectEnvSecrets(data []byte) ([]byte, error) {
	var containers []map[string]json.RawMessage
	if err := json.Unmarshal(data, &containers); err != nil {
		return nil, fmt.Errorf("decode Docker inspect JSON: %w", err)
	}
	for _, container := range containers {
		configJSON, ok := container["Config"]
		if !ok {
			continue
		}
		var config map[string]json.RawMessage
		if err := json.Unmarshal(configJSON, &config); err != nil {
			return nil, fmt.Errorf("decode Docker inspect config: %w", err)
		}
		envJSON, ok := config["Env"]
		if !ok {
			continue
		}
		var environment []string
		if err := json.Unmarshal(envJSON, &environment); err != nil {
			return nil, fmt.Errorf("decode Docker inspect environment: %w", err)
		}
		for index, entry := range environment {
			name, _, hasValue := strings.Cut(entry, "=")
			if hasValue && isSecretEnvironmentName(name) {
				environment[index] = name + "=REDACTED"
			}
		}
		redactedEnv, err := json.Marshal(environment)
		if err != nil {
			return nil, fmt.Errorf("encode redacted Docker inspect environment: %w", err)
		}
		config["Env"] = redactedEnv
		redactedConfig, err := json.Marshal(config)
		if err != nil {
			return nil, fmt.Errorf("encode redacted Docker inspect config: %w", err)
		}
		container["Config"] = redactedConfig
	}
	redacted, err := json.MarshalIndent(containers, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode redacted Docker inspect JSON: %w", err)
	}
	return redacted, nil
}

func isSecretEnvironmentName(name string) bool {
	upper := strings.ToUpper(name)
	for _, marker := range []string{"TOKEN", "PASSWORD", "SECRET", "COOKIE", "PRIVATE_KEY"} {
		if strings.Contains(upper, marker) {
			return true
		}
	}
	return false
}
