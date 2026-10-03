package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	overallRunTimeout  = 60 * time.Minute
	scenarioRunTimeout = 8 * time.Minute
	chaosDockerfile    = "tests/chaos/cluster-storage-v1/Dockerfile"
	chaosControlURL    = "http://127.0.0.1:18080"
)

type commandRunner interface {
	Run(ctx context.Context, workingDirectory, name string, args ...string) ([]byte, error)
}

type realCommandRunner struct{}

func (realCommandRunner) Run(ctx context.Context, workingDirectory, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = workingDirectory
	return command.CombinedOutput()
}

func Run(ctx context.Context, options runOptions) (Summary, error) {
	profile, err := storageProfile(options.Profile)
	if err != nil {
		return Summary{}, err
	}
	options.Profile = profile
	summary := Summary{RunID: options.RunID, Profile: profile, Boundary: profileBoundary(profile), Status: "failed", StartedAt: time.Now().UTC()}
	if err := validateRunID(options.RunID); err != nil {
		return summary, err
	}
	if err := validateOutputDirectory(options.OutputDir); err != nil {
		return summary, err
	}
	outputDir, err := filepath.Abs(options.OutputDir)
	if err != nil {
		return summary, fmt.Errorf("resolve output directory: %w", err)
	}
	if options.RepoRoot == "" {
		return summary, errors.New("repository root is required")
	}
	if options.Runner == nil {
		options.Runner = realCommandRunner{}
	}
	repoRoot, err := filepath.Abs(options.RepoRoot)
	if err != nil {
		return summary, fmt.Errorf("resolve repository root: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, overallRunTimeout)
	defer cancel()

	buildLogPath := filepath.Join(outputDir, "docker-build.log")
	if err := os.WriteFile(buildLogPath, nil, 0o600); err != nil {
		return summary, fmt.Errorf("create Docker build log: %w", err)
	}
	manifestValue := manifest{
		SchemaVersion: 2, RunID: options.RunID, Status: "running", StartedAt: summary.StartedAt,
		Profile: profile, Boundary: profileBoundary(profile),
		Platform: platformMetadata{HostOS: runtime.GOOS, HostArch: runtime.GOARCH},
		Command:  append([]string(nil), os.Args...),
	}
	if err := writeJSONFile(filepath.Join(outputDir, "manifest.json"), manifestValue); err != nil {
		return summary, fmt.Errorf("write initial manifest: %w", err)
	}

	finish := func(runErr error) (Summary, error) {
		summary.FinishedAt = time.Now().UTC()
		if runErr == nil {
			summary.Status = "passed"
			manifestValue.Status = "passed"
		} else {
			summary.Status = "failed"
			summary.Error = runErr.Error()
			manifestValue.Status = "failed"
			manifestValue.Error = runErr.Error()
		}
		manifestValue.FinishedAt = summary.FinishedAt
		if err := writeJSONFile(filepath.Join(outputDir, "summary.json"), summary); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("write summary: %w", err))
			manifestValue.Status = "failed"
			manifestValue.Error = runErr.Error()
		}
		if err := writeJSONFile(filepath.Join(outputDir, "manifest.json"), manifestValue); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("write manifest: %w", err))
		}
		if runErr != nil {
			summary.Status = "failed"
			summary.Error = runErr.Error()
		}
		return summary, runErr
	}

	if err := validateDockerPlatform(ctx, options.Runner, repoRoot, &manifestValue.Platform); err != nil {
		return finish(err)
	}
	if err := writeJSONFile(filepath.Join(outputDir, "manifest.json"), manifestValue); err != nil {
		return finish(fmt.Errorf("write platform manifest: %w", err))
	}

	imageRef := chaosImageName + ":" + chaosImageTag
	imageTarget := "chaos-node"
	if profile == localExt4Profile {
		imageRef += "-ext4"
		imageTarget = "ext4-node"
	}
	buildOutput, buildErr := options.Runner.Run(ctx, repoRoot, "docker", "build", "--platform", "linux/amd64", "--target", imageTarget, "--tag", imageRef, "--file", chaosDockerfile, ".")
	if err := os.WriteFile(buildLogPath, buildOutput, 0o600); err != nil {
		return finish(fmt.Errorf("write Docker build log: %w", err))
	}
	if buildErr != nil {
		return finish(fmt.Errorf("build chaos node image: %w", buildErr))
	}
	image, inspectOutput, inspectErr := inspectChaosImage(ctx, options.Runner, repoRoot, imageRef)
	if err := os.WriteFile(filepath.Join(outputDir, "image-inspect.json"), inspectOutput, 0o600); err != nil {
		return finish(fmt.Errorf("write image inspection: %w", err))
	}
	if inspectErr != nil {
		return finish(inspectErr)
	}
	if image.OS != "linux" || normalizeArchitecture(image.Architecture) != "amd64" {
		return finish(fmt.Errorf("chaos image must be linux/amd64, got %s/%s", image.OS, image.Architecture))
	}
	manifestValue.Platform.ImageOS = image.OS
	manifestValue.Platform.ImageArch = image.Architecture
	manifestValue.Images = map[string]imageMetadata{imageRef: image}
	if err := writeJSONFile(filepath.Join(outputDir, "manifest.json"), manifestValue); err != nil {
		return finish(fmt.Errorf("write image manifest: %w", err))
	}

	var scenarioErrors []error
	for _, scenarioID := range profileScenarios(profile) {
		scenarioCtx, scenarioCancel := context.WithTimeout(ctx, scenarioRunTimeout)
		result := runStorageScenario(scenarioCtx, scenarioRunOptions{
			RunID: options.RunID, ScenarioID: scenarioID, RepoRoot: repoRoot,
			Profile:   profile,
			OutputDir: outputDir, Image: image.ID, Runner: options.Runner,
		})
		scenarioCancel()
		summary.Scenarios = append(summary.Scenarios, result)
		if result.Status != "passed" {
			scenarioErrors = append(scenarioErrors, fmt.Errorf("%s: %s", scenarioID, result.Error))
		}
		if err := writeJSONFile(filepath.Join(outputDir, "summary.json"), summary); err != nil {
			scenarioErrors = append(scenarioErrors, fmt.Errorf("write summary after %s: %w", scenarioID, err))
			break
		}
	}
	return finish(errors.Join(scenarioErrors...))
}

func validateRunID(runID string) error {
	if runID == "" || len(runID) > 32 || runID == "." || runID == ".." {
		return errors.New("run ID must be a path-safe identifier of at most 32 characters")
	}
	for _, character := range runID {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' {
			continue
		}
		return errors.New("run ID may contain only letters, digits, hyphens, and underscores")
	}
	return nil
}

func validateOutputDirectory(path string) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("output directory is required")
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(path, 0o755)
	}
	if err != nil {
		return fmt.Errorf("stat output directory: %w", err)
	}
	if !info.IsDir() {
		return errors.New("output path is not a directory")
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read output directory: %w", err)
	}
	if len(entries) != 0 {
		return errors.New("output directory must be empty")
	}
	return nil
}

func validateDockerPlatform(ctx context.Context, runner commandRunner, repoRoot string, platform *platformMetadata) error {
	osOutput, err := runner.Run(ctx, repoRoot, "docker", "info", "--format", "{{.OSType}}")
	if err != nil {
		return fmt.Errorf("query Docker operating system: %w", err)
	}
	architectureOutput, err := runner.Run(ctx, repoRoot, "docker", "info", "--format", "{{.Architecture}}")
	if err != nil {
		return fmt.Errorf("query Docker architecture: %w", err)
	}
	platform.DockerOS = strings.TrimSpace(string(osOutput))
	platform.DockerArch = strings.TrimSpace(string(architectureOutput))
	if platform.DockerOS != "linux" || normalizeArchitecture(platform.DockerArch) != "amd64" {
		return fmt.Errorf("Docker engine must be linux/amd64, got %s/%s", platform.DockerOS, platform.DockerArch)
	}
	return nil
}

func normalizeArchitecture(architecture string) string {
	switch architecture {
	case "x86_64":
		return "amd64"
	case "aarch64":
		return "arm64"
	default:
		return architecture
	}
}

func inspectChaosImage(ctx context.Context, runner commandRunner, repoRoot, imageRef string) (imageMetadata, []byte, error) {
	output, err := runner.Run(ctx, repoRoot, "docker", "image", "inspect", "--format", "{{json .}}", imageRef)
	if err != nil {
		return imageMetadata{}, output, fmt.Errorf("inspect chaos node image: %w", err)
	}
	var inspection struct {
		ID           string   `json:"Id"`
		OS           string   `json:"Os"`
		Architecture string   `json:"Architecture"`
		RepoDigests  []string `json:"RepoDigests"`
	}
	if err := json.Unmarshal(output, &inspection); err != nil {
		return imageMetadata{}, output, fmt.Errorf("decode chaos image inspection: %w", err)
	}
	if inspection.ID == "" || inspection.OS == "" || inspection.Architecture == "" {
		return imageMetadata{}, output, errors.New("Docker image inspection omitted ID, OS, or architecture")
	}
	return imageMetadata{
		ID: inspection.ID, OS: inspection.OS, Architecture: inspection.Architecture,
		RepoDigests: inspection.RepoDigests,
	}, output, nil
}

func writeJSONFile(path string, value any) error {
	contents, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode JSON: %w", err)
	}
	contents = append(contents, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), ".chaos-evidence-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

type scenarioRunOptions struct {
	RunID      string
	Profile    string
	ScenarioID string
	RepoRoot   string
	OutputDir  string
	Image      string
	Runner     commandRunner
}

type scenarioDocker struct {
	options         scenarioRunOptions
	networkName     string
	networkCreated  bool
	containerNames  map[string]string
	volumeNames     map[string]string
	managementPorts map[string]string
	mqttPorts       map[string]string
	recorder        *eventRecorder
}

func newScenarioDocker(options scenarioRunOptions, recorder *eventRecorder) *scenarioDocker {
	prefix := fmt.Sprintf("narumq-chaos-%s-%s", options.RunID, options.ScenarioID)
	return &scenarioDocker{
		options: options, networkName: prefix,
		containerNames:  make(map[string]string, chaosNodeCount),
		volumeNames:     make(map[string]string, chaosNodeCount),
		managementPorts: make(map[string]string, chaosNodeCount),
		mqttPorts:       make(map[string]string, chaosNodeCount), recorder: recorder,
	}
}

func (scenario *scenarioDocker) createNetwork(ctx context.Context) error {
	output, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "network", "create", scenario.networkName)
	if err != nil {
		recordErr := scenario.recorder.record("control", "", map[string]any{
			"operation": "network-create", "network": scenario.networkName,
			"output": strings.TrimSpace(string(output)), "error": err.Error(),
		})
		return errors.Join(fmt.Errorf("create Docker network: %w", err), recordErr)
	}
	scenario.networkCreated = true
	return scenario.recorder.record("control", "", map[string]any{
		"operation": "network-create", "network": scenario.networkName,
		"output": strings.TrimSpace(string(output)), "error": "",
	})
}

func (scenario *scenarioDocker) startNodes(ctx context.Context, fixture *chaosFixture) error {
	for _, node := range fixture.Nodes {
		name := fmt.Sprintf("narumq-chaos-%s-%s-node%s", scenario.options.RunID, scenario.options.ScenarioID, node.ID)
		args := []string{
			"run", "--detach", "--name", name, "--hostname", node.DNSName,
			"--network", scenario.networkName, "--network-alias", node.DNSName,
			"--cpus", "1", "--memory", "512m", "--read-only",
			"--tmpfs", "/tmp:size=16m", "--tmpfs", "/run/narumq-chaos:size=1m",
			"--mount", fmt.Sprintf("type=bind,source=%s,destination=/config,readonly", filepath.ToSlash(node.ConfigDir)),
			"--publish", "127.0.0.1::8080", "--publish", "127.0.0.1::1883",
		}
		if scenario.options.Profile == localExt4Profile {
			volume, err := scenario.createExt4Volume(ctx, node.ID)
			if err != nil {
				return err
			}
			args = append(args, "--privileged", "--mount", "type=volume,source="+volume+",destination=/images")
		} else if usesChaosFS(scenario.options.ScenarioID) {
			args = append(args,
				"--tmpfs", "/var/lib/narumq-backing:size=256m",
				"--tmpfs", "/var/lib/narumq:size=1m",
				"--device", "/dev/fuse:/dev/fuse:rwm",
				"--cap-add", "SYS_ADMIN", "--security-opt", "apparmor=unconfined",
			)
		} else {
			args = append(args, "--tmpfs", "/var/lib/narumq:size=64m")
		}
		args = append(args, scenario.options.Image,
			"supervise", "--broker", "/narumq", "--broker-config", node.ConfigPath,
			"--data-dir", "/var/lib/narumq/node", "--jwks-file", "/config/jwks.json",
			"--jwks-listen", "127.0.0.1:18080", "--delay-file", "/run/narumq-chaos/io-delay",
		)
		if scenario.options.Profile != localExt4Profile && usesChaosFS(scenario.options.ScenarioID) {
			args = append(args, "--chaosfs-backing", "/var/lib/narumq-backing", "--chaosfs-mount", "/var/lib/narumq")
		}
		output, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", args...)
		if err != nil {
			recordErr := scenario.recorder.record("control", node.ID, map[string]any{
				"operation": "container-start", "name": name,
				"output": strings.TrimSpace(string(output)), "error": err.Error(),
			})
			return errors.Join(fmt.Errorf("start node %s container: %w", node.ID, err), recordErr)
		}
		containerID := strings.TrimSpace(string(output))
		if containerID == "" {
			return fmt.Errorf("start node %s container returned an empty ID", node.ID)
		}
		scenario.containerNames[node.ID] = name
		if err := scenario.recorder.record("control", node.ID, map[string]any{"operation": "container-start", "name": name, "containerId": containerID}); err != nil {
			return err
		}
		for _, port := range []struct {
			containerPort string
			mapped        map[string]string
		}{
			{containerPort: "8080/tcp", mapped: scenario.managementPorts},
			{containerPort: "1883/tcp", mapped: scenario.mqttPorts},
		} {
			portOutput, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "port", name, port.containerPort)
			if err != nil {
				recordErr := scenario.recorder.record("control", node.ID, map[string]any{
					"operation": "port-map", "containerPort": port.containerPort,
					"output": strings.TrimSpace(string(portOutput)), "error": err.Error(),
				})
				return errors.Join(fmt.Errorf("query node %s host port %s: %w", node.ID, port.containerPort, err), recordErr)
			}
			portValue, err := parseManagementPort(portOutput)
			if err != nil {
				recordErr := scenario.recorder.record("control", node.ID, map[string]any{
					"operation": "port-map", "containerPort": port.containerPort,
					"output": strings.TrimSpace(string(portOutput)), "error": err.Error(),
				})
				return errors.Join(fmt.Errorf("parse node %s host port: %w", node.ID, err), recordErr)
			}
			port.mapped[node.ID] = portValue
			if err := scenario.recorder.record("control", node.ID, map[string]any{
				"operation": "port-map", "containerPort": port.containerPort, "hostPort": portValue, "error": "",
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func usesChaosFS(scenarioID string) bool {
	switch scenarioID {
	case "follower-slow-io", "follower-sync-error", "follower-torn-power-loss", "cluster-power-loss":
		return true
	default:
		return false
	}
}

func (scenario *scenarioDocker) execNode(ctx context.Context, nodeID string, command string, args ...string) ([]byte, error) {
	name, ok := scenario.containerNames[nodeID]
	if !ok {
		return nil, fmt.Errorf("container for node %s is unavailable", nodeID)
	}
	commandArgs := append([]string{"exec", name, "/narumq-chaosnode", command}, args...)
	output, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", commandArgs...)
	if recordErr := scenario.recorder.record("control", nodeID, map[string]any{
		"operation": command, "args": args, "output": strings.TrimSpace(string(output)), "error": errorString(err),
	}); recordErr != nil {
		return output, errors.Join(err, recordErr)
	}
	if err != nil {
		return output, fmt.Errorf("node %s %s: %w", nodeID, command, err)
	}
	return output, nil
}
func (scenario *scenarioDocker) chaosControl(ctx context.Context, nodeID, command string, args ...string) ([]byte, error) {
	commandArgs := append([]string{"--control", chaosControlURL}, args...)
	return scenario.execNode(ctx, nodeID, command, commandArgs...)
}

func (scenario *scenarioDocker) signalBrokerRestart(ctx context.Context, nodeID string) error {
	name, ok := scenario.containerNames[nodeID]
	if !ok {
		return fmt.Errorf("container for node %s is unavailable", nodeID)
	}
	output, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "kill", "--signal", "SIGUSR1", name)
	if recordErr := scenario.recorder.record("control", nodeID, map[string]any{
		"operation": "broker-restart-signal", "output": strings.TrimSpace(string(output)), "error": errorString(err),
	}); err != nil {
		return errors.Join(err, recordErr)
	}
	if err != nil {
		return fmt.Errorf("signal broker restart on node %s: %w", nodeID, err)
	}
	return nil
}

func (scenario *scenarioDocker) containerRunning(ctx context.Context, nodeID string) (bool, error) {
	name, ok := scenario.containerNames[nodeID]
	if !ok {
		return false, fmt.Errorf("container for node %s is unavailable", nodeID)
	}
	output, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "inspect", "--format", "{{.State.Running}}", name)
	if err != nil {
		recordErr := scenario.recorder.record("control", nodeID, map[string]any{
			"operation": "container-running", "error": err.Error(),
		})
		return false, errors.Join(fmt.Errorf("inspect node %s container state: %w", nodeID, err), recordErr)
	}
	running := strings.TrimSpace(string(output)) == "true"
	if err := scenario.recorder.record("control", nodeID, map[string]any{"operation": "container-running", "running": running, "error": ""}); err != nil {
		return false, err
	}
	return running, nil
}

func (scenario *scenarioDocker) cleanup(ctx context.Context) error {
	var cleanupErrors []error
	for _, nodeID := range []string{"1", "2", "3"} {
		name, ok := scenario.containerNames[nodeID]
		if !ok {
			continue
		}
		output, stopErr := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "stop", "--timeout", "20", name)
		if recordErr := scenario.recorder.record("control", nodeID, map[string]any{
			"operation": "container-stop", "output": strings.TrimSpace(string(output)), "error": errorString(stopErr),
		}); recordErr != nil {
			cleanupErrors = append(cleanupErrors, recordErr)
		}
		if stopErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("stop node %s container: %w (%s)", nodeID, stopErr, strings.TrimSpace(string(output))))
		}

		logs, logsErr := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "logs", name)
		logPath := filepath.Join(scenario.options.OutputDir, "scenarios", scenario.options.ScenarioID, "node"+nodeID+".log")
		if writeErr := os.WriteFile(logPath, logs, 0o600); writeErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("write node %s logs: %w", nodeID, writeErr))
		}
		if logsErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("collect node %s logs: %w", nodeID, logsErr))
		}
		if scenario.options.Profile == localExt4Profile {
			if err := validateExt4Cleanup(logs); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("node %s ext4 cleanup: %w", nodeID, err))
			}
		}
		if recordErr := scenario.recorder.record("control", nodeID, map[string]any{
			"operation": "logs-collected", "bytes": len(logs), "error": errorString(logsErr),
		}); recordErr != nil {
			cleanupErrors = append(cleanupErrors, recordErr)
		}
		output, removeErr := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "rm", name)
		if recordErr := scenario.recorder.record("control", nodeID, map[string]any{
			"operation": "container-remove", "output": strings.TrimSpace(string(output)), "error": errorString(removeErr),
		}); recordErr != nil {
			cleanupErrors = append(cleanupErrors, recordErr)
		}
		if removeErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove node %s container: %w (%s)", nodeID, removeErr, strings.TrimSpace(string(output))))
		}
	}
	if scenario.networkCreated {
		output, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "network", "rm", scenario.networkName)
		if recordErr := scenario.recorder.record("control", "", map[string]any{
			"operation": "network-remove", "network": scenario.networkName,
			"output": strings.TrimSpace(string(output)), "error": errorString(err),
		}); recordErr != nil {
			cleanupErrors = append(cleanupErrors, recordErr)
		}
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove Docker network: %w (%s)", err, strings.TrimSpace(string(output))))
		}
	}
	if err := scenario.removeExt4Volumes(ctx); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}
	return errors.Join(cleanupErrors...)
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
