package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	brokerImageTarget = "narumq"
	goImageTarget     = "go-client"
	pythonImageTarget = "python-client"

	crossReadinessSchemaVersion = 1
)

type crossReadiness struct {
	SchemaVersion int    `json:"schemaVersion"`
	CaseID        string `json:"caseId"`
	SDK           string `json:"sdk"`
	Role          string `json:"role"`
	Protocol      string `json:"protocol"`
	Ready         bool   `json:"ready"`
}

func readCrossReadiness(path string, expected ExpectedCase, subscriber ExpectedResult) (crossReadiness, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return crossReadiness{}, fmt.Errorf("read subscriber readiness: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var readiness crossReadiness
	if err := decoder.Decode(&readiness); err != nil {
		return crossReadiness{}, fmt.Errorf("decode subscriber readiness: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return crossReadiness{}, errors.New("subscriber readiness contains a second JSON value")
		}
		return crossReadiness{}, fmt.Errorf("decode trailing subscriber readiness: %w", err)
	}
	if readiness.SchemaVersion != crossReadinessSchemaVersion ||
		readiness.CaseID != expected.CaseID ||
		readiness.SDK != subscriber.SDK ||
		readiness.Role != "subscriber" ||
		readiness.Protocol != expected.Protocol ||
		!readiness.Ready {
		return crossReadiness{}, fmt.Errorf("subscriber readiness does not match case %q", expected.CaseID)
	}
	return readiness, nil
}

// commandRunner is the Docker process boundary used by the orchestrator.
type commandRunner interface {
	Run(ctx context.Context, workingDirectory, name string, args ...string) ([]byte, error)
}

type realCommandRunner struct{}

func (realCommandRunner) Run(ctx context.Context, workingDirectory, name string, args ...string) ([]byte, error) {
	command := commandContext(ctx, workingDirectory, name, args...)
	return command.CombinedOutput()
}

// commandContext is isolated to keep command execution replaceable in tests.
func commandContext(ctx context.Context, workingDirectory, name string, args ...string) *execCmd {
	return newExecCmd(ctx, workingDirectory, name, args...)
}

// execCmd is a small adapter around os/exec kept behind commandRunner.
type execCmd struct {
	ctx              context.Context
	workingDirectory string
	name             string
	args             []string
}

func newExecCmd(ctx context.Context, workingDirectory, name string, args ...string) *execCmd {
	return &execCmd{ctx: ctx, workingDirectory: workingDirectory, name: name, args: args}
}

func (c *execCmd) CombinedOutput() ([]byte, error) {
	cmd := execCommandContext(c.ctx, c.name, c.args...)
	if c.workingDirectory != "" {
		cmd.Dir = c.workingDirectory
	}
	return cmd.CombinedOutput()
}

// runOptions controls one compatibility evidence run.
type runOptions struct {
	RunID       string
	OutputDir   string
	ProfilePath string
	RepoRoot    string
	Runner      commandRunner
	Now         func() time.Time
}

type imageMetadata struct {
	ID           string   `json:"id"`
	RepoDigests  []string `json:"repoDigests"`
	Architecture string   `json:"architecture"`
	OS           string   `json:"os"`
}

type runManifest struct {
	SchemaVersion int                      `json:"schemaVersion"`
	RunID         string                   `json:"runId"`
	Status        string                   `json:"status"`
	StartedAt     string                   `json:"startedAt"`
	FinishedAt    string                   `json:"finishedAt,omitempty"`
	Platform      platformManifest         `json:"platform"`
	Profile       any                      `json:"profile"`
	Images        map[string]imageMetadata `json:"images,omitempty"`
	Command       []string                 `json:"command,omitempty"`
	Error         string                   `json:"error,omitempty"`
}

type platformManifest struct {
	HostOS     string `json:"hostOS"`
	HostArch   string `json:"hostArch"`
	DockerOS   string `json:"dockerOS,omitempty"`
	DockerArch string `json:"dockerArch,omitempty"`
}

type caseEvent struct {
	CaseID     string `json:"caseId"`
	Status     string `json:"status"`
	StartedAt  string `json:"startedAt"`
	FinishedAt string `json:"finishedAt"`
	Error      string `json:"error,omitempty"`
}

type compatibilityRun struct {
	options       runOptions
	profile       Profile
	outputDir     string
	images        map[string]string
	imageMetadata map[string]imageMetadata
	containerMu   sync.Mutex
	containers    []string
	networks      []string
	firstFailure  error
	caseFailures  map[string]error
	results       map[string][]Result
	unexpected    []string
	startedAt     time.Time
}

// Run executes one complete compatibility evidence run and writes its summary.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-042
func Run(ctx context.Context, options runOptions) (Summary, error) {
	profile, _, err := LoadProfile(options.ProfilePath)
	if err != nil {
		return Summary{}, err
	}
	ids := make([]string, 0, len(profile.ExpectedCases))
	for _, expected := range profile.ExpectedCases {
		ids = append(ids, expected.CaseID)
	}
	return runSuite(ctx, options, suiteSpec{
		profile: profile, caseIDs: ids,
		runCase: func(ctx context.Context, run *compatibilityRun, caseID string) error {
			for _, expected := range profile.ExpectedCases {
				if expected.CaseID == caseID {
					return run.runCase(ctx, expected)
				}
			}
			return fmt.Errorf("unknown case %q", caseID)
		},
		summarize: func(run *compatibilityRun, startedAt, finishedAt string) Summary {
			return AggregateSummary(options.RunID, profile, run.results, run.unexpected, startedAt, finishedAt)
		},
	})
}

type suiteSpec struct {
	profile   any
	caseIDs   []string
	runCase   func(context.Context, *compatibilityRun, string) error
	summarize func(run *compatibilityRun, startedAt, finishedAt string) Summary
}

func runSuite(ctx context.Context, options runOptions, suite suiteSpec) (Summary, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if options.Runner == nil {
		options.Runner = realCommandRunner{}
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if err := validateRunID(options.RunID); err != nil {
		return Summary{}, err
	}
	if err := validateOutputDirectory(options.OutputDir); err != nil {
		return Summary{}, err
	}
	if options.ProfilePath == "" {
		return Summary{}, errors.New("profile path is required")
	}
	if options.RepoRoot == "" {
		options.RepoRoot = findRepoRoot(options.ProfilePath)
	}
	startedAt := options.Now().UTC()
	run := &compatibilityRun{
		options:   options,
		outputDir: options.OutputDir, images: make(map[string]string),
		imageMetadata: make(map[string]imageMetadata), results: make(map[string][]Result), startedAt: startedAt,
	}
	if profile, ok := suite.profile.(Profile); ok {
		run.profile = profile
	}
	manifest := runManifest{
		SchemaVersion: 1, RunID: options.RunID, Status: "running", StartedAt: startedAt.Format(time.RFC3339Nano),
		Platform: platformManifest{HostOS: runtime.GOOS, HostArch: runtime.GOARCH}, Profile: suite.profile,
		Command: append([]string(nil), os.Args...),
	}
	if err := writeJSON(filepath.Join(options.OutputDir, "profile.json"), suite.profile); err != nil {
		return Summary{}, fmt.Errorf("write profile artifact: %w", err)
	}
	if err := writeJSON(filepath.Join(options.OutputDir, "manifest.json"), manifest); err != nil {
		return Summary{}, fmt.Errorf("write manifest artifact: %w", err)
	}
	casesFile, err := os.OpenFile(filepath.Join(options.OutputDir, "cases.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return Summary{}, fmt.Errorf("create cases artifact: %w", err)
	}
	defer casesFile.Close()
	caseEncoder := json.NewEncoder(casesFile)
	defer func() { _ = casesFile.Sync() }()
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		run.cleanup(cleanupCtx)
	}()

	failure := run.prepare(ctx, &manifest)
	run.caseFailures = make(map[string]error, len(suite.caseIDs))
	if failure == nil {
		for _, caseID := range suite.caseIDs {
			caseStart := options.Now().UTC()
			caseErr := suite.runCase(ctx, run, caseID)
			caseStatus := "passed"
			if caseErr != nil {
				caseStatus = "failed"
				run.caseFailures[caseID] = caseErr
				run.rememberFailure(caseErr)
			}
			event := caseEvent{CaseID: caseID, Status: caseStatus, StartedAt: caseStart.Format(time.RFC3339Nano), FinishedAt: options.Now().UTC().Format(time.RFC3339Nano)}
			if caseErr != nil {
				event.Error = caseErr.Error()
			}
			if err := caseEncoder.Encode(event); err != nil {
				run.rememberFailure(fmt.Errorf("write cases artifact: %w", err))
			}
		}
	} else {
		run.rememberFailure(failure)
	}
	_ = casesFile.Sync()
	finishedAt := options.Now().UTC()
	if run.firstFailure != nil {
		manifest.Status = "failed"
		manifest.Error = run.firstFailure.Error()
	} else {
		manifest.Status = "passed"
	}
	manifest.FinishedAt = finishedAt.Format(time.RFC3339Nano)
	manifest.Images = run.imageMetadata
	if err := writeJSON(filepath.Join(options.OutputDir, "manifest.json"), manifest); err != nil {
		run.rememberFailure(fmt.Errorf("write final manifest: %w", err))
	}
	summary := suite.summarize(run, startedAt.Format(time.RFC3339Nano), finishedAt.Format(time.RFC3339Nano))
	for index := range summary.CaseSummaries {
		item := &summary.CaseSummaries[index]
		if failure := run.caseFailures[item.CaseID]; failure != nil {
			if item.Status == "passed" {
				item.Status = "failed"
				summary.PassedCases--
				summary.FailedCases++
			}
			item.Error = failure.Error()
			summary.Status = "failed"
		}
	}
	if run.firstFailure != nil && summary.Error == "" {
		summary.Error = run.firstFailure.Error()
		summary.Status = "failed"
	}
	if err := writeJSON(filepath.Join(options.OutputDir, "summary.json"), summary); err != nil {
		return summary, fmt.Errorf("write summary artifact: %w", err)
	}
	if summary.Status != "passed" {
		if run.firstFailure != nil {
			return summary, run.firstFailure
		}
		return summary, errors.New(summary.Error)
	}
	return summary, run.firstFailure
}

func (run *compatibilityRun) prepare(ctx context.Context, manifest *runManifest) error {
	output, err := run.command(ctx, "docker", "info", "--format", "{{.OSType}}")
	if err != nil {
		return fmt.Errorf("docker is unavailable: %w", err)
	}
	manifest.Platform.DockerOS = strings.TrimSpace(string(output))
	if manifest.Platform.DockerOS != "linux" {
		return fmt.Errorf("docker reports unsupported operating system %q", manifest.Platform.DockerOS)
	}
	output, err = run.command(ctx, "docker", "info", "--format", "{{.Architecture}}")
	if err != nil {
		return fmt.Errorf("inspect docker architecture: %w", err)
	}
	manifest.Platform.DockerArch = strings.TrimSpace(string(output))
	if manifest.Platform.DockerArch != "amd64" && manifest.Platform.DockerArch != "x86_64" {
		return fmt.Errorf("docker reports unsupported architecture %q", manifest.Platform.DockerArch)
	}
	builds := []struct {
		key    string
		target string
		file   string
	}{
		{key: "broker", target: brokerImageTarget, file: filepath.Join("tests", "benchmark", "Dockerfile")},
		{key: "go", target: goImageTarget, file: filepath.Join("tests", "compatibility", "mqtt-clients-v1", "Dockerfile")},
		{key: "python", target: pythonImageTarget, file: filepath.Join("tests", "compatibility", "mqtt-clients-v1", "Dockerfile")},
	}
	for _, build := range builds {
		reference := fmt.Sprintf("narumq-compat-%s-%s:phase1", build.key, run.options.RunID)
		run.images[build.key] = reference
		logPath := filepath.Join(run.outputDir, "docker-build-"+build.key+".log")
		args := []string{"build", "--platform", "linux/amd64", "--target", build.target, "--tag", reference, "--file", filepath.Join(run.options.RepoRoot, build.file), run.options.RepoRoot}
		output, buildErr := run.command(ctx, "docker", args...)
		if writeErr := os.WriteFile(logPath, output, 0o644); writeErr != nil {
			return fmt.Errorf("write %s build log: %w", build.key, writeErr)
		}
		if buildErr != nil {
			return fmt.Errorf("build %s image: %w", build.key, buildErr)
		}
		metadata, inspectErr := run.inspectImage(ctx, reference)
		if inspectErr != nil {
			return fmt.Errorf("inspect %s image: %w", build.key, inspectErr)
		}
		if metadata.OS != "linux" || (metadata.Architecture != "amd64" && metadata.Architecture != "x86_64") {
			return fmt.Errorf("image %s has unsupported platform %s/%s", build.key, metadata.OS, metadata.Architecture)
		}
		run.imageMetadata[build.key] = metadata
		if err := writeJSON(filepath.Join(run.outputDir, build.key+"-image-inspect.json"), metadata); err != nil {
			return err
		}
	}
	return nil
}

func (run *compatibilityRun) inspectImage(ctx context.Context, reference string) (imageMetadata, error) {
	output, err := run.command(ctx, "docker", "image", "inspect", "--format", "{{json .}}", reference)
	if err != nil {
		return imageMetadata{}, err
	}
	var metadata imageMetadata
	if err := json.Unmarshal(bytesTrimSpace(output), &metadata); err != nil {
		return imageMetadata{}, fmt.Errorf("decode image metadata: %w", err)
	}
	if strings.TrimSpace(metadata.ID) == "" {
		return imageMetadata{}, errors.New("image metadata has empty ID")
	}
	return metadata, nil
}

func (run *compatibilityRun) runCase(parent context.Context, expected ExpectedCase) error {
	caseDir := filepath.Join(run.outputDir, "cases", expected.CaseID)
	if !pathWithin(run.outputDir, caseDir) {
		return errors.New("case artifact path escapes run output")
	}
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		return fmt.Errorf("create case directory: %w", err)
	}
	caseCtx, cancel := context.WithTimeout(parent, caseTimeout(run.profile))
	defer cancel()
	brokerName := containerName(run.options.RunID, expected.CaseID, "broker")
	run.registerContainer(brokerName)
	brokerArgs := []string{"run", "--detach", "--name", brokerName, "--cpus", "1", "--memory", "512m", "--read-only", "--tmpfs", "/tmp:size=64m", run.images["broker"], "-tcp-listen", "127.0.0.1:1883", "-allow-insecure-anonymous-mqtt"}
	brokerOutput, err := run.command(caseCtx, "docker", brokerArgs...)
	_ = os.WriteFile(filepath.Join(caseDir, "broker-start.log"), brokerOutput, 0o644)
	if err != nil {
		return fmt.Errorf("start broker for %s: %w", expected.CaseID, err)
	}
	if err := run.waitForBroker(caseCtx, brokerName); err != nil {
		run.collectBrokerArtifacts(context.Background(), brokerName, caseDir)
		return err
	}
	address := "127.0.0.1:1883"
	proxyName := ""
	if expected.Proxy {
		proxyName = containerName(run.options.RunID, expected.CaseID, "proxy")
		run.registerContainer(proxyName)
		proxyDir := filepath.Join(caseDir, "proxy")
		if err := os.MkdirAll(proxyDir, 0o755); err != nil {
			return err
		}
		proxyArgs := []string{"run", "--detach", "--name", proxyName, "--network", "container:" + brokerName, "--cpus", "1", "--memory", "512m", "--read-only", "--tmpfs", "/tmp:size=64m", "--mount", "type=bind,source=" + hostPath(proxyDir) + ",destination=/output", "--entrypoint", "/proxy", run.images["go"], "--listen", "127.0.0.1:1884", "--broker", "127.0.0.1:1883", "--qos", strconv.Itoa(proxyQoS(expected.CaseID)), "--trace", "/output/proxy-trace.jsonl"}
		if output, err := run.command(caseCtx, "docker", proxyArgs...); err != nil {
			_ = os.WriteFile(filepath.Join(caseDir, "proxy-start.log"), output, 0o644)
		}
		address = "127.0.0.1:1884"
	}
	if len(expected.ExpectedResults) == 2 {
		if err := run.runCrossCase(caseCtx, expected, brokerName, proxyName, address, caseDir); err != nil {
			run.collectBrokerArtifacts(context.Background(), brokerName, caseDir)
			return err
		}
	} else {
		role := expected.ExpectedResults[0]
		roleDir := filepath.Join(caseDir, role.Role)
		if err := os.MkdirAll(roleDir, 0o755); err != nil {
			return err
		}
		output, clientErr := run.runClient(caseCtx, expected, role, brokerName, address, roleDir, false, "")
		if err := os.WriteFile(filepath.Join(roleDir, "client-stdout-stderr.log"), output, 0o644); err != nil {
			return err
		}
		if clientErr != nil {
			run.rememberFailure(fmt.Errorf("case %s client: %w", expected.CaseID, clientErr))
		}
	}
	run.collectBrokerArtifacts(context.Background(), brokerName, caseDir)
	if err := run.collectCaseResults(expected, caseDir); err != nil {
		return err
	}
	if expected.Proxy {
		tracePath := filepath.Join(caseDir, "proxy", "proxy-trace.jsonl")
		if _, err := os.Stat(tracePath); err != nil {
			return fmt.Errorf("proxy trace missing: %w", err)
		}
	}
	return nil
}

func (run *compatibilityRun) runCrossCase(ctx context.Context, expected ExpectedCase, brokerName, proxyName, address, caseDir string) error {
	publisher := expected.ExpectedResults[0]
	subscriber := expected.ExpectedResults[1]
	subscriberDir := filepath.Join(caseDir, subscriber.Role)
	publisherDir := filepath.Join(caseDir, publisher.Role)
	if err := os.MkdirAll(subscriberDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(publisherDir, 0o755); err != nil {
		return err
	}
	subscriberName := containerName(run.options.RunID, expected.CaseID, subscriber.Role)
	run.registerContainer(subscriberName)
	subOutput, err := run.runClient(ctx, expected, subscriber, brokerName, address, subscriberDir, true, "")
	if err != nil {
		return fmt.Errorf("start subscriber: %w", err)
	}
	_ = os.WriteFile(filepath.Join(subscriberDir, "client-start.log"), subOutput, 0o644)
	readyPath := filepath.Join(subscriberDir, "ready.json")
	if err := waitForFile(ctx, readyPath); err != nil {
		return fmt.Errorf("wait for subscriber readiness: %w", err)
	}
	readiness, err := readCrossReadiness(readyPath, expected, subscriber)
	if err != nil {
		return err
	}
	readinessPath := filepath.Join(publisherDir, "readiness.json")
	if err := writeJSON(readinessPath, readiness); err != nil {
		return fmt.Errorf("write publisher readiness input: %w", err)
	}
	// The validated readiness input proves the publisher starts after the subscriber.
	pubOutput, pubErr := run.runClient(ctx, expected, publisher, brokerName, address, publisherDir, false, readinessPath)
	_ = os.WriteFile(filepath.Join(publisherDir, "client-stdout-stderr.log"), pubOutput, 0o644)
	if pubErr != nil {
		return fmt.Errorf("run publisher: %w", pubErr)
	}
	waitOutput, waitErr := run.command(ctx, "docker", "wait", subscriberName)
	_ = os.WriteFile(filepath.Join(subscriberDir, "client-wait.log"), waitOutput, 0o644)
	if waitErr != nil {
		return fmt.Errorf("wait subscriber: %w", waitErr)
	}
	logs, logsErr := run.command(context.Background(), "docker", "logs", subscriberName)
	_ = os.WriteFile(filepath.Join(subscriberDir, "client-stdout-stderr.log"), logs, 0o644)
	if logsErr != nil {
		return fmt.Errorf("collect subscriber logs: %w", logsErr)
	}
	return nil
}

func (run *compatibilityRun) runClient(ctx context.Context, expected ExpectedCase, role ExpectedResult, brokerName, address, roleDir string, detached bool, readinessPath string) ([]byte, error) {
	image := run.images[role.SDK]
	if role.SDK != "go" && role.SDK != "python" {
		return nil, fmt.Errorf("unsupported client SDK %q", role.SDK)
	}
	if readinessPath != "" {
		if !pathWithin(roleDir, readinessPath) {
			return nil, errors.New("readiness input escapes publisher output")
		}
		info, err := os.Stat(readinessPath)
		if err != nil {
			return nil, fmt.Errorf("stat readiness input: %w", err)
		}
		if info.IsDir() {
			return nil, errors.New("readiness input is a directory")
		}
	}
	name := containerName(run.options.RunID, expected.CaseID, role.Role)
	if detached {
		run.registerContainer(name)
	}
	args := []string{"run"}
	if detached {
		args = append(args, "--detach")
	} else {
		args = append(args, "--rm")
	}
	args = append(args, "--name", name, "--network", "container:"+brokerName, "--cpus", "1", "--memory", "512m", "--read-only", "--tmpfs", "/tmp:size=64m", "--mount", "type=bind,source="+hostPath(run.options.ProfilePath)+",destination=/input/profile.json,readonly", "--mount", "type=bind,source="+hostPath(roleDir)+",destination=/output")
	if readinessPath != "" {
		args = append(args, "--mount", "type=bind,source="+hostPath(readinessPath)+",destination=/input/readiness.json,readonly")
	}
	if expected.Proxy {
		proxyDir := filepath.Join(filepath.Dir(roleDir), "proxy")
		args = append(args, "--mount", "type=bind,source="+hostPath(proxyDir)+",destination=/proxy-output,readonly")
	}
	args = append(args, image, "--suite", "phase1", "--case", expected.CaseID, "--role", role.Role, "--address", address, "--profile", "/input/profile.json", "--output", "/output")
	return run.command(ctx, "docker", args...)
}

func (run *compatibilityRun) waitForBroker(ctx context.Context, containerName string) error {
	for {
		if _, err := run.command(ctx, "docker", "inspect", "--format", "{{.State.Running}}", containerName); err != nil {
			return fmt.Errorf("inspect broker startup: %w", err)
		}
		logs, err := run.command(ctx, "docker", "logs", containerName)
		if err == nil && strings.Contains(string(logs), "MQTT TCP listener started") {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait broker readiness: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (run *compatibilityRun) collectBrokerArtifacts(ctx context.Context, containerName, caseDir string) {
	inspect, inspectErr := run.command(ctx, "docker", "inspect", containerName)
	if inspectErr == nil {
		_ = os.WriteFile(filepath.Join(caseDir, "broker-inspect.json"), inspect, 0o644)
	}
	logs, logsErr := run.command(ctx, "docker", "logs", containerName)
	if logsErr == nil {
		_ = os.WriteFile(filepath.Join(caseDir, "broker.log"), logs, 0o644)
	}
}

func (run *compatibilityRun) collectCaseResults(expected ExpectedCase, caseDir string) error {
	results := make([]Result, 0, len(expected.ExpectedResults))
	for _, role := range expected.ExpectedResults {
		path := filepath.Join(caseDir, role.Role, "result.json")
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				run.results[expected.CaseID] = results
				return fmt.Errorf("case %s result for role %s is missing", expected.CaseID, role.Role)
			}
			return fmt.Errorf("read case %s result: %w", expected.CaseID, err)
		}
		result, err := DecodeResult(data)
		if err != nil {
			return fmt.Errorf("decode case %s result: %w", expected.CaseID, err)
		}
		results = append(results, result)
	}
	run.results[expected.CaseID] = results
	if err := ValidateCaseResults(expected, results); err != nil {
		return err
	}
	return nil
}

func (run *compatibilityRun) command(ctx context.Context, name string, args ...string) ([]byte, error) {
	output, err := run.options.Runner.Run(ctx, run.options.RepoRoot, name, args...)
	if err != nil {
		return output, fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return output, nil
}

func (run *compatibilityRun) registerContainer(name string) {
	run.containerMu.Lock()
	defer run.containerMu.Unlock()
	for _, existing := range run.containers {
		if existing == name {
			return
		}
	}
	run.containers = append(run.containers, name)
}

func (run *compatibilityRun) registerNetwork(name string) {
	run.containerMu.Lock()
	defer run.containerMu.Unlock()
	run.networks = append(run.networks, name)
}

func (run *compatibilityRun) cleanup(ctx context.Context) {
	run.containerMu.Lock()
	containers := append([]string(nil), run.containers...)
	networks := append([]string(nil), run.networks...)
	run.containerMu.Unlock()
	for index := len(containers) - 1; index >= 0; index-- {
		_, _ = run.options.Runner.Run(ctx, run.options.RepoRoot, "docker", "rm", "--force", containers[index])
	}
	for index := len(networks) - 1; index >= 0; index-- {
		_, _ = run.options.Runner.Run(ctx, run.options.RepoRoot, "docker", "network", "rm", networks[index])
	}
}

func (run *compatibilityRun) rememberFailure(err error) {
	if err == nil || run.firstFailure != nil {
		return
	}
	run.firstFailure = err
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func bytesTrimSpace(value []byte) []byte { return []byte(strings.TrimSpace(string(value))) }

func errorString(err error) struct {
	Error string `json:"error,omitempty"`
} {
	if err == nil {
		return struct {
			Error string `json:"error,omitempty"`
		}{}
	}
	return struct {
		Error string `json:"error,omitempty"`
	}{Error: err.Error()}
}

func containerName(runID, caseID, role string) string {
	return "narumq-compat-" + runID + "-" + caseID + "-" + role
}

func proxyQoS(caseID string) int {
	if strings.Contains(caseID, "qos2") {
		return 2
	}
	return 1
}

func hostPath(path string) string { return filepath.ToSlash(path) }

func waitForFile(ctx context.Context, path string) error {
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func findRepoRoot(profilePath string) string {
	path, err := filepath.Abs(profilePath)
	if err != nil {
		return "."
	}
	root := filepath.Dir(path)
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			return root
		}
		parent := filepath.Dir(root)
		if parent == root {
			return "."
		}
		root = parent
	}
}

// execCommandContext remains replaceable for command-boundary tests.
var execCommandContext = exec.CommandContext
