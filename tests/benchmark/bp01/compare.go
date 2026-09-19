package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	emqxImageReference      = "emqx/emqx:5.8.8@sha256:4593b6e30196754779fa14483332cb0be56c233f1c8a387b1308bec4337c0144"
	emqxVersion             = "5.8.8"
	emqxEdition             = "Open Source Edition"
	emqxLicense             = "Apache-2.0"
	emqxEOLEnd              = "2026-02-28"
	buildBaseImageReference = "golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd"
	plannedBuildBaseImage   = "golang:1.26.5-bookworm@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251"
	startupTimeout          = 30 * time.Second
)

type artifactProfile struct {
	RawSHA256 string  `json:"rawSha256"`
	Resolved  Profile `json:"resolved"`
}

type gitMetadata struct {
	Commit string `json:"commit"`
	Dirty  bool   `json:"dirty"`
}

type dockerMetadata struct {
	ClientServerVersion string `json:"clientServerVersion"`
}

type resourceLimits struct {
	CPUs              float64 `json:"cpus"`
	MemoryBytes       int64   `json:"memoryBytes"`
	ReadOnlyRootFS    bool    `json:"readOnlyRootFilesystem"`
	NetworkMode       string  `json:"networkMode"`
	GeneratorCPUs     float64 `json:"generatorCpus"`
	GeneratorMemory   int64   `json:"generatorMemoryBytes"`
	RunnerReadOnlyFS  bool    `json:"runnerReadOnlyRootFilesystem"`
	RunnerNetworkMode string  `json:"runnerNetworkMode"`
}

type targetManifest struct {
	Target            string         `json:"target"`
	Status            string         `json:"status"`
	Success           bool           `json:"success"`
	StartedAt         string         `json:"startedAt"`
	FinishedAt        string         `json:"finishedAt"`
	ArtifactDirectory string         `json:"artifactDirectory"`
	SummaryPath       string         `json:"summaryPath"`
	SamplesPath       string         `json:"samplesPath"`
	BrokerStatsPath   string         `json:"brokerStatsPath"`
	BrokerLogPath     string         `json:"brokerLogPath"`
	RunnerLogPath     string         `json:"runnerLogPath"`
	ContainerInspect  string         `json:"containerInspectPath"`
	ImageReference    string         `json:"imageReference"`
	ImageID           string         `json:"imageId"`
	ImageRepoDigests  []string       `json:"imageRepoDigests"`
	RunnerImageID     string         `json:"runnerImageId"`
	ProfileSHA256     string         `json:"profileSha256"`
	ResolvedProfile   Profile        `json:"resolvedProfile"`
	ResourceLimits    resourceLimits `json:"resourceLimits"`
	NetworkMode       string         `json:"networkMode"`
	FailureReason     string         `json:"failureReason,omitempty"`
	Summary           *Summary       `json:"summary,omitempty"`
}

type manifest struct {
	SchemaVersion              int                       `json:"schemaVersion"`
	RunID                      string                    `json:"runId"`
	StartedAt                  string                    `json:"startedAt"`
	FinishedAt                 string                    `json:"finishedAt"`
	Purpose                    string                    `json:"purpose"`
	ReleaseGating              bool                      `json:"releaseGating"`
	Profile                    artifactProfile           `json:"profile"`
	Git                        gitMetadata               `json:"git"`
	GoVersion                  string                    `json:"goVersion"`
	OS                         string                    `json:"os"`
	Architecture               string                    `json:"architecture"`
	Docker                     dockerMetadata            `json:"docker"`
	Command                    []string                  `json:"command"`
	BuildBaseImage             string                    `json:"buildBaseImage"`
	PlannedBuildBaseImage      string                    `json:"plannedBuildBaseImage"`
	BuildBaseImageSubstitution string                    `json:"buildBaseImageSubstitution"`
	RunnerImage                string                    `json:"runnerImage"`
	RunnerImageID              string                    `json:"runnerImageId"`
	EMQX                       emqxMetadata              `json:"emqx"`
	Targets                    map[string]targetManifest `json:"targets"`
	FailureReason              string                    `json:"failureReason,omitempty"`
}

type emqxMetadata struct {
	Version        string `json:"version"`
	Edition        string `json:"edition"`
	License        string `json:"license"`
	EOL            string `json:"eol"`
	ImageReference string `json:"imageReference"`
	ImageIndex     string `json:"imageIndexDigest"`
	Historical     bool   `json:"historicalComparator"`
}

type comparison struct {
	SchemaVersion int                         `json:"schemaVersion"`
	RunID         string                      `json:"runId"`
	Equivalent    bool                        `json:"equivalent"`
	ReleaseGating bool                        `json:"releaseGating"`
	Reason        string                      `json:"reason"`
	Targets       map[string]comparisonTarget `json:"targets"`
}

type comparisonTarget struct {
	Success       bool   `json:"success"`
	SummaryPath   string `json:"summaryPath"`
	FailureReason string `json:"failureReason,omitempty"`
}

func compareCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-01 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp01", "root directory for benchmark artifacts")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected compare arguments: %v", flags.Args())
	}
	if *profilePath == "" {
		return errors.New("compare requires --profile")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	profilePathAbs, err := filepath.Abs(*profilePath)
	if err != nil {
		return fmt.Errorf("resolve profile path: %w", err)
	}
	outputRootAbs, err := filepath.Abs(*outputRoot)
	if err != nil {
		return fmt.Errorf("resolve output path: %w", err)
	}
	profile, profileBytes, profileHash, err := LoadProfile(profilePathAbs)
	if err != nil {
		return err
	}
	repoRoot, err := repositoryRoot()
	if err != nil {
		return err
	}
	runID := newRunID()
	runDir := filepath.Join(outputRootAbs, runID)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("create run artifact directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "profile.json"), profileBytes, 0o644); err != nil {
		return fmt.Errorf("write resolved profile source: %w", err)
	}

	startedAt := time.Now().UTC()
	metadata := collectHostMetadata(repoRoot)
	currentManifest := manifest{
		SchemaVersion:              1,
		RunID:                      runID,
		StartedAt:                  startedAt.Format(time.RFC3339Nano),
		Purpose:                    profile.Purpose,
		ReleaseGating:              false,
		Profile:                    artifactProfile{RawSHA256: profileHash, Resolved: profile},
		Git:                        metadata.git,
		GoVersion:                  metadata.goVersion,
		OS:                         runtime.GOOS,
		Architecture:               runtime.GOARCH,
		Docker:                     dockerMetadata{ClientServerVersion: metadata.dockerVersion},
		Command:                    append([]string(nil), []string{"compare", "--profile", *profilePath, "--output", *outputRoot}...),
		BuildBaseImage:             buildBaseImageReference,
		PlannedBuildBaseImage:      plannedBuildBaseImage,
		BuildBaseImageSubstitution: "planned digest was unavailable in Docker Hub; immutable digest resolved from golang:1.26.5-bookworm on 2026-09-19",
		RunnerImage:                "",
		Targets:                    make(map[string]targetManifest),
		EMQX: emqxMetadata{
			Version:        emqxVersion,
			Edition:        emqxEdition,
			License:        emqxLicense,
			EOL:            emqxEOLEnd,
			ImageReference: emqxImageReference,
			ImageIndex:     "sha256:4593b6e30196754779fa14483332cb0be56c233f1c8a387b1308bec4337c0144",
			Historical:     true,
		},
	}
	writeFailure := func(reason string) error {
		currentManifest.FailureReason = reason
		currentManifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if writeErr := writeManifest(runDir, currentManifest); writeErr != nil {
			return fmt.Errorf("%s; write manifest: %w", reason, writeErr)
		}
		comparisonResult := buildComparison(currentManifest)
		if compareErr := writeComparison(runDir, comparisonResult); compareErr != nil {
			return fmt.Errorf("%s; write comparison: %w", reason, compareErr)
		}
		return errors.New(reason)
	}

	images, err := buildBenchmarkImages(ctx, repoRoot, runDir, runID)
	if err != nil {
		return writeFailure(err.Error())
	}
	currentManifest.RunnerImage = images.runnerReference
	currentManifest.RunnerImageID = images.runnerID
	if err := ensureEMQXImage(ctx, runDir); err != nil {
		return writeFailure(err.Error())
	}

	for _, target := range []string{"narumq", "emqx"} {
		targetDir := filepath.Join(runDir, target)
		imageReference := images.narumqReference
		if target == "emqx" {
			imageReference = emqxImageReference
		}
		targetResult := runTarget(ctx, target, targetDir, filepath.Join(runDir, "profile.json"), imageReference, images.runnerReference, images.runnerID, profileHash, profile, runID)
		currentManifest.Targets[target] = targetResult
	}
	currentManifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := writeManifest(runDir, currentManifest); err != nil {
		return err
	}
	comparisonResult := buildComparison(currentManifest)
	if err := writeComparison(runDir, comparisonResult); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(stdout, "%s\n", filepath.Join(runDir, "manifest.json")); err != nil {
		return fmt.Errorf("write compare result: %w", err)
	}
	if !comparisonResult.Equivalent {
		return fmt.Errorf("BP-01 comparison is not equivalent: %s", comparisonResult.Reason)
	}
	return nil
}

type hostMetadata struct {
	git           gitMetadata
	goVersion     string
	dockerVersion string
}

func collectHostMetadata(repoRoot string) hostMetadata {
	commit, _ := commandText(context.Background(), repoRoot, "git", "rev-parse", "HEAD")
	status, _ := commandText(context.Background(), repoRoot, "git", "status", "--porcelain", "--untracked-files=all")
	goVersion, _ := commandText(context.Background(), repoRoot, "go", "version")
	dockerVersion, _ := commandText(context.Background(), repoRoot, "docker", "version", "--format", "{{json .}}")
	return hostMetadata{
		git:           gitMetadata{Commit: strings.TrimSpace(commit), Dirty: strings.TrimSpace(status) != ""},
		goVersion:     strings.TrimSpace(goVersion),
		dockerVersion: strings.TrimSpace(dockerVersion),
	}
}

func repositoryRoot() (string, error) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for current := workingDirectory; ; current = filepath.Dir(current) {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("repository root with go.mod was not found")
		}
	}
}

func newRunID() string {
	return "bp01-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}

type builtImages struct {
	narumqReference string
	narumqID        string
	runnerReference string
	runnerID        string
}

func buildBenchmarkImages(ctx context.Context, repoRoot, runDir, runID string) (builtImages, error) {
	narumqReference := "narumq-bp01-narumq:" + runID
	runnerReference := "narumq-bp01-runner:" + runID
	for target, reference := range map[string]string{"narumq": narumqReference, "runner": runnerReference} {
		output, err := commandOutput(ctx, repoRoot, "docker", "build", "--progress=plain", "--target", target, "--tag", reference, "--file", filepath.Join("tests", "benchmark", "bp01", "Dockerfile"), ".")
		if writeErr := os.WriteFile(filepath.Join(runDir, "docker-build-"+target+".log"), output, 0o644); writeErr != nil {
			return builtImages{}, fmt.Errorf("write Docker build log for %s: %w", target, writeErr)
		}
		if err != nil {
			return builtImages{}, fmt.Errorf("build %s image: %w", target, err)
		}
	}
	narumqID, err := dockerImageID(ctx, repoRoot, narumqReference)
	if err != nil {
		return builtImages{}, err
	}
	runnerID, err := dockerImageID(ctx, repoRoot, runnerReference)
	if err != nil {
		return builtImages{}, err
	}
	return builtImages{narumqReference: narumqReference, narumqID: narumqID, runnerReference: runnerReference, runnerID: runnerID}, nil
}

func ensureEMQXImage(ctx context.Context, runDir string) error {
	output, err := commandOutput(ctx, "", "docker", "pull", emqxImageReference)
	if writeErr := os.WriteFile(filepath.Join(runDir, "docker-pull-emqx.log"), output, 0o644); writeErr != nil {
		return fmt.Errorf("write EMQX pull log: %w", writeErr)
	}
	if err != nil {
		return fmt.Errorf("pull exact EMQX image: %w", err)
	}
	digests, err := dockerRepoDigests(ctx, "", emqxImageReference)
	if err != nil {
		return err
	}
	want := "emqx/emqx@" + strings.SplitN(strings.SplitN(emqxImageReference, "@", 2)[1], "@", 2)[0]
	for _, digest := range digests {
		if digest == want {
			return nil
		}
	}
	return fmt.Errorf("EMQX image repo digest mismatch: got %v, want %s", digests, want)
}

func runTarget(ctx context.Context, target, targetDir, profilePath, imageReference, runnerReference, runnerID, profileHash string, profile Profile, runID string) targetManifest {
	startedAt := time.Now().UTC()
	result := targetManifest{
		Target:            target,
		Status:            "failed",
		StartedAt:         startedAt.Format(time.RFC3339Nano),
		ArtifactDirectory: targetDir,
		SummaryPath:       filepath.Join(targetDir, "summary.json"),
		SamplesPath:       filepath.Join(targetDir, "samples.jsonl"),
		BrokerStatsPath:   filepath.Join(targetDir, "broker-stats.jsonl"),
		BrokerLogPath:     filepath.Join(targetDir, "broker.log"),
		RunnerLogPath:     filepath.Join(targetDir, "runner.log"),
		ContainerInspect:  filepath.Join(targetDir, "container-inspect.json"),
		ImageReference:    imageReference,
		RunnerImageID:     runnerID,
		ProfileSHA256:     profileHash,
		ResolvedProfile:   profile,
		NetworkMode:       "none",
		ResourceLimits: resourceLimits{
			CPUs:              profile.BrokerCPUs,
			MemoryBytes:       profile.BrokerMemoryBytes,
			ReadOnlyRootFS:    true,
			NetworkMode:       "none",
			GeneratorCPUs:     profile.GeneratorCPUs,
			GeneratorMemory:   profile.GeneratorMemoryBytes,
			RunnerReadOnlyFS:  true,
			RunnerNetworkMode: "container:<broker>",
		},
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return finishTargetFailure(result, err)
	}
	containerName := "bp01-" + runID + "-" + target
	volumeNames := []string{""}
	cleanup := func() {
		_, _ = commandOutput(ctx, "", "docker", "rm", "--force", containerName)
		for _, volumeName := range volumeNames {
			if volumeName != "" {
				_, _ = commandOutput(ctx, "", "docker", "volume", "rm", "--force", volumeName)
			}
		}
	}
	defer cleanup()

	createArgs := []string{"run", "--detach", "--name", containerName, "--network", "none", "--cpus", formatCPU(profile.BrokerCPUs), "--memory", strconv.FormatInt(profile.BrokerMemoryBytes, 10) + "b", "--read-only", "--tmpfs", "/tmp:size=64m"}
	if target == "emqx" {
		dataVolume := containerName + "-data"
		logVolume := containerName + "-log"
		volumeNames = []string{dataVolume, logVolume}
		for _, volumeName := range volumeNames {
			if output, err := commandOutput(ctx, "", "docker", "volume", "create", volumeName); err != nil {
				return finishTargetFailure(result, fmt.Errorf("create volume %s: %w", volumeName, err))
			} else if strings.TrimSpace(string(output)) == "" {
				return finishTargetFailure(result, fmt.Errorf("create volume %s returned no name", volumeName))
			}
		}
		createArgs = append(createArgs, "--mount", "type=volume,source="+dataVolume+",destination=/opt/emqx/data", "--mount", "type=volume,source="+logVolume+",destination=/opt/emqx/log")
		createArgs = append(createArgs, "--env", "EMQX_NODE__NAME=emqx@127.0.0.1", "--env", "EMQX_NODE__COOKIE=bp01-emqx-cookie")
	}
	createArgs = append(createArgs, imageReference)
	if target == "narumq" {
		createArgs = append(createArgs, "-tcp-listen", "127.0.0.1:1883")
	}
	output, err := commandOutput(ctx, "", "docker", createArgs...)
	if err != nil {
		return finishTargetFailure(result, fmt.Errorf("start %s broker: %w", target, err))
	}
	if strings.TrimSpace(string(output)) == "" {
		return finishTargetFailure(result, fmt.Errorf("start %s broker returned no container ID", target))
	}
	if err := waitForContainerRunning(ctx, containerName, startupTimeout); err != nil {
		result = finishTargetFailure(result, err)
		return result
	}
	if err := waitForBrokerReady(ctx, containerName, target, startupTimeout); err != nil {
		result = finishTargetFailure(result, err)
		return result
	}
	var imageErr error
	result.ImageID, imageErr = dockerImageID(ctx, "", imageReference)
	if imageErr != nil {
		return finishTargetFailure(result, imageErr)
	}
	result.ImageRepoDigests, imageErr = dockerRepoDigests(ctx, "", imageReference)
	if imageErr != nil {
		return finishTargetFailure(result, imageErr)
	}
	statsCancel, statsDone := startStatsSampler(ctx, containerName, result.BrokerStatsPath)
	runDuration := time.Duration(profile.WarmupMillis+profile.MeasureMillis+profile.DrainMillis)*time.Millisecond + startupTimeout
	runContext, cancel := context.WithTimeout(ctx, runDuration)
	runnerArgs := []string{"run", "--rm", "--name", containerName + "-runner", "--network", "container:" + containerName, "--cpus", formatCPU(profile.GeneratorCPUs), "--memory", strconv.FormatInt(profile.GeneratorMemoryBytes, 10) + "b", "--read-only", "--tmpfs", "/tmp:size=64m", "--mount", "type=bind,source=" + profilePath + ",destination=/input/profile.json,readonly", "--mount", "type=bind,source=" + targetDir + ",destination=/output", runnerReference, "run", "--profile", "/input/profile.json", "--target", target, "--address", "127.0.0.1:1883", "--output", "/output"}
	runnerOutput, runnerErr := commandOutput(runContext, "", "docker", runnerArgs...)
	cancel()
	statsCancel()
	statsErr := <-statsDone
	if statsErr != nil {
		result = finishTargetFailure(result, fmt.Errorf("broker stats failed: %w", statsErr))
	} else if sampleErr := appendBrokerStatsSample(ctx, containerName, result.BrokerStatsPath); sampleErr != nil && result.FailureReason == "" {
		result = finishTargetFailure(result, fmt.Errorf("final broker stats sample failed: %w", sampleErr))
	}
	if writeErr := os.WriteFile(result.RunnerLogPath, runnerOutput, 0o644); writeErr != nil && runnerErr == nil {
		result = finishTargetFailure(result, fmt.Errorf("write runner log: %w", writeErr))
	}
	if runnerErr != nil {
		result = finishTargetFailure(result, fmt.Errorf("runner failed: %w", runnerErr))
	}
	if inspect, inspectErr := commandOutput(ctx, "", "docker", "inspect", containerName); inspectErr == nil {
		if writeErr := os.WriteFile(result.ContainerInspect, inspect, 0o644); writeErr != nil && result.FailureReason == "" {
			result = finishTargetFailure(result, fmt.Errorf("write container inspect: %w", writeErr))
		}
	} else if result.FailureReason == "" {
		result = finishTargetFailure(result, fmt.Errorf("inspect broker container: %w", inspectErr))
	}
	if logs, logErr := commandOutput(ctx, "", "docker", "logs", containerName); logErr == nil {
		if writeErr := os.WriteFile(result.BrokerLogPath, logs, 0o644); writeErr != nil && result.FailureReason == "" {
			result = finishTargetFailure(result, fmt.Errorf("write broker log: %w", writeErr))
		}
	} else if result.FailureReason == "" {
		result = finishTargetFailure(result, fmt.Errorf("collect broker logs: %w", logErr))
	}
	if summaryData, summaryErr := os.ReadFile(result.SummaryPath); summaryErr == nil {
		var summary Summary
		if decodeErr := json.Unmarshal(summaryData, &summary); decodeErr == nil {
			result.Summary = &summary
			if !summary.Valid && result.FailureReason == "" {
				result = finishTargetFailure(result, errors.New("summary marked target invalid"))
			}
		} else if result.FailureReason == "" {
			result = finishTargetFailure(result, fmt.Errorf("decode summary: %w", decodeErr))
		}
	} else if result.FailureReason == "" {
		result = finishTargetFailure(result, fmt.Errorf("read summary: %w", summaryErr))
	}
	if result.FailureReason == "" {
		result.Status = "success"
		result.Success = true
	}
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return result
}

func finishTargetFailure(result targetManifest, err error) targetManifest {
	if err != nil && result.FailureReason == "" {
		result.FailureReason = err.Error()
	}
	result.Status = "failed"
	result.Success = false
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return result
}

func formatCPU(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
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
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return ctx.Err()
			case <-timer.C:
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
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func startStatsSampler(ctx context.Context, containerName, outputPath string) (func(), <-chan error) {
	samplerContext, cancel := context.WithCancel(ctx)
	stopRequested := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(stopRequested)
			cancel()
		})
	}
	isStopped := func() bool {
		select {
		case <-stopRequested:
			return true
		default:
			return false
		}
	}
	done := make(chan error, 1)
	go func() {
		file, err := os.Create(outputPath)
		if err != nil {
			if isStopped() {
				done <- nil
			} else {
				done <- err
			}
			return
		}
		defer file.Close()
		writeSample := func() error {
			output, err := commandOutput(samplerContext, "", "docker", "stats", "--no-stream", "--format", "{{json .}}", containerName)
			if err != nil {
				return err
			}
			line := strings.TrimSpace(string(output))
			if line == "" {
				return errors.New("docker stats returned no sample")
			}
			_, err = fmt.Fprintf(file, "{\"collectedAt\":%q,\"container\":%q,\"docker\":%s}\n", time.Now().UTC().Format(time.RFC3339Nano), containerName, line)
			return err
		}
		if err := writeSample(); err != nil {
			if isStopped() {
				done <- nil
			} else {
				done <- err
			}
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-samplerContext.Done():
				if isStopped() {
					done <- nil
				} else {
					done <- samplerContext.Err()
				}
				return
			case <-ticker.C:
				if err := writeSample(); err != nil {
					if isStopped() {
						done <- nil
					} else {
						done <- err
					}
					return
				}
			}
		}
	}()
	return stop, done
}

func appendBrokerStatsSample(ctx context.Context, containerName, outputPath string) error {
	output, err := commandOutput(ctx, "", "docker", "stats", "--no-stream", "--format", "{{json .}}", containerName)
	if err != nil {
		return err
	}
	line := strings.TrimSpace(string(output))
	if line == "" {
		return errors.New("docker stats returned no sample")
	}
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "{\"collectedAt\":%q,\"container\":%q,\"docker\":%s}\n", time.Now().UTC().Format(time.RFC3339Nano), containerName, line)
	return err
}

func dockerImageID(ctx context.Context, workingDirectory, imageReference string) (string, error) {
	output, err := commandOutput(ctx, workingDirectory, "docker", "image", "inspect", "--format", "{{.Id}}", imageReference)
	if err != nil {
		return "", fmt.Errorf("inspect image %s: %w", imageReference, err)
	}
	return strings.TrimSpace(string(output)), nil
}

func dockerRepoDigests(ctx context.Context, workingDirectory, imageReference string) ([]string, error) {
	output, err := commandOutput(ctx, workingDirectory, "docker", "image", "inspect", "--format", "{{json .RepoDigests}}", imageReference)
	if err != nil {
		return nil, fmt.Errorf("inspect image digests %s: %w", imageReference, err)
	}
	var digests []string
	if err := json.Unmarshal(output, &digests); err != nil {
		return nil, fmt.Errorf("decode image digests %s: %w", imageReference, err)
	}
	return digests, nil
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

func writeManifest(runDir string, value manifest) error {
	return writeJSONFile(filepath.Join(runDir, "manifest.json"), value)
}

func writeComparison(runDir string, value comparison) error {
	return writeJSONFile(filepath.Join(runDir, "comparison.json"), value)
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func buildComparison(value manifest) comparison {
	result := comparison{
		SchemaVersion: 1,
		RunID:         value.RunID,
		ReleaseGating: false,
		Targets:       make(map[string]comparisonTarget, len(value.Targets)),
	}
	for target, targetValue := range value.Targets {
		result.Targets[target] = comparisonTarget{Success: targetValue.Success, SummaryPath: targetValue.SummaryPath, FailureReason: targetValue.FailureReason}
	}
	narumq, narumqOK := value.Targets["narumq"]
	emqx, emqxOK := value.Targets["emqx"]
	if !narumqOK || !emqxOK {
		result.Reason = "both target manifests are required"
		return result
	}
	if !narumq.Success || !emqx.Success {
		result.Reason = "both target runs must succeed"
		return result
	}
	if value.Profile.RawSHA256 == "" || narumq.ProfileSHA256 == "" || narumq.ProfileSHA256 != emqx.ProfileSHA256 || narumq.ProfileSHA256 != value.Profile.RawSHA256 || !reflect.DeepEqual(narumq.ResolvedProfile, emqx.ResolvedProfile) {
		result.Reason = "resolved profile digest mismatch"
		return result
	}
	if !reflect.DeepEqual(narumq.ResourceLimits, emqx.ResourceLimits) || narumq.NetworkMode != emqx.NetworkMode || narumq.RunnerImageID != emqx.RunnerImageID {
		result.Reason = "target resource, network, or runner image conditions differ"
		return result
	}
	if narumq.Summary == nil || emqx.Summary == nil || !narumq.Summary.Valid || !emqx.Summary.Valid {
		result.Reason = "both summaries must be valid"
		return result
	}
	result.Equivalent = true
	result.Reason = "both target runs succeeded under the resolved BP-01 conditions"
	return result
}
