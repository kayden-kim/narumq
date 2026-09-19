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
	"time"

	"narumq/tests/benchmark/internal/environment"
)

const (
	emqxImageReference      = environment.EMQXImageReference
	emqxVersion             = environment.EMQXVersion
	emqxEdition             = environment.EMQXEdition
	emqxLicense             = environment.EMQXLicense
	emqxEOLEnd              = environment.EMQXEOLEnd
	buildBaseImageReference = environment.BuildBaseImageReference
	plannedBuildBaseImage   = environment.PlannedBuildBaseImage
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
	Target              string           `json:"target"`
	Status              string           `json:"status"`
	Success             bool             `json:"success"`
	StartedAt           string           `json:"startedAt"`
	FinishedAt          string           `json:"finishedAt"`
	ArtifactDirectory   string           `json:"artifactDirectory"`
	WorkloadSummaryPath string           `json:"workloadSummaryPath"`
	ResourceSummaryPath string           `json:"resourceSummaryPath"`
	ConnectionsPath     string           `json:"connectionsPath"`
	PhasesPath          string           `json:"phasesPath"`
	BrokerStatsPath     string           `json:"brokerStatsPath"`
	BrokerLogPath       string           `json:"brokerLogPath"`
	RunnerLogPath       string           `json:"runnerLogPath"`
	ContainerInspect    string           `json:"containerInspectPath"`
	ImageReference      string           `json:"imageReference"`
	ImageID             string           `json:"imageId"`
	ImageRepoDigests    []string         `json:"imageRepoDigests"`
	RunnerImageID       string           `json:"runnerImageId"`
	ProfileSHA256       string           `json:"profileSha256"`
	ResolvedProfile     Profile          `json:"resolvedProfile"`
	ResourceLimits      resourceLimits   `json:"resourceLimits"`
	NetworkMode         string           `json:"networkMode"`
	FailureReason       string           `json:"failureReason,omitempty"`
	WorkloadSummary     *WorkloadSummary `json:"workloadSummary,omitempty"`
	ResourceSummary     *ResourceSummary `json:"resourceSummary,omitempty"`
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
	SummaryPath                string                    `json:"summaryPath"`
	ComparisonPath             string                    `json:"comparisonPath"`
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

type combinedSummary struct {
	SchemaVersion int                       `json:"schemaVersion"`
	RunID         string                    `json:"runId"`
	ReleaseGating bool                      `json:"releaseGating"`
	Valid         bool                      `json:"valid"`
	Targets       map[string]combinedTarget `json:"targets"`
}

type combinedTarget struct {
	Valid           bool             `json:"valid"`
	WorkloadSummary *WorkloadSummary `json:"workloadSummary,omitempty"`
	ResourceSummary *ResourceSummary `json:"resourceSummary,omitempty"`
	FailureReason   string           `json:"failureReason,omitempty"`
}

type comparison struct {
	SchemaVersion   int                         `json:"schemaVersion"`
	RunID           string                      `json:"runId"`
	Equivalent      bool                        `json:"equivalent"`
	ReleaseGating   bool                        `json:"releaseGating"`
	Reason          string                      `json:"reason"`
	ProfileSHA256   string                      `json:"profileSha256,omitempty"`
	ResolvedProfile *Profile                    `json:"resolvedProfile,omitempty"`
	ResourceLimits  resourceLimits              `json:"resourceLimits"`
	NetworkMode     string                      `json:"networkMode"`
	RunnerImageID   string                      `json:"runnerImageId"`
	Targets         map[string]comparisonTarget `json:"targets"`
}

type comparisonTarget struct {
	Success             bool   `json:"success"`
	WorkloadSummaryPath string `json:"workloadSummaryPath"`
	ResourceSummaryPath string `json:"resourceSummaryPath"`
	FailureReason       string `json:"failureReason,omitempty"`
}

func compareCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-05 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp05", "root directory for benchmark artifacts")
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
		return fmt.Errorf("write profile source: %w", err)
	}
	startedAt := time.Now().UTC()
	metadata := collectHostMetadata(repoRoot)
	currentManifest := manifest{
		SchemaVersion:              1,
		RunID:                      runID,
		StartedAt:                  startedAt.Format(time.RFC3339Nano),
		Purpose:                    profile.Purpose,
		ReleaseGating:              false,
		SummaryPath:                filepath.Join(runDir, "summary.json"),
		ComparisonPath:             filepath.Join(runDir, "comparison.json"),
		Profile:                    artifactProfile{RawSHA256: profileHash, Resolved: profile},
		Git:                        metadata.git,
		GoVersion:                  metadata.goVersion,
		OS:                         runtime.GOOS,
		Architecture:               runtime.GOARCH,
		Docker:                     dockerMetadata{ClientServerVersion: metadata.dockerVersion},
		Command:                    []string{"compare", "--profile", *profilePath, "--output", *outputRoot},
		BuildBaseImage:             buildBaseImageReference,
		PlannedBuildBaseImage:      plannedBuildBaseImage,
		BuildBaseImageSubstitution: "planned digest was unavailable in Docker Hub; immutable digest resolved from golang:1.26.5-bookworm on 2026-09-19",
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
		if writeErr := writeCombinedSummary(runDir, currentManifest); writeErr != nil {
			return fmt.Errorf("%s; write summary: %w", reason, writeErr)
		}
		if writeErr := writeComparison(runDir, buildComparison(currentManifest)); writeErr != nil {
			return fmt.Errorf("%s; write comparison: %w", reason, writeErr)
		}
		return errors.New(reason)
	}

	images, err := environment.BuildImages(ctx, environment.BuildOptions{BenchmarkID: "bp05", RunID: runID, RepoRoot: repoRoot, ArtifactDir: runDir, RunnerTarget: "bp05-runner"})
	if err != nil {
		return writeFailure(err.Error())
	}
	currentManifest.RunnerImage = images.RunnerReference
	currentManifest.RunnerImageID = images.RunnerID
	if err := environment.EnsureEMQXImage(ctx, runDir); err != nil {
		return writeFailure(err.Error())
	}
	targetTimeout := targetTimeoutFor(profile)
	for _, target := range []string{"narumq", "emqx"} {
		targetDir := filepath.Join(runDir, target)
		imageReference := images.NaruMQReference
		if target == "emqx" {
			imageReference = emqxImageReference
		}
		result, runErr := environment.RunTarget(ctx, environment.TargetOptions{
			BenchmarkID:            "bp05",
			RunID:                  runID,
			Target:                 target,
			ArtifactDirectory:      targetDir,
			ProfilePath:            filepath.Join(runDir, "profile.json"),
			ImageReference:         imageReference,
			RunnerReference:        images.RunnerReference,
			RunnerImageID:          images.RunnerID,
			RunnerTarget:           images.RunnerTarget,
			BrokerCPUs:             profile.BrokerCPUs,
			BrokerMemoryBytes:      profile.BrokerMemoryBytes,
			GeneratorCPUs:          profile.GeneratorCPUs,
			GeneratorMemoryBytes:   profile.GeneratorMemoryBytes,
			PreRunSamplingDuration: time.Duration(profile.BaselineMillis) * time.Millisecond,
			Timeout:                targetTimeout,
			StartupTimeout:         environment.DefaultStartupTimeout,
			RunnerArgs:             []string{"run", "--profile", "/input/profile.json", "--target", target, "--address", "127.0.0.1:1883", "--output", "/output"},
		})
		targetValue := finishTarget(result, profile, profileHash, targetDir, imageReference, runErr)
		currentManifest.Targets[target] = targetValue
	}
	currentManifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := writeManifest(runDir, currentManifest); err != nil {
		return err
	}
	if err := writeCombinedSummary(runDir, currentManifest); err != nil {
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
		return fmt.Errorf("BP-05 comparison is not equivalent: %s", comparisonResult.Reason)
	}
	return nil
}

func targetTimeoutFor(profile Profile) time.Duration {
	baseline := time.Duration(profile.BaselineMillis) * time.Millisecond
	idle := time.Duration(profile.IdleMillis) * time.Millisecond
	initialRamp := time.Duration(uint64(profile.ConnectionCount)/profile.RampConnectionsPerSecond+2) * time.Second
	reconnectRamp := time.Duration(uint64(profile.ReconnectCount)/profile.ReconnectRatePerSecond+2) * time.Second
	return environment.DefaultStartupTimeout + baseline + idle + initialRamp + reconnectRamp + 2*environment.DefaultHandshakeTimeout + 30*time.Second
}

func finishTarget(result environment.TargetResult, profile Profile, profileHash, targetDir, imageReference string, runErr error) targetManifest {
	value := targetManifest{
		Target:              result.Target,
		Status:              result.Status,
		StartedAt:           result.StartedAt,
		FinishedAt:          result.FinishedAt,
		ArtifactDirectory:   targetDir,
		WorkloadSummaryPath: filepath.Join(targetDir, "workload-summary.json"),
		ResourceSummaryPath: filepath.Join(targetDir, "resource-summary.json"),
		ConnectionsPath:     filepath.Join(targetDir, "connections.jsonl"),
		PhasesPath:          filepath.Join(targetDir, "phases.jsonl"),
		BrokerStatsPath:     result.BrokerStatsPath,
		BrokerLogPath:       result.BrokerLogPath,
		RunnerLogPath:       result.RunnerLogPath,
		ContainerInspect:    result.ContainerInspect,
		ImageReference:      result.ImageReference,
		ImageID:             result.ImageID,
		ImageRepoDigests:    append([]string(nil), result.ImageRepoDigests...),
		RunnerImageID:       result.RunnerImageID,
		ProfileSHA256:       profileHash,
		ResolvedProfile:     profile,
		NetworkMode:         "none",
		FailureReason:       result.FailureReason,
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
	if value.Target == "" {
		value.Target = result.Target
	}
	if value.ImageReference == "" {
		value.ImageReference = imageReference
	}
	if value.BrokerStatsPath == "" {
		value.BrokerStatsPath = filepath.Join(targetDir, "broker-stats.jsonl")
	}
	if runErr != nil && value.FailureReason == "" {
		value.FailureReason = runErr.Error()
	}
	if data, err := os.ReadFile(value.WorkloadSummaryPath); err == nil {
		var workload WorkloadSummary
		if err := json.Unmarshal(data, &workload); err == nil {
			value.WorkloadSummary = &workload
		} else if value.FailureReason == "" {
			value.FailureReason = fmt.Sprintf("decode workload summary: %v", err)
		}
	} else if value.FailureReason == "" {
		value.FailureReason = fmt.Sprintf("read workload summary: %v", err)
	}
	resource, resourceErr := ComputeResourceSummary(value.BrokerStatsPath, value.PhasesPath, value.WorkloadSummaryPath)
	if err := writeJSONFile(value.ResourceSummaryPath, resource); err != nil && value.FailureReason == "" {
		value.FailureReason = fmt.Sprintf("write resource summary: %v", err)
	}
	if resourceErr == nil || resource.SchemaVersion != 0 {
		value.ResourceSummary = &resource
	}
	if resourceErr != nil && value.FailureReason == "" {
		value.FailureReason = resourceErr.Error()
	}
	if value.FailureReason == "" && !result.Success {
		value.FailureReason = "environment target failed"
	}
	if value.FailureReason == "" && value.WorkloadSummary != nil && value.ResourceSummary != nil && value.WorkloadSummary.Valid && value.ResourceSummary.Valid {
		value.Status = "success"
		value.Success = true
	} else {
		value.Status = "failed"
		value.Success = false
	}
	if value.FinishedAt == "" {
		value.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	return value
}

func buildComparison(value manifest) comparison {
	result := comparison{SchemaVersion: 1, RunID: value.RunID, ReleaseGating: false, Targets: make(map[string]comparisonTarget, len(value.Targets))}
	for target, targetValue := range value.Targets {
		result.Targets[target] = comparisonTarget{Success: targetValue.Success, WorkloadSummaryPath: targetValue.WorkloadSummaryPath, ResourceSummaryPath: targetValue.ResourceSummaryPath, FailureReason: targetValue.FailureReason}
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
	if value.Profile.RawSHA256 == "" || narumq.ProfileSHA256 != value.Profile.RawSHA256 || emqx.ProfileSHA256 != value.Profile.RawSHA256 || !reflect.DeepEqual(narumq.ResolvedProfile, emqx.ResolvedProfile) || !reflect.DeepEqual(narumq.ResolvedProfile, value.Profile.Resolved) {
		result.Reason = "resolved profile digest mismatch"
		return result
	}
	if !reflect.DeepEqual(narumq.ResourceLimits, emqx.ResourceLimits) || narumq.NetworkMode != emqx.NetworkMode || narumq.RunnerImageID != emqx.RunnerImageID {
		result.Reason = "target resource, network, or runner image conditions differ"
		return result
	}
	if narumq.WorkloadSummary == nil || emqx.WorkloadSummary == nil || !narumq.WorkloadSummary.Valid || !emqx.WorkloadSummary.Valid || narumq.ResourceSummary == nil || emqx.ResourceSummary == nil || !narumq.ResourceSummary.Valid || !emqx.ResourceSummary.Valid {
		result.Reason = "both workload and resource summaries must be valid"
		return result
	}
	result.Equivalent = true
	result.ProfileSHA256 = value.Profile.RawSHA256
	resolved := value.Profile.Resolved
	result.ResolvedProfile = &resolved
	result.ResourceLimits = narumq.ResourceLimits
	result.NetworkMode = narumq.NetworkMode
	result.RunnerImageID = narumq.RunnerImageID
	result.Reason = "both target runs succeeded under the resolved BP-05 conditions"
	return result
}

func writeManifest(runDir string, value manifest) error {
	return writeJSONFile(filepath.Join(runDir, "manifest.json"), value)
}

func writeComparison(runDir string, value comparison) error {
	return writeJSONFile(filepath.Join(runDir, "comparison.json"), value)
}

func writeCombinedSummary(runDir string, value manifest) error {
	summary := combinedSummary{SchemaVersion: 1, RunID: value.RunID, ReleaseGating: false, Targets: make(map[string]combinedTarget, len(value.Targets))}
	summary.Valid = true
	for target, targetValue := range value.Targets {
		valid := targetValue.Success && targetValue.WorkloadSummary != nil && targetValue.ResourceSummary != nil && targetValue.WorkloadSummary.Valid && targetValue.ResourceSummary.Valid
		summary.Targets[target] = combinedTarget{Valid: valid, WorkloadSummary: targetValue.WorkloadSummary, ResourceSummary: targetValue.ResourceSummary, FailureReason: targetValue.FailureReason}
		if !valid {
			summary.Valid = false
		}
	}
	if len(value.Targets) != 2 {
		summary.Valid = false
	}
	return writeJSONFile(filepath.Join(runDir, "summary.json"), summary)
}

func writeJSONFile(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	defer func() {
		_ = os.Remove(tempPath)
	}()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
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
	return hostMetadata{git: gitMetadata{Commit: strings.TrimSpace(commit), Dirty: strings.TrimSpace(status) != ""}, goVersion: strings.TrimSpace(goVersion), dockerVersion: strings.TrimSpace(dockerVersion)}
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
	return "bp05-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}

func commandText(ctx context.Context, workingDirectory, name string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, name, args...)
	if workingDirectory != "" {
		command.Dir = workingDirectory
	}
	output, err := command.CombinedOutput()
	return string(output), err
}
