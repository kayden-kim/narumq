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
	"narumq/tests/benchmark/internal/runtimeartifact"
)

const (
	emqxImageReference         = environment.EMQXImageReference
	emqxVersion                = environment.EMQXVersion
	emqxEdition                = environment.EMQXEdition
	emqxLicense                = environment.EMQXLicense
	emqxEOLEnd                 = environment.EMQXEOLEnd
	buildBaseImageReference    = environment.BuildBaseImageReference
	plannedBuildBaseImage      = environment.PlannedBuildBaseImage
	notApplicableRuntimeReason = "Go runtime/metrics applies only to the NaruMQ Go process"
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
	Target              string                   `json:"target"`
	Status              string                   `json:"status"`
	Success             bool                     `json:"success"`
	StartedAt           string                   `json:"startedAt"`
	FinishedAt          string                   `json:"finishedAt"`
	ArtifactDirectory   string                   `json:"artifactDirectory"`
	WorkloadSummaryPath string                   `json:"workloadSummaryPath"`
	ResourceSummaryPath string                   `json:"resourceSummaryPath"`
	ConnectionsPath     string                   `json:"connectionsPath"`
	PhasesPath          string                   `json:"phasesPath"`
	RuntimeMetricsPath  string                   `json:"runtimeMetricsPath"`
	RuntimeSummaryPath  string                   `json:"runtimeSummaryPath"`
	BrokerStatsPath     string                   `json:"brokerStatsPath"`
	BrokerLogPath       string                   `json:"brokerLogPath"`
	RunnerLogPath       string                   `json:"runnerLogPath"`
	ContainerInspect    string                   `json:"containerInspectPath"`
	ImageReference      string                   `json:"imageReference"`
	ImageID             string                   `json:"imageId"`
	ImageRepoDigests    []string                 `json:"imageRepoDigests"`
	RunnerImageID       string                   `json:"runnerImageId"`
	ProfileSHA256       string                   `json:"profileSha256"`
	ResolvedProfile     Profile                  `json:"resolvedProfile"`
	ResourceLimits      resourceLimits           `json:"resourceLimits"`
	NetworkMode         string                   `json:"networkMode"`
	FailureReason       string                   `json:"failureReason,omitempty"`
	WorkloadSummary     *WorkloadSummary         `json:"workloadSummary,omitempty"`
	ResourceSummary     *ResourceSummary         `json:"resourceSummary,omitempty"`
	RuntimeSummary      *runtimeartifact.Summary `json:"runtimeSummary,omitempty"`
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
	Valid           bool                     `json:"valid"`
	WorkloadSummary *WorkloadSummary         `json:"workloadSummary,omitempty"`
	ResourceSummary *ResourceSummary         `json:"resourceSummary,omitempty"`
	RuntimeSummary  *runtimeartifact.Summary `json:"runtimeSummary,omitempty"`
	FailureReason   string                   `json:"failureReason,omitempty"`
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
	Success              bool   `json:"success"`
	WorkloadSummaryPath  string `json:"workloadSummaryPath"`
	ResourceSummaryPath  string `json:"resourceSummaryPath"`
	RuntimeSummaryPath   string `json:"runtimeSummaryPath"`
	RuntimeApplicability string `json:"runtimeApplicability"`
	FailureReason        string `json:"failureReason,omitempty"`
}

type comparisonRunOptions struct {
	Profile       Profile
	ProfileBytes  []byte
	ProfileSHA256 string
	RepoRoot      string
	RunID         string
	RunDir        string
	Images        environment.ImageSet
	Metadata      hostMetadata
	Command       []string
	TargetOrder   []string
}

type comparisonExecutor func(context.Context, comparisonRunOptions) (manifest, comparison, error)

func validateTargetOrder(order []string) error {
	if len(order) != 2 {
		return fmt.Errorf("target order must contain exactly two targets")
	}
	seen := make(map[string]struct{}, len(order))
	for _, target := range order {
		if target != "narumq" && target != "emqx" {
			return fmt.Errorf("unsupported benchmark target %q", target)
		}
		if _, exists := seen[target]; exists {
			return fmt.Errorf("target order contains duplicate target %q", target)
		}
		seen[target] = struct{}{}
	}
	return nil
}

func newComparisonManifest(options comparisonRunOptions, startedAt time.Time) manifest {
	return manifest{
		SchemaVersion:              2,
		RunID:                      options.RunID,
		StartedAt:                  startedAt.UTC().Format(time.RFC3339Nano),
		Purpose:                    options.Profile.Purpose,
		ReleaseGating:              false,
		SummaryPath:                filepath.Join(options.RunDir, "summary.json"),
		ComparisonPath:             filepath.Join(options.RunDir, "comparison.json"),
		Profile:                    artifactProfile{RawSHA256: options.ProfileSHA256, Resolved: options.Profile},
		Git:                        options.Metadata.git,
		GoVersion:                  options.Metadata.goVersion,
		OS:                         runtime.GOOS,
		Architecture:               runtime.GOARCH,
		Docker:                     dockerMetadata{ClientServerVersion: options.Metadata.dockerVersion},
		Command:                    append([]string(nil), options.Command...),
		BuildBaseImage:             buildBaseImageReference,
		PlannedBuildBaseImage:      plannedBuildBaseImage,
		BuildBaseImageSubstitution: "planned digest was unavailable in Docker Hub; immutable digest resolved from golang:1.26.5-bookworm on 2026-09-19",
		RunnerImage:                options.Images.RunnerReference,
		RunnerImageID:              options.Images.RunnerID,
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
	command := []string{"compare", "--profile", *profilePath, "--output", *outputRoot}
	metadata := collectHostMetadata(repoRoot)
	options := comparisonRunOptions{
		Profile:       profile,
		ProfileBytes:  profileBytes,
		ProfileSHA256: profileHash,
		RepoRoot:      repoRoot,
		RunID:         runID,
		RunDir:        runDir,
		Metadata:      metadata,
		Command:       command,
		TargetOrder:   []string{"narumq", "emqx"},
	}
	currentManifest := newComparisonManifest(options, time.Now().UTC())
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

	images, err := environment.BuildImages(ctx, environment.BuildOptions{
		BenchmarkID:  "bp05",
		RunID:        runID,
		RepoRoot:     repoRoot,
		ArtifactDir:  runDir,
		RunnerTarget: "bp05-runner",
	})
	if err != nil {
		return writeFailure(err.Error())
	}
	options.Images = images
	currentManifest.RunnerImage = images.RunnerReference
	currentManifest.RunnerImageID = images.RunnerID
	if err := environment.EnsureEMQXImage(ctx, runDir, environment.EMQXImageReference); err != nil {
		return writeFailure(err.Error())
	}

	_, _, runErr := runComparison(ctx, options)
	if _, writeErr := fmt.Fprintf(stdout, "%s\n", filepath.Join(runDir, "manifest.json")); writeErr != nil {
		if runErr != nil {
			return errors.Join(runErr, fmt.Errorf("write compare result: %w", writeErr))
		}
		return fmt.Errorf("write compare result: %w", writeErr)
	}
	if runErr != nil {
		return runErr
	}
	return nil
}

func runComparison(ctx context.Context, options comparisonRunOptions) (manifest, comparison, error) {
	var emptyManifest manifest
	var emptyComparison comparison
	if err := validateTargetOrder(options.TargetOrder); err != nil {
		return emptyManifest, emptyComparison, err
	}
	if strings.TrimSpace(options.RunDir) == "" {
		return emptyManifest, emptyComparison, errors.New("run artifact directory is required")
	}
	if strings.TrimSpace(options.RunID) == "" {
		return emptyManifest, emptyComparison, errors.New("run ID is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := os.MkdirAll(options.RunDir, 0o755); err != nil {
		return emptyManifest, emptyComparison, fmt.Errorf("create run artifact directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(options.RunDir, "profile.json"), options.ProfileBytes, 0o644); err != nil {
		return emptyManifest, emptyComparison, fmt.Errorf("write profile source: %w", err)
	}

	currentManifest := newComparisonManifest(options, time.Now().UTC())
	var runErrors []error
	targetTimeout := targetTimeoutFor(options.Profile)
	for _, target := range options.TargetOrder {
		targetDir := filepath.Join(options.RunDir, target)
		imageReference := options.Images.NaruMQReference
		if target == "emqx" {
			imageReference = emqxImageReference
		}
		collectRuntimeMetrics := target == "narumq"
		runtimeMetricsInterval := time.Duration(0)
		if collectRuntimeMetrics {
			runtimeMetricsInterval = 500 * time.Millisecond
		}
		result, runErr := environment.RunTarget(ctx, environment.TargetOptions{
			BenchmarkID:            "bp05",
			RunID:                  options.RunID,
			Target:                 target,
			ArtifactDirectory:      targetDir,
			ProfilePath:            filepath.Join(options.RunDir, "profile.json"),
			ImageReference:         imageReference,
			RunnerReference:        options.Images.RunnerReference,
			RunnerImageID:          options.Images.RunnerID,
			RunnerTarget:           options.Images.RunnerTarget,
			BrokerCPUs:             options.Profile.BrokerCPUs,
			BrokerMemoryBytes:      options.Profile.BrokerMemoryBytes,
			GeneratorCPUs:          options.Profile.GeneratorCPUs,
			GeneratorMemoryBytes:   options.Profile.GeneratorMemoryBytes,
			PreRunSamplingDuration: time.Duration(options.Profile.BaselineMillis) * time.Millisecond,
			Timeout:                targetTimeout,
			StartupTimeout:         environment.DefaultStartupTimeout,
			CollectRuntimeMetrics:  collectRuntimeMetrics,
			RuntimeMetricsInterval: runtimeMetricsInterval,
			RunnerArgs:             []string{"run", "--profile", "/input/profile.json", "--target", target, "--address", "127.0.0.1:1883", "--output", "/output"},
		})
		targetValue := finishTarget(result, options.Profile, options.ProfileSHA256, targetDir, imageReference, runErr)
		currentManifest.Targets[target] = targetValue
		if runErr != nil {
			runErrors = append(runErrors, fmt.Errorf("%s target: %w", target, runErr))
		} else if !targetValue.Success && targetValue.FailureReason != "" {
			runErrors = append(runErrors, fmt.Errorf("%s target failed: %s", target, targetValue.FailureReason))
		}
	}
	currentManifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	comparisonResult := buildComparison(currentManifest)
	if err := writeManifest(options.RunDir, currentManifest); err != nil {
		runErrors = append(runErrors, fmt.Errorf("write manifest: %w", err))
	}
	if err := writeCombinedSummary(options.RunDir, currentManifest); err != nil {
		runErrors = append(runErrors, fmt.Errorf("write summary: %w", err))
	}
	if err := writeComparison(options.RunDir, comparisonResult); err != nil {
		runErrors = append(runErrors, fmt.Errorf("write comparison: %w", err))
	}
	if !comparisonResult.Equivalent {
		runErrors = append(runErrors, fmt.Errorf("BP-05 comparison is not equivalent: %s", comparisonResult.Reason))
	}
	return currentManifest, comparisonResult, errors.Join(runErrors...)
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
		RuntimeMetricsPath:  result.RuntimeMetricsPath,
		RuntimeSummaryPath:  filepath.Join(targetDir, "runtime-summary.json"),

		BrokerStatsPath:  result.BrokerStatsPath,
		BrokerLogPath:    result.BrokerLogPath,
		RunnerLogPath:    result.RunnerLogPath,
		ContainerInspect: result.ContainerInspect,
		ImageReference:   result.ImageReference,
		ImageID:          result.ImageID,
		ImageRepoDigests: append([]string(nil), result.ImageRepoDigests...),
		RunnerImageID:    result.RunnerImageID,
		ProfileSHA256:    profileHash,
		ResolvedProfile:  profile,
		NetworkMode:      "none",
		FailureReason:    result.FailureReason,
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
	if value.RuntimeMetricsPath == "" && value.Target == "narumq" {
		value.RuntimeMetricsPath = filepath.Join(targetDir, "runtime-metrics.jsonl")
	}
	if value.RuntimeSummaryPath == "" {
		value.RuntimeSummaryPath = filepath.Join(targetDir, "runtime-summary.json")
	}

	if runErr != nil {
		value.FailureReason = appendFailureReason(value.FailureReason, runErr)
	}
	if data, err := os.ReadFile(value.WorkloadSummaryPath); err == nil {
		var workload WorkloadSummary
		if err := json.Unmarshal(data, &workload); err == nil {
			value.WorkloadSummary = &workload
		} else {
			value.FailureReason = appendFailureReason(value.FailureReason, fmt.Errorf("decode workload summary: %w", err))
		}
	} else {
		value.FailureReason = appendFailureReason(value.FailureReason, fmt.Errorf("read workload summary: %w", err))
	}
	resource, resourceErr := ComputeResourceSummary(value.BrokerStatsPath, value.PhasesPath, value.WorkloadSummaryPath)
	if err := writeJSONFile(value.ResourceSummaryPath, resource); err != nil {
		value.FailureReason = appendFailureReason(value.FailureReason, fmt.Errorf("write resource summary: %w", err))
	}
	if resourceErr == nil || resource.SchemaVersion != 0 {
		value.ResourceSummary = &resource
	}
	if resourceErr != nil {
		value.FailureReason = appendFailureReason(value.FailureReason, resourceErr)
	}
	var runtimeSummary runtimeartifact.Summary
	var runtimeErr error
	if value.Target == "emqx" {
		runtimeSummary = runtimeartifact.NotApplicable(value.Target, notApplicableRuntimeReason)
	} else {
		runtimeSummary, runtimeErr = runtimeartifact.Compute(value.RuntimeMetricsPath, value.PhasesPath, "idle", value.Target)
	}
	value.RuntimeSummary = &runtimeSummary
	if err := writeJSONFile(value.RuntimeSummaryPath, runtimeSummary); err != nil {
		value.FailureReason = appendFailureReason(value.FailureReason, fmt.Errorf("write runtime summary: %w", err))
	}
	if runtimeErr != nil {
		value.FailureReason = appendFailureReason(value.FailureReason, runtimeErr)
	}
	if !validRuntimeSummary(value.Target, value.RuntimeSummary) {
		value.FailureReason = appendFailureReason(value.FailureReason, errors.New("runtime summary is invalid or has unexpected applicability"))
	}

	if !result.Success {
		value.FailureReason = appendFailureReason(value.FailureReason, errors.New("environment target failed"))
	}
	if value.FailureReason == "" && value.WorkloadSummary != nil && value.ResourceSummary != nil && value.WorkloadSummary.Valid && value.ResourceSummary.Valid && validRuntimeSummary(value.Target, value.RuntimeSummary) {
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

func appendFailureReason(existing string, err error) string {
	if err == nil {
		return existing
	}
	if existing == "" {
		return err.Error()
	}
	if existing == err.Error() {
		return existing
	}
	return errors.Join(errors.New(existing), err).Error()
}

func buildComparison(value manifest) comparison {
	result := comparison{SchemaVersion: 2, RunID: value.RunID, ReleaseGating: false, Targets: make(map[string]comparisonTarget, len(value.Targets))}
	for target, targetValue := range value.Targets {
		applicability := ""
		if targetValue.RuntimeSummary != nil {
			applicability = targetValue.RuntimeSummary.Applicability
		}
		result.Targets[target] = comparisonTarget{
			Success:              targetValue.Success,
			WorkloadSummaryPath:  targetValue.WorkloadSummaryPath,
			ResourceSummaryPath:  targetValue.ResourceSummaryPath,
			RuntimeSummaryPath:   targetValue.RuntimeSummaryPath,
			RuntimeApplicability: applicability,
			FailureReason:        targetValue.FailureReason,
		}
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
	if narumq.WorkloadSummary == nil || emqx.WorkloadSummary == nil || !narumq.WorkloadSummary.Valid || !emqx.WorkloadSummary.Valid || narumq.ResourceSummary == nil || emqx.ResourceSummary == nil || !narumq.ResourceSummary.Valid || !emqx.ResourceSummary.Valid || !validRuntimeSummary("narumq", narumq.RuntimeSummary) || !validRuntimeSummary("emqx", emqx.RuntimeSummary) {
		result.Reason = "workload, resource, and target runtime summaries must be valid"
		return result
	}
	result.Equivalent = true
	result.ProfileSHA256 = value.Profile.RawSHA256
	resolved := value.Profile.Resolved
	result.ResolvedProfile = &resolved
	result.ResourceLimits = narumq.ResourceLimits
	result.NetworkMode = narumq.NetworkMode
	result.RunnerImageID = narumq.RunnerImageID
	result.Reason = "same profile, resource, and execution conditions have valid workload/resource artifacts and target-specific runtime applicability"
	return result
}

func validRuntimeSummary(target string, summary *runtimeartifact.Summary) bool {
	if summary == nil || !summary.Valid || summary.ReleaseGating {
		return false
	}
	if target == "emqx" {
		return summary.Applicability == "not_applicable" && summary.NotApplicableReason != ""
	}
	return summary.Applicability == "required"
}

func writeManifest(runDir string, value manifest) error {
	return writeJSONFile(filepath.Join(runDir, "manifest.json"), value)
}
func writeComparison(runDir string, value comparison) error {
	return writeJSONFile(filepath.Join(runDir, "comparison.json"), value)
}

func writeCombinedSummary(runDir string, value manifest) error {
	summary := combinedSummary{SchemaVersion: 2, RunID: value.RunID, ReleaseGating: false, Targets: make(map[string]combinedTarget, len(value.Targets))}
	summary.Valid = true
	for target, targetValue := range value.Targets {
		valid := targetValue.Success && targetValue.WorkloadSummary != nil && targetValue.ResourceSummary != nil && targetValue.WorkloadSummary.Valid && targetValue.ResourceSummary.Valid && validRuntimeSummary(target, targetValue.RuntimeSummary)
		summary.Targets[target] = combinedTarget{
			Valid:           valid,
			WorkloadSummary: targetValue.WorkloadSummary,
			ResourceSummary: targetValue.ResourceSummary,
			RuntimeSummary:  targetValue.RuntimeSummary,
			FailureReason:   targetValue.FailureReason,
		}
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
