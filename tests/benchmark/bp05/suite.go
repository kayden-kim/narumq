package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"narumq/tests/benchmark/internal/environment"
	"narumq/tests/benchmark/internal/runtimeartifact"
	"narumq/tests/benchmark/internal/suiteartifact"
)

type suiteRun struct {
	Repetition        int      `json:"repetition"`
	RunID             string   `json:"runId"`
	StartedAt         string   `json:"startedAt"`
	FinishedAt        string   `json:"finishedAt"`
	TargetOrder       []string `json:"targetOrder"`
	ArtifactDirectory string   `json:"artifactDirectory"`
	Equivalent        bool     `json:"equivalent"`
	Success           bool     `json:"success"`
	FailureReason     string   `json:"failureReason,omitempty"`
}

type suiteManifest struct {
	SchemaVersion              int            `json:"schemaVersion"`
	SuiteID                    string         `json:"suiteId"`
	BenchmarkID                string         `json:"benchmarkId"`
	StartedAt                  string         `json:"startedAt"`
	FinishedAt                 string         `json:"finishedAt"`
	ProfileSHA256              string         `json:"profileSha256"`
	ResolvedProfile            Profile        `json:"resolvedProfile"`
	RequestedRepetitions       int            `json:"requestedRepetitions"`
	CompletedRepetitions       int            `json:"completedRepetitions"`
	OrderStrategy              string         `json:"orderStrategy"`
	ReleaseGating              bool           `json:"releaseGating"`
	Git                        gitMetadata    `json:"git"`
	GoVersion                  string         `json:"goVersion"`
	OS                         string         `json:"os"`
	Architecture               string         `json:"architecture"`
	Docker                     dockerMetadata `json:"docker"`
	BuildBaseImage             string         `json:"buildBaseImage"`
	PlannedBuildBaseImage      string         `json:"plannedBuildBaseImage"`
	BuildBaseImageSubstitution string         `json:"buildBaseImageSubstitution"`
	NaruMQImage                string         `json:"narumqImage"`
	NaruMQImageID              string         `json:"narumqImageId"`
	RunnerImage                string         `json:"runnerImage"`
	RunnerImageID              string         `json:"runnerImageId"`
	EMQX                       emqxMetadata   `json:"emqx"`
	Runs                       []suiteRun     `json:"runs"`
	Valid                      bool           `json:"valid"`
	FailureReason              string         `json:"failureReason,omitempty"`
}

type suiteCPUReason struct {
	Repetition int    `json:"repetition"`
	RunID      string `json:"runId"`
	Reason     string `json:"reason"`
}

type suiteTargetSummary struct {
	Metrics              map[string]suiteartifact.MetricSummary `json:"metrics"`
	RuntimeApplicability string                                 `json:"runtimeApplicability"`
	CPUMetricsAvailable  *bool                                  `json:"cpuMetricsAvailable,omitempty"`
	CPUMetricsReasons    []suiteCPUReason                       `json:"cpuMetricsReasons,omitempty"`
}

type suiteSummary struct {
	SchemaVersion         int                           `json:"schemaVersion"`
	SuiteID               string                        `json:"suiteId"`
	BenchmarkID           string                        `json:"benchmarkId"`
	RepetitionCount       int                           `json:"repetitionCount"`
	SuccessfulRepetitions int                           `json:"successfulRepetitions"`
	ReleaseGating         bool                          `json:"releaseGating"`
	Valid                 bool                          `json:"valid"`
	Targets               map[string]suiteTargetSummary `json:"targets"`
	FailureReason         string                        `json:"failureReason,omitempty"`
}

type suiteExecutionOptions struct {
	Profile       Profile
	ProfileBytes  []byte
	ProfileSHA256 string
	RepoRoot      string
	SuiteID       string
	SuiteDir      string
	Repetitions   int
	Images        environment.ImageSet
	Metadata      hostMetadata
	Command       []string
}

type suiteRunData struct {
	repetition int
	runID      string
	manifest   manifest
	comparison comparison
	success    bool
}
type runtimeMetricRun struct {
	repetition int
	runID      string
	summary    *runtimeartifact.Summary
}

var bp05SuiteMetricUnits = map[string]string{
	"initialConnectionRatePerSecond":       "count/second",
	"initialConnectLatencyP50Nanoseconds":  "nanoseconds",
	"initialConnectLatencyP95Nanoseconds":  "nanoseconds",
	"initialConnectLatencyP99Nanoseconds":  "nanoseconds",
	"initialConnectLatencyP999Nanoseconds": "nanoseconds",
	"reconnectRatePerSecond":               "count/second",
	"reconnectLatencyP50Nanoseconds":       "nanoseconds",
	"reconnectLatencyP95Nanoseconds":       "nanoseconds",
	"reconnectLatencyP99Nanoseconds":       "nanoseconds",
	"reconnectLatencyP999Nanoseconds":      "nanoseconds",
	"maxScheduleLagNanoseconds":            "nanoseconds",
	"memoryBytesPerIdleConnection":         "bytes/connection",
}

var bp05RuntimeMetricUnits = map[string]string{
	"allocationBytesPerSecond": "bytes/second",
	"allocationsPerSecond":     "allocations/second",
	"gcPauseP50Seconds":        "seconds",
	"gcPauseP95Seconds":        "seconds",
	"gcPauseP99Seconds":        "seconds",
	"gcPauseP999Seconds":       "seconds",
	"gcCPUSeconds":             "seconds",
	"runtimeCPUSeconds":        "seconds",
	"gcCPUFraction":            "ratio",
}

func suiteCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("suite", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-05 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp05", "root directory for benchmark artifacts")
	repetitions := flags.Int("repetitions", suiteartifact.MinimumRepetitions, "number of independent repetitions")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected suite arguments: %v", flags.Args())
	}
	if strings.TrimSpace(*profilePath) == "" {
		return errors.New("suite requires --profile")
	}
	if strings.TrimSpace(*outputRoot) == "" {
		return errors.New("suite requires a non-empty --output")
	}
	if *repetitions < suiteartifact.MinimumRepetitions {
		return fmt.Errorf("suite requires at least %d repetitions", suiteartifact.MinimumRepetitions)
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
	suiteID := newSuiteID()
	suiteDir := filepath.Join(outputRootAbs, suiteID)
	if err := os.MkdirAll(suiteDir, 0o755); err != nil {
		return fmt.Errorf("create suite artifact directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(suiteDir, "profile.json"), profileBytes, 0o644); err != nil {
		return fmt.Errorf("write suite profile: %w", err)
	}
	command := []string{"suite", "--profile", *profilePath, "--output", *outputRoot, "--repetitions", strconv.Itoa(*repetitions)}
	options := suiteExecutionOptions{
		Profile:       profile,
		ProfileBytes:  profileBytes,
		ProfileSHA256: profileHash,
		RepoRoot:      repoRoot,
		SuiteID:       suiteID,
		SuiteDir:      suiteDir,
		Repetitions:   *repetitions,
		Metadata:      collectHostMetadata(repoRoot),
		Command:       command,
	}
	currentManifest := newSuiteManifest(options, time.Now().UTC())
	if err := writeSuiteManifest(suiteDir, currentManifest); err != nil {
		return err
	}
	images, err := environment.BuildImages(ctx, environment.BuildOptions{
		BenchmarkID:  "bp05",
		RunID:        suiteID,
		RepoRoot:     repoRoot,
		ArtifactDir:  suiteDir,
		RunnerTarget: "bp05-runner",
	})
	if err != nil {
		return finishSuitePreparationFailure(stdout, suiteDir, currentManifest, err)
	}
	options.Images = images
	currentManifest.NaruMQImage = images.NaruMQReference
	currentManifest.NaruMQImageID = images.NaruMQID
	currentManifest.RunnerImage = images.RunnerReference
	currentManifest.RunnerImageID = images.RunnerID
	if err := writeSuiteManifest(suiteDir, currentManifest); err != nil {
		return finishSuitePreparationFailure(stdout, suiteDir, currentManifest, err)
	}
	if err := environment.EnsureEMQXImage(ctx, suiteDir); err != nil {
		return finishSuitePreparationFailure(stdout, suiteDir, currentManifest, err)
	}

	_, _, suiteErr := executeSuite(ctx, options, runComparison)
	if _, writeErr := fmt.Fprintf(stdout, "%s\n", filepath.Join(suiteDir, "suite-manifest.json")); writeErr != nil {
		if suiteErr != nil {
			return errors.Join(suiteErr, fmt.Errorf("write suite result: %w", writeErr))
		}
		return fmt.Errorf("write suite result: %w", writeErr)
	}
	return suiteErr
}

func newSuiteID() string {
	return "bp05-suite-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}

func newSuiteManifest(options suiteExecutionOptions, startedAt time.Time) suiteManifest {
	return suiteManifest{
		SchemaVersion:              1,
		SuiteID:                    options.SuiteID,
		BenchmarkID:                "bp05",
		StartedAt:                  startedAt.UTC().Format(time.RFC3339Nano),
		ProfileSHA256:              options.ProfileSHA256,
		ResolvedProfile:            options.Profile,
		RequestedRepetitions:       options.Repetitions,
		OrderStrategy:              "alternating",
		ReleaseGating:              false,
		Git:                        options.Metadata.git,
		GoVersion:                  options.Metadata.goVersion,
		OS:                         runtime.GOOS,
		Architecture:               runtime.GOARCH,
		Docker:                     dockerMetadata{ClientServerVersion: options.Metadata.dockerVersion},
		BuildBaseImage:             buildBaseImageReference,
		PlannedBuildBaseImage:      plannedBuildBaseImage,
		BuildBaseImageSubstitution: "planned digest was unavailable in Docker Hub; immutable digest resolved from golang:1.26.5-bookworm on 2026-09-19",
		NaruMQImage:                options.Images.NaruMQReference,
		NaruMQImageID:              options.Images.NaruMQID,
		RunnerImage:                options.Images.RunnerReference,
		RunnerImageID:              options.Images.RunnerID,
		EMQX: emqxMetadata{
			Version:        emqxVersion,
			Edition:        emqxEdition,
			License:        emqxLicense,
			EOL:            emqxEOLEnd,
			ImageReference: emqxImageReference,
			ImageIndex:     "sha256:4593b6e30196754779fa14483332cb0be56c233f1c8a387b1308bec4337c0144",
			Historical:     true,
		},
		Runs: make([]suiteRun, 0, options.Repetitions),
	}
}

func finishSuitePreparationFailure(stdout io.Writer, suiteDir string, value suiteManifest, reason error) error {
	value.FailureReason = reason.Error()
	value.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	value.Valid = false
	var writeErrors []error
	if err := writeSuiteManifest(suiteDir, value); err != nil {
		writeErrors = append(writeErrors, fmt.Errorf("write suite manifest: %w", err))
	}
	if err := writeSuiteSummary(suiteDir, emptySuiteSummary(value, reason.Error())); err != nil {
		writeErrors = append(writeErrors, fmt.Errorf("write suite summary: %w", err))
	}
	if _, err := fmt.Fprintf(stdout, "%s\n", filepath.Join(suiteDir, "suite-manifest.json")); err != nil {
		writeErrors = append(writeErrors, fmt.Errorf("write suite result: %w", err))
	}
	return errors.Join(append([]error{reason}, writeErrors...)...)
}

func executeSuite(ctx context.Context, options suiteExecutionOptions, execute comparisonExecutor) (suiteManifest, suiteSummary, error) {
	value := newSuiteManifest(options, time.Now().UTC())
	summary := emptySuiteSummary(value, "")
	if execute == nil {
		return value, summary, errors.New("comparison executor is required")
	}
	if options.Repetitions < suiteartifact.MinimumRepetitions {
		return value, summary, fmt.Errorf("suite requires at least %d repetitions", suiteartifact.MinimumRepetitions)
	}
	if strings.TrimSpace(options.SuiteID) == "" || strings.TrimSpace(options.SuiteDir) == "" {
		return value, summary, errors.New("suite ID and directory are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := os.MkdirAll(options.SuiteDir, 0o755); err != nil {
		return value, summary, fmt.Errorf("create suite artifact directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(options.SuiteDir, "profile.json"), options.ProfileBytes, 0o644); err != nil {
		return value, summary, fmt.Errorf("write suite profile: %w", err)
	}
	if err := writeSuiteManifest(options.SuiteDir, value); err != nil {
		return value, summary, err
	}

	var failures []error
	var runData []suiteRunData
	for repetition := 1; repetition <= options.Repetitions; repetition++ {
		if err := ctx.Err(); err != nil {
			failure := fmt.Errorf("stop before repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			value.FailureReason = appendSuiteFailure(value.FailureReason, failure)
			break
		}
		targetOrder, err := suiteartifact.AlternatingTargetOrder(repetition)
		if err != nil {
			failures = append(failures, err)
			value.FailureReason = appendSuiteFailure(value.FailureReason, err)
			break
		}
		runID := fmt.Sprintf("%s-r%03d", options.SuiteID, repetition)
		runDir := filepath.Join(options.SuiteDir, "runs", fmt.Sprintf("%03d", repetition))
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			failure := fmt.Errorf("create repetition %d artifact directory: %w", repetition, err)
			failures = append(failures, failure)
			value.FailureReason = appendSuiteFailure(value.FailureReason, failure)
			break
		}
		startedAt := time.Now().UTC()
		childManifest, childComparison, runErr := execute(ctx, comparisonRunOptions{
			Profile:       options.Profile,
			ProfileBytes:  options.ProfileBytes,
			ProfileSHA256: options.ProfileSHA256,
			RepoRoot:      options.RepoRoot,
			RunID:         runID,
			RunDir:        runDir,
			Images:        options.Images,
			Metadata:      options.Metadata,
			Command:       options.Command,
			TargetOrder:   targetOrder,
		})
		finishedAt := time.Now().UTC()
		runSuccess := comparisonRunSucceeded(childManifest, childComparison, runErr)
		runFailure := comparisonRunFailure(childManifest, childComparison, runErr)
		record := suiteRun{
			Repetition:        repetition,
			RunID:             runID,
			StartedAt:         startedAt.Format(time.RFC3339Nano),
			FinishedAt:        finishedAt.Format(time.RFC3339Nano),
			TargetOrder:       append([]string(nil), targetOrder...),
			ArtifactDirectory: filepath.ToSlash(filepath.Join("runs", fmt.Sprintf("%03d", repetition))),
			Equivalent:        childComparison.Equivalent,
			Success:           runSuccess,
			FailureReason:     runFailure,
		}
		runData = append(runData, suiteRunData{repetition: repetition, runID: runID, manifest: childManifest, comparison: childComparison, success: runSuccess})
		value.Runs = append(value.Runs, record)
		value.CompletedRepetitions++
		if !runSuccess {
			failure := errors.New(runFailure)
			failures = append(failures, failure)
			value.FailureReason = appendSuiteFailure(value.FailureReason, failure)
		}
		if err := writeSuiteManifest(options.SuiteDir, value); err != nil {
			value.Runs = value.Runs[:len(value.Runs)-1]
			value.CompletedRepetitions--
			failure := fmt.Errorf("write suite manifest after repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			value.FailureReason = appendSuiteFailure(value.FailureReason, failure)
			break
		}
		if err := ctx.Err(); err != nil {
			failure := fmt.Errorf("stop after repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			value.FailureReason = appendSuiteFailure(value.FailureReason, failure)
			break
		}
	}

	value.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	summary = buildSuiteSummary(options, value, runData)
	if summary.FailureReason != "" {
		value.FailureReason = appendSuiteFailure(value.FailureReason, errors.New(summary.FailureReason))
	}
	value.Valid = summary.Valid
	if err := writeSuiteSummary(options.SuiteDir, summary); err != nil {
		failure := fmt.Errorf("write suite summary: %w", err)
		failures = append(failures, failure)
		summary.Valid = false
		summary.FailureReason = appendSuiteFailure(summary.FailureReason, failure)
		value.FailureReason = appendSuiteFailure(value.FailureReason, failure)
		value.Valid = false
	}
	if err := writeSuiteManifest(options.SuiteDir, value); err != nil {
		failure := fmt.Errorf("write final suite manifest: %w", err)
		failures = append(failures, failure)
		summary.Valid = false
		value.FailureReason = appendSuiteFailure(value.FailureReason, failure)
		value.Valid = false
	}
	if !summary.Valid {
		if summary.FailureReason == "" {
			failures = append(failures, errors.New("suite summary is invalid"))
		} else {
			failures = append(failures, errors.New(summary.FailureReason))
		}
	}
	return value, summary, errors.Join(failures...)
}

func comparisonRunSucceeded(value manifest, result comparison, runErr error) bool {
	if runErr != nil || !result.Equivalent {
		return false
	}
	for _, target := range []string{"narumq", "emqx"} {
		targetValue, ok := value.Targets[target]
		if !ok || !targetValue.Success {
			return false
		}
	}
	return true
}

func comparisonRunFailure(value manifest, result comparison, runErr error) string {
	var failures []error
	if runErr != nil {
		failures = append(failures, runErr)
	}
	if !result.Equivalent {
		reason := result.Reason
		if reason == "" {
			reason = "comparison was not equivalent"
		}
		failures = append(failures, errors.New(reason))
	}
	for _, target := range []string{"narumq", "emqx"} {
		targetValue, ok := value.Targets[target]
		if !ok {
			failures = append(failures, fmt.Errorf("%s target manifest is missing", target))
		} else if !targetValue.Success {
			reason := targetValue.FailureReason
			if reason == "" {
				reason = "target run failed"
			}
			failures = append(failures, fmt.Errorf("%s target: %s", target, reason))
		}
	}
	if len(failures) == 0 {
		return ""
	}
	return errors.Join(failures...).Error()
}

func buildSuiteSummary(options suiteExecutionOptions, value suiteManifest, runs []suiteRunData) suiteSummary {
	summary := emptySuiteSummary(value, "")
	summary.RepetitionCount = options.Repetitions
	for _, run := range runs {
		if run.success {
			summary.SuccessfulRepetitions++
		}
	}
	var failures []error
	if value.CompletedRepetitions != options.Repetitions {
		failures = append(failures, fmt.Errorf("completed repetitions %d, want %d", value.CompletedRepetitions, options.Repetitions))
	}
	if summary.SuccessfulRepetitions != options.Repetitions {
		failures = append(failures, fmt.Errorf("successful repetitions %d, want %d", summary.SuccessfulRepetitions, options.Repetitions))
	}
	if value.FailureReason != "" {
		failures = append(failures, errors.New(value.FailureReason))
	}

	targetSamples := map[string]map[string][]suiteartifact.MetricSample{
		"narumq": make(map[string][]suiteartifact.MetricSample),
		"emqx":   make(map[string][]suiteartifact.MetricSample),
	}
	runtimeSummaries := make([]runtimeMetricRun, 0, len(runs))
	cpuReasons := make([]suiteCPUReason, 0)
	allCPUMetricsAvailable := true
	allRuntimeSummariesPresent := true
	for _, run := range runs {
		if !run.success {
			continue
		}
		for _, target := range []string{"narumq", "emqx"} {
			targetValue, ok := run.manifest.Targets[target]
			if !ok {
				failures = append(failures, fmt.Errorf("repetition %d missing %s target", run.repetition, target))
				continue
			}
			metrics, runtimeSummary, err := bp05Metrics(target, targetValue)
			if err != nil {
				failures = append(failures, fmt.Errorf("repetition %d %s metrics: %w", run.repetition, target, err))
				continue
			}
			for key, metricValue := range metrics {
				targetSamples[target][key] = append(targetSamples[target][key], suiteartifact.MetricSample{Repetition: run.repetition, RunID: run.runID, Value: metricValue})
			}
			if target == "narumq" {
				runtimeSummaries = append(runtimeSummaries, runtimeMetricRun{repetition: run.repetition, runID: run.runID, summary: runtimeSummary})
				if !runtimeSummary.CPUMetricsAvailable {
					allCPUMetricsAvailable = false
					reason := runtimeSummary.CPUAvailabilityReason
					if reason == "" {
						reason = "CPU metrics unavailable"
					}
					cpuReasons = append(cpuReasons, suiteCPUReason{Repetition: run.repetition, RunID: run.runID, Reason: reason})
				}
			}
		}
	}
	if len(runtimeSummaries) != options.Repetitions {
		allRuntimeSummariesPresent = false
	}
	narumqSummary := summary.Targets["narumq"]
	narumqSummary.CPUMetricsReasons = cpuReasons
	if allRuntimeSummariesPresent {
		available := allCPUMetricsAvailable
		narumqSummary.CPUMetricsAvailable = &available
	}
	if allRuntimeSummariesPresent && allCPUMetricsAvailable {
		for _, runtimeRun := range runtimeSummaries {
			runtimeSummary := runtimeRun.summary
			cpuMetrics := map[string]float64{
				"gcCPUSeconds":      runtimeSummary.GCCPUSeconds,
				"runtimeCPUSeconds": runtimeSummary.RuntimeCPUSeconds,
				"gcCPUFraction":     runtimeSummary.GCCPUFraction,
			}
			for key, metricValue := range cpuMetrics {
				targetSamples["narumq"][key] = append(targetSamples["narumq"][key], suiteartifact.MetricSample{Repetition: runtimeRun.repetition, RunID: runtimeRun.runID, Value: metricValue})
			}
		}
	}
	summary.Targets["narumq"] = narumqSummary

	for _, target := range []string{"narumq", "emqx"} {
		targetSummary := summary.Targets[target]
		definitions := bp05SuiteMetricUnits
		if target == "narumq" {
			definitions = mergeMetricDefinitions(bp05SuiteMetricUnits, bp05RuntimeMetricUnits)
			if allRuntimeSummariesPresent && !allCPUMetricsAvailable {
				definitions = mergeMetricDefinitions(bp05SuiteMetricUnits, map[string]string{
					"allocationBytesPerSecond": "bytes/second",
					"allocationsPerSecond":     "allocations/second",
					"gcPauseP50Seconds":        "seconds",
					"gcPauseP95Seconds":        "seconds",
					"gcPauseP99Seconds":        "seconds",
					"gcPauseP999Seconds":       "seconds",
				})
			}
		}
		for key, unit := range definitions {
			samples := targetSamples[target][key]
			if len(samples) != options.Repetitions {
				failures = append(failures, fmt.Errorf("%s metric %s has %d samples, want %d", target, key, len(samples), options.Repetitions))
				continue
			}
			metricSummary, err := suiteartifact.SummarizeMetric(unit, samples)
			if err != nil {
				failures = append(failures, fmt.Errorf("summarize %s metric %s: %w", target, key, err))
				continue
			}
			targetSummary.Metrics[key] = metricSummary
		}
		summary.Targets[target] = targetSummary
	}
	if !allRuntimeSummariesPresent {
		failures = append(failures, errors.New("NaruMQ runtime summary is missing from one or more repetitions"))
	}
	if len(failures) > 0 {
		summary.FailureReason = errors.Join(failures...).Error()
		for target, targetSummary := range summary.Targets {
			targetSummary.Metrics = map[string]suiteartifact.MetricSummary{}
			summary.Targets[target] = targetSummary
		}
		return summary
	}
	summary.Valid = true
	return summary
}

func bp05Metrics(target string, value targetManifest) (map[string]float64, *runtimeartifact.Summary, error) {
	if !value.Success || value.WorkloadSummary == nil || !value.WorkloadSummary.Valid {
		return nil, nil, errors.New("workload summary is invalid")
	}
	if value.ResourceSummary == nil || !value.ResourceSummary.Valid {
		return nil, nil, errors.New("resource summary is invalid")
	}
	if !validRuntimeSummary(target, value.RuntimeSummary) {
		return nil, nil, errors.New("runtime summary is invalid or has unexpected applicability")
	}
	workload := value.WorkloadSummary
	initial := workload.InitialConnectLatency
	reconnect := workload.ReconnectLatency
	metrics := map[string]float64{
		"initialConnectionRatePerSecond":       workload.InitialConnectionRatePerSecond,
		"initialConnectLatencyP50Nanoseconds":  float64(initial.P50Nanoseconds),
		"initialConnectLatencyP95Nanoseconds":  float64(initial.P95Nanoseconds),
		"initialConnectLatencyP99Nanoseconds":  float64(initial.P99Nanoseconds),
		"initialConnectLatencyP999Nanoseconds": float64(initial.P999Nanoseconds),
		"reconnectRatePerSecond":               workload.ReconnectRatePerSecond,
		"reconnectLatencyP50Nanoseconds":       float64(reconnect.P50Nanoseconds),
		"reconnectLatencyP95Nanoseconds":       float64(reconnect.P95Nanoseconds),
		"reconnectLatencyP99Nanoseconds":       float64(reconnect.P99Nanoseconds),
		"reconnectLatencyP999Nanoseconds":      float64(reconnect.P999Nanoseconds),
		"maxScheduleLagNanoseconds":            float64(workload.MaxScheduleLagNanoseconds),
		"memoryBytesPerIdleConnection":         value.ResourceSummary.MemoryBytesPerIdleConnection,
	}
	if target == "narumq" {
		runtimeSummary := value.RuntimeSummary
		metrics["allocationBytesPerSecond"] = runtimeSummary.AllocationBytesPerSecond
		metrics["allocationsPerSecond"] = runtimeSummary.AllocationsPerSecond
		metrics["gcPauseP50Seconds"] = runtimeSummary.GCPauseP50Seconds
		metrics["gcPauseP95Seconds"] = runtimeSummary.GCPauseP95Seconds
		metrics["gcPauseP99Seconds"] = runtimeSummary.GCPauseP99Seconds
		metrics["gcPauseP999Seconds"] = runtimeSummary.GCPauseP999Seconds
		return metrics, runtimeSummary, nil
	}
	return metrics, value.RuntimeSummary, nil
}

func mergeMetricDefinitions(first, second map[string]string) map[string]string {
	merged := make(map[string]string, len(first)+len(second))
	for key, unit := range first {
		merged[key] = unit
	}
	for key, unit := range second {
		merged[key] = unit
	}
	return merged
}

func emptySuiteSummary(value suiteManifest, reason string) suiteSummary {
	return suiteSummary{
		SchemaVersion:   1,
		SuiteID:         value.SuiteID,
		BenchmarkID:     "bp05",
		RepetitionCount: value.RequestedRepetitions,
		ReleaseGating:   false,
		Valid:           false,
		Targets: map[string]suiteTargetSummary{
			"narumq": {Metrics: map[string]suiteartifact.MetricSummary{}, RuntimeApplicability: "required"},
			"emqx":   {Metrics: map[string]suiteartifact.MetricSummary{}, RuntimeApplicability: "not_applicable"},
		},
		FailureReason: reason,
	}
}

func appendSuiteFailure(existing string, err error) string {
	if err == nil {
		return existing
	}
	if existing == "" {
		return err.Error()
	}
	return errors.Join(errors.New(existing), err).Error()
}

func writeSuiteManifest(suiteDir string, value suiteManifest) error {
	return writeJSONFile(filepath.Join(suiteDir, "suite-manifest.json"), value)
}

func writeSuiteSummary(suiteDir string, value suiteSummary) error {
	return writeJSONFile(filepath.Join(suiteDir, "suite-summary.json"), value)
}
