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
	"narumq/tests/benchmark/internal/suiteartifact"
)

type bp04SuiteRun struct {
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

type bp04SuiteManifest struct {
	SchemaVersion         int                `json:"schemaVersion"`
	SuiteID               string             `json:"suiteId"`
	BenchmarkID           string             `json:"benchmarkId"`
	StartedAt             string             `json:"startedAt"`
	FinishedAt            string             `json:"finishedAt,omitempty"`
	ProfileSHA256         string             `json:"profileSha256"`
	ResolvedProfile       Profile            `json:"resolvedProfile"`
	RequestedRepetitions  int                `json:"requestedRepetitions"`
	CompletedRepetitions  int                `json:"completedRepetitions"`
	OrderStrategy         string             `json:"orderStrategy"`
	ReleaseGating         bool               `json:"releaseGating"`
	Git                   bp04GitMetadata    `json:"git"`
	GoVersion             string             `json:"goVersion"`
	OS                    string             `json:"os"`
	Architecture          string             `json:"architecture"`
	Docker                bp04DockerMetadata `json:"docker"`
	BuildBaseImage        string             `json:"buildBaseImage"`
	PlannedBuildBaseImage string             `json:"plannedBuildBaseImage"`
	NaruMQImage           string             `json:"narumqImage"`
	NaruMQImageID         string             `json:"narumqImageId"`
	RunnerImage           string             `json:"runnerImage"`
	RunnerImageID         string             `json:"runnerImageId"`
	EMQX                  bp04EMQXMetadata   `json:"emqx"`
	Topology              bp04Topology       `json:"topology"`
	Runs                  []bp04SuiteRun     `json:"runs"`
	Valid                 bool               `json:"valid"`
	FailureReason         string             `json:"failureReason,omitempty"`
}

type bp04SuiteTargetSummary struct {
	Metrics               map[string]suiteartifact.MetricSummary `json:"metrics"`
	NetworkStatsAvailable bool                                   `json:"networkStatsAvailable"`
}

type bp04SuiteSummary struct {
	SchemaVersion         int                               `json:"schemaVersion"`
	SuiteID               string                            `json:"suiteId"`
	BenchmarkID           string                            `json:"benchmarkId"`
	RepetitionCount       int                               `json:"repetitionCount"`
	SuccessfulRepetitions int                               `json:"successfulRepetitions"`
	ReleaseGating         bool                              `json:"releaseGating"`
	Valid                 bool                              `json:"valid"`
	Targets               map[string]bp04SuiteTargetSummary `json:"targets"`
	FailureReason         string                            `json:"failureReason,omitempty"`
}

type bp04SuiteOptions struct {
	Profile       Profile
	ProfileBytes  []byte
	ProfileSHA256 string
	RepoRoot      string
	SuiteID       string
	SuiteDir      string
	Repetitions   int
	Images        environment.ImageSet
	Metadata      bp04HostMetadata
	Command       []string
}

type bp04SuiteRunData struct {
	Repetition int
	RunID      string
	Manifest   bp04Manifest
	Success    bool
}

func suiteCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("suite", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-04 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp04", "root directory for benchmark artifacts")
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
	profileAbs, err := filepath.Abs(*profilePath)
	if err != nil {
		return fmt.Errorf("resolve profile path: %w", err)
	}
	outputAbs, err := filepath.Abs(*outputRoot)
	if err != nil {
		return fmt.Errorf("resolve output path: %w", err)
	}
	profile, profileBytes, profileHash, err := LoadProfile(profileAbs)
	if err != nil {
		return err
	}
	repoRoot, err := bp04RepositoryRoot()
	if err != nil {
		return err
	}
	suiteID := newBP04SuiteID()
	suiteDir := filepath.Join(outputAbs, suiteID)
	if err := os.MkdirAll(suiteDir, 0o755); err != nil {
		return fmt.Errorf("create BP-04 suite directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(suiteDir, "profile.json"), profileBytes, 0o644); err != nil {
		return fmt.Errorf("write suite profile: %w", err)
	}
	command := []string{"suite", "--profile", *profilePath, "--output", *outputRoot, "--repetitions", strconv.Itoa(*repetitions)}
	options := bp04SuiteOptions{
		Profile:       profile,
		ProfileBytes:  profileBytes,
		ProfileSHA256: profileHash,
		RepoRoot:      repoRoot,
		SuiteID:       suiteID,
		SuiteDir:      suiteDir,
		Repetitions:   *repetitions,
		Metadata:      collectBP04HostMetadata(repoRoot),
		Command:       command,
	}
	manifest := newBP04SuiteManifest(options, time.Now().UTC())
	summary := emptyBP04SuiteSummary(manifest, "")
	if err := writeBP04SuiteArtifacts(suiteDir, manifest, summary); err != nil {
		return err
	}
	if err := environment.EnsureEMQXImage(ctx, suiteDir, bp04EMQXImageReference); err != nil {
		return finishBP04SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	images, err := environment.BuildImages(ctx, environment.BuildOptions{
		BenchmarkID:  "bp04",
		RunID:        suiteID,
		RepoRoot:     repoRoot,
		ArtifactDir:  suiteDir,
		RunnerTarget: "bp04-runner",
	})
	if err != nil {
		return finishBP04SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	options.Images = images
	manifest.NaruMQImage = images.NaruMQReference
	manifest.NaruMQImageID = images.NaruMQID
	manifest.RunnerImage = images.RunnerReference
	manifest.RunnerImageID = images.RunnerID
	if err := writeBP04SuiteArtifacts(suiteDir, manifest, summary); err != nil {
		return finishBP04SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	final, finalSummary, suiteErr := executeBP04Suite(ctx, options)
	if writeErr := writeBP04SuiteArtifacts(suiteDir, final, finalSummary); writeErr != nil {
		suiteErr = errors.Join(suiteErr, fmt.Errorf("write suite artifacts: %w", writeErr))
	}
	if _, err := fmt.Fprintln(stdout, filepath.Join(suiteDir, "suite-manifest.json")); err != nil {
		suiteErr = errors.Join(suiteErr, fmt.Errorf("write suite result: %w", err))
	}
	return suiteErr
}

func newBP04SuiteID() string {
	return "bp04-suite-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}

func newBP04SuiteManifest(options bp04SuiteOptions, startedAt time.Time) bp04SuiteManifest {
	return bp04SuiteManifest{
		SchemaVersion:         1,
		SuiteID:               options.SuiteID,
		BenchmarkID:           "bp04",
		StartedAt:             startedAt.UTC().Format(time.RFC3339Nano),
		ProfileSHA256:         options.ProfileSHA256,
		ResolvedProfile:       options.Profile,
		RequestedRepetitions:  options.Repetitions,
		OrderStrategy:         "alternating target order; odd repetitions run NaruMQ then EMQX, even repetitions run EMQX then NaruMQ",
		ReleaseGating:         false,
		Git:                   options.Metadata.Git,
		GoVersion:             options.Metadata.GoVersion,
		OS:                    runtime.GOOS,
		Architecture:          runtime.GOARCH,
		Docker:                bp04DockerMetadata{ClientServerVersion: options.Metadata.DockerVersion},
		BuildBaseImage:        environment.BuildBaseImageReference,
		PlannedBuildBaseImage: environment.PlannedBuildBaseImage,
		NaruMQImage:           options.Images.NaruMQReference,
		NaruMQImageID:         options.Images.NaruMQID,
		RunnerImage:           options.Images.RunnerReference,
		RunnerImageID:         options.Images.RunnerID,
		EMQX:                  bp04EMQXMetadata{Version: "5.8.8", Edition: "Open Source", ImageReference: bp04EMQXImageReference, ImageIndex: strings.SplitN(bp04EMQXImageReference, "@", 2)[1]},
		Topology: bp04Topology{
			Nodes: bp04NodeCount, NetworkMode: bp04NetworkMode,
			NaruMQStorage:   "Raft-backed cluster with per-node Docker named volumes",
			EMQXStorage:     "static EMQX OSS cluster with per-node Docker named volumes; storage equivalence is not claimed",
			RouteVisibility: "QoS 0 publish-to-subscribe visibility measured from SUBSCRIBE write to first delivery",
		},
		Runs:  make([]bp04SuiteRun, 0, options.Repetitions),
		Valid: false,
	}
}

func executeBP04Suite(ctx context.Context, options bp04SuiteOptions) (bp04SuiteManifest, bp04SuiteSummary, error) {
	manifest := newBP04SuiteManifest(options, time.Now().UTC())
	summary := emptyBP04SuiteSummary(manifest, "")
	if options.Repetitions < suiteartifact.MinimumRepetitions {
		return manifest, summary, fmt.Errorf("suite requires at least %d repetitions", suiteartifact.MinimumRepetitions)
	}
	if strings.TrimSpace(options.SuiteID) == "" || strings.TrimSpace(options.SuiteDir) == "" {
		return manifest, summary, errors.New("suite ID and directory are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := os.MkdirAll(options.SuiteDir, 0o755); err != nil {
		return manifest, summary, fmt.Errorf("create suite directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(options.SuiteDir, "profile.json"), options.ProfileBytes, 0o644); err != nil {
		return manifest, summary, fmt.Errorf("write suite profile: %w", err)
	}
	if err := writeBP04SuiteArtifacts(options.SuiteDir, manifest, summary); err != nil {
		return manifest, summary, err
	}
	var failures []error
	var runs []bp04SuiteRunData
	for repetition := 1; repetition <= options.Repetitions; repetition++ {
		if err := ctx.Err(); err != nil {
			failure := fmt.Errorf("stop before repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP04SuiteFailure(manifest.FailureReason, failure)
			break
		}
		order, err := suiteartifact.AlternatingTargetOrder(repetition)
		if err != nil {
			failures = append(failures, err)
			manifest.FailureReason = appendBP04SuiteFailure(manifest.FailureReason, err)
			break
		}
		runID := fmt.Sprintf("%s-r%03d", options.SuiteID, repetition)
		runDir := filepath.Join(options.SuiteDir, "runs", fmt.Sprintf("%03d", repetition))
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			failure := fmt.Errorf("create repetition %d directory: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP04SuiteFailure(manifest.FailureReason, failure)
			break
		}
		if err := os.WriteFile(filepath.Join(runDir, "profile.json"), options.ProfileBytes, 0o644); err != nil {
			failure := fmt.Errorf("write repetition %d profile: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP04SuiteFailure(manifest.FailureReason, failure)
			break
		}
		startedAt := time.Now().UTC()
		childOptions := bp04RunOptions{
			Profile:       options.Profile,
			ProfileSHA256: options.ProfileSHA256,
			RunID:         runID,
			RunDir:        runDir,
			Images:        options.Images,
			Metadata:      options.Metadata,
			Command:       append([]string(nil), options.Command...),
			TargetOrder:   order,
		}
		childManifest := newBP04Manifest(childOptions, startedAt)
		childManifest.RunnerImage = options.Images.RunnerReference
		childManifest.RunnerImageID = options.Images.RunnerID
		childManifest, runErr := executeBP04Comparison(ctx, childOptions, childManifest)
		childSummary := summarizeBP04Run(childManifest)
		comparison := buildBP04Comparison(childManifest, childSummary)
		if err := writeBP04RunArtifacts(runDir, childManifest); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("write repetition artifacts: %w", err))
		}
		finishedAt := time.Now().UTC()
		success := bp04SuiteRunSucceeded(childManifest, comparison, childSummary, runErr)
		failureReason := bp04SuiteRunFailure(childManifest, comparison, childSummary, runErr)
		run := bp04SuiteRun{
			Repetition:        repetition,
			RunID:             runID,
			StartedAt:         startedAt.Format(time.RFC3339Nano),
			FinishedAt:        finishedAt.Format(time.RFC3339Nano),
			TargetOrder:       append([]string(nil), order...),
			ArtifactDirectory: filepath.ToSlash(filepath.Join("runs", fmt.Sprintf("%03d", repetition))),
			Equivalent:        comparison.Equivalent,
			Success:           success,
			FailureReason:     failureReason,
		}
		runs = append(runs, bp04SuiteRunData{Repetition: repetition, RunID: runID, Manifest: childManifest, Success: success})
		manifest.Runs = append(manifest.Runs, run)
		manifest.CompletedRepetitions++
		if !success {
			failure := errors.New(failureReason)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP04SuiteFailure(manifest.FailureReason, failure)
		}
		if err := writeBP04SuiteArtifacts(options.SuiteDir, manifest, summary); err != nil {
			failure := fmt.Errorf("write suite manifest after repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP04SuiteFailure(manifest.FailureReason, failure)
			break
		}
		if err := ctx.Err(); err != nil {
			failure := fmt.Errorf("stop after repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP04SuiteFailure(manifest.FailureReason, failure)
			break
		}
	}
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	summary = buildBP04SuiteSummary(options, manifest, runs)
	manifest.Valid = summary.Valid
	if summary.FailureReason != "" {
		manifest.FailureReason = appendBP04SuiteFailure(manifest.FailureReason, errors.New(summary.FailureReason))
	}
	if err := writeBP04SuiteSummary(options.SuiteDir, summary); err != nil {
		failure := fmt.Errorf("write final suite summary: %w", err)
		failures = append(failures, failure)
		summary.Valid = false
		summary.FailureReason = appendBP04SuiteFailure(summary.FailureReason, failure)
		manifest.Valid = false
		manifest.FailureReason = appendBP04SuiteFailure(manifest.FailureReason, failure)
	}
	if err := writeBP04SuiteManifest(options.SuiteDir, manifest); err != nil {
		failure := fmt.Errorf("write final suite manifest: %w", err)
		failures = append(failures, failure)
		manifest.Valid = false
		manifest.FailureReason = appendBP04SuiteFailure(manifest.FailureReason, failure)
	}
	if !summary.Valid {
		if summary.FailureReason == "" {
			failures = append(failures, errors.New("suite summary is invalid"))
		} else {
			failures = append(failures, errors.New(summary.FailureReason))
		}
	}
	return manifest, summary, errors.Join(failures...)
}

func bp04SuiteRunSucceeded(manifest bp04Manifest, comparison bp04Comparison, summary bp04Summary, runErr error) bool {
	if runErr != nil || !comparison.Equivalent || !summary.Valid {
		return false
	}
	for _, target := range []string{"narumq", "emqx"} {
		entry, ok := manifest.Targets[target]
		if !ok || !entry.Success || !entry.Valid {
			return false
		}
	}
	return true
}

func bp04SuiteRunFailure(manifest bp04Manifest, comparison bp04Comparison, summary bp04Summary, runErr error) string {
	var failures []error
	if runErr != nil {
		failures = append(failures, runErr)
	}
	if !comparison.Equivalent {
		failures = append(failures, errors.New("target profile/resource/runner/topology equivalence failed"))
	}
	if !summary.Valid && summary.FailureReason != "" {
		failures = append(failures, errors.New(summary.FailureReason))
	}
	for _, target := range []string{"narumq", "emqx"} {
		entry, ok := manifest.Targets[target]
		if !ok {
			failures = append(failures, fmt.Errorf("%s target manifest is missing", target))
		} else if !entry.Valid || !entry.Success {
			reason := entry.FailureReason
			if reason == "" {
				reason = "target is invalid"
			}
			failures = append(failures, fmt.Errorf("%s target: %s", target, reason))
		}
	}
	if len(failures) == 0 {
		return ""
	}
	return errors.Join(failures...).Error()
}

func buildBP04SuiteSummary(options bp04SuiteOptions, manifest bp04SuiteManifest, runs []bp04SuiteRunData) bp04SuiteSummary {
	summary := emptyBP04SuiteSummary(manifest, "")
	summary.RepetitionCount = options.Repetitions
	var failures []error
	if manifest.CompletedRepetitions != options.Repetitions {
		failures = append(failures, fmt.Errorf("completed repetitions %d, want %d", manifest.CompletedRepetitions, options.Repetitions))
	}
	if len(runs) != options.Repetitions {
		failures = append(failures, fmt.Errorf("suite has %d run results, want %d", len(runs), options.Repetitions))
	}
	if manifest.FailureReason != "" {
		failures = append(failures, errors.New(manifest.FailureReason))
	}
	for _, run := range runs {
		if run.Success {
			summary.SuccessfulRepetitions++
		} else {
			failures = append(failures, fmt.Errorf("repetition %d did not produce two valid targets", run.Repetition))
		}
	}
	if summary.SuccessfulRepetitions != options.Repetitions {
		failures = append(failures, fmt.Errorf("successful repetitions %d, want %d", summary.SuccessfulRepetitions, options.Repetitions))
	}
	networkAvailable := summary.SuccessfulRepetitions == options.Repetitions && len(runs) == options.Repetitions
	for _, run := range runs {
		if !run.Success {
			continue
		}
		for _, target := range []string{"narumq", "emqx"} {
			entry, ok := run.Manifest.Targets[target]
			if !ok || entry.ResourceSummary == nil || !entry.ResourceSummary.NetworkStatsAvailable {
				networkAvailable = false
			}
		}
	}
	targetSamples := map[string]map[string][]suiteartifact.MetricSample{
		"narumq": make(map[string][]suiteartifact.MetricSample),
		"emqx":   make(map[string][]suiteartifact.MetricSample),
	}
	for _, run := range runs {
		if !run.Success {
			continue
		}
		for _, target := range []string{"narumq", "emqx"} {
			entry, ok := run.Manifest.Targets[target]
			if !ok {
				failures = append(failures, fmt.Errorf("repetition %d missing %s target", run.Repetition, target))
				continue
			}
			metrics, err := bp04Metrics(target, entry)
			if err != nil {
				failures = append(failures, fmt.Errorf("repetition %d %s metrics: %w", run.Repetition, target, err))
				continue
			}
			if !networkAvailable {
				for key := range metrics {
					if strings.Contains(key, "_network_tx_bytes_per_delivery") {
						delete(metrics, key)
					}
				}
			}
			for key, value := range metrics {
				targetSamples[target][key] = append(targetSamples[target][key], suiteartifact.MetricSample{Repetition: run.Repetition, RunID: run.RunID, Value: value})
			}
		}
	}
	definitions := bp04SuiteMetricUnits(options.Profile, networkAvailable)
	for _, target := range []string{"narumq", "emqx"} {
		targetSummary := summary.Targets[target]
		targetSummary.NetworkStatsAvailable = networkAvailable
		for key, unit := range definitions {
			samples := targetSamples[target][key]
			if len(samples) != options.Repetitions {
				failures = append(failures, fmt.Errorf("%s metric %s has %d samples, want %d", target, key, len(samples), options.Repetitions))
				continue
			}
			metric, err := suiteartifact.SummarizeMetric(unit, samples)
			if err != nil {
				failures = append(failures, fmt.Errorf("summarize %s metric %s: %w", target, key, err))
				continue
			}
			targetSummary.Metrics[key] = metric
		}
		summary.Targets[target] = targetSummary
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

func emptyBP04SuiteSummary(manifest bp04SuiteManifest, reason string) bp04SuiteSummary {
	return bp04SuiteSummary{
		SchemaVersion:   1,
		SuiteID:         manifest.SuiteID,
		BenchmarkID:     "bp04",
		RepetitionCount: manifest.RequestedRepetitions,
		ReleaseGating:   false,
		Valid:           false,
		Targets: map[string]bp04SuiteTargetSummary{
			"narumq": {Metrics: map[string]suiteartifact.MetricSummary{}, NetworkStatsAvailable: true},
			"emqx":   {Metrics: map[string]suiteartifact.MetricSummary{}, NetworkStatsAvailable: true},
		},
		FailureReason: reason,
	}
}

func finishBP04SuitePreparationFailure(stdout io.Writer, suiteDir string, manifest bp04SuiteManifest, cause error) error {
	manifest.FailureReason = cause.Error()
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	manifest.Valid = false
	summary := emptyBP04SuiteSummary(manifest, cause.Error())
	writeErr := writeBP04SuiteArtifacts(suiteDir, manifest, summary)
	_, outputErr := fmt.Fprintln(stdout, filepath.Join(suiteDir, "suite-manifest.json"))
	return errors.Join(cause, writeErr, outputErr)
}

func appendBP04SuiteFailure(existing string, err error) string {
	if err == nil {
		return existing
	}
	if existing == "" {
		return err.Error()
	}
	return errors.Join(errors.New(existing), err).Error()
}

func writeBP04SuiteArtifacts(directory string, manifest bp04SuiteManifest, summary bp04SuiteSummary) error {
	if err := writeBP04SuiteJSON(filepath.Join(directory, "suite-summary.json"), summary); err != nil {
		return err
	}
	return writeBP04SuiteJSON(filepath.Join(directory, "suite-manifest.json"), manifest)
}

func writeBP04SuiteManifest(directory string, value bp04SuiteManifest) error {
	return writeBP04SuiteJSON(filepath.Join(directory, "suite-manifest.json"), value)
}

func writeBP04SuiteSummary(directory string, value bp04SuiteSummary) error {
	return writeBP04SuiteJSON(filepath.Join(directory, "suite-summary.json"), value)
}

func writeBP04SuiteJSON(path string, value any) error {
	return writeBP04JSON(path, value)
}
