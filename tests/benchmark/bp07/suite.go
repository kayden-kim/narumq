package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"narumq/tests/benchmark/internal/environment"
	"narumq/tests/benchmark/internal/suiteartifact"
)

type bp07SuiteRun struct {
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

type bp07SuiteManifest struct {
	SchemaVersion              int                `json:"schemaVersion"`
	SuiteID                    string             `json:"suiteId"`
	BenchmarkID                string             `json:"benchmarkId"`
	StartedAt                  string             `json:"startedAt"`
	FinishedAt                 string             `json:"finishedAt,omitempty"`
	ProfileSHA256              string             `json:"profileSha256"`
	ResolvedProfile            Profile            `json:"resolvedProfile"`
	RequestedRepetitions       int                `json:"requestedRepetitions"`
	CompletedRepetitions       int                `json:"completedRepetitions"`
	OrderStrategy              string             `json:"orderStrategy"`
	ReleaseGating              bool               `json:"releaseGating"`
	Git                        bp07GitMetadata    `json:"git"`
	GoVersion                  string             `json:"goVersion"`
	OS                         string             `json:"os"`
	Architecture               string             `json:"architecture"`
	Docker                     bp07DockerMetadata `json:"docker"`
	BuildBaseImage             string             `json:"buildBaseImage"`
	PlannedBuildBaseImage      string             `json:"plannedBuildBaseImage"`
	BuildBaseImageSubstitution string             `json:"buildBaseImageSubstitution"`
	NaruMQImage                string             `json:"narumqImage"`
	NaruMQImageID              string             `json:"narumqImageId"`
	RunnerImage                string             `json:"runnerImage"`
	RunnerImageID              string             `json:"runnerImageId"`
	EMQX                       bp07EMQXMetadata   `json:"emqx"`
	Topology                   bp07Topology       `json:"topology"`
	Runs                       []bp07SuiteRun     `json:"runs"`
	Valid                      bool               `json:"valid"`
	FailureReason              string             `json:"failureReason,omitempty"`
}

type bp07SuiteTargetSummary struct {
	Metrics               map[string]suiteartifact.MetricSummary `json:"metrics"`
	NetworkStatsAvailable bool                                   `json:"networkStatsAvailable"`
}

type bp07SuiteSummary struct {
	SchemaVersion         int                               `json:"schemaVersion"`
	SuiteID               string                            `json:"suiteId"`
	BenchmarkID           string                            `json:"benchmarkId"`
	RepetitionCount       int                               `json:"repetitionCount"`
	SuccessfulRepetitions int                               `json:"successfulRepetitions"`
	ReleaseGating         bool                              `json:"releaseGating"`
	Valid                 bool                              `json:"valid"`
	Targets               map[string]bp07SuiteTargetSummary `json:"targets"`
	FailureReason         string                            `json:"failureReason,omitempty"`
}

type bp07SuiteOptions struct {
	Profile       Profile
	ProfileBytes  []byte
	ProfileSHA256 string
	RepoRoot      string
	SuiteID       string
	SuiteDir      string
	Repetitions   int
	Images        environment.ImageSet
	Metadata      bp07HostMetadata
	Command       []string
}

type bp07SuiteRunData struct {
	Repetition int
	RunID      string
	Manifest   bp07Manifest
	Comparison bp07Comparison
	Success    bool
}

func suiteCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("suite", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-07 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp07", "root directory for benchmark artifacts")
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
	repoRoot, err := bp07RepositoryRoot()
	if err != nil {
		return err
	}
	suiteID := newBP07SuiteID()
	suiteDir := filepath.Join(outputAbs, suiteID)
	if err := os.MkdirAll(suiteDir, 0o755); err != nil {
		return fmt.Errorf("create BP-07 suite directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(suiteDir, "profile.json"), profileBytes, 0o644); err != nil {
		return fmt.Errorf("write suite profile: %w", err)
	}
	command := append([]string{"suite"}, args...)
	options := bp07SuiteOptions{
		Profile:       profile,
		ProfileBytes:  profileBytes,
		ProfileSHA256: profileHash,
		RepoRoot:      repoRoot,
		SuiteID:       suiteID,
		SuiteDir:      suiteDir,
		Repetitions:   *repetitions,
		Metadata:      collectBP07HostMetadata(repoRoot),
		Command:       command,
	}
	manifest := newBP07SuiteManifest(options, time.Now().UTC())
	summary := emptyBP07SuiteSummary(manifest, "")
	if err := writeBP07SuiteArtifacts(suiteDir, manifest, summary); err != nil {
		return err
	}
	if err := environment.EnsureEMQXImage(ctx, suiteDir, bp07EMQXImageReference); err != nil {
		return finishBP07SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	images, err := environment.BuildImages(ctx, environment.BuildOptions{
		BenchmarkID:  "bp07",
		RunID:        suiteID,
		RepoRoot:     repoRoot,
		ArtifactDir:  suiteDir,
		RunnerTarget: "bp07-runner",
	})
	if err != nil {
		return finishBP07SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	options.Images = images
	manifest.NaruMQImage = images.NaruMQReference
	manifest.NaruMQImageID = images.NaruMQID
	manifest.RunnerImage = images.RunnerReference
	manifest.RunnerImageID = images.RunnerID
	if err := writeBP07SuiteArtifacts(suiteDir, manifest, summary); err != nil {
		return finishBP07SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	final, finalSummary, suiteErr := executeBP07Suite(ctx, options)
	if writeErr := writeBP07SuiteArtifacts(suiteDir, final, finalSummary); writeErr != nil {
		if suiteErr != nil {
			return errors.Join(suiteErr, fmt.Errorf("write suite artifacts: %w", writeErr))
		}
		return fmt.Errorf("write suite artifacts: %w", writeErr)
	}
	if _, err := fmt.Fprintln(stdout, filepath.Join(suiteDir, "suite-manifest.json")); err != nil {
		if suiteErr != nil {
			return errors.Join(suiteErr, fmt.Errorf("write suite result: %w", err))
		}
		return fmt.Errorf("write suite result: %w", err)
	}
	return suiteErr
}

func newBP07SuiteID() string {
	return "bp07-suite-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}

func newBP07SuiteManifest(options bp07SuiteOptions, startedAt time.Time) bp07SuiteManifest {
	base := newBP07Manifest(bp07RunOptions{
		Profile:       options.Profile,
		ProfileBytes:  options.ProfileBytes,
		ProfileSHA256: options.ProfileSHA256,
		RepoRoot:      options.RepoRoot,
		RunID:         options.SuiteID,
		RunDir:        options.SuiteDir,
		Images:        options.Images,
		Metadata:      options.Metadata,
		Command:       options.Command,
	}, startedAt)
	return bp07SuiteManifest{
		SchemaVersion:              1,
		SuiteID:                    options.SuiteID,
		BenchmarkID:                "bp07",
		StartedAt:                  startedAt.UTC().Format(time.RFC3339Nano),
		ProfileSHA256:              options.ProfileSHA256,
		ResolvedProfile:            options.Profile,
		RequestedRepetitions:       options.Repetitions,
		OrderStrategy:              "alternating",
		ReleaseGating:              false,
		Git:                        base.Git,
		GoVersion:                  base.GoVersion,
		OS:                         base.OS,
		Architecture:               base.Architecture,
		Docker:                     base.Docker,
		BuildBaseImage:             base.BuildBaseImage,
		PlannedBuildBaseImage:      base.PlannedBuildBaseImage,
		BuildBaseImageSubstitution: base.BuildBaseImageSubstitution,
		NaruMQImage:                options.Images.NaruMQReference,
		NaruMQImageID:              options.Images.NaruMQID,
		RunnerImage:                options.Images.RunnerReference,
		RunnerImageID:              options.Images.RunnerID,
		EMQX:                       base.EMQX,
		Topology:                   base.Topology,
		Runs:                       make([]bp07SuiteRun, 0, options.Repetitions),
	}
}

func executeBP07Suite(ctx context.Context, options bp07SuiteOptions) (bp07SuiteManifest, bp07SuiteSummary, error) {
	manifest := newBP07SuiteManifest(options, time.Now().UTC())
	summary := emptyBP07SuiteSummary(manifest, "")
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
	if err := writeBP07SuiteArtifacts(options.SuiteDir, manifest, summary); err != nil {
		return manifest, summary, err
	}
	var failures []error
	var runs []bp07SuiteRunData
	for repetition := 1; repetition <= options.Repetitions; repetition++ {
		if err := ctx.Err(); err != nil {
			failure := fmt.Errorf("stop before repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP07SuiteFailure(manifest.FailureReason, failure)
			break
		}
		order, err := suiteartifact.AlternatingTargetOrder(repetition)
		if err != nil {
			failures = append(failures, err)
			manifest.FailureReason = appendBP07SuiteFailure(manifest.FailureReason, err)
			break
		}
		runID := fmt.Sprintf("%s-r%03d", options.SuiteID, repetition)
		runDir := filepath.Join(options.SuiteDir, "runs", fmt.Sprintf("%03d", repetition))
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			failure := fmt.Errorf("create repetition %d directory: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP07SuiteFailure(manifest.FailureReason, failure)
			break
		}
		startedAt := time.Now().UTC()
		childOptions := bp07RunOptions{
			Profile:       options.Profile,
			ProfileBytes:  options.ProfileBytes,
			ProfileSHA256: options.ProfileSHA256,
			RepoRoot:      options.RepoRoot,
			RunID:         runID,
			RunDir:        runDir,
			Images:        options.Images,
			Metadata:      options.Metadata,
			Command:       append([]string(nil), options.Command...),
			TargetOrder:   order,
		}
		childManifest := newBP07Manifest(childOptions, startedAt)
		childManifest.RunnerImage = options.Images.RunnerReference
		childManifest.RunnerImageID = options.Images.RunnerID
		childManifest, runErr := executeBP07Comparison(ctx, childOptions, childManifest)
		childSummary := summarizeBP07Run(childManifest)
		comparison := buildBP07Comparison(childManifest, childSummary)
		if err := writeBP07RunArtifacts(runDir, childManifest); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("write repetition artifacts: %w", err))
		}
		finishedAt := time.Now().UTC()
		success := bp07SuiteRunSucceeded(childManifest, comparison, childSummary, runErr)
		failureReason := bp07SuiteRunFailure(childManifest, comparison, childSummary, runErr)
		run := bp07SuiteRun{
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
		runs = append(runs, bp07SuiteRunData{Repetition: repetition, RunID: runID, Manifest: childManifest, Comparison: comparison, Success: success})
		manifest.Runs = append(manifest.Runs, run)
		manifest.CompletedRepetitions++
		if !success {
			failure := errors.New(failureReason)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP07SuiteFailure(manifest.FailureReason, failure)
		}
		if err := writeBP07SuiteArtifacts(options.SuiteDir, manifest, summary); err != nil {
			failure := fmt.Errorf("write suite manifest after repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP07SuiteFailure(manifest.FailureReason, failure)
			break
		}
		if err := ctx.Err(); err != nil {
			failure := fmt.Errorf("stop after repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP07SuiteFailure(manifest.FailureReason, failure)
			break
		}
	}
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	summary = buildBP07SuiteSummary(options, manifest, runs)
	manifest.Valid = summary.Valid
	if summary.FailureReason != "" {
		manifest.FailureReason = appendBP07SuiteFailure(manifest.FailureReason, errors.New(summary.FailureReason))
	}
	if err := writeBP07SuiteSummary(options.SuiteDir, summary); err != nil {
		failure := fmt.Errorf("write final suite summary: %w", err)
		failures = append(failures, failure)
		summary.Valid = false
		summary.FailureReason = appendBP07SuiteFailure(summary.FailureReason, failure)
		manifest.Valid = false
		manifest.FailureReason = appendBP07SuiteFailure(manifest.FailureReason, failure)
	}
	if err := writeBP07SuiteManifest(options.SuiteDir, manifest); err != nil {
		failure := fmt.Errorf("write final suite manifest: %w", err)
		failures = append(failures, failure)
		manifest.Valid = false
		manifest.FailureReason = appendBP07SuiteFailure(manifest.FailureReason, failure)
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

func bp07SuiteRunSucceeded(manifest bp07Manifest, comparison bp07Comparison, summary bp07Summary, runErr error) bool {
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

func bp07SuiteRunFailure(manifest bp07Manifest, comparison bp07Comparison, summary bp07Summary, runErr error) string {
	var failures []error
	if runErr != nil {
		failures = append(failures, runErr)
	}
	if !comparison.Equivalent {
		failures = append(failures, errors.New("target resource/profile/runner/topology equivalence failed"))
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

func bp07Metrics(target string, value bp07TargetManifest) (map[string]float64, error) {
	if target != "narumq" && target != "emqx" {
		return nil, fmt.Errorf("unsupported BP-07 target %q", target)
	}
	if !value.Success || !value.Valid || value.WorkloadSummary == nil || !value.WorkloadSummary.Valid {
		return nil, errors.New("target workload summary is invalid")
	}
	if value.ResourceSummary == nil || !value.ResourceSummary.Valid {
		return nil, errors.New("target resource summary is invalid")
	}
	if value.RollingSummary == nil || !value.RollingSummary.Valid {
		return nil, errors.New("target rolling summary is invalid")
	}
	workload := value.WorkloadSummary
	rolling := value.RollingSummary
	wantRounds := value.ResolvedProfile.rounds()
	// Profile validation limits rolling to thirty rounds.
	var workloadRounds [30]*roundSummary
	var rollingSeen [30]bool
	if wantRounds < 1 || wantRounds > len(workloadRounds) || len(workload.Rounds) != wantRounds || len(rolling.Rounds) != wantRounds {
		return nil, errors.New("workload and rolling summaries must cover every profile round")
	}
	for index := range workload.Rounds {
		round := &workload.Rounds[index]
		if round.Round < 1 || round.Round > wantRounds || workloadRounds[round.Round-1] != nil {
			return nil, fmt.Errorf("unexpected or duplicate workload round %d", round.Round)
		}
		workloadRounds[round.Round-1] = round
	}
	metrics := make(map[string]float64, wantRounds*3+3)
	for _, round := range rolling.Rounds {
		if round.Round < 1 || round.Round > wantRounds || rollingSeen[round.Round-1] {
			return nil, fmt.Errorf("unexpected or duplicate rolling round %d", round.Round)
		}
		rollingSeen[round.Round-1] = true
		prefix := fmt.Sprintf("round%02d_", round.Round)
		metrics[prefix+"drain_millis"] = round.DrainMillis
		metrics[prefix+"restart_ready_millis"] = round.RestartReadyMillis
		metrics[prefix+"reconnect_latency_max_millis"] = workloadRounds[round.Round-1].ReconnectLatencyMillis.Max
	}
	var reconnects uint64
	for _, round := range workload.Rounds {
		reconnects += round.Reconnects
	}
	metrics["reconnects_total"] = float64(reconnects)
	metrics["qos1_duplicates_total"] = float64(workload.Totals.QoS1Duplicates)
	var qos0Lost uint64
	qos0Streams := 0
	for _, stream := range workload.Streams {
		if stream.QoS != 0 {
			continue
		}
		qos0Streams++
		qos0Lost += stream.QoS0Lost
	}
	if qos0Streams == 0 {
		return nil, errors.New("workload has no QoS 0 stream statistics")
	}
	metrics["qos0_lost_total"] = float64(qos0Lost)
	return metrics, nil
}

func bp07SuiteMetricUnits(profile Profile) map[string]string {
	units := make(map[string]string, profile.rounds()*3+3)
	for round := 1; round <= profile.rounds(); round++ {
		prefix := fmt.Sprintf("round%02d_", round)
		units[prefix+"drain_millis"] = "ms"
		units[prefix+"restart_ready_millis"] = "ms"
		units[prefix+"reconnect_latency_max_millis"] = "ms"
	}
	units["reconnects_total"] = "connections"
	units["qos1_duplicates_total"] = "messages"
	units["qos0_lost_total"] = "messages"
	return units
}

func buildBP07SuiteSummary(options bp07SuiteOptions, manifest bp07SuiteManifest, runs []bp07SuiteRunData) bp07SuiteSummary {
	summary := emptyBP07SuiteSummary(manifest, "")
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
	targetSamples := map[string]map[string][]suiteartifact.MetricSample{
		"narumq": make(map[string][]suiteartifact.MetricSample),
		"emqx":   make(map[string][]suiteartifact.MetricSample),
	}
	networkAvailable := map[string]bool{"narumq": true, "emqx": true}
	for _, run := range runs {
		if run.Success {
			summary.SuccessfulRepetitions++
		}
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
			if entry.ResourceSummary == nil || !entry.ResourceSummary.NetworkStatsAvailable {
				networkAvailable[target] = false
			}
			metrics, err := bp07Metrics(target, entry)
			if err != nil {
				failures = append(failures, fmt.Errorf("repetition %d %s metrics: %w", run.Repetition, target, err))
				continue
			}
			for key, value := range metrics {
				targetSamples[target][key] = append(targetSamples[target][key], suiteartifact.MetricSample{Repetition: run.Repetition, RunID: run.RunID, Value: value})
			}
		}
	}
	definitions := bp07SuiteMetricUnits(options.Profile)
	for _, target := range []string{"narumq", "emqx"} {
		targetSummary := summary.Targets[target]
		targetSummary.NetworkStatsAvailable = networkAvailable[target]
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
	if summary.SuccessfulRepetitions != options.Repetitions {
		failures = append(failures, fmt.Errorf("successful repetitions %d, want %d", summary.SuccessfulRepetitions, options.Repetitions))
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

func emptyBP07SuiteSummary(manifest bp07SuiteManifest, reason string) bp07SuiteSummary {
	return bp07SuiteSummary{
		SchemaVersion:   1,
		SuiteID:         manifest.SuiteID,
		BenchmarkID:     "bp07",
		RepetitionCount: manifest.RequestedRepetitions,
		ReleaseGating:   false,
		Valid:           false,
		Targets: map[string]bp07SuiteTargetSummary{
			"narumq": {Metrics: map[string]suiteartifact.MetricSummary{}, NetworkStatsAvailable: true},
			"emqx":   {Metrics: map[string]suiteartifact.MetricSummary{}, NetworkStatsAvailable: true},
		},
		FailureReason: reason,
	}
}

func finishBP07SuitePreparationFailure(stdout io.Writer, suiteDir string, manifest bp07SuiteManifest, reason error) error {
	manifest.FailureReason = reason.Error()
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	manifest.Valid = false
	summary := emptyBP07SuiteSummary(manifest, reason.Error())
	writeErr := writeBP07SuiteArtifacts(suiteDir, manifest, summary)
	_, outputErr := fmt.Fprintln(stdout, filepath.Join(suiteDir, "suite-manifest.json"))
	return errors.Join(reason, writeErr, outputErr)
}

func appendBP07SuiteFailure(existing string, err error) string {
	if err == nil {
		return existing
	}
	if existing == "" {
		return err.Error()
	}
	return errors.Join(errors.New(existing), err).Error()
}

func writeBP07SuiteArtifacts(directory string, manifest bp07SuiteManifest, summary bp07SuiteSummary) error {
	if err := writeBP07SuiteSummary(directory, summary); err != nil {
		return err
	}
	return writeBP07SuiteManifest(directory, manifest)
}

func writeBP07SuiteManifest(directory string, value bp07SuiteManifest) error {
	return writeBP07SuiteJSON(filepath.Join(directory, "suite-manifest.json"), value)
}

func writeBP07SuiteSummary(directory string, value bp07SuiteSummary) error {
	return writeBP07SuiteJSON(filepath.Join(directory, "suite-summary.json"), value)
}

func writeBP07SuiteJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	return nil
}
