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

type bp03SuiteRun struct {
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

type bp03SuiteManifest struct {
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
	Git                        bp03GitMetadata    `json:"git"`
	GoVersion                  string             `json:"goVersion"`
	OS                         string             `json:"os"`
	Architecture               string             `json:"architecture"`
	Docker                     bp03DockerMetadata `json:"docker"`
	BuildBaseImage             string             `json:"buildBaseImage"`
	PlannedBuildBaseImage      string             `json:"plannedBuildBaseImage"`
	BuildBaseImageSubstitution string             `json:"buildBaseImageSubstitution"`
	NaruMQImage                string             `json:"narumqImage"`
	NaruMQImageID              string             `json:"narumqImageId"`
	RunnerImage                string             `json:"runnerImage"`
	RunnerImageID              string             `json:"runnerImageId"`
	EMQX                       bp03EMQXMetadata   `json:"emqx"`
	Topology                   bp03Topology       `json:"topology"`
	Runs                       []bp03SuiteRun     `json:"runs"`
	Valid                      bool               `json:"valid"`
	FailureReason              string             `json:"failureReason,omitempty"`
}

type bp03SuiteTargetSummary struct {
	Metrics               map[string]suiteartifact.MetricSummary `json:"metrics"`
	NetworkStatsAvailable bool                                   `json:"networkStatsAvailable"`
}

type bp03SuiteSummary struct {
	SchemaVersion         int                               `json:"schemaVersion"`
	SuiteID               string                            `json:"suiteId"`
	BenchmarkID           string                            `json:"benchmarkId"`
	RepetitionCount       int                               `json:"repetitionCount"`
	SuccessfulRepetitions int                               `json:"successfulRepetitions"`
	ReleaseGating         bool                              `json:"releaseGating"`
	Valid                 bool                              `json:"valid"`
	Targets               map[string]bp03SuiteTargetSummary `json:"targets"`
	FailureReason         string                            `json:"failureReason,omitempty"`
}

type bp03SuiteOptions struct {
	Profile       Profile
	ProfileBytes  []byte
	ProfileSHA256 string
	RepoRoot      string
	SuiteID       string
	SuiteDir      string
	Repetitions   int
	Images        environment.ImageSet
	Metadata      bp03HostMetadata
	Command       []string
}

type bp03SuiteRunData struct {
	Repetition int
	RunID      string
	Manifest   bp03Manifest
	Comparison bp03Comparison
	Success    bool
}

func suiteCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("suite", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-03 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp03", "root directory for benchmark artifacts")
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
	repoRoot, err := bp03RepositoryRoot()
	if err != nil {
		return err
	}
	suiteID := newBP03SuiteID()
	suiteDir := filepath.Join(outputAbs, suiteID)
	if err := os.MkdirAll(suiteDir, 0o755); err != nil {
		return fmt.Errorf("create BP-03 suite directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(suiteDir, "profile.json"), profileBytes, 0o644); err != nil {
		return fmt.Errorf("write suite profile: %w", err)
	}
	command := []string{"suite", "--profile", *profilePath, "--output", *outputRoot, "--repetitions", strconv.Itoa(*repetitions)}
	options := bp03SuiteOptions{
		Profile:       profile,
		ProfileBytes:  profileBytes,
		ProfileSHA256: profileHash,
		RepoRoot:      repoRoot,
		SuiteID:       suiteID,
		SuiteDir:      suiteDir,
		Repetitions:   *repetitions,
		Metadata:      collectBP03HostMetadata(repoRoot),
		Command:       command,
	}
	manifest := newBP03SuiteManifest(options, time.Now().UTC())
	summary := emptyBP03SuiteSummary(manifest, "")
	if err := writeBP03SuiteArtifacts(suiteDir, manifest, summary); err != nil {
		return err
	}
	if err := environment.EnsureEMQXImage(ctx, suiteDir, bp03EMQXImageReference); err != nil {
		return finishBP03SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	images, err := environment.BuildImages(ctx, environment.BuildOptions{
		BenchmarkID:  "bp03",
		RunID:        suiteID,
		RepoRoot:     repoRoot,
		ArtifactDir:  suiteDir,
		RunnerTarget: "bp03-runner",
	})
	if err != nil {
		return finishBP03SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	options.Images = images
	manifest.NaruMQImage = images.NaruMQReference
	manifest.NaruMQImageID = images.NaruMQID
	manifest.RunnerImage = images.RunnerReference
	manifest.RunnerImageID = images.RunnerID
	if err := writeBP03SuiteArtifacts(suiteDir, manifest, summary); err != nil {
		return finishBP03SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	final, finalSummary, suiteErr := executeBP03Suite(ctx, options)
	if writeErr := writeBP03SuiteArtifacts(suiteDir, final, finalSummary); writeErr != nil {
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

func newBP03SuiteID() string {
	return "bp03-suite-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}

func newBP03SuiteManifest(options bp03SuiteOptions, startedAt time.Time) bp03SuiteManifest {
	base := newBP03Manifest(bp03RunOptions{
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
	return bp03SuiteManifest{
		SchemaVersion:              1,
		SuiteID:                    options.SuiteID,
		BenchmarkID:                "bp03",
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
		Runs:                       make([]bp03SuiteRun, 0, options.Repetitions),
	}
}

func executeBP03Suite(ctx context.Context, options bp03SuiteOptions) (bp03SuiteManifest, bp03SuiteSummary, error) {
	manifest := newBP03SuiteManifest(options, time.Now().UTC())
	summary := emptyBP03SuiteSummary(manifest, "")
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
	if err := writeBP03SuiteArtifacts(options.SuiteDir, manifest, summary); err != nil {
		return manifest, summary, err
	}
	var failures []error
	var runs []bp03SuiteRunData
	for repetition := 1; repetition <= options.Repetitions; repetition++ {
		if err := ctx.Err(); err != nil {
			failure := fmt.Errorf("stop before repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP03SuiteFailure(manifest.FailureReason, failure)
			break
		}
		order, err := suiteartifact.AlternatingTargetOrder(repetition)
		if err != nil {
			failures = append(failures, err)
			manifest.FailureReason = appendBP03SuiteFailure(manifest.FailureReason, err)
			break
		}
		runID := fmt.Sprintf("%s-r%03d", options.SuiteID, repetition)
		runDir := filepath.Join(options.SuiteDir, "runs", fmt.Sprintf("%03d", repetition))
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			failure := fmt.Errorf("create repetition %d directory: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP03SuiteFailure(manifest.FailureReason, failure)
			break
		}
		startedAt := time.Now().UTC()
		childOptions := bp03RunOptions{
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
		childManifest := newBP03Manifest(childOptions, startedAt)
		childManifest.RunnerImage = options.Images.RunnerReference
		childManifest.RunnerImageID = options.Images.RunnerID
		childManifest, runErr := executeBP03Comparison(ctx, childOptions, childManifest)
		childSummary := summarizeBP03Run(childManifest)
		comparison := buildBP03Comparison(childManifest, childSummary)
		if err := writeBP03RunArtifacts(runDir, childManifest); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("write repetition artifacts: %w", err))
		}
		finishedAt := time.Now().UTC()
		success := bp03SuiteRunSucceeded(childManifest, comparison, childSummary, runErr)
		failureReason := bp03SuiteRunFailure(childManifest, comparison, childSummary, runErr)
		run := bp03SuiteRun{
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
		runs = append(runs, bp03SuiteRunData{Repetition: repetition, RunID: runID, Manifest: childManifest, Comparison: comparison, Success: success})
		manifest.Runs = append(manifest.Runs, run)
		manifest.CompletedRepetitions++
		if !success {
			failure := errors.New(failureReason)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP03SuiteFailure(manifest.FailureReason, failure)
		}
		if err := writeBP03SuiteArtifacts(options.SuiteDir, manifest, summary); err != nil {
			failure := fmt.Errorf("write suite manifest after repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP03SuiteFailure(manifest.FailureReason, failure)
			break
		}
		if err := ctx.Err(); err != nil {
			failure := fmt.Errorf("stop after repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP03SuiteFailure(manifest.FailureReason, failure)
			break
		}
	}
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	summary = buildBP03SuiteSummary(options, manifest, runs)
	manifest.Valid = summary.Valid
	if summary.FailureReason != "" {
		manifest.FailureReason = appendBP03SuiteFailure(manifest.FailureReason, errors.New(summary.FailureReason))
	}
	if err := writeBP03SuiteSummary(options.SuiteDir, summary); err != nil {
		failure := fmt.Errorf("write final suite summary: %w", err)
		failures = append(failures, failure)
		summary.Valid = false
		summary.FailureReason = appendBP03SuiteFailure(summary.FailureReason, failure)
		manifest.Valid = false
		manifest.FailureReason = appendBP03SuiteFailure(manifest.FailureReason, failure)
	}
	if err := writeBP03SuiteManifest(options.SuiteDir, manifest); err != nil {
		failure := fmt.Errorf("write final suite manifest: %w", err)
		failures = append(failures, failure)
		manifest.Valid = false
		manifest.FailureReason = appendBP03SuiteFailure(manifest.FailureReason, failure)
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

func bp03SuiteRunSucceeded(manifest bp03Manifest, comparison bp03Comparison, summary bp03Summary, runErr error) bool {
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

func bp03SuiteRunFailure(manifest bp03Manifest, comparison bp03Comparison, summary bp03Summary, runErr error) string {
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

func bp03Metrics(target string, value bp03TargetManifest) (map[string]float64, error) {
	if target != "narumq" && target != "emqx" {
		return nil, fmt.Errorf("unsupported BP-03 target %q", target)
	}
	if !value.Success || !value.Valid || value.WorkloadSummary == nil || !value.WorkloadSummary.Valid {
		return nil, errors.New("target workload summary is invalid")
	}
	if value.ResourceSummary == nil || !value.ResourceSummary.Valid {
		return nil, errors.New("target resource summary is invalid")
	}
	if target == "narumq" && (value.WALSummary == nil || !value.WALSummary.Valid) {
		return nil, errors.New("NaruMQ WAL summary is invalid")
	}
	workload := value.WorkloadSummary
	resources := value.ResourceSummary
	metrics := make(map[string]float64)
	for _, step := range workload.Steps {
		resource, ok := bp03ResourceWindow(*resources, fmt.Sprintf("step-%02d-measure", step.Step))
		if !ok {
			return nil, fmt.Errorf("missing resource window for step %02d", step.Step)
		}
		for _, qos := range []string{"1", "2"} {
			stats, ok := step.QoS[qos]
			if !ok {
				return nil, fmt.Errorf("step %02d has no QoS %s statistics", step.Step, qos)
			}
			prefix := fmt.Sprintf("step%02d_q%s_", step.Step, qos)
			metrics[prefix+"ack_latency_p99_ns"] = float64(stats.AckLatencyNanoseconds.P99)
			metrics[prefix+"ack_latency_p999_ns"] = float64(stats.AckLatencyNanoseconds.P999)
			metrics[prefix+"delivery_latency_p99_ns"] = float64(stats.DeliveryLatencyNanoseconds.P99)
			metrics[prefix+"ack_rate_per_second"] = stats.AckRatePerSecond
		}
		if resource.BlockWriteBytesPerAckedMessage == nil {
			return nil, fmt.Errorf("step %02d has no block-write bytes per acknowledged message", step.Step)
		}
		metrics[fmt.Sprintf("step%02d_block_write_bytes_per_acked_message", step.Step)] = *resource.BlockWriteBytesPerAckedMessage
		if resources.NetworkStatsAvailable {
			if resource.NetworkTxBytesPerAckedMessage == nil {
				return nil, fmt.Errorf("step %02d has no network bytes per acknowledged message", step.Step)
			}
			metrics[fmt.Sprintf("step%02d_network_tx_bytes_per_acked_message", step.Step)] = *resource.NetworkTxBytesPerAckedMessage
		}
		if target == "narumq" {
			wal, ok := bp03WALWindow(*value.WALSummary, fmt.Sprintf("step-%02d", step.Step))
			if !ok {
				return nil, fmt.Errorf("missing WAL window for step %02d", step.Step)
			}
			if wal.Cluster.WALBytesPerAckedMessage == nil {
				return nil, fmt.Errorf("step %02d has no WAL bytes per acknowledged message", step.Step)
			}
			metrics[fmt.Sprintf("step%02d_wal_syncs_per_second", step.Step)] = wal.Cluster.SyncsPerSecond
			metrics[fmt.Sprintf("step%02d_wal_mean_entries_per_sync", step.Step)] = wal.Cluster.MeanEntriesPerSync
			metrics[fmt.Sprintf("step%02d_wal_bytes_per_acked_message", step.Step)] = *wal.Cluster.WALBytesPerAckedMessage
		}
	}
	metrics["offline_reconnect_drain_millis_max"] = workload.Offline.ReconnectDrainMillis.Max
	return metrics, nil
}

func bp03ResourceWindow(summary resourceSummary, name string) (resourceWindow, bool) {
	for _, window := range summary.Windows {
		if window.Window == name {
			return window, true
		}
	}
	return resourceWindow{}, false
}

func bp03WALWindow(summary WALSummary, name string) (walWindowSummary, bool) {
	for _, window := range summary.Windows {
		if window.Name == name {
			return window, true
		}
	}
	return walWindowSummary{}, false
}

func bp03SuiteMetricUnits(profile Profile, target string, networkAvailable bool) (map[string]string, error) {
	if target != "narumq" && target != "emqx" {
		return nil, fmt.Errorf("unsupported BP-03 target %q", target)
	}
	units := make(map[string]string)
	for step := range profile.RatesPerStreamPerSecond {
		for _, qos := range []string{"1", "2"} {
			prefix := fmt.Sprintf("step%02d_q%s_", step+1, qos)
			units[prefix+"ack_latency_p99_ns"] = "ns"
			units[prefix+"ack_latency_p999_ns"] = "ns"
			units[prefix+"delivery_latency_p99_ns"] = "ns"
			units[prefix+"ack_rate_per_second"] = "messages/s"
		}
		if networkAvailable {
			units[fmt.Sprintf("step%02d_network_tx_bytes_per_acked_message", step+1)] = "bytes/message"
		}
		units[fmt.Sprintf("step%02d_block_write_bytes_per_acked_message", step+1)] = "bytes/message"
		if target == "narumq" {
			units[fmt.Sprintf("step%02d_wal_syncs_per_second", step+1)] = "syncs/s"
			units[fmt.Sprintf("step%02d_wal_mean_entries_per_sync", step+1)] = "entries/sync"
			units[fmt.Sprintf("step%02d_wal_bytes_per_acked_message", step+1)] = "bytes/message"
		}
	}
	units["offline_reconnect_drain_millis_max"] = "ms"
	return units, nil
}

func buildBP03SuiteSummary(options bp03SuiteOptions, manifest bp03SuiteManifest, runs []bp03SuiteRunData) bp03SuiteSummary {
	summary := emptyBP03SuiteSummary(manifest, "")
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
			metrics, err := bp03Metrics(target, entry)
			if err != nil {
				failures = append(failures, fmt.Errorf("repetition %d %s metrics: %w", run.Repetition, target, err))
				continue
			}
			for key, value := range metrics {
				targetSamples[target][key] = append(targetSamples[target][key], suiteartifact.MetricSample{Repetition: run.Repetition, RunID: run.RunID, Value: value})
			}
		}
	}
	for _, target := range []string{"narumq", "emqx"} {
		targetSummary := summary.Targets[target]
		targetSummary.NetworkStatsAvailable = networkAvailable[target]
		if !networkAvailable[target] {
			for key := range targetSamples[target] {
				if strings.Contains(key, "_network_tx_bytes_per_acked_message") {
					delete(targetSamples[target], key)
				}
			}
		}
		definitions, err := bp03SuiteMetricUnits(options.Profile, target, networkAvailable[target])
		if err != nil {
			failures = append(failures, err)
			continue
		}
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

func emptyBP03SuiteSummary(manifest bp03SuiteManifest, reason string) bp03SuiteSummary {
	return bp03SuiteSummary{
		SchemaVersion:   1,
		SuiteID:         manifest.SuiteID,
		BenchmarkID:     "bp03",
		RepetitionCount: manifest.RequestedRepetitions,
		ReleaseGating:   false,
		Valid:           false,
		Targets: map[string]bp03SuiteTargetSummary{
			"narumq": {Metrics: map[string]suiteartifact.MetricSummary{}, NetworkStatsAvailable: true},
			"emqx":   {Metrics: map[string]suiteartifact.MetricSummary{}, NetworkStatsAvailable: true},
		},
		FailureReason: reason,
	}
}

func finishBP03SuitePreparationFailure(stdout io.Writer, suiteDir string, manifest bp03SuiteManifest, reason error) error {
	manifest.FailureReason = reason.Error()
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	manifest.Valid = false
	summary := emptyBP03SuiteSummary(manifest, reason.Error())
	writeErr := writeBP03SuiteArtifacts(suiteDir, manifest, summary)
	_, outputErr := fmt.Fprintln(stdout, filepath.Join(suiteDir, "suite-manifest.json"))
	return errors.Join(reason, writeErr, outputErr)
}

func appendBP03SuiteFailure(existing string, err error) string {
	if err == nil {
		return existing
	}
	if existing == "" {
		return err.Error()
	}
	return errors.Join(errors.New(existing), err).Error()
}

func writeBP03SuiteArtifacts(directory string, manifest bp03SuiteManifest, summary bp03SuiteSummary) error {
	if err := writeBP03SuiteSummary(directory, summary); err != nil {
		return err
	}
	return writeBP03SuiteManifest(directory, manifest)
}

func writeBP03SuiteManifest(directory string, value bp03SuiteManifest) error {
	return writeBP03SuiteJSON(filepath.Join(directory, "suite-manifest.json"), value)
}

func writeBP03SuiteSummary(directory string, value bp03SuiteSummary) error {
	return writeBP03SuiteJSON(filepath.Join(directory, "suite-summary.json"), value)
}

func writeBP03SuiteJSON(path string, value any) error {
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
