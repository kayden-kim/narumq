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

type bp02SuiteRun struct {
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
type bp02SuiteManifest struct {
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
	Git                   bp02GitMetadata    `json:"git"`
	GoVersion             string             `json:"goVersion"`
	OS                    string             `json:"os"`
	Architecture          string             `json:"architecture"`
	Docker                bp02DockerMetadata `json:"docker"`
	BuildBaseImage        string             `json:"buildBaseImage"`
	PlannedBuildBaseImage string             `json:"plannedBuildBaseImage"`
	NaruMQImage           string             `json:"narumqImage"`
	NaruMQImageID         string             `json:"narumqImageId"`
	RunnerImage           string             `json:"runnerImage"`
	RunnerImageID         string             `json:"runnerImageId"`
	EMQX                  bp02EMQXMetadata   `json:"emqx"`
	Topology              bp02Topology       `json:"topology"`
	Runs                  []bp02SuiteRun     `json:"runs"`
	Valid                 bool               `json:"valid"`
	FailureReason         string             `json:"failureReason,omitempty"`
}
type bp02SuiteTargetSummary struct {
	Metrics               map[string]suiteartifact.MetricSummary `json:"metrics"`
	NetworkStatsAvailable bool                                   `json:"networkStatsAvailable"`
}
type bp02SuiteSummary struct {
	SchemaVersion         int                               `json:"schemaVersion"`
	SuiteID               string                            `json:"suiteId"`
	BenchmarkID           string                            `json:"benchmarkId"`
	RepetitionCount       int                               `json:"repetitionCount"`
	SuccessfulRepetitions int                               `json:"successfulRepetitions"`
	ReleaseGating         bool                              `json:"releaseGating"`
	Valid                 bool                              `json:"valid"`
	Targets               map[string]bp02SuiteTargetSummary `json:"targets"`
	FailureReason         string                            `json:"failureReason,omitempty"`
}
type bp02SuiteOptions struct {
	Profile       Profile
	ProfileBytes  []byte
	ProfileSHA256 string
	RepoRoot      string
	SuiteID       string
	SuiteDir      string
	Repetitions   int
	Images        environment.ImageSet
	Metadata      bp02HostMetadata
	Command       []string
}
type bp02SuiteRunData struct {
	Repetition int
	RunID      string
	Manifest   bp02Manifest
	Success    bool
}

func suiteCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("suite", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-02 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp02", "root directory for benchmark artifacts")
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
	repoRoot, err := bp02RepositoryRoot()
	if err != nil {
		return err
	}
	suiteID := newBP02SuiteID()
	suiteDir := filepath.Join(outputAbs, suiteID)
	if err := os.MkdirAll(suiteDir, 0o755); err != nil {
		return fmt.Errorf("create BP-02 suite directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(suiteDir, "profile.json"), profileBytes, 0o644); err != nil {
		return fmt.Errorf("write suite profile: %w", err)
	}
	command := []string{"suite", "--profile", *profilePath, "--output", *outputRoot, "--repetitions", strconv.Itoa(*repetitions)}
	options := bp02SuiteOptions{Profile: profile, ProfileBytes: profileBytes, ProfileSHA256: profileHash, RepoRoot: repoRoot, SuiteID: suiteID, SuiteDir: suiteDir, Repetitions: *repetitions, Metadata: collectBP02HostMetadata(repoRoot), Command: command}
	manifest := newBP02SuiteManifest(options, time.Now().UTC())
	summary := emptyBP02SuiteSummary(manifest, "")
	if err := writeBP02SuiteArtifacts(suiteDir, manifest, summary); err != nil {
		return err
	}
	if err := environment.EnsureEMQXImage(ctx, suiteDir, environment.EMQXImageReference); err != nil {
		return finishBP02SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	images, err := environment.BuildImages(ctx, environment.BuildOptions{BenchmarkID: "bp02", RunID: suiteID, RepoRoot: repoRoot, ArtifactDir: suiteDir, RunnerTarget: "bp02-runner"})
	if err != nil {
		return finishBP02SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	options.Images = images
	manifest.NaruMQImage, manifest.NaruMQImageID = images.NaruMQReference, images.NaruMQID
	manifest.RunnerImage, manifest.RunnerImageID = images.RunnerReference, images.RunnerID
	if err := writeBP02SuiteArtifacts(suiteDir, manifest, summary); err != nil {
		return finishBP02SuitePreparationFailure(stdout, suiteDir, manifest, err)
	}
	final, finalSummary, suiteErr := executeBP02Suite(ctx, options)
	if writeErr := writeBP02SuiteArtifacts(suiteDir, final, finalSummary); writeErr != nil {
		suiteErr = errors.Join(suiteErr, fmt.Errorf("write suite artifacts: %w", writeErr))
	}
	if _, err := fmt.Fprintln(stdout, filepath.Join(suiteDir, "suite-manifest.json")); err != nil {
		suiteErr = errors.Join(suiteErr, fmt.Errorf("write suite result: %w", err))
	}
	return suiteErr
}

func newBP02SuiteID() string {
	return "bp02-suite-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}
func newBP02SuiteManifest(options bp02SuiteOptions, startedAt time.Time) bp02SuiteManifest {
	return bp02SuiteManifest{SchemaVersion: 1, SuiteID: options.SuiteID, BenchmarkID: "bp02", StartedAt: startedAt.UTC().Format(time.RFC3339Nano), ProfileSHA256: options.ProfileSHA256, ResolvedProfile: options.Profile, RequestedRepetitions: options.Repetitions, OrderStrategy: "alternating target order; odd repetitions run NaruMQ then EMQX, even repetitions run EMQX then NaruMQ", ReleaseGating: false, Git: options.Metadata.Git, GoVersion: options.Metadata.GoVersion, OS: runtime.GOOS, Architecture: runtime.GOARCH, Docker: bp02DockerMetadata{ClientServerVersion: options.Metadata.DockerVersion}, BuildBaseImage: environment.BuildBaseImageReference, PlannedBuildBaseImage: environment.PlannedBuildBaseImage, NaruMQImage: options.Images.NaruMQReference, NaruMQImageID: options.Images.NaruMQID, RunnerImage: options.Images.RunnerReference, RunnerImageID: options.Images.RunnerID, EMQX: bp02EMQXMetadata{Version: "5.8.8", Edition: "Open Source", ImageReference: environment.EMQXImageReference, ImageIndex: strings.SplitN(environment.EMQXImageReference, "@", 2)[1]}, Topology: bp02Topology{Nodes: bp02NodeCount, NetworkMode: bp02NetworkMode, NaruMQStorage: "Raft-backed cluster with per-node Docker named volumes", EMQXStorage: "static EMQX OSS cluster with per-node Docker named volumes; storage equivalence is not claimed", SessionStorage: map[string]string{"narumq": "raft-replicated persistent session", "emqx": "memory session (OSS)"}}, Runs: make([]bp02SuiteRun, 0, options.Repetitions)}
}
func executeBP02Suite(ctx context.Context, options bp02SuiteOptions) (bp02SuiteManifest, bp02SuiteSummary, error) {
	manifest := newBP02SuiteManifest(options, time.Now().UTC())
	summary := emptyBP02SuiteSummary(manifest, "")
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
	if err := writeBP02SuiteArtifacts(options.SuiteDir, manifest, summary); err != nil {
		return manifest, summary, err
	}
	var failures []error
	var runs []bp02SuiteRunData
	for repetition := 1; repetition <= options.Repetitions; repetition++ {
		if err := ctx.Err(); err != nil {
			failure := fmt.Errorf("stop before repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP02SuiteFailure(manifest.FailureReason, failure)
			break
		}
		order, err := suiteartifact.AlternatingTargetOrder(repetition)
		if err != nil {
			failures = append(failures, err)
			manifest.FailureReason = appendBP02SuiteFailure(manifest.FailureReason, err)
			break
		}
		runID := fmt.Sprintf("%s-r%03d", options.SuiteID, repetition)
		runDir := filepath.Join(options.SuiteDir, "runs", fmt.Sprintf("%03d", repetition))
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			failure := fmt.Errorf("create repetition %d directory: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP02SuiteFailure(manifest.FailureReason, failure)
			break
		}
		if err := os.WriteFile(filepath.Join(runDir, "profile.json"), options.ProfileBytes, 0o644); err != nil {
			failure := fmt.Errorf("write repetition %d profile: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP02SuiteFailure(manifest.FailureReason, failure)
			break
		}
		startedAt := time.Now().UTC()
		childOptions := bp02RunOptions{Profile: options.Profile, ProfileSHA256: options.ProfileSHA256, RunID: runID, RunDir: runDir, Images: options.Images, Metadata: options.Metadata, Command: append([]string(nil), options.Command...), TargetOrder: order}
		childManifest := newBP02Manifest(childOptions, startedAt)
		childManifest.RunnerImage = options.Images.RunnerReference
		childManifest.RunnerImageID = options.Images.RunnerID
		childManifest, runErr := executeBP02Comparison(ctx, childOptions, childManifest)
		childSummary := summarizeBP02Run(childManifest)
		comparison := buildBP02Comparison(childManifest, childSummary)
		if err := writeBP02RunArtifacts(runDir, childManifest); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("write repetition artifacts: %w", err))
		}
		finishedAt := time.Now().UTC()
		success := bp02SuiteRunSucceeded(childManifest, comparison, childSummary, runErr)
		failureReason := bp02SuiteRunFailure(childManifest, comparison, childSummary, runErr)
		run := bp02SuiteRun{Repetition: repetition, RunID: runID, StartedAt: startedAt.Format(time.RFC3339Nano), FinishedAt: finishedAt.Format(time.RFC3339Nano), TargetOrder: append([]string(nil), order...), ArtifactDirectory: filepath.ToSlash(filepath.Join("runs", fmt.Sprintf("%03d", repetition))), Equivalent: comparison.Equivalent, Success: success, FailureReason: failureReason}
		runs = append(runs, bp02SuiteRunData{Repetition: repetition, RunID: runID, Manifest: childManifest, Success: success})
		manifest.Runs = append(manifest.Runs, run)
		manifest.CompletedRepetitions++
		if !success {
			failure := errors.New(failureReason)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP02SuiteFailure(manifest.FailureReason, failure)
		}
		if err := writeBP02SuiteArtifacts(options.SuiteDir, manifest, summary); err != nil {
			failure := fmt.Errorf("write suite manifest after repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP02SuiteFailure(manifest.FailureReason, failure)
			break
		}
		if err := ctx.Err(); err != nil {
			failure := fmt.Errorf("stop after repetition %d: %w", repetition, err)
			failures = append(failures, failure)
			manifest.FailureReason = appendBP02SuiteFailure(manifest.FailureReason, failure)
			break
		}
	}
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	summary = buildBP02SuiteSummary(options, manifest, runs)
	manifest.Valid = summary.Valid
	if summary.FailureReason != "" {
		manifest.FailureReason = appendBP02SuiteFailure(manifest.FailureReason, errors.New(summary.FailureReason))
	}
	if err := writeBP02SuiteSummary(options.SuiteDir, summary); err != nil {
		failure := fmt.Errorf("write final suite summary: %w", err)
		failures = append(failures, failure)
		summary.Valid = false
		summary.FailureReason = appendBP02SuiteFailure(summary.FailureReason, failure)
		manifest.Valid = false
		manifest.FailureReason = appendBP02SuiteFailure(manifest.FailureReason, failure)
	}
	if err := writeBP02SuiteManifest(options.SuiteDir, manifest); err != nil {
		failure := fmt.Errorf("write final suite manifest: %w", err)
		failures = append(failures, failure)
		manifest.Valid = false
		manifest.FailureReason = appendBP02SuiteFailure(manifest.FailureReason, failure)
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
func bp02SuiteRunSucceeded(manifest bp02Manifest, comparison bp02Comparison, summary bp02Summary, runErr error) bool {
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
func bp02SuiteRunFailure(manifest bp02Manifest, comparison bp02Comparison, summary bp02Summary, runErr error) string {
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
func buildBP02SuiteSummary(options bp02SuiteOptions, manifest bp02SuiteManifest, runs []bp02SuiteRunData) bp02SuiteSummary {
	summary := emptyBP02SuiteSummary(manifest, "")
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
	targetSamples := map[string]map[string][]suiteartifact.MetricSample{"narumq": {}, "emqx": {}}
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
			metrics, err := bp02Metrics(target, entry)
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
	definitions := bp02SuiteMetricUnits(options.Profile, networkAvailable)
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
func emptyBP02SuiteSummary(manifest bp02SuiteManifest, reason string) bp02SuiteSummary {
	return bp02SuiteSummary{SchemaVersion: 1, SuiteID: manifest.SuiteID, BenchmarkID: "bp02", RepetitionCount: manifest.RequestedRepetitions, ReleaseGating: false, Targets: map[string]bp02SuiteTargetSummary{"narumq": {Metrics: map[string]suiteartifact.MetricSummary{}, NetworkStatsAvailable: true}, "emqx": {Metrics: map[string]suiteartifact.MetricSummary{}, NetworkStatsAvailable: true}}, FailureReason: reason}
}
func finishBP02SuitePreparationFailure(stdout io.Writer, suiteDir string, manifest bp02SuiteManifest, cause error) error {
	manifest.FailureReason = cause.Error()
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	summary := emptyBP02SuiteSummary(manifest, cause.Error())
	writeErr := writeBP02SuiteArtifacts(suiteDir, manifest, summary)
	_, outputErr := fmt.Fprintln(stdout, filepath.Join(suiteDir, "suite-manifest.json"))
	return errors.Join(cause, writeErr, outputErr)
}
func appendBP02SuiteFailure(existing string, err error) string {
	if err == nil {
		return existing
	}
	if existing == "" {
		return err.Error()
	}
	return errors.Join(errors.New(existing), err).Error()
}
func writeBP02SuiteArtifacts(directory string, manifest bp02SuiteManifest, summary bp02SuiteSummary) error {
	if err := writeBP02SuiteJSON(filepath.Join(directory, "suite-summary.json"), summary); err != nil {
		return err
	}
	return writeBP02SuiteJSON(filepath.Join(directory, "suite-manifest.json"), manifest)
}
func writeBP02SuiteManifest(directory string, value bp02SuiteManifest) error {
	return writeBP02SuiteJSON(filepath.Join(directory, "suite-manifest.json"), value)
}
func writeBP02SuiteSummary(directory string, value bp02SuiteSummary) error {
	return writeBP02SuiteJSON(filepath.Join(directory, "suite-summary.json"), value)
}
func writeBP02SuiteJSON(path string, value any) error { return writeBP02JSON(path, value) }

func bp02Metrics(target string, value bp02TargetManifest) (map[string]float64, error) {
	if target != "narumq" && target != "emqx" {
		return nil, fmt.Errorf("unsupported BP-02 target %q", target)
	}
	if !value.Success || !value.Valid || value.WorkloadSummary == nil || !value.WorkloadSummary.Valid {
		return nil, errors.New("target workload summary is invalid")
	}
	if value.ResourceSummary == nil || !value.ResourceSummary.Valid {
		return nil, errors.New("target resource summary is invalid")
	}
	metrics := make(map[string]float64)
	streams := []string{"a_telemetry", "a_alert", "a_command", "a_job", "b_telemetry", "b_alert"}
	for _, step := range value.WorkloadSummary.Steps {
		resource, ok := resourceWindowByName(value.ResourceSummary.Windows, fmt.Sprintf("step-%02d-measure", step.Step))
		if !ok {
			return nil, fmt.Errorf("missing resource window for step %02d", step.Step)
		}
		metrics[fmt.Sprintf("step%02d_delivered_per_second", step.Step)] = step.DeliveredPerSecond
		metrics[fmt.Sprintf("step%02d_cluster_median_cpu_percent", step.Step)] = resource.Cluster.MedianCPUPercent
		for _, name := range streams {
			stream, exists := step.Streams[name]
			if !exists {
				return nil, fmt.Errorf("step %02d has no %s stream summary", step.Step, name)
			}
			prefix := fmt.Sprintf("step%02d_%s_delivery_latency_", step.Step, name)
			metrics[prefix+"p99_ns"] = float64(stream.DeliveryLatency.P99Nanoseconds)
			metrics[prefix+"p999_ns"] = float64(stream.DeliveryLatency.P999Nanoseconds)
			if name == "a_alert" || name == "a_command" || name == "a_job" || name == "b_alert" {
				metrics[fmt.Sprintf("step%02d_%s_publish_ack_latency_p99_ns", step.Step, name)] = float64(stream.PublishAckLatency.P99Nanoseconds)
			}
		}
		if value.ResourceSummary.NetworkStatsAvailable {
			if resource.Deliveries == 0 {
				return nil, fmt.Errorf("step %02d has zero deliveries for network metric", step.Step)
			}
			metrics[fmt.Sprintf("step%02d_network_tx_bytes_per_delivery", step.Step)] = float64(resource.Cluster.NetworkTxBytes) / float64(resource.Deliveries)
		}
	}
	if len(bp02SetupClients(value.ResolvedProfile)) > 0 {
		if value.WorkloadSummary.Setup.TLSClients > 0 {
			metrics["setup_tls_connect_latency_p99_ns"] = float64(value.WorkloadSummary.Setup.TLSConnectLatency.P99Nanoseconds)
		}
		if value.WorkloadSummary.Setup.TCPClients > 0 {
			metrics["setup_tcp_connect_latency_p99_ns"] = float64(value.WorkloadSummary.Setup.TCPConnectLatency.P99Nanoseconds)
		}
	}
	metrics["offline_reconnect_connack_latency_p99_ns"] = float64(value.WorkloadSummary.Offline.ReconnectConnAckLatency.P99Nanoseconds)
	metrics["offline_drain_duration_max_ns"] = float64(value.WorkloadSummary.Offline.DrainDurationMaxNanoseconds)
	metrics["retained_replay_duration_max_ns"] = float64(value.WorkloadSummary.Retained.ReplayDurationMaxNanoseconds)
	metrics["memory_bytes_per_setup_client"] = value.ResourceSummary.Memory.MemoryBytesPerSetupClient
	return metrics, nil
}
func bp02SuiteMetricUnits(profile Profile, networkAvailable bool) map[string]string {
	units := make(map[string]string)
	streams := []string{"a_telemetry", "a_alert", "a_command", "a_job", "b_telemetry", "b_alert"}
	for step := range profile.RatesPerStreamPerSecond {
		prefix := fmt.Sprintf("step%02d_", step+1)
		units[prefix+"delivered_per_second"] = "deliveries/s"
		units[prefix+"cluster_median_cpu_percent"] = "percent"
		for _, name := range streams {
			sp := fmt.Sprintf("step%02d_%s_delivery_latency_", step+1, name)
			units[sp+"p99_ns"] = "ns"
			units[sp+"p999_ns"] = "ns"
			if name == "a_alert" || name == "a_command" || name == "a_job" || name == "b_alert" {
				units[fmt.Sprintf("step%02d_%s_publish_ack_latency_p99_ns", step+1, name)] = "ns"
			}
		}
		if networkAvailable {
			units[prefix+"network_tx_bytes_per_delivery"] = "bytes"
		}
	}
	setup := bp02SetupClients(profile)
	hasTLS, hasTCP := false, false
	for _, client := range setup {
		if client.tls {
			hasTLS = true
		} else {
			hasTCP = true
		}
	}
	if hasTLS {
		units["setup_tls_connect_latency_p99_ns"] = "ns"
	}
	if hasTCP {
		units["setup_tcp_connect_latency_p99_ns"] = "ns"
	}
	units["offline_reconnect_connack_latency_p99_ns"] = "ns"
	units["offline_drain_duration_max_ns"] = "ns"
	units["retained_replay_duration_max_ns"] = "ns"
	units["memory_bytes_per_setup_client"] = "bytes"
	return units
}
