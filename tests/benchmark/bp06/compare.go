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
	bp06BuildBaseImageSubstitution = "planned digest was unavailable in Docker Hub; immutable digest resolved from golang:1.26.5-bookworm on 2026-09-19"
	bp06NetworkMode                = "bridge network; broker shares netns owner"
	bp06EMQXImageReference         = "emqx/emqx-enterprise:5.8.8@sha256:d166efa91eae706ffbfca3f47e36fae0fd441507a8a57bda622da09e4a019c14"
	bp06EMQXDurabilityReady        = `begin
		Config = emqx_ds_replication_layer_meta:db_config(messages),
		Sites = emqx_ds_replication_layer_meta:db_sites(messages),
		Shards = emqx_ds_replication_layer_meta:shards(messages),
		{ok, Authenticators} = emqx_authn_chains:list_authenticators('mqtt:global'),
		emqx:get_config([durable_sessions, enable]) =:= true andalso
		emqx:get_config([durable_sessions, heartbeat_interval]) =:= 5000 andalso
		lists:member({messages, builtin_raft}, emqx_ds:which_dbs()) andalso
		maps:get(n_sites, Config) =:= 3 andalso
		maps:get(replication_factor, Config) =:= 3 andalso
		length(Sites) =:= 3 andalso length(Shards) =:= 1 andalso
		lists:sort([emqx_ds_replication_layer_meta:node(Site) || Site <- Sites]) =:= lists:sort(emqx:running_nodes()) andalso
		lists:all(fun(Shard) ->
			lists:sort(emqx_ds_replication_layer_meta:replica_set(messages, Shard)) =:= lists:sort(Sites) andalso
			emqx_ds_replication_layer_meta:replica_set_transitions(messages, Shard) =:= []
		end, Shards) andalso
		lists:any(fun(#{id := <<"jwt">>, enable := true}) -> true; (_) -> false end, Authenticators)
	end`
	bp06EMQXDurabilitySnapshot = `{emqx:get_config([durable_sessions]),
		emqx_ds_replication_layer_meta:db_config(messages), emqx:get_config([authentication]),
		[{Shard, emqx_ds_replication_layer_meta:replica_set(messages, Shard),
			emqx_ds_replication_layer_meta:replica_set_transitions(messages, Shard)}
			|| Shard <- emqx_ds_replication_layer_meta:shards(messages)]}`
)

type bp06ArtifactProfile struct {
	RawSHA256 string  `json:"rawSha256"`
	Resolved  Profile `json:"resolved"`
}

type bp06GitMetadata struct {
	Commit string `json:"commit"`
	Dirty  bool   `json:"dirty"`
}

type bp06DockerMetadata struct {
	ClientServerVersion string `json:"clientServerVersion"`
}

type bp06HostMetadata struct {
	Git           bp06GitMetadata
	GoVersion     string
	DockerVersion string
}

type bp06ResourceLimits struct {
	BrokerCPUs       float64 `json:"brokerCpus"`
	BrokerMemory     int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs    float64 `json:"generatorCpus"`
	GeneratorMemory  int64   `json:"generatorMemoryBytes"`
	BrokerReadOnlyFS bool    `json:"brokerReadOnlyRootFilesystem"`
	RunnerReadOnlyFS bool    `json:"runnerReadOnlyRootFilesystem"`
	NetworkMode      string  `json:"networkMode"`
}

type bp06Topology struct {
	Nodes              int    `json:"nodes"`
	NetworkMode        string `json:"networkMode"`
	NaruMQStorage      string `json:"naruMQStorage"`
	EMQXSessionStorage string `json:"emqxSessionStorage"`
	RuntimeMetrics     string `json:"runtimeMetrics"`
}

type bp06EMQXMetadata struct {
	Version        string `json:"version"`
	Edition        string `json:"edition"`
	License        string `json:"license"`
	EOL            string `json:"eol,omitempty"`
	ImageReference string `json:"imageReference"`
	ImageIndex     string `json:"imageIndexDigest"`
	Historical     bool   `json:"historicalComparator"`
}

type bp06NodeArtifacts struct {
	Name                 string `json:"name"`
	BrokerContainer      string `json:"brokerContainer"`
	NetworkOwner         string `json:"networkOwnerContainer"`
	BrokerStatsPath      string `json:"brokerStatsPath"`
	NetworkStatsPath     string `json:"networkStatsPath"`
	BrokerLogPath        string `json:"brokerLogPath"`
	ContainerInspectPath string `json:"containerInspectPath"`
}

type bp06TargetManifest struct {
	Target              string              `json:"target"`
	Status              string              `json:"status"`
	Success             bool                `json:"success"`
	Valid               bool                `json:"valid"`
	StartedAt           string              `json:"startedAt,omitempty"`
	FinishedAt          string              `json:"finishedAt,omitempty"`
	ArtifactDirectory   string              `json:"artifactDirectory"`
	WorkloadSummaryPath string              `json:"workloadSummaryPath"`
	ResourceSummaryPath string              `json:"resourceSummaryPath"`
	FailureSummaryPath  string              `json:"failureSummaryPath"`
	PhasesPath          string              `json:"phasesPath"`
	MessagesPath        string              `json:"messagesPath"`
	RunnerLogPath       string              `json:"runnerLogPath,omitempty"`
	Nodes               []bp06NodeArtifacts `json:"nodes,omitempty"`
	ImageReference      string              `json:"imageReference"`
	ImageID             string              `json:"imageId,omitempty"`
	ImageRepoDigests    []string            `json:"imageRepoDigests,omitempty"`
	RunnerImageID       string              `json:"runnerImageId"`
	ProfileSHA256       string              `json:"profileSha256"`
	ResolvedProfile     Profile             `json:"resolvedProfile"`
	ResourceLimits      bp06ResourceLimits  `json:"resourceLimits"`
	NetworkMode         string              `json:"networkMode"`
	WorkloadSummary     *workloadSummary    `json:"workloadSummary,omitempty"`
	ResourceSummary     *resourceSummary    `json:"resourceSummary,omitempty"`
	FailureSummary      *failureSummary     `json:"failureSummary,omitempty"`
	FailureReason       string              `json:"failureReason,omitempty"`
}

type bp06Manifest struct {
	SchemaVersion              int                           `json:"schemaVersion"`
	RunID                      string                        `json:"runId"`
	StartedAt                  string                        `json:"startedAt"`
	FinishedAt                 string                        `json:"finishedAt,omitempty"`
	Purpose                    string                        `json:"purpose"`
	ReleaseGating              bool                          `json:"releaseGating"`
	Profile                    bp06ArtifactProfile           `json:"profile"`
	Git                        bp06GitMetadata               `json:"git"`
	GoVersion                  string                        `json:"goVersion"`
	OS                         string                        `json:"os"`
	Architecture               string                        `json:"architecture"`
	Docker                     bp06DockerMetadata            `json:"docker"`
	Command                    []string                      `json:"command"`
	BuildBaseImage             string                        `json:"buildBaseImage"`
	PlannedBuildBaseImage      string                        `json:"plannedBuildBaseImage"`
	BuildBaseImageSubstitution string                        `json:"buildBaseImageSubstitution"`
	RunnerImage                string                        `json:"runnerImage"`
	RunnerImageID              string                        `json:"runnerImageId"`
	EMQX                       bp06EMQXMetadata              `json:"emqx"`
	Topology                   bp06Topology                  `json:"topology"`
	SummaryPath                string                        `json:"summaryPath"`
	ComparisonPath             string                        `json:"comparisonPath"`
	Targets                    map[string]bp06TargetManifest `json:"targets"`
	FailureReason              string                        `json:"failureReason,omitempty"`
}

type bp06TargetSummary struct {
	Valid           bool             `json:"valid"`
	WorkloadSummary *workloadSummary `json:"workloadSummary,omitempty"`
	ResourceSummary *resourceSummary `json:"resourceSummary,omitempty"`
	FailureSummary  *failureSummary  `json:"failureSummary,omitempty"`
	FailureReason   string           `json:"failureReason,omitempty"`
}

type bp06Summary struct {
	SchemaVersion int                          `json:"schemaVersion"`
	RunID         string                       `json:"runId"`
	ReleaseGating bool                         `json:"releaseGating"`
	Valid         bool                         `json:"valid"`
	Targets       map[string]bp06TargetSummary `json:"targets"`
	FailureReason string                       `json:"failureReason,omitempty"`
}

type bp06Difference struct {
	Dimension string `json:"dimension"`
	Reason    string `json:"reason"`
}

type bp06ComparisonTarget struct {
	Success             bool   `json:"success"`
	Valid               bool   `json:"valid"`
	WorkloadSummaryPath string `json:"workloadSummaryPath"`
	ResourceSummaryPath string `json:"resourceSummaryPath"`
	FailureSummaryPath  string `json:"failureSummaryPath"`
	FailureReason       string `json:"failureReason,omitempty"`
}

type bp06Comparison struct {
	SchemaVersion         int                             `json:"schemaVersion"`
	RunID                 string                          `json:"runId"`
	Equivalent            bool                            `json:"equivalent"`
	ReleaseGating         bool                            `json:"releaseGating"`
	Reason                string                          `json:"reason"`
	ProfileSHA256         string                          `json:"profileSha256"`
	ResolvedProfile       Profile                         `json:"resolvedProfile"`
	ResourceLimits        bp06ResourceLimits              `json:"resourceLimits"`
	Topology              bp06Topology                    `json:"topology"`
	RunnerImageID         string                          `json:"runnerImageId"`
	RTODirectlyComparable bool                            `json:"rtoDirectlyComparable"`
	NonEquivalent         []bp06Difference                `json:"nonEquivalent"`
	Targets               map[string]bp06ComparisonTarget `json:"targets"`
}

type bp06RunOptions struct {
	Profile       Profile
	ProfileBytes  []byte
	ProfileSHA256 string
	RepoRoot      string
	RunID         string
	RunDir        string
	Images        environment.ImageSet
	Metadata      bp06HostMetadata
	Command       []string
	TargetOrder   []string
}

func compareCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-06 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp06", "root directory for benchmark artifacts")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected compare arguments: %v", flags.Args())
	}
	if strings.TrimSpace(*profilePath) == "" {
		return errors.New("compare requires --profile")
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
	repoRoot, err := bp06RepositoryRoot()
	if err != nil {
		return err
	}
	runID := newBP06RunID()
	runDir := filepath.Join(outputAbs, runID)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("create BP-06 artifact directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "profile.json"), profileBytes, 0o644); err != nil {
		return fmt.Errorf("write profile source: %w", err)
	}
	options := bp06RunOptions{
		Profile:       profile,
		ProfileBytes:  profileBytes,
		ProfileSHA256: profileHash,
		RepoRoot:      repoRoot,
		RunID:         runID,
		RunDir:        runDir,
		Metadata:      collectBP06HostMetadata(repoRoot),
		Command:       []string{"compare", "--profile", *profilePath, "--output", *outputRoot},
		TargetOrder:   []string{"narumq", "emqx"},
	}
	current := newBP06Manifest(options, time.Now().UTC())
	writeFailure := func(cause error) error {
		current.FailureReason = cause.Error()
		current.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if writeErr := writeBP06RunArtifacts(runDir, current); writeErr != nil {
			return errors.Join(cause, fmt.Errorf("write failure artifacts: %w", writeErr))
		}
		_, _ = fmt.Fprintln(stdout, filepath.Join(runDir, "manifest.json"))
		return cause
	}
	if err := environment.EnsureEMQXImage(ctx, runDir, bp06EMQXImageReference); err != nil {
		return writeFailure(err)
	}
	images, err := environment.BuildImages(ctx, environment.BuildOptions{
		BenchmarkID:  "bp06",
		RunID:        runID,
		RepoRoot:     repoRoot,
		ArtifactDir:  runDir,
		RunnerTarget: "bp06-runner",
	})
	if err != nil {
		return writeFailure(err)
	}
	options.Images = images
	current.RunnerImage = images.RunnerReference
	current.RunnerImageID = images.RunnerID
	final, runErr := executeBP06Comparison(ctx, options, current)
	if writeErr := writeBP06RunArtifacts(runDir, final); writeErr != nil {
		if runErr != nil {
			return errors.Join(runErr, fmt.Errorf("write comparison artifacts: %w", writeErr))
		}
		return fmt.Errorf("write comparison artifacts: %w", writeErr)
	}
	if _, err := fmt.Fprintln(stdout, filepath.Join(runDir, "manifest.json")); err != nil {
		if runErr != nil {
			return errors.Join(runErr, fmt.Errorf("write compare result: %w", err))
		}
		return fmt.Errorf("write compare result: %w", err)
	}
	return runErr
}

func newBP06Manifest(options bp06RunOptions, startedAt time.Time) bp06Manifest {
	imageIndex := ""
	if parts := strings.SplitN(bp06EMQXImageReference, "@", 2); len(parts) == 2 {
		imageIndex = parts[1]
	}
	return bp06Manifest{
		SchemaVersion:              1,
		RunID:                      options.RunID,
		StartedAt:                  startedAt.UTC().Format(time.RFC3339Nano),
		Purpose:                    options.Profile.Purpose,
		ReleaseGating:              false,
		Profile:                    bp06ArtifactProfile{RawSHA256: options.ProfileSHA256, Resolved: options.Profile},
		Git:                        options.Metadata.Git,
		GoVersion:                  options.Metadata.GoVersion,
		OS:                         runtime.GOOS,
		Architecture:               runtime.GOARCH,
		Docker:                     bp06DockerMetadata{ClientServerVersion: options.Metadata.DockerVersion},
		Command:                    append([]string(nil), options.Command...),
		BuildBaseImage:             environment.BuildBaseImageReference,
		PlannedBuildBaseImage:      environment.PlannedBuildBaseImage,
		BuildBaseImageSubstitution: bp06BuildBaseImageSubstitution,
		EMQX: bp06EMQXMetadata{
			Version:        "5.8.8",
			Edition:        "Enterprise",
			License:        "EMQX Enterprise evaluation; 25 concurrent sessions; non-production only",
			ImageReference: bp06EMQXImageReference,
			ImageIndex:     imageIndex,
			Historical:     true,
		},
		Topology: bp06Topology{
			Nodes:              3,
			NetworkMode:        bp06NetworkMode,
			NaruMQStorage:      "Raft WAL asynchronous single-writer group commit with fsync before dependent replication and ACK on docker named volume",
			EMQXSessionStorage: "durable sessions; builtin_raft messages; 3 sites; 3 replicas; 1 shard; docker named volumes",
			RuntimeMetrics:     "not_collected: -cluster-config rejects other flags",
		},
		SummaryPath:    filepath.Join(options.RunDir, "summary.json"),
		ComparisonPath: filepath.Join(options.RunDir, "comparison.json"),
		Targets:        make(map[string]bp06TargetManifest),
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-242
func executeBP06Comparison(ctx context.Context, options bp06RunOptions, manifest bp06Manifest) (final bp06Manifest, runErr error) {
	if len(options.TargetOrder) == 0 {
		options.TargetOrder = []string{"narumq", "emqx"}
	}
	if err := validateBP06TargetOrder(options.TargetOrder); err != nil {
		manifest.FailureReason = err.Error()
		manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return manifest, err
	}
	if err := os.MkdirAll(options.RunDir, 0o755); err != nil {
		manifest.FailureReason = fmt.Sprintf("create run artifact directory: %v", err)
		return manifest, err
	}
	if err := os.WriteFile(filepath.Join(options.RunDir, "profile.json"), options.ProfileBytes, 0o644); err != nil {
		manifest.FailureReason = fmt.Sprintf("write profile source: %v", err)
		return manifest, err
	}
	var runErrors []error
	fixture, err := newBP06Fixture(options.Profile, options.RunID)
	if err != nil {
		manifest.FailureReason = fmt.Sprintf("create benchmark fixture: %v", err)
		manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return manifest, err
	}
	defer func() {
		if closeErr := fixture.Close(); closeErr != nil {
			closeErr = fmt.Errorf("remove temporary fixture: %w", closeErr)
			runErr = errors.Join(runErr, closeErr)
			if manifest.FailureReason == "" {
				manifest.FailureReason = closeErr.Error()
			} else {
				manifest.FailureReason = errors.Join(errors.New(manifest.FailureReason), closeErr).Error()
			}
			manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
			final = manifest
		}
	}()
	timeout := bp06Timeout(options.Profile)
	for _, target := range options.TargetOrder {
		targetManifest, targetErr := runBP06Target(ctx, options, fixture, target, timeout)
		manifest.Targets[target] = targetManifest
		if targetErr != nil {
			runErrors = append(runErrors, fmt.Errorf("%s: %w", target, targetErr))
		}
	}
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if len(runErrors) > 0 {
		manifest.FailureReason = errors.Join(runErrors...).Error()
	}
	return manifest, errors.Join(runErrors...)
}

func validateBP06TargetOrder(order []string) error {
	if len(order) != 2 {
		return errors.New("target order must contain exactly two targets")
	}
	seen := make(map[string]struct{}, len(order))
	for _, target := range order {
		if target != "narumq" && target != "emqx" {
			return fmt.Errorf("unsupported BP-06 target %q", target)
		}
		if _, exists := seen[target]; exists {
			return fmt.Errorf("target order contains duplicate %q", target)
		}
		seen[target] = struct{}{}
	}
	return nil
}

func runBP06Target(ctx context.Context, options bp06RunOptions, fixture *bp06Fixture, target string, timeout time.Duration) (bp06TargetManifest, error) {
	artifactDir := filepath.Join(options.RunDir, target)
	if err := os.MkdirAll(filepath.Join(artifactDir, "control"), 0o755); err != nil {
		return bp06TargetManifest{Target: target, Status: "failed", ArtifactDirectory: artifactDir, FailureReason: err.Error()}, err
	}
	profilePath := filepath.Join(options.RunDir, "profile.json")
	resourceLimits := bp06ResourceLimits{
		BrokerCPUs:       options.Profile.BrokerCPUs,
		BrokerMemory:     options.Profile.BrokerMemoryBytes,
		GeneratorCPUs:    options.Profile.GeneratorCPUs,
		GeneratorMemory:  options.Profile.GeneratorMemoryBytes,
		BrokerReadOnlyFS: true,
		RunnerReadOnlyFS: true,
		NetworkMode:      "bridge",
	}
	clusterOptions := environment.ClusterTargetOptions{
		BenchmarkID:                 "bp06",
		RunID:                       options.RunID,
		Target:                      target,
		ArtifactDirectory:           artifactDir,
		ProfilePath:                 profilePath,
		RunnerReference:             options.Images.RunnerReference,
		RunnerImageID:               options.Images.RunnerID,
		RunnerTarget:                options.Images.RunnerTarget,
		NetworkOwnerArgs:            []string{"serve-jwks", "--file", "/config/jwks.json", "--listen", "127.0.0.1:18080"},
		NetworkOwnerConfigDirectory: fixture.NetworkOwnerConfigDirectory,
		RunnerConfigDirectory:       fixture.RunnerConfigDirectory,
		Nodes:                       bp06ClusterNodes(target, options, fixture),
		BrokerCPUs:                  options.Profile.BrokerCPUs,
		BrokerMemoryBytes:           options.Profile.BrokerMemoryBytes,
		GeneratorCPUs:               options.Profile.GeneratorCPUs,
		GeneratorMemoryBytes:        options.Profile.GeneratorMemoryBytes,
		Timeout:                     timeout,
		StartupTimeout:              120 * time.Second,
		AllowBrokerRestart:          true,
	}
	if target == "narumq" {
		clusterOptions.ImageReference = options.Images.NaruMQReference
		clusterOptions.RunnerArgs = []string{
			"run", "--profile", "/input/profile.json", "--target", "narumq",
			"--nodes", "node1.bp06.test:1883,node2.bp06.test:1883,node3.bp06.test:1883",
			"--management", "node1.bp06.test:8080,node2.bp06.test:8080,node3.bp06.test:8080",
			"--credentials", "/runner-config/credentials.json", "--output", "/output",
		}
	} else {
		clusterOptions.ImageReference = bp06EMQXImageReference
		clusterOptions.EMQXReadyCondition = bp06EMQXDurabilityReady
		clusterOptions.EMQXSnapshotExpression = bp06EMQXDurabilitySnapshot
		clusterOptions.RunnerArgs = []string{
			"run", "--profile", "/input/profile.json", "--target", "emqx",
			"--nodes", "node1.bp06.test:1883,node2.bp06.test:1883,node3.bp06.test:1883",
			"--credentials", "/runner-config/credentials.json", "--output", "/output",
		}
	}
	clusterOptions.Controller = newFailureController(target, options.Profile, options.RunID, artifactDir, fixture, clusterOptions)
	result, runErr := environment.RunClusterTarget(ctx, clusterOptions)
	manifest := bp06TargetManifest{
		Target:              target,
		Status:              result.Status,
		Success:             result.Success,
		StartedAt:           result.StartedAt,
		FinishedAt:          result.FinishedAt,
		ArtifactDirectory:   artifactDir,
		WorkloadSummaryPath: filepath.Join(artifactDir, "workload-summary.json"),
		ResourceSummaryPath: filepath.Join(artifactDir, "resource-summary.json"),
		FailureSummaryPath:  filepath.Join(artifactDir, "failure-summary.json"),
		PhasesPath:          filepath.Join(artifactDir, "phases.jsonl"),
		MessagesPath:        filepath.Join(artifactDir, "messages.jsonl"),
		RunnerLogPath:       result.RunnerLogPath,
		ImageReference:      result.ImageReference,
		ImageID:             result.ImageID,
		ImageRepoDigests:    append([]string(nil), result.ImageRepoDigests...),
		RunnerImageID:       result.RunnerImageID,
		ProfileSHA256:       options.ProfileSHA256,
		ResolvedProfile:     options.Profile,
		ResourceLimits:      resourceLimits,
		NetworkMode:         bp06NetworkMode,
		FailureReason:       result.FailureReason,
	}
	manifest.Nodes = make([]bp06NodeArtifacts, 0, len(result.Nodes))
	for _, node := range result.Nodes {
		manifest.Nodes = append(manifest.Nodes, bp06NodeArtifacts{
			Name:                 node.Name,
			BrokerContainer:      node.BrokerContainer,
			NetworkOwner:         node.NetworkOwnerContainer,
			BrokerStatsPath:      node.BrokerStatsPath,
			NetworkStatsPath:     node.NetworkStatsPath,
			BrokerLogPath:        node.BrokerLogPath,
			ContainerInspectPath: node.ContainerInspectPath,
		})
	}
	var artifactErrors []error
	var workload workloadSummary
	if err := readBP06JSON(manifest.WorkloadSummaryPath, &workload); err != nil {
		artifactErrors = append(artifactErrors, fmt.Errorf("read workload summary: %w", err))
	} else {
		manifest.WorkloadSummary = &workload
	}
	if manifest.WorkloadSummary != nil {
		resources, err := buildResourceSummary(target, result.Nodes, filepath.Join(artifactDir, "failure.jsonl"))
		if err != nil {
			resources.Valid = false
			resources.FailureReasons = append(resources.FailureReasons, err.Error())
			artifactErrors = append(artifactErrors, fmt.Errorf("build resource summary: %w", err))
		}
		manifest.ResourceSummary = &resources
		if err := writeResourceSummary(artifactDir, resources); err != nil {
			artifactErrors = append(artifactErrors, fmt.Errorf("write resource summary: %w", err))
		}
	}
	var failure failureSummary
	if err := readBP06JSON(manifest.FailureSummaryPath, &failure); err != nil {
		artifactErrors = append(artifactErrors, fmt.Errorf("read failure summary: %w", err))
	} else {
		manifest.FailureSummary = &failure
	}
	manifest.Valid = result.Success && manifest.WorkloadSummary != nil && manifest.WorkloadSummary.Valid && manifest.ResourceSummary != nil && manifest.ResourceSummary.Valid && manifest.FailureSummary != nil && manifest.FailureSummary.Valid
	if !manifest.Valid && manifest.FailureReason == "" {
		manifest.FailureReason = "target artifact validity requirements were not met"
	}
	if runErr != nil {
		artifactErrors = append(artifactErrors, runErr)
	}
	if !manifest.Valid && len(artifactErrors) == 0 {
		artifactErrors = append(artifactErrors, errors.New(manifest.FailureReason))
	}
	if len(artifactErrors) > 0 {
		manifest.Status = "failed"
		manifest.Success = false
		if manifest.FailureReason == "" {
			manifest.FailureReason = errors.Join(artifactErrors...).Error()
		} else {
			manifest.FailureReason = errors.Join(append([]error{errors.New(manifest.FailureReason)}, artifactErrors...)...).Error()
		}
		return manifest, errors.New(manifest.FailureReason)
	}
	return manifest, nil
}

func bp06ClusterNodes(target string, options bp06RunOptions, fixture *bp06Fixture) []environment.ClusterNodeSpec {
	nodes := make([]environment.ClusterNodeSpec, 3)
	for index := range nodes {
		number := index + 1
		name := fmt.Sprintf("node%d", number)
		node := environment.ClusterNodeSpec{
			Name:            name,
			DNSName:         name + ".bp06.test",
			ConfigDirectory: fixture.NodeConfigDirectories[index],
			MountConfig:     target == "narumq",
			DataDestination: "/var/lib/narumq",
		}
		if target == "narumq" {
			node.BrokerArgs = []string{"-cluster-config", "/config/node.json"}
			node.PublishedPorts = []string{"8080/tcp"}
		} else {
			node.ConfigDirectory = fixture.EMQXConfigDirectory
			node.MountConfig = true
			node.DataDestination = "/opt/emqx/data"
			node.LogDestination = "/opt/emqx/log"
			node.ReadyLogMarker = "EMQX Enterprise 5.8.8 is running now!"
			node.BrokerEnv = []string{
				"EMQX_NODE__NAME=emqx@" + name + ".bp06.test",
				"EMQX_NODE__COOKIE=bp06-" + options.RunID + "-emqx-cookie",
				"EMQX_CLUSTER__DISCOVERY_STRATEGY=static",
				"EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.bp06.test,emqx@node2.bp06.test,emqx@node3.bp06.test]",
				"EMQX_DURABLE_SESSIONS__ENABLE=true",
				"EMQX_DURABLE_STORAGE__MESSAGES__BACKEND=builtin_raft",
				"EMQX_DURABLE_STORAGE__MESSAGES__N_SITES=3",
				"EMQX_DURABLE_STORAGE__MESSAGES__REPLICATION_FACTOR=3",
				"EMQX_DURABLE_STORAGE__MESSAGES__N_SHARDS=1",
				`EMQX_AUTHENTICATION=[{"mechanism":"jwt","enable":true,"use_jwks":true,"endpoint":"http://127.0.0.1:18080/jwks.json","refresh_interval":300,"from":"password","verify_claims":{"iss":"https://issuer.example","aud":"narumq","sub":"bp06-client"}}]`,
				`EMQX_AUTHORIZATION__SOURCES=[{"type":"file","enable":true,"path":"/config/acl.conf"}]`,
				"EMQX_AUTHORIZATION__NO_MATCH=deny",
			}
		}
		nodes[index] = node
	}
	return nodes
}

func bp06ResourceLimitsEqual(left, right bp06ResourceLimits) bool {
	return reflect.DeepEqual(left, right)
}

func buildBP06Comparison(manifest bp06Manifest, summary bp06Summary) bp06Comparison {
	left, leftOK := manifest.Targets["narumq"]
	right, rightOK := manifest.Targets["emqx"]
	equivalent := leftOK && rightOK && left.ProfileSHA256 == right.ProfileSHA256 &&
		bp06ResourceLimitsEqual(left.ResourceLimits, right.ResourceLimits) &&
		left.RunnerImageID == right.RunnerImageID && manifest.Topology.Nodes == 3
	comparison := bp06Comparison{
		SchemaVersion:         1,
		RunID:                 manifest.RunID,
		Equivalent:            equivalent,
		ReleaseGating:         false,
		Reason:                "identical workload profile, resource limits, runner image, and three-node topology; exact ACK/storage, crash durability, security, resync conditions, and session recovery contracts remain non-equivalent",
		ProfileSHA256:         manifest.Profile.RawSHA256,
		ResolvedProfile:       manifest.Profile.Resolved,
		ResourceLimits:        left.ResourceLimits,
		Topology:              manifest.Topology,
		RunnerImageID:         manifest.RunnerImageID,
		RTODirectlyComparable: false,
		NonEquivalent: []bp06Difference{
			{Dimension: "durability", Reason: "EMQX uses Durable Storage batching and session persistence; equivalence to NaruMQ's quorum WAL fsync-before-ACK contract is not verified"},
			{Dimension: "authentication", Reason: "both validate JWKS JWT; EMQX fixture does not reproduce NaruMQ tenant/client-ID bindings or peer mTLS"},
			{Dimension: "resyncCondition", Reason: "NaruMQ resync completes when the leader match index for the restarted voter reaches the leader applied index observed after restart; EMQX resync completes when the pinned cluster readiness condition passes"},
			{Dimension: "sessionRecovery", Reason: "NaruMQ restores persistent sessions from replicated session metadata; EMQX resumes durable sessions from builtin_raft storage; equivalence is not proven"},
			{Dimension: "crashDurability", Reason: "EMQX 5.8.8 acknowledges QoS 1/2 publishes before durable storage replication (store_batch sync=false) and checkpoints durable session state every durable_sessions.heartbeat_interval (5000ms); SIGKILL-window ACKed gaps and QoS 2 redeliveries are recorded as comparatorDurabilityExceptions instead of invalidating the EMQX target; NaruMQ has no such exception"},
		},
		Targets: make(map[string]bp06ComparisonTarget, 2),
	}
	for _, target := range []string{"narumq", "emqx"} {
		entry, ok := manifest.Targets[target]
		if !ok {
			continue
		}
		comparison.Targets[target] = bp06ComparisonTarget{
			Success:             entry.Success,
			Valid:               summary.Targets[target].Valid,
			WorkloadSummaryPath: entry.WorkloadSummaryPath,
			ResourceSummaryPath: entry.ResourceSummaryPath,
			FailureSummaryPath:  entry.FailureSummaryPath,
			FailureReason:       entry.FailureReason,
		}
	}
	return comparison
}

func summarizeBP06Run(manifest bp06Manifest) bp06Summary {
	summary := bp06Summary{SchemaVersion: 1, RunID: manifest.RunID, ReleaseGating: false, Valid: true, Targets: make(map[string]bp06TargetSummary, 2), FailureReason: manifest.FailureReason}
	for _, target := range []string{"narumq", "emqx"} {
		entry, ok := manifest.Targets[target]
		if !ok {
			summary.Valid = false
			if summary.FailureReason == "" {
				summary.FailureReason = "target " + target + " did not produce a manifest"
			}
			continue
		}
		item := bp06TargetSummary{
			Valid:           entry.Valid,
			WorkloadSummary: entry.WorkloadSummary,
			ResourceSummary: entry.ResourceSummary,
			FailureSummary:  entry.FailureSummary,
			FailureReason:   entry.FailureReason,
		}
		summary.Targets[target] = item
		if !entry.Valid {
			summary.Valid = false
			if summary.FailureReason == "" {
				summary.FailureReason = target + " target is invalid"
			}
		}
	}
	return summary
}

func writeBP06RunArtifacts(directory string, manifest bp06Manifest) error {
	summary := summarizeBP06Run(manifest)
	comparison := buildBP06Comparison(manifest, summary)
	var errs []error
	for name, value := range map[string]any{
		"manifest.json":   manifest,
		"summary.json":    summary,
		"comparison.json": comparison,
	} {
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			errs = append(errs, fmt.Errorf("encode %s: %w", name, err))
			continue
		}
		data = append(data, '\n')
		if err := os.WriteFile(filepath.Join(directory, name), data, 0o644); err != nil {
			errs = append(errs, fmt.Errorf("write %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

func readBP06JSON(path string, destination any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func bp06RepositoryRoot() (string, error) {
	current, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("repository root with go.mod was not found")
		}
		current = parent
	}
}

func newBP06RunID() string {
	return "bp06-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}

func collectBP06HostMetadata(repoRoot string) bp06HostMetadata {
	commit, _ := bp06CommandText(context.Background(), repoRoot, "git", "rev-parse", "HEAD")
	status, _ := bp06CommandText(context.Background(), repoRoot, "git", "status", "--porcelain", "--untracked-files=all")
	goVersion, _ := bp06CommandText(context.Background(), repoRoot, "go", "version")
	dockerVersion, _ := bp06CommandText(context.Background(), repoRoot, "docker", "version", "--format", "{{json .}}")
	return bp06HostMetadata{
		Git:           bp06GitMetadata{Commit: strings.TrimSpace(commit), Dirty: strings.TrimSpace(status) != ""},
		GoVersion:     strings.TrimSpace(goVersion),
		DockerVersion: strings.TrimSpace(dockerVersion),
	}
}

func bp06CommandText(ctx context.Context, workingDirectory, name string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, name, args...)
	if workingDirectory != "" {
		command.Dir = workingDirectory
	}
	output, err := command.CombinedOutput()
	return string(output), err
}
