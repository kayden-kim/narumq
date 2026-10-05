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
	bp07BuildBaseImageSubstitution = "planned digest was unavailable in Docker Hub; immutable digest resolved from golang:1.26.5-bookworm on 2026-09-19"
	bp07NetworkMode                = "bridge network; broker shares netns owner"
	bp07EMQXImageReference         = "emqx/emqx-enterprise:5.8.8@sha256:d166efa91eae706ffbfca3f47e36fae0fd441507a8a57bda622da09e4a019c14"
	bp07EMQXDurabilityReady        = `begin
		Config = emqx_ds_replication_layer_meta:db_config(messages),
		Sites = emqx_ds_replication_layer_meta:db_sites(messages),
		Shards = emqx_ds_replication_layer_meta:shards(messages),
		{ok, Authenticators} = emqx_authn_chains:list_authenticators('mqtt:global'),
		emqx:get_config([durable_sessions, enable]) =:= true andalso
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
	bp07EMQXDurabilitySnapshot = `{emqx:get_config([durable_sessions]),
		emqx_ds_replication_layer_meta:db_config(messages), emqx:get_config([authentication]),
		[{Shard, emqx_ds_replication_layer_meta:replica_set(messages, Shard),
			emqx_ds_replication_layer_meta:replica_set_transitions(messages, Shard)}
			|| Shard <- emqx_ds_replication_layer_meta:shards(messages)]}`
)

type bp07ArtifactProfile struct {
	RawSHA256 string  `json:"rawSha256"`
	Resolved  Profile `json:"resolved"`
}

type bp07GitMetadata struct {
	Commit string `json:"commit"`
	Dirty  bool   `json:"dirty"`
}

type bp07DockerMetadata struct {
	ClientServerVersion string `json:"clientServerVersion"`
}

type bp07HostMetadata struct {
	Git           bp07GitMetadata
	GoVersion     string
	DockerVersion string
}

type bp07ResourceLimits struct {
	BrokerCPUs       float64 `json:"brokerCpus"`
	BrokerMemory     int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs    float64 `json:"generatorCpus"`
	GeneratorMemory  int64   `json:"generatorMemoryBytes"`
	BrokerReadOnlyFS bool    `json:"brokerReadOnlyRootFilesystem"`
	RunnerReadOnlyFS bool    `json:"runnerReadOnlyRootFilesystem"`
	NetworkMode      string  `json:"networkMode"`
}

type bp07Topology struct {
	Nodes              int    `json:"nodes"`
	NetworkMode        string `json:"networkMode"`
	NaruMQStorage      string `json:"naruMQStorage"`
	EMQXSessionStorage string `json:"emqxSessionStorage"`
	RuntimeMetrics     string `json:"runtimeMetrics"`
}

type bp07EMQXMetadata struct {
	Version        string `json:"version"`
	Edition        string `json:"edition"`
	License        string `json:"license"`
	EOL            string `json:"eol,omitempty"`
	ImageReference string `json:"imageReference"`
	ImageIndex     string `json:"imageIndexDigest"`
	Historical     bool   `json:"historicalComparator"`
}

type bp07NodeArtifacts struct {
	Name                 string `json:"name"`
	BrokerContainer      string `json:"brokerContainer"`
	NetworkOwner         string `json:"networkOwnerContainer"`
	BrokerStatsPath      string `json:"brokerStatsPath"`
	NetworkStatsPath     string `json:"networkStatsPath"`
	BrokerLogPath        string `json:"brokerLogPath"`
	ContainerInspectPath string `json:"containerInspectPath"`
}

type bp07TargetManifest struct {
	Target              string              `json:"target"`
	Status              string              `json:"status"`
	Success             bool                `json:"success"`
	Valid               bool                `json:"valid"`
	StartedAt           string              `json:"startedAt,omitempty"`
	FinishedAt          string              `json:"finishedAt,omitempty"`
	ArtifactDirectory   string              `json:"artifactDirectory"`
	WorkloadSummaryPath string              `json:"workloadSummaryPath"`
	ResourceSummaryPath string              `json:"resourceSummaryPath"`
	RollingSummaryPath  string              `json:"rollingSummaryPath"`
	PhasesPath          string              `json:"phasesPath"`
	MessagesPath        string              `json:"messagesPath"`
	RunnerLogPath       string              `json:"runnerLogPath,omitempty"`
	Nodes               []bp07NodeArtifacts `json:"nodes,omitempty"`
	ImageReference      string              `json:"imageReference"`
	ImageID             string              `json:"imageId,omitempty"`
	ImageRepoDigests    []string            `json:"imageRepoDigests,omitempty"`
	RunnerImageID       string              `json:"runnerImageId"`
	ProfileSHA256       string              `json:"profileSha256"`
	ResolvedProfile     Profile             `json:"resolvedProfile"`
	ResourceLimits      bp07ResourceLimits  `json:"resourceLimits"`
	NetworkMode         string              `json:"networkMode"`
	WorkloadSummary     *workloadSummary    `json:"workloadSummary,omitempty"`
	ResourceSummary     *resourceSummary    `json:"resourceSummary,omitempty"`
	RollingSummary      *rollingSummary     `json:"rollingSummary,omitempty"`
	FailureReason       string              `json:"failureReason,omitempty"`
}

type bp07Manifest struct {
	SchemaVersion              int                           `json:"schemaVersion"`
	RunID                      string                        `json:"runId"`
	StartedAt                  string                        `json:"startedAt"`
	FinishedAt                 string                        `json:"finishedAt,omitempty"`
	Purpose                    string                        `json:"purpose"`
	ReleaseGating              bool                          `json:"releaseGating"`
	Profile                    bp07ArtifactProfile           `json:"profile"`
	Git                        bp07GitMetadata               `json:"git"`
	GoVersion                  string                        `json:"goVersion"`
	OS                         string                        `json:"os"`
	Architecture               string                        `json:"architecture"`
	Docker                     bp07DockerMetadata            `json:"docker"`
	Command                    []string                      `json:"command"`
	BuildBaseImage             string                        `json:"buildBaseImage"`
	PlannedBuildBaseImage      string                        `json:"plannedBuildBaseImage"`
	BuildBaseImageSubstitution string                        `json:"buildBaseImageSubstitution"`
	RunnerImage                string                        `json:"runnerImage"`
	RunnerImageID              string                        `json:"runnerImageId"`
	EMQX                       bp07EMQXMetadata              `json:"emqx"`
	Topology                   bp07Topology                  `json:"topology"`
	SummaryPath                string                        `json:"summaryPath"`
	ComparisonPath             string                        `json:"comparisonPath"`
	Targets                    map[string]bp07TargetManifest `json:"targets"`
	FailureReason              string                        `json:"failureReason,omitempty"`
}

type bp07TargetSummary struct {
	Valid           bool             `json:"valid"`
	WorkloadSummary *workloadSummary `json:"workloadSummary,omitempty"`
	ResourceSummary *resourceSummary `json:"resourceSummary,omitempty"`
	RollingSummary  *rollingSummary  `json:"rollingSummary,omitempty"`
	FailureReason   string           `json:"failureReason,omitempty"`
}

type bp07Summary struct {
	SchemaVersion int                          `json:"schemaVersion"`
	RunID         string                       `json:"runId"`
	ReleaseGating bool                         `json:"releaseGating"`
	Valid         bool                         `json:"valid"`
	Targets       map[string]bp07TargetSummary `json:"targets"`
	FailureReason string                       `json:"failureReason,omitempty"`
}

type bp07Difference struct {
	Dimension string `json:"dimension"`
	Reason    string `json:"reason"`
}

type bp07ComparisonTarget struct {
	Success             bool   `json:"success"`
	Valid               bool   `json:"valid"`
	WorkloadSummaryPath string `json:"workloadSummaryPath"`
	ResourceSummaryPath string `json:"resourceSummaryPath"`
	RollingSummaryPath  string `json:"rollingSummaryPath"`
	FailureReason       string `json:"failureReason,omitempty"`
}

type bp07Comparison struct {
	SchemaVersion               int                             `json:"schemaVersion"`
	RunID                       string                          `json:"runId"`
	Equivalent                  bool                            `json:"equivalent"`
	ReleaseGating               bool                            `json:"releaseGating"`
	Reason                      string                          `json:"reason"`
	ProfileSHA256               string                          `json:"profileSha256"`
	ResolvedProfile             Profile                         `json:"resolvedProfile"`
	ResourceLimits              bp07ResourceLimits              `json:"resourceLimits"`
	Topology                    bp07Topology                    `json:"topology"`
	RunnerImageID               string                          `json:"runnerImageId"`
	DrainTimeDirectlyComparable bool                            `json:"drainTimeDirectlyComparable"`
	NonEquivalent               []bp07Difference                `json:"nonEquivalent"`
	Targets                     map[string]bp07ComparisonTarget `json:"targets"`
}

type bp07RunOptions struct {
	Profile       Profile
	ProfileBytes  []byte
	ProfileSHA256 string
	RepoRoot      string
	RunID         string
	RunDir        string
	Images        environment.ImageSet
	Metadata      bp07HostMetadata
	Command       []string
	TargetOrder   []string
}

func compareCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-07 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp07", "root directory for benchmark artifacts")
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
	repoRoot, err := bp07RepositoryRoot()
	if err != nil {
		return err
	}
	runID := newBP07RunID()
	runDir := filepath.Join(outputAbs, runID)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("create BP-07 artifact directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "profile.json"), profileBytes, 0o644); err != nil {
		return fmt.Errorf("write profile source: %w", err)
	}
	options := bp07RunOptions{
		Profile:       profile,
		ProfileBytes:  profileBytes,
		ProfileSHA256: profileHash,
		RepoRoot:      repoRoot,
		RunID:         runID,
		RunDir:        runDir,
		Metadata:      collectBP07HostMetadata(repoRoot),
		Command:       []string{"compare", "--profile", *profilePath, "--output", *outputRoot},
		TargetOrder:   []string{"narumq", "emqx"},
	}
	current := newBP07Manifest(options, time.Now().UTC())
	writeFailure := func(cause error) error {
		current.FailureReason = cause.Error()
		current.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if writeErr := writeBP07RunArtifacts(runDir, current); writeErr != nil {
			return errors.Join(cause, fmt.Errorf("write failure artifacts: %w", writeErr))
		}
		_, _ = fmt.Fprintln(stdout, filepath.Join(runDir, "manifest.json"))
		return cause
	}
	if err := environment.EnsureEMQXImage(ctx, runDir, bp07EMQXImageReference); err != nil {
		return writeFailure(err)
	}
	images, err := environment.BuildImages(ctx, environment.BuildOptions{
		BenchmarkID:  "bp07",
		RunID:        runID,
		RepoRoot:     repoRoot,
		ArtifactDir:  runDir,
		RunnerTarget: "bp07-runner",
	})
	if err != nil {
		return writeFailure(err)
	}
	options.Images = images
	current.RunnerImage = images.RunnerReference
	current.RunnerImageID = images.RunnerID
	final, runErr := executeBP07Comparison(ctx, options, current)
	if writeErr := writeBP07RunArtifacts(runDir, final); writeErr != nil {
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

func newBP07Manifest(options bp07RunOptions, startedAt time.Time) bp07Manifest {
	imageIndex := ""
	if parts := strings.SplitN(bp07EMQXImageReference, "@", 2); len(parts) == 2 {
		imageIndex = parts[1]
	}
	return bp07Manifest{
		SchemaVersion:              1,
		RunID:                      options.RunID,
		StartedAt:                  startedAt.UTC().Format(time.RFC3339Nano),
		Purpose:                    options.Profile.Purpose,
		ReleaseGating:              false,
		Profile:                    bp07ArtifactProfile{RawSHA256: options.ProfileSHA256, Resolved: options.Profile},
		Git:                        options.Metadata.Git,
		GoVersion:                  options.Metadata.GoVersion,
		OS:                         runtime.GOOS,
		Architecture:               runtime.GOARCH,
		Docker:                     bp07DockerMetadata{ClientServerVersion: options.Metadata.DockerVersion},
		Command:                    append([]string(nil), options.Command...),
		BuildBaseImage:             environment.BuildBaseImageReference,
		PlannedBuildBaseImage:      environment.PlannedBuildBaseImage,
		BuildBaseImageSubstitution: bp07BuildBaseImageSubstitution,
		EMQX: bp07EMQXMetadata{
			Version:        "5.8.8",
			Edition:        "Enterprise",
			License:        "EMQX Enterprise evaluation; 25 concurrent sessions; non-production only",
			ImageReference: bp07EMQXImageReference,
			ImageIndex:     imageIndex,
			Historical:     true,
		},
		Topology: bp07Topology{
			Nodes:              3,
			NetworkMode:        bp07NetworkMode,
			NaruMQStorage:      "Raft WAL asynchronous single-writer group commit with fsync before dependent replication and ACK on docker named volume",
			EMQXSessionStorage: "durable sessions; builtin_raft messages; 3 sites; 3 replicas; 1 shard; docker named volumes",
			RuntimeMetrics:     "not_collected: -cluster-config rejects other flags",
		},
		SummaryPath:    filepath.Join(options.RunDir, "summary.json"),
		ComparisonPath: filepath.Join(options.RunDir, "comparison.json"),
		Targets:        make(map[string]bp07TargetManifest),
	}
}

func executeBP07Comparison(ctx context.Context, options bp07RunOptions, manifest bp07Manifest) (final bp07Manifest, runErr error) {
	if len(options.TargetOrder) == 0 {
		options.TargetOrder = []string{"narumq", "emqx"}
	}
	if err := validateBP07TargetOrder(options.TargetOrder); err != nil {
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
	fixture, err := newBP07Fixture(options.Profile, options.RunID)
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
	timeout := bp07Timeout(options.Profile)
	for _, target := range options.TargetOrder {
		targetManifest, targetErr := runBP07Target(ctx, options, fixture, target, timeout)
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

func validateBP07TargetOrder(order []string) error {
	if len(order) != 2 {
		return errors.New("target order must contain exactly two targets")
	}
	seen := make(map[string]struct{}, len(order))
	for _, target := range order {
		if target != "narumq" && target != "emqx" {
			return fmt.Errorf("unsupported BP-07 target %q", target)
		}
		if _, exists := seen[target]; exists {
			return fmt.Errorf("target order contains duplicate %q", target)
		}
		seen[target] = struct{}{}
	}
	return nil
}

func runBP07Target(ctx context.Context, options bp07RunOptions, fixture *bp07Fixture, target string, timeout time.Duration) (bp07TargetManifest, error) {
	artifactDir := filepath.Join(options.RunDir, target)
	if err := os.MkdirAll(filepath.Join(artifactDir, "control"), 0o755); err != nil {
		return bp07TargetManifest{Target: target, Status: "failed", ArtifactDirectory: artifactDir, FailureReason: err.Error()}, err
	}
	profilePath := filepath.Join(options.RunDir, "profile.json")
	resourceLimits := bp07ResourceLimits{
		BrokerCPUs:       options.Profile.BrokerCPUs,
		BrokerMemory:     options.Profile.BrokerMemoryBytes,
		GeneratorCPUs:    options.Profile.GeneratorCPUs,
		GeneratorMemory:  options.Profile.GeneratorMemoryBytes,
		BrokerReadOnlyFS: true,
		RunnerReadOnlyFS: true,
		NetworkMode:      "bridge",
	}
	clusterOptions := environment.ClusterTargetOptions{
		BenchmarkID:                 "bp07",
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
		Nodes:                       bp07ClusterNodes(target, options, fixture),
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
			"--nodes", "node1.bp07.test:1883,node2.bp07.test:1883,node3.bp07.test:1883",
			"--management", "node1.bp07.test:8080,node2.bp07.test:8080,node3.bp07.test:8080",
			"--credentials", "/runner-config/credentials.json", "--output", "/output",
		}
	} else {
		clusterOptions.ImageReference = bp07EMQXImageReference
		clusterOptions.EMQXReadyCondition = bp07EMQXDurabilityReady
		clusterOptions.EMQXSnapshotExpression = bp07EMQXDurabilitySnapshot
		clusterOptions.RunnerArgs = []string{
			"run", "--profile", "/input/profile.json", "--target", "emqx",
			"--nodes", "node1.bp07.test:1883,node2.bp07.test:1883,node3.bp07.test:1883",
			"--credentials", "/runner-config/credentials.json", "--output", "/output",
		}
	}
	clusterOptions.Controller = newRollingController(target, options.Profile, options.RunID, artifactDir, fixture, clusterOptions)
	result, runErr := environment.RunClusterTarget(ctx, clusterOptions)
	manifest := bp07TargetManifest{
		Target:              target,
		Status:              result.Status,
		Success:             result.Success,
		StartedAt:           result.StartedAt,
		FinishedAt:          result.FinishedAt,
		ArtifactDirectory:   artifactDir,
		WorkloadSummaryPath: filepath.Join(artifactDir, "workload-summary.json"),
		ResourceSummaryPath: filepath.Join(artifactDir, "resource-summary.json"),
		RollingSummaryPath:  filepath.Join(artifactDir, "rolling-summary.json"),
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
		NetworkMode:         bp07NetworkMode,
		FailureReason:       result.FailureReason,
	}
	manifest.Nodes = make([]bp07NodeArtifacts, 0, len(result.Nodes))
	for _, node := range result.Nodes {
		manifest.Nodes = append(manifest.Nodes, bp07NodeArtifacts{
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
	if err := readBP07JSON(manifest.WorkloadSummaryPath, &workload); err != nil {
		artifactErrors = append(artifactErrors, fmt.Errorf("read workload summary: %w", err))
	} else {
		manifest.WorkloadSummary = &workload
	}
	if manifest.WorkloadSummary != nil {
		resources, err := buildResourceSummary(target, result.Nodes, manifest.PhasesPath)
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
	var rolling rollingSummary
	if err := readBP07JSON(manifest.RollingSummaryPath, &rolling); err != nil {
		artifactErrors = append(artifactErrors, fmt.Errorf("read rolling summary: %w", err))
	} else {
		manifest.RollingSummary = &rolling
	}
	manifest.Valid = result.Success && manifest.WorkloadSummary != nil && manifest.WorkloadSummary.Valid && manifest.ResourceSummary != nil && manifest.ResourceSummary.Valid && manifest.RollingSummary != nil && manifest.RollingSummary.Valid
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

func bp07ClusterNodes(target string, options bp07RunOptions, fixture *bp07Fixture) []environment.ClusterNodeSpec {
	nodes := make([]environment.ClusterNodeSpec, 3)
	for index := range nodes {
		number := index + 1
		name := fmt.Sprintf("node%d", number)
		node := environment.ClusterNodeSpec{
			Name:            name,
			DNSName:         name + ".bp07.test",
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
				"EMQX_NODE__NAME=emqx@" + name + ".bp07.test",
				"EMQX_NODE__COOKIE=bp07-" + options.RunID + "-emqx-cookie",
				"EMQX_CLUSTER__DISCOVERY_STRATEGY=static",
				"EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.bp07.test,emqx@node2.bp07.test,emqx@node3.bp07.test]",
				"EMQX_DURABLE_SESSIONS__ENABLE=true",
				"EMQX_DURABLE_STORAGE__MESSAGES__BACKEND=builtin_raft",
				"EMQX_DURABLE_STORAGE__MESSAGES__N_SITES=3",
				"EMQX_DURABLE_STORAGE__MESSAGES__REPLICATION_FACTOR=3",
				"EMQX_DURABLE_STORAGE__MESSAGES__N_SHARDS=1",
				`EMQX_AUTHENTICATION=[{"mechanism":"jwt","enable":true,"use_jwks":true,"endpoint":"http://127.0.0.1:18080/jwks.json","refresh_interval":300,"from":"password","verify_claims":{"iss":"https://issuer.example","aud":"narumq","sub":"bp07-client"}}]`,
				`EMQX_AUTHORIZATION__SOURCES=[{"type":"file","enable":true,"path":"/config/acl.conf"}]`,
				"EMQX_AUTHORIZATION__NO_MATCH=deny",
			}
		}
		nodes[index] = node
	}
	return nodes
}

func bp07ResourceLimitsEqual(left, right bp07ResourceLimits) bool {
	return reflect.DeepEqual(left, right)
}

func buildBP07Comparison(manifest bp07Manifest, summary bp07Summary) bp07Comparison {
	left, leftOK := manifest.Targets["narumq"]
	right, rightOK := manifest.Targets["emqx"]
	equivalent := leftOK && rightOK && left.ProfileSHA256 == right.ProfileSHA256 &&
		bp07ResourceLimitsEqual(left.ResourceLimits, right.ResourceLimits) &&
		left.RunnerImageID == right.RunnerImageID && manifest.Topology.Nodes == 3
	comparison := bp07Comparison{
		SchemaVersion:               1,
		RunID:                       manifest.RunID,
		Equivalent:                  equivalent,
		ReleaseGating:               false,
		Reason:                      "identical workload profile, resource limits, runner image, and three-node topology; both use replicated durable message storage, but drain barriers, Will outcomes, exact ACK/storage, and security contracts remain non-equivalent",
		ProfileSHA256:               manifest.Profile.RawSHA256,
		ResolvedProfile:             manifest.Profile.Resolved,
		ResourceLimits:              left.ResourceLimits,
		Topology:                    manifest.Topology,
		RunnerImageID:               manifest.RunnerImageID,
		DrainTimeDirectlyComparable: false,
		NonEquivalent: []bp07Difference{
			{Dimension: "drainBarrier", Reason: "NaruMQ completes at the replicated SAFE_TO_STOP barrier; EMQX evacuation reaches prohibiting after a fixed wait-takeover period"},
			{Dimension: "durability", Reason: "EMQX uses Durable Storage batching and session persistence; equivalence to NaruMQ's quorum WAL fsync-before-ACK contract is not verified"},
			{Dimension: "authentication", Reason: "both validate JWKS JWT; EMQX fixture does not reproduce NaruMQ tenant/client-ID bindings or peer mTLS"},
			{Dimension: "willContract", Reason: "NaruMQ Will outcomes are validated against ADR-0028; EMQX Will deliveries are recorded only"},
		},
		Targets: make(map[string]bp07ComparisonTarget, 2),
	}
	for _, target := range []string{"narumq", "emqx"} {
		entry, ok := manifest.Targets[target]
		if !ok {
			continue
		}
		comparison.Targets[target] = bp07ComparisonTarget{
			Success:             entry.Success,
			Valid:               summary.Targets[target].Valid,
			WorkloadSummaryPath: entry.WorkloadSummaryPath,
			ResourceSummaryPath: entry.ResourceSummaryPath,
			RollingSummaryPath:  entry.RollingSummaryPath,
			FailureReason:       entry.FailureReason,
		}
	}
	return comparison
}

func summarizeBP07Run(manifest bp07Manifest) bp07Summary {
	summary := bp07Summary{SchemaVersion: 1, RunID: manifest.RunID, ReleaseGating: false, Valid: true, Targets: make(map[string]bp07TargetSummary, 2), FailureReason: manifest.FailureReason}
	for _, target := range []string{"narumq", "emqx"} {
		entry, ok := manifest.Targets[target]
		if !ok {
			summary.Valid = false
			if summary.FailureReason == "" {
				summary.FailureReason = "target " + target + " did not produce a manifest"
			}
			continue
		}
		item := bp07TargetSummary{
			Valid:           entry.Valid,
			WorkloadSummary: entry.WorkloadSummary,
			ResourceSummary: entry.ResourceSummary,
			RollingSummary:  entry.RollingSummary,
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

func writeBP07RunArtifacts(directory string, manifest bp07Manifest) error {
	summary := summarizeBP07Run(manifest)
	comparison := buildBP07Comparison(manifest, summary)
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

func readBP07JSON(path string, destination any) error {
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

func bp07RepositoryRoot() (string, error) {
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

func newBP07RunID() string {
	return "bp07-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}

func collectBP07HostMetadata(repoRoot string) bp07HostMetadata {
	commit, _ := bp07CommandText(context.Background(), repoRoot, "git", "rev-parse", "HEAD")
	status, _ := bp07CommandText(context.Background(), repoRoot, "git", "status", "--porcelain", "--untracked-files=all")
	goVersion, _ := bp07CommandText(context.Background(), repoRoot, "go", "version")
	dockerVersion, _ := bp07CommandText(context.Background(), repoRoot, "docker", "version", "--format", "{{json .}}")
	return bp07HostMetadata{
		Git:           bp07GitMetadata{Commit: strings.TrimSpace(commit), Dirty: strings.TrimSpace(status) != ""},
		GoVersion:     strings.TrimSpace(goVersion),
		DockerVersion: strings.TrimSpace(dockerVersion),
	}
}

func bp07CommandText(ctx context.Context, workingDirectory, name string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, name, args...)
	if workingDirectory != "" {
		command.Dir = workingDirectory
	}
	output, err := command.CombinedOutput()
	return string(output), err
}
