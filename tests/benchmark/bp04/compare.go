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
	bp04EMQXImageReference = "emqx/emqx:5.8.8@sha256:4593b6e30196754779fa14483332cb0be56c233f1c8a387b1308bec4337c0144"
	bp04NetworkMode        = "bridge network; broker shares network-owner namespace"
)

type bp04GitMetadata struct {
	Commit string `json:"commit"`
	Dirty  bool   `json:"dirty"`
}

type bp04DockerMetadata struct {
	ClientServerVersion string `json:"clientServerVersion"`
}

type bp04HostMetadata struct {
	Git           bp04GitMetadata
	GoVersion     string
	DockerVersion string
}

type bp04ResourceLimits struct {
	BrokerCPUs      float64 `json:"brokerCpus"`
	BrokerMemory    int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs   float64 `json:"generatorCpus"`
	GeneratorMemory int64   `json:"generatorMemoryBytes"`
	NetworkMode     string  `json:"networkMode"`
}

type bp04Topology struct {
	Nodes           int    `json:"nodes"`
	NetworkMode     string `json:"networkMode"`
	NaruMQStorage   string `json:"naruMQStorage"`
	EMQXStorage     string `json:"emqxStorage"`
	RouteVisibility string `json:"routeVisibility"`
}

type bp04EMQXMetadata struct {
	Version        string `json:"version"`
	Edition        string `json:"edition"`
	ImageReference string `json:"imageReference"`
	ImageIndex     string `json:"imageIndexDigest"`
}

type bp04ArtifactProfile struct {
	RawSHA256 string  `json:"rawSha256"`
	Resolved  Profile `json:"resolved"`
}

type bp04NodeArtifacts struct {
	Name                 string `json:"name"`
	BrokerContainer      string `json:"brokerContainer"`
	NetworkOwner         string `json:"networkOwnerContainer"`
	BrokerStatsPath      string `json:"brokerStatsPath"`
	NetworkStatsPath     string `json:"networkStatsPath"`
	BrokerLogPath        string `json:"brokerLogPath"`
	ContainerInspectPath string `json:"containerInspectPath"`
}

type bp04TargetManifest struct {
	Target              string              `json:"target"`
	Status              string              `json:"status"`
	Success             bool                `json:"success"`
	Valid               bool                `json:"valid"`
	StartedAt           string              `json:"startedAt,omitempty"`
	FinishedAt          string              `json:"finishedAt,omitempty"`
	ArtifactDirectory   string              `json:"artifactDirectory"`
	WorkloadSummaryPath string              `json:"workloadSummaryPath"`
	ResourceSummaryPath string              `json:"resourceSummaryPath"`
	PhasesPath          string              `json:"phasesPath"`
	MessagesPath        string              `json:"messagesPath"`
	RouteProbesPath     string              `json:"routeProbesPath"`
	ConnectionsPath     string              `json:"connectionsPath"`
	RunnerLogPath       string              `json:"runnerLogPath,omitempty"`
	Nodes               []bp04NodeArtifacts `json:"nodes,omitempty"`
	ImageReference      string              `json:"imageReference"`
	ImageID             string              `json:"imageId,omitempty"`
	ImageRepoDigests    []string            `json:"imageRepoDigests,omitempty"`
	RunnerImageID       string              `json:"runnerImageId"`
	ProfileSHA256       string              `json:"profileSha256"`
	ResolvedProfile     Profile             `json:"resolvedProfile"`
	ResourceLimits      bp04ResourceLimits  `json:"resourceLimits"`
	NetworkMode         string              `json:"networkMode"`
	WorkloadSummary     *workloadSummary    `json:"workloadSummary,omitempty"`
	ResourceSummary     *resourceSummary    `json:"resourceSummary,omitempty"`
	FailureReason       string              `json:"failureReason,omitempty"`
}

type bp04Manifest struct {
	SchemaVersion         int                           `json:"schemaVersion"`
	RunID                 string                        `json:"runId"`
	StartedAt             string                        `json:"startedAt"`
	FinishedAt            string                        `json:"finishedAt,omitempty"`
	Purpose               string                        `json:"purpose"`
	ReleaseGating         bool                          `json:"releaseGating"`
	Profile               bp04ArtifactProfile           `json:"profile"`
	Git                   bp04GitMetadata               `json:"git"`
	GoVersion             string                        `json:"goVersion"`
	OS                    string                        `json:"os"`
	Architecture          string                        `json:"architecture"`
	Docker                bp04DockerMetadata            `json:"docker"`
	Command               []string                      `json:"command"`
	BuildBaseImage        string                        `json:"buildBaseImage"`
	PlannedBuildBaseImage string                        `json:"plannedBuildBaseImage"`
	RunnerImage           string                        `json:"runnerImage"`
	RunnerImageID         string                        `json:"runnerImageId"`
	EMQX                  bp04EMQXMetadata              `json:"emqx"`
	Topology              bp04Topology                  `json:"topology"`
	SummaryPath           string                        `json:"summaryPath"`
	ComparisonPath        string                        `json:"comparisonPath"`
	Targets               map[string]bp04TargetManifest `json:"targets"`
	FailureReason         string                        `json:"failureReason,omitempty"`
}

type bp04TargetSummary struct {
	Valid           bool             `json:"valid"`
	WorkloadSummary *workloadSummary `json:"workloadSummary,omitempty"`
	ResourceSummary *resourceSummary `json:"resourceSummary,omitempty"`
	FailureReason   string           `json:"failureReason,omitempty"`
}

type bp04Summary struct {
	SchemaVersion int                          `json:"schemaVersion"`
	RunID         string                       `json:"runId"`
	ReleaseGating bool                         `json:"releaseGating"`
	Valid         bool                         `json:"valid"`
	Targets       map[string]bp04TargetSummary `json:"targets"`
	FailureReason string                       `json:"failureReason,omitempty"`
}

type bp04Difference struct {
	Dimension string `json:"dimension"`
	Reason    string `json:"reason"`
}

type bp04ComparisonTarget struct {
	Success             bool   `json:"success"`
	Valid               bool   `json:"valid"`
	WorkloadSummaryPath string `json:"workloadSummaryPath"`
	ResourceSummaryPath string `json:"resourceSummaryPath"`
	FailureReason       string `json:"failureReason,omitempty"`
}

type bp04Comparison struct {
	SchemaVersion   int                             `json:"schemaVersion"`
	RunID           string                          `json:"runId"`
	Equivalent      bool                            `json:"equivalent"`
	ReleaseGating   bool                            `json:"releaseGating"`
	Reason          string                          `json:"reason"`
	ProfileSHA256   string                          `json:"profileSha256"`
	ResolvedProfile Profile                         `json:"resolvedProfile"`
	ResourceLimits  bp04ResourceLimits              `json:"resourceLimits"`
	Topology        bp04Topology                    `json:"topology"`
	RunnerImageID   string                          `json:"runnerImageId"`
	NonEquivalent   []bp04Difference                `json:"nonEquivalent"`
	Targets         map[string]bp04ComparisonTarget `json:"targets"`
}

type bp04RunOptions struct {
	Profile       Profile
	ProfileSHA256 string
	RunID         string
	RunDir        string
	Images        environment.ImageSet
	Metadata      bp04HostMetadata
	Command       []string
	TargetOrder   []string
}

func compareCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-04 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp04", "root directory for benchmark artifacts")
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
	repoRoot, err := bp04RepositoryRoot()
	if err != nil {
		return err
	}
	runID := newBP04RunID()
	runDir := filepath.Join(outputAbs, runID)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("create BP-04 artifact directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "profile.json"), profileBytes, 0o644); err != nil {
		return fmt.Errorf("write BP-04 profile source: %w", err)
	}
	options := bp04RunOptions{
		Profile:       profile,
		ProfileSHA256: profileHash,
		RunID:         runID,
		RunDir:        runDir,
		Metadata:      collectBP04HostMetadata(repoRoot),
		Command:       []string{"compare", "--profile", *profilePath, "--output", *outputRoot},
		TargetOrder:   []string{"narumq", "emqx"},
	}
	manifest := newBP04Manifest(options, time.Now().UTC())
	if err := writeBP04RunArtifacts(runDir, manifest); err != nil {
		return err
	}
	if err := environment.EnsureEMQXImage(ctx, runDir, bp04EMQXImageReference); err != nil {
		return finishBP04PreparationFailure(stdout, runDir, manifest, err)
	}
	images, err := environment.BuildImages(ctx, environment.BuildOptions{
		BenchmarkID:  "bp04",
		RunID:        runID,
		RepoRoot:     repoRoot,
		ArtifactDir:  runDir,
		RunnerTarget: "bp04-runner",
	})
	if err != nil {
		return finishBP04PreparationFailure(stdout, runDir, manifest, err)
	}
	options.Images = images
	manifest.RunnerImage = images.RunnerReference
	manifest.RunnerImageID = images.RunnerID
	final, runErr := executeBP04Comparison(ctx, options, manifest)
	if writeErr := writeBP04RunArtifacts(runDir, final); writeErr != nil {
		runErr = errors.Join(runErr, fmt.Errorf("write comparison artifacts: %w", writeErr))
	}
	if _, err := fmt.Fprintln(stdout, filepath.Join(runDir, "manifest.json")); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("write compare result: %w", err))
	}
	return runErr
}

func executeBP04Comparison(ctx context.Context, options bp04RunOptions, initial bp04Manifest) (final bp04Manifest, runErr error) {
	manifest := initial
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateBP04TargetOrder(options.TargetOrder); err != nil {
		manifest.FailureReason = err.Error()
		return manifest, err
	}
	if err := os.MkdirAll(options.RunDir, 0o755); err != nil {
		manifest.FailureReason = err.Error()
		return manifest, err
	}
	fixture, err := newBP04Fixture(options.Profile, options.RunID)
	if err != nil {
		manifest.FailureReason = fmt.Sprintf("create BP-04 fixture: %v", err)
		manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return manifest, err
	}
	defer func() {
		if closeErr := fixture.Close(); closeErr != nil {
			closeErr = fmt.Errorf("remove BP-04 fixture: %w", closeErr)
			runErr = errors.Join(runErr, closeErr)
			manifest.FailureReason = appendBP04Failure(manifest.FailureReason, closeErr)
		}
		manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		final = manifest
	}()
	timeout, err := bp04Timeout(options.Profile)
	if err != nil {
		manifest.FailureReason = err.Error()
		return manifest, err
	}
	var failures []error
	for _, target := range options.TargetOrder {
		if err := ctx.Err(); err != nil {
			failures = append(failures, err)
			manifest.FailureReason = appendBP04Failure(manifest.FailureReason, err)
			break
		}
		entry, targetErr := runBP04Target(ctx, options, fixture, target, timeout)
		manifest.Targets[target] = entry
		if targetErr != nil {
			failures = append(failures, fmt.Errorf("%s: %w", target, targetErr))
			manifest.FailureReason = appendBP04Failure(manifest.FailureReason, failures[len(failures)-1])
		}
		if err := writeBP04RunArtifacts(options.RunDir, manifest); err != nil {
			failures = append(failures, fmt.Errorf("write manifest after %s: %w", target, err))
			manifest.FailureReason = appendBP04Failure(manifest.FailureReason, failures[len(failures)-1])
		}
	}
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return manifest, errors.Join(failures...)
}

func runBP04Target(ctx context.Context, options bp04RunOptions, fixture *bp04Fixture, target string, timeout time.Duration) (bp04TargetManifest, error) {
	artifactDir := filepath.Join(options.RunDir, target)
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return bp04TargetManifest{Target: target, Status: "failed", ArtifactDirectory: artifactDir, FailureReason: err.Error()}, err
	}
	resourceLimits := bp04ResourceLimits{
		BrokerCPUs:      options.Profile.BrokerCPUs,
		BrokerMemory:    options.Profile.BrokerMemoryBytes,
		GeneratorCPUs:   options.Profile.GeneratorCPUs,
		GeneratorMemory: options.Profile.GeneratorMemoryBytes,
		NetworkMode:     "bridge",
	}
	clusterOptions := environment.ClusterTargetOptions{
		BenchmarkID:                 "bp04",
		RunID:                       options.RunID,
		Target:                      target,
		ArtifactDirectory:           artifactDir,
		ProfilePath:                 filepath.Join(options.RunDir, "profile.json"),
		RunnerReference:             options.Images.RunnerReference,
		RunnerImageID:               options.Images.RunnerID,
		RunnerTarget:                options.Images.RunnerTarget,
		NetworkOwnerArgs:            []string{"serve-jwks", "--file", "/config/jwks.json", "--listen", "127.0.0.1:18080"},
		NetworkOwnerConfigDirectory: fixture.NetworkOwnerConfigDirectory,
		RunnerConfigDirectory:       fixture.RunnerConfigDirectory,
		Nodes:                       bp04ClusterNodes(target, options, fixture),
		BrokerCPUs:                  options.Profile.BrokerCPUs,
		BrokerMemoryBytes:           options.Profile.BrokerMemoryBytes,
		GeneratorCPUs:               options.Profile.GeneratorCPUs,
		GeneratorMemoryBytes:        options.Profile.GeneratorMemoryBytes,
		Timeout:                     timeout,
		StartupTimeout:              120 * time.Second,
		LiveStatsStream:             true,
	}
	if target == "narumq" {
		clusterOptions.ImageReference = options.Images.NaruMQReference
		clusterOptions.RunnerArgs = []string{
			"run", "--profile", "/input/profile.json", "--target", "narumq",
			"--nodes", "node1.bp04.test:1883,node2.bp04.test:1883,node3.bp04.test:1883",
			"--management", "node1.bp04.test:8080,node2.bp04.test:8080,node3.bp04.test:8080",
			"--credentials", "/runner-config/credentials.json", "--output", "/output",
		}
	} else if target == "emqx" {
		clusterOptions.ImageReference = bp04EMQXImageReference
		clusterOptions.EMQXReadyCondition = bp04EMQXReadyCondition
		clusterOptions.RunnerArgs = []string{
			"run", "--profile", "/input/profile.json", "--target", "emqx",
			"--nodes", "node1.bp04.test:1883,node2.bp04.test:1883,node3.bp04.test:1883",
			"--credentials", "/runner-config/credentials.json", "--output", "/output",
		}
	} else {
		return bp04TargetManifest{Target: target, Status: "failed", ArtifactDirectory: artifactDir, FailureReason: "unsupported target"}, fmt.Errorf("unsupported BP-04 target %q", target)
	}
	result, runErr := environment.RunClusterTarget(ctx, clusterOptions)
	manifest := bp04TargetManifest{
		Target:              target,
		Status:              result.Status,
		Success:             result.Success,
		StartedAt:           result.StartedAt,
		FinishedAt:          result.FinishedAt,
		ArtifactDirectory:   artifactDir,
		WorkloadSummaryPath: filepath.Join(artifactDir, "workload-summary.json"),
		ResourceSummaryPath: filepath.Join(artifactDir, "resource-summary.json"),
		PhasesPath:          filepath.Join(artifactDir, "phases.jsonl"),
		MessagesPath:        filepath.Join(artifactDir, "messages.jsonl"),
		RouteProbesPath:     filepath.Join(artifactDir, "route-probes.jsonl"),
		ConnectionsPath:     filepath.Join(artifactDir, "connections.jsonl"),
		RunnerLogPath:       result.RunnerLogPath,
		ImageReference:      result.ImageReference,
		ImageID:             result.ImageID,
		ImageRepoDigests:    append([]string(nil), result.ImageRepoDigests...),
		RunnerImageID:       result.RunnerImageID,
		ProfileSHA256:       options.ProfileSHA256,
		ResolvedProfile:     options.Profile,
		ResourceLimits:      resourceLimits,
		NetworkMode:         bp04NetworkMode,
		FailureReason:       result.FailureReason,
	}
	manifest.Nodes = make([]bp04NodeArtifacts, 0, len(result.Nodes))
	for _, node := range result.Nodes {
		manifest.Nodes = append(manifest.Nodes, bp04NodeArtifacts{
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
	if err := readBP04JSON(manifest.WorkloadSummaryPath, &workload); err != nil {
		artifactErrors = append(artifactErrors, fmt.Errorf("read workload summary: %w", err))
	} else {
		manifest.WorkloadSummary = &workload
		resources, resourceErr := buildResourceSummary(target, result.Nodes, manifest.PhasesPath, workload)
		if resourceErr != nil {
			resources.Valid = false
			resources.FailureReasons = append(resources.FailureReasons, resourceErr.Error())
			artifactErrors = append(artifactErrors, fmt.Errorf("build resource summary: %w", resourceErr))
		}
		manifest.ResourceSummary = &resources
		if err := writeResourceSummary(artifactDir, resources); err != nil {
			artifactErrors = append(artifactErrors, fmt.Errorf("write resource summary: %w", err))
		}
	}
	manifest.Valid = result.Success && manifest.WorkloadSummary != nil && manifest.WorkloadSummary.Valid && manifest.ResourceSummary != nil && manifest.ResourceSummary.Valid
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

func bp04ClusterNodes(target string, options bp04RunOptions, fixture *bp04Fixture) []environment.ClusterNodeSpec {
	nodes := make([]environment.ClusterNodeSpec, bp04NodeCount)
	for index := range nodes {
		number := index + 1
		name := fmt.Sprintf("node%d", number)
		node := environment.ClusterNodeSpec{
			Name:            name,
			DNSName:         name + ".bp04.test",
			ConfigDirectory: fixture.NodeConfigDirectories[index],
			MountConfig:     target == "narumq",
			DataDestination: "/var/lib/narumq",
		}
		if target == "narumq" {
			node.BrokerArgs = []string{"-cluster-config", "/config/node.json"}
		} else {
			node.ConfigDirectory = fixture.EMQXConfigDirectory
			node.MountConfig = true
			node.DataDestination = "/opt/emqx/data"
			node.LogDestination = "/opt/emqx/log"
			node.BrokerEnv = []string{
				"EMQX_NODE__NAME=emqx@" + name + ".bp04.test",
				"EMQX_NODE__COOKIE=bp04-" + options.RunID + "-emqx-cookie",
				"EMQX_CLUSTER__DISCOVERY_STRATEGY=static",
				"EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.bp04.test,emqx@node2.bp04.test,emqx@node3.bp04.test]",
				"EMQX_AUTHENTICATION=[{\"mechanism\":\"jwt\",\"enable\":true,\"use_jwks\":true,\"endpoint\":\"http://127.0.0.1:18080/jwks.json\",\"refresh_interval\":300,\"from\":\"password\",\"verify_claims\":{\"iss\":\"https://issuer.example\",\"aud\":\"narumq\",\"sub\":\"bp04-client\"}}]",
				`EMQX_AUTHORIZATION__SOURCES=[{"type":"file","enable":true,"path":"/config/acl.conf"}]`,
				"EMQX_AUTHORIZATION__NO_MATCH=deny",
			}
		}
		nodes[index] = node
	}
	return nodes
}

const bp04EMQXReadyCondition = `begin
		{ok, Authenticators} = emqx_authn_chains:list_authenticators('mqtt:global'),
		lists:any(fun(#{id := <<"jwt">>, enable := true}) -> true; (_) -> false end, Authenticators)
	end`

func bp04ResourceLimitsEqual(left, right bp04ResourceLimits) bool {
	return reflect.DeepEqual(left, right)
}

func buildBP04Comparison(manifest bp04Manifest, summary bp04Summary) bp04Comparison {
	left, leftOK := manifest.Targets["narumq"]
	right, rightOK := manifest.Targets["emqx"]
	equivalent := leftOK && rightOK &&
		left.ProfileSHA256 != "" && left.ProfileSHA256 == manifest.Profile.RawSHA256 && right.ProfileSHA256 == manifest.Profile.RawSHA256 &&
		bp04ResourceLimitsEqual(left.ResourceLimits, right.ResourceLimits) &&
		left.RunnerImageID != "" && left.RunnerImageID == right.RunnerImageID && left.RunnerImageID == manifest.RunnerImageID &&
		len(left.Nodes) == bp04NodeCount && len(right.Nodes) == bp04NodeCount && manifest.Topology.Nodes == bp04NodeCount
	reason := "profile, CPU/memory limits, runner image, and three-node topology match; routing, authentication, overlap delivery, and shared-member selection remain non-equivalent"
	if !equivalent {
		reason = "profile SHA, CPU/memory limits, runner image ID, or three-node topology does not match"
	}
	comparison := bp04Comparison{
		SchemaVersion:   1,
		RunID:           manifest.RunID,
		Equivalent:      equivalent,
		ReleaseGating:   false,
		Reason:          reason,
		ProfileSHA256:   manifest.Profile.RawSHA256,
		ResolvedProfile: manifest.Profile.Resolved,
		ResourceLimits:  bp04ResourceLimits{BrokerCPUs: manifest.Profile.Resolved.BrokerCPUs, BrokerMemory: manifest.Profile.Resolved.BrokerMemoryBytes, GeneratorCPUs: manifest.Profile.Resolved.GeneratorCPUs, GeneratorMemory: manifest.Profile.Resolved.GeneratorMemoryBytes, NetworkMode: "bridge"},
		Topology:        manifest.Topology,
		RunnerImageID:   manifest.RunnerImageID,
		NonEquivalent: []bp04Difference{
			{Dimension: "overlapDelivery", Reason: "NaruMQ delivers one granted-QoS copy per session; EMQX may deliver one copy per matching subscription. Extra copies are recorded and excluded from direct comparison."},
			{Dimension: "sharedSelection", Reason: "Both targets use shared subscriptions, but cluster-wide member selection order is implementation-specific; memberDeliveries are recorded, not compared."},
			{Dimension: "authentication", Reason: "Both targets validate JWKS JWTs; the EMQX fixture does not reproduce NaruMQ tenant/client-ID binding or peer mTLS."},
			{Dimension: "subscriptionReplication", Reason: "NaruMQ replicates committed subscriptions through Raft and projects node-local routes; EMQX OSS uses its cluster database. Only client-observed route visibility is measured."},
		},
		Targets: make(map[string]bp04ComparisonTarget, 2),
	}
	for _, target := range []string{"narumq", "emqx"} {
		entry, ok := manifest.Targets[target]
		if !ok {
			continue
		}
		comparison.Targets[target] = bp04ComparisonTarget{
			Success:             entry.Success,
			Valid:               summary.Targets[target].Valid,
			WorkloadSummaryPath: entry.WorkloadSummaryPath,
			ResourceSummaryPath: entry.ResourceSummaryPath,
			FailureReason:       entry.FailureReason,
		}
	}
	return comparison
}

func summarizeBP04Run(manifest bp04Manifest) bp04Summary {
	summary := bp04Summary{SchemaVersion: 1, RunID: manifest.RunID, ReleaseGating: false, Valid: true, Targets: make(map[string]bp04TargetSummary, 2), FailureReason: manifest.FailureReason}
	for _, target := range []string{"narumq", "emqx"} {
		entry, ok := manifest.Targets[target]
		if !ok {
			summary.Valid = false
			if summary.FailureReason == "" {
				summary.FailureReason = "target " + target + " did not produce a manifest"
			}
			continue
		}
		summary.Targets[target] = bp04TargetSummary{Valid: entry.Valid, WorkloadSummary: entry.WorkloadSummary, ResourceSummary: entry.ResourceSummary, FailureReason: entry.FailureReason}
		if !entry.Valid {
			summary.Valid = false
			if summary.FailureReason == "" {
				summary.FailureReason = target + " target is invalid"
			}
		}
	}
	return summary
}

func writeBP04RunArtifacts(directory string, manifest bp04Manifest) error {
	summary := summarizeBP04Run(manifest)
	comparison := buildBP04Comparison(manifest, summary)
	var artifactErrors []error
	for name, value := range map[string]any{"manifest.json": manifest, "summary.json": summary, "comparison.json": comparison} {
		if err := writeBP04JSON(filepath.Join(directory, name), value); err != nil {
			artifactErrors = append(artifactErrors, err)
		}
	}
	return errors.Join(artifactErrors...)
}

func writeBP04JSON(path string, value any) error {
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

func readBP04JSON(path string, destination any) error {
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

func bp04RepositoryRoot() (string, error) {
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

func newBP04Manifest(options bp04RunOptions, startedAt time.Time) bp04Manifest {
	return bp04Manifest{
		SchemaVersion:         1,
		RunID:                 options.RunID,
		StartedAt:             startedAt.UTC().Format(time.RFC3339Nano),
		Purpose:               options.Profile.Purpose,
		ReleaseGating:         false,
		Profile:               bp04ArtifactProfile{RawSHA256: options.ProfileSHA256, Resolved: options.Profile},
		Git:                   options.Metadata.Git,
		GoVersion:             options.Metadata.GoVersion,
		OS:                    runtime.GOOS,
		Architecture:          runtime.GOARCH,
		Docker:                bp04DockerMetadata{ClientServerVersion: options.Metadata.DockerVersion},
		Command:               append([]string(nil), options.Command...),
		BuildBaseImage:        environment.BuildBaseImageReference,
		PlannedBuildBaseImage: environment.PlannedBuildBaseImage,
		RunnerImage:           options.Images.RunnerReference,
		RunnerImageID:         options.Images.RunnerID,
		EMQX: bp04EMQXMetadata{
			Version: "5.8.8", Edition: "Open Source", ImageReference: bp04EMQXImageReference,
			ImageIndex: strings.SplitN(bp04EMQXImageReference, "@", 2)[1],
		},
		Topology: bp04Topology{
			Nodes: bp04NodeCount, NetworkMode: bp04NetworkMode,
			NaruMQStorage:   "Raft-backed cluster with per-node Docker named volumes",
			EMQXStorage:     "static EMQX OSS cluster with per-node Docker named volumes; storage equivalence is not claimed",
			RouteVisibility: "QoS 0 publish-to-subscribe visibility measured from SUBSCRIBE write to first delivery",
		},
		SummaryPath:    filepath.Join(options.RunDir, "summary.json"),
		ComparisonPath: filepath.Join(options.RunDir, "comparison.json"),
		Targets:        make(map[string]bp04TargetManifest),
	}
}

func finishBP04PreparationFailure(stdout io.Writer, directory string, manifest bp04Manifest, cause error) error {
	manifest.FailureReason = cause.Error()
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	writeErr := writeBP04RunArtifacts(directory, manifest)
	_, outputErr := fmt.Fprintln(stdout, filepath.Join(directory, "manifest.json"))
	return errors.Join(cause, writeErr, outputErr)
}

func appendBP04Failure(existing string, err error) string {
	if err == nil {
		return existing
	}
	if existing == "" {
		return err.Error()
	}
	return errors.Join(errors.New(existing), err).Error()
}

func newBP04RunID() string {
	return "bp04-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}

func collectBP04HostMetadata(repoRoot string) bp04HostMetadata {
	commit, _ := bp04CommandText(context.Background(), repoRoot, "git", "rev-parse", "HEAD")
	status, _ := bp04CommandText(context.Background(), repoRoot, "git", "status", "--porcelain", "--untracked-files=all")
	goVersion, _ := bp04CommandText(context.Background(), repoRoot, "go", "version")
	dockerVersion, _ := bp04CommandText(context.Background(), repoRoot, "docker", "version", "--format", "{{json .}}")
	return bp04HostMetadata{
		Git:           bp04GitMetadata{Commit: strings.TrimSpace(commit), Dirty: strings.TrimSpace(status) != ""},
		GoVersion:     strings.TrimSpace(goVersion),
		DockerVersion: strings.TrimSpace(dockerVersion),
	}
}

func bp04CommandText(ctx context.Context, workingDirectory, name string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, name, args...)
	if workingDirectory != "" {
		command.Dir = workingDirectory
	}
	output, err := command.CombinedOutput()
	return string(output), err
}

func validateBP04TargetOrder(order []string) error {
	if len(order) != 2 {
		return errors.New("target order must contain exactly two targets")
	}
	seen := make(map[string]struct{}, len(order))
	for _, target := range order {
		if target != "narumq" && target != "emqx" {
			return fmt.Errorf("unsupported BP-04 target %q", target)
		}
		if _, exists := seen[target]; exists {
			return fmt.Errorf("target order contains duplicate %q", target)
		}
		seen[target] = struct{}{}
	}
	return nil
}

func bp04Metrics(target string, value bp04TargetManifest) (map[string]float64, error) {
	if target != "narumq" && target != "emqx" {
		return nil, fmt.Errorf("unsupported BP-04 target %q", target)
	}
	if !value.Success || !value.Valid || value.WorkloadSummary == nil || !value.WorkloadSummary.Valid {
		return nil, errors.New("target workload summary is invalid")
	}
	if value.ResourceSummary == nil || !value.ResourceSummary.Valid {
		return nil, errors.New("target resource summary is invalid")
	}
	metrics := make(map[string]float64)
	for _, step := range value.WorkloadSummary.Steps {
		resource, ok := resourceWindowByName(value.ResourceSummary.Windows, fmt.Sprintf("step-%02d-measure", step.Step))
		if !ok {
			return nil, fmt.Errorf("missing resource window for step %02d", step.Step)
		}
		metrics[fmt.Sprintf("step%02d_delivered_per_second", step.Step)] = step.DeliveredPerSecond
		metrics[fmt.Sprintf("step%02d_cluster_median_cpu_percent", step.Step)] = resource.Cluster.MedianCPUPercent
		for _, className := range []string{classFanout, classWildcard, classOverlap, classShared} {
			class, exists := step.Classes[className]
			if !exists {
				return nil, fmt.Errorf("step %02d has no %s class summary", step.Step, className)
			}
			prefix := fmt.Sprintf("step%02d_%s_delivery_latency_", step.Step, className)
			metrics[prefix+"p99_ns"] = float64(class.DeliveryLatency.P99Nanoseconds)
			metrics[prefix+"p999_ns"] = float64(class.DeliveryLatency.P999Nanoseconds)
		}
		if value.ResourceSummary.NetworkStatsAvailable {
			if resource.Deliveries == 0 {
				return nil, fmt.Errorf("step %02d has zero deliveries for network metric", step.Step)
			}
			metrics[fmt.Sprintf("step%02d_network_tx_bytes_per_delivery", step.Step)] = float64(resource.Cluster.NetworkTxBytes) / float64(resource.Deliveries)
		}
	}
	metrics["route_visible_latency_p50_ns"] = float64(value.WorkloadSummary.RoutePropagation.RouteVisibleLatency.P50Nanoseconds)
	metrics["route_visible_latency_p99_ns"] = float64(value.WorkloadSummary.RoutePropagation.RouteVisibleLatency.P99Nanoseconds)
	metrics["route_visible_latency_max_ns"] = float64(value.WorkloadSummary.RoutePropagation.RouteVisibleLatency.MaxNanoseconds)
	metrics["subscribe_ack_latency_p99_ns"] = float64(value.WorkloadSummary.Setup.SubscribeAckLatency.P99Nanoseconds)
	metrics["memory_bytes_per_setup_client"] = value.ResourceSummary.Memory.MemoryBytesPerSetupClient
	return metrics, nil
}

func bp04SuiteMetricUnits(profile Profile, networkAvailable bool) map[string]string {
	units := make(map[string]string)
	for step := range profile.RatesPerStreamPerSecond {
		prefix := fmt.Sprintf("step%02d_", step+1)
		units[prefix+"delivered_per_second"] = "deliveries/s"
		units[prefix+"cluster_median_cpu_percent"] = "percent"
		for _, class := range []string{classFanout, classWildcard, classOverlap, classShared} {
			classPrefix := prefix + class + "_delivery_latency_"
			units[classPrefix+"p99_ns"] = "ns"
			units[classPrefix+"p999_ns"] = "ns"
		}
		if networkAvailable {
			units[prefix+"network_tx_bytes_per_delivery"] = "bytes"
		}
	}
	units["route_visible_latency_p50_ns"] = "ns"
	units["route_visible_latency_p99_ns"] = "ns"
	units["route_visible_latency_max_ns"] = "ns"
	units["subscribe_ack_latency_p99_ns"] = "ns"
	units["memory_bytes_per_setup_client"] = "bytes/client"
	return units
}
