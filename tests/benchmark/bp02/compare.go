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

const bp02NetworkMode = "bridge network; broker shares network-owner namespace"
const bp02TLSNodes = "node1.bp02.test:8883,node2.bp02.test:8883,node3.bp02.test:8883"

type bp02GitMetadata struct {
	Commit string `json:"commit"`
	Dirty  bool   `json:"dirty"`
}

type bp02DockerMetadata struct {
	ClientServerVersion string `json:"clientServerVersion"`
}

type bp02HostMetadata struct {
	Git           bp02GitMetadata
	GoVersion     string
	DockerVersion string
}

type bp02ResourceLimits struct {
	BrokerCPUs      float64 `json:"brokerCpus"`
	BrokerMemory    int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs   float64 `json:"generatorCpus"`
	GeneratorMemory int64   `json:"generatorMemoryBytes"`
	NetworkMode     string  `json:"networkMode"`
}

type bp02Topology struct {
	Nodes          int               `json:"nodes"`
	NetworkMode    string            `json:"networkMode"`
	NaruMQStorage  string            `json:"naruMQStorage"`
	EMQXStorage    string            `json:"emqxStorage"`
	SessionStorage map[string]string `json:"sessionStorage"`
}

type bp02EMQXMetadata struct {
	Version        string `json:"version"`
	Edition        string `json:"edition"`
	ImageReference string `json:"imageReference"`
	ImageIndex     string `json:"imageIndexDigest"`
}

type bp02ArtifactProfile struct {
	RawSHA256 string  `json:"rawSha256"`
	Resolved  Profile `json:"resolved"`
}

type bp02NodeArtifacts struct {
	Name                 string `json:"name"`
	BrokerContainer      string `json:"brokerContainer"`
	NetworkOwner         string `json:"networkOwnerContainer"`
	BrokerStatsPath      string `json:"brokerStatsPath"`
	NetworkStatsPath     string `json:"networkStatsPath"`
	BrokerLogPath        string `json:"brokerLogPath"`
	ContainerInspectPath string `json:"containerInspectPath"`
}

type bp02TargetManifest struct {
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
	RetainedPath        string              `json:"retainedPath"`
	ConnectionsPath     string              `json:"connectionsPath"`
	RunnerLogPath       string              `json:"runnerLogPath,omitempty"`
	Nodes               []bp02NodeArtifacts `json:"nodes,omitempty"`
	ImageReference      string              `json:"imageReference"`
	ImageID             string              `json:"imageId,omitempty"`
	ImageRepoDigests    []string            `json:"imageRepoDigests,omitempty"`
	RunnerImageID       string              `json:"runnerImageId"`
	ProfileSHA256       string              `json:"profileSha256"`
	ResolvedProfile     Profile             `json:"resolvedProfile"`
	ResourceLimits      bp02ResourceLimits  `json:"resourceLimits"`
	NetworkMode         string              `json:"networkMode"`
	WorkloadSummary     *workloadSummary    `json:"workloadSummary,omitempty"`
	ResourceSummary     *resourceSummary    `json:"resourceSummary,omitempty"`
	FailureReason       string              `json:"failureReason,omitempty"`
}

type bp02Manifest struct {
	SchemaVersion         int                           `json:"schemaVersion"`
	RunID                 string                        `json:"runId"`
	StartedAt             string                        `json:"startedAt"`
	FinishedAt            string                        `json:"finishedAt,omitempty"`
	Purpose               string                        `json:"purpose"`
	ReleaseGating         bool                          `json:"releaseGating"`
	Profile               bp02ArtifactProfile           `json:"profile"`
	Git                   bp02GitMetadata               `json:"git"`
	GoVersion             string                        `json:"goVersion"`
	OS                    string                        `json:"os"`
	Architecture          string                        `json:"architecture"`
	Docker                bp02DockerMetadata            `json:"docker"`
	Command               []string                      `json:"command"`
	BuildBaseImage        string                        `json:"buildBaseImage"`
	PlannedBuildBaseImage string                        `json:"plannedBuildBaseImage"`
	RunnerImage           string                        `json:"runnerImage"`
	RunnerImageID         string                        `json:"runnerImageId"`
	EMQX                  bp02EMQXMetadata              `json:"emqx"`
	Topology              bp02Topology                  `json:"topology"`
	SummaryPath           string                        `json:"summaryPath"`
	ComparisonPath        string                        `json:"comparisonPath"`
	Targets               map[string]bp02TargetManifest `json:"targets"`
	FailureReason         string                        `json:"failureReason,omitempty"`
}

type bp02TargetSummary struct {
	Valid           bool             `json:"valid"`
	WorkloadSummary *workloadSummary `json:"workloadSummary,omitempty"`
	ResourceSummary *resourceSummary `json:"resourceSummary,omitempty"`
	FailureReason   string           `json:"failureReason,omitempty"`
}

type bp02Summary struct {
	SchemaVersion int                          `json:"schemaVersion"`
	RunID         string                       `json:"runId"`
	ReleaseGating bool                         `json:"releaseGating"`
	Valid         bool                         `json:"valid"`
	Targets       map[string]bp02TargetSummary `json:"targets"`
	FailureReason string                       `json:"failureReason,omitempty"`
}

type bp02Difference struct {
	Dimension string `json:"dimension"`
	Reason    string `json:"reason"`
}

type bp02ComparisonTarget struct {
	Success             bool   `json:"success"`
	Valid               bool   `json:"valid"`
	WorkloadSummaryPath string `json:"workloadSummaryPath"`
	ResourceSummaryPath string `json:"resourceSummaryPath"`
	FailureReason       string `json:"failureReason,omitempty"`
}

type bp02Comparison struct {
	SchemaVersion   int                             `json:"schemaVersion"`
	RunID           string                          `json:"runId"`
	Equivalent      bool                            `json:"equivalent"`
	ReleaseGating   bool                            `json:"releaseGating"`
	Reason          string                          `json:"reason"`
	ProfileSHA256   string                          `json:"profileSha256"`
	ResolvedProfile Profile                         `json:"resolvedProfile"`
	ResourceLimits  bp02ResourceLimits              `json:"resourceLimits"`
	Topology        bp02Topology                    `json:"topology"`
	RunnerImageID   string                          `json:"runnerImageId"`
	NonEquivalent   []bp02Difference                `json:"nonEquivalent"`
	Targets         map[string]bp02ComparisonTarget `json:"targets"`
}

type bp02RunOptions struct {
	Profile       Profile
	ProfileSHA256 string
	RunID         string
	RunDir        string
	Images        environment.ImageSet
	Metadata      bp02HostMetadata
	Command       []string
	TargetOrder   []string
}

func compareCommand(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-02 profile JSON")
	outputRoot := flags.String("output", ".artifacts/benchmark/bp02", "root directory for benchmark artifacts")
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
	repoRoot, err := bp02RepositoryRoot()
	if err != nil {
		return err
	}
	runID := newBP02RunID()
	runDir := filepath.Join(outputAbs, runID)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fmt.Errorf("create BP-02 artifact directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "profile.json"), profileBytes, 0o644); err != nil {
		return fmt.Errorf("write BP-02 profile source: %w", err)
	}
	options := bp02RunOptions{
		Profile:       profile,
		ProfileSHA256: profileHash,
		RunID:         runID,
		RunDir:        runDir,
		Metadata:      collectBP02HostMetadata(repoRoot),
		Command:       []string{"compare", "--profile", *profilePath, "--output", *outputRoot},
		TargetOrder:   []string{"narumq", "emqx"},
	}
	manifest := newBP02Manifest(options, time.Now().UTC())
	if err := writeBP02RunArtifacts(runDir, manifest); err != nil {
		return err
	}
	if err := environment.EnsureEMQXImage(ctx, runDir, environment.EMQXImageReference); err != nil {
		return finishBP02PreparationFailure(stdout, runDir, manifest, err)
	}
	images, err := environment.BuildImages(ctx, environment.BuildOptions{
		BenchmarkID:  "bp02",
		RunID:        runID,
		RepoRoot:     repoRoot,
		ArtifactDir:  runDir,
		RunnerTarget: "bp02-runner",
	})
	if err != nil {
		return finishBP02PreparationFailure(stdout, runDir, manifest, err)
	}
	options.Images = images
	manifest.RunnerImage = images.RunnerReference
	manifest.RunnerImageID = images.RunnerID
	final, runErr := executeBP02Comparison(ctx, options, manifest)
	if writeErr := writeBP02RunArtifacts(runDir, final); writeErr != nil {
		runErr = errors.Join(runErr, fmt.Errorf("write comparison artifacts: %w", writeErr))
	}
	if _, err := fmt.Fprintln(stdout, filepath.Join(runDir, "manifest.json")); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("write compare result: %w", err))
	}
	return runErr
}

func executeBP02Comparison(ctx context.Context, options bp02RunOptions, initial bp02Manifest) (final bp02Manifest, runErr error) {
	manifest := initial
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateBP02TargetOrder(options.TargetOrder); err != nil {
		manifest.FailureReason = err.Error()
		return manifest, err
	}
	if err := os.MkdirAll(options.RunDir, 0o755); err != nil {
		manifest.FailureReason = err.Error()
		return manifest, err
	}
	fixture, err := newBP02Fixture(options.Profile, options.RunID)
	if err != nil {
		manifest.FailureReason = fmt.Sprintf("create BP-02 fixture: %v", err)
		manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return manifest, err
	}
	defer func() {
		if closeErr := fixture.Close(); closeErr != nil {
			closeErr = fmt.Errorf("remove BP-02 fixture: %w", closeErr)
			runErr = errors.Join(runErr, closeErr)
			manifest.FailureReason = appendBP02Failure(manifest.FailureReason, closeErr)
		}
		manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		final = manifest
	}()
	timeout, err := bp02Timeout(options.Profile)
	if err != nil {
		manifest.FailureReason = err.Error()
		return manifest, err
	}
	var failures []error
	for _, target := range options.TargetOrder {
		if err := ctx.Err(); err != nil {
			failures = append(failures, err)
			manifest.FailureReason = appendBP02Failure(manifest.FailureReason, err)
			break
		}
		entry, targetErr := runBP02Target(ctx, options, fixture, target, timeout)
		manifest.Targets[target] = entry
		if targetErr != nil {
			failures = append(failures, fmt.Errorf("%s: %w", target, targetErr))
			manifest.FailureReason = appendBP02Failure(manifest.FailureReason, failures[len(failures)-1])
		}
		if err := writeBP02RunArtifacts(options.RunDir, manifest); err != nil {
			failures = append(failures, fmt.Errorf("write manifest after %s: %w", target, err))
			manifest.FailureReason = appendBP02Failure(manifest.FailureReason, failures[len(failures)-1])
		}
	}
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return manifest, errors.Join(failures...)
}

func runBP02Target(ctx context.Context, options bp02RunOptions, fixture *bp02Fixture, target string, timeout time.Duration) (bp02TargetManifest, error) {
	artifactDir := filepath.Join(options.RunDir, target)
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return bp02TargetManifest{Target: target, Status: "failed", ArtifactDirectory: artifactDir, FailureReason: err.Error()}, err
	}
	resourceLimits := bp02ResourceLimits{
		BrokerCPUs:      options.Profile.BrokerCPUs,
		BrokerMemory:    options.Profile.BrokerMemoryBytes,
		GeneratorCPUs:   options.Profile.GeneratorCPUs,
		GeneratorMemory: options.Profile.GeneratorMemoryBytes,
		NetworkMode:     "bridge",
	}
	clusterOptions := environment.ClusterTargetOptions{
		BenchmarkID:                 "bp02",
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
		Nodes:                       bp02ClusterNodes(target, options, fixture),
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
			"--nodes", "node1.bp02.test:1883,node2.bp02.test:1883,node3.bp02.test:1883",
			"--tls-nodes", bp02TLSNodes,
			"--management", "node1.bp02.test:8080,node2.bp02.test:8080,node3.bp02.test:8080",
			"--credentials", "/runner-config/credentials.json", "--output", "/output",
		}
	} else if target == "emqx" {
		clusterOptions.ImageReference = environment.EMQXImageReference
		clusterOptions.EMQXReadyCondition = bp02EMQXReadyCondition
		clusterOptions.RunnerArgs = []string{
			"run", "--profile", "/input/profile.json", "--target", "emqx",
			"--nodes", "node1.bp02.test:1883,node2.bp02.test:1883,node3.bp02.test:1883",
			"--tls-nodes", bp02TLSNodes,
			"--credentials", "/runner-config/credentials.json", "--output", "/output",
		}
	} else {
		return bp02TargetManifest{Target: target, Status: "failed", ArtifactDirectory: artifactDir, FailureReason: "unsupported target"}, fmt.Errorf("unsupported BP-02 target %q", target)
	}
	result, runErr := environment.RunClusterTarget(ctx, clusterOptions)
	manifest := bp02TargetManifest{
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
		RetainedPath:        filepath.Join(artifactDir, "retained.jsonl"),
		ConnectionsPath:     filepath.Join(artifactDir, "connections.jsonl"),
		ImageID:             result.ImageID,
		ImageRepoDigests:    append([]string(nil), result.ImageRepoDigests...),
		RunnerImageID:       result.RunnerImageID,
		ProfileSHA256:       options.ProfileSHA256,
		ResolvedProfile:     options.Profile,
		ResourceLimits:      resourceLimits,
		NetworkMode:         bp02NetworkMode,
		FailureReason:       result.FailureReason,
	}
	manifest.Nodes = make([]bp02NodeArtifacts, 0, len(result.Nodes))
	for _, node := range result.Nodes {
		manifest.Nodes = append(manifest.Nodes, bp02NodeArtifacts{
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
	if err := readBP02JSON(manifest.WorkloadSummaryPath, &workload); err != nil {
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

func bp02ClusterNodes(target string, options bp02RunOptions, fixture *bp02Fixture) []environment.ClusterNodeSpec {
	nodes := make([]environment.ClusterNodeSpec, bp02NodeCount)
	for index := range nodes {
		number := index + 1
		name := fmt.Sprintf("node%d", number)
		node := environment.ClusterNodeSpec{
			Name:            name,
			DNSName:         name + ".bp02.test",
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
				"EMQX_NODE__NAME=emqx@" + name + ".bp02.test",
				"EMQX_NODE__COOKIE=bp02-" + options.RunID + "-emqx-cookie",
				"EMQX_CLUSTER__DISCOVERY_STRATEGY=static",
				"EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.bp02.test,emqx@node2.bp02.test,emqx@node3.bp02.test]",
				"EMQX_AUTHENTICATION=[{\"mechanism\":\"jwt\",\"enable\":true,\"use_jwks\":true,\"endpoint\":\"http://127.0.0.1:18080/jwks.json\",\"refresh_interval\":300,\"from\":\"password\",\"verify_claims\":{\"iss\":\"https://issuer.example\",\"aud\":\"narumq\",\"sub\":\"${username}\"}}]",
				`EMQX_AUTHORIZATION__SOURCES=[{"type":"file","enable":true,"path":"/config/acl.conf"}]`,
				"EMQX_AUTHORIZATION__NO_MATCH=deny",
				"EMQX_LISTENERS__SSL__DEFAULT__BIND=0.0.0.0:8883",
				"EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__CERTFILE=/config/server.pem",
				"EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__KEYFILE=/config/server-key.pem",
				"EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__CACERTFILE=/config/ca.pem",
				"EMQX_LISTENERS__SSL__DEFAULT__SSL_OPTIONS__VERIFY=verify_none",
			}
		}
		nodes[index] = node
	}
	return nodes
}

const bp02EMQXReadyCondition = `begin
		{ok, Authenticators} = emqx_authn_chains:list_authenticators('mqtt:global'),
		lists:any(fun(#{id := <<"jwt">>, enable := true}) -> true; (_) -> false end, Authenticators)
	end`

func bp02ResourceLimitsEqual(left, right bp02ResourceLimits) bool {
	return reflect.DeepEqual(left, right)
}

func buildBP02Comparison(manifest bp02Manifest, summary bp02Summary) bp02Comparison {
	left, leftOK := manifest.Targets["narumq"]
	right, rightOK := manifest.Targets["emqx"]
	equivalent := leftOK && rightOK &&
		left.ProfileSHA256 != "" && left.ProfileSHA256 == manifest.Profile.RawSHA256 && right.ProfileSHA256 == manifest.Profile.RawSHA256 &&
		bp02ResourceLimitsEqual(left.ResourceLimits, right.ResourceLimits) &&
		left.RunnerImageID != "" && left.RunnerImageID == right.RunnerImageID && left.RunnerImageID == manifest.RunnerImageID &&
		len(left.Nodes) == bp02NodeCount && len(right.Nodes) == bp02NodeCount && manifest.Topology.Nodes == bp02NodeCount
	reason := "profile SHA, CPU/memory limits, runner image ID, and three-node topology match; the six listed dimensions remain non-equivalent"
	if !equivalent {
		reason = "profile SHA, CPU/memory limits, runner image ID, or three-node topology does not match"
	}
	comparison := bp02Comparison{
		SchemaVersion:   1,
		RunID:           manifest.RunID,
		Equivalent:      equivalent,
		ReleaseGating:   false,
		Reason:          reason,
		ProfileSHA256:   manifest.Profile.RawSHA256,
		ResolvedProfile: manifest.Profile.Resolved,
		ResourceLimits:  bp02ResourceLimits{BrokerCPUs: manifest.Profile.Resolved.BrokerCPUs, BrokerMemory: manifest.Profile.Resolved.BrokerMemoryBytes, GeneratorCPUs: manifest.Profile.Resolved.GeneratorCPUs, GeneratorMemory: manifest.Profile.Resolved.GeneratorMemoryBytes, NetworkMode: "bridge"},
		Topology:        manifest.Topology,
		RunnerImageID:   manifest.RunnerImageID,
		NonEquivalent: []bp02Difference{
			{Dimension: "sessionDurability", Reason: "NaruMQ commits persistent sessions, subscriptions, QoS 1/2 inflight and offline messages through Raft quorum; EMQX OSS 5.8.8 keeps memory sessions. Offline results are not a durability comparison."},
			{Dimension: "tenantIsolation", Reason: "NaruMQ binds JWT (iss, sub) to a tenant namespace and allowed client IDs; EMQX uses username-scoped file ACLs on bp02/a/# and bp02/b/#. Same raw topics across tenants are not exercised."},
			{Dimension: "authentication", Reason: "Both verify RS256 JWTs from the same JWKS; EMQX checks sub against the username and does not reproduce client-ID binding or peer mTLS."},
			{Dimension: "tlsListener", Reason: "Both offer server-authenticated TLS 1.2+ from the same CA; NaruMQ reuses each node peer certificate, EMQX shares one three-SAN certificate. Cipher and session-resumption behavior are implementation-specific."},
			{Dimension: "sharedSelection", Reason: "Both use shared subscriptions; cluster-wide member selection order is implementation-specific. memberDeliveries are recorded, not compared."},
			{Dimension: "retainedReplication", Reason: "NaruMQ replicates retained state through Raft; EMQX uses its retainer storage. Only client-observed retained replay is compared."},
		},
		Targets: make(map[string]bp02ComparisonTarget, 2),
	}
	for _, target := range []string{"narumq", "emqx"} {
		entry, ok := manifest.Targets[target]
		if !ok {
			continue
		}
		comparison.Targets[target] = bp02ComparisonTarget{
			Success:             entry.Success,
			Valid:               summary.Targets[target].Valid,
			WorkloadSummaryPath: entry.WorkloadSummaryPath,
			ResourceSummaryPath: entry.ResourceSummaryPath,
			FailureReason:       entry.FailureReason,
		}
	}
	return comparison
}

func summarizeBP02Run(manifest bp02Manifest) bp02Summary {
	summary := bp02Summary{SchemaVersion: 1, RunID: manifest.RunID, ReleaseGating: false, Valid: true, Targets: make(map[string]bp02TargetSummary, 2), FailureReason: manifest.FailureReason}
	for _, target := range []string{"narumq", "emqx"} {
		entry, ok := manifest.Targets[target]
		if !ok {
			summary.Valid = false
			if summary.FailureReason == "" {
				summary.FailureReason = "target " + target + " did not produce a manifest"
			}
			continue
		}
		summary.Targets[target] = bp02TargetSummary{Valid: entry.Valid, WorkloadSummary: entry.WorkloadSummary, ResourceSummary: entry.ResourceSummary, FailureReason: entry.FailureReason}
		if !entry.Valid {
			summary.Valid = false
			if summary.FailureReason == "" {
				summary.FailureReason = target + " target is invalid"
			}
		}
	}
	return summary
}

func writeBP02RunArtifacts(directory string, manifest bp02Manifest) error {
	summary := summarizeBP02Run(manifest)
	comparison := buildBP02Comparison(manifest, summary)
	var artifactErrors []error
	for name, value := range map[string]any{"manifest.json": manifest, "summary.json": summary, "comparison.json": comparison} {
		if err := writeBP02JSON(filepath.Join(directory, name), value); err != nil {
			artifactErrors = append(artifactErrors, err)
		}
	}
	return errors.Join(artifactErrors...)
}

func writeBP02JSON(path string, value any) error {
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

func readBP02JSON(path string, destination any) error {
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

func bp02RepositoryRoot() (string, error) {
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

func newBP02Manifest(options bp02RunOptions, startedAt time.Time) bp02Manifest {
	return bp02Manifest{
		SchemaVersion:         1,
		RunID:                 options.RunID,
		StartedAt:             startedAt.UTC().Format(time.RFC3339Nano),
		Purpose:               options.Profile.Purpose,
		ReleaseGating:         false,
		Profile:               bp02ArtifactProfile{RawSHA256: options.ProfileSHA256, Resolved: options.Profile},
		Git:                   options.Metadata.Git,
		GoVersion:             options.Metadata.GoVersion,
		OS:                    runtime.GOOS,
		Architecture:          runtime.GOARCH,
		Docker:                bp02DockerMetadata{ClientServerVersion: options.Metadata.DockerVersion},
		Command:               append([]string(nil), options.Command...),
		BuildBaseImage:        environment.BuildBaseImageReference,
		PlannedBuildBaseImage: environment.PlannedBuildBaseImage,
		RunnerImage:           options.Images.RunnerReference,
		RunnerImageID:         options.Images.RunnerID,
		EMQX: bp02EMQXMetadata{
			Version: environment.EMQXVersion, Edition: "Open Source", ImageReference: environment.EMQXImageReference,
			ImageIndex: strings.SplitN(environment.EMQXImageReference, "@", 2)[1],
		},
		Topology: bp02Topology{
			Nodes: bp02NodeCount, NetworkMode: bp02NetworkMode,
			NaruMQStorage: "Raft-backed cluster with per-node Docker named volumes",
			EMQXStorage:   "static EMQX OSS cluster with per-node Docker named volumes; storage equivalence is not claimed",
			SessionStorage: map[string]string{
				"narumq": "raft-replicated persistent session",
				"emqx":   "memory session (OSS)",
			},
		},
		SummaryPath:    filepath.Join(options.RunDir, "summary.json"),
		ComparisonPath: filepath.Join(options.RunDir, "comparison.json"),
		Targets:        make(map[string]bp02TargetManifest),
	}
}

func finishBP02PreparationFailure(stdout io.Writer, directory string, manifest bp02Manifest, cause error) error {
	manifest.FailureReason = cause.Error()
	manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	writeErr := writeBP02RunArtifacts(directory, manifest)
	_, outputErr := fmt.Fprintln(stdout, filepath.Join(directory, "manifest.json"))
	return errors.Join(cause, writeErr, outputErr)
}

func appendBP02Failure(existing string, err error) string {
	if err == nil {
		return existing
	}
	if existing == "" {
		return err.Error()
	}
	return errors.Join(errors.New(existing), err).Error()
}

func newBP02RunID() string {
	return "bp02-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + strconv.Itoa(os.Getpid())
}

func collectBP02HostMetadata(repoRoot string) bp02HostMetadata {
	commit, _ := bp02CommandText(context.Background(), repoRoot, "git", "rev-parse", "HEAD")
	status, _ := bp02CommandText(context.Background(), repoRoot, "git", "status", "--porcelain", "--untracked-files=all")
	goVersion, _ := bp02CommandText(context.Background(), repoRoot, "go", "version")
	dockerVersion, _ := bp02CommandText(context.Background(), repoRoot, "docker", "version", "--format", "{{json .}}")
	return bp02HostMetadata{
		Git:           bp02GitMetadata{Commit: strings.TrimSpace(commit), Dirty: strings.TrimSpace(status) != ""},
		GoVersion:     strings.TrimSpace(goVersion),
		DockerVersion: strings.TrimSpace(dockerVersion),
	}
}

func bp02CommandText(ctx context.Context, workingDirectory, name string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, name, args...)
	if workingDirectory != "" {
		command.Dir = workingDirectory
	}
	output, err := command.CombinedOutput()
	return string(output), err
}

func validateBP02TargetOrder(order []string) error {
	if len(order) != 2 {
		return errors.New("target order must contain exactly two targets")
	}
	seen := make(map[string]struct{}, len(order))
	for _, target := range order {
		if target != "narumq" && target != "emqx" {
			return fmt.Errorf("unsupported BP-02 target %q", target)
		}
		if _, exists := seen[target]; exists {
			return fmt.Errorf("target order contains duplicate %q", target)
		}
		seen[target] = struct{}{}
	}
	return nil
}
