package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	localFUSEProfile = "local-fuse-v1"
	localExt4Profile = "local-ext4-v1"
)

var ext4Scenarios = []string{"follower-disk-full", "quorum-disk-full", "cluster-process-crash", "filesystem-recovery-boundaries"}

func storageProfile(profile string) (string, error) {
	if profile == "" {
		return localFUSEProfile, nil
	}
	if profile != localFUSEProfile && profile != localExt4Profile {
		return "", fmt.Errorf("unsupported storage profile %q", profile)
	}
	return profile, nil
}

func profileBoundary(profile string) string {
	if profile == localExt4Profile {
		return "local Linux ext4; process crash and injected recovery inputs; shared host; no EBS or power-loss guarantee"
	}
	return "local tmpfs/FUSE fault and power-cut simulation; no production power-loss guarantee"
}

func profileScenarios(profile string) []string {
	if profile == localExt4Profile {
		return ext4Scenarios
	}
	return storageFaultScenarios
}

type ext4MountEvidence struct {
	Event        string `json:"event"`
	Kernel       string `json:"kernel"`
	Filesystem   string `json:"filesystem"`
	MountOptions string `json:"mountOptions"`
	LoopDevice   string `json:"loopDevice"`
	ImageMiB     int    `json:"imageMiB"`
	BrokerSHA256 string `json:"brokerSHA256"`
}

func parseExt4Mount(logs []byte, imageMiB int) (ext4MountEvidence, error) {
	scanner := bufio.NewScanner(bytes.NewReader(logs))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var found *ext4MountEvidence
	for scanner.Scan() {
		var event ext4MountEvidence
		if json.Unmarshal(scanner.Bytes(), &event) != nil || event.Event != "ext4-mount" {
			continue
		}
		if found != nil {
			return ext4MountEvidence{}, errors.New("multiple ext4 mount events")
		}
		found = &event
	}
	if err := scanner.Err(); err != nil {
		return ext4MountEvidence{}, err
	}
	if found == nil {
		return ext4MountEvidence{}, errors.New("missing actual ext4 mount evidence")
	}
	checksum, err := hex.DecodeString(found.BrokerSHA256)
	if found.Filesystem != "ext4" || found.Kernel == "" || found.ImageMiB != imageMiB ||
		!strings.HasPrefix(found.LoopDevice, "/dev/loop") ||
		!slices.Contains(strings.Split(found.MountOptions, ","), "data=ordered") || err != nil || len(checksum) != 32 {
		return ext4MountEvidence{}, fmt.Errorf("invalid ext4 profile evidence: %+v", *found)
	}
	return *found, nil
}

func validateExt4Cleanup(logs []byte) error {
	scanner := bufio.NewScanner(bytes.NewReader(logs))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var event struct {
			Event  string `json:"event"`
			Status string `json:"status"`
		}
		if json.Unmarshal(scanner.Bytes(), &event) == nil && event.Event == "ext4-cleanup" && event.Status == "passed" {
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("ext4 unmount and loop detach were not confirmed")
}

func (scenario *scenarioDocker) createExt4Volume(ctx context.Context, nodeID string) (string, error) {
	name := scenario.networkName + "-node" + nodeID + "-ext4"
	output, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "volume", "ls", "--filter", "name=^"+name+"$", "--format", "{{.Name}}")
	if err != nil {
		return "", fmt.Errorf("check ext4 volume ownership: %w", err)
	}
	if len(bytes.TrimSpace(output)) != 0 {
		return "", fmt.Errorf("refusing to reuse existing ext4 volume %s", name)
	}
	output, err = scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "volume", "create", name)
	if err != nil {
		return "", fmt.Errorf("create ext4 backing volume: %w (%s)", err, output)
	}
	scenario.volumeNames[nodeID] = name
	if err := scenario.recorder.record("ext4-volume", nodeID, map[string]string{"operation": "create", "volume": name}); err != nil {
		return "", err
	}
	return name, nil
}

func (scenario *storageFaultScenario) captureExt4Mounts(ctx context.Context) error {
	scenario.evidence.Filesystems = make(map[string]ext4MountEvidence, chaosNodeCount)
	for _, nodeID := range nodeIDs() {
		logs, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "logs", scenario.docker.containerNames[nodeID])
		if err != nil {
			return fmt.Errorf("read node %s ext4 mount evidence: %w", nodeID, err)
		}
		mount, err := parseExt4Mount(logs, 256)
		if err != nil {
			return fmt.Errorf("node %s: %w", nodeID, err)
		}
		scenario.evidence.Filesystems[nodeID] = mount
		if err := scenario.recorder.record("filesystem", nodeID, mount); err != nil {
			return err
		}
	}
	return nil
}

var requiredRecoveryTests = []string{
	"narumq/internal/adapters/raft.TestStableStoreSnapshotCompactsWALToSnapshotTail",
	"narumq/internal/adapters/raft.TestStableStoreCompactionSplitsLargeTailIntoFrames",
	"narumq/internal/adapters/raft.TestStableStoreRecoversEveryCompactionCrashState",
	"narumq/internal/adapters/raft.TestStableStoreCompactionFailureKeepsRecoverableState",
	"narumq/internal/adapters/raft.TestWALReaderHandlesTornAndCorruptFrames",
	"narumq/tests/integration.TestSnapshotFinalizationCrashRecoveryAcrossProcessRestart",
	"narumq/tests/integration.TestDynamicClusterWALCompactionProcessAcceptance",
	"narumq/tests/integration.TestClusterBackupRestoreProcessCrashAndResync",
	"narumq/tests/integration.TestDynamicClusterVoterVolumeLossProcessAcceptance",
	"narumq/tests/integration.TestDynamicClusterJoinerVoterVolumeLossProcessAcceptance",
}

func validateRecoveryTests(logs []byte) ([]string, error) {
	passed := make(map[string]bool, len(requiredRecoveryTests))
	packages := make(map[string]bool, 2)
	scanner := bufio.NewScanner(bytes.NewReader(logs))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var event struct {
			Action  string
			Package string
			Test    string
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("decode recovery evidence: %w", err)
		}
		if event.Action == "skip" || event.Action == "fail" {
			return nil, fmt.Errorf("recovery test %s.%s reported %s", event.Package, event.Test, event.Action)
		}
		if event.Action == "pass" {
			if event.Test == "" {
				packages[event.Package] = true
			} else {
				passed[event.Package+"."+event.Test] = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	for _, test := range requiredRecoveryTests {
		if !passed[test] {
			return nil, fmt.Errorf("required recovery test %s did not pass", test)
		}
	}
	for _, name := range []string{"narumq/internal/adapters/raft", "narumq/tests/integration"} {
		if !packages[name] {
			return nil, fmt.Errorf("recovery package %s did not finish successfully", name)
		}
	}
	return slices.Clone(requiredRecoveryTests), nil
}

func runExt4RecoveryBoundaries(ctx context.Context, options scenarioRunOptions) (result scenarioSummary) {
	result = scenarioSummary{ID: options.ScenarioID, Status: "failed"}
	directory := filepath.Join(options.OutputDir, "scenarios", options.ScenarioID)
	recorder, err := newEventRecorder(directory)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	docker := newScenarioDocker(options, recorder)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), scenarioCleanupTimeout)
		cleanupErr := docker.cleanup(cleanupCtx)
		cancel()
		result.Error = joinErrorMessage(result.Error, cleanupErr)
		result.Error = joinErrorMessage(result.Error, recorder.close())
		if result.Error == "" {
			result.Status = "passed"
		}
	}()
	volume, err := docker.createExt4Volume(ctx, "1")
	if err != nil {
		result.Error = err.Error()
		return result
	}
	name := docker.networkName + "-node1"
	created, createErr := options.Runner.Run(ctx, options.RepoRoot, "docker", "create", "--name", name,
		"--privileged", "--read-only", "--cpus", "4", "--memory", "2g",
		"--mount", "type=volume,source="+volume+",destination=/images",
		"--tmpfs", "/tmp:size=1g", "--env", "NARUMQ_EXT4_MIB=1024", options.Image, "recovery-boundaries")
	if createErr != nil {
		result.Error = fmt.Sprintf("create recovery container: %v (%s)", createErr, created)
		return result
	}
	docker.containerNames["1"] = name
	output, runErr := options.Runner.Run(ctx, options.RepoRoot, "docker", "start", "--attach", name)
	writeErr := os.WriteFile(filepath.Join(directory, "recovery.jsonl"), output, 0o600)
	if runErr != nil || writeErr != nil {
		result.Error = errors.Join(runErr, writeErr).Error()
		return result
	}
	mount, err := parseExt4Mount(output, 1024)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Evidence.Filesystems = map[string]ext4MountEvidence{"1": mount}
	result.Evidence.RecoveryTests, err = validateRecoveryTests(output)
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func (scenario *scenarioDocker) removeExt4Volumes(ctx context.Context) error {
	var errs []error
	for _, nodeID := range []string{"1", "2", "3"} {
		name, ok := scenario.volumeNames[nodeID]
		if !ok {
			continue
		}
		output, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "volume", "rm", name)
		errs = append(errs, err, scenario.recorder.record("ext4-volume", nodeID, map[string]string{"operation": "remove", "volume": name, "output": strings.TrimSpace(string(output)), "error": errorString(err)}))
	}
	return errors.Join(errs...)
}

func (scenario *scenarioDocker) signalBrokerCrash(ctx context.Context, nodeID string) error {
	name, ok := scenario.containerNames[nodeID]
	if !ok {
		return fmt.Errorf("container for node %s is unavailable", nodeID)
	}
	output, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "kill", "--signal", "SIGUSR2", name)
	return errors.Join(err, scenario.recorder.record("control", nodeID, map[string]string{"operation": "broker-process-crash", "output": strings.TrimSpace(string(output)), "error": errorString(err)}))
}

func (scenario *storageFaultScenario) runClusterProcessCrash(ctx context.Context, leaderID string) error {
	expected := make([]string, 0, 2)
	for _, qos := range []byte{1, 2} {
		topic := durableTopic(scenario.options.ScenarioID, "leader", fmt.Sprintf("before-qos%d", qos))
		if err := scenario.durablePublish(ctx, leaderID, "chaos-durable-pub-3", topic, qos, "before-fault", true); err != nil {
			return err
		}
		expected = append(expected, topic)
	}
	if err := scenario.durableSubscriber.close(); err != nil {
		return err
	}
	scenario.durableSubscriber = nil
	before, err := scenario.node(ctx, leaderID, leaderID)
	if err != nil {
		return err
	}
	if before.LastAppliedIndex <= 0 {
		return errors.New("missing committed index before broker crash")
	}
	scenario.evidence.LeaderAppliedIndexStart = before.LastAppliedIndex
	for _, nodeID := range nodeIDs() {
		if err := scenario.docker.signalBrokerCrash(ctx, nodeID); err != nil {
			return err
		}
	}
	scenario.evidence.BrokerCrashExitCodes = make(map[string]int, chaosNodeCount)
	if err := waitFor(ctx, 20*time.Second, 100*time.Millisecond, func() (bool, error) {
		for _, nodeID := range nodeIDs() {
			logs, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "logs", scenario.docker.containerNames[nodeID])
			if err != nil {
				return false, err
			}
			if err := validateBrokerCrash(logs); err != nil {
				return false, fmt.Errorf("node %s process crash: %w", nodeID, err)
			}
			scenario.evidence.BrokerCrashExitCodes[nodeID] = 137
		}
		return true, nil
	}); err != nil {
		return err
	}
	for _, nodeID := range nodeIDs() {
		if err := scenario.docker.signalBrokerRestart(ctx, nodeID); err != nil {
			return err
		}
	}
	if err := scenario.waitAllReady(ctx, recoveryTimeout); err != nil {
		return err
	}
	recoveredLeader, err := scenario.waitLeaderAgreement(ctx)
	if err != nil {
		return err
	}
	scenario.evidence.RecoveryLeaderNodeID = recoveredLeader
	converged, err := scenario.waitForIndexConvergence(ctx, before.LastAppliedIndex-1, recoveryTimeout)
	if err != nil {
		return err
	}
	scenario.evidence.ConvergedAppliedIndex = converged
	if err := scenario.reconnectDurableSubscriber(ctx, recoveredLeader); err != nil {
		return err
	}
	return scenario.verifyDurableDelivery(ctx, recoveredLeader, expected, nil)
}

func validateBrokerCrash(logs []byte) error {
	scanner := bufio.NewScanner(bytes.NewReader(logs))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		var event struct {
			Event    string `json:"event"`
			ExitCode int    `json:"exitCode"`
		}
		if json.Unmarshal(scanner.Bytes(), &event) == nil && event.Event == "broker-crash" {
			if event.ExitCode != 137 {
				return fmt.Errorf("broker crash exit code %d, want 137", event.ExitCode)
			}
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("missing broker process SIGKILL evidence")
}
