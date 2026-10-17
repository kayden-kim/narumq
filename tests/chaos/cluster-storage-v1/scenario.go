package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	startupTimeout         = 120 * time.Second
	recoveryTimeout        = 150 * time.Second
	followerDetectTimeout  = 90 * time.Second
	slowDetectTimeout      = 60 * time.Second
	progressWindow         = 30 * time.Second
	quorumHaltWindow       = 20 * time.Second
	probeInterval          = time.Second
	progressProbeInterval  = 500 * time.Millisecond
	queueSampleInterval    = time.Second
	scenarioCleanupTimeout = 2 * time.Minute
)

type storageFaultScenario struct {
	options           scenarioRunOptions
	fixture           *chaosFixture
	docker            *scenarioDocker
	recorder          *eventRecorder
	api               *apiHTTPClient
	evidence          scenarioEvidence
	probeID           int
	durableSubscriber *durableClient
}

func runStorageScenario(ctx context.Context, options scenarioRunOptions) (result scenarioSummary) {
	if options.Profile == localExt4Profile && options.ScenarioID == "filesystem-recovery-boundaries" {
		return runExt4RecoveryBoundaries(ctx, options)
	}
	result = scenarioSummary{ID: options.ScenarioID, Status: "failed"}
	scenarioDirectory := filepath.Join(options.OutputDir, "scenarios", options.ScenarioID)
	recorder, err := newEventRecorder(scenarioDirectory)
	if err != nil {
		result.Error = fmt.Sprintf("create scenario event log: %v", err)
		return result
	}
	scenario := &storageFaultScenario{options: options, recorder: recorder}
	if err := recorder.record("scenario", "", map[string]any{"operation": "start", "scenario": options.ScenarioID}); err != nil {
		result.Error = fmt.Sprintf("record scenario start: %v", err)
	}
	fixture, fixtureErr := createChaosFixture()
	if fixtureErr != nil {
		result.Error = joinErrorMessage(result.Error, fixtureErr)
	} else {
		scenario.fixture = fixture
		scenario.docker = newScenarioDocker(options, recorder)
		scenario.api = newAPIHTTPClient(fixture.AdminToken, scenario.docker.managementPorts)
		runErr := scenario.run(ctx)
		if runErr != nil {
			result.Error = runErr.Error()
		}
	}

	if scenario.docker != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), scenarioCleanupTimeout)
		cleanupErr := scenario.docker.cleanup(cleanupCtx)
		cancel()
		if cleanupErr != nil {
			result.Error = joinErrorMessage(result.Error, fmt.Errorf("scenario cleanup: %w", cleanupErr))
		}
	}
	if err := recorder.record("scenario", "", map[string]any{"operation": "finish", "error": result.Error}); err != nil {
		result.Error = joinErrorMessage(result.Error, fmt.Errorf("record scenario finish: %w", err))
	}
	if err := recorder.close(); err != nil {
		result.Error = joinErrorMessage(result.Error, fmt.Errorf("close scenario event log: %w", err))
	}
	if scenario.fixture != nil {
		if err := scenario.fixture.close(); err != nil {
			result.Error = joinErrorMessage(result.Error, fmt.Errorf("remove temporary fixture: %w", err))
		}
	}
	result.Evidence = scenario.evidence
	if result.Error == "" {
		result.Status = "passed"
	}
	return result
}

func (scenario *storageFaultScenario) run(ctx context.Context) error {
	if err := scenario.docker.createNetwork(ctx); err != nil {
		return err
	}
	if err := scenario.docker.startNodes(ctx, scenario.fixture); err != nil {
		return err
	}
	leader, err := scenario.commonStartup(ctx)
	if err != nil {
		return err
	}
	if scenario.options.Profile == localExt4Profile {
		if err := scenario.captureExt4Mounts(ctx); err != nil {
			return err
		}
	}
	if err := scenario.prepareDurableSubscriber(ctx, leader); err != nil {
		return err
	}
	defer func() {
		if scenario.durableSubscriber != nil {
			_ = scenario.durableSubscriber.close()
		}
	}()
	scenario.evidence.LeaderNodeID = leader
	followers := followerNodeIDs(leader)
	switch scenario.options.ScenarioID {
	case "follower-disk-full":
		return scenario.runFollowerDiskFull(ctx, leader, followers[0], followers[1])
	case "quorum-disk-full":
		return scenario.runQuorumDiskFull(ctx, leader, followers)
	case "follower-slow-io":
		return scenario.runFollowerSlowIO(ctx, leader, followers[0], followers[1])
	case "follower-sync-error":
		return scenario.runFollowerSyncError(ctx, leader, followers[0], followers[1])
	case "follower-torn-power-loss":
		return scenario.runFollowerTornPowerLoss(ctx, leader, followers[0], followers[1])
	case "cluster-power-loss":
		return scenario.runClusterPowerLoss(ctx, leader, followers)
	case "cluster-process-crash":
		return scenario.runClusterProcessCrash(ctx, leader)
	case "follower-disk-pressure":
		return scenario.runFollowerDiskPressure(ctx, leader, followers[0], followers[1])
	default:
		return fmt.Errorf("unknown storage-fault scenario %q", scenario.options.ScenarioID)
	}
}

func (scenario *storageFaultScenario) commonStartup(ctx context.Context) (string, error) {
	if err := scenario.waitAllReady(ctx, startupTimeout); err != nil {
		return "", err
	}
	leader, err := scenario.waitLeaderAgreement(ctx)
	if err != nil {
		return "", err
	}
	if err := waitFor(ctx, 30*time.Second, probeInterval, func() (bool, error) {
		result := scenario.mqtt(ctx, leader, "chaos-baseline", "chaos/baseline/#")
		if result.Classification != "accepted" {
			return false, fmt.Errorf("baseline MQTT probe on leader %s was %s", leader, result.Classification)
		}
		return true, nil
	}); err != nil {
		return "", fmt.Errorf("leader baseline MQTT admission: %w", err)
	}
	if err := waitFor(ctx, 30*time.Second, probeInterval, func() (bool, error) {
		for _, nodeID := range nodeIDs() {
			response, status, _, err := scenario.subscriptions(ctx, nodeID)
			if err != nil {
				return false, err
			}
			if status != http.StatusOK || !hasSubscription(response, "chaos-baseline", "chaos/baseline/#") {
				return false, fmt.Errorf("node %s does not list the baseline subscription", nodeID)
			}
		}
		return true, nil
	}); err != nil {
		return "", fmt.Errorf("baseline subscription did not appear on every node: %w", err)
	}
	return leader, nil
}

func (scenario *storageFaultScenario) waitAllReady(ctx context.Context, timeout time.Duration) error {
	if err := waitFor(ctx, timeout, probeInterval, func() (bool, error) {
		for _, nodeID := range nodeIDs() {
			observation, err := scenario.health(ctx, nodeID, "/health/ready")
			if err != nil {
				return false, err
			}
			if observation.HTTPStatus != http.StatusOK {
				return false, fmt.Errorf("node %s readiness returned HTTP %d", nodeID, observation.HTTPStatus)
			}
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("wait for all nodes to become ready: %w", err)
	}
	return nil
}

func (scenario *storageFaultScenario) waitLeaderAgreement(ctx context.Context) (string, error) {
	var leader string
	if err := waitFor(ctx, 30*time.Second, probeInterval, func() (bool, error) {
		var agreed string
		for _, nodeID := range nodeIDs() {
			cluster, status, _, err := scenario.cluster(ctx, nodeID)
			if err != nil {
				return false, err
			}
			if status != http.StatusOK || cluster.LeaderNodeID == nil {
				return false, fmt.Errorf("node %s has no cluster leader yet", nodeID)
			}
			if cluster.ClusterID != "narumq-chaos" {
				return false, fmt.Errorf("node %s reports unexpected cluster ID %q", nodeID, cluster.ClusterID)
			}
			if agreed == "" {
				agreed = *cluster.LeaderNodeID
			} else if agreed != *cluster.LeaderNodeID {
				return false, fmt.Errorf("nodes disagree on leader: %s and %s", agreed, *cluster.LeaderNodeID)
			}
		}
		if !containsNodeID(nodeIDs(), agreed) {
			return false, fmt.Errorf("cluster reported unknown leader node %q", agreed)
		}
		leader = agreed
		return true, nil
	}); err != nil {
		return "", fmt.Errorf("wait for leader agreement: %w", err)
	}
	return leader, nil
}

func (scenario *storageFaultScenario) health(ctx context.Context, nodeID, path string) (healthObservation, error) {
	observation, err := scenario.api.health(ctx, nodeID, path)
	recordErr := scenario.recorder.record("http", nodeID, map[string]any{
		"path": path, "statusCode": observation.HTTPStatus, "healthStatus": observation.Status,
		"reason": observation.Reason, "errorCode": observation.ErrorCode,
		"errorMessage": observation.ErrorMessage, "durationMs": observation.DurationMs, "error": errorString(err),
	})
	return observation, errors.Join(err, recordErr)
}

func (scenario *storageFaultScenario) cluster(ctx context.Context, nodeID string) (clusterData, int, int64, error) {
	observation, status, duration, err := scenario.api.cluster(ctx, nodeID)
	recordErr := scenario.recorder.record("http", nodeID, map[string]any{
		"path": "/api/v1/cluster", "statusCode": status, "clusterId": observation.ClusterID,
		"leaderNodeId": observation.LeaderNodeID, "quorumAvailable": observation.QuorumAvailable,
		"durationMs": duration, "error": errorString(err),
	})
	return observation, status, duration, errors.Join(err, recordErr)
}

func (scenario *storageFaultScenario) node(ctx context.Context, observerID, nodeID string) (nodeData, error) {
	observation, status, duration, err := scenario.api.node(ctx, observerID, nodeID)
	recordErr := scenario.recorder.record("http", observerID, map[string]any{
		"path": "/api/v1/nodes/" + nodeID, "statusCode": status, "nodeId": observation.NodeID,
		"ready": observation.Ready, "state": observation.State, "role": observation.Role,
		"lastAppliedIndex": observation.LastAppliedIndex, "replicationHealth": observation.ReplicationHealth,
		"lastErrorCode": observation.LastErrorCode, "storagePressure": observation.StoragePressure,
		"durationMs": duration, "error": errorString(err),
	})
	if recordErr == nil && observation.ReplicationQueue != nil {
		recordErr = scenario.recorder.record("queue", observerID, map[string]any{
			"targetNodeId": nodeID, "queue": observation.ReplicationQueue,
		})
	}
	if err == nil && status != http.StatusOK {
		err = fmt.Errorf("node summary from node %s returned HTTP %d", observerID, status)
	}
	return observation, errors.Join(err, recordErr)
}

func (scenario *storageFaultScenario) subscriptions(ctx context.Context, nodeID string) (subscriptionEnvelope, int, int64, error) {
	observation, status, duration, err := scenario.api.subscriptions(ctx, nodeID)
	recordErr := scenario.recorder.record("http", nodeID, map[string]any{
		"path": "/api/v1/subscriptions?tenantId=chaos&limit=50", "statusCode": status,
		"subscriptions": observation.Data, "hasMore": observation.Page.HasMore,
		"durationMs": duration, "error": errorString(err),
	})
	return observation, status, duration, errors.Join(err, recordErr)
}

func (scenario *storageFaultScenario) mqtt(ctx context.Context, nodeID, clientID, filter string) mqttProbeResult {
	address := "127.0.0.1:" + scenario.docker.mqttPorts[nodeID]
	result := probeMQTT(ctx, address, scenario.fixture.AccessToken, clientID, filter)
	err := scenario.recorder.record("mqtt", nodeID, map[string]any{
		"clientId": clientID, "topicFilter": filter, "classification": result.Classification,
		"connackCode": result.ConnackCode, "subackCode": result.SubackCode,
		"durationMs": result.DurationMs, "error": result.Error,
	})
	if err != nil {
		result.Error = joinErrorMessage(result.Error, err)
		result.Classification = "closed"
	}
	return result
}

func (scenario *storageFaultScenario) mqttWithReadiness(ctx context.Context, nodeID, clientID, filter string) (mqttProbeResult, bool, error) {
	ready, err := scenario.health(ctx, nodeID, "/health/ready")
	if err != nil {
		return mqttProbeResult{}, false, err
	}
	probe := scenario.mqtt(ctx, nodeID, clientID, filter)
	return probe, ready.HTTPStatus == http.StatusOK, nil
}

func (scenario *storageFaultScenario) nextProbeID() string {
	scenario.probeID = scenario.probeID%8 + 1
	return fmt.Sprintf("chaos-probe-%d", scenario.probeID)
}

func recoveryClientID(nodeID string) string {
	return "chaos-recovery-" + nodeID
}

func (scenario *storageFaultScenario) sampleQueue(ctx context.Context, leaderID, targetID string) (nodeData, error) {
	node, err := scenario.node(ctx, leaderID, targetID)
	if err != nil {
		return nodeData{}, err
	}
	if node.ReplicationQueue == nil {
		return nodeData{}, fmt.Errorf("leader %s omitted replication queue for node %s", leaderID, targetID)
	}
	sample := *node.ReplicationQueue
	if err := validateQueueSample(sample); err != nil {
		return nodeData{}, err
	}
	scenario.evidence.QueueSamples++
	if sample.OutboundMessages > scenario.evidence.MaxOutboundMessages {
		scenario.evidence.MaxOutboundMessages = sample.OutboundMessages
	}
	if sample.OutboundBytes > scenario.evidence.MaxOutboundBytes {
		scenario.evidence.MaxOutboundBytes = sample.OutboundBytes
	}
	if sample.RejectedMessages > scenario.evidence.MaxRejectedMessages {
		scenario.evidence.MaxRejectedMessages = sample.RejectedMessages
	}
	if queueCapReached(sample) {
		scenario.evidence.QueueCapReached = true
	}
	return node, nil
}

func (scenario *storageFaultScenario) fillDisk(ctx context.Context, nodeID string) (diskFillEvidence, error) {
	output, err := scenario.docker.execNode(ctx, nodeID, "fill-disk", "--path", "/var/lib/narumq/chaos-ballast.bin")
	if err != nil {
		return diskFillEvidence{}, err
	}
	var result struct {
		BytesWritten   uint64 `json:"bytesWritten"`
		FreeBytesAfter uint64 `json:"freeBytesAfter"`
		StoppedBy      string `json:"stoppedBy"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		return diskFillEvidence{}, fmt.Errorf("decode fill-disk result from node %s: %w", nodeID, err)
	}
	if result.StoppedBy != "ENOSPC" || result.FreeBytesAfter >= 4096 {
		return diskFillEvidence{}, fmt.Errorf("node %s filesystem did not reach ENOSPC: stoppedBy=%q freeBytesAfter=%d", nodeID, result.StoppedBy, result.FreeBytesAfter)
	}
	evidence := diskFillEvidence{BytesWritten: result.BytesWritten, FreeBytesAfter: result.FreeBytesAfter, StoppedBy: result.StoppedBy}
	if scenario.evidence.DiskFills == nil {
		scenario.evidence.DiskFills = make(map[string]diskFillEvidence)
	}
	scenario.evidence.DiskFills[nodeID] = evidence
	if scenario.options.ScenarioID != "quorum-disk-full" {
		scenario.evidence.DiskFill = &evidence
	}
	return evidence, scenario.recorder.record("disk", nodeID, evidence)
}

func (scenario *storageFaultScenario) fillDiskLeaving(ctx context.Context, nodeID string, leave uint64) (diskFillEvidence, error) {
	output, err := scenario.docker.execNode(ctx, nodeID, "fill-disk", "--path", "/var/lib/narumq/chaos-ballast.bin", "--leave-bytes", fmt.Sprint(leave))
	if err != nil {
		return diskFillEvidence{}, err
	}
	var evidence diskFillEvidence
	if err := json.Unmarshal(output, &evidence); err != nil {
		return diskFillEvidence{}, fmt.Errorf("decode fill-disk result: %w", err)
	}
	if evidence.StoppedBy != "leave-bytes" || evidence.FreeBytesAfter >= 8<<20 {
		return diskFillEvidence{}, fmt.Errorf("node %s did not reach disk pressure: %+v", nodeID, evidence)
	}
	scenario.evidence.DiskFill = &evidence
	return evidence, scenario.recorder.record("disk", nodeID, evidence)
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-205
func (scenario *storageFaultScenario) runFollowerDiskPressure(ctx context.Context, leader, target, healthy string) error {
	scenario.evidence.TargetNodeIDs = []string{target}
	if _, err := scenario.fillDiskLeaving(ctx, target, 4<<20); err != nil {
		return err
	}
	start := time.Now()
	if err := waitFor(ctx, 10*time.Second, probeInterval, func() (bool, error) {
		node, err := scenario.node(ctx, target, target)
		if err != nil {
			return false, err
		}
		return node.StoragePressure && node.ReplicationHealth == "HEALTHY" && node.Ready, nil
	}); err != nil {
		return fmt.Errorf("wait follower disk pressure: %w", err)
	}
	scenario.evidence.DetectionDurationMs = time.Since(start).Milliseconds()
	ready, err := scenario.health(ctx, target, "/health/ready")
	if err != nil {
		return err
	}
	if ready.HTTPStatus != http.StatusOK {
		return fmt.Errorf("disk pressure changed readiness: HTTP %d", ready.HTTPStatus)
	}
	scenario.evidence.TargetReadiness = &ready
	scenario.evidence.TargetReplicationHealth = "HEALTHY"
	pressure := true
	scenario.evidence.TargetStoragePressure = &pressure
	prefix := "chaos/durable/follower-disk-pressure/"
	if err := scenario.publishDiskPressureRejected(ctx, target, prefix+"target/rejected"); err != nil {
		return err
	}
	if err := scenario.durablePublish(ctx, leader, "chaos-durable-pub-1", prefix+"leader/during", 1, "during-fault", true); err != nil {
		return err
	}
	if err := scenario.durablePublish(ctx, healthy, "chaos-durable-pub-2", prefix+"healthy/during", 1, "during-fault", true); err != nil {
		return err
	}
	if _, err := scenario.docker.execNode(ctx, target, "free-disk", "--path", "/var/lib/narumq/chaos-ballast.bin"); err != nil {
		return err
	}
	if err := waitFor(ctx, 10*time.Second, probeInterval, func() (bool, error) {
		node, err := scenario.node(ctx, target, target)
		if err != nil {
			return false, err
		}
		return !node.StoragePressure, nil
	}); err != nil {
		return fmt.Errorf("wait follower pressure recovery: %w", err)
	}
	if err := scenario.durablePublish(ctx, target, "chaos-durable-pub-3", prefix+"target/after", 1, "after-recovery", true); err != nil {
		return err
	}
	return scenario.verifyDurableDelivery(ctx, leader, []string{prefix + "leader/during", prefix + "healthy/during", prefix + "target/after"}, nil)
}

func (scenario *storageFaultScenario) publishDiskPressureRejected(ctx context.Context, nodeID, topic string) error {
	client, _, err := dialDurableClient(ctx, "127.0.0.1:"+scenario.docker.mqttPorts[nodeID], scenario.fixture.AccessToken, "chaos-pressure-reject", true)
	if err != nil {
		return err
	}
	defer client.close()
	packetID, err := client.publish(ctx, topic, []byte(topic), 1)
	if err != nil {
		return err
	}
	ackCtx, cancel := context.WithTimeout(ctx, durableAckTimeout)
	outcome, ackErr := client.awaitAck(ackCtx, packetID, 1)
	cancel()
	if err := scenario.recordDurableAck(nodeID, "chaos-pressure-reject", topic, 1, "during-fault", outcome, ackErr); err != nil {
		return err
	}
	if outcome != durableClosed {
		return fmt.Errorf("disk-pressure publish outcome %q, want closed within %s: %v", outcome, durableAckTimeout, ackErr)
	}
	return nil
}

func (scenario *storageFaultScenario) inspectFailedFollower(ctx context.Context, nodeID, expectedCode, expectedMessage string) error {
	ready, err := scenario.health(ctx, nodeID, "/health/ready")
	if err != nil {
		return err
	}
	if ready.HTTPStatus != http.StatusServiceUnavailable || ready.ErrorMessage != expectedMessage {
		return fmt.Errorf("node %s readiness = HTTP %d %q, want 503 %q", nodeID, ready.HTTPStatus, ready.ErrorMessage, expectedMessage)
	}
	live, err := scenario.health(ctx, nodeID, "/health/live")
	if err != nil {
		return err
	}
	if live.HTTPStatus != http.StatusServiceUnavailable || live.ErrorMessage != expectedMessage {
		return fmt.Errorf("node %s liveness = HTTP %d %q, want 503 %q", nodeID, live.HTTPStatus, live.ErrorMessage, expectedMessage)
	}
	summary, err := scenario.node(ctx, nodeID, nodeID)
	if err != nil {
		return err
	}
	if summary.Ready || summary.ReplicationHealth != "FAILED" || summary.LastErrorCode != expectedCode || !summary.StoragePressure {
		return fmt.Errorf("node %s failed summary mismatch: ready=%t health=%q code=%q pressure=%t", nodeID, summary.Ready, summary.ReplicationHealth, summary.LastErrorCode, summary.StoragePressure)
	}
	running, err := scenario.docker.containerRunning(ctx, nodeID)
	if err != nil {
		return err
	}
	if !running {
		return fmt.Errorf("node %s container exited after storage failure", nodeID)
	}
	if summary.ReadinessReason == nil || *summary.ReadinessReason != expectedMessage {
		return fmt.Errorf("node %s summary readiness reason = %v, want %q", nodeID, summary.ReadinessReason, expectedMessage)
	}
	scenario.evidence.TargetReadiness = &ready
	scenario.evidence.TargetLiveness = &live
	scenario.evidence.TargetReplicationHealth = summary.ReplicationHealth
	scenario.evidence.TargetLastErrorCode = summary.LastErrorCode
	pressure := summary.StoragePressure
	scenario.evidence.TargetStoragePressure = &pressure
	return nil
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-026, cluster-session-and-delivery.md:CLU-SCN-084, cluster-session-and-delivery.md:CLU-SCN-160
func (scenario *storageFaultScenario) runFollowerDiskFull(ctx context.Context, leaderID, targetID, healthyID string) error {
	scenario.evidence.TargetNodeIDs = []string{targetID}
	if _, err := scenario.fillDisk(ctx, targetID); err != nil {
		return err
	}
	detectedAt := time.Now()
	if err := waitFor(ctx, followerDetectTimeout, progressProbeInterval, func() (bool, error) {
		probe := scenario.mqtt(ctx, leaderID, scenario.nextProbeID(), "chaos/probe/#")
		if probe.Classification != "accepted" {
			return false, fmt.Errorf("leader MQTT progress probe was %s during follower failure detection", probe.Classification)
		}
		target, err := scenario.node(ctx, targetID, targetID)
		if err != nil {
			return false, err
		}
		return target.ReplicationHealth == "FAILED", nil
	}); err != nil {
		return fmt.Errorf("target follower did not report FAILED: %w", err)
	}
	scenario.evidence.DetectionDurationMs = time.Since(detectedAt).Milliseconds()
	if err := scenario.inspectFailedFollower(ctx, targetID, "storage_no_space", "raft storage failed: no space left; restart required"); err != nil {
		return err
	}
	for attempt := 0; attempt < 3; attempt++ {
		probe := scenario.mqtt(ctx, targetID, scenario.nextProbeID(), "chaos/probe/#")
		if probe.Classification == "accepted" {
			return fmt.Errorf("failed follower %s accepted an MQTT probe", targetID)
		}
	}
	expectedDurableTopics, err := scenario.publishFollowerFaultMessages(ctx, leaderID, healthyID)
	if err != nil {
		return err
	}
	if err := scenario.observeHealthyProgress(ctx, leaderID, targetID, healthyID, true, true); err != nil {
		return err
	}
	if err := scenario.recoverFollower(ctx, leaderID, targetID, 120*time.Second, true); err != nil {
		return err
	}
	return scenario.verifyDurableDelivery(ctx, leaderID, expectedDurableTopics, nil)
}

func (scenario *storageFaultScenario) observeHealthyProgress(ctx context.Context, leaderID, targetID, healthyID string, requireQueueCap, requireAppliedConvergence bool) error {
	leaderStart, err := scenario.node(ctx, leaderID, leaderID)
	if err != nil {
		return err
	}
	healthyStart, err := scenario.node(ctx, healthyID, healthyID)
	if err != nil {
		return err
	}
	scenario.evidence.LeaderAppliedIndexStart = leaderStart.LastAppliedIndex
	progressStarted := time.Now()
	progressDeadline := progressStarted.Add(progressWindow)
	convergenceDeadline := progressStarted.Add(10 * time.Second)
	convergedInWindow := false
	var progressErrors []error
	nextQueueSample := progressStarted
	for time.Now().Before(progressDeadline) {
		probeStarted := time.Now()
		probe := scenario.mqtt(ctx, leaderID, scenario.nextProbeID(), "chaos/probe/#")
		scenario.evidence.ProgressAttempts++
		if probe.Classification == "accepted" {
			scenario.evidence.ProgressAccepted++
		}
		latency := time.Since(probeStarted).Milliseconds()
		if latency > scenario.evidence.MaxLeaderProbeLatencyMs {
			scenario.evidence.MaxLeaderProbeLatencyMs = latency
		}
		if probe.Classification != "accepted" {
			progressErrors = append(progressErrors, fmt.Errorf("leader progress MQTT probe was %s", probe.Classification))
		}
		if !time.Now().Before(nextQueueSample) {
			targetSummary, queueErr := scenario.sampleQueue(ctx, leaderID, targetID)
			if queueErr != nil {
				progressErrors = append(progressErrors, queueErr)
			} else if targetSummary.ReplicationHealth == "UNREACHABLE" {
				scenario.evidence.LeaderUnreachableTarget = true
			}
			nextQueueSample = time.Now().Add(queueSampleInterval)
		}
		leader, leaderErr := scenario.node(ctx, leaderID, leaderID)
		healthy, healthyErr := scenario.node(ctx, healthyID, healthyID)
		if leaderErr != nil {
			progressErrors = append(progressErrors, leaderErr)
		} else if healthyErr != nil {
			progressErrors = append(progressErrors, healthyErr)
		} else {
			scenario.evidence.LeaderAppliedIndexEnd = leader.LastAppliedIndex
			scenario.evidence.FollowerAppliedIndexEnd = healthy.LastAppliedIndex
			if time.Now().Before(convergenceDeadline) &&
				leader.LastAppliedIndex > leaderStart.LastAppliedIndex &&
				healthy.LastAppliedIndex > healthyStart.LastAppliedIndex &&
				healthy.LastAppliedIndex >= leader.LastAppliedIndex {
				convergedInWindow = true
			}
		}
		if err := sleepContext(ctx, progressProbeInterval); err != nil {
			progressErrors = append(progressErrors, err)
			break
		}
	}
	if scenario.evidence.ProgressAttempts == 0 || scenario.evidence.ProgressAccepted != scenario.evidence.ProgressAttempts {
		progressErrors = append(progressErrors, fmt.Errorf("leader accepted %d of %d MQTT progress probes", scenario.evidence.ProgressAccepted, scenario.evidence.ProgressAttempts))
	}
	if scenario.evidence.MaxLeaderProbeLatencyMs >= 5000 {
		progressErrors = append(progressErrors, fmt.Errorf("leader progress probe latency reached %dms, want <5000ms", scenario.evidence.MaxLeaderProbeLatencyMs))
	}
	if scenario.evidence.QueueSamples == 0 {
		progressErrors = append(progressErrors, errors.New("no replication queue samples were collected"))
	}
	if requireQueueCap && !scenario.evidence.QueueCapReached {
		progressErrors = append(progressErrors, errors.New("replication queue cap was not observed"))
	}
	if requireQueueCap && !scenario.evidence.LeaderUnreachableTarget {
		progressErrors = append(progressErrors, fmt.Errorf("leader did not observe follower %s as UNREACHABLE", targetID))
	}
	if requireAppliedConvergence && !convergedInWindow {
		progressErrors = append(progressErrors, errors.New("leader and healthy follower applied indexes did not increase and converge within 10s"))
	}
	return errors.Join(progressErrors...)
}

func (scenario *storageFaultScenario) recoverFollower(ctx context.Context, leaderID, targetID string, timeout time.Duration, requireCleanError bool) error {
	leader, err := scenario.node(ctx, leaderID, leaderID)
	if err != nil {
		return err
	}
	freeOutput, err := scenario.docker.execNode(ctx, targetID, "free-disk", "--path", "/var/lib/narumq/chaos-ballast.bin")
	if err != nil {
		return err
	}
	if err := scenario.recorder.record("disk", targetID, map[string]any{"operation": "free-disk", "output": strings.TrimSpace(string(freeOutput))}); err != nil {
		return err
	}
	if err := scenario.docker.signalBrokerRestart(ctx, targetID); err != nil {
		return err
	}
	return scenario.awaitFollowerRecovery(ctx, targetID, leader.LastAppliedIndex, timeout, requireCleanError)
}

func (scenario *storageFaultScenario) awaitFollowerRecovery(ctx context.Context, targetID string, leaderIndex int64, timeout time.Duration, requireCleanError bool) error {
	recoveryStarted := time.Now()
	var latest nodeData
	if err := waitFor(ctx, timeout, probeInterval, func() (bool, error) {
		ready, err := scenario.health(ctx, targetID, "/health/ready")
		if err != nil {
			return false, err
		}
		if ready.HTTPStatus != http.StatusOK {
			return false, fmt.Errorf("node %s recovery readiness returned HTTP %d", targetID, ready.HTTPStatus)
		}
		latest, err = scenario.node(ctx, targetID, targetID)
		if err != nil {
			return false, err
		}
		return latest.ReplicationHealth == "HEALTHY" && latest.LastAppliedIndex >= leaderIndex, nil
	}); err != nil {
		return fmt.Errorf("node %s did not recover: %w", targetID, err)
	}
	scenario.evidence.RecoveryDurationMs = time.Since(recoveryStarted).Milliseconds()
	if latest.LastAppliedIndex < leaderIndex {
		return fmt.Errorf("recovered node %s applied index %d is behind leader recovery index %d", targetID, latest.LastAppliedIndex, leaderIndex)
	}
	if latest.StoragePressure || !latest.Ready {
		return fmt.Errorf("recovered node %s still reports storage pressure or not-ready", targetID)
	}
	if requireCleanError && latest.LastErrorCode != "" {
		return fmt.Errorf("recovered node %s retained terminal storage error %q", targetID, latest.LastErrorCode)
	}
	if err := scenario.waitForBaselineSubscription(ctx, targetID, 30*time.Second); err != nil {
		return fmt.Errorf("recovered node %s baseline subscription: %w", targetID, err)
	}
	if err := waitFor(ctx, 30*time.Second, probeInterval, func() (bool, error) {
		probe, ready, err := scenario.mqttWithReadiness(ctx, targetID, recoveryClientID(targetID), "chaos/probe/#")
		if err != nil {
			return false, err
		}
		return ready && probe.Classification == "accepted", nil
	}); err != nil {
		return fmt.Errorf("recovered node %s recovery MQTT admission: %w", targetID, err)
	}
	if err := scenario.reconnectOriginalBaseline(ctx, targetID); err != nil {
		return err
	}
	scenario.evidence.ConvergedAppliedIndex = latest.LastAppliedIndex
	return nil
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-027, cluster-session-and-delivery.md:CLU-SCN-084, cluster-session-and-delivery.md:CLU-SCN-161
func (scenario *storageFaultScenario) runQuorumDiskFull(ctx context.Context, leaderID string, followers []string) error {
	scenario.evidence.TargetNodeIDs = append([]string(nil), followers...)
	preFaultQoS1 := durableTopic(scenario.options.ScenarioID, "leader", "before-qos1")
	preFaultQoS2 := durableTopic(scenario.options.ScenarioID, "leader", "before-qos2")
	if err := scenario.durablePublish(ctx, leaderID, "chaos-durable-pub-3", preFaultQoS1, 1, "before-fault", true); err != nil {
		return err
	}
	if err := scenario.durablePublish(ctx, leaderID, "chaos-durable-pub-3", preFaultQoS2, 2, "before-fault", true); err != nil {
		return err
	}
	leaderAddress := "127.0.0.1:" + scenario.docker.mqttPorts[leaderID]
	cleanHaltClient, _, err := dialDurableClient(ctx, leaderAddress, scenario.fixture.AccessToken, "chaos-durable-halt-1", true)
	if err != nil {
		return err
	}
	defer cleanHaltClient.close()
	persistentHaltClient, _, err := dialDurableClient(ctx, leaderAddress, scenario.fixture.AccessToken, "chaos-durable-halt-2", false)
	if err != nil {
		return err
	}
	defer persistentHaltClient.close()
	for _, nodeID := range followers {
		if _, err := scenario.fillDisk(ctx, nodeID); err != nil {
			return err
		}
	}
	detectionStarted := time.Now()
	failed := make(map[string]bool, len(followers))
	if err := waitFor(ctx, startupTimeout, 500*time.Millisecond, func() (bool, error) {
		scenario.mqtt(ctx, leaderID, scenario.nextProbeID(), "chaos/probe/#")
		allFailed := true
		for _, nodeID := range followers {
			summary, err := scenario.node(ctx, nodeID, nodeID)
			if err != nil {
				allFailed = false
				continue
			}
			if summary.ReplicationHealth == "FAILED" {
				failed[nodeID] = true
			} else {
				allFailed = false
			}
			leaderView, err := scenario.node(ctx, leaderID, nodeID)
			if err != nil {
				allFailed = false
				continue
			}
			if leaderView.ReplicationHealth != "UNREACHABLE" {
				allFailed = false
			}
		}
		return allFailed && len(failed) == len(followers), nil
	}); err != nil {
		return fmt.Errorf("both follower disks did not fail and disconnect: %w", err)
	}
	scenario.evidence.DetectionDurationMs = time.Since(detectionStarted).Milliseconds()
	for _, nodeID := range followers {
		if err := scenario.inspectFailedFollower(ctx, nodeID, "storage_no_space", "raft storage failed: no space left; restart required"); err != nil {
			return err
		}
	}
	leaderStart, err := scenario.node(ctx, leaderID, leaderID)
	if err != nil {
		return err
	}
	scenario.evidence.LeaderAppliedIndexStart = leaderStart.LastAppliedIndex
	cluster, clusterStatus, _, err := scenario.cluster(ctx, leaderID)
	if err != nil {
		return err
	}
	if clusterStatus != http.StatusOK {
		return fmt.Errorf("leader cluster API returned HTTP %d during quorum loss", clusterStatus)
	}
	if cluster.QuorumAvailable {
		return errors.New("leader still reports quorum available after both followers failed")
	}
	quorum := cluster.QuorumAvailable
	scenario.evidence.QuorumAvailableDuring = &quorum
	ready, err := scenario.health(ctx, leaderID, "/health/ready")
	if err != nil {
		return err
	}
	if ready.HTTPStatus != http.StatusServiceUnavailable {
		return fmt.Errorf("leader readiness during quorum loss returned HTTP %d, want 503", ready.HTTPStatus)
	}
	scenario.evidence.LeaderReadiness = &ready
	duringFaultQoS1 := durableTopic(scenario.options.ScenarioID, "quorum", "unacked-qos1")
	duringFaultQoS2 := durableTopic(scenario.options.ScenarioID, "quorum", "unacked-qos2")
	qos2State, err := scenario.publishDuringQuorumFault(ctx, leaderID, cleanHaltClient, persistentHaltClient, duringFaultQoS1, duringFaultQoS2)
	if err != nil {
		return err
	}
	if len(scenario.evidence.DurableAck.UnackedDuringFault) != 2 || len(scenario.evidence.DurableAck.AckedDuringFault) != 0 {
		return fmt.Errorf("quorum durable ACK outcomes: acked=%v unacked=%v, want 0 ACKed and 2 unacked", scenario.evidence.DurableAck.AckedDuringFault, scenario.evidence.DurableAck.UnackedDuringFault)
	}
	if err := cleanHaltClient.close(); err != nil {
		return fmt.Errorf("close clean durable publisher before recovery: %w", err)
	}
	if err := persistentHaltClient.close(); err != nil {
		return fmt.Errorf("close persistent durable publisher before recovery: %w", err)
	}

	haltStarted := time.Now()
	for time.Since(haltStarted) < quorumHaltWindow {
		probe := scenario.mqtt(ctx, leaderID, scenario.nextProbeID(), "chaos/probe/#")
		scenario.evidence.HaltProbeAttempts++
		if probe.Classification == "accepted" {
			scenario.evidence.HaltAccepted++
		}
		for _, nodeID := range followers {
			if _, err := scenario.sampleQueue(ctx, leaderID, nodeID); err != nil {
				return err
			}
		}
		leader, err := scenario.node(ctx, leaderID, leaderID)
		if err != nil {
			return err
		}
		scenario.evidence.AppliedIndexEnd = leader.LastAppliedIndex
		if leader.LastAppliedIndex != scenario.evidence.LeaderAppliedIndexStart {
			return fmt.Errorf("leader applied index changed during quorum halt: %d to %d", scenario.evidence.LeaderAppliedIndexStart, leader.LastAppliedIndex)
		}
		if err := sleepContext(ctx, progressProbeInterval); err != nil {
			return err
		}
	}
	scenario.evidence.HaltDurationMs = time.Since(haltStarted).Milliseconds()
	if scenario.evidence.HaltDurationMs < quorumHaltWindow.Milliseconds() {
		return fmt.Errorf("quorum halt lasted %dms, want at least %dms", scenario.evidence.HaltDurationMs, quorumHaltWindow.Milliseconds())
	}
	if scenario.evidence.HaltProbeAttempts < 3 {
		return fmt.Errorf("quorum halt had %d MQTT attempts, want at least 3", scenario.evidence.HaltProbeAttempts)
	}
	if scenario.evidence.HaltAccepted != 0 {
		return fmt.Errorf("leader accepted %d MQTT probes without quorum", scenario.evidence.HaltAccepted)
	}
	if scenario.evidence.QueueSamples == 0 {
		return errors.New("no quorum-halt replication queue samples were collected")
	}
	ready, err = scenario.health(ctx, leaderID, "/health/ready")
	if err != nil {
		return err
	}
	if ready.HTTPStatus != http.StatusServiceUnavailable {
		return fmt.Errorf("leader readiness after halt returned HTTP %d, want 503", ready.HTTPStatus)
	}
	scenario.evidence.LeaderReadiness = &ready
	cluster, clusterStatus, _, err = scenario.cluster(ctx, leaderID)
	if err != nil {
		return err
	}
	if clusterStatus != http.StatusOK || cluster.QuorumAvailable {
		return fmt.Errorf("leader quorum after halt = %t with HTTP %d, want false and HTTP 200", cluster.QuorumAvailable, clusterStatus)
	}
	quorum = cluster.QuorumAvailable
	scenario.evidence.QuorumAvailableDuring = &quorum
	recoveryStarted := time.Now()
	for _, nodeID := range followers {
		freeOutput, err := scenario.docker.execNode(ctx, nodeID, "free-disk", "--path", "/var/lib/narumq/chaos-ballast.bin")
		if err != nil {
			return err
		}
		if err := scenario.recorder.record("disk", nodeID, map[string]any{"operation": "free-disk", "output": strings.TrimSpace(string(freeOutput))}); err != nil {
			return err
		}
		if err := scenario.docker.signalBrokerRestart(ctx, nodeID); err != nil {
			return err
		}
	}
	if err := waitFor(ctx, recoveryTimeout, probeInterval, func() (bool, error) {
		for _, nodeID := range nodeIDs() {
			ready, err := scenario.health(ctx, nodeID, "/health/ready")
			if err != nil || ready.HTTPStatus != http.StatusOK {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("all nodes did not become ready after quorum recovery: %w", err)
	}
	recoveredCluster, _, _, err := scenario.cluster(ctx, leaderID)
	if err != nil {
		return err
	}
	if !recoveredCluster.QuorumAvailable {
		return errors.New("leader did not restore quorum after follower recovery")
	}
	indexTimeout := 30*time.Second - time.Since(recoveryStarted)
	if indexTimeout <= 0 {
		return errors.New("applied indexes did not converge within 30s of follower recovery")
	}
	convergedIndex, err := scenario.waitForIndexConvergence(ctx, scenario.evidence.LeaderAppliedIndexStart, indexTimeout)
	if err != nil {
		return fmt.Errorf("applied indexes did not converge after quorum recovery: %w", err)
	}
	for _, nodeID := range nodeIDs() {
		if err := waitFor(ctx, 30*time.Second, probeInterval, func() (bool, error) {
			probe, ready, err := scenario.mqttWithReadiness(ctx, nodeID, recoveryClientID(nodeID), "chaos/probe/#")
			if err != nil {
				return false, err
			}
			return ready && probe.Classification == "accepted", nil
		}); err != nil {
			return fmt.Errorf("node %s did not accept MQTT after quorum recovery: %w", nodeID, err)
		}
		if err := scenario.waitForBaselineSubscription(ctx, nodeID, 30*time.Second); err != nil {
			return fmt.Errorf("node %s baseline subscription after recovery: %w", nodeID, err)
		}
	}
	if err := scenario.reconnectOriginalBaseline(ctx, leaderID); err != nil {
		return err
	}
	recoveryClient, sessionPresent, err := dialDurableClient(ctx, leaderAddress, scenario.fixture.AccessToken, "chaos-durable-halt-2", false)
	if err != nil {
		return err
	}
	defer recoveryClient.close()
	sessionPresentEvidence := sessionPresent
	scenario.evidence.DurableAck.RecoverySessionPresent = &sessionPresentEvidence
	if !sessionPresent {
		return errors.New("persistent QoS 2 publisher session was not present after recovery")
	}
	recoveryAckCtx, cancelRecoveryAck := context.WithTimeout(ctx, durableAckTimeout)
	recoveryOutcome, recoveryAckErr := recoveryClient.resumeQoS2(recoveryAckCtx, qos2State.packetID, qos2State.topic, []byte(qos2State.topic), qos2State.pubrecReceived)
	cancelRecoveryAck()
	if err := scenario.recordDurableAck(leaderID, "chaos-durable-halt-2", qos2State.topic, 2, "after-recovery", recoveryOutcome, recoveryAckErr); err != nil {
		return err
	}
	if recoveryOutcome != durableAcked {
		return fmt.Errorf("replayed QoS 2 publish %s outcome %q, want ACK: %v", qos2State.topic, recoveryOutcome, recoveryAckErr)
	}
	if err := scenario.verifyDurableDelivery(ctx, leaderID,
		[]string{preFaultQoS1, preFaultQoS2, duringFaultQoS2},
		[]string{duringFaultQoS1}); err != nil {
		return err
	}
	scenario.evidence.RecoveryDurationMs = time.Since(recoveryStarted).Milliseconds()
	scenario.evidence.ConvergedAppliedIndex = convergedIndex
	return nil
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-028, cluster-session-and-delivery.md:CLU-SCN-084
func (scenario *storageFaultScenario) runFollowerSlowIO(ctx context.Context, leaderID, targetID, healthyID string) error {
	scenario.evidence.TargetNodeIDs = []string{targetID}
	if _, err := scenario.docker.execNode(ctx, targetID, "set-io-delay", "--file", "/run/narumq-chaos/io-delay", "--ms", "2000"); err != nil {
		return err
	}
	probeOutput, err := scenario.docker.execNode(ctx, targetID, "probe-io", "--path", "/var/lib/narumq/chaos-io-probe.bin")
	if err != nil {
		return err
	}
	var ioProbe struct {
		Bytes      int   `json:"bytes"`
		DurationMs int64 `json:"durationMs"`
	}
	if err := json.Unmarshal(probeOutput, &ioProbe); err != nil {
		return fmt.Errorf("decode slow-I/O probe result: %w", err)
	}
	scenario.evidence.IOProbeDurationMs = ioProbe.DurationMs
	if ioProbe.Bytes != 4096 || ioProbe.DurationMs < 2000 {
		return fmt.Errorf("slow-I/O probe measured %dms for %d bytes, want 4096 bytes and >=2000ms", ioProbe.DurationMs, ioProbe.Bytes)
	}

	detectedAt := time.Now()
	var degraded nodeData
	err = waitFor(ctx, slowDetectTimeout, progressProbeInterval, func() (bool, error) {
		probe := scenario.mqtt(ctx, leaderID, scenario.nextProbeID(), "chaos/probe/#")
		if probe.Classification != "accepted" {
			return false, fmt.Errorf("leader MQTT progress probe was %s during slow-I/O detection", probe.Classification)
		}
		var err error
		degraded, err = scenario.node(ctx, targetID, targetID)
		if err != nil {
			return false, err
		}
		return degraded.ReplicationHealth == "DEGRADED", nil
	})
	if err != nil {
		return fmt.Errorf("target follower did not report DEGRADED: %w", err)
	}
	scenario.evidence.DetectionDurationMs = time.Since(detectedAt).Milliseconds()
	ready, err := scenario.health(ctx, targetID, "/health/ready")
	if err != nil {
		return err
	}
	if ready.HTTPStatus != http.StatusServiceUnavailable || ready.ErrorMessage != "raft storage I/O is slow" {
		return fmt.Errorf("slow-I/O follower readiness = HTTP %d %q, want 503 %q", ready.HTTPStatus, ready.ErrorMessage, "raft storage I/O is slow")
	}
	live, err := scenario.health(ctx, targetID, "/health/live")
	if err != nil {
		return err
	}
	if live.HTTPStatus != http.StatusOK {
		return fmt.Errorf("slow-I/O follower liveness returned HTTP %d, want 200", live.HTTPStatus)
	}
	if degraded.LastErrorCode != "storage_slow" || !degraded.StoragePressure || degraded.Ready {
		return fmt.Errorf("degraded follower summary mismatch: ready=%t code=%q pressure=%t", degraded.Ready, degraded.LastErrorCode, degraded.StoragePressure)
	}
	if degraded.ReadinessReason == nil || *degraded.ReadinessReason != "raft storage I/O is slow" {
		return fmt.Errorf("degraded follower readiness reason = %v, want %q", degraded.ReadinessReason, "raft storage I/O is slow")
	}
	scenario.evidence.TargetReadiness = &ready
	scenario.evidence.TargetLiveness = &live
	scenario.evidence.TargetReplicationHealth = degraded.ReplicationHealth
	scenario.evidence.TargetLastErrorCode = degraded.LastErrorCode
	pressure := degraded.StoragePressure
	scenario.evidence.TargetStoragePressure = &pressure
	scenario.evidence.UptimeBeforeRecoveryMs = degraded.UptimeMillis
	for attempt := 0; attempt < 3; attempt++ {
		probe := scenario.mqtt(ctx, targetID, scenario.nextProbeID(), "chaos/probe/#")
		if probe.Classification == "accepted" {
			return fmt.Errorf("degraded follower %s accepted MQTT", targetID)
		}
	}
	expectedDurableTopics, err := scenario.publishFollowerFaultMessages(ctx, leaderID, healthyID)
	if err != nil {
		return err
	}
	if err := scenario.observeHealthyProgress(ctx, leaderID, targetID, healthyID, false, false); err != nil {
		return err
	}
	recoveryStarted := time.Now()
	if _, err := scenario.docker.execNode(ctx, targetID, "set-io-delay", "--file", "/run/narumq-chaos/io-delay", "--ms", "0"); err != nil {
		return err
	}
	recoveryTimeout := slowDetectTimeout - time.Since(recoveryStarted)
	if recoveryTimeout <= 0 {
		return errors.New("slow-I/O follower recovery exceeded 60s")
	}
	var recovered nodeData
	if err := waitFor(ctx, recoveryTimeout, probeInterval, func() (bool, error) {
		ready, err := scenario.health(ctx, targetID, "/health/ready")
		if err != nil {
			return false, err
		}
		if ready.HTTPStatus != http.StatusOK {
			return false, fmt.Errorf("slow-I/O follower readiness returned HTTP %d", ready.HTTPStatus)
		}
		recovered, err = scenario.node(ctx, targetID, targetID)
		if err != nil {
			return false, err
		}
		return recovered.ReplicationHealth == "HEALTHY" && !recovered.StoragePressure, nil
	}); err != nil {
		return fmt.Errorf("slow-I/O follower did not recover: %w", err)
	}
	scenario.evidence.RecoveryDurationMs = time.Since(recoveryStarted).Milliseconds()
	if recovered.LastErrorCode != "storage_slow" {
		return fmt.Errorf("recovered slow-I/O follower lastErrorCode = %q, want storage_slow", recovered.LastErrorCode)
	}
	if !recovered.Ready {
		return errors.New("recovered slow-I/O follower remains not-ready")
	}
	leader, err := scenario.node(ctx, leaderID, leaderID)
	if err != nil {
		return err
	}
	indexTimeout := 30*time.Second - time.Since(recoveryStarted)
	if indexTimeout <= 0 {
		return errors.New("slow-I/O follower did not catch up within 30s of I/O recovery")
	}
	if err := waitFor(ctx, indexTimeout, probeInterval, func() (bool, error) {
		var err error
		recovered, err = scenario.node(ctx, targetID, targetID)
		if err != nil {
			return false, err
		}
		leader, err = scenario.node(ctx, leaderID, leaderID)
		if err != nil {
			return false, err
		}
		return recovered.LastAppliedIndex >= leader.LastAppliedIndex, nil
	}); err != nil {
		return fmt.Errorf("slow-I/O follower did not catch up within 30s: %w", err)
	}
	if recovered.UptimeMillis <= scenario.evidence.UptimeBeforeRecoveryMs {
		return fmt.Errorf("slow-I/O follower uptime did not increase: %d -> %d", scenario.evidence.UptimeBeforeRecoveryMs, recovered.UptimeMillis)
	}
	scenario.evidence.UptimeAfterRecoveryMs = recovered.UptimeMillis
	scenario.evidence.ConvergedAppliedIndex = recovered.LastAppliedIndex
	if err := waitFor(ctx, 30*time.Second, probeInterval, func() (bool, error) {
		probe, ready, err := scenario.mqttWithReadiness(ctx, targetID, recoveryClientID(targetID), "chaos/probe/#")
		if err != nil {
			return false, err
		}
		return ready && probe.Classification == "accepted", nil
	}); err != nil {
		return fmt.Errorf("slow-I/O follower recovery MQTT admission: %w", err)
	}
	if err := scenario.waitForBaselineSubscription(ctx, targetID, 30*time.Second); err != nil {
		return fmt.Errorf("slow-I/O follower baseline subscription after recovery: %w", err)
	}
	if err := scenario.reconnectOriginalBaseline(ctx, targetID); err != nil {
		return err
	}
	if err := scenario.verifyDurableDelivery(ctx, leaderID, expectedDurableTopics, nil); err != nil {
		return err
	}
	logs, err := scenario.docker.nodeLogs(ctx, targetID)
	if err != nil {
		return err
	}
	if strings.Contains(string(logs), `"event":"broker-restart"`) || strings.Contains(string(logs), `"event":"broker-exit"`) {
		return errors.New("slow-I/O node log contains broker-restart or broker-exit event")
	}
	return nil
}

func (scenario *storageFaultScenario) waitForIndexConvergence(ctx context.Context, minimum int64, timeout time.Duration) (int64, error) {
	var converged int64
	err := waitFor(ctx, timeout, probeInterval, func() (bool, error) {
		indexes := make([]int64, 0, chaosNodeCount)
		for _, nodeID := range nodeIDs() {
			observation, err := scenario.node(ctx, nodeID, nodeID)
			if err != nil {
				return false, err
			}
			if observation.ReplicationHealth != "HEALTHY" || observation.LastAppliedIndex <= minimum {
				return false, fmt.Errorf("node %s has health %s and applied index %d; want HEALTHY and >%d", nodeID, observation.ReplicationHealth, observation.LastAppliedIndex, minimum)
			}
			indexes = append(indexes, observation.LastAppliedIndex)
		}
		sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
		if indexes[0] != indexes[len(indexes)-1] {
			return false, fmt.Errorf("applied indexes have not converged: %v", indexes)
		}
		converged = indexes[0]
		return true, nil
	})
	return converged, err
}

func (scenario *storageFaultScenario) waitForBaselineSubscription(ctx context.Context, nodeID string, timeout time.Duration) error {
	return waitFor(ctx, timeout, probeInterval, func() (bool, error) {
		response, status, _, err := scenario.subscriptions(ctx, nodeID)
		if err != nil {
			return false, err
		}
		if status != http.StatusOK || !hasSubscription(response, "chaos-baseline", "chaos/baseline/#") {
			return false, fmt.Errorf("node %s does not list the baseline subscription", nodeID)
		}
		return true, nil
	})
}

func (scenario *storageFaultScenario) reconnectOriginalBaseline(ctx context.Context, ownerID string) error {
	err := waitFor(ctx, 30*time.Second, probeInterval, func() (bool, error) {
		probe, ready, err := scenario.mqttWithReadiness(ctx, ownerID, "chaos-baseline", "chaos/baseline/#")
		if err != nil {
			return false, err
		}
		return ready && probe.Classification == "accepted", nil
	})
	if err != nil {
		return fmt.Errorf("original baseline client did not reconnect on node %s: %w", ownerID, err)
	}
	scenario.evidence.OriginalBaselineReconnectAccepted = true
	return nil
}

func hasSubscription(response subscriptionEnvelope, clientID, topicFilter string) bool {
	for _, subscription := range response.Data {
		if subscription.TenantID == "chaos" && subscription.ClientID == clientID && subscription.TopicFilter == topicFilter {
			return true
		}
	}
	return false
}

func followerNodeIDs(leaderID string) []string {
	followers := make([]string, 0, chaosNodeCount-1)
	for _, nodeID := range nodeIDs() {
		if nodeID != leaderID {
			followers = append(followers, nodeID)
		}
	}
	return followers
}

func nodeIDs() []string {
	return []string{"1", "2", "3"}
}

func containsNodeID(nodeIDs []string, target string) bool {
	for _, nodeID := range nodeIDs {
		if nodeID == target {
			return true
		}
	}
	return false
}

func waitFor(ctx context.Context, timeout, interval time.Duration, check func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := check()
		if done && err == nil {
			return nil
		}
		if err != nil {
			lastErr = err
		}
		if !time.Now().Before(deadline) {
			if lastErr != nil {
				return fmt.Errorf("condition was not met within %s: %w", timeout, lastErr)
			}
			return fmt.Errorf("condition was not met within %s", timeout)
		}
		if err := sleepContext(ctx, interval); err != nil {
			return err
		}
	}
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (scenario *scenarioDocker) nodeLogs(ctx context.Context, nodeID string) ([]byte, error) {
	name, ok := scenario.containerNames[nodeID]
	if !ok {
		return nil, fmt.Errorf("container for node %s is unavailable", nodeID)
	}
	logs, err := scenario.options.Runner.Run(ctx, scenario.options.RepoRoot, "docker", "logs", name)
	if err != nil {
		return logs, fmt.Errorf("collect node %s logs: %w", nodeID, err)
	}
	logPath := filepath.Join(scenario.options.OutputDir, "scenarios", scenario.options.ScenarioID, "node"+nodeID+".log")
	if err := os.WriteFile(logPath, logs, 0o600); err != nil {
		return logs, fmt.Errorf("write node %s logs: %w", nodeID, err)
	}
	if err := scenario.recorder.record("control", nodeID, map[string]any{"operation": "logs-sampled", "bytes": len(logs)}); err != nil {
		return logs, err
	}
	return logs, nil
}

func joinErrorMessage(message string, err error) string {
	if err == nil {
		return message
	}
	if message == "" {
		return err.Error()
	}
	return errors.Join(errors.New(message), err).Error()
}
