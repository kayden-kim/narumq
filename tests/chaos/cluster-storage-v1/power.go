package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

type storageControlStatus struct {
	Mode          string   `json:"mode"`
	HeldWalFsyncs int      `json:"heldWalFsyncs"`
	HeldPaths     []string `json:"heldPaths"`
	PoweredOff    bool     `json:"poweredOff"`
}

func parsePowerCut(output []byte) (powerCutEvidence, error) {
	var evidence powerCutEvidence
	if err := json.Unmarshal(output, &evidence); err != nil {
		return powerCutEvidence{}, fmt.Errorf("decode power-cut response: %w", err)
	}
	return evidence, nil
}

func validateTornWALCut(evidence powerCutEvidence) error {
	if !evidence.Torn {
		return errors.New("power cut did not report torn=true")
	}
	if evidence.BrokerExitCode != 137 {
		return fmt.Errorf("power-cut broker exit code = %d, want 137", evidence.BrokerExitCode)
	}
	if evidence.ReleasedFsyncs < 1 {
		return fmt.Errorf("power cut released %d held WAL fsyncs, want at least one", evidence.ReleasedFsyncs)
	}
	for _, file := range evidence.Files {
		if filepath.Clean(filepath.FromSlash(file.Path)) != filepath.Join("node", "wal.bin") {
			continue
		}
		if file.DurableBytes <= 0 {
			return fmt.Errorf("rolled-back WAL durable bytes = %d, want >0", file.DurableBytes)
		}
		if file.UnsyncedBytes <= 0 {
			return fmt.Errorf("rolled-back WAL unsynced bytes = %d, want >0", file.UnsyncedBytes)
		}
		if file.KeptBytes <= 0 || file.KeptBytes >= file.UnsyncedBytes {
			return fmt.Errorf("rolled-back WAL kept bytes = %d of %d unsynced bytes, want a non-empty strict prefix", file.KeptBytes, file.UnsyncedBytes)
		}
		return nil
	}
	return errors.New("power-cut rollback did not report a wal.bin file")
}

func (scenario *storageFaultScenario) setStorageMode(ctx context.Context, nodeID, mode string) error {
	output, err := scenario.docker.chaosControl(ctx, nodeID, "storage-mode", "--mode", mode)
	if err != nil {
		return err
	}
	var status storageControlStatus
	if err := json.Unmarshal(output, &status); err != nil {
		return fmt.Errorf("decode node %s storage-mode response: %w", nodeID, err)
	}
	if status.Mode != mode {
		return fmt.Errorf("node %s storage mode = %q, want %q", nodeID, status.Mode, mode)
	}
	return scenario.recorder.record("storage-mode", nodeID, status)
}

func (scenario *storageFaultScenario) storageStatus(ctx context.Context, nodeID string) (storageControlStatus, error) {
	output, err := scenario.docker.chaosControl(ctx, nodeID, "storage-status")
	if err != nil {
		return storageControlStatus{}, err
	}
	var status storageControlStatus
	if err := json.Unmarshal(output, &status); err != nil {
		return storageControlStatus{}, fmt.Errorf("decode node %s storage-status response: %w", nodeID, err)
	}
	return status, nil
}

func (scenario *storageFaultScenario) waitForHeldWALFsync(ctx context.Context, nodeID string, timeout time.Duration) (storageControlStatus, error) {
	var held storageControlStatus
	err := waitFor(ctx, timeout, 50*time.Millisecond, func() (bool, error) {
		status, err := scenario.storageStatus(ctx, nodeID)
		if err != nil {
			return false, err
		}
		for _, path := range status.HeldPaths {
			if filepath.Base(filepath.FromSlash(path)) == "wal.bin" {
				held = status
				return status.HeldWalFsyncs > 0, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return storageControlStatus{}, fmt.Errorf("node %s did not hold a WAL fsync: %w", nodeID, err)
	}
	if err := scenario.recorder.record("storage-status", nodeID, held); err != nil {
		return storageControlStatus{}, err
	}
	return held, nil
}

func (scenario *storageFaultScenario) powerCut(ctx context.Context, nodeID string, torn bool) (powerCutEvidence, error) {
	arguments := []string(nil)
	if torn {
		arguments = append(arguments, "--torn")
	}
	output, err := scenario.docker.chaosControl(ctx, nodeID, "power-cut", arguments...)
	if err != nil {
		return powerCutEvidence{}, err
	}
	evidence, err := parsePowerCut(output)
	if err != nil {
		return powerCutEvidence{}, err
	}
	if err := scenario.recorder.record("power-cut", nodeID, evidence); err != nil {
		return powerCutEvidence{}, err
	}
	return evidence, nil
}

func (scenario *storageFaultScenario) powerOn(ctx context.Context, nodeID string) error {
	output, err := scenario.docker.chaosControl(ctx, nodeID, "power-on")
	if err != nil {
		return err
	}
	var status struct {
		PoweredOff bool `json:"poweredOff"`
	}
	if err := json.Unmarshal(output, &status); err != nil {
		return fmt.Errorf("decode node %s power-on response: %w", nodeID, err)
	}
	if status.PoweredOff {
		return fmt.Errorf("node %s remained powered off after power-on", nodeID)
	}
	return scenario.recorder.record("power-on", nodeID, status)
}

func supervisorBrokerExitCodes(logs []byte) []int {
	var codes []int
	for _, line := range strings.Split(string(logs), "\n") {
		var event struct {
			Event    string `json:"event"`
			ExitCode int    `json:"exitCode"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Event == "broker-exit" {
			codes = append(codes, event.ExitCode)
		}
	}
	return codes
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-029, cluster-session-and-delivery.md:CLU-SCN-084
func (scenario *storageFaultScenario) runFollowerSyncError(ctx context.Context, leaderID, targetID, healthyID string) error {
	scenario.evidence.TargetNodeIDs = []string{targetID}
	faultModeReset := false
	defer func() {
		if faultModeReset {
			return
		}
		resetCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := scenario.setStorageMode(resetCtx, targetID, "normal"); err != nil {
			_ = scenario.recorder.record("storage-mode-reset-error", targetID, map[string]string{"error": err.Error()})
		}
	}()
	if err := scenario.setStorageMode(ctx, targetID, "fail-fsync"); err != nil {
		return err
	}
	detectedAt := time.Now()
	if err := waitFor(ctx, followerDetectTimeout, progressProbeInterval, func() (bool, error) {
		probe := scenario.mqtt(ctx, leaderID, scenario.nextProbeID(), "chaos/probe/#")
		if probe.Classification != "accepted" {
			return false, fmt.Errorf("leader MQTT progress probe was %s during follower sync-error detection", probe.Classification)
		}
		target, err := scenario.node(ctx, targetID, targetID)
		if err != nil {
			return false, err
		}
		return target.ReplicationHealth == "FAILED", nil
	}); err != nil {
		return fmt.Errorf("target follower did not report FAILED after fsync error: %w", err)
	}
	scenario.evidence.DetectionDurationMs = time.Since(detectedAt).Milliseconds()
	if err := scenario.inspectFailedFollower(ctx, targetID, "storage_io_error", "raft storage failed: I/O error; restart required"); err != nil {
		return err
	}
	leaderReady, err := scenario.health(ctx, leaderID, "/health/ready")
	if err != nil {
		return err
	}
	if leaderReady.HTTPStatus != 200 {
		return fmt.Errorf("leader readiness during follower sync error returned HTTP %d, want 200", leaderReady.HTTPStatus)
	}
	scenario.evidence.LeaderReadiness = &leaderReady
	for attempt := 0; attempt < 3; attempt++ {
		probe := scenario.mqtt(ctx, targetID, scenario.nextProbeID(), "chaos/probe/#")
		if probe.Classification == "accepted" {
			return fmt.Errorf("failed follower %s accepted an MQTT probe", targetID)
		}
	}
	expectedTopics, err := scenario.publishFollowerFaultMessages(ctx, leaderID, healthyID)
	if err != nil {
		return err
	}
	if err := scenario.observeHealthyProgress(ctx, leaderID, targetID, healthyID, true, true); err != nil {
		return err
	}
	logsBeforeRestart, err := scenario.docker.nodeLogs(ctx, targetID)
	if err != nil {
		return err
	}
	previousExitCount := len(supervisorBrokerExitCodes(logsBeforeRestart))
	if err := scenario.docker.signalBrokerRestart(ctx, targetID); err != nil {
		return err
	}
	reopenTopic := durableTopic(scenario.options.ScenarioID, "leader", "reopen-trigger-qos1")
	if err := scenario.durablePublish(ctx, leaderID, "chaos-durable-pub-3", reopenTopic, 1, "during-fault", true); err != nil {
		return fmt.Errorf("publish recovery probe while fail-fsync remains active: %w", err)
	}
	expectedTopics = append(expectedTopics, reopenTopic)
	var ready healthObservation
	var readyErr error
	var reopenExitCode *int
	err = waitFor(ctx, 30*time.Second, progressProbeInterval, func() (bool, error) {
		ready, readyErr = scenario.health(ctx, targetID, "/health/ready")
		if readyErr == nil && ready.HTTPStatus == 503 && ready.ErrorMessage == "raft storage failed: I/O error; restart required" {
			return true, nil
		}
		if readyErr == nil {
			return false, nil
		}
		logs, err := scenario.docker.nodeLogs(ctx, targetID)
		if err != nil {
			return false, err
		}
		exitCodes := supervisorBrokerExitCodes(logs)
		if len(exitCodes) > previousExitCount && exitCodes[previousExitCount] != 0 {
			code := exitCodes[previousExitCount]
			reopenExitCode = &code
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("follower did not fail closed after reopen with fail-fsync active: %w", err)
	}
	scenario.evidence.ReopenExitCode = reopenExitCode
	scenario.evidence.ReopenFailClosed = true
	if err := scenario.recorder.record("reopen", targetID, map[string]any{
		"exitCode": reopenExitCode, "failClosed": true, "readyStatus": ready.HTTPStatus,
		"readyMessage": ready.ErrorMessage, "healthError": errorString(readyErr),
	}); err != nil {
		return err
	}
	if err := scenario.setStorageMode(ctx, targetID, "normal"); err != nil {
		return err
	}
	faultModeReset = true
	leader, err := scenario.node(ctx, leaderID, leaderID)
	if err != nil {
		return err
	}
	if err := scenario.docker.signalBrokerRestart(ctx, targetID); err != nil {
		return err
	}
	if err := scenario.awaitFollowerRecovery(ctx, targetID, leader.LastAppliedIndex, 120*time.Second, true); err != nil {
		return err
	}
	return scenario.verifyDurableDelivery(ctx, leaderID, expectedTopics, nil)
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-030, cluster-session-and-delivery.md:CLU-SCN-084
func (scenario *storageFaultScenario) runFollowerTornPowerLoss(ctx context.Context, leaderID, targetID, healthyID string) error {
	scenario.evidence.TargetNodeIDs = []string{targetID}
	if err := scenario.setStorageMode(ctx, targetID, "hold-wal-fsync"); err != nil {
		return err
	}
	topic := durableTopic(scenario.options.ScenarioID, "leader", "held-qos1")
	publishDone := make(chan error, 1)
	go func() {
		publishDone <- scenario.durablePublish(ctx, leaderID, "chaos-durable-pub-3", topic, 1, "during-fault", true)
	}()
	held, err := scenario.waitForHeldWALFsync(ctx, targetID, 15*time.Second)
	if err != nil {
		return err
	}
	scenario.evidence.HeldWALFsyncs = held.HeldWalFsyncs
	select {
	case err := <-publishDone:
		if err != nil {
			return fmt.Errorf("durable publish while follower WAL fsync was held: %w", err)
		}
	case <-time.After(durableAckTimeout + time.Second):
		return errors.New("durable publish did not receive an ACK while the follower WAL fsync was held")
	case <-ctx.Done():
		return ctx.Err()
	}
	healthy, err := scenario.node(ctx, healthyID, healthyID)
	if err != nil {
		return err
	}
	if !healthy.Ready || healthy.ReplicationHealth != "HEALTHY" {
		return fmt.Errorf("healthy follower %s is not healthy before target power cut: ready=%t health=%q", healthyID, healthy.Ready, healthy.ReplicationHealth)
	}
	cut, err := scenario.powerCut(ctx, targetID, true)
	if err != nil {
		return err
	}
	if err := validateTornWALCut(cut); err != nil {
		return err
	}
	scenario.evidence.PowerCuts = map[string]powerCutEvidence{targetID: cut}
	if err := scenario.powerOn(ctx, targetID); err != nil {
		return err
	}
	if err := scenario.waitAllReady(ctx, startupTimeout); err != nil {
		return fmt.Errorf("cluster did not become ready after follower power-on: %w", err)
	}
	leader, err := scenario.waitLeaderAgreement(ctx)
	if err != nil {
		return err
	}
	leaderState, err := scenario.node(ctx, leader, leader)
	if err != nil {
		return err
	}
	if err := scenario.awaitFollowerRecovery(ctx, targetID, leaderState.LastAppliedIndex, 120*time.Second, true); err != nil {
		return err
	}
	scenario.evidence.ReopenFailClosed = false
	if err := scenario.verifyDurableDelivery(ctx, leader, []string{topic}, nil); err != nil {
		return err
	}
	return nil
}

func (scenario *storageFaultScenario) reconnectDurableSubscriber(ctx context.Context, nodeID string) error {
	oldClient := scenario.durableSubscriber
	if oldClient != nil {
		_ = oldClient.close()
	}
	address := "127.0.0.1:" + scenario.docker.mqttPorts[nodeID]
	client, sessionPresent, err := dialDurableClient(ctx, address, scenario.fixture.AccessToken, "chaos-durable-sub", false)
	if err != nil {
		return err
	}
	if !sessionPresent {
		_ = client.close()
		return errors.New("persistent durable subscriber session was not present after cluster power loss")
	}
	scenario.durableSubscriber = client
	present := true
	scenario.evidence.DurableAck.RecoverySessionPresent = &present
	if err := waitFor(ctx, 30*time.Second, probeInterval, func() (bool, error) {
		for _, memberID := range nodeIDs() {
			subscriptions, status, _, err := scenario.subscriptions(ctx, memberID)
			if err != nil {
				return false, err
			}
			if status != 200 || !hasSubscription(subscriptions, "chaos-durable-sub", "chaos/durable/#") {
				return false, fmt.Errorf("node %s does not list the recovered durable ACK subscription", memberID)
			}
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("recovered durable ACK subscription did not appear on every node: %w", err)
	}
	return nil
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-031, cluster-session-and-delivery.md:CLU-SCN-084
func (scenario *storageFaultScenario) runClusterPowerLoss(ctx context.Context, leaderID string, followers []string) error {
	scenario.evidence.TargetNodeIDs = append([]string(nil), nodeIDs()...)
	beforeQoS1 := durableTopic(scenario.options.ScenarioID, "leader", "before-qos1")
	beforeQoS2 := durableTopic(scenario.options.ScenarioID, "leader", "before-qos2")
	if err := scenario.durablePublish(ctx, leaderID, "chaos-durable-pub-1", beforeQoS1, 1, "before-fault", true); err != nil {
		return err
	}
	if err := scenario.durablePublish(ctx, leaderID, "chaos-durable-pub-2", beforeQoS2, 2, "before-fault", true); err != nil {
		return err
	}
	leaderBefore, err := scenario.node(ctx, leaderID, leaderID)
	if err != nil {
		return err
	}
	if err := scenario.setStorageMode(ctx, followers[0], "hold-wal-fsync"); err != nil {
		return err
	}
	var ackedCount atomic.Int64
	publisherCtx, cancelPublisher := context.WithCancel(ctx)
	defer cancelPublisher()
	inFlightTopics := make(chan string, 1)
	publisherResults := scenario.startInflightPublisher(publisherCtx, leaderID, &ackedCount, inFlightTopics)
	held, err := scenario.waitForHeldWALFsync(ctx, followers[0], 15*time.Second)
	if err != nil {
		return err
	}
	scenario.evidence.HeldWALFsyncs = held.HeldWalFsyncs
	wantPreCutAcked := int64(inflightCutSequence - 1)
	if err := waitFor(ctx, 5*time.Second, 50*time.Millisecond, func() (bool, error) {
		return ackedCount.Load() >= wantPreCutAcked, nil
	}); err != nil {
		return fmt.Errorf("in-flight publisher received fewer than %d ACKs while follower WAL fsync was held: %w", wantPreCutAcked, err)
	}
	var inFlightTopic string
	select {
	case inFlightTopic = <-inFlightTopics:
	case result, ok := <-publisherResults:
		if !ok {
			return errors.New("in-flight publisher exited before sending its cut-window publish")
		}
		if result.err != nil {
			return fmt.Errorf("in-flight publisher exited before sending its cut-window publish: %w", result.err)
		}
		if len(result.unacked) != 0 {
			return fmt.Errorf("in-flight publisher stopped at topic %s with outcome %q before the cut-window barrier: %v", result.unacked[0], result.unackedOutcome, result.unackedErr)
		}
		return errors.New("in-flight publisher exited before sending its cut-window publish")
	case <-time.After(5 * time.Second):
		return errors.New("in-flight publisher did not send its cut-window publish")
	case <-ctx.Done():
		return ctx.Err()
	}
	if got := ackedCount.Load(); got != wantPreCutAcked {
		return fmt.Errorf("in-flight publisher ACK count at cut barrier = %d, want %d", got, wantPreCutAcked)
	}
	scenario.evidence.InFlightPublishTopic = inFlightTopic
	if err := scenario.recorder.record("publish-in-flight", leaderID, map[string]any{
		"topic": inFlightTopic, "qos": 1, "pubackObserved": false,
	}); err != nil {
		return err
	}

	leaderState, err := scenario.node(ctx, leaderID, leaderID)
	if err != nil {
		return err
	}
	scenario.evidence.AppliedIndexEnd = leaderState.LastAppliedIndex
	scenario.evidence.PowerCuts = make(map[string]powerCutEvidence, chaosNodeCount)
	type powerCutTarget struct {
		nodeID string
		torn   bool
	}
	type powerCutResult struct {
		nodeID   string
		evidence powerCutEvidence
		err      error
	}
	targets := []powerCutTarget{
		{nodeID: followers[0], torn: true},
		{nodeID: leaderID},
		{nodeID: followers[1]},
	}
	cutBarrier := make(chan struct{})
	cutResults := make(chan powerCutResult, len(targets))
	for _, target := range targets {
		go func(target powerCutTarget) {
			<-cutBarrier
			evidence, err := scenario.powerCut(ctx, target.nodeID, target.torn)
			cutResults <- powerCutResult{nodeID: target.nodeID, evidence: evidence, err: err}
		}(target)
	}
	close(cutBarrier)
	var cutErrors []error
	for range targets {
		result := <-cutResults
		if result.err != nil {
			cutErrors = append(cutErrors, fmt.Errorf("power-cut node %s: %w", result.nodeID, result.err))
			continue
		}
		if result.nodeID == followers[0] {
			if err := validateTornWALCut(result.evidence); err != nil {
				cutErrors = append(cutErrors, fmt.Errorf("follower %s torn power cut: %w", result.nodeID, err))
				continue
			}
		}
		scenario.evidence.PowerCuts[result.nodeID] = result.evidence
	}
	if err := errors.Join(cutErrors...); err != nil {
		cancelPublisher()
		return err
	}
	cancelPublisher()

	var publishResult inflightPublishResult
	select {
	case result, ok := <-publisherResults:
		if !ok {
			return errors.New("in-flight publisher exited without a result")
		}
		publishResult = result
	case <-time.After(5 * time.Second):
		return errors.New("in-flight publisher did not stop after cluster power loss")
	case <-ctx.Done():
		return ctx.Err()
	}
	if publishResult.err != nil {
		return fmt.Errorf("in-flight publisher failed: %w", publishResult.err)
	}
	if len(publishResult.acked) != int(wantPreCutAcked) {
		return fmt.Errorf("in-flight publisher ACKed %d topics, want %d before the cut-window publish", len(publishResult.acked), wantPreCutAcked)
	}
	if len(publishResult.unacked) != 1 || publishResult.unacked[0] != inFlightTopic {
		return fmt.Errorf("in-flight publisher unacknowledged topics = %v, want only cut-window topic %s", publishResult.unacked, inFlightTopic)
	}
	if publishResult.unackedOutcome != durableUnknown {
		return fmt.Errorf("cut-window PUBACK outcome = %q, want unknown after power cut", publishResult.unackedOutcome)
	}
	if len(publishResult.acked) >= 40 {
		return fmt.Errorf("in-flight publisher completed all 40 publishes before the power cut")
	}
	scenario.evidence.InFlightPublishAckOutcome = string(publishResult.unackedOutcome)
	for _, topic := range publishResult.acked {
		if err := scenario.recordDurableAck(leaderID, "chaos-durable-pub-3", topic, 1, "during-fault", durableAcked, nil); err != nil {
			return err
		}
	}
	if err := scenario.recordDurableAck(leaderID, "chaos-durable-pub-3", publishResult.unacked[0], 1, "during-fault", publishResult.unackedOutcome, publishResult.unackedErr); err != nil {
		return err
	}

	for _, nodeID := range nodeIDs() {
		if err := scenario.powerOn(ctx, nodeID); err != nil {
			return err
		}
	}
	if err := scenario.waitAllReady(ctx, startupTimeout); err != nil {
		return fmt.Errorf("cluster did not become ready after power-on: %w", err)
	}
	leader, err := scenario.waitLeaderAgreement(ctx)
	if err != nil {
		return err
	}
	scenario.evidence.RecoveryLeaderNodeID = leader
	converged, err := scenario.waitForIndexConvergence(ctx, leaderBefore.LastAppliedIndex, 30*time.Second)
	if err != nil {
		return fmt.Errorf("cluster applied indexes did not converge after power loss: %w", err)
	}
	scenario.evidence.ConvergedAppliedIndex = converged
	for _, nodeID := range nodeIDs() {
		if err := waitFor(ctx, 30*time.Second, probeInterval, func() (bool, error) {
			probe, ready, err := scenario.mqttWithReadiness(ctx, nodeID, recoveryClientID(nodeID), "chaos/probe/#")
			if err != nil {
				return false, err
			}
			return ready && probe.Classification == "accepted", nil
		}); err != nil {
			return fmt.Errorf("node %s did not accept MQTT after cluster power loss: %w", nodeID, err)
		}
		if err := scenario.waitForBaselineSubscription(ctx, nodeID, 30*time.Second); err != nil {
			return fmt.Errorf("node %s baseline subscription after cluster power loss: %w", nodeID, err)
		}
	}
	if err := scenario.reconnectOriginalBaseline(ctx, leader); err != nil {
		return err
	}
	if err := scenario.reconnectDurableSubscriber(ctx, leader); err != nil {
		return err
	}
	scenario.evidence.ReopenFailClosed = false
	expected := append([]string{beforeQoS1, beforeQoS2}, publishResult.acked...)
	return scenario.verifyDurableDelivery(ctx, leader, expected, publishResult.unacked)
}
