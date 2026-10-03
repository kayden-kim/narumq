package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Traceability: CLU-11/NFR-15; docs/specs/cluster-session-and-delivery.md §8, §11.17.
func TestDynamicClusterReleaseCompatibilityProcessAcceptance(t *testing.T) {
	if os.Getenv("NARUMQ_RELEASE_COMPAT") != "1" {
		t.Skip("set NARUMQ_RELEASE_COMPAT=1 to run release N-1/N compatibility acceptance")
	}

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	binN2 := fixture.buildBinary(t, "narumq-1.0.0", "-X main.buildReleaseVersion=1.0.0")
	binN1 := fixture.buildBinary(t, "narumq-1.1.0", "-X main.buildReleaseVersion=1.1.0")
	binN := fixture.buildBinary(t, "narumq-1.2.0", "-X main.buildReleaseVersion=1.2.0 -X main.buildFeatures=compatibility-probe")
	runClusterReleaseCompatibilityAcceptance(t, fixture, binN2, binN1, binN)
}

func runClusterReleaseCompatibilityAcceptance(t *testing.T, fixture *clusterMembershipProcessFixture, binN2, binN1, binN string) {
	ctx := fixture.ctx
	configs := []string{fixture.bootstrapConfig(1), fixture.bootstrapConfig(2), fixture.bootstrapConfig(3)}
	processes := make([]*clusterMembershipProcess, 3)

	// Scenario 1: bootstrap the complete cluster on N-1 and establish the durable session.
	for index := range processes {
		process, err := fixture.harness.startBinary(binN1, fmt.Sprintf("N-1 bootstrap node %d", index+1), configs[index], fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start N-1 bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrapRelease(t, ctx, processes, "1.1.0")
	clusterReleaseCompatWaitVersions(t, ctx, processes[0], map[string]string{"1": "1.1.0", "2": "1.1.0", "3": "1.1.0"})
	t.Log(`N-1 bootstrap committed versions {"1":"1.1.0","2":"1.1.0","3":"1.1.0"}`)
	subscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "compat-sub", false)
	clusterDurableProcessSubscribe(t, subscriber, 1, "compat/durable", 1)
	clusterMembershipProcessWaitForRouteProjection(t, ctx, processes, "compat/durable")
	clusterDurableProcessWriteFatal(t, subscriber, 0xE0, nil)
	_ = subscriber.Close()
	clusterReleaseCompatDurableRoundTrip(t, fixture, 2, 3, "phase-1")

	// Scenario 2: an N follower recovers a snapshot produced by the two N-1 peers.
	leaderID, _ := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	f1ID := clusterReleaseCompatSmallestNonLeader(leaderID, []uint64{1, 2, 3})
	followerApplied := clusterReleaseCompatLastApplied(t, ctx, processes[f1ID-1], f1ID)
	if err := processes[f1ID-1].stop(10 * time.Second); err != nil {
		t.Fatalf("stop follower %d before snapshot transfer: %v", f1ID, err)
	}
	runningIDs := clusterReleaseCompatOtherIDs(f1ID)
	for _, nodeID := range runningIDs {
		clusterMembershipProcessWaitForStatus(t, ctx, processes[nodeID-1], "/health/ready", http.StatusOK)
	}
	loadLeaderID, _ := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	t.Logf("creating snapshots beyond stopped node %d applied index %d via live nodes %v, leader publisher %d", f1ID, followerApplied, runningIDs, loadLeaderID)
	lastSnapshotPayload, leaderSnapshotIndex := clusterReleaseCompatLoadUntilSnapshot(t, ctx, fixture, processes, loadLeaderID, runningIDs, followerApplied)
	f1, err := fixture.harness.startBinary(binN, fmt.Sprintf("N follower %d", f1ID), configs[f1ID-1], fixture.managementAddresses[f1ID-1])
	if err != nil {
		t.Fatalf("start N follower %d: %v", f1ID, err)
	}
	processes[f1ID-1] = f1
	clusterMembershipProcessWaitForStatus(t, ctx, f1, "/health/ready", http.StatusOK)
	clusterReleaseCompatWaitVersions(t, ctx, f1, clusterReleaseCompatVersionsFor("1.1.0", map[uint64]string{f1ID: "1.2.0"}))
	clusterReleaseCompatAssertSnapshot(t, ctx, fixture, f1, f1ID, followerApplied, leaderSnapshotIndex)
	clusterReleaseCompatAssertRetained(t, fixture, f1ID, lastSnapshotPayload)
	_, featureState, featureBody, err := clusterReleaseCompatFeatures(ctx, f1)
	if err != nil {
		t.Fatalf("read N follower feature profile: %v body=%s", err, featureBody)
	}
	if !clusterReleaseCompatMemberHasFeature(featureState, f1ID, "compatibility-probe") {
		t.Fatalf("N follower feature state = %+v, want compatibility-probe on node %d", featureState, f1ID)
	}
	status, body := clusterReleaseCompatActivate(t, ctx, f1)
	clusterReleaseCompatAssertActivationFailure(t, status, body, http.StatusConflict, "feature_not_supported_by_all_members")
	// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
	// Historical members lack metadata support; the mixed cluster must keep its legacy wire path.
	for _, member := range featureState.Members {
		if slices.Contains(member.Features, "session-metadata") {
			continue
		}
		metadataStatus, metadataBody, metadataErr := clusterMembershipProcessRequest(ctx, http.MethodPost,
			f1.managementURL()+"/api/v1/cluster/features/session-metadata", nil, clusterMembershipProcessAdmin, "")
		if metadataErr != nil {
			t.Fatalf("activate metadata in historical mixed cluster: %v", metadataErr)
		}
		clusterReleaseCompatAssertActivationFailure(t, metadataStatus, metadataBody, http.StatusConflict, "feature_not_supported_by_all_members")
		t.Log("historical mixed cluster rejected session-metadata activation")
		break
	}
	oldID := runningIDs[0]
	clusterReleaseCompatDurableRoundTrip(t, fixture, oldID, f1ID, "phase-2")

	// Scenario 3: drain and upgrade a non-leader still running N-1.
	leaderID, leaderURL := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	drainID := clusterReleaseCompatSmallestVersionedNonLeader(t, ctx, processes, leaderID, "1.1.0")
	status, drainBody, err := clusterDrainProcessStart(ctx, leaderURL, drainID, clusterDrainProcessRequest{
		ServerReference: fixture.mqttAddresses[f1ID-1], ConnectionRatePerSecond: 2,
		LoadBalancerPropagationDelayMs: 1500, TimeoutMs: 60000,
	}, "compat-drain-D")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("start drain for node %d status=%d body=%s err=%v", drainID, status, drainBody, err)
	}
	drain := clusterDrainProcessWaitForPhase(t, ctx, processes[drainID-1], drainID, "COMPLETED")
	if drain.State != "SAFE_TO_STOP" || !drain.SafeToStop {
		t.Fatalf("drain result for node %d = %+v, want SAFE_TO_STOP", drainID, drain)
	}
	processes[drainID-1] = clusterReleaseCompatRestart(t, fixture, processes, drainID, binN, configs[drainID-1])
	wantVersions := clusterReleaseCompatVersionsFor("1.1.0", map[uint64]string{f1ID: "1.2.0", drainID: "1.2.0"})
	clusterReleaseCompatWaitVersions(t, ctx, processes[f1ID-1], wantVersions)

	// Scenario 4: force a mixed-cluster leader failure, then prove N can snapshot to N-1.
	yID := clusterReleaseCompatOnlyVersionID(t, ctx, processes, "1.1.0")
	leaderID, _ = clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	if leaderID != yID {
		if err := processes[leaderID-1].crash(5 * time.Second); err != nil {
			t.Fatalf("crash mixed-version leader %d: %v", leaderID, err)
		}
		clusterReleaseCompatWaitForLeader(t, ctx, processes, 3)
		restartedLeader, startErr := fixture.harness.startBinary(binN, fmt.Sprintf("recovered N leader %d", leaderID), configs[leaderID-1], fixture.managementAddresses[leaderID-1])
		if startErr != nil {
			t.Fatalf("restart failed leader %d on N: %v", leaderID, startErr)
		}
		processes[leaderID-1] = restartedLeader
		clusterMembershipProcessWaitForStatus(t, ctx, restartedLeader, "/health/ready", http.StatusOK)
	}
	yApplied := clusterReleaseCompatLastApplied(t, ctx, processes[yID-1], yID)
	if err := processes[yID-1].crash(5 * time.Second); err != nil {
		t.Fatalf("crash N-1 node %d for N leader snapshot transfer: %v", yID, err)
	}
	nLeaderID, _ := clusterReleaseCompatWaitForLeader(t, ctx, processes, 3)
	versionsBeforeSnapshot, err := clusterReleaseCompatReadVersions(ctx, processes[nLeaderID-1])
	if err != nil || versionsBeforeSnapshot[fmt.Sprint(nLeaderID)] != "1.2.0" {
		t.Fatalf("new leader %d version=%q versions=%v err=%v, want N (1.2.0)", nLeaderID, versionsBeforeSnapshot[fmt.Sprint(nLeaderID)], versionsBeforeSnapshot, err)
	}
	runningIDs = clusterReleaseCompatOtherIDs(yID)
	lastSnapshotPayload, leaderSnapshotIndex = clusterReleaseCompatLoadUntilSnapshot(t, ctx, fixture, processes, nLeaderID, runningIDs, yApplied)
	y, err := fixture.harness.startBinary(binN1, fmt.Sprintf("N-1 follower %d", yID), configs[yID-1], fixture.managementAddresses[yID-1])
	if err != nil {
		t.Fatalf("restart N-1 follower %d: %v", yID, err)
	}
	processes[yID-1] = y
	clusterMembershipProcessWaitForStatus(t, ctx, y, "/health/ready", http.StatusOK)
	clusterReleaseCompatAssertSnapshot(t, ctx, fixture, y, yID, yApplied, leaderSnapshotIndex)
	clusterReleaseCompatAssertRetained(t, fixture, yID, lastSnapshotPayload)
	clusterReleaseCompatWaitVersions(t, ctx, y, clusterReleaseCompatVersionsFor("1.2.0", map[uint64]string{yID: "1.1.0"}))
	nSubscriberID := clusterReleaseCompatFirstOtherID(yID)
	clusterReleaseCompatDurableRoundTrip(t, fixture, yID, nSubscriberID, "phase-4")

	// Scenario 5: roll both N processes back one at a time while preserving durable sessions.
	rollbackPayloads := []string{"phase-5a", "phase-5b"}
	rollbackCount := 0
	for rollbackCount < 2 {
		leaderID, _ = clusterMembershipProcessFindLeader(t, ctx, processes, 3)
		versions, readErr := clusterReleaseCompatReadVersions(ctx, processes[leaderID-1])
		if readErr != nil {
			t.Fatalf("read versions before rollback %d: %v", rollbackCount+1, readErr)
		}
		var rollbackID uint64
		for id := uint64(1); id <= 3; id++ {
			if versions[fmt.Sprint(id)] == "1.2.0" && id != leaderID && (rollbackID == 0 || id < rollbackID) {
				rollbackID = id
			}
		}
		if rollbackID == 0 {
			for id := uint64(1); id <= 3; id++ {
				if versions[fmt.Sprint(id)] == "1.2.0" {
					rollbackID = id
					break
				}
			}
		}
		if rollbackID == 0 {
			t.Fatalf("no N node remains to roll back: %v", versions)
		}
		processes[rollbackID-1] = clusterReleaseCompatRestart(t, fixture, processes, rollbackID, binN1, configs[rollbackID-1])
		versions[fmt.Sprint(rollbackID)] = "1.1.0"
		clusterReleaseCompatWaitVersions(t, ctx, processes[leaderID-1], versions)
		subscribeID := yID
		if rollbackCount == 1 {
			subscribeID = clusterReleaseCompatFirstOtherID(rollbackID)
		}
		if subscribeID == rollbackID {
			subscribeID = clusterReleaseCompatFirstOtherID(rollbackID)
		}
		clusterReleaseCompatDurableRoundTrip(t, fixture, rollbackID, subscribeID, rollbackPayloads[rollbackCount])
		rollbackCount++
	}
	clusterReleaseCompatWaitVersions(t, ctx, processes[0], map[string]string{"1": "1.1.0", "2": "1.1.0", "3": "1.1.0"})

	// Scenario 6: re-upgrade to N, prove N/N snapshot transfer in both peer roles, then reject N-2.
	for upgraded := 0; upgraded < 3; upgraded++ {
		leaderID, _ = clusterMembershipProcessFindLeader(t, ctx, processes, 3)
		versions, readErr := clusterReleaseCompatReadVersions(ctx, processes[leaderID-1])
		if readErr != nil {
			t.Fatalf("read versions before re-upgrade %d: %v", upgraded+1, readErr)
		}
		var upgradeID uint64
		for id := uint64(1); id <= 3; id++ {
			if versions[fmt.Sprint(id)] == "1.1.0" && id != leaderID && (upgradeID == 0 || id < upgradeID) {
				upgradeID = id
			}
		}
		if upgradeID == 0 {
			for id := uint64(1); id <= 3; id++ {
				if versions[fmt.Sprint(id)] == "1.1.0" {
					upgradeID = id
					break
				}
			}
		}
		if upgradeID == 0 {
			t.Fatalf("no N-1 node remains to re-upgrade: %v", versions)
		}
		processes[upgradeID-1] = clusterReleaseCompatRestart(t, fixture, processes, upgradeID, binN, configs[upgradeID-1])
		versions[fmt.Sprint(upgradeID)] = "1.2.0"
		clusterReleaseCompatWaitVersions(t, ctx, processes[leaderID-1], versions)
	}
	leaderID, _ = clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	versions, err := clusterReleaseCompatReadVersions(ctx, processes[leaderID-1])
	if err != nil || !clusterReleaseCompatVersionsEqual(versions, map[string]string{"1": "1.2.0", "2": "1.2.0", "3": "1.2.0"}) {
		t.Fatalf("versions after re-upgrade = %v err=%v, want all N", versions, err)
	}
	snapshotTargets := []uint64{
		clusterReleaseCompatSmallestNonLeader(leaderID, []uint64{1, 2, 3}),
		leaderID,
	}
	for transfer, targetID := range snapshotTargets {
		previousApplied := clusterReleaseCompatLastApplied(t, ctx, processes[targetID-1], targetID)
		if err := processes[targetID-1].stop(10 * time.Second); err != nil {
			t.Fatalf("stop N node %d before same-release snapshot transfer %d: %v", targetID, transfer+1, err)
		}
		runningIDs := clusterReleaseCompatOtherIDs(targetID)
		for _, nodeID := range runningIDs {
			clusterMembershipProcessWaitForStatus(t, ctx, processes[nodeID-1], "/health/ready", http.StatusOK)
		}
		snapshotLeaderID, _ := clusterReleaseCompatWaitForLeader(t, ctx, processes, 3)
		versionsBeforeSnapshot, readErr := clusterReleaseCompatReadVersions(ctx, processes[snapshotLeaderID-1])
		if readErr != nil || !clusterReleaseCompatVersionsEqual(versionsBeforeSnapshot, map[string]string{
			"1": "1.2.0", "2": "1.2.0", "3": "1.2.0",
		}) {
			t.Fatalf("same-release snapshot source versions = %v err=%v, want all N", versionsBeforeSnapshot, readErr)
		}
		t.Logf("same-release N snapshot transfer %d: node %d receives from N leader %d beyond applied index %d", transfer+1, targetID, snapshotLeaderID, previousApplied)
		nSnapshotPayload, nSnapshotIndex := clusterReleaseCompatLoadUntilSnapshot(t, ctx, fixture, processes, snapshotLeaderID, runningIDs, previousApplied)
		recovered, startErr := fixture.harness.startBinary(binN, fmt.Sprintf("N same-release snapshot follower %d transfer %d", targetID, transfer+1), configs[targetID-1], fixture.managementAddresses[targetID-1])
		if startErr != nil {
			t.Fatalf("restart N snapshot follower %d: %v", targetID, startErr)
		}
		processes[targetID-1] = recovered
		clusterMembershipProcessWaitForStatus(t, ctx, recovered, "/health/ready", http.StatusOK)
		clusterReleaseCompatWaitVersions(t, ctx, recovered, clusterReleaseCompatVersionsFor("1.2.0", nil))
		clusterReleaseCompatAssertSnapshot(t, ctx, fixture, recovered, targetID, previousApplied, nSnapshotIndex)
		clusterReleaseCompatAssertRetained(t, fixture, targetID, nSnapshotPayload)
	}
	leaderID, _ = clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	n2ID := clusterReleaseCompatSmallestNonLeader(leaderID, []uint64{1, 2, 3})
	if err := processes[n2ID-1].stop(10 * time.Second); err != nil {
		t.Fatalf("stop node %d before N-2 restart: %v", n2ID, err)
	}
	n2, err := fixture.harness.startBinary(binN2, fmt.Sprintf("N-2 node %d", n2ID), configs[n2ID-1], fixture.managementAddresses[n2ID-1])
	if err != nil {
		t.Fatalf("start N-2 node %d: %v", n2ID, err)
	}
	processes[n2ID-1] = n2
	clusterReleaseCompatWaitForStatus(t, ctx, n2, http.StatusServiceUnavailable, 20*time.Second)
	n2ObservationDeadline := time.Now().Add(10 * time.Second)
	observations := 0
	for next := time.Now(); next.Before(n2ObservationDeadline); next = next.Add(time.Second) {
		if err := n2.isAlive(); err != nil {
			t.Fatalf("N-2 process exited during rejection window: %v", err)
		}
		status, body, requestErr := clusterMembershipProcessRequest(ctx, http.MethodGet, n2.managementURL()+"/health/ready", nil, "", "")
		if requestErr != nil || status != http.StatusServiceUnavailable {
			t.Fatalf("N-2 readiness during rejection window=(%d,%s,%v), want 503", status, body, requestErr)
		}
		for id, process := range processes {
			if uint64(id+1) == n2ID {
				continue
			}
			readyStatus, readyBody, readyErr := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+"/health/ready", nil, "", "")
			if readyErr != nil || readyStatus != http.StatusOK {
				t.Fatalf("N node %d readiness during N-2 rejection=(%d,%s,%v), want 200", id+1, readyStatus, readyBody, readyErr)
			}
		}
		committed, readErr := clusterReleaseCompatReadVersions(ctx, processes[leaderID-1])
		if readErr != nil || committed[fmt.Sprint(n2ID)] != "1.2.0" {
			t.Fatalf("committed N-2 member profile = %v err=%v, want node %d remain 1.2.0", committed, readErr, n2ID)
		}
		observations++
		time.Sleep(time.Until(next.Add(time.Second)))
	}
	if observations < 9 {
		t.Fatalf("observed N-2 rejection %d times in ten seconds, want at least 9", observations)
	}
	t.Logf("N-2 rejection observed %d times with unchanged committed N profile", observations)
	processes[n2ID-1] = clusterReleaseCompatRestart(t, fixture, processes, n2ID, binN, configs[n2ID-1])
	clusterReleaseCompatWaitVersions(t, ctx, processes[n2ID-1], map[string]string{"1": "1.2.0", "2": "1.2.0", "3": "1.2.0"})

	// Scenario 7: activate the synthetic feature and block an N-1 rollback until N returns.
	leaderID, _ = clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	status, body = clusterReleaseCompatActivate(t, ctx, processes[leaderID-1])
	if status != http.StatusOK {
		t.Fatalf("activate compatibility-probe status=%d body=%s, want 200", status, body)
	}
	state := clusterReleaseCompatDecodeFeatureState(t, body)
	if !slices.Equal(state.ActiveFeatures, []string{"compatibility-probe"}) {
		t.Fatalf("active features after activation = %v, want compatibility-probe", state.ActiveFeatures)
	}
	status, replayBody := clusterReleaseCompatActivate(t, ctx, processes[leaderID-1])
	if status != http.StatusOK || !slices.Equal(clusterReleaseCompatDecodeFeatureState(t, replayBody).ActiveFeatures, []string{"compatibility-probe"}) {
		t.Fatalf("feature activation replay status=%d body=%s, want 200 with unchanged active feature", status, replayBody)
	}
	rollbackID := clusterReleaseCompatSmallestNonLeader(leaderID, []uint64{1, 2, 3})
	if err := processes[rollbackID-1].stop(10 * time.Second); err != nil {
		t.Fatalf("stop follower %d before feature rollback: %v", rollbackID, err)
	}
	rolledBack, err := fixture.harness.startBinary(binN1, fmt.Sprintf("N-1 rollback node %d", rollbackID), configs[rollbackID-1], fixture.managementAddresses[rollbackID-1])
	if err != nil {
		t.Fatalf("start N-1 rollback node %d: %v", rollbackID, err)
	}
	processes[rollbackID-1] = rolledBack
	clusterMembershipProcessWaitForStatus(t, ctx, rolledBack, "/health/live", http.StatusOK)
	incompatibleReason := clusterReleaseCompatWaitIncompatibleReason(t, ctx, rolledBack, 20*time.Second)
	if !strings.HasPrefix(incompatibleReason, "incompatible_state_schema: ") {
		t.Fatalf("N-1 rollback readiness reason = %q, want incompatible_state_schema prefix", incompatibleReason)
	}
	t.Logf("N-1 rollback readiness reason = %q", incompatibleReason)
	liveStatus, liveBody, liveErr := clusterMembershipProcessRequest(ctx, http.MethodGet, rolledBack.managementURL()+"/health/live", nil, "", "")
	if liveErr != nil || liveStatus != http.StatusOK {
		t.Fatalf("N-1 rollback liveness=(%d,%s,%v), want 200 while incompatible readiness is 503", liveStatus, liveBody, liveErr)
	}
	for id, process := range processes {
		if uint64(id+1) == rollbackID {
			continue
		}
		readyStatus, readyBody, readyErr := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+"/health/ready", nil, "", "")
		if readyErr != nil || readyStatus != http.StatusOK {
			t.Fatalf("N node %d readiness during rollback block=(%d,%s,%v), want 200", id+1, readyStatus, readyBody, readyErr)
		}
	}
	committed, committedErr := clusterReleaseCompatReadVersions(ctx, processes[leaderID-1])
	if committedErr != nil || committed[fmt.Sprint(rollbackID)] != "1.2.0" {
		t.Fatalf("committed rollback node profile=%v err=%v, want node %d remain 1.2.0", committed, committedErr, rollbackID)
	}
	clusterReleaseCompatWaitVersions(t, ctx, processes[leaderID-1], map[string]string{"1": "1.2.0", "2": "1.2.0", "3": "1.2.0"})
	otherIDs := clusterReleaseCompatOtherIDs(rollbackID)
	clusterReleaseCompatDurableRoundTrip(t, fixture, otherIDs[0], otherIDs[1], "phase-7")
	processes[rollbackID-1] = clusterReleaseCompatRestart(t, fixture, processes, rollbackID, binN, configs[rollbackID-1])
	clusterReleaseCompatWaitVersions(t, ctx, processes[rollbackID-1], map[string]string{"1": "1.2.0", "2": "1.2.0", "3": "1.2.0"})
	_, finalFeatureState, finalFeatureBody, err := clusterReleaseCompatFeatures(ctx, processes[rollbackID-1])
	if err != nil || !slices.Equal(finalFeatureState.ActiveFeatures, []string{"compatibility-probe"}) {
		t.Fatalf("active feature after N recovery = %+v err=%v body=%s", finalFeatureState, err, finalFeatureBody)
	}
}

type clusterReleaseCompatMember struct {
	NodeID         string   `json:"nodeId"`
	ReleaseVersion string   `json:"releaseVersion"`
	Features       []string `json:"features"`
}

type clusterReleaseCompatFeatureState struct {
	ActiveFeatures []string                     `json:"activeFeatures"`
	Members        []clusterReleaseCompatMember `json:"members"`
}

func clusterReleaseCompatRestart(t *testing.T, fixture *clusterMembershipProcessFixture, processes []*clusterMembershipProcess, nodeID uint64, binary, configPath string) *clusterMembershipProcess {
	t.Helper()
	previous := processes[nodeID-1]
	if err := previous.stop(10 * time.Second); err != nil {
		t.Fatalf("stop node %d before binary restart: %v", nodeID, err)
	}
	restarted, err := fixture.harness.startBinary(binary, fmt.Sprintf("node %d restarted", nodeID), configPath, fixture.managementAddresses[nodeID-1])
	if err != nil {
		t.Fatalf("restart node %d: %v", nodeID, err)
	}
	processes[nodeID-1] = restarted
	clusterMembershipProcessWaitForStatus(t, fixture.ctx, restarted, "/health/ready", http.StatusOK)
	return restarted
}
func clusterReleaseCompatWaitForLeader(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, expectedCount int) (uint64, string) {
	t.Helper()
	var leaderID uint64
	clusterMembershipProcessWait(t, ctx, "live elected cluster leader", func() (bool, string, error) {
		for _, process := range processes {
			if process == nil || process.isAlive() != nil {
				continue
			}
			summary, status, _, err := clusterMembershipProcessGetSummary(ctx, process.managementURL(), clusterMembershipProcessAdmin)
			if err != nil || status != http.StatusOK || summary.LeaderNodeID == nil {
				continue
			}
			if summary.MemberCount != expectedCount {
				return false, fmt.Sprintf("cluster summary member count=%d want=%d", summary.MemberCount, expectedCount), nil
			}
			id, err := strconv.ParseUint(*summary.LeaderNodeID, 10, 64)
			if err != nil || id == 0 || id > uint64(len(processes)) {
				continue
			}
			leader := processes[id-1]
			if leader == nil || leader.isAlive() != nil {
				continue
			}
			leaderSummary, leaderStatus, _, leaderErr := clusterMembershipProcessGetSummary(ctx, leader.managementURL(), clusterMembershipProcessAdmin)
			if leaderErr != nil || leaderStatus != http.StatusOK || leaderSummary.LeaderNodeID == nil || *leaderSummary.LeaderNodeID != strconv.FormatUint(id, 10) {
				continue
			}
			leaderID = id
			return true, "", nil
		}
		return false, "no live process reports itself as the elected leader", nil
	})
	return leaderID, processes[leaderID-1].managementURL()
}

func clusterReleaseCompatReadVersions(ctx context.Context, process *clusterMembershipProcess) (map[string]string, error) {
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+"/api/v1/nodes", nil, clusterMembershipProcessAdmin, "")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("node list status=%d body=%s", status, body)
	}
	var envelope struct {
		Data []struct {
			NodeID  string `json:"nodeId"`
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode node list: %w", err)
	}
	versions := make(map[string]string, len(envelope.Data))
	for _, node := range envelope.Data {
		if node.NodeID == "" || node.Version == "" {
			return nil, fmt.Errorf("node list has incomplete identity: %+v", node)
		}
		if _, exists := versions[node.NodeID]; exists {
			return nil, fmt.Errorf("node list repeats node %q", node.NodeID)
		}
		versions[node.NodeID] = node.Version
	}
	return versions, nil
}

func clusterReleaseCompatWaitVersions(t *testing.T, ctx context.Context, process *clusterMembershipProcess, want map[string]string) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("committed release profiles %v", want), func() (bool, string, error) {
		versions, err := clusterReleaseCompatReadVersions(ctx, process)
		if err != nil {
			return false, err.Error(), nil
		}
		if clusterReleaseCompatVersionsEqual(versions, want) {
			return true, "", nil
		}
		return false, fmt.Sprintf("committed versions=%v want=%v", versions, want), nil
	})
}

func clusterReleaseCompatVersionsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for nodeID, version := range right {
		if left[nodeID] != version {
			return false
		}
	}
	return true
}

func clusterReleaseCompatLastApplied(t *testing.T, ctx context.Context, process *clusterMembershipProcess, nodeID uint64) uint64 {
	t.Helper()
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/v1/nodes/%d", process.managementURL(), nodeID), nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("read node %d detail status=%d body=%s err=%v", nodeID, status, body, err)
	}
	var envelope struct {
		Data struct {
			LastAppliedIndex uint64 `json:"lastAppliedIndex"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode node %d detail: %v body=%s", nodeID, err, body)
	}
	return envelope.Data.LastAppliedIndex
}

func clusterReleaseCompatFeatures(ctx context.Context, process *clusterMembershipProcess) (int, clusterReleaseCompatFeatureState, []byte, error) {
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+"/api/v1/cluster/features", nil, clusterMembershipProcessAdmin, "")
	if err != nil {
		return status, clusterReleaseCompatFeatureState{}, body, err
	}
	var envelope struct {
		Data clusterReleaseCompatFeatureState `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return status, clusterReleaseCompatFeatureState{}, body, fmt.Errorf("decode cluster feature state: %w", err)
	}
	return status, envelope.Data, body, nil
}

func clusterReleaseCompatDecodeFeatureState(t *testing.T, body []byte) clusterReleaseCompatFeatureState {
	t.Helper()
	var envelope struct {
		Data clusterReleaseCompatFeatureState `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode cluster feature state: %v body=%s", err, body)
	}
	return envelope.Data
}

func clusterReleaseCompatActivate(t *testing.T, ctx context.Context, process *clusterMembershipProcess) (int, []byte) {
	t.Helper()
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodPost,
		process.managementURL()+"/api/v1/cluster/features/compatibility-probe", nil, clusterMembershipProcessAdmin, "")
	if err != nil {
		t.Fatalf("activate compatibility-probe: %v", err)
	}
	return status, body
}

func clusterReleaseCompatAssertActivationFailure(t *testing.T, status int, body []byte, wantStatus int, wantCode string) {
	t.Helper()
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode feature activation error: %v body=%s", err, body)
	}
	if status != wantStatus || envelope.Error.Code != wantCode || envelope.Error.Retryable {
		t.Fatalf("feature activation=(%d,%+v), want (%d,%s) non-retryable", status, envelope.Error, wantStatus, wantCode)
	}
}

func clusterReleaseCompatMemberHasFeature(state clusterReleaseCompatFeatureState, nodeID uint64, feature string) bool {
	for _, member := range state.Members {
		if member.NodeID == fmt.Sprint(nodeID) && slices.Contains(member.Features, feature) {
			return true
		}
	}
	return false
}

func clusterReleaseCompatLoadUntilSnapshot(t *testing.T, ctx context.Context, fixture *clusterMembershipProcessFixture, processes []*clusterMembershipProcess, publishID uint64, runningIDs []uint64, followerApplied uint64) (string, uint64) {
	t.Helper()
	minimumExistingSnapshot := ^uint64(0)
	for _, nodeID := range runningIDs {
		latest := clusterSubscriptionReplayLatestSnapshotIndex(t, filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-data", nodeID)))
		if latest < minimumExistingSnapshot {
			minimumExistingSnapshot = latest
		}
	}
	threshold := followerApplied
	if minimumExistingSnapshot > threshold {
		threshold = minimumExistingSnapshot
	}
	connection := clusterDurableProcessConnect(t, fixture.mqttAddresses[publishID-1], fixture.accessToken, "compat-load", true)
	defer func() {
		if connection != nil {
			_ = connection.Close()
		}
	}()
	const topic = "compat/snapshot"
	var lastPayload string
	var snapshotIndex uint64
	for index := 1; index <= 3000; index++ {
		lastPayload = fmt.Sprintf("load-%d", index)
		packetID := uint16(index%65535 + 1)
		body := []byte{0, byte(len(topic))}
		body = append(body, topic...)
		body = append(body, byte(packetID>>8), byte(packetID))
		body = append(body, lastPayload...)
		for attempt := 0; attempt < 10; attempt++ {
			clusterMembershipProcessWaitForStatus(t, ctx, processes[publishID-1], "/health/ready", http.StatusOK)
			err := clusterDurableProcessWrite(connection, 0x33, body)
			if err == nil {
				err = clusterReleaseCompatAck(connection, packetID)
			}
			if err == nil {
				break
			}
			if !clusterMembershipProcessIsClosed(err) {
				t.Fatalf("snapshot load %d packet %d: %v", index, packetID, err)
			}
			_ = connection.Close()
			if attempt == 9 {
				t.Fatalf("snapshot load %d packet %d: connection closed before PUBACK after %d attempts: %v", index, packetID, attempt+1, err)
			}
			t.Logf("snapshot load %d packet %d connection closed before PUBACK; reconnecting", index, packetID)
			connection = clusterDurableProcessConnect(t, fixture.mqttAddresses[publishID-1], fixture.accessToken, "compat-load", true)
		}
		if index%100 != 0 {
			continue
		}
		for _, nodeID := range runningIDs {
			clusterMembershipProcessWaitForStatus(t, ctx, processes[nodeID-1], "/health/ready", http.StatusOK)
		}
		minimum := ^uint64(0)
		for _, nodeID := range runningIDs {
			latest := clusterSubscriptionReplayLatestSnapshotIndex(t, filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-data", nodeID)))
			if latest < minimum {
				minimum = latest
			}
		}
		if minimum > threshold {
			snapshotIndex = minimum
			break
		}
	}
	if snapshotIndex == 0 {
		var observations []string
		for _, nodeID := range runningIDs {
			latest := clusterSubscriptionReplayLatestSnapshotIndex(t, filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-data", nodeID)))
			applied := clusterReleaseCompatLastApplied(t, ctx, processes[nodeID-1], nodeID)
			observations = append(observations, fmt.Sprintf("node %d snapshot=%d applied=%d", nodeID, latest, applied))
		}
		t.Fatalf("retained load did not advance every running snapshot beyond threshold %d (follower applied %d): %s", threshold, followerApplied, strings.Join(observations, "; "))
	}
	if err := clusterDurableProcessWrite(connection, 0xE0, nil); err != nil {
		t.Fatalf("disconnect snapshot load client: %v", err)
	}
	return lastPayload, snapshotIndex
}
func clusterReleaseCompatAck(connection net.Conn, wantID uint16) error {
	_ = connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer connection.SetReadDeadline(time.Time{})
	kind, _, body, err := clusterDurableProcessReadRaw(connection)
	if err != nil {
		return err
	}
	if kind != 4 || len(body) < 2 || binary.BigEndian.Uint16(body[:2]) != wantID {
		return fmt.Errorf("retained-load PUBACK kind=%d body=%x, want kind=4 packet ID=%d", kind, body, wantID)
	}
	return nil
}

func clusterReleaseCompatAssertSnapshot(t *testing.T, ctx context.Context, fixture *clusterMembershipProcessFixture, process *clusterMembershipProcess, nodeID, previousApplied, expectedSnapshot uint64) {
	t.Helper()
	latest := clusterSubscriptionReplayLatestSnapshotIndex(t, filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-data", nodeID)))
	if previousApplied >= expectedSnapshot {
		t.Fatalf("node %d previous applied index=%d, want transferred snapshot boundary %d beyond pre-stop state", nodeID, previousApplied, expectedSnapshot)
	}
	if latest < expectedSnapshot {
		applied := clusterReleaseCompatLastApplied(t, ctx, process, nodeID)
		t.Fatalf("node %d installed snapshot=%d applied=%d previousApplied=%d, want transferred snapshot >= %d", nodeID, latest, applied, previousApplied, expectedSnapshot)
	}
}

func clusterReleaseCompatAssertRetained(t *testing.T, fixture *clusterMembershipProcessFixture, nodeID uint64, wantPayload string) {
	t.Helper()
	connection := clusterMembershipProcessOpenCleanSubscribedSession(t, fixture.mqttAddresses[nodeID-1], fixture.accessToken, "compat-retained-check", "compat/snapshot")
	defer connection.Close()
	retained := clusterMembershipProcessReadRetained(t, connection, 1)
	if retained["compat/snapshot"] != wantPayload {
		t.Fatalf("node %d retained snapshot payload=%q, want %q", nodeID, retained["compat/snapshot"], wantPayload)
	}
}

func clusterReleaseCompatDurableRoundTrip(t *testing.T, fixture *clusterMembershipProcessFixture, publishNode, subscribeNode uint64, wantPayload string) {
	t.Helper()
	publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[publishNode-1], fixture.accessToken, "compat-pub", true)
	clusterDurableProcessPublish(t, publisher, "compat/durable", 1, 1, wantPayload)
	clusterDurableProcessAck(t, publisher, 4, 1)
	clusterDurableProcessWriteFatal(t, publisher, 0xE0, nil)
	_ = publisher.Close()

	subscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[subscribeNode-1], fixture.accessToken, "compat-sub", false)
	defer subscriber.Close()
	if !clusterDurableProcessSessionPresent(t, subscriber) {
		t.Fatalf("compat-sub resumed on node %d with SessionPresent=false", subscribeNode)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		packet := clusterDurableProcessRead(t, subscriber)
		if packet.kind != 3 || packet.qos != 1 || packet.topic != "compat/durable" {
			t.Fatalf("durable compatibility delivery = %+v, want QoS1 PUBLISH on compat/durable", packet)
		}
		clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(packet.packetID >> 8), byte(packet.packetID)})
		if packet.payload == wantPayload {
			clusterDurableProcessWriteFatal(t, subscriber, 0xE0, nil)
			return
		}
	}
	t.Fatalf("compat-sub on node %d did not receive durable payload %q within five seconds", subscribeNode, wantPayload)
}

func clusterReleaseCompatWaitForStatus(t *testing.T, ctx context.Context, process *clusterMembershipProcess, wantStatus int, timeout time.Duration) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	clusterMembershipProcessWait(t, waitCtx, fmt.Sprintf("%s readiness status %d", process.name, wantStatus), func() (bool, string, error) {
		if err := process.isAlive(); err != nil {
			return false, "", err
		}
		status, body, err := clusterMembershipProcessRequest(waitCtx, http.MethodGet, process.managementURL()+"/health/ready", nil, "", "")
		if err != nil {
			return false, err.Error(), nil
		}
		if status == wantStatus {
			return true, "", nil
		}
		return false, fmt.Sprintf("status=%d body=%s", status, body), nil
	})
}

func clusterReleaseCompatWaitIncompatibleReason(t *testing.T, ctx context.Context, process *clusterMembershipProcess, timeout time.Duration) string {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var reason string
	clusterMembershipProcessWait(t, waitCtx, "incompatible release-profile readiness reason", func() (bool, string, error) {
		if err := process.isAlive(); err != nil {
			return false, "", err
		}
		status, body, err := clusterMembershipProcessRequest(waitCtx, http.MethodGet, process.managementURL()+"/health/ready", nil, "", "")
		if err != nil {
			return false, fmt.Sprintf("readiness error=%v", err), nil
		}
		if status != http.StatusServiceUnavailable {
			return false, fmt.Sprintf("status=%d body=%s", status, body), nil
		}
		var envelope struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			return false, fmt.Sprintf("decode readiness response: %v body=%s", err, body), nil
		}
		if !strings.HasPrefix(envelope.Error.Message, "incompatible_state_schema: ") {
			return false, fmt.Sprintf("reason=%q body=%s", envelope.Error.Message, body), nil
		}
		reason = envelope.Error.Message
		return true, "", nil
	})
	return reason
}

func clusterReleaseCompatSmallestNonLeader(leaderID uint64, nodeIDs []uint64) uint64 {
	var selected uint64
	for _, nodeID := range nodeIDs {
		if nodeID != leaderID && (selected == 0 || nodeID < selected) {
			selected = nodeID
		}
	}
	return selected
}

func clusterReleaseCompatSmallestVersionedNonLeader(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, leaderID uint64, version string) uint64 {
	t.Helper()
	versions, err := clusterReleaseCompatReadVersions(ctx, processes[leaderID-1])
	if err != nil {
		t.Fatalf("read node versions: %v", err)
	}
	var selected uint64
	for id := uint64(1); id <= uint64(len(processes)); id++ {
		if id != leaderID && versions[fmt.Sprint(id)] == version && (selected == 0 || id < selected) {
			selected = id
		}
	}
	if selected == 0 {
		t.Fatalf("no non-leader running release %s: leader=%d versions=%v", version, leaderID, versions)
	}
	return selected
}

func clusterReleaseCompatOnlyVersionID(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, version string) uint64 {
	t.Helper()
	leaderID, _ := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	versions, err := clusterReleaseCompatReadVersions(ctx, processes[leaderID-1])
	if err != nil {
		t.Fatalf("read node versions: %v", err)
	}
	var found uint64
	for id := uint64(1); id <= uint64(len(processes)); id++ {
		if versions[fmt.Sprint(id)] == version {
			if found != 0 {
				t.Fatalf("multiple nodes run release %s: versions=%v", version, versions)
			}
			found = id
		}
	}
	if found == 0 {
		t.Fatalf("no node runs release %s: versions=%v", version, versions)
	}
	return found
}

func clusterReleaseCompatOtherIDs(excluded uint64) []uint64 {
	ids := make([]uint64, 0, 2)
	for id := uint64(1); id <= 3; id++ {
		if id != excluded {
			ids = append(ids, id)
		}
	}
	return ids
}

func clusterReleaseCompatFirstOtherID(excluded uint64) uint64 {
	for id := uint64(1); id <= 3; id++ {
		if id != excluded {
			return id
		}
	}
	return 0
}

func clusterReleaseCompatVersionsFor(defaultVersion string, overrides map[uint64]string) map[string]string {
	versions := make(map[string]string, 3)
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		version := defaultVersion
		if override, exists := overrides[nodeID]; exists {
			version = override
		}
		versions[fmt.Sprint(nodeID)] = version
	}
	return versions
}
