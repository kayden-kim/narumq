package integration

import (
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Traceability: CLU-02/CLU-03/CLU-06/DEP-16; docs/specs/cluster-session-and-delivery.md §2.2, §7, §11.14.
func TestDynamicClusterPersistentSubscriptionReplayProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 4, "")
	ctx := fixture.ctx
	nodes := make([]*clusterMembershipProcess, 3)
	for index := range nodes {
		process, err := fixture.harness.start(
			fmt.Sprintf("subscription replay bootstrap node %d", index+1),
			fixture.bootstrapConfig(uint64(index+1)), fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", index+1, err)
		}
		nodes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)

	const (
		subscriberID = "replay-sub"
		plusFilter   = "replay/+/plus"
		exactFilter  = "replay/exact"
		goneFilter   = "replay/gone/#"
		lateFilter   = "replay/late"
	)
	phaseASubscriptions := []string{
		subscriberID + "|" + plusFilter + "|2",
		subscriberID + "|" + exactFilter + "|1",
	}

	// Phase A: persist subscriptions and an unsubscribe, then detach the session.
	subscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, subscriberID, false)
	defer subscriber.Close()
	clusterDurableProcessSubscribe(t, subscriber, 1, exactFilter, 1)
	clusterDurableProcessSubscribe(t, subscriber, 2, plusFilter, 2)
	clusterDurableProcessSubscribe(t, subscriber, 3, goneFilter, 1)
	clusterDurableProcessUnsubscribe(t, subscriber, 4, goneFilter)
	clusterDurableProcessWaitForSubscriptions(t, ctx, nodes, "acceptance", phaseASubscriptions...)
	clusterDurableProcessWriteFatal(t, subscriber, 0xE0, nil)
	clusterMembershipProcessAssertConnectionClosed(t, subscriber, 2*time.Second)
	_ = subscriber.Close()

	// Phase B: crash every process and replay the committed WAL on restart.
	for index, process := range nodes {
		if err := process.crash(5 * time.Second); err != nil {
			t.Fatalf("crash bootstrap node %d: %v", index+1, err)
		}
	}
	for index := range nodes {
		process, err := fixture.harness.start(
			fmt.Sprintf("subscription replay bootstrap node %d restart", index+1),
			fixture.bootstrapConfig(uint64(index+1)), fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("restart bootstrap node %d: %v", index+1, err)
		}
		nodes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	clusterDurableProcessWaitForSubscriptions(t, ctx, nodes, "acceptance", phaseASubscriptions...)

	publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "replay-pub-1", true)
	defer publisher.Close()
	clusterDurableProcessPublish(t, publisher, exactFilter, 1, 10, "exact-1")
	clusterDurableProcessAck(t, publisher, 4, 10)
	clusterDurableProcessPublish(t, publisher, "replay/a/plus", 2, 20, "plus-1")
	clusterDurableProcessAck(t, publisher, 5, 20)
	clusterDurableProcessPubrel(t, publisher, 20)
	clusterDurableProcessAck(t, publisher, 7, 20)
	clusterDurableProcessPublish(t, publisher, "replay/gone/x", 1, 30, "gone-1")
	clusterDurableProcessAck(t, publisher, 4, 30)

	subscriber = clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, subscriberID, false)
	defer subscriber.Close()
	if !clusterDurableProcessSessionPresent(t, subscriber) {
		t.Fatal("subscription replay reconnect returned SessionPresent=false")
	}
	exact := clusterDurableProcessRead(t, subscriber)
	if exact.kind != 3 || exact.qos != 1 || exact.topic != exactFilter || exact.payload != "exact-1" {
		t.Fatalf("replayed exact delivery = %+v, want QoS1 exact-1", exact)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(exact.packetID >> 8), byte(exact.packetID)})
	plus := clusterDurableProcessRead(t, subscriber)
	if plus.kind != 3 || plus.qos != 2 || plus.topic != "replay/a/plus" || plus.payload != "plus-1" {
		t.Fatalf("replayed wildcard delivery = %+v, want QoS2 plus-1", plus)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x50, []byte{byte(plus.packetID >> 8), byte(plus.packetID)})
	clusterDurableProcessAck(t, subscriber, 6, plus.packetID)
	clusterDurableProcessWriteFatal(t, subscriber, 0x70, []byte{byte(plus.packetID >> 8), byte(plus.packetID)})
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	clusterDurableProcessPublish(t, publisher, exactFilter, 1, 11, "sentinel-1")
	clusterDurableProcessAck(t, publisher, 4, 11)
	sentinel := clusterDurableProcessRead(t, subscriber)
	if sentinel.kind != 3 || sentinel.qos != 1 || sentinel.topic != exactFilter || sentinel.payload != "sentinel-1" {
		t.Fatalf("post-replay sentinel = %+v, want QoS1 sentinel-1 with no stale gone-1 delivery", sentinel)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(sentinel.packetID >> 8), byte(sentinel.packetID)})

	// Phase C: a joining node must expose its complete route projection on first readiness.
	preJoin := make([]uint64, 3)
	for index := range nodes {
		dataDir := filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-data", index+1))
		preJoin[index] = clusterSubscriptionReplayLatestSnapshotIndex(t, dataDir)
	}
	_, leaderURL := clusterMembershipProcessFindLeader(t, ctx, nodes, 3)
	stageStatus, stageBody, err := clusterMembershipProcessRequest(ctx, http.MethodPost,
		leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(4), clusterMembershipProcessAdmin,
		"subscription-replay-join-node-4")
	if err != nil {
		t.Fatalf("stage subscription replay joiner: %v", err)
	}
	operation, err := clusterMembershipProcessDecodeOperation(stageBody)
	if err != nil || stageStatus != http.StatusAccepted || operation.Action != "ADD" || operation.Phase != "STAGED" {
		t.Fatalf("stage subscription replay joiner status=%d operation=%+v err=%v body=%s, want 202 ADD/STAGED",
			stageStatus, operation, err, stageBody)
	}
	joinerConfig := fixture.joinerConfig(4, []uint64{1, 2, 3})
	joiner, err := fixture.harness.start("subscription replay joiner node 4", joinerConfig, fixture.managementAddresses[3])
	if err != nil {
		t.Fatalf("start subscription replay joiner: %v", err)
	}
	clusterMembershipProcessWait(t, ctx, "joiner first readiness with complete projection", func() (bool, string, error) {
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, joiner.managementURL()+"/health/ready", nil, "", "")
		if err != nil {
			return false, "joiner readiness request: " + err.Error(), nil
		}
		if status != http.StatusOK {
			return false, fmt.Sprintf("joiner readiness status=%d body=%s", status, body), nil
		}
		got, err := clusterDurableProcessListSubscriptions(ctx, joiner, "acceptance")
		if err != nil {
			return false, "list joiner subscriptions at first readiness: " + err.Error(), err
		}
		if !slices.Equal(got, phaseASubscriptions) {
			return false, fmt.Sprintf("first-ready joiner subscriptions = %v, want %v", got, phaseASubscriptions),
				fmt.Errorf("joiner became ready before its subscription projection was complete")
		}
		return true, "joiner ready with complete subscription projection", nil
	})
	clusterMembershipProcessWaitForOperation(t, ctx, nodes[0], operation.OperationID, "SUCCEEDED")
	allNodes := append(append([]*clusterMembershipProcess(nil), nodes...), joiner)
	clusterMembershipProcessWaitForClusterSize(t, ctx, allNodes, 4)

	joinerPublisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[3], fixture.accessToken, "replay-pub-4", true)
	defer joinerPublisher.Close()
	clusterDurableProcessPublish(t, joinerPublisher, exactFilter, 1, 40, "via-joiner")
	clusterDurableProcessAck(t, joinerPublisher, 4, 40)
	viaJoiner := clusterDurableProcessRead(t, subscriber)
	if viaJoiner.kind != 3 || viaJoiner.qos != 1 || viaJoiner.topic != exactFilter || viaJoiner.payload != "via-joiner" {
		t.Fatalf("joiner ingress delivery = %+v, want QoS1 via-joiner", viaJoiner)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(viaJoiner.packetID >> 8), byte(viaJoiner.packetID)})

	postJoin := make([]uint64, 3)
	for index := range nodes {
		dataDir := filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-data", index+1))
		postJoin[index] = clusterSubscriptionReplayLatestSnapshotIndex(t, dataDir)
		if postJoin[index] <= preJoin[index] {
			t.Fatalf("node %d snapshot index after join = %d, want greater than pre-join index %d",
				index+1, postJoin[index], preJoin[index])
		}
	}

	clusterMembershipProcessWaitForClusterSize(t, ctx, allNodes, 4)
	readyStatus, readyBody, readyErr := clusterMembershipProcessRequest(ctx, http.MethodGet,
		nodes[0].managementURL()+"/health/ready", nil, "", "")
	if readyErr != nil || readyStatus != http.StatusOK {
		t.Fatalf("node 1 readiness before WAL-tail PUBLISH status=%d body=%s err=%v", readyStatus, readyBody, readyErr)
	}
	// Phase D: leave SEND and a new subscription in the WAL tail after the join snapshot.
	clusterDurableProcessPublish(t, publisher, exactFilter, 1, 12, "inflight-1")
	clusterDurableProcessAck(t, publisher, 4, 12)
	inflight := clusterDurableProcessRead(t, subscriber)
	if inflight.kind != 3 || inflight.qos != 1 || inflight.packetID == 0 || inflight.topic != exactFilter || inflight.payload != "inflight-1" {
		t.Fatalf("unacknowledged delivery before crash = %+v, want QoS1 inflight-1", inflight)
	}
	clusterDurableProcessSubscribe(t, subscriber, 5, lateFilter, 1)
	allNodes = append([]*clusterMembershipProcess(nil), nodes...)
	allNodes = append(allNodes, joiner)
	phaseDSubscriptions := []string{
		subscriberID + "|" + plusFilter + "|2",
		subscriberID + "|" + exactFilter + "|1",
		subscriberID + "|" + lateFilter + "|1",
	}
	clusterDurableProcessWaitForSubscriptions(t, ctx, allNodes, "acceptance", phaseDSubscriptions...)
	for index := range nodes {
		dataDir := filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-data", index+1))
		got := clusterSubscriptionReplayLatestSnapshotIndex(t, dataDir)
		if got != postJoin[index] {
			t.Fatalf("node %d snapshot index after WAL-tail mutations = %d, want unchanged %d",
				index+1, got, postJoin[index])
		}
	}

	for index, process := range allNodes {
		if err := process.crash(5 * time.Second); err != nil {
			t.Fatalf("crash cluster node %d before WAL-tail replay: %v", index+1, err)
		}
	}
	allNodes = make([]*clusterMembershipProcess, 4)
	for index := range 3 {
		process, err := fixture.harness.start(
			fmt.Sprintf("subscription replay bootstrap node %d second restart", index+1),
			fixture.bootstrapConfig(uint64(index+1)), fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("restart bootstrap node %d after WAL-tail mutations: %v", index+1, err)
		}
		allNodes[index] = process
	}
	joiner, err = fixture.harness.start("subscription replay joiner node 4 restart", joinerConfig, fixture.managementAddresses[3])
	if err != nil {
		t.Fatalf("restart joiner after WAL-tail mutations: %v", err)
	}
	allNodes[3] = joiner
	clusterMembershipProcessWaitForClusterSize(t, ctx, allNodes, 4)
	clusterDurableProcessWaitForSubscriptions(t, ctx, allNodes, "acceptance", phaseDSubscriptions...)

	_ = subscriber.Close()
	subscriber = clusterDurableProcessConnect(t, fixture.mqttAddresses[3], fixture.accessToken, subscriberID, false)
	defer subscriber.Close()
	if !clusterDurableProcessSessionPresent(t, subscriber) {
		t.Fatal("WAL-tail replay reconnect returned SessionPresent=false")
	}
	retried := clusterDurableProcessRead(t, subscriber)
	if retried.kind != 3 || retried.qos != 1 || !retried.dup || retried.packetID != inflight.packetID ||
		retried.topic != inflight.topic || retried.payload != "inflight-1" {
		t.Fatalf("WAL-tail inflight replay = %+v, want DUP PUBLISH with packet ID %d and payload inflight-1",
			retried, inflight.packetID)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(retried.packetID >> 8), byte(retried.packetID)})

	latePublisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "replay-pub-2", true)
	defer latePublisher.Close()
	clusterDurableProcessPublish(t, latePublisher, lateFilter, 1, 50, "late-1")
	clusterDurableProcessAck(t, latePublisher, 4, 50)
	late := clusterDurableProcessRead(t, subscriber)
	if late.kind != 3 || late.qos != 1 || late.topic != lateFilter || late.payload != "late-1" {
		t.Fatalf("WAL-tail subscription delivery = %+v, want QoS1 late-1", late)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(late.packetID >> 8), byte(late.packetID)})
}

func clusterSubscriptionReplayLatestSnapshotIndex(t *testing.T, dataDir string) uint64 {
	t.Helper()
	const (
		prefix     = "snapshot-"
		indexWidth = 20
		suffix     = ".bin"
	)
	var latest uint64
	err := filepath.WalkDir(dataDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) ||
			len(name) != len(prefix)+indexWidth+len(suffix) {
			return nil
		}
		index, err := clusterSubscriptionReplayParseSnapshotIndex(name[len(prefix) : len(prefix)+indexWidth])
		if err != nil {
			return fmt.Errorf("parse snapshot index from %q: %w", path, err)
		}
		if index > latest {
			latest = index
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk snapshots in %s: %v", dataDir, err)
	}
	return latest
}

func clusterSubscriptionReplayParseSnapshotIndex(indexText string) (uint64, error) {
	const indexWidth = 20
	if len(indexText) != indexWidth {
		return 0, fmt.Errorf("snapshot index has %d bytes, want %d", len(indexText), indexWidth)
	}
	for offset := range len(indexText) {
		if indexText[offset] < '0' || indexText[offset] > '9' {
			return 0, fmt.Errorf("snapshot index %q is not decimal", indexText)
		}
	}
	return strconv.ParseUint(indexText, 10, 64)
}

func TestClusterSubscriptionReplaySnapshotIndexRejectsNonDecimalDigits(t *testing.T) {
	for _, indexText := range []string{"+0000000000000000001", "0000000000000000000x"} {
		if _, err := clusterSubscriptionReplayParseSnapshotIndex(indexText); err == nil {
			t.Errorf("parse snapshot index %q succeeded; want non-decimal error", indexText)
		}
	}
}
