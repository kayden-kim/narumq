package integration

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Traceability: CLU-14; ADR-0062; docs/contracts/raft-snapshot-stream-v1.md; cluster spec §11.37.
//
// A joiner needs a Raft snapshot larger than one peer frame (1 MiB). After feature activation
// the snapshot travels over the snapshot stream and the joiner becomes a voter with retained state.
func TestDynamicClusterSnapshotStreamProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 5, "")
	ctx := fixture.ctx
	bootstrap := make([]*clusterMembershipProcess, 3)
	for index := range bootstrap {
		id := uint64(index + 1)
		process, err := fixture.harness.start(fmt.Sprintf("bootstrap %d", id), fixture.bootstrapConfig(id), fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start bootstrap %d: %v", id, err)
		}
		bootstrap[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, bootstrap)

	// Three retained messages of 700,000 bytes each: the serialized snapshot is above 1 MiB.
	// The payload is a repeating text per topic so the digest identifies each one.
	const retainedBytes = 700_000
	want := make(map[string]string, 3)
	for index := range 3 {
		topic := fmt.Sprintf("retained/snapshot-stream/%d", index)
		payload := strings.Repeat(fmt.Sprintf("%d-block-", index), retainedBytes/8+1)[:retainedBytes]
		clusterMembershipProcessPublishRetained(t, fixture.mqttAddresses[index%3], fixture.accessToken, "retained-publisher", topic, []byte(payload))
		digest := sha256.Sum256([]byte(payload))
		want[topic] = hex.EncodeToString(digest[:])
	}

	byID := map[uint64]*clusterMembershipProcess{1: bootstrap[0], 2: bootstrap[1], 3: bootstrap[2]}
	stage := func(id uint64) string {
		t.Helper()
		_, leaderURL := clusterJoinerVolumeLossLeader(t, ctx, byID, 3)
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodPost, leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(id), clusterMembershipProcessAdmin, fmt.Sprintf("snapshot-stream-stage-%d", id))
		operation, decodeErr := clusterMembershipProcessDecodeOperation(body)
		if err != nil || decodeErr != nil || status != http.StatusAccepted || operation.MemberID != id || operation.Phase != "STAGED" {
			t.Fatalf("stage %d: status=%d body=%s err=%v decode=%v", id, status, body, err, decodeErr)
		}
		return operation.OperationID
	}

	clusterProcessActivateFeature(t, ctx, bootstrap, "snapshot-stream")
	operation := stage(5)
	joiner, err := fixture.harness.start("snapshot-stream joiner 5", fixture.joinerConfig(5, []uint64{1, 2, 3}), fixture.managementAddresses[4])
	if err != nil {
		t.Fatalf("start joiner 5: %v", err)
	}
	all := append(append([]*clusterMembershipProcess(nil), bootstrap...), joiner)
	clusterMembershipProcessWaitForOperation(t, ctx, bootstrap[0], operation, "SUCCEEDED")
	clusterMembershipProcessWaitForClusterSize(t, ctx, all, 4)
	clusterMembershipProcessAssertVotersAndRetired(t, ctx, all, []string{"1", "2", "3", "5"}, "")

	var started, completed int
	for _, node := range bootstrap {
		logs := node.logs.String()
		started += strings.Count(logs, "raft snapshot stream started")
		completed += strings.Count(logs, "raft snapshot stream completed")
		if strings.Contains(logs, "raft snapshot stream failed") {
			t.Logf("%s logged a failed attempt before the transfer succeeded (allowed: Raft retries)", node.name)
		}
	}
	if started == 0 || completed == 0 {
		t.Fatalf("leader logs show started=%d completed=%d snapshot streams, want at least one of each", started, completed)
	}
	if !strings.Contains(joiner.logs.String(), "raft snapshot stream received") {
		t.Fatalf("joiner did not log a received snapshot stream:\n%s", joiner.logs.String())
	}

	// The joiner serves the retained state that only the snapshot could have carried.
	subscriber := clusterMembershipProcessOpenCleanSubscribedSession(t, fixture.mqttAddresses[4], fixture.accessToken, "retained-subscriber-joiner", "retained/snapshot-stream/#")
	got := clusterMembershipProcessReadRetained(t, subscriber, len(want))
	_ = subscriber.Close()
	for topic, wantDigest := range want {
		digest := sha256.Sum256([]byte(got[topic]))
		if hex.EncodeToString(digest[:]) != wantDigest {
			t.Errorf("joiner retained %s: %d bytes, digest %x, want digest %s", topic, len(got[topic]), digest, wantDigest)
		}
	}
	// A restart replays the installed snapshot from the joiner's own volume.
	if err := joiner.crash(5 * time.Second); err != nil {
		t.Fatalf("crash joiner 5: %v", err)
	}
	joiner, err = fixture.harness.start("snapshot-stream joiner 5 restart", fixture.joinerConfig(5, []uint64{1, 2, 3}), fixture.managementAddresses[4])
	if err != nil {
		t.Fatalf("restart joiner 5: %v", err)
	}
	all[3] = joiner
	clusterMembershipProcessWaitForClusterSize(t, ctx, all, 4)
	t.Logf("snapshot stream: joiner 5 voter after >1 MiB snapshot; started=%d completed=%d; retained digests match", started, completed)
}
