package integration

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// Traceability: CLU-12; Cluster §4.2; ADR-0036; docs/specs/cluster-session-and-delivery.md §11.33.
//
// The ingress node's forwarded proposal is discarded after Raft emitted it, then the leader is killed.
// That is the state of a request that was sent to the old leader and never committed. Without
// `proposal-recovery` the ingress node must time out and close the MQTT connection. With it the same
// command is re-proposed to the new leader inside the original 5 s caller deadline and the connection stays.
func TestDynamicClusterProposalRecoveryLeaderKillInFlightCommit(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		recovery bool
		qos2     bool
	}{
		{name: "puback-complete/recovery-active", recovery: true},
		{name: "puback-complete/recovery-inactive"},
		{name: "pubrel-release/recovery-active", recovery: true, qos2: true},
		{name: "pubrel-release/recovery-inactive", qos2: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newClusterMembershipProcessFixture(t, 3, "narumq_failpoint")
			controlPaths := make([]string, 3)
			nodes := make([]*clusterMembershipProcess, 3)
			for id := uint64(1); id <= 3; id++ {
				controlPaths[id-1] = filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-fault.json", id))
				node, err := fixture.harness.launch(fmt.Sprintf("proposal recovery node %d", id),
					fixture.bootstrapConfig(id), fixture.managementAddresses[id-1],
					[]string{"NARUMQ_FAILPOINT_CONTROL=" + controlPaths[id-1]})
				if err != nil {
					t.Fatalf("start node %d: %v", id, err)
				}
				nodes[id-1] = node
			}
			clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
			for _, node := range nodes {
				clusterMembershipProcessWaitForStatus(t, fixture.ctx, node, "/health/ready", 200)
			}
			if testCase.recovery {
				clusterProcessActivateFeature(t, fixture.ctx, nodes, "proposal-recovery")
				for _, node := range nodes {
					clusterMembershipProcessWaitForStatus(t, fixture.ctx, node, "/health/ready", 200)
				}
			}

			leaderID, _ := clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
			ingressID := smallestNonLeaderID(leaderID, 3)
			otherID := uint64(0)
			for id := uint64(1); id <= 3; id++ {
				if id != leaderID && id != ingressID {
					otherID = id
				}
			}
			address := func(id uint64) string { return fixture.mqttAddresses[id-1] }
			const filter = "recovery/inflight/#"
			const topic = "recovery/inflight/one"

			if testCase.qos2 {
				clusterProposalRecoveryKillDuringRelease(t, fixture, nodes, controlPaths, leaderID, ingressID, address(otherID), address(ingressID), filter, topic, testCase.recovery)
			} else {
				clusterProposalRecoveryKillDuringComplete(t, fixture, nodes, controlPaths, leaderID, ingressID, address(otherID), address(ingressID), filter, topic, testCase.recovery)
			}
		})
	}
}

// The subscriber lives on the ingress node. Its PUBACK starts a COMPLETE commit that is lost with the leader.
func clusterProposalRecoveryKillDuringComplete(t *testing.T, fixture *clusterMembershipProcessFixture, nodes []*clusterMembershipProcess,
	controlPaths []string, leaderID, ingressID uint64, publisherAddress, subscriberAddress, filter, topic string, recovery bool) {
	t.Helper()
	subscriber := clusterDurableProcessConnect(t, subscriberAddress, fixture.accessToken, "recovery-sub", false)
	defer subscriber.Close()
	clusterDurableProcessSubscribe(t, subscriber, 1, filter, 1)
	clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, nodes, "acceptance", "recovery-sub|"+filter+"|1")
	publisher := clusterDurableProcessConnect(t, publisherAddress, fixture.accessToken, "recovery-pub", true)
	defer publisher.Close()

	clusterDurableProcessPublish(t, publisher, topic, 1, 11, "first")
	clusterDurableProcessAck(t, publisher, 4, 11)
	first := clusterDurableProcessRead(t, subscriber)
	if first.kind != 3 || first.payload != "first" || first.qos != 1 {
		t.Fatalf("subscriber first delivery = %+v, want QoS 1 PUBLISH first", first)
	}

	clusterProposalRecoveryWriteDropControl(t, controlPaths[ingressID-1], leaderID)
	clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(first.packetID >> 8), byte(first.packetID)})
	time.Sleep(500 * time.Millisecond)
	killedAt := time.Now()
	if err := nodes[leaderID-1].crash(10 * time.Second); err != nil {
		t.Fatalf("crash leader %d: %v", leaderID, err)
	}

	if !recovery {
		closedAfter, ok := clusterProposalRecoveryAwaitClose(subscriber, 12*time.Second)
		if !ok {
			t.Fatal("recovery inactive: subscriber connection stayed open although its COMPLETE commit was lost")
		}
		t.Logf("recovery inactive: subscriber closed %v after leader kill", closedAfter.Round(time.Millisecond))
		return
	}

	if closedAfter, closed := clusterProposalRecoveryAwaitClose(subscriber, 8*time.Second); closed {
		t.Fatalf("recovery active: subscriber connection closed %v after leader kill", closedAfter)
	}
	t.Logf("recovery active: subscriber connection stayed open %v after leader kill", time.Since(killedAt).Round(time.Millisecond))

	clusterDurableProcessPublish(t, publisher, topic, 1, 12, "second")
	clusterDurableProcessAck(t, publisher, 4, 12)
	second := clusterDurableProcessRead(t, subscriber)
	if second.kind != 3 || second.payload != "second" || second.dup {
		t.Fatalf("subscriber delivery after recovery = %+v, want only new PUBLISH second", second)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(second.packetID >> 8), byte(second.packetID)})
	clusterDurableProcessWriteFatal(t, subscriber, 0xE0, nil)
	_ = subscriber.Close()

	// A COMPLETE that was not committed would redeliver "first" with DUP after the session resumes.
	resumed := clusterDurableProcessConnect(t, subscriberAddress, fixture.accessToken, "recovery-sub", false)
	defer resumed.Close()
	if !clusterDurableProcessSessionPresent(t, resumed) {
		t.Fatal("persistent subscriber session was lost")
	}
	clusterProposalRecoveryAssertSilent(t, resumed, 2*time.Second, "redelivery after COMPLETE recovery")
}

// The publisher lives on the ingress node. Its PUBREL starts a RELEASE commit that is lost with the leader.
func clusterProposalRecoveryKillDuringRelease(t *testing.T, fixture *clusterMembershipProcessFixture, nodes []*clusterMembershipProcess,
	controlPaths []string, leaderID, ingressID uint64, subscriberAddress, publisherAddress, filter, topic string, recovery bool) {
	t.Helper()
	subscriber := clusterDurableProcessConnect(t, subscriberAddress, fixture.accessToken, "recovery-sub", false)
	defer subscriber.Close()
	clusterDurableProcessSubscribe(t, subscriber, 1, filter, 2)
	clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, nodes, "acceptance", "recovery-sub|"+filter+"|2")
	publisher := clusterDurableProcessConnect(t, publisherAddress, fixture.accessToken, "recovery-pub", false)
	defer publisher.Close()

	clusterDurableProcessPublish(t, publisher, topic, 2, 21, "released")
	clusterDurableProcessAck(t, publisher, 5, 21)

	clusterProposalRecoveryWriteDropControl(t, controlPaths[ingressID-1], leaderID)
	clusterDurableProcessPubrel(t, publisher, 21)
	time.Sleep(500 * time.Millisecond)
	killedAt := time.Now()
	if err := nodes[leaderID-1].crash(10 * time.Second); err != nil {
		t.Fatalf("crash leader %d: %v", leaderID, err)
	}

	if !recovery {
		deadline := time.Now().Add(12 * time.Second)
		for {
			_ = publisher.SetReadDeadline(deadline)
			kind, _, _, err := clusterDurableProcessReadRaw(publisher)
			if err != nil {
				if !clusterMembershipProcessIsClosed(err) {
					t.Fatalf("recovery inactive: publisher read error %v, want closed connection without PUBCOMP", err)
				}
				break
			}
			if kind == 7 {
				t.Fatal("recovery inactive: PUBCOMP arrived although the RELEASE commit was lost")
			}
		}
		t.Logf("recovery inactive: publisher closed %v after leader kill", time.Since(killedAt).Round(time.Millisecond))
		return
	}

	_ = publisher.SetReadDeadline(time.Now().Add(8 * time.Second))
	kind, _, body, err := clusterDurableProcessReadRaw(publisher)
	_ = publisher.SetReadDeadline(time.Time{})
	if err != nil || kind != 7 || len(body) < 2 || body[0] != 0 || body[1] != 21 {
		t.Fatalf("recovery active: publisher packet after PUBREL = kind %d body %x err %v, want PUBCOMP 21", kind, body, err)
	}
	t.Logf("recovery active: PUBCOMP %v after leader kill", time.Since(killedAt).Round(time.Millisecond))

	delivered := clusterDurableProcessRead(t, subscriber)
	if delivered.kind != 3 || delivered.payload != "released" || delivered.qos != 2 {
		t.Fatalf("subscriber delivery = %+v, want QoS 2 PUBLISH released", delivered)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x50, []byte{byte(delivered.packetID >> 8), byte(delivered.packetID)})
	release := clusterDurableProcessRead(t, subscriber)
	if release.kind != 6 || release.packetID != delivered.packetID {
		t.Fatalf("subscriber packet after PUBREC = %+v, want PUBREL %d", release, delivered.packetID)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x70, []byte{byte(release.packetID >> 8), byte(release.packetID)})
	clusterProposalRecoveryAssertSilent(t, subscriber, 2*time.Second, "second delivery of the released message")
}

func clusterProposalRecoveryWriteDropControl(t *testing.T, path string, leaderID uint64) {
	t.Helper()
	clusterFaultWriteControlJSON(t, path, []byte(fmt.Sprintf(`{"dropProposalPeers":[%d]}`, leaderID)))
}

// clusterProposalRecoveryAwaitClose waits for the peer to close the connection and reports how long that took.
// It returns false when the connection stays open for the whole window. Packets that arrive meanwhile are ignored.
func clusterProposalRecoveryAwaitClose(conn net.Conn, window time.Duration) (time.Duration, bool) {
	started := time.Now()
	_ = conn.SetReadDeadline(started.Add(window))
	defer conn.SetReadDeadline(time.Time{})
	for {
		_, _, _, err := clusterDurableProcessReadRaw(conn)
		if err == nil {
			continue
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return 0, false
		}
		return time.Since(started), true
	}
}

func clusterProposalRecoveryAssertSilent(t *testing.T, conn net.Conn, window time.Duration, what string) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(window))
	defer conn.SetReadDeadline(time.Time{})
	kind, _, body, err := clusterDurableProcessReadRaw(conn)
	if err == nil {
		t.Fatalf("unexpected %s: packet kind=%d body=%x", what, kind, body)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("connection failed while waiting for %s: %v", what, err)
	}
}
