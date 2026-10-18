package integration

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Traceability: CLU-06/CLU-07/CLU-09; docs/specs/cluster-session-and-delivery.md §11.13; docs/decisions/ADR-0003-durable-ack-barrier.md §9.
// Acceptance: NARUMQ_DURABLE_ACK_FAULTS
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-081
func TestDynamicClusterDurableAckReplicaKillProcessAcceptance(t *testing.T) {
	clusterDurableFaultRequireOptIn(t)
	fixture, nodes := clusterDurableProcessStart(t)
	clusterDurableFaultPrepareSubscriber(t, fixture, nodes)

	leaderKills := map[byte]bool{1: false, 2: false}
	followerKills := map[byte]bool{1: false, 2: false}
	index := 0
	for _, qos := range []byte{1, 2} {
		victims := []uint64{1, 2, 3}
		for len(victims) > 0 {
			leaderID, _ := clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
			victimIndex := -1
			switch {
			case !leaderKills[qos]:
				for candidateIndex, candidateID := range victims {
					if candidateID == leaderID {
						victimIndex = candidateIndex
						break
					}
				}
			case !followerKills[qos]:
				for candidateIndex, candidateID := range victims {
					if candidateID != leaderID {
						victimIndex = candidateIndex
						break
					}
				}
			default:
				victimIndex = 0
			}
			if victimIndex < 0 {
				t.Fatalf("QoS %d cannot select remaining replica for leader=%d victims=%v", qos, leaderID, victims)
			}
			victimID := victims[victimIndex]
			victims = append(victims[:victimIndex], victims[victimIndex+1:]...)
			role := "follower"
			if victimID == leaderID {
				role = "leader"
				leaderKills[qos] = true
			} else {
				followerKills[qos] = true
			}
			clientID := "durable-fault-pub-q1"
			clean := true
			if qos == 2 {
				clientID = "durable-fault-pub-q2"
				clean = false
			}
			topic := fmt.Sprintf("durable-fault/kill/qos%d/node%d", qos, victimID)
			packetID := uint16(200 + index)
			index++
			publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[victimID-1], fixture.accessToken, clientID, clean)
			clusterDurableProcessPublish(t, publisher, topic, qos, packetID, topic)
			if !clusterDurableFaultAwaitAck(t, publisher, map[byte]byte{1: 4, 2: 5}[qos], packetID, 15*time.Second) {
				t.Fatalf("QoS %d %s publish to node %d did not receive its success ACK", qos, role, victimID)
			}
			if err := nodes[victimID-1].crash(5 * time.Second); err != nil {
				t.Fatalf("crash node %d immediately after QoS %d ACK: %v", victimID, qos, err)
			}
			_ = publisher.Close()
			if qos == 2 {
				liveID := smallestLiveClusterNodeID(nodes, victimID)
				clusterDurableFaultPublishQoS2Complete(t, fixture.mqttAddresses[liveID-1], fixture.accessToken,
					clientID, packetID, topic, topic)
			}

			liveID := smallestLiveClusterNodeID(nodes, victimID)
			counts := clusterDurableFaultCollect(t, fixture.mqttAddresses[liveID-1], fixture.accessToken,
				fixture.mqttAddresses[liveID-1], "durable-fault/sentinel/kill-"+strconv.Itoa(index))
			clusterDurableFaultAssertTopicSet(t, counts, topic)
			if counts[topic] != 1 {
				t.Fatalf("QoS %d ACKed topic %q delivery count = %d, want exactly 1 (role=%s)", qos, topic, counts[topic], role)
			}

			restarted, err := fixture.harness.start(fmt.Sprintf("durable ACK replica-kill node %d restart", victimID),
				fixture.bootstrapConfig(victimID), fixture.managementAddresses[victimID-1])
			if err != nil {
				t.Fatalf("restart node %d: %v", victimID, err)
			}
			nodes[victimID-1] = restarted
			clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
		}
	}
	for _, qos := range []byte{1, 2} {
		if !leaderKills[qos] || !followerKills[qos] {
			t.Fatalf("QoS %d replica-kill roles: leader=%t follower=%t, want both", qos, leaderKills[qos], followerKills[qos])
		}
	}
}

// Traceability: CLU-06/CLU-07/CLU-09; docs/specs/cluster-session-and-delivery.md §11.13; docs/decisions/ADR-0003-durable-ack-barrier.md §9.
// Acceptance: NARUMQ_DURABLE_ACK_FAULTS
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-082
func TestDynamicClusterDurableAckCrashPointProcessAcceptance(t *testing.T) {
	clusterDurableFaultRequireOptIn(t)
	fixture := newClusterMembershipProcessFixture(t, 3, "narumq_failpoint")
	controlPaths := make([]string, 3)
	nodes := make([]*clusterMembershipProcess, 3)
	for id := uint64(1); id <= 3; id++ {
		controlPaths[id-1] = filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-fault.json", id))
		node, err := fixture.harness.launch(fmt.Sprintf("durable ACK crash-point node %d", id),
			fixture.bootstrapConfig(id), fixture.managementAddresses[id-1],
			[]string{"NARUMQ_FAILPOINT_CONTROL=" + controlPaths[id-1]})
		if err != nil {
			t.Fatalf("start node %d: %v", id, err)
		}
		nodes[id-1] = node
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
	clusterDurableFaultPrepareSubscriber(t, fixture, nodes)

	points := []string{
		"delivery-wal-before-write",
		"delivery-wal-before-sync",
		"delivery-wal-after-sync",
		"delivery-commit-before-apply",
		"delivery-commit-after-apply",
	}
	roles := []string{"follower", "leader"}
	index := 0
	for _, point := range points {
		for _, role := range roles {
			for _, qos := range []byte{1, 2} {
				point, role, qos, subtestIndex := point, role, qos, index
				index++
				name := fmt.Sprintf("%s/%s/qos%d", point, role, qos)
				if !t.Run(name, func(t *testing.T) {
					clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
					leaderID, _ := clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
					targetID, ingressID := leaderID, leaderID
					if role == "follower" {
						targetID = smallestNonLeaderID(leaderID, 3)
					} else {
						ingressID = smallestNonLeaderID(leaderID, 3)
					}

					clientID := "durable-fault-pub-q1"
					clean := true
					if qos == 2 {
						clientID = "durable-fault-pub-q2"
						clean = false
					}
					topic := fmt.Sprintf("durable-fault/crash/%s/%s/qos%d", point, role, qos)
					packetID := uint16(100 + subtestIndex)
					clusterDurableFaultWriteControl(t, controlPaths[targetID-1], point, nil)
					publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[ingressID-1], fixture.accessToken, clientID, clean)
					clusterDurableProcessPublish(t, publisher, topic, qos, packetID, topic)
					acked := clusterDurableFaultAwaitAck(t, publisher, map[byte]byte{1: 4, 2: 5}[qos], packetID, 15*time.Second)
					clusterMembershipProcessRequireFailpointExit(t, nodes[targetID-1], point)
					clusterDurableFaultClearControl(t, controlPaths[targetID-1])
					_ = publisher.Close()

					restarted, err := fixture.harness.launch(fmt.Sprintf("durable ACK crash-point node %d restart", targetID),
						fixture.bootstrapConfig(targetID), fixture.managementAddresses[targetID-1],
						[]string{"NARUMQ_FAILPOINT_CONTROL=" + controlPaths[targetID-1]})
					if err != nil {
						t.Fatalf("restart node %d: %v", targetID, err)
					}
					nodes[targetID-1] = restarted
					clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
					if qos == 2 {
						clusterDurableFaultPublishQoS2Complete(t, fixture.mqttAddresses[ingressID-1], fixture.accessToken,
							clientID, packetID, topic, topic)
					}

					leaderID, _ = clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
					counts := clusterDurableFaultCollect(t, fixture.mqttAddresses[targetID-1], fixture.accessToken,
						fixture.mqttAddresses[leaderID-1], fmt.Sprintf("durable-fault/sentinel/crash-%d", subtestIndex))
					clusterDurableFaultAssertTopicSet(t, counts, topic)
					count := counts[topic]
					if role == "follower" {
						if !acked || count != 1 {
							t.Fatalf("follower %s qos%d target=%d ingress=%d acked=%t count=%d; want ACK and exactly one delivery",
								point, qos, targetID, ingressID, acked, count)
						}
					} else if point == "delivery-wal-before-write" || point == "delivery-wal-before-sync" || point == "delivery-wal-after-sync" {
						if acked {
							t.Fatalf("leader %s qos%d target=%d ingress=%d received success ACK before WAL send", point, qos, targetID, ingressID)
						}
						if qos == 2 && count != 1 || qos == 1 && count > 1 {
							t.Fatalf("leader %s qos%d delivery count=%d; want QoS2 exactly one, QoS1 at most one", point, qos, count)
						}
					} else if qos == 2 && count != 1 || qos == 1 && acked && count != 1 || qos == 1 && !acked && count > 1 {
						t.Fatalf("leader %s qos%d acked=%t delivery count=%d violates durable delivery expectation", point, qos, acked, count)
					}
					t.Logf("point=%s role=%s qos=%d target=%d ingress=%d acked=%t count=%d", point, role, qos, targetID, ingressID, acked, count)
				}) {
					t.FailNow()
				}
			}
		}
	}
}

// Traceability: CLU-06/CLU-07/CLU-09; docs/specs/cluster-session-and-delivery.md §11.13; docs/decisions/ADR-0003-durable-ack-barrier.md §9.
// Acceptance: NARUMQ_DURABLE_ACK_FAULTS
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-083, cluster-session-and-delivery.md:CLU-SCN-193
func TestDynamicClusterDurableAckPartitionProcessAcceptance(t *testing.T) {
	clusterDurableFaultRequireOptIn(t)
	fixture := newClusterMembershipProcessFixture(t, 3, "narumq_failpoint")
	controlPaths := make([]string, 3)
	nodes := make([]*clusterMembershipProcess, 3)
	for id := uint64(1); id <= 3; id++ {
		controlPaths[id-1] = filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-fault.json", id))
		node, err := fixture.harness.launch(fmt.Sprintf("durable ACK partition node %d", id),
			fixture.bootstrapConfig(id), fixture.managementAddresses[id-1],
			[]string{"NARUMQ_FAILPOINT_CONTROL=" + controlPaths[id-1]})
		if err != nil {
			t.Fatalf("start node %d: %v", id, err)
		}
		nodes[id-1] = node
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
	clusterDurableFaultPrepareSubscriber(t, fixture, nodes)

	if !t.Run("follower-isolated", func(t *testing.T) {
		leaderID, _ := clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
		followerID := smallestNonLeaderID(leaderID, 3)
		qos1 := clusterDurableProcessConnect(t, fixture.mqttAddresses[followerID-1], fixture.accessToken, "durable-fault-isolated-q1", true)
		qos2 := clusterDurableProcessConnect(t, fixture.mqttAddresses[followerID-1], fixture.accessToken, "durable-fault-isolated-q2", false)
		defer qos1.Close()
		defer qos2.Close()

		peers := otherClusterNodeIDs(followerID, 3)
		clusterDurableFaultWriteControl(t, controlPaths[followerID-1], "", peers)
		isolatedQ1Topic := "durable-fault/partition/follower-isolated/isolated-q1"
		isolatedQ2Topic := "durable-fault/partition/follower-isolated/isolated-q2"
		clusterDurableProcessPublish(t, qos1, isolatedQ1Topic, 1, 401, isolatedQ1Topic)
		if clusterDurableFaultAwaitAck(t, qos1, 4, 401, 15*time.Second) {
			t.Fatal("isolated follower QoS1 publish received PUBACK")
		}
		clusterDurableProcessPublish(t, qos2, isolatedQ2Topic, 2, 402, isolatedQ2Topic)
		if clusterDurableFaultAwaitAck(t, qos2, 5, 402, 15*time.Second) {
			t.Fatal("isolated follower QoS2 publish received PUBREC")
		}

		majorityQ1Topic := "durable-fault/partition/follower-isolated/majority-q1"
		majorityQ2Topic := "durable-fault/partition/follower-isolated/majority-q2"
		majorityQ1 := clusterDurableProcessConnect(t, fixture.mqttAddresses[leaderID-1], fixture.accessToken, "durable-fault-pub-q1", true)
		defer majorityQ1.Close()
		clusterDurableProcessPublish(t, majorityQ1, majorityQ1Topic, 1, 411, majorityQ1Topic)
		if !clusterDurableFaultAwaitAck(t, majorityQ1, 4, 411, 15*time.Second) {
			t.Fatal("majority-side QoS1 publish did not receive PUBACK")
		}
		majorityQ2 := clusterDurableProcessConnect(t, fixture.mqttAddresses[leaderID-1], fixture.accessToken, "durable-fault-pub-q2", false)
		defer majorityQ2.Close()
		clusterDurableProcessPublish(t, majorityQ2, majorityQ2Topic, 2, 412, majorityQ2Topic)
		if !clusterDurableFaultAwaitAck(t, majorityQ2, 5, 412, 15*time.Second) {
			t.Fatal("majority-side QoS2 publish did not receive PUBREC")
		}
		clusterDurableProcessPubrel(t, majorityQ2, 412)
		if !clusterDurableFaultAwaitAck(t, majorityQ2, 7, 412, 15*time.Second) {
			t.Fatal("majority-side QoS2 publish did not receive PUBCOMP")
		}

		clusterDurableFaultClearControl(t, controlPaths[followerID-1])
		clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
		_ = qos2.Close()
		clusterDurableFaultPublishQoS2Complete(t, fixture.mqttAddresses[followerID-1], fixture.accessToken,
			"durable-fault-isolated-q2", 402, isolatedQ2Topic, isolatedQ2Topic)
		counts := clusterDurableFaultCollect(t, fixture.mqttAddresses[followerID-1], fixture.accessToken,
			fixture.mqttAddresses[leaderID-1], "durable-fault/sentinel/partition-follower")
		clusterDurableFaultAssertTopicSet(t, counts, isolatedQ1Topic, isolatedQ2Topic, majorityQ1Topic, majorityQ2Topic)
		if counts[majorityQ1Topic] != 1 || counts[majorityQ2Topic] != 1 || counts[isolatedQ2Topic] != 1 || counts[isolatedQ1Topic] > 1 {
			t.Fatalf("follower-isolated delivery counts = %v", counts)
		}
		t.Logf("follower-isolated leader=%d target=%d counts=%v", leaderID, followerID, counts)
	}) {
		t.FailNow()
	}

	if !t.Run("leader-isolated", func(t *testing.T) {
		leaderID, _ := clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
		qos1 := clusterDurableProcessConnect(t, fixture.mqttAddresses[leaderID-1], fixture.accessToken, "durable-fault-isolated-q1", true)
		qos2 := clusterDurableProcessConnect(t, fixture.mqttAddresses[leaderID-1], fixture.accessToken, "durable-fault-isolated-q2", false)
		defer qos1.Close()
		defer qos2.Close()
		clusterDurableFaultWriteControl(t, controlPaths[leaderID-1], "", otherClusterNodeIDs(leaderID, 3))

		isolatedQ1Topic := "durable-fault/partition/leader-isolated/isolated-q1"
		isolatedQ2Topic := "durable-fault/partition/leader-isolated/isolated-q2"
		clusterDurableProcessPublish(t, qos1, isolatedQ1Topic, 1, 501, isolatedQ1Topic)
		if clusterDurableFaultAwaitAck(t, qos1, 4, 501, 15*time.Second) {
			t.Fatal("isolated leader QoS1 publish received PUBACK")
		}
		clusterDurableProcessPublish(t, qos2, isolatedQ2Topic, 2, 502, isolatedQ2Topic)
		if clusterDurableFaultAwaitAck(t, qos2, 5, 502, 15*time.Second) {
			t.Fatal("isolated leader QoS2 publish received PUBREC")
		}
		_ = qos2.Close()

		var newLeaderID uint64
		clusterMembershipProcessWait(t, fixture.ctx, "isolated leader to step down and majority nodes to agree", func() (bool, string, error) {
			var agreedLeader string
			for _, peerID := range otherClusterNodeIDs(leaderID, 3) {
				summary, status, body, err := clusterMembershipProcessGetSummary(fixture.ctx,
					nodes[peerID-1].managementURL(), clusterMembershipProcessAdmin)
				if err != nil || status != http.StatusOK || summary.LeaderNodeID == nil {
					return false, fmt.Sprintf("node %d leader unavailable: status=%d body=%s err=%v", peerID, status, body, err), nil
				}
				if *summary.LeaderNodeID == strconv.FormatUint(leaderID, 10) {
					return false, fmt.Sprintf("isolated node %d still reports itself as leader", leaderID), nil
				}
				if agreedLeader == "" {
					agreedLeader = *summary.LeaderNodeID
				} else if agreedLeader != *summary.LeaderNodeID {
					return false, fmt.Sprintf("majority leader views disagree: %s and %s", agreedLeader, *summary.LeaderNodeID), nil
				}
			}
			parsed, err := strconv.ParseUint(agreedLeader, 10, 64)
			if err != nil || parsed == 0 || parsed > 3 {
				return false, fmt.Sprintf("majority reported invalid leader %q", agreedLeader), nil
			}
			newLeaderID = parsed
			return true, "majority nodes agree on a new leader", nil
		})

		majorityQ1Topic := "durable-fault/partition/leader-isolated/majority-q1"
		majorityQ2Topic := "durable-fault/partition/leader-isolated/majority-q2"
		majorityQ1 := clusterDurableProcessConnect(t, fixture.mqttAddresses[newLeaderID-1], fixture.accessToken, "durable-fault-pub-q1", true)
		defer majorityQ1.Close()
		clusterDurableProcessPublish(t, majorityQ1, majorityQ1Topic, 1, 511, majorityQ1Topic)
		if !clusterDurableFaultAwaitAck(t, majorityQ1, 4, 511, 15*time.Second) {
			t.Fatal("majority QoS1 publish did not receive PUBACK")
		}
		majorityQ2 := clusterDurableProcessConnect(t, fixture.mqttAddresses[newLeaderID-1], fixture.accessToken, "durable-fault-pub-q2", false)
		defer majorityQ2.Close()
		clusterDurableProcessPublish(t, majorityQ2, majorityQ2Topic, 2, 512, majorityQ2Topic)
		if !clusterDurableFaultAwaitAck(t, majorityQ2, 5, 512, 15*time.Second) {
			t.Fatal("majority QoS2 publish did not receive PUBREC")
		}
		clusterDurableProcessPubrel(t, majorityQ2, 512)
		if !clusterDurableFaultAwaitAck(t, majorityQ2, 7, 512, 15*time.Second) {
			t.Fatal("majority QoS2 publish did not receive PUBCOMP")
		}

		clusterDurableFaultClearControl(t, controlPaths[leaderID-1])
		clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
		clusterDurableFaultPublishQoS2Complete(t, fixture.mqttAddresses[leaderID-1], fixture.accessToken,
			"durable-fault-isolated-q2", 502, isolatedQ2Topic, isolatedQ2Topic)
		currentLeaderID, _ := clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
		counts := clusterDurableFaultCollect(t, fixture.mqttAddresses[leaderID-1], fixture.accessToken,
			fixture.mqttAddresses[currentLeaderID-1], "durable-fault/sentinel/partition-leader")
		clusterDurableFaultAssertTopicSet(t, counts, isolatedQ1Topic, isolatedQ2Topic, majorityQ1Topic, majorityQ2Topic)
		if counts[majorityQ1Topic] != 1 || counts[majorityQ2Topic] != 1 || counts[isolatedQ2Topic] != 1 || counts[isolatedQ1Topic] > 1 {
			t.Fatalf("leader-isolated delivery counts = %v", counts)
		}
		t.Logf("leader-isolated leader=%d replacement=%d counts=%v", leaderID, newLeaderID, counts)
	}) {
		t.FailNow()
	}
}

func clusterDurableFaultRequireOptIn(t *testing.T) {
	t.Helper()
	if os.Getenv("NARUMQ_DURABLE_ACK_FAULTS") != "1" {
		t.Skip("set NARUMQ_DURABLE_ACK_FAULTS=1 to run ADR-0003 durable ACK fault acceptance")
	}
}

func clusterDurableFaultPrepareSubscriber(t *testing.T, fixture *clusterMembershipProcessFixture, nodes []*clusterMembershipProcess) {
	t.Helper()
	subscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "durable-fault-sub", false)
	clusterDurableProcessSubscribe(t, subscriber, 1, "durable-fault/#", 2)
	clusterMembershipProcessWaitForRouteProjection(t, fixture.ctx, nodes, "durable-fault/#")
	clusterDurableProcessWriteFatal(t, subscriber, 0xE0, nil)
	clusterMembershipProcessAssertConnectionClosed(t, subscriber, 2*time.Second)
	_ = subscriber.Close()
}

func clusterDurableFaultAwaitAck(t *testing.T, conn net.Conn, kind byte, id uint16, timeout time.Duration) bool {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("set ACK read deadline: %v", err)
	}
	defer conn.SetReadDeadline(time.Time{})
	gotKind, _, body, err := clusterDurableProcessReadRaw(conn)
	if err != nil {
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() || clusterMembershipProcessIsClosed(err) {
			return false
		}
		t.Fatalf("read MQTT ACK kind=%d packet ID=%d: %v", kind, id, err)
	}
	if gotKind != kind || len(body) < 2 || binary.BigEndian.Uint16(body[:2]) != id {
		t.Fatalf("MQTT packet kind=%d body=%x, want ACK kind=%d packet ID=%d", gotKind, body, kind, id)
	}
	return len(body) == 2 || body[2] == 0
}

func clusterDurableFaultPublishQoS2Complete(t *testing.T, address, token, clientID string, id uint16, topic, payload string) {
	t.Helper()
	conn := clusterDurableProcessConnect(t, address, token, clientID, false)
	defer conn.Close()
	if !clusterDurableProcessSessionPresent(t, conn) {
		t.Fatalf("QoS2 publisher %q reconnect returned SessionPresent=false", clientID)
	}
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	body = append(body, byte(id>>8), byte(id))
	body = append(body, payload...)
	if err := clusterDurableProcessWrite(conn, 0x3C, body); err != nil {
		t.Fatalf("send DUP QoS2 PUBLISH: %v", err)
	}
	if !clusterDurableFaultAwaitAck(t, conn, 5, id, 15*time.Second) {
		t.Fatalf("QoS2 DUP PUBLISH packet ID %d did not receive PUBREC", id)
	}
	clusterDurableProcessPubrel(t, conn, id)
	if !clusterDurableFaultAwaitAck(t, conn, 7, id, 15*time.Second) {
		t.Fatalf("QoS2 PUBREL packet ID %d did not receive PUBCOMP", id)
	}
}

func clusterDurableFaultCollect(t *testing.T, subscriberAddress, token, sentinelAddress, sentinelTopic string) map[string]int {
	t.Helper()
	subscriber := clusterDurableProcessConnect(t, subscriberAddress, token, "durable-fault-sub", false)
	defer subscriber.Close()
	if !clusterDurableProcessSessionPresent(t, subscriber) {
		t.Fatal("durable subscriber reconnect returned SessionPresent=false")
	}
	sentinelPublisher := clusterDurableProcessConnect(t, sentinelAddress, token, "durable-fault-sentinel", true)
	clusterDurableProcessPublish(t, sentinelPublisher, sentinelTopic, 1, 65500, sentinelTopic)
	if !clusterDurableFaultAwaitAck(t, sentinelPublisher, 4, 65500, 15*time.Second) {
		t.Fatal("sentinel publish did not receive PUBACK")
	}
	_ = sentinelPublisher.Close()

	deadline := time.Now().Add(30 * time.Second)
	counts := make(map[string]int)
	outstanding := make(map[uint16]struct{})
	sentinelSeen := false
	for !sentinelSeen || len(outstanding) != 0 {
		packet := clusterDurableFaultReadPacket(t, subscriber, deadline)
		switch packet.kind {
		case 3:
			sentinel := packet.topic == sentinelTopic
			if sentinel {
				sentinelSeen = true
			} else {
				counts[packet.topic]++
			}
			switch packet.qos {
			case 1:
				clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(packet.packetID >> 8), byte(packet.packetID)})
			case 2:
				if packet.packetID == 0 {
					t.Fatalf("durable QoS2 PUBLISH %q has packet ID 0", packet.topic)
				}
				outstanding[packet.packetID] = struct{}{}
				clusterDurableProcessWriteFatal(t, subscriber, 0x50, []byte{byte(packet.packetID >> 8), byte(packet.packetID)})
			default:
				t.Fatalf("durable subscriber received unexpected QoS %d for %q", packet.qos, packet.topic)
			}
		case 6:
			if _, exists := outstanding[packet.packetID]; !exists {
				t.Fatalf("subscriber received PUBREL for unknown packet ID %d", packet.packetID)
			}
			delete(outstanding, packet.packetID)
			clusterDurableProcessWriteFatal(t, subscriber, 0x70, []byte{byte(packet.packetID >> 8), byte(packet.packetID)})
		default:
			t.Fatalf("subscriber received packet kind %d while collecting durable deliveries", packet.kind)
		}
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0xE0, nil)
	return counts
}

func clusterDurableFaultReadPacket(t *testing.T, conn net.Conn, deadline time.Time) clusterDurableProcessPacket {
	t.Helper()
	if !time.Now().Before(deadline) {
		t.Fatal("timed out waiting for durable delivery sentinel")
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set durable delivery read deadline: %v", err)
	}
	kind, flags, body, err := clusterDurableProcessReadRaw(conn)
	if err != nil {
		t.Fatalf("read durable delivery packet: %v", err)
	}
	packet := clusterDurableProcessPacket{kind: kind, dup: flags&8 != 0, qos: (flags >> 1) & 3}
	if kind == 3 {
		if len(body) < 2 {
			t.Fatalf("short PUBLISH %x", body)
		}
		topicLength := int(binary.BigEndian.Uint16(body[:2]))
		if len(body) < 2+topicLength {
			t.Fatalf("short PUBLISH topic %x", body)
		}
		packet.topic = string(body[2 : 2+topicLength])
		offset := 2 + topicLength
		if packet.qos > 0 {
			if len(body) < offset+2 {
				t.Fatalf("short PUBLISH packet ID %x", body)
			}
			packet.packetID = binary.BigEndian.Uint16(body[offset : offset+2])
			offset += 2
		}
		packet.payload = string(body[offset:])
	} else if len(body) >= 2 {
		packet.packetID = binary.BigEndian.Uint16(body[:2])
	}
	return packet
}

func clusterDurableFaultWriteControl(t *testing.T, path, crash string, isolate []uint64) {
	t.Helper()
	data, err := json.Marshal(struct {
		Crash        string   `json:"crash"`
		IsolatePeers []uint64 `json:"isolatePeers"`
	}{Crash: crash, IsolatePeers: isolate})
	if err != nil {
		t.Fatalf("encode control file: %v", err)
	}
	clusterFaultWriteControlJSON(t, path, data)
}

func clusterFaultWriteControlJSON(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create control directory: %v", err)
	}
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o600); err != nil {
		t.Fatalf("write temporary control file: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := os.Rename(tempPath, path)
		if err == nil {
			break
		}
		if !time.Now().Before(deadline) {
			if writeErr := os.WriteFile(path, data, 0o600); writeErr != nil {
				t.Fatalf("replace control file: rename failed: %v; in-place write failed: %v", err, writeErr)
			}
			_ = os.Remove(tempPath)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read control file after write: %v", err)
	}
	if !bytes.Equal(written, data) {
		t.Fatalf("control file contents = %s, want %s", written, data)
	}
}

func clusterDurableFaultClearControl(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := os.Remove(path)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("remove control file %q: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func clusterDurableFaultAssertTopicSet(t *testing.T, counts map[string]int, allowed ...string) {
	t.Helper()
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, topic := range allowed {
		allowedSet[topic] = struct{}{}
	}
	for topic := range counts {
		if _, ok := allowedSet[topic]; !ok {
			t.Fatalf("unexpected durable delivery topic %q in count map %v", topic, counts)
		}
	}
}

func smallestNonLeaderID(leaderID uint64, nodeCount int) uint64 {
	for id := uint64(1); id <= uint64(nodeCount); id++ {
		if id != leaderID {
			return id
		}
	}
	return 0
}

func smallestLiveClusterNodeID(nodes []*clusterMembershipProcess, excludedID uint64) uint64 {
	for id := uint64(1); id <= uint64(len(nodes)); id++ {
		if id == excludedID || nodes[id-1] == nil {
			continue
		}
		select {
		case <-nodes[id-1].done:
			continue
		default:
			return id
		}
	}
	return 0
}

func otherClusterNodeIDs(nodeID uint64, nodeCount int) []uint64 {
	peers := make([]uint64, 0, nodeCount-1)
	for id := uint64(1); id <= uint64(nodeCount); id++ {
		if id != nodeID {
			peers = append(peers, id)
		}
	}
	return peers
}
