package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"testing"
	"time"
)

// Traceability: CLU-02/CLU-07/DEP-06/DEP-07/DEP-08/DEP-10; docs/specs/drain-and-rolling-update.md §7.2, §10; ADR-0051 §7.
func TestDynamicClusterReplacementRollingUpdateProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 6, "")
	ctx := fixture.ctx
	nodes := make(map[uint64]*clusterMembershipProcess, 6)
	for id := uint64(1); id <= 3; id++ {
		process, err := fixture.harness.start(
			fmt.Sprintf("replacement bootstrap node %d", id),
			fixture.bootstrapConfig(id),
			fixture.managementAddresses[id-1],
		)
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", id, err)
		}
		nodes[id] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, clusterReplacementProcesses(nodes))

	liveV5, sessionPresent, reason, err := clusterDrainProcessConnectV5(
		fixture.mqttAddresses[0], fixture.accessToken, "replace-live-v5", false,
	)
	if err != nil || reason != 0 || sessionPresent {
		t.Fatalf("initial MQTT 5 connection present=%v reason=%#x err=%v, want new session", sessionPresent, reason, err)
	}
	clusterDrainProcessSubscribeV5(t, liveV5, 1, "replace/q1", 1)
	clusterDrainProcessSubscribeV5(t, liveV5, 2, "replace/q2", 2)

	liveV311 := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "replace-live-v311", false)
	clusterDurableProcessSubscribe(t, liveV311, 1, "replace/q1", 1)
	clusterDurableProcessSubscribe(t, liveV311, 2, "replace/q2", 2)
	offlineConn := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "replace-offline-v311", false)
	clusterDurableProcessSubscribe(t, offlineConn, 1, "replace/q1", 1)
	clusterDurableProcessSubscribe(t, offlineConn, 2, "replace/q2", 2)
	if err := clusterDurableProcessWrite(offlineConn, 0xE0, nil); err != nil {
		t.Fatalf("disconnect offline subscriber: %v", err)
	}
	clusterMembershipProcessAssertConnectionClosed(t, offlineConn, 2*time.Second)
	_ = offlineConn.Close()

	receivers := []*clusterReplacementReceiver{
		{clientID: "replace-live-v5", v5: true, conn: liveV5, received: make(map[string]map[int]int), pendingRelease: make(map[uint16]int)},
		{clientID: "replace-live-v311", conn: liveV311, received: make(map[string]map[int]int), pendingRelease: make(map[uint16]int)},
	}
	defer func() {
		for _, receiver := range receivers {
			if receiver.conn != nil {
				_ = receiver.conn.Close()
			}
		}
		if offlineConn != nil {
			_ = offlineConn.Close()
		}
	}()
	wantSubscriptions := []string{
		"replace-live-v311|replace/q1|1",
		"replace-live-v311|replace/q2|2",
		"replace-live-v5|replace/q1|1",
		"replace-live-v5|replace/q2|2",
		"replace-offline-v311|replace/q1|1",
		"replace-offline-v311|replace/q2|2",
	}
	clusterDurableProcessWaitForSubscriptions(t, ctx, clusterReplacementProcesses(nodes), "acceptance", wantSubscriptions...)

	rounds := []struct {
		old          uint64
		replacement  uint64
		reference    uint64
		seeds        []uint64
		minTakeovers uint64
	}{
		{old: 1, replacement: 4, reference: 2, seeds: []uint64{1, 2, 3}, minTakeovers: 3},
		{old: 2, replacement: 5, reference: 3, seeds: []uint64{2, 3, 4}, minTakeovers: 2},
		{old: 3, replacement: 6, reference: 4, seeds: []uint64{3, 4, 5}, minTakeovers: 2},
	}
	const messagesPerTopicPerBatch = 3
	publisher := clusterReplacementPublisher{
		published: map[string]int{"replace/q1": 0, "replace/q2": 0},
	}
	retired := ""
	voterIDs := func() []string {
		ids := make([]uint64, 0, len(nodes))
		for id := range nodes {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		voters := make([]string, 0, len(ids))
		for _, id := range ids {
			voters = append(voters, strconv.FormatUint(id, 10))
		}
		return voters
	}

	for roundIndex, round := range rounds {
		t.Logf("replacement round %d: %d -> %d", roundIndex+1, round.old, round.replacement)
		live := clusterReplacementProcesses(nodes)
		clusterMembershipProcessAssertVotersAndRetired(t, ctx, live, voterIDs(), retired)

		stageBody := clusterReplacementLeaderRequest(
			t, ctx, nodes, http.MethodPost, "/api/v1/cluster/members",
			fixture.stageRequestBody(round.replacement),
			fmt.Sprintf("replace-stage-node-%d", round.replacement),
		)
		stage, err := clusterMembershipProcessDecodeOperation(stageBody)
		if err != nil || stage.Action != "ADD" || stage.MemberID != round.replacement || stage.OperationID == "" {
			t.Fatalf("stage node %d operation=%+v err=%v body=%s", round.replacement, stage, err, stageBody)
		}
		joiner, err := fixture.harness.start(
			fmt.Sprintf("replacement node %d", round.replacement),
			fixture.joinerConfig(round.replacement, round.seeds),
			fixture.managementAddresses[round.replacement-1],
		)
		if err != nil {
			t.Fatalf("start replacement node %d: %v", round.replacement, err)
		}
		nodes[round.replacement] = joiner
		clusterMembershipProcessWaitForOperation(t, ctx, nodes[round.reference], stage.OperationID, "SUCCEEDED")
		clusterMembershipProcessWaitForStatus(t, ctx, joiner, "/health/ready", http.StatusOK)
		live4 := clusterReplacementProcesses(nodes)
		clusterMembershipProcessWaitForClusterSize(t, ctx, live4, 4)
		clusterMembershipProcessAssertVotersAndRetired(t, ctx, live4, voterIDs(), retired)

		publisher.publishBatch(t, fixture.mqttAddresses[round.replacement-1], fixture.accessToken, messagesPerTopicPerBatch)
		for _, receiver := range receivers {
			receiver.collect(t, publisher.published)
		}

		drainBody, err := json.Marshal(clusterDrainProcessRequest{
			ServerReference:                    fixture.mqttAddresses[round.reference-1],
			ConnectionRatePerSecond:            10,
			LoadBalancerPropagationDelayMillis: 1000,
			TimeoutMillis:                      60000,
		})
		if err != nil {
			t.Fatalf("encode drain request for node %d: %v", round.old, err)
		}
		clusterReplacementLeaderRequest(
			t, ctx, nodes, http.MethodPost, fmt.Sprintf("/api/v1/nodes/%d/drain", round.old),
			drainBody, fmt.Sprintf("replace-drain-node-%d", round.old),
		)

		_ = receivers[0].conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		kind, _, disconnectBody, err := clusterDurableProcessReadRaw(receivers[0].conn)
		if err != nil || kind != 14 {
			t.Fatalf("read MQTT 5 redirect for client %q: kind=%d body=%x err=%v", receivers[0].clientID, kind, disconnectBody, err)
		}
		clusterDrainProcessAssertServerReference(t, disconnectBody, fixture.mqttAddresses[round.reference-1])
		_ = receivers[0].conn.Close()
		receivers[0].conn = nil

		_ = receivers[1].conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		kind, _, _, err = clusterDurableProcessReadRaw(receivers[1].conn)
		if err == nil || !clusterMembershipProcessIsClosed(err) {
			t.Fatalf("MQTT 3.1.1 client %q read kind=%d err=%v, want a closed connection", receivers[1].clientID, kind, err)
		}
		_ = receivers[1].conn.Close()
		receivers[1].conn = nil

		publisher.publishBatch(t, fixture.mqttAddresses[round.replacement-1], fixture.accessToken, messagesPerTopicPerBatch)
		drainOperation := clusterDrainProcessWaitForPhase(t, ctx, nodes[round.replacement], round.old, "COMPLETED")
		minTakeovers := round.minTakeovers
		if roundIndex == 0 && drainOperation.Sessions.TakenOver < minTakeovers {
			status, body, getErr := clusterMembershipProcessRequest(ctx, http.MethodGet,
				fmt.Sprintf("%s/api/v1/nodes/%d/drain", nodes[round.replacement].managementURL(), round.old),
				nil, clusterMembershipProcessAdmin, "")
			t.Logf("round 1 takeover count=%d; drain GET status=%d body=%s err=%v",
				drainOperation.Sessions.TakenOver, status, body, getErr)
			minTakeovers = 2
		}
		if drainOperation.State != "SAFE_TO_STOP" || !drainOperation.SafeToStop ||
			drainOperation.Connections.Remaining != 0 || drainOperation.Sessions.Failed != 0 ||
			drainOperation.Sessions.TakenOver < minTakeovers {
			t.Fatalf("drain barrier for node %d = %+v, want SAFE_TO_STOP, zero connections/failures and at least %d takeovers",
				round.old, drainOperation, minTakeovers)
		}

		v5Conn := clusterReplacementReconnectV5(t, fixture.mqttAddresses[round.reference-1], fixture.accessToken, "replace-live-v5")
		v311Conn := clusterDurableProcessConnect(t, fixture.mqttAddresses[round.reference-1], fixture.accessToken, "replace-live-v311", false)
		if !clusterDurableProcessSessionPresent(t, v311Conn) {
			t.Fatal("MQTT 3.1.1 live session was not present after drain")
		}
		receivers[0].conn = v5Conn
		receivers[1].conn = v311Conn
		for _, receiver := range receivers {
			receiver.collect(t, publisher.published)
		}

		removeBody := clusterReplacementLeaderRequest(
			t, ctx, nodes, http.MethodDelete, fmt.Sprintf("/api/v1/cluster/members/%d", round.old),
			nil, fmt.Sprintf("replace-remove-node-%d", round.old),
		)
		removal, err := clusterMembershipProcessDecodeOperation(removeBody)
		if err != nil || removal.Action != "REMOVE" || removal.MemberID != round.old {
			t.Fatalf("remove node %d operation=%+v err=%v body=%s", round.old, removal, err, removeBody)
		}
		clusterMembershipProcessWaitForOperation(t, ctx, nodes[round.replacement], removal.OperationID, "SUCCEEDED")
		clusterDrainProcessWaitForNotFound(t, ctx, nodes[round.replacement], round.old)
		if err := nodes[round.old].stop(10 * time.Second); err != nil {
			t.Fatalf("stop removed node %d: %v", round.old, err)
		}
		delete(nodes, round.old)
		retired = strconv.FormatUint(round.old, 10)

		live3 := clusterReplacementProcesses(nodes)
		clusterMembershipProcessWaitForClusterSize(t, ctx, live3, 3)
		clusterMembershipProcessAssertVotersAndRetired(t, ctx, live3, voterIDs(), retired)
		clusterDurableProcessWaitForSubscriptions(t, ctx, live3, "acceptance", wantSubscriptions...)

		publisher.publishBatch(t, fixture.mqttAddresses[round.replacement-1], fixture.accessToken, messagesPerTopicPerBatch)
		for _, receiver := range receivers {
			receiver.collect(t, publisher.published)
		}
	}

	offlineConn = clusterDurableProcessConnect(
		t, fixture.mqttAddresses[5], fixture.accessToken, "replace-offline-v311", false,
	)
	if !clusterDurableProcessSessionPresent(t, offlineConn) {
		t.Fatal("offline MQTT 3.1.1 session was not present after three replacements")
	}
	offlineReceiver := &clusterReplacementReceiver{
		clientID:       "replace-offline-v311",
		conn:           offlineConn,
		received:       make(map[string]map[int]int),
		pendingRelease: make(map[uint16]int),
	}
	offlineReceiver.collect(t, publisher.published)
	for _, receiver := range append(receivers, offlineReceiver) {
		t.Logf("QoS 1 duplicates %s=%d", receiver.clientID, receiver.qos1Duplicates)
		if receiver.conn != nil {
			_ = receiver.conn.Close()
			receiver.conn = nil
		}
	}
	offlineConn = nil
}

type clusterReplacementPublisher struct {
	published map[string]int
	packetID  uint16
}

func (p *clusterReplacementPublisher) publishBatch(t *testing.T, address, token string, perTopic int) {
	t.Helper()
	conn := clusterDurableProcessConnect(t, address, token, "replace-publisher", true)
	defer conn.Close()
	for range perTopic {
		for _, message := range []struct {
			topic string
			qos   byte
		}{
			{topic: "replace/q1", qos: 1},
			{topic: "replace/q2", qos: 2},
		} {
			seq := p.published[message.topic] + 1
			p.packetID++
			if err := clusterMatrixSendPublish(conn, message.topic, message.qos, p.packetID, strconv.Itoa(seq), false); err != nil {
				t.Fatalf("publish topic=%q qos=%d seq=%d: %v", message.topic, message.qos, seq, err)
			}
			p.published[message.topic] = seq
		}
	}
	if err := clusterDurableProcessWrite(conn, 0xE0, nil); err != nil {
		t.Fatalf("disconnect replacement publisher: %v", err)
	}
	_ = conn.Close()
}

type clusterReplacementReceiver struct {
	clientID       string
	v5             bool
	conn           net.Conn
	received       map[string]map[int]int
	pendingRelease map[uint16]int
	qos1Duplicates int
}

func (r *clusterReplacementReceiver) collect(t *testing.T, published map[string]int) {
	t.Helper()
	if r.conn == nil {
		t.Fatalf("receiver %q has no active connection", r.clientID)
	}
	for {
		complete := r.complete(published)
		timeout := 15 * time.Second
		if complete {
			timeout = time.Second
		}
		_ = r.conn.SetReadDeadline(time.Now().Add(timeout))
		kind, flags, body, err := clusterDurableProcessReadRaw(r.conn)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				if complete {
					return
				}
				t.Fatalf("receiver %q timed out with missing deliveries: %s", r.clientID, r.missing(published))
			}
			t.Fatalf("receiver %q read packet: %v", r.clientID, err)
		}
		switch kind {
		case 3:
			delivery, err := clusterMatrixDecodePublish(flags, body, r.v5)
			if err != nil {
				t.Fatalf("receiver %q decode PUBLISH: %v", r.clientID, err)
			}
			if delivery.topic != "replace/q1" && delivery.topic != "replace/q2" {
				t.Fatalf("receiver %q received unexpected topic %q", r.clientID, delivery.topic)
			}
			seq, err := strconv.Atoi(delivery.payload)
			if err != nil || seq < 1 || seq > published[delivery.topic] {
				t.Fatalf("receiver %q received topic=%q payload=%q seq=%d err=%v, published=%d",
					r.clientID, delivery.topic, delivery.payload, seq, err, published[delivery.topic])
			}
			if r.received[delivery.topic] == nil {
				r.received[delivery.topic] = make(map[int]int)
			}
			switch delivery.qos {
			case 1:
				if r.received[delivery.topic][seq] > 0 {
					r.qos1Duplicates++
				} else {
					r.received[delivery.topic][seq] = 1
				}
				clusterDurableProcessWriteFatal(t, r.conn, 0x40,
					[]byte{byte(delivery.packetID >> 8), byte(delivery.packetID)})
			case 2:
				if delivery.dup && r.pendingRelease[delivery.packetID] == seq {
					clusterDurableProcessWriteFatal(t, r.conn, 0x50,
						[]byte{byte(delivery.packetID >> 8), byte(delivery.packetID)})
					continue
				}
				if r.received[delivery.topic][seq] > 0 {
					t.Fatalf("receiver %q received duplicate QoS 2 delivery topic=%q seq=%d packet=%d",
						r.clientID, delivery.topic, seq, delivery.packetID)
				}
				r.received[delivery.topic][seq] = 1
				r.pendingRelease[delivery.packetID] = seq
				clusterDurableProcessWriteFatal(t, r.conn, 0x50,
					[]byte{byte(delivery.packetID >> 8), byte(delivery.packetID)})
			default:
				t.Fatalf("receiver %q received unexpected QoS %d for topic=%q seq=%d",
					r.clientID, delivery.qos, delivery.topic, seq)
			}
		case 6:
			if len(body) < 2 {
				t.Fatalf("receiver %q received truncated PUBREL body=%x", r.clientID, body)
			}
			packetID := binary.BigEndian.Uint16(body[:2])
			delete(r.pendingRelease, packetID)
			clusterDurableProcessWriteFatal(t, r.conn, 0x70, []byte{byte(packetID >> 8), byte(packetID)})
		default:
			t.Fatalf("receiver %q received unexpected MQTT packet kind=%d body=%x", r.clientID, kind, body)
		}
	}
}

func (r *clusterReplacementReceiver) complete(published map[string]int) bool {
	for topic, last := range published {
		for seq := 1; seq <= last; seq++ {
			if r.received[topic][seq] == 0 {
				return false
			}
		}
	}
	return len(r.pendingRelease) == 0
}

func (r *clusterReplacementReceiver) missing(published map[string]int) string {
	missing := make([]string, 0, len(published))
	for topic, last := range published {
		var absent []int
		for seq := 1; seq <= last; seq++ {
			if r.received[topic][seq] == 0 {
				absent = append(absent, seq)
			}
		}
		missing = append(missing, fmt.Sprintf("%s=%v (published=%d)", topic, absent, last))
	}
	sort.Strings(missing)
	return fmt.Sprint(missing)
}

func clusterReplacementReconnectV5(t *testing.T, address, token, clientID string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastReason byte
	var lastErr error
	for time.Now().Before(deadline) {
		conn, present, reason, err := clusterDrainProcessConnectV5(address, token, clientID, false)
		if err != nil {
			t.Fatalf("reconnect MQTT 5 client %q to %s: %v", clientID, address, err)
		}
		if reason == 0 {
			if !present {
				_ = conn.Close()
				t.Fatalf("MQTT 5 client %q did not resume its session", clientID)
			}
			return conn
		}
		_ = conn.Close()
		lastReason = reason
		if reason != 0x88 {
			t.Fatalf("reconnect MQTT 5 client %q returned CONNACK reason %#x, want success or 0x88", clientID, reason)
		}
		lastErr = fmt.Errorf("CONNACK reason %#x", reason)
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("reconnect MQTT 5 client %q to %s timed out after Server Unavailable: reason=%#x err=%v",
		clientID, address, lastReason, lastErr)
	return nil
}

func clusterReplacementProcesses(nodes map[uint64]*clusterMembershipProcess) []*clusterMembershipProcess {
	ids := make([]uint64, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	processes := make([]*clusterMembershipProcess, 0, len(ids))
	for _, id := range ids {
		processes = append(processes, nodes[id])
	}
	return processes
}

func clusterReplacementLeaderRequest(
	t *testing.T,
	ctx context.Context,
	nodes map[uint64]*clusterMembershipProcess,
	method, path string,
	body []byte,
	idempotencyKey string,
) []byte {
	t.Helper()
	var responseBody []byte
	clusterMembershipProcessWait(t, ctx, method+" "+path+" on leader", func() (bool, string, error) {
		ids := make([]uint64, 0, len(nodes))
		for id := range nodes {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			process := nodes[id]
			if process == nil || process.isAlive() != nil {
				continue
			}
			summary, status, detail, err := clusterMembershipProcessGetSummary(ctx, process.managementURL(), clusterMembershipProcessAdmin)
			if err != nil || status != http.StatusOK || summary.LeaderNodeID == nil {
				return false, fmt.Sprintf("%s cluster summary status=%d body=%s err=%v", process.name, status, detail, err), nil
			}
			leaderID, err := strconv.ParseUint(*summary.LeaderNodeID, 10, 64)
			if err != nil || nodes[leaderID] == nil {
				return false, fmt.Sprintf("%s reported unavailable leader %q: %v", process.name, *summary.LeaderNodeID, err), nil
			}
			leader := nodes[leaderID]
			status, response, err := clusterMembershipProcessRequest(
				ctx, method, leader.managementURL()+path, body, clusterMembershipProcessAdmin, idempotencyKey,
			)
			if err != nil {
				return false, fmt.Sprintf("request through leader %d: %v", leaderID, err), err
			}
			if status == http.StatusAccepted || status == http.StatusOK {
				responseBody = response
				return true, "", nil
			}
			if status == http.StatusServiceUnavailable {
				return false, fmt.Sprintf("leader %d returned 503 body=%s", leaderID, response), nil
			}
			if status == http.StatusConflict {
				var envelope struct {
					Error struct {
						Code string `json:"code"`
					} `json:"error"`
				}
				if err := json.Unmarshal(response, &envelope); err != nil {
					return false, "", fmt.Errorf("decode leader conflict response: %w; body=%s", err, response)
				}
				switch envelope.Error.Code {
				case "not_leader", "membership_change_in_progress", "node_drain_in_progress":
					return false, fmt.Sprintf("leader %d returned retryable %s", leaderID, envelope.Error.Code), nil
				default:
					return false, "", fmt.Errorf("leader %d returned non-retryable conflict %q body=%s", leaderID, envelope.Error.Code, response)
				}
			}
			return false, "", fmt.Errorf("status=%d body=%s", status, response)
		}
		return false, "no live node reported a usable leader", nil
	})
	return responseBody
}
