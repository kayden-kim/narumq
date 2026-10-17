package integration

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

const clusterRolling100Rounds = 100

// Traceability: DEP-06/DEP-07/DEP-10/DEP-12; docs/specs/drain-and-rolling-update.md §9, §10.
// Acceptance: NARUMQ_ROLLING_100
// Scenarios: drain-and-rolling-update.md:DRN-SCN-018
func TestDynamicClusterContinuousRollingDrainProcessAcceptance(t *testing.T) {
	if os.Getenv("NARUMQ_ROLLING_100") != "1" {
		t.Skip("set NARUMQ_ROLLING_100=1 to run the 100-round continuous rolling drain acceptance")
	}

	rounds := clusterRolling100Rounds
	if value := os.Getenv("NARUMQ_ROLLING_ROUNDS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			t.Fatalf("NARUMQ_ROLLING_ROUNDS=%q must be a positive integer", value)
		}
		rounds = parsed
	}
	if rounds != clusterRolling100Rounds {
		t.Logf("dry run: %d rounds; a passing dry run is reported as skipped, not as 100-round acceptance evidence", rounds)
	}

	fixture := newClusterMembershipProcessFixtureWithLifetime(t, 3, "", 3*time.Hour)
	ctx := fixture.ctx
	nodes := make([]*clusterMembershipProcess, 3)
	for index := range nodes {
		process, err := fixture.harness.start(
			fmt.Sprintf("rolling bootstrap node %d", index+1),
			fixture.bootstrapConfig(uint64(index+1)),
			fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("start rolling node %d: %v", index+1, err)
		}
		nodes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	clusterSessionMetadataActivate(t, ctx, nodes)

	order := [3]uint64{3, 1, 2}
	addr := func(nodeID uint64) string { return fixture.mqttAddresses[nodeID-1] }
	v5Conn, present, reason, err := clusterDrainProcessConnectV5(
		addr(order[0]), fixture.accessToken, "roll-sub-v5", false,
	)
	if err != nil || reason != 0 || present {
		t.Fatalf("initial MQTT 5 receiver present=%v reason=%#x err=%v, want a new session", present, reason, err)
	}
	v311Conn := clusterDurableProcessConnect(t, addr(order[0]), fixture.accessToken, "roll-sub-v311", false)
	if clusterDurableProcessSessionPresent(t, v311Conn) {
		t.Fatal("initial MQTT 3.1.1 receiver unexpectedly resumed a session")
	}
	clusterDrainProcessSubscribeV5(t, v5Conn, 1, "roll/q1", 1)
	clusterDrainProcessSubscribeV5(t, v5Conn, 2, "roll/q2", 2)
	clusterDurableProcessSubscribe(t, v311Conn, 1, "roll/q1", 1)
	clusterDurableProcessSubscribe(t, v311Conn, 2, "roll/q2", 2)
	receivers := []*clusterRollingReceiver{
		{
			clientID:       "roll-sub-v5",
			v5:             true,
			conn:           v5Conn,
			received:       make(map[string]map[int]int),
			pendingRelease: make(map[uint16]int),
			maxFirst:       make(map[string]int),
		},
		{
			clientID:       "roll-sub-v311",
			conn:           v311Conn,
			received:       make(map[string]map[int]int),
			pendingRelease: make(map[uint16]int),
			maxFirst:       make(map[string]int),
		},
	}
	defer func() {
		for _, receiver := range receivers {
			if receiver.conn != nil {
				_ = receiver.conn.Close()
			}
		}
	}()

	safeWill := &clusterSessionMetadataWill{topic: "roll/will/safe", payload: "safe", delay: 8}
	safe := clusterSessionMetadataConnect(t, addr(order[0]), fixture.accessToken, "roll-will-v5", 300, safeWill)
	defer func() {
		if safe != nil {
			_ = safe.Close()
		}
	}()
	published := map[string]int{"roll/q1": 0, "roll/q2": 0}
	var publisherPacketID uint16
	currentPublishNode := order[1]
	publish := func(perTopic, retainedRound int) {
		t.Helper()
		conn := clusterDurableProcessConnect(t, addr(currentPublishNode), fixture.accessToken, "roll-pub", true)
		for range perTopic {
			for _, message := range []struct {
				topic string
				qos   byte
			}{
				{topic: "roll/q1", qos: 1},
				{topic: "roll/q2", qos: 2},
			} {
				seq := published[message.topic] + 1
				publisherPacketID++
				if err := clusterMatrixSendPublish(conn, message.topic, message.qos, publisherPacketID, strconv.Itoa(seq), false); err != nil {
					t.Fatalf("publish topic=%q qos=%d seq=%d: %v", message.topic, message.qos, seq, err)
				}
				published[message.topic] = seq
			}
		}
		if retainedRound > 0 {
			topic := fmt.Sprintf("roll/retained/%03d", retainedRound)
			publisherPacketID++
			if err := clusterMatrixSendPublish(conn, topic, 1, publisherPacketID, fmt.Sprintf("retained-%03d", retainedRound), true); err != nil {
				t.Fatalf("publish retained topic=%q: %v", topic, err)
			}
		}
		if err := clusterDurableProcessWrite(conn, 0xE0, nil); err != nil {
			t.Fatalf("disconnect rolling publisher: %v", err)
		}
		_ = conn.Close()
	}
	wantSubscriptions := []string{
		"roll-sub-v311|roll/q1|1",
		"roll-sub-v311|roll/q2|2",
		"roll-sub-v5|roll/q1|1",
		"roll-sub-v5|roll/q2|2",
	}
	clusterDurableProcessWaitForSubscriptions(t, ctx, nodes, "acceptance", wantSubscriptions...)

	total := make(map[string]int64, len(clusterDrainWillReasons))
	for round := 1; round <= rounds; round++ {
		started := time.Now()
		index := round - 1
		d := order[index%len(order)]
		ref := order[(index+1)%len(order)]
		w := order[(index+2)%len(order)]
		currentPublishNode = w

		before := clusterDrainWillOutcomes(t, ctx, nodes)
		willConn := clusterDurableProcessConnectWill(t, addr(d), fixture.accessToken, "roll-will-v311", true,
			&clusterDurableProcessWill{topic: "roll/will/v311", payload: fmt.Sprintf("v311-%03d", round)})
		observer := clusterDurableProcessConnect(t, addr(w), fixture.accessToken, "roll-observer", true)
		clusterDurableProcessSubscribe(t, observer, 1, "roll/will/#", 0)
		clusterDurableProcessWaitForSubscriptions(t, ctx, nodes, "acceptance",
			append(wantSubscriptions, "roll-observer|roll/will/#|0")...)

		publish(3, round)
		for _, receiver := range receivers {
			receiver.collect(t, published)
		}

		_, leaderURL := clusterMembershipProcessFindLeader(t, ctx, nodes, 3)
		status, body, err := clusterDrainProcessStart(ctx, leaderURL, d, clusterDrainProcessRequest{
			ServerReference:                    addr(ref),
			ConnectionRatePerSecond:            10,
			LoadBalancerPropagationDelayMillis: 1000,
			TimeoutMillis:                      60000,
		}, fmt.Sprintf("roll-drain-%03d", round))
		if err != nil || status != http.StatusAccepted {
			t.Fatalf("round %d drain status=%d body=%s err=%v", round, status, body, err)
		}

		publish(3, 0)
		receivers[0].awaitClose(t, addr(ref))
		receivers[1].awaitClose(t, "")
		_ = receivers[0].conn.Close()
		receivers[0].conn = nil
		_ = receivers[1].conn.Close()
		receivers[1].conn = nil
		clusterDrainWillReadServerDisconnect(t, safe, addr(ref))
		_ = safe.Close()
		clusterMembershipProcessAssertConnectionClosed(t, willConn, 20*time.Second)

		publish(3, 0)
		safe = clusterSessionMetadataConnect(t, addr(ref), fixture.accessToken, "roll-will-v5", 300, safeWill)
		safeAt := time.Now()
		if !clusterDurableProcessSessionPresent(t, safe) {
			t.Fatalf("round %d safe reconnect lost session", round)
		}
		receivers[0].conn = clusterReplacementReconnectV5(t, addr(ref), fixture.accessToken, "roll-sub-v5")
		receivers[1].conn = clusterDurableProcessConnect(t, addr(ref), fixture.accessToken, "roll-sub-v311", false)
		if !clusterDurableProcessSessionPresent(t, receivers[1].conn) {
			t.Fatalf("round %d MQTT 3.1.1 receiver lost persistent session", round)
		}
		for _, receiver := range receivers {
			receiver.collect(t, published)
		}

		witness := nodes[ref-1]
		op := clusterDrainProcessWaitForPhase(t, ctx, witness, d, "COMPLETED")
		if op.State != "SAFE_TO_STOP" || !op.SafeToStop || op.Connections.Remaining != 0 || op.Sessions.Failed != 0 {
			t.Fatalf("round %d drain barrier for node %d = %+v, want SAFE_TO_STOP, zero connections and failures", round, d, op)
		}

		publish(3, 0)
		for _, receiver := range receivers {
			receiver.collect(t, published)
		}

		got := clusterDrainWillCollect(t, observer, 1, 20*time.Second)
		want := map[string]string{"roll/will/v311": fmt.Sprintf("v311-%03d", round)}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d observed Wills=%v want=%v", round, got, want)
		}
		remaining := time.Until(safeAt.Add(10 * time.Second))
		if remaining <= 0 {
			t.Fatalf("round %d missed safe Will observation window", round)
		}
		clusterSessionMetadataAssertNoPublish(t, observer, remaining)

		after := clusterDrainWillOutcomes(t, ctx, nodes)
		clusterDrainWillAssertDelta(t, round, before, after, map[string]int64{
			"suppressed_reconnected_within_delay": 1,
			"expected_mqtt311_server_disconnect":  1,
		})
		for _, reason := range clusterDrainWillReasons {
			total[reason] += after[reason] - before[reason]
		}
		_ = observer.Close()
		_ = willConn.Close()

		if err := nodes[d-1].stop(10 * time.Second); err != nil {
			t.Fatalf("stop node %d after round %d: %v", d, round, err)
		}
		restarted, err := fixture.harness.start(
			fmt.Sprintf("rolling node %d restart round %d", d, round),
			fixture.bootstrapConfig(d),
			fixture.managementAddresses[d-1],
		)
		if err != nil {
			t.Fatalf("restart node %d after round %d: %v", d, round, err)
		}
		nodes[d-1] = restarted
		clusterMembershipProcessWaitForStatus(t, ctx, restarted, "/health/ready", http.StatusOK)
		clusterDrainProcessWaitForNotFound(t, ctx, restarted, d)
		clusterRollingAssertRetained(t, addr(d), fixture.accessToken, round%2 == 1, round)

		t.Logf("round %d node %d: delta=%v qos1Dup=%d reorder=%d elapsed=%s",
			round, d, map[string]int64{
				"suppressed_reconnected_within_delay": after["suppressed_reconnected_within_delay"] - before["suppressed_reconnected_within_delay"],
				"expected_mqtt311_server_disconnect":  after["expected_mqtt311_server_disconnect"] - before["expected_mqtt311_server_disconnect"],
			}, receivers[0].qos1Duplicates+receivers[1].qos1Duplicates,
			receivers[0].reorders+receivers[1].reorders, time.Since(started))
	}

	if total["unexpected_supported_client"] != 0 || total["unplanned_failure"] != 0 ||
		total["suppressed_reconnected_within_delay"] != int64(rounds) ||
		total["expected_mqtt311_server_disconnect"] != int64(rounds) {
		t.Fatalf("rolling Will outcomes=%v rounds=%d", total, rounds)
	}
	for _, receiver := range receivers {
		if receiver.conn != nil {
			_ = receiver.conn.Close()
			receiver.conn = nil
		}
	}
	t.Logf("rolling total rounds=%d published=%v qos1Dup=%d reorder=%d WillOutcomes=%v",
		rounds, published,
		receivers[0].qos1Duplicates+receivers[1].qos1Duplicates,
		receivers[0].reorders+receivers[1].reorders, total)
	if rounds != clusterRolling100Rounds {
		t.Skipf("dry run of %d rounds passed; only %d rounds count as DRN-SCN-018 acceptance", rounds, clusterRolling100Rounds)
	}
}

type clusterRollingReceiver struct {
	clientID       string
	v5             bool
	conn           net.Conn
	published      map[string]int
	received       map[string]map[int]int
	pendingRelease map[uint16]int
	maxFirst       map[string]int
	qos1Duplicates int
	reorders       int
}

func (r *clusterRollingReceiver) collect(t *testing.T, published map[string]int) {
	t.Helper()
	if r.conn == nil {
		t.Fatalf("receiver %q has no active connection", r.clientID)
	}
	r.published = published
	for {
		complete := r.complete(published)
		timeout := 15 * time.Second
		if complete {
			timeout = time.Second
		}
		_ = r.conn.SetReadDeadline(time.Now().Add(timeout))
		kind, flags, body, err := clusterDurableProcessReadRaw(r.conn)
		_ = r.conn.SetReadDeadline(time.Time{})
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
		r.handlePacket(t, kind, flags, body)
	}
}

func (r *clusterRollingReceiver) awaitClose(t *testing.T, wantReference string) {
	t.Helper()
	if r.conn == nil {
		t.Fatalf("receiver %q has no active connection while awaiting drain", r.clientID)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_ = r.conn.SetReadDeadline(deadline)
		kind, flags, body, err := clusterDurableProcessReadRaw(r.conn)
		_ = r.conn.SetReadDeadline(time.Time{})
		if err != nil {
			if !r.v5 && clusterMembershipProcessIsClosed(err) {
				return
			}
			t.Fatalf("receiver %q awaiting drain close: %v", r.clientID, err)
		}
		if kind == 14 {
			if !r.v5 {
				t.Fatalf("MQTT 3.1.1 receiver %q received server DISCONNECT", r.clientID)
			}
			clusterDrainProcessAssertServerReference(t, body, wantReference)
			return
		}
		if kind != 3 && kind != 6 {
			t.Fatalf("receiver %q received unexpected packet kind=%d while draining", r.clientID, kind)
		}
		r.handlePacket(t, kind, flags, body)
	}
	t.Fatalf("receiver %q did not close during drain", r.clientID)
}

func (r *clusterRollingReceiver) handlePacket(t *testing.T, kind, flags byte, body []byte) {
	t.Helper()
	switch kind {
	case 3:
		delivery, err := clusterMatrixDecodePublish(flags, body, r.v5)
		if err != nil {
			t.Fatalf("receiver %q decode PUBLISH: %v", r.clientID, err)
		}
		if delivery.topic != "roll/q1" && delivery.topic != "roll/q2" {
			t.Fatalf("receiver %q received unexpected topic %q", r.clientID, delivery.topic)
		}
		seq, err := strconv.Atoi(delivery.payload)
		if err != nil || seq < 1 {
			t.Fatalf("receiver %q received invalid topic=%q payload=%q seq=%d err=%v",
				r.clientID, delivery.topic, delivery.payload, seq, err)
		}
		if seq > r.published[delivery.topic] {
			t.Fatalf("receiver %q received unpublished topic=%q seq=%d published=%d", r.clientID, delivery.topic, seq, r.published[delivery.topic])
		}
		wantQoS := byte(1)
		if delivery.topic == "roll/q2" {
			wantQoS = 2
		}
		if delivery.qos != wantQoS {
			t.Fatalf("receiver %q topic=%q delivery QoS=%d want=%d", r.clientID, delivery.topic, delivery.qos, wantQoS)
		}
		if r.received[delivery.topic] == nil {
			r.received[delivery.topic] = make(map[int]int)
		}
		switch delivery.qos {
		case 1:
			if r.received[delivery.topic][seq] > 0 {
				r.qos1Duplicates++
			} else {
				r.recordFirstDelivery(delivery.topic, seq)
				r.received[delivery.topic][seq] = 1
			}
			clusterDurableProcessWriteFatal(t, r.conn, 0x40, []byte{byte(delivery.packetID >> 8), byte(delivery.packetID)})
		case 2:
			if delivery.dup && r.pendingRelease[delivery.packetID] == seq {
				clusterDurableProcessWriteFatal(t, r.conn, 0x50, []byte{byte(delivery.packetID >> 8), byte(delivery.packetID)})
				return
			}
			if r.received[delivery.topic][seq] > 0 {
				t.Fatalf("receiver %q received duplicate QoS 2 delivery topic=%q seq=%d packet=%d dup=%v",
					r.clientID, delivery.topic, seq, delivery.packetID, delivery.dup)
			}
			r.recordFirstDelivery(delivery.topic, seq)
			r.received[delivery.topic][seq] = 1
			r.pendingRelease[delivery.packetID] = seq
			clusterDurableProcessWriteFatal(t, r.conn, 0x50, []byte{byte(delivery.packetID >> 8), byte(delivery.packetID)})
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

func (r *clusterRollingReceiver) recordFirstDelivery(topic string, seq int) {
	if max, ok := r.maxFirst[topic]; ok && seq < max {
		r.reorders++
	}
	if max, ok := r.maxFirst[topic]; !ok || seq > max {
		r.maxFirst[topic] = seq
	}
}

func (r *clusterRollingReceiver) complete(published map[string]int) bool {
	for topic, last := range published {
		for seq := 1; seq <= last; seq++ {
			if r.received[topic][seq] == 0 {
				return false
			}
		}
	}
	return len(r.pendingRelease) == 0
}

func (r *clusterRollingReceiver) missing(published map[string]int) string {
	missing := make([]string, 0, len(published))
	for topic, last := range published {
		absent := make([]int, 0)
		for seq := 1; seq <= last; seq++ {
			if r.received[topic][seq] == 0 {
				absent = append(absent, seq)
			}
		}
		missing = append(missing, fmt.Sprintf("%s=%v (published=%d)", topic, absent, last))
	}
	return fmt.Sprint(missing)
}

func clusterRollingAssertRetained(t *testing.T, address, token string, v5 bool, rounds int) {
	t.Helper()
	want := make(map[string]string, rounds)
	for round := 1; round <= rounds; round++ {
		want[fmt.Sprintf("roll/retained/%03d", round)] = fmt.Sprintf("retained-%03d", round)
	}
	deadline := time.Now().Add(20 * time.Second)
	var last map[string]string
	var lastDuplicates []string
	for time.Now().Before(deadline) {
		var conn net.Conn
		if v5 {
			conn = clusterSessionMetadataConnect(t, address, token, "roll-retained-v5", 0, nil)
			if clusterDurableProcessSessionPresent(t, conn) {
				t.Fatal("MQTT 5 retained checker unexpectedly resumed a session")
			}
			clusterDrainProcessSubscribeV5(t, conn, 1, "roll/retained/#", 0)
		} else {
			conn = clusterDurableProcessConnect(t, address, token, "roll-retained-v311", true)
			if clusterDurableProcessSessionPresent(t, conn) {
				t.Fatal("MQTT 3.1.1 retained checker unexpectedly resumed a session")
			}
			clusterDurableProcessSubscribe(t, conn, 1, "roll/retained/#", 0)
		}

		got := make(map[string]string, len(want))
		duplicates := make([]string, 0)
		for time.Now().Before(deadline) {
			readTimeout := 2 * time.Second
			if remaining := time.Until(deadline); remaining < readTimeout {
				readTimeout = remaining
			}
			if readTimeout <= 0 {
				break
			}
			delivery, err := clusterMatrixReadPublishFrame(conn, v5, readTimeout)
			if err != nil {
				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() {
					break
				}
				t.Fatalf("read retained replay: %v", err)
			}
			if !delivery.retain {
				t.Fatalf("retained replay topic=%q payload=%q lacks RETAIN flag", delivery.topic, delivery.payload)
			}
			if _, exists := got[delivery.topic]; exists {
				duplicates = append(duplicates, delivery.topic)
			}
			got[delivery.topic] = delivery.payload
		}
		// The MQTT 5 checker uses Session Expiry Interval 0, so its temporary subscription ends here.
		if err := clusterDurableProcessWrite(conn, 0xE0, nil); err != nil {
			t.Fatalf("disconnect retained checker: %v", err)
		}
		_ = conn.Close()
		if reflect.DeepEqual(got, want) && len(duplicates) == 0 {
			return
		}
		last = got
		lastDuplicates = duplicates
	}
	missing := make(map[string]string)
	extra := make(map[string]string)
	for topic, payload := range want {
		if got, exists := last[topic]; !exists || got != payload {
			missing[topic] = payload
		}
	}
	for topic, payload := range last {
		if _, exists := want[topic]; !exists {
			extra[topic] = payload
		}
	}
	t.Fatalf("retained replay mismatch after %d rounds: missing_or_wrong=%v extra=%v duplicates=%v observed=%v",
		rounds, missing, extra, lastDuplicates, last)
}
