package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"
)

const clusterRetainedQoSPrefix = "retained/qos12/"
const clusterRetainedQoSFilter = "retained/qos12/#"

type clusterRetainedQoSValue struct {
	qos          int
	payloadBytes int
}

// Traceability: PRD MQTT-04/CLU-08; docs/specs/mqtt-protocol-and-session.md §5; docs/specs/cluster-session-and-delivery.md §4.2, §11.32.
func TestDynamicClusterRetainedQoS12ProcessAcceptance(t *testing.T) {
	t.Parallel()

	// 1. Persistent live subscriber and cross-node publisher.
	fixture, nodes := clusterDurableProcessStart(t)
	t.Cleanup(func() {
		if t.Failed() {
			clusterMatrixLogBrokerDiagnostics(t, fixture, nodes)
		}
	})
	addr, token, ctx := fixture.mqttAddresses, fixture.accessToken, fixture.ctx
	live := clusterDurableProcessConnect(t, addr[2], token, "retained-qos12-live", false)
	t.Cleanup(func() { _ = live.Close() })
	clusterDurableProcessSubscribe(t, live, 1, clusterRetainedQoSFilter, 2)
	clusterMembershipProcessWaitForRouteProjection(t, ctx, nodes, clusterRetainedQoSFilter)
	pub := clusterDurableProcessConnect(t, addr[0], token, "retained-qos12-pub", true)
	t.Cleanup(func() { _ = pub.Close() })
	for _, process := range nodes {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}
	d := func(topic, payload string, qos byte) clusterMatrixDelivery {
		return clusterMatrixDelivery{topic: clusterRetainedQoSPrefix + topic, payload: payload, qos: qos, retain: true}
	}
	replay := func(node, id int, qos byte, want ...clusterMatrixDelivery) {
		t.Helper()
		got := clusterRetainedQoSReplay(t, addr[node], token, fmt.Sprintf("retained-qos12-replay-%d", id), qos)
		clusterRetainedQoSAssertReplay(t, got, want)
	}
	one := clusterRetainedQoSPrefix + "one"
	two := clusterRetainedQoSPrefix + "two"

	// 2. Successful QoS 1/2 ACKs follow retained commit and live delivery.
	clusterMatrixPublish(t, pub, one, 1, 11, "one", true)
	clusterRetainedQoSReadLive(t, live, one, "one", 1)
	clusterMatrixPublish(t, pub, two, 2, 12, "two", true)
	clusterRetainedQoSReadLive(t, live, two, "two", 2)
	clusterRetainedQoSWaitForState(t, ctx, nodes, map[string]clusterRetainedQoSValue{
		one: {1, 3}, two: {2, 3},
	})

	// 3. Replay preserves stored QoS, capped by each granted QoS.
	replay(1, 1, 2, d("one", "one", 1), d("two", "two", 2))
	replay(2, 2, 1, d("one", "one", 1), d("two", "two", 1))
	replay(0, 3, 0, d("one", "one", 0), d("two", "two", 0))

	// 4. Replace also changes stored QoS; zero payload deletes but fans out.
	clusterMatrixPublish(t, pub, one, 2, 13, "uno", true)
	clusterRetainedQoSReadLive(t, live, one, "uno", 2)
	clusterMatrixPublish(t, pub, two, 1, 14, "", true)
	clusterRetainedQoSReadLive(t, live, two, "", 1)
	clusterRetainedQoSWaitForState(t, ctx, nodes, map[string]clusterRetainedQoSValue{one: {2, 3}})
	replay(1, 4, 2, d("one", "uno", 2))

	// 5. MQTT 5 expiry applies to replay on MQTT 3.1.1 connections too.
	v5 := clusterMatrixConnectV5WithServerUnavailableRetry(t, fixture, nodes, addr[1], "retained-qos12-pub-v5")
	t.Cleanup(func() { _ = v5.Close() })
	start := time.Now()
	expiring1 := clusterRetainedQoSPrefix + "expiring-1"
	expiring2 := clusterRetainedQoSPrefix + "expiring-2"
	clusterRetainedQoSPublishV5(t, v5, expiring1, 1, 21, "e1", 8)
	clusterRetainedQoSReadLive(t, live, expiring1, "e1", 1)
	clusterRetainedQoSPublishV5(t, v5, expiring2, 2, 22, "e2", 8)
	clusterRetainedQoSReadLive(t, live, expiring2, "e2", 2)
	clusterRetainedQoSWaitForState(t, ctx, nodes, map[string]clusterRetainedQoSValue{
		expiring1: {1, 2}, expiring2: {2, 2}, one: {2, 3},
	})
	if elapsed := time.Since(start); elapsed >= 7*time.Second {
		t.Fatalf("expiry window exhausted before replay: %s", elapsed)
	}
	replay(0, 5, 2, d("expiring-1", "e1", 1), d("expiring-2", "e2", 2), d("one", "uno", 2))
	time.Sleep(time.Until(start.Add(9 * time.Second)))
	clusterRetainedQoSWaitForState(t, ctx, nodes, map[string]clusterRetainedQoSValue{one: {2, 3}})
	replay(2, 6, 2, d("one", "uno", 2))
	_ = v5.Close()

	// 6. ACKed retained data survives one voter loss (CLU-08).
	if err := nodes[0].crash(5 * time.Second); err != nil {
		t.Fatalf("crash retained publisher voter: %v", err)
	}
	_ = pub.Close()
	replay(1, 7, 2, d("one", "uno", 2))

	// 7. QoS 2 RECEIVE is committed, but PUBREL has not begun fan-out.
	nq1 := clusterDurableProcessConnect(t, addr[2], token, "retained-qos12-nq-1", true)
	t.Cleanup(func() { _ = nq1.Close() })
	nq2 := clusterDurableProcessConnect(t, addr[2], token, "retained-qos12-nq-2", false)
	t.Cleanup(func() { _ = nq2.Close() })
	pending := clusterRetainedQoSPrefix + "pending"
	if err := clusterRetainedQoSSendPublish(nq2, pending, 2, 41, "pending"); err != nil {
		t.Fatalf("send pending QoS 2 retained: %v", err)
	}
	clusterDurableProcessAck(t, nq2, 5, 41)
	clusterRetainedQoSPing(t, live)

	// 8. No successful PUBACK/PUBCOMP or live fan-out without quorum.
	if err := nodes[1].crash(5 * time.Second); err != nil {
		t.Fatalf("crash second retained voter: %v", err)
	}
	fail := clusterRetainedQoSPrefix + "fail"
	if err := clusterRetainedQoSSendPublish(nq1, fail, 1, 42, "fail"); err != nil {
		t.Fatalf("send non-quorum QoS 1 retained: %v", err)
	}
	clusterRetainedQoSAssertNoSuccessFor(t, nq1, 4, 42, 8*time.Second)
	clusterDurableProcessPubrel(t, nq2, 41)
	clusterRetainedQoSAssertNoSuccessFor(t, nq2, 7, 41, 8*time.Second)
	liveOpen := clusterRetainedQoSPingIfOpen(t, live)
	t.Logf("retained quorum-loss liveOpen=%t", liveOpen)

	// 9. Restart the original voter data directories and restore quorum.
	for _, i := range []int{0, 1} {
		restarted, err := fixture.harness.start(fmt.Sprintf("retained qos12 voter %d restart", i+1),
			fixture.bootstrapConfig(uint64(i+1)), fixture.managementAddresses[i])
		if err != nil {
			t.Fatalf("restart retained voter %d: %v", i+1, err)
		}
		nodes[i] = restarted
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)

	// 10. Resume the persistent publisher's original QoS 2 exchange.
	_ = nq1.Close()
	_ = nq2.Close()
	if !liveOpen {
		_ = live.Close()
		live = clusterDurableProcessConnect(t, addr[2], token, "retained-qos12-live", false)
		if !clusterDurableProcessSessionPresent(t, live) {
			t.Fatal("retained live reconnect returned SessionPresent=false")
		}
	}
	nq2 = clusterDurableProcessConnect(t, addr[2], token, "retained-qos12-nq-2", false)
	if !clusterDurableProcessSessionPresent(t, nq2) {
		t.Fatal("retained QoS 2 publisher reconnect returned SessionPresent=false")
	}
	clusterDurableProcessPubrel(t, nq2, 41)
	clusterDurableProcessAck(t, nq2, 7, 41)
	clusterRetainedQoSReadLive(t, live, pending, "pending", 2)
	clusterRetainedQoSPing(t, live)

	// 11. A caller timeout does not prove retained mutation was unapplied.
	clusterMatrixWaitForRetained(t, ctx, nodes, pending, len("pending"))
	got := clusterRetainedQoSReplay(t, addr[0], token, "retained-qos12-replay-8", 2)
	filtered := got[:0]
	lateCommit := false
	for _, delivery := range got {
		if delivery.topic == fail {
			if delivery.payload != "fail" || delivery.qos != 1 {
				t.Fatalf("late retained commit=%+v, want payload=fail qos=1", delivery)
			}
			lateCommit = true
			t.Logf("late-committed retained %q observed; Cluster §4.2 caller-timeout boundary", fail)
			continue
		}
		filtered = append(filtered, delivery)
	}
	t.Logf("retained recovery lateFailCommit=%t", lateCommit)
	clusterRetainedQoSAssertReplay(t, filtered, []clusterMatrixDelivery{d("one", "uno", 2), d("pending", "pending", 2)})
}

func clusterRetainedQoSWaitForState(t *testing.T, ctx context.Context, nodes []*clusterMembershipProcess, want map[string]clusterRetainedQoSValue) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, "retained QoS 1/2 projections", func() (bool, string, error) {
		for _, process := range nodes {
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
				process.managementURL()+"/api/v1/retained?tenantId=acceptance&topicPrefix="+url.QueryEscape(clusterRetainedQoSPrefix),
				nil, clusterMembershipProcessAdmin, "")
			if err != nil {
				return false, fmt.Sprintf("%s retained query: %v", process.name, err), nil
			}
			if status != http.StatusOK {
				return false, fmt.Sprintf("%s retained status=%d body=%s", process.name, status, body), nil
			}
			var response struct {
				Data []struct {
					Topic        string `json:"topic"`
					QoS          int    `json:"qos"`
					PayloadBytes int    `json:"payloadBytes"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &response); err != nil {
				return false, "", fmt.Errorf("%s decode retained response: %w", process.name, err)
			}
			got := make(map[string]clusterRetainedQoSValue, len(response.Data))
			for _, value := range response.Data {
				got[value.Topic] = clusterRetainedQoSValue{qos: value.QoS, payloadBytes: value.PayloadBytes}
			}
			if !reflect.DeepEqual(got, want) {
				return false, fmt.Sprintf("%s retained state=%v, want %v", process.name, got, want), nil
			}
		}
		return true, "all node retained projections agree", nil
	})
}

func clusterRetainedQoSReplay(t *testing.T, address, token, clientID string, grantedQoS byte) []clusterMatrixDelivery {
	t.Helper()
	conn := clusterDurableProcessConnect(t, address, token, clientID, true)
	defer conn.Close()
	clusterDurableProcessSubscribe(t, conn, 1, clusterRetainedQoSFilter, grantedQoS)
	var deliveries []clusterMatrixDelivery
	seen := make(map[string]bool)
	pending := make(map[uint16]bool)
	defer conn.SetReadDeadline(time.Time{})
	for {
		window := time.Second
		if len(pending) > 0 {
			window = 5 * time.Second
		}
		_ = conn.SetReadDeadline(time.Now().Add(window))
		kind, flags, body, err := clusterDurableProcessReadRaw(conn)
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() && len(pending) == 0 {
			return deliveries
		}
		if err != nil {
			t.Fatalf("retained replay %s: pending=%v err=%v", clientID, pending, err)
		}
		// Independent QoS 2 exchanges can send another PUBLISH before PUBREL.
		if kind == 6 {
			if len(body) < 2 || !pending[binary.BigEndian.Uint16(body[:2])] {
				t.Fatalf("unexpected retained PUBREL %s: %x", clientID, body)
			}
			id := binary.BigEndian.Uint16(body[:2])
			clusterDurableProcessWriteFatal(t, conn, 0x70, body[:2])
			delete(pending, id)
			continue
		}
		if kind != 3 || len(body) < 2 {
			t.Fatalf("unexpected retained packet %s: kind=%d flags=%#x body=%x", clientID, kind, flags, body)
		}
		topicLength := int(binary.BigEndian.Uint16(body[:2]))
		if topicLength > len(body)-2 {
			t.Fatalf("truncated retained topic %s: %x", clientID, body)
		}
		delivery := clusterMatrixDelivery{
			topic: string(body[2 : 2+topicLength]), qos: (flags >> 1) & 3,
			retain: flags&1 != 0, dup: flags&8 != 0,
		}
		offset := 2 + topicLength
		if delivery.qos > 0 {
			if offset+2 > len(body) {
				t.Fatalf("truncated retained packet ID %s: %x", clientID, body)
			}
			delivery.packetID = binary.BigEndian.Uint16(body[offset : offset+2])
			offset += 2
		}
		delivery.payload = string(body[offset:])
		if !delivery.retain || delivery.dup || delivery.qos > 2 || seen[delivery.topic] {
			t.Fatalf("invalid or duplicate retained replay %s: %+v", clientID, delivery)
		}
		seen[delivery.topic] = true
		deliveries = append(deliveries, delivery)
		if delivery.qos == 2 {
			if delivery.packetID == 0 || pending[delivery.packetID] {
				t.Fatalf("invalid retained QoS 2 packet ID %s: %+v", clientID, delivery)
			}
			pending[delivery.packetID] = true
			clusterDurableProcessWriteFatal(t, conn, 0x50, []byte{byte(delivery.packetID >> 8), byte(delivery.packetID)})
		} else {
			clusterMatrixAck(t, conn, delivery)
		}
	}
}

func clusterRetainedQoSAssertReplay(t *testing.T, got, want []clusterMatrixDelivery) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("retained replay=%+v, want %+v", got, want)
	}
	for i, delivery := range got {
		expected := want[i]
		if delivery.topic != expected.topic || delivery.payload != expected.payload || delivery.qos != expected.qos || delivery.retain != expected.retain {
			t.Fatalf("retained replay[%d]=%+v, want %+v", i, delivery, expected)
		}
	}
}

func clusterRetainedQoSSendPublish(conn net.Conn, topic string, qos byte, packetID uint16, payload string) error {
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	if qos > 0 {
		body = append(body, byte(packetID>>8), byte(packetID))
	}
	body = append(body, payload...)
	return clusterDurableProcessWrite(conn, 0x30|qos<<1|1, body)
}

func clusterRetainedQoSPublishV5(t *testing.T, conn net.Conn, topic string, qos byte, packetID uint16, payload string, expirySeconds uint32) {
	t.Helper()
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	body = append(body, byte(packetID>>8), byte(packetID), 5, 0x02)
	body = binary.BigEndian.AppendUint32(body, expirySeconds)
	body = append(body, payload...)
	if err := clusterDurableProcessWrite(conn, 0x30|qos<<1|1, body); err != nil {
		t.Fatalf("send retained MQTT 5 PUBLISH: %v", err)
	}
	await := func(want byte) {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		kind, _, ack, err := clusterDurableProcessReadRaw(conn)
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil || kind != want || len(ack) < 2 || binary.BigEndian.Uint16(ack[:2]) != packetID || (len(ack) >= 3 && ack[2] >= 0x80) {
			t.Fatalf("retained MQTT 5 ACK kind=%d body=%x err=%v, want kind=%d packet=%d success", kind, ack, err, want, packetID)
		}
	}
	if qos == 1 {
		await(4)
	} else {
		await(5)
		clusterDurableProcessPubrel(t, conn, packetID)
		await(7)
	}
}

func clusterRetainedQoSAssertNoSuccessFor(t *testing.T, conn net.Conn, kind byte, packetID uint16, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	_ = conn.SetReadDeadline(deadline)
	defer conn.SetReadDeadline(time.Time{})
	for time.Now().Before(deadline) {
		got, flags, body, err := clusterDurableProcessReadRaw(conn)
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return
		}
		if clusterMembershipProcessIsClosed(err) {
			return
		}
		if err != nil {
			t.Fatalf("read non-quorum retained response: %v", err)
		}
		if got == kind && len(body) >= 2 && binary.BigEndian.Uint16(body[:2]) == packetID && (len(body) < 3 || body[2] == 0) {
			t.Fatalf("success packet kind=%d packet=%d without retained quorum", kind, packetID)
		}
		t.Logf("non-success retained response: kind=%d flags=%#x body=%x", got, flags, body)
	}
}

func clusterRetainedQoSPing(t *testing.T, conn net.Conn) {
	t.Helper()
	if !clusterRetainedQoSPingIfOpen(t, conn) {
		t.Fatal("expected open MQTT connection for retained delivery barrier")
	}
}

func clusterRetainedQoSPingIfOpen(t *testing.T, conn net.Conn) bool {
	t.Helper()
	if err := clusterDurableProcessWrite(conn, 0xC0, nil); err != nil {
		if clusterMembershipProcessIsClosed(err) {
			t.Logf("retained live connection closed while writing PINGREQ: %v", err)
			return false
		}
		t.Fatalf("write retained PINGREQ: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	kind, flags, body, err := clusterDurableProcessReadRaw(conn)
	_ = conn.SetReadDeadline(time.Time{})
	if clusterMembershipProcessIsClosed(err) {
		t.Logf("retained live connection closed while reading PINGRESP: %v", err)
		return false
	}
	if err != nil || kind != 13 {
		t.Fatalf("retained PINGRESP kind=%d flags=%#x body=%x err=%v", kind, flags, body, err)
	}
	return true
}

func clusterRetainedQoSReadLive(t *testing.T, conn net.Conn, topic, payload string, qos byte) {
	t.Helper()
	delivery, err := clusterMatrixReadPublishFrame(conn, false, 10*time.Second)
	if err != nil {
		t.Fatalf("read retained live delivery: %v", err)
	}
	if delivery.topic != topic || delivery.payload != payload || delivery.qos != qos || delivery.retain {
		t.Fatalf("retained live delivery=%+v, want topic=%q payload=%q qos=%d retain=false", delivery, topic, payload, qos)
	}
	clusterMatrixAck(t, conn, delivery)
}
