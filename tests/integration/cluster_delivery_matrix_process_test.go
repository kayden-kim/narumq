package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// Traceability: CLU-03/CLU-06/CLU-07/CLU-08; docs/specs/cluster-session-and-delivery.md §3, §3.1, §4.2, §11.15.
func TestDynamicClusterCrossNodeDeliveryMatrixProcessAcceptance(t *testing.T) {
	fixture, nodes := clusterDurableProcessStart(t)
	publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "matrix-pub", true)
	t.Cleanup(func() { _ = publisher.Close() })
	ctx := fixture.ctx
	want := make([]string, 0)

	addSubscription := func(clientID, filter string, qos byte) {
		want = append(want, fmt.Sprintf("%s|%s|%d", clientID, filter, qos))
	}

	t.Run("qos-filter-matrix", func(t *testing.T) {
		type subscriber struct {
			clientID string
			filter   string
			topic    string
			qos      byte
			conn     net.Conn
		}
		subscribers := make([]subscriber, 0, 9)
		for qos := byte(0); qos <= 2; qos++ {
			suffix := fmt.Sprintf("s%d", qos)
			entries := []subscriber{
				{clientID: "matrix-exact-" + suffix, filter: "matrix/exact/" + suffix, topic: "matrix/exact/" + suffix, qos: qos, conn: clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "matrix-exact-"+suffix, false)},
				{clientID: "matrix-plus-" + suffix, filter: "matrix/plus/+/" + suffix, topic: "matrix/plus/x/" + suffix, qos: qos, conn: clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "matrix-plus-"+suffix, false)},
				{clientID: "matrix-hash-" + suffix, filter: "matrix/hash/" + suffix + "/#", topic: "matrix/hash/" + suffix + "/a/b", qos: qos, conn: clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "matrix-hash-"+suffix, false)},
			}
			for _, entry := range entries {
				clusterDurableProcessSubscribe(t, entry.conn, 1, entry.filter, qos)
				addSubscription(entry.clientID, entry.filter, qos)
				subscribers = append(subscribers, entry)
			}
			clusterMatrixWaitForSubscriptions(t, ctx, nodes, want...)
		}

		packetID := uint16(100)
		for _, subscriber := range subscribers {
			for publishQoS := byte(0); publishQoS <= 2; publishQoS++ {
				clusterMatrixPublish(t, publisher, subscriber.topic, publishQoS, packetID, fmt.Sprintf("p%d", publishQoS), false)
				packetID++
			}
		}
		for _, subscriber := range subscribers {
			seen := make(map[string]bool, 3)
			for range 3 {
				delivery := clusterMatrixReadPublish(t, subscriber.conn, false, 5*time.Second)
				if delivery.topic != subscriber.topic || delivery.dup || delivery.retain {
					t.Fatalf("client %s delivery = %+v, want topic %q, DUP=0, RETAIN=0", subscriber.clientID, delivery, subscriber.topic)
				}
				var publishQoS byte
				switch delivery.payload {
				case "p0":
					publishQoS = 0
				case "p1":
					publishQoS = 1
				case "p2":
					publishQoS = 2
				default:
					t.Fatalf("client %s payload = %q, want p0/p1/p2", subscriber.clientID, delivery.payload)
				}
				if seen[delivery.payload] {
					t.Fatalf("client %s received duplicate payload %q", subscriber.clientID, delivery.payload)
				}
				seen[delivery.payload] = true
				wantQoS := publishQoS
				if subscriber.qos < wantQoS {
					wantQoS = subscriber.qos
				}
				if delivery.qos != wantQoS || (delivery.qos > 0 && delivery.packetID == 0) {
					t.Fatalf("client %s delivery QoS/packet ID = %d/%d, want QoS %d and nonzero ID when QoS>0", subscriber.clientID, delivery.qos, delivery.packetID, wantQoS)
				}
				clusterMatrixAck(t, subscriber.conn, delivery)
			}
			if len(seen) != 3 {
				t.Fatalf("client %s payloads = %v, want p0, p1, p2", subscriber.clientID, seen)
			}
			clusterMatrixAssertQuiet(t, subscriber.conn, time.Second)
		}
	})

	t.Run("shared-group", func(t *testing.T) {
		connectionA, _, codeA, err := clusterDrainProcessConnectV5(fixture.mqttAddresses[1], fixture.accessToken, "matrix-shared-a", false)
		if err != nil || codeA != 0 {
			t.Fatalf("connect MQTT 5 shared member A: code=%d err=%v", codeA, err)
		}
		defer connectionA.Close()
		connectionB, _, codeB, err := clusterDrainProcessConnectV5(fixture.mqttAddresses[2], fixture.accessToken, "matrix-shared-b", false)
		if err != nil || codeB != 0 {
			t.Fatalf("connect MQTT 5 shared member B: code=%d err=%v", codeB, err)
		}
		defer connectionB.Close()
		filter := "$share/matrix-group/matrix/shared/#"
		clusterMatrixSubscribeV5(t, fixture, nodes, connectionA, 1, filter, 0x01, 1)
		clusterMatrixSubscribeV5(t, fixture, nodes, connectionB, 1, filter, 0x01, 1)
		addSubscription("matrix-shared-a", "matrix/shared/#", 1)
		addSubscription("matrix-shared-b", "matrix/shared/#", 1)
		clusterMatrixWaitForSubscriptions(t, ctx, nodes, want...)

		wantTopics := make(map[string]string, 6)
		packetID := uint16(200)
		sendSharedPublish := func(topic string, qos byte, packetID uint16, payload string) {
			if err := clusterMatrixSendPublish(publisher, topic, qos, packetID, payload, false); err != nil {
				for _, process := range nodes {
					exitCode, exited := process.exitCode()
					t.Logf("node=%s alive=%v exited=%t exitCode=%d logs=%q", process.name, process.isAlive(), exited, exitCode, process.logs.String())
				}
				t.Fatalf("shared publish topic=%q qos=%d packet=%d: %v", topic, qos, packetID, err)
			}
		}
		for index := 1; index <= 3; index++ {
			topic := fmt.Sprintf("matrix/shared/q1/%d", index)
			payload := fmt.Sprintf("q1-%d", index)
			wantTopics[topic] = payload
			sendSharedPublish(topic, 1, packetID, payload)
			packetID++
		}
		for index := 1; index <= 3; index++ {
			topic := fmt.Sprintf("matrix/shared/q0/%d", index)
			payload := fmt.Sprintf("q0-%d", index)
			wantTopics[topic] = payload
			sendSharedPublish(topic, 0, packetID, payload)
			packetID++
		}

		type sharedDelivery struct {
			clientID string
			delivery clusterMatrixDelivery
			err      error
		}
		events := make(chan sharedDelivery, 8)
		stopReaders := make(chan struct{})
		var readers sync.WaitGroup
		for _, member := range []struct {
			clientID string
			conn     net.Conn
		}{{"matrix-shared-a", connectionA}, {"matrix-shared-b", connectionB}} {
			readers.Add(1)
			go func(member struct {
				clientID string
				conn     net.Conn
			}) {
				defer readers.Done()
				for {
					select {
					case <-stopReaders:
						return
					default:
					}
					delivery, err := clusterMatrixReadPublishFrame(member.conn, true, 5*time.Second)
					select {
					case events <- sharedDelivery{clientID: member.clientID, delivery: delivery, err: err}:
					case <-stopReaders:
						return
					}
					if err != nil {
						return
					}
				}
			}(member)
		}
		readersStopped := false
		stopAndJoinReaders := func() {
			if readersStopped {
				return
			}
			readersStopped = true
			close(stopReaders)
			_ = connectionA.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
			_ = connectionB.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
			readers.Wait()
			_ = connectionA.SetReadDeadline(time.Time{})
			_ = connectionB.SetReadDeadline(time.Time{})
		}
		defer stopAndJoinReaders()

		seen := make(map[string]int, len(wantTopics))
		memberCounts := map[string]int{"matrix-shared-a": 0, "matrix-shared-b": 0}
		for range 6 {
			var event sharedDelivery
			select {
			case event = <-events:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for shared-subscription delivery")
			}
			if event.err != nil {
				t.Fatalf("read shared member %s: %v; seen=%v memberCounts=%v", event.clientID, event.err, seen, memberCounts)
			}
			wantPayload, ok := wantTopics[event.delivery.topic]
			if !ok || event.delivery.payload != wantPayload || event.delivery.dup || event.delivery.retain {
				t.Fatalf("shared member %s delivery = %+v (topic=%q payload=%q known=%t wantedPayload=%q dup=%t retain=%t), want one of %v",
					event.clientID, event.delivery, event.delivery.topic, event.delivery.payload, ok, wantPayload, event.delivery.dup, event.delivery.retain, wantTopics)
			}
			if seen[event.delivery.topic] != 0 {
				t.Fatalf("shared topic %q was delivered more than once", event.delivery.topic)
			}
			wantQoS := byte(1)
			if strings.HasPrefix(event.delivery.topic, "matrix/shared/q0/") {
				wantQoS = 0
			}
			if event.delivery.qos != wantQoS || (event.delivery.qos > 0 && event.delivery.packetID == 0) {
				t.Fatalf("shared topic %q QoS/packet ID=%d/%d, want QoS %d",
					event.delivery.topic, event.delivery.qos, event.delivery.packetID, wantQoS)
			}
			seen[event.delivery.topic]++
			memberCounts[event.clientID]++
			recipient := connectionA
			if event.clientID == "matrix-shared-b" {
				recipient = connectionB
			}
			clusterMatrixAck(t, recipient, event.delivery)
		}
		stopAndJoinReaders()
		for len(events) > 0 {
			event := <-events
			if event.err == nil {
				t.Fatalf("unexpected extra shared delivery from %s: %+v", event.clientID, event.delivery)
			}
		}
		if len(seen) != 6 || memberCounts["matrix-shared-a"] == 0 || memberCounts["matrix-shared-b"] == 0 {
			t.Fatalf("shared topics=%v member counts=%v; want six unique topics and both members selected", seen, memberCounts)
		}
		clusterMatrixAssertQuiet(t, connectionA, time.Second)
		clusterMatrixAssertQuiet(t, connectionB, time.Second)
	})

	t.Run("retained-and-options", func(t *testing.T) {
		clusterMatrixPublish(t, publisher, "retain/one", 1, 300, "r1", true)
		clusterMatrixWaitForRetained(t, ctx, nodes, "retain/one", len("r1"))

		retainV311 := clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "retain-v311", false)
		defer retainV311.Close()
		clusterDurableProcessSubscribe(t, retainV311, 1, "retain/#", 1)
		addSubscription("retain-v311", "retain/#", 1)
		clusterMatrixWaitForSubscriptions(t, ctx, nodes, want...)
		retained := clusterMatrixReadPublish(t, retainV311, false, 5*time.Second)
		if retained.topic != "retain/one" || retained.payload != "r1" || retained.qos != 1 || !retained.retain {
			t.Fatalf("MQTT 3.1.1 retained delivery = %+v, want retain/one/r1 QoS1 RETAIN=1", retained)
		}
		clusterMatrixAck(t, retainV311, retained)

		retainedCases := []struct {
			clientID string
			options  byte
			retained bool
			repeat   bool
		}{
			{clientID: "retain-rh0", options: 0x01, retained: true},
			{clientID: "retain-rh1", options: 0x11, retained: true, repeat: true},
			{clientID: "retain-rh2", options: 0x21, retained: false},
		}
		connections := make([]net.Conn, len(retainedCases))
		for index, test := range retainedCases {
			connection, _, code, err := clusterDrainProcessConnectV5(fixture.mqttAddresses[1], fixture.accessToken, test.clientID, false)
			if err != nil || code != 0 {
				clusterMatrixLogBrokerDiagnostics(t, fixture, nodes)
				t.Fatalf("connect MQTT 5 %s: code=%d err=%v", test.clientID, code, err)
			}
			connections[index] = connection
			t.Cleanup(func() { _ = connection.Close() })
		}
		for index, test := range retainedCases {
			connection := connections[index]
			clusterMatrixSubscribeV5(t, fixture, nodes, connection, 1, "retain/one", test.options, 1)
			addSubscription(test.clientID, "retain/one", 1)
			clusterMatrixWaitForSubscriptions(t, ctx, nodes, want...)
			if test.retained {
				delivery := clusterMatrixReadPublish(t, connection, true, 5*time.Second)
				if delivery.topic != "retain/one" || delivery.payload != "r1" || delivery.qos != 1 || !delivery.retain {
					t.Fatalf("MQTT 5 %s retained delivery = %+v, want retain/one/r1 QoS1 RETAIN=1", test.clientID, delivery)
				}
				clusterMatrixAck(t, connection, delivery)
			}
			if test.repeat {
				clusterMatrixSubscribeV5(t, fixture, nodes, connection, 2, "retain/one", test.options, 1)
				clusterMatrixAssertQuiet(t, connection, time.Second)
			}
			if !test.retained {
				clusterMatrixAssertQuiet(t, connection, time.Second)
			}
		}

		rap1, _, code, err := clusterDrainProcessConnectV5(fixture.mqttAddresses[2], fixture.accessToken, "rap-1", false)
		if err != nil || code != 0 {
			t.Fatalf("connect MQTT 5 rap-1: code=%d err=%v", code, err)
		}
		defer rap1.Close()
		rap0, _, code, err := clusterDrainProcessConnectV5(fixture.mqttAddresses[2], fixture.accessToken, "rap-0", false)
		if err != nil || code != 0 {
			t.Fatalf("connect MQTT 5 rap-0: code=%d err=%v", code, err)
		}
		defer rap0.Close()
		clusterMatrixSubscribeV5(t, fixture, nodes, rap1, 1, "rap/live", 0x09, 1)
		clusterMatrixSubscribeV5(t, fixture, nodes, rap0, 1, "rap/live", 0x01, 1)
		addSubscription("rap-1", "rap/live", 1)
		addSubscription("rap-0", "rap/live", 1)
		clusterMatrixWaitForSubscriptions(t, ctx, nodes, want...)
		clusterMatrixPublish(t, publisher, "rap/live", 1, 301, "live", true)
		for _, recipient := range []struct {
			clientID string
			conn     net.Conn
			retain   bool
		}{{"rap-1", rap1, true}, {"rap-0", rap0, false}} {
			delivery := clusterMatrixReadPublish(t, recipient.conn, true, 5*time.Second)
			if delivery.topic != "rap/live" || delivery.payload != "live" || delivery.qos != 1 || delivery.retain != recipient.retain {
				t.Fatalf("%s delivery = %+v, want RAP retain=%t", recipient.clientID, delivery, recipient.retain)
			}
			clusterMatrixAck(t, recipient.conn, delivery)
		}

		noLocal := clusterMatrixConnectV5WithServerUnavailableRetry(t, fixture, nodes, fixture.mqttAddresses[1], "nolocal-v5")
		defer noLocal.Close()
		clusterMatrixSubscribeV5(t, fixture, nodes, noLocal, 1, "nolocal/topic", 0x05, 1)
		addSubscription("nolocal-v5", "nolocal/topic", 1)
		clusterMatrixWaitForSubscriptions(t, ctx, nodes, want...)
		clusterMatrixPublishV5(t, fixture, nodes, noLocal, "nolocal/topic", 1, 7, "self")
		if err := clusterMatrixSendPublish(publisher, "nolocal/topic", 1, 302, "remote", false); err != nil {
			for _, process := range nodes {
				exitCode, exited := process.exitCode()
				t.Logf("node=%s alive=%v exited=%t exitCode=%d logs=%q", process.name, process.isAlive(), exited, exitCode, process.logs.String())
			}
			t.Fatalf("publish No Local remote topic: %v", err)
		}
		delivery := clusterMatrixReadPublish(t, noLocal, true, 5*time.Second)
		if delivery.topic != "nolocal/topic" || delivery.payload != "remote" || delivery.qos != 1 {
			t.Fatalf("No Local subscriber received %+v, want only remote publish", delivery)
		}
		clusterMatrixAck(t, noLocal, delivery)
		clusterMatrixAssertQuiet(t, noLocal, time.Second)
	})
}

func clusterMatrixConnectV5WithServerUnavailableRetry(
	t *testing.T,
	fixture *clusterMembershipProcessFixture,
	nodes []*clusterMembershipProcess,
	address, clientID string,
) net.Conn {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		connection, _, code, err := clusterDrainProcessConnectV5(address, fixture.accessToken, clientID, false)
		if err != nil {
			clusterMatrixLogBrokerDiagnostics(t, fixture, nodes)
			t.Fatalf("connect MQTT 5 %s: code=%d err=%v", clientID, code, err)
		}
		if code == 0 {
			if attempts > 1 {
				t.Logf("MQTT 5 CONNECT %s succeeded after %d transient 0x88 Server Unavailable rejections", clientID, attempts-1)
			}
			return connection
		}
		_ = connection.Close()
		if code != 0x88 {
			clusterMatrixLogBrokerDiagnostics(t, fixture, nodes)
			t.Fatalf("connect MQTT 5 %s: code=%d, want success or transient 0x88 Server Unavailable rejection", clientID, code)
		}
		time.Sleep(100 * time.Millisecond)
	}
	clusterMatrixLogBrokerDiagnostics(t, fixture, nodes)
	t.Fatalf("connect MQTT 5 %s stayed unavailable after %d attempts with reason 0x88", clientID, attempts)
	return nil
}
func clusterMatrixWaitForSubscriptions(t *testing.T, ctx context.Context, nodes []*clusterMembershipProcess, want ...string) {
	t.Helper()
	clusterDurableProcessWaitForSubscriptions(t, ctx, nodes, want...)
	for _, process := range nodes {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}
}

// clusterMatrixDelivery is one decoded cross-node MQTT PUBLISH frame.
type clusterMatrixDelivery struct {
	topic    string
	payload  string
	qos      byte
	packetID uint16
	dup      bool
	retain   bool
}

func clusterMatrixPublish(t *testing.T, connection net.Conn, topic string, qos byte, packetID uint16, payload string, retain bool) {
	t.Helper()
	if err := clusterMatrixSendPublish(connection, topic, qos, packetID, payload, retain); err != nil {
		t.Fatalf("publish topic=%q qos=%d packet=%d: %v", topic, qos, packetID, err)
	}
}

func clusterMatrixSendPublish(connection net.Conn, topic string, qos byte, packetID uint16, payload string, retain bool) error {
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	if qos > 0 {
		body = append(body, byte(packetID>>8), byte(packetID))
	}
	body = append(body, payload...)
	flags := qos << 1
	if retain {
		flags |= 1
	}
	if err := clusterDurableProcessWrite(connection, 0x30|flags, body); err != nil {
		return err
	}
	switch qos {
	case 1:
		return clusterMatrixAwaitAck(connection, 4, packetID, 15*time.Second)
	case 2:
		if err := clusterMatrixAwaitAck(connection, 5, packetID, 15*time.Second); err != nil {
			return err
		}
		if err := clusterDurableProcessWrite(connection, 0x62, []byte{byte(packetID >> 8), byte(packetID)}); err != nil {
			return err
		}
		return clusterMatrixAwaitAck(connection, 7, packetID, 15*time.Second)
	default:
		return nil
	}
}

func clusterMatrixAwaitAck(connection net.Conn, want byte, packetID uint16, timeout time.Duration) error {
	_ = connection.SetReadDeadline(time.Now().Add(timeout))
	defer connection.SetReadDeadline(time.Time{})
	kind, _, body, err := clusterDurableProcessReadRaw(connection)
	if err != nil {
		return fmt.Errorf("read MQTT ACK kind=%d packet=%d: %w", want, packetID, err)
	}
	if kind != want || len(body) < 2 || binary.BigEndian.Uint16(body[:2]) != packetID {
		return fmt.Errorf("MQTT ACK kind=%d body=%x, want kind=%d packet=%d", kind, body, want, packetID)
	}
	return nil
}

func clusterMatrixPublishV5(t *testing.T, fixture *clusterMembershipProcessFixture, nodes []*clusterMembershipProcess, connection net.Conn, topic string, qos byte, packetID uint16, payload string) {
	t.Helper()
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	if qos > 0 {
		body = append(body, byte(packetID>>8), byte(packetID))
	}
	body = append(body, 0)
	body = append(body, payload...)
	if err := clusterDurableProcessWrite(connection, 0x30|qos<<1, body); err != nil {
		t.Fatalf("send MQTT 5 QoS %d PUBLISH: %v", qos, err)
	}
	if qos == 1 && !clusterDurableFaultAwaitAck(t, connection, 4, packetID, 5*time.Second) {
		clusterMatrixLogBrokerDiagnostics(t, fixture, nodes)
		t.Fatalf("MQTT 5 QoS 1 PUBLISH packet %d did not receive PUBACK", packetID)
	}
}

func clusterMatrixReadPublish(t *testing.T, connection net.Conn, v5 bool, timeout time.Duration) clusterMatrixDelivery {
	t.Helper()
	delivery, err := clusterMatrixReadPublishFrame(connection, v5, timeout)
	if err != nil {
		t.Fatalf("read MQTT PUBLISH: %v", err)
	}
	return delivery
}

func clusterMatrixReadPublishFrame(connection net.Conn, v5 bool, timeout time.Duration) (clusterMatrixDelivery, error) {
	_ = connection.SetReadDeadline(time.Now().Add(timeout))
	defer connection.SetReadDeadline(time.Time{})
	kind, flags, body, err := clusterDurableProcessReadRaw(connection)
	if err != nil {
		return clusterMatrixDelivery{}, err
	}
	if kind != 3 || len(body) < 2 {
		return clusterMatrixDelivery{}, fmt.Errorf("unexpected MQTT packet kind=%d body=%x", kind, body)
	}
	topicLength := int(binary.BigEndian.Uint16(body[:2]))
	if topicLength > len(body)-2 {
		return clusterMatrixDelivery{}, fmt.Errorf("truncated MQTT PUBLISH topic: %x", body)
	}
	delivery := clusterMatrixDelivery{
		topic:  string(body[2 : 2+topicLength]),
		qos:    (flags >> 1) & 3,
		dup:    flags&8 != 0,
		retain: flags&1 != 0,
	}
	if delivery.qos > 2 {
		return clusterMatrixDelivery{}, fmt.Errorf("invalid MQTT PUBLISH QoS %d", delivery.qos)
	}
	offset := 2 + topicLength
	if delivery.qos > 0 {
		if offset+2 > len(body) {
			return clusterMatrixDelivery{}, fmt.Errorf("truncated MQTT PUBLISH packet ID: %x", body)
		}
		delivery.packetID = binary.BigEndian.Uint16(body[offset : offset+2])
		offset += 2
	}
	if v5 {
		propertyLength, propertiesStart, err := clusterDrainProcessReadVariableByteInteger(body, offset)
		if err != nil || propertyLength > len(body)-propertiesStart {
			return clusterMatrixDelivery{}, fmt.Errorf("MQTT 5 PUBLISH property length=%d offset=%d body=%x err=%v",
				propertyLength, propertiesStart, body, err)
		}
		offset = propertiesStart + propertyLength
	}
	delivery.payload = string(body[offset:])
	return delivery, nil
}

func clusterMatrixAck(t *testing.T, connection net.Conn, delivery clusterMatrixDelivery) {
	t.Helper()
	body := []byte{byte(delivery.packetID >> 8), byte(delivery.packetID)}
	switch delivery.qos {
	case 0:
	case 1:
		clusterDurableProcessWriteFatal(t, connection, 0x40, body)
	case 2:
		clusterDurableProcessWriteFatal(t, connection, 0x50, body)
		clusterDurableProcessAck(t, connection, 6, delivery.packetID)
		clusterDurableProcessWriteFatal(t, connection, 0x70, body)
	default:
		t.Fatalf("cannot acknowledge invalid QoS %d", delivery.qos)
	}
}

func clusterMatrixAssertQuiet(t *testing.T, connection net.Conn, duration time.Duration) {
	t.Helper()
	_ = connection.SetReadDeadline(time.Now().Add(duration))
	kind, flags, body, err := clusterDurableProcessReadRaw(connection)
	_ = connection.SetReadDeadline(time.Time{})
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return
	}
	if err != nil {
		t.Fatalf("quiet MQTT connection ended with error: %v", err)
	}
	t.Fatalf("unexpected MQTT packet while quiet: kind=%d flags=%#x body=%x", kind, flags, body)
}

func clusterMatrixSubscribeV5(t *testing.T, fixture *clusterMembershipProcessFixture, nodes []*clusterMembershipProcess, connection net.Conn, packetID uint16, filter string, options byte, grantedQoS byte) {
	t.Helper()
	body := []byte{byte(packetID >> 8), byte(packetID), 0, byte(len(filter) >> 8), byte(len(filter))}
	body = append(body, filter...)
	body = append(body, options)
	if err := clusterDurableProcessWrite(connection, 0x82, body); err != nil {
		t.Fatalf("send MQTT 5 SUBSCRIBE: %v", err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	kind, _, ack, err := clusterDurableProcessReadRaw(connection)
	_ = connection.SetReadDeadline(time.Time{})
	if err != nil || kind != 9 || len(ack) != 4 || binary.BigEndian.Uint16(ack[:2]) != packetID || ack[2] != 0 || ack[3] != grantedQoS {
		clusterMatrixLogBrokerDiagnostics(t, fixture, nodes)
		t.Fatalf("MQTT 5 SUBACK kind=%d body=%x err=%v, want packet=%d reason=%d", kind, ack, err, packetID, grantedQoS)
	}
}
func clusterMatrixWaitForRetained(t *testing.T, ctx context.Context, nodes []*clusterMembershipProcess, topic string, payloadBytes int) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("retained projection for %q", topic), func() (bool, string, error) {
		for _, process := range nodes {
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
				process.managementURL()+"/api/v1/retained?tenantId=acceptance", nil, clusterMembershipProcessAdmin, "")
			if err != nil {
				return false, fmt.Sprintf("%s retained query: %v", process.name, err), nil
			}
			if status != http.StatusOK {
				return false, fmt.Sprintf("%s retained status=%d body=%s", process.name, status, body), nil
			}
			var response struct {
				Data []struct {
					TenantID     string `json:"tenantId"`
					Topic        string `json:"topic"`
					PayloadBytes int    `json:"payloadBytes"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &response); err != nil {
				return false, "", fmt.Errorf("%s decode retained response: %w", process.name, err)
			}
			found := false
			for _, retained := range response.Data {
				if retained.TenantID == "acceptance" && retained.Topic == topic && retained.PayloadBytes == payloadBytes {
					found = true
					break
				}
			}
			if !found {
				return false, fmt.Sprintf("%s retained projection=%+v, want %q with %d payload bytes",
					process.name, response.Data, topic, payloadBytes), nil
			}
		}
		return true, "all node retained projections agree", nil
	})
}

func clusterMatrixLogBrokerDiagnostics(t *testing.T, fixture *clusterMembershipProcessFixture, nodes []*clusterMembershipProcess) {
	t.Helper()
	for _, process := range nodes {
		exitCode, exited := process.exitCode()
		t.Logf("node=%s alive=%v exited=%t exitCode=%d logs=%q",
			process.name, process.isAlive(), exited, exitCode, process.logs.String())

		readinessCtx, cancelReadiness := context.WithTimeout(fixture.ctx, time.Second)
		readinessStatus, readinessBody, readinessErr := clusterMembershipProcessRequest(
			readinessCtx, http.MethodGet, process.managementURL()+"/health/ready", nil, "", "")
		cancelReadiness()
		t.Logf("node=%s readiness status=%d body=%s err=%v",
			process.name, readinessStatus, readinessBody, readinessErr)

		summaryCtx, cancelSummary := context.WithTimeout(fixture.ctx, time.Second)
		summary, summaryStatus, summaryBody, summaryErr := clusterMembershipProcessGetSummary(
			summaryCtx, process.managementURL(), clusterMembershipProcessAdmin)
		cancelSummary()
		t.Logf("node=%s cluster status=%d summary=%+v body=%q err=%v",
			process.name, summaryStatus, summary, summaryBody, summaryErr)
	}
}
