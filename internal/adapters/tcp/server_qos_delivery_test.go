package tcp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	"narumq/internal/application/connect"
	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/delivery"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

func takeDeliveries(t *testing.T, queue applicationdeliveryqueue.Queue, sessionID routing.SessionID, now time.Time) []applicationdeliveryqueue.DeliveryDTO {
	t.Helper()
	var items []applicationdeliveryqueue.DeliveryDTO
	for {
		item, ok, err := queue.Peek(sessionID, now)
		if errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
			return items
		}
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return items
		}
		items = append(items, item)
		if err := queue.CompleteHead(sessionID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDispatchPublishQoS1AndDuplicate(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = "pub-session"
	conn.version = mqtt.ProtocolV5

	runtime.server.deliveries.Register("sub-session")
	if _, err := runtime.server.subscriptions.Subscribe("sub-session", mqtt.Subscribe{
		Version: mqtt.ProtocolV5,
		Subscriptions: []mqtt.Subscription{
			{Filter: "sensors/temp", QoS: mqtt.QoSAtLeastOnce},
		},
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}

	// PacketID = 0 should return ErrProtocol
	err := conn.dispatchPublish(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "sensors/temp",
		Payload:  []byte("20"),
		QoS:      mqtt.QoSAtLeastOnce,
		PacketID: 0,
	})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("expected ErrProtocol for PacketID 0, got %v", err)
	}

	// Valid QoS 1 publish
	pubPacket := mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "sensors/temp",
		Payload:  []byte("20"),
		QoS:      mqtt.QoSAtLeastOnce,
		PacketID: 10,
	}
	if err := conn.dispatchPublish(pubPacket); err != nil {
		t.Fatalf("dispatchPublish failed: %v", err)
	}

	// Verify PUBACK was enqueued to control queue
	select {
	case msg := <-conn.controlQueue:
		puback, ok := msg.packet.(*mqtt.PubAck)
		if !ok || puback.PacketID != 10 {
			t.Fatalf("expected PubAck with PacketID 10, got %#v", msg.packet)
		}
	default:
		t.Fatal("expected PubAck in control queue")
	}

	// Verify delivery was enqueued for sub-session
	items := takeDeliveries(t, runtime.server.deliveries, "sub-session", time.Time{})
	if len(items) != 1 || string(items[0].Payload) != "20" {
		t.Fatalf("expected 1 delivered item, got %#v", items)
	}

	// Duplicate QoS 1 publish should return identical PUBACK without delivering again
	dupPacket := pubPacket
	dupPacket.DUP = true
	if err := conn.dispatchPublish(dupPacket); err != nil {
		t.Fatalf("duplicate dispatchPublish failed: %v", err)
	}

	select {
	case msg := <-conn.controlQueue:
		puback, ok := msg.packet.(*mqtt.PubAck)
		if !ok || puback.PacketID != 10 {
			t.Fatalf("expected duplicate PubAck with PacketID 10, got %#v", msg.packet)
		}
	default:
		t.Fatal("expected duplicate PubAck in control queue")
	}

	// Verify no new items in delivery queue
	itemsAfterDup := takeDeliveries(t, runtime.server.deliveries, "sub-session", time.Time{})
	if len(itemsAfterDup) != 0 {
		t.Fatalf("duplicate publish caused extra delivery: %#v", itemsAfterDup)
	}
}

func TestDispatchPublishQoS2AndHandshake(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = "pub2-session"
	conn.version = mqtt.ProtocolV5
	runtime.server.deliveries.Register("sub2-session")
	if _, err := runtime.server.subscriptions.Subscribe("sub2-session", mqtt.Subscribe{
		Version: mqtt.ProtocolV5,
		Subscriptions: []mqtt.Subscription{
			{Filter: "events/state", QoS: mqtt.QoSExactlyOnce},
		},
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}

	// PacketID 0 -> ErrProtocol
	err := conn.dispatchPublish(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "events/state",
		Payload:  []byte("on"),
		QoS:      mqtt.QoSExactlyOnce,
		PacketID: 0,
	})
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("expected ErrProtocol for PacketID 0, got %v", err)
	}

	// Send initial QoS 2 publish
	pubPacket := mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "events/state",
		Payload:  []byte("on"),
		QoS:      mqtt.QoSExactlyOnce,
		PacketID: 25,
	}
	if err := conn.dispatchPublish(pubPacket); err != nil {
		t.Fatalf("dispatchPublish QoS 2 failed: %v", err)
	}

	// Verify PUBREC was enqueued
	select {
	case msg := <-conn.controlQueue:
		pubrec, ok := msg.packet.(*mqtt.PubRec)
		if !ok || pubrec.PacketID != 25 {
			t.Fatalf("expected PubRec with PacketID 25, got %#v", msg.packet)
		}
	default:
		t.Fatal("expected PubRec in control queue")
	}

	// Message should NOT be delivered yet prior to PUBREL
	items := takeDeliveries(t, runtime.server.deliveries, "sub2-session", time.Time{})
	if len(items) != 0 {
		t.Fatalf("QoS 2 delivered prematurely: %#v", items)
	}

	// Duplicate QoS 2 publish before PUBREL -> should re-send PUBREC
	dupPacket := pubPacket
	dupPacket.DUP = true
	if err := conn.dispatchPublish(dupPacket); err != nil {
		t.Fatalf("duplicate dispatchPublish QoS 2 failed: %v", err)
	}
	select {
	case msg := <-conn.controlQueue:
		pubrec, ok := msg.packet.(*mqtt.PubRec)
		if !ok || pubrec.PacketID != 25 {
			t.Fatalf("expected duplicate PubRec with PacketID 25, got %#v", msg.packet)
		}
	default:
		t.Fatal("expected duplicate PubRec in control queue")
	}

	// PUBREL with PacketID 0 -> ErrProtocol
	if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 0}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("expected ErrProtocol for PubRel PacketID 0, got %v", err)
	}

	// Client sends PUBREL with PacketID 25
	if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 25}); err != nil {
		t.Fatalf("dispatchPubRel failed: %v", err)
	}

	// Verify PUBCOMP was enqueued
	select {
	case msg := <-conn.controlQueue:
		pubcomp, ok := msg.packet.(*mqtt.PubComp)
		if !ok || pubcomp.PacketID != 25 {
			t.Fatalf("expected PubComp with PacketID 25, got %#v", msg.packet)
		}
	default:
		t.Fatal("expected PubComp in control queue")
	}

	// Verify message was delivered upon PUBREL
	itemsAfterRel := takeDeliveries(t, runtime.server.deliveries, "sub2-session", time.Time{})
	if len(itemsAfterRel) != 1 || string(itemsAfterRel[0].Payload) != "on" {
		t.Fatalf("expected 1 delivered item after PUBREL, got %#v", itemsAfterRel)
	}

	// Duplicate PUBREL after complete -> should re-send PUBCOMP without extra delivery
	if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 25}); err != nil {
		t.Fatalf("duplicate dispatchPubRel failed: %v", err)
	}
	select {
	case msg := <-conn.controlQueue:
		pubcomp, ok := msg.packet.(*mqtt.PubComp)
		if !ok || pubcomp.PacketID != 25 {
			t.Fatalf("expected duplicate PubComp with PacketID 25, got %#v", msg.packet)
		}
	default:
		t.Fatal("expected duplicate PubComp in control queue")
	}

	// Unknown PUBREL -> PubComp with ReasonCode 0x92 in v5
	if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 999}); err != nil {
		t.Fatalf("unknown dispatchPubRel failed: %v", err)
	}
	select {
	case msg := <-conn.controlQueue:
		pubcomp, ok := msg.packet.(*mqtt.PubComp)
		if !ok || pubcomp.PacketID != 999 || pubcomp.ReasonCode != 0x92 {
			t.Fatalf("expected PubComp with 0x92 for unknown ID, got %#v", msg.packet)
		}
	default:
		t.Fatal("expected PubComp for unknown ID in control queue")
	}
	// A subsequent inbound exchange must still acquire deliveryMu.
	nextPublish := make(chan error, 1)
	go func() {
		nextPublish <- conn.dispatchPublish(mqtt.Publish{
			Version: mqtt.ProtocolV5, Topic: "after/unknown-pubrel", Payload: []byte("next"),
			QoS: mqtt.QoSExactlyOnce, PacketID: 1000,
		})
	}()
	select {
	case err := <-nextPublish:
		if err != nil {
			t.Fatalf("next QoS2 PUBLISH after unknown PUBREL: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("next QoS2 PUBLISH blocked after unknown PUBREL")
	}
	select {
	case msg := <-conn.controlQueue:
		pubrec, ok := msg.packet.(*mqtt.PubRec)
		if !ok || pubrec.PacketID != 1000 {
			t.Fatalf("next QoS2 response = %#v, want PUBREC for 1000", msg.packet)
		}
	default:
		t.Fatal("next QoS2 PUBLISH did not enqueue PUBREC")
	}
}

// Traceability: PRD MQTT-08/TEST-03/TEST-04/TEST-05; docs/specs/mqtt-protocol-and-session.md §5; MQTT-3.3.2-5/6.
func TestQoS2MessageExpiryUsesOriginalReceiveTimestamp(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	clock := runtime.server.config.Clock.(*adapterTestClock)
	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	const subscriberSession = "expiry-subscriber"
	runtime.server.deliveries.Register(subscriberSession)
	if _, err := runtime.server.subscriptions.Subscribe(subscriberSession, mqtt.Subscribe{
		Version: mqtt.ProtocolV5,
		Subscriptions: []mqtt.Subscription{
			{Filter: "expiry/qos2", QoS: mqtt.QoSExactlyOnce},
		},
	}, clock.now); err != nil {
		t.Fatal(err)
	}

	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = "expiry-publisher"
	conn.version = mqtt.ProtocolV5
	if err := conn.dispatchPublish(mqtt.Publish{
		Version:                  mqtt.ProtocolV5,
		Topic:                    "expiry/qos2",
		Payload:                  []byte("expired"),
		QoS:                      mqtt.QoSExactlyOnce,
		PacketID:                 21,
		HasMessageExpiryInterval: true,
		MessageExpiryInterval:    1,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-conn.controlQueue:
		pubrec, ok := message.packet.(*mqtt.PubRec)
		if !ok || pubrec.PacketID != 21 {
			t.Fatalf("expected PubRec for packet 21, got %#v", message.packet)
		}
	default:
		t.Fatal("expected PubRec before PUBREL")
	}

	clock.now = clock.now.Add(2 * time.Second)
	if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 21}); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-conn.controlQueue:
		pubcomp, ok := message.packet.(*mqtt.PubComp)
		if !ok || pubcomp.PacketID != 21 {
			t.Fatalf("expected PubComp for packet 21, got %#v", message.packet)
		}
	default:
		t.Fatal("expected PubComp after PUBREL")
	}

	items := takeDeliveries(t, runtime.server.deliveries, subscriberSession, clock.now)
	if len(items) != 0 {
		t.Fatalf("expired QoS 2 publication was delivered: %#v", items)
	}
}

func TestOutboundQoS1AndQoS2Lifecycle(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 4, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = "subscriber-client"
	conn.version = mqtt.ProtocolV5

	runtime.server.deliveries.Register(conn.sessionID)

	// 1. Outbound QoS 1
	if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: conn.sessionID,
		Topic:     "test/qos1",
		Payload:   []byte("qos1-data"),
		QoS:       mqtt.QoSAtLeastOnce,
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}

	deliveryResult := make(chan error, 1)
	go func() { deliveryResult <- conn.writeDeliveries() }()
	frame := readTestFrame(t, clientSocket)
	if len(frame) == 0 {
		t.Fatal("empty QoS 1 publish frame")
	}
	if err := <-deliveryResult; err != nil {
		t.Fatalf("writeDeliveries failed: %v", err)
	}

	conn.deliveryMu.Lock()
	if len(conn.outbound) != 1 {
		conn.deliveryMu.Unlock()
		t.Fatalf("expected 1 outbound inflight, got %d", len(conn.outbound))
	}
	var qos1PacketID mqtt.PacketIdentifier
	for id := range conn.outbound {
		qos1PacketID = id
	}
	conn.deliveryMu.Unlock()

	// Client sends PubAck
	if err := conn.dispatchPubAck(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: qos1PacketID}); err != nil {
		t.Fatalf("dispatchPubAck failed: %v", err)
	}

	conn.deliveryMu.Lock()
	if len(conn.outbound) != 0 {
		conn.deliveryMu.Unlock()
		t.Fatalf("expected 0 outbound after PubAck, got %d", len(conn.outbound))
	}
	conn.deliveryMu.Unlock()

	// 2. Outbound QoS 2
	if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: conn.sessionID,
		Topic:     "test/qos2",
		Payload:   []byte("qos2-data"),
		QoS:       mqtt.QoSExactlyOnce,
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}

	go func() { deliveryResult <- conn.writeDeliveries() }()
	frame2 := readTestFrame(t, clientSocket)
	if len(frame2) == 0 {
		t.Fatal("empty QoS 2 publish frame")
	}
	if err := <-deliveryResult; err != nil {
		t.Fatalf("writeDeliveries QoS 2 failed: %v", err)
	}

	conn.deliveryMu.Lock()
	if len(conn.outbound) != 1 {
		conn.deliveryMu.Unlock()
		t.Fatalf("expected 1 outbound inflight for QoS 2, got %d", len(conn.outbound))
	}
	var qos2PacketID mqtt.PacketIdentifier
	for id := range conn.outbound {
		qos2PacketID = id
	}
	conn.deliveryMu.Unlock()

	// Client sends PubRec
	if err := conn.dispatchPubRec(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: qos2PacketID}); err != nil {
		t.Fatalf("dispatchPubRec failed: %v", err)
	}

	// Verify PubRel enqueued in control queue
	select {
	case msg := <-conn.controlQueue:
		pubrel, ok := msg.packet.(*mqtt.PubRel)
		if !ok || pubrel.PacketID != qos2PacketID {
			t.Fatalf("expected PubRel with PacketID %d, got %#v", qos2PacketID, msg.packet)
		}
	default:
		t.Fatal("expected PubRel in control queue")
	}

	// Client sends PubComp
	if err := conn.dispatchPubComp(&mqtt.PubComp{Version: mqtt.ProtocolV5, PacketID: qos2PacketID}); err != nil {
		t.Fatalf("dispatchPubComp failed: %v", err)
	}

	conn.deliveryMu.Lock()
	if len(conn.outbound) != 0 {
		conn.deliveryMu.Unlock()
		t.Fatalf("expected 0 outbound after PubComp, got %d", len(conn.outbound))
	}
	conn.deliveryMu.Unlock()
}

// Traceability: PRD MQTT-02/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §3.2.
func TestDispatchPubRecUsesCurrentProtocolVersion(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	connection := newConnection(runtime.server, serverSocket)
	connection.sessionID = "cross-version"
	connection.version = mqtt.ProtocolV5
	if err := runtime.server.deliveries.Register(connection.sessionID); err != nil {
		t.Fatal(err)
	}
	state, _, err := delivery.NewOutbound(mqtt.Publish{
		Version:  mqtt.ProtocolV311,
		Topic:    "cross-version/topic",
		QoS:      mqtt.QoSExactlyOnce,
		PacketID: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	outbound := applicationdeliveryqueue.OutboundDTO{
		SessionID: connection.sessionID,
		PacketID:  7,
		State:     state,
	}
	if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: connection.sessionID,
		Topic:     "cross-version/topic",
		QoS:       mqtt.QoSExactlyOnce,
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.server.deliveries.PromoteHead(outbound, time.Time{}); err != nil {
		t.Fatal(err)
	}
	connection.deliveryMu.Lock()
	connection.outbound[outbound.PacketID] = outbound
	connection.deliveryMu.Unlock()

	if err := connection.dispatchPubRec(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: 7}); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-connection.controlQueue:
		pubrel, ok := message.packet.(*mqtt.PubRel)
		if !ok {
			t.Fatalf("queued packet = %#v, want PUBREL", message.packet)
		}
		if pubrel.Version != mqtt.ProtocolV5 || pubrel.PacketID != 7 {
			t.Fatalf("queued PUBREL = %#v, want MQTT 5.0 packet 7", pubrel)
		}
	default:
		t.Fatal("expected PUBREL after PUBREC")
	}
}

func TestDispatchPubAckRecCompEdgeCases(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 4, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = "subscriber-edge"
	conn.version = mqtt.ProtocolV5

	// 1. PacketID == 0 checks
	if err := conn.dispatchPubAck(&mqtt.PubAck{PacketID: 0}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("dispatchPubAck ID 0 = %v, want ErrProtocol", err)
	}
	if err := conn.dispatchPubRec(&mqtt.PubRec{PacketID: 0}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("dispatchPubRec ID 0 = %v, want ErrProtocol", err)
	}
	if err := conn.dispatchPubComp(&mqtt.PubComp{PacketID: 0}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("dispatchPubComp ID 0 = %v, want ErrProtocol", err)
	}

	// 2. Non-existent PacketID checks (should safely return nil)
	if err := conn.dispatchPubAck(&mqtt.PubAck{PacketID: 999}); err != nil {
		t.Fatalf("dispatchPubAck non-existent = %v", err)
	}
	if err := conn.dispatchPubRec(&mqtt.PubRec{PacketID: 999}); err != nil {
		t.Fatalf("dispatchPubRec non-existent = %v", err)
	}
	if err := conn.dispatchPubComp(&mqtt.PubComp{PacketID: 999}); err != nil {
		t.Fatalf("dispatchPubComp non-existent = %v", err)
	}

	// 3. PubRec error reason code (>= 0x80) completes exchange immediately
	runtime.server.deliveries.Register(conn.sessionID)
	if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: conn.sessionID,
		Topic:     "test/qos2-err",
		Payload:   []byte("data"),
		QoS:       mqtt.QoSExactlyOnce,
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = conn.writeDeliveries() }()
	_ = readTestFrame(t, clientSocket)

	var qos2ID mqtt.PacketIdentifier
	conn.deliveryMu.Lock()
	for id := range conn.outbound {
		qos2ID = id
	}
	conn.deliveryMu.Unlock()

	// Mismatched packet (PubAck sent for QoS 2 outbound) -> ErrProtocol
	if err := conn.dispatchPubAck(&mqtt.PubAck{PacketID: qos2ID}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("mismatched PubAck = %v, want ErrProtocol", err)
	}

	// PubRec with reason code 0x80 (error) -> completes and deletes outbound
	if err := conn.dispatchPubRec(&mqtt.PubRec{PacketID: qos2ID, ReasonCode: 0x80}); err != nil {
		t.Fatalf("dispatchPubRec 0x80 error = %v", err)
	}
	conn.deliveryMu.Lock()
	if _, exists := conn.outbound[qos2ID]; exists {
		t.Fatal("outbound was not deleted on PubRec error")
	}
	conn.deliveryMu.Unlock()

	// 4. PubComp mismatched packet
	if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: conn.sessionID,
		Topic:     "test/qos1-err",
		Payload:   []byte("data"),
		QoS:       mqtt.QoSAtLeastOnce,
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = conn.writeDeliveries() }()
	_ = readTestFrame(t, clientSocket)

	var qos1ID mqtt.PacketIdentifier
	conn.deliveryMu.Lock()
	for id := range conn.outbound {
		qos1ID = id
	}
	conn.deliveryMu.Unlock()

	// PubComp sent for QoS 1 outbound -> ErrProtocol
	if err := conn.dispatchPubComp(&mqtt.PubComp{PacketID: qos1ID}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("mismatched PubComp = %v, want ErrProtocol", err)
	}
}

func TestDispatchPublishAndPubRelEdgeCases(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = "edge-publisher"
	conn.version = mqtt.ProtocolV5

	// Invalid QoS > 2 -> ErrProtocol
	if err := conn.dispatchPublish(mqtt.Publish{
		Topic: "test/invalid",
		QoS:   mqtt.QoS(3),
	}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("invalid QoS dispatchPublish = %v, want ErrProtocol", err)
	}

	// MQTT 3.1.1 PubRel for unknown ID -> sends standard PubComp
	conn311 := newConnection(runtime.server, serverSocket)
	conn311.sessionID = "edge311"
	conn311.version = mqtt.ProtocolV311
	if err := conn311.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV311, PacketID: 55}); err != nil {
		t.Fatalf("MQTT 3.1.1 unknown PubRel = %v", err)
	}
	select {
	case msg := <-conn311.controlQueue:
		pubcomp, ok := msg.packet.(*mqtt.PubComp)
		if !ok || pubcomp.PacketID != 55 {
			t.Fatalf("expected PubComp with ID 55, got %#v", msg.packet)
		}
	default:
		t.Fatal("expected PubComp in controlQueue")
	}

	// Test enqueueNegativePublishAck helper
	if err := conn.enqueueNegativePublishAck(mqtt.Publish{QoS: mqtt.QoSAtLeastOnce, PacketID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := conn.enqueueNegativePublishAck(mqtt.Publish{QoS: mqtt.QoSExactlyOnce, PacketID: 2}); err != nil {
		t.Fatal(err)
	}
}

func TestWriteDeliveriesEdgeCases(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 4, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = "edge-writer"
	conn.version = mqtt.ProtocolV5
	runtime.server.deliveries.Register(conn.sessionID)

	// Exhaust allocator
	for i := 1; i <= 65535; i++ {
		_, _ = conn.packetIDs.acquire()
	}

	// Enqueue QoS 1 delivery
	if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: conn.sessionID,
		Topic:     "test/exhausted",
		Payload:   []byte("exhausted"),
		QoS:       mqtt.QoSAtLeastOnce,
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}

	// writeDeliveries should safely return nil and re-enqueue item
	if err := conn.writeDeliveries(); err != nil {
		t.Fatalf("writeDeliveries when exhausted = %v, want nil", err)
	}

	item, ok, err := runtime.server.deliveries.Peek(conn.sessionID, time.Time{})
	if err != nil || !ok {
		t.Fatalf("item was not retained after allocator exhaustion: item=%#v err=%v ok=%v", item, err, ok)
	}
	items := []applicationdeliveryqueue.DeliveryDTO{item}
	if len(items) != 1 || items[0].Topic != "test/exhausted" {
		t.Fatalf("item was not re-enqueued properly: %#v", items)
	}
}

func newBoundedRetainedRuntime(t *testing.T, messageCap, byteCap, queueCap int) *adapterTestRuntime {
	t.Helper()
	listener := newPipeListener()
	routes := routing.NewTable()
	queue, err := deliveryqueue.New(queueCap)
	if err != nil {
		t.Fatal(err)
	}
	repository := sessionstore.New()
	retainedRepository, err := memoryretained.New(messageCap, byteCap)
	if err != nil {
		t.Fatal(err)
	}
	clock := &adapterTestClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
	willBridge := NewWillBridge()
	connections, err := connect.New(clock, repository, queue, routes, adapterTestScheduler{}, willBridge)
	if err != nil {
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	publications, err := publish.New(clock, routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	config := defaultAdapterConfig()
	config.Clock = clock
	decoder, err := mqtt.NewDecoder(config.MaxPacketSize)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(config.MaxPacketSize)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(listener, config, decoder, encoder, connections, subscriptions, publications, queue, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	willBridge.Bind(server)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	client, serverConnection := net.Pipe()
	listener.connections <- serverConnection
	select {
	case <-listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("server did not accept net.Pipe connection")
	}
	return &adapterTestRuntime{server: server, listener: listener, client: client, cancel: cancel, done: done, repo: repository}
}

func TestDispatchPublishAndPubRelRetainedFull(t *testing.T) {
	runtime := newBoundedRetainedRuntime(t, 1, 4, 10)
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	// 1. QoS 1 MQTT 5 -> PubAck with 0x97
	conn5 := newConnection(runtime.server, serverSocket)
	conn5.sessionID = "pub5-full"
	conn5.version = mqtt.ProtocolV5
	if err := conn5.dispatchPublish(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "test/full",
		Payload:  []byte("oversized-payload"),
		Retain:   true,
		QoS:      mqtt.QoSAtLeastOnce,
		PacketID: 10,
	}); err != nil {
		t.Fatalf("dispatchPublish QoS 1 full err: %v", err)
	}
	select {
	case msg := <-conn5.controlQueue:
		ack, ok := msg.packet.(*mqtt.PubAck)
		if !ok || ack.PacketID != 10 || ack.ReasonCode != 0x97 {
			t.Fatalf("expected PubAck 0x97, got %#v", msg.packet)
		}
	default:
		t.Fatal("expected PubAck in controlQueue")
	}

	// 2. QoS 1 MQTT 3.1.1 -> closes connection
	conn311 := newConnection(runtime.server, serverSocket)
	conn311.sessionID = "pub311-full"
	conn311.version = mqtt.ProtocolV311
	if err := conn311.dispatchPublish(mqtt.Publish{
		Version:  mqtt.ProtocolV311,
		Topic:    "test/full",
		Payload:  []byte("oversized-payload"),
		Retain:   true,
		QoS:      mqtt.QoSAtLeastOnce,
		PacketID: 11,
	}); err != nil {
		t.Fatalf("dispatchPublish QoS 1 311 full err: %v", err)
	}
	if !conn311.isStopping() {
		t.Fatal("expected connection to stop on 3.1.1 QoS 1 retained full")
	}

	// 3. QoS 2 MQTT 5 -> PubRel fails with Disconnect 0x97
	conn5QoS2 := newConnection(runtime.server, serverSocket)
	conn5QoS2.sessionID = "pub5-qos2-full"
	conn5QoS2.version = mqtt.ProtocolV5
	if err := conn5QoS2.dispatchPublish(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "test/full",
		Payload:  []byte("oversized-payload"),
		Retain:   true,
		QoS:      mqtt.QoSExactlyOnce,
		PacketID: 20,
	}); err != nil {
		t.Fatal(err)
	}
	// Drain PubRec
	<-conn5QoS2.controlQueue

	// Client sends PubRel
	go func() {
		_ = conn5QoS2.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 20})
	}()
	select {
	case msg := <-conn5QoS2.controlQueue:
		disc, ok := msg.packet.(*mqtt.Disconnect)
		if !ok || disc.ReasonCode != 0x97 {
			t.Fatalf("expected Disconnect 0x97, got %#v", msg.packet)
		}
		if msg.done != nil {
			msg.done <- nil
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for disconnect")
	}

	// 4. QoS 2 MQTT 3.1.1 -> PubRel closes connection
	conn311QoS2 := newConnection(runtime.server, serverSocket)
	conn311QoS2.sessionID = "pub311-qos2-full"
	conn311QoS2.version = mqtt.ProtocolV311
	if err := conn311QoS2.dispatchPublish(mqtt.Publish{
		Version:  mqtt.ProtocolV311,
		Topic:    "test/full",
		Payload:  []byte("oversized-payload"),
		Retain:   true,
		QoS:      mqtt.QoSExactlyOnce,
		PacketID: 21,
	}); err != nil {
		t.Fatal(err)
	}
	<-conn311QoS2.controlQueue
	if err := conn311QoS2.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV311, PacketID: 21}); err != nil {
		t.Fatal(err)
	}
	if !conn311QoS2.isStopping() {
		t.Fatal("expected connection to stop on 3.1.1 QoS 2 retained full")
	}
}

func TestDispatchPublishAndPubRelSessionSaturation(t *testing.T) {
	runtime := newBoundedRetainedRuntime(t, 32, 4096, 1)
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	// Register and subscribe sat-session
	runtime.server.deliveries.Register("sat-session")
	subConn := newConnection(runtime.server, serverSocket)
	subConn.sessionID = "sat-session"
	subConn.accepted = true
	runtime.server.bindSession("sat-session", subConn)
	if _, err := runtime.server.subscriptions.Subscribe("sat-session", mqtt.Subscribe{
		Version: mqtt.ProtocolV5,
		Subscriptions: []mqtt.Subscription{
			{Filter: "sat/#", QoS: mqtt.QoSAtLeastOnce},
		},
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}

	// Fill queue with 1 item so it's saturated
	if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: "sat-session",
		Topic:     "sat/1",
		Payload:   []byte("1"),
		QoS:       mqtt.QoSAtMostOnce,
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}

	// Publisher sends QoS 1 to sat/2 -> fan-out saturates sat-session -> closeSession called!
	pubConn := newConnection(runtime.server, serverSocket)
	pubConn.sessionID = "publisher-sat"
	pubConn.version = mqtt.ProtocolV5
	if err := pubConn.dispatchPublish(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "sat/2",
		Payload:  []byte("2"),
		QoS:      mqtt.QoSAtLeastOnce,
		PacketID: 50,
	}); err != nil {
		t.Fatal(err)
	}
	if !subConn.isStopping() {
		t.Fatal("expected saturated subscriber connection to be closed")
	}

	// Re-bind a new sub connection for QoS 2 saturation test
	serverSocket2, clientSocket2 := net.Pipe()
	defer serverSocket2.Close()
	defer clientSocket2.Close()
	runtime.server.deliveries.Register("sat-session-2")
	subConn2 := newConnection(runtime.server, serverSocket2)
	subConn2.sessionID = "sat-session-2"
	subConn2.accepted = true
	runtime.server.bindSession("sat-session-2", subConn2)
	if _, err := runtime.server.subscriptions.Subscribe("sat-session-2", mqtt.Subscribe{
		Version: mqtt.ProtocolV5,
		Subscriptions: []mqtt.Subscription{
			{Filter: "sat2/#", QoS: mqtt.QoSExactlyOnce},
		},
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: "sat-session-2",
		Topic:     "sat2/1",
		Payload:   []byte("1"),
		QoS:       mqtt.QoSAtMostOnce,
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}

	// Publisher sends QoS 2 to sat2/3
	if err := pubConn.dispatchPublish(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "sat2/3",
		Payload:  []byte("3"),
		QoS:      mqtt.QoSExactlyOnce,
		PacketID: 60,
	}); err != nil {
		t.Fatal(err)
	}
	<-pubConn.controlQueue // PubRec

	// PubRel triggers fan-out -> saturates -> closeSession called!
	if err := pubConn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 60}); err != nil {
		t.Fatal(err)
	}
	if !subConn2.isStopping() {
		t.Fatal("expected saturated subscriber connection 2 to be closed on PubRel")
	}
}

func TestDispatchPublishAndPubRelInvalidTransitions(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = "trans-session"
	conn.version = mqtt.ProtocolV5

	// 1. QoS 1 invalid transition on duplicate
	conn.deliveryMu.Lock()
	conn.inbound[77] = inboundState{state: delivery.Inbound{}}
	conn.deliveryMu.Unlock()

	if err := conn.dispatchPublish(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "test/topic",
		QoS:      mqtt.QoSAtLeastOnce,
		PacketID: 77,
	}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("expected ErrProtocol on invalid transition, got %v", err)
	}

	// 2. QoS 2 invalid transition on duplicate
	conn.deliveryMu.Lock()
	conn.inbound[78] = inboundState{state: delivery.Inbound{}}
	conn.deliveryMu.Unlock()

	if err := conn.dispatchPublish(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "test/topic",
		QoS:      mqtt.QoSExactlyOnce,
		PacketID: 78,
	}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("expected ErrProtocol on invalid transition QoS 2, got %v", err)
	}

	// 3. PubRel invalid transition
	if err := conn.dispatchPubRel(&mqtt.PubRel{
		Version:  mqtt.ProtocolV5,
		PacketID: 78,
	}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("expected ErrProtocol on invalid transition PubRel, got %v", err)
	}
}

// Traceability: PRD MQTT-02/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §3.2.
func TestWriteRecoveredDeliveriesStateAndWriteFailures(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()

	connection := newConnection(runtime.server, serverSocket)
	connection.sessionID = "recovered-client"
	connection.recovered = []applicationdeliveryqueue.OutboundDTO{{
		SessionID: connection.sessionID,
		PacketID:  1,
	}}
	if err := connection.writeDeliveries(); !errors.Is(err, delivery.ErrInvalidTransition) {
		t.Fatalf("invalid recovered state error = %v, want invalid transition", err)
	}

	state, _, err := delivery.NewOutbound(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "recovered/topic",
		Payload:  []byte("payload"),
		QoS:      mqtt.QoSAtLeastOnce,
		PacketID: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	completeState, _, err := state.Receive(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: 2})
	if err != nil {
		t.Fatal(err)
	}
	connection.recovered = []applicationdeliveryqueue.OutboundDTO{{
		SessionID: connection.sessionID,
		PacketID:  2,
		State:     completeState,
	}}
	if err := connection.writeRecoveredDeliveries(); !errors.Is(err, delivery.ErrStateComplete) {
		t.Fatalf("completed recovered state error = %v, want state complete", err)
	}

	failingServer, failingClient := net.Pipe()
	defer failingServer.Close()
	connection = newConnection(runtime.server, failingServer)
	connection.sessionID = "write-failure-client"
	state, _, err = delivery.NewOutbound(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "recovered/failure",
		Payload:  []byte("payload"),
		QoS:      mqtt.QoSExactlyOnce,
		PacketID: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	connection.recovered = []applicationdeliveryqueue.OutboundDTO{{
		SessionID: connection.sessionID,
		PacketID:  3,
		State:     state,
	}}
	if err := failingClient.Close(); err != nil {
		t.Fatal(err)
	}
	if err := connection.writeRecoveredDeliveries(); err == nil {
		t.Fatal("writeRecoveredDeliveries() succeeded after peer close")
	}
}

// Traceability: PRD MQTT-02/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §3.2.
func TestWriteDeliveriesRetainsHeadOnOutboundCapacityAndConflict(t *testing.T) {
	t.Run("outbound capacity", func(t *testing.T) {
		runtime := newAdapterTestRuntime(t, 1, defaultAdapterConfig())
		defer runtime.stop(t)

		serverSocket, clientSocket := net.Pipe()
		defer serverSocket.Close()
		defer clientSocket.Close()

		connection := newConnection(runtime.server, serverSocket)
		connection.sessionID = "capacity-client"
		connection.version = mqtt.ProtocolV5
		if err := runtime.server.deliveries.Register(connection.sessionID); err != nil {
			t.Fatal(err)
		}
		for _, topic := range []string{"first", "second"} {
			if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
				SessionID: connection.sessionID,
				Topic:     topic,
				Payload:   []byte(topic),
				QoS:       mqtt.QoSAtLeastOnce,
			}, time.Time{}); err != nil {
				t.Fatal(err)
			}
			if topic == "first" {
				result := make(chan error, 1)
				go func() { result <- connection.writeDeliveries() }()
				_ = readTestFrame(t, clientSocket)
				if err := <-result; err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := connection.writeDeliveries(); err != nil {
			t.Fatalf("capacity writeDeliveries() error = %v", err)
		}
		head, ok, err := runtime.server.deliveries.Peek(connection.sessionID, time.Time{})
		if err != nil || !ok || head.Topic != "second" {
			t.Fatalf("capacity head = %#v, %v, %v", head, ok, err)
		}
	})

	t.Run("packet identifier conflict", func(t *testing.T) {
		runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
		defer runtime.stop(t)

		serverSocket, clientSocket := net.Pipe()
		defer serverSocket.Close()
		defer clientSocket.Close()

		connection := newConnection(runtime.server, serverSocket)
		connection.sessionID = "conflict-client"
		connection.version = mqtt.ProtocolV5
		if err := runtime.server.deliveries.Register(connection.sessionID); err != nil {
			t.Fatal(err)
		}
		state, _, err := delivery.NewOutbound(mqtt.Publish{
			Version:  mqtt.ProtocolV5,
			Topic:    "stored",
			Payload:  []byte("stored"),
			QoS:      mqtt.QoSAtLeastOnce,
			PacketID: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
			SessionID: connection.sessionID,
			Topic:     "stored",
			QoS:       mqtt.QoSAtLeastOnce,
		}, time.Time{}); err != nil {
			t.Fatal(err)
		}
		if err := runtime.server.deliveries.PromoteHead(applicationdeliveryqueue.OutboundDTO{
			SessionID: connection.sessionID,
			PacketID:  1,
			State:     state,
		}, time.Time{}); err != nil {
			t.Fatal(err)
		}
		if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
			SessionID: connection.sessionID,
			Topic:     "blocked",
			QoS:       mqtt.QoSAtLeastOnce,
		}, time.Time{}); err != nil {
			t.Fatal(err)
		}
		if err := connection.writeDeliveries(); !errors.Is(err, ErrProtocol) {
			t.Fatalf("conflict writeDeliveries() error = %v, want ErrProtocol", err)
		}
		head, ok, err := runtime.server.deliveries.Peek(connection.sessionID, time.Time{})
		if err != nil || !ok || head.Topic != "blocked" {
			t.Fatalf("conflict head = %#v, %v, %v", head, ok, err)
		}
	})
}

type scriptedDeliveryQueue struct {
	applicationdeliveryqueue.Queue
	items               []applicationdeliveryqueue.DeliveryDTO
	peekIndex           int
	peekErr             error
	completeHeadErr     error
	promoteErr          error
	completeOutboundErr error
}

func (queue *scriptedDeliveryQueue) Peek(sessionID routing.SessionID, now time.Time) (applicationdeliveryqueue.DeliveryDTO, bool, error) {
	if queue.peekIndex < len(queue.items) {
		item := queue.items[queue.peekIndex]
		queue.peekIndex++
		return item, true, nil
	}
	if queue.peekErr != nil {
		return applicationdeliveryqueue.DeliveryDTO{}, false, queue.peekErr
	}
	return applicationdeliveryqueue.DeliveryDTO{}, false, nil
}

func (queue *scriptedDeliveryQueue) PeekBatch(sessionID routing.SessionID, now time.Time, destination []applicationdeliveryqueue.DeliveryDTO) (int, error) {
	if len(destination) == 0 {
		return 0, nil
	}
	head, ok, err := queue.Peek(sessionID, now)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	destination[0] = head
	return 1, nil
}

func (queue *scriptedDeliveryQueue) PromoteBatch(outbounds []applicationdeliveryqueue.OutboundDTO, now time.Time) (int, error) {
	if len(outbounds) == 0 {
		return 0, nil
	}
	err := queue.PromoteHead(outbounds[0], now)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func (queue *scriptedDeliveryQueue) CompleteHead(sessionID routing.SessionID) error {
	return queue.completeHeadErr
}

func (queue *scriptedDeliveryQueue) PromoteHead(outbound applicationdeliveryqueue.OutboundDTO, now time.Time) error {
	return queue.promoteErr
}

func (queue *scriptedDeliveryQueue) CompleteOutbound(sessionID routing.SessionID, packetID mqtt.PacketIdentifier) error {
	return queue.completeOutboundErr
}

func (queue *scriptedDeliveryQueue) BeginReplaceOutbound(outbound applicationdeliveryqueue.OutboundDTO) (applicationdeliveryqueue.PendingTransition, error) {
	if err := queue.ReplaceOutbound(outbound); err != nil {
		return nil, err
	}
	return applicationdeliveryqueue.CompletedTransition{}, nil
}

func (queue *scriptedDeliveryQueue) BeginCompleteOutbound(outbound applicationdeliveryqueue.OutboundDTO) (applicationdeliveryqueue.PendingTransition, error) {
	if err := queue.CompleteOutbound(outbound.SessionID, outbound.PacketID); err != nil {
		return nil, err
	}
	return applicationdeliveryqueue.CompletedTransition{}, nil
}

type controlInterleavingDeliveryQueue struct {
	*scriptedDeliveryQueue
	onFirstPeek func()
}

func (q *controlInterleavingDeliveryQueue) Peek(sessionID routing.SessionID, now time.Time) (applicationdeliveryqueue.DeliveryDTO, bool, error) {
	if q.onFirstPeek != nil {
		q.onFirstPeek()
	}
	return q.scriptedDeliveryQueue.Peek(sessionID, now)
}

func (q *controlInterleavingDeliveryQueue) PeekBatch(sessionID routing.SessionID, now time.Time, destination []applicationdeliveryqueue.DeliveryDTO) (int, error) {
	if q.onFirstPeek != nil {
		q.onFirstPeek()
	}
	return q.scriptedDeliveryQueue.PeekBatch(sessionID, now, destination)
}

type retryablePromotionDeliveryQueue struct {
	applicationdeliveryqueue.Queue
	failOnce chan struct{}
	attempts chan applicationdeliveryqueue.OutboundDTO
}

func (queue *retryablePromotionDeliveryQueue) PromoteHead(
	outbound applicationdeliveryqueue.OutboundDTO,
	now time.Time,
) error {
	queue.attempts <- outbound
	select {
	case <-queue.failOnce:
		return applicationdeliveryqueue.ErrRetryableUnavailable
	default:
		return queue.Queue.PromoteHead(outbound, now)
	}
}

func (queue *retryablePromotionDeliveryQueue) PromoteBatch(
	outbounds []applicationdeliveryqueue.OutboundDTO,
	now time.Time,
) (int, error) {
	if len(outbounds) == 0 {
		return 0, nil
	}
	err := queue.PromoteHead(outbounds[0], now)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// Traceability: PRD MQTT-02/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §3.2.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-071
func TestDeliveryWriterRetainsStateOnQueueAndWireErrors(t *testing.T) {
	newRuntime := func(t *testing.T) *adapterTestRuntime {
		t.Helper()
		return newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	}

	t.Run("complete head error", func(t *testing.T) {
		runtime := newRuntime(t)
		defer runtime.stop(t)
		queueErr := errors.New("complete head failed")
		runtime.server.deliveries = &scriptedDeliveryQueue{
			items:           []applicationdeliveryqueue.DeliveryDTO{{SessionID: "qos0-error", Topic: "topic"}},
			completeHeadErr: queueErr,
		}
		serverSocket, clientSocket := net.Pipe()
		defer serverSocket.Close()
		defer clientSocket.Close()
		connection := newConnection(runtime.server, serverSocket)
		connection.sessionID = "qos0-error"
		connection.version = mqtt.ProtocolV5
		result := make(chan error, 1)
		go func() { result <- connection.writeDeliveries() }()
		_ = readTestFrame(t, clientSocket)
		if err := <-result; !errors.Is(err, queueErr) {
			t.Fatalf("complete head error = %v, want %v", err, queueErr)
		}
	})

	for _, queueErr := range []error{applicationdeliveryqueue.ErrExpired, applicationdeliveryqueue.ErrFull} {
		queueErr := queueErr
		t.Run(queueErr.Error(), func(t *testing.T) {
			runtime := newRuntime(t)
			defer runtime.stop(t)
			runtime.server.deliveries = &scriptedDeliveryQueue{
				items:      []applicationdeliveryqueue.DeliveryDTO{{SessionID: "promote-error", Topic: "topic", QoS: mqtt.QoSAtLeastOnce}},
				promoteErr: queueErr,
			}
			serverSocket, clientSocket := net.Pipe()
			defer serverSocket.Close()
			defer clientSocket.Close()
			connection := newConnection(runtime.server, serverSocket)
			connection.sessionID = "promote-error"
			connection.version = mqtt.ProtocolV5
			if err := connection.writeDeliveries(); err != nil {
				t.Fatalf("writeDeliveries() error = %v", err)
			}
		})
	}

	t.Run("later durable notification retries with the same Packet ID", func(t *testing.T) {
		runtime := newRuntime(t)
		defer runtime.stop(t)
		baseQueue := runtime.server.deliveries
		sessionID := routing.SessionID("retryable-notification")
		if err := baseQueue.Register(sessionID); err != nil {
			t.Fatal(err)
		}
		queue := &retryablePromotionDeliveryQueue{
			Queue:    baseQueue,
			failOnce: make(chan struct{}, 1),
			attempts: make(chan applicationdeliveryqueue.OutboundDTO, 4),
		}
		queue.failOnce <- struct{}{}
		runtime.server.deliveries = queue
		serverSocket, clientSocket := net.Pipe()
		connection := newConnection(runtime.server, serverSocket)
		connection.sessionID = sessionID
		connection.version = mqtt.ProtocolV5
		if !runtime.server.bindSession(sessionID, connection) {
			t.Fatal("failed to bind delivery session")
		}
		go connection.writerLoop()
		defer func() {
			connection.requestClose()
			<-connection.writerDone
			runtime.server.unbindSession(sessionID, connection)
			_ = clientSocket.Close()
		}()
		readAttempt := func() applicationdeliveryqueue.OutboundDTO {
			t.Helper()
			select {
			case attempt := <-queue.attempts:
				return attempt
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for durable promotion")
				return applicationdeliveryqueue.OutboundDTO{}
			}
		}
		readPublish := func() *mqtt.Publish {
			t.Helper()
			frame := readTestFrame(t, clientSocket)
			packet, consumed, err := runtime.server.limits.Load().decoder.Decode(frame, mqtt.ProtocolV5)
			if err != nil || consumed != len(frame) {
				t.Fatalf("decode delivery frame: packet=%T consumed=%d err=%v", packet, consumed, err)
				return nil
			}
			publish, ok := packet.(*mqtt.Publish)
			if !ok {
				t.Fatalf("delivery packet=%T, want PUBLISH", packet)
				return nil
			}
			return publish
		}

		if err := runtime.server.DeliverDurable(applicationdeliveryqueue.DeliveryDTO{
			SessionID: sessionID, Topic: "retry/first", Payload: []byte("first"), QoS: mqtt.QoSAtLeastOnce,
			DurableSequence: 1, DurableEpoch: 1,
		}); err != nil {
			t.Fatal(err)
		}
		firstAttempt := readAttempt()
		if firstAttempt.PacketID == 0 {
			t.Fatal("first promotion used a zero Packet ID")
		}
		if err := runtime.server.DeliverDurable(applicationdeliveryqueue.DeliveryDTO{
			SessionID: sessionID, Topic: "retry/second", Payload: []byte("second"), QoS: mqtt.QoSAtLeastOnce,
			DurableSequence: 2, DurableEpoch: 1,
		}); err != nil {
			t.Fatal(err)
		}
		retryAttempt := readAttempt()
		if retryAttempt.PacketID != firstAttempt.PacketID {
			t.Fatalf("retry Packet ID=%d, want preserved %d", retryAttempt.PacketID, firstAttempt.PacketID)
		}
		firstPublish := readPublish()
		if firstPublish.PacketID != firstAttempt.PacketID || firstPublish.Topic != "retry/first" {
			t.Fatalf("first retried PUBLISH=%#v, want packet %d topic retry/first", firstPublish, firstAttempt.PacketID)
		}
		secondAttempt := readAttempt()
		if secondAttempt.PacketID == firstAttempt.PacketID {
			t.Fatalf("next delivery reused in-flight Packet ID %d", secondAttempt.PacketID)
		}
		secondPublish := readPublish()
		if secondPublish.PacketID != secondAttempt.PacketID || secondPublish.Topic != "retry/second" {
			t.Fatalf("second PUBLISH=%#v, want packet %d topic retry/second", secondPublish, secondAttempt.PacketID)
		}
	})

	t.Run("retry timer resumes retained head", func(t *testing.T) {
		runtime := newRuntime(t)
		defer runtime.stop(t)
		baseQueue := runtime.server.deliveries
		sessionID := routing.SessionID("retryable-timer")
		if err := baseQueue.Register(sessionID); err != nil {
			t.Fatal(err)
		}
		queue := &retryablePromotionDeliveryQueue{
			Queue:    baseQueue,
			failOnce: make(chan struct{}, 1),
			attempts: make(chan applicationdeliveryqueue.OutboundDTO, 2),
		}
		queue.failOnce <- struct{}{}
		runtime.server.deliveries = queue
		serverSocket, clientSocket := net.Pipe()
		connection := newConnection(runtime.server, serverSocket)
		connection.sessionID = sessionID
		connection.version = mqtt.ProtocolV5
		if !runtime.server.bindSession(sessionID, connection) {
			t.Fatal("failed to bind delivery session")
		}
		go connection.writerLoop()
		defer func() {
			connection.requestClose()
			<-connection.writerDone
			runtime.server.unbindSession(sessionID, connection)
			_ = clientSocket.Close()
		}()
		readAttempt := func() applicationdeliveryqueue.OutboundDTO {
			t.Helper()
			select {
			case attempt := <-queue.attempts:
				return attempt
			case <-time.After(deliveryRetryInterval + time.Second):
				t.Fatal("timed out waiting for timed durable retry")
				return applicationdeliveryqueue.OutboundDTO{}
			}
		}
		if err := runtime.server.DeliverDurable(applicationdeliveryqueue.DeliveryDTO{
			SessionID: sessionID, Topic: "retry/timer", Payload: []byte("timer"), QoS: mqtt.QoSExactlyOnce,
			DurableSequence: 1, DurableEpoch: 1,
		}); err != nil {
			t.Fatal(err)
		}
		firstAttempt := readAttempt()
		retryAttempt := readAttempt()
		if retryAttempt.PacketID != firstAttempt.PacketID {
			t.Fatalf("timer retry Packet ID=%d, want preserved %d", retryAttempt.PacketID, firstAttempt.PacketID)
		}
		frame := readTestFrame(t, clientSocket)
		packet, consumed, err := runtime.server.limits.Load().decoder.Decode(frame, mqtt.ProtocolV5)
		publish, ok := packet.(*mqtt.Publish)
		if err != nil || consumed != len(frame) || !ok ||
			publish.Topic != "retry/timer" || publish.PacketID != firstAttempt.PacketID {
			t.Fatalf("timer retry PUBLISH=%#v consumed=%d err=%v", packet, consumed, err)
		}
	})

	t.Run("missing promotion state closes the writer", func(t *testing.T) {
		runtime := newRuntime(t)
		defer runtime.stop(t)
		runtime.server.deliveries = &scriptedDeliveryQueue{
			items: []applicationdeliveryqueue.DeliveryDTO{{
				SessionID: "missing-state", Topic: "topic", QoS: mqtt.QoSAtLeastOnce,
			}},
			promoteErr: applicationdeliveryqueue.ErrUnavailable,
		}
		serverSocket, clientSocket := net.Pipe()
		connection := newConnection(runtime.server, serverSocket)
		connection.sessionID = "missing-state"
		connection.version = mqtt.ProtocolV5
		go connection.writerLoop()
		defer func() {
			connection.requestClose()
			<-connection.writerDone
			_ = clientSocket.Close()
		}()
		connection.wakeDelivery()
		select {
		case <-connection.writerDone:
		case <-time.After(time.Second):
			t.Fatal("writer remained open after promotion reported a missing session state")
		}
		if !connection.isStopping() {
			t.Fatal("missing promotion state did not close the connection")
		}
	})

	t.Run("promote unknown error", func(t *testing.T) {
		runtime := newRuntime(t)
		defer runtime.stop(t)
		queueErr := errors.New("promote failed")
		runtime.server.deliveries = &scriptedDeliveryQueue{
			items:      []applicationdeliveryqueue.DeliveryDTO{{SessionID: "promote-error", Topic: "topic", QoS: mqtt.QoSAtLeastOnce}},
			promoteErr: queueErr,
		}
		serverSocket, clientSocket := net.Pipe()
		defer serverSocket.Close()
		defer clientSocket.Close()
		connection := newConnection(runtime.server, serverSocket)
		connection.sessionID = "promote-error"
		connection.version = mqtt.ProtocolV5
		if err := connection.writeDeliveries(); !errors.Is(err, queueErr) {
			t.Fatalf("promote unknown error = %v, want %v", err, queueErr)
		}
	})

	t.Run("expired recovered publication delete error", func(t *testing.T) {
		runtime := newRuntime(t)
		defer runtime.stop(t)
		queueErr := errors.New("complete outbound failed")
		runtime.server.deliveries = &scriptedDeliveryQueue{completeOutboundErr: queueErr}
		serverSocket, clientSocket := net.Pipe()
		defer serverSocket.Close()
		defer clientSocket.Close()
		connection := newConnection(runtime.server, serverSocket)
		connection.sessionID = "expired-recovered"
		connection.version = mqtt.ProtocolV5
		state, _, err := delivery.NewOutbound(mqtt.Publish{
			Version:  mqtt.ProtocolV5,
			Topic:    "expired",
			QoS:      mqtt.QoSAtLeastOnce,
			PacketID: 4,
		})
		if err != nil {
			t.Fatal(err)
		}
		connection.recovered = []applicationdeliveryqueue.OutboundDTO{{
			SessionID: connection.sessionID,
			PacketID:  4,
			State:     state,
			ExpiresAt: time.Date(2025, time.December, 31, 23, 59, 59, 0, time.UTC),
		}}
		if err := connection.writeRecoveredDeliveries(); !errors.Is(err, queueErr) {
			t.Fatalf("expired recovered error = %v, want %v", err, queueErr)
		}
	})

	t.Run("recovered pubrel wire error", func(t *testing.T) {
		runtime := newRuntime(t)
		defer runtime.stop(t)
		serverSocket, clientSocket := net.Pipe()
		defer serverSocket.Close()
		connection := newConnection(runtime.server, serverSocket)
		connection.sessionID = "pubrel-wire-error"
		connection.version = mqtt.ProtocolV5
		state, _, err := delivery.NewOutbound(mqtt.Publish{
			Version:  mqtt.ProtocolV5,
			Topic:    "pubrel",
			QoS:      mqtt.QoSExactlyOnce,
			PacketID: 5,
		})
		if err != nil {
			t.Fatal(err)
		}
		state, _, err = state.Receive(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: 5})
		if err != nil {
			t.Fatal(err)
		}
		connection.recovered = []applicationdeliveryqueue.OutboundDTO{{
			SessionID: connection.sessionID,
			PacketID:  5,
			State:     state,
		}}
		if err := clientSocket.Close(); err != nil {
			t.Fatal(err)
		}
		if err := connection.writeRecoveredDeliveries(); err == nil {
			t.Fatal("writeRecoveredDeliveries() succeeded after PUBREL peer close")
		}
	})

	t.Run("queued control PUBREL is serviced before remaining delivery backlog", func(t *testing.T) {
		runtime := newRuntime(t)
		defer runtime.stop(t)
		serverSocket, clientSocket := net.Pipe()
		defer serverSocket.Close()
		defer clientSocket.Close()
		sessionID := routing.SessionID("control-ahead-of-backlog")
		connection := newConnection(runtime.server, serverSocket)
		connection.sessionID = sessionID
		connection.version = mqtt.ProtocolV5

		enqueuedPubRel := false
		hookQueue := &scriptedDeliveryQueue{
			items: []applicationdeliveryqueue.DeliveryDTO{
				{SessionID: sessionID, Topic: "delivery/1"},
				{SessionID: sessionID, Topic: "delivery/2"},
			},
		}
		originalPeek := hookQueue.items
		_ = originalPeek
		// Wrap Peek so that after the first item is inspected, a PUBREL is queued into controlQueue
		wrappedQueue := &controlInterleavingDeliveryQueue{
			scriptedDeliveryQueue: hookQueue,
			onFirstPeek: func() {
				if !enqueuedPubRel {
					enqueuedPubRel = true
					_ = connection.enqueueControl(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 99})
				}
			},
		}
		runtime.server.deliveries = wrappedQueue

		done := make(chan error, 1)
		go func() { done <- connection.writeDeliveries() }()

		f1 := readTestFrame(t, clientSocket)
		p1, _, err := runtime.server.limits.Load().decoder.Decode(f1, mqtt.ProtocolV5)
		if err != nil {
			t.Fatalf("decode frame 1: %v", err)
		}
		pub1, ok := p1.(*mqtt.Publish)
		if !ok || pub1.Topic != "delivery/1" {
			t.Fatalf("frame 1 = %T %+v, want PUBLISH delivery/1", p1, p1)
		}

		f2 := readTestFrame(t, clientSocket)
		p2, _, err := runtime.server.limits.Load().decoder.Decode(f2, mqtt.ProtocolV5)
		if err != nil {
			t.Fatalf("decode frame 2: %v", err)
		}
		pubrel, ok := p2.(*mqtt.PubRel)
		if !ok || pubrel.PacketID != 99 {
			t.Fatalf("frame 2 = %T %+v, want PUBREL packet 99 serviced before delivery/2", p2, p2)
		}

		f3 := readTestFrame(t, clientSocket)
		p3, _, err := runtime.server.limits.Load().decoder.Decode(f3, mqtt.ProtocolV5)
		if err != nil {
			t.Fatalf("decode frame 3: %v", err)
		}
		pub2, ok := p3.(*mqtt.Publish)
		if !ok || pub2.Topic != "delivery/2" {
			t.Fatalf("frame 3 = %T %+v, want PUBLISH delivery/2", p3, p3)
		}

		if err := <-done; err != nil {
			t.Fatalf("writeDeliveries error: %v", err)
		}
	})
}
