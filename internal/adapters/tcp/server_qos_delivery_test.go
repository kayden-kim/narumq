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
	}); err != nil {
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
	items, err := runtime.server.deliveries.Drain("sub-session")
	if err != nil {
		t.Fatal(err)
	}
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
	itemsAfterDup, err := runtime.server.deliveries.Drain("sub-session")
	if err != nil {
		t.Fatal(err)
	}
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
	}); err != nil {
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
	items, err := runtime.server.deliveries.Drain("sub2-session")
	if err != nil {
		t.Fatal(err)
	}
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
	itemsAfterRel, err := runtime.server.deliveries.Drain("sub2-session")
	if err != nil {
		t.Fatal(err)
	}
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
	}); err != nil {
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
	}); err != nil {
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
	}); err != nil {
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
	}); err != nil {
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
	}); err != nil {
		t.Fatal(err)
	}

	// writeDeliveries should safely return nil and re-enqueue item
	if err := conn.writeDeliveries(); err != nil {
		t.Fatalf("writeDeliveries when exhausted = %v, want nil", err)
	}

	// Verify item was re-enqueued
	items, err := runtime.server.deliveries.Drain(conn.sessionID)
	if err != nil {
		t.Fatal(err)
	}
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
	publications, err := publish.New(routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	config := defaultAdapterConfig()
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
	}); err != nil {
		t.Fatal(err)
	}

	// Fill queue with 1 item so it's saturated
	if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: "sat-session",
		Topic:     "sat/1",
		Payload:   []byte("1"),
		QoS:       mqtt.QoSAtMostOnce,
	}); err != nil {
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
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: "sat-session-2",
		Topic:     "sat2/1",
		Payload:   []byte("1"),
		QoS:       mqtt.QoSAtMostOnce,
	}); err != nil {
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
	conn.inbound[77] = delivery.Inbound{} // zero value has phase: inboundInvalid
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
	conn.inbound[78] = delivery.Inbound{}
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
