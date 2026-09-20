package tcp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	"narumq/internal/application/connect"
	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

type adapterTestClock struct {
	now time.Time
}

func (clock *adapterTestClock) Now() time.Time {
	return clock.now
}

type pipeListener struct {
	connections chan net.Conn
	accepted    chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		connections: make(chan net.Conn, 1),
		accepted:    make(chan struct{}, 1),
		closed:      make(chan struct{}),
	}
}

func (listener *pipeListener) Accept() (net.Conn, error) {
	select {
	case connection := <-listener.connections:
		listener.accepted <- struct{}{}
		return connection, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *pipeListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}

func (listener *pipeListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
}

type adapterTestRuntime struct {
	server   *Server
	listener *pipeListener
	client   net.Conn
	cancel   context.CancelFunc
	done     chan error
	repo     *sessionstore.MemorySessionRepository
}

func newAdapterTestRuntime(t *testing.T, queueCapacity int, config Config) *adapterTestRuntime {
	t.Helper()
	listener := newPipeListener()
	routes := routing.NewTable()
	queue, err := deliveryqueue.New(queueCapacity)
	if err != nil {
		t.Fatal(err)
	}
	repository := sessionstore.New()
	retainedRepository, err := memoryretained.New(32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	clock := &adapterTestClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
	connections, err := connect.New(clock, repository, queue, routes)
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

func (runtime *adapterTestRuntime) stop(t *testing.T) {
	t.Helper()
	runtime.cancel()
	select {
	case err := <-runtime.done:
		if err != nil {
			t.Fatalf("Serve() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not join after cancellation")
	}
	_ = runtime.client.Close()
}

func defaultAdapterConfig() Config {
	return Config{
		MaxPacketSize:        1024,
		ConnectTimeout:       250 * time.Millisecond,
		WriteTimeout:         100 * time.Millisecond,
		ControlQueueCapacity: 4,
	}
}

func encodeTestPacket(t *testing.T, packet mqtt.Packet) []byte {
	t.Helper()
	encoder, err := mqtt.NewEncoder(1024)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encoder.Append(nil, packet)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func readTestFrame(t *testing.T, connection net.Conn) []byte {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var header [1]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		t.Fatalf("read frame header: %v", err)
	}
	remainingLength := 0
	multiplier := 1
	var encodedLength []byte
	for {
		var value [1]byte
		if _, err := io.ReadFull(connection, value[:]); err != nil {
			t.Fatalf("read remaining length: %v", err)
		}
		encodedLength = append(encodedLength, value[0])
		remainingLength += int(value[0]&0x7F) * multiplier
		if value[0]&0x80 == 0 {
			break
		}
		multiplier *= 128
	}
	body := make([]byte, remainingLength)
	if _, err := io.ReadFull(connection, body); err != nil {
		t.Fatalf("read frame body: %v", err)
	}
	return append(append([]byte{header[0]}, encodedLength...), body...)
}

func writeTestPackets(t *testing.T, connection net.Conn, packets ...mqtt.Packet) {
	t.Helper()
	var wire []byte
	for _, packet := range packets {
		wire = append(wire, encodeTestPacket(t, packet)...)
	}
	if _, err := connection.Write(wire); err != nil {
		t.Fatalf("write packets: %v", err)
	}
}

func requireConnectionClosed(t *testing.T, connection net.Conn) {
	t.Helper()
	requireConnectionClosedWithin(t, connection, time.Second)
}

func requireConnectionClosedWithin(t *testing.T, connection net.Conn, timeout time.Duration) {
	t.Helper()
	_ = connection.SetReadDeadline(time.Now().Add(timeout))
	var value [256]byte
	for {
		if _, err := connection.Read(value[:]); err != nil {
			var networkError net.Error
			if errors.As(err, &networkError) && networkError.Timeout() {
				t.Fatalf("connection did not close before deadline: %v", err)
			}
			return
		}
	}
}

// Traceability: PRD MQTT-01/MQTT-03/TEST-03; docs/specs/mqtt-protocol-and-session.md §1 and §2.
func TestTCPConnectTimeoutDoesNotCreateSession(t *testing.T) {
	config := defaultAdapterConfig()
	config.ConnectTimeout = 25 * time.Millisecond
	runtime := newAdapterTestRuntime(t, 2, config)
	defer runtime.stop(t)
	requireConnectionClosed(t, runtime.client)
	if _, found, err := runtime.repo.Load("timeout-client"); err != nil || found {
		t.Fatalf("timeout session = found %v, err %v", found, err)
	}
}

// Traceability: PRD MQTT-01/MQTT-02/TEST-03; fragmented frames and multiple
// complete frames must use the decoder boundary without losing bytes.
func TestTCPFragmentedConnectAndMultipleFrames(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)
	connect := &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "fragmented"}
	connectWire := encodeTestPacket(t, connect)
	if _, err := runtime.client.Write(connectWire[:2]); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.client.Write(connectWire[2:]); err != nil {
		t.Fatal(err)
	}
	connAck := readTestFrame(t, runtime.client)
	if connAck[0] != 0x20 || connAck[2] != 0x00 {
		t.Fatalf("CONNACK wire = % X", connAck)
	}
	writeTestPackets(t, runtime.client, &mqtt.PingReq{Version: mqtt.ProtocolV5}, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
	pingResp := readTestFrame(t, runtime.client)
	if len(pingResp) != 2 || pingResp[0] != 0xD0 || pingResp[1] != 0x00 {
		t.Fatalf("PINGRESP wire = % X", pingResp)
	}
	requireConnectionClosed(t, runtime.client)
	if _, found, err := runtime.repo.Load("fragmented"); err != nil || found {
		t.Fatalf("normal disconnect session = found %v, err %v", found, err)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestTCPMalformedAndOversizeInputClosesBeforeSessionCreation(t *testing.T) {
	config := defaultAdapterConfig()
	config.MaxPacketSize = 8
	runtime := newAdapterTestRuntime(t, 2, config)
	defer runtime.stop(t)
	if _, err := runtime.client.Write([]byte{0x10, 0x09}); err != nil {
		t.Fatal(err)
	}
	requireConnectionClosed(t, runtime.client)
	if _, found, err := runtime.repo.Load("oversize"); err != nil || found {
		t.Fatalf("oversize session = found %v, err %v", found, err)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestTCPKeepAliveTimeoutClosesConnection(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)
	writeTestPackets(t, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "keepalive", KeepAlive: 1})
	_ = readTestFrame(t, runtime.client)
	requireConnectionClosedWithin(t, runtime.client, 3*time.Second)
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestTCPControlQueueFullClosesConnection(t *testing.T) {
	config := defaultAdapterConfig()
	config.ControlQueueCapacity = 1
	runtime := newAdapterTestRuntime(t, 2, config)
	defer runtime.stop(t)
	writeTestPackets(t, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "control-full"})
	_ = readTestFrame(t, runtime.client)
	ping := encodeTestPacket(t, &mqtt.PingReq{Version: mqtt.ProtocolV5})
	var wire []byte
	for i := 0; i < 16; i++ {
		wire = append(wire, ping...)
	}
	_, _ = runtime.client.Write(wire)
	requireConnectionClosedWithin(t, runtime.client, 3*time.Second)
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
// A blocked subscriber writer plus one queued item must turn the next enqueue
// into a deterministic connection close rather than unbounded memory growth.
func TestTCPDeliveryQueueSaturationClosesSubscriber(t *testing.T) {
	config := defaultAdapterConfig()
	config.WriteTimeout = 10 * time.Second
	runtime := newAdapterTestRuntime(t, 1, config)
	defer runtime.stop(t)

	writeTestPackets(t, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "subscriber"})
	_ = readTestFrame(t, runtime.client)
	writeTestPackets(t, runtime.client, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{
			{Filter: "saturation/#", QoS: mqtt.QoSAtMostOnce},
		},
	})
	_ = readTestFrame(t, runtime.client)

	publisherClient, publisherServer := net.Pipe()
	defer publisherClient.Close()
	runtime.listener.connections <- publisherServer
	select {
	case <-runtime.listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("server did not accept publisher connection")
	}
	writeTestPackets(t, publisherClient, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "publisher"})
	_ = readTestFrame(t, publisherClient)

	for packetID := byte(1); packetID <= 3; packetID++ {
		writeTestPackets(t, publisherClient, &mqtt.Publish{
			Version: mqtt.ProtocolV5,
			Topic:   "saturation/topic",
			Payload: []byte{packetID},
		})
	}
	writeTestPackets(t, publisherClient, &mqtt.PingReq{Version: mqtt.ProtocolV5})
	pingResp := readTestFrame(t, publisherClient)
	if len(pingResp) != 2 || pingResp[0] != 0xD0 || pingResp[1] != 0x00 {
		t.Fatalf("publisher PINGRESP = % X", pingResp)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, found, err := runtime.repo.Load("subscriber")
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("saturated subscriber session remained stored")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestTCPBlockedWriteHonorsTimeout(t *testing.T) {
	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()
	encoder, err := mqtt.NewEncoder(16)
	if err != nil {
		t.Fatal(err)
	}
	runtimeServer := &Server{
		config:  Config{WriteTimeout: 20 * time.Millisecond},
		encoder: encoder,
	}
	connection := &connection{server: runtimeServer, socket: serverSocket}
	done := make(chan error, 1)
	go func() {
		done <- connection.writePacket(&mqtt.PingResp{Version: mqtt.ProtocolV5})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked writer unexpectedly completed")
		}
		var networkError net.Error
		if !errors.As(err, &networkError) || !networkError.Timeout() {
			t.Fatalf("blocked writer error = %v, want timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked writer did not honor timeout")
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; control packets must remain ahead
// of deliveries once both writer inputs are ready.
func TestTCPSingleWriterPrioritizesControlOverDelivery(t *testing.T) {
	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()
	queue, err := deliveryqueue.New(2)
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = routing.SessionID("subscriber")
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID,
		Topic:     "ordered/topic",
		Payload:   []byte("delivery"),
		QoS:       mqtt.QoSAtMostOnce,
	}); err != nil {
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(1024)
	if err != nil {
		t.Fatal(err)
	}
	runtimeServer := &Server{
		config:     Config{WriteTimeout: time.Second},
		encoder:    encoder,
		deliveries: queue,
	}
	connection := &connection{
		server:       runtimeServer,
		socket:       serverSocket,
		controlQueue: make(chan controlMessage, 1),
		deliveryWake: make(chan struct{}, 1),
		stop:         make(chan struct{}),
		writerDone:   make(chan struct{}),
		sessionID:    sessionID,
		version:      mqtt.ProtocolV5,
	}
	connection.controlQueue <- controlMessage{packet: &mqtt.PingResp{Version: mqtt.ProtocolV5}}
	connection.deliveryWake <- struct{}{}
	go connection.writerLoop()

	first := readTestFrame(t, clientSocket)
	second := readTestFrame(t, clientSocket)
	if len(first) != 2 || first[0] != 0xD0 || first[1] != 0x00 {
		t.Fatalf("first frame = % X, want PINGRESP", first)
	}
	if len(second) < 2 || second[0] != 0x30 {
		t.Fatalf("second frame = % X, want QoS 0 PUBLISH", second)
	}
	connection.requestClose()
	select {
	case <-connection.writerDone:
	case <-time.After(time.Second):
		t.Fatal("single writer did not stop")
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; write timeout must close the target
// connection, not merely return an error from the low-level write helper.
func TestTCPWriterClosesConnectionAfterWriteTimeout(t *testing.T) {
	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()
	encoder, err := mqtt.NewEncoder(16)
	if err != nil {
		t.Fatal(err)
	}
	runtimeServer := &Server{
		config:  Config{WriteTimeout: 20 * time.Millisecond},
		encoder: encoder,
	}
	connection := &connection{
		server:       runtimeServer,
		socket:       serverSocket,
		controlQueue: make(chan controlMessage, 1),
		deliveryWake: make(chan struct{}, 1),
		stop:         make(chan struct{}),
		writerDone:   make(chan struct{}),
	}
	connection.controlQueue <- controlMessage{packet: &mqtt.PingResp{Version: mqtt.ProtocolV5}}
	go connection.writerLoop()
	select {
	case <-connection.writerDone:
	case <-time.After(time.Second):
		t.Fatal("writer did not stop after write timeout")
	}
	_ = clientSocket.SetReadDeadline(time.Now().Add(time.Second))
	var value [1]byte
	if _, err := clientSocket.Read(value[:]); err == nil {
		t.Fatal("client socket remained open after write timeout")
	} else {
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			t.Fatalf("client socket was not closed: %v", err)
		}
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; cancellation must close sockets and
// join every started connection goroutine.
func TestTCPCancellationJoinsConnections(t *testing.T) {
	config := defaultAdapterConfig()
	config.ConnectTimeout = 10 * time.Second
	runtime := newAdapterTestRuntime(t, 2, config)
	writeTestPackets(t, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "cancel"})
	_ = readTestFrame(t, runtime.client)
	runtime.cancel()
	select {
	case err := <-runtime.done:
		if err != nil {
			t.Fatalf("Serve() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not join active connection")
	}
	_ = runtime.client.Close()
}

func TestNewServerRejectsInvalidDependenciesAndBounds(t *testing.T) {
	listener := newPipeListener()
	decoder, err := mqtt.NewDecoder(16)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(16)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewServer(nil, defaultAdapterConfig(), decoder, encoder, nil, nil, nil, nil, nil); !errors.Is(err, ErrNilListener) {
		t.Fatalf("nil listener error = %v", err)
	}
	if _, err := NewServer(listener, Config{}, decoder, encoder, nil, nil, nil, nil, nil); !errors.Is(err, ErrNilConnectUseCase) {
		// Dependency checks are deliberately performed before configuration checks.
		// A nil use case is the first missing required boundary after the listener and codec.
		t.Fatalf("nil use case error = %v", err)
	}
}

var _ applicationdeliveryqueue.Queue = (*deliveryqueue.Queue)(nil)
