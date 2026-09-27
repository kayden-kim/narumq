package tcp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
	applicationauthentication "narumq/internal/application/ports/authentication"
	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type adapterTestClock struct {
	now time.Time
}

func (clock *adapterTestClock) Now() time.Time {
	return clock.now
}

type adapterTestScheduler struct{}

func (adapterTestScheduler) AfterFunc(d time.Duration, f func()) connect.Timer {
	return time.AfterFunc(d, f)
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
	cluster  *adapterTestClusterState
}

type adapterTestClusterState struct {
	mu                sync.Mutex
	ownershipCalls    int
	subscriptionCalls []uint32
	subscriptionErr   error
}

func (state *adapterTestClusterState) AcquireOwnership(
	context.Context, string, string, uint64,
) (uint64, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.ownershipCalls++
	return 1, nil
}

func (state *adapterTestClusterState) CommitSubscription(
	_ context.Context,
	_ string, _ string,
	_ uint64, _ uint64,
	_ string, _ uint32, _ bool, _ bool, _ uint32,
	action uint32,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.subscriptionCalls = append(state.subscriptionCalls, action)
	if state.subscriptionErr != nil {
		return state.subscriptionErr
	}
	return nil
}

func (state *adapterTestClusterState) CommitDelivery(
	_ context.Context, change membership.DeliveryChangeDTO,
) (membership.ApplyResultDTO, error) {
	if change.Action != membership.DeliveryActionAttach {
		return membership.ApplyResultDTO{
			Outcome:  membership.ApplyOutcomeApplied,
			Delivery: &membership.DeliveryResultDTO{Found: true},
		}, nil
	}
	session := membership.DeliverySessionStateDTO{
		TenantID: change.TenantID, ClientID: change.ClientID, NextSequence: 1,
	}
	return membership.ApplyResultDTO{
		Outcome:  membership.ApplyOutcomeApplied,
		Delivery: &membership.DeliveryResultDTO{Session: &session},
	}, nil
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
	config.Clock = clock
	willBridge := NewWillBridge()
	var clusterState *adapterTestClusterState
	var connectionOptions []connect.Option
	var subscriptionOptions []subscribe.Option
	if config.ClusterMode {
		clusterState = &adapterTestClusterState{}
		connectionOptions = append(connectionOptions,
			connect.WithOwnershipCoordinator(clusterState, config.LocalMemberID),
			connect.WithDurableDeliveryCommitter(clusterState),
		)
		subscriptionOptions = append(subscriptionOptions, subscribe.WithCommitter(clusterState))
	}
	connections, err := connect.New(clock, repository, queue, routes, adapterTestScheduler{}, willBridge, connectionOptions...)
	if err != nil {
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes, queue, retainedRepository, subscriptionOptions...)
	if err != nil {
		t.Fatal(err)
	}
	publications, err := publish.New(clock, routes, queue, retainedRepository)
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
	return &adapterTestRuntime{server: server, listener: listener, client: client, cancel: cancel, done: done, repo: repository, cluster: clusterState}
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

func TestTCPClusterRejectsQoS1AndQoS2Mutations(t *testing.T) {
	for _, testCase := range []struct {
		name string
		qos  mqtt.QoS
	}{
		{name: "qos1", qos: mqtt.QoSAtLeastOnce},
		{name: "qos2", qos: mqtt.QoSExactlyOnce},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			config := defaultAdapterConfig()
			config.ClusterMode = true
			config.LocalMemberID = 4
			config.ClusterReady = func() bool { return true }
			runtime := newAdapterTestRuntime(t, 2, config)
			defer runtime.stop(t)

			writeTestPackets(t, runtime.client, &mqtt.Connect{
				Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "cluster-client",
			})
			_ = readTestFrame(t, runtime.client)
			writeTestPackets(t, runtime.client, &mqtt.Publish{
				Version: mqtt.ProtocolV5, Topic: "cluster/test", QoS: testCase.qos, PacketID: 1,
				Payload: []byte("not durably replicated"),
			})
			frame := readTestFrame(t, runtime.client)
			packet, consumed, err := runtime.server.decoder.Decode(frame, mqtt.ProtocolV5)
			if err != nil || consumed != len(frame) {
				t.Fatalf("decode QoS rejection: packet=%T consumed=%d err=%v", packet, consumed, err)
			}
			disconnect, ok := packet.(*mqtt.Disconnect)
			if !ok || disconnect.ReasonCode != 0x89 {
				t.Fatalf("cluster QoS %d rejection = %#v, want Server busy DISCONNECT", testCase.qos, packet)
			}
		})
	}
}

func TestTCPClusterReadinessAndSubscriptionCommitFailuresRejectPackets(t *testing.T) {
	t.Run("not-ready-connect", func(t *testing.T) {
		config := defaultAdapterConfig()
		config.ClusterMode = true
		config.LocalMemberID = 4
		config.ClusterReady = func() bool { return false }
		runtime := newAdapterTestRuntime(t, 2, config)
		defer runtime.stop(t)

		writeTestPackets(t, runtime.client, &mqtt.Connect{
			Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "cluster-client",
		})
		frame := readTestFrame(t, runtime.client)
		packet, consumed, err := runtime.server.decoder.Decode(frame, mqtt.ProtocolV5)
		if err != nil || consumed != len(frame) {
			t.Fatalf("decode rejected CONNACK: packet=%T consumed=%d err=%v", packet, consumed, err)
		}
		connAck, ok := packet.(*mqtt.ConnAck)
		if !ok || connAck.ReasonCode != 0x88 {
			t.Fatalf("not-ready CONNACK = %#v", packet)
		}
		runtime.cluster.mu.Lock()
		ownershipCalls := runtime.cluster.ownershipCalls
		runtime.cluster.mu.Unlock()
		if ownershipCalls != 0 {
			t.Fatalf("not-ready CONNECT committed ownership %d times", ownershipCalls)
		}
	})

	t.Run("subscription-quorum-failure", func(t *testing.T) {
		config := defaultAdapterConfig()
		config.ClusterMode = true
		config.LocalMemberID = 4
		config.ClusterReady = func() bool { return true }
		runtime := newAdapterTestRuntime(t, 2, config)
		defer runtime.stop(t)
		runtime.cluster.mu.Lock()
		runtime.cluster.subscriptionErr = errors.New("quorum unavailable")
		runtime.cluster.mu.Unlock()

		writeTestPackets(t, runtime.client, &mqtt.Connect{
			Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "cluster-client",
		})
		_ = readTestFrame(t, runtime.client)
		writeTestPackets(t, runtime.client, &mqtt.Subscribe{
			Version: mqtt.ProtocolV5, PacketID: 1,
			Subscriptions: []mqtt.Subscription{{Filter: "sensors/#", QoS: mqtt.QoSAtLeastOnce}},
		})
		if err := runtime.client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			if !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
				t.Fatal(err)
			}
		} else {
			var response [1]byte
			count, err := runtime.client.Read(response[:])
			if err == nil || count != 0 {
				t.Fatalf("failed subscription emitted %d response bytes (% X), err=%v", count, response[:count], err)
			}
		}
		runtime.cluster.mu.Lock()
		actions := append([]uint32(nil), runtime.cluster.subscriptionCalls...)
		runtime.cluster.mu.Unlock()
		if !reflect.DeepEqual(actions, []uint32{1}) {
			t.Fatalf("committed subscription attempts = %v", actions)
		}
	})
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
		config:  Config{Clock: &adapterTestClock{now: time.Now()}, WriteTimeout: 20 * time.Millisecond},
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
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(1024)
	if err != nil {
		t.Fatal(err)
	}
	runtimeServer := &Server{
		config:     Config{Clock: &adapterTestClock{now: time.Now()}, WriteTimeout: time.Second},
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

// Traceability: PRD MQTT-03/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §3.2.
func TestTCPDeliveryBlockedUntilConnAckReady(t *testing.T) {
	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()
	queue, err := deliveryqueue.New(2)
	if err != nil {
		t.Fatal(err)
	}
	const sessionID = routing.SessionID("connack-gate")
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID,
		Topic:     "gate/topic",
		Payload:   []byte("delivery"),
		QoS:       mqtt.QoSAtMostOnce,
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(1024)
	if err != nil {
		t.Fatal(err)
	}
	runtimeServer := &Server{
		config:     Config{Clock: &adapterTestClock{now: time.Now()}, WriteTimeout: time.Second},
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
	connection.deliveryBlocked.Store(true)
	connection.deliveryWake <- struct{}{}
	go connection.writerLoop()
	deadline := time.Now().Add(time.Second)
	for len(connection.deliveryWake) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("writer did not consume blocked delivery wake")
		}
		time.Sleep(time.Millisecond)
	}
	if err := connection.enqueueBarrier(); err != nil {
		t.Fatalf("writer barrier after blocked wake = %v", err)
	}
	if err := clientSocket.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var header [1]byte
	if _, err := clientSocket.Read(header[:]); err == nil {
		t.Fatal("delivery was written while CONNACK gate was blocked")
	} else {
		var networkError net.Error
		if !errors.As(err, &networkError) || !networkError.Timeout() {
			t.Fatalf("blocked delivery read error = %v, want timeout", err)
		}
	}

	connection.deliveryBlocked.Store(false)
	connection.wakeDelivery()
	frame := readTestFrame(t, clientSocket)
	if len(frame) < 2 || frame[0] != 0x30 {
		t.Fatalf("unblocked delivery frame = % X, want QoS 0 PUBLISH", frame)
	}
	connection.requestClose()
	select {
	case <-connection.writerDone:
	case <-time.After(time.Second):
		t.Fatal("gated writer did not stop")
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
		config:  Config{Clock: &adapterTestClock{now: time.Now()}, WriteTimeout: 20 * time.Millisecond},
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

func TestWillBridge_Unbound(t *testing.T) {
	bridge := NewWillBridge()
	if err := bridge.PublishWill("session", mqtt.Will{Topic: "test"}); err != nil {
		t.Fatalf("unbound PublishWill error = %v", err)
	}
}

type testServerAuthenticator struct {
	result applicationauthentication.Result
}

func (a *testServerAuthenticator) Authenticate(ctx context.Context, credential applicationauthentication.Credential) (applicationauthentication.Result, error) {
	return a.result, nil
}

// Requirements: SEC-06, TEN-04
func TestTCPServer_UnauthorizedPublishAndSubscribe(t *testing.T) {
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	allowedTopicFilter, err := tenancy.NewTopicFilter(tenant, "allowed/#")
	if err != nil {
		t.Fatal(err)
	}
	rulePub, err := security.NewRule(tenant, "device", security.EffectAllow, security.ActionPublish, allowedTopicFilter)
	if err != nil {
		t.Fatal(err)
	}
	ruleSub, err := security.NewRule(tenant, "device", security.EffectAllow, security.ActionSubscribe, allowedTopicFilter)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := security.NewPolicy([]security.Rule{rulePub, ruleSub})
	if err != nil {
		t.Fatal(err)
	}

	auth := &testServerAuthenticator{
		result: applicationauthentication.Result{
			TenantID:         tenant,
			Subject:          "device-1",
			CredentialID:     "cred-1",
			AllowedClientIDs: []string{"client-1"},
			Roles:            []security.Role{"device"},
			ExpiresAt:        time.Now().Add(time.Hour),
		},
	}

	listener := newPipeListener()
	routes := routing.NewTable()
	queue, err := deliveryqueue.New(16)
	if err != nil {
		t.Fatal(err)
	}
	repository := sessionstore.New()
	retainedRepository, err := memoryretained.New(32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	clock := &adapterTestClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
	willBridge := NewWillBridge()
	connections, err := connect.New(clock, repository, queue, routes, adapterTestScheduler{}, willBridge, connect.WithSecurity(auth, nil, policy))
	if err != nil {
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes, queue, retainedRepository, subscribe.WithPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	publications, err := publish.New(clock, routes, queue, retainedRepository, publish.WithPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(1024)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(1024)
	if err != nil {
		t.Fatal(err)
	}
	cfg := defaultAdapterConfig()
	cfg.Clock = clock
	cfg.Policy = policy
	server, err := NewServer(listener, cfg, decoder, encoder, connections, subscriptions, publications, queue, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		<-done
	})

	client, serverConn := net.Pipe()
	listener.connections <- serverConn
	<-listener.accepted

	// CONNECT with valid credentials
	connectPkt := &mqtt.Connect{
		Version:     mqtt.ProtocolV5,
		ClientID:    "client-1",
		HasUsername: true,
		HasPassword: true,
		Password:    []byte("dummy.jwt"),
	}
	if _, err := client.Write(encodeTestPacket(t, connectPkt)); err != nil {
		t.Fatal(err)
	}
	connAckWire := readTestFrame(t, client)
	decodedConnAck, _, err := decoder.Decode(connAckWire, mqtt.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	connAck, ok := decodedConnAck.(*mqtt.ConnAck)
	if !ok || connAck.ReasonCode != 0 {
		t.Fatalf("CONNACK = %#v, want ReasonCode 0", decodedConnAck)
	}

	// Publish QoS 1 to unauthorized topic -> must get PubAck 0x87
	unauthPub := &mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		PacketID: 10,
		Topic:    "forbidden/topic",
		Payload:  []byte("data"),
		QoS:      mqtt.QoSAtLeastOnce,
	}
	if _, err := client.Write(encodeTestPacket(t, unauthPub)); err != nil {
		t.Fatal(err)
	}
	pubAckWire := readTestFrame(t, client)
	decodedPubAck, _, err := decoder.Decode(pubAckWire, mqtt.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	pubAck, ok := decodedPubAck.(*mqtt.PubAck)
	if !ok || pubAck.PacketID != 10 || pubAck.ReasonCode != 0x87 {
		t.Fatalf("unauthorized PubAck = %#v, want PacketID 10 and ReasonCode 0x87", decodedPubAck)
	}

	// Subscribe to unauthorized filter -> must get SubAck 0x87
	unauthSub := &mqtt.Subscribe{
		Version:       mqtt.ProtocolV5,
		PacketID:      20,
		Subscriptions: []mqtt.Subscription{{Filter: "forbidden/#", QoS: mqtt.QoSAtLeastOnce}},
	}
	if _, err := client.Write(encodeTestPacket(t, unauthSub)); err != nil {
		t.Fatal(err)
	}
	subAckWire := readTestFrame(t, client)
	decodedSubAck, _, err := decoder.Decode(subAckWire, mqtt.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	subAck, ok := decodedSubAck.(*mqtt.SubAck)
	if !ok || subAck.PacketID != 20 || len(subAck.ReasonCodes) != 1 || subAck.ReasonCodes[0] != 0x87 {
		t.Fatalf("unauthorized SubAck = %#v, want ReasonCode 0x87", decodedSubAck)
	}

	// Publish QoS 1 to authorized topic -> must get PubAck 0x00
	authPub := &mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		PacketID: 30,
		Topic:    "allowed/data",
		Payload:  []byte("data"),
		QoS:      mqtt.QoSAtLeastOnce,
	}
	if _, err := client.Write(encodeTestPacket(t, authPub)); err != nil {
		t.Fatal(err)
	}
	authPubAckWire := readTestFrame(t, client)
	decodedAuthPubAck, _, err := decoder.Decode(authPubAckWire, mqtt.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	authPubAck, ok := decodedAuthPubAck.(*mqtt.PubAck)
	if !ok || authPubAck.PacketID != 30 || authPubAck.ReasonCode != 0x00 {
		t.Fatalf("authorized PubAck = %#v, want ReasonCode 0x00", decodedAuthPubAck)
	}

	// Subscribe to authorized filter -> must get SubAck 0x01
	authSub := &mqtt.Subscribe{
		Version:       mqtt.ProtocolV5,
		PacketID:      40,
		Subscriptions: []mqtt.Subscription{{Filter: "allowed/data", QoS: mqtt.QoSAtLeastOnce}},
	}
	if _, err := client.Write(encodeTestPacket(t, authSub)); err != nil {
		t.Fatal(err)
	}
	authSubAckWire := readTestFrame(t, client)
	decodedAuthSubAck, _, err := decoder.Decode(authSubAckWire, mqtt.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	authSubAck, ok := decodedAuthSubAck.(*mqtt.SubAck)
	if !ok || authSubAck.PacketID != 40 || len(authSubAck.ReasonCodes) != 1 || authSubAck.ReasonCodes[0] != 0x01 {
		t.Fatalf("authorized SubAck = %#v, want ReasonCode 0x01", decodedAuthSubAck)
	}

	_ = client.Close()
}

type testServerCertificateAuthenticator struct {
	result      applicationauthentication.Result
	certificate applicationauthentication.ClientCertificateDTO
	calls       int
}

func (a *testServerCertificateAuthenticator) AuthenticateClientCertificate(ctx context.Context, certificate applicationauthentication.ClientCertificateDTO) (applicationauthentication.Result, error) {
	a.calls++
	a.certificate = certificate
	return a.result, nil
}

func newCertificateAdapterTestRuntime(t *testing.T, config Config, authenticator applicationauthentication.ClientCertificateAuthenticationProvider) *adapterTestRuntime {
	t.Helper()
	listener := newPipeListener()
	routes := routing.NewTable()
	queue, err := deliveryqueue.New(2)
	if err != nil {
		t.Fatal(err)
	}
	repository := sessionstore.New()
	retainedRepository, err := memoryretained.New(32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	clock := &adapterTestClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
	config.Clock = clock
	willBridge := NewWillBridge()
	connections, err := connect.New(clock, repository, queue, routes, adapterTestScheduler{}, willBridge, connect.WithSecurity(nil, authenticator, security.Policy{}))
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

// Requirements: SEC-03, SEC-04; TCP carries only the application certificate DTO.
func TestTCPServerPassesExtractedCertificateToConnect(t *testing.T) {
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	authenticator := &testServerCertificateAuthenticator{result: applicationauthentication.Result{
		TenantID:         tenant,
		Subject:          "certificate-subject",
		CredentialID:     "certificate-credential",
		AllowedClientIDs: []string{"certificate-client"},
		Roles:            []security.Role{"publisher"},
		ExpiresAt:        time.Date(2026, time.January, 1, 1, 0, 0, 0, time.UTC),
	}}
	extractorCalls := 0
	config := defaultAdapterConfig()
	config.ClientCertificateExtractor = func(net.Conn) (applicationauthentication.ClientCertificateDTO, bool) {
		extractorCalls++
		return applicationauthentication.ClientCertificateDTO{
			CommonName: "certificate-device",
			NotAfter:   time.Date(2026, time.January, 1, 1, 0, 0, 0, time.UTC),
		}, true
	}
	runtime := newCertificateAdapterTestRuntime(t, config, authenticator)
	defer runtime.stop(t)

	writeTestPackets(t, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "certificate-client"})
	connAck := readTestFrame(t, runtime.client)
	if len(connAck) < 4 || connAck[0] != 0x20 || connAck[3] != 0 {
		t.Fatalf("mTLS CONNACK = % X", connAck)
	}
	if extractorCalls != 1 || authenticator.calls != 1 {
		t.Fatalf("certificate propagation calls = extractor:%d authenticator:%d", extractorCalls, authenticator.calls)
	}
	wantNotAfter := time.Date(2026, time.January, 1, 1, 0, 0, 0, time.UTC)
	if authenticator.certificate.CommonName != "certificate-device" || !authenticator.certificate.NotAfter.Equal(wantNotAfter) {
		t.Fatalf("certificate DTO = %#v, want CommonName certificate-device and NotAfter %s", authenticator.certificate, wantNotAfter)
	}
}

// Requirements: SEC-04; no verified certificate keeps the certificate-free path.
func TestTCPServerCertificateExtractorAbsenceUsesAnonymousPath(t *testing.T) {
	extractorCalls := 0
	config := defaultAdapterConfig()
	config.ClientCertificateExtractor = func(net.Conn) (applicationauthentication.ClientCertificateDTO, bool) {
		extractorCalls++
		return applicationauthentication.ClientCertificateDTO{}, false
	}
	runtime := newAdapterTestRuntime(t, 2, config)
	defer runtime.stop(t)

	writeTestPackets(t, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "anonymous-client"})
	connAck := readTestFrame(t, runtime.client)
	if len(connAck) < 4 || connAck[0] != 0x20 || connAck[3] != 0 {
		t.Fatalf("certificate-free CONNACK = % X", connAck)
	}
	if extractorCalls != 1 {
		t.Fatalf("certificate extractor calls = %d, want 1", extractorCalls)
	}
}

var _ applicationdeliveryqueue.Queue = (*deliveryqueue.Queue)(nil)
