package tcp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"testing"
	"time"

	"narumq/internal/application/connect"
	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
)

func TestNewServerValidatesEveryDependencyAndBound(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)
	valid := runtime.server
	cases := []struct {
		name string
		make func() (*Server, error)
		want error
	}{
		{name: "decoder", make: func() (*Server, error) {
			return NewServer(runtime.listener, valid.config, nil, valid.encoder, valid.connections, valid.subscriptions, valid.publications, valid.deliveries, nil)
		}, want: ErrNilDecoder},
		{name: "encoder", make: func() (*Server, error) {
			return NewServer(runtime.listener, valid.config, valid.decoder, nil, valid.connections, valid.subscriptions, valid.publications, valid.deliveries, nil)
		}, want: ErrNilEncoder},
		{name: "connect", make: func() (*Server, error) {
			return NewServer(runtime.listener, valid.config, valid.decoder, valid.encoder, nil, valid.subscriptions, valid.publications, valid.deliveries, nil)
		}, want: ErrNilConnectUseCase},
		{name: "subscribe", make: func() (*Server, error) {
			return NewServer(runtime.listener, valid.config, valid.decoder, valid.encoder, valid.connections, nil, valid.publications, valid.deliveries, nil)
		}, want: ErrNilSubscribeUseCase},
		{name: "publish", make: func() (*Server, error) {
			return NewServer(runtime.listener, valid.config, valid.decoder, valid.encoder, valid.connections, valid.subscriptions, nil, valid.deliveries, nil)
		}, want: ErrNilPublishUseCase},
		{name: "delivery queue", make: func() (*Server, error) {
			return NewServer(runtime.listener, valid.config, valid.decoder, valid.encoder, valid.connections, valid.subscriptions, valid.publications, nil, nil)
		}, want: ErrNilDeliveryQueue},
		{name: "clock", make: func() (*Server, error) {
			return NewServer(runtime.listener, Config{}, valid.decoder, valid.encoder, valid.connections, valid.subscriptions, valid.publications, valid.deliveries, nil)
		}, want: ErrNilClock},
		{name: "config", make: func() (*Server, error) {
			return NewServer(runtime.listener, Config{Clock: valid.config.Clock}, valid.decoder, valid.encoder, valid.connections, valid.subscriptions, valid.publications, valid.deliveries, nil)
		}, want: ErrInvalidConfig},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			created, err := testCase.make()
			if created != nil || !errors.Is(err, testCase.want) {
				t.Fatalf("NewServer() = (%#v, %v), want %v", created, err, testCase.want)
			}
		})
	}
	created, err := NewServer(runtime.listener, valid.config, valid.decoder, valid.encoder, valid.connections, valid.subscriptions, valid.publications, valid.deliveries, nil)
	if err != nil || created == nil || created.logger == nil {
		t.Fatalf("NewServer(nil logger) = (%#v, %v), want default logger", created, err)
	}
}

func TestServeAndServerSessionHelpersBoundaries(t *testing.T) {
	var nilServer *Server
	if err := nilServer.Serve(context.Background()); !errors.Is(err, ErrNilListener) {
		t.Fatalf("nil Server.Serve() error = %v", err)
	}
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)
	if err := runtime.server.Serve(nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context Serve() error = %v", err)
	}
	firstSocket, firstPeer := net.Pipe()
	defer firstPeer.Close()
	connection := newConnection(runtime.server, firstSocket)
	if !runtime.server.bindSession("session", connection) {
		t.Fatal("first bindSession() failed")
	}
	secondSocket, secondPeer := net.Pipe()
	defer secondPeer.Close()
	duplicate := newConnection(runtime.server, secondSocket)
	if runtime.server.bindSession("session", duplicate) {
		t.Fatal("duplicate bindSession() succeeded")
	}
	runtime.server.wakeSession("session")
	runtime.server.closeSession("session")
	runtime.server.unbindSession("session", duplicate)
	runtime.server.unbindSession("session", connection)
	runtime.server.stopAll()
	duplicate.requestClose()
	connection.requestClose()
}

func TestConnectionQueueAndDispatchBoundaries(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)
	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()
	connection := &connection{
		server:       runtime.server,
		socket:       serverSocket,
		controlQueue: make(chan controlMessage, 1),
		deliveryWake: make(chan struct{}, 1),
		stop:         make(chan struct{}),
		writerDone:   make(chan struct{}),
		sessionID:    "session",
		version:      mqtt.ProtocolV5,
	}
	if err := connection.enqueueControl(nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("nil enqueueControl() error = %v", err)
	}
	if err := connection.enqueueAndWait(nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("nil enqueueAndWait() error = %v", err)
	}
	if err := connection.dispatch(&mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "unexpected"}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("unexpected dispatch() error = %v", err)
	}
	connection.version = mqtt.ProtocolV311
	if err := connection.enqueueNegativePublishAck(mqtt.Publish{QoS: mqtt.QoSAtLeastOnce, PacketID: 1}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("MQTT 3.1.1 negative ACK error = %v", err)
	}
	connection.version = mqtt.ProtocolV5
	if err := connection.enqueueNegativePublishAck(mqtt.Publish{QoS: mqtt.QoSAtMostOnce}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("QoS 0 negative ACK error = %v", err)
	}
	if err := connection.enqueueControl(&mqtt.PingResp{Version: mqtt.ProtocolV5}); err != nil {
		t.Fatal(err)
	}
	if err := connection.enqueueControl(&mqtt.PingResp{Version: mqtt.ProtocolV5}); !errors.Is(err, ErrControlQueueFull) {
		t.Fatalf("full enqueueControl() error = %v", err)
	}
	connection.requestClose()
	if err := connection.enqueueControl(&mqtt.PingResp{Version: mqtt.ProtocolV5}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("closed enqueueControl() error = %v", err)
	}
}

func TestHandleUnsupportedVersionBranches(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)
	for _, testCase := range []struct {
		name      string
		err       error
		wantClose bool
	}{
		{name: "non unsupported", err: mqtt.ErrMalformedPacket, wantClose: false},
		{name: "MQIsdp", err: &mqtt.UnsupportedProtocolVersionError{ProtocolName: "MQIsdp", Level: 3}, wantClose: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			serverSocket, clientSocket := net.Pipe()
			defer clientSocket.Close()
			connection := &connection{server: runtime.server, socket: serverSocket, controlQueue: make(chan controlMessage, 1), stop: make(chan struct{}), writerDone: make(chan struct{})}
			if got := connection.handleUnsupportedVersion(testCase.err); got != testCase.wantClose {
				t.Fatalf("handleUnsupportedVersion() = %v, want %v", got, testCase.wantClose)
			}
			if !testCase.wantClose {
				_ = serverSocket.Close()
			}
		})
	}
}

func TestUnsupportedMQTTVersionSendsConnAck(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)
	for _, testCase := range []struct {
		name    string
		version byte
	}{
		{name: "MQTT 3.1.1 fallback", version: 4},
		{name: "MQTT 5 fallback", version: byte(mqtt.ProtocolV5)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			serverSocket, clientSocket := net.Pipe()
			connection := &connection{server: runtime.server, socket: serverSocket, controlQueue: make(chan controlMessage, 1), stop: make(chan struct{}), writerDone: make(chan struct{})}
			go connection.writerLoop()
			result := make(chan bool, 1)
			go func() {
				result <- connection.handleUnsupportedVersion(&mqtt.UnsupportedProtocolVersionError{ProtocolName: "MQTT", Level: testCase.version})
			}()
			_ = readTestFrame(t, clientSocket)
			if !<-result {
				t.Fatal("handleUnsupportedVersion() did not handle unsupported MQTT")
			}
			_ = clientSocket.Close()
			<-connection.writerDone
		})
	}
}

func TestConnectionLifecycleQueueAndDeliveryBranches(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)
	serverSocket, clientSocket := net.Pipe()
	connection := newConnection(runtime.server, serverSocket)
	defer clientSocket.Close()
	connection.controlQueue = make(chan controlMessage, 1)
	connection.writerDone = make(chan struct{})
	connection.controlQueue <- controlMessage{packet: &mqtt.PingResp{Version: mqtt.ProtocolV5}}
	if err := connection.enqueueAndWait(&mqtt.PingResp{Version: mqtt.ProtocolV5}); !errors.Is(err, ErrControlQueueFull) {
		t.Fatalf("full enqueueAndWait() error = %v", err)
	}
	connection.requestClose()
	if err := connection.enqueueAndWait(&mqtt.PingResp{Version: mqtt.ProtocolV5}); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("closed enqueueAndWait() error = %v", err)
	}
	connection.wakeDelivery()
	connection.deliveryWake <- struct{}{}
	connection.wakeDelivery()

	barrierSocket, barrierPeer := net.Pipe()
	barrier := newConnection(runtime.server, barrierSocket)
	barrierPeer.Close()
	go barrier.writerLoop()
	if err := barrier.enqueueBarrier(); err != nil {
		t.Fatalf("enqueueBarrier() error = %v", err)
	}
	barrier.requestClose()
	<-barrier.writerDone

	failingSocket, failingPeer := net.Pipe()
	failing := newConnection(runtime.server, failingSocket)
	failing.controlQueue <- controlMessage{done: make(chan error, 1)}
	failing.failQueuedControls()
	select {
	case err := <-failing.controlQueue:
		t.Fatalf("failQueuedControls left message %#v", err)
	default:
	}
	failingPeer.Close()
	failing.requestClose()

	runtime.server.deliveries.Register("delivery-session")
	deliverySocket, deliveryPeer := net.Pipe()
	delivery := newConnection(runtime.server, deliverySocket)
	delivery.sessionID = "delivery-session"
	delivery.version = mqtt.ProtocolV5
	if err := runtime.server.deliveries.TryEnqueue(applicationDelivery("delivery-session", mqtt.QoSAtMostOnce), time.Time{}); err != nil {
		t.Fatal(err)
	}
	deliveryResult := make(chan error, 1)
	go func() { deliveryResult <- delivery.writeDeliveries() }()
	_ = readTestFrame(t, deliveryPeer)
	if err := <-deliveryResult; err != nil {
		t.Fatalf("writeDeliveries() error = %v", err)
	}
	deliveryPeer.Close()
	delivery.requestClose()

	qosSocket, qosPeer := net.Pipe()
	qos := newConnection(runtime.server, qosSocket)
	qos.sessionID = "qos-session"
	qos.version = mqtt.ProtocolV5
	runtime.server.deliveries.Register(qos.sessionID)
	if err := runtime.server.deliveries.TryEnqueue(applicationDelivery(qos.sessionID, mqtt.QoSAtLeastOnce), time.Time{}); err != nil {
		t.Fatal(err)
	}
	deliveryResult = make(chan error, 1)
	go func() { deliveryResult <- qos.writeDeliveries() }()
	frame := readTestFrame(t, qosPeer)
	if len(frame) == 0 {
		t.Fatal("expected delivery frame")
	}
	if err := <-deliveryResult; err != nil {
		t.Fatalf("writeDeliveries() QoS 1 error = %v", err)
	}
	if err := runtime.server.deliveries.TryEnqueue(applicationDelivery(qos.sessionID, mqtt.QoS(3)), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := qos.writeDeliveries(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("writeDeliveries() invalid QoS error = %v", err)
	}
	qosPeer.Close()
	qos.requestClose()
}
func applicationDelivery(session routing.SessionID, qos mqtt.QoS) applicationdeliveryqueue.DeliveryDTO {
	return applicationdeliveryqueue.DeliveryDTO{SessionID: session, Topic: "topic", Payload: []byte("payload"), QoS: qos}
}
func TestEnqueueAndWaitReturnsConnectionClosedWhenStoppedWhileWaiting(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()
	connection := newConnection(runtime.server, serverSocket)
	result := make(chan error, 1)
	go func() {
		result <- connection.enqueueAndWait(&mqtt.PingResp{Version: mqtt.ProtocolV5})
	}()

	select {
	case <-connection.controlQueue:
	case <-time.After(time.Second):
		t.Fatal("control packet was not enqueued")
	}

	connection.requestClose()

	select {
	case err := <-result:
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("enqueueAndWait() error = %v, want %v", err, ErrConnectionClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("enqueueAndWait() did not unblock after connection close")
	}
}

type failingConnectSessionRepository struct {
	err error
}

func (repository failingConnectSessionRepository) Load(string) (session.Lifecycle, bool, error) {
	return session.Lifecycle{}, false, repository.err
}

func (repository failingConnectSessionRepository) Save(session.Lifecycle) error {
	return repository.err
}

func (repository failingConnectSessionRepository) Delete(string) error {
	return repository.err
}

func TestReadLoopReturnsSessionActivationError(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	wantErr := errors.New("session repository unavailable")
	connections, err := connect.New(
		&adapterTestClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)},
		failingConnectSessionRepository{err: wantErr},
		runtime.server.deliveries,
		routing.NewTable(),
		adapterTestScheduler{},
		runtime.server,
	)
	if err != nil {
		t.Fatalf("connect.New() error = %v", err)
	}
	runtime.server.connections = connections

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()
	connection := newConnection(runtime.server, serverSocket)
	encoded := encodeTestPacket(t, &mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		ClientID:   "activation-error",
		CleanStart: true,
	})
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := clientSocket.Write(encoded)
		writeDone <- writeErr
	}()

	if err := connection.readLoop(); !errors.Is(err, wantErr) {
		t.Fatalf("readLoop() error = %v, want %v", err, wantErr)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("client write error = %v", err)
	}
}

type deadlineErrorConnection struct {
	net.Conn
	err error
}

func (connection deadlineErrorConnection) SetReadDeadline(time.Time) error {
	return connection.err
}

func TestReadLoopReturnsReadDeadlineError(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()
	wantErr := errors.New("read deadline unavailable")
	connection := newConnection(runtime.server, deadlineErrorConnection{Conn: serverSocket, err: wantErr})
	if err := connection.readLoop(); !errors.Is(err, wantErr) {
		t.Fatalf("readLoop() error = %v, want %v", err, wantErr)
	}
}

type mockTLSConn struct {
	net.Conn
	state tls.ConnectionState
}

func (m mockTLSConn) ConnectionState() tls.ConnectionState {
	return m.state
}

func TestServerAddListenerAndMultiListenerServe(t *testing.T) {
	var nilServer *Server
	if err := nilServer.AddListener(nil); !errors.Is(err, ErrNilListener) {
		t.Fatalf("nilServer.AddListener(nil) = %v, want %v", err, ErrNilListener)
	}

	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())

	if err := runtime.server.AddListener(nil); !errors.Is(err, ErrNilListener) {
		t.Fatalf("AddListener(nil) = %v, want %v", err, ErrNilListener)
	}

	secondListener := newPipeListener()
	if err := runtime.server.AddListener(secondListener); err != nil {
		t.Fatalf("AddListener(secondListener) = %v", err)
	}

	// Connect client to second listener
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	secondListener.connections <- serverConn
	select {
	case <-secondListener.accepted:
	case <-time.After(time.Second):
		t.Fatal("secondListener connection not accepted")
	}

	connectPacket := &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "multi-listener-client"}
	writeTestPackets(t, clientConn, connectPacket)
	frame := readTestFrame(t, clientConn)
	if len(frame) < 2 || frame[0] != 0x20 {
		t.Fatalf("expected CONNACK frame, got %v", frame)
	}

	// AddListener after stop returns ErrConnectionClosed
	runtime.stop(t)
	stoppedListener := newPipeListener()
	if err := runtime.server.AddListener(stoppedListener); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("AddListener on stopped server = %v, want %v", err, ErrConnectionClosed)
	}
	select {
	case <-stoppedListener.closed:
	default:
		t.Fatal("stoppedListener was not closed")
	}
}

func TestServerVerifyClientID(t *testing.T) {
	cfg := defaultAdapterConfig()
	cfg.VerifyClientID = true
	runtime := newAdapterTestRuntime(t, 2, cfg)
	defer runtime.stop(t)

	peerCert := &x509.Certificate{
		DNSNames:       []string{"client-dns.example.com"},
		EmailAddresses: []string{"client@example.com"},
	}
	peerCert.Subject.CommonName = "client-cn"
	tlsState := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{peerCert},
	}

	testCases := []struct {
		name       string
		version    mqtt.ProtocolVersion
		clientID   string
		wantAccept bool
		wantReason byte
	}{
		{name: "v5-match-cn", version: mqtt.ProtocolV5, clientID: "client-cn", wantAccept: true},
		{name: "v5-match-dns", version: mqtt.ProtocolV5, clientID: "client-dns.example.com", wantAccept: true},
		{name: "v5-match-email", version: mqtt.ProtocolV5, clientID: "client@example.com", wantAccept: true},
		{name: "v5-mismatch", version: mqtt.ProtocolV5, clientID: "wrong-client", wantAccept: false, wantReason: 0x87},
		{name: "v311-match-cn", version: mqtt.ProtocolV311, clientID: "client-cn", wantAccept: true},
		{name: "v311-mismatch", version: mqtt.ProtocolV311, clientID: "wrong-client", wantAccept: false, wantReason: 0x05},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			serverPipe, clientPipe := net.Pipe()
			defer clientPipe.Close()
			mockConn := mockTLSConn{Conn: serverPipe, state: tlsState}
			runtime.listener.connections <- mockConn
			select {
			case <-runtime.listener.accepted:
			case <-time.After(time.Second):
				t.Fatal("connection not accepted")
			}
			connectPacket := &mqtt.Connect{
				Version:    tc.version,
				CleanStart: true,
				ClientID:   tc.clientID,
			}
			writeTestPackets(t, clientPipe, connectPacket)
			frame := readTestFrame(t, clientPipe)
			if len(frame) < 3 || frame[0] != 0x20 {
				t.Fatalf("expected CONNACK, got %v", frame)
			}
			reason := frame[3]
			if tc.wantAccept {
				if reason != 0 {
					t.Fatalf("expected reason 0, got 0x%02X", reason)
				}
			} else {
				if reason != tc.wantReason {
					t.Fatalf("expected reason 0x%02X, got 0x%02X", tc.wantReason, reason)
				}
			}
		})
	}
}

func TestServerVerifyClientIDCustomVerifier(t *testing.T) {
	customCalled := false
	cfg := defaultAdapterConfig()
	cfg.VerifyClientID = true
	cfg.ClientIdentityVerifier = func(conn net.Conn, clientID string) bool {
		customCalled = true
		return clientID == "custom-allowed"
	}
	runtime := newAdapterTestRuntime(t, 2, cfg)
	defer runtime.stop(t)

	serverPipe, clientPipe := net.Pipe()
	runtime.listener.connections <- serverPipe
	select {
	case <-runtime.listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("connection not accepted")
	}
	connectPacket := &mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		CleanStart: true,
		ClientID:   "custom-allowed",
	}
	writeTestPackets(t, clientPipe, connectPacket)
	frame := readTestFrame(t, clientPipe)
	if len(frame) < 4 || frame[0] != 0x20 || frame[3] != 0 {
		t.Fatalf("expected accepted CONNACK, got %v", frame)
	}
	if !customCalled {
		t.Fatal("custom verifier was not called")
	}
}
