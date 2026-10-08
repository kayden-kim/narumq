package tcp

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
)

// Traceability: PRD MQTT-11/MQTT-12/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.1.
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
		{name: "remaining length above protocol maximum", make: func() (*Server, error) {
			config := valid.config
			config.MaxPacketSize = 1 << 28
			return NewServer(runtime.listener, config, valid.decoder, valid.encoder, valid.connections, valid.subscriptions, valid.publications, valid.deliveries, nil)
		}, want: ErrInvalidConfig},
		{name: "packet buffer integer overflow", make: func() (*Server, error) {
			config := valid.config
			config.MaxPacketSize = int(^uint(0) >> 1)
			return NewServer(runtime.listener, config, valid.decoder, valid.encoder, valid.connections, valid.subscriptions, valid.publications, valid.deliveries, nil)
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

func TestCloseSessionForTakeoverRemovesBindingAndStopsSocket(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)
	serverSocket, clientSocket := net.Pipe()
	defer clientSocket.Close()
	connection := newConnection(runtime.server, serverSocket)
	connection.sessionID = "session"
	connection.disconnectReason = session.DisconnectWithWill
	if !runtime.server.bindSession(connection.sessionID, connection) {
		t.Fatal("bindSession() failed")
	}

	if !runtime.server.CloseSessionForTakeover(connection.sessionID) {
		t.Fatal("CloseSessionForTakeover() did not find the active session")
	}
	if runtime.server.bySession[connection.sessionID] != nil {
		t.Fatal("takeover left the old connection bound to the session")
	}
	if !connection.takeover.Load() {
		t.Fatal("takeover close did not mark the connection as non-abnormal")
	}
	select {
	case <-connection.stop:
	default:
		t.Fatal("takeover did not stop the old connection")
	}
	if runtime.server.CloseSessionForTakeover(connection.sessionID) {
		t.Fatal("second takeover close unexpectedly found the removed binding")
	}
}

func TestPendingOwnershipFenceIsTenantScoped(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	const clientID = "shared-client-id"
	socketA, peerA := net.Pipe()
	defer peerA.Close()
	connectionA := newConnection(runtime.server, socketA)
	defer connectionA.requestClose()
	connectionA.ownership = subscribe.SessionOwnership{
		TenantID: "tenant-a", ClientID: clientID, OwnerNodeID: 1, Epoch: 1,
	}
	runtime.server.trackConnecting(clientID, connectionA)

	socketB, peerB := net.Pipe()
	defer peerB.Close()
	connectionB := newConnection(runtime.server, socketB)
	defer connectionB.requestClose()
	connectionB.ownership = subscribe.SessionOwnership{
		TenantID: "tenant-b", ClientID: clientID, OwnerNodeID: 1, Epoch: 1,
	}
	runtime.server.trackConnecting(clientID, connectionB)

	runtime.server.RecordCommittedOwnership(membership.OwnershipStateDTO{
		TenantID: "tenant-b", ClientID: clientID, OwnerNodeID: 1, Epoch: 1,
	})
	runtime.server.RecordCommittedOwnership(membership.OwnershipStateDTO{
		TenantID: "tenant-a", ClientID: clientID, OwnerNodeID: 2, Epoch: 2,
	})

	if runtime.server.bindSession("tenant-a/session", connectionA) || !connectionA.takeover.Load() {
		t.Fatal("tenant A stale connection bound despite its newer ownership fence")
	}
	if !runtime.server.bindSession("tenant-b/session", connectionB) {
		t.Fatal("tenant A ownership fence rejected tenant B's matching epoch")
	}
}

// Traceability: CLU-07; docs/specs/cluster-session-and-delivery.md §4.2.
func TestCommittedOwnershipFenceDoesNotRegressToStaleEpoch(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	const clientID = "stale-owner"
	socket, peer := net.Pipe()
	defer peer.Close()
	connection := newConnection(runtime.server, socket)
	defer connection.requestClose()
	connection.ownership = subscribe.SessionOwnership{
		TenantID: "tenant-a", ClientID: clientID, OwnerNodeID: 1, Epoch: 3,
	}
	runtime.server.trackConnecting(clientID, connection)
	runtime.server.RecordCommittedOwnership(membership.OwnershipStateDTO{
		TenantID: "tenant-a", ClientID: clientID, OwnerNodeID: 2, Epoch: 4,
	})
	runtime.server.RecordCommittedOwnership(membership.OwnershipStateDTO{
		TenantID: "tenant-a", ClientID: clientID, OwnerNodeID: 1, Epoch: 3,
	})

	if runtime.server.bindSession("tenant-a/session", connection) || !connection.takeover.Load() {
		t.Fatal("stale ownership commit allowed the older connection to bind")
	}
}

// Traceability: CLU-07; docs/specs/cluster-session-and-delivery.md §4.2.
func TestStoppedConnectionCannotBindSession(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer runtime.stop(t)

	socket, peer := net.Pipe()
	defer peer.Close()
	connection := newConnection(runtime.server, socket)
	runtime.server.trackConnecting("stopped-client", connection)
	connection.requestClose()

	if runtime.server.bindSession("tenant-a/stopped-client", connection) {
		t.Fatal("stopped connection bound to a session")
	}
	if connection.connectingTracked {
		t.Fatal("stopped connection remained in the pending CONNECT registry")
	}
}

func TestCommittedTakeoverFencesConnectBeforeSessionBinding(t *testing.T) {
	const clientID = "in-flight-client"
	clusterConfig := func(memberID uint64) Config {
		config := defaultAdapterConfig()
		config.ClusterMode = true
		config.LocalMemberID = memberID
		config.ClusterReady = func() bool { return true }
		config.ClusterAdmissionOpen = func() bool { return true }
		config.ClusterProjectionReady = func() bool { return true }
		return config
	}

	staleRuntime := newAdapterTestRuntime(t, 2, clusterConfig(1))
	defer staleRuntime.stop(t)

	started := make(chan struct{})
	release := make(chan struct{})
	staleRuntime.cluster.mu.Lock()
	staleRuntime.cluster.ownershipEpoch = 1
	staleRuntime.cluster.ownershipCommitted = func(tenantID, committedClientID string, ownerNodeID, epoch uint64) {
		staleRuntime.server.RecordCommittedOwnership(membership.OwnershipStateDTO{
			TenantID: tenantID, ClientID: committedClientID, OwnerNodeID: ownerNodeID, Epoch: epoch,
		})
	}
	staleRuntime.cluster.ownershipCommitStarted = started
	staleRuntime.cluster.ownershipCommitRelease = release
	staleRuntime.cluster.mu.Unlock()

	writeTestPackets(t, staleRuntime.client, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, ClientID: clientID,
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first CONNECT did not reach its committed ownership pause")
	}

	staleRuntime.server.mu.Lock()
	pending := staleRuntime.server.connectingByClientID[clientID]
	pendingCount := 0
	if pending != nil {
		pendingCount = len(pending.connections)
	}
	staleRuntime.server.mu.Unlock()
	if pendingCount != 1 {
		t.Fatalf("in-flight CONNECT registrations = %d, want 1", pendingCount)
	}

	staleRuntime.server.RecordCommittedOwnership(membership.OwnershipStateDTO{
		TenantID: "default", ClientID: clientID, OwnerNodeID: 2, Epoch: 2,
	})
	close(release)

	if err := staleRuntime.client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var response [1]byte
	count, err := staleRuntime.client.Read(response[:])
	if count != 0 {
		t.Fatalf("stale CONNECT received CONNACK bytes % X", response[:count])
	}
	if err == nil {
		t.Fatal("stale CONNECT socket remained open without CONNACK")
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		t.Fatalf("stale CONNECT did not close after takeover: %v", err)
	}
	if _, found, err := staleRuntime.repo.Load(clientID); err != nil || found {
		t.Fatalf("rejected stale CONNECT local session = found %v, err %v; want cleanup", found, err)
	}

	currentRuntime := newAdapterTestRuntime(t, 2, clusterConfig(2))
	defer currentRuntime.stop(t)

	currentRuntime.cluster.mu.Lock()
	currentRuntime.cluster.ownershipEpoch = 2
	currentRuntime.cluster.ownershipCommitted = func(tenantID, committedClientID string, ownerNodeID, epoch uint64) {
		currentRuntime.server.RecordCommittedOwnership(membership.OwnershipStateDTO{
			TenantID: tenantID, ClientID: committedClientID, OwnerNodeID: ownerNodeID, Epoch: epoch,
		})
	}
	currentRuntime.cluster.mu.Unlock()
	writeTestPackets(t, currentRuntime.client, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, ClientID: clientID,
	})
	connAck := readTestFrame(t, currentRuntime.client)
	if len(connAck) < 4 || connAck[0] != 0x20 || connAck[3] != 0 {
		t.Fatalf("current owner CONNACK = % X, want success", connAck)
	}

	staleRuntime.server.mu.Lock()
	staleBound := false
	for _, connection := range staleRuntime.server.bySession {
		staleBound = staleBound || connection.ownership.ClientID == clientID
	}
	staleRuntime.server.mu.Unlock()
	currentRuntime.server.mu.Lock()
	currentBound := false
	for _, connection := range currentRuntime.server.bySession {
		currentBound = currentBound || (connection.ownership.ClientID == clientID &&
			connection.ownership.OwnerNodeID == 2 && connection.ownership.Epoch == 2)
	}
	currentRuntime.server.mu.Unlock()
	if staleBound || !currentBound {
		t.Fatalf("session owners after takeover: stale bound=%t, current epoch-2 bound=%t", staleBound, currentBound)
	}
}

func TestOldConnectionCleanupCannotDisconnectReplacementEpoch(t *testing.T) {
	const clientID = "replacement-client"
	config := defaultAdapterConfig()
	config.ClusterMode = true
	config.LocalMemberID = 1
	config.ClusterReady = func() bool { return true }
	config.ClusterAdmissionOpen = func() bool { return true }
	config.ClusterProjectionReady = func() bool { return true }
	runtime := newAdapterTestRuntime(t, 2, config)
	defer runtime.stop(t)

	recordOwnership := func(tenantID, committedClientID string, ownerNodeID, epoch uint64) {
		runtime.server.RecordCommittedOwnership(membership.OwnershipStateDTO{
			TenantID: tenantID, ClientID: committedClientID, OwnerNodeID: ownerNodeID, Epoch: epoch,
		})
	}
	runtime.cluster.mu.Lock()
	runtime.cluster.ownershipEpoch = 1
	runtime.cluster.ownershipCommitted = recordOwnership
	runtime.cluster.mu.Unlock()

	writeTestPackets(t, runtime.client, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, ClientID: clientID,
	})
	connAck := readTestFrame(t, runtime.client)
	if len(connAck) < 4 || connAck[0] != 0x20 || connAck[3] != 0 {
		t.Fatalf("initial owner CONNACK = % X, want success", connAck)
	}

	runtime.server.mu.Lock()
	var old *connection
	for _, candidate := range runtime.server.bySession {
		if candidate.ownership.ClientID == clientID {
			old = candidate
			break
		}
	}
	runtime.server.mu.Unlock()
	if old == nil {
		t.Fatal("initial connection was not bound")
	}

	old.deliveryMu.Lock()
	cleanupGateHeld := true
	defer func() {
		if cleanupGateHeld {
			old.deliveryMu.Unlock()
		}
	}()
	if !runtime.server.CloseSessionForTakeover(old.sessionID) {
		t.Fatal("CloseSessionForTakeover() did not close the old connection")
	}
	if err := runtime.server.connections.Disconnect(old.sessionID, session.DisconnectNormal); err != nil {
		t.Fatalf("detach old local lifecycle: %v", err)
	}

	runtime.cluster.mu.Lock()
	runtime.cluster.ownershipEpoch = 2
	runtime.cluster.mu.Unlock()
	serverSocket, replacementClient := net.Pipe()
	defer replacementClient.Close()
	defer serverSocket.Close()
	runtime.listener.connections <- serverSocket
	select {
	case <-runtime.listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("replacement TCP connection was not accepted")
	}
	writeTestPackets(t, replacementClient, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, ClientID: clientID,
	})
	connAck = readTestFrame(t, replacementClient)
	if len(connAck) < 4 || connAck[0] != 0x20 || connAck[3] != 0 {
		t.Fatalf("replacement owner CONNACK = % X, want success", connAck)
	}

	runtime.server.mu.Lock()
	replacement := runtime.server.bySession[old.sessionID]
	runtime.server.mu.Unlock()
	if replacement == nil || replacement == old || replacement.ownership.Epoch != 2 {
		t.Fatalf("bound replacement = %#v, want a distinct epoch-2 connection", replacement)
	}

	old.deliveryMu.Unlock()
	cleanupGateHeld = false
	old.cleanup()
	result, err := runtime.server.connections.ConnectContext(context.Background(), connect.ConnectRequestDTO{
		Packet: mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: clientID},
	})
	if err != nil || result.Accepted {
		t.Fatalf("duplicate CONNECT after old cleanup = (%#v, %v), want replacement still connected", result, err)
	}
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

// Traceability: PRD MQTT-12; docs/specs/mqtt-protocol-and-session.md §7.1.
func TestNewServerRejectsIncompleteClusterBoundary(t *testing.T) {
	clusterConfig := defaultAdapterConfig()
	clusterConfig.ClusterMode = true
	clusterConfig.LocalMemberID = 1
	clusterConfig.ClusterReady = func() bool { return true }
	clusterConfig.ClusterAdmissionOpen = func() bool { return true }
	clusterConfig.ClusterProjectionReady = func() bool { return true }
	clusterRuntime := newAdapterTestRuntime(t, 2, clusterConfig)
	defer clusterRuntime.stop(t)
	standaloneRuntime := newAdapterTestRuntime(t, 2, defaultAdapterConfig())
	defer standaloneRuntime.stop(t)

	base := clusterRuntime.server
	tests := []struct {
		name          string
		config        Config
		connections   *connect.UseCase
		subscriptions *subscribe.UseCase
	}{
		{
			name: "missing local member",
			config: func() Config {
				config := base.config
				config.LocalMemberID = 0
				return config
			}(),
			connections: base.connections, subscriptions: base.subscriptions,
		},
		{
			name: "missing readiness callback",
			config: func() Config {
				config := base.config
				config.ClusterReady = nil
				return config
			}(),
			connections: base.connections, subscriptions: base.subscriptions,
		},
		{
			name: "missing admission callback",
			config: func() Config {
				config := base.config
				config.ClusterAdmissionOpen = nil
				return config
			}(),
			connections: base.connections, subscriptions: base.subscriptions,
		},
		{
			name: "missing projection callback",
			config: func() Config {
				config := base.config
				config.ClusterProjectionReady = nil
				return config
			}(),
			connections: base.connections, subscriptions: base.subscriptions,
		},
		{
			name:          "missing ownership coordinator",
			config:        base.config,
			connections:   standaloneRuntime.server.connections,
			subscriptions: base.subscriptions,
		},
		{
			name:          "missing subscription committer",
			config:        base.config,
			connections:   base.connections,
			subscriptions: standaloneRuntime.server.subscriptions,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			created, err := NewServer(
				base.listeners[0], test.config, base.decoder, base.encoder,
				test.connections, test.subscriptions, base.publications, base.deliveries, nil,
			)
			if created != nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewServer(cluster boundary) = (%#v, %v), want nil and %v", created, err, ErrInvalidConfig)
			}
		})
	}
}
