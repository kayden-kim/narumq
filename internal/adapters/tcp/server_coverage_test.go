package tcp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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
	"narumq/internal/domain/security"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type csvToggleWriteConn struct {
	net.Conn
	fail atomic.Bool
	err  error
}

func (connection *csvToggleWriteConn) Write(data []byte) (int, error) {
	if connection.fail.Load() {
		return 0, connection.err
	}
	return connection.Conn.Write(data)
}

// csvQueue decorates the memory delivery queue with injectable persistence failures.
type csvQueue struct {
	*deliveryqueue.Queue

	mu                  sync.Mutex
	listOutbound        func(routing.SessionID) ([]applicationdeliveryqueue.OutboundDTO, error)
	peek                func(routing.SessionID, time.Time) (applicationdeliveryqueue.DeliveryDTO, bool, error)
	completeOutboundErr error
	replaceOutboundErr  error
}

func (queue *csvQueue) setListOutbound(hook func(routing.SessionID) ([]applicationdeliveryqueue.OutboundDTO, error)) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.listOutbound = hook
}

func (queue *csvQueue) setPeek(hook func(routing.SessionID, time.Time) (applicationdeliveryqueue.DeliveryDTO, bool, error)) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.peek = hook
}

func (queue *csvQueue) setOutboundErrors(completeErr, replaceErr error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.completeOutboundErr = completeErr
	queue.replaceOutboundErr = replaceErr
}

func (queue *csvQueue) ListQueued(sessionID routing.SessionID, now time.Time) ([]applicationdeliveryqueue.DeliveryDTO, error) {
	return queue.Queue.ListQueued(sessionID, now)
}

func (queue *csvQueue) ListOutbound(sessionID routing.SessionID) ([]applicationdeliveryqueue.OutboundDTO, error) {
	queue.mu.Lock()
	hook := queue.listOutbound
	queue.mu.Unlock()
	if hook != nil {
		return hook(sessionID)
	}
	return queue.Queue.ListOutbound(sessionID)
}

func (queue *csvQueue) PeekBatch(sessionID routing.SessionID, now time.Time, destination []applicationdeliveryqueue.DeliveryDTO) (int, error) {
	queue.mu.Lock()
	hook := queue.peek
	queue.mu.Unlock()
	if hook != nil {
		if len(destination) == 0 {
			return 0, nil
		}
		item, ok, err := hook(sessionID, now)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, nil
		}
		destination[0] = item
		return 1, nil
	}
	return queue.Queue.PeekBatch(sessionID, now, destination)
}

func (queue *csvQueue) Peek(sessionID routing.SessionID, now time.Time) (applicationdeliveryqueue.DeliveryDTO, bool, error) {
	queue.mu.Lock()
	hook := queue.peek
	queue.mu.Unlock()
	if hook != nil {
		return hook(sessionID, now)
	}
	return queue.Queue.Peek(sessionID, now)
}

func (queue *csvQueue) CompleteOutbound(sessionID routing.SessionID, packetID mqtt.PacketIdentifier) error {
	queue.mu.Lock()
	err := queue.completeOutboundErr
	queue.mu.Unlock()
	if err != nil {
		return err
	}
	return queue.Queue.CompleteOutbound(sessionID, packetID)
}

func (queue *csvQueue) ReplaceOutbound(outbound applicationdeliveryqueue.OutboundDTO) error {
	queue.mu.Lock()
	err := queue.replaceOutboundErr
	queue.mu.Unlock()
	if err != nil {
		return err
	}
	return queue.Queue.ReplaceOutbound(outbound)
}

func (queue *csvQueue) BeginReplaceOutbound(outbound applicationdeliveryqueue.OutboundDTO) (applicationdeliveryqueue.PendingTransition, error) {
	if err := queue.ReplaceOutbound(outbound); err != nil {
		return nil, err
	}
	return applicationdeliveryqueue.CompletedTransition{}, nil
}

func (queue *csvQueue) BeginCompleteOutbound(outbound applicationdeliveryqueue.OutboundDTO) (applicationdeliveryqueue.PendingTransition, error) {
	if err := queue.CompleteOutbound(outbound.SessionID, outbound.PacketID); err != nil {
		return nil, err
	}
	return applicationdeliveryqueue.CompletedTransition{}, nil
}
func (queue *csvQueue) ResetDurable(
	sessionID routing.SessionID,
	epoch uint64,
	queued []applicationdeliveryqueue.DeliveryDTO,
	outbound []applicationdeliveryqueue.OutboundDTO,
	now time.Time,
) error {
	return queue.Queue.ResetDurable(sessionID, epoch, queued, outbound, now)
}

// csvLogBuffer is a goroutine-safe slog sink.
type csvLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (logs *csvLogBuffer) Write(value []byte) (int, error) {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	return logs.buffer.Write(value)
}

func (logs *csvLogBuffer) String() string {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	return logs.buffer.String()
}

// csvToggleSessions fails every repository operation once armed.
type csvToggleSessions struct {
	inner *sessionstore.MemorySessionRepository
	fail  atomic.Bool
	err   error
}

func (sessions *csvToggleSessions) Load(clientID string) (session.Lifecycle, bool, error) {
	if sessions.fail.Load() {
		return session.Lifecycle{}, false, sessions.err
	}
	return sessions.inner.Load(clientID)
}

func (sessions *csvToggleSessions) Save(state session.Lifecycle) error {
	if sessions.fail.Load() {
		return sessions.err
	}
	return sessions.inner.Save(state)
}

func (sessions *csvToggleSessions) Delete(clientID string) error {
	if sessions.fail.Load() {
		return sessions.err
	}
	return sessions.inner.Delete(clientID)
}

type csvServerOptions struct {
	queueCapacity int
	decoderLimit  int
	sessions      connect.SessionRepository
	config        *Config
}

type csvDeps struct {
	server   *Server
	queue    *csvQueue
	memory   *deliveryqueue.Queue
	sessions connect.SessionRepository
	logs     *csvLogBuffer
	clock    *adapterTestClock
}

// csvNewServer builds a non-serving Server whose delivery port is a csvQueue.
func csvNewServer(t *testing.T, listener net.Listener, options csvServerOptions) *csvDeps {
	t.Helper()
	config := defaultAdapterConfig()
	if options.config != nil {
		config = *options.config
	}
	if options.queueCapacity == 0 {
		options.queueCapacity = 4
	}
	if options.decoderLimit == 0 {
		options.decoderLimit = config.MaxPacketSize
	}
	if options.sessions == nil {
		options.sessions = sessionstore.New()
	}
	memory, err := deliveryqueue.New(options.queueCapacity)
	if err != nil {
		t.Fatal(err)
	}
	queue := &csvQueue{Queue: memory}
	retainedRepository, err := memoryretained.New(32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	clock := &adapterTestClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
	config.Clock = clock
	routes := routing.NewTable()
	willBridge := NewWillBridge()
	connections, err := connect.New(clock, options.sessions, memory, routes, adapterTestScheduler{}, willBridge)
	if err != nil {
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes, memory, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	publications, err := publish.New(clock, routes, memory, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(options.decoderLimit)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(config.MaxPacketSize)
	if err != nil {
		t.Fatal(err)
	}
	logs := &csvLogBuffer{}
	server, err := NewServer(listener, config, decoder, encoder, connections, subscriptions, publications, queue, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	willBridge.Bind(server)
	t.Cleanup(server.stopAll)
	return &csvDeps{server: server, queue: queue, memory: memory, sessions: options.sessions, logs: logs, clock: clock}
}

type csvServing struct {
	*csvDeps
	listener *pipeListener
}

// csvServe starts Serve on a pipe listener and joins it during test cleanup.
func csvServe(t *testing.T, options csvServerOptions) *csvServing {
	t.Helper()
	listener := newPipeListener()
	deps := csvNewServer(t, listener, options)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- deps.server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve() error = %v, want nil", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Serve() did not join after cancellation")
		}
	})
	return &csvServing{csvDeps: deps, listener: listener}
}

func (serving *csvServing) dial(t *testing.T) net.Conn {
	t.Helper()
	client, serverSide := net.Pipe()
	serving.listener.connections <- serverSide
	select {
	case <-serving.listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("server did not accept pipe connection")
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func csvReadPacket(t *testing.T, server *Server, conn net.Conn, version mqtt.ProtocolVersion) mqtt.Packet {
	t.Helper()
	frame := readTestFrame(t, conn)
	packet, consumed, err := server.limits.Load().decoder.Decode(frame, version)
	if err != nil || consumed != len(frame) {
		t.Fatalf("decode frame % X: packet=%T consumed=%d err=%v", frame, packet, consumed, err)
	}
	return packet
}

func csvConnect(t *testing.T, server *Server, conn net.Conn, packet *mqtt.Connect) {
	t.Helper()
	writeTestPackets(t, conn, packet)
	connAck, ok := csvReadPacket(t, server, conn, packet.Version).(*mqtt.ConnAck)
	if !ok || connAck.ReasonCode != 0 {
		t.Fatalf("CONNACK = %#v, want success", connAck)
	}
}

func csvSubscribe(t *testing.T, server *Server, conn net.Conn, packetID mqtt.PacketIdentifier, filter string) {
	t.Helper()
	writeTestPackets(t, conn, &mqtt.Subscribe{
		Version: mqtt.ProtocolV5, PacketID: packetID,
		Subscriptions: []mqtt.Subscription{{Filter: filter, QoS: mqtt.QoSAtMostOnce}},
	})
	subAck, ok := csvReadPacket(t, server, conn, mqtt.ProtocolV5).(*mqtt.SubAck)
	if !ok || subAck.PacketID != packetID || len(subAck.ReasonCodes) != 1 || subAck.ReasonCodes[0] != 0 {
		t.Fatalf("SUBACK = %#v, want granted QoS 0", subAck)
	}
}

func csvWaitActiveConnections(t *testing.T, server *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		server.mu.Lock()
		active := len(server.active)
		server.mu.Unlock()
		if active == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("active connections = %d, want %d", active, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func csvRequireClosedWithoutResponse(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		if errors.Is(err, io.ErrClosedPipe) {
			return
		}
		t.Fatal(err)
	}
	var response [64]byte
	count, err := conn.Read(response[:])
	if count > 0 {
		t.Fatalf("connection emitted % X before closing", response[:count])
	}
	var networkError net.Error
	if err == nil || (errors.As(err, &networkError) && networkError.Timeout()) {
		t.Fatalf("connection stayed open: %v", err)
	}
}

func csvRequireStopped(t *testing.T, conn *connection, want bool) {
	t.Helper()
	if got := conn.isStopping(); got != want {
		t.Fatalf("connection stopping = %v, want %v", got, want)
	}
}

func csvTakeControl(t *testing.T, conn *connection) mqtt.Packet {
	t.Helper()
	select {
	case message := <-conn.controlQueue:
		return message.packet
	default:
		t.Fatal("control queue is empty")
		return nil
	}
}

func csvRequireNoControl(t *testing.T, conn *connection) {
	t.Helper()
	select {
	case message := <-conn.controlQueue:
		t.Fatalf("unexpected control packet %#v", message.packet)
	default:
	}
}

// csvClusterRuntime returns a cluster-mode runtime whose readiness follows ready.
func csvClusterRuntime(t *testing.T, ready *atomic.Bool) *adapterTestRuntime {
	t.Helper()
	config := defaultAdapterConfig()
	config.ClusterMode = true
	config.LocalMemberID = 4
	config.ClusterReady = ready.Load
	config.ClusterAdmissionOpen = func() bool { return true }
	config.ClusterProjectionReady = ready.Load
	return newAdapterTestRuntime(t, 2, config)
}

func TestCSVNewServerRejectsIncompleteClusterWiring(t *testing.T) {
	queue, err := deliveryqueue.New(2)
	if err != nil {
		t.Fatal(err)
	}
	retainedRepository, err := memoryretained.New(4, 64)
	if err != nil {
		t.Fatal(err)
	}
	clock := &adapterTestClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
	routes := routing.NewTable()
	cluster := &adapterTestClusterState{}
	newConnections := func(options ...connect.Option) *connect.UseCase {
		useCase, err := connect.New(clock, sessionstore.New(), queue, routes, adapterTestScheduler{}, NewWillBridge(), options...)
		if err != nil {
			t.Fatal(err)
		}
		return useCase
	}
	newSubscriptions := func(options ...subscribe.Option) *subscribe.UseCase {
		useCase, err := subscribe.New(routes, queue, retainedRepository, options...)
		if err != nil {
			t.Fatal(err)
		}
		return useCase
	}
	ownedConnections := newConnections(connect.WithOwnershipCoordinator(cluster, 4))
	committedSubscriptions := newSubscriptions(subscribe.WithCommitter(cluster))
	publications, err := publish.New(clock, routes, queue, retainedRepository)
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
	clusterConfig := func(memberID uint64, ready func() bool) Config {
		config := defaultAdapterConfig()
		config.Clock = clock
		config.ClusterMode = true
		config.LocalMemberID = memberID
		config.ClusterReady = ready
		config.ClusterAdmissionOpen = func() bool { return true }
		config.ClusterProjectionReady = func() bool { return true }
		return config
	}
	alwaysReady := func() bool { return true }
	for _, testCase := range []struct {
		name          string
		config        Config
		connections   *connect.UseCase
		subscriptions *subscribe.UseCase
		want          error
	}{
		{name: "missing member", config: clusterConfig(0, alwaysReady), connections: ownedConnections, subscriptions: committedSubscriptions, want: ErrInvalidConfig},
		{name: "missing readiness", config: clusterConfig(4, nil), connections: ownedConnections, subscriptions: committedSubscriptions, want: ErrInvalidConfig},
		{
			name: "missing admission hook",
			config: func() Config {
				config := clusterConfig(4, alwaysReady)
				config.ClusterAdmissionOpen = nil
				return config
			}(),
			connections: ownedConnections, subscriptions: committedSubscriptions, want: ErrInvalidConfig,
		},
		{
			name: "missing local projection readiness",
			config: func() Config {
				config := clusterConfig(4, alwaysReady)
				config.ClusterProjectionReady = nil
				return config
			}(),
			connections: ownedConnections, subscriptions: committedSubscriptions, want: ErrInvalidConfig,
		},
		{name: "ownership for another member", config: clusterConfig(5, alwaysReady), connections: ownedConnections, subscriptions: committedSubscriptions, want: ErrInvalidConfig},
		{name: "no ownership coordinator", config: clusterConfig(4, alwaysReady), connections: newConnections(), subscriptions: committedSubscriptions, want: ErrInvalidConfig},
		{name: "no subscription committer", config: clusterConfig(4, alwaysReady), connections: ownedConnections, subscriptions: newSubscriptions(), want: ErrInvalidConfig},
		{name: "complete wiring", config: clusterConfig(4, alwaysReady), connections: ownedConnections, subscriptions: committedSubscriptions},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server, err := NewServer(newPipeListener(), testCase.config, decoder, encoder, testCase.connections, testCase.subscriptions, publications, queue, nil)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("NewServer() error = %v, want %v", err, testCase.want)
			}
			if (server != nil) != (testCase.want == nil) {
				t.Fatalf("NewServer() server = %#v with error %v", server, err)
			}
		})
	}
}

// csvFailingListener fails its first Accept, then blocks until closed.
type csvFailingListener struct {
	err       error
	failed    atomic.Bool
	closed    chan struct{}
	closeOnce sync.Once
}

func (listener *csvFailingListener) Accept() (net.Conn, error) {
	if !listener.failed.Swap(true) {
		return nil, listener.err
	}
	<-listener.closed
	return nil, net.ErrClosed
}

func (listener *csvFailingListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}

func (listener *csvFailingListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
}

func TestCSVServeReturnsUnexpectedAcceptErrorAndStopsAllListeners(t *testing.T) {
	wantErr := errors.New("accept: too many open files")
	failing := &csvFailingListener{err: wantErr, closed: make(chan struct{})}
	deps := csvNewServer(t, failing, csvServerOptions{})
	healthy := newPipeListener()
	if err := deps.server.AddListener(healthy); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- deps.server.Serve(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Serve() error = %v, want %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() kept running after an unexpected accept error")
	}
	for name, closed := range map[string]chan struct{}{"failing": failing.closed, "healthy": healthy.closed} {
		select {
		case <-closed:
		default:
			t.Fatalf("%s listener left open after accept failure", name)
		}
	}
	if err := deps.server.AddListener(newPipeListener()); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("AddListener() after failed Serve = %v, want %v", err, ErrConnectionClosed)
	}
}

func TestCSVAddActiveAfterStopClosesLateConnection(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	deps.server.stopAll()
	serverSide, client := net.Pipe()
	defer client.Close()
	late := newConnection(deps.server, serverSide)
	deps.server.addActive(late)
	csvRequireStopped(t, late, true)
	deps.server.mu.Lock()
	_, tracked := deps.server.active[late]
	deps.server.mu.Unlock()
	if tracked {
		t.Fatal("stopped server tracked a late connection")
	}
	if _, err := serverSide.Write([]byte{1}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("late connection server socket write error = %v, want closed pipe", err)
	}
}

// csvObserveWill connects a watcher subscribed to the victim's Will topic,
// applies closeVictim, waits for the victim to be fully cleaned up, and
// returns the first payload the watcher receives after publishing a marker.
func csvObserveWill(t *testing.T, closeVictim func(t *testing.T, serving *csvServing, victim net.Conn)) string {
	t.Helper()
	serving := csvServe(t, csvServerOptions{})
	watcher := serving.dial(t)
	csvConnect(t, serving.server, watcher, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "watcher"})
	csvSubscribe(t, serving.server, watcher, 1, "will/topic")

	victim := serving.dial(t)
	csvConnect(t, serving.server, victim, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "victim",
		Will: &mqtt.Will{Topic: "will/topic", Payload: []byte("gone")},
	})
	closeVictim(t, serving, victim)
	csvWaitActiveConnections(t, serving.server, 1)

	writeTestPackets(t, watcher, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "will/topic", Payload: []byte("marker")})
	received, ok := csvReadPacket(t, serving.server, watcher, mqtt.ProtocolV5).(*mqtt.Publish)
	if !ok {
		t.Fatalf("watcher received %#v, want PUBLISH", received)
	}
	return string(received.Payload)
}

func TestCSVTakeoverCloseSuppressesWillButAbnormalCloseDoesNot(t *testing.T) {
	var missing *Server
	if missing.CloseSessionForTakeover("victim") {
		t.Fatal("nil server reported a takeover close")
	}
	t.Run("takeover", func(t *testing.T) {
		got := csvObserveWill(t, func(t *testing.T, serving *csvServing, victim net.Conn) {
			if !serving.server.CloseSessionForTakeover("victim") {
				t.Fatal("CloseSessionForTakeover() did not find the victim session")
			}
			requireConnectionClosed(t, victim)
		})
		if got != "marker" {
			t.Fatalf("takeover close published Will %q before the marker", got)
		}
	})
	t.Run("abnormal socket close", func(t *testing.T) {
		got := csvObserveWill(t, func(t *testing.T, _ *csvServing, victim net.Conn) {
			_ = victim.Close()
		})
		if got != "gone" {
			t.Fatalf("abnormal close delivered %q first, want Will payload", got)
		}
	})
}

func TestCSVDisconnectReasonCodeControlsWillPublication(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		reasonCode byte
		want       string
	}{
		{name: "normal", reasonCode: 0x00, want: "marker"},
		{name: "with will", reasonCode: 0x04, want: "gone"},
		{name: "unspecified error", reasonCode: 0x80, want: "gone"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got := csvObserveWill(t, func(t *testing.T, _ *csvServing, victim net.Conn) {
				writeTestPackets(t, victim, &mqtt.Disconnect{Version: mqtt.ProtocolV5, ReasonCode: testCase.reasonCode})
				requireConnectionClosed(t, victim)
			})
			if got != testCase.want {
				t.Fatalf("DISCONNECT 0x%02X delivered %q first, want %q", testCase.reasonCode, got, testCase.want)
			}
		})
	}
}

func TestCSVCleanupLogsSessionDisconnectFailure(t *testing.T) {
	sessions := &csvToggleSessions{inner: sessionstore.New(), err: errors.New("session store offline")}
	serving := csvServe(t, csvServerOptions{sessions: sessions})
	client := serving.dial(t)
	csvConnect(t, serving.server, client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "flaky"})
	if strings.Contains(serving.logs.String(), "MQTT session cleanup failed") {
		t.Fatalf("cleanup failure logged before disconnect: %s", serving.logs.String())
	}
	sessions.fail.Store(true)
	_ = client.Close()
	csvWaitActiveConnections(t, serving.server, 0)
	logs := serving.logs.String()
	if !strings.Contains(logs, "MQTT session cleanup failed") || !strings.Contains(logs, "session store offline") {
		t.Fatalf("cleanup logs = %q, want session cleanup failure with cause", logs)
	}
	serving.server.mu.Lock()
	_, bound := serving.server.bySession["flaky"]
	serving.server.mu.Unlock()
	if bound {
		t.Fatal("failed session cleanup left the connection bound")
	}
}

func TestCSVReadLoopEnforcesAdapterBoundWhenDecoderAllowsMore(t *testing.T) {
	config := defaultAdapterConfig()
	config.MaxPacketSize = 8
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{config: &config, decoderLimit: 1024})
	serverSide, client := net.Pipe()
	defer client.Close()
	conn := newConnection(deps.server, serverSide)
	go func() {
		// Declares a 100-byte CONNECT body; the adapter buffer holds only 13 bytes.
		_, _ = client.Write(append([]byte{0x10, 100}, make([]byte, 30)...))
	}()
	if err := conn.readLoop(); !errors.Is(err, mqtt.ErrPacketTooLarge) {
		t.Fatalf("readLoop() error = %v, want %v", err, mqtt.ErrPacketTooLarge)
	}
	_ = serverSide.Close()
}

func TestConnectionRunLogsPacketHandlingErrorsButNotPeerClose(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		closePeer  bool
		wantLogged bool
	}{
		{name: "malformed MQTT packet", wantLogged: true},
		{name: "peer closed", closePeer: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
			serverSide, clientSide := net.Pipe()
			connection := newConnection(deps.server, serverSide)
			done := make(chan struct{})
			go func() {
				connection.run()
				close(done)
			}()
			if testCase.closePeer {
				_ = clientSide.Close()
			} else if _, err := clientSide.Write([]byte{0x10, 0x00}); err != nil {
				t.Fatalf("write malformed CONNECT = %v", err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("connection.run() did not stop after the peer packet ended")
			}
			_ = clientSide.Close()
			logs := deps.logs.String()
			logged := strings.Contains(logs, "MQTT connection handling failed")
			if logged != testCase.wantLogged {
				t.Fatalf("connection logs = %q, want handling error logged=%v", logs, testCase.wantLogged)
			}
		})
	}
}

// csvDeadlineConn fails SetReadDeadline starting with call number failFrom.
type csvDeadlineConn struct {
	net.Conn
	calls    atomic.Int32
	failFrom int32
	err      error
}

func (conn *csvDeadlineConn) SetReadDeadline(deadline time.Time) error {
	if conn.calls.Add(1) >= conn.failFrom {
		return conn.err
	}
	return conn.Conn.SetReadDeadline(deadline)
}

func TestCSVReadLoopReturnsKeepAliveDeadlineRefreshError(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	serverSide, client := net.Pipe()
	defer client.Close()
	wantErr := errors.New("keepalive deadline unavailable")
	conn := newConnection(deps.server, &csvDeadlineConn{Conn: serverSide, failFrom: 2, err: wantErr})
	go conn.writerLoop()
	result := make(chan error, 1)
	go func() { result <- conn.readLoop() }()

	csvConnect(t, deps.server, client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "deadline", KeepAlive: 10})
	select {
	case err := <-result:
		if !errors.Is(err, wantErr) {
			t.Fatalf("readLoop() error = %v, want %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop() ignored the deadline refresh failure")
	}
	conn.cleanup()
	conn.requestClose()
	<-conn.writerDone
	if _, found, err := deps.sessions.Load("deadline"); err != nil || found {
		t.Fatalf("session after failed connection cleanup: found=%v err=%v", found, err)
	}
}

func TestCSVActivateReturnsConnAckWriteFailure(t *testing.T) {
	t.Run("rejected connect", func(t *testing.T) {
		deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
		serverSide, client := net.Pipe()
		defer client.Close()
		conn := newConnection(deps.server, serverSide)
		conn.requestClose()
		if err := conn.activate(mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true}); !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("activate(rejected) error = %v, want %v", err, ErrConnectionClosed)
		}
		if conn.accepted {
			t.Fatal("rejected CONNECT marked the connection accepted")
		}
	})
	t.Run("cluster not ready", func(t *testing.T) {
		var ready atomic.Bool
		runtime := csvClusterRuntime(t, &ready)
		defer runtime.stop(t)
		serverSide, client := net.Pipe()
		defer client.Close()
		conn := newConnection(runtime.server, serverSide)
		conn.requestClose()
		if err := conn.activate(mqtt.Connect{Version: mqtt.ProtocolV311, CleanStart: true, ClientID: "cluster"}); !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("activate(not ready) error = %v, want %v", err, ErrConnectionClosed)
		}
		runtime.cluster.mu.Lock()
		ownershipCalls := runtime.cluster.ownershipCalls
		runtime.cluster.mu.Unlock()
		if ownershipCalls != 0 {
			t.Fatalf("not-ready activation acquired ownership %d times", ownershipCalls)
		}
	})
}

func TestCSVActivateRejectsSessionBoundToAnotherConnection(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	ownerSide, ownerPeer := net.Pipe()
	defer ownerPeer.Close()
	defer ownerSide.Close()
	owner := newConnection(deps.server, ownerSide)
	if !deps.server.bindSession("duplicate", owner) {
		t.Fatal("bindSession(owner) failed")
	}
	serverSide, client := net.Pipe()
	defer client.Close()
	defer serverSide.Close()
	conn := newConnection(deps.server, serverSide)
	if err := conn.activate(mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "duplicate"}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("activate(duplicate) error = %v, want %v", err, ErrProtocol)
	}
	if conn.accepted {
		t.Fatal("duplicate binding left the connection accepted")
	}
	deps.server.mu.Lock()
	bound := deps.server.bySession["duplicate"]
	deps.server.mu.Unlock()
	if bound != owner {
		t.Fatal("duplicate activation replaced the existing session binding")
	}
	if _, found, err := deps.sessions.Load("duplicate"); err != nil || found {
		t.Fatalf("rejected duplicate session persisted: found=%v err=%v", found, err)
	}
	csvRequireNoControl(t, conn)
}

func csvOutbound(t *testing.T, sessionID routing.SessionID, packetID mqtt.PacketIdentifier, qos mqtt.QoS, payload string) applicationdeliveryqueue.OutboundDTO {
	t.Helper()
	state, _, err := delivery.NewOutbound(mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "recovered/topic", Payload: []byte(payload), QoS: qos, PacketID: packetID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return applicationdeliveryqueue.OutboundDTO{SessionID: sessionID, PacketID: packetID, State: state}
}

func TestCSVActivateRejectsInvalidRecoveredOutbound(t *testing.T) {
	listErr := errors.New("outbound index unreadable")
	for _, testCase := range []struct {
		name string
		list func(t *testing.T) ([]applicationdeliveryqueue.OutboundDTO, error)
		want error
	}{
		{name: "list failure", list: func(*testing.T) ([]applicationdeliveryqueue.OutboundDTO, error) {
			return nil, listErr
		}, want: listErr},
		{name: "foreign session", list: func(t *testing.T) ([]applicationdeliveryqueue.OutboundDTO, error) {
			return []applicationdeliveryqueue.OutboundDTO{csvOutbound(t, "someone-else", 1, mqtt.QoSAtLeastOnce, "x")}, nil
		}, want: ErrProtocol},
		{name: "duplicate packet id", list: func(t *testing.T) ([]applicationdeliveryqueue.OutboundDTO, error) {
			return []applicationdeliveryqueue.OutboundDTO{
				csvOutbound(t, "recovering", 4, mqtt.QoSAtLeastOnce, "a"),
				csvOutbound(t, "recovering", 4, mqtt.QoSExactlyOnce, "b"),
			}, nil
		}, want: ErrProtocol},
		{name: "zero packet id", list: func(*testing.T) ([]applicationdeliveryqueue.OutboundDTO, error) {
			return []applicationdeliveryqueue.OutboundDTO{{SessionID: "recovering"}}, nil
		}, want: ErrProtocol},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
			deps.queue.setListOutbound(func(routing.SessionID) ([]applicationdeliveryqueue.OutboundDTO, error) {
				return testCase.list(t)
			})
			serverSide, client := net.Pipe()
			defer client.Close()
			defer serverSide.Close()
			conn := newConnection(deps.server, serverSide)
			if err := conn.activate(mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "recovering"}); !errors.Is(err, testCase.want) {
				t.Fatalf("activate() error = %v, want %v", err, testCase.want)
			}
			csvRequireNoControl(t, conn)
			if !conn.deliveryBlocked.Load() {
				t.Fatal("invalid recovery unblocked deliveries")
			}
		})
	}
}

func TestCSVRecoveredOutboundIsResentInPacketIDOrder(t *testing.T) {
	serving := csvServe(t, csvServerOptions{})
	recovered := []applicationdeliveryqueue.OutboundDTO{
		csvOutbound(t, "recover", 9, mqtt.QoSAtLeastOnce, "nine"),
		csvOutbound(t, "recover", 3, mqtt.QoSExactlyOnce, "three"),
	}
	serving.queue.setListOutbound(func(sessionID routing.SessionID) ([]applicationdeliveryqueue.OutboundDTO, error) {
		if sessionID != "recover" {
			return serving.memory.ListOutbound(sessionID)
		}
		return append([]applicationdeliveryqueue.OutboundDTO(nil), recovered...), nil
	})
	client := serving.dial(t)
	csvConnect(t, serving.server, client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "recover"})
	for _, want := range []struct {
		packetID mqtt.PacketIdentifier
		qos      mqtt.QoS
		payload  string
	}{
		{packetID: 3, qos: mqtt.QoSExactlyOnce, payload: "three"},
		{packetID: 9, qos: mqtt.QoSAtLeastOnce, payload: "nine"},
	} {
		resent, ok := csvReadPacket(t, serving.server, client, mqtt.ProtocolV5).(*mqtt.Publish)
		if !ok || resent.PacketID != want.packetID || !resent.DUP || resent.QoS != want.qos || string(resent.Payload) != want.payload {
			t.Fatalf("recovered PUBLISH = %#v, want DUP id %d QoS %d payload %q", resent, want.packetID, want.qos, want.payload)
		}
	}
}

func TestCSVOutboundAckPersistenceFailuresKeepInflightState(t *testing.T) {
	persistErr := errors.New("outbound store unavailable")
	qos2AwaitingComp := csvOutbound(t, "receiver", 3, mqtt.QoSExactlyOnce, "c")
	released, _, err := qos2AwaitingComp.State.Receive(&mqtt.PubRec{PacketID: 3})
	if err != nil {
		t.Fatal(err)
	}
	qos2AwaitingComp.State = released
	for _, testCase := range []struct {
		name        string
		outbound    applicationdeliveryqueue.OutboundDTO
		completeErr error
		replaceErr  error
		dispatch    func(conn *connection) error
		want        error
		wantResend  mqtt.Packet
	}{
		{
			name: "puback complete failure", outbound: csvOutbound(t, "receiver", 1, mqtt.QoSAtLeastOnce, "a"), completeErr: persistErr,
			dispatch: func(conn *connection) error {
				return conn.dispatchPubAck(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: 1})
			},
			want:       persistErr,
			wantResend: &mqtt.Publish{},
		},
		{
			name: "pubrec for qos1 exchange", outbound: csvOutbound(t, "receiver", 1, mqtt.QoSAtLeastOnce, "a"),
			dispatch: func(conn *connection) error {
				return conn.dispatchPubRec(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: 1})
			},
			want:       ErrProtocol,
			wantResend: &mqtt.Publish{},
		},
		{
			name: "failed pubrec complete failure", outbound: csvOutbound(t, "receiver", 2, mqtt.QoSExactlyOnce, "b"), completeErr: persistErr,
			dispatch: func(conn *connection) error {
				return conn.dispatchPubRec(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: 2, ReasonCode: 0x80})
			},
			want:       persistErr,
			wantResend: &mqtt.Publish{},
		},
		{
			name: "pubrec replace failure", outbound: csvOutbound(t, "receiver", 2, mqtt.QoSExactlyOnce, "b"), replaceErr: persistErr,
			dispatch: func(conn *connection) error {
				return conn.dispatchPubRec(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: 2})
			},
			want:       persistErr,
			wantResend: &mqtt.Publish{},
		},
		{
			name: "pubcomp complete failure", outbound: qos2AwaitingComp, completeErr: persistErr,
			dispatch: func(conn *connection) error {
				return conn.dispatchPubComp(&mqtt.PubComp{Version: mqtt.ProtocolV5, PacketID: 3})
			},
			want:       persistErr,
			wantResend: &mqtt.PubRel{},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
			deps.queue.setOutboundErrors(testCase.completeErr, testCase.replaceErr)
			serverSide, client := net.Pipe()
			defer client.Close()
			defer serverSide.Close()
			conn := newConnection(deps.server, serverSide)
			conn.sessionID = "receiver"
			conn.version = mqtt.ProtocolV5
			if !conn.packetIDs.reserve(testCase.outbound.PacketID) {
				t.Fatal("reserve() failed")
			}
			conn.outbound[testCase.outbound.PacketID] = testCase.outbound

			if err := testCase.dispatch(conn); !errors.Is(err, testCase.want) {
				t.Fatalf("dispatch error = %v, want %v", err, testCase.want)
			}
			stored, exists := conn.outbound[testCase.outbound.PacketID]
			if !exists {
				t.Fatal("failed acknowledgement dropped the in-flight exchange")
			}
			if conn.packetIDs.reserve(testCase.outbound.PacketID) {
				t.Fatal("failed acknowledgement released the packet identifier")
			}
			action, err := stored.State.Resume()
			if err != nil {
				t.Fatal(err)
			}
			switch testCase.wantResend.(type) {
			case *mqtt.Publish:
				if _, ok := action.Send.(*mqtt.Publish); !ok {
					t.Fatalf("resume after failure = %#v, want PUBLISH retransmission", action.Send)
				}
			case *mqtt.PubRel:
				if _, ok := action.Send.(*mqtt.PubRel); !ok {
					t.Fatalf("resume after failure = %#v, want PUBREL retransmission", action.Send)
				}
			}
			csvRequireNoControl(t, conn)
		})
	}
}

func TestCSVUnsubscribeStopsRoutingToSession(t *testing.T) {
	serving := csvServe(t, csvServerOptions{})
	client := serving.dial(t)
	csvConnect(t, serving.server, client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "reader"})
	csvSubscribe(t, serving.server, client, 1, "news/#")
	writeTestPackets(t, client, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "news/a", Payload: []byte("one")})
	if first, ok := csvReadPacket(t, serving.server, client, mqtt.ProtocolV5).(*mqtt.Publish); !ok || string(first.Payload) != "one" {
		t.Fatalf("subscribed delivery = %#v, want payload one", first)
	}

	writeTestPackets(t, client, &mqtt.Unsubscribe{Version: mqtt.ProtocolV5, PacketID: 2, Filters: []string{"news/#"}})
	unsubAck, ok := csvReadPacket(t, serving.server, client, mqtt.ProtocolV5).(*mqtt.UnsubAck)
	if !ok || unsubAck.PacketID != 2 || len(unsubAck.ReasonCodes) != 1 || unsubAck.ReasonCodes[0] != 0x00 {
		t.Fatalf("UNSUBACK = %#v, want packet 2 reason Success", unsubAck)
	}
	writeTestPackets(t, client, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "news/a", Payload: []byte("two")})
	csvSubscribe(t, serving.server, client, 3, "other")
	writeTestPackets(t, client, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "other", Payload: []byte("three")})
	next, ok := csvReadPacket(t, serving.server, client, mqtt.ProtocolV5).(*mqtt.Publish)
	if !ok || string(next.Payload) != "three" {
		t.Fatalf("delivery after unsubscribe = %#v, want only payload three", next)
	}
}

func TestCSVClusterUnsubscribeCommitFailureClosesWithoutAck(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	runtime := csvClusterRuntime(t, &ready)
	defer runtime.stop(t)
	csvConnect(t, runtime.server, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "cluster-unsub"})
	runtime.cluster.mu.Lock()
	runtime.cluster.subscriptionErr = errors.New("quorum unavailable")
	runtime.cluster.mu.Unlock()
	writeTestPackets(t, runtime.client, &mqtt.Unsubscribe{Version: mqtt.ProtocolV5, PacketID: 1, Filters: []string{"sensors/#"}})
	csvRequireClosedWithoutResponse(t, runtime.client)
	runtime.cluster.mu.Lock()
	calls := append([]uint32(nil), runtime.cluster.subscriptionCalls...)
	runtime.cluster.mu.Unlock()
	if len(calls) != 1 || calls[0] != 2 {
		t.Fatalf("committed subscription actions = %v, want one remove", calls)
	}
}

func TestCSVClusterNotReadyClosesMutatingPackets(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		packet mqtt.Packet
	}{
		{name: "subscribe", packet: &mqtt.Subscribe{
			Version: mqtt.ProtocolV5, PacketID: 1,
			Subscriptions: []mqtt.Subscription{{Filter: "sensors/#", QoS: mqtt.QoSAtMostOnce}},
		}},
		{name: "unsubscribe", packet: &mqtt.Unsubscribe{Version: mqtt.ProtocolV5, PacketID: 1, Filters: []string{"sensors/#"}}},
		{name: "qos0 publish", packet: &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "sensors/a", Payload: []byte("x")}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var ready atomic.Bool
			ready.Store(true)
			runtime := csvClusterRuntime(t, &ready)
			defer runtime.stop(t)
			csvConnect(t, runtime.server, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "cluster-client"})
			ready.Store(false)
			writeTestPackets(t, runtime.client, testCase.packet)
			csvRequireClosedWithoutResponse(t, runtime.client)
			runtime.cluster.mu.Lock()
			calls := len(runtime.cluster.subscriptionCalls)
			runtime.cluster.mu.Unlock()
			if calls != 0 {
				t.Fatalf("not-ready cluster committed %d subscription mutations", calls)
			}
		})
	}
}

func TestCSVClusterQoS1PublishFromMQTT311ClosesWithoutDisconnect(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	runtime := csvClusterRuntime(t, &ready)
	defer runtime.stop(t)
	csvConnect(t, runtime.server, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV311, CleanStart: true, ClientID: "legacy"})
	encoded := encodeTestPacket(t, &mqtt.Publish{
		Version: mqtt.ProtocolV311, Topic: "cluster/test", QoS: mqtt.QoSAtLeastOnce,
		PacketID: 1, Payload: []byte("x"),
	})
	if _, err := runtime.client.Write(encoded); err != nil &&
		!errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write rejected MQTT 3.1.1 PUBLISH: %v", err)
	}
	csvRequireClosedWithoutResponse(t, runtime.client)
}

func TestCSVControlBarrierBoundaries(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})

	t.Run("disconnect on stopped connection", func(t *testing.T) {
		serverSide, client := net.Pipe()
		defer client.Close()
		conn := newConnection(deps.server, serverSide)
		conn.version = mqtt.ProtocolV5
		conn.requestClose()
		if err := conn.dispatch(&mqtt.Disconnect{Version: mqtt.ProtocolV5, ReasonCode: 0x80}); !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("dispatch(DISCONNECT) error = %v, want %v", err, ErrConnectionClosed)
		}
		if conn.disconnectReason != session.DisconnectAbnormal {
			t.Fatalf("disconnect reason = %v, want abnormal for reason 0x80", conn.disconnectReason)
		}
	})
	t.Run("full control queue", func(t *testing.T) {
		serverSide, client := net.Pipe()
		defer client.Close()
		conn := newConnection(deps.server, serverSide)
		for range cap(conn.controlQueue) {
			conn.controlQueue <- controlMessage{packet: &mqtt.PingResp{Version: mqtt.ProtocolV5}}
		}
		if err := conn.enqueueBarrier(); !errors.Is(err, ErrControlQueueFull) {
			t.Fatalf("enqueueBarrier() error = %v, want %v", err, ErrControlQueueFull)
		}
		csvRequireStopped(t, conn, true)
	})
	t.Run("stopped while waiting", func(t *testing.T) {
		serverSide, client := net.Pipe()
		defer client.Close()
		conn := newConnection(deps.server, serverSide)
		result := make(chan error, 1)
		go func() { result <- conn.enqueueBarrier() }()
		select {
		case message := <-conn.controlQueue:
			if !message.barrier || message.packet != nil {
				t.Fatalf("queued barrier = %#v, want packet-free barrier", message)
			}
		case <-time.After(time.Second):
			t.Fatal("barrier was not queued")
		}
		conn.requestClose()
		select {
		case err := <-result:
			if !errors.Is(err, ErrConnectionClosed) {
				t.Fatalf("enqueueBarrier() error = %v, want %v", err, ErrConnectionClosed)
			}
		case <-time.After(time.Second):
			t.Fatal("enqueueBarrier() did not observe connection close")
		}
	})
}

func csvRegisterSubscriber(t *testing.T, server *Server, memory *deliveryqueue.Queue, sessionID routing.SessionID, filter string, qos mqtt.QoS) {
	t.Helper()
	if err := memory.Register(sessionID); err != nil {
		t.Fatal(err)
	}
	result, err := server.subscriptions.Subscribe(sessionID, mqtt.Subscribe{
		Version: mqtt.ProtocolV5, PacketID: 1,
		Subscriptions: []mqtt.Subscription{{Filter: filter, QoS: qos}},
	}, server.config.Clock.Now())
	if err != nil || len(result.Ack.ReasonCodes) != 1 || result.Ack.ReasonCodes[0] != byte(qos) {
		t.Fatalf("Subscribe(%s) = %#v, %v", filter, result, err)
	}
}

func TestCSVDispatchPublishPropagatesRoutingAndWriteFailures(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	csvRegisterSubscriber(t, deps.server, deps.memory, "sub", "data/#", mqtt.QoSAtLeastOnce)
	newPublisher := func(t *testing.T) *connection {
		t.Helper()
		serverSide, client := net.Pipe()
		t.Cleanup(func() { _ = client.Close(); _ = serverSide.Close() })
		conn := newConnection(deps.server, serverSide)
		conn.sessionID = "publisher"
		conn.version = mqtt.ProtocolV5
		return conn
	}

	t.Run("qos0 invalid topic", func(t *testing.T) {
		conn := newPublisher(t)
		if err := conn.dispatchPublish(mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "data/#"}); !errors.Is(err, routing.ErrInvalidTopicName) {
			t.Fatalf("dispatchPublish(QoS0) error = %v, want %v", err, routing.ErrInvalidTopicName)
		}
	})
	t.Run("qos1 invalid topic", func(t *testing.T) {
		conn := newPublisher(t)
		err := conn.dispatchPublish(mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "data/#", QoS: mqtt.QoSAtLeastOnce, PacketID: 5})
		if !errors.Is(err, routing.ErrInvalidTopicName) {
			t.Fatalf("dispatchPublish(QoS1) error = %v, want %v", err, routing.ErrInvalidTopicName)
		}
		if _, exists := conn.inbound[5]; exists {
			t.Fatal("failed QoS 1 publish recorded inbound state")
		}
		csvRequireNoControl(t, conn)
	})
	t.Run("qos2 release invalid topic", func(t *testing.T) {
		conn := newPublisher(t)
		if err := conn.dispatchPublish(mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "data/+", QoS: mqtt.QoSExactlyOnce, PacketID: 6}); err != nil {
			t.Fatalf("dispatchPublish(QoS2) error = %v", err)
		}
		if pubRec, ok := csvTakeControl(t, conn).(*mqtt.PubRec); !ok || pubRec.PacketID != 6 {
			t.Fatalf("QoS 2 receipt = %#v, want PUBREC 6", pubRec)
		}
		if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 6}); !errors.Is(err, routing.ErrInvalidTopicName) {
			t.Fatalf("dispatchPubRel() error = %v, want %v", err, routing.ErrInvalidTopicName)
		}
		csvRequireNoControl(t, conn)
	})
	t.Run("qos1 puback after close", func(t *testing.T) {
		conn := newPublisher(t)
		conn.requestClose()
		err := conn.dispatchPublish(mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "data/one", Payload: []byte("one"), QoS: mqtt.QoSAtLeastOnce, PacketID: 7})
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("dispatchPublish(closed QoS1) error = %v, want %v", err, ErrConnectionClosed)
		}
		if _, exists := conn.inbound[7]; !exists {
			t.Fatal("accepted QoS 1 publish did not record duplicate-suppression state")
		}
		if items := takeDeliveries(t, deps.memory, "sub", time.Time{}); len(items) != 1 || string(items[0].Payload) != "one" {
			t.Fatalf("subscriber deliveries = %#v, want one", items)
		}
	})
	t.Run("qos2 pubcomp after close", func(t *testing.T) {
		conn := newPublisher(t)
		if err := conn.dispatchPublish(mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "data/two", Payload: []byte("two"), QoS: mqtt.QoSExactlyOnce, PacketID: 8}); err != nil {
			t.Fatal(err)
		}
		_ = csvTakeControl(t, conn)
		conn.requestClose()
		if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 8}); !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("dispatchPubRel(closed) error = %v, want %v", err, ErrConnectionClosed)
		}
		if items := takeDeliveries(t, deps.memory, "sub", time.Time{}); len(items) != 1 || string(items[0].Payload) != "two" {
			t.Fatalf("subscriber deliveries = %#v, want two", items)
		}
	})
}

func TestCSVRetainedFullNotificationOnClosedConnection(t *testing.T) {
	runtime := newBoundedRetainedRuntime(t, 1, 4, 10)
	defer runtime.stop(t)
	newPublisher := func(t *testing.T) *connection {
		t.Helper()
		serverSide, client := net.Pipe()
		t.Cleanup(func() { _ = client.Close(); _ = serverSide.Close() })
		conn := newConnection(runtime.server, serverSide)
		conn.sessionID = "retainer"
		conn.version = mqtt.ProtocolV5
		return conn
	}
	t.Run("qos0", func(t *testing.T) {
		conn := newPublisher(t)
		conn.requestClose()
		err := conn.dispatchPublish(mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "retained/a", Payload: []byte("oversized"), Retain: true})
		if !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("dispatchPublish(retained full) error = %v, want %v", err, ErrConnectionClosed)
		}
	})
	t.Run("qos2 release", func(t *testing.T) {
		conn := newPublisher(t)
		if err := conn.dispatchPublish(mqtt.Publish{
			Version: mqtt.ProtocolV5, Topic: "retained/b", Payload: []byte("oversized"), Retain: true,
			QoS: mqtt.QoSExactlyOnce, PacketID: 9,
		}); err != nil {
			t.Fatal(err)
		}
		_ = csvTakeControl(t, conn)
		conn.requestClose()
		if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 9}); !errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("dispatchPubRel(retained full) error = %v, want %v", err, ErrConnectionClosed)
		}
	})
}

func TestCSVUnauthorizedPublishIsRejectedPerProtocolVersion(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	csvRegisterSubscriber(t, deps.server, deps.memory, "watch", "#", mqtt.QoSAtMostOnce)
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := security.NewPrincipal(tenant, "device-client", "device-1", "cred-1", []security.Role{"device"}, time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name      string
		version   mqtt.ProtocolVersion
		publish   mqtt.Publish
		wantAck   mqtt.Packet
		wantClose bool
	}{
		{name: "mqtt5 qos1 invalid tenant topic", version: mqtt.ProtocolV5, publish: mqtt.Publish{Topic: "bad/#", QoS: mqtt.QoSAtLeastOnce, PacketID: 2},
			wantAck: &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: 2, ReasonCode: 0x87}},
		{name: "mqtt5 qos2 denied", version: mqtt.ProtocolV5, publish: mqtt.Publish{Topic: "denied/topic", QoS: mqtt.QoSExactlyOnce, PacketID: 3},
			wantAck: &mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: 3, ReasonCode: 0x87}},
		{name: "mqtt5 qos0 denied", version: mqtt.ProtocolV5, publish: mqtt.Publish{Topic: "denied/topic"}},
		{name: "mqtt311 qos1 denied", version: mqtt.ProtocolV311, publish: mqtt.Publish{Topic: "denied/topic", QoS: mqtt.QoSAtLeastOnce, PacketID: 4}, wantClose: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			serverSide, client := net.Pipe()
			defer client.Close()
			defer serverSide.Close()
			conn := newConnection(deps.server, serverSide)
			conn.sessionID = "device"
			conn.version = testCase.version
			conn.principal = principal
			publish := testCase.publish
			publish.Version = testCase.version
			publish.Payload = []byte("secret")
			if err := conn.dispatchPublish(publish); err != nil {
				t.Fatalf("dispatchPublish() error = %v", err)
			}
			csvRequireStopped(t, conn, testCase.wantClose)
			if testCase.wantAck == nil {
				csvRequireNoControl(t, conn)
			} else {
				got := csvTakeControl(t, conn)
				gotWire := encodeTestPacket(t, got)
				wantWire := encodeTestPacket(t, testCase.wantAck)
				if !bytes.Equal(gotWire, wantWire) {
					t.Fatalf("rejection = %#v, want %#v", got, testCase.wantAck)
				}
			}
			if _, exists := conn.inbound[publish.PacketID]; exists {
				t.Fatal("rejected publish recorded inbound state")
			}
		})
	}
	if items := takeDeliveries(t, deps.memory, "watch", time.Time{}); len(items) != 0 {
		t.Fatalf("unauthorized publishes were routed: %#v", items)
	}
}

// TestCSVWriteDeliveriesQueuePeekOutcomes verifies the delivery write path handles each queue peek outcome.
// Scenarios: performance-and-verification.md:PRF-SCN-088
func TestCSVWriteDeliveriesQueuePeekOutcomes(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	serverSide, client := net.Pipe()
	defer client.Close()
	defer serverSide.Close()
	conn := newConnection(deps.server, serverSide)
	conn.sessionID = "peeker"
	conn.version = mqtt.ProtocolV5

	peekErr := errors.New("queue storage corrupted")
	deps.queue.setPeek(func(routing.SessionID, time.Time) (applicationdeliveryqueue.DeliveryDTO, bool, error) {
		return applicationdeliveryqueue.DeliveryDTO{}, false, peekErr
	})
	if err := conn.writeDeliveries(); !errors.Is(err, peekErr) {
		t.Fatalf("writeDeliveries() error = %v, want %v", err, peekErr)
	}

	deps.queue.setPeek(func(routing.SessionID, time.Time) (applicationdeliveryqueue.DeliveryDTO, bool, error) {
		return applicationdeliveryqueue.DeliveryDTO{Topic: "ignored", QoS: mqtt.QoSAtLeastOnce}, false, nil
	})
	if err := conn.writeDeliveries(); err != nil {
		t.Fatalf("writeDeliveries(empty) error = %v", err)
	}
	if len(conn.outbound) != 0 {
		t.Fatalf("empty peek created outbound state %#v", conn.outbound)
	}
	deps.queue.setPeek(func(routing.SessionID, time.Time) (applicationdeliveryqueue.DeliveryDTO, bool, error) {
		return applicationdeliveryqueue.DeliveryDTO{}, false, peekErr
	})
	conn.requestClose()
	if err := conn.writeDeliveries(); err != nil {
		t.Fatalf("writeDeliveries(closed with peek error) error = %v, want nil", err)
	}
}

// TestCSVSubscribeAckWriteFailureKeepsDeliveryBlocked verifies a SUBACK write failure keeps delivery blocked and stops the writer.
// Scenarios: performance-and-verification.md:PRF-SCN-088
func TestCSVSubscribeAckWriteFailureKeepsDeliveryBlocked(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	serverSide, client := net.Pipe()
	defer client.Close()
	toggle := &csvToggleWriteConn{Conn: serverSide, err: errors.New("ack write failed")}
	conn := newConnection(deps.server, toggle)
	go conn.writerLoop()
	defer func() {
		conn.requestClose()
		<-conn.writerDone
	}()

	activated := make(chan error, 1)
	go func() {
		activated <- conn.activate(mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "sub-fail"})
	}()
	if packet := csvReadPacket(t, deps.server, client, mqtt.ProtocolV5); packet == nil {
		t.Fatal("received nil CONNACK packet")
	} else if _, ok := packet.(*mqtt.ConnAck); !ok {
		t.Fatalf("activation packet = %T, want CONNACK", packet)
	}
	if err := <-activated; err != nil {
		t.Fatalf("activate() error = %v, want nil", err)
	}

	toggle.fail.Store(true)
	errAck := toggle.err
	err := conn.dispatch(&mqtt.Subscribe{
		Version: mqtt.ProtocolV5, PacketID: 1,
		Subscriptions: []mqtt.Subscription{{Filter: "sub-fail/topic", QoS: mqtt.QoSAtMostOnce}},
	})
	if !errors.Is(err, errAck) && !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("dispatch(SUBSCRIBE) error = %v, want %v or %v", err, errAck, ErrConnectionClosed)
	}
	if !conn.deliveryBlocked.Load() {
		t.Fatal("deliveryBlocked = false after SUBACK write failure, want true")
	}
	select {
	case <-conn.writerDone:
	case <-time.After(time.Second):
		t.Fatal("writerDone did not close after SUBACK write failure")
	}
}

// TestCSVWriterLoopQueuedControlCloseAndStoppedFailure verifies the writer loop closes after a queued control packet and reports a stopped failure.
// Scenarios: performance-and-verification.md:PRF-SCN-088
func TestCSVWriterLoopQueuedControlCloseAndStoppedFailure(t *testing.T) {
	t.Run("close after queued control", func(t *testing.T) {
		deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
		serverSide, client := net.Pipe()
		defer client.Close()
		defer serverSide.Close()
		conn := newConnection(deps.server, serverSide)
		done := make(chan error, 1)
		conn.controlQueue <- controlMessage{
			packet: &mqtt.Disconnect{Version: mqtt.ProtocolV5}, done: done, closeAfter: true,
		}
		go conn.writerLoop()
		packet := csvReadPacket(t, deps.server, client, mqtt.ProtocolV5)
		if _, ok := packet.(*mqtt.Disconnect); !ok {
			t.Fatalf("queued control packet = %T, want DISCONNECT", packet)
		}
		if err := <-done; err != nil {
			t.Fatalf("queued DISCONNECT done error = %v, want nil", err)
		}
		select {
		case <-conn.writerDone:
		case <-time.After(time.Second):
			t.Fatal("writerDone did not close after queued DISCONNECT")
		}
		if !conn.isStopping() {
			t.Fatal("isStopping() = false after closeAfter control, want true")
		}
	})

	t.Run("stopped connection control failure", func(t *testing.T) {
		deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
		serverSide, client := net.Pipe()
		defer client.Close()
		defer serverSide.Close()
		conn := newConnection(deps.server, serverSide)
		_ = client.Close()
		conn.requestClose()
		done := make(chan error, 1)
		conn.controlQueue <- controlMessage{packet: &mqtt.PingResp{Version: mqtt.ProtocolV5}, done: done}
		conn.writerLoop()
		if err := <-done; !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("stopped control done error = %v, want %v", err, io.ErrClosedPipe)
		}
		if strings.Contains(deps.logs.String(), "MQTT writer failed") {
			t.Fatalf("stopped control logged unexpected writer failure: %s", deps.logs.String())
		}
	})
}

func TestCSVWriteDeliveriesKeepsPromotedMessageInflightOnWireFailure(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	serverSide, client := net.Pipe()
	defer serverSide.Close()
	conn := newConnection(deps.server, serverSide)
	conn.sessionID = "wire"
	conn.version = mqtt.ProtocolV5
	if err := deps.memory.Register(conn.sessionID); err != nil {
		t.Fatal(err)
	}
	if err := deps.memory.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: conn.sessionID, Topic: "wire/topic", Payload: []byte("payload"), QoS: mqtt.QoSAtLeastOnce,
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	if err := conn.writeDeliveries(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writeDeliveries() error = %v, want %v", err, io.ErrClosedPipe)
	}
	outbound, err := deps.memory.ListOutbound(conn.sessionID)
	if err != nil || len(outbound) != 1 {
		t.Fatalf("durable outbound after wire failure = %#v, %v; want one in-flight exchange", outbound, err)
	}
	if _, tracked := conn.outbound[outbound[0].PacketID]; !tracked {
		t.Fatalf("connection lost in-flight packet %d", outbound[0].PacketID)
	}
	action, err := outbound[0].State.Resume()
	if err != nil {
		t.Fatal(err)
	}
	if resent, ok := action.Send.(*mqtt.Publish); !ok || resent.Topic != "wire/topic" || !resent.DUP {
		t.Fatalf("resume = %#v, want DUP PUBLISH wire/topic", action.Send)
	}
	if _, ok, err := deps.memory.Peek(conn.sessionID, time.Time{}); err != nil || ok {
		t.Fatalf("promoted head still queued: ok=%t err=%v", ok, err)
	}
}

// csvZeroWriteConn accepts writes without making progress.
type csvZeroWriteConn struct {
	net.Conn
}

func (csvZeroWriteConn) Write([]byte) (int, error) {
	return 0, nil
}

func TestCSVWritePacketRejectsZeroProgressWrite(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	serverSide, client := net.Pipe()
	defer client.Close()
	defer serverSide.Close()
	conn := newConnection(deps.server, csvZeroWriteConn{Conn: serverSide})
	if err := conn.writePacket(&mqtt.PingResp{Version: mqtt.ProtocolV5}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writePacket() error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestCSVPacketTypeNames(t *testing.T) {
	for _, testCase := range []struct {
		packet mqtt.Packet
		want   string
	}{
		{packet: &mqtt.Connect{}, want: "CONNECT"},
		{packet: &mqtt.ConnAck{}, want: "CONNACK"},
		{packet: &mqtt.Publish{}, want: "PUBLISH"},
		{packet: &mqtt.PubAck{}, want: "PUBACK"},
		{packet: &mqtt.PubRec{}, want: "PUBREC"},
		{packet: &mqtt.PubRel{}, want: "PUBREL"},
		{packet: &mqtt.PubComp{}, want: "PUBCOMP"},
		{packet: &mqtt.Subscribe{}, want: "SUBSCRIBE"},
		{packet: &mqtt.SubAck{}, want: "SUBACK"},
		{packet: &mqtt.Unsubscribe{}, want: "UNSUBSCRIBE"},
		{packet: &mqtt.UnsubAck{}, want: "UNSUBACK"},
		{packet: &mqtt.PingReq{}, want: "PINGREQ"},
		{packet: &mqtt.PingResp{}, want: "PINGRESP"},
		{packet: &mqtt.Disconnect{}, want: "DISCONNECT"},
		{packet: nil, want: "UNKNOWN"},
		{packet: mqtt.PingReq{}, want: "UNKNOWN"},
	} {
		if got := packetTypeName(testCase.packet); got != testCase.want {
			t.Errorf("packetTypeName(%T) = %q, want %q", testCase.packet, got, testCase.want)
		}
	}
	var detached *connection
	if detached.metrics() != nil || (&connection{}).metrics() != nil {
		t.Fatal("connection without server reported a metrics recorder")
	}
}

func TestCSVPublishWillRoutingOutcomes(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{queueCapacity: 1})
	if err := deps.server.PublishWill("victim", tenancy.SessionKey{}, mqtt.Will{Topic: "will/#", Payload: []byte("x")}); !errors.Is(err, routing.ErrInvalidTopicName) {
		t.Fatalf("PublishWill(invalid topic) error = %v, want %v", err, routing.ErrInvalidTopicName)
	}

	csvRegisterSubscriber(t, deps.server, deps.memory, "slow", "will/#", mqtt.QoSAtMostOnce)
	if err := deps.memory.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "slow", Topic: "will/old", Payload: []byte("old")}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	serverSide, client := net.Pipe()
	defer client.Close()
	slow := newConnection(deps.server, serverSide)
	if !deps.server.bindSession("slow", slow) {
		t.Fatal("bindSession(slow) failed")
	}
	if err := deps.server.PublishWill("victim", tenancy.SessionKey{}, mqtt.Will{Topic: "will/victim", Payload: []byte("gone")}); err != nil {
		t.Fatalf("PublishWill(saturated) error = %v", err)
	}
	csvRequireStopped(t, slow, true)
	if items := takeDeliveries(t, deps.memory, "slow", time.Time{}); len(items) != 1 || string(items[0].Payload) != "old" {
		t.Fatalf("saturated subscriber queue = %#v, want only the original item", items)
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-055
func TestCSVDeliverForwardedRoutingOutcomes(t *testing.T) {
	var detached *Server
	if err := detached.DeliverForwarded(applicationdeliveryqueue.DeliveryDTO{
		SessionID: "registered", Topic: "will/one", QoS: mqtt.QoSAtMostOnce,
	}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("DeliverForwarded(nil server) error = %v, want %v", err, applicationdeliveryqueue.ErrUnavailable)
	}

	deps := csvNewServer(t, newPipeListener(), csvServerOptions{queueCapacity: 1})
	csvRegisterSubscriber(t, deps.server, deps.memory, "registered", "will/#", mqtt.QoSAtMostOnce)
	serverSide, client := net.Pipe()
	defer client.Close()
	defer serverSide.Close()
	registered := newConnection(deps.server, serverSide)
	if !deps.server.bindSession("registered", registered) {
		t.Fatal("bindSession(registered) failed")
	}
	defer registered.requestClose()
	forwarded := applicationdeliveryqueue.DeliveryDTO{
		SessionID: "registered", Topic: "will/one", Payload: []byte("forwarded"), QoS: mqtt.QoSAtMostOnce,
	}
	if err := deps.server.DeliverForwarded(forwarded); err != nil {
		t.Fatalf("DeliverForwarded(registered) error = %v", err)
	}
	select {
	case <-registered.deliveryWake:
	default:
		t.Fatal("registered subscriber was not woken after forwarded enqueue")
	}
	if items := takeDeliveries(t, deps.memory, "registered", time.Time{}); len(items) != 1 ||
		string(items[0].Payload) != "forwarded" {
		t.Fatalf("registered forwarded queue = %#v, want one forwarded item", items)
	}

	if err := deps.server.DeliverForwarded(applicationdeliveryqueue.DeliveryDTO{
		SessionID: "missing", Topic: "will/one", QoS: mqtt.QoSAtMostOnce,
	}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("DeliverForwarded(unregistered) error = %v, want %v", err, applicationdeliveryqueue.ErrUnavailable)
	}

	csvRegisterSubscriber(t, deps.server, deps.memory, "slow", "will/#", mqtt.QoSAtMostOnce)
	if err := deps.memory.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: "slow", Topic: "will/old", Payload: []byte("old"),
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	slowSide, slowClient := net.Pipe()
	defer slowClient.Close()
	defer slowSide.Close()
	slow := newConnection(deps.server, slowSide)
	if !deps.server.bindSession("slow", slow) {
		t.Fatal("bindSession(slow) failed")
	}
	if err := deps.server.DeliverForwarded(applicationdeliveryqueue.DeliveryDTO{
		SessionID: "slow", Topic: "will/one", Payload: []byte("full"), QoS: mqtt.QoSAtMostOnce,
	}); !errors.Is(err, applicationdeliveryqueue.ErrFull) {
		t.Fatalf("DeliverForwarded(full) error = %v, want %v", err, applicationdeliveryqueue.ErrFull)
	}
	csvRequireStopped(t, slow, true)
	if items := takeDeliveries(t, deps.memory, "slow", time.Time{}); len(items) != 1 ||
		string(items[0].Payload) != "old" {
		t.Fatalf("full forwarded queue = %#v, want only the original item", items)
	}
}
