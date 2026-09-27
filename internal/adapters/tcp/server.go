// Package tcp provides the development-only loopback MQTT ingress adapter.
package tcp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
	applicationauthentication "narumq/internal/application/ports/authentication"
	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/application/ports/deliveryqueue"
	portsmetrics "narumq/internal/application/ports/metrics"
	applicationretained "narumq/internal/application/ports/retained"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/delivery"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrNilListener means that the server has no accepted socket source.
	ErrNilListener = errors.New("tcp listener is nil")
	// ErrNilDecoder means that packet framing cannot be validated.
	ErrNilDecoder = errors.New("tcp MQTT decoder is nil")
	// ErrNilEncoder means that outbound packets cannot be encoded.
	ErrNilEncoder = errors.New("tcp MQTT encoder is nil")
	// ErrNilConnectUseCase means that session activation is unavailable.
	ErrNilConnectUseCase = errors.New("tcp connect use case is nil")
	// ErrNilSubscribeUseCase means that subscription handling is unavailable.
	ErrNilSubscribeUseCase = errors.New("tcp subscribe use case is nil")
	// ErrNilPublishUseCase means that publication handling is unavailable.
	ErrNilPublishUseCase = errors.New("tcp publish use case is nil")
	// ErrNilDeliveryQueue means that delivery wakeups have no queue source.
	ErrNilDeliveryQueue = errors.New("tcp delivery queue is nil")
	// ErrNilClock means that expiry and delivery timestamps have no time source.
	ErrNilClock = errors.New("tcp clock is nil")
	// ErrInvalidConfig means that a bounded runtime value is not positive.
	ErrInvalidConfig = errors.New("tcp server configuration is invalid")
	// ErrControlQueueFull means that a connection cannot accept another control packet.
	ErrControlQueueFull = errors.New("tcp control queue full")
	// ErrConnectionClosed means that a connection writer stopped before a packet was sent.
	ErrConnectionClosed = errors.New("tcp connection closed")
	// ErrProtocol means that the client sent a packet invalid for the current state.
	ErrProtocol = errors.New("MQTT protocol error")
	// ErrClusterNotReady means that replicated state cannot safely accept a mutation.
	ErrClusterNotReady = errors.New("cluster is not ready")
)

// Config bounds packet memory, handshake waiting, socket writes and per-connection
// control output. Delivery queue capacity is owned by the injected queue.
type Config struct {
	Clock                      applicationclock.Clock
	MaxPacketSize              int
	ConnectTimeout             time.Duration
	WriteTimeout               time.Duration
	ControlQueueCapacity       int
	ClientCertificateExtractor func(net.Conn) (applicationauthentication.ClientCertificateDTO, bool)
	Policy                     security.Policy
	Metrics                    portsmetrics.MetricsRecorder
	ClusterMode                bool
	LocalMemberID              uint64
	ClusterReady               func() bool
}

// Server owns one listener, all connection goroutines and the session registry.
type Server struct {
	listeners     []net.Listener
	config        Config
	decoder       *mqtt.Decoder
	encoder       *mqtt.Encoder
	connections   *connect.UseCase
	subscriptions *subscribe.UseCase
	publications  *publish.UseCase
	deliveries    deliveryqueue.Queue
	logger        *slog.Logger

	mu                   sync.Mutex
	active               map[*connection]struct{}
	bySession            map[routing.SessionID]*connection
	stopped              bool
	stopOnce             sync.Once
	listenersWaitGroup   sync.WaitGroup
	connectionsWaitGroup sync.WaitGroup
	serveCtx             context.Context
	recordAcceptErr      func(error)
}

// NewServer creates a TCP ingress over application use cases and packet codec.
// It does not start goroutines; Serve owns the runtime lifecycle.
func NewServer(
	listener net.Listener,
	config Config,
	decoder *mqtt.Decoder,
	encoder *mqtt.Encoder,
	connections *connect.UseCase,
	subscriptions *subscribe.UseCase,
	publications *publish.UseCase,
	deliveries deliveryqueue.Queue,
	logger *slog.Logger,
) (*Server, error) {
	if listener == nil {
		return nil, ErrNilListener
	}
	if decoder == nil {
		return nil, ErrNilDecoder
	}
	if encoder == nil {
		return nil, ErrNilEncoder
	}
	if connections == nil {
		return nil, ErrNilConnectUseCase
	}
	if subscriptions == nil {
		return nil, ErrNilSubscribeUseCase
	}
	if publications == nil {
		return nil, ErrNilPublishUseCase
	}
	if deliveries == nil {
		return nil, ErrNilDeliveryQueue
	}
	if config.Clock == nil {
		return nil, ErrNilClock
	}
	if config.MaxPacketSize <= 0 || config.ConnectTimeout <= 0 || config.WriteTimeout <= 0 || config.ControlQueueCapacity <= 0 {
		return nil, ErrInvalidConfig
	}
	if config.ClusterMode && (config.LocalMemberID == 0 ||
		config.ClusterReady == nil ||
		!connections.ClusterOwnershipConfigured(config.LocalMemberID) ||
		!subscriptions.ClusterCommitterConfigured()) {
		return nil, ErrInvalidConfig
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		listeners:     []net.Listener{listener},
		config:        config,
		decoder:       decoder,
		encoder:       encoder,
		connections:   connections,
		subscriptions: subscriptions,
		publications:  publications,
		deliveries:    deliveries,
		logger:        logger,
		active:        make(map[*connection]struct{}),
		bySession:     make(map[routing.SessionID]*connection),
	}, nil
}

func (server *Server) clusterReady() bool {
	return !server.config.ClusterMode || server.config.ClusterReady()
}

// AddListener registers an additional listener (such as a TLS listener) to accept connections.
func (server *Server) AddListener(listener net.Listener) error {
	if server == nil {
		return ErrNilListener
	}
	if listener == nil {
		return ErrNilListener
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.stopped {
		_ = listener.Close()
		return ErrConnectionClosed
	}
	server.listeners = append(server.listeners, listener)
	if server.serveCtx != nil && server.recordAcceptErr != nil {
		server.startListener(server.serveCtx, listener, server.recordAcceptErr)
	}
	return nil
}

func (server *Server) startListener(ctx context.Context, l net.Listener, recordErr func(error)) {
	server.listenersWaitGroup.Add(1)
	go func() {
		defer server.listenersWaitGroup.Done()
		for {
			socket, err := l.Accept()
			if err != nil {
				if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
					recordErr(err)
				}
				break
			}
			connection := newConnection(server, socket)
			server.addActive(connection)
			server.connectionsWaitGroup.Add(1)
			go func() {
				defer server.connectionsWaitGroup.Done()
				connection.run()
			}()
		}
	}()
}

// Serve accepts connections until all listeners are closed, the context is
// cancelled, or an unexpected accept error occurs. Cancellation and normal
// listener closure return nil after all connection goroutines finish.
func (server *Server) Serve(ctx context.Context) error {
	if server == nil {
		return ErrNilListener
	}
	if ctx == nil {
		return ErrInvalidConfig
	}
	serveDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			server.stopAll()
		case <-serveDone:
		}
	}()

	var (
		errMu     sync.Mutex
		acceptErr error
	)
	recordErr := func(err error) {
		errMu.Lock()
		if acceptErr == nil {
			acceptErr = err
		}
		errMu.Unlock()
		server.stopAll()
	}

	server.mu.Lock()
	server.serveCtx = ctx
	server.recordAcceptErr = recordErr
	listeners := append([]net.Listener(nil), server.listeners...)
	server.mu.Unlock()

	for _, listener := range listeners {
		server.startListener(ctx, listener, recordErr)
	}

	server.listenersWaitGroup.Wait()
	close(serveDone)
	server.stopAll()
	server.connectionsWaitGroup.Wait()
	return acceptErr
}

func newConnection(server *Server, socket net.Conn) *connection {
	connection := &connection{
		server:           server,
		socket:           socket,
		controlQueue:     make(chan controlMessage, server.config.ControlQueueCapacity),
		deliveryWake:     make(chan struct{}, 1),
		stop:             make(chan struct{}),
		writerDone:       make(chan struct{}),
		disconnectReason: session.DisconnectAbnormal,
		inbound:          make(map[mqtt.PacketIdentifier]inboundState),
		outbound:         make(map[mqtt.PacketIdentifier]deliveryqueue.OutboundDTO),
		packetIDs:        newPacketIDAllocator(),
	}
	return connection
}

func (server *Server) addActive(connection *connection) {
	server.mu.Lock()
	if server.stopped {
		server.mu.Unlock()
		connection.requestClose()
		return
	}
	server.active[connection] = struct{}{}
	server.mu.Unlock()
}

func (server *Server) removeActive(connection *connection) {
	server.mu.Lock()
	delete(server.active, connection)
	server.mu.Unlock()
}

func (server *Server) bindSession(sessionID routing.SessionID, connection *connection) bool {
	server.mu.Lock()
	defer server.mu.Unlock()
	if existing, exists := server.bySession[sessionID]; exists && existing != connection {
		return false
	}
	server.bySession[sessionID] = connection
	return true
}

func (server *Server) unbindSession(sessionID routing.SessionID, connection *connection) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if existing, exists := server.bySession[sessionID]; exists && existing == connection {
		delete(server.bySession, sessionID)
	}
}

func (server *Server) wakeSession(sessionID routing.SessionID) {
	server.mu.Lock()
	connection := server.bySession[sessionID]
	server.mu.Unlock()
	if connection != nil {
		connection.wakeDelivery()
	}
}

func (server *Server) closeSession(sessionID routing.SessionID) {
	server.mu.Lock()
	connection := server.bySession[sessionID]
	server.mu.Unlock()
	if connection != nil {
		connection.requestClose()
	}
}

// DeliverDurable queues one already-committed QoS 1/2 delivery on its live owner.
func (server *Server) DeliverDurable(delivery deliveryqueue.DeliveryDTO) error {
	if server == nil || delivery.DurableSequence == 0 || delivery.DurableEpoch == 0 ||
		(delivery.QoS != mqtt.QoSAtLeastOnce && delivery.QoS != mqtt.QoSExactlyOnce) {
		return deliveryqueue.ErrUnavailable
	}
	if err := server.deliveries.TryEnqueue(delivery, server.config.Clock.Now()); err != nil {
		if errors.Is(err, deliveryqueue.ErrFull) {
			server.closeSession(delivery.SessionID)
		}
		return err
	}
	server.wakeSession(delivery.SessionID)
	return nil
}

// CloseSessionForTakeover closes the local socket after a committed cluster
// ownership change, without treating takeover as an abnormal client failure.
func (server *Server) CloseSessionForTakeover(sessionID routing.SessionID) bool {
	if server == nil {
		return false
	}
	server.mu.Lock()
	connection := server.bySession[sessionID]
	if connection != nil {
		delete(server.bySession, sessionID)
		connection.takeover.Store(true)
	}
	server.mu.Unlock()
	if connection == nil {
		return false
	}
	connection.requestClose()
	return true
}

func (server *Server) stopAll() {
	server.stopOnce.Do(func() {
		server.mu.Lock()
		server.stopped = true
		listeners := append([]net.Listener(nil), server.listeners...)
		connections := make([]*connection, 0, len(server.active))
		for connection := range server.active {
			connections = append(connections, connection)
		}
		server.mu.Unlock()
		for _, listener := range listeners {
			_ = listener.Close()
		}
		for _, connection := range connections {
			connection.requestClose()
		}
		if server.connections != nil {
			_ = server.connections.Close()
		}
	})
}

type controlMessage struct {
	packet  mqtt.Packet
	done    chan error
	barrier bool
}
type inboundState struct {
	state      delivery.Inbound
	receivedAt time.Time
}

type connection struct {
	server       *Server
	socket       net.Conn
	controlQueue chan controlMessage
	deliveryWake chan struct{}
	stop         chan struct{}
	writerDone   chan struct{}

	stopOnce         sync.Once
	cleanupOnce      sync.Once
	sessionID        routing.SessionID
	principal        security.Principal
	version          mqtt.ProtocolVersion
	keepAlive        uint16
	accepted         bool
	ownership        subscribe.SessionOwnership
	disconnectReason session.DisconnectReason
	recovered        []deliveryqueue.OutboundDTO
	deliveryBlocked  atomic.Bool
	takeover         atomic.Bool

	deliveryMu sync.Mutex
	inbound    map[mqtt.PacketIdentifier]inboundState
	outbound   map[mqtt.PacketIdentifier]deliveryqueue.OutboundDTO
	packetIDs  *packetIDAllocator
}

func (connection *connection) metrics() portsmetrics.MetricsRecorder {
	if connection != nil && connection.server != nil {
		return connection.server.config.Metrics
	}
	return nil
}

func (connection *connection) run() {
	go connection.writerLoop()
	_ = connection.readLoop()
	connection.cleanup()
	connection.requestClose()
	<-connection.writerDone
	connection.server.removeActive(connection)
}

func (connection *connection) requestClose() {
	connection.stopOnce.Do(func() {
		close(connection.stop)
		_ = connection.socket.Close()
	})
}

func (connection *connection) isStopping() bool {
	select {
	case <-connection.stop:
		return true
	default:
		return false
	}
}

func (connection *connection) cleanup() {
	connection.cleanupOnce.Do(func() {
		if connection.accepted {
			proto := portsmetrics.ProtocolMQTT50
			if connection.version == mqtt.ProtocolV311 {
				proto = portsmetrics.ProtocolMQTT311
			}
			transport := portsmetrics.TransportTCP
			if _, isTLS := connection.socket.(*tls.Conn); isTLS {
				transport = portsmetrics.TransportTLS
			}
			if metrics := connection.metrics(); metrics != nil {
				metrics.ConnectionClosed(proto, transport)
			}
			connection.server.unbindSession(connection.sessionID, connection)
			disconnectReason := connection.disconnectReason
			if connection.takeover.Load() {
				disconnectReason = session.DisconnectNormal
			}
			if err := connection.server.connections.Disconnect(connection.sessionID, disconnectReason); err != nil && !errors.Is(err, connect.ErrSessionNotFound) {
				connection.server.logger.Warn("MQTT session cleanup failed", "error", err.Error())
			}
		}
		connection.deliveryMu.Lock()
		clear(connection.inbound)
		clear(connection.outbound)
		connection.deliveryMu.Unlock()
	})
}

func (connection *connection) readLoop() error {
	input := make([]byte, connection.server.config.MaxPacketSize+5)
	if err := connection.socket.SetReadDeadline(time.Now().Add(connection.server.config.ConnectTimeout)); err != nil {
		return err
	}
	buffered := 0
	connected := false
	version := mqtt.ProtocolUnknown
	for {
		if buffered == len(input) {
			return mqtt.ErrPacketTooLarge
		}
		readCount, readErr := connection.socket.Read(input[buffered:])
		buffered += readCount
		for buffered > 0 {
			packet, consumed, err := connection.server.decoder.Decode(input[:buffered], version)
			if errors.Is(err, mqtt.ErrIncompletePacket) {
				if buffered == len(input) {
					return mqtt.ErrPacketTooLarge
				}
				break
			}
			if err != nil {
				if !connected {
					if handled := connection.handleUnsupportedVersion(err); handled {
						return nil
					}
				}
				return err
			}
			if consumed <= 0 || consumed > buffered {
				return mqtt.ErrMalformedPacket
			}
			if metrics := connection.metrics(); metrics != nil {
				metrics.PacketReceived(packetTypeName(packet))
			}
			copy(input, input[consumed:buffered])
			buffered -= consumed
			if !connected {
				connectPacket, ok := packet.(*mqtt.Connect)
				if !ok {
					return ErrProtocol
				}
				if err := connection.activate(*connectPacket); err != nil {
					return err
				}
				if connection.isStopping() {
					return nil
				}
				connected = true
				version = connection.version
			} else if err := connection.dispatch(packet); err != nil {
				return err
			}
			if connection.isStopping() {
				return nil
			}
			if err := connection.refreshReadDeadline(); err != nil {
				return err
			}
			if buffered == 0 {
				break
			}
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (connection *connection) handleUnsupportedVersion(err error) bool {
	var unsupported *mqtt.UnsupportedProtocolVersionError
	if !errors.As(err, &unsupported) || unsupported == nil {
		return false
	}
	if unsupported.ProtocolName == "MQIsdp" && unsupported.Level == 3 {
		if metrics := connection.metrics(); metrics != nil {
			metrics.ConnectRejected(portsmetrics.ProtocolMQTT310, portsmetrics.ConnectRejectReasonUnsupportedProtocolVersion)
		}
		connection.server.logger.Warn(
			"unsupported MQTT protocol",
			"reason", portsmetrics.ConnectRejectReasonUnsupportedProtocolVersion,
			"protocol", portsmetrics.ProtocolMQTT310,
		)
		connection.requestClose()
		return true
	}
	version := mqtt.ProtocolV311
	reasonCode := byte(0x01)
	if unsupported.ProtocolName == "MQTT" && unsupported.Level == 3 {
		if metrics := connection.metrics(); metrics != nil {
			metrics.ConnectRejected(portsmetrics.ProtocolMQTT310, portsmetrics.ConnectRejectReasonUnsupportedProtocolVersion)
		}
		connection.server.logger.Warn(
			"unsupported MQTT protocol",
			"reason", portsmetrics.ConnectRejectReasonUnsupportedProtocolVersion,
			"protocol", portsmetrics.ProtocolMQTT310,
		)
	} else if unsupported.ProtocolName == "MQTT" && unsupported.Level == byte(mqtt.ProtocolV5) {
		version = mqtt.ProtocolV5
		reasonCode = 0x84
	}
	connection.server.logger.Warn("unsupported MQTT protocol", "reason", "unsupported_protocol_version")
	_ = connection.enqueueAndWait(&mqtt.ConnAck{Version: version, ReasonCode: reasonCode})
	connection.requestClose()
	return true
}

func (connection *connection) activate(packet mqtt.Connect) error {
	if connection.server.config.ClusterMode && !connection.server.clusterReady() {
		reasonCode := byte(0x03)
		if packet.Version == mqtt.ProtocolV5 {
			reasonCode = 0x88
		}
		if err := connection.enqueueAndWait(&mqtt.ConnAck{Version: packet.Version, ReasonCode: reasonCode}); err != nil {
			return err
		}
		connection.requestClose()
		return nil
	}
	connection.deliveryBlocked.Store(true)
	var clientCertificate *applicationauthentication.ClientCertificateDTO
	if extractor := connection.server.config.ClientCertificateExtractor; extractor != nil {
		if extracted, ok := extractor(connection.socket); ok {
			clientCertificate = &extracted
		}
	}
	result, err := connection.server.connections.ConnectContext(context.Background(), connect.ConnectRequestDTO{
		Packet:            packet,
		ClientCertificate: clientCertificate,
	})
	if err != nil {
		return err
	}
	if !result.Accepted {
		if err := connection.enqueueAndWait(&result.ConnAck); err != nil {
			return err
		}
		connection.requestClose()
		return nil
	}
	connection.sessionID = result.SessionID
	connection.version = packet.Version
	connection.principal = result.Principal
	tenantID := "default"
	if result.Principal.Subject() != "" {
		tenantID = result.Principal.TenantID().String()
	}
	connection.ownership = subscribe.SessionOwnership{
		TenantID: tenantID, ClientID: packet.ClientID,
		OwnerNodeID: connection.server.config.LocalMemberID, Epoch: result.OwnershipEpoch,
	}
	connection.accepted = true
	if result.DeliverySession != nil {
		if err := connection.restoreInbound(result.DeliverySession); err != nil {
			return err
		}
	} else if connection.server.config.ClusterMode {
		return ErrProtocol
	}
	connection.keepAlive = packet.KeepAlive
	if !connection.server.bindSession(connection.sessionID, connection) {
		connection.accepted = false
		_ = connection.server.connections.Disconnect(connection.sessionID, session.DisconnectAbnormal)
		return ErrProtocol
	}
	recovered, err := connection.server.deliveries.ListOutbound(connection.sessionID)
	if err != nil {
		return err
	}
	connection.deliveryMu.Lock()
	for _, outbound := range recovered {
		if outbound.SessionID != connection.sessionID || !connection.packetIDs.reserve(outbound.PacketID) {
			connection.deliveryMu.Unlock()
			return ErrProtocol
		}
		connection.outbound[outbound.PacketID] = outbound
		connection.recovered = append(connection.recovered, outbound)
	}
	sort.Slice(connection.recovered, func(left, right int) bool {
		return connection.recovered[left].PacketID < connection.recovered[right].PacketID
	})
	connection.deliveryMu.Unlock()
	proto := portsmetrics.ProtocolMQTT50
	if packet.Version == mqtt.ProtocolV311 {
		proto = portsmetrics.ProtocolMQTT311
	}
	transport := portsmetrics.TransportTCP
	if _, isTLS := connection.socket.(*tls.Conn); isTLS {
		transport = portsmetrics.TransportTLS
	}
	if metrics := connection.metrics(); metrics != nil {
		metrics.ConnectionOpened(proto, transport)
	}
	if err := connection.enqueueAndWait(&result.ConnAck); err != nil {
		return err
	}
	connection.deliveryBlocked.Store(false)
	connection.wakeDelivery()
	return nil
}
func (connection *connection) restoreInbound(session *membership.DeliverySessionStateDTO) error {
	if session == nil {
		return nil
	}
	now := connection.server.config.Clock.Now()
	for _, record := range session.Inbound {
		if record.PacketID == 0 || record.PacketID > 65535 {
			return ErrProtocol
		}
		packetID := mqtt.PacketIdentifier(record.PacketID)
		packet := mqtt.Publish{
			Version:  connection.version,
			Topic:    record.Topic,
			Payload:  record.Payload,
			Retain:   record.Retain,
			QoS:      mqtt.QoSExactlyOnce,
			PacketID: packetID,
		}
		receivedAt := now
		if record.ExpiresAtUnixMillis > 0 {
			receivedAt = time.UnixMilli(int64(record.ExpiresAtUnixMillis))
			packet.HasMessageExpiryInterval = true
			packet.MessageExpiryInterval = 0
		}
		state, _, err := delivery.NewInbound(packet)
		if err != nil {
			return ErrProtocol
		}
		state, _, err = state.Accept()
		if err != nil {
			return ErrProtocol
		}
		if record.Released {
			state, _, err = state.Receive(&mqtt.PubRel{Version: connection.version, PacketID: packetID})
			if err != nil {
				return ErrProtocol
			}
			state, _, err = state.Accept()
			if err != nil {
				return ErrProtocol
			}
		}
		if _, exists := connection.inbound[packetID]; exists {
			return ErrProtocol
		}
		connection.inbound[packetID] = inboundState{state: state, receivedAt: receivedAt}
	}
	return nil
}

func (connection *connection) dispatch(packet mqtt.Packet) error {
	switch value := packet.(type) {
	case *mqtt.Subscribe:
		if connection.server.config.ClusterMode && !connection.server.clusterReady() {
			return ErrClusterNotReady
		}
		result, err := connection.server.subscriptions.SubscribeCommitted(
			context.Background(), connection.sessionID, *value, connection.server.config.Clock.Now(),
			connection.ownership, connection.principal,
		)
		if err != nil {
			return err
		}
		if err := connection.enqueueAndWait(&result.Ack); err != nil {
			return err
		}
		if result.RetainedQueued > 0 {
			connection.wakeDelivery()
		}
		return nil
	case *mqtt.Unsubscribe:
		if connection.server.config.ClusterMode && !connection.server.clusterReady() {
			return ErrClusterNotReady
		}
		ack, err := connection.server.subscriptions.UnsubscribeCommitted(
			context.Background(), connection.sessionID, *value, connection.ownership, connection.principal,
		)
		if err != nil {
			return err
		}
		return connection.enqueueAndWait(&ack)
	case *mqtt.Publish:
		return connection.dispatchPublish(*value)
	case *mqtt.PubAck:
		return connection.dispatchPubAck(value)
	case *mqtt.PubRec:
		return connection.dispatchPubRec(value)
	case *mqtt.PubRel:
		return connection.dispatchPubRel(value)
	case *mqtt.PubComp:
		return connection.dispatchPubComp(value)
	case *mqtt.PingReq:
		return connection.enqueueControl(&mqtt.PingResp{Version: connection.version})
	case *mqtt.Disconnect:
		if value.ReasonCode == 0x00 {
			connection.disconnectReason = session.DisconnectNormal
		} else if value.ReasonCode == 0x04 {
			connection.disconnectReason = session.DisconnectWithWill
		} else {
			connection.disconnectReason = session.DisconnectAbnormal
		}
		if value.ReasonCode != 0 {
			connection.server.logger.Info("MQTT client disconnected with reason", "reason_code", value.ReasonCode)
		}
		if err := connection.enqueueBarrier(); err != nil {
			return err
		}
		connection.cleanup()
		connection.requestClose()
		return nil
	default:
		return ErrProtocol
	}
}

func (connection *connection) dispatchPublish(packet mqtt.Publish) error {
	if connection.principal.Subject() != "" {
		topic, err := tenancy.NewTopic(connection.principal.TenantID(), packet.Topic)
		if err != nil || connection.server.config.Policy.AuthorizeTopic(connection.principal, security.ActionPublish, topic) != nil {
			return connection.rejectUnauthorizedPublish(packet)
		}
	}
	if connection.server.config.ClusterMode && packet.QoS > mqtt.QoSAtMostOnce && !connection.server.publications.DurableEnabled() {
		if connection.version == mqtt.ProtocolV5 {
			if err := connection.enqueueAndWait(&mqtt.Disconnect{Version: connection.version, ReasonCode: 0x89}); err != nil {
				return err
			}
		}
		connection.requestClose()
		return nil
	}
	if connection.server.config.ClusterMode && !connection.server.clusterReady() {
		connection.requestClose()
		return nil
	}
	if packet.QoS == mqtt.QoSAtMostOnce {
		result, err := connection.server.publications.Publish(connection.sessionID, packet, connection.server.config.Clock.Now())
		if err != nil {
			if errors.Is(err, applicationretained.ErrFull) {
				if connection.version == mqtt.ProtocolV5 {
					if err := connection.enqueueAndWait(&mqtt.Disconnect{Version: connection.version, ReasonCode: 0x97}); err != nil {
						return err
					}
				}
				connection.cleanup()
				connection.requestClose()
				return nil
			}
			return err
		}
		for _, sessionID := range result.Delivered {
			connection.server.wakeSession(sessionID)
		}
		for _, sessionID := range result.Saturated {
			connection.server.closeSession(sessionID)
		}
		return nil
	}

	if packet.QoS == mqtt.QoSAtLeastOnce {
		if packet.PacketID == 0 {
			return ErrProtocol
		}
		receivedAt := connection.server.config.Clock.Now()
		connection.deliveryMu.Lock()
		if existing, exists := connection.inbound[packet.PacketID]; exists {
			nextState, action, err := existing.state.Receive(&packet)
			if err != nil {
				connection.deliveryMu.Unlock()
				return ErrProtocol
			}
			existing.state = nextState
			connection.inbound[packet.PacketID] = existing
			connection.deliveryMu.Unlock()
			if action.Send != nil {
				return connection.enqueueControl(action.Send)
			}
			return nil
		}

		state, action, err := delivery.NewInbound(packet)
		if err != nil {
			connection.deliveryMu.Unlock()
			return ErrProtocol
		}

		result, err := connection.server.publications.Publish(connection.sessionID, *action.Deliver, receivedAt)
		if err != nil {
			connection.deliveryMu.Unlock()
			if errors.Is(err, applicationretained.ErrFull) || errors.Is(err, publish.ErrDurableDeliveryCapacity) {
				if connection.version == mqtt.ProtocolV5 {
					return connection.enqueueControl(&mqtt.PubAck{
						Version:    connection.version,
						PacketID:   packet.PacketID,
						ReasonCode: 0x97,
					})
				}
				connection.cleanup()
				connection.requestClose()
				return nil
			}
			return err
		}

		acceptedState, ackAction, err := state.Accept()
		if err != nil {
			connection.deliveryMu.Unlock()
			return ErrProtocol
		}
		connection.inbound[packet.PacketID] = inboundState{state: acceptedState, receivedAt: receivedAt}
		connection.deliveryMu.Unlock()

		if ackAction.Send != nil {
			if err := connection.enqueueControl(ackAction.Send); err != nil {
				return err
			}
		}
		for _, sessionID := range result.Delivered {
			connection.server.wakeSession(sessionID)
		}
		for _, sessionID := range result.Saturated {
			connection.server.closeSession(sessionID)
		}
		return nil
	}

	if packet.QoS == mqtt.QoSExactlyOnce {
		if packet.PacketID == 0 {
			return ErrProtocol
		}
		receivedAt := connection.server.config.Clock.Now()
		connection.deliveryMu.Lock()
		if existing, exists := connection.inbound[packet.PacketID]; exists {
			nextState, action, err := existing.state.Receive(&packet)
			if err != nil {
				connection.deliveryMu.Unlock()
				return ErrProtocol
			}
			existing.state = nextState
			connection.inbound[packet.PacketID] = existing
			connection.deliveryMu.Unlock()
			if action.Send != nil {
				return connection.enqueueControl(action.Send)
			}
			return nil
		}

		if connection.server.config.ClusterMode {
			err := connection.server.publications.ReceiveQoS2(
				connection.sessionID, connection.ownership.OwnerNodeID, connection.ownership.Epoch,
				packet, receivedAt, connection.principal,
			)
			if err != nil {
				connection.deliveryMu.Unlock()
				if errors.Is(err, publish.ErrDurableDeliveryCapacity) {
					if connection.version == mqtt.ProtocolV5 {
						return connection.enqueueControl(&mqtt.PubRec{
							Version: connection.version, PacketID: packet.PacketID, ReasonCode: 0x97,
						})
					}
					connection.cleanup()
					connection.requestClose()
					return nil
				}
				return err
			}
		}
		state, _, err := delivery.NewInbound(packet)
		if err != nil {
			connection.deliveryMu.Unlock()
			return ErrProtocol
		}
		acceptedState, recAction, err := state.Accept()
		if err != nil {
			connection.deliveryMu.Unlock()
			return ErrProtocol
		}
		connection.inbound[packet.PacketID] = inboundState{state: acceptedState, receivedAt: receivedAt}
		connection.deliveryMu.Unlock()

		if recAction.Send != nil {
			return connection.enqueueControl(recAction.Send)
		}
		return nil
	}

	return ErrProtocol
}

func (connection *connection) dispatchPubRel(packet *mqtt.PubRel) error {
	if packet.PacketID == 0 {
		return ErrProtocol
	}
	connection.deliveryMu.Lock()
	entry, exists := connection.inbound[packet.PacketID]
	if !exists {
		connection.deliveryMu.Unlock()
		reasonCode := byte(0x92)
		return connection.enqueueControl(&mqtt.PubComp{
			Version:    connection.version,
			PacketID:   packet.PacketID,
			ReasonCode: reasonCode,
		})
	}

	nextState, action, err := entry.state.Receive(packet)
	if err != nil {
		connection.deliveryMu.Unlock()
		return ErrProtocol
	}

	if action.Deliver != nil {
		var result publish.ResultDTO
		var err error
		if connection.server.config.ClusterMode {
			result, err = connection.server.publications.PublishQoS2Release(
				connection.sessionID, connection.ownership.OwnerNodeID, connection.ownership.Epoch,
				*action.Deliver, entry.receivedAt, connection.principal,
			)
		} else {
			result, err = connection.server.publications.Publish(
				connection.sessionID, *action.Deliver, entry.receivedAt, connection.principal,
			)
		}
		if err != nil {
			connection.deliveryMu.Unlock()
			if errors.Is(err, applicationretained.ErrFull) {
				if connection.version == mqtt.ProtocolV5 {
					if err := connection.enqueueAndWait(&mqtt.Disconnect{Version: connection.version, ReasonCode: 0x97}); err != nil {
						return err
					}
				}
				connection.cleanup()
				connection.requestClose()
				return nil
			}
			return err
		}
		acceptedState, compAction, err := nextState.Accept()
		if err != nil {
			connection.deliveryMu.Unlock()
			return ErrProtocol
		}
		connection.inbound[packet.PacketID] = inboundState{state: acceptedState, receivedAt: entry.receivedAt}
		connection.deliveryMu.Unlock()

		if compAction.Send != nil {
			if err := connection.enqueueControl(compAction.Send); err != nil {
				return err
			}
		}
		for _, sessionID := range result.Delivered {
			connection.server.wakeSession(sessionID)
		}
		for _, sessionID := range result.Saturated {
			connection.server.closeSession(sessionID)
		}
		return nil
	}

	connection.inbound[packet.PacketID] = inboundState{state: nextState, receivedAt: entry.receivedAt}
	connection.deliveryMu.Unlock()
	if action.Send != nil {
		return connection.enqueueControl(action.Send)
	}
	return nil
}

func (connection *connection) dispatchPubAck(packet *mqtt.PubAck) error {
	if packet.PacketID == 0 {
		return ErrProtocol
	}
	connection.deliveryMu.Lock()
	outbound, exists := connection.outbound[packet.PacketID]
	if !exists {
		connection.deliveryMu.Unlock()
		return nil
	}
	nextState, action, err := outbound.State.Receive(packet)
	if err != nil {
		connection.deliveryMu.Unlock()
		return ErrProtocol
	}
	if action.Complete {
		if err := connection.server.deliveries.CompleteOutbound(connection.sessionID, packet.PacketID); err != nil {
			connection.deliveryMu.Unlock()
			return err
		}
		delete(connection.outbound, packet.PacketID)
		connection.packetIDs.release(packet.PacketID)
		connection.deliveryMu.Unlock()
		connection.wakeDelivery()
		return nil
	}
	outbound.State = nextState
	if err := connection.server.deliveries.ReplaceOutbound(outbound); err != nil {
		connection.deliveryMu.Unlock()
		return err
	}
	connection.outbound[packet.PacketID] = outbound
	connection.deliveryMu.Unlock()
	return nil
}

func (connection *connection) dispatchPubRec(packet *mqtt.PubRec) error {
	if packet.PacketID == 0 {
		return ErrProtocol
	}
	connection.deliveryMu.Lock()
	outbound, exists := connection.outbound[packet.PacketID]
	if !exists {
		connection.deliveryMu.Unlock()
		return nil
	}
	nextState, action, err := outbound.State.Receive(packet)
	if err != nil {
		connection.deliveryMu.Unlock()
		return ErrProtocol
	}
	if action.Complete {
		if err := connection.server.deliveries.CompleteOutbound(connection.sessionID, packet.PacketID); err != nil {
			connection.deliveryMu.Unlock()
			return err
		}
		delete(connection.outbound, packet.PacketID)
		connection.packetIDs.release(packet.PacketID)
		connection.deliveryMu.Unlock()
		connection.wakeDelivery()
		return nil
	}
	outbound.State = nextState
	if err := connection.server.deliveries.ReplaceOutbound(outbound); err != nil {
		connection.deliveryMu.Unlock()
		return err
	}
	connection.outbound[packet.PacketID] = outbound
	toSend := action.Send
	if pubrel, ok := toSend.(*mqtt.PubRel); ok {
		pubrel.Version = connection.version
	}
	connection.deliveryMu.Unlock()

	if toSend != nil {
		return connection.enqueueControl(toSend)
	}
	return nil
}

func (connection *connection) dispatchPubComp(packet *mqtt.PubComp) error {
	if packet.PacketID == 0 {
		return ErrProtocol
	}
	connection.deliveryMu.Lock()
	outbound, exists := connection.outbound[packet.PacketID]
	if !exists {
		connection.deliveryMu.Unlock()
		return nil
	}
	nextState, action, err := outbound.State.Receive(packet)
	if err != nil {
		connection.deliveryMu.Unlock()
		return ErrProtocol
	}
	if action.Complete {
		if err := connection.server.deliveries.CompleteOutbound(connection.sessionID, packet.PacketID); err != nil {
			connection.deliveryMu.Unlock()
			return err
		}
		delete(connection.outbound, packet.PacketID)
		connection.packetIDs.release(packet.PacketID)
		connection.deliveryMu.Unlock()
		connection.wakeDelivery()
		return nil
	}
	outbound.State = nextState
	if err := connection.server.deliveries.ReplaceOutbound(outbound); err != nil {
		connection.deliveryMu.Unlock()
		return err
	}
	connection.outbound[packet.PacketID] = outbound
	connection.deliveryMu.Unlock()
	return nil
}

func (connection *connection) enqueueNegativePublishAck(packet mqtt.Publish) error {
	if connection.version != mqtt.ProtocolV5 {
		return ErrProtocol
	}
	if packet.QoS == mqtt.QoSAtLeastOnce {
		return connection.enqueueControl(&mqtt.PubAck{Version: connection.version, PacketID: packet.PacketID, ReasonCode: 0x83})
	}
	if packet.QoS == mqtt.QoSExactlyOnce {
		return connection.enqueueControl(&mqtt.PubRec{Version: connection.version, PacketID: packet.PacketID, ReasonCode: 0x83})
	}
	return ErrProtocol
}

func (connection *connection) rejectUnauthorizedPublish(packet mqtt.Publish) error {
	if connection.version == mqtt.ProtocolV5 {
		if packet.QoS == mqtt.QoSAtLeastOnce {
			return connection.enqueueControl(&mqtt.PubAck{Version: connection.version, PacketID: packet.PacketID, ReasonCode: 0x87})
		}
		if packet.QoS == mqtt.QoSExactlyOnce {
			return connection.enqueueControl(&mqtt.PubRec{Version: connection.version, PacketID: packet.PacketID, ReasonCode: 0x87})
		}
		return nil
	}
	connection.requestClose()
	return nil
}

func (connection *connection) enqueueControl(packet mqtt.Packet) error {
	if packet == nil {
		return ErrProtocol
	}
	select {
	case <-connection.stop:
		return ErrConnectionClosed
	default:
	}
	select {
	case <-connection.stop:
		return ErrConnectionClosed
	case connection.controlQueue <- controlMessage{packet: packet}:
		return nil
	default:
		connection.requestClose()
		return ErrControlQueueFull
	}
}

func (connection *connection) enqueueAndWait(packet mqtt.Packet) error {
	if packet == nil {
		return ErrProtocol
	}
	done := make(chan error, 1)
	select {
	case <-connection.stop:
		return ErrConnectionClosed
	default:
	}
	select {
	case <-connection.stop:
		return ErrConnectionClosed
	case connection.controlQueue <- controlMessage{packet: packet, done: done}:
	default:
		connection.requestClose()
		return ErrControlQueueFull
	}
	select {
	case err := <-done:
		return err
	case <-connection.stop:
		return ErrConnectionClosed
	}
}

func (connection *connection) enqueueBarrier() error {
	done := make(chan error, 1)
	select {
	case <-connection.stop:
		return ErrConnectionClosed
	default:
	}
	select {
	case <-connection.stop:
		return ErrConnectionClosed
	case connection.controlQueue <- controlMessage{done: done, barrier: true}:
	default:
		connection.requestClose()
		return ErrControlQueueFull
	}
	select {
	case err := <-done:
		return err
	case <-connection.stop:
		return ErrConnectionClosed
	}
}

func (connection *connection) wakeDelivery() {
	select {
	case <-connection.stop:
		return
	default:
	}
	select {
	case connection.deliveryWake <- struct{}{}:
	default:
	}
}

func (connection *connection) refreshReadDeadline() error {
	if connection.keepAlive == 0 {
		return connection.socket.SetReadDeadline(time.Time{})
	}
	interval := time.Duration(connection.keepAlive) * time.Second * 3 / 2
	return connection.socket.SetReadDeadline(time.Now().Add(interval))
}

func (connection *connection) writerLoop() {
	defer close(connection.writerDone)
	defer connection.failQueuedControls()
	for {
		if message, ok := connection.nextControl(); ok {
			if err := connection.writeMessage(message); err != nil {
				connection.requestClose()
				return
			}
			continue
		}
		select {
		case <-connection.stop:
			return
		case message := <-connection.controlQueue:
			if err := connection.writeMessage(message); err != nil {
				connection.requestClose()
				return
			}
		case <-connection.deliveryWake:
			if err := connection.writeDeliveries(); err != nil {
				connection.requestClose()
				return
			}
		}
	}
}

func (connection *connection) nextControl() (controlMessage, bool) {
	select {
	case message := <-connection.controlQueue:
		return message, true
	default:
		return controlMessage{}, false
	}
}

func (connection *connection) writeMessage(message controlMessage) error {
	var err error
	if !message.barrier {
		err = connection.writePacket(message.packet)
	}
	if message.done != nil {
		message.done <- err
	}
	return err

}
func (connection *connection) failQueuedControls() {
	for {
		select {
		case message := <-connection.controlQueue:
			if message.done != nil {
				message.done <- ErrConnectionClosed
			}
		default:
			return
		}
	}
}

func (connection *connection) writeDeliveries() error {
	if connection.deliveryBlocked.Load() {
		return nil
	}
	if err := connection.writeRecoveredDeliveries(); err != nil {
		return err
	}
	for {
		now := connection.server.config.Clock.Now()
		item, ok, err := connection.server.deliveries.Peek(connection.sessionID, now)
		if err != nil {
			if connection.isStopping() || errors.Is(err, deliveryqueue.ErrUnavailable) {
				return nil
			}
			return err
		}
		if !ok {
			return nil
		}
		remainingExpiryInterval, hasExpiry := item.RemainingExpiryInterval(now)
		if item.QoS == mqtt.QoSAtMostOnce {
			packet := mqtt.Publish{
				Version:                  connection.version,
				Topic:                    item.Topic,
				Payload:                  item.Payload,
				Retain:                   item.Retain,
				QoS:                      mqtt.QoSAtMostOnce,
				HasMessageExpiryInterval: connection.version == mqtt.ProtocolV5 && hasExpiry,
				MessageExpiryInterval:    remainingExpiryInterval,
			}
			if err := connection.writePacket(&packet); err != nil {
				return err
			}
			if err := connection.server.deliveries.CompleteHead(connection.sessionID); err != nil {
				return err
			}
			continue
		}
		if item.QoS > mqtt.QoSExactlyOnce {
			return ErrProtocol
		}

		connection.deliveryMu.Lock()
		packetID, err := connection.packetIDs.acquire()
		if err != nil {
			connection.deliveryMu.Unlock()
			return nil
		}
		publish := mqtt.Publish{
			Version:                  connection.version,
			Topic:                    item.Topic,
			Payload:                  item.Payload,
			Retain:                   item.Retain,
			QoS:                      item.QoS,
			PacketID:                 packetID,
			HasMessageExpiryInterval: connection.version == mqtt.ProtocolV5 && hasExpiry,
			MessageExpiryInterval:    remainingExpiryInterval,
		}
		state, action, err := delivery.NewOutbound(publish)
		if err != nil {
			connection.packetIDs.release(packetID)
			connection.deliveryMu.Unlock()
			return err
		}
		outbound := deliveryqueue.OutboundDTO{
			SessionID:       connection.sessionID,
			PacketID:        packetID,
			State:           state,
			ExpiresAt:       item.ExpiresAt,
			DurableSequence: item.DurableSequence,
			DurableEpoch:    item.DurableEpoch,
		}
		err = connection.server.deliveries.PromoteHead(outbound, now)
		switch {
		case err == nil:
			connection.outbound[packetID] = outbound
		case errors.Is(err, deliveryqueue.ErrExpired):
			connection.packetIDs.release(packetID)
			connection.deliveryMu.Unlock()
			continue
		case errors.Is(err, deliveryqueue.ErrFull):
			connection.packetIDs.release(packetID)
			connection.deliveryMu.Unlock()
			return nil
		case errors.Is(err, deliveryqueue.ErrPacketIDConflict):
			connection.packetIDs.release(packetID)
			connection.deliveryMu.Unlock()
			return ErrProtocol
		default:
			connection.packetIDs.release(packetID)
			connection.deliveryMu.Unlock()
			return err
		}
		connection.deliveryMu.Unlock()
		if err := connection.writePacket(action.Send); err != nil {
			return err
		}
	}
}

func (connection *connection) writeRecoveredDeliveries() error {
	connection.deliveryMu.Lock()
	recovered := connection.recovered
	connection.recovered = nil
	connection.deliveryMu.Unlock()
	for _, outbound := range recovered {
		action, err := outbound.State.Resume()
		if err != nil {
			return err
		}
		switch packet := action.Send.(type) {
		case *mqtt.Publish:
			now := connection.server.config.Clock.Now()
			if !outbound.ExpiresAt.IsZero() && !now.Before(outbound.ExpiresAt) {
				if err := connection.server.deliveries.CompleteOutbound(connection.sessionID, outbound.PacketID); err != nil {
					return err
				}
				connection.deliveryMu.Lock()
				delete(connection.outbound, outbound.PacketID)
				connection.packetIDs.release(outbound.PacketID)
				connection.deliveryMu.Unlock()
				continue
			}
			packet.Version = connection.version
			packet.DUP = true
			remainingExpiryInterval, hasExpiry := (deliveryqueue.DeliveryDTO{ExpiresAt: outbound.ExpiresAt}).RemainingExpiryInterval(now)
			packet.HasMessageExpiryInterval = connection.version == mqtt.ProtocolV5 && hasExpiry
			packet.MessageExpiryInterval = remainingExpiryInterval
			if err := connection.writePacket(packet); err != nil {
				return err
			}
		case *mqtt.PubRel:
			packet.Version = connection.version
			if err := connection.writePacket(packet); err != nil {
				return err
			}
		default:
			return ErrProtocol
		}
	}
	return nil
}

func (connection *connection) writePacket(packet mqtt.Packet) error {
	encoded, err := connection.server.encoder.Append(nil, packet)
	if err != nil {
		return err
	}
	if err := connection.socket.SetWriteDeadline(time.Now().Add(connection.server.config.WriteTimeout)); err != nil {
		return err
	}
	for len(encoded) > 0 {
		written, err := connection.socket.Write(encoded)
		if written > 0 {
			encoded = encoded[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	if metrics := connection.metrics(); metrics != nil {
		metrics.PacketSent(packetTypeName(packet))
	}
	return nil
}

func packetTypeName(packet mqtt.Packet) string {
	switch packet.(type) {
	case *mqtt.Connect:
		return "CONNECT"
	case *mqtt.ConnAck:
		return "CONNACK"
	case *mqtt.Publish:
		return "PUBLISH"
	case *mqtt.PubAck:
		return "PUBACK"
	case *mqtt.PubRec:
		return "PUBREC"
	case *mqtt.PubRel:
		return "PUBREL"
	case *mqtt.PubComp:
		return "PUBCOMP"
	case *mqtt.Subscribe:
		return "SUBSCRIBE"
	case *mqtt.SubAck:
		return "SUBACK"
	case *mqtt.Unsubscribe:
		return "UNSUBSCRIBE"
	case *mqtt.UnsubAck:
		return "UNSUBACK"
	case *mqtt.PingReq:
		return "PINGREQ"
	case *mqtt.PingResp:
		return "PINGRESP"
	case *mqtt.Disconnect:
		return "DISCONNECT"
	default:
		return "UNKNOWN"
	}
}

// PublishWill implements connect.WillPublisher by routing a will message through publications
// and waking or closing targeted subscriber sessions.
func (server *Server) PublishWill(sessionID routing.SessionID, will mqtt.Will) error {
	publishPacket := mqtt.Publish{
		Version: mqtt.ProtocolV5,
		QoS:     mqtt.QoS(will.QoS),
		Retain:  will.Retain,
		Topic:   will.Topic,
		Payload: will.Payload,
	}
	result, err := server.publications.Publish(sessionID, publishPacket, server.config.Clock.Now())
	if err != nil {
		return err
	}
	for _, targetSession := range result.Delivered {
		server.wakeSession(targetSession)
	}
	for _, targetSession := range result.Saturated {
		server.closeSession(targetSession)
	}
	return nil
}

// DeliverForwarded accepts a QoS 0 delivery from the cluster forwarding lane.
// A full registered-session queue closes that subscriber session.
func (server *Server) DeliverForwarded(delivery deliveryqueue.DeliveryDTO) error {
	if server == nil || server.publications == nil {
		return deliveryqueue.ErrUnavailable
	}
	err := server.publications.AcceptForwarded(delivery, server.config.Clock.Now())
	if err == nil {
		server.wakeSession(delivery.SessionID)
	}
	if errors.Is(err, deliveryqueue.ErrFull) {
		server.closeSession(delivery.SessionID)
	}
	return err
}

// WillBridge routes will publications from connect.UseCase to the active Server.
type WillBridge struct {
	mu     sync.RWMutex
	target connect.WillPublisher
}

// NewWillBridge creates an empty will publisher bridge.
func NewWillBridge() *WillBridge {
	return &WillBridge{}
}

// Bind connects the bridge to the concrete publisher (typically the TCP Server).
func (b *WillBridge) Bind(target connect.WillPublisher) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.target = target
}

// PublishWill forwards the will message to the bound publisher.
func (b *WillBridge) PublishWill(sessionID routing.SessionID, will mqtt.Will) error {
	b.mu.RLock()
	target := b.target
	b.mu.RUnlock()
	if target == nil {
		return nil
	}
	return target.PublishWill(sessionID, will)
}
