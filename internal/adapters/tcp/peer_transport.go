package tcp

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"narumq/internal/domain/release"
)

const (
	peerProtocolMajor  = 1
	peerProtocolMinor  = 0
	peerHelloFixedSize = 14
	peerHelloMaxSize   = 222
	peerMaxFrameSize   = 1 << 20
	peerQueueMessages  = 32
	peerQueueBytes     = 4 << 20
	peerIOTimeout      = 5 * time.Second
	peerRetryInterval  = 100 * time.Millisecond
)

const (
	dynamicRaftMagic         = "NMQRAFT2"
	dynamicForwardMagic      = "NMQFWRD2"
	dynamicForwardFrameLimit = (1 << 20) + (128 << 10)
	forwardQueueMessages     = 1024
	forwardQueueBytes        = 8 << 20
)

var (
	ErrPeerBackpressure    = errors.New("raft peer outbound queue is full")
	ErrPeerTransportClosed = errors.New("raft peer transport is closed")
	ErrInvalidPeerMessage  = errors.New("invalid raft peer message")
	ErrUnknownPeer         = errors.New("unknown raft peer")
)

// PeerMessageDTO is a transport-owned envelope around opaque peer stream bytes.
// Dynamic v2 inbound queue accounting is released by PeerInboundDelivery.Complete.
type PeerMessageDTO struct {
	From uint64
	To   uint64
	Data []byte
}

// PeerInboundDelivery holds one authenticated peer frame until its consumer completes processing.
type PeerInboundDelivery struct {
	Message    PeerMessageDTO
	EnqueuedAt time.Time
	peer       *dynamicPeerState
	frame      peerFrame
	id         uint64
}

// Complete releases the delivery and its inbound queue accounting.
func (delivery PeerInboundDelivery) Complete() {
	if delivery.peer != nil {
		delivery.peer.completeInboundDelivery(delivery.frame, delivery.id)
	}
}

// PeerEndpoint identifies one fixed voter and its network/TLS identity.
type PeerEndpoint struct {
	Address string
	DNSName string
}

// PeerTransportConfig contains the fixed-voter identity and listener/TLS configuration.
type PeerTransportConfig struct {
	NodeID       uint64
	ClusterID    string
	LocalDNSName string
	Listener     net.Listener
	ServerTLS    *tls.Config
	ClientTLS    *tls.Config
	Peers        map[uint64]PeerEndpoint
}

// PeerTransportStatusDTO reports bounded queue use and the last sanitized peer error.
type PeerTransportStatusDTO struct {
	PeerID                 uint64
	Connected              bool
	OutboundQueuedMessages int
	OutboundQueuedBytes    int
	InboundQueuedMessages  int
	InboundQueuedBytes     int
	RejectedMessages       uint64
	LastError              string
}

// TCPPeerTransport owns a fixed-voter TLS listener, peer queues, and network workers.
// It has no dependency on the Raft adapter or Raft protobuf types.
type TCPPeerTransport struct {
	localID   uint64
	clusterID string
	localDNS  string
	listener  net.Listener
	serverTLS *tls.Config
	clientTLS *tls.Config
	peers     map[uint64]*peerTransportState
	receive   chan PeerMessageDTO

	mu           sync.Mutex
	started      bool
	closed       bool
	ctx          context.Context
	cancel       context.CancelFunc
	stopOnce     sync.Once
	finishOnce   sync.Once
	done         chan struct{}
	workers      sync.WaitGroup
	connectionMu sync.Mutex
	connections  map[net.Conn]struct{}
}

type peerFrame struct {
	data             []byte
	size             int
	identityRevision uint64
	enqueuedAt       time.Time
}

type peerTransportState struct {
	owner    *TCPPeerTransport
	id       uint64
	endpoint PeerEndpoint

	mu         sync.Mutex
	wake       chan struct{}
	inbox      chan peerFrame
	connection net.Conn
	stopped    bool

	outbox      [peerQueueMessages]*peerFrame
	outHead     int
	outCount    int
	outCurrent  *peerFrame
	outMessages int
	outBytes    int
	inMessages  int
	inBytes     int
	rejected    uint64
	lastError   string

	sessionMu sync.Mutex
}

// NewTCPPeerTransport validates fixed-voter identities and creates a TLS listener.
// The listener and TLS configurations are owned by the returned transport.
func NewTCPPeerTransport(config PeerTransportConfig) (*TCPPeerTransport, error) {
	if config.NodeID < 1 || config.NodeID > 3 {
		return nil, fmt.Errorf("raft peer node ID must be in {1,2,3}")
	}
	if !validClusterID(config.ClusterID) {
		return nil, fmt.Errorf("raft peer cluster ID must be valid UTF-8 with 1..200 bytes")
	}
	if config.Listener == nil {
		return nil, fmt.Errorf("raft peer listener is required")
	}
	if !validDNSName(config.LocalDNSName) {
		return nil, fmt.Errorf("local peer DNS name is required")
	}
	if len(config.Peers) != 2 {
		return nil, fmt.Errorf("exactly two remote fixed-voter peers are required")
	}
	if _, ok := config.Peers[config.NodeID]; ok {
		return nil, fmt.Errorf("local node ID must not appear in peer list")
	}
	for peerID := uint64(1); peerID <= 3; peerID++ {
		if peerID == config.NodeID {
			continue
		}
		endpoint, ok := config.Peers[peerID]
		if !ok {
			return nil, fmt.Errorf("peer ID %d is required", peerID)
		}
		if err := validatePeerEndpoint(endpoint); err != nil {
			return nil, fmt.Errorf("peer ID %d: %w", peerID, err)
		}
	}
	serverTLS, err := validatedServerTLS(config.ServerTLS, config.LocalDNSName)
	if err != nil {
		return nil, fmt.Errorf("server TLS config: %w", err)
	}
	clientTLS, err := validatedClientTLS(config.ClientTLS, config.LocalDNSName)
	if err != nil {
		return nil, fmt.Errorf("client TLS config: %w", err)
	}

	transport := &TCPPeerTransport{
		localID:     config.NodeID,
		clusterID:   config.ClusterID,
		localDNS:    config.LocalDNSName,
		listener:    tls.NewListener(config.Listener, serverTLS),
		serverTLS:   serverTLS,
		clientTLS:   clientTLS,
		receive:     make(chan PeerMessageDTO),
		done:        make(chan struct{}),
		connections: make(map[net.Conn]struct{}),
		peers:       make(map[uint64]*peerTransportState, 2),
	}
	for peerID, endpoint := range config.Peers {
		transport.peers[peerID] = &peerTransportState{
			owner:    transport,
			id:       peerID,
			endpoint: endpoint,
			wake:     make(chan struct{}, 1),
			inbox:    make(chan peerFrame, peerQueueMessages),
		}
	}
	return transport, nil
}

// Serve starts peer workers and accepts the expected inbound connections until cancellation.
func (transport *TCPPeerTransport) Serve(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("peer transport context is required")
	}
	transport.mu.Lock()
	if transport.closed {
		transport.mu.Unlock()
		return ErrPeerTransportClosed
	}
	if transport.started {
		transport.mu.Unlock()
		return fmt.Errorf("peer transport Serve may only be called once")
	}
	transport.started = true
	transport.ctx, transport.cancel = context.WithCancel(ctx)
	runCtx := transport.ctx
	transport.mu.Unlock()

	stopContext := context.AfterFunc(ctx, transport.stop)
	for _, peer := range transport.peers {
		transport.workers.Add(1)
		go func(peer *peerTransportState) {
			defer transport.workers.Done()
			peer.deliverInbound(runCtx, transport.receive)
		}(peer)
		if peer.id > transport.localID {
			transport.workers.Add(1)
			go func(peer *peerTransportState) {
				defer transport.workers.Done()
				transport.dialPeer(runCtx, peer)
			}(peer)
		}
	}

	err := transport.acceptLoop(runCtx)
	stopContext()
	transport.stop()
	transport.workers.Wait()
	transport.finish()
	if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
		return nil
	}
	return err
}

// Send copies a local Raft message into the destination peer's bounded FIFO.
// Success means local queue admission only, not remote delivery or commitment.
func (transport *TCPPeerTransport) Send(message PeerMessageDTO) error {
	if message.From != transport.localID || message.To == transport.localID || len(message.Data) == 0 || len(message.Data) > peerMaxFrameSize {
		return ErrInvalidPeerMessage
	}
	peer, ok := transport.peers[message.To]
	if !ok {
		return ErrInvalidPeerMessage
	}
	frameSize := len(message.Data) + 4
	peer.mu.Lock()
	if peer.stopped {
		peer.mu.Unlock()
		return ErrPeerTransportClosed
	}
	if peer.outMessages >= peerQueueMessages || peer.outBytes+frameSize > peerQueueBytes {
		peer.rejected++
		peer.lastError = "outbound queue full"
		connection := peer.connection
		peer.mu.Unlock()
		if connection != nil {
			_ = connection.Close()
		}
		return ErrPeerBackpressure
	}
	frame := &peerFrame{data: append([]byte(nil), message.Data...), size: frameSize}
	tail := (peer.outHead + peer.outCount) % peerQueueMessages
	peer.outbox[tail] = frame
	peer.outCount++
	peer.outMessages++
	peer.outBytes += frameSize
	peer.mu.Unlock()
	peer.signal()
	return nil
}

// Receive returns inbound messages after authenticated framing. It closes after shutdown.
func (transport *TCPPeerTransport) Receive() <-chan PeerMessageDTO {
	return transport.receive
}

// Status returns per-peer queue and connection state without exposing raw network errors.
func (transport *TCPPeerTransport) Status(peerID uint64) (PeerTransportStatusDTO, error) {
	peer, ok := transport.peers[peerID]
	if !ok {
		return PeerTransportStatusDTO{}, ErrUnknownPeer
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return PeerTransportStatusDTO{
		PeerID:                 peer.id,
		Connected:              peer.connection != nil,
		OutboundQueuedMessages: peer.outMessages,
		OutboundQueuedBytes:    peer.outBytes,
		InboundQueuedMessages:  peer.inMessages,
		InboundQueuedBytes:     peer.inBytes,
		RejectedMessages:       peer.rejected,
		LastError:              peer.lastError,
	}, nil
}

// ResetPeer closes the active connection after an invalid Raft message.
// The reset is isolated to one fixed voter and does not clear queued outbound data.
func (transport *TCPPeerTransport) ResetPeer(peerID uint64) error {
	peer, ok := transport.peers[peerID]
	if !ok {
		return ErrUnknownPeer
	}
	peer.mu.Lock()
	if peer.stopped {
		peer.mu.Unlock()
		return ErrPeerTransportClosed
	}
	connection := peer.connection
	peer.connection = nil
	peer.rejected++
	peer.lastError = "invalid peer message"
	peer.mu.Unlock()
	if connection != nil {
		_ = connection.Close()
	}
	return nil
}

// Close cancels workers, closes owned sockets/listener, drains queue accounting, and waits.
func (transport *TCPPeerTransport) Close() error {
	transport.stop()
	<-transport.done
	return nil
}

func (transport *TCPPeerTransport) stop() {
	transport.stopOnce.Do(func() {
		transport.mu.Lock()
		transport.closed = true
		started := transport.started
		cancel := transport.cancel
		transport.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		_ = transport.listener.Close()
		transport.connectionMu.Lock()
		for connection := range transport.connections {
			_ = connection.Close()
		}
		transport.connectionMu.Unlock()
		for _, peer := range transport.peers {
			peer.stop()
		}
		if !started {
			transport.finish()
		}
	})
}

func (transport *TCPPeerTransport) finish() {
	transport.finishOnce.Do(func() {
		close(transport.receive)
		close(transport.done)
	})
}

func (transport *TCPPeerTransport) acceptLoop(ctx context.Context) error {
	incomingLimit := 0
	for peerID := range transport.peers {
		if peerID < transport.localID {
			incomingLimit++
		}
	}
	if incomingLimit == 0 {
		incomingLimit = 1
	}
	semaphore := make(chan struct{}, incomingLimit)
	for {
		socket, err := transport.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept raft peer connection: %w", err)
		}
		if !transport.trackConnection(socket) {
			_ = socket.Close()
			continue
		}
		select {
		case semaphore <- struct{}{}:
		default:
			transport.untrackConnection(socket)
			_ = socket.Close()
			continue
		}
		transport.workers.Add(1)
		go func(socket net.Conn) {
			defer transport.workers.Done()
			defer func() { <-semaphore }()
			defer transport.untrackConnection(socket)
			defer socket.Close()
			transport.handleInbound(ctx, socket)
		}(socket)
	}
}

func (transport *TCPPeerTransport) handleInbound(ctx context.Context, socket net.Conn) {
	connection, ok := socket.(*tls.Conn)
	if !ok {
		return
	}
	if err := setConnectionDeadline(connection); err != nil {
		return
	}
	handshakeCtx, cancel := context.WithTimeout(ctx, peerIOTimeout)
	err := connection.HandshakeContext(handshakeCtx)
	cancel()
	if err != nil {
		return
	}
	if err := writePeerHello(connection, transport.clusterID, transport.localID); err != nil {
		return
	}
	peerID, err := readPeerHello(connection, transport.clusterID)
	if err != nil {
		return
	}
	peer, ok := transport.peers[peerID]
	if !ok || peerID >= transport.localID {
		return
	}
	state := connection.ConnectionState()
	if len(state.PeerCertificates) == 0 || state.PeerCertificates[0].VerifyHostname(peer.endpoint.DNSName) != nil {
		peer.recordError("peer certificate identity mismatch")
		return
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		return
	}
	peer.recordConnected()
	peer.runConnection(ctx, connection)
}

func (transport *TCPPeerTransport) dialPeer(ctx context.Context, peer *peerTransportState) {
	for ctx.Err() == nil {
		clientTLS := transport.clientTLS.Clone()
		clientTLS.ServerName = peer.endpoint.DNSName
		dialCtx, cancel := context.WithTimeout(ctx, peerIOTimeout)
		dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: peerIOTimeout}, Config: clientTLS}
		socket, err := dialer.DialContext(dialCtx, "tcp", peer.endpoint.Address)
		cancel()
		if err != nil {
			peer.recordError("peer connection failed")
			if !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		connection, ok := socket.(*tls.Conn)
		if !ok {
			_ = socket.Close()
			peer.recordError("peer TLS handshake failed")
			if !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		if err := setConnectionDeadline(connection); err != nil {
			_ = connection.Close()
			peer.recordError("peer handshake failed")
			if !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		if err := writePeerHello(connection, transport.clusterID, transport.localID); err != nil {
			_ = connection.Close()
			peer.recordError("peer handshake failed")
			if !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		peerID, err := readPeerHello(connection, transport.clusterID)
		if err != nil || peerID != peer.id {
			_ = connection.Close()
			peer.recordError("peer identity mismatch")
			if !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		state := connection.ConnectionState()
		if len(state.PeerCertificates) == 0 || state.PeerCertificates[0].VerifyHostname(peer.endpoint.DNSName) != nil {
			_ = connection.Close()
			peer.recordError("peer certificate identity mismatch")
			if !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		if err := connection.SetDeadline(time.Time{}); err != nil {
			_ = connection.Close()
			peer.recordError("peer handshake failed")
			if !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		peer.recordConnected()
		peer.runConnection(ctx, connection)
		if ctx.Err() != nil {
			return
		}
		if !waitPeerRetry(ctx) {
			return
		}
	}
}

func (transport *TCPPeerTransport) trackConnection(connection net.Conn) bool {
	transport.mu.Lock()
	if transport.closed {
		transport.mu.Unlock()
		return false
	}
	transport.connectionMu.Lock()
	transport.connections[connection] = struct{}{}
	transport.connectionMu.Unlock()
	transport.mu.Unlock()
	return true
}

func (transport *TCPPeerTransport) untrackConnection(connection net.Conn) {
	transport.connectionMu.Lock()
	delete(transport.connections, connection)
	transport.connectionMu.Unlock()
}

func (peer *peerTransportState) runConnection(ctx context.Context, connection net.Conn) {
	peer.mu.Lock()
	if peer.stopped {
		peer.mu.Unlock()
		_ = connection.Close()
		return
	}
	previous := peer.connection
	peer.connection = connection
	peer.lastError = ""
	peer.mu.Unlock()
	if previous != nil && previous != connection {
		_ = previous.Close()
	}

	peer.sessionMu.Lock()
	defer peer.sessionMu.Unlock()
	peer.mu.Lock()
	current := peer.connection == connection && !peer.stopped
	peer.mu.Unlock()
	if !current {
		_ = connection.Close()
		return
	}

	readErr := make(chan error, 1)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			data, err := readPeerFrame(connection)
			if err != nil {
				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
					peer.recordError("peer read failed")
				}
				readErr <- err
				return
			}
			if err := peer.enqueueInbound(data); err != nil {
				peer.recordError("inbound queue full")
				readErr <- err
				return
			}
		}
	}()
	defer func() {
		_ = connection.Close()
		<-readDone
		peer.mu.Lock()
		if peer.connection == connection {
			peer.connection = nil
			if !peer.stopped && peer.lastError == "" {
				peer.lastError = "peer disconnected"
			}
		}
		peer.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-readErr:
			return
		default:
		}
		frame := peer.takeOutbound()
		if frame == nil {
			select {
			case <-ctx.Done():
				return
			case <-readErr:
				return
			case <-peer.wake:
				continue
			}
		}
		if err := writePeerFrame(connection, frame.data); err != nil {
			peer.recordError("peer write failed")
			return
		}
		peer.completeOutbound(frame)
	}
}

func (peer *peerTransportState) deliverInbound(ctx context.Context, destination chan<- PeerMessageDTO) {
	for {
		select {
		case <-ctx.Done():
			peer.drainInbound()
			return
		case frame := <-peer.inbox:
			message := PeerMessageDTO{From: peer.id, To: peer.owner.localID, Data: frame.data}
			select {
			case destination <- message:
				peer.completeInbound(frame)
			case <-ctx.Done():
				peer.completeInbound(frame)
				peer.drainInbound()
				return
			}
		}
	}
}

func (peer *peerTransportState) enqueueInbound(data []byte) error {
	frame := &peerFrame{data: data, size: len(data) + 4}
	peer.mu.Lock()
	if peer.stopped {
		peer.mu.Unlock()
		return ErrPeerTransportClosed
	}
	if peer.inMessages >= peerQueueMessages || peer.inBytes+frame.size > peerQueueBytes {
		peer.mu.Unlock()
		return ErrPeerBackpressure
	}
	peer.inMessages++
	peer.inBytes += frame.size
	peer.inbox <- *frame
	peer.mu.Unlock()
	return nil
}

func (peer *peerTransportState) completeInbound(frame peerFrame) {
	peer.mu.Lock()
	peer.inMessages--
	peer.inBytes -= frame.size
	peer.mu.Unlock()
}

func (peer *peerTransportState) drainInbound() {
	for {
		select {
		case frame := <-peer.inbox:
			peer.completeInbound(frame)
		default:
			return
		}
	}
}

func (peer *peerTransportState) takeOutbound() *peerFrame {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.stopped {
		return nil
	}
	if peer.outCurrent != nil {
		return peer.outCurrent
	}
	if peer.outCount == 0 {
		return nil
	}
	peer.outCurrent = peer.outbox[peer.outHead]
	peer.outbox[peer.outHead] = nil
	peer.outHead = (peer.outHead + 1) % peerQueueMessages
	peer.outCount--
	return peer.outCurrent
}

func (peer *peerTransportState) completeOutbound(frame *peerFrame) {
	peer.mu.Lock()
	if peer.outCurrent == frame {
		peer.outCurrent = nil
		peer.outMessages--
		peer.outBytes -= frame.size
	}
	peer.mu.Unlock()
}

func (peer *peerTransportState) signal() {
	select {
	case peer.wake <- struct{}{}:
	default:
	}
}

func (peer *peerTransportState) recordConnected() {
	peer.mu.Lock()
	peer.lastError = ""
	peer.mu.Unlock()
}

func (peer *peerTransportState) recordError(message string) {
	peer.mu.Lock()
	if !peer.stopped {
		peer.lastError = message
	}
	peer.mu.Unlock()
}

func (peer *peerTransportState) stop() {
	peer.mu.Lock()
	peer.stopped = true
	connection := peer.connection
	peer.connection = nil
	for index := range peer.outbox {
		peer.outbox[index] = nil
	}
	peer.outHead = 0
	peer.outCount = 0
	peer.outCurrent = nil
	peer.outMessages = 0
	peer.outBytes = 0
	peer.mu.Unlock()
	if connection != nil {
		_ = connection.Close()
	}
	peer.signal()
}

func writePeerHello(writer io.Writer, clusterID string, nodeID uint64) error {
	if !validClusterID(clusterID) || nodeID == 0 || nodeID > 3 {
		return ErrInvalidPeerMessage
	}
	var hello [peerHelloMaxSize]byte
	copy(hello[:8], "NMQRAFT1")
	binary.BigEndian.PutUint16(hello[8:10], peerProtocolMajor)
	binary.BigEndian.PutUint16(hello[10:12], peerProtocolMinor)
	binary.BigEndian.PutUint16(hello[12:14], uint16(len(clusterID)))
	position := peerHelloFixedSize
	position += copy(hello[position:], clusterID)
	binary.BigEndian.PutUint64(hello[position:position+8], nodeID)
	return writeFull(writer, hello[:position+8])
}

func readPeerHello(reader io.Reader, expectedClusterID string) (uint64, error) {
	var fixed [peerHelloFixedSize]byte
	if _, err := io.ReadFull(reader, fixed[:]); err != nil {
		return 0, fmt.Errorf("read peer hello header: %w", err)
	}
	if string(fixed[:8]) != "NMQRAFT1" || binary.BigEndian.Uint16(fixed[8:10]) != peerProtocolMajor || binary.BigEndian.Uint16(fixed[10:12]) != peerProtocolMinor {
		return 0, fmt.Errorf("peer protocol mismatch")
	}
	clusterLength := int(binary.BigEndian.Uint16(fixed[12:14]))
	if clusterLength < 1 || clusterLength > 200 {
		return 0, fmt.Errorf("invalid peer cluster ID length")
	}
	var variable [208]byte
	if _, err := io.ReadFull(reader, variable[:clusterLength+8]); err != nil {
		return 0, fmt.Errorf("read peer hello body: %w", err)
	}
	clusterID := variable[:clusterLength]
	if !utf8.Valid(clusterID) || string(clusterID) != expectedClusterID {
		return 0, fmt.Errorf("peer cluster ID mismatch")
	}
	peerID := binary.BigEndian.Uint64(variable[clusterLength : clusterLength+8])
	if peerID < 1 || peerID > 3 {
		return 0, fmt.Errorf("invalid peer node ID")
	}
	return peerID, nil
}

func readPeerFrame(reader net.Conn) ([]byte, error) {
	if err := reader.SetReadDeadline(time.Now().Add(peerIOTimeout)); err != nil {
		return nil, err
	}
	var prefix [4]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length == 0 || length > peerMaxFrameSize {
		return nil, fmt.Errorf("invalid peer frame length")
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func writePeerFrame(writer net.Conn, payload []byte) error {
	if len(payload) == 0 || len(payload) > peerMaxFrameSize {
		return ErrInvalidPeerMessage
	}
	if err := writer.SetWriteDeadline(time.Now().Add(peerIOTimeout)); err != nil {
		return err
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
	if err := writeFull(writer, prefix[:]); err != nil {
		return err
	}
	return writeFull(writer, payload)
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func setConnectionDeadline(connection net.Conn) error {
	return connection.SetDeadline(time.Now().Add(peerIOTimeout))
}

func waitPeerRetry(ctx context.Context) bool {
	timer := time.NewTimer(peerRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func validClusterID(clusterID string) bool {
	return len(clusterID) >= 1 && len(clusterID) <= 200 && utf8.ValidString(clusterID)
}

func validDNSName(name string) bool {
	return name != "" && name == strings.TrimSpace(name) && net.ParseIP(name) == nil && !strings.ContainsAny(name, " \t\r\n")
}

func validatePeerEndpoint(endpoint PeerEndpoint) error {
	if endpoint.Address == "" {
		return fmt.Errorf("peer TCP address is required")
	}
	host, port, err := net.SplitHostPort(endpoint.Address)
	if err != nil || host == "" {
		return fmt.Errorf("peer TCP address must be host:port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("peer TCP port must be between 1 and 65535")
	}
	if !validDNSName(endpoint.DNSName) {
		return fmt.Errorf("peer DNS name is required")
	}
	return nil
}

func validatedServerTLS(source *tls.Config, localDNSName string) (*tls.Config, error) {
	if source == nil || len(source.Certificates) == 0 || source.ClientCAs == nil || source.InsecureSkipVerify {
		return nil, fmt.Errorf("server certificate, trusted client CA, and certificate verification are required")
	}
	config := source.Clone()
	config.MinVersion = tls.VersionTLS13
	config.MaxVersion = tls.VersionTLS13
	config.ClientAuth = tls.RequireAndVerifyClientCert
	if err := validateLocalCertificate(config.Certificates[0], localDNSName); err != nil {
		return nil, err
	}
	return config, nil
}

func validatedClientTLS(source *tls.Config, localDNSName string) (*tls.Config, error) {
	if source == nil || len(source.Certificates) == 0 || source.RootCAs == nil || source.InsecureSkipVerify {
		return nil, fmt.Errorf("client certificate, trusted server CA, and certificate verification are required")
	}
	config := source.Clone()
	config.MinVersion = tls.VersionTLS13
	config.MaxVersion = tls.VersionTLS13
	config.ServerName = ""
	if err := validateLocalCertificate(config.Certificates[0], localDNSName); err != nil {
		return nil, err
	}
	return config, nil
}

func validateLocalCertificate(certificate tls.Certificate, localDNSName string) error {
	leaf := certificate.Leaf
	if leaf == nil {
		if len(certificate.Certificate) == 0 {
			return fmt.Errorf("local TLS certificate is missing")
		}
		parsed, err := x509.ParseCertificate(certificate.Certificate[0])
		if err != nil {
			return fmt.Errorf("parse local TLS certificate: %w", err)
		}
		leaf = parsed
	}
	if err := leaf.VerifyHostname(localDNSName); err != nil {
		return fmt.Errorf("local TLS certificate must contain DNS SAN %q", localDNSName)
	}
	return nil
}

const (
	dynamicProtocolMajor       = 2
	dynamicProtocolMinor       = 0
	dynamicSchemaMajor         = 1
	dynamicSchemaMinor         = 0
	dynamicHelloLimit          = 1024
	dynamicAdmissionLimit      = 64 << 10
	dynamicFrameLimit          = 1 << 20
	dynamicBootstrapProbeMagic = "NMQBOOT2"
)

// PeerPhaseDTO is a committed cluster-membership phase used by v2 peer transport.
type PeerPhaseDTO uint8

const (
	// PeerPhaseStaged identifies a committed admission record that is not yet a Raft peer.
	PeerPhaseStaged PeerPhaseDTO = 1
	// PeerPhaseLearner identifies a member added as a non-voting Raft learner.
	PeerPhaseLearner PeerPhaseDTO = 2
	// PeerPhaseCatchingUp identifies a learner applying its snapshot and log tail.
	PeerPhaseCatchingUp PeerPhaseDTO = 3
	// PeerPhaseVoter identifies a committed voting member.
	PeerPhaseVoter PeerPhaseDTO = 4
	// PeerPhaseLeaving identifies a voter undergoing planned ownership transfer.
	PeerPhaseLeaving PeerPhaseDTO = 5
	// PeerPhaseRetired identifies a removed, tombstoned member.
	PeerPhaseRetired PeerPhaseDTO = 6
)

// PeerIdentityDTO binds a peer address and protocol identity to its TLS leaf certificate.
type PeerIdentityDTO struct {
	MemberID             uint64
	ClusterID            string
	Address              string
	DNSSAN               string
	CertificateSHA256    [sha256.Size]byte
	ReleaseVersion       string
	ClusterProtocolMajor uint32
	ClusterProtocolMinor uint32
	StateSchemaMajor     uint32
	StateSchemaMinor     uint32
	Phase                PeerPhaseDTO
	OperationID          string
}

// SeedAdmissionRequestDTO carries the candidate identity proved during seed admission.
type SeedAdmissionRequestDTO struct {
	Candidate PeerIdentityDTO
}

// SeedAdmissionResponseDTO contains the committed stage and current voters returned to a joiner.
type SeedAdmissionResponseDTO struct {
	Accepted      bool
	Candidate     PeerIdentityDTO
	CurrentVoters []PeerIdentityDTO
}

// SeedAdmissionHandler resolves a candidate against committed staged membership.
// Returning an error or an unaccepted response sends only the generic rejection frame.
type SeedAdmissionHandler func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error)

// DynamicPeerTransportConfig configures the additive v2 TLS listener and committed peer set.
type DynamicPeerTransportConfig struct {
	LocalIdentity           PeerIdentityDTO
	Listener                net.Listener
	ServerTLS               *tls.Config
	ClientTLS               *tls.Config
	Peers                   []PeerIdentityDTO
	AdmissionHandler        SeedAdmissionHandler
	BootstrapProgress       BootstrapProgressHandler
	BootstrapAllowedPeerIDs []uint64
}

// DynamicTCPPeerTransport owns a dynamic v2 TLS listener and its reconciled peer workers.
type DynamicTCPPeerTransport struct {
	mu                 sync.Mutex
	local              PeerIdentityDTO
	localID            uint64
	clusterID          string
	listener           net.Listener
	serverTLS          *tls.Config
	clientTLS          *tls.Config
	admission          SeedAdmissionHandler
	bootstrapProgress  BootstrapProgressHandler
	bootstrapMu        sync.Mutex
	bootstrapAllowed   map[uint64]struct{}
	peers              map[uint64]*dynamicPeerState
	forwardPeers       map[uint64]*dynamicPeerState
	inboundReady       chan struct{}
	outboundReady      chan struct{}
	forwardReady       chan struct{}
	connections        map[net.Conn]struct{}
	inboundConnections int
	started            bool
	closed             bool
	ctx                context.Context
	cancel             context.CancelFunc
	workers            sync.WaitGroup
	done               chan struct{}
	finishOnce         sync.Once
}

type dynamicLane uint8

const (
	dynamicLaneRaft dynamicLane = iota
	dynamicLaneForward
)

func (lane dynamicLane) magic() string {
	if lane == dynamicLaneForward {
		return dynamicForwardMagic
	}
	return dynamicRaftMagic
}

func (lane dynamicLane) frameLimit() uint32 {
	if lane == dynamicLaneForward {
		return dynamicForwardFrameLimit
	}
	return dynamicFrameLimit
}

func (lane dynamicLane) queueMessages() int {
	if lane == dynamicLaneForward {
		return forwardQueueMessages
	}
	return peerQueueMessages
}

func (lane dynamicLane) queueBytes() int {
	if lane == dynamicLaneForward {
		return forwardQueueBytes
	}
	return peerQueueBytes
}

func (transport *DynamicTCPPeerTransport) lanePeersLocked(lane dynamicLane) map[uint64]*dynamicPeerState {
	if lane == dynamicLaneForward {
		return transport.forwardPeers
	}
	return transport.peers
}

type dynamicPeerState struct {
	owner            *DynamicTCPPeerTransport
	id               uint64
	identity         PeerIdentityDTO
	identityRevision uint64
	identityChanging bool
	lane             dynamicLane

	mu                      sync.Mutex
	wake                    chan struct{}
	inbox                   chan peerFrame
	deliveryWake            chan struct{}
	capacityWake            chan struct{}
	inboundDelivering       bool
	inboundDeliveryRevision uint64
	inboundDeliveryID       uint64
	connection              net.Conn
	pending                 net.Conn
	stopped                 bool
	ctx                     context.Context
	cancel                  context.CancelFunc

	outbox      []*peerFrame
	outHead     int
	outCount    int
	outCurrent  *peerFrame
	outMessages int
	outBytes    int
	inMessages  int
	inBytes     int
	rejected    uint64
	lastError   string
	sessionMu   sync.Mutex
}

// NewDynamicTCPPeerTransport validates v2 identities and creates a TLS 1.3 mTLS listener.
func NewDynamicTCPPeerTransport(config DynamicPeerTransportConfig) (*DynamicTCPPeerTransport, error) {
	if config.Listener == nil {
		return nil, fmt.Errorf("dynamic peer listener is required")
	}
	if config.AdmissionHandler == nil {
		return nil, fmt.Errorf("seed admission handler is required")
	}
	if err := validateDynamicInitialIdentity(config.LocalIdentity); err != nil {
		return nil, fmt.Errorf("local peer identity: %w", err)
	}
	serverTLS, err := validatedServerTLS(config.ServerTLS, config.LocalIdentity.DNSSAN)
	if err != nil {
		return nil, fmt.Errorf("server TLS config: %w", err)
	}
	if err := validateDynamicCertificate(serverTLS.Certificates[0], config.LocalIdentity); err != nil {
		return nil, fmt.Errorf("server TLS identity: %w", err)
	}
	clientTLS, err := validatedClientTLS(config.ClientTLS, config.LocalIdentity.DNSSAN)
	if err != nil {
		return nil, fmt.Errorf("client TLS config: %w", err)
	}
	if err := validateDynamicCertificate(clientTLS.Certificates[0], config.LocalIdentity); err != nil {
		return nil, fmt.Errorf("client TLS identity: %w", err)
	}

	transport := &DynamicTCPPeerTransport{
		local:             config.LocalIdentity,
		localID:           config.LocalIdentity.MemberID,
		clusterID:         config.LocalIdentity.ClusterID,
		listener:          tls.NewListener(config.Listener, serverTLS),
		serverTLS:         serverTLS,
		clientTLS:         clientTLS,
		admission:         config.AdmissionHandler,
		bootstrapProgress: config.BootstrapProgress,
		bootstrapAllowed:  make(map[uint64]struct{}, len(config.BootstrapAllowedPeerIDs)),
		peers:             make(map[uint64]*dynamicPeerState),
		forwardPeers:      make(map[uint64]*dynamicPeerState),
		inboundReady:      make(chan struct{}, 1),
		outboundReady:     make(chan struct{}, 1),
		forwardReady:      make(chan struct{}, 1),
		connections:       make(map[net.Conn]struct{}),
		done:              make(chan struct{}),
	}
	if err := validateDynamicPeerSet(config.LocalIdentity, config.Peers); err != nil {
		_ = transport.listener.Close()
		return nil, err
	}
	for _, id := range config.BootstrapAllowedPeerIDs {
		transport.bootstrapAllowed[id] = struct{}{}
	}
	for _, identity := range config.Peers {
		transport.peers[identity.MemberID] = newDynamicLanePeerState(transport, identity, dynamicLaneRaft)
		transport.forwardPeers[identity.MemberID] = newDynamicLanePeerState(transport, identity, dynamicLaneForward)
	}
	return transport, nil
}

// Serve starts dynamic peer workers and accepts admission, Raft and forwarding connections.
func (transport *DynamicTCPPeerTransport) Serve(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("peer transport context is required")
	}
	transport.mu.Lock()
	if transport.closed {
		transport.mu.Unlock()
		return ErrPeerTransportClosed
	}
	if transport.started {
		transport.mu.Unlock()
		return fmt.Errorf("peer transport Serve may only be called once")
	}
	transport.started = true
	transport.ctx, transport.cancel = context.WithCancel(ctx)
	runCtx := transport.ctx
	for _, peer := range transport.peers {
		transport.startPeerLocked(peer)
	}
	for _, peer := range transport.forwardPeers {
		transport.startPeerLocked(peer)
	}
	transport.mu.Unlock()

	stopContext := context.AfterFunc(ctx, transport.shutdown)
	err := transport.acceptLoop(runCtx)
	stopContext()
	transport.shutdown()
	transport.workers.Wait()
	transport.finish()
	if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
		return nil
	}
	return err
}

// Send copies a local Raft message into the destination peer's bounded v2 FIFO.
func (transport *DynamicTCPPeerTransport) Send(message PeerMessageDTO) error {
	return transport.sendLane(message, dynamicLaneRaft)
}

// Forward copies a live delivery frame into the destination peer's bounded forwarding FIFO.
func (transport *DynamicTCPPeerTransport) Forward(message PeerMessageDTO) error {
	return transport.sendLane(message, dynamicLaneForward)
}

func (transport *DynamicTCPPeerTransport) sendLane(message PeerMessageDTO, lane dynamicLane) error {
	transport.mu.Lock()
	peer, ok := transport.lanePeersLocked(lane)[message.To]
	localID := transport.local.MemberID
	localPhase := transport.local.Phase
	closed := transport.closed
	transport.mu.Unlock()
	if closed {
		return ErrPeerTransportClosed
	}
	if !ok {
		return fmt.Errorf("%w: destination peer %d is not registered", ErrInvalidPeerMessage, message.To)
	}
	if localPhase == PeerPhaseStaged {
		if lane == dynamicLaneRaft {
			return fmt.Errorf("%w: staged local member %d cannot send Raft messages", ErrInvalidPeerMessage, localID)
		}
		return fmt.Errorf("%w: staged local member %d cannot send forwarding frames", ErrInvalidPeerMessage, localID)
	}
	if message.From != localID {
		return fmt.Errorf("%w: message source %d does not match local member %d", ErrInvalidPeerMessage, message.From, localID)
	}
	if message.To == localID {
		return fmt.Errorf("%w: message destination is the local member %d", ErrInvalidPeerMessage, localID)
	}
	if len(message.Data) == 0 || uint64(len(message.Data)) > uint64(lane.frameLimit()) {
		return fmt.Errorf("%w: peer payload length %d is outside the allowed range", ErrInvalidPeerMessage, len(message.Data))
	}
	frameSize := len(message.Data) + 4
	peer.mu.Lock()
	if peer.stopped {
		peer.mu.Unlock()
		return ErrPeerTransportClosed
	}
	if peer.outMessages >= lane.queueMessages() || peer.outBytes+frameSize > lane.queueBytes() {
		peer.rejected++
		if peer.lastError == "" {
			peer.lastError = "outbound queue full"
		}
		peer.mu.Unlock()
		return ErrPeerBackpressure
	}
	frame := &peerFrame{data: append([]byte(nil), message.Data...), size: frameSize}
	tail := (peer.outHead + peer.outCount) % len(peer.outbox)
	peer.outbox[tail] = frame
	peer.outCount++
	peer.outMessages++
	peer.outBytes += frameSize
	peer.mu.Unlock()
	peer.signal()
	return nil
}

// InboundReady is signaled when one or more authenticated Raft frames are staged.
func (transport *DynamicTCPPeerTransport) InboundReady() <-chan struct{} {
	return transport.inboundReady
}

// ForwardReady is signaled when one or more authenticated forwarding frames are staged.
func (transport *DynamicTCPPeerTransport) ForwardReady() <-chan struct{} {
	return transport.forwardReady
}

// TakeInbound leases one staged Raft frame to its consumer. Complete the delivery after processing it.
func (transport *DynamicTCPPeerTransport) TakeInbound() (PeerInboundDelivery, bool) {
	return transport.takeLane(dynamicLaneRaft)
}

// TakeForward leases one staged forwarding frame to its consumer. Complete it after processing.
func (transport *DynamicTCPPeerTransport) TakeForward() (PeerInboundDelivery, bool) {
	return transport.takeLane(dynamicLaneForward)
}

func (transport *DynamicTCPPeerTransport) takeLane(lane dynamicLane) (PeerInboundDelivery, bool) {
	transport.mu.Lock()
	if transport.closed {
		transport.mu.Unlock()
		return PeerInboundDelivery{}, false
	}
	for _, peer := range transport.lanePeersLocked(lane) {
		peer.mu.Lock()
		if peer.stopped || peer.identityChanging || peer.inboundDelivering {
			peer.mu.Unlock()
			continue
		}
		select {
		case frame := <-peer.inbox:
			if frame.identityRevision != peer.identityRevision {
				peer.completeInboundLocked(frame)
				peer.mu.Unlock()
				continue
			}
			peer.inboundDeliveryID++
			if peer.inboundDeliveryID == 0 {
				peer.inboundDeliveryID++
			}
			peer.inboundDelivering = true
			peer.inboundDeliveryRevision = frame.identityRevision
			delivery := PeerInboundDelivery{
				Message:    PeerMessageDTO{From: peer.id, To: transport.localID, Data: frame.data},
				EnqueuedAt: frame.enqueuedAt,
				peer:       peer,
				frame:      frame,
				id:         peer.inboundDeliveryID,
			}
			peer.mu.Unlock()
			transport.signalLane(lane)
			transport.mu.Unlock()
			return delivery, true
		default:
			peer.mu.Unlock()
		}
	}
	transport.mu.Unlock()
	return PeerInboundDelivery{}, false
}

func (transport *DynamicTCPPeerTransport) signalLane(lane dynamicLane) {
	ready := transport.inboundReady
	if lane == dynamicLaneForward {
		ready = transport.forwardReady
	}
	if ready == nil {
		return
	}
	select {
	case ready <- struct{}{}:
	default:
	}
}

// Status returns queue use and sanitized connection state for a committed peer.
func (transport *DynamicTCPPeerTransport) Status(peerID uint64) (PeerTransportStatusDTO, error) {
	transport.mu.Lock()
	peer, ok := transport.peers[peerID]
	transport.mu.Unlock()
	if !ok {
		return PeerTransportStatusDTO{}, ErrUnknownPeer
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return PeerTransportStatusDTO{
		PeerID: peer.id, Connected: peer.connection != nil,
		OutboundQueuedMessages: peer.outMessages, OutboundQueuedBytes: peer.outBytes,
		InboundQueuedMessages: peer.inMessages, InboundQueuedBytes: peer.inBytes,
		RejectedMessages: peer.rejected, LastError: peer.lastError,
	}, nil
}

// ResetPeer closes one peer socket after an invalid Raft message.
func (transport *DynamicTCPPeerTransport) ResetPeer(peerID uint64) error {
	transport.mu.Lock()
	peer, ok := transport.peers[peerID]
	closed := transport.closed
	transport.mu.Unlock()
	if !ok {
		return ErrUnknownPeer
	}
	if closed {
		return ErrPeerTransportClosed
	}
	peer.mu.Lock()
	connection := peer.connection
	peer.connection = nil
	peer.signalInboundCapacityLocked()
	peer.rejected++
	peer.lastError = "invalid peer message"
	peer.mu.Unlock()
	if connection != nil {
		_ = connection.Close()
	}
	return nil
}

// LocalIdentity returns a copy of the current local peer identity.
func (transport *DynamicTCPPeerTransport) LocalIdentity() PeerIdentityDTO {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.local
}

// UpdateLocalIdentity applies a committed local phase or operation-ID change.
// Identity, address and certificate bindings are immutable after construction.
func (transport *DynamicTCPPeerTransport) UpdateLocalIdentity(identity PeerIdentityDTO) error {
	if err := validateDynamicLocalIdentity(identity); err != nil {
		return err
	}
	transport.mu.Lock()
	if transport.closed {
		transport.mu.Unlock()
		return ErrPeerTransportClosed
	}
	current := transport.local
	if !sameImmutableIdentity(current, identity) {
		transport.mu.Unlock()
		return fmt.Errorf("local peer identity binding cannot change")
	}
	if localPeerPhaseRank(identity.Phase) < localPeerPhaseRank(current.Phase) {
		transport.mu.Unlock()
		return nil
	}
	if current.OperationID != "" && identity.Phase != PeerPhaseVoter &&
		identity.Phase != PeerPhaseLeaving && identity.OperationID != current.OperationID {
		transport.mu.Unlock()
		return fmt.Errorf("local peer membership operation cannot change")
	}
	if samePeerIdentity(current, identity) {
		transport.mu.Unlock()
		return nil
	}
	transport.local = identity
	connections := make([]net.Conn, 0, len(transport.connections))
	for connection := range transport.connections {
		connections = append(connections, connection)
	}
	transport.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
	return nil
}

func localPeerPhaseRank(phase PeerPhaseDTO) int {
	switch phase {
	case PeerPhaseStaged:
		return 1
	case PeerPhaseLearner:
		return 2
	case PeerPhaseCatchingUp:
		return 3
	case PeerPhaseVoter:
		return 4
	case PeerPhaseLeaving:
		return 5
	default:
		return 0
	}
}

func (transport *DynamicTCPPeerTransport) updateAdmissionIdentity(identity PeerIdentityDTO, peers []PeerIdentityDTO) error {
	if err := validateDynamicLocalIdentity(identity); err != nil {
		return err
	}
	transport.mu.Lock()
	if transport.closed {
		transport.mu.Unlock()
		return ErrPeerTransportClosed
	}
	current := transport.local
	if !sameImmutableIdentity(current, identity) ||
		!sameAdmissionCandidate(current, identity) ||
		admissionPhaseRank(identity.Phase) < admissionPhaseRank(current.Phase) ||
		(current.OperationID != "" && identity.Phase != PeerPhaseVoter && current.OperationID != identity.OperationID) ||
		(current.Phase == PeerPhaseVoter && identity.Phase != PeerPhaseVoter) {
		transport.mu.Unlock()
		return fmt.Errorf("seed admission response phase is stale")
	}
	if err := validateDynamicPeerSet(identity, peers); err != nil {
		transport.mu.Unlock()
		return fmt.Errorf("seed admission peer list is invalid: %w", err)
	}
	identityChanged := !samePeerIdentity(current, identity)
	transport.local = identity
	retired, refreshedConnections := transport.replacePeersLocked(peers)
	connections := refreshedConnections
	if identityChanged {
		for connection := range transport.connections {
			connections = append(connections, connection)
		}
	}
	transport.mu.Unlock()
	for _, peer := range retired {
		peer.stop()
	}
	for _, connection := range connections {
		_ = connection.Close()
	}
	return nil
}

// ReconcilePeers atomically replaces the committed remote peer registry.
// Phase and operation updates preserve outbound frames and block new inbound leases until active deliveries complete; they then advance the identity revision and discard old staged frames.
func (transport *DynamicTCPPeerTransport) ReconcilePeers(identities []PeerIdentityDTO) error {
	transport.mu.Lock()
	if transport.closed {
		transport.mu.Unlock()
		return ErrPeerTransportClosed
	}
	if err := validateDynamicPeerSet(transport.local, identities); err != nil {
		transport.mu.Unlock()
		return err
	}
	retired, refreshedConnections := transport.replacePeersLocked(identities)
	transport.mu.Unlock()
	for _, peer := range retired {
		peer.stop()
	}
	for _, connection := range refreshedConnections {
		_ = connection.Close()
	}
	return nil
}

func (transport *DynamicTCPPeerTransport) replacePeersLocked(
	identities []PeerIdentityDTO,
) ([]*dynamicPeerState, []net.Conn) {
	raftPeers, raftRetired, raftRefreshed := transport.replaceLanePeersLocked(
		transport.peers, identities, dynamicLaneRaft,
	)
	forwardPeers, forwardRetired, forwardRefreshed := transport.replaceLanePeersLocked(
		transport.forwardPeers, identities, dynamicLaneForward,
	)
	transport.peers = raftPeers
	transport.forwardPeers = forwardPeers
	retired := append(raftRetired, forwardRetired...)
	refreshedConnections := append(raftRefreshed, forwardRefreshed...)
	return retired, refreshedConnections
}

func (transport *DynamicTCPPeerTransport) replaceLanePeersLocked(
	current map[uint64]*dynamicPeerState,
	identities []PeerIdentityDTO,
	lane dynamicLane,
) (map[uint64]*dynamicPeerState, []*dynamicPeerState, []net.Conn) {
	next := make(map[uint64]*dynamicPeerState, len(identities))
	retired := make([]*dynamicPeerState, 0)
	refreshedConnections := make([]net.Conn, 0)
	for _, identity := range identities {
		old, exists := current[identity.MemberID]
		if exists {
			old.mu.Lock()
			switch {
			case samePeerIdentity(old.identity, identity):
				old.identity = identity
				old.mu.Unlock()
				next[identity.MemberID] = old
				continue
			case sameImmutableIdentity(old.identity, identity):
				old.beginIdentityChangeLocked()
				connection, pending := old.connection, old.pending
				old.connection = nil
				old.pending = nil
				old.mu.Unlock()
				waitForInboundDelivery(old)
				old.mu.Lock()
				old.identity = identity
				old.advanceIdentityRevisionLocked()
				old.lastError = ""
				old.mu.Unlock()
				if connection != nil {
					refreshedConnections = append(refreshedConnections, connection)
				}
				if pending != nil && pending != connection {
					refreshedConnections = append(refreshedConnections, pending)
				}
				old.signal()
				next[identity.MemberID] = old
				continue
			default:
				old.mu.Unlock()
				old.retire()
				retired = append(retired, old)
			}
		}
		peer := newDynamicLanePeerState(transport, identity, lane)
		next[identity.MemberID] = peer
		if transport.started {
			transport.startPeerLocked(peer)
		}
	}
	for peerID, peer := range current {
		if _, exists := next[peerID]; !exists {
			peer.retire()
			retired = append(retired, peer)
		}
	}
	return next, retired, refreshedConnections
}

// RequestSeedAdmission proves the staged local identity to a seed, then atomically
// installs the committed local phase and returned voter peers.
func (transport *DynamicTCPPeerTransport) RequestSeedAdmission(
	ctx context.Context,
	seedAddress string,
	seedDNSName string,
) ([]PeerIdentityDTO, error) {
	if ctx == nil {
		return nil, fmt.Errorf("seed admission context is required")
	}
	if !validV2Address(seedAddress) || !validV2DNSName(seedDNSName) {
		return nil, fmt.Errorf("seed address and DNS name are required")
	}
	transport.mu.Lock()
	local := transport.local
	closed := transport.closed
	clientTLS := transport.clientTLS.Clone()
	transport.mu.Unlock()
	if closed {
		return nil, ErrPeerTransportClosed
	}
	if admissionPhaseRank(local.Phase) == 0 {
		return nil, fmt.Errorf("seed admission requires an active candidate identity")
	}
	clientTLS.ServerName = seedDNSName
	dialCtx, cancel := context.WithTimeout(ctx, peerIOTimeout)
	defer cancel()
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: peerIOTimeout}, Config: clientTLS}
	socket, err := dialer.DialContext(dialCtx, "tcp", seedAddress)
	if err != nil {
		return nil, fmt.Errorf("seed admission connection failed")
	}
	connection, ok := socket.(*tls.Conn)
	if !ok {
		_ = socket.Close()
		return nil, fmt.Errorf("seed admission TLS connection failed")
	}
	if !transport.trackConnection(connection) {
		_ = connection.Close()
		return nil, ErrPeerTransportClosed
	}
	defer transport.untrackConnection(connection)
	defer connection.Close()
	stopClose := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stopClose()
	if err := setConnectionDeadline(connection); err != nil {
		return nil, fmt.Errorf("seed admission handshake failed")
	}
	handshakeCtx, handshakeCancel := context.WithTimeout(ctx, peerIOTimeout)
	err = connection.HandshakeContext(handshakeCtx)
	handshakeCancel()
	if err != nil {
		return nil, fmt.Errorf("seed admission TLS handshake failed")
	}
	requestedCandidate := local
	requestedCandidate.Phase = PeerPhaseStaged
	requestedCandidate.OperationID = ""
	request := SeedAdmissionRequestDTO{Candidate: requestedCandidate}
	payload, err := encodeSeedAdmissionRequest(request)
	if err != nil {
		return nil, err
	}
	if err := writeDynamicFrame(connection, payload, dynamicAdmissionLimit); err != nil {
		return nil, fmt.Errorf("seed admission request failed")
	}
	responsePayload, err := readDynamicFrame(connection, dynamicAdmissionLimit)
	if err != nil {
		return nil, fmt.Errorf("seed admission response failed")
	}
	response, err := decodeSeedAdmissionResponse(responsePayload)
	if err != nil || !response.Accepted {
		return nil, fmt.Errorf("seed admission rejected")
	}
	// Older seeds may approve a stable voter. A fresh joiner has no durable
	// history to justify installing that identity or entering its Raft streams.
	if local.Phase == PeerPhaseStaged && response.Candidate.Phase == PeerPhaseVoter {
		return nil, fmt.Errorf("fresh joiner cannot install voter seed identity")
	}
	// Address and release are not echoed by §1.5; they remain bound to the local request.
	response.Candidate.Address = local.Address
	response.Candidate.ReleaseVersion = local.ReleaseVersion
	for index := range response.CurrentVoters {
		response.CurrentVoters[index].ReleaseVersion = local.ReleaseVersion
	}
	if !sameAdmissionCandidate(local, response.Candidate) ||
		!validAdmissionCandidateIdentity(response.Candidate) ||
		admissionPhaseRank(response.Candidate.Phase) < admissionPhaseRank(local.Phase) ||
		(local.OperationID != "" && response.Candidate.Phase != PeerPhaseVoter &&
			response.Candidate.OperationID != local.OperationID) ||
		(local.Phase == PeerPhaseVoter && response.Candidate.Phase != PeerPhaseVoter) ||
		len(response.CurrentVoters) != 3 {
		return nil, fmt.Errorf("seed admission response identity mismatch")
	}
	seen := make(map[uint64]struct{}, 3)
	for index := range response.CurrentVoters {
		endpoint := &response.CurrentVoters[index]
		if endpoint.MemberID == local.MemberID || endpoint.ClusterID != local.ClusterID ||
			!release.CompatibleStrings(local.ReleaseVersion, endpoint.ReleaseVersion) ||
			endpoint.ClusterProtocolMajor != dynamicProtocolMajor ||
			endpoint.ClusterProtocolMinor != dynamicProtocolMinor ||
			endpoint.StateSchemaMajor != dynamicSchemaMajor ||
			endpoint.StateSchemaMinor != dynamicSchemaMinor ||
			endpoint.Phase != PeerPhaseVoter || endpoint.OperationID != "" ||
			validateDynamicIdentity(*endpoint, false) != nil {
			return nil, fmt.Errorf("seed admission response voter list is invalid")
		}
		if _, exists := seen[endpoint.MemberID]; exists {
			return nil, fmt.Errorf("seed admission response voter list is invalid")
		}
		seen[endpoint.MemberID] = struct{}{}
	}
	sort.Slice(response.CurrentVoters, func(left, right int) bool {
		return response.CurrentVoters[left].MemberID < response.CurrentVoters[right].MemberID
	})
	if err := transport.updateAdmissionIdentity(response.Candidate, response.CurrentVoters); err != nil {
		return nil, fmt.Errorf("apply committed seed admission state: %w", err)
	}
	return response.CurrentVoters, nil
}

func admissionPhaseRank(phase PeerPhaseDTO) int {
	switch phase {
	case PeerPhaseStaged:
		return 1
	case PeerPhaseLearner:
		return 2
	case PeerPhaseCatchingUp:
		return 3
	case PeerPhaseVoter:
		return 4
	default:
		return 0
	}
}

func validAdmissionCandidateFields(identity PeerIdentityDTO) bool {
	switch identity.Phase {
	case PeerPhaseStaged, PeerPhaseLearner, PeerPhaseCatchingUp:
		return validOperationID(identity.OperationID)
	case PeerPhaseVoter:
		return identity.OperationID == ""
	default:
		return false
	}
}

func validAdmissionCandidateIdentity(identity PeerIdentityDTO) bool {
	return validAdmissionCandidateFields(identity) && validateDynamicIdentity(identity, true) == nil
}

func newDynamicPeerState(owner *DynamicTCPPeerTransport, identity PeerIdentityDTO) *dynamicPeerState {
	return newDynamicLanePeerState(owner, identity, dynamicLaneRaft)
}

func newDynamicLanePeerState(
	owner *DynamicTCPPeerTransport,
	identity PeerIdentityDTO,
	lane dynamicLane,
) *dynamicPeerState {
	queueMessages := lane.queueMessages()
	peer := &dynamicPeerState{
		owner: owner, id: identity.MemberID, identity: identity, lane: lane,
		wake: make(chan struct{}, 1), inbox: make(chan peerFrame, queueMessages),
		outbox: make([]*peerFrame, queueMessages), deliveryWake: make(chan struct{}, 1),
	}
	if lane == dynamicLaneRaft {
		peer.capacityWake = make(chan struct{}, 1)
	}
	return peer
}

func (peer *dynamicPeerState) identitySnapshot() (PeerIdentityDTO, uint64, bool) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return peer.identity, peer.identityRevision, peer.stopped
}

func (transport *DynamicTCPPeerTransport) startPeerLocked(peer *dynamicPeerState) {
	peer.ctx, peer.cancel = context.WithCancel(transport.ctx)
	if peer.id > transport.local.MemberID {
		transport.workers.Add(1)
		go func() {
			defer transport.workers.Done()
			transport.dialPeer(peer.ctx, peer)
		}()
	}
}

func (transport *DynamicTCPPeerTransport) acceptLoop(ctx context.Context) error {
	for {
		socket, err := transport.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept dynamic peer connection: %w", err)
		}
		if !transport.trackInboundConnection(socket) {
			_ = socket.Close()
			continue
		}
		transport.workers.Add(1)
		go func(connection net.Conn) {
			defer transport.workers.Done()
			defer transport.untrackInboundConnection(connection)
			defer connection.Close()
			transport.handleInbound(ctx, connection)
		}(socket)
	}
}

func (transport *DynamicTCPPeerTransport) handleInbound(ctx context.Context, socket net.Conn) {
	connection, ok := socket.(*tls.Conn)
	if !ok || setConnectionDeadline(connection) != nil {
		return
	}
	handshakeCtx, cancel := context.WithTimeout(ctx, peerIOTimeout)
	err := connection.HandshakeContext(handshakeCtx)
	cancel()
	if err != nil {
		return
	}
	payload, err := readDynamicHandshakeFrame(connection)
	if err != nil {
		return
	}
	switch string(payload[:8]) {
	case "NMQJOIN2":
		transport.handleAdmission(ctx, connection, payload)
	case dynamicBootstrapProbeMagic:
		transport.handleBootstrapProbe(connection, payload)
	case dynamicRaftMagic:
		transport.handleStreamInbound(connection, payload, dynamicLaneRaft)
	case dynamicForwardMagic:
		transport.handleStreamInbound(connection, payload, dynamicLaneForward)
	}
}

func (transport *DynamicTCPPeerTransport) handleAdmission(
	ctx context.Context,
	connection *tls.Conn,
	payload []byte,
) {
	reject := func() {
		_ = writeDynamicFrame(connection, rejectedSeedAdmissionResponse(), dynamicAdmissionLimit)
	}
	request, err := decodeSeedAdmissionRequest(payload)
	if err != nil || !matchesPeerCertificate(connection, request.Candidate) {
		reject()
		return
	}
	transport.mu.Lock()
	localClusterID := transport.clusterID
	localReleaseVersion := transport.local.ReleaseVersion
	closed := transport.closed
	handler := transport.admission
	transport.mu.Unlock()
	if closed || request.Candidate.ClusterID != localClusterID ||
		!release.CompatibleStrings(localReleaseVersion, request.Candidate.ReleaseVersion) {
		reject()
		return
	}
	admissionCtx, cancel := context.WithTimeout(ctx, peerIOTimeout)
	response, err := handler(admissionCtx, request.Candidate)
	cancel()
	if err != nil || !response.Accepted || validateAdmissionResult(request.Candidate, response) != nil {
		reject()
		return
	}
	encoded, err := encodeSeedAdmissionResponse(response)
	if err != nil {
		return
	}
	if err := writeDynamicFrame(connection, encoded, dynamicAdmissionLimit); err != nil {
		return
	}
}

func (transport *DynamicTCPPeerTransport) handleStreamInbound(
	connection *tls.Conn,
	payload []byte,
	lane dynamicLane,
) {
	remote, err := decodeDynamicPeerHello(payload, lane.magic())
	if err != nil {
		return
	}
	transport.mu.Lock()
	peer, exists := transport.lanePeersLocked(lane)[remote.MemberID]
	local := transport.local
	transport.mu.Unlock()
	if !exists {
		return
	}
	expectedIdentity, identityRevision, stopped := peer.identitySnapshot()
	if stopped {
		peer.recordError("peer stopped")
		return
	}
	if !matchesPeerCertificate(connection, expectedIdentity) {
		peer.recordError("peer certificate identity mismatch")
		return
	}
	if remote.MemberID >= local.MemberID {
		peer.recordError(fmt.Sprintf("direction mismatch: remote %d >= local %d", remote.MemberID, local.MemberID))
		return
	}
	if !compatiblePeerIdentity(expectedIdentity, remote) {
		peer.recordError(fmt.Sprintf("incompatible identity: expected(phase=%d,op=%s) remote(phase=%d,op=%s)", expectedIdentity.Phase, expectedIdentity.OperationID, remote.Phase, remote.OperationID))
		return
	}
	if !release.CompatibleStrings(local.ReleaseVersion, remote.ReleaseVersion) {
		peer.recordError(fmt.Sprintf("incompatible release: local=%s remote=%s", local.ReleaseVersion, remote.ReleaseVersion))
		return
	}
	localPayload, err := encodeDynamicPeerHello(local, lane.magic())
	if err != nil {
		peer.recordError("peer handshake failed")
		return
	}
	if err := writeDynamicFrame(connection, localPayload, dynamicHelloLimit); err != nil {
		peer.recordError("peer handshake failed")
		return
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		return
	}
	peer.recordConnected()
	peer.runConnection(peer.ctx, connection, identityRevision)
}

func (transport *DynamicTCPPeerTransport) dialPeer(ctx context.Context, peer *dynamicPeerState) {
	for ctx.Err() == nil {
		transport.mu.Lock()
		local := transport.local
		closed := transport.closed
		transport.mu.Unlock()
		if closed {
			return
		}
		identity, identityRevision, stopped := peer.identitySnapshot()
		if stopped {
			return
		}
		if local.Phase == PeerPhaseStaged {
			if !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		clientTLS := transport.clientTLS.Clone()
		clientTLS.ServerName = identity.DNSSAN
		dialCtx, cancel := context.WithTimeout(ctx, peerIOTimeout)
		dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: peerIOTimeout}, Config: clientTLS}
		socket, err := dialer.DialContext(dialCtx, "tcp", identity.Address)
		cancel()
		if err != nil {
			peer.recordError("peer connection failed")
			if !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		connection, ok := socket.(*tls.Conn)
		if !ok {
			_ = socket.Close()
			if !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		if !transport.trackConnection(connection) {
			_ = connection.Close()
			return
		}
		peer.mu.Lock()
		if peer.stopped || peer.identityRevision != identityRevision {
			stopped = peer.stopped
			peer.mu.Unlock()
			_ = connection.Close()
			transport.untrackConnection(connection)
			if stopped || !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		peer.pending = connection
		peer.mu.Unlock()
		err = setConnectionDeadline(connection)
		if err == nil {
			handshakeCtx, handshakeCancel := context.WithTimeout(ctx, peerIOTimeout)
			err = connection.HandshakeContext(handshakeCtx)
			handshakeCancel()
		}
		if err == nil {
			var hello []byte
			hello, err = encodeDynamicPeerHello(local, peer.lane.magic())
			if err == nil {
				err = writeDynamicFrame(connection, hello, dynamicHelloLimit)
			}
		}
		var remote PeerIdentityDTO
		if err == nil {
			var response []byte
			response, err = readDynamicFrame(connection, dynamicHelloLimit)
			if err == nil {
				remote, err = decodeDynamicPeerHello(response, peer.lane.magic())
			}
		}
		currentIdentity, currentRevision, stopped := peer.identitySnapshot()
		if err == nil {
			if stopped {
				err = fmt.Errorf("peer stopped")
			} else if currentRevision != identityRevision {
				err = fmt.Errorf("revision mismatch: current %d != id %d", currentRevision, identityRevision)
			} else if !matchesPeerCertificate(connection, currentIdentity) {
				err = fmt.Errorf("peer identity mismatch")
			} else if !compatiblePeerIdentity(currentIdentity, remote) {
				err = fmt.Errorf("incompatible identity: current(phase=%d,op=%s) remote(phase=%d,op=%s)", currentIdentity.Phase, currentIdentity.OperationID, remote.Phase, remote.OperationID)
			} else if !release.CompatibleStrings(local.ReleaseVersion, remote.ReleaseVersion) {
				err = fmt.Errorf("incompatible release: local=%s remote=%s", local.ReleaseVersion, remote.ReleaseVersion)
			}
		}
		if err != nil || connection.SetDeadline(time.Time{}) != nil {
			peer.clearPending(connection)
			_ = connection.Close()
			transport.untrackConnection(connection)
			if currentRevision == identityRevision && !stopped && err != nil {
				peer.recordError(err.Error())
			}
			if stopped || !waitPeerRetry(ctx) {
				return
			}
			continue
		}
		peer.recordConnected()
		peer.runConnection(ctx, connection, identityRevision)
		transport.untrackConnection(connection)
		if !waitPeerRetry(ctx) {
			return
		}
	}
}

func matchesPeerCertificate(connection *tls.Conn, identity PeerIdentityDTO) bool {
	state := connection.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return false
	}
	certificate := state.PeerCertificates[0]
	if sha256.Sum256(certificate.Raw) != identity.CertificateSHA256 {
		return false
	}
	for _, name := range certificate.DNSNames {
		if name == identity.DNSSAN {
			return true
		}
	}
	return false
}

func (transport *DynamicTCPPeerTransport) trackInboundConnection(connection net.Conn) bool {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	limit := 2*len(transport.peers) + 1 // Two streams per peer plus one seed-admission slot.
	if transport.closed || transport.inboundConnections >= limit {
		return false
	}
	transport.inboundConnections++
	transport.connections[connection] = struct{}{}
	return true
}

func (transport *DynamicTCPPeerTransport) untrackInboundConnection(connection net.Conn) {
	transport.mu.Lock()
	delete(transport.connections, connection)
	transport.inboundConnections--
	transport.mu.Unlock()
}

func (transport *DynamicTCPPeerTransport) trackConnection(connection net.Conn) bool {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.closed {
		return false
	}
	transport.connections[connection] = struct{}{}
	return true
}

func (transport *DynamicTCPPeerTransport) untrackConnection(connection net.Conn) {
	transport.mu.Lock()
	delete(transport.connections, connection)
	transport.mu.Unlock()
}

func (transport *DynamicTCPPeerTransport) shutdown() {
	transport.mu.Lock()
	if transport.closed {
		transport.mu.Unlock()
		return
	}
	transport.closed = true
	cancel := transport.cancel
	peers := make([]*dynamicPeerState, 0, len(transport.peers)+len(transport.forwardPeers))
	for _, peer := range transport.peers {
		peers = append(peers, peer)
	}
	for _, peer := range transport.forwardPeers {
		peers = append(peers, peer)
	}
	connections := make([]net.Conn, 0, len(transport.connections))
	for connection := range transport.connections {
		connections = append(connections, connection)
	}
	started := transport.started
	transport.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	_ = transport.listener.Close()
	for _, peer := range peers {
		peer.stop()
	}
	for _, connection := range connections {
		_ = connection.Close()
	}
	if !started {
		transport.finish()
	}
}

// Close cancels v2 workers, closes sockets and drains all peer queues.
func (transport *DynamicTCPPeerTransport) Close() error {
	transport.shutdown()
	<-transport.done
	return nil
}

func (transport *DynamicTCPPeerTransport) finish() {
	transport.finishOnce.Do(func() {
		close(transport.inboundReady)
		if transport.forwardReady != nil {
			close(transport.forwardReady)
		}
		close(transport.done)
	})
}

func (peer *dynamicPeerState) clearPending(connection net.Conn) {
	peer.mu.Lock()
	if peer.pending == connection {
		peer.pending = nil
	}
	peer.mu.Unlock()
}

func (peer *dynamicPeerState) runConnection(ctx context.Context, connection net.Conn, identityRevision uint64) {
	peer.mu.Lock()
	if peer.stopped || peer.identityRevision != identityRevision {
		peer.mu.Unlock()
		_ = connection.Close()
		return
	}
	previous := peer.connection
	peer.connection = connection
	peer.signalInboundCapacityLocked()
	if peer.pending == connection {
		peer.pending = nil
	}
	peer.lastError = ""
	peer.mu.Unlock()
	if previous != nil && previous != connection {
		_ = previous.Close()
	}
	peer.sessionMu.Lock()
	defer peer.sessionMu.Unlock()
	peer.mu.Lock()
	current := peer.connection == connection && !peer.stopped && peer.identityRevision == identityRevision
	peer.mu.Unlock()
	if !current {
		_ = connection.Close()
		return
	}
	readErr := make(chan error, 1)
	readDone := make(chan struct{})
	readerCtx, cancelReader := context.WithCancel(ctx)
	go func() {
		defer close(readDone)
		for {
			if peer.lane == dynamicLaneRaft {
				if err := peer.receiveRaftFrame(readerCtx, connection, identityRevision); err != nil {
					if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) &&
						!errors.Is(err, ErrPeerTransportClosed) && readerCtx.Err() == nil {
						peer.recordError("peer read failed")
					}
					readErr <- err
					return
				}
				continue
			}
			data, err := readDynamicIdleFrame(connection, peer.lane.frameLimit())
			if err != nil {
				if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
					peer.recordError("peer read failed")
				}
				readErr <- err
				return
			}
			if err := peer.enqueueInbound(data, identityRevision); err != nil {
				if !errors.Is(err, ErrPeerTransportClosed) {
					peer.recordError("inbound queue full")
				}
				readErr <- err
				return
			}
		}
	}()
	defer func() {
		cancelReader()
		_ = connection.Close()
		<-readDone
		peer.mu.Lock()
		if peer.connection == connection {
			peer.connection = nil
			if !peer.stopped && peer.lastError == "" {
				peer.lastError = "peer disconnected"
			}
		}
		peer.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-readErr:
			return
		default:
		}
		frame := peer.takeOutbound()
		if frame == nil {
			select {
			case <-ctx.Done():
				return
			case <-readErr:
				return
			case <-peer.wake:
				continue
			}
		}
		if err := writeDynamicFrame(connection, frame.data, peer.lane.frameLimit()); err != nil {
			peer.recordError("peer write failed")
			return
		}
		peer.completeOutbound(frame)
	}
}

func (peer *dynamicPeerState) enqueueInbound(data []byte, identityRevision uint64) error {
	frame := &peerFrame{data: data, size: len(data) + 4, identityRevision: identityRevision}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.stopped || peer.identityChanging || peer.identityRevision != identityRevision {
		return ErrPeerTransportClosed
	}
	if peer.inMessages >= peer.lane.queueMessages() || peer.inBytes+frame.size > peer.lane.queueBytes() {
		return ErrPeerBackpressure
	}
	peer.inMessages++
	peer.inBytes += frame.size
	frame.enqueuedAt = time.Now()
	peer.inbox <- *frame
	if peer.owner != nil {
		peer.owner.signalLane(peer.lane)
	}
	return nil
}

func (peer *dynamicPeerState) completeInbound(frame peerFrame) {
	peer.mu.Lock()
	peer.completeInboundLocked(frame)
	peer.mu.Unlock()
}

func (peer *dynamicPeerState) completeInboundLocked(frame peerFrame) {
	if frame.identityRevision == peer.identityRevision && peer.inMessages > 0 && peer.inBytes >= frame.size {
		peer.inMessages--
		peer.inBytes -= frame.size
		peer.signalInboundCapacityLocked()
	}
}

func (peer *dynamicPeerState) discardInboundLocked() {
	for {
		select {
		case <-peer.inbox:
		default:
			peer.inMessages = 0
			peer.inBytes = 0
			return
		}
	}
}

func (peer *dynamicPeerState) beginIdentityChangeLocked() {
	if peer.stopped || peer.identityChanging {
		return
	}
	peer.identityChanging = true
	peer.signalInboundCapacityLocked()
}

func (peer *dynamicPeerState) completeInboundDelivery(frame peerFrame, deliveryID uint64) {
	peer.mu.Lock()
	completed := peer.inboundDelivering &&
		peer.inboundDeliveryID == deliveryID &&
		peer.inboundDeliveryRevision == frame.identityRevision
	if completed {
		peer.inboundDelivering = false
		peer.completeInboundLocked(frame)
	}
	peer.mu.Unlock()
	if completed {
		select {
		case peer.deliveryWake <- struct{}{}:
		default:
		}
	}
}

func (peer *dynamicPeerState) advanceIdentityRevisionLocked() {
	peer.identityRevision++
	peer.identityChanging = false
	peer.discardInboundLocked()
}

func waitForInboundDelivery(peer *dynamicPeerState) {
	for {
		peer.mu.Lock()
		delivering := peer.inboundDelivering
		wake := peer.deliveryWake
		peer.mu.Unlock()
		if !delivering {
			return
		}
		<-wake
	}
}

func (peer *dynamicPeerState) retire() {
	peer.mu.Lock()
	if !peer.stopped {
		peer.beginIdentityChangeLocked()
		peer.stopped = true
	}
	peer.mu.Unlock()
	waitForInboundDelivery(peer)
}

func (peer *dynamicPeerState) drainInbound() {
	for {
		select {
		case frame := <-peer.inbox:
			peer.completeInbound(frame)
		default:
			return
		}
	}
}

func (peer *dynamicPeerState) takeOutbound() *peerFrame {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.stopped {
		return nil
	}
	if peer.outCurrent != nil {
		return peer.outCurrent
	}
	if peer.outCount == 0 {
		return nil
	}
	peer.outCurrent = peer.outbox[peer.outHead]
	peer.outbox[peer.outHead] = nil
	peer.outHead = (peer.outHead + 1) % len(peer.outbox)
	peer.outCount--
	return peer.outCurrent
}

func (peer *dynamicPeerState) completeOutbound(frame *peerFrame) {
	peer.mu.Lock()
	if peer.outCurrent == frame {
		peer.outCurrent = nil
		peer.outMessages--
		peer.outBytes -= frame.size
	}
	peer.mu.Unlock()
	if peer.lane == dynamicLaneRaft {
		peer.owner.signalOutboundCapacity()
	}
}

func (peer *dynamicPeerState) signal() {
	select {
	case peer.wake <- struct{}{}:
	default:
	}
}

func (peer *dynamicPeerState) recordConnected() {
	peer.mu.Lock()
	peer.lastError = ""
	peer.mu.Unlock()
}

func (peer *dynamicPeerState) recordError(message string) {
	peer.mu.Lock()
	if !peer.stopped {
		peer.lastError = message
	}
	peer.mu.Unlock()
}

func (peer *dynamicPeerState) stop() {
	peer.mu.Lock()
	if !peer.stopped {
		peer.beginIdentityChangeLocked()
		peer.stopped = true
	}
	connection := peer.connection
	pending := peer.pending
	cancel := peer.cancel
	peer.connection = nil
	peer.pending = nil
	peer.signalInboundCapacityLocked()
	for index := range peer.outbox {
		peer.outbox[index] = nil
	}
	peer.outHead = 0
	peer.outCount = 0
	peer.outCurrent = nil
	peer.outMessages = 0
	peer.outBytes = 0
	peer.inMessages = 0
	peer.inBytes = 0
	peer.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if pending != nil && pending != connection {
		_ = pending.Close()
	}
	if connection != nil {
		_ = connection.Close()
	}
	waitForInboundDelivery(peer)
	for {
		select {
		case <-peer.inbox:
		default:
			peer.signal()
			return
		}
	}
}

func validateDynamicPeerSet(local PeerIdentityDTO, peers []PeerIdentityDTO) error {
	seen := make(map[uint64]struct{}, len(peers))
	for _, identity := range peers {
		if identity.MemberID == local.MemberID {
			return fmt.Errorf("local member ID must not appear in peer list")
		}
		if _, exists := seen[identity.MemberID]; exists {
			return fmt.Errorf("duplicate dynamic peer ID")
		}
		seen[identity.MemberID] = struct{}{}
		if identity.ClusterID != local.ClusterID || !release.CompatibleStrings(local.ReleaseVersion, identity.ReleaseVersion) ||
			identity.ClusterProtocolMajor != local.ClusterProtocolMajor ||
			identity.ClusterProtocolMinor != local.ClusterProtocolMinor ||
			identity.StateSchemaMajor != local.StateSchemaMajor ||
			identity.StateSchemaMinor != local.StateSchemaMinor {
			return fmt.Errorf("dynamic peer compatibility mismatch")
		}
		if err := validateDynamicIdentity(identity, false); err != nil {
			return fmt.Errorf("dynamic peer %d: %w", identity.MemberID, err)
		}
	}
	return nil
}

func validateDynamicIdentity(identity PeerIdentityDTO, allowStaged bool) error {
	if identity.MemberID == 0 || !validClusterID(identity.ClusterID) {
		return fmt.Errorf("member and cluster identity are required")
	}
	if !validV2Address(identity.Address) || !validV2DNSName(identity.DNSSAN) {
		return fmt.Errorf("peer address and DNS SAN are invalid")
	}
	if _, err := release.Parse(identity.ReleaseVersion); err != nil {
		return fmt.Errorf("release version is invalid")
	}
	if identity.ClusterProtocolMajor != dynamicProtocolMajor || identity.ClusterProtocolMinor != dynamicProtocolMinor ||
		identity.StateSchemaMajor != dynamicSchemaMajor || identity.StateSchemaMinor != dynamicSchemaMinor {
		return fmt.Errorf("unsupported peer protocol or state schema")
	}
	switch identity.Phase {
	case PeerPhaseStaged:
		if !allowStaged || (identity.OperationID != "" && !validOperationID(identity.OperationID)) {
			return fmt.Errorf("staged identity is valid only for admission")
		}
	case PeerPhaseLearner, PeerPhaseCatchingUp, PeerPhaseLeaving:
		if !validOperationID(identity.OperationID) {
			return fmt.Errorf("membership operation ID is invalid")
		}
	case PeerPhaseVoter:
		if identity.OperationID != "" {
			return fmt.Errorf("stable voter operation ID must be empty")
		}
	default:
		return fmt.Errorf("peer phase is not admitted")
	}
	return nil
}

func validateDynamicInitialIdentity(identity PeerIdentityDTO) error {
	return validateDynamicIdentity(identity, true)
}

func validateDynamicLocalIdentity(identity PeerIdentityDTO) error {
	if err := validateDynamicIdentity(identity, true); err != nil {
		return err
	}
	if identity.Phase == PeerPhaseStaged && !validOperationID(identity.OperationID) {
		return fmt.Errorf("staged local identity requires a committed operation ID")
	}
	return nil
}

func validV2Address(address string) bool {
	if len(address) < 1 || len(address) > 255 || !utf8.ValidString(address) || address != strings.TrimSpace(address) {
		return false
	}
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	if err != nil || host == "" || host == "*" || strings.ContainsAny(host, " \t\r\n/\\") ||
		(ip != nil && ip.IsUnspecified()) {
		return false
	}
	portNumber, err := strconv.Atoi(port)
	return err == nil && portNumber >= 1 && portNumber <= 65535 && strconv.Itoa(portNumber) == port
}

func validV2DNSName(name string) bool {
	if len(name) < 1 || len(name) > 253 || name != strings.ToLower(name) || net.ParseIP(name) != nil {
		return false
	}
	for _, character := range name {
		if character > 127 {
			return false
		}
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}

func validOperationID(operationID string) bool {
	if len(operationID) != 36 || operationID[8] != '-' || operationID[13] != '-' ||
		operationID[18] != '-' || operationID[23] != '-' {
		return false
	}
	for index, character := range operationID {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func validateDynamicCertificate(certificate tls.Certificate, identity PeerIdentityDTO) error {
	leaf, err := dynamicCertificateLeaf(certificate)
	if err != nil {
		return err
	}
	if sha256.Sum256(leaf.Raw) != identity.CertificateSHA256 {
		return fmt.Errorf("TLS leaf fingerprint does not match peer identity")
	}
	for _, dnsName := range leaf.DNSNames {
		if dnsName == identity.DNSSAN {
			return nil
		}
	}
	return fmt.Errorf("TLS leaf DNS SAN does not match peer identity")
}

func dynamicCertificateLeaf(certificate tls.Certificate) (*x509.Certificate, error) {
	if certificate.Leaf != nil {
		return certificate.Leaf, nil
	}
	if len(certificate.Certificate) == 0 {
		return nil, fmt.Errorf("TLS certificate is missing")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse TLS leaf certificate: %w", err)
	}
	return leaf, nil
}

func sameImmutableIdentity(left, right PeerIdentityDTO) bool {
	left.Phase, right.Phase = PeerPhaseStaged, PeerPhaseStaged
	left.OperationID, right.OperationID = "", ""
	return samePeerIdentity(left, right)
}

func samePeerIdentity(left, right PeerIdentityDTO) bool {
	return left.MemberID == right.MemberID && left.ClusterID == right.ClusterID &&
		left.Address == right.Address && left.DNSSAN == right.DNSSAN &&
		left.CertificateSHA256 == right.CertificateSHA256 &&
		left.ClusterProtocolMajor == right.ClusterProtocolMajor &&
		left.ClusterProtocolMinor == right.ClusterProtocolMinor &&
		left.StateSchemaMajor == right.StateSchemaMajor && left.StateSchemaMinor == right.StateSchemaMinor &&
		left.Phase == right.Phase && left.OperationID == right.OperationID
}

func compatiblePeerIdentity(expected, remote PeerIdentityDTO) bool {
	if !sameImmutableIdentity(expected, remote) {
		return false
	}
	if expected.Phase == remote.Phase && expected.OperationID == remote.OperationID {
		return true
	}
	if (expected.Phase == PeerPhaseVoter && remote.Phase == PeerPhaseLeaving && remote.OperationID != "") ||
		(expected.Phase == PeerPhaseLeaving && remote.Phase == PeerPhaseVoter && expected.OperationID != "") {
		return true
	}
	if (expected.Phase == PeerPhaseVoter && remote.Phase == PeerPhaseCatchingUp && remote.OperationID != "") ||
		(expected.Phase == PeerPhaseCatchingUp && remote.Phase == PeerPhaseVoter && expected.OperationID != "") {
		return true
	}
	if (expected.Phase == PeerPhaseLearner && remote.Phase == PeerPhaseCatchingUp) ||
		(expected.Phase == PeerPhaseCatchingUp && remote.Phase == PeerPhaseLearner) {
		return expected.OperationID == remote.OperationID
	}
	return false
}

func sameAdmissionCandidate(requested, committed PeerIdentityDTO) bool {
	requested.Phase, committed.Phase = PeerPhaseStaged, PeerPhaseStaged
	requested.OperationID, committed.OperationID = "", ""
	return samePeerIdentity(requested, committed)
}

func encodeDynamicPeerHello(identity PeerIdentityDTO, magic string) ([]byte, error) {
	if magic != dynamicRaftMagic && magic != dynamicForwardMagic {
		return nil, ErrInvalidPeerMessage
	}
	if err := validateDynamicIdentity(identity, false); err != nil {
		return nil, err
	}
	buffer := make([]byte, 0, dynamicHelloLimit)
	buffer = append(buffer, magic...)
	buffer = appendUint16(buffer, dynamicProtocolMajor)
	buffer = appendUint16(buffer, dynamicProtocolMinor)
	buffer = appendString16(buffer, identity.ClusterID)
	buffer = appendUint64(buffer, identity.MemberID)
	buffer = appendString16(buffer, identity.Address)
	buffer = appendString16(buffer, identity.DNSSAN)
	buffer = append(buffer, identity.CertificateSHA256[:]...)
	buffer = appendString16(buffer, identity.ReleaseVersion)
	buffer = appendUint16(buffer, identity.ClusterProtocolMajor)
	buffer = appendUint16(buffer, identity.ClusterProtocolMinor)
	buffer = appendUint16(buffer, identity.StateSchemaMajor)
	buffer = appendUint16(buffer, identity.StateSchemaMinor)
	buffer = append(buffer, byte(identity.Phase))
	buffer = append(buffer, identity.OperationID...)
	if len(buffer) > dynamicHelloLimit {
		return nil, ErrInvalidPeerMessage
	}
	return buffer, nil
}

func decodeDynamicPeerHello(payload []byte, magic string) (PeerIdentityDTO, error) {
	if (magic != dynamicRaftMagic && magic != dynamicForwardMagic) ||
		len(payload) > dynamicHelloLimit || len(payload) < 8 || string(payload[:8]) != magic {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	reader := dynamicWireReader{data: payload, position: 8}
	protocolMajor, ok := reader.uint16()
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	protocolMinor, ok := reader.uint16()
	if !ok || protocolMajor != dynamicProtocolMajor || protocolMinor != dynamicProtocolMinor {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	clusterID, ok := reader.string16(200, false)
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	memberID, ok := reader.uint64()
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	address, ok := reader.string16(255, false)
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	dnsSAN, ok := reader.string16(253, true)
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	fingerprint, ok := reader.bytes(sha256.Size)
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	release, ok := reader.string16(64, false)
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	clusterMajor, ok := reader.uint16()
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	clusterMinor, ok := reader.uint16()
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	schemaMajor, ok := reader.uint16()
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	schemaMinor, ok := reader.uint16()
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	phase, ok := reader.byte()
	if !ok {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	operationID, ok := reader.operationID()
	if !ok || reader.position != len(payload) {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	identity := PeerIdentityDTO{
		MemberID: memberID, ClusterID: clusterID, Address: address, DNSSAN: dnsSAN,
		ReleaseVersion: release, ClusterProtocolMajor: uint32(clusterMajor),
		ClusterProtocolMinor: uint32(clusterMinor), StateSchemaMajor: uint32(schemaMajor),
		StateSchemaMinor: uint32(schemaMinor), Phase: PeerPhaseDTO(phase), OperationID: operationID,
	}
	copy(identity.CertificateSHA256[:], fingerprint)
	if err := validateDynamicIdentity(identity, false); err != nil {
		return PeerIdentityDTO{}, ErrInvalidPeerMessage
	}
	return identity, nil
}

func encodeSeedAdmissionRequest(request SeedAdmissionRequestDTO) ([]byte, error) {
	candidate := request.Candidate
	if err := validateDynamicIdentity(candidate, true); err != nil || candidate.Phase != PeerPhaseStaged {
		return nil, ErrInvalidPeerMessage
	}
	buffer := make([]byte, 0, 512)
	buffer = append(buffer, "NMQJOIN2"...)
	buffer = appendUint16(buffer, dynamicProtocolMajor)
	buffer = appendUint16(buffer, dynamicProtocolMinor)
	buffer = appendString16(buffer, candidate.ClusterID)
	buffer = appendUint64(buffer, candidate.MemberID)
	buffer = appendString16(buffer, candidate.Address)
	buffer = appendString16(buffer, candidate.DNSSAN)
	buffer = append(buffer, candidate.CertificateSHA256[:]...)
	buffer = appendString16(buffer, candidate.ReleaseVersion)
	buffer = appendUint16(buffer, candidate.StateSchemaMajor)
	buffer = appendUint16(buffer, candidate.StateSchemaMinor)
	if len(buffer) > dynamicAdmissionLimit {
		return nil, ErrInvalidPeerMessage
	}
	return buffer, nil
}

func decodeSeedAdmissionRequest(payload []byte) (SeedAdmissionRequestDTO, error) {
	if len(payload) < 8 || len(payload) > dynamicAdmissionLimit || string(payload[:8]) != "NMQJOIN2" {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	reader := dynamicWireReader{data: payload, position: 8}
	protocolMajor, ok := reader.uint16()
	if !ok {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	protocolMinor, ok := reader.uint16()
	if !ok || protocolMajor != dynamicProtocolMajor || protocolMinor != dynamicProtocolMinor {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	clusterID, ok := reader.string16(200, false)
	if !ok {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	memberID, ok := reader.uint64()
	if !ok {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	address, ok := reader.string16(255, false)
	if !ok {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	dnsSAN, ok := reader.string16(253, true)
	if !ok {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	fingerprint, ok := reader.bytes(sha256.Size)
	if !ok {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	release, ok := reader.string16(64, false)
	if !ok {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	schemaMajor, ok := reader.uint16()
	if !ok {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	schemaMinor, ok := reader.uint16()
	if !ok || reader.position != len(payload) {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	candidate := PeerIdentityDTO{
		MemberID: memberID, ClusterID: clusterID, Address: address, DNSSAN: dnsSAN,
		ReleaseVersion: release, ClusterProtocolMajor: dynamicProtocolMajor,
		ClusterProtocolMinor: dynamicProtocolMinor, StateSchemaMajor: uint32(schemaMajor),
		StateSchemaMinor: uint32(schemaMinor), Phase: PeerPhaseStaged,
	}
	copy(candidate.CertificateSHA256[:], fingerprint)
	if err := validateDynamicIdentity(candidate, true); err != nil {
		return SeedAdmissionRequestDTO{}, ErrInvalidPeerMessage
	}
	return SeedAdmissionRequestDTO{Candidate: candidate}, nil
}

func validateAdmissionResult(candidate PeerIdentityDTO, response SeedAdmissionResponseDTO) error {
	if !response.Accepted || !sameAdmissionCandidate(candidate, response.Candidate) ||
		!validAdmissionCandidateIdentity(response.Candidate) || len(response.CurrentVoters) != 3 {
		return ErrInvalidPeerMessage
	}
	voters := append([]PeerIdentityDTO(nil), response.CurrentVoters...)
	sort.Slice(voters, func(left, right int) bool { return voters[left].MemberID < voters[right].MemberID })
	for index, voter := range voters {
		if voter.ClusterID != candidate.ClusterID || !release.CompatibleStrings(candidate.ReleaseVersion, voter.ReleaseVersion) ||
			voter.ClusterProtocolMinor != candidate.ClusterProtocolMinor ||
			voter.StateSchemaMajor != candidate.StateSchemaMajor ||
			voter.StateSchemaMinor != candidate.StateSchemaMinor ||
			voter.Phase != PeerPhaseVoter || voter.OperationID != "" ||
			voter.MemberID == candidate.MemberID || validateDynamicIdentity(voter, false) != nil ||
			(index > 0 && voters[index-1].MemberID == voter.MemberID) {
			return ErrInvalidPeerMessage
		}
	}
	return nil
}

func encodeSeedAdmissionResponse(response SeedAdmissionResponseDTO) ([]byte, error) {
	if !response.Accepted {
		return rejectedSeedAdmissionResponse(), nil
	}
	if validateAdmissionResult(response.Candidate, response) != nil {
		return nil, ErrInvalidPeerMessage
	}
	voters := append([]PeerIdentityDTO(nil), response.CurrentVoters...)
	sort.Slice(voters, func(left, right int) bool { return voters[left].MemberID < voters[right].MemberID })
	buffer := make([]byte, 0, 2048)
	buffer = append(buffer, "NMQJOIN2"...)
	buffer = appendUint16(buffer, dynamicProtocolMajor)
	buffer = appendUint16(buffer, dynamicProtocolMinor)
	buffer = append(buffer, 1)
	buffer = appendString16(buffer, response.Candidate.ClusterID)
	buffer = appendString16(buffer, response.Candidate.OperationID)
	buffer = appendUint64(buffer, response.Candidate.MemberID)
	buffer = append(buffer, response.Candidate.CertificateSHA256[:]...)
	buffer = appendString16(buffer, response.Candidate.DNSSAN)
	buffer = append(buffer, byte(response.Candidate.Phase))
	buffer = appendUint16(buffer, response.Candidate.StateSchemaMajor)
	buffer = appendUint16(buffer, response.Candidate.StateSchemaMinor)
	buffer = appendUint16(buffer, uint32(len(voters)))
	for _, voter := range voters {
		buffer = appendUint64(buffer, voter.MemberID)
		buffer = appendString16(buffer, voter.Address)
		buffer = appendString16(buffer, voter.DNSSAN)
		buffer = append(buffer, voter.CertificateSHA256[:]...)
		buffer = append(buffer, byte(PeerPhaseVoter))
	}
	if len(buffer) > dynamicAdmissionLimit {
		return nil, ErrInvalidPeerMessage
	}
	return buffer, nil
}

func decodeSeedAdmissionResponse(payload []byte) (SeedAdmissionResponseDTO, error) {
	if len(payload) < 13 || len(payload) > dynamicAdmissionLimit || string(payload[:8]) != "NMQJOIN2" ||
		binary.BigEndian.Uint16(payload[8:10]) != dynamicProtocolMajor ||
		binary.BigEndian.Uint16(payload[10:12]) != dynamicProtocolMinor {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	status := payload[12]
	if status == 0 {
		if len(payload) != 13 {
			return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
		}
		return SeedAdmissionResponseDTO{}, nil
	}
	if status != 1 {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	reader := dynamicWireReader{data: payload, position: 13}
	clusterID, ok := reader.string16(200, false)
	if !ok {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	operationLength, ok := reader.uint16()
	if !ok || (operationLength != 0 && operationLength != 36) {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	operationBytes, ok := reader.bytes(int(operationLength))
	if !ok {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	operationID := string(operationBytes)
	memberID, ok := reader.uint64()
	if !ok {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	fingerprint, ok := reader.bytes(sha256.Size)
	if !ok {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	dnsSAN, ok := reader.string16(253, true)
	if !ok {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	phase, ok := reader.byte()
	if !ok || !validAdmissionCandidateFields(PeerIdentityDTO{Phase: PeerPhaseDTO(phase), OperationID: operationID}) {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	schemaMajor, ok := reader.uint16()
	if !ok {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	schemaMinor, ok := reader.uint16()
	if !ok {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	count, ok := reader.uint16()
	if !ok || count != 3 {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	response := SeedAdmissionResponseDTO{Accepted: true}
	response.Candidate = PeerIdentityDTO{
		MemberID: memberID, ClusterID: clusterID, DNSSAN: dnsSAN,
		CertificateSHA256: [sha256.Size]byte{}, ClusterProtocolMajor: dynamicProtocolMajor,
		ClusterProtocolMinor: dynamicProtocolMinor, StateSchemaMajor: uint32(schemaMajor),
		StateSchemaMinor: uint32(schemaMinor), Phase: PeerPhaseDTO(phase), OperationID: operationID,
	}
	copy(response.Candidate.CertificateSHA256[:], fingerprint)
	lastID := uint64(0)
	for range count {
		voterID, ok := reader.uint64()
		if !ok || voterID == 0 || voterID <= lastID {
			return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
		}
		address, ok := reader.string16(255, false)
		if !ok {
			return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
		}
		voterDNS, ok := reader.string16(253, true)
		if !ok {
			return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
		}
		voterFingerprint, ok := reader.bytes(sha256.Size)
		if !ok {
			return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
		}
		voterPhase, ok := reader.byte()
		if !ok || PeerPhaseDTO(voterPhase) != PeerPhaseVoter {
			return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
		}
		voter := PeerIdentityDTO{
			MemberID: voterID, ClusterID: clusterID, Address: address, DNSSAN: voterDNS,
			ReleaseVersion: "", ClusterProtocolMajor: dynamicProtocolMajor,
			ClusterProtocolMinor: dynamicProtocolMinor, StateSchemaMajor: uint32(schemaMajor),
			StateSchemaMinor: uint32(schemaMinor), Phase: PeerPhaseVoter,
		}
		copy(voter.CertificateSHA256[:], voterFingerprint)
		response.CurrentVoters = append(response.CurrentVoters, voter)
		lastID = voterID
	}
	if reader.position != len(payload) {
		return SeedAdmissionResponseDTO{}, ErrInvalidPeerMessage
	}
	return response, nil
}

func rejectedSeedAdmissionResponse() []byte {
	response := make([]byte, 0, 13)
	response = append(response, "NMQJOIN2"...)
	response = appendUint16(response, dynamicProtocolMajor)
	response = appendUint16(response, dynamicProtocolMinor)
	return append(response, 0)
}

func readDynamicHandshakeFrame(connection net.Conn) ([]byte, error) {
	if err := connection.SetReadDeadline(time.Now().Add(peerIOTimeout)); err != nil {
		return nil, err
	}
	var prefix [4]byte
	if _, err := io.ReadFull(connection, prefix[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length < 8 || length > dynamicAdmissionLimit {
		return nil, ErrInvalidPeerMessage
	}
	var magic [8]byte
	if _, err := io.ReadFull(connection, magic[:]); err != nil {
		return nil, err
	}
	maximum := uint32(0)
	switch string(magic[:]) {
	case "NMQJOIN2":
		maximum = dynamicAdmissionLimit
	case dynamicRaftMagic:
		maximum = dynamicHelloLimit
	case dynamicForwardMagic:
		maximum = dynamicHelloLimit
	case dynamicBootstrapProbeMagic:
		maximum = dynamicHelloLimit
	default:
		return nil, ErrInvalidPeerMessage
	}
	if length > maximum {
		return nil, ErrInvalidPeerMessage
	}
	payload := make([]byte, int(length))
	copy(payload, magic[:])
	if _, err := io.ReadFull(connection, payload[8:]); err != nil {
		return nil, err
	}
	return payload, nil
}

func readDynamicFrame(connection net.Conn, maximum uint32) ([]byte, error) {
	if err := connection.SetReadDeadline(time.Now().Add(peerIOTimeout)); err != nil {
		return nil, err
	}
	var prefix [4]byte
	if _, err := io.ReadFull(connection, prefix[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length == 0 || length > maximum {
		return nil, ErrInvalidPeerMessage
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(connection, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func readDynamicIdleFrame(connection net.Conn, maximum uint32) ([]byte, error) {
	if err := connection.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	var prefix [4]byte
	if _, err := io.ReadFull(connection, prefix[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length == 0 || length > maximum {
		return nil, ErrInvalidPeerMessage
	}
	if err := connection.SetReadDeadline(time.Now().Add(peerIOTimeout)); err != nil {
		return nil, err
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(connection, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func writeDynamicFrame(connection net.Conn, payload []byte, maximum uint32) error {
	if len(payload) == 0 || uint64(len(payload)) > uint64(maximum) {
		return ErrInvalidPeerMessage
	}
	if err := connection.SetWriteDeadline(time.Now().Add(peerIOTimeout)); err != nil {
		return err
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
	if err := writeFull(connection, prefix[:]); err != nil {
		return err
	}
	return writeFull(connection, payload)
}

func appendUint16(destination []byte, value uint32) []byte {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], uint16(value))
	return append(destination, encoded[:]...)
}

func appendUint64(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendString16(destination []byte, value string) []byte {
	destination = appendUint16(destination, uint32(len(value)))
	return append(destination, value...)
}

func (reader *dynamicWireReader) operationID() (string, bool) {
	length := len(reader.data) - reader.position
	if length == 0 {
		return "", true
	}
	if length != 36 {
		return "", false
	}
	value, ok := reader.fixedString(length)
	return value, ok && validOperationID(value)
}

type dynamicWireReader struct {
	data     []byte
	position int
}

func (reader *dynamicWireReader) bytes(length int) ([]byte, bool) {
	if length < 0 || length > len(reader.data)-reader.position {
		return nil, false
	}
	value := reader.data[reader.position : reader.position+length]
	reader.position += length
	return value, true
}

func (reader *dynamicWireReader) byte() (byte, bool) {
	value, ok := reader.bytes(1)
	if !ok {
		return 0, false
	}
	return value[0], true
}

func (reader *dynamicWireReader) uint16() (uint16, bool) {
	value, ok := reader.bytes(2)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint16(value), true
}

func (reader *dynamicWireReader) uint64() (uint64, bool) {
	value, ok := reader.bytes(8)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint64(value), true
}

func (reader *dynamicWireReader) string16(maximum int, ascii bool) (string, bool) {
	length, ok := reader.uint16()
	if !ok || length == 0 || int(length) > maximum {
		return "", false
	}
	value, ok := reader.bytes(int(length))
	if !ok || !utf8.Valid(value) {
		return "", false
	}
	if ascii {
		for _, character := range value {
			if character > 127 {
				return "", false
			}
		}
	}
	return string(value), true
}

func (reader *dynamicWireReader) fixedString(length int) (string, bool) {
	value, ok := reader.bytes(length)
	if !ok {
		return "", false
	}
	for _, character := range value {
		if character > 127 {
			return "", false
		}
	}
	return string(value), true
}
