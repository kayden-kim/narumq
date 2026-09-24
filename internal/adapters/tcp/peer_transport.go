package tcp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
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

var (
	ErrPeerBackpressure    = errors.New("raft peer outbound queue is full")
	ErrPeerTransportClosed = errors.New("raft peer transport is closed")
	ErrInvalidPeerMessage  = errors.New("invalid raft peer message")
	ErrUnknownPeer         = errors.New("unknown raft peer")
)

// PeerMessageDTO is a transport-owned envelope around opaque serialized Raft bytes.
// Data ownership transfers to the receiver after Receive returns the message.
type PeerMessageDTO struct {
	From uint64
	To   uint64
	Data []byte
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
	data []byte
	size int
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
	if errors.Is(err, net.ErrClosed) || runCtx.Err() != nil {
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
