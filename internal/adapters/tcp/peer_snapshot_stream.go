package tcp

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"

	"narumq/internal/domain/release"
)

const (
	snapshotStreamOutboundSlots   = 2
	snapshotStreamAttempts        = 3
	snapshotStreamTransferTimeout = 2 * time.Minute
	snapshotStreamCompleteTimeout = 30 * time.Second
	snapshotStreamPartialTTL      = 30 * time.Second
)

// SnapshotStreamDTO is one serialized Raft MsgSnap above the peer frame limit (ADR-0062).
type SnapshotStreamDTO struct {
	To    uint64
	Index uint64
	Term  uint64
	// Data is the protobuf-serialized MsgSnap.
	Data []byte
}

// SnapshotStreamReceiptDTO is a snapshot received over the stream and verified against its manifest.
type SnapshotStreamReceiptDTO struct {
	From  uint64
	Index uint64
	Term  uint64
	// Data is the protobuf-serialized MsgSnap. The handler may keep it.
	Data []byte
}

// SnapshotStreamHandler hands a verified snapshot to the local Raft node. An error makes the
// receiver answer REJECTED.
type SnapshotStreamHandler func(context.Context, SnapshotStreamReceiptDTO) error

// SnapshotStreamError reports why one snapshot transfer failed. Reason is one of the
// SnapshotReason constants.
type SnapshotStreamError struct {
	Reason string
	Err    error
}

func (err *SnapshotStreamError) Error() string {
	if err.Err == nil {
		return "raft snapshot stream failed: " + err.Reason
	}
	return "raft snapshot stream failed: " + err.Reason + ": " + err.Err.Error()
}

func (err *SnapshotStreamError) Unwrap() error { return err.Err }

// SnapshotStreamFailureReason returns the stable reason of a SendSnapshot error.
func SnapshotStreamFailureReason(err error) string {
	var streamErr *SnapshotStreamError
	if errors.As(err, &streamErr) {
		return streamErr.Reason
	}
	return SnapshotReasonConnection
}

func snapshotFailure(reason string, err error) *SnapshotStreamError {
	return &SnapshotStreamError{Reason: reason, Err: err}
}

// snapshotPartial holds the bytes received so far from one sender so a reconnect can resume.
type snapshotPartial struct {
	from     uint64
	manifest snapshotManifest
	data     []byte
	timer    *time.Timer
}

func snapshotStreamMemberEligible(identity PeerIdentityDTO) bool {
	switch identity.Phase {
	case PeerPhaseLearner, PeerPhaseCatchingUp, PeerPhaseVoter, PeerPhaseLeaving:
		return true
	default:
		return false
	}
}

// SnapshotStreamChunkCount returns how many chunks a snapshot of totalBytes is sent in.
func SnapshotStreamChunkCount(totalBytes int) int {
	return (totalBytes + snapshotStreamChunkSize - 1) / snapshotStreamChunkSize
}

// SendSnapshot streams one oversized MsgSnap to a committed peer and returns when the peer handed
// it to its Raft node, or when the transfer failed. It blocks; the caller runs it outside the Raft
// Ready loop. The returned error is a *SnapshotStreamError.
func (transport *DynamicTCPPeerTransport) SendSnapshot(ctx context.Context, request SnapshotStreamDTO) error {
	if ctx == nil {
		return snapshotFailure(SnapshotReasonCanceled, errors.New("snapshot stream context is required"))
	}
	if request.To == 0 || request.Index == 0 || request.Term == 0 || len(request.Data) == 0 ||
		uint64(len(request.Data)) > snapshotStreamMaxBytes {
		return snapshotFailure(SnapshotReasonInvalidManifest, ErrInvalidPeerMessage)
	}
	transport.mu.Lock()
	peer, exists := transport.peers[request.To]
	runContext := transport.ctx
	started, closed := transport.started, transport.closed
	slots := transport.snapshotOutboundSlots
	transport.mu.Unlock()
	if closed {
		return snapshotFailure(SnapshotReasonCanceled, ErrPeerTransportClosed)
	}
	if !started || runContext == nil {
		return snapshotFailure(SnapshotReasonCanceled, errors.New("peer transport is not serving"))
	}
	if !exists {
		return snapshotFailure(SnapshotReasonConnection, fmt.Errorf("snapshot peer %d is not committed", request.To))
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		return snapshotFailure(SnapshotReasonBackpressure, ErrPeerBackpressure)
	}

	transferCtx, cancel := context.WithTimeout(ctx, snapshotStreamTransferTimeout)
	defer cancel()
	stopTransportCancellation := context.AfterFunc(runContext, cancel)
	defer stopTransportCancellation()
	peer.mu.Lock()
	peerContext := peer.ctx
	peer.mu.Unlock()
	if peerContext != nil {
		stopPeerCancellation := context.AfterFunc(peerContext, cancel)
		defer stopPeerCancellation()
	}

	manifest := snapshotManifest{
		index: request.Index, term: request.Term, total: uint64(len(request.Data)),
		digest: sha256.Sum256(request.Data),
	}
	var lastErr *SnapshotStreamError
	for attempt := range snapshotStreamAttempts {
		if attempt != 0 && !waitPeerRetry(transferCtx) {
			break
		}
		err := transport.sendSnapshotAttempt(transferCtx, peer, manifest, request.Data)
		if err == nil {
			return nil
		}
		lastErr = err
		if err.Reason != SnapshotReasonConnection {
			return err
		}
	}
	if err := transferCtx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return snapshotFailure(SnapshotReasonTimeout, err)
		}
		return snapshotFailure(SnapshotReasonCanceled, err)
	}
	return lastErr
}

// sendSnapshotAttempt performs one connection of a transfer. Only a SnapshotReasonConnection
// failure is worth another attempt; the receiver keeps the bytes it already has.
func (transport *DynamicTCPPeerTransport) sendSnapshotAttempt(
	ctx context.Context,
	peer *dynamicPeerState,
	manifest snapshotManifest,
	data []byte,
) (result *SnapshotStreamError) {
	defer func() {
		if result != nil && result.Reason == SnapshotReasonConnection && ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				result = snapshotFailure(SnapshotReasonTimeout, ctx.Err())
			} else {
				result = snapshotFailure(SnapshotReasonCanceled, ctx.Err())
			}
		}
	}()
	connectionFailure := func(err error) *SnapshotStreamError { return snapshotFailure(SnapshotReasonConnection, err) }

	peerIdentity, peerRevision, stopped := peer.identitySnapshot()
	transport.mu.Lock()
	local := transport.local
	transport.mu.Unlock()
	if stopped || !snapshotStreamMemberEligible(peerIdentity) || !snapshotStreamMemberEligible(local) {
		return connectionFailure(fmt.Errorf("snapshot peer %d is not committed", peer.id))
	}

	clientTLS := transport.clientTLS.Clone()
	clientTLS.ServerName = peerIdentity.DNSSAN
	dialer := &tls.Dialer{NetDialer: newPeerNetDialer(), Config: clientTLS}
	socket, err := dialer.DialContext(ctx, "tcp", peerIdentity.Address)
	if err != nil {
		return connectionFailure(fmt.Errorf("dial snapshot peer %d: %w", peer.id, err))
	}
	connection, ok := socket.(*tls.Conn)
	if !ok {
		_ = socket.Close()
		return connectionFailure(fmt.Errorf("snapshot peer %d did not establish TLS", peer.id))
	}
	if !transport.trackConnection(connection) {
		_ = connection.Close()
		return snapshotFailure(SnapshotReasonCanceled, ErrPeerTransportClosed)
	}
	defer func() {
		transport.untrackConnection(connection)
		_ = connection.Close()
	}()
	stopConnectionOnCancellation := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stopConnectionOnCancellation()
	if err := connection.HandshakeContext(ctx); err != nil {
		return connectionFailure(fmt.Errorf("handshake with snapshot peer %d: %w", peer.id, err))
	}
	// Verify the expected member before any hello leaves this node (ADR-0061).
	if !matchesPeerCertificate(connection, peerIdentity) {
		return connectionFailure(fmt.Errorf("snapshot peer %d certificate identity mismatch", peer.id))
	}
	if err := setConnectionDeadline(connection); err != nil {
		return connectionFailure(err)
	}
	localHello, err := encodeDynamicPeerHello(local, dynamicSnapshotMagic)
	if err != nil {
		return connectionFailure(err)
	}
	if err := writeDynamicFrame(connection, localHello, dynamicHelloLimit); err != nil {
		return connectionFailure(err)
	}
	remotePayload, err := readDynamicFrame(connection, dynamicHelloLimit)
	if err != nil {
		return connectionFailure(err)
	}
	remote, err := decodeDynamicPeerHello(remotePayload, dynamicSnapshotMagic)
	if err != nil {
		return connectionFailure(err)
	}
	if !compatiblePeerIdentity(peerIdentity, remote) || !snapshotStreamMemberEligible(remote) ||
		!release.CompatibleStrings(local.ReleaseVersion, remote.ReleaseVersion) {
		return connectionFailure(fmt.Errorf("snapshot peer %d identity is incompatible", peer.id))
	}
	currentPeer, currentRevision, stopped := peer.identitySnapshot()
	transport.mu.Lock()
	currentLocal, closed := transport.local, transport.closed
	registered := transport.peers[peer.id] == peer
	transport.mu.Unlock()
	if closed || stopped || !registered {
		return snapshotFailure(SnapshotReasonCanceled, ErrPeerTransportClosed)
	}
	if currentRevision != peerRevision || !snapshotStreamMemberEligible(currentPeer) ||
		!matchesPeerCertificate(connection, currentPeer) || !compatiblePeerIdentity(currentPeer, remote) ||
		!snapshotStreamMemberEligible(currentLocal) || !compatiblePeerIdentity(local, currentLocal) {
		return connectionFailure(fmt.Errorf("snapshot peer %d changed while establishing the stream", peer.id))
	}

	if err := writeDynamicFrame(connection, encodeSnapshotManifest(manifest), snapshotStreamFrameLimit); err != nil {
		return connectionFailure(err)
	}
	accept, err := readDynamicFrame(connection, snapshotStreamFrameLimit)
	if err != nil {
		return connectionFailure(err)
	}
	if failure := snapshotPeerError(accept); failure != nil {
		return failure
	}
	offset, ok := decodeSnapshotOffsetFrame(accept, snapshotFrameAccept)
	if !ok || offset > manifest.total {
		return snapshotFailure(SnapshotReasonOutOfOrder, errors.New("snapshot peer returned an invalid resume offset"))
	}
	buffer := make([]byte, 0, snapshotChunkHeaderSize+snapshotStreamChunkSize)
	for offset < manifest.total {
		end := min(offset+snapshotStreamChunkSize, manifest.total)
		buffer = encodeSnapshotChunk(buffer, offset, data[offset:end])
		if err := writeDynamicFrame(connection, buffer, snapshotStreamFrameLimit); err != nil {
			return connectionFailure(err)
		}
		ack, err := readDynamicFrame(connection, snapshotStreamFrameLimit)
		if err != nil {
			return connectionFailure(err)
		}
		if failure := snapshotPeerError(ack); failure != nil {
			return failure
		}
		next, ok := decodeSnapshotOffsetFrame(ack, snapshotFrameAck)
		if !ok || next != end {
			return snapshotFailure(SnapshotReasonOutOfOrder, errors.New("snapshot peer acknowledged an unexpected offset"))
		}
		offset = next
	}
	if err := writeDynamicFrame(connection, []byte{snapshotFrameDone}, snapshotStreamFrameLimit); err != nil {
		return connectionFailure(err)
	}
	complete, err := readSnapshotFrame(connection, snapshotStreamCompleteTimeout)
	if err != nil {
		return connectionFailure(err)
	}
	if failure := snapshotPeerError(complete); failure != nil {
		return failure
	}
	if len(complete) != 1 || complete[0] != snapshotFrameComplete {
		return snapshotFailure(SnapshotReasonOutOfOrder, errors.New("snapshot peer sent an unexpected completion"))
	}
	return nil
}

// snapshotPeerError maps an ERROR frame to a failure. Other frames return nil.
func snapshotPeerError(payload []byte) *SnapshotStreamError {
	if len(payload) == 0 || payload[0] != snapshotFrameError {
		return nil
	}
	if len(payload) != 2 {
		return snapshotFailure(SnapshotReasonOutOfOrder, errors.New("snapshot peer sent a malformed error"))
	}
	return snapshotFailure(snapshotErrorReason(payload[1]), errors.New("snapshot peer rejected the transfer"))
}

// readSnapshotFrame reads one frame that may arrive after a longer wait than peerIOTimeout.
func readSnapshotFrame(connection net.Conn, timeout time.Duration) ([]byte, error) {
	if err := connection.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	var prefix [4]byte
	if _, err := io.ReadFull(connection, prefix[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length == 0 || length > snapshotStreamFrameLimit {
		return nil, ErrInvalidPeerMessage
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(connection, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (transport *DynamicTCPPeerTransport) handleSnapshotInbound(
	ctx context.Context,
	connection *tls.Conn,
	payload []byte,
) {
	remote, err := decodeDynamicPeerHello(payload, dynamicSnapshotMagic)
	if err != nil {
		return
	}
	transport.mu.Lock()
	peer, exists := transport.peers[remote.MemberID]
	local := transport.local
	closed := transport.closed
	transport.mu.Unlock()
	if closed || !exists || !snapshotStreamMemberEligible(local) {
		return
	}
	// authorized repeats the checks of the Raft lane. It runs before accepting memory and again
	// before the snapshot reaches Raft, because commit changes can land in between.
	authorized := func() (PeerIdentityDTO, bool) {
		transport.mu.Lock()
		current, currentLocal, stillClosed := transport.peers[remote.MemberID], transport.local, transport.closed
		transport.mu.Unlock()
		if stillClosed || current != peer || !snapshotStreamMemberEligible(currentLocal) ||
			!compatiblePeerIdentity(local, currentLocal) {
			return PeerIdentityDTO{}, false
		}
		expected, _, stopped := current.identitySnapshot()
		if stopped || !snapshotStreamMemberEligible(expected) || !matchesPeerCertificate(connection, expected) ||
			!compatiblePeerIdentity(expected, remote) ||
			!release.CompatibleStrings(currentLocal.ReleaseVersion, remote.ReleaseVersion) {
			return PeerIdentityDTO{}, false
		}
		return currentLocal, true
	}
	if _, ok := authorized(); !ok {
		return
	}
	localHello, err := encodeDynamicPeerHello(local, dynamicSnapshotMagic)
	if err != nil || writeDynamicFrame(connection, localHello, dynamicHelloLimit) != nil {
		return
	}
	reject := func(code byte) {
		_ = writeDynamicFrame(connection, []byte{snapshotFrameError, code}, snapshotStreamFrameLimit)
	}

	frame, err := readDynamicFrame(connection, snapshotStreamFrameLimit)
	if err != nil {
		return
	}
	manifest, ok := decodeSnapshotManifest(frame)
	if !ok {
		reject(snapshotErrorInvalidManifest)
		return
	}
	if transport.snapshotHandler == nil {
		reject(snapshotErrorUnsupported)
		return
	}
	select {
	case transport.snapshotInboundSlot <- struct{}{}:
		defer func() { <-transport.snapshotInboundSlot }()
	default:
		reject(snapshotErrorBusy)
		return
	}
	transferContext, cancelTransfer := context.WithTimeout(ctx, snapshotStreamTransferTimeout)
	defer cancelTransfer()
	stopOnTimeout := context.AfterFunc(transferContext, func() { _ = connection.Close() })
	defer stopOnTimeout()

	partial := transport.takeSnapshotPartial(remote.MemberID, manifest)
	// keep decides what happens to the bytes when this connection ends before COMPLETE.
	keep := true
	defer func() {
		if keep {
			transport.retainSnapshotPartial(partial)
		}
	}()
	if err := writeDynamicFrame(connection, encodeSnapshotOffsetFrame(snapshotFrameAccept, uint64(len(partial.data))),
		snapshotStreamFrameLimit); err != nil {
		return
	}
	for {
		frame, err := readDynamicFrame(connection, snapshotStreamFrameLimit)
		if err != nil || transferContext.Err() != nil {
			var timeout net.Error
			if transferContext.Err() != nil || errors.As(err, &timeout) && timeout.Timeout() {
				keep = false
			}
			return
		}
		switch frame[0] {
		case snapshotFrameChunk:
			offset, checksum, data, ok := decodeSnapshotChunk(frame)
			switch {
			case !ok || offset != uint64(len(partial.data)):
				keep = false
				reject(snapshotErrorOutOfOrder)
				return
			case uint64(len(partial.data))+uint64(len(data)) > manifest.total:
				keep = false
				reject(snapshotErrorOverflow)
				return
			case crc32.Checksum(data, snapshotCRCTable) != checksum:
				keep = false
				reject(snapshotErrorChecksum)
				return
			}
			if partial.data == nil {
				// Reserve once, only after the first valid chunk. Never grow beyond total_bytes.
				partial.data = make([]byte, 0, int(manifest.total))
			}
			partial.data = append(partial.data, data...)
			if err := writeDynamicFrame(connection, encodeSnapshotOffsetFrame(snapshotFrameAck, uint64(len(partial.data))),
				snapshotStreamFrameLimit); err != nil {
				return
			}
		case snapshotFrameDone:
			keep = false
			switch {
			case len(frame) != 1 || uint64(len(partial.data)) != manifest.total:
				reject(snapshotErrorOverflow)
				return
			case sha256.Sum256(partial.data) != manifest.digest:
				reject(snapshotErrorDigest)
				return
			}
			if _, ok := authorized(); !ok {
				return
			}
			handlerContext, cancel := context.WithTimeout(transferContext, snapshotStreamCompleteTimeout)
			err := transport.snapshotHandler(handlerContext, SnapshotStreamReceiptDTO{
				From: remote.MemberID, Index: manifest.index, Term: manifest.term, Data: partial.data,
			})
			cancel()
			if err != nil {
				reject(snapshotErrorRejected)
				return
			}
			_ = writeDynamicFrame(connection, []byte{snapshotFrameComplete}, snapshotStreamFrameLimit)
			return
		default:
			keep = false
			reject(snapshotErrorOutOfOrder)
			return
		}
	}
}

// takeSnapshotPartial returns the retained bytes of the same transfer from the same sender, or a
// fresh empty state. Any other retained state is dropped: a newer manifest supersedes it.
func (transport *DynamicTCPPeerTransport) takeSnapshotPartial(from uint64, manifest snapshotManifest) *snapshotPartial {
	transport.snapshotMu.Lock()
	defer transport.snapshotMu.Unlock()
	retained := transport.snapshotPartial
	transport.snapshotPartial = nil
	if retained != nil {
		if retained.timer != nil {
			retained.timer.Stop()
			retained.timer = nil
		}
		if retained.from == from && retained.manifest == manifest {
			return retained
		}
	}
	return &snapshotPartial{from: from, manifest: manifest}
}

// retainSnapshotPartial keeps the bytes of an interrupted transfer for snapshotStreamPartialTTL.
func (transport *DynamicTCPPeerTransport) retainSnapshotPartial(partial *snapshotPartial) {
	if len(partial.data) == 0 {
		return
	}
	transport.snapshotMu.Lock()
	defer transport.snapshotMu.Unlock()
	if transport.isClosed() {
		return
	}
	transport.snapshotPartial = partial
	partial.timer = time.AfterFunc(snapshotStreamPartialTTL, func() {
		transport.snapshotMu.Lock()
		defer transport.snapshotMu.Unlock()
		if transport.snapshotPartial == partial {
			transport.snapshotPartial = nil
		}
	})
}

// isClosed reports whether the transport shut down. It takes transport.mu, so callers must not hold it.
func (transport *DynamicTCPPeerTransport) isClosed() bool {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.closed
}
