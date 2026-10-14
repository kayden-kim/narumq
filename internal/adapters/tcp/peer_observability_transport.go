package tcp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"narumq/internal/domain/release"
)

// QueryObservability requests one node-local snapshot over an isolated mTLS connection.
func (transport *DynamicTCPPeerTransport) QueryObservability(
	ctx context.Context,
	peerID uint64,
	kind PeerObservabilityKind,
) (PeerObservabilityResponseDTO, error) {
	if ctx == nil {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("observability query context is required")
	}
	if err := ctx.Err(); err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	if !validPeerObservabilityKind(kind) || peerID == 0 {
		return PeerObservabilityResponseDTO{}, ErrInvalidPeerMessage
	}

	transport.mu.Lock()
	peer, exists := transport.peers[peerID]
	local := transport.local
	runContext := transport.ctx
	started := transport.started
	closed := transport.closed
	transport.mu.Unlock()
	if closed {
		return PeerObservabilityResponseDTO{}, ErrPeerTransportClosed
	}
	if !started || runContext == nil {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("peer transport is not serving")
	}
	if !exists {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("observability peer %d is not committed", peerID)
	}
	peerIdentity, peerRevision, stopped := peer.identitySnapshot()
	if stopped || !peerObservabilityMemberEligible(peerIdentity) {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("observability peer %d is not committed", peerID)
	}
	if !peerObservabilityMemberEligible(local) {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("local peer %d is not committed", local.MemberID)
	}
	if !local.ObservabilitySupported || !peerIdentity.ObservabilitySupported {
		if err := ctx.Err(); err != nil {
			return PeerObservabilityResponseDTO{}, err
		}
		return unsupportedPeerObservabilityResponse(peerID, kind), nil
	}

	rpcContext, cancel := context.WithTimeout(ctx, peerIOTimeout)
	defer cancel()
	stopTransportCancellation := context.AfterFunc(runContext, cancel)
	defer stopTransportCancellation()
	select {
	case transport.observabilityOutboundSlots <- struct{}{}:
		defer func() { <-transport.observabilityOutboundSlots }()
	case <-rpcContext.Done():
		return PeerObservabilityResponseDTO{}, rpcContext.Err()
	case <-runContext.Done():
		return PeerObservabilityResponseDTO{}, ErrPeerTransportClosed
	}

	clientTLS := transport.clientTLS.Clone()
	clientTLS.ServerName = peerIdentity.DNSSAN
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: peerIOTimeout},
		Config:    clientTLS,
	}
	socket, err := dialer.DialContext(rpcContext, "tcp", peerIdentity.Address)
	if err != nil {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("dial observability peer %d: %w", peerID, err)
	}
	connection, ok := socket.(*tls.Conn)
	if !ok {
		_ = socket.Close()
		return PeerObservabilityResponseDTO{}, fmt.Errorf("observability peer %d did not establish TLS", peerID)
	}
	if !transport.trackConnection(connection) {
		_ = connection.Close()
		return PeerObservabilityResponseDTO{}, ErrPeerTransportClosed
	}
	defer func() {
		transport.untrackConnection(connection)
		_ = connection.Close()
	}()
	stopConnectionOnCancellation := context.AfterFunc(rpcContext, func() { _ = connection.Close() })
	defer stopConnectionOnCancellation()
	if err := connection.HandshakeContext(rpcContext); err != nil {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("handshake with observability peer %d: %w", peerID, err)
	}
	if !matchesPeerCertificate(connection, peerIdentity) {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("observability peer %d certificate identity mismatch", peerID)
	}
	if err := setConnectionDeadline(connection); err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	localHello, err := encodeDynamicPeerHello(local, peerObservabilityMagic)
	if err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	if err := writeDynamicFrame(connection, localHello, dynamicHelloLimit); err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	remotePayload, err := readDynamicFrame(connection, dynamicHelloLimit)
	if err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	remoteIdentity, err := decodeDynamicPeerHello(remotePayload, peerObservabilityMagic)
	if err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	if !compatiblePeerIdentity(peerIdentity, remoteIdentity) || !peerObservabilityMemberEligible(remoteIdentity) ||
		!release.CompatibleStrings(local.ReleaseVersion, remoteIdentity.ReleaseVersion) {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("observability peer %d identity is incompatible", peerID)
	}

	currentPeerIdentity, currentPeerRevision, stopped := peer.identitySnapshot()
	transport.mu.Lock()
	currentLocalIdentity := transport.local
	transportClosed := transport.closed
	transport.mu.Unlock()
	if transportClosed || stopped {
		return PeerObservabilityResponseDTO{}, ErrPeerTransportClosed
	}
	if currentPeerRevision != peerRevision || !peerObservabilityMemberEligible(currentPeerIdentity) ||
		!matchesPeerCertificate(connection, currentPeerIdentity) ||
		!compatiblePeerIdentity(currentPeerIdentity, remoteIdentity) {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("observability peer %d changed while establishing the query", peerID)
	}
	if !peerObservabilityMemberEligible(currentLocalIdentity) ||
		!compatiblePeerIdentity(local, currentLocalIdentity) ||
		!release.CompatibleStrings(currentLocalIdentity.ReleaseVersion, remoteIdentity.ReleaseVersion) {
		return PeerObservabilityResponseDTO{}, fmt.Errorf("local peer %d changed while establishing the query", currentLocalIdentity.MemberID)
	}
	if !currentLocalIdentity.ObservabilitySupported || !currentPeerIdentity.ObservabilitySupported {
		if err := rpcContext.Err(); err != nil {
			return PeerObservabilityResponseDTO{}, err
		}
		return unsupportedPeerObservabilityResponse(peerID, kind), nil
	}
	requestPayload, err := encodePeerObservabilityRequest(PeerObservabilityRequestDTO{Kind: kind, TargetMemberID: peerID})
	if err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	if err := writeDynamicFrame(connection, requestPayload, peerObservabilityRequestSize); err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	responsePayload, err := readDynamicFrame(connection, uint32(peerObservabilityResponseLimit))
	if err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	response, err := decodePeerObservabilityResponse(responsePayload, kind, peerID)
	if err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	if err := waitForDynamicPeerClose(connection, "observability"); err != nil {
		return PeerObservabilityResponseDTO{}, err
	}
	return response, nil
}

func (transport *DynamicTCPPeerTransport) handleObservabilityInbound(
	ctx context.Context,
	connection *tls.Conn,
	payload []byte,
) {
	remoteIdentity, err := decodeDynamicPeerHello(payload, peerObservabilityMagic)
	if err != nil {
		return
	}
	transport.mu.Lock()
	peer, exists := transport.peers[remoteIdentity.MemberID]
	localIdentity := transport.local
	closed := transport.closed
	handler := transport.observabilityHandler
	slots := transport.observabilityInboundSlots
	transport.mu.Unlock()
	if closed || !exists || !peerObservabilityMemberEligible(localIdentity) {
		return
	}
	expectedIdentity, _, stopped := peer.identitySnapshot()
	if stopped || !peerObservabilityMemberEligible(expectedIdentity) ||
		!matchesPeerCertificate(connection, expectedIdentity) ||
		!compatiblePeerIdentity(expectedIdentity, remoteIdentity) ||
		!release.CompatibleStrings(localIdentity.ReleaseVersion, remoteIdentity.ReleaseVersion) {
		return
	}
	localHello, err := encodeDynamicPeerHello(localIdentity, peerObservabilityMagic)
	if err != nil || writeDynamicFrame(connection, localHello, dynamicHelloLimit) != nil {
		return
	}
	requestPayload, err := readDynamicFrame(connection, peerObservabilityRequestSize)
	if err != nil {
		return
	}
	request, err := decodePeerObservabilityRequest(requestPayload)
	if err != nil || request.TargetMemberID != localIdentity.MemberID {
		return
	}
	transport.mu.Lock()
	currentPeer, peerStillCommitted := transport.peers[remoteIdentity.MemberID]
	currentLocalIdentity := transport.local
	closed = transport.closed
	handler = transport.observabilityHandler
	transport.mu.Unlock()
	if closed || !peerStillCommitted || currentPeer != peer ||
		!peerObservabilityMemberEligible(currentLocalIdentity) ||
		!compatiblePeerIdentity(localIdentity, currentLocalIdentity) {
		return
	}
	currentPeerIdentity, _, stopped := currentPeer.identitySnapshot()
	if stopped || !peerObservabilityMemberEligible(currentPeerIdentity) ||
		!matchesPeerCertificate(connection, currentPeerIdentity) ||
		!compatiblePeerIdentity(currentPeerIdentity, remoteIdentity) ||
		!release.CompatibleStrings(currentLocalIdentity.ReleaseVersion, remoteIdentity.ReleaseVersion) {
		return
	}
	localIdentity, expectedIdentity = currentLocalIdentity, currentPeerIdentity
	if !localIdentity.ObservabilitySupported || !expectedIdentity.ObservabilitySupported {
		_ = writePeerObservabilityResponse(connection, localIdentity.MemberID, request.Kind, PeerObservabilityStatusUnsupported, nil)
		return
	}
	if handler == nil {
		_ = writePeerObservabilityResponse(connection, localIdentity.MemberID, request.Kind, PeerObservabilityStatusInternalError, nil)
		return
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		_ = writePeerObservabilityResponse(connection, localIdentity.MemberID, request.Kind, PeerObservabilityStatusBusy, nil)
		return
	}
	if err := connection.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	handlerContext, cancel := context.WithTimeout(ctx, peerIOTimeout)
	peerClosed := make(chan struct{})
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		var trailing [1]byte
		for {
			n, readErr := connection.Read(trailing[:])
			if n > 0 || readErr != nil {
				close(peerClosed)
				cancel()
				return
			}
		}
	}()
	body, handlerErr := handler(handlerContext, request.Kind)
	cancel()
	select {
	case <-peerClosed:
		_ = connection.Close()
		<-monitorDone
		return
	default:
	}
	if ctx.Err() != nil {
		_ = connection.Close()
		<-monitorDone
		return
	}
	status := PeerObservabilityStatusOK
	if handlerErr != nil {
		status = PeerObservabilityStatusInternalError
		body = nil
	} else if len(body) > peerObservabilityResponseLimit-peerObservabilityResponseHeaderSize {
		status = PeerObservabilityStatusSnapshotTooLarge
		body = nil
	}
	_ = writePeerObservabilityResponse(connection, localIdentity.MemberID, request.Kind, status, body)
	_ = connection.Close()
	<-monitorDone
}

func writePeerObservabilityResponse(
	connection net.Conn,
	responderID uint64,
	kind PeerObservabilityKind,
	status PeerObservabilityStatus,
	body []byte,
) error {
	payload, err := encodePeerObservabilityResponse(PeerObservabilityResponseDTO{
		Status: status, ResponderMemberID: responderID, Kind: kind, Body: body,
	})
	if err != nil {
		payload, err = encodePeerObservabilityResponse(PeerObservabilityResponseDTO{
			Status: PeerObservabilityStatusInternalError, ResponderMemberID: responderID, Kind: kind,
		})
		if err != nil {
			return err
		}
	}
	return writeDynamicFrame(connection, payload, uint32(peerObservabilityResponseLimit))
}

func unsupportedPeerObservabilityResponse(peerID uint64, kind PeerObservabilityKind) PeerObservabilityResponseDTO {
	return PeerObservabilityResponseDTO{
		Status: PeerObservabilityStatusUnsupported, ResponderMemberID: peerID, Kind: kind,
	}
}

func peerObservabilityMemberEligible(identity PeerIdentityDTO) bool {
	switch identity.Phase {
	case PeerPhaseLearner, PeerPhaseCatchingUp, PeerPhaseVoter, PeerPhaseLeaving:
		return true
	default:
		return false
	}
}
