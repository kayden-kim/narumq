package tcp

// PeerOutboundCapacityDTO is a snapshot, not a reservation or delivery guarantee.
// An active socket write remains charged until it completes.
type PeerOutboundCapacityDTO struct {
	Messages int
	Bytes    int
}

// OutboundCapacity returns unused capacity in one dynamic Raft peer FIFO.
func (transport *DynamicTCPPeerTransport) OutboundCapacity(peerID uint64) (PeerOutboundCapacityDTO, error) {
	transport.mu.Lock()
	peer, found := transport.peers[peerID]
	closed := transport.closed
	transport.mu.Unlock()
	if closed {
		return PeerOutboundCapacityDTO{}, ErrPeerTransportClosed
	}
	if !found {
		return PeerOutboundCapacityDTO{}, ErrUnknownPeer
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.stopped {
		return PeerOutboundCapacityDTO{}, ErrPeerTransportClosed
	}
	return PeerOutboundCapacityDTO{
		Messages: peerQueueMessages - peer.outMessages,
		Bytes:    peerQueueBytes - peer.outBytes,
	}, nil
}

// OutboundReady coalesces completed Raft writes. Consumers must re-read capacity.
func (transport *DynamicTCPPeerTransport) OutboundReady() <-chan struct{} {
	return transport.outboundReady
}

func (transport *DynamicTCPPeerTransport) signalOutboundCapacity() {
	select {
	case transport.outboundReady <- struct{}{}:
	default:
	}
}
