package tcp

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"time"
)

// receiveRaftFrame reserves staged and in-progress bytes before allocation.
// Idle prefix reads retain the existing behavior; capacity and payload share
// the payload deadline. runConnection closes the socket on reader cancellation.
func (peer *dynamicPeerState) receiveRaftFrame(ctx context.Context, connection net.Conn, revision uint64) error {
	if err := connection.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	var prefix [4]byte
	if _, err := io.ReadFull(connection, prefix[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length == 0 || length > dynamicFrameLimit {
		return ErrInvalidPeerMessage
	}
	deadline := time.Now().Add(peerIOTimeout)
	if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	frame := peerFrame{size: int(length) + 4, identityRevision: revision}
	if err := peer.reserveInbound(ctx, connection, revision, frame.size, deadline); err != nil {
		return err
	}
	staged := false
	defer func() {
		if !staged {
			peer.completeInbound(frame)
		}
	}()
	frame.data = make([]byte, int(length))
	if _, err := io.ReadFull(connection, frame.data); err != nil {
		return err
	}
	if peer.owner != nil {
		peer.owner.clearBootstrapAllowance(peer.id)
	}
	peer.mu.Lock()
	if err := ctx.Err(); err != nil {
		peer.mu.Unlock()
		return err
	}
	if peer.stopped || peer.identityChanging || peer.identityRevision != revision || peer.connection != connection {
		peer.mu.Unlock()
		return ErrPeerTransportClosed
	}
	frame.enqueuedAt = time.Now()
	peer.inbox <- frame
	staged = true
	peer.mu.Unlock()
	if peer.owner != nil {
		peer.owner.signalLane(peer.lane)
	}
	return nil
}

func (peer *dynamicPeerState) reserveInbound(ctx context.Context, connection net.Conn, revision uint64, size int, deadline time.Time) error {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return os.ErrDeadlineExceeded
		}
		peer.mu.Lock()
		if peer.stopped || peer.identityChanging || peer.identityRevision != revision || peer.connection != connection {
			peer.mu.Unlock()
			return ErrPeerTransportClosed
		}
		if peer.inMessages < peerQueueMessages && peer.inBytes+size <= peerQueueBytes {
			peer.inMessages++
			peer.inBytes += size
			peer.mu.Unlock()
			return nil
		}
		wake := peer.capacityWake
		peer.mu.Unlock()
		if timer == nil {
			timer = time.NewTimer(time.Until(deadline))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return os.ErrDeadlineExceeded
		case <-wake:
		}
	}
}

// The session mutex permits one reader per peer; one coalesced wake is enough.
func (peer *dynamicPeerState) signalInboundCapacityLocked() {
	select {
	case peer.capacityWake <- struct{}{}:
	default:
	}
}
