package tcp

import (
	"context"
	"net"
	"testing"
	"time"
)

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.4.
func TestDynamicOutboundCapacityRemainsChargedUntilWireCompletion(t *testing.T) {
	transport, peer := cptMinimalDynamic(
		dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11),
		dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22),
	)
	transport.outboundReady = make(chan struct{}, 1)
	for _, marker := range []byte{11, 22} {
		if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{marker}}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	local, remote := net.Pipe()
	done := cptRun(func() { peer.runConnection(ctx, local, peer.identityRevision) })
	t.Cleanup(func() { cancel(); remote.Close(); cptWaitDone(t, done, "outbound writer shutdown"); peer.stop() })
	cptWaitUntil(t, "in-progress write", func() bool {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		return peer.outCurrent != nil
	})
	capacity, err := transport.OutboundCapacity(2)
	if err != nil || capacity.Messages != peerQueueMessages-2 || capacity.Bytes != peerQueueBytes-10 {
		t.Fatalf("in-progress frame escaped accounting: capacity=%+v err=%v", capacity, err)
	}
	select {
	case <-transport.OutboundReady():
		t.Fatal("capacity advertised before the socket accepted its frame")
	default:
	}
	for _, marker := range []byte{11, 22} {
		remote.SetReadDeadline(time.Now().Add(cptWaitTimeout))
		payload, err := readDynamicFrame(remote, dynamicFrameLimit)
		if err != nil || len(payload) != 1 || payload[0] != marker {
			t.Fatalf("FIFO wire frame=%v err=%v want=%d", payload, err, marker)
		}
	}
	select {
	case <-transport.OutboundReady():
	case <-time.After(cptWaitTimeout):
		t.Fatal("wire completion did not wake the capacity consumer")
	}
	cptWaitUntil(t, "fully reclaimed outbound capacity", func() bool {
		capacity, err := transport.OutboundCapacity(2)
		return err == nil && capacity.Messages == peerQueueMessages && capacity.Bytes == peerQueueBytes
	})
}
