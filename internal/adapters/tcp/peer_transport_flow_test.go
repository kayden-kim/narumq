package tcp

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func TestDynamicRaftInboundBackpressureResumesFIFO(t *testing.T) {
	for _, test := range []struct {
		name               string
		count, payloadSize int
	}{
		{name: "message cap", count: peerQueueMessages, payloadSize: 1},
		{name: "byte cap", count: 3, payloadSize: int(dynamicFrameLimit)},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport, peer := cptMinimalDynamic(
				dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11),
				dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22),
			)
			for index := range test.count {
				data := make([]byte, test.payloadSize)
				data[0] = byte(index)
				if err := peer.enqueueInbound(data, peer.identityRevision); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			local, remote := net.Pipe()
			done := cptRun(func() { peer.runConnection(ctx, local, peer.identityRevision) })
			t.Cleanup(func() { cancel(); remote.Close(); cptWaitDone(t, done, "session cleanup"); peer.stop() })
			remote.SetWriteDeadline(time.Now().Add(cptWaitTimeout))
			if _, err := remote.Write(cptFramePrefix(uint32(test.payloadSize))); err != nil {
				t.Fatal(err)
			}
			first, ok := transport.TakeInbound()
			if !ok || first.Message.Data[0] != 0 {
				t.Fatal("oldest frame was not leased")
			}
			payload := make([]byte, test.payloadSize)
			payload[0] = 0xfe
			remote.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
			n, err := remote.Write(payload)
			var timeout net.Error
			if n != 0 || !errors.As(err, &timeout) || !timeout.Timeout() {
				t.Fatalf("full queue accepted payload: n=%d err=%v", n, err)
			}
			status := cptDynamicStatus(peer)
			if !status.Connected || status.InboundQueuedMessages != test.count || status.InboundQueuedBytes != test.count*(test.payloadSize+4) {
				t.Fatalf("lease bypassed capacity or closed connection: %+v", status)
			}
			first.Complete()
			remote.SetWriteDeadline(time.Now().Add(cptWaitTimeout))
			if err := writeFull(remote, payload); err != nil {
				t.Fatal(err)
			}
			for index := 1; index <= test.count; index++ {
				var delivery PeerInboundDelivery
				cptWaitUntil(t, "next FIFO delivery", func() bool { var ok bool; delivery, ok = transport.TakeInbound(); return ok })
				want := byte(index)
				if index == test.count {
					want = 0xfe
				}
				if len(delivery.Message.Data) != test.payloadSize || delivery.Message.Data[0] != want {
					t.Fatalf("delivery %d: size=%d marker=%d want=%d", index, len(delivery.Message.Data), delivery.Message.Data[0], want)
				}
				status = cptDynamicStatus(peer)
				if status.InboundQueuedMessages > peerQueueMessages || status.InboundQueuedBytes > peerQueueBytes {
					t.Fatalf("capacity exceeded: %+v", status)
				}
				delivery.Complete()
			}
			status = cptDynamicStatus(peer)
			if status.InboundQueuedMessages != 0 || status.InboundQueuedBytes != 0 {
				t.Fatalf("reservation leaked: %+v", status)
			}
		})
	}
}

func TestDynamicRaftInboundWaitUnblocksOnLifecycleChange(t *testing.T) {
	for _, reason := range []string{"cancel", "identity", "stop", "replace", "write failure"} {
		t.Run(reason, func(t *testing.T) {
			transport, peer := cptMinimalDynamic(
				dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11),
				dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22),
			)
			for range peerQueueMessages {
				if err := peer.enqueueInbound([]byte{1}, peer.identityRevision); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			local, remote := net.Pipe()
			connection := &cptConn{Conn: local, writeErr: errors.New("write failure")}
			done := cptRun(func() { peer.runConnection(ctx, connection, peer.identityRevision) })
			t.Cleanup(func() { cancel(); remote.Close(); cptWaitDone(t, done, "session cleanup"); peer.stop() })
			remote.SetWriteDeadline(time.Now().Add(cptWaitTimeout))
			if _, err := remote.Write(cptFramePrefix(1)); err != nil {
				t.Fatal(err)
			}
			switch reason {
			case "cancel":
				cancel()
			case "identity":
				peer.mu.Lock()
				peer.beginIdentityChangeLocked()
				peer.advanceIdentityRevisionLocked()
				peer.mu.Unlock()
			case "stop":
				peer.stop()
			case "replace":
				newLocal, newRemote := net.Pipe()
				defer newRemote.Close()
				second := cptRun(func() { peer.runConnection(ctx, newLocal, peer.identityRevision) })
				cptWaitDone(t, done, "replaced session exit")
				cancel()
				cptWaitDone(t, second, "replacement session exit")
			case "write failure":
				if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{2}}); err != nil {
					t.Fatal(err)
				}
			}
			cptWaitDone(t, done, "capacity wait cancellation")
			status := cptDynamicStatus(peer)
			wantCount := peerQueueMessages
			if reason == "identity" || reason == "stop" {
				wantCount = 0
			}
			if status.Connected || status.InboundQueuedMessages != wantCount || status.InboundQueuedBytes != 5*wantCount {
				t.Fatalf("lifecycle change leaked or dispatched a waiting frame: %+v", status)
			}
		})
	}
}

func TestDynamicRaftInboundReservationReturnsOnTruncatedPayload(t *testing.T) {
	peer := newDynamicPeerState(nil, dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22))
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	peer.connection = local
	result := make(chan error, 1)
	go func() { result <- peer.receiveRaftFrame(context.Background(), local, peer.identityRevision) }()
	remote.SetWriteDeadline(time.Now().Add(cptWaitTimeout))
	if _, err := remote.Write(cptFramePrefix(128)); err != nil {
		t.Fatal(err)
	}
	cptWaitUntil(t, "in-progress reservation", func() bool {
		status := cptDynamicStatus(peer)
		return status.InboundQueuedMessages == 1 && status.InboundQueuedBytes == 132
	})
	if _, err := remote.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	remote.Close()
	select {
	case err := <-result:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("truncated payload: %v", err)
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("truncated frame reader did not exit")
	}
	status := cptDynamicStatus(peer)
	if status.InboundQueuedMessages != 0 || status.InboundQueuedBytes != 0 || len(peer.inbox) != 0 {
		t.Fatalf("truncated frame leaked or dispatched: %+v", status)
	}
}

func TestDynamicRaftInboundCapacityDeadlineRejectsWithoutReservation(t *testing.T) {
	peer := newDynamicPeerState(nil, dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22))
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	peer.connection = local
	for range peerQueueMessages {
		if err := peer.enqueueInbound([]byte{1}, peer.identityRevision); err != nil {
			t.Fatal(err)
		}
	}
	err := peer.reserveInbound(context.Background(), local, peer.identityRevision, 5, time.Now().Add(20*time.Millisecond))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("capacity deadline: %v", err)
	}
	status := cptDynamicStatus(peer)
	if status.InboundQueuedMessages != peerQueueMessages || status.InboundQueuedBytes != 5*peerQueueMessages {
		t.Fatalf("timed-out reservation changed queue: %+v", status)
	}
}

func TestDynamicRaftInboundInvalidLengthIsNeverStaged(t *testing.T) {
	for _, length := range []uint32{0, dynamicFrameLimit + 1} {
		peer := newDynamicPeerState(nil, dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22))
		local, remote := net.Pipe()
		peer.connection = local
		result := make(chan error, 1)
		go func() { result <- peer.receiveRaftFrame(context.Background(), local, peer.identityRevision) }()
		remote.SetWriteDeadline(time.Now().Add(cptWaitTimeout))
		if _, err := remote.Write(cptFramePrefix(length)); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-result:
			if !errors.Is(err, ErrInvalidPeerMessage) {
				t.Fatalf("length %d: %v", length, err)
			}
		case <-time.After(cptWaitTimeout):
			t.Fatal("invalid frame reader did not exit")
		}
		local.Close()
		remote.Close()
		status := cptDynamicStatus(peer)
		if status.InboundQueuedMessages != 0 || status.InboundQueuedBytes != 0 || len(peer.inbox) != 0 {
			t.Fatalf("invalid length reserved or dispatched: %+v", status)
		}
	}
}

type flowCancelAfterPayloadConn struct {
	net.Conn
	cancel context.CancelFunc
}

func (connection *flowCancelAfterPayloadConn) Read(data []byte) (int, error) {
	count, err := connection.Conn.Read(data)
	if len(data) == 1 && count == 1 {
		connection.cancel()
	}
	return count, err
}

func TestDynamicRaftInboundCancellationBeforeStagingReturnsReservation(t *testing.T) {
	peer := newDynamicPeerState(nil, dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	connection := &flowCancelAfterPayloadConn{Conn: local, cancel: cancel}
	peer.connection = connection
	result := make(chan error, 1)
	go func() { result <- peer.receiveRaftFrame(ctx, connection, peer.identityRevision) }()
	remote.SetWriteDeadline(time.Now().Add(cptWaitTimeout))
	if err := writeFull(remote, append(cptFramePrefix(1), 0x09)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation after payload read: %v", err)
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("cancelled frame reader did not exit")
	}
	status := cptDynamicStatus(peer)
	if status.InboundQueuedMessages != 0 || status.InboundQueuedBytes != 0 || len(peer.inbox) != 0 {
		t.Fatalf("cancelled frame leaked or dispatched: %+v", status)
	}
}

type flowCapacityWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *flowCapacityWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestDynamicRaftResetUnblocksActiveCapacityWait(t *testing.T) {
	transport, peer := cptMinimalDynamic(
		dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11),
		dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22),
	)
	for range peerQueueMessages {
		if err := peer.enqueueInbound([]byte{1}, peer.identityRevision); err != nil {
			t.Fatal(err)
		}
	}
	parent, cancel := context.WithCancel(context.Background())
	ctx := &flowCapacityWaitContext{Context: parent, waiting: make(chan struct{})}
	local, remote := net.Pipe()
	peer.connection = local
	result := make(chan error, 1)
	done := cptRun(func() { result <- peer.receiveRaftFrame(ctx, local, peer.identityRevision) })
	t.Cleanup(func() { cancel(); local.Close(); remote.Close(); cptWaitDone(t, done, "reader cleanup"); peer.stop() })
	remote.SetWriteDeadline(time.Now().Add(cptWaitTimeout))
	if _, err := remote.Write(cptFramePrefix(1)); err != nil {
		t.Fatal(err)
	}
	// Done is evaluated only after reserveInbound checks the full queue and
	// captures its wake channel. Reset cannot win before that capacity check.
	select {
	case <-ctx.waiting:
	case <-time.After(time.Second):
		t.Fatal("reader never entered its capacity wait")
	}
	if err := transport.ResetPeer(2); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrPeerTransportClosed) {
			t.Fatalf("reset capacity wait: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("reset socket left the capacity waiter alive")
	}
	status := cptDynamicStatus(peer)
	if status.Connected || status.InboundQueuedMessages != peerQueueMessages || status.InboundQueuedBytes != 5*peerQueueMessages {
		t.Fatalf("reset leaked or dispatched a waiting frame: %+v", status)
	}
}
