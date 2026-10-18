package tcp

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// snapBranchManualContext is canceled exactly when the test says so and reports the chosen error,
// so deadline and cancellation handling are exercised without waiting for real time.
type snapBranchManualContext struct {
	context.Context
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func newSnapBranchManualContext() *snapBranchManualContext {
	return &snapBranchManualContext{Context: context.Background(), done: make(chan struct{})}
}

func (ctx *snapBranchManualContext) Done() <-chan struct{} { return ctx.done }

func (ctx *snapBranchManualContext) Err() error {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.err
}

func (ctx *snapBranchManualContext) finish(err error) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.err == nil {
		ctx.err = err
		close(ctx.done)
	}
}

func snapBranchLiteralTransport(local PeerIdentityDTO, peers ...PeerIdentityDTO) *DynamicTCPPeerTransport {
	transport := &DynamicTCPPeerTransport{
		local: local, started: true, ctx: context.Background(),
		peers:                 make(map[uint64]*dynamicPeerState, len(peers)),
		snapshotOutboundSlots: make(chan struct{}, snapshotStreamOutboundSlots),
	}
	for _, identity := range peers {
		transport.peers[identity.MemberID] = newDynamicLanePeerState(nil, identity, dynamicLaneRaft)
	}
	return transport
}

func snapBranchIdentities() (voter, peer PeerIdentityDTO) {
	voter = dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", PeerPhaseVoter, "", 0x11)
	peer = dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", 0x22)
	return voter, peer
}

func snapBranchRequest() SnapshotStreamDTO {
	return SnapshotStreamDTO{To: 2, Index: 1, Term: 1, Data: []byte("snapshot")}
}

func TestSnapshotStreamErrorReportsReasonAndCause(t *testing.T) {
	cause := errors.New("receiver socket reset")
	withCause := snapshotFailure(SnapshotReasonConnection, cause)
	if !errors.Is(withCause, cause) {
		t.Fatal("SnapshotStreamError does not unwrap to its cause")
	}
	if text := withCause.Error(); !strings.Contains(text, SnapshotReasonConnection) || !strings.Contains(text, cause.Error()) {
		t.Fatalf("error text %q does not carry the reason and the cause", text)
	}
	withoutCause := snapshotFailure(SnapshotReasonBusy, nil)
	if withoutCause.Unwrap() != nil {
		t.Fatal("a failure without a cause unwraps to something")
	}
	if text := withoutCause.Error(); !strings.Contains(text, SnapshotReasonBusy) {
		t.Fatalf("error text %q does not carry the reason", text)
	}
}

func TestSnapshotStreamFailureReasonDefaultsToConnection(t *testing.T) {
	if got := SnapshotStreamFailureReason(errors.New("unclassified")); got != SnapshotReasonConnection {
		t.Fatalf("unclassified error reason = %q, want %q", got, SnapshotReasonConnection)
	}
	if got := SnapshotStreamFailureReason(nil); got != SnapshotReasonConnection {
		t.Fatalf("nil error reason = %q, want %q", got, SnapshotReasonConnection)
	}
	wrapped := fmt.Errorf("raft adapter: %w", snapshotFailure(SnapshotReasonDigest, nil))
	if got := SnapshotStreamFailureReason(wrapped); got != SnapshotReasonDigest {
		t.Fatalf("wrapped failure reason = %q, want %q", got, SnapshotReasonDigest)
	}
}

func TestSnapshotStreamOnlyCommittedPhasesMayTransfer(t *testing.T) {
	for _, phase := range []PeerPhaseDTO{PeerPhaseStaged, PeerPhaseRetired, PeerPhaseDTO(0)} {
		identity := dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", phase, "", 0x11)
		if snapshotStreamMemberEligible(identity) {
			t.Errorf("phase %d may transfer snapshots", phase)
		}
	}
	for _, phase := range []PeerPhaseDTO{PeerPhaseLearner, PeerPhaseCatchingUp, PeerPhaseVoter, PeerPhaseLeaving} {
		identity := dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", phase, testOperationID, 0x11)
		if !snapshotStreamMemberEligible(identity) {
			t.Errorf("committed phase %d may not transfer snapshots", phase)
		}
	}
}

func TestSnapshotStreamSendRefusesUnusableTransports(t *testing.T) {
	voter, peer := snapBranchIdentities()
	var nilContext context.Context
	t.Run("missing context", func(t *testing.T) {
		err := snapBranchLiteralTransport(voter, peer).SendSnapshot(nilContext, snapBranchRequest())
		if got := SnapshotStreamFailureReason(err); got != SnapshotReasonCanceled {
			t.Fatalf("reason = %q (%v), want canceled", got, err)
		}
	})
	t.Run("closed transport", func(t *testing.T) {
		transport := snapBranchLiteralTransport(voter, peer)
		transport.closed = true
		err := transport.SendSnapshot(context.Background(), snapBranchRequest())
		if got := SnapshotStreamFailureReason(err); got != SnapshotReasonCanceled || !errors.Is(err, ErrPeerTransportClosed) {
			t.Fatalf("reason = %q (%v), want canceled with ErrPeerTransportClosed", got, err)
		}
	})
	t.Run("transport that is not serving", func(t *testing.T) {
		transport := snapBranchLiteralTransport(voter, peer)
		transport.started = false
		err := transport.SendSnapshot(context.Background(), snapBranchRequest())
		if got := SnapshotStreamFailureReason(err); got != SnapshotReasonCanceled || errors.Is(err, ErrPeerTransportClosed) {
			t.Fatalf("reason = %q (%v), want canceled and distinct from a closed transport", got, err)
		}
		transport = snapBranchLiteralTransport(voter, peer)
		transport.ctx = nil
		if got := SnapshotStreamFailureReason(transport.SendSnapshot(context.Background(), snapBranchRequest())); got != SnapshotReasonCanceled {
			t.Fatalf("reason without a run context = %q, want canceled", got)
		}
	})
}

func TestSnapshotStreamSendReportsCallerDeadlineAndCancellation(t *testing.T) {
	_, peer := snapBranchIdentities()
	staged := dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", PeerPhaseStaged, testOperationID, 0x11)
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, test := range map[string]struct {
		ctx    context.Context
		reason string
	}{
		"expired deadline": {expired, SnapshotReasonTimeout},
		"canceled caller":  {canceled, SnapshotReasonCanceled},
	} {
		err := snapBranchLiteralTransport(staged, peer).SendSnapshot(test.ctx, snapBranchRequest())
		if got := SnapshotStreamFailureReason(err); got != test.reason {
			t.Errorf("%s: reason = %q (%v), want %q", name, got, err, test.reason)
		}
	}
}

func TestSnapshotStreamSendGivesUpOnPeersThatAreNotCommitted(t *testing.T) {
	voter, peer := snapBranchIdentities()
	staged := dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", PeerPhaseStaged, testOperationID, 0x11)
	retired := dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseRetired, "", 0x22)
	for name, build := range map[string]func() *DynamicTCPPeerTransport{
		"local member is staged": func() *DynamicTCPPeerTransport { return snapBranchLiteralTransport(staged, peer) },
		"peer is retired":        func() *DynamicTCPPeerTransport { return snapBranchLiteralTransport(voter, retired) },
		"peer was stopped": func() *DynamicTCPPeerTransport {
			transport := snapBranchLiteralTransport(voter, peer)
			state := transport.peers[2]
			state.mu.Lock()
			state.stopped = true
			state.mu.Unlock()
			return transport
		},
	} {
		err := build().SendSnapshot(context.Background(), snapBranchRequest())
		if got := SnapshotStreamFailureReason(err); got != SnapshotReasonConnection {
			t.Errorf("%s: reason = %q (%v), want connection (nothing was dialed)", name, got, err)
		}
	}
}

func TestSnapshotStreamRetainedPartialLifecycle(t *testing.T) {
	manifest := snapshotManifest{index: 5, term: 2, total: 10}
	t.Run("empty transfers are not retained", func(t *testing.T) {
		transport := &DynamicTCPPeerTransport{}
		transport.retainSnapshotPartial(&snapshotPartial{from: 1, manifest: manifest})
		if transport.snapshotPartial != nil {
			t.Fatal("a partial without bytes was retained")
		}
	})
	t.Run("closed transport keeps nothing", func(t *testing.T) {
		transport := &DynamicTCPPeerTransport{closed: true}
		transport.retainSnapshotPartial(&snapshotPartial{from: 1, manifest: manifest, data: []byte{1, 2, 3}})
		if transport.snapshotPartial != nil {
			t.Fatal("a closed transport retained snapshot bytes")
		}
	})
	t.Run("retained bytes expire", func(t *testing.T) {
		transport := &DynamicTCPPeerTransport{}
		partial := &snapshotPartial{from: 1, manifest: manifest, data: []byte{1, 2, 3}}
		transport.retainSnapshotPartial(partial)
		transport.snapshotMu.Lock()
		retained, timer := transport.snapshotPartial, partial.timer
		transport.snapshotMu.Unlock()
		if retained != partial || timer == nil {
			t.Fatalf("retained = %p with timer %v, want the partial and a running TTL timer", retained, timer)
		}
		timer.Reset(0) // the TTL elapses
		cptWaitUntil(t, "retained bytes to expire", func() bool {
			transport.snapshotMu.Lock()
			defer transport.snapshotMu.Unlock()
			return transport.snapshotPartial == nil
		})
	})
	t.Run("same transfer from the same sender resumes", func(t *testing.T) {
		transport := &DynamicTCPPeerTransport{}
		partial := &snapshotPartial{from: 1, manifest: manifest, data: []byte{1, 2, 3}}
		transport.snapshotPartial = partial
		got := transport.takeSnapshotPartial(1, manifest)
		if got != partial || len(got.data) != 3 {
			t.Fatalf("take = %+v, want the retained bytes", got)
		}
		if transport.snapshotPartial != nil {
			t.Fatal("taking a partial left it retained")
		}
	})
	t.Run("taking a partial stops its expiry timer", func(t *testing.T) {
		transport := &DynamicTCPPeerTransport{}
		transport.retainSnapshotPartial(&snapshotPartial{from: 1, manifest: manifest, data: []byte{1}})
		got := transport.takeSnapshotPartial(1, manifest)
		if len(got.data) != 1 || got.timer != nil {
			t.Fatalf("take = %+v, want the retained bytes with the timer cleared", got)
		}
	})
	t.Run("another sender or snapshot starts from zero", func(t *testing.T) {
		newer := manifest
		newer.index++
		for name, take := range map[string]func(*DynamicTCPPeerTransport) *snapshotPartial{
			"another sender": func(transport *DynamicTCPPeerTransport) *snapshotPartial {
				return transport.takeSnapshotPartial(7, manifest)
			},
			"another snapshot": func(transport *DynamicTCPPeerTransport) *snapshotPartial {
				return transport.takeSnapshotPartial(1, newer)
			},
		} {
			transport := &DynamicTCPPeerTransport{}
			transport.snapshotPartial = &snapshotPartial{from: 1, manifest: manifest, data: []byte{1, 2, 3}}
			got := take(transport)
			if len(got.data) != 0 {
				t.Errorf("%s resumed %d retained bytes", name, len(got.data))
			}
			if transport.snapshotPartial != nil {
				t.Errorf("%s left the superseded bytes retained", name)
			}
		}
	})
}

func TestReadSnapshotFrameRejectsBrokenFraming(t *testing.T) {
	limitPlusOne := make([]byte, 4)
	binary.BigEndian.PutUint32(limitPlusOne, uint32(snapshotStreamFrameLimit)+1)
	for name, test := range map[string]struct {
		wire    []byte
		want    []byte
		wantErr error
	}{
		"complete frame":        {wire: []byte{0, 0, 0, 1, snapshotFrameComplete}, want: []byte{snapshotFrameComplete}},
		"zero length":           {wire: []byte{0, 0, 0, 0}, wantErr: ErrInvalidPeerMessage},
		"above the frame limit": {wire: limitPlusOne, wantErr: ErrInvalidPeerMessage},
		"closed before prefix":  {wire: nil, wantErr: io.EOF},
		"truncated prefix":      {wire: []byte{0, 0}, wantErr: io.ErrUnexpectedEOF},
		"truncated payload":     {wire: []byte{0, 0, 0, 9, snapshotFrameComplete}, wantErr: io.ErrUnexpectedEOF},
	} {
		reader, writer := net.Pipe()
		go func() {
			_, _ = writer.Write(test.wire)
			_ = writer.Close()
		}()
		got, err := readSnapshotFrame(reader, time.Second)
		_ = reader.Close()
		if test.wantErr != nil {
			if !errors.Is(err, test.wantErr) || got != nil {
				t.Errorf("%s: frame = %x, %v; want %v", name, got, err, test.wantErr)
			}
			continue
		}
		if err != nil || string(got) != string(test.want) {
			t.Errorf("%s: frame = %x, %v; want %x", name, got, err, test.want)
		}
	}
}

func TestSnapshotStreamSenderStopsRetryingWhenCallerEnds(t *testing.T) {
	for name, test := range map[string]struct {
		end    error
		reason string
	}{
		"caller cancels":          {context.Canceled, SnapshotReasonCanceled},
		"caller deadline elapses": {context.DeadlineExceeded, SnapshotReasonTimeout},
	} {
		t.Run(name, func(t *testing.T) {
			pair := newSnapshotStreamPair(t, false, nil)
			server := newFakeSnapshotReceiver(t, pair)
			server.script = func(connection net.Conn, number int) {} // drops every connection after the hello
			ctx := newSnapBranchManualContext()
			data := randomSnapshot(t, 1000)
			result := make(chan error, 1)
			go func() { result <- pair.sender.SendSnapshot(ctx, pair.request(data)) }()
			cptWaitUntil(t, "the first attempt to reach the receiver", func() bool { return server.connectionCount() >= 1 })
			ctx.finish(test.end)
			select {
			case err := <-result:
				if got := SnapshotStreamFailureReason(err); got != test.reason || !errors.Is(err, test.end) {
					t.Fatalf("SendSnapshot = %v (reason %q), want %q wrapping %v", err, got, test.reason, test.end)
				}
			case <-time.After(cptWaitTimeout):
				t.Fatal("SendSnapshot kept retrying after the caller ended")
			}
		})
	}
}

func TestSnapshotStreamSenderReportsUnreachableReceiver(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	if err := pair.receiverListener.Close(); err != nil {
		t.Fatal(err)
	}
	err := pair.sender.SendSnapshot(context.Background(), pair.request(randomSnapshot(t, 1000)))
	var dialErr *net.OpError
	if got := SnapshotStreamFailureReason(err); got != SnapshotReasonConnection || !errors.As(err, &dialErr) {
		t.Fatalf("SendSnapshot = %v (reason %q), want a connection failure caused by the dial", err, got)
	}
	pair.sender.mu.Lock()
	tracked := len(pair.sender.connections)
	pair.sender.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("failed dials left %d tracked connections", tracked)
	}
}

func TestSnapshotStreamSenderSendsNothingWhenItsOwnIdentityIsInvalid(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	server := newFakeSnapshotReceiver(t, pair)
	pair.sender.mu.Lock()
	pair.sender.local.ClusterID = ""
	pair.sender.mu.Unlock()
	err := pair.sender.SendSnapshot(context.Background(), pair.request(randomSnapshot(t, 1000)))
	if got := SnapshotStreamFailureReason(err); got != SnapshotReasonConnection {
		t.Fatalf("SendSnapshot reason = %q (%v), want connection", got, err)
	}
	if count := server.connectionCount(); count != 0 {
		t.Fatalf("receiver saw %d snapshot hellos from a sender with an invalid identity", count)
	}
}

// snapBranchRawReceiver answers snapshot hellos by hand; the script owns every byte after the
// sender's hello, including the receiver's own hello.
type snapBranchRawReceiver struct {
	count atomic.Int32
	wg    sync.WaitGroup
}

func newSnapBranchRawReceiver(t *testing.T, pair *snapshotStreamPair, script func(connection net.Conn, number int)) *snapBranchRawReceiver {
	t.Helper()
	server := &snapBranchRawReceiver{}
	listener := tls.NewListener(pair.receiverListener, pair.receiverNode.serverTLS)
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			server.wg.Add(1)
			go func() {
				defer server.wg.Done()
				defer connection.Close()
				payload, err := readDynamicFrame(connection, dynamicHelloLimit)
				if err != nil || len(payload) < 8 || string(payload[:8]) != dynamicSnapshotMagic {
					return // the sender's Raft-lane dialers also reach this listener
				}
				script(connection, int(server.count.Add(1)))
			}()
		}
	}()
	t.Cleanup(func() {
		_ = pair.receiverListener.Close()
		server.wg.Wait()
	})
	return server
}

func snapBranchHelloFrame(t *testing.T, identity PeerIdentityDTO) []byte {
	t.Helper()
	hello, err := encodeDynamicPeerHello(identity, dynamicSnapshotMagic)
	if err != nil {
		t.Fatal(err)
	}
	return hello
}

func TestSnapshotStreamSenderRejectsBadReceiverHello(t *testing.T) {
	for name, test := range map[string]struct {
		hello    func(t *testing.T, identity PeerIdentityDTO) []byte
		mutate   func(pair *snapshotStreamPair)
		attempts int
		reason   string
		closed   bool
	}{
		"hello that is not an identity": {
			hello:    func(*testing.T, PeerIdentityDTO) []byte { return []byte(dynamicSnapshotMagic + "junk") },
			attempts: snapshotStreamAttempts, reason: SnapshotReasonConnection,
		},
		"receiver of another cluster": {
			hello: func(t *testing.T, identity PeerIdentityDTO) []byte {
				identity.ClusterID = "another-cluster"
				return snapBranchHelloFrame(t, identity)
			},
			attempts: snapshotStreamAttempts, reason: SnapshotReasonConnection,
		},
		"receiver at another address": {
			hello: func(t *testing.T, identity PeerIdentityDTO) []byte {
				identity.Address = "127.0.0.1:1"
				return snapBranchHelloFrame(t, identity)
			},
			attempts: snapshotStreamAttempts, reason: SnapshotReasonConnection,
		},
		"peer stopped while connecting": {
			mutate: func(pair *snapshotStreamPair) {
				pair.sender.mu.Lock()
				peer := pair.sender.peers[2]
				pair.sender.mu.Unlock()
				peer.mu.Lock()
				peer.stopped = true
				peer.mu.Unlock()
			},
			attempts: 1, reason: SnapshotReasonCanceled, closed: true,
		},
		"peer left the committed set while connecting": {
			mutate: func(pair *snapshotStreamPair) {
				pair.sender.mu.Lock()
				delete(pair.sender.peers, 2)
				pair.sender.mu.Unlock()
			},
			attempts: 1, reason: SnapshotReasonCanceled, closed: true,
		},
		"peer identity revision advanced while connecting": {
			mutate: func(pair *snapshotStreamPair) {
				pair.sender.mu.Lock()
				peer := pair.sender.peers[2]
				pair.sender.mu.Unlock()
				peer.mu.Lock()
				peer.identityRevision++
				peer.mu.Unlock()
			},
			attempts: snapshotStreamAttempts, reason: SnapshotReasonConnection,
		},
		"sender no longer committed while connecting": {
			mutate: func(pair *snapshotStreamPair) {
				pair.sender.mu.Lock()
				pair.sender.local.Phase = PeerPhaseStaged
				pair.sender.mu.Unlock()
			},
			attempts: 1, reason: SnapshotReasonConnection,
		},
	} {
		t.Run(name, func(t *testing.T) {
			pair := newSnapshotStreamPair(t, false, nil)
			manifestSeen := make(chan error, snapshotStreamAttempts)
			hello := snapBranchHelloFrame(t, pair.receiverIdentity)
			if test.hello != nil {
				hello = test.hello(t, pair.receiverIdentity)
			}
			data := randomSnapshot(t, 1000)
			newSnapBranchRawReceiver(t, pair, func(connection net.Conn, number int) {
				if test.mutate != nil {
					test.mutate(pair)
				}
				_ = writeDynamicFrame(connection, hello, dynamicHelloLimit)
				_, err := readDynamicFrame(connection, snapshotStreamFrameLimit)
				manifestSeen <- err
			})
			err := pair.sender.SendSnapshot(context.Background(), pair.request(data))
			if got := SnapshotStreamFailureReason(err); got != test.reason {
				t.Fatalf("SendSnapshot reason = %q (%v), want %q", got, err, test.reason)
			}
			if test.closed && !errors.Is(err, ErrPeerTransportClosed) {
				t.Fatalf("SendSnapshot = %v, want ErrPeerTransportClosed for a peer that is gone", err)
			}
			for range test.attempts {
				if manifestErr := obsAwait(t, manifestSeen, "the receiver to finish an attempt"); manifestErr == nil {
					t.Fatal("the sender sent a manifest although the receiver failed the identity exchange")
				}
			}
		})
	}
}

// snapBranchAcceptThroughDone plays a receiver that accepts from offset 0, acknowledges every chunk
// and stops after reading DONE. It reports whether DONE arrived.
func snapBranchAcceptThroughDone(connection net.Conn) bool {
	if _, err := readDynamicFrame(connection, snapshotStreamFrameLimit); err != nil {
		return false
	}
	if writeDynamicFrame(connection, encodeSnapshotOffsetFrame(snapshotFrameAccept, 0), snapshotStreamFrameLimit) != nil {
		return false
	}
	received := 0
	for {
		frame, err := readDynamicFrame(connection, snapshotStreamFrameLimit)
		if err != nil {
			return false
		}
		switch frame[0] {
		case snapshotFrameChunk:
			_, _, chunk, ok := decodeSnapshotChunk(frame)
			if !ok {
				return false
			}
			received += len(chunk)
			if writeDynamicFrame(connection, encodeSnapshotOffsetFrame(snapshotFrameAck, uint64(received)), snapshotStreamFrameLimit) != nil {
				return false
			}
		case snapshotFrameDone:
			return true
		default:
			return false
		}
	}
}

func TestSnapshotStreamSenderMapsMalformedReceiverFramesWithoutRetry(t *testing.T) {
	afterManifest := func(frame []byte) func(net.Conn) {
		return func(connection net.Conn) {
			_, _ = readDynamicFrame(connection, snapshotStreamFrameLimit)
			_ = writeDynamicFrame(connection, frame, snapshotStreamFrameLimit)
		}
	}
	afterChunk := func(frame []byte) func(net.Conn) {
		return func(connection net.Conn) {
			_, _ = readDynamicFrame(connection, snapshotStreamFrameLimit)
			_ = writeDynamicFrame(connection, encodeSnapshotOffsetFrame(snapshotFrameAccept, 0), snapshotStreamFrameLimit)
			_, _ = readDynamicFrame(connection, snapshotStreamFrameLimit)
			_ = writeDynamicFrame(connection, frame, snapshotStreamFrameLimit)
		}
	}
	afterDone := func(frame []byte) func(net.Conn) {
		return func(connection net.Conn) {
			if snapBranchAcceptThroughDone(connection) {
				_ = writeDynamicFrame(connection, frame, snapshotStreamFrameLimit)
			}
		}
	}
	for _, test := range []struct {
		name   string
		script func(net.Conn)
		reason string
	}{
		{"ACCEPT of the wrong kind", afterManifest([]byte{0x7E}), SnapshotReasonOutOfOrder},
		{"ACCEPT beyond the snapshot", afterManifest(encodeSnapshotOffsetFrame(snapshotFrameAccept, 1001)), SnapshotReasonOutOfOrder},
		{"ERROR frame with a stray byte", afterManifest([]byte{snapshotFrameError, snapshotErrorBusy, 0}), SnapshotReasonOutOfOrder},
		{"ERROR frame without a code", afterManifest([]byte{snapshotFrameError}), SnapshotReasonOutOfOrder},
		{"ERROR while acknowledging a chunk", afterChunk([]byte{snapshotFrameError, snapshotErrorChecksum}), SnapshotReasonChecksum},
		{"ERROR instead of COMPLETE", afterDone([]byte{snapshotFrameError, snapshotErrorRejected}), SnapshotReasonRejected},
		{"unknown completion frame", afterDone([]byte{0x55}), SnapshotReasonOutOfOrder},
		{"COMPLETE with a stray byte", afterDone([]byte{snapshotFrameComplete, 0}), SnapshotReasonOutOfOrder},
	} {
		t.Run(test.name, func(t *testing.T) {
			pair := newSnapshotStreamPair(t, false, nil)
			server := newFakeSnapshotReceiver(t, pair)
			server.script = func(connection net.Conn, number int) { test.script(connection) }
			err := pair.sender.SendSnapshot(context.Background(), pair.request(randomSnapshot(t, 1000)))
			if got := SnapshotStreamFailureReason(err); got != test.reason {
				t.Fatalf("SendSnapshot reason = %q (%v), want %q", got, err, test.reason)
			}
			if count := server.connectionCount(); count != 1 {
				t.Fatalf("connections = %d, want 1: protocol violations are not retried", count)
			}
		})
	}
}

func TestSnapshotStreamSenderRetriesWhenCompletionNeverArrives(t *testing.T) {
	data := randomSnapshot(t, 1000)
	oversized := make([]byte, 4)
	binary.BigEndian.PutUint32(oversized, uint32(snapshotStreamFrameLimit)+1)
	for name, fault := range map[string]func(net.Conn){
		"connection closes before COMPLETE": func(net.Conn) {},
		"COMPLETE frame has zero length":    func(connection net.Conn) { _, _ = connection.Write([]byte{0, 0, 0, 0}) },
		"COMPLETE frame is truncated":       func(connection net.Conn) { _, _ = connection.Write([]byte{0, 0, 0, 9, snapshotFrameComplete}) },
		"COMPLETE frame above the limit":    func(connection net.Conn) { _, _ = connection.Write(oversized) },
	} {
		t.Run(name, func(t *testing.T) {
			pair := newSnapshotStreamPair(t, false, nil)
			server := newFakeSnapshotReceiver(t, pair)
			server.script = func(connection net.Conn, number int) {
				if number == 1 {
					if snapBranchAcceptThroughDone(connection) {
						fault(connection)
					}
					return
				}
				server.acceptTransfer(connection, len(data), -1) // the receiver kept every byte
			}
			if err := pair.sender.SendSnapshot(context.Background(), pair.request(data)); err != nil {
				t.Fatalf("SendSnapshot: %v", err)
			}
			if count := server.connectionCount(); count != 2 {
				t.Fatalf("connections = %d, want 2 (unreadable completion, then a resumed finish)", count)
			}
			if got := server.resumeOffsets(); len(got) != 1 || got[0] != len(data) {
				t.Fatalf("resume offsets = %v, want [%d]", got, len(data))
			}
			if frames := server.chunkFrames(); len(frames) != 0 {
				t.Fatalf("sender resent %d chunks that the receiver already had", len(frames))
			}
		})
	}
}

func snapBranchRawSenderDial(t *testing.T, pair *snapshotStreamPair) *tls.Conn {
	t.Helper()
	connection, err := tls.Dial("tcp", pair.receiverIdentity.Address, pair.senderNode.dialConfig(pair.receiverIdentity.DNSSAN))
	if err != nil {
		t.Fatalf("dial receiver: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func TestSnapshotStreamReceiverClosesSilentlyOnBadHello(t *testing.T) {
	t.Run("hello that is not an identity", func(t *testing.T) {
		pair := newSnapshotStreamPair(t, true, nil)
		connection := snapBranchRawSenderDial(t, pair)
		if err := writeDynamicFrame(connection, []byte(dynamicSnapshotMagic+"junk"), dynamicHelloLimit); err != nil {
			t.Fatal(err)
		}
		if _, err := readDynamicFrame(connection, dynamicHelloLimit); err == nil {
			t.Fatal("receiver answered a malformed hello")
		}
		pair.expectNoReceipt()
	})
	t.Run("receiver cannot encode its own hello", func(t *testing.T) {
		pair := newSnapshotStreamPair(t, true, nil)
		pair.receiver.mu.Lock()
		pair.receiver.local.ClusterID = ""
		pair.receiver.mu.Unlock()
		connection := snapBranchRawSenderDial(t, pair)
		if err := writeDynamicFrame(connection, snapBranchHelloFrame(t, pair.senderIdentity), dynamicHelloLimit); err != nil {
			t.Fatal(err)
		}
		if _, err := readDynamicFrame(connection, dynamicHelloLimit); err == nil {
			t.Fatal("receiver answered although its own identity is invalid")
		}
		pair.expectNoReceipt()
	})
}

func TestSnapshotStreamReceiverRechecksSenderBeforeHandingSnapshotToRaft(t *testing.T) {
	data := randomSnapshot(t, 1000)
	pair := newSnapshotStreamPair(t, true, nil)
	connection := pair.dialAsSender()
	pair.acceptManifest(connection, pair.manifest(data))
	snapshotWrite(t, connection, encodeSnapshotChunk(nil, 0, data))
	if next, ok := decodeSnapshotOffsetFrame(snapshotRead(t, connection), snapshotFrameAck); !ok || next != uint64(len(data)) {
		t.Fatalf("chunk ACK = %d, %v", next, ok)
	}
	// The sender leaves the committed set after every byte arrived but before DONE.
	if err := pair.receiver.ReconcilePeers(nil); err != nil {
		t.Fatal(err)
	}
	snapshotWrite(t, connection, []byte{snapshotFrameDone})
	if payload, err := readDynamicFrame(connection, snapshotStreamFrameLimit); err == nil {
		t.Fatalf("receiver answered %x to a sender that is no longer committed", payload)
	}
	pair.expectNoReceipt()
}

func TestSnapshotStreamReceiverDropsPartialTransferOnShutdown(t *testing.T) {
	data := randomSnapshot(t, 2*snapshotStreamChunkSize)
	pair := newSnapshotStreamPair(t, true, nil)
	connection := pair.dialAsSender()
	pair.acceptManifest(connection, pair.manifest(data))
	snapshotWrite(t, connection, encodeSnapshotChunk(nil, 0, data[:snapshotStreamChunkSize]))
	snapshotRead(t, connection)
	if err := pair.receiver.Close(); err != nil {
		t.Fatalf("close receiver: %v", err)
	}
	pair.receiver.snapshotMu.Lock()
	retained := pair.receiver.snapshotPartial
	pair.receiver.snapshotMu.Unlock()
	if retained != nil {
		t.Fatalf("a stopped receiver retained %d bytes of an unfinished transfer", len(retained.data))
	}
	pair.expectNoReceipt()
}
