package tcp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// snapshotStreamPair is a sender (member 1) and a receiver address (member 2). The receiver is a
// real transport unless the test plays it by hand with a fake listener.
type snapshotStreamPair struct {
	t                *testing.T
	sender           *DynamicTCPPeerTransport
	receiver         *DynamicTCPPeerTransport
	senderNode       *cptNode
	receiverNode     *cptNode
	senderIdentity   PeerIdentityDTO
	receiverIdentity PeerIdentityDTO
	receiverListener net.Listener
	received         chan SnapshotStreamReceiptDTO
}

func newSnapshotStreamPair(t *testing.T, realReceiver bool, handler SnapshotStreamHandler) *snapshotStreamPair {
	t.Helper()
	pair := &snapshotStreamPair{
		t: t, senderNode: cptNewNode(t, "node1.example"), receiverNode: cptNewNode(t, "node2.example"),
		received: make(chan SnapshotStreamReceiptDTO, 4),
	}
	cptTrustEachOther(pair.senderNode, pair.receiverNode)
	senderListener, receiverListener := cptListen(t), cptListen(t)
	pair.receiverListener = receiverListener
	pair.senderIdentity = pair.senderNode.identity(1, senderListener.Addr().String(), PeerPhaseVoter, "")
	pair.receiverIdentity = pair.receiverNode.identity(2, receiverListener.Addr().String(), PeerPhaseVoter, "")
	var err error
	pair.sender, err = NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: pair.senderIdentity, Listener: senderListener,
		ServerTLS: pair.senderNode.serverTLS, ClientTLS: pair.senderNode.clientTLS,
		Peers: []PeerIdentityDTO{pair.receiverIdentity}, AdmissionHandler: cptRejectAdmission,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pair.sender.Close() })
	cptServe(t, pair.sender.Serve)
	if realReceiver {
		if handler == nil {
			handler = func(_ context.Context, receipt SnapshotStreamReceiptDTO) error {
				pair.received <- receipt
				return nil
			}
		}
		pair.receiver, err = NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
			LocalIdentity: pair.receiverIdentity, Listener: receiverListener,
			ServerTLS: pair.receiverNode.serverTLS, ClientTLS: pair.receiverNode.clientTLS,
			Peers: []PeerIdentityDTO{pair.senderIdentity}, AdmissionHandler: cptRejectAdmission,
			SnapshotStreamHandler: handler,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pair.receiver.Close() })
		cptServe(t, pair.receiver.Serve)
	}
	for _, transport := range []*DynamicTCPPeerTransport{pair.sender, pair.receiver} {
		if transport == nil {
			continue
		}
		cptWaitUntil(t, "transport start", func() bool {
			transport.mu.Lock()
			defer transport.mu.Unlock()
			return transport.started
		})
	}
	return pair
}

func randomSnapshot(t *testing.T, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return data
}

func (pair *snapshotStreamPair) request(data []byte) SnapshotStreamDTO {
	return SnapshotStreamDTO{To: 2, Index: 40, Term: 3, Data: data}
}

func (pair *snapshotStreamPair) manifest(data []byte) snapshotManifest {
	return snapshotManifest{index: 40, term: 3, total: uint64(len(data)), digest: sha256.Sum256(data)}
}

// dialAsSender opens the stream to the real receiver and completes the hello exchange, like the sender.
func (pair *snapshotStreamPair) dialAsSender() *tls.Conn {
	pair.t.Helper()
	connection, err := tls.Dial("tcp", pair.receiverIdentity.Address, pair.senderNode.dialConfig(pair.receiverIdentity.DNSSAN))
	if err != nil {
		pair.t.Fatalf("dial receiver: %v", err)
	}
	pair.t.Cleanup(func() { _ = connection.Close() })
	hello, err := encodeDynamicPeerHello(pair.senderIdentity, dynamicSnapshotMagic)
	if err != nil {
		pair.t.Fatal(err)
	}
	if err := writeDynamicFrame(connection, hello, dynamicHelloLimit); err != nil {
		pair.t.Fatal(err)
	}
	reply, err := readDynamicFrame(connection, dynamicHelloLimit)
	if err != nil {
		pair.t.Fatalf("receiver hello: %v", err)
	}
	if _, err := decodeDynamicPeerHello(reply, dynamicSnapshotMagic); err != nil {
		pair.t.Fatal(err)
	}
	return connection
}

func snapshotWrite(t *testing.T, connection net.Conn, payload []byte) {
	t.Helper()
	if err := writeDynamicFrame(connection, payload, snapshotStreamFrameLimit); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func snapshotRead(t *testing.T, connection net.Conn) []byte {
	t.Helper()
	payload, err := readDynamicFrame(connection, snapshotStreamFrameLimit)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return payload
}

// expectSnapshotError reads one frame and requires an ERROR with the given code, then a closed connection.
func expectSnapshotError(t *testing.T, connection net.Conn, code byte) {
	t.Helper()
	payload := snapshotRead(t, connection)
	if !bytes.Equal(payload, []byte{snapshotFrameError, code}) {
		t.Fatalf("frame = %x, want ERROR code %d", payload, code)
	}
	if _, err := readDynamicFrame(connection, snapshotStreamFrameLimit); err == nil {
		t.Fatal("receiver kept the connection open after an ERROR")
	}
}

func (pair *snapshotStreamPair) acceptManifest(connection net.Conn, manifest snapshotManifest) uint64 {
	pair.t.Helper()
	snapshotWrite(pair.t, connection, encodeSnapshotManifest(manifest))
	offset, ok := decodeSnapshotOffsetFrame(snapshotRead(pair.t, connection), snapshotFrameAccept)
	if !ok {
		pair.t.Fatal("receiver did not ACCEPT the manifest")
	}
	return offset
}

func (pair *snapshotStreamPair) expectNoReceipt() {
	pair.t.Helper()
	select {
	case receipt := <-pair.received:
		pair.t.Fatalf("receiver handed a rejected snapshot to Raft: %d bytes", len(receipt.Data))
	case <-time.After(100 * time.Millisecond):
	}
}

// Traceability: CLU-14; docs/contracts/raft-snapshot-stream-v1.md; docs/specs/cluster-session-and-delivery.md §11.37.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamDeliversSnapshotOverRealTransports(t *testing.T) {
	pair := newSnapshotStreamPair(t, true, nil)
	data := randomSnapshot(t, 3<<20+12345)
	if err := pair.sender.SendSnapshot(context.Background(), pair.request(data)); err != nil {
		t.Fatalf("SendSnapshot: %v", err)
	}
	select {
	case receipt := <-pair.received:
		if receipt.From != 1 || receipt.Index != 40 || receipt.Term != 3 || !bytes.Equal(receipt.Data, data) {
			t.Fatalf("receipt from %d index %d term %d with %d bytes does not match the sent snapshot",
				receipt.From, receipt.Index, receipt.Term, len(receipt.Data))
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("receiver did not hand the snapshot to Raft")
	}
	pair.receiver.snapshotMu.Lock()
	retained := pair.receiver.snapshotPartial
	pair.receiver.snapshotMu.Unlock()
	if retained != nil {
		t.Fatal("receiver retained bytes after a completed transfer")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamSenderUsesBoundedFramesAndChunks(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	data := randomSnapshot(t, 2*snapshotStreamChunkSize+777)
	server := newFakeSnapshotReceiver(t, pair)
	server.script = func(connection net.Conn, number int) { server.acceptTransfer(connection, 0, -1) }
	if err := pair.sender.SendSnapshot(context.Background(), pair.request(data)); err != nil {
		t.Fatalf("SendSnapshot: %v", err)
	}
	frames := server.chunkFrames()
	if len(frames) != 3 {
		t.Fatalf("chunk frames = %d, want 3 for %d bytes", len(frames), len(data))
	}
	for index, length := range frames {
		if length > snapshotStreamFrameLimit {
			t.Fatalf("chunk frame %d has %d bytes, above the %d byte frame limit", index, length, snapshotStreamFrameLimit)
		}
		if index < 2 && length != snapshotChunkHeaderSize+snapshotStreamChunkSize {
			t.Fatalf("chunk frame %d has %d bytes, want %d", index, length, snapshotChunkHeaderSize+snapshotStreamChunkSize)
		}
	}
	if !bytes.Equal(server.reassembled(), data) {
		t.Fatal("fake receiver reassembled different bytes than the sender sent")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamSenderResumesAfterConnectionLoss(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	data := randomSnapshot(t, 4*snapshotStreamChunkSize+10)
	server := newFakeSnapshotReceiver(t, pair)
	server.script = func(connection net.Conn, number int) {
		if number == 1 {
			server.acceptTransfer(connection, 0, 2) // drop after two chunks
			return
		}
		server.acceptTransfer(connection, 2*snapshotStreamChunkSize, -1)
	}
	if err := pair.sender.SendSnapshot(context.Background(), pair.request(data)); err != nil {
		t.Fatalf("SendSnapshot: %v", err)
	}
	if server.connectionCount() != 2 {
		t.Fatalf("connections = %d, want 2 (one interrupted, one resumed)", server.connectionCount())
	}
	if !bytes.Equal(server.reassembled(), data) {
		t.Fatal("resumed transfer reassembled different bytes")
	}
	if got := server.resumeOffsets(); len(got) != 2 || got[0] != 0 || got[1] != 2*snapshotStreamChunkSize {
		t.Fatalf("resume offsets = %v, want [0 %d]", got, 2*snapshotStreamChunkSize)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamSenderResumesAfterLosingLastAcknowledgement(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	data := randomSnapshot(t, 2*snapshotStreamChunkSize)
	server := newFakeSnapshotReceiver(t, pair)
	server.script = func(connection net.Conn, number int) {
		if number == 1 {
			server.acceptTransfer(connection, 0, 2)
			return
		}
		server.acceptTransfer(connection, len(data), -1)
	}
	if err := pair.sender.SendSnapshot(context.Background(), pair.request(data)); err != nil {
		t.Fatalf("resume a complete payload before DONE: %v", err)
	}
	if server.connectionCount() != 2 || !bytes.Equal(server.reassembled(), data) {
		t.Fatal("sender failed to finish the retained complete payload")
	}
	if frames := server.chunkFrames(); len(frames) != 2 {
		t.Fatalf("sent %d chunks, want 2: complete payload must not be sent again", len(frames))
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamSenderMapsReceiverErrorsWithoutRetry(t *testing.T) {
	for _, test := range []struct {
		code   byte
		reason string
	}{
		{snapshotErrorBusy, SnapshotReasonBusy},
		{snapshotErrorDigest, SnapshotReasonDigest},
		{snapshotErrorChecksum, SnapshotReasonChecksum},
		{snapshotErrorRejected, SnapshotReasonRejected},
		{snapshotErrorUnsupported, SnapshotReasonUnsupported},
	} {
		t.Run(test.reason, func(t *testing.T) {
			pair := newSnapshotStreamPair(t, false, nil)
			server := newFakeSnapshotReceiver(t, pair)
			server.script = func(connection net.Conn, number int) {
				_, _ = readDynamicFrame(connection, snapshotStreamFrameLimit) // manifest
				_ = writeDynamicFrame(connection, []byte{snapshotFrameError, test.code}, snapshotStreamFrameLimit)
			}
			err := pair.sender.SendSnapshot(context.Background(), pair.request(randomSnapshot(t, 1000)))
			if got := SnapshotStreamFailureReason(err); got != test.reason {
				t.Fatalf("SendSnapshot reason = %q (%v), want %q", got, err, test.reason)
			}
			if server.connectionCount() != 1 {
				t.Fatalf("connections = %d, want 1: protocol errors are not retried", server.connectionCount())
			}
		})
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamSenderRejectsUnexpectedAcknowledgement(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	server := newFakeSnapshotReceiver(t, pair)
	server.script = func(connection net.Conn, number int) {
		_, _ = readDynamicFrame(connection, snapshotStreamFrameLimit)
		_ = writeDynamicFrame(connection, encodeSnapshotOffsetFrame(snapshotFrameAccept, 0), snapshotStreamFrameLimit)
		_, _ = readDynamicFrame(connection, snapshotStreamFrameLimit) // first chunk
		_ = writeDynamicFrame(connection, encodeSnapshotOffsetFrame(snapshotFrameAck, 7), snapshotStreamFrameLimit)
	}
	err := pair.sender.SendSnapshot(context.Background(), pair.request(randomSnapshot(t, 1000)))
	if got := SnapshotStreamFailureReason(err); got != SnapshotReasonOutOfOrder {
		t.Fatalf("SendSnapshot reason = %q (%v), want out_of_order", got, err)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamSenderGivesUpAfterRepeatedConnectionLoss(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	server := newFakeSnapshotReceiver(t, pair)
	server.script = func(connection net.Conn, number int) {} // closes right after the hello
	err := pair.sender.SendSnapshot(context.Background(), pair.request(randomSnapshot(t, 1000)))
	if got := SnapshotStreamFailureReason(err); got != SnapshotReasonConnection {
		t.Fatalf("SendSnapshot reason = %q (%v), want connection", got, err)
	}
	if server.connectionCount() != snapshotStreamAttempts {
		t.Fatalf("connections = %d, want %d attempts", server.connectionCount(), snapshotStreamAttempts)
	}
}

func TestSnapshotStreamSenderRefusesWrongCertificateBeforeHello(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	other := cptNewNode(t, "node2.example") // same DNS name, different key and certificate
	cptTrustEachOther(pair.senderNode, other)
	var snapshotHellos atomic.Int32
	listener := tls.NewListener(pair.receiverListener, other.serverTLS)
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func(connection net.Conn) {
				defer connection.Close()
				if payload, err := readDynamicFrame(connection, dynamicHelloLimit); err == nil &&
					len(payload) >= 8 && string(payload[:8]) == dynamicSnapshotMagic {
					snapshotHellos.Add(1)
				}
			}(connection)
		}
	}()
	err := pair.sender.SendSnapshot(context.Background(), pair.request(randomSnapshot(t, 1000)))
	if got := SnapshotStreamFailureReason(err); got != SnapshotReasonConnection {
		t.Fatalf("SendSnapshot reason = %q (%v), want connection", got, err)
	}
	if snapshotHellos.Load() != 0 {
		t.Fatalf("sender sent %d snapshot hellos to a member with the wrong certificate", snapshotHellos.Load())
	}
}

func TestSnapshotStreamSenderRejectsInvalidRequestsAndLimitsConcurrency(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	for name, request := range map[string]SnapshotStreamDTO{
		"empty":        {To: 2, Index: 1, Term: 1},
		"zero index":   {To: 2, Term: 1, Data: []byte{1}},
		"zero term":    {To: 2, Index: 1, Data: []byte{1}},
		"no target":    {Index: 1, Term: 1, Data: []byte{1}},
		"above limit":  {To: 2, Index: 1, Term: 1, Data: make([]byte, snapshotStreamMaxBytes+1)},
		"unknown peer": {To: 9, Index: 1, Term: 1, Data: []byte{1}},
	} {
		err := pair.sender.SendSnapshot(context.Background(), request)
		if err == nil {
			t.Errorf("%s: SendSnapshot accepted an invalid request", name)
		}
	}

	blocked := make(chan struct{})
	entered := make(chan struct{}, 4*snapshotStreamOutboundSlots*snapshotStreamAttempts)
	server := newFakeSnapshotReceiver(t, pair)
	server.script = func(connection net.Conn, number int) {
		entered <- struct{}{}
		<-blocked
	}
	var wait sync.WaitGroup
	for range snapshotStreamOutboundSlots {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_ = pair.sender.SendSnapshot(context.Background(), pair.request(randomSnapshot(t, 100)))
		}()
	}
	for range snapshotStreamOutboundSlots {
		select {
		case <-entered:
		case <-time.After(cptWaitTimeout):
			t.Fatal("transfers did not start")
		}
	}
	err := pair.sender.SendSnapshot(context.Background(), pair.request(randomSnapshot(t, 100)))
	if got := SnapshotStreamFailureReason(err); got != SnapshotReasonBackpressure || !errors.Is(err, ErrPeerBackpressure) {
		t.Fatalf("third concurrent SendSnapshot = %v (reason %q), want backpressure", err, got)
	}
	close(blocked)
	wait.Wait()
}

func TestSnapshotStreamSenderCancelsWithContext(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	server := newFakeSnapshotReceiver(t, pair)
	stuck := make(chan struct{})
	server.script = func(connection net.Conn, number int) { <-stuck }
	defer close(stuck)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- pair.sender.SendSnapshot(ctx, pair.request(randomSnapshot(t, 100))) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if got := SnapshotStreamFailureReason(err); got != SnapshotReasonCanceled {
			t.Fatalf("SendSnapshot reason = %q (%v), want canceled", got, err)
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("SendSnapshot ignored context cancellation")
	}
}

func TestSnapshotStreamSenderCancelsWhenPeerStops(t *testing.T) {
	pair := newSnapshotStreamPair(t, false, nil)
	server := newFakeSnapshotReceiver(t, pair)
	entered, release := make(chan struct{}), make(chan struct{})
	server.script = func(connection net.Conn, number int) {
		close(entered)
		<-release
	}
	defer close(release)
	result := make(chan error, 1)
	go func() { result <- pair.sender.SendSnapshot(context.Background(), pair.request([]byte("snapshot"))) }()
	select {
	case <-entered:
	case <-time.After(cptWaitTimeout):
		t.Fatal("snapshot stream did not connect")
	}
	pair.sender.mu.Lock()
	peer := pair.sender.peers[2]
	pair.sender.mu.Unlock()
	peer.stop()
	select {
	case err := <-result:
		if got := SnapshotStreamFailureReason(err); got != SnapshotReasonCanceled {
			t.Fatalf("peer removal reason = %q (%v), want canceled", got, err)
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("snapshot stream did not stop with its peer")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamReceiverRejectsBadTransfers(t *testing.T) {
	data := randomSnapshot(t, 2*snapshotStreamChunkSize)
	good := func(pair *snapshotStreamPair) snapshotManifest { return pair.manifest(data) }
	chunk := func(offset int, payload []byte) []byte {
		return encodeSnapshotChunk(nil, uint64(offset), payload)
	}
	for _, test := range []struct {
		name string
		code byte
		run  func(t *testing.T, pair *snapshotStreamPair, connection net.Conn)
	}{
		{"chunk skips an offset", snapshotErrorOutOfOrder, func(t *testing.T, pair *snapshotStreamPair, connection net.Conn) {
			pair.acceptManifest(connection, good(pair))
			snapshotWrite(t, connection, chunk(snapshotStreamChunkSize, data[snapshotStreamChunkSize:]))
		}},
		{"chunk repeats an offset", snapshotErrorOutOfOrder, func(t *testing.T, pair *snapshotStreamPair, connection net.Conn) {
			pair.acceptManifest(connection, good(pair))
			snapshotWrite(t, connection, chunk(0, data[:snapshotStreamChunkSize]))
			snapshotRead(t, connection)
			snapshotWrite(t, connection, chunk(0, data[:snapshotStreamChunkSize]))
		}},
		{"chunk fails its CRC32C", snapshotErrorChecksum, func(t *testing.T, pair *snapshotStreamPair, connection net.Conn) {
			pair.acceptManifest(connection, good(pair))
			payload := chunk(0, data[:snapshotStreamChunkSize])
			payload[len(payload)-1] ^= 0xFF
			snapshotWrite(t, connection, payload)
		}},
		{"chunk runs past the manifest", snapshotErrorOverflow, func(t *testing.T, pair *snapshotStreamPair, connection net.Conn) {
			manifest := good(pair)
			manifest.total = 10
			pair.acceptManifest(connection, manifest)
			snapshotWrite(t, connection, chunk(0, data[:11]))
		}},
		{"DONE before every byte arrived", snapshotErrorOverflow, func(t *testing.T, pair *snapshotStreamPair, connection net.Conn) {
			pair.acceptManifest(connection, good(pair))
			snapshotWrite(t, connection, chunk(0, data[:snapshotStreamChunkSize]))
			snapshotRead(t, connection)
			snapshotWrite(t, connection, []byte{snapshotFrameDone})
		}},
		{"whole-snapshot digest differs", snapshotErrorDigest, func(t *testing.T, pair *snapshotStreamPair, connection net.Conn) {
			manifest := good(pair)
			manifest.digest[0] ^= 0xFF
			pair.acceptManifest(connection, manifest)
			for offset := 0; offset < len(data); offset += snapshotStreamChunkSize {
				snapshotWrite(t, connection, chunk(offset, data[offset:offset+snapshotStreamChunkSize]))
				snapshotRead(t, connection)
			}
			snapshotWrite(t, connection, []byte{snapshotFrameDone})
		}},
		{"manifest total is zero", snapshotErrorInvalidManifest, func(t *testing.T, pair *snapshotStreamPair, connection net.Conn) {
			manifest := good(pair)
			manifest.total = 0
			snapshotWrite(t, connection, encodeSnapshotManifest(manifest))
		}},
		{"manifest total exceeds 64 MiB", snapshotErrorInvalidManifest, func(t *testing.T, pair *snapshotStreamPair, connection net.Conn) {
			manifest := good(pair)
			manifest.total = snapshotStreamMaxBytes + 1
			snapshotWrite(t, connection, encodeSnapshotManifest(manifest))
		}},
		{"first frame is not a manifest", snapshotErrorInvalidManifest, func(t *testing.T, pair *snapshotStreamPair, connection net.Conn) {
			snapshotWrite(t, connection, []byte{snapshotFrameDone})
		}},
		{"unknown frame kind", snapshotErrorOutOfOrder, func(t *testing.T, pair *snapshotStreamPair, connection net.Conn) {
			pair.acceptManifest(connection, good(pair))
			snapshotWrite(t, connection, []byte{0x7E, 1, 2, 3})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			pair := newSnapshotStreamPair(t, true, nil)
			connection := pair.dialAsSender()
			test.run(t, pair, connection)
			expectSnapshotError(t, connection, test.code)
			pair.expectNoReceipt()
		})
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamReceiverRefusesOversizedFrames(t *testing.T) {
	pair := newSnapshotStreamPair(t, true, nil)
	connection := pair.dialAsSender()
	// A length prefix above the frame limit is refused before any payload is read.
	if _, err := connection.Write([]byte{0x00, 0x10, 0x00, 0x01}); err != nil {
		t.Fatal(err)
	}
	if _, err := readDynamicFrame(connection, snapshotStreamFrameLimit); err == nil {
		t.Fatal("receiver answered a frame above the limit")
	}
	pair.expectNoReceipt()
}

func TestSnapshotStreamReceiverAdmitsOneTransferAtATime(t *testing.T) {
	pair := newSnapshotStreamPair(t, true, nil)
	data := randomSnapshot(t, 1000)
	first := pair.dialAsSender()
	pair.acceptManifest(first, pair.manifest(data))
	second := pair.dialAsSender()
	snapshotWrite(t, second, encodeSnapshotManifest(pair.manifest(data)))
	expectSnapshotError(t, second, snapshotErrorBusy)
	// The first transfer is unaffected and still completes.
	snapshotWrite(t, first, encodeSnapshotChunk(nil, 0, data))
	if next, ok := decodeSnapshotOffsetFrame(snapshotRead(t, first), snapshotFrameAck); !ok || next != uint64(len(data)) {
		t.Fatalf("first transfer ACK = %d, %v", next, ok)
	}
	snapshotWrite(t, first, []byte{snapshotFrameDone})
	if got := snapshotRead(t, first); !bytes.Equal(got, []byte{snapshotFrameComplete}) {
		t.Fatalf("first transfer completion = %x", got)
	}
	select {
	case receipt := <-pair.received:
		if !bytes.Equal(receipt.Data, data) {
			t.Fatal("receipt differs from the sent snapshot")
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("first transfer never reached Raft")
	}
}

func TestSnapshotStreamReceiverReportsRejectedAndUnsupported(t *testing.T) {
	data := randomSnapshot(t, 1000)
	t.Run("Raft rejects the snapshot", func(t *testing.T) {
		pair := newSnapshotStreamPair(t, true, func(context.Context, SnapshotStreamReceiptDTO) error {
			return errors.New("raft backpressure")
		})
		connection := pair.dialAsSender()
		pair.acceptManifest(connection, pair.manifest(data))
		snapshotWrite(t, connection, encodeSnapshotChunk(nil, 0, data))
		snapshotRead(t, connection)
		snapshotWrite(t, connection, []byte{snapshotFrameDone})
		expectSnapshotError(t, connection, snapshotErrorRejected)
	})
	t.Run("no handler", func(t *testing.T) {
		pair := newSnapshotStreamPair(t, true, nil)
		pair.receiver.snapshotHandler = nil
		connection := pair.dialAsSender()
		snapshotWrite(t, connection, encodeSnapshotManifest(pair.manifest(data)))
		expectSnapshotError(t, connection, snapshotErrorUnsupported)
	})
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamReceiverResumesSameTransferOnly(t *testing.T) {
	data := randomSnapshot(t, 3*snapshotStreamChunkSize)
	pair := newSnapshotStreamPair(t, true, nil)
	manifest := pair.manifest(data)

	first := pair.dialAsSender()
	pair.acceptManifest(first, manifest)
	for offset := 0; offset < 2*snapshotStreamChunkSize; offset += snapshotStreamChunkSize {
		snapshotWrite(t, first, encodeSnapshotChunk(nil, uint64(offset), data[offset:offset+snapshotStreamChunkSize]))
		snapshotRead(t, first)
	}
	_ = first.Close()
	cptWaitUntil(t, "receiver to retain the partial transfer", func() bool {
		pair.receiver.snapshotMu.Lock()
		defer pair.receiver.snapshotMu.Unlock()
		return pair.receiver.snapshotPartial != nil && len(pair.receiver.snapshotPartial.data) == 2*snapshotStreamChunkSize
	})

	// A different snapshot from the same sender supersedes the retained bytes.
	other := manifest
	other.index++
	superseding := pair.dialAsSender()
	if offset := pair.acceptManifest(superseding, other); offset != 0 {
		t.Fatalf("a newer snapshot resumed at offset %d, want 0", offset)
	}
	_ = superseding.Close()
	cptWaitUntil(t, "receiver to drop the superseded bytes", func() bool {
		pair.receiver.snapshotMu.Lock()
		defer pair.receiver.snapshotMu.Unlock()
		return pair.receiver.snapshotPartial == nil
	})

	// The first transfer restarts from zero because its bytes were superseded, then completes.
	again := pair.dialAsSender()
	if offset := pair.acceptManifest(again, manifest); offset != 0 {
		t.Fatalf("superseded transfer resumed at offset %d, want 0", offset)
	}
	for offset := 0; offset < len(data); offset += snapshotStreamChunkSize {
		snapshotWrite(t, again, encodeSnapshotChunk(nil, uint64(offset), data[offset:offset+snapshotStreamChunkSize]))
		snapshotRead(t, again)
	}
	snapshotWrite(t, again, []byte{snapshotFrameDone})
	if got := snapshotRead(t, again); !bytes.Equal(got, []byte{snapshotFrameComplete}) {
		t.Fatalf("completion = %x", got)
	}
	if receipt := <-pair.received; !bytes.Equal(receipt.Data, data) {
		t.Fatal("receipt differs from the sent snapshot")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamReceiverResumeContinuesRetainedBytes(t *testing.T) {
	data := randomSnapshot(t, 3*snapshotStreamChunkSize)
	pair := newSnapshotStreamPair(t, true, nil)
	manifest := pair.manifest(data)
	first := pair.dialAsSender()
	pair.acceptManifest(first, manifest)
	snapshotWrite(t, first, encodeSnapshotChunk(nil, 0, data[:snapshotStreamChunkSize]))
	snapshotRead(t, first)
	_ = first.Close()
	cptWaitUntil(t, "receiver to retain the partial transfer", func() bool {
		pair.receiver.snapshotMu.Lock()
		defer pair.receiver.snapshotMu.Unlock()
		return pair.receiver.snapshotPartial != nil
	})
	second := pair.dialAsSender()
	if offset := pair.acceptManifest(second, manifest); offset != snapshotStreamChunkSize {
		t.Fatalf("resume offset = %d, want %d", offset, snapshotStreamChunkSize)
	}
	for offset := snapshotStreamChunkSize; offset < len(data); offset += snapshotStreamChunkSize {
		snapshotWrite(t, second, encodeSnapshotChunk(nil, uint64(offset), data[offset:offset+snapshotStreamChunkSize]))
		snapshotRead(t, second)
	}
	snapshotWrite(t, second, []byte{snapshotFrameDone})
	if got := snapshotRead(t, second); !bytes.Equal(got, []byte{snapshotFrameComplete}) {
		t.Fatalf("completion = %x", got)
	}
	if receipt := <-pair.received; !bytes.Equal(receipt.Data, data) {
		t.Fatal("resumed receipt differs from the sent snapshot")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamReceiverRefusesUnknownSender(t *testing.T) {
	pair := newSnapshotStreamPair(t, true, nil)
	stranger := cptNewNode(t, "node9.example")
	cptTrustEachOther(stranger, pair.receiverNode)
	connection, err := tls.Dial("tcp", pair.receiverIdentity.Address, stranger.dialConfig(pair.receiverIdentity.DNSSAN))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	hello, err := encodeDynamicPeerHello(stranger.identity(9, "127.0.0.1:1", PeerPhaseVoter, ""), dynamicSnapshotMagic)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDynamicFrame(connection, hello, dynamicHelloLimit); err != nil {
		t.Fatal(err)
	}
	if _, err := readDynamicFrame(connection, dynamicHelloLimit); err == nil {
		t.Fatal("receiver answered the hello of a member it does not know")
	}
	pair.expectNoReceipt()
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamReceiverRefusesSenderWithMismatchedCertificate(t *testing.T) {
	pair := newSnapshotStreamPair(t, true, nil)
	impostor := cptNewNode(t, "node1.example") // member 1's DNS name with another key
	cptTrustEachOther(impostor, pair.receiverNode)
	connection, err := tls.Dial("tcp", pair.receiverIdentity.Address, impostor.dialConfig(pair.receiverIdentity.DNSSAN))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	hello, err := encodeDynamicPeerHello(pair.senderIdentity, dynamicSnapshotMagic)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDynamicFrame(connection, hello, dynamicHelloLimit); err != nil {
		t.Fatal(err)
	}
	if _, err := readDynamicFrame(connection, dynamicHelloLimit); err == nil {
		t.Fatal("receiver answered a sender whose certificate is not the committed one")
	}
	pair.expectNoReceipt()
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-260
func TestSnapshotStreamWireCodecsRejectMalformedFrames(t *testing.T) {
	manifest := snapshotManifest{index: 1, term: 2, total: 3, digest: sha256.Sum256([]byte("x"))}
	encoded := encodeSnapshotManifest(manifest)
	if len(encoded) != snapshotManifestSize {
		t.Fatalf("manifest length = %d, want %d", len(encoded), snapshotManifestSize)
	}
	if decoded, ok := decodeSnapshotManifest(encoded); !ok || decoded != manifest {
		t.Fatalf("manifest round trip = %+v, %v", decoded, ok)
	}
	for name, payload := range map[string][]byte{
		"short":      encoded[:len(encoded)-1],
		"long":       append(bytes.Clone(encoded), 0),
		"wrong kind": append([]byte{snapshotFrameDone}, encoded[1:]...),
		"zero index": append(append([]byte{snapshotFrameManifest}, make([]byte, 8)...), encoded[9:]...),
	} {
		if _, ok := decodeSnapshotManifest(payload); ok {
			t.Errorf("%s manifest was accepted", name)
		}
	}
	chunk := encodeSnapshotChunk(nil, 9, []byte("data"))
	if offset, _, data, ok := decodeSnapshotChunk(chunk); !ok || offset != 9 || string(data) != "data" {
		t.Fatalf("chunk round trip = %d %q %v", offset, data, ok)
	}
	if _, _, _, ok := decodeSnapshotChunk(encodeSnapshotChunk(nil, 0, nil)); ok {
		t.Error("a chunk without data was accepted")
	}
	if _, ok := decodeSnapshotOffsetFrame([]byte{snapshotFrameAck, 0, 0}, snapshotFrameAck); ok {
		t.Error("a short offset frame was accepted")
	}
	if _, ok := decodeSnapshotOffsetFrame(encodeSnapshotOffsetFrame(snapshotFrameAck, 1), snapshotFrameAccept); ok {
		t.Error("an offset frame of the wrong kind was accepted")
	}
}

// fakeSnapshotReceiver plays the receiving peer by hand so a test controls every reply.
type fakeSnapshotReceiver struct {
	t       *testing.T
	pair    *snapshotStreamPair
	script  func(connection net.Conn, number int)
	mu      sync.Mutex
	count   int
	chunks  []int
	data    []byte
	offsets []int
}

func newFakeSnapshotReceiver(t *testing.T, pair *snapshotStreamPair) *fakeSnapshotReceiver {
	t.Helper()
	server := &fakeSnapshotReceiver{t: t, pair: pair}
	listener := tls.NewListener(pair.receiverListener, pair.receiverNode.serverTLS)
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go server.handle(connection)
		}
	}()
	return server
}

func (server *fakeSnapshotReceiver) handle(connection net.Conn) {
	defer connection.Close()
	payload, err := readDynamicFrame(connection, dynamicHelloLimit)
	if err != nil || len(payload) < 8 || string(payload[:8]) != dynamicSnapshotMagic {
		return // the sender's Raft-lane dialers also reach this listener
	}
	hello, err := encodeDynamicPeerHello(server.pair.receiverIdentity, dynamicSnapshotMagic)
	if err != nil || writeDynamicFrame(connection, hello, dynamicHelloLimit) != nil {
		return
	}
	server.mu.Lock()
	server.count++
	number := server.count
	server.mu.Unlock()
	if server.script != nil {
		server.script(connection, number)
	}
}

// acceptTransfer answers one transfer. It tells the sender to resume at resume, acknowledges every
// chunk and closes the connection after dropAfter chunks (-1 never drops).
func (server *fakeSnapshotReceiver) acceptTransfer(connection net.Conn, resume int, dropAfter int) {
	if _, err := readDynamicFrame(connection, snapshotStreamFrameLimit); err != nil {
		return
	}
	server.mu.Lock()
	server.offsets = append(server.offsets, resume)
	if resume == 0 {
		server.data = nil
	}
	server.mu.Unlock()
	if writeDynamicFrame(connection, encodeSnapshotOffsetFrame(snapshotFrameAccept, uint64(resume)), snapshotStreamFrameLimit) != nil {
		return
	}
	received := 0
	for {
		frame, err := readDynamicFrame(connection, snapshotStreamFrameLimit)
		if err != nil {
			return
		}
		switch frame[0] {
		case snapshotFrameChunk:
			offset, _, chunk, ok := decodeSnapshotChunk(frame)
			server.mu.Lock()
			server.chunks = append(server.chunks, len(frame))
			if !ok || int(offset) != len(server.data) {
				server.mu.Unlock()
				_ = writeDynamicFrame(connection, []byte{snapshotFrameError, snapshotErrorOutOfOrder}, snapshotStreamFrameLimit)
				return
			}
			server.data = append(server.data, chunk...)
			length := len(server.data)
			server.mu.Unlock()
			received++
			if dropAfter >= 0 && received >= dropAfter {
				return
			}
			if writeDynamicFrame(connection, encodeSnapshotOffsetFrame(snapshotFrameAck, uint64(length)), snapshotStreamFrameLimit) != nil {
				return
			}
		case snapshotFrameDone:
			_ = writeDynamicFrame(connection, []byte{snapshotFrameComplete}, snapshotStreamFrameLimit)
			return
		default:
			return
		}
	}
}

func (server *fakeSnapshotReceiver) connectionCount() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.count
}

func (server *fakeSnapshotReceiver) chunkFrames() []int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]int(nil), server.chunks...)
}

func (server *fakeSnapshotReceiver) reassembled() []byte {
	server.mu.Lock()
	defer server.mu.Unlock()
	return bytes.Clone(server.data)
}

func (server *fakeSnapshotReceiver) resumeOffsets() []int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]int(nil), server.offsets...)
}
