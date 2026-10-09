package tcp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

func TestPeerTransportHelloGoldenVector(t *testing.T) {
	var encoded bytes.Buffer
	if err := writePeerHello(&encoded, "x", 1); err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString("4e4d515241465431000100000001780000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded.Bytes(), want) {
		t.Fatalf("hello bytes = %x, want %x", encoded.Bytes(), want)
	}
	peerID, err := readPeerHello(bytes.NewReader(want), "x")
	if err != nil || peerID != 1 {
		t.Fatalf("read hello = (%d, %v), want (1, nil)", peerID, err)
	}
}

func TestPeerTransportHelloRejectsInvalidFields(t *testing.T) {
	valid := make([]byte, peerHelloFixedSize+1+8)
	copy(valid[:8], "NMQRAFT1")
	binary.BigEndian.PutUint16(valid[8:10], peerProtocolMajor)
	binary.BigEndian.PutUint16(valid[10:12], peerProtocolMinor)
	binary.BigEndian.PutUint16(valid[12:14], 1)
	valid[14] = 'x'
	binary.BigEndian.PutUint64(valid[15:23], 1)

	cases := []struct {
		name    string
		mutate  func([]byte)
		cluster string
	}{
		{name: "bad magic", mutate: func(data []byte) { data[0] = 'X' }, cluster: "x"},
		{name: "major mismatch", mutate: func(data []byte) { binary.BigEndian.PutUint16(data[8:10], 2) }, cluster: "x"},
		{name: "minor mismatch", mutate: func(data []byte) { binary.BigEndian.PutUint16(data[10:12], 1) }, cluster: "x"},
		{name: "zero cluster length", mutate: func(data []byte) { binary.BigEndian.PutUint16(data[12:14], 0) }, cluster: "x"},
		{name: "oversized cluster length", mutate: func(data []byte) { binary.BigEndian.PutUint16(data[12:14], 201) }, cluster: "x"},
		{name: "invalid UTF-8", mutate: func(data []byte) { data[14] = 0xff }, cluster: "x"},
		{name: "cluster mismatch", mutate: func([]byte) {}, cluster: "other"},
		{name: "unknown node", mutate: func(data []byte) { binary.BigEndian.PutUint64(data[15:23], 4) }, cluster: "x"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			data := bytes.Clone(valid)
			test.mutate(data)
			if _, err := readPeerHello(bytes.NewReader(data), test.cluster); err == nil {
				t.Fatal("readPeerHello accepted an invalid hello")
			}
		})
	}
	if _, err := readPeerHello(bytes.NewReader(nil), "x"); err == nil {
		t.Fatal("readPeerHello accepted a truncated header")
	}
	if _, err := readPeerHello(bytes.NewReader(valid[:peerHelloFixedSize]), "x"); err == nil {
		t.Fatal("readPeerHello accepted a truncated body")
	}
	if err := writePeerHello(io.Discard, "", 1); err == nil {
		t.Fatal("writePeerHello accepted an empty cluster ID")
	}
	if err := writePeerHello(io.Discard, "x", 4); err == nil {
		t.Fatal("writePeerHello accepted an unknown node ID")
	}
}

func TestPeerTransportFrameGoldenVectorAndLengthBounds(t *testing.T) {
	left, right := net.Pipe()
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- writePeerFrame(left, []byte{0x08, 0x01})
		_ = left.Close()
	}()
	var wire [6]byte
	if _, err := io.ReadFull(right, wire[:]); err != nil {
		t.Fatal(err)
	}
	_ = right.Close()
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire[:], []byte{0x00, 0x00, 0x00, 0x02, 0x08, 0x01}) {
		t.Fatalf("frame bytes = %x, want 000000020801", wire)
	}
	got, err := readPeerFrameBytes(t, 2, []byte{0x08, 0x01})
	if err != nil || !bytes.Equal(got, []byte{0x08, 0x01}) {
		t.Fatalf("read golden frame = (%x, %v)", got, err)
	}

	maxPayload := bytes.Repeat([]byte{0xa5}, peerMaxFrameSize)
	got, err = readPeerFrameBytes(t, uint32(len(maxPayload)), maxPayload)
	if err != nil {
		t.Fatalf("maximum frame rejected: %v", err)
	}
	if len(got) != peerMaxFrameSize || got[0] != 0xa5 || got[len(got)-1] != 0xa5 {
		t.Fatalf("maximum frame payload boundaries are wrong")
	}

	for _, test := range []struct {
		name   string
		length uint32
	}{{name: "zero", length: 0}, {name: "over_limit", length: peerMaxFrameSize + 1}} {
		t.Run(test.name, func(t *testing.T) {
			_, err := readPeerFrameBytes(t, test.length, nil)
			if err == nil {
				t.Fatalf("frame length %d was accepted", test.length)
			}
		})
	}
	if _, err := readPeerFrameBytes(t, 3, []byte{1, 2}); err == nil {
		t.Fatal("truncated frame was accepted")
	}
}
func TestPeerTransportFrameWriterReturnsSocketFailure(t *testing.T) {
	writer, reader := net.Pipe()
	_ = reader.Close()
	if err := writePeerFrame(writer, []byte{0x08, 0x01}); err == nil {
		t.Fatal("writePeerFrame succeeded after peer closed")
	}
	if err := writePeerHello(writer, "cluster", 1); err == nil {
		t.Fatal("writePeerHello succeeded after peer closed")
	}
	var partial shortPeerWriter
	partial.maxWrite = 1
	if err := writeFull(&partial, []byte{1, 2, 3}); err != nil || !bytes.Equal(partial.Bytes(), []byte{1, 2, 3}) {
		t.Fatalf("writeFull did not finish short writes: bytes=%v err=%v", partial.Bytes(), err)
	}
	partial.maxWrite = 0
	if err := writeFull(&partial, []byte{4}); err != io.ErrShortWrite {
		t.Fatalf("writeFull zero write = %v, want io.ErrShortWrite", err)
	}
	if err := writePeerFrame(nil, nil); err != ErrInvalidPeerMessage {
		t.Fatalf("writePeerFrame accepted an empty payload: %v", err)
	}
	if err := writePeerFrame(nil, make([]byte, peerMaxFrameSize+1)); err != ErrInvalidPeerMessage {
		t.Fatalf("writePeerFrame accepted an oversized payload: %v", err)
	}
	_ = writer.Close()
}

func readPeerFrameBytes(t *testing.T, length uint32, payload []byte) ([]byte, error) {
	t.Helper()
	reader, writer := net.Pipe()
	done := make(chan struct{})
	go func() {
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], length)
		_ = writeFull(writer, prefix[:])
		if len(payload) != 0 {
			_ = writeFull(writer, payload)
		}
		_ = writer.Close()
		close(done)
	}()
	got, err := readPeerFrame(reader)
	_ = reader.Close()
	<-done
	return got, err
}

func TestPeerTransportSendQueueBoundsCountAndTracksWriter(t *testing.T) {
	transport, peer := newQueueTestTransport()
	for _, message := range []PeerMessageDTO{
		{From: 2, To: 2, Data: []byte{1}},
		{From: 1, To: 1, Data: []byte{1}},
		{From: 1, To: 99, Data: []byte{1}},
		{From: 1, To: 2},
		{From: 1, To: 2, Data: make([]byte, peerMaxFrameSize+1)},
	} {
		if err := transport.Send(message); err != ErrInvalidPeerMessage {
			t.Fatalf("Send(%+v) = %v, want ErrInvalidPeerMessage", message, err)
		}
	}
	local, remote := net.Pipe()
	defer remote.Close()
	t.Cleanup(peer.stop)
	peer.mu.Lock()
	peer.connection = local
	peer.mu.Unlock()
	payload := []byte{0x08, 0x01}
	for range peerQueueMessages {
		if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: payload}); err != nil {
			t.Fatalf("Send at queue capacity: %v", err)
		}
	}
	payload[0] = 0xff
	first := peer.takeOutbound()
	if first == nil || first.data[0] != 0x08 {
		t.Fatal("Send did not retain an owned copy of message bytes")
	}
	status, err := transport.Status(2)
	if err != nil {
		t.Fatal(err)
	}
	if status.OutboundQueuedMessages != peerQueueMessages || status.OutboundQueuedBytes != peerQueueMessages*6 {
		t.Fatalf("in-progress writer not counted: %+v", status)
	}
	if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{1}}); err != ErrPeerBackpressure {
		t.Fatalf("Send at full queue = %v, want ErrPeerBackpressure", err)
	}
	if _, err := remote.Read(make([]byte, 1)); err == nil {
		t.Fatal("outbound backpressure did not close the active connection")
	}
	status, _ = transport.Status(2)
	if status.RejectedMessages != 1 || status.LastError != "outbound queue full" {
		t.Fatalf("backpressure status = %+v", status)
	}
	peer.completeOutbound(first)
	status, _ = transport.Status(2)
	if status.OutboundQueuedMessages != peerQueueMessages-1 {
		t.Fatalf("completed writer item remained queued: %+v", status)
	}
	peer.stop()
	status, _ = transport.Status(2)
	if status.OutboundQueuedMessages != 0 || status.OutboundQueuedBytes != 0 {
		t.Fatalf("Close left outbound queue data: %+v", status)
	}
	if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{1}}); err != ErrPeerTransportClosed {
		t.Fatalf("Send after peer stop = %v, want ErrPeerTransportClosed", err)
	}
	if peer.takeOutbound() != nil {
		t.Fatal("stopped peer returned an outbound frame")
	}
}

func TestPeerTransportSendQueueBoundsFramedBytes(t *testing.T) {
	transport, peer := newQueueTestTransport()
	payload := make([]byte, peerMaxFrameSize)
	for index := range 3 {
		if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: payload}); err != nil {
			t.Fatalf("Send maximum frame %d: %v", index, err)
		}
	}
	if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: payload}); err != ErrPeerBackpressure {
		t.Fatalf("Send beyond byte capacity = %v, want ErrPeerBackpressure", err)
	}
	status, err := transport.Status(2)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes := 3 * (peerMaxFrameSize + 4)
	if status.OutboundQueuedMessages != 3 || status.OutboundQueuedBytes != wantBytes {
		t.Fatalf("byte-bound status = %+v, want 3 messages/%d bytes", status, wantBytes)
	}
	peer.stop()
}

func TestPeerTransportInboundQueueBoundsFramedBytes(t *testing.T) {
	_, peer := newQueueTestTransport()
	payload := make([]byte, peerMaxFrameSize)
	for range 3 {
		if err := peer.enqueueInbound(payload); err != nil {
			t.Fatalf("enqueue maximum frame: %v", err)
		}
	}
	if err := peer.enqueueInbound(payload); err != ErrPeerBackpressure {
		t.Fatalf("enqueue beyond byte capacity = %v, want ErrPeerBackpressure", err)
	}
	status := peer.status()
	wantBytes := 3 * (peerMaxFrameSize + 4)
	if status.InboundQueuedMessages != 3 || status.InboundQueuedBytes != wantBytes {
		t.Fatalf("inbound byte-bound status = %+v", status)
	}
	peer.drainInbound()
	status = peer.status()
	if status.InboundQueuedMessages != 0 || status.InboundQueuedBytes != 0 {
		t.Fatalf("inbound queue drain left bytes: %+v", status)
	}
	for range peerQueueMessages {
		if err := peer.enqueueInbound([]byte{1}); err != nil {
			t.Fatalf("enqueue message-count boundary: %v", err)
		}
	}
	if err := peer.enqueueInbound([]byte{1}); err != ErrPeerBackpressure {
		t.Fatalf("enqueue beyond message-count capacity = %v, want ErrPeerBackpressure", err)
	}
	status = peer.status()
	if status.InboundQueuedMessages != peerQueueMessages || status.InboundQueuedBytes != peerQueueMessages*5 {
		t.Fatalf("inbound message-count status = %+v", status)
	}
	peer.drainInbound()
	peer.stop()
	if err := peer.enqueueInbound([]byte{1}); err != ErrPeerTransportClosed {
		t.Fatalf("enqueue after peer stop = %v, want ErrPeerTransportClosed", err)
	}
}

func TestPeerTransportInboundCancellationDrainsStagedFrames(t *testing.T) {
	_, peer := newQueueTestTransport()
	for range 2 {
		if err := peer.enqueueInbound([]byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		peer.deliverInbound(ctx, make(chan PeerMessageDTO))
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for len(peer.inbox) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(peer.inbox) != 1 {
		t.Fatalf("delivery worker did not block with one staged frame: %d", len(peer.inbox))
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("delivery worker did not stop after cancellation")
	}
	status := peer.status()
	if status.InboundQueuedMessages != 0 || status.InboundQueuedBytes != 0 {
		t.Fatalf("canceled delivery left staged frames: %+v", status)
	}
}

func TestPeerTransportConnectionBoundsAndCancellation(t *testing.T) {
	t.Run("inbound queue full closes connection", func(t *testing.T) {
		_, peer := newQueueTestTransport()
		for range peerQueueMessages {
			if err := peer.enqueueInbound([]byte{1}); err != nil {
				t.Fatal(err)
			}
		}
		local, remote := net.Pipe()
		defer remote.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			peer.runConnection(ctx, local)
			close(done)
		}()
		if err := writePeerFrame(remote, []byte{2}); err != nil {
			t.Fatalf("write frame into full inbound queue: %v", err)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("peer connection did not stop when inbound staging was full")
		}
		status := peer.status()
		if status.LastError != "inbound queue full" || status.InboundQueuedMessages != peerQueueMessages {
			t.Fatalf("inbound queue overflow status = %+v", status)
		}
		peer.drainInbound()
	})

	t.Run("cancel closes idle connection", func(t *testing.T) {
		_, peer := newQueueTestTransport()
		local, remote := net.Pipe()
		defer remote.Close()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			peer.runConnection(ctx, local)
			close(done)
		}()
		deadline := time.Now().Add(time.Second)
		for !peer.status().Connected && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if !peer.status().Connected {
			cancel()
			t.Fatal("peer connection did not start")
		}
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("peer connection did not stop after cancellation")
		}
		if peer.status().Connected {
			t.Fatal("canceled peer connection remained active")
		}
	})
}

// Traceability: SEC-09; docs/contracts/raft-peer-transport-v1.md §1.
func TestPeerTransportRejectsInboundCertificateForWrongVoter(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, clientTLS := testPeerTLSConfigs(t, "node2.example")
	impostorServerTLS, impostorClientTLS := testPeerTLSConfigs(t, "impostor.example")
	serverTLS.ClientCAs.AddCert(impostorServerTLS.Certificates[0].Leaf)
	impostorClientTLS.RootCAs.AddCert(serverTLS.Certificates[0].Leaf)
	impostorClientTLS.ServerName = "node2.example"
	transport, err := NewTCPPeerTransport(PeerTransportConfig{
		NodeID:       2,
		ClusterID:    "test-cluster",
		LocalDNSName: "node2.example",
		Listener:     listener,
		ServerTLS:    serverTLS,
		ClientTLS:    clientTLS,
		Peers: map[uint64]PeerEndpoint{
			1: {Address: "127.0.0.1:1", DNSName: "node1.example"},
			3: {Address: "127.0.0.1:1", DNSName: "node3.example"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveResult := make(chan error, 1)
	go func() { serveResult <- transport.Serve(ctx) }()
	connection, err := tls.Dial("tcp", listener.Addr().String(), impostorClientTLS.Clone())
	if err != nil {
		_ = transport.Close()
		t.Fatalf("complete authenticated TLS handshake: %v", err)
	}
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	defer connection.Close()
	if peerID, err := readPeerHello(connection, "test-cluster"); err != nil || peerID != 2 {
		_ = transport.Close()
		t.Fatalf("read local hello = (%d, %v), want (2, nil)", peerID, err)
	}
	if err := writePeerHello(connection, "test-cluster", 1); err != nil {
		_ = transport.Close()
		t.Fatalf("write allowlisted voter hello: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, statusErr := transport.Status(1)
		if statusErr == nil && status.LastError == "peer certificate identity mismatch" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, _ := transport.Status(1)
	if status.Connected || status.LastError != "peer certificate identity mismatch" {
		_ = transport.Close()
		t.Fatalf("wrong voter certificate was not rejected: %+v", status)
	}
	cancel()
	if err := <-serveResult; err != nil {
		t.Fatalf("serve shutdown: %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPeerTransportLimitsConcurrentInboundConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverTLS, clientTLS := testPeerTLSConfigs(t, "node2.example")
	transport, err := NewTCPPeerTransport(PeerTransportConfig{
		NodeID:       2,
		ClusterID:    "test-cluster",
		LocalDNSName: "node2.example",
		Listener:     listener,
		ServerTLS:    serverTLS,
		ClientTLS:    clientTLS,
		Peers: map[uint64]PeerEndpoint{
			1: {Address: "127.0.0.1:1", DNSName: "node1.example"},
			3: {Address: "127.0.0.1:1", DNSName: "node3.example"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	first, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		_ = transport.Close()
		t.Fatal(err)
	}
	defer first.Close()
	acceptResult := make(chan error, 1)
	go func() { acceptResult <- transport.acceptLoop(ctx) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		transport.connectionMu.Lock()
		accepted := len(transport.connections)
		transport.connectionMu.Unlock()
		if accepted == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	transport.connectionMu.Lock()
	accepted := len(transport.connections)
	transport.connectionMu.Unlock()
	if accepted != 1 {
		cancel()
		_ = transport.Close()
		t.Fatalf("first inbound connection was not tracked: %d", accepted)
	}
	second, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		cancel()
		_ = transport.Close()
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := second.Read(make([]byte, 1)); err == nil {
		cancel()
		_ = transport.Close()
		t.Fatal("transport accepted inbound connections beyond the fixed-voter limit")
	}
	cancel()
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-acceptResult:
		if err != nil {
			t.Fatalf("accept loop shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("accept loop did not stop after cancellation")
	}
	transport.workers.Wait()
}

func TestPeerTransportCloseCancelsServeAndClearsQueues(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, clientTLS := testPeerTLSConfigs(t, "node1.example")
	transport, err := NewTCPPeerTransport(PeerTransportConfig{
		NodeID:       1,
		ClusterID:    "test-cluster",
		LocalDNSName: "node1.example",
		Listener:     listener,
		ServerTLS:    serverTLS,
		ClientTLS:    clientTLS,
		Peers: map[uint64]PeerEndpoint{
			2: {Address: "127.0.0.1:1", DNSName: "node2.example"},
			3: {Address: "127.0.0.1:1", DNSName: "node3.example"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Serve(nil); err == nil {
		t.Fatal("Serve accepted a nil context")
	}
	if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() { serveResult <- transport.Serve(ctx) }()
	cancel()
	select {
	case err := <-serveResult:
		if err != nil {
			t.Fatalf("Serve after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not stop after cancellation")
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-transport.Receive(); ok {
		t.Fatal("Receive remained open after Close")
	}
	status, err := transport.Status(2)
	if err != nil {
		t.Fatal(err)
	}
	if status.OutboundQueuedMessages != 0 || status.OutboundQueuedBytes != 0 {
		t.Fatalf("Close left queue data: %+v", status)
	}
	if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{1}}); err != ErrPeerTransportClosed {
		t.Fatalf("Send after Close = %v, want ErrPeerTransportClosed", err)
	}
	if _, err := transport.Status(99); err != ErrUnknownPeer {
		t.Fatalf("Status for unknown peer = %v, want ErrUnknownPeer", err)
	}
}

func TestPeerTransportOutboundHandshakeFailuresStayPeerLocal(t *testing.T) {
	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener3, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listener1.Close()
		t.Fatal(err)
	}
	plainListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listener1.Close()
		_ = listener3.Close()
		t.Fatal(err)
	}
	serverTLS1, clientTLS1 := testPeerTLSConfigs(t, "node1.example")
	serverTLS3, clientTLS3 := testPeerTLSConfigs(t, "node3.example")
	clientTLS1.RootCAs.AddCert(serverTLS3.Certificates[0].Leaf)
	serverTLS3.ClientCAs.AddCert(serverTLS1.Certificates[0].Leaf)
	clientTLS3.RootCAs.AddCert(serverTLS1.Certificates[0].Leaf)
	transport1, err := NewTCPPeerTransport(PeerTransportConfig{
		NodeID:       1,
		ClusterID:    "test-cluster",
		LocalDNSName: "node1.example",
		Listener:     listener1,
		ServerTLS:    serverTLS1,
		ClientTLS:    clientTLS1,
		Peers: map[uint64]PeerEndpoint{
			2: {Address: listener3.Addr().String(), DNSName: "node3.example"},
			3: {Address: plainListener.Addr().String(), DNSName: "node3.example"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	transport3, err := NewTCPPeerTransport(PeerTransportConfig{
		NodeID:       3,
		ClusterID:    "test-cluster",
		LocalDNSName: "node3.example",
		Listener:     listener3,
		ServerTLS:    serverTLS3,
		ClientTLS:    clientTLS3,
		Peers: map[uint64]PeerEndpoint{
			1: {Address: listener1.Addr().String(), DNSName: "node1.example"},
			2: {Address: plainListener.Addr().String(), DNSName: "node2.example"},
		},
	})
	if err != nil {
		_ = transport1.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = transport1.Close()
		_ = transport3.Close()
		_ = listener1.Close()
		_ = listener3.Close()
		_ = plainListener.Close()
	})
	plainDone := make(chan struct{})
	go func() {
		defer close(plainDone)
		for {
			connection, err := plainListener.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	serve1Result := make(chan error, 1)
	serve3Result := make(chan error, 1)
	go func() { serve1Result <- transport1.Serve(ctx) }()
	go func() { serve3Result <- transport3.Serve(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status2, _ := transport1.Status(2)
		status3, _ := transport1.Status(3)
		if status2.LastError == "peer identity mismatch" && status3.LastError == "peer connection failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	status2, _ := transport1.Status(2)
	status3, _ := transport1.Status(3)
	if status2.LastError != "peer identity mismatch" || status3.LastError != "peer connection failed" {
		t.Fatalf("outbound peer failures were not isolated: peer2=%+v peer3=%+v", status2, status3)
	}
	cancel()
	for _, result := range []<-chan error{serve1Result, serve3Result} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("transport shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("transport did not stop after cancellation")
		}
	}
	_ = listener1.Close()
	_ = listener3.Close()
	_ = plainListener.Close()
	select {
	case <-plainDone:
	case <-time.After(5 * time.Second):
		t.Fatal("plain TLS-failure peer accept loop did not stop")
	}
}

func TestPeerTransportNewRejectsInvalidPeerSetAndTLS(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverTLS, clientTLS := testPeerTLSConfigs(t, "node1.example")
	base := PeerTransportConfig{
		NodeID:       1,
		ClusterID:    "cluster",
		LocalDNSName: "node1.example",
		Listener:     listener,
		ServerTLS:    serverTLS,
		ClientTLS:    clientTLS,
		Peers: map[uint64]PeerEndpoint{
			2: {Address: "127.0.0.1:10002", DNSName: "node2.example"},
			3: {Address: "127.0.0.1:10003", DNSName: "node3.example"},
		},
	}
	invalid := []struct {
		name   string
		mutate func(*PeerTransportConfig)
	}{
		{name: "bad node id", mutate: func(config *PeerTransportConfig) { config.NodeID = 4 }},
		{name: "empty cluster id", mutate: func(config *PeerTransportConfig) { config.ClusterID = "" }},
		{name: "missing peer", mutate: func(config *PeerTransportConfig) { delete(config.Peers, 3) }},
		{name: "peer address without port", mutate: func(config *PeerTransportConfig) {
			endpoint := config.Peers[2]
			endpoint.Address = "localhost"
			config.Peers[2] = endpoint
		}},
		{name: "peer DNS SAN missing", mutate: func(config *PeerTransportConfig) {
			endpoint := config.Peers[2]
			endpoint.DNSName = ""
			config.Peers[2] = endpoint
		}},
		{name: "server client CA missing", mutate: func(config *PeerTransportConfig) {
			config.ServerTLS = config.ServerTLS.Clone()
			config.ServerTLS.ClientCAs = nil
		}},
		{name: "outbound root CA missing", mutate: func(config *PeerTransportConfig) {
			config.ClientTLS = config.ClientTLS.Clone()
			config.ClientTLS.RootCAs = nil
		}},
		{name: "TLS bypass enabled", mutate: func(config *PeerTransportConfig) {
			config.ClientTLS = config.ClientTLS.Clone()
			config.ClientTLS.InsecureSkipVerify = true
		}},
		{name: "invalid cluster ID encoding", mutate: func(config *PeerTransportConfig) { config.ClusterID = string([]byte{0xff}) }},
		{name: "cluster ID exceeds limit", mutate: func(config *PeerTransportConfig) { config.ClusterID = string(make([]byte, 201)) }},
		{name: "missing listener", mutate: func(config *PeerTransportConfig) { config.Listener = nil }},
		{name: "IP used as local DNS name", mutate: func(config *PeerTransportConfig) { config.LocalDNSName = "127.0.0.1" }},
		{name: "local ID in peer set", mutate: func(config *PeerTransportConfig) {
			delete(config.Peers, 3)
			config.Peers[1] = PeerEndpoint{Address: "127.0.0.1:10001", DNSName: "node1.example"}
		}},
		{name: "nonnumeric peer port", mutate: func(config *PeerTransportConfig) {
			endpoint := config.Peers[2]
			endpoint.Address = "127.0.0.1:port"
			config.Peers[2] = endpoint
		}},
		{name: "peer port out of range", mutate: func(config *PeerTransportConfig) {
			endpoint := config.Peers[2]
			endpoint.Address = "127.0.0.1:65536"
			config.Peers[2] = endpoint
		}},
		{name: "local certificate SAN mismatch", mutate: func(config *PeerTransportConfig) { config.LocalDNSName = "node2.example" }},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			config := clonePeerTransportConfig(base)
			test.mutate(&config)
			if transport, err := NewTCPPeerTransport(config); err == nil {
				_ = transport.Close()
				t.Fatal("NewTCPPeerTransport accepted invalid config")
			}
		})
	}
}

func TestPeerTransportLocalCertificateValidation(t *testing.T) {
	serverTLS, _ := testPeerTLSConfigs(t, "node1.example")
	valid := serverTLS.Certificates[0]
	if err := validateLocalCertificate(valid, "node1.example"); err != nil {
		t.Fatalf("validate valid local identity: %v", err)
	}
	parsedOnDemand := valid
	parsedOnDemand.Leaf = nil
	if err := validateLocalCertificate(parsedOnDemand, "node1.example"); err != nil {
		t.Fatalf("validate parsed local identity: %v", err)
	}
	for name, certificate := range map[string]tls.Certificate{
		"missing":     {},
		"invalid DER": {Certificate: [][]byte{{0xff}}},
	} {
		if err := validateLocalCertificate(certificate, "node1.example"); err == nil {
			t.Fatalf("validate %s certificate: unexpectedly succeeded", name)
		}
	}
	if err := validateLocalCertificate(valid, "node2.example"); err == nil {
		t.Fatal("validateLocalCertificate accepted a certificate without the configured DNS SAN")
	}
}
func newQueueTestTransport() (*TCPPeerTransport, *peerTransportState) {
	transport := &TCPPeerTransport{localID: 1, peers: make(map[uint64]*peerTransportState, 2)}
	peer := &peerTransportState{
		owner: transport,
		id:    2,
		wake:  make(chan struct{}, 1),
		inbox: make(chan peerFrame, peerQueueMessages),
	}
	transport.peers[2] = peer
	transport.peers[3] = &peerTransportState{owner: transport, id: 3}
	return transport, peer
}

func (peer *peerTransportState) status() PeerTransportStatusDTO {
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
	}
}

func clonePeerTransportConfig(source PeerTransportConfig) PeerTransportConfig {
	config := source
	config.Peers = make(map[uint64]PeerEndpoint, len(source.Peers))
	for peerID, endpoint := range source.Peers {
		config.Peers[peerID] = endpoint
	}
	config.ServerTLS = source.ServerTLS.Clone()
	config.ClientTLS = source.ClientTLS.Clone()
	return config
}

type shortPeerWriter struct {
	maxWrite int
	buffer   bytes.Buffer
}

func (writer *shortPeerWriter) Write(data []byte) (int, error) {
	if len(data) > writer.maxWrite {
		data = data[:writer.maxWrite]
	}
	if len(data) == 0 {
		return 0, nil
	}
	return writer.buffer.Write(data)
}

func (writer *shortPeerWriter) Bytes() []byte {
	return writer.buffer.Bytes()
}

func testPeerTLSConfigs(t *testing.T, dnsName string) (*tls.Config, *tls.Config) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	certificateTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: dnsName},
		DNSNames:              []string{dnsName},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, certificateTemplate, certificateTemplate, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pair.Leaf = leaf
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert},
		&tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: pool}
}
func TestPeerTransportResetPeerClosesOnlyActivePeerConnection(t *testing.T) {
	transport, peer := newQueueTestTransport()
	local, remote := net.Pipe()
	defer remote.Close()
	peer.mu.Lock()
	peer.connection = local
	peer.mu.Unlock()

	if err := transport.ResetPeer(2); err != nil {
		t.Fatalf("reset active peer connection: %v", err)
	}
	if _, err := remote.Read(make([]byte, 1)); err == nil {
		t.Fatal("ResetPeer left the active connection open")
	}
	status, err := transport.Status(2)
	if err != nil {
		t.Fatal(err)
	}
	if status.Connected || status.RejectedMessages != 1 || status.LastError != "invalid peer message" {
		t.Fatalf("peer status after reset = %+v", status)
	}
	if err := transport.ResetPeer(99); err != ErrUnknownPeer {
		t.Fatalf("reset unknown peer error = %v, want %v", err, ErrUnknownPeer)
	}
	peer.mu.Lock()
	peer.stopped = true
	peer.mu.Unlock()
	if err := transport.ResetPeer(2); err != ErrPeerTransportClosed {
		t.Fatalf("reset stopped peer error = %v, want %v", err, ErrPeerTransportClosed)
	}
}
