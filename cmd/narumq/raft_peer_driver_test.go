package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"math/big"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
)

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-014
func TestProcessRaftMessagesContinuesOtherPeersAndSkipsRejectedFIFO(t *testing.T) {
	node, err := raftadapter.Open(raftadapter.Config{
		NodeID:    1,
		PeerIDs:   []raftadapter.NodeID{1, 2, 3},
		Directory: filepath.Join(t.TempDir(), "node"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	transport := newDriverQueueTransport(t)
	defer transport.Close()

	for range 32 {
		err := transport.Send(tcpadapter.PeerMessageDTO{From: 1, To: 3, Data: []byte{0xff}})
		if err != nil {
			t.Fatalf("fill peer 3 queue before its capacity: %v", err)
		}
	}
	before3, err := transport.Status(3)
	if err != nil {
		t.Fatal(err)
	}
	before2, err := transport.Status(2)
	if err != nil {
		t.Fatal(err)
	}
	messages := []raftadapter.MessageDTO{
		{From: 1, To: 3, Data: []byte{1}, IsSnapshot: true},
		{From: 1, To: 3, Data: []byte{2}, IsSnapshot: true},
		{From: 1, To: 2, Data: []byte{3}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := processRaftMessages(context.Background(), node, transport, logger, messages); err != nil {
		t.Fatalf("process Raft messages after backpressure: %v", err)
	}
	after3, _ := transport.Status(3)
	if after3.OutboundQueuedMessages != before3.OutboundQueuedMessages || after3.RejectedMessages != before3.RejectedMessages+1 {
		t.Fatalf("rejected peer received later FIFO frames or repeated rejection: before=%+v after=%+v", before3, after3)
	}
	after2, _ := transport.Status(2)
	if after2.OutboundQueuedMessages != before2.OutboundQueuedMessages+1 {
		t.Fatalf("healthy peer did not receive its message after peer 3 backpressure: before=%+v after=%+v", before2, after2)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-013, cluster-session-and-delivery.md:CLU-SCN-014, performance-and-verification.md:PRF-SCN-097
func TestRaftPeerDriverTicksQueuesAndStops(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener2.Close() })
	listener3, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener3.Close() })
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	certificateTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "node1.example"},
		DNSNames:              []string{"node1.example"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, certificateTemplate, certificateTemplate, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	identity := tls.Certificate{Certificate: [][]byte{certificateDER}, PrivateKey: privateKey, Leaf: certificate}
	node3Key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	node3Template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "node3.example"},
		DNSNames:     []string{"node3.example"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	node3DER, err := x509.CreateCertificate(rand.Reader, node3Template, certificate, &node3Key.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	node3Certificate, err := x509.ParseCertificate(node3DER)
	if err != nil {
		t.Fatal(err)
	}
	node3Identity := tls.Certificate{Certificate: [][]byte{node3DER, certificateDER}, PrivateKey: node3Key, Leaf: node3Certificate}
	node2Key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	node2Template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "node2.example"},
		DNSNames:     []string{"node2.example"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	node2DER, err := x509.CreateCertificate(rand.Reader, node2Template, certificate, &node2Key.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	node2Certificate, err := x509.ParseCertificate(node2DER)
	if err != nil {
		t.Fatal(err)
	}
	node2Identity := tls.Certificate{Certificate: [][]byte{node2DER, certificateDER}, PrivateKey: node2Key, Leaf: node2Certificate}
	serverTLS := &tls.Config{Certificates: []tls.Certificate{identity}, ClientCAs: roots}
	clientTLS := &tls.Config{Certificates: []tls.Certificate{identity}, RootCAs: roots}
	transport, err := tcpadapter.NewTCPPeerTransport(tcpadapter.PeerTransportConfig{
		NodeID:       1,
		ClusterID:    "driver-test",
		LocalDNSName: "node1.example",
		Listener:     listener,
		ServerTLS:    serverTLS,
		ClientTLS:    clientTLS,
		Peers: map[uint64]tcpadapter.PeerEndpoint{
			2: {Address: listener2.Addr().String(), DNSName: "node2.example"},
			3: {Address: listener3.Addr().String(), DNSName: "node3.example"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	serverTLS2 := &tls.Config{Certificates: []tls.Certificate{node2Identity}, ClientCAs: roots}
	clientTLS2 := &tls.Config{Certificates: []tls.Certificate{node2Identity}, RootCAs: roots}
	transport2, err := tcpadapter.NewTCPPeerTransport(tcpadapter.PeerTransportConfig{
		NodeID:       2,
		ClusterID:    "driver-test",
		LocalDNSName: "node2.example",
		Listener:     listener2,
		ServerTLS:    serverTLS2,
		ClientTLS:    clientTLS2,
		Peers: map[uint64]tcpadapter.PeerEndpoint{
			1: {Address: listener.Addr().String(), DNSName: "node1.example"},
			3: {Address: listener3.Addr().String(), DNSName: "node3.example"},
		},
	})
	if err != nil {
		_ = transport.Close()
		t.Fatal(err)
	}
	serverTLS3 := &tls.Config{Certificates: []tls.Certificate{node3Identity}, ClientCAs: roots}
	clientTLS3 := &tls.Config{Certificates: []tls.Certificate{node3Identity}, RootCAs: roots}
	transport3, err := tcpadapter.NewTCPPeerTransport(tcpadapter.PeerTransportConfig{
		NodeID:       3,
		ClusterID:    "driver-test",
		LocalDNSName: "node3.example",
		Listener:     listener3,
		ServerTLS:    serverTLS3,
		ClientTLS:    clientTLS3,
		Peers: map[uint64]tcpadapter.PeerEndpoint{
			1: {Address: listener.Addr().String(), DNSName: "node1.example"},
			2: {Address: listener2.Addr().String(), DNSName: "node2.example"},
		},
	})
	if err != nil {
		_ = transport.Close()
		t.Fatal(err)
	}
	for range 32 {
		if err := transport.Send(tcpadapter.PeerMessageDTO{From: 1, To: 3, Data: []byte{0xff}}); err != nil {
			_ = transport.Close()
			t.Fatalf("prefill isolated peer queue: %v", err)
		}
	}
	for range 32 {
		if err := transport2.Send(tcpadapter.PeerMessageDTO{From: 2, To: 3, Data: []byte{0xff}}); err != nil {
			_ = transport.Close()
			_ = transport2.Close()
			t.Fatalf("prefill peer2 isolated peer queue: %v", err)
		}
	}
	node1, err := raftadapter.Open(raftadapter.Config{
		NodeID:    1,
		PeerIDs:   []raftadapter.NodeID{1, 2, 3},
		Directory: filepath.Join(t.TempDir(), "node-1"),
	})
	if err != nil {
		_ = transport.Close()
		_ = transport2.Close()
		_ = transport3.Close()
		t.Fatal(err)
	}
	node2, err := raftadapter.Open(raftadapter.Config{
		NodeID:    2,
		PeerIDs:   []raftadapter.NodeID{1, 2, 3},
		Directory: filepath.Join(t.TempDir(), "node-2"),
	})
	if err != nil {
		_ = node1.Close()
		_ = transport.Close()
		_ = transport2.Close()
		_ = transport3.Close()
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = transport.Close()
		_ = transport2.Close()
		_ = transport3.Close()
		_ = node1.Close()
		_ = node2.Close()
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	result1 := make(chan error, 1)
	result2 := make(chan error, 1)
	go func() { result1 <- runRaftPeerDriver(ctx, node1, transport, logger) }()
	go func() { result2 <- runRaftPeerDriver(ctx, node2, transport2, logger) }()

	// etcd raft ignores Campaign until the bootstrap ConfChange entries are applied.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := node1.Status()
		if status.CommitIndex > 0 && status.AppliedIndex >= status.CommitIndex {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status := node1.Status(); status.CommitIndex == 0 || status.AppliedIndex < status.CommitIndex {
		cancel()
		<-result1
		<-result2
		t.Fatalf("node1 did not apply bootstrap configuration before campaign: %+v", status)
	}
	if err := node1.Campaign(); err != nil {
		cancel()
		<-result1
		<-result2
		t.Fatal(err)
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leaderID := uint64(0)
		if node1.Status().Role == raftadapter.RoleLeader {
			leaderID = 1
		} else if node2.Status().Role == raftadapter.RoleLeader {
			leaderID = 2
		}
		if leaderID != 0 {
			leaderTransport := transport
			if leaderID == 2 {
				leaderTransport = transport2
			}
			status, statusErr := leaderTransport.Status(3)
			if statusErr == nil && status.RejectedMessages > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	leaderID := uint64(0)
	if node1.Status().Role == raftadapter.RoleLeader {
		leaderID = 1
	} else if node2.Status().Role == raftadapter.RoleLeader {
		leaderID = 2
	}
	leaderNode, followerNode, leaderTransport := node1, node2, transport
	if leaderID == 2 {
		leaderNode, followerNode, leaderTransport = node2, node1, transport2
	}
	status3, _ := leaderTransport.Status(3)
	if leaderID == 0 || status3.OutboundQueuedMessages != 32 || status3.RejectedMessages == 0 {
		cancel()
		<-result1
		<-result2
		t.Fatalf("healthy voter did not elect a leader while peer3 queue stayed full: node1=%+v node2=%+v peer3=%+v", node1.Status(), node2.Status(), status3)
	}

	command := []byte("majority progress with isolated peer")
	if err := leaderNode.Propose(command); err != nil {
		t.Fatalf("propose with one isolated peer: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		commands := followerNode.AppliedCommands()
		if len(commands) != 0 && string(commands[len(commands)-1]) == string(command) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	commands := followerNode.AppliedCommands()
	if len(commands) == 0 || string(commands[len(commands)-1]) != string(command) {
		t.Fatalf("healthy peer did not apply committed command under peer3 backpressure: %q", commands)
	}

	var fillerCounts [4]atomic.Int32
	regeneratedMessage := make(chan struct{}, 1)
	go func() {
		for message := range transport3.Receive() {
			if message.From != 1 && message.From != 2 {
				continue
			}
			if len(message.Data) == 1 && message.Data[0] == 0xff {
				fillerCounts[message.From].Add(1)
				continue
			}
			if message.From == leaderID && fillerCounts[leaderID].Load() >= 32 {
				select {
				case regeneratedMessage <- struct{}{}:
				default:
				}
			}
		}
	}()
	serve3Result := make(chan error, 1)
	go func() { serve3Result <- transport3.Serve(ctx) }()
	select {
	case <-regeneratedMessage:
		if fillerCounts[leaderID].Load() < 32 {
			t.Fatalf("regenerated Raft message arrived after only %d leader fillers", fillerCounts[leaderID].Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Raft did not regenerate peer traffic after queue pressure cleared; leader=%d fillers=%d", leaderID, fillerCounts[leaderID].Load())
	}
	leaderTransportStatus, err := leaderTransport.Status(3)
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		peerStatus, statusErr := transport3.Status(leaderID)
		if statusErr == nil && peerStatus.Connected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	peerStatus, _ := transport3.Status(leaderID)
	if !peerStatus.Connected {
		t.Fatalf("peer3 did not establish a leader connection: %+v", peerStatus)
	}
	if err := transport3.Send(tcpadapter.PeerMessageDTO{From: 3, To: leaderID, Data: []byte{0xff}}); err != nil {
		t.Fatalf("send malformed peer message: %v", err)
	}
	resetObserved := false
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, statusErr := leaderTransport.Status(3)
		if statusErr == nil && status.RejectedMessages > leaderTransportStatus.RejectedMessages {
			if status.Connected {
				t.Fatalf("malformed peer connection remained connected after reset: %+v", status)
			}
			resetObserved = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !resetObserved {
		status3, _ = leaderTransport.Status(3)
		t.Fatalf("production driver did not reject malformed peer connection: before=%+v after=%+v", leaderTransportStatus, status3)
	}
	reconnected := false
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, statusErr := leaderTransport.Status(3)
		if statusErr == nil && status.Connected {
			reconnected = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !reconnected {
		status3, _ = leaderTransport.Status(3)
		t.Fatalf("peer did not reconnect after malformed frame reset: %+v", status3)
	}
	leaderCommands := leaderNode.AppliedCommands()
	if len(leaderCommands) != len(commands) || string(leaderCommands[len(leaderCommands)-1]) != string(command) {
		t.Fatalf("malformed peer message changed applied command state: %q", leaderCommands)
	}

	cancel()
	for _, result := range []<-chan error{result1, result2, serve3Result} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("peer driver shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Raft peer worker did not stop after cancellation")
		}
	}
	for _, check := range []struct {
		transport *tcpadapter.TCPPeerTransport
		peers     []uint64
	}{
		{transport: transport, peers: []uint64{2, 3}},
		{transport: transport2, peers: []uint64{1, 3}},
	} {
		for _, peerID := range check.peers {
			status, err := check.transport.Status(peerID)
			if err != nil {
				t.Fatal(err)
			}
			if status.OutboundQueuedMessages != 0 || status.OutboundQueuedBytes != 0 {
				t.Fatalf("shutdown retained peer %d queue: %+v", peerID, status)
			}
		}
	}
	if _, ok := <-transport.Receive(); ok {
		t.Fatal("node1 transport Receive remained open after driver shutdown")
	}
	if err := node1.Close(); err != nil {
		t.Fatalf("close node1 Raft node: %v", err)
	}
	if err := node2.Close(); err != nil {
		t.Fatalf("close node2 Raft node: %v", err)
	}
}
func newDriverQueueTransport(t *testing.T) *tcpadapter.TCPPeerTransport {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "node1.example"},
		DNSNames:              []string{"node1.example"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	identity := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
	transport, err := tcpadapter.NewTCPPeerTransport(tcpadapter.PeerTransportConfig{
		NodeID:       1,
		ClusterID:    "driver-test",
		LocalDNSName: "node1.example",
		Listener:     listener,
		ServerTLS:    &tls.Config{Certificates: []tls.Certificate{identity}, ClientCAs: roots},
		ClientTLS:    &tls.Config{Certificates: []tls.Certificate{identity}, RootCAs: roots},
		Peers: map[uint64]tcpadapter.PeerEndpoint{
			2: {Address: "127.0.0.1:10002", DNSName: "node2.example"},
			3: {Address: "127.0.0.1:10003", DNSName: "node3.example"},
		},
	})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = transport.Close()
		_ = listener.Close()
	})
	return transport
}
