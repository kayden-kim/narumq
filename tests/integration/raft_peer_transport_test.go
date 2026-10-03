package integration

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
)

func TestRaftPeerTransportThreeNodeReplicationAndCatchup(t *testing.T) {
	cluster := newLiveRaftPeerCluster(t)
	cluster.start(1)
	cluster.start(2)
	cluster.start(3)
	if err := cluster.nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	leader := cluster.waitForLeader()
	initial := [][]byte{[]byte("committed-before-disconnect")}
	cluster.propose(leader, initial[0])
	cluster.waitForApplied([]raftadapter.NodeID{1, 2, 3}, initial)

	cluster.stop(3)
	leader = cluster.waitForLeader()
	beforeDisconnect := cluster.nodes[3].AppliedCommands()
	deadline := time.Now().Add(3 * time.Second)
	peerDisconnected := false
	for time.Now().Before(deadline) {
		allDisconnected := true
		for nodeID := raftadapter.NodeID(1); nodeID <= 2; nodeID++ {
			status, err := cluster.transports[nodeID].Status(3)
			if err != nil {
				t.Fatal(err)
			}
			allDisconnected = allDisconnected && !status.Connected
		}
		if allDisconnected {
			peerDisconnected = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !peerDisconnected {
		t.Fatal("leader peers did not observe node 3 transport closure")
	}
	for nodeID := raftadapter.NodeID(1); nodeID <= 2; nodeID++ {
		for attempt := range 64 {
			err := cluster.transports[nodeID].Send(tcpadapter.PeerMessageDTO{
				From: uint64(nodeID),
				To:   3,
				Data: []byte{0xff},
			})
			if errors.Is(err, tcpadapter.ErrPeerBackpressure) {
				break
			}
			if err != nil {
				t.Fatalf("node %d peer 3 Send(): %v", nodeID, err)
			}
			if attempt == 63 {
				t.Fatalf("node %d peer 3 queue did not reach backpressure", nodeID)
			}
		}
		status, err := cluster.transports[nodeID].Status(3)
		if err != nil {
			t.Fatal(err)
		}
		if status.OutboundQueuedMessages != 32 || status.OutboundQueuedBytes > 4<<20 || status.RejectedMessages == 0 {
			t.Fatalf("node %d peer 3 queue did not enforce bounded backpressure: %+v", nodeID, status)
		}
	}
	disconnectedCommands := [][]byte{
		[]byte("during-disconnect-1"),
		[]byte("during-disconnect-2"),
		[]byte("during-disconnect-3"),
		[]byte("during-disconnect-4"),
	}
	for _, command := range disconnectedCommands {
		cluster.propose(leader, command)
	}
	allExpected := append(append([][]byte(nil), initial...), disconnectedCommands...)
	if got := cluster.nodes[3].AppliedCommands(); !reflect.DeepEqual(got, beforeDisconnect) {
		t.Fatalf("disconnected node applied commands = %q, want %q", got, beforeDisconnect)
	}
	for nodeID := raftadapter.NodeID(1); nodeID <= 2; nodeID++ {
		status, err := cluster.transports[nodeID].Status(3)
		if err != nil {
			t.Fatal(err)
		}
		if status.OutboundQueuedMessages > 32 || status.OutboundQueuedBytes > 4<<20 {
			t.Fatalf("node %d peer 3 queue exceeded bound: %+v", nodeID, status)
		}
	}

	cluster.restart(3)
	cluster.waitForApplied([]raftadapter.NodeID{1, 2, 3}, allExpected)
}

func TestRaftPeerTransportRejectsUnauthenticatedAndMalformedPeers(t *testing.T) {
	pki := newPeerTransportPKI(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	address := listener.Addr().String()
	node, err := raftadapter.Open(raftadapter.Config{
		NodeID:    3,
		PeerIDs:   []raftadapter.NodeID{1, 2, 3},
		Directory: filepath.Join(t.TempDir(), "node-3"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Errorf("close raft node: %v", err)
		}
	})
	for {
		batch, err := processReadyForTest(t, node)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Messages) == 0 && len(batch.Commands) == 0 {
			break
		}
	}
	baselineStatus := node.Status()
	baselineCommands := node.AppliedCommands()
	transport, err := tcpadapter.NewTCPPeerTransport(peerTransportConfig(3, listener, pki, nil))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() { serveResult <- transport.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := transport.Close(); err != nil {
			t.Errorf("close peer transport: %v", err)
		}
	})

	foreign := newPeerTransportPKI(t)
	if conn, err := dialRawPeer(address, foreign.certificates[2], pki.roots, "node3.example"); err == nil {
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_ = writeFullTest(conn, testPeerHelloBytes("test-cluster", 1, 0, 2))
		from, to, term := uint64(2), uint64(3), uint64(1)
		payload, marshalErr := proto.Marshal(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: &from, To: &to, Term: &term})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		_ = writeFullTest(conn, testPeerFrameBytes(payload))
		waitPeerSocketClosed(t, conn)
		_ = conn.Close()
	}
	assertNoInboundPeerMessage(t, transport.Receive())
	assertNodeUnchanged(t, node, baselineStatus, baselineCommands)

	conn := dialAndReadServerHello(t, address, pki.certificates[2], pki.roots)
	writeTestPeerHello(t, conn, "test-cluster", 1, 0, 1)
	waitPeerSocketClosed(t, conn)
	_ = conn.Close()
	assertNoInboundPeerMessage(t, transport.Receive())
	assertNodeUnchanged(t, node, baselineStatus, baselineCommands)

	conn = dialAndReadServerHello(t, address, pki.certificates[2], pki.roots)
	writeTestPeerHello(t, conn, "wrong-cluster", 1, 0, 2)
	waitPeerSocketClosed(t, conn)
	_ = conn.Close()
	assertNoInboundPeerMessage(t, transport.Receive())
	assertNodeUnchanged(t, node, baselineStatus, baselineCommands)

	conn = dialAndReadServerHello(t, address, pki.certificates[2], pki.roots)
	writeTestPeerHello(t, conn, "test-cluster", 2, 0, 2)
	waitPeerSocketClosed(t, conn)
	_ = conn.Close()
	assertNoInboundPeerMessage(t, transport.Receive())
	assertNodeUnchanged(t, node, baselineStatus, baselineCommands)

	conn = dialAndReadServerHello(t, address, pki.certificates[2], pki.roots)
	writeTestPeerHello(t, conn, "test-cluster", 1, 0, 2)
	messageType := raftpb.MsgHeartbeat
	from, to, term := uint64(1), uint64(3), uint64(7)
	mismatched, err := proto.Marshal(&raftpb.Message{Type: messageType.Enum(), From: &from, To: &to, Term: &term})
	if err != nil {
		t.Fatal(err)
	}
	writeTestPeerFrame(t, conn, mismatched)
	message := receivePeerMessage(t, transport.Receive())
	if _, err := node.Step(raftadapter.MessageDTO{From: raftadapter.NodeID(message.From), To: raftadapter.NodeID(message.To), Data: message.Data}); err == nil {
		t.Fatal("Node.Step accepted a protobuf From value different from authenticated peer identity")
	}
	if err := transport.ResetPeer(message.From); err != nil {
		t.Fatalf("reset peer after identity mismatch: %v", err)
	}
	waitPeerSocketClosed(t, conn)
	_ = conn.Close()
	assertNodeUnchanged(t, node, baselineStatus, baselineCommands)

	conn = dialAndReadServerHello(t, address, pki.certificates[2], pki.roots)
	writeTestPeerHello(t, conn, "test-cluster", 1, 0, 2)
	writeTestPeerFrame(t, conn, []byte{0xff})
	message = receivePeerMessage(t, transport.Receive())
	if _, err := node.Step(raftadapter.MessageDTO{From: raftadapter.NodeID(message.From), To: raftadapter.NodeID(message.To), Data: message.Data}); err == nil {
		t.Fatal("Node.Step accepted malformed Raft protobuf bytes")
	}
	if err := transport.ResetPeer(message.From); err != nil {
		t.Fatalf("reset peer after malformed protobuf: %v", err)
	}
	waitPeerSocketClosed(t, conn)
	_ = conn.Close()
	assertNodeUnchanged(t, node, baselineStatus, baselineCommands)

	conn = dialAndReadServerHello(t, address, pki.certificates[2], pki.roots)
	writeTestPeerHello(t, conn, "test-cluster", 1, 0, 2)
	var oversized [4]byte
	binary.BigEndian.PutUint32(oversized[:], 1<<20+1)
	if err := writeFullTest(conn, oversized[:]); err != nil {
		t.Fatal(err)
	}
	waitPeerSocketClosed(t, conn)
	_ = conn.Close()
	assertNoInboundPeerMessage(t, transport.Receive())
	assertNodeUnchanged(t, node, baselineStatus, baselineCommands)

	conn = dialAndReadServerHello(t, address, pki.certificates[2], pki.roots)
	writeTestPeerHello(t, conn, "test-cluster", 1, 0, 2)
	var truncated [4]byte
	binary.BigEndian.PutUint32(truncated[:], 3)
	if err := writeFullTest(conn, truncated[:]); err != nil {
		t.Fatal(err)
	}
	if err := writeFullTest(conn, []byte{0x08, 0x01}); err != nil {
		t.Fatal(err)
	}
	waitPeerSocketClosed(t, conn)
	_ = conn.Close()
	assertNoInboundPeerMessage(t, transport.Receive())
	assertNodeUnchanged(t, node, baselineStatus, baselineCommands)

	cancel()
	select {
	case err := <-serveResult:
		if err != nil {
			t.Fatalf("Serve after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("transport Serve did not exit after cancellation")
	}
}

type liveRaftPeerCluster struct {
	t          *testing.T
	pki        *peerTransportPKI
	nodes      map[raftadapter.NodeID]*raftadapter.Node
	addresses  map[uint64]string
	transports map[raftadapter.NodeID]*tcpadapter.TCPPeerTransport
	cancels    map[raftadapter.NodeID]context.CancelFunc
	results    map[raftadapter.NodeID]chan error
}

func newLiveRaftPeerCluster(t *testing.T) *liveRaftPeerCluster {
	t.Helper()
	pki := newPeerTransportPKI(t)
	cluster := &liveRaftPeerCluster{
		t:          t,
		pki:        pki,
		nodes:      make(map[raftadapter.NodeID]*raftadapter.Node, 3),
		addresses:  make(map[uint64]string, 3),
		transports: make(map[raftadapter.NodeID]*tcpadapter.TCPPeerTransport, 3),
		cancels:    make(map[raftadapter.NodeID]context.CancelFunc, 3),
		results:    make(map[raftadapter.NodeID]chan error, 3),
	}
	listeners := make(map[uint64]net.Listener, 3)
	t.Cleanup(func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	})
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[nodeID] = listener
		cluster.addresses[nodeID] = listener.Addr().String()
	}
	for nodeID := raftadapter.NodeID(1); nodeID <= 3; nodeID++ {
		node, err := raftadapter.Open(raftadapter.Config{
			NodeID:    nodeID,
			PeerIDs:   []raftadapter.NodeID{1, 2, 3},
			Directory: filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", nodeID)),
		})
		if err != nil {
			t.Fatal(err)
		}
		cluster.nodes[nodeID] = node
		transport, err := tcpadapter.NewTCPPeerTransport(peerTransportConfig(uint64(nodeID), listeners[uint64(nodeID)], pki, cluster.addresses))
		if err != nil {
			t.Fatal(err)
		}
		cluster.transports[nodeID] = transport
	}
	t.Cleanup(cluster.close)
	return cluster
}

func (cluster *liveRaftPeerCluster) start(nodeID raftadapter.NodeID) {
	cluster.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	cluster.cancels[nodeID] = cancel
	cluster.results[nodeID] = result
	transport := cluster.transports[nodeID]
	node := cluster.nodes[nodeID]
	go func() { result <- runIntegrationRaftPeerLoop(ctx, node, transport) }()
}
func runIntegrationRaftPeerLoop(ctx context.Context, node *raftadapter.Node, transport *tcpadapter.TCPPeerTransport) error {
	serveResult := make(chan error, 1)
	go func() { serveResult <- transport.Serve(ctx) }()
	defer transport.Close()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	processReady := func() error {
		for {
			batch, err := node.ProcessReady()
			if err != nil {
				return err
			}
			if err := node.CompleteApply(batch.ApplySequence); err != nil {
				return err
			}
			for _, message := range batch.Messages {
				err := transport.Send(tcpadapter.PeerMessageDTO{
					From: uint64(message.From),
					To:   uint64(message.To),
					Data: message.Data,
				})
				if err != nil && !errors.Is(err, tcpadapter.ErrPeerBackpressure) {
					if errors.Is(err, tcpadapter.ErrPeerTransportClosed) && ctx.Err() != nil {
						return nil
					}
					return err
				}
			}
			if !batch.Progress {
				return nil
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-serveResult:
			if ctx.Err() != nil {
				return nil
			}
			if err == nil {
				return fmt.Errorf("peer transport stopped unexpectedly")
			}
			return err
		case <-node.StorageReady():
			if err := processReady(); err != nil {
				return err
			}
		case message, ok := <-transport.Receive():
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("peer transport receive closed")
			}
			if _, err := node.Step(raftadapter.MessageDTO{
				From: raftadapter.NodeID(message.From),
				To:   raftadapter.NodeID(message.To),
				Data: message.Data,
			}); err != nil && !errors.Is(err, raftadapter.ErrInvalidMessage) && !errors.Is(err, raftadapter.ErrBackpressure) {
				return err
			}
			if err := processReady(); err != nil {
				return err
			}
		case <-ticker.C:
			if err := node.Tick(); err != nil {
				return err
			}
			if err := processReady(); err != nil {
				return err
			}
		}
	}
}

func (cluster *liveRaftPeerCluster) stop(nodeID raftadapter.NodeID) {
	cluster.t.Helper()
	cancel := cluster.cancels[nodeID]
	if cancel == nil {
		return
	}
	cancel()
	select {
	case err := <-cluster.results[nodeID]:
		if err != nil {
			cluster.t.Fatalf("stop node %d driver: %v", nodeID, err)
		}
	case <-time.After(5 * time.Second):
		cluster.t.Fatalf("node %d driver did not stop", nodeID)
	}
	delete(cluster.cancels, nodeID)
	delete(cluster.results, nodeID)
}

func (cluster *liveRaftPeerCluster) restart(nodeID raftadapter.NodeID) {
	cluster.t.Helper()
	listener, err := net.Listen("tcp", cluster.addresses[uint64(nodeID)])
	if err != nil {
		cluster.t.Fatalf("rebind node %d peer listener: %v", nodeID, err)
	}
	transport, err := tcpadapter.NewTCPPeerTransport(peerTransportConfig(uint64(nodeID), listener, cluster.pki, cluster.addresses))
	if err != nil {
		cluster.t.Fatalf("recreate node %d peer transport: %v", nodeID, err)
	}
	cluster.transports[nodeID] = transport
	cluster.start(nodeID)
}

func (cluster *liveRaftPeerCluster) propose(leader raftadapter.NodeID, command []byte) {
	cluster.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := cluster.nodes[leader].Propose(command); err == nil {
			return
		} else if !errors.Is(err, raftadapter.ErrNotLeader) {
			cluster.t.Fatalf("node %d Propose(): %v", leader, err)
		}
		leader = cluster.waitForLeader()
	}
	cluster.t.Fatal("proposal did not reach a leader")
}

func (cluster *liveRaftPeerCluster) waitForLeader() raftadapter.NodeID {
	cluster.t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for nodeID, result := range cluster.results {
			select {
			case err := <-result:
				cluster.t.Fatalf("node %d driver stopped unexpectedly: %v", nodeID, err)
			default:
			}
		}
		for nodeID, node := range cluster.nodes {
			if _, active := cluster.results[nodeID]; !active {
				continue
			}
			if node.Status().Role == raftadapter.RoleLeader {
				return nodeID
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	cluster.t.Fatal("three-node Raft cluster did not elect a leader")
	return 0
}

func (cluster *liveRaftPeerCluster) waitForApplied(nodeIDs []raftadapter.NodeID, want [][]byte) {
	cluster.t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		applied := true
		for _, nodeID := range nodeIDs {
			if !reflect.DeepEqual(cluster.nodes[nodeID].AppliedCommands(), want) {
				applied = false
				break
			}
		}
		if applied {
			return
		}
		for nodeID, result := range cluster.results {
			select {
			case err := <-result:
				cluster.t.Fatalf("node %d driver stopped unexpectedly: %v", nodeID, err)
			default:
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, nodeID := range nodeIDs {
		cluster.t.Logf("node %d applied = %q", nodeID, cluster.nodes[nodeID].AppliedCommands())
	}
	cluster.t.Fatalf("Raft nodes did not apply the same commands: want %q", want)
}

func (cluster *liveRaftPeerCluster) close() {
	cluster.t.Helper()
	for nodeID := range cluster.cancels {
		cluster.stop(nodeID)
	}
	for nodeID, node := range cluster.nodes {
		if err := node.Close(); err != nil {
			cluster.t.Errorf("close node %d WAL: %v", nodeID, err)
		}
	}
}

type peerTransportPKI struct {
	roots         *x509.CertPool
	caCertificate *x509.Certificate
	caKey         *ecdsa.PrivateKey
	certificates  map[uint64]tls.Certificate
}

func newPeerTransportPKI(t *testing.T) *peerTransportPKI {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(100),
		Subject:               pkix.Name{CommonName: "NaruMQ test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(caCert)
	pki := &peerTransportPKI{
		roots:         roots,
		caCertificate: caCert,
		caKey:         caKey,
		certificates:  make(map[uint64]tls.Certificate, 3),
	}
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		pki.certificates[nodeID] = issuePeerCertificate(t, caCert, caKey, nodeID, fmt.Sprintf("node%d.example", nodeID))
	}
	return pki
}

func issuePeerCertificate(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, serial uint64, dnsName string) tls.Certificate {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          new(big.Int).SetUint64(serial),
		Subject:               pkix.Name{CommonName: dnsName},
		DNSNames:              []string{dnsName},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &privateKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	pair.Leaf, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

func peerTransportConfig(nodeID uint64, listener net.Listener, pki *peerTransportPKI, addresses map[uint64]string) tcpadapter.PeerTransportConfig {
	localCertificate := pki.certificates[nodeID]
	localDNS := fmt.Sprintf("node%d.example", nodeID)
	peers := make(map[uint64]tcpadapter.PeerEndpoint, 2)
	for peerID := uint64(1); peerID <= 3; peerID++ {
		if peerID == nodeID {
			continue
		}
		address := "127.0.0.1:1"
		if addresses != nil {
			address = addresses[peerID]
		}
		peers[peerID] = tcpadapter.PeerEndpoint{Address: address, DNSName: fmt.Sprintf("node%d.example", peerID)}
	}
	return tcpadapter.PeerTransportConfig{
		NodeID:       nodeID,
		ClusterID:    "test-cluster",
		LocalDNSName: localDNS,
		Listener:     listener,
		ServerTLS:    &tls.Config{Certificates: []tls.Certificate{localCertificate}, ClientCAs: pki.roots},
		ClientTLS:    &tls.Config{Certificates: []tls.Certificate{localCertificate}, RootCAs: pki.roots},
		Peers:        peers,
	}
}

func dialRawPeer(address string, certificate tls.Certificate, roots *x509.CertPool, serverName string) (*tls.Conn, error) {
	return tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", address, &tls.Config{
		Certificates: []tls.Certificate{certificate},
		RootCAs:      roots,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	})
}

func dialAndReadServerHello(t *testing.T, address string, certificate tls.Certificate, roots *x509.CertPool) *tls.Conn {
	t.Helper()
	connection, err := dialRawPeer(address, certificate, roots, "node3.example")
	if err != nil {
		t.Fatalf("TLS dial peer: %v", err)
	}
	if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	peerID, err := readPeerHelloForTest(connection, "test-cluster")
	if err != nil || peerID != 3 {
		_ = connection.Close()
		t.Fatalf("server hello = (%d, %v), want node 3", peerID, err)
	}
	return connection
}

func writeTestPeerHello(t *testing.T, connection net.Conn, clusterID string, major, minor uint16, nodeID uint64) {
	t.Helper()
	if err := writeFullTest(connection, testPeerHelloBytes(clusterID, major, minor, nodeID)); err != nil {
		t.Fatal(err)
	}
}

func testPeerHelloBytes(clusterID string, major, minor uint16, nodeID uint64) []byte {
	var hello bytes.Buffer
	var fixed [14]byte
	copy(fixed[:8], "NMQRAFT1")
	binary.BigEndian.PutUint16(fixed[8:10], major)
	binary.BigEndian.PutUint16(fixed[10:12], minor)
	binary.BigEndian.PutUint16(fixed[12:14], uint16(len(clusterID)))
	hello.Write(fixed[:])
	hello.WriteString(clusterID)
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], nodeID)
	hello.Write(id[:])
	return hello.Bytes()
}

func readPeerHelloForTest(reader io.Reader, clusterID string) (uint64, error) {
	var fixed [14]byte
	if _, err := io.ReadFull(reader, fixed[:]); err != nil {
		return 0, err
	}
	length := int(binary.BigEndian.Uint16(fixed[12:14]))
	body := make([]byte, length+8)
	if _, err := io.ReadFull(reader, body); err != nil {
		return 0, err
	}
	if string(fixed[:8]) != "NMQRAFT1" || string(body[:length]) != clusterID {
		return 0, fmt.Errorf("invalid test server hello")
	}
	return binary.BigEndian.Uint64(body[length:]), nil
}

func writeTestPeerFrame(t *testing.T, connection net.Conn, payload []byte) {
	t.Helper()
	if err := writeFullTest(connection, testPeerFrameBytes(payload)); err != nil {
		t.Fatal(err)
	}
}

func testPeerFrameBytes(payload []byte) []byte {
	frame := make([]byte, len(payload)+4)
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	return frame
}

func writeFullTest(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func waitPeerSocketClosed(t *testing.T, connection net.Conn) {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var data [1]byte
	if _, err := connection.Read(data[:]); err == nil {
		t.Fatal("peer connection remained open after rejected input")
	}
}

func receivePeerMessage(t *testing.T, messages <-chan tcpadapter.PeerMessageDTO) tcpadapter.PeerMessageDTO {
	t.Helper()
	select {
	case message := <-messages:
		return message
	case <-time.After(2 * time.Second):
		t.Fatal("validly authenticated framed message was not staged")
		return tcpadapter.PeerMessageDTO{}
	}
}

func assertNoInboundPeerMessage(t *testing.T, messages <-chan tcpadapter.PeerMessageDTO) {
	t.Helper()
	select {
	case message := <-messages:
		t.Fatalf("rejected peer input reached Receive: %+v", message)
	case <-time.After(100 * time.Millisecond):
	}
}

func assertNodeUnchanged(t *testing.T, node *raftadapter.Node, status raftadapter.StatusDTO, commands [][]byte) {
	t.Helper()
	if got := node.Status(); !reflect.DeepEqual(got, status) {
		t.Fatalf("rejected peer input changed Raft status: got %+v, want %+v", got, status)
	}
	if got := node.AppliedCommands(); !reflect.DeepEqual(got, commands) {
		t.Fatalf("rejected peer input changed applied commands: got %q, want %q", got, commands)
	}
}
