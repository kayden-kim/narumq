package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-12
func TestParseRaftPeerConfigStrict(t *testing.T) {
	valid := raftPeerConfigFile{
		ClusterID:     "cluster-a",
		NodeID:        1,
		Directory:     "data/node-1",
		ListenAddress: "127.0.0.1:9001",
		LocalDNSName:  "node1.example",
		CertFile:      "node1.crt",
		KeyFile:       "node1.key",
		CAFile:        "ca.crt",
		Peers: []raftPeerConfigEndpoint{
			{ID: 2, Address: "127.0.0.1:9002", DNSName: "node2.example"},
			{ID: 3, Address: "127.0.0.1:9003", DNSName: "node3.example"},
		},
	}
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := parseRaftPeerConfig(bytes.NewReader(encoded)); err != nil || got.NodeID != valid.NodeID {
		t.Fatalf("parse valid config = (%+v, %v)", got, err)
	}

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "unknown top-level field", input: `{"clusterId":"x","nodeId":1,"directory":"d","listenAddress":"127.0.0.1:1","localDnsName":"node1.example","certFile":"c","keyFile":"k","caFile":"a","peers":[{"id":2,"address":"127.0.0.1:2","dnsName":"node2.example"},{"id":3,"address":"127.0.0.1:3","dnsName":"node3.example"}],"extra":true}`, want: "unknown field"},
		{name: "unknown peer field", input: `{"clusterId":"x","nodeId":1,"directory":"d","listenAddress":"127.0.0.1:1","localDnsName":"node1.example","certFile":"c","keyFile":"k","caFile":"a","peers":[{"id":2,"address":"127.0.0.1:2","dnsName":"node2.example","role":"voter"},{"id":3,"address":"127.0.0.1:3","dnsName":"node3.example"}]}`, want: "unknown field"},
		{name: "trailing JSON", input: string(encoded) + ` {}`, want: "trailing JSON"},
		{name: "duplicate peer id", input: mustMarshalPeerConfig(t, func(config *raftPeerConfigFile) { config.Peers[1].ID = 2 }), want: "duplicate peer id"},
		{name: "local peer id", input: mustMarshalPeerConfig(t, func(config *raftPeerConfigFile) { config.Peers[0].ID = 1 }), want: "must not equal nodeId"},
		{name: "missing fixed peer", input: mustMarshalPeerConfig(t, func(config *raftPeerConfigFile) { config.Peers[1].ID = 1 }), want: "must not equal nodeId"},
		{name: "missing directory", input: mustMarshalPeerConfig(t, func(config *raftPeerConfigFile) { config.Directory = " " }), want: "directory is required"},
		{name: "wrong voter count", input: mustMarshalPeerConfig(t, func(config *raftPeerConfigFile) { config.Peers = config.Peers[:1] }), want: "exactly the other two"},
		{name: "invalid local voter", input: mustMarshalPeerConfig(t, func(config *raftPeerConfigFile) { config.NodeID = 4 }), want: "fixed voters"},
		{name: "duplicate top-level key", input: strings.Replace(string(encoded), `"clusterId":"cluster-a"`, `"clusterId":"cluster-a","clusterId":"cluster-a"`, 1), want: "duplicate JSON key"},
		{name: "duplicate peer key", input: strings.Replace(string(encoded), `"id":2`, `"id":2,"id":2`, 1), want: "duplicate JSON key"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseRaftPeerConfig(strings.NewReader(test.input))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("parse config error = %v, want %q", err, test.want)
			}
		})
	}
}

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-03
func TestRaftPeerOnlyRejectsOtherFlags(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "empty config path", args: []string{"-raft-peer-config="}, want: "non-empty file path"},
		{name: "MQTT listener conflict", args: []string{"-raft-peer-config", "peer.json", "-tcp-listen", "127.0.0.1:0"}, want: "cannot be combined"},
		{name: "management conflict", args: []string{"-raft-peer-config", "peer.json", "-http-listen", "127.0.0.1:0"}, want: "cannot be combined"},
		{name: "security conflict", args: []string{"-raft-peer-config", "peer.json", "-security-config", "security.json"}, want: "cannot be combined"},
		{name: "positional argument", args: []string{"-raft-peer-config", "peer.json", "extra"}, want: "positional arguments"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := runWithContext(context.Background(), test.args, logger)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("runWithContext error = %v, want %q", err, test.want)
			}
		})
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-016
func TestRaftPeerOnlyStartsThreePeersAndStops(t *testing.T) {
	addresses := make(map[uint64]string, 3)
	listeners := make([]net.Listener, 0, 3)
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		addresses[nodeID] = listener.Addr().String()
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}

	root := t.TempDir()
	caPEM, nodeCertFiles := writePeerOnlyCertificates(t, root)
	caPath := filepath.Join(root, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	type process struct {
		cancel   context.CancelFunc
		result   chan error
		logs     *lockedLogBuffer
		finished bool
	}
	processes := make(map[uint64]*process, 3)
	t.Cleanup(func() {
		for _, process := range processes {
			process.cancel()
		}
		for nodeID, process := range processes {
			if process.finished {
				continue
			}
			select {
			case err := <-process.result:
				if err != nil {
					t.Errorf("peer-only node %d cleanup: %v", nodeID, err)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("peer-only node %d did not stop during cleanup", nodeID)
			}
		}
	})

	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		config := raftPeerConfigFile{
			ClusterID:     "cli-peer-cluster",
			NodeID:        nodeID,
			Directory:     filepath.Join(root, fmt.Sprintf("data-node-%d", nodeID)),
			ListenAddress: addresses[nodeID],
			LocalDNSName:  fmt.Sprintf("node%d.example", nodeID),
			CertFile:      nodeCertFiles[nodeID].certPath,
			KeyFile:       nodeCertFiles[nodeID].keyPath,
			CAFile:        caPath,
		}
		for peerID := uint64(1); peerID <= 3; peerID++ {
			if peerID == nodeID {
				continue
			}
			config.Peers = append(config.Peers, raftPeerConfigEndpoint{
				ID:      peerID,
				Address: addresses[peerID],
				DNSName: fmt.Sprintf("node%d.example", peerID),
			})
		}
		configPath := filepath.Join(root, fmt.Sprintf("node-%d.json", nodeID))
		encoded, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		logs := &lockedLogBuffer{}
		result := make(chan error, 1)
		processes[nodeID] = &process{cancel: cancel, result: result, logs: logs}
		logger := slog.New(slog.NewJSONHandler(logs, nil))
		go func(ctx context.Context, args []string, logger *slog.Logger, result chan<- error) {
			result <- runWithContext(ctx, args, logger)
		}(ctx, []string{"-raft-peer-config", configPath}, logger, result)
	}

	deadline := time.Now().Add(10 * time.Second)
	leaderObserved := false
	for time.Now().Before(deadline) {
		for nodeID, process := range processes {
			if strings.Contains(process.logs.String(), `"role":"leader"`) {
				leaderObserved = true
			}
			select {
			case err := <-process.result:
				process.finished = true
				t.Fatalf("peer-only node %d exited before cancellation: %v", nodeID, err)
			default:
			}
		}
		if leaderObserved {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !leaderObserved {
		for _, process := range processes {
			process.cancel()
		}
		t.Fatal("three peer-only processes did not elect a Raft leader")
	}
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		walPath := filepath.Join(root, fmt.Sprintf("data-node-%d", nodeID), "wal.bin")
		info, err := os.Stat(walPath)
		if err != nil || info.Size() == 0 {
			t.Fatalf("node %d WAL not persisted: info=%v err=%v", nodeID, info, err)
		}
		if strings.Contains(processes[nodeID].logs.String(), `"READY"`) {
			t.Fatalf("peer-only node %d advertised readiness", nodeID)
		}
	}

	for _, process := range processes {
		process.cancel()
	}
	for nodeID, process := range processes {
		select {
		case err := <-process.result:
			process.finished = true
			if err != nil {
				t.Fatalf("peer-only node %d shutdown: %v", nodeID, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("peer-only node %d did not stop after cancellation", nodeID)
		}
	}
	for nodeID, address := range addresses {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("node %d left peer listener bound after cancellation: %v", nodeID, err)
		}
		_ = listener.Close()
	}
}

func mustMarshalPeerConfig(t *testing.T, mutate func(*raftPeerConfigFile)) string {
	t.Helper()
	config := raftPeerConfigFile{
		ClusterID:     "cluster-a",
		NodeID:        1,
		Directory:     "data/node-1",
		ListenAddress: "127.0.0.1:9001",
		LocalDNSName:  "node1.example",
		CertFile:      "node1.crt",
		KeyFile:       "node1.key",
		CAFile:        "ca.crt",
		Peers: []raftPeerConfigEndpoint{
			{ID: 2, Address: "127.0.0.1:9002", DNSName: "node2.example"},
			{ID: 3, Address: "127.0.0.1:9003", DNSName: "node3.example"},
		},
	}
	mutate(&config)
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

type peerOnlyCertificateFiles struct {
	certPath string
	keyPath  string
}

func writePeerOnlyCertificates(t *testing.T, directory string) ([]byte, map[uint64]peerOnlyCertificateFiles) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(100),
		Subject:               pkix.Name{CommonName: "NaruMQ CLI test CA"},
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
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	files := make(map[uint64]peerOnlyCertificateFiles, 3)
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		dnsName := fmt.Sprintf("node%d.example", nodeID)
		leafTemplate := &x509.Certificate{
			SerialNumber:          new(big.Int).SetUint64(nodeID),
			Subject:               pkix.Name{CommonName: dnsName},
			DNSNames:              []string{dnsName},
			NotBefore:             now.Add(-time.Minute),
			NotAfter:              now.Add(time.Hour),
			KeyUsage:              x509.KeyUsageDigitalSignature,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			BasicConstraintsValid: true,
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &privateKey.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
		if err != nil {
			t.Fatal(err)
		}
		certPath := filepath.Join(directory, fmt.Sprintf("node-%d.crt", nodeID))
		keyPath := filepath.Join(directory, fmt.Sprintf("node-%d.key", nodeID))
		if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
			t.Fatal(err)
		}
		files[nodeID] = peerOnlyCertificateFiles{certPath: certPath, keyPath: keyPath}
	}
	return caPEM, files
}

type lockedLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (buffer *lockedLogBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Write(data)
}

func (buffer *lockedLogBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}
