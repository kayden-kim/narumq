package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
	tlsadapter "narumq/internal/adapters/tls"
)

type raftPeerConfigFile struct {
	ClusterID     string                   `json:"clusterId"`
	NodeID        uint64                   `json:"nodeId"`
	Directory     string                   `json:"directory"`
	ListenAddress string                   `json:"listenAddress"`
	LocalDNSName  string                   `json:"localDnsName"`
	CertFile      string                   `json:"certFile"`
	KeyFile       string                   `json:"keyFile"`
	CAFile        string                   `json:"caFile"`
	Peers         []raftPeerConfigEndpoint `json:"peers"`
}

type raftPeerConfigEndpoint struct {
	ID      uint64 `json:"id"`
	Address string `json:"address"`
	DNSName string `json:"dnsName"`
}

func loadRaftPeerConfigFile(path string) (raftPeerConfigFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return raftPeerConfigFile{}, fmt.Errorf("read raft peer config file %q: %w", path, err)
	}
	config, err := parseRaftPeerConfig(bytes.NewReader(data))
	if err != nil {
		return raftPeerConfigFile{}, err
	}
	return config, nil
}

func parseRaftPeerConfig(reader io.Reader) (raftPeerConfigFile, error) {
	if reader == nil {
		return raftPeerConfigFile{}, errors.New("raft peer config reader is nil")
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return raftPeerConfigFile{}, fmt.Errorf("read raft peer config: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config raftPeerConfigFile
	if err := decoder.Decode(&config); err != nil {
		return raftPeerConfigFile{}, fmt.Errorf("decode raft peer config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return raftPeerConfigFile{}, errors.New("raft peer config contains trailing JSON value")
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return raftPeerConfigFile{}, fmt.Errorf("check raft peer config JSON keys: %w", err)
	}

	if err := validateRaftPeerConfig(config); err != nil {
		return raftPeerConfigFile{}, err
	}
	return config, nil
}

func validateRaftPeerConfig(config raftPeerConfigFile) error {
	if strings.TrimSpace(config.ClusterID) == "" {
		return errors.New("clusterId is required")
	}
	if config.NodeID < 1 || config.NodeID > 3 {
		return errors.New("nodeId must be one of the fixed voters 1, 2, or 3")
	}
	if strings.TrimSpace(config.Directory) == "" {
		return errors.New("directory is required")
	}
	if strings.TrimSpace(config.ListenAddress) == "" {
		return errors.New("listenAddress is required")
	}
	if strings.TrimSpace(config.LocalDNSName) == "" {
		return errors.New("localDnsName is required")
	}
	if strings.TrimSpace(config.CertFile) == "" || strings.TrimSpace(config.KeyFile) == "" || strings.TrimSpace(config.CAFile) == "" {
		return errors.New("certFile, keyFile, and caFile are required")
	}
	if len(config.Peers) != 2 {
		return errors.New("peers must contain exactly the other two fixed voters")
	}
	seen := make(map[uint64]struct{}, len(config.Peers))
	for _, peer := range config.Peers {
		if peer.ID < 1 || peer.ID > 3 {
			return fmt.Errorf("peer id %d must be one of the fixed voters 1, 2, or 3", peer.ID)
		}
		if peer.ID == config.NodeID {
			return fmt.Errorf("peer id %d must not equal nodeId", peer.ID)
		}
		if _, exists := seen[peer.ID]; exists {
			return fmt.Errorf("duplicate peer id %d", peer.ID)
		}
		seen[peer.ID] = struct{}{}
		if strings.TrimSpace(peer.Address) == "" || strings.TrimSpace(peer.DNSName) == "" {
			return fmt.Errorf("peer %d address and dnsName are required", peer.ID)
		}
	}
	for peerID := uint64(1); peerID <= 3; peerID++ {
		if peerID == config.NodeID {
			continue
		}
		if _, exists := seen[peerID]; !exists {
			return fmt.Errorf("peer id %d is required", peerID)
		}
	}
	return nil
}

func runRaftPeerOnly(ctx context.Context, configPath string, logger *slog.Logger) error {
	config, err := loadRaftPeerConfigFile(configPath)
	if err != nil {
		return fmt.Errorf("load raft peer config: %w", err)
	}
	serverTLS, err := tlsadapter.BuildTLSConfig(tlsadapter.Config{
		CertFile:   config.CertFile,
		KeyFile:    config.KeyFile,
		CAFile:     config.CAFile,
		ClientAuth: tlsadapter.ClientAuthRequire,
		MinVersion: "1.3",
	})
	if err != nil {
		return fmt.Errorf("configure raft peer TLS: %w", err)
	}
	clientTLS := serverTLS.Clone()
	clientTLS.ClientAuth = tls.NoClientCert
	clientTLS.RootCAs = serverTLS.ClientCAs
	clientTLS.ClientCAs = nil
	clientTLS.ServerName = ""
	clientTLS.MinVersion = tls.VersionTLS13
	clientTLS.MaxVersion = tls.VersionTLS13

	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for raft peers on %q: %w", config.ListenAddress, err)
	}
	node, err := raftadapter.Open(raftadapter.Config{
		NodeID:    raftadapter.NodeID(config.NodeID),
		PeerIDs:   []raftadapter.NodeID{1, 2, 3},
		Directory: config.Directory,
	})
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("open raft node: %w", err)
	}

	peers := make(map[uint64]tcpadapter.PeerEndpoint, len(config.Peers))
	for _, peer := range config.Peers {
		peers[peer.ID] = tcpadapter.PeerEndpoint{Address: peer.Address, DNSName: peer.DNSName}
	}
	transport, err := tcpadapter.NewTCPPeerTransport(tcpadapter.PeerTransportConfig{
		NodeID:       config.NodeID,
		ClusterID:    config.ClusterID,
		LocalDNSName: config.LocalDNSName,
		Listener:     listener,
		ServerTLS:    serverTLS,
		ClientTLS:    clientTLS,
		Peers:        peers,
	})
	if err != nil {
		_ = listener.Close()
		_ = node.Close()
		return fmt.Errorf("create raft peer transport: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("Raft peer-only node started", "node_id", config.NodeID, "address", listener.Addr().String())
	driverErr := runRaftPeerDriver(ctx, node, transport, logger)
	closeErr := node.Close()
	if driverErr != nil {
		return driverErr
	}
	if closeErr != nil {
		return fmt.Errorf("close raft node: %w", closeErr)
	}
	logger.Info("Raft peer-only node stopped", "node_id", config.NodeID)
	return nil
}
