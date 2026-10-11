package tcp

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// BootstrapProbeStatusDTO is a peer's answer to an empty-directory bootstrap probe.
type BootstrapProbeStatusDTO uint8

const (
	BootstrapProbeEmpty      BootstrapProbeStatusDTO = 1
	BootstrapProbeReady      BootstrapProbeStatusDTO = 2
	BootstrapProbeProgressed BootstrapProbeStatusDTO = 3
)

// BootstrapProbeResultDTO reports the peer's current bootstrap boundary.
type BootstrapProbeResultDTO struct {
	Status            BootstrapProbeStatusDTO
	Term, CommitIndex uint64
}

// BootstrapProgressDTO contains the running node's Raft progress.
type BootstrapProgressDTO struct{ Term, CommitIndex uint64 }

// BootstrapProgressHandler reads the running node's current Raft progress.
type BootstrapProgressHandler func() BootstrapProgressDTO

func encodeBootstrapProbeRequest(clusterID string, memberID uint64) []byte {
	payload := make([]byte, 0, 22+len(clusterID))
	payload = append(payload, dynamicBootstrapProbeMagic...)
	payload = appendUint16(payload, dynamicProtocolMajor)
	payload = appendUint16(payload, dynamicProtocolMinor)
	payload = appendString16(payload, clusterID)
	return binary.BigEndian.AppendUint64(payload, memberID)
}

func decodeBootstrapProbeRequest(payload []byte) (string, uint64, error) {
	reader := dynamicWireReader{data: payload}
	magic, magicOK := reader.fixedString(8)
	major, majorOK := reader.uint16()
	minor, minorOK := reader.uint16()
	clusterID, clusterOK := reader.string16(200, false)
	memberID, memberOK := reader.uint64()
	if !magicOK || magic != dynamicBootstrapProbeMagic || !majorOK || major != dynamicProtocolMajor || !minorOK || minor != dynamicProtocolMinor || !clusterOK || !memberOK || memberID == 0 || reader.position != len(payload) || len(payload) > dynamicHelloLimit {
		return "", 0, ErrInvalidPeerMessage
	}
	return clusterID, memberID, nil
}

func encodeBootstrapProbeResponse(result BootstrapProbeResultDTO) []byte {
	payload := make([]byte, 0, 29)
	payload = append(payload, dynamicBootstrapProbeMagic...)
	payload = appendUint16(payload, dynamicProtocolMajor)
	payload = appendUint16(payload, dynamicProtocolMinor)
	payload = append(payload, byte(result.Status))
	payload = binary.BigEndian.AppendUint64(payload, result.Term)
	return binary.BigEndian.AppendUint64(payload, result.CommitIndex)
}

func decodeBootstrapProbeResponse(payload []byte) (BootstrapProbeResultDTO, error) {
	reader := dynamicWireReader{data: payload}
	magic, magicOK := reader.fixedString(8)
	major, majorOK := reader.uint16()
	minor, minorOK := reader.uint16()
	status, statusOK := reader.byte()
	term, termOK := reader.uint64()
	commit, commitOK := reader.uint64()
	if len(payload) != 29 || !magicOK || magic != dynamicBootstrapProbeMagic || !majorOK || major != dynamicProtocolMajor || !minorOK || minor != dynamicProtocolMinor || !statusOK || status > byte(BootstrapProbeProgressed) || !termOK || !commitOK || (status <= byte(BootstrapProbeEmpty) && (term != 0 || commit != 0)) {
		return BootstrapProbeResultDTO{}, errors.New("bootstrap probe response is invalid")
	}
	if status == 0 {
		return BootstrapProbeResultDTO{}, errors.New("bootstrap probe rejected")
	}
	return BootstrapProbeResultDTO{Status: BootstrapProbeStatusDTO(status), Term: term, CommitIndex: commit}, nil
}

// ServeBootstrapProbe answers configured peers until canceled, leaving the listener open.
func ServeBootstrapProbe(ctx context.Context, listener net.Listener, serverTLS *tls.Config, local PeerIdentityDTO, peers []PeerIdentityDTO) error {
	deadlineListener, ok := listener.(interface{ SetDeadline(time.Time) error })
	if !ok {
		return fmt.Errorf("bootstrap probe listener must support deadlines")
	}
	cfg, err := validatedServerTLS(serverTLS, local.DNSSAN)
	if err != nil {
		return err
	}
	if err := validateDynamicCertificate(cfg.Certificates[0], local); err != nil {
		return err
	}
	if err := validateDynamicPeerSet(local, peers); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	var handlers sync.WaitGroup
	stopped := make(chan struct{})
	context.AfterFunc(runCtx, func() {
		_ = deadlineListener.SetDeadline(time.Now())
		mu.Lock()
		for connection := range connections {
			_ = connection.Close()
		}
		mu.Unlock()
		close(stopped)
	})
	defer func() {
		cancel()
		<-stopped
		handlers.Wait()
		_ = deadlineListener.SetDeadline(time.Time{})
	}()
	for {
		socket, err := listener.Accept()
		if err != nil {
			if runCtx.Err() != nil {
				return nil
			}
			return err
		}
		mu.Lock()
		if runCtx.Err() != nil {
			mu.Unlock()
			_ = socket.Close()
			return nil
		}
		connections[socket] = struct{}{}
		mu.Unlock()
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			defer func() {
				_ = socket.Close()
				mu.Lock()
				delete(connections, socket)
				mu.Unlock()
			}()
			connection := tls.Server(socket, cfg)
			if setConnectionDeadline(connection) != nil {
				return
			}
			handshakeCtx, stop := context.WithTimeout(runCtx, peerIOTimeout)
			err := connection.HandshakeContext(handshakeCtx)
			stop()
			if err != nil {
				return
			}
			payload, err := readDynamicHandshakeFrame(connection)
			if err != nil || string(payload[:8]) != dynamicBootstrapProbeMagic {
				return
			}
			result := BootstrapProbeResultDTO{}
			clusterID, memberID, err := decodeBootstrapProbeRequest(payload)
			if err == nil && clusterID == local.ClusterID && memberID != local.MemberID {
				for _, peer := range peers {
					if peer.MemberID == memberID && matchesPeerCertificate(connection, peer) {
						result.Status = BootstrapProbeEmpty
						break
					}
				}
			}
			_ = writeDynamicFrame(connection, encodeBootstrapProbeResponse(result), dynamicHelloLimit)
		}()
	}
}

// ProbeBootstrapPeer authenticates one configured peer and reads its bootstrap status.
func ProbeBootstrapPeer(ctx context.Context, clientTLS *tls.Config, local, peer PeerIdentityDTO) (BootstrapProbeResultDTO, error) {
	cfg, err := validatedClientTLS(clientTLS, local.DNSSAN)
	if err != nil {
		return BootstrapProbeResultDTO{}, err
	}
	cfg.ServerName = peer.DNSSAN
	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: peerIOTimeout}, Config: cfg}
	socket, err := dialer.DialContext(ctx, "tcp", peer.Address)
	if err != nil {
		var opErr *net.OpError
		if errors.As(err, &opErr) && opErr.Op == "dial" {
			return BootstrapProbeResultDTO{}, fmt.Errorf("bootstrap probe connection failed: %w", err)
		}
		return BootstrapProbeResultDTO{}, fmt.Errorf("bootstrap probe TLS handshake failed: %w", err)
	}
	defer socket.Close()
	connection := socket.(*tls.Conn)
	if err := setConnectionDeadline(connection); err != nil {
		return BootstrapProbeResultDTO{}, err
	}
	handshakeCtx, cancel := context.WithTimeout(ctx, peerIOTimeout)
	err = connection.HandshakeContext(handshakeCtx)
	cancel()
	if err != nil {
		return BootstrapProbeResultDTO{}, fmt.Errorf("bootstrap probe TLS handshake failed: %w", err)
	}
	if !matchesPeerCertificate(connection, peer) {
		return BootstrapProbeResultDTO{}, fmt.Errorf("bootstrap probe peer identity mismatch")
	}
	if err := writeDynamicFrame(connection, encodeBootstrapProbeRequest(local.ClusterID, local.MemberID), dynamicHelloLimit); err != nil {
		return BootstrapProbeResultDTO{}, err
	}
	payload, err := readDynamicFrame(connection, dynamicHelloLimit)
	if err != nil {
		return BootstrapProbeResultDTO{}, err
	}
	return decodeBootstrapProbeResponseAndWaitForClose(payload, connection)
}

func decodeBootstrapProbeResponseAndWaitForClose(payload []byte, connection io.Reader) (BootstrapProbeResultDTO, error) {
	result, err := decodeBootstrapProbeResponse(payload)
	if err != nil {
		return BootstrapProbeResultDTO{}, err
	}
	var trailing [1]byte
	for {
		n, err := connection.Read(trailing[:])
		if n > 0 {
			return BootstrapProbeResultDTO{}, fmt.Errorf("bootstrap probe response has trailing data")
		}
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return BootstrapProbeResultDTO{}, fmt.Errorf("wait for bootstrap probe close: %w", err)
		}
	}
}

func (transport *DynamicTCPPeerTransport) handleBootstrapProbe(connection *tls.Conn, payload []byte) {
	result := BootstrapProbeResultDTO{}
	clusterID, memberID, err := decodeBootstrapProbeRequest(payload)
	transport.mu.Lock()
	localID, localClusterID := transport.localID, transport.clusterID
	peer := transport.peers[memberID]
	handler := transport.bootstrapProgress
	transport.mu.Unlock()
	if err == nil && clusterID == localClusterID && memberID != localID && peer != nil && handler != nil {
		identity, _, stopped := peer.identitySnapshot()
		if !stopped && matchesPeerCertificate(connection, identity) {
			progress := handler()
			transport.bootstrapMu.Lock()
			_, allowed := transport.bootstrapAllowed[memberID]
			transport.bootstrapMu.Unlock()
			result = BootstrapProbeResultDTO{Status: BootstrapProbeProgressed, Term: progress.Term, CommitIndex: progress.CommitIndex}
			if progress.Term <= 1 && progress.CommitIndex <= 3 || allowed {
				result.Status = BootstrapProbeReady
			}
		}
	}
	_ = writeDynamicFrame(connection, encodeBootstrapProbeResponse(result), dynamicHelloLimit)
}

func (transport *DynamicTCPPeerTransport) clearBootstrapAllowance(memberID uint64) {
	transport.bootstrapMu.Lock()
	delete(transport.bootstrapAllowed, memberID)
	transport.bootstrapMu.Unlock()
}
