package tcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	cptWaitTimeout = 5 * time.Second
	cptCluster     = "test-cluster"
)

var cptErrAccept = errors.New("cpt injected accept failure")

// cptConn wraps a pipe endpoint with an injectable write failure and reports
// reads on a locally closed pipe as net.ErrClosed, matching TCP sockets.
type cptConn struct {
	net.Conn
	writeErr    error
	writeCalled *atomic.Bool
}

func (connection *cptConn) Write(data []byte) (int, error) {
	if connection.writeCalled != nil {
		connection.writeCalled.Store(true)
	}
	if connection.writeErr != nil {
		return 0, connection.writeErr
	}
	return connection.Conn.Write(data)
}

func (connection *cptConn) Read(data []byte) (int, error) {
	count, err := connection.Conn.Read(data)
	if errors.Is(err, io.ErrClosedPipe) {
		err = net.ErrClosed
	}
	return count, err
}

// cptFailingListener fails every Accept with a non-closure error.
type cptFailingListener struct{}

func (cptFailingListener) Accept() (net.Conn, error) { return nil, cptErrAccept }
func (cptFailingListener) Close() error              { return nil }
func (cptFailingListener) Addr() net.Addr            { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

type cptNode struct {
	dnsName   string
	serverTLS *tls.Config
	clientTLS *tls.Config
}

func cptNewNode(t *testing.T, dnsName string) *cptNode {
	t.Helper()
	serverTLS, clientTLS := testPeerTLSConfigs(t, dnsName)
	return &cptNode{dnsName: dnsName, serverTLS: serverTLS, clientTLS: clientTLS}
}

func (node *cptNode) leaf() *x509.Certificate {
	return node.serverTLS.Certificates[0].Leaf
}

func (node *cptNode) identity(memberID uint64, address string, phase PeerPhaseDTO, operationID string) PeerIdentityDTO {
	return identityForTestCertificate(memberID, address, node.dnsName, phase, operationID, "1.0.0", node.serverTLS.Certificates[0])
}

func (node *cptNode) dialConfig(serverName string) *tls.Config {
	config := node.clientTLS.Clone()
	config.ServerName = serverName
	return config
}

func (node *cptNode) newDynamic(
	t *testing.T,
	listener net.Listener,
	local PeerIdentityDTO,
	peers []PeerIdentityDTO,
	handler SeedAdmissionHandler,
) *DynamicTCPPeerTransport {
	t.Helper()
	if handler == nil {
		handler = cptRejectAdmission
	}
	transport, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local, Listener: listener, ServerTLS: node.serverTLS, ClientTLS: node.clientTLS,
		Peers: peers, AdmissionHandler: handler,
	})
	if err != nil {
		t.Fatalf("create dynamic peer transport: %v", err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	return transport
}

func (node *cptNode) newFixed(t *testing.T, nodeID uint64, listener net.Listener, peers map[uint64]PeerEndpoint) *TCPPeerTransport {
	t.Helper()
	transport, err := NewTCPPeerTransport(PeerTransportConfig{
		NodeID: nodeID, ClusterID: cptCluster, LocalDNSName: node.dnsName, Listener: listener,
		ServerTLS: node.serverTLS, ClientTLS: node.clientTLS, Peers: peers,
	})
	if err != nil {
		t.Fatalf("create fixed peer transport: %v", err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	return transport
}

func cptTrustEachOther(left, right *cptNode) {
	left.serverTLS.ClientCAs.AddCert(right.leaf())
	left.clientTLS.RootCAs.AddCert(right.leaf())
	right.serverTLS.ClientCAs.AddCert(left.leaf())
	right.clientTLS.RootCAs.AddCert(left.leaf())
}

func cptRejectAdmission(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
	return SeedAdmissionResponseDTO{}, errors.New("cpt: not a seed")
}

func cptListen(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func cptServe(t *testing.T, serve func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-result:
			if err != nil {
				t.Errorf("Serve after cancellation: %v", err)
			}
		case <-time.After(cptWaitTimeout):
			t.Error("Serve did not stop after cancellation")
		}
	})
}

func cptWaitUntil(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(cptWaitTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func cptRun(run func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		run()
	}()
	return done
}

func cptWaitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(cptWaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func cptAssertClosed(t *testing.T, connection net.Conn) {
	t.Helper()
	_ = connection.SetReadDeadline(time.Now().Add(cptWaitTimeout))
	var probe [1]byte
	_, err := connection.Read(probe[:])
	if err == nil {
		t.Fatal("connection delivered data, want it closed")
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatal("connection remained open")
	}
}

// cptFeed returns a pipe endpoint that yields data followed by EOF.
func cptFeed(t *testing.T, data []byte) net.Conn {
	t.Helper()
	reader, writer := net.Pipe()
	go func() {
		_, _ = writer.Write(data)
		_ = writer.Close()
	}()
	t.Cleanup(func() { _ = reader.Close() })
	return reader
}

func cptFramePrefix(length uint32) []byte {
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], length)
	return prefix[:]
}

func cptFingerprint(value byte) [sha256.Size]byte {
	var fingerprint [sha256.Size]byte
	for index := range fingerprint {
		fingerprint[index] = value
	}
	return fingerprint
}

func cptVoters() []PeerIdentityDTO {
	voters := make([]PeerIdentityDTO, 0, 3)
	for id := uint64(1); id <= 3; id++ {
		digit := string(rune('0' + id))
		voters = append(voters, dynamicTestIdentity(id, "127.0.0.1:1900"+digit, "node"+digit+".example",
			PeerPhaseVoter, "", byte(0x10+id)))
	}
	return voters
}

func cptDynamicPeer(transport *DynamicTCPPeerTransport, peerID uint64) *dynamicPeerState {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.peers[peerID]
}

func cptDynamicStatus(peer *dynamicPeerState) PeerTransportStatusDTO {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return PeerTransportStatusDTO{
		PeerID: peer.id, Connected: peer.connection != nil,
		OutboundQueuedMessages: peer.outMessages, OutboundQueuedBytes: peer.outBytes,
		InboundQueuedMessages: peer.inMessages, InboundQueuedBytes: peer.inBytes,
		RejectedMessages: peer.rejected, LastError: peer.lastError,
	}
}

func cptMinimalDynamic(local, remote PeerIdentityDTO) (*DynamicTCPPeerTransport, *dynamicPeerState) {
	transport := &DynamicTCPPeerTransport{
		local: local, localID: local.MemberID,
		peers:        make(map[uint64]*dynamicPeerState),
		inboundReady: make(chan struct{}, 1),
	}
	peer := newDynamicPeerState(transport, remote)
	transport.peers[remote.MemberID] = peer
	return transport, peer
}

// cptHelloBytes encodes a v2 Raft hello without validating the identity.
func cptHelloBytes(identity PeerIdentityDTO) []byte {
	buffer := []byte("NMQRAFT2")
	buffer = appendUint16(buffer, dynamicProtocolMajor)
	buffer = appendUint16(buffer, dynamicProtocolMinor)
	buffer = appendString16(buffer, identity.ClusterID)
	buffer = appendUint64(buffer, identity.MemberID)
	buffer = appendString16(buffer, identity.Address)
	buffer = appendString16(buffer, identity.DNSSAN)
	buffer = append(buffer, identity.CertificateSHA256[:]...)
	buffer = appendString16(buffer, identity.ReleaseVersion)
	buffer = appendUint16(buffer, identity.ClusterProtocolMajor)
	buffer = appendUint16(buffer, identity.ClusterProtocolMinor)
	buffer = appendUint16(buffer, identity.StateSchemaMajor)
	buffer = appendUint16(buffer, identity.StateSchemaMinor)
	buffer = append(buffer, byte(identity.Phase))
	return append(buffer, identity.OperationID...)
}

// cptAdmissionRequestBytes encodes a seed admission request without validation.
func cptAdmissionRequestBytes(candidate PeerIdentityDTO) []byte {
	buffer := []byte("NMQJOIN2")
	buffer = appendUint16(buffer, dynamicProtocolMajor)
	buffer = appendUint16(buffer, dynamicProtocolMinor)
	buffer = appendString16(buffer, candidate.ClusterID)
	buffer = appendUint64(buffer, candidate.MemberID)
	buffer = appendString16(buffer, candidate.Address)
	buffer = appendString16(buffer, candidate.DNSSAN)
	buffer = append(buffer, candidate.CertificateSHA256[:]...)
	buffer = appendString16(buffer, candidate.ReleaseVersion)
	buffer = appendUint16(buffer, candidate.StateSchemaMajor)
	return appendUint16(buffer, candidate.StateSchemaMinor)
}

// cptAdmissionResponseBytes encodes an accepted admission response without validation.
func cptAdmissionResponseBytes(candidate PeerIdentityDTO, voters []PeerIdentityDTO) []byte {
	buffer := []byte("NMQJOIN2")
	buffer = appendUint16(buffer, dynamicProtocolMajor)
	buffer = appendUint16(buffer, dynamicProtocolMinor)
	buffer = append(buffer, 1)
	buffer = appendString16(buffer, candidate.ClusterID)
	buffer = appendString16(buffer, candidate.OperationID)
	buffer = appendUint64(buffer, candidate.MemberID)
	buffer = append(buffer, candidate.CertificateSHA256[:]...)
	buffer = appendString16(buffer, candidate.DNSSAN)
	buffer = append(buffer, byte(candidate.Phase))
	buffer = appendUint16(buffer, candidate.StateSchemaMajor)
	buffer = appendUint16(buffer, candidate.StateSchemaMinor)
	buffer = appendUint16(buffer, uint32(len(voters)))
	for _, voter := range voters {
		buffer = appendUint64(buffer, voter.MemberID)
		buffer = appendString16(buffer, voter.Address)
		buffer = appendString16(buffer, voter.DNSSAN)
		buffer = append(buffer, voter.CertificateSHA256[:]...)
		buffer = append(buffer, byte(voter.Phase))
	}
	return buffer
}

func TestCptDynamicPeerHelloDecodeRejectsTruncatedAndInvalidFields(t *testing.T) {
	valid := dynamicTestIdentity(1, "n:1", "n", PeerPhaseLearner, testOperationID, 0x11)
	encoded, err := encodeDynamicPeerHello(valid, dynamicRaftMagic)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cptHelloBytes(valid), encoded) {
		t.Fatalf("test hello encoder differs from production encoder\n got: %x\nwant: %x", cptHelloBytes(valid), encoded)
	}
	for length := 0; length < len(encoded); length++ {
		if _, err := decodeDynamicPeerHello(encoded[:length], dynamicRaftMagic); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Fatalf("hello truncated to %d of %d bytes: err=%v", length, len(encoded), err)
		}
	}
	mutate := func(change func(*PeerIdentityDTO)) []byte {
		identity := valid
		change(&identity)
		return cptHelloBytes(identity)
	}
	cases := map[string][]byte{
		"oversized":                    append(bytes.Clone(encoded), make([]byte, dynamicHelloLimit)...),
		"cluster ID is not UTF-8":      mutate(func(identity *PeerIdentityDTO) { identity.ClusterID = "\xff" }),
		"DNS SAN is not ASCII":         mutate(func(identity *PeerIdentityDTO) { identity.DNSSAN = "n\u00e9" }),
		"empty release":                mutate(func(identity *PeerIdentityDTO) { identity.ReleaseVersion = "" }),
		"unsupported cluster protocol": mutate(func(identity *PeerIdentityDTO) { identity.ClusterProtocolMajor = 3 }),
		"operation ID is not ASCII": mutate(func(identity *PeerIdentityDTO) {
			identity.OperationID = strings.Repeat("a", 34) + "\u00e9"
		}),
		"operation ID has no separators": mutate(func(identity *PeerIdentityDTO) { identity.OperationID = strings.Repeat("a", 36) }),
		"operation ID is short":          mutate(func(identity *PeerIdentityDTO) { identity.OperationID = "abc" }),
		"retired phase": mutate(func(identity *PeerIdentityDTO) {
			identity.Phase = PeerPhaseRetired
			identity.OperationID = ""
		}),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if identity, err := decodeDynamicPeerHello(payload, dynamicRaftMagic); !errors.Is(err, ErrInvalidPeerMessage) {
				t.Fatalf("invalid hello decoded: identity=%+v err=%v", identity, err)
			}
		})
	}
}

func TestCptSeedAdmissionRequestCodecRejectsInvalidCandidates(t *testing.T) {
	valid := dynamicTestIdentity(4, "n:4", "n", PeerPhaseStaged, "", 0x44)
	encoded, err := encodeSeedAdmissionRequest(SeedAdmissionRequestDTO{Candidate: valid})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cptAdmissionRequestBytes(valid), encoded) {
		t.Fatalf("test request encoder differs from production encoder\n got: %x\nwant: %x", cptAdmissionRequestBytes(valid), encoded)
	}
	decoded, err := decodeSeedAdmissionRequest(encoded)
	if err != nil || !samePeerIdentity(decoded.Candidate, valid) {
		t.Fatalf("round trip admission request: candidate=%+v err=%v", decoded.Candidate, err)
	}
	for length := 0; length < len(encoded); length++ {
		if _, err := decodeSeedAdmissionRequest(encoded[:length]); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Fatalf("request truncated to %d of %d bytes: err=%v", length, len(encoded), err)
		}
	}
	mutate := func(change func(*PeerIdentityDTO)) []byte {
		candidate := valid
		change(&candidate)
		return cptAdmissionRequestBytes(candidate)
	}
	cases := map[string][]byte{
		"oversized":               append(bytes.Clone(encoded), make([]byte, dynamicAdmissionLimit)...),
		"cluster ID is not UTF-8": mutate(func(candidate *PeerIdentityDTO) { candidate.ClusterID = "\xff" }),
		"DNS SAN is not ASCII":    mutate(func(candidate *PeerIdentityDTO) { candidate.DNSSAN = "n\u00e9" }),
		"release too long":        mutate(func(candidate *PeerIdentityDTO) { candidate.ReleaseVersion = strings.Repeat("r", 65) }),
		"address without port":    mutate(func(candidate *PeerIdentityDTO) { candidate.Address = "bad" }),
		"unsupported schema":      mutate(func(candidate *PeerIdentityDTO) { candidate.StateSchemaMajor = 2 }),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if request, err := decodeSeedAdmissionRequest(payload); !errors.Is(err, ErrInvalidPeerMessage) {
				t.Fatalf("invalid request decoded: candidate=%+v err=%v", request.Candidate, err)
			}
		})
	}
	for name, candidate := range map[string]PeerIdentityDTO{
		"committed voter":           dynamicTestIdentity(4, "n:4", "n", PeerPhaseVoter, "", 0x44),
		"staged with bad address":   dynamicTestIdentity(4, "n", "n", PeerPhaseStaged, "", 0x44),
		"staged with bad operation": dynamicTestIdentity(4, "n:4", "n", PeerPhaseStaged, "op", 0x44),
	} {
		if _, err := encodeSeedAdmissionRequest(SeedAdmissionRequestDTO{Candidate: candidate}); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Fatalf("encode %s request: err=%v, want ErrInvalidPeerMessage", name, err)
		}
	}
}

func TestCptSeedAdmissionResponseCodecRejectsInvalidResponses(t *testing.T) {
	candidate := dynamicTestIdentity(4, "n:4", "node4.example", PeerPhaseLearner, testOperationID, 0x44)
	voters := cptVoters()
	encoded, err := encodeSeedAdmissionResponse(SeedAdmissionResponseDTO{
		Accepted: true, Candidate: candidate, CurrentVoters: []PeerIdentityDTO{voters[2], voters[0], voters[1]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(cptAdmissionResponseBytes(candidate, voters), encoded) {
		t.Fatalf("test response encoder differs from production encoder\n got: %x\nwant: %x",
			cptAdmissionResponseBytes(candidate, voters), encoded)
	}
	for length := 0; length < len(encoded); length++ {
		if _, err := decodeSeedAdmissionResponse(encoded[:length]); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Fatalf("response truncated to %d of %d bytes: err=%v", length, len(encoded), err)
		}
	}
	withCandidate := func(change func(*PeerIdentityDTO)) []byte {
		mutated := candidate
		change(&mutated)
		return cptAdmissionResponseBytes(mutated, voters)
	}
	withVoters := func(change func([]PeerIdentityDTO) []PeerIdentityDTO) []byte {
		return cptAdmissionResponseBytes(candidate, change(append([]PeerIdentityDTO(nil), voters...)))
	}
	unknownStatus := bytes.Clone(encoded)
	unknownStatus[12] = 2
	cases := map[string][]byte{
		"rejection with trailing byte": append(rejectedSeedAdmissionResponse(), 0),
		"unknown status":               unknownStatus,
		"oversized":                    append(bytes.Clone(encoded), make([]byte, dynamicAdmissionLimit)...),
		"short operation ID":           withCandidate(func(identity *PeerIdentityDTO) { identity.OperationID = "abcde" }),
		"leaving candidate": withCandidate(func(identity *PeerIdentityDTO) {
			identity.Phase = PeerPhaseLeaving
		}),
		"staged candidate without operation": withCandidate(func(identity *PeerIdentityDTO) {
			identity.Phase = PeerPhaseStaged
			identity.OperationID = ""
		}),
		"candidate DNS SAN is not ASCII": withCandidate(func(identity *PeerIdentityDTO) { identity.DNSSAN = "n\u00e9" }),
		"two voters": withVoters(func(list []PeerIdentityDTO) []PeerIdentityDTO {
			return list[:2]
		}),
		"voters out of order": withVoters(func(list []PeerIdentityDTO) []PeerIdentityDTO {
			list[0], list[1] = list[1], list[0]
			return list
		}),
		"zero voter ID": withVoters(func(list []PeerIdentityDTO) []PeerIdentityDTO {
			list[0].MemberID = 0
			return list
		}),
		"learner voter": withVoters(func(list []PeerIdentityDTO) []PeerIdentityDTO {
			list[1].Phase = PeerPhaseLearner
			return list
		}),
		"voter DNS SAN is not ASCII": withVoters(func(list []PeerIdentityDTO) []PeerIdentityDTO {
			list[2].DNSSAN = "n\u00e9"
			return list
		}),
		"trailing byte": append(bytes.Clone(encoded), 0),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if response, err := decodeSeedAdmissionResponse(payload); !errors.Is(err, ErrInvalidPeerMessage) {
				t.Fatalf("invalid response decoded: response=%+v err=%v", response, err)
			}
		})
	}

	rejected, err := encodeSeedAdmissionResponse(SeedAdmissionResponseDTO{Candidate: PeerIdentityDTO{MemberID: 99}})
	if err != nil || !bytes.Equal(rejected, rejectedSeedAdmissionResponse()) {
		t.Fatalf("unaccepted response encoding = %x err=%v, want generic rejection", rejected, err)
	}
	decoded, err := decodeSeedAdmissionResponse(rejected)
	if err != nil || decoded.Accepted || len(decoded.CurrentVoters) != 0 {
		t.Fatalf("decode generic rejection: response=%+v err=%v", decoded, err)
	}
	if _, err := encodeSeedAdmissionResponse(SeedAdmissionResponseDTO{
		Accepted: true, Candidate: candidate, CurrentVoters: voters[:2],
	}); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("accepted response with two voters encoded: err=%v", err)
	}
}

func TestCptAdmissionResultValidation(t *testing.T) {
	requested := dynamicTestIdentity(4, "n:4", "node4.example", PeerPhaseStaged, "", 0x44)
	requested.ReleaseVersion = "1.1.0"
	committed := requested
	committed.OperationID = testOperationID
	valid := SeedAdmissionResponseDTO{Accepted: true, Candidate: committed, CurrentVoters: cptVoters()}
	if err := validateAdmissionResult(requested, valid); err != nil {
		t.Fatalf("valid admission result rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*SeedAdmissionResponseDTO)
	}{
		{name: "not accepted", mutate: func(response *SeedAdmissionResponseDTO) { response.Accepted = false }},
		{name: "different candidate certificate", mutate: func(response *SeedAdmissionResponseDTO) {
			response.Candidate.CertificateSHA256 = cptFingerprint(0x99)
		}},
		{name: "voter candidate keeps operation", mutate: func(response *SeedAdmissionResponseDTO) {
			response.Candidate.Phase = PeerPhaseVoter
		}},
		{name: "two voters", mutate: func(response *SeedAdmissionResponseDTO) {
			response.CurrentVoters = response.CurrentVoters[:2]
		}},
		{name: "voter cluster differs", mutate: func(response *SeedAdmissionResponseDTO) {
			response.CurrentVoters[0].ClusterID = "other"
		}},
		{name: "voter release differs", mutate: func(response *SeedAdmissionResponseDTO) {
			response.CurrentVoters[1].ReleaseVersion = "1.3.0"
		}},
		{name: "voter schema differs", mutate: func(response *SeedAdmissionResponseDTO) {
			response.CurrentVoters[2].StateSchemaMinor = 1
		}},
		{name: "learner voter", mutate: func(response *SeedAdmissionResponseDTO) {
			response.CurrentVoters[0].Phase = PeerPhaseLearner
			response.CurrentVoters[0].OperationID = testOperationID
		}},
		{name: "voter with operation", mutate: func(response *SeedAdmissionResponseDTO) {
			response.CurrentVoters[0].OperationID = testOperationID
		}},
		{name: "candidate listed as voter", mutate: func(response *SeedAdmissionResponseDTO) {
			response.CurrentVoters[2].MemberID = requested.MemberID
		}},
		{name: "voter address invalid", mutate: func(response *SeedAdmissionResponseDTO) {
			response.CurrentVoters[1].Address = "bad"
		}},
		{name: "duplicate voter", mutate: func(response *SeedAdmissionResponseDTO) {
			response.CurrentVoters[1].MemberID = response.CurrentVoters[0].MemberID
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			response := valid
			response.CurrentVoters = append([]PeerIdentityDTO(nil), valid.CurrentVoters...)
			test.mutate(&response)
			if err := validateAdmissionResult(requested, response); !errors.Is(err, ErrInvalidPeerMessage) {
				t.Fatalf("invalid admission result accepted: err=%v", err)
			}
		})
	}
}

func TestCptDynamicIdentityValidation(t *testing.T) {
	voter := dynamicTestIdentity(1, "127.0.0.1:1", "node1.example", PeerPhaseVoter, "", 0x11)
	with := func(change func(*PeerIdentityDTO)) PeerIdentityDTO {
		identity := voter
		change(&identity)
		return identity
	}
	cases := []struct {
		name         string
		identity     PeerIdentityDTO
		allowStaged  bool
		wantAccepted bool
	}{
		{name: "voter", identity: voter, wantAccepted: true},
		{name: "staged admission without operation", identity: with(func(identity *PeerIdentityDTO) { identity.Phase = PeerPhaseStaged }), allowStaged: true, wantAccepted: true},
		{name: "staged admission with operation", identity: with(func(identity *PeerIdentityDTO) {
			identity.Phase, identity.OperationID = PeerPhaseStaged, testOperationID
		}), allowStaged: true, wantAccepted: true},
		{name: "leaving with operation", identity: with(func(identity *PeerIdentityDTO) {
			identity.Phase, identity.OperationID = PeerPhaseLeaving, testOperationID
		}), wantAccepted: true},
		{name: "zero member", identity: with(func(identity *PeerIdentityDTO) { identity.MemberID = 0 })},
		{name: "empty cluster", identity: with(func(identity *PeerIdentityDTO) { identity.ClusterID = "" })},
		{name: "address without port", identity: with(func(identity *PeerIdentityDTO) { identity.Address = "node1.example" })},
		{name: "uppercase DNS SAN", identity: with(func(identity *PeerIdentityDTO) { identity.DNSSAN = "Node1.example" })},
		{name: "empty release", identity: with(func(identity *PeerIdentityDTO) { identity.ReleaseVersion = "" })},
		{name: "release too long", identity: with(func(identity *PeerIdentityDTO) { identity.ReleaseVersion = strings.Repeat("r", 65) })},
		{name: "release not UTF-8", identity: with(func(identity *PeerIdentityDTO) { identity.ReleaseVersion = "\xff" })},
		{name: "cluster protocol major", identity: with(func(identity *PeerIdentityDTO) { identity.ClusterProtocolMajor = 3 })},
		{name: "cluster protocol minor", identity: with(func(identity *PeerIdentityDTO) { identity.ClusterProtocolMinor = 1 })},
		{name: "schema major", identity: with(func(identity *PeerIdentityDTO) { identity.StateSchemaMajor = 2 })},
		{name: "schema minor", identity: with(func(identity *PeerIdentityDTO) { identity.StateSchemaMinor = 1 })},
		{name: "staged Raft peer", identity: with(func(identity *PeerIdentityDTO) { identity.Phase = PeerPhaseStaged })},
		{name: "staged admission with malformed operation", identity: with(func(identity *PeerIdentityDTO) {
			identity.Phase, identity.OperationID = PeerPhaseStaged, "op"
		}), allowStaged: true},
		{name: "learner without operation", identity: with(func(identity *PeerIdentityDTO) { identity.Phase = PeerPhaseLearner })},
		{name: "catching up with malformed operation", identity: with(func(identity *PeerIdentityDTO) {
			identity.Phase, identity.OperationID = PeerPhaseCatchingUp, strings.ToUpper(testOperationID[:35])+"A"
		})},
		{name: "leaving without operation", identity: with(func(identity *PeerIdentityDTO) { identity.Phase = PeerPhaseLeaving })},
		{name: "voter with operation", identity: with(func(identity *PeerIdentityDTO) { identity.OperationID = testOperationID })},
		{name: "retired", identity: with(func(identity *PeerIdentityDTO) { identity.Phase = PeerPhaseRetired })},
		{name: "unknown phase", identity: with(func(identity *PeerIdentityDTO) { identity.Phase = 0 })},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := validateDynamicIdentity(test.identity, test.allowStaged)
			if (err == nil) != test.wantAccepted {
				t.Fatalf("validateDynamicIdentity = %v, want accepted=%t", err, test.wantAccepted)
			}
		})
	}
}

func TestCptDynamicAddressDNSAndOperationGrammar(t *testing.T) {
	addresses := map[string]bool{
		"127.0.0.1:1":                      true,
		"node.example:65535":               true,
		"[::1]:9000":                       true,
		"":                                 false,
		strings.Repeat("a", 251) + ":1234": false,
		" node.example:1":                  false,
		"node.example:1 ":                  false,
		"node.example":                     false,
		":1":                               false,
		"*:1":                              false,
		"a/b:1":                            false,
		"0.0.0.0:1":                        false,
		"[::]:1":                           false,
		"node.example:0":                   false,
		"node.example:65536":               false,
		"node.example:01":                  false,
		"node.example:port":                false,
		"\xff:1":                           false,
		"node\texample:1":                  false,
		strings.Repeat("b", 200) + ".example:12345": true,
	}
	for address, want := range addresses {
		if got := validV2Address(address); got != want {
			t.Errorf("validV2Address(%q) = %t, want %t", address, got, want)
		}
	}
	names := map[string]bool{
		"node1.example":                      true,
		"a-b.c":                              true,
		"":                                   false,
		strings.Repeat("a.", 127):            false,
		"Node1.example":                      false,
		"127.0.0.1":                          false,
		"nod\u00e9.example":                  false,
		"a..b":                               false,
		"-a.b":                               false,
		"a-.b":                               false,
		"a_b.c":                              false,
		strings.Repeat("l", 64) + ".example": false,
		strings.Repeat("l", 63) + ".example": true,
		"node1.example" + strings.Repeat(".a", 10): true,
	}
	for name, want := range names {
		if got := validV2DNSName(name); got != want {
			t.Errorf("validV2DNSName(%q) = %t, want %t", name, got, want)
		}
	}
	operations := map[string]bool{
		testOperationID:                                  true,
		"":                                               false,
		strings.Repeat("0", 36):                          false,
		"00000000-0000-4000-8000-00000000000A":           false,
		"00000000-0000-4000-8000-00000000000g":           false,
		"0000000-00000-4000-8000-000000000004":           false,
		"00000000-0000-4000-8000-0000000000004":          false,
		"00000000-0000-4000-8000-00000000000\u00e9"[:36]: false,
	}
	for operationID, want := range operations {
		if got := validOperationID(operationID); got != want {
			t.Errorf("validOperationID(%q) = %t, want %t", operationID, got, want)
		}
	}
}

func TestCptCompatiblePeerIdentityTransitions(t *testing.T) {
	base := dynamicTestIdentity(2, "n:2", "node2.example", PeerPhaseVoter, "", 0x22)
	phase := func(value PeerPhaseDTO, operationID string) PeerIdentityDTO {
		identity := base
		identity.Phase, identity.OperationID = value, operationID
		return identity
	}
	rebound := base
	rebound.Address = "n:3"
	cases := []struct {
		name             string
		expected, remote PeerIdentityDTO
		want             bool
	}{
		{name: "identical voter", expected: base, remote: base, want: true},
		{name: "voter sees leaving", expected: base, remote: phase(PeerPhaseLeaving, testOperationID), want: true},
		{name: "leaving sees voter", expected: phase(PeerPhaseLeaving, testOperationID), remote: base, want: true},
		{name: "voter sees leaving without operation", expected: base, remote: phase(PeerPhaseLeaving, ""), want: false},
		{name: "voter sees catching up", expected: base, remote: phase(PeerPhaseCatchingUp, testOperationID), want: true},
		{name: "learner sees catching up", expected: phase(PeerPhaseLearner, testOperationID), remote: phase(PeerPhaseCatchingUp, testOperationID), want: true},
		{name: "catching up sees learner", expected: phase(PeerPhaseCatchingUp, testOperationID), remote: phase(PeerPhaseLearner, testOperationID), want: true},
		{name: "learner sees other operation", expected: phase(PeerPhaseLearner, testOperationID), remote: phase(PeerPhaseCatchingUp, testOperationID5), want: false},
		{name: "voter sees learner", expected: base, remote: phase(PeerPhaseLearner, testOperationID), want: false},
		{name: "address rebinding", expected: base, remote: rebound, want: false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := compatiblePeerIdentity(test.expected, test.remote); got != test.want {
				t.Fatalf("compatiblePeerIdentity = %t, want %t", got, test.want)
			}
		})
	}
}

func TestCptDynamicCertificateBinding(t *testing.T) {
	node := cptNewNode(t, "node1.example")
	certificate := node.serverTLS.Certificates[0]
	identity := node.identity(1, "127.0.0.1:1", PeerPhaseVoter, "")
	withoutLeaf := certificate
	withoutLeaf.Leaf = nil
	otherSAN := identity
	otherSAN.DNSSAN = "node2.example"
	otherFingerprint := identity
	otherFingerprint.CertificateSHA256 = cptFingerprint(0x99)
	cases := []struct {
		name        string
		certificate tls.Certificate
		identity    PeerIdentityDTO
		wantErr     string
	}{
		{name: "parsed leaf", certificate: certificate, identity: identity},
		{name: "DER only", certificate: withoutLeaf, identity: identity},
		{name: "missing certificate", identity: identity, wantErr: "TLS certificate is missing"},
		{name: "invalid DER", certificate: tls.Certificate{Certificate: [][]byte{{0xff}}}, identity: identity, wantErr: "parse TLS leaf certificate"},
		{name: "fingerprint mismatch", certificate: certificate, identity: otherFingerprint, wantErr: "fingerprint does not match"},
		{name: "DNS SAN mismatch", certificate: certificate, identity: otherSAN, wantErr: "DNS SAN does not match"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := validateDynamicCertificate(test.certificate, test.identity)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("valid certificate binding rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("certificate binding error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestCptDynamicFrameIORejectsClosedTruncatedAndUnknownStreams(t *testing.T) {
	closedReader, closedPeer := net.Pipe()
	_ = closedReader.Close()
	_ = closedPeer.Close()
	if _, err := readDynamicFrame(closedReader, dynamicFrameLimit); err == nil {
		t.Fatal("readDynamicFrame read from a closed connection")
	}
	if _, err := readDynamicHandshakeFrame(closedReader); err == nil {
		t.Fatal("readDynamicHandshakeFrame read from a closed connection")
	}
	if err := writeDynamicFrame(closedReader, []byte{1}, dynamicFrameLimit); err == nil {
		t.Fatal("writeDynamicFrame wrote to a closed connection")
	}

	handshakeCases := []struct {
		name    string
		stream  []byte
		wantErr error
	}{
		{name: "empty stream", stream: nil, wantErr: io.EOF},
		{name: "truncated magic", stream: append(cptFramePrefix(20), "NMQ"...), wantErr: io.ErrUnexpectedEOF},
		{name: "unknown magic", stream: append(cptFramePrefix(20), "NMQXXXX2"...), wantErr: ErrInvalidPeerMessage},
		{name: "truncated body", stream: append(cptFramePrefix(20), "NMQRAFT2abc"...), wantErr: io.ErrUnexpectedEOF},
	}
	for _, test := range handshakeCases {
		t.Run("handshake "+test.name, func(t *testing.T) {
			if _, err := readDynamicHandshakeFrame(cptFeed(t, test.stream)); !errors.Is(err, test.wantErr) {
				t.Fatalf("readDynamicHandshakeFrame error = %v, want %v", err, test.wantErr)
			}
		})
	}
	valid := append(cptFramePrefix(12), "NMQJOIN2wxyz"...)
	if payload, err := readDynamicHandshakeFrame(cptFeed(t, valid)); err != nil || string(payload) != "NMQJOIN2wxyz" {
		t.Fatalf("read valid handshake frame = %q err=%v", payload, err)
	}

	frameCases := []struct {
		name    string
		stream  []byte
		wantErr error
	}{
		{name: "zero length", stream: cptFramePrefix(0), wantErr: ErrInvalidPeerMessage},
		{name: "over limit", stream: cptFramePrefix(dynamicHelloLimit + 1), wantErr: ErrInvalidPeerMessage},
		{name: "truncated payload", stream: append(cptFramePrefix(5), 1, 2), wantErr: io.ErrUnexpectedEOF},
	}
	for _, test := range frameCases {
		t.Run("frame "+test.name, func(t *testing.T) {
			if _, err := readDynamicFrame(cptFeed(t, test.stream), dynamicHelloLimit); !errors.Is(err, test.wantErr) {
				t.Fatalf("readDynamicFrame error = %v, want %v", err, test.wantErr)
			}
		})
	}

	if err := writeDynamicFrame(nil, nil, dynamicFrameLimit); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("empty dynamic frame written: %v", err)
	}
	if err := writeDynamicFrame(nil, make([]byte, dynamicHelloLimit+1), dynamicHelloLimit); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("oversized dynamic frame written: %v", err)
	}
	local, remote := net.Pipe()
	defer remote.Close()
	injected := errors.New("cpt write failure")
	if err := writeDynamicFrame(&cptConn{Conn: local, writeErr: injected}, []byte{1}, dynamicFrameLimit); !errors.Is(err, injected) {
		t.Fatalf("dynamic frame write failure = %v, want %v", err, injected)
	}
	if err := writePeerFrame(&cptConn{Conn: local, writeErr: injected}, []byte{1}); !errors.Is(err, injected) {
		t.Fatalf("fixed frame write failure = %v, want %v", err, injected)
	}
	_ = local.Close()
	if _, err := readPeerFrame(local); err == nil {
		t.Fatal("readPeerFrame read from a closed connection")
	}
}

func TestCptFixedPeerTransportConfigRejectsMissingIdentityInputs(t *testing.T) {
	node := cptNewNode(t, "node1.example")
	_, foreignClientTLS := testPeerTLSConfigs(t, "node2.example")
	listener := cptListen(t)
	base := PeerTransportConfig{
		NodeID: 1, ClusterID: cptCluster, LocalDNSName: "node1.example", Listener: listener,
		ServerTLS: node.serverTLS, ClientTLS: node.clientTLS,
		Peers: map[uint64]PeerEndpoint{
			2: {Address: "127.0.0.1:10002", DNSName: "node2.example"},
			3: {Address: "127.0.0.1:10003", DNSName: "node3.example"},
		},
	}
	cases := []struct {
		name    string
		mutate  func(*PeerTransportConfig)
		wantErr string
	}{
		{name: "voter outside fixed set", mutate: func(config *PeerTransportConfig) {
			delete(config.Peers, 3)
			config.Peers[4] = PeerEndpoint{Address: "127.0.0.1:10004", DNSName: "node4.example"}
		}, wantErr: "peer ID 3 is required"},
		{name: "empty peer address", mutate: func(config *PeerTransportConfig) {
			config.Peers[2] = PeerEndpoint{DNSName: "node2.example"}
		}, wantErr: "peer TCP address is required"},
		{name: "client certificate for another node", mutate: func(config *PeerTransportConfig) {
			config.ClientTLS = foreignClientTLS
		}, wantErr: "client TLS config"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := clonePeerTransportConfig(base)
			test.mutate(&config)
			transport, err := NewTCPPeerTransport(config)
			if err == nil {
				_ = transport.Close()
				t.Fatal("invalid fixed peer config accepted")
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("NewTCPPeerTransport error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestCptFixedPeerTransportServeLifecycleErrors(t *testing.T) {
	node := cptNewNode(t, "node3.example")
	peers := func() map[uint64]PeerEndpoint {
		return map[uint64]PeerEndpoint{
			1: {Address: "127.0.0.1:1", DNSName: "node1.example"},
			2: {Address: "127.0.0.1:1", DNSName: "node2.example"},
		}
	}
	t.Run("serve after close", func(t *testing.T) {
		transport := node.newFixed(t, 3, cptListen(t), peers())
		if err := transport.Close(); err != nil {
			t.Fatal(err)
		}
		if err := transport.Serve(context.Background()); !errors.Is(err, ErrPeerTransportClosed) {
			t.Fatalf("Serve after Close = %v, want ErrPeerTransportClosed", err)
		}
	})
	t.Run("second serve", func(t *testing.T) {
		transport := node.newFixed(t, 3, cptListen(t), peers())
		cptAssertSingleServe(t, transport.Serve)
	})
	t.Run("accept failure", func(t *testing.T) {
		transport := node.newFixed(t, 3, cptFailingListener{}, peers())
		if err := transport.Serve(context.Background()); !errors.Is(err, cptErrAccept) {
			t.Fatalf("Serve with failing listener = %v, want %v", err, cptErrAccept)
		}
		if _, open := <-transport.Receive(); open {
			t.Fatal("Receive remained open after accept failure")
		}
	})
}

func TestCptDynamicServeLifecycleErrors(t *testing.T) {
	node := cptNewNode(t, "node1.example")
	local := node.identity(1, "127.0.0.1:19001", PeerPhaseVoter, "")
	t.Run("nil context", func(t *testing.T) {
		transport := node.newDynamic(t, cptListen(t), local, nil, nil)
		//nolint:staticcheck // Nil context rejection is the behavior under test.
		if err := transport.Serve(nil); err == nil {
			t.Fatal("Serve accepted a nil context")
		}
	})
	t.Run("serve after close", func(t *testing.T) {
		transport := node.newDynamic(t, cptListen(t), local, nil, nil)
		if err := transport.Close(); err != nil {
			t.Fatal(err)
		}
		if err := transport.Serve(context.Background()); !errors.Is(err, ErrPeerTransportClosed) {
			t.Fatalf("Serve after Close = %v, want ErrPeerTransportClosed", err)
		}
	})
	t.Run("second serve", func(t *testing.T) {
		transport := node.newDynamic(t, cptListen(t), local, nil, nil)
		cptAssertSingleServe(t, transport.Serve)
	})
	t.Run("accept failure", func(t *testing.T) {
		transport := node.newDynamic(t, cptFailingListener{}, local, nil, nil)
		if err := transport.Serve(context.Background()); !errors.Is(err, cptErrAccept) {
			t.Fatalf("Serve with failing listener = %v, want %v", err, cptErrAccept)
		}
		if _, open := <-transport.InboundReady(); open {
			t.Fatal("InboundReady remained open after accept failure")
		}
	})
}

func cptAssertSingleServe(t *testing.T, serve func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- serve(ctx) }()
	go func() { results <- serve(ctx) }()
	select {
	case err := <-results:
		if err == nil || !strings.Contains(err.Error(), "only be called once") {
			t.Fatalf("first completed Serve = %v, want single-call rejection", err)
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("concurrent Serve calls were both admitted")
	}
	cancel()
	select {
	case err := <-results:
		if err != nil {
			t.Fatalf("admitted Serve after cancellation: %v", err)
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("admitted Serve did not stop after cancellation")
	}
}

func TestCptNewDynamicTransportRejectsInvalidConfig(t *testing.T) {
	node := cptNewNode(t, "node1.example")
	_, otherClientTLS := testPeerTLSConfigs(t, "node1.example")
	local := node.identity(1, "127.0.0.1:19001", PeerPhaseVoter, "")
	cases := []struct {
		name    string
		mutate  func(*DynamicPeerTransportConfig)
		wantErr string
	}{
		{name: "missing listener", mutate: func(config *DynamicPeerTransportConfig) { config.Listener = nil }, wantErr: "listener is required"},
		{name: "missing admission handler", mutate: func(config *DynamicPeerTransportConfig) { config.AdmissionHandler = nil }, wantErr: "admission handler is required"},
		{name: "retired local identity", mutate: func(config *DynamicPeerTransportConfig) {
			config.LocalIdentity.Phase = PeerPhaseRetired
		}, wantErr: "local peer identity"},
		{name: "missing server TLS", mutate: func(config *DynamicPeerTransportConfig) { config.ServerTLS = nil }, wantErr: "server TLS config"},
		{name: "local fingerprint differs from server certificate", mutate: func(config *DynamicPeerTransportConfig) {
			config.LocalIdentity.CertificateSHA256 = cptFingerprint(0x99)
		}, wantErr: "server TLS identity"},
		{name: "missing client TLS", mutate: func(config *DynamicPeerTransportConfig) { config.ClientTLS = nil }, wantErr: "client TLS config"},
		{name: "client certificate differs from local identity", mutate: func(config *DynamicPeerTransportConfig) {
			config.ClientTLS = otherClientTLS
		}, wantErr: "client TLS identity"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := DynamicPeerTransportConfig{
				LocalIdentity: local, Listener: cptListen(t), ServerTLS: node.serverTLS, ClientTLS: node.clientTLS,
				AdmissionHandler: cptRejectAdmission,
			}
			test.mutate(&config)
			transport, err := NewDynamicTCPPeerTransport(config)
			if err == nil {
				_ = transport.Close()
				t.Fatal("invalid dynamic peer config accepted")
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("NewDynamicTCPPeerTransport error = %v, want %q", err, test.wantErr)
			}
		})
	}

	listener := cptListen(t)
	if transport, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local, Listener: listener, ServerTLS: node.serverTLS, ClientTLS: node.clientTLS,
		Peers: []PeerIdentityDTO{local}, AdmissionHandler: cptRejectAdmission,
	}); err == nil {
		_ = transport.Close()
		t.Fatal("peer set containing the local member accepted")
	}
	accepted := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if connection != nil {
			_ = connection.Close()
		}
		accepted <- err
	}()
	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("rejected transport left its listener accepting connections")
		}
	case <-time.After(cptWaitTimeout):
		_ = listener.Close()
		t.Fatal("rejected transport did not close its listener")
	}
}

func TestCptDynamicSendResetAndCloseRejections(t *testing.T) {
	node := cptNewNode(t, "node1.example")
	local := node.identity(1, "127.0.0.1:19001", PeerPhaseVoter, "")
	remote := dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", 0x22)
	transport := node.newDynamic(t, cptListen(t), local, []PeerIdentityDTO{remote}, nil)
	peer := cptDynamicPeer(transport, remote.MemberID)

	invalid := []struct {
		name    string
		message PeerMessageDTO
	}{
		{name: "foreign source", message: PeerMessageDTO{From: 3, To: 2, Data: []byte{1}}},
		{name: "empty payload", message: PeerMessageDTO{From: 1, To: 2}},
		{name: "oversized payload", message: PeerMessageDTO{From: 1, To: 2, Data: make([]byte, dynamicFrameLimit+1)}},
	}
	for _, test := range invalid {
		if err := transport.Send(test.message); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Fatalf("Send %s = %v, want ErrInvalidPeerMessage", test.name, err)
		}
	}
	if status := cptDynamicStatus(peer); status.OutboundQueuedMessages != 0 || status.RejectedMessages != 0 {
		t.Fatalf("invalid messages changed queue state: %+v", status)
	}

	if err := transport.ResetPeer(9); !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("ResetPeer unknown = %v, want ErrUnknownPeer", err)
	}
	active, activeRemote := net.Pipe()
	defer activeRemote.Close()
	peer.mu.Lock()
	peer.connection = active
	peer.mu.Unlock()
	if err := transport.ResetPeer(remote.MemberID); err != nil {
		t.Fatalf("ResetPeer active connection: %v", err)
	}
	cptAssertClosed(t, activeRemote)
	status, err := transport.Status(remote.MemberID)
	if err != nil || status.Connected || status.RejectedMessages != 1 || status.LastError != "invalid peer message" {
		t.Fatalf("status after reset = %+v err=%v", status, err)
	}

	congested, congestedRemote := net.Pipe()
	defer congestedRemote.Close()
	peer.mu.Lock()
	peer.connection = congested
	peer.mu.Unlock()
	for index := range peerQueueMessages {
		if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{byte(index)}}); err != nil {
			t.Fatalf("queue message %d: %v", index, err)
		}
	}
	if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{0xff}}); !errors.Is(err, ErrPeerBackpressure) {
		t.Fatalf("Send to full queue = %v, want ErrPeerBackpressure", err)
	}
	status, _ = transport.Status(remote.MemberID)
	if status.RejectedMessages != 2 || status.OutboundQueuedMessages != peerQueueMessages || status.LastError != "invalid peer message" {
		t.Fatalf("status after backpressure = %+v, want prior error kept and two rejections", status)
	}

	peer.mu.Lock()
	revision := peer.identityRevision
	peer.mu.Unlock()
	if err := peer.enqueueInbound([]byte{0x01}, revision); err != nil {
		t.Fatalf("stage inbound frame: %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{1}}); !errors.Is(err, ErrPeerTransportClosed) {
		t.Fatalf("Send after Close = %v, want ErrPeerTransportClosed", err)
	}
	if err := transport.ResetPeer(remote.MemberID); !errors.Is(err, ErrPeerTransportClosed) {
		t.Fatalf("ResetPeer after Close = %v, want ErrPeerTransportClosed", err)
	}
	if delivery, ok := transport.TakeInbound(); ok {
		t.Fatalf("TakeInbound after Close leased %+v", delivery.Message)
	}
	if err := transport.ReconcilePeers([]PeerIdentityDTO{remote}); !errors.Is(err, ErrPeerTransportClosed) {
		t.Fatalf("ReconcilePeers after Close = %v, want ErrPeerTransportClosed", err)
	}
}

func TestCptDynamicInboundLeaseAllowsOneDeliveryPerPeer(t *testing.T) {
	local := dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11)
	remote := dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22)
	transport, peer := cptMinimalDynamic(local, remote)
	defer peer.stop()
	for _, payload := range [][]byte{{0x01}, {0x02, 0x02}} {
		if err := peer.enqueueInbound(payload, peer.identityRevision); err != nil {
			t.Fatalf("stage inbound frame: %v", err)
		}
	}
	first, ok := transport.TakeInbound()
	if !ok || !bytes.Equal(first.Message.Data, []byte{0x01}) || first.Message.From != 2 || first.Message.To != 1 {
		t.Fatalf("first lease = %+v ok=%t", first.Message, ok)
	}
	if second, ok := transport.TakeInbound(); ok {
		t.Fatalf("second lease %+v granted while the first was outstanding", second.Message)
	}
	first.Complete()
	first.Complete()
	PeerInboundDelivery{}.Complete()
	if status := cptDynamicStatus(peer); status.InboundQueuedMessages != 1 || status.InboundQueuedBytes != 6 {
		t.Fatalf("repeated Complete changed accounting: %+v", status)
	}
	second, ok := transport.TakeInbound()
	if !ok || !bytes.Equal(second.Message.Data, []byte{0x02, 0x02}) {
		t.Fatalf("second lease = %+v ok=%t", second.Message, ok)
	}
	second.Complete()
	if status := cptDynamicStatus(peer); status.InboundQueuedMessages != 0 || status.InboundQueuedBytes != 0 {
		t.Fatalf("completed leases left accounting: %+v", status)
	}

	peer.mu.Lock()
	peer.inboundDeliveryID = ^uint64(0)
	peer.mu.Unlock()
	if err := peer.enqueueInbound([]byte{0x03}, peer.identityRevision); err != nil {
		t.Fatalf("stage inbound frame: %v", err)
	}
	wrapped, ok := transport.TakeInbound()
	if !ok || wrapped.id == 0 {
		t.Fatalf("wrapped delivery ID = %d ok=%t, want a nonzero lease ID", wrapped.id, ok)
	}
	wrapped.Complete()
	if status := cptDynamicStatus(peer); status.InboundQueuedMessages != 0 {
		t.Fatalf("wrapped delivery did not release accounting: %+v", status)
	}
}

func TestCptDynamicUpdateLocalIdentityRejections(t *testing.T) {
	node := cptNewNode(t, "node1.example")
	local := node.identity(1, "127.0.0.1:19001", PeerPhaseLearner, testOperationID)
	transport := node.newDynamic(t, cptListen(t), local, nil, nil)
	with := func(change func(*PeerIdentityDTO)) PeerIdentityDTO {
		identity := local
		change(&identity)
		return identity
	}
	rejected := []struct {
		name     string
		identity PeerIdentityDTO
		wantErr  string
	}{
		{name: "retired phase", identity: with(func(identity *PeerIdentityDTO) {
			identity.Phase, identity.OperationID = PeerPhaseRetired, ""
		}), wantErr: "peer phase is not admitted"},
		{name: "staged without operation", identity: with(func(identity *PeerIdentityDTO) {
			identity.Phase, identity.OperationID = PeerPhaseStaged, ""
		}), wantErr: "requires a committed operation ID"},
		{name: "address rebinding", identity: with(func(identity *PeerIdentityDTO) {
			identity.Address = "127.0.0.1:19009"
		}), wantErr: "binding cannot change"},
		{name: "operation replaced", identity: with(func(identity *PeerIdentityDTO) {
			identity.Phase, identity.OperationID = PeerPhaseCatchingUp, testOperationID5
		}), wantErr: "operation cannot change"},
	}
	for _, test := range rejected {
		t.Run(test.name, func(t *testing.T) {
			err := transport.UpdateLocalIdentity(test.identity)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("UpdateLocalIdentity error = %v, want %q", err, test.wantErr)
			}
			if !samePeerIdentity(transport.LocalIdentity(), local) {
				t.Fatalf("rejected update changed local identity to %+v", transport.LocalIdentity())
			}
		})
	}

	staged := with(func(identity *PeerIdentityDTO) { identity.Phase = PeerPhaseStaged })
	if err := transport.UpdateLocalIdentity(staged); err != nil {
		t.Fatalf("stale staged update: %v", err)
	}
	if transport.LocalIdentity().Phase != PeerPhaseLearner {
		t.Fatalf("stale staged update regressed phase to %d", transport.LocalIdentity().Phase)
	}
	voter := with(func(identity *PeerIdentityDTO) { identity.Phase, identity.OperationID = PeerPhaseVoter, "" })
	if err := transport.UpdateLocalIdentity(voter); err != nil {
		t.Fatalf("committed voter update: %v", err)
	}
	leaving := with(func(identity *PeerIdentityDTO) {
		identity.Phase, identity.OperationID = PeerPhaseLeaving, testOperationID5
	})
	if err := transport.UpdateLocalIdentity(leaving); err != nil {
		t.Fatalf("committed leaving update: %v", err)
	}
	if !samePeerIdentity(transport.LocalIdentity(), leaving) {
		t.Fatalf("local identity = %+v, want leaving %+v", transport.LocalIdentity(), leaving)
	}
	if err := transport.UpdateLocalIdentity(voter); err != nil {
		t.Fatalf("stale voter update after leaving: %v", err)
	}
	if transport.LocalIdentity().Phase != PeerPhaseLeaving {
		t.Fatalf("stale voter update regressed leaving phase to %d", transport.LocalIdentity().Phase)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if err := transport.UpdateLocalIdentity(leaving); !errors.Is(err, ErrPeerTransportClosed) {
		t.Fatalf("UpdateLocalIdentity after Close = %v, want ErrPeerTransportClosed", err)
	}
}

func TestCptDynamicAdmissionIdentityInstallsAndRetiresPeers(t *testing.T) {
	node := cptNewNode(t, "node4.example")
	local := node.identity(4, "127.0.0.1:19004", PeerPhaseStaged, testOperationID)
	transport := node.newDynamic(t, cptListen(t), local, nil, nil)
	voters := cptVoters()
	learner := local
	learner.Phase = PeerPhaseLearner

	retired := local
	retired.Phase = PeerPhaseRetired
	if err := transport.updateAdmissionIdentity(retired, voters); err == nil {
		t.Fatal("retired admission identity installed")
	}
	if err := transport.updateAdmissionIdentity(learner, []PeerIdentityDTO{voters[0], voters[0]}); err == nil ||
		!strings.Contains(err.Error(), "peer list is invalid") {
		t.Fatalf("duplicate voter list error = %v", err)
	}
	if !samePeerIdentity(transport.LocalIdentity(), local) {
		t.Fatalf("rejected admission changed local identity to %+v", transport.LocalIdentity())
	}
	if err := transport.updateAdmissionIdentity(learner, voters); err != nil {
		t.Fatalf("install committed learner: %v", err)
	}
	for _, voter := range voters {
		if _, err := transport.Status(voter.MemberID); err != nil {
			t.Fatalf("voter %d not installed: %v", voter.MemberID, err)
		}
	}
	removed := cptDynamicPeer(transport, voters[1].MemberID)
	if err := transport.updateAdmissionIdentity(learner, []PeerIdentityDTO{voters[0], voters[2]}); err != nil {
		t.Fatalf("shrink committed voter list: %v", err)
	}
	if _, err := transport.Status(voters[1].MemberID); !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("removed voter still registered: %v", err)
	}
	removed.mu.Lock()
	stopped := removed.stopped
	removed.mu.Unlock()
	if !stopped {
		t.Fatal("removed voter worker state was not stopped")
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if err := transport.updateAdmissionIdentity(learner, voters); !errors.Is(err, ErrPeerTransportClosed) {
		t.Fatalf("admission update after Close = %v, want ErrPeerTransportClosed", err)
	}
}

func TestCptDynamicReconcilePeersRejectsInvalidSetsAtomically(t *testing.T) {
	node := cptNewNode(t, "node1.example")
	local := node.identity(1, "127.0.0.1:19001", PeerPhaseVoter, "")
	existing := dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", 0x22)
	addition := dynamicTestIdentity(3, "127.0.0.1:19003", "node3.example", PeerPhaseVoter, "", 0x33)
	transport := node.newDynamic(t, cptListen(t), local, []PeerIdentityDTO{existing}, nil)
	registered := cptDynamicPeer(transport, existing.MemberID)
	with := func(change func(*PeerIdentityDTO)) PeerIdentityDTO {
		identity := addition
		change(&identity)
		return identity
	}
	cases := []struct {
		name    string
		peers   []PeerIdentityDTO
		wantErr string
	}{
		{name: "local member", peers: []PeerIdentityDTO{addition, local}, wantErr: "local member ID"},
		{name: "duplicate member", peers: []PeerIdentityDTO{addition, addition}, wantErr: "duplicate dynamic peer ID"},
		{name: "release mismatch", peers: []PeerIdentityDTO{with(func(identity *PeerIdentityDTO) { identity.ReleaseVersion = "1.2.0" })}, wantErr: "compatibility mismatch"},
		{name: "invalid address", peers: []PeerIdentityDTO{with(func(identity *PeerIdentityDTO) { identity.Address = "bad" })}, wantErr: "dynamic peer 3"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := transport.ReconcilePeers(test.peers)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ReconcilePeers error = %v, want %q", err, test.wantErr)
			}
			if cptDynamicPeer(transport, existing.MemberID) != registered {
				t.Fatal("rejected reconcile replaced the registered peer")
			}
			if _, err := transport.Status(addition.MemberID); !errors.Is(err, ErrUnknownPeer) {
				t.Fatalf("rejected reconcile registered peer 3: %v", err)
			}
		})
	}
}

func TestCptValidateDynamicPeerSetUsesAdjacentReleaseWindow(t *testing.T) {
	cases := []struct {
		name      string
		local     string
		peer      string
		wantError bool
	}{
		{name: "local older", local: "1.1.0", peer: "1.2.0"},
		{name: "local newer", local: "1.2.0", peer: "1.1.0"},
		{name: "reject N-2", local: "1.2.0", peer: "1.0.0", wantError: true},
		{name: "reject newer N+2", local: "1.0.0", peer: "1.2.0", wantError: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			local := dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", PeerPhaseVoter, "", 0x11)
			local.ReleaseVersion = test.local
			peer := dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", 0x22)
			peer.ReleaseVersion = test.peer
			err := validateDynamicPeerSet(local, []PeerIdentityDTO{peer})
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "compatibility mismatch") {
					t.Fatalf("validateDynamicPeerSet error = %v, want compatibility mismatch", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateDynamicPeerSet rejected adjacent releases: %v", err)
			}
		})
	}
}

func TestCptDynamicHelloAcceptsAdjacentReleasesAndRefreshesProfile(t *testing.T) {
	left, right := cptNewNode(t, "node1.example"), cptNewNode(t, "node2.example")
	cptTrustEachOther(left, right)
	leftListener, rightListener := cptListen(t), cptListen(t)
	leftIdentity := left.identity(1, leftListener.Addr().String(), PeerPhaseVoter, "")
	leftIdentity.ReleaseVersion = "1.1.0"
	rightIdentity := right.identity(2, rightListener.Addr().String(), PeerPhaseVoter, "")
	rightIdentity.ReleaseVersion = "1.2.0"
	leftTransport := left.newDynamic(t, leftListener, leftIdentity, []PeerIdentityDTO{rightIdentity}, nil)
	rightTransport := right.newDynamic(t, rightListener, rightIdentity, []PeerIdentityDTO{leftIdentity}, nil)
	cptServe(t, leftTransport.Serve)
	cptServe(t, rightTransport.Serve)
	cptWaitUntil(t, "adjacent-release peer handshake", func() bool {
		leftStatus, leftErr := leftTransport.Status(rightIdentity.MemberID)
		rightStatus, rightErr := rightTransport.Status(leftIdentity.MemberID)
		return leftErr == nil && rightErr == nil && leftStatus.Connected && rightStatus.Connected
	})

	leftPeer := cptDynamicPeer(leftTransport, rightIdentity.MemberID)
	if leftPeer == nil {
		t.Fatal("adjacent-release peer was not registered")
	}
	oldIdentity, _, _ := leftPeer.identitySnapshot()
	refreshed := rightIdentity
	refreshed.ReleaseVersion = "1.1.0"
	if !samePeerIdentity(oldIdentity, refreshed) {
		t.Fatalf("release-only peer update changed immutable identity: old=%+v refreshed=%+v", oldIdentity, refreshed)
	}
	if err := leftTransport.ReconcilePeers([]PeerIdentityDTO{refreshed}); err != nil {
		t.Fatalf("refresh adjacent peer release profile: %v", err)
	}
	if status, err := leftTransport.Status(rightIdentity.MemberID); err != nil || !status.Connected {
		t.Fatalf("release-only profile refresh dropped live peer: status=%+v err=%v", status, err)
	}
	identity, _, stopped := cptDynamicPeer(leftTransport, rightIdentity.MemberID).identitySnapshot()
	if stopped || identity.ReleaseVersion != refreshed.ReleaseVersion {
		t.Fatalf("peer profile after refresh = %+v stopped=%t", identity, stopped)
	}

	message := PeerMessageDTO{From: leftIdentity.MemberID, To: rightIdentity.MemberID, Data: []byte("adjacent-release")}
	if err := leftTransport.Send(message); err != nil {
		t.Fatalf("send over adjacent-release connection: %v", err)
	}
	select {
	case <-rightTransport.InboundReady():
		delivery, received := rightTransport.TakeInbound()
		if !received || !bytes.Equal(delivery.Message.Data, message.Data) {
			t.Fatalf("received message = %+v received=%t", delivery.Message, received)
		}
		delivery.Complete()
	case <-time.After(cptWaitTimeout):
		t.Fatal("adjacent-release peer message was not delivered")
	}
}

func TestCptDynamicHelloRejectsNMinusTwoRelease(t *testing.T) {
	left, right := cptNewNode(t, "node1.example"), cptNewNode(t, "node2.example")
	cptTrustEachOther(left, right)
	leftListener, rightListener := cptListen(t), cptListen(t)
	leftIdentity := left.identity(1, leftListener.Addr().String(), PeerPhaseVoter, "")
	leftIdentity.ReleaseVersion = "1.0.0"
	rightIdentity := right.identity(2, rightListener.Addr().String(), PeerPhaseVoter, "")
	rightIdentity.ReleaseVersion = "1.2.0"
	leftView := rightIdentity
	leftView.ReleaseVersion = "1.1.0"
	rightView := leftIdentity
	rightView.ReleaseVersion = "1.1.0"
	leftTransport := left.newDynamic(t, leftListener, leftIdentity, []PeerIdentityDTO{leftView}, nil)
	rightTransport := right.newDynamic(t, rightListener, rightIdentity, []PeerIdentityDTO{rightView}, nil)
	cptServe(t, leftTransport.Serve)
	cptServe(t, rightTransport.Serve)
	cptWaitUntil(t, "N-2 release handshake rejection", func() bool {
		rightStatus, rightErr := rightTransport.Status(leftIdentity.MemberID)
		return rightErr == nil && strings.HasPrefix(rightStatus.LastError, "incompatible release:")
	})
	for _, pair := range []struct {
		name      string
		transport *DynamicTCPPeerTransport
		peerID    uint64
	}{
		{name: "lower member outbound", transport: leftTransport, peerID: rightIdentity.MemberID},
		{name: "higher member inbound", transport: rightTransport, peerID: leftIdentity.MemberID},
	} {
		status, err := pair.transport.Status(pair.peerID)
		if err != nil || status.Connected {
			t.Fatalf("%s status = %+v err=%v", pair.name, status, err)
		}
	}
}

func TestCptDynamicOutboundHelloRejectsNMinusTwoRelease(t *testing.T) {
	dialer, server := cptNewNode(t, "node1.example"), cptNewNode(t, "node2.example")
	cptTrustEachOther(dialer, server)
	transportListener, serverListener := cptListen(t), cptListen(t)
	local := dialer.identity(1, transportListener.Addr().String(), PeerPhaseVoter, "")
	local.ReleaseVersion = "1.2.0"
	expectedRemote := server.identity(2, serverListener.Addr().String(), PeerPhaseVoter, "")
	expectedRemote.ReleaseVersion = "1.1.0"
	actualRemote := expectedRemote
	actualRemote.ReleaseVersion = "1.0.0"
	transport := dialer.newDynamic(t, transportListener, local, []PeerIdentityDTO{expectedRemote}, nil)

	serverResult := make(chan error, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			connection, err := serverListener.Accept()
			if err != nil {
				select {
				case serverResult <- err:
				default:
				}
				return
			}
			func() {
				defer connection.Close()
				_ = connection.SetDeadline(time.Now().Add(cptWaitTimeout))
				tlsConnection := tls.Server(connection, server.serverTLS.Clone())
				if err := tlsConnection.Handshake(); err != nil {
					return
				}
				if _, err := readDynamicFrame(tlsConnection, dynamicHelloLimit); err != nil {
					return
				}
				hello, err := encodeDynamicPeerHello(actualRemote, dynamicRaftMagic)
				if err == nil {
					err = writeDynamicFrame(tlsConnection, hello, dynamicHelloLimit)
				}
				if err == nil {
					select {
					case serverResult <- nil:
					default:
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = serverListener.Close()
		select {
		case <-serverDone:
		case <-time.After(cptWaitTimeout):
			t.Error("fake peer did not stop after listener closure")
		}
	})
	cptServe(t, transport.Serve)
	deadline := time.Now().Add(cptWaitTimeout)
	for {
		status, err := transport.Status(expectedRemote.MemberID)
		if err == nil && strings.HasPrefix(status.LastError, "incompatible release:") {
			break
		}
		if time.Now().After(deadline) {
			select {
			case serverErr := <-serverResult:
				t.Fatalf("outbound N-2 hello status = %+v err=%v; fake peer result=%v", status, err, serverErr)
			default:
				t.Fatalf("outbound N-2 hello status = %+v err=%v; fake peer did not finish the handshake", status, err)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if status, err := transport.Status(expectedRemote.MemberID); err != nil || status.Connected {
		t.Fatalf("outbound N-2 peer status = %+v err=%v", status, err)
	}
	select {
	case err := <-serverResult:
		if err != nil {
			t.Fatalf("fake peer handshake: %v", err)
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("fake peer did not complete the hello exchange")
	}
}

func TestCptDynamicPhaseRefreshClosesActiveAndPendingSockets(t *testing.T) {
	local := dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11)
	remote := dynamicTestIdentity(2, "n:2", "n2", PeerPhaseLearner, testOperationID, 0x22)
	transport, peer := cptMinimalDynamic(local, remote)
	defer peer.stop()
	active, activeRemote := net.Pipe()
	defer activeRemote.Close()
	pending, pendingRemote := net.Pipe()
	defer pendingRemote.Close()
	peer.mu.Lock()
	peer.connection, peer.pending = active, pending
	peer.mu.Unlock()
	if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{0x05}}); err != nil {
		t.Fatal(err)
	}
	updated := remote
	updated.Phase = PeerPhaseCatchingUp
	if err := transport.ReconcilePeers([]PeerIdentityDTO{updated}); err != nil {
		t.Fatalf("phase refresh: %v", err)
	}
	cptAssertClosed(t, activeRemote)
	cptAssertClosed(t, pendingRemote)
	if cptDynamicPeer(transport, remote.MemberID) != peer {
		t.Fatal("phase refresh replaced the peer state")
	}
	status := cptDynamicStatus(peer)
	if status.Connected || status.OutboundQueuedMessages != 1 {
		t.Fatalf("status after phase refresh = %+v, want disconnected with queued frame", status)
	}
	peer.mu.Lock()
	pendingAfter, phaseAfter := peer.pending, peer.identity.Phase
	peer.mu.Unlock()
	if pendingAfter != nil || phaseAfter != PeerPhaseCatchingUp {
		t.Fatalf("phase refresh left pending=%v phase=%d", pendingAfter, phaseAfter)
	}
}

func TestCptDynamicPeerStopClosesPendingDial(t *testing.T) {
	peer := newDynamicPeerState(nil, dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22))
	pending, pendingRemote := net.Pipe()
	defer pendingRemote.Close()
	peer.mu.Lock()
	peer.pending = pending
	peer.mu.Unlock()
	peer.stop()
	cptAssertClosed(t, pendingRemote)
}

func TestCptPeerConnectionRejectsStoppedOrStaleSessions(t *testing.T) {
	ctx := context.Background()
	t.Run("fixed stopped peer", func(t *testing.T) {
		_, peer := newQueueTestTransport()
		peer.stop()
		local, remote := net.Pipe()
		defer remote.Close()
		peer.runConnection(ctx, local)
		cptAssertClosed(t, remote)
		if peer.status().Connected {
			t.Fatal("stopped peer adopted a connection")
		}
	})
	t.Run("dynamic stale revision", func(t *testing.T) {
		peer := newDynamicPeerState(nil, dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22))
		defer peer.stop()
		local, remote := net.Pipe()
		defer remote.Close()
		peer.runConnection(ctx, local, peer.identityRevision+1)
		cptAssertClosed(t, remote)
		if cptDynamicStatus(peer).Connected {
			t.Fatal("stale revision adopted a connection")
		}
	})
	t.Run("dynamic stopped peer", func(t *testing.T) {
		peer := newDynamicPeerState(nil, dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22))
		peer.stop()
		local, remote := net.Pipe()
		defer remote.Close()
		peer.runConnection(ctx, local, peer.identityRevision)
		cptAssertClosed(t, remote)
	})
}

func TestCptFixedPeerConnectionReplacementClosesSupersededSessions(t *testing.T) {
	transport, peer := newQueueTestTransport()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stale, staleRemote := net.Pipe()
	defer staleRemote.Close()
	peer.mu.Lock()
	peer.connection = stale
	peer.mu.Unlock()

	peer.sessionMu.Lock()
	waiting, waitingRemote := net.Pipe()
	defer waitingRemote.Close()
	waitingDone := cptRun(func() { peer.runConnection(ctx, waiting) })
	cptWaitUntil(t, "waiting session registration", func() bool {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		return peer.connection == waiting
	})
	cptAssertClosed(t, staleRemote)
	current, currentRemote := net.Pipe()
	defer currentRemote.Close()
	currentDone := cptRun(func() { peer.runConnection(ctx, current) })
	cptWaitUntil(t, "current session registration", func() bool {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		return peer.connection == current
	})
	cptAssertClosed(t, waitingRemote)
	peer.sessionMu.Unlock()

	if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{0x42}}); err != nil {
		t.Fatal(err)
	}
	payload, err := readPeerFrame(currentRemote)
	if err != nil || !bytes.Equal(payload, []byte{0x42}) {
		t.Fatalf("current session frame = %x err=%v", payload, err)
	}
	cancel()
	cptWaitDone(t, waitingDone, "superseded session exit")
	cptWaitDone(t, currentDone, "current session exit")
	if peer.status().Connected {
		t.Fatal("canceled sessions left the peer connected")
	}
}

func TestCptDynamicPeerConnectionReplacementClosesSupersededSessions(t *testing.T) {
	local := dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11)
	remote := dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22)
	transport, peer := cptMinimalDynamic(local, remote)
	defer peer.stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	revision := peer.identityRevision
	stale, staleRemote := net.Pipe()
	defer staleRemote.Close()
	peer.mu.Lock()
	peer.connection = stale
	peer.mu.Unlock()

	peer.sessionMu.Lock()
	waiting, waitingRemote := net.Pipe()
	defer waitingRemote.Close()
	waitingDone := cptRun(func() { peer.runConnection(ctx, waiting, revision) })
	cptWaitUntil(t, "waiting session registration", func() bool {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		return peer.connection == waiting
	})
	cptAssertClosed(t, staleRemote)
	current, currentRemote := net.Pipe()
	defer currentRemote.Close()
	currentDone := cptRun(func() { peer.runConnection(ctx, current, revision) })
	cptWaitUntil(t, "current session registration", func() bool {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		return peer.connection == current
	})
	cptAssertClosed(t, waitingRemote)
	peer.sessionMu.Unlock()

	if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{0x43}}); err != nil {
		t.Fatal(err)
	}
	payload, err := readDynamicFrame(currentRemote, dynamicFrameLimit)
	if err != nil || !bytes.Equal(payload, []byte{0x43}) {
		t.Fatalf("current session frame = %x err=%v", payload, err)
	}
	cancel()
	cptWaitDone(t, waitingDone, "superseded session exit")
	cptWaitDone(t, currentDone, "current session exit")
	if cptDynamicStatus(peer).Connected {
		t.Fatal("canceled sessions left the peer connected")
	}
}

func TestCptPeerConnectionWriteFailureRetainsInFlightFrame(t *testing.T) {
	injected := errors.New("cpt write failure")
	t.Run("fixed", func(t *testing.T) {
		transport, peer := newQueueTestTransport()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{0x07, 0x08}}); err != nil {
			t.Fatal(err)
		}
		failing, failingRemote := net.Pipe()
		defer failingRemote.Close()
		var writeCalled atomic.Bool
		peer.runConnection(ctx, &cptConn{Conn: failing, writeErr: injected, writeCalled: &writeCalled})
		if !writeCalled.Load() {
			t.Fatal("fixed peer never attempted the injected failing write")
		}
		status := peer.status()
		if status.Connected || status.OutboundQueuedMessages != 1 || status.OutboundQueuedBytes != 6 {
			t.Fatalf("status after write failure = %+v", status)
		}
		healthy, healthyRemote := net.Pipe()
		defer healthyRemote.Close()
		done := cptRun(func() { peer.runConnection(ctx, healthy) })
		payload, err := readPeerFrame(healthyRemote)
		if err != nil || !bytes.Equal(payload, []byte{0x07, 0x08}) {
			t.Fatalf("retransmitted frame = %x err=%v", payload, err)
		}
		cptWaitUntil(t, "retransmitted frame completion", func() bool { return peer.status().OutboundQueuedMessages == 0 })
		cancel()
		cptWaitDone(t, done, "fixed session exit")
	})
	t.Run("dynamic", func(t *testing.T) {
		local := dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11)
		remote := dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22)
		transport, peer := cptMinimalDynamic(local, remote)
		defer peer.stop()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := transport.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte{0x09}}); err != nil {
			t.Fatal(err)
		}
		failing, failingRemote := net.Pipe()
		defer failingRemote.Close()
		var writeCalled atomic.Bool
		peer.runConnection(ctx, &cptConn{Conn: failing, writeErr: injected, writeCalled: &writeCalled}, peer.identityRevision)
		if !writeCalled.Load() {
			t.Fatal("dynamic peer never attempted the injected failing write")
		}
		status := cptDynamicStatus(peer)
		if status.Connected || status.OutboundQueuedMessages != 1 || status.OutboundQueuedBytes != 5 {
			t.Fatalf("status after write failure = %+v", status)
		}
		healthy, healthyRemote := net.Pipe()
		defer healthyRemote.Close()
		done := cptRun(func() { peer.runConnection(ctx, healthy, peer.identityRevision) })
		payload, err := readDynamicFrame(healthyRemote, dynamicFrameLimit)
		if err != nil || !bytes.Equal(payload, []byte{0x09}) {
			t.Fatalf("retransmitted frame = %x err=%v", payload, err)
		}
		cptWaitUntil(t, "retransmitted frame completion", func() bool { return cptDynamicStatus(peer).OutboundQueuedMessages == 0 })
		cancel()
		cptWaitDone(t, done, "dynamic session exit")
	})
}

func cptInboundSession(t *testing.T, transport *DynamicTCPPeerTransport, client *cptNode, serverName string) (*tls.Conn, <-chan struct{}) {
	t.Helper()
	done := cptRun(func() {
		socket, err := transport.listener.Accept()
		if err != nil {
			return
		}
		defer socket.Close()
		transport.handleInbound(context.Background(), socket)
	})
	connection, err := tls.Dial("tcp", transport.listener.Addr().String(), client.dialConfig(serverName))
	if err != nil {
		t.Fatalf("dial dynamic peer listener: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	_ = connection.SetDeadline(time.Now().Add(cptWaitTimeout))
	return connection, done
}

func TestCptDynamicInboundRejectsUnexpectedHandshakes(t *testing.T) {
	server := cptNewNode(t, "node2.example")
	client := cptNewNode(t, "node1.example")
	cptTrustEachOther(server, client)
	listener := cptListen(t)
	clientIdentity := client.identity(1, "127.0.0.1:19001", PeerPhaseVoter, "")
	withMember := func(memberID uint64) PeerIdentityDTO {
		identity := clientIdentity
		identity.MemberID = memberID
		return identity
	}
	foreignSAN := withMember(7)
	foreignSAN.DNSSAN = "other.example"
	var admissionCalls atomic.Int32
	transport := server.newDynamic(t, listener, server.identity(2, listener.Addr().String(), PeerPhaseVoter, ""),
		[]PeerIdentityDTO{clientIdentity, withMember(3), withMember(4), foreignSAN},
		func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			admissionCalls.Add(1)
			return SeedAdmissionResponseDTO{}, errors.New("cpt: no committed staged member")
		})
	cptDynamicPeer(transport, 4).stop()

	hello := func(identity PeerIdentityDTO) []byte {
		payload, err := encodeDynamicPeerHello(identity, dynamicRaftMagic)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	raftCases := []struct {
		name      string
		payload   []byte
		peerID    uint64
		wantError string
	}{
		{name: "malformed hello", payload: append([]byte("NMQRAFT2"), 0, 2, 0)},
		{name: "unregistered member", payload: hello(withMember(9))},
		{name: "stopped peer", payload: hello(withMember(4)), peerID: 4},
		{name: "certificate SAN mismatch", payload: hello(foreignSAN), peerID: 7, wantError: "peer certificate identity mismatch"},
		{name: "higher member must dial", payload: hello(withMember(3)), peerID: 3, wantError: "direction mismatch: remote 3 >= local 2"},
	}
	for _, test := range raftCases {
		t.Run(test.name, func(t *testing.T) {
			connection, done := cptInboundSession(t, transport, client, server.dnsName)
			if err := writeDynamicFrame(connection, test.payload, dynamicHelloLimit); err != nil {
				t.Fatalf("write hello: %v", err)
			}
			cptAssertClosed(t, connection)
			cptWaitDone(t, done, "inbound handler exit")
			if test.peerID == 0 {
				return
			}
			status, err := transport.Status(test.peerID)
			if err != nil || status.Connected || status.LastError != test.wantError {
				t.Fatalf("peer %d status = %+v err=%v, want error %q", test.peerID, status, err, test.wantError)
			}
		})
	}
	if status, _ := transport.Status(clientIdentity.MemberID); status.LastError != "" || status.Connected {
		t.Fatalf("rejected handshakes changed unrelated peer 1: %+v", status)
	}

	request := func(change func(*PeerIdentityDTO)) []byte {
		candidate := withMember(9)
		candidate.Phase, candidate.OperationID = PeerPhaseStaged, ""
		change(&candidate)
		payload, err := encodeSeedAdmissionRequest(SeedAdmissionRequestDTO{Candidate: candidate})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	admissionCases := []struct {
		name      string
		payload   []byte
		wantCalls int32
	}{
		{name: "malformed request", payload: append([]byte("NMQJOIN2"), 0, 2)},
		{name: "certificate fingerprint mismatch", payload: request(func(candidate *PeerIdentityDTO) {
			candidate.CertificateSHA256 = cptFingerprint(0x99)
		})},
		{name: "release mismatch", payload: request(func(candidate *PeerIdentityDTO) { candidate.ReleaseVersion = "1.2.0" })},
		{name: "cluster mismatch", payload: request(func(candidate *PeerIdentityDTO) { candidate.ClusterID = "other" })},
		{name: "matching candidate reaches handler", payload: request(func(*PeerIdentityDTO) {}), wantCalls: 1},
	}
	for _, test := range admissionCases {
		t.Run("admission "+test.name, func(t *testing.T) {
			before := admissionCalls.Load()
			connection, done := cptInboundSession(t, transport, client, server.dnsName)
			if err := writeDynamicFrame(connection, test.payload, dynamicAdmissionLimit); err != nil {
				t.Fatalf("write admission request: %v", err)
			}
			response, err := readDynamicFrame(connection, dynamicAdmissionLimit)
			if err != nil || !bytes.Equal(response, rejectedSeedAdmissionResponse()) {
				t.Fatalf("admission response = %x err=%v, want generic rejection", response, err)
			}
			cptWaitDone(t, done, "admission handler exit")
			if calls := admissionCalls.Load() - before; calls != test.wantCalls {
				t.Fatalf("admission handler calls = %d, want %d", calls, test.wantCalls)
			}
		})
	}
}

func TestCptDynamicAdmissionAbortsWhenLocalIdentityChanges(t *testing.T) {
	seed := cptNewNode(t, "seed.example")
	candidate := cptNewNode(t, "node4.example")
	cptTrustEachOther(seed, candidate)
	listener := cptListen(t)
	seedLocal := seed.identity(5, listener.Addr().String(), PeerPhaseVoter, "")
	leaving := seedLocal
	leaving.Phase, leaving.OperationID = PeerPhaseLeaving, testOperationID5
	var transport *DynamicTCPPeerTransport
	var calls atomic.Int32
	handler := func(_ context.Context, requested PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
		calls.Add(1)
		if err := transport.UpdateLocalIdentity(leaving); err != nil {
			return SeedAdmissionResponseDTO{}, err
		}
		committed := requested
		committed.OperationID = testOperationID
		return SeedAdmissionResponseDTO{Accepted: true, Candidate: committed, CurrentVoters: cptVoters()}, nil
	}
	transport = seed.newDynamic(t, listener, seedLocal, nil, handler)
	cptServe(t, transport.Serve)

	connection, err := tls.Dial("tcp", listener.Addr().String(), candidate.dialConfig(seed.dnsName))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(cptWaitTimeout))
	payload, err := encodeSeedAdmissionRequest(SeedAdmissionRequestDTO{
		Candidate: candidate.identity(4, "127.0.0.1:19004", PeerPhaseStaged, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDynamicFrame(connection, payload, dynamicAdmissionLimit); err != nil {
		t.Fatal(err)
	}
	if response, err := readDynamicFrame(connection, dynamicAdmissionLimit); err == nil {
		t.Fatalf("admission response %x delivered after the seed identity changed", response)
	}
	if calls.Load() != 1 {
		t.Fatalf("admission handler calls = %d, want 1", calls.Load())
	}
	if !samePeerIdentity(transport.LocalIdentity(), leaving) {
		t.Fatalf("seed identity = %+v, want %+v", transport.LocalIdentity(), leaving)
	}
}

func TestCptDynamicInboundConnectionsAreBoundedByPeerObservationAndSnapshotSlots(t *testing.T) {
	server := cptNewNode(t, "seed.example")
	client := cptNewNode(t, "node4.example")
	cptTrustEachOther(server, client)
	listener := cptListen(t)
	transport := server.newDynamic(t, listener, server.identity(5, listener.Addr().String(), PeerPhaseVoter, ""), nil, nil)
	cptServe(t, transport.Serve)
	address := listener.Addr().String()

	inboundLimit := 2 + dynamicObservabilitySlots // One admission slot and one snapshot stream slot.
	held := make([]*tls.Conn, 0, inboundLimit)
	for range inboundLimit {
		connection, err := tls.Dial("tcp", address, client.dialConfig(server.dnsName))
		if err != nil {
			t.Fatalf("dial inbound connection within the admission, observation and snapshot slots: %v", err)
		}
		held = append(held, connection)
	}
	if overflow, err := tls.Dial("tcp", address, client.dialConfig(server.dnsName)); err == nil {
		_ = overflow.Close()
		for _, connection := range held {
			_ = connection.Close()
		}
		t.Fatal("inbound connection beyond the admission, observation and snapshot slots completed a TLS handshake")
	}
	for _, connection := range held {
		_ = connection.Close()
	}
	cptWaitUntil(t, "released admission, observation and snapshot slots", func() bool {
		connection, err := tls.Dial("tcp", address, client.dialConfig(server.dnsName))
		if err != nil {
			return false
		}
		_ = connection.Close()
		return true
	})
}

func TestCptDynamicStagedLocalDefersRaftDialUntilAdmitted(t *testing.T) {
	node := cptNewNode(t, "node1.example")
	peerListener := cptListen(t)
	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			connection, err := peerListener.Accept()
			if err != nil {
				return
			}
			accepted <- connection
		}
	}()
	t.Cleanup(func() {
		_ = peerListener.Close()
		for {
			select {
			case connection := <-accepted:
				_ = connection.Close()
			default:
				return
			}
		}
	})
	local := node.identity(1, "127.0.0.1:19001", PeerPhaseStaged, testOperationID)
	remote := dynamicTestIdentity(2, peerListener.Addr().String(), "node2.example", PeerPhaseVoter, "", 0x22)
	remote.ReleaseVersion = local.ReleaseVersion
	transport := node.newDynamic(t, cptListen(t), local, []PeerIdentityDTO{remote}, nil)
	cptServe(t, transport.Serve)

	select {
	case connection := <-accepted:
		_ = connection.Close()
		t.Fatal("staged local member dialed a Raft peer")
	case <-time.After(3 * peerRetryInterval):
	}
	learner := local
	learner.Phase = PeerPhaseLearner
	if err := transport.UpdateLocalIdentity(learner); err != nil {
		t.Fatal(err)
	}
	select {
	case connection := <-accepted:
		_ = connection.Close()
	case <-time.After(cptWaitTimeout):
		t.Fatal("admitted learner did not dial its Raft peer")
	}
}

func TestCptDynamicDialRejectsUnexpectedRemoteIdentity(t *testing.T) {
	cases := []struct {
		name       string
		register   func(PeerIdentityDTO) PeerIdentityDTO
		wantPrefix string
	}{
		{name: "certificate fingerprint", register: func(identity PeerIdentityDTO) PeerIdentityDTO {
			identity.CertificateSHA256 = cptFingerprint(0x99)
			return identity
		}, wantPrefix: "peer identity mismatch"},
		{name: "committed phase", register: func(identity PeerIdentityDTO) PeerIdentityDTO {
			identity.Phase, identity.OperationID = PeerPhaseLearner, testOperationID
			return identity
		}, wantPrefix: "incompatible identity"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dialer := cptNewNode(t, "node1.example")
			acceptor := cptNewNode(t, "node2.example")
			cptTrustEachOther(dialer, acceptor)
			dialerListener, acceptorListener := cptListen(t), cptListen(t)
			dialerIdentity := dialer.identity(1, dialerListener.Addr().String(), PeerPhaseVoter, "")
			acceptorIdentity := acceptor.identity(2, acceptorListener.Addr().String(), PeerPhaseVoter, "")
			dialing := dialer.newDynamic(t, dialerListener, dialerIdentity, []PeerIdentityDTO{test.register(acceptorIdentity)}, nil)
			accepting := acceptor.newDynamic(t, acceptorListener, acceptorIdentity, []PeerIdentityDTO{dialerIdentity}, nil)
			cptServe(t, accepting.Serve)
			cptServe(t, dialing.Serve)
			cptWaitUntil(t, "outbound identity rejection", func() bool {
				status, err := dialing.Status(acceptorIdentity.MemberID)
				return err == nil && strings.HasPrefix(status.LastError, test.wantPrefix)
			})
			if status, _ := dialing.Status(acceptorIdentity.MemberID); status.Connected {
				t.Fatalf("rejected remote identity became connected: %+v", status)
			}
		})
	}
}

func TestCptFixedPeerTransportRejectsInboundHelloFromNonLowerVoter(t *testing.T) {
	server := cptNewNode(t, "node3.example")
	client := cptNewNode(t, "node1.example")
	cptTrustEachOther(server, client)
	listener := cptListen(t)
	transport := server.newFixed(t, 3, listener, map[uint64]PeerEndpoint{
		1: {Address: "127.0.0.1:1", DNSName: "node1.example"},
		2: {Address: "127.0.0.1:1", DNSName: "node2.example"},
	})
	cptServe(t, transport.Serve)
	dial := func(helloID uint64) *tls.Conn {
		connection, err := tls.Dial("tcp", listener.Addr().String(), client.dialConfig(server.dnsName))
		if err != nil {
			t.Fatalf("dial fixed peer listener: %v", err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		_ = connection.SetDeadline(time.Now().Add(cptWaitTimeout))
		if peerID, err := readPeerHello(connection, cptCluster); err != nil || peerID != 3 {
			t.Fatalf("server hello = %d err=%v, want 3", peerID, err)
		}
		if err := writePeerHello(connection, cptCluster, helloID); err != nil {
			t.Fatalf("write client hello: %v", err)
		}
		return connection
	}
	cptAssertClosed(t, dial(3))
	if status, _ := transport.Status(1); status.Connected {
		t.Fatalf("self-identified hello connected peer 1: %+v", status)
	}
	dial(1)
	cptWaitUntil(t, "lower voter connection", func() bool {
		status, _ := transport.Status(1)
		return status.Connected
	})
}

func TestCptFixedPeerTransportRedialsAfterPeerFailures(t *testing.T) {
	cases := []struct {
		name      string
		useTLS    bool
		serve     func(net.Conn)
		wantError string
	}{
		{name: "TLS handshake refused", wantError: "peer connection failed"},
		{name: "hello names another voter", useTLS: true, serve: func(connection net.Conn) {
			_ = writePeerHello(connection, cptCluster, 1)
			_, _ = readPeerHello(connection, cptCluster)
		}, wantError: "peer identity mismatch"},
		{name: "session ends after hello", useTLS: true, serve: func(connection net.Conn) {
			_ = writePeerHello(connection, cptCluster, 3)
			_, _ = readPeerHello(connection, cptCluster)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			local := cptNewNode(t, "node2.example")
			remote := cptNewNode(t, "node3.example")
			cptTrustEachOther(local, remote)
			remoteListener := cptListen(t)
			var accepted atomic.Int32
			go func() {
				for {
					socket, err := remoteListener.Accept()
					if err != nil {
						return
					}
					accepted.Add(1)
					go func() {
						defer socket.Close()
						if !test.useTLS {
							return
						}
						connection := tls.Server(socket, remote.serverTLS)
						_ = connection.SetDeadline(time.Now().Add(cptWaitTimeout))
						if connection.Handshake() != nil {
							return
						}
						test.serve(connection)
					}()
				}
			}()
			transport := local.newFixed(t, 2, cptListen(t), map[uint64]PeerEndpoint{
				1: {Address: "127.0.0.1:1", DNSName: "node1.example"},
				3: {Address: remoteListener.Addr().String(), DNSName: "node3.example"},
			})
			cptServe(t, transport.Serve)
			cptWaitUntil(t, "second outbound attempt", func() bool { return accepted.Load() >= 2 })
			if test.wantError == "" {
				return
			}
			cptWaitUntil(t, "peer-local failure status", func() bool {
				status, _ := transport.Status(3)
				return status.LastError == test.wantError
			})
		})
	}
}

func TestCptSeedAdmissionRequestValidation(t *testing.T) {
	node := cptNewNode(t, "node4.example")
	learner := node.identity(4, "127.0.0.1:19004", PeerPhaseLearner, testOperationID)
	transport := newAdmissionCandidateTransport(t, learner, node.serverTLS, node.clientTLS)
	t.Cleanup(func() { _ = transport.Close() })
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name    string
		ctx     context.Context
		address string
		dnsName string
		wantErr string
	}{
		{name: "nil context", address: "127.0.0.1:9", dnsName: "seed.example", wantErr: "context is required"},
		{name: "address without port", ctx: context.Background(), address: "seed.example", dnsName: "seed.example", wantErr: "seed address and DNS name"},
		{name: "uppercase DNS name", ctx: context.Background(), address: "127.0.0.1:9", dnsName: "Seed.example", wantErr: "seed address and DNS name"},
		{name: "canceled dial", ctx: canceled, address: "127.0.0.1:9", dnsName: "seed.example", wantErr: "seed admission connection failed"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			voters, err := transport.RequestSeedAdmission(test.ctx, test.address, test.dnsName)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("RequestSeedAdmission = %+v err=%v, want %q", voters, err, test.wantErr)
			}
		})
	}

	leaving := node.identity(4, "127.0.0.1:19004", PeerPhaseLeaving, testOperationID)
	leavingTransport := newAdmissionCandidateTransport(t, leaving, node.serverTLS, node.clientTLS)
	t.Cleanup(func() { _ = leavingTransport.Close() })
	if _, err := leavingTransport.RequestSeedAdmission(context.Background(), "127.0.0.1:9", "seed.example"); err == nil ||
		!strings.Contains(err.Error(), "active candidate identity") {
		t.Fatalf("leaving member admission error = %v", err)
	}

	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.RequestSeedAdmission(context.Background(), "127.0.0.1:9", "seed.example"); !errors.Is(err, ErrPeerTransportClosed) {
		t.Fatalf("admission after Close = %v, want ErrPeerTransportClosed", err)
	}
}

func TestCptSeedAdmissionRejectsInvalidSeedResponses(t *testing.T) {
	seed := cptNewNode(t, "seed.example")
	candidate := cptNewNode(t, "node4.example")
	cptTrustEachOther(seed, candidate)
	seedListener := tls.NewListener(cptListen(t), seed.serverTLS)
	local := candidate.identity(4, "127.0.0.1:19004", PeerPhaseLearner, testOperationID)
	transport := newAdmissionCandidateTransport(t, local, candidate.serverTLS, candidate.clientTLS)
	t.Cleanup(func() { _ = transport.Close() })
	voters := cptVoters()
	withCandidate := func(change func(*PeerIdentityDTO)) []byte {
		committed := local
		change(&committed)
		return cptAdmissionResponseBytes(committed, voters)
	}
	withVoter := func(index int, change func(*PeerIdentityDTO)) []byte {
		list := append([]PeerIdentityDTO(nil), voters...)
		change(&list[index])
		return cptAdmissionResponseBytes(local, list)
	}
	exchange := func(t *testing.T, response []byte) ([]PeerIdentityDTO, error) {
		t.Helper()
		requests := make(chan []byte, 1)
		go func() {
			socket, err := seedListener.Accept()
			if err != nil {
				requests <- nil
				return
			}
			defer socket.Close()
			_ = socket.SetDeadline(time.Now().Add(cptWaitTimeout))
			request, err := readDynamicFrame(socket, dynamicAdmissionLimit)
			requests <- request
			if err != nil || response == nil {
				return
			}
			_ = writeDynamicFrame(socket, response, dynamicAdmissionLimit)
		}()
		result, err := transport.RequestSeedAdmission(context.Background(), seedListener.Addr().String(), seed.dnsName)
		var request []byte
		select {
		case request = <-requests:
		case <-time.After(cptWaitTimeout):
			t.Fatal("fake seed received no admission request")
		}
		decoded, decodeErr := decodeSeedAdmissionRequest(request)
		if decodeErr != nil || decoded.Candidate.MemberID != local.MemberID || decoded.Candidate.Phase != PeerPhaseStaged ||
			decoded.Candidate.OperationID != "" || decoded.Candidate.CertificateSHA256 != local.CertificateSHA256 {
			t.Fatalf("admission request candidate = %+v err=%v, want staged proof of local identity", decoded.Candidate, decodeErr)
		}
		return result, err
	}
	cases := []struct {
		name     string
		response []byte
		wantErr  string
	}{
		{name: "connection closed before response", wantErr: "seed admission response failed"},
		{name: "generic rejection", response: rejectedSeedAdmissionResponse(), wantErr: "seed admission rejected"},
		{name: "malformed response", response: []byte("NMQJOIN2\x00\x02\x00\x00\x07"), wantErr: "seed admission rejected"},
		{name: "candidate certificate differs", response: withCandidate(func(identity *PeerIdentityDTO) {
			identity.CertificateSHA256 = cptFingerprint(0x99)
		}), wantErr: "identity mismatch"},
		{name: "phase regression", response: withCandidate(func(identity *PeerIdentityDTO) {
			identity.Phase = PeerPhaseStaged
		}), wantErr: "identity mismatch"},
		{name: "operation replaced", response: withCandidate(func(identity *PeerIdentityDTO) {
			identity.Phase, identity.OperationID = PeerPhaseCatchingUp, testOperationID5
		}), wantErr: "identity mismatch"},
		{name: "voter list includes candidate", response: withVoter(2, func(identity *PeerIdentityDTO) {
			identity.MemberID = local.MemberID
		}), wantErr: "voter list is invalid"},
		{name: "voter DNS SAN invalid", response: withVoter(0, func(identity *PeerIdentityDTO) {
			identity.DNSSAN = "Node1.example"
		}), wantErr: "voter list is invalid"},
		{name: "voter address invalid", response: withVoter(1, func(identity *PeerIdentityDTO) {
			identity.Address = "bad"
		}), wantErr: "voter list is invalid"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result, err := exchange(t, test.response)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("RequestSeedAdmission = %+v err=%v, want %q", result, err, test.wantErr)
			}
			if !samePeerIdentity(transport.LocalIdentity(), local) {
				t.Fatalf("rejected admission changed local identity to %+v", transport.LocalIdentity())
			}
			if _, err := transport.Status(voters[0].MemberID); !errors.Is(err, ErrUnknownPeer) {
				t.Fatalf("rejected admission installed voter peers: %v", err)
			}
		})
	}

	result, err := exchange(t, withCandidate(func(identity *PeerIdentityDTO) { identity.Phase = PeerPhaseCatchingUp }))
	if err != nil || len(result) != 3 {
		t.Fatalf("valid committed admission = %+v err=%v", result, err)
	}
	if phase := transport.LocalIdentity().Phase; phase != PeerPhaseCatchingUp {
		t.Fatalf("local phase after admission = %d, want catching up", phase)
	}
	if _, err := transport.Status(voters[0].MemberID); err != nil {
		t.Fatalf("valid admission did not install voter peers: %v", err)
	}
}
