package tcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testOperationID  = "00000000-0000-4000-8000-000000000004"
	testOperationID5 = "00000000-0000-4000-8000-000000000005"
)

func takeDynamicPeerMessage(t *testing.T, transport *DynamicTCPPeerTransport) PeerMessageDTO {
	t.Helper()
	select {
	case _, open := <-transport.InboundReady():
		if !open {
			t.Fatal("dynamic peer inbound-ready channel closed before message delivery")
		}
		delivery, received := transport.TakeInbound()
		if !received {
			t.Fatal("inbound-ready signal had no staged message")
		}
		message := delivery.Message
		delivery.Complete()
		return message
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for dynamic peer message")
		return PeerMessageDTO{}
	}
}

func TestDynamicPeerHelloGoldenBytes(t *testing.T) {
	identity := dynamicTestIdentity(1, "n:1", "n", PeerPhaseVoter, "", 0x11)
	got, err := encodeDynamicPeerHello(identity, dynamicRaftMagic)
	if err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString("4e4d51524146543200020000000163000000000000000100036e3a3100016e" +
		strings.Repeat("11", sha256.Size) + "0005312e302e30000200000001000004")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("v2 hello bytes differ\n got: %x\nwant: %x", got, want)
	}
	assertDynamicFramedPayload(t, want, dynamicHelloLimit)
	decoded, err := decodeDynamicPeerHello(want, dynamicRaftMagic)
	if err != nil || !samePeerIdentity(decoded, identity) {
		t.Fatalf("decode golden v2 hello: identity=%+v err=%v", decoded, err)
	}
}

func TestDynamicSeedAdmissionGoldenBytes(t *testing.T) {
	candidate := dynamicTestIdentity(4, "n:4", "n", PeerPhaseStaged, testOperationID, 0x22)
	got, err := encodeSeedAdmissionRequest(SeedAdmissionRequestDTO{Candidate: candidate})
	if err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString("4e4d514a4f494e3200020000000163000000000000000400036e3a3400016e" +
		strings.Repeat("22", sha256.Size) + "0005312e302e3000010000")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("seed admission request bytes differ\n got: %x\nwant: %x", got, want)
	}
	assertDynamicFramedPayload(t, want, dynamicAdmissionLimit)

	response := SeedAdmissionResponseDTO{
		Accepted:  true,
		Candidate: candidate,
	}
	response.Candidate.OperationID = testOperationID
	for id := uint64(1); id <= 3; id++ {
		response.CurrentVoters = append(response.CurrentVoters,
			dynamicTestIdentity(id, "n:"+string(rune('0'+id)), "n", PeerPhaseVoter, "", byte(0x10+id)))
	}
	got, err = encodeSeedAdmissionResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	want = append([]byte("NMQJOIN2"), 0, 2, 0, 0, 1, 0, 1, 'c')
	want = append(want, 0, byte(len(testOperationID)))
	want = append(want, testOperationID...)
	want = append(want, 0, 0, 0, 0, 0, 0, 0, 4)
	want = append(want, bytes.Repeat([]byte{0x22}, sha256.Size)...)
	want = append(want, 0, 1, 'n', byte(PeerPhaseStaged), 0, 1, 0, 0, 0, 3)
	for id := uint64(1); id <= 3; id++ {
		want = append(want, 0, 0, 0, 0, 0, 0, 0, byte(id))
		want = append(want, 0, 3, 'n', ':', byte('0'+id), 0, 1, 'n')
		want = append(want, bytes.Repeat([]byte{byte(0x10 + id)}, sha256.Size)...)
		want = append(want, byte(PeerPhaseVoter))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("seed admission response bytes differ\n got: %x\nwant: %x", got, want)
	}
	assertDynamicFramedPayload(t, got, dynamicAdmissionLimit)
	decoded, err := decodeSeedAdmissionResponse(want)
	if err != nil || !decoded.Accepted || decoded.Candidate.MemberID != 4 || decoded.Candidate.OperationID != testOperationID || len(decoded.CurrentVoters) != 3 {
		t.Fatalf("decode golden admission response: response=%+v err=%v", decoded, err)
	}
}

func TestDynamicPeerHelloAndAdmissionRejectMalformedData(t *testing.T) {
	identity := dynamicTestIdentity(1, "n:1", "n", PeerPhaseVoter, "", 0x11)
	hello, err := encodeDynamicPeerHello(identity, dynamicRaftMagic)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"trailing hello byte": append(append([]byte(nil), hello...), 0),
		"wrong protocol":      append([]byte(nil), hello...),
		"zero member ID":      append([]byte(nil), hello...),
	}
	cases["wrong protocol"][9] = 3
	for index := 15; index < 23; index++ {
		cases["zero member ID"][index] = 0
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeDynamicPeerHello(payload, dynamicRaftMagic); err == nil {
				t.Fatal("malformed peer hello was accepted")
			}
		})
	}
	stagedHello := identity
	stagedHello.Phase = PeerPhaseStaged
	if _, err := encodeDynamicPeerHello(stagedHello, dynamicRaftMagic); err == nil {
		t.Fatal("STAGED identity was encoded as a Raft peer")
	}
	learnerHello := identity
	learnerHello.Phase = PeerPhaseLearner
	if _, err := encodeDynamicPeerHello(learnerHello, dynamicRaftMagic); err == nil {
		t.Fatal("learner hello without an operation ID was accepted")
	}
	learnerHello.OperationID = testOperationID
	learnerBytes, err := encodeDynamicPeerHello(learnerHello, dynamicRaftMagic)
	if err != nil {
		t.Fatalf("encode valid learner hello: %v", err)
	}
	if !bytes.Equal(learnerBytes[len(learnerBytes)-36:], []byte(testOperationID)) {
		t.Fatalf("learner operation ID is not exactly the trailing 36 ASCII bytes: %x", learnerBytes[len(learnerBytes)-36:])
	}
	if decoded, err := decodeDynamicPeerHello(learnerBytes, dynamicRaftMagic); err != nil || !samePeerIdentity(decoded, learnerHello) {
		t.Fatalf("decode learner hello: identity=%+v err=%v", decoded, err)
	}

	candidate := dynamicTestIdentity(4, "n:4", "n", PeerPhaseStaged, "", 0x22)
	request, err := encodeSeedAdmissionRequest(SeedAdmissionRequestDTO{Candidate: candidate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeSeedAdmissionRequest(append(request, 0)); err == nil {
		t.Fatal("trailing admission request byte was accepted")
	}
	rejected := rejectedSeedAdmissionResponse()
	wantRejected := []byte("NMQJOIN2\x00\x02\x00\x00\x00")
	if !bytes.Equal(rejected, wantRejected) {
		t.Fatalf("generic rejected response differs: got %x want %x", rejected, wantRejected)
	}
	assertDynamicFramedPayload(t, rejected, dynamicAdmissionLimit)
}

func TestDynamicHandshakeFrameLengthsAreBoundedBeforeAllocation(t *testing.T) {
	cases := []struct {
		name   string
		length uint32
		magic  string
	}{
		{name: "zero", length: 0},
		{name: "oversized admission", length: dynamicAdmissionLimit + 1},
		{name: "oversized raft hello", length: dynamicHelloLimit + 1, magic: "NMQRAFT2"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			reader, writer := net.Pipe()
			defer reader.Close()
			defer writer.Close()
			go func() {
				frame := []byte{byte(test.length >> 24), byte(test.length >> 16), byte(test.length >> 8), byte(test.length)}
				frame = append(frame, test.magic...)
				_, _ = writer.Write(frame)
			}()
			if _, err := readDynamicHandshakeFrame(reader); err == nil {
				t.Fatal("invalid handshake frame length was accepted")
			}
		})
	}
}

func TestDynamicPeerReconcileAndQueueBounds(t *testing.T) {
	serverTLS, clientTLS := testPeerTLSConfigs(t, "node1.example")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	local := identityForTestCertificate(1, "127.0.0.1:19001", "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS.Certificates[0])
	transport, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS,
		AdmissionHandler: func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			return SeedAdmissionResponseDTO{}, errors.New("not staged")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	peer := dynamicTestIdentity(4, "127.0.0.1:19004", "node4.example", PeerPhaseVoter, "", 0x44)
	peer.ReleaseVersion = local.ReleaseVersion
	if err := transport.ReconcilePeers([]PeerIdentityDTO{peer}); err != nil {
		t.Fatal(err)
	}
	old := transport.peers[peer.MemberID]
	for index := 0; index < peerQueueMessages; index++ {
		if err := transport.Send(PeerMessageDTO{From: local.MemberID, To: peer.MemberID, Data: []byte{1}}); err != nil {
			t.Fatalf("queue message %d: %v", index, err)
		}
	}
	if frame := old.takeOutbound(); frame == nil {
		t.Fatal("outbound writer had no queued frame")
	}
	if err := transport.Send(PeerMessageDTO{From: local.MemberID, To: peer.MemberID, Data: []byte{2}}); !errors.Is(err, ErrPeerBackpressure) {
		t.Fatalf("message-count queue bound: got %v", err)
	}
	status, err := transport.Status(peer.MemberID)
	if err != nil || status.OutboundQueuedMessages != peerQueueMessages || status.LastError != "outbound queue full" {
		t.Fatalf("queue count status: %+v err=%v", status, err)
	}
	old.recordError("peer identity mismatch")
	if err := transport.Send(PeerMessageDTO{From: local.MemberID, To: peer.MemberID, Data: []byte{3}}); !errors.Is(err, ErrPeerBackpressure) {
		t.Fatalf("full queue did not apply backpressure again: %v", err)
	}
	if status, err := transport.Status(peer.MemberID); err != nil || status.LastError != "peer identity mismatch" {
		t.Fatalf("queue backpressure hid the connection failure: %+v err=%v", status, err)
	}
	activeSocket, retiredSocket := net.Pipe()
	old.mu.Lock()
	old.connection = activeSocket
	old.mu.Unlock()
	defer retiredSocket.Close()
	if err := transport.Send(PeerMessageDTO{From: local.MemberID, To: peer.MemberID, Data: []byte{4}}); !errors.Is(err, ErrPeerBackpressure) {
		t.Fatalf("connected full queue: got %v", err)
	}
	if err := retiredSocket.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	writeResult := make(chan error, 1)
	go func() {
		_, err := activeSocket.Write([]byte{0x42})
		writeResult <- err
	}()
	var received [1]byte
	if _, err := io.ReadFull(retiredSocket, received[:]); err != nil {
		t.Fatalf("outbound backpressure closed the active connection: %v", err)
	}
	if err := <-writeResult; err != nil || received[0] != 0x42 {
		t.Fatalf("active connection probe: received=%x err=%v", received, err)
	}

	rebound := peer
	rebound.Address = "127.0.0.1:19005"
	if err := transport.ReconcilePeers([]PeerIdentityDTO{rebound}); err != nil {
		t.Fatal(err)
	}
	_ = retiredSocket.SetReadDeadline(time.Now().Add(time.Second))
	var probe [1]byte
	if _, err := retiredSocket.Read(probe[:]); !errors.Is(err, net.ErrClosed) &&
		!errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("rebound peer socket remained open: %v", err)
	}
	old.mu.Lock()
	oldStopped, oldMessages, oldBytes := old.stopped, old.outMessages, old.outBytes
	old.mu.Unlock()
	if !oldStopped || oldMessages != 0 || oldBytes != 0 {
		t.Fatalf("rebound queue was not retired: stopped=%v messages=%d bytes=%d", oldStopped, oldMessages, oldBytes)
	}
	for index := 0; index < 3; index++ {
		if err := transport.Send(PeerMessageDTO{From: local.MemberID, To: peer.MemberID, Data: make([]byte, dynamicFrameLimit)}); err != nil {
			t.Fatalf("queue byte-bound message %d: %v", index, err)
		}
	}
	reboundPeer := transport.peers[peer.MemberID]
	if frame := reboundPeer.takeOutbound(); frame == nil {
		t.Fatal("byte-bound writer had no queued frame")
	}
	if err := transport.Send(PeerMessageDTO{From: local.MemberID, To: peer.MemberID, Data: make([]byte, dynamicFrameLimit)}); !errors.Is(err, ErrPeerBackpressure) {
		t.Fatalf("framed-byte queue bound: got %v", err)
	}
	if err := transport.ReconcilePeers(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.Status(peer.MemberID); !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("removed peer remains registered: %v", err)
	}
	if err := transport.Send(PeerMessageDTO{From: local.MemberID, To: peer.MemberID, Data: []byte{1}}); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("removed peer accepted a message: %v", err)
	}
}

func TestDynamicSeedAdmissionRequiresTLSBoundCommittedStage(t *testing.T) {
	seedServerTLS, seedClientTLS := testPeerTLSConfigs(t, "seed.example")
	candidate4ServerTLS, candidate4ClientTLS := testPeerTLSConfigs(t, "node4.example")
	candidate5ServerTLS, candidate5ClientTLS := testPeerTLSConfigs(t, "node5.example")
	seedServerTLS.ClientCAs.AddCert(candidate4ServerTLS.Certificates[0].Leaf)
	seedServerTLS.ClientCAs.AddCert(candidate5ServerTLS.Certificates[0].Leaf)
	candidate4ClientTLS.RootCAs.AddCert(seedServerTLS.Certificates[0].Leaf)
	candidate5ClientTLS.RootCAs.AddCert(seedServerTLS.Certificates[0].Leaf)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	seedLocal := identityForTestCertificate(1, "127.0.0.1:19001", "seed.example", PeerPhaseVoter, "", "1.0.0", seedServerTLS.Certificates[0])
	voters := []PeerIdentityDTO{seedLocal}
	voters = append(voters,
		dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", 0x12),
		dynamicTestIdentity(3, "127.0.0.1:19003", "node3.example", PeerPhaseVoter, "", 0x13))
	for index := range voters {
		voters[index].ReleaseVersion = "1.0.0"
	}
	var responsePhase atomic.Uint32
	responsePhase.Store(uint32(PeerPhaseStaged))
	seed, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: seedLocal, Listener: listener, ServerTLS: seedServerTLS, ClientTLS: seedClientTLS,
		AdmissionHandler: func(_ context.Context, candidate PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			if candidate.MemberID != 4 || candidate.DNSSAN != "node4.example" {
				return SeedAdmissionResponseDTO{}, errors.New("no committed staged member")
			}
			committed := candidate
			committed.Phase = PeerPhaseDTO(responsePhase.Load())
			if committed.Phase != PeerPhaseVoter {
				committed.OperationID = testOperationID
			}
			return SeedAdmissionResponseDTO{Accepted: true, Candidate: committed, CurrentVoters: voters}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() { serveResult <- seed.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case <-serveResult:
		case <-time.After(time.Second):
			t.Error("seed transport did not stop")
		}
		_ = seed.Close()
	}()

	candidate4 := identityForTestCertificate(4, "127.0.0.1:19004", "node4.example", PeerPhaseStaged, "", "1.0.0", candidate4ServerTLS.Certificates[0])
	candidate4.ReleaseVersion = "1.1.0"
	candidate5 := identityForTestCertificate(5, "127.0.0.1:19005", "node5.example", PeerPhaseStaged, testOperationID5, "1.0.0", candidate5ServerTLS.Certificates[0])
	candidate4Transport := newAdmissionCandidateTransport(t, candidate4, candidate4ServerTLS, candidate4ClientTLS)
	defer candidate4Transport.Close()
	endpoints, err := candidate4Transport.RequestSeedAdmission(context.Background(), listener.Addr().String(), "seed.example")
	if err != nil {
		t.Fatalf("committed candidate admission failed: %v", err)
	}
	if len(endpoints) != 3 || endpoints[0].MemberID != 1 || endpoints[1].MemberID != 2 || endpoints[2].MemberID != 3 {
		t.Fatalf("voter endpoint list is not sorted: %+v", endpoints)
	}
	if candidate4Transport.local.OperationID != testOperationID || candidate4Transport.local.Phase != PeerPhaseStaged {
		t.Fatalf("admitted candidate identity = %+v, want committed staged operation %q", candidate4Transport.local, testOperationID)
	}
	if _, err := candidate4Transport.Status(endpoints[0].MemberID); err != nil {
		t.Fatalf("seed admission did not install returned voter peer: %v", err)
	}
	if err := candidate4Transport.Send(PeerMessageDTO{From: candidate4.MemberID, To: endpoints[0].MemberID, Data: []byte{1}}); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("STAGED member entered a Raft stream: %v", err)
	}
	for _, phase := range []PeerPhaseDTO{PeerPhaseLearner, PeerPhaseCatchingUp, PeerPhaseVoter} {
		responsePhase.Store(uint32(phase))
		if _, err := candidate4Transport.RequestSeedAdmission(context.Background(), listener.Addr().String(), "seed.example"); err != nil {
			t.Fatalf("refresh committed phase %d admission identity: %v", phase, err)
		}
		if candidate4Transport.local.Phase != phase {
			t.Fatalf("local admission phase = %d, want %d", candidate4Transport.local.Phase, phase)
		}
		if _, err := candidate4Transport.Status(endpoints[0].MemberID); err != nil {
			t.Fatalf("phase %d admission lost returned voter peer: %v", phase, err)
		}
		if phase == PeerPhaseVoter && candidate4Transport.local.OperationID != "" {
			t.Fatalf("stable voter retained operation ID %q", candidate4Transport.local.OperationID)
		}
	}
	if err := candidate4Transport.Send(PeerMessageDTO{From: candidate4.MemberID, To: endpoints[0].MemberID, Data: []byte{1}}); err != nil {
		t.Fatalf("committed learner accepted no Raft message: %v", err)
	}

	candidate5Transport := newAdmissionCandidateTransport(t, candidate5, candidate5ServerTLS, candidate5ClientTLS)
	defer candidate5Transport.Close()
	if _, err := candidate5Transport.RequestSeedAdmission(context.Background(), listener.Addr().String(), "seed.example"); err == nil {
		t.Fatal("candidate without a committed stage was admitted")
	}
}

// Traceability: CLU-02/CLU-07/CLU-14; ADR-0024; cluster spec §11.20.
func TestDynamicSeedAdmissionRejectsFreshVoterIdentity(t *testing.T) {
	seedServerTLS, seedClientTLS := testPeerTLSConfigs(t, "seed.example")
	candidateServerTLS, candidateClientTLS := testPeerTLSConfigs(t, "node4.example")
	seedServerTLS.ClientCAs.AddCert(candidateServerTLS.Certificates[0].Leaf)
	candidateClientTLS.RootCAs.AddCert(seedServerTLS.Certificates[0].Leaf)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	seedIdentity := identityForTestCertificate(1, "127.0.0.1:19001", "seed.example", PeerPhaseVoter, "", "1.0.0", seedServerTLS.Certificates[0])
	voters := []PeerIdentityDTO{
		seedIdentity,
		dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", 0x12),
		dynamicTestIdentity(3, "127.0.0.1:19003", "node3.example", PeerPhaseVoter, "", 0x13),
	}
	for index := range voters {
		voters[index].ReleaseVersion = "1.0.0"
	}
	seed, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: seedIdentity, Listener: listener, ServerTLS: seedServerTLS, ClientTLS: seedClientTLS,
		AdmissionHandler: func(_ context.Context, candidate PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			candidate.Phase = PeerPhaseVoter
			candidate.OperationID = ""
			return SeedAdmissionResponseDTO{Accepted: true, Candidate: candidate, CurrentVoters: voters}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() { serveResult <- seed.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case <-serveResult:
		case <-time.After(time.Second):
			t.Error("seed transport did not stop")
		}
		_ = seed.Close()
	}()

	candidate := identityForTestCertificate(4, "127.0.0.1:19004", "node4.example", PeerPhaseStaged, "", "1.0.0", candidateServerTLS.Certificates[0])
	transport := newAdmissionCandidateTransport(t, candidate, candidateServerTLS, candidateClientTLS)
	defer transport.Close()
	if endpoints, err := transport.RequestSeedAdmission(ctx, listener.Addr().String(), "seed.example"); err == nil {
		t.Errorf("fresh joiner accepted VOTER identity with endpoints %+v", endpoints)
	}
	if local := transport.LocalIdentity(); !samePeerIdentity(local, candidate) {
		t.Errorf("rejected seed changed local identity: got %+v, want %+v", local, candidate)
	}
	for _, voter := range voters {
		if status, err := transport.Status(voter.MemberID); err == nil {
			t.Errorf("rejected seed installed voter %d: %+v", voter.MemberID, status)
		}
	}
	if err := transport.Send(PeerMessageDTO{From: candidate.MemberID, To: seedIdentity.MemberID, Data: []byte{1}}); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Errorf("fresh joiner Raft send error=%v, want ErrInvalidPeerMessage", err)
	}
}

func TestDynamicJoinerAcceptCapacityExpandsAfterAdmission(t *testing.T) {
	names := []string{
		"node1.example", "node2.example", "node3.example", "node4.example", "node5.example",
	}
	serverTLS := make([]*tls.Config, len(names))
	clientTLS := make([]*tls.Config, len(names))
	for index, name := range names {
		serverTLS[index], clientTLS[index] = testPeerTLSConfigs(t, name)
	}
	localServerTLS := serverTLS[3]
	for index := 0; index < 3; index++ {
		localServerTLS.ClientCAs.AddCert(serverTLS[index].Certificates[0].Leaf)
		clientTLS[index].RootCAs.AddCert(localServerTLS.Certificates[0].Leaf)
	}
	localServerTLS.ClientCAs.AddCert(serverTLS[4].Certificates[0].Leaf)
	clientTLS[4].RootCAs.AddCert(localServerTLS.Certificates[0].Leaf)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	local := identityForTestCertificate(
		4, listener.Addr().String(), names[3], PeerPhaseStaged, testOperationID, "1.0.0",
		localServerTLS.Certificates[0],
	)
	voterAddresses := []string{"127.0.0.1:19001", "127.0.0.1:19002", "127.0.0.1:19003"}
	voters := make([]PeerIdentityDTO, 3)
	for index := range voters {
		voters[index] = identityForTestCertificate(
			uint64(index+1), voterAddresses[index], names[index], PeerPhaseVoter, "", "1.0.0",
			serverTLS[index].Certificates[0],
		)
	}
	candidate := identityForTestCertificate(
		5, "127.0.0.1:19005", names[4], PeerPhaseStaged, testOperationID5, "1.0.0",
		serverTLS[4].Certificates[0],
	)
	admissionStarted := make(chan struct{}, 1)
	admissionRelease := make(chan struct{})
	transport, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local, Listener: listener, ServerTLS: localServerTLS, ClientTLS: clientTLS[3],
		AdmissionHandler: func(ctx context.Context, requested PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			if requested.MemberID != candidate.MemberID {
				return SeedAdmissionResponseDTO{}, errors.New("unexpected admission candidate")
			}
			select {
			case admissionStarted <- struct{}{}:
			default:
			}
			select {
			case <-admissionRelease:
				return SeedAdmissionResponseDTO{}, errors.New("test admission released")
			case <-ctx.Done():
				return SeedAdmissionResponseDTO{}, ctx.Err()
			}
		},
	})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() { serveResult <- transport.Serve(ctx) }()
	var connections []net.Conn
	defer func() {
		close(admissionRelease)
		for _, connection := range connections {
			_ = connection.Close()
		}
		cancel()
		select {
		case serveErr := <-serveResult:
			if serveErr != nil {
				t.Errorf("Serve returned an error: %v", serveErr)
			}
		case <-time.After(2 * time.Second):
			t.Error("dynamic peer transport did not stop")
		}
		if err := transport.Close(); err != nil {
			t.Errorf("close dynamic peer transport: %v", err)
		}
	}()

	dialTLS := func(config *tls.Config) (net.Conn, error) {
		clientConfig := config.Clone()
		clientConfig.ServerName = local.DNSSAN
		dialCtx, dialCancel := context.WithTimeout(ctx, time.Second)
		defer dialCancel()
		return (&tls.Dialer{
			NetDialer: &net.Dialer{Timeout: time.Second},
			Config:    clientConfig,
		}).DialContext(dialCtx, "tcp", listener.Addr().String())
	}
	admissionConnection, err := dialTLS(clientTLS[4])
	if err != nil {
		t.Fatalf("dial initial admission connection: %v", err)
	}
	connections = append(connections, admissionConnection)
	admissionPayload, err := encodeSeedAdmissionRequest(SeedAdmissionRequestDTO{Candidate: candidate})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDynamicFrame(admissionConnection, admissionPayload, dynamicAdmissionLimit); err != nil {
		t.Fatalf("write initial admission request: %v", err)
	}
	select {
	case <-admissionStarted:
	case <-time.After(time.Second):
		t.Fatal("initial admission connection did not occupy an inbound slot")
	}

	admittedLocal := local
	admittedLocal.Phase = PeerPhaseCatchingUp
	if err := transport.updateAdmissionIdentity(admittedLocal, voters); err != nil {
		t.Fatalf("install admitted identity and voters: %v", err)
	}

	for index, voter := range voters {
		connection, dialErr := dialTLS(clientTLS[index])
		if dialErr != nil {
			t.Fatalf("dial voter %d after admission expanded the accept limit: %v", voter.MemberID, dialErr)
		}
		connections = append(connections, connection)
		if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("set voter %d handshake deadline: %v", voter.MemberID, err)
		}
		hello, err := encodeDynamicPeerHello(voter, dynamicRaftMagic)
		if err != nil {
			t.Fatalf("encode voter %d hello: %v", voter.MemberID, err)
		}
		if err := writeDynamicFrame(connection, hello, dynamicHelloLimit); err != nil {
			t.Fatalf("write voter %d hello: %v", voter.MemberID, err)
		}
		response, err := readDynamicFrame(connection, dynamicHelloLimit)
		if err != nil {
			t.Fatalf("read voter %d hello response: %v", voter.MemberID, err)
		}
		remote, err := decodeDynamicPeerHello(response, dynamicRaftMagic)
		if err != nil || !samePeerIdentity(remote, admittedLocal) {
			t.Fatalf("voter %d received local identity %+v, decode error=%v", voter.MemberID, remote, err)
		}
	}

	transport.mu.Lock()
	inboundConnections := transport.inboundConnections
	transport.mu.Unlock()
	if inboundConnections != 4 {
		t.Fatalf("active inbound connections = %d, want 3 voters plus the reserved admission slot", inboundConnections)
	}
}

func TestDynamicPeerRejectsTLSCertificateIdentityMismatchBeforeRaftDelivery(t *testing.T) {
	serverTLS1, clientTLS1 := testPeerTLSConfigs(t, "node1.example")
	serverTLS2, clientTLS2 := testPeerTLSConfigs(t, "node2.example")
	serverTLS1.ClientCAs.AddCert(serverTLS2.Certificates[0].Leaf)
	clientTLS1.RootCAs.AddCert(serverTLS2.Certificates[0].Leaf)
	serverTLS2.ClientCAs.AddCert(serverTLS1.Certificates[0].Leaf)
	clientTLS2.RootCAs.AddCert(serverTLS1.Certificates[0].Leaf)

	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listener1.Close()
		t.Fatal(err)
	}
	local1 := identityForTestCertificate(1, listener1.Addr().String(), "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS1.Certificates[0])
	local2 := identityForTestCertificate(2, listener2.Addr().String(), "node2.example", PeerPhaseVoter, "", "1.0.0", serverTLS2.Certificates[0])
	wrongNode2 := local2
	wrongNode2.CertificateSHA256[0] ^= 0xff
	newRejectingHandler := func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
		return SeedAdmissionResponseDTO{}, errors.New("not staged")
	}
	transport1, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local1, Listener: listener1, ServerTLS: serverTLS1, ClientTLS: clientTLS1,
		Peers: []PeerIdentityDTO{wrongNode2}, AdmissionHandler: newRejectingHandler,
	})
	if err != nil {
		t.Fatal(err)
	}
	transport2, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local2, Listener: listener2, ServerTLS: serverTLS2, ClientTLS: clientTLS2,
		Peers: []PeerIdentityDTO{local1}, AdmissionHandler: newRejectingHandler,
	})
	if err != nil {
		_ = transport1.Close()
		t.Fatal(err)
	}
	if err := transport1.Send(PeerMessageDTO{From: local1.MemberID, To: local2.MemberID, Data: []byte{1, 2, 3}}); err != nil {
		t.Fatalf("queue message for mismatched peer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result1 := make(chan error, 1)
	result2 := make(chan error, 1)
	go func() { result1 <- transport1.Serve(ctx) }()
	go func() { result2 <- transport2.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case <-result1:
		case <-time.After(time.Second):
			t.Error("first peer transport did not stop")
		}
		select {
		case <-result2:
		case <-time.After(time.Second):
			t.Error("second peer transport did not stop")
		}
		_ = transport1.Close()
		_ = transport2.Close()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status, statusErr := transport1.Status(2)
		if statusErr == nil && status.LastError == "peer identity mismatch" {
			if status.Connected {
				t.Fatalf("mismatched remote was marked connected: %+v", status)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, err := transport1.Status(2)
	if err != nil || status.LastError != "peer identity mismatch" || status.Connected {
		t.Fatalf("TLS-bound identity mismatch was not rejected: status=%+v err=%v", status, err)
	}
	select {
	case _, open := <-transport2.InboundReady():
		if !open {
			t.Fatal("dynamic peer transport closed before identity-rejection check")
		}
		if delivery, received := transport2.TakeInbound(); received {
			message := delivery.Message
			delivery.Complete()
			t.Fatalf("Raft payload crossed an unauthenticated hello: %+v", message)
		}
	case <-time.After(150 * time.Millisecond):
	}
}

// Traceability: PRD CLU-02/SEC-09; docs/specs/cluster-session-and-delivery.md §11.5 (Compatibility and identity rejection).
func TestDynamicPeerRejectsForeignCAAndSANAndClusterMismatchBeforeRaftDelivery(t *testing.T) {
	t.Run("foreign CA rejected at TLS handshake before delivery", func(t *testing.T) {
		serverTLS2, clientTLS2 := testPeerTLSConfigs(t, "node2.example")
		foreignServerTLS, foreignClientTLS := testPeerTLSConfigs(t, "foreign.example")
		// Foreign client trusts server 2 so the failure isolates the server rejecting the foreign client CA.
		foreignClientTLS.RootCAs.AddCert(serverTLS2.Certificates[0].Leaf)

		listener2, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener2.Close()

		foreignListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer foreignListener.Close()

		local2 := identityForTestCertificate(2, listener2.Addr().String(), "node2.example", PeerPhaseVoter, "", "1.0.0", serverTLS2.Certificates[0])
		foreignLocal := identityForTestCertificate(1, foreignListener.Addr().String(), "foreign.example", PeerPhaseVoter, "", "1.0.0", foreignServerTLS.Certificates[0])

		rejectingHandler := func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			return SeedAdmissionResponseDTO{}, errors.New("not staged")
		}

		transport2, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
			LocalIdentity: local2, Listener: listener2, ServerTLS: serverTLS2, ClientTLS: clientTLS2,
			Peers: []PeerIdentityDTO{foreignLocal}, AdmissionHandler: rejectingHandler,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer transport2.Close()

		// foreignLocal has ID 1 < 2, so transportForeign dials transport2.
		transportForeign, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
			LocalIdentity: foreignLocal, Listener: foreignListener, ServerTLS: foreignServerTLS, ClientTLS: foreignClientTLS,
			Peers: []PeerIdentityDTO{local2}, AdmissionHandler: rejectingHandler,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer transportForeign.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = transport2.Serve(ctx) }()
		go func() { _ = transportForeign.Serve(ctx) }()

		_ = transportForeign.Send(PeerMessageDTO{From: 1, To: 2, Data: []byte("foreign-ca-payload")})

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			statusForeign, errForeign := transportForeign.Status(2)
			if errForeign == nil && statusForeign.LastError != "" && !statusForeign.Connected {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		statusForeign, err := transportForeign.Status(2)
		if err != nil || statusForeign.Connected || statusForeign.LastError == "" {
			t.Fatalf("foreign transport did not record server rejection of foreign CA: status=%+v err=%v", statusForeign, err)
		}
		status2, err := transport2.Status(1)
		if err != nil || status2.Connected {
			t.Fatalf("server transport connected unexpectedly: status=%+v err=%v", status2, err)
		}

		select {
		case _, open := <-transport2.InboundReady():
			if open {
				if delivery, received := transport2.TakeInbound(); received {
					delivery.Complete()
					t.Fatalf("foreign CA message reached transport2 inbound: %+v", delivery.Message)
				}
			}
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("SAN mismatch rejected before delivery", func(t *testing.T) {
		serverTLS1, clientTLS1 := testPeerTLSConfigs(t, "node1.example")
		serverTLS2, clientTLS2 := testPeerTLSConfigs(t, "attacker.example")
		serverTLS1.ClientCAs.AddCert(serverTLS2.Certificates[0].Leaf)
		clientTLS1.RootCAs.AddCert(serverTLS2.Certificates[0].Leaf)
		serverTLS2.ClientCAs.AddCert(serverTLS1.Certificates[0].Leaf)
		clientTLS2.RootCAs.AddCert(serverTLS1.Certificates[0].Leaf)

		listener1, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener1.Close()
		listener2, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener2.Close()

		local1 := identityForTestCertificate(1, listener1.Addr().String(), "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS1.Certificates[0])
		expectedPeer2 := identityForTestCertificate(2, listener2.Addr().String(), "node2.example", PeerPhaseVoter, "", "1.0.0", serverTLS2.Certificates[0])
		expectedPeer2.DNSSAN = "node2.example"

		local2 := identityForTestCertificate(2, listener2.Addr().String(), "attacker.example", PeerPhaseVoter, "", "1.0.0", serverTLS2.Certificates[0])

		rejectingHandler := func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			return SeedAdmissionResponseDTO{}, errors.New("not staged")
		}
		transport1, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
			LocalIdentity: local1, Listener: listener1, ServerTLS: serverTLS1, ClientTLS: clientTLS1,
			Peers: []PeerIdentityDTO{expectedPeer2}, AdmissionHandler: rejectingHandler,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer transport1.Close()
		transport2, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
			LocalIdentity: local2, Listener: listener2, ServerTLS: serverTLS2, ClientTLS: clientTLS2,
			Peers: []PeerIdentityDTO{local1}, AdmissionHandler: rejectingHandler,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer transport2.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = transport1.Serve(ctx) }()
		go func() { _ = transport2.Serve(ctx) }()

		_ = transport2.Send(PeerMessageDTO{From: 2, To: 1, Data: []byte("unauthorized")})

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			status, err := transport1.Status(2)
			if err == nil && (status.LastError == "peer identity mismatch" || status.LastError == "peer TLS handshake failed" || status.LastError == "peer handshake failed") {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		select {
		case _, open := <-transport1.InboundReady():
			if open {
				if delivery, received := transport1.TakeInbound(); received {
					delivery.Complete()
					t.Fatalf("SAN-mismatched message reached transport inbound: %+v", delivery.Message)
				}
			}
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("cluster ID mismatch rejected before delivery", func(t *testing.T) {
		serverTLS1, clientTLS1 := testPeerTLSConfigs(t, "node1.example")
		serverTLS2, clientTLS2 := testPeerTLSConfigs(t, "node2.example")
		serverTLS1.ClientCAs.AddCert(serverTLS2.Certificates[0].Leaf)
		clientTLS1.RootCAs.AddCert(serverTLS2.Certificates[0].Leaf)
		serverTLS2.ClientCAs.AddCert(serverTLS1.Certificates[0].Leaf)
		clientTLS2.RootCAs.AddCert(serverTLS1.Certificates[0].Leaf)

		listener1, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener1.Close()
		listener2, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener2.Close()

		local1 := identityForTestCertificate(1, listener1.Addr().String(), "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS1.Certificates[0])
		local1.ClusterID = "cluster-alpha"
		local2 := identityForTestCertificate(2, listener2.Addr().String(), "node2.example", PeerPhaseVoter, "", "1.0.0", serverTLS2.Certificates[0])
		local2.ClusterID = "cluster-beta"

		rejectingHandler := func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			return SeedAdmissionResponseDTO{}, errors.New("not staged")
		}
		peer2For1 := local2
		peer2For1.ClusterID = local1.ClusterID

		transport1, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
			LocalIdentity: local1, Listener: listener1, ServerTLS: serverTLS1, ClientTLS: clientTLS1,
			Peers: []PeerIdentityDTO{peer2For1}, AdmissionHandler: rejectingHandler,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer transport1.Close()

		peer1For2 := local1
		peer1For2.ClusterID = local2.ClusterID

		transport2, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
			LocalIdentity: local2, Listener: listener2, ServerTLS: serverTLS2, ClientTLS: clientTLS2,
			Peers: []PeerIdentityDTO{peer1For2}, AdmissionHandler: rejectingHandler,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer transport2.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = transport1.Serve(ctx) }()
		go func() { _ = transport2.Serve(ctx) }()

		_ = transport2.Send(PeerMessageDTO{From: 2, To: 1, Data: []byte("cluster-mismatch-data")})

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			status, err := transport1.Status(2)
			if err == nil && status.LastError != "" && !status.Connected {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		select {
		case _, open := <-transport1.InboundReady():
			if open {
				if delivery, received := transport1.TakeInbound(); received {
					delivery.Complete()
					t.Fatalf("cluster-mismatched message reached transport inbound: %+v", delivery.Message)
				}
			}
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("malformed hello header rejected before delivery", func(t *testing.T) {
		serverTLS1, clientTLS1 := testPeerTLSConfigs(t, "node1.example")
		clientTLS2Config, _ := testPeerTLSConfigs(t, "node2.example")
		serverTLS1.ClientCAs.AddCert(clientTLS2Config.Certificates[0].Leaf)
		clientTLS1.RootCAs.AddCert(clientTLS2Config.Certificates[0].Leaf)

		listener1, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener1.Close()

		local1 := identityForTestCertificate(1, listener1.Addr().String(), "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS1.Certificates[0])
		peer2 := identityForTestCertificate(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", "1.0.0", clientTLS2Config.Certificates[0])

		transport1, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
			LocalIdentity: local1, Listener: listener1, ServerTLS: serverTLS1, ClientTLS: clientTLS1,
			Peers: []PeerIdentityDTO{peer2},
			AdmissionHandler: func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
				return SeedAdmissionResponseDTO{}, errors.New("not staged")
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer transport1.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _ = transport1.Serve(ctx) }()

		tlsClientConfig := clientTLS2Config.Clone()
		tlsClientConfig.ServerName = "node1.example"
		tlsClientConfig.RootCAs = clientTLS1.RootCAs
		conn, err := tls.Dial("tcp", listener1.Addr().String(), tlsClientConfig)
		if err == nil {
			_, _ = conn.Write([]byte("GARBAGE_HELLO_BYTES_000000000000000000000000"))
			_ = conn.Close()
		}

		select {
		case _, open := <-transport1.InboundReady():
			if open {
				if delivery, received := transport1.TakeInbound(); received {
					delivery.Complete()
					t.Fatalf("malformed hello message reached transport inbound: %+v", delivery.Message)
				}
			}
		case <-time.After(100 * time.Millisecond):
		}
	})
}

func TestDynamicPeerTransportPreservesQueuedFramesAcrossPhaseChange(t *testing.T) {
	serverTLS1, clientTLS1 := testPeerTLSConfigs(t, "node1.example")
	serverTLS2, clientTLS2 := testPeerTLSConfigs(t, "node2.example")
	serverTLS1.ClientCAs.AddCert(serverTLS2.Certificates[0].Leaf)
	clientTLS1.RootCAs.AddCert(serverTLS2.Certificates[0].Leaf)
	serverTLS2.ClientCAs.AddCert(serverTLS1.Certificates[0].Leaf)
	clientTLS2.RootCAs.AddCert(serverTLS1.Certificates[0].Leaf)

	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listener1.Close()
		t.Fatal(err)
	}
	local1 := identityForTestCertificate(
		1, listener1.Addr().String(), "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS1.Certificates[0],
	)
	local2 := identityForTestCertificate(
		2, listener2.Addr().String(), "node2.example", PeerPhaseCatchingUp, testOperationID, "1.0.0", serverTLS2.Certificates[0],
	)
	learner2 := local2
	learner2.Phase = PeerPhaseLearner
	newRejectingHandler := func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
		return SeedAdmissionResponseDTO{}, errors.New("candidate is not a seed")
	}
	transport1, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local1, Listener: listener1, ServerTLS: serverTLS1, ClientTLS: clientTLS1,
		Peers: []PeerIdentityDTO{learner2}, AdmissionHandler: newRejectingHandler,
	})
	if err != nil {
		t.Fatal(err)
	}
	transport2, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local2, Listener: listener2, ServerTLS: serverTLS2, ClientTLS: clientTLS2,
		Peers: []PeerIdentityDTO{local1}, AdmissionHandler: newRejectingHandler,
	})
	if err != nil {
		_ = transport1.Close()
		t.Fatal(err)
	}
	defer transport1.Close()
	defer transport2.Close()

	payload := []byte{0x08, 0x01, 0x12, 0x01, 0x7f}
	if err := transport1.Send(PeerMessageDTO{From: local1.MemberID, To: local2.MemberID, Data: payload}); err != nil {
		t.Fatalf("queue Raft frame before serving: %v", err)
	}
	if err := transport1.ReconcilePeers([]PeerIdentityDTO{local2}); err != nil {
		t.Fatalf("reconcile committed phase change: %v", err)
	}
	status, err := transport1.Status(local2.MemberID)
	if err != nil || status.OutboundQueuedMessages != 1 {
		t.Fatalf("queued frame was discarded during phase update: status=%+v err=%v", status, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveResults := make(chan error, 2)
	go func() { serveResults <- transport1.Serve(ctx) }()
	go func() { serveResults <- transport2.Serve(ctx) }()
	defer func() {
		cancel()
		for range 2 {
			select {
			case <-serveResults:
			case <-time.After(time.Second):
				t.Error("peer transport did not stop")
			}
		}
	}()

	message := takeDynamicPeerMessage(t, transport2)
	if message.From != local1.MemberID || message.To != local2.MemberID || !bytes.Equal(message.Data, payload) {
		t.Fatalf("received peer message = %+v, want from=%d to=%d data=%x",
			message, local1.MemberID, local2.MemberID, payload)
	}
	deadline := time.Now().Add(time.Second)
	for {
		status, err := transport1.Status(local2.MemberID)
		if err != nil {
			t.Fatalf("read outbound queue status after delivery: %v", err)
		}
		if status.OutboundQueuedMessages == 0 {
			if status.OutboundQueuedBytes != 0 {
				t.Fatalf("delivered frame left %d outbound queued bytes", status.OutboundQueuedBytes)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivered frame remained queued: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDynamicPeerPhaseRefreshPreservesInProgressOutboundFrame(t *testing.T) {
	local := dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11)
	remote := dynamicTestIdentity(2, "n:2", "n2", PeerPhaseLearner, testOperationID, 0x22)
	transport := &DynamicTCPPeerTransport{
		local: local, localID: local.MemberID,
		peers: make(map[uint64]*dynamicPeerState),
	}
	peer := newDynamicPeerState(transport, remote)
	transport.peers[remote.MemberID] = peer
	defer peer.stop()

	connection, remoteConnection := net.Pipe()
	defer remoteConnection.Close()
	peer.mu.Lock()
	peer.connection = connection
	peer.mu.Unlock()

	payload := []byte{0x08, 0x01, 0x12, 0x01, 0x7f}
	if err := transport.Send(PeerMessageDTO{From: local.MemberID, To: remote.MemberID, Data: payload}); err != nil {
		t.Fatalf("queue outbound Raft frame: %v", err)
	}
	inProgress := peer.takeOutbound()
	if inProgress == nil {
		t.Fatal("outbound frame did not become the in-progress frame")
	}

	updated := remote
	updated.Phase = PeerPhaseCatchingUp
	transport.mu.Lock()
	retired, refreshedConnections := transport.replacePeersLocked([]PeerIdentityDTO{updated})
	transport.mu.Unlock()
	if len(retired) != 0 || len(refreshedConnections) != 1 {
		t.Fatalf("phase refresh changed peer or active connection: retired=%d connections=%d",
			len(retired), len(refreshedConnections))
	}
	for _, refreshed := range refreshedConnections {
		_ = refreshed.Close()
	}

	peer.mu.Lock()
	preserved := peer.outCurrent == inProgress
	messages, framedBytes := peer.outMessages, peer.outBytes
	peer.mu.Unlock()
	if !preserved || messages != 1 || framedBytes != len(payload)+4 {
		t.Fatalf("phase refresh lost in-progress outbound frame: preserved=%t messages=%d bytes=%d",
			preserved, messages, framedBytes)
	}
	if retried := peer.takeOutbound(); retried != inProgress {
		t.Fatalf("reconnected writer received frame %p, want retained frame %p", retried, inProgress)
	}
	peer.completeOutbound(inProgress)
	peer.mu.Lock()
	messages, framedBytes = peer.outMessages, peer.outBytes
	peer.mu.Unlock()
	if messages != 0 || framedBytes != 0 {
		t.Fatalf("completed in-progress frame left queue accounting: messages=%d bytes=%d", messages, framedBytes)
	}
}

// Traceability: CLU-02/CLU-14; docs/specs/cluster-session-and-delivery.md §11.5.
func TestDynamicSendRejectsStagedForwardingAndStoppedPeers(t *testing.T) {
	staged := dynamicTestIdentity(1, "n:1", "n1", PeerPhaseStaged, testOperationID, 0x11)
	remote := dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22)
	transport := &DynamicTCPPeerTransport{
		local: staged, localID: staged.MemberID,
		forwardPeers: make(map[uint64]*dynamicPeerState),
	}
	forwardPeer := newDynamicLanePeerState(transport, remote, dynamicLaneForward)
	transport.forwardPeers[remote.MemberID] = forwardPeer
	defer forwardPeer.stop()
	if err := transport.Forward(PeerMessageDTO{
		From: staged.MemberID, To: remote.MemberID, Data: []byte{1},
	}); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("staged member Forward() error = %v, want invalid peer message", err)
	}

	voter := staged
	voter.Phase = PeerPhaseVoter
	voter.OperationID = ""
	transport.local = voter
	peer := newDynamicPeerState(transport, remote)
	transport.peers = map[uint64]*dynamicPeerState{remote.MemberID: peer}
	peer.stop()
	if err := transport.Send(PeerMessageDTO{
		From: voter.MemberID, To: remote.MemberID, Data: []byte{1},
	}); !errors.Is(err, ErrPeerTransportClosed) {
		t.Fatalf("Send() to stopped peer error = %v, want transport closed", err)
	}
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.2, §11.28.
func TestDynamicPeerReclaimUnsentReturnsOnlyUnwrittenProposals(t *testing.T) {
	local := dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11)
	remote := dynamicTestIdentity(2, "n:2", "n2", PeerPhaseVoter, "", 0x22)
	transport := &DynamicTCPPeerTransport{
		local: local, localID: local.MemberID,
		peers: make(map[uint64]*dynamicPeerState),
	}
	peer := newDynamicPeerState(transport, remote)
	transport.peers[remote.MemberID] = peer
	defer peer.stop()
	for _, marker := range []byte{'A', 'B', 'C', 'D'} {
		if err := transport.Send(PeerMessageDTO{
			From: local.MemberID, To: remote.MemberID, Data: []byte{marker},
			Reclaimable: marker != 'B',
		}); err != nil {
			t.Fatal(err)
		}
	}
	before, err := transport.Status(remote.MemberID)
	if err != nil {
		t.Fatal(err)
	}
	inProgress := peer.takeOutbound()
	if inProgress == nil || !bytes.Equal(inProgress.data, []byte{'A'}) {
		t.Fatalf("in-progress frame = %v, want A", inProgress)
	}
	frames, err := transport.ReclaimUnsent(remote.MemberID, 1, 1<<20)
	if err != nil || len(frames) != 1 || !bytes.Equal(frames[0], []byte{'C'}) {
		t.Fatalf("first reclaim = %q, %v, want [C]", frames, err)
	}
	after, err := transport.Status(remote.MemberID)
	if err != nil || after.OutboundQueuedMessages != 3 || after.OutboundQueuedBytes != before.OutboundQueuedBytes-5 {
		t.Fatalf("reclaimed accounting = %+v, %v; before = %+v", after, err, before)
	}
	frames, err = transport.ReclaimUnsent(remote.MemberID, 1, 0)
	if err != nil || len(frames) != 0 {
		t.Fatalf("zero byte credit reclaimed frame: %q, %v", frames, err)
	}
	frames, err = transport.ReclaimUnsent(remote.MemberID, 1, 1<<20)
	if err != nil || len(frames) != 1 || !bytes.Equal(frames[0], []byte{'D'}) {
		t.Fatalf("second reclaim = %q, %v, want [D]", frames, err)
	}
	if retried := peer.takeOutbound(); retried != inProgress {
		t.Fatal("reclaim removed or replaced in-progress A")
	}
	peer.completeOutbound(inProgress)
	ordinary := peer.takeOutbound()
	if ordinary == nil || !bytes.Equal(ordinary.data, []byte{'B'}) {
		t.Fatalf("next outbound = %v, want B", ordinary)
	}
	peer.completeOutbound(ordinary)
	if err := transport.Send(PeerMessageDTO{
		From: local.MemberID, To: remote.MemberID, Data: []byte{'E'}, Reclaimable: true,
	}); err != nil {
		t.Fatal(err)
	}
	connection, remoteConnection := net.Pipe()
	defer remoteConnection.Close()
	peer.mu.Lock()
	peer.connection = connection
	peer.mu.Unlock()
	frames, err = transport.ReclaimUnsent(remote.MemberID, 1, 1<<20)
	if err != nil || frames != nil {
		t.Fatalf("connected peer reclaim = %q, %v, want nil", frames, err)
	}
	frames, err = transport.ReclaimUnsent(99, 1, 1<<20)
	if frames != nil || !errors.Is(err, ErrUnknownPeer) {
		t.Fatalf("unknown peer reclaim = %q, %v", frames, err)
	}
}

func TestDynamicPeerTransportExchangesRaftFrames(t *testing.T) {
	serverTLS1, clientTLS1 := testPeerTLSConfigs(t, "node1.example")
	serverTLS2, clientTLS2 := testPeerTLSConfigs(t, "node2.example")
	serverTLS1.ClientCAs.AddCert(serverTLS2.Certificates[0].Leaf)
	clientTLS1.RootCAs.AddCert(serverTLS2.Certificates[0].Leaf)
	serverTLS2.ClientCAs.AddCert(serverTLS1.Certificates[0].Leaf)
	clientTLS2.RootCAs.AddCert(serverTLS1.Certificates[0].Leaf)

	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listener1.Close()
		t.Fatal(err)
	}
	local1 := identityForTestCertificate(1, listener1.Addr().String(), "node1.example",
		PeerPhaseVoter, "", "1.0.0", serverTLS1.Certificates[0])
	local2 := identityForTestCertificate(2, listener2.Addr().String(), "node2.example",
		PeerPhaseVoter, "", "1.0.0", serverTLS2.Certificates[0])
	newRejectingHandler := func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
		return SeedAdmissionResponseDTO{}, errors.New("candidate is not a seed")
	}
	transport1, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local1, Listener: listener1, ServerTLS: serverTLS1, ClientTLS: clientTLS1,
		Peers: []PeerIdentityDTO{local2}, AdmissionHandler: newRejectingHandler,
	})
	if err != nil {
		t.Fatal(err)
	}
	transport2, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local2, Listener: listener2, ServerTLS: serverTLS2, ClientTLS: clientTLS2,
		Peers: []PeerIdentityDTO{local1}, AdmissionHandler: newRejectingHandler,
	})
	if err != nil {
		_ = transport1.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveResults := make(chan error, 2)
	go func() { serveResults <- transport1.Serve(ctx) }()
	go func() { serveResults <- transport2.Serve(ctx) }()
	defer func() {
		cancel()
		for range 2 {
			select {
			case <-serveResults:
			case <-time.After(time.Second):
				t.Error("peer transport did not stop")
			}
		}
		_ = transport1.Close()
		_ = transport2.Close()
	}()

	payload := []byte{0x08, 0x01, 0x12, 0x01, 0x7f}
	if err := transport1.Send(PeerMessageDTO{From: local1.MemberID, To: local2.MemberID, Data: payload}); err != nil {
		t.Fatalf("queue Raft frame: %v", err)
	}
	message := takeDynamicPeerMessage(t, transport2)
	if message.From != local1.MemberID || message.To != local2.MemberID || !bytes.Equal(message.Data, payload) {
		t.Fatalf("received peer message = %+v, want from=%d to=%d data=%x",
			message, local1.MemberID, local2.MemberID, payload)
	}
}

func TestDynamicPeerTransportServeCancellation(t *testing.T) {
	serverTLS, clientTLS := testPeerTLSConfigs(t, "node1.example")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	local := identityForTestCertificate(1, "127.0.0.1:19001", "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS.Certificates[0])
	transport, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS,
		AdmissionHandler: func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			return SeedAdmissionResponseDTO{}, errors.New("not staged")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- transport.Serve(ctx) }()
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Serve after cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after cancellation")
	}
	if _, open := <-transport.InboundReady(); open {
		t.Fatal("InboundReady remained open after transport cancellation")
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func newAdmissionCandidateTransport(
	t *testing.T,
	identity PeerIdentityDTO,
	serverTLS *tls.Config,
	clientTLS *tls.Config,
) *DynamicTCPPeerTransport {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transport, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: identity, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS,
		AdmissionHandler: func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			return SeedAdmissionResponseDTO{}, errors.New("candidate is not a seed")
		},
	})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	return transport
}

func identityForTestCertificate(
	memberID uint64,
	address string,
	dnsName string,
	phase PeerPhaseDTO,
	operationID string,
	release string,
	certificate tls.Certificate,
) PeerIdentityDTO {
	identity := dynamicTestIdentity(memberID, address, dnsName, phase, operationID, 0)
	identity.ReleaseVersion = release
	identity.CertificateSHA256 = sha256.Sum256(certificate.Leaf.Raw)
	return identity
}

func dynamicTestIdentity(
	memberID uint64,
	address string,
	dnsName string,
	phase PeerPhaseDTO,
	operationID string,
	fingerprintByte byte,
) PeerIdentityDTO {
	identity := PeerIdentityDTO{
		MemberID: memberID, ClusterID: "c", Address: address, DNSSAN: dnsName,
		ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, ClusterProtocolMinor: 0,
		StateSchemaMajor: 1, StateSchemaMinor: 0, Phase: phase, OperationID: operationID,
	}
	for index := range identity.CertificateSHA256 {
		identity.CertificateSHA256[index] = fingerprintByte
	}
	return identity
}

func TestDynamicPeerInboundQueueBoundsAndRetirement(t *testing.T) {
	identity := dynamicTestIdentity(2, "n:2", "n", PeerPhaseVoter, "", 0x12)
	peer := newDynamicPeerState(nil, identity)
	identityRevision := peer.identityRevision
	for range peerQueueMessages {
		if err := peer.enqueueInbound([]byte{1}, identityRevision); err != nil {
			t.Fatalf("queue inbound message: %v", err)
		}
	}
	if err := peer.enqueueInbound([]byte{2}, identityRevision); !errors.Is(err, ErrPeerBackpressure) {
		t.Fatalf("inbound message-count bound: got %v", err)
	}
	peer.drainInbound()
	largePayload := make([]byte, dynamicFrameLimit)
	for range 3 {
		if err := peer.enqueueInbound(largePayload, identityRevision); err != nil {
			t.Fatalf("queue inbound byte-bound message: %v", err)
		}
	}
	if err := peer.enqueueInbound(largePayload, identityRevision); !errors.Is(err, ErrPeerBackpressure) {
		t.Fatalf("inbound framed-byte bound: got %v", err)
	}
	peer.stop()
	peer.mu.Lock()
	messages, framedBytes := peer.inMessages, peer.inBytes
	peer.mu.Unlock()
	if messages != 0 || framedBytes != 0 {
		t.Fatalf("retired inbound queue was not cleared: messages=%d bytes=%d", messages, framedBytes)
	}
}

func TestDynamicPeerPhaseRefreshDiscardsInboundAndRejectsStaleFrames(t *testing.T) {
	local := dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11)
	remote := dynamicTestIdentity(2, "n:2", "n2", PeerPhaseLearner, testOperationID, 0x22)
	transport := &DynamicTCPPeerTransport{
		local: local, localID: local.MemberID,
		peers: make(map[uint64]*dynamicPeerState),
	}
	peer := newDynamicPeerState(transport, remote)
	transport.peers[remote.MemberID] = peer
	defer peer.stop()

	oldRevision := peer.identityRevision
	if err := peer.enqueueInbound([]byte{0x01}, oldRevision); err != nil {
		t.Fatalf("queue inbound frame under old phase: %v", err)
	}
	outbound := []byte{0x02}
	if err := transport.Send(PeerMessageDTO{From: local.MemberID, To: remote.MemberID, Data: outbound}); err != nil {
		t.Fatalf("queue outbound Raft frame: %v", err)
	}

	updated := remote
	updated.Phase = PeerPhaseCatchingUp
	transport.mu.Lock()
	retired, refreshedConnections := transport.replacePeersLocked([]PeerIdentityDTO{updated})
	transport.mu.Unlock()
	if len(retired) != 0 || len(refreshedConnections) != 0 {
		t.Fatalf("phase-only refresh retired peer or connection: retired=%d connections=%d", len(retired), len(refreshedConnections))
	}
	if err := peer.enqueueInbound([]byte{0x03}, oldRevision); !errors.Is(err, ErrPeerTransportClosed) {
		t.Fatalf("old identity revision admitted inbound frame: %v", err)
	}

	peer.mu.Lock()
	inboundMessages, inboundBytes := peer.inMessages, peer.inBytes
	outboundMessages, outboundBytes := peer.outMessages, peer.outBytes
	newRevision := peer.identityRevision
	peer.mu.Unlock()
	if newRevision != oldRevision+1 {
		t.Fatalf("identity revision=%d, want %d", newRevision, oldRevision+1)
	}
	if len(peer.inbox) != 0 || inboundMessages != 0 || inboundBytes != 0 {
		t.Fatalf("old-phase inbound queue was retained: queued=%d messages=%d bytes=%d",
			len(peer.inbox), inboundMessages, inboundBytes)
	}
	if outboundMessages != 1 || outboundBytes != len(outbound)+4 {
		t.Fatalf("outbound Raft queue was not preserved: messages=%d bytes=%d",
			outboundMessages, outboundBytes)
	}
}

func TestDynamicPeerPhaseRefreshWaitsForInboundDeliveryCompletion(t *testing.T) {
	local := dynamicTestIdentity(1, "n:1", "n1", PeerPhaseVoter, "", 0x11)
	remote := dynamicTestIdentity(2, "n:2", "n2", PeerPhaseLearner, testOperationID, 0x22)
	transport := &DynamicTCPPeerTransport{
		local: local, localID: local.MemberID,
		peers:        make(map[uint64]*dynamicPeerState),
		inboundReady: make(chan struct{}, 1),
	}
	peer := newDynamicPeerState(transport, remote)
	transport.peers[remote.MemberID] = peer
	defer peer.stop()

	oldRevision := peer.identityRevision
	for _, payload := range [][]byte{{0x08, 0x01}, {0x08, 0x02}} {
		if err := peer.enqueueInbound(payload, oldRevision); err != nil {
			t.Fatalf("queue inbound frame for old revision: %v", err)
		}
	}
	delivery, received := transport.TakeInbound()
	if !received || delivery.Message.From != remote.MemberID || delivery.Message.To != local.MemberID {
		t.Fatalf("take inbound delivery: received=%t message=%+v", received, delivery.Message)
	}

	updated := remote
	updated.Phase = PeerPhaseCatchingUp
	reconcileDone := make(chan error, 1)
	go func() {
		reconcileDone <- transport.ReconcilePeers([]PeerIdentityDTO{updated})
	}()
	deadline := time.Now().Add(time.Second)
	for {
		peer.mu.Lock()
		transitionPending := peer.identityChanging
		peer.mu.Unlock()
		if transitionPending {
			break
		}
		if time.Now().After(deadline) {
			delivery.Complete()
			<-reconcileDone
			t.Fatal("phase refresh did not begin the peer identity transition")
		}
		time.Sleep(time.Millisecond)
	}

	peer.mu.Lock()
	revisionDuringDelivery := peer.identityRevision
	transitionPending := peer.identityChanging
	deliveryActive := peer.inboundDelivering
	peer.mu.Unlock()
	if revisionDuringDelivery != oldRevision || !transitionPending || !deliveryActive {
		delivery.Complete()
		t.Fatalf("phase refresh crossed an outstanding delivery: revision=%d transition=%t active=%t",
			revisionDuringDelivery, transitionPending, deliveryActive)
	}
	select {
	case err := <-reconcileDone:
		delivery.Complete()
		t.Fatalf("phase refresh returned before delivery completion: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	delivery.Complete()
	select {
	case err := <-reconcileDone:
		if err != nil {
			t.Fatalf("reconcile phase update: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("phase refresh did not resume after delivery completion")
	}

	peer.mu.Lock()
	revisionAfterRefresh := peer.identityRevision
	deliveryActive = peer.inboundDelivering
	inboundMessages, inboundBytes := peer.inMessages, peer.inBytes
	peer.mu.Unlock()
	if revisionAfterRefresh != oldRevision+1 || deliveryActive || inboundMessages != 0 || inboundBytes != 0 {
		t.Fatalf("phase refresh left prior delivery state: revision=%d active=%t messages=%d bytes=%d",
			revisionAfterRefresh, deliveryActive, inboundMessages, inboundBytes)
	}
	if err := peer.enqueueInbound([]byte{0x08, 0x03}, oldRevision); !errors.Is(err, ErrPeerTransportClosed) {
		t.Fatalf("old-revision frame was admitted after refresh: %v", err)
	}
	if _, received := transport.TakeInbound(); received {
		t.Fatal("old-revision staged frame remained available after refresh")
	}
}

func TestDynamicPeerLocalIdentityUpdatesCommittedPhase(t *testing.T) {
	serverTLS, clientTLS := testPeerTLSConfigs(t, "node1.example")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	local := identityForTestCertificate(1, "127.0.0.1:19001", "node1.example", PeerPhaseLearner, testOperationID, "1.0.0", serverTLS.Certificates[0])
	transport, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS,
		AdmissionHandler: func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
			return SeedAdmissionResponseDTO{}, errors.New("not staged")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	connection, other := net.Pipe()
	if !transport.trackConnection(connection) {
		t.Fatal("track local peer connection")
	}
	defer func() {
		transport.untrackConnection(connection)
		_ = connection.Close()
		_ = other.Close()
	}()
	if err := transport.UpdateLocalIdentity(local); err != nil {
		t.Fatalf("apply unchanged local identity: %v", err)
	}
	if err := transport.updateAdmissionIdentity(local, nil); err != nil {
		t.Fatalf("apply unchanged seed admission identity: %v", err)
	}
	writeResult := make(chan error, 1)
	go func() {
		_, err := connection.Write([]byte{0x42})
		writeResult <- err
	}()
	var received [1]byte
	if _, err := io.ReadFull(other, received[:]); err != nil || received[0] != 0x42 {
		t.Fatalf("unchanged identity closed its peer connection: byte=%x err=%v", received[0], err)
	}
	if err := <-writeResult; err != nil {
		t.Fatalf("unchanged identity interrupted peer write: %v", err)
	}
	catchingUp := local
	catchingUp.Phase = PeerPhaseCatchingUp
	if err := transport.UpdateLocalIdentity(catchingUp); err != nil {
		t.Fatalf("apply committed catching-up phase: %v", err)
	}
	if err := transport.UpdateLocalIdentity(local); err != nil {
		t.Fatalf("ignore stale committed learner phase: %v", err)
	}
	if transport.local.Phase != PeerPhaseCatchingUp {
		t.Fatalf("stale committed phase regressed local identity to %d", transport.local.Phase)
	}
	staleConnection, staleOther := net.Pipe()
	if !transport.trackConnection(staleConnection) {
		t.Fatal("track peer connection for stale phase update")
	}
	defer func() {
		transport.untrackConnection(staleConnection)
		_ = staleConnection.Close()
		_ = staleOther.Close()
	}()
	writeResult = make(chan error, 1)
	go func() {
		_, err := staleConnection.Write([]byte{0x43})
		writeResult <- err
	}()
	if _, err := io.ReadFull(staleOther, received[:]); err != nil || received[0] != 0x43 {
		t.Fatalf("stale phase update closed peer connection: byte=%x err=%v", received[0], err)
	}
	if err := <-writeResult; err != nil {
		t.Fatalf("stale phase update interrupted peer write: %v", err)
	}
	if err := transport.updateAdmissionIdentity(local, nil); err == nil {
		t.Fatal("stale seed phase regressed committed local identity")
	}
	voter := catchingUp
	voter.Phase = PeerPhaseVoter
	voter.OperationID = ""
	if err := transport.UpdateLocalIdentity(voter); err != nil {
		t.Fatalf("apply committed voter phase: %v", err)
	}
}

func assertDynamicFramedPayload(t *testing.T, payload []byte, maximum uint32) {
	t.Helper()
	reader, writer := net.Pipe()
	defer reader.Close()
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- writeDynamicFrame(writer, payload, maximum)
		_ = writer.Close()
	}()
	frame := make([]byte, len(payload)+4)
	if _, err := io.ReadFull(reader, frame); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 4, len(payload)+4)
	binary.BigEndian.PutUint32(want, uint32(len(payload)))
	want = append(want, payload...)
	if !bytes.Equal(frame, want) {
		t.Fatalf("framed bytes differ\\n got: %x\\nwant: %x", frame, want)
	}
	if err := <-writeResult; err != nil {
		t.Fatal(err)
	}
}

func TestDynamicDialDropsConnectionWhenIdentityChangesDuringHandshake(t *testing.T) {
	n1 := cptNewNode(t, "node1.example")
	n2 := cptNewNode(t, "node2.example")
	cptTrustEachOther(n1, n2)
	dialListener := cptListen(t)
	transport := n1.newDynamic(t, cptListen(t), n1.identity(1, "127.0.0.1:19001", PeerPhaseVoter, ""),
		[]PeerIdentityDTO{n2.identity(2, dialListener.Addr().String(), PeerPhaseVoter, "")}, nil)
	cptServe(t, transport.Serve)

	listener := dialListener.(*net.TCPListener)
	if err := listener.SetDeadline(time.Now().Add(cptWaitTimeout)); err != nil {
		t.Fatalf("set dial listener deadline: %v", err)
	}
	sockets := make([]net.Conn, 0, 2)
	for len(sockets) < 2 {
		socket, err := listener.Accept()
		if err != nil {
			t.Fatalf("accept dial lane %d: %v", len(sockets)+1, err)
		}
		sockets = append(sockets, socket)
	}
	defer func() {
		for _, socket := range sockets {
			_ = socket.Close()
		}
	}()

	leaving := n2.identity(2, dialListener.Addr().String(), PeerPhaseLeaving, "00000000-0000-4000-8000-000000000031")
	if err := transport.ReconcilePeers([]PeerIdentityDTO{leaving}); err != nil {
		t.Fatalf("ReconcilePeers(Leaving) error = %v, want nil", err)
	}
	for index, socket := range sockets {
		tlsConn := tls.Server(socket, n2.serverTLS)
		if err := tlsConn.Handshake(); err != nil {
			t.Fatalf("TLS handshake lane %d: %v", index, err)
		}
		if err := tlsConn.SetReadDeadline(time.Now().Add(cptWaitTimeout)); err != nil {
			t.Fatalf("set lane %d read deadline: %v", index, err)
		}
		var payload [1]byte
		_, err := tlsConn.Read(payload[:])
		if !errors.Is(err, io.EOF) {
			t.Fatalf("lane %d read after identity change = %v, want %v", index, err, io.EOF)
		}
	}

	if err := listener.SetDeadline(time.Now().Add(cptWaitTimeout)); err != nil {
		t.Fatalf("set retry listener deadline: %v", err)
	}
	retry, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept retried dial: %v", err)
	}
	_ = retry.Close()
}

func TestDynamicDialSuppressesErrorWhenIdentityChangesBeforeHelloResponse(t *testing.T) {
	n1 := cptNewNode(t, "node1.example")
	n2 := cptNewNode(t, "node2.example")
	cptTrustEachOther(n1, n2)
	dialListener := cptListen(t)
	transport := n1.newDynamic(t, cptListen(t), n1.identity(1, "127.0.0.1:19001", PeerPhaseVoter, ""),
		[]PeerIdentityDTO{n2.identity(2, dialListener.Addr().String(), PeerPhaseVoter, "")}, nil)
	cptServe(t, transport.Serve)

	listener := dialListener.(*net.TCPListener)
	if err := listener.SetDeadline(time.Now().Add(cptWaitTimeout)); err != nil {
		t.Fatalf("set dial listener deadline: %v", err)
	}
	type lane struct {
		socket net.Conn
		tls    *tls.Conn
	}
	lanes := make([]lane, 0, 2)
	for len(lanes) < 2 {
		socket, err := listener.Accept()
		if err != nil {
			t.Fatalf("accept dial lane %d: %v", len(lanes)+1, err)
		}
		tlsConn := tls.Server(socket, n2.serverTLS)
		if err := tlsConn.Handshake(); err != nil {
			_ = socket.Close()
			t.Fatalf("TLS handshake lane %d: %v", len(lanes), err)
		}
		if err := tlsConn.SetReadDeadline(time.Now().Add(cptWaitTimeout)); err != nil {
			_ = socket.Close()
			t.Fatalf("set lane %d read deadline: %v", len(lanes), err)
		}
		if _, err := readDynamicFrame(tlsConn, dynamicHelloLimit); err != nil {
			_ = socket.Close()
			t.Fatalf("read lane %d hello: %v", len(lanes), err)
		}
		lanes = append(lanes, lane{socket: socket, tls: tlsConn})
	}
	defer func() {
		for _, lane := range lanes {
			_ = lane.socket.Close()
		}
	}()

	leaving := n2.identity(2, dialListener.Addr().String(), PeerPhaseLeaving, "00000000-0000-4000-8000-000000000031")
	if err := transport.ReconcilePeers([]PeerIdentityDTO{leaving}); err != nil {
		t.Fatalf("ReconcilePeers(Leaving) error = %v, want nil", err)
	}
	for index, lane := range lanes {
		var payload [1]byte
		if _, err := lane.tls.Read(payload[:]); err == nil {
			t.Fatalf("server read lane %d after identity change succeeded, want error", index)
		}
	}

	if err := listener.SetDeadline(time.Now().Add(cptWaitTimeout)); err != nil {
		t.Fatalf("set retry listener deadline: %v", err)
	}
	retry, err := listener.Accept()
	if err != nil {
		t.Fatalf("accept retried dial: %v", err)
	}
	_ = retry.Close()
	cptWaitUntil(t, "cleared peer LastError", func() bool {
		return cptDynamicStatus(cptDynamicPeer(transport, 2)).LastError == ""
	})
}
