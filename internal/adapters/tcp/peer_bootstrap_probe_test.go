package tcp

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestBootstrapProbeGoldenBytes(t *testing.T) {
	request := encodeBootstrapProbeRequest("c", 2)
	if got := hex.EncodeToString(request); got != "4e4d51424f4f5432000200000001630000000000000002" {
		t.Fatalf("request=%s", got)
	}
	clusterID, memberID, err := decodeBootstrapProbeRequest(request)
	if err != nil || clusterID != "c" || memberID != 2 {
		t.Fatalf("decoded request=(%q,%d,%v)", clusterID, memberID, err)
	}
	result := BootstrapProbeResultDTO{Status: BootstrapProbeProgressed, Term: 2, CommitIndex: 9}
	response := encodeBootstrapProbeResponse(result)
	if got := hex.EncodeToString(response); got != "4e4d51424f4f5432000200000300000000000000020000000000000009" {
		t.Fatalf("response=%s", got)
	}
	if got, err := decodeBootstrapProbeResponse(response); err != nil || got != result {
		t.Fatalf("decoded response=%+v err=%v", got, err)
	}
}

func TestBootstrapProbeRejectsInvalidWire(t *testing.T) {
	request := encodeBootstrapProbeRequest("c", 2)
	for _, payload := range [][]byte{request[:len(request)-1], append(bytes.Clone(request), 0), encodeBootstrapProbeRequest("", 2), encodeBootstrapProbeRequest("c", 0), encodeBootstrapProbeRequest(string(bytes.Repeat([]byte{'c'}, 201)), 2), encodeBootstrapProbeRequest("\xff", 2)} {
		if _, _, err := decodeBootstrapProbeRequest(payload); err == nil {
			t.Fatalf("accepted request %x", payload)
		}
	}
	response := encodeBootstrapProbeResponse(BootstrapProbeResultDTO{Status: BootstrapProbeReady, Term: 1, CommitIndex: 3})
	for _, index := range []int{0, 9, 11, 12} {
		invalid := bytes.Clone(response)
		invalid[index] = 0xff
		if _, err := decodeBootstrapProbeResponse(invalid); err == nil {
			t.Fatalf("accepted malformed response at %d", index)
		}
	}
	for _, payload := range [][]byte{response[:28], append(bytes.Clone(response), 0), encodeBootstrapProbeResponse(BootstrapProbeResultDTO{Status: BootstrapProbeEmpty, Term: 1}), encodeBootstrapProbeResponse(BootstrapProbeResultDTO{CommitIndex: 1})} {
		if _, err := decodeBootstrapProbeResponse(payload); err == nil || err.Error() != "bootstrap probe response is invalid" {
			t.Fatalf("response=%x err=%v", payload, err)
		}
	}
}

func TestServeBootstrapProbeAnswersConfiguredPeerOnly(t *testing.T) {
	server1, client1 := testPeerTLSConfigs(t, "node1.example")
	server2, client2 := testPeerTLSConfigs(t, "node2.example")
	server4, client4 := testPeerTLSConfigs(t, "node4.example")
	server1.ClientCAs.AddCert(server2.Certificates[0].Leaf)
	server1.ClientCAs.AddCert(server4.Certificates[0].Leaf)
	client2.RootCAs.AddCert(server1.Certificates[0].Leaf)
	client4.RootCAs.AddCert(server1.Certificates[0].Leaf)
	server2.ClientCAs.AddCert(server1.Certificates[0].Leaf)
	client1.RootCAs.AddCert(server2.Certificates[0].Leaf)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	local := identityForTestCertificate(1, listener.Addr().String(), "node1.example", PeerPhaseVoter, "", "1.0.0", server1.Certificates[0])
	peer := identityForTestCertificate(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", "1.0.0", server2.Certificates[0])
	unknown := identityForTestCertificate(4, "127.0.0.1:19004", "node4.example", PeerPhaseVoter, "", "1.0.0", server4.Certificates[0])
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ServeBootstrapProbe(ctx, listener, server1, local, []PeerIdentityDTO{peer}) }()
	got, err := ProbeBootstrapPeer(ctx, client2, peer, local)
	if err != nil || got != (BootstrapProbeResultDTO{Status: BootstrapProbeEmpty}) {
		t.Fatalf("configured peer=%+v err=%v", got, err)
	}
	if _, err := ProbeBootstrapPeer(ctx, client4, unknown, local); err == nil || err.Error() != "bootstrap probe rejected" {
		t.Fatalf("unknown peer err=%v", err)
	}
	impostor := unknown
	impostor.MemberID = peer.MemberID
	if _, err := ProbeBootstrapPeer(ctx, client4, impostor, local); err == nil || err.Error() != "bootstrap probe rejected" {
		t.Fatalf("wrong certificate err=%v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("probe server cancel=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("probe server did not cancel")
	}
	accepted := make(chan error, 1)
	go func() {
		socket, err := listener.Accept()
		if err == nil {
			_ = socket.Close()
		}
		accepted <- err
	}()
	socket, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("listener not reusable: %v", err)
	}
	_ = socket.Close()
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("accept after cancel: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listener accept after cancel stalled")
	}
}

func TestDynamicTransportBootstrapProbeAllowanceClearsOnRaftFrame(t *testing.T) {
	server1, client1 := testPeerTLSConfigs(t, "node1.example")
	server2, client2 := testPeerTLSConfigs(t, "node2.example")
	server1.ClientCAs.AddCert(server2.Certificates[0].Leaf)
	client1.RootCAs.AddCert(server2.Certificates[0].Leaf)
	server2.ClientCAs.AddCert(server1.Certificates[0].Leaf)
	client2.RootCAs.AddCert(server1.Certificates[0].Leaf)
	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listener1.Close()
		t.Fatal(err)
	}
	local1 := identityForTestCertificate(1, listener1.Addr().String(), "node1.example", PeerPhaseVoter, "", "1.0.0", server1.Certificates[0])
	local2 := identityForTestCertificate(2, listener2.Addr().String(), "node2.example", PeerPhaseVoter, "", "1.0.0", server2.Certificates[0])
	handler := func(context.Context, PeerIdentityDTO) (SeedAdmissionResponseDTO, error) {
		return SeedAdmissionResponseDTO{}, errors.New("not a seed")
	}
	transport1, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{LocalIdentity: local1, Listener: listener1, ServerTLS: server1, ClientTLS: client1, Peers: []PeerIdentityDTO{local2}, AdmissionHandler: handler})
	if err != nil {
		t.Fatal(err)
	}
	var initial atomic.Bool
	transport2, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{LocalIdentity: local2, Listener: listener2, ServerTLS: server2, ClientTLS: client2, Peers: []PeerIdentityDTO{local1}, AdmissionHandler: handler, BootstrapAllowedPeerIDs: []uint64{1}, BootstrapProgress: func() BootstrapProgressDTO {
		if initial.Load() {
			return BootstrapProgressDTO{Term: 1, CommitIndex: 3}
		}
		return BootstrapProgressDTO{Term: 2, CommitIndex: 4}
	}})
	if err != nil {
		_ = transport1.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	results := make(chan error, 2)
	go func() { results <- transport1.Serve(ctx) }()
	go func() { results <- transport2.Serve(ctx) }()
	defer func() {
		cancel()
		for range 2 {
			select {
			case err := <-results:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(time.Second):
				t.Error("transport did not stop")
			}
		}
		_ = transport1.Close()
		_ = transport2.Close()
	}()
	assertProbe := func(want BootstrapProbeResultDTO) {
		t.Helper()
		got, err := ProbeBootstrapPeer(ctx, client1, local1, local2)
		if err != nil || got != want {
			t.Fatalf("probe=%+v err=%v want=%+v", got, err, want)
		}
	}
	assertProbe(BootstrapProbeResultDTO{Status: BootstrapProbeReady, Term: 2, CommitIndex: 4})
	payload := []byte{0x08, 0x01, 0x12, 0x01, 0x7f}
	if err := transport1.Send(PeerMessageDTO{From: 1, To: 2, Data: payload}); err != nil {
		t.Fatal(err)
	}
	message := takeDynamicPeerMessage(t, transport2)
	if !bytes.Equal(message.Data, payload) {
		t.Fatalf("Raft message=%x", message.Data)
	}
	assertProbe(BootstrapProbeResultDTO{Status: BootstrapProbeProgressed, Term: 2, CommitIndex: 4})
	initial.Store(true)
	assertProbe(BootstrapProbeResultDTO{Status: BootstrapProbeReady, Term: 1, CommitIndex: 3})
	if _, err := ProbeBootstrapPeer(ctx, client2, local2, local1); err == nil || err.Error() != "bootstrap probe rejected" {
		t.Fatalf("nil progress handler err=%v", err)
	}
}
