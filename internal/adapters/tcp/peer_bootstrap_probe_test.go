package tcp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-125
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

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-125
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

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-125
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

// TestServeBootstrapProbeDropsFailedHandshakeAndBadMagic verifies the bootstrap probe server drops a failed handshake and bad magic.
// Scenarios: performance-and-verification.md:PRF-SCN-088
func TestServeBootstrapProbeDropsFailedHandshakeAndBadMagic(t *testing.T) {
	serverTLS, clientTLS := testPeerTLSConfigs(t, "node1.example")
	peerServerTLS, peerClientTLS := testPeerTLSConfigs(t, "node2.example")
	serverTLS.ClientCAs.AddCert(peerServerTLS.Certificates[0].Leaf)
	clientTLS.RootCAs.AddCert(peerServerTLS.Certificates[0].Leaf)
	peerServerTLS.ClientCAs.AddCert(serverTLS.Certificates[0].Leaf)
	peerClientTLS.RootCAs.AddCert(serverTLS.Certificates[0].Leaf)
	listener := cptListen(t)
	local := identityForTestCertificate(1, listener.Addr().String(), "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS.Certificates[0])
	peer := identityForTestCertificate(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", "1.0.0", peerServerTLS.Certificates[0])
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	serverDone := make(chan error, 1)
	go func() { serverDone <- ServeBootstrapProbe(ctx, listener, serverTLS, local, []PeerIdentityDTO{peer}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("ServeBootstrapProbe after cancellation: %v", err)
			}
		case <-time.After(cptWaitTimeout):
			t.Error("ServeBootstrapProbe did not stop after cancellation")
		}
	})

	raw, err := net.DialTimeout("tcp", listener.Addr().String(), cptWaitTimeout)
	if err != nil {
		t.Fatalf("dial plain TCP client: %v", err)
	}
	if _, err := raw.Write([]byte("not a tls client hello\r\n\r\n")); err != nil {
		_ = raw.Close()
		t.Fatalf("write plain TCP payload: %v", err)
	}
	_ = raw.SetReadDeadline(time.Now().Add(cptWaitTimeout))
	var rawResponse [1]byte
	_, err = raw.Read(rawResponse[:])
	_ = raw.Close()
	if err == nil {
		t.Fatal("plain TCP read succeeded, want failed-handshake error")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("plain TCP read error = %v, want non-deadline error", err)
	}

	tlsConfig := peerClientTLS.Clone()
	tlsConfig.ServerName = "node1.example"
	tlsConn, err := tls.Dial("tcp", listener.Addr().String(), tlsConfig)
	if err != nil {
		t.Fatalf("TLS dial for bad magic: %v", err)
	}
	if err := writeDynamicFrame(tlsConn, []byte("BADMAGIC-request"), dynamicHelloLimit); err != nil {
		_ = tlsConn.Close()
		t.Fatalf("write bad-magic frame: %v", err)
	}
	_ = tlsConn.SetReadDeadline(time.Now().Add(cptWaitTimeout))
	_, err = readDynamicFrame(tlsConn, dynamicHelloLimit)
	_ = tlsConn.Close()
	if err == nil {
		t.Fatal("bad-magic response read succeeded, want error")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("bad-magic response error = %v, want non-deadline error", err)
	}

	got, err := ProbeBootstrapPeer(ctx, peerClientTLS, peer, local)
	if err != nil || got != (BootstrapProbeResultDTO{Status: BootstrapProbeEmpty}) {
		t.Fatalf("healthy probe after malformed clients = %+v, err=%v; want empty result and nil", got, err)
	}
}

// TestProbeBootstrapPeerReportsResponseReadFailure verifies the bootstrap probe reports a response read failure.
// Scenarios: performance-and-verification.md:PRF-SCN-088
func TestProbeBootstrapPeerReportsResponseReadFailure(t *testing.T) {
	local := cptNewNode(t, "node1.example")
	remote := cptNewNode(t, "node2.example")
	cptTrustEachOther(local, remote)
	listener := cptListen(t)
	serverDone := make(chan error, 1)
	go func() {
		socket, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer socket.Close()
		tlsConn := tls.Server(socket, remote.serverTLS)
		if err := tlsConn.Handshake(); err != nil {
			serverDone <- err
			return
		}
		if _, err := readDynamicFrame(tlsConn, dynamicHelloLimit); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), cptWaitTimeout)
	defer cancel()
	addr := listener.Addr().String()
	got, err := ProbeBootstrapPeer(ctx, local.clientTLS,
		local.identity(1, "127.0.0.1:1", PeerPhaseVoter, ""),
		remote.identity(2, addr, PeerPhaseVoter, ""))
	if err == nil || got != (BootstrapProbeResultDTO{}) {
		t.Fatalf("ProbeBootstrapPeer result = %+v, err=%v; want zero result and non-nil error", got, err)
	}
	select {
	case serverErr := <-serverDone:
		if serverErr != nil {
			t.Fatalf("response-failure test server: %v", serverErr)
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("response-failure test server did not consume probe request")
	}
}

func TestDecodeBootstrapProbeResponseWaitsForConnectionClose(t *testing.T) {
	result := BootstrapProbeResultDTO{Status: BootstrapProbeReady, Term: 1, CommitIndex: 3}
	validPayload := encodeBootstrapProbeResponse(result)
	readFailure := errors.New("probe close read failed")
	tests := []struct {
		name    string
		payload []byte
		reader  io.Reader
		want    BootstrapProbeResultDTO
		wantErr string
	}{
		{
			name:    "valid response closes cleanly",
			payload: validPayload,
			reader:  strings.NewReader(""),
			want:    result,
		},
		{
			name:    "transient empty read",
			payload: validPayload,
			reader:  &bootstrapProbeNoProgressReader{},
			want:    result,
		},
		{
			name:    "malformed response",
			payload: []byte("invalid"),
			reader:  strings.NewReader(""),
			wantErr: "bootstrap probe response is invalid",
		},
		{
			name:    "trailing data",
			payload: validPayload,
			reader:  strings.NewReader("x"),
			wantErr: "bootstrap probe response has trailing data",
		},
		{
			name:    "close read failure",
			payload: validPayload,
			reader:  bootstrapProbeErrorReader{err: readFailure},
			wantErr: "wait for bootstrap probe close: probe close read failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeBootstrapProbeResponseAndWaitForClose(test.payload, test.reader)
			if test.wantErr != "" {
				if err == nil || err.Error() != test.wantErr {
					t.Fatalf("result=%+v err=%v, want error %q", got, err, test.wantErr)
				}
				if got != (BootstrapProbeResultDTO{}) {
					t.Fatalf("result=%+v, want zero result on error", got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("result=%+v err=%v, want %+v", got, err, test.want)
			}
		})
	}
}

type bootstrapProbeErrorReader struct {
	err error
}

func (reader bootstrapProbeErrorReader) Read([]byte) (int, error) {
	return 0, reader.err
}

type bootstrapProbeNoProgressReader struct {
	returnEOF bool
}

func (reader *bootstrapProbeNoProgressReader) Read([]byte) (int, error) {
	if reader.returnEOF {
		return 0, io.EOF
	}
	reader.returnEOF = true
	return 0, nil
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-126
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

type bootstrapProbeNoDeadlineListener struct {
	net.Listener
}

type bootstrapProbeFixedListener struct {
	accept func() (net.Conn, error)
}

func (listener *bootstrapProbeFixedListener) Accept() (net.Conn, error) {
	return listener.accept()
}

func (listener *bootstrapProbeFixedListener) Close() error {
	return nil
}

func (listener *bootstrapProbeFixedListener) Addr() net.Addr {
	return &net.TCPAddr{}
}

func (listener *bootstrapProbeFixedListener) SetDeadline(time.Time) error {
	return nil
}

func TestBootstrapProbeValidatesListenerTLSIdentityAndPeerSet(t *testing.T) {
	serverTLS, _ := testPeerTLSConfigs(t, "node1.example")
	makeLocal := func(t *testing.T, address string) PeerIdentityDTO {
		t.Helper()
		return identityForTestCertificate(1, address, "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS.Certificates[0])
	}

	t.Run("listener without deadlines", func(t *testing.T) {
		listener := cptListen(t)
		local := makeLocal(t, listener.Addr().String())
		wrapped := &bootstrapProbeNoDeadlineListener{Listener: listener}
		if err := ServeBootstrapProbe(context.Background(), wrapped, nil, local, nil); err == nil {
			t.Fatal("listener without deadline support was accepted")
		}
	})

	t.Run("invalid TLS configuration", func(t *testing.T) {
		listener := cptListen(t)
		local := makeLocal(t, listener.Addr().String())
		if err := ServeBootstrapProbe(context.Background(), listener, nil, local, nil); err == nil {
			t.Fatal("nil server TLS configuration was accepted")
		}
	})

	t.Run("certificate identity mismatch", func(t *testing.T) {
		listener := cptListen(t)
		local := makeLocal(t, listener.Addr().String())
		local.DNSSAN = "wrong.example"
		if err := ServeBootstrapProbe(context.Background(), listener, serverTLS, local, nil); err == nil {
			t.Fatal("server certificate with a mismatched identity was accepted")
		}
	})

	t.Run("peer set contains local identity", func(t *testing.T) {
		listener := cptListen(t)
		local := makeLocal(t, listener.Addr().String())
		if err := ServeBootstrapProbe(context.Background(), listener, serverTLS, local, []PeerIdentityDTO{local}); err == nil {
			t.Fatal("peer set containing the local member was accepted")
		}
	})
}

func TestBootstrapProbeReturnsUnexpectedAcceptError(t *testing.T) {
	serverTLS, _ := testPeerTLSConfigs(t, "node1.example")
	local := identityForTestCertificate(1, "127.0.0.1:19001", "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS.Certificates[0])
	wantErr := errors.New("bootstrap probe accept failed")
	listener := &bootstrapProbeFixedListener{accept: func() (net.Conn, error) {
		return nil, wantErr
	}}
	if err := ServeBootstrapProbe(context.Background(), listener, serverTLS, local, nil); !errors.Is(err, wantErr) {
		t.Fatalf("ServeBootstrapProbe error = %v, want %v", err, wantErr)
	}
}

func TestProbeBootstrapPeerReportsDialAndHandshakeFailures(t *testing.T) {
	localNode := cptNewNode(t, "node1.example")
	remoteNode := cptNewNode(t, "node2.example")
	local := localNode.identity(1, "127.0.0.1:19001", PeerPhaseVoter, "")

	t.Run("dial failure", func(t *testing.T) {
		peer := remoteNode.identity(2, "not-an-address", PeerPhaseVoter, "")
		_, err := ProbeBootstrapPeer(context.Background(), localNode.clientTLS, local, peer)
		var operationErr *net.OpError
		if err == nil || !errors.As(err, &operationErr) || operationErr.Op != "dial" {
			t.Fatalf("ProbeBootstrapPeer error = %v, want dial failure", err)
		}
	})

	t.Run("TLS handshake failure", func(t *testing.T) {
		listener := cptListen(t)
		accepted := make(chan struct{})
		go func() {
			socket, err := listener.Accept()
			if err == nil {
				_ = socket.Close()
			}
			close(accepted)
		}()
		peer := remoteNode.identity(2, listener.Addr().String(), PeerPhaseVoter, "")
		ctx, cancel := context.WithTimeout(context.Background(), cptWaitTimeout)
		defer cancel()
		_, err := ProbeBootstrapPeer(ctx, localNode.clientTLS, local, peer)
		if err == nil || !strings.Contains(err.Error(), "bootstrap probe TLS handshake failed") {
			t.Fatalf("ProbeBootstrapPeer error = %v, want TLS handshake failure", err)
		}
		select {
		case <-accepted:
		case <-time.After(cptWaitTimeout):
			t.Fatal("TLS test server did not accept the probe connection")
		}
	})
}
