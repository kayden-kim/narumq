package tcp

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// sameAddressDialFixture describes two committed members that share one peer address, as
// happens when a new member ID takes over a lost voter's slot (ADR-0061). Member 3 is the
// lost voter and is expected to present certificate X; member 4 is the live replacement that
// presents certificate Y. Both certificates carry the same DNS SAN. Member 1 dials both.
type sameAddressDialFixture struct {
	local1        PeerIdentityDTO
	lost3         PeerIdentityDTO
	live4         PeerIdentityDTO
	serverTLS1    *tls.Config
	clientTLS1    *tls.Config
	serverTLS4    *tls.Config
	clientTLS4    *tls.Config
	listener1     net.Listener
	listener4     net.Listener
	sharedAddress string
}

func newSameAddressDialFixture(t *testing.T) sameAddressDialFixture {
	t.Helper()
	serverTLS1, clientTLS1 := testPeerTLSConfigs(t, "node1.example")
	serverTLS4, clientTLS4 := testPeerTLSConfigs(t, "slot3.example")
	lostServerTLS, _ := testPeerTLSConfigs(t, "slot3.example")
	serverTLS4.ClientCAs.AddCert(serverTLS1.Certificates[0].Leaf)
	clientTLS4.RootCAs.AddCert(serverTLS1.Certificates[0].Leaf)
	clientTLS1.RootCAs.AddCert(serverTLS4.Certificates[0].Leaf)
	serverTLS1.ClientCAs.AddCert(serverTLS4.Certificates[0].Leaf)
	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener1.Close() })
	listener4, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener4.Close() })
	address := listener4.Addr().String()
	return sameAddressDialFixture{
		local1:        identityForTestCertificate(1, listener1.Addr().String(), "node1.example", PeerPhaseVoter, "", "1.0.0", serverTLS1.Certificates[0]),
		lost3:         identityForTestCertificate(3, address, "slot3.example", PeerPhaseVoter, "", "1.0.0", lostServerTLS.Certificates[0]),
		live4:         identityForTestCertificate(4, address, "slot3.example", PeerPhaseVoter, "", "1.0.0", serverTLS4.Certificates[0]),
		serverTLS1:    serverTLS1,
		clientTLS1:    clientTLS1,
		serverTLS4:    serverTLS4,
		clientTLS4:    clientTLS4,
		listener1:     listener1,
		listener4:     listener4,
		sharedAddress: address,
	}
}

// A dialer that expects member 3's certificate must verify the leaf certificate right after the
// TLS handshake. If the listener presents another certificate, the dialer closes the connection
// without sending its stream hello.
//
// Traceability: CLU-02/SEC-09; docs/contracts/raft-membership-v2.md §1.1 and §1.5; ADR-0061.
// Scenarios: tenancy-and-security.md:SEC-SCN-009, cluster-session-and-delivery.md:CLU-SCN-021, cluster-session-and-delivery.md:CLU-SCN-138
func TestDynamicDialVerifiesPeerCertificateBeforeSendingHello(t *testing.T) {
	fixture := newSameAddressDialFixture(t)
	// Stand in for member 4: it completes the mTLS handshake with certificate Y at member 3's address.
	type helloRead struct {
		payload     []byte
		handshakeOK bool
		err         error
	}
	observed := make(chan helloRead, 1024)
	go func() {
		for {
			socket, err := fixture.listener4.Accept()
			if err != nil {
				return
			}
			go func() {
				defer socket.Close()
				// Without session tickets the dialer has no unread bytes when it closes, so the close
				// is a FIN and cannot reset the handshake the stand-in is still finishing.
				serverConfig := fixture.serverTLS4.Clone()
				serverConfig.SessionTicketsDisabled = true
				server := tls.Server(socket, serverConfig)
				if err := server.Handshake(); err != nil {
					observed <- helloRead{err: err}
					return
				}
				payload, err := readDynamicFrame(server, dynamicHelloLimit)
				observed <- helloRead{payload: payload, handshakeOK: true, err: err}
			}()
		}
	}()

	transport1, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: fixture.local1, Listener: fixture.listener1, ServerTLS: fixture.serverTLS1, ClientTLS: fixture.clientTLS1,
		Peers: []PeerIdentityDTO{fixture.lost3}, AdmissionHandler: cptRejectAdmission,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport1.Serve(ctx) }()
	defer func() {
		cancel()
		<-done
		_ = transport1.Close()
	}()

	// Both lanes dial the shared address. Judge every connection on which the stand-in completed the
	// TLS handshake: the dialer must close it without a hello. Collect four so that each lane's
	// retry loop is observed after its first mismatch.
	completed := 0
	for completed < 4 {
		select {
		case got := <-observed:
			if !got.handshakeOK {
				continue
			}
			completed++
			if got.err == nil {
				t.Fatalf("dialer sent a %d-byte stream hello to a listener whose certificate is not the expected member's", len(got.payload))
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("stand-in listener completed only %d TLS handshakes within 10 s", completed)
		}
	}
	waitForPeerStatus(t, transport1, 3, func(status PeerTransportStatusDTO) bool {
		return status.LastError == "peer identity mismatch"
	})
	if status, err := transport1.Status(3); err != nil || status.Connected {
		t.Fatalf("mismatched peer status = %+v, err=%v, want not connected", status, err)
	}
}

// Member 1 holds a dialer for the lost member 3 and one for the live member 4. Both dial the same
// address. The retries for member 3 must not close or replace the connection member 1 legitimately
// holds to member 4.
//
// Traceability: CLU-14; ADR-0061; docs/contracts/raft-membership-v2.md §1.5.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-021, cluster-session-and-delivery.md:CLU-SCN-138
func TestDynamicSameAddressStaleDialerDoesNotEvictLiveConnection(t *testing.T) {
	fixture := newSameAddressDialFixture(t)
	transport1, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: fixture.local1, Listener: fixture.listener1, ServerTLS: fixture.serverTLS1, ClientTLS: fixture.clientTLS1,
		Peers: []PeerIdentityDTO{fixture.lost3, fixture.live4}, AdmissionHandler: cptRejectAdmission,
	})
	if err != nil {
		t.Fatal(err)
	}
	transport4, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: fixture.live4, Listener: fixture.listener4, ServerTLS: fixture.serverTLS4, ClientTLS: fixture.clientTLS4,
		Peers: []PeerIdentityDTO{fixture.local1}, AdmissionHandler: cptRejectAdmission,
	})
	if err != nil {
		_ = transport1.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	done4 := make(chan error, 1)
	go func() { done1 <- transport1.Serve(ctx) }()
	go func() { done4 <- transport4.Serve(ctx) }()
	defer func() {
		cancel()
		<-done1
		<-done4
		_ = transport1.Close()
		_ = transport4.Close()
	}()

	waitForPeerStatus(t, transport4, 1, func(status PeerTransportStatusDTO) bool { return status.Connected })
	waitForPeerStatus(t, transport1, 4, func(status PeerTransportStatusDTO) bool { return status.Connected })
	live := func() net.Conn {
		transport4.mu.Lock()
		peer := transport4.lanePeersLocked(dynamicLaneRaft)[1]
		transport4.mu.Unlock()
		peer.mu.Lock()
		defer peer.mu.Unlock()
		return peer.connection
	}
	before := live()
	if before == nil {
		t.Fatal("member 4 has no connection from member 1 after both sides reported connected")
	}

	// Member 3's dialer retries every peerRetryInterval. Watch several retries; each one reaches
	// member 4's listener because the address is shared.
	sawMismatch := false
	deadline := time.Now().Add(6 * peerRetryInterval)
	for time.Now().Before(deadline) {
		if now := live(); now != before {
			t.Fatalf("member 4's connection from member 1 was replaced by a dialer that expects member 3")
		}
		if status, err := transport1.Status(3); err == nil {
			if status.Connected {
				t.Fatalf("member 3's dialer was marked connected to member 4: %+v", status)
			}
			sawMismatch = sawMismatch || status.LastError == "peer identity mismatch"
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !sawMismatch {
		t.Fatal("member 3's dialer never reported the identity mismatch, so the shared-address case was not exercised")
	}
	if status, err := transport1.Status(4); err != nil || !status.Connected {
		t.Fatalf("member 1's connection to member 4 = %+v, err=%v, want connected", status, err)
	}
}

func waitForPeerStatus(t *testing.T, transport *DynamicTCPPeerTransport, peerID uint64, ready func(PeerTransportStatusDTO) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if status, err := transport.Status(peerID); err == nil && ready(status) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	status, err := transport.Status(peerID)
	t.Fatalf("peer %d did not reach the expected status: %+v, err=%v", peerID, status, err)
}
