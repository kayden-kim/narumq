package tcp

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestPeerObservabilityHelloKeepsExistingIdentityLayout(t *testing.T) {
	identity := dynamicTestIdentity(7, "127.0.0.1:19007", "node7.example", PeerPhaseVoter, "", 0x77)
	identity.ObservabilitySupported = true
	raftHello, err := encodeDynamicPeerHello(identity, dynamicRaftMagic)
	if err != nil {
		t.Fatalf("encode Raft hello: %v", err)
	}
	observationHello, err := encodeDynamicPeerHello(identity, peerObservabilityMagic)
	if err != nil {
		t.Fatalf("encode observability hello: %v", err)
	}
	want := append([]byte(nil), raftHello...)
	copy(want[:8], peerObservabilityMagic)
	if !bytes.Equal(observationHello, want) {
		t.Fatalf("observability hello differs from existing layout: got %x, want %x", observationHello, want)
	}
	decoded, err := decodeDynamicPeerHello(observationHello, peerObservabilityMagic)
	if err != nil || !samePeerIdentity(decoded, identity) || decoded.ObservabilitySupported {
		t.Fatalf("decode observability hello = %+v, %v", decoded, err)
	}
}

func TestPeerObservabilityUsesAuthenticatedOneShotRPCBothDirections(t *testing.T) {
	node1, node2 := cptNewNode(t, "node1.example"), cptNewNode(t, "node2.example")
	cptTrustEachOther(node1, node2)
	listener1, listener2 := cptListen(t), cptListen(t)
	identity1 := peerObservabilityTestIdentity(node1, 1, listener1.Addr().String())
	identity2 := peerObservabilityTestIdentity(node2, 2, listener2.Addr().String())

	var nodeCalls, metricCalls atomic.Int32
	transport1 := newPeerObservabilityTestTransport(t, node1, listener1, identity1, []PeerIdentityDTO{identity2}, func(_ context.Context, kind PeerObservabilityKind) ([]byte, error) {
		if kind != PeerObservabilityMetrics {
			return nil, errors.New("unexpected kind")
		}
		metricCalls.Add(1)
		return []byte(`{"memberId":"1","kind":"metrics"}`), nil
	})
	transport2 := newPeerObservabilityTestTransport(t, node2, listener2, identity2, []PeerIdentityDTO{identity1}, func(_ context.Context, kind PeerObservabilityKind) ([]byte, error) {
		if kind != PeerObservabilityNode {
			return nil, errors.New("unexpected kind")
		}
		nodeCalls.Add(1)
		return []byte(`{"memberId":"2","kind":"node"}`), nil
	})
	startPeerObservabilityTestTransport(t, transport1)
	startPeerObservabilityTestTransport(t, transport2)

	response, err := transport1.QueryObservability(context.Background(), identity2.MemberID, PeerObservabilityNode)
	if err != nil {
		t.Fatalf("node 1 query to node 2: %v", err)
	}
	if response.Status != PeerObservabilityStatusOK || response.ResponderMemberID != identity2.MemberID || response.Kind != PeerObservabilityNode || string(response.Body) != `{"memberId":"2","kind":"node"}` {
		t.Fatalf("node 1 response = %+v", response)
	}
	response, err = transport2.QueryObservability(context.Background(), identity1.MemberID, PeerObservabilityMetrics)
	if err != nil {
		t.Fatalf("node 2 query to node 1: %v", err)
	}
	if response.Status != PeerObservabilityStatusOK || response.ResponderMemberID != identity1.MemberID || response.Kind != PeerObservabilityMetrics || string(response.Body) != `{"memberId":"1","kind":"metrics"}` {
		t.Fatalf("node 2 response = %+v", response)
	}
	if nodeCalls.Load() != 1 || metricCalls.Load() != 1 {
		t.Fatalf("local handlers called node=%d metrics=%d, want one each", nodeCalls.Load(), metricCalls.Load())
	}
	for _, peer := range []*dynamicPeerState{transport1.peers[identity2.MemberID], transport2.peers[identity1.MemberID]} {
		peer.mu.Lock()
		inMessages, outMessages := peer.inMessages, peer.outMessages
		peer.mu.Unlock()
		if inMessages != 0 || outMessages != 0 {
			t.Fatalf("observability used the Raft queue: inbound=%d outbound=%d", inMessages, outMessages)
		}
	}
}

func TestPeerObservabilityDoesNotDialUnsupportedPeer(t *testing.T) {
	node := cptNewNode(t, "node1.example")
	listener := cptListen(t)
	local := peerObservabilityTestIdentity(node, 1, listener.Addr().String())
	unsupported := dynamicTestIdentity(2, "127.0.0.1:1", "node2.example", PeerPhaseVoter, "", 0x22)
	unsupported.ObservabilitySupported = false
	transport := newPeerObservabilityTestTransport(t, node, listener, local, []PeerIdentityDTO{unsupported}, nil)
	startPeerObservabilityTestTransport(t, transport)

	response, err := transport.QueryObservability(context.Background(), unsupported.MemberID, PeerObservabilityNode)
	if err != nil {
		t.Fatalf("unsupported query: %v", err)
	}
	if response.Status != PeerObservabilityStatusUnsupported || response.ResponderMemberID != unsupported.MemberID || len(response.Body) != 0 {
		t.Fatalf("unsupported response = %+v", response)
	}
	transport.mu.Lock()
	connectionCount := len(transport.connections)
	transport.mu.Unlock()
	if connectionCount != 0 {
		t.Fatalf("unsupported peer opened %d connections", connectionCount)
	}
	supported := unsupported
	supported.ObservabilitySupported = true
	if err := transport.ReconcilePeers([]PeerIdentityDTO{supported}); err != nil {
		t.Fatalf("advertise peer support: %v", err)
	}
	local.ObservabilitySupported = false
	if err := transport.UpdateLocalIdentity(local); err != nil {
		t.Fatalf("remove caller support: %v", err)
	}
	response, err = transport.QueryObservability(context.Background(), unsupported.MemberID, PeerObservabilityNode)
	if err != nil || response.Status != PeerObservabilityStatusUnsupported {
		t.Fatalf("unsupported caller response = %+v, %v", response, err)
	}
	transport.mu.Lock()
	connectionCount = len(transport.connections)
	transport.mu.Unlock()
	if connectionCount != 0 {
		t.Fatalf("unsupported caller opened %d connections", connectionCount)
	}
}

func TestPeerObservabilityCapabilityRequiresLocalHandler(t *testing.T) {
	node := cptNewNode(t, "node1.example")
	listener := cptListen(t)
	local := peerObservabilityTestIdentity(node, 1, listener.Addr().String())
	_, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local, Listener: listener, ServerTLS: node.serverTLS, ClientTLS: node.clientTLS,
		AdmissionHandler: cptRejectAdmission,
	})
	if err == nil {
		t.Fatal("local peer advertised observability without a handler")
	}
}

func TestPeerObservabilityRejectsSpoofedIdentityAndMalformedRequest(t *testing.T) {
	t.Run("certificate does not match committed member", func(t *testing.T) {
		server, committed, impostor := cptNewNode(t, "server.example"), cptNewNode(t, "committed.example"), cptNewNode(t, "impostor.example")
		cptTrustEachOther(server, committed)
		cptTrustEachOther(server, impostor)
		listener := cptListen(t)
		local := peerObservabilityTestIdentity(server, 2, listener.Addr().String())
		remote := peerObservabilityTestIdentity(committed, 1, "127.0.0.1:19001")
		transport := newPeerObservabilityTestTransport(t, server, listener, local, []PeerIdentityDTO{remote}, func(context.Context, PeerObservabilityKind) ([]byte, error) {
			return nil, errors.New("spoofed identity reached the local handler")
		})
		startPeerObservabilityTestTransport(t, transport)

		connection, err := dialAndSendPeerObservabilityHello(impostor, server.dnsName, listener.Addr().String(), remote)
		if err != nil {
			t.Fatalf("TLS dial/send spoofed hello: %v", err)
		}
		defer connection.Close()
		if _, err := readDynamicFrame(connection, dynamicHelloLimit); err == nil {
			t.Fatal("server replied to a member ID presented with a different certificate")
		}
	})

	t.Run("invalid request closes without an error response", func(t *testing.T) {
		server, client := cptNewNode(t, "server.example"), cptNewNode(t, "client.example")
		cptTrustEachOther(server, client)
		listener := cptListen(t)
		local := peerObservabilityTestIdentity(server, 2, listener.Addr().String())
		remote := peerObservabilityTestIdentity(client, 1, "127.0.0.1:19001")
		transport := newPeerObservabilityTestTransport(t, server, listener, local, []PeerIdentityDTO{remote}, func(context.Context, PeerObservabilityKind) ([]byte, error) {
			return nil, errors.New("malformed request reached the local handler")
		})
		startPeerObservabilityTestTransport(t, transport)

		connection := dialPeerObservabilityTest(t, client, server.dnsName, listener.Addr().String(), remote)
		defer connection.Close()
		invalidRequest := []byte{0, 2, byte(PeerObservabilityNode), 0, 0, 0, 0, 0, 0, 0, 2}
		if err := writeDynamicFrame(connection, invalidRequest, peerObservabilityRequestSize); err != nil {
			t.Fatalf("write invalid request: %v", err)
		}
		if _, err := readDynamicFrame(connection, uint32(peerObservabilityResponseLimit)); err == nil {
			t.Fatal("server replied to a malformed request")
		}
		_ = connection.Close()
		wrongTargetConnection := dialPeerObservabilityTest(t, client, server.dnsName, listener.Addr().String(), remote)
		defer wrongTargetConnection.Close()
		wrongTargetRequest, err := encodePeerObservabilityRequest(PeerObservabilityRequestDTO{
			Kind: PeerObservabilityNode, TargetMemberID: remote.MemberID,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := writeDynamicFrame(wrongTargetConnection, wrongTargetRequest, peerObservabilityRequestSize); err != nil {
			t.Fatalf("write wrong-target request: %v", err)
		}
		if _, err := readDynamicFrame(wrongTargetConnection, uint32(peerObservabilityResponseLimit)); err == nil {
			t.Fatal("server replied to a request for another target")
		}
	})
}

// Scenarios: management-api-and-observability.md:MGT-SCN-044
func TestPeerObservabilitySanitizesInvalidAndOversizedSnapshots(t *testing.T) {
	server, client := cptNewNode(t, "server.example"), cptNewNode(t, "client.example")
	cptTrustEachOther(server, client)
	listener := cptListen(t)
	local := peerObservabilityTestIdentity(server, 2, listener.Addr().String())
	remote := peerObservabilityTestIdentity(client, 1, "127.0.0.1:19001")
	var calls atomic.Int32
	transport := newPeerObservabilityTestTransport(t, server, listener, local, []PeerIdentityDTO{remote}, func(context.Context, PeerObservabilityKind) ([]byte, error) {
		switch calls.Add(1) {
		case 1:
			return []byte(`{"duplicate":1,"duplicate":2}`), nil
		default:
			return make([]byte, peerObservabilityResponseLimit-peerObservabilityResponseHeaderSize+1), nil
		}
	})
	clientTransport := newPeerObservabilityTestTransport(t, client, cptListen(t), remote, []PeerIdentityDTO{local}, nil)
	startPeerObservabilityTestTransport(t, transport)
	startPeerObservabilityTestTransport(t, clientTransport)

	response, err := clientTransport.QueryObservability(context.Background(), local.MemberID, PeerObservabilityMetrics)
	if err != nil {
		t.Fatalf("query duplicate-key snapshot: %v", err)
	}
	if response.Status != PeerObservabilityStatusInternalError || len(response.Body) != 0 {
		t.Fatalf("duplicate-key response = %+v", response)
	}
	response, err = clientTransport.QueryObservability(context.Background(), local.MemberID, PeerObservabilityMetrics)
	if err != nil {
		t.Fatalf("query oversized snapshot: %v", err)
	}
	if response.Status != PeerObservabilityStatusSnapshotTooLarge || len(response.Body) != 0 {
		t.Fatalf("oversized response = %+v", response)
	}
}

func TestPeerObservabilityOutboundSlotsBoundConcurrentQueries(t *testing.T) {
	server, client := cptNewNode(t, "server.example"), cptNewNode(t, "client.example")
	cptTrustEachOther(server, client)
	serverListener, clientListener := cptListen(t), cptListen(t)
	local := peerObservabilityTestIdentity(server, 2, serverListener.Addr().String())
	remote := peerObservabilityTestIdentity(client, 1, clientListener.Addr().String())
	entered := make(chan struct{}, dynamicObservabilitySlots)
	release := make(chan struct{})
	serverTransport := newPeerObservabilityTestTransport(t, server, serverListener, local, []PeerIdentityDTO{remote}, func(ctx context.Context, _ PeerObservabilityKind) ([]byte, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return []byte(`{"ok":true}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	clientTransport := newPeerObservabilityTestTransport(t, client, clientListener, remote, []PeerIdentityDTO{local}, nil)
	startPeerObservabilityTestTransport(t, serverTransport)
	startPeerObservabilityTestTransport(t, clientTransport)

	results := make(chan error, dynamicObservabilitySlots)
	for range dynamicObservabilitySlots {
		go func() {
			response, err := clientTransport.QueryObservability(context.Background(), local.MemberID, PeerObservabilityNode)
			if err == nil && response.Status != PeerObservabilityStatusOK {
				err = errors.New("observation query did not return OK")
			}
			results <- err
		}()
	}
	for range dynamicObservabilitySlots {
		select {
		case <-entered:
		case <-time.After(cptWaitTimeout):
			t.Fatal("server did not receive all eight concurrent queries")
		}
	}
	queryContext, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := clientTransport.QueryObservability(queryContext, local.MemberID, PeerObservabilityNode); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ninth concurrent query error = %v, want context deadline while waiting for a slot", err)
	}
	close(release)
	for range dynamicObservabilitySlots {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("query in first eight slots: %v", err)
			}
		case <-time.After(cptWaitTimeout):
			t.Fatal("a query holding an outbound slot did not finish")
		}
	}
}

func TestPeerObservabilityServerSlotsReturnBusyAndReleaseOnCancellation(t *testing.T) {
	server, client := cptNewNode(t, "server.example"), cptNewNode(t, "client.example")
	cptTrustEachOther(server, client)
	listener := cptListen(t)
	local := peerObservabilityTestIdentity(server, 2, listener.Addr().String())
	remote := peerObservabilityTestIdentity(client, 1, "127.0.0.1:19001")
	entered := make(chan struct{}, dynamicObservabilitySlots+1)
	release := make(chan struct{})
	transport := newPeerObservabilityTestTransport(t, server, listener, local, []PeerIdentityDTO{remote}, func(ctx context.Context, _ PeerObservabilityKind) ([]byte, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return []byte(`{"ok":true}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	startPeerObservabilityTestTransport(t, transport)

	connections := make([]*tls.Conn, dynamicObservabilitySlots)
	for index := range connections {
		connections[index] = dialPeerObservabilityTest(t, client, server.dnsName, listener.Addr().String(), remote)
		request, err := encodePeerObservabilityRequest(PeerObservabilityRequestDTO{Kind: PeerObservabilityNode, TargetMemberID: local.MemberID})
		if err != nil {
			t.Fatal(err)
		}
		if err := writeDynamicFrame(connections[index], request, peerObservabilityRequestSize); err != nil {
			t.Fatalf("write request %d: %v", index, err)
		}
	}
	for range dynamicObservabilitySlots {
		select {
		case <-entered:
		case <-time.After(cptWaitTimeout):
			t.Fatal("server did not occupy all eight observation slots")
		}
	}
	busyConnection := dialPeerObservabilityTest(t, client, server.dnsName, listener.Addr().String(), remote)
	busyRequest, err := encodePeerObservabilityRequest(PeerObservabilityRequestDTO{Kind: PeerObservabilityNode, TargetMemberID: local.MemberID})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDynamicFrame(busyConnection, busyRequest, peerObservabilityRequestSize); err != nil {
		t.Fatalf("write ninth request: %v", err)
	}
	busyPayload, err := readDynamicFrame(busyConnection, uint32(peerObservabilityResponseLimit))
	if err != nil {
		t.Fatalf("read BUSY response: %v", err)
	}
	busyResponse, err := decodePeerObservabilityResponse(busyPayload, PeerObservabilityNode, local.MemberID)
	if err != nil || busyResponse.Status != PeerObservabilityStatusBusy {
		t.Fatalf("ninth request response = %+v, %v; want BUSY", busyResponse, err)
	}
	if err := waitForDynamicPeerClose(busyConnection, "observability"); err != nil {
		t.Fatalf("wait for BUSY connection close: %v", err)
	}
	_ = busyConnection.Close()

	close(release)
	for index, connection := range connections {
		payload, err := readDynamicFrame(connection, uint32(peerObservabilityResponseLimit))
		if err != nil {
			t.Fatalf("read response %d: %v", index, err)
		}
		response, err := decodePeerObservabilityResponse(payload, PeerObservabilityNode, local.MemberID)
		if err != nil || response.Status != PeerObservabilityStatusOK {
			t.Fatalf("response %d = %+v, %v", index, response, err)
		}
		if err := waitForDynamicPeerClose(connection, "observability"); err != nil {
			t.Fatalf("wait for response %d close: %v", index, err)
		}
		_ = connection.Close()
	}

	var calls atomic.Int32
	cancellationStarted := make(chan struct{})
	transport.mu.Lock()
	transport.observabilityHandler = func(ctx context.Context, _ PeerObservabilityKind) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(cancellationStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return []byte(`{"released":true}`), nil
	}
	transport.mu.Unlock()
	clientListener := cptListen(t)
	clientTransport := newPeerObservabilityTestTransport(t, client, clientListener, remote, []PeerIdentityDTO{local}, nil)
	startPeerObservabilityTestTransport(t, clientTransport)
	queryContext, cancelQuery := context.WithCancel(context.Background())
	queryDone := make(chan error, 1)
	go func() {
		_, queryErr := clientTransport.QueryObservability(queryContext, local.MemberID, PeerObservabilityNode)
		queryDone <- queryErr
	}()
	select {
	case <-cancellationStarted:
	case <-time.After(cptWaitTimeout):
		cancelQuery()
		t.Fatal("canceled query did not reach the server handler")
	}
	cancelQuery()
	select {
	case err := <-queryDone:
		if err == nil {
			t.Fatal("canceled query returned success")
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("canceled query did not close its connection")
	}
	deadline := time.NewTimer(cptWaitTimeout)
	defer deadline.Stop()
	for len(transport.observabilityInboundSlots) != 0 {
		select {
		case <-deadline.C:
			t.Fatal("canceled request did not release its server slot")
		case <-time.After(time.Millisecond):
		}
	}
	response, err := clientTransport.QueryObservability(context.Background(), local.MemberID, PeerObservabilityNode)
	if err != nil || response.Status != PeerObservabilityStatusOK {
		t.Fatalf("query after cancellation = %+v, %v; server slot was not returned", response, err)
	}
}

func TestPeerObservabilityShutdownCancelsAndWaitsForHandler(t *testing.T) {
	server, client := cptNewNode(t, "server.example"), cptNewNode(t, "client.example")
	cptTrustEachOther(server, client)
	serverListener, clientListener := cptListen(t), cptListen(t)
	local := peerObservabilityTestIdentity(server, 2, serverListener.Addr().String())
	remote := peerObservabilityTestIdentity(client, 1, clientListener.Addr().String())
	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})
	transport := newPeerObservabilityTestTransport(t, server, serverListener, local, []PeerIdentityDTO{remote}, func(ctx context.Context, _ PeerObservabilityKind) ([]byte, error) {
		close(handlerStarted)
		<-ctx.Done()
		close(handlerDone)
		return nil, ctx.Err()
	})
	clientTransport := newPeerObservabilityTestTransport(t, client, clientListener, remote, []PeerIdentityDTO{local}, nil)
	serverCancel, serverDone := startPeerObservabilityTestTransport(t, transport)
	startPeerObservabilityTestTransport(t, clientTransport)

	queryDone := make(chan error, 1)
	go func() {
		_, err := clientTransport.QueryObservability(context.Background(), local.MemberID, PeerObservabilityNode)
		queryDone <- err
	}()
	select {
	case <-handlerStarted:
	case <-time.After(cptWaitTimeout):
		t.Fatal("query did not reach server handler")
	}
	serverCancel()
	select {
	case <-handlerDone:
	case <-time.After(cptWaitTimeout):
		t.Fatal("transport shutdown did not cancel the local handler")
	}
	select {
	case <-serverDone:
	case <-time.After(cptWaitTimeout):
		t.Fatal("transport shutdown did not wait for handler completion")
	}
	select {
	case err := <-queryDone:
		if err == nil {
			t.Fatal("query succeeded after target shutdown")
		}
	case <-time.After(cptWaitTimeout):
		t.Fatal("query did not exit after target shutdown")
	}
}

func peerObservabilityTestIdentity(node *cptNode, memberID uint64, address string) PeerIdentityDTO {
	identity := node.identity(memberID, address, PeerPhaseVoter, "")
	identity.ObservabilitySupported = true
	return identity
}

func newPeerObservabilityTestTransport(
	t *testing.T,
	node *cptNode,
	listener net.Listener,
	local PeerIdentityDTO,
	peers []PeerIdentityDTO,
	handler PeerObservabilityHandler,
) *DynamicTCPPeerTransport {
	t.Helper()
	if local.ObservabilitySupported && handler == nil {
		handler = func(context.Context, PeerObservabilityKind) ([]byte, error) {
			return []byte(`{"test":true}`), nil
		}
	}
	transport, err := NewDynamicTCPPeerTransport(DynamicPeerTransportConfig{
		LocalIdentity: local, Listener: listener, ServerTLS: node.serverTLS, ClientTLS: node.clientTLS,
		Peers: peers, AdmissionHandler: cptRejectAdmission, ObservabilityHandler: handler,
	})
	if err != nil {
		t.Fatalf("create peer observability transport: %v", err)
	}
	return transport
}

func startPeerObservabilityTestTransport(t *testing.T, transport *DynamicTCPPeerTransport) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var serveErr error
	go func() {
		serveErr = transport.Serve(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			if serveErr != nil {
				t.Errorf("peer observability transport Serve: %v", serveErr)
			}
		case <-time.After(cptWaitTimeout):
			t.Error("peer observability transport did not stop")
		}
	})
	deadline := time.NewTimer(cptWaitTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		transport.mu.Lock()
		started := transport.started
		transport.mu.Unlock()
		if started {
			return cancel, done
		}
		select {
		case <-done:
			t.Fatalf("peer observability transport stopped during startup: %v", serveErr)
		case <-deadline.C:
			t.Fatal("peer observability transport did not start")
		case <-ticker.C:
		}
	}
}

func dialAndSendPeerObservabilityHello(node *cptNode, serverName, address string, identity PeerIdentityDTO) (*tls.Conn, error) {
	clientTLS := node.clientTLS.Clone()
	clientTLS.ServerName = serverName
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: cptWaitTimeout}, Config: clientTLS}
	ctx, cancel := context.WithTimeout(context.Background(), cptWaitTimeout)
	defer cancel()
	socket, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	connection, ok := socket.(*tls.Conn)
	if !ok {
		_ = socket.Close()
		return nil, errors.New("TLS dial did not return a TLS connection")
	}
	hello, err := encodeDynamicPeerHello(identity, peerObservabilityMagic)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	if err := writeDynamicFrame(connection, hello, dynamicHelloLimit); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return connection, nil
}

func dialPeerObservabilityTest(t *testing.T, node *cptNode, serverName, address string, identity PeerIdentityDTO) *tls.Conn {
	t.Helper()
	connection, err := dialAndSendPeerObservabilityHello(node, serverName, address, identity)
	if err != nil {
		t.Fatalf("dial peer observability: %v", err)
	}
	payload, err := readDynamicFrame(connection, dynamicHelloLimit)
	if err != nil {
		_ = connection.Close()
		t.Fatalf("read peer observability hello: %v", err)
	}
	remote, err := decodeDynamicPeerHello(payload, peerObservabilityMagic)
	if err != nil || !peerObservabilityMemberEligible(remote) {
		_ = connection.Close()
		t.Fatalf("decode peer observability hello: %+v, %v", remote, err)
	}
	return connection
}

func TestPeerObservabilityEligibilityRejectsStagedAndRetiredMembers(t *testing.T) {
	for _, phase := range []PeerPhaseDTO{PeerPhaseStaged, PeerPhaseRetired, PeerPhaseDTO(0)} {
		identity := dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", phase, "", 0x11)
		if peerObservabilityMemberEligible(identity) {
			t.Errorf("phase %d was eligible for observability", phase)
		}
	}
	for _, phase := range []PeerPhaseDTO{PeerPhaseLearner, PeerPhaseCatchingUp, PeerPhaseVoter, PeerPhaseLeaving} {
		identity := dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", phase, testOperationID, 0x11)
		if phase == PeerPhaseVoter {
			identity.OperationID = ""
		}
		if !peerObservabilityMemberEligible(identity) {
			t.Errorf("committed phase %d was not eligible for observability", phase)
		}
	}
}

func TestPeerObservabilityQueryRejectsUncommittedMembers(t *testing.T) {
	local := dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", PeerPhaseVoter, "", 0x11)
	local.ObservabilitySupported = true
	target := dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", 0x22)
	target.ObservabilitySupported = true
	for _, test := range []struct {
		name   string
		local  PeerIdentityDTO
		target PeerIdentityDTO
	}{
		{
			name:   "staged caller",
			local:  dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", PeerPhaseStaged, testOperationID, 0x11),
			target: target,
		},
		{
			name:   "staged target",
			local:  local,
			target: dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseStaged, testOperationID, 0x22),
		},
		{
			name:   "retired target",
			local:  local,
			target: dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseRetired, "", 0x22),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			peer := newDynamicLanePeerState(nil, test.target, dynamicLaneRaft)
			transport := &DynamicTCPPeerTransport{
				local: test.local, started: true, ctx: context.Background(),
				peers: map[uint64]*dynamicPeerState{test.target.MemberID: peer},
			}
			if _, err := transport.QueryObservability(context.Background(), test.target.MemberID, PeerObservabilityNode); err == nil {
				t.Fatal("query for an uncommitted member was accepted")
			}
		})
	}
}
