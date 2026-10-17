package tcp

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	observationports "narumq/internal/application/ports/observability"
)

// obsCountingContext reports cancellation from its (live+1)-th Err call on, which models a caller
// that gives up at an exact point of QueryObservability without a timing race.
type obsCountingContext struct {
	context.Context
	mu       sync.Mutex
	calls    int
	liveCall int
}

func (ctx *obsCountingContext) Err() error {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.calls++
	if ctx.calls > ctx.liveCall {
		return context.Canceled
	}
	return nil
}

func obsLiteralTransport(local PeerIdentityDTO, peers ...PeerIdentityDTO) *DynamicTCPPeerTransport {
	transport := &DynamicTCPPeerTransport{
		local: local, started: true, ctx: context.Background(),
		peers:                      make(map[uint64]*dynamicPeerState, len(peers)),
		observabilityOutboundSlots: make(chan struct{}, 1),
	}
	for _, identity := range peers {
		transport.peers[identity.MemberID] = newDynamicLanePeerState(nil, identity, dynamicLaneRaft)
	}
	return transport
}

func obsLiteralIdentities() (local, peer PeerIdentityDTO) {
	local = dynamicTestIdentity(1, "127.0.0.1:19001", "node1.example", PeerPhaseVoter, "", 0x11)
	local.ObservabilitySupported = true
	peer = dynamicTestIdentity(2, "127.0.0.1:19002", "node2.example", PeerPhaseVoter, "", 0x22)
	peer.ObservabilitySupported = true
	return local, peer
}

func TestPeerObservabilityQueryRefusesInvalidCallsBeforeAnyIO(t *testing.T) {
	local, peer := obsLiteralIdentities()
	var nilContext context.Context
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("missing context", func(t *testing.T) {
		if _, err := obsLiteralTransport(local, peer).QueryObservability(nilContext, 2, PeerObservabilityNode); err == nil {
			t.Fatal("query without a context succeeded")
		}
	})
	t.Run("canceled caller", func(t *testing.T) {
		_, err := obsLiteralTransport(local, peer).QueryObservability(canceled, 2, PeerObservabilityNode)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled query error = %v, want context.Canceled", err)
		}
	})
	t.Run("invalid kind or target", func(t *testing.T) {
		transport := obsLiteralTransport(local, peer)
		for name, call := range map[string]func() error{
			"kind zero": func() error {
				_, err := transport.QueryObservability(context.Background(), 2, PeerObservabilityKind(0))
				return err
			},
			"unknown kind": func() error {
				_, err := transport.QueryObservability(context.Background(), 2, PeerObservabilityKind(3))
				return err
			},
			"target zero": func() error {
				_, err := transport.QueryObservability(context.Background(), 0, PeerObservabilityNode)
				return err
			},
		} {
			if err := call(); !errors.Is(err, ErrInvalidPeerMessage) {
				t.Errorf("%s error = %v, want ErrInvalidPeerMessage", name, err)
			}
		}
	})
	t.Run("closed transport", func(t *testing.T) {
		transport := obsLiteralTransport(local, peer)
		transport.closed = true
		if _, err := transport.QueryObservability(context.Background(), 2, PeerObservabilityNode); !errors.Is(err, ErrPeerTransportClosed) {
			t.Fatalf("closed transport error = %v, want ErrPeerTransportClosed", err)
		}
	})
	t.Run("transport that is not serving", func(t *testing.T) {
		transport := obsLiteralTransport(local, peer)
		transport.started = false
		_, err := transport.QueryObservability(context.Background(), 2, PeerObservabilityNode)
		if err == nil || errors.Is(err, ErrPeerTransportClosed) || errors.Is(err, ErrInvalidPeerMessage) {
			t.Fatalf("not-serving error = %v, want a distinct failure", err)
		}
		transport = obsLiteralTransport(local, peer)
		transport.ctx = nil
		if _, err := transport.QueryObservability(context.Background(), 2, PeerObservabilityNode); err == nil {
			t.Fatal("query without a run context succeeded")
		}
	})
	t.Run("member that is not committed", func(t *testing.T) {
		_, err := obsLiteralTransport(local).QueryObservability(context.Background(), 2, PeerObservabilityNode)
		if err == nil || errors.Is(err, ErrPeerTransportClosed) || errors.Is(err, ErrInvalidPeerMessage) {
			t.Fatalf("unknown member error = %v, want a distinct failure", err)
		}
	})
}

func TestPeerObservabilityQueryDoesNotAnswerUnsupportedAfterCallerGaveUp(t *testing.T) {
	local, peer := obsLiteralIdentities()
	peer.ObservabilitySupported = false
	ctx := &obsCountingContext{Context: context.Background(), liveCall: 1}
	response, err := obsLiteralTransport(local, peer).QueryObservability(ctx, 2, PeerObservabilityNode)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if response.Status != 0 || response.Body != nil {
		t.Fatalf("a caller that gave up received a response: %+v", response)
	}
	if ctx.calls < 2 {
		t.Fatalf("context checked %d times, want the unsupported answer re-checked", ctx.calls)
	}
}

func TestPeerObservabilityQueryStopsWhenTransportEndsWhileWaitingForSlot(t *testing.T) {
	local, peer := obsLiteralIdentities()
	runContext, endRun := context.WithCancel(context.Background())
	endRun()
	transport := obsLiteralTransport(local, peer)
	transport.ctx = runContext
	transport.observabilityOutboundSlots <- struct{}{} // every outbound slot is taken
	for range 25 {
		response, err := transport.QueryObservability(context.Background(), 2, PeerObservabilityNode)
		if !errors.Is(err, ErrPeerTransportClosed) && !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want ErrPeerTransportClosed or context.Canceled", err)
		}
		if response.Status != 0 || response.Body != nil {
			t.Fatalf("a stopped transport returned a response: %+v", response)
		}
		if len(transport.observabilityOutboundSlots) != 1 {
			t.Fatalf("outbound slots held = %d, want the pre-filled slot untouched", len(transport.observabilityOutboundSlots))
		}
	}
}

// obsScriptServer plays a committed peer by hand on a raw TLS listener so a test controls every
// byte after the handshake.
type obsScriptServer struct {
	t        *testing.T
	node     *cptNode
	identity PeerIdentityDTO
	listener net.Listener
	wg       sync.WaitGroup
}

func newObsScriptServer(t *testing.T, node *cptNode, memberID uint64) *obsScriptServer {
	t.Helper()
	listener := cptListen(t)
	return &obsScriptServer{
		t: t, node: node, listener: listener,
		identity: peerObservabilityTestIdentity(node, memberID, listener.Addr().String()),
	}
}

func (server *obsScriptServer) serve(script func(connection *tls.Conn)) {
	tlsListener := tls.NewListener(server.listener, server.node.serverTLS)
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		for {
			socket, err := tlsListener.Accept()
			if err != nil {
				return
			}
			connection, ok := socket.(*tls.Conn)
			if !ok {
				_ = socket.Close()
				continue
			}
			server.wg.Add(1)
			go func() {
				defer server.wg.Done()
				defer connection.Close()
				script(connection)
			}()
		}
	}()
	server.t.Cleanup(func() {
		_ = server.listener.Close()
		server.wg.Wait()
	})
}

func obsScriptReadHello(connection net.Conn) error {
	_, err := readDynamicFrame(connection, dynamicHelloLimit)
	return err
}

func obsScriptWriteHello(connection net.Conn, identity PeerIdentityDTO) error {
	hello, err := encodeDynamicPeerHello(identity, peerObservabilityMagic)
	if err != nil {
		return err
	}
	return writeDynamicFrame(connection, hello, dynamicHelloLimit)
}

// obsScriptScenario is a querying member (2) with a scripted committed peer (1).
type obsScriptScenario struct {
	server         *obsScriptServer
	client         *DynamicTCPPeerTransport
	clientIdentity PeerIdentityDTO
	peerIdentity   PeerIdentityDTO
}

// newObsScriptScenario registers the scripted peer with the client; tweakPeer changes only the
// identity the client believes in, not the certificate the peer presents.
func newObsScriptScenario(t *testing.T, tweakPeer func(*PeerIdentityDTO)) *obsScriptScenario {
	t.Helper()
	serverNode, clientNode := cptNewNode(t, "server.example"), cptNewNode(t, "client.example")
	cptTrustEachOther(serverNode, clientNode)
	scenario := &obsScriptScenario{server: newObsScriptServer(t, serverNode, 1)}
	scenario.peerIdentity = scenario.server.identity
	if tweakPeer != nil {
		tweakPeer(&scenario.peerIdentity)
	}
	clientListener := cptListen(t)
	scenario.clientIdentity = peerObservabilityTestIdentity(clientNode, 2, clientListener.Addr().String())
	scenario.client = newPeerObservabilityTestTransport(t, clientNode, clientListener, scenario.clientIdentity,
		[]PeerIdentityDTO{scenario.peerIdentity}, nil)
	startPeerObservabilityTestTransport(t, scenario.client)
	return scenario
}

func (scenario *obsScriptScenario) query(kind PeerObservabilityKind) (PeerObservabilityResponseDTO, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cptWaitTimeout)
	defer cancel()
	return scenario.client.QueryObservability(ctx, scenario.server.identity.MemberID, kind)
}

func (scenario *obsScriptScenario) openConnections() int {
	scenario.client.mu.Lock()
	defer scenario.client.mu.Unlock()
	return len(scenario.client.connections)
}

func obsAwait(t *testing.T, results <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(cptWaitTimeout):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func TestPeerObservabilityQueryFailsWhenPeerIsUnreachable(t *testing.T) {
	serverNode, clientNode := cptNewNode(t, "server.example"), cptNewNode(t, "client.example")
	cptTrustEachOther(serverNode, clientNode)
	closed := cptListen(t)
	deadServer := peerObservabilityTestIdentity(serverNode, 1, closed.Addr().String())
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	clientListener := cptListen(t)
	clientIdentity := peerObservabilityTestIdentity(clientNode, 2, clientListener.Addr().String())
	client := newPeerObservabilityTestTransport(t, clientNode, clientListener, clientIdentity, []PeerIdentityDTO{deadServer}, nil)
	startPeerObservabilityTestTransport(t, client)

	response, err := client.QueryObservability(context.Background(), 1, PeerObservabilityNode)
	if err == nil || errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("query to a dead peer error = %v, want a connection failure", err)
	}
	if response.Status != 0 || response.Body != nil {
		t.Fatalf("a failed dial returned a response: %+v", response)
	}
	client.mu.Lock()
	tracked := len(client.connections)
	client.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("failed dial left %d tracked connections", tracked)
	}
	_, readerErr := NewPeerObservabilityReader(client).QueryNodeSnapshot(context.Background(), "1")
	requirePeerQueryCode(t, readerErr, observationports.QueryErrorNodeUnreachable)
}

func TestPeerObservabilityQuerySendsNothingUntilPeerIdentityIsVerified(t *testing.T) {
	for name, test := range map[string]struct {
		tweakPeer func(*PeerIdentityDTO)
		breakSelf func(*DynamicTCPPeerTransport)
	}{
		"peer certificate is not the committed one": {
			tweakPeer: func(peer *PeerIdentityDTO) { peer.CertificateSHA256[0] ^= 0xFF },
		},
		"local identity cannot be encoded": {
			breakSelf: func(transport *DynamicTCPPeerTransport) {
				transport.mu.Lock()
				transport.local.ClusterID = ""
				transport.mu.Unlock()
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			helloSeen := make(chan error, 4)
			scenario := newObsScriptScenario(t, test.tweakPeer)
			scenario.server.serve(func(connection *tls.Conn) { helloSeen <- obsScriptReadHello(connection) })
			if test.breakSelf != nil {
				test.breakSelf(scenario.client)
			}
			response, err := scenario.query(PeerObservabilityNode)
			if err == nil || response.Status != 0 {
				t.Fatalf("query = %+v, %v; want a failure", response, err)
			}
			if helloErr := obsAwait(t, helloSeen, "the peer to observe the connection"); helloErr == nil {
				t.Fatal("the querying member sent its hello although the peer could not be verified")
			}
			if open := scenario.openConnections(); open != 0 {
				t.Fatalf("failed query left %d tracked connections", open)
			}
		})
	}
}

func TestPeerObservabilityQueryRejectsBrokenOrIncompatiblePeerHello(t *testing.T) {
	garbage := []byte(peerObservabilityMagic + "not-an-identity")
	for name, test := range map[string]struct {
		reply       func(connection net.Conn, identity PeerIdentityDTO) error
		wantInvalid bool
	}{
		"peer closes instead of answering": {
			reply: func(net.Conn, PeerIdentityDTO) error { return errors.New("peer closes the connection") },
		},
		"peer answers with a malformed hello": {
			reply: func(connection net.Conn, _ PeerIdentityDTO) error {
				return writeDynamicFrame(connection, garbage, dynamicHelloLimit)
			},
			wantInvalid: true,
		},
		"peer belongs to another cluster": {
			reply: func(connection net.Conn, identity PeerIdentityDTO) error {
				identity.ClusterID = "another-cluster"
				return obsScriptWriteHello(connection, identity)
			},
		},
		"peer presents another address": {
			reply: func(connection net.Conn, identity PeerIdentityDTO) error {
				identity.Address = "127.0.0.1:1"
				return obsScriptWriteHello(connection, identity)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requestSeen := make(chan error, 4)
			scenario := newObsScriptScenario(t, nil)
			scenario.server.serve(func(connection *tls.Conn) {
				if err := obsScriptReadHello(connection); err != nil {
					requestSeen <- err
					return
				}
				if err := test.reply(connection, scenario.server.identity); err != nil {
					requestSeen <- err
					return
				}
				_, err := readDynamicFrame(connection, peerObservabilityRequestSize)
				requestSeen <- err
			})
			response, err := scenario.query(PeerObservabilityNode)
			if err == nil || response.Status != 0 || response.Body != nil {
				t.Fatalf("query = %+v, %v; want a failure and no response", response, err)
			}
			if test.wantInvalid != errors.Is(err, ErrInvalidPeerMessage) {
				t.Fatalf("error = %v, ErrInvalidPeerMessage = %v, want %v", err, errors.Is(err, ErrInvalidPeerMessage), test.wantInvalid)
			}
			if requestErr := obsAwait(t, requestSeen, "the peer to finish"); requestErr == nil {
				t.Fatal("the querying member sent a request to a peer that failed the identity exchange")
			}
			if open := scenario.openConnections(); open != 0 {
				t.Fatalf("failed query left %d tracked connections", open)
			}
			if test.wantInvalid {
				_, readerErr := NewPeerObservabilityReader(scenario.client).QueryNodeSnapshot(context.Background(), "1")
				requirePeerQueryCode(t, readerErr, observationports.QueryErrorInvalidResponse)
			}
		})
	}
}

func TestPeerObservabilityQueryRechecksCommitmentAfterHello(t *testing.T) {
	notClosed := func(t *testing.T, err error) {
		t.Helper()
		if err == nil || errors.Is(err, ErrPeerTransportClosed) || errors.Is(err, ErrInvalidPeerMessage) {
			t.Fatalf("error = %v, want a membership-change failure", err)
		}
	}
	for name, test := range map[string]struct {
		change func(scenario *obsScriptScenario) error
		check  func(t *testing.T, response PeerObservabilityResponseDTO, err error)
	}{
		"peer removed from the committed set": {
			change: func(scenario *obsScriptScenario) error { return scenario.client.ReconcilePeers(nil) },
			check: func(t *testing.T, response PeerObservabilityResponseDTO, err error) {
				if !errors.Is(err, ErrPeerTransportClosed) || response.Status != 0 {
					t.Fatalf("query = %+v, %v; want ErrPeerTransportClosed", response, err)
				}
			},
		},
		"peer membership phase advanced": {
			change: func(scenario *obsScriptScenario) error {
				advanced := scenario.peerIdentity
				advanced.Phase, advanced.OperationID = PeerPhaseCatchingUp, testOperationID
				return scenario.client.ReconcilePeers([]PeerIdentityDTO{advanced})
			},
			check: func(t *testing.T, response PeerObservabilityResponseDTO, err error) {
				notClosed(t, err)
				if response.Status != 0 {
					t.Fatalf("a changed peer produced a response: %+v", response)
				}
			},
		},
		"local member no longer committed": {
			change: func(scenario *obsScriptScenario) error {
				scenario.client.mu.Lock()
				scenario.client.local.Phase = PeerPhaseStaged
				scenario.client.mu.Unlock()
				return nil
			},
			check: func(t *testing.T, response PeerObservabilityResponseDTO, err error) {
				notClosed(t, err)
				if response.Status != 0 {
					t.Fatalf("a staged caller produced a response: %+v", response)
				}
			},
		},
		"local member stops advertising observability": {
			change: func(scenario *obsScriptScenario) error {
				local := scenario.clientIdentity
				local.ObservabilitySupported = false
				return scenario.client.UpdateLocalIdentity(local)
			},
			check: func(t *testing.T, response PeerObservabilityResponseDTO, err error) {
				if err != nil || response.Status != PeerObservabilityStatusUnsupported || response.ResponderMemberID != 1 ||
					response.Kind != PeerObservabilityMetrics || len(response.Body) != 0 {
					t.Fatalf("query = %+v, %v; want an UNSUPPORTED answer without a body", response, err)
				}
			},
		},
		"peer stops advertising observability": {
			change: func(scenario *obsScriptScenario) error {
				peer := scenario.peerIdentity
				peer.ObservabilitySupported = false
				return scenario.client.ReconcilePeers([]PeerIdentityDTO{peer})
			},
			check: func(t *testing.T, response PeerObservabilityResponseDTO, err error) {
				if err != nil || response.Status != PeerObservabilityStatusUnsupported || response.ResponderMemberID != 1 ||
					response.Kind != PeerObservabilityMetrics || len(response.Body) != 0 {
					t.Fatalf("query = %+v, %v; want an UNSUPPORTED answer without a body", response, err)
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := make(chan error, 4)
			requestSeen := make(chan error, 4)
			scenario := newObsScriptScenario(t, nil)
			scenario.server.serve(func(connection *tls.Conn) {
				if err := obsScriptReadHello(connection); err != nil {
					changed <- err
					requestSeen <- err
					return
				}
				changed <- test.change(scenario)
				if err := obsScriptWriteHello(connection, scenario.server.identity); err != nil {
					requestSeen <- err
					return
				}
				_, err := readDynamicFrame(connection, peerObservabilityRequestSize)
				requestSeen <- err
			})
			response, err := scenario.query(PeerObservabilityMetrics)
			if changeErr := obsAwait(t, changed, "the membership change"); changeErr != nil {
				t.Fatalf("membership change failed: %v", changeErr)
			}
			test.check(t, response, err)
			if requestErr := obsAwait(t, requestSeen, "the peer to finish"); requestErr == nil {
				t.Fatal("a request frame reached the peer although the query could not proceed")
			}
			if open := scenario.openConnections(); open != 0 {
				t.Fatalf("query left %d tracked connections", open)
			}
		})
	}
}

func TestPeerObservabilityQueryValidatesPeerResponse(t *testing.T) {
	const body = `{"nodeId":"1"}`
	respondWith := func(response PeerObservabilityResponseDTO) func(connection net.Conn) {
		return func(connection net.Conn) {
			payload, err := encodePeerObservabilityResponse(response)
			if err != nil {
				return
			}
			_ = writeDynamicFrame(connection, payload, uint32(peerObservabilityResponseLimit))
		}
	}
	for name, test := range map[string]struct {
		respond     func(connection net.Conn)
		wantOK      bool
		wantInvalid bool
	}{
		"well-formed response": {
			respond: respondWith(PeerObservabilityResponseDTO{
				Status: PeerObservabilityStatusOK, ResponderMemberID: 1, Kind: PeerObservabilityNode, Body: []byte(body),
			}),
			wantOK: true,
		},
		"answer from another member": {
			respond: respondWith(PeerObservabilityResponseDTO{
				Status: PeerObservabilityStatusOK, ResponderMemberID: 9, Kind: PeerObservabilityNode, Body: []byte(body),
			}),
			wantInvalid: true,
		},
		"answer for another kind": {
			respond: respondWith(PeerObservabilityResponseDTO{
				Status: PeerObservabilityStatusOK, ResponderMemberID: 1, Kind: PeerObservabilityMetrics, Body: []byte(body),
			}),
			wantInvalid: true,
		},
		"trailing byte after the response": {
			respond: func(connection net.Conn) {
				respondWith(PeerObservabilityResponseDTO{
					Status: PeerObservabilityStatusOK, ResponderMemberID: 1, Kind: PeerObservabilityNode, Body: []byte(body),
				})(connection)
				_, _ = connection.Write([]byte{0x01})
			},
		},
		"peer closes without answering": {
			respond: func(net.Conn) {},
		},
	} {
		t.Run(name, func(t *testing.T) {
			scenario := newObsScriptScenario(t, nil)
			scenario.server.serve(func(connection *tls.Conn) {
				if obsScriptReadHello(connection) != nil || obsScriptWriteHello(connection, scenario.server.identity) != nil {
					return
				}
				if _, err := readDynamicFrame(connection, peerObservabilityRequestSize); err != nil {
					return
				}
				test.respond(connection)
			})
			response, err := scenario.query(PeerObservabilityNode)
			if test.wantOK {
				if err != nil || response.Status != PeerObservabilityStatusOK || response.ResponderMemberID != 1 ||
					response.Kind != PeerObservabilityNode || string(response.Body) != body {
					t.Fatalf("query = %+v, %v; want the peer's OK response", response, err)
				}
				return
			}
			if err == nil || response.Status != 0 || response.Body != nil {
				t.Fatalf("query = %+v, %v; want a failure and no response", response, err)
			}
			if test.wantInvalid != errors.Is(err, ErrInvalidPeerMessage) {
				t.Fatalf("error = %v, ErrInvalidPeerMessage = %v, want %v", err, errors.Is(err, ErrInvalidPeerMessage), test.wantInvalid)
			}
			if test.wantInvalid {
				_, readerErr := NewPeerObservabilityReader(scenario.client).QueryNodeSnapshot(context.Background(), "1")
				requirePeerQueryCode(t, readerErr, observationports.QueryErrorInvalidResponse)
			}
			if open := scenario.openConnections(); open != 0 {
				t.Fatalf("failed query left %d tracked connections", open)
			}
		})
	}
}

func obsDialTLS(t *testing.T, node *cptNode, serverName, address string) *tls.Conn {
	t.Helper()
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: cptWaitTimeout}, Config: node.dialConfig(serverName)}
	ctx, cancel := context.WithTimeout(context.Background(), cptWaitTimeout)
	defer cancel()
	socket, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatalf("dial observability listener: %v", err)
	}
	connection, ok := socket.(*tls.Conn)
	if !ok {
		_ = socket.Close()
		t.Fatal("TLS dial did not return a TLS connection")
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

// obsInboundFixture is a real serving member (2) that a hand-driven committed peer (1) talks to.
type obsInboundFixture struct {
	server     *DynamicTCPPeerTransport
	serverNode *cptNode
	clientNode *cptNode
	local      PeerIdentityDTO
	remote     PeerIdentityDTO
	address    string
	calls      atomic.Int32
	mu         sync.Mutex
	respond    func() ([]byte, error)
}

func newObsInboundFixture(t *testing.T) *obsInboundFixture {
	t.Helper()
	fixture := &obsInboundFixture{
		serverNode: cptNewNode(t, "server.example"), clientNode: cptNewNode(t, "client.example"),
		respond: func() ([]byte, error) { return []byte(`{"ok":true}`), nil },
	}
	cptTrustEachOther(fixture.serverNode, fixture.clientNode)
	listener := cptListen(t)
	fixture.address = listener.Addr().String()
	fixture.local = peerObservabilityTestIdentity(fixture.serverNode, 2, fixture.address)
	fixture.remote = peerObservabilityTestIdentity(fixture.clientNode, 1, "127.0.0.1:19001")
	fixture.server = newPeerObservabilityTestTransport(t, fixture.serverNode, listener, fixture.local,
		[]PeerIdentityDTO{fixture.remote}, func(context.Context, PeerObservabilityKind) ([]byte, error) {
			fixture.calls.Add(1)
			fixture.mu.Lock()
			respond := fixture.respond
			fixture.mu.Unlock()
			return respond()
		})
	startPeerObservabilityTestTransport(t, fixture.server)
	return fixture
}

func (fixture *obsInboundFixture) dial(t *testing.T) *tls.Conn {
	t.Helper()
	connection := dialPeerObservabilityTest(t, fixture.clientNode, fixture.serverNode.dnsName, fixture.address, fixture.remote)
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func (fixture *obsInboundFixture) sendRequest(t *testing.T, connection net.Conn, kind PeerObservabilityKind) {
	t.Helper()
	request, err := encodePeerObservabilityRequest(PeerObservabilityRequestDTO{Kind: kind, TargetMemberID: fixture.local.MemberID})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDynamicFrame(connection, request, peerObservabilityRequestSize); err != nil {
		t.Fatalf("write request: %v", err)
	}
}

func (fixture *obsInboundFixture) readResponse(t *testing.T, connection net.Conn, kind PeerObservabilityKind) PeerObservabilityResponseDTO {
	t.Helper()
	payload, err := readDynamicFrame(connection, uint32(peerObservabilityResponseLimit))
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	response, err := decodePeerObservabilityResponse(payload, kind, fixture.local.MemberID)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return response
}

func (fixture *obsInboundFixture) requireSilence(t *testing.T, connection net.Conn) {
	t.Helper()
	if payload, err := readDynamicFrame(connection, uint32(peerObservabilityResponseLimit)); err == nil {
		t.Fatalf("server answered with %d bytes, want it to close without a response", len(payload))
	}
	if calls := fixture.calls.Load(); calls != 0 {
		t.Fatalf("local snapshot handler ran %d times for a rejected request", calls)
	}
}

func TestPeerObservabilityServerClosesSilentlyOnUntrustedHelloOrRequest(t *testing.T) {
	t.Run("malformed hello", func(t *testing.T) {
		fixture := newObsInboundFixture(t)
		connection := obsDialTLS(t, fixture.clientNode, fixture.serverNode.dnsName, fixture.address)
		if err := writeDynamicFrame(connection, []byte(peerObservabilityMagic+"junk"), dynamicHelloLimit); err != nil {
			t.Fatal(err)
		}
		if _, err := readDynamicFrame(connection, dynamicHelloLimit); err == nil {
			t.Fatal("server answered a malformed hello")
		}
		fixture.requireSilence(t, connection)
	})
	t.Run("member that is not committed", func(t *testing.T) {
		fixture := newObsInboundFixture(t)
		stranger := peerObservabilityTestIdentity(fixture.clientNode, 99, "127.0.0.1:19099")
		connection, err := dialAndSendPeerObservabilityHello(fixture.clientNode, fixture.serverNode.dnsName, fixture.address, stranger)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		if _, err := readDynamicFrame(connection, dynamicHelloLimit); err == nil {
			t.Fatal("server answered the hello of an uncommitted member")
		}
		fixture.requireSilence(t, connection)
	})
	t.Run("serving member cannot encode its own hello", func(t *testing.T) {
		fixture := newObsInboundFixture(t)
		fixture.server.mu.Lock()
		fixture.server.local.ClusterID = ""
		fixture.server.mu.Unlock()
		connection, err := dialAndSendPeerObservabilityHello(fixture.clientNode, fixture.serverNode.dnsName, fixture.address, fixture.remote)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		if _, err := readDynamicFrame(connection, dynamicHelloLimit); err == nil {
			t.Fatal("server answered although its own identity is invalid")
		}
		fixture.requireSilence(t, connection)
	})
	t.Run("request frame above the request size", func(t *testing.T) {
		fixture := newObsInboundFixture(t)
		connection := fixture.dial(t)
		if err := writeDynamicFrame(connection, make([]byte, peerObservabilityRequestSize+1), 64); err != nil {
			t.Fatal(err)
		}
		fixture.requireSilence(t, connection)
	})
}

func TestPeerObservabilityServerRechecksCommitmentBeforeServing(t *testing.T) {
	t.Run("peer removed after hello", func(t *testing.T) {
		fixture := newObsInboundFixture(t)
		connection := fixture.dial(t)
		if err := fixture.server.ReconcilePeers(nil); err != nil {
			t.Fatal(err)
		}
		fixture.sendRequest(t, connection, PeerObservabilityNode)
		fixture.requireSilence(t, connection)
	})
	t.Run("peer phase changed after hello", func(t *testing.T) {
		fixture := newObsInboundFixture(t)
		connection := fixture.dial(t)
		changed := fixture.remote
		changed.Phase, changed.OperationID = PeerPhaseLearner, testOperationID
		if err := fixture.server.ReconcilePeers([]PeerIdentityDTO{changed}); err != nil {
			t.Fatal(err)
		}
		fixture.sendRequest(t, connection, PeerObservabilityNode)
		fixture.requireSilence(t, connection)
	})
}

func TestPeerObservabilityServerAnswersWithStatusInsteadOfSnapshot(t *testing.T) {
	t.Run("serving member stops advertising observability", func(t *testing.T) {
		fixture := newObsInboundFixture(t)
		connection := fixture.dial(t)
		local := fixture.local
		local.ObservabilitySupported = false
		if err := fixture.server.UpdateLocalIdentity(local); err != nil {
			t.Fatal(err)
		}
		fixture.sendRequest(t, connection, PeerObservabilityNode)
		response := fixture.readResponse(t, connection, PeerObservabilityNode)
		if response.Status != PeerObservabilityStatusUnsupported || len(response.Body) != 0 || fixture.calls.Load() != 0 {
			t.Fatalf("response = %+v, handler calls = %d; want UNSUPPORTED without running the handler", response, fixture.calls.Load())
		}
	})
	t.Run("requesting member stops advertising observability", func(t *testing.T) {
		fixture := newObsInboundFixture(t)
		connection := fixture.dial(t)
		remote := fixture.remote
		remote.ObservabilitySupported = false
		if err := fixture.server.ReconcilePeers([]PeerIdentityDTO{remote}); err != nil {
			t.Fatal(err)
		}
		fixture.sendRequest(t, connection, PeerObservabilityMetrics)
		response := fixture.readResponse(t, connection, PeerObservabilityMetrics)
		if response.Status != PeerObservabilityStatusUnsupported || len(response.Body) != 0 || fixture.calls.Load() != 0 {
			t.Fatalf("response = %+v, handler calls = %d; want UNSUPPORTED without running the handler", response, fixture.calls.Load())
		}
	})
	t.Run("handler removed", func(t *testing.T) {
		fixture := newObsInboundFixture(t)
		connection := fixture.dial(t)
		fixture.server.mu.Lock()
		fixture.server.observabilityHandler = nil
		fixture.server.mu.Unlock()
		fixture.sendRequest(t, connection, PeerObservabilityNode)
		response := fixture.readResponse(t, connection, PeerObservabilityNode)
		if response.Status != PeerObservabilityStatusInternalError || len(response.Body) != 0 {
			t.Fatalf("response = %+v, want INTERNAL_ERROR without a body", response)
		}
	})
	t.Run("handler fails", func(t *testing.T) {
		fixture := newObsInboundFixture(t)
		fixture.mu.Lock()
		fixture.respond = func() ([]byte, error) {
			return []byte(`{"leak":"partial snapshot"}`), errors.New("query backend failed")
		}
		fixture.mu.Unlock()
		connection := fixture.dial(t)
		fixture.sendRequest(t, connection, PeerObservabilityNode)
		response := fixture.readResponse(t, connection, PeerObservabilityNode)
		if response.Status != PeerObservabilityStatusInternalError || len(response.Body) != 0 {
			t.Fatalf("response = %+v, want INTERNAL_ERROR without the handler's partial body", response)
		}
		if fixture.calls.Load() != 1 {
			t.Fatalf("handler ran %d times, want 1", fixture.calls.Load())
		}
	})
}
