package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
)

func newReadAdmissionTestCluster(t *testing.T) [3]*clusterRuntime {
	t.Helper()
	directory := t.TempDir()
	ca, caKey, caPEM := newClusterRuntimeTestCA(t)
	caPath := filepath.Join(directory, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	var runtimes [3]*clusterRuntime
	var identities [3]tcpadapter.PeerIdentityDTO
	var configs [3]clusterConfigFile
	var listeners [3]net.Listener
	for index := range runtimes {
		id := uint64(index + 1)
		dns := fmt.Sprintf("node%d.example.test", id)
		cert, key, fingerprint := issueClusterRuntimeTestCertificate(t, ca, caKey, dns, int64(id+1))
		certPath := filepath.Join(directory, fmt.Sprintf("node%d.pem", id))
		keyPath := filepath.Join(directory, fmt.Sprintf("node%d.key", id))
		if err := os.WriteFile(certPath, cert, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyPath, key, 0o600); err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[index] = listener
		t.Cleanup(func() { listener.Close() })
		configs[index] = clusterConfigFile{
			ClusterID: "read-admission", NodeID: id, LocalDNSName: dns,
			AdvertisedAddress: listener.Addr().String(), ReleaseVersion: buildReleaseVersion,
			ClusterProtocolMajor: 2, StateSchemaMajor: 1,
			PeerTLS: clusterPeerTLS{CertFile: certPath, KeyFile: keyPath, CAFile: caPath},
		}
		identities[index] = configuredLocalPeerIdentity(configs[index], fingerprint)
		identities[index].Phase = tcpadapter.PeerPhaseVoter
		node, err := raftadapter.Open(raftadapter.Config{
			NodeID: raftadapter.NodeID(id), DynamicMembership: true, ClusterID: "read-admission",
			CertificateSHA256: fingerprint, BootstrapVoterIDs: []raftadapter.NodeID{1, 2, 3},
			Directory: filepath.Join(directory, fmt.Sprintf("wal%d", id)),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { node.Close() })
		runtimes[index] = &clusterRuntime{
			config: configs[index], node: node, requests: make(chan clusterDriverRequest, 128),
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
	}
	// Establish a committed term before involving the bounded socket queues.
	settle := func() {
		for range 64 {
			var messages []raftadapter.MessageDTO
			for _, runtime := range runtimes {
				batch, err := processReadyForTest(t, runtime.node)
				if err != nil {
					t.Fatal(err)
				}
				messages = append(messages, batch.Messages...)
			}
			if len(messages) == 0 {
				return
			}
			for _, message := range messages {
				if _, err := runtimes[message.To-1].node.Step(message); err != nil {
					t.Fatal(err)
				}
			}
		}
		t.Fatal("Raft election did not settle")
	}
	settle()
	if err := runtimes[0].node.Campaign(); err != nil {
		t.Fatal(err)
	}
	settle()
	for index, runtime := range runtimes {
		serverTLS, clientTLS, _, err := buildClusterPeerTLS(configs[index])
		if err != nil {
			t.Fatal(err)
		}
		var peers []tcpadapter.PeerIdentityDTO
		for peerIndex, identity := range identities {
			if peerIndex != index {
				peers = append(peers, identity)
			}
		}
		transport, err := tcpadapter.NewDynamicTCPPeerTransport(tcpadapter.DynamicPeerTransportConfig{
			LocalIdentity: identities[index], Listener: listeners[index],
			ServerTLS: serverTLS, ClientTLS: clientTLS, Peers: peers,
			AdmissionHandler: func(context.Context, tcpadapter.PeerIdentityDTO) (tcpadapter.SeedAdmissionResponseDTO, error) {
				return tcpadapter.SeedAdmissionResponseDTO{}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		runtime.transport = transport
		runtime.inboundQueue = transport
		t.Cleanup(func() { transport.Close() })
	}
	return runtimes
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.4.
func TestClusterFollowerReadBurstCompletesAfterEgressDrain(t *testing.T) {
	testClusterReadBurst(t, []int{1}, 3)
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.4.
func TestClusterLeaderReadBurstPreservesHealthyQuorumWithBlockedMinority(t *testing.T) {
	testClusterReadBurst(t, []int{0}, 2)
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.4.
func TestClusterConcurrentOriginsReadBurstCompletesAfterEgressDrain(t *testing.T) {
	testClusterReadBurst(t, []int{0, 1, 2}, 3)
}

func testClusterReadBurst(t *testing.T, issuers []int, connected int) {
	t.Helper()
	runtimes := newReadAdmissionTestCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	results := make([]chan clusterReadResponse, 64*len(issuers))
	var reads [3]map[string]clusterReadWaiter
	var proposals [3][]clusterProposalWaiter
	for index := range reads {
		reads[index] = make(map[string]clusterReadWaiter)
	}
	for index := range results {
		results[index] = make(chan clusterReadResponse, 1)
		issuer := issuers[index/64]
		runtimes[issuer].requests <- clusterDriverRequest{
			kind: clusterDriverRead, contextKey: fmt.Sprintf("fresh-%d-%d", issuer, index),
			context: ctx, readResult: results[index],
		}
	}
	process := func(index int) {
		runtime := runtimes[index]
		if err := runtime.stageQueuedDriverWork(reads[index], &proposals[index], ctx.Done()); err != nil {
			t.Fatal(err)
		}
		batch, err := processReadyForTest(t, runtime.node)
		if err != nil {
			t.Fatal(err)
		}
		runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
		runtime.completeReadStates(batch.ReadStates, reads[index])
		if err := runtime.sendMessages(ctx, batch.Messages); err != nil {
			t.Fatal(err)
		}
	}
	// No writer exists yet: the first burst must survive the fixed egress cap.
	for _, issuer := range issuers {
		process(issuer)
	}
	serveResults := make(chan error, 3)
	for _, runtime := range runtimes[:connected] {
		go func() { serveResults <- runtime.transport.Serve(ctx) }()
	}
	defer func() {
		cancel()
		for range connected {
			if err := <-serveResults; err != nil {
				t.Errorf("transport shutdown: %v", err)
			}
		}
	}()
	completed := 0
	for completed < len(results) && ctx.Err() == nil {
		for index := range runtimes {
			process(index)
		}
		for index, result := range results {
			if result == nil {
				continue
			}
			select {
			case response := <-result:
				if response.err != nil || response.index == 0 {
					t.Fatalf("request %d result=%+v", index, response)
				}
				results[index] = nil
				completed++
			default:
			}
		}
		time.Sleep(time.Millisecond)
	}
	if completed != len(results) {
		t.Fatalf("fresh proofs completed=%d/%d after egress drain", completed, len(results))
	}
	for _, runtime := range runtimes {
		for _, id := range []uint64{1, 2, 3} {
			if id == runtime.config.NodeID {
				continue
			}
			status, err := runtime.transport.Status(id)
			if err != nil || (id <= uint64(connected) && status.RejectedMessages != 0) || status.OutboundQueuedMessages > 32 || status.OutboundQueuedBytes > 4<<20 {
				t.Fatalf("node %d peer %d status=%+v err=%v", runtime.config.NodeID, id, status, err)
			}
		}
	}
	t.Logf("all %d independent fresh proofs completed within %s with %d connected nodes; no healthy-peer rejections", len(results), clusterReadTimeout, connected)
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.4.
func TestClusterUnissuedReadReturnsCancellationAndDriverFailure(t *testing.T) {
	for _, reason := range []string{"count", "bytes", "driver exit"} {
		t.Run(reason, func(t *testing.T) {
			runtimes := newReadAdmissionTestCluster(t)
			runtime := runtimes[1]
			if reason == "bytes" {
				for range 3 {
					if err := runtime.transport.Send(tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: make([]byte, 1<<20)}); err != nil {
						t.Fatal(err)
					}
				}
				if err := runtime.transport.Send(tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: make([]byte, (1<<20)-144)}); err != nil {
					t.Fatal(err)
				}
			} else {
				for range 32 {
					if err := runtime.transport.Send(tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: []byte{1}}); err != nil {
						t.Fatal(err)
					}
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan clusterReadResponse, 1)
			request := clusterDriverRequest{kind: clusterDriverRead, contextKey: "unissued", context: ctx, readResult: result}
			reads := make(map[string]clusterReadWaiter)
			var proposals []clusterProposalWaiter
			if runtime.tryDriverRequest(request, reads, &proposals) {
				t.Fatal("read was issued without egress space")
			}
			want := context.Canceled
			if reason == "driver exit" {
				want = context.DeadlineExceeded
				runtime.failQueuedDriverRequests(want)
			} else {
				cancel()
				runtime.stageQueuedDriverRequests(1, reads, &proposals)
			}
			select {
			case response := <-result:
				if response.err != want {
					t.Fatalf("unissued read result=%v want=%v", response.err, want)
				}
			default:
				t.Fatal("unissued read lost its cancellation or driver failure result")
			}
			if runtime.hasDeferredRequest || len(reads) != 0 {
				t.Fatal("completed unissued read remained owned by the driver")
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.5.
func TestClusterLeaderlessReadCompletesWhenLeaderReturns(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	issuer := runtimes[1]
	baseline := issuer.node.Status().AppliedIndex
	if err := issuer.node.ForgetLeader(); err != nil {
		t.Fatal(err)
	}
	if issuer.node.Status().LeaderID != 0 {
		t.Fatal("follower did not forget its leader")
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	result := make(chan clusterReadResponse, 1)
	var reads [3]map[string]clusterReadWaiter
	var proposals [3][]clusterProposalWaiter
	for index := range reads {
		reads[index] = make(map[string]clusterReadWaiter)
	}
	issuer.tryDriverRequest(clusterDriverRequest{
		kind: clusterDriverRead, contextKey: "read-without-leader", context: ctx, readResult: result,
	}, reads[1], &proposals[1])
	for range 64 {
		if err := runtimes[0].node.Tick(); err != nil {
			t.Fatal(err)
		}
		var messages []raftadapter.MessageDTO
		for index, runtime := range runtimes {
			runtime.stageQueuedDriverRequests(clusterDriverStageLimit, reads[index], &proposals[index])
			batch, err := processReadyForTest(t, runtime.node)
			if err != nil {
				t.Fatal(err)
			}
			runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
			runtime.completeReadStates(batch.ReadStates, reads[index])
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if _, err := runtimes[message.To-1].node.Step(message); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case response := <-result:
			if response.err != nil || response.index < baseline || response.index > issuer.node.Status().AppliedIndex {
				t.Fatalf("restored-leader quorum proof = %+v, baseline=%d applied=%d", response, baseline, issuer.node.Status().AppliedIndex)
			}
			if issuer.node.Status().CommitIndex != baseline {
				t.Fatalf("ReadIndex changed commit index from %d to %d", baseline, issuer.node.Status().CommitIndex)
			}
			return
		default:
		}
	}
	t.Fatalf("leader restored to %d but the original read never completed", issuer.node.Status().LeaderID)
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.5.
func TestClusterIssuedReadRecoversAcrossLeadershipTransfer(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	issuer := runtimes[0]
	baseline := issuer.node.Status().AppliedIndex
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	result := make(chan clusterReadResponse, 1)
	var reads [3]map[string]clusterReadWaiter
	var proposals [3][]clusterProposalWaiter
	for index := range reads {
		reads[index] = make(map[string]clusterReadWaiter)
	}
	issuer.tryDriverRequest(clusterDriverRequest{
		kind: clusterDriverRead, contextKey: "issued-before-transfer", context: ctx, readResult: result,
	}, reads[0], &proposals[0])
	// The old leader accepted this read, but its quorum heartbeats did not
	// arrive before leadership changed. Do not deliver this Ready's messages.
	batch, err := processReadyForTest(t, issuer.node)
	if err != nil {
		t.Fatal(err)
	}
	issuer.projectionIndex.Store(baseline)
	issuer.completeReadStates(batch.ReadStates, reads[0])
	select {
	case response := <-result:
		t.Fatalf("read completed without a remote quorum: %+v", response)
	default:
	}
	if err := issuer.node.TransferLeadership(2); err != nil {
		t.Fatal(err)
	}
	for range 64 {
		var messages []raftadapter.MessageDTO
		for index, runtime := range runtimes {
			if err := runtime.stageQueuedDriverWork(reads[index], &proposals[index], nil); err != nil {
				t.Fatal(err)
			}
			batch, err := processReadyForTest(t, runtime.node)
			if err != nil {
				t.Fatal(err)
			}
			runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
			runtime.completeReadStates(batch.ReadStates, reads[index])
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if _, err := runtimes[message.To-1].node.Step(message); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case response := <-result:
			status := issuer.node.Status()
			if response.err != nil || response.index <= baseline || response.index > status.AppliedIndex ||
				status.Role != raftadapter.RoleFollower || status.LeaderID != 2 || ctx.Err() != nil {
				t.Fatalf("new leader fresh proof=%+v status=%+v baseline=%d context=%v", response, status, baseline, ctx.Err())
			}
			return
		default:
		}
	}
	t.Fatalf("issued read was lost across leadership transfer: %+v", issuer.node.Status())
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.5.
func TestClusterFreshReadsFromConcurrentOriginsDoNotCollide(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	var requests [3]*clusterReadRequest
	var reads [3]map[string]clusterReadWaiter
	var proposals [3][]clusterProposalWaiter
	baseline := runtimes[0].node.Status().AppliedIndex
	for index, runtime := range runtimes {
		reads[index] = make(map[string]clusterReadWaiter)
		request, err := runtime.beginReadBarrier(ctx)
		if err != nil {
			t.Fatal(err)
		}
		requests[index] = request
	}
	var completed [3]bool
	for range 64 {
		var messages []raftadapter.MessageDTO
		for index, runtime := range runtimes {
			if err := runtime.stageQueuedDriverWork(reads[index], &proposals[index], nil); err != nil {
				t.Fatal(err)
			}
			batch, err := processReadyForTest(t, runtime.node)
			if err != nil {
				t.Fatal(err)
			}
			runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
			runtime.completeReadStates(batch.ReadStates, reads[index])
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if _, err := runtimes[message.To-1].node.Step(message); err != nil {
				t.Fatal(err)
			}
		}
		for index, request := range requests {
			if completed[index] {
				continue
			}
			select {
			case response := <-request.result:
				status := runtimes[index].node.Status()
				if response.err != nil || response.index != baseline || status.CommitIndex != baseline {
					t.Fatalf("node %d fresh read=%+v status=%+v baseline=%d", index+1, response, status, baseline)
				}
				completed[index] = true
			default:
			}
		}
		if completed == [3]bool{true, true, true} {
			return
		}
	}
	t.Fatalf("concurrent origins lost fresh proofs: completed=%v", completed)
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.5.
func TestClusterReissuedReadRejectsPreviousQuorumProof(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	issuer := runtimes[0]
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	request, err := issuer.beginReadBarrier(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var reads [3]map[string]clusterReadWaiter
	var proposals [3][]clusterProposalWaiter
	for index := range reads {
		reads[index] = make(map[string]clusterReadWaiter)
	}
	pump := func(hold bool) []raftadapter.ReadStateDTO {
		var held []raftadapter.ReadStateDTO
		var messages []raftadapter.MessageDTO
		for index, runtime := range runtimes {
			if err := runtime.stageQueuedDriverWork(reads[index], &proposals[index], nil); err != nil {
				t.Fatal(err)
			}
			batch, err := processReadyForTest(t, runtime.node)
			if err != nil {
				t.Fatal(err)
			}
			runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
			if index == 0 && hold {
				held = append(held, batch.ReadStates...)
			} else {
				runtime.completeReadStates(batch.ReadStates, reads[index])
			}
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if _, err := runtimes[message.To-1].node.Step(message); err != nil {
				t.Fatal(err)
			}
		}
		return held
	}
	var previous []raftadapter.ReadStateDTO
	for range 64 {
		previous = pump(true)
		if len(previous) != 0 {
			break
		}
	}
	if len(previous) != 1 {
		t.Fatalf("actual Raft quorum returned %d proofs for one read", len(previous))
	}
	if err := issuer.node.TransferLeadership(2); err != nil {
		t.Fatal(err)
	}
	for range 64 {
		pump(true)
		if issuer.node.Status().Role == raftadapter.RoleFollower && issuer.node.Status().LeaderID == 2 {
			break
		}
	}
	issuer.refreshReadWaiters(reads[0])
	issuer.completeReadStates(previous, reads[0])
	select {
	case response := <-request.result:
		t.Fatalf("previous emission completed the replacement fresh proof: %+v", response)
	default:
	}
	for range 64 {
		pump(false)
		select {
		case response := <-request.result:
			if response.err != nil || response.index <= previous[0].Index ||
				response.index > issuer.node.Status().AppliedIndex || ctx.Err() != nil {
				t.Fatalf("replacement fresh proof=%+v previous=%+v", response, previous[0])
			}
			return
		default:
		}
	}
	t.Fatal("replacement fresh proof did not complete on the new leader")
}
