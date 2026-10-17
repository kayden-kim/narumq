package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/membership"
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

func readAdmissionTestMachine(t *testing.T, runtimes [3]*clusterRuntime) *membership.StateMachine {
	t.Helper()
	members := make([]membership.MemberIdentityDTO, len(runtimes))
	for index, runtime := range runtimes {
		identity := runtime.transport.LocalIdentity()
		members[index] = membership.MemberIdentityDTO{
			MemberID: identity.MemberID, ClusterID: identity.ClusterID, Address: identity.Address,
			DNSSAN: identity.DNSSAN, CertificateSHA256: append([]byte(nil), identity.CertificateSHA256[:]...),
			ReleaseVersion: identity.ReleaseVersion, ClusterProtocolMajor: identity.ClusterProtocolMajor,
			ClusterProtocolMinor: identity.ClusterProtocolMinor, StateSchemaMajor: identity.StateSchemaMajor,
			StateSchemaMinor: identity.StateSchemaMinor, Phase: membership.MemberPhaseVoter,
		}
	}
	machine := membership.NewStateMachine()
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatalf("initialize read-admission membership machine: %v", err)
	}
	return machine
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.4.
// Scenarios: performance-and-verification.md:PRF-SCN-011
func TestClusterFollowerReadBurstCompletesAfterEgressDrain(t *testing.T) {
	testClusterReadBurst(t, []int{1}, 3, 3)
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.4.
// Scenarios: performance-and-verification.md:PRF-SCN-012
func TestClusterLeaderReadBurstPreservesHealthyQuorumWithBlockedMinority(t *testing.T) {
	testClusterReadBurst(t, []int{0}, 2, 2)
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.4.
// Scenarios: performance-and-verification.md:PRF-SCN-011, performance-and-verification.md:PRF-SCN-012
func TestClusterConcurrentOriginsReadBurstCompletesAfterEgressDrain(t *testing.T) {
	testClusterReadBurst(t, []int{0, 1, 2}, 2, 3)
}

func testClusterReadBurst(t *testing.T, issuers []int, initiallyConnected, connected int) {
	t.Helper()
	runtimes := newReadAdmissionTestCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	var baseline [3]uint64
	var queueLimits [3][3]tcpadapter.PeerOutboundCapacityDTO
	for index, runtime := range runtimes {
		baseline[index] = runtime.node.Status().AppliedIndex
		for peerIndex := range runtimes {
			if peerIndex == index {
				continue
			}
			capacity, err := runtime.transport.OutboundCapacity(uint64(peerIndex + 1))
			if err != nil {
				t.Fatal(err)
			}
			queueLimits[index][peerIndex] = capacity
		}
	}
	checkQueueBounds := func(index int) {
		t.Helper()
		for peerIndex, limit := range queueLimits[index] {
			if peerIndex == index {
				continue
			}
			status, err := runtimes[index].transport.Status(uint64(peerIndex + 1))
			if err != nil || status.OutboundQueuedMessages > limit.Messages || status.OutboundQueuedBytes > limit.Bytes {
				t.Fatalf("node %d peer %d status=%+v limit=%+v err=%v", index+1, peerIndex+1, status, limit, err)
			}
		}
	}
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
		checkQueueBounds(index)
	}
	// No writer exists yet: the first burst must survive the fixed egress cap.
	for _, issuer := range issuers {
		process(issuer)
	}
	serveResults := make(chan error, 3)
	serving := initiallyConnected
	startTransports := func(from, to int) {
		for _, runtime := range runtimes[from:to] {
			go func() { serveResults <- runtime.transport.Serve(ctx) }()
		}
	}
	startTransports(0, serving)
	defer func() {
		cancel()
		for range serving {
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
				issuer := issuers[index/64]
				if response.err != nil || response.index == 0 || response.index < baseline[issuer] {
					t.Fatalf("request %d result=%+v baseline=%d", index, response, baseline[issuer])
				}
				results[index] = nil
				completed++
			default:
			}
		}
		if serving < connected && completed >= 64*initiallyConnected {
			// The majority has completed its burst without the last peer draining.
			// Now its original 64 requests must complete under the same deadline.
			startTransports(serving, connected)
			serving = connected
		}
		goruntime.Gosched()
	}
	if completed != len(results) {
		t.Fatalf("fresh proofs completed=%d/%d after egress drain", completed, len(results))
	}
	for index := range runtimes {
		checkQueueBounds(index)
	}
	t.Logf("all %d independent fresh proofs reached their pre-burst applied index within %s with %d connected nodes; peer queues stayed bounded", len(results), clusterReadTimeout, connected)
}

// Traceability: CLU-14; docs/specs/performance-and-verification.md §3.4.
// Scenarios: performance-and-verification.md:PRF-SCN-013
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
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-059, performance-and-verification.md:PRF-SCN-026
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
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-059, performance-and-verification.md:PRF-SCN-026
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
// Scenarios: performance-and-verification.md:PRF-SCN-026
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
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-059, performance-and-verification.md:PRF-SCN-026
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

// Traceability: CLU-07; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-073
func TestClusterDurableCommitCallerTimeoutLateReplayAppliesOnceWithoutSuccess(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	ingress := runtimes[1]
	ingress.storage = newClusterStorageMonitor(time.Now, nil)
	ingress.machine = retainedTestMachine(t, ingress.config, ingress.config.ReleaseVersion)
	ingress.publishPressure = &clusterPublishPressureController{runtime: ingress, maxCommitLag: time.Second}
	backlogContains := func(pending *clusterPendingCommit) bool {
		b := &ingress.publishPressure.backlog
		b.mu.Lock()
		defer b.mu.Unlock()
		for entry := b.entries.Front(); entry != nil; entry = entry.Next() {
			if entry == pending.backlogEntry {
				return true
			}
		}
		return false
	}
	var reads [3]map[string]clusterReadWaiter
	var proposals [3][]clusterProposalWaiter
	for index := range reads {
		reads[index] = make(map[string]clusterReadWaiter)
	}
	type waitOutcome struct {
		result membership.ApplyResultDTO
		err    error
		at     time.Time
	}
	begin := func(ctx context.Context, fill byte) (*clusterPendingCommit, <-chan waitOutcome) {
		t.Helper()
		pending, err := ingress.BeginDelivery(ctx, membership.DeliveryChangeDTO{
			TenantID: "tenant-a", ClientID: "publisher-a", Action: membership.DeliveryActionPublish,
			ProposedAtUnixMillis: 1_700_000_000_000 + uint64(fill),
			Topic:                "events/late", Payload: bytes.Repeat([]byte{fill}, 40_000), PublishQoS: 1,
			Targets: []membership.DeliveryTargetDTO{{ClientID: "client-a"}},
		})
		if err != nil {
			t.Fatalf("begin durable publish %c: %v", fill, err)
		}
		commit, ok := pending.(*clusterPendingCommit)
		if !ok {
			t.Fatalf("durable publish %c returned pending type %T", fill, pending)
		}
		outcomes := make(chan waitOutcome, 1)
		go func() {
			result, err := commit.Wait()
			outcomes <- waitOutcome{result: result, err: err, at: time.Now()}
		}()
		return commit, outcomes
	}
	callerCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	timedOut, timedOutWait := begin(callerCtx, 'a')
	later, laterWait := begin(context.Background(), 'b')

	// Confirm both fresh proofs without submitting either command yet.
	for round := 0; round < 64 && (!timedOut.readConfirmed || !later.readConfirmed); round++ {
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
	}
	if !timedOut.readConfirmed || timedOut.readErr != nil || !later.readConfirmed || later.readErr != nil {
		t.Fatalf("fresh proofs did not complete before submission: first=%t/%v later=%t/%v",
			timedOut.readConfirmed, timedOut.readErr, later.readConfirmed, later.readErr)
	}
	if err := ingress.node.ForgetLeader(); err != nil {
		t.Fatal(err)
	}
	if ingress.node.Status().LeaderID != 0 {
		t.Fatal("ingress did not forget its leader before proposal submission")
	}
	// Each encoded command exceeds half the batch budget, forcing two
	// separate submissions into the adapter's pre-append rejection FIFO.
	if !ingress.resolveCommitReads(reads[1], &proposals[1]) {
		t.Fatal("fresh-proof commands were not accepted for deferred submission")
	}
	if len(proposals[1]) != 2 || !bytes.Equal(proposals[1][0].data, timedOut.data) ||
		!bytes.Equal(proposals[1][1].data, later.data) {
		t.Fatal("deferred proposal waiters lost submission order")
	}
	if !ingress.node.StoragePending() {
		t.Fatal("adapter did not retain the leaderless proposals as pending storage")
	}

	var timeoutOutcome waitOutcome
	select {
	case timeoutOutcome = <-timedOutWait:
	case <-time.After(5 * time.Second):
		t.Fatal("original Wait did not return after its caller deadline")
	}
	deadline, ok := timedOut.context.Deadline()
	if !ok || timeoutOutcome.at.Before(deadline) || !errors.Is(timeoutOutcome.err, membership.ErrQuorumUnavailable) {
		t.Fatalf("original Wait must fail at its caller deadline: outcome=%+v deadline=%v", timeoutOutcome, deadline)
	}
	if !backlogContains(timedOut) {
		t.Fatal("caller timeout removed an accepted proposal before its late apply")
	}
	select {
	case outcome := <-laterWait:
		t.Fatalf("later Wait completed before leader recovery: %+v", outcome)
	default:
	}

	var applied [3][][]byte
	for range 64 {
		// Once all replicas apply, stop ticking and consume the remaining
		// peer responses before checking the storage checkpoint.
		if len(applied[0]) < 2 || len(applied[1]) < 2 || len(applied[2]) < 2 {
			if err := runtimes[0].node.Tick(); err != nil {
				t.Fatal(err)
			}
		}
		var messages []raftadapter.MessageDTO
		for index, runtime := range runtimes {
			// StoragePending includes the FIFO. Do not wait for it to empty
			// until an incoming heartbeat restores the ingress leader.
			if index == 1 && runtime.node.Status().LeaderID == 0 {
				continue
			}
			batch, err := processReadyForTest(t, runtime.node)
			if err != nil {
				t.Fatal(err)
			}
			runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
			applied[index] = append(applied[index], batch.Commands...)
			if index == 1 {
				for _, command := range batch.Commands {
					ingress.stageProposalResult(command, membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeApplied}, &proposals[1])
				}
				ingress.resolveAppliedProposals(&proposals[1])
			}
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if _, err := runtimes[message.To-1].node.Step(message); err != nil {
				t.Fatal(err)
			}
		}
		if len(messages) == 0 && len(applied[0]) >= 2 && len(applied[1]) >= 2 && len(applied[2]) >= 2 {
			break
		}
	}
	select {
	case outcome := <-laterWait:
		if outcome.err != nil || outcome.result.Outcome != membership.ApplyOutcomeApplied {
			t.Fatalf("later Wait did not succeed after FIFO replay: %+v", outcome)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("later Wait did not complete after FIFO replay")
	}
	if backlogContains(timedOut) || backlogContains(later) {
		t.Fatal("applied proposals retained backlog entries after caller completion")
	}
	for index, commands := range applied {
		if len(commands) != 2 || !bytes.Equal(commands[0], timedOut.data) || !bytes.Equal(commands[1], later.data) {
			t.Fatalf("node %d did not apply exactly the original two commands in FIFO order: count=%d", index+1, len(commands))
		}
	}
	select {
	case response := <-timedOut.result:
		if response.err != nil || response.result.Outcome != membership.ApplyOutcomeApplied {
			t.Fatalf("abandoned waiter did not receive its buffered late apply result: %+v", response)
		}
	default:
		t.Fatal("late apply result was not buffered after the original Wait returned an error")
	}
	if len(proposals[1]) != 0 || ingress.node.StoragePending() {
		t.Fatalf("FIFO replay left pending work: waiters=%d storage=%t", len(proposals[1]), ingress.node.StoragePending())
	}
	// Further heartbeat turns must not re-propose an already accepted command.
	for range 8 {
		if err := runtimes[0].node.Tick(); err != nil {
			t.Fatal(err)
		}
		var messages []raftadapter.MessageDTO
		for index, runtime := range runtimes {
			batch, err := processReadyForTest(t, runtime.node)
			if err != nil {
				t.Fatal(err)
			}
			if len(batch.Commands) != 0 {
				t.Fatalf("node %d applied duplicate commands after FIFO drained: count=%d", index+1, len(batch.Commands))
			}
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if _, err := runtimes[message.To-1].node.Step(message); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Log("caller Wait returned ErrQuorumUnavailable after deadline; all three nodes applied a×40000 then b×40000 once; late result stayed buffered")
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.5, §11.28.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-200
func TestClusterAppliedDeliveryStopsBackpressureBeforeCallerWait(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	ingress := runtimes[0]
	ingress.storage = newClusterStorageMonitor(time.Now, nil)
	ingress.machine = retainedTestMachine(t, ingress.config, ingress.config.ReleaseVersion)
	ingress.publishPressure = &clusterPublishPressureController{runtime: ingress, maxCommitLag: time.Second}
	ingress.observeCommitLeadership()
	pending, err := ingress.BeginDelivery(context.Background(), membership.DeliveryChangeDTO{
		TenantID: "tenant-a", ClientID: "publisher-a", Action: membership.DeliveryActionPublish,
		ProposedAtUnixMillis: 1_700_000_000_001, Topic: "events/committed", Payload: []byte("committed"), PublishQoS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	commit := pending.(*clusterPendingCommit)
	defer commit.deliveryCancel()
	// Control backlog age without sleeping or depending on clock resolution.
	backlog := &ingress.publishPressure.backlog
	backlog.mu.Lock()
	startedAt := time.Now().Add(-2 * time.Second)
	commit.backlogEntry.Value.(*clusterBacklogEntry).startedAt = startedAt
	backlog.reachableSince = startedAt
	backlog.mu.Unlock()
	if err := ingress.publishPressure.CheckPressure(); err == nil {
		t.Fatal("uncommitted delivery did not apply backpressure")
	}
	var reads [3]map[string]clusterReadWaiter
	var proposals [3][]clusterProposalWaiter
	for index := range reads {
		reads[index] = make(map[string]clusterReadWaiter)
	}
	applied := false
	for round := 0; round < 64 && !applied; round++ {
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
			if index == 0 {
				for commandIndex, data := range batch.Commands {
					command, err := decodeMembershipCommand(data)
					if err != nil {
						t.Fatal(err)
					}
					result, err := ingress.machine.Apply(batch.CommandIndexes[commandIndex], command)
					if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
						t.Fatalf("actual committed delivery failed to apply: result=%+v err=%v", result, err)
					}
					ingress.stageProposalResult(data, result, &proposals[index])
					applied = bytes.Equal(data, commit.data)
				}
				ingress.resolveAppliedProposals(&proposals[index])
				ingress.resolveCommitReads(reads[index], &proposals[index])
			}
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if _, err := runtimes[message.To-1].node.Step(message); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !applied {
		t.Fatal("delivery did not commit through the real three-node Raft cluster")
	}
	// A batch consumer can still be waiting for an earlier proposal. This
	// completed command must not reject unrelated PUBLISHes during that wait.
	if err := ingress.publishPressure.CheckPressure(); err != nil {
		t.Fatalf("committed delivery still rejects PUBLISH before caller Wait: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unapplied, err := ingress.BeginDelivery(ctx, membership.DeliveryChangeDTO{
		TenantID: "tenant-a", ClientID: "publisher-a", Action: membership.DeliveryActionPublish,
		ProposedAtUnixMillis: 1_700_000_000_002, Topic: "events/uncommitted", Payload: []byte("uncommitted"), PublishQoS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	backlog.mu.Lock()
	unapplied.(*clusterPendingCommit).backlogEntry.Value.(*clusterBacklogEntry).startedAt = startedAt
	backlog.mu.Unlock()
	result, err := pending.Wait()
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("delayed caller did not receive its committed result: result=%+v err=%v", result, err)
	}
	if err := ingress.publishPressure.CheckPressure(); err == nil {
		t.Fatal("consuming a completed result removed another uncommitted delivery")
	}
	cancel()
	if err := ingress.stageQueuedDriverWork(reads[0], &proposals[0], nil); err != nil {
		t.Fatal(err)
	}
	ingress.resolveCommitReads(reads[0], &proposals[0])
	if err := ingress.publishPressure.CheckPressure(); err != nil {
		t.Fatalf("driver did not retire cancelled pre-submission delivery before caller Wait: %v", err)
	}
	if _, err := unapplied.Wait(); !errors.Is(err, membership.ErrQuorumUnavailable) {
		t.Fatalf("cancelled unapplied delivery returned %v", err)
	}
	if err := ingress.publishPressure.CheckPressure(); err != nil {
		t.Fatalf("cancelled caller left backpressure active: %v", err)
	}
	t.Log("real quorum commit removed backpressure before the batch consumer called Wait")
}

// Traceability: CLU-07/CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestReadAdmissionCapacityGuardsAndPeerBudget(t *testing.T) {
	var missing *clusterRuntime
	if messages, bytes := missing.forwardedProposalCapacity(2); messages != 0 || bytes != 0 {
		t.Fatalf("nil runtime forwarded capacity = (%d, %d), want (0, 0)", messages, bytes)
	}
	runtime := &clusterRuntime{}
	if got := runtime.readIndexCapacity(0); got != clusterDriverStageLimit {
		t.Fatalf("readIndexCapacity without transport = %d, want %d", got, clusterDriverStageLimit)
	}
	if got := runtime.readIndexCapacityForStatus(raftadapter.StatusDTO{}, 0); got != clusterDriverStageLimit {
		t.Fatalf("readIndexCapacityForStatus without transport = %d, want %d", got, clusterDriverStageLimit)
	}
	if messages, bytes := runtime.forwardedProposalCapacity(2); messages != 0 || bytes != 0 {
		t.Fatalf("runtime without transport forwarded capacity = (%d, %d), want (0, 0)", messages, bytes)
	}

	runtime = newReadAdmissionTestCluster(t)[0]
	follower := raftadapter.StatusDTO{
		NodeID: 1, LeaderID: 2, Role: raftadapter.RoleFollower,
		VoterIDs: []raftadapter.NodeID{1, 2, 3},
	}
	if got := runtime.readIndexCapacityForStatus(follower, 0); got != 5 {
		t.Fatalf("follower read capacity = %d, want 5", got)
	}
	follower.LeaderID = 0
	if got := runtime.readIndexCapacityForStatus(follower, 0); got != 0 {
		t.Fatalf("leaderless follower read capacity = %d, want 0", got)
	}
	follower.LeaderID = 99
	if got := runtime.readIndexCapacityForStatus(follower, 0); got != 0 {
		t.Fatalf("unknown leader read capacity = %d, want 0", got)
	}

	leader := raftadapter.StatusDTO{
		NodeID: 1, LeaderID: 1, Role: raftadapter.RoleLeader,
		VoterIDs: []raftadapter.NodeID{1, 2, 3},
	}
	if got := runtime.readIndexCapacityForStatus(leader, 0); got != 5 {
		t.Fatalf("three-voter leader read capacity = %d, want 5", got)
	}
	leader.VoterIDs = []raftadapter.NodeID{1}
	if got := runtime.readIndexCapacityForStatus(leader, 0); got != clusterReadIndexLimit {
		t.Fatalf("single-voter leader read capacity = %d, want %d", got, clusterReadIndexLimit)
	}
	leader.VoterIDs = []raftadapter.NodeID{1, 2, 3}
	if got := runtime.readIndexCapacityForStatus(leader, 5); got != 0 {
		t.Fatalf("fully staged leader read capacity = %d, want 0", got)
	}

	controlPath := filepath.Join(t.TempDir(), "fault-control.json")
	if err := os.WriteFile(controlPath, []byte(`{"isolatePeers":[2]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime.faults = &clusterFaultControl{path: controlPath, logger: runtime.logger}
	follower.LeaderID = 2
	if got := runtime.readIndexCapacityForStatus(follower, 0); got != 0 {
		t.Fatalf("isolated leader read capacity = %d, want 0", got)
	}
	if messages, bytes := runtime.forwardedProposalCapacity(2); messages != 0 || bytes != 0 {
		t.Fatalf("isolated peer forwarded capacity = (%d, %d), want (0, 0)", messages, bytes)
	}
}

// Traceability: CLU-07/CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestReadAdmissionDeferredAndOversizedProofHelpers(t *testing.T) {
	runtime := &clusterRuntime{}
	runtime.hasDeferredRequest = true
	if done := runtime.deferredRequestDone(); done != nil {
		t.Fatal("deferred request without a context exposed a done channel")
	}
	key := string(make([]byte, 128-clusterReadProofHeader+1))
	if err := runtime.issueReadProof(key, new(clusterReadWaiter), raftadapter.StatusDTO{}); !errors.Is(err, raftadapter.ErrInvalidMessage) {
		t.Fatalf("issueReadProof oversized key error = %v, want %v", err, raftadapter.ErrInvalidMessage)
	}
}

// Traceability: CLU-07/CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestRefreshReadWaitersCompletesInvalidProofWaiters(t *testing.T) {
	runtime := newReadAdmissionTestCluster(t)[0]
	if runtime.node.Status().LeaderID == 0 {
		t.Fatal("test Raft node has no leader")
	}
	// A nil transport provides a positive staging allowance without peer I/O.
	runtime.transport = nil
	key := string(make([]byte, 128-clusterReadProofHeader+1))
	pending := &clusterPendingCommit{}
	response := make(chan clusterReadResponse, 1)
	for _, testCase := range []struct {
		name    string
		waiter  clusterReadWaiter
		wantKey bool
	}{
		{name: "pending commit", waiter: clusterReadWaiter{commit: pending}},
		{name: "caller response", waiter: clusterReadWaiter{ctx: context.Background(), result: response}},
		{name: "recovery proof", waiter: clusterReadWaiter{}, wantKey: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			reads := map[string]clusterReadWaiter{key: testCase.waiter}
			if testCase.wantKey {
				runtime.recoveryProofKey = key
			}
			runtime.refreshReadWaiters(reads)
			if len(reads) != 0 {
				t.Fatalf("invalid proof waiter remains queued: %d", len(reads))
			}
			if testCase.waiter.commit != nil {
				if !pending.readConfirmed || !errors.Is(pending.readErr, raftadapter.ErrInvalidMessage) {
					t.Fatalf("pending commit read result = confirmed %v err %v", pending.readConfirmed, pending.readErr)
				}
				return
			}
			if testCase.waiter.result != nil {
				select {
				case got := <-response:
					if !errors.Is(got.err, raftadapter.ErrInvalidMessage) {
						t.Fatalf("caller proof error = %v, want %v", got.err, raftadapter.ErrInvalidMessage)
					}
				default:
					t.Fatal("invalid proof did not complete the caller response")
				}
				return
			}
			if runtime.recoveryProofKey != "" {
				t.Fatalf("failed recovery proof key = %q, want cleared", runtime.recoveryProofKey)
			}
		})
	}
}

// Traceability: CLU-07/CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestRefreshReadWaitersKeepsRequestWhenPeerCapacityIsExhausted(t *testing.T) {
	runtime := newReadAdmissionTestCluster(t)[0]
	if runtime.node.Status().LeaderID == 0 {
		t.Fatal("test Raft node has no leader")
	}
	runtime.readIndexStaged = clusterDriverStageLimit
	response := make(chan clusterReadResponse, 1)
	reads := map[string]clusterReadWaiter{
		"deferred": {ctx: context.Background(), result: response},
	}

	runtime.refreshReadWaiters(reads)
	waiter, remains := reads["deferred"]
	if !remains || waiter.proofSequence != 0 {
		t.Fatalf("capacity exhaustion removed or issued the deferred read: remains=%v waiter=%+v", remains, waiter)
	}
	if len(response) != 0 {
		t.Fatal("capacity exhaustion completed the read without a quorum proof")
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §3.5.
func TestIssueReadProofReturnsClosedRaftNodeError(t *testing.T) {
	runtime := newReadAdmissionTestCluster(t)[0]
	status := runtime.node.Status()
	if status.LeaderID == 0 {
		t.Fatal("test Raft node has no leader")
	}
	if err := runtime.node.Close(); err != nil {
		t.Fatalf("close test Raft node: %v", err)
	}

	waiter := clusterReadWaiter{}
	err := runtime.issueReadProof("valid-context", &waiter, status)
	if !errors.Is(err, raftadapter.ErrClosed) {
		t.Fatalf("issueReadProof() error = %v, want %v", err, raftadapter.ErrClosed)
	}
	if waiter.proofSequence != 0 {
		t.Fatalf("failed proof recorded sequence %d", waiter.proofSequence)
	}
}

// readProofContext builds the wire context that issueReadProof gives to Raft.
func readProofContext(nodeID, nonce uint64, key string) []byte {
	wire := binary.BigEndian.AppendUint64(nil, nodeID)
	wire = binary.BigEndian.AppendUint64(wire, nonce)
	return append(wire, key...)
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.38.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-265
func TestClusterLostProofResponseIsReissuedAfterRetryInterval(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	follower := runtimes[2]
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	baseline := follower.node.Status().AppliedIndex
	const key = "lost-proof-response"
	var reads [3]map[string]clusterReadWaiter
	var proposals [3][]clusterProposalWaiter
	for index := range reads {
		reads[index] = make(map[string]clusterReadWaiter)
	}
	result := make(chan clusterReadResponse, 1)
	follower.requests <- clusterDriverRequest{kind: clusterDriverRead, contextKey: key, context: ctx, readResult: result}

	lost := 0
	// settle delivers Raft messages until every node is idle. A dropped proof response models
	// the leader's full peer FIFO rejecting it; the sender does not retransmit it.
	settle := func(dropResponses bool) {
		t.Helper()
		for range 64 {
			var messages []raftadapter.MessageDTO
			for index, runtime := range runtimes {
				if err := runtime.stageQueuedDriverWork(reads[index], &proposals[index], ctx.Done()); err != nil {
					t.Fatal(err)
				}
				batch, err := processReadyForTest(t, runtime.node)
				if err != nil {
					t.Fatal(err)
				}
				runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
				runtime.completeReadStates(batch.ReadStates, reads[index])
				runtime.readIndexStaged = 0
				messages = append(messages, batch.Messages...)
			}
			if len(messages) == 0 {
				return
			}
			for _, message := range messages {
				if dropResponses && message.Type == "MsgReadIndexResp" {
					lost++
					continue
				}
				if _, err := runtimes[message.To-1].node.Step(message); err != nil {
					t.Fatal(err)
				}
			}
		}
		t.Fatal("Raft nodes did not settle")
	}

	settle(true)
	waiter, pending := reads[2][key]
	if lost != 1 || !pending || waiter.proofSequence == 0 || len(result) != 0 {
		t.Fatalf("lost responses=%d pending=%v waiter=%+v results=%d, want one lost response and an unanswered proof",
			lost, pending, waiter, len(result))
	}
	first := waiter.proofSequence

	follower.refreshReadWaiters(reads[2])
	if got := reads[2][key].proofSequence; got != first {
		t.Fatalf("proof was re-issued before the retry interval: nonce %d, want %d", got, first)
	}

	waiter.issuedAt = time.Now().Add(-clusterReadProofRetryInterval)
	reads[2][key] = waiter
	follower.refreshReadWaiters(reads[2])
	reissued := reads[2][key].proofSequence
	if reissued == 0 || reissued == first {
		t.Fatalf("proof nonce after the retry interval = %d, want a new nonce after %d", reissued, first)
	}

	// The lost emission's ReadState must not complete or replace the new proof.
	staleState := raftadapter.ReadStateDTO{RequestContext: readProofContext(follower.config.NodeID, first, key), Index: baseline}
	remaining := follower.completeReadStates([]raftadapter.ReadStateDTO{staleState}, reads[2])
	if len(remaining) != 0 || len(result) != 0 || reads[2][key].proofSequence != reissued {
		t.Fatalf("stale ReadState completed or replaced the re-issued proof: remaining=%d results=%d waiter=%+v",
			len(remaining), len(result), reads[2][key])
	}

	settle(false)
	select {
	case response := <-result:
		if response.err != nil || response.index < baseline {
			t.Fatalf("re-issued proof result=%+v baseline=%d", response, baseline)
		}
	default:
		t.Fatal("re-issued proof did not complete the read")
	}
	if len(reads[2]) != 0 {
		t.Fatalf("completed read remains queued: %d", len(reads[2]))
	}
}

// Traceability: CLU-07/CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.38.
func TestRefreshReadWaitersReissuesOnlyUnansweredExpiredProofs(t *testing.T) {
	runtime := newReadAdmissionTestCluster(t)[0]
	status := runtime.node.Status()
	if status.LeaderID == 0 {
		t.Fatal("test Raft node has no leader")
	}
	expired := time.Now().Add(-clusterReadProofRetryInterval)
	ended, endContext := context.WithCancel(context.Background())
	endContext()
	for _, testCase := range []struct {
		name        string
		ctx         context.Context
		issuedAt    time.Time
		answered    bool
		staged      int
		wantReissue bool
	}{
		{name: "fresh proof", issuedAt: time.Now()},
		{name: "unanswered expired proof", issuedAt: expired, wantReissue: true},
		{name: "answered expired proof", issuedAt: expired, answered: true},
		{name: "expired proof with ended context", ctx: ended, issuedAt: expired},
		{name: "expired proof without outbound capacity", issuedAt: expired, staged: clusterDriverStageLimit},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			readContext := testCase.ctx
			if readContext == nil {
				readContext = context.Background()
			}
			waiter := clusterReadWaiter{ctx: readContext, result: make(chan clusterReadResponse, 1)}
			if err := runtime.issueReadProof("proof", &waiter, status); err != nil {
				t.Fatal(err)
			}
			first := waiter.proofSequence
			waiter.issuedAt, waiter.answered = testCase.issuedAt, testCase.answered
			reads := map[string]clusterReadWaiter{"proof": waiter}
			runtime.readIndexStaged = testCase.staged
			defer func() { runtime.readIndexStaged = 0 }()

			runtime.refreshReadWaiters(reads)
			got, remains := reads["proof"]
			if !remains {
				t.Fatal("refresh removed the read")
			}
			if reissued := got.proofSequence != first; reissued != testCase.wantReissue || got.proofSequence == 0 {
				t.Fatalf("proof nonce = %d, first = %d, want re-issue %v", got.proofSequence, first, testCase.wantReissue)
			}
			if testCase.wantReissue && !got.issuedAt.After(expired) {
				t.Fatalf("re-issued proof kept the old emission time %v", got.issuedAt)
			}
		})
	}
}

// Traceability: CLU-07/CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.38.
func TestCompleteReadStatesMarksProofWaitingForApplyAsAnswered(t *testing.T) {
	runtime := newReadAdmissionTestCluster(t)[0]
	status := runtime.node.Status()
	waiter := clusterReadWaiter{ctx: context.Background(), result: make(chan clusterReadResponse, 1)}
	if err := runtime.issueReadProof("answered", &waiter, status); err != nil {
		t.Fatal(err)
	}
	reads := map[string]clusterReadWaiter{"answered": waiter}
	runtime.projectionIndex.Store(status.AppliedIndex)
	ahead := raftadapter.ReadStateDTO{
		RequestContext: readProofContext(runtime.config.NodeID, waiter.proofSequence, "answered"),
		Index:          status.AppliedIndex + 1,
	}

	remaining := runtime.completeReadStates([]raftadapter.ReadStateDTO{ahead}, reads)
	if len(remaining) != 1 || !reads["answered"].answered || len(waiter.result) != 0 {
		t.Fatalf("ReadState ahead of apply: remaining=%d waiter=%+v results=%d, want it kept and marked answered",
			len(remaining), reads["answered"], len(waiter.result))
	}
}
