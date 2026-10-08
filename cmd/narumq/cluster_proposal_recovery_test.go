package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/adapters/raft/records"
	"narumq/internal/application/membership"
)

// Traceability: CLU-12; Cluster §4.2; ADR-0036, ADR-0037.
func TestClusterProposalRecoveryRequiresNewQuorumProofAndAppliesOnce(t *testing.T) {
	runtimes := newProposalRecoveryTestCluster(t)
	ingress := runtimes[1]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := membership.CommandDTO{SchemaMajor: 1, OperationID: "00000000-0000-4000-8000-000000000003", Delivery: &membership.DeliveryChangeDTO{TenantID: "acme", ClientID: "pub", Action: membership.DeliveryActionPublish, Topic: "events/recovered", Payload: []byte("one"), PublishQoS: 1, ProposedAtUnixMillis: 100, Targets: []membership.DeliveryTargetDTO{{ClientID: "sub1"}}}, Proposal: &membership.ProposalIdentityDTO{OriginNodeID: 2, ProofIndex: 4, RequestID: "00000000-0000-4000-8000-000000000004"}}
	data, err := encodeMembershipCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.node.ProposeForwarded(data); err != nil {
		t.Fatal(err)
	}
	status := ingress.node.Status()
	originalLeadership := raftLeadership{leader: status.LeaderID, term: status.Term, role: status.Role}
	waiters := []clusterProposalWaiter{{context: ctx, command: command, data: data, requestID: command.Proposal.RequestID, proofIndex: 4, leadership: originalLeadership, result: make(chan clusterApplyResponse, 1)}}
	resultC := waiters[0].result
	reads := make(map[string]clusterReadWaiter)
	// The old leader receives and persists the proposal without delivering its
	original := processRecoveryBatch(t, ingress, &waiters, reads)
	var forwarded bool
	for _, message := range original.Messages {
		if message.Type == "MsgProp" {
			forwarded = true
			if _, err := runtimes[0].node.Step(message); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !forwarded {
		t.Fatal("original proposal did not reach old leader")
	}
	old := processRecoveryBatch(t, runtimes[0], nil, nil)
	if len(old.Commands) != 0 {
		t.Fatal("old leader applied proposal without quorum")
	}
	if proposed, err := ingress.recoverProposals(ctx, reads, &waiters); err != nil || proposed {
		t.Fatalf("replayed without a leadership change: %t %v", proposed, err)
	}
	// Remove old leader from the message pump and elect node 3.
	if err := runtimes[2].node.Campaign(); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 128 && len(resultC) == 0; round++ {
		for _, index := range []int{1, 2} {
			if err := runtimes[index].node.Tick(); err != nil {
				t.Fatal(err)
			}
		}
		var messages []raftadapter.MessageDTO
		for _, index := range []int{1, 2} {
			runtime := runtimes[index]
			var nodeWaiters *[]clusterProposalWaiter
			var nodeReads map[string]clusterReadWaiter
			if index == 1 {
				nodeWaiters = &waiters
				nodeReads = reads
			}
			batch := processRecoveryBatch(t, runtime, nodeWaiters, nodeReads)
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if message.To != 1 {
				if _, err := runtimes[message.To-1].node.Step(message); err != nil {
					t.Fatal(err)
				}
			}
		}
		if _, err := ingress.recoverProposals(ctx, reads, &waiters); err != nil {
			t.Fatal(err)
		}
	}
	if len(resultC) == 0 {
		t.Fatalf("recovery did not commit: ingress=%+v peer=%+v reads=%+v proofWait=%v proofReady=%v", ingress.node.Status(), runtimes[2].node.Status(), reads, ingress.recoveryProofWait, ingress.recoveryProofReady)
	}
	result := (<-resultC).result
	if result.Outcome != membership.ApplyOutcomeApplied || result.Delivery == nil || len(result.Delivery.Enqueued) != 1 || result.Delivery.Enqueued[0].Sequence != 1 {
		t.Fatalf("recovered delivery: %+v", result)
	}
	// Deliver the immutable copy once more: state must not enqueue sequence 2.
	if err := ingress.node.ProposeForwarded(data); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 32; round++ {
		var messages []raftadapter.MessageDTO
		for _, index := range []int{1, 2} {
			batch := processRecoveryBatch(t, runtimes[index], nil, nil)
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if message.To != 1 {
				if _, err := runtimes[message.To-1].node.Step(message); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	snapshot, err := ingress.machine.Snapshot(ingress.machine.LastAppliedIndex())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.DeliverySessions) != 1 || len(snapshot.DeliverySessions[0].Messages) != 1 || snapshot.DeliverySessions[0].NextSequence != 2 || !bytes.Equal(snapshot.DeliverySessions[0].Messages[0].Payload, []byte("one")) {
		t.Fatalf("duplicate mutated delivery state: %+v", snapshot.DeliverySessions)
	}
	if ctx.Err() != nil {
		t.Fatal("recovery extended the original deadline")
	}
	assertProposalReceiptSurvivesBackupRestore(t, ingress, snapshot, data, result.Index)
	t.Log("old leader persisted without quorum; new leader fresh proof preceded replay; one sequence remained after delayed duplicate")
}

func processRecoveryBatch(t *testing.T, runtime *clusterRuntime, waiters *[]clusterProposalWaiter, reads map[string]clusterReadWaiter) raftadapter.BatchDTO {
	t.Helper()
	batch, err := processReadyForTest(t, runtime.node)
	if err != nil {
		t.Fatal(err)
	}
	for i, data := range batch.Commands {
		command, err := decodeMembershipCommand(data)
		if err != nil {
			t.Fatal(err)
		}
		result, err := runtime.machine.Apply(batch.CommandIndexes[i], command)
		if err != nil {
			t.Fatal(err)
		}
		if waiters != nil {
			runtime.stageProposalResult(data, result, waiters)
		}
	}
	if runtime.node.Status().AppliedIndex > runtime.machine.LastAppliedIndex() {
		if err := runtime.machine.AdvanceAppliedIndex(runtime.node.Status().AppliedIndex); err != nil {
			t.Fatal(err)
		}
	}
	runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
	if reads != nil {
		runtime.completeReadStates(batch.ReadStates, reads)
	}
	return batch
}

func newProposalRecoveryTestCluster(t *testing.T) [3]*clusterRuntime {
	runtimes := newReadAdmissionTestCluster(t)
	members := make([]membership.MemberIdentityDTO, 3)
	for i := range members {
		config := runtimes[i].config
		_, _, fingerprint, err := buildClusterPeerTLS(config)
		if err != nil {
			t.Fatal(err)
		}
		members[i] = membership.MemberIdentityDTO{MemberID: uint64(i + 1), ClusterID: config.ClusterID, Address: config.AdvertisedAddress, DNSSAN: config.LocalDNSName, CertificateSHA256: fingerprint[:], ReleaseVersion: config.ReleaseVersion, ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter, Features: []string{membership.ProposalRecoveryFeature}}
	}
	for _, runtime := range runtimes {
		machine := membership.NewStateMachine()
		if err := machine.InitializeBootstrap(1, members); err != nil {
			t.Fatal(err)
		}
		if result, err := machine.Apply(2, membership.CommandDTO{SchemaMajor: 1, OperationID: "00000000-0000-4000-8000-000000000001", FeatureActivation: &membership.FeatureActivationDTO{Feature: membership.ProposalRecoveryFeature}}); err != nil || result.Outcome != membership.ApplyOutcomeApplied {
			t.Fatalf("activation: %+v %v", result, err)
		}
		if result, err := machine.Apply(3, membership.CommandDTO{SchemaMajor: 1, OperationID: "00000000-0000-4000-8000-000000000002", Ownership: &membership.OwnershipChangeDTO{TenantID: "acme", ClientID: "sub1", OwnerNodeID: 2, Epoch: 1, Action: membership.OwnershipActionAcquire}}); err != nil || result.Outcome != membership.ApplyOutcomeApplied {
			t.Fatalf("ownership: %+v %v", result, err)
		}
		runtime.machine = machine
		runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
		if result, err := machine.Apply(4, membership.CommandDTO{SchemaMajor: 1, OperationID: "00000000-0000-4000-8000-000000000005", Subscription: &membership.SubscriptionChangeDTO{TenantID: "acme", ClientID: "sub1", OwnerNodeID: 2, Epoch: 1, Filter: "events/#", QoS: 1, Action: membership.SubscriptionActionAdd}}); err != nil || result.Outcome != membership.ApplyOutcomeApplied {
			t.Fatalf("subscription: %+v %v", result, err)
		}
	}
	return runtimes
}

// Traceability: CLU-12; Cluster §4.2; ADR-0036 original caller deadline.
func TestClusterProposalRecoveryNoQuorumNoSuccessAndNoLateAck(t *testing.T) {
	runtimes := newProposalRecoveryTestCluster(t)
	ingress := runtimes[1]
	old := ingress.node.Status()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := membership.CommandDTO{SchemaMajor: 1, OperationID: "00000000-0000-4000-8000-000000000101", Delivery: &membership.DeliveryChangeDTO{TenantID: "acme", ClientID: "pub", Action: membership.DeliveryActionPublish, Topic: "events/no-quorum", Payload: []byte("one"), PublishQoS: 1, ProposedAtUnixMillis: 100, Targets: []membership.DeliveryTargetDTO{{ClientID: "sub1"}}}, Proposal: &membership.ProposalIdentityDTO{OriginNodeID: 2, ProofIndex: 4, RequestID: "00000000-0000-4000-8000-000000000102"}}
	data, err := encodeMembershipCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := ingress.node.ProposeForwarded(data); err != nil {
		t.Fatal(err)
	}
	resultC := make(chan clusterApplyResponse, 1)
	waiters := []clusterProposalWaiter{{context: ctx, command: command, data: data, requestID: command.Proposal.RequestID, proofIndex: 4, leadership: raftLeadership{leader: old.LeaderID, term: old.Term, role: old.Role}, result: resultC}}
	reads := make(map[string]clusterReadWaiter)
	batch := processRecoveryBatch(t, ingress, &waiters, reads)
	for _, message := range batch.Messages {
		if message.To == 1 {
			if _, err := runtimes[0].node.Step(message); err != nil {
				t.Fatal(err)
			}
		}
	}
	processRecoveryBatch(t, runtimes[0], nil, nil)
	if err := runtimes[2].node.Campaign(); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 128 && (ingress.node.Status().LeaderID == 0 || ingress.node.Status().LeaderID == 1); round++ {
		var messages []raftadapter.MessageDTO
		for _, index := range []int{1, 2} {
			if err := runtimes[index].node.Tick(); err != nil {
				t.Fatal(err)
			}
			batch := processRecoveryBatch(t, runtimes[index], nil, nil)
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if message.To != 1 {
				if _, err := runtimes[message.To-1].node.Step(message); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if leader := ingress.node.Status().LeaderID; leader == 0 || leader == 1 {
		t.Fatalf("replacement leader was not elected: ingress=%+v peer=%+v", ingress.node.Status(), runtimes[2].node.Status())
	}
	// Stop all delivery between the two survivors before requesting a new proof.
	for range 8 {
		if _, err := ingress.recoverProposals(context.Background(), reads, &waiters); err != nil {
			t.Fatal(err)
		}
		batch := processRecoveryBatch(t, ingress, &waiters, reads)
		if len(batch.Commands) != 0 || len(resultC) != 0 {
			t.Fatal("recovery succeeded without a new quorum proof")
		}
	}
	if len(reads) != 1 || ingress.recoveryProofReady {
		t.Fatal("fresh proof was not awaiting a quorum")
	}
	cancel()
	if _, err := ingress.recoverProposals(context.Background(), reads, &waiters); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-resultC:
		if response.err == nil || response.result.Outcome == membership.ApplyOutcomeApplied {
			t.Fatalf("expired caller received success: %+v", response)
		}
	default:
		t.Fatal("expired caller was not resolved")
	}
	// A delayed original frame may still commit. It cannot produce another ACK.
	if err := ingress.node.ProposeForwarded(data); err != nil {
		t.Fatal(err)
	}
	for range 32 {
		var messages []raftadapter.MessageDTO
		for _, index := range []int{1, 2} {
			if err := runtimes[index].node.Tick(); err != nil {
				t.Fatal(err)
			}
			batch := processRecoveryBatch(t, runtimes[index], nil, nil)
			messages = append(messages, batch.Messages...)
		}
		for _, message := range messages {
			if message.To != 1 {
				if _, err := runtimes[message.To-1].node.Step(message); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	snapshot, err := ingress.machine.Snapshot(ingress.machine.LastAppliedIndex())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.DeliverySessions) != 1 || len(snapshot.DeliverySessions[0].Messages) != 1 || snapshot.DeliverySessions[0].NextSequence != 2 {
		t.Fatalf("delayed frame did not apply once: %+v", snapshot.DeliverySessions)
	}
	if len(resultC) != 0 {
		t.Fatal("delayed durable apply produced a late ACK")
	}
}

func assertProposalReceiptSurvivesBackupRestore(t *testing.T, runtime *clusterRuntime, snapshot membership.SnapshotDTO, original []byte, firstIndex uint64) {
	t.Helper()
	encoded, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.node.SnapshotWithApplicationData(encoded); err != nil {
		t.Fatal(err)
	}
	image, err := runtime.node.BackupImage()
	if err != nil {
		t.Fatal(err)
	}
	backup, err := buildClusterBackupArchive(runtime.config, image, encoded)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := records.DecodeBackupArchive(backup.Archive)
	if err != nil {
		t.Fatal(err)
	}
	config := runtime.config
	config.Role = "bootstrap"
	config.BootstrapMembers = make([]clusterBootstrapMember, len(snapshot.Members))
	for i, member := range snapshot.Members {
		config.BootstrapMembers[i] = clusterBootstrapMember{MemberID: member.MemberID, Address: member.Address, DNSSAN: member.DNSSAN, CertificateSHA256: hex.EncodeToString(member.CertificateSHA256)}
	}
	if _, err := clusterRestoreSeed(config, archive); err == nil {
		t.Fatal("restore build without proposal-recovery accepted active receipt state")
	}
	config.Features = []string{membership.ProposalRecoveryFeature}
	seed, err := clusterRestoreSeed(config, archive)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if err := raftadapter.BeginRestore(directory); err != nil {
		t.Fatal(err)
	}
	if err := raftadapter.CompleteRestore(directory, seed); err != nil {
		t.Fatal(err)
	}
	node, err := raftadapter.Open(raftadapter.Config{NodeID: raftadapter.NodeID(config.NodeID), DynamicMembership: true, ClusterID: config.ClusterID, CertificateSHA256: [32]byte(snapshot.Members[config.NodeID-1].CertificateSHA256), BootstrapVoterIDs: []raftadapter.NodeID{1, 2, 3}, Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	restoredSnapshot, err := decodeMembershipSnapshot(node.ApplicationSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	restored := membership.NewStateMachine()
	if err := restored.Restore(restoredSnapshot); err != nil {
		t.Fatal(err)
	}
	command, err := decodeMembershipCommand(original)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := restored.Apply(restored.LastAppliedIndex()+1, command)
	if err != nil || !replay.Replayed || replay.Index != firstIndex {
		t.Fatalf("disk restored receipt changed first result: %+v %v", replay, err)
	}
	state, found := restored.DeliverySession("acme", "sub1")
	if !found || len(state.Messages) != 1 || state.NextSequence != 2 {
		t.Fatalf("restored replay enqueued again: %+v", state)
	}
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.2, §11.33.
func TestRecoverProposalsHandlesMissingMachineAndEmptyLedger(t *testing.T) {
	var proposals []clusterProposalWaiter
	if replayed, err := (&clusterRuntime{}).recoverProposals(context.Background(), nil, &proposals); err != nil || replayed {
		t.Fatalf("recoverProposals without a machine = %t, %v; want false, nil", replayed, err)
	}

	for _, staleProjection := range []bool{false, true} {
		machine := membership.NewStateMachine()
		runtime := &clusterRuntime{
			machine: machine, recoveryProofKey: "stale-proof",
			recoveryProofWait: true, recoveryProofReady: true,
		}
		if staleProjection {
			if err := machine.AdvanceAppliedIndex(1); err != nil {
				t.Fatal(err)
			}
		}
		reads := map[string]clusterReadWaiter{"stale-proof": {}}
		if replayed, err := runtime.recoverProposals(context.Background(), reads, &proposals); err != nil || replayed {
			t.Fatalf("recoverProposals with empty ledger (staleProjection=%t) = %t, %v; want false, nil",
				staleProjection, replayed, err)
		}
		if len(reads) != 0 || runtime.recoveryProofKey != "" || runtime.recoveryProofWait || runtime.recoveryProofReady {
			t.Fatalf("empty ledger retained recovery state: reads=%d key=%q wait=%v ready=%v",
				len(reads), runtime.recoveryProofKey, runtime.recoveryProofWait, runtime.recoveryProofReady)
		}
	}
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.2, §11.33.
func TestPrepareProposalValidatesAndFreezesOriginalCommandDigest(t *testing.T) {
	runtime := &clusterRuntime{config: clusterConfigFile{NodeID: 2}}
	if err := runtime.prepareProposal(&clusterPendingCommit{data: []byte{0xff}}, 0); err == nil {
		t.Fatal("prepareProposal accepted malformed command bytes")
	}

	command := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: "00000000-0000-4000-8000-000000000211",
		FeatureActivation: &membership.FeatureActivationDTO{Feature: membership.ProposalRecoveryFeature},
	}
	original, err := encodeMembershipCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	pending := &clusterPendingCommit{data: bytes.Clone(original), readProofIndex: 7}
	if err := runtime.prepareProposal(pending, 6); err != nil {
		t.Fatalf("prepareProposal() error = %v", err)
	}
	identity := pending.command.Proposal
	if identity == nil || identity.OriginNodeID != 2 || identity.ProofIndex != 7 ||
		identity.RetiredThrough != 6 || identity.RequestID == "" {
		t.Fatalf("proposal identity = %+v", identity)
	}
	wantDigest := sha256.Sum256(original)
	if !bytes.Equal(identity.CommandDigest, wantDigest[:]) {
		t.Fatalf("proposal digest = %x, want original command digest %x", identity.CommandDigest, wantDigest)
	}
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.2, §11.33.
func TestRecoverProposalsUsesReceiptAndRejectsReceiptIdentityMismatch(t *testing.T) {
	runtime := newProposalRecoveryTestCluster(t)[0]
	command := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: "00000000-0000-4000-8000-000000000221",
		Delivery: &membership.DeliveryChangeDTO{
			TenantID: "acme", ClientID: "publisher", Action: membership.DeliveryActionPublish,
			Topic: "events/recovered", Payload: []byte("one"), PublishQoS: 1, ProposedAtUnixMillis: 100,
			Targets: []membership.DeliveryTargetDTO{{ClientID: "sub1"}},
		},
	}
	data, err := encodeMembershipCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	pending := &clusterPendingCommit{
		data: data, requestID: "00000000-0000-4000-8000-000000000222", readProofIndex: 4,
	}
	if err := runtime.prepareProposal(pending, 0); err != nil {
		t.Fatal(err)
	}
	firstIndex := runtime.machine.LastAppliedIndex() + 1
	firstResult, err := runtime.machine.Apply(firstIndex, pending.command)
	if err != nil || firstResult.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("apply receipt source command: result=%+v err=%v", firstResult, err)
	}
	runtime.projectionIndex.Store(runtime.machine.LastAppliedIndex())

	tests := []struct {
		name         string
		mutate       func(*membership.ProposalIdentityDTO)
		wantMismatch bool
	}{
		{name: "matching receipt"},
		{name: "different proof index", mutate: func(identity *membership.ProposalIdentityDTO) { identity.ProofIndex++ }, wantMismatch: true},
		{name: "different command digest", mutate: func(identity *membership.ProposalIdentityDTO) {
			identity.CommandDigest = bytes.Clone(identity.CommandDigest)
			identity.CommandDigest[0] ^= 0xff
		}, wantMismatch: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			identity := *pending.command.Proposal
			identity.CommandDigest = bytes.Clone(identity.CommandDigest)
			if testCase.mutate != nil {
				testCase.mutate(&identity)
			}
			waiterCommand := pending.command
			waiterCommand.Proposal = &identity
			result := make(chan clusterApplyResponse, 1)
			waiters := []clusterProposalWaiter{{
				context: context.Background(), command: waiterCommand, data: pending.data,
				requestID: identity.RequestID, proofIndex: identity.ProofIndex, result: result,
			}}
			replayed, recoverErr := runtime.recoverProposals(context.Background(), make(map[string]clusterReadWaiter), &waiters)
			if testCase.wantMismatch {
				if recoverErr != membership.ErrInvalidCommand || replayed {
					t.Fatalf("recoverProposals() = %t, %v; want invalid receipt identity", replayed, recoverErr)
				}
				return
			}
			if recoverErr != nil || replayed || len(waiters) != 0 {
				t.Fatalf("matching receipt recovery = replayed %t err %v waiters %d", replayed, recoverErr, len(waiters))
			}
			select {
			case got := <-result:
				if got.err != nil || got.result.Index != firstResult.Index || got.result.Outcome != firstResult.Outcome {
					t.Fatalf("replayed receipt response = %+v, want original result %+v", got, firstResult)
				}
			default:
				t.Fatal("matching receipt did not resolve the original waiter")
			}
		})
	}

	status := runtime.node.Status()
	leadership := raftLeadership{leader: status.LeaderID, term: status.Term, role: status.Role}
	runtime.recoveryLeadership = leadership
	runtime.recoveryProofReady = true
	waiterCommand := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: "00000000-0000-4000-8000-000000000223",
		Proposal: &membership.ProposalIdentityDTO{
			OriginNodeID: 2, ProofIndex: 6, RequestID: "00000000-0000-4000-8000-000000000224",
			CommandDigest: make([]byte, sha256.Size),
		},
	}
	waiters := []clusterProposalWaiter{{context: context.Background(), command: waiterCommand, leadership: leadership}}
	if replayed, err := runtime.recoverProposals(context.Background(), make(map[string]clusterReadWaiter), &waiters); err != nil || replayed || runtime.recoveryProofReady {
		t.Fatalf("same-leadership waiter recovery = replayed %t err %v proofReady %v",
			replayed, err, runtime.recoveryProofReady)
	}
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.2, §11.33.
func TestRecoverProposalsFlushesCountAndByteBoundedBatches(t *testing.T) {
	for _, test := range []struct {
		name         string
		count        int
		payloadBytes int
	}{
		{name: "count limit", count: clusterDriverStageLimit + 1, payloadBytes: 1},
		{name: "byte limit", count: 2, payloadBytes: clusterProposalBatchBytes/2 + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := newProposalRecoveryTestCluster(t)[0]
			status := runtime.node.Status()
			if status.LeaderID == 0 || status.Role != raftadapter.RoleLeader {
				t.Fatalf("test node status = %+v, want elected leader", status)
			}
			leadership := raftLeadership{leader: status.LeaderID, term: status.Term, role: status.Role}
			runtime.recoveryLeadership = leadership
			runtime.recoveryProofReady = true

			proposals := make([]clusterProposalWaiter, 0, test.count)
			for index := range test.count {
				command := membership.CommandDTO{
					SchemaMajor: membership.SchemaMajor,
					OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012x", index+1),
					Delivery: &membership.DeliveryChangeDTO{
						TenantID: "acme", ClientID: "publisher", Action: membership.DeliveryActionPublish,
						Topic: "events/replay", Payload: make([]byte, test.payloadBytes),
						PublishQoS: 1, ProposedAtUnixMillis: 100,
						Targets: []membership.DeliveryTargetDTO{{ClientID: "sub1"}},
					},
				}
				data, err := encodeMembershipCommand(command)
				if err != nil {
					t.Fatalf("encode replay command %d: %v", index, err)
				}
				pending := &clusterPendingCommit{
					data: data, requestID: fmt.Sprintf("00000000-0000-4000-8000-%012x", index+test.count+1),
					readProofIndex: uint64(index + 1),
				}
				if err := runtime.prepareProposal(pending, 0); err != nil {
					t.Fatalf("prepare replay command %d: %v", index, err)
				}
				proposals = append(proposals, clusterProposalWaiter{
					context: context.Background(), command: pending.command, data: pending.data,
				})
			}

			replayed, err := runtime.recoverProposals(context.Background(), nil, &proposals)
			if err != nil || !replayed {
				t.Fatalf("recoverProposals() = (%v, %v), want replayed batch", replayed, err)
			}
			if runtime.recoveryProofReady {
				t.Fatal("successful replay kept the fresh proof ready")
			}
			for index, waiter := range proposals {
				if waiter.leadership != leadership {
					t.Errorf("replayed proposal %d leadership = %+v, want %+v", index, waiter.leadership, leadership)
				}
			}
		})
	}
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.2, §11.33.
func TestRecoverProposalsDropsStaleProofWhenCapacityIsExhausted(t *testing.T) {
	runtime := newProposalRecoveryTestCluster(t)[0]
	status := runtime.node.Status()
	previousLeadership := raftLeadership{leader: status.LeaderID + 1, term: status.Term, role: status.Role}
	runtime.recoveryLeadership = previousLeadership
	runtime.recoveryProofKey = "previous-proof"
	runtime.recoveryProofReady = true
	runtime.readIndexStaged = clusterDriverStageLimit
	reads := map[string]clusterReadWaiter{"previous-proof": {}}
	proposals := []clusterProposalWaiter{{
		context: context.Background(),
		command: membership.CommandDTO{Proposal: &membership.ProposalIdentityDTO{
			OriginNodeID: 2, RequestID: "00000000-0000-4000-8000-000000000301",
		}},
		leadership: previousLeadership,
	}}

	replayed, err := runtime.recoverProposals(context.Background(), reads, &proposals)
	if err != nil || replayed {
		t.Fatalf("recoverProposals() = (%v, %v), want no replay while capacity is exhausted", replayed, err)
	}
	if len(reads) != 0 || runtime.recoveryProofKey != "" || !runtime.recoveryProofWait || runtime.recoveryProofReady {
		t.Fatalf("capacity-blocked recovery state = reads:%d key:%q wait:%v ready:%v",
			len(reads), runtime.recoveryProofKey, runtime.recoveryProofWait, runtime.recoveryProofReady)
	}
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.2, §11.33.
func TestRecoverProposalsReturnsClosedNodeErrors(t *testing.T) {
	t.Run("new proof", func(t *testing.T) {
		runtime := newProposalRecoveryTestCluster(t)[0]
		status := runtime.node.Status()
		if status.LeaderID == 0 || status.Role != raftadapter.RoleLeader {
			t.Fatalf("test node status = %+v, want elected leader", status)
		}
		proposals := []clusterProposalWaiter{{
			context: context.Background(),
			command: membership.CommandDTO{Proposal: &membership.ProposalIdentityDTO{
				OriginNodeID: 2, RequestID: "00000000-0000-4000-8000-000000000302",
			}},
		}}
		if err := runtime.node.Close(); err != nil {
			t.Fatalf("close test Raft node: %v", err)
		}

		replayed, err := runtime.recoverProposals(context.Background(), make(map[string]clusterReadWaiter), &proposals)
		if replayed || err != raftadapter.ErrClosed {
			t.Fatalf("recoverProposals() = (%v, %v), want closed-node error before replay", replayed, err)
		}
	})

	t.Run("proposal batch", func(t *testing.T) {
		for _, count := range []int{1, clusterDriverStageLimit + 1} {
			t.Run(fmt.Sprintf("%d proposals", count), func(t *testing.T) {
				runtime := newProposalRecoveryTestCluster(t)[0]
				status := runtime.node.Status()
				if status.LeaderID == 0 || status.Role != raftadapter.RoleLeader {
					t.Fatalf("test node status = %+v, want elected leader", status)
				}
				leadership := raftLeadership{leader: status.LeaderID, term: status.Term, role: status.Role}
				runtime.recoveryLeadership = leadership
				runtime.recoveryProofReady = true
				proposals := make([]clusterProposalWaiter, count)
				for index := range proposals {
					proposals[index] = clusterProposalWaiter{
						context: context.Background(),
						command: membership.CommandDTO{Proposal: &membership.ProposalIdentityDTO{
							OriginNodeID: 2, RequestID: fmt.Sprintf("00000000-0000-4000-8000-%012x", index+0x400),
						}},
						data: []byte{byte(index + 1)},
					}
				}
				if err := runtime.node.Close(); err != nil {
					t.Fatalf("close test Raft node: %v", err)
				}

				replayed, err := runtime.recoverProposals(context.Background(), make(map[string]clusterReadWaiter), &proposals)
				if replayed || err != raftadapter.ErrClosed {
					t.Fatalf("recoverProposals(count=%d) = (%v, %v), want closed-node error", count, replayed, err)
				}
			})
		}
	})
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.2, §11.33.
func TestRefreshPendingProposalProofCompletesInvalidContextKey(t *testing.T) {
	runtime := newReadAdmissionTestCluster(t)[0]
	pending := &clusterPendingCommit{
		contextKey:  string(make([]byte, 128-clusterReadProofHeader+1)),
		readContext: context.Background(),
	}
	reads := make(map[string]clusterReadWaiter)
	runtime.refreshPendingProposalProof(pending, reads)
	if !pending.readConfirmed || pending.readErr != raftadapter.ErrInvalidMessage || len(reads) != 0 {
		t.Fatalf("pending read refresh: confirmed=%v error=%v reads=%d",
			pending.readConfirmed, pending.readErr, len(reads))
	}
}
