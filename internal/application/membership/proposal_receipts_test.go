package membership

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func testProposalCommand(origin uint64, proof, floor uint64, reqID string, payload *DeliveryChangeDTO) CommandDTO {
	cmd := CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: reqID,
		Delivery: payload,
	}
	digest := sha256.Sum256([]byte(reqID + payload.Topic))
	cmd.Proposal = &ProposalIdentityDTO{
		OriginNodeID:   origin,
		ProofIndex:     proof,
		RetiredThrough: floor,
		RequestID:      reqID,
		CommandDigest:  digest[:],
	}
	return cmd
}

func setupProposalTestMachine(t *testing.T) (*StateMachine, []MemberIdentityDTO) {
	t.Helper()
	members := []MemberIdentityDTO{
		{MemberID: 1, ClusterID: "c1", Address: "127.0.0.1:1001", DNSSAN: "n1", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: MemberPhaseVoter, Features: []string{ProposalRecoveryFeature}},
		{MemberID: 2, ClusterID: "c1", Address: "127.0.0.1:1002", DNSSAN: "n2", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: MemberPhaseVoter, Features: []string{ProposalRecoveryFeature}},
		{MemberID: 3, ClusterID: "c1", Address: "127.0.0.1:1003", DNSSAN: "n3", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: MemberPhaseVoter, Features: []string{ProposalRecoveryFeature}},
	}
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatal(err)
	}
	// Activate feature
	_, err := machine.Apply(2, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: "00000000-0000-4000-8000-000000000099",
		FeatureActivation: &FeatureActivationDTO{Feature: ProposalRecoveryFeature},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Setup session and subscription for delivery target
	applyMustSucceed(t, machine, 3, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: "00000000-0000-4000-8000-000000000001",
		Ownership: &OwnershipChangeDTO{TenantID: "acme", ClientID: "sub1", OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire},
	})
	applyMustSucceed(t, machine, 4, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: "00000000-0000-4000-8000-000000000002",
		Subscription: &SubscriptionChangeDTO{TenantID: "acme", ClientID: "sub1", OwnerNodeID: 1, Epoch: 1, Filter: "t/#", QoS: 1, Action: SubscriptionActionAdd},
	})
	return machine, members
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-233
func TestProposalReceiptLifecycleAndReplay(t *testing.T) {
	machine, _ := setupProposalTestMachine(t)
	reqID := "00000000-0000-4000-8000-000000000101"
	payload := &DeliveryChangeDTO{
		TenantID: "acme", ClientID: "pub", Action: DeliveryActionPublish,
		Topic: "t/1", Payload: []byte("msg1"), PublishQoS: 1, ProposedAtUnixMillis: 100,
		Targets: []DeliveryTargetDTO{{ClientID: "sub1"}},
	}
	cmd := testProposalCommand(1, 5, 0, reqID, payload)

	// First application: must apply and save receipt
	res1, err := machine.Apply(6, cmd)
	if err != nil {
		t.Fatalf("first apply error: %v", err)
	}
	if res1.Outcome != ApplyOutcomeApplied || res1.Replayed {
		t.Fatalf("res1 outcome = %d, replayed = %v", res1.Outcome, res1.Replayed)
	}
	if res1.Delivery == nil || len(res1.Delivery.Enqueued) != 1 {
		t.Fatalf("res1 delivery enqueued = %+v", res1.Delivery)
	}

	receipt, found := machine.ProposalReceipt(1, reqID)
	if !found {
		t.Fatal("receipt not found after first apply")
	}
	if receipt.RequestID != reqID || receipt.ProofIndex != 5 {
		t.Fatalf("receipt mismatch: %+v", receipt)
	}

	// Second application (exact replay): must return cached result without re-enqueueing
	res2, err := machine.Apply(7, cmd)
	if err != nil {
		t.Fatalf("replay apply error: %v", err)
	}
	if res2.Outcome != ApplyOutcomeApplied || !res2.Replayed {
		t.Fatalf("res2 outcome = %d, replayed = %v", res2.Outcome, res2.Replayed)
	}
	if res2.Index != res1.Index {
		t.Fatalf("res2 index = %d, want original index %d", res2.Index, res1.Index)
	}

	// Verify delivery queue in machine has sequence 1 only once
	deliveryState := machine.delivery[sessionKey{tenantID: "acme", clientID: "sub1"}]
	if len(deliveryState.Messages) != 1 || deliveryState.NextSequence != 2 {
		t.Fatalf("expected exactly 1 message in delivery queue, got %d, nextSeq=%d", len(deliveryState.Messages), deliveryState.NextSequence)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-233
func TestProposalDigestMismatchRejected(t *testing.T) {
	machine, _ := setupProposalTestMachine(t)
	reqID := "00000000-0000-4000-8000-000000000102"
	payload := &DeliveryChangeDTO{
		TenantID: "acme", ClientID: "pub", Action: DeliveryActionPublish,
		Topic: "t/1", Payload: []byte("msg1"), PublishQoS: 1, ProposedAtUnixMillis: 100,
		Targets: []DeliveryTargetDTO{{ClientID: "sub1"}},
	}
	cmd1 := testProposalCommand(1, 5, 0, reqID, payload)
	res1, err := machine.Apply(6, cmd1)
	if err != nil || res1.Outcome != ApplyOutcomeApplied {
		t.Fatalf("apply cmd1: res=%+v err=%v", res1, err)
	}

	// Same RequestID, but different payload / digest
	payload2 := &DeliveryChangeDTO{
		TenantID: "acme", ClientID: "pub", Action: DeliveryActionPublish,
		Topic: "t/tampered", Payload: []byte("msg2"), PublishQoS: 1, ProposedAtUnixMillis: 100,
		Targets: []DeliveryTargetDTO{{ClientID: "sub1"}},
	}
	cmd2 := testProposalCommand(1, 5, 0, reqID, payload2)
	res2, err := machine.Apply(7, cmd2)
	if err != nil {
		t.Fatalf("apply cmd2 error: %v", err)
	}
	if res2.Outcome != ApplyOutcomeConflict || res2.Conflict != ConflictProposalIdentity {
		t.Fatalf("res2 outcome = %d, conflict = %s, want ConflictProposalIdentity", res2.Outcome, res2.Conflict)
	}
	original, found := machine.ProposalReceipt(1, reqID)
	if !found || original.ProofIndex != 5 || !bytes.Equal(original.CommandDigest, cmd1.Proposal.CommandDigest) {
		t.Fatalf("original receipt changed after identity conflict: %+v found=%v", original, found)
	}
	if count, total, retired := machine.ProposalAdmission(1, 0); count != 1 || total != 1 || retired != 0 {
		t.Fatalf("identity conflict changed admission state to (%d,%d,%d)", count, total, retired)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-233
func TestProposalRetirementFloorFencing(t *testing.T) {
	machine, _ := setupProposalTestMachine(t)
	reqID1 := "00000000-0000-4000-8000-000000000103"
	payload := &DeliveryChangeDTO{
		TenantID: "acme", ClientID: "pub", Action: DeliveryActionPublish,
		Topic: "t/1", Payload: []byte("msg1"), PublishQoS: 1, ProposedAtUnixMillis: 100,
		Targets: []DeliveryTargetDTO{{ClientID: "sub1"}},
	}
	cmd1 := testProposalCommand(1, 5, 0, reqID1, payload)
	_, _ = machine.Apply(6, cmd1)

	// Now a second command advances the retirement floor to 5
	reqID2 := "00000000-0000-4000-8000-000000000104"
	cmd2 := testProposalCommand(1, 6, 5, reqID2, payload)
	res2, err := machine.Apply(7, cmd2)
	if err != nil || res2.Outcome != ApplyOutcomeApplied {
		t.Fatalf("apply cmd2: res=%+v err=%v", res2, err)
	}

	// Receipt for reqID1 should now be retired
	if _, found := machine.ProposalReceipt(1, reqID1); found {
		t.Fatal("receipt reqID1 should have been retired")
	}

	// A late arriving command with proofIndex <= 5 from origin 1 must be rejected as retired
	lateReqID := "00000000-0000-4000-8000-000000000105"
	lateCmd := testProposalCommand(1, 5, 0, lateReqID, payload)
	resLate, err := machine.Apply(8, lateCmd)
	if err != nil {
		t.Fatalf("apply lateCmd error: %v", err)
	}
	if resLate.Outcome != ApplyOutcomeConflict || resLate.Conflict != ConflictProposalRetired {
		t.Fatalf("resLate = %+v, want ConflictProposalRetired", resLate)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-236
func TestProposalOriginsSnapshotRoundTrip(t *testing.T) {
	machine, _ := setupProposalTestMachine(t)
	reqID := "00000000-0000-4000-8000-000000000106"
	payload := &DeliveryChangeDTO{
		TenantID: "acme", ClientID: "pub", Action: DeliveryActionPublish,
		Topic: "t/1", Payload: []byte("msg1"), PublishQoS: 1, ProposedAtUnixMillis: 100,
		Targets: []DeliveryTargetDTO{{ClientID: "sub1"}},
	}
	cmd := testProposalCommand(1, 5, 2, reqID, payload)
	res1, err := machine.Apply(6, cmd)
	if err != nil || res1.Outcome != ApplyOutcomeApplied {
		t.Fatalf("apply cmd: %+v, err=%v", res1, err)
	}

	// Take snapshot
	snapshot, err := machine.Snapshot(6)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snapshot.ProposalOrigins) != 1 {
		t.Fatalf("snapshot proposal origins count = %d, want 1", len(snapshot.ProposalOrigins))
	}
	origin := snapshot.ProposalOrigins[0]
	if origin.OriginNodeID != 1 || origin.RetiredThrough != 2 || len(origin.Receipts) != 1 {
		t.Fatalf("snapshot origin = %+v", origin)
	}

	// Restore into a fresh machine
	newMachine := NewStateMachine()
	if err := newMachine.Restore(snapshot); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Check receipt is restored
	receipt, found := newMachine.ProposalReceipt(1, reqID)
	if !found {
		t.Fatal("receipt not found after restore")
	}
	if receipt.RequestID != reqID || receipt.Result.Index != 6 {
		t.Fatalf("restored receipt mismatch: %+v", receipt)
	}

	// Replay on restored machine must be idempotent
	resReplay, err := newMachine.Apply(7, cmd)
	if err != nil || resReplay.Outcome != ApplyOutcomeApplied || !resReplay.Replayed {
		t.Fatalf("replay on restored machine: %+v, err=%v", resReplay, err)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-235
func TestProposalReceiptCapacityLimits(t *testing.T) {
	machine, _ := setupProposalTestMachine(t)
	payload := &DeliveryChangeDTO{
		TenantID: "acme", ClientID: "pub", Action: DeliveryActionPublish,
		Topic: "t/1", Payload: []byte("msg"), PublishQoS: 1, ProposedAtUnixMillis: 100,
		Targets: []DeliveryTargetDTO{{ClientID: "sub1"}},
	}

	// Fill origin 1 receipts up to MaxProposalReceiptsPerOrigin
	for i := 1; i <= MaxProposalReceiptsPerOrigin; i++ {
		reqID := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		cmd := testProposalCommand(1, 5, 0, reqID, payload)
		res, err := machine.Apply(uint64(10+i), cmd)
		if err != nil || res.Outcome != ApplyOutcomeApplied {
			t.Fatalf("apply %d: res=%+v err=%v", i, res, err)
		}
	}

	// Next command on origin 1 must be rejected with ConflictProposalCapacity
	overID := fmt.Sprintf("00000000-0000-4000-8000-%012d", MaxProposalReceiptsPerOrigin+1)
	overCmd := testProposalCommand(1, 5, 0, overID, payload)
	resOver, err := machine.Apply(uint64(10+MaxProposalReceiptsPerOrigin+1), overCmd)
	if err != nil {
		t.Fatalf("apply over error: %v", err)
	}
	if resOver.Outcome != ApplyOutcomeConflict || resOver.Conflict != ConflictProposalCapacity {
		t.Fatalf("overCmd outcome = %d, conflict = %s, want ConflictProposalCapacity", resOver.Outcome, resOver.Conflict)
	}
	// Rejected proof remains fenced after restoring a full ledger and after GC.
	snapshot, err := machine.Snapshot(resOver.Index)
	if err != nil {
		t.Fatal(err)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	rejected, err := restored.Apply(resOver.Index+1, overCmd)
	if err != nil || rejected.Conflict != ConflictProposalCapacity || rejected.Index != resOver.Index {
		t.Fatalf("restored capacity fence: %+v %v", rejected, err)
	}
	fresh := testProposalCommand(1, rejected.Index+1, 5, "00000000-0000-4000-8000-000000009999", payload)
	applyMustSucceed(t, restored, resOver.Index+2, fresh)
	late, err := restored.Apply(resOver.Index+3, overCmd)
	if err != nil || late.Outcome != ApplyOutcomeConflict {
		t.Fatalf("capacity-rejected command applied after GC: %+v %v", late, err)
	}
	if _, found := restored.ProposalReceipt(1, overID); found {
		t.Fatal("rejected command created a receipt after GC")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-234
func TestProposalConflictDoesNotBecomeSuccessAfterOwnershipAppears(t *testing.T) {
	machine, _ := setupProposalTestMachine(t)
	requestID := "00000000-0000-4000-8000-000000007001"
	digest := sha256.Sum256([]byte(requestID))
	command := CommandDTO{SchemaMajor: SchemaMajor, OperationID: requestID,
		Subscription: &SubscriptionChangeDTO{TenantID: "acme", ClientID: "later", OwnerNodeID: 1, Epoch: 1, Filter: "t/#", QoS: 1, Action: SubscriptionActionAdd},
		Proposal:     &ProposalIdentityDTO{OriginNodeID: 1, ProofIndex: 5, RequestID: requestID, CommandDigest: digest[:]}}
	first, err := machine.Apply(6, command)
	if err != nil || first.Conflict != ConflictStaleEpoch {
		t.Fatalf("initial conflict: %+v %v", first, err)
	}
	applyMustSucceed(t, machine, 7, CommandDTO{SchemaMajor: SchemaMajor, OperationID: "00000000-0000-4000-8000-000000007002", Ownership: &OwnershipChangeDTO{TenantID: "acme", ClientID: "later", OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire}})
	replay, err := machine.Apply(8, command)
	if err != nil || !replay.Replayed || replay.Index != first.Index || replay.Conflict != first.Conflict {
		t.Fatalf("conflict changed on replay: %+v %v", replay, err)
	}
	for _, subscription := range machine.Subscriptions() {
		if subscription.ClientID == "later" {
			t.Fatal("cached conflict added subscription")
		}
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-234
func TestProposalRequestIdentitySeparatesMembershipTransitions(t *testing.T) {
	machine, _ := setupProposalTestMachine(t)
	operationID := "00000000-0000-4000-8000-000000008001"
	stage := stageRecord(operationID, "join-four", 4)
	stage.Member.ClusterID = "c1"
	stage.Member.ReleaseVersion = "1.0.0"
	stage.Member.Features = []string{ProposalRecoveryFeature}
	first := testCommand(operationID, &stage)
	firstDigest := sha256.Sum256([]byte("stage-four"))
	first.Proposal = &ProposalIdentityDTO{OriginNodeID: 1, ProofIndex: 5, RequestID: "00000000-0000-4000-8000-000000008002", CommandDigest: firstDigest[:]}
	staged := applyMustSucceed(t, machine, 6, first)
	if result, err := machine.ApplyConfiguration(7, operationID, ConfigurationActionAddLearner, 4); err != nil || result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("learner configuration: %+v %v", result, err)
	}
	member := *stage.Member
	member.Phase = MemberPhaseCatchingUp
	second := testCommand(operationID, &MembershipRecordDTO{OperationID: operationID, Action: MembershipActionAddLearner, IdempotencyKey: "join-four", Member: &member})
	secondDigest := sha256.Sum256([]byte("catch-up-four"))
	second.Proposal = &ProposalIdentityDTO{OriginNodeID: 1, ProofIndex: 7, RequestID: "00000000-0000-4000-8000-000000008003", CommandDigest: secondDigest[:]}
	catching := applyMustSucceed(t, machine, 8, second)
	if catching.Operation == nil || catching.Operation.Phase != OperationPhaseCatchingUp {
		t.Fatalf("second command shared first receipt: %+v", catching)
	}
	replay, err := machine.Apply(9, first)
	if err != nil || !replay.Replayed || replay.Operation.Phase != staged.Operation.Phase || replay.Index != staged.Index {
		t.Fatalf("first operation view changed: %+v %v", replay, err)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-235
func TestProposalWillResultBudgetRejectsBeforeMutationAndRemembersConflict(t *testing.T) {
	machine, _ := setupProposalTestMachine(t)
	index := uint64(5)
	for memberID := uint64(1); memberID <= 3; memberID++ {
		applyMustSucceed(t, machine, index, CommandDTO{SchemaMajor: SchemaMajor, OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", 10000+index), MemberProfile: &MemberProfileChangeDTO{MemberID: memberID, ReleaseVersion: "1.0.0", Features: []string{ProposalRecoveryFeature, SessionMetadataFeature}}})
		index++
	}
	applyMustSucceed(t, machine, index, CommandDTO{SchemaMajor: SchemaMajor, OperationID: "00000000-0000-4000-8000-000000010099", FeatureActivation: &FeatureActivationDTO{Feature: SessionMetadataFeature}})
	index++
	payload := make([]byte, MaxSessionWillPayloadBytes)
	payload[0] = 42
	var rejected CommandDTO
	var conflict ApplyResultDTO
	for epoch := uint64(1); epoch <= uint64(MaxProposalResultBytes/MaxSessionWillPayloadBytes+1); epoch++ {
		requestID := fmt.Sprintf("00000000-0000-4000-8000-%012d", 11000+index)
		digest := sha256.Sum256([]byte(requestID))
		acquire := CommandDTO{SchemaMajor: SchemaMajor, OperationID: requestID, Ownership: &OwnershipChangeDTO{TenantID: "acme", ClientID: "large-will", OwnerNodeID: 1, Epoch: epoch, ExpectedEpoch: epoch - 1, Action: OwnershipActionAcquire, Session: &SessionConnectDTO{ProtocolVersion: 5, Retention: SessionRetentionInfinite, ExpiryIntervalSeconds: ^uint32(0), Will: &SessionWillDTO{Topic: "t/will", Payload: payload, QoS: 1}}}, Proposal: &ProposalIdentityDTO{OriginNodeID: 1, ProofIndex: index - 1, RequestID: requestID, CommandDigest: digest[:]}}
		applyMustSucceed(t, machine, index, acquire)
		index++
		requestID = fmt.Sprintf("00000000-0000-4000-8000-%012d", 11000+index)
		digest = sha256.Sum256([]byte(requestID))
		claim := CommandDTO{SchemaMajor: SchemaMajor, OperationID: requestID, SessionMetadata: &SessionMetadataChangeDTO{TenantID: "acme", ClientID: "large-will", OwnerNodeID: 1, Epoch: epoch, Action: SessionMetadataActionDetach, WillDisposition: SessionWillDispositionClaim, ProposedAtUnixMillis: index}, Proposal: &ProposalIdentityDTO{OriginNodeID: 1, ProofIndex: index - 1, RequestID: requestID, CommandDigest: digest[:]}}
		result, err := machine.Apply(index, claim)
		if err != nil {
			t.Fatal(err)
		}
		index++
		if result.Conflict == ConflictProposalCapacity {
			rejected, conflict = claim, result
			break
		}
		if result.Outcome != ApplyOutcomeApplied || result.SessionMetadata == nil || result.SessionMetadata.Will == nil || result.SessionMetadata.Will.Payload[0] != 42 {
			t.Fatalf("Will claim result: %+v", result)
		}
		// Caller mutation must not change the result stored for replay.
		result.SessionMetadata.Will.Payload[0] = 99
		receipt, found := machine.ProposalReceipt(1, requestID)
		if !found || receipt.Result.SessionMetadata.Will.Payload[0] != 42 {
			t.Fatal("caller mutated stored Will receipt")
		}
	}
	if rejected.Proposal == nil {
		t.Fatal("large Will results exceeded the bounded result budget")
	}
	state, found := machine.GetSessionMetadata("acme", "large-will")
	if !found || !state.Connected || state.Will == nil || state.Will.Payload[0] != 42 {
		t.Fatalf("capacity conflict consumed Will: %+v", state)
	}
	fresh := testProposalCommand(1, index-1, rejected.Proposal.ProofIndex-1, "00000000-0000-4000-8000-000000019999", &DeliveryChangeDTO{TenantID: "acme", ClientID: "pub", Action: DeliveryActionPublish, Topic: "t/gc", PublishQoS: 1, ProposedAtUnixMillis: index})
	applyMustSucceed(t, machine, index, fresh)
	snapshot, err := machine.Snapshot(index)
	if err != nil {
		t.Fatal(err)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	replay, err := restored.Apply(index+1, rejected)
	if err != nil || !replay.Replayed || replay.Conflict != ConflictProposalCapacity || replay.Index != conflict.Index {
		t.Fatalf("capacity conflict changed after GC and restore: %+v %v", replay, err)
	}
	state, found = restored.GetSessionMetadata("acme", "large-will")
	if !found || !state.Connected || state.Will == nil {
		t.Fatal("replay consumed Will after capacity became available")
	}
}
func TestProposalIdentityValidationRejectsWithoutAdvancingState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ProposalIdentityDTO)
	}{
		{name: "zero origin", mutate: func(identity *ProposalIdentityDTO) { identity.OriginNodeID = 0 }},
		{name: "zero proof index", mutate: func(identity *ProposalIdentityDTO) { identity.ProofIndex = 0 }},
		{name: "retirement reaches proof", mutate: func(identity *ProposalIdentityDTO) { identity.RetiredThrough = identity.ProofIndex }},
		{name: "malformed request ID", mutate: func(identity *ProposalIdentityDTO) { identity.RequestID = "not-a-uuid" }},
		{name: "short digest", mutate: func(identity *ProposalIdentityDTO) {
			identity.CommandDigest = identity.CommandDigest[:len(identity.CommandDigest)-1]
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine, _ := setupProposalTestMachine(t)
			requestID := "00000000-0000-4000-8000-000000020001"
			command := testProposalCommand(1, 5, 0, requestID, &DeliveryChangeDTO{
				TenantID: "acme", ClientID: "pub", Action: DeliveryActionPublish,
				Topic: "t/validation", Payload: []byte("value"), PublishQoS: 1, ProposedAtUnixMillis: 100,
			})
			test.mutate(command.Proposal)
			beforeIndex := machine.LastAppliedIndex()
			beforeCount, beforeTotal, beforeRetired := machine.ProposalAdmission(1, 0)
			if _, err := machine.Apply(beforeIndex+1, command); !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("Apply error = %v, want ErrInvalidCommand", err)
			}
			if got := machine.LastAppliedIndex(); got != beforeIndex {
				t.Fatalf("invalid proposal advanced index to %d, want %d", got, beforeIndex)
			}
			count, total, retired := machine.ProposalAdmission(1, 0)
			if count != beforeCount || total != beforeTotal || retired != beforeRetired {
				t.Fatalf("invalid proposal changed admission state from (%d,%d,%d) to (%d,%d,%d)",
					beforeCount, beforeTotal, beforeRetired, count, total, retired)
			}
			if _, found := machine.ProposalReceipt(1, requestID); found {
				t.Fatal("invalid proposal created a receipt")
			}
		})
	}
}

func TestProposalSnapshotRestoreRejectsInvalidReceiptStateWithoutMutation(t *testing.T) {
	const requestID = "00000000-0000-4000-8000-000000020002"
	payload := &DeliveryChangeDTO{
		TenantID: "acme", ClientID: "pub", Action: DeliveryActionPublish,
		Topic: "t/restore-validation", Payload: []byte("value"), PublishQoS: 1,
		ProposedAtUnixMillis: 100,
	}
	source, _ := setupProposalTestMachine(t)
	command := testProposalCommand(1, 5, 0, requestID, payload)
	if result, err := source.Apply(6, command); err != nil || result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("seed source receipt: result=%+v error=%v", result, err)
	}
	valid, err := source.Snapshot(6)
	if err != nil {
		t.Fatalf("source snapshot: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*SnapshotDTO)
	}{
		{name: "proposal feature missing", mutate: func(snapshot *SnapshotDTO) {
			snapshot.ActiveFeatures = nil
			snapshot.ObjectCount--
		}},
		{name: "origin missing member", mutate: func(snapshot *SnapshotDTO) {
			snapshot.ProposalOrigins[0].OriginNodeID = 99
		}},
		{name: "rejected fence mismatch", mutate: func(snapshot *SnapshotDTO) {
			snapshot.ProposalOrigins[0].RejectedThrough = 1
		}},
		{name: "rejected fence not below index", mutate: func(snapshot *SnapshotDTO) {
			snapshot.ProposalOrigins[0].RejectedThrough = 5
			snapshot.ProposalOrigins[0].RejectedIndex = 5
		}},
		{name: "receipt proof retired", mutate: func(snapshot *SnapshotDTO) {
			snapshot.ProposalOrigins[0].RetiredThrough = snapshot.ProposalOrigins[0].Receipts[0].ProofIndex
		}},
		{name: "receipt proof not before result", mutate: func(snapshot *SnapshotDTO) {
			snapshot.ProposalOrigins[0].Receipts[0].ProofIndex = snapshot.ProposalOrigins[0].Receipts[0].Result.Index
		}},
		{name: "receipt result beyond snapshot", mutate: func(snapshot *SnapshotDTO) {
			snapshot.ProposalOrigins[0].Receipts[0].Result.Index = snapshot.LastIncludedIndex + 1
		}},
		{name: "receipt digest length", mutate: func(snapshot *SnapshotDTO) {
			snapshot.ProposalOrigins[0].Receipts[0].CommandDigest = []byte{1}
		}},
		{name: "receipt marked replayed", mutate: func(snapshot *SnapshotDTO) {
			snapshot.ProposalOrigins[0].Receipts[0].Result.Replayed = true
		}},
		{name: "receipt invalid historical outcome", mutate: func(snapshot *SnapshotDTO) {
			snapshot.ProposalOrigins[0].Receipts[0].Result.Outcome = ApplyOutcome(255)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target, _ := setupProposalTestMachine(t)
			if _, err := target.Apply(6, command); err != nil {
				t.Fatalf("seed target receipt: %v", err)
			}
			before, err := target.Snapshot(6)
			if err != nil {
				t.Fatalf("target snapshot before restore: %v", err)
			}
			invalid := cloneSnapshot(valid)
			test.mutate(&invalid)
			if err := target.Restore(invalid); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("Restore error = %v, want ErrInvalidSnapshot", err)
			}
			after, err := target.Snapshot(6)
			if err != nil {
				t.Fatalf("target snapshot after rejected restore: %v", err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("rejected restore changed target state\nbefore: %+v\nafter:  %+v", before, after)
			}
			receipt, found := target.ProposalReceipt(1, requestID)
			if !found || !reflect.DeepEqual(receipt, before.ProposalOrigins[0].Receipts[0]) {
				t.Fatalf("target receipt after rejected restore = %+v, found=%v", receipt, found)
			}
		})
	}
}
