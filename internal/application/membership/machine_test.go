package membership

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

const (
	testOperation4 = "00000000-0000-4000-8000-000000000004"
	testOperation5 = "00000000-0000-4000-8000-000000000005"
	testOperation6 = "00000000-0000-4000-8000-000000000006"
	testOperation7 = "00000000-0000-4000-8000-000000000007"
	testOperation8 = "00000000-0000-4000-8000-000000000008"
)

func testMember(memberID uint64, phase MemberPhase, operationID string) MemberIdentityDTO {
	return MemberIdentityDTO{
		MemberID: memberID, ClusterID: "cluster-a",
		Address:           fmt.Sprintf("node%d.example:19%03d", memberID, memberID),
		DNSSAN:            fmt.Sprintf("node%d.example", memberID),
		CertificateSHA256: bytes.Repeat([]byte{byte(memberID)}, 32),
		ReleaseVersion:    "1.2.3", ClusterProtocolMajor: 2,
		StateSchemaMajor: 1, Phase: phase, OperationID: operationID,
	}
}

func testVoters() []MemberIdentityDTO {
	return []MemberIdentityDTO{
		testMember(1, MemberPhaseVoter, ""),
		testMember(2, MemberPhaseVoter, ""),
		testMember(3, MemberPhaseVoter, ""),
	}
}

func testCommand(operationID string, membership *MembershipRecordDTO) CommandDTO {
	return CommandDTO{SchemaMajor: 1, OperationID: operationID, Membership: membership}
}

func stageRecord(operationID, key string, memberID uint64) MembershipRecordDTO {
	member := testMember(memberID, MemberPhaseStaged, operationID)
	return MembershipRecordDTO{OperationID: operationID, Action: MembershipActionStageJoin, IdempotencyKey: key, Member: &member}
}

func applyMustSucceed(t *testing.T, machine *StateMachine, index uint64, command CommandDTO) ApplyResultDTO {
	t.Helper()
	result, err := machine.Apply(index, command)
	if err != nil {
		t.Fatalf("Apply(%d) returned error: %v", index, err)
	}
	if result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("Apply(%d) outcome = %v, conflict %q", index, result.Outcome, result.Conflict)
	}
	return result
}

func promoteVoterForTest(t *testing.T, machine *StateMachine, index uint64, memberID uint64) uint64 {
	t.Helper()
	operationID := fmt.Sprintf("00000000-0000-4000-8000-%012d", memberID)
	key := fmt.Sprintf("join-%d", memberID)
	stage := stageRecord(operationID, key, memberID)
	applyMustSucceed(t, machine, index, testCommand(operationID, &stage))
	learner, err := machine.ApplyConfiguration(index+1, operationID, ConfigurationActionAddLearner, memberID)
	if err != nil || learner.Outcome != ApplyOutcomeApplied {
		t.Fatalf("add learner: result=%+v err=%v", learner, err)
	}
	member := testMember(memberID, MemberPhaseCatchingUp, operationID)
	catchingUp := MembershipRecordDTO{
		OperationID: operationID, Action: MembershipActionAddLearner,
		IdempotencyKey: key, Member: &member,
	}
	applyMustSucceed(t, machine, index+2, testCommand(operationID, &catchingUp))
	promoted, err := machine.ApplyConfiguration(index+3, operationID, ConfigurationActionPromoteVoter, memberID)
	if err != nil || promoted.Outcome != ApplyOutcomeApplied || promoted.Operation.Phase != OperationPhaseSucceeded {
		t.Fatalf("promote voter: result=%+v err=%v", promoted, err)
	}
	return index + 4
}

func TestStateMachineInitializeBootstrapRequiresThreeCanonicalVoters(t *testing.T) {
	machine := NewStateMachine()
	voters := testVoters()
	if err := machine.InitializeBootstrap(0, voters); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("zero bootstrap index error=%v, want ErrInvalidCommand", err)
	}
	if err := machine.InitializeBootstrap(1, voters[:2]); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("bootstrap member count error=%v, want ErrInvalidCommand", err)
	}
	unsorted := append([]MemberIdentityDTO(nil), voters...)
	unsorted[0], unsorted[1] = unsorted[1], unsorted[0]
	if err := machine.InitializeBootstrap(1, unsorted); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("unsorted bootstrap error=%v, want ErrInvalidCommand", err)
	}
	mismatchedProfile := append([]MemberIdentityDTO(nil), voters...)
	mismatchedProfile[2].ReleaseVersion = "1.2.4"
	if err := machine.InitializeBootstrap(1, mismatchedProfile); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("mixed bootstrap profile error=%v, want ErrInvalidCommand", err)
	}
	if err := machine.InitializeBootstrap(17, voters); err != nil {
		t.Fatalf("valid bootstrap: %v", err)
	}
	if machine.LastAppliedIndex() != 17 || len(machine.Operations()) != 0 || !reflect.DeepEqual(machine.Members(), voters) {
		t.Fatalf("bootstrap state index=%d members=%+v operations=%+v", machine.LastAppliedIndex(), machine.Members(), machine.Operations())
	}
	if err := machine.InitializeBootstrap(18, voters); !errors.Is(err, ErrBootstrapAlreadyInitialized) {
		t.Fatalf("repeated bootstrap error=%v, want ErrBootstrapAlreadyInitialized", err)
	}
}

func TestStateMachineCommittedIndexBoundaries(t *testing.T) {
	machine := NewStateMachine()
	valid := CommandDTO{
		SchemaMajor: 1, OperationID: testOperation4,
		Ownership: &OwnershipChangeDTO{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire},
	}
	if _, err := machine.Apply(0, valid); !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("zero index error = %v, want ErrInvalidIndex", err)
	}
	membership := stageRecord(testOperation4, "join-4", 4)
	membership.CommittedIndex = 9
	if _, err := machine.Apply(1, testCommand(testOperation4, &membership)); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("proposal committed index error=%v, want ErrInvalidCommand", err)
	}
	if machine.LastAppliedIndex() != 0 || len(machine.Members()) != 0 {
		t.Fatal("proposal-supplied committed index changed machine state")
	}

	invalid := valid
	invalid.SchemaMinor = 1
	if _, err := machine.Apply(1, invalid); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("invalid command error = %v, want ErrInvalidCommand", err)
	}
	if machine.LastAppliedIndex() != 0 {
		t.Fatalf("invalid command advanced index to %d", machine.LastAppliedIndex())
	}
	applyMustSucceed(t, machine, 1, valid)
	if _, err := machine.Apply(1, valid); !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("repeated index error = %v, want ErrInvalidIndex", err)
	}
	if machine.LastAppliedIndex() != 1 {
		t.Fatalf("repeated index changed last applied index to %d", machine.LastAppliedIndex())
	}
}

func TestStateMachineJoinConfigurationPhaseAlignment(t *testing.T) {
	machine := NewStateMachine()
	stage := stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, machine, 1, testCommand(testOperation4, &stage))

	wrongMember, err := machine.ApplyConfiguration(2, testOperation4, ConfigurationActionAddLearner, 5)
	if err != nil || wrongMember.Outcome != ApplyOutcomeConflict || wrongMember.Conflict != ConflictConfigurationMismatch {
		t.Fatalf("misaligned learner configuration = (%+v, %v), want configuration conflict", wrongMember, err)
	}
	if machine.LastAppliedIndex() != 2 || machine.Members()[0].Phase != MemberPhaseStaged {
		t.Fatalf("misaligned configuration partially changed state: index=%d members=%+v", machine.LastAppliedIndex(), machine.Members())
	}

	learner, err := machine.ApplyConfiguration(3, testOperation4, ConfigurationActionAddLearner, 4)
	if err != nil || learner.Outcome != ApplyOutcomeApplied || learner.Operation.Phase != OperationPhaseLearner {
		t.Fatalf("learner configuration = (%+v, %v), want learner phase", learner, err)
	}
	tooEarly, err := machine.ApplyConfiguration(4, testOperation4, ConfigurationActionPromoteVoter, 4)
	if err != nil || tooEarly.Outcome != ApplyOutcomeConflict || tooEarly.Conflict != ConflictConfigurationMismatch {
		t.Fatalf("early promotion = (%+v, %v), want configuration conflict", tooEarly, err)
	}
	if machine.Members()[0].Phase != MemberPhaseLearner {
		t.Fatalf("early promotion changed member phase to %v", machine.Members()[0].Phase)
	}

	catchingUpMember := testMember(4, MemberPhaseCatchingUp, testOperation4)
	catchingUp := MembershipRecordDTO{
		OperationID: testOperation4, Action: MembershipActionAddLearner,
		IdempotencyKey: "join-4", Member: &catchingUpMember,
	}
	catchingResult := applyMustSucceed(t, machine, 5, testCommand(testOperation4, &catchingUp))
	if catchingResult.Operation.Phase != OperationPhaseCatchingUp || machine.Members()[0].Phase != MemberPhaseCatchingUp {
		t.Fatalf("catch-up transition result=%+v member=%+v", catchingResult.Operation, machine.Members()[0])
	}
	promoted, err := machine.ApplyConfiguration(6, testOperation4, ConfigurationActionPromoteVoter, 4)
	if err != nil || promoted.Outcome != ApplyOutcomeApplied || promoted.Operation.Phase != OperationPhaseSucceeded {
		t.Fatalf("promotion = (%+v, %v), want succeeded", promoted, err)
	}
	members := machine.Members()
	if members[0].Phase != MemberPhaseVoter || members[0].OperationID != "" {
		t.Fatalf("promoted identity = %+v, want stable voter", members[0])
	}
}

func TestStateMachineRemovalConfigurationAlignmentAndTombstone(t *testing.T) {
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	leavingMember := testMember(3, MemberPhaseLeaving, testOperation6)
	remove := MembershipRecordDTO{
		OperationID: testOperation6, Action: MembershipActionBeginRemove,
		IdempotencyKey: "remove-3", Member: &leavingMember, TargetMemberID: 3,
	}
	start := applyMustSucceed(t, machine, 2, testCommand(testOperation6, &remove))
	if start.Operation.Phase != OperationPhaseRemoving {
		t.Fatalf("planned removal phase = %s, want REMOVING", start.Operation.Phase)
	}
	misaligned, err := machine.ApplyConfiguration(3, testOperation6, ConfigurationActionPromoteVoter, 3)
	if err != nil || misaligned.Outcome != ApplyOutcomeConflict || misaligned.Conflict != ConflictConfigurationMismatch {
		t.Fatalf("wrong removal configuration = (%+v, %v)", misaligned, err)
	}
	removed, err := machine.ApplyConfiguration(4, testOperation6, ConfigurationActionRemoveMember, 3)
	if err != nil || removed.Outcome != ApplyOutcomeApplied || removed.Operation.Phase != OperationPhaseSucceeded {
		t.Fatalf("committed removal = (%+v, %v)", removed, err)
	}
	members := machine.Members()
	if members[2].Phase != MemberPhaseRetired || members[2].OperationID != "" {
		t.Fatalf("removed member = %+v, want retired tombstone", members[2])
	}
	reserved := stageRecord(testOperation4, "reuse-3", 3)
	result := mustApplyResult(t, machine, 5, testCommand(testOperation4, &reserved))
	if result.Conflict != ConflictMemberIDReserved {
		t.Fatalf("reused retired member ID conflict = %q, want %q", result.Conflict, ConflictMemberIDReserved)
	}
}

func TestStateMachineAbortRetainsMemberIDTombstone(t *testing.T) {
	machine := NewStateMachine()
	stage := stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, machine, 1, testCommand(testOperation4, &stage))
	secondStage := stageRecord(testOperation5, "join-5", 5)
	inProgress := mustApplyResult(t, machine, 2, testCommand(testOperation5, &secondStage))
	if inProgress.Conflict != ConflictMembershipInProgress || len(machine.Members()) != 1 {
		t.Fatalf("second membership operation result=%+v members=%+v", inProgress, machine.Members())
	}

	retired := testMember(4, MemberPhaseRetired, "")
	abort := MembershipRecordDTO{
		OperationID: testOperation4, Action: MembershipActionAbortStage,
		IdempotencyKey: "join-4", Member: &retired,
	}
	aborted := applyMustSucceed(t, machine, 3, testCommand(testOperation4, &abort))
	if aborted.Operation.Phase != OperationPhaseAborted || machine.Members()[0].Phase != MemberPhaseRetired {
		t.Fatalf("abort result=%+v member=%+v", aborted.Operation, machine.Members()[0])
	}
	reuse := stageRecord(testOperation5, "join-5", 4)
	result := mustApplyResult(t, machine, 4, testCommand(testOperation5, &reuse))
	if result.Conflict != ConflictMemberIDReserved || machine.Members()[0].Phase != MemberPhaseRetired {
		t.Fatalf("retired ID reuse result=%+v members=%+v", result, machine.Members())
	}
}

func TestStateMachineIdempotencyAndContentConflict(t *testing.T) {
	machine := NewStateMachine()
	original := stageRecord(testOperation4, "same-key", 4)
	first := applyMustSucceed(t, machine, 1, testCommand(testOperation4, &original))
	retry := stageRecord(testOperation5, "same-key", 4)
	retried := applyMustSucceed(t, machine, 2, testCommand(testOperation5, &retry))
	if retried.Operation.OperationID != first.Operation.OperationID || retried.Operation.CommittedIndex != first.Operation.CommittedIndex {
		t.Fatalf("same-content retry result=%+v, original=%+v", retried.Operation, first.Operation)
	}
	changed := stageRecord("00000000-0000-4000-8000-000000000007", "same-key", 4)
	changed.Member.Address = "other.example:19004"
	conflict := mustApplyResult(t, machine, 3, testCommand(changed.OperationID, &changed))
	if conflict.Conflict != ConflictIdempotency || machine.LastAppliedIndex() != 3 || len(machine.Operations()) != 1 {
		t.Fatalf("changed-content retry = %+v, index=%d ops=%+v", conflict, machine.LastAppliedIndex(), machine.Operations())
	}
	otherKey := stageRecord("00000000-0000-4000-8000-000000000008", "other-key", 4)
	reserved := mustApplyResult(t, machine, 4, testCommand(otherKey.OperationID, &otherKey))
	if reserved.Conflict != ConflictMemberIDReserved {
		t.Fatalf("same member ID with new key conflict=%q", reserved.Conflict)
	}
}

func TestStateMachineOwnershipFencingAndSubscriptionUpdateRemove(t *testing.T) {
	machine := NewStateMachine()
	acquire := CommandDTO{
		SchemaMajor: 1, OperationID: testOperation4,
		Ownership: &OwnershipChangeDTO{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire},
	}
	applyMustSucceed(t, machine, 1, acquire)
	staleTransfer := CommandDTO{
		SchemaMajor: 1, OperationID: testOperation5,
		Ownership: &OwnershipChangeDTO{TenantID: "tenant", ClientID: "client", ExpectedEpoch: 0, OwnerNodeID: 2, Epoch: 1, Action: OwnershipActionTransfer},
	}
	if result := mustApplyResult(t, machine, 2, staleTransfer); result.Conflict != ConflictStaleEpoch {
		t.Fatalf("stale ownership transfer conflict=%q", result.Conflict)
	}
	transfer := staleTransfer
	transfer.Ownership = &OwnershipChangeDTO{TenantID: "tenant", ClientID: "client", ExpectedEpoch: 1, OwnerNodeID: 2, Epoch: 2, Action: OwnershipActionTransfer}
	applyMustSucceed(t, machine, 3, transfer)
	if owner, _ := machine.GetOwnership("tenant", "client"); owner.OwnerNodeID != 2 || owner.Epoch != 2 || owner.CommittedIndex != 3 {
		t.Fatalf("transferred ownership = %+v", owner)
	}

	staleSubscription := CommandDTO{
		SchemaMajor: 1, OperationID: testOperation6,
		Subscription: &SubscriptionChangeDTO{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Filter: "orders/#", QoS: 1, Action: SubscriptionActionAdd},
	}
	if result := mustApplyResult(t, machine, 4, staleSubscription); result.Conflict != ConflictStaleEpoch {
		t.Fatalf("stale subscription conflict=%q", result.Conflict)
	}
	add := staleSubscription
	add.Subscription = &SubscriptionChangeDTO{
		TenantID: "tenant", ClientID: "client", OwnerNodeID: 2, Epoch: 2,
		Filter: "orders/#", QoS: 2, NoLocal: true, RetainAsPublished: true,
		RetainHandling: 2, Action: SubscriptionActionAdd,
	}
	applyMustSucceed(t, machine, 5, add)
	update := add
	update.OperationID = "00000000-0000-4000-8000-000000000009"
	update.Subscription = &SubscriptionChangeDTO{
		TenantID: "tenant", ClientID: "client", OwnerNodeID: 2, Epoch: 2,
		Filter: "orders/#", QoS: 1, RetainHandling: 1, Action: SubscriptionActionAdd,
	}
	updated := applyMustSucceed(t, machine, 6, update)
	if updated.Subscription == nil || updated.Subscription.QoS != 1 || updated.Subscription.NoLocal || updated.Subscription.RetainAsPublished || updated.Subscription.RetainHandling != 1 {
		t.Fatalf("subscription update result=%+v", updated.Subscription)
	}
	remove := update
	remove.OperationID = "00000000-0000-4000-8000-000000000010"
	remove.Subscription = &SubscriptionChangeDTO{TenantID: "tenant", ClientID: "client", OwnerNodeID: 2, Epoch: 2, Filter: "orders/#", Action: SubscriptionActionRemove}
	applyMustSucceed(t, machine, 7, remove)
	if subscriptions := machine.Subscriptions(); len(subscriptions) != 0 {
		t.Fatalf("subscriptions after remove = %+v", subscriptions)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-063
func TestStateMachineOwnershipTransferRebindsSubscriptionsAndPreservesDelivery(t *testing.T) {
	machine := NewStateMachine()
	applyMustSucceed(t, machine, 1, CommandDTO{
		SchemaMajor: 1, OperationID: testOperation4,
		Ownership: &OwnershipChangeDTO{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire},
	})
	applyMustSucceed(t, machine, 2, CommandDTO{
		SchemaMajor: 1, OperationID: testOperation5,
		Subscription: &SubscriptionChangeDTO{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Filter: "orders/#", QoS: 1, NoLocal: true, Action: SubscriptionActionAdd},
	})
	applyMustSucceed(t, machine, 3, CommandDTO{
		SchemaMajor: 1, OperationID: testOperation6,
		Delivery: &DeliveryChangeDTO{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 1},
	})
	applyMustSucceed(t, machine, 4, CommandDTO{
		SchemaMajor: 1, OperationID: testOperation7,
		Delivery: &DeliveryChangeDTO{
			TenantID: "tenant", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 2,
			Topic: "orders/new", Payload: []byte("payload"), PublishQoS: 1,
			Targets: []DeliveryTargetDTO{{ClientID: "client"}},
		},
	})
	transfer := CommandDTO{
		SchemaMajor: 1, OperationID: testOperation8,
		Ownership: &OwnershipChangeDTO{TenantID: "tenant", ClientID: "client", ExpectedEpoch: 1, OwnerNodeID: 2, Epoch: 2, Action: OwnershipActionTransfer},
	}
	applyMustSucceed(t, machine, 5, transfer)
	subscriptions := machine.Subscriptions()
	if len(subscriptions) != 1 || subscriptions[0].OwnerNodeID != 2 || subscriptions[0].Epoch != 2 ||
		subscriptions[0].Filter != "orders/#" || subscriptions[0].QoS != 1 || !subscriptions[0].NoLocal {
		t.Fatalf("subscription state after ownership transfer = %+v", subscriptions)
	}
	state, exists := machine.DeliverySession("tenant", "client")
	if !exists || state.NextSequence != 2 || len(state.Messages) != 1 || string(state.Messages[0].Payload) != "payload" {
		t.Fatalf("delivery state after ownership transfer = (%+v, %t)", state, exists)
	}
	stale := CommandDTO{
		SchemaMajor: 1, OperationID: "00000000-0000-4000-8000-000000000011",
		Delivery: &DeliveryChangeDTO{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
			Action: DeliveryActionSend, ProposedAtUnixMillis: 3, Sequence: 1, PacketID: 7},
	}
	if result := mustApplyResult(t, machine, 6, stale); result.Conflict != ConflictStaleEpoch {
		t.Fatalf("stale delivery command after transfer conflict = %q", result.Conflict)
	}
	if state, _ := machine.DeliverySession("tenant", "client"); state.Messages[0].PacketID != 0 {
		t.Fatalf("stale owner changed transferred delivery: %+v", state)
	}
	applyMustSucceed(t, machine, 7, CommandDTO{
		SchemaMajor: 1, OperationID: "00000000-0000-4000-8000-000000000012",
		Ownership: &OwnershipChangeDTO{TenantID: "tenant", ClientID: "client", ExpectedEpoch: 2, OwnerNodeID: 2, Epoch: 2, Action: OwnershipActionRelease},
	})
	if _, exists := machine.DeliverySession("tenant", "client"); exists {
		t.Fatal("ownership release retained durable delivery state")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-063
func TestStateMachineDeliveryInboundReleaseAndSnapshotValidation(t *testing.T) {
	machine := NewStateMachine()
	applyMustSucceed(t, machine, 1, CommandDTO{
		SchemaMajor: 1, OperationID: testOperation4,
		Ownership: &OwnershipChangeDTO{TenantID: "tenant", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire},
	})
	applyMustSucceed(t, machine, 2, CommandDTO{
		SchemaMajor: 1, OperationID: testOperation5,
		Delivery: &DeliveryChangeDTO{TenantID: "tenant", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 1},
	})
	receive := DeliveryChangeDTO{TenantID: "tenant", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1,
		Action: DeliveryActionReceive, ProposedAtUnixMillis: 2, PacketID: 9, Topic: "orders/new", Payload: []byte("payload")}
	applyMustSucceed(t, machine, 3, CommandDTO{SchemaMajor: 1, OperationID: testOperation6, Delivery: &receive})
	result := applyMustSucceed(t, machine, 4, CommandDTO{SchemaMajor: 1, OperationID: testOperation7, Delivery: &DeliveryChangeDTO{
		TenantID: "tenant", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1,
		Action: DeliveryActionRelease, ProposedAtUnixMillis: 3, PacketID: 9, PublishQoS: 2,
	}})
	if result.Delivery == nil || !result.Delivery.Found || !result.Delivery.Released {
		t.Fatalf("first RELEASE result = %+v", result.Delivery)
	}
	state, _ := machine.DeliverySession("tenant", "publisher")
	if len(state.Inbound) != 1 || !state.Inbound[0].Released || state.Inbound[0].ReleasedIndex != 4 ||
		len(state.Inbound[0].Payload) != 0 || state.Inbound[0].Topic != "orders/new" {
		t.Fatalf("RELEASE did not retain a compact tombstone: %+v", state.Inbound)
	}
	duplicate := applyMustSucceed(t, machine, 5, CommandDTO{SchemaMajor: 1, OperationID: testOperation8, Delivery: &DeliveryChangeDTO{
		TenantID: "tenant", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1,
		Action: DeliveryActionRelease, ProposedAtUnixMillis: 4, PacketID: 9, PublishQoS: 2,
	}})
	if duplicate.Delivery.Found || !duplicate.Delivery.Released || len(duplicate.Delivery.Enqueued) != 0 {
		t.Fatalf("duplicate RELEASE result = %+v", duplicate.Delivery)
	}
	snapshot, err := machine.Snapshot(5)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	invalid := cloneSnapshot(snapshot)
	invalid.DeliverySessions[0].Inbound[0].ReleasedIndex = 0
	if err := NewStateMachine().Restore(invalid); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("invalid delivery snapshot restore error = %v", err)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore delivery snapshot: %v", err)
	}
	if got, ok := restored.DeliverySession("tenant", "publisher"); !ok || !reflect.DeepEqual(got, state) {
		t.Fatalf("restored delivery session = (%+v, %t), want (%+v, true)", got, ok, state)
	}
}

func TestStateMachineSnapshotCanonicalOrderAndDeepOwnership(t *testing.T) {
	machine := NewStateMachine()
	voters := testVoters()
	if err := machine.InitializeBootstrap(1, voters); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	for index, tenantID := range []string{"zeta", "alpha"} {
		command := CommandDTO{
			SchemaMajor: 1, OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index+10),
			Ownership: &OwnershipChangeDTO{TenantID: tenantID, ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire},
		}
		applyMustSucceed(t, machine, uint64(index+2), command)
	}
	tenants := []string{"zeta", "alpha"}
	for index, tenantID := range tenants {
		subscription := &SubscriptionChangeDTO{TenantID: tenantID, ClientID: "client", OwnerNodeID: 1, Epoch: 1, Filter: "events/#", QoS: 1, Action: SubscriptionActionAdd}
		command := CommandDTO{SchemaMajor: 1, OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index+20), Subscription: subscription}
		applyMustSucceed(t, machine, uint64(index+4), command)
	}
	for index, tenantID := range tenants {
		delivery := &DeliveryChangeDTO{TenantID: tenantID, ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 1}
		command := CommandDTO{SchemaMajor: 1, OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index+30), Delivery: delivery}
		applyMustSucceed(t, machine, uint64(index+6), command)
	}
	for index, tenantID := range tenants {
		publish := &DeliveryChangeDTO{
			TenantID: tenantID, ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 2,
			Topic: "events/in", Payload: []byte(tenantID), PublishQoS: 1, Targets: []DeliveryTargetDTO{{ClientID: "client"}},
		}
		command := CommandDTO{SchemaMajor: 1, OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index+40), Delivery: publish}
		applyMustSucceed(t, machine, uint64(index+8), command)
	}
	stage := stageRecord(testOperation5, "join-5", 5)
	applyMustSucceed(t, machine, 10, testCommand(testOperation5, &stage))
	retired := testMember(5, MemberPhaseRetired, "")
	abort := MembershipRecordDTO{OperationID: testOperation5, Action: MembershipActionAbortStage, IdempotencyKey: "join-5", Member: &retired}
	applyMustSucceed(t, machine, 11, testCommand(testOperation5, &abort))
	stage = stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, machine, 12, testCommand(testOperation4, &stage))

	snapshot, err := machine.Snapshot(12)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.ObjectCount != 15 || snapshot.Members[0].MemberID != 1 || snapshot.Members[4].MemberID != 5 || snapshot.Ownerships[0].TenantID != "alpha" || snapshot.Subscriptions[0].TenantID != "alpha" || snapshot.DeliverySessions[0].TenantID != "alpha" || snapshot.MembershipHistory[0].OperationID != testOperation4 {
		t.Fatalf("snapshot order/count is not canonical: %+v", snapshot)
	}
	repeated, err := machine.Snapshot(12)
	if err != nil || !reflect.DeepEqual(snapshot, repeated) {
		t.Fatalf("repeated snapshot differs: err=%v", err)
	}
	if _, err := machine.Snapshot(11); !errors.Is(err, ErrSnapshotIndexMismatch) {
		t.Fatalf("wrong requested snapshot index error=%v", err)
	}

	restoreInput := cloneSnapshot(snapshot)
	snapshot.Members[0].CertificateSHA256[0] ^= 0xff
	snapshot.DeliverySessions[0].Messages[0].Payload[0] = 'X'
	if machine.Members()[0].CertificateSHA256[0] != voters[0].CertificateSHA256[0] || string(machine.DeliverySessions()[0].Messages[0].Payload) != "alpha" {
		t.Fatal("snapshot result shared mutable memory with state machine")
	}
	members := machine.Members()
	members[0].CertificateSHA256[0] ^= 0xff
	deliverySessions := machine.DeliverySessions()
	deliverySessions[0].Messages[0].Payload[0] = 'Y'
	if machine.Members()[0].CertificateSHA256[0] != voters[0].CertificateSHA256[0] || string(machine.DeliverySessions()[0].Messages[0].Payload) != "alpha" {
		t.Fatal("read-only getters shared mutable memory with state machine")
	}

	restored := NewStateMachine()
	if err := restored.Restore(restoreInput); err != nil {
		t.Fatalf("restore canonical snapshot: %v", err)
	}
	if restored.LastAppliedIndex() != 12 || !reflect.DeepEqual(restored.Members(), machine.Members()) || !reflect.DeepEqual(restored.Ownerships(), machine.Ownerships()) || !reflect.DeepEqual(restored.Subscriptions(), machine.Subscriptions()) || !reflect.DeepEqual(restored.DeliverySessions(), machine.DeliverySessions()) {
		t.Fatalf("restored state differs: index=%d", restored.LastAppliedIndex())
	}
	aborted, exists := restored.GetOperation(testOperation5)
	if !exists || aborted.CommittedIndex != 11 || aborted.Record.CommittedIndex != 11 {
		t.Fatalf("restored aborted operation lost exact committed index: %+v", aborted)
	}
	staged, exists := restored.GetOperation(testOperation4)
	if !exists || staged.CommittedIndex != 12 || staged.Record.CommittedIndex != 12 {
		t.Fatalf("restored staged operation lost exact committed index: %+v", staged)
	}
}

func TestStateMachineRestoreRejectsCorruptionDuplicatesAndIndexMismatch(t *testing.T) {
	source := NewStateMachine()
	if err := source.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("initialize source bootstrap: %v", err)
	}
	valid, err := source.Snapshot(1)
	if err != nil {
		t.Fatal(err)
	}
	target := NewStateMachine()
	duplicateMember := cloneSnapshot(valid)
	duplicateMember.Members = append(duplicateMember.Members, cloneMember(duplicateMember.Members[0]))
	duplicateMember.ObjectCount++
	if err := target.Restore(duplicateMember); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("duplicate member restore error=%v, want ErrInvalidSnapshot", err)
	}
	if target.LastAppliedIndex() != 0 || len(target.Members()) != 0 {
		t.Fatal("corrupt restore partially changed state")
	}
	unsorted := cloneSnapshot(valid)
	unsorted.Members[0], unsorted.Members[1] = unsorted.Members[1], unsorted.Members[0]
	if err := target.Restore(unsorted); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("unsorted member restore error=%v, want ErrInvalidSnapshot", err)
	}
	wrongCount := cloneSnapshot(valid)
	wrongCount.ObjectCount++
	if err := target.Restore(wrongCount); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("wrong object count restore error=%v, want ErrInvalidSnapshot", err)
	}

	stageMachine := NewStateMachine()
	stage := stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, stageMachine, 1, testCommand(testOperation4, &stage))
	retired := testMember(4, MemberPhaseRetired, "")
	abort := MembershipRecordDTO{OperationID: testOperation4, Action: MembershipActionAbortStage, IdempotencyKey: "join-4", Member: &retired}
	applyMustSucceed(t, stageMachine, 2, testCommand(testOperation4, &abort))
	withHistory, err := stageMachine.Snapshot(2)
	if err != nil {
		t.Fatal(err)
	}
	duplicateHistory := cloneSnapshot(withHistory)
	duplicateHistory.MembershipHistory = append(duplicateHistory.MembershipHistory, cloneMembershipRecord(duplicateHistory.MembershipHistory[0]))
	duplicateHistory.ObjectCount++
	if err := target.Restore(duplicateHistory); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("duplicate operation restore error=%v, want ErrInvalidSnapshot", err)
	}
	zeroHistoryIndex := cloneSnapshot(withHistory)
	zeroHistoryIndex.MembershipHistory[0].CommittedIndex = 0
	if err := target.Restore(zeroHistoryIndex); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("zero operation history index restore error=%v, want ErrInvalidSnapshot", err)
	}
	futureHistoryIndex := cloneSnapshot(withHistory)
	futureHistoryIndex.MembershipHistory[0].CommittedIndex = withHistory.LastIncludedIndex + 1
	if err := target.Restore(futureHistoryIndex); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("future operation history index restore error=%v, want ErrInvalidSnapshot", err)
	}

	current := NewStateMachine()
	ownership := CommandDTO{SchemaMajor: 1, OperationID: testOperation4, Ownership: &OwnershipChangeDTO{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire}}
	applyMustSucceed(t, current, 1, ownership)
	if err := current.Restore(valid); !errors.Is(err, ErrSnapshotIndexMismatch) {
		t.Fatalf("same-index state mismatch restore error=%v, want ErrSnapshotIndexMismatch", err)
	}
	applyMustSucceed(t, current, 2, CommandDTO{SchemaMajor: 1, OperationID: testOperation5, Ownership: &OwnershipChangeDTO{TenantID: "other", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire}})
	if err := current.Restore(valid); !errors.Is(err, ErrSnapshotIndexMismatch) {
		t.Fatalf("stale snapshot restore error=%v, want ErrSnapshotIndexMismatch", err)
	}
	if current.LastAppliedIndex() != 2 {
		t.Fatalf("rejected snapshot changed machine index to %d", current.LastAppliedIndex())
	}
}

func mustApplyResult(t *testing.T, machine *StateMachine, index uint64, command CommandDTO) ApplyResultDTO {
	t.Helper()
	result, err := machine.Apply(index, command)
	if err != nil {
		t.Fatalf("Apply(%d) returned error: %v", index, err)
	}
	if result.Outcome != ApplyOutcomeConflict {
		t.Fatalf("Apply(%d) = %+v, want deterministic conflict", index, result)
	}
	return result
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-023
func TestAdvanceAppliedIndexPreservesCommittedStateAcrossEmptyRaftEntries(t *testing.T) {
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(3, testVoters()); err != nil {
		t.Fatal(err)
	}
	record := stageRecord(testOperation4, "join-4", 4)
	applyMustSucceed(t, machine, 5, testCommand(testOperation4, &record))
	if err := machine.AdvanceAppliedIndex(7); err != nil {
		t.Fatalf("advance over empty Raft entries: %v", err)
	}
	if _, err := machine.Snapshot(5); !errors.Is(err, ErrSnapshotIndexMismatch) {
		t.Fatalf("snapshot at stale application index = %v", err)
	}
	snapshot, err := machine.Snapshot(7)
	if err != nil {
		t.Fatalf("snapshot at Raft applied index: %v", err)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore after empty entries: %v", err)
	}
	if restored.LastAppliedIndex() != 7 || !reflect.DeepEqual(restored.Members(), machine.Members()) ||
		!reflect.DeepEqual(restored.Operations(), machine.Operations()) {
		t.Fatalf("restored state differs after empty entries: index=%d", restored.LastAppliedIndex())
	}
	if err := restored.AdvanceAppliedIndex(6); !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("backward index advance = %v", err)
	}
}
