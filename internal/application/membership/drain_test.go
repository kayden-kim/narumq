package membership

import (
	"errors"
	"testing"

	"narumq/internal/domain/nodedrain"
)

const (
	drainTestOperation1 = "00000000-0000-4000-8000-000000000101"
	drainTestOperation2 = "00000000-0000-4000-8000-000000000102"
)

func newDrainTestMachine(t *testing.T) *StateMachine {
	t.Helper()
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("InitializeBootstrap: %v", err)
	}
	return machine
}

func drainTestCommand(action NodeDrainAction, memberID uint64, drainOperationID string, proposedAt uint64) CommandDTO {
	change := NodeDrainChangeDTO{
		Action: action, MemberID: memberID, DrainOperationID: drainOperationID,
		ProposedAtUnixMillis: proposedAt,
	}
	if action == NodeDrainActionStart {
		change.IdempotencyKey = "drain-test"
		change.ConnectionRatePerSecond = 2
		change.LoadBalancerPropagationDelayMillis = 250
		change.TimeoutMillis = 2_000
	}
	return CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: testOperation4,
		NodeDrain: &change,
	}
}

func applyDrainTestCommand(t *testing.T, machine *StateMachine, index uint64, command CommandDTO) ApplyResultDTO {
	t.Helper()
	result, err := machine.Apply(index, command)
	if err != nil {
		t.Fatalf("Apply drain command at index %d: %v", index, err)
	}
	return result
}

func applyDrainTestStart(t *testing.T, machine *StateMachine, index uint64, memberID uint64, operationID string) ApplyResultDTO {
	t.Helper()
	result := applyDrainTestCommand(t, machine, index, drainTestCommand(NodeDrainActionStart, memberID, operationID, uint64(1_000+index)))
	if result.Outcome != ApplyOutcomeApplied || result.NodeDrain == nil {
		t.Fatalf("START result = %+v, want applied drain state", result)
	}
	return result
}

// Traceability: DEP-01/DEP-02/DEP-06; docs/specs/drain-and-rolling-update.md §1, §2, §3, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-005
func TestStateMachineNodeDrainLifecycleAndProgress(t *testing.T) {
	machine := newDrainTestMachine(t)
	started := applyDrainTestStart(t, machine, 2, 1, drainTestOperation1)
	if started.NodeDrain.Phase != nodedrain.PhaseExcludingTraffic || started.NodeDrain.DeadlineAtUnixMillis-started.NodeDrain.StartedAtUnixMillis != 2_000 {
		t.Fatalf("START state = %+v", started.NodeDrain)
	}

	progress := NodeDrainProgressDTO{ConnectionsInitial: 4, ConnectionsRemaining: 3, SessionsReadyForTakeover: 2}
	advance := drainTestCommand(NodeDrainActionAdvance, 1, drainTestOperation1, 1_003)
	advance.NodeDrain.Phase = nodedrain.PhaseWaitingForLB
	advance.NodeDrain.Progress = progress
	if result := applyDrainTestCommand(t, machine, 3, advance); result.Outcome != ApplyOutcomeApplied || result.NodeDrain.Phase != nodedrain.PhaseWaitingForLB || result.NodeDrain.PhaseStartedAtUnixMillis != 1_003 {
		t.Fatalf("advance to WAITING_FOR_LB = %+v", result)
	}

	progress.ConnectionsRemaining = 2
	advance = drainTestCommand(NodeDrainActionAdvance, 1, drainTestOperation1, 1_004)
	advance.NodeDrain.Phase = nodedrain.PhaseWaitingForLB
	advance.NodeDrain.Progress = progress
	if result := applyDrainTestCommand(t, machine, 4, advance); result.Outcome != ApplyOutcomeApplied || result.NodeDrain.Progress.ConnectionsRemaining != 2 || result.NodeDrain.PhaseStartedAtUnixMillis != 1_003 {
		t.Fatalf("same-phase progress = %+v", result)
	}

	for offset, phase := range []nodedrain.Phase{
		nodedrain.PhaseVerifyingSessionState,
		nodedrain.PhaseTransferringSessions,
		nodedrain.PhaseVerifyingBarrier,
	} {
		advance = drainTestCommand(NodeDrainActionAdvance, 1, drainTestOperation1, uint64(1_005+offset))
		advance.NodeDrain.Phase = phase
		result := applyDrainTestCommand(t, machine, uint64(5+offset), advance)
		if result.Outcome != ApplyOutcomeApplied || result.NodeDrain.Phase != phase || result.NodeDrain.PhaseStartedAtUnixMillis != advance.NodeDrain.ProposedAtUnixMillis {
			t.Fatalf("advance to %s = %+v", phase, result)
		}
	}

	progress.ConnectionsRemaining = 0
	progress.SessionsTakenOver = 2
	complete := drainTestCommand(NodeDrainActionComplete, 1, drainTestOperation1, 1_010)
	complete.NodeDrain.Progress = progress
	completed := applyDrainTestCommand(t, machine, 8, complete)
	if completed.Outcome != ApplyOutcomeApplied || completed.NodeDrain.Phase != nodedrain.PhaseCompleted || completed.NodeDrain.Progress.SessionsTakenOver != 2 {
		t.Fatalf("COMPLETE result = %+v", completed)
	}

	clear := drainTestCommand(NodeDrainActionClear, 1, drainTestOperation1, 1_011)
	cleared := applyDrainTestCommand(t, machine, 9, clear)
	if cleared.Outcome != ApplyOutcomeApplied || cleared.NodeDrain == nil || cleared.NodeDrain.Phase != nodedrain.PhaseCompleted {
		t.Fatalf("CLEAR result = %+v, want pre-delete completed record", cleared)
	}
	if _, exists := machine.GetNodeDrain(1); exists {
		t.Fatal("CLEAR left a node drain record")
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §1, §2, §3, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-005
func TestStateMachineNodeDrainStartConflicts(t *testing.T) {
	tests := []struct {
		name         string
		target       uint64
		prepare      func(*testing.T, *StateMachine)
		wantConflict ApplyConflictCode
	}{
		{name: "missing member", target: 99, wantConflict: ConflictMemberNotFound},
		{
			name: "non-voter", target: 4, wantConflict: ConflictPhase,
			prepare: func(t *testing.T, machine *StateMachine) {
				join := stageRecord(testOperation4, "join-4", 4)
				applyMustSucceed(t, machine, 2, testCommand(testOperation4, &join))
			},
		},
		{
			name: "same member already draining", target: 1, wantConflict: ConflictNodeDrainActive,
			prepare: func(t *testing.T, machine *StateMachine) {
				applyDrainTestStart(t, machine, 2, 1, drainTestOperation1)
			},
		},
		{
			name: "another member already draining", target: 2, wantConflict: ConflictNodeDrainInProgress,
			prepare: func(t *testing.T, machine *StateMachine) {
				applyDrainTestStart(t, machine, 2, 1, drainTestOperation1)
			},
		},
		{
			name: "membership operation in progress", target: 1, wantConflict: ConflictMembershipInProgress,
			prepare: func(t *testing.T, machine *StateMachine) {
				join := stageRecord(testOperation4, "join-4", 4)
				applyMustSucceed(t, machine, 2, testCommand(testOperation4, &join))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine := newDrainTestMachine(t)
			index := uint64(2)
			if test.prepare != nil {
				test.prepare(t, machine)
				index = machine.LastAppliedIndex() + 1
			}
			result := applyDrainTestCommand(t, machine, index, drainTestCommand(NodeDrainActionStart, test.target, drainTestOperation2, 2_000+index))
			if result.Outcome != ApplyOutcomeConflict || result.Conflict != test.wantConflict {
				t.Fatalf("START conflict = %q, want %q (result %+v)", result.Conflict, test.wantConflict, result)
			}
		})
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §1, §2.3, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-005
func TestStateMachineNodeDrainCancelRestartAndCompletedCannotCancel(t *testing.T) {
	machine := newDrainTestMachine(t)
	applyDrainTestStart(t, machine, 2, 2, drainTestOperation1)
	cancel := drainTestCommand(NodeDrainActionCancel, 2, drainTestOperation1, 1_003)
	cancelled := applyDrainTestCommand(t, machine, 3, cancel)
	if cancelled.Outcome != ApplyOutcomeApplied || cancelled.NodeDrain.Phase != nodedrain.PhaseCancelled {
		t.Fatalf("CANCEL result = %+v", cancelled)
	}

	restarted := applyDrainTestStart(t, machine, 4, 2, drainTestOperation2)
	if restarted.NodeDrain.DrainOperationID != drainTestOperation2 || restarted.NodeDrain.Phase != nodedrain.PhaseExcludingTraffic {
		t.Fatalf("restarted drain = %+v", restarted.NodeDrain)
	}
	for offset, phase := range []nodedrain.Phase{
		nodedrain.PhaseWaitingForLB,
		nodedrain.PhaseVerifyingSessionState,
		nodedrain.PhaseTransferringSessions,
		nodedrain.PhaseVerifyingBarrier,
	} {
		advance := drainTestCommand(NodeDrainActionAdvance, 2, drainTestOperation2, uint64(2_005+offset))
		advance.NodeDrain.Phase = phase
		if result := applyDrainTestCommand(t, machine, uint64(5+offset), advance); result.Outcome != ApplyOutcomeApplied {
			t.Fatalf("advance to %s = %+v", phase, result)
		}
	}
	complete := drainTestCommand(NodeDrainActionComplete, 2, drainTestOperation2, 2_010)
	if result := applyDrainTestCommand(t, machine, 9, complete); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("COMPLETE result = %+v", result)
	}
	cancel = drainTestCommand(NodeDrainActionCancel, 2, drainTestOperation2, 2_011)
	if result := applyDrainTestCommand(t, machine, 10, cancel); result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictDrainNotCancellable {
		t.Fatalf("COMPLETED CANCEL = %+v, want %q", result, ConflictDrainNotCancellable)
	}
}

// Traceability: DEP-10, CLU-14; docs/specs/drain-and-rolling-update.md §2.3.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-003
func TestStateMachineFailedDrainCancellationClearsFailure(t *testing.T) {
	machine := newDrainTestMachine(t)
	applyDrainTestStart(t, machine, 2, 2, drainTestOperation1)
	failure := drainTestCommand(NodeDrainActionFail, 2, drainTestOperation1, 1_003)
	failure.NodeDrain.FailureCode = "drain_timeout"
	failure.NodeDrain.BlockingReason = "insufficient_replicas"
	if result := applyDrainTestCommand(t, machine, 3, failure); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("FAIL result = %+v", result)
	}
	cancel := drainTestCommand(NodeDrainActionCancel, 2, drainTestOperation1, 1_004)
	result := applyDrainTestCommand(t, machine, 4, cancel)
	if result.Outcome != ApplyOutcomeApplied || result.NodeDrain.Phase != nodedrain.PhaseCancelled {
		t.Fatalf("CANCEL result = %+v", result)
	}
	if result.NodeDrain.FailureCode != "" || result.NodeDrain.BlockingReason != "" {
		t.Fatalf("cancelled drain retained failure fields: %+v", result.NodeDrain)
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §1, §3, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-009
func TestStateMachineNodeRemovalDeletesDrainRecord(t *testing.T) {
	machine := newDrainTestMachine(t)
	applyDrainTestStart(t, machine, 2, 3, drainTestOperation1)
	leaving := testMember(3, MemberPhaseLeaving, testOperation6)
	remove := MembershipRecordDTO{
		OperationID: testOperation6, Action: MembershipActionBeginRemove,
		IdempotencyKey: "remove-3", Member: &leaving, TargetMemberID: 3,
	}
	applyMustSucceed(t, machine, 3, testCommand(testOperation6, &remove))
	if result, err := machine.ApplyConfiguration(4, testOperation6, ConfigurationActionRemoveMember, 3); err != nil || result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("remove member configuration = (%+v, %v)", result, err)
	}
	if _, exists := machine.GetNodeDrain(3); exists {
		t.Fatal("member removal left its drain record")
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §1, §3, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-009
func TestStateMachineNodeDrainSnapshotRoundTripAndValidation(t *testing.T) {
	machine := newDrainTestMachine(t)
	applyDrainTestStart(t, machine, 2, 1, drainTestOperation1)
	snapshot, err := machine.Snapshot(2)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snapshot.ObjectCount != 4 || len(snapshot.NodeDrains) != 1 {
		t.Fatalf("snapshot object count/drains = %d/%d, want 4/1", snapshot.ObjectCount, len(snapshot.NodeDrains))
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	state, exists := restored.GetNodeDrain(1)
	if !exists || state.DrainOperationID != drainTestOperation1 || state.Phase != nodedrain.PhaseExcludingTraffic {
		t.Fatalf("restored drain = (%+v, %v)", state, exists)
	}

	invalid := cloneSnapshot(snapshot)
	invalid.NodeDrains[0].DeadlineAtUnixMillis++
	if err := NewStateMachine().Restore(invalid); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("inconsistent deadline restore error = %v, want ErrInvalidSnapshot", err)
	}

	multipleActive := cloneSnapshot(snapshot)
	second := multipleActive.NodeDrains[0]
	second.MemberID = 2
	second.DrainOperationID = drainTestOperation2
	multipleActive.NodeDrains = append(multipleActive.NodeDrains, second)
	multipleActive.ObjectCount++
	if err := NewStateMachine().Restore(multipleActive); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("multiple active drain restore error = %v, want ErrInvalidSnapshot", err)
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §2, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-005
func TestValidateNodeDrainCommandRejectsActionSpecificFieldsAndOverflow(t *testing.T) {
	valid := drainTestCommand(NodeDrainActionStart, 1, drainTestOperation1, 1_000)
	invalid := []struct {
		name   string
		mutate func(*NodeDrainChangeDTO)
	}{
		{name: "advance without phase", mutate: func(change *NodeDrainChangeDTO) { change.Action = NodeDrainActionAdvance }},
		{name: "start with progress", mutate: func(change *NodeDrainChangeDTO) { change.Progress.ConnectionsInitial = 1 }},
		{name: "overflowing deadline", mutate: func(change *NodeDrainChangeDTO) { change.ProposedAtUnixMillis = ^uint64(0) - 999 }},
		{name: "failure code on start", mutate: func(change *NodeDrainChangeDTO) { change.FailureCode = "timeout" }},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			command := valid
			change := *valid.NodeDrain
			command.NodeDrain = &change
			test.mutate(command.NodeDrain)
			if err := validateCommand(command); !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("validateCommand error = %v, want ErrInvalidCommand", err)
			}
		})
	}
}
