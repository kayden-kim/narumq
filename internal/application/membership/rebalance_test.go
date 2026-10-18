package membership

import (
	"testing"

	"narumq/internal/domain/connrebalance"
)

// Traceability: CLU-13; docs/specs/cluster-session-and-delivery.md §9 and §11.31.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-224
func TestConnectionRebalanceApplyConflicts(t *testing.T) {
	t.Run("feature inactive", func(t *testing.T) {
		machine := NewStateMachine()
		if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
			t.Fatal(err)
		}
		result := applyRebalanceTestCommand(t, machine, 2, startRebalanceTestChange("rb-inactive", 2))
		if result.Conflict != ConflictFeatureInactive {
			t.Fatalf("conflict = %q, want feature inactive", result.Conflict)
		}
	})
	t.Run("active drain", func(t *testing.T) {
		machine := activeConnectionRebalanceMachine(t)
		drain := NodeDrainChangeDTO{
			Action: NodeDrainActionStart, MemberID: 1, DrainOperationID: releaseProfileOperation(3),
			IdempotencyKey: "drain-key", ServerReference: "lb.example:1883", ConnectionRatePerSecond: 5,
			LoadBalancerPropagationDelayMillis: 100, TimeoutMillis: 10_000, ProposedAtUnixMillis: 100,
		}
		applyMustSucceed(t, machine, 3, CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(3), NodeDrain: &drain})
		result := applyRebalanceTestCommand(t, machine, 4, startRebalanceTestChange("rb-drain-conflict", 200))
		if result.Conflict != ConflictNodeDrainInProgress {
			t.Fatalf("conflict = %q, want node drain in progress", result.Conflict)
		}
	})
	t.Run("already running", func(t *testing.T) {
		machine := activeConnectionRebalanceMachine(t)
		applyMustSucceed(t, machine, 3, connectionRebalanceCommand(3, startRebalanceTestChange("rb-running-1", 100)))
		result := applyRebalanceTestCommand(t, machine, 4, startRebalanceTestChange("rb-running-2", 200))
		if result.Conflict != ConflictConnectionRebalanceInProgress {
			t.Fatalf("conflict = %q, want rebalance in progress", result.Conflict)
		}
	})
	t.Run("membership operation in progress", func(t *testing.T) {
		machine := activeConnectionRebalanceMachine(t)
		record := stageRecord(releaseProfileOperation(3), "join-key", 4)
		applyMustSucceed(t, machine, 3, testCommand(record.OperationID, &record))
		result := applyRebalanceTestCommand(t, machine, 4, startRebalanceTestChange("rb-membership-conflict", 200))
		if result.Conflict != ConflictMembershipInProgress {
			t.Fatalf("conflict = %q, want membership in progress", result.Conflict)
		}
	})
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-224
func TestConnectionRebalanceReportsConvergeAndValidateOrdering(t *testing.T) {
	machine := activeConnectionRebalanceMachine(t)
	applyMustSucceed(t, machine, 3, connectionRebalanceCommand(3, startRebalanceTestChange("rb-report", 100)))
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		change := ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionReport, RebalanceOperationID: releaseProfileOperation(100), NodeID: nodeID, Connections: 2, ProposedAtUnixMillis: 200 + nodeID}
		result := applyRebalanceTestCommand(t, machine, 3+nodeID, change)
		if result.Outcome != ApplyOutcomeApplied {
			t.Fatalf("report node %d outcome = %+v", nodeID, result)
		}
	}
	state, ok := machine.ConnectionRebalance()
	if !ok || state.Phase != connrebalance.PhaseCompleted || state.FinishedAtUnixMillis != 203 {
		t.Fatalf("completed state = %+v, exists=%v", state, ok)
	}

	machine = activeConnectionRebalanceMachine(t)
	applyMustSucceed(t, machine, 3, connectionRebalanceCommand(3, startRebalanceTestChange("rb-order", 100)))
	applyRebalanceTestCommand(t, machine, 4, ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionReport, RebalanceOperationID: releaseProfileOperation(100), NodeID: 1, Connections: 1, ProposedAtUnixMillis: 200})
	outsider := ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionReport, RebalanceOperationID: releaseProfileOperation(100), NodeID: 4, ProposedAtUnixMillis: 201}
	if result := applyRebalanceTestCommand(t, machine, 5, outsider); result.Conflict != ConflictPhase {
		t.Fatalf("nonparticipant conflict = %q, want phase", result.Conflict)
	}
	stale := ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionReport, RebalanceOperationID: releaseProfileOperation(100), NodeID: 1, Connections: 1, ProposedAtUnixMillis: 199}
	if result := applyRebalanceTestCommand(t, machine, 6, stale); result.Conflict != ConflictPhase {
		t.Fatalf("stale report conflict = %q, want phase", result.Conflict)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-224
func TestConnectionRebalanceFailureCancellationAndDrainExclusion(t *testing.T) {
	t.Run("fail", func(t *testing.T) {
		machine := activeConnectionRebalanceMachine(t)
		applyMustSucceed(t, machine, 3, connectionRebalanceCommand(3, startRebalanceTestChange("rb-fail", 100)))
		change := ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionFail, RebalanceOperationID: releaseProfileOperation(100), FailureCode: connrebalance.FailureTimeout, ProposedAtUnixMillis: 500}
		result := applyRebalanceTestCommand(t, machine, 4, change)
		if result.ConnectionRebalance == nil || result.ConnectionRebalance.Phase != connrebalance.PhaseFailed || result.ConnectionRebalance.FailureCode != connrebalance.FailureTimeout {
			t.Fatalf("failed result = %+v", result)
		}
	})
	t.Run("cancel running", func(t *testing.T) {
		machine := activeConnectionRebalanceMachine(t)
		applyMustSucceed(t, machine, 3, connectionRebalanceCommand(3, startRebalanceTestChange("rb-cancel", 100)))
		change := ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionCancel, RebalanceOperationID: releaseProfileOperation(100), ProposedAtUnixMillis: 500}
		result := applyRebalanceTestCommand(t, machine, 4, change)
		if result.ConnectionRebalance == nil || result.ConnectionRebalance.Phase != connrebalance.PhaseCancelled {
			t.Fatalf("cancel result = %+v", result)
		}
		change.ProposedAtUnixMillis++
		result = applyRebalanceTestCommand(t, machine, 5, change)
		if result.Outcome != ApplyOutcomeApplied || result.ConnectionRebalance.Phase != connrebalance.PhaseCancelled {
			t.Fatalf("repeated cancel result = %+v", result)
		}
	})
	t.Run("completed cannot cancel", func(t *testing.T) {
		machine := activeConnectionRebalanceMachine(t)
		applyMustSucceed(t, machine, 3, connectionRebalanceCommand(3, startRebalanceTestChange("rb-complete", 100)))
		for nodeID := uint64(1); nodeID <= 3; nodeID++ {
			result := applyRebalanceTestCommand(t, machine, 3+nodeID, ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionReport, RebalanceOperationID: releaseProfileOperation(100), NodeID: nodeID, ProposedAtUnixMillis: 200 + nodeID})
			if result.Outcome != ApplyOutcomeApplied {
				t.Fatalf("report node %d failed to apply: %+v", nodeID, result)
			}
		}
		cancel := ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionCancel, RebalanceOperationID: releaseProfileOperation(100), ProposedAtUnixMillis: 300}
		if result := applyRebalanceTestCommand(t, machine, 7, cancel); result.Conflict != ConflictRebalanceNotCancellable {
			t.Fatalf("completed cancel conflict = %q, want not cancellable", result.Conflict)
		}
	})
	t.Run("drain blocked while running", func(t *testing.T) {
		machine := activeConnectionRebalanceMachine(t)
		applyMustSucceed(t, machine, 3, connectionRebalanceCommand(3, startRebalanceTestChange("rb-no-drain", 100)))
		drain := NodeDrainChangeDTO{Action: NodeDrainActionStart, MemberID: 1, DrainOperationID: releaseProfileOperation(4), IdempotencyKey: "drain-key", ServerReference: "lb.example:1883", ConnectionRatePerSecond: 5, LoadBalancerPropagationDelayMillis: 100, TimeoutMillis: 10_000, ProposedAtUnixMillis: 200}
		result, err := machine.Apply(4, CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(4), NodeDrain: &drain})
		if err != nil {
			t.Fatalf("apply drain command: %v", err)
		}
		if result.Conflict != ConflictConnectionRebalanceInProgress {
			t.Fatalf("drain conflict = %q, want rebalance in progress", result.Conflict)
		}
	})
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-224
func TestConnectionRebalanceSnapshotRoundTripAndFeatureRequirement(t *testing.T) {
	machine := activeConnectionRebalanceMachine(t)
	applyMustSucceed(t, machine, 3, connectionRebalanceCommand(3, startRebalanceTestChange("rb-snapshot", 100)))
	snapshot, err := machine.Snapshot(3)
	if err != nil {
		t.Fatal(err)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	got, ok := restored.ConnectionRebalance()
	if !ok || got.RebalanceOperationID != releaseProfileOperation(100) || len(got.Participants) != 3 {
		t.Fatalf("restored rebalance = %+v, exists=%v", got, ok)
	}
	got.Participants[0].Connections = 99
	if original, _ := restored.ConnectionRebalance(); original.Participants[0].Connections != 0 {
		t.Fatal("getter returned an aliased participant slice")
	}

	for index, feature := range snapshot.ActiveFeatures {
		if feature == ConnectionRebalanceFeature {
			snapshot.ActiveFeatures = append(snapshot.ActiveFeatures[:index], snapshot.ActiveFeatures[index+1:]...)
			snapshot.ObjectCount--
			break
		}
	}
	if err := NewStateMachine().Restore(snapshot); err != ErrInvalidSnapshot {
		t.Fatalf("restore without active rebalance feature error = %v, want ErrInvalidSnapshot", err)
	}
}

func activeConnectionRebalanceMachine(t *testing.T) *StateMachine {
	t.Helper()
	members := testVoters()
	for i := range members {
		members[i].Features = []string{ConnectionRebalanceFeature}
	}
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	result := activateFeatureForTest(t, machine, 2, ConnectionRebalanceFeature)
	if result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("activate connection rebalance outcome=%+v", result)
	}
	return machine
}

func startRebalanceTestChange(_ string, proposedAt uint64) ConnectionRebalanceChangeDTO {
	return ConnectionRebalanceChangeDTO{
		Action: ConnectionRebalanceActionStart, RebalanceOperationID: releaseProfileOperation(proposedAt),
		IdempotencyKey: "rebalance-key", ServerReference: "lb.example:1883",
		MaxDeviationConnections: 1, ConnectionRatePerSecond: 5,
		LoadBalancerPropagationDelayMillis: 100, TimeoutMillis: 10_000,
		ProposedAtUnixMillis: proposedAt,
	}
}

func connectionRebalanceCommand(index uint64, change ConnectionRebalanceChangeDTO) CommandDTO {
	return CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(index), ConnectionRebalance: &change}
}

func applyRebalanceTestCommand(t *testing.T, machine *StateMachine, index uint64, change ConnectionRebalanceChangeDTO) ApplyResultDTO {
	t.Helper()
	result, err := machine.Apply(index, connectionRebalanceCommand(index, change))
	if err != nil {
		t.Fatalf("apply rebalance command: %v", err)
	}
	return result
}

func TestConnectionRebalanceValidationBounds(t *testing.T) {
	valid := startRebalanceTestChange("ignored", 100)
	valid.MaxDeviationConnections = connrebalance.MinMaxDeviationConnections
	if err := validateConnectionRebalanceChange(valid); err != nil {
		t.Fatalf("minimum deviation rejected: %v", err)
	}
	valid.MaxDeviationConnections = connrebalance.MaxMaxDeviationConnections
	if err := validateConnectionRebalanceChange(valid); err != nil {
		t.Fatalf("maximum deviation rejected: %v", err)
	}
	for _, deviation := range []uint64{0, connrebalance.MaxMaxDeviationConnections + 1} {
		invalid := valid
		invalid.MaxDeviationConnections = deviation
		if err := validateConnectionRebalanceChange(invalid); err == nil {
			t.Errorf("deviation %d unexpectedly accepted", deviation)
		}
	}
	report := ConnectionRebalanceChangeDTO{
		Action: ConnectionRebalanceActionReport, RebalanceOperationID: releaseProfileOperation(100),
		NodeID: 1, Connections: connrebalance.MaxReportedConnections, ProposedAtUnixMillis: 200,
	}
	if err := validateConnectionRebalanceChange(report); err != nil {
		t.Fatalf("maximum reported connections rejected: %v", err)
	}
	report.Connections++
	if err := validateConnectionRebalanceChange(report); err == nil {
		t.Fatal("reported connection count above maximum unexpectedly accepted")
	}
}
