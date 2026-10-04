package records

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func validConnectionRebalanceStart() *ConnectionRebalanceChange {
	return &ConnectionRebalanceChange{
		Action:               ConnectionRebalanceAction_CONNECTION_REBALANCE_ACTION_START,
		RebalanceOperationId: op1, IdempotencyKey: "rb-1", ServerReference: "lb.example:1883",
		MaxDeviationConnections: 1, ConnectionRatePerSecond: 2,
		LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 2000, ProposedAtUnixMillis: 100,
	}
}

func validConnectionRebalanceState() *ConnectionRebalanceState {
	return &ConnectionRebalanceState{
		RebalanceOperationId: op1, IdempotencyKey: "rb-1",
		Phase:           ConnectionRebalancePhase_CONNECTION_REBALANCE_PHASE_RUNNING,
		ServerReference: "lb.example:1883", MaxDeviationConnections: 1,
		ConnectionRatePerSecond: 2, LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 2000,
		StartedAtUnixMillis: 100, DeadlineAtUnixMillis: 2100, CommittedIndex: 3,
		Participants: []*ConnectionRebalanceReport{{NodeId: 1}, {NodeId: 2}},
	}
}

// Traceability: CLU-13; docs/specs/cluster-session-and-delivery.md §9, §11.31.
func TestConnectionRebalanceCommandCodecValidation(t *testing.T) {
	valid := &CommandEnvelope{
		SchemaMajor: 1, OperationId: op1,
		Payload: &CommandEnvelope_ConnectionRebalance{ConnectionRebalance: validConnectionRebalanceStart()},
	}
	wire, err := MarshalCommand(valid)
	if err != nil {
		t.Fatalf("MarshalCommand(valid rebalance): %v", err)
	}
	decoded, err := UnmarshalCommand(wire)
	if err != nil || !proto.Equal(decoded, valid) {
		t.Fatalf("rebalance command round trip = %v, %v", decoded, err)
	}

	cases := map[string]func(*ConnectionRebalanceChange){
		"unspecified action":      func(change *ConnectionRebalanceChange) { change.Action = 0 },
		"unknown action":          func(change *ConnectionRebalanceChange) { change.Action = ConnectionRebalanceAction(99) },
		"invalid operation id":    func(change *ConnectionRebalanceChange) { change.RebalanceOperationId = "not-a-uuid" },
		"missing proposal time":   func(change *ConnectionRebalanceChange) { change.ProposedAtUnixMillis = 0 },
		"deviation too low":       func(change *ConnectionRebalanceChange) { change.MaxDeviationConnections = 0 },
		"deviation too high":      func(change *ConnectionRebalanceChange) { change.MaxDeviationConnections = 1_000_001 },
		"start report field":      func(change *ConnectionRebalanceChange) { change.Connections = 1 },
		"start failure field":     func(change *ConnectionRebalanceChange) { change.FailureCode = "failed" },
		"start deadline overflow": func(change *ConnectionRebalanceChange) { change.ProposedAtUnixMillis = ^uint64(0) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			command := proto.Clone(valid).(*CommandEnvelope)
			mutate(command.GetConnectionRebalance())
			if _, err := MarshalCommand(command); err == nil {
				t.Fatal("MarshalCommand accepted invalid connection rebalance start")
			}
		})
	}

	invalidActions := []*ConnectionRebalanceChange{
		{Action: ConnectionRebalanceAction_CONNECTION_REBALANCE_ACTION_REPORT, RebalanceOperationId: op1, NodeId: 1, Connections: maxRebalanceConnections + 1, ProposedAtUnixMillis: 1},
		{Action: ConnectionRebalanceAction_CONNECTION_REBALANCE_ACTION_REPORT, RebalanceOperationId: op1, NodeId: 1, IdempotencyKey: "unexpected", ProposedAtUnixMillis: 1},
		{Action: ConnectionRebalanceAction_CONNECTION_REBALANCE_ACTION_FAIL, RebalanceOperationId: op1, FailureCode: "Bad-code", ProposedAtUnixMillis: 1},
		{Action: ConnectionRebalanceAction_CONNECTION_REBALANCE_ACTION_FAIL, RebalanceOperationId: op1, FailureCode: "failure", Connections: 1, ProposedAtUnixMillis: 1},
		{Action: ConnectionRebalanceAction_CONNECTION_REBALANCE_ACTION_CANCEL, RebalanceOperationId: op1, FailureCode: "failure", ProposedAtUnixMillis: 1},
	}
	for _, change := range invalidActions {
		if _, err := MarshalCommand(&CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_ConnectionRebalance{ConnectionRebalance: change}}); err == nil {
			t.Fatalf("MarshalCommand accepted invalid action payload: %v", change)
		}
	}
}

// Traceability: CLU-13; docs/specs/cluster-session-and-delivery.md §9, §11.31.
func TestConnectionRebalanceSnapshotFeatureGateAndObjectCount(t *testing.T) {
	state := &ReplicatedStateSnapshot{
		ConnectionRebalance: validConnectionRebalanceState(),
		ActiveFeatures:      []string{"connection-rebalance"},
	}
	wire, err := MarshalSnapshot(state, 4)
	if err != nil {
		t.Fatalf("MarshalSnapshot(rebalance): %v", err)
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(rebalance): %v", err)
	}
	if envelope.ObjectCount != 2 || envelope.State.ConnectionRebalance == nil {
		t.Fatalf("snapshot object count/state = %d / %v, want 2 / present", envelope.ObjectCount, envelope.State.ConnectionRebalance)
	}

	withoutFeature := proto.Clone(state).(*ReplicatedStateSnapshot)
	withoutFeature.ActiveFeatures = nil
	if _, err := MarshalSnapshot(withoutFeature, 4); err == nil {
		t.Fatal("MarshalSnapshot accepted rebalance state without active feature")
	}

	invalid := proto.Clone(state).(*ReplicatedStateSnapshot)
	invalid.ConnectionRebalance.Participants[1].NodeId = 1
	if _, err := MarshalSnapshot(invalid, 4); err == nil {
		t.Fatal("MarshalSnapshot accepted unsorted/duplicate participants")
	}
}
