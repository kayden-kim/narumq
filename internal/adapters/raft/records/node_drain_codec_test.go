package records

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"google.golang.org/protobuf/proto"
)

func validDrainChange(action NodeDrainAction) *NodeDrainChange {
	return &NodeDrainChange{
		Action: action, MemberId: 1, DrainOperationId: op1,
		IdempotencyKey: "drain-1", ConnectionRatePerSecond: 2,
		LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 2000,
		ProposedAtUnixMillis: 100,
	}
}

func validDrainState(memberID uint64, phase NodeDrainPhase) *NodeDrainState {
	return &NodeDrainState{
		DrainOperationId: op1, MemberId: memberID, IdempotencyKey: "drain-1", Phase: phase,
		ConnectionRatePerSecond: 2, LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 2000,
		StartedAtUnixMillis: 100, DeadlineAtUnixMillis: 2100, PhaseStartedAtUnixMillis: 100,
		CommittedIndex: 3,
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §1, §3, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-009
func TestNodeDrainCommandCodecValidation(t *testing.T) {
	valid := &CommandEnvelope{
		SchemaMajor: 1, OperationId: op1,
		Payload: &CommandEnvelope_NodeDrain{NodeDrain: validDrainChange(NodeDrainAction_NODE_DRAIN_ACTION_START)},
	}
	wire, err := MarshalCommand(valid)
	if err != nil {
		t.Fatalf("MarshalCommand(valid drain): %v", err)
	}
	decoded, err := UnmarshalCommand(wire)
	if err != nil || !proto.Equal(decoded, valid) {
		t.Fatalf("drain command round trip = %v, %v", decoded, err)
	}

	cases := map[string]func(*NodeDrainChange){
		"invalid action":           func(change *NodeDrainChange) { change.Action = NodeDrainAction(99) },
		"invalid UUID":             func(change *NodeDrainChange) { change.DrainOperationId = "not-a-uuid" },
		"missing idempotency":      func(change *NodeDrainChange) { change.IdempotencyKey = "" },
		"rate out of bounds":       func(change *NodeDrainChange) { change.ConnectionRatePerSecond = 0 },
		"server reference control": func(change *NodeDrainChange) { change.ServerReference = "host\n" },
		"start extra progress":     func(change *NodeDrainChange) { change.Progress = &NodeDrainProgress{SessionsFailed: 1} },
		"advance extra parameters": func(change *NodeDrainChange) {
			change.Action = NodeDrainAction_NODE_DRAIN_ACTION_ADVANCE
			change.Phase = NodeDrainPhase_NODE_DRAIN_PHASE_WAITING_FOR_LB
		},
		"failure bad token": func(change *NodeDrainChange) {
			change.Action = NodeDrainAction_NODE_DRAIN_ACTION_FAIL
			change.IdempotencyKey = ""
			change.ConnectionRatePerSecond = 0
			change.LoadBalancerPropagationDelayMillis = 0
			change.TimeoutMillis = 0
			change.FailureCode = "Bad-code"
		},
		"cancel progress": func(change *NodeDrainChange) {
			change.Action = NodeDrainAction_NODE_DRAIN_ACTION_CANCEL
			change.IdempotencyKey = ""
			change.ConnectionRatePerSecond = 0
			change.LoadBalancerPropagationDelayMillis = 0
			change.TimeoutMillis = 0
			change.Progress = &NodeDrainProgress{ConnectionsRemaining: 1}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			command := proto.Clone(valid).(*CommandEnvelope)
			mutate(command.GetNodeDrain())
			if _, err := MarshalCommand(command); err == nil {
				t.Fatal("MarshalCommand accepted invalid node drain change")
			}
		})
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §3, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-009
func TestNodeDrainSnapshotRoundTripSortAndObjectCount(t *testing.T) {
	state := &ReplicatedStateSnapshot{
		Members: []*MemberIdentity{crrVoter(2), crrVoter(1)},
		NodeDrains: []*NodeDrainState{
			validDrainState(2, NodeDrainPhase_NODE_DRAIN_PHASE_CANCELLED),
			validDrainState(1, NodeDrainPhase_NODE_DRAIN_PHASE_CANCELLED),
		},
	}
	wire, err := MarshalSnapshot(state, 4)
	if err != nil {
		t.Fatalf("MarshalSnapshot(drain): %v", err)
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(drain): %v", err)
	}
	if envelope.ObjectCount != 4 || len(envelope.State.NodeDrains) != 2 {
		t.Fatalf("snapshot object count / drains = %d / %d, want 4 / 2", envelope.ObjectCount, len(envelope.State.NodeDrains))
	}
	if envelope.State.NodeDrains[0].MemberId != 1 || envelope.State.NodeDrains[1].MemberId != 2 {
		t.Fatalf("node drains are not sorted by member ID: %d, %d", envelope.State.NodeDrains[0].MemberId, envelope.State.NodeDrains[1].MemberId)
	}
	reencoded, err := MarshalSnapshot(envelope.State, envelope.LastIncludedIndex)
	if err != nil || !bytes.Equal(reencoded, wire) {
		t.Fatalf("canonical drain snapshot round trip: err=%v", err)
	}
	if state.NodeDrains[0].MemberId != 2 || state.NodeDrains[1].MemberId != 1 {
		t.Fatal("MarshalSnapshot mutated caller-owned node drain ordering")
	}

	badCount := proto.Clone(envelope).(*SnapshotEnvelope)
	badCount.ObjectCount++
	badCountWire, err := deterministic.Marshal(badCount)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnmarshalSnapshot(badCountWire); err == nil {
		t.Fatal("UnmarshalSnapshot accepted incorrect count including node drains")
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §3, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-009
func TestNodeDrainSnapshotRejectsOrderingDuplicatesAndInvalidState(t *testing.T) {
	base := &ReplicatedStateSnapshot{
		Members: []*MemberIdentity{crrVoter(1), crrVoter(2)},
		NodeDrains: []*NodeDrainState{
			validDrainState(1, NodeDrainPhase_NODE_DRAIN_PHASE_CANCELLED),
			validDrainState(2, NodeDrainPhase_NODE_DRAIN_PHASE_CANCELLED),
		},
	}
	wire, err := MarshalSnapshot(base, 4)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatal(err)
	}

	unsorted := proto.Clone(decoded).(*SnapshotEnvelope)
	unsorted.State.NodeDrains[0], unsorted.State.NodeDrains[1] = unsorted.State.NodeDrains[1], unsorted.State.NodeDrains[0]
	stateBytes, err := deterministic.Marshal(unsorted.State)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(stateBytes)
	unsorted.Sha256 = digest[:]
	unsortedWire, err := deterministic.Marshal(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnmarshalSnapshot(unsortedWire); err == nil {
		t.Fatal("UnmarshalSnapshot accepted unsorted node drains")
	}

	duplicate := proto.Clone(base).(*ReplicatedStateSnapshot)
	duplicate.NodeDrains[1].MemberId = 1
	if _, err := MarshalSnapshot(duplicate, 4); err == nil {
		t.Fatal("MarshalSnapshot accepted duplicate node drain member IDs")
	}

	invalid := proto.Clone(base).(*ReplicatedStateSnapshot)
	invalid.NodeDrains[0].DeadlineAtUnixMillis++
	if _, err := MarshalSnapshot(invalid, 4); err == nil {
		t.Fatal("MarshalSnapshot accepted inconsistent drain deadline")
	}
}
