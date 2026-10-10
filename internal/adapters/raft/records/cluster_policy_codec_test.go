package records

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
)

func clusterPolicyTestRecordChange(expectedVersion, version uint64, key string) *ClusterPolicyChange {
	return &ClusterPolicyChange{
		ExpectedVersion: expectedVersion, Version: version, IdempotencyKey: key,
		Policy: &ClusterPolicy{MqttLimits: &MqttLimits{
			MaxPacketSize: 1_048_576, ReceiveMaximum: 64,
			ConnectTimeoutMillis: 10_000, PacketReadTimeoutMillis: 30_000, WriteTimeoutMillis: 5_000,
		}},
	}
}

func clusterPolicyTestCommand(change *ClusterPolicyChange) *CommandEnvelope {
	return &CommandEnvelope{
		SchemaMajor: 1, OperationId: op1,
		Payload: &CommandEnvelope_ClusterPolicy{ClusterPolicy: change},
	}
}

// Traceability: API-09; docs/contracts/replicated-state-v1.md §3; ADR-0052 §1.1.1.
func TestClusterPolicyCommandCodecRoundTripAndBoundaries(t *testing.T) {
	valid := clusterPolicyTestCommand(clusterPolicyTestRecordChange(0, 1, "policy-1"))
	wire, err := MarshalCommand(valid)
	if err != nil {
		t.Fatalf("MarshalCommand(valid): %v", err)
	}
	decoded, err := UnmarshalCommand(wire)
	if err != nil || !proto.Equal(decoded, valid) {
		t.Fatalf("command round trip = %v, %v", decoded, err)
	}
	reencoded, err := MarshalCommand(decoded)
	if err != nil || !bytes.Equal(reencoded, wire) {
		t.Fatalf("command re-encode differs: %v", err)
	}

	maxVersion := ^uint64(0)
	boundary := clusterPolicyTestCommand(clusterPolicyTestRecordChange(maxVersion-1, maxVersion, "!"))
	limits := boundary.Payload.(*CommandEnvelope_ClusterPolicy).ClusterPolicy.Policy.MqttLimits
	limits.MaxPacketSize = maxClusterPolicyPacketSize
	limits.ReceiveMaximum = maxClusterPolicyReceiveMaximum
	limits.ConnectTimeoutMillis = ^uint32(0)
	limits.PacketReadTimeoutMillis = ^uint32(0)
	limits.WriteTimeoutMillis = ^uint32(0)
	if _, err := MarshalCommand(boundary); err != nil {
		t.Fatalf("MarshalCommand(maximum valid limits): %v", err)
	}

	invalid := []struct {
		name   string
		mutate func(*ClusterPolicyChange)
	}{
		{"nil change", nil},
		{"missing policy", func(change *ClusterPolicyChange) { change.Policy = nil }},
		{"missing limits", func(change *ClusterPolicyChange) { change.Policy.MqttLimits = nil }},
		{"zero version", func(change *ClusterPolicyChange) { change.Version = 0 }},
		{"version not increasing", func(change *ClusterPolicyChange) { change.ExpectedVersion = 1; change.Version = 1 }},
		{"empty idempotency key", func(change *ClusterPolicyChange) { change.IdempotencyKey = "" }},
		{"space in idempotency key", func(change *ClusterPolicyChange) { change.IdempotencyKey = "bad key" }},
		{"packet size zero", func(change *ClusterPolicyChange) { change.Policy.MqttLimits.MaxPacketSize = 0 }},
		{"packet size too large", func(change *ClusterPolicyChange) {
			change.Policy.MqttLimits.MaxPacketSize = maxClusterPolicyPacketSize + 1
		}},
		{"receive maximum zero", func(change *ClusterPolicyChange) { change.Policy.MqttLimits.ReceiveMaximum = 0 }},
		{"receive maximum too large", func(change *ClusterPolicyChange) {
			change.Policy.MqttLimits.ReceiveMaximum = maxClusterPolicyReceiveMaximum + 1
		}},
		{"zero connect timeout", func(change *ClusterPolicyChange) { change.Policy.MqttLimits.ConnectTimeoutMillis = 0 }},
		{"zero packet read timeout", func(change *ClusterPolicyChange) { change.Policy.MqttLimits.PacketReadTimeoutMillis = 0 }},
		{"zero write timeout", func(change *ClusterPolicyChange) { change.Policy.MqttLimits.WriteTimeoutMillis = 0 }},
	}
	for _, testCase := range invalid {
		t.Run(testCase.name, func(t *testing.T) {
			change := proto.Clone(valid.Payload.(*CommandEnvelope_ClusterPolicy).ClusterPolicy).(*ClusterPolicyChange)
			if testCase.mutate == nil {
				change = nil
			} else {
				testCase.mutate(change)
			}
			if _, err := MarshalCommand(clusterPolicyTestCommand(change)); err == nil {
				t.Fatal("MarshalCommand() accepted invalid cluster policy command")
			}
		})
	}
}

// Traceability: API-09; docs/contracts/replicated-state-v1.md §3; ADR-0052 §1.1.1.
func TestClusterPolicySnapshotFeatureValidationAndObjectCount(t *testing.T) {
	state := &ReplicatedStateSnapshot{
		ActiveFeatures: []string{"cluster-policy"},
		ClusterPolicy: &ClusterPolicyState{
			Version: 1, PreviousVersion: 0, IdempotencyKey: "policy-1",
			Policy: clusterPolicyTestRecordChange(0, 1, "policy-1").Policy,
		},
	}
	wire, err := MarshalSnapshot(state, 3)
	if err != nil {
		t.Fatalf("MarshalSnapshot(valid policy): %v", err)
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil || envelope.ObjectCount != 2 || !proto.Equal(envelope.State.ClusterPolicy, state.ClusterPolicy) {
		t.Fatalf("snapshot round trip = count %d, state %v, error %v", envelope.GetObjectCount(), envelope.GetState().GetClusterPolicy(), err)
	}

	withoutFeature := proto.Clone(state).(*ReplicatedStateSnapshot)
	withoutFeature.ActiveFeatures = nil
	if _, err := MarshalSnapshot(withoutFeature, 3); err == nil {
		t.Fatal("MarshalSnapshot() accepted policy without active feature")
	}

	badCount := proto.Clone(envelope).(*SnapshotEnvelope)
	badCount.ObjectCount--
	encoded, err := deterministic.Marshal(badCount)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnmarshalSnapshot(encoded); err == nil {
		t.Fatal("UnmarshalSnapshot() accepted incorrect policy object count")
	}

	invalid := []struct {
		name   string
		mutate func(*ClusterPolicyState)
	}{
		{"zero version", func(state *ClusterPolicyState) { state.Version = 0 }},
		{"previous version is not lower", func(state *ClusterPolicyState) { state.PreviousVersion = state.Version }},
		{"empty idempotency key", func(state *ClusterPolicyState) { state.IdempotencyKey = "" }},
		{"missing policy", func(state *ClusterPolicyState) { state.Policy = nil }},
		{"packet size too large", func(state *ClusterPolicyState) {
			state.Policy.MqttLimits.MaxPacketSize = maxClusterPolicyPacketSize + 1
		}},
	}
	for _, testCase := range invalid {
		t.Run(testCase.name, func(t *testing.T) {
			invalidState := proto.Clone(state).(*ReplicatedStateSnapshot)
			testCase.mutate(invalidState.ClusterPolicy)
			if _, err := MarshalSnapshot(invalidState, 3); err == nil {
				t.Fatal("MarshalSnapshot() accepted invalid cluster policy state")
			}
		})
	}
}
