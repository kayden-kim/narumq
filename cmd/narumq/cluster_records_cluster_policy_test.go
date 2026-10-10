package main

import (
	"reflect"
	"testing"

	"narumq/internal/application/membership"
)

func clusterPolicyBridgeFixture() membership.ClusterPolicyDTO {
	return membership.ClusterPolicyDTO{MQTTLimits: membership.MQTTLimitsDTO{
		MaxPacketSize: 1_048_576, ReceiveMaximum: 64,
		ConnectTimeoutMillis: 10_000, PacketReadTimeoutMillis: 30_000, WriteTimeoutMillis: 5_000,
	}}
}

// Traceability: API-09; docs/contracts/replicated-state-v1.md §3; ADR-0052 §1.1.1.
func TestClusterPolicyCommandRecordBridgeRoundTrip(t *testing.T) {
	command := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
		ClusterPolicy: &membership.ClusterPolicyChangeDTO{
			ExpectedVersion: 7, Version: 8, Policy: clusterPolicyBridgeFixture(), IdempotencyKey: "policy-8",
		},
	}
	wire, err := encodeMembershipCommand(command)
	if err != nil {
		t.Fatalf("encodeMembershipCommand(): %v", err)
	}
	decoded, err := decodeMembershipCommand(wire)
	if err != nil {
		t.Fatalf("decodeMembershipCommand(): %v", err)
	}
	if !reflect.DeepEqual(decoded, command) {
		t.Fatalf("command round trip = %+v, want %+v", decoded, command)
	}
}

// Traceability: API-09; docs/contracts/replicated-state-v1.md §3; ADR-0052 §1.1.1.
func TestClusterPolicySnapshotRecordBridgeRoundTripAndObjectCount(t *testing.T) {
	want := membership.ClusterPolicyStateDTO{
		Version: 8, PreviousVersion: 7, Policy: clusterPolicyBridgeFixture(), IdempotencyKey: "policy-8",
	}
	snapshot := membership.SnapshotDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		LastIncludedIndex: 12, ActiveFeatures: []string{membership.ClusterPolicyFeature},
		ClusterPolicy: &want, ObjectCount: 2,
	}
	wire, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("encodeMembershipSnapshot(): %v", err)
	}
	decoded, err := decodeMembershipSnapshot(wire)
	if err != nil {
		t.Fatalf("decodeMembershipSnapshot(): %v", err)
	}
	if decoded.ClusterPolicy == nil || *decoded.ClusterPolicy != want || decoded.ObjectCount != 2 || decoded.LastIncludedIndex != snapshot.LastIncludedIndex {
		t.Fatalf("snapshot policy bridge = %+v, count=%d, index=%d; want %+v, 2, %d", decoded.ClusterPolicy, decoded.ObjectCount, decoded.LastIncludedIndex, want, snapshot.LastIncludedIndex)
	}
}
