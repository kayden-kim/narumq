package main

import (
	"bytes"
	"reflect"
	"testing"

	"narumq/internal/application/membership"
	"narumq/internal/domain/connrebalance"
)

// Traceability: CLU-13; docs/specs/cluster-session-and-delivery.md §9, §11.31.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-224
func TestClusterConnectionRebalanceRecordBridgeRoundTrips(t *testing.T) {
	commands := []membership.CommandDTO{
		{
			SchemaMajor: 1, OperationID: "00000000-0000-4000-8000-000000000001",
			ConnectionRebalance: &membership.ConnectionRebalanceChangeDTO{
				Action:               membership.ConnectionRebalanceActionStart,
				RebalanceOperationID: "00000000-0000-4000-8000-000000000001", IdempotencyKey: "rb-1",
				ServerReference: "lb.example:1883", MaxDeviationConnections: 1, ConnectionRatePerSecond: 2,
				LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 2000, ProposedAtUnixMillis: 100,
			},
		},
		{
			SchemaMajor: 1, OperationID: "00000000-0000-4000-8000-000000000002",
			ConnectionRebalance: &membership.ConnectionRebalanceChangeDTO{
				Action:               membership.ConnectionRebalanceActionReport,
				RebalanceOperationID: "00000000-0000-4000-8000-000000000001",
				NodeID:               1, Connections: 12, SessionsTransferred: 3, Shedding: true, ProposedAtUnixMillis: 200,
			},
		},
	}
	for _, command := range commands {
		encoded, err := encodeMembershipCommand(command)
		if err != nil {
			t.Fatalf("encode command: %v", err)
		}
		decoded, err := decodeMembershipCommand(encoded)
		if err != nil {
			t.Fatalf("decode command: %v", err)
		}
		if !reflect.DeepEqual(decoded, command) {
			t.Fatalf("command round trip = %#v, want %#v", decoded, command)
		}
		reencoded, err := encodeMembershipCommand(decoded)
		if err != nil || !bytes.Equal(reencoded, encoded) {
			t.Fatalf("command re-encode error = %v", err)
		}
	}

	state := membership.ConnectionRebalanceStateDTO{
		RebalanceOperationID: "00000000-0000-4000-8000-000000000001", IdempotencyKey: "rb-1",
		Phase: connrebalance.PhaseRunning, ServerReference: "lb.example:1883",
		MaxDeviationConnections: 1, ConnectionRatePerSecond: 2,
		LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 2000,
		StartedAtUnixMillis: 100, DeadlineAtUnixMillis: 2100,
		Participants: []membership.ConnectionRebalanceReportDTO{
			{NodeID: 1, Connections: 12, SessionsTransferred: 3, Shedding: true, ReportedAtUnixMillis: 200},
			{NodeID: 2, Connections: 7, ReportedAtUnixMillis: 200},
		},
		CommittedIndex: 3,
	}
	snapshot := membership.SnapshotDTO{
		SchemaMajor: membership.SchemaMajor, LastIncludedIndex: 4, ObjectCount: 2,
		ActiveFeatures: []string{membership.ConnectionRebalanceFeature}, ConnectionRebalance: &state,
	}
	encoded, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	decoded, err := decodeMembershipSnapshot(encoded)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if decoded.ConnectionRebalance == nil || !reflect.DeepEqual(*decoded.ConnectionRebalance, state) {
		t.Fatalf("snapshot rebalance round trip = %#v, want %#v", decoded.ConnectionRebalance, state)
	}
	if decoded.ObjectCount != snapshot.ObjectCount {
		t.Fatalf("snapshot object count = %d, want %d", decoded.ObjectCount, snapshot.ObjectCount)
	}
	reencoded, err := encodeMembershipSnapshot(decoded)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("snapshot re-encode error = %v", err)
	}
}
