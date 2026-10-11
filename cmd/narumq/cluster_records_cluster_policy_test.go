package main

import (
	"reflect"
	"testing"

	"narumq/internal/application/membership"
)

func clusterPolicyBridgeFixture() membership.ClusterPolicyDTO {
	return membership.ClusterPolicyDTO{
		MQTTLimits: membership.MQTTLimitsDTO{
			MaxPacketSize: 1_048_576, ReceiveMaximum: 64,
			ConnectTimeoutMillis: 10_000, PacketReadTimeoutMillis: 30_000, WriteTimeoutMillis: 5_000,
		},
		TenantQuotas: &membership.ClusterTenantQuotasDTO{
			Default: membership.TenantStorageQuotaDTO{
				RetainedBytes: 1024, OfflineQueueMessages: 64, OfflineQueueBytes: 2048, StorageBytes: 4096,
			},
			Tenants: []membership.TenantStorageQuotaDTO{{
				TenantID: "tenant-a", RetainedBytes: 512, OfflineQueueMessages: 32,
				OfflineQueueBytes: 1024, StorageBytes: 2048,
			}},
		},
	}
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

// Traceability: CFG-SCN-34; docs/contracts/replicated-state-v1.md §3; ADR-0052 §1.1.1.
func TestClusterPolicyTenantQuotaCommandAndSnapshotRecordBridgeRoundTrip(t *testing.T) {
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
	decodedCommand, err := decodeMembershipCommand(wire)
	if err != nil {
		t.Fatalf("decodeMembershipCommand(): %v", err)
	}
	if !reflect.DeepEqual(decodedCommand, command) {
		t.Fatalf("command round trip = %+v, want %+v", decodedCommand, command)
	}

	want := membership.ClusterPolicyStateDTO{
		Version: 8, PreviousVersion: 7, Policy: clusterPolicyBridgeFixture(), IdempotencyKey: "policy-8",
	}
	snapshot := membership.SnapshotDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		LastIncludedIndex: 12,
		ActiveFeatures:    []string{membership.ClusterPolicyFeature, membership.ClusterPolicyTenantQuotasFeature},
		ClusterPolicy:     &want, ObjectCount: 5,
	}
	snapshotWire, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("encodeMembershipSnapshot(): %v", err)
	}
	decodedSnapshot, err := decodeMembershipSnapshot(snapshotWire)
	if err != nil {
		t.Fatalf("decodeMembershipSnapshot(): %v", err)
	}
	if decodedSnapshot.ClusterPolicy == nil || decodedSnapshot.ClusterPolicy.Version != want.Version ||
		decodedSnapshot.ClusterPolicy.PreviousVersion != want.PreviousVersion ||
		!membership.EqualClusterPolicy(decodedSnapshot.ClusterPolicy.Policy, want.Policy) ||
		decodedSnapshot.ClusterPolicy.IdempotencyKey != want.IdempotencyKey ||
		decodedSnapshot.ObjectCount != 5 || decodedSnapshot.LastIncludedIndex != snapshot.LastIncludedIndex {
		t.Fatalf("snapshot policy bridge = %+v, count=%d, index=%d; want %+v, 5, %d",
			decodedSnapshot.ClusterPolicy, decodedSnapshot.ObjectCount, decodedSnapshot.LastIncludedIndex, want, snapshot.LastIncludedIndex)
	}
}
