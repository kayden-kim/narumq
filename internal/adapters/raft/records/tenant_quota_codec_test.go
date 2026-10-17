package records

import (
	"bytes"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
)

func tenantQuotaProfileRecordForTest(version uint64) *TenantQuotaProfile {
	return &TenantQuotaProfile{
		Version: version,
		DefaultQuota: &TenantStorageQuota{
			RetainedBytes: maxTenantQuotaRetainedBytes, OfflineQueueMessages: maxTenantQuotaOfflineQueueMessages,
			OfflineQueueBytes: maxTenantQuotaOfflineQueueBytes, StorageBytes: maxTenantQuotaStorageBytes,
		},
		Tenants: []*TenantStorageQuota{
			{TenantId: "tenant-a", RetainedBytes: 1, OfflineQueueMessages: 1, OfflineQueueBytes: 1, StorageBytes: 1},
			{TenantId: "tenant-b", RetainedBytes: 2, OfflineQueueMessages: 2, OfflineQueueBytes: 2, StorageBytes: 2},
		},
	}
}

// Traceability: TEN-08; docs/specs/cluster-session-and-delivery.md §11.16.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-099
func TestTenantQuotaCommandCodecRoundTripsAndRejectsInvalidProfiles(t *testing.T) {
	command := &CommandEnvelope{
		SchemaMajor: 1, OperationId: op1,
		Payload: &CommandEnvelope_TenantQuota{TenantQuota: &TenantQuotaChange{Profile: tenantQuotaProfileRecordForTest(7)}},
	}
	wire, err := MarshalCommand(command)
	if err != nil {
		t.Fatalf("MarshalCommand(): %v", err)
	}
	decoded, err := UnmarshalCommand(wire)
	if err != nil || !proto.Equal(decoded, command) {
		t.Fatalf("UnmarshalCommand() = (%v, %v), want command round trip", decoded, err)
	}
	reencoded, err := MarshalCommand(decoded)
	if err != nil || !bytes.Equal(reencoded, wire) {
		t.Fatalf("canonical command re-encode error = %v", err)
	}

	invalidProfiles := []struct {
		name    string
		profile *TenantQuotaProfile
	}{
		{name: "nil profile", profile: nil},
		{name: "nil default quota", profile: func() *TenantQuotaProfile { p := tenantQuotaProfileRecordForTest(1); p.DefaultQuota = nil; return p }()},
		{name: "default tenant id", profile: func() *TenantQuotaProfile {
			p := tenantQuotaProfileRecordForTest(1)
			p.DefaultQuota.TenantId = "tenant-a"
			return p
		}()},
		{name: "zero quota", profile: func() *TenantQuotaProfile {
			p := tenantQuotaProfileRecordForTest(1)
			p.DefaultQuota.RetainedBytes = 0
			return p
		}()},
		{name: "quota above capacity", profile: func() *TenantQuotaProfile {
			p := tenantQuotaProfileRecordForTest(1)
			p.Tenants[0].StorageBytes = maxTenantQuotaStorageBytes + 1
			return p
		}()},
		{name: "invalid tenant id", profile: func() *TenantQuotaProfile {
			p := tenantQuotaProfileRecordForTest(1)
			p.Tenants[0].TenantId = "tenant/a"
			return p
		}()},
		{name: "unsorted tenant ids", profile: func() *TenantQuotaProfile {
			p := tenantQuotaProfileRecordForTest(1)
			p.Tenants[0], p.Tenants[1] = p.Tenants[1], p.Tenants[0]
			return p
		}()},
		{name: "duplicate tenant id", profile: func() *TenantQuotaProfile {
			p := tenantQuotaProfileRecordForTest(1)
			p.Tenants[1].TenantId = p.Tenants[0].TenantId
			return p
		}()},
		{name: "nil tenant entry", profile: func() *TenantQuotaProfile { p := tenantQuotaProfileRecordForTest(1); p.Tenants[0] = nil; return p }()},
		{name: "too many tenant entries", profile: func() *TenantQuotaProfile {
			p := tenantQuotaProfileRecordForTest(1)
			p.Tenants = make([]*TenantStorageQuota, maxTenantQuotaTenants+1)
			for index := range p.Tenants {
				p.Tenants[index] = &TenantStorageQuota{TenantId: fmt.Sprintf("tenant-%04d", index), RetainedBytes: 1, OfflineQueueMessages: 1, OfflineQueueBytes: 1, StorageBytes: 1}
			}
			return p
		}()},
	}
	for _, test := range invalidProfiles {
		t.Run(test.name, func(t *testing.T) {
			invalid := &CommandEnvelope{
				SchemaMajor: 1, OperationId: op1,
				Payload: &CommandEnvelope_TenantQuota{TenantQuota: &TenantQuotaChange{Profile: test.profile}},
			}
			if _, err := MarshalCommand(invalid); err == nil {
				t.Fatal("MarshalCommand() accepted invalid quota profile")
			}
			encoded, err := proto.Marshal(invalid)
			if err != nil {
				t.Fatalf("proto.Marshal(invalid): %v", err)
			}
			if _, err := UnmarshalCommand(encoded); err == nil {
				t.Fatal("UnmarshalCommand() accepted invalid quota profile")
			}
		})
	}
}

// Traceability: TEN-08; docs/specs/cluster-session-and-delivery.md §10 and §11.16.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-099
func TestTenantQuotaSnapshotCodecCountsProfileAndRejectsInvalidState(t *testing.T) {
	state := &ReplicatedStateSnapshot{TenantQuotaProfile: tenantQuotaProfileRecordForTest(8)}
	before := proto.Clone(state)
	wire, err := MarshalSnapshot(state, 12)
	if err != nil {
		t.Fatalf("MarshalSnapshot(): %v", err)
	}
	if !proto.Equal(state, before) {
		t.Fatal("MarshalSnapshot() mutated the source quota profile")
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(): %v", err)
	}
	if envelope.ObjectCount != 3 || !proto.Equal(envelope.State.TenantQuotaProfile, state.TenantQuotaProfile) {
		t.Fatalf("quota snapshot = count %d profile %v, want count 3 and profile round trip", envelope.ObjectCount, envelope.State.TenantQuotaProfile)
	}
	reencoded, err := MarshalSnapshot(envelope.State, 12)
	if err != nil || !bytes.Equal(reencoded, wire) {
		t.Fatalf("canonical quota snapshot re-encode error = %v", err)
	}

	wrongCount := proto.Clone(envelope).(*SnapshotEnvelope)
	wrongCount.ObjectCount--
	wrongCountWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(wrongCount)
	if err != nil {
		t.Fatalf("marshal wrong object count: %v", err)
	}
	if _, err := UnmarshalSnapshot(wrongCountWire); err == nil {
		t.Fatal("UnmarshalSnapshot() accepted a quota-aware object-count mismatch")
	}

	unsorted := proto.Clone(state).(*ReplicatedStateSnapshot)
	unsorted.TenantQuotaProfile.Tenants[0], unsorted.TenantQuotaProfile.Tenants[1] = unsorted.TenantQuotaProfile.Tenants[1], unsorted.TenantQuotaProfile.Tenants[0]
	if _, err := MarshalSnapshot(unsorted, 12); err == nil {
		t.Fatal("MarshalSnapshot() accepted an unsorted tenant quota profile")
	}
	invalidEnvelope := &SnapshotEnvelope{
		SchemaMajor: 1, LastIncludedIndex: 12,
		ObjectCount: uint64(tenantQuotaProfileObjectCount(unsorted.TenantQuotaProfile)),
		Sha256:      make([]byte, 32), State: unsorted,
	}
	invalidWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(invalidEnvelope)
	if err != nil {
		t.Fatalf("marshal invalid snapshot profile: %v", err)
	}
	if _, err := UnmarshalSnapshot(invalidWire); err == nil {
		t.Fatal("UnmarshalSnapshot() accepted an unsorted tenant quota profile")
	}
}
