package membership

import (
	"errors"
	"strings"
	"testing"
)

func clusterPolicyTestValue(receiveMaximum uint32) ClusterPolicyDTO {
	return ClusterPolicyDTO{MQTTLimits: MQTTLimitsDTO{
		MaxPacketSize: 1024, ReceiveMaximum: receiveMaximum,
		ConnectTimeoutMillis: 10_000, PacketReadTimeoutMillis: 30_000, WriteTimeoutMillis: 5_000,
	}}
}

func clusterPolicyTestCommand(index, expectedVersion, version uint64, policy ClusterPolicyDTO, key string) CommandDTO {
	return CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(index),
		ClusterPolicy: &ClusterPolicyChangeDTO{
			ExpectedVersion: expectedVersion, Version: version, Policy: policy, IdempotencyKey: key,
		},
	}
}

func clusterPolicyTestMachine(t *testing.T) *StateMachine {
	t.Helper()
	machine := releaseProfileMachine(t, "1.2.0", []string{ClusterPolicyFeature})
	if result := activateFeatureForTest(t, machine, 2, ClusterPolicyFeature); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("activate ClusterPolicy feature = %+v", result)
	}
	return machine
}

// Traceability: API-09; docs/contracts/replicated-state-v1.md §1-3; ADR-0052 §1.1.1.
func TestClusterPolicyApplyFeatureCASAndLatestKeyReplay(t *testing.T) {
	machine := releaseProfileMachine(t, "1.2.0", []string{ClusterPolicyFeature})
	policy := clusterPolicyTestValue(64)
	inactive, err := machine.Apply(2, clusterPolicyTestCommand(2, 0, 1, policy, "policy-1"))
	if err != nil || inactive.Outcome != ApplyOutcomeConflict || inactive.Conflict != ConflictFeatureInactive {
		t.Fatalf("inactive policy apply = %+v, %v; want feature_inactive", inactive, err)
	}
	if result := activateFeatureForTest(t, machine, 3, ClusterPolicyFeature); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("activate feature = %+v", result)
	}

	first, err := machine.Apply(4, clusterPolicyTestCommand(4, 0, 1, policy, "policy-1"))
	if err != nil || first.Outcome != ApplyOutcomeApplied || first.Replayed {
		t.Fatalf("initial policy apply = %+v, %v", first, err)
	}
	replay, err := machine.Apply(5, clusterPolicyTestCommand(5, 0, 1, policy, "policy-1"))
	if err != nil || replay.Outcome != ApplyOutcomeApplied {
		t.Fatalf("latest-key replay = %+v, %v", replay, err)
	}
	changed := clusterPolicyTestValue(32)
	idempotencyConflict, err := machine.Apply(6, clusterPolicyTestCommand(6, 0, 1, changed, "policy-1"))
	if err != nil || idempotencyConflict.Outcome != ApplyOutcomeConflict || idempotencyConflict.Conflict != ConflictIdempotency {
		t.Fatalf("same key with changed policy = %+v, %v; want idempotency_conflict", idempotencyConflict, err)
	}
	stale, err := machine.Apply(7, clusterPolicyTestCommand(7, 0, 1, changed, "policy-old"))
	if err != nil || stale.Outcome != ApplyOutcomeConflict || stale.Conflict != ConflictClusterPolicyVersion {
		t.Fatalf("stale policy update = %+v, %v; want cluster_policy_version_conflict", stale, err)
	}
	second, err := machine.Apply(8, clusterPolicyTestCommand(8, 1, 2, changed, "policy-2"))
	if err != nil || second.Outcome != ApplyOutcomeApplied || second.Replayed {
		t.Fatalf("second policy version = %+v, %v", second, err)
	}
	state, found := machine.ClusterPolicy()
	want := ClusterPolicyStateDTO{Version: 2, Policy: changed, IdempotencyKey: "policy-2", PreviousVersion: 1}
	if !found || state != want || machine.LastAppliedIndex() != 8 {
		t.Fatalf("committed policy = (%+v, %t), index %d; want %+v at index 8", state, found, machine.LastAppliedIndex(), want)
	}
}

// Traceability: API-09; docs/contracts/replicated-state-v1.md §1-3; ADR-0052 §1.1.1.
func TestClusterPolicyCommandValidationAndVersionBounds(t *testing.T) {
	valid := ClusterPolicyChangeDTO{ExpectedVersion: 0, Version: 1, Policy: clusterPolicyTestValue(64), IdempotencyKey: "policy-1"}
	maximum := valid
	maximum.Policy.MQTTLimits = MQTTLimitsDTO{
		MaxPacketSize: maxClusterPolicyPacketSize, ReceiveMaximum: 65535,
		ConnectTimeoutMillis: ^uint32(0), PacketReadTimeoutMillis: ^uint32(0), WriteTimeoutMillis: ^uint32(0),
	}
	if err := validateClusterPolicyChange(maximum); err != nil {
		t.Fatalf("maximum ClusterPolicy limits: %v", err)
	}

	invalid := []struct {
		name   string
		change ClusterPolicyChangeDTO
	}{
		{name: "version zero", change: func() ClusterPolicyChangeDTO { c := valid; c.Version = 0; return c }()},
		{name: "version not greater than expected", change: func() ClusterPolicyChangeDTO { c := valid; c.ExpectedVersion = 1; return c }()},
		{name: "packet size zero", change: func() ClusterPolicyChangeDTO { c := valid; c.Policy.MQTTLimits.MaxPacketSize = 0; return c }()},
		{name: "packet size above protocol maximum", change: func() ClusterPolicyChangeDTO {
			c := valid
			c.Policy.MQTTLimits.MaxPacketSize = maxClusterPolicyPacketSize + 1
			return c
		}()},
		{name: "receive maximum zero", change: func() ClusterPolicyChangeDTO { c := valid; c.Policy.MQTTLimits.ReceiveMaximum = 0; return c }()},
		{name: "receive maximum above protocol maximum", change: func() ClusterPolicyChangeDTO { c := valid; c.Policy.MQTTLimits.ReceiveMaximum = 65536; return c }()},
		{name: "zero timeout", change: func() ClusterPolicyChangeDTO { c := valid; c.Policy.MQTTLimits.WriteTimeoutMillis = 0; return c }()},
		{name: "non-printable key", change: func() ClusterPolicyChangeDTO { c := valid; c.IdempotencyKey = "policy key"; return c }()},
		{name: "key too long", change: func() ClusterPolicyChangeDTO { c := valid; c.IdempotencyKey = strings.Repeat("a", 129); return c }()},
	}
	for _, testCase := range invalid {
		t.Run(testCase.name, func(t *testing.T) {
			machine := clusterPolicyTestMachine(t)
			change := testCase.change
			result, err := machine.Apply(3, CommandDTO{
				SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(3),
				ClusterPolicy: &change,
			})
			if !errors.Is(err, ErrInvalidCommand) || result.Index != 0 || machine.LastAppliedIndex() != 2 {
				t.Fatalf("invalid command result=(%+v,%v), applied index=%d; want ErrInvalidCommand without state change", result, err, machine.LastAppliedIndex())
			}
		})
	}

	machine := clusterPolicyTestMachine(t)
	command := clusterPolicyTestCommand(3, 0, 1, clusterPolicyTestValue(64), "policy-1")
	command.FeatureActivation = &FeatureActivationDTO{Feature: ClusterPolicyFeature}
	if _, err := machine.Apply(3, command); !errors.Is(err, ErrInvalidCommand) || machine.LastAppliedIndex() != 2 {
		t.Fatalf("multiple payload command error=%v applied index=%d; want ErrInvalidCommand without state change", err, machine.LastAppliedIndex())
	}
}

// Traceability: API-09; docs/contracts/replicated-state-v1.md §1-3; ADR-0052 §1.1.1.
func TestClusterPolicySnapshotRoundTripAndRejectsInvalidState(t *testing.T) {
	machine := clusterPolicyTestMachine(t)
	policy := clusterPolicyTestValue(64)
	if _, err := machine.Apply(3, clusterPolicyTestCommand(3, 0, 1, policy, "policy-1")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := machine.Snapshot(3)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.ObjectCount != 5 || snapshot.ClusterPolicy == nil {
		t.Fatalf("snapshot object count/state = (%d, %+v), want 5 and a policy state", snapshot.ObjectCount, snapshot.ClusterPolicy)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore valid policy snapshot: %v", err)
	}
	restoredSnapshot, err := restored.Snapshot(3)
	if err != nil || !sameSnapshotState(snapshot, restoredSnapshot) {
		t.Fatalf("restored policy snapshot = (%+v, %v), want canonical original", restoredSnapshot, err)
	}

	cases := []struct {
		name   string
		mutate func(*SnapshotDTO)
	}{
		{name: "feature missing", mutate: func(s *SnapshotDTO) { s.ActiveFeatures = nil; s.ObjectCount-- }},
		{name: "zero version", mutate: func(s *SnapshotDTO) { s.ClusterPolicy.Version = 0 }},
		{name: "previous version is not lower", mutate: func(s *SnapshotDTO) { s.ClusterPolicy.PreviousVersion = s.ClusterPolicy.Version }},
		{name: "invalid key", mutate: func(s *SnapshotDTO) { s.ClusterPolicy.IdempotencyKey = "bad key" }},
		{name: "invalid packet limit", mutate: func(s *SnapshotDTO) { s.ClusterPolicy.Policy.MQTTLimits.MaxPacketSize = maxClusterPolicyPacketSize + 1 }},
		{name: "object count mismatch", mutate: func(s *SnapshotDTO) { s.ObjectCount++ }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			invalid := cloneSnapshot(snapshot)
			testCase.mutate(&invalid)
			target := NewStateMachine()
			if err := target.Restore(invalid); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("Restore() error = %v, want ErrInvalidSnapshot", err)
			}
			if target.LastAppliedIndex() != 0 || len(target.Members()) != 0 {
				t.Fatal("invalid policy snapshot changed target state")
			}
			if _, found := target.ClusterPolicy(); found {
				t.Fatal("invalid policy snapshot installed ClusterPolicy")
			}
		})
	}
}
