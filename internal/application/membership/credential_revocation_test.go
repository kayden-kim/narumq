package membership

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func credentialRevocationOperationID(sequence uint64) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", sequence)
}

func credentialRevocationCommand(operationID string, action CredentialRevocationAction, credentialID string, proposedAt uint64) CommandDTO {
	return CommandDTO{
		SchemaMajor: SchemaMajor,
		OperationID: operationID,
		CredentialRevocation: &CredentialRevocationChangeDTO{
			Action: action, CredentialID: credentialID, ProposedAtUnixMillis: proposedAt,
		},
	}
}

func activateCredentialRevocationForTest(t *testing.T, machine *StateMachine, index uint64) {
	t.Helper()
	applyMustSucceed(t, machine, index, CommandDTO{
		SchemaMajor:       SchemaMajor,
		OperationID:       credentialRevocationOperationID(index),
		FeatureActivation: &FeatureActivationDTO{Feature: CredentialRevocationFeature},
	})
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; ADR-0049.
// Scenarios: tenancy-and-security.md:SEC-SCN-036
func TestCredentialRevocationApplyFeatureGateAndTransitions(t *testing.T) {
	machine := NewStateMachine()
	credentialID := strings.Repeat("é", 256)
	inactive, err := machine.Apply(1, credentialRevocationCommand(
		credentialRevocationOperationID(1), CredentialRevocationActionRevoke, credentialID, 100,
	))
	if err != nil || inactive.Outcome != ApplyOutcomeConflict || inactive.Conflict != ConflictFeatureInactive {
		t.Fatalf("inactive revocation = %+v, %v; want feature_inactive conflict", inactive, err)
	}
	if machine.IsCredentialRevoked(credentialID) {
		t.Fatal("inactive revocation changed committed state")
	}

	activateCredentialRevocationForTest(t, machine, 2)
	first := applyMustSucceed(t, machine, 3, credentialRevocationCommand(
		credentialRevocationOperationID(3), CredentialRevocationActionRevoke, credentialID, 100,
	))
	if first.Index != 3 || !machine.IsCredentialRevoked(credentialID) {
		t.Fatalf("first revocation result = %+v; credential is not revoked", first)
	}
	applyMustSucceed(t, machine, 4, credentialRevocationCommand(
		credentialRevocationOperationID(4), CredentialRevocationActionRevoke, credentialID, 200,
	))
	state, ok := machine.GetCredentialRevocation(credentialID)
	if !ok || state.RevokedAtUnixMillis != 100 {
		t.Fatalf("repeated revocation = %+v, %v; want original timestamp 100", state, ok)
	}
	if entries := machine.CredentialRevocations(); len(entries) != 1 || entries[0] != state {
		t.Fatalf("credential revocation list = %+v; want the committed entry", entries)
	}

	applyMustSucceed(t, machine, 5, credentialRevocationCommand(
		credentialRevocationOperationID(5), CredentialRevocationActionRestore, credentialID, 300,
	))
	if machine.IsCredentialRevoked(credentialID) || len(machine.CredentialRevocations()) != 0 {
		t.Fatal("restore did not remove the committed revocation")
	}
	applyMustSucceed(t, machine, 6, credentialRevocationCommand(
		credentialRevocationOperationID(6), CredentialRevocationActionRestore, credentialID, 400,
	))
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; ADR-0049.
// Scenarios: tenancy-and-security.md:SEC-SCN-036
func TestCredentialRevocationApplyRejectsInvalidCommandsWithoutAdvancing(t *testing.T) {
	cases := []struct {
		name   string
		change CredentialRevocationChangeDTO
	}{
		{"empty id", CredentialRevocationChangeDTO{Action: CredentialRevocationActionRevoke, ProposedAtUnixMillis: 1}},
		{"oversized id", CredentialRevocationChangeDTO{Action: CredentialRevocationActionRevoke, CredentialID: strings.Repeat("k", MaxCredentialIDBytes+1), ProposedAtUnixMillis: 1}},
		{"invalid utf8 id", CredentialRevocationChangeDTO{Action: CredentialRevocationActionRevoke, CredentialID: string([]byte{0xff}), ProposedAtUnixMillis: 1}},
		{"zero proposal time", CredentialRevocationChangeDTO{Action: CredentialRevocationActionRevoke, CredentialID: "credential", ProposedAtUnixMillis: 0}},
		{"unspecified action", CredentialRevocationChangeDTO{CredentialID: "credential", ProposedAtUnixMillis: 1}},
		{"unknown action", CredentialRevocationChangeDTO{Action: CredentialRevocationAction(99), CredentialID: "credential", ProposedAtUnixMillis: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			machine := NewStateMachine()
			_, err := machine.Apply(1, CommandDTO{
				SchemaMajor: SchemaMajor, OperationID: credentialRevocationOperationID(1), CredentialRevocation: &tc.change,
			})
			if !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("Apply() error = %v, want ErrInvalidCommand", err)
			}
			if machine.LastAppliedIndex() != 0 || len(machine.CredentialRevocations()) != 0 {
				t.Fatal("invalid command advanced or changed replicated state")
			}
		})
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; ADR-0049.
// Scenarios: tenancy-and-security.md:SEC-SCN-036
func TestCredentialRevocationSnapshotRestorePreservesSortedState(t *testing.T) {
	machine := NewStateMachine()
	activateCredentialRevocationForTest(t, machine, 1)
	applyMustSucceed(t, machine, 2, credentialRevocationCommand(
		credentialRevocationOperationID(2), CredentialRevocationActionRevoke, "credential-z", 20,
	))
	applyMustSucceed(t, machine, 3, credentialRevocationCommand(
		credentialRevocationOperationID(3), CredentialRevocationActionRevoke, "credential-a", 10,
	))

	snapshot, err := machine.Snapshot(3)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if snapshot.ObjectCount != 3 || len(snapshot.CredentialRevocations) != 2 || snapshot.CredentialRevocations[0].CredentialID != "credential-a" || snapshot.CredentialRevocations[1].CredentialID != "credential-z" {
		t.Fatalf("snapshot credential revocations/count = %+v / %d", snapshot.CredentialRevocations, snapshot.ObjectCount)
	}

	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore(): %v", err)
	}
	if !restored.IsCredentialRevoked("credential-a") || !restored.IsCredentialRevoked("credential-z") {
		t.Fatal("restored machine lost credential revocations")
	}
	if state, ok := restored.GetCredentialRevocation("credential-a"); !ok || state.RevokedAtUnixMillis != 10 {
		t.Fatalf("restored credential state = %+v, %v", state, ok)
	}
	restoredSnapshot, err := restored.Snapshot(3)
	if err != nil || !sameSnapshotState(snapshot, restoredSnapshot) {
		t.Fatalf("restored snapshot = %+v, %v; want original state", restoredSnapshot, err)
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; ADR-0049.
// Scenarios: tenancy-and-security.md:SEC-SCN-036
func TestCredentialRevocationCapacityBoundary(t *testing.T) {
	machine := NewStateMachine()
	activateCredentialRevocationForTest(t, machine, 1)
	for index := 0; index < MaxCredentialRevocations; index++ {
		change := credentialRevocationCommand(
			credentialRevocationOperationID(uint64(index+1000)), CredentialRevocationActionRevoke,
			fmt.Sprintf("credential-%04d", index), uint64(index+1),
		)
		result, err := machine.Apply(uint64(index+2), change)
		if err != nil || result.Outcome != ApplyOutcomeApplied {
			t.Fatalf("Apply(revocation %d) = %+v, %v", index, result, err)
		}
	}
	result, err := machine.Apply(uint64(MaxCredentialRevocations+2), credentialRevocationCommand(
		credentialRevocationOperationID(uint64(MaxCredentialRevocations+1000)), CredentialRevocationActionRevoke, "credential-overflow", 5000,
	))
	if err != nil || result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictCredentialRevocationCapacity {
		t.Fatalf("capacity result = %+v, %v; want credential_revocation_capacity_exceeded", result, err)
	}
	if len(machine.CredentialRevocations()) != MaxCredentialRevocations || machine.IsCredentialRevoked("credential-overflow") {
		t.Fatal("capacity conflict changed the revocation list")
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; ADR-0049.
// Scenarios: tenancy-and-security.md:SEC-SCN-036
func TestCredentialRevocationSnapshotRejectsInvalidState(t *testing.T) {
	base := SnapshotDTO{
		SchemaMajor: SchemaMajor, LastIncludedIndex: 1, ObjectCount: 2,
		ActiveFeatures:        []string{CredentialRevocationFeature},
		CredentialRevocations: []CredentialRevocationStateDTO{{CredentialID: "credential-a", RevokedAtUnixMillis: 1}},
	}
	cases := []struct {
		name   string
		mutate func(*SnapshotDTO)
	}{
		{"missing feature", func(snapshot *SnapshotDTO) { snapshot.ActiveFeatures = nil; snapshot.ObjectCount = 1 }},
		{"zero revoked time", func(snapshot *SnapshotDTO) { snapshot.CredentialRevocations[0].RevokedAtUnixMillis = 0 }},
		{"oversized credential id", func(snapshot *SnapshotDTO) {
			snapshot.CredentialRevocations[0].CredentialID = strings.Repeat("k", MaxCredentialIDBytes+1)
		}},
		{"unsorted credential ids", func(snapshot *SnapshotDTO) {
			snapshot.CredentialRevocations = []CredentialRevocationStateDTO{
				{CredentialID: "credential-z", RevokedAtUnixMillis: 2},
				{CredentialID: "credential-a", RevokedAtUnixMillis: 1},
			}
			snapshot.ObjectCount = 3
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := cloneSnapshot(base)
			tc.mutate(&snapshot)
			machine := NewStateMachine()
			if err := machine.Restore(snapshot); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("Restore() error = %v, want ErrInvalidSnapshot", err)
			}
			if machine.LastAppliedIndex() != 0 || len(machine.CredentialRevocations()) != 0 {
				t.Fatal("invalid snapshot changed machine state")
			}
		})
	}
}
