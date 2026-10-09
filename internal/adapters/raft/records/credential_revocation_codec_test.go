package records

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

func credentialRevocationCommand(action CredentialRevocationAction, credentialID string, proposedAt uint64) *CommandEnvelope {
	return &CommandEnvelope{
		SchemaMajor: 1,
		OperationId: op1,
		Payload: &CommandEnvelope_CredentialRevocation{CredentialRevocation: &CredentialRevocationChange{
			Action: action, CredentialId: credentialID, ProposedAtUnixMillis: proposedAt,
		}},
	}
}

func credentialRevocationSnapshot(entries ...*CredentialRevocationState) *ReplicatedStateSnapshot {
	return &ReplicatedStateSnapshot{
		ActiveFeatures:        []string{"credential-revocation"},
		CredentialRevocations: entries,
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; docs/contracts/replicated-state-v1.md §2, §4.
func TestCredentialRevocationCommandCodecRoundTripAndValidation(t *testing.T) {
	for _, action := range []CredentialRevocationAction{
		CredentialRevocationAction_CREDENTIAL_REVOCATION_ACTION_REVOKE,
		CredentialRevocationAction_CREDENTIAL_REVOCATION_ACTION_RESTORE,
	} {
		command := credentialRevocationCommand(action, strings.Repeat("é", 256), 1)
		wire, err := MarshalCommand(command)
		if err != nil {
			t.Fatalf("MarshalCommand(valid action %d): %v", action, err)
		}
		decoded, err := UnmarshalCommand(wire)
		if err != nil || !proto.Equal(decoded, command) {
			t.Fatalf("credential revocation command round trip = %v, %v", decoded, err)
		}
	}

	cases := []struct {
		name    string
		command *CommandEnvelope
	}{
		{"missing payload", crrCommand(&CommandEnvelope_CredentialRevocation{})},
		{"unspecified action", credentialRevocationCommand(0, "key", 1)},
		{"unknown action", credentialRevocationCommand(CredentialRevocationAction(99), "key", 1)},
		{"empty credential id", credentialRevocationCommand(CredentialRevocationAction_CREDENTIAL_REVOCATION_ACTION_REVOKE, "", 1)},
		{"oversized credential id", credentialRevocationCommand(CredentialRevocationAction_CREDENTIAL_REVOCATION_ACTION_REVOKE, strings.Repeat("k", 513), 1)},
		{"invalid utf8 credential id", credentialRevocationCommand(CredentialRevocationAction_CREDENTIAL_REVOCATION_ACTION_REVOKE, string([]byte{0xff}), 1)},
		{"missing proposal time", credentialRevocationCommand(CredentialRevocationAction_CREDENTIAL_REVOCATION_ACTION_REVOKE, "key", 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := MarshalCommand(tc.command)
			if err == nil {
				t.Fatal("MarshalCommand accepted an invalid credential revocation")
			}
		})
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; docs/contracts/replicated-state-v1.md §4.
func TestCredentialRevocationSnapshotRoundTripSortsAndCountsEntries(t *testing.T) {
	maxID := strings.Repeat("é", 256)
	state := credentialRevocationSnapshot(
		&CredentialRevocationState{CredentialId: "credential-z", RevokedAtUnixMillis: 20},
		&CredentialRevocationState{CredentialId: maxID, RevokedAtUnixMillis: 30},
		&CredentialRevocationState{CredentialId: "credential-a", RevokedAtUnixMillis: 10},
	)
	before := proto.Clone(state)
	wire, err := MarshalSnapshot(state, 40)
	if err != nil {
		t.Fatalf("MarshalSnapshot(valid revocations): %v", err)
	}
	if !proto.Equal(before, state) {
		t.Fatal("MarshalSnapshot modified its input")
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(valid revocations): %v", err)
	}
	if envelope.ObjectCount != 4 || len(envelope.State.CredentialRevocations) != 3 {
		t.Fatalf("snapshot count and revocations = %d and %d; want 4 and 3", envelope.ObjectCount, len(envelope.State.CredentialRevocations))
	}
	got := envelope.State.CredentialRevocations
	if got[0].CredentialId != "credential-a" || got[1].CredentialId != "credential-z" || got[2].CredentialId != maxID {
		t.Fatalf("revocations were not sorted by credential ID: %q, %q, %q", got[0].CredentialId, got[1].CredentialId, got[2].CredentialId)
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; docs/contracts/replicated-state-v1.md §4.
func TestCredentialRevocationSnapshotRejectsInvalidState(t *testing.T) {
	cases := []struct {
		name  string
		state *ReplicatedStateSnapshot
	}{
		{"missing feature", &ReplicatedStateSnapshot{CredentialRevocations: []*CredentialRevocationState{{CredentialId: "key", RevokedAtUnixMillis: 1}}}},
		{"nil entry", credentialRevocationSnapshot(nil)},
		{"empty credential id", credentialRevocationSnapshot(&CredentialRevocationState{RevokedAtUnixMillis: 1})},
		{"oversized credential id", credentialRevocationSnapshot(&CredentialRevocationState{CredentialId: strings.Repeat("k", 513), RevokedAtUnixMillis: 1})},
		{"invalid utf8 credential id", credentialRevocationSnapshot(&CredentialRevocationState{CredentialId: string([]byte{0xff}), RevokedAtUnixMillis: 1})},
		{"zero revoked time", credentialRevocationSnapshot(&CredentialRevocationState{CredentialId: "key"})},
		{"duplicate credential id", credentialRevocationSnapshot(
			&CredentialRevocationState{CredentialId: "key", RevokedAtUnixMillis: 1},
			&CredentialRevocationState{CredentialId: "key", RevokedAtUnixMillis: 2},
		)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := MarshalSnapshot(tc.state, 1)
			if err == nil {
				t.Fatal("MarshalSnapshot accepted invalid credential revocation state")
			}
		})
	}

	unsorted := credentialRevocationSnapshot(
		&CredentialRevocationState{CredentialId: "key-z", RevokedAtUnixMillis: 2},
		&CredentialRevocationState{CredentialId: "key-a", RevokedAtUnixMillis: 1},
	)
	if _, err := UnmarshalSnapshot(crrRawSnapshot(t, unsorted, 1)); err == nil {
		t.Fatal("UnmarshalSnapshot accepted unsorted credential revocations")
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; docs/contracts/replicated-state-v1.md §4.
func TestCredentialRevocationSnapshotCapacityBoundary(t *testing.T) {
	entries := make([]*CredentialRevocationState, maxCredentialRevocations)
	for index := range entries {
		entries[index] = &CredentialRevocationState{
			CredentialId:        fmt.Sprintf("credential-%04d", index),
			RevokedAtUnixMillis: 1,
		}
	}
	wire, err := MarshalSnapshot(credentialRevocationSnapshot(entries...), 2)
	if err != nil {
		t.Fatalf("MarshalSnapshot(%d entries): %v", maxCredentialRevocations, err)
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(%d entries): %v", maxCredentialRevocations, err)
	}
	if len(envelope.State.CredentialRevocations) != maxCredentialRevocations {
		t.Fatalf("restored revocation count = %d, want %d", len(envelope.State.CredentialRevocations), maxCredentialRevocations)
	}
	tooMany := append(entries, &CredentialRevocationState{CredentialId: "credential-overflow", RevokedAtUnixMillis: 1})
	if _, err := MarshalSnapshot(credentialRevocationSnapshot(tooMany...), 2); err == nil {
		t.Fatal("MarshalSnapshot accepted more than the credential revocation limit")
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; docs/contracts/replicated-state-v1.md §2.
func TestCredentialRevocationCommandReferenceVector(t *testing.T) {
	command := credentialRevocationCommand(CredentialRevocationAction_CREDENTIAL_REVOCATION_ACTION_REVOKE, "k", 100)
	wire, err := MarshalCommand(command)
	if err != nil {
		t.Fatalf("MarshalCommand(reference vector): %v", err)
	}
	want, err := hex.DecodeString("08011a2430303030303030302d303030302d343030302d383030302d303030303030303030303031ca0107080112016b1864")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, want) {
		t.Fatalf("credential revocation wire = %x, want %x", wire, want)
	}
	decoded, err := UnmarshalCommand(want)
	if err != nil || !proto.Equal(decoded, command) {
		t.Fatalf("UnmarshalCommand(reference vector) = %v, %v", decoded, err)
	}
}
