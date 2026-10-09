package main

import (
	"bytes"
	"reflect"
	"testing"

	"narumq/internal/application/membership"
)

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; ADR-0049.
func TestClusterCredentialRevocationRecordBridgeRoundTrips(t *testing.T) {
	commands := []membership.CommandDTO{
		{
			SchemaMajor: membership.SchemaMajor, OperationID: "00000000-0000-4000-8000-000000000001",
			CredentialRevocation: &membership.CredentialRevocationChangeDTO{
				Action: membership.CredentialRevocationActionRevoke, CredentialID: "credential-a", ProposedAtUnixMillis: 100,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: "00000000-0000-4000-8000-000000000002",
			CredentialRevocation: &membership.CredentialRevocationChangeDTO{
				Action: membership.CredentialRevocationActionRestore, CredentialID: "credential-a", ProposedAtUnixMillis: 200,
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

	if _, err := encodeMembershipCommand(membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: "00000000-0000-4000-8000-000000000003",
		CredentialRevocation: &membership.CredentialRevocationChangeDTO{
			Action: membership.CredentialRevocationActionUnspecified, CredentialID: "credential-a", ProposedAtUnixMillis: 300,
		},
	}); err == nil {
		t.Fatal("encoded an unspecified credential revocation action")
	}

	snapshot := membership.SnapshotDTO{
		SchemaMajor: membership.SchemaMajor, LastIncludedIndex: 10, ObjectCount: 3,
		ActiveFeatures: []string{membership.CredentialRevocationFeature},
		CredentialRevocations: []membership.CredentialRevocationStateDTO{
			{CredentialID: "credential-a", RevokedAtUnixMillis: 100},
			{CredentialID: "credential-z", RevokedAtUnixMillis: 200},
		},
	}
	encoded, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	decoded, err := decodeMembershipSnapshot(encoded)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if !reflect.DeepEqual(decoded.CredentialRevocations, snapshot.CredentialRevocations) || decoded.ObjectCount != snapshot.ObjectCount {
		t.Fatalf("snapshot round trip entries/count = %#v/%d, want %#v/%d", decoded.CredentialRevocations, decoded.ObjectCount, snapshot.CredentialRevocations, snapshot.ObjectCount)
	}
	reencoded, err := encodeMembershipSnapshot(decoded)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("snapshot re-encode error = %v", err)
	}
}
