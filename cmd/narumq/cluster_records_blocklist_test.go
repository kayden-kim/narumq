package main

import (
	"bytes"
	"reflect"
	"testing"

	"narumq/internal/application/membership"
	"narumq/internal/domain/security"
)

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-043
func TestClusterBlocklistRecordBridgeRoundTrips(t *testing.T) {
	entries := []security.BlockEntry{
		{Kind: security.BlockKindClientID, TenantID: "tenant-a", Value: "client-a"},
		{Kind: security.BlockKindUsername, Value: "user-a"},
		{Kind: security.BlockKindCertificateSHA256, Value: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		{Kind: security.BlockKindIPPrefix, Value: "203.0.113.0/24"},
	}
	commands := make([]membership.CommandDTO, 0, len(entries)*2)
	for i, entry := range entries {
		for _, action := range []membership.BlocklistAction{membership.BlocklistActionBlock, membership.BlocklistActionUnblock} {
			commands = append(commands, membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor,
				OperationID: "00000000-0000-4000-8000-000000000001",
				Blocklist: &membership.BlocklistChangeDTO{
					Action: action, Entry: entry, ProposedAtUnixMillis: uint64(100 + i),
				},
			})
		}
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
			t.Fatalf("command re-encode differs: %v", err)
		}
	}

	invalidCommands := []struct {
		name   string
		action membership.BlocklistAction
		entry  security.BlockEntry
	}{
		{"unspecified kind", membership.BlocklistActionBlock, security.BlockEntry{Value: "user-a"}},
		{"unspecified action", membership.BlocklistActionUnspecified, entries[1]},
	}
	for _, tc := range invalidCommands {
		t.Run(tc.name, func(t *testing.T) {
			command := membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor,
				OperationID: "00000000-0000-4000-8000-000000000002",
				Blocklist:   &membership.BlocklistChangeDTO{Action: tc.action, Entry: tc.entry, ProposedAtUnixMillis: 200},
			}
			if _, err := encodeMembershipCommand(command); err == nil {
				t.Fatal("encoded invalid blocklist command")
			}
		})
	}

	snapshot := membership.SnapshotDTO{
		SchemaMajor: membership.SchemaMajor, LastIncludedIndex: 10, ObjectCount: 5,
		ActiveFeatures: []string{membership.ClientBlocklistFeature}, BlocklistVersion: 5,
		Blocklist: []membership.BlocklistStateDTO{
			{Entry: entries[0], BlockedAtUnixMillis: 10},
			{Entry: entries[1], BlockedAtUnixMillis: 20},
			{Entry: entries[2], BlockedAtUnixMillis: 30},
			{Entry: entries[3], BlockedAtUnixMillis: 40},
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
	if !reflect.DeepEqual(decoded.Blocklist, snapshot.Blocklist) || decoded.BlocklistVersion != snapshot.BlocklistVersion || decoded.ObjectCount != snapshot.ObjectCount {
		t.Fatalf("snapshot round trip blocklist/version/count = %#v/%d/%d, want %#v/%d/%d", decoded.Blocklist, decoded.BlocklistVersion, decoded.ObjectCount, snapshot.Blocklist, snapshot.BlocklistVersion, snapshot.ObjectCount)
	}
	reencoded, err := encodeMembershipSnapshot(decoded)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("snapshot re-encode differs: %v", err)
	}
}
