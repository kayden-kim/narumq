package records

import (
	"bytes"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
)

func blocklistCommand(action BlocklistAction, kind BlocklistKind, tenantID, value string) *CommandEnvelope {
	return &CommandEnvelope{
		SchemaMajor: 1,
		OperationId: op1,
		Payload: &CommandEnvelope_Blocklist{Blocklist: &BlocklistChange{
			Action:               action,
			Entry:                &BlocklistEntry{Kind: kind, TenantId: tenantID, Value: value},
			ProposedAtUnixMillis: 100,
		}},
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-040, tenancy-and-security.md:SEC-SCN-043
func TestBlocklistCommandCodecRoundTripAndValidation(t *testing.T) {
	entries := []struct {
		kind     BlocklistKind
		tenantID string
		value    string
	}{
		{BlocklistKind_BLOCKLIST_KIND_CLIENT_ID, "tenant-a", "client-a"},
		{BlocklistKind_BLOCKLIST_KIND_USERNAME, "", "user-a"},
		{BlocklistKind_BLOCKLIST_KIND_CERTIFICATE_SHA256, "", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		{BlocklistKind_BLOCKLIST_KIND_IP_PREFIX, "", "203.0.113.0/24"},
	}
	for _, entry := range entries {
		for _, action := range []BlocklistAction{
			BlocklistAction_BLOCKLIST_ACTION_BLOCK,
			BlocklistAction_BLOCKLIST_ACTION_UNBLOCK,
		} {
			command := blocklistCommand(action, entry.kind, entry.tenantID, entry.value)
			wire, err := MarshalCommand(command)
			if err != nil {
				t.Fatalf("MarshalCommand(kind %d, action %d): %v", entry.kind, action, err)
			}
			decoded, err := UnmarshalCommand(wire)
			if err != nil || !proto.Equal(decoded, command) {
				t.Fatalf("blocklist command round trip = %v, %v", decoded, err)
			}
			reencoded, err := MarshalCommand(decoded)
			if err != nil || !bytes.Equal(reencoded, wire) {
				t.Fatalf("blocklist command re-encode differs: %v", err)
			}
		}
	}

	invalid := []struct {
		name    string
		command *CommandEnvelope
	}{
		{"unspecified kind", blocklistCommand(BlocklistAction_BLOCKLIST_ACTION_BLOCK, BlocklistKind_BLOCKLIST_KIND_UNSPECIFIED, "", "user-a")},
		{"unspecified action", blocklistCommand(BlocklistAction_BLOCKLIST_ACTION_UNSPECIFIED, BlocklistKind_BLOCKLIST_KIND_USERNAME, "", "user-a")},
		{"noncanonical IP prefix", blocklistCommand(BlocklistAction_BLOCKLIST_ACTION_BLOCK, BlocklistKind_BLOCKLIST_KIND_IP_PREFIX, "", "203.0.113.5/24")},
		{"invalid entry", blocklistCommand(BlocklistAction_BLOCKLIST_ACTION_BLOCK, BlocklistKind_BLOCKLIST_KIND_CLIENT_ID, "", "client-a")},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := MarshalCommand(tc.command); err == nil {
				t.Fatal("MarshalCommand accepted invalid blocklist command")
			}
		})
	}
}

func blocklistSnapshot(entries ...*BlocklistState) *ReplicatedStateSnapshot {
	return &ReplicatedStateSnapshot{
		ActiveFeatures:   []string{"client-blocklist"},
		Blocklist:        entries,
		BlocklistVersion: 1,
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-043
func TestBlocklistSnapshotSortsCountsAndLimits(t *testing.T) {
	state := blocklistSnapshot(
		&BlocklistState{Entry: &BlocklistEntry{Kind: BlocklistKind_BLOCKLIST_KIND_IP_PREFIX, Value: "203.0.113.0/24"}, BlockedAtUnixMillis: 40},
		&BlocklistState{Entry: &BlocklistEntry{Kind: BlocklistKind_BLOCKLIST_KIND_CERTIFICATE_SHA256, Value: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}, BlockedAtUnixMillis: 30},
		&BlocklistState{Entry: &BlocklistEntry{Kind: BlocklistKind_BLOCKLIST_KIND_USERNAME, Value: "user-a"}, BlockedAtUnixMillis: 20},
		&BlocklistState{Entry: &BlocklistEntry{Kind: BlocklistKind_BLOCKLIST_KIND_CLIENT_ID, TenantId: "tenant-a", Value: "client-a"}, BlockedAtUnixMillis: 10},
	)
	before := proto.Clone(state)
	wire, err := MarshalSnapshot(state, 10)
	if err != nil {
		t.Fatalf("MarshalSnapshot(valid blocklist): %v", err)
	}
	if !proto.Equal(before, state) {
		t.Fatal("MarshalSnapshot modified its input")
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(valid blocklist): %v", err)
	}
	got := envelope.State.Blocklist
	if envelope.ObjectCount != 5 || len(got) != 4 || envelope.State.BlocklistVersion != 1 {
		t.Fatalf("snapshot count, entries and version = %d, %d and %d; want 5, 4 and 1", envelope.ObjectCount, len(got), envelope.State.BlocklistVersion)
	}
	for i, kind := range []BlocklistKind{
		BlocklistKind_BLOCKLIST_KIND_CLIENT_ID,
		BlocklistKind_BLOCKLIST_KIND_USERNAME,
		BlocklistKind_BLOCKLIST_KIND_CERTIFICATE_SHA256,
		BlocklistKind_BLOCKLIST_KIND_IP_PREFIX,
	} {
		if got[i].Entry.Kind != kind {
			t.Fatalf("sorted blocklist entry %d kind = %d; want %d", i, got[i].Entry.Kind, kind)
		}
	}

	for _, count := range []int{maxBlocklistEntries, maxBlocklistEntries + 1} {
		entries := make([]*BlocklistState, count)
		for i := range entries {
			entries[i] = &BlocklistState{
				Entry:               &BlocklistEntry{Kind: BlocklistKind_BLOCKLIST_KIND_CLIENT_ID, TenantId: "tenant-a", Value: fmt.Sprintf("client-%04d", i)},
				BlockedAtUnixMillis: uint64(i + 1),
			}
		}
		boundary := blocklistSnapshot(entries...)
		boundary.BlocklistVersion = 1
		boundaryWire, err := MarshalSnapshot(boundary, uint64(count+1))
		if count == maxBlocklistEntries && err != nil {
			t.Fatalf("MarshalSnapshot(%d blocklist entries): %v", count, err)
		}
		if count == maxBlocklistEntries+1 && err == nil {
			t.Fatalf("MarshalSnapshot accepted %d blocklist entries", count)
		}
		if count == maxBlocklistEntries {
			decoded, err := UnmarshalSnapshot(boundaryWire)
			if err != nil {
				t.Fatalf("UnmarshalSnapshot(%d blocklist entries): %v", count, err)
			}
			if len(decoded.State.Blocklist) != count || decoded.ObjectCount != uint64(count+1) {
				t.Fatalf("capacity snapshot entries/count = %d/%d; want %d/%d", len(decoded.State.Blocklist), decoded.ObjectCount, count, count+1)
			}
		}
	}
}
