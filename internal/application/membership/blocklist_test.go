package membership

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"narumq/internal/domain/security"
)

func blocklistTestOperationID(index uint64) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", index)
}

func blocklistTestEntry(t *testing.T, kind security.BlockKind, tenant, value string) security.BlockEntry {
	t.Helper()
	entry, err := security.NewBlockEntry(kind, tenant, value)
	if err != nil {
		t.Fatalf("NewBlockEntry(%v, %q, %q): %v", kind, tenant, value, err)
	}
	return entry
}

func blocklistTestCommand(index uint64, action BlocklistAction, entry security.BlockEntry, proposedAt uint64) CommandDTO {
	return CommandDTO{SchemaMajor: SchemaMajor, OperationID: blocklistTestOperationID(index), Blocklist: &BlocklistChangeDTO{
		Action: action, Entry: entry, ProposedAtUnixMillis: proposedAt,
	}}
}

func activateBlocklistForTest(t *testing.T, machine *StateMachine, index uint64) {
	t.Helper()
	applyMustSucceed(t, machine, index, CommandDTO{SchemaMajor: SchemaMajor, OperationID: blocklistTestOperationID(index), FeatureActivation: &FeatureActivationDTO{Feature: ClientBlocklistFeature}})
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-043
func TestBlocklistApplyFeatureGateTransitionsAndVersion(t *testing.T) {
	machine := NewStateMachine()
	entry := blocklistTestEntry(t, security.BlockKindClientID, "tenant", "client")
	inactive, err := machine.Apply(1, blocklistTestCommand(1, BlocklistActionBlock, entry, 100))
	if err != nil || inactive.Outcome != ApplyOutcomeConflict || inactive.Conflict != ConflictFeatureInactive {
		t.Fatalf("inactive block = %+v, %v; want feature_inactive conflict", inactive, err)
	}
	if machine.IsBlocked(entry) || machine.Blocklist().PolicyVersion != 0 {
		t.Fatal("inactive block changed blocklist state")
	}

	activateBlocklistForTest(t, machine, 2)
	first := applyMustSucceed(t, machine, 3, blocklistTestCommand(3, BlocklistActionBlock, entry, 100))
	if first.Blocklist == nil || !first.Blocklist.Blocked || first.Blocklist.BlockedAtUnixMillis != 100 || first.Blocklist.PolicyVersion != 3 {
		t.Fatalf("first block status = %+v; want blocked at 100, version 3", first.Blocklist)
	}
	repeated := applyMustSucceed(t, machine, 4, blocklistTestCommand(4, BlocklistActionBlock, entry, 200))
	if repeated.Blocklist == nil || !repeated.Blocklist.Blocked || repeated.Blocklist.BlockedAtUnixMillis != 100 || repeated.Blocklist.PolicyVersion != 3 {
		t.Fatalf("repeated block status = %+v; original time and version must persist", repeated.Blocklist)
	}
	unblocked := applyMustSucceed(t, machine, 5, blocklistTestCommand(5, BlocklistActionUnblock, entry, 300))
	if unblocked.Blocklist == nil || unblocked.Blocklist.Blocked || unblocked.Blocklist.PolicyVersion != 5 || machine.IsBlocked(entry) {
		t.Fatalf("unblock result = %+v; entry remains blocked or version did not advance", unblocked.Blocklist)
	}
	missing := blocklistTestEntry(t, security.BlockKindUsername, "", "missing")
	noOp := applyMustSucceed(t, machine, 6, blocklistTestCommand(6, BlocklistActionUnblock, missing, 400))
	if noOp.Blocklist == nil || noOp.Blocklist.Blocked || noOp.Blocklist.PolicyVersion != 5 || machine.Blocklist().PolicyVersion != 5 {
		t.Fatalf("unblock missing result = %+v; no-op must preserve version 5", noOp.Blocklist)
	}

	capacity := NewStateMachine()
	activateBlocklistForTest(t, capacity, 1)
	for index := range MaxBlocklistEntries {
		value := fmt.Sprintf("entry-%04d", index)
		candidate := blocklistTestEntry(t, security.BlockKindUsername, "", value)
		result, applyErr := capacity.Apply(uint64(index+2), blocklistTestCommand(uint64(index+2), BlocklistActionBlock, candidate, uint64(index+1)))
		if applyErr != nil || result.Outcome != ApplyOutcomeApplied {
			t.Fatalf("fill blocklist at %d = %+v, %v", index, result, applyErr)
		}
	}
	before := capacity.Blocklist()
	overflow := blocklistTestEntry(t, security.BlockKindUsername, "", "entry-overflow")
	result, err := capacity.Apply(MaxBlocklistEntries+2, blocklistTestCommand(MaxBlocklistEntries+2, BlocklistActionBlock, overflow, 9000))
	after := capacity.Blocklist()
	if err != nil || result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictBlocklistCapacity {
		t.Fatalf("capacity result = %+v, %v; want blocklist capacity conflict", result, err)
	}
	if !reflect.DeepEqual(after, before) || capacity.IsBlocked(overflow) {
		t.Fatal("capacity conflict changed blocklist state")
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-043
func TestBlocklistSnapshotRoundTripAndRejectsInvalidState(t *testing.T) {
	machine := NewStateMachine()
	activateBlocklistForTest(t, machine, 1)
	entryZ := blocklistTestEntry(t, security.BlockKindUsername, "", "zulu")
	entryA := blocklistTestEntry(t, security.BlockKindUsername, "", "alpha")
	applyMustSucceed(t, machine, 2, blocklistTestCommand(2, BlocklistActionBlock, entryZ, 20))
	applyMustSucceed(t, machine, 3, blocklistTestCommand(3, BlocklistActionBlock, entryA, 30))
	snapshot, err := machine.Snapshot(3)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if snapshot.ObjectCount != 3 || snapshot.BlocklistVersion != 3 || len(snapshot.Blocklist) != 2 || snapshot.Blocklist[0].Entry != entryA || snapshot.Blocklist[1].Entry != entryZ {
		t.Fatalf("snapshot blocklist/count/version = %+v / %d / %d", snapshot.Blocklist, snapshot.ObjectCount, snapshot.BlocklistVersion)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore(): %v", err)
	}
	if got := restored.Blocklist(); got.PolicyVersion != 3 || len(got.Entries) != 2 || got.Entries[0].Entry != entryA || got.Entries[1].Entry != entryZ {
		t.Fatalf("restored blocklist = %+v", got)
	}
	restoredSnapshot, err := restored.Snapshot(3)
	if err != nil || !sameSnapshotState(snapshot, restoredSnapshot) {
		t.Fatalf("restored snapshot = %+v, %v; want original state", restoredSnapshot, err)
	}

	cases := []struct {
		name   string
		mutate func(*SnapshotDTO)
	}{
		{"missing feature", func(state *SnapshotDTO) { state.ActiveFeatures = nil; state.ObjectCount-- }},
		{"unsorted entries", func(state *SnapshotDTO) {
			state.Blocklist[0], state.Blocklist[1] = state.Blocklist[1], state.Blocklist[0]
		}},
		{"noncanonical entry", func(state *SnapshotDTO) {
			state.Blocklist[0].Entry = security.BlockEntry{Kind: security.BlockKindIPPrefix, Value: "203.0.113.5/24"}
		}},
		{"version after included index", func(state *SnapshotDTO) { state.LastIncludedIndex = state.BlocklistVersion - 1 }},
		{"entries without version", func(state *SnapshotDTO) { state.BlocklistVersion = 0 }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			invalid := cloneSnapshot(snapshot)
			test.mutate(&invalid)
			target := NewStateMachine()
			if err := target.Restore(invalid); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("Restore() error = %v, want ErrInvalidSnapshot", err)
			}
			if target.LastAppliedIndex() != 0 || len(target.Blocklist().Entries) != 0 || target.Blocklist().PolicyVersion != 0 {
				t.Fatal("invalid restore changed the target machine")
			}
		})
	}
}
