package membership

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"narumq/internal/domain/security"
)

type blocklistServiceTestPort struct {
	*stateMachinePort
}

func (port *blocklistServiceTestPort) Blocklist(ctx context.Context) (BlocklistDTO, error) {
	if err := ctx.Err(); err != nil {
		return BlocklistDTO{}, err
	}
	return port.machine.Blocklist(), nil
}

type blocklistServiceTestClock struct {
	now time.Time
}

func (clock *blocklistServiceTestClock) Now() time.Time { return clock.now }

func newBlocklistServiceTestFixture(t *testing.T) (*BlocklistService, *blocklistServiceTestPort, *blocklistServiceTestClock) {
	t.Helper()
	machine := NewStateMachine()
	activateBlocklistForTest(t, machine, 1)
	port := &blocklistServiceTestPort{stateMachinePort: &stateMachinePort{machine: machine, nextIndex: machine.LastAppliedIndex()}}
	clock := &blocklistServiceTestClock{now: time.UnixMilli(1_700_000_000_000)}
	service, err := NewBlocklistService(port, clock)
	if err != nil {
		t.Fatalf("NewBlocklistService(): %v", err)
	}
	return service, port, clock
}

func blocklistServiceTestErrorIs(t *testing.T, err error, want ServiceErrorCode) bool {
	t.Helper()
	var serviceError *ServiceError
	return errors.As(err, &serviceError) && serviceError.Code == want
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-043
func TestBlocklistServiceCommitsAndReportsPolicyVersion(t *testing.T) {
	service, port, clock := newBlocklistServiceTestFixture(t)
	entry, err := security.NewBlockEntry(security.BlockKindClientID, "tenant", "client")
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := service.Block(context.Background(), entry)
	if err != nil || !blocked.Blocked || blocked.Entry != entry || blocked.BlockedAtUnixMillis != uint64(clock.now.UnixMilli()) || blocked.PolicyVersion == 0 {
		t.Fatalf("Block() = %+v, %v", blocked, err)
	}
	if port.commitCalls != 1 {
		t.Fatalf("commits = %d, want 1", port.commitCalls)
	}
	listed, err := service.ListBlocklist(context.Background())
	if err != nil || listed.PolicyVersion != blocked.PolicyVersion || len(listed.Entries) != 1 || listed.Entries[0] != (BlocklistStateDTO{Entry: entry, BlockedAtUnixMillis: blocked.BlockedAtUnixMillis}) {
		t.Fatalf("ListBlocklist() = %+v, %v; want committed entry and version", listed, err)
	}

	clock.now = clock.now.Add(time.Minute)
	removed, err := service.Unblock(context.Background(), entry)
	if err != nil || removed.Blocked || removed.BlockedAtUnixMillis != 0 || removed.PolicyVersion <= blocked.PolicyVersion {
		t.Fatalf("Unblock() = %+v, %v; want an advanced policy version", removed, err)
	}
	listed, err = service.ListBlocklist(context.Background())
	if err != nil || len(listed.Entries) != 0 || listed.PolicyVersion != removed.PolicyVersion {
		t.Fatalf("ListBlocklist() after unblock = %+v, %v", listed, err)
	}
	if port.commitCalls != 2 {
		t.Fatalf("commits = %d, want 2", port.commitCalls)
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-040, tenancy-and-security.md:SEC-SCN-043
func TestBlocklistServiceRejectsInvalidEntryWithoutCommit(t *testing.T) {
	service, port, _ := newBlocklistServiceTestFixture(t)
	noncanonical := security.BlockEntry{Kind: security.BlockKindIPPrefix, Value: "203.0.113.5/24"}
	if _, err := service.Block(context.Background(), noncanonical); !blocklistServiceTestErrorIs(t, err, ServiceErrorInvalidRequest) {
		t.Fatalf("Block(noncanonical) error = %v, want invalid_request", err)
	}
	if port.commitCalls != 0 || len(port.machine.Blocklist().Entries) != 0 {
		t.Fatalf("invalid entry committed: calls=%d state=%+v", port.commitCalls, port.machine.Blocklist())
	}

	machine := NewStateMachine()
	activateBlocklistForTest(t, machine, 1)
	for index := range MaxBlocklistEntries {
		value := fmt.Sprintf("capacity-%04d", index)
		entry, err := security.NewBlockEntry(security.BlockKindUsername, "", value)
		if err != nil {
			t.Fatal(err)
		}
		sequence := uint64(index + 2)
		result, applyErr := machine.Apply(sequence, blocklistTestCommand(sequence, BlocklistActionBlock, entry, sequence))
		if applyErr != nil || result.Outcome != ApplyOutcomeApplied {
			t.Fatalf("fill capacity at %d = %+v, %v", index, result, applyErr)
		}
	}
	portAtCapacity := &blocklistServiceTestPort{stateMachinePort: &stateMachinePort{machine: machine, nextIndex: machine.LastAppliedIndex()}}
	capacityService, err := NewBlocklistService(portAtCapacity, &blocklistServiceTestClock{now: time.UnixMilli(1_700_000_000_000)})
	if err != nil {
		t.Fatal(err)
	}
	extra, err := security.NewBlockEntry(security.BlockKindUsername, "", "capacity-overflow")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capacityService.Block(context.Background(), extra); !blocklistServiceTestErrorIs(t, err, ServiceErrorBlocklistCapacity) {
		t.Fatalf("Block() at capacity error = %v, want blocklist_capacity_exceeded", err)
	}
	if portAtCapacity.commitCalls != 1 || len(machine.Blocklist().Entries) != MaxBlocklistEntries || machine.Blocklist().PolicyVersion != uint64(MaxBlocklistEntries+1) {
		t.Fatalf("capacity conflict changed state: calls=%d blocklist=%+v", portAtCapacity.commitCalls, machine.Blocklist())
	}
}
