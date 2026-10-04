package membership

import "testing"

// CLU-13 verifies selection and epoch-fenced transfer planning for connection rebalance.
func TestPlanRebalanceTransfer(t *testing.T) {
	const localNodeID = 7
	candidates := []OwnershipStateDTO{
		{TenantID: "tenant", ClientID: "foreign", OwnerNodeID: 8, Epoch: 3},
		{TenantID: "tenant", ClientID: "max-epoch", OwnerNodeID: localNodeID, Epoch: ^uint64(0)},
		{TenantID: "tenant", ClientID: "skipped", OwnerNodeID: localNodeID, Epoch: 4},
		{TenantID: "tenant", ClientID: "transfer", OwnerNodeID: localNodeID, Epoch: 9},
		{TenantID: "tenant", ClientID: "later", OwnerNodeID: localNodeID, Epoch: 10},
	}

	change, ok := PlanRebalanceTransfer(localNodeID, 2, candidates, func(candidate OwnershipStateDTO) bool {
		return candidate.ClientID == "skipped"
	})
	if !ok {
		t.Fatal("expected a transferable candidate")
	}
	want := OwnershipChangeDTO{
		TenantID:      "tenant",
		ClientID:      "transfer",
		ExpectedEpoch: 9,
		OwnerNodeID:   2,
		Epoch:         10,
		Action:        OwnershipActionTransfer,
	}
	if change != want {
		t.Fatalf("change = %+v, want %+v", change, want)
	}
}

func TestPlanRebalanceTransferWithoutEligibleCandidate(t *testing.T) {
	const localNodeID = 7
	candidates := []OwnershipStateDTO{
		{TenantID: "tenant", ClientID: "foreign", OwnerNodeID: 8, Epoch: 3},
		{TenantID: "tenant", ClientID: "max-epoch", OwnerNodeID: localNodeID, Epoch: ^uint64(0)},
	}
	if change, ok := PlanRebalanceTransfer(localNodeID, 2, candidates, nil); ok {
		t.Fatalf("unexpected transfer: %+v", change)
	}
}

func TestSelectRebalanceTarget(t *testing.T) {
	participants := []ConnectionRebalanceReportDTO{
		{NodeID: 9, Connections: 1}, // Lowest count, but disconnected.
		{NodeID: 7, Connections: 0}, // Local node is never a target.
		{NodeID: 5, Connections: 3},
		{NodeID: 3, Connections: 3},
		{NodeID: 11, Connections: 2}, // Not connected and not eligible.
	}
	connected := []uint64{3, 5, 7}
	target, ok := SelectRebalanceTarget(7, participants, connected)
	if !ok {
		t.Fatal("expected a connected non-local participant")
	}
	if target != 3 {
		t.Fatalf("target = %d, want 3 (lowest connection count, then lowest node ID)", target)
	}
}

func TestSelectRebalanceTargetExcludesNonParticipantsAndDisconnectedNodes(t *testing.T) {
	participants := []ConnectionRebalanceReportDTO{
		{NodeID: 2, Connections: 1},
		{NodeID: 4, Connections: 2},
	}
	if target, ok := SelectRebalanceTarget(1, participants, []uint64{3, 4}); !ok || target != 4 {
		t.Fatalf("target = %d, ok = %v; want 4, true", target, ok)
	}
	if target, ok := SelectRebalanceTarget(1, participants, []uint64{3}); ok {
		t.Fatalf("unexpected target %d without a connected participant", target)
	}
}
