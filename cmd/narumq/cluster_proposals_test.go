package main

import (
	"context"
	"narumq/internal/application/membership"
	"testing"
	"time"
)

func TestSafeRetirementFloorProgress(t *testing.T) {
	runtime := &clusterRuntime{}

	proposals := []clusterProposalWaiter{
		{requestID: "r1", proofIndex: 10, applied: false},
		{requestID: "r2", proofIndex: 15, applied: false},
		{requestID: "r3", proofIndex: 8, applied: false},
	}

	// Lowest unapplied proofIndex is 8. Safe floor for new command with proofIndex 20 is 8 - 1 = 7.
	floor := runtime.safeRetirementFloor(20, proposals)
	if floor != 7 {
		t.Fatalf("floor = %d, want 7", floor)
	}

	// Once r3 applies:
	proposals[2].applied = true
	// Lowest unapplied proofIndex is now 10. Safe floor for new command with proofIndex 25 is 10 - 1 = 9.
	floor = runtime.safeRetirementFloor(25, proposals)
	if floor != 9 {
		t.Fatalf("floor = %d, want 9", floor)
	}

	// A command already staged in this driver turn must also keep its receipt.
	staged := &clusterPendingCommit{context: context.Background(), readProofIndex: 5}
	floor = runtime.safeRetirementFloor(30, proposals, staged)
	if floor != 4 {
		t.Fatalf("staged command would be retired: floor=%d", floor)
	}
}

func TestHasUnappliedProposalsChecksTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	proposals := []clusterProposalWaiter{
		{requestID: "r1", applied: true},
		{requestID: "r2", context: ctx, applied: false}, // timed out
	}
	if hasUnappliedProposals(proposals) {
		t.Fatal("expected false when unapplied proposals are timed out")
	}

	liveCtx, liveCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer liveCancel()
	proposals = append(proposals, clusterProposalWaiter{requestID: "r3", context: liveCtx, applied: false, command: membership.CommandDTO{Proposal: &membership.ProposalIdentityDTO{}}})
	if !hasUnappliedProposals(proposals) {
		t.Fatal("expected true when live unapplied proposal exists")
	}
}
