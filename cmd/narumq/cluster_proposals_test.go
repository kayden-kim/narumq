package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/membership"
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

func TestReclaimUnsentProposalsStopsWhenDeferredRoomIsZero(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	follower := runtimes[1]
	message := tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: []byte("forwarded-proposal"), Reclaimable: true}
	if err := follower.transport.Send(message); err != nil {
		t.Fatalf("queue forwarded proposal %+v: %v", message, err)
	}
	if err := follower.node.Close(); err != nil {
		t.Fatalf("close follower Raft node: %v", err)
	}
	follower.reclaimUnsentProposals()
	status, err := follower.transport.Status(1)
	if err != nil {
		t.Fatalf("outbound peer status for peer 1: %v", err)
	}
	if status.OutboundQueuedMessages != 1 {
		t.Fatalf("queued outbound messages for peer 1 = %d, want 1 (status=%+v)", status.OutboundQueuedMessages, status)
	}
	frames, err := follower.transport.ReclaimUnsent(1, 1, 1<<20)
	if err != nil {
		t.Fatalf("ReclaimUnsent(1, 1, 1<<20): %v", err)
	}
	want := [][]byte{[]byte("forwarded-proposal")}
	if !reflect.DeepEqual(frames, want) {
		t.Fatalf("reclaimed frames = %q, want %q", frames, want)
	}
}
