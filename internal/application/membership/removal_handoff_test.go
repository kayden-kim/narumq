package membership

import (
	"reflect"
	"testing"
)

func TestPlanRemovalHandoffTransfersLeavingOwnerSessions(t *testing.T) {
	leavingID := uint64(3)
	voterIDs := []uint64{1, 2, 3, 4}
	ownerships := []OwnershipStateDTO{
		{TenantID: "a", ClientID: "x", OwnerNodeID: 3, Epoch: 2, CommittedIndex: 10},
		{TenantID: "a", ClientID: "y", OwnerNodeID: 1, Epoch: 5, CommittedIndex: 11},
	}
	subscriptions := []SubscriptionStateDTO{
		{TenantID: "a", ClientID: "x", OwnerNodeID: 3, Epoch: 2, Filter: "topic/#"},
		{TenantID: "a", ClientID: "y", OwnerNodeID: 1, Epoch: 5, Filter: "other/#"},
	}
	plan := PlanRemovalHandoff(leavingID, voterIDs, ownerships, subscriptions)
	if plan.Block != RemovalHandoffBlockNone {
		t.Fatalf("unexpected block: %s", plan.Block)
	}
	if plan.RemoveReady {
		t.Fatal("expected RemoveReady to be false when transfers exist")
	}
	if plan.SuccessorNodeID != 1 {
		t.Fatalf("expected successor 1, got %d", plan.SuccessorNodeID)
	}
	wantTransfers := []OwnershipChangeDTO{
		{
			TenantID:      "a",
			ClientID:      "x",
			ExpectedEpoch: 2,
			OwnerNodeID:   1,
			Epoch:         3,
			Action:        OwnershipActionTransfer,
		},
	}
	if !reflect.DeepEqual(plan.Transfers, wantTransfers) {
		t.Fatalf("transfers = %+v, want %+v", plan.Transfers, wantTransfers)
	}
}

func TestPlanRemovalHandoffTransferAcceptedByStateMachine(t *testing.T) {
	machine := NewStateMachine()
	// Initial setup: Node 3 acquires session a/x at epoch 1 then re-acquires at epoch 2
	applyMustSucceed(t, machine, 1, CommandDTO{
		SchemaMajor: 1, OperationID: testOperation4,
		Ownership: &OwnershipChangeDTO{
			TenantID: "a", ClientID: "x", ExpectedEpoch: 0, OwnerNodeID: 3, Epoch: 1, Action: OwnershipActionAcquire,
		},
	})
	applyMustSucceed(t, machine, 2, CommandDTO{
		SchemaMajor: 1, OperationID: testOperation5,
		Ownership: &OwnershipChangeDTO{
			TenantID: "a", ClientID: "x", ExpectedEpoch: 1, OwnerNodeID: 3, Epoch: 2, Action: OwnershipActionAcquire,
		},
	})
	applyMustSucceed(t, machine, 3, CommandDTO{
		SchemaMajor: 1, OperationID: testOperation6,
		Subscription: &SubscriptionChangeDTO{
			TenantID: "a", ClientID: "x", OwnerNodeID: 3, Epoch: 2, Filter: "topic/#", QoS: 0,
			Action: SubscriptionActionAdd,
		},
	})

	leavingID := uint64(3)
	voterIDs := []uint64{1, 2, 3, 4}

	plan := PlanRemovalHandoff(leavingID, voterIDs, machine.Ownerships(), machine.Subscriptions())
	if len(plan.Transfers) != 1 {
		t.Fatalf("expected 1 transfer, got %d", len(plan.Transfers))
	}

	// Apply the transfer through machine
	transferCmd := CommandDTO{
		SchemaMajor: 1, OperationID: testOperation7,
		Ownership: &plan.Transfers[0],
	}
	res := applyMustSucceed(t, machine, 4, transferCmd)
	if res.Outcome != ApplyOutcomeApplied {
		t.Fatalf("expected Applied, got %v", res.Outcome)
	}

	subs := machine.Subscriptions()
	if len(subs) != 1 || subs[0].OwnerNodeID != 1 || subs[0].Epoch != 3 {
		t.Fatalf("subscription not rebound: %+v", subs)
	}

	replan := PlanRemovalHandoff(leavingID, voterIDs, machine.Ownerships(), machine.Subscriptions())
	if replan.Block != RemovalHandoffBlockNone {
		t.Fatalf("unexpected block after transfer: %s", replan.Block)
	}
	if !replan.RemoveReady {
		t.Fatal("expected RemoveReady=true after transfer")
	}
}

func TestPlanRemovalHandoffBlocksRemoval(t *testing.T) {
	cases := []struct {
		name          string
		leavingID     uint64
		voterIDs      []uint64
		ownerships    []OwnershipStateDTO
		subscriptions []SubscriptionStateDTO
		wantBlock     RemovalHandoffBlock
	}{
		{
			name:      "no successor",
			leavingID: 3,
			voterIDs:  []uint64{3},
			wantBlock: RemovalHandoffBlockNoSuccessor,
		},
		{
			name:      "no successor",
			leavingID: 3,
			voterIDs:  []uint64{3},
			wantBlock: RemovalHandoffBlockNoSuccessor,
		},
		{
			name:      "epoch exhausted",
			leavingID: 3,
			voterIDs:  []uint64{1, 2, 3},
			ownerships: []OwnershipStateDTO{
				{TenantID: "t", ClientID: "c", OwnerNodeID: 3, Epoch: ^uint64(0)},
			},
			wantBlock: RemovalHandoffBlockEpochExhausted,
		},
		{
			name:      "orphan subscription",
			leavingID: 3,
			voterIDs:  []uint64{1, 2, 3},
			subscriptions: []SubscriptionStateDTO{
				{TenantID: "t", ClientID: "c", OwnerNodeID: 3, Epoch: 1, Filter: "f/#"},
			},
			wantBlock: RemovalHandoffBlockOrphanSubscription,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := PlanRemovalHandoff(tc.leavingID, tc.voterIDs, tc.ownerships, tc.subscriptions)
			if plan.Block != tc.wantBlock {
				t.Fatalf("Block = %q, want %q", plan.Block, tc.wantBlock)
			}
			if len(plan.Transfers) != 0 {
				t.Fatalf("expected no transfers when blocked, got: %+v", plan.Transfers)
			}
			if plan.RemoveReady {
				t.Fatal("expected RemoveReady to be false when blocked")
			}
		})
	}
}
