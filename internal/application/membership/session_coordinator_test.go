package membership

import (
	"context"
	"errors"
	"testing"
)

func TestSessionCoordinatorCommitsEpochsAndFencesOldSubscriptions(t *testing.T) {
	service, port := newServiceFixture(t)
	ctx := context.Background()
	epoch, err := service.AcquireOwnership(ctx, "tenant", "device-1", 1)
	if err != nil || epoch != 1 {
		t.Fatalf("initial ownership epoch=(%d, %v), want 1", epoch, err)
	}
	if err := service.CommitSubscription(ctx, "tenant", "device-1", 1, epoch, "sensors/#", 1, false, false, 0, uint32(SubscriptionActionAdd)); err != nil {
		t.Fatalf("commit initial subscription: %v", err)
	}

	takeoverEpoch, err := service.AcquireOwnership(ctx, "tenant", "device-1", 2)
	if err != nil || takeoverEpoch != 2 {
		t.Fatalf("takeover ownership epoch=(%d, %v), want 2", takeoverEpoch, err)
	}
	if err := service.CommitSubscription(ctx, "tenant", "device-1", 1, epoch, "sensors/#", 0, false, false, 0, uint32(SubscriptionActionRemove)); err == nil {
		t.Fatal("old owner subscription mutation was accepted after takeover")
	} else {
		var serviceError *ServiceError
		if !errors.As(err, &serviceError) || serviceError.Code != ServiceErrorOperationConflict {
			t.Fatalf("stale subscription error=%v, want operation conflict", err)
		}
	}
	ownership, exists := port.machine.GetOwnership("tenant", "device-1")
	if !exists || ownership.OwnerNodeID != 2 || ownership.Epoch != 2 || ownership.CommittedIndex != 4 {
		t.Fatalf("takeover ownership=%+v exists=%t", ownership, exists)
	}
	subscriptions := port.machine.Subscriptions()
	if len(subscriptions) != 1 || subscriptions[0].OwnerNodeID != 2 || subscriptions[0].Epoch != 2 {
		t.Fatalf("ownership transfer did not rebind committed subscription metadata: %+v", subscriptions)
	}

	if err := service.CommitSubscription(ctx, "tenant", "device-1", 2, takeoverEpoch, "sensors/#", 1, true, true, 2, uint32(SubscriptionActionAdd)); err != nil {
		t.Fatalf("new owner subscription mutation: %v", err)
	}
	subscriptions = port.machine.Subscriptions()
	if len(subscriptions) != 1 || subscriptions[0].OwnerNodeID != 2 || subscriptions[0].Epoch != 2 || !subscriptions[0].NoLocal || !subscriptions[0].RetainAsPublished || subscriptions[0].RetainHandling != 2 {
		t.Fatalf("new owner subscription projection=%+v", subscriptions)
	}
	if port.machine.LastAppliedIndex() != 6 {
		t.Fatalf("committed application index=%d, want 6", port.machine.LastAppliedIndex())
	}
}
