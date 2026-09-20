package deliveryqueue

import (
	"testing"
	"time"
)

// Traceability: PRD MQTT-08/TEST-04/TEST-05; docs/specs/mqtt-protocol-and-session.md §5; MQTT-3.3.2-6.
func TestDeliveryDTOExpiryContract(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	withoutExpiry := DeliveryDTO{}
	if withoutExpiry.Expired(now) {
		t.Fatal("zero expiry unexpectedly expired")
	}
	if remaining, ok := withoutExpiry.RemainingExpiryInterval(now); ok || remaining != 0 {
		t.Fatalf("zero expiry remaining = (%d, %v), want (0, false)", remaining, ok)
	}

	deadline := now.Add(1500 * time.Millisecond)
	beforeDeadline := DeliveryDTO{ExpiresAt: deadline}
	if beforeDeadline.Expired(now) {
		t.Fatal("delivery expired before its deadline")
	}
	if remaining, ok := beforeDeadline.RemainingExpiryInterval(now.Add(600 * time.Millisecond)); !ok || remaining != 1 {
		t.Fatalf("sub-second remaining = (%d, %v), want (1, true)", remaining, ok)
	}

	exactDeadline := DeliveryDTO{ExpiresAt: deadline}
	if !exactDeadline.Expired(deadline) {
		t.Fatal("delivery was not expired at its exact deadline")
	}
	if remaining, ok := exactDeadline.RemainingExpiryInterval(deadline); !ok || remaining != 0 {
		t.Fatalf("exact deadline remaining = (%d, %v), want (0, true)", remaining, ok)
	}

	maxInterval := uint64(^uint32(0))
	saturated := DeliveryDTO{ExpiresAt: now.Add(time.Duration(maxInterval+1) * time.Second)}
	if remaining, ok := saturated.RemainingExpiryInterval(now); !ok || remaining != ^uint32(0) {
		t.Fatalf("saturated remaining = (%d, %v), want (%d, true)", remaining, ok, ^uint32(0))
	}
}
