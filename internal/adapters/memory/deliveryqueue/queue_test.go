package deliveryqueue

import (
	"errors"
	"sync"
	"testing"
	"time"

	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestNewAndRegisterValidateBounds(t *testing.T) {
	if _, err := New(0); !errors.Is(err, ErrInvalidCapacity) {
		t.Fatalf("New(0) error = %v, want %v", err, ErrInvalidCapacity)
	}
	queue, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register(""); !errors.Is(err, routing.ErrInvalidSessionID) {
		t.Fatalf("Register(\"\") error = %v, want %v", err, routing.ErrInvalidSessionID)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "missing"}, time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("unregistered enqueue error = %v, want %v", err, applicationdeliveryqueue.ErrUnavailable)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestQueueCapacityFIFOAndPayloadOwnership(t *testing.T) {
	queue, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	firstPayload := []byte("first")
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events/1", Payload: firstPayload, QoS: mqtt.QoSAtMostOnce}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	firstPayload[0] = 'X'
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events/2", Payload: []byte("second"), QoS: mqtt.QoSAtLeastOnce}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events/3", Payload: []byte("third")}, time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrFull) {
		t.Fatalf("full enqueue error = %v, want %v", err, applicationdeliveryqueue.ErrFull)
	}

	drained, err := queue.Drain("session-a", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(drained) != 2 || drained[0].Topic != "events/1" || drained[1].Topic != "events/2" {
		t.Fatalf("drain = %#v, want FIFO events/1,events/2", drained)
	}
	if string(drained[0].Payload) != "first" {
		t.Fatalf("stored payload changed after caller mutation: %q", drained[0].Payload)
	}
	drained[0].Payload[0] = 'Y'
	drainedAgain, err := queue.Drain("session-a", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(drainedAgain) != 0 {
		t.Fatalf("second drain = %#v, want empty", drainedAgain)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestQueueRegisterPreservesAndRemoveMakesUnavailable(t *testing.T) {
	queue, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events"}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	items, err := queue.Drain("session-a", time.Time{})
	if err != nil || len(items) != 1 {
		t.Fatalf("re-register drain = %#v, %v; want one item", items, err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events"}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	queue.Remove("session-a")
	if _, err := queue.Drain("session-a", time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("removed drain error = %v, want %v", err, applicationdeliveryqueue.ErrUnavailable)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestQueueConcurrentRegisterEnqueueAndDrain(t *testing.T) {
	queue, err := New(32)
	if err != nil {
		t.Fatal(err)
	}
	sessionIDs := []routing.SessionID{"session-a", "session-b", "session-c", "session-d"}
	for _, sessionID := range sessionIDs {
		if err := queue.Register(sessionID); err != nil {
			t.Fatal(err)
		}
	}

	var waitGroup sync.WaitGroup
	for worker := range sessionIDs {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			sessionID := sessionIDs[worker]
			for iteration := 0; iteration < 250; iteration++ {
				switch iteration % 4 {
				case 0:
					_ = queue.Register(sessionID)
				case 1:
					_ = queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
						SessionID: sessionID,
						Topic:     "events/concurrent",
						Payload:   []byte{byte(iteration)},
					}, time.Time{})
				case 2:
					_, _ = queue.Drain(sessionID, time.Time{})
				default:
					queue.Remove(sessionID)
					_ = queue.Register(sessionID)
				}
			}
		}(worker)
	}
	waitGroup.Wait()

	for _, sessionID := range sessionIDs {
		if err := queue.Register(sessionID); err != nil {
			t.Fatal(err)
		}
		if _, err := queue.Drain(sessionID, time.Time{}); err != nil {
			t.Fatalf("final Drain(%q) error = %v", sessionID, err)
		}
	}
}
func TestNilQueueAndInvalidSessionOperations(t *testing.T) {
	var queue *Queue
	queue.Remove("session")
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session"}, time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil TryEnqueue() error = %v", err)
	}
	if _, err := queue.Drain("session", time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil Drain() error = %v", err)
	}
	valid, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := valid.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: ""}, time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("empty TryEnqueue() error = %v", err)
	}
	if _, err := valid.Drain(routing.SessionID(""), time.Time{}); !errors.Is(err, routing.ErrInvalidSessionID) {
		t.Fatalf("empty Drain() error = %v", err)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestQueueBatchEnqueueIsAtomicAndOwnsPayload(t *testing.T) {
	queue, err := New(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	firstPayload := []byte("first")
	secondPayload := []byte("second")
	batch := []applicationdeliveryqueue.DeliveryDTO{
		{SessionID: "session-a", Topic: "events/1", Payload: firstPayload},
		{SessionID: "session-a", Topic: "events/2", Payload: secondPayload},
	}
	if err := queue.TryEnqueueBatch(batch, time.Time{}); err != nil {
		t.Fatal(err)
	}
	firstPayload[0] = 'X'
	secondPayload[0] = 'Y'
	drained, err := queue.Drain("session-a", time.Time{})
	if err != nil || len(drained) != 2 || string(drained[0].Payload) != "first" || string(drained[1].Payload) != "second" {
		t.Fatalf("batch drain = %#v, %v", drained, err)
	}

	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "existing"}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	overflow := []applicationdeliveryqueue.DeliveryDTO{
		{SessionID: "session-a", Topic: "events/3"},
		{SessionID: "session-a", Topic: "events/4"},
		{SessionID: "session-a", Topic: "events/5"},
	}
	if err := queue.TryEnqueueBatch(overflow, time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrFull) {
		t.Fatalf("full batch error = %v, want %v", err, applicationdeliveryqueue.ErrFull)
	}
	drained, err = queue.Drain("session-a", time.Time{})
	if err != nil || len(drained) != 1 || drained[0].Topic != "existing" {
		t.Fatalf("full batch partially appended = %#v, %v", drained, err)
	}
	if err := queue.TryEnqueueBatch([]applicationdeliveryqueue.DeliveryDTO{
		{SessionID: "session-a", Topic: "valid"},
		{SessionID: "session-b", Topic: "mixed"},
	}, time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("mixed batch error = %v, want %v", err, applicationdeliveryqueue.ErrUnavailable)
	}
}

// Traceability: PRD MQTT-08/MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §3.2, §5, §7; MQTT-3.3.2-5/6.
func TestQueueExpiryPurgesAtDeadlineAndRoundsRemainingInterval(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	delivery := applicationdeliveryqueue.DeliveryDTO{
		SessionID: "session-a",
		Topic:     "events/expiry",
		Payload:   []byte("payload"),
		ExpiresAt: now.Add(1500 * time.Millisecond),
	}
	if delivery.Expired(now) {
		t.Fatal("delivery expired before its deadline")
	}
	if remaining, ok := delivery.RemainingExpiryInterval(now.Add(600 * time.Millisecond)); !ok || remaining != 1 {
		t.Fatalf("remaining expiry = (%d, %v), want (1, true)", remaining, ok)
	}
	if delivery.Expired(now.Add(1499 * time.Millisecond)) {
		t.Fatal("delivery expired before exact deadline")
	}
	if !delivery.Expired(now.Add(1500 * time.Millisecond)) {
		t.Fatal("delivery did not expire at exact deadline")
	}

	queue, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(delivery, now); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: "session-a",
		Topic:     "events/replacement",
		Payload:   []byte("replacement"),
	}, now.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("enqueue after expiry = %v", err)
	}
	items, err := queue.Drain("session-a", now.Add(1500*time.Millisecond))
	if err != nil || len(items) != 1 || items[0].Topic != "events/replacement" {
		t.Fatalf("drain after expiry = %#v, %v", items, err)
	}
}
