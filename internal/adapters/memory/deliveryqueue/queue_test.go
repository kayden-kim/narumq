package deliveryqueue

import (
	"errors"
	"sync"
	"testing"

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
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "missing"}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
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
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events/1", Payload: firstPayload, QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	firstPayload[0] = 'X'
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events/2", Payload: []byte("second"), QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events/3", Payload: []byte("third")}); !errors.Is(err, applicationdeliveryqueue.ErrFull) {
		t.Fatalf("full enqueue error = %v, want %v", err, applicationdeliveryqueue.ErrFull)
	}

	drained, err := queue.Drain("session-a")
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
	drainedAgain, err := queue.Drain("session-a")
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
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events"}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	items, err := queue.Drain("session-a")
	if err != nil || len(items) != 1 {
		t.Fatalf("re-register drain = %#v, %v; want one item", items, err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events"}); err != nil {
		t.Fatal(err)
	}
	queue.Remove("session-a")
	if _, err := queue.Drain("session-a"); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
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
					})
				case 2:
					_, _ = queue.Drain(sessionID)
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
		if _, err := queue.Drain(sessionID); err != nil {
			t.Fatalf("final Drain(%q) error = %v", sessionID, err)
		}
	}
}
