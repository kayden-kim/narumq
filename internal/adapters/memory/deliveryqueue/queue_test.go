package deliveryqueue

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	domaindelivery "narumq/internal/domain/delivery"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

func testNow() time.Time {
	return time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
}

func outboundFor(t *testing.T, sessionID routing.SessionID, packetID mqtt.PacketIdentifier, expiresAt time.Time) applicationdeliveryqueue.OutboundDTO {
	t.Helper()
	state, _, err := domaindelivery.NewOutbound(mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "events",
		Payload:  []byte("payload"),
		QoS:      mqtt.QoSAtLeastOnce,
		PacketID: packetID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return applicationdeliveryqueue.OutboundDTO{
		SessionID: sessionID,
		PacketID:  packetID,
		State:     state,
		ExpiresAt: expiresAt,
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §11.25.
func TestBeginDetachRejectsLocalEnqueueUntilFinish(t *testing.T) {
	queue, err := New(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("s"); err != nil {
		t.Fatal(err)
	}
	now := testNow()
	a := applicationdeliveryqueue.DeliveryDTO{SessionID: "s", Topic: "a", Payload: []byte("original")}
	b := applicationdeliveryqueue.DeliveryDTO{SessionID: "s", Topic: "b"}
	durable := applicationdeliveryqueue.DeliveryDTO{SessionID: "s", Topic: "durable", QoS: mqtt.QoSAtLeastOnce, DurableSequence: 1, DurableEpoch: 1}
	if err := queue.TryEnqueue(a, now); err != nil {
		t.Fatal(err)
	}
	pending, err := queue.BeginDetach("s", now)
	if err != nil || !reflect.DeepEqual(pending, []applicationdeliveryqueue.DeliveryDTO{a}) {
		t.Fatalf("BeginDetach = %+v, %v", pending, err)
	}
	pending[0].Payload[0] = 'X'
	if err := queue.TryEnqueue(b, now); !errors.Is(err, applicationdeliveryqueue.ErrDetaching) {
		t.Fatalf("local enqueue = %v", err)
	}
	if err := queue.TryEnqueue(durable, now); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.BeginDetach("s", now); !errors.Is(err, applicationdeliveryqueue.ErrDetaching) {
		t.Fatalf("second BeginDetach = %v", err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := queue.AwaitDetach(ctx, "s"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired wait = %v", err)
	}
	queue.FinishDetach("s", false)
	if err := queue.AwaitDetach(ctx, "s"); err != nil {
		t.Fatalf("finished wait = %v", err)
	}
	if err := queue.TryEnqueue(b, now); err != nil {
		t.Fatal(err)
	}
	queued, err := queue.ListQueued("s", now)
	if err != nil || !reflect.DeepEqual(queued, []applicationdeliveryqueue.DeliveryDTO{a, durable, b}) {
		t.Fatalf("resumed FIFO = %+v, %v", queued, err)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §11.25.
func TestFinishDetachCommittedRemovesAndWakesWaiters(t *testing.T) {
	testDetachWaiterWake(t, true)
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §11.25.
func TestRemoveWakesDetachWaiters(t *testing.T) {
	testDetachWaiterWake(t, false)
}

func testDetachWaiterWake(t *testing.T, finish bool) {
	t.Helper()
	queue, err := New(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("s"); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.BeginDetach("s", testNow()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	// Done is evaluated only after AwaitDetach captures the active gate.
	// This proves that the waiter started before session removal.
	waitCtx := &detachWaitContext{Context: ctx, entered: make(chan struct{})}
	go func() { result <- queue.AwaitDetach(waitCtx, "s") }()
	select {
	case <-waitCtx.entered:
	case <-ctx.Done():
		t.Fatal("AwaitDetach did not enter its wait")
	}
	if finish {
		queue.FinishDetach("s", true)
	} else {
		queue.Remove("s")
	}
	if err := <-result; err != nil {
		t.Fatalf("wait result = %v", err)
	}
	if _, err := queue.ListQueued("s", testNow()); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("removed ListQueued = %v", err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "s", Topic: "b"}, testNow()); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("removed enqueue = %v", err)
	}
}

type detachWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *detachWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

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

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §3.2, §7.
func TestQueueFIFOAndPayloadOwnership(t *testing.T) {
	queue, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	firstPayload := []byte("first")
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events/1", Payload: firstPayload}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	firstPayload[0] = 'X'
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events/2", Payload: []byte("second")}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "events/3"}, time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrFull) {
		t.Fatalf("full enqueue error = %v, want %v", err, applicationdeliveryqueue.ErrFull)
	}

	head, ok, err := queue.Peek("session-a", time.Time{})
	if err != nil || !ok || head.Topic != "events/1" || string(head.Payload) != "first" {
		t.Fatalf("first Peek() = %#v, %v, %v", head, ok, err)
	}
	head.Payload[0] = 'Y'
	again, ok, err := queue.Peek("session-a", time.Time{})
	if err != nil || !ok || string(again.Payload) != "first" {
		t.Fatalf("Peek payload ownership = %#v, %v, %v", again, ok, err)
	}
	if err := queue.CompleteHead("session-a"); err != nil {
		t.Fatal(err)
	}
	next, ok, err := queue.Peek("session-a", time.Time{})
	if err != nil || !ok || next.Topic != "events/2" {
		t.Fatalf("second Peek() = %#v, %v, %v", next, ok, err)
	}
	if err := queue.CompleteHead("session-a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := queue.Peek("session-a", time.Time{}); err != nil || ok {
		t.Fatalf("empty Peek() = ok %v, err %v, want empty without error", ok, err)
	}
}

// Traceability: PRD MQTT-08/MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §3.2, §7; MQTT-3.3.2-5/6.
func TestQueueExpiryPurgeAndBatchAtomicity(t *testing.T) {
	now := testNow()
	queue, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueueBatch([]applicationdeliveryqueue.DeliveryDTO{
		{SessionID: "session-a", Topic: "expired", ExpiresAt: now},
		{SessionID: "session-a", Topic: "live", ExpiresAt: now.Add(time.Second)},
	}, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "replacement"}, now); err != nil {
		t.Fatalf("enqueue after expiry purge = %v", err)
	}
	head, ok, err := queue.Peek("session-a", now)
	if err != nil || !ok || head.Topic != "live" {
		t.Fatalf("Peek after expiry purge = %#v, %v, %v", head, ok, err)
	}
	if err := queue.TryEnqueueBatch([]applicationdeliveryqueue.DeliveryDTO{
		{SessionID: "session-a", Topic: "replacement"},
		{SessionID: "session-a", Topic: "overflow"},
	}, now); !errors.Is(err, applicationdeliveryqueue.ErrFull) {
		t.Fatalf("atomic overflow error = %v, want %v", err, applicationdeliveryqueue.ErrFull)
	}
	if err := queue.CompleteHead("session-a"); err != nil {
		t.Fatal(err)
	}
	next, ok, err := queue.Peek("session-a", now)
	if err != nil || !ok || next.Topic != "replacement" {
		t.Fatalf("replacement after purge = %#v, %v, %v", next, ok, err)
	}
}

// Traceability: PRD MQTT-02/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §3.2.
func TestQueuePromoteHeadExpiryAndAtomicFailures(t *testing.T) {
	now := testNow()
	queue, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueueBatch([]applicationdeliveryqueue.DeliveryDTO{
		{SessionID: "session-a", Topic: "expired", ExpiresAt: now},
		{SessionID: "session-a", Topic: "live"},
	}, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := queue.PromoteHead(outboundFor(t, "session-a", 1, now), now); !errors.Is(err, applicationdeliveryqueue.ErrExpired) {
		t.Fatalf("expired PromoteHead() error = %v, want %v", err, applicationdeliveryqueue.ErrExpired)
	}
	head, ok, err := queue.Peek("session-a", now)
	if err != nil || !ok || head.Topic != "live" {
		t.Fatalf("head after expired promotion = %#v, %v, %v", head, ok, err)
	}

	if err := queue.PromoteHead(outboundFor(t, "session-a", 7, time.Time{}), now); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "next"}, now); err != nil {
		t.Fatal(err)
	}
	if err := queue.PromoteHead(outboundFor(t, "session-a", 7, time.Time{}), now); !errors.Is(err, applicationdeliveryqueue.ErrPacketIDConflict) {
		t.Fatalf("duplicate PromoteHead() error = %v, want %v", err, applicationdeliveryqueue.ErrPacketIDConflict)
	}
	head, ok, err = queue.Peek("session-a", now)
	if err != nil || !ok || head.Topic != "next" {
		t.Fatalf("duplicate promotion popped head = %#v, %v, %v", head, ok, err)
	}
}

// Traceability: PRD MQTT-02/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §3.2, §7.
func TestQueueOutboundBoundSortingAndRemoval(t *testing.T) {
	now := testNow()
	queue, err := New(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []mqtt.PacketIdentifier{9, 2, 7} {
		if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "queued"}, now); err != nil {
			t.Fatal(err)
		}
		if err := queue.PromoteHead(outboundFor(t, "session-a", id, time.Time{}), now); err != nil {
			t.Fatal(err)
		}
	}
	outbound, err := queue.ListOutbound("session-a")
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := []mqtt.PacketIdentifier{outbound[0].PacketID, outbound[1].PacketID, outbound[2].PacketID}
	if want := []mqtt.PacketIdentifier{2, 7, 9}; !reflect.DeepEqual(gotIDs, want) {
		t.Fatalf("ListOutbound IDs = %v, want %v", gotIDs, want)
	}
	updated := outbound[1]
	if err := queue.ReplaceOutbound(updated); err != nil {
		t.Fatal(err)
	}
	if err := queue.ReplaceOutbound(outboundFor(t, "session-a", 11, time.Time{})); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("missing ReplaceOutbound() error = %v, want unavailable", err)
	}
	if err := queue.CompleteOutbound("session-a", 7); err != nil {
		t.Fatal(err)
	}
	if err := queue.CompleteOutbound("session-a", 7); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("missing CompleteOutbound() error = %v, want unavailable", err)
	}
	queue.Register("session-a")
	if outbound, err := queue.ListOutbound("session-a"); err != nil || len(outbound) != 2 {
		t.Fatalf("re-register outbound state = %#v, %v", outbound, err)
	}
	queue.Remove("session-a")
	if _, err := queue.ListOutbound("session-a"); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("removed ListOutbound() error = %v, want unavailable", err)
	}
}

// Traceability: CLU-05/CLU-06; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestQueueResetDurableReplacesStateAndBoundsLocalQueue(t *testing.T) {
	now := testNow()
	queue, err := New(3)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := routing.SessionID("session-durable")
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}

	oldDurable := applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "old/outbound", Payload: []byte("old"),
		DurableSequence: 1, DurableEpoch: 1,
	}
	if err := queue.TryEnqueue(oldDurable, now); err != nil {
		t.Fatal(err)
	}
	if err := queue.PromoteHead(outboundFor(t, sessionID, 1, time.Time{}), now); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: sessionID, Topic: "local/outbound"}, now); err != nil {
		t.Fatal(err)
	}
	if err := queue.PromoteHead(outboundFor(t, sessionID, 2, time.Time{}), now); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueueBatch([]applicationdeliveryqueue.DeliveryDTO{
		{SessionID: sessionID, Topic: "old/queued", DurableSequence: 2, DurableEpoch: 1},
		{SessionID: sessionID, Topic: "local/keep"},
		{SessionID: sessionID, Topic: "local/drop"},
	}, now); err != nil {
		t.Fatal(err)
	}

	durableQueued := []applicationdeliveryqueue.DeliveryDTO{
		{SessionID: sessionID, Topic: "new/one", Payload: []byte("one"), DurableSequence: 3, DurableEpoch: 2},
		{SessionID: sessionID, Topic: "expired", DurableSequence: 4, DurableEpoch: 2, ExpiresAt: now.Add(-time.Second)},
		{SessionID: sessionID, Topic: "new/two", DurableSequence: 5, DurableEpoch: 2},
	}
	durableOutbound := outboundFor(t, sessionID, 3, time.Time{})
	durableOutbound.DurableSequence, durableOutbound.DurableEpoch = 1, 2
	if err := queue.ResetDurable(sessionID, 2, durableQueued, []applicationdeliveryqueue.OutboundDTO{durableOutbound}, now); err != nil {
		t.Fatalf("ResetDurable() error = %v", err)
	}
	durableQueued[0].Payload[0] = 'X'

	queued, err := queue.ListQueued(sessionID, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 3 ||
		queued[0].Topic != "new/one" || queued[0].DurableSequence != 3 || queued[0].DurableEpoch != 2 || string(queued[0].Payload) != "one" ||
		queued[1].Topic != "new/two" || queued[1].DurableSequence != 5 ||
		queued[2].Topic != "local/keep" || queued[2].DurableSequence != 0 {
		t.Fatalf("ResetDurable() queued order/state = %+v", queued)
	}
	outbound, err := queue.ListOutbound(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(outbound) != 2 ||
		outbound[0].PacketID != 2 || outbound[0].DurableEpoch != 0 ||
		outbound[1].PacketID != 3 || outbound[1].DurableSequence != 1 || outbound[1].DurableEpoch != 2 {
		t.Fatalf("ResetDurable() outbound replacement = %+v", outbound)
	}
}

// Traceability: CLU-05/CLU-06; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestQueueResetDurablePacketIDConflictIsAtomic(t *testing.T) {
	now := testNow()
	queue, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := routing.SessionID("session-durable")
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: sessionID, Topic: "local/queued"}, now); err != nil {
		t.Fatal(err)
	}
	localOutbound := outboundFor(t, sessionID, 7, time.Time{})
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: sessionID, Topic: "local/outbound"}, now); err != nil {
		t.Fatal(err)
	}
	if err := queue.PromoteHead(localOutbound, now); err != nil {
		t.Fatal(err)
	}
	beforeQueued, err := queue.ListQueued(sessionID, now)
	if err != nil {
		t.Fatal(err)
	}
	beforeOutbound, err := queue.ListOutbound(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	conflicting := outboundFor(t, sessionID, 7, time.Time{})
	conflicting.DurableSequence, conflicting.DurableEpoch = 1, 2
	if err := queue.ResetDurable(sessionID, 2, []applicationdeliveryqueue.DeliveryDTO{
		{SessionID: sessionID, Topic: "durable", DurableSequence: 1, DurableEpoch: 2},
	}, []applicationdeliveryqueue.OutboundDTO{conflicting}, now); !errors.Is(err, applicationdeliveryqueue.ErrPacketIDConflict) {
		t.Fatalf("ResetDurable() conflict error = %v, want packet identifier conflict", err)
	}
	afterQueued, err := queue.ListQueued(sessionID, now)
	if err != nil {
		t.Fatal(err)
	}
	afterOutbound, err := queue.ListOutbound(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterQueued, beforeQueued) || !reflect.DeepEqual(afterOutbound, beforeOutbound) {
		t.Fatalf("failed ResetDurable() mutated queue: before=(%+v,%+v) after=(%+v,%+v)",
			beforeQueued, beforeOutbound, afterQueued, afterOutbound)
	}
}

func TestQueueOutboundCapacityLeavesHeadOnFailure(t *testing.T) {
	now := testNow()
	queue, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session-a"); err != nil {
		t.Fatal(err)
	}
	if err := queue.PromoteHead(outboundFor(t, "session-a", 1, time.Time{}), now); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("empty PromoteHead() error = %v, want unavailable", err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "first"}, now); err != nil {
		t.Fatal(err)
	}
	if err := queue.PromoteHead(outboundFor(t, "session-a", 1, time.Time{}), now); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session-a", Topic: "blocked"}, now); err != nil {
		t.Fatal(err)
	}
	if err := queue.PromoteHead(outboundFor(t, "session-a", 2, time.Time{}), now); !errors.Is(err, applicationdeliveryqueue.ErrFull) {
		t.Fatalf("bounded PromoteHead() error = %v, want %v", err, applicationdeliveryqueue.ErrFull)
	}
	head, ok, err := queue.Peek("session-a", now)
	if err != nil || !ok || head.Topic != "blocked" {
		t.Fatalf("full promotion popped head = %#v, %v, %v", head, ok, err)
	}
}

func TestQueueConcurrentStateAccess(t *testing.T) {
	queue, err := New(32)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register("session"); err != nil {
		t.Fatal(err)
	}
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			for iteration := 0; iteration < 100; iteration++ {
				now := testNow()
				if iteration%4 == 0 {
					_ = queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session", Payload: []byte{byte(worker), byte(iteration)}}, now)
				} else if iteration%4 == 1 {
					if _, _, err := queue.Peek("session", now); err != nil && !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
						t.Errorf("Peek() error = %v", err)
					}
				} else if iteration%4 == 2 {
					_ = queue.CompleteHead("session")
				} else {
					_ = queue.Register("session")
				}
			}
		}(worker)
	}
	waitGroup.Wait()
	if err := queue.Register("session"); err != nil {
		t.Fatal(err)
	}
}

func TestNilQueueAndInvalidOperations(t *testing.T) {
	var queue *Queue
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: "session"}, time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil TryEnqueue() error = %v", err)
	}
	if _, _, err := queue.Peek("session", time.Time{}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil Peek() error = %v", err)
	}
	valid, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := valid.Peek(routing.SessionID(""), time.Time{}); !errors.Is(err, routing.ErrInvalidSessionID) {
		t.Fatalf("empty Peek() error = %v", err)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §3.2, §7.
func TestQueueUnavailableAndInvalidOperations(t *testing.T) {
	now := testNow()
	sessionID := routing.SessionID("session-a")
	outbound := outboundFor(t, sessionID, 1, time.Time{})

	var nilQueue *Queue
	if err := nilQueue.Register(sessionID); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil Register() error = %v, want unavailable", err)
	}
	nilQueue.Remove(sessionID)
	if err := nilQueue.TryEnqueueBatch(nil, now); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil TryEnqueueBatch() error = %v, want unavailable", err)
	}
	if err := nilQueue.CompleteHead(sessionID); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil CompleteHead() error = %v, want unavailable", err)
	}
	if err := nilQueue.PromoteHead(outbound, now); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil PromoteHead() error = %v, want unavailable", err)
	}
	if _, err := nilQueue.ListOutbound(sessionID); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil ListOutbound() error = %v, want unavailable", err)
	}
	if err := nilQueue.ReplaceOutbound(outbound); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil ReplaceOutbound() error = %v, want unavailable", err)
	}
	if err := nilQueue.CompleteOutbound(sessionID, outbound.PacketID); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil CompleteOutbound() error = %v, want unavailable", err)
	}

	queue, err := New(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.CompleteHead(""); !errors.Is(err, routing.ErrInvalidSessionID) {
		t.Fatalf("invalid CompleteHead() error = %v, want invalid session", err)
	}
	if err := queue.PromoteHead(applicationdeliveryqueue.OutboundDTO{SessionID: sessionID}, now); !errors.Is(err, applicationdeliveryqueue.ErrPacketIDConflict) {
		t.Fatalf("zero PacketID PromoteHead() error = %v, want conflict", err)
	}
	if err := queue.PromoteHead(applicationdeliveryqueue.OutboundDTO{SessionID: "", PacketID: 1}, now); !errors.Is(err, routing.ErrInvalidSessionID) {
		t.Fatalf("invalid PromoteHead() error = %v, want invalid session", err)
	}
	if _, err := queue.ListOutbound(""); !errors.Is(err, routing.ErrInvalidSessionID) {
		t.Fatalf("invalid ListOutbound() error = %v, want invalid session", err)
	}
	if err := queue.ReplaceOutbound(applicationdeliveryqueue.OutboundDTO{SessionID: "", PacketID: 1}); !errors.Is(err, routing.ErrInvalidSessionID) {
		t.Fatalf("invalid ReplaceOutbound() error = %v, want invalid session", err)
	}
	if err := queue.CompleteOutbound("", 1); !errors.Is(err, routing.ErrInvalidSessionID) {
		t.Fatalf("invalid CompleteOutbound() error = %v, want invalid session", err)
	}
	if err := queue.TryEnqueueBatch([]applicationdeliveryqueue.DeliveryDTO{{SessionID: ""}}, now); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("invalid TryEnqueueBatch() error = %v, want unavailable", err)
	}
	if err := queue.ReplaceOutbound(applicationdeliveryqueue.OutboundDTO{SessionID: sessionID, PacketID: 0}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("zero ReplaceOutbound() error = %v, want unavailable", err)
	}
	if err := queue.CompleteHead("missing"); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("missing CompleteHead() error = %v, want unavailable", err)
	}
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}
	if err := queue.TryEnqueueBatch([]applicationdeliveryqueue.DeliveryDTO{{SessionID: "missing"}}, now); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("missing TryEnqueueBatch() error = %v, want unavailable", err)
	}
	if err := queue.TryEnqueueBatch(nil, now); err != nil {
		t.Fatalf("empty TryEnqueueBatch() error = %v", err)
	}
	if err := queue.TryEnqueueBatch([]applicationdeliveryqueue.DeliveryDTO{
		{SessionID: sessionID, Topic: "expired", ExpiresAt: now},
	}, now); err != nil {
		t.Fatalf("expired-only TryEnqueueBatch() error = %v", err)
	}
	if err := queue.TryEnqueueBatch([]applicationdeliveryqueue.DeliveryDTO{
		{SessionID: sessionID, Topic: "valid"},
		{SessionID: ""},
	}, now); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("mixed-session TryEnqueueBatch() error = %v, want unavailable", err)
	}
	if err := queue.CompleteHead(sessionID); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("empty CompleteHead() error = %v, want unavailable", err)
	}
	if err := queue.PromoteHead(outbound, now); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("empty PromoteHead() error = %v, want unavailable", err)
	}
	if _, err := queue.ListOutbound("missing"); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("missing ListOutbound() error = %v, want unavailable", err)
	}
	if err := queue.ReplaceOutbound(outbound); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("missing ReplaceOutbound() error = %v, want unavailable", err)
	}
	if err := queue.CompleteOutbound(sessionID, 0); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("zero CompleteOutbound() error = %v, want unavailable", err)
	}
	if err := queue.CompleteOutbound(sessionID, outbound.PacketID); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("missing CompleteOutbound() error = %v, want unavailable", err)
	}
	queue.sessions[sessionID].outbound = nil
	if err := queue.Register(sessionID); err != nil {
		t.Fatalf("re-register nil outbound map = %v", err)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/cluster-session-and-delivery.md §4.1, §11.9.
func TestListQueuedReturnsLiveFIFOCopiesWithoutMutation(t *testing.T) {
	now := testNow()
	var nilQueue *Queue
	if _, err := nilQueue.ListQueued("session-1", now); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nilQueue.ListQueued() error = %v, want unavailable", err)
	}

	queue, err := New(10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.ListQueued("", now); !errors.Is(err, routing.ErrInvalidSessionID) {
		t.Fatalf("queue.ListQueued(\"\") error = %v, want invalid session id", err)
	}
	if _, err := queue.ListQueued("missing", now); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("queue.ListQueued(missing) error = %v, want unavailable", err)
	}

	sessionID := routing.SessionID("session-live")
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}

	deliveries := []applicationdeliveryqueue.DeliveryDTO{
		{SessionID: sessionID, Topic: "topic/1", Payload: []byte("payload-1"), QoS: mqtt.QoSAtMostOnce},
		{SessionID: sessionID, Topic: "topic/2", Payload: []byte("payload-2"), QoS: mqtt.QoSAtMostOnce, ExpiresAt: now.Add(time.Minute)},
		{SessionID: sessionID, Topic: "topic/3", Payload: []byte("payload-3"), QoS: mqtt.QoSAtMostOnce},
	}
	if err := queue.TryEnqueueBatch(deliveries, now); err != nil {
		t.Fatalf("TryEnqueueBatch() error = %v", err)
	}

	listed, err := queue.ListQueued(sessionID, now)
	if err != nil {
		t.Fatalf("ListQueued() error = %v", err)
	}
	if len(listed) != 3 {
		t.Fatalf("len(listed) = %d, want 3 before expiry", len(listed))
	}
	if listed[0].Topic != "topic/1" || string(listed[0].Payload) != "payload-1" {
		t.Fatalf("listed[0] = %#v, want topic/1", listed[0])
	}
	if listed[1].Topic != "topic/2" || string(listed[1].Payload) != "payload-2" {
		t.Fatalf("listed[1] = %#v, want topic/2", listed[1])
	}
	if listed[2].Topic != "topic/3" || string(listed[2].Payload) != "payload-3" {
		t.Fatalf("listed[2] = %#v, want topic/3", listed[2])
	}

	// A later read filters an expired entry but leaves the queue untouched.
	listedLater, err := queue.ListQueued(sessionID, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("ListQueued() after expiry: %v", err)
	}
	if len(listedLater) != 2 || listedLater[0].Topic != "topic/1" || listedLater[1].Topic != "topic/3" {
		t.Fatalf("ListQueued() after expiry = %#v, want topic/1 then topic/3", listedLater)
	}
	if len(queue.sessions[sessionID].queued) != 3 {
		t.Fatalf("ListQueued() mutated queue length to %d, want 3", len(queue.sessions[sessionID].queued))
	}

	// Mutate a returned payload: it must not affect queue state.
	listed[0].Payload[0] = 'X'
	peeked, ok, err := queue.Peek(sessionID, now)
	if err != nil || !ok {
		t.Fatalf("Peek() after ListQueued = %#v, %v, %v", peeked, ok, err)
	}
	if string(peeked.Payload) != "payload-1" {
		t.Fatalf("Peek payload mutated: got %s, want payload-1", string(peeked.Payload))
	}
	if peeked.Topic != "topic/1" {
		t.Fatalf("Peek topic = %s, want topic/1", peeked.Topic)
	}
}
