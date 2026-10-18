package publish

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

type recordingRemoteForwarder struct {
	calls   []deliveryqueue.DeliveryDTO
	handled bool
	err     error
	hook    func(deliveryqueue.DeliveryDTO) (bool, error)
}

func (forwarder *recordingRemoteForwarder) ForwardRemote(
	_ context.Context,
	delivery deliveryqueue.DeliveryDTO,
) (bool, error) {
	forwarder.calls = append(forwarder.calls, delivery)
	if forwarder.hook != nil {
		return forwarder.hook(delivery)
	}
	return forwarder.handled, forwarder.err
}

func newRemoteForwardTestUseCase(t *testing.T, routes *routing.Table, queue *recordingQueue, opts ...Option) *UseCase {
	t.Helper()
	useCase, err := New(&fakeClock{now: testNow()}, routes, queue, &recordingRetainedRepository{}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return useCase
}

func addRemoteForwardTestRoute(t *testing.T, routes *routing.Table, sessionID routing.SessionID, filter string) {
	t.Helper()
	if _, err := routes.Subscribe(sessionID, mqtt.Subscription{Filter: filter, QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-054
func TestPublishRemoteForwardHandledDoesNotUseLocalQueue(t *testing.T) {
	routes := routing.NewTable()
	addRemoteForwardTestRoute(t, routes, "remote-session", "events/#")
	queue := &recordingQueue{}
	forwarder := &recordingRemoteForwarder{handled: true}
	useCase := newRemoteForwardTestUseCase(t, routes, queue, WithRemoteForwarder(forwarder))

	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("value"), QoS: mqtt.QoSAtMostOnce,
	}, testNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(forwarder.calls) != 1 || forwarder.calls[0].SessionID != "remote-session" {
		t.Fatalf("forwarder calls = %+v, want one remote-session delivery", forwarder.calls)
	}
	if len(queue.attempts) != 0 {
		t.Fatalf("local queue attempts = %+v, want none", queue.attempts)
	}
	want := ResultDTO{
		Delivered: []routing.SessionID{"remote-session"},
		Saturated: []routing.SessionID{}, Unavailable: []routing.SessionID{},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("Publish result = %#v, want %#v", result, want)
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §4.1, §11.10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-054
func TestPublishOfflineHandledDoesNotCallRemoteForwarder(t *testing.T) {
	routes := routing.NewTable()
	addRemoteForwardTestRoute(t, routes, "detached-session", "events/#")
	queue := &recordingQueue{}
	offline := &recordingOfflineCommitter{handled: true}
	forwarder := &recordingRemoteForwarder{handled: true}
	useCase := newRemoteForwardTestUseCase(t, routes, queue,
		WithOfflineCommitter(offline), WithRemoteForwarder(forwarder))

	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("value"), QoS: mqtt.QoSAtMostOnce,
	}, testNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(offline.calls) != 1 || offline.calls[0].SessionID != "detached-session" {
		t.Fatalf("offline committer calls = %+v, want detached-session", offline.calls)
	}
	if len(forwarder.calls) != 0 || len(queue.attempts) != 0 {
		t.Fatalf("forwarder calls = %d, queue attempts = %d; want neither", len(forwarder.calls), len(queue.attempts))
	}
	if len(result.Delivered) != 1 || result.Delivered[0] != "detached-session" {
		t.Fatalf("delivered = %v, want detached-session", result.Delivered)
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-054
func TestPublishRemoteForwardUnhandledUsesLocalQueue(t *testing.T) {
	routes := routing.NewTable()
	addRemoteForwardTestRoute(t, routes, "local-fallback", "events/#")
	queue := &recordingQueue{}
	forwarder := &recordingRemoteForwarder{handled: false}
	useCase := newRemoteForwardTestUseCase(t, routes, queue, WithRemoteForwarder(forwarder))

	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("value"), QoS: mqtt.QoSAtMostOnce,
	}, testNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(forwarder.calls) != 1 || len(queue.attempts) != 1 || queue.attempts[0].SessionID != "local-fallback" {
		t.Fatalf("forwarder calls = %d, queue attempts = %+v", len(forwarder.calls), queue.attempts)
	}
	if len(result.Delivered) != 1 || result.Delivered[0] != "local-fallback" {
		t.Fatalf("delivered = %v, want local-fallback", result.Delivered)
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-054
func TestPublishRemoteForwardFullFallsBackShared(t *testing.T) {
	routes := routing.NewTable()
	addRemoteForwardTestRoute(t, routes, "member-a", "$share/group/events")
	addRemoteForwardTestRoute(t, routes, "member-b", "$share/group/events")
	queue := &recordingQueue{}
	calls := 0
	forwarder := &recordingRemoteForwarder{
		hook: func(delivery deliveryqueue.DeliveryDTO) (bool, error) {
			calls++
			if calls == 1 {
				return true, deliveryqueue.ErrFull
			}
			return false, nil
		},
	}
	useCase := newRemoteForwardTestUseCase(t, routes, queue, WithRemoteForwarder(forwarder))

	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events", Payload: []byte("value"), QoS: mqtt.QoSAtMostOnce,
	}, testNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(forwarder.calls) != 2 || forwarder.calls[0].SessionID == forwarder.calls[1].SessionID {
		t.Fatalf("forwarder candidates = %+v, want saturated primary then a distinct fallback", forwarder.calls)
	}
	if len(result.Saturated) != 1 || result.Saturated[0] != forwarder.calls[0].SessionID {
		t.Fatalf("saturated = %v, want primary %q", result.Saturated, forwarder.calls[0].SessionID)
	}
	if len(result.Delivered) != 1 || result.Delivered[0] != forwarder.calls[1].SessionID {
		t.Fatalf("delivered = %v, want fallback %q", result.Delivered, forwarder.calls[1].SessionID)
	}
	if len(queue.attempts) != 1 || queue.attempts[0].SessionID != forwarder.calls[1].SessionID {
		t.Fatalf("local fallback attempts = %+v, want selected fallback", queue.attempts)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-054
func TestPublishRemoteForwardErrorDependsOnHandling(t *testing.T) {
	forwardErr := errors.New("remote forward unavailable")
	for _, tc := range []struct {
		name         string
		handled      bool
		wantErr      bool
		wantEnqueued bool
	}{
		{name: "handled error stops local enqueue", handled: true, wantErr: true},
		{name: "unhandled error falls back locally", wantEnqueued: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routes := routing.NewTable()
			addRemoteForwardTestRoute(t, routes, "subscriber", "events/#")
			queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
			forwarder := &recordingRemoteForwarder{handled: tc.handled, err: forwardErr}
			useCase := newRemoteForwardTestUseCase(t, routes, queue, WithRemoteForwarder(forwarder))

			result, err := useCase.Publish("publisher", mqtt.Publish{
				Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("value"),
			}, testNow())
			if tc.wantErr {
				if !errors.Is(err, forwardErr) {
					t.Fatalf("Publish error = %v, want %v", err, forwardErr)
				}
				if len(result.Delivered) != 0 || len(result.Saturated) != 0 {
					t.Fatalf("result after failed handled forward = %+v, want no outcome", result)
				}
				if len(queue.attempts) != 0 {
					t.Fatalf("handled forwarding error caused local enqueue: %+v", queue.attempts)
				}
			} else {
				if err != nil {
					t.Fatalf("Publish error = %v, want nil after local fallback", err)
				}
				if len(result.Delivered) != 1 || result.Delivered[0] != "subscriber" {
					t.Fatalf("delivered = %v, want subscriber", result.Delivered)
				}
				if len(queue.deliveries) != 1 || queue.deliveries[0].SessionID != "subscriber" {
					t.Fatalf("unhandled forwarding error did not enqueue locally: %+v", queue.deliveries)
				}
			}
		})
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-054
func TestAcceptForwardedValidatesExpiryAndQoS(t *testing.T) {
	now := testNow()
	routes := routing.NewTable()
	queue := &recordingQueue{}
	offline := &recordingOfflineCommitter{}
	forwarder := &recordingRemoteForwarder{}
	useCase := newRemoteForwardTestUseCase(t, routes, queue,
		WithOfflineCommitter(offline), WithRemoteForwarder(forwarder))

	invalidQoS := deliveryqueue.DeliveryDTO{SessionID: "local", Topic: "events/one", QoS: mqtt.QoSAtLeastOnce}
	if err := useCase.AcceptForwarded(invalidQoS, now); !errors.Is(err, routing.ErrInvalidQoS) {
		t.Fatalf("QoS 1 error = %v, want ErrInvalidQoS", err)
	}
	if len(queue.attempts) != 0 {
		t.Fatalf("queue attempts after invalid QoS = %d, want 0", len(queue.attempts))
	}

	expired := deliveryqueue.DeliveryDTO{
		SessionID: "local", Topic: "events/one", QoS: mqtt.QoSAtMostOnce, ExpiresAt: now,
	}
	if err := useCase.AcceptForwarded(expired, now); err != nil {
		t.Fatalf("expired delivery error = %v, want nil", err)
	}
	if len(queue.attempts) != 0 {
		t.Fatalf("queue attempts after expired delivery = %d, want 0", len(queue.attempts))
	}

	valid := deliveryqueue.DeliveryDTO{
		SessionID: "local", Topic: "events/one", Payload: []byte("value"), QoS: mqtt.QoSAtMostOnce,
	}
	if err := useCase.AcceptForwarded(valid, now); err != nil {
		t.Fatalf("valid forwarded delivery error = %v", err)
	}
	if len(queue.attempts) != 1 || queue.attempts[0].SessionID != valid.SessionID ||
		string(queue.attempts[0].Payload) != "value" {
		t.Fatalf("queue attempts = %+v, want one valid forwarded delivery", queue.attempts)
	}

	invalidTopic := valid
	invalidTopic.Topic = "events/#"
	if err := useCase.AcceptForwarded(invalidTopic, now); !errors.Is(err, routing.ErrInvalidTopicName) {
		t.Fatalf("wildcard topic error = %v, want ErrInvalidTopicName", err)
	}
	if len(queue.attempts) != 1 {
		t.Fatalf("queue attempts after invalid topic = %d, want 1", len(queue.attempts))
	}

	invalidSession := valid
	invalidSession.SessionID = ""
	if err := useCase.AcceptForwarded(invalidSession, now); !errors.Is(err, routing.ErrInvalidSessionID) {
		t.Fatalf("empty session error = %v, want ErrInvalidSessionID", err)
	}
	if len(queue.attempts) != 1 {
		t.Fatalf("queue attempts after invalid session = %d, want 1", len(queue.attempts))
	}
	if len(offline.calls) != 0 || len(forwarder.calls) != 0 {
		t.Fatalf("forwarded receiver called offline=%d remote=%d times, want neither", len(offline.calls), len(forwarder.calls))
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §11.25.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-178
func TestAcceptForwardedDetachingIsUnavailable(t *testing.T) {
	queue := &recordingQueue{errors: map[routing.SessionID]error{"sub": deliveryqueue.ErrDetaching}}
	useCase := newRemoteForwardTestUseCase(t, routing.NewTable(), queue)
	err := useCase.AcceptForwarded(deliveryqueue.DeliveryDTO{SessionID: "sub", Topic: "events/one"}, testNow())
	if !errors.Is(err, deliveryqueue.ErrUnavailable) || len(queue.awaitCalls) != 0 || len(queue.deliveries) != 0 {
		t.Fatalf("forward during DETACH: error=%v awaits=%v deliveries=%v", err, queue.awaitCalls, queue.deliveries)
	}
}
