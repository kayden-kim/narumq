package publish

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

type recordingOfflineCommitter struct {
	calls   []deliveryqueue.DeliveryDTO
	handled bool
	err     error
	hook    func(delivery deliveryqueue.DeliveryDTO) (bool, error)
}

func (c *recordingOfflineCommitter) CommitOffline(_ context.Context, delivery deliveryqueue.DeliveryDTO, _ time.Time) (bool, error) {
	c.calls = append(c.calls, delivery)
	if c.hook != nil {
		return c.hook(delivery)
	}
	return c.handled, c.err
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-042
func TestPublishOfflineCommitHandledDelivered(t *testing.T) {
	now := testNow()
	clock := &fakeClock{now: now}
	routes := routing.NewTable()
	tenantID, _ := tenancy.NewTenantID("tenant-a")
	_ = routes.BindSessionTenant("publisher", tenantID)
	_ = routes.BindSessionTenant("detached-sub", tenantID)
	_, _ = routes.Subscribe("detached-sub", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce})

	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	repository := &recordingRetainedRepository{}
	committer := &recordingOfflineCommitter{handled: true, err: nil}

	useCase, err := New(clock, routes, queue, repository, WithOfflineCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}

	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("val"), QoS: mqtt.QoSAtMostOnce,
	}, now)
	if err != nil {
		t.Fatalf("Publish error = %v", err)
	}

	if len(committer.calls) != 1 || committer.calls[0].SessionID != "detached-sub" {
		t.Fatalf("committer calls = %+v, want 1 call for detached-sub", committer.calls)
	}
	// Local queue should NOT be called when handled=true
	if len(queue.deliveries) != 0 {
		t.Fatalf("local queue was called: %+v", queue.deliveries)
	}
	if len(result.Delivered) != 1 || result.Delivered[0] != "detached-sub" {
		t.Fatalf("result.Delivered = %+v, want [detached-sub]", result.Delivered)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestPublishOfflineCommitHandledFullFallsBackShared(t *testing.T) {
	now := testNow()
	clock := &fakeClock{now: now}
	routes := routing.NewTable()
	tenantID, _ := tenancy.NewTenantID("tenant-a")
	_ = routes.BindSessionTenant("publisher", tenantID)
	_ = routes.BindSessionTenant("member-1", tenantID)
	_ = routes.BindSessionTenant("member-2", tenantID)

	// Shared subscription
	_, _ = routes.Subscribe("member-1", mqtt.Subscription{Filter: "$share/group/events", QoS: mqtt.QoSAtMostOnce})
	_, _ = routes.Subscribe("member-2", mqtt.Subscription{Filter: "$share/group/events", QoS: mqtt.QoSAtMostOnce})

	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	repository := &recordingRetainedRepository{}

	// member-1 returns ErrFull from offline committer; member-2 returns handled=false (local queue)
	committer := &recordingOfflineCommitter{
		hook: func(delivery deliveryqueue.DeliveryDTO) (bool, error) {
			if delivery.SessionID == "member-1" {
				return true, deliveryqueue.ErrFull
			}
			return false, nil
		},
	}

	useCase, err := New(clock, routes, queue, repository, WithOfflineCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}

	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events", Payload: []byte("val"), QoS: mqtt.QoSAtMostOnce,
	}, now)
	if err != nil {
		t.Fatalf("Publish error = %v", err)
	}

	// member-1 was saturated and member-2 delivered
	if len(result.Saturated) != 1 || result.Saturated[0] != "member-1" {
		t.Fatalf("result.Saturated = %+v, want [member-1]", result.Saturated)
	}
	if len(result.Delivered) != 1 || result.Delivered[0] != "member-2" {
		t.Fatalf("result.Delivered = %+v, want [member-2]", result.Delivered)
	}
	// member-2 went to local queue
	if len(queue.deliveries) != 1 || queue.deliveries[0].SessionID != "member-2" {
		t.Fatalf("queue deliveries = %+v, want [member-2]", queue.deliveries)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestPublishOfflineCommitUnhandledUsesLocalQueue(t *testing.T) {
	now := testNow()
	clock := &fakeClock{now: now}
	routes := routing.NewTable()
	tenantID, _ := tenancy.NewTenantID("tenant-a")
	_ = routes.BindSessionTenant("publisher", tenantID)
	_ = routes.BindSessionTenant("connected-sub", tenantID)
	_, _ = routes.Subscribe("connected-sub", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce})

	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	repository := &recordingRetainedRepository{}
	committer := &recordingOfflineCommitter{handled: false}

	useCase, err := New(clock, routes, queue, repository, WithOfflineCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}

	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("val"), QoS: mqtt.QoSAtMostOnce,
	}, now)
	if err != nil {
		t.Fatalf("Publish error = %v", err)
	}

	if len(committer.calls) != 1 {
		t.Fatalf("committer calls = %d, want 1", len(committer.calls))
	}
	if len(queue.deliveries) != 1 || queue.deliveries[0].SessionID != "connected-sub" {
		t.Fatalf("queue deliveries = %+v, want [connected-sub]", queue.deliveries)
	}
	if len(result.Delivered) != 1 || result.Delivered[0] != "connected-sub" {
		t.Fatalf("result.Delivered = %+v, want [connected-sub]", result.Delivered)
	}
}
func TestPublishOfflineCommitErrorDependsOnHandling(t *testing.T) {
	commitErr := errors.New("offline commit unavailable")
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
			offline := &recordingOfflineCommitter{handled: tc.handled, err: commitErr}
			forwarder := &recordingRemoteForwarder{handled: false}
			useCase := newRemoteForwardTestUseCase(t, routes, queue,
				WithOfflineCommitter(offline), WithRemoteForwarder(forwarder))

			result, err := useCase.Publish("publisher", mqtt.Publish{
				Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("value"),
			}, testNow())
			if tc.wantErr {
				if !errors.Is(err, commitErr) {
					t.Fatalf("Publish error = %v, want %v", err, commitErr)
				}
				if len(result.Delivered) != 0 || len(result.Saturated) != 0 {
					t.Fatalf("result after failed handled commit = %+v, want no outcome", result)
				}
			} else {
				if err != nil {
					t.Fatalf("Publish error = %v, want nil after local fallback", err)
				}
				if len(result.Delivered) != 1 || result.Delivered[0] != "subscriber" {
					t.Fatalf("delivered = %v, want subscriber", result.Delivered)
				}
			}
			if tc.wantEnqueued {
				if len(queue.deliveries) != 1 || queue.deliveries[0].SessionID != "subscriber" ||
					len(forwarder.calls) != 1 {
					t.Fatalf("unhandled commit did not fall back through forwarder and queue: queued=%+v forwarded=%+v",
						queue.deliveries, forwarder.calls)
				}
			} else if len(queue.attempts) != 0 || len(forwarder.calls) != 0 {
				t.Fatalf("handled commit error caused later delivery: queued=%+v forwarded=%+v",
					queue.attempts, forwarder.calls)
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §11.25.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-177
func TestPublishLocalDetachTransition(t *testing.T) {
	for _, tc := range []struct {
		name        string
		queueErr    error
		commitAfter bool
		noCommitter bool
		waitErr     error
		keepGate    bool
		unavailable bool
		commitCalls int
		awaitCalls  int
		localCopies int
		queueCalls  int
		waitAdvance time.Duration
		expiry      uint32
		expired     bool
	}{
		{name: "committed", queueErr: deliveryqueue.ErrDetaching, commitAfter: true, commitCalls: 2, awaitCalls: 1, queueCalls: 1},
		{name: "failed", queueErr: deliveryqueue.ErrDetaching, commitCalls: 2, awaitCalls: 1, localCopies: 1, queueCalls: 2},
		{name: "removed", queueErr: deliveryqueue.ErrUnavailable, commitAfter: true, commitCalls: 2, queueCalls: 1},
		{name: "removed-without-offline-committer", queueErr: deliveryqueue.ErrUnavailable, noCommitter: true, unavailable: true, queueCalls: 1},
		{name: "wait-timeout", queueErr: deliveryqueue.ErrDetaching, waitErr: context.DeadlineExceeded, unavailable: true, commitCalls: 1, awaitCalls: 1, queueCalls: 1},
		{name: "second-detach-does-not-wait", queueErr: deliveryqueue.ErrDetaching, keepGate: true, unavailable: true, commitCalls: 2, awaitCalls: 1, queueCalls: 2},
		{name: "missing-after-recheck", queueErr: deliveryqueue.ErrUnavailable, unavailable: true, commitCalls: 2, queueCalls: 2},
		{name: "committed-expired-at-deadline", queueErr: deliveryqueue.ErrDetaching, commitAfter: true, waitAdvance: time.Second, expiry: 1, expired: true, commitCalls: 1, awaitCalls: 1, queueCalls: 1},
		{name: "failed-expired-after-deadline", queueErr: deliveryqueue.ErrDetaching, waitAdvance: 2 * time.Second, expiry: 1, expired: true, commitCalls: 1, awaitCalls: 1, queueCalls: 1},
		{name: "committed-live-before-deadline", queueErr: deliveryqueue.ErrDetaching, commitAfter: true, waitAdvance: time.Second - time.Nanosecond, expiry: 1, commitCalls: 2, awaitCalls: 1, queueCalls: 1},
		{name: "failed-live-before-deadline", queueErr: deliveryqueue.ErrDetaching, waitAdvance: time.Second - time.Nanosecond, expiry: 1, commitCalls: 2, awaitCalls: 1, localCopies: 1, queueCalls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routes := routing.NewTable()
			tenantID, err := tenancy.NewTenantID("tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []routing.SessionID{"publisher", "detached-sub"} {
				if err := routes.BindSessionTenant(id, tenantID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := routes.Subscribe("detached-sub", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
				t.Fatal(err)
			}
			queue := &recordingQueue{errors: map[routing.SessionID]error{"detached-sub": tc.queueErr}}
			clock := &fakeClock{now: testNow()}
			queue.awaitHook = func(id routing.SessionID) error {
				clock.now = clock.now.Add(tc.waitAdvance)
				if !tc.keepGate {
					delete(queue.errors, id)
				}
				return tc.waitErr
			}
			committer := &recordingOfflineCommitter{}
			committer.hook = func(deliveryqueue.DeliveryDTO) (bool, error) {
				return tc.commitAfter && len(committer.calls) > 1, nil
			}
			var options []Option
			if !tc.noCommitter {
				options = append(options, WithOfflineCommitter(committer))
			}
			useCase, err := New(clock, routes, queue, &recordingRetainedRepository{}, options...)
			if err != nil {
				t.Fatal(err)
			}
			result, err := useCase.Publish("publisher", mqtt.Publish{
				Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("value"),
				HasMessageExpiryInterval: tc.expiry > 0, MessageExpiryInterval: tc.expiry,
			}, testNow())
			if err != nil {
				t.Fatal(err)
			}
			if tc.expired {
				if len(result.Delivered) != 0 || len(result.Unavailable) != 0 || len(result.Saturated) != 0 {
					t.Fatalf("expired result = %+v, want accepted without delivery", result)
				}
			} else if tc.unavailable {
				if !reflect.DeepEqual(result.Unavailable, []routing.SessionID{"detached-sub"}) || len(result.Delivered) != 0 {
					t.Fatalf("unavailable result = %+v", result)
				}
			} else if !reflect.DeepEqual(result.Delivered, []routing.SessionID{"detached-sub"}) || len(result.Unavailable) != 0 {
				t.Fatalf("delivered result = %+v", result)
			}
			if len(committer.calls) != tc.commitCalls || len(queue.awaitCalls) != tc.awaitCalls || len(queue.deliveries) != tc.localCopies || len(queue.calls) != tc.queueCalls {
				t.Fatalf("commit=%d await=%v copies=%v queue=%d; want %d/%d/%d/%d",
					len(committer.calls), queue.awaitCalls, queue.deliveries, len(queue.calls), tc.commitCalls, tc.awaitCalls, tc.localCopies, tc.queueCalls)
			}
		})
	}
}
