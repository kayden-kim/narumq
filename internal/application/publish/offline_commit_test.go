package publish

import (
	"context"
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
