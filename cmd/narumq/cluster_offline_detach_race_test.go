package main

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"narumq/internal/adapters/clock"
	memoryqueue "narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/publish"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type detachRaceOwnershipCoordinator struct{}

func (detachRaceOwnershipCoordinator) AcquireOwnership(context.Context, string, string, uint64) (uint64, error) {
	return 1, nil
}

type detachRaceOfflineCoordinator struct {
	detached         atomic.Bool
	entered          chan struct{}
	release          chan struct{}
	publishChecked   chan struct{}
	publishCheckOnce sync.Once
	mu               sync.Mutex
	detachPending    []deliveryqueue.DeliveryDTO
	enqueued         []deliveryqueue.DeliveryDTO
}

func (coordinator *detachRaceOfflineCoordinator) DetachSession(ctx context.Context, request connect.DetachRequestDTO) error {
	coordinator.mu.Lock()
	coordinator.detachPending = append([]deliveryqueue.DeliveryDTO(nil), request.Pending...)
	coordinator.mu.Unlock()
	close(coordinator.entered)
	select {
	case <-coordinator.release:
		coordinator.detached.Store(true)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*detachRaceOfflineCoordinator) ClaimOffline(context.Context, string, string, uint64, uint64, time.Time) (connect.OfflineClaimDTO, error) {
	return connect.OfflineClaimDTO{}, nil
}

func (coordinator *detachRaceOfflineCoordinator) CommitOffline(_ context.Context, item deliveryqueue.DeliveryDTO, _ time.Time) (bool, error) {
	if !coordinator.detached.Load() {
		if coordinator.publishChecked != nil {
			coordinator.publishCheckOnce.Do(func() { close(coordinator.publishChecked) })
		}
		return false, nil
	}
	coordinator.mu.Lock()
	coordinator.enqueued = append(coordinator.enqueued, item)
	coordinator.mu.Unlock()
	return true, nil
}

type detachRaceQueue struct {
	*memoryqueue.Queue
	awaitEntered chan struct{}
	awaitOnce    sync.Once
}

func (queue *detachRaceQueue) AwaitDetach(ctx context.Context, sessionID routing.SessionID) error {
	queue.awaitOnce.Do(func() { close(queue.awaitEntered) })
	return queue.Queue.AwaitDetach(ctx, sessionID)
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §11.25.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-182
func TestClusterDetachCapturesConcurrentLocalQoS0Publish(t *testing.T) {
	localQueue, err := memoryqueue.New(16)
	if err != nil {
		t.Fatal(err)
	}
	queue := &detachRaceQueue{Queue: localQueue, awaitEntered: make(chan struct{})}
	offline := &detachRaceOfflineCoordinator{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(offline.release) }) }
	defer release()
	routes := routing.NewTable()
	systemClock := clock.SystemClock{}
	connections, err := connect.New(systemClock, sessionstore.New(), queue, routes, clock.SystemScheduler{}, &recoveryWillPublisher{},
		connect.WithOwnershipCoordinator(detachRaceOwnershipCoordinator{}, 1), connect.WithOfflineCoordinator(offline))
	if err != nil {
		t.Fatal(err)
	}
	defer connections.Close()
	retainedRepository, err := memoryretained.New(8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	publications, err := publish.New(systemClock, routes, queue, retainedRepository, publish.WithOfflineCommitter(offline))
	if err != nil {
		t.Fatal(err)
	}
	res, err := connections.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "race-sub", SessionExpiryInterval: 60})
	if err != nil || !res.Accepted {
		t.Fatalf("Connect = %+v, %v", res, err)
	}
	tenant, err := tenancy.NewTenantID("default")
	if err != nil {
		t.Fatal(err)
	}
	if err := routes.BindSessionTenant("race-pub", tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := routes.Subscribe(res.SessionID, mqtt.Subscription{Filter: "race/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	first, err := publications.Publish("race-pub", mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "race/1", Payload: []byte("first")}, systemClock.Now())
	if err != nil || !reflect.DeepEqual(first.Delivered, []routing.SessionID{res.SessionID}) {
		t.Fatalf("first Publish = %+v, %v", first, err)
	}
	queued, err := queue.ListQueued(res.SessionID, systemClock.Now())
	if err != nil || len(queued) != 1 || queued[0].Topic != "race/1" {
		t.Fatalf("first local queue = %+v, %v", queued, err)
	}
	// Arm the signal after the initial connected publish, before concurrency starts.
	offline.publishChecked = make(chan struct{})
	disconnectDone := make(chan error, 1)
	go func() { disconnectDone <- connections.Disconnect(res.SessionID, session.DisconnectNormal) }()
	waitDetachRaceSignal(t, offline.entered, "DETACH entry")
	type publicationResult struct {
		result publish.ResultDTO
		err    error
	}
	publishDone := make(chan publicationResult, 1)
	go func() {
		result, err := publications.Publish("race-pub", mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "race/2", Payload: []byte("second")}, systemClock.Now())
		publishDone <- publicationResult{result: result, err: err}
	}()
	waitDetachRaceSignal(t, offline.publishChecked, "pre-DETACH publish check")
	var second publicationResult
	completed := false
	select {
	case <-queue.awaitEntered:
	case second = <-publishDone:
		completed = true
	case <-time.After(5 * time.Second):
		t.Fatal("publish neither waited nor completed")
	}
	release()
	select {
	case err := <-disconnectDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("disconnect did not complete")
	}
	if !completed {
		select {
		case second = <-publishDone:
		case <-time.After(5 * time.Second):
			t.Fatal("publish did not complete")
		}
	}
	if second.err != nil || !reflect.DeepEqual(second.result.Delivered, []routing.SessionID{res.SessionID}) {
		t.Fatalf("second Publish = %+v, %v", second.result, second.err)
	}
	offline.mu.Lock()
	pendingTopics := make([]string, 0, len(offline.detachPending))
	for _, item := range offline.detachPending {
		pendingTopics = append(pendingTopics, item.Topic)
	}
	enqueuedTopics := make([]string, 0, len(offline.enqueued))
	for _, item := range offline.enqueued {
		enqueuedTopics = append(enqueuedTopics, item.Topic)
	}
	offline.mu.Unlock()
	if !reflect.DeepEqual(pendingTopics, []string{"race/1"}) || !reflect.DeepEqual(enqueuedTopics, []string{"race/2"}) {
		t.Fatalf("offline FIFO: DETACH=%v ENQUEUE=%v", pendingTopics, enqueuedTopics)
	}
	if _, err := queue.ListQueued(res.SessionID, systemClock.Now()); !errors.Is(err, deliveryqueue.ErrUnavailable) {
		t.Fatalf("local queue after DETACH = %v", err)
	}
}

type detachRaceForwardSink struct{ publications *publish.UseCase }

func (sink detachRaceForwardSink) DeliverForwarded(delivery deliveryqueue.DeliveryDTO) error {
	return sink.publications.AcceptForwarded(delivery, time.Now())
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §11.26.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-184
func TestClusterForwardDuringDetachIsDroppedWithoutBlockingLane(t *testing.T) {
	localQueue, err := memoryqueue.New(16)
	if err != nil {
		t.Fatal(err)
	}
	queue := &detachRaceQueue{Queue: localQueue, awaitEntered: make(chan struct{})}
	offline := &detachRaceOfflineCoordinator{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(offline.release) }) }
	defer release()
	routes := routing.NewTable()
	systemClock := clock.SystemClock{}
	connections, err := connect.New(systemClock, sessionstore.New(), queue, routes, clock.SystemScheduler{}, &recoveryWillPublisher{},
		connect.WithOwnershipCoordinator(detachRaceOwnershipCoordinator{}, 1), connect.WithOfflineCoordinator(offline))
	if err != nil {
		t.Fatal(err)
	}
	defer connections.Close()
	retainedRepository, err := memoryretained.New(8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	publications, err := publish.New(systemClock, routes, queue, retainedRepository, publish.WithOfflineCommitter(offline))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := connections.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "race-sub", SessionExpiryInterval: 60})
	if err != nil || !sub.Accepted {
		t.Fatalf("Connect race-sub = %+v, %v", sub, err)
	}
	other, err := connections.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "race-other", SessionExpiryInterval: 60})
	if err != nil || !other.Accepted {
		t.Fatalf("Connect race-other = %+v, %v", other, err)
	}
	runtime, _ := newClusterForwardTestRuntime(t, 1)
	tenant, err := tenancy.NewTenantID("default")
	if err != nil {
		t.Fatal(err)
	}
	for i, item := range []struct {
		clientID    string
		sessionID   routing.SessionID
		operationID string
	}{
		{"race-sub", sub.SessionID, "00000000-0000-4000-8000-000000000011"},
		{"race-other", other.SessionID, "00000000-0000-4000-8000-000000000012"},
	} {
		result, err := runtime.machine.Apply(uint64(5+i), membership.CommandDTO{
			SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
			OperationID: item.operationID,
			Ownership: &membership.OwnershipChangeDTO{
				TenantID: "default", ClientID: item.clientID, OwnerNodeID: 1,
				Epoch: 1, ExpectedEpoch: 0, Action: membership.OwnershipActionAcquire,
			},
		})
		if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
			t.Fatalf("Acquire %s = %+v, %v", item.clientID, result, err)
		}
		key, err := tenancy.NewSessionKey(tenant, item.clientID)
		if err != nil || routing.SessionID(key.RoutingID()) != item.sessionID {
			t.Fatalf("session identity %s = %q, %v; want %q", item.clientID, key.RoutingID(), err, item.sessionID)
		}
	}
	runtime.forwardSink = detachRaceForwardSink{publications: publications}
	if err := routes.BindSessionTenant("race-pub", tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := routes.Subscribe(sub.SessionID, mqtt.Subscription{Filter: "race/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	first, err := publications.Publish("race-pub", mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "race/1", Payload: []byte("first")}, systemClock.Now())
	if err != nil || !reflect.DeepEqual(first.Delivered, []routing.SessionID{sub.SessionID}) {
		t.Fatalf("first Publish = %+v, %v", first, err)
	}
	queued, err := queue.ListQueued(sub.SessionID, systemClock.Now())
	if err != nil || len(queued) != 1 || queued[0].Topic != "race/1" {
		t.Fatalf("first local queue = %+v, %v", queued, err)
	}
	frame := func(sessionID routing.SessionID, topic string) tcpadapter.PeerInboundDelivery {
		t.Helper()
		payload, err := tcpadapter.EncodeForwardedDelivery(tcpadapter.ForwardedDeliveryDTO{
			SessionID: string(sessionID), Topic: topic, Payload: []byte(topic),
		})
		if err != nil {
			t.Fatal(err)
		}
		return tcpadapter.PeerInboundDelivery{Message: tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: payload}}
	}
	during := frame(sub.SessionID, "race/forward-during")
	otherDuring := frame(other.SessionID, "race/other-during")
	disconnectDone := make(chan error, 1)
	go func() { disconnectDone <- connections.Disconnect(sub.SessionID, session.DisconnectNormal) }()
	waitDetachRaceSignal(t, offline.entered, "DETACH entry")
	forwardDone := make(chan struct{})
	go func() {
		runtime.acceptForwardedFrame(during)
		runtime.acceptForwardedFrame(otherDuring)
		close(forwardDone)
	}()
	waitDetachRaceSignal(t, forwardDone, "forward lane completion while DETACH is blocked")
	queued, err = queue.ListQueued(other.SessionID, systemClock.Now())
	if err != nil || len(queued) != 1 || queued[0].Topic != "race/other-during" {
		t.Fatalf("other session queue = %+v, %v", queued, err)
	}
	release()
	select {
	case err := <-disconnectDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("disconnect did not complete")
	}
	offline.mu.Lock()
	pendingTopics := make([]string, 0, len(offline.detachPending))
	for _, item := range offline.detachPending {
		pendingTopics = append(pendingTopics, item.Topic)
	}
	enqueuedCount := len(offline.enqueued)
	offline.mu.Unlock()
	if !reflect.DeepEqual(pendingTopics, []string{"race/1"}) || enqueuedCount != 0 {
		t.Fatalf("offline capture: DETACH=%v ENQUEUE count=%d", pendingTopics, enqueuedCount)
	}
	ownership, found := runtime.machine.GetOwnership("default", "race-sub")
	if !found || ownership.OwnerNodeID != 1 {
		t.Fatalf("ownership after DETACH = %+v, found=%t", ownership, found)
	}
	runtime.acceptForwardedFrame(frame(sub.SessionID, "race/forward-after"))
	offline.mu.Lock()
	enqueuedCount = len(offline.enqueued)
	offline.mu.Unlock()
	if enqueuedCount != 0 {
		t.Fatalf("forward after DETACH retried as ENQUEUE: count=%d", enqueuedCount)
	}
	if _, err := queue.ListQueued(sub.SessionID, systemClock.Now()); !errors.Is(err, deliveryqueue.ErrUnavailable) {
		t.Fatalf("local queue after DETACH = %v", err)
	}
	t.Logf("blocked DETACH: forward lane completed, other queue=race/other-during; DETACH=%v ENQUEUE=0; post-DETACH forward dropped", pendingTopics)
}

func waitDetachRaceSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
