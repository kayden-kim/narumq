package connect

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type fakeOfflineCoordinator struct {
	detachCalls []DetachRequestDTO
	detachErr   error
	claimCalls  []claimCall
	claimResult OfflineClaimDTO
	claimErr    error
	claimHook   func()
}

type claimCall struct {
	tenantID, clientID string
	ownerNodeID, epoch uint64
	claimedAt          time.Time
}

func (c *fakeOfflineCoordinator) DetachSession(_ context.Context, request DetachRequestDTO) error {
	c.detachCalls = append(c.detachCalls, request)
	return c.detachErr
}

func (c *fakeOfflineCoordinator) ClaimOffline(_ context.Context, tenantID, clientID string, ownerNodeID, epoch uint64, claimedAt time.Time) (OfflineClaimDTO, error) {
	c.claimCalls = append(c.claimCalls, claimCall{
		tenantID: tenantID, clientID: clientID, ownerNodeID: ownerNodeID, epoch: epoch, claimedAt: claimedAt,
	})
	if c.claimHook != nil {
		c.claimHook()
	}
	return c.claimResult, c.claimErr
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9, §11.25.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-179, cluster-session-and-delivery.md:CLU-SCN-042
func TestClusterDisconnectDetachesPendingQueueAndRemovesLocalCopy(t *testing.T) {
	now := time.Date(2026, time.January, 1, 10, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	routes := routing.NewTable()
	scheduler := &fakeScheduler{}
	publisher := &fakeWillPublisher{}
	coordinator := &fakeOwnershipCoordinator{epoch: 5}
	offline := &fakeOfflineCoordinator{}

	useCase, err := New(
		clock, repository, queue, routes, scheduler, publisher,
		WithOwnershipCoordinator(coordinator, 1),
		WithOfflineCoordinator(offline),
	)
	if err != nil {
		t.Fatal(err)
	}

	// 1. MQTT 5 persistent session with SessionExpiryInterval = 60
	connectReq := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "client-v5",
		SessionExpiryInterval: 60,
	}
	res, err := useCase.Connect(connectReq)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted {
		t.Fatalf("Connect not accepted: %v", res.ConnAck.ReasonCode)
	}

	// Add pending deliveries
	d1 := deliveryqueue.DeliveryDTO{SessionID: res.SessionID, Topic: "t/1", Payload: []byte("one"), QoS: mqtt.QoSAtMostOnce}
	d2 := deliveryqueue.DeliveryDTO{SessionID: res.SessionID, Topic: "t/2", Payload: []byte("two"), QoS: mqtt.QoSAtMostOnce}
	if err := queue.TryEnqueueBatch([]deliveryqueue.DeliveryDTO{d1, d2}, now); err != nil {
		t.Fatal(err)
	}

	// Disconnect normally
	if err := useCase.Disconnect(res.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	if len(offline.detachCalls) != 1 {
		t.Fatalf("detach calls = %d, want 1", len(offline.detachCalls))
	}
	if !reflect.DeepEqual(queue.detachFinishes, []bool{true}) {
		t.Fatalf("detach results = %v, want [true]", queue.detachFinishes)
	}
	call := offline.detachCalls[0]
	if call.TenantID != "default" || call.ClientID != "client-v5" || call.OwnerNodeID != 1 || call.Epoch != 5 {
		t.Fatalf("detach call fields mismatch: %+v", call)
	}
	if call.SessionExpiresAt != now.Add(60*time.Second) {
		t.Fatalf("SessionExpiresAt = %v, want %v", call.SessionExpiresAt, now.Add(60*time.Second))
	}
	if call.DetachedAt != now {
		t.Fatalf("DetachedAt = %v, want %v", call.DetachedAt, now)
	}
	if len(call.Pending) != 2 || call.Pending[0].Topic != "t/1" || call.Pending[1].Topic != "t/2" {
		t.Fatalf("detach pending deliveries mismatch: %+v", call.Pending)
	}

	// Local queue should be removed
	queued, _ := queue.ListQueued(res.SessionID, now)
	if len(queued) != 0 || queue.registered[res.SessionID] {
		t.Fatalf("local queue was not removed: queued=%+v, registered=%v", queued, queue.registered[res.SessionID])
	}

	// 2. MQTT 3.1.1 persistent session (CleanSession = 0): SessionExpiresAt must be zero
	coordinator.epoch = 7
	connectReq3 := mqtt.Connect{
		Version:    mqtt.ProtocolV311,
		ClientID:   "client-v3",
		CleanStart: false,
	}
	res3, err := useCase.Connect(connectReq3)
	if err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect(res3.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	if len(offline.detachCalls) != 2 {
		t.Fatalf("detach calls = %d, want 2", len(offline.detachCalls))
	}
	call3 := offline.detachCalls[1]
	if !call3.SessionExpiresAt.IsZero() {
		t.Fatalf("MQTT 3.1.1 persistent session should have zero SessionExpiresAt, got %v", call3.SessionExpiresAt)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9, §11.25.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-180, cluster-session-and-delivery.md:CLU-SCN-042
func TestClusterDetachFailureKeepsLocalQueue(t *testing.T) {
	now := time.Date(2026, time.January, 1, 10, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	routes := routing.NewTable()
	scheduler := &fakeScheduler{}
	publisher := &fakeWillPublisher{}
	coordinator := &fakeOwnershipCoordinator{epoch: 3}
	offline := &fakeOfflineCoordinator{detachErr: errors.New("detach quorum timeout")}

	useCase, err := New(
		clock, repository, queue, routes, scheduler, publisher,
		WithOwnershipCoordinator(coordinator, 1),
		WithOfflineCoordinator(offline),
	)
	if err != nil {
		t.Fatal(err)
	}

	connectReq := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "client-f",
		SessionExpiryInterval: 60,
	}
	resF, err := useCase.Connect(connectReq)
	if err != nil {
		t.Fatal(err)
	}

	d := deliveryqueue.DeliveryDTO{SessionID: resF.SessionID, Topic: "t/keep", Payload: []byte("keep"), QoS: mqtt.QoSAtMostOnce}
	_ = queue.TryEnqueueBatch([]deliveryqueue.DeliveryDTO{d}, now)

	if err := useCase.Disconnect(resF.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	// Detach was called but failed
	if len(offline.detachCalls) != 1 {
		t.Fatalf("detach calls = %d, want 1", len(offline.detachCalls))
	}
	if !reflect.DeepEqual(queue.detachFinishes, []bool{false}) || !queue.registered[resF.SessionID] {
		t.Fatalf("failed detach results = %v, registered = %v", queue.detachFinishes, queue.registered[resF.SessionID])
	}
	// Local deliveries must NOT be removed on detach failure
	queued, _ := queue.ListQueued(resF.SessionID, now)
	if len(queued) != 1 || queued[0].Topic != "t/keep" {
		t.Fatalf("local queue should be preserved on detach failure, got %+v", queued)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §11.25.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-181
func TestClusterDisconnectSkipsDetachWhenBeginDetachFails(t *testing.T) {
	queue := newFakeDeliveryQueue()
	offline := &fakeOfflineCoordinator{}
	useCase, err := New(&fakeClock{now: time.Date(2026, time.January, 1, 10, 0, 0, 0, time.UTC)}, newFakeSessionRepository(), queue, routing.NewTable(), &fakeScheduler{}, &fakeWillPublisher{},
		WithOwnershipCoordinator(&fakeOwnershipCoordinator{epoch: 1}, 1), WithOfflineCoordinator(offline))
	if err != nil {
		t.Fatal(err)
	}
	res, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "begin-failure", SessionExpiryInterval: 60})
	if err != nil || !res.Accepted {
		t.Fatalf("Connect = %+v, %v", res, err)
	}
	queue.beginDetachErr = deliveryqueue.ErrUnavailable
	if err := useCase.Disconnect(res.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	if len(offline.detachCalls) != 0 || len(queue.detachFinishes) != 0 || !queue.registered[res.SessionID] {
		t.Fatalf("failed BeginDetach: calls = %v, finishes = %v, registered = %v", offline.detachCalls, queue.detachFinishes, queue.registered[res.SessionID])
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestCleanDisconnectDoesNotDetach(t *testing.T) {
	now := time.Date(2026, time.January, 1, 10, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	routes := routing.NewTable()
	scheduler := &fakeScheduler{}
	publisher := &fakeWillPublisher{}
	coordinator := &fakeOwnershipCoordinator{epoch: 1}
	offline := &fakeOfflineCoordinator{}

	useCase, err := New(
		clock, repository, queue, routes, scheduler, publisher,
		WithOwnershipCoordinator(coordinator, 1),
		WithOfflineCoordinator(offline),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Clean session (CleanSession=true in 3.1.1)
	connectReq := mqtt.Connect{
		Version:    mqtt.ProtocolV311,
		ClientID:   "client-clean",
		CleanStart: true,
	}
	resClean, err := useCase.Connect(connectReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect(resClean.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}

	if len(offline.detachCalls) != 0 {
		t.Fatalf("clean session disconnect triggered detach: %+v", offline.detachCalls)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-042
func TestClusterConnectClaimRestoresOfflineDeliveriesAndSessionPresent(t *testing.T) {
	now := time.Date(2026, time.January, 1, 10, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository() // no local session stored
	queue := newFakeDeliveryQueue()
	routes := routing.NewTable()
	scheduler := &fakeScheduler{}
	publisher := &fakeWillPublisher{}
	coordinator := &fakeOwnershipCoordinator{epoch: 2}

	tenantID, _ := tenancy.NewTenantID("default")
	sKey, _ := tenancy.NewSessionKey(tenantID, "client-claim")
	routingID := routing.SessionID(sKey.RoutingID())
	claimedDeliveries := []deliveryqueue.DeliveryDTO{
		{SessionID: routingID, Topic: "claimed/1", Payload: []byte("p1"), QoS: mqtt.QoSAtMostOnce},
		{SessionID: routingID, Topic: "claimed/2", Payload: []byte("p2"), QoS: mqtt.QoSAtMostOnce},
	}
	offline := &fakeOfflineCoordinator{
		claimResult: OfflineClaimDTO{
			Found:      true,
			Deliveries: claimedDeliveries,
		},
	}

	useCase, err := New(
		clock, repository, queue, routes, scheduler, publisher,
		WithOwnershipCoordinator(coordinator, 2),
		WithOfflineCoordinator(offline),
	)
	if err != nil {
		t.Fatal(err)
	}

	// 1. CleanStart=0 on node with no local session
	connectReq := mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		ClientID:   "client-claim",
		CleanStart: false,
	}
	res, err := useCase.Connect(connectReq)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted {
		t.Fatalf("connect rejected: %v", res.ConnAck.ReasonCode)
	}
	// SessionPresent must be true because offline state was found
	if !res.ConnAck.SessionPresent {
		t.Fatalf("SessionPresent = false, want true")
	}

	// Deliveries must be enqueued locally
	queued, _ := queue.ListQueued(res.SessionID, now)
	if len(queued) != 2 || queued[0].Topic != "claimed/1" || queued[1].Topic != "claimed/2" {
		t.Fatalf("queued deliveries mismatch: %+v", queued)
	}

	// 2. CleanStart=1: claim is performed, but SessionPresent must be false and deliveries discarded
	repository2 := newFakeSessionRepository()
	queue2 := newFakeDeliveryQueue()
	useCase2, _ := New(
		clock, repository2, queue2, routes, scheduler, publisher,
		WithOwnershipCoordinator(coordinator, 2),
		WithOfflineCoordinator(offline),
	)
	connectReqClean := mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		ClientID:   "client-claim-clean",
		CleanStart: true,
	}
	resClean, err := useCase2.Connect(connectReqClean)
	if err != nil {
		t.Fatal(err)
	}
	if resClean.ConnAck.SessionPresent {
		t.Fatalf("CleanStart=1 gave SessionPresent = true, want false")
	}
	queuedClean, _ := queue2.ListQueued("client-claim-clean", now)
	if len(queuedClean) != 0 {
		t.Fatalf("CleanStart=1 should discard deliveries, got %+v", queuedClean)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-042
func TestClusterConnectClaimFailureRejectsAndRollsBack(t *testing.T) {
	now := time.Date(2026, time.January, 1, 10, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	routes := routing.NewTable()
	scheduler := &fakeScheduler{}
	publisher := &fakeWillPublisher{}
	coordinator := &fakeOwnershipCoordinator{epoch: 1}
	offline := &fakeOfflineCoordinator{claimErr: errors.New("claim quorum unavailable")}
	claimObservedTenantBinding := false
	offline.claimHook = func() {
		if len(queue.registers) != 1 {
			t.Errorf("queue registrations before claim = %d, want 1", len(queue.registers))
			return
		}
		claimObservedTenantBinding = routes.SessionTenant(queue.registers[0]).String() == "default"
	}

	useCase, err := New(
		clock, repository, queue, routes, scheduler, publisher,
		WithOwnershipCoordinator(coordinator, 1),
		WithOfflineCoordinator(offline),
	)
	if err != nil {
		t.Fatal(err)
	}

	connectReq := mqtt.Connect{
		Version:  mqtt.ProtocolV5,
		ClientID: "client-fail",
	}
	res, err := useCase.Connect(connectReq)
	if err != nil {
		t.Fatalf("Connect() returned error: %v", err)
	}
	if res.Accepted {
		t.Fatalf("Connect was accepted, want rejected")
	}
	if res.ConnAck.ReasonCode != 0x88 {
		t.Fatalf("ReasonCode = 0x%02x, want 0x88 (ownership unavailable)", res.ConnAck.ReasonCode)
	}

	// Verify rollback: local session should not exist
	_, found, _ := repository.Load("client-fail")
	if found {
		t.Fatalf("session was saved despite claim failure")
	}
	if !claimObservedTenantBinding {
		t.Fatal("tenant route was not bound before failed claim")
	}
	if got := routes.SessionTenant(queue.registers[0]); got.String() != "" {
		t.Fatalf("tenant route after first-session claim failure = %q, want unbound", got.String())
	}

	if len(queue.registers) != 1 {
		t.Fatalf("queue registrations = %d, want 1", len(queue.registers))
	}
	if queue.registered[queue.registers[0]] {
		t.Fatal("delivery queue remained registered after first-session claim failure")
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-042
func TestClusterConnectClaimFailurePreservesExistingSessionTimer(t *testing.T) {
	now := time.Date(2026, time.January, 1, 10, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	routes := routing.NewTable()
	scheduler := &fakeScheduler{}
	publisher := &fakeWillPublisher{}
	coordinator := &fakeOwnershipCoordinator{epoch: 1}
	offline := &fakeOfflineCoordinator{}

	useCase, err := New(
		clock, repository, queue, routes, scheduler, publisher,
		WithOwnershipCoordinator(coordinator, 1),
		WithOfflineCoordinator(offline),
	)
	if err != nil {
		t.Fatal(err)
	}

	packet := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "client-existing",
		SessionExpiryInterval: 60,
	}
	initial, err := useCase.Connect(packet)
	if err != nil || !initial.Accepted {
		t.Fatalf("initial Connect() = %#v, %v", initial, err)
	}
	pending := deliveryqueue.DeliveryDTO{
		SessionID: initial.SessionID, Topic: "pending/topic",
		Payload: []byte("pending"), QoS: mqtt.QoSAtMostOnce,
	}
	if err := queue.TryEnqueueBatch([]deliveryqueue.DeliveryDTO{pending}, now); err != nil {
		t.Fatal(err)
	}

	offline.detachErr = errors.New("detach quorum unavailable")
	if err := useCase.Disconnect(initial.SessionID, session.DisconnectNormal); err != nil {
		t.Fatalf("Disconnect() error = %v", err)
	}
	timerKey := string(initial.SessionID)
	timers, exists := useCase.activeTimers[timerKey]
	if !exists || timers == nil || timers.expiryTimer == nil {
		t.Fatalf("expiry timer after disconnect = %#v, want active", timers)
	}
	expiryTimer, isFakeTimer := timers.expiryTimer.(*fakeTimer)
	if !isFakeTimer || expiryTimer.stopped {
		t.Fatalf("expiry timer after disconnect = %#v, want active fake timer", timers.expiryTimer)
	}
	before, found, err := repository.Load(packet.ClientID)
	if err != nil || !found {
		t.Fatalf("stored session after disconnect = (%#v, %v, %v)", before, found, err)
	}
	expiresAt, hasExpiry := before.ExpiresAt()
	if !hasExpiry || !expiresAt.Equal(now.Add(60*time.Second)) {
		t.Fatalf("session expiry = (%v, %v), want %v", expiresAt, hasExpiry, now.Add(60*time.Second))
	}

	offline.claimErr = errors.New("claim quorum unavailable")
	coordinator.epoch = 2
	rejected, err := useCase.Connect(packet)
	if err != nil || rejected.Accepted || rejected.ConnAck.ReasonCode != 0x88 {
		t.Fatalf("reconnect with failed CLAIM = %#v, %v, want ownership-unavailable rejection", rejected, err)
	}

	after, found, err := repository.Load(packet.ClientID)
	if err != nil || !found || after.Connected() {
		t.Fatalf("stored session after failed CLAIM = (%#v, %v, %v), want prior disconnected session", after, found, err)
	}
	afterExpiry, hasExpiry := after.ExpiresAt()
	if !hasExpiry || !afterExpiry.Equal(expiresAt) {
		t.Fatalf("restored session expiry = (%v, %v), want %v", afterExpiry, hasExpiry, expiresAt)
	}
	if got := useCase.activeTimers[timerKey]; got != timers || expiryTimer.stopped {
		t.Fatalf("expiry timer after failed CLAIM = %#v, stopped=%v; want same active timer %#v", got, expiryTimer.stopped, timers)
	}
	queued, err := queue.ListQueued(initial.SessionID, now)
	if err != nil || len(queued) != 1 || queued[0].Topic != pending.Topic {
		t.Fatalf("queued deliveries after failed CLAIM = %#v, %v, want prior pending delivery", queued, err)
	}
	if got := routes.SessionTenant(initial.SessionID).String(); got != "default" {
		t.Fatalf("tenant route after existing-session CLAIM failure = %q, want default", got)
	}
}
