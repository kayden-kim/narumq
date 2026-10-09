package publish

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	applicationretained "narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type recordingQueue struct {
	deliveries []applicationdeliveryqueue.DeliveryDTO
	attempts   []applicationdeliveryqueue.DeliveryDTO
	errors     map[routing.SessionID]error
	calls      []routing.SessionID
	onEnqueue  func()
	awaitCalls []routing.SessionID
	awaitHook  func(routing.SessionID) error
}

// recordingQueue models the application delivery queue contract by copying retained payloads.
func (queue *recordingQueue) TryEnqueue(delivery applicationdeliveryqueue.DeliveryDTO, now time.Time) error {
	if queue.onEnqueue != nil {
		queue.onEnqueue()
	}
	queue.attempts = append(queue.attempts, delivery)
	queue.calls = append(queue.calls, delivery.SessionID)
	if err := queue.errors[delivery.SessionID]; err != nil {
		return err
	}
	if delivery.Payload != nil {
		delivery.Payload = append([]byte(nil), delivery.Payload...)
	}
	queue.deliveries = append(queue.deliveries, delivery)
	return nil
}

func (queue *recordingQueue) TryEnqueueBatch(deliveries []applicationdeliveryqueue.DeliveryDTO, now time.Time) error {
	for _, delivery := range deliveries {
		if err := queue.TryEnqueue(delivery, now); err != nil {
			return err
		}
	}
	return nil
}

func (queue *recordingQueue) Register(sessionID routing.SessionID) error {
	return nil
}

func (queue *recordingQueue) Remove(sessionID routing.SessionID) {}

func (*recordingQueue) PeekBatch(routing.SessionID, time.Time, []applicationdeliveryqueue.DeliveryDTO) (int, error) {
	return 0, nil
}
func (*recordingQueue) PromoteBatch([]applicationdeliveryqueue.OutboundDTO, time.Time) (int, error) {
	return 0, nil
}
func (*recordingQueue) Peek(routing.SessionID, time.Time) (applicationdeliveryqueue.DeliveryDTO, bool, error) {
	return applicationdeliveryqueue.DeliveryDTO{}, false, nil
}
func (*recordingQueue) CompleteHead(routing.SessionID) error { return nil }
func (*recordingQueue) PromoteHead(applicationdeliveryqueue.OutboundDTO, time.Time) error {
	return nil
}
func (*recordingQueue) ListQueued(routing.SessionID, time.Time) ([]applicationdeliveryqueue.DeliveryDTO, error) {
	return nil, nil
}
func (*recordingQueue) BeginDetach(routing.SessionID, time.Time) ([]applicationdeliveryqueue.DeliveryDTO, error) {
	return nil, nil
}
func (*recordingQueue) FinishDetach(routing.SessionID, bool) {}
func (queue *recordingQueue) AwaitDetach(_ context.Context, sessionID routing.SessionID) error {
	queue.awaitCalls = append(queue.awaitCalls, sessionID)
	if queue.awaitHook != nil {
		return queue.awaitHook(sessionID)
	}
	return nil
}
func (*recordingQueue) ListOutbound(routing.SessionID) ([]applicationdeliveryqueue.OutboundDTO, error) {
	return nil, nil
}
func (*recordingQueue) ReplaceOutbound(applicationdeliveryqueue.OutboundDTO) error { return nil }
func (*recordingQueue) CompleteOutbound(routing.SessionID, mqtt.PacketIdentifier) error {
	return nil
}

func (queue *recordingQueue) BeginReplaceOutbound(outbound applicationdeliveryqueue.OutboundDTO) (applicationdeliveryqueue.PendingTransition, error) {
	if err := queue.ReplaceOutbound(outbound); err != nil {
		return nil, err
	}
	return applicationdeliveryqueue.CompletedTransition{}, nil
}

func (queue *recordingQueue) BeginCompleteOutbound(outbound applicationdeliveryqueue.OutboundDTO) (applicationdeliveryqueue.PendingTransition, error) {
	if err := queue.CompleteOutbound(outbound.SessionID, outbound.PacketID); err != nil {
		return nil, err
	}
	return applicationdeliveryqueue.CompletedTransition{}, nil
}
func (*recordingQueue) ResetDurable(routing.SessionID, uint64, []applicationdeliveryqueue.DeliveryDTO, []applicationdeliveryqueue.OutboundDTO, time.Time) error {
	return nil
}

type retainedDelete struct {
	tenantID tenancy.TenantID
	topic    string
}

type recordingRetainedRepository struct {
	replaces   []applicationretained.MessageDTO
	deletes    []retainedDelete
	replaceErr error
	deleteErr  error
}

func (repository *recordingRetainedRepository) Replace(message applicationretained.MessageDTO, _ time.Time) error {
	if repository.replaceErr != nil {
		return repository.replaceErr
	}
	message.Payload = append([]byte(nil), message.Payload...)
	repository.replaces = append(repository.replaces, message)
	return nil
}

func (repository *recordingRetainedRepository) Delete(tenantID tenancy.TenantID, topic string) error {
	if repository.deleteErr != nil {
		return repository.deleteErr
	}
	repository.deletes = append(repository.deletes, retainedDelete{tenantID: tenantID, topic: topic})
	return nil
}

func (repository *recordingRetainedRepository) List(time.Time) ([]applicationretained.MessageDTO, error) {
	return nil, nil
}

type fakeClock struct {
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	return clock.now
}

func testNow() time.Time {
	return time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
}

func newPublishUseCase(t *testing.T, routes *routing.Table, queue *recordingQueue, repository *recordingRetainedRepository) (*UseCase, error) {
	t.Helper()
	return New(&fakeClock{now: testNow()}, routes, queue, repository)
}

// Traceability: PRD MQTT-02/MQTT-06/MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §3.2 and §7.
func TestNewRejectsNilDependencies(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{}
	retainedRepository := &recordingRetainedRepository{}
	if _, err := New(nil, routes, queue, retainedRepository); !errors.Is(err, ErrNilClock) {
		t.Fatalf("nil clock error = %v, want %v", err, ErrNilClock)
	}
	clock := &fakeClock{}
	if _, err := New(clock, nil, queue, retainedRepository); !errors.Is(err, ErrNilRoutingTable) {
		t.Fatalf("nil routes error = %v, want %v", err, ErrNilRoutingTable)
	}
	if _, err := New(clock, routes, nil, retainedRepository); !errors.Is(err, ErrNilDeliveryQueue) {
		t.Fatalf("nil queue error = %v, want %v", err, ErrNilDeliveryQueue)
	}
	if _, err := New(clock, routes, queue, nil); !errors.Is(err, ErrNilRetainedRepository) {
		t.Fatalf("nil retained repository error = %v, want %v", err, ErrNilRetainedRepository)
	}
}

// Traceability: PRD MQTT-02/MQTT-06/MQTT-11/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §3.2 item 1 and §7; MQTT-3.3.1-2/4.
func TestPublishFanoutUsesOverlapQoS(t *testing.T) {
	routes := routing.NewTable()
	if _, err := routes.Subscribe("session-b", mqtt.Subscription{Filter: "sensors/#", QoS: mqtt.QoSExactlyOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := routes.Subscribe("session-a", mqtt.Subscription{Filter: "sensors/+/temp", QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	retainedRepository := &recordingRetainedRepository{}
	useCase, err := newPublishUseCase(t, routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("temperature")
	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5,
		QoS:     mqtt.QoSExactlyOnce,
		Topic:   "sensors/room1/temp",
		Payload: payload,
	}, testNow())
	if err != nil {
		t.Fatal(err)
	}
	wantResult := ResultDTO{Delivered: []routing.SessionID{"session-a", "session-b"}, Saturated: []routing.SessionID{}, Unavailable: []routing.SessionID{}}
	if !reflect.DeepEqual(result, wantResult) {
		t.Fatalf("result = %#v, want %#v", result, wantResult)
	}
	if len(queue.deliveries) != 2 {
		t.Fatalf("deliveries = %d, want 2", len(queue.deliveries))
	}
	if queue.deliveries[0].QoS != mqtt.QoSAtLeastOnce || queue.deliveries[1].QoS != mqtt.QoSExactlyOnce {
		t.Fatalf("delivery QoS = %d, %d; want 1, 2", queue.deliveries[0].QoS, queue.deliveries[1].QoS)
	}
	if queue.deliveries[0].Retain || queue.deliveries[1].Retain {
		t.Fatal("non-retained publication unexpectedly set RETAIN")
	}
	if string(queue.deliveries[0].Payload) != "temperature" || string(queue.deliveries[1].Payload) != "temperature" {
		t.Fatalf("delivery payloads = %q, %q; want temperature, temperature", queue.deliveries[0].Payload, queue.deliveries[1].Payload)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestPublishContinuesAfterSaturationAndUnavailableSessions(t *testing.T) {
	routes := routing.NewTable()
	for _, sessionID := range []routing.SessionID{"a", "b", "c"} {
		if _, err := routes.Subscribe(sessionID, mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
			t.Fatal(err)
		}
	}
	queue := &recordingQueue{errors: map[routing.SessionID]error{
		"b": applicationdeliveryqueue.ErrFull,
		"c": applicationdeliveryqueue.ErrUnavailable,
	}}
	retainedRepository := &recordingRetainedRepository{}
	useCase, err := newPublishUseCase(t, routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	result, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV311, Topic: "events/created"}, testNow())
	if err != nil {
		t.Fatal(err)
	}
	want := ResultDTO{Delivered: []routing.SessionID{"a"}, Saturated: []routing.SessionID{"b"}, Unavailable: []routing.SessionID{"c"}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("result = %#v, want %#v", result, want)
	}
	if !reflect.DeepEqual(queue.calls, []routing.SessionID{"a", "b", "c"}) {
		t.Fatalf("queue calls = %#v, want a,b,c", queue.calls)
	}
}

// Traceability: PRD MQTT-07/TEST-03; MQTT-4.8.2-3.
func TestPublishSharedSubscriptionHandoff(t *testing.T) {
	routes := routing.NewTable()
	filter := "$share/workers/tasks/+"
	subscriptions := []struct {
		sessionID routing.SessionID
		qos       mqtt.QoS
		rap       bool
	}{
		{sessionID: "worker-a", qos: mqtt.QoSAtMostOnce},
		{sessionID: "worker-b", qos: mqtt.QoSAtLeastOnce, rap: true},
		{sessionID: "worker-c", qos: mqtt.QoSExactlyOnce},
	}
	for _, subscription := range subscriptions {
		if _, err := routes.Subscribe(subscription.sessionID, mqtt.Subscription{
			Filter:            filter,
			QoS:               subscription.qos,
			RetainAsPublished: subscription.rap,
		}); err != nil {
			t.Fatalf("Subscribe(%q) error = %v", subscription.sessionID, err)
		}
	}

	initial, err := routes.Match("publisher", "tasks/item", mqtt.QoSExactlyOnce, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(initial) != 1 || initial[0].Primary.SessionID != "worker-a" {
		t.Fatalf("initial shared match = %#v, want worker-a primary", initial)
	}

	queue := &recordingQueue{errors: map[routing.SessionID]error{
		"worker-b": applicationdeliveryqueue.ErrFull,
		"worker-c": applicationdeliveryqueue.ErrUnavailable,
	}}
	useCase, err := newPublishUseCase(t, routes, queue, &recordingRetainedRepository{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5,
		QoS:     mqtt.QoSExactlyOnce,
		Topic:   "tasks/item",
		Payload: []byte("payload"),
		Retain:  true,
	}, testNow())
	if err != nil {
		t.Fatal(err)
	}

	wantResult := ResultDTO{
		Delivered:   []routing.SessionID{"worker-a"},
		Saturated:   []routing.SessionID{"worker-b"},
		Unavailable: []routing.SessionID{"worker-c"},
	}
	if !reflect.DeepEqual(result, wantResult) {
		t.Fatalf("result = %#v, want %#v", result, wantResult)
	}
	if !reflect.DeepEqual(queue.calls, []routing.SessionID{"worker-b", "worker-c", "worker-a"}) {
		t.Fatalf("queue calls = %#v, want worker-b,worker-c,worker-a", queue.calls)
	}
	if len(queue.attempts) != 3 {
		t.Fatalf("queue attempts = %d, want 3", len(queue.attempts))
	}
	wantAttempts := []struct {
		sessionID routing.SessionID
		qos       mqtt.QoS
		retain    bool
	}{
		{sessionID: "worker-b", qos: mqtt.QoSAtLeastOnce, retain: true},
		{sessionID: "worker-c", qos: mqtt.QoSExactlyOnce},
		{sessionID: "worker-a", qos: mqtt.QoSAtMostOnce},
	}
	for index, wantAttempt := range wantAttempts {
		attempt := queue.attempts[index]
		if attempt.SessionID != wantAttempt.sessionID || attempt.QoS != wantAttempt.qos || attempt.Retain != wantAttempt.retain {
			t.Fatalf("attempt %d = %#v, want session=%q qos=%d retain=%t", index, attempt, wantAttempt.sessionID, wantAttempt.qos, wantAttempt.retain)
		}
	}
	if len(queue.deliveries) != 1 || queue.deliveries[0].SessionID != "worker-a" {
		t.Fatalf("successful deliveries = %#v, want only worker-a", queue.deliveries)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestPublishReturnsPartialResultForUnknownQueueError(t *testing.T) {
	routes := routing.NewTable()
	for _, sessionID := range []routing.SessionID{"a", "b"} {
		if _, err := routes.Subscribe(sessionID, mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
			t.Fatal(err)
		}
	}
	unknown := errors.New("writer stopped")
	queue := &recordingQueue{errors: map[routing.SessionID]error{"b": unknown}}
	retainedRepository := &recordingRetainedRepository{}
	useCase, err := newPublishUseCase(t, routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	result, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/created"}, testNow())
	if !errors.Is(err, unknown) {
		t.Fatalf("error = %v, want %v", err, unknown)
	}
	if !reflect.DeepEqual(result.Delivered, []routing.SessionID{"a"}) || len(result.Saturated) != 0 || len(result.Unavailable) != 0 {
		t.Fatalf("partial result = %#v, want only a delivered", result)
	}
}

// Traceability: PRD MQTT-06/MQTT-12/TEST-04; docs/specs/mqtt-protocol-and-session.md §3.2.
func TestPublishRejectsInvalidTopicBeforeQueue(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	retainedRepository := &recordingRetainedRepository{}
	useCase, err := newPublishUseCase(t, routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/+"}, testNow()); !errors.Is(err, routing.ErrInvalidTopicName) {
		t.Fatalf("error = %v, want invalid topic name", err)
	}
	if len(queue.calls) != 0 {
		t.Fatalf("queue calls = %#v, want none", queue.calls)
	}
}

// Traceability: PRD MQTT-04/MQTT-08/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5; MQTT-3.3.1-5/6/7.
func TestPublishRetainedReplaceDeleteAndNonRetainedNoop(t *testing.T) {
	routes := routing.NewTable()
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := routes.BindSessionTenant("publisher", tenantID); err != nil {
		t.Fatal(err)
	}
	if err := routes.BindSessionTenant("subscriber", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := routes.Subscribe("subscriber", mqtt.Subscription{Filter: "sensors/#", QoS: mqtt.QoSAtMostOnce, RetainAsPublished: true}); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	retainedRepository := &recordingRetainedRepository{}
	useCase, err := newPublishUseCase(t, routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("old")
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtMostOnce, Retain: true, Topic: "sensors/state", Payload: payload}, testNow()); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if len(retainedRepository.replaces) != 1 || string(retainedRepository.replaces[0].Payload) != "old" ||
		retainedRepository.replaces[0].PublisherSessionID != "publisher" || retainedRepository.replaces[0].TenantID != tenantID {
		t.Fatalf("retained replaces = %#v, want owned tenant-scoped old value", retainedRepository.replaces)
	}
	if len(queue.deliveries) != 1 || !queue.deliveries[0].Retain {
		t.Fatalf("live retained delivery = %#v, want RETAIN=true", queue.deliveries)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtMostOnce, Topic: "sensors/state", Payload: []byte("live")}, testNow()); err != nil {
		t.Fatal(err)
	}
	if len(retainedRepository.replaces) != 1 {
		t.Fatalf("non-retained publish changed repository = %#v", retainedRepository.replaces)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtMostOnce, Retain: true, Topic: "sensors/state"}, testNow()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retainedRepository.deletes, []retainedDelete{{tenantID: tenantID, topic: "sensors/state"}}) {
		t.Fatalf("retained deletes = %#v, want exact tenant and topic", retainedRepository.deletes)
	}
	if len(queue.deliveries) != 3 || !queue.deliveries[2].Retain || len(queue.deliveries[2].Payload) != 0 {
		t.Fatalf("zero-byte live delivery = %#v, want retained empty publication", queue.deliveries[2:])
	}
}

// Traceability: PRD MQTT-04/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5.
func TestPublishRetainedFullStopsBeforeFanout(t *testing.T) {
	routes := routing.NewTable()
	if _, err := routes.Subscribe("subscriber", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	retainedRepository := &recordingRetainedRepository{replaceErr: applicationretained.ErrFull}
	useCase, err := newPublishUseCase(t, routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtMostOnce, Retain: true, Topic: "events/state", Payload: []byte("value")}, testNow()); !errors.Is(err, applicationretained.ErrFull) {
		t.Fatalf("retained capacity error = %v, want %v", err, applicationretained.ErrFull)
	}
	if len(queue.calls) != 0 || len(retainedRepository.replaces) != 0 {
		t.Fatalf("capacity failure caused side effects: queue=%v retained=%#v", queue.calls, retainedRepository.replaces)
	}
}

// Traceability: PRD MQTT-08/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §4 항목 4 and §5.
func TestPublishAppliesNoLocalAndRetainAsPublished(t *testing.T) {
	routes := routing.NewTable()
	for sessionID, subscription := range map[routing.SessionID]mqtt.Subscription{
		"publisher": {Filter: "events/#", QoS: mqtt.QoSAtMostOnce, NoLocal: true, RetainAsPublished: true},
		"rap-false": {Filter: "events/#", QoS: mqtt.QoSAtMostOnce},
		"rap-true":  {Filter: "events/#", QoS: mqtt.QoSAtMostOnce, RetainAsPublished: true},
	} {
		if _, err := routes.Subscribe(sessionID, subscription); err != nil {
			t.Fatal(err)
		}
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	useCase, err := newPublishUseCase(t, routes, queue, &recordingRetainedRepository{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtMostOnce, Retain: true, Topic: "events/state", Payload: []byte("value")}, testNow()); err != nil {
		t.Fatal(err)
	}
	if len(queue.deliveries) != 2 {
		t.Fatalf("deliveries = %#v, want publisher excluded", queue.deliveries)
	}
	for _, delivery := range queue.deliveries {
		if delivery.SessionID == "publisher" {
			t.Fatal("No Local publisher received its own publication")
		}
		if delivery.SessionID == "rap-false" && delivery.Retain {
			t.Fatal("RAP=false delivery retained RETAIN")
		}
		if delivery.SessionID == "rap-true" && !delivery.Retain {
			t.Fatal("RAP=true delivery cleared RETAIN")
		}
	}
}

// Traceability: PRD MQTT-04/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5.
func TestPublishRetainedSupportsQoS1AndQoS2(t *testing.T) {
	for _, qos := range []mqtt.QoS{mqtt.QoSAtLeastOnce, mqtt.QoSExactlyOnce} {
		routes := routing.NewTable()
		queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
		retainedRepository := &recordingRetainedRepository{}
		useCase, err := newPublishUseCase(t, routes, queue, retainedRepository)
		if err != nil {
			t.Fatal(err)
		}
		publishPacket := mqtt.Publish{
			Version: mqtt.ProtocolV5,
			QoS:     qos,
			Retain:  true,
			Topic:   "sensors/temperature",
			Payload: []byte("23.5"),
		}
		if _, err := useCase.Publish("publisher", publishPacket, testNow()); err != nil {
			t.Fatalf("QoS %d retained publish failed: %v", qos, err)
		}
		if len(retainedRepository.replaces) != 1 {
			t.Fatalf("expected 1 retained replacement, got %d", len(retainedRepository.replaces))
		}
		if retainedRepository.replaces[0].QoS != qos {
			t.Fatalf("retained QoS = %v, want %v", retainedRepository.replaces[0].QoS, qos)
		}
		if string(retainedRepository.replaces[0].Payload) != "23.5" {
			t.Fatalf("retained payload = %q, want '23.5'", string(retainedRepository.replaces[0].Payload))
		}
	}
}

// Traceability: PRD MQTT-04/MQTT-08/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §3.2, §5; MQTT-3.3.2-5/6.
func TestPublishStoresOneAbsoluteExpiryDeadlineAndAcceptsExpiredPublication(t *testing.T) {
	now := testNow()
	clock := &fakeClock{now: now}
	routes := routing.NewTable()
	if _, err := routes.Subscribe("subscriber", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	repository := &recordingRetainedRepository{}
	useCase, err := New(clock, routes, queue, repository)
	if err != nil {
		t.Fatal(err)
	}
	receivedAt := now
	packet := mqtt.Publish{
		Version:                  mqtt.ProtocolV5,
		Retain:                   true,
		Topic:                    "events/state",
		Payload:                  []byte("value"),
		HasMessageExpiryInterval: true,
		MessageExpiryInterval:    10,
	}
	if _, err := useCase.Publish("publisher", packet, receivedAt); err != nil {
		t.Fatal(err)
	}
	wantDeadline := receivedAt.Add(10 * time.Second)
	if len(repository.replaces) != 1 || !repository.replaces[0].ExpiresAt.Equal(wantDeadline) {
		t.Fatalf("retained deadline = %#v, want %v", repository.replaces, wantDeadline)
	}
	if len(queue.deliveries) != 1 || !queue.deliveries[0].ExpiresAt.Equal(wantDeadline) {
		t.Fatalf("delivery deadline = %#v, want %v", queue.deliveries, wantDeadline)
	}

	clock.now = wantDeadline
	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version:                  mqtt.ProtocolV5,
		Retain:                   true,
		Topic:                    "events/state",
		Payload:                  []byte("expired"),
		HasMessageExpiryInterval: true,
		MessageExpiryInterval:    1,
	}, wantDeadline.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Delivered) != 0 || len(result.Saturated) != 0 || len(result.Unavailable) != 0 {
		t.Fatalf("expired result = %#v, want accepted without delivery", result)
	}
	if !reflect.DeepEqual(repository.deletes, []retainedDelete{{topic: "events/state"}}) {
		t.Fatalf("expired retained deletes = %#v, want topic deletion", repository.deletes)
	}
}

// Traceability: SEC-06, TEN-04
func TestPublish_UnauthorizedRejection(t *testing.T) {
	now := testNow()
	clock := &fakeClock{now: now}
	routes := routing.NewTable()
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	repository := &recordingRetainedRepository{}

	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	allowedFilter, err := tenancy.NewTopicFilter(tenant, "allowed/#")
	if err != nil {
		t.Fatal(err)
	}
	rule, err := security.NewRule(tenant, "publisher", security.EffectAllow, security.ActionPublish, allowedFilter)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := security.NewPolicy([]security.Rule{rule})
	if err != nil {
		t.Fatal(err)
	}

	useCase, err := New(clock, routes, queue, repository, WithPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}

	principal, err := security.NewPrincipal(
		tenant,
		"client-1",
		"device-1",
		"cred-1",
		[]security.Role{"publisher"},
		now.Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Publish to unauthorized topic with retain
	packet := mqtt.Publish{
		Version: mqtt.ProtocolV5,
		Topic:   "forbidden/topic",
		Payload: []byte("payload"),
		Retain:  true,
		QoS:     mqtt.QoSAtLeastOnce,
	}
	_, err = useCase.Publish("client-1", packet, now, principal)
	if !errors.Is(err, security.ErrNotAuthorized) {
		t.Fatalf("unauthorized publish error = %v, want ErrNotAuthorized", err)
	}

	// Mutation order check: no state modified on rejection!
	if len(repository.replaces) != 0 || len(repository.deletes) != 0 {
		t.Fatalf("retained state mutated on rejected publish: replaces=%v deletes=%v", repository.replaces, repository.deletes)
	}
	if len(queue.deliveries) != 0 {
		t.Fatalf("delivery queue mutated on rejected publish: %v", queue.deliveries)
	}

	// Publish to authorized topic succeeds
	allowedPacket := mqtt.Publish{
		Version: mqtt.ProtocolV5,
		Topic:   "allowed/status",
		Payload: []byte("ok"),
		Retain:  true,
		QoS:     mqtt.QoSAtMostOnce,
	}
	res, err := useCase.Publish("client-1", allowedPacket, now, principal)
	if err != nil {
		t.Fatalf("authorized publish failed: %v", err)
	}
	_ = res
	if len(repository.replaces) != 1 {
		t.Fatalf("expected retained message saved for authorized publish, got %d", len(repository.replaces))
	}
}
