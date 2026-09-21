package subscribe

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type recordingQueue struct {
	deliveries []deliveryqueue.DeliveryDTO
	batchSizes []int
	err        error
}

func (queue *recordingQueue) Register(routing.SessionID) error { return nil }
func (queue *recordingQueue) Remove(routing.SessionID)         {}
func (queue *recordingQueue) TryEnqueue(delivery deliveryqueue.DeliveryDTO, now time.Time) error {
	return queue.TryEnqueueBatch([]deliveryqueue.DeliveryDTO{delivery}, now)
}
func (queue *recordingQueue) TryEnqueueBatch(deliveries []deliveryqueue.DeliveryDTO, now time.Time) error {
	queue.batchSizes = append(queue.batchSizes, len(deliveries))
	if queue.err != nil {
		return queue.err
	}
	for _, delivery := range deliveries {
		delivery.Payload = append([]byte(nil), delivery.Payload...)
		queue.deliveries = append(queue.deliveries, delivery)
	}
	return nil
}
func (*recordingQueue) Peek(routing.SessionID, time.Time) (deliveryqueue.DeliveryDTO, bool, error) {
	return deliveryqueue.DeliveryDTO{}, false, nil
}
func (*recordingQueue) CompleteHead(routing.SessionID) error                   { return nil }
func (*recordingQueue) PromoteHead(deliveryqueue.OutboundDTO, time.Time) error { return nil }
func (*recordingQueue) ListOutbound(routing.SessionID) ([]deliveryqueue.OutboundDTO, error) {
	return nil, nil
}
func (*recordingQueue) ReplaceOutbound(deliveryqueue.OutboundDTO) error { return nil }
func (*recordingQueue) CompleteOutbound(routing.SessionID, mqtt.PacketIdentifier) error {
	return nil
}

type recordingRetained struct {
	messages []retained.MessageDTO
	err      error
}

func (repository *recordingRetained) Replace(message retained.MessageDTO, _ time.Time) error {
	return nil
}
func (repository *recordingRetained) Delete(string) error { return nil }
func (repository *recordingRetained) List(time.Time) ([]retained.MessageDTO, error) {
	if repository.err != nil {
		return nil, repository.err
	}
	messages := make([]retained.MessageDTO, len(repository.messages))
	for index, message := range repository.messages {
		messages[index] = message
		messages[index].Payload = append([]byte(nil), message.Payload...)
	}
	return messages, nil
}

func newSubscribeUseCase(t *testing.T, routes *routing.Table, queue *recordingQueue, repository *recordingRetained) *UseCase {
	t.Helper()
	useCase, err := New(routes, queue, repository)
	if err != nil {
		t.Fatal(err)
	}
	return useCase
}

// Traceability: PRD MQTT-06/MQTT-07/MQTT-08/TEST-05; docs/specs/mqtt-protocol-and-session.md §4 항목 5.
func TestNewRejectsNilDependencies(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{}
	repository := &recordingRetained{}
	if _, err := New(nil, queue, repository); !errors.Is(err, ErrNilRoutingTable) {
		t.Fatalf("nil routes error = %v, want %v", err, ErrNilRoutingTable)
	}
	if _, err := New(routes, nil, repository); !errors.Is(err, ErrNilDeliveryQueue) {
		t.Fatalf("nil queue error = %v, want %v", err, ErrNilDeliveryQueue)
	}
	if _, err := New(routes, queue, nil); !errors.Is(err, ErrNilRetainedRepository) {
		t.Fatalf("nil retained repository error = %v, want %v", err, ErrNilRetainedRepository)
	}
}

// Traceability: PRD MQTT-06/MQTT-07/MQTT-08/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §4 항목 1, 4, 5; MQTT-3.8.3-1/2.
func TestSubscribeKeepsOrderedReasonsAndIsolatesFailures(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{}
	repository := &recordingRetained{messages: []retained.MessageDTO{{PublisherSessionID: "client-a", Topic: "sensors/room", Payload: []byte("value"), QoS: mqtt.QoSExactlyOnce}}}
	useCase := newSubscribeUseCase(t, routes, queue, repository)
	packet := mqtt.Subscribe{
		Version: mqtt.ProtocolV5,
		Subscriptions: []mqtt.Subscription{
			{Filter: "sensors/#", QoS: mqtt.QoSExactlyOnce},
			{Filter: "broken/#/filter", QoS: mqtt.QoSAtMostOnce},
			{Filter: "$share/team/sensors/#", QoS: mqtt.QoSAtLeastOnce},
			{Filter: "sensors/room", QoS: mqtt.QoSAtMostOnce, NoLocal: true},
			{Filter: "alerts/+", QoS: mqtt.QoSAtLeastOnce},
		},
	}
	result, err := useCase.Subscribe("client-a", packet, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Ack.Version != packet.Version || result.Ack.PacketID != packet.PacketID {
		t.Fatalf("ack identity = %#v, want version %d packet id %d", result.Ack, packet.Version, packet.PacketID)
	}
	wantReasons := []byte{0x02, 0x8F, 0x01, 0x00, 0x01}
	if !reflect.DeepEqual(result.Ack.ReasonCodes, wantReasons) {
		t.Fatalf("reasons = % X, want % X", result.Ack.ReasonCodes, wantReasons)
	}
	if result.RetainedQueued != 1 || len(queue.deliveries) != 1 || queue.deliveries[0].Topic != "sensors/room" || !queue.deliveries[0].Retain {
		t.Fatalf("retained replay = %#v, count %d; want one retained delivery", queue.deliveries, result.RetainedQueued)
	}
	if _, ok := routes.Lookup("client-a", "broken/#/filter"); ok {
		t.Fatal("invalid filter was committed")
	}
	matches, err := routes.Match("publisher", "sensors/room", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 || matches[0].SessionID != "client-a" || matches[1].SessionID != "client-a" {
		t.Fatalf("sensors route = %#v", matches)
	}
	matches, err = routes.Match("publisher", "alerts/critical", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].SessionID != "client-a" || matches[0].QoS != mqtt.QoSAtLeastOnce {
		t.Fatalf("alerts route = %#v", matches)
	}
}

// Traceability: PRD MQTT-04/MQTT-08/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §4 항목 3/4 and §5; MQTT-3.8.3-3 and MQTT-3.8.4-3/4.
func TestSubscribeRetainHandlingAndReplayQoS(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{}
	repository := &recordingRetained{messages: []retained.MessageDTO{
		{PublisherSessionID: "subscriber", Topic: "a/one", Payload: []byte("local"), QoS: mqtt.QoSExactlyOnce},
		{PublisherSessionID: "other", Topic: "a/two", Payload: []byte("remote"), QoS: mqtt.QoSExactlyOnce},
		{PublisherSessionID: "other", Topic: "b/one", Payload: []byte("outside"), QoS: mqtt.QoSAtLeastOnce},
	}}
	useCase := newSubscribeUseCase(t, routes, queue, repository)
	base := mqtt.Subscription{Filter: "a/#", QoS: mqtt.QoSAtLeastOnce, NoLocal: true, RetainAsPublished: true}
	first, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{base}}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if first.RetainedQueued != 1 || len(queue.deliveries) != 1 || queue.deliveries[0].Topic != "a/two" || queue.deliveries[0].QoS != mqtt.QoSAtLeastOnce || !queue.deliveries[0].Retain {
		t.Fatalf("first replay = %#v, count %d; want remote min-QoS replay", queue.deliveries, first.RetainedQueued)
	}
	if _, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 2, Subscriptions: []mqtt.Subscription{{Filter: "a/#", QoS: mqtt.QoSAtMostOnce, RetainHandling: mqtt.SendRetainedIfNew}}}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if len(queue.deliveries) != 1 {
		t.Fatalf("RH=1 replayed an existing filter: %#v", queue.deliveries)
	}
	third, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 3, Subscriptions: []mqtt.Subscription{{Filter: "a/#", QoS: mqtt.QoSAtMostOnce, RetainHandling: mqtt.DoNotSendRetained}}}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if third.RetainedQueued != 0 || len(queue.deliveries) != 1 {
		t.Fatalf("RH=2 replayed retained values: %#v, count %d", queue.deliveries, third.RetainedQueued)
	}
}

// Traceability: PRD MQTT-08/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §4 항목 4; MQTT-3.8.3-1/2.
func TestSubscribeMQTT311RejectsRuntimeOptions(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{}
	repository := &recordingRetained{}
	useCase := newSubscribeUseCase(t, routes, queue, repository)
	result, err := useCase.Subscribe("client-a", mqtt.Subscribe{
		Version:  mqtt.ProtocolV311,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{
			{Filter: "a", QoS: mqtt.QoSAtMostOnce, RetainAsPublished: true},
			{Filter: "b", QoS: mqtt.QoSAtMostOnce, NoLocal: true},
			{Filter: "c", QoS: mqtt.QoSAtMostOnce, RetainHandling: mqtt.DoNotSendRetained},
		},
	}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{0x80, 0x80, 0x80}) {
		t.Fatalf("reasons = % X, want all unsupported", result.Ack.ReasonCodes)
	}
	for _, filter := range []string{"a", "b", "c"} {
		if _, ok := routes.Lookup("client-a", filter); ok {
			t.Fatalf("MQTT 3.1.1 unsupported option committed for %q", filter)
		}
	}
}

// Traceability: PRD MQTT-04/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5.
func TestSubscribeRetainedCapacityDoesNotCommitRoute(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{err: deliveryqueue.ErrFull}
	repository := &recordingRetained{messages: []retained.MessageDTO{{Topic: "events/one", Payload: []byte("value"), QoS: mqtt.QoSAtMostOnce}}}
	useCase := newSubscribeUseCase(t, routes, queue, repository)
	result, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 4, Subscriptions: []mqtt.Subscription{{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}}}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{0x97}) || result.RetainedQueued != 0 {
		t.Fatalf("capacity result = %#v, want reason 97 and no replay", result)
	}
	if _, ok := routes.Lookup("subscriber", "events/#"); ok {
		t.Fatal("route committed after retained queue capacity failure")
	}
}

// Traceability: PRD MQTT-04/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5.
func TestSubscribePropagatesReplayDependencies(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{err: deliveryqueue.ErrUnavailable}
	repository := &recordingRetained{messages: []retained.MessageDTO{{Topic: "events/one", Payload: []byte("value"), QoS: mqtt.QoSAtMostOnce}}}
	useCase := newSubscribeUseCase(t, routes, queue, repository)
	if _, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, Subscriptions: []mqtt.Subscription{{Filter: "events/#"}}}, time.Time{}); !errors.Is(err, deliveryqueue.ErrUnavailable) {
		t.Fatalf("queue unavailable error = %v, want %v", err, deliveryqueue.ErrUnavailable)
	}
	repository.err = errors.New("retained repository stopped")
	queue.err = nil
	if _, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, Subscriptions: []mqtt.Subscription{{Filter: "events/#", RetainHandling: mqtt.SendRetainedIfNew}}}, time.Time{}); !errors.Is(err, repository.err) {
		t.Fatalf("repository error = %v, want %v", err, repository.err)
	}
	if _, ok := routes.Lookup("subscriber", "events/#"); ok {
		t.Fatal("route committed after replay dependency failure")
	}
}

// Traceability: PRD MQTT-06/MQTT-07/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §4 항목 6; MQTT-3.10.3-1/2 and MQTT-3.11.3-1/2.
func TestUnsubscribeReasonsAndIdempotence(t *testing.T) {
	routes := routing.NewTable()
	if _, err := routes.Subscribe("client-a", mqtt.Subscription{Filter: "sensors/#", QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	useCase := newSubscribeUseCase(t, routes, &recordingQueue{}, &recordingRetained{})
	ack := useCase.Unsubscribe("client-a", mqtt.Unsubscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 10,
		Filters:  []string{"sensors/#", "missing/#", "broken/#/filter", "$share/team/sensors/#"},
	})
	wantReasons := []byte{0x00, 0x11, 0x8F, 0x11}
	if !reflect.DeepEqual(ack.ReasonCodes, wantReasons) {
		t.Fatalf("reasons = % X, want % X", ack.ReasonCodes, wantReasons)
	}
	matches, err := routes.Match("publisher", "sensors/room", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("route remained after unsubscribe: %#v", matches)
	}

	v311Ack := useCase.Unsubscribe("client-a", mqtt.Unsubscribe{Version: mqtt.ProtocolV311, PacketID: 11, Filters: []string{"missing/#", "broken/#/filter"}})
	if v311Ack.Version != mqtt.ProtocolV311 || v311Ack.PacketID != 11 || len(v311Ack.ReasonCodes) != 0 {
		t.Fatalf("MQTT 3.1.1 UNSUBACK = %#v, want packet-id-only", v311Ack)
	}
}

// Traceability: PRD MQTT-06/MQTT-07/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §4; MQTT-3.8.3-1/2.
func TestSubscribeMapsValidationReasonsByProtocol(t *testing.T) {
	useCase := newSubscribeUseCase(t, routing.NewTable(), &recordingQueue{}, &recordingRetained{})
	v5, err := useCase.Subscribe("", mqtt.Subscribe{Version: mqtt.ProtocolV5, Subscriptions: []mqtt.Subscription{{Filter: "topic"}, {Filter: "$share/group/topic"}}}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v5.Ack.ReasonCodes, []byte{0x8F, 0x8F}) {
		t.Fatalf("MQTT 5 validation reasons = % X", v5.Ack.ReasonCodes)
	}
	v311, err := useCase.Subscribe("client", mqtt.Subscribe{Version: mqtt.ProtocolV311, Subscriptions: []mqtt.Subscription{{Filter: "topic", QoS: 3}, {Filter: "$share//topic"}}}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v311.Ack.ReasonCodes, []byte{0x80, 0x80}) {
		t.Fatalf("MQTT 3.1.1 validation reasons = % X", v311.Ack.ReasonCodes)
	}
}

// Traceability: PRD MQTT-07/TEST-03; MQTT-3.8.3-4.
func TestSubscribeSharedSubscriptionRejectsNoLocal(t *testing.T) {
	routes := routing.NewTable()
	useCase := newSubscribeUseCase(t, routes, &recordingQueue{}, &recordingRetained{})

	// MQTT 5: Protocol Error (0x82)
	v5Result, err := useCase.Subscribe("client-v5", mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{
			{Filter: "$share/group/tasks", QoS: mqtt.QoSAtLeastOnce, NoLocal: true},
		},
	}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v5Result.Ack.ReasonCodes, []byte{0x82}) {
		t.Fatalf("v5 reasons = % X, want [0x82]", v5Result.Ack.ReasonCodes)
	}
	if _, ok := routes.Lookup("client-v5", "$share/group/tasks"); ok {
		t.Fatal("route committed after NoLocal protocol error")
	}

	// MQTT 3.1.1: Failure (0x80)
	v3Result, err := useCase.Subscribe("client-v3", mqtt.Subscribe{
		Version:  mqtt.ProtocolV311,
		PacketID: 2,
		Subscriptions: []mqtt.Subscription{
			{Filter: "$share/group/tasks", QoS: mqtt.QoSAtLeastOnce, NoLocal: true},
		},
	}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v3Result.Ack.ReasonCodes, []byte{0x80}) {
		t.Fatalf("v3 reasons = % X, want [0x80]", v3Result.Ack.ReasonCodes)
	}
	if _, ok := routes.Lookup("client-v3", "$share/group/tasks"); ok {
		t.Fatal("v3 route committed after NoLocal error")
	}
}

// Traceability: PRD MQTT-07/TEST-03; MQTT-4.8.2-1.
func TestSubscribeSharedSubscriptionSuppressesRetainedReplay(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{}
	repository := &recordingRetained{messages: []retained.MessageDTO{
		{Topic: "tasks/urgent", Payload: []byte("retained-task"), QoS: mqtt.QoSAtLeastOnce},
	}}
	useCase := newSubscribeUseCase(t, routes, queue, repository)

	result, err := useCase.Subscribe("worker", mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{
			{Filter: "$share/workers/tasks/+", QoS: mqtt.QoSAtLeastOnce, RetainHandling: mqtt.SendRetainedAtSubscribe},
		},
	}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{0x01}) {
		t.Fatalf("reasons = % X, want [0x01]", result.Ack.ReasonCodes)
	}
	if result.RetainedQueued != 0 || len(queue.deliveries) != 0 {
		t.Fatalf("retained was replayed for shared subscription: queued = %d, deliveries = %#v", result.RetainedQueued, queue.deliveries)
	}

	// Live publication matches the worker
	matches, err := routes.Match("publisher", "tasks/urgent", mqtt.QoSAtLeastOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].SessionID != "worker" {
		t.Fatalf("live match = %#v, want worker", matches)
	}
}

// Traceability: PRD MQTT-07/TEST-03; MQTT-4.8.2-1.
func TestSubscribeMQTT311AcceptsSharedSubscription(t *testing.T) {
	routes := routing.NewTable()
	useCase := newSubscribeUseCase(t, routes, &recordingQueue{}, &recordingRetained{})

	result, err := useCase.Subscribe("legacy-client", mqtt.Subscribe{
		Version:  mqtt.ProtocolV311,
		PacketID: 5,
		Subscriptions: []mqtt.Subscription{
			{Filter: "$share/pool/metrics", QoS: mqtt.QoSAtMostOnce},
		},
	}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{0x00}) {
		t.Fatalf("MQTT 3.1.1 shared sub reason = % X, want [0x00]", result.Ack.ReasonCodes)
	}
	if _, ok := routes.Lookup("legacy-client", "$share/pool/metrics"); !ok {
		t.Fatal("shared subscription not committed for MQTT 3.1.1")
	}
}

// Traceability: PRD MQTT-08/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5; MQTT-3.3.2-5/6.
func TestSubscribeReplayPreservesRetainedExpiryDeadline(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	routes := routing.NewTable()
	queue := &recordingQueue{}
	repository := &recordingRetained{messages: []retained.MessageDTO{{
		PublisherSessionID: "publisher",
		Topic:              "events/state",
		Payload:            []byte("value"),
		QoS:                mqtt.QoSAtLeastOnce,
		ExpiresAt:          now.Add(10 * time.Second),
	}}}
	useCase := newSubscribeUseCase(t, routes, queue, repository)
	result, err := useCase.Subscribe("subscriber", mqtt.Subscribe{
		Version: mqtt.ProtocolV5,
		Subscriptions: []mqtt.Subscription{{
			Filter: "events/#",
			QoS:    mqtt.QoSAtMostOnce,
		}},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.RetainedQueued != 1 || len(queue.deliveries) != 1 {
		t.Fatalf("replay result = %#v, deliveries = %#v", result, queue.deliveries)
	}
	if !queue.deliveries[0].ExpiresAt.Equal(now.Add(10 * time.Second)) {
		t.Fatalf("replay deadline = %v, want %v", queue.deliveries[0].ExpiresAt, now.Add(10*time.Second))
	}
}

// Requirements: SEC-06, TEN-04
func TestSubscribe_UnauthorizedRejection(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	allowedFilter, err := tenancy.NewTopicFilter(tenant, "allowed/#")
	if err != nil {
		t.Fatal(err)
	}
	ruleSub, err := security.NewRule(tenant, "client", security.EffectAllow, security.ActionSubscribe, allowedFilter)
	if err != nil {
		t.Fatal(err)
	}
	ruleUnsub, err := security.NewRule(tenant, "client", security.EffectAllow, security.ActionUnsubscribe, allowedFilter)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := security.NewPolicy([]security.Rule{ruleSub, ruleUnsub})
	if err != nil {
		t.Fatal(err)
	}

	routes := routing.NewTable()
	queue := &recordingQueue{}
	repository := &recordingRetained{messages: []retained.MessageDTO{{
		PublisherSessionID: "pub",
		Topic:              "forbidden/topic",
		Payload:            []byte("data"),
		QoS:                mqtt.QoSAtLeastOnce,
	}}}
	useCase, err := New(routes, queue, repository, WithPolicy(policy))
	if err != nil {
		t.Fatal(err)
	}

	principal, err := security.NewPrincipal(
		tenant,
		"client-1",
		"device-1",
		"cred-1",
		[]security.Role{"client"},
		now.Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe to unauthorized filter
	subPacket := mqtt.Subscribe{
		Version:       mqtt.ProtocolV5,
		PacketID:      1,
		Subscriptions: []mqtt.Subscription{{Filter: "forbidden/#", QoS: mqtt.QoSAtLeastOnce}},
	}
	res, err := useCase.Subscribe("client-1", subPacket, now, principal)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ack.ReasonCodes) != 1 || res.Ack.ReasonCodes[0] != 0x87 {
		t.Fatalf("unauthorized subscribe reason = %v, want 0x87", res.Ack.ReasonCodes)
	}
	// Mutation check: route not added, retained not replayed
	if _, found := routes.Lookup("client-1", "forbidden/#"); found {
		t.Fatal("route was added despite authorization failure")
	}
	if len(queue.deliveries) != 0 {
		t.Fatalf("retained replayed despite authorization failure: %v", queue.deliveries)
	}

	// Subscribe to authorized filter succeeds
	subAllowed := mqtt.Subscribe{
		Version:       mqtt.ProtocolV5,
		PacketID:      2,
		Subscriptions: []mqtt.Subscription{{Filter: "allowed/events", QoS: mqtt.QoSAtLeastOnce}},
	}
	res, err = useCase.Subscribe("client-1", subAllowed, now, principal)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ack.ReasonCodes) != 1 || res.Ack.ReasonCodes[0] != 0x01 {
		t.Fatalf("authorized subscribe reason = %v, want 0x01", res.Ack.ReasonCodes)
	}
	if _, found := routes.Lookup("client-1", "allowed/events"); !found {
		t.Fatal("route was not added for authorized subscription")
	}

	// Unsubscribe from unauthorized filter fails without mutating routes
	unsubUnauthorized := mqtt.Unsubscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 3,
		Filters:  []string{"forbidden/something"},
	}
	unsubAck := useCase.Unsubscribe("client-1", unsubUnauthorized, principal)
	if len(unsubAck.ReasonCodes) != 1 || unsubAck.ReasonCodes[0] != 0x87 {
		t.Fatalf("unauthorized unsubscribe reason = %v, want 0x87", unsubAck.ReasonCodes)
	}

	// Unsubscribe from authorized filter succeeds and removes route
	unsubAuthorized := mqtt.Unsubscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 4,
		Filters:  []string{"allowed/events"},
	}
	unsubAck = useCase.Unsubscribe("client-1", unsubAuthorized, principal)
	if len(unsubAck.ReasonCodes) != 1 || unsubAck.ReasonCodes[0] != 0x00 {
		t.Fatalf("authorized unsubscribe reason = %v, want 0x00", unsubAck.ReasonCodes)
	}
	if _, found := routes.Lookup("client-1", "allowed/events"); found {
		t.Fatal("route was not removed for authorized unsubscription")
	}
}
