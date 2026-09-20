package subscribe

import (
	"errors"
	"reflect"
	"testing"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

type recordingQueue struct {
	deliveries []deliveryqueue.DeliveryDTO
	batchSizes []int
	err        error
}

func (queue *recordingQueue) Register(routing.SessionID) error { return nil }
func (queue *recordingQueue) Remove(routing.SessionID)         {}
func (queue *recordingQueue) TryEnqueue(delivery deliveryqueue.DeliveryDTO) error {
	return queue.TryEnqueueBatch([]deliveryqueue.DeliveryDTO{delivery})
}
func (queue *recordingQueue) TryEnqueueBatch(deliveries []deliveryqueue.DeliveryDTO) error {
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
func (queue *recordingQueue) Drain(routing.SessionID) ([]deliveryqueue.DeliveryDTO, error) {
	return nil, nil
}

type recordingRetained struct {
	messages []retained.MessageDTO
	err      error
}

func (repository *recordingRetained) Replace(message retained.MessageDTO) error { return nil }
func (repository *recordingRetained) Delete(string) error                       { return nil }
func (repository *recordingRetained) List() ([]retained.MessageDTO, error) {
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
	result, err := useCase.Subscribe("client-a", packet)
	if err != nil {
		t.Fatal(err)
	}
	if result.Ack.Version != packet.Version || result.Ack.PacketID != packet.PacketID {
		t.Fatalf("ack identity = %#v, want version %d packet id %d", result.Ack, packet.Version, packet.PacketID)
	}
	wantReasons := []byte{0x02, 0x8F, 0x9E, 0x00, 0x01}
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
	if len(matches) != 1 || matches[0].SessionID != "client-a" || matches[0].QoS != mqtt.QoSExactlyOnce {
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
	first, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{base}})
	if err != nil {
		t.Fatal(err)
	}
	if first.RetainedQueued != 1 || len(queue.deliveries) != 1 || queue.deliveries[0].Topic != "a/two" || queue.deliveries[0].QoS != mqtt.QoSAtLeastOnce || !queue.deliveries[0].Retain {
		t.Fatalf("first replay = %#v, count %d; want remote min-QoS replay", queue.deliveries, first.RetainedQueued)
	}
	if _, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 2, Subscriptions: []mqtt.Subscription{{Filter: "a/#", QoS: mqtt.QoSAtMostOnce, RetainHandling: mqtt.SendRetainedIfNew}}}); err != nil {
		t.Fatal(err)
	}
	if len(queue.deliveries) != 1 {
		t.Fatalf("RH=1 replayed an existing filter: %#v", queue.deliveries)
	}
	third, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 3, Subscriptions: []mqtt.Subscription{{Filter: "a/#", QoS: mqtt.QoSAtMostOnce, RetainHandling: mqtt.DoNotSendRetained}}})
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
	})
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
	result, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 4, Subscriptions: []mqtt.Subscription{{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}}})
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
	if _, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, Subscriptions: []mqtt.Subscription{{Filter: "events/#"}}}); !errors.Is(err, deliveryqueue.ErrUnavailable) {
		t.Fatalf("queue unavailable error = %v, want %v", err, deliveryqueue.ErrUnavailable)
	}
	repository.err = errors.New("retained repository stopped")
	queue.err = nil
	if _, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, Subscriptions: []mqtt.Subscription{{Filter: "events/#", RetainHandling: mqtt.SendRetainedIfNew}}}); !errors.Is(err, repository.err) {
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
	wantReasons := []byte{0x00, 0x11, 0x8F, 0x8F}
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
	v5, err := useCase.Subscribe("", mqtt.Subscribe{Version: mqtt.ProtocolV5, Subscriptions: []mqtt.Subscription{{Filter: "topic"}, {Filter: "$share/group/topic"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v5.Ack.ReasonCodes, []byte{0x8F, 0x8F}) {
		t.Fatalf("MQTT 5 validation reasons = % X", v5.Ack.ReasonCodes)
	}
	v311, err := useCase.Subscribe("client", mqtt.Subscribe{Version: mqtt.ProtocolV311, Subscriptions: []mqtt.Subscription{{Filter: "topic", QoS: 3}, {Filter: "$share/group/topic"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v311.Ack.ReasonCodes, []byte{0x80, 0x80}) {
		t.Fatalf("MQTT 3.1.1 validation reasons = % X", v311.Ack.ReasonCodes)
	}
}
