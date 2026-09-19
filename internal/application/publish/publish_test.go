package publish

import (
	"errors"
	"reflect"
	"testing"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

type recordingQueue struct {
	deliveries []DeliveryDTO
	errors     map[routing.SessionID]error
	calls      []routing.SessionID
}

// recordingQueue models the DeliveryQueue contract by copying retained payloads.
func (queue *recordingQueue) TryEnqueue(delivery DeliveryDTO) error {
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

// Traceability: PRD MQTT-02/MQTT-06/MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §3.2 and §7.
func TestNewRejectsNilDependencies(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{}
	if _, err := New(nil, queue); !errors.Is(err, ErrNilRoutingTable) {
		t.Fatalf("nil routes error = %v, want %v", err, ErrNilRoutingTable)
	}
	if _, err := New(routes, nil); !errors.Is(err, ErrNilDeliveryQueue) {
		t.Fatalf("nil queue error = %v, want %v", err, ErrNilDeliveryQueue)
	}
}

// Traceability: PRD MQTT-02/MQTT-06/MQTT-11/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §3.2 item 1 and §7; MQTT-3.3.1-2/4.
func TestPublishFanoutUsesOverlapQoSAndRetain(t *testing.T) {
	routes := routing.NewTable()
	if err := routes.Subscribe("session-b", "sensors/#", mqtt.QoSExactlyOnce); err != nil {
		t.Fatal(err)
	}
	if err := routes.Subscribe("session-a", "sensors/+/temp", mqtt.QoSAtLeastOnce); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	useCase, err := New(routes, queue)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("temperature")
	result, err := useCase.Publish(mqtt.Publish{
		Version: mqtt.ProtocolV5,
		QoS:     mqtt.QoSExactlyOnce,
		Retain:  true,
		Topic:   "sensors/room1/temp",
		Payload: payload,
	})
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
	if !queue.deliveries[0].Retain || !queue.deliveries[1].Retain {
		t.Fatal("retain flag was not forwarded")
	}
	if string(queue.deliveries[0].Payload) != "temperature" || string(queue.deliveries[1].Payload) != "temperature" {
		t.Fatalf("delivery payloads = %q, %q; want temperature, temperature", queue.deliveries[0].Payload, queue.deliveries[1].Payload)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestPublishContinuesAfterSaturationAndUnavailableSessions(t *testing.T) {
	routes := routing.NewTable()
	for _, sessionID := range []routing.SessionID{"a", "b", "c"} {
		if err := routes.Subscribe(sessionID, "events/#", mqtt.QoSAtMostOnce); err != nil {
			t.Fatal(err)
		}
	}
	queue := &recordingQueue{errors: map[routing.SessionID]error{
		"b": ErrQueueFull,
		"c": ErrQueueUnavailable,
	}}
	useCase, err := New(routes, queue)
	if err != nil {
		t.Fatal(err)
	}
	result, err := useCase.Publish(mqtt.Publish{Version: mqtt.ProtocolV311, Topic: "events/created"})
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

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.
func TestPublishReturnsPartialResultForUnknownQueueError(t *testing.T) {
	routes := routing.NewTable()
	for _, sessionID := range []routing.SessionID{"a", "b"} {
		if err := routes.Subscribe(sessionID, "events/#", mqtt.QoSAtMostOnce); err != nil {
			t.Fatal(err)
		}
	}
	unknown := errors.New("writer stopped")
	queue := &recordingQueue{errors: map[routing.SessionID]error{"b": unknown}}
	useCase, err := New(routes, queue)
	if err != nil {
		t.Fatal(err)
	}
	result, err := useCase.Publish(mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/created"})
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
	useCase, err := New(routes, queue)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Publish(mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/+"}); !errors.Is(err, routing.ErrInvalidTopicName) {
		t.Fatalf("error = %v, want invalid topic name", err)
	}
	if len(queue.calls) != 0 {
		t.Fatalf("queue calls = %#v, want none", queue.calls)
	}
}
