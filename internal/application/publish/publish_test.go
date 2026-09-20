package publish

import (
	"errors"
	"reflect"
	"testing"

	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	applicationretained "narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

type recordingQueue struct {
	deliveries []applicationdeliveryqueue.DeliveryDTO
	errors     map[routing.SessionID]error
	calls      []routing.SessionID
}

// recordingQueue models the application delivery queue contract by copying retained payloads.
func (queue *recordingQueue) TryEnqueue(delivery applicationdeliveryqueue.DeliveryDTO) error {
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

func (queue *recordingQueue) TryEnqueueBatch(deliveries []applicationdeliveryqueue.DeliveryDTO) error {
	for _, delivery := range deliveries {
		if err := queue.TryEnqueue(delivery); err != nil {
			return err
		}
	}
	return nil
}

func (queue *recordingQueue) Register(sessionID routing.SessionID) error {
	return nil
}

func (queue *recordingQueue) Remove(sessionID routing.SessionID) {}

func (queue *recordingQueue) Drain(sessionID routing.SessionID) ([]applicationdeliveryqueue.DeliveryDTO, error) {
	return nil, nil
}

type recordingRetainedRepository struct {
	replaces   []applicationretained.MessageDTO
	deletes    []string
	replaceErr error
	deleteErr  error
}

func (repository *recordingRetainedRepository) Replace(message applicationretained.MessageDTO) error {
	if repository.replaceErr != nil {
		return repository.replaceErr
	}
	message.Payload = append([]byte(nil), message.Payload...)
	repository.replaces = append(repository.replaces, message)
	return nil
}

func (repository *recordingRetainedRepository) Delete(topic string) error {
	if repository.deleteErr != nil {
		return repository.deleteErr
	}
	repository.deletes = append(repository.deletes, topic)
	return nil
}

func (repository *recordingRetainedRepository) List() ([]applicationretained.MessageDTO, error) {
	return nil, nil
}

// Traceability: PRD MQTT-02/MQTT-06/MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §3.2 and §7.
func TestNewRejectsNilDependencies(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{}
	retainedRepository := &recordingRetainedRepository{}
	if _, err := New(nil, queue, retainedRepository); !errors.Is(err, ErrNilRoutingTable) {
		t.Fatalf("nil routes error = %v, want %v", err, ErrNilRoutingTable)
	}
	if _, err := New(routes, nil, retainedRepository); !errors.Is(err, ErrNilDeliveryQueue) {
		t.Fatalf("nil queue error = %v, want %v", err, ErrNilDeliveryQueue)
	}
	if _, err := New(routes, queue, nil); !errors.Is(err, ErrNilRetainedRepository) {
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
	useCase, err := New(routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("temperature")
	result, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5,
		QoS:     mqtt.QoSExactlyOnce,
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
	useCase, err := New(routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	result, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV311, Topic: "events/created"})
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
		if _, err := routes.Subscribe(sessionID, mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
			t.Fatal(err)
		}
	}
	unknown := errors.New("writer stopped")
	queue := &recordingQueue{errors: map[routing.SessionID]error{"b": unknown}}
	retainedRepository := &recordingRetainedRepository{}
	useCase, err := New(routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	result, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/created"})
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
	useCase, err := New(routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/+"}); !errors.Is(err, routing.ErrInvalidTopicName) {
		t.Fatalf("error = %v, want invalid topic name", err)
	}
	if len(queue.calls) != 0 {
		t.Fatalf("queue calls = %#v, want none", queue.calls)
	}
}

// Traceability: PRD MQTT-04/MQTT-08/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5; MQTT-3.3.1-5/6/7.
func TestPublishRetainedReplaceDeleteAndNonRetainedNoop(t *testing.T) {
	routes := routing.NewTable()
	if _, err := routes.Subscribe("subscriber", mqtt.Subscription{Filter: "sensors/#", QoS: mqtt.QoSAtMostOnce, RetainAsPublished: true}); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	retainedRepository := &recordingRetainedRepository{}
	useCase, err := New(routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("old")
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtMostOnce, Retain: true, Topic: "sensors/state", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if len(retainedRepository.replaces) != 1 || string(retainedRepository.replaces[0].Payload) != "old" || retainedRepository.replaces[0].PublisherSessionID != "publisher" {
		t.Fatalf("retained replaces = %#v, want owned old value", retainedRepository.replaces)
	}
	if len(queue.deliveries) != 1 || !queue.deliveries[0].Retain {
		t.Fatalf("live retained delivery = %#v, want RETAIN=true", queue.deliveries)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtMostOnce, Topic: "sensors/state", Payload: []byte("live")}); err != nil {
		t.Fatal(err)
	}
	if len(retainedRepository.replaces) != 1 {
		t.Fatalf("non-retained publish changed repository = %#v", retainedRepository.replaces)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtMostOnce, Retain: true, Topic: "sensors/state"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retainedRepository.deletes, []string{"sensors/state"}) {
		t.Fatalf("retained deletes = %#v, want exact topic", retainedRepository.deletes)
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
	useCase, err := New(routes, queue, retainedRepository)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtMostOnce, Retain: true, Topic: "events/state", Payload: []byte("value")}); !errors.Is(err, applicationretained.ErrFull) {
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
	useCase, err := New(routes, queue, &recordingRetainedRepository{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtMostOnce, Retain: true, Topic: "events/state", Payload: []byte("value")}); err != nil {
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
		useCase, err := New(routes, queue, retainedRepository)
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
		if _, err := useCase.Publish("publisher", publishPacket); err != nil {
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
