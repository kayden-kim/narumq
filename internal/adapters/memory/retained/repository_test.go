package retained

import (
	"errors"
	"reflect"
	"testing"
	"time"

	applicationretained "narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

// Traceability: PRD MQTT-04/MQTT-08/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5.
func TestRetainedRepositoryReplaceDeleteCapacityAndOwnership(t *testing.T) {
	repository, err := New(2, 50)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("first")
	first := applicationretained.MessageDTO{PublisherSessionID: "publisher-a", Topic: "a", Payload: payload, QoS: mqtt.QoSAtMostOnce}
	if err := repository.Replace(first, time.Time{}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	listed, err := repository.List(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || !reflect.DeepEqual(listed[0].Payload, []byte("first")) {
		t.Fatalf("List() = %#v, want owned first payload", listed)
	}
	listed[0].Payload[0] = 'Y'
	listedAgain, err := repository.List(time.Time{})
	if err != nil || !reflect.DeepEqual(listedAgain[0].Payload, []byte("first")) {
		t.Fatalf("List() after caller mutation = %#v, %v", listedAgain, err)
	}

	if err := repository.Replace(applicationretained.MessageDTO{PublisherSessionID: "publisher-a", Topic: "a", Payload: []byte("replacement"), QoS: mqtt.QoSAtLeastOnce}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Replace(applicationretained.MessageDTO{PublisherSessionID: "publisher-b", Topic: "b", Payload: []byte("second"), QoS: mqtt.QoSAtMostOnce}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Replace(applicationretained.MessageDTO{PublisherSessionID: "publisher-c", Topic: "c", Payload: []byte("overflow"), QoS: mqtt.QoSAtMostOnce}, time.Time{}); !errors.Is(err, applicationretained.ErrFull) {
		t.Fatalf("capacity Replace() error = %v, want %v", err, applicationretained.ErrFull)
	}
	listed, err = repository.List(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || listed[0].Topic != "a" || listed[1].Topic != "b" || string(listed[0].Payload) != "replacement" {
		t.Fatalf("capacity preserved values = %#v", listed)
	}

	if err := repository.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := repository.Delete("a"); err != nil {
		t.Fatal(err)
	}
	listed, err = repository.List(time.Time{})
	if err != nil || len(listed) != 1 || listed[0].Topic != "b" {
		t.Fatalf("List() after idempotent Delete = %#v, %v", listed, err)
	}
	if err := repository.Replace(applicationretained.MessageDTO{PublisherSessionID: "publisher-c", Topic: "c", Payload: []byte("overflow"), QoS: mqtt.QoSAtMostOnce}, time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedRepositoryRejectsInvalidValues(t *testing.T) {
	if _, err := New(0, 1); !errors.Is(err, ErrInvalidCapacity) {
		t.Fatalf("New(0,1) error = %v, want %v", err, ErrInvalidCapacity)
	}
	repository, err := New(1, 32)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		message applicationretained.MessageDTO
		want    error
	}{
		{name: "empty-topic", message: applicationretained.MessageDTO{PublisherSessionID: "client", Payload: []byte("value")}, want: routing.ErrInvalidTopicName},
		{name: "empty-payload", message: applicationretained.MessageDTO{PublisherSessionID: "client", Topic: "topic"}, want: ErrInvalidMessage},
		{name: "empty-publisher", message: applicationretained.MessageDTO{Topic: "topic", Payload: []byte("value")}, want: routing.ErrInvalidSessionID},
		{name: "invalid-qos", message: applicationretained.MessageDTO{PublisherSessionID: "client", Topic: "topic", Payload: []byte("value"), QoS: mqtt.QoS(3)}, want: routing.ErrInvalidQoS},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := repository.Replace(testCase.message, time.Time{}); !errors.Is(err, testCase.want) {
				t.Fatalf("Replace() error = %v, want %v", err, testCase.want)
			}
		})
	}
	if err := repository.Delete(""); !errors.Is(err, routing.ErrInvalidTopicName) {
		t.Fatalf("Delete(\"\") error = %v, want %v", err, routing.ErrInvalidTopicName)
	}
}

// Traceability: PRD MQTT-04/MQTT-08/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5; MQTT-3.3.2-5.
func TestRetainedRepositoryExpiryPurgesBytesBeforeReplace(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	repository, err := New(1, 30)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Replace(applicationretained.MessageDTO{
		PublisherSessionID: "publisher-a",
		Topic:              "expired",
		Payload:            []byte("value"),
		ExpiresAt:          now.Add(time.Second),
	}, now); err != nil {
		t.Fatal(err)
	}
	if err := repository.Replace(applicationretained.MessageDTO{
		PublisherSessionID: "publisher-b",
		Topic:              "replacement",
		Payload:            []byte("value"),
	}, now.Add(time.Second)); err != nil {
		t.Fatalf("replace after expiry = %v", err)
	}
	messages, err := repository.List(now.Add(time.Second))
	if err != nil || len(messages) != 1 || messages[0].Topic != "replacement" {
		t.Fatalf("list after expiry = %#v, %v", messages, err)
	}
}
