package routing

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"narumq/internal/domain/mqtt"
)

// Traceability: PRD MQTT-06/TEST-03/04; MQTT-4.7.1-1/2 and MQTT-4.7.2-1/2.
func TestValidateTopicFiltersAndNames(t *testing.T) {
	filterCases := []struct {
		name    string
		filter  string
		wantErr error
	}{
		{name: "empty", filter: "", wantErr: ErrInvalidTopicFilter},
		{name: "embedded-plus", filter: "sensors/te+mp", wantErr: ErrInvalidTopicFilter},
		{name: "embedded-hash", filter: "sensors/te#mp", wantErr: ErrInvalidTopicFilter},
		{name: "hash-not-last", filter: "sensors/#/temp", wantErr: ErrInvalidTopicFilter},
		{name: "shared", filter: "$share/group/sensors/#", wantErr: ErrSharedSubscriptionUnsupported},
		{name: "nul", filter: "sensors/\x00", wantErr: ErrInvalidTopicFilter},
		{name: "valid-empty-level", filter: "sensors//temp", wantErr: nil},
		{name: "valid-wildcards", filter: "sensors/+/temp/#", wantErr: nil},
	}
	for _, testCase := range filterCases {
		t.Run("filter/"+testCase.name, func(t *testing.T) {
			err := ValidateTopicFilter(testCase.filter)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("error = %v, want %v", err, testCase.wantErr)
			}
		})
	}

	topicCases := []struct {
		name    string
		topic   string
		wantErr error
	}{
		{name: "empty", topic: "", wantErr: ErrInvalidTopicName},
		{name: "wildcard-plus", topic: "sensors/+", wantErr: ErrInvalidTopicName},
		{name: "wildcard-hash", topic: "sensors/#", wantErr: ErrInvalidTopicName},
		{name: "nul", topic: "sensors/\x00", wantErr: ErrInvalidTopicName},
		{name: "valid-empty-level", topic: "sensors//temp", wantErr: nil},
		{name: "valid-system", topic: "$SYS/broker/uptime", wantErr: nil},
	}
	for _, testCase := range topicCases {
		t.Run("topic/"+testCase.name, func(t *testing.T) {
			err := ValidateTopicName(testCase.topic)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("error = %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

// Traceability: PRD MQTT-06/TEST-03/04; MQTT-4.7.1-1/2 and MQTT-4.7.2-1/2.
func TestWildcardMatchBoundariesAndSystemTopics(t *testing.T) {
	table := NewTable()
	for sessionID, filter := range map[SessionID]string{
		"exact":  "sport/tennis/player1",
		"single": "sport/+/player1",
		"multi":  "sport/#",
		"all":    "#",
		"system": "$SYS/#",
		"root":   "+/broker/uptime",
	} {
		if err := table.Subscribe(sessionID, filter, mqtt.QoSExactlyOnce); err != nil {
			t.Fatalf("Subscribe(%q, %q) error = %v", sessionID, filter, err)
		}
	}

	cases := []struct {
		name  string
		topic string
		want  []Match
	}{
		{
			name:  "exact-level-match",
			topic: "sport/tennis/player1",
			want: []Match{
				{SessionID: "all", QoS: mqtt.QoSExactlyOnce},
				{SessionID: "exact", QoS: mqtt.QoSExactlyOnce},
				{SessionID: "multi", QoS: mqtt.QoSExactlyOnce},
				{SessionID: "single", QoS: mqtt.QoSExactlyOnce},
			},
		},
		{
			name:  "hash-parent-match",
			topic: "sport",
			want:  []Match{{SessionID: "all", QoS: mqtt.QoSExactlyOnce}, {SessionID: "multi", QoS: mqtt.QoSExactlyOnce}},
		},
		{
			name:  "empty-level-single",
			topic: "sport//player1",
			want:  []Match{{SessionID: "all", QoS: mqtt.QoSExactlyOnce}, {SessionID: "multi", QoS: mqtt.QoSExactlyOnce}, {SessionID: "single", QoS: mqtt.QoSExactlyOnce}},
		},
		{
			name:  "system-topic-explicit-only",
			topic: "$SYS/broker/uptime",
			want:  []Match{{SessionID: "system", QoS: mqtt.QoSExactlyOnce}},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := table.Match(testCase.topic, mqtt.QoSExactlyOnce)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, testCase.want) {
				t.Fatalf("matches = %#v, want %#v", got, testCase.want)
			}
		})
	}
}

// Traceability: PRD MQTT-06/MQTT-11/TEST-03/04; docs/specs/mqtt-protocol-and-session.md §4 항목 2, 6.
func TestSubscriptionReplacementUnsubscribeAndOverlapQoS(t *testing.T) {
	table := NewTable()
	if err := table.Subscribe("client", "sensors/#", mqtt.QoSExactlyOnce); err != nil {
		t.Fatal(err)
	}
	if err := table.Subscribe("client", "sensors/+/temp", mqtt.QoSAtMostOnce); err != nil {
		t.Fatal(err)
	}
	if err := table.Subscribe("client", "sensors/#", mqtt.QoSAtLeastOnce); err != nil {
		t.Fatal(err)
	}

	matches, err := table.Match("sensors/room/temp", mqtt.QoSExactlyOnce)
	if err != nil {
		t.Fatal(err)
	}
	want := []Match{{SessionID: "client", QoS: mqtt.QoSAtLeastOnce}}
	if !reflect.DeepEqual(matches, want) {
		t.Fatalf("overlap matches = %#v, want %#v", matches, want)
	}

	removed, err := table.Unsubscribe("client", "sensors/#")
	if err != nil || !removed {
		t.Fatalf("first unsubscribe = (%v, %v), want (true, nil)", removed, err)
	}
	removed, err = table.Unsubscribe("client", "sensors/#")
	if err != nil || removed {
		t.Fatalf("idempotent unsubscribe = (%v, %v), want (false, nil)", removed, err)
	}
	matches, err = table.Match("sensors/room/temp", mqtt.QoSExactlyOnce)
	if err != nil {
		t.Fatal(err)
	}
	want = []Match{{SessionID: "client", QoS: mqtt.QoSAtMostOnce}}
	if !reflect.DeepEqual(matches, want) {
		t.Fatalf("remaining matches = %#v, want %#v", matches, want)
	}
}

// Traceability: PRD MQTT-06/MQTT-12/TEST-04; MQTT-1.5.3-1/2 and MQTT-4.7.1-1/2.
func TestRoutingValidationErrors(t *testing.T) {
	table := NewTable()
	cases := []struct {
		name string
		call func() error
		want error
	}{
		{name: "empty-session-subscribe", call: func() error { return table.Subscribe("", "a/#", mqtt.QoSAtMostOnce) }, want: ErrInvalidSessionID},
		{name: "invalid-qos", call: func() error { return table.Subscribe("client", "a/#", mqtt.QoS(3)) }, want: ErrInvalidQoS},
		{name: "invalid-publish-qos", call: func() error { _, err := table.Match("a", mqtt.QoS(3)); return err }, want: ErrInvalidQoS},
		{name: "shared-unsubscribe", call: func() error { _, err := table.Unsubscribe("client", "$share/g/a"); return err }, want: ErrSharedSubscriptionUnsupported},
		{name: "invalid-session-unsubscribe", call: func() error { _, err := table.Unsubscribe("", "a"); return err }, want: ErrInvalidSessionID},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := testCase.call(); !errors.Is(err, testCase.want) {
				t.Fatalf("error = %v, want %v", err, testCase.want)
			}
		})
	}
}

// Traceability: PRD MQTT-06/MQTT-11/TEST-03/04/05;
// docs/specs/mqtt-protocol-and-session.md §4 항목 1, 2, 6;
// MQTT-4.7.1-1/2 and MQTT-4.7.2-1/2.
func TestTableConcurrentSubscribeUnsubscribeAndMatch(t *testing.T) {
	table := NewTable()
	sessionIDs := []SessionID{"session-a", "session-b", "session-c", "session-d"}
	filters := []string{"events/+", "events/#", "events/room"}
	for index, sessionID := range sessionIDs {
		if err := table.Subscribe(sessionID, filters[index%len(filters)], mqtt.QoSAtLeastOnce); err != nil {
			t.Fatal(err)
		}
	}

	var waitGroup sync.WaitGroup
	for worker := range sessionIDs {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			sessionID := sessionIDs[worker]
			for iteration := 0; iteration < 200; iteration++ {
				switch iteration % 3 {
				case 0:
					if err := table.Subscribe(sessionID, filters[iteration%len(filters)], mqtt.QoS(iteration%3)); err != nil {
						t.Errorf("Subscribe(%q) error = %v", sessionID, err)
					}
				case 1:
					if _, err := table.Unsubscribe(sessionID, filters[iteration%len(filters)]); err != nil {
						t.Errorf("Unsubscribe(%q) error = %v", sessionID, err)
					}
				default:
					if _, err := table.Match("events/room", mqtt.QoSExactlyOnce); err != nil {
						t.Errorf("Match error = %v", err)
					}
				}
			}
		}(worker)
	}
	waitGroup.Wait()
}
