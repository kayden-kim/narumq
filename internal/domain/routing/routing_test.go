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
		if _, err := table.Subscribe(sessionID, mqtt.Subscription{Filter: filter, QoS: mqtt.QoSExactlyOnce}); err != nil {
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
			got, err := table.Match("publisher", testCase.topic, mqtt.QoSExactlyOnce, false)
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
	if _, err := table.Subscribe("client", mqtt.Subscription{Filter: "sensors/#", QoS: mqtt.QoSExactlyOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe("client", mqtt.Subscription{Filter: "sensors/+/temp", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe("client", mqtt.Subscription{Filter: "sensors/#", QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}

	matches, err := table.Match("publisher", "sensors/room/temp", mqtt.QoSExactlyOnce, false)
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
	matches, err = table.Match("publisher", "sensors/room/temp", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	want = []Match{{SessionID: "client", QoS: mqtt.QoSAtMostOnce}}
	if !reflect.DeepEqual(matches, want) {
		t.Fatalf("remaining matches = %#v, want %#v", matches, want)
	}
}

// Traceability: PRD MQTT-03/TEST-03/04; stale subscriptions are removed when
// a session is replaced or expires.
func TestRemoveSessionIsAtomicAndIdempotent(t *testing.T) {
	table := NewTable()
	if _, err := table.Subscribe("client", mqtt.Subscription{Filter: "sensors/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe("client", mqtt.Subscription{Filter: "alerts/#", QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	if !table.RemoveSession("client") {
		t.Fatal("first RemoveSession() = false, want true")
	}
	if table.RemoveSession("client") {
		t.Fatal("second RemoveSession() = true, want false")
	}
	matches, err := table.Match("publisher", "sensors/room", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("matches after RemoveSession() = %#v, want empty", matches)
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
		{name: "empty-session-subscribe", call: func() error {
			_, err := table.Subscribe("", mqtt.Subscription{Filter: "a/#", QoS: mqtt.QoSAtMostOnce})
			return err
		}, want: ErrInvalidSessionID},
		{name: "invalid-qos", call: func() error {
			_, err := table.Subscribe("client", mqtt.Subscription{Filter: "a/#", QoS: mqtt.QoS(3)})
			return err
		}, want: ErrInvalidQoS},
		{name: "invalid-publish-qos", call: func() error { _, err := table.Match("publisher", "a", mqtt.QoS(3), false); return err }, want: ErrInvalidQoS},
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
		if _, err := table.Subscribe(sessionID, mqtt.Subscription{Filter: filters[index%len(filters)], QoS: mqtt.QoSAtLeastOnce}); err != nil {
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
					if _, err := table.Subscribe(sessionID, mqtt.Subscription{Filter: filters[iteration%len(filters)], QoS: mqtt.QoS(iteration % 3)}); err != nil {
						t.Errorf("Subscribe(%q) error = %v", sessionID, err)
					}
				case 1:
					if _, err := table.Unsubscribe(sessionID, filters[iteration%len(filters)]); err != nil {
						t.Errorf("Unsubscribe(%q) error = %v", sessionID, err)
					}
				default:
					if _, err := table.Match("publisher", "events/room", mqtt.QoSExactlyOnce, false); err != nil {
						t.Errorf("Match error = %v", err)
					}
				}
			}
		}(worker)
	}
	waitGroup.Wait()
}
func TestRoutingRejectsInvalidOperationsAtBoundaries(t *testing.T) {
	table := NewTable()
	for _, sessionID := range []SessionID{"", "bad\x00session"} {
		if _, err := table.Subscribe(sessionID, mqtt.Subscription{Filter: "topic", QoS: mqtt.QoSAtMostOnce}); !errors.Is(err, ErrInvalidSessionID) {
			t.Fatalf("Subscribe(%q) error = %v, want ErrInvalidSessionID", sessionID, err)
		}
		if _, err := table.Unsubscribe(sessionID, "topic"); !errors.Is(err, ErrInvalidSessionID) {
			t.Fatalf("Unsubscribe(%q) error = %v, want ErrInvalidSessionID", sessionID, err)
		}
	}
	if _, err := table.Subscribe("session", mqtt.Subscription{Filter: "$share/group/topic", QoS: mqtt.QoSAtMostOnce}); !errors.Is(err, ErrSharedSubscriptionUnsupported) {
		t.Fatalf("shared Subscribe() error = %v, want ErrSharedSubscriptionUnsupported", err)
	}
	if _, err := table.Subscribe("session", mqtt.Subscription{Filter: "topic/#/tail", QoS: mqtt.QoSAtMostOnce}); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("invalid wildcard Subscribe() error = %v, want ErrInvalidTopicFilter", err)
	}
	if _, err := table.Subscribe("session", mqtt.Subscription{Filter: "topic", QoS: mqtt.QoS(3)}); !errors.Is(err, ErrInvalidQoS) {
		t.Fatalf("invalid QoS Subscribe() error = %v, want ErrInvalidQoS", err)
	}
	if _, err := table.Match("publisher", "topic", mqtt.QoS(3), false); !errors.Is(err, ErrInvalidQoS) {
		t.Fatalf("invalid QoS Match() error = %v, want ErrInvalidQoS", err)
	}
	if _, err := table.Match("publisher", "", mqtt.QoSAtMostOnce, false); !errors.Is(err, ErrInvalidTopicName) {
		t.Fatalf("invalid topic Match() error = %v, want ErrInvalidTopicName", err)
	}
	if removed := table.RemoveSession("missing"); removed {
		t.Fatal("RemoveSession() reported removal for missing session")
	}
}

func TestRoutingSystemTopicWildcardBoundary(t *testing.T) {
	table := NewTable()
	if _, err := table.Subscribe("system", mqtt.Subscription{Filter: "#", QoS: mqtt.QoSExactlyOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe("exact", mqtt.Subscription{Filter: "$SYS/#", QoS: mqtt.QoSExactlyOnce}); err != nil {
		t.Fatal(err)
	}
	matches, err := table.Match("publisher", "$SYS/health", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].SessionID != "exact" {
		t.Fatalf("system topic matches = %#v, want exact subscription only", matches)
	}
}

func TestRoutingInitializesNilSubscriptionMapAndHandlesNilTable(t *testing.T) {
	var table Table
	if _, err := table.Subscribe(SessionID("session"), mqtt.Subscription{Filter: "topic", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatalf("Subscribe() on zero Table error = %v", err)
	}
	if removed, err := table.Unsubscribe(SessionID("session"), "topic"); err != nil || !removed {
		t.Fatalf("Unsubscribe() = (%v, %v), want removed", removed, err)
	}
	var nilTable *Table
	if nilTable.RemoveSession(SessionID("session")) {
		t.Fatal("nil Table RemoveSession() reported removal")
	}
}

func TestRoutingRejectsInvalidUTF8AndMatchesDollarTopics(t *testing.T) {
	if !errors.Is(ValidateTopicFilter(string([]byte{0xff})), ErrInvalidTopicFilter) {
		t.Fatal("invalid UTF-8 filter was accepted")
	}
	if !errors.Is(ValidateTopicName(string([]byte{0xff})), ErrInvalidTopicName) {
		t.Fatal("invalid UTF-8 topic was accepted")
	}
	table := NewTable()
	if _, err := table.Subscribe("plain", mqtt.Subscription{Filter: "+/status", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	if matches, err := table.Match("publisher", "$sys/status", mqtt.QoSAtMostOnce, false); err != nil || len(matches) != 0 {
		t.Fatalf("dollar topic wildcard match = %#v, error = %v", matches, err)
	}
	if _, err := table.Subscribe("literal", mqtt.Subscription{Filter: "$sys/status", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	if matches, err := table.Match("publisher", "$sys/status", mqtt.QoSAtMostOnce, false); err != nil || len(matches) != 1 {
		t.Fatalf("literal dollar topic match = %#v, error = %v", matches, err)
	}
}

// Traceability: PRD MQTT-08/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §4 항목 4 and §5.
func TestRoutingSubscriptionOptionsAndRetainFlags(t *testing.T) {
	table := NewTable()
	if replaced, err := table.Subscribe("self", mqtt.Subscription{
		Filter:            "events/#",
		QoS:               mqtt.QoSAtLeastOnce,
		NoLocal:           true,
		RetainAsPublished: true,
	}); err != nil || replaced {
		t.Fatalf("first self Subscribe() = (%v, %v), want (false, nil)", replaced, err)
	}
	if matches, err := table.Match("self", "events/created", mqtt.QoSExactlyOnce, true); err != nil || len(matches) != 0 {
		t.Fatalf("No Local self match = %#v, %v, want empty", matches, err)
	}
	if replaced, err := table.Subscribe("self", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil || !replaced {
		t.Fatalf("replacement Subscribe() = (%v, %v), want (true, nil)", replaced, err)
	}
	if subscription, found := table.Lookup("self", "events/#"); !found || subscription.QoS != mqtt.QoSAtMostOnce || subscription.NoLocal {
		t.Fatalf("Lookup() = (%#v, %v), want replaced options", subscription, found)
	}
	if _, found := table.Lookup("self", "missing/#"); found {
		t.Fatal("Lookup() found missing filter")
	}

	for _, subscription := range []struct {
		sessionID SessionID
		subscribe mqtt.Subscription
	}{
		{sessionID: "rap-false", subscribe: mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSExactlyOnce}},
		{sessionID: "rap-true", subscribe: mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSExactlyOnce, RetainAsPublished: true}},
		{sessionID: "overlap", subscribe: mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}},
		{sessionID: "overlap", subscribe: mqtt.Subscription{Filter: "events/+/created", QoS: mqtt.QoSAtLeastOnce, RetainAsPublished: true}},
	} {
		if _, err := table.Subscribe(subscription.sessionID, subscription.subscribe); err != nil {
			t.Fatal(err)
		}
	}
	matches, err := table.Match("publisher", "events/room/created", mqtt.QoSExactlyOnce, true)
	if err != nil {
		t.Fatal(err)
	}
	want := []Match{
		{SessionID: "overlap", QoS: mqtt.QoSAtLeastOnce, Retain: true},
		{SessionID: "rap-false", QoS: mqtt.QoSExactlyOnce},
		{SessionID: "rap-true", QoS: mqtt.QoSExactlyOnce, Retain: true},
		{SessionID: "self", QoS: mqtt.QoSAtMostOnce},
	}
	if !reflect.DeepEqual(matches, want) {
		t.Fatalf("option-aware matches = %#v, want %#v", matches, want)
	}
	if matches, err := table.Match("publisher", "$SYS/health", mqtt.QoSAtMostOnce, true); err != nil || len(matches) != 0 {
		t.Fatalf("system topic wildcard match = %#v, %v, want empty", matches, err)
	}
	if matched, err := TopicMatchesFilter("$SYS/health", "$SYS/#"); err != nil || !matched {
		t.Fatalf("TopicMatchesFilter() = (%v, %v), want true", matched, err)
	}
}

func TestRoutingCoversBoundaryValidationBranches(t *testing.T) {
	var table Table
	if _, found := table.Lookup("", "topic"); found {
		t.Fatal("Lookup() found invalid session")
	}
	if _, found := table.Lookup("session", "bad/#/filter"); found {
		t.Fatal("Lookup() found invalid filter")
	}
	if _, err := table.Unsubscribe("session", "topic"); err != nil {
		t.Fatalf("Unsubscribe() on nil map error = %v", err)
	}
	if _, err := table.Subscribe("session", mqtt.Subscription{Filter: "topic", RetainHandling: mqtt.RetainHandling(3)}); !errors.Is(err, ErrInvalidRetainHandling) {
		t.Fatalf("invalid Retain Handling error = %v, want %v", err, ErrInvalidRetainHandling)
	}
	if _, err := table.Match("", "topic", mqtt.QoSAtMostOnce, false); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("invalid publisher session error = %v, want %v", err, ErrInvalidSessionID)
	}
	if _, err := TopicMatchesFilter("bad+", "topic"); !errors.Is(err, ErrInvalidTopicName) {
		t.Fatalf("invalid topic matcher error = %v, want %v", err, ErrInvalidTopicName)
	}
	if _, err := TopicMatchesFilter("topic", "bad/#/filter"); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("invalid filter matcher error = %v, want %v", err, ErrInvalidTopicFilter)
	}
	for _, value := range []string{string(rune(0xFDD0)), string(rune(0xFFFE))} {
		if !errors.Is(ValidateTopicName(value), ErrInvalidTopicName) {
			t.Fatalf("noncharacter topic %U was accepted", []rune(value)[0])
		}
		if !errors.Is(ValidateTopicFilter(value), ErrInvalidTopicFilter) {
			t.Fatalf("noncharacter filter %U was accepted", []rune(value)[0])
		}
	}
}
