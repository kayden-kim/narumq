package routing

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/tenancy"
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
		{name: "shared-valid", filter: "$share/group/sensors/#", wantErr: nil},
		{name: "shared-invalid-empty-sharename", filter: "$share//sensors/#", wantErr: ErrInvalidTopicFilter},
		{name: "shared-invalid-sharename-plus", filter: "$share/gro+up/sensors/#", wantErr: ErrInvalidTopicFilter},
		{name: "shared-invalid-sharename-hash", filter: "$share/gro#up/sensors/#", wantErr: ErrInvalidTopicFilter},
		{name: "shared-invalid-nested", filter: "$share/group/$share/g2/sensors", wantErr: ErrInvalidTopicFilter},
		{name: "shared-invalid-empty-filter", filter: "$share/group/", wantErr: ErrInvalidTopicFilter},
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
				{Primary: Target{SessionID: "all", QoS: mqtt.QoSExactlyOnce}},
				{Primary: Target{SessionID: "exact", QoS: mqtt.QoSExactlyOnce}},
				{Primary: Target{SessionID: "multi", QoS: mqtt.QoSExactlyOnce}},
				{Primary: Target{SessionID: "single", QoS: mqtt.QoSExactlyOnce}},
			},
		},
		{
			name:  "hash-parent-match",
			topic: "sport",
			want:  []Match{{Primary: Target{SessionID: "all", QoS: mqtt.QoSExactlyOnce}}, {Primary: Target{SessionID: "multi", QoS: mqtt.QoSExactlyOnce}}},
		},
		{
			name:  "empty-level-single",
			topic: "sport//player1",
			want:  []Match{{Primary: Target{SessionID: "all", QoS: mqtt.QoSExactlyOnce}}, {Primary: Target{SessionID: "multi", QoS: mqtt.QoSExactlyOnce}}, {Primary: Target{SessionID: "single", QoS: mqtt.QoSExactlyOnce}}},
		},
		{
			name:  "system-topic-explicit-only",
			topic: "$SYS/broker/uptime",
			want:  []Match{{Primary: Target{SessionID: "system", QoS: mqtt.QoSExactlyOnce}}},
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
	want := []Match{{Primary: Target{SessionID: "client", QoS: mqtt.QoSAtLeastOnce}}}
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
	want = []Match{{Primary: Target{SessionID: "client", QoS: mqtt.QoSAtMostOnce}}}
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
		{name: "invalid-shared-unsubscribe", call: func() error { _, err := table.Unsubscribe("client", "$share//a"); return err }, want: ErrInvalidTopicFilter},
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
	if _, err := table.Subscribe("session", mqtt.Subscription{Filter: "$share//topic", QoS: mqtt.QoSAtMostOnce}); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("shared Subscribe() error = %v, want ErrInvalidTopicFilter", err)
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
	if len(matches) != 1 || matches[0].Primary.SessionID != "exact" {
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
		{Primary: Target{SessionID: "overlap", QoS: mqtt.QoSAtLeastOnce, Retain: true}},
		{Primary: Target{SessionID: "rap-false", QoS: mqtt.QoSExactlyOnce}},
		{Primary: Target{SessionID: "rap-true", QoS: mqtt.QoSExactlyOnce, Retain: true}},
		{Primary: Target{SessionID: "self", QoS: mqtt.QoSAtMostOnce}},
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
	if _, found := table.Lookup("session", "$share/g/topic"); found {
		t.Fatal("Lookup() found on nil shared map")
	}
	if _, found := table.Lookup("session", "topic"); found {
		t.Fatal("Lookup() found on nil subscriptions map")
	}
	if removed, err := table.Unsubscribe("session", "$share/g/topic"); err != nil || removed {
		t.Fatalf("Unsubscribe() on nil shared map = (%t, %v)", removed, err)
	}
	if _, err := table.Unsubscribe("session", "topic"); err != nil {
		t.Fatalf("Unsubscribe() on nil map error = %v", err)
	}
	matched, err := TopicMatchesFilter("sensors/temp", "$share/group/sensors/temp")
	if err != nil || !matched {
		t.Fatalf("TopicMatchesFilter on shared filter = (%t, %v), want (true, nil)", matched, err)
	}
	tableWithNilGroup := &Table{
		shared: map[string]map[SessionID]mqtt.Subscription{
			"$share/g/empty": nil,
		},
	}
	matches, err := tableWithNilGroup.Match("pub", "empty", mqtt.QoSAtMostOnce, false)
	if err != nil || len(matches) != 0 {
		t.Fatalf("Match() on nil shared members = (%#v, %v), want empty", matches, err)
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

// Traceability: PRD MQTT-07/TEST-03; MQTT-4.8.2-1 and MQTT-4.8.2-2.
func TestParseSharedFilter(t *testing.T) {
	cases := []struct {
		name       string
		filter     string
		wantShare  string
		wantTopic  string
		wantShared bool
		wantErr    error
	}{
		{name: "non-shared-simple", filter: "sensors/temperature", wantShare: "", wantTopic: "sensors/temperature", wantShared: false, wantErr: nil},
		{name: "non-shared-wildcard", filter: "sensors/+/room/#", wantShare: "", wantTopic: "sensors/+/room/#", wantShared: false, wantErr: nil},
		{name: "shared-simple", filter: "$share/group1/sensors/temperature", wantShare: "group1", wantTopic: "sensors/temperature", wantShared: true, wantErr: nil},
		{name: "shared-wildcard", filter: "$share/g2/sensors/+/temp/#", wantShare: "g2", wantTopic: "sensors/+/temp/#", wantShared: true, wantErr: nil},
		{name: "no-slash", filter: "$share/group", wantShare: "", wantTopic: "", wantShared: true, wantErr: ErrInvalidTopicFilter},
		{name: "empty-share-name", filter: "$share//topic", wantShare: "", wantTopic: "", wantShared: true, wantErr: ErrInvalidTopicFilter},
		{name: "share-name-plus", filter: "$share/g+/topic", wantShare: "", wantTopic: "", wantShared: true, wantErr: ErrInvalidTopicFilter},
		{name: "share-name-hash", filter: "$share/g#/topic", wantShare: "", wantTopic: "", wantShared: true, wantErr: ErrInvalidTopicFilter},
		{name: "empty-topic-filter", filter: "$share/group/", wantShare: "", wantTopic: "", wantShared: true, wantErr: ErrInvalidTopicFilter},
		{name: "nested-shared", filter: "$share/g1/$share/g2/topic", wantShare: "", wantTopic: "", wantShared: true, wantErr: ErrInvalidTopicFilter},
		{name: "topic-filter-bad-wildcard", filter: "$share/g1/sensors/#/room", wantShare: "", wantTopic: "", wantShared: true, wantErr: ErrInvalidTopicFilter},
		{name: "topic-filter-embedded-plus", filter: "$share/g1/sensors/te+mp", wantShare: "", wantTopic: "", wantShared: true, wantErr: ErrInvalidTopicFilter},
		{name: "invalid-utf8", filter: "$share/g1/sensors/\x00", wantShare: "", wantTopic: "", wantShared: true, wantErr: ErrInvalidTopicFilter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			share, topic, isShared, err := ParseSharedFilter(tc.filter)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ParseSharedFilter(%q) error = %v, want %v", tc.filter, err, tc.wantErr)
			}
			if share != tc.wantShare || topic != tc.wantTopic || isShared != tc.wantShared {
				t.Fatalf("ParseSharedFilter(%q) = (%q, %q, %t), want (%q, %q, %t)", tc.filter, share, topic, isShared, tc.wantShare, tc.wantTopic, tc.wantShared)
			}
		})
	}
}

// Traceability: PRD MQTT-07/TEST-03; MQTT-4.8.2-3.
func TestSharedSubscriptionRoundRobin(t *testing.T) {
	table := NewTable()
	filter := "$share/workers/tasks/+"
	subscriptions := []struct {
		sessionID SessionID
		qos       mqtt.QoS
		rap       bool
	}{
		{sessionID: "worker-1", qos: mqtt.QoSAtMostOnce},
		{sessionID: "worker-2", qos: mqtt.QoSAtLeastOnce, rap: true},
		{sessionID: "worker-3", qos: mqtt.QoSExactlyOnce},
	}
	for _, subscription := range subscriptions {
		if _, err := table.Subscribe(subscription.sessionID, mqtt.Subscription{
			Filter:            filter,
			QoS:               subscription.qos,
			RetainAsPublished: subscription.rap,
		}); err != nil {
			t.Fatalf("Subscribe(%q) error = %v", subscription.sessionID, err)
		}
	}

	want := []Match{
		{
			Primary: Target{SessionID: "worker-1", QoS: mqtt.QoSAtMostOnce},
			Fallbacks: []Target{
				{SessionID: "worker-2", QoS: mqtt.QoSAtLeastOnce, Retain: true},
				{SessionID: "worker-3", QoS: mqtt.QoSExactlyOnce},
			},
		},
		{
			Primary: Target{SessionID: "worker-2", QoS: mqtt.QoSAtLeastOnce, Retain: true},
			Fallbacks: []Target{
				{SessionID: "worker-3", QoS: mqtt.QoSExactlyOnce},
				{SessionID: "worker-1", QoS: mqtt.QoSAtMostOnce},
			},
		},
		{
			Primary: Target{SessionID: "worker-3", QoS: mqtt.QoSExactlyOnce},
			Fallbacks: []Target{
				{SessionID: "worker-1", QoS: mqtt.QoSAtMostOnce},
				{SessionID: "worker-2", QoS: mqtt.QoSAtLeastOnce, Retain: true},
			},
		},
		{
			Primary: Target{SessionID: "worker-1", QoS: mqtt.QoSAtMostOnce},
			Fallbacks: []Target{
				{SessionID: "worker-2", QoS: mqtt.QoSAtLeastOnce, Retain: true},
				{SessionID: "worker-3", QoS: mqtt.QoSExactlyOnce},
			},
		},
	}
	for i, wantMatch := range want {
		matches, err := table.Match("publisher", "tasks/item", mqtt.QoSExactlyOnce, true)
		if err != nil {
			t.Fatalf("Match() iter %d error = %v", i, err)
		}
		if !reflect.DeepEqual(matches, []Match{wantMatch}) {
			t.Fatalf("Match() iter %d = %#v, want %#v", i, matches, []Match{wantMatch})
		}
	}
}

// Traceability: PRD MQTT-07/TEST-03; MQTT-4.8.2-3.
func TestSharedSubscriptionMultipleGroups(t *testing.T) {
	table := NewTable()
	if _, err := table.Subscribe("group1-worker", mqtt.Subscription{Filter: "$share/group1/events/#", QoS: mqtt.QoSExactlyOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe("group2-worker", mqtt.Subscription{Filter: "$share/group2/events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}

	matches, err := table.Match("publisher", "events/user/login", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("len(matches) = %d, want 2 (one per group)", len(matches))
	}
	if matches[0].Primary.SessionID != "group1-worker" || matches[0].Primary.QoS != mqtt.QoSExactlyOnce {
		t.Fatalf("group1 match = %#v", matches[0])
	}
	if matches[1].Primary.SessionID != "group2-worker" || matches[1].Primary.QoS != mqtt.QoSAtMostOnce {
		t.Fatalf("group2 match = %#v", matches[1])
	}
}

// Traceability: PRD MQTT-07/TEST-03; MQTT-4.8.2-3.
func TestSharedSubscriptionWithNonShared(t *testing.T) {
	table := NewTable()
	const sessionID = SessionID("multi-client")
	if _, err := table.Subscribe(sessionID, mqtt.Subscription{Filter: "sensors/+", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe(sessionID, mqtt.Subscription{Filter: "$share/group/sensors/+", QoS: mqtt.QoSExactlyOnce}); err != nil {
		t.Fatal(err)
	}

	matches, err := table.Match("publisher", "sensors/temperature", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("len(matches) = %d, want 2 (one non-shared, one shared)", len(matches))
	}
	if matches[0].Primary.SessionID != sessionID || matches[1].Primary.SessionID != sessionID {
		t.Fatalf("matches = %#v, want both for %q", matches, sessionID)
	}
	q0Found := matches[0].Primary.QoS == mqtt.QoSAtMostOnce || matches[1].Primary.QoS == mqtt.QoSAtMostOnce
	q2Found := matches[0].Primary.QoS == mqtt.QoSExactlyOnce || matches[1].Primary.QoS == mqtt.QoSExactlyOnce
	if !q0Found || !q2Found {
		t.Fatalf("matches QoS = [%d, %d], want 0 and 2", matches[0].Primary.QoS, matches[1].Primary.QoS)
	}
}

// Traceability: PRD MQTT-07/TEST-03; MQTT-4.8.2-1 and MQTT-4.8.2-3.
func TestSharedSubscriptionUnsubscribeAndSessionRemoval(t *testing.T) {
	table := NewTable()
	filter := "$share/group/jobs"
	if _, err := table.Subscribe("worker-a", mqtt.Subscription{Filter: filter, QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe("worker-b", mqtt.Subscription{Filter: filter, QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	// Unsubscribe an absent session from existing shared filter
	removedAbsent, err := table.Unsubscribe("absent-worker", filter)
	if err != nil || removedAbsent {
		t.Fatalf("Unsubscribe(absent) = (%t, %v), want (false, nil)", removedAbsent, err)
	}

	removed, err := table.Unsubscribe("worker-a", filter)
	if err != nil || !removed {
		t.Fatalf("Unsubscribe(worker-a) = (%t, %v), want (true, nil)", removed, err)
	}

	// Unsubscribe the last member (worker-b) so group is deleted
	removedLast, err := table.Unsubscribe("worker-b", filter)
	if err != nil || !removedLast {
		t.Fatalf("Unsubscribe(worker-b) = (%t, %v), want (true, nil)", removedLast, err)
	}

	// Subscribe worker-c and test RemoveSession cleanup
	if _, err := table.Subscribe("worker-c", mqtt.Subscription{Filter: filter, QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	if !table.RemoveSession("worker-c") {
		t.Fatal("RemoveSession(worker-c) = false, want true")
	}

	matches, err := table.Match("pub", "jobs", mqtt.QoSAtLeastOnce, false)
	if err != nil || len(matches) != 0 {
		t.Fatalf("matches after all removed = %#v, want none", matches)
	}

	removed, err = table.Unsubscribe("worker-b", filter)
	if err != nil || removed {
		t.Fatalf("Unsubscribe after removal = (%t, %v), want (false, nil)", removed, err)
	}
}

// Requirements: TEN-03, TEN-05
func TestRoutingTable_TenantIsolation(t *testing.T) {
	table := NewTable()
	tenantA, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}

	// Bind sessions
	if err := table.BindSessionTenant("sub-a", tenantA); err != nil {
		t.Fatal(err)
	}
	if err := table.BindSessionTenant("pub-a", tenantA); err != nil {
		t.Fatal(err)
	}
	if err := table.BindSessionTenant("sub-b", tenantB); err != nil {
		t.Fatal(err)
	}
	if err := table.BindSessionTenant("pub-b", tenantB); err != nil {
		t.Fatal(err)
	}
	if got := table.SessionTenant("sub-a"); got != tenantA {
		t.Fatalf("SessionTenant(sub-a) = %v, want %v", got, tenantA)
	}
	var nilTable *Table
	if got := nilTable.SessionTenant("sub-a"); got != (tenancy.TenantID{}) {
		t.Fatalf("nilTable.SessionTenant() = %v, want zero", got)
	}
	if err := table.BindSessionTenant("", tenantA); err == nil {
		t.Fatal("BindSessionTenant with empty sessionID should fail")
	}
	if got := table.SessionTenant("unknown"); got != (tenancy.TenantID{}) {
		t.Fatalf("SessionTenant(unknown) = %v, want zero", got)
	}

	// Both subscribe to exact same filter
	filter := "sensors/#"
	if _, err := table.Subscribe("sub-a", mqtt.Subscription{Filter: filter, QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe("sub-b", mqtt.Subscription{Filter: filter, QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}

	// Publication from pub-a must ONLY match sub-a, never sub-b
	matchesA, err := table.Match("pub-a", "sensors/temp", mqtt.QoSAtLeastOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matchesA) != 1 || matchesA[0].Primary.SessionID != "sub-a" {
		t.Fatalf("pub-a matches = %#v, want only sub-a", matchesA)
	}

	// Publication from pub-b must ONLY match sub-b, never sub-a
	matchesB, err := table.Match("pub-b", "sensors/temp", mqtt.QoSAtLeastOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matchesB) != 1 || matchesB[0].Primary.SessionID != "sub-b" {
		t.Fatalf("pub-b matches = %#v, want only sub-b", matchesB)
	}

	// Shared subscription isolation
	sharedFilter := "$share/workers/jobs"
	if _, err := table.Subscribe("sub-a", mqtt.Subscription{Filter: sharedFilter, QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe("sub-b", mqtt.Subscription{Filter: sharedFilter, QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}

	sharedMatchesA, err := table.Match("pub-a", "jobs", mqtt.QoSAtLeastOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(sharedMatchesA) != 1 || sharedMatchesA[0].Primary.SessionID != "sub-a" {
		t.Fatalf("shared pub-a matches = %#v, want only sub-a", sharedMatchesA)
	}

	sharedMatchesB, err := table.Match("pub-b", "jobs", mqtt.QoSAtLeastOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(sharedMatchesB) != 1 || sharedMatchesB[0].Primary.SessionID != "sub-b" {
		t.Fatalf("shared pub-b matches = %#v, want only sub-b", sharedMatchesB)
	}
}

func TestMatchForTenantAfterPublisherSessionRemovalKeepsTenantIsolation(t *testing.T) {
	table := NewTable()
	tenantA, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	publisherKey, err := tenancy.NewSessionKey(tenantA, "publisher")
	if err != nil {
		t.Fatal(err)
	}
	publisher := SessionID(publisherKey.RoutingID())
	subscriberAKey, err := tenancy.NewSessionKey(tenantA, "subscriber-a")
	if err != nil {
		t.Fatal(err)
	}
	subscriberA := SessionID(subscriberAKey.RoutingID())
	subscriberBKey, err := tenancy.NewSessionKey(tenantB, "subscriber-b")
	if err != nil {
		t.Fatal(err)
	}
	subscriberB := SessionID(subscriberBKey.RoutingID())
	for _, binding := range []struct {
		sessionID SessionID
		tenantID  tenancy.TenantID
	}{
		{sessionID: publisher, tenantID: tenantA},
		{sessionID: subscriberA, tenantID: tenantA},
		{sessionID: subscriberB, tenantID: tenantB},
	} {
		if err := table.BindSessionTenant(binding.sessionID, binding.tenantID); err != nil {
			t.Fatal(err)
		}
	}
	for _, subscriber := range []SessionID{subscriberA, subscriberB} {
		if _, err := table.Subscribe(subscriber, mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSExactlyOnce}); err != nil {
			t.Fatal(err)
		}
	}
	table.RemoveSession(publisher)

	matches, err := table.MatchForTenant(publisher, tenantA, "events/will", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].Primary.SessionID != subscriberA {
		t.Fatalf("tenant-scoped matches = %+v, want only %q", matches, subscriberA)
	}
	unscopedMatches, err := table.Match(publisher, "events/will", mqtt.QoSExactlyOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(unscopedMatches) != 0 {
		t.Fatalf("unscoped matches = %+v, want no tenant subscriptions", unscopedMatches)
	}
}

func TestReplaceProjectionWithSessionsAndTenantBinding(t *testing.T) {
	tenantA, _ := tenancy.NewTenantID("tenant-a")
	tenantB, _ := tenancy.NewTenantID("tenant-b")
	table := NewTable()

	// SessionTenant on nil table
	var nilTable *Table
	if got := nilTable.SessionTenant("session-1"); got.String() != "" {
		t.Fatalf("expected empty tenant for nil table, got: %v", got)
	}

	// SessionTenant on uninitialized map
	emptyTable := &Table{}
	if got := emptyTable.SessionTenant("session-1"); got.String() != "" {
		t.Fatalf("expected empty tenant for uninitialized table, got: %v", got)
	}

	// BindSessionTenant with nil sessionTenants map
	if err := emptyTable.BindSessionTenant("session-1", tenantA); err != nil {
		t.Fatalf("BindSessionTenant on emptyTable: %v", err)
	}
	if got := emptyTable.SessionTenant("session-1"); got != tenantA {
		t.Fatalf("expected tenantA, got %v", got)
	}
	if err := emptyTable.BindSessionTenant("", tenantA); !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("expected ErrInvalidSessionID, got %v", err)
	}

	// Shared cursor preservation
	sharedFilter := "$share/group/sensors/#"
	if err := table.BindSessionTenant("sub-1", tenantA); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Subscribe("sub-1", mqtt.Subscription{Filter: sharedFilter, QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	table.sharedCursors[sharedCursorKey{tenantID: tenantA, filter: sharedFilter}] = 42

	// ReplaceProjectionWithSessions with empty tenant error
	err := table.ReplaceProjectionWithSessions([]ProjectionSession{
		{TenantID: tenancy.TenantID{}, SessionID: "session-1"},
	}, nil)
	if err == nil || err.Error() != "route projection entry has no tenant" {
		t.Fatalf("expected empty tenant error, got: %v", err)
	}

	// ReplaceProjectionWithSessions with multiple tenants for same session
	err = table.ReplaceProjectionWithSessions([]ProjectionSession{
		{TenantID: tenantA, SessionID: "session-1"},
		{TenantID: tenantB, SessionID: "session-1"},
	}, nil)
	if err == nil || err.Error() != "route projection session belongs to multiple tenants" {
		t.Fatalf("expected multiple tenants error, got: %v", err)
	}

	// Duplicate subscription in projection
	err = table.ReplaceProjectionWithSessions(nil, []ProjectionSubscription{
		{TenantID: tenantA, SessionID: "session-1", Subscription: mqtt.Subscription{Filter: "topic/#", QoS: 0}},
		{TenantID: tenantA, SessionID: "session-1", Subscription: mqtt.Subscription{Filter: "topic/#", QoS: 1}},
	})
	if err == nil || err.Error() != "duplicate subscription in route projection" {
		t.Fatalf("expected duplicate subscription error, got: %v", err)
	}

	// Invalid subscription filter in projection
	err = table.ReplaceProjectionWithSessions(nil, []ProjectionSubscription{
		{TenantID: tenantA, SessionID: "session-1", Subscription: mqtt.Subscription{Filter: "topic/+/invalid#", QoS: 0}},
	})
	if err == nil {
		t.Fatal("expected invalid subscription error")
	}

	// Valid replacement preserving shared cursor
	err = table.ReplaceProjectionWithSessions(
		[]ProjectionSession{{TenantID: tenantA, SessionID: "sub-1"}},
		[]ProjectionSubscription{
			{TenantID: tenantA, SessionID: "sub-1", Subscription: mqtt.Subscription{Filter: sharedFilter, QoS: mqtt.QoSAtLeastOnce}},
		},
	)
	if err != nil {
		t.Fatalf("valid replacement failed: %v", err)
	}
	cursorKey := sharedCursorKey{tenantID: tenantA, filter: sharedFilter}
	if table.sharedCursors[cursorKey] != 42 {
		t.Fatalf("shared cursor not preserved: %d", table.sharedCursors[cursorKey])
	}
}

func TestReplaceProjectionPreservesTenantSharedRoundRobinCursor(t *testing.T) {
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	filter := "$share/group/sensors/#"
	sessions := []ProjectionSession{
		{TenantID: tenantID, SessionID: "subscriber-a"},
		{TenantID: tenantID, SessionID: "subscriber-b"},
	}
	projection := []ProjectionSubscription{
		{TenantID: tenantID, SessionID: "subscriber-a", Subscription: mqtt.Subscription{Filter: filter, QoS: mqtt.QoSAtLeastOnce}},
		{TenantID: tenantID, SessionID: "subscriber-b", Subscription: mqtt.Subscription{Filter: filter, QoS: mqtt.QoSAtLeastOnce}},
	}
	table := NewTable()
	if err := table.ReplaceProjectionWithSessions(sessions, projection); err != nil {
		t.Fatal(err)
	}

	first, err := table.MatchForTenant("publisher", tenantID, "sensors/one", mqtt.QoSAtLeastOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Primary.SessionID != "subscriber-a" {
		t.Fatalf("first shared target = %+v, want subscriber-a", first)
	}
	if err := table.ReplaceProjectionWithSessions(sessions, projection); err != nil {
		t.Fatal(err)
	}
	second, err := table.MatchForTenant("publisher", tenantID, "sensors/one", mqtt.QoSAtLeastOnce, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].Primary.SessionID != "subscriber-b" {
		t.Fatalf("shared target after projection replacement = %+v, want subscriber-b", second)
	}
}
