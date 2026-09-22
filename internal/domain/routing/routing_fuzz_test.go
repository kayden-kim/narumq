package routing

import (
	"errors"
	"strings"
	"testing"

	"narumq/internal/domain/mqtt"
)

// Traceability: PRD TEST-13.
func FuzzTopicMatching(f *testing.F) {
	f.Add("sport/tennis/player1", "sport/tennis/player1", byte(2), byte(2), byte(0))
	f.Add("sport/+/player1", "sport/tennis/player1", byte(1), byte(2), byte(1))
	f.Add("sport/#", "sport/tennis/player1", byte(2), byte(1), byte(2))
	f.Add("sport//player1", "sport//player1", byte(0), byte(2), byte(3))
	f.Add("$SYS/#", "$SYS/broker/uptime", byte(2), byte(2), byte(4))
	f.Add("#", "$SYS/broker/uptime", byte(2), byte(2), byte(5))
	f.Add("sport/#/player1", "sport/tennis/player1", byte(2), byte(2), byte(6))
	f.Add(string([]byte{'s', 'p', 'o', 'r', 't', '/', 0}), "sport/tennis", byte(1), byte(1), byte(7))
	f.Add(string([]byte{0xff}), string([]byte{0xfe}), byte(0), byte(0), byte(8))
	f.Add("sport/tennis", "sport/tennis", byte(2), byte(2), byte(9))

	f.Fuzz(func(t *testing.T, rawFilter, rawTopic string, grantedRaw, publishRaw, qosRaw byte) {
		filterErr := ValidateTopicFilter(rawFilter)
		if filterErr != nil {
			wantErr := ErrInvalidTopicFilter
			if !errors.Is(filterErr, wantErr) {
				t.Fatalf("ValidateTopicFilter(%q) error = %v, want %v", rawFilter, filterErr, wantErr)
			}

			table := NewTable()
			if _, err := table.Subscribe(SessionID("filter-session"), mqtt.Subscription{Filter: rawFilter, QoS: mqtt.QoSAtMostOnce}); !errors.Is(err, wantErr) {
				t.Fatalf("Subscribe(%q) error = %v, want %v", rawFilter, err, wantErr)
			}
			matches, err := table.Match("probe-publisher", "state/probe", mqtt.QoSAtMostOnce, false)
			if err != nil || len(matches) != 0 {
				t.Fatalf("invalid filter changed table: matches = %#v, error = %v", matches, err)
			}
		}

		topicErr := ValidateTopicName(rawTopic)
		if topicErr != nil {
			if !errors.Is(topicErr, ErrInvalidTopicName) {
				t.Fatalf("ValidateTopicName(%q) error = %v, want ErrInvalidTopicName", rawTopic, topicErr)
			}

			table := NewTable()
			matches, err := table.Match("probe-publisher", rawTopic, mqtt.QoSAtMostOnce, false)
			if !errors.Is(err, ErrInvalidTopicName) || matches != nil {
				t.Fatalf("Match(%q) = (%#v, %v), want invalid topic and no matches", rawTopic, matches, err)
			}
			matches, err = table.Match("probe-publisher", "state/probe", mqtt.QoSAtMostOnce, false)
			if err != nil || len(matches) != 0 {
				t.Fatalf("invalid topic changed table: matches = %#v, error = %v", matches, err)
			}
		}

		if qosRaw >= 3 {
			table := NewTable()
			invalidQoS := mqtt.QoS(qosRaw)
			if _, err := table.Subscribe(SessionID("qos-session"), mqtt.Subscription{Filter: "state/probe", QoS: invalidQoS}); !errors.Is(err, ErrInvalidQoS) {
				t.Fatalf("Subscribe() invalid QoS error = %v, want ErrInvalidQoS", err)
			}
			matches, err := table.Match("probe-publisher", "state/probe", mqtt.QoSAtMostOnce, false)
			if err != nil || len(matches) != 0 {
				t.Fatalf("invalid QoS changed table: matches = %#v, error = %v", matches, err)
			}
			matches, err = table.Match("probe-publisher", "state/probe", invalidQoS, false)
			if !errors.Is(err, ErrInvalidQoS) || matches != nil {
				t.Fatalf("Match() invalid QoS = (%#v, %v), want invalid QoS and no matches", matches, err)
			}
		}

		if filterErr != nil || topicErr != nil {
			return
		}

		grantedQoS := mqtt.QoS(grantedRaw % 3)
		publishQoS := mqtt.QoS(publishRaw % 3)
		table := NewTable()
		const sessionID = SessionID("fuzz-session")
		grantedSubscription := mqtt.Subscription{
			Filter:            rawFilter,
			QoS:               grantedQoS,
			NoLocal:           qosRaw&0x01 != 0,
			RetainAsPublished: qosRaw&0x02 != 0,
		}
		if _, err := table.Subscribe(sessionID, grantedSubscription); err != nil {
			t.Fatalf("Subscribe(%q) error = %v", rawFilter, err)
		}
		publisherSessionID := SessionID("fuzz-publisher")
		if grantedSubscription.NoLocal {
			publisherSessionID = sessionID
		}
		publishedRetain := qosRaw&0x04 != 0
		matches, err := table.Match(publisherSessionID, rawTopic, publishQoS, publishedRetain)
		if err != nil {
			t.Fatalf("Match(%q) error = %v", rawTopic, err)
		}

		oracleMatches := referenceTopicMatchesFilter(rawTopic, rawFilter)
		if !IsSharedFilter(rawFilter) && grantedSubscription.NoLocal && publisherSessionID == sessionID {
			oracleMatches = false
		}
		if oracleMatches {
			wantQoS := grantedQoS
			if publishQoS < wantQoS {
				wantQoS = publishQoS
			}
			wantRetain := publishedRetain && grantedSubscription.RetainAsPublished
			if len(matches) != 1 || matches[0].Primary.SessionID != sessionID || matches[0].Primary.QoS != wantQoS || matches[0].Primary.Retain != wantRetain {
				t.Fatalf("matches = %#v, want one session %q at QoS %d retain %t", matches, sessionID, wantQoS, wantRetain)
			}
			return
		}
		if len(matches) != 0 {
			t.Fatalf("matches = %#v, want no match", matches)
		}
	})
}

func referenceTopicMatchesFilter(topic, filter string) bool {
	if IsSharedFilter(filter) {
		_, subFilter, _, err := ParseSharedFilter(filter)
		if err == nil {
			filter = subFilter
		}
	}
	topicLevels := strings.Split(topic, "/")
	filterLevels := strings.Split(filter, "/")
	if strings.HasPrefix(topic, "$") && len(filterLevels) > 0 {
		if filterLevels[0] == "+" || filterLevels[0] == "#" {
			return false
		}
	}
	return referenceMatchLevels(topicLevels, filterLevels)
}

func referenceMatchLevels(topicLevels, filterLevels []string) bool {
	if len(filterLevels) == 0 {
		return len(topicLevels) == 0
	}
	if filterLevels[0] == "#" {
		return true
	}
	if len(topicLevels) == 0 {
		return false
	}
	if filterLevels[0] != "+" && filterLevels[0] != topicLevels[0] {
		return false
	}
	return referenceMatchLevels(topicLevels[1:], filterLevels[1:])
}
