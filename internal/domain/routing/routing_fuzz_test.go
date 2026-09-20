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
			if IsSharedFilter(rawFilter) {
				wantErr = ErrSharedSubscriptionUnsupported
			}
			if !errors.Is(filterErr, wantErr) {
				t.Fatalf("ValidateTopicFilter(%q) error = %v, want %v", rawFilter, filterErr, wantErr)
			}

			table := NewTable()
			if err := table.Subscribe(SessionID("filter-session"), rawFilter, mqtt.QoSAtMostOnce); !errors.Is(err, wantErr) {
				t.Fatalf("Subscribe(%q) error = %v, want %v", rawFilter, err, wantErr)
			}
			matches, err := table.Match("state/probe", mqtt.QoSAtMostOnce)
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
			matches, err := table.Match(rawTopic, mqtt.QoSAtMostOnce)
			if !errors.Is(err, ErrInvalidTopicName) || matches != nil {
				t.Fatalf("Match(%q) = (%#v, %v), want invalid topic and no matches", rawTopic, matches, err)
			}
			matches, err = table.Match("state/probe", mqtt.QoSAtMostOnce)
			if err != nil || len(matches) != 0 {
				t.Fatalf("invalid topic changed table: matches = %#v, error = %v", matches, err)
			}
		}

		if qosRaw >= 3 {
			table := NewTable()
			invalidQoS := mqtt.QoS(qosRaw)
			if err := table.Subscribe(SessionID("qos-session"), "state/probe", invalidQoS); !errors.Is(err, ErrInvalidQoS) {
				t.Fatalf("Subscribe() invalid QoS error = %v, want ErrInvalidQoS", err)
			}
			matches, err := table.Match("state/probe", mqtt.QoSAtMostOnce)
			if err != nil || len(matches) != 0 {
				t.Fatalf("invalid QoS changed table: matches = %#v, error = %v", matches, err)
			}
			matches, err = table.Match("state/probe", invalidQoS)
			if !errors.Is(err, ErrInvalidQoS) || matches != nil {
				t.Fatalf("Match() invalid QoS = (%#v, %v), want invalid QoS and no matches", matches, err)
			}
		}

		if filterErr != nil || topicErr != nil {
			return
		}

		grantedQoS := mqtt.QoS(grantedRaw % 3)
		publishQoS := mqtt.QoS(publishRaw % 3)
		const sessionID = SessionID("fuzz-session")
		table := NewTable()
		if err := table.Subscribe(sessionID, rawFilter, grantedQoS); err != nil {
			t.Fatalf("Subscribe(%q) error = %v", rawFilter, err)
		}
		matches, err := table.Match(rawTopic, publishQoS)
		if err != nil {
			t.Fatalf("Match(%q) error = %v", rawTopic, err)
		}

		if referenceTopicMatchesFilter(rawTopic, rawFilter) {
			wantQoS := grantedQoS
			if publishQoS < wantQoS {
				wantQoS = publishQoS
			}
			if len(matches) != 1 || matches[0].SessionID != sessionID || matches[0].QoS != wantQoS {
				t.Fatalf("matches = %#v, want one session %q at QoS %d", matches, sessionID, wantQoS)
			}
			return
		}
		if len(matches) != 0 {
			t.Fatalf("matches = %#v, want no match", matches)
		}
	})
}

func referenceTopicMatchesFilter(topic, filter string) bool {
	topicLevels := strings.Split(topic, "/")
	filterLevels := strings.Split(filter, "/")
	if strings.HasPrefix(topic, "$") && len(filterLevels) > 0 {
		if firstLevel := filterLevels[0]; firstLevel == "+" || firstLevel == "#" {
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
