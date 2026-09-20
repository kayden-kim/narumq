// Package routing owns canonical MQTT topic/filter validation and non-shared
// subscription matching. It does not know about sockets, queues, tenants, or
// retained-message storage.
package routing

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"narumq/internal/domain/mqtt"
)

var (
	// ErrInvalidSessionID means that a route operation has no valid session key.
	ErrInvalidSessionID = errors.New("invalid routing session id")
	// ErrInvalidTopicFilter means that a subscription filter is not matchable.
	ErrInvalidTopicFilter = errors.New("invalid MQTT topic filter")
	// ErrInvalidTopicName means that a publication topic is not valid.
	ErrInvalidTopicName = errors.New("invalid MQTT topic name")
	// ErrInvalidQoS means that a route QoS is outside MQTT QoS 0..2.
	ErrInvalidQoS = errors.New("invalid MQTT QoS")
	// ErrSharedSubscriptionUnsupported means that this table only supports
	// non-shared subscriptions.
	ErrSharedSubscriptionUnsupported = errors.New("shared subscription is unsupported")
	// ErrInvalidRetainHandling means that a subscription option is outside the
	// MQTT retain handling range.
	ErrInvalidRetainHandling = errors.New("invalid MQTT retain handling")
)

// SessionID identifies one active subscriber session within a routing table.
type SessionID string

// Match is one session-level delivery target and its effective delivery QoS.
// Retain is the RETAIN flag for established-subscription delivery.
type Match struct {
	SessionID SessionID
	QoS       mqtt.QoS
	Retain    bool
}

// Table stores subscriptions and matches publications against a consistent
// snapshot. The initial Phase 0 implementation deliberately scans the small
// table instead of introducing an index whose invalidation rules are not yet
// needed.
type Table struct {
	mu            sync.RWMutex
	subscriptions map[SessionID]map[string]mqtt.Subscription
}

// NewTable creates an empty synchronized routing table.
func NewTable() *Table {
	return &Table{subscriptions: make(map[SessionID]map[string]mqtt.Subscription)}
}

// ValidateTopicFilter validates MQTT wildcard placement and UTF-8 rules.
func ValidateTopicFilter(filter string) error {
	if filter == "" || !validMQTTString(filter) {
		return ErrInvalidTopicFilter
	}
	if IsSharedFilter(filter) {
		return ErrSharedSubscriptionUnsupported
	}
	levels := strings.Split(filter, "/")
	for index, level := range levels {
		if level == "+" {
			continue
		}
		if level == "#" {
			if index != len(levels)-1 {
				return ErrInvalidTopicFilter
			}
			continue
		}
		if strings.ContainsAny(level, "+#") {
			return ErrInvalidTopicFilter
		}
	}
	return nil
}

// ValidateTopicName validates a concrete publication topic without wildcards.
func ValidateTopicName(topic string) error {
	if topic == "" || !validMQTTString(topic) || strings.ContainsAny(topic, "+#") {
		return ErrInvalidTopicName
	}
	return nil
}

// IsSharedFilter reports whether filter uses MQTT 5 shared-subscription syntax.
func IsSharedFilter(filter string) bool {
	return strings.HasPrefix(filter, "$share/")
}

// Subscribe adds or replaces one non-shared subscription for sessionID.
func (table *Table) Subscribe(sessionID SessionID, subscription mqtt.Subscription) (replaced bool, err error) {
	if err := ValidateSessionID(sessionID); err != nil {
		return false, err
	}
	if err := ValidateTopicFilter(subscription.Filter); err != nil {
		return false, err
	}
	if subscription.QoS > mqtt.QoSExactlyOnce {
		return false, ErrInvalidQoS
	}
	if subscription.RetainHandling > mqtt.DoNotSendRetained {
		return false, ErrInvalidRetainHandling
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	if table.subscriptions == nil {
		table.subscriptions = make(map[SessionID]map[string]mqtt.Subscription)
	}
	byFilter := table.subscriptions[sessionID]
	if byFilter == nil {
		byFilter = make(map[string]mqtt.Subscription)
		table.subscriptions[sessionID] = byFilter
	}
	_, replaced = byFilter[subscription.Filter]
	byFilter[subscription.Filter] = subscription
	return replaced, nil
}

// Lookup returns the exact subscription for sessionID and filter without
// changing the route table.
func (table *Table) Lookup(sessionID SessionID, filter string) (mqtt.Subscription, bool) {
	if table == nil || ValidateSessionID(sessionID) != nil || ValidateTopicFilter(filter) != nil {
		return mqtt.Subscription{}, false
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	byFilter := table.subscriptions[sessionID]
	if byFilter == nil {
		return mqtt.Subscription{}, false
	}
	subscription, found := byFilter[filter]
	return subscription, found
}

// Unsubscribe removes one filter. Removing an absent filter is an idempotent
// success and reports removed=false.
func (table *Table) Unsubscribe(sessionID SessionID, filter string) (removed bool, err error) {
	if err := ValidateSessionID(sessionID); err != nil {
		return false, err
	}
	if err := ValidateTopicFilter(filter); err != nil {
		return false, err
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	byFilter := table.subscriptions[sessionID]
	if byFilter == nil {
		return false, nil
	}
	if _, ok := byFilter[filter]; !ok {
		return false, nil
	}
	delete(byFilter, filter)
	if len(byFilter) == 0 {
		delete(table.subscriptions, sessionID)
	}
	return true, nil
}

// RemoveSession removes every subscription owned by sessionID in one
// synchronized operation. It is idempotent and reports whether anything was
// removed.
func (table *Table) RemoveSession(sessionID SessionID) bool {
	if table == nil {
		return false
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if _, exists := table.subscriptions[sessionID]; !exists {
		return false
	}
	delete(table.subscriptions, sessionID)
	return true
}

// Match returns one target per session, ordered by SessionID. Overlapping
// filters in one session collapse to one target using the highest granted QoS.
func (table *Table) Match(
	publisherSessionID SessionID,
	topic string,
	publishQoS mqtt.QoS,
	publishedRetain bool,
) ([]Match, error) {
	if err := ValidateSessionID(publisherSessionID); err != nil {
		return nil, err
	}
	if err := ValidateTopicName(topic); err != nil {
		return nil, err
	}
	if publishQoS > mqtt.QoSExactlyOnce {
		return nil, ErrInvalidQoS
	}

	table.mu.RLock()
	matches := make([]Match, 0, len(table.subscriptions))
	for sessionID, byFilter := range table.subscriptions {
		var (
			maxGranted mqtt.QoS
			matched    bool
			retain     bool
		)
		for filter, subscription := range byFilter {
			matchesTopic := topicMatchesFilter(topic, filter)
			if !matchesTopic || (subscription.NoLocal && sessionID == publisherSessionID) {
				continue
			}
			if !matched || subscription.QoS > maxGranted {
				maxGranted = subscription.QoS
			}
			if publishedRetain && subscription.RetainAsPublished {
				retain = true
			}
			matched = true
		}
		if matched {
			effectiveQoS := publishQoS
			if maxGranted < effectiveQoS {
				effectiveQoS = maxGranted
			}
			matches = append(matches, Match{SessionID: sessionID, QoS: effectiveQoS, Retain: retain})
		}
	}
	table.mu.RUnlock()

	sort.Slice(matches, func(left, right int) bool {
		return matches[left].SessionID < matches[right].SessionID
	})
	return matches, nil
}

// ValidateSessionID validates the session key used by routing operations.
func ValidateSessionID(sessionID SessionID) error {
	if sessionID == "" || strings.ContainsRune(string(sessionID), '\x00') {
		return ErrInvalidSessionID
	}
	return nil
}

func validMQTTString(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, codePoint := range value {
		if codePoint == 0 || (codePoint >= 0xD800 && codePoint <= 0xDFFF) {
			return false
		}
		if codePoint >= 0xFDD0 && codePoint <= 0xFDEF {
			return false
		}
		if codePoint&0xFFFF == 0xFFFE || codePoint&0xFFFF == 0xFFFF {
			return false
		}
	}
	return true
}

// TopicMatchesFilter validates a concrete topic and filter, then applies the
// production MQTT wildcard and system-topic matcher.
func TopicMatchesFilter(topic, filter string) (bool, error) {
	if err := ValidateTopicName(topic); err != nil {
		return false, err
	}
	if err := ValidateTopicFilter(filter); err != nil {
		return false, err
	}
	return topicMatchesFilter(topic, filter), nil
}
func topicMatchesFilter(topic, filter string) bool {
	if strings.HasPrefix(topic, "$") {
		firstLevelEnd := strings.IndexByte(filter, '/')
		firstLevel := filter
		if firstLevelEnd >= 0 {
			firstLevel = filter[:firstLevelEnd]
		}
		if firstLevel == "+" || firstLevel == "#" {
			return false
		}
	}

	topicLevels := strings.Split(topic, "/")
	filterLevels := strings.Split(filter, "/")
	for index, filterLevel := range filterLevels {
		if filterLevel == "#" {
			return true
		}
		if index >= len(topicLevels) {
			return false
		}
		if filterLevel != "+" && filterLevel != topicLevels[index] {
			return false
		}
	}
	return len(filterLevels) == len(topicLevels)
}
