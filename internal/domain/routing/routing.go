// Package routing owns canonical MQTT topic/filter validation and subscription
// matching (both non-shared and shared). It does not know about sockets, queues,
// tenants, or retained-message storage.
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
	// ErrSharedSubscriptionUnsupported is retained for backwards compatibility
	// but is no longer returned for valid shared subscriptions.
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
	shared        map[string]map[SessionID]mqtt.Subscription // sharedFilter -> SessionID -> Subscription
	sharedCursors map[string]uint64                          // sharedFilter -> round-robin cursor
}

// NewTable creates an empty synchronized routing table.
func NewTable() *Table {
	return &Table{
		subscriptions: make(map[SessionID]map[string]mqtt.Subscription),
		shared:        make(map[string]map[SessionID]mqtt.Subscription),
		sharedCursors: make(map[string]uint64),
	}
}

// ValidateTopicFilter validates MQTT wildcard placement and UTF-8 rules.
func ValidateTopicFilter(filter string) error {
	if filter == "" || !validMQTTString(filter) {
		return ErrInvalidTopicFilter
	}
	if IsSharedFilter(filter) {
		_, _, _, err := ParseSharedFilter(filter)
		return err
	}
	return validateFilterLevels(filter)
}

func validateFilterLevels(filter string) error {
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

// ParseSharedFilter parses an MQTT 5.0 shared subscription filter ($share/{ShareName}/{TopicFilter}).
// If filter does not start with "$share/", it returns isShared=false and the original filter.
func ParseSharedFilter(filter string) (shareName string, topicFilter string, isShared bool, err error) {
	if !strings.HasPrefix(filter, "$share/") {
		return "", filter, false, nil
	}
	if !validMQTTString(filter) {
		return "", "", true, ErrInvalidTopicFilter
	}
	rest := filter[len("$share/"):]
	slashIdx := strings.IndexByte(rest, '/')
	if slashIdx < 0 {
		return "", "", true, ErrInvalidTopicFilter
	}
	shareName = rest[:slashIdx]
	if len(shareName) == 0 || strings.ContainsAny(shareName, "/+#") {
		return "", "", true, ErrInvalidTopicFilter
	}
	topicFilter = rest[slashIdx+1:]
	if len(topicFilter) == 0 || strings.HasPrefix(topicFilter, "$share/") {
		return "", "", true, ErrInvalidTopicFilter
	}
	if err := validateFilterLevels(topicFilter); err != nil {
		return "", "", true, err
	}
	return shareName, topicFilter, true, nil
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
	if table.shared == nil {
		table.shared = make(map[string]map[SessionID]mqtt.Subscription)
	}
	if table.sharedCursors == nil {
		table.sharedCursors = make(map[string]uint64)
	}

	if IsSharedFilter(subscription.Filter) {
		bySession := table.shared[subscription.Filter]
		if bySession == nil {
			bySession = make(map[SessionID]mqtt.Subscription)
			table.shared[subscription.Filter] = bySession
		}
		_, replaced = bySession[sessionID]
		bySession[sessionID] = subscription
		return replaced, nil
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
	if IsSharedFilter(filter) {
		if table.shared == nil {
			return mqtt.Subscription{}, false
		}
		bySession := table.shared[filter]
		if bySession == nil {
			return mqtt.Subscription{}, false
		}
		subscription, found := bySession[sessionID]
		return subscription, found
	}
	if table.subscriptions == nil {
		return mqtt.Subscription{}, false
	}
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
	if IsSharedFilter(filter) {
		if table.shared == nil {
			return false, nil
		}
		bySession := table.shared[filter]
		if bySession == nil {
			return false, nil
		}
		if _, ok := bySession[sessionID]; !ok {
			return false, nil
		}
		delete(bySession, sessionID)
		if len(bySession) == 0 {
			delete(table.shared, filter)
			if table.sharedCursors != nil {
				delete(table.sharedCursors, filter)
			}
		}
		return true, nil
	}

	if table.subscriptions == nil {
		return false, nil
	}
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
	var removed bool
	if table.subscriptions != nil {
		if _, exists := table.subscriptions[sessionID]; exists {
			delete(table.subscriptions, sessionID)
			removed = true
		}
	}
	if table.shared != nil {
		for filter, bySession := range table.shared {
			if _, exists := bySession[sessionID]; exists {
				delete(bySession, sessionID)
				removed = true
				if len(bySession) == 0 {
					delete(table.shared, filter)
					if table.sharedCursors != nil {
						delete(table.sharedCursors, filter)
					}
				}
			}
		}
	}
	return removed
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

	table.mu.Lock()
	defer table.mu.Unlock()

	matches := make([]Match, 0, len(table.subscriptions)+len(table.shared))
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

	if len(table.shared) > 0 {
		sharedFilters := make([]string, 0, len(table.shared))
		for f := range table.shared {
			sharedFilters = append(sharedFilters, f)
		}
		sort.Strings(sharedFilters)

		if table.sharedCursors == nil {
			table.sharedCursors = make(map[string]uint64)
		}

		for _, sharedFilter := range sharedFilters {
			members := table.shared[sharedFilter]
			if len(members) == 0 {
				continue
			}
			_, topicFilter, _, _ := ParseSharedFilter(sharedFilter)
			if !topicMatchesFilter(topic, topicFilter) {
				continue
			}

			candidates := make([]SessionID, 0, len(members))
			for sID := range members {
				candidates = append(candidates, sID)
			}
			sort.Slice(candidates, func(i, j int) bool {
				return candidates[i] < candidates[j]
			})

			cursor := table.sharedCursors[sharedFilter]
			chosenSession := candidates[cursor%uint64(len(candidates))]
			table.sharedCursors[sharedFilter]++

			sub := members[chosenSession]
			effectiveQoS := publishQoS
			if sub.QoS < effectiveQoS {
				effectiveQoS = sub.QoS
			}
			retain := publishedRetain && sub.RetainAsPublished
			matches = append(matches, Match{SessionID: chosenSession, QoS: effectiveQoS, Retain: retain})
		}
	}

	sort.SliceStable(matches, func(left, right int) bool {
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
	targetFilter := filter
	if IsSharedFilter(filter) {
		_, subFilter, _, _ := ParseSharedFilter(filter)
		targetFilter = subFilter
	}
	return topicMatchesFilter(topic, targetFilter), nil
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
