// Package routing owns canonical MQTT topic/filter validation and subscription
// matching (both non-shared and shared). It does not know about sockets, queues,
// tenants, or retained-message storage.
package routing

import (
	"errors"
	"sort"
	"strings"
	"sync"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrInvalidSessionID means that a route operation has no valid session key.
	ErrInvalidSessionID = errors.New("invalid routing session id")
	// ErrInvalidTopicFilter aliases the tenancy filter validation contract.
	ErrInvalidTopicFilter = tenancy.ErrInvalidTopicFilter
	// ErrInvalidTopicName aliases the tenancy topic validation contract.
	ErrInvalidTopicName = tenancy.ErrInvalidTopicName
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

// Target identifies one session and its effective delivery options.
type Target struct {
	SessionID SessionID
	QoS       mqtt.QoS
	Retain    bool
}

// Match is one logical delivery route with an ordered candidate list.
type Match struct {
	Primary   Target
	Fallbacks []Target
}

// ProjectionSession is one committed tenant-scoped owner in a route projection.
type ProjectionSession struct {
	TenantID  tenancy.TenantID
	SessionID SessionID
}

// ProjectionSubscription is one subscription in a full committed-state route projection.
type ProjectionSubscription struct {
	TenantID     tenancy.TenantID
	SessionID    SessionID
	Subscription mqtt.Subscription
}

// Table stores subscriptions and matches publications against a consistent
// snapshot. The initial Phase 0 implementation deliberately scans the small
// table instead of introducing an index whose invalidation rules are not yet
// needed.
type Table struct {
	mu             sync.RWMutex
	subscriptions  map[SessionID]map[string]mqtt.Subscription
	shared         map[string]map[SessionID]mqtt.Subscription // sharedFilter -> SessionID -> Subscription
	sharedCursors  map[string]uint64                          // sharedFilter -> round-robin cursor
	sessionTenants map[SessionID]tenancy.TenantID
}

// NewTable creates an empty synchronized routing table.
func NewTable() *Table {
	return &Table{
		subscriptions:  make(map[SessionID]map[string]mqtt.Subscription),
		shared:         make(map[string]map[SessionID]mqtt.Subscription),
		sharedCursors:  make(map[string]uint64),
		sessionTenants: make(map[SessionID]tenancy.TenantID),
	}
}

// ReplaceProjection validates and atomically replaces the complete route projection.
// Shared-subscription round-robin cursors are retained for groups present in both views.
func (table *Table) ReplaceProjection(projection []ProjectionSubscription) error {
	return table.ReplaceProjectionWithSessions(nil, projection)
}

// ReplaceProjectionWithSessions also installs committed tenant bindings for
// owned sessions that do not currently have subscriptions.
func (table *Table) ReplaceProjectionWithSessions(
	sessions []ProjectionSession,
	projection []ProjectionSubscription,
) error {
	next := NewTable()
	type projectionKey struct {
		sessionID SessionID
		filter    string
	}
	seen := make(map[projectionKey]struct{}, len(projection))
	sessionTenants := make(map[SessionID]tenancy.TenantID, len(sessions)+len(projection))
	bindTenant := func(sessionID SessionID, tenantID tenancy.TenantID) error {
		if tenantID.String() == "" {
			return errors.New("route projection entry has no tenant")
		}
		if existing, exists := sessionTenants[sessionID]; exists && existing != tenantID {
			return errors.New("route projection session belongs to multiple tenants")
		}
		sessionTenants[sessionID] = tenantID
		return next.BindSessionTenant(sessionID, tenantID)
	}
	for _, entry := range sessions {
		if err := bindTenant(entry.SessionID, entry.TenantID); err != nil {
			return err
		}
	}
	for _, entry := range projection {
		key := projectionKey{sessionID: entry.SessionID, filter: entry.Subscription.Filter}
		if _, duplicate := seen[key]; duplicate {
			return errors.New("duplicate subscription in route projection")
		}
		seen[key] = struct{}{}
		if err := bindTenant(entry.SessionID, entry.TenantID); err != nil {
			return err
		}
		if _, err := next.Subscribe(entry.SessionID, entry.Subscription); err != nil {
			return err
		}
	}

	table.mu.Lock()
	for filter := range next.shared {
		if cursor, exists := table.sharedCursors[filter]; exists {
			next.sharedCursors[filter] = cursor
		}
	}
	table.subscriptions = next.subscriptions
	table.shared = next.shared
	table.sharedCursors = next.sharedCursors
	table.sessionTenants = next.sessionTenants
	table.mu.Unlock()
	return nil
}

// ValidateTopicFilter validates MQTT wildcard placement and UTF-8 rules.
func ValidateTopicFilter(filter string) error {
	return tenancy.ValidateTopicFilter(filter)
}

// ParseSharedFilter parses an MQTT 5.0 shared subscription filter.
// If filter does not start with "$share/", it returns isShared=false and the original filter.
func ParseSharedFilter(filter string) (shareName string, topicFilter string, isShared bool, err error) {
	return tenancy.ParseSharedFilter(filter)
}

// ValidateTopicName validates a concrete publication topic without wildcards.
func ValidateTopicName(topic string) error {
	return tenancy.ValidateTopicName(topic)
}

// IsSharedFilter reports whether filter uses MQTT 5 shared-subscription syntax.
func IsSharedFilter(filter string) bool {
	return tenancy.IsSharedFilter(filter)
}

// BindSessionTenant associates a session ID with a tenant namespace.
func (table *Table) BindSessionTenant(sessionID SessionID, tenantID tenancy.TenantID) error {
	if err := ValidateSessionID(sessionID); err != nil {
		return err
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.sessionTenants == nil {
		table.sessionTenants = make(map[SessionID]tenancy.TenantID)
	}
	table.sessionTenants[sessionID] = tenantID
	return nil
}

// SessionTenant returns the tenant namespace bound to sessionID, or zero if unbound.
func (table *Table) SessionTenant(sessionID SessionID) tenancy.TenantID {
	if table == nil {
		return tenancy.TenantID{}
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if table.sessionTenants == nil {
		return tenancy.TenantID{}
	}
	return table.sessionTenants[sessionID]
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
	if table.sessionTenants != nil {
		if _, exists := table.sessionTenants[sessionID]; exists {
			delete(table.sessionTenants, sessionID)
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

// Match returns one logical route per matching non-shared session or shared
// filter, ordered by the primary SessionID. Overlapping non-shared filters in
// one session collapse to one target using the highest granted QoS.
func (table *Table) Match(
	publisherSessionID SessionID,
	topic string,
	publishQoS mqtt.QoS,
	publishedRetain bool,
) ([]Match, error) {
	if err := validateMatchInput(publisherSessionID, topic, publishQoS); err != nil {
		return nil, err
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	return table.matchLocked(table.sessionTenants[publisherSessionID], publisherSessionID, topic, publishQoS, publishedRetain), nil
}

// MatchForTenant matches a publication using its authenticated tenant after the
// publisher session has been removed from the routing projection.
func (table *Table) MatchForTenant(
	publisherSessionID SessionID,
	publisherTenant tenancy.TenantID,
	topic string,
	publishQoS mqtt.QoS,
	publishedRetain bool,
) ([]Match, error) {
	if err := validateMatchInput(publisherSessionID, topic, publishQoS); err != nil {
		return nil, err
	}
	if publisherTenant.String() == "" {
		return nil, tenancy.ErrInvalidTenantID
	}

	table.mu.Lock()
	defer table.mu.Unlock()
	if boundTenant := table.sessionTenants[publisherSessionID]; boundTenant != (tenancy.TenantID{}) && boundTenant != publisherTenant {
		return nil, tenancy.ErrTenantMismatch
	}
	return table.matchLocked(publisherTenant, publisherSessionID, topic, publishQoS, publishedRetain), nil
}

func validateMatchInput(publisherSessionID SessionID, topic string, publishQoS mqtt.QoS) error {
	if err := ValidateSessionID(publisherSessionID); err != nil {
		return err
	}
	if err := ValidateTopicName(topic); err != nil {
		return err
	}
	if publishQoS > mqtt.QoSExactlyOnce {
		return ErrInvalidQoS
	}
	return nil
}

func (table *Table) matchLocked(
	publisherTenant tenancy.TenantID,
	publisherSessionID SessionID,
	topic string,
	publishQoS mqtt.QoS,
	publishedRetain bool,
) []Match {
	matches := make([]Match, 0, len(table.subscriptions)+len(table.shared))
	for sessionID, byFilter := range table.subscriptions {
		if table.sessionTenants[sessionID] != publisherTenant {
			continue
		}
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
			matches = append(matches, Match{
				Primary: Target{
					SessionID: sessionID,
					QoS:       effectiveQoS,
					Retain:    retain,
				},
			})
		}
	}

	if len(table.shared) > 0 {
		sharedFilters := make([]string, 0, len(table.shared))
		for filter := range table.shared {
			sharedFilters = append(sharedFilters, filter)
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

			candidates := make([]Target, 0, len(members))
			for sessionID, subscription := range members {
				if table.sessionTenants[sessionID] != publisherTenant {
					continue
				}
				effectiveQoS := publishQoS
				if subscription.QoS < effectiveQoS {
					effectiveQoS = subscription.QoS
				}
				candidates = append(candidates, Target{
					SessionID: sessionID,
					QoS:       effectiveQoS,
					Retain:    publishedRetain && subscription.RetainAsPublished,
				})
			}
			if len(candidates) == 0 {
				continue
			}
			sort.Slice(candidates, func(left, right int) bool {
				return candidates[left].SessionID < candidates[right].SessionID
			})

			cursorKey := sharedFilter
			if publisherTenant != (tenancy.TenantID{}) {
				cursorKey = publisherTenant.String() + "/" + sharedFilter
			}
			cursor := table.sharedCursors[cursorKey]
			primaryIndex := int(cursor % uint64(len(candidates)))
			primary := candidates[primaryIndex]
			var fallbacks []Target
			if len(candidates) > 1 {
				fallbacks = make([]Target, 0, len(candidates)-1)
				for offset := 1; offset < len(candidates); offset++ {
					fallbacks = append(fallbacks, candidates[(primaryIndex+offset)%len(candidates)])
				}
			}
			table.sharedCursors[cursorKey]++
			matches = append(matches, Match{Primary: primary, Fallbacks: fallbacks})
		}
	}

	sort.SliceStable(matches, func(left, right int) bool {
		return matches[left].Primary.SessionID < matches[right].Primary.SessionID
	})
	return matches
}

// ValidateSessionID validates the session key used by routing operations.
func ValidateSessionID(sessionID SessionID) error {
	if sessionID == "" || strings.ContainsRune(string(sessionID), '\x00') {
		return ErrInvalidSessionID
	}
	return nil
}

// TopicMatchesFilter validates a concrete topic and filter, then applies the
// production MQTT wildcard and system-topic matcher.
func TopicMatchesFilter(topic, filter string) (bool, error) {
	return tenancy.TopicMatchesFilter(topic, filter)
}

func topicMatchesFilter(topic, filter string) bool {
	matched, err := tenancy.TopicMatchesFilter(topic, filter)
	return err == nil && matched
}

// SubscriptionRecord represents a snapshot of one active subscription.
type SubscriptionRecord struct {
	TenantID    tenancy.TenantID
	SessionID   SessionID
	TopicFilter string
	QoS         mqtt.QoS
	SharedGroup *string
}

// Snapshot returns a consistent snapshot of all registered subscriptions.
func (table *Table) Snapshot() []SubscriptionRecord {
	if table == nil {
		return nil
	}
	table.mu.RLock()
	defer table.mu.RUnlock()

	var records []SubscriptionRecord

	// Non-shared subscriptions
	for sessionID, byFilter := range table.subscriptions {
		tenantID := table.sessionTenants[sessionID]
		for _, sub := range byFilter {
			records = append(records, SubscriptionRecord{
				TenantID:    tenantID,
				SessionID:   sessionID,
				TopicFilter: sub.Filter,
				QoS:         sub.QoS,
				SharedGroup: nil,
			})
		}
	}

	// Shared subscriptions
	for fullFilter, bySession := range table.shared {
		shareName, topicFilter, _, _ := ParseSharedFilter(fullFilter)
		group := shareName
		for sessionID, sub := range bySession {
			tenantID := table.sessionTenants[sessionID]
			records = append(records, SubscriptionRecord{
				TenantID:    tenantID,
				SessionID:   sessionID,
				TopicFilter: topicFilter,
				QoS:         sub.QoS,
				SharedGroup: &group,
			})
		}
	}

	return records
}
