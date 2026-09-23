// Package takeover implements a deterministic session ownership state machine.
package takeover

import (
	"errors"
	"sort"
	"sync"

	"narumq/internal/domain/delivery"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrOutOfOrderCommit indicates that an application sequence index is zero or not strictly greater than the last applied sequence position.
	ErrOutOfOrderCommit = errors.New("out of order commit index")
)

// AcquireOwnershipCommandDTO carries input parameters for acquiring session ownership.
type AcquireOwnershipCommandDTO struct {
	SessionKey    tenancy.SessionKey
	ExpectedEpoch session.Epoch
	OwnerNodeID   string
}

// PersistentSubscriptionCommandDTO replaces one persistent subscription after ownership fencing.
type PersistentSubscriptionCommandDTO struct {
	SessionKey   tenancy.SessionKey
	OwnerNodeID  string
	Epoch        session.Epoch
	Subscription mqtt.Subscription
}

// QoSInflightCommandDTO replaces one directional inflight exchange after ownership fencing.
type QoSInflightCommandDTO struct {
	SessionKey  tenancy.SessionKey
	OwnerNodeID string
	Epoch       session.Epoch
	Publish     mqtt.Publish
}

// ApplyResultDTO represents the deterministic outcome of applying a committed ownership command.
type ApplyResultDTO struct {
	Ownership session.Ownership
	Applied   bool
	Err       error
}

// StateMachine maintains the application-owned session aggregate in committed order.
type StateMachine struct {
	mu               sync.RWMutex
	lastAppliedIndex uint64
	ownerships       map[tenancy.SessionKey]session.Ownership
	subscriptions    map[tenancy.SessionKey]map[string]mqtt.Subscription
	inbound          map[tenancy.SessionKey]map[mqtt.PacketIdentifier]delivery.Inbound
	outbound         map[tenancy.SessionKey]map[mqtt.PacketIdentifier]delivery.Outbound
}

// NewStateMachine constructs an empty session aggregate.
func NewStateMachine() *StateMachine {
	return &StateMachine{
		ownerships:    make(map[tenancy.SessionKey]session.Ownership),
		subscriptions: make(map[tenancy.SessionKey]map[string]mqtt.Subscription),
		inbound:       make(map[tenancy.SessionKey]map[mqtt.PacketIdentifier]delivery.Inbound),
		outbound:      make(map[tenancy.SessionKey]map[mqtt.PacketIdentifier]delivery.Outbound),
	}
}

// LastAppliedIndex returns the highest application committed-command sequence position applied so far.
func (sm *StateMachine) LastAppliedIndex() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastAppliedIndex
}

// Ownership returns the current ownership record for the given key, if any.
func (sm *StateMachine) Ownership(key tenancy.SessionKey) (session.Ownership, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	record, exists := sm.ownerships[key]
	return record, exists
}

// PersistentSubscriptions returns an independent, filter-sorted snapshot.
func (sm *StateMachine) PersistentSubscriptions(key tenancy.SessionKey) []mqtt.Subscription {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	byFilter := sm.subscriptions[key]
	if len(byFilter) == 0 {
		return nil
	}
	result := make([]mqtt.Subscription, 0, len(byFilter))
	for _, subscription := range byFilter {
		result = append(result, subscription)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Filter < result[j].Filter })
	return result
}

// InboundQoSInflight returns an independent, packet-ID-sorted snapshot.
func (sm *StateMachine) InboundQoSInflight(key tenancy.SessionKey) []mqtt.PacketIdentifier {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sortedPacketIDs(sm.inbound[key])
}

// OutboundQoSInflight returns an independent, packet-ID-sorted snapshot.
func (sm *StateMachine) OutboundQoSInflight(key tenancy.SessionKey) []mqtt.PacketIdentifier {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sortedPacketIDs(sm.outbound[key])
}

func sortedPacketIDs[T any](entries map[mqtt.PacketIdentifier]T) []mqtt.PacketIdentifier {
	if len(entries) == 0 {
		return nil
	}
	ids := make([]mqtt.PacketIdentifier, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Authorize verifies that the given key, ownerNodeID, and epoch match the active ownership.
func (sm *StateMachine) Authorize(key tenancy.SessionKey, ownerNodeID string, epoch session.Epoch) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	record, exists := sm.ownerships[key]
	if !exists {
		return session.ErrInvalidOwnership
	}
	return record.Authorize(key, ownerNodeID, epoch)
}

// Apply processes a committed ownership command at the specified application sequence index.
//
// The commitIndex parameter is an application-owned monotonic sequence number
// assigned while consuming the ownership aggregate's committed-command stream.
// It is not a Raft log index or the consensus adapter's StatusDTO.CommitIndex.
//
// Invariants:
//   - commitIndex must be non-zero and strictly greater than lastAppliedIndex.
//   - If commitIndex is invalid or out-of-order, ErrOutOfOrderCommit is returned without modifying any state.
//   - If commitIndex is valid, lastAppliedIndex advances to commitIndex.
//   - If the command fails domain validation (e.g. ErrStaleEpoch, ErrInvalidOwnership),
//     the domain error is returned in ApplyResultDTO with Applied = false, leaving the ownership map untouched.
//   - If the command succeeds, the new ownership is recorded and returned with Applied = true.
func (sm *StateMachine) Apply(commitIndex uint64, command AcquireOwnershipCommandDTO) ApplyResultDTO {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if err := sm.advance(commitIndex); err != nil {
		return ApplyResultDTO{Err: err}
	}

	var current *session.Ownership
	if existing, exists := sm.ownerships[command.SessionKey]; exists {
		current = &existing
	}

	record, err := session.AcquireOwnership(current, command.SessionKey, command.ExpectedEpoch, command.OwnerNodeID, commitIndex)
	if err != nil {
		return ApplyResultDTO{
			Applied: false,
			Err:     err,
		}
	}

	sm.ownerships[command.SessionKey] = record
	return ApplyResultDTO{
		Ownership: record,
		Applied:   true,
		Err:       nil,
	}
}

// ApplyPersistentSubscription atomically fences and replaces one subscription.
func (sm *StateMachine) ApplyPersistentSubscription(commitIndex uint64, command PersistentSubscriptionCommandDTO) ApplyResultDTO {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if err := sm.advance(commitIndex); err != nil {
		return ApplyResultDTO{Err: err}
	}
	record, err := sm.authorize(command.SessionKey, command.OwnerNodeID, command.Epoch)
	if err != nil {
		return ApplyResultDTO{Err: err}
	}
	subscription := command.Subscription
	if err := routing.ValidateTopicFilter(subscription.Filter); err != nil {
		return ApplyResultDTO{Err: err}
	}
	if subscription.QoS > mqtt.QoSExactlyOnce {
		return ApplyResultDTO{Err: routing.ErrInvalidQoS}
	}
	if subscription.RetainHandling > mqtt.DoNotSendRetained {
		return ApplyResultDTO{Err: routing.ErrInvalidRetainHandling}
	}
	if sm.subscriptions[command.SessionKey] == nil {
		sm.subscriptions[command.SessionKey] = make(map[string]mqtt.Subscription)
	}
	sm.subscriptions[command.SessionKey][subscription.Filter] = subscription
	return ApplyResultDTO{Ownership: record, Applied: true}
}

// ApplyInboundQoSInflight atomically fences and replaces one inbound QoS exchange.
func (sm *StateMachine) ApplyInboundQoSInflight(commitIndex uint64, command QoSInflightCommandDTO) ApplyResultDTO {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if err := sm.advance(commitIndex); err != nil {
		return ApplyResultDTO{Err: err}
	}
	record, err := sm.authorize(command.SessionKey, command.OwnerNodeID, command.Epoch)
	if err != nil {
		return ApplyResultDTO{Err: err}
	}
	state, _, err := delivery.NewInbound(command.Publish)
	if err != nil {
		return ApplyResultDTO{Err: err}
	}
	if sm.inbound[command.SessionKey] == nil {
		sm.inbound[command.SessionKey] = make(map[mqtt.PacketIdentifier]delivery.Inbound)
	}
	sm.inbound[command.SessionKey][command.Publish.PacketID] = state
	return ApplyResultDTO{Ownership: record, Applied: true}
}

// ApplyOutboundQoSInflight atomically fences and replaces one outbound QoS exchange.
func (sm *StateMachine) ApplyOutboundQoSInflight(commitIndex uint64, command QoSInflightCommandDTO) ApplyResultDTO {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if err := sm.advance(commitIndex); err != nil {
		return ApplyResultDTO{Err: err}
	}
	record, err := sm.authorize(command.SessionKey, command.OwnerNodeID, command.Epoch)
	if err != nil {
		return ApplyResultDTO{Err: err}
	}
	state, _, err := delivery.NewOutbound(command.Publish)
	if err != nil {
		return ApplyResultDTO{Err: err}
	}
	if sm.outbound[command.SessionKey] == nil {
		sm.outbound[command.SessionKey] = make(map[mqtt.PacketIdentifier]delivery.Outbound)
	}
	sm.outbound[command.SessionKey][command.Publish.PacketID] = state
	return ApplyResultDTO{Ownership: record, Applied: true}
}

// advance is called under the aggregate lock before any command validation.
func (sm *StateMachine) advance(commitIndex uint64) error {
	if commitIndex == 0 || commitIndex <= sm.lastAppliedIndex {
		return ErrOutOfOrderCommit
	}
	sm.lastAppliedIndex = commitIndex
	return nil
}

func (sm *StateMachine) authorize(key tenancy.SessionKey, ownerNodeID string, epoch session.Epoch) (session.Ownership, error) {
	record, exists := sm.ownerships[key]
	if !exists {
		return session.Ownership{}, session.ErrInvalidOwnership
	}
	if err := record.Authorize(key, ownerNodeID, epoch); err != nil {
		return session.Ownership{}, err
	}
	return record, nil
}
