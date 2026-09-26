package membership

import (
	"bytes"
	"sort"

	"narumq/internal/domain/tenancy"
)

type retainedKey struct {
	tenantID string
	topic    string
}

func (machine *StateMachine) applyRetained(index uint64, change RetainedChangeDTO) ApplyResultDTO {
	for key, current := range machine.retained {
		if current.ExpiresAtUnixMillis == 0 || current.ExpiresAtUnixMillis > change.ProposedAtUnixMillis {
			continue
		}
		delete(machine.retained, key)
		machine.retainedBytes -= retainedStateBytes(current)
	}

	key := retainedKey{tenantID: change.TenantID, topic: change.Topic}
	current, exists := machine.retained[key]
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	if change.Action == RetainedActionDelete {
		if exists {
			delete(machine.retained, key)
			machine.retainedBytes -= retainedStateBytes(current)
		}
		return result
	}

	state := RetainedStateDTO{
		TenantID: change.TenantID, Topic: change.Topic, PublisherClientID: change.PublisherClientID,
		QoS: change.QoS, Payload: change.Payload, ExpiresAtUnixMillis: change.ExpiresAtUnixMillis,
	}
	currentBytes := machine.retainedBytes
	count := len(machine.retained)
	if exists {
		currentBytes -= retainedStateBytes(current)
	} else {
		count++
	}
	stateBytes := retainedStateBytes(state)
	if count > MaxRetainedMessages || stateBytes > MaxRetainedBytes-currentBytes {
		return conflictResult(index, ConflictRetainedCapacity)
	}
	state.Payload = bytes.Clone(change.Payload)
	machine.retained[key] = state
	machine.retainedBytes = currentBytes + stateBytes
	return result
}

// RetainedMessages returns a tenant/topic-sorted, deep-owned copy of committed retained values.
func (machine *StateMachine) RetainedMessages() []RetainedStateDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	messages := make([]RetainedStateDTO, 0, len(machine.retained))
	for _, message := range machine.retained {
		messages = append(messages, cloneRetainedState(message))
	}
	sort.Slice(messages, func(left, right int) bool {
		return compareRetained(messages[left], messages[right]) < 0
	})
	return messages
}

func cloneRetainedState(state RetainedStateDTO) RetainedStateDTO {
	state.Payload = bytes.Clone(state.Payload)
	return state
}

func retainedStateBytes(state RetainedStateDTO) int {
	tenantID, _ := tenancy.NewTenantID(state.TenantID)
	session, _ := tenancy.NewSessionKey(tenantID, state.PublisherClientID)
	return len(state.TenantID) + len(state.Topic) + len(state.Payload) + len(session.RoutingID())
}
