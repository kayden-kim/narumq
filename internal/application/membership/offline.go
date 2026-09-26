package membership

import (
	"bytes"
	"sort"
)

func (machine *StateMachine) applyOffline(index uint64, change OfflineChangeDTO) ApplyResultDTO {
	machine.purgeExpiredOfflineLocked(change.ProposedAtUnixMillis)

	key := sessionKey{tenantID: change.TenantID, clientID: change.ClientID}
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}

	switch change.Action {
	case OfflineActionDetach:
		if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
			return conflictResult(index, ConflictStaleEpoch)
		}
		record, exists := machine.offline[key]
		if exists && record.Epoch == change.Epoch {
			// Idempotent retry for the same detach epoch: no-op
			return result
		}
		nextSeq := uint64(1)
		var existingMessages []OfflineMessageDTO
		existingCount := 0
		existingBytes := 0
		if exists {
			nextSeq = record.NextSequence
			existingMessages = record.Messages
			existingCount = len(record.Messages)
			for _, msg := range record.Messages {
				existingBytes += offlineMessageBytes(change.TenantID, change.ClientID, msg)
			}
		}

		newCount := len(change.Messages)
		if existingCount+newCount > MaxOfflineMessagesPerSession {
			return conflictResult(index, ConflictOfflineCapacity)
		}
		if machine.offlineMessages-existingCount+existingCount+newCount > MaxOfflineMessages {
			return conflictResult(index, ConflictOfflineCapacity)
		}

		newBytes := 0
		for _, msg := range change.Messages {
			newBytes += offlineMessageBytes(change.TenantID, change.ClientID, msg)
		}
		if machine.offlineBytes-existingBytes+existingBytes+newBytes > MaxOfflineBytes {
			return conflictResult(index, ConflictOfflineCapacity)
		}

		merged := make([]OfflineMessageDTO, 0, existingCount+newCount)
		merged = append(merged, cloneOfflineMessages(existingMessages)...)
		for _, msg := range change.Messages {
			clonedMsg := msg
			clonedMsg.Sequence = nextSeq
			clonedMsg.Payload = bytes.Clone(msg.Payload)
			nextSeq++
			merged = append(merged, clonedMsg)
		}

		if exists {
			machine.offlineMessages -= existingCount
			machine.offlineBytes -= existingBytes
		}
		machine.offline[key] = OfflineSessionStateDTO{
			TenantID:                   change.TenantID,
			ClientID:                   change.ClientID,
			Epoch:                      change.Epoch,
			SessionExpiresAtUnixMillis: change.SessionExpiresAtUnixMillis,
			NextSequence:               nextSeq,
			Messages:                   merged,
		}
		machine.offlineMessages += len(merged)
		machine.offlineBytes += existingBytes + newBytes
		return result

	case OfflineActionEnqueue:
		record, exists := machine.offline[key]
		if !exists || record.Epoch != change.Epoch {
			return conflictResult(index, ConflictOfflineSessionMissing)
		}
		if len(record.Messages) >= MaxOfflineMessagesPerSession {
			return conflictResult(index, ConflictOfflineCapacity)
		}
		if machine.offlineMessages >= MaxOfflineMessages {
			return conflictResult(index, ConflictOfflineCapacity)
		}
		msg := change.Messages[0]
		addedBytes := offlineMessageBytes(change.TenantID, change.ClientID, msg)
		if machine.offlineBytes+addedBytes > MaxOfflineBytes {
			return conflictResult(index, ConflictOfflineCapacity)
		}

		clonedMsg := msg
		clonedMsg.Sequence = record.NextSequence
		clonedMsg.Payload = bytes.Clone(msg.Payload)
		record.NextSequence++
		record.Messages = append(record.Messages, clonedMsg)
		machine.offline[key] = record
		machine.offlineMessages++
		machine.offlineBytes += addedBytes
		return result

	case OfflineActionClaim:
		if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
			return conflictResult(index, ConflictStaleEpoch)
		}
		record, exists := machine.offline[key]
		if !exists {
			result.OfflineClaim = &OfflineClaimDTO{
				Found:    false,
				Messages: nil,
			}
			return result
		}
		claimedMessages := cloneOfflineMessages(record.Messages)
		machine.removeOfflineLocked(key)
		result.OfflineClaim = &OfflineClaimDTO{
			Found:    true,
			Messages: claimedMessages,
		}
		return result

	default:
		return conflictResult(index, ConflictNone)
	}
}

func (machine *StateMachine) removeOfflineLocked(key sessionKey) {
	record, exists := machine.offline[key]
	if !exists {
		return
	}
	delete(machine.offline, key)
	machine.offlineMessages -= len(record.Messages)
	for _, msg := range record.Messages {
		machine.offlineBytes -= offlineMessageBytes(key.tenantID, key.clientID, msg)
	}
}

func (machine *StateMachine) purgeExpiredOfflineLocked(proposedAt uint64) {
	if proposedAt == 0 {
		return
	}
	for key, record := range machine.offline {
		if record.SessionExpiresAtUnixMillis > 0 && record.SessionExpiresAtUnixMillis <= proposedAt {
			machine.removeOfflineLocked(key)
			continue
		}
		// Purge expired individual messages
		retained := make([]OfflineMessageDTO, 0, len(record.Messages))
		removedCount := 0
		removedBytes := 0
		for _, msg := range record.Messages {
			if msg.ExpiresAtUnixMillis > 0 && msg.ExpiresAtUnixMillis <= proposedAt {
				removedCount++
				removedBytes += offlineMessageBytes(key.tenantID, key.clientID, msg)
				continue
			}
			retained = append(retained, msg)
		}
		if removedCount > 0 {
			record.Messages = retained
			machine.offline[key] = record
			machine.offlineMessages -= removedCount
			machine.offlineBytes -= removedBytes
		}
	}
}

func offlineMessageBytes(tenantID, clientID string, message OfflineMessageDTO) int {
	return len(tenantID) + len(clientID) + len(message.Topic) + len(message.Payload)
}

// OfflineSessions returns a tenant/client-sorted, deep-owned copy of committed offline sessions.
func (machine *StateMachine) OfflineSessions() []OfflineSessionStateDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	sessions := make([]OfflineSessionStateDTO, 0, len(machine.offline))
	for _, session := range machine.offline {
		sessions = append(sessions, cloneOfflineSession(session))
	}
	sort.Slice(sessions, func(left, right int) bool {
		return compareOffline(sessions[left], sessions[right]) < 0
	})
	return sessions
}

// DetachedEpoch reports whether the session has a committed offline record and its epoch.
func (machine *StateMachine) DetachedEpoch(tenantID, clientID string) (uint64, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	key := sessionKey{tenantID: tenantID, clientID: clientID}
	record, exists := machine.offline[key]
	if !exists {
		return 0, false
	}
	return record.Epoch, true
}

func cloneOfflineSession(session OfflineSessionStateDTO) OfflineSessionStateDTO {
	session.Messages = cloneOfflineMessages(session.Messages)
	return session
}

func cloneOfflineMessages(messages []OfflineMessageDTO) []OfflineMessageDTO {
	if len(messages) == 0 {
		return nil
	}
	copied := make([]OfflineMessageDTO, len(messages))
	for i, msg := range messages {
		copied[i] = msg
		copied[i].Payload = bytes.Clone(msg.Payload)
	}
	return copied
}

func compareOffline(left, right OfflineSessionStateDTO) int {
	return compareSession(left.TenantID, left.ClientID, right.TenantID, right.ClientID)
}
