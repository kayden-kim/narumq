package membership

import (
	"bytes"
	"sort"

	"narumq/internal/domain/routing"
)

func (machine *StateMachine) applyDelivery(index uint64, change DeliveryChangeDTO) ApplyResultDTO {
	if !machine.sharedRerouteActiveLocked() {
		for _, target := range change.Targets {
			if target.SharedFilter != "" {
				return conflictResult(index, ConflictFeatureInactive)
			}
		}
	}
	machine.purgeExpiredDeliveryLocked(change.ProposedAtUnixMillis)
	key := sessionKey{tenantID: change.TenantID, clientID: change.ClientID}
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied, Delivery: &DeliveryResultDTO{}}
	switch change.Action {
	case DeliveryActionPublish:
		result.Delivery.Found = true
		if change.ExpiresAtUnixMillis > 0 && change.ExpiresAtUnixMillis <= change.ProposedAtUnixMillis {
			return result
		}
		result.Delivery.Enqueued, result.Delivery.Saturated, result.Delivery.Skipped, result.TenantQuota = machine.enqueueDeliveryTargetsLocked(change.TenantID, change.Topic, change.Payload, change.PublishQoS, change.Retain, change.ExpiresAtUnixMillis, change.Targets)
		return result
	case DeliveryActionReceive:
		if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
			return conflictResult(index, ConflictStaleEpoch)
		}
		record, exists := machine.delivery[key]
		if !exists {
			return conflictResult(index, ConflictDeliverySessionMissing)
		}
		for _, inbound := range record.Inbound {
			if inbound.PacketID == change.PacketID {
				if !inbound.Released {
					return result
				}
				break
			}
		}
		newInbound := DeliveryInboundDTO{PacketID: change.PacketID, Topic: change.Topic, Payload: change.Payload, Retain: change.Retain, ExpiresAtUnixMillis: change.ExpiresAtUnixMillis}
		oldIndex := -1
		for i, inbound := range record.Inbound {
			if inbound.PacketID == change.PacketID && inbound.Released {
				oldIndex = i
				break
			}
		}
		removeIndex := -1
		if oldIndex < 0 && len(record.Inbound) >= MaxDeliveryInboundPerSession {
			for i, inbound := range record.Inbound {
				if inbound.Released && (removeIndex < 0 || inbound.ReleasedIndex < record.Inbound[removeIndex].ReleasedIndex) {
					removeIndex = i
				}
			}
			if removeIndex < 0 {
				return conflictResult(index, ConflictDeliveryCapacity)
			}
		}
		oldBytes := 0
		if oldIndex >= 0 {
			oldBytes = deliveryInboundBytes(key, record.Inbound[oldIndex])
		} else if removeIndex >= 0 {
			oldBytes = deliveryInboundBytes(key, record.Inbound[removeIndex])
		}
		newBytes := deliveryInboundBytes(key, newInbound)
		if machine.deliveryEntries-oldBoolCount(oldIndex >= 0 || removeIndex >= 0)+1 > MaxDeliveryEntries || machine.deliveryBytes-oldBytes+newBytes > MaxDeliveryBytes {
			return conflictResult(index, ConflictDeliveryCapacity)
		}
		if rejection := machine.tenantQuotaExceededLocked(change.TenantID, 0, 0, 0, newBytes-oldBytes); rejection != nil {
			result := conflictResult(index, ConflictDeliveryCapacity)
			result.TenantQuota = rejection
			return result
		}
		newInbound.Payload = bytes.Clone(change.Payload)
		if oldIndex >= 0 {
			record.Inbound = append(record.Inbound[:oldIndex], record.Inbound[oldIndex+1:]...)
		} else if removeIndex >= 0 {
			record.Inbound = append(record.Inbound[:removeIndex], record.Inbound[removeIndex+1:]...)
		}
		record.Inbound = append(record.Inbound, newInbound)
		sort.Slice(record.Inbound, func(i, j int) bool { return record.Inbound[i].PacketID < record.Inbound[j].PacketID })
		machine.delivery[key] = record
		machine.addDeliveryInboundUsageLocked(change.TenantID, 1-oldBoolCount(oldIndex >= 0 || removeIndex >= 0), newBytes-oldBytes)
		return result
	case DeliveryActionRelease:
		if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
			return conflictResult(index, ConflictStaleEpoch)
		}
		record, exists := machine.delivery[key]
		if !exists {
			return result
		}
		inboundIndex := -1
		for i := range record.Inbound {
			if record.Inbound[i].PacketID == change.PacketID {
				inboundIndex = i
				break
			}
		}
		if inboundIndex < 0 {
			return result
		}
		inbound := record.Inbound[inboundIndex]
		if inbound.Released {
			result.Delivery.Released = true
			return result
		}
		result.Delivery.Found = true
		result.Delivery.Released = true
		publishPayload := bytes.Clone(inbound.Payload)
		machine.addDeliveryInboundUsageLocked(change.TenantID, 0, -len(inbound.Payload))
		inbound.Payload = nil
		inbound.Released = true
		inbound.ReleasedIndex = index
		record.Inbound[inboundIndex] = inbound
		machine.delivery[key] = record
		if inbound.ExpiresAtUnixMillis == 0 || inbound.ExpiresAtUnixMillis > change.ProposedAtUnixMillis {
			result.Delivery.Enqueued, result.Delivery.Saturated, result.Delivery.Skipped, result.TenantQuota = machine.enqueueDeliveryTargetsLocked(change.TenantID, inbound.Topic, publishPayload, change.PublishQoS, inbound.Retain, inbound.ExpiresAtUnixMillis, change.Targets)
		}
		return result
	case DeliveryActionSend:
		if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
			return conflictResult(index, ConflictStaleEpoch)
		}
		record, exists := machine.delivery[key]
		if !exists {
			return conflictResult(index, ConflictDeliveryMissing)
		}
		messageIndex := deliveryMessageBySequence(record.Messages, change.Sequence)
		if messageIndex < 0 {
			return conflictResult(index, ConflictDeliveryMissing)
		}
		message := record.Messages[messageIndex]
		if message.PacketID == change.PacketID {
			return result
		}
		if message.PacketID != 0 {
			return conflictResult(index, ConflictDeliveryPacketID)
		}
		for _, existing := range record.Messages {
			if existing.PacketID == change.PacketID {
				return conflictResult(index, ConflictDeliveryPacketID)
			}
		}
		message.PacketID = change.PacketID
		if message.QoS == 1 {
			message.Phase = DeliveryPhaseAwaitPubAck
		} else {
			message.Phase = DeliveryPhaseAwaitPubRec
		}
		record.Messages[messageIndex] = message
		machine.delivery[key] = record
		return result
	case DeliveryActionAdvance:
		if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
			return conflictResult(index, ConflictStaleEpoch)
		}
		record, exists := machine.delivery[key]
		if !exists {
			return conflictResult(index, ConflictDeliveryMissing)
		}
		for i, message := range record.Messages {
			if message.PacketID == change.PacketID {
				if message.Phase == DeliveryPhaseAwaitPubComp {
					return result
				}
				if message.Phase != DeliveryPhaseAwaitPubRec {
					return conflictResult(index, ConflictDeliveryMissing)
				}
				message.Phase = DeliveryPhaseAwaitPubComp
				record.Messages[i] = message
				machine.delivery[key] = record
				return result
			}
		}
		return conflictResult(index, ConflictDeliveryMissing)
	case DeliveryActionComplete:
		if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
			return conflictResult(index, ConflictStaleEpoch)
		}
		record, exists := machine.delivery[key]
		if !exists {
			return result
		}
		for i, message := range record.Messages {
			if message.PacketID == change.PacketID {
				machine.addDeliveryMessageUsageLocked(key.tenantID, -1, -deliveryMessageBytes(key, message))
				record.Messages = append(record.Messages[:i], record.Messages[i+1:]...)
				machine.delivery[key] = record
				return result
			}
		}
		return result
	case DeliveryActionAttach:
		if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
			return conflictResult(index, ConflictStaleEpoch)
		}
		record, exists := machine.delivery[key]
		if !exists {
			record = DeliverySessionStateDTO{TenantID: change.TenantID, ClientID: change.ClientID, NextSequence: 1}
		} else {
			result.Delivery.Found = !change.Reset
		}
		var rerouted []DeliveryMessageDTO
		if change.Reset {
			if exists {
				rerouted = reroutableSharedMessages(record, change.ProposedAtUnixMillis)
				machine.clearDeliveryEntriesLocked(key, &record)
			}
			machine.removeSubscriptionsLocked(key)
		}
		record.SessionExpiresAtUnixMillis = 0
		machine.delivery[key] = record
		machine.rerouteSharedLocked(key, rerouted, change.OwnerNodeID)
		result.Delivery.Session = new(cloneDeliverySession(record))
		return result
	case DeliveryActionDetach:
		if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
			return conflictResult(index, ConflictStaleEpoch)
		}
		if record, exists := machine.delivery[key]; exists {
			record.SessionExpiresAtUnixMillis = change.SessionExpiresAtUnixMillis
			machine.delivery[key] = record
		}
		return result
	case DeliveryActionDiscard:
		if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
			return conflictResult(index, ConflictStaleEpoch)
		}
		machine.discardSessionDeliveryLocked(key, change.ProposedAtUnixMillis, change.OwnerNodeID)
		return result
	default:
		return conflictResult(index, ConflictNone)
	}
}

func oldBoolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func deliveryMessageBySequence(messages []DeliveryMessageDTO, sequence uint64) int {
	index := sort.Search(len(messages), func(i int) bool { return messages[i].Sequence >= sequence })
	if index == len(messages) || messages[index].Sequence != sequence {
		return -1
	}
	return index
}

func (machine *StateMachine) enqueueDeliveryTargetsLocked(tenantID, topic string, payload []byte, publishQoS uint32, retain bool, expiry uint64, targets []DeliveryTargetDTO) (enqueued []DeliveryEnqueuedDTO, saturated, skipped []string, quotaRejection *TenantQuotaRejectionDTO) {
	for _, target := range targets {
		key := sessionKey{tenantID: tenantID, clientID: target.ClientID}
		qos, eligible := machine.deliveryTargetQoSLocked(key, topic, publishQoS)
		if !eligible {
			skipped = append(skipped, target.ClientID)
			continue
		}
		record, exists := machine.delivery[key]
		if !exists {
			record = DeliverySessionStateDTO{TenantID: tenantID, ClientID: target.ClientID, NextSequence: 1}
			machine.delivery[key] = record
		}
		if record.NextSequence == ^uint64(0) {
			saturated = append(saturated, target.ClientID)
			continue
		}
		sharedFilter := ""
		if target.SharedFilter != "" && machine.sharedOriginLocked(key, target.SharedFilter) {
			sharedFilter = target.SharedFilter
		}
		message := DeliveryMessageDTO{Sequence: record.NextSequence, Topic: topic, Payload: payload, QoS: qos, Retain: target.Retain, ExpiresAtUnixMillis: expiry, Phase: DeliveryPhaseQueued, SharedFilter: sharedFilter}
		messageBytes := deliveryMessageBytes(key, message)
		if len(record.Messages) >= MaxDeliveryMessagesPerSession || machine.deliveryEntries >= MaxDeliveryEntries || machine.deliveryBytes+messageBytes > MaxDeliveryBytes {
			saturated = append(saturated, target.ClientID)
			continue
		}
		if rejection := machine.tenantQuotaExceededLocked(tenantID, 0, 1, messageBytes, messageBytes); rejection != nil {
			if quotaRejection == nil {
				quotaRejection = rejection
			}
			saturated = append(saturated, target.ClientID)
			continue
		}
		message.Payload = bytes.Clone(payload)
		record.NextSequence++
		record.Messages = append(record.Messages, message)
		machine.delivery[key] = record
		machine.addDeliveryMessageUsageLocked(key.tenantID, 1, messageBytes)
		enqueued = append(enqueued, DeliveryEnqueuedDTO{ClientID: target.ClientID, Sequence: message.Sequence})
	}
	return
}

func (machine *StateMachine) deliveryTargetQoSLocked(key sessionKey, topic string, publishQoS uint32) (qos uint32, eligible bool) {
	owner, exists := machine.ownerships[key]
	if !exists {
		return 0, false
	}
	maxQoS := uint32(0)
	for _, subscription := range machine.subscriptions {
		if subscription.TenantID != key.tenantID || subscription.ClientID != key.clientID || subscription.OwnerNodeID != owner.OwnerNodeID || subscription.Epoch != owner.Epoch || subscription.QoS == 0 {
			continue
		}
		_, filter, _, err := routing.ParseSharedFilter(subscription.Filter)
		if err != nil {
			continue
		}
		matched, err := routing.TopicMatchesFilter(topic, filter)
		if err == nil && matched && subscription.QoS > maxQoS {
			maxQoS = subscription.QoS
		}
	}
	if maxQoS == 0 {
		return 0, false
	}
	qos = publishQoS
	if maxQoS < qos {
		qos = maxQoS
	}
	return qos, true
}

func (machine *StateMachine) ensureDeliveryRecordLocked(key sessionKey) {
	if _, exists := machine.delivery[key]; !exists {
		machine.delivery[key] = DeliverySessionStateDTO{TenantID: key.tenantID, ClientID: key.clientID, NextSequence: 1}
	}
}

func (machine *StateMachine) clearDeliveryEntriesLocked(key sessionKey, record *DeliverySessionStateDTO) {
	for _, message := range record.Messages {
		machine.addDeliveryMessageUsageLocked(key.tenantID, -1, -deliveryMessageBytes(key, message))
	}
	for _, inbound := range record.Inbound {
		machine.addDeliveryInboundUsageLocked(key.tenantID, -1, -deliveryInboundBytes(key, inbound))
	}
	record.Messages = nil
	record.Inbound = nil
}

func (machine *StateMachine) removeDeliveryLocked(key sessionKey) {
	record, exists := machine.delivery[key]
	if !exists {
		return
	}
	machine.clearDeliveryEntriesLocked(key, &record)
	delete(machine.delivery, key)
}
func (machine *StateMachine) removeSubscriptionsLocked(key sessionKey) {
	for subscriptionID := range machine.subscriptions {
		if subscriptionID.sessionKey == key {
			delete(machine.subscriptions, subscriptionID)
		}
	}
}

func (machine *StateMachine) purgeExpiredDeliveryLocked(proposedAt uint64) {
	if proposedAt == 0 {
		return
	}
	var expired []sessionKey
	for key, record := range machine.delivery {
		if record.SessionExpiresAtUnixMillis > 0 && record.SessionExpiresAtUnixMillis <= proposedAt {
			expired = append(expired, key)
			continue
		}
		kept := record.Messages[:0]
		for _, message := range record.Messages {
			if message.PacketID == 0 && message.ExpiresAtUnixMillis > 0 && message.ExpiresAtUnixMillis <= proposedAt {
				machine.addDeliveryMessageUsageLocked(key.tenantID, -1, -deliveryMessageBytes(key, message))
				continue
			}
			kept = append(kept, message)
		}
		record.Messages = kept
		machine.delivery[key] = record
	}
	// Map order is not deterministic: remove every expired session first, then re-route in key order.
	sort.Slice(expired, func(i, j int) bool {
		if expired[i].tenantID != expired[j].tenantID {
			return expired[i].tenantID < expired[j].tenantID
		}
		return expired[i].clientID < expired[j].clientID
	})
	rerouted := make([][]DeliveryMessageDTO, len(expired))
	for i, key := range expired {
		rerouted[i] = reroutableSharedMessages(machine.delivery[key], proposedAt)
		machine.removeDeliveryLocked(key)
		machine.removeSubscriptionsLocked(key)
	}
	for i, key := range expired {
		machine.rerouteSharedLocked(key, rerouted[i], 0)
	}
}

func deliveryMessageBytes(key sessionKey, message DeliveryMessageDTO) int {
	return deliveryEntryBytes(key, message.Topic, message.Payload) + len(message.SharedFilter)
}

func deliveryInboundBytes(key sessionKey, inbound DeliveryInboundDTO) int {
	return deliveryEntryBytes(key, inbound.Topic, inbound.Payload)
}

func deliveryEntryBytes(key sessionKey, topic string, payload []byte) int {
	return len(key.tenantID) + len(key.clientID) + len(topic) + len(payload)
}

func cloneDeliverySession(session DeliverySessionStateDTO) DeliverySessionStateDTO {
	session.Messages = append([]DeliveryMessageDTO(nil), session.Messages...)
	for i := range session.Messages {
		session.Messages[i].Payload = bytes.Clone(session.Messages[i].Payload)
	}
	session.Inbound = append([]DeliveryInboundDTO(nil), session.Inbound...)
	for i := range session.Inbound {
		session.Inbound[i].Payload = bytes.Clone(session.Inbound[i].Payload)
	}
	return session
}

func (machine *StateMachine) DeliverySessions() []DeliverySessionStateDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	sessions := make([]DeliverySessionStateDTO, 0, len(machine.delivery))
	for _, record := range machine.delivery {
		sessions = append(sessions, cloneDeliverySession(record))
	}
	sort.Slice(sessions, func(i, j int) bool {
		return compareSession(sessions[i].TenantID, sessions[i].ClientID, sessions[j].TenantID, sessions[j].ClientID) < 0
	})
	return sessions
}

func (machine *StateMachine) DeliverySession(tenantID, clientID string) (DeliverySessionStateDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	record, exists := machine.delivery[sessionKey{tenantID: tenantID, clientID: clientID}]
	return cloneDeliverySession(record), exists
}

func (machine *StateMachine) DeliveryMessage(tenantID, clientID string, sequence uint64) (DeliveryMessageDTO, bool) {
	record, exists := machine.DeliverySession(tenantID, clientID)
	if !exists {
		return DeliveryMessageDTO{}, false
	}
	index := deliveryMessageBySequence(record.Messages, sequence)
	if index < 0 {
		return DeliveryMessageDTO{}, false
	}
	return record.Messages[index], true
}
