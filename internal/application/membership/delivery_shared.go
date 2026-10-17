package membership

import (
	"bytes"
	"slices"
	"sort"

	"narumq/internal/domain/routing"
)

// MaxSharedFilterBytes bounds the shared-subscription origin stored with a delivery message.
const MaxSharedFilterBytes = 65535

type sharedMember struct {
	clientID          string
	ownerNodeID       uint64
	retainAsPublished bool
}

func validSharedFilter(filter string) bool {
	if len(filter) == 0 || len(filter) > MaxSharedFilterBytes {
		return false
	}
	_, _, shared, err := routing.ParseSharedFilter(filter)
	return err == nil && shared
}

// TakeDeliveryReroutes returns and clears the messages that apply moved to another
// shared-subscription member since the previous call. The list is node-local.
func (machine *StateMachine) TakeDeliveryReroutes() []DeliveryRerouteDTO {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	reroutes := machine.deliveryReroutes
	machine.deliveryReroutes = nil
	return reroutes
}

func (machine *StateMachine) sharedRerouteActiveLocked() bool {
	return slices.Contains(machine.activeFeatures, SharedSubscriptionRerouteFeature)
}

// sharedOriginLocked reports whether key currently holds a QoS 1+ subscription to the exact shared filter.
func (machine *StateMachine) sharedOriginLocked(key sessionKey, filter string) bool {
	owner, exists := machine.ownerships[key]
	if !exists {
		return false
	}
	subscription, exists := machine.subscriptions[subscriptionKey{sessionKey: key, filter: filter}]
	return exists && subscription.QoS > 0 && subscription.OwnerNodeID == owner.OwnerNodeID && subscription.Epoch == owner.Epoch
}

// reroutableSharedMessages selects the messages of a record that must survive its removal:
// unexpired shared-origin messages not yet sent, or sent at QoS 1 and not yet acknowledged.
// QoS 2 messages whose PUBLISH was sent are never moved (MQTT-4.8.2-5).
func reroutableSharedMessages(record DeliverySessionStateDTO, now uint64) []DeliveryMessageDTO {
	var messages []DeliveryMessageDTO
	for _, message := range record.Messages {
		if message.SharedFilter == "" || (message.Phase != DeliveryPhaseQueued && message.Phase != DeliveryPhaseAwaitPubAck) {
			continue
		}
		if now > 0 && message.ExpiresAtUnixMillis > 0 && message.ExpiresAtUnixMillis <= now {
			continue
		}
		messages = append(messages, message)
	}
	return messages
}

// discardSessionDeliveryLocked removes a session's delivery record and subscriptions and moves
// its shared-origin messages to the remaining members of their groups. preferOwner is the node
// that proposed the command (0 when unknown); its members are tried first because that node is alive.
func (machine *StateMachine) discardSessionDeliveryLocked(key sessionKey, now, preferOwner uint64) {
	var messages []DeliveryMessageDTO
	if record, exists := machine.delivery[key]; exists {
		messages = reroutableSharedMessages(record, now)
	}
	machine.removeDeliveryLocked(key)
	machine.removeSubscriptionsLocked(key)
	machine.rerouteSharedLocked(key, messages, preferOwner)
}

func (machine *StateMachine) sharedMembersLocked(origin sessionKey, filter string) []sharedMember {
	var members []sharedMember
	for id, subscription := range machine.subscriptions {
		if id.tenantID != origin.tenantID || id.filter != filter || id.clientID == origin.clientID || subscription.QoS == 0 {
			continue
		}
		owner, exists := machine.ownerships[id.sessionKey]
		if !exists || owner.OwnerNodeID != subscription.OwnerNodeID || owner.Epoch != subscription.Epoch {
			continue
		}
		members = append(members, sharedMember{clientID: id.clientID, ownerNodeID: owner.OwnerNodeID, retainAsPublished: subscription.RetainAsPublished})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].clientID < members[j].clientID })
	return members
}

// rerouteSharedLocked queues each message on another member of its shared group. Members are
// ordered by client ID and taken round-robin starting after the origin. Members owned by
// preferOwner come first. A member that cannot take the message because of a delivery limit is
// skipped. A message nobody can take is dropped.
func (machine *StateMachine) rerouteSharedLocked(origin sessionKey, messages []DeliveryMessageDTO, preferOwner uint64) {
	if len(messages) == 0 {
		return
	}
	members := make(map[string][]sharedMember)
	cursors := make(map[string]int)
	for _, message := range messages {
		group, loaded := members[message.SharedFilter]
		if !loaded {
			group = machine.sharedMembersLocked(origin, message.SharedFilter)
			members[message.SharedFilter] = group
			cursors[message.SharedFilter] = sort.Search(len(group), func(i int) bool { return group[i].clientID > origin.clientID })
		}
	placed:
		for _, preferredPass := range []bool{true, false} {
			for step := range len(group) {
				i := (cursors[message.SharedFilter] + step) % len(group)
				if (preferOwner != 0 && group[i].ownerNodeID == preferOwner) != preferredPass {
					continue
				}
				if machine.enqueueRerouteLocked(origin.tenantID, group[i], message) {
					cursors[message.SharedFilter] = (i + 1) % len(group)
					break placed
				}
			}
		}
	}
}

func (machine *StateMachine) enqueueRerouteLocked(tenantID string, member sharedMember, source DeliveryMessageDTO) bool {
	key := sessionKey{tenantID: tenantID, clientID: member.clientID}
	qos, eligible := machine.deliveryTargetQoSLocked(key, source.Topic, source.QoS)
	if !eligible {
		return false
	}
	record, exists := machine.delivery[key]
	if !exists {
		record = DeliverySessionStateDTO{TenantID: tenantID, ClientID: member.clientID, NextSequence: 1}
	}
	if record.NextSequence == ^uint64(0) || len(record.Messages) >= MaxDeliveryMessagesPerSession {
		return false
	}
	message := DeliveryMessageDTO{
		Sequence: record.NextSequence, Topic: source.Topic, Payload: source.Payload, QoS: qos,
		Retain: source.Retain && member.retainAsPublished, ExpiresAtUnixMillis: source.ExpiresAtUnixMillis,
		Phase: DeliveryPhaseQueued, SharedFilter: source.SharedFilter,
	}
	messageBytes := deliveryMessageBytes(key, message)
	if machine.deliveryEntries >= MaxDeliveryEntries || machine.deliveryBytes+messageBytes > MaxDeliveryBytes {
		return false
	}
	if machine.tenantQuotaExceededLocked(tenantID, 0, 1, messageBytes, messageBytes) != nil {
		return false
	}
	message.Payload = bytes.Clone(source.Payload)
	record.NextSequence++
	record.Messages = append(record.Messages, message)
	machine.delivery[key] = record
	machine.addDeliveryMessageUsageLocked(tenantID, 1, messageBytes)
	machine.deliveryReroutes = append(machine.deliveryReroutes, DeliveryRerouteDTO{TenantID: tenantID, ClientID: member.clientID, Sequence: message.Sequence})
	return true
}
