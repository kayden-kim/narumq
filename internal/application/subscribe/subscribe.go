// Package subscribe orchestrates ordered MQTT subscription acknowledgements and
// mutates the domain routing table without owning wire or adapter details.
package subscribe

import (
	"errors"
	"sort"
	"time"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

var (
	// ErrNilRoutingTable means that a use case was constructed without routing.
	ErrNilRoutingTable = errors.New("subscribe routing table is nil")
	// ErrNilDeliveryQueue means that retained replay has no queue port.
	ErrNilDeliveryQueue = errors.New("subscribe delivery queue is nil")
	// ErrNilRetainedRepository means that retained replay has no state port.
	ErrNilRetainedRepository = errors.New("subscribe retained repository is nil")
)

// ResultDTO reports the ordered SUBACK and the number of retained deliveries
// committed to the queue for this SUBSCRIBE request.
type ResultDTO struct {
	Ack            mqtt.SubAck
	RetainedQueued int
}

// UseCase applies subscription commands to one routing table and replays
// retained values through the application-owned delivery queue.
type UseCase struct {
	routes   *routing.Table
	queue    deliveryqueue.Queue
	retained retained.Repository
}

// New creates a subscription use case over routes, queue, and retained state.
func New(routes *routing.Table, queue deliveryqueue.Queue, retainedRepository retained.Repository) (*UseCase, error) {
	if routes == nil {
		return nil, ErrNilRoutingTable
	}
	if queue == nil {
		return nil, ErrNilDeliveryQueue
	}
	if retainedRepository == nil {
		return nil, ErrNilRetainedRepository
	}
	return &UseCase{routes: routes, queue: queue, retained: retainedRepository}, nil
}

// Subscribe validates and applies every entry independently. Retained replay
// is enqueued as a complete batch before the corresponding route is committed.
func (useCase *UseCase) Subscribe(sessionID routing.SessionID, packet mqtt.Subscribe, now time.Time) (ResultDTO, error) {
	result := ResultDTO{
		Ack: mqtt.SubAck{Version: packet.Version, PacketID: packet.PacketID, ReasonCodes: make([]byte, 0, len(packet.Subscriptions))},
	}
	for _, subscription := range packet.Subscriptions {
		reasonCode, retainedQueued, err := useCase.subscribeOne(sessionID, packet.Version, subscription, now)
		if err != nil {
			return result, err
		}
		result.Ack.ReasonCodes = append(result.Ack.ReasonCodes, reasonCode)
		result.RetainedQueued += retainedQueued
	}
	return result, nil
}

// Unsubscribe removes every valid filter independently. MQTT 5 receives one
// reason per request; MQTT 3.1.1 keeps its packet-identifier-only contract.
func (useCase *UseCase) Unsubscribe(sessionID routing.SessionID, packet mqtt.Unsubscribe) mqtt.UnsubAck {
	ack := mqtt.UnsubAck{Version: packet.Version, PacketID: packet.PacketID}
	if packet.Version == mqtt.ProtocolV311 {
		for _, filter := range packet.Filters {
			if !validUnsubscribeFilter(filter) {
				continue
			}
			_, _ = useCase.routes.Unsubscribe(sessionID, filter)
		}
		return ack
	}

	ack.ReasonCodes = make([]byte, 0, len(packet.Filters))
	for _, filter := range packet.Filters {
		ack.ReasonCodes = append(ack.ReasonCodes, useCase.unsubscribeReason(sessionID, filter))
	}
	return ack
}

func (useCase *UseCase) subscribeOne(sessionID routing.SessionID, version mqtt.ProtocolVersion, subscription mqtt.Subscription, now time.Time) (byte, int, error) {
	if err := routing.ValidateSessionID(sessionID); err != nil {
		return invalidSubscriptionReason(version), 0, nil
	}
	if err := routing.ValidateTopicFilter(subscription.Filter); err != nil {
		return invalidSubscriptionReason(version), 0, nil
	}
	if subscription.QoS > mqtt.QoSExactlyOnce || subscription.RetainHandling > mqtt.DoNotSendRetained {
		return invalidSubscriptionReason(version), 0, nil
	}
	if version == mqtt.ProtocolV311 && (subscription.NoLocal || subscription.RetainAsPublished || subscription.RetainHandling != mqtt.SendRetainedAtSubscribe) {
		return invalidSubscriptionReason(version), 0, nil
	}
	isShared := routing.IsSharedFilter(subscription.Filter)
	if isShared && subscription.NoLocal {
		return protocolErrorReason(version), 0, nil
	}
	_, existed := useCase.routes.Lookup(sessionID, subscription.Filter)
	replay := version == mqtt.ProtocolV311
	switch subscription.RetainHandling {
	case mqtt.SendRetainedAtSubscribe:
		replay = true
	case mqtt.SendRetainedIfNew:
		replay = !existed
	case mqtt.DoNotSendRetained:
		replay = false
	}
	if isShared {
		replay = false
	}
	retainedQueued := 0
	if replay {
		batch, err := useCase.retainedBatch(sessionID, subscription, now)
		if err != nil {
			return 0, 0, err
		}
		if err := useCase.queue.TryEnqueueBatch(batch, now); err != nil {
			if errors.Is(err, deliveryqueue.ErrFull) {
				return replayFullReason(version), 0, nil
			}
			return 0, 0, err
		}
		retainedQueued = len(batch)
	}
	if _, err := useCase.routes.Subscribe(sessionID, subscription); err != nil {
		return invalidSubscriptionReason(version), 0, nil
	}
	return byte(subscription.QoS), retainedQueued, nil
}

func (useCase *UseCase) retainedBatch(sessionID routing.SessionID, subscription mqtt.Subscription, now time.Time) ([]deliveryqueue.DeliveryDTO, error) {
	messages, err := useCase.retained.List(now)
	if err != nil {
		return nil, err
	}
	batch := make([]deliveryqueue.DeliveryDTO, 0, len(messages))
	for _, message := range messages {
		matched, err := routing.TopicMatchesFilter(message.Topic, subscription.Filter)
		if err != nil {
			return nil, err
		}
		if !matched || (subscription.NoLocal && message.PublisherSessionID == sessionID) {
			continue
		}
		qos := message.QoS
		if subscription.QoS < qos {
			qos = subscription.QoS
		}
		batch = append(batch, deliveryqueue.DeliveryDTO{
			SessionID: sessionID,
			Topic:     message.Topic,
			Payload:   message.Payload,
			QoS:       qos,
			Retain:    true,
			ExpiresAt: message.ExpiresAt,
		})
	}
	sort.Slice(batch, func(left, right int) bool {
		return batch[left].Topic < batch[right].Topic
	})
	return batch, nil
}

func (useCase *UseCase) unsubscribeReason(sessionID routing.SessionID, filter string) byte {
	if err := routing.ValidateTopicFilter(filter); err != nil {
		return 0x8F
	}
	removed, err := useCase.routes.Unsubscribe(sessionID, filter)
	if err != nil {
		return 0x8F
	}
	if !removed {
		return 0x11
	}
	return 0x00
}

func validUnsubscribeFilter(filter string) bool {
	return routing.ValidateTopicFilter(filter) == nil
}

func invalidSubscriptionReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x8F
	}
	return 0x80
}

func replayFullReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x97
	}
	return 0x80
}

func protocolErrorReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x82
	}
	return 0x80
}
