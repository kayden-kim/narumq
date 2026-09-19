// Package subscribe orchestrates ordered MQTT subscription acknowledgements and
// mutates the domain routing table without owning wire or adapter details.
package subscribe

import (
	"errors"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

var (
	// ErrNilRoutingTable means that a use case was constructed without routing.
	ErrNilRoutingTable = errors.New("subscribe routing table is nil")
)

// UseCase applies subscription commands to one routing table.
type UseCase struct {
	routes *routing.Table
}

// New creates a subscription use case over routes.
func New(routes *routing.Table) (*UseCase, error) {
	if routes == nil {
		return nil, ErrNilRoutingTable
	}
	return &UseCase{routes: routes}, nil
}

// Subscribe validates and applies every entry independently. Returned reason
// codes retain request order even when one entry fails.
func (useCase *UseCase) Subscribe(sessionID routing.SessionID, packet mqtt.Subscribe) mqtt.SubAck {
	ack := mqtt.SubAck{Version: packet.Version, PacketID: packet.PacketID}
	ack.ReasonCodes = make([]byte, 0, len(packet.Subscriptions))
	for _, subscription := range packet.Subscriptions {
		reasonCode := useCase.subscribeReason(sessionID, packet.Version, subscription)
		ack.ReasonCodes = append(ack.ReasonCodes, reasonCode)
	}
	return ack
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

func (useCase *UseCase) subscribeReason(sessionID routing.SessionID, version mqtt.ProtocolVersion, subscription mqtt.Subscription) byte {
	if err := routing.ValidateTopicFilter(subscription.Filter); err != nil {
		if errors.Is(err, routing.ErrSharedSubscriptionUnsupported) {
			return sharedSubscriptionReason(version)
		}
		return invalidSubscriptionReason(version)
	}
	if hasUnsupportedOptions(subscription) {
		if version == mqtt.ProtocolV5 {
			return 0x83
		}
		return 0x80
	}
	if err := useCase.routes.Subscribe(sessionID, subscription.Filter, subscription.QoS); err != nil {
		if errors.Is(err, routing.ErrSharedSubscriptionUnsupported) {
			return sharedSubscriptionReason(version)
		}
		return invalidSubscriptionReason(version)
	}
	return byte(subscription.QoS)
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

func hasUnsupportedOptions(subscription mqtt.Subscription) bool {
	return subscription.NoLocal || subscription.RetainAsPublished || subscription.RetainHandling != mqtt.SendRetainedAtSubscribe
}

func invalidSubscriptionReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x8F
	}
	return 0x80
}

func sharedSubscriptionReason(version mqtt.ProtocolVersion) byte {
	if version == mqtt.ProtocolV5 {
		return 0x9E
	}
	return 0x80
}
