package mqtt

import "encoding/binary"

// RetainHandling controls when a broker sends retained messages for a new
// subscription. The current routing slice preserves this value but does not
// apply retained-message runtime behavior.
type RetainHandling byte

const (
	// SendRetainedAtSubscribe requests retained messages at every subscribe.
	SendRetainedAtSubscribe RetainHandling = iota
	// SendRetainedIfNew requests retained messages only for a new subscription.
	SendRetainedIfNew
	// DoNotSendRetained disables retained messages for this subscription.
	DoNotSendRetained
)

// Subscription is one topic filter and its MQTT 5 subscription options.
type Subscription struct {
	// Filter is the UTF-8 topic filter requested by the client.
	Filter string
	// QoS is the maximum QoS granted for this subscription.
	QoS QoS
	// NoLocal prevents delivery of publications from the same client.
	NoLocal bool
	// RetainAsPublished preserves the incoming PUBLISH retain flag.
	RetainAsPublished bool
	// RetainHandling controls retained-message delivery at subscription time.
	RetainHandling RetainHandling
}

// Subscribe requests one or more topic subscriptions.
type Subscribe struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// PacketID identifies this subscribe request.
	PacketID PacketIdentifier
	// Subscriptions preserves the request order for SUBACK reasons.
	Subscriptions []Subscription
}

func (p Subscribe) packetType() PacketType {
	return PacketTypeSubscribe
}

func (p Subscribe) packetVersion() ProtocolVersion {
	return p.Version
}

// SubAck reports one reason code for every requested subscription.
type SubAck struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// PacketID identifies the subscribe request.
	PacketID PacketIdentifier
	// ReasonCodes preserves the request order.
	ReasonCodes []byte
}

func (p SubAck) packetType() PacketType {
	return PacketTypeSubAck
}

func (p SubAck) packetVersion() ProtocolVersion {
	return p.Version
}

// Unsubscribe removes one or more topic filters.
type Unsubscribe struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// PacketID identifies this unsubscribe request.
	PacketID PacketIdentifier
	// Filters preserves the request order.
	Filters []string
}

func (p Unsubscribe) packetType() PacketType {
	return PacketTypeUnsubscribe
}

func (p Unsubscribe) packetVersion() ProtocolVersion {
	return p.Version
}

// UnsubAck reports one reason code for every requested filter in MQTT 5.
type UnsubAck struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// PacketID identifies the unsubscribe request.
	PacketID PacketIdentifier
	// ReasonCodes is empty for MQTT 3.1.1 and ordered for MQTT 5.
	ReasonCodes []byte
}

func (p UnsubAck) packetType() PacketType {
	return PacketTypeUnsubAck
}

func (p UnsubAck) packetVersion() ProtocolVersion {
	return p.Version
}

func decodeSubscribe(body []byte, version ProtocolVersion) (Packet, error) {
	if version != ProtocolV311 && version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if len(body) < 2 {
		return nil, ErrMalformedPacket
	}
	packetID := PacketIdentifier(binary.BigEndian.Uint16(body[:2]))
	if packetID == 0 {
		return nil, ErrMalformedPacket
	}
	position := 2
	if version == ProtocolV5 {
		if err := skipProperties(body, &position); err != nil {
			return nil, err
		}
	}
	if position == len(body) {
		return nil, ErrMalformedPacket
	}

	subscriptions := make([]Subscription, 0, 1)
	for position < len(body) {
		filter, err := readUTF8Field(body, &position, false)
		if err != nil {
			return nil, ErrMalformedPacket
		}
		if position >= len(body) {
			return nil, ErrMalformedPacket
		}
		options := body[position]
		position++
		subscription, err := decodeSubscriptionOptions(filter, options, version)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, subscription)
	}
	return &Subscribe{Version: version, PacketID: packetID, Subscriptions: subscriptions}, nil
}

func decodeSubscriptionOptions(filter string, options byte, version ProtocolVersion) (Subscription, error) {
	if options&0xC0 != 0 {
		return Subscription{}, ErrMalformedPacket
	}
	qos := QoS(options & 0x03)
	if qos > QoSExactlyOnce {
		return Subscription{}, ErrMalformedPacket
	}
	subscription := Subscription{Filter: filter, QoS: qos}
	if version == ProtocolV311 {
		if options&0xFC != 0 {
			return Subscription{}, ErrMalformedPacket
		}
		return subscription, nil
	}
	if version != ProtocolV5 {
		return Subscription{}, ErrUnsupportedProtocolVersion
	}
	subscription.NoLocal = options&0x04 != 0
	subscription.RetainAsPublished = options&0x08 != 0
	subscription.RetainHandling = RetainHandling((options >> 4) & 0x03)
	if subscription.RetainHandling > DoNotSendRetained {
		return Subscription{}, ErrMalformedPacket
	}
	return subscription, nil
}

func encodeSubscribe(packet Subscribe, maxPacketSize int) ([]byte, error) {
	if packet.Version != ProtocolV311 && packet.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if packet.PacketID == 0 || len(packet.Subscriptions) == 0 {
		return nil, ErrMalformedPacket
	}
	bodySize := 2
	if packet.Version == ProtocolV5 {
		bodySize++
	}
	for _, subscription := range packet.Subscriptions {
		if !validSubscriptionFilter(subscription.Filter) {
			return nil, ErrMalformedPacket
		}
		if _, err := subscriptionOptions(subscription, packet.Version); err != nil {
			return nil, err
		}
		bodySize += 2 + len(subscription.Filter) + 1
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}
	body := make([]byte, 0, bodySize)
	var packetID [2]byte
	binary.BigEndian.PutUint16(packetID[:], uint16(packet.PacketID))
	body = append(body, packetID[:]...)
	if packet.Version == ProtocolV5 {
		body = append(body, 0)
	}
	for _, subscription := range packet.Subscriptions {
		body = appendUTF8Field(body, subscription.Filter)
		options, _ := subscriptionOptions(subscription, packet.Version)
		body = append(body, options)
	}
	return body, nil
}

func subscriptionOptions(subscription Subscription, version ProtocolVersion) (byte, error) {
	if subscription.QoS > QoSExactlyOnce {
		return 0, ErrMalformedPacket
	}
	if version == ProtocolV311 {
		if subscription.NoLocal || subscription.RetainAsPublished || subscription.RetainHandling != SendRetainedAtSubscribe {
			return 0, ErrMalformedPacket
		}
		return byte(subscription.QoS), nil
	}
	if version != ProtocolV5 {
		return 0, ErrUnsupportedProtocolVersion
	}
	if subscription.RetainHandling > DoNotSendRetained {
		return 0, ErrMalformedPacket
	}
	options := byte(subscription.QoS)
	if subscription.NoLocal {
		options |= 0x04
	}
	if subscription.RetainAsPublished {
		options |= 0x08
	}
	options |= byte(subscription.RetainHandling) << 4
	return options, nil
}

func decodeSubAck(body []byte, version ProtocolVersion) (Packet, error) {
	if version != ProtocolV311 && version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if len(body) < 2 {
		return nil, ErrMalformedPacket
	}
	packetID := PacketIdentifier(binary.BigEndian.Uint16(body[:2]))
	if packetID == 0 {
		return nil, ErrMalformedPacket
	}
	position := 2
	if version == ProtocolV5 {
		if err := skipProperties(body, &position); err != nil {
			return nil, err
		}
	}
	if position == len(body) {
		return nil, ErrMalformedPacket
	}
	reasonCodes := cloneBytes(body[position:])
	for _, reasonCode := range reasonCodes {
		if !validSubscribeAckReason(version, reasonCode) {
			return nil, ErrMalformedPacket
		}
	}
	return &SubAck{Version: version, PacketID: packetID, ReasonCodes: reasonCodes}, nil
}

func encodeSubAck(packet SubAck, maxPacketSize int) ([]byte, error) {
	if packet.Version != ProtocolV311 && packet.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if packet.PacketID == 0 || len(packet.ReasonCodes) == 0 {
		return nil, ErrMalformedPacket
	}
	for _, reasonCode := range packet.ReasonCodes {
		if !validSubscribeAckReason(packet.Version, reasonCode) {
			return nil, ErrMalformedPacket
		}
	}
	bodySize := 2 + len(packet.ReasonCodes)
	if packet.Version == ProtocolV5 {
		bodySize++
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}
	body := make([]byte, 0, bodySize)
	var packetID [2]byte
	binary.BigEndian.PutUint16(packetID[:], uint16(packet.PacketID))
	body = append(body, packetID[:]...)
	if packet.Version == ProtocolV5 {
		body = append(body, 0)
	}
	return append(body, packet.ReasonCodes...), nil
}

func validSubscribeAckReason(version ProtocolVersion, reasonCode byte) bool {
	if version == ProtocolV311 {
		return reasonCode == 0x00 || reasonCode == 0x01 || reasonCode == 0x02 || reasonCode == 0x80
	}
	if version != ProtocolV5 {
		return false
	}
	switch reasonCode {
	case 0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2:
		return true
	default:
		return false
	}
}

func decodeUnsubscribe(body []byte, version ProtocolVersion) (Packet, error) {
	if version != ProtocolV311 && version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if len(body) < 2 {
		return nil, ErrMalformedPacket
	}
	packetID := PacketIdentifier(binary.BigEndian.Uint16(body[:2]))
	if packetID == 0 {
		return nil, ErrMalformedPacket
	}
	position := 2
	if version == ProtocolV5 {
		if err := skipProperties(body, &position); err != nil {
			return nil, err
		}
	}
	if position == len(body) {
		return nil, ErrMalformedPacket
	}
	filters := make([]string, 0, 1)
	for position < len(body) {
		filter, err := readUTF8Field(body, &position, false)
		if err != nil {
			return nil, ErrMalformedPacket
		}
		filters = append(filters, filter)
	}
	return &Unsubscribe{Version: version, PacketID: packetID, Filters: filters}, nil
}

func encodeUnsubscribe(packet Unsubscribe, maxPacketSize int) ([]byte, error) {
	if packet.Version != ProtocolV311 && packet.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if packet.PacketID == 0 || len(packet.Filters) == 0 {
		return nil, ErrMalformedPacket
	}
	bodySize := 2
	if packet.Version == ProtocolV5 {
		bodySize++
	}
	for _, filter := range packet.Filters {
		if !validSubscriptionFilter(filter) {
			return nil, ErrMalformedPacket
		}
		bodySize += 2 + len(filter)
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}
	body := make([]byte, 0, bodySize)
	var packetID [2]byte
	binary.BigEndian.PutUint16(packetID[:], uint16(packet.PacketID))
	body = append(body, packetID[:]...)
	if packet.Version == ProtocolV5 {
		body = append(body, 0)
	}
	for _, filter := range packet.Filters {
		body = appendUTF8Field(body, filter)
	}
	return body, nil
}

func decodeUnsubAck(body []byte, version ProtocolVersion) (Packet, error) {
	if version != ProtocolV311 && version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if len(body) < 2 {
		return nil, ErrMalformedPacket
	}
	packetID := PacketIdentifier(binary.BigEndian.Uint16(body[:2]))
	if packetID == 0 {
		return nil, ErrMalformedPacket
	}
	if version == ProtocolV311 {
		if len(body) != 2 {
			return nil, ErrMalformedPacket
		}
		return &UnsubAck{Version: version, PacketID: packetID}, nil
	}
	position := 2
	if err := skipProperties(body, &position); err != nil {
		return nil, err
	}
	if position == len(body) {
		return nil, ErrMalformedPacket
	}
	reasonCodes := cloneBytes(body[position:])
	for _, reasonCode := range reasonCodes {
		if !validUnsubAckReason(reasonCode) {
			return nil, ErrMalformedPacket
		}
	}
	return &UnsubAck{Version: version, PacketID: packetID, ReasonCodes: reasonCodes}, nil
}

func encodeUnsubAck(packet UnsubAck, maxPacketSize int) ([]byte, error) {
	if packet.Version != ProtocolV311 && packet.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if packet.PacketID == 0 {
		return nil, ErrMalformedPacket
	}
	if packet.Version == ProtocolV311 && len(packet.ReasonCodes) != 0 {
		return nil, ErrMalformedPacket
	}
	if packet.Version == ProtocolV5 {
		if len(packet.ReasonCodes) == 0 {
			return nil, ErrMalformedPacket
		}
		for _, reasonCode := range packet.ReasonCodes {
			if !validUnsubAckReason(reasonCode) {
				return nil, ErrMalformedPacket
			}
		}
	}
	bodySize := 2
	if packet.Version == ProtocolV5 {
		bodySize += 1 + len(packet.ReasonCodes)
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}
	body := make([]byte, 0, bodySize)
	var packetID [2]byte
	binary.BigEndian.PutUint16(packetID[:], uint16(packet.PacketID))
	body = append(body, packetID[:]...)
	if packet.Version == ProtocolV5 {
		body = append(body, 0)
		body = append(body, packet.ReasonCodes...)
	}
	return body, nil
}

func validUnsubAckReason(reasonCode byte) bool {
	switch reasonCode {
	case 0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91:
		return true
	default:
		return false
	}
}

func validSubscriptionFilter(filter string) bool {
	return len(filter) <= 65535 && len(filter) > 0 && validMQTTUTF8(filter)
}
