package mqtt

// Publish is the validated domain representation of a QoS 0 PUBLISH packet.
type Publish struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// DUP is the fixed-header duplicate-delivery flag and must be false for QoS 0.
	DUP bool
	// Retain is the fixed-header retained-message flag.
	Retain bool
	// Topic is the non-empty UTF-8 topic name.
	Topic string
	// Payload is the binary application payload.
	Payload []byte
}

func (p Publish) packetType() PacketType {
	return PacketTypePublish
}

func (p Publish) packetVersion() ProtocolVersion {
	return p.Version
}

func decodePublish(body []byte, version ProtocolVersion, flags byte) (Packet, error) {
	position := 0
	topic, err := readUTF8Field(body, &position, false)
	if err != nil || !validMQTTTopicName(topic) {
		return nil, ErrMalformedPacket
	}
	if version == ProtocolV5 {
		if err := skipProperties(body, &position); err != nil {
			return nil, err
		}
	}
	return &Publish{
		Version: version,
		DUP:     flags&0x08 != 0,
		Retain:  flags&0x01 != 0,
		Topic:   topic,
		Payload: cloneBytes(body[position:]),
	}, nil
}

func encodePublish(publish Publish, maxPacketSize int) ([]byte, error) {
	if publish.Version != ProtocolV311 && publish.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if publish.DUP {
		return nil, ErrMalformedPacket
	}
	if !validMQTTTopicName(publish.Topic) || len(publish.Topic) > 65535 {
		return nil, ErrMalformedPacket
	}
	bodySize := 2 + len(publish.Topic) + len(publish.Payload)
	if publish.Version == ProtocolV5 {
		bodySize++
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}
	body := make([]byte, 0, bodySize)
	body = appendUTF8Field(body, publish.Topic)
	if publish.Version == ProtocolV5 {
		body = append(body, 0)
	}
	body = append(body, publish.Payload...)
	return body, nil
}
