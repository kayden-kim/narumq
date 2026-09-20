package mqtt

import "encoding/binary"

// Publish is the validated domain representation of an MQTT PUBLISH packet.
type Publish struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// DUP is the fixed-header duplicate-delivery flag.
	DUP bool
	// QoS is the requested MQTT delivery guarantee.
	QoS QoS
	// Retain is the fixed-header retained-message flag.
	Retain bool
	// PacketID identifies the QoS 1 or QoS 2 exchange.
	PacketID PacketIdentifier
	// Topic is the non-empty UTF-8 topic name.
	Topic string
	// Payload is the binary application payload.
	Payload []byte
	// HasMessageExpiryInterval distinguishes an absent property from zero.
	HasMessageExpiryInterval bool
	// MessageExpiryInterval is the MQTT 5 lifetime in seconds.
	MessageExpiryInterval uint32
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
	qos := QoS((flags >> 1) & 0x03)
	var packetID PacketIdentifier
	if qos != QoSAtMostOnce {
		if len(body)-position < 2 {
			return nil, ErrMalformedPacket
		}
		packetID = PacketIdentifier(binary.BigEndian.Uint16(body[position : position+2]))
		position += 2
		if packetID == 0 {
			return nil, ErrMalformedPacket
		}
	}
	var (
		hasMessageExpiryInterval bool
		messageExpiryInterval    uint32
	)
	if version == ProtocolV5 {
		propertyLength, consumed, err := decodeVariableByteInteger(body[position:])
		if err != nil {
			return nil, ErrMalformedPacket
		}
		position += consumed
		if propertyLength > len(body)-position {
			return nil, ErrMalformedPacket
		}
		propertyEnd := position + propertyLength
		for position < propertyEnd {
			propertyIdentifier := body[position]
			position++
			switch propertyIdentifier {
			case 0x02:
				if hasMessageExpiryInterval || propertyEnd-position < 4 {
					return nil, ErrMalformedPacket
				}
				messageExpiryInterval = binary.BigEndian.Uint32(body[position : position+4])
				position += 4
				hasMessageExpiryInterval = true
			default:
				return nil, ErrUnsupportedProperty
			}
		}
	}
	return &Publish{
		Version:                  version,
		DUP:                      flags&0x08 != 0,
		QoS:                      qos,
		Retain:                   flags&0x01 != 0,
		PacketID:                 packetID,
		Topic:                    topic,
		Payload:                  cloneBytes(body[position:]),
		HasMessageExpiryInterval: hasMessageExpiryInterval,
		MessageExpiryInterval:    messageExpiryInterval,
	}, nil
}

func encodePublish(publish Publish, maxPacketSize int) ([]byte, error) {
	if publish.Version != ProtocolV311 && publish.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if publish.QoS > QoSExactlyOnce {
		return nil, ErrMalformedPacket
	}
	if publish.QoS == QoSAtMostOnce {
		if publish.DUP || publish.PacketID != 0 {
			return nil, ErrMalformedPacket
		}
	} else if publish.PacketID == 0 {
		return nil, ErrMalformedPacket
	}
	if !validMQTTTopicName(publish.Topic) || len(publish.Topic) > 65535 {
		return nil, ErrMalformedPacket
	}
	if publish.Version == ProtocolV311 && publish.HasMessageExpiryInterval {
		return nil, ErrMalformedPacket
	}
	bodySize := 2 + len(publish.Topic) + len(publish.Payload)
	if publish.QoS != QoSAtMostOnce {
		bodySize += 2
	}
	if publish.Version == ProtocolV5 {
		bodySize++
		if publish.HasMessageExpiryInterval {
			bodySize += 5
		}
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}
	body := make([]byte, 0, bodySize)
	body = appendUTF8Field(body, publish.Topic)
	if publish.QoS != QoSAtMostOnce {
		var packetID [2]byte
		binary.BigEndian.PutUint16(packetID[:], uint16(publish.PacketID))
		body = append(body, packetID[:]...)
	}
	if publish.Version == ProtocolV5 {
		if !publish.HasMessageExpiryInterval {
			body = append(body, 0)
		} else {
			body = append(body, 5, 0x02)
			var interval [4]byte
			binary.BigEndian.PutUint32(interval[:], publish.MessageExpiryInterval)
			body = append(body, interval[:]...)
		}
	}
	body = append(body, publish.Payload...)
	return body, nil
}
