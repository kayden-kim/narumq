package mqtt

import "encoding/binary"

// ConnAck is the validated domain representation of an MQTT CONNACK packet.
type ConnAck struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// SessionPresent reports whether an existing session was resumed.
	SessionPresent bool
	// ReasonCode is the MQTT version-specific connection result.
	ReasonCode byte
	// ReceiveMaximum is the server's inbound QoS 1/2 limit; zero omits the property.
	ReceiveMaximum uint16
	// MaximumPacketSize is the server's total packet byte limit; zero omits it.
	MaximumPacketSize uint32
}

func (c ConnAck) packetType() PacketType {
	return PacketTypeConnAck
}

func (c ConnAck) packetVersion() ProtocolVersion {
	return c.Version
}

func decodeConnAck(body []byte, version ProtocolVersion) (Packet, error) {
	if version == ProtocolV311 {
		if len(body) != 2 {
			return nil, ErrMalformedPacket
		}
		if body[0]&0xFE != 0 || !validConnAckReasonV311(body[1]) {
			return nil, ErrMalformedPacket
		}
		if body[1] != 0 && body[0]&0x01 != 0 {
			return nil, ErrMalformedPacket
		}
		return &ConnAck{
			Version:        version,
			SessionPresent: body[0]&0x01 != 0,
			ReasonCode:     body[1],
		}, nil
	}
	if version != ProtocolV5 || len(body) < 3 {
		return nil, ErrMalformedPacket
	}
	if body[0]&0xFE != 0 || !validConnAckReasonV5(body[1]) {
		return nil, ErrMalformedPacket
	}
	if body[1] != 0 && body[0]&0x01 != 0 {
		return nil, ErrMalformedPacket
	}
	position := 2
	receiveMaximum, maximumPacketSize, err := decodeConnAckProperties(body, &position)
	if err != nil {
		return nil, err
	}
	if position != len(body) {
		return nil, ErrMalformedPacket
	}
	return &ConnAck{
		Version:           version,
		SessionPresent:    body[0]&0x01 != 0,
		ReasonCode:        body[1],
		ReceiveMaximum:    receiveMaximum,
		MaximumPacketSize: maximumPacketSize,
	}, nil
}

func encodeConnAck(connAck ConnAck, maxPacketSize int) ([]byte, error) {
	if connAck.Version != ProtocolV311 && connAck.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if !validConnAckReason(connAck.Version, connAck.ReasonCode) || (connAck.ReasonCode != 0 && connAck.SessionPresent) {
		return nil, ErrMalformedPacket
	}
	if connAck.Version == ProtocolV311 && (connAck.ReceiveMaximum != 0 || connAck.MaximumPacketSize != 0) {
		return nil, ErrMalformedPacket
	}
	bodySize := 2
	if connAck.Version == ProtocolV5 {
		bodySize++
		if connAck.ReceiveMaximum != 0 {
			bodySize += 3
		}
		if connAck.MaximumPacketSize != 0 {
			bodySize += 5
		}
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}
	flags := byte(0)
	if connAck.SessionPresent {
		flags = 1
	}
	body := make([]byte, 0, bodySize)
	body = append(body, flags, connAck.ReasonCode)
	if connAck.Version == ProtocolV5 {
		body = append(body, byte(bodySize-3))
		if connAck.ReceiveMaximum != 0 {
			body = append(body, 0x21)
			body = binary.BigEndian.AppendUint16(body, connAck.ReceiveMaximum)
		}
		if connAck.MaximumPacketSize != 0 {
			body = append(body, 0x27)
			body = binary.BigEndian.AppendUint32(body, connAck.MaximumPacketSize)
		}
	}
	return body, nil
}

func decodeConnAckProperties(body []byte, position *int) (uint16, uint32, error) {
	length, consumed, err := decodeVariableByteInteger(body[*position:])
	if err != nil {
		return 0, 0, ErrMalformedPacket
	}
	*position += consumed
	if length > len(body)-*position {
		return 0, 0, ErrMalformedPacket
	}
	end := *position + length
	var receiveMaximum uint16
	var maximumPacketSize uint32
	for *position < end {
		identifier := body[*position]
		*position++
		switch identifier {
		case 0x21:
			if receiveMaximum != 0 || end-*position < 2 {
				return 0, 0, ErrMalformedPacket
			}
			receiveMaximum = binary.BigEndian.Uint16(body[*position : *position+2])
			*position += 2
			if receiveMaximum == 0 {
				return 0, 0, ErrMalformedPacket
			}
		case 0x27:
			if maximumPacketSize != 0 || end-*position < 4 {
				return 0, 0, ErrMalformedPacket
			}
			maximumPacketSize = binary.BigEndian.Uint32(body[*position : *position+4])
			*position += 4
			if maximumPacketSize == 0 {
				return 0, 0, ErrMalformedPacket
			}
		default:
			return 0, 0, ErrUnsupportedProperty
		}
	}
	return receiveMaximum, maximumPacketSize, nil
}

func validConnAckReason(version ProtocolVersion, reasonCode byte) bool {
	if version == ProtocolV311 {
		return validConnAckReasonV311(reasonCode)
	}
	if version == ProtocolV5 {
		return validConnAckReasonV5(reasonCode)
	}
	return false
}

func validConnAckReasonV311(reasonCode byte) bool {
	return reasonCode <= 5
}

func validConnAckReasonV5(reasonCode byte) bool {
	switch reasonCode {
	case 0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x90, 0x95, 0x97, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9F:
		return true
	default:
		return false
	}
}
