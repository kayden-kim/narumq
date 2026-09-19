package mqtt

import "encoding/binary"

// PubAck acknowledges a QoS 1 PUBLISH.
type PubAck struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// PacketID identifies the PUBLISH being acknowledged.
	PacketID PacketIdentifier
	// ReasonCode reports the MQTT 5 acknowledgement result.
	ReasonCode byte
}

func (p PubAck) packetType() PacketType {
	return PacketTypePubAck
}

func (p PubAck) packetVersion() ProtocolVersion {
	return p.Version
}

// PubRec acknowledges receipt of a QoS 2 PUBLISH.
type PubRec struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// PacketID identifies the PUBLISH being acknowledged.
	PacketID PacketIdentifier
	// ReasonCode reports the MQTT 5 acknowledgement result.
	ReasonCode byte
}

func (p PubRec) packetType() PacketType {
	return PacketTypePubRec
}

func (p PubRec) packetVersion() ProtocolVersion {
	return p.Version
}

// PubRel releases a QoS 2 PUBLISH after PUBREC.
type PubRel struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// PacketID identifies the PUBLISH being released.
	PacketID PacketIdentifier
	// ReasonCode reports the MQTT 5 release result.
	ReasonCode byte
}

func (p PubRel) packetType() PacketType {
	return PacketTypePubRel
}

func (p PubRel) packetVersion() ProtocolVersion {
	return p.Version
}

// PubComp completes a QoS 2 exchange after PUBREL.
type PubComp struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// PacketID identifies the QoS 2 exchange being completed.
	PacketID PacketIdentifier
	// ReasonCode reports the MQTT 5 completion result.
	ReasonCode byte
}

func (p PubComp) packetType() PacketType {
	return PacketTypePubComp
}

func (p PubComp) packetVersion() ProtocolVersion {
	return p.Version
}

func decodeAcknowledgement(packetType PacketType, body []byte, version ProtocolVersion) (Packet, error) {
	packetID, reasonCode, err := decodeAcknowledgementBody(packetType, body, version)
	if err != nil {
		return nil, err
	}
	switch packetType {
	case PacketTypePubAck:
		return &PubAck{Version: version, PacketID: packetID, ReasonCode: reasonCode}, nil
	case PacketTypePubRec:
		return &PubRec{Version: version, PacketID: packetID, ReasonCode: reasonCode}, nil
	case PacketTypePubRel:
		return &PubRel{Version: version, PacketID: packetID, ReasonCode: reasonCode}, nil
	case PacketTypePubComp:
		return &PubComp{Version: version, PacketID: packetID, ReasonCode: reasonCode}, nil
	default:
		return nil, ErrMalformedPacket
	}
}

func decodeAcknowledgementBody(packetType PacketType, body []byte, version ProtocolVersion) (PacketIdentifier, byte, error) {
	if version != ProtocolV311 && version != ProtocolV5 {
		return 0, 0, ErrUnsupportedProtocolVersion
	}
	if len(body) < 2 {
		return 0, 0, ErrMalformedPacket
	}
	packetID := PacketIdentifier(binary.BigEndian.Uint16(body[:2]))
	if packetID == 0 {
		return 0, 0, ErrMalformedPacket
	}
	if version == ProtocolV311 {
		if len(body) != 2 {
			return 0, 0, ErrMalformedPacket
		}
		return packetID, 0, nil
	}

	var reasonCode byte
	switch len(body) {
	case 2:
		reasonCode = 0
	case 3:
		reasonCode = body[2]
		if !validAcknowledgementReason(version, packetType, reasonCode) {
			return 0, 0, ErrMalformedPacket
		}
		return packetID, reasonCode, nil
	default:
		reasonCode = body[2]
		if !validAcknowledgementReason(version, packetType, reasonCode) {
			return 0, 0, ErrMalformedPacket
		}
		position := 3
		propertyLength, consumed, err := decodeVariableByteInteger(body[position:])
		if err != nil {
			return 0, 0, ErrMalformedPacket
		}
		position += consumed
		if propertyLength > len(body)-position {
			return 0, 0, ErrMalformedPacket
		}
		if propertyLength != 0 {
			return 0, 0, ErrUnsupportedProperty
		}
		position += propertyLength
		if position != len(body) {
			return 0, 0, ErrMalformedPacket
		}
	}
	return packetID, reasonCode, nil
}

func encodeAcknowledgement(packetType PacketType, packetID PacketIdentifier, version ProtocolVersion, reasonCode byte, maxPacketSize int) ([]byte, error) {
	if version != ProtocolV311 && version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if packetID == 0 || !validAcknowledgementReason(version, packetType, reasonCode) {
		return nil, ErrMalformedPacket
	}
	bodySize := 2
	if version == ProtocolV5 && reasonCode != 0 {
		bodySize = 4
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}
	body := make([]byte, bodySize)
	binary.BigEndian.PutUint16(body[:2], uint16(packetID))
	if bodySize == 4 {
		body[2] = reasonCode
		body[3] = 0
	}
	return body, nil
}

func validAcknowledgementReason(version ProtocolVersion, packetType PacketType, reasonCode byte) bool {
	if version == ProtocolV311 {
		return reasonCode == 0
	}
	if version != ProtocolV5 {
		return false
	}
	switch packetType {
	case PacketTypePubAck, PacketTypePubRec:
		switch reasonCode {
		case 0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99:
			return true
		default:
			return false
		}
	case PacketTypePubRel, PacketTypePubComp:
		return reasonCode == 0x00 || reasonCode == 0x92
	default:
		return false
	}
}
