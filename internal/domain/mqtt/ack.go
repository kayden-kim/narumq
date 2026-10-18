package mqtt

import "encoding/binary"

// propertyReasonString is the MQTT 5 Reason String property identifier.
const propertyReasonString = 0x1F

// PubAck acknowledges a QoS 1 PUBLISH.
type PubAck struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// PacketID identifies the PUBLISH being acknowledged.
	PacketID PacketIdentifier
	// ReasonCode reports the MQTT 5 acknowledgement result.
	ReasonCode byte
	// ReasonString optionally explains a failure reason code (0x80 or above) in MQTT 5.
	ReasonString string
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
	// ReasonString optionally explains a failure reason code (0x80 or above) in MQTT 5.
	ReasonString string
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
	packetID, reasonCode, reasonString, err := decodeAcknowledgementBody(packetType, body, version)
	if err != nil {
		return nil, err
	}
	switch packetType {
	case PacketTypePubAck:
		return &PubAck{Version: version, PacketID: packetID, ReasonCode: reasonCode, ReasonString: reasonString}, nil
	case PacketTypePubRec:
		return &PubRec{Version: version, PacketID: packetID, ReasonCode: reasonCode, ReasonString: reasonString}, nil
	case PacketTypePubRel:
		return &PubRel{Version: version, PacketID: packetID, ReasonCode: reasonCode}, nil
	case PacketTypePubComp:
		return &PubComp{Version: version, PacketID: packetID, ReasonCode: reasonCode}, nil
	default:
		return nil, ErrMalformedPacket
	}
}

func decodeAcknowledgementBody(packetType PacketType, body []byte, version ProtocolVersion) (PacketIdentifier, byte, string, error) {
	if version != ProtocolV311 && version != ProtocolV5 {
		return 0, 0, "", ErrUnsupportedProtocolVersion
	}
	if len(body) < 2 {
		return 0, 0, "", ErrMalformedPacket
	}
	packetID := PacketIdentifier(binary.BigEndian.Uint16(body[:2]))
	if packetID == 0 {
		return 0, 0, "", ErrMalformedPacket
	}
	if version == ProtocolV311 {
		if len(body) != 2 {
			return 0, 0, "", ErrMalformedPacket
		}
		return packetID, 0, "", nil
	}

	switch len(body) {
	case 2:
		return packetID, 0, "", nil
	case 3:
		reasonCode := body[2]
		if !validAcknowledgementReason(version, packetType, reasonCode) {
			return 0, 0, "", ErrMalformedPacket
		}
		return packetID, reasonCode, "", nil
	}
	reasonCode := body[2]
	if !validAcknowledgementReason(version, packetType, reasonCode) {
		return 0, 0, "", ErrMalformedPacket
	}
	position := 3
	propertyLength, consumed, err := decodeVariableByteInteger(body[position:])
	if err != nil {
		return 0, 0, "", ErrMalformedPacket
	}
	position += consumed
	if propertyLength > len(body)-position {
		return 0, 0, "", ErrMalformedPacket
	}
	reasonString, err := decodeAcknowledgementProperties(packetType, body[position:position+propertyLength])
	if err != nil {
		return 0, 0, "", err
	}
	if position+propertyLength != len(body) {
		return 0, 0, "", ErrMalformedPacket
	}
	return packetID, reasonCode, reasonString, nil
}

// decodeAcknowledgementProperties accepts one Reason String on PUBACK and PUBREC.
// Every other property, and every property on PUBREL and PUBCOMP, is unsupported.
func decodeAcknowledgementProperties(packetType PacketType, properties []byte) (string, error) {
	reasonString := ""
	seenReasonString := false
	for position := 0; position < len(properties); {
		identifier := properties[position]
		position++
		if identifier != propertyReasonString || (packetType != PacketTypePubAck && packetType != PacketTypePubRec) {
			return "", ErrUnsupportedProperty
		}
		if seenReasonString || len(properties)-position < 2 {
			return "", ErrMalformedPacket
		}
		length := int(binary.BigEndian.Uint16(properties[position : position+2]))
		position += 2
		if length > len(properties)-position {
			return "", ErrMalformedPacket
		}
		reasonString = string(properties[position : position+length])
		if !validMQTTUTF8(reasonString) {
			return "", ErrMalformedPacket
		}
		position += length
		seenReasonString = true
	}
	return reasonString, nil
}

// encodeAcknowledgement encodes one acknowledgement body. A Reason String is written only
// on an MQTT 5 PUBACK or PUBREC with a failure reason code. When the Reason String would
// push the body above maxPacketSize, only the Reason String is dropped.
func encodeAcknowledgement(packetType PacketType, packetID PacketIdentifier, version ProtocolVersion, reasonCode byte, reasonString string, maxPacketSize int) ([]byte, error) {
	if version != ProtocolV311 && version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if packetID == 0 || !validAcknowledgementReason(version, packetType, reasonCode) {
		return nil, ErrMalformedPacket
	}
	if reasonString != "" && !validAcknowledgementReasonString(version, packetType, reasonCode, reasonString) {
		return nil, ErrMalformedPacket
	}
	bodySize := 2
	if version == ProtocolV5 && reasonCode != 0 {
		bodySize = 4
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}
	if reasonString != "" {
		propertyLength := 3 + len(reasonString)
		if withReasonSize := 3 + variableByteIntegerSize(propertyLength) + propertyLength; withReasonSize <= maxPacketSize {
			body := make([]byte, 0, withReasonSize)
			body = binary.BigEndian.AppendUint16(body, uint16(packetID))
			body = append(body, reasonCode)
			body = appendVariableByteInteger(body, propertyLength)
			body = append(body, propertyReasonString)
			return appendUTF8Field(body, reasonString), nil
		}
	}
	body := make([]byte, bodySize)
	binary.BigEndian.PutUint16(body[:2], uint16(packetID))
	if bodySize == 4 {
		body[2] = reasonCode
		body[3] = 0
	}
	return body, nil
}

func validAcknowledgementReasonString(version ProtocolVersion, packetType PacketType, reasonCode byte, reasonString string) bool {
	return version == ProtocolV5 && (packetType == PacketTypePubAck || packetType == PacketTypePubRec) &&
		reasonCode >= 0x80 && len(reasonString) <= 65535 && validMQTTUTF8(reasonString)
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
