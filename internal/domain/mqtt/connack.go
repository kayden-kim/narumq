package mqtt

// ConnAck is the validated domain representation of an MQTT CONNACK packet.
type ConnAck struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// SessionPresent reports whether an existing session was resumed.
	SessionPresent bool
	// ReasonCode is the MQTT version-specific connection result.
	ReasonCode byte
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
	if err := skipProperties(body, &position); err != nil {
		return nil, err
	}
	if position != len(body) {
		return nil, ErrMalformedPacket
	}
	return &ConnAck{
		Version:        version,
		SessionPresent: body[0]&0x01 != 0,
		ReasonCode:     body[1],
	}, nil
}

func encodeConnAck(connAck ConnAck, maxPacketSize int) ([]byte, error) {
	if connAck.Version != ProtocolV311 && connAck.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if !validConnAckReason(connAck.Version, connAck.ReasonCode) || (connAck.ReasonCode != 0 && connAck.SessionPresent) {
		return nil, ErrMalformedPacket
	}
	bodySize := 2
	if connAck.Version == ProtocolV5 {
		bodySize++
	}
	if bodySize > maxPacketSize {
		return nil, ErrPacketTooLarge
	}
	flags := byte(0)
	if connAck.SessionPresent {
		flags = 1
	}
	body := []byte{flags, connAck.ReasonCode}
	if connAck.Version == ProtocolV5 {
		body = append(body, 0)
	}
	return body, nil
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
