package mqtt

import (
	"strings"
	"unicode/utf8"
)

// PingReq requests a keep-alive response from the broker.
type PingReq struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
}

func (p PingReq) packetType() PacketType {
	return PacketTypePingReq
}

func (p PingReq) packetVersion() ProtocolVersion {
	return p.Version
}

// PingResp acknowledges a PINGREQ.
type PingResp struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
}

func (p PingResp) packetType() PacketType {
	return PacketTypePingResp
}

func (p PingResp) packetVersion() ProtocolVersion {
	return p.Version
}

// Disconnect requests a normal or reasoned MQTT connection termination.
type Disconnect struct {
	// Version is the negotiated MQTT protocol version.
	Version ProtocolVersion
	// ReasonCode is zero for normal termination.
	ReasonCode byte
	// ServerReference is MQTT 5 property 0x1C, sent from server to client.
	ServerReference string
}

func (p Disconnect) packetType() PacketType {
	return PacketTypeDisconnect
}

func (p Disconnect) packetVersion() ProtocolVersion {
	return p.Version
}

func decodePingReq(body []byte, version ProtocolVersion) (Packet, error) {
	if version != ProtocolV311 && version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if len(body) != 0 {
		return nil, ErrMalformedPacket
	}
	return &PingReq{Version: version}, nil
}

func decodePingResp(body []byte, version ProtocolVersion) (Packet, error) {
	if version != ProtocolV311 && version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if len(body) != 0 {
		return nil, ErrMalformedPacket
	}
	return &PingResp{Version: version}, nil
}

func decodeDisconnect(body []byte, version ProtocolVersion) (Packet, error) {
	if version == ProtocolV311 {
		if len(body) != 0 {
			return nil, ErrMalformedPacket
		}
		return &Disconnect{Version: version}, nil
	}
	if version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if len(body) == 0 {
		return &Disconnect{Version: version}, nil
	}
	if !validDisconnectReason(body[0]) {
		return nil, ErrMalformedPacket
	}
	if len(body) == 1 {
		return &Disconnect{Version: version, ReasonCode: body[0]}, nil
	}
	propertyLength, consumed, err := decodeVariableByteInteger(body[1:])
	if err != nil {
		return nil, ErrMalformedPacket
	}
	position := 1 + consumed
	if propertyLength > len(body)-position {
		return nil, ErrMalformedPacket
	}
	if propertyLength != 0 {
		return nil, ErrUnsupportedProperty
	}
	if position+propertyLength != len(body) {
		return nil, ErrMalformedPacket
	}
	return &Disconnect{Version: version, ReasonCode: body[0]}, nil
}

func encodePingReq(ping PingReq, maxPacketSize int) ([]byte, error) {
	if ping.Version != ProtocolV311 && ping.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if maxPacketSize < 0 {
		return nil, ErrPacketTooLarge
	}
	return nil, nil
}

func encodePingResp(ping PingResp, maxPacketSize int) ([]byte, error) {
	if ping.Version != ProtocolV311 && ping.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if maxPacketSize < 0 {
		return nil, ErrPacketTooLarge
	}
	return nil, nil
}

func encodeDisconnect(disconnect Disconnect, maxPacketSize int) ([]byte, error) {
	if disconnect.Version != ProtocolV311 && disconnect.Version != ProtocolV5 {
		return nil, ErrUnsupportedProtocolVersion
	}
	if disconnect.ServerReference != "" {
		if disconnect.Version != ProtocolV5 || (disconnect.ReasonCode != 0x9C && disconnect.ReasonCode != 0x9D) ||
			len(disconnect.ServerReference) > 65535 || !utf8.ValidString(disconnect.ServerReference) ||
			strings.IndexByte(disconnect.ServerReference, 0) >= 0 {
			return nil, ErrMalformedPacket
		}
		propertyLength := 3 + len(disconnect.ServerReference)
		body := []byte{disconnect.ReasonCode}
		body = appendVariableByteInteger(body, propertyLength)
		body = append(body, 0x1C)
		body = appendUTF8Field(body, disconnect.ServerReference)
		if len(body) > maxPacketSize {
			return nil, ErrPacketTooLarge
		}
		return body, nil
	}
	if disconnect.Version == ProtocolV311 {
		if disconnect.ReasonCode != 0 {
			return nil, ErrMalformedPacket
		}
		return nil, nil
	}
	if !validDisconnectReason(disconnect.ReasonCode) {
		return nil, ErrMalformedPacket
	}
	if disconnect.ReasonCode == 0 {
		return nil, nil
	}
	if maxPacketSize < 2 {
		return nil, ErrPacketTooLarge
	}
	return []byte{disconnect.ReasonCode, 0}, nil
}

func validDisconnectReason(reasonCode byte) bool {
	switch reasonCode {
	case 0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8D, 0x8E, 0x8F, 0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2:
		return true
	default:
		return false
	}
}
