package mqtt

// Decoder parses one MQTT control packet at a time.
type Decoder struct {
	maxPacketSize int
}

// NewDecoder creates a decoder whose maximum Remaining Length is maxPacketSize.
func NewDecoder(maxPacketSize int) (*Decoder, error) {
	if err := validatePacketLimit(maxPacketSize); err != nil {
		return nil, err
	}
	return &Decoder{maxPacketSize: maxPacketSize}, nil
}

// Decode parses the first packet in src and returns the packet and bytes used.
// The source is borrowed only during this call; returned packet fields own all
// data copied from src. A failed decode always reports zero bytes consumed.
func (d *Decoder) Decode(src []byte, version ProtocolVersion) (Packet, int, error) {
	if d == nil || len(src) == 0 {
		return nil, 0, ErrIncompletePacket
	}

	packetType := PacketType(src[0] >> 4)
	flags := src[0] & 0x0F
	if err := validateFixedHeader(packetType, flags); err != nil {
		return nil, 0, err
	}
	if version != ProtocolUnknown && version != ProtocolV311 && version != ProtocolV5 {
		return nil, 0, ErrUnsupportedProtocolVersion
	}
	if version == ProtocolUnknown && packetType != PacketTypeConnect {
		return nil, 0, ErrProtocolVersionRequired
	}

	remainingLength, lengthBytes, err := decodeVariableByteInteger(src[1:])
	if err != nil {
		return nil, 0, err
	}
	if remainingLength > d.maxPacketSize {
		return nil, 0, ErrPacketTooLarge
	}
	frameHeaderLength := 1 + lengthBytes
	if remainingLength > len(src)-frameHeaderLength {
		return nil, 0, ErrIncompletePacket
	}
	body := src[frameHeaderLength : frameHeaderLength+remainingLength]

	var packet Packet
	switch packetType {
	case PacketTypeConnect:
		packet, err = decodeConnect(body, version)
	case PacketTypeConnAck:
		packet, err = decodeConnAck(body, version)
	case PacketTypePublish:
		packet, err = decodePublish(body, version, flags)
	default:
		return nil, 0, ErrMalformedPacket
	}
	if err != nil {
		return nil, 0, err
	}
	return packet, frameHeaderLength + remainingLength, nil
}

func validateFixedHeader(packetType PacketType, flags byte) error {
	switch packetType {
	case PacketTypeConnect, PacketTypeConnAck:
		if flags != 0 {
			return ErrMalformedPacket
		}
	case PacketTypePublish:
		qos := (flags >> 1) & 0x03
		switch qos {
		case 0:
			// MQTT-3.3.1-2 requires DUP=0 for QoS 0.
			if flags&0x08 != 0 {
				return ErrMalformedPacket
			}
			return nil
		case 1, 2:
			return ErrUnsupportedQoS
		default:
			return ErrMalformedPacket
		}
	default:
		return ErrMalformedPacket
	}
	return nil
}
