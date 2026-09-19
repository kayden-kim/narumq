package mqtt

// Encoder appends validated MQTT packets to caller-provided byte slices.
type Encoder struct {
	maxPacketSize int
}

// NewEncoder creates an encoder whose maximum Remaining Length is maxPacketSize.
func NewEncoder(maxPacketSize int) (*Encoder, error) {
	if err := validatePacketLimit(maxPacketSize); err != nil {
		return nil, err
	}
	return &Encoder{maxPacketSize: maxPacketSize}, nil
}

// Append validates packet and appends its canonical wire representation to dst.
// Validation and size failures leave dst's length and contents unchanged.
func (e *Encoder) Append(dst []byte, packet Packet) ([]byte, error) {
	if e == nil || packet == nil {
		return dst, ErrMalformedPacket
	}

	var (
		packetType PacketType
		flags      byte
		body       []byte
		err        error
	)
	switch value := packet.(type) {
	case Connect:
		packetType = PacketTypeConnect
		flags = 0
		body, err = encodeConnect(value, e.maxPacketSize)
	case *Connect:
		if value == nil {
			return dst, ErrMalformedPacket
		}
		packetType = PacketTypeConnect
		flags = 0
		body, err = encodeConnect(*value, e.maxPacketSize)
	case ConnAck:
		packetType = PacketTypeConnAck
		flags = 0
		body, err = encodeConnAck(value, e.maxPacketSize)
	case *ConnAck:
		if value == nil {
			return dst, ErrMalformedPacket
		}
		packetType = PacketTypeConnAck
		flags = 0
		body, err = encodeConnAck(*value, e.maxPacketSize)
	case Publish:
		packetType = PacketTypePublish
		if value.Retain {
			flags |= 0x01
		}
		body, err = encodePublish(value, e.maxPacketSize)
	case *Publish:
		if value == nil {
			return dst, ErrMalformedPacket
		}
		packetType = PacketTypePublish
		if value.Retain {
			flags |= 0x01
		}
		body, err = encodePublish(*value, e.maxPacketSize)
	default:
		return dst, ErrMalformedPacket
	}
	if err != nil {
		return dst, err
	}

	dst = append(dst, byte(packetType<<4)|flags)
	dst = appendVariableByteInteger(dst, len(body))
	dst = append(dst, body...)
	return dst, nil
}
