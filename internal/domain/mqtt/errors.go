package mqtt

import "errors"

var (
	// ErrIncompletePacket means that more bytes are needed to finish one frame.
	ErrIncompletePacket = errors.New("incomplete MQTT packet")
	// ErrMalformedPacket means that the frame or its fields violate MQTT rules.
	ErrMalformedPacket = errors.New("malformed MQTT packet")
	// ErrPacketTooLarge means that a packet exceeds the configured limit.
	ErrPacketTooLarge = errors.New("MQTT packet is too large")
	// ErrProtocolVersionRequired means that a non-CONNECT packet lacks context.
	ErrProtocolVersionRequired = errors.New("MQTT protocol version is required")
	// ErrUnsupportedProtocolVersion means that the protocol level is unsupported.
	ErrUnsupportedProtocolVersion = errors.New("unsupported MQTT protocol version")
	// ErrUnsupportedProperty means that a valid property section is not supported.
	ErrUnsupportedProperty = errors.New("unsupported MQTT property")
	// ErrUnsupportedQoS means that the packet uses a valid but unsupported QoS.
	ErrUnsupportedQoS = errors.New("unsupported MQTT QoS")
)

// UnsupportedProtocolVersionError reports the protocol identity rejected by
// the codec without including CONNECT credentials or payload data in its text.
type UnsupportedProtocolVersionError struct {
	// ProtocolName is the protocol name observed in CONNECT.
	ProtocolName string
	// Level is the protocol level observed in CONNECT.
	Level byte
}

// Error implements error.
func (e UnsupportedProtocolVersionError) Error() string {
	return "unsupported MQTT protocol level"
}

// Is classifies the typed error as ErrUnsupportedProtocolVersion.
func (e UnsupportedProtocolVersionError) Is(target error) bool {
	return target == ErrUnsupportedProtocolVersion
}

// As supports both pointer and value errors.As targets.
func (e *UnsupportedProtocolVersionError) As(target any) bool {
	switch typedTarget := target.(type) {
	case **UnsupportedProtocolVersionError:
		*typedTarget = e
		return true
	case *UnsupportedProtocolVersionError:
		*typedTarget = *e
		return true
	default:
		return false
	}
}
