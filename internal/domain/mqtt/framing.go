package mqtt

import "unicode/utf8"

const maxRemainingLength = 268435455

// validatePacketLimit validates the configured MQTT Remaining Length limit.
func validatePacketLimit(maxPacketSize int) error {
	if maxPacketSize < 1 || maxPacketSize > maxRemainingLength {
		return ErrPacketTooLarge
	}
	return nil
}

// decodeVariableByteInteger decodes one canonical MQTT variable byte integer.
// It reports ErrIncompletePacket only when another byte could make the value
// complete; a fifth continuation byte and non-canonical encodings are malformed.
func decodeVariableByteInteger(src []byte) (int, int, error) {
	value := 0
	multiplier := 1
	minimum := 0
	for index := range 4 {
		if index >= len(src) {
			return 0, 0, ErrIncompletePacket
		}
		current := src[index]
		value += int(current&0x7F) * multiplier
		if current&0x80 == 0 {
			if value < minimum {
				return 0, 0, ErrMalformedPacket
			}
			return value, index + 1, nil
		}
		minimum = multiplier * 128
		multiplier *= 128
	}
	return 0, 0, ErrMalformedPacket
}

// variableByteIntegerSize returns the canonical encoded width of value.
func variableByteIntegerSize(value int) int {
	switch {
	case value < 128:
		return 1
	case value < 16384:
		return 2
	case value < 2097152:
		return 3
	default:
		return 4
	}
}

// PacketSize returns total packet bytes for a valid MQTT Remaining Length.
func PacketSize(remainingLength int) int {
	return 1 + variableByteIntegerSize(remainingLength) + remainingLength
}

// appendVariableByteInteger appends a canonical MQTT variable byte integer.
func appendVariableByteInteger(dst []byte, value int) []byte {
	for {
		digit := byte(value % 128)
		value /= 128
		if value > 0 {
			digit |= 0x80
		}
		dst = append(dst, digit)
		if value == 0 {
			return dst
		}
	}
}

// validMQTTUTF8 applies the MQTT well-formed UTF-8 restrictions.
func validMQTTUTF8(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, codePoint := range value {
		if codePoint == 0 || (codePoint >= 0xD800 && codePoint <= 0xDFFF) {
			return false
		}
		if codePoint >= 0xFDD0 && codePoint <= 0xFDEF {
			return false
		}
		if codePoint&0xFFFF == 0xFFFE || codePoint&0xFFFF == 0xFFFF {
			return false
		}
	}
	return true
}

// validMQTTTopicName validates a Topic Name, including its no-wildcard rule.
func validMQTTTopicName(value string) bool {
	if value == "" || !validMQTTUTF8(value) {
		return false
	}
	for _, codePoint := range value {
		if codePoint == '+' || codePoint == '#' {
			return false
		}
	}
	return true
}

// cloneBytes copies a byte field so decoded packets own their data.
func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	copyOfValue := make([]byte, len(value))
	copy(copyOfValue, value)
	return copyOfValue
}
