package mqtt

import (
	"errors"
	"testing"
)

func TestRemainingLengthCanonicalBoundaries_MQTT153_1_2_MQTT154_1_2(t *testing.T) {
	cases := []struct {
		name  string
		value int
		wire  []byte
		trace string
	}{
		{name: "zero", value: 0, wire: []byte{0}, trace: "PRD TEST-03; MQTT-3.1.2-1; MQTT-1.5.3-1"},
		{name: "one-byte-maximum", value: 127, wire: []byte{127}, trace: "PRD TEST-03; MQTT-3.1.2-1; MQTT-1.5.3-1"},
		{name: "two-byte-minimum", value: 128, wire: []byte{0x80, 0x01}, trace: "PRD TEST-03; MQTT-3.1.2-1; MQTT-1.5.3-1"},
		{name: "two-byte-maximum", value: 16383, wire: []byte{0xFF, 0x7F}, trace: "PRD TEST-03; MQTT-3.1.2-1; MQTT-1.5.3-1"},
		{name: "three-byte-minimum", value: 16384, wire: []byte{0x80, 0x80, 0x01}, trace: "PRD TEST-03; MQTT-3.1.2-1; MQTT-1.5.3-1"},
		{name: "three-byte-maximum", value: 2097151, wire: []byte{0xFF, 0xFF, 0x7F}, trace: "PRD TEST-03; MQTT-3.1.2-1; MQTT-1.5.3-1"},
		{name: "four-byte-minimum", value: 2097152, wire: []byte{0x80, 0x80, 0x80, 0x01}, trace: "PRD TEST-03; MQTT-3.1.2-1; MQTT-1.5.3-1"},
		{name: "four-byte-maximum", value: MaxRemainingLength, wire: []byte{0xFF, 0xFF, 0xFF, 0x7F}, trace: "PRD TEST-03; MQTT-3.1.2-1; MQTT-1.5.3-1"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			encoded := appendVariableByteInteger(nil, testCase.value)
			if string(encoded) != string(testCase.wire) {
				t.Fatalf("encoded %v, want %v (%s)", encoded, testCase.wire, testCase.trace)
			}
			decoded, consumed, err := decodeVariableByteInteger(testCase.wire)
			if err != nil {
				t.Fatalf("decodeVariableByteInteger() error = %v", err)
			}
			if decoded != testCase.value || consumed != len(testCase.wire) {
				t.Fatalf("decoded (%d, %d), want (%d, %d)", decoded, consumed, testCase.value, len(testCase.wire))
			}
		})
	}
}

func TestRemainingLengthRejectsInvalidEncodings_MQTT153_1_2_MQTT154_1_2(t *testing.T) {
	cases := []struct {
		name string
		wire []byte
		want error
	}{
		{name: "truncated", wire: []byte{0x80}, want: ErrIncompletePacket},
		{name: "non-canonical-zero", wire: []byte{0x80, 0x00}, want: ErrMalformedPacket},
		{name: "non-canonical-127", wire: []byte{0xFF, 0x00}, want: ErrMalformedPacket},
		{name: "fifth-continuation", wire: []byte{0x80, 0x80, 0x80, 0x80, 0x00}, want: ErrMalformedPacket},
		{name: "fourth-continuation-without-fifth", wire: []byte{0x80, 0x80, 0x80, 0x80}, want: ErrMalformedPacket},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, _, err := decodeVariableByteInteger(testCase.wire)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("error = %v, want errors.Is(..., %v)", err, testCase.want)
			}
		})
	}
}

// Traceability: PRD MQTT-11; MQTT-1.2.7-1.
func TestRemainingLengthPacketLimitIsCheckedBeforeIncompleteBodyAllocation_MQTT127_1(t *testing.T) {
	decoder, err := NewDecoder(10)
	if err != nil {
		t.Fatalf("NewDecoder() error = %v", err)
	}
	_, consumed, err := decoder.Decode([]byte{0x30, 0x0B}, ProtocolV311)
	if !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("Decode() error = %v, want ErrPacketTooLarge", err)
	}
	if consumed != 0 {
		t.Fatalf("Decode() consumed %d bytes after limit failure", consumed)
	}
}

func TestRemainingLengthDecodeErrorsThroughFrameParser(t *testing.T) {
	decoder, err := NewDecoder(MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name  string
		frame []byte
		want  error
	}{
		{name: "truncated", frame: []byte{0x30, 0x80}, want: ErrIncompletePacket},
		{name: "noncanonical", frame: []byte{0x30, 0x80, 0x00}, want: ErrMalformedPacket},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, consumed, err := decoder.Decode(testCase.frame, ProtocolV311)
			if !errors.Is(err, testCase.want) || consumed != 0 {
				t.Fatalf("Decode() = (consumed %d, error %v), want (0, %v)", consumed, err, testCase.want)
			}
		})
	}
}

// Traceability: PRD MQTT-11; configured packet limits must fail before body processing.
func TestRemainingLengthPacketLimitConfiguration(t *testing.T) {
	for _, maxPacketSize := range []int{0, -1, MaxRemainingLength + 1} {
		if _, err := NewDecoder(maxPacketSize); !errors.Is(err, ErrPacketTooLarge) {
			t.Fatalf("NewDecoder(%d) error = %v, want ErrPacketTooLarge", maxPacketSize, err)
		}
		if _, err := NewEncoder(maxPacketSize); !errors.Is(err, ErrPacketTooLarge) {
			t.Fatalf("NewEncoder(%d) error = %v, want ErrPacketTooLarge", maxPacketSize, err)
		}
	}
}
