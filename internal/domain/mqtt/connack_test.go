package mqtt

import (
	"bytes"
	"errors"
	"testing"
)

func TestConnAckRoundTrip_MQTT311_MQTT5_MQTT322(t *testing.T) {
	cases := []struct {
		name  string
		input ConnAck
		trace string
	}{
		{
			name:  "v311-success-session-present",
			input: ConnAck{Version: ProtocolV311, SessionPresent: true, ReasonCode: 0},
			trace: "PRD MQTT-01/TEST-03; MQTT-3.2.2-1/4",
		},
		{
			name:  "v311-failure",
			input: ConnAck{Version: ProtocolV311, ReasonCode: 5},
			trace: "PRD MQTT-01/TEST-03; MQTT-3.2.2-1/4",
		},
		{
			name:  "v5-success",
			input: ConnAck{Version: ProtocolV5, SessionPresent: true, ReasonCode: 0},
			trace: "PRD MQTT-01/TEST-04; MQTT-3.2.2-1/6/8",
		},
		{
			name:  "v5-failure",
			input: ConnAck{Version: ProtocolV5, ReasonCode: 0x87},
			trace: "PRD MQTT-01/TEST-04; MQTT-3.2.2-1/6/8",
		},
	}
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			wire := testEncode(t, encoder, &testCase.input)
			decoded := testDecode(t, decoder, wire, testCase.input.Version)
			requireSemanticEqual(t, &testCase.input, decoded)
			if len(wire) == 0 {
				t.Fatalf("empty wire for %s", testCase.trace)
			}
		})
	}
}

func TestConnAckCanonicalWire_MQTT311_MQTT5(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	v311 := testEncode(t, encoder, ConnAck{Version: ProtocolV311})
	if !bytes.Equal(v311, []byte{0x20, 0x02, 0x00, 0x00}) {
		t.Fatalf("MQTT 3.1.1 CONNACK = %v", v311)
	}
	v5 := testEncode(t, encoder, ConnAck{Version: ProtocolV5})
	if !bytes.Equal(v5, []byte{0x20, 0x03, 0x00, 0x00, 0x00}) {
		t.Fatalf("MQTT 5 CONNACK = %v", v5)
	}
}

func TestConnAckReasonCodeAllowLists_MQTT311_3_2_2_1_MQTT5_3_2_2_8(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for reasonCode := byte(0); reasonCode <= 5; reasonCode++ {
		body := []byte{0, reasonCode}
		if _, _, err := decoder.Decode(testFrame(0x20, body), ProtocolV311); err != nil {
			t.Fatalf("MQTT 3.1.1 reason 0x%02X rejected: %v", reasonCode, err)
		}
	}
	for _, reasonCode := range []byte{0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x90, 0x95, 0x97, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9F} {
		body := []byte{0, reasonCode, 0}
		if _, _, err := decoder.Decode(testFrame(0x20, body), ProtocolV5); err != nil {
			t.Fatalf("MQTT 5 reason 0x%02X rejected: %v", reasonCode, err)
		}
	}
	for _, testCase := range []struct {
		version    ProtocolVersion
		reasonCode byte
	}{
		{ProtocolV311, 6},
		{ProtocolV311, 0x80},
		{ProtocolV5, 0x01},
		{ProtocolV5, 0x8B},
		{ProtocolV5, 0xFF},
	} {
		body := []byte{0, testCase.reasonCode}
		if testCase.version == ProtocolV5 {
			body = append(body, 0)
		}
		_, _, err := decoder.Decode(testFrame(0x20, body), testCase.version)
		if !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("version %d reason 0x%02X error = %v, want malformed", testCase.version, testCase.reasonCode, err)
		}
	}
}

func TestConnAckRejectsMalformedAndUnsupportedProperties_MQTT5_1_5_5_1(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		body  []byte
		want  error
		trace string
	}{
		{name: "reserved-ack-flags", body: []byte{2, 0, 0}, want: ErrMalformedPacket, trace: "PRD MQTT-12/TEST-04; MQTT-2.2.2-1/2"},
		{name: "session-present-on-failure", body: []byte{1, 0x87, 0}, want: ErrMalformedPacket, trace: "PRD MQTT-01/TEST-04; PRD MQTT-12/TEST-04; MQTT-3.2.2-1/6"},
		{name: "nonzero-properties", body: []byte{0, 0, 1, 0}, want: ErrUnsupportedProperty, trace: "PRD MQTT-12/TEST-04; MQTT-1.5.5-1"},
		{name: "truncated-properties", body: []byte{0, 0, 1}, want: ErrMalformedPacket, trace: "PRD MQTT-12/TEST-04; MQTT-1.5.5-1"},
		{name: "trailing-byte", body: []byte{0, 0, 0, 0}, want: ErrMalformedPacket, trace: "PRD MQTT-12/TEST-04; MQTT-3.2.2-8"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, consumed, err := decoder.Decode(testFrame(0x20, testCase.body), ProtocolV5)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("Decode() error = %v, want %v (%s)", err, testCase.want, testCase.trace)
			}
			if consumed != 0 {
				t.Fatalf("Decode() consumed %d bytes on error", consumed)
			}
		})
	}
}

func TestConnAckSessionPresentInvariantAndProtocolContext(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []ProtocolVersion{ProtocolV311, ProtocolV5} {
		body := []byte{1, 1}
		if version == ProtocolV5 {
			body = append(body, 0)
		}
		_, _, err := decoder.Decode(testFrame(0x20, body), version)
		if !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("version %d error = %v, want malformed", version, err)
		}
	}
	_, _, err = decoder.Decode([]byte{0x20, 0x02, 0, 0}, ProtocolUnknown)
	if !errors.Is(err, ErrProtocolVersionRequired) {
		t.Fatalf("unknown-version error = %v, want ErrProtocolVersionRequired", err)
	}
}

func TestConnAckEncoderLeavesDestinationUnchangedOnError(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	destination := []byte{0x11, 0x22}
	before := append([]byte(nil), destination...)
	_, err = encoder.Append(destination, ConnAck{Version: ProtocolV5, ReasonCode: 0x01})
	if !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("Append() error = %v, want malformed", err)
	}
	if !bytes.Equal(destination, before) {
		t.Fatalf("destination changed after error")
	}
}
