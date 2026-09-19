package mqtt

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestPublishRoundTripAndOwnership_MQTT311_MQTT5_MQTT331_MQTT332(t *testing.T) {
	cases := []struct {
		name  string
		input Publish
		trace string
	}{
		{
			name:  "v311-empty-payload",
			input: Publish{Version: ProtocolV311, Topic: "sensor/empty"},
			trace: "PRD MQTT-01/TEST-03; MQTT-3.3.1-2/4; MQTT-3.3.2-1/2",
		},
		{
			name:  "v311-dup-retain",
			input: Publish{Version: ProtocolV311, Retain: true, Topic: "sensor/value", Payload: []byte("hello")},
			trace: "PRD MQTT-01/TEST-03; MQTT-3.3.1-2/4; MQTT-3.3.2-1/2",
		},
		{
			name:  "v5-empty-payload",
			input: Publish{Version: ProtocolV5, Topic: "sensor/v5"},
			trace: "PRD MQTT-01/TEST-04; MQTT-3.3.1-2/4; MQTT-3.3.2-1/2",
		},
		{
			name:  "v5-binary-payload",
			input: Publish{Version: ProtocolV5, Retain: true, Topic: "sensor/binary", Payload: []byte{0x00, 0xFF, 0x01}},
			trace: "PRD MQTT-01/TEST-04; MQTT-3.3.1-2/4; MQTT-3.3.2-1/2",
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
			beforeMutation := packetValue(decoded)
			for index := range wire {
				wire[index] = 0x5A
			}
			if !reflect.DeepEqual(beforeMutation, packetValue(decoded)) {
				t.Fatalf("decoded PUBLISH changed after source mutation (%s)", testCase.trace)
			}
		})
	}
}

func TestPublishCanonicalWire_MQTT311_MQTT5(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	v311 := testEncode(t, encoder, Publish{Version: ProtocolV311, Topic: "a", Payload: []byte("hello")})
	if !bytes.Equal(v311, []byte{0x30, 0x08, 0x00, 0x01, 'a', 'h', 'e', 'l', 'l', 'o'}) {
		t.Fatalf("MQTT 3.1.1 PUBLISH = %v", v311)
	}
	v5 := testEncode(t, encoder, Publish{Version: ProtocolV5, Topic: "a", Payload: []byte("hello")})
	if !bytes.Equal(v5, []byte{0x30, 0x09, 0x00, 0x01, 'a', 0x00, 'h', 'e', 'l', 'l', 'o'}) {
		t.Fatalf("MQTT 5 PUBLISH = %v", v5)
	}
}

func TestPublishRejectsQoSAndMalformedTopics_MQTT331_1_2_MQTT333_1_2(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	validV311 := testFrame(0x30, []byte{0x00, 0x01, 'a'})
	cases := []struct {
		name    string
		frame   []byte
		version ProtocolVersion
		want    error
		trace   string
	}{
		{name: "qos-one", frame: append([]byte{0x32}, validV311[1:]...), version: ProtocolV311, want: ErrUnsupportedQoS, trace: "PRD MQTT-12/TEST-03; MQTT-3.3.1-2"},
		{name: "qos-two", frame: append([]byte{0x34}, validV311[1:]...), version: ProtocolV311, want: ErrUnsupportedQoS, trace: "PRD MQTT-12/TEST-03; MQTT-3.3.1-2"},
		{name: "qos-three", frame: append([]byte{0x36}, validV311[1:]...), version: ProtocolV311, want: ErrMalformedPacket, trace: "PRD MQTT-12/TEST-03; MQTT-3.3.1-2"},
		{name: "dup-on-qos-zero", frame: append([]byte{0x38}, validV311[1:]...), version: ProtocolV311, want: ErrMalformedPacket, trace: "PRD MQTT-12/TEST-03; MQTT-3.3.1-2"},
		{name: "empty-topic", frame: testFrame(0x30, []byte{0, 0}), version: ProtocolV311, want: ErrMalformedPacket, trace: "PRD MQTT-12/TEST-03; MQTT-3.3.2-1"},
		{name: "invalid-topic-utf8", frame: testFrame(0x30, []byte{0, 2, 0xC0, 0x80}), version: ProtocolV311, want: ErrMalformedPacket, trace: "PRD MQTT-12/TEST-03; MQTT-1.5.3-1/2"},
		{name: "nul-topic", frame: testFrame(0x30, []byte{0, 1, 0}), version: ProtocolV311, want: ErrMalformedPacket, trace: "PRD MQTT-12/TEST-03; MQTT-1.5.3-1/2"},
		{name: "wildcard-hash-topic", frame: testFrame(0x30, []byte{0, 1, '#'}), version: ProtocolV311, want: ErrMalformedPacket, trace: "PRD MQTT-12/TEST-03; MQTT-3.3.2-1"},
		{name: "wildcard-plus-topic", frame: testFrame(0x30, []byte{0, 1, '+'}), version: ProtocolV311, want: ErrMalformedPacket, trace: "PRD MQTT-12/TEST-03; MQTT-3.3.2-1"},
		{name: "unknown-version", frame: validV311, version: ProtocolUnknown, want: ErrProtocolVersionRequired, trace: "PRD MQTT-01/TEST-03; PRD MQTT-12/TEST-03; MQTT-3.3.1-2"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, consumed, err := decoder.Decode(testCase.frame, testCase.version)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("Decode() error = %v, want %v (%s)", err, testCase.want, testCase.trace)
			}
			if consumed != 0 {
				t.Fatalf("Decode() consumed %d bytes on error", consumed)
			}
		})
	}
}

func TestPublishRejectsMQTT5Properties_MQTT5_1_5_5_1(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		body []byte
		want error
	}{
		{name: "nonzero-properties", body: []byte{0, 1, 'a', 1, 0}, want: ErrUnsupportedProperty},
		{name: "truncated-properties", body: []byte{0, 1, 'a', 1}, want: ErrMalformedPacket},
		{name: "noncanonical-properties-length", body: []byte{0, 1, 'a', 0x80, 0}, want: ErrMalformedPacket},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, _, err := decoder.Decode(testFrame(0x30, testCase.body), ProtocolV5)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("Decode() error = %v, want %v", err, testCase.want)
			}
		})
	}
}

func TestPublishEncoderLeavesDestinationUnchangedOnErrorAndHonorsLimit(t *testing.T) {
	encoder, err := NewEncoder(8)
	if err != nil {
		t.Fatal(err)
	}
	destination := []byte{0xDE, 0xAD}
	before := append([]byte(nil), destination...)
	_, err = encoder.Append(destination, Publish{Version: ProtocolV311, Topic: "topic", Payload: []byte("too large")})
	if !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("Append() error = %v, want ErrPacketTooLarge", err)
	}
	if !bytes.Equal(destination, before) {
		t.Fatalf("destination changed after size error")
	}
	_, err = encoder.Append(destination, Publish{Version: ProtocolV311, Topic: "bad\x00topic"})
	if !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("Append() invalid topic error = %v, want ErrMalformedPacket", err)
	}
	_, err = encoder.Append(destination, Publish{Version: ProtocolV311, DUP: true, Topic: "topic"})
	if !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("Append() DUP error = %v, want ErrMalformedPacket", err)
	}
}
