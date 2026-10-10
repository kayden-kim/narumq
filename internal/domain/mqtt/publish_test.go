package mqtt

import (
	"bytes"
	"errors"
	"fmt"
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
		{
			name:  "v311-qos-one",
			input: Publish{Version: ProtocolV311, QoS: QoSAtLeastOnce, PacketID: 7, Topic: "sensor/qos1", Payload: []byte("at-least-once")},
			trace: "PRD MQTT-02/TEST-03; MQTT-3.3.1-2/4; MQTT-4.3.2-1",
		},
		{
			name:  "v5-qos-two-duplicate",
			input: Publish{Version: ProtocolV5, DUP: true, QoS: QoSExactlyOnce, Retain: true, PacketID: 9, Topic: "sensor/qos2", Payload: []byte{0x00, 0xFF}},
			trace: "PRD MQTT-02/TEST-04; MQTT-3.3.1-2/4; MQTT-4.3.3-1/2",
		},
	}
	decoder, err := NewDecoder(MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := NewEncoder(MaxRemainingLength)
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
	encoder, err := NewEncoder(MaxRemainingLength)
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
func TestPublishQoSWireAndPacketIdentifierValidation_MQTT02_MQTT331_2_4_MQTT332_1(t *testing.T) {
	encoder, err := NewEncoder(MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	qos1 := testEncode(t, encoder, Publish{Version: ProtocolV311, QoS: QoSAtLeastOnce, PacketID: 0x1234, Topic: "a", Payload: []byte("hello")})
	if want := []byte{0x32, 0x0A, 0x00, 0x01, 'a', 0x12, 0x34, 'h', 'e', 'l', 'l', 'o'}; !bytes.Equal(qos1, want) {
		t.Fatalf("QoS 1 PUBLISH = %v, want %v", qos1, want)
	}
	qos2 := testEncode(t, encoder, Publish{Version: ProtocolV5, QoS: QoSExactlyOnce, PacketID: 7, Topic: "a", Payload: []byte("hello")})
	if want := []byte{0x34, 0x0B, 0x00, 0x01, 'a', 0x00, 0x07, 0x00, 'h', 'e', 'l', 'l', 'o'}; !bytes.Equal(qos2, want) {
		t.Fatalf("QoS 2 PUBLISH = %v, want %v", qos2, want)
	}

	decoder, err := NewDecoder(MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name  string
		frame []byte
	}{
		{name: "qos-one-zero-packet-id", frame: testFrame(0x32, []byte{0, 1, 'a', 0, 0})},
		{name: "qos-two-zero-packet-id", frame: testFrame(0x34, []byte{0, 1, 'a', 0, 0})},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, consumed, err := decoder.Decode(testCase.frame, ProtocolV311)
			if !errors.Is(err, ErrMalformedPacket) || consumed != 0 {
				t.Fatalf("Decode() = (consumed %d, error %v), want malformed with zero consumption", consumed, err)
			}
		})
	}
	for _, publish := range []Publish{
		{Version: ProtocolV311, QoS: QoSAtMostOnce, PacketID: 1, Topic: "a"},
		{Version: ProtocolV311, QoS: QoSAtLeastOnce, Topic: "a"},
		{Version: ProtocolV311, QoS: QoS(3), PacketID: 1, Topic: "a"},
	} {
		if _, err := encoder.Append(nil, publish); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("Append(%#v) error = %v, want malformed", publish, err)
		}
	}
}

func TestPublishRejectsQoSAndMalformedTopics_MQTT331_1_2_MQTT333_1_2(t *testing.T) {
	decoder, err := NewDecoder(MaxRemainingLength)
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
		{name: "qos-one-missing-packet-id", frame: append([]byte{0x32}, validV311[1:]...), version: ProtocolV311, want: ErrMalformedPacket, trace: "PRD MQTT-02/TEST-03; MQTT-3.3.1-2"},
		{name: "qos-two-missing-packet-id", frame: append([]byte{0x34}, validV311[1:]...), version: ProtocolV311, want: ErrMalformedPacket, trace: "PRD MQTT-02/TEST-03; MQTT-3.3.1-2"},
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
	decoder, err := NewDecoder(MaxRemainingLength)
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

// Traceability: PRD MQTT-01/MQTT-12/TEST-04; docs/specs/mqtt-protocol-and-session.md §3.2, §5; MQTT-3.3.2-5/6.
func TestPublishMessageExpiryProperty_MQTT5(t *testing.T) {
	decoder, err := NewDecoder(MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := NewEncoder(MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, interval := range []uint32{0, 1, ^uint32(0)} {
		t.Run(fmt.Sprintf("interval-%d", interval), func(t *testing.T) {
			input := Publish{
				Version:                  ProtocolV5,
				Topic:                    "a",
				Payload:                  []byte("hello"),
				HasMessageExpiryInterval: true,
				MessageExpiryInterval:    interval,
			}
			wire := testEncode(t, encoder, input)
			decoded := testDecode(t, decoder, wire, ProtocolV5)
			requireSemanticEqual(t, &input, decoded)
			wantProperty := []byte{0x05, 0x02, byte(interval >> 24), byte(interval >> 16), byte(interval >> 8), byte(interval)}
			if !bytes.Contains(wire, wantProperty) {
				t.Fatalf("wire = %v, want property %v", wire, wantProperty)
			}
		})
	}
}

// Traceability: PRD MQTT-01/MQTT-12/TEST-04; docs/specs/mqtt-protocol-and-session.md §3.2, §5; MQTT-3.3.2-5.
func TestPublishMessageExpiryPropertyRejectsInvalidSections(t *testing.T) {
	decoder, err := NewDecoder(MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		body []byte
		want error
	}{
		{name: "duplicate", body: []byte{0, 1, 'a', 10, 0x02, 0, 0, 0, 1, 0x02, 0, 0, 0, 2}, want: ErrMalformedPacket},
		{name: "truncated-value", body: []byte{0, 1, 'a', 4, 0x02, 0, 0, 0}, want: ErrMalformedPacket},
		{name: "unknown-property", body: []byte{0, 1, 'a', 1, 0x01}, want: ErrUnsupportedProperty},
		{name: "declared-section-too-long", body: []byte{0, 1, 'a', 5, 0x02, 0, 0}, want: ErrMalformedPacket},
		{name: "noncanonical-length", body: []byte{0, 1, 'a', 0x80, 0}, want: ErrMalformedPacket},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, _, err := decoder.Decode(testFrame(0x30, testCase.body), ProtocolV5)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("Decode() error = %v, want %v", err, testCase.want)
			}
		})
	}
	encoder, err := NewEncoder(MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Append(nil, Publish{Version: ProtocolV311, Topic: "a", HasMessageExpiryInterval: true}); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("MQTT 3.1.1 expiry property error = %v, want %v", err, ErrMalformedPacket)
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

// Traceability: PRD MQTT-11/TEST-04/05.
func TestPublishPacketSizeAtRemainingLengthBoundaries(t *testing.T) {
	encoder, _ := NewEncoder(MaxRemainingLength)
	for _, version := range []ProtocolVersion{ProtocolV311, ProtocolV5} {
		for _, qos := range []QoS{QoSAtMostOnce, QoSAtLeastOnce, QoSExactlyOnce} {
			for _, expiry := range []bool{false, true} {
				if version == ProtocolV311 && expiry {
					continue
				}
				for _, size := range []int{0, 120, 127, 128, 16380, 16384} {
					input := Publish{Version: version, QoS: qos, Topic: "size", Payload: make([]byte, size), HasMessageExpiryInterval: expiry, MessageExpiryInterval: 60}
					if qos != QoSAtMostOnce {
						input.PacketID = 1
					}
					wire := testEncode(t, encoder, input)
					if got := PublishPacketSize(input); got != len(wire) {
						t.Fatalf("v%d qos%d expiry%v payload%d: size %d, wire %d", version, qos, expiry, size, got, len(wire))
					}
				}
			}
		}
	}
}
