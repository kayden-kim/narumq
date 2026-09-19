package mqtt

import (
	"bytes"
	"errors"
	"testing"
)

// Traceability: PRD MQTT-02/TEST-03/04; MQTT-3.4.2-1, MQTT-3.5.2-1,
// MQTT-3.6.2-1, and MQTT-3.7.2-1.
func TestAcknowledgementRoundTrip_MQTT311_MQTT5(t *testing.T) {
	cases := []struct {
		name   string
		packet Packet
		trace  string
	}{
		{name: "v311-puback", packet: &PubAck{Version: ProtocolV311, PacketID: 1}, trace: "PRD MQTT-02/TEST-03; MQTT-3.4.2-1"},
		{name: "v311-pubrec", packet: &PubRec{Version: ProtocolV311, PacketID: 2}, trace: "PRD MQTT-02/TEST-03; MQTT-3.5.2-1"},
		{name: "v311-pubrel", packet: &PubRel{Version: ProtocolV311, PacketID: 3}, trace: "PRD MQTT-02/TEST-03; MQTT-3.6.2-1"},
		{name: "v311-pubcomp", packet: &PubComp{Version: ProtocolV311, PacketID: 4}, trace: "PRD MQTT-02/TEST-03; MQTT-3.7.2-1"},
		{name: "v5-puback-success", packet: &PubAck{Version: ProtocolV5, PacketID: 5}, trace: "PRD MQTT-02/TEST-04; MQTT-3.4.2-1"},
		{name: "v5-pubrec-failure", packet: &PubRec{Version: ProtocolV5, PacketID: 6, ReasonCode: 0x97}, trace: "PRD MQTT-02/TEST-04; MQTT-3.5.2-1"},
		{name: "v5-pubrel-not-found", packet: &PubRel{Version: ProtocolV5, PacketID: 7, ReasonCode: 0x92}, trace: "PRD MQTT-02/TEST-04; MQTT-3.6.2-1"},
		{name: "v5-pubcomp-not-found", packet: &PubComp{Version: ProtocolV5, PacketID: 8, ReasonCode: 0x92}, trace: "PRD MQTT-02/TEST-04; MQTT-3.7.2-1"},
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
			wire := testEncode(t, encoder, testCase.packet)
			decoded := testDecode(t, decoder, wire, testCase.packet.packetVersion())
			requireSemanticEqual(t, testCase.packet, decoded)
			for index := range wire {
				wire[index] = 0xA5
			}
			requireSemanticEqual(t, testCase.packet, decoded)
		})
	}
}

func TestAcknowledgementCanonicalWire_MQTT311_MQTT5(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		packet Packet
		want   []byte
	}{
		{name: "puback-v311", packet: &PubAck{Version: ProtocolV311, PacketID: 0x0123}, want: []byte{0x40, 0x02, 0x01, 0x23}},
		{name: "pubrec-v5-success", packet: &PubRec{Version: ProtocolV5, PacketID: 0x0123}, want: []byte{0x50, 0x02, 0x01, 0x23}},
		{name: "pubrel-v5-failure", packet: &PubRel{Version: ProtocolV5, PacketID: 0x0123, ReasonCode: 0x92}, want: []byte{0x62, 0x04, 0x01, 0x23, 0x92, 0x00}},
		{name: "pubcomp-v311", packet: &PubComp{Version: ProtocolV311, PacketID: 0x0123}, want: []byte{0x70, 0x02, 0x01, 0x23}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testEncode(t, encoder, testCase.packet); !bytes.Equal(got, testCase.want) {
				t.Fatalf("wire = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestAcknowledgementDecodeFormsAndPropertyErrors_MQTT5(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name       string
		packetType byte
		flags      byte
		body       []byte
		reasonCode byte
	}{
		{name: "puback-implicit-success", packetType: 4, flags: 0, body: []byte{0, 1}, reasonCode: 0},
		{name: "pubrec-reason-only", packetType: 5, flags: 0, body: []byte{0, 1, 0x10}, reasonCode: 0x10},
		{name: "pubrel-reason-and-empty-properties", packetType: 6, flags: 2, body: []byte{0, 1, 0, 0}, reasonCode: 0},
		{name: "pubcomp-reason-only", packetType: 7, flags: 0, body: []byte{0, 1, 0x92}, reasonCode: 0x92},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			packet, consumed, err := decoder.Decode(testFrame(testCase.packetType<<4|testCase.flags, testCase.body), ProtocolV5)
			if err != nil || consumed == 0 {
				t.Fatalf("Decode() = (consumed %d, error %v)", consumed, err)
			}
			if got := acknowledgementReason(packet); got != testCase.reasonCode {
				t.Fatalf("reason code = 0x%02X, want 0x%02X", got, testCase.reasonCode)
			}
		})
	}
	for _, testCase := range []struct {
		name string
		body []byte
		want error
	}{
		{name: "unknown-reason", body: []byte{0, 1, 0x01}, want: ErrMalformedPacket},
		{name: "truncated-property-value", body: []byte{0, 1, 0, 1}, want: ErrMalformedPacket},
		{name: "unsupported-property", body: []byte{0, 1, 0, 1, 0x00}, want: ErrUnsupportedProperty},
		{name: "zero-packet-id", body: []byte{0, 0}, want: ErrMalformedPacket},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, consumed, err := decoder.Decode(testFrame(0x40, testCase.body), ProtocolV5)
			if !errors.Is(err, testCase.want) || consumed != 0 {
				t.Fatalf("Decode() = (consumed %d, error %v), want (0, %v)", consumed, err, testCase.want)
			}
		})
	}
}

func TestAcknowledgementValidationAndFlags_MQTT311_MQTT5(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name   string
		header byte
	}{
		{name: "puback-reserved-flags", header: 0x41},
		{name: "pubrel-missing-required-flag", header: 0x60},
		{name: "pubrel-wrong-flags", header: 0x63},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, consumed, err := decoder.Decode(testFrame(testCase.header, []byte{0, 1}), ProtocolV311)
			if !errors.Is(err, ErrMalformedPacket) || consumed != 0 {
				t.Fatalf("Decode() = (consumed %d, error %v), want malformed", consumed, err)
			}
		})
	}
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, packet := range []Packet{
		&PubAck{Version: ProtocolV311},
		&PubAck{Version: ProtocolV311, PacketID: 1, ReasonCode: 1},
		&PubRel{Version: ProtocolV5, PacketID: 1, ReasonCode: 1},
		&PubComp{Version: ProtocolV5, PacketID: 1, ReasonCode: 0x91},
	} {
		if _, err := encoder.Append(nil, packet); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("Append(%T) error = %v, want malformed", packet, err)
		}
	}
}

func acknowledgementReason(packet Packet) byte {
	switch value := packet.(type) {
	case *PubAck:
		return value.ReasonCode
	case *PubRec:
		return value.ReasonCode
	case *PubRel:
		return value.ReasonCode
	case *PubComp:
		return value.ReasonCode
	default:
		return 0xFF
	}
}
