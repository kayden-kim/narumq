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
func TestAcknowledgementRejectsInvalidBodiesAndReasons(t *testing.T) {
	invalidBodies := [][]byte{{}, {0, 0}, {0, 1, 0x01}, {0, 1, 0x00, 0x01}, {0, 1, 0x00, 0x01, 0x00}}
	for _, body := range invalidBodies {
		if _, _, err := decodeAcknowledgementBody(PacketTypePubAck, body, ProtocolV5); err == nil {
			t.Fatalf("decodeAcknowledgementBody(% X) unexpectedly succeeded", body)
		}
	}
	if _, _, err := decodeAcknowledgementBody(PacketTypePubAck, []byte{0, 1}, ProtocolUnknown); !errors.Is(err, ErrUnsupportedProtocolVersion) {
		t.Fatalf("unknown acknowledgement version error = %v", err)
	}
	for _, packetType := range []PacketType{PacketTypePubAck, PacketTypePubRec, PacketTypePubRel, PacketTypePubComp, PacketTypeConnect} {
		if _, err := encodeAcknowledgement(packetType, 1, ProtocolV5, 0xFF, 64); err == nil {
			t.Fatalf("encodeAcknowledgement(%d) accepted invalid reason", packetType)
		}
	}
	if _, err := encodeAcknowledgement(PacketTypePubAck, 1, ProtocolV5, 0x10, 2); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("acknowledgement size error = %v, want ErrPacketTooLarge", err)
	}
	if _, err := decodeAcknowledgement(PacketTypeConnect, []byte{0, 1}, ProtocolV5); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("unknown acknowledgement packet error = %v, want ErrMalformedPacket", err)
	}
}

func TestAcknowledgementAndConnectPropertyEdgeCases(t *testing.T) {
	if _, _, err := decodeAcknowledgementBody(PacketTypePubAck, []byte{0, 1, 0}, ProtocolV311); err == nil {
		t.Fatal("MQTT 3.1.1 acknowledgement accepted a reason byte")
	}
	if _, _, err := decodeAcknowledgementBody(PacketTypePubAck, []byte{0, 1, 0xFF, 0}, ProtocolV5); err == nil {
		t.Fatal("acknowledgement accepted invalid reason code")
	}
	if _, _, err := decodeAcknowledgementBody(PacketTypePubAck, []byte{0, 1, 0, 0x80}, ProtocolV5); err == nil {
		t.Fatal("acknowledgement accepted malformed property length")
	}
	if _, _, err := decodeAcknowledgementBody(PacketTypePubAck, []byte{0, 1, 0, 0, 1}, ProtocolV5); err == nil {
		t.Fatal("acknowledgement accepted trailing property bytes")
	}
	if _, err := encodeAcknowledgement(PacketTypePubAck, 1, ProtocolUnknown, 0, 64); !errors.Is(err, ErrUnsupportedProtocolVersion) {
		t.Fatalf("unsupported acknowledgement version = %v", err)
	}
	position := 0
	if _, err := decodeConnectProperties([]byte{0x80}, &position); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("malformed CONNECT properties = %v", err)
	}
	position = 0
	if _, err := decodeConnectProperties([]byte{4, 0, 0}, &position); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("truncated CONNECT properties = %v", err)
	}
}
