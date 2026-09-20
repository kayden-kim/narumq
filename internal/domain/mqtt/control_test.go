package mqtt

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

// Traceability: PRD MQTT-01/MQTT-11/TEST-03; docs/specs/mqtt-protocol-and-session.md §1 and §7.
func TestControlPacketsRoundTripAndCanonicalForms(t *testing.T) {
	cases := []struct {
		name    string
		packet  Packet
		version ProtocolVersion
		wire    []byte
	}{
		{name: "pingreq-v311", packet: &PingReq{Version: ProtocolV311}, version: ProtocolV311, wire: []byte{0xC0, 0x00}},
		{name: "pingreq-v5", packet: &PingReq{Version: ProtocolV5}, version: ProtocolV5, wire: []byte{0xC0, 0x00}},
		{name: "pingresp-v311", packet: &PingResp{Version: ProtocolV311}, version: ProtocolV311, wire: []byte{0xD0, 0x00}},
		{name: "pingresp-v5", packet: &PingResp{Version: ProtocolV5}, version: ProtocolV5, wire: []byte{0xD0, 0x00}},
		{name: "disconnect-v311", packet: &Disconnect{Version: ProtocolV311}, version: ProtocolV311, wire: []byte{0xE0, 0x00}},
		{name: "disconnect-v5-normal", packet: &Disconnect{Version: ProtocolV5}, version: ProtocolV5, wire: []byte{0xE0, 0x00}},
		{name: "disconnect-v5-reason", packet: &Disconnect{Version: ProtocolV5, ReasonCode: 0x83}, version: ProtocolV5, wire: []byte{0xE0, 0x02, 0x83, 0x00}},
		{name: "disconnect-v5-payload-format-invalid", packet: &Disconnect{Version: ProtocolV5, ReasonCode: 0x99}, version: ProtocolV5, wire: []byte{0xE0, 0x02, 0x99, 0x00}},
	}
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			encoded, err := encoder.Append(nil, testCase.packet)
			if err != nil {
				t.Fatalf("Append() error = %v", err)
			}
			if !bytes.Equal(encoded, testCase.wire) {
				t.Fatalf("encoded = % X, want % X", encoded, testCase.wire)
			}
			decoded, consumed, err := decoder.Decode(testCase.wire, testCase.version)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if consumed != len(testCase.wire) {
				t.Fatalf("Decode() consumed %d bytes, want %d", consumed, len(testCase.wire))
			}
			requireSemanticEqual(t, testCase.packet, decoded)
		})
	}

	decoded, consumed, err := decoder.Decode([]byte{0xE0, 0x01, 0x00}, ProtocolV5)
	if err != nil || consumed != 3 {
		t.Fatalf("DISCONNECT reason-only decode = (%d, %v), want (3, nil)", consumed, err)
	}
	requireSemanticEqual(t, &Disconnect{Version: ProtocolV5}, decoded)
}

func TestDisconnectReasonAllowlist(t *testing.T) {
	reasons := []byte{
		0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8D,
		0x8E, 0x8F, 0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99,
		0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2,
	}
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	for _, reason := range reasons {
		t.Run(fmt.Sprintf("reason-%02X", reason), func(t *testing.T) {
			packet := &Disconnect{Version: ProtocolV5, ReasonCode: reason}
			wire, err := encoder.Append(nil, packet)
			if err != nil {
				t.Fatalf("Append() error = %v", err)
			}
			decoded, consumed, err := decoder.Decode(wire, ProtocolV5)
			if err != nil || consumed != len(wire) {
				t.Fatalf("Decode() = (%d, %v), want %d, nil", consumed, err, len(wire))
			}
			requireSemanticEqual(t, packet, decoded)
		})
	}
}

// Traceability: PRD MQTT-01/MQTT-11/TEST-03/04; MQTT-3.12.4-1/2
// and MQTT-3.14.4-1/2; invalid control frames must fail without reporting
// a partial frame.
func TestControlPacketsRejectInvalidFramesAndFlags(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		wire    []byte
		version ProtocolVersion
		wantErr error
	}{
		{name: "pingreq-body", wire: []byte{0xC0, 0x01, 0x00}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "pingresp-flag", wire: []byte{0xD1, 0x00}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "disconnect-flag", wire: []byte{0xE1, 0x00}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "disconnect-v311-body", wire: []byte{0xE0, 0x01, 0x00}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "disconnect-invalid-reason", wire: []byte{0xE0, 0x01, 0x01}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "disconnect-property-truncated", wire: []byte{0xE0, 0x02, 0x83, 0x01}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "disconnect-property-unsupported", wire: []byte{0xE0, 0x03, 0x83, 0x01, 0x00}, version: ProtocolV5, wantErr: ErrUnsupportedProperty},
		{name: "disconnect-noncanonical-property-length", wire: []byte{0xE0, 0x03, 0x83, 0x80, 0x00}, version: ProtocolV5, wantErr: ErrMalformedPacket},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			packet, consumed, err := decoder.Decode(testCase.wire, testCase.version)
			if packet != nil || consumed != 0 || !errors.Is(err, testCase.wantErr) {
				t.Fatalf("Decode() = (%#v, %d, %v), want nil, 0, %v", packet, consumed, err, testCase.wantErr)
			}
		})
	}
}

func TestControlPacketEncodingPreservesDestinationOnFailure(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	cases := []Packet{
		&PingReq{Version: ProtocolUnknown},
		&PingResp{Version: ProtocolUnknown},
		&Disconnect{Version: ProtocolV311, ReasonCode: 0x04},
		&Disconnect{Version: ProtocolV5, ReasonCode: 0x01},
	}
	for _, packet := range cases {
		destination := []byte{0xAA, 0xBB}
		before := append([]byte(nil), destination...)
		encoded, err := encoder.Append(destination, packet)
		if err == nil {
			t.Fatalf("Append(%T) unexpectedly succeeded", packet)
		}
		if !bytes.Equal(encoded, before) {
			t.Fatalf("Append(%T) changed destination to % X, want % X", packet, encoded, before)
		}
	}
}
func TestControlCodecRejectsVersionsAndSizeBounds(t *testing.T) {
	for _, version := range []ProtocolVersion{ProtocolUnknown, 3} {
		if _, err := decodePingReq(nil, version); !errors.Is(err, ErrUnsupportedProtocolVersion) {
			t.Fatalf("decodePingReq(%d) error = %v", version, err)
		}
		if _, err := decodePingResp(nil, version); !errors.Is(err, ErrUnsupportedProtocolVersion) {
			t.Fatalf("decodePingResp(%d) error = %v", version, err)
		}
		if _, err := decodeDisconnect(nil, version); !errors.Is(err, ErrUnsupportedProtocolVersion) {
			t.Fatalf("decodeDisconnect(%d) error = %v", version, err)
		}
	}
	if _, err := decodePingReq([]byte{0}, ProtocolV5); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("PINGREQ body error = %v", err)
	}
	if _, err := decodePingResp([]byte{0}, ProtocolV5); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("PINGRESP body error = %v", err)
	}
	if _, err := encodePingReq(PingReq{Version: ProtocolV5}, -1); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("PINGREQ negative size error = %v", err)
	}
	if _, err := encodePingResp(PingResp{Version: ProtocolV5}, -1); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("PINGRESP negative size error = %v", err)
	}
	if _, err := encodeDisconnect(Disconnect{Version: ProtocolV5, ReasonCode: 0x04}, 1); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("DISCONNECT size error = %v", err)
	}
	if _, err := decodeDisconnect([]byte{0x00, 0x80, 0x00}, ProtocolV5); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("noncanonical DISCONNECT property length error = %v", err)
	}
}
