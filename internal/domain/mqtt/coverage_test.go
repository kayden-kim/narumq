package mqtt

import (
	"bytes"
	"errors"
	"testing"
)

// Traceability: PRD MQTT-08/MQTT-12/TEST-03/04; MQTT-3.8.3-4, MQTT-3.9.3-1/2,
// MQTT-3.10.3-1/2, and MQTT-3.11.3-1/2.
func TestCmmSubscriptionFamilyDecodeRejectsMalformedBodies(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		header  byte
		body    []byte
		version ProtocolVersion
		wantErr error
	}{
		{name: "subscribe-v5-qos-three", header: 0x82, body: []byte{0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x03}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "subscribe-v311-qos-three", header: 0x82, body: []byte{0x00, 0x01, 0x00, 0x01, 'a', 0x03}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "suback-v311-missing-reasons", header: 0x90, body: []byte{0x00, 0x01}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "suback-v5-missing-reasons", header: 0x90, body: []byte{0x00, 0x01, 0x00}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "suback-v5-unknown-reason", header: 0x90, body: []byte{0x00, 0x01, 0x00, 0x00, 0x03}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "suback-v311-v5-only-reason", header: 0x90, body: []byte{0x00, 0x01, 0x97}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-v311-truncated-id", header: 0xA2, body: []byte{0x00}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-v5-id-zero", header: 0xA2, body: []byte{0x00, 0x00, 0x00, 0x00, 0x01, 'a'}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-v5-truncated-properties", header: 0xA2, body: []byte{0x00, 0x01}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-v5-unsupported-property", header: 0xA2, body: []byte{0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 'a'}, version: ProtocolV5, wantErr: ErrUnsupportedProperty},
		{name: "unsubscribe-v5-truncated-filter", header: 0xA2, body: []byte{0x00, 0x01, 0x00, 0x00, 0x03, 'a'}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-v311-empty-filter", header: 0xA2, body: []byte{0x00, 0x01, 0x00, 0x00}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-v311-invalid-utf8-filter", header: 0xA2, body: []byte{0x00, 0x01, 0x00, 0x01, 0xFF}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "unsuback-v5-truncated-id", header: 0xB0, body: []byte{0x00}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "unsuback-v311-id-zero", header: 0xB0, body: []byte{0x00, 0x00}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "unsuback-v5-truncated-properties", header: 0xB0, body: []byte{0x00, 0x01}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "unsuback-v5-unsupported-property", header: 0xB0, body: []byte{0x00, 0x01, 0x01, 0x00, 0x00}, version: ProtocolV5, wantErr: ErrUnsupportedProperty},
		{name: "unsuback-v5-subscribe-only-reason", header: 0xB0, body: []byte{0x00, 0x01, 0x00, 0x01}, version: ProtocolV5, wantErr: ErrMalformedPacket},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			packet, consumed, err := decoder.Decode(testFrame(testCase.header, testCase.body), testCase.version)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("Decode() error = %v, want %v", err, testCase.wantErr)
			}
			if packet != nil || consumed != 0 {
				t.Fatalf("Decode() = (%#v, %d), want no packet and zero consumed", packet, consumed)
			}
		})
	}
}

// Traceability: PRD MQTT-08/TEST-03/04; MQTT-3.8.3-4 and MQTT-3.9.3-2.
func TestCmmSubscriptionOptionsRejectQoSThree(t *testing.T) {
	for _, version := range []ProtocolVersion{ProtocolV311, ProtocolV5} {
		subscription, err := decodeSubscriptionOptions("topic", 0x03, version)
		if !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("decodeSubscriptionOptions(0x03, %d) error = %v, want malformed", version, err)
		}
		if subscription != (Subscription{}) {
			t.Fatalf("decodeSubscriptionOptions(0x03, %d) = %#v, want zero subscription", version, subscription)
		}
	}
	decoded, err := decodeSubscriptionOptions("topic", 0x02, ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.QoS != QoSExactlyOnce || decoded.Filter != "topic" {
		t.Fatalf("decodeSubscriptionOptions(0x02) = %#v, want QoS 2 on topic", decoded)
	}
}

// Traceability: PRD MQTT-08/TEST-03/04; docs/specs/mqtt-protocol-and-session.md §3.
func TestCmmSubscriptionFamilyEncodeRejectsInvalidPackets(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	valid := []Subscription{{Filter: "a", QoS: QoSAtMostOnce}}
	cases := []struct {
		name    string
		packet  Packet
		wantErr error
	}{
		{name: "subscribe-unknown-version", packet: &Subscribe{Version: ProtocolUnknown, PacketID: 1, Subscriptions: valid}, wantErr: ErrUnsupportedProtocolVersion},
		{name: "subscribe-level-three", packet: Subscribe{Version: 3, PacketID: 1, Subscriptions: valid}, wantErr: ErrUnsupportedProtocolVersion},
		{name: "subscribe-id-zero", packet: &Subscribe{Version: ProtocolV5, Subscriptions: valid}, wantErr: ErrMalformedPacket},
		{name: "subscribe-no-subscriptions", packet: &Subscribe{Version: ProtocolV311, PacketID: 1}, wantErr: ErrMalformedPacket},
		{name: "subscribe-v311-no-local", packet: &Subscribe{Version: ProtocolV311, PacketID: 1, Subscriptions: []Subscription{{Filter: "a", NoLocal: true}}}, wantErr: ErrMalformedPacket},
		{name: "suback-unknown-version", packet: &SubAck{Version: ProtocolUnknown, PacketID: 1, ReasonCodes: []byte{0x00}}, wantErr: ErrUnsupportedProtocolVersion},
		{name: "suback-id-zero", packet: SubAck{Version: ProtocolV5, ReasonCodes: []byte{0x00}}, wantErr: ErrMalformedPacket},
		{name: "suback-no-reasons", packet: &SubAck{Version: ProtocolV5, PacketID: 1}, wantErr: ErrMalformedPacket},
		{name: "suback-v311-v5-only-reason", packet: &SubAck{Version: ProtocolV311, PacketID: 1, ReasonCodes: []byte{0x00, 0x87}}, wantErr: ErrMalformedPacket},
		{name: "suback-v5-unknown-reason", packet: &SubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0x03}}, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-unknown-version", packet: &Unsubscribe{Version: ProtocolUnknown, PacketID: 1, Filters: []string{"a"}}, wantErr: ErrUnsupportedProtocolVersion},
		{name: "unsubscribe-id-zero", packet: Unsubscribe{Version: ProtocolV5, Filters: []string{"a"}}, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-no-filters", packet: &Unsubscribe{Version: ProtocolV311, PacketID: 1}, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-empty-filter", packet: &Unsubscribe{Version: ProtocolV5, PacketID: 1, Filters: []string{"a", ""}}, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-invalid-utf8-filter", packet: &Unsubscribe{Version: ProtocolV311, PacketID: 1, Filters: []string{"\xff"}}, wantErr: ErrMalformedPacket},
		{name: "unsuback-unknown-version", packet: &UnsubAck{Version: ProtocolUnknown, PacketID: 1}, wantErr: ErrUnsupportedProtocolVersion},
		{name: "unsuback-id-zero", packet: UnsubAck{Version: ProtocolV5, ReasonCodes: []byte{0x00}}, wantErr: ErrMalformedPacket},
		{name: "unsuback-v311-with-reasons", packet: &UnsubAck{Version: ProtocolV311, PacketID: 1, ReasonCodes: []byte{0x00}}, wantErr: ErrMalformedPacket},
		{name: "unsuback-v5-subscribe-only-reason", packet: &UnsubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0x00, 0x02}}, wantErr: ErrMalformedPacket},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			destination := []byte{0xCA, 0xFE}
			before := append([]byte(nil), destination...)
			got, err := encoder.Append(destination, testCase.packet)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("Append() error = %v, want %v", err, testCase.wantErr)
			}
			if !bytes.Equal(got, before) {
				t.Fatalf("Append() destination = % X, want unchanged % X", got, before)
			}
		})
	}
}

// Traceability: PRD MQTT-08/TEST-03/04; MQTT-3.9.3-2.
func TestCmmSubscribeAckReasonProfileByVersion(t *testing.T) {
	cases := []struct {
		name       string
		version    ProtocolVersion
		reasonCode byte
		want       bool
	}{
		{name: "unknown-version-success", version: ProtocolUnknown, reasonCode: 0x00, want: false},
		{name: "level-three-failure", version: 3, reasonCode: 0x80, want: false},
		{name: "v311-granted-qos2", version: ProtocolV311, reasonCode: 0x02, want: true},
		{name: "v311-v5-only-quota", version: ProtocolV311, reasonCode: 0x97, want: false},
		{name: "v5-quota", version: ProtocolV5, reasonCode: 0x97, want: true},
		{name: "v5-wildcards-unsupported", version: ProtocolV5, reasonCode: 0xA2, want: true},
		{name: "v5-no-subscription-existed", version: ProtocolV5, reasonCode: 0x11, want: false},
		{name: "v5-qos-three", version: ProtocolV5, reasonCode: 0x03, want: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := validSubscribeAckReason(testCase.version, testCase.reasonCode); got != testCase.want {
				t.Fatalf("validSubscribeAckReason(%d, 0x%02X) = %t, want %t", testCase.version, testCase.reasonCode, got, testCase.want)
			}
		})
	}
}
