package mqtt

import (
	"bytes"
	"errors"
	"testing"
)

// Traceability: PRD MQTT-06/MQTT-08/TEST-03/04; MQTT-3.8.3-1/2,
// MQTT-3.9.3-1/2, MQTT-3.10.3-1/2, and MQTT-3.11.3-1/2.
func TestSubscriptionPacketsRoundTrip_MQTT311_MQTT5(t *testing.T) {
	cases := []struct {
		name    string
		packet  Packet
		version ProtocolVersion
	}{
		{
			name: "subscribe-v311",
			packet: &Subscribe{
				Version:  ProtocolV311,
				PacketID: 1,
				Subscriptions: []Subscription{
					{Filter: "sensors/+/temp", QoS: QoSAtLeastOnce},
					{Filter: "alerts/#", QoS: QoSExactlyOnce},
				},
			},
			version: ProtocolV311,
		},
		{
			name: "subscribe-v5",
			packet: &Subscribe{
				Version:  ProtocolV5,
				PacketID: 2,
				Subscriptions: []Subscription{
					{
						Filter:            "sensors/room1/temp",
						QoS:               QoSAtLeastOnce,
						NoLocal:           true,
						RetainAsPublished: true,
						RetainHandling:    SendRetainedIfNew,
					},
					{Filter: "alerts/#", QoS: QoSExactlyOnce, RetainHandling: DoNotSendRetained},
				},
			},
			version: ProtocolV5,
		},
		{
			name:    "suback-v311",
			packet:  &SubAck{Version: ProtocolV311, PacketID: 3, ReasonCodes: []byte{0x00, 0x01, 0x02, 0x80}},
			version: ProtocolV311,
		},
		{
			name:    "suback-v5",
			packet:  &SubAck{Version: ProtocolV5, PacketID: 4, ReasonCodes: []byte{0x00, 0x83, 0x9E, 0xA2}},
			version: ProtocolV5,
		},
		{
			name:    "unsubscribe-v311",
			packet:  &Unsubscribe{Version: ProtocolV311, PacketID: 5, Filters: []string{"sensors/+/temp", "alerts/#"}},
			version: ProtocolV311,
		},
		{
			name:    "unsubscribe-v5",
			packet:  &Unsubscribe{Version: ProtocolV5, PacketID: 6, Filters: []string{"sensors/+/temp", "$share/team/alerts/#"}},
			version: ProtocolV5,
		},
		{
			name:    "unsuback-v311",
			packet:  &UnsubAck{Version: ProtocolV311, PacketID: 7},
			version: ProtocolV311,
		},
		{
			name:    "unsuback-v5",
			packet:  &UnsubAck{Version: ProtocolV5, PacketID: 8, ReasonCodes: []byte{0x00, 0x11, 0x8F}},
			version: ProtocolV5,
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
			encoded := testEncode(t, encoder, testCase.packet)
			decoded := testDecode(t, decoder, encoded, testCase.version)
			requireSemanticEqual(t, testCase.packet, decoded)
		})
	}
}

// Traceability: PRD MQTT-01/MQTT-06/MQTT-08/TEST-03/04; docs/specs/mqtt-protocol-and-session.md §3 and §4; MQTT-3.8.3-1/2 and MQTT-3.9.3-1/2.
func TestSubscriptionPacketsCanonicalWireAndFlags(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		packet Packet
		want   []byte
	}{
		{
			name:   "subscribe-v311",
			packet: &Subscribe{Version: ProtocolV311, PacketID: 1, Subscriptions: []Subscription{{Filter: "a", QoS: QoSAtLeastOnce}}},
			want:   []byte{0x82, 0x06, 0x00, 0x01, 0x00, 0x01, 'a', 0x01},
		},
		{
			name:   "subscribe-v5",
			packet: &Subscribe{Version: ProtocolV5, PacketID: 1, Subscriptions: []Subscription{{Filter: "a", QoS: QoSAtLeastOnce, NoLocal: true, RetainAsPublished: true, RetainHandling: DoNotSendRetained}}},
			want:   []byte{0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x2D},
		},
		{
			name:   "suback-v311",
			packet: &SubAck{Version: ProtocolV311, PacketID: 1, ReasonCodes: []byte{0x00}},
			want:   []byte{0x90, 0x03, 0x00, 0x01, 0x00},
		},
		{
			name:   "suback-v5",
			packet: &SubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0x00}},
			want:   []byte{0x90, 0x04, 0x00, 0x01, 0x00, 0x00},
		},
		{
			name:   "unsubscribe-v311",
			packet: &Unsubscribe{Version: ProtocolV311, PacketID: 1, Filters: []string{"a"}},
			want:   []byte{0xA2, 0x05, 0x00, 0x01, 0x00, 0x01, 'a'},
		},
		{
			name:   "unsubscribe-v5",
			packet: &Unsubscribe{Version: ProtocolV5, PacketID: 1, Filters: []string{"a"}},
			want:   []byte{0xA2, 0x06, 0x00, 0x01, 0x00, 0x00, 0x01, 'a'},
		},
		{
			name:   "unsuback-v311",
			packet: &UnsubAck{Version: ProtocolV311, PacketID: 1},
			want:   []byte{0xB0, 0x02, 0x00, 0x01},
		},
		{
			name:   "unsuback-v5",
			packet: &UnsubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0x00}},
			want:   []byte{0xB0, 0x04, 0x00, 0x01, 0x00, 0x00},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := testEncode(t, encoder, testCase.packet)
			if !bytes.Equal(got, testCase.want) {
				t.Fatalf("wire = % X, want % X", got, testCase.want)
			}
		})
	}
}

// Traceability: PRD MQTT-08/MQTT-12/TEST-03/04; MQTT-1.5.3-1/2, MQTT-1.5.5-1, and MQTT-3.8.3-1/2.
func TestSubscriptionDecodeValidationAndPropertyProfile(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		frame   []byte
		version ProtocolVersion
		wantErr error
	}{
		{name: "subscribe-fixed-flags-zero", frame: []byte{0x80, 0x00}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "subscribe-fixed-flags-four", frame: []byte{0x84, 0x00}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "suback-fixed-flags-two", frame: []byte{0x92, 0x00}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "unsubscribe-fixed-flags-zero", frame: []byte{0xA0, 0x00}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "subscribe-id-zero", frame: []byte{0x82, 0x05, 0x00, 0x00, 0x00, 0x01, 'a', 0x00}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "subscribe-empty-payload", frame: []byte{0x82, 0x02, 0x00, 0x01}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "subscribe-v311-options", frame: []byte{0x82, 0x05, 0x00, 0x01, 0x00, 0x01, 'a', 0x04}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "subscribe-v5-truncated-filter-utf8", frame: []byte{0x82, 0x05, 0x00, 0x01, 0x00, 0x03, 'a'}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "subscribe-v5-reserved-options", frame: []byte{0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0xC0}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "subscribe-v5-reserved-retain-handling", frame: []byte{0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x30}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "subscribe-v5-truncated-properties", frame: []byte{0x82, 0x02, 0x00, 0x01}, version: ProtocolV5, wantErr: ErrMalformedPacket},
		{name: "subscribe-v5-unsupported-property", frame: []byte{0x82, 0x04, 0x00, 0x01, 0x01, 0x00}, version: ProtocolV5, wantErr: ErrUnsupportedProperty},
		{name: "suback-invalid-reason", frame: []byte{0x90, 0x03, 0x00, 0x01, 0x03}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "unsuback-v311-reason", frame: []byte{0xB0, 0x03, 0x00, 0x01, 0x00}, version: ProtocolV311, wantErr: ErrMalformedPacket},
		{name: "unsuback-v5-empty-reasons", frame: []byte{0xB0, 0x03, 0x00, 0x01, 0x00}, version: ProtocolV5, wantErr: ErrMalformedPacket},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, consumed, err := decoder.Decode(testCase.frame, testCase.version)
			if consumed != 0 {
				t.Fatalf("consumed = %d, want zero", consumed)
			}
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("error = %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

// Traceability: PRD MQTT-11/MQTT-12/TEST-01/04; docs/specs/mqtt-protocol-and-session.md §7.
func TestSubscriptionOwnershipAndDestinationInvariant(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	packet := &Subscribe{Version: ProtocolV5, PacketID: 1, Subscriptions: []Subscription{{Filter: "a/+/c", QoS: QoSAtLeastOnce}}}
	wire := testEncode(t, encoder, packet)
	decoded := testDecode(t, decoder, wire, ProtocolV5).(*Subscribe)
	for index := range wire {
		wire[index] = 'x'
	}
	if decoded.Subscriptions[0].Filter != "a/+/c" {
		t.Fatalf("decoded filter changed after source mutation: %q", decoded.Subscriptions[0].Filter)
	}

	reasons := &SubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0x00, 0x83}}
	reasonWire := testEncode(t, encoder, reasons)
	decodedReasons := testDecode(t, decoder, reasonWire, ProtocolV5).(*SubAck)
	for index := range reasonWire {
		reasonWire[index] = 'x'
	}
	if !bytes.Equal(decodedReasons.ReasonCodes, []byte{0x00, 0x83}) {
		t.Fatalf("decoded reasons changed after source mutation: % X", decodedReasons.ReasonCodes)
	}

	destination := []byte{0xCA, 0xFE}
	before := append([]byte(nil), destination...)
	got, err := encoder.Append(destination, &Subscribe{Version: ProtocolV5, PacketID: 1, Subscriptions: []Subscription{{Filter: "", QoS: QoSAtMostOnce}}})
	if !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("invalid subscribe error = %v, want malformed", err)
	}
	if !bytes.Equal(got, before) {
		t.Fatalf("destination changed on error: got % X, want % X", got, before)
	}
}

func TestSubscriptionEncoderSizeAndVersionValidation(t *testing.T) {
	encoder, err := NewEncoder(3)
	if err != nil {
		t.Fatal(err)
	}
	cases := []Packet{
		&Subscribe{Version: ProtocolV5, PacketID: 1, Subscriptions: []Subscription{{Filter: "a"}}},
		&SubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0}},
		&Unsubscribe{Version: ProtocolV5, PacketID: 1, Filters: []string{"a"}},
		&UnsubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0}},
	}
	for _, packet := range cases {
		if _, err := encoder.Append(nil, packet); !errors.Is(err, ErrPacketTooLarge) {
			t.Errorf("Append(%T) error = %v, want packet too large", packet, err)
		}
	}
}
