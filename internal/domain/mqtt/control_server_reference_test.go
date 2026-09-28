package mqtt

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// Traceability: DEP-06; docs/specs/drain-and-rolling-update.md §4, §10.
func TestDisconnectServerReferenceEncoding(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	packet := &Disconnect{Version: ProtocolV5, ReasonCode: 0x9C, ServerReference: "a:1"}
	wire, err := encoder.Append(nil, packet)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	want := []byte{0xE0, 0x08, 0x9C, 0x06, 0x1C, 0x00, 0x03, 'a', ':', '1'}
	if !bytes.Equal(wire, want) {
		t.Fatalf("encoded DISCONNECT = % X, want % X", wire, want)
	}
	body, err := encodeDisconnect(Disconnect{Version: ProtocolV5, ReasonCode: 0x9D, ServerReference: "a:1"}, 8)
	if err != nil || len(body) != 8 {
		t.Fatalf("permanent move body = % X, error = %v; want 8-byte body", body, err)
	}
	if _, err := encodeDisconnect(*packet, 7); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("undersized packet limit error = %v, want ErrPacketTooLarge", err)
	}
}

// Traceability: DEP-06; docs/specs/drain-and-rolling-update.md §4, §10.
func TestDisconnectRejectsInvalidServerReference(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   ProtocolVersion
		reason    byte
		reference string
	}{
		{name: "wrong reason", version: ProtocolV5, reason: 0x8B, reference: "a:1"},
		{name: "wrong protocol", version: ProtocolV311, reason: 0, reference: "a:1"},
		{name: "invalid utf8", version: ProtocolV5, reason: 0x9C, reference: string([]byte{0xff})},
		{name: "nul", version: ProtocolV5, reason: 0x9C, reference: "a\x00b"},
		{name: "oversized utf8 field", version: ProtocolV5, reason: 0x9C, reference: strings.Repeat("a", 65536)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := encodeDisconnect(Disconnect{
				Version: test.version, ReasonCode: test.reason, ServerReference: test.reference,
			}, maxRemainingLength); !errors.Is(err, ErrMalformedPacket) {
				t.Fatalf("encodeDisconnect() error = %v, want ErrMalformedPacket", err)
			}
		})
	}
	decoded, err := decodeDisconnect([]byte{0x9C, 0x06, 0x1C, 0, 3, 'a', ':', '1'}, ProtocolV5)
	if !errors.Is(err, ErrUnsupportedProperty) || decoded != nil {
		t.Fatalf("inbound Server Reference property = (%T, %v), want ErrUnsupportedProperty", decoded, err)
	}
	legacy, err := encodeDisconnect(Disconnect{Version: ProtocolV5, ReasonCode: 0x9C}, 2)
	if err != nil || !bytes.Equal(legacy, []byte{0x9C, 0x00}) {
		t.Fatalf("DISCONNECT without Server Reference = % X, %v; want 9C 00", legacy, err)
	}
}
