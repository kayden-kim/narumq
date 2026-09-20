package mqtt

import (
	"errors"
	"testing"
)

func TestDecoderRejectsReservedPacketTypeAndControlVersion(t *testing.T) {
	decoder, err := NewDecoder(64)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decoder.Decode([]byte{0xF0, 0}, ProtocolV5); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("reserved packet type error = %v", err)
	}
	if _, err := encodeDisconnect(Disconnect{Version: ProtocolUnknown}, 64); !errors.Is(err, ErrUnsupportedProtocolVersion) {
		t.Fatalf("unsupported DISCONNECT version = %v", err)
	}
	if _, err := encodePingReq(PingReq{Version: ProtocolUnknown}, 64); !errors.Is(err, ErrUnsupportedProtocolVersion) {
		t.Fatalf("unsupported PINGREQ version = %v", err)
	}
}
