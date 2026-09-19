package mqtt

import (
	"bytes"
	"testing"
)

func testFrame(header byte, body []byte) []byte {
	frame := []byte{header}
	frame = appendVariableByteInteger(frame, len(body))
	return append(frame, body...)
}

func testDecode(t *testing.T, decoder *Decoder, frame []byte, version ProtocolVersion) Packet {
	t.Helper()
	packet, consumed, err := decoder.Decode(frame, version)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if consumed != len(frame) {
		t.Fatalf("Decode() consumed %d bytes, want %d", consumed, len(frame))
	}
	return packet
}

func testEncode(t *testing.T, encoder *Encoder, packet Packet) []byte {
	t.Helper()
	encoded, err := encoder.Append(nil, packet)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	return encoded
}

func packetValue(packet Packet) any {
	switch value := packet.(type) {
	case *Connect:
		return *value
	case *ConnAck:
		return *value
	case *Publish:
		return *value
	default:
		return packet
	}
}

func requireSemanticEqual(t *testing.T, want Packet, got Packet) {
	t.Helper()
	switch wantValue := want.(type) {
	case *Connect:
		gotValue, ok := got.(*Connect)
		if !ok || wantValue.Version != gotValue.Version || wantValue.CleanStart != gotValue.CleanStart || wantValue.KeepAlive != gotValue.KeepAlive || wantValue.ClientID != gotValue.ClientID || wantValue.HasUsername != gotValue.HasUsername || wantValue.Username != gotValue.Username || wantValue.HasPassword != gotValue.HasPassword || !bytes.Equal(wantValue.Password, gotValue.Password) || !willsEqual(wantValue.Will, gotValue.Will) {
			t.Fatalf("CONNECT mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *ConnAck:
		gotValue, ok := got.(*ConnAck)
		if !ok || *wantValue != *gotValue {
			t.Fatalf("CONNACK mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *Publish:
		gotValue, ok := got.(*Publish)
		if !ok || wantValue.Version != gotValue.Version || wantValue.DUP != gotValue.DUP || wantValue.Retain != gotValue.Retain || wantValue.Topic != gotValue.Topic || !bytes.Equal(wantValue.Payload, gotValue.Payload) {
			t.Fatalf("PUBLISH mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	default:
		t.Fatalf("unsupported packet type %T", want)
	}
}

func willsEqual(want *Will, got *Will) bool {
	if want == nil || got == nil {
		return want == got
	}
	return want.QoS == got.QoS && want.Retain == got.Retain && want.Topic == got.Topic && bytes.Equal(want.Payload, got.Payload)
}
