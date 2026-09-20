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
	case *Subscribe:
		return *value
	case *SubAck:
		return *value
	case *Unsubscribe:
		return *value
	case *UnsubAck:
		return *value
	case *PubAck:
		return *value
	case *PubRec:
		return *value
	case *PubRel:
		return *value
	case *PubComp:
		return *value
	case *PingReq:
		return *value
	case *PingResp:
		return *value
	case *Disconnect:
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
		if !ok || wantValue.Version != gotValue.Version || wantValue.CleanStart != gotValue.CleanStart || wantValue.SessionExpiryInterval != gotValue.SessionExpiryInterval || wantValue.KeepAlive != gotValue.KeepAlive || wantValue.ClientID != gotValue.ClientID || wantValue.HasUsername != gotValue.HasUsername || wantValue.Username != gotValue.Username || wantValue.HasPassword != gotValue.HasPassword || !bytes.Equal(wantValue.Password, gotValue.Password) || !willsEqual(wantValue.Will, gotValue.Will) {
			t.Fatalf("CONNECT mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *ConnAck:
		gotValue, ok := got.(*ConnAck)
		if !ok || *wantValue != *gotValue {
			t.Fatalf("CONNACK mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *Publish:
		gotValue, ok := got.(*Publish)
		if !ok || wantValue.Version != gotValue.Version || wantValue.DUP != gotValue.DUP || wantValue.QoS != gotValue.QoS || wantValue.Retain != gotValue.Retain || wantValue.PacketID != gotValue.PacketID || wantValue.Topic != gotValue.Topic || !bytes.Equal(wantValue.Payload, gotValue.Payload) || wantValue.HasMessageExpiryInterval != gotValue.HasMessageExpiryInterval || wantValue.MessageExpiryInterval != gotValue.MessageExpiryInterval {
			t.Fatalf("PUBLISH mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *Subscribe:
		gotValue, ok := got.(*Subscribe)
		if !ok || wantValue.Version != gotValue.Version || wantValue.PacketID != gotValue.PacketID || !subscriptionsEqual(wantValue.Subscriptions, gotValue.Subscriptions) {
			t.Fatalf("SUBSCRIBE mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *SubAck:
		gotValue, ok := got.(*SubAck)
		if !ok || wantValue.Version != gotValue.Version || wantValue.PacketID != gotValue.PacketID || !bytes.Equal(wantValue.ReasonCodes, gotValue.ReasonCodes) {
			t.Fatalf("SUBACK mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *Unsubscribe:
		gotValue, ok := got.(*Unsubscribe)
		if !ok || wantValue.Version != gotValue.Version || wantValue.PacketID != gotValue.PacketID || !stringSlicesEqual(wantValue.Filters, gotValue.Filters) {
			t.Fatalf("UNSUBSCRIBE mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *UnsubAck:
		gotValue, ok := got.(*UnsubAck)
		if !ok || wantValue.Version != gotValue.Version || wantValue.PacketID != gotValue.PacketID || !bytes.Equal(wantValue.ReasonCodes, gotValue.ReasonCodes) {
			t.Fatalf("UNSUBACK mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *PubAck:
		gotValue, ok := got.(*PubAck)
		if !ok || *wantValue != *gotValue {
			t.Fatalf("PUBACK mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *PubRec:
		gotValue, ok := got.(*PubRec)
		if !ok || *wantValue != *gotValue {
			t.Fatalf("PUBREC mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *PubRel:
		gotValue, ok := got.(*PubRel)
		if !ok || *wantValue != *gotValue {
			t.Fatalf("PUBREL mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *PubComp:
		gotValue, ok := got.(*PubComp)
		if !ok || *wantValue != *gotValue {
			t.Fatalf("PUBCOMP mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *PingReq:
		gotValue, ok := got.(*PingReq)
		if !ok || *wantValue != *gotValue {
			t.Fatalf("PINGREQ mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *PingResp:
		gotValue, ok := got.(*PingResp)
		if !ok || *wantValue != *gotValue {
			t.Fatalf("PINGRESP mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	case *Disconnect:
		gotValue, ok := got.(*Disconnect)
		if !ok || *wantValue != *gotValue {
			t.Fatalf("DISCONNECT mismatch:\nwant %#v\ngot  %#v", wantValue, got)
		}
	default:
		t.Fatalf("unsupported packet type %T", want)
	}
}

func willsEqual(want *Will, got *Will) bool {
	if want == nil || got == nil {
		return want == got
	}
	return want.QoS == got.QoS && want.Retain == got.Retain && want.Topic == got.Topic && bytes.Equal(want.Payload, got.Payload) && want.DelayInterval == got.DelayInterval
}

func subscriptionsEqual(want []Subscription, got []Subscription) bool {
	if len(want) != len(got) {
		return false
	}
	for index := range want {
		if want[index] != got[index] {
			return false
		}
	}
	return true
}

func stringSlicesEqual(want []string, got []string) bool {
	if len(want) != len(got) {
		return false
	}
	for index := range want {
		if want[index] != got[index] {
			return false
		}
	}
	return true
}
