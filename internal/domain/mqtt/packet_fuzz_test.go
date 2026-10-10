package mqtt

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

// Traceability: PRD MQTT-12; malformed input must not panic or report partial consumption.
func FuzzDecoder(f *testing.F) {
	f.Add([]byte{0x10, 0x0D, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x00, 0x00, 0x01, 'a'}, byte(0))
	f.Add([]byte{0x20, 0x03, 0x00, 0x00, 0x00}, byte(2))
	f.Add([]byte{0x30, 0x08, 0x00, 0x01, 'a', 'h', 'e', 'l', 'l', 'o'}, byte(1))
	f.Add([]byte{0x30, 0x80}, byte(1))
	f.Add([]byte{0x32, 0x05, 0x00, 0x01, 'a', 0x00, 0x07}, byte(1))
	f.Add([]byte{0x50, 0x02, 0x00, 0x07}, byte(2))
	f.Add([]byte{0x62, 0x04, 0x00, 0x07, 0x92, 0x00}, byte(2))
	f.Add([]byte{0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x01}, byte(2))
	f.Add([]byte{0xC0, 0x00}, byte(1))
	f.Add([]byte{0xD0, 0x00}, byte(2))
	f.Add([]byte{0xE0, 0x02, 0x83, 0x00}, byte(2))

	decoder, err := NewDecoder(MaxRemainingLength)
	if err != nil {
		f.Fatalf("NewDecoder() error = %v", err)
	}
	f.Fuzz(func(t *testing.T, source []byte, versionByte byte) {
		version := fuzzVersion(versionByte)
		packet, consumed, err := decoder.Decode(source, version)
		if err != nil {
			if consumed != 0 {
				t.Fatalf("failed Decode() consumed %d bytes", consumed)
			}
			if !isDefinedCodecError(err) {
				t.Fatalf("undefined Decode() error: %v", err)
			}
			return
		}
		if packet == nil {
			t.Fatal("successful Decode() returned nil packet")
		}
		if consumed <= 0 || consumed > len(source) {
			t.Fatalf("successful Decode() consumed %d of %d bytes", consumed, len(source))
		}
		if packet.packetVersion() != ProtocolV311 && packet.packetVersion() != ProtocolV5 {
			t.Fatalf("successful packet has invalid version %d", packet.packetVersion())
		}
	})
}

// Traceability: PRD MQTT-12; successful codec round trips must preserve packet semantics.
func FuzzCodecRoundTrip(f *testing.F) {
	f.Add([]byte{0x10, 0x0D, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x00, 0x00, 0x01, 'a'}, byte(0))
	f.Add([]byte{0x20, 0x03, 0x00, 0x00, 0x00}, byte(2))
	f.Add([]byte{0x30, 0x0B, 0x00, 0x01, 'a', 0x05, 0x02, 0x00, 0x00, 0x00, 0x0A, 'h', 'i'}, byte(2))
	f.Add([]byte{0x10, 0x13, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 'a'}, byte(0))
	f.Add([]byte{0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 'a', 0x01}, byte(2))
	f.Add([]byte{0xC0, 0x00}, byte(1))
	f.Add([]byte{0xD0, 0x00}, byte(2))
	f.Add([]byte{0xE0, 0x02, 0x83, 0x00}, byte(2))

	decoder, err := NewDecoder(MaxRemainingLength)
	if err != nil {
		f.Fatalf("NewDecoder() error = %v", err)
	}
	encoder, err := NewEncoder(MaxRemainingLength)
	if err != nil {
		f.Fatalf("NewEncoder() error = %v", err)
	}
	f.Fuzz(func(t *testing.T, source []byte, versionByte byte) {
		packet, _, err := decoder.Decode(source, fuzzVersion(versionByte))
		if err != nil {
			if !isDefinedCodecError(err) {
				t.Fatalf("undefined Decode() error: %v", err)
			}
			return
		}
		encoded, err := encoder.Append(nil, packet)
		if err != nil {
			t.Fatalf("re-encoding decoded packet: %v", err)
		}
		decodedAgain, consumed, err := decoder.Decode(encoded, packet.packetVersion())
		if err != nil {
			t.Fatalf("re-decoding encoded packet: %v", err)
		}
		if consumed != len(encoded) {
			t.Fatalf("re-decoding consumed %d of %d bytes", consumed, len(encoded))
		}
		if !fuzzPacketsEqual(packet, decodedAgain) {
			t.Fatalf("round-trip changed packet:\nwant %#v\ngot  %#v", packetValue(packet), packetValue(decodedAgain))
		}
	})
}

func fuzzVersion(value byte) ProtocolVersion {
	switch value % 3 {
	case 1:
		return ProtocolV311
	case 2:
		return ProtocolV5
	default:
		return ProtocolUnknown
	}
}

func isDefinedCodecError(err error) bool {
	return errors.Is(err, ErrIncompletePacket) ||
		errors.Is(err, ErrMalformedPacket) ||
		errors.Is(err, ErrPacketTooLarge) ||
		errors.Is(err, ErrProtocolVersionRequired) ||
		errors.Is(err, ErrUnsupportedProtocolVersion) ||
		errors.Is(err, ErrUnsupportedProperty)
}

func fuzzPacketsEqual(left Packet, right Packet) bool {
	switch leftValue := left.(type) {
	case *Connect:
		rightValue, ok := right.(*Connect)
		if !ok {
			return false
		}
		return leftValue.Version == rightValue.Version && leftValue.CleanStart == rightValue.CleanStart && leftValue.SessionExpiryInterval == rightValue.SessionExpiryInterval && leftValue.ReceiveMaximum == rightValue.ReceiveMaximum && leftValue.MaximumPacketSize == rightValue.MaximumPacketSize && leftValue.KeepAlive == rightValue.KeepAlive && leftValue.ClientID == rightValue.ClientID && leftValue.HasUsername == rightValue.HasUsername && leftValue.Username == rightValue.Username && leftValue.HasPassword == rightValue.HasPassword && bytes.Equal(leftValue.Password, rightValue.Password) && fuzzWillsEqual(leftValue.Will, rightValue.Will)
	case *ConnAck:
		rightValue, ok := right.(*ConnAck)
		return ok && *leftValue == *rightValue
	case *Publish:
		rightValue, ok := right.(*Publish)
		return ok && leftValue.Version == rightValue.Version && leftValue.DUP == rightValue.DUP && leftValue.QoS == rightValue.QoS && leftValue.Retain == rightValue.Retain && leftValue.PacketID == rightValue.PacketID && leftValue.Topic == rightValue.Topic && bytes.Equal(leftValue.Payload, rightValue.Payload) && leftValue.HasMessageExpiryInterval == rightValue.HasMessageExpiryInterval && leftValue.MessageExpiryInterval == rightValue.MessageExpiryInterval
	case *Subscribe:
		rightValue, ok := right.(*Subscribe)
		return ok && leftValue.Version == rightValue.Version && leftValue.PacketID == rightValue.PacketID && subscriptionsEqual(leftValue.Subscriptions, rightValue.Subscriptions)
	case *SubAck:
		rightValue, ok := right.(*SubAck)
		return ok && leftValue.Version == rightValue.Version && leftValue.PacketID == rightValue.PacketID && bytes.Equal(leftValue.ReasonCodes, rightValue.ReasonCodes)
	case *Unsubscribe:
		rightValue, ok := right.(*Unsubscribe)
		return ok && leftValue.Version == rightValue.Version && leftValue.PacketID == rightValue.PacketID && stringSlicesEqual(leftValue.Filters, rightValue.Filters)
	case *UnsubAck:
		rightValue, ok := right.(*UnsubAck)
		return ok && leftValue.Version == rightValue.Version && leftValue.PacketID == rightValue.PacketID && bytes.Equal(leftValue.ReasonCodes, rightValue.ReasonCodes)
	case *PubAck:
		rightValue, ok := right.(*PubAck)
		return ok && *leftValue == *rightValue
	case *PubRec:
		rightValue, ok := right.(*PubRec)
		return ok && *leftValue == *rightValue
	case *PubRel:
		rightValue, ok := right.(*PubRel)
		return ok && *leftValue == *rightValue
	case *PubComp:
		rightValue, ok := right.(*PubComp)
		return ok && *leftValue == *rightValue
	case *PingReq:
		rightValue, ok := right.(*PingReq)
		return ok && *leftValue == *rightValue
	case *PingResp:
		rightValue, ok := right.(*PingResp)
		return ok && *leftValue == *rightValue
	case *Disconnect:
		rightValue, ok := right.(*Disconnect)
		return ok && *leftValue == *rightValue
	default:
		return reflect.DeepEqual(left, right)
	}
}

func fuzzWillsEqual(left *Will, right *Will) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.QoS == right.QoS && left.Retain == right.Retain && left.Topic == right.Topic && bytes.Equal(left.Payload, right.Payload) && left.DelayInterval == right.DelayInterval
}
