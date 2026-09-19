package mqtt

import (
	"bytes"
	"errors"
	"testing"
)

type unsupportedTestPacket struct{}

func (unsupportedTestPacket) packetType() PacketType {
	return PacketType(15)
}

func (unsupportedTestPacket) packetVersion() ProtocolVersion {
	return ProtocolV311
}

func TestConnectEdgeValidationAndContracts_MQTT01_MQTT12_TEST03(t *testing.T) {
	base := Connect{Version: ProtocolV311, CleanStart: true, ClientID: "client"}
	cases := []struct {
		name  string
		input Connect
		want  error
	}{
		{name: "unsupported-version", input: Connect{Version: ProtocolUnknown}, want: ErrUnsupportedProtocolVersion},
		{name: "invalid-client-utf8", input: func() Connect { value := base; value.ClientID = "bad\x00client"; return value }(), want: ErrMalformedPacket},
		{name: "empty-client-without-clean-start", input: Connect{Version: ProtocolV311, ClientID: ""}, want: ErrMalformedPacket},
		{name: "invalid-username-utf8", input: func() Connect { value := base; value.HasUsername = true; value.Username = "bad\x00user"; return value }(), want: ErrMalformedPacket},
		{name: "username-without-presence", input: func() Connect { value := base; value.Username = "unexpected"; return value }(), want: ErrMalformedPacket},
		{name: "oversized-username", input: func() Connect {
			value := base
			value.HasUsername = true
			value.Username = string(make([]byte, 65536))
			return value
		}(), want: ErrMalformedPacket},
		{name: "wildcard-will-topic", input: func() Connect { value := base; value.Will = &Will{Topic: "will/#"}; return value }(), want: ErrMalformedPacket},
		{name: "password-without-presence", input: func() Connect { value := base; value.Password = []byte{1}; return value }(), want: ErrMalformedPacket},
		{name: "password-without-username", input: func() Connect { value := base; value.HasPassword = true; return value }(), want: ErrMalformedPacket},
		{name: "oversized-password", input: func() Connect {
			value := base
			value.HasUsername = true
			value.Username = "user"
			value.HasPassword = true
			value.Password = make([]byte, 65536)
			return value
		}(), want: ErrMalformedPacket},
		{name: "invalid-will-qos", input: func() Connect { value := base; value.Will = &Will{QoS: 3, Topic: "will"}; return value }(), want: ErrMalformedPacket},
		{name: "empty-will-topic", input: func() Connect { value := base; value.Will = &Will{}; return value }(), want: ErrMalformedPacket},
		{name: "invalid-will-topic", input: func() Connect { value := base; value.Will = &Will{Topic: "bad\x00topic"}; return value }(), want: ErrMalformedPacket},
		{name: "oversized-will-payload", input: func() Connect {
			value := base
			value.Will = &Will{Topic: "will", Payload: make([]byte, 65536)}
			return value
		}(), want: ErrMalformedPacket},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := validateConnect(testCase.input)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("validateConnect() error = %v, want %v", err, testCase.want)
			}
		})
	}
}

func TestConnectProtocolVersionAndPacketContracts_MQTT01_MQTT13_MQTT312_MQTT313(t *testing.T) {
	cases := []struct {
		name         string
		protocolName string
		level        byte
		want         ProtocolVersion
		err          error
	}{
		{name: "v311", protocolName: "MQTT", level: 4, want: ProtocolV311},
		{name: "v5", protocolName: "MQTT", level: 5, want: ProtocolV5},
		{name: "legacy", protocolName: "MQIsdp", level: 3, err: ErrUnsupportedProtocolVersion},
		{name: "legacy-wrong-level", protocolName: "MQIsdp", level: 4, err: ErrMalformedPacket},
		{name: "unknown-name", protocolName: "Other", level: 4, err: ErrMalformedPacket},
		{name: "unknown-level", protocolName: "MQTT", level: 8, err: ErrUnsupportedProtocolVersion},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := connectProtocolVersion(testCase.protocolName, testCase.level)
			if testCase.err != nil {
				if !errors.Is(err, testCase.err) {
					t.Fatalf("error = %v, want %v", err, testCase.err)
				}
				return
			}
			if err != nil || got != testCase.want {
				t.Fatalf("connectProtocolVersion() = (%d, %v), want (%d, nil)", got, err, testCase.want)
			}
		})
	}

	connect := Connect{Version: ProtocolV311, CleanStart: true, ClientID: "id"}
	if connect.packetType() != PacketTypeConnect || connect.packetVersion() != ProtocolV311 {
		t.Fatal("CONNECT packet contract mismatch")
	}
	connAck := ConnAck{Version: ProtocolV5}
	if connAck.packetType() != PacketTypeConnAck || connAck.packetVersion() != ProtocolV5 {
		t.Fatal("CONNACK packet contract mismatch")
	}
	publish := Publish{Version: ProtocolV311, Topic: "topic"}
	if publish.packetType() != PacketTypePublish || publish.packetVersion() != ProtocolV311 {
		t.Fatal("PUBLISH packet contract mismatch")
	}

	typed := UnsupportedProtocolVersionError{ProtocolName: "MQIsdp", Level: 3}
	if typed.Error() == "" || typed.Is(ErrMalformedPacket) || !typed.Is(ErrUnsupportedProtocolVersion) {
		t.Fatal("unsupported-version error contract mismatch")
	}
	if (&typed).As(new(struct{})) {
		t.Fatal("unsupported-version error accepted an unrelated As target")
	}
	var typedPointer *UnsupportedProtocolVersionError
	if !(&typed).As(&typedPointer) || typedPointer != &typed {
		t.Fatal("unsupported-version error pointer As target mismatch")
	}
}

func TestRemainingLengthFramingHelperEdges_MQTT153_1_2_MQTT154_1_2(t *testing.T) {
	for _, value := range []int{0, 127, 128, 16383, 16384, 2097151, 2097152, maxRemainingLength} {
		if got := variableByteIntegerSize(value); got < 1 || got > 4 {
			t.Fatalf("variableByteIntegerSize(%d) = %d", value, got)
		}
	}
	if validMQTTUTF8("valid") == false || validMQTTUTF8("\u0000") || validMQTTUTF8("\uFDD0") || validMQTTUTF8("\U0001FFFF") || validMQTTUTF8(string([]byte{0xC0, 0x80})) {
		t.Fatal("MQTT UTF-8 validation mismatch")
	}
	if cloneBytes(nil) != nil {
		t.Fatal("cloneBytes(nil) must remain nil")
	}
	original := []byte{1, 2, 3}
	copyOfOriginal := cloneBytes(original)
	original[0] = 9
	if !bytes.Equal(copyOfOriginal, []byte{1, 2, 3}) {
		t.Fatal("cloneBytes did not copy data")
	}
	if _, err := NewDecoder(maxRemainingLength); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEncoder(maxRemainingLength); err != nil {
		t.Fatal(err)
	}
}

func TestConnectDecoderAndEncoderEdgeContracts_MQTT01_MQTT12_MQTT13(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	var nilDecoder *Decoder
	if _, _, err := nilDecoder.Decode([]byte{0x30}, ProtocolV311); !errors.Is(err, ErrIncompletePacket) {
		t.Fatalf("nil decoder error = %v", err)
	}
	for _, testCase := range []struct {
		name    string
		frame   []byte
		version ProtocolVersion
		want    error
	}{
		{name: "invalid-version", frame: []byte{0x30, 0x00}, version: ProtocolVersion(6), want: ErrUnsupportedProtocolVersion},
		{name: "unknown-packet", frame: []byte{0x40, 0x00}, version: ProtocolV311, want: ErrMalformedPacket},
		{name: "truncated-body", frame: []byte{0x30, 0x02, 0x00}, version: ProtocolV311, want: ErrIncompletePacket},
		{name: "empty-body", frame: []byte{0x30, 0x00}, version: ProtocolV311, want: ErrMalformedPacket},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, consumed, err := decoder.Decode(testCase.frame, testCase.version)
			if !errors.Is(err, testCase.want) || consumed != 0 {
				t.Fatalf("Decode() = (consumed %d, error %v), want (0, %v)", consumed, err, testCase.want)
			}
		})
	}

	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	var nilEncoder *Encoder
	if _, err := nilEncoder.Append([]byte{1}, &Publish{Version: ProtocolV311, Topic: "topic"}); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("nil encoder error = %v", err)
	}
	var nilConnect *Connect
	var nilConnAck *ConnAck
	var nilPublish *Publish
	for _, packet := range []Packet{nilConnect, nilConnAck, nilPublish, unsupportedTestPacket{}} {
		if _, err := encoder.Append([]byte{1}, packet); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("Append(%T) error = %v, want malformed", packet, err)
		}
	}
	if _, err := encodeConnAck(ConnAck{Version: ProtocolUnknown}, maxRemainingLength); !errors.Is(err, ErrUnsupportedProtocolVersion) {
		t.Fatalf("encodeConnAck() error = %v", err)
	}
	if _, err := encodePublish(Publish{Version: ProtocolUnknown, Topic: "topic"}, maxRemainingLength); !errors.Is(err, ErrUnsupportedProtocolVersion) {
		t.Fatalf("encodePublish() error = %v", err)
	}
	if _, err := decodeConnAck([]byte{0, 0}, ProtocolUnknown); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("decodeConnAck() error = %v", err)
	}
	if validConnAckReason(ProtocolUnknown, 0) {
		t.Fatal("unknown protocol reason unexpectedly accepted")
	}
}

func TestConnectDecodeRareMalformedBranches_MQTT01_MQTT12_TEST03_TEST04(t *testing.T) {
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		body    []byte
		version ProtocolVersion
		want    error
	}{
		{name: "missing-level", body: []byte{0, 4, 'M', 'Q', 'T', 'T'}, version: ProtocolUnknown, want: ErrMalformedPacket},
		{name: "missing-fixed-fields", body: []byte{0, 4, 'M', 'Q', 'T', 'T', 4}, version: ProtocolUnknown, want: ErrMalformedPacket},
		{name: "will-qos-without-will", body: []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0x0A, 0, 0, 0, 1, 'c'}, version: ProtocolUnknown, want: ErrMalformedPacket},
		{name: "empty-client-without-clean-start", body: []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, 0, 0}, version: ProtocolUnknown, want: ErrMalformedPacket},
		{name: "v5-will-property", body: []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 0x06, 0, 0, 0, 0, 1, 'c', 1, 0, 0, 4, 'w', 'i', 'l', 'l', 0, 0}, version: ProtocolUnknown, want: ErrUnsupportedProperty},
		{name: "v5-truncated-will-property", body: []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 0x06, 0, 0, 0, 0, 1, 'c', 1}, version: ProtocolUnknown, want: ErrMalformedPacket},
		{name: "empty-will-topic", body: []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0x06, 0, 0, 0, 1, 'c', 0, 0, 0}, version: ProtocolUnknown, want: ErrMalformedPacket},
		{name: "missing-will-payload", body: []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0x06, 0, 0, 0, 1, 'c', 0, 4, 'w', 'i', 'l', 'l'}, version: ProtocolUnknown, want: ErrMalformedPacket},
		{name: "missing-username", body: []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0x82, 0, 0, 0, 1, 'c'}, version: ProtocolUnknown, want: ErrMalformedPacket},
		{name: "missing-password", body: []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0xC2, 0, 0, 0, 1, 'c', 0, 0}, version: ProtocolUnknown, want: ErrMalformedPacket},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, consumed, err := decoder.Decode(testFrame(0x10, testCase.body), testCase.version)
			if !errors.Is(err, testCase.want) || consumed != 0 {
				t.Fatalf("Decode() = (consumed %d, error %v), want (0, %v)", consumed, err, testCase.want)
			}
		})
	}
}

func TestConnectConnAckPublishSizeAndValueBranches_MQTT01_MQTT11_MQTT12(t *testing.T) {
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decoder.Decode(testFrame(0x20, []byte{0}), ProtocolV311); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("short MQTT 3.1.1 CONNACK error = %v, want ErrMalformedPacket", err)
	}
	if _, err := encodeConnect(Connect{Version: ProtocolV311, CleanStart: true, ClientID: "id"}, 1); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("encodeConnect() error = %v, want ErrPacketTooLarge", err)
	}
	if _, err := encodeConnAck(ConnAck{Version: ProtocolV311}, 1); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("encodeConnAck() error = %v, want ErrPacketTooLarge", err)
	}
	if _, err := encodeConnAck(ConnAck{Version: ProtocolV311, SessionPresent: true, ReasonCode: 1}, maxRemainingLength); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("invalid Session Present CONNACK error = %v, want ErrMalformedPacket", err)
	}
	encoded, err := encoder.Append(nil, Publish{Version: ProtocolV311, Retain: true, Topic: "topic"})
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != 0x31 {
		t.Fatalf("value PUBLISH fixed header = 0x%02X, want 0x31", encoded[0])
	}
}
