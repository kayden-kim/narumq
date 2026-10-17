package mqtt

import (
	"bytes"
	"errors"
	"reflect"
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
		{name: "v311-session-expiry", input: func() Connect { value := base; value.SessionExpiryInterval = 1; return value }(), want: ErrMalformedPacket},
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
		{name: "v311-will-delay-interval", input: func() Connect {
			value := base
			value.Will = &Will{Topic: "will", DelayInterval: 5}
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
func TestWill_Clone(t *testing.T) {
	var nilWill *Will
	if nilWill.Clone() != nil {
		t.Fatal("nil Will.Clone() must return nil")
	}
	original := &Will{
		QoS:           1,
		Retain:        true,
		Topic:         "test/will",
		Payload:       []byte("initial-payload"),
		DelayInterval: 15,
	}
	cloned := original.Clone()
	if cloned == nil {
		t.Fatal("cloned Will must not be nil")
	}
	if !reflect.DeepEqual(original, cloned) {
		t.Fatalf("cloned Will mismatch: got %#v, want %#v", cloned, original)
	}
	original.Payload[0] = 'X'
	if cloned.Payload[0] == 'X' {
		t.Fatal("mutating original.Payload affected cloned.Payload")
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
	subscribe := Subscribe{Version: ProtocolV5}
	if subscribe.packetType() != PacketTypeSubscribe || subscribe.packetVersion() != ProtocolV5 {
		t.Fatal("SUBSCRIBE packet contract mismatch")
	}
	unsubscribe := Unsubscribe{Version: ProtocolV311}
	if unsubscribe.packetType() != PacketTypeUnsubscribe || unsubscribe.packetVersion() != ProtocolV311 {
		t.Fatal("UNSUBSCRIBE packet contract mismatch")
	}
	ackCases := []struct {
		name       string
		packet     Packet
		packetType PacketType
		version    ProtocolVersion
	}{
		{name: "PUBACK", packet: PubAck{Version: ProtocolV5}, packetType: PacketTypePubAck, version: ProtocolV5},
		{name: "PUBREC", packet: PubRec{Version: ProtocolV5}, packetType: PacketTypePubRec, version: ProtocolV5},
		{name: "PUBREL", packet: PubRel{Version: ProtocolV311}, packetType: PacketTypePubRel, version: ProtocolV311},
		{name: "PUBCOMP", packet: PubComp{Version: ProtocolV5}, packetType: PacketTypePubComp, version: ProtocolV5},
		{name: "SUBACK", packet: SubAck{Version: ProtocolV5}, packetType: PacketTypeSubAck, version: ProtocolV5},
		{name: "UNSUBACK", packet: UnsubAck{Version: ProtocolV311}, packetType: PacketTypeUnsubAck, version: ProtocolV311},
		{name: "PINGREQ", packet: PingReq{Version: ProtocolV5}, packetType: PacketTypePingReq, version: ProtocolV5},
		{name: "PINGRESP", packet: PingResp{Version: ProtocolV5}, packetType: PacketTypePingResp, version: ProtocolV5},
		{name: "DISCONNECT", packet: Disconnect{Version: ProtocolV5}, packetType: PacketTypeDisconnect, version: ProtocolV5},
	}
	for _, testCase := range ackCases {
		t.Run(testCase.name+" packet contract", func(t *testing.T) {
			if testCase.packet.packetType() != testCase.packetType || testCase.packet.packetVersion() != testCase.version {
				t.Fatalf("%s packet contract mismatch", testCase.name)
			}
		})
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

// Scenarios: performance-and-verification.md:PRF-SCN-100
func TestRemainingLengthFramingHelperEdges_MQTT153_1_2_MQTT154_1_2(t *testing.T) {
	cases := []struct {
		value int
		want  int
	}{
		{value: 0, want: 1},
		{value: 127, want: 1},
		{value: 128, want: 2},
		{value: 16383, want: 2},
		{value: 16384, want: 3},
		{value: 2097151, want: 3},
		{value: 2097152, want: 4},
		{value: MaxRemainingLength, want: 4},
	}
	for _, testCase := range cases {
		value, want := testCase.value, testCase.want
		if got := variableByteIntegerSize(value); got != want {
			t.Fatalf("variableByteIntegerSize(%d) = %d, want %d", value, got, want)
		}
		if got := PacketSize(value); got != 1+want+value {
			t.Fatalf("PacketSize(%d) = %d, want %d", value, got, 1+want+value)
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
	if _, err := NewDecoder(MaxRemainingLength); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEncoder(MaxRemainingLength); err != nil {
		t.Fatal(err)
	}
}

func TestConnectDecoderAndEncoderEdgeContracts_MQTT01_MQTT12_MQTT13(t *testing.T) {
	decoder, err := NewDecoder(MaxRemainingLength)
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

	encoder, err := NewEncoder(MaxRemainingLength)
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
	var nilPubAck *PubAck
	var nilPubRec *PubRec
	var nilPubRel *PubRel
	var nilPubComp *PubComp
	var nilSubscribe *Subscribe
	var nilSubAck *SubAck
	var nilUnsubscribe *Unsubscribe
	var nilUnsubAck *UnsubAck
	for _, packet := range []Packet{nilConnect, nilConnAck, nilPublish, nilPubAck, nilPubRec, nilPubRel, nilPubComp, nilSubscribe, nilSubAck, nilUnsubscribe, nilUnsubAck, unsupportedTestPacket{}} {
		if _, err := encoder.Append([]byte{1}, packet); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("Append(%T) error = %v, want malformed", packet, err)
		}
	}
	if _, err := encodeConnAck(ConnAck{Version: ProtocolUnknown}, MaxRemainingLength); !errors.Is(err, ErrUnsupportedProtocolVersion) {
		t.Fatalf("encodeConnAck() error = %v", err)
	}
	if _, err := encodePublish(Publish{Version: ProtocolUnknown, Topic: "topic"}, MaxRemainingLength); !errors.Is(err, ErrUnsupportedProtocolVersion) {
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
	decoder, err := NewDecoder(MaxRemainingLength)
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
	encoder, err := NewEncoder(MaxRemainingLength)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := NewDecoder(MaxRemainingLength)
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
	if _, err := encodeConnAck(ConnAck{Version: ProtocolV311, SessionPresent: true, ReasonCode: 1}, MaxRemainingLength); !errors.Is(err, ErrMalformedPacket) {
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
