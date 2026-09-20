package mqtt

import (
	"bytes"
	"errors"
	"testing"
)

type unknownPacket struct{}

func (unknownPacket) packetType() PacketType         { return 0 }
func (unknownPacket) packetVersion() ProtocolVersion { return ProtocolV5 }

func TestEncoderRejectsNilUnknownAndUnsupportedPackets(t *testing.T) {
	encoder, err := NewEncoder(64)
	if err != nil {
		t.Fatal(err)
	}
	var nilPing *PingReq
	cases := []Packet{nil, nilPing, unknownPacket{}, &PingReq{Version: ProtocolUnknown}}
	for index, packet := range cases {
		destination := []byte{0xAA}
		before := bytes.Clone(destination)
		encoded, err := encoder.Append(destination, packet)
		if err == nil || !bytes.Equal(encoded, before) {
			t.Fatalf("case %d Append() = (% X, %v), want unchanged destination and error", index, encoded, err)
		}
	}
	if _, err := NewEncoder(0); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("NewEncoder(0) error = %v, want ErrPacketTooLarge", err)
	}
	var nilEncoder *Encoder
	if _, err := nilEncoder.Append([]byte{1}, &PingReq{Version: ProtocolV5}); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("nil Encoder.Append() error = %v, want ErrMalformedPacket", err)
	}
}

func TestEncoderSupportsAllPacketValueAndPointerForms(t *testing.T) {
	encoder, err := NewEncoder(1024)
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		ConnAck{Version: ProtocolV5}, &ConnAck{Version: ProtocolV5},
		Publish{Version: ProtocolV5, Topic: "topic", Payload: []byte("payload")}, &Publish{Version: ProtocolV5, Topic: "topic", Payload: []byte("payload")},
		PubAck{Version: ProtocolV5, PacketID: 1}, &PubAck{Version: ProtocolV5, PacketID: 1},
		PubRec{Version: ProtocolV5, PacketID: 1}, &PubRec{Version: ProtocolV5, PacketID: 1},
		PubRel{Version: ProtocolV5, PacketID: 1}, &PubRel{Version: ProtocolV5, PacketID: 1},
		PubComp{Version: ProtocolV5, PacketID: 1}, &PubComp{Version: ProtocolV5, PacketID: 1},
		Subscribe{Version: ProtocolV5, PacketID: 1, Subscriptions: []Subscription{{Filter: "topic", QoS: QoSAtMostOnce}}}, &Subscribe{Version: ProtocolV5, PacketID: 1, Subscriptions: []Subscription{{Filter: "topic", QoS: QoSAtMostOnce}}},
		SubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0}}, &SubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0}},
		Unsubscribe{Version: ProtocolV5, PacketID: 1, Filters: []string{"topic"}}, &Unsubscribe{Version: ProtocolV5, PacketID: 1, Filters: []string{"topic"}},
		UnsubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0}}, &UnsubAck{Version: ProtocolV5, PacketID: 1, ReasonCodes: []byte{0}},
		PingReq{Version: ProtocolV5}, &PingReq{Version: ProtocolV5},
		PingResp{Version: ProtocolV5}, &PingResp{Version: ProtocolV5},
		Disconnect{Version: ProtocolV5}, &Disconnect{Version: ProtocolV5},
	}
	for index, packet := range packets {
		if _, err := encoder.Append(nil, packet); err != nil {
			t.Fatalf("packet %d (%T) encode error = %v", index, packet, err)
		}
	}
	var nilPackets = []Packet{
		(*ConnAck)(nil), (*Publish)(nil), (*PubAck)(nil), (*PubRec)(nil), (*PubRel)(nil), (*PubComp)(nil),
		(*Subscribe)(nil), (*SubAck)(nil), (*Unsubscribe)(nil), (*UnsubAck)(nil), (*PingReq)(nil), (*PingResp)(nil), (*Disconnect)(nil),
	}
	for index, packet := range nilPackets {
		if _, err := encoder.Append(nil, packet); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("nil packet %d (%T) error = %v", index, packet, err)
		}
	}
}
