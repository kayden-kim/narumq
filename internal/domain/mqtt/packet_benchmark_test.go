package mqtt

import "testing"

func BenchmarkDecoderPublishQoS0_1KiB(b *testing.B) {
	payload := make([]byte, 1024)
	packet := Publish{Version: ProtocolV5, Topic: "benchmark/topic", Payload: payload}
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		b.Fatal(err)
	}
	wire, err := encoder.Append(nil, &packet)
	if err != nil {
		b.Fatal(err)
	}
	decoder, err := NewDecoder(maxRemainingLength)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(wire)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := decoder.Decode(wire, ProtocolV5); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncoderPublishQoS0_1KiB(b *testing.B) {
	packet := Publish{Version: ProtocolV5, Topic: "benchmark/topic", Payload: make([]byte, 1024)}
	encoder, err := NewEncoder(maxRemainingLength)
	if err != nil {
		b.Fatal(err)
	}
	destination := make([]byte, 0, 1100)
	b.SetBytes(int64(len(packet.Payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		destination = destination[:0]
		if _, err := encoder.Append(destination, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodecPacketCorpus(b *testing.B) {
	payload := make([]byte, 1024)
	packets := []struct {
		name   string
		packet Packet
	}{
		{
			name: "publish-qos0",
			packet: &Publish{
				Version: ProtocolV5,
				Topic:   "benchmark/topic",
				Payload: payload,
			},
		},
		{
			name: "publish-qos1",
			packet: &Publish{
				Version:  ProtocolV5,
				Topic:    "benchmark/topic",
				QoS:      QoSAtLeastOnce,
				PacketID: 1,
				Payload:  payload,
			},
		},
		{
			name: "publish-qos2",
			packet: &Publish{
				Version:  ProtocolV5,
				Topic:    "benchmark/topic",
				QoS:      QoSExactlyOnce,
				PacketID: 2,
				Payload:  payload,
			},
		},
		{
			name:   "puback",
			packet: &PubAck{Version: ProtocolV5, PacketID: 3},
		},
		{
			name:   "pubrec",
			packet: &PubRec{Version: ProtocolV5, PacketID: 4},
		},
		{
			name:   "pubrel",
			packet: &PubRel{Version: ProtocolV5, PacketID: 5},
		},
		{
			name:   "pubcomp",
			packet: &PubComp{Version: ProtocolV5, PacketID: 6},
		},
		{
			name: "subscribe",
			packet: &Subscribe{
				Version:  ProtocolV5,
				PacketID: 7,
				Subscriptions: []Subscription{
					{Filter: "benchmark/#", QoS: QoSAtLeastOnce, RetainHandling: SendRetainedIfNew},
				},
			},
		},
		{
			name:   "suback",
			packet: &SubAck{Version: ProtocolV5, PacketID: 7, ReasonCodes: []byte{0x00}},
		},
		{
			name:   "unsubscribe",
			packet: &Unsubscribe{Version: ProtocolV5, PacketID: 8, Filters: []string{"benchmark/#"}},
		},
		{
			name:   "unsuback",
			packet: &UnsubAck{Version: ProtocolV5, PacketID: 8, ReasonCodes: []byte{0x00}},
		},
	}

	for _, testCase := range packets {
		testCase := testCase
		b.Run("decode/"+testCase.name, func(b *testing.B) {
			encoder, err := NewEncoder(maxRemainingLength)
			if err != nil {
				b.Fatal(err)
			}
			wire, err := encoder.Append(nil, testCase.packet)
			if err != nil {
				b.Fatal(err)
			}
			decoder, err := NewDecoder(maxRemainingLength)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(wire)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, _, err := decoder.Decode(wire, ProtocolV5); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("encode/"+testCase.name, func(b *testing.B) {
			encoder, err := NewEncoder(maxRemainingLength)
			if err != nil {
				b.Fatal(err)
			}
			destination := make([]byte, 0, 1100)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				destination = destination[:0]
				if _, err := encoder.Append(destination, testCase.packet); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
