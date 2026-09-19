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
