package tcp

import (
	"io"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

// Traceability: PRD MQTT-12/TEST-13.
func FuzzConnectionInput(f *testing.F) {
	encoder, err := mqtt.NewEncoder(256)
	if err != nil {
		f.Fatal(err)
	}
	seeds := [][]byte{nil, {0x30, 0xff, 0x7f}, {0x30, 0xff, 0xff, 0xff, 0xff}, {0x30}, {0x30, 4, 0, 1, 0xff, 0}}
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		publish, err := encoder.Append(nil, &mqtt.Publish{Version: version, QoS: mqtt.QoSAtLeastOnce, PacketID: 1, Topic: "fuzz"})
		if err != nil {
			f.Fatal(err)
		}
		f.Add(byte(version), publish)
		var quota []byte
		for id := mqtt.PacketIdentifier(1); id <= 3; id++ {
			quota, err = encoder.Append(quota, &mqtt.Publish{Version: version, QoS: mqtt.QoSExactlyOnce, PacketID: id, Topic: "fuzz"})
			if err != nil {
				f.Fatal(err)
			}
		}
		f.Add(byte(version), quota)
		for _, seed := range seeds {
			f.Add(byte(version), seed)
		}
	}
	f.Fuzz(func(t *testing.T, version byte, input []byte) {
		cfg := defaultAdapterConfig()
		cfg.MaxPacketSize = 256
		cfg.ReceiveMaximum = 2
		cfg.PacketReadTimeout = 50 * time.Millisecond
		cfg.ConnectTimeout = 100 * time.Millisecond
		cfg.WriteTimeout = 50 * time.Millisecond
		fixture := newAdapterTestRuntime(t, 4, cfg)
		defer fixture.stop(t)
		protocol := mqtt.ProtocolV311
		if version%2 != 0 {
			protocol = mqtt.ProtocolV5
		}
		wire, err := encoder.Append(nil, &mqtt.Connect{Version: protocol, ClientID: "fuzz", CleanStart: true})
		if err != nil {
			t.Fatal(err)
		}
		wire = append(wire, input...)
		written, drained := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(written)
			_ = fixture.client.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = fixture.client.Write(wire)
			_ = fixture.client.Close()
		}()
		go func() { defer close(drained); _, _ = io.Copy(io.Discard, fixture.client) }()
		<-written
		<-drained
		deadline := time.Now().Add(2 * time.Second)
		for fixture.server.ActiveConnectionCount() != 0 {
			if time.Now().After(deadline) {
				t.Fatal("connection goroutines did not exit")
			}
			time.Sleep(time.Millisecond)
		}
	})
}
