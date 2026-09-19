package mqttclient

import (
	"net"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

func TestReadConnAckAcceptsBrokerProperties(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()
	wire := []byte{
		0x20, 0x08,
		0x00, 0x00,
		0x05, 0x21, 0x00, 0x0A, 0x24, 0x00,
	}
	go func() { _, _ = writer.Write(wire) }()
	decoder, err := mqtt.NewDecoder(MaxPacketSize)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{connection: reader, decoder: decoder, handshakeTimeout: time.Second}
	connAck, err := client.readConnAck(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if connAck.Version != mqtt.ProtocolV5 || connAck.SessionPresent || connAck.ReasonCode != 0 {
		t.Fatalf("readConnAck() = %#v, want successful MQTT 5 CONNACK", connAck)
	}
}

func TestReadWireFrameMidFrameTimeoutIsFatal(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()
	go func() { _, _ = writer.Write([]byte{0x30}) }()
	_, err := readWireFrame(reader, time.Second, 20*time.Millisecond)
	if err == nil || IsTimeout(err) {
		t.Fatalf("readWireFrame() error = %v, want fatal non-timeout mid-frame error", err)
	}
}

func TestReadWireFrameRejectsUnterminatedRemainingLength(t *testing.T) {
	reader, writer := net.Pipe()
	defer reader.Close()
	defer writer.Close()
	go func() { _, _ = writer.Write([]byte{0x30, 0x80, 0x80, 0x80, 0x80}) }()
	_, err := readWireFrame(reader, time.Second, time.Second)
	if err != mqtt.ErrMalformedPacket {
		t.Fatalf("readWireFrame() error = %v, want malformed packet", err)
	}
}
