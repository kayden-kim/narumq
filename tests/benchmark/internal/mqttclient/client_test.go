package mqttclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

func TestConnectResumesSessionWithSelectedOptions(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer connection.Close()
		frame, err := readWireFrame(connection, time.Second, time.Second)
		if err != nil {
			serverErr <- err
			return
		}
		decoder, err := mqtt.NewDecoder(MaxPacketSize)
		if err != nil {
			serverErr <- err
			return
		}
		packet, consumed, err := decoder.Decode(frame.wire, mqtt.ProtocolV5)
		if err != nil {
			serverErr <- err
			return
		}
		connect, ok := packet.(*mqtt.Connect)
		if !ok || consumed != len(frame.wire) {
			serverErr <- errors.New("server did not receive a complete CONNECT packet")
			return
		}
		if len(frame.body) < 8 || frame.body[7] != 0xC0 {
			var flags byte
			if len(frame.body) >= 8 {
				flags = frame.body[7]
			}
			serverErr <- fmt.Errorf("CONNECT flags = %#x, want %#x", flags, 0xC0)
			return
		}
		if connect.CleanStart || connect.SessionExpiryInterval != 300 || !connect.HasUsername || connect.Username != "jwt" ||
			!connect.HasPassword || string(connect.Password) != "token" {
			serverErr <- fmt.Errorf("decoded CONNECT = %#v, want selected session and credentials", connect)
			return
		}
		_, err = connection.Write([]byte{0x20, 0x03, 0x01, 0x00, 0x00})
		serverErr <- err
	}()

	client, timing, err := Connect(context.Background(), ConnectOptions{
		Address:               listener.Addr().String(),
		ClientID:              "resume-client",
		HandshakeTimeout:      time.Second,
		CleanStart:            false,
		SessionExpiryInterval: 300,
		Username:              "jwt",
		Password:              []byte("token"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if !timing.SessionPresent {
		t.Fatal("ConnectTiming.SessionPresent = false, want true")
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestConnectRejectsSessionPresentForCleanStart(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer connection.Close()
		if _, err := readWireFrame(connection, time.Second, time.Second); err != nil {
			serverErr <- err
			return
		}
		_, err = connection.Write([]byte{0x20, 0x03, 0x01, 0x00, 0x00})
		serverErr <- err
	}()

	client, _, err := Connect(context.Background(), ConnectOptions{
		Address:          listener.Addr().String(),
		ClientID:         "clean-client",
		HandshakeTimeout: time.Second,
		CleanStart:       true,
	})
	if err == nil {
		if client != nil {
			_ = client.Close()
		}
		t.Fatal("Connect() succeeded, want Session Present rejection")
	}
	if serverErr := <-serverErr; serverErr != nil {
		t.Fatal(serverErr)
	}
}

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
