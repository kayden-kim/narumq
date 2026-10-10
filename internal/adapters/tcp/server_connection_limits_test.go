package tcp

import (
	"errors"
	"net"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

// Traceability: docs/specs/configuration-and-runtime-options.md CFG-SCN-26; docs/specs/mqtt-protocol-and-session.md §3.2.
func TestServerSetConnectionLimitsAppliesToNewConnectionsOnly(t *testing.T) {
	config := defaultAdapterConfig()
	runtime := newAdapterTestRuntime(t, 2, config)
	defer runtime.stop(t)

	connect := func(client net.Conn, clientID string) *mqtt.ConnAck {
		t.Helper()
		writeTestPackets(t, client, &mqtt.Connect{
			Version: mqtt.ProtocolV5, CleanStart: true, ClientID: clientID,
		})
		frame := readTestFrame(t, client)
		packet, consumed, err := runtime.server.limits.Load().decoder.Decode(frame, mqtt.ProtocolV5)
		if err != nil || consumed != len(frame) {
			t.Fatalf("decode %s CONNACK: packet=%T consumed=%d err=%v", clientID, packet, consumed, err)
		}
		ack, ok := packet.(*mqtt.ConnAck)
		if !ok || ack.ReasonCode != 0 {
			t.Fatalf("%s CONNACK = %#v, want successful CONNACK", clientID, packet)
		}
		return ack
	}

	ackA := connect(runtime.client, "limits-a")
	if ackA.ReceiveMaximum != config.ReceiveMaximum || ackA.MaximumPacketSize != uint32(mqtt.PacketSize(config.MaxPacketSize)) {
		t.Fatalf("connection A limits = (receive %d, packet %d), want (receive %d, packet %d)",
			ackA.ReceiveMaximum, ackA.MaximumPacketSize, config.ReceiveMaximum, config.MaxPacketSize)
	}

	limits := ConnectionLimits{
		MaxPacketSize: 128, ReceiveMaximum: 2,
		ConnectTimeout: time.Second, PacketReadTimeout: time.Second, WriteTimeout: time.Second,
	}
	var nilServer *Server
	if err := nilServer.SetConnectionLimits(limits); !errors.Is(err, ErrNilListener) {
		t.Fatalf("nil Server.SetConnectionLimits() error = %v, want ErrNilListener", err)
	}
	if err := runtime.server.SetConnectionLimits(limits); err != nil {
		t.Fatalf("SetConnectionLimits() error = %v", err)
	}

	writeTestPackets(t, runtime.client,
		&mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "limits/old", Payload: make([]byte, 180)},
		&mqtt.PingReq{Version: mqtt.ProtocolV5},
	)
	frameA := readTestFrame(t, runtime.client)
	packet, consumed, err := runtime.server.limits.Load().decoder.Decode(frameA, mqtt.ProtocolV5)
	if err != nil || consumed != len(frameA) {
		t.Fatalf("decode connection A response: packet=%T consumed=%d err=%v", packet, consumed, err)
	}
	if _, ok := packet.(*mqtt.PingResp); !ok {
		t.Fatalf("connection A response = %T, want PINGRESP after 180-byte PUBLISH", packet)
	}

	newConnection := func() net.Conn {
		t.Helper()
		serverSocket, client := net.Pipe()
		runtime.listener.connections <- serverSocket
		select {
		case <-runtime.listener.accepted:
		case <-time.After(time.Second):
			_ = client.Close()
			t.Fatal("server did not accept new connection")
		}
		return client
	}

	clientB := newConnection()
	defer clientB.Close()
	ackB := connect(clientB, "limits-b")
	if ackB.ReceiveMaximum != limits.ReceiveMaximum || ackB.MaximumPacketSize != uint32(mqtt.PacketSize(limits.MaxPacketSize)) {
		t.Fatalf("connection B limits = (receive %d, packet %d), want (receive %d, packet %d)",
			ackB.ReceiveMaximum, ackB.MaximumPacketSize, limits.ReceiveMaximum, limits.MaxPacketSize)
	}
	oversizedPublish := encodeTestPacket(t, &mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "limits/new", Payload: make([]byte, 180),
	})
	writeDone := make(chan struct{})
	go func() {
		_, _ = clientB.Write(oversizedPublish)
		close(writeDone)
	}()
	frameB := readTestFrame(t, clientB)
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("oversized PUBLISH write did not finish after server rejection")
	}
	packetB, consumedB, err := runtime.server.limits.Load().decoder.Decode(frameB, mqtt.ProtocolV5)
	disconnectB, ok := packetB.(*mqtt.Disconnect)
	if err != nil || consumedB != len(frameB) || !ok || disconnectB.ReasonCode != 0x95 {
		t.Fatalf("connection B oversized PUBLISH response = (%#v, consumed %d, %v), want DISCONNECT 0x95", packetB, consumedB, err)
	}

	invalidPacketSize := limits
	invalidPacketSize.MaxPacketSize = 0
	if err := runtime.server.SetConnectionLimits(invalidPacketSize); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("SetConnectionLimits(MaxPacketSize=0) error = %v, want ErrInvalidConfig", err)
	}
	invalidReceiveMaximum := limits
	invalidReceiveMaximum.ReceiveMaximum = 0
	if err := runtime.server.SetConnectionLimits(invalidReceiveMaximum); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("SetConnectionLimits(ReceiveMaximum=0) error = %v, want ErrInvalidConfig", err)
	}

	clientC := newConnection()
	defer clientC.Close()
	ackC := connect(clientC, "limits-c")
	if ackC.ReceiveMaximum != limits.ReceiveMaximum || ackC.MaximumPacketSize != uint32(mqtt.PacketSize(limits.MaxPacketSize)) {
		t.Fatalf("connection C limits after rejected updates = (receive %d, packet %d), want (receive %d, packet %d)",
			ackC.ReceiveMaximum, ackC.MaximumPacketSize, limits.ReceiveMaximum, limits.MaxPacketSize)
	}
}
