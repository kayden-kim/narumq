package integration

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"narumq/internal/adapters/clock"
	"narumq/internal/adapters/memory/deliveryqueue"
	"narumq/internal/adapters/memory/sessionstore"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/connect"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

type ingressFixture struct {
	server   *tcpadapter.Server
	listener net.Listener
	cancel   context.CancelFunc
	done     chan error
	repo     *sessionstore.MemorySessionRepository
	encoder  *mqtt.Encoder
	decoder  *mqtt.Decoder
}

func newIngressFixture(t *testing.T) *ingressFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	routes := routing.NewTable()
	deliveries, err := deliveryqueue.New(64)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	repository := sessionstore.New()
	connections, err := connect.New(clock.SystemClock{}, repository, deliveries, routes)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	publications, err := publish.New(routes, deliveries)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(1048576)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(1048576)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	server, err := tcpadapter.NewServer(
		listener,
		tcpadapter.Config{
			MaxPacketSize:        1048576,
			ConnectTimeout:       time.Second,
			WriteTimeout:         time.Second,
			ControlQueueCapacity: 16,
		},
		decoder,
		encoder,
		connections,
		subscriptions,
		publications,
		deliveries,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	return &ingressFixture{server: server, listener: listener, cancel: cancel, done: done, repo: repository, encoder: encoder, decoder: decoder}
}

func (fixture *ingressFixture) close(t *testing.T) {
	t.Helper()
	fixture.cancel()
	select {
	case err := <-fixture.done:
		if err != nil {
			t.Fatalf("Serve() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not stop")
	}
}

func (fixture *ingressFixture) dial(t *testing.T) net.Conn {
	t.Helper()
	connection, err := net.DialTimeout("tcp", fixture.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func writePacket(t *testing.T, connection net.Conn, encoder *mqtt.Encoder, packet mqtt.Packet) {
	t.Helper()
	wire, err := encoder.Append(nil, packet)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write(wire); err != nil {
		t.Fatal(err)
	}
}

func readPacket(t *testing.T, connection net.Conn, decoder *mqtt.Decoder, version mqtt.ProtocolVersion) mqtt.Packet {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var header [1]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		t.Fatalf("read MQTT header: %v", err)
	}
	frame := []byte{header[0]}
	multiplier := 1
	for {
		var encoded [1]byte
		if _, err := io.ReadFull(connection, encoded[:]); err != nil {
			t.Fatalf("read MQTT remaining length: %v", err)
		}
		frame = append(frame, encoded[0])
		if encoded[0]&0x80 == 0 {
			break
		}
		multiplier++
		if multiplier > 4 {
			t.Fatal("remaining length used more than four bytes")
		}
	}
	remainingLength := 0
	multiplier = 1
	for _, encoded := range frame[1:] {
		remainingLength += int(encoded&0x7F) * multiplier
		multiplier *= 128
	}
	body := make([]byte, remainingLength)
	if _, err := io.ReadFull(connection, body); err != nil {
		t.Fatalf("read MQTT body: %v", err)
	}
	frame = append(frame, body...)
	packet, consumed, err := decoder.Decode(frame, version)
	if err != nil {
		t.Fatalf("decode server packet % X: %v", frame, err)
	}
	if consumed != len(frame) {
		t.Fatalf("server packet consumed %d of %d bytes", consumed, len(frame))
	}
	return packet
}

func expectClosed(t *testing.T, connection net.Conn) {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var value [1]byte
	_, err := connection.Read(value[:])
	if err == nil {
		t.Fatal("connection remained open")
	}
}

// Traceability: PRD MQTT-01/MQTT-02/MQTT-06/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §1, §2, §3.2, §7; OASIS MQTT-3.1.2, MQTT-3.8.4, MQTT-3.3.4, MQTT-3.12.4.
func TestMQTTIngress(t *testing.T) {
	t.Run("qos0 fanout ping and disconnect", func(t *testing.T) {
		fixture := newIngressFixture(t)
		defer fixture.close(t)
		subscriber := fixture.dial(t)
		defer subscriber.Close()
		writePacket(t, subscriber, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "subscriber"})
		connAck := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV311).(*mqtt.ConnAck)
		if connAck.ReasonCode != 0 || connAck.SessionPresent {
			t.Fatalf("subscriber CONNACK = %#v", connAck)
		}
		writePacket(t, subscriber, fixture.encoder, &mqtt.Subscribe{Version: mqtt.ProtocolV311, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "sensors/+", QoS: mqtt.QoSAtMostOnce}}})
		subAck := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV311).(*mqtt.SubAck)
		if subAck.PacketID != 1 || len(subAck.ReasonCodes) != 1 || subAck.ReasonCodes[0] != 0 {
			t.Fatalf("subscriber SUBACK = %#v", subAck)
		}

		publisher := fixture.dial(t)
		defer publisher.Close()
		writePacket(t, publisher, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "publisher"})
		publisherAck := readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
		if publisherAck.ReasonCode != 0 {
			t.Fatalf("publisher CONNACK = %#v", publisherAck)
		}
		writePacket(t, publisher, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "sensors/room1", Payload: []byte("temperature")})
		delivery := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV311).(*mqtt.Publish)
		if delivery.Topic != "sensors/room1" || string(delivery.Payload) != "temperature" || delivery.QoS != mqtt.QoSAtMostOnce {
			t.Fatalf("subscriber delivery = %#v", delivery)
		}
		writePacket(t, subscriber, fixture.encoder, &mqtt.PingReq{Version: mqtt.ProtocolV311})
		if _, ok := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV311).(*mqtt.PingResp); !ok {
			t.Fatal("subscriber did not receive PINGRESP")
		}
		writePacket(t, publisher, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		expectClosed(t, publisher)
		writePacket(t, subscriber, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV311})
		expectClosed(t, subscriber)
	})

	t.Run("persistent reconnect reports session present", func(t *testing.T) {
		fixture := newIngressFixture(t)
		defer fixture.close(t)
		client := fixture.dial(t)
		writePacket(t, client, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "persistent", SessionExpiryInterval: 60})
		first := readPacket(t, client, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
		if first.SessionPresent || first.ReasonCode != 0 {
			t.Fatalf("first persistent CONNACK = %#v", first)
		}
		writePacket(t, client, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		expectClosed(t, client)
		client.Close()

		client = fixture.dial(t)
		defer client.Close()
		writePacket(t, client, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "persistent", SessionExpiryInterval: 60})
		resumed := readPacket(t, client, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
		if !resumed.SessionPresent || resumed.ReasonCode != 0 {
			t.Fatalf("resumed persistent CONNACK = %#v", resumed)
		}
		writePacket(t, client, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		expectClosed(t, client)
	})

	t.Run("unsupported and malformed connect do not create sessions", func(t *testing.T) {
		fixture := newIngressFixture(t)
		defer fixture.close(t)
		unsupported := fixture.dial(t)
		unsupportedWire := []byte{0x10, 0x0C, 0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p', 0x03, 0x02, 0x00, 0x00}
		if _, err := unsupported.Write(unsupportedWire); err != nil {
			t.Fatal(err)
		}
		expectClosed(t, unsupported)
		unsupported.Close()
		if _, found, err := fixture.repo.Load("unsupported"); err != nil || found {
			t.Fatalf("unsupported session = found %v, err %v", found, err)
		}

		malformed := fixture.dial(t)
		if _, err := malformed.Write([]byte{0x10, 0x02, 0x00, 0x00}); err != nil {
			t.Fatal(err)
		}
		expectClosed(t, malformed)
		malformed.Close()
		if _, found, err := fixture.repo.Load("malformed"); err != nil || found {
			t.Fatalf("malformed session = found %v, err %v", found, err)
		}
	})

	t.Run("qos negative acknowledgements and MQTT311 close", func(t *testing.T) {
		fixture := newIngressFixture(t)
		defer fixture.close(t)
		mqtt5 := fixture.dial(t)
		defer mqtt5.Close()
		writePacket(t, mqtt5, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "qos5"})
		if ack := readPacket(t, mqtt5, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
			t.Fatalf("MQTT 5 CONNACK = %#v", ack)
		}
		writePacket(t, mqtt5, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 7, Topic: "qos/one", Payload: []byte("one")})
		pubAck := readPacket(t, mqtt5, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
		if pubAck.PacketID != 7 || pubAck.ReasonCode != 0x83 {
			t.Fatalf("negative PUBACK = %#v", pubAck)
		}
		writePacket(t, mqtt5, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSExactlyOnce, PacketID: 8, Topic: "qos/two", Payload: []byte("two")})
		pubRec := readPacket(t, mqtt5, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubRec)
		if pubRec.PacketID != 8 || pubRec.ReasonCode != 0x83 {
			t.Fatalf("negative PUBREC = %#v", pubRec)
		}
		writePacket(t, mqtt5, fixture.encoder, &mqtt.PingReq{Version: mqtt.ProtocolV5})
		if _, ok := readPacket(t, mqtt5, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PingResp); !ok {
			t.Fatal("MQTT 5 connection did not remain after negative ACKs")
		}

		mqtt311 := fixture.dial(t)
		defer mqtt311.Close()
		writePacket(t, mqtt311, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "qos311"})
		if ack := readPacket(t, mqtt311, fixture.decoder, mqtt.ProtocolV311).(*mqtt.ConnAck); ack.ReasonCode != 0 {
			t.Fatalf("MQTT 3.1.1 CONNACK = %#v", ack)
		}
		writePacket(t, mqtt311, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV311, QoS: mqtt.QoSAtLeastOnce, PacketID: 9, Topic: "qos/one", Payload: []byte("one")})
		expectClosed(t, mqtt311)
	})
}
