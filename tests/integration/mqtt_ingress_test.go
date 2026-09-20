package integration

import (
	"context"
	"io"
	"log/slog"
	"net"
	"reflect"
	"testing"
	"time"

	"narumq/internal/adapters/clock"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
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
	return newIngressFixtureWithRetainedCapacity(t, 1024, 1<<20)
}

func newIngressFixtureWithRetainedCapacity(t *testing.T, messageCapacity, byteCapacity int) *ingressFixture {
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
	retainedRepository, err := memoryretained.New(messageCapacity, byteCapacity)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	repository := sessionstore.New()
	willBridge := tcpadapter.NewWillBridge()
	connections, err := connect.New(clock.SystemClock{}, repository, deliveries, routes, clock.SystemScheduler{}, willBridge)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes, deliveries, retainedRepository)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	publications, err := publish.New(routes, deliveries, retainedRepository)
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
	willBridge.Bind(server)
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

func expectNoPacket(t *testing.T, connection net.Conn) {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var header [1]byte
	_, err := connection.Read(header[:])
	if err == nil {
		t.Fatalf("unexpected MQTT packet header 0x%02X", header[0])
	}
	if networkError, ok := err.(net.Error); !ok || !networkError.Timeout() {
		t.Fatalf("reading unexpected packet: %v", err)
	}
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

// Traceability: PRD MQTT-01/MQTT-02/MQTT-06/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §1, §2, §3.2, §7.
// Wire-level acceptance scenarios cover CONNECT, SUBSCRIBE, QoS 0 PUBLISH,
// PINGRESP, DISCONNECT, persistent Session Present, and rejected CONNECT.
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

// Traceability: PRD MQTT-04/MQTT-08/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §4 and §5; MQTT-3.3.1-5/6/7/8/9/10/11/12/13, MQTT-3.8.3-3, MQTT-3.8.4-3/4.
func TestMQTTIngressRetainedMessageAndSubscriptionOptions(t *testing.T) {
	fixture := newIngressFixture(t)
	defer fixture.close(t)

	publisher := fixture.dial(t)
	defer publisher.Close()
	writePacket(t, publisher, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "publisher"})
	if ack := readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("publisher CONNACK = %#v", ack)
	}
	writePacket(t, publisher, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("one"), Retain: true})
	writePacket(t, publisher, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{{
			Filter:            "events/#",
			QoS:               mqtt.QoSAtLeastOnce,
			NoLocal:           true,
			RetainAsPublished: true,
		}},
	})
	if ack := readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 1 || !reflect.DeepEqual(ack.ReasonCodes, []byte{0x01}) {
		t.Fatalf("publisher SUBACK = %#v", ack)
	}
	expectNoPacket(t, publisher)

	readerFalse := fixture.dial(t)
	defer readerFalse.Close()
	writePacket(t, readerFalse, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "reader-false"})
	if ack := readPacket(t, readerFalse, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("reader-false CONNACK = %#v", ack)
	}
	writePacket(t, readerFalse, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 2,
		Subscriptions: []mqtt.Subscription{{
			Filter:            "events/#",
			QoS:               mqtt.QoSAtLeastOnce,
			RetainHandling:    mqtt.SendRetainedAtSubscribe,
			RetainAsPublished: false,
		}},
	})
	if ack := readPacket(t, readerFalse, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 2 || !reflect.DeepEqual(ack.ReasonCodes, []byte{0x01}) {
		t.Fatalf("reader-false SUBACK = %#v", ack)
	}
	replayedFalse := readPacket(t, readerFalse, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
	if replayedFalse.Topic != "events/one" || string(replayedFalse.Payload) != "one" || !replayedFalse.Retain {
		t.Fatalf("reader-false retained replay = %#v", replayedFalse)
	}

	readerTrue := fixture.dial(t)
	defer readerTrue.Close()
	writePacket(t, readerTrue, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "reader-true"})
	if ack := readPacket(t, readerTrue, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("reader-true CONNACK = %#v", ack)
	}
	writePacket(t, readerTrue, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 3,
		Subscriptions: []mqtt.Subscription{{
			Filter:            "events/#",
			QoS:               mqtt.QoSAtLeastOnce,
			RetainAsPublished: true,
		}},
	})
	if ack := readPacket(t, readerTrue, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 3 || !reflect.DeepEqual(ack.ReasonCodes, []byte{0x01}) {
		t.Fatalf("reader-true SUBACK = %#v", ack)
	}
	replayedTrue := readPacket(t, readerTrue, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
	if replayedTrue.Topic != "events/one" || !replayedTrue.Retain {
		t.Fatalf("reader-true retained replay = %#v", replayedTrue)
	}

	writePacket(t, publisher, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/two", Payload: []byte("two"), Retain: true})
	liveFalse := readPacket(t, readerFalse, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
	liveTrue := readPacket(t, readerTrue, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
	if liveFalse.Topic != "events/two" || liveFalse.Retain {
		t.Fatalf("RAP=false live delivery = %#v", liveFalse)
	}
	if liveTrue.Topic != "events/two" || !liveTrue.Retain {
		t.Fatalf("RAP=true live delivery = %#v", liveTrue)
	}

	writePacket(t, readerFalse, fixture.encoder, &mqtt.Subscribe{
		Version:       mqtt.ProtocolV5,
		PacketID:      4,
		Subscriptions: []mqtt.Subscription{{Filter: "events/#", QoS: mqtt.QoSAtMostOnce, RetainHandling: mqtt.SendRetainedIfNew}},
	})
	if ack := readPacket(t, readerFalse, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 4 || !reflect.DeepEqual(ack.ReasonCodes, []byte{0x00}) {
		t.Fatalf("reader-false RH=1 SUBACK = %#v", ack)
	}
	expectNoPacket(t, readerFalse)

	for _, connection := range []net.Conn{publisher, readerFalse, readerTrue} {
		writePacket(t, connection, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		expectClosed(t, connection)
	}
}

// Traceability: PRD MQTT-04/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5; MQTT-3.3.1-5/6.
func TestMQTTIngressRetainedCapacityDisconnect(t *testing.T) {
	fixture := newIngressFixtureWithRetainedCapacity(t, 1, 1)
	defer fixture.close(t)

	mqtt5 := fixture.dial(t)
	defer mqtt5.Close()
	writePacket(t, mqtt5, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "retained-v5"})
	if ack := readPacket(t, mqtt5, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("MQTT 5 CONNACK = %#v", ack)
	}
	writePacket(t, mqtt5, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "capacity", Payload: []byte("too-large"), Retain: true})
	disconnect := readPacket(t, mqtt5, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Disconnect)
	if disconnect.ReasonCode != 0x97 {
		t.Fatalf("MQTT 5 retained capacity DISCONNECT = %#v, want reason 0x97", disconnect)
	}
	expectClosed(t, mqtt5)

	mqtt311 := fixture.dial(t)
	defer mqtt311.Close()
	writePacket(t, mqtt311, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "retained-v311"})
	if ack := readPacket(t, mqtt311, fixture.decoder, mqtt.ProtocolV311).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("MQTT 3.1.1 CONNACK = %#v", ack)
	}
	writePacket(t, mqtt311, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV311, Topic: "capacity", Payload: []byte("too-large"), Retain: true})
	expectClosed(t, mqtt311)
}

// Traceability: PRD MQTT-01/TEST-04; MQTT-3.1.2-8; MQTT-3.1.2-10; MQTT-3.1.3-9; MQTT-3.14.4-3.
func TestMQTTIngressWillMessagesAndSessionExpiry(t *testing.T) {
	fixture := newIngressFixture(t)
	defer fixture.close(t)

	t.Run("socket drop publishes will immediately", func(t *testing.T) {
		subscriber := fixture.dial(t)
		defer subscriber.Close()
		writePacket(t, subscriber, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "sub-drop"})
		if ack := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
			t.Fatalf("subscriber CONNACK = %#v", ack)
		}
		writePacket(t, subscriber, fixture.encoder, &mqtt.Subscribe{
			Version:       mqtt.ProtocolV5,
			PacketID:      1,
			Subscriptions: []mqtt.Subscription{{Filter: "status/+", QoS: mqtt.QoSAtMostOnce}},
		})
		if ack := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 1 {
			t.Fatalf("SUBACK = %#v", ack)
		}

		client311 := fixture.dial(t)
		writePacket(t, client311, fixture.encoder, &mqtt.Connect{
			Version:  mqtt.ProtocolV311,
			ClientID: "client-311-drop",
			Will: &mqtt.Will{
				Topic:   "status/client-311",
				Payload: []byte("lost-311"),
			},
		})
		if ack := readPacket(t, client311, fixture.decoder, mqtt.ProtocolV311).(*mqtt.ConnAck); ack.ReasonCode != 0 {
			t.Fatalf("client311 CONNACK = %#v", ack)
		}
		client311.Close()

		will311 := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
		if will311.Topic != "status/client-311" || string(will311.Payload) != "lost-311" {
			t.Fatalf("expected 311 will message, got: %#v", will311)
		}

		client5 := fixture.dial(t)
		writePacket(t, client5, fixture.encoder, &mqtt.Connect{
			Version:               mqtt.ProtocolV5,
			ClientID:              "client-5-drop",
			CleanStart:            true,
			SessionExpiryInterval: 0,
			Will: &mqtt.Will{
				Topic:         "status/client-5",
				Payload:       []byte("lost-5"),
				DelayInterval: 0,
			},
		})
		if ack := readPacket(t, client5, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
			t.Fatalf("client5 CONNACK = %#v", ack)
		}
		client5.Close()

		will5 := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
		if will5.Topic != "status/client-5" || string(will5.Payload) != "lost-5" {
			t.Fatalf("expected 5 will message, got: %#v", will5)
		}
		writePacket(t, subscriber, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		expectClosed(t, subscriber)
	})

	t.Run("normal disconnect suppresses will", func(t *testing.T) {
		subscriber := fixture.dial(t)
		defer subscriber.Close()
		writePacket(t, subscriber, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "sub-normal"})
		if ack := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
			t.Fatalf("subscriber CONNACK = %#v", ack)
		}
		writePacket(t, subscriber, fixture.encoder, &mqtt.Subscribe{
			Version:       mqtt.ProtocolV5,
			PacketID:      1,
			Subscriptions: []mqtt.Subscription{{Filter: "status/+", QoS: mqtt.QoSAtMostOnce}},
		})
		_ = readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5)

		client := fixture.dial(t)
		defer client.Close()
		writePacket(t, client, fixture.encoder, &mqtt.Connect{
			Version:  mqtt.ProtocolV5,
			ClientID: "client-normal-disc",
			Will: &mqtt.Will{
				Topic:   "status/normal",
				Payload: []byte("should-not-publish"),
			},
		})
		if ack := readPacket(t, client, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
			t.Fatalf("client CONNACK = %#v", ack)
		}
		writePacket(t, client, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5, ReasonCode: 0x00})
		expectClosed(t, client)
		expectNoPacket(t, subscriber)
		writePacket(t, subscriber, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		expectClosed(t, subscriber)
	})

	t.Run("disconnect with will 0x04 publishes will", func(t *testing.T) {
		subscriber := fixture.dial(t)
		defer subscriber.Close()
		writePacket(t, subscriber, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "sub-with-will"})
		_ = readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5)
		writePacket(t, subscriber, fixture.encoder, &mqtt.Subscribe{
			Version:       mqtt.ProtocolV5,
			PacketID:      1,
			Subscriptions: []mqtt.Subscription{{Filter: "status/+", QoS: mqtt.QoSAtMostOnce}},
		})
		_ = readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5)

		client := fixture.dial(t)
		defer client.Close()
		writePacket(t, client, fixture.encoder, &mqtt.Connect{
			Version:  mqtt.ProtocolV5,
			ClientID: "client-disc-with-will",
			Will: &mqtt.Will{
				Topic:   "status/with-will",
				Payload: []byte("explicit-will"),
			},
		})
		_ = readPacket(t, client, fixture.decoder, mqtt.ProtocolV5)
		writePacket(t, client, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5, ReasonCode: 0x04})
		expectClosed(t, client)

		willPacket := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
		if willPacket.Topic != "status/with-will" || string(willPacket.Payload) != "explicit-will" {
			t.Fatalf("expected explicit will, got: %#v", willPacket)
		}
		writePacket(t, subscriber, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		expectClosed(t, subscriber)
	})

	t.Run("will delay suppression on reconnect", func(t *testing.T) {
		subscriber := fixture.dial(t)
		defer subscriber.Close()
		writePacket(t, subscriber, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "sub-delay-rec"})
		_ = readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5)
		writePacket(t, subscriber, fixture.encoder, &mqtt.Subscribe{
			Version:       mqtt.ProtocolV5,
			PacketID:      1,
			Subscriptions: []mqtt.Subscription{{Filter: "status/+", QoS: mqtt.QoSAtMostOnce}},
		})
		_ = readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5)

		client := fixture.dial(t)
		writePacket(t, client, fixture.encoder, &mqtt.Connect{
			Version:               mqtt.ProtocolV5,
			ClientID:              "client-delay-rec",
			CleanStart:            true,
			SessionExpiryInterval: 30,
			Will: &mqtt.Will{
				Topic:         "status/delayed",
				Payload:       []byte("suppressed-will"),
				DelayInterval: 1,
			},
		})
		_ = readPacket(t, client, fixture.decoder, mqtt.ProtocolV5)
		client.Close()

		time.Sleep(50 * time.Millisecond)
		reconnect := fixture.dial(t)
		defer reconnect.Close()
		writePacket(t, reconnect, fixture.encoder, &mqtt.Connect{
			Version:               mqtt.ProtocolV5,
			ClientID:              "client-delay-rec",
			CleanStart:            false,
			SessionExpiryInterval: 30,
		})
		ack := readPacket(t, reconnect, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
		if ack.ReasonCode != 0 || !ack.SessionPresent {
			t.Fatalf("reconnect failed: %#v", ack)
		}

		time.Sleep(1200 * time.Millisecond)
		expectNoPacket(t, subscriber)
		writePacket(t, reconnect, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		expectClosed(t, reconnect)
		writePacket(t, subscriber, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		expectClosed(t, subscriber)
	})

	t.Run("session expiry timer clears persistent session", func(t *testing.T) {
		client := fixture.dial(t)
		writePacket(t, client, fixture.encoder, &mqtt.Connect{
			Version:               mqtt.ProtocolV5,
			ClientID:              "client-expiry-timer",
			CleanStart:            true,
			SessionExpiryInterval: 1,
		})
		if ack := readPacket(t, client, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
			t.Fatalf("client CONNACK = %#v", ack)
		}
		writePacket(t, client, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5, ReasonCode: 0x00})
		expectClosed(t, client)
		client.Close()

		if _, found, err := fixture.repo.Load("client-expiry-timer"); err != nil || !found {
			t.Fatalf("session should still exist before expiry: found=%v, err=%v", found, err)
		}

		time.Sleep(1300 * time.Millisecond)

		if _, found, err := fixture.repo.Load("client-expiry-timer"); err != nil || found {
			t.Fatalf("session should be purged after expiry timer: found=%v", found)
		}

		reconnect := fixture.dial(t)
		defer reconnect.Close()
		writePacket(t, reconnect, fixture.encoder, &mqtt.Connect{
			Version:               mqtt.ProtocolV5,
			ClientID:              "client-expiry-timer",
			CleanStart:            false,
			SessionExpiryInterval: 30,
		})
		reconnectAck := readPacket(t, reconnect, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
		if reconnectAck.ReasonCode != 0 || reconnectAck.SessionPresent {
			t.Fatalf("expected SessionPresent=false after expiry, got: %#v", reconnectAck)
		}
		writePacket(t, reconnect, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
		expectClosed(t, reconnect)
	})
}
