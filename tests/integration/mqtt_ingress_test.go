package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"narumq/internal/adapters/clock"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/connect"
	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

type ingressClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *ingressClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *ingressClock) Advance(delta time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(delta)
	clock.mu.Unlock()
}

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
	return newIngressFixtureWithClock(t, messageCapacity, byteCapacity, clock.SystemClock{})
}

func newIngressFixtureWithClock(t *testing.T, messageCapacity, byteCapacity int, clockSource applicationclock.Clock) *ingressFixture {
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
	connections, err := connect.New(clockSource, repository, deliveries, routes, clock.SystemScheduler{}, willBridge)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes, deliveries, retainedRepository)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	publications, err := publish.New(clockSource, routes, deliveries, retainedRepository)
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
			Clock:                clockSource,
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
}

// Traceability: PRD MQTT-04/MQTT-08/TEST-03/TEST-04/TEST-05; docs/specs/mqtt-protocol-and-session.md §4, §5; MQTT-3.3.2-5/6.
func TestMQTTIngressMessageExpiry(t *testing.T) {
	testClock := &ingressClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
	fixture := newIngressFixtureWithClock(t, 1024, 1<<20, testClock)
	defer fixture.close(t)

	publisher := fixture.dial(t)
	defer publisher.Close()
	writePacket(t, publisher, fixture.encoder, &mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		CleanStart: true,
		ClientID:   "expiry-publisher",
	})
	if ack := readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("publisher CONNACK = %#v", ack)
	}

	subscriber := fixture.dial(t)
	defer subscriber.Close()
	writePacket(t, subscriber, fixture.encoder, &mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		CleanStart: true,
		ClientID:   "expiry-subscriber",
	})
	if ack := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("subscriber CONNACK = %#v", ack)
	}

	writePacket(t, publisher, fixture.encoder, &mqtt.Publish{
		Version:                  mqtt.ProtocolV5,
		Topic:                    "expiry/remaining",
		Payload:                  []byte("remaining"),
		Retain:                   true,
		QoS:                      mqtt.QoSAtLeastOnce,
		PacketID:                 101,
		HasMessageExpiryInterval: true,
		MessageExpiryInterval:    10,
	})
	if ack := readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubAck); ack.PacketID != 101 {
		t.Fatalf("remaining expiry PUBACK = %#v", ack)
	}
	testClock.Advance(3 * time.Second)

	writePacket(t, subscriber, fixture.encoder, &mqtt.Subscribe{
		Version:       mqtt.ProtocolV5,
		PacketID:      1,
		Subscriptions: []mqtt.Subscription{{Filter: "expiry/remaining", QoS: mqtt.QoSAtMostOnce, RetainHandling: mqtt.SendRetainedAtSubscribe}},
	})
	if ack := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 1 {
		t.Fatalf("remaining expiry SUBACK = %#v", ack)
	}
	delivery := readPacket(t, subscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
	if delivery.Topic != "expiry/remaining" || string(delivery.Payload) != "remaining" || !delivery.Retain {
		t.Fatalf("remaining expiry delivery = %#v", delivery)
	}
	if !delivery.HasMessageExpiryInterval || delivery.MessageExpiryInterval != 7 {
		t.Fatalf("remaining expiry interval = %d (present=%v), want 7", delivery.MessageExpiryInterval, delivery.HasMessageExpiryInterval)
	}

	writePacket(t, publisher, fixture.encoder, &mqtt.Publish{
		Version:                  mqtt.ProtocolV5,
		Topic:                    "expiry/deadline",
		Payload:                  []byte("deadline"),
		Retain:                   true,
		QoS:                      mqtt.QoSAtLeastOnce,
		PacketID:                 102,
		HasMessageExpiryInterval: true,
		MessageExpiryInterval:    10,
	})
	if ack := readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubAck); ack.PacketID != 102 {
		t.Fatalf("deadline expiry PUBACK = %#v", ack)
	}
	testClock.Advance(10 * time.Second)

	deadlineSubscriber := fixture.dial(t)
	defer deadlineSubscriber.Close()
	writePacket(t, deadlineSubscriber, fixture.encoder, &mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		CleanStart: true,
		ClientID:   "expiry-deadline-subscriber",
	})
	if ack := readPacket(t, deadlineSubscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("deadline subscriber CONNACK = %#v", ack)
	}
	writePacket(t, deadlineSubscriber, fixture.encoder, &mqtt.Subscribe{
		Version:       mqtt.ProtocolV5,
		PacketID:      2,
		Subscriptions: []mqtt.Subscription{{Filter: "expiry/deadline", QoS: mqtt.QoSAtMostOnce, RetainHandling: mqtt.SendRetainedAtSubscribe}},
	})
	if ack := readPacket(t, deadlineSubscriber, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 2 {
		t.Fatalf("deadline expiry SUBACK = %#v", ack)
	}
	expectNoPacket(t, deadlineSubscriber)
}

// Traceability: PRD MQTT-02/TEST-03/TEST-04/TEST-05; docs/specs/mqtt-protocol-and-session.md §3.1, §3.2, §7; MQTT-4.3.2-1, MQTT-4.3.2-2.
func TestMQTTIngressQoS1Handshake(t *testing.T) {
	fixture := newIngressFixture(t)
	defer fixture.close(t)

	for _, tc := range []struct {
		name    string
		version mqtt.ProtocolVersion
	}{
		{name: "MQTT 5.0", version: mqtt.ProtocolV5},
		{name: "MQTT 3.1.1", version: mqtt.ProtocolV311},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subConn := fixture.dial(t)
			defer subConn.Close()
			writePacket(t, subConn, fixture.encoder, &mqtt.Connect{
				Version:    tc.version,
				CleanStart: true,
				ClientID:   "qos1-sub-" + tc.name,
			})
			connAck := readPacket(t, subConn, fixture.decoder, tc.version).(*mqtt.ConnAck)
			if connAck.ReasonCode != 0 {
				t.Fatalf("sub CONNACK failed: %#v", connAck)
			}
			writePacket(t, subConn, fixture.encoder, &mqtt.Subscribe{
				Version:  tc.version,
				PacketID: 1,
				Subscriptions: []mqtt.Subscription{
					{Filter: "qos1/handshake", QoS: mqtt.QoSAtLeastOnce},
				},
			})
			subAck := readPacket(t, subConn, fixture.decoder, tc.version).(*mqtt.SubAck)
			if subAck.PacketID != 1 {
				t.Fatalf("sub SUBACK packet ID = %d, want 1", subAck.PacketID)
			}

			pubConn := fixture.dial(t)
			defer pubConn.Close()
			writePacket(t, pubConn, fixture.encoder, &mqtt.Connect{
				Version:    tc.version,
				CleanStart: true,
				ClientID:   "qos1-pub-" + tc.name,
			})
			pubConnAck := readPacket(t, pubConn, fixture.decoder, tc.version).(*mqtt.ConnAck)
			if pubConnAck.ReasonCode != 0 {
				t.Fatalf("pub CONNACK failed: %#v", pubConnAck)
			}

			// Publish QoS 1 message
			writePacket(t, pubConn, fixture.encoder, &mqtt.Publish{
				Version:  tc.version,
				Topic:    "qos1/handshake",
				Payload:  []byte("hello-qos1"),
				QoS:      mqtt.QoSAtLeastOnce,
				PacketID: 101,
			})

			// Publisher receives PUBACK
			pubAck := readPacket(t, pubConn, fixture.decoder, tc.version).(*mqtt.PubAck)
			if pubAck.PacketID != 101 {
				t.Fatalf("PUBACK packet ID = %d, want 101", pubAck.PacketID)
			}
			if tc.version == mqtt.ProtocolV5 && pubAck.ReasonCode != 0 {
				t.Fatalf("PUBACK reason code = 0x%02X, want 0", pubAck.ReasonCode)
			}

			// Subscriber receives delivery
			delivery := readPacket(t, subConn, fixture.decoder, tc.version).(*mqtt.Publish)
			if delivery.Topic != "qos1/handshake" || string(delivery.Payload) != "hello-qos1" {
				t.Fatalf("delivered publication = %#v", delivery)
			}
			if delivery.QoS != mqtt.QoSAtLeastOnce || delivery.PacketID == 0 {
				t.Fatalf("delivery QoS/PacketID invalid: QoS=%v, PacketID=%v", delivery.QoS, delivery.PacketID)
			}
			// Subscriber acknowledges delivery
			writePacket(t, subConn, fixture.encoder, &mqtt.PubAck{
				Version:  tc.version,
				PacketID: delivery.PacketID,
			})

			// Publisher re-sends duplicate QoS 1 PUBLISH (DUP=true)
			writePacket(t, pubConn, fixture.encoder, &mqtt.Publish{
				Version:  tc.version,
				Topic:    "qos1/handshake",
				Payload:  []byte("hello-qos1"),
				QoS:      mqtt.QoSAtLeastOnce,
				PacketID: 101,
				DUP:      true,
			})

			// Publisher receives PUBACK again
			dupAck := readPacket(t, pubConn, fixture.decoder, tc.version).(*mqtt.PubAck)
			if dupAck.PacketID != 101 {
				t.Fatalf("dup PUBACK packet ID = %d, want 101", dupAck.PacketID)
			}

			// Subscriber must NOT receive duplicate message
			expectNoPacket(t, subConn)
		})
	}
}

// Traceability: PRD MQTT-02/TEST-04/TEST-05; docs/specs/mqtt-protocol-and-session.md §3.1, §3.2, §7; MQTT-4.3.3-1, MQTT-4.3.3-2.
func TestMQTTIngressQoS2Handshake(t *testing.T) {
	fixture := newIngressFixture(t)
	defer fixture.close(t)

	for _, tc := range []struct {
		name    string
		version mqtt.ProtocolVersion
	}{
		{name: "MQTT 5.0", version: mqtt.ProtocolV5},
		{name: "MQTT 3.1.1", version: mqtt.ProtocolV311},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subConn := fixture.dial(t)
			defer subConn.Close()
			writePacket(t, subConn, fixture.encoder, &mqtt.Connect{
				Version:    tc.version,
				CleanStart: true,
				ClientID:   "qos2-sub-" + tc.name,
			})
			connAck := readPacket(t, subConn, fixture.decoder, tc.version).(*mqtt.ConnAck)
			if connAck.ReasonCode != 0 {
				t.Fatalf("sub CONNACK failed: %#v", connAck)
			}
			writePacket(t, subConn, fixture.encoder, &mqtt.Subscribe{
				Version:  tc.version,
				PacketID: 2,
				Subscriptions: []mqtt.Subscription{
					{Filter: "qos2/handshake", QoS: mqtt.QoSExactlyOnce},
				},
			})
			subAck := readPacket(t, subConn, fixture.decoder, tc.version).(*mqtt.SubAck)
			if subAck.PacketID != 2 {
				t.Fatalf("sub SUBACK packet ID = %d, want 2", subAck.PacketID)
			}

			pubConn := fixture.dial(t)
			defer pubConn.Close()
			writePacket(t, pubConn, fixture.encoder, &mqtt.Connect{
				Version:    tc.version,
				CleanStart: true,
				ClientID:   "qos2-pub-" + tc.name,
			})
			pubConnAck := readPacket(t, pubConn, fixture.decoder, tc.version).(*mqtt.ConnAck)
			if pubConnAck.ReasonCode != 0 {
				t.Fatalf("pub CONNACK failed: %#v", pubConnAck)
			}

			// Publisher sends QoS 2 PUBLISH
			writePacket(t, pubConn, fixture.encoder, &mqtt.Publish{
				Version:  tc.version,
				Topic:    "qos2/handshake",
				Payload:  []byte("hello-qos2"),
				QoS:      mqtt.QoSExactlyOnce,
				PacketID: 202,
			})

			// Publisher receives PUBREC
			pubRec := readPacket(t, pubConn, fixture.decoder, tc.version).(*mqtt.PubRec)
			if pubRec.PacketID != 202 {
				t.Fatalf("PUBREC packet ID = %d, want 202", pubRec.PacketID)
			}
			if tc.version == mqtt.ProtocolV5 && pubRec.ReasonCode != 0 {
				t.Fatalf("PUBREC reason code = 0x%02X, want 0", pubRec.ReasonCode)
			}

			// Message must NOT be delivered yet prior to PUBREL
			expectNoPacket(t, subConn)

			// Publisher resends duplicate QoS 2 PUBLISH (DUP=true)
			writePacket(t, pubConn, fixture.encoder, &mqtt.Publish{
				Version:  tc.version,
				Topic:    "qos2/handshake",
				Payload:  []byte("hello-qos2"),
				QoS:      mqtt.QoSExactlyOnce,
				PacketID: 202,
				DUP:      true,
			})
			dupRec := readPacket(t, pubConn, fixture.decoder, tc.version).(*mqtt.PubRec)
			if dupRec.PacketID != 202 {
				t.Fatalf("dup PUBREC packet ID = %d, want 202", dupRec.PacketID)
			}

			// Publisher sends PUBREL
			writePacket(t, pubConn, fixture.encoder, &mqtt.PubRel{
				Version:  tc.version,
				PacketID: 202,
			})

			// Publisher receives PUBCOMP
			pubComp := readPacket(t, pubConn, fixture.decoder, tc.version).(*mqtt.PubComp)
			if pubComp.PacketID != 202 {
				t.Fatalf("PUBCOMP packet ID = %d, want 202", pubComp.PacketID)
			}
			if tc.version == mqtt.ProtocolV5 && pubComp.ReasonCode != 0 {
				t.Fatalf("PUBCOMP reason code = 0x%02X, want 0", pubComp.ReasonCode)
			}

			// Subscriber now receives the delivery
			delivery := readPacket(t, subConn, fixture.decoder, tc.version).(*mqtt.Publish)
			if delivery.Topic != "qos2/handshake" || string(delivery.Payload) != "hello-qos2" {
				t.Fatalf("delivered publication = %#v", delivery)
			}
			if delivery.QoS != mqtt.QoSExactlyOnce || delivery.PacketID == 0 {
				t.Fatalf("delivery QoS/PacketID invalid: QoS=%v, PacketID=%v", delivery.QoS, delivery.PacketID)
			}

			// Subscriber completes QoS 2 outbound handshake with broker
			writePacket(t, subConn, fixture.encoder, &mqtt.PubRec{
				Version:  tc.version,
				PacketID: delivery.PacketID,
			})
			brokerRel := readPacket(t, subConn, fixture.decoder, tc.version).(*mqtt.PubRel)
			if brokerRel.PacketID != delivery.PacketID {
				t.Fatalf("broker PUBREL packet ID = %d, want %d", brokerRel.PacketID, delivery.PacketID)
			}
			writePacket(t, subConn, fixture.encoder, &mqtt.PubComp{
				Version:  tc.version,
				PacketID: delivery.PacketID,
			})

			// Duplicate PUBREL from publisher returns duplicate PUBCOMP
			writePacket(t, pubConn, fixture.encoder, &mqtt.PubRel{
				Version:  tc.version,
				PacketID: 202,
			})
			dupComp := readPacket(t, pubConn, fixture.decoder, tc.version).(*mqtt.PubComp)
			if dupComp.PacketID != 202 {
				t.Fatalf("dup PUBCOMP packet ID = %d, want 202", dupComp.PacketID)
			}

			// Subscriber must NOT receive duplicate
			expectNoPacket(t, subConn)
		})
	}
}

// Traceability: PRD MQTT-02/TEST-03/TEST-04/TEST-05; docs/specs/mqtt-protocol-and-session.md §3.1, §3.2, §7; MQTT-4.3.2-1, MQTT-4.3.2-2, MQTT-4.3.3-1, MQTT-4.3.3-2.
func TestMQTTIngressQoSOutboundDelivery(t *testing.T) {
	fixture := newIngressFixture(t)
	defer fixture.close(t)

	// Subscriber 1: QoS 1 (MQTT 5)
	sub1 := fixture.dial(t)
	defer sub1.Close()
	writePacket(t, sub1, fixture.encoder, &mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		CleanStart: true,
		ClientID:   "sub-qos1-client",
	})
	if ack := readPacket(t, sub1, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("sub1 CONNACK failed: %#v", ack)
	}
	writePacket(t, sub1, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 10,
		Subscriptions: []mqtt.Subscription{
			{Filter: "test/outbound/qos1", QoS: mqtt.QoSAtLeastOnce},
		},
	})
	if ack := readPacket(t, sub1, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 10 {
		t.Fatalf("sub1 SUBACK failed: %#v", ack)
	}

	// Subscriber 2: QoS 2 (MQTT 3.1.1)
	sub2 := fixture.dial(t)
	defer sub2.Close()
	writePacket(t, sub2, fixture.encoder, &mqtt.Connect{
		Version:  mqtt.ProtocolV311,
		ClientID: "sub-qos2-client",
	})
	if ack := readPacket(t, sub2, fixture.decoder, mqtt.ProtocolV311).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("sub2 CONNACK failed: %#v", ack)
	}
	writePacket(t, sub2, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV311,
		PacketID: 20,
		Subscriptions: []mqtt.Subscription{
			{Filter: "test/outbound/qos2", QoS: mqtt.QoSExactlyOnce},
		},
	})
	if ack := readPacket(t, sub2, fixture.decoder, mqtt.ProtocolV311).(*mqtt.SubAck); ack.PacketID != 20 {
		t.Fatalf("sub2 SUBACK failed: %#v", ack)
	}

	// Publisher sends to both
	pub := fixture.dial(t)
	defer pub.Close()
	writePacket(t, pub, fixture.encoder, &mqtt.Connect{
		Version:    mqtt.ProtocolV5,
		CleanStart: true,
		ClientID:   "pub-outbound-client",
	})
	if ack := readPacket(t, pub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("pub CONNACK failed: %#v", ack)
	}

	// 1. QoS 1 Outbound Delivery Flow
	writePacket(t, pub, fixture.encoder, &mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "test/outbound/qos1",
		Payload:  []byte("outbound-1"),
		QoS:      mqtt.QoSAtLeastOnce,
		PacketID: 1,
	})
	pubAck := readPacket(t, pub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
	if pubAck.PacketID != 1 {
		t.Fatalf("pub PUBACK packet ID = %d, want 1", pubAck.PacketID)
	}

	deliv1 := readPacket(t, sub1, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
	if deliv1.Topic != "test/outbound/qos1" || string(deliv1.Payload) != "outbound-1" || deliv1.QoS != mqtt.QoSAtLeastOnce {
		t.Fatalf("deliv1 invalid: %#v", deliv1)
	}
	// Sub1 replies PUBACK
	writePacket(t, sub1, fixture.encoder, &mqtt.PubAck{
		Version:  mqtt.ProtocolV5,
		PacketID: deliv1.PacketID,
	})

	// 2. QoS 2 Outbound Delivery Flow
	writePacket(t, pub, fixture.encoder, &mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "test/outbound/qos2",
		Payload:  []byte("outbound-2"),
		QoS:      mqtt.QoSExactlyOnce,
		PacketID: 2,
	})
	pubRec := readPacket(t, pub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubRec)
	if pubRec.PacketID != 2 {
		t.Fatalf("pub PUBREC packet ID = %d, want 2", pubRec.PacketID)
	}
	writePacket(t, pub, fixture.encoder, &mqtt.PubRel{
		Version:  mqtt.ProtocolV5,
		PacketID: 2,
	})
	pubComp := readPacket(t, pub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubComp)
	if pubComp.PacketID != 2 {
		t.Fatalf("pub PUBCOMP packet ID = %d, want 2", pubComp.PacketID)
	}

	deliv2 := readPacket(t, sub2, fixture.decoder, mqtt.ProtocolV311).(*mqtt.Publish)
	if deliv2.Topic != "test/outbound/qos2" || string(deliv2.Payload) != "outbound-2" || deliv2.QoS != mqtt.QoSExactlyOnce {
		t.Fatalf("deliv2 invalid: %#v", deliv2)
	}
	// Sub2 sends PUBREC
	writePacket(t, sub2, fixture.encoder, &mqtt.PubRec{
		Version:  mqtt.ProtocolV311,
		PacketID: deliv2.PacketID,
	})
	// Sub2 receives PUBREL from broker
	brokerRel := readPacket(t, sub2, fixture.decoder, mqtt.ProtocolV311).(*mqtt.PubRel)
	if brokerRel.PacketID != deliv2.PacketID {
		t.Fatalf("broker PUBREL ID = %d, want %d", brokerRel.PacketID, deliv2.PacketID)
	}
	// Sub2 sends PUBCOMP to broker
	writePacket(t, sub2, fixture.encoder, &mqtt.PubComp{
		Version:  mqtt.ProtocolV311,
		PacketID: deliv2.PacketID,
	})

	// Ensure no unexpected packets remaining on connections
	expectNoPacket(t, sub1)
	expectNoPacket(t, sub2)
	expectNoPacket(t, pub)
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

// Traceability: PRD MQTT-07/TEST-03; MQTT-3.8.3-4, MQTT-4.8.2-1, MQTT-4.8.2-2, MQTT-4.8.2-3.
func TestMQTTIngressSharedSubscription(t *testing.T) {
	fixture := newIngressFixture(t)
	defer fixture.close(t)

	// 1. Connect Worker A with $share/workers/tasks/+
	workerA := fixture.dial(t)
	defer workerA.Close()
	writePacket(t, workerA, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "worker-a"})
	if ack := readPacket(t, workerA, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("worker-a CONNACK = %#v", ack)
	}
	writePacket(t, workerA, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{{
			Filter: "$share/workers/tasks/+",
			QoS:    mqtt.QoSAtLeastOnce,
		}},
	})
	if ack := readPacket(t, workerA, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 1 || !reflect.DeepEqual(ack.ReasonCodes, []byte{0x01}) {
		t.Fatalf("worker-a SUBACK = %#v", ack)
	}

	// 2. Connect Worker B with $share/workers/tasks/+
	workerB := fixture.dial(t)
	defer workerB.Close()
	writePacket(t, workerB, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "worker-b"})
	if ack := readPacket(t, workerB, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("worker-b CONNACK = %#v", ack)
	}
	writePacket(t, workerB, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{{
			Filter: "$share/workers/tasks/+",
			QoS:    mqtt.QoSAtLeastOnce,
		}},
	})
	if ack := readPacket(t, workerB, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 1 || !reflect.DeepEqual(ack.ReasonCodes, []byte{0x01}) {
		t.Fatalf("worker-b SUBACK = %#v", ack)
	}

	// 3. Connect Auditor with non-shared tasks/+
	auditor := fixture.dial(t)
	defer auditor.Close()
	writePacket(t, auditor, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "auditor"})
	if ack := readPacket(t, auditor, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("auditor CONNACK = %#v", ack)
	}
	writePacket(t, auditor, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{{
			Filter: "tasks/+",
			QoS:    mqtt.QoSAtLeastOnce,
		}},
	})
	if ack := readPacket(t, auditor, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 1 || !reflect.DeepEqual(ack.ReasonCodes, []byte{0x01}) {
		t.Fatalf("auditor SUBACK = %#v", ack)
	}

	// 4. Connect Publisher
	publisher := fixture.dial(t)
	defer publisher.Close()
	writePacket(t, publisher, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "publisher"})
	if ack := readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("publisher CONNACK = %#v", ack)
	}

	// 5. Publisher publishes tasks/1, tasks/2, tasks/3, tasks/4 at QoS 1
	// Auditor must receive all 4 messages.
	// Worker A and Worker B must receive 2 messages each via round-robin.
	tasks := []string{"tasks/1", "tasks/2", "tasks/3", "tasks/4"}
	expectedWorker := []net.Conn{workerA, workerB, workerA, workerB}
	workerNames := []string{"worker-a", "worker-b", "worker-a", "worker-b"}

	for i, taskTopic := range tasks {
		pktID := mqtt.PacketIdentifier(i + 1)
		writePacket(t, publisher, fixture.encoder, &mqtt.Publish{
			Version:  mqtt.ProtocolV5,
			Topic:    taskTopic,
			Payload:  []byte(fmt.Sprintf("payload-%d", pktID)),
			QoS:      mqtt.QoSAtLeastOnce,
			PacketID: pktID,
		})

		// Publisher gets PUBACK
		if pubAck := readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubAck); pubAck.PacketID != pktID {
			t.Fatalf("publisher PUBACK = %#v, want packet ID %d", pubAck, pktID)
		}

		// Auditor receives publication
		auditorPub := readPacket(t, auditor, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
		if auditorPub.Topic != taskTopic || string(auditorPub.Payload) != fmt.Sprintf("payload-%d", pktID) {
			t.Fatalf("auditor publish = %#v, want topic %s", auditorPub, taskTopic)
		}
		writePacket(t, auditor, fixture.encoder, &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: auditorPub.PacketID})

		// Expected worker receives publication
		targetWorker := expectedWorker[i]
		workerPub := readPacket(t, targetWorker, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
		if workerPub.Topic != taskTopic || string(workerPub.Payload) != fmt.Sprintf("payload-%d", pktID) {
			t.Fatalf("%s publish = %#v, want topic %s", workerNames[i], workerPub, taskTopic)
		}
		writePacket(t, targetWorker, fixture.encoder, &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: workerPub.PacketID})
	}

	// 6. Test NoLocal on shared subscription rejected with 0x82 Protocol Error (MQTT-3.8.3-4)
	writePacket(t, publisher, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 10,
		Subscriptions: []mqtt.Subscription{{
			Filter:  "$share/workers/tasks/+",
			QoS:     mqtt.QoSAtLeastOnce,
			NoLocal: true,
		}},
	})
	if ack := readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 10 || !reflect.DeepEqual(ack.ReasonCodes, []byte{0x82}) {
		t.Fatalf("NoLocal shared subscription SUBACK = %#v, want reason 0x82", ack)
	}

	// 7. Test Retained message suppression for shared subscription (MQTT-4.8.2-1)
	// Publish retained message to tasks/retained
	writePacket(t, publisher, fixture.encoder, &mqtt.Publish{
		Version:  mqtt.ProtocolV5,
		Topic:    "tasks/retained",
		Payload:  []byte("retained-content"),
		QoS:      mqtt.QoSAtLeastOnce,
		PacketID: 20,
		Retain:   true,
	})
	if pubAck := readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubAck); pubAck.PacketID != 20 {
		t.Fatalf("retained publish PUBACK = %#v", pubAck)
	}
	// Auditor gets the live retained delivery
	auditorRetained := readPacket(t, auditor, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
	if auditorRetained.Topic != "tasks/retained" {
		t.Fatalf("auditor retained = %#v", auditorRetained)
	}
	writePacket(t, auditor, fixture.encoder, &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: auditorRetained.PacketID})
	// One of worker-a / worker-b gets the live retained delivery (cursor 4 -> chosen worker-a)
	workerLive := readPacket(t, workerA, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
	if workerLive.Topic != "tasks/retained" {
		t.Fatalf("worker-a live = %#v", workerLive)
	}
	writePacket(t, workerA, fixture.encoder, &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: workerLive.PacketID})

	// Now connect a new Worker C and subscribe to $share/workers/tasks/+ with SendRetainedAtSubscribe
	workerC := fixture.dial(t)
	defer workerC.Close()
	writePacket(t, workerC, fixture.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "worker-c"})
	if ack := readPacket(t, workerC, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); ack.ReasonCode != 0 {
		t.Fatalf("worker-c CONNACK = %#v", ack)
	}
	writePacket(t, workerC, fixture.encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{{
			Filter:         "$share/workers/tasks/+",
			QoS:            mqtt.QoSAtLeastOnce,
			RetainHandling: mqtt.SendRetainedAtSubscribe,
		}},
	})
	if ack := readPacket(t, workerC, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); ack.PacketID != 1 || !reflect.DeepEqual(ack.ReasonCodes, []byte{0x01}) {
		t.Fatalf("worker-c SUBACK = %#v", ack)
	}

	// Worker C must NOT receive any retained replay
	expectNoPacket(t, workerC)

	// Clean socket checks
	expectNoPacket(t, workerA)
	expectNoPacket(t, workerB)
	expectNoPacket(t, auditor)
	expectNoPacket(t, publisher)
}
