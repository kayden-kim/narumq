package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	authentication "narumq/internal/adapters/authentication"
	metricsadapter "narumq/internal/adapters/metrics"
	tlsadapter "narumq/internal/adapters/tls"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

// Traceability: PRD MQTT-10; docs/specs/mqtt-protocol-and-session.md §1.
func TestMQTTWebSocketProtocolAndQoS(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{WS: true})
	defer fixture.close(t)
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		t.Run(fmt.Sprintf("protocol-%d", version), func(t *testing.T) {
			subscriber := websocketMQTTDial(t, "ws", fixture.wsAddr, nil)
			defer subscriber.Close()
			websocketMQTTConnect(t, fixture, subscriber, version, "protocol-subscriber", nil)
			writePacket(t, subscriber, fixture.encoder, &mqtt.Subscribe{Version: version, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "ws/qos/#", QoS: mqtt.QoSExactlyOnce}}})
			if ack, ok := readPacket(t, subscriber, fixture.decoder, version).(*mqtt.SubAck); !ok || len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] > 2 {
				t.Fatalf("SUBACK=%#v", ack)
			}
			publisher := websocketMQTTDial(t, "ws", fixture.wsAddr, nil)
			defer publisher.Close()
			websocketMQTTConnect(t, fixture, publisher, version, "protocol-publisher", nil)
			for qos := byte(0); qos <= 2; qos++ {
				var packetID mqtt.PacketIdentifier
				if qos > 0 {
					packetID = mqtt.PacketIdentifier(qos + 10)
				}
				writePacket(t, publisher, fixture.encoder, &mqtt.Publish{Version: version, Topic: fmt.Sprintf("ws/qos/%d", qos), Payload: []byte(fmt.Sprintf("payload-%d", qos)), QoS: mqtt.QoS(qos), PacketID: packetID})
				switch qos {
				case 1:
					if _, ok := readPacket(t, publisher, fixture.decoder, version).(*mqtt.PubAck); !ok {
						t.Fatal("expected PUBACK")
					}
				case 2:
					if _, ok := readPacket(t, publisher, fixture.decoder, version).(*mqtt.PubRec); !ok {
						t.Fatal("expected PUBREC")
					}
					writePacket(t, publisher, fixture.encoder, &mqtt.PubRel{Version: version, PacketID: packetID})
					if _, ok := readPacket(t, publisher, fixture.decoder, version).(*mqtt.PubComp); !ok {
						t.Fatal("expected PUBCOMP")
					}
				}
				delivery, ok := readPacket(t, subscriber, fixture.decoder, version).(*mqtt.Publish)
				if !ok || delivery.Topic != fmt.Sprintf("ws/qos/%d", qos) || string(delivery.Payload) != fmt.Sprintf("payload-%d", qos) || byte(delivery.QoS) != qos {
					t.Fatalf("QoS %d delivery=%#v", qos, delivery)
				}
				ackSubscriberPublish(t, fixture, subscriber, version, delivery)
			}
		})
	}
}

// Traceability: PRD MQTT-10; docs/specs/mqtt-protocol-and-session.md §1.
func TestMQTTWebSocketCrossTransportDelivery(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{WS: true, WSS: true, DualTCP: true})
	defer fixture.close(t)
	wsSub := websocketMQTTDial(t, "ws", fixture.wsAddr, nil)
	defer wsSub.Close()
	websocketMQTTConnect(t, fixture, wsSub, mqtt.ProtocolV5, "ws-subscriber", nil)
	websocketMQTTSubscribe(t, fixture, wsSub, mqtt.ProtocolV5, "cross/tcp-ws")
	tcpPub, err := net.Dial("tcp", fixture.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer tcpPub.Close()
	mqttTCPConnect(t, fixture, tcpPub, "tcp-publisher")
	writePacket(t, tcpPub, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "cross/tcp-ws", Payload: []byte("tcp-to-ws")})
	assertWebSocketPublish(t, fixture, wsSub, mqtt.ProtocolV5, "cross/tcp-ws", "tcp-to-ws")

	tlsSub := fixture.dialTLS(t, nil, nil)
	defer tlsSub.Close()
	mqttTCPConnect(t, fixture, tlsSub, "tls-subscriber")
	writePacket(t, tlsSub, fixture.encoder, &mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 2, Subscriptions: []mqtt.Subscription{{Filter: "cross/wss-tls", QoS: mqtt.QoSAtMostOnce}}})
	readPacket(t, tlsSub, fixture.decoder, mqtt.ProtocolV5)
	wssPub := websocketMQTTDial(t, "wss", fixture.wssAddr, fixture.tlsClientConfig(t, nil))
	defer wssPub.Close()
	websocketMQTTConnect(t, fixture, wssPub, mqtt.ProtocolV5, "wss-publisher", nil)
	writePacket(t, wssPub, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "cross/wss-tls", Payload: []byte("wss-to-tls")})
	assertWebSocketPublish(t, fixture, tlsSub, mqtt.ProtocolV5, "cross/wss-tls", "wss-to-tls")
}

// Traceability: PRD MQTT-10; SEC-05; docs/specs/mqtt-protocol-and-session.md §1.
func TestMQTTWebSocketPasswordAuthentication(t *testing.T) {
	password := []byte("ws-password")
	encodedHash, err := authentication.HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	passwordHash, err := authentication.ParsePasswordHash(encodedHash)
	if err != nil {
		t.Fatal(err)
	}
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	fixture := newTLSFixture(t, tlsFixtureOptions{WSS: true, PasswordBindings: []authentication.PasswordIdentityBinding{{Username: "ws-user", PasswordHash: passwordHash, TenantID: tenantID, Subject: "ws-user", CredentialID: "ws-password", AllowedClientIDs: []string{"ws-password-client"}, Roles: []security.Role{"client"}}}})
	defer fixture.close(t)
	conn := websocketMQTTDial(t, "wss", fixture.wssAddr, fixture.tlsClientConfig(t, nil))
	defer conn.Close()
	websocketMQTTConnect(t, fixture, conn, mqtt.ProtocolV5, "ws-password-client", &mqtt.Connect{HasUsername: true, Username: "ws-user", HasPassword: true, Password: password})
}

// Traceability: PRD MQTT-10; SEC-03; docs/specs/mqtt-protocol-and-session.md §1.
func TestMQTTWebSocketWSSMTLSIdentityAndClientCertificateRejection(t *testing.T) {
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	topicFilter, err := tenancy.NewTopicFilter(tenantID, "identity/#")
	if err != nil {
		t.Fatal(err)
	}
	rule, err := security.NewRule(tenantID, security.Role("ws-device"), security.EffectAllow, security.ActionSubscribe, topicFilter)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := security.NewPolicy([]security.Rule{rule})
	if err != nil {
		t.Fatal(err)
	}
	fixture := newTLSFixture(t, tlsFixtureOptions{WSS: true, ClientAuth: tlsadapter.ClientAuthRequire, MTLSIdentity: "ws-device", SecurityPolicy: &policy})
	defer fixture.close(t)
	clientCert := generateIntegrationClientCert(t, fixture.ca, "ws-device", false)
	conn := websocketMQTTDial(t, "wss", fixture.wssAddr, fixture.tlsClientConfig(t, &clientCert))
	defer conn.Close()
	websocketMQTTConnect(t, fixture, conn, mqtt.ProtocolV5, "ws-device", nil)
	websocketMQTTSubscribe(t, fixture, conn, mqtt.ProtocolV5, "identity/#")
	if unauthenticated, _, err := coderws.Dial(
		context.Background(),
		"wss://"+fixture.wssAddr+"/mqtt",
		websocketMQTTDialOptions(fixture.tlsClientConfig(t, nil)),
	); err == nil {
		unauthenticated.CloseNow()
		t.Fatal("WSS client without a certificate completed the TLS/WebSocket handshake")
	}
}

// Traceability: PRD MQTT-10; docs/specs/mqtt-protocol-and-session.md §1.
func TestMQTTWebSocketKeepAliveTimeoutPublishesWillWithoutWarning(t *testing.T) {
	var logs synchronizedBuffer
	fixture := newTLSFixture(t, tlsFixtureOptions{WS: true, LogOutput: &logs})
	defer fixture.close(t)
	watcher := websocketMQTTDial(t, "ws", fixture.wsAddr, nil)
	defer watcher.Close()
	websocketMQTTConnect(t, fixture, watcher, mqtt.ProtocolV5, "will-watcher", nil)
	websocketMQTTSubscribe(t, fixture, watcher, mqtt.ProtocolV5, "ws/will")
	willClient := websocketMQTTDial(t, "ws", fixture.wsAddr, nil)
	defer willClient.Close()
	websocketMQTTConnect(t, fixture, willClient, mqtt.ProtocolV5, "will-sender", &mqtt.Connect{KeepAlive: 1, Will: &mqtt.Will{Topic: "ws/will", Payload: []byte("expired"), QoS: 0}})
	if err := willClient.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, willClient); errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("WebSocket did not close after MQTT KeepAlive timeout: %v", err)
	}
	assertWebSocketPublish(t, fixture, watcher, mqtt.ProtocolV5, "ws/will", "expired")
	if strings.Contains(logs.String(), "MQTT connection handling failed") {
		t.Fatalf("WebSocket keep-alive timeout emitted warning: %s", logs.String())
	}
}

// Traceability: PRD MQTT-10; SEC-12; docs/specs/mqtt-protocol-and-session.md §1.
func TestMQTTWebSocketBlocklistedSourceIPRejected(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{WS: true, Blocklist: []security.BlockEntry{{Kind: security.BlockKindIPPrefix, Value: "127.0.0.1/32"}}})
	defer fixture.close(t)
	conn := websocketMQTTDial(t, "ws", fixture.wsAddr, nil)
	defer conn.Close()
	wire, err := fixture.encoder.Append(nil, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "blocked-ws"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(wire); err != nil {
		t.Fatal(err)
	}
	packet := readPacket(t, conn, fixture.decoder, mqtt.ProtocolV5)
	ack, ok := packet.(*mqtt.ConnAck)
	if !ok || ack.ReasonCode == 0 {
		t.Fatalf("blocklisted source CONNACK=%#v, want rejection", packet)
	}
}

// Traceability: PRD MQTT-10; docs/specs/mqtt-protocol-and-session.md §1.
func TestMQTTWebSocketFrameBoundaries(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{WS: true})
	defer fixture.close(t)
	client := websocketMQTTDialRaw(t, "ws", fixture.wsAddr, nil)
	defer client.CloseNow()
	connectWire, err := fixture.encoder.Append(nil, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "split-frame"})
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range [][]byte{connectWire[:2], connectWire[2:]} {
		if err := client.Write(context.Background(), coderws.MessageBinary, part); err != nil {
			t.Fatal(err)
		}
	}
	conn := coderws.NetConn(context.Background(), client, coderws.MessageBinary)
	if _, ok := readPacket(t, conn, fixture.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck); !ok {
		t.Fatal("split CONNECT did not receive CONNACK")
	}
	writePacket(t, conn, fixture.encoder, &mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "frame/#", QoS: mqtt.QoSAtMostOnce}}})
	if _, ok := readPacket(t, conn, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck); !ok {
		t.Fatal("SUBSCRIBE did not receive SUBACK")
	}
	publishA, err := fixture.encoder.Append(nil, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "frame/a", Payload: []byte("A")})
	if err != nil {
		t.Fatal(err)
	}
	publishB, err := fixture.encoder.Append(nil, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "frame/b", Payload: []byte("B")})
	if err != nil {
		t.Fatal(err)
	}
	combined := append(append([]byte(nil), publishA...), publishB...)
	if err := client.Write(context.Background(), coderws.MessageBinary, combined); err != nil {
		t.Fatal(err)
	}
	assertWebSocketPublish(t, fixture, conn, mqtt.ProtocolV5, "frame/a", "A")
	assertWebSocketPublish(t, fixture, conn, mqtt.ProtocolV5, "frame/b", "B")
}

// Traceability: PRD MQTT-10; docs/specs/mqtt-protocol-and-session.md §1.
func TestMQTTWebSocketCloseWithLiveClients(t *testing.T) {
	fixture := newTLSFixture(t, tlsFixtureOptions{WS: true})
	clients := make([]net.Conn, 3)
	for i := range clients {
		clients[i] = websocketMQTTDial(t, "ws", fixture.wsAddr, nil)
		websocketMQTTConnect(t, fixture, clients[i], mqtt.ProtocolV5, fmt.Sprintf("live-%d", i), nil)
	}
	start := time.Now()
	fixture.close(t)
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("fixture shutdown with live WebSockets took %s", elapsed)
	}
	for _, conn := range clients {
		_ = conn.Close()
	}
}

// Traceability: PRD MQTT-10; PRD MQTT-09; docs/specs/mqtt-protocol-and-session.md §1.
func TestMQTTWebSocketMetricsAndAdmission(t *testing.T) {
	registry := metricsadapter.NewRegistry()
	fixture := newTLSFixture(t, tlsFixtureOptions{WS: true, WSS: true, WSMaxConnections: 1, Metrics: registry})
	defer fixture.close(t)
	ws := websocketMQTTDial(t, "ws", fixture.wsAddr, nil)
	websocketMQTTConnect(t, fixture, ws, mqtt.ProtocolV5, "metric-ws", nil)
	wss := websocketMQTTDial(t, "wss", fixture.wssAddr, fixture.tlsClientConfig(t, nil))
	websocketMQTTConnect(t, fixture, wss, mqtt.ProtocolV5, "metric-wss", nil)
	if _, _, err := coderws.Dial(context.Background(), "ws://"+fixture.wsAddr+"/mqtt", &coderws.DialOptions{Subprotocols: []string{"mqtt"}}); err == nil {
		t.Fatal("second WS client exceeded max connections without rejection")
	}
	_ = ws.Close()
	_ = wss.Close()
	rendered := string(registry.Render(false))
	for _, want := range []string{
		`narumq_connections_total{protocol="MQTT_5_0",transport="ws",result="success"} 1`,
		`narumq_connections_total{protocol="MQTT_5_0",transport="wss",result="success"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing connection metric %q: %s", want, rendered)
		}
	}
	if !strings.Contains(rendered, `narumq_mqtt_listener_admission_rejected_total{transport="ws",reason="max_connections"} 1`) {
		t.Fatalf("missing WS max-connection admission metric: %s", rendered)
	}
}

func websocketMQTTDial(t *testing.T, scheme, address string, tlsConfig *tls.Config) net.Conn {
	t.Helper()
	conn := websocketMQTTDialRaw(t, scheme, address, tlsConfig)
	return coderws.NetConn(context.Background(), conn, coderws.MessageBinary)
}

func websocketMQTTDialRaw(t *testing.T, scheme, address string, tlsConfig *tls.Config) *coderws.Conn {
	t.Helper()
	options := websocketMQTTDialOptions(tlsConfig)
	conn, _, err := coderws.Dial(context.Background(), scheme+"://"+address+"/mqtt", options)
	if err != nil {
		t.Fatalf("dial %s WebSocket %s: %v", scheme, address, err)
	}
	return conn
}

func websocketMQTTDialOptions(tlsConfig *tls.Config) *coderws.DialOptions {
	options := &coderws.DialOptions{Subprotocols: []string{"mqtt"}}
	if tlsConfig != nil {
		options.HTTPClient = &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
	}
	return options
}

func (f *tlsFixture) tlsClientConfig(t *testing.T, clientCert *testCert) *tls.Config {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(f.ca.cert)
	config := &tls.Config{RootCAs: roots, ServerName: "127.0.0.1"}
	if clientCert != nil {
		pair, err := tls.X509KeyPair(clientCert.certPEM, clientCert.keyPEM)
		if err != nil {
			t.Fatal(err)
		}
		config.Certificates = []tls.Certificate{pair}
	}
	return config
}

func websocketMQTTConnect(t *testing.T, f *tlsFixture, conn net.Conn, version mqtt.ProtocolVersion, clientID string, credentials *mqtt.Connect) {
	t.Helper()
	packet := &mqtt.Connect{Version: version, CleanStart: true, ClientID: clientID}
	if credentials != nil {
		packet.HasUsername, packet.Username, packet.HasPassword, packet.Password = credentials.HasUsername, credentials.Username, credentials.HasPassword, credentials.Password
		packet.KeepAlive, packet.Will = credentials.KeepAlive, credentials.Will
	}
	writePacket(t, conn, f.encoder, packet)
	ack, ok := readPacket(t, conn, f.decoder, version).(*mqtt.ConnAck)
	if !ok || ack.ReasonCode != 0 {
		t.Fatalf("CONNECT %q CONNACK=%#v", clientID, ack)
	}
}

func websocketMQTTSubscribe(t *testing.T, f *tlsFixture, conn net.Conn, version mqtt.ProtocolVersion, topic string) {
	t.Helper()
	writePacket(t, conn, f.encoder, &mqtt.Subscribe{Version: version, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: topic, QoS: mqtt.QoSAtMostOnce}}})
	ack, ok := readPacket(t, conn, f.decoder, version).(*mqtt.SubAck)
	if !ok || len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] != 0 {
		t.Fatalf("SUBACK=%#v", ack)
	}
}

func mqttTCPConnect(t *testing.T, f *tlsFixture, conn net.Conn, clientID string) {
	t.Helper()
	writePacket(t, conn, f.encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: clientID})
	ack, ok := readPacket(t, conn, f.decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
	if !ok || ack.ReasonCode != 0 {
		t.Fatalf("TCP CONNACK=%#v", ack)
	}
}

func assertWebSocketPublish(t *testing.T, f *tlsFixture, conn net.Conn, version mqtt.ProtocolVersion, topic, payload string) {
	t.Helper()
	packet, ok := readPacket(t, conn, f.decoder, version).(*mqtt.Publish)
	if !ok || packet.Topic != topic || string(packet.Payload) != payload {
		t.Fatalf("PUBLISH=%#v, want %s=%q", packet, topic, payload)
	}
}

func ackSubscriberPublish(t *testing.T, f *tlsFixture, conn net.Conn, version mqtt.ProtocolVersion, packet *mqtt.Publish) {
	t.Helper()
	switch packet.QoS {
	case mqtt.QoSAtLeastOnce:
		writePacket(t, conn, f.encoder, &mqtt.PubAck{Version: version, PacketID: packet.PacketID})
	case mqtt.QoSExactlyOnce:
		writePacket(t, conn, f.encoder, &mqtt.PubRec{Version: version, PacketID: packet.PacketID})
		if _, ok := readPacket(t, conn, f.decoder, version).(*mqtt.PubRel); !ok {
			t.Fatal("expected PUBREL")
		}
		writePacket(t, conn, f.encoder, &mqtt.PubComp{Version: version, PacketID: packet.PacketID})
	}
}

type synchronizedBuffer struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.Write(p)
}
func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.String()
}
