package integration

import (
	"bytes"
	"fmt"
	"net"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/domain/mqtt"
)

func connectResourceClient(t *testing.T, fixture *ingressFixture, packet mqtt.Connect) (net.Conn, *mqtt.ConnAck) {
	t.Helper()
	socket := fixture.dial(t)
	t.Cleanup(func() { socket.Close() })
	writePacket(t, socket, fixture.encoder, &packet)
	ack := readPacket(t, socket, fixture.decoder, packet.Version).(*mqtt.ConnAck)
	if ack.ReasonCode != 0 {
		t.Fatalf("CONNECT rejected: %#v", ack)
	}
	return socket, ack
}

func subscribeResourceClient(t *testing.T, fixture *ingressFixture, socket net.Conn, filter string, qos mqtt.QoS) {
	t.Helper()
	writePacket(t, socket, fixture.encoder, &mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: filter, QoS: qos}}})
	ack := readPacket(t, socket, fixture.decoder, mqtt.ProtocolV5).(*mqtt.SubAck)
	if len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] != byte(qos) {
		t.Fatalf("SUBACK = %#v", ack)
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; MQTT-3.3.4-9.
func TestMQTTOutboundHonorsClientReceiveMaximum(t *testing.T) {
	t.Parallel()
	fixture := newIngressFixture(t)
	defer fixture.close(t)
	sub, _ := connectResourceClient(t, fixture, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "limited-sub", CleanStart: true, ReceiveMaximum: 2})
	subscribeResourceClient(t, fixture, sub, "quota/#", mqtt.QoSAtLeastOnce)
	pub, _ := connectResourceClient(t, fixture, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "publisher", CleanStart: true})
	for id := mqtt.PacketIdentifier(1); id <= 5; id++ {
		writePacket(t, pub, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: id, Topic: fmt.Sprint("quota/", id)})
		ack := readPacket(t, pub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
		if ack.PacketID != id {
			t.Fatalf("PUBACK = %#v", ack)
		}
	}
	var pending []*mqtt.Publish
	for id := 1; id <= 2; id++ {
		packet := readPacket(t, sub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
		if packet.Topic != fmt.Sprint("quota/", id) {
			t.Fatalf("PUBLISH = %#v", packet)
		}
		pending = append(pending, packet)
	}
	expectNoPacket(t, sub)
	for id := 3; id <= 5; id++ {
		writePacket(t, sub, fixture.encoder, &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: pending[0].PacketID})
		pending = pending[1:]
		packet := readPacket(t, sub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
		if packet.Topic != fmt.Sprint("quota/", id) {
			t.Fatalf("PUBLISH = %#v", packet)
		}
		pending = append(pending, packet)
		expectNoPacket(t, sub)
	}
	for _, packet := range pending {
		writePacket(t, sub, fixture.encoder, &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: packet.PacketID})
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; MQTT-3.1.2-24/25.
func TestMQTTOutboundDiscardsPacketsAboveClientMaximumPacketSize(t *testing.T) {
	t.Parallel()
	fixture := newIngressFixture(t)
	defer fixture.close(t)
	connect := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "size-sub", MaximumPacketSize: 128, SessionExpiryInterval: 60}
	sub, _ := connectResourceClient(t, fixture, connect)
	subscribeResourceClient(t, fixture, sub, "size/#", mqtt.QoSAtLeastOnce)
	pub, _ := connectResourceClient(t, fixture, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "publisher", CleanStart: true})
	for index, item := range []struct {
		topic   string
		qos     mqtt.QoS
		payload []byte
	}{
		{"size/large1", mqtt.QoSAtLeastOnce, make([]byte, 512)}, {"size/small1", mqtt.QoSAtLeastOnce, []byte("one")},
		{"size/large0", mqtt.QoSAtMostOnce, make([]byte, 512)}, {"size/small0", mqtt.QoSAtMostOnce, []byte("zero")},
	} {
		packet := mqtt.Publish{Version: mqtt.ProtocolV5, Topic: item.topic, QoS: item.qos, Payload: item.payload}
		if item.qos != mqtt.QoSAtMostOnce {
			packet.PacketID = mqtt.PacketIdentifier(index + 1)
		}
		writePacket(t, pub, fixture.encoder, &packet)
		if item.qos != mqtt.QoSAtMostOnce {
			_ = readPacket(t, pub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PubAck)
		}
	}
	first := readPacket(t, sub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
	second := readPacket(t, sub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.Publish)
	if first.Topic != "size/small1" || second.Topic != "size/small0" {
		t.Fatalf("deliveries = %#v, %#v", first, second)
	}
	writePacket(t, sub, fixture.encoder, &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: first.PacketID})
	// PINGRESP orders the final ACK before DISCONNECT and reconnect.
	writePacket(t, sub, fixture.encoder, &mqtt.PingReq{Version: mqtt.ProtocolV5})
	_ = readPacket(t, sub, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PingResp)
	writePacket(t, sub, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
	sub.Close()
	waitForSessionDisconnected(t, fixture.repo, connect.ClientID)
	resumed, ack := connectResourceClient(t, fixture, connect)
	if !ack.SessionPresent {
		t.Fatal("persistent session was not resumed")
	}
	expectNoPacket(t, resumed)
}

func waitResourceConnections(t *testing.T, fixture *ingressFixture, count int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for fixture.server.ActiveConnectionCount() != count {
		if time.Now().After(deadline) {
			t.Fatalf("active connections = %d, want %d", fixture.server.ActiveConnectionCount(), count)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func requireResourceClosed(t *testing.T, socket net.Conn, timeout time.Duration) {
	t.Helper()
	if err := socket.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatal(err)
	}
	var value [1]byte
	_, err := socket.Read(value[:])
	if err == nil {
		t.Fatal("connection remained open or sent unexpected bytes")
	}
	if network, ok := err.(net.Error); ok && network.Timeout() {
		t.Fatalf("connection did not close: %v", err)
	}
}

// Traceability: PRD MQTT-11/MQTT-12/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7·§8.
func TestMQTTResourceLimitsReleaseConnectionsGoroutinesAndMemory(t *testing.T) {
	// Do not use t.Parallel: goroutine and heap measurements cover this process.
	fixture := newIngressFixtureWithTCPConfig(t, 8, func(cfg *tcpadapter.Config) {
		cfg.MaxPacketSize = 1 << 20
		cfg.ConnectTimeout = 500 * time.Millisecond
		cfg.WriteTimeout = 300 * time.Millisecond
		cfg.PacketReadTimeout = 300 * time.Millisecond
		cfg.ReceiveMaximum = 4
		cfg.ControlQueueCapacity = 16
	})
	defer fixture.close(t)
	warm, _ := connectResourceClient(t, fixture, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "warm", CleanStart: true})
	writePacket(t, warm, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
	warm.Close()
	waitResourceConnections(t, fixture, 0, time.Second)
	runtime.GC()
	baselineGoroutines := runtime.NumGoroutine()
	var baseline, measured runtime.MemStats
	runtime.ReadMemStats(&baseline)
	var clients []net.Conn
	defer func() {
		for _, client := range clients {
			client.Close()
		}
	}()
	start := time.Now()
	for range 64 {
		socket := fixture.dial(t)
		clients = append(clients, socket)
		if _, err := socket.Write(append([]byte{0x10, 0xff, 0xff, 0x3f}, make([]byte, 100)...)); err != nil {
			t.Fatal(err)
		}
	}
	waitResourceConnections(t, fixture, 64, 200*time.Millisecond)
	runtime.GC()
	runtime.ReadMemStats(&measured)
	increase := int64(measured.HeapAlloc) - int64(baseline.HeapAlloc)
	t.Logf("64 partial CONNECT sockets heap increase: %d bytes", increase)
	if increase >= 16<<20 {
		t.Fatalf("pre-CONNECT heap increase %d >= 16 MiB", increase)
	}
	for _, socket := range clients {
		requireResourceClosed(t, socket, time.Until(start.Add(3*time.Second)))
		socket.Close()
	}
	waitResourceConnections(t, fixture, 0, time.Second)

	var partials []net.Conn
	for index := range 16 {
		socket, _ := connectResourceClient(t, fixture, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: fmt.Sprint("partial-", index), CleanStart: true})
		clients = append(clients, socket)
		partials = append(partials, socket)
		if _, err := socket.Write([]byte{0x32, 10}); err != nil {
			t.Fatal(err)
		}
	}
	func() {
		trickled := make(chan struct{})
		stopTrickle := make(chan struct{})
		defer func() { close(stopTrickle); <-trickled }()
		go func() {
			defer close(trickled)
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopTrickle:
					return
				case <-ticker.C:
					for _, socket := range partials {
						_ = socket.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
						_, _ = socket.Write([]byte{0})
					}
				}
			}
		}()
		for _, socket := range partials {
			requireResourceClosed(t, socket, 2*time.Second)
			socket.Close()
		}
	}()
	waitResourceConnections(t, fixture, 0, time.Second)

	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV5, mqtt.ProtocolV311} {
		for _, scenario := range []struct {
			name   string
			count  int
			reason byte
		}{{"oversize", 8, 0x95}, {"utf8", 8, 0x81}, {"quota", 4, 0x93}} {
			for index := range scenario.count {
				socket, _ := connectResourceClient(t, fixture, mqtt.Connect{Version: version, ClientID: fmt.Sprintf("%d-%s-%d", version, scenario.name, index), CleanStart: true})
				clients = append(clients, socket)
				switch scenario.name {
				case "oversize":
					if _, err := socket.Write([]byte{0x30, 0x81, 0x80, 0x40}); err != nil {
						t.Fatal(err)
					}
				case "utf8":
					body := []byte{0, 1, 0xff}
					if version == mqtt.ProtocolV5 {
						body = append(body, 0)
					}
					if _, err := socket.Write(append([]byte{0x30, byte(len(body))}, body...)); err != nil {
						t.Fatal(err)
					}
				case "quota":
					for id := mqtt.PacketIdentifier(1); id <= 5; id++ {
						writePacket(t, socket, fixture.encoder, &mqtt.Publish{Version: version, QoS: mqtt.QoSExactlyOnce, PacketID: id, Topic: "quota"})
						if id <= 4 {
							rec := readPacket(t, socket, fixture.decoder, version).(*mqtt.PubRec)
							if rec.PacketID != id {
								t.Fatalf("PUBREC = %#v", rec)
							}
						}
					}
				}
				if version == mqtt.ProtocolV5 {
					packet := readPacket(t, socket, fixture.decoder, version).(*mqtt.Disconnect)
					if packet.ReasonCode != scenario.reason {
						t.Fatalf("%s DISCONNECT = %#v", scenario.name, packet)
					}
				}
				requireResourceClosed(t, socket, 2*time.Second)
				socket.Close()
			}
		}
	}
	waitResourceConnections(t, fixture, 0, time.Second)

	slow, _ := connectResourceClient(t, fixture, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "slow-reader", CleanStart: true})
	clients = append(clients, slow)
	subscribeResourceClient(t, fixture, slow, "slow/#", mqtt.QoSAtMostOnce)
	if err := slow.(*net.TCPConn).SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	publisher, _ := connectResourceClient(t, fixture, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "load-publisher", CleanStart: true})
	clients = append(clients, publisher)
	payload := make([]byte, 64<<10)
	for range 64 {
		writePacket(t, publisher, fixture.encoder, &mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "slow/load", Payload: payload})
	}
	waitResourceConnections(t, fixture, 1, 5*time.Second)
	writePacket(t, publisher, fixture.encoder, &mqtt.PingReq{Version: mqtt.ProtocolV5})
	_ = readPacket(t, publisher, fixture.decoder, mqtt.ProtocolV5).(*mqtt.PingResp)
	writePacket(t, publisher, fixture.encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
	for _, socket := range clients {
		socket.Close()
	}
	waitResourceConnections(t, fixture, 0, 5*time.Second)
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baselineGoroutines {
		if time.Now().After(deadline) {
			var stacks bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&stacks, 1)
			t.Fatalf("goroutines = %d, baseline %d\n%s", runtime.NumGoroutine(), baselineGoroutines, stacks.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	runtime.GC()
	runtime.ReadMemStats(&measured)
	t.Logf("cleanup goroutines: baseline=%d final=%d; heap: baseline=%d final=%d", baselineGoroutines, runtime.NumGoroutine(), baseline.HeapAlloc, measured.HeapAlloc)
	if measured.HeapAlloc > baseline.HeapAlloc+16<<20 {
		t.Fatalf("cleanup heap %d exceeds baseline %d + 16 MiB", measured.HeapAlloc, baseline.HeapAlloc)
	}
}
