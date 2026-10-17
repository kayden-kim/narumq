package tcp

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/delivery"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

// Traceability: PRD MQTT-11/MQTT-12/TEST-04/05.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-029
func TestTCPPartialPacketAfterConnectTimesOut(t *testing.T) {
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			cfg := defaultAdapterConfig()
			cfg.PacketReadTimeout = 200 * time.Millisecond
			fixture := newAdapterTestRuntime(t, 4, cfg)
			defer fixture.stop(t)
			writeTestPackets(t, fixture.client, &mqtt.Connect{Version: version, CleanStart: true, ClientID: "partial"})
			_ = readTestFrame(t, fixture.client)
			time.Sleep(600 * time.Millisecond)
			writeTestPackets(t, fixture.client, &mqtt.PingReq{Version: version})
			response, _, err := fixture.server.limits.Load().decoder.Decode(readTestFrame(t, fixture.client), version)
			if _, ok := response.(*mqtt.PingResp); err != nil || !ok {
				t.Fatalf("idle PING = (%T, %v)", response, err)
			}
			if _, err := fixture.client.Write([]byte{0x30, 10}); err != nil {
				t.Fatal(err)
			}
			requireConnectionClosedWithin(t, fixture.client, time.Second)
		})
	}
}

// Traceability: PRD MQTT-11/TEST-04/05.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-026
func TestTCPConnAckAdvertisesServerLimits(t *testing.T) {
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			cfg := defaultAdapterConfig()
			fixture := newAdapterTestRuntime(t, 4, cfg)
			defer fixture.stop(t)
			writeTestPackets(t, fixture.client, &mqtt.Connect{Version: version, CleanStart: true, ClientID: "limits"})
			wire := readTestFrame(t, fixture.client)
			packet, _, err := fixture.server.limits.Load().decoder.Decode(wire, version)
			if err != nil {
				t.Fatal(err)
			}
			ack := packet.(*mqtt.ConnAck)
			if version == mqtt.ProtocolV5 {
				if ack.ReceiveMaximum != cfg.ReceiveMaximum || ack.MaximumPacketSize != uint32(mqtt.PacketSize(cfg.MaxPacketSize)) {
					t.Fatalf("CONNACK = %#v", ack)
				}
			} else if len(wire) != 4 {
				t.Fatalf("v311 CONNACK = %x", wire)
			}
		})
	}
}

// Traceability: PRD MQTT-11/MQTT-12/TEST-04/05; MQTT-4.13.1-1.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-027
func TestTCPProtocolErrorsAfterConnectDisconnectWithReason(t *testing.T) {
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		for _, scenario := range []struct {
			name   string
			reason byte
		}{
			{"oversize", 0x95}, {"utf8", 0x81}, {"connect", 0x82}, {"quota", 0x93},
		} {
			t.Run(fmt.Sprintf("%d/%s", version, scenario.name), func(t *testing.T) {
				cfg := defaultAdapterConfig()
				cfg.MaxPacketSize = 64
				cfg.ReceiveMaximum = 2
				fixture := newAdapterTestRuntime(t, 4, cfg)
				defer fixture.stop(t)
				writeTestPackets(t, fixture.client, &mqtt.Connect{Version: version, CleanStart: true, ClientID: "errors"})
				_ = readTestFrame(t, fixture.client)
				switch scenario.name {
				case "oversize":
					if _, err := fixture.client.Write([]byte{0x30, 65}); err != nil {
						t.Fatal(err)
					}
				case "utf8":
					body := []byte{0, 1, 0xff}
					if version == mqtt.ProtocolV5 {
						body = append(body, 0)
					}
					if _, err := fixture.client.Write(append([]byte{0x30, byte(len(body))}, body...)); err != nil {
						t.Fatal(err)
					}
				case "connect":
					writeTestPackets(t, fixture.client, &mqtt.Connect{Version: version, CleanStart: true, ClientID: "again"})
				case "quota":
					for id := mqtt.PacketIdentifier(1); id <= 2; id++ {
						writeTestPackets(t, fixture.client, &mqtt.Publish{Version: version, QoS: mqtt.QoSExactlyOnce, PacketID: id, Topic: "quota"})
						packet, _, err := fixture.server.limits.Load().decoder.Decode(readTestFrame(t, fixture.client), version)
						if rec, ok := packet.(*mqtt.PubRec); err != nil || !ok || rec.PacketID != id {
							t.Fatalf("PUBREC = (%#v, %v)", packet, err)
						}
					}
					writeTestPackets(t, fixture.client, &mqtt.Publish{Version: version, QoS: mqtt.QoSExactlyOnce, PacketID: 3, Topic: "quota"})
				}
				if version == mqtt.ProtocolV5 {
					packet, _, err := fixture.server.limits.Load().decoder.Decode(readTestFrame(t, fixture.client), version)
					if disconnect, ok := packet.(*mqtt.Disconnect); err != nil || !ok || disconnect.ReasonCode != scenario.reason {
						t.Fatalf("DISCONNECT = (%#v, %v), want %x", packet, err, scenario.reason)
					}
				}
				requireConnectionClosedWithin(t, fixture.client, time.Second)
			})
		}
	}
}

// Traceability: PRD MQTT-11/TEST-04/05.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-028
func TestTCPReceiveMaximumQuotaReturnsAfterAcknowledgement(t *testing.T) {
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			cfg := defaultAdapterConfig()
			cfg.ReceiveMaximum = 2
			fixture := newAdapterTestRuntime(t, 4, cfg)
			defer fixture.stop(t)
			writeTestPackets(t, fixture.client, &mqtt.Connect{Version: version, CleanStart: true, ClientID: "quota"})
			_ = readTestFrame(t, fixture.client)
			for id := mqtt.PacketIdentifier(1); id <= 5; id++ {
				writeTestPackets(t, fixture.client, &mqtt.Publish{Version: version, QoS: mqtt.QoSAtLeastOnce, PacketID: id, Topic: "quota"})
				packet, _, err := fixture.server.limits.Load().decoder.Decode(readTestFrame(t, fixture.client), version)
				if ack, ok := packet.(*mqtt.PubAck); err != nil || !ok || ack.PacketID != id {
					t.Fatalf("PUBACK = (%#v, %v)", packet, err)
				}
			}
			for index, id := range []mqtt.PacketIdentifier{1, 2, 1} {
				writeTestPackets(t, fixture.client, &mqtt.Publish{Version: version, QoS: mqtt.QoSExactlyOnce, PacketID: id, DUP: index == 2, Topic: "quota"})
				packet, _, err := fixture.server.limits.Load().decoder.Decode(readTestFrame(t, fixture.client), version)
				if rec, ok := packet.(*mqtt.PubRec); err != nil || !ok || rec.PacketID != id {
					t.Fatalf("PUBREC = (%#v, %v)", packet, err)
				}
			}
			writeTestPackets(t, fixture.client, &mqtt.PubRel{Version: version, PacketID: 1})
			packet, _, err := fixture.server.limits.Load().decoder.Decode(readTestFrame(t, fixture.client), version)
			if comp, ok := packet.(*mqtt.PubComp); err != nil || !ok || comp.PacketID != 1 {
				t.Fatalf("PUBCOMP = (%#v, %v)", packet, err)
			}
			writeTestPackets(t, fixture.client, &mqtt.Publish{Version: version, QoS: mqtt.QoSExactlyOnce, PacketID: 3, Topic: "quota"})
			packet, _, err = fixture.server.limits.Load().decoder.Decode(readTestFrame(t, fixture.client), version)
			if rec, ok := packet.(*mqtt.PubRec); err != nil || !ok || rec.PacketID != 3 {
				t.Fatalf("replacement PUBREC = (%#v, %v)", packet, err)
			}
		})
	}
}

func newLimitsDeliveryConnection(t *testing.T) (*connection, net.Conn) {
	t.Helper()
	fixture := newAdapterTestRuntime(t, 8, defaultAdapterConfig())
	t.Cleanup(func() { fixture.stop(t) })
	socket, client := net.Pipe()
	t.Cleanup(func() { socket.Close(); client.Close() })
	conn := newConnection(fixture.server, socket)
	conn.sessionID = "limits-sub"
	conn.version = mqtt.ProtocolV5
	if err := conn.server.deliveries.Register(conn.sessionID); err != nil {
		t.Fatal(err)
	}
	return conn, client
}

func writeLimitsDeliveryBatch(t *testing.T, conn *connection, client net.Conn, count int) []mqtt.Packet {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- conn.writeDeliveries() }()
	var packets []mqtt.Packet
	for range count {
		packet, _, err := conn.server.limits.Load().decoder.Decode(readTestFrame(t, client), conn.version)
		if err != nil {
			t.Fatal(err)
		}
		packets = append(packets, packet)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery writer sent more packets than permitted or stalled")
	}
	return packets
}

// Traceability: PRD MQTT-11/TEST-04/05; MQTT-3.3.4-9.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-036
func TestWriteDeliveriesHonorsClientReceiveMaximum(t *testing.T) {
	conn, client := newLimitsDeliveryConnection(t)
	conn.clientReceiveMaximum = 2
	for index := range 3 {
		if err := conn.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: conn.sessionID, Topic: fmt.Sprint("quota/", index), QoS: mqtt.QoSAtLeastOnce}, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	packets := writeLimitsDeliveryBatch(t, conn, client, 2)
	queued, err := conn.server.deliveries.ListQueued(conn.sessionID, time.Time{})
	if err != nil || len(queued) != 1 || queued[0].Topic != "quota/2" {
		t.Fatalf("queued = (%#v, %v)", queued, err)
	}
	if err := conn.dispatchPubAck(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: packets[0].(*mqtt.Publish).PacketID}); err != nil {
		t.Fatal(err)
	}
	last := writeLimitsDeliveryBatch(t, conn, client, 1)[0].(*mqtt.Publish)
	if last.Topic != "quota/2" || len(conn.outbound) != 2 {
		t.Fatalf("third publish = %#v, inflight %d", last, len(conn.outbound))
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; MQTT-3.3.4-9/10.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-036
func TestWriteRecoveredDeliveriesHonorsClientReceiveMaximum(t *testing.T) {
	conn, client := newLimitsDeliveryConnection(t)
	conn.clientReceiveMaximum = 1
	for id := mqtt.PacketIdentifier(1); id <= 3; id++ {
		pub := mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSExactlyOnce, PacketID: id, Topic: fmt.Sprint("recover/", id)}
		if err := conn.server.deliveries.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{SessionID: conn.sessionID, Topic: pub.Topic, QoS: pub.QoS}, time.Time{}); err != nil {
			t.Fatal(err)
		}
		state, _, err := delivery.NewOutbound(pub)
		if err != nil {
			t.Fatal(err)
		}
		if id == 3 {
			state, _, err = state.Receive(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: id})
			if err != nil {
				t.Fatal(err)
			}
		}
		item := applicationdeliveryqueue.OutboundDTO{SessionID: conn.sessionID, PacketID: id, State: state}
		if _, err := conn.server.deliveries.PromoteBatch([]applicationdeliveryqueue.OutboundDTO{item}, time.Time{}); err != nil {
			t.Fatal(err)
		}
		if !conn.packetIDs.reserve(id) {
			t.Fatal("packet ID already reserved")
		}
		conn.outbound[id] = item
		conn.recovered = append(conn.recovered, item)
	}
	first := writeLimitsDeliveryBatch(t, conn, client, 1)[0]
	if rel, ok := first.(*mqtt.PubRel); !ok || rel.PacketID != 3 || len(conn.recovered) != 2 {
		t.Fatalf("recovery = %#v, deferred %#v", first, conn.recovered)
	}
	if err := conn.dispatchPubComp(&mqtt.PubComp{Version: mqtt.ProtocolV5, PacketID: 3}); err != nil {
		t.Fatal(err)
	}
	next := writeLimitsDeliveryBatch(t, conn, client, 1)[0].(*mqtt.Publish)
	if next.PacketID != 1 || !next.DUP || len(conn.recovered) != 1 {
		t.Fatalf("resumed PUBLISH = %#v, deferred %#v", next, conn.recovered)
	}
}

type limitsRecordingQueue struct {
	applicationdeliveryqueue.Queue
	events []string
}

func (queue *limitsRecordingQueue) PromoteBatch(items []applicationdeliveryqueue.OutboundDTO, now time.Time) (int, error) {
	count, err := queue.Queue.PromoteBatch(items, now)
	for _, item := range items[:count] {
		queue.events = append(queue.events, fmt.Sprint("promote:", item.PacketID))
	}
	return count, err
}

func (queue *limitsRecordingQueue) CompleteOutbound(sessionID routing.SessionID, id mqtt.PacketIdentifier) error {
	err := queue.Queue.CompleteOutbound(sessionID, id)
	if err == nil {
		queue.events = append(queue.events, fmt.Sprint("complete:", id))
	}
	return err
}

// Traceability: PRD MQTT-11/TEST-04/05; MQTT-3.1.2-24/25.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-037
func TestWriteDeliveriesDiscardsPublishAboveClientMaximumPacketSize(t *testing.T) {
	conn, client := newLimitsDeliveryConnection(t)
	conn.clientMaximumPacketSize = 64
	queue := &limitsRecordingQueue{Queue: conn.server.deliveries}
	conn.server.deliveries = queue
	for _, item := range []applicationdeliveryqueue.DeliveryDTO{
		{Topic: "large/0", QoS: mqtt.QoSAtMostOnce, Payload: make([]byte, 128)},
		{Topic: "large/1", QoS: mqtt.QoSAtLeastOnce, Payload: make([]byte, 128)},
		{Topic: "small/1", QoS: mqtt.QoSAtLeastOnce, Payload: []byte("ok")},
	} {
		item.SessionID = conn.sessionID
		if err := queue.TryEnqueue(item, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	publish := writeLimitsDeliveryBatch(t, conn, client, 1)[0].(*mqtt.Publish)
	outbound, err := queue.ListOutbound(conn.sessionID)
	if err != nil || publish.Topic != "small/1" || len(outbound) != 1 || outbound[0].PacketID != publish.PacketID {
		t.Fatalf("delivered %#v, outbound (%#v, %v)", publish, outbound, err)
	}
	if !reflect.DeepEqual(queue.events, []string{"promote:1", "complete:1", "promote:2"}) {
		t.Fatalf("durable discard order = %v", queue.events)
	}
	if !conn.packetIDs.reserve(1) {
		t.Fatal("discarded packet ID was not released")
	}
}

// Traceability: PRD MQTT-11/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §7.1.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-038
func TestTCPConnAckAboveClientMaximumPacketSizeClosesWithoutSending(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprint("cluster-unavailable=", unavailable), func(t *testing.T) {
			cfg := defaultAdapterConfig()
			if unavailable {
				cfg.ClusterMode = true
				cfg.LocalMemberID = 1
				cfg.ClusterReady = func() bool { return false }
				cfg.ClusterAdmissionOpen = func() bool { return false }
				cfg.ClusterProjectionReady = func() bool { return false }
			}
			fixture := newAdapterTestRuntime(t, 4, cfg)
			defer fixture.stop(t)
			writeTestPackets(t, fixture.client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "tiny-limit", MaximumPacketSize: 1})
			requireConnectionClosedWithin(t, fixture.client, time.Second)
		})
	}
}
