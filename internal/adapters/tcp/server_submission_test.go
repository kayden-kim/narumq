package tcp

import (
	"bytes"
	"errors"
	"net"
	"testing"

	"narumq/internal/application/publish"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/security"
)

// Traceability: docs/specs/mqtt-protocol-and-session.md §3.1.
func TestCompleteInboundPublishMapsBackpressureByProtocol(t *testing.T) {
	wantBackpressure := publish.ErrPublishBackpressure
	for _, testCase := range []struct {
		name      string
		version   mqtt.ProtocolVersion
		qos       mqtt.QoS
		wantClose bool
		wantAck   mqtt.Packet
		wantErr   error
	}{
		{name: "mqtt5 qos1", version: mqtt.ProtocolV5, qos: mqtt.QoSAtLeastOnce,
			wantAck: &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: 1, ReasonCode: 0x97}},
		{name: "mqtt5 qos2", version: mqtt.ProtocolV5, qos: mqtt.QoSExactlyOnce,
			wantAck: &mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: 1, ReasonCode: 0x97}},
		{name: "mqtt311 closes", version: mqtt.ProtocolV311, qos: mqtt.QoSAtLeastOnce, wantClose: true},
		{name: "unrelated error", version: mqtt.ProtocolV5, qos: mqtt.QoSAtLeastOnce,
			wantErr: errors.New("publication unavailable")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
			serverSide, client := net.Pipe()
			defer client.Close()
			connection := newConnection(deps.server, serverSide)
			defer connection.requestClose()
			connection.version = testCase.version
			packet := &mqtt.Publish{Version: testCase.version, QoS: testCase.qos, PacketID: 1}
			err := wantBackpressure
			if testCase.wantErr != nil {
				err = testCase.wantErr
			}
			submitted := submittedPhaseWork{work: phaseWork{packet: packet}, err: err}
			gotErr := connection.completeInboundPublish(submitted)
			if testCase.wantErr == nil {
				if gotErr != nil {
					t.Fatalf("completeInboundPublish() error = %v, want nil", gotErr)
				}
			} else if !errors.Is(gotErr, testCase.wantErr) {
				t.Fatalf("completeInboundPublish() error = %v, want %v", gotErr, testCase.wantErr)
			}
			if gotClose := connection.isStopping(); gotClose != testCase.wantClose {
				t.Fatalf("connection stopping = %t, want %t", gotClose, testCase.wantClose)
			}
			if testCase.wantAck == nil {
				if len(connection.controlQueue) != 0 {
					t.Fatalf("unexpected control packet: %T", (<-connection.controlQueue).packet)
				}
				return
			}
			select {
			case message := <-connection.controlQueue:
				gotWire := encodeTestPacket(t, message.packet)
				wantWire := encodeTestPacket(t, testCase.wantAck)
				if !bytes.Equal(gotWire, wantWire) {
					t.Fatalf("ack = %#v, want %#v", message.packet, testCase.wantAck)
				}
			default:
				t.Fatal("expected negative acknowledgement")
			}
		})
	}
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §3.1.
func TestCompleteInboundPublishMapsAuthorizationDenialToMQTT5Ack(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	serverSide, client := net.Pipe()
	defer client.Close()
	connection := newConnection(deps.server, serverSide)
	defer connection.requestClose()
	connection.version = mqtt.ProtocolV5
	packet := &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 7}
	submitted := submittedPhaseWork{work: phaseWork{packet: packet}, err: security.ErrNotAuthorized}
	if err := connection.completeInboundPublish(submitted); err != nil {
		t.Fatalf("completeInboundPublish() error = %v", err)
	}
	select {
	case message := <-connection.controlQueue:
		want := &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: 7, ReasonCode: 0x87}
		if got, want := encodeTestPacket(t, message.packet), encodeTestPacket(t, want); !bytes.Equal(got, want) {
			t.Fatalf("authorization response = %#v, want %#v", message.packet, want)
		}
	default:
		t.Fatal("expected MQTT 5 authorization acknowledgement")
	}
}

// Traceability: CLU-03/CLU-14; docs/specs/mqtt-protocol-and-session.md §3.1.
func TestDurablePublishProjectionDenialIsNotSocketFailure(t *testing.T) {
	for _, qos := range []mqtt.QoS{mqtt.QoSAtLeastOnce, mqtt.QoSExactlyOnce} {
		committer := new(durableTCPCommitter)
		runtime, conn := durableTCPRuntime(t, mqtt.ProtocolV311, committer)
		runtime.server.config.ClusterProjectionReady = func() bool { return false }
		packet := &mqtt.Publish{Version: mqtt.ProtocolV311, Topic: "durable/item", QoS: qos, PacketID: 1, Payload: []byte("denied")}
		submitted := conn.beginInboundPublish(phaseWork{packet: packet, receivedAt: runtime.server.config.Clock.Now()})
		err := conn.completeInboundPublish(submitted)
		if !errors.Is(err, ErrClusterNotReady) || errors.Is(err, ErrConnectionClosed) {
			t.Fatalf("projection denial qos=%d = %v", qos, err)
		}
		if conn.isStopping() || len(conn.inbound) != 0 || len(conn.controlQueue) != 0 {
			t.Fatal("denied preparation changed handshake or socket state")
		}
		if len(committer.changes) != 0 || committer.barriers != 0 {
			t.Fatal("projection denial submitted a durable mutation")
		}
	}
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §3.1.
func TestBeginOutboundAckRejectsZeroAndIgnoresUnknownPacketID(t *testing.T) {
	deps := csvNewServer(t, newPipeListener(), csvServerOptions{})
	serverSide, client := net.Pipe()
	defer client.Close()
	connection := newConnection(deps.server, serverSide)
	defer connection.requestClose()
	work := phaseWork{packet: &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: 1}}

	zero := connection.beginOutboundAck(work, 0)
	if !errors.Is(zero.err, ErrProtocol) {
		t.Fatalf("zero packet identifier error = %v, want %v", zero.err, ErrProtocol)
	}
	if zero.transition != nil || zero.outbound.PacketID != 0 {
		t.Fatalf("zero packet identifier produced state change: %+v", zero)
	}

	unknown := connection.beginOutboundAck(work, 1)
	if unknown.err != nil || unknown.transition != nil {
		t.Fatalf("unknown packet identifier submission = %+v, want no-op", unknown)
	}
	if err := connection.completeOutboundAck(unknown); err != nil {
		t.Fatalf("complete unknown acknowledgment: %v", err)
	}
	if len(connection.outbound) != 0 || len(connection.packetIDs.used) != 0 || len(connection.controlQueue) != 0 {
		t.Fatalf("unknown acknowledgment changed outbound state: pending=%d packetIDs=%d controls=%d",
			len(connection.outbound), len(connection.packetIDs.used), len(connection.controlQueue))
	}
}
