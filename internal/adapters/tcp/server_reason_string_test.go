package tcp

import (
	"errors"
	"testing"

	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/application/publish"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

func tcpAckReasonString(packet mqtt.Packet) string {
	switch ack := packet.(type) {
	case *mqtt.PubAck:
		return ack.ReasonString
	case *mqtt.PubRec:
		return ack.ReasonString
	default:
		return "not-an-ack"
	}
}

// Traceability: PRD MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.2.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-056
func TestTCPRetainedCapacityRejectionCarriesReasonString(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 8, defaultAdapterConfig(), func(server *Server) {
		tenant, _ := tenancy.NewTenantID("default")
		key, _ := tenancy.NewSessionKey(tenant, "publisher")
		routes := routing.NewTable()
		if err := routes.BindSessionTenant(routing.SessionID(key.RoutingID()), tenant); err != nil {
			t.Fatal(err)
		}
		retained, err := memoryretained.New(1, 4096)
		if err != nil {
			t.Fatal(err)
		}
		server.publications, err = publish.New(server.config.Clock, routes, server.deliveries, retained)
		if err != nil {
			t.Fatal(err)
		}
	})
	defer runtime.stop(t)
	writeTestPackets(t, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "publisher"})
	if frame := readTestFrame(t, runtime.client); frame[0] != 0x20 || frame[3] != 0 {
		t.Fatalf("CONNACK=%x", frame)
	}
	for id, topic := range []string{"retained/first", "retained/second"} {
		writeTestPackets(t, runtime.client, &mqtt.Publish{Version: mqtt.ProtocolV5, QoS: 1, Retain: true, PacketID: mqtt.PacketIdentifier(id + 1), Topic: topic, Payload: []byte("value")})
		packet, _, err := runtime.server.limits.Load().decoder.Decode(readTestFrame(t, runtime.client), mqtt.ProtocolV5)
		if err != nil {
			t.Fatal(err)
		}
		ack, ok := packet.(*mqtt.PubAck)
		if !ok {
			t.Fatalf("packet = %T", packet)
		}
		wantCode, wantReason := byte(0), ""
		if id == 1 {
			wantCode, wantReason = 0x97, "server_backpressure:retained_capacity"
		}
		if ack.ReasonCode != wantCode || ack.ReasonString != wantReason {
			t.Fatalf("%s ACK = code 0x%02X reason %q, want 0x%02X %q", topic, ack.ReasonCode, ack.ReasonString, wantCode, wantReason)
		}
	}
}

// Traceability: PRD MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.2.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-057
func TestWritePacketDropsReasonStringAboveClientMaximumPacketSize(t *testing.T) {
	const reason = "tenant_limit:message_rate"
	for _, ack := range []mqtt.Packet{
		&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: 1, ReasonCode: 0x97, ReasonString: reason},
		&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: 1, ReasonCode: 0x97, ReasonString: reason},
	} {
		bare := []byte{0x40, 0x04, 0x00, 0x01, 0x97, 0x00}
		if _, ok := ack.(*mqtt.PubRec); ok {
			bare[0] = 0x50
		}
		for _, testCase := range []struct {
			name         string
			maximum      uint32
			wantErr      error
			wantReason   string
			wantBareOnly bool
		}{
			{name: "no-client-limit", maximum: 0, wantReason: reason},
			{name: "fits-with-reason-string", maximum: 1024, wantReason: reason},
			{name: "reason-string-dropped", maximum: uint32(len(bare)), wantBareOnly: true},
			{name: "bare-packet-too-large", maximum: uint32(len(bare)) - 1, wantErr: ErrClientPacketTooLarge},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				conn, client := newLimitsDeliveryConnection(t)
				conn.clientMaximumPacketSize = testCase.maximum
				done := make(chan error, 1)
				go func() { done <- conn.writePacket(ack) }()
				if testCase.wantErr != nil {
					if err := <-done; !errors.Is(err, testCase.wantErr) {
						t.Fatalf("writePacket() error = %v, want %v", err, testCase.wantErr)
					}
					return
				}
				frame := readTestFrame(t, client)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if testCase.wantBareOnly {
					if string(frame) != string(bare) {
						t.Fatalf("frame = % X, want % X", frame, bare)
					}
					return
				}
				packet, _, err := conn.server.limits.Load().decoder.Decode(frame, mqtt.ProtocolV5)
				if err != nil {
					t.Fatal(err)
				}
				if got := tcpAckReasonString(packet); got != testCase.wantReason {
					t.Fatalf("Reason String = %q, want %q", got, testCase.wantReason)
				}
			})
		}
		// Stripping diagnostics must not mutate the packet queued before the write.
		if got := tcpAckReasonString(ack); got != reason {
			t.Fatalf("queued Reason String was mutated: %q", got)
		}
	}
}
