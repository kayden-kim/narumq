package tcp

import (
	"testing"

	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/application/membership"
	"narumq/internal/application/publish"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

type rejectingTCPPublishAdmission struct{}

func (rejectingTCPPublishAdmission) CheckPressure() error {
	return publish.NewRejectionError(publish.ErrPublishBackpressure, publish.RejectionClassServerBackpressure, publish.RejectionCauseCommitLag)
}
func (rejectingTCPPublishAdmission) CheckDeliveryCapacity(membership.DeliveryChangeDTO) error {
	return nil
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.5, §11.28.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-197, mqtt-protocol-and-session.md:MQTT-SCN-056
func TestTCPPublishBackpressureReasonCodes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version mqtt.ProtocolVersion
		qos     mqtt.QoS
		retain  bool
		header  byte
	}{
		{"v5-qos1-phased", mqtt.ProtocolV5, 1, false, 0x40},
		{"v5-qos1-inline", mqtt.ProtocolV5, 1, true, 0x40},
		{"v5-qos2", mqtt.ProtocolV5, 2, false, 0x50},
		{"v5-retained-qos0", mqtt.ProtocolV5, 0, true, 0xe0},
		{"v311-qos1", mqtt.ProtocolV311, 1, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := defaultAdapterConfig()
			config.ClusterMode = true
			config.LocalMemberID = 4
			config.ClusterReady = func() bool { return true }
			config.ClusterAdmissionOpen = func() bool { return true }
			config.ClusterProjectionReady = func() bool { return true }
			runtime := newAdapterTestRuntime(t, 8, config, func(server *Server) {
				tenant, _ := tenancy.NewTenantID("default")
				key, _ := tenancy.NewSessionKey(tenant, "publisher")
				routes := routing.NewTable()
				if err := routes.BindSessionTenant(routing.SessionID(key.RoutingID()), tenant); err != nil {
					t.Fatal(err)
				}
				retained, err := memoryretained.New(32, 4096)
				if err != nil {
					t.Fatal(err)
				}
				server.publications, err = publish.New(server.config.Clock, routes, server.deliveries, retained,
					publish.WithDurableDeliveryCommitter(&durableTCPCommitter{}), publish.WithAdmission(rejectingTCPPublishAdmission{}))
				if err != nil {
					t.Fatal(err)
				}
			})
			defer runtime.stop(t)
			writeTestPackets(t, runtime.client, &mqtt.Connect{Version: tc.version, CleanStart: true, ClientID: "publisher"})
			frame := readTestFrame(t, runtime.client)
			if frame[0] != 0x20 || frame[3] != 0 {
				t.Fatalf("CONNACK=%x", frame)
			}
			id := mqtt.PacketIdentifier(7)
			if tc.qos == 0 {
				id = 0
			}
			writeTestPackets(t, runtime.client, &mqtt.Publish{Version: tc.version, Topic: "durable/item", Payload: []byte("rejected"), QoS: tc.qos, Retain: tc.retain, PacketID: id})
			if tc.header == 0 {
				requireConnectionClosed(t, runtime.client)
				return
			}
			frame = readTestFrame(t, runtime.client)
			reasonIndex := 4
			if tc.qos == 0 {
				reasonIndex = 2
			}
			if frame[0] != tc.header || len(frame) <= reasonIndex || frame[reasonIndex] != 0x97 {
				t.Fatalf("rejection wire=%x", frame)
			}
			if tc.qos == 0 {
				requireConnectionClosed(t, runtime.client)
				return
			}
			packet, _, err := runtime.server.limits.Load().decoder.Decode(frame, tc.version)
			if err != nil {
				t.Fatal(err)
			}
			if got := tcpAckReasonString(packet); got != "server_backpressure:commit_lag" {
				t.Fatalf("Reason String = %q", got)
			}
			writeTestPackets(t, runtime.client, &mqtt.PingReq{Version: tc.version})
			frame = readTestFrame(t, runtime.client)
			if len(frame) != 2 || frame[0] != 0xd0 || frame[1] != 0 {
				t.Fatalf("PINGRESP=%x", frame)
			}
		})
	}
}
