package tcp

import (
	"errors"
	"testing"

	"narumq/internal/domain/mqtt"
)

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
