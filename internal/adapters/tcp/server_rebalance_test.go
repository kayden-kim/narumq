package tcp

import (
	"net"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

// Traceability: CLU-13; docs/specs/cluster-session-and-delivery.md §9, §11.31.
func TestConnectedSessionIDsExcludePreConnectSockets(t *testing.T) {
	config := defaultAdapterConfig()
	config.ClusterMode = true
	config.LocalMemberID = 4
	config.ClusterReady = func() bool { return true }
	config.ClusterAdmissionOpen = func() bool { return true }
	config.ClusterProjectionReady = func() bool { return true }
	runtime := newAdapterTestRuntime(t, 8, config)
	defer runtime.stop(t)
	writeTestPackets(t, runtime.client, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "rb-connected"})
	_ = readTestFrame(t, runtime.client)
	pendingClient, pendingServer := net.Pipe()
	defer pendingClient.Close()
	defer pendingServer.Close()
	runtime.listener.connections <- pendingServer
	select {
	case <-runtime.listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("pending socket was not accepted")
	}
	if !waitForActiveConnections(runtime.server, 2, time.Second) {
		t.Fatalf("active sockets=%d, want2", runtime.server.ActiveConnectionCount())
	}
	ids := runtime.server.ConnectedSessionIDs()
	if len(ids) != 1 {
		t.Fatalf("connected sessions=%v, want one", ids)
	}
	runtime.server.mu.Lock()
	bound := runtime.server.bySession[ids[0]]
	runtime.server.mu.Unlock()
	if bound == nil || bound.ownership.ClientID != "rb-connected" {
		t.Fatalf("session %q is not bound to rb-connected", ids[0])
	}
	original := ids[0]
	ids[0] = "changed"
	if got := runtime.server.ConnectedSessionIDs(); len(got) != 1 || got[0] != original {
		t.Fatalf("caller mutated registry: %v", got)
	}
	var nilServer *Server
	if nilServer.ConnectedSessionIDs() != nil {
		t.Fatal("nil server returned sessions")
	}
}
