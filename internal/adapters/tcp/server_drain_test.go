package tcp

import (
	"errors"
	"net"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

// Traceability: DEP-06/DEP-07; docs/specs/drain-and-rolling-update.md §4, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-008
func TestCloseSessionForDrainSendsRedirectAndSuppressesWill(t *testing.T) {
	config := defaultAdapterConfig()
	config.ClusterMode = true
	config.LocalMemberID = 4
	config.ClusterReady = func() bool { return true }
	config.ClusterAdmissionOpen = func() bool { return true }
	config.ClusterProjectionReady = func() bool { return true }
	runtime := newAdapterTestRuntime(t, 8, config)
	defer runtime.stop(t)

	writeTestPackets(t, runtime.client, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "drain-observer",
	})
	_ = readTestFrame(t, runtime.client)
	writeTestPackets(t, runtime.client, &mqtt.Subscribe{
		Version: mqtt.ProtocolV5, PacketID: 1,
		Subscriptions: []mqtt.Subscription{{Filter: "will/#", QoS: mqtt.QoSAtMostOnce}},
	})
	_ = readTestFrame(t, runtime.client)

	producerClient, producerServer := net.Pipe()
	t.Cleanup(func() {
		_ = producerClient.Close()
		_ = producerServer.Close()
	})
	runtime.listener.connections <- producerServer
	select {
	case <-runtime.listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("server did not accept drain producer")
	}
	writeTestPackets(t, producerClient, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, SessionExpiryInterval: 60,
		ClientID: "drain-producer", Will: &mqtt.Will{Topic: "will/drained", Payload: []byte("must-not-publish")},
	})
	_ = readTestFrame(t, producerClient)

	var producerSessionID routing.SessionID
	runtime.server.mu.Lock()
	for sessionID, connection := range runtime.server.bySession {
		if connection.ownership.ClientID == "drain-producer" {
			producerSessionID = sessionID
			break
		}
	}
	runtime.server.mu.Unlock()
	if producerSessionID == "" {
		t.Fatal("producer session was not registered")
	}
	if !runtime.server.CloseSessionForDrain(producerSessionID, "next:1883") {
		t.Fatal("CloseSessionForDrain() = false, want true")
	}
	frame := readTestFrame(t, producerClient)
	want := []byte{0xE0, 0x0E, 0x9C, 0x0C, 0x1C, 0x00, 0x09, 'n', 'e', 'x', 't', ':', '1', '8', '8', '3'}
	if string(frame) != string(want) {
		t.Fatalf("drain DISCONNECT = % X, want % X", frame, want)
	}
	requireConnectionClosed(t, producerClient)

	if err := runtime.client.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var observed [1]byte
	count, err := runtime.client.Read(observed[:])
	var networkErr net.Error
	if count != 0 || !errors.As(err, &networkErr) || !networkErr.Timeout() {
		t.Fatalf("observer received Will data (% X), read error = %v", observed[:count], err)
	}
}

// Traceability: DEP-07; docs/specs/drain-and-rolling-update.md §4, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-008
func TestCloseSessionForDrainClosesMQTT311WithoutPacket(t *testing.T) {
	config := defaultAdapterConfig()
	config.ClusterMode = true
	config.LocalMemberID = 4
	config.ClusterReady = func() bool { return true }
	config.ClusterAdmissionOpen = func() bool { return true }
	config.ClusterProjectionReady = func() bool { return true }
	runtime := newAdapterTestRuntime(t, 8, config)
	defer runtime.stop(t)

	writeTestPackets(t, runtime.client, &mqtt.Connect{
		Version: mqtt.ProtocolV311, CleanStart: true, ClientID: "drain-v311",
	})
	_ = readTestFrame(t, runtime.client)
	var sessionID routing.SessionID
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runtime.server.mu.Lock()
		for id, connection := range runtime.server.bySession {
			if connection.ownership.ClientID == "drain-v311" {
				sessionID = id
				break
			}
		}
		runtime.server.mu.Unlock()
		if sessionID != "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if sessionID == "" {
		t.Fatal("MQTT 3.1.1 session was not registered")
	}
	if !runtime.server.CloseSessionForDrain(sessionID, "unused.example:1883") {
		t.Fatal("CloseSessionForDrain() = false, want true")
	}
	requireConnectionClosed(t, runtime.client)
}

// Traceability: DEP-07; docs/specs/drain-and-rolling-update.md §4, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-008
func TestCloseSessionForDrainDoesNotClaimUnknownSession(t *testing.T) {
	server := &Server{bySession: make(map[routing.SessionID]*connection)}
	if server.CloseSessionForDrain("missing-session", "next.example:1883") {
		t.Fatal("CloseSessionForDrain() reported an unknown session")
	}
	if len(server.bySession) != 0 {
		t.Fatalf("unknown drain changed session bindings: %v", server.bySession)
	}
}

// Traceability: DEP-03; docs/specs/drain-and-rolling-update.md §2.1, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-008
func TestTCPClusterAdmissionClosedRejectsConnectBeforeReadiness(t *testing.T) {
	var readinessCalls int
	config := defaultAdapterConfig()
	config.ClusterMode = true
	config.LocalMemberID = 4
	config.ClusterReady = func() bool {
		readinessCalls++
		return true
	}
	config.ClusterAdmissionOpen = func() bool { return false }
	config.ClusterProjectionReady = func() bool { return true }
	runtime := newAdapterTestRuntime(t, 2, config)
	defer runtime.stop(t)

	if !waitForActiveConnections(runtime.server, 1, time.Second) {
		t.Fatal("accepted pre-CONNECT socket was not counted as active")
	}
	writeTestPackets(t, runtime.client, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "drain-admission",
	})
	frame := readTestFrame(t, runtime.client)
	packet, consumed, err := runtime.server.limits.Load().decoder.Decode(frame, mqtt.ProtocolV5)
	if err != nil || consumed != len(frame) {
		t.Fatalf("decode rejected CONNACK: packet=%T consumed=%d err=%v", packet, consumed, err)
	}
	connAck, ok := packet.(*mqtt.ConnAck)
	if !ok || connAck.ReasonCode != 0x88 {
		t.Fatalf("drain admission CONNACK = %#v, want reason 0x88", packet)
	}
	if readinessCalls != 0 {
		t.Fatalf("cluster readiness called %d times after admission closed", readinessCalls)
	}
}

func waitForActiveConnections(server *Server, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if server.ActiveConnectionCount() == want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return server.ActiveConnectionCount() == want
}
