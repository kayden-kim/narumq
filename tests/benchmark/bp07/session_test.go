package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/jsonl"
	"narumq/tests/benchmark/internal/mqttclient"
)

type sessionTestHealth struct {
	status atomic.Int32
	server *httptest.Server
}

func newSessionTestHealth(status int) *sessionTestHealth {
	health := &sessionTestHealth{}
	health.status.Store(int32(status))
	health.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(int(health.status.Load()))
	}))
	return health
}

func newSessionTestDialer(t *testing.T, nodes []string, health []*sessionTestHealth) *readinessDialer {
	t.Helper()
	urls := make([]string, len(health))
	for index, endpoint := range health {
		urls[index] = endpoint.server.URL
		t.Cleanup(endpoint.server.Close)
	}
	healthLog, err := jsonl.Open(filepath.Join(t.TempDir(), "health.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dialer := newReadinessDialer("emqx", nodes, urls, "", 5*time.Millisecond, healthLog)
	dialer.Start(ctx)
	t.Cleanup(func() {
		cancel()
		dialer.Close()
		if err := healthLog.Close(); err != nil {
			t.Errorf("close health log: %v", err)
		}
	})
	return dialer
}

// TestSessionReconnectUsesReadyNodeAndResumesPersistentSession verifies reconnect to a ready node resumes the persistent session.
// Scenarios: performance-and-verification.md:PRF-SCN-019
func TestSessionReconnectUsesReadyNodeAndResumesPersistentSession(t *testing.T) {
	listenerA := listenSessionTestNode(t)
	listenerB := listenSessionTestNode(t)
	healthA := newSessionTestHealth(http.StatusOK)
	healthB := newSessionTestHealth(http.StatusServiceUnavailable)
	dialer := newSessionTestDialer(t, []string{listenerA.Addr().String(), listenerB.Addr().String()}, []*sessionTestHealth{healthA, healthB})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	profile := sessionTestProfile()
	collector := newCollector("emqx", profile)
	var round currentRound
	round.set(roundMarker{Round: 1, Node: "node1", State: "draining"})
	eventPath := filepath.Join(t.TempDir(), "connections.jsonl")
	events, err := jsonl.Open(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	session := newSessionClient(clientSpec{
		id: "bp07-idle-01", role: "idle", version: mqtt.ProtocolV5, persistent: true,
	}, 10, profile, credentials{}, dialer, collector, &round, events)
	defer func() {
		if err := session.close(); err != nil {
			t.Errorf("close session: %v", err)
		}
		if err := events.Close(); err != nil {
			t.Errorf("close event log: %v", err)
		}
	}()

	// Consume CONNECT before the server can report a successful completion.
	aConnect := make(chan *mqtt.Connect)
	aErr := make(chan error, 1)
	go func() {
		connection, err := listenerA.Accept()
		if err != nil {
			aErr <- err
			return
		}
		defer connection.Close()
		connect, err := sessionTestHandshake(connection, false)
		if err != nil {
			aErr <- err
			return
		}
		aConnect <- connect
		healthA.status.Store(http.StatusServiceUnavailable)
		healthB.status.Store(http.StatusOK)
		if err := writeSessionTestDisconnect(connection, 0x9C, bp07LBReference); err != nil {
			aErr <- err
			return
		}
		aErr <- nil
	}()
	bConnect := make(chan *mqtt.Connect)
	bErr := make(chan error, 1)
	go func() {
		connection, err := listenerB.Accept()
		if err != nil {
			bErr <- err
			return
		}
		defer connection.Close()
		connect, err := sessionTestHandshake(connection, true)
		if err != nil {
			bErr <- err
			return
		}
		bConnect <- connect
		packet, err := readSessionTestPacket(connection, mqtt.ProtocolV5)
		if err != nil {
			bErr <- err
			return
		}
		if _, ok := packet.(*mqtt.Disconnect); !ok {
			bErr <- fmt.Errorf("received %T during cleanup, want DISCONNECT", packet)
			return
		}
		bErr <- nil
	}()

	if err := session.start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case connect := <-aConnect:
		if connect.ClientID != "bp07-idle-01" || !connect.CleanStart || connect.SessionExpiryInterval != profile.SessionExpirySeconds {
			t.Fatalf("initial CONNECT = %#v, want clean MQTT 5 persistent CONNECT", connect)
		}
	case err := <-aErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case connect := <-bConnect:
		if connect.CleanStart || connect.SessionExpiryInterval != profile.SessionExpirySeconds {
			t.Fatalf("resumed CONNECT = %#v, want CleanStart=false with session expiry", connect)
		}
	case err := <-bErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := <-aErr; err != nil {
		t.Fatal(err)
	}
	waitForSessionNode(t, session, "node2", ctx)
	summary := collector.summary()
	if !summary.Valid {
		t.Fatalf("redirect workload became invalid: %#v", summary)
	}
	if len(summary.Rounds) != 1 || summary.Rounds[0].Reconnects != 1 {
		t.Fatalf("redirect round summary = %#v, want one attributed reconnect", summary.Rounds)
	}
	if err := session.close(); err != nil {
		t.Fatal(err)
	}
	if err := <-bErr; err != nil {
		t.Fatal(err)
	}
	if summary := collector.summary(); !summary.Valid || summary.Totals.UnplannedDisconnects != 0 {
		t.Fatalf("intentional close changed workload validity: %#v", summary)
	}
	if err := events.Close(); err != nil {
		t.Fatal(err)
	}
	eventData, err := os.ReadFile(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	disconnect, reconnect := findSessionEvents(t, eventData)
	if disconnect.ReasonCode != 0x9C || disconnect.ServerReference != bp07LBReference {
		t.Fatalf("disconnect event = %#v, want reason 0x9C and the shared reference", disconnect)
	}
	if reconnect.Node != "node2" || reconnect.ReconnectLatencyMillis <= 0 {
		t.Fatalf("reconnect event = %#v, want node2 and positive reconnect latency", reconnect)
	}
}

// TestSessionRejectsUnexpectedServerReference verifies an unexpected Server Reference is a protocol error after resuming the session.
// Scenarios: performance-and-verification.md:PRF-SCN-019
func TestSessionRejectsUnexpectedServerReference(t *testing.T) {
	listenerA := listenSessionTestNode(t)
	listenerB := listenSessionTestNode(t)
	healthA := newSessionTestHealth(http.StatusOK)
	healthB := newSessionTestHealth(http.StatusServiceUnavailable)
	dialer := newSessionTestDialer(t, []string{listenerA.Addr().String(), listenerB.Addr().String()}, []*sessionTestHealth{healthA, healthB})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	profile := sessionTestProfile()
	collector := newCollector("emqx", profile)
	var round currentRound
	round.set(roundMarker{Round: 1, Node: "node1", State: "ready"})
	events, err := jsonl.Open(filepath.Join(t.TempDir(), "connections.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	session := newSessionClient(clientSpec{
		id: "bp07-idle-01", role: "idle", version: mqtt.ProtocolV5, persistent: true,
	}, 10, profile, credentials{}, dialer, collector, &round, events)
	defer func() {
		if err := session.close(); err != nil {
			t.Errorf("close session: %v", err)
		}
		if err := events.Close(); err != nil {
			t.Errorf("close event log: %v", err)
		}
	}()

	resumed := make(chan *mqtt.Connect, 1)
	serverErr := make(chan error, 1)
	go func() {
		for attempt := range 2 {
			connection, err := listenerA.Accept()
			if err != nil {
				serverErr <- err
				return
			}
			connect, err := sessionTestHandshake(connection, attempt == 1)
			if err != nil {
				_ = connection.Close()
				serverErr <- err
				return
			}
			if attempt == 0 {
				if err := writeSessionTestDisconnect(connection, 0x9C, "unexpected.bp07.test:1883"); err != nil {
					_ = connection.Close()
					serverErr <- err
					return
				}
				_ = connection.Close()
				continue
			}
			resumed <- connect
			packet, err := readSessionTestPacket(connection, mqtt.ProtocolV5)
			if err == nil {
				if _, ok := packet.(*mqtt.Disconnect); !ok {
					err = fmt.Errorf("received %T during cleanup, want DISCONNECT", packet)
				}
			}
			_ = connection.Close()
			if err != nil {
				serverErr <- err
				return
			}
		}
		serverErr <- nil
	}()

	if err := session.start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case connect := <-resumed:
		if connect.CleanStart {
			t.Fatal("reconnect used CleanStart=true")
		}
	case err := <-serverErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitForSessionNode(t, session, "node1", ctx)
	if err := session.close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	if got := collector.summary().Totals.UnplannedDisconnects; got != 1 {
		t.Fatalf("unplanned disconnects = %d, want only the redirected disconnect", got)
	}
	if got := collector.summary().Totals.ProtocolErrors; got != 1 {
		t.Fatalf("protocol errors = %d, want 1 for an unexpected Server Reference", got)
	}
	if collector.summary().Valid {
		t.Fatal("unexpected Server Reference did not invalidate the workload")
	}
}

// TestRollingRoundRemainsMonotonicForDisconnectClassification verifies a stale round marker does not revert disconnect attribution.
// Scenarios: performance-and-verification.md:PRF-SCN-020
func TestRollingRoundRemainsMonotonicForDisconnectClassification(t *testing.T) {
	profile := sessionTestProfile()
	profile.ReplacementOrder = []int{3, 1, 2}
	outputDir := t.TempDir()
	controlDir := filepath.Join(outputDir, "control")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatal(err)
	}

	for _, marker := range []roundMarker{
		{Round: 1, Node: "node3", State: "draining"},
		{Round: 2, Node: "node1", State: "draining"},
		{Round: 3, Node: "node2", State: "draining"},
	} {
		path := filepath.Join(controlDir, fmt.Sprintf("round-%02d.json", marker.Round))
		if err := writeAtomicJSON(path, marker); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeAtomicJSON(filepath.Join(controlDir, "rolling-complete.json"), rollingCompletion{Status: "success"}); err != nil {
		t.Fatal(err)
	}

	var current currentRound
	if err := watchRolling(context.Background(), profile, outputDir, &current); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(controlDir, "round-03.json")); err != nil {
		t.Fatal(err)
	}
	if err := watchRolling(context.Background(), profile, outputDir, &current); err != nil {
		t.Fatal(err)
	}

	collector := newCollector("emqx", profile)
	if round, planned := collector.recordDisconnect("bp07-idle-01", "node2", current.snapshot(), "0x9C", bp07LBReference, time.Now()); !planned || round != 3 {
		t.Fatalf("node2 evacuation attribution = (%d, %v), want (3, true)", round, planned)
	}
	if round, planned := collector.recordDisconnect("bp07-idle-01", "node1", current.snapshot(), "eof", "", time.Now()); planned || round != 0 {
		t.Fatalf("other-node EOF attribution = (%d, %v), want (0, false)", round, planned)
	}

	summary := collector.summary()
	if summary.Totals.UnplannedDisconnects != 1 {
		t.Fatalf("unplanned disconnects = %d, want 1 for the other-node EOF", summary.Totals.UnplannedDisconnects)
	}
	for _, round := range summary.Rounds {
		if round.Round == 3 {
			if round.Disconnects != 1 || round.ServerReferences[bp07LBReference] != 1 {
				t.Fatalf("round 3 summary = %#v, want the node2 evacuation and shared reference", round)
			}
			return
		}
	}
	t.Fatal("round 3 summary is missing")
}

func sessionTestProfile() Profile {
	return Profile{
		Seed: 7, PayloadBytes: 128, PublishRatePerStreamPerSecond: 10,
		MaxInflightPerPublisher: 8, AckTimeoutMillis: 1000,
		SessionExpirySeconds: 300, KeepAliveSeconds: 5,
		ReconnectTimeoutMillis: 5000, MaxMessages: 16,
		ReplacementOrder: []int{1}, Cycles: 1,
	}
}

func listenSessionTestNode(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func sessionTestHandshake(connection net.Conn, sessionPresent bool) (*mqtt.Connect, error) {
	packet, err := readSessionTestPacket(connection, mqtt.ProtocolV5)
	if err != nil {
		return nil, err
	}
	connect, ok := packet.(*mqtt.Connect)
	if !ok {
		return nil, fmt.Errorf("received %T, want CONNECT", packet)
	}
	encoder, err := mqtt.NewEncoder(mqttclient.MaxPacketSize)
	if err != nil {
		return nil, err
	}
	ack, err := encoder.Append(nil, &mqtt.ConnAck{Version: mqtt.ProtocolV5, SessionPresent: sessionPresent})
	if err != nil {
		return nil, err
	}
	if err := writeSessionTestBytes(connection, ack); err != nil {
		return nil, err
	}
	return connect, nil
}

func readSessionTestPacket(connection net.Conn, version mqtt.ProtocolVersion) (mqtt.Packet, error) {
	if err := connection.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return nil, err
	}
	var header [1]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return nil, err
	}
	wire := []byte{header[0]}
	remainingLength := 0
	multiplier := 1
	for range 4 {
		var encoded [1]byte
		if _, err := io.ReadFull(connection, encoded[:]); err != nil {
			return nil, err
		}
		wire = append(wire, encoded[0])
		remainingLength += int(encoded[0]&0x7f) * multiplier
		if encoded[0]&0x80 == 0 {
			break
		}
		multiplier *= 128
	}
	if remainingLength < 0 || remainingLength > mqttclient.MaxPacketSize {
		return nil, errors.New("test broker received invalid Remaining Length")
	}
	body := make([]byte, remainingLength)
	if _, err := io.ReadFull(connection, body); err != nil {
		return nil, err
	}
	wire = append(wire, body...)
	decoder, err := mqtt.NewDecoder(mqttclient.MaxPacketSize)
	if err != nil {
		return nil, err
	}
	packet, consumed, err := decoder.Decode(wire, version)
	if err != nil {
		return nil, err
	}
	if consumed != len(wire) {
		return nil, errors.New("test broker decoded a partial MQTT packet")
	}
	return packet, nil
}

func writeSessionTestDisconnect(connection net.Conn, reason byte, serverReference string) error {
	body := []byte{reason}
	if serverReference != "" {
		properties := []byte{0x1C, byte(len(serverReference) >> 8), byte(len(serverReference))}
		properties = append(properties, serverReference...)
		body = append(body, byte(len(properties)))
		body = append(body, properties...)
	}
	wire := []byte{0xE0, byte(len(body))}
	wire = append(wire, body...)
	return writeSessionTestBytes(connection, wire)
}

func writeSessionTestBytes(connection net.Conn, data []byte) error {
	for len(data) > 0 {
		written, err := connection.Write(data)
		if written > 0 {
			data = data[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func waitForSessionNode(t *testing.T, session *sessionClient, node string, ctx context.Context) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		session.mu.Lock()
		connected := session.current != nil && session.node == node
		session.mu.Unlock()
		if connected {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("session did not reconnect to %s: %v", node, ctx.Err())
		case <-ticker.C:
		}
	}
}

type sessionEventRecord struct {
	Event                  string  `json:"event"`
	Node                   string  `json:"node"`
	ReasonCode             byte    `json:"reasonCode"`
	ServerReference        string  `json:"serverReference"`
	ReconnectLatencyMillis float64 `json:"reconnectLatencyMillis"`
}

func findSessionEvents(t *testing.T, data []byte) (sessionEventRecord, sessionEventRecord) {
	t.Helper()
	var disconnect, reconnect sessionEventRecord
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		var event sessionEventRecord
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatal(err)
		}
		if event.Event == "disconnect" {
			disconnect = event
		}
		if event.Event == "connect" && event.ReconnectLatencyMillis > 0 {
			reconnect = event
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if disconnect.Event == "" || reconnect.Event == "" {
		t.Fatalf("connection events did not contain disconnect and reconnect: %s", data)
	}
	return disconnect, reconnect
}
