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

// TestSessionReconnectUsesReadyNodeAndResumesPersistentSession verifies reconnect attribution to the failure round on a ready node.
// Scenarios: performance-and-verification.md:PRF-SCN-030
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
	round.set(roundMarker{Round: 1, Node: "node1", State: "killing"})
	eventPath := filepath.Join(t.TempDir(), "connections.jsonl")
	events, err := jsonl.Open(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	session := newSessionClient(clientSpec{
		id: "bp06-idle-01", role: "idle", version: mqtt.ProtocolV5, persistent: true,
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
		if err := writeSessionTestDisconnect(connection, 0x9C, ""); err != nil {
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
		if connect.ClientID != "bp06-idle-01" || !connect.CleanStart || connect.SessionExpiryInterval != profile.SessionExpirySeconds {
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
	if err := session.close(); err != nil {
		t.Fatal(err)
	}
	if err := <-bErr; err != nil {
		t.Fatal(err)
	}
	summary := collector.summary()
	if len(summary.Rounds) != 3 || summary.Rounds[0].Reconnects != 1 || summary.Totals.ProtocolErrors != 0 {
		t.Fatalf("planned failure round summary = %#v, want one reconnect and no protocol errors", summary)
	}
	if got := collector.rounds[1].disconnectsByClient; len(got) != 1 || got[0].ClientID != "bp06-idle-01" || got[0].ReconnectedAt.IsZero() {
		t.Fatalf("planned disconnect/reconnect attribution = %#v", got)
	}
	if err := events.Close(); err != nil {
		t.Fatal(err)
	}
	eventData, err := os.ReadFile(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	disconnect, reconnect := findSessionEvents(t, eventData)
	if disconnect.ReasonCode != 0x9C || disconnect.ServerReference != "" {
		t.Fatalf("disconnect event = %#v, want reason 0x9C without Server Reference", disconnect)
	}
	if reconnect.Node != "node2" || reconnect.ReconnectLatencyMillis <= 0 {
		t.Fatalf("reconnect event = %#v, want node2 and positive reconnect latency", reconnect)
	}
}

// TestSessionRejectsUnexpectedServerReference verifies that an unplanned redirected disconnect invalidates the workload.
// Scenarios: performance-and-verification.md:PRF-SCN-030
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
		id: "bp06-idle-01", role: "idle", version: mqtt.ProtocolV5, persistent: true,
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
				if err := writeSessionTestDisconnect(connection, 0x9C, "unexpected.bp06.test:1883"); err != nil {
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

// TestWatchFailureAcknowledgesKillingAndIgnoresStaleRoundMarkers verifies control marker handling and stale round marker rejection.
// Scenarios: performance-and-verification.md:PRF-SCN-030
func TestWatchFailureAcknowledgesKillingAndIgnoresStaleRoundMarkers(t *testing.T) {
	profile := sessionTestProfile()
	profile.KillOrder = []int{3, 1, 2}
	outputDir := t.TempDir()
	controlDir := filepath.Join(outputDir, "control")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeAtomicJSON(filepath.Join(controlDir, "round-01.json"), roundMarker{Round: 1, Node: "node3", State: "killing"}); err != nil {
		t.Fatal(err)
	}
	if err := writeAtomicJSON(filepath.Join(controlDir, "round-02.json"), roundMarker{Round: 2, Node: "node1", State: "down"}); err != nil {
		t.Fatal(err)
	}
	if err := writeAtomicJSON(filepath.Join(controlDir, "failure-complete.json"), failureCompletion{Status: "success"}); err != nil {
		t.Fatal(err)
	}
	collector := newCollector("emqx", profile)
	var current currentRound
	current.set(roundMarker{Round: 3, Node: "node2", State: "ready"})
	if err := watchFailure(context.Background(), profile, outputDir, &current, collector); err != nil {
		t.Fatal(err)
	}
	if got := current.snapshot(); got.Round != 3 || got.Node != "node2" {
		t.Fatalf("stale marker regressed current round to %#v", got)
	}
	var ack struct {
		Round      int    `json:"round"`
		State      string `json:"state"`
		ObservedAt string `json:"observedAt"`
	}
	if err := readControlJSON(filepath.Join(controlDir, "round-01-ack.json"), &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Round != 1 || ack.State != "killing" || ack.ObservedAt == "" {
		t.Fatalf("marker acknowledgement = %#v", ack)
	}
	if got := collector.rounds[2].downObservedAt; got.IsZero() {
		t.Fatal("first down marker observation was not recorded")
	}
}

// TestFailureDisconnectAttributionRequiresSameNodeAndKillingOrDown verifies disconnect attribution to planned rounds and unplanned counting.
// Scenarios: performance-and-verification.md:PRF-SCN-030
func TestFailureDisconnectAttributionRequiresSameNodeAndKillingOrDown(t *testing.T) {
	profile := sessionTestProfile()
	collector := newCollector("emqx", profile)
	at := time.Now()
	for _, tc := range []struct {
		marker roundMarker
		node   string
		want   bool
	}{
		{roundMarker{Round: 1, Node: "node1", State: "killing"}, "node1", true},
		{roundMarker{Round: 1, Node: "node1", State: "down"}, "node1", true},
		{roundMarker{Round: 1, Node: "node1", State: "restarting"}, "node1", false},
		{roundMarker{Round: 1, Node: "node1", State: "ready"}, "node1", false},
		{roundMarker{Round: 1, Node: "node1", State: "killing"}, "node2", false},
	} {
		_, got := collector.recordDisconnect("bp06-idle-01", tc.node, tc.marker, "eof", "", at)
		if got != tc.want {
			t.Fatalf("attribution for %#v node %s = %v, want %v", tc.marker, tc.node, got, tc.want)
		}
	}
	summary := collector.summary()
	if summary.Rounds[0].Disconnects != 2 || summary.Totals.UnplannedDisconnects != 3 {
		t.Fatalf("planned/unplanned totals = %#v / %d", summary.Rounds[0], summary.Totals.UnplannedDisconnects)
	}
}
func sessionTestProfile() Profile {
	return Profile{
		Seed: 7, PayloadBytes: 128, PublishRatePerStreamPerSecond: 10,
		MaxInflightPerPublisher: 8, AckTimeoutMillis: 1000,
		SessionExpirySeconds: 300, KeepAliveSeconds: 5,
		ReconnectTimeoutMillis: 5000, RecoveryTimeoutMillis: 30000, MaxMessages: 16,
		KillOrder: []int{1}, Cycles: 1,
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
