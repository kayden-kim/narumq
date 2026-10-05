package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/jsonl"
	"narumq/tests/benchmark/internal/mqttclient"
)

type dialerTestHealth struct {
	status            atomic.Int32
	badAuthorization  atomic.Bool
	wantAuthorization string
	server            *httptest.Server
}

func newDialerTestHealth(t *testing.T, status int, wantAuthorization string) *dialerTestHealth {
	t.Helper()
	health := &dialerTestHealth{wantAuthorization: wantAuthorization}
	health.status.Store(int32(status))
	health.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != health.wantAuthorization {
			health.badAuthorization.Store(true)
		}
		writer.WriteHeader(int(health.status.Load()))
	}))
	t.Cleanup(health.server.Close)
	return health
}

type dialerTestTCPNode struct {
	listener net.Listener
	accepted chan string
	pings    chan string
}

func newDialerTestTCPNode(t *testing.T, label string) *dialerTestTCPNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node := &dialerTestTCPNode{
		listener: listener,
		accepted: make(chan string, 8),
		pings:    make(chan string, 8),
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func(connection net.Conn) {
				defer connection.Close()
				if _, err := sessionTestHandshake(connection, false); err != nil {
					return
				}
				node.accepted <- label
				for {
					packet, err := readSessionTestPacket(connection, mqtt.ProtocolV5)
					if err != nil {
						return
					}
					if _, ok := packet.(*mqtt.PingReq); ok {
						node.pings <- label
					}
				}
			}(connection)
		}
	}()
	return node
}

func TestReadinessDialerTracksHTTPReadinessTransitions(t *testing.T) {
	tcpNode1 := newDialerTestTCPNode(t, "node1")
	tcpNode2 := newDialerTestTCPNode(t, "node2")
	health1 := newDialerTestHealth(t, http.StatusOK, "Bearer test-token")
	health2 := newDialerTestHealth(t, http.StatusServiceUnavailable, "Bearer test-token")
	healthLog, logPath := openDialerTestHealthLog(t)
	dialer := newReadinessDialer(
		"narumq",
		[]string{tcpNode1.listener.Addr().String(), tcpNode2.listener.Addr().String()},
		[]string{health1.server.URL, health2.server.URL},
		"test-token",
		5*time.Millisecond,
		healthLog,
	)
	t.Cleanup(func() {
		dialer.Close()
		if err := healthLog.Close(); err != nil {
			t.Errorf("close health log: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	dialer.Start(ctx)
	waitForDialerHealthState(t, logPath, "node1", true, http.StatusOK)
	waitForDialerHealthState(t, logPath, "node2", false, http.StatusServiceUnavailable)

	waitCtx, cancelWait := context.WithTimeout(ctx, 30*time.Millisecond)
	if err := dialer.WaitAllReady(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		cancelWait()
		t.Fatalf("WaitAllReady() error = %v, want deadline exceeded while node2 is unavailable", err)
	}
	cancelWait()

	health2.status.Store(http.StatusOK)
	if err := dialer.WaitAllReady(ctx); err != nil {
		t.Fatalf("WaitAllReady() after both nodes became ready: %v", err)
	}
	client1, _, label, err := dialer.Dial(ctx, mqttclient.ConnectOptions{ClientID: "dialer-transition-1"})
	if err != nil {
		t.Fatalf("Dial() to initial ready node: %v", err)
	}
	defer client1.Close()
	if label != "node1" {
		t.Fatalf("first Dial() node = %q, want node1", label)
	}
	requireDialerTCPEvent(t, tcpNode1.accepted, "node1", "initial connection")

	health1.status.Store(http.StatusServiceUnavailable)
	waitForDialerHealthState(t, logPath, "node1", false, http.StatusServiceUnavailable)
	client2, _, label, err := dialer.Dial(ctx, mqttclient.ConnectOptions{ClientID: "dialer-transition-2"})
	if err != nil {
		t.Fatalf("Dial() while node1 is unavailable: %v", err)
	}
	defer client2.Close()
	if label != "node2" {
		t.Fatalf("Dial() node after node1 became unavailable = %q, want node2", label)
	}
	requireDialerTCPEvent(t, tcpNode2.accepted, "node2", "connection after node1 became unavailable")

	if err := client1.Write(&mqtt.PingReq{Version: mqtt.ProtocolV5}, time.Second); err != nil {
		t.Fatalf("write on connection opened before node1 became unavailable: %v", err)
	}
	requireDialerTCPEvent(t, tcpNode1.pings, "node1", "existing connection after readiness transition")

	health2.status.Store(http.StatusServiceUnavailable)
	waitForDialerHealthState(t, logPath, "node2", false, http.StatusServiceUnavailable)
	noReadyCtx, cancelNoReady := context.WithTimeout(ctx, 30*time.Millisecond)
	_, _, _, err = dialer.Dial(noReadyCtx, mqttclient.ConnectOptions{ClientID: "dialer-no-ready-node"})
	cancelNoReady()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dial() with no ready nodes error = %v, want deadline exceeded", err)
	}

	health1.status.Store(http.StatusOK)
	waitForDialerHealthState(t, logPath, "node1", true, http.StatusOK)
	client3, _, label, err := dialer.Dial(ctx, mqttclient.ConnectOptions{ClientID: "dialer-transition-3"})
	if err != nil {
		t.Fatalf("Dial() after node1 recovered: %v", err)
	}
	defer client3.Close()
	if label != "node1" {
		t.Fatalf("Dial() node after node1 recovered = %q, want node1", label)
	}
	requireDialerTCPEvent(t, tcpNode1.accepted, "node1", "connection after node1 recovered")

	if health1.badAuthorization.Load() || health2.badAuthorization.Load() {
		t.Fatal("NaruMQ health requests did not include the expected Bearer token")
	}
	for _, event := range readDialerHealthEvents(t, logPath) {
		if _, err := time.Parse(time.RFC3339Nano, event.At); err != nil {
			t.Errorf("health event at %q is not RFC3339Nano: %v", event.At, err)
		}
	}
}

func TestReadinessDialerDialsReadyNodesRoundRobin(t *testing.T) {
	tcpNode1 := newDialerTestTCPNode(t, "node1")
	tcpNode2 := newDialerTestTCPNode(t, "node2")
	health1 := newDialerTestHealth(t, http.StatusOK, "")
	health2 := newDialerTestHealth(t, http.StatusOK, "")
	healthLog, _ := openDialerTestHealthLog(t)
	dialer := newReadinessDialer(
		"emqx",
		[]string{tcpNode1.listener.Addr().String(), tcpNode2.listener.Addr().String()},
		[]string{health1.server.URL, health2.server.URL},
		"must-not-be-sent",
		5*time.Millisecond,
		healthLog,
	)
	t.Cleanup(func() {
		dialer.Close()
		if err := healthLog.Close(); err != nil {
			t.Errorf("close health log: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	dialer.Start(ctx)
	if err := dialer.WaitAllReady(ctx); err != nil {
		t.Fatalf("WaitAllReady(): %v", err)
	}

	nodes := []*dialerTestTCPNode{tcpNode1, tcpNode2}
	clientIDs := [...]string{
		"dialer-round-robin-1",
		"dialer-round-robin-2",
		"dialer-round-robin-3",
		"dialer-round-robin-4",
	}
	for index, expected := range []string{"node1", "node2", "node1", "node2"} {
		client, _, label, err := dialer.Dial(ctx, mqttclient.ConnectOptions{ClientID: clientIDs[index]})
		if err != nil {
			t.Fatalf("Dial() attempt %d: %v", index+1, err)
		}
		if label != expected {
			_ = client.Close()
			t.Fatalf("Dial() attempt %d returned %q, want %q", index+1, label, expected)
		}
		requireDialerTCPEvent(t, nodes[index%len(nodes)].accepted, expected, "round-robin connection")
		if err := client.Close(); err != nil {
			t.Fatalf("close dialed client %d: %v", index+1, err)
		}
	}
	if health1.badAuthorization.Load() || health2.badAuthorization.Load() {
		t.Fatal("EMQX health requests unexpectedly included an Authorization header")
	}
}

func TestReadinessDialerReturnsHealthLogErrors(t *testing.T) {
	health := newDialerTestHealth(t, http.StatusOK, "")
	healthLog, err := jsonl.Open(filepath.Join(t.TempDir(), "health.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if err := healthLog.Close(); err != nil {
		t.Fatal(err)
	}
	dialer := newReadinessDialer("emqx", []string{"127.0.0.1:1"}, []string{health.server.URL}, "", 5*time.Millisecond, healthLog)
	t.Cleanup(dialer.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dialer.Start(ctx)

	if err := dialer.WaitAllReady(ctx); err == nil {
		t.Fatal("WaitAllReady() returned nil after the health log write failed")
	}
	if _, _, _, err := dialer.Dial(ctx, mqttclient.ConnectOptions{ClientID: "dialer-log-error"}); err == nil {
		t.Fatal("Dial() returned nil error after the health log write failed")
	}
}

func openDialerTestHealthLog(t *testing.T) (*jsonl.Writer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "health.jsonl")
	writer, err := jsonl.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return writer, path
}

func waitForDialerHealthState(t *testing.T, path, node string, ready bool, status int) readinessTransition {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var latest readinessTransition
		found := false
		for _, event := range readDialerHealthEvents(t, path) {
			if event.Node == node {
				latest = event
				found = true
			}
		}
		if found && latest.Ready == ready && latest.Status == status {
			return latest
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("latest health event for %s did not become ready=%t status=%d", node, ready, status)
	return readinessTransition{}
}

func readDialerHealthEvents(t *testing.T, path string) []readinessTransition {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lastNewline := bytes.LastIndexByte(data, '\n')
	if lastNewline < 0 {
		return nil
	}
	var events []readinessTransition
	scanner := bufio.NewScanner(bytes.NewReader(data[:lastNewline+1]))
	for scanner.Scan() {
		var event readinessTransition
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("decode health event %q: %v", scanner.Text(), err)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read health events: %v", err)
	}
	return events
}

func requireDialerTCPEvent(t *testing.T, events <-chan string, want, description string) {
	t.Helper()
	select {
	case got := <-events:
		if got != want {
			t.Fatalf("%s reached %s, want %s", description, got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s at %s", description, want)
	}
}
