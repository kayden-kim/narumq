package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"narumq/internal/adapters/clock"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/connect"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

type bp05IngressFixture struct {
	listener net.Listener
	cancel   context.CancelFunc
	done     chan error
}

func newBP05IngressFixture(t *testing.T) *bp05IngressFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	routes := routing.NewTable()
	deliveries, err := deliveryqueue.New(64)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	retainedRepository, err := memoryretained.New(1024, 1<<20)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	repository := sessionstore.New()
	connections, err := connect.New(clock.SystemClock{}, repository, deliveries, routes)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes, deliveries, retainedRepository)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	publications, err := publish.New(routes, deliveries, retainedRepository)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(1 << 20)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(1 << 20)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	server, err := tcpadapter.NewServer(
		listener,
		tcpadapter.Config{
			MaxPacketSize:        1 << 20,
			ConnectTimeout:       time.Second,
			WriteTimeout:         time.Second,
			ControlQueueCapacity: 16,
		},
		decoder,
		encoder,
		connections,
		subscriptions,
		publications,
		deliveries,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	return &bp05IngressFixture{listener: listener, cancel: cancel, done: done}
}

func (fixture *bp05IngressFixture) close(t *testing.T) {
	t.Helper()
	fixture.cancel()
	select {
	case err := <-fixture.done:
		if err != nil {
			t.Fatalf("Serve() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not stop")
	}
	_ = fixture.listener.Close()
}

func TestRunAgainstInProcessNaruMQ(t *testing.T) {
	fixture := newBP05IngressFixture(t)
	defer fixture.close(t)

	profile := validBP05Profile()
	profile.ProfileID = "bp05-local-runner"
	profile.ConnectionCount = 8
	profile.RampConnectionsPerSecond = 8
	profile.BaselineMillis = 2000
	profile.IdleMillis = 2000
	profile.ReconnectCount = 2
	profile.ReconnectRatePerSecond = 8
	profile.KeepAliveSeconds = 2
	profile.PingIntervalMillis = 400
	outputDir := t.TempDir()
	summary, err := Run(context.Background(), RunOptions{
		Profile:   profile,
		Target:    "narumq",
		Address:   fixture.listener.Addr().String(),
		OutputDir: outputDir,
	})
	if err != nil {
		t.Fatalf("Run() error = %v, summary = %#v", err, summary)
	}
	if !summary.Valid || summary.SucceededInitialConnections != 8 || summary.IdleSurvivors != 8 || summary.ReconnectSucceeded != 2 || summary.PingErrors != 0 || summary.ProtocolErrors != 0 || summary.TransportErrors != 0 || summary.GeneratorOverload != 0 || summary.FinalPingErrors != 0 {
		t.Fatalf("Run() summary = %#v, want valid 8-connection smoke", summary)
	}
	for _, name := range []string{"connections.jsonl", "phases.jsonl", "workload-summary.json"} {
		if data, err := os.ReadFile(outputDir + string(os.PathSeparator) + name); err != nil || len(data) == 0 {
			t.Fatalf("artifact %s error=%v bytes=%d", name, err, len(data))
		}
	}
	phasesData, err := os.ReadFile(outputDir + string(os.PathSeparator) + "phases.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var phaseEvents []struct {
		Phase     string `json:"phase"`
		Event     string `json:"event"`
		Timestamp string `json:"timestamp"`
	}
	for _, line := range strings.Split(strings.TrimSpace(string(phasesData)), "\n") {
		var event struct {
			Phase     string `json:"phase"`
			Event     string `json:"event"`
			Timestamp string `json:"timestamp"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		if _, err := time.Parse(time.RFC3339Nano, event.Timestamp); err != nil {
			t.Fatalf("phase timestamp %q: %v", event.Timestamp, err)
		}
		phaseEvents = append(phaseEvents, event)
	}
	wantPhases := []struct {
		phase string
		event string
	}{
		{"ramp", "start"},
		{"ramp", "end"},
		{"idle", "start"},
		{"idle", "end"},
		{"reconnect", "start"},
		{"reconnect", "end"},
		{"final_check", "start"},
		{"final_check", "end"},
		{"cleanup", "start"},
		{"cleanup", "end"},
	}
	if len(phaseEvents) != len(wantPhases) {
		t.Fatalf("phase event count = %d, want %d (%#v)", len(phaseEvents), len(wantPhases), phaseEvents)
	}
	for index, want := range wantPhases {
		if phaseEvents[index].Phase != want.phase || phaseEvents[index].Event != want.event {
			t.Fatalf("phase event %d = %#v, want %q/%q", index, phaseEvents[index], want.phase, want.event)
		}
	}
}
