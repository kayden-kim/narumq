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

type runnerIngressFixture struct {
	listener net.Listener
	cancel   context.CancelFunc
	done     chan error
}

func newRunnerIngressFixture(t *testing.T) *runnerIngressFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	routes := routing.NewTable()
	deliveries, err := deliveryqueue.New(64)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	retainedRepository, err := memoryretained.New(1024, 1<<20)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	repository := sessionstore.New()
	willBridge := tcpadapter.NewWillBridge()
	systemClock := clock.SystemClock{}
	connections, err := connect.New(systemClock, repository, deliveries, routes, clock.SystemScheduler{}, willBridge)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes, deliveries, retainedRepository)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	publications, err := publish.New(systemClock, routes, deliveries, retainedRepository)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(benchmarkMaxPacketSize)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(benchmarkMaxPacketSize)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	server, err := tcpadapter.NewServer(
		listener,
		tcpadapter.Config{
			Clock:                systemClock,
			MaxPacketSize:        benchmarkMaxPacketSize,
			ConnectTimeout:       time.Second,
			WriteTimeout:         time.Second,
			ControlQueueCapacity: 16,
			ReceiveMaximum:       64, PacketReadTimeout: 10 * time.Second,
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
		listener.Close()
		t.Fatal(err)
	}
	willBridge.Bind(server)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	return &runnerIngressFixture{listener: listener, cancel: cancel, done: done}
}

func (fixture *runnerIngressFixture) close(t *testing.T) {
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
}

// Acceptance: NARUMQ_LOAD_QUALITY
// Scenarios: performance-and-verification.md:PRF-SCN-122
func TestRunAgainstInProcessNaruMQ(t *testing.T) {
	if os.Getenv("NARUMQ_LOAD_QUALITY") != "1" {
		t.Skip("set NARUMQ_LOAD_QUALITY=1 to run the BP-01 in-process load run")
	}
	fixture := newRunnerIngressFixture(t)
	defer fixture.close(t)

	profile := Profile{
		ProfileID:            "bp01-local-runner",
		Purpose:              purposeSmoke,
		Seed:                 7,
		Topic:                "bp01/local",
		PayloadBytes:         32,
		PublishRatePerSecond: 20,
		WarmupMillis:         25,
		MeasureMillis:        250,
		DrainMillis:          250,
		MaxSamples:           16,
		BrokerCPUs:           1,
		BrokerMemoryBytes:    1 << 30,
		GeneratorCPUs:        1,
		GeneratorMemoryBytes: 1 << 30,
	}
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
	if !summary.Valid || summary.Sent == 0 || summary.Sent != summary.Delivered || summary.ProtocolErrors != 0 {
		t.Fatalf("Run() summary = %#v, want valid delivered workload", summary)
	}
	samples, err := os.ReadFile(outputDir + string(os.PathSeparator) + "samples.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if len(strings.TrimSpace(string(samples))) == 0 {
		t.Fatal("samples.jsonl is empty")
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
		{phaseWarmup, "start"},
		{phaseWarmup, "end"},
		{phaseMeasurement, "start"},
		{phaseMeasurement, "end"},
		{phaseDrain, "start"},
		{phaseDrain, "end"},
	}
	if len(phaseEvents) != len(wantPhases) {
		t.Fatalf("phase event count = %d, want %d", len(phaseEvents), len(wantPhases))
	}
	for index, want := range wantPhases {
		if phaseEvents[index].Phase != want.phase || phaseEvents[index].Event != want.event {
			t.Fatalf("phase event %d = %#v, want %#v", index, phaseEvents[index], want)
		}
	}
}
