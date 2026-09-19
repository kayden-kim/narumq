package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"narumq/internal/adapters/clock"
	"narumq/internal/adapters/memory/deliveryqueue"
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
	repository := sessionstore.New()
	connections, err := connect.New(clock.SystemClock{}, repository, deliveries, routes)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(routes)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	publications, err := publish.New(routes, deliveries)
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
			MaxPacketSize:        benchmarkMaxPacketSize,
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
		listener.Close()
		t.Fatal(err)
	}
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

func TestRunAgainstInProcessNaruMQ(t *testing.T) {
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
}
