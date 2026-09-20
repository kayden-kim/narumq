package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"narumq/internal/adapters/clock"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	"narumq/internal/adapters/runtimemetrics"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/connect"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

func TestServeWithRuntimeMetricsSelectBranches(t *testing.T) {
	t.Run("server completion", func(t *testing.T) {
		listener, server := newRuntimeMetricsTestServer(t)
		if err := listener.Close(); err != nil {
			t.Fatalf("listener.Close() error = %v", err)
		}
		sampler := newRuntimeMetricsTestSampler(t)
		if err := serveWithRuntimeMetrics(context.Background(), server, sampler); err != nil {
			t.Fatalf("serveWithRuntimeMetrics() error = %v", err)
		}
	})

	t.Run("sampler completion", func(t *testing.T) {
		listener, server := newRuntimeMetricsTestServer(t)
		t.Cleanup(func() { _ = listener.Close() })
		sampler := newRuntimeMetricsTestSampler(t)
		canceledContext, cancel := context.WithCancel(context.Background())
		cancel()
		if err := sampler.Run(canceledContext); err != nil {
			t.Fatalf("sampler.Run(canceled) error = %v", err)
		}
		err := serveWithRuntimeMetrics(context.Background(), server, sampler)
		if err == nil || !strings.Contains(err.Error(), "runtime metrics sampler") {
			t.Fatalf("serveWithRuntimeMetrics() error = %v, want sampler error", err)
		}
	})
}

func newRuntimeMetricsTestServer(t *testing.T) (net.Listener, *tcpadapter.Server) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	routes := routing.NewTable()
	deliveries, err := deliveryqueue.New(2)
	if err != nil {
		t.Fatalf("deliveryqueue.New() error = %v", err)
	}
	retainedRepository, err := memoryretained.New(8, 1024)
	if err != nil {
		t.Fatalf("memoryretained.New() error = %v", err)
	}
	sessions := sessionstore.New()
	willBridge := tcpadapter.NewWillBridge()
	connections, err := connect.New(clock.SystemClock{}, sessions, deliveries, routes, clock.SystemScheduler{}, willBridge)
	if err != nil {
		t.Fatalf("connect.New() error = %v", err)
	}
	subscriptions, err := subscribe.New(routes, deliveries, retainedRepository)
	if err != nil {
		t.Fatalf("subscribe.New() error = %v", err)
	}
	publications, err := publish.New(routes, deliveries, retainedRepository)
	if err != nil {
		t.Fatalf("publish.New() error = %v", err)
	}
	decoder, err := mqtt.NewDecoder(1024)
	if err != nil {
		t.Fatalf("mqtt.NewDecoder() error = %v", err)
	}
	encoder, err := mqtt.NewEncoder(1024)
	if err != nil {
		t.Fatalf("mqtt.NewEncoder() error = %v", err)
	}
	server, err := tcpadapter.NewServer(
		listener,
		tcpadapter.Config{
			MaxPacketSize:        1024,
			ConnectTimeout:       time.Second,
			WriteTimeout:         time.Second,
			ControlQueueCapacity: 2,
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
		t.Fatalf("tcpadapter.NewServer() error = %v", err)
	}
	willBridge.Bind(server)
	return listener, server
}

func newRuntimeMetricsTestSampler(t *testing.T) *runtimemetrics.Sampler {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime-metrics.jsonl")
	sampler, err := runtimemetrics.New(path, time.Hour)
	if err != nil {
		t.Fatalf("runtimemetrics.New() error = %v", err)
	}
	return sampler
}
