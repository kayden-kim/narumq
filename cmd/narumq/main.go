package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
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
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(os.Args[1:], logger); err != nil {
		logger.Error("narumq startup or runtime failure", "error", err.Error())
		os.Exit(1)
	}
}

func run(args []string, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runWithContext(ctx, args, logger)
}

func runWithContext(ctx context.Context, args []string, logger *slog.Logger) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}
	flagSet := flag.NewFlagSet("narumq", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)
	tcpListen := flagSet.String("tcp-listen", "", "development-only loopback TCP listen address")
	maxPacketSize := flagSet.Int("max-packet-size", 1048576, "maximum MQTT Remaining Length")
	connectTimeout := flagSet.Duration("connect-timeout", 10*time.Second, "CONNECT handshake timeout")
	writeTimeout := flagSet.Duration("write-timeout", 5*time.Second, "socket write timeout")
	deliveryQueueCapacity := flagSet.Int("delivery-queue-capacity", 64, "per-session delivery queue capacity")
	controlQueueCapacity := flagSet.Int("control-queue-capacity", 16, "per-connection control queue capacity")
	retainedMessageCapacity := flagSet.Int("retained-message-capacity", 1024, "maximum retained message count")
	retainedByteCapacity := flagSet.Int("retained-byte-capacity", 16777216, "maximum retained payload bytes")
	runtimeMetricsOutput := flagSet.String("runtime-metrics-output", "", "JSONL output for Go runtime metrics")
	runtimeMetricsInterval := flagSet.Duration("runtime-metrics-interval", 0, "sampling interval for Go runtime metrics")
	if err := flagSet.Parse(args); err != nil {
		return err
	}
	metricsRequested := *runtimeMetricsOutput != "" || *runtimeMetricsInterval != 0
	if (*runtimeMetricsOutput == "") != (*runtimeMetricsInterval == 0) {
		return errors.New("-runtime-metrics-output and -runtime-metrics-interval must be provided together")
	}
	if metricsRequested && *runtimeMetricsInterval <= 0 {
		return errors.New("-runtime-metrics-interval must be greater than zero")
	}
	if *tcpListen == "" {
		if metricsRequested {
			return errors.New("-tcp-listen is required when runtime metrics collection is enabled")
		}
		logger.Info("MQTT TCP listener disabled", "development_only", true)
		return nil
	}
	if *retainedMessageCapacity <= 0 || *retainedByteCapacity <= 0 {
		return errors.New("-retained-message-capacity and -retained-byte-capacity must be greater than zero")
	}
	if err := validateLoopbackListenAddress(*tcpListen); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *tcpListen)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", *tcpListen, err)
	}
	defer listener.Close()

	routes := routing.NewTable()
	deliveries, err := deliveryqueue.New(*deliveryQueueCapacity)
	if err != nil {
		return fmt.Errorf("create delivery queue: %w", err)
	}
	retainedRepository, err := memoryretained.New(*retainedMessageCapacity, *retainedByteCapacity)
	if err != nil {
		return fmt.Errorf("create retained repository: %w", err)
	}
	sessions := sessionstore.New()
	systemClock := clock.SystemClock{}
	connections, err := connect.New(systemClock, sessions, deliveries, routes)
	if err != nil {
		return fmt.Errorf("create connect use case: %w", err)
	}
	subscriptions, err := subscribe.New(routes, deliveries, retainedRepository)
	if err != nil {
		return fmt.Errorf("create subscribe use case: %w", err)
	}
	publications, err := publish.New(routes, deliveries, retainedRepository)
	if err != nil {
		return fmt.Errorf("create publish use case: %w", err)
	}
	decoder, err := mqtt.NewDecoder(*maxPacketSize)
	if err != nil {
		return fmt.Errorf("create MQTT decoder: %w", err)
	}
	encoder, err := mqtt.NewEncoder(*maxPacketSize)
	if err != nil {
		return fmt.Errorf("create MQTT encoder: %w", err)
	}
	server, err := tcpadapter.NewServer(
		listener,
		tcpadapter.Config{
			MaxPacketSize:        *maxPacketSize,
			ConnectTimeout:       *connectTimeout,
			WriteTimeout:         *writeTimeout,
			ControlQueueCapacity: *controlQueueCapacity,
		},
		decoder,
		encoder,
		connections,
		subscriptions,
		publications,
		deliveries,
		logger,
	)
	if err != nil {
		return fmt.Errorf("create TCP server: %w", err)
	}

	logger.Info("MQTT TCP listener started", "address", listener.Addr().String(), "development_only", true, "qos0_only", true, "retained_supported", true)
	if !metricsRequested {
		if err := server.Serve(ctx); err != nil {
			return fmt.Errorf("serve MQTT TCP listener: %w", err)
		}
		logger.Info("MQTT TCP listener stopped", "address", listener.Addr().String())
		return nil
	}
	sampler, err := runtimemetrics.New(*runtimeMetricsOutput, *runtimeMetricsInterval)
	if err != nil {
		return err
	}
	if err := serveWithRuntimeMetrics(ctx, server, sampler); err != nil {
		return err
	}
	logger.Info("MQTT TCP listener stopped", "address", listener.Addr().String())
	return nil
}

func serveWithRuntimeMetrics(ctx context.Context, server *tcpadapter.Server, sampler *runtimemetrics.Sampler) error {
	serverContext, cancelServer := context.WithCancel(ctx)
	defer cancelServer()
	samplerContext, cancelSampler := context.WithCancel(ctx)
	defer cancelSampler()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(serverContext)
	}()
	samplerDone := make(chan error, 1)
	go func() {
		samplerDone <- sampler.Run(samplerContext)
	}()

	select {
	case samplerErr := <-samplerDone:
		cancelServer()
		serverErr := <-serverDone
		if samplerErr != nil {
			samplerErr = fmt.Errorf("runtime metrics sampler: %w", samplerErr)
		}
		if serverErr != nil {
			serverErr = fmt.Errorf("serve MQTT TCP listener: %w", serverErr)
		}
		if samplerErr != nil && serverErr != nil {
			return errors.Join(samplerErr, serverErr)
		}
		if samplerErr != nil {
			return samplerErr
		}
		return serverErr
	case serverErr := <-serverDone:
		cancelSampler()
		samplerErr := <-samplerDone
		if serverErr != nil {
			serverErr = fmt.Errorf("serve MQTT TCP listener: %w", serverErr)
		}
		if samplerErr != nil {
			samplerErr = fmt.Errorf("runtime metrics sampler: %w", samplerErr)
		}
		if serverErr != nil && samplerErr != nil {
			return errors.Join(serverErr, samplerErr)
		}
		if serverErr != nil {
			return serverErr
		}
		return samplerErr
	}
}

func validateLoopbackListenAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid -tcp-listen %q: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !isLiteralLoopback(host, ip) {
		return errors.New("-tcp-listen must use literal loopback IP 127.0.0.0/8 or ::1")
	}
	return nil
}

func isLiteralLoopback(host string, ip net.IP) bool {
	if strings.Contains(host, ":") {
		return host == "::1"
	}
	ipv4 := ip.To4()
	return ipv4 != nil && ipv4[0] == 127
}
