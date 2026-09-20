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
	tlsadapter "narumq/internal/adapters/tls"
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
	tlsListen := flagSet.String("tls-listen", "", "TLS listen address")
	tlsCert := flagSet.String("tls-cert", "", "server certificate PEM file")
	tlsKey := flagSet.String("tls-key", "", "server private key PEM file")
	tlsCA := flagSet.String("tls-ca", "", "client CA certificate PEM file")
	tlsClientAuth := flagSet.String("tls-client-auth", "none", "client authentication policy (none, request, require)")
	tlsMinVersion := flagSet.String("tls-min-version", "1.2", "minimum TLS version (1.2, 1.3)")
	tlsVerifyClientID := flagSet.Bool("tls-verify-client-id", false, "enforce Client ID matches client certificate identity")
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
	if *tcpListen == "" && *tlsListen == "" {
		if metricsRequested {
			return errors.New("-tcp-listen or -tls-listen is required when runtime metrics collection is enabled")
		}
		logger.Info("MQTT TCP listener disabled", "development_only", true)
		return nil
	}
	if *retainedMessageCapacity <= 0 || *retainedByteCapacity <= 0 {
		return errors.New("-retained-message-capacity and -retained-byte-capacity must be greater than zero")
	}

	var (
		listeners   []net.Listener
		tcpListener net.Listener
		tlsListener net.Listener
	)
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()

	if *tcpListen != "" {
		if err := validateLoopbackListenAddress(*tcpListen); err != nil {
			return err
		}
		var err error
		tcpListener, err = net.Listen("tcp", *tcpListen)
		if err != nil {
			return fmt.Errorf("listen on %q: %w", *tcpListen, err)
		}
		listeners = append(listeners, tcpListener)
	}

	if *tlsListen != "" {
		tlsConfig, err := tlsadapter.BuildTLSConfig(tlsadapter.Config{
			CertFile:       *tlsCert,
			KeyFile:        *tlsKey,
			CAFile:         *tlsCA,
			ClientAuth:     tlsadapter.ClientAuthMode(*tlsClientAuth),
			MinVersion:     *tlsMinVersion,
			VerifyClientID: *tlsVerifyClientID,
		})
		if err != nil {
			return fmt.Errorf("configure TLS: %w", err)
		}
		var errListen error
		tlsListener, errListen = tlsadapter.Listen("tcp", *tlsListen, tlsConfig)
		if errListen != nil {
			return fmt.Errorf("listen TLS on %q: %w", *tlsListen, errListen)
		}
		listeners = append(listeners, tlsListener)
	}
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
	systemScheduler := clock.SystemScheduler{}
	willBridge := tcpadapter.NewWillBridge()
	connections, err := connect.New(systemClock, sessions, deliveries, routes, systemScheduler, willBridge)
	if err != nil {
		return fmt.Errorf("create connect use case: %w", err)
	}
	subscriptions, err := subscribe.New(routes, deliveries, retainedRepository)
	if err != nil {
		return fmt.Errorf("create subscribe use case: %w", err)
	}
	publications, err := publish.New(systemClock, routes, deliveries, retainedRepository)
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
		listeners[0],
		tcpadapter.Config{
			Clock:                  systemClock,
			MaxPacketSize:          *maxPacketSize,
			ConnectTimeout:         *connectTimeout,
			WriteTimeout:           *writeTimeout,
			ControlQueueCapacity:   *controlQueueCapacity,
			VerifyClientID:         *tlsVerifyClientID,
			ClientIdentityVerifier: tlsadapter.VerifyConnClientIdentity,
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
	for i := 1; i < len(listeners); i++ {
		if err := server.AddListener(listeners[i]); err != nil {
			return fmt.Errorf("add listener: %w", err)
		}
	}
	willBridge.Bind(server)
	if tcpListener != nil {
		logger.Info("MQTT TCP listener started", "address", tcpListener.Addr().String(), "development_only", true, "qos0_only", true, "retained_supported", true)
	}
	if tlsListener != nil {
		logger.Info("MQTT TLS listener started", "address", tlsListener.Addr().String(), "qos0_only", true, "retained_supported", true)
	}
	stopLogging := func() {
		if tcpListener != nil {
			logger.Info("MQTT TCP listener stopped", "address", tcpListener.Addr().String())
		}
		if tlsListener != nil {
			logger.Info("MQTT TLS listener stopped", "address", tlsListener.Addr().String())
		}
	}
	if !metricsRequested {
		if err := server.Serve(ctx); err != nil {
			return fmt.Errorf("serve MQTT TCP listener: %w", err)
		}
		stopLogging()
		return nil
	}
	sampler, err := runtimemetrics.New(*runtimeMetricsOutput, *runtimeMetricsInterval)
	if err != nil {
		return err
	}
	if err := serveWithRuntimeMetrics(ctx, server, sampler); err != nil {
		return err
	}
	stopLogging()
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
