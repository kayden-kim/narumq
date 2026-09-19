package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
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

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(os.Args[1:], logger); err != nil {
		logger.Error("narumq startup or runtime failure", "error", err.Error())
		os.Exit(1)
	}
}

func run(args []string, logger *slog.Logger) error {
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
	if err := flagSet.Parse(args); err != nil {
		return err
	}
	if *tcpListen == "" {
		logger.Info("MQTT TCP listener disabled", "development_only", true)
		return nil
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
	sessions := sessionstore.New()
	systemClock := clock.SystemClock{}
	connections, err := connect.New(systemClock, sessions, deliveries, routes)
	if err != nil {
		return fmt.Errorf("create connect use case: %w", err)
	}
	subscriptions, err := subscribe.New(routes)
	if err != nil {
		return fmt.Errorf("create subscribe use case: %w", err)
	}
	publications, err := publish.New(routes, deliveries)
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

	logger.Info("MQTT TCP listener started", "address", listener.Addr().String(), "development_only", true, "qos0_only", true)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.Serve(ctx); err != nil {
		return fmt.Errorf("serve MQTT TCP listener: %w", err)
	}
	logger.Info("MQTT TCP listener stopped", "address", listener.Addr().String())
	return nil
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
