package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	authenticationadapter "narumq/internal/adapters/authentication"
	"narumq/internal/adapters/clock"
	adaptershttp "narumq/internal/adapters/http/management"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	adaptersmetrics "narumq/internal/adapters/metrics"
	"narumq/internal/adapters/runtimemetrics"
	tcpadapter "narumq/internal/adapters/tcp"
	tlsadapter "narumq/internal/adapters/tls"
	"narumq/internal/application/connect"
	appmgmt "narumq/internal/application/management"
	portsmgmt "narumq/internal/application/ports/management"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"net"
	"net/http"
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
	httpListen := flagSet.String("http-listen", "", "management HTTP listen address")
	httpAuthToken := flagSet.String("http-auth-token", "", "management API Bearer token")
	tlsCert := flagSet.String("tls-cert", "", "server certificate PEM file")
	tlsKey := flagSet.String("tls-key", "", "server private key PEM file")
	tlsCA := flagSet.String("tls-ca", "", "client CA certificate PEM file")
	tlsClientAuth := flagSet.String("tls-client-auth", "none", "client authentication policy (none, request, require)")
	tlsMinVersion := flagSet.String("tls-min-version", "1.2", "minimum TLS version (1.2, 1.3)")
	maxPacketSize := flagSet.Int("max-packet-size", 1048576, "maximum MQTT Remaining Length")
	connectTimeout := flagSet.Duration("connect-timeout", 10*time.Second, "CONNECT handshake timeout")
	writeTimeout := flagSet.Duration("write-timeout", 5*time.Second, "socket write timeout")
	deliveryQueueCapacity := flagSet.Int("delivery-queue-capacity", 64, "per-session delivery queue capacity")
	controlQueueCapacity := flagSet.Int("control-queue-capacity", 16, "per-connection control queue capacity")
	retainedMessageCapacity := flagSet.Int("retained-message-capacity", 1024, "maximum retained message count")
	retainedByteCapacity := flagSet.Int("retained-byte-capacity", 16777216, "maximum retained payload bytes")
	runtimeMetricsOutput := flagSet.String("runtime-metrics-output", "", "JSONL output for Go runtime metrics")
	runtimeMetricsInterval := flagSet.Duration("runtime-metrics-interval", 0, "sampling interval for Go runtime metrics")
	securityConfigFile := flagSet.String("security-config", "", "path to strict JSON security configuration file")
	raftPeerConfigFile := flagSet.String("raft-peer-config", "", "strict JSON config for fixed-voter peer-only mode")
	clusterConfigPath := flagSet.String("cluster-config", "", "strict combined configuration for dynamic cluster mode")
	clusterRestoreArchive := flagSet.String("cluster-restore", "", "verify and seed a cluster backup archive, then exit")
	allowInsecureAnonymousMQTT := flagSet.Bool("allow-insecure-anonymous-mqtt", false, "development-only anonymous MQTT over loopback TCP")
	if err := flagSet.Parse(args); err != nil {
		return err
	}
	peerOnlyRequested := false
	flagSet.Visit(func(parsedFlag *flag.Flag) {
		if parsedFlag.Name == "raft-peer-config" {
			peerOnlyRequested = true
		}
	})
	clusterModeRequested := false
	clusterRestoreRequested := false
	flagSet.Visit(func(parsedFlag *flag.Flag) {
		switch parsedFlag.Name {
		case "cluster-config":
			clusterModeRequested = true
		case "cluster-restore":
			clusterRestoreRequested = true
		}
	})
	if clusterRestoreRequested && !clusterModeRequested {
		return errors.New("-cluster-restore requires -cluster-config")
	}
	if clusterModeRequested {
		if *clusterConfigPath == "" {
			return errors.New("-cluster-config requires a non-empty file path")
		}
		if clusterRestoreRequested && *clusterRestoreArchive == "" {
			return errors.New("-cluster-restore requires a non-empty file path")
		}
		if flagSet.NArg() != 0 {
			return errors.New("-cluster-config does not accept positional arguments")
		}
		var conflictingFlags []string
		flagSet.Visit(func(parsedFlag *flag.Flag) {
			if parsedFlag.Name != "cluster-config" && parsedFlag.Name != "cluster-restore" {
				conflictingFlags = append(conflictingFlags, "-"+parsedFlag.Name)
			}
		})
		if len(conflictingFlags) != 0 {
			return fmt.Errorf("-cluster-config cannot be combined with %s", strings.Join(conflictingFlags, ", "))
		}
		config, err := loadClusterConfigFile(*clusterConfigPath)
		if err != nil {
			return fmt.Errorf("load cluster config: %w", err)
		}
		if clusterRestoreRequested {
			return restoreClusterBackup(config, *clusterRestoreArchive)
		}
		return runClusterMode(ctx, config, logger)
	}
	if peerOnlyRequested {
		if *raftPeerConfigFile == "" {
			return errors.New("-raft-peer-config requires a non-empty file path")
		}
		if flagSet.NArg() != 0 {
			return errors.New("-raft-peer-config does not accept positional arguments")
		}
		var conflictingFlags []string
		flagSet.Visit(func(parsedFlag *flag.Flag) {
			if parsedFlag.Name != "raft-peer-config" {
				conflictingFlags = append(conflictingFlags, "-"+parsedFlag.Name)
			}
		})
		if len(conflictingFlags) != 0 {
			return fmt.Errorf("-raft-peer-config cannot be combined with %s", strings.Join(conflictingFlags, ", "))
		}
		return runRaftPeerOnly(ctx, *raftPeerConfigFile, logger)
	}
	metricsRequested := *runtimeMetricsOutput != "" || *runtimeMetricsInterval != 0
	if (*runtimeMetricsOutput == "") != (*runtimeMetricsInterval == 0) {
		return errors.New("-runtime-metrics-output and -runtime-metrics-interval must be provided together")
	}
	if metricsRequested && *runtimeMetricsInterval <= 0 {
		return errors.New("-runtime-metrics-interval must be greater than zero")
	}
	if *httpListen != "" && *httpAuthToken == "" {
		if err := validateManagementListenAddress(*httpListen); err != nil {
			return fmt.Errorf("unauthenticated management listener: %w", err)
		}
	}
	mqttRequested := *tcpListen != "" || *tlsListen != ""
	if *securityConfigFile != "" && *allowInsecureAnonymousMQTT {
		return errors.New("-security-config and -allow-insecure-anonymous-mqtt cannot be used together")
	}
	if *allowInsecureAnonymousMQTT && (!mqttRequested || *tlsListen != "" || *tcpListen == "") {
		return errors.New("-allow-insecure-anonymous-mqtt requires a loopback-only -tcp-listen and cannot be used with -tls-listen")
	}
	if mqttRequested && *securityConfigFile == "" && !*allowInsecureAnonymousMQTT {
		return errors.New("-security-config is required when an MQTT listener is enabled")
	}
	if *tcpListen == "" && *tlsListen == "" && *httpListen == "" {
		if metricsRequested {
			return errors.New("-tcp-listen, -tls-listen, or -http-listen is required when runtime metrics collection is enabled")
		}
		logger.Info("MQTT TCP and management HTTP listeners disabled", "development_only", true)
		return nil
	}
	if *retainedMessageCapacity <= 0 || *retainedByteCapacity <= 0 {
		return errors.New("-retained-message-capacity and -retained-byte-capacity must be greater than zero")
	}

	systemClock := clock.SystemClock{}
	var (
		securityBundle *SecurityBundle
		jwtProvider    *authenticationadapter.JWKSAuthenticationProvider
		mtlsProvider   *authenticationadapter.MTLSAuthenticationProvider
	)
	if *securityConfigFile != "" {
		bundle, err := loadSecurityConfigFile(*securityConfigFile)
		if err != nil {
			return fmt.Errorf("load security config: %w", err)
		}
		securityBundle = bundle
		if bundle.JWTConfig != nil {
			jwtConfig := *bundle.JWTConfig
			jwtConfig.HTTPClient = &http.Client{Timeout: jwtConfig.HTTPTimeout}
			jwtConfig.Clock = systemClock
			provider, err := authenticationadapter.NewJWKSAuthenticationProvider(jwtConfig)
			if err != nil {
				return fmt.Errorf("create JWKS authentication provider: %w", err)
			}
			jwtProvider = provider
		}
		if len(bundle.MTLSBindings) > 0 {
			provider, err := authenticationadapter.NewMTLSAuthenticationProvider(systemClock, bundle.MTLSBindings)
			if err != nil {
				return fmt.Errorf("create mTLS authentication provider: %w", err)
			}
			mtlsProvider = provider
		}
	}
	if *tlsListen != "" {
		if err := validateTLSAuthenticationBindings(tlsadapter.ClientAuthMode(*tlsClientAuth), securityBundle); err != nil {
			return err
		}
	}
	var (
		listeners          []net.Listener
		tcpListener        net.Listener
		tlsListener        net.Listener
		managementListener net.Listener
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
			CertFile:   *tlsCert,
			KeyFile:    *tlsKey,
			CAFile:     *tlsCA,
			ClientAuth: tlsadapter.ClientAuthMode(*tlsClientAuth),
			MinVersion: *tlsMinVersion,
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

	if *httpListen != "" {
		var errListen error
		managementListener, errListen = net.Listen("tcp", *httpListen)
		if errListen != nil {
			return fmt.Errorf("listen management HTTP on %q: %w", *httpListen, errListen)
		}
	}
	if managementListener != nil {
		defer managementListener.Close()
	}
	routes := routing.NewTable()
	metricsRegistry := adaptersmetrics.NewRegistry()
	deliveries, err := deliveryqueue.New(*deliveryQueueCapacity)
	if err != nil {
		return fmt.Errorf("create delivery queue: %w", err)
	}
	retainedRepository, err := memoryretained.New(*retainedMessageCapacity, *retainedByteCapacity, metricsRegistry)
	if err != nil {
		return fmt.Errorf("create retained repository: %w", err)
	}
	sessions := sessionstore.New()
	systemScheduler := clock.SystemScheduler{}
	willBridge := tcpadapter.NewWillBridge()

	var (
		connectOpts     []connect.Option
		subscribeOpts   []subscribe.Option
		publishOpts     []publish.Option
		tcpConfigPolicy security.Policy
	)
	connectOpts = append(connectOpts, connect.WithMetrics(metricsRegistry))
	subscribeOpts = append(subscribeOpts, subscribe.WithMetrics(metricsRegistry))
	publishOpts = append(publishOpts, publish.WithMetrics(metricsRegistry))
	if securityBundle != nil {
		connectOpts = append(connectOpts, connect.WithSecurity(jwtProvider, mtlsProvider, securityBundle.Policy))
		subscribeOpts = append(subscribeOpts, subscribe.WithPolicy(securityBundle.Policy))
		publishOpts = append(publishOpts, publish.WithPolicy(securityBundle.Policy))
		tcpConfigPolicy = securityBundle.Policy
	}

	connections, err := connect.New(systemClock, sessions, deliveries, routes, systemScheduler, willBridge, connectOpts...)
	if err != nil {
		return fmt.Errorf("create connect use case: %w", err)
	}
	subscriptions, err := subscribe.New(routes, deliveries, retainedRepository, subscribeOpts...)
	if err != nil {
		return fmt.Errorf("create subscribe use case: %w", err)
	}
	publications, err := publish.New(systemClock, routes, deliveries, retainedRepository, publishOpts...)
	if err != nil {
		return fmt.Errorf("create publish use case: %w", err)
	}
	var server *tcpadapter.Server
	if len(listeners) > 0 {
		decoder, err := mqtt.NewDecoder(*maxPacketSize)
		if err != nil {
			return fmt.Errorf("create MQTT decoder: %w", err)
		}
		encoder, err := mqtt.NewEncoder(*maxPacketSize)
		if err != nil {
			return fmt.Errorf("create MQTT encoder: %w", err)
		}
		server, err = tcpadapter.NewServer(
			listeners[0],
			tcpadapter.Config{
				Clock:                      systemClock,
				MaxPacketSize:              *maxPacketSize,
				ConnectTimeout:             *connectTimeout,
				WriteTimeout:               *writeTimeout,
				ControlQueueCapacity:       *controlQueueCapacity,
				ClientCertificateExtractor: tlsadapter.ExtractVerifiedClientCertificateDTO,
				Policy:                     tcpConfigPolicy,
				Metrics:                    metricsRegistry,
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
	}

	managementState := appmgmt.NewService(
		appmgmt.ServiceConfig{
			ClusterID:      "narumq-cluster-1",
			NodeID:         "narumq-node-1",
			ReleaseVersion: "1.0.0",
		},
		sessions,
		routes,
		retainedRepository,
		metricsRegistry,
	)
	var managementServer *http.Server
	if managementListener != nil {
		managementServer = adaptershttp.NewServer(
			adaptershttp.ServerConfig{
				ListenAddr:            managementListener.Addr().String(),
				AuthToken:             *httpAuthToken,
				AllowAnonymousProbes:  true,
				AllowAnonymousMetrics: *httpAuthToken == "",
			},
			managementState,
			managementState,
			metricsRegistry,
		)
	}
	if tcpListener != nil {
		logger.Info("MQTT TCP listener started", "address", tcpListener.Addr().String(), "development_only", true, "qos0_only", true, "retained_supported", true)
	}
	if tlsListener != nil {
		logger.Info("MQTT TLS listener started", "address", tlsListener.Addr().String(), "qos0_only", true, "retained_supported", true)
	}
	if managementListener != nil {
		logger.Info("management HTTP listener started", "address", managementListener.Addr().String())
	}
	managementState.SetNodeState(portsmgmt.NodeStateReady, nil)

	var sampler *runtimemetrics.Sampler
	if metricsRequested {
		sampler, err = runtimemetrics.New(*runtimeMetricsOutput, *runtimeMetricsInterval)
		if err != nil {
			return err
		}
	}
	if err := serveServices(ctx, server, managementServer, managementListener, sampler, managementState); err != nil {
		return err
	}
	if tcpListener != nil {
		logger.Info("MQTT TCP listener stopped", "address", tcpListener.Addr().String())
	}
	if tlsListener != nil {
		logger.Info("MQTT TLS listener stopped", "address", tlsListener.Addr().String())
	}
	if managementListener != nil {
		logger.Info("management HTTP listener stopped", "address", managementListener.Addr().String())
	}
	return nil
}

type serviceResult struct {
	name string
	err  error
}

func serveServices(
	ctx context.Context,
	mqttServer *tcpadapter.Server,
	httpServer *http.Server,
	httpListener net.Listener,
	sampler *runtimemetrics.Sampler,
	state *appmgmt.Service,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan serviceResult, 3)
	running := 0
	if mqttServer != nil {
		running++
		go func() {
			done <- serviceResult{name: "MQTT TCP listener", err: mqttServer.Serve(serveCtx)}
		}()
	}
	if httpServer != nil && httpListener != nil {
		running++
		go func() {
			err := httpServer.Serve(httpListener)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			done <- serviceResult{name: "management HTTP listener", err: err}
		}()
	}
	if sampler != nil {
		running++
		go func() {
			done <- serviceResult{name: "runtime metrics sampler", err: sampler.Run(serveCtx)}
		}()
	}
	if running == 0 {
		return nil
	}

	shutdownHTTP := func() {
		if httpServer == nil {
			return
		}
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}

	shutdownStarted := false
	var serviceErrs []error
	for remaining := running; remaining > 0; {
		var contextDone <-chan struct{}
		if !shutdownStarted {
			contextDone = ctx.Done()
		}
		select {
		case <-contextDone:
			shutdownStarted = true
			if state != nil {
				state.SetNodeState(portsmgmt.NodeStateDraining, nil)
			}
			cancel()
			shutdownHTTP()
		case result := <-done:
			remaining--
			if result.err != nil {
				serviceErrs = append(serviceErrs, fmt.Errorf("%s: %w", result.name, result.err))
				if !shutdownStarted {
					shutdownStarted = true
					if state != nil {
						state.SetNodeState(portsmgmt.NodeStateDraining, nil)
					}
					cancel()
					shutdownHTTP()
				}
			}
		}
	}

	if state != nil {
		state.SetNodeState(portsmgmt.NodeStateSafeToStop, nil)
	}
	if len(serviceErrs) > 0 {
		return errors.Join(serviceErrs...)
	}
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
	return validateLiteralLoopbackAddress("-tcp-listen", address)
}

func validateManagementListenAddress(address string) error {
	return validateLiteralLoopbackAddress("-http-listen", address)
}

func validateTLSAuthenticationBindings(clientAuth tlsadapter.ClientAuthMode, bundle *SecurityBundle) error {
	bindingCount := 0
	if bundle != nil {
		bindingCount = len(bundle.MTLSBindings)
	}
	switch clientAuth {
	case "", tlsadapter.ClientAuthNone:
		if bindingCount > 0 {
			return errors.New("-tls-client-auth none cannot be used with mTLS identity bindings")
		}
	case tlsadapter.ClientAuthRequest:
		if bindingCount == 0 {
			return fmt.Errorf("-tls-client-auth %s requires at least one mTLS identity binding", clientAuth)
		}
		if bundle == nil || bundle.JWTConfig == nil {
			return errors.New("-tls-client-auth request requires JWT configuration when mTLS identities are enabled")
		}
	case tlsadapter.ClientAuthRequire:
		if bindingCount == 0 {
			return fmt.Errorf("-tls-client-auth %s requires at least one mTLS identity binding", clientAuth)
		}
	}
	return nil
}

func validateLiteralLoopbackAddress(flagName, address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid %s %q: %w", flagName, address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !isLiteralLoopback(host, ip) {
		return fmt.Errorf("%s must use literal loopback IP 127.0.0.0/8 or ::1", flagName)
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
