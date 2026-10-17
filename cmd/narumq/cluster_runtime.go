package main

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	goruntime "runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"narumq/internal/adapters/auditlog"
	authenticationadapter "narumq/internal/adapters/authentication"
	"narumq/internal/adapters/clock"
	"narumq/internal/adapters/diskspace"
	adaptershttp "narumq/internal/adapters/http/management"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	adaptersmetrics "narumq/internal/adapters/metrics"
	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/adapters/runtimemetrics"
	tcpadapter "narumq/internal/adapters/tcp"
	tlsadapter "narumq/internal/adapters/tls"
	"narumq/internal/application/audit"
	"narumq/internal/application/connect"
	appmgmt "narumq/internal/application/management"
	"narumq/internal/application/membership"
	appobservability "narumq/internal/application/observability"
	authenticationports "narumq/internal/application/ports/authentication"
	managementports "narumq/internal/application/ports/management"
	portsmetrics "narumq/internal/application/ports/metrics"
	observationports "narumq/internal/application/ports/observability"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/nodedrain"
	"narumq/internal/domain/release"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

const (
	clusterReadTimeout                     = 2 * time.Second
	clusterProjectionAdmissionPollInterval = 10 * time.Millisecond
	clusterTickInterval                    = 100 * time.Millisecond
	clusterSnapshotInterval                = 1000
	clusterHeldProposalTimeout             = 10 * time.Second
	clusterDriverStageLimit                = 64
	clusterDefaultControlQueueCapacity     = 256
	clusterDefaultDeliveryQueueCapacity    = 256
	clusterCommitFailureLogInterval        = time.Second
	clusterAuditDenialEventsPerSecond      = 20
	clusterDataPlaneAuditEventsPerSecond   = 20
	clusterDataPlaneAuditQueueCapacity     = 256
)

var (
	errClusterRuntimeNotReady      = errors.New("cluster runtime is not ready")
	errClusterStorageUnhealthy     = errors.New("cluster raft storage is not healthy")
	errClusterApplyDivergence      = errors.New("committed cluster state diverges from membership state")
	errClusterProfileCompatibility = errors.New("retained, offline, durable delivery, node drain, and tenant quota replication require every active member to run a compatible release and schema")
)

// runClusterMode composes the dynamic Raft control plane with MQTT admission and
// management. Raft sees only the opaque bytes produced by the typed state bridge.
func runClusterMode(ctx context.Context, config clusterConfigFile, logger *slog.Logger) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := validateClusterConfig(config); err != nil {
		return fmt.Errorf("validate cluster config: %w", err)
	}

	peerServerTLS, peerClientTLS, localFingerprint, err := buildClusterPeerTLS(config)
	if err != nil {
		return err
	}
	bootstrapIDs, bootstrapMembers, err := clusterBootstrapIdentities(config)
	if err != nil {
		return err
	}
	failpoint, err := loadClusterFailpoint()
	if err != nil {
		return fmt.Errorf("load cluster failpoint: %w", err)
	}
	if failpoint != clusterFailpointNone {
		logger.Warn("cluster failpoint armed", "failpoint", string(failpoint))
	}
	faults := newClusterFaultControl(loadClusterFaultControlPath(), logger)
	if faults.enabled() {
		logger.Warn("cluster fault control armed", "path", faults.path)
	}

	var walBoundaryHook func(raftadapter.WALBoundaryDTO)
	if faults.enabled() || deliveryWALFailpoint(raftadapter.WALBeforeWrite) == failpoint ||
		deliveryWALFailpoint(raftadapter.WALBeforeSync) == failpoint ||
		deliveryWALFailpoint(raftadapter.WALAfterSync) == failpoint {
		walBoundaryHook = func(boundary raftadapter.WALBoundaryDTO) {
			point := deliveryWALFailpoint(boundary.Stage)
			if point == clusterFailpointNone || !containsDeliveryAckCommand(boundary.Entries) {
				return
			}
			if failpoint == point || faults.crash() == point {
				triggerClusterFailpoint(point)
			}
		}
	}

	var persistenceHook func(raftadapter.PersistenceBoundaryDTO)
	if failpoint != clusterFailpointNone {
		persistenceHook = func(boundary raftadapter.PersistenceBoundaryDTO) {
			if failpoint.matchesPersistence(boundary) {
				triggerClusterFailpoint(failpoint)
			}
		}
	}

	var peerListener net.Listener
	var bootstrapAllowed []uint64
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	if config.Role == "bootstrap" {
		empty, err := clusterDirectoryEmpty(config.Directory)
		if err != nil {
			return err
		}
		if empty {
			peerListener, err = net.Listen("tcp", config.ListenAddress)
			if err != nil {
				return fmt.Errorf("listen for cluster peers on %q: %w", config.ListenAddress, err)
			}
			listeners = append(listeners, peerListener)
			local := configuredLocalPeerIdentity(config, localFingerprint)
			peers, err := configuredBootstrapPeerIdentities(config)
			if err != nil {
				return err
			}
			probeCtx, stopProbe := context.WithCancel(ctx)
			serveResult := make(chan error, 1)
			go func() {
				serveResult <- tcpadapter.ServeBootstrapProbe(probeCtx, peerListener, peerServerTLS, local, peers)
			}()
			if failpoint.delaysBootstrapProbe() {
				select {
				case <-time.After(clusterBootstrapProbeFailpointDelay):
				case <-ctx.Done():
					stopProbe()
					<-serveResult
					return ctx.Err()
				}
			}
			bootstrapAllowed, err = runClusterBootstrapProbe(ctx, peers, func(c context.Context, p tcpadapter.PeerIdentityDTO) (tcpadapter.BootstrapProbeResultDTO, error) {
				return tcpadapter.ProbeBootstrapPeer(c, peerClientTLS, local, p)
			}, clusterBootstrapProbeInterval, logger)
			stopProbe()
			if serveErr := <-serveResult; serveErr != nil {
				return serveErr
			}
			if err != nil {
				return err
			}
		}
	}

	var runtime *clusterRuntime

	var observabilityQuery observationports.QueryService
	var peerObservabilityHandler tcpadapter.PeerObservabilityHandler
	metricsRegistry := adaptersmetrics.NewRegistry()
	storageMonitor := newClusterStorageMonitor(time.Now, logger)
	node, err := raftadapter.Open(raftadapter.Config{
		NodeID: raftadapter.NodeID(config.NodeID), Directory: config.Directory,
		DynamicMembership: true, ClusterID: config.ClusterID,
		CertificateSHA256: localFingerprint, BootstrapVoterIDs: bootstrapIDs,
		PersistenceBoundaryHook: persistenceHook, WALBoundaryHook: walBoundaryHook, Logger: logger,
		SnapshotStreamActive: func() bool { return runtime.snapshotStreamActive() },
		ForwardedProposalCapacity: func(peerID raftadapter.NodeID) (int, int) {
			return runtime.forwardedProposalCapacity(peerID)
		},
		WALSyncObserver: func(sync raftadapter.WALSyncDTO) {
			metricsRegistry.ObserveWALSync(sync.Duration, sync.Entries, sync.FrameBytes)
		},
		StorageActivityObserver: func(started bool) {
			if started {
				storageMonitor.begin()
			} else {
				storageMonitor.end()
			}
		},
	})
	if err != nil {
		return fmt.Errorf("open dynamic raft node: %w", err)
	}
	defer node.Close()

	machine, initialMessages, err := restoreClusterState(ctx, node, config, bootstrapMembers)
	if err != nil {
		return err
	}
	if peerListener == nil {
		peerListener, err = net.Listen("tcp", config.ListenAddress)
		if err != nil {
			return fmt.Errorf("listen for cluster peers on %q: %w", config.ListenAddress, err)
		}
		listeners = append(listeners, peerListener)
	}

	retainedRepository, err := memoryretained.New(membership.MaxRetainedMessages, membership.MaxRetainedBytes, metricsRegistry)
	if err != nil {
		return fmt.Errorf("create retained repository: %w", err)
	}
	runtime = &clusterRuntime{
		config: config, logger: logger, node: node, machine: machine,
		retained: retainedRepository,
		storage:  storageMonitor,
		metrics:  metricsRegistry,
		routes:   routing.NewTable(),
		requests: make(chan clusterDriverRequest, 128), initialMessages: initialMessages,
		driverStopped:      make(chan struct{}),
		localIdentity:      configuredLocalPeerIdentity(config, localFingerprint),
		admittedCandidates: make(map[uint64]string),
		failpoint:          failpoint,
		faults:             faults,
		handoffProposed:    make(map[string]uint64),
	}
	metricsRegistry.ObserveSubscriptions(runtime.routes)
	runtime.tenantQuotaController = &clusterTenantQuotaController{runtime: runtime}
	runtime.clusterPolicyController = &clusterPolicyController{runtime: runtime}
	runtime.clusterPolicyController.refreshCommittedState()
	minFree, maxLag := config.publishBackpressure()
	runtime.publishPressure = &clusterPublishPressureController{
		runtime: runtime, minFreeBytes: minFree, maxCommitLag: maxLag, freeBytes: diskspace.FreeBytes,
	}
	runtime.releaseProfile = &clusterReleaseProfileController{runtime: runtime}
	runtime.drain = &clusterDrainController{runtime: runtime}
	runtime.rebalance = &clusterRebalanceController{runtime: runtime}
	runtime.sessionRecovery = &clusterSessionRecoveryController{
		runtime: runtime, keys: make(chan clusterSessionRecoveryKey, clusterSessionRecoveryQueueCapacity),
	}
	runtime.retainedResyncRequired.Store(true)
	if local, found := committedLocalIdentity(machine, config.NodeID, config.ReleaseVersion); found {
		runtime.localIdentity = local
	}
	if machineInitialised(machine) {
		runtime.initialized.Store(true)
	}
	if snapshotData := node.ApplicationSnapshot(); len(snapshotData) != 0 {
		snapshot, decodeErr := decodeMembershipSnapshot(snapshotData)
		if decodeErr != nil {
			return fmt.Errorf("decode recovered application snapshot: %w", decodeErr)
		}
		runtime.lastSnapshotIndex = snapshot.LastIncludedIndex
	}
	// WAL replay can advance committed membership beyond the recovered snapshot; refresh it before peer transport starts.
	// persistTypedSnapshot skips the write when trailing empty Raft entries leave the typed state behind the Raft applied index.
	if runtime.initialized.Load() && runtime.machine.LastAppliedIndex() > runtime.lastSnapshotIndex {
		if err := runtime.persistTypedSnapshot(true); err != nil {
			return fmt.Errorf("persist initialized membership snapshot: %w", err)
		}
	}

	initialPeers, incompatibilityReason := clusterPeerSetForReleaseWindow(
		config.ReleaseVersion, committedPeerIdentities(machine, node.Status(), config.NodeID),
	)
	if incompatibilityReason != "" {
		runtime.releaseProfile.setIncompatible(incompatibilityReason)
	}
	transport, err := tcpadapter.NewDynamicTCPPeerTransport(tcpadapter.DynamicPeerTransportConfig{
		LocalIdentity: runtime.localIdentity, Listener: peerListener,
		ServerTLS: peerServerTLS, ClientTLS: peerClientTLS,
		Peers: initialPeers, AdmissionHandler: runtime.admitPeer,
		SnapshotStreamHandler: runtime.receiveSnapshotStream,
		BootstrapProgress:     runtime.bootstrapProgress, BootstrapAllowedPeerIDs: bootstrapAllowed,
		ObservabilityHandler: func(ctx context.Context, kind tcpadapter.PeerObservabilityKind) ([]byte, error) {
			if peerObservabilityHandler == nil {
				return nil, errors.New("peer observability query service is unavailable")
			}
			return peerObservabilityHandler(ctx, kind)
		},
	})
	if err != nil {
		return fmt.Errorf("create dynamic raft peer transport: %w", err)
	}
	runtime.transport = transport
	runtime.snapshotStreams = newClusterSnapshotStreams(logger, transport, node, runtime.faults.isolates)
	runtime.inboundQueue = transport
	defer transport.Close()
	runtime.forwarder = transport
	runtime.routes.SetOwnerReachability(runtime.ownerReachable)
	runtime.refreshRouteProjection()

	securityBundle, credentialProvider, mtlsProvider, credentialMethod, err := clusterMQTTSecurity(config)
	if err != nil {
		return err
	}
	runtime.securityBundle = securityBundle
	mqttListeners, err := clusterMQTTListeners(config, metricsRegistry, logger)
	if err != nil {
		return err
	}
	listeners = append(listeners, mqttListeners...)
	managementListener, err := net.Listen("tcp", config.Management.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for management HTTP on %q: %w", config.Management.ListenAddress, err)
	}
	listeners = append(listeners, managementListener)

	systemClock := clock.SystemClock{}
	memoryDeliveries, err := deliveryqueue.New(clusterDefaultDeliveryQueueCapacity)
	if err != nil {
		return fmt.Errorf("create delivery queue: %w", err)
	}
	runtime.localDeliveries = memoryDeliveries
	deliveries := &clusterDurableQueue{Queue: runtime.localDeliveries, runtime: runtime, clock: systemClock}
	sessions := sessionstore.New()
	willBridge := tcpadapter.NewWillBridge()
	systemScheduler := clock.SystemScheduler{}
	connectOptions := []connect.Option{
		connect.WithOwnershipCoordinator(runtime, config.NodeID),
		connect.WithSessionMetadataCoordinator(runtime),
		connect.WithWillOutcomeRecorder(metricsRegistry),
		connect.WithOfflineCoordinator(runtime),
		connect.WithDurableDeliveryCommitter(runtime),
	}
	subscribeOptions := []subscribe.Option{
		subscribe.WithCommitter(runtime),
	}
	publishOptions := []publish.Option{
		publish.WithMetrics(metricsRegistry), publish.WithRetainedCommitter(runtime),
		publish.WithOfflineCommitter(runtime), publish.WithRemoteForwarder(runtime),
		publish.WithDurableDeliveryCommitter(runtime),
		publish.WithAdmission(runtime.publishPressure),
	}
	var tcpPolicy security.Policy
	tenantLimits, err := newTenantLimitService(securityBundle, metricsRegistry, logger)
	if err != nil {
		return fmt.Errorf("create tenant limits: %w", err)
	}
	if securityBundle != nil {
		connectOptions = append(connectOptions, connect.WithSecurity(credentialProvider, mtlsProvider, securityBundle.Policy, credentialMethod))
		subscribeOptions = append(subscribeOptions, subscribe.WithPolicy(securityBundle.Policy))
		publishOptions = append(publishOptions, publish.WithPolicy(securityBundle.Policy))
		tcpPolicy = securityBundle.Policy
		if tenantLimits != nil {
			connectOptions = append(connectOptions, connect.WithTenantLimits(tenantLimits, sessions))
			subscribeOptions = append(subscribeOptions, subscribe.WithTenantLimits(tenantLimits))
		}
	}
	auditPublisher, err := auditlog.OpenFileAuditPublisher(auditlog.FileAuditPublisherConfig{
		Directory: config.Management.AuditDirectory, MaxFileBytes: auditlog.DefaultMaxFileBytes,
	})
	if err != nil {
		return fmt.Errorf("configure management audit log: %w", err)
	}
	defer func() {
		if err := auditPublisher.Close(); err != nil {
			logger.Error("close management audit log", "error", err.Error())
		}
	}()
	if err := auditPublisher.Ready(); err != nil {
		logger.Error("audit log is unavailable; management mutations will be rejected and MQTT denial events will not be recorded", "error", err.Error())
	}
	dataPlaneAudit, err := audit.NewDataPlaneRecorder(audit.DataPlaneRecorderConfig{
		ClusterID: config.ClusterID, NodeID: config.NodeID, ReleaseVersion: config.ReleaseVersion,
		EventsPerSecond: clusterDataPlaneAuditEventsPerSecond, QueueCapacity: clusterDataPlaneAuditQueueCapacity,
	}, auditPublisher, systemClock, metricsRegistry)
	if err != nil {
		return fmt.Errorf("create MQTT denial audit recorder: %w", err)
	}
	dataPlaneAudit.Start()
	defer dataPlaneAudit.Close()
	connectOptions = append(connectOptions, connect.WithDataPlaneAudit(dataPlaneAudit))
	connectOptions = append(connectOptions, connect.WithMetrics(metricsRegistry))
	connectOptions = append(connectOptions, connect.WithCredentialRevocationChecker(runtime))
	connectOptions = append(connectOptions, connect.WithBlocklistMatcher(runtime))
	connections, err := connect.New(systemClock, sessions, deliveries, runtime.routes, systemScheduler, willBridge, connectOptions...)
	if err != nil {
		return fmt.Errorf("create cluster connect use case: %w", err)
	}
	runtime.connectUseCase = connections
	defer connections.Close()
	subscriptions, err := subscribe.New(runtime.routes, deliveries, retainedRepository, subscribeOptions...)
	if err != nil {
		return fmt.Errorf("create cluster subscribe use case: %w", err)
	}
	publications, err := publish.New(systemClock, runtime.routes, deliveries, retainedRepository, publishOptions...)
	if err != nil {
		return fmt.Errorf("create cluster publish use case: %w", err)
	}

	var mqttServer *tcpadapter.Server
	if len(mqttListeners) != 0 {
		committed, found := runtime.machine.ClusterPolicy()
		var committedPolicy *membership.ClusterPolicyStateDTO
		if found {
			committedPolicy = &committed
		}
		featureActive := slices.Contains(runtime.machine.ActiveFeatures(), membership.ClusterPolicyFeature)
		limits, _, _ := effectiveClusterMQTTLimits(config, committedPolicy, featureActive)
		connectionLimits := tcpConnectionLimits(limits)
		decoder, err := mqtt.NewDecoder(connectionLimits.MaxPacketSize)
		if err != nil {
			return fmt.Errorf("create cluster MQTT decoder: %w", err)
		}
		encoder, err := mqtt.NewEncoder(mqtt.MaxRemainingLength)
		if err != nil {
			return fmt.Errorf("create cluster MQTT encoder: %w", err)
		}
		mqttServer, err = tcpadapter.NewServer(mqttListeners[0], tcpadapter.Config{
			Clock: systemClock, MaxPacketSize: connectionLimits.MaxPacketSize,
			ConnectTimeout: connectionLimits.ConnectTimeout, WriteTimeout: connectionLimits.WriteTimeout,
			ControlQueueCapacity: clusterDefaultControlQueueCapacity, ClientCertificateExtractor: tlsadapter.ExtractVerifiedClientCertificateDTO,
			CredentialRevocationChecker: runtime, TimerScheduler: systemScheduler,
			ReceiveMaximum: connectionLimits.ReceiveMaximum, PacketReadTimeout: connectionLimits.PacketReadTimeout,
			Policy: tcpPolicy, Metrics: metricsRegistry, ClusterMode: true,
			TenantLimits:   tenantLimits,
			DataPlaneAudit: dataPlaneAudit,
			LocalMemberID:  config.NodeID, ClusterReady: runtime.clusterReady,
			ClusterAdmissionOpen:   runtime.clusterAdmissionOpen,
			ClusterProjectionReady: runtime.clusterProjectionReady,
		}, decoder, encoder, connections, subscriptions, publications, deliveries, logger)
		if err != nil {
			return fmt.Errorf("create cluster MQTT server: %w", err)
		}
		runtime.mqttServer = mqttServer
		mqttServer.ReplaceCredentialRevocations(runtime.machine.CredentialRevocations())
		mqttServer.ReplaceBlocklist(runtime.machine.BlocklistEntries())
		runtime.durableSink = mqttServer
		runtime.forwardSink = mqttServer
		for _, listener := range mqttListeners[1:] {
			if err := mqttServer.AddListener(listener); err != nil {
				return fmt.Errorf("add cluster MQTT listener: %w", err)
			}
		}
		willBridge.Bind(mqttServer)
	}

	membershipService, err := membership.NewService(runtime, config.replicationProfile())
	if err != nil {
		return fmt.Errorf("create membership service: %w", err)
	}

	clusterPolicyService, err := membership.NewClusterPolicyService(runtime)
	if err != nil {
		return fmt.Errorf("create cluster policy service: %w", err)
	}
	credentialRevocationService, err := membership.NewCredentialRevocationService(runtime, systemClock)
	if err != nil {
		return fmt.Errorf("create credential revocation service: %w", err)
	}
	blocklistService, err := membership.NewBlocklistService(runtime, systemClock)
	if err != nil {
		return fmt.Errorf("create client blocklist service: %w", err)
	}
	drainService, err := membership.NewDrainService(runtime, systemClock)
	if err != nil {
		return fmt.Errorf("create node drain service: %w", err)
	}
	rebalanceService, err := membership.NewRebalanceService(runtime, systemClock)
	if err != nil {
		return fmt.Errorf("create connection rebalance service: %w", err)
	}
	auditService, err := audit.NewService(audit.ServiceConfig{
		ClusterID: config.ClusterID, NodeID: config.NodeID, ReleaseVersion: config.ReleaseVersion,
		DenialEventsPerSecond: clusterAuditDenialEventsPerSecond,
	}, auditPublisher, systemClock, metricsRegistry)
	if err != nil {
		return fmt.Errorf("create management audit service: %w", err)
	}
	baseManagement := appmgmt.NewService(appmgmt.ServiceConfig{
		ClusterID: config.ClusterID, NodeID: strconv.FormatUint(config.NodeID, 10), ReleaseVersion: config.ReleaseVersion,
	}, sessions, runtime.routes, retainedRepository, metricsRegistry)
	baseManagement.UseClusterSessionSource(runtime)
	runtime.managementState = baseManagement
	observabilitySource := &observabilitySource{
		runtime: runtime, state: baseManagement, metrics: metricsRegistry, clock: systemClock,
		clusterID: config.ClusterID, localNodeID: strconv.FormatUint(config.NodeID, 10),
		releaseVersion: config.ReleaseVersion,
	}
	observabilityQuery, err = appobservability.NewService(
		observabilitySource, observabilitySource, tcpadapter.NewPeerObservabilityReader(transport), systemClock,
	)
	if err != nil {
		return fmt.Errorf("create observability query service: %w", err)
	}
	peerObservabilityHandler = tcpadapter.NewPeerObservabilityHandler(observabilityQuery, observabilitySource.localNodeID)
	stateProvider := &clusterStateProvider{StateProvider: baseManagement, runtime: runtime}
	healthChecker := &clusterHealthChecker{HealthChecker: baseManagement, runtime: runtime}
	credentials, err := loadClusterManagementCredentials(config.Management)
	if err != nil {
		return err
	}
	var oidcConfig *adaptershttp.OIDCConfig
	if credentials.OIDCVerifier != nil {
		verifierConfig := *credentials.OIDCVerifier
		verifierConfig.HTTPClient = &http.Client{Timeout: verifierConfig.HTTPTimeout}
		verifierConfig.Clock = systemClock
		verifier, err := authenticationadapter.NewJWTVerifier(verifierConfig)
		if err != nil {
			return fmt.Errorf("create management OIDC verifier: %w", err)
		}
		oidcConfig = &adaptershttp.OIDCConfig{Verifier: verifier, Identities: credentials.OIDCIdentities}
	}
	auditQueryService, err := audit.NewQueryService(auditPublisher)
	if err != nil {
		return fmt.Errorf("create management audit query service: %w", err)
	}
	requestRate := 0
	if config.Management.RequestRate != nil {
		requestRate = *config.Management.RequestRate
	}
	authFailureRate := 0
	if config.Management.AuthFailureRate != nil {
		authFailureRate = *config.Management.AuthFailureRate
	}
	managementServer := adaptershttp.NewServerWithMembership(adaptershttp.ServerConfig{
		ListenAddr: managementListener.Addr().String(), AuthToken: credentials.AdminToken,
		APIKeys: credentials.APIKeys, OIDC: oidcConfig,
		AllowAnonymousProbes: true,
		RequestRate:          requestRate, AuthFailureRate: authFailureRate, Clock: systemClock,
		ClusterID: config.ClusterID, LocalNodeID: config.NodeID, Logger: logger,
		Observability: observabilityQuery,
	}, stateProvider, healthChecker, metricsRegistry, membershipService, drainService, rebalanceService, runtime, auditService, auditQueryService, credentialRevocationService, blocklistService, clusterPolicyService, runtime)
	var sampler *runtimemetrics.Sampler
	if config.RuntimeMetrics != nil {
		createdSampler, err := runtimemetrics.New(config.RuntimeMetrics.OutputFile, config.runtimeMetricsInterval())
		if err != nil {
			return err
		}
		sampler = createdSampler
	}
	return runtime.serve(ctx, mqttServer, managementServer, managementListener, sampler)
}

type clusterCommitFailureKey struct {
	stage       string
	class       string
	commandKind string
}

type clusterCommitFailureState struct {
	firstAt       time.Time
	lastAt        time.Time
	lastLoggedAt  time.Time
	count         uint64
	reportedCount uint64
}

type clusterCommitFailureSummary struct {
	firstAt      time.Time
	lastAt       time.Time
	totalCount   uint64
	sinceLastLog uint64
}

type clusterInboundRaftQueue interface {
	TakeInbound() (tcpadapter.PeerInboundDelivery, bool)
}

type clusterRuntime struct {
	config                  clusterConfigFile
	securityBundle          *SecurityBundle
	logger                  *slog.Logger
	node                    *raftadapter.Node
	transport               *tcpadapter.DynamicTCPPeerTransport
	snapshotStreams         *clusterSnapshotStreams
	inboundQueue            clusterInboundRaftQueue
	forwarder               clusterPeerForwarder
	forwardSink             clusterForwardSink
	machine                 *membership.StateMachine
	retained                *memoryretained.Repository
	routes                  *routing.Table
	mqttServer              *tcpadapter.Server
	localDeliveries         *deliveryqueue.Queue
	durableSink             clusterDurableSink
	durableMu               sync.Mutex
	durableAttachments      map[routing.SessionID]clusterDurableAttachment
	storage                 *clusterStorageMonitor
	drain                   *clusterDrainController
	rebalance               *clusterRebalanceController
	tenantQuotaController   *clusterTenantQuotaController
	clusterPolicyController *clusterPolicyController
	publishPressure         *clusterPublishPressureController
	releaseProfile          *clusterReleaseProfileController
	sessionRecovery         *clusterSessionRecoveryController
	tenantQuotaRejectionLog tenantQuotaRejectionReporter
	connectUseCase          *connect.UseCase
	metrics                 *adaptersmetrics.Registry
	localCommits            atomic.Int64
	commitFailureMu         sync.Mutex
	commitFailureLogs       map[clusterCommitFailureKey]clusterCommitFailureState

	requests                    chan clusterDriverRequest
	projectionRequests          []clusterDriverRequest
	backupRequests              []clusterDriverRequest
	pendingCommits              []*clusterPendingCommit
	deferredRequest             clusterDriverRequest
	hasDeferredRequest          bool
	readIndexStaged             int
	forwardedProposalIsolations []uint64 // Driver-owned fault snapshot for the callback under Node's lock.
	driverStopped               chan struct{}
	driverStopOnce              sync.Once
	sequence                    atomic.Uint64
	initialized                 atomic.Bool
	projectionMu                sync.Mutex
	projectionAdmissionSequence atomic.Uint64
	projectionIndex             atomic.Uint64
	projectionFailed            atomic.Bool
	driverFailed                atomic.Bool
	admissionBootstrapped       atomic.Bool
	admissionDynamicMembership  atomic.Bool
	admissionLocalVoter         atomic.Bool
	admissionLocalPhaseVoter    atomic.Bool
	admissionAppliedIndex       atomic.Uint64
	retainedResyncRequired      atomic.Bool
	lastSnapshotIndex           uint64
	snapshotPending             bool
	initialMessages             []raftadapter.MessageDTO
	autoCatchupPending          string
	leadershipTransferTo        raftadapter.NodeID
	leadershipTransferTerm      uint64
	leadershipTransferTicks     int
	recoveryProofKey            string
	recoveryProofWait           bool
	recoveryProofReady          bool
	recoveryLeadership          raftLeadership

	localMu            sync.Mutex
	localIdentity      tcpadapter.PeerIdentityDTO
	admissionMu        sync.Mutex
	admittedCandidates map[uint64]string
	seedPeerMu         sync.RWMutex
	seedPeers          []tcpadapter.PeerIdentityDTO
	managementState    *appmgmt.Service
	failpoint          clusterFailpoint
	faults             *clusterFaultControl
	removalBlockLogged string
	handoffProposed    map[string]uint64
}

type clusterDriverRequestKind uint8

const (
	clusterDriverRead clusterDriverRequestKind = iota + 1
	clusterDriverProposal
	clusterDriverCancelRead
	clusterDriverBackup
	clusterDriverProjection
	clusterDriverCommit
)

type clusterDriverRequest struct {
	kind         clusterDriverRequestKind
	queuedAt     time.Time
	contextKey   string
	context      context.Context
	data         []byte
	readResult   chan clusterReadResponse
	applyResult  chan clusterApplyResponse
	backupResult chan clusterBackupImageResponse
	pending      *clusterPendingCommit
}

type clusterReadResponse struct {
	index uint64
	err   error
}

type clusterReadRequest struct {
	runtime *clusterRuntime
	context context.Context
	key     string
	result  chan clusterReadResponse
}

type clusterApplyResponse struct {
	result membership.ApplyResultDTO
	err    error
}

type clusterBackupImageResponse struct {
	image            raftadapter.BackupImageDTO
	boundarySnapshot []byte
	err              error
}

type clusterReadWaiter struct {
	ctx           context.Context
	startedAt     time.Time
	result        chan clusterReadResponse
	commit        *clusterPendingCommit
	leadership    raftLeadership
	proofSequence uint64
	issuedAt      time.Time // Emission time of proofSequence; the zero value never expires.
	answered      bool      // The ReadState arrived and waits for local apply and projection.
}

type clusterProposalWaiter struct {
	context      context.Context
	requestID    string
	proofIndex   uint64
	leadership   raftLeadership
	command      membership.CommandDTO
	data         []byte
	startedAt    time.Time
	result       chan clusterApplyResponse
	response     clusterApplyResponse
	applied      bool
	backlogEntry *list.Element
}

type clusterApplyEvent struct {
	index         uint64
	command       []byte
	configuration *raftadapter.CommittedConfigurationDTO
}

func (runtime *clusterRuntime) serve(parent context.Context, mqttServer *tcpadapter.Server, managementServer *http.Server, managementListener net.Listener, sampler *runtimemetrics.Sampler) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	driverDone := make(chan error, 1)
	go func() { driverDone <- runtime.runDriver(ctx) }()
	runtime.tenantQuotaController.start(ctx)
	runtime.clusterPolicyController.start(ctx)
	runtime.publishPressure.start(ctx)
	runtime.releaseProfile.start(ctx)
	runtime.drain.start(ctx)
	runtime.rebalance.start(ctx)
	runtime.sessionRecovery.start(ctx)

	var admissionDone chan error
	if runtime.config.Role == "joiner" {
		admissionDone = make(chan error, 1)
		go func() { admissionDone <- runtime.runSeedAdmission(ctx) }()
	}

	serviceDone := make(chan serviceResult, 4)
	remaining := 1
	go func() {
		serviceDone <- serviceResult{name: "cluster delivery forwarding receiver", err: runtime.runForwardReceiver(ctx)}
	}()
	if mqttServer != nil {
		remaining++
		go func() { serviceDone <- serviceResult{name: "cluster MQTT listener", err: mqttServer.Serve(ctx)} }()
	}
	go func() {
		err := managementServer.Serve(managementListener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serviceDone <- serviceResult{name: "cluster management HTTP listener", err: err}
	}()
	remaining++
	if sampler != nil {
		remaining++
		go func() {
			serviceDone <- serviceResult{name: "cluster runtime metrics sampler", err: sampler.Run(ctx)}
		}()
	}

	var results []error
	parentDone := parent.Done()
	shutdown := false
	for driverDone != nil || remaining > 0 || admissionDone != nil {
		select {
		case <-parentDone:
			parentDone = nil
			if !shutdown {
				shutdown = true
				runtime.managementState.SetNodeState(managementports.NodeStateDraining, nil)
				cancel()
				runtime.shutdownHTTP(managementServer)
			}
		case err := <-driverDone:
			driverDone = nil
			keepServing := !shutdown && runtime.handleDriverFailure(err)
			if runtime.transport != nil {
				_ = runtime.transport.Close()
			}
			if keepServing {
				results = append(results, fmt.Errorf("cluster raft driver: %w", err))
				continue
			}
			if !shutdown {
				if err == nil {
					err = errors.New("driver stopped unexpectedly")
				}
				results = append(results, fmt.Errorf("cluster raft driver: %w", err))
				shutdown = true
				runtime.managementState.SetNodeState(managementports.NodeStateDraining, nil)
				cancel()
				runtime.shutdownHTTP(managementServer)
			}
		case result := <-serviceDone:
			remaining--
			if result.name == "cluster delivery forwarding receiver" && result.err == nil &&
				runtime.storage != nil && runtime.storage.view().health == clusterReplicationFailed {
				continue
			}
			if result.err != nil && !shutdown {
				results = append(results, fmt.Errorf("%s: %w", result.name, result.err))
				shutdown = true
				runtime.managementState.SetNodeState(managementports.NodeStateDraining, nil)
				cancel()
				runtime.shutdownHTTP(managementServer)
			} else if result.err == nil && !shutdown {
				results = append(results, fmt.Errorf("%s stopped unexpectedly", result.name))
				shutdown = true
				cancel()
				runtime.shutdownHTTP(managementServer)
			}
		case err := <-admissionDone:
			admissionDone = nil
			if err != nil && !shutdown {
				results = append(results, fmt.Errorf("seed admission: %w", err))
				shutdown = true
				cancel()
				runtime.shutdownHTTP(managementServer)
			}
		}
	}
	runtime.drain.stopAndWait()
	runtime.rebalance.stopAndWait()
	runtime.sessionRecovery.stopAndWait()
	runtime.tenantQuotaController.stopAndWait()
	runtime.clusterPolicyController.stopAndWait()
	runtime.publishPressure.stopAndWait()
	runtime.releaseProfile.stopAndWait()
	runtime.managementState.SetNodeState(managementports.NodeStateSafeToStop, nil)
	return errors.Join(results...)
}

func (runtime *clusterRuntime) shutdownHTTP(server *http.Server) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func (runtime *clusterRuntime) runSeedAdmission(ctx context.Context) error {
	for ctx.Err() == nil {
		if identity, found := committedLocalIdentity(runtime.machine, runtime.config.NodeID, runtime.config.ReleaseVersion); found {
			switch identity.Phase {
			case tcpadapter.PeerPhaseLearner, tcpadapter.PeerPhaseCatchingUp, tcpadapter.PeerPhaseVoter:
				return nil
			}
		}
		var lastErr error
		var lastFailedSeed string
		admitted := false
		for _, seed := range runtime.config.SeedAddresses {
			seedHost, _, err := net.SplitHostPort(seed)
			if err != nil {
				continue
			}
			voters, err := runtime.transport.RequestSeedAdmission(ctx, seed, seedHost)
			if err == nil {
				runtime.seedPeerMu.Lock()
				runtime.seedPeers = append(runtime.seedPeers[:0], voters...)
				runtime.seedPeerMu.Unlock()
				local := runtime.transport.LocalIdentity()
				if local.Phase == tcpadapter.PeerPhaseLearner || local.Phase == tcpadapter.PeerPhaseCatchingUp || local.Phase == tcpadapter.PeerPhaseVoter {
					return nil
				}
				admitted = true
				break
			}
			lastErr = err
			lastFailedSeed = seed
		}
		if !admitted && lastErr != nil && ctx.Err() == nil {
			runtime.logger.Warn("dynamic peer seed admission failed",
				"seed_address", lastFailedSeed, "error", lastErr)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}

func (runtime *clusterRuntime) runDriver(ctx context.Context) (driverErr error) {
	serveResult := make(chan error, 1)
	go func() { serveResult <- runtime.transport.Serve(ctx) }()
	ticker := time.NewTicker(clusterTickInterval)
	defer ticker.Stop()
	// Snapshot stream transfers end with the driver, whatever the reason.
	streamContext, stopSnapshotStreams := context.WithCancel(ctx)
	defer func() {
		stopSnapshotStreams()
		runtime.snapshotStreams.wait()
	}()
	reads := make(map[string]clusterReadWaiter)
	var proposals []clusterProposalWaiter
	deferredReads := make([]raftadapter.ReadStateDTO, 0)
	defer func() {
		runtime.driverFailed.Store(true)
		runtime.driverStopOnce.Do(func() {
			if runtime.driverStopped != nil {
				close(runtime.driverStopped)
			}
		})
		exitErr := driverErr
		if exitErr == nil {
			exitErr = errors.New("cluster raft driver stopped before pending work completed")
		}
		for key, waiter := range reads {
			if waiter.result != nil {
				runtime.observeRaftStage(adaptersmetrics.RaftReadIndex, waiter.startedAt)
				waiter.result <- clusterReadResponse{err: exitErr}
			}
			delete(reads, key)
		}
		runtime.failPendingProposals(&proposals, exitErr)
		runtime.failQueuedDriverRequests(exitErr)
	}()
	lastLeadership := raftLeadership{}
	membershipTickPending := false
	tickDriver := func() error {
		status := runtime.node.Status()
		if status.Role == raftadapter.RoleLeader {
			for _, peerID := range status.LearnerIDs {
				if peerStatus, err := runtime.transport.Status(uint64(peerID)); err == nil && !peerStatus.Connected {
					_ = runtime.node.ReportPeerMessageFailure(peerID, true)
				}
			}
		}
		if err := runtime.node.Tick(); err != nil {
			return fmt.Errorf("tick dynamic raft node: %w", err)
		}
		if runtime.projectionFailed.Load() {
			runtime.refreshRouteProjection()
		}
		for key, waiter := range reads {
			if waiter.ctx != nil && waiter.ctx.Err() != nil {
				delete(reads, key)
				if waiter.commit != nil {
					runtime.finishCommitRead(waiter.commit, waiter.ctx.Err())
				} else if waiter.result != nil {
					runtime.observeRaftStage(adaptersmetrics.RaftReadIndex, waiter.startedAt)
					waiter.result <- clusterReadResponse{err: waiter.ctx.Err()}
				}
			}
		}
		membershipTickPending = true
		return nil
	}
	maintainMembership := func() (bool, error) {
		if !membershipTickPending {
			return false, nil
		}
		status := runtime.node.Status()
		if status.AppliedIndex < status.CommitIndex || runtime.node.StoragePending() {
			return false, nil
		}
		leadership := raftLeadership{leader: status.LeaderID, term: status.Term, role: status.Role}
		if leadership != lastLeadership {
			lastLeadership = leadership
			runtime.autoCatchupPending = ""
			clear(runtime.handoffProposed)
			runtime.removalBlockLogged = ""
			// Command recovery is observed on every Ready cycle, not only maintenance ticks.
		}
		if err := runtime.advanceMembership(); err != nil {
			return false, err
		}
		if err := runtime.advanceDrainLeadership(); err != nil {
			return false, err
		}
		membershipTickPending = false
		return true, nil
	}

	processReady := func() (readyErr error) {
		// The even/odd sequence brackets Ready processing without blocking admission's storage-health checks.
		runtime.projectionAdmissionSequence.Add(1)
		defer func() { runtime.finishProjectionAdmissionSequence(readyErr) }()
		for {
			select {
			case <-ctx.Done():
				return nil
			case err := <-serveResult:
				if ctx.Err() != nil {
					return nil
				}
				if err == nil {
					return errors.New("dynamic peer transport stopped unexpectedly")
				}
				return fmt.Errorf("serve dynamic peer transport: %w", err)
			case <-ticker.C:
				if err := tickDriver(); err != nil {
					return err
				}
			default:
			}
			runtime.refreshReadWaiters(reads)
			runtime.observeCommitLeadership()
			runtime.reclaimUnsentProposals()
			// Read injected fault files before Node owns its mutex.
			if runtime.faults.enabled() {
				control, _, _ := runtime.faults.read()
				runtime.forwardedProposalIsolations = control.IsolatePeers
			}
			readyStarted := time.Now()
			batch, err := runtime.node.ProcessReady()
			runtime.observeRaftStage(adaptersmetrics.RaftReady, readyStarted)
			if err != nil {
				return fmt.Errorf("process raft Ready: %w", err)
			}
			if clusterBatchEmpty(batch) {
				runtime.readIndexStaged = 0
				status := runtime.node.Status()
				projectionReady := !runtime.projectionFailed.Load()
				if projectionReady {
					runtime.projectionIndex.Store(status.AppliedIndex)
				}
				runtime.publishProjectionAdmissionStatus(status)
				if projectionReady {
					// Applied proposals do not wait for unrelated later Ready work.
					runtime.resolveAppliedProposals(&proposals)
				}
				deferredReads = runtime.completeReadStates(deferredReads, reads)
				runtime.resolveProjectionRequests(nil, status.AppliedIndex >= status.CommitIndex)
				recovered, err := runtime.recoverProposals(ctx, reads, &proposals)
				if err != nil {
					return err
				}
				runtime.resolveBackupRequests(nil)
				if runtime.node.StoragePending() {
					return nil // Resume on storage completion without blocking peer/tick work.
				}
				if err := runtime.persistTypedSnapshot(false); err != nil {
					return err
				}
				maintained, err := maintainMembership()
				if err != nil {
					return err
				}
				proposed := runtime.resolveCommitReads(reads, &proposals)
				if maintained || proposed || recovered || runtime.hasDeferredRequest {
					beforeReads, beforeProposals := len(reads), len(proposals)
					if err := runtime.stageQueuedDriverWork(reads, &proposals, ctx.Done()); err != nil {
						return err
					}
					if !maintained && !proposed && len(reads) == beforeReads && len(proposals) == beforeProposals {
						return nil
					}
					continue
				}
				return nil
			}
			applyStarted := time.Now()
			err = runtime.applyBatch(batch, &proposals)
			runtime.observeRaftStage(adaptersmetrics.RaftApply, applyStarted)
			if err != nil {
				return err
			}
			if err := runtime.node.CompleteApply(batch.ApplySequence); err != nil {
				return fmt.Errorf("complete application apply: %w", err)
			}
			if len(batch.Commands) != 0 || len(batch.ConfigurationChanges) != 0 {
				if err := runtime.maybeSnapshot(len(batch.ConfigurationChanges) != 0); err != nil {
					return err
				}
			}
			deferredReads = append(deferredReads, batch.ReadStates...)
			sendStarted := time.Now()
			err = runtime.sendMessages(ctx, batch.Messages)
			runtime.observeRaftStage(adaptersmetrics.RaftSend, sendStarted)
			if err != nil {
				return err
			}
			runtime.snapshotStreams.start(streamContext, batch.SnapshotStreams)
			status := runtime.node.Status()
			runtime.publishProjectionAdmissionStatus(status)
			deferredReads = runtime.completeReadStates(deferredReads, reads)
			if !runtime.projectionFailed.Load() {
				runtime.resolveAppliedProposals(&proposals)
			}
			runtime.resolveProjectionRequests(nil, status.AppliedIndex >= status.CommitIndex)
			runtime.resolveBackupRequests(nil)
			runtime.resolveCommitReads(reads, &proposals)
			if _, err := maintainMembership(); err != nil {
				return err
			}
			// Give independent socket writers a turn before generating more Ready messages.
			goruntime.Gosched()
			// Disk I/O continues independently while bounded driver work is staged.
			if err := runtime.stageQueuedDriverWork(reads, &proposals, ctx.Done()); err != nil {
				return err
			}
		}
	}
	if err := runtime.sendMessages(ctx, runtime.initialMessages); err != nil {
		return err
	}
	runtime.initialMessages = nil
	if err := processReady(); err != nil {
		return err
	}
	for {
		requests := runtime.driverRequestChannel()
		select {
		case <-ctx.Done():
			return nil
		case err := <-serveResult:
			if ctx.Err() != nil {
				return nil
			}
			if err == nil {
				return errors.New("dynamic peer transport stopped unexpectedly")
			}
			return fmt.Errorf("serve dynamic peer transport: %w", err)
		case <-runtime.deferredRequestDone():
			if err := runtime.stageQueuedDriverWork(reads, &proposals, ctx.Done()); err != nil {
				return err
			}
			if err := processReady(); err != nil {
				return err
			}
		case <-runtime.transport.OutboundReady():
			if !runtime.hasDeferredRequest {
				continue
			}
			if err := runtime.stageQueuedDriverWork(reads, &proposals, ctx.Done()); err != nil {
				return err
			}
			if err := processReady(); err != nil {
				return err
			}
		case <-runtime.node.StorageReady():
			if err := processReady(); err != nil {
				return err
			}
		case <-runtime.snapshotStreams.wakeChannel():
			if err := processReady(); err != nil {
				return err
			}
		case request := <-requests:
			runtime.tryDriverRequest(request, reads, &proposals)
			if err := runtime.stageQueuedDriverWork(reads, &proposals, ctx.Done()); err != nil {
				return err
			}
			if err := processReady(); err != nil {
				return err
			}
		case _, ok := <-runtime.transport.InboundReady():
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return errors.New("dynamic peer transport inbound-ready channel closed unexpectedly")
			}
			delivery, received := runtime.inboundQueue.TakeInbound()
			if !received {
				continue
			}
			_, err := runtime.stepInboundDelivery(delivery)
			if err != nil {
				return err
			}
			if err := runtime.stageQueuedDriverWork(reads, &proposals, ctx.Done()); err != nil {
				return err
			}
			if err := processReady(); err != nil {
				return err
			}
		case <-ticker.C:
			if err := tickDriver(); err != nil {
				return err
			}
			if err := processReady(); err != nil {
				return err
			}
		}
	}
}

func (runtime *clusterRuntime) handleDriverRequest(
	request clusterDriverRequest,
	reads map[string]clusterReadWaiter,
	proposals *[]clusterProposalWaiter,
) {
	switch request.kind {
	case clusterDriverProjection:
		runtime.projectionRequests = append(runtime.projectionRequests, request)
	case clusterDriverRead:
		if request.context != nil && request.context.Err() != nil {
			request.readResult <- clusterReadResponse{err: request.context.Err()}
			return
		}
		startedAt := time.Now()
		waiter := clusterReadWaiter{ctx: request.context, result: request.readResult, startedAt: startedAt}
		if err := runtime.issueReadProof(request.contextKey, &waiter, runtime.node.Status()); err != nil {
			runtime.observeRaftStage(adaptersmetrics.RaftReadIndex, startedAt)
			request.readResult <- clusterReadResponse{err: err}
			return
		}
		reads[request.contextKey] = waiter
	case clusterDriverCommit:
		pending := request.pending
		runtime.observeRaftStage(adaptersmetrics.RaftCommitQueue, request.queuedAt)
		pending.readStartedAt = time.Now()
		runtime.pendingCommits = append(runtime.pendingCommits, pending)
		if err := pending.readContext.Err(); err != nil {
			runtime.finishCommitRead(pending, err)
			return
		}
		waiter := clusterReadWaiter{ctx: pending.readContext, commit: pending}
		if err := runtime.issueReadProof(pending.contextKey, &waiter, runtime.node.Status()); err != nil {
			runtime.finishCommitRead(pending, err)
			return
		}
		reads[pending.contextKey] = waiter
	case clusterDriverProposal:
		startedAt := time.Now()
		var backlogEntry *list.Element
		if request.pending != nil {
			backlogEntry = request.pending.backlogEntry
		}
		if err := runtime.node.ProposeForwarded(request.data); err != nil {
			runtime.observeRaftStage(adaptersmetrics.RaftProposal, startedAt)
			runtime.removeCommitBacklog(backlogEntry)
			request.applyResult <- clusterApplyResponse{err: err}
			return
		}
		if backlogEntry != nil {
			runtime.observeCommitLeadership()
			runtime.markCommitProposed(backlogEntry)
		}
		var reqID string
		var proofIdx uint64
		var reqCmd membership.CommandDTO
		if request.pending != nil {
			reqID = request.pending.requestID
			proofIdx = request.pending.readProofIndex
			reqCmd = request.pending.command
		} else if cmd, err := decodeMembershipCommand(request.data); err == nil {
			reqCmd = cmd
			if cmd.Proposal != nil {
				reqID = cmd.Proposal.RequestID
				proofIdx = cmd.Proposal.ProofIndex
			}
		}
		*proposals = append(*proposals, clusterProposalWaiter{
			context:      request.context,
			requestID:    reqID,
			proofIndex:   proofIdx,
			command:      reqCmd,
			data:         request.data,
			result:       request.applyResult,
			startedAt:    startedAt,
			backlogEntry: backlogEntry,
		})
	case clusterDriverBackup:
		if request.context != nil && request.context.Err() != nil {
			request.backupResult <- clusterBackupImageResponse{err: request.context.Err()}
			return
		}
		runtime.backupRequests = append(runtime.backupRequests, request)
	case clusterDriverCancelRead:
		if waiter, exists := reads[request.contextKey]; exists {
			if waiter.commit == nil {
				runtime.observeRaftStage(adaptersmetrics.RaftReadIndex, waiter.startedAt)
			}
			delete(reads, request.contextKey)
			if waiter.commit != nil {
				runtime.finishCommitRead(waiter.commit, context.Canceled)
			} else if waiter.result != nil {
				waiter.result <- clusterReadResponse{err: context.Canceled}
			}
		}
	}
}

func (runtime *clusterRuntime) resolveProjectionRequests(driverErr error, stable bool) {
	remaining := runtime.projectionRequests[:0]
	for _, request := range runtime.projectionRequests {
		err := driverErr
		if request.context != nil && request.context.Err() != nil {
			err = request.context.Err()
		} else if err == nil {
			storage := runtime.storage.view()
			if storage.health != clusterReplicationHealthy {
				err = errClusterRuntimeNotReady
				runtime.logReadinessFailure("publish", "readiness_storage", err, storage, 0, nil, 0, 0, false)
			} else if !stable {
				remaining = append(remaining, request)
				continue
			} else if snapshot := runtime.projectionReadinessSnapshot(); !snapshot.admitsBarrier() {
				err = errClusterRuntimeNotReady
				sequence := runtime.projectionAdmissionSequence.Load()
				runtime.logReadinessFailure("publish", "readiness_local_state", err, storage, 0, &snapshot, sequence, sequence, false)
			}
		}
		if request.readResult != nil {
			request.readResult <- clusterReadResponse{err: err}
		}
	}
	clear(runtime.projectionRequests[len(remaining):])
	runtime.projectionRequests = remaining
}

func (runtime *clusterRuntime) stepInboundDelivery(delivery tcpadapter.PeerInboundDelivery) (raftadapter.StepResultDTO, error) {
	runtime.observeRaftStage(adaptersmetrics.RaftInboundQueue, delivery.EnqueuedAt)
	message := delivery.Message
	if runtime.faults.isolates(message.From) {
		delivery.Complete()
		return raftadapter.StepResultDTO{}, nil
	}
	proposal, err := runtime.node.Step(raftadapter.MessageDTO{
		From: raftadapter.NodeID(message.From), To: raftadapter.NodeID(message.To), Data: message.Data,
	})
	delivery.Complete()
	if err != nil {
		if errors.Is(err, raftadapter.ErrBackpressure) {
			// Replication retries; a forwarded proposal rejected by the full
			// admission budget can still time out at its original caller unacked.
			return raftadapter.StepResultDTO{}, nil
		}
		if errors.Is(err, raftadapter.ErrInvalidMessage) {
			_ = runtime.transport.ResetPeer(message.From)
			return raftadapter.StepResultDTO{}, nil
		}
		return raftadapter.StepResultDTO{}, fmt.Errorf("step Raft message from peer %d: %w", message.From, err)
	}
	return proposal, nil
}

// stageQueuedDriverRequests stages up to limit already-queued requests without blocking.
func (runtime *clusterRuntime) stageQueuedDriverRequests(
	limit int,
	reads map[string]clusterReadWaiter,
	proposals *[]clusterProposalWaiter,
) (int, bool) {
	staged := 0
	control := false
	if runtime.hasDeferredRequest && limit > 0 {
		request := runtime.deferredRequest
		if !runtime.tryDriverRequest(request, reads, proposals) {
			return 0, true
		}
		runtime.deferredRequest = clusterDriverRequest{}
		runtime.hasDeferredRequest = false
		staged++
	}
	for staged < limit {
		select {
		case request := <-runtime.requests:
			before := len(*proposals)
			if !runtime.tryDriverRequest(request, reads, proposals) {
				return staged, true
			}
			control = control || request.kind == clusterDriverRead || request.kind == clusterDriverProjection ||
				request.kind == clusterDriverBackup || (request.kind == clusterDriverProposal && len(*proposals) == before)
			staged++
		default:
			return staged, control
		}
	}
	return staged, control
}

// stageQueuedInbound returns the count and whether immediate control progress is needed.
func (runtime *clusterRuntime) stageQueuedInbound(limit int) (int, bool, error) {
	staged := 0
	control := false
	for staged < limit {
		delivery, received := runtime.inboundQueue.TakeInbound()
		if !received {
			return staged, control, nil
		}
		staged++
		proposal, err := runtime.stepInboundDelivery(delivery)
		if err != nil {
			return staged, true, err
		}
		control = control || !proposal.Proposal
		if proposal.ReadIndexRequest {
			return staged, true, nil
		}
	}
	return staged, control, nil
}

// stageQueuedDriverWork shares a 64-item additional-work budget and reserves inbound Raft progress first.
func (runtime *clusterRuntime) stageQueuedDriverWork(
	reads map[string]clusterReadWaiter,
	proposals *[]clusterProposalWaiter,
	stop <-chan struct{},
) error {
	select {
	case <-stop:
		return nil
	default:
	}
	staged, _, err := runtime.stageQueuedInbound(clusterDriverStageLimit)
	if err != nil {
		return err
	}
	runtime.refreshReadWaiters(reads)
	remaining := clusterDriverStageLimit - staged
	requests, _ := runtime.stageQueuedDriverRequests(remaining, reads, proposals)
	_, _, err = runtime.stageQueuedInbound(remaining - requests)
	return err
}

type raftLeadership struct {
	leader raftadapter.NodeID
	term   uint64
	role   raftadapter.Role
}

func clusterBatchEmpty(batch raftadapter.BatchDTO) bool {
	return !batch.Progress && batch.ApplySequence == 0 && len(batch.Messages) == 0 && len(batch.Commands) == 0 && len(batch.ConfigurationChanges) == 0 &&
		len(batch.SnapshotData) == 0 && len(batch.ReadStates) == 0 && len(batch.SnapshotStreams) == 0
}

func (runtime *clusterRuntime) deliveryCrashArmed(point clusterFailpoint) bool {
	return runtime.failpoint == point || runtime.faults.crash() == point
}

func (runtime *clusterRuntime) applyBatch(batch raftadapter.BatchDTO, proposals *[]clusterProposalWaiter) error {
	if len(batch.Commands) != len(batch.CommandIndexes) {
		return errors.New("raft command data and indexes are not aligned")
	}
	if len(batch.SnapshotData) != 0 {
		snapshot, err := decodeMembershipSnapshot(batch.SnapshotData)
		if err != nil || snapshot.LastIncludedIndex != batch.SnapshotIndex {
			if err == nil {
				err = errors.New("raft snapshot index differs from typed application snapshot")
			}
			return fmt.Errorf("restore committed application snapshot: %w", err)
		}
		if err := runtime.machine.Restore(snapshot); err != nil {
			return fmt.Errorf("restore membership state machine: %w", err)
		}
		if runtime.mqttServer != nil {
			runtime.mqttServer.ReplaceCredentialRevocations(runtime.machine.CredentialRevocations())
			runtime.mqttServer.ReplaceBlocklist(runtime.machine.BlocklistEntries())
		}
		if err := runtime.applyCommittedClusterMQTTLimits(); err != nil {
			return err
		}
		if runtime.clusterPolicyController != nil {
			runtime.clusterPolicyController.refreshCommittedState()
		}
		runtime.releaseProfile.observeSnapshotRestore()
		runtime.sessionRecovery.requestFullScan()
		runtime.retainedResyncRequired.Store(true)
		runtime.lastSnapshotIndex = snapshot.LastIncludedIndex
		runtime.initialized.Store(true)
	}
	events := make([]clusterApplyEvent, 0, len(batch.Commands)+len(batch.ConfigurationChanges))
	for index, command := range batch.Commands {
		events = append(events, clusterApplyEvent{index: batch.CommandIndexes[index], command: command})
	}
	for index := range batch.ConfigurationChanges {
		change := batch.ConfigurationChanges[index]
		events = append(events, clusterApplyEvent{index: change.Index, configuration: &change})
	}
	sort.Slice(events, func(left, right int) bool { return events[left].index < events[right].index })
	for index, event := range events {
		if index > 0 && events[index-1].index == event.index {
			return fmt.Errorf("duplicate committed application event at raft index %d", event.index)
		}
		if event.configuration != nil {
			if !runtime.initialized.Load() {
				return errors.New("configuration event arrived before typed state initialization")
			}
			result, err := runtime.applyConfiguration(*event.configuration)
			if err != nil {
				return err
			}
			if result.Outcome != membership.ApplyOutcomeApplied {
				return fmt.Errorf("%w at raft index %d: %s", errClusterApplyDivergence, event.index, result.Conflict)
			}
			if result.Operation != nil {
				runtime.logger.Info("committed cluster membership operation",
					"operation_id", result.Operation.OperationID,
					"member_id", result.Operation.MemberID,
					"phase", result.Operation.Phase,
					"committed_index", result.Operation.CommittedIndex)
			}
			if runtime.failpoint.matchesConfigurationApply(event.configuration.Action) {
				triggerClusterFailpoint(runtime.failpoint)
			}
			continue
		}
		command, err := decodeMembershipCommand(event.command)
		if err != nil {
			return fmt.Errorf("decode committed command at raft index %d: %w", event.index, err)
		}
		if !runtime.initialized.Load() {
			return errors.New("application command arrived before typed state initialization")
		}
		deliveryAckCommand := isDeliveryAckCommand(command)
		if deliveryAckCommand && runtime.deliveryCrashArmed(failpointDeliveryCommitBeforeApply) {
			triggerClusterFailpoint(failpointDeliveryCommitBeforeApply)
		}
		previousClusterPolicy, hadPreviousClusterPolicy := runtime.machine.ClusterPolicy()
		result, err := runtime.machine.Apply(event.index, command)
		if err != nil {
			return fmt.Errorf("apply committed command at raft index %d: %w", event.index, err)
		}
		if command.ClusterPolicy != nil && result.Outcome == membership.ApplyOutcomeApplied {
			currentClusterPolicy, found := runtime.machine.ClusterPolicy()
			if found && (!hadPreviousClusterPolicy || currentClusterPolicy.Policy.MQTTLimits != previousClusterPolicy.Policy.MQTTLimits) {
				if err := runtime.applyCommittedClusterMQTTLimits(); err != nil {
					return err
				}
			}
			if runtime.clusterPolicyController != nil {
				runtime.clusterPolicyController.refreshCommittedState()
			}
		}
		if change := command.CredentialRevocation; change != nil &&
			result.Outcome == membership.ApplyOutcomeApplied && runtime.mqttServer != nil {
			runtime.mqttServer.ApplyCredentialRevocation(
				change.CredentialID, runtime.machine.IsCredentialRevoked(change.CredentialID),
			)
		}
		if change := command.Blocklist; change != nil &&
			result.Outcome == membership.ApplyOutcomeApplied && runtime.mqttServer != nil {
			runtime.mqttServer.ApplyBlocklist(change.Entry, runtime.machine.IsBlocked(change.Entry))
		}
		if result.Replayed {
			runtime.stageProposalResult(event.command, result, proposals)
			continue
		}
		runtime.releaseProfile.observeAppliedCommand(command, result)
		if deliveryAckCommand && runtime.deliveryCrashArmed(failpointDeliveryCommitAfterApply) {
			triggerClusterFailpoint(failpointDeliveryCommitAfterApply)
		}
		if result.Outcome == membership.ApplyOutcomeConflict && command.Membership != nil &&
			command.Membership.Action == membership.MembershipActionAddLearner {
			return fmt.Errorf("%w at raft index %d: %s", errClusterApplyDivergence, event.index, result.Conflict)
		}
		if result.Operation != nil {
			runtime.logger.Info("committed cluster membership operation",
				"operation_id", result.Operation.OperationID,
				"member_id", result.Operation.MemberID,
				"phase", result.Operation.Phase,
				"committed_index", result.Operation.CommittedIndex)
		}
		if command.NodeDrain != nil && result.Outcome == membership.ApplyOutcomeApplied && result.NodeDrain != nil {
			runtime.logger.Info("committed node drain transition",
				"drain_operation_id", result.NodeDrain.DrainOperationID,
				"member_id", result.NodeDrain.MemberID,
				"action", command.NodeDrain.Action,
				"phase", result.NodeDrain.Phase.String(),
				"failure_code", result.NodeDrain.FailureCode,
				"committed_index", event.index)
			if runtime.metrics != nil && result.NodeDrain.MemberID == runtime.config.NodeID {
				runtime.metrics.NodeDrainTransition(result.NodeDrain.Phase.String())
				if phase := result.NodeDrain.Phase; phase.Valid() {
					progress := result.NodeDrain.Progress
					runtime.metrics.SetNodeDrainProgress(phase.Active() && !phase.Terminal(), adaptersmetrics.NodeDrainProgress{
						ConnectionsInitial:       progress.ConnectionsInitial,
						ConnectionsRemaining:     progress.ConnectionsRemaining,
						SessionsReadyForTakeover: progress.SessionsReadyForTakeover,
						SessionsTakenOver:        progress.SessionsTakenOver,
						SessionsFailed:           progress.SessionsFailed,
						PendingWAL:               progress.PendingWAL,
						UnderReplicated:          progress.UnderReplicated,
					})
				}
			}
		}
		if command.Membership != nil && command.Membership.Action == membership.MembershipActionBeginRemove &&
			result.Outcome == membership.ApplyOutcomeApplied && runtime.failpoint.matchesBeginRemoveApply() {
			triggerClusterFailpoint(runtime.failpoint)
		}
		if command.Ownership != nil && result.Outcome == membership.ApplyOutcomeApplied && result.Ownership != nil {
			sessionID, err := clusterSessionID(command.Ownership.TenantID, command.Ownership.ClientID)
			if err != nil {
				return fmt.Errorf("committed ownership session identity is invalid: %w", err)
			}
			if runtime.mqttServer != nil {
				runtime.mqttServer.RecordCommittedOwnership(*result.Ownership)
				if runtime.drain != nil && !runtime.drain.admissionOpen() && result.Ownership.OwnerNodeID != runtime.config.NodeID {
					runtime.mqttServer.CloseSessionForDrain(sessionID, runtime.drain.currentServerReference())
				} else if runtime.rebalance != nil && !runtime.rebalance.admissionOpen() && result.Ownership.OwnerNodeID != runtime.config.NodeID {
					runtime.mqttServer.CloseSessionForDrain(sessionID, runtime.rebalance.currentServerReference())
				} else {
					runtime.mqttServer.CloseSessionForTakeover(sessionID)
				}
			}
			runtime.removeStaleDurableAttachment(sessionID, *result.Ownership)
			if result.Ownership.OwnerNodeID == runtime.config.NodeID && command.Ownership.Session == nil {
				runtime.sessionRecovery.enqueue(command.Ownership.TenantID, command.Ownership.ClientID)
			}
		}
		if command.Retained != nil && result.Outcome == membership.ApplyOutcomeApplied {
			runtime.syncRetainedProjection(*command.Retained)
		}
		if command.Delivery != nil && result.Outcome == membership.ApplyOutcomeApplied && result.Delivery != nil {
			if len(result.Delivery.Enqueued) > 0 {
				runtime.notifyDurable(command.Delivery.TenantID, result.Delivery.Enqueued)
			}
			if len(result.Delivery.Saturated) > 0 && runtime.logger != nil {
				runtime.logger.Warn("durable delivery targets saturated", "tenant_id", command.Delivery.TenantID, "saturated", result.Delivery.Saturated)
			}
			if len(result.Delivery.Skipped) > 0 && runtime.logger != nil {
				runtime.logger.Warn("durable delivery targets skipped", "tenant_id", command.Delivery.TenantID, "skipped", result.Delivery.Skipped)
			}
		}
		if reroutes := runtime.machine.TakeDeliveryReroutes(); len(reroutes) > 0 {
			runtime.notifyDeliveryReroutes(reroutes)
		}
		runtime.stageProposalResult(event.command, result, proposals)
	}
	if runtime.initialized.Load() {
		// Ready can end with empty Raft entries after its last typed event.
		// Snapshot metadata and typed state must use the same applied index.
		if err := runtime.machine.AdvanceAppliedIndex(runtime.node.Status().AppliedIndex); err != nil {
			return fmt.Errorf("advance committed membership index: %w", err)
		}
	}
	if len(events) != 0 || len(batch.SnapshotData) != 0 || runtime.projectionFailed.Load() {
		runtime.refreshRouteProjection()
	} else {
		runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
	}
	if runtime.drain != nil {
		runtime.drain.sync()
	}
	if runtime.rebalance != nil {
		runtime.rebalance.sync()
	}
	return runtime.reconcileCommittedPeers()
}

func (runtime *clusterRuntime) applyConfiguration(change raftadapter.CommittedConfigurationDTO) (membership.ApplyResultDTO, error) {
	var action membership.ConfigurationAction
	switch change.Action {
	case raftadapter.MembershipAddLearner:
		action = membership.ConfigurationActionAddLearner
	case raftadapter.MembershipPromoteVoter:
		action = membership.ConfigurationActionPromoteVoter
	case raftadapter.MembershipRemove:
		action = membership.ConfigurationActionRemoveMember
	default:
		return membership.ApplyResultDTO{}, fmt.Errorf("unsupported committed raft configuration action %d", change.Action)
	}
	result, err := runtime.machine.ApplyConfiguration(change.Index, change.OperationID, action, uint64(change.MemberID))
	if err != nil {
		return membership.ApplyResultDTO{}, fmt.Errorf("apply committed configuration at raft index %d: %w", change.Index, err)
	}
	return result, nil
}

func (runtime *clusterRuntime) stageProposalResult(data []byte, result membership.ApplyResultDTO, waiters *[]clusterProposalWaiter) {
	for index := range *waiters {
		waiter := &(*waiters)[index]
		if waiter.applied || !bytes.Equal(waiter.data, data) {
			continue
		}
		waiter.response = clusterApplyResponse{result: result}
		waiter.applied = true
		return
	}
}

func (runtime *clusterRuntime) resolveAppliedProposals(waiters *[]clusterProposalWaiter) {
	for index := 0; index < len(*waiters); {
		waiter := (*waiters)[index]
		if !waiter.applied {
			index++
			continue
		}
		runtime.observeRaftStage(adaptersmetrics.RaftProposal, waiter.startedAt)
		// Retire commit work before a caller consumes its ordered result.
		runtime.removeCommitBacklog(waiter.backlogEntry)
		waiter.result <- waiter.response
		last := len(*waiters) - 1
		copy((*waiters)[index:], (*waiters)[index+1:])
		(*waiters)[last] = clusterProposalWaiter{}
		*waiters = (*waiters)[:last]
	}
}

func (runtime *clusterRuntime) failPendingProposals(waiters *[]clusterProposalWaiter, driverErr error) {
	if len(*waiters) == 0 {
		return
	}
	if driverErr == nil {
		driverErr = errors.New("cluster raft driver stopped before proposal projection completed")
	}
	for _, waiter := range *waiters {
		runtime.observeRaftStage(adaptersmetrics.RaftProposal, waiter.startedAt)
		runtime.removeCommitBacklog(waiter.backlogEntry)
		if waiter.result != nil {
			waiter.result <- clusterApplyResponse{err: driverErr}
		}
	}
	*waiters = nil
}

func (runtime *clusterRuntime) failQueuedDriverRequests(driverErr error) {
	runtime.resolveProjectionRequests(driverErr, true)
	runtime.failCommitReads(driverErr)
	runtime.resolveBackupRequests(driverErr)
	for {
		var request clusterDriverRequest
		if runtime.hasDeferredRequest {
			request = runtime.deferredRequest
			runtime.deferredRequest = clusterDriverRequest{}
			runtime.hasDeferredRequest = false
		} else {
			select {
			case request = <-runtime.requests:
			default:
				return
			}
		}
		switch request.kind {
		case clusterDriverRead, clusterDriverProjection:
			if request.readResult != nil {
				request.readResult <- clusterReadResponse{err: driverErr}
			}
		case clusterDriverCommit:
			runtime.removeCommitBacklog(request.pending.backlogEntry)
			request.pending.staged <- clusterCommitStage{err: driverErr, barrierFailure: true}
		case clusterDriverProposal:
			if request.pending != nil {
				runtime.removeCommitBacklog(request.pending.backlogEntry)
			}
			if request.applyResult != nil {
				request.applyResult <- clusterApplyResponse{err: driverErr}
			}
		case clusterDriverBackup:
			if request.backupResult != nil {
				request.backupResult <- clusterBackupImageResponse{err: driverErr}
			}
		}
	}
}

func (runtime *clusterRuntime) completeReadStates(states []raftadapter.ReadStateDTO, waiters map[string]clusterReadWaiter) []raftadapter.ReadStateDTO {
	remaining := states[:0]
	status := runtime.node.Status()
	for _, state := range states {
		if len(state.RequestContext) <= clusterReadProofHeader ||
			binary.BigEndian.Uint64(state.RequestContext[:8]) != uint64(status.NodeID) {
			continue
		}
		key := string(state.RequestContext[clusterReadProofHeader:])
		waiter, exists := waiters[key]
		if !exists || waiter.proofSequence == 0 ||
			binary.BigEndian.Uint64(state.RequestContext[8:clusterReadProofHeader]) != waiter.proofSequence {
			continue
		}
		if waiter.ctx != nil && waiter.ctx.Err() != nil {
			delete(waiters, key)
			if waiter.commit != nil {
				runtime.finishCommitRead(waiter.commit, waiter.ctx.Err())
			} else if waiter.result != nil {
				runtime.observeRaftStage(adaptersmetrics.RaftReadIndex, waiter.startedAt)
				waiter.result <- clusterReadResponse{err: waiter.ctx.Err()}
			}
			continue
		}
		if state.Index > status.AppliedIndex || state.Index > runtime.projectionIndex.Load() {
			waiter.answered = true
			waiters[key] = waiter
			remaining = append(remaining, state)
			continue
		}
		delete(waiters, key)
		if key == runtime.recoveryProofKey {
			runtime.recoveryProofWait = false
			runtime.recoveryProofKey = ""
			runtime.recoveryProofReady = true
		}
		if waiter.commit != nil {
			waiter.commit.readProofIndex = state.Index
			runtime.finishCommitRead(waiter.commit, nil)
		} else {
			runtime.observeRaftStage(adaptersmetrics.RaftReadIndex, waiter.startedAt)
			if waiter.result != nil {
				waiter.result <- clusterReadResponse{index: state.Index}
			}
		}
	}
	return remaining
}

// Only the driver touches readConfirmed/readErr and this FIFO. A later fresh
// proof cannot make a command overtake an earlier command on leader change.
func (runtime *clusterRuntime) resolveCommitReads(reads map[string]clusterReadWaiter, proposals *[]clusterProposalWaiter) bool {
	consumed := 0
	proposed := false
	var batch [clusterDriverStageLimit]*clusterPendingCommit
	batchCount, batchBytes := 0, 0
	for consumed < len(runtime.pendingCommits) {
		pending := runtime.pendingCommits[consumed]
		if !pending.readConfirmed && pending.readContext.Err() == nil {
			break
		}
		if !pending.readConfirmed {
			runtime.finishCommitRead(pending, pending.readContext.Err())
		}
		delete(reads, pending.contextKey)
		stage := clusterCommitStage{context: pending.context}
		if err := pending.readContext.Err(); err != nil {
			stage.err, stage.barrierFailure = err, true
		} else if pending.readErr != nil {
			stage.err, stage.barrierFailure = pending.readErr, true
		} else if runtime.storage.view().health == clusterReplicationFailed {
			stage.err, stage.barrierFailure = errClusterStorageUnhealthy, true
		} else if clusterCommandRequiresSessionMetadata(pending.command) &&
			!slices.Contains(runtime.machine.ActiveFeatures(), membership.SessionMetadataFeature) {
			stage.err = errClusterSessionMetadataInactive
		} else if pending.command.ConnectionRebalance != nil &&
			!slices.Contains(runtime.machine.ActiveFeatures(), membership.ConnectionRebalanceFeature) {
			stage.err = errClusterConnectionRebalanceInactive
		} else if (pending.command.Retained != nil || pending.command.Offline != nil ||
			pending.command.Delivery != nil || pending.command.NodeDrain != nil || pending.command.TenantQuota != nil ||
			pending.command.ConnectionRebalance != nil) &&
			!clusterProfilesCompatible(runtime.machine.Members(), runtime.config) {
			stage.err = errClusterProfileCompatibility
		} else {
			if runtime.faults.enabled() {
				kind, _ := clusterHeldCommandLabel(pending.command)
				stage.held = kind != "" && runtime.faults.holds(kind)
			}
			if !stage.held {
				if runtime.machine != nil && slices.Contains(runtime.machine.ActiveFeatures(), membership.ProposalRecoveryFeature) && pending.command.Proposal == nil {
					floor := runtime.safeRetirementFloor(pending.readProofIndex, *proposals, batch[:batchCount]...)
					count, total, retired := runtime.machine.ProposalAdmission(runtime.config.NodeID, floor)
					if pending.readProofIndex <= retired || count+len(*proposals)+batchCount >= membership.MaxProposalReceiptsPerOrigin || total+len(*proposals)+batchCount >= membership.MaxProposalReceipts {
						if runtime.machine.LastAppliedIndex() > pending.readProofIndex {
							runtime.refreshPendingProposalProof(pending, reads)
						}
						break
					}
					stage.err = runtime.prepareProposal(pending, floor)
				}
				if stage.err == nil {
					entryBytes := len(pending.data) + 32
					if batchCount == len(batch) || (batchCount != 0 && batchBytes+entryBytes > clusterProposalBatchBytes) {
						proposed = runtime.submitCommitBatch(batch[:batchCount], proposals) || proposed
						clear(batch[:batchCount])
						batchCount, batchBytes = 0, 0
					}
					batch[batchCount] = pending
					batchCount++
					batchBytes += entryBytes
				}
			}
		}
		if stage.err != nil {
			runtime.removeCommitBacklog(pending.backlogEntry)
		}
		pending.staged <- stage
		runtime.pendingCommits[consumed] = nil
		consumed++
	}
	if batchCount != 0 {
		proposed = runtime.submitCommitBatch(batch[:batchCount], proposals) || proposed
	}
	if consumed != 0 {
		remaining := copy(runtime.pendingCommits, runtime.pendingCommits[consumed:])
		clear(runtime.pendingCommits[remaining:])
		runtime.pendingCommits = runtime.pendingCommits[:remaining]
	}
	return proposed
}

func (runtime *clusterRuntime) failCommitReads(err error) {
	for _, pending := range runtime.pendingCommits {
		runtime.finishCommitRead(pending, err)
		runtime.removeCommitBacklog(pending.backlogEntry)
		pending.staged <- clusterCommitStage{err: err, barrierFailure: true}
	}
	clear(runtime.pendingCommits)
	runtime.pendingCommits = runtime.pendingCommits[:0]
}

func (runtime *clusterRuntime) sendMessages(ctx context.Context, messages []raftadapter.MessageDTO) error {
	defer func() { runtime.readIndexStaged = 0 }()
	rejected := make(map[raftadapter.NodeID]struct{}, len(messages))
	for _, message := range messages {
		if runtime.faults.isolates(uint64(message.To)) || message.Type == "MsgProp" && runtime.faults.dropsProposals(uint64(message.To)) {
			continue
		}
		// A Ready batch can contain a response to a peer removed by that same committed change.
		// Keep registered seed peers available before a joining node has a local ConfState.
		if _, statusErr := runtime.transport.Status(uint64(message.To)); errors.Is(statusErr, tcpadapter.ErrUnknownPeer) {
			continue
		}
		if _, found := rejected[message.To]; found {
			if message.Type == "MsgProp" {
				_ = runtime.node.DeferForwardedProposal(message.Data)
				continue
			}
			if message.IsSnapshot {
				_ = runtime.node.ReportPeerMessageFailure(message.To, true)
			}
			continue
		}
		// Raft may forward a proposal or ReadIndex while retaining its original
		// requester in the protobuf From field. The transport identity is the
		// authenticated immediate sender, not that logical requester.
		err := runtime.transport.Send(tcpadapter.PeerMessageDTO{
			From: runtime.config.NodeID, To: uint64(message.To), Data: message.Data,
			Reclaimable: message.Type == "MsgProp",
		})
		if err == nil {
			continue
		}
		if errors.Is(err, tcpadapter.ErrPeerBackpressure) {
			if message.Type == "MsgProp" && runtime.node.DeferForwardedProposal(message.Data) == nil {
				continue
			}
			rejected[message.To] = struct{}{}
			if reportErr := runtime.node.ReportPeerMessageFailure(message.To, message.IsSnapshot); reportErr != nil && ctx.Err() == nil {
				localStatus := runtime.node.Status()
				// RawNode cannot report a peer unreachable until it appears in the local ConfState.
				if errors.Is(reportErr, raftadapter.ErrInvalidMessage) &&
					isUnconfiguredRemotePeer(localStatus, runtime.config.NodeID, message.To) {
					if peerStatus, statusErr := runtime.transport.Status(uint64(message.To)); statusErr == nil {
						runtime.logger.Warn("dynamic peer backpressure for peer outside local Raft configuration",
							"peer_id", message.To, "connected", peerStatus.Connected,
							"last_error", peerStatus.LastError)
					}
					continue
				}
				if status, statusErr := runtime.transport.Status(uint64(message.To)); statusErr == nil {
					runtime.logger.Error("cannot report dynamic raft peer backpressure",
						"peer_id", message.To, "last_error", status.LastError)
				}
				return fmt.Errorf("report unreachable raft peer %d: %w", message.To, reportErr)
			}
			if status, statusErr := runtime.transport.Status(uint64(message.To)); statusErr == nil {
				runtime.logger.Warn("dynamic peer backpressure", "peer_id", message.To,
					"connected", status.Connected, "queued_messages", status.OutboundQueuedMessages,
					"queued_bytes", status.OutboundQueuedBytes, "rejected_messages", status.RejectedMessages,
					"message_type", message.Type, "last_error", status.LastError)
			}
			continue
		}
		if errors.Is(err, tcpadapter.ErrPeerTransportClosed) && ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("queue raft message for peer %d: %w", message.To, err)
	}
	runtime.reclaimUnsentProposals()
	return nil
}

func (runtime *clusterRuntime) advanceMembership() error {
	status := runtime.node.Status()
	if status.Role != raftadapter.RoleLeader || !runtime.initialized.Load() {
		return nil
	}
	if runtime.leadershipTransferTo != 0 {
		if status.Term == runtime.leadershipTransferTerm {
			runtime.leadershipTransferTicks++
			if runtime.leadershipTransferTicks <= 20 {
				return nil
			}
		}
		runtime.leadershipTransferTo = 0
		runtime.leadershipTransferTerm = 0
		runtime.leadershipTransferTicks = 0
	}
	operations := runtime.machine.Operations()
	members := runtime.machine.Members()
	byID := make(map[uint64]membership.MemberIdentityDTO, len(members))
	for _, member := range members {
		byID[member.MemberID] = member
	}
	if runtime.autoCatchupPending != "" {
		stillPending := false
		for _, operation := range operations {
			if operation.OperationID == runtime.autoCatchupPending && operation.Phase == membership.OperationPhaseLearner {
				stillPending = true
				break
			}
		}
		if !stillPending {
			runtime.autoCatchupPending = ""
		}
	}
	for _, operation := range operations {
		switch operation.Phase {
		case membership.OperationPhaseStaged:
			runtime.admissionMu.Lock()
			admittedOperation, admitted := runtime.admittedCandidates[operation.MemberID]
			runtime.admissionMu.Unlock()
			if admitted && admittedOperation == operation.OperationID {
				return runtime.proposeMembershipChange(raftadapter.MembershipChangeDTO{
					OperationID: operation.OperationID, MemberID: raftadapter.NodeID(operation.MemberID),
					Action: raftadapter.MembershipAddLearner,
				})
			}
		case membership.OperationPhaseLearner:
			if runtime.autoCatchupPending == operation.OperationID {
				continue
			}
			member, found := byID[operation.MemberID]
			if !found || operation.Record.Action != membership.MembershipActionAddLearner {
				continue
			}
			catchingUp := member
			catchingUp.Phase = membership.MemberPhaseCatchingUp
			catchingUp.OperationID = operation.OperationID
			command := membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: operation.OperationID,
				Membership: &membership.MembershipRecordDTO{
					OperationID: operation.OperationID, Action: membership.MembershipActionAddLearner,
					IdempotencyKey: operation.Record.IdempotencyKey, Member: &catchingUp,
				},
			}
			accepted, err := runtime.proposeInternal(command)
			if err != nil {
				return err
			}
			if accepted {
				runtime.logger.Info("proposed cluster membership catch-up phase",
					"operation_id", operation.OperationID, "member_id", operation.MemberID)
				runtime.autoCatchupPending = operation.OperationID
			}
			return nil
		case membership.OperationPhaseCatchingUp:
			match, ok := runtime.node.PeerMatchIndex(raftadapter.NodeID(operation.MemberID))
			if ok && match >= status.CommitIndex && status.AppliedIndex >= status.CommitIndex {
				return runtime.proposeMembershipChange(raftadapter.MembershipChangeDTO{
					OperationID: operation.OperationID, MemberID: raftadapter.NodeID(operation.MemberID),
					Action: raftadapter.MembershipPromoteVoter,
				})
			}
		case membership.OperationPhaseRemoving:
			if operation.MemberID == runtime.config.NodeID {
				return runtime.transferLocalLeadership(status)
			}
			voterIDs := make([]uint64, 0, len(status.VoterIDs))
			for _, id := range status.VoterIDs {
				voterIDs = append(voterIDs, uint64(id))
			}
			var plan membership.RemovalHandoffPlanDTO
			if !membership.RemovalKeepsReplicationFactor(operation.MemberID, voterIDs, runtime.config.replicationProfile()) {
				plan = membership.RemovalHandoffPlanDTO{Block: membership.RemovalHandoffBlockReplicationFactor}
			} else {
				plan = membership.PlanRemovalHandoff(
					operation.MemberID, voterIDs,
					runtime.machine.Ownerships(), runtime.machine.Subscriptions(),
				)
			}
			if plan.Block != "" {
				logKey := operation.OperationID + ":" + string(plan.Block)
				if logKey != runtime.removalBlockLogged {
					runtime.removalBlockLogged = logKey
					runtime.logger.Warn("planned removal waits for session handoff",
						"operation_id", operation.OperationID,
						"member_id", operation.MemberID,
						"reason", string(plan.Block))
				}
				return nil
			}
			runtime.removalBlockLogged = ""
			if len(plan.Transfers) != 0 {
				for _, transfer := range plan.Transfers {
					key := transfer.TenantID + "\x00" + transfer.ClientID
					if runtime.handoffProposed[key] == transfer.ExpectedEpoch {
						continue
					}
					opID, err := newClusterOperationID()
					if err != nil {
						return fmt.Errorf("generate handoff operation id: %w", err)
					}
					transferCopy := transfer
					accepted, err := runtime.proposeInternal(membership.CommandDTO{
						SchemaMajor: membership.SchemaMajor,
						SchemaMinor: membership.SchemaMinor,
						OperationID: opID,
						Ownership:   &transferCopy,
					})
					if err != nil {
						return err
					}
					if accepted {
						runtime.handoffProposed[key] = transfer.ExpectedEpoch
					}
				}
				activeKeys := make(map[string]struct{}, len(plan.Transfers))
				for _, transfer := range plan.Transfers {
					activeKeys[transfer.TenantID+"\x00"+transfer.ClientID] = struct{}{}
				}
				for key := range runtime.handoffProposed {
					if _, exists := activeKeys[key]; !exists {
						delete(runtime.handoffProposed, key)
					}
				}
				runtime.logger.Info("proposed planned-removal session handoff",
					"operation_id", operation.OperationID,
					"member_id", operation.MemberID,
					"successor_node_id", plan.SuccessorNodeID,
					"session_count", len(plan.Transfers))
				return nil
			}
			if plan.RemoveReady {
				return runtime.proposeMembershipChange(raftadapter.MembershipChangeDTO{
					OperationID: operation.OperationID,
					MemberID:    raftadapter.NodeID(operation.MemberID),
					Action:      raftadapter.MembershipRemove,
				})
			}
			return nil
		}
	}
	return nil
}

func (runtime *clusterRuntime) proposeMembershipChange(change raftadapter.MembershipChangeDTO) error {
	err := runtime.node.ProposeMembershipChange(change)
	if err != nil && (errors.Is(err, raftadapter.ErrMembershipChangeInProgress) || errors.Is(err, raftadapter.ErrNotLeader)) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("propose committed membership transition: %w", err)
	}
	return nil
}

func (runtime *clusterRuntime) proposeInternal(command membership.CommandDTO) (bool, error) {
	data, err := encodeMembershipCommand(command)
	if err != nil {
		return false, fmt.Errorf("encode automatic membership command: %w", err)
	}
	if err := runtime.node.Propose(data); err != nil {
		if errors.Is(err, raftadapter.ErrNotLeader) || errors.Is(err, raftadapter.ErrBackpressure) {
			return false, nil
		}
		return false, fmt.Errorf("propose automatic membership command: %w", err)
	}
	return true, nil
}

// maybeSnapshot compacts the log periodically and immediately after a membership configuration change.
// A joining learner can restore only snapshots whose ConfState already includes that learner.
func (runtime *clusterRuntime) maybeSnapshot(force bool) error {
	return runtime.persistTypedSnapshot(force)
}

func (runtime *clusterRuntime) persistTypedSnapshot(force bool) error {
	force = force || runtime.snapshotPending
	status := runtime.node.Status()
	lastApplied := runtime.machine.LastAppliedIndex()
	if status.AppliedIndex == 0 || lastApplied != status.AppliedIndex ||
		(!force && status.AppliedIndex-runtime.lastSnapshotIndex < clusterSnapshotInterval) {
		return nil
	}
	if runtime.node.StoragePending() {
		runtime.snapshotPending = true
		return nil
	}
	snapshot, err := runtime.machine.Snapshot(lastApplied)
	if err != nil {
		return fmt.Errorf("create typed membership snapshot: %w", err)
	}
	data, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		return fmt.Errorf("encode typed membership snapshot: %w", err)
	}
	runtime.storage.begin()
	snapshotErr := runtime.node.SnapshotWithApplicationData(data)
	runtime.storage.end()
	if snapshotErr != nil {
		return fmt.Errorf("persist typed membership snapshot: %w", snapshotErr)
	}
	runtime.lastSnapshotIndex = lastApplied
	runtime.snapshotPending = false
	return nil
}

func (runtime *clusterRuntime) rebuildRouteProjection() error {
	runtime.projectionMu.Lock()
	defer runtime.projectionMu.Unlock()
	if runtime.failpoint.failsRouteProjection() {
		return errors.New("route projection rebuild failpoint")
	}
	if runtime.retainedResyncRequired.Load() {
		if err := runtime.rebuildRetainedProjectionLocked(); err != nil {
			return err
		}
	}

	projectionSessions := make([]routing.ProjectionSession, 0)
	for _, ownership := range runtime.machine.Ownerships() {
		tenantID, err := tenancy.NewTenantID(ownership.TenantID)
		if err != nil {
			return fmt.Errorf("committed ownership tenant is invalid: %w", err)
		}
		sessionID, err := clusterSessionID(ownership.TenantID, ownership.ClientID)
		if err != nil {
			return fmt.Errorf("committed ownership session identity is invalid: %w", err)
		}
		projectionSessions = append(projectionSessions, routing.ProjectionSession{
			TenantID: tenantID, SessionID: sessionID, OwnerNodeID: ownership.OwnerNodeID,
		})
	}

	projection := make([]routing.ProjectionSubscription, 0)
	for _, subscription := range runtime.machine.Subscriptions() {
		owner, exists := runtime.machine.GetOwnership(subscription.TenantID, subscription.ClientID)
		if !exists || owner.OwnerNodeID != subscription.OwnerNodeID || owner.Epoch != subscription.Epoch {
			continue
		}
		if subscription.QoS > uint32(mqtt.QoSExactlyOnce) || subscription.RetainHandling > uint32(mqtt.DoNotSendRetained) {
			return errors.New("committed subscription has invalid MQTT options")
		}
		tenantID, err := tenancy.NewTenantID(subscription.TenantID)
		if err != nil {
			return fmt.Errorf("committed subscription tenant is invalid: %w", err)
		}
		sessionID, err := clusterSessionID(subscription.TenantID, subscription.ClientID)
		if err != nil {
			return fmt.Errorf("committed subscription session identity is invalid: %w", err)
		}
		projection = append(projection, routing.ProjectionSubscription{
			TenantID: tenantID, SessionID: sessionID,
			Subscription: mqtt.Subscription{
				Filter: subscription.Filter, QoS: mqtt.QoS(subscription.QoS),
				NoLocal: subscription.NoLocal, RetainAsPublished: subscription.RetainAsPublished,
				RetainHandling: mqtt.RetainHandling(subscription.RetainHandling),
			},
		})
	}
	if err := runtime.routes.ReplaceProjectionWithSessions(projectionSessions, projection); err != nil {
		return fmt.Errorf("replace committed route projection: %w", err)
	}
	runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
	return nil
}

func (runtime *clusterRuntime) refreshRouteProjection() {
	if err := runtime.rebuildRouteProjection(); err != nil {
		if !runtime.projectionFailed.Swap(true) {
			runtime.logger.Warn("cluster route projection rebuild failed", "error", err.Error())
		}
		return
	}
	runtime.projectionFailed.Store(false)
}

func clusterSessionID(tenantID, clientID string) (routing.SessionID, error) {
	tenant, err := tenancy.NewTenantID(tenantID)
	if err != nil {
		return "", err
	}
	session, err := tenancy.NewSessionKey(tenant, clientID)
	if err != nil {
		return "", err
	}
	return routing.SessionID(session.RoutingID()), nil
}

func (runtime *clusterRuntime) reconcileCommittedPeers() error {
	status := runtime.node.Status()
	runtime.seedPeerMu.RLock()
	peers := mergeSeedAdmissionPeers(
		committedPeerIdentities(runtime.machine, status, runtime.config.NodeID),
		runtime.seedPeers, status, runtime.config.NodeID,
	)
	peers, incompatibilityReason := clusterPeerSetForReleaseWindow(runtime.config.ReleaseVersion, peers)
	if incompatibilityReason != "" {
		runtime.releaseProfile.setIncompatible(incompatibilityReason)
	}
	err := runtime.transport.ReconcilePeers(peers)
	runtime.seedPeerMu.RUnlock()
	if err != nil {
		return fmt.Errorf("reconcile committed raft peers: %w", err)
	}
	local, found := committedLocalIdentity(runtime.machine, runtime.config.NodeID, runtime.config.ReleaseVersion)
	if !found {
		return nil
	}
	localIsVoter := containsNodeID(status.VoterIDs, runtime.config.NodeID)
	localIsLearner := containsNodeID(status.LearnerIDs, runtime.config.NodeID)
	if localIsVoter {
		switch local.Phase {
		case tcpadapter.PeerPhaseVoter:
			local.OperationID = ""
		case tcpadapter.PeerPhaseLeaving:
		default:
			return fmt.Errorf("committed local phase %d does not match Raft voter membership", local.Phase)
		}
	} else if localIsLearner {
		switch local.Phase {
		case tcpadapter.PeerPhaseLearner, tcpadapter.PeerPhaseCatchingUp:
		default:
			return fmt.Errorf("committed local phase %d does not match Raft learner membership", local.Phase)
		}
	} else if local.Phase != tcpadapter.PeerPhaseStaged && local.Phase != tcpadapter.PeerPhaseRetired {
		return fmt.Errorf("committed local phase %d has no Raft membership", local.Phase)
	}
	if local.Phase == tcpadapter.PeerPhaseRetired {
		return nil
	}
	runtime.localMu.Lock()
	runtime.localIdentity = local
	runtime.localMu.Unlock()
	if err := runtime.transport.UpdateLocalIdentity(local); err != nil {
		return fmt.Errorf("update committed local peer identity: %w", err)
	}
	return nil
}

func (runtime *clusterRuntime) admitPeer(ctx context.Context, candidate tcpadapter.PeerIdentityDTO) (tcpadapter.SeedAdmissionResponseDTO, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return tcpadapter.SeedAdmissionResponseDTO{}, err
	}
	status := runtime.node.Status()
	if status.Role != raftadapter.RoleLeader || !runtime.initialized.Load() {
		return tcpadapter.SeedAdmissionResponseDTO{}, raftadapter.ErrNotLeader
	}
	members := runtime.machine.Members()
	var committed *membership.MemberIdentityDTO
	for index := range members {
		member := &members[index]
		if member.MemberID == candidate.MemberID && samePeerRequest(*member, candidate) {
			committed = member
			break
		}
	}
	if committed == nil {
		return tcpadapter.SeedAdmissionResponseDTO{}, errors.New("candidate has no committed membership identity")
	}
	candidateIdentity, err := peerIdentityFromMember(committed)
	if err != nil {
		return tcpadapter.SeedAdmissionResponseDTO{}, err
	}
	candidateIsVoter := containsNodeID(status.VoterIDs, candidate.MemberID)
	candidateIsLearner := containsNodeID(status.LearnerIDs, candidate.MemberID)
	switch candidateIdentity.Phase {
	case tcpadapter.PeerPhaseStaged:
		if candidateIdentity.OperationID == "" || candidateIsVoter || candidateIsLearner {
			return tcpadapter.SeedAdmissionResponseDTO{}, errors.New("staged candidate conflicts with committed Raft membership")
		}
	case tcpadapter.PeerPhaseLearner, tcpadapter.PeerPhaseCatchingUp:
		if candidateIdentity.OperationID == "" || !candidateIsLearner || candidateIsVoter {
			return tcpadapter.SeedAdmissionResponseDTO{}, errors.New("learner candidate conflicts with committed Raft membership")
		}
	case tcpadapter.PeerPhaseVoter:
		// A recovered voter resumes from local state without seed admission.
		// Incumbent count alone admits a lost voter in a four-voter cluster.
		return tcpadapter.SeedAdmissionResponseDTO{}, errors.New("voter must recover from persistent local state")
	default:
		return tcpadapter.SeedAdmissionResponseDTO{}, errors.New("candidate phase is not eligible for seed admission")
	}
	voterIDs := make(map[uint64]struct{}, len(status.VoterIDs))
	for _, id := range status.VoterIDs {
		if uint64(id) != candidate.MemberID {
			voterIDs[uint64(id)] = struct{}{}
		}
	}
	voters := make([]tcpadapter.PeerIdentityDTO, 0, len(voterIDs))
	for _, member := range members {
		if member.Phase != membership.MemberPhaseVoter || member.MemberID == candidate.MemberID {
			continue
		}
		if _, exists := voterIDs[member.MemberID]; !exists {
			continue
		}
		identity, err := peerIdentityFromMember(&member)
		if err != nil {
			return tcpadapter.SeedAdmissionResponseDTO{}, err
		}
		voters = append(voters, identity)
	}
	if len(voters) != 3 || len(voters) != len(voterIDs) {
		return tcpadapter.SeedAdmissionResponseDTO{}, errors.New("seed admission requires three incumbent voters")
	}
	sort.Slice(voters, func(left, right int) bool { return voters[left].MemberID < voters[right].MemberID })
	if candidateIdentity.Phase == tcpadapter.PeerPhaseStaged {
		runtime.admissionMu.Lock()
		runtime.admittedCandidates[candidate.MemberID] = candidateIdentity.OperationID
		runtime.admissionMu.Unlock()
	}
	return tcpadapter.SeedAdmissionResponseDTO{Accepted: true, Candidate: candidateIdentity, CurrentVoters: voters}, nil
}

func samePeerRequest(member membership.MemberIdentityDTO, candidate tcpadapter.PeerIdentityDTO) bool {
	return member.MemberID == candidate.MemberID && member.ClusterID == candidate.ClusterID &&
		member.Address == candidate.Address && member.DNSSAN == candidate.DNSSAN &&
		bytes.Equal(member.CertificateSHA256, candidate.CertificateSHA256[:]) && release.CompatibleStrings(member.ReleaseVersion, candidate.ReleaseVersion) &&
		member.ClusterProtocolMajor == candidate.ClusterProtocolMajor && member.ClusterProtocolMinor == candidate.ClusterProtocolMinor &&
		member.StateSchemaMajor == candidate.StateSchemaMajor && member.StateSchemaMinor == candidate.StateSchemaMinor
}

func (runtime *clusterRuntime) hasFreshProof() bool {
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	return runtime.checkReadiness(ctx, "summary") == nil
}

func (runtime *clusterRuntime) clusterReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	return runtime.checkReadiness(ctx, "mqtt") == nil
}

func (runtime *clusterRuntime) clusterProjectionReady() bool {
	if runtime == nil || runtime.node == nil || runtime.machine == nil || runtime.storage == nil {
		return false
	}
	for {
		// Stable fast path avoids a driver round trip. During Ready, queue a
		// checkpoint instead of trying to sample its short quiescent window.
		storage := runtime.storage.view()
		if storage.health != clusterReplicationHealthy {
			runtime.logReadinessFailure("publish", "readiness_storage", errClusterStorageUnhealthy, storage, 0, nil, 0, 0, false)
			return false
		}
		sequence := runtime.projectionAdmissionSequence.Load()
		if sequence&1 != 0 {
			return runtime.awaitProjectionAdmission()
		}
		snapshot := runtime.projectionReadinessSnapshot()
		if runtime.projectionAdmissionSequence.Load() != sequence {
			continue
		}
		storage = runtime.storage.view()
		if storage.health != clusterReplicationHealthy {
			runtime.logReadinessFailure("publish", "readiness_storage", errClusterStorageUnhealthy, storage, 0, nil, sequence, sequence, false)
			return false
		}
		if !snapshot.admitsBarrier() {
			runtime.logReadinessFailure("publish", "readiness_local_state", errClusterRuntimeNotReady, storage, 0, &snapshot, sequence, sequence, false)
			return false
		}
		return true
	}
}

func (runtime *clusterRuntime) awaitProjectionAdmission() bool {
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	result := make(chan clusterReadResponse, 1)
	request := clusterDriverRequest{kind: clusterDriverProjection, context: ctx, readResult: result}
	requests := runtime.requests
	ticker := time.NewTicker(clusterProjectionAdmissionPollInterval)
	defer ticker.Stop()
	for {
		storage := runtime.storage.view()
		if runtime.driverFailed.Load() || storage.health != clusterReplicationHealthy {
			return false
		}
		select {
		case requests <- request:
			requests = nil
		case response := <-result:
			return response.err == nil && !runtime.driverFailed.Load() &&
				runtime.storage.view().health == clusterReplicationHealthy
		case <-ticker.C:
		case <-runtime.driverStopped:
			return false
		case <-ctx.Done():
			runtime.logReadinessFailure("publish", "readiness_projection_checkpoint", ctx.Err(), storage, 0, nil, 0, 0, false)
			return false
		}
	}
}

func (runtime *clusterRuntime) finishProjectionAdmissionSequence(readyErr error) {
	if readyErr != nil {
		runtime.driverFailed.Store(true)
	}
	runtime.projectionAdmissionSequence.Add(1)
}

type clusterReadinessSnapshot struct {
	driverFailed                bool
	initialized                 bool
	projectionFailed            bool
	bootstrapped                bool
	dynamicMembership           bool
	localVoter                  bool
	localPhaseVoter             bool
	tenantQuotaProfileCommitted bool
	releaseProfileSynced        bool
	projectionIndex             uint64
	appliedIndex                uint64
}

func (s clusterReadinessSnapshot) hasLocalAdmissionState() bool {
	return s.initialized &&
		!s.projectionFailed &&
		s.bootstrapped &&
		!s.driverFailed &&
		s.dynamicMembership &&
		s.localVoter &&
		s.localPhaseVoter &&
		s.tenantQuotaProfileCommitted &&
		s.releaseProfileSynced
}

func (s clusterReadinessSnapshot) admitsBarrier() bool {
	return s.hasLocalAdmissionState() && s.projectionIndex >= s.appliedIndex
}

func (s clusterReadinessSnapshot) satisfiedAt(barrier uint64) bool {
	return s.admitsBarrier() && barrier <= s.appliedIndex && barrier <= s.projectionIndex
}
func (runtime *clusterRuntime) hasCommittedTenantQuotaProfile() bool {
	if runtime == nil || runtime.machine == nil {
		return false
	}
	_, found := runtime.machine.TenantQuotaProfile()
	return found
}

func (runtime *clusterRuntime) projectionReadinessSnapshot() clusterReadinessSnapshot {
	return clusterReadinessSnapshot{
		initialized:                 runtime.initialized.Load(),
		projectionFailed:            runtime.projectionFailed.Load(),
		bootstrapped:                runtime.admissionBootstrapped.Load(),
		dynamicMembership:           runtime.admissionDynamicMembership.Load(),
		localVoter:                  runtime.admissionLocalVoter.Load(),
		localPhaseVoter:             runtime.admissionLocalPhaseVoter.Load(),
		tenantQuotaProfileCommitted: runtime.hasCommittedTenantQuotaProfile(),
		releaseProfileSynced:        runtime.releaseProfileSynced(),
		projectionIndex:             runtime.projectionIndex.Load(),
		appliedIndex:                runtime.admissionAppliedIndex.Load(),
		driverFailed:                runtime.driverFailed.Load(),
	}
}

func (runtime *clusterRuntime) publishProjectionAdmissionStatus(status raftadapter.StatusDTO) {
	member, found := committedLocalIdentity(runtime.machine, runtime.config.NodeID, runtime.config.ReleaseVersion)
	runtime.admissionBootstrapped.Store(status.Bootstrapped)
	runtime.admissionDynamicMembership.Store(status.DynamicMembership)
	runtime.admissionLocalVoter.Store(containsNodeID(status.VoterIDs, runtime.config.NodeID))
	runtime.admissionLocalPhaseVoter.Store(found && member.Phase == tcpadapter.PeerPhaseVoter)
	runtime.admissionAppliedIndex.Store(status.AppliedIndex)
}

func (runtime *clusterRuntime) checkReadiness(ctx context.Context, purpose string) error {
	if ctx == nil {
		return errClusterRuntimeNotReady
	}
	storage := runtime.storage.view()
	if storage.health != clusterReplicationHealthy {
		runtime.logReadinessFailure(purpose, "readiness_storage", errClusterStorageUnhealthy, storage, 0, nil, 0, 0, false)
		return errClusterStorageUnhealthy
	}
	barrier, err := runtime.requestReadBarrier(ctx)
	if err != nil {
		runtime.logReadinessFailure(purpose, "readiness_fresh_read", err, storage, 0, nil, 0, 0, false)
		return err
	}
	var sequenceBefore uint64
	if runtime.logger != nil {
		sequenceBefore = runtime.projectionAdmissionSequence.Load()
	}
	// Use the driver-published application boundary, not Node's in-flight apply
	// index. A later Ready may already be applying while this proof completes.
	snapshot := runtime.projectionReadinessSnapshot()
	if !snapshot.satisfiedAt(barrier) {
		if runtime.logger != nil {
			sequenceAfter := runtime.projectionAdmissionSequence.Load()
			runtime.logReadinessFailure(purpose, "readiness_local_state", errClusterRuntimeNotReady, storage, barrier, &snapshot, sequenceBefore, sequenceAfter, true)
		}
		return errClusterRuntimeNotReady
	}
	return nil
}

func (runtime *clusterRuntime) beginReadBarrier(ctx context.Context) (*clusterReadRequest, error) {
	if ctx == nil {
		return nil, errClusterRuntimeNotReady
	}
	if runtime.driverFailed.Load() {
		return nil, raftadapter.ErrFailed
	}
	select {
	case <-runtime.driverStopped:
		return nil, raftadapter.ErrFailed
	default:
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], runtime.sequence.Add(1))
	key := string(raw[:])
	result := make(chan clusterReadResponse, 1)
	request := &clusterReadRequest{runtime: runtime, context: ctx, key: key, result: result}
	driverRequest := clusterDriverRequest{kind: clusterDriverRead, contextKey: key, context: ctx, readResult: result}
	select {
	case runtime.requests <- driverRequest:
		return request, nil
	case <-runtime.driverStopped:
		return nil, raftadapter.ErrFailed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (request *clusterReadRequest) wait() (uint64, error) {
	select {
	case response := <-request.result:
		return response.index, response.err
	case <-request.runtime.driverStopped:
		select {
		case response := <-request.result:
			return response.index, response.err
		default:
			return 0, raftadapter.ErrFailed
		}
	case <-request.context.Done():
		select {
		case request.runtime.requests <- clusterDriverRequest{kind: clusterDriverCancelRead, contextKey: request.key}:
		default:
		}
		return 0, request.context.Err()
	}
}

func (runtime *clusterRuntime) requestReadBarrier(ctx context.Context) (uint64, error) {
	request, err := runtime.beginReadBarrier(ctx)
	if err != nil {
		return 0, err
	}
	return request.wait()
}

func (runtime *clusterRuntime) readBarrier(ctx context.Context) error {
	if ctx == nil {
		return errClusterRuntimeNotReady
	}
	checkCtx := ctx
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok {
		checkCtx, cancel = context.WithTimeout(ctx, clusterReadTimeout)
		defer cancel()
	}
	_, err := runtime.requestReadBarrier(checkCtx)
	return err
}

func (runtime *clusterRuntime) Commit(ctx context.Context, command membership.CommandDTO) (membership.ApplyResultDTO, error) {
	pending, err := runtime.beginCommit(ctx, command, nil)
	if err != nil {
		return membership.ApplyResultDTO{}, err
	}
	return pending.Wait()
}

type clusterCommitStage struct {
	context        context.Context
	held           bool
	err            error
	barrierFailure bool
}

type clusterPendingCommit struct {
	runtime        *clusterRuntime
	context        context.Context
	readContext    context.Context
	readCancel     context.CancelFunc
	deliveryCancel context.CancelFunc
	backlogEntry   *list.Element
	contextKey     string
	command        membership.CommandDTO
	data           []byte
	requestID      string
	readProofIndex uint64
	staged         chan clusterCommitStage
	result         chan clusterApplyResponse
	// Driver-owned; Wait only receives the immutable stage through staged.
	readStartedAt time.Time
	readConfirmed bool
	readErr       error
}

func (pending *clusterPendingCommit) awaitCommitStage() clusterCommitStage {
	select {
	case stage := <-pending.staged:
		return stage
	default:
	}
	select {
	case stage := <-pending.staged:
		return stage
	case <-pending.runtime.driverStopped:
		select {
		case stage := <-pending.staged:
			return stage
		default:
			return clusterCommitStage{err: raftadapter.ErrFailed, barrierFailure: true}
		}
	case <-pending.readContext.Done():
		select {
		case stage := <-pending.staged:
			return stage
		default:
			return clusterCommitStage{err: pending.readContext.Err(), barrierFailure: true}
		}
	}
}

func (pending *clusterPendingCommit) Wait() (membership.ApplyResultDTO, error) {
	defer pending.runtime.localCommits.Add(-1)
	if pending.deliveryCancel != nil {
		defer pending.deliveryCancel()
	}
	stage := pending.awaitCommitStage()
	if pending.readCancel != nil {
		pending.readCancel()
	}
	if stage.err != nil {
		if stage.barrierFailure {
			pending.runtime.logCommitFailure("read_barrier", pending.readContext, pending.command, stage.err)
			return membership.ApplyResultDTO{}, classifyClusterPortError(stage.err)
		}
		return membership.ApplyResultDTO{}, stage.err
	}
	if stage.held {
		// Test-only held commands must not block driver progress or ticks.
		label, _ := pending.runtime.faults.holdCommand(pending.command)
		proposalCtx, cancel := context.WithTimeout(context.WithoutCancel(pending.context), clusterHeldProposalTimeout)
		defer cancel()
		select {
		case pending.runtime.requests <- clusterDriverRequest{
			kind: clusterDriverProposal, data: pending.data, applyResult: pending.result, pending: pending,
		}:
		case <-pending.runtime.driverStopped:
			pending.runtime.removeCommitBacklog(pending.backlogEntry)
			pending.runtime.faults.reportHeldResult(label, membership.ApplyResultDTO{}, membership.ErrQuorumUnavailable)
			return membership.ApplyResultDTO{}, membership.ErrQuorumUnavailable
		case <-proposalCtx.Done():
			pending.runtime.removeCommitBacklog(pending.backlogEntry)
			pending.runtime.faults.reportHeldResult(label, membership.ApplyResultDTO{}, proposalCtx.Err())
			return membership.ApplyResultDTO{}, membership.ErrQuorumUnavailable
		}
		result, err := pending.runtime.waitProposal(proposalCtx, pending.result, pending.command)
		pending.runtime.faults.reportHeldResult(label, result, err)
		if err == nil {
			pending.runtime.reportTenantQuotaRejection(result.TenantQuota)
			pending.runtime.reportDeliverySaturation(result)
		}
		return result, err
	}
	result, err := pending.runtime.waitProposal(stage.context, pending.result, pending.command)
	if err == nil {
		pending.runtime.reportTenantQuotaRejection(result.TenantQuota)
		pending.runtime.reportDeliverySaturation(result)
	}
	return result, err
}

func (runtime *clusterRuntime) beginCommit(ctx context.Context, command membership.CommandDTO, backlogEntry *list.Element) (*clusterPendingCommit, error) {
	runtime.localCommits.Add(1)
	submitted := false
	var readCancel context.CancelFunc
	defer func() {
		if !submitted {
			runtime.localCommits.Add(-1)
			if readCancel != nil {
				readCancel()
			}
		}
	}()
	if ctx == nil {
		runtime.logCommitFailure("invalid_context", ctx, command, membership.ErrQuorumUnavailable)
		return nil, membership.ErrQuorumUnavailable
	}
	if runtime.driverFailed.Load() {
		runtime.logCommitFailure("read_barrier", ctx, command, raftadapter.ErrFailed)
		return nil, membership.ErrQuorumUnavailable
	}
	select {
	case <-runtime.driverStopped:
		runtime.logCommitFailure("read_barrier", ctx, command, raftadapter.ErrFailed)
		return nil, membership.ErrQuorumUnavailable
	default:
	}
	if runtime.storage.view().health == clusterReplicationFailed {
		runtime.logCommitFailure("storage_failed", ctx, command, membership.ErrQuorumUnavailable)
		return nil, membership.ErrQuorumUnavailable
	}
	requestID := ""
	if command.Proposal != nil && command.Proposal.RequestID != "" {
		requestID = command.Proposal.RequestID
	}
	data, err := encodeMembershipCommand(command)
	if err != nil {
		return nil, err
	}
	readContext := ctx
	if _, deadline := ctx.Deadline(); !deadline {
		readContext, readCancel = context.WithTimeout(ctx, clusterReadTimeout)
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], runtime.sequence.Add(1))
	pending := &clusterPendingCommit{
		runtime: runtime, context: ctx, readContext: readContext, readCancel: readCancel,
		contextKey: string(raw[:]), command: command, data: data, requestID: requestID,
		staged: make(chan clusterCommitStage, 1), result: make(chan clusterApplyResponse, 1),
		backlogEntry: backlogEntry,
	}
	select {
	case runtime.requests <- clusterDriverRequest{kind: clusterDriverCommit, pending: pending, queuedAt: time.Now()}:
		submitted = true
		return pending, nil
	case <-runtime.driverStopped:
		runtime.logCommitFailure("read_barrier", ctx, command, raftadapter.ErrFailed)
		return nil, membership.ErrQuorumUnavailable
	case <-readContext.Done():
		runtime.logCommitFailure("read_barrier", ctx, command, readContext.Err())
		return nil, classifyClusterPortError(readContext.Err())
	}
}
func classifyCommitFailure(err error) string {
	switch {
	case err == nil:
		return "unspecified"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, membership.ErrNotLeader):
		return "not_leader"
	case errors.Is(err, raftadapter.ErrFailed):
		return "raft_failed"
	case errors.Is(err, membership.ErrQuorumUnavailable):
		return "quorum_unavailable"
	default:
		return "other"
	}
}

func (runtime *clusterRuntime) commitFailureLogSummary(stage, class, commandKind string, now time.Time) (clusterCommitFailureSummary, bool) {
	key := clusterCommitFailureKey{stage: stage, class: class, commandKind: commandKind}
	runtime.commitFailureMu.Lock()
	defer runtime.commitFailureMu.Unlock()
	if runtime.commitFailureLogs == nil {
		runtime.commitFailureLogs = make(map[clusterCommitFailureKey]clusterCommitFailureState)
	}
	state, exists := runtime.commitFailureLogs[key]
	if !exists {
		state = clusterCommitFailureState{
			firstAt: now, lastAt: now, lastLoggedAt: now, count: 1, reportedCount: 1,
		}
		runtime.commitFailureLogs[key] = state
		return clusterCommitFailureSummary{
			firstAt: now, lastAt: now, totalCount: 1, sinceLastLog: 1,
		}, true
	}
	state.count++
	state.lastAt = now
	if now.Sub(state.lastLoggedAt) < clusterCommitFailureLogInterval {
		runtime.commitFailureLogs[key] = state
		return clusterCommitFailureSummary{}, false
	}
	summary := clusterCommitFailureSummary{
		firstAt: state.firstAt, lastAt: state.lastAt,
		totalCount: state.count, sinceLastLog: state.count - state.reportedCount,
	}
	state.lastLoggedAt = now
	state.reportedCount = state.count
	runtime.commitFailureLogs[key] = state
	return summary, true
}

func (runtime *clusterRuntime) logCommitFailure(stage string, ctx context.Context, command membership.CommandDTO, err error) {
	if runtime == nil || runtime.logger == nil {
		return
	}
	class := classifyCommitFailure(err)
	commandKind := ""
	if command.Delivery != nil {
		commandKind = "durable_delivery"
	}
	summary, emit := runtime.commitFailureLogSummary(stage, class, commandKind, time.Now())
	if !emit {
		return
	}
	fields := []any{
		"commit_stage", stage, "failure_class", class, "error", err,
		"failure_count", summary.totalCount, "failures_since_previous_log", summary.sinceLastLog,
		"first_failure_at", summary.firstAt, "last_failure_at", summary.lastAt,
	}
	if commandKind != "" {
		fields = append(fields, "command_kind", commandKind)
	}
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			fields = append(fields, "context_error", contextErr)
		}
	}
	if runtime.node != nil {
		status := runtime.node.Status()
		fields = append(fields, "node_id", status.NodeID, "leader_id", status.LeaderID,
			"role", uint8(status.Role), "term", status.Term, "commit_index", status.CommitIndex,
			"applied_index", status.AppliedIndex, "voter_ids", status.VoterIDs, "learner_ids", status.LearnerIDs)
		if runtime.transport != nil {
			peerStatuses := make([]tcpadapter.PeerTransportStatusDTO, 0, len(status.VoterIDs)+len(status.LearnerIDs))
			peerStatusErrors := make([]string, 0)
			appendPeerStatus := func(peerID raftadapter.NodeID) {
				if peerID == status.NodeID {
					return
				}
				peerStatus, statusErr := runtime.transport.Status(uint64(peerID))
				if statusErr != nil {
					peerStatusErrors = append(peerStatusErrors, fmt.Sprintf("peer %d: %v", peerID, statusErr))
					return
				}
				peerStatuses = append(peerStatuses, peerStatus)
			}
			for _, peerID := range status.VoterIDs {
				appendPeerStatus(peerID)
			}
			for _, peerID := range status.LearnerIDs {
				appendPeerStatus(peerID)
			}
			fields = append(fields, "peer_statuses", peerStatuses)
			if len(peerStatusErrors) > 0 {
				fields = append(fields, "peer_status_errors", peerStatusErrors)
			}
		}
	}
	if runtime.storage != nil {
		storage := runtime.storage.view()
		fields = append(fields, "storage_health", storage.health, "storage_error_code", storage.lastErrorCode,
			"storage_pressure", storage.storagePressure, "storage_reason", storage.reason)
	}
	fields = append(fields, "driver_failed", runtime.driverFailed.Load())
	driverStopped := false
	if runtime.driverStopped != nil {
		select {
		case <-runtime.driverStopped:
			driverStopped = true
		default:
		}
	}
	fields = append(fields, "driver_stopped", driverStopped)
	runtime.logger.Warn("cluster command commit failed", fields...)
}

func (runtime *clusterRuntime) waitProposal(ctx context.Context, result <-chan clusterApplyResponse, command membership.CommandDTO) (membership.ApplyResultDTO, error) {
	select {
	case response := <-result:
		if response.err != nil {
			runtime.logCommitFailure("proposal_apply", ctx, command, response.err)
		}
		return clusterApplyResponseResult(response)
	case <-runtime.driverStopped:
		select {
		case response := <-result:
			if response.err != nil {
				runtime.logCommitFailure("proposal_apply", ctx, command, response.err)
			}
			return clusterApplyResponseResult(response)
		default:
			runtime.logCommitFailure("proposal", ctx, command, membership.ErrQuorumUnavailable)
			return membership.ApplyResultDTO{}, membership.ErrQuorumUnavailable
		}
	case <-ctx.Done():
		failure := context.Cause(ctx)
		if failure == nil {
			failure = membership.ErrQuorumUnavailable
		}
		runtime.logCommitFailure("proposal", ctx, command, failure)
		return membership.ApplyResultDTO{}, membership.ErrQuorumUnavailable
	}
}

func clusterApplyResponseResult(response clusterApplyResponse) (membership.ApplyResultDTO, error) {
	if response.err != nil {
		return membership.ApplyResultDTO{}, classifyClusterPortError(response.err)
	}
	return response.result, nil
}

func clusterProfilesCompatible(members []membership.MemberIdentityDTO, config clusterConfigFile) bool {
	activeMembers := 0
	for _, member := range members {
		if member.Phase == membership.MemberPhaseRetired {
			continue
		}
		activeMembers++
		if member.ClusterID != config.ClusterID ||
			!release.CompatibleStrings(member.ReleaseVersion, config.ReleaseVersion) ||
			member.ClusterProtocolMajor != config.ClusterProtocolMajor ||
			member.ClusterProtocolMinor != config.ClusterProtocolMinor ||
			member.StateSchemaMajor != config.StateSchemaMajor ||
			member.StateSchemaMinor != config.StateSchemaMinor {
			return false
		}
	}
	return activeMembers > 0
}

func (runtime *clusterRuntime) Members(ctx context.Context) ([]membership.MemberIdentityDTO, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return nil, classifyClusterPortError(err)
	}
	return runtime.machine.Members(), nil
}

func (runtime *clusterRuntime) CredentialRevocations(ctx context.Context) ([]membership.CredentialRevocationStateDTO, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return nil, classifyClusterPortError(err)
	}
	return runtime.machine.CredentialRevocations(), nil
}

func (runtime *clusterRuntime) MatchBlocklist(subject security.BlockSubject) (security.BlockKind, bool) {
	return runtime.machine.MatchBlocklist(subject)
}

func (runtime *clusterRuntime) Blocklist(ctx context.Context) (membership.BlocklistDTO, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return membership.BlocklistDTO{}, classifyClusterPortError(err)
	}
	return runtime.machine.Blocklist(), nil
}

func (runtime *clusterRuntime) IsCredentialRevoked(ctx context.Context, credentialID string) (bool, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return false, classifyClusterPortError(err)
	}
	return runtime.machine.IsCredentialRevoked(credentialID), nil
}

func (runtime *clusterRuntime) ActiveFeatures(ctx context.Context) ([]string, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return nil, classifyClusterPortError(err)
	}
	return runtime.machine.ActiveFeatures(), nil
}

func (runtime *clusterRuntime) GetOperation(ctx context.Context, operationID string) (membership.MembershipOperationDTO, bool, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return membership.MembershipOperationDTO{}, false, classifyClusterPortError(err)
	}
	operation, found := runtime.machine.GetOperation(operationID)
	return operation, found, nil
}

func (runtime *clusterRuntime) GetOperationByIdempotencyKey(ctx context.Context, key string) (membership.MembershipOperationDTO, bool, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return membership.MembershipOperationDTO{}, false, classifyClusterPortError(err)
	}
	operation, found := runtime.machine.GetOperationByIdempotencyKey(key)
	return operation, found, nil
}

func (runtime *clusterRuntime) GetOwnership(ctx context.Context, tenantID, clientID string) (membership.OwnershipStateDTO, bool, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return membership.OwnershipStateDTO{}, false, classifyClusterPortError(err)
	}
	ownership, found := runtime.machine.GetOwnership(tenantID, clientID)
	return ownership, found, nil
}
func (runtime *clusterRuntime) GetNodeDrain(ctx context.Context, memberID uint64) (membership.NodeDrainStateDTO, bool, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return membership.NodeDrainStateDTO{}, false, classifyClusterPortError(err)
	}
	state, found := runtime.machine.GetNodeDrain(memberID)
	return state, found, nil
}

func (runtime *clusterRuntime) GetConnectionRebalance(ctx context.Context) (membership.ConnectionRebalanceStateDTO, bool, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return membership.ConnectionRebalanceStateDTO{}, false, classifyClusterPortError(err)
	}
	state, found := runtime.machine.ConnectionRebalance()
	return state, found, nil
}

func (runtime *clusterRuntime) GetClusterPolicy(ctx context.Context) (membership.ClusterPolicyStateDTO, bool, error) {
	if err := runtime.readBarrier(ctx); err != nil {
		return membership.ClusterPolicyStateDTO{}, false, classifyClusterPortError(err)
	}
	state, found := runtime.machine.ClusterPolicy()
	return state, found, nil
}
func (runtime *clusterRuntime) AcquireOwnership(ctx context.Context, tenantID, clientID string, ownerNodeID uint64) (uint64, error) {
	epoch, _, err := runtime.acquireOwnership(ctx, tenantID, clientID, ownerNodeID, nil)
	return epoch, err
}

func (runtime *clusterRuntime) acquireOwnership(ctx context.Context, tenantID, clientID string, ownerNodeID uint64, session *membership.SessionConnectDTO) (uint64, *membership.SupersededWillDTO, error) {
	if ownerNodeID != runtime.config.NodeID {
		return 0, nil, membership.ErrNotLeader
	}
	ownership, found, err := runtime.GetOwnership(ctx, tenantID, clientID)
	if err != nil {
		return 0, nil, err
	}
	var expectedEpoch, epoch uint64
	action := membership.OwnershipActionAcquire
	epoch = 1
	if found {
		if ownership.Epoch == ^uint64(0) {
			return 0, nil, membership.ErrQuorumUnavailable
		}
		expectedEpoch, epoch = ownership.Epoch, ownership.Epoch+1
		if ownership.OwnerNodeID != ownerNodeID {
			action = membership.OwnershipActionTransfer
		}
	}
	operationID, err := newClusterOperationID()
	if err != nil {
		return 0, nil, membership.ErrQuorumUnavailable
	}
	result, err := runtime.Commit(ctx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: operationID,
		Ownership: &membership.OwnershipChangeDTO{
			TenantID: tenantID, ClientID: clientID, ExpectedEpoch: expectedEpoch,
			OwnerNodeID: ownerNodeID, Epoch: epoch, Action: action, Session: session,
		},
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied || result.Ownership == nil {
		return 0, nil, membership.ErrQuorumUnavailable
	}
	if runtime.failpoint.matchesOwnershipAcquireAfterCommit() {
		triggerClusterFailpoint(runtime.failpoint)
	}
	return result.Ownership.Epoch, result.SupersededWill, nil
}

func (runtime *clusterRuntime) CommitSubscription(ctx context.Context, tenantID, clientID string, ownerNodeID, epoch uint64, filter string, qos uint32, noLocal, retainAsPublished bool, retainHandling, action uint32) error {
	var subscriptionAction membership.SubscriptionAction
	switch action {
	case uint32(membership.SubscriptionActionAdd):
		subscriptionAction = membership.SubscriptionActionAdd
	case uint32(membership.SubscriptionActionRemove):
		subscriptionAction = membership.SubscriptionActionRemove
	default:
		return membership.ErrQuorumUnavailable
	}
	operationID, err := newClusterOperationID()
	if err != nil {
		return membership.ErrQuorumUnavailable
	}
	result, err := runtime.Commit(ctx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: operationID,
		Subscription: &membership.SubscriptionChangeDTO{
			TenantID: tenantID, ClientID: clientID, OwnerNodeID: ownerNodeID, Epoch: epoch,
			Filter: filter, QoS: qos, NoLocal: noLocal, RetainAsPublished: retainAsPublished,
			RetainHandling: retainHandling, Action: subscriptionAction,
		},
	})
	if err != nil {
		return err
	}
	if result.Outcome != membership.ApplyOutcomeApplied {
		return membership.ErrQuorumUnavailable
	}
	return nil
}

func classifyClusterPortError(err error) error {
	if errors.Is(err, raftadapter.ErrNotLeader) {
		return membership.ErrNotLeader
	}
	if err != nil {
		return membership.ErrQuorumUnavailable
	}
	return nil
}

func newClusterOperationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(raw[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func restoreClusterState(ctx context.Context, node *raftadapter.Node, config clusterConfigFile, bootstrap []membership.MemberIdentityDTO) (*membership.StateMachine, []raftadapter.MessageDTO, error) {
	machine := membership.NewStateMachine()
	var initialMessages []raftadapter.MessageDTO
	status := node.Status()
	if snapshotData := node.ApplicationSnapshot(); len(snapshotData) != 0 {
		snapshot, err := decodeMembershipSnapshot(snapshotData)
		if err != nil {
			return nil, nil, fmt.Errorf("decode durable typed snapshot: %w", err)
		}
		if snapshot.LastIncludedIndex > status.AppliedIndex {
			return nil, nil, errors.New("typed snapshot is ahead of raft applied index")
		}
		if err := machine.Restore(snapshot); err != nil {
			return nil, nil, fmt.Errorf("restore durable typed state: %w", err)
		}
	} else if config.Role == "bootstrap" {
		for status.AppliedIndex < 3 {
			batch, err := node.ProcessReady()
			if err != nil {
				return nil, nil, fmt.Errorf("apply initial raft bootstrap: %w", err)
			}
			if len(batch.Commands) != 0 || len(batch.ConfigurationChanges) != 0 || len(batch.SnapshotData) != 0 {
				return nil, nil, errors.New("fresh bootstrap contained application data before member initialization")
			}
			initialMessages = append(initialMessages, batch.Messages...)
			if err := node.CompleteApply(batch.ApplySequence); err != nil {
				return nil, nil, fmt.Errorf("complete initial bootstrap: %w", err)
			}
			if !batch.Progress && node.StoragePending() {
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				case <-node.StorageReady():
				}
			}
			status = node.Status()
		}
		if status.AppliedIndex < 3 || len(bootstrap) != 3 {
			return nil, nil, errors.New("bootstrap raft configuration is not committed")
		}
		if err := machine.InitializeBootstrap(3, bootstrap); err != nil {
			return nil, nil, fmt.Errorf("initialize bootstrap membership state: %w", err)
		}
	}
	if config.Role == "bootstrap" && machine.LastAppliedIndex() == 0 {
		return nil, nil, errors.New("bootstrap membership state was not initialized")
	}
	if machine.LastAppliedIndex() != 0 {
		if err := replayClusterApplied(machine, node.AppliedCommandRecords(), node.AppliedConfigurationChanges()); err != nil {
			return nil, nil, err
		}
	}
	return machine, initialMessages, nil
}

func machineInitialised(machine *membership.StateMachine) bool {
	return machine != nil && machine.LastAppliedIndex() != 0
}

func replayClusterApplied(machine *membership.StateMachine, commands []raftadapter.AppliedCommandDTO, configurations []raftadapter.CommittedConfigurationDTO) error {
	events := make([]clusterApplyEvent, 0, len(commands)+len(configurations))
	for _, command := range commands {
		events = append(events, clusterApplyEvent{index: command.Index, command: command.Data})
	}
	for index := range configurations {
		configuration := configurations[index]
		events = append(events, clusterApplyEvent{index: configuration.Index, configuration: &configuration})
	}
	sort.Slice(events, func(left, right int) bool { return events[left].index < events[right].index })
	lastApplied := machine.LastAppliedIndex()
	for index, event := range events {
		if index > 0 && events[index-1].index == event.index {
			return fmt.Errorf("duplicate durable application event at raft index %d", event.index)
		}
		if event.index <= lastApplied {
			continue
		}
		if event.configuration != nil {
			action, err := configurationAction(event.configuration.Action)
			if err != nil {
				return err
			}
			result, err := machine.ApplyConfiguration(event.index, event.configuration.OperationID, action, uint64(event.configuration.MemberID))
			if err != nil {
				return fmt.Errorf("replay committed configuration at raft index %d: %w", event.index, err)
			}
			if result.Outcome != membership.ApplyOutcomeApplied {
				return fmt.Errorf("%w at raft index %d: %s", errClusterApplyDivergence, event.index, result.Conflict)
			}
			continue
		}
		command, err := decodeMembershipCommand(event.command)
		if err != nil {
			return fmt.Errorf("decode durable command at raft index %d: %w", event.index, err)
		}
		if _, err := machine.Apply(event.index, command); err != nil {
			return fmt.Errorf("replay committed command at raft index %d: %w", event.index, err)
		}
	}
	return nil
}

func configurationAction(action raftadapter.MembershipChangeAction) (membership.ConfigurationAction, error) {
	switch action {
	case raftadapter.MembershipAddLearner:
		return membership.ConfigurationActionAddLearner, nil
	case raftadapter.MembershipPromoteVoter:
		return membership.ConfigurationActionPromoteVoter, nil
	case raftadapter.MembershipRemove:
		return membership.ConfigurationActionRemoveMember, nil
	default:
		return 0, fmt.Errorf("unsupported committed raft configuration action %d", action)
	}
}

func buildClusterPeerTLS(config clusterConfigFile) (*tls.Config, *tls.Config, [32]byte, error) {
	serverTLS, err := tlsadapter.BuildTLSConfig(tlsadapter.Config{
		CertFile: config.PeerTLS.CertFile, KeyFile: config.PeerTLS.KeyFile, CAFile: config.PeerTLS.CAFile,
		ClientAuth: tlsadapter.ClientAuthRequire, MinVersion: "1.3",
	})
	if err != nil {
		return nil, nil, [32]byte{}, fmt.Errorf("configure cluster peer TLS: %w", err)
	}
	if len(serverTLS.Certificates) == 0 || len(serverTLS.Certificates[0].Certificate) == 0 {
		return nil, nil, [32]byte{}, errors.New("cluster peer certificate is missing")
	}
	leaf, err := x509.ParseCertificate(serverTLS.Certificates[0].Certificate[0])
	if err != nil {
		return nil, nil, [32]byte{}, fmt.Errorf("parse cluster peer certificate: %w", err)
	}
	serverTLS.Certificates[0].Leaf = leaf
	clientTLS := serverTLS.Clone()
	clientTLS.ClientAuth = tls.NoClientCert
	clientTLS.RootCAs = serverTLS.ClientCAs
	clientTLS.ClientCAs = nil
	clientTLS.ServerName = ""
	clientTLS.MinVersion = tls.VersionTLS13
	clientTLS.MaxVersion = tls.VersionTLS13
	return serverTLS, clientTLS, sha256.Sum256(leaf.Raw), nil
}

func clusterBootstrapIdentities(config clusterConfigFile) ([]raftadapter.NodeID, []membership.MemberIdentityDTO, error) {
	if config.Role != "bootstrap" {
		return nil, nil, nil
	}
	ids := make([]raftadapter.NodeID, 0, len(config.BootstrapMembers))
	members := make([]membership.MemberIdentityDTO, 0, len(config.BootstrapMembers))
	for _, configured := range config.BootstrapMembers {
		fingerprint, err := hex.DecodeString(configured.CertificateSHA256)
		if err != nil || len(fingerprint) != sha256.Size {
			return nil, nil, errors.New("bootstrap certificate fingerprint is invalid")
		}
		ids = append(ids, raftadapter.NodeID(configured.MemberID))
		members = append(members, membership.MemberIdentityDTO{
			MemberID: configured.MemberID, ClusterID: config.ClusterID, Address: configured.Address,
			DNSSAN: configured.DNSSAN, CertificateSHA256: fingerprint,
			ReleaseVersion: config.ReleaseVersion, Features: slices.Clone(config.Features),
			ClusterProtocolMajor: config.ClusterProtocolMajor, ClusterProtocolMinor: config.ClusterProtocolMinor,
			StateSchemaMajor: config.StateSchemaMajor, StateSchemaMinor: config.StateSchemaMinor,
			Phase: membership.MemberPhaseVoter,
		})
	}
	return ids, members, nil
}

func configuredLocalPeerIdentity(config clusterConfigFile, fingerprint [32]byte) tcpadapter.PeerIdentityDTO {
	phase := tcpadapter.PeerPhaseStaged
	if config.Role == "bootstrap" {
		phase = tcpadapter.PeerPhaseVoter
	}
	return tcpadapter.PeerIdentityDTO{
		MemberID: config.NodeID, ClusterID: config.ClusterID, Address: config.AdvertisedAddress,
		DNSSAN: config.LocalDNSName, CertificateSHA256: fingerprint, ReleaseVersion: config.ReleaseVersion,
		ClusterProtocolMajor: config.ClusterProtocolMajor, ClusterProtocolMinor: config.ClusterProtocolMinor,
		StateSchemaMajor: config.StateSchemaMajor, StateSchemaMinor: config.StateSchemaMinor, Phase: phase,
		ObservabilitySupported: release.Contains(config.Features, release.ClusterObservabilityFeature),
	}
}

func committedLocalIdentity(machine *membership.StateMachine, memberID uint64, releaseVersion string) (tcpadapter.PeerIdentityDTO, bool) {
	for _, member := range machine.Members() {
		if member.MemberID == memberID {
			identity, err := peerIdentityFromMember(&member)
			if err != nil {
				return tcpadapter.PeerIdentityDTO{}, false
			}
			identity.ReleaseVersion = releaseVersion
			return identity, true
		}
	}
	return tcpadapter.PeerIdentityDTO{}, false
}

func peerIdentityFromMember(member *membership.MemberIdentityDTO) (tcpadapter.PeerIdentityDTO, error) {
	if member == nil || len(member.CertificateSHA256) != sha256.Size {
		return tcpadapter.PeerIdentityDTO{}, errors.New("committed member certificate fingerprint is invalid")
	}
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], member.CertificateSHA256)
	phase, err := peerPhaseFromMembership(member.Phase)
	if err != nil {
		return tcpadapter.PeerIdentityDTO{}, err
	}
	return tcpadapter.PeerIdentityDTO{
		MemberID: member.MemberID, ClusterID: member.ClusterID, Address: member.Address, DNSSAN: member.DNSSAN,
		CertificateSHA256: fingerprint, ReleaseVersion: member.ReleaseVersion,
		ClusterProtocolMajor: member.ClusterProtocolMajor, ClusterProtocolMinor: member.ClusterProtocolMinor,
		StateSchemaMajor: member.StateSchemaMajor, StateSchemaMinor: member.StateSchemaMinor,
		Phase: phase, OperationID: member.OperationID,
		ObservabilitySupported: release.Contains(member.Features, release.ClusterObservabilityFeature),
	}, nil
}

func peerPhaseFromMembership(phase membership.MemberPhase) (tcpadapter.PeerPhaseDTO, error) {
	switch phase {
	case membership.MemberPhaseStaged:
		return tcpadapter.PeerPhaseStaged, nil
	case membership.MemberPhaseLearner:
		return tcpadapter.PeerPhaseLearner, nil
	case membership.MemberPhaseCatchingUp:
		return tcpadapter.PeerPhaseCatchingUp, nil
	case membership.MemberPhaseVoter:
		return tcpadapter.PeerPhaseVoter, nil
	case membership.MemberPhaseLeaving:
		return tcpadapter.PeerPhaseLeaving, nil
	case membership.MemberPhaseRetired:
		return tcpadapter.PeerPhaseRetired, nil
	default:
		return 0, errors.New("committed member has unknown lifecycle phase")
	}
}

func committedPeerIdentities(machine *membership.StateMachine, status raftadapter.StatusDTO, localID uint64) []tcpadapter.PeerIdentityDTO {
	active := make(map[uint64]struct{}, len(status.VoterIDs)+len(status.LearnerIDs))
	for _, id := range status.VoterIDs {
		active[uint64(id)] = struct{}{}
	}
	for _, id := range status.LearnerIDs {
		active[uint64(id)] = struct{}{}
	}
	var peers []tcpadapter.PeerIdentityDTO
	for _, member := range machine.Members() {
		if member.MemberID == localID || member.Phase == membership.MemberPhaseStaged || member.Phase == membership.MemberPhaseRetired {
			continue
		}
		if _, exists := active[member.MemberID]; !exists {
			continue
		}
		identity, err := peerIdentityFromMember(&member)
		if err == nil {
			peers = append(peers, identity)
		}
	}
	sort.Slice(peers, func(left, right int) bool { return peers[left].MemberID < peers[right].MemberID })
	return peers
}
func clusterPeerSetForReleaseWindow(localRelease string, peers []tcpadapter.PeerIdentityDTO) ([]tcpadapter.PeerIdentityDTO, string) {
	const reason = "incompatible_state_schema: committed cluster release profiles are outside the supported compatibility window"
	var sawIncompatiblePeer bool
	var incompatibilityReason string
	compatiblePeers := peers[:0]
	for _, peer := range peers {
		if !release.CompatibleStrings(localRelease, peer.ReleaseVersion) {
			sawIncompatiblePeer = true
			continue
		}
		for _, previous := range compatiblePeers {
			if !release.CompatibleStrings(previous.ReleaseVersion, peer.ReleaseVersion) {
				incompatibilityReason = reason
			}
		}
		compatiblePeers = append(compatiblePeers, peer)
	}
	if sawIncompatiblePeer && len(compatiblePeers) == 0 {
		incompatibilityReason = reason
	}
	return compatiblePeers, incompatibilityReason
}

// mergeSeedAdmissionPeers keeps seed-authenticated voter identities until the
// joining node's replicated state can provide them, while dropping seeds that
// Raft no longer considers active.
func mergeSeedAdmissionPeers(
	committed []tcpadapter.PeerIdentityDTO,
	admitted []tcpadapter.PeerIdentityDTO,
	status raftadapter.StatusDTO,
	localID uint64,
) []tcpadapter.PeerIdentityDTO {
	if len(admitted) == 0 {
		return committed
	}
	active := make(map[uint64]struct{}, len(status.VoterIDs)+len(status.LearnerIDs))
	for _, id := range status.VoterIDs {
		if uint64(id) != localID {
			active[uint64(id)] = struct{}{}
		}
	}
	for _, id := range status.LearnerIDs {
		if uint64(id) != localID {
			active[uint64(id)] = struct{}{}
		}
	}
	peers := make(map[uint64]tcpadapter.PeerIdentityDTO, len(committed)+len(admitted))
	for _, identity := range committed {
		if identity.MemberID != localID {
			peers[identity.MemberID] = identity
		}
	}
	for _, identity := range admitted {
		if identity.MemberID == localID {
			continue
		}
		if len(active) != 0 {
			if _, exists := active[identity.MemberID]; !exists {
				continue
			}
		}
		if _, exists := peers[identity.MemberID]; !exists {
			peers[identity.MemberID] = identity
		}
	}
	result := make([]tcpadapter.PeerIdentityDTO, 0, len(peers))
	for _, identity := range peers {
		result = append(result, identity)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].MemberID < result[right].MemberID
	})
	return result
}

func containsNodeID(ids []raftadapter.NodeID, target uint64) bool {
	for _, id := range ids {
		if uint64(id) == target {
			return true
		}
	}
	return false
}
func isUnconfiguredRemotePeer(status raftadapter.StatusDTO, localID uint64, peerID raftadapter.NodeID) bool {
	return peerID != 0 && uint64(peerID) != localID &&
		!containsNodeID(status.VoterIDs, uint64(peerID)) &&
		!containsNodeID(status.LearnerIDs, uint64(peerID))
}

func clusterMQTTListeners(config clusterConfigFile, recorder portsmetrics.ListenerAdmissionRecorder, logger *slog.Logger) ([]net.Listener, error) {
	var tlsConfig *tls.Config
	if config.MQTT.tlsListenerConfigured() {
		if config.MQTT.TLS == nil {
			return nil, errors.New("cluster MQTT TLS listener requires mqtt.tls")
		}
		var err error
		tlsConfig, err = tlsadapter.BuildTLSConfig(tlsadapter.Config{
			CertFile: config.MQTT.TLS.CertFile, KeyFile: config.MQTT.TLS.KeyFile,
			CAFile: config.MQTT.TLS.CAFile, ClientAuth: tlsadapter.ClientAuthMode(config.MQTT.TLS.ClientAuth),
			MinVersion: config.MQTT.TLS.MinVersion,
		})
		if err != nil {
			return nil, fmt.Errorf("configure cluster MQTT TLS: %w", err)
		}
	}

	var listeners []net.Listener
	closeOpened := func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}
	if config.MQTT.TCPListenAddress != "" {
		listener, err := openMQTTListener(portsmetrics.TransportTCP, config.MQTT.TCPListenAddress, nil, config.MQTT.TCPMaxConnections, config.MQTT.TCPConnectionRate, recorder)
		if err != nil {
			return nil, fmt.Errorf("listen for cluster MQTT TCP on %q: %w", config.MQTT.TCPListenAddress, err)
		}
		listeners = append(listeners, listener)
	}
	if config.MQTT.TLSListenAddress != "" {
		listener, err := openMQTTListener(portsmetrics.TransportTLS, config.MQTT.TLSListenAddress, tlsConfig, config.MQTT.TLSMaxConnections, config.MQTT.TLSConnectionRate, recorder)
		if err != nil {
			closeOpened()
			return nil, fmt.Errorf("listen for cluster MQTT TLS on %q: %w", config.MQTT.TLSListenAddress, err)
		}
		listeners = append(listeners, listener)
	}
	if webSocket := config.MQTT.WebSocket; webSocket != nil && webSocket.ListenAddress != "" {
		listener, err := openWebSocketListener(webSocket.ListenAddress, nil, webSocket.MaxConnections, webSocket.ConnectionRate, recorder, logger)
		if err != nil {
			closeOpened()
			return nil, fmt.Errorf("listen for cluster MQTT WebSocket on %q: %w", webSocket.ListenAddress, err)
		}
		listeners = append(listeners, listener)
	}
	if webSocket := config.MQTT.WebSocket; webSocket != nil && webSocket.TLSListenAddress != "" {
		listener, err := openWebSocketListener(webSocket.TLSListenAddress, tlsConfig, webSocket.TLSMaxConnections, webSocket.TLSConnectionRate, recorder, logger)
		if err != nil {
			closeOpened()
			return nil, fmt.Errorf("listen for cluster MQTT WSS on %q: %w", webSocket.TLSListenAddress, err)
		}
		listeners = append(listeners, listener)
	}
	return listeners, nil
}

func clusterMQTTSecurity(config clusterConfigFile) (*SecurityBundle, authenticationports.AuthenticationProvider, *authenticationadapter.MTLSAuthenticationProvider, string, error) {
	bundle, err := loadSecurityConfigFile(config.MQTT.SecurityConfigFile)
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("load cluster MQTT security config: %w", err)
	}
	if config.MQTT.tlsListenerConfigured() && config.MQTT.TLS != nil {
		if err := validateTLSAuthenticationBindings(
			"mqtt.tls.clientAuth",
			tlsadapter.ClientAuthMode(config.MQTT.TLS.ClientAuth),
			bundle,
		); err != nil {
			return nil, nil, nil, "", err
		}
	}

	var (
		credentialProvider authenticationports.AuthenticationProvider
		credentialMethod   string
		mtlsProvider       *authenticationadapter.MTLSAuthenticationProvider
	)
	if bundle.JWTConfig != nil {
		jwtConfig := *bundle.JWTConfig
		jwtConfig.HTTPClient = &http.Client{Timeout: jwtConfig.HTTPTimeout}
		jwtConfig.Clock = clock.SystemClock{}
		provider, err := authenticationadapter.NewJWKSAuthenticationProvider(jwtConfig)
		if err != nil {
			return nil, nil, nil, "", fmt.Errorf("create cluster MQTT JWT provider: %w", err)
		}
		credentialProvider = provider
		credentialMethod = portsmetrics.AuthMethodJWT
	}
	if len(bundle.PasswordBindings) != 0 {
		provider, err := authenticationadapter.NewPasswordAuthenticationProvider(bundle.PasswordBindings)
		if err != nil {
			return nil, nil, nil, "", fmt.Errorf("create cluster MQTT password provider: %w", err)
		}
		credentialProvider = provider
		credentialMethod = portsmetrics.AuthMethodPassword
	}
	if len(bundle.MTLSBindings) != 0 {
		mtlsProvider, err = authenticationadapter.NewMTLSAuthenticationProvider(clock.SystemClock{}, bundle.MTLSBindings)
		if err != nil {
			return nil, nil, nil, "", fmt.Errorf("create cluster MQTT mTLS provider: %w", err)
		}
	}
	return bundle, credentialProvider, mtlsProvider, credentialMethod, nil
}

type clusterHealthChecker struct {
	managementports.HealthChecker
	runtime *clusterRuntime
}

func (checker *clusterHealthChecker) CheckReadiness(ctx context.Context) (managementports.HealthStatus, *string) {
	view := checker.runtime.storage.view()
	if view.health == clusterReplicationFailed {
		return managementports.HealthStatusNotReady, &view.reason
	}
	if checker.runtime.drain != nil && !checker.runtime.drain.admissionOpen() {
		reason := "node drain record is active"
		return managementports.HealthStatusNotReady, &reason
	}
	if checker.runtime.rebalance != nil && !checker.runtime.rebalance.admissionOpen() {
		reason := "connection rebalance shedding"
		return managementports.HealthStatusNotReady, &reason
	}
	if checker.runtime.managementState.NodeState() == managementports.NodeStateDraining ||
		checker.runtime.managementState.NodeState() == managementports.NodeStateSafeToStop {
		reason := "node is draining"
		return managementports.HealthStatusNotReady, &reason
	}
	if view.health == clusterReplicationDegraded {
		return managementports.HealthStatusNotReady, &view.reason
	}
	if reason := checker.runtime.releaseProfileIncompatibleReason(); reason != nil {
		checker.runtime.managementState.SetNodeState(managementports.NodeStateJoining, reason)
		return managementports.HealthStatusNotReady, reason
	}
	if ctx != nil {
		checkCtx, cancel := context.WithTimeout(ctx, clusterReadTimeout)
		defer cancel()
		ctx = checkCtx
	}
	if err := checker.runtime.checkReadiness(ctx, "health"); err != nil {
		reason := "cluster quorum, committed voter role, route projection, tenant quota profile, or release profile is not ready"
		checker.runtime.managementState.SetNodeState(managementports.NodeStateJoining, &reason)
		return managementports.HealthStatusNotReady, &reason
	}
	checker.runtime.managementState.SetNodeState(managementports.NodeStateReady, nil)
	return managementports.HealthStatusUp, nil
}

func (checker *clusterHealthChecker) CheckLiveness(ctx context.Context) (managementports.HealthStatus, *string) {
	view := checker.runtime.storage.view()
	if view.health == clusterReplicationFailed {
		return managementports.HealthStatusDown, &view.reason
	}
	return checker.HealthChecker.CheckLiveness(ctx)
}

func (checker *clusterHealthChecker) CheckStartup(ctx context.Context) (managementports.HealthStatus, *string) {
	if !checker.runtime.initialized.Load() {
		reason := "typed raft state has not been recovered"
		return managementports.HealthStatusStarting, &reason
	}
	status, reason := checker.HealthChecker.CheckStartup(ctx)
	if status != managementports.HealthStatusStarting {
		return status, reason
	}
	if ready, readinessReason := checker.CheckReadiness(ctx); ready != managementports.HealthStatusUp {
		return managementports.HealthStatusStarting, readinessReason
	}
	return managementports.HealthStatusUp, nil
}

func (checker *clusterHealthChecker) CheckDrain(ctx context.Context) (managementports.HealthStatus, *string) {
	return checker.HealthChecker.CheckDrain(ctx)
}

type clusterStateProvider struct {
	managementports.StateProvider
	runtime *clusterRuntime
}

func (provider *clusterStateProvider) GetClusterSummary(ctx context.Context) managementports.ClusterSummaryDTO {
	summary := provider.StateProvider.GetClusterSummary(ctx)
	status := provider.runtime.node.Status()
	activeMembers := 0
	joining := 0
	ready := 0
	draining := 0
	safeToStop := 0
	for _, member := range provider.runtime.machine.Members() {
		if member.Phase == membership.MemberPhaseRetired {
			continue
		}
		activeMembers++
		if drain, exists := provider.runtime.machine.GetNodeDrain(member.MemberID); exists && drain.Phase.Active() {
			switch nodedrain.LifecycleOf(drain.Phase) {
			case nodedrain.LifecycleDraining:
				draining++
				continue
			case nodedrain.LifecycleSafeToStop:
				safeToStop++
				continue
			}
		}
		if member.MemberID == provider.runtime.config.NodeID && provider.runtime.hasFreshProof() && member.Phase == membership.MemberPhaseVoter {
			ready++
		} else {
			joining++
		}
	}
	summary.ClusterID = provider.runtime.config.ClusterID
	summary.ReleaseVersion = provider.runtime.config.ReleaseVersion
	summary.MemberCount = activeMembers
	summary.QuorumSize = len(status.VoterIDs)/2 + 1
	profile := provider.runtime.config.replicationProfile()
	summary.ReplicationFactor = profile.Factor
	summary.WriteQuorum = profile.WriteQuorum
	summary.QuorumAvailable = provider.runtime.hasFreshProof()
	summary.NodesByState = managementports.NodesByStateDTO{
		Joining: joining, Ready: ready, Draining: draining, SafeToStop: safeToStop,
	}
	summary.UnderReplicated = len(status.LearnerIDs)
	if status.LeaderID == 0 {
		summary.LeaderNodeID = nil
	} else {
		leader := strconv.FormatUint(uint64(status.LeaderID), 10)
		summary.LeaderNodeID = &leader
	}
	return summary
}

func (provider *clusterStateProvider) GetNodeSummary(ctx context.Context, nodeID string) (*managementports.NodeSummaryDTO, error) {
	id, err := strconv.ParseUint(nodeID, 10, 64)
	if err != nil {
		return nil, appmgmt.ErrNodeNotFound
	}
	for _, member := range provider.runtime.machine.Members() {
		if member.MemberID == id {
			return provider.nodeSummary(ctx, member), nil
		}
	}
	return nil, appmgmt.ErrNodeNotFound
}

func (provider *clusterStateProvider) ListNodeSummaries(ctx context.Context, filter managementports.NodeFilter) ([]managementports.NodeSummaryDTO, *managementports.PageDTO, error) {
	if filter.Limit < 0 || filter.Limit > appmgmt.MaxPageLimit {
		return nil, nil, appmgmt.ErrInvalidPageSize
	}
	limit := filter.Limit
	if limit == 0 {
		limit = 50
	}
	offset, err := decodeClusterCursor(filter.Cursor)
	if err != nil {
		return nil, nil, appmgmt.ErrInvalidCursor
	}
	members := provider.runtime.machine.Members()
	items := make([]managementports.NodeSummaryDTO, 0, len(members))
	for _, member := range members {
		item := *provider.nodeSummary(ctx, member)
		if filter.State == "" || filter.State == item.State {
			items = append(items, item)
		}
	}
	start := offset
	if start > len(items) {
		start = len(items)
	}
	end := start + limit
	if end > len(items) {
		end = len(items)
	}
	page := &managementports.PageDTO{HasMore: end < len(items)}
	if page.HasMore {
		cursor := encodeClusterCursor(end)
		page.NextCursor = &cursor
	}
	return items[start:end], page, nil
}

func (provider *clusterStateProvider) nodeSummary(ctx context.Context, member membership.MemberIdentityDTO) *managementports.NodeSummaryDTO {
	id := strconv.FormatUint(member.MemberID, 10)
	status := provider.runtime.node.Status()
	state := managementports.NodeStateJoining
	ready := false
	var reason *string
	role := "LEARNER"
	applied := int64(0)
	connections := 0
	uptime := int64(0)
	var replicationHealth *string
	var lastErrorCode *string
	var replicationQueue *managementports.ReplicationQueueDTO
	storagePressure := false
	if containsNodeID(status.VoterIDs, member.MemberID) {
		role = "FOLLOWER"
		if uint64(status.LeaderID) == member.MemberID {
			role = "LEADER"
		}
	}
	if member.Phase == membership.MemberPhaseRetired {
		role = "RETIRED"
		state = managementports.NodeStateSafeToStop
	} else if member.MemberID == provider.runtime.config.NodeID {
		base, _ := provider.StateProvider.GetNodeSummary(ctx, id)
		if base != nil {
			state, ready, reason, connections, uptime = base.State, base.Ready, base.ReadinessReason, base.ConnectionCount, base.UptimeMillis
		}
		applied = int64(status.AppliedIndex)
		view := provider.runtime.storage.view()
		replicationHealth = &view.health
		if view.health != clusterReplicationHealthy {
			ready = false
			reason = &view.reason
		}
		if view.lastErrorCode != "" {
			code := view.lastErrorCode
			lastErrorCode = &code
		}
		storagePressure = view.storagePressure || provider.runtime.publishPressure.diskPressureActive()
	} else {
		message := "remote readiness is not available from this node"
		if drain, exists := provider.runtime.machine.GetNodeDrain(member.MemberID); exists && drain.Phase.Active() {
			state = managementports.NodeState(nodedrain.LifecycleOf(drain.Phase))
			message = "node drain record is active"
		}
		reason = &message
		if match, ok := provider.runtime.node.PeerMatchIndex(raftadapter.NodeID(member.MemberID)); ok {
			applied = int64(match)
		}
		if peerStatus, err := provider.runtime.transport.Status(member.MemberID); err == nil {
			health, errorCode, queue := remoteReplicationView(peerStatus)
			replicationHealth = &health
			lastErrorCode = errorCode
			replicationQueue = queue
		}
	}
	return &managementports.NodeSummaryDTO{
		NodeID: id, Version: member.ReleaseVersion, UptimeMillis: uptime, State: state, Ready: ready,
		ReadinessReason: reason, Role: role, LastAppliedIndex: applied, ConnectionCount: connections,
		StoragePressure: storagePressure, ReplicationHealth: replicationHealth,
		LastErrorCode: lastErrorCode, ReplicationQueue: replicationQueue,
	}
}

func remoteReplicationView(status tcpadapter.PeerTransportStatusDTO) (string, *string, *managementports.ReplicationQueueDTO) {
	health := clusterReplicationUnreachable
	if status.Connected {
		health = clusterReplicationHealthy
	}
	var errorCode *string
	switch status.LastError {
	case "":
	case "peer disconnected":
		code := clusterErrorPeerDisconnected
		errorCode = &code
	case "outbound queue full":
		code := clusterErrorPeerQueueFull
		errorCode = &code
	case "invalid peer message":
		code := clusterErrorPeerInvalidMessage
		errorCode = &code
	default:
		code := clusterErrorPeerTransport
		errorCode = &code
	}
	queue := &managementports.ReplicationQueueDTO{
		OutboundMessages: status.OutboundQueuedMessages,
		OutboundBytes:    status.OutboundQueuedBytes,
		RejectedMessages: status.RejectedMessages,
	}
	return health, errorCode, queue
}

func decodeClusterCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	value := string(decoded)
	if !strings.HasPrefix(value, "cur:") {
		return 0, errors.New("invalid cursor")
	}
	offset, err := strconv.Atoi(strings.TrimPrefix(value, "cur:"))
	if err != nil || offset < 0 {
		return 0, errors.New("invalid cursor")
	}
	return offset, nil
}

func encodeClusterCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte("cur:" + strconv.Itoa(offset)))
}

var _ membership.CommandPort = (*clusterRuntime)(nil)
var _ membership.ClusterPolicyPort = (*clusterRuntime)(nil)
var _ connect.OwnershipCoordinator = (*clusterRuntime)(nil)
var _ subscribe.SubscriptionCommitter = (*clusterRuntime)(nil)
var _ managementports.HealthChecker = (*clusterHealthChecker)(nil)
var _ managementports.StateProvider = (*clusterStateProvider)(nil)
