package main

import (
	"bytes"
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
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	authenticationadapter "narumq/internal/adapters/authentication"
	"narumq/internal/adapters/clock"
	adaptershttp "narumq/internal/adapters/http/management"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	adaptersmetrics "narumq/internal/adapters/metrics"
	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
	tlsadapter "narumq/internal/adapters/tls"
	"narumq/internal/application/connect"
	appmgmt "narumq/internal/application/management"
	"narumq/internal/application/membership"
	managementports "narumq/internal/application/ports/management"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

const (
	clusterReadTimeout       = 2 * time.Second
	clusterTickInterval      = 100 * time.Millisecond
	clusterSnapshotInterval  = 1000
	clusterDefaultPacketSize = 1 << 20
)

var (
	errClusterRuntimeNotReady           = errors.New("cluster runtime is not ready")
	errClusterStorageUnhealthy          = errors.New("cluster raft storage is not healthy")
	errClusterApplyDivergence           = errors.New("committed cluster state diverges from membership state")
	errClusterExactProfileCompatibility = errors.New("retained, offline, and durable delivery replication requires every active member to match the local release and schema")
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

	var persistenceHook func(raftadapter.PersistenceBoundaryDTO)
	if failpoint != clusterFailpointNone {
		persistenceHook = func(boundary raftadapter.PersistenceBoundaryDTO) {
			if failpoint.matchesPersistence(boundary) {
				triggerClusterFailpoint(failpoint)
			}
		}
	}

	node, err := raftadapter.Open(raftadapter.Config{
		NodeID: raftadapter.NodeID(config.NodeID), Directory: config.Directory,
		DynamicMembership: true, ClusterID: config.ClusterID,
		CertificateSHA256: localFingerprint, BootstrapVoterIDs: bootstrapIDs,
		PersistenceBoundaryHook: persistenceHook,
	})
	if err != nil {
		return fmt.Errorf("open dynamic raft node: %w", err)
	}
	defer node.Close()

	machine, initialMessages, err := restoreClusterState(node, config, bootstrapMembers)
	if err != nil {
		return err
	}
	peerListener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for cluster peers on %q: %w", config.ListenAddress, err)
	}
	listeners := []net.Listener{peerListener}
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()

	metricsRegistry := adaptersmetrics.NewRegistry()
	retainedRepository, err := memoryretained.New(membership.MaxRetainedMessages, membership.MaxRetainedBytes, metricsRegistry)
	if err != nil {
		return fmt.Errorf("create retained repository: %w", err)
	}
	runtime := &clusterRuntime{
		config: config, logger: logger, node: node, machine: machine,
		retained: retainedRepository,
		storage:  newClusterStorageMonitor(time.Now, logger),
		routes:   routing.NewTable(),
		requests: make(chan clusterDriverRequest, 128), initialMessages: initialMessages,
		localIdentity:      configuredLocalPeerIdentity(config, localFingerprint),
		admittedCandidates: make(map[uint64]string),
		failpoint:          failpoint,
		handoffProposed:    make(map[string]uint64),
	}
	runtime.retainedResyncRequired.Store(true)
	if local, found := committedLocalIdentity(machine, config.NodeID); found {
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

	transport, err := tcpadapter.NewDynamicTCPPeerTransport(tcpadapter.DynamicPeerTransportConfig{
		LocalIdentity: runtime.localIdentity, Listener: peerListener,
		ServerTLS: peerServerTLS, ClientTLS: peerClientTLS,
		Peers:            committedPeerIdentities(machine, node.Status(), config.NodeID),
		AdmissionHandler: runtime.admitPeer,
	})
	if err != nil {
		return fmt.Errorf("create dynamic raft peer transport: %w", err)
	}
	runtime.transport = transport
	runtime.forwarder = transport
	runtime.refreshRouteProjection()

	securityBundle, jwtProvider, mtlsProvider, err := clusterMQTTSecurity(config)
	if err != nil {
		return err
	}
	mqttListeners, err := clusterMQTTListeners(config)
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
	memoryDeliveries, err := deliveryqueue.New(64)
	if err != nil {
		return fmt.Errorf("create delivery queue: %w", err)
	}
	runtime.localDeliveries = memoryDeliveries
	deliveries := &clusterDurableQueue{Queue: runtime.localDeliveries, runtime: runtime, clock: systemClock}
	sessions := sessionstore.New()
	willBridge := tcpadapter.NewWillBridge()
	systemScheduler := clock.SystemScheduler{}
	connectOptions := []connect.Option{
		connect.WithMetrics(metricsRegistry),
		connect.WithOwnershipCoordinator(runtime, config.NodeID),
		connect.WithOfflineCoordinator(runtime),
		connect.WithDurableDeliveryCommitter(runtime),
	}
	subscribeOptions := []subscribe.Option{
		subscribe.WithMetrics(metricsRegistry),
		subscribe.WithCommitter(runtime),
	}
	publishOptions := []publish.Option{
		publish.WithMetrics(metricsRegistry), publish.WithRetainedCommitter(runtime),
		publish.WithOfflineCommitter(runtime), publish.WithRemoteForwarder(runtime),
		publish.WithDurableDeliveryCommitter(runtime),
	}
	var tcpPolicy security.Policy
	if securityBundle != nil {
		connectOptions = append(connectOptions, connect.WithSecurity(jwtProvider, mtlsProvider, securityBundle.Policy))
		subscribeOptions = append(subscribeOptions, subscribe.WithPolicy(securityBundle.Policy))
		publishOptions = append(publishOptions, publish.WithPolicy(securityBundle.Policy))
		tcpPolicy = securityBundle.Policy
	}
	connections, err := connect.New(systemClock, sessions, deliveries, runtime.routes, systemScheduler, willBridge, connectOptions...)
	if err != nil {
		return fmt.Errorf("create cluster connect use case: %w", err)
	}
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
		decoder, err := mqtt.NewDecoder(clusterDefaultPacketSize)
		if err != nil {
			return fmt.Errorf("create cluster MQTT decoder: %w", err)
		}
		encoder, err := mqtt.NewEncoder(clusterDefaultPacketSize)
		if err != nil {
			return fmt.Errorf("create cluster MQTT encoder: %w", err)
		}
		mqttServer, err = tcpadapter.NewServer(mqttListeners[0], tcpadapter.Config{
			Clock: systemClock, MaxPacketSize: clusterDefaultPacketSize,
			ConnectTimeout: 10 * time.Second, WriteTimeout: 5 * time.Second,
			ControlQueueCapacity: 16, ClientCertificateExtractor: tlsadapter.ExtractVerifiedClientCertificateDTO,
			Policy: tcpPolicy, Metrics: metricsRegistry, ClusterMode: true,
			LocalMemberID: config.NodeID, ClusterReady: runtime.clusterReady,
		}, decoder, encoder, connections, subscriptions, publications, deliveries, logger)
		if err != nil {
			return fmt.Errorf("create cluster MQTT server: %w", err)
		}
		runtime.mqttServer = mqttServer
		runtime.durableSink = mqttServer
		runtime.forwardSink = mqttServer
		for _, listener := range mqttListeners[1:] {
			if err := mqttServer.AddListener(listener); err != nil {
				return fmt.Errorf("add cluster MQTT listener: %w", err)
			}
		}
		willBridge.Bind(mqttServer)
	}

	membershipService, err := membership.NewService(runtime)
	if err != nil {
		return fmt.Errorf("create membership service: %w", err)
	}
	baseManagement := appmgmt.NewService(appmgmt.ServiceConfig{
		ClusterID: config.ClusterID, NodeID: strconv.FormatUint(config.NodeID, 10), ReleaseVersion: config.ReleaseVersion,
	}, sessions, runtime.routes, retainedRepository, metricsRegistry)
	runtime.managementState = baseManagement
	stateProvider := &clusterStateProvider{StateProvider: baseManagement, runtime: runtime}
	healthChecker := &clusterHealthChecker{HealthChecker: baseManagement, runtime: runtime}
	tokenBytes, err := os.ReadFile(config.Management.ClusterAdminTokenFile)
	if err != nil {
		return fmt.Errorf("read cluster admin token file: %w", err)
	}
	adminToken := strings.TrimSpace(string(tokenBytes))
	if adminToken == "" {
		return errors.New("cluster admin token file is empty")
	}
	managementServer := adaptershttp.NewServerWithMembership(adaptershttp.ServerConfig{
		ListenAddr: managementListener.Addr().String(), AuthToken: adminToken,
		AuthTokenRole: adaptershttp.RoleClusterAdmin, AllowAnonymousProbes: true,
	}, stateProvider, healthChecker, metricsRegistry, membershipService, runtime)
	return runtime.serve(ctx, mqttServer, managementServer, managementListener)
}

type clusterRuntime struct {
	config             clusterConfigFile
	logger             *slog.Logger
	node               *raftadapter.Node
	transport          *tcpadapter.DynamicTCPPeerTransport
	forwarder          clusterPeerForwarder
	forwardSink        clusterForwardSink
	machine            *membership.StateMachine
	retained           *memoryretained.Repository
	routes             *routing.Table
	mqttServer         *tcpadapter.Server
	localDeliveries    *deliveryqueue.Queue
	durableSink        clusterDurableSink
	durableMu          sync.Mutex
	durableAttachments map[routing.SessionID]clusterDurableAttachment
	storage            *clusterStorageMonitor

	requests                chan clusterDriverRequest
	sequence                atomic.Uint64
	initialized             atomic.Bool
	projectionMu            sync.Mutex
	projectionIndex         atomic.Uint64
	projectionFailed        atomic.Bool
	retainedResyncRequired  atomic.Bool
	lastSnapshotIndex       uint64
	initialMessages         []raftadapter.MessageDTO
	autoCatchupPending      string
	leadershipTransferTo    raftadapter.NodeID
	leadershipTransferTerm  uint64
	leadershipTransferTicks int

	localMu            sync.Mutex
	localIdentity      tcpadapter.PeerIdentityDTO
	admissionMu        sync.Mutex
	admittedCandidates map[uint64]string
	seedPeerMu         sync.RWMutex
	seedPeers          []tcpadapter.PeerIdentityDTO
	managementState    *appmgmt.Service
	failpoint          clusterFailpoint
	removalBlockLogged string
	handoffProposed    map[string]uint64
}

type clusterDriverRequestKind uint8

const (
	clusterDriverRead clusterDriverRequestKind = iota + 1
	clusterDriverProposal
	clusterDriverCancelRead
	clusterDriverBackup
)

type clusterDriverRequest struct {
	kind         clusterDriverRequestKind
	contextKey   string
	context      context.Context
	data         []byte
	readResult   chan clusterReadResponse
	applyResult  chan clusterApplyResponse
	backupResult chan clusterBackupImageResponse
}

type clusterReadResponse struct {
	index uint64
	err   error
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
	ctx    context.Context
	result chan clusterReadResponse
}

type clusterProposalWaiter struct {
	data   []byte
	result chan clusterApplyResponse
}

type clusterApplyEvent struct {
	index         uint64
	command       []byte
	configuration *raftadapter.CommittedConfigurationDTO
}

func (runtime *clusterRuntime) serve(parent context.Context, mqttServer *tcpadapter.Server, managementServer *http.Server, managementListener net.Listener) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	driverDone := make(chan error, 1)
	go func() { driverDone <- runtime.runDriver(ctx) }()

	var admissionDone chan error
	if runtime.config.Role == "joiner" {
		admissionDone = make(chan error, 1)
		go func() { admissionDone <- runtime.runSeedAdmission(ctx) }()
	}

	serviceDone := make(chan serviceResult, 3)
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
			if !shutdown && runtime.handleDriverFailure(err) {
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
		if identity, found := committedLocalIdentity(runtime.machine, runtime.config.NodeID); found {
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

func (runtime *clusterRuntime) runDriver(ctx context.Context) error {
	serveResult := make(chan error, 1)
	go func() { serveResult <- runtime.transport.Serve(ctx) }()
	defer runtime.transport.Close()
	ticker := time.NewTicker(clusterTickInterval)
	defer ticker.Stop()
	reads := make(map[string]clusterReadWaiter)
	var proposals []clusterProposalWaiter
	deferredReads := make([]raftadapter.ReadStateDTO, 0)
	lastLeadership := raftLeadership{}

	processReady := func() error {
		for {
			runtime.storage.begin()
			batch, err := runtime.node.ProcessReady()
			runtime.storage.end()
			if err != nil {
				return fmt.Errorf("process raft Ready: %w", err)
			}
			if clusterBatchEmpty(batch) {
				if !runtime.projectionFailed.Load() {
					runtime.projectionIndex.Store(runtime.node.Status().AppliedIndex)
				}
				deferredReads = runtime.completeReadStates(deferredReads, reads)
				return nil
			}
			if err := runtime.applyBatch(batch, &proposals); err != nil {
				return err
			}
			deferredReads = append(deferredReads, batch.ReadStates...)
			deferredReads = runtime.completeReadStates(deferredReads, reads)
			if err := runtime.sendMessages(ctx, batch.Messages); err != nil {
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
		case request := <-runtime.requests:
			switch request.kind {
			case clusterDriverRead:
				if request.context != nil && request.context.Err() != nil {
					request.readResult <- clusterReadResponse{err: request.context.Err()}
					continue
				}
				if err := runtime.node.ReadIndex([]byte(request.contextKey)); err != nil {
					request.readResult <- clusterReadResponse{err: err}
					continue
				}
				reads[request.contextKey] = clusterReadWaiter{ctx: request.context, result: request.readResult}
			case clusterDriverProposal:
				if err := runtime.node.ProposeForwarded(request.data); err != nil {
					request.applyResult <- clusterApplyResponse{err: err}
					continue
				}
				proposals = append(proposals, clusterProposalWaiter{data: request.data, result: request.applyResult})
			case clusterDriverBackup:
				if request.context != nil && request.context.Err() != nil {
					request.backupResult <- clusterBackupImageResponse{err: request.context.Err()}
					continue
				}
				request.backupResult <- runtime.captureBackupImage()
			case clusterDriverCancelRead:
				if waiter, exists := reads[request.contextKey]; exists {
					delete(reads, request.contextKey)
					waiter.result <- clusterReadResponse{err: context.Canceled}
				}
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
			delivery, received := runtime.transport.TakeInbound()
			if !received {
				continue
			}
			message := delivery.Message
			err := runtime.node.Step(raftadapter.MessageDTO{
				From: raftadapter.NodeID(message.From), To: raftadapter.NodeID(message.To), Data: message.Data,
			})
			delivery.Complete()
			if err != nil {
				if errors.Is(err, raftadapter.ErrInvalidMessage) {
					_ = runtime.transport.ResetPeer(message.From)
					continue
				}
				return fmt.Errorf("step Raft message from peer %d: %w", message.From, err)
			}
			if err := processReady(); err != nil {
				return err
			}
		case <-ticker.C:
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
			if err := processReady(); err != nil {
				return err
			}
			for key, waiter := range reads {
				if waiter.ctx != nil && waiter.ctx.Err() != nil {
					delete(reads, key)
					waiter.result <- clusterReadResponse{err: waiter.ctx.Err()}
				}
			}
			status = runtime.node.Status()
			leadership := raftLeadership{leader: status.LeaderID, term: status.Term, role: status.Role}
			if leadership != lastLeadership {
				lastLeadership = leadership
				runtime.autoCatchupPending = ""
				clear(runtime.handoffProposed)
				runtime.removalBlockLogged = ""
			}
			if err := runtime.advanceMembership(); err != nil {
				return err
			}
			if err := processReady(); err != nil {
				return err
			}
		}
	}
}

type raftLeadership struct {
	leader raftadapter.NodeID
	term   uint64
	role   raftadapter.Role
}

func clusterBatchEmpty(batch raftadapter.BatchDTO) bool {
	return len(batch.Messages) == 0 && len(batch.Commands) == 0 && len(batch.ConfigurationChanges) == 0 &&
		len(batch.SnapshotData) == 0 && len(batch.ReadStates) == 0
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
		result, err := runtime.machine.Apply(event.index, command)
		if err != nil {
			return fmt.Errorf("apply committed command at raft index %d: %w", event.index, err)
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
				runtime.mqttServer.CloseSessionForTakeover(sessionID)
			}
			runtime.removeStaleDurableAttachment(sessionID, *result.Ownership)
		}
		if command.Retained != nil && result.Outcome == membership.ApplyOutcomeApplied {
			runtime.syncRetainedProjection(*command.Retained)
		}
		if command.Delivery != nil && result.Outcome == membership.ApplyOutcomeApplied &&
			result.Delivery != nil && len(result.Delivery.Enqueued) > 0 {
			runtime.notifyDurable(command.Delivery.TenantID, result.Delivery.Enqueued)
		}
		runtime.resolveProposal(event.command, result, proposals)
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
	if len(events) != 0 {
		if err := runtime.maybeSnapshot(len(batch.ConfigurationChanges) != 0); err != nil {
			return err
		}
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

func (runtime *clusterRuntime) resolveProposal(data []byte, result membership.ApplyResultDTO, waiters *[]clusterProposalWaiter) {
	for index := range *waiters {
		if !bytes.Equal((*waiters)[index].data, data) {
			continue
		}
		waiter := (*waiters)[index]
		copy((*waiters)[index:], (*waiters)[index+1:])
		*waiters = (*waiters)[:len(*waiters)-1]
		waiter.result <- clusterApplyResponse{result: result}
		return
	}
}

func (runtime *clusterRuntime) completeReadStates(states []raftadapter.ReadStateDTO, waiters map[string]clusterReadWaiter) []raftadapter.ReadStateDTO {
	remaining := states[:0]
	status := runtime.node.Status()
	for _, state := range states {
		key := string(state.RequestContext)
		waiter, exists := waiters[key]
		if !exists {
			continue
		}
		if waiter.ctx != nil && waiter.ctx.Err() != nil {
			delete(waiters, key)
			waiter.result <- clusterReadResponse{err: waiter.ctx.Err()}
			continue
		}
		if state.Index > status.AppliedIndex || state.Index > runtime.projectionIndex.Load() {
			remaining = append(remaining, state)
			continue
		}
		delete(waiters, key)
		waiter.result <- clusterReadResponse{index: state.Index}
	}
	return remaining
}

func (runtime *clusterRuntime) sendMessages(ctx context.Context, messages []raftadapter.MessageDTO) error {
	rejected := make(map[raftadapter.NodeID]struct{}, len(messages))
	for _, message := range messages {
		// A Ready batch can contain a response to a peer removed by that same committed change.
		// Keep registered seed peers available before a joining node has a local ConfState.
		if _, statusErr := runtime.transport.Status(uint64(message.To)); errors.Is(statusErr, tcpadapter.ErrUnknownPeer) {
			continue
		}
		if _, found := rejected[message.To]; found {
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
		})
		if err == nil {
			continue
		}
		if errors.Is(err, tcpadapter.ErrPeerBackpressure) {
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
					"last_error", status.LastError)
			}
			continue
		}
		if errors.Is(err, tcpadapter.ErrPeerTransportClosed) && ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("queue raft message for peer %d: %w", message.To, err)
	}
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
				var transferee raftadapter.NodeID
				for _, voterID := range status.VoterIDs {
					if voterID != raftadapter.NodeID(runtime.config.NodeID) {
						if peerStatus, err := runtime.transport.Status(uint64(voterID)); err == nil && peerStatus.Connected {
							transferee = voterID
							break
						}
					}
				}
				if transferee == 0 {
					return nil
				}
				if err := runtime.node.TransferLeadership(transferee); err != nil {
					if errors.Is(err, raftadapter.ErrNotLeader) {
						return nil
					}
					return fmt.Errorf("transfer leadership before removing local voter: %w", err)
				}
				runtime.leadershipTransferTo = transferee
				runtime.leadershipTransferTerm = status.Term
				runtime.leadershipTransferTicks = 0
				return nil
			}
			voterIDs := make([]uint64, 0, len(status.VoterIDs))
			for _, id := range status.VoterIDs {
				voterIDs = append(voterIDs, uint64(id))
			}
			plan := membership.PlanRemovalHandoff(
				operation.MemberID, voterIDs,
				runtime.machine.Ownerships(), runtime.machine.Subscriptions(),
			)
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
	status := runtime.node.Status()
	lastApplied := runtime.machine.LastAppliedIndex()
	if status.AppliedIndex == 0 || lastApplied != status.AppliedIndex ||
		(!force && status.AppliedIndex-runtime.lastSnapshotIndex < clusterSnapshotInterval) {
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
			TenantID: tenantID, SessionID: sessionID,
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
	err := runtime.transport.ReconcilePeers(peers)
	runtime.seedPeerMu.RUnlock()
	if err != nil {
		return fmt.Errorf("reconcile committed raft peers: %w", err)
	}
	local, found := committedLocalIdentity(runtime.machine, runtime.config.NodeID)
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
		if candidateIdentity.OperationID != "" || !candidateIsVoter || candidateIsLearner {
			return tcpadapter.SeedAdmissionResponseDTO{}, errors.New("voter candidate conflicts with committed Raft membership")
		}
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
		bytes.Equal(member.CertificateSHA256, candidate.CertificateSHA256[:]) && member.ReleaseVersion == candidate.ReleaseVersion &&
		member.ClusterProtocolMajor == candidate.ClusterProtocolMajor && member.ClusterProtocolMinor == candidate.ClusterProtocolMinor &&
		member.StateSchemaMajor == candidate.StateSchemaMajor && member.StateSchemaMinor == candidate.StateSchemaMinor
}

func (runtime *clusterRuntime) hasFreshProof() bool {
	return runtime.clusterReady()
}

func (runtime *clusterRuntime) clusterReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), clusterReadTimeout)
	defer cancel()
	return runtime.checkReadiness(ctx) == nil
}

type clusterReadinessSnapshot struct {
	initialized       bool
	projectionFailed  bool
	bootstrapped      bool
	dynamicMembership bool
	localVoter        bool
	localPhaseVoter   bool
	projectionIndex   uint64
	appliedIndex      uint64
}

func (s clusterReadinessSnapshot) admitsBarrier() bool {
	return s.initialized &&
		!s.projectionFailed &&
		s.projectionIndex >= s.appliedIndex &&
		s.bootstrapped &&
		s.dynamicMembership &&
		s.localVoter &&
		s.localPhaseVoter
}

func (s clusterReadinessSnapshot) satisfiedAt(barrier uint64) bool {
	return s.admitsBarrier() && barrier <= s.appliedIndex && barrier <= s.projectionIndex
}

func (runtime *clusterRuntime) readinessSnapshot() clusterReadinessSnapshot {
	status := runtime.node.Status()
	member, found := committedLocalIdentity(runtime.machine, runtime.config.NodeID)
	return clusterReadinessSnapshot{
		initialized:       runtime.initialized.Load(),
		projectionFailed:  runtime.projectionFailed.Load(),
		bootstrapped:      status.Bootstrapped,
		dynamicMembership: status.DynamicMembership,
		localVoter:        containsNodeID(status.VoterIDs, runtime.config.NodeID),
		localPhaseVoter:   found && member.Phase == tcpadapter.PeerPhaseVoter,
		projectionIndex:   runtime.projectionIndex.Load(),
		appliedIndex:      status.AppliedIndex,
	}
}

func (runtime *clusterRuntime) checkReadiness(ctx context.Context) error {
	if ctx == nil {
		return errClusterRuntimeNotReady
	}
	if runtime.storage.view().health != clusterReplicationHealthy {
		return errClusterStorageUnhealthy
	}
	barrier, err := runtime.requestReadBarrier(ctx)
	if err != nil {
		return err
	}
	if !runtime.readinessSnapshot().satisfiedAt(barrier) {
		return errClusterRuntimeNotReady
	}
	return nil
}

func (runtime *clusterRuntime) requestReadBarrier(ctx context.Context) (uint64, error) {
	if ctx == nil {
		return 0, errClusterRuntimeNotReady
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], runtime.sequence.Add(1))
	key := string(raw[:])
	result := make(chan clusterReadResponse, 1)
	request := clusterDriverRequest{kind: clusterDriverRead, contextKey: key, context: ctx, readResult: result}
	select {
	case runtime.requests <- request:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	select {
	case response := <-result:
		return response.index, response.err
	case <-ctx.Done():
		select {
		case runtime.requests <- clusterDriverRequest{kind: clusterDriverCancelRead, contextKey: key}:
		default:
		}
		return 0, ctx.Err()
	}
}

func (runtime *clusterRuntime) readBarrier(ctx context.Context) error {
	if ctx == nil {
		return errClusterRuntimeNotReady
	}
	checkCtx, cancel := context.WithTimeout(ctx, clusterReadTimeout)
	defer cancel()
	_, err := runtime.requestReadBarrier(checkCtx)
	return err
}

func (runtime *clusterRuntime) Commit(ctx context.Context, command membership.CommandDTO) (membership.ApplyResultDTO, error) {
	if ctx == nil || runtime.storage.view().health == clusterReplicationFailed {
		return membership.ApplyResultDTO{}, membership.ErrQuorumUnavailable
	}
	if err := runtime.readBarrier(ctx); err != nil {
		return membership.ApplyResultDTO{}, classifyClusterPortError(err)
	}
	if (command.Retained != nil || command.Offline != nil || command.Delivery != nil) && !clusterExactProfileCompatible(runtime.machine.Members(), runtime.config) {
		return membership.ApplyResultDTO{}, errClusterExactProfileCompatibility
	}
	data, err := encodeMembershipCommand(command)
	if err != nil {
		return membership.ApplyResultDTO{}, err
	}
	result := make(chan clusterApplyResponse, 1)
	select {
	case runtime.requests <- clusterDriverRequest{kind: clusterDriverProposal, data: data, applyResult: result}:
	case <-ctx.Done():
		return membership.ApplyResultDTO{}, membership.ErrQuorumUnavailable
	}
	select {
	case response := <-result:
		if response.err != nil {
			return membership.ApplyResultDTO{}, classifyClusterPortError(response.err)
		}
		return response.result, nil
	case <-ctx.Done():
		return membership.ApplyResultDTO{}, membership.ErrQuorumUnavailable
	}
}

func clusterExactProfileCompatible(members []membership.MemberIdentityDTO, config clusterConfigFile) bool {
	activeMembers := 0
	for _, member := range members {
		if member.Phase == membership.MemberPhaseRetired {
			continue
		}
		activeMembers++
		if member.ClusterID != config.ClusterID ||
			member.ReleaseVersion != config.ReleaseVersion ||
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

func (runtime *clusterRuntime) AcquireOwnership(ctx context.Context, tenantID, clientID string, ownerNodeID uint64) (uint64, error) {
	if ownerNodeID != runtime.config.NodeID {
		return 0, membership.ErrNotLeader
	}
	ownership, found, err := runtime.GetOwnership(ctx, tenantID, clientID)
	if err != nil {
		return 0, err
	}
	var expectedEpoch, epoch uint64
	action := membership.OwnershipActionAcquire
	epoch = 1
	if found {
		if ownership.Epoch == ^uint64(0) {
			return 0, membership.ErrQuorumUnavailable
		}
		expectedEpoch, epoch = ownership.Epoch, ownership.Epoch+1
		if ownership.OwnerNodeID != ownerNodeID {
			action = membership.OwnershipActionTransfer
		}
	}
	operationID, err := newClusterOperationID()
	if err != nil {
		return 0, membership.ErrQuorumUnavailable
	}
	result, err := runtime.Commit(ctx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: operationID,
		Ownership: &membership.OwnershipChangeDTO{
			TenantID: tenantID, ClientID: clientID, ExpectedEpoch: expectedEpoch,
			OwnerNodeID: ownerNodeID, Epoch: epoch, Action: action,
		},
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied || result.Ownership == nil {
		return 0, membership.ErrQuorumUnavailable
	}
	if runtime.failpoint.matchesOwnershipAcquireAfterCommit() {
		triggerClusterFailpoint(runtime.failpoint)
	}
	return result.Ownership.Epoch, nil
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

func restoreClusterState(node *raftadapter.Node, config clusterConfigFile, bootstrap []membership.MemberIdentityDTO) (*membership.StateMachine, []raftadapter.MessageDTO, error) {
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
		if status.AppliedIndex < 3 {
			batch, err := node.ProcessReady()
			if err != nil {
				return nil, nil, fmt.Errorf("apply initial raft bootstrap: %w", err)
			}
			if len(batch.Commands) != 0 || len(batch.ConfigurationChanges) != 0 || len(batch.SnapshotData) != 0 {
				return nil, nil, errors.New("fresh bootstrap contained application data before member initialization")
			}
			initialMessages = append(initialMessages, batch.Messages...)
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
			ReleaseVersion: config.ReleaseVersion, ClusterProtocolMajor: config.ClusterProtocolMajor,
			ClusterProtocolMinor: config.ClusterProtocolMinor, StateSchemaMajor: config.StateSchemaMajor,
			StateSchemaMinor: config.StateSchemaMinor, Phase: membership.MemberPhaseVoter,
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
	}
}

func committedLocalIdentity(machine *membership.StateMachine, memberID uint64) (tcpadapter.PeerIdentityDTO, bool) {
	for _, member := range machine.Members() {
		if member.MemberID == memberID {
			identity, err := peerIdentityFromMember(&member)
			return identity, err == nil
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

func clusterMQTTListeners(config clusterConfigFile) ([]net.Listener, error) {
	var listeners []net.Listener
	if config.MQTT.TCPListenAddress != "" {
		listener, err := net.Listen("tcp", config.MQTT.TCPListenAddress)
		if err != nil {
			return nil, fmt.Errorf("listen for cluster MQTT TCP on %q: %w", config.MQTT.TCPListenAddress, err)
		}
		listeners = append(listeners, listener)
	}
	if config.MQTT.TLSListenAddress != "" {
		tlsConfig, err := tlsadapter.BuildTLSConfig(tlsadapter.Config{
			CertFile: config.PeerTLS.CertFile, KeyFile: config.PeerTLS.KeyFile, MinVersion: "1.2",
		})
		if err != nil {
			for _, listener := range listeners {
				_ = listener.Close()
			}
			return nil, fmt.Errorf("configure cluster MQTT TLS: %w", err)
		}
		listener, err := tlsadapter.Listen("tcp", config.MQTT.TLSListenAddress, tlsConfig)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("listen for cluster MQTT TLS on %q: %w", config.MQTT.TLSListenAddress, err)
		}
		listeners = append(listeners, listener)
	}
	return listeners, nil
}

func clusterMQTTSecurity(config clusterConfigFile) (*SecurityBundle, *authenticationadapter.JWKSAuthenticationProvider, *authenticationadapter.MTLSAuthenticationProvider, error) {
	bundle, err := loadSecurityConfigFile(config.MQTT.SecurityConfigFile)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load cluster MQTT security config: %w", err)
	}
	var jwtProvider *authenticationadapter.JWKSAuthenticationProvider
	var mtlsProvider *authenticationadapter.MTLSAuthenticationProvider
	if bundle.JWTConfig != nil {
		jwtConfig := *bundle.JWTConfig
		jwtConfig.HTTPClient = &http.Client{Timeout: jwtConfig.HTTPTimeout}
		jwtConfig.Clock = clock.SystemClock{}
		jwtProvider, err = authenticationadapter.NewJWKSAuthenticationProvider(jwtConfig)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("create cluster MQTT JWT provider: %w", err)
		}
	}
	if len(bundle.MTLSBindings) != 0 {
		mtlsProvider, err = authenticationadapter.NewMTLSAuthenticationProvider(clock.SystemClock{}, bundle.MTLSBindings)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("create cluster MQTT mTLS provider: %w", err)
		}
	}
	return bundle, jwtProvider, mtlsProvider, nil
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
	if checker.runtime.managementState.NodeState() == managementports.NodeStateDraining ||
		checker.runtime.managementState.NodeState() == managementports.NodeStateSafeToStop {
		reason := "node is draining"
		return managementports.HealthStatusNotReady, &reason
	}
	if view.health == clusterReplicationDegraded {
		return managementports.HealthStatusNotReady, &view.reason
	}
	if ctx != nil {
		checkCtx, cancel := context.WithTimeout(ctx, clusterReadTimeout)
		defer cancel()
		ctx = checkCtx
	}
	if err := checker.runtime.checkReadiness(ctx); err != nil {
		reason := "cluster quorum, committed voter role, or route projection is not ready"
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
	return checker.HealthChecker.CheckStartup(ctx)
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
	for _, member := range provider.runtime.machine.Members() {
		if member.Phase == membership.MemberPhaseRetired {
			continue
		}
		activeMembers++
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
	summary.QuorumAvailable = provider.runtime.hasFreshProof()
	summary.NodesByState = managementports.NodesByStateDTO{Joining: joining, Ready: ready}
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
			state, ready, reason, connections, uptime = base.State, base.Ready, base.ReadinessReason, base.ConnectionCount, base.UptimeMs
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
		storagePressure = view.storagePressure
	} else {
		message := "remote readiness is not available from this node"
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
		NodeID: id, Version: member.ReleaseVersion, UptimeMs: uptime, State: state, Ready: ready,
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
var _ connect.OwnershipCoordinator = (*clusterRuntime)(nil)
var _ subscribe.SubscriptionCommitter = (*clusterRuntime)(nil)
var _ managementports.HealthChecker = (*clusterHealthChecker)(nil)
var _ managementports.StateProvider = (*clusterStateProvider)(nil)
