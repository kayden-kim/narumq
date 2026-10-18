package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
	appmgmt "narumq/internal/application/management"
	"narumq/internal/application/membership"
	managementports "narumq/internal/application/ports/management"
	"narumq/internal/domain/nodedrain"
)

func commitBoundaryRuntime(t *testing.T, logSink io.Writer) *clusterRuntime {
	t.Helper()
	if logSink == nil {
		logSink = io.Discard
	}
	config := validBootstrapClusterConfig()
	return &clusterRuntime{
		config:  config,
		machine: retainedTestMachine(t, config, config.ReleaseVersion),
		storage: newClusterStorageMonitor(time.Now, nil),
		logger:  slog.New(slog.NewTextHandler(logSink, nil)),
	}
}

func commitBoundaryPending(runtime *clusterRuntime, command membership.CommandDTO, readContext context.Context) *clusterPendingCommit {
	return &clusterPendingCommit{
		runtime: runtime, context: context.Background(), readContext: readContext, command: command,
		staged: make(chan clusterCommitStage, 1), result: make(chan clusterApplyResponse, 1),
	}
}

func commitBoundaryOwnershipCommand() membership.CommandDTO {
	return membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: "00000000-0000-4000-8000-000000000401",
		Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 1, Epoch: 1,
			Action: membership.OwnershipActionAcquire,
		},
	}
}

func commitBoundaryStage(t *testing.T, pending *clusterPendingCommit) clusterCommitStage {
	t.Helper()
	select {
	case stage := <-pending.staged:
		return stage
	default:
		t.Fatal("pending commit was not staged")
		return clusterCommitStage{}
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-072
func TestClusterResolveCommitReadsRejectsUnsafeCommandsBeforeProposal(t *testing.T) {
	cancelledRead, cancel := context.WithCancel(context.Background())
	cancel()
	proofErr := raftadapter.ErrNotLeader
	tests := []struct {
		name        string
		command     membership.CommandDTO
		readContext context.Context
		confirmed   bool
		readErr     error
		mutate      func(*clusterRuntime)
		wantErr     error
		wantBarrier bool
	}{
		{
			name: "expired read context", readContext: cancelledRead,
			wantErr: context.Canceled, wantBarrier: true,
		},
		{
			name: "failed read proof", readContext: context.Background(), confirmed: true, readErr: proofErr,
			wantErr: proofErr, wantBarrier: true,
		},
		{
			name: "failed storage", readContext: context.Background(), confirmed: true,
			mutate: func(runtime *clusterRuntime) {
				runtime.storage.fail(errors.Join(raftadapter.ErrFailed, raftadapter.ErrStorage))
			},
			wantErr: errClusterStorageUnhealthy, wantBarrier: true,
		},
		{
			name: "session metadata feature inactive", readContext: context.Background(), confirmed: true,
			command: membership.CommandDTO{SessionMetadata: &membership.SessionMetadataChangeDTO{}},
			wantErr: errClusterSessionMetadataInactive,
		},
		{
			name: "connection rebalance feature inactive", readContext: context.Background(), confirmed: true,
			command: membership.CommandDTO{ConnectionRebalance: &membership.ConnectionRebalanceChangeDTO{}},
			wantErr: errClusterConnectionRebalanceInactive,
		},
		{
			name: "member release profile incompatible with local profile", readContext: context.Background(), confirmed: true,
			command: membership.CommandDTO{NodeDrain: &membership.NodeDrainChangeDTO{}},
			mutate:  func(runtime *clusterRuntime) { runtime.config.ClusterProtocolMajor++ },
			wantErr: errClusterProfileCompatibility,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := commitBoundaryRuntime(t, nil)
			if test.mutate != nil {
				test.mutate(runtime)
			}
			pending := commitBoundaryPending(runtime, test.command, test.readContext)
			pending.readConfirmed, pending.readErr = test.confirmed, test.readErr
			runtime.pendingCommits = []*clusterPendingCommit{pending}

			proposed := runtime.resolveCommitReads(map[string]clusterReadWaiter{}, new([]clusterProposalWaiter))
			if proposed {
				t.Fatal("rejected command was reported as proposed")
			}
			stage := commitBoundaryStage(t, pending)
			if !errors.Is(stage.err, test.wantErr) || stage.barrierFailure != test.wantBarrier || stage.held {
				t.Fatalf("stage = %+v, want error %v barrierFailure=%v", stage, test.wantErr, test.wantBarrier)
			}
			if len(runtime.pendingCommits) != 0 {
				t.Fatalf("pending commits after rejection = %d, want 0", len(runtime.pendingCommits))
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-072
func TestClusterResolveCommitReadsKeepsSubmissionOrderBehindUnconfirmedProof(t *testing.T) {
	runtime := commitBoundaryRuntime(t, nil)
	first := commitBoundaryPending(runtime, membership.CommandDTO{}, context.Background())
	second := commitBoundaryPending(runtime, membership.CommandDTO{}, context.Background())
	second.readConfirmed, second.readErr = true, errors.New("second proof failed")
	runtime.pendingCommits = []*clusterPendingCommit{first, second}

	if runtime.resolveCommitReads(map[string]clusterReadWaiter{}, new([]clusterProposalWaiter)) {
		t.Fatal("resolveCommitReads proposed while the oldest proof was outstanding")
	}
	if len(first.staged) != 0 || len(second.staged) != 0 || len(runtime.pendingCommits) != 2 {
		t.Fatalf("later commit overtook the unconfirmed proof: staged=%d/%d pending=%d",
			len(first.staged), len(second.staged), len(runtime.pendingCommits))
	}

	firstErr := errors.New("first proof failed")
	first.readConfirmed, first.readErr = true, firstErr
	runtime.resolveCommitReads(map[string]clusterReadWaiter{}, new([]clusterProposalWaiter))
	if stage := commitBoundaryStage(t, first); !errors.Is(stage.err, firstErr) || !stage.barrierFailure {
		t.Fatalf("first stage = %+v, want its own proof failure", stage)
	}
	if stage := commitBoundaryStage(t, second); !errors.Is(stage.err, second.readErr) || !stage.barrierFailure {
		t.Fatalf("second stage = %+v, want its own proof failure", stage)
	}
	if len(runtime.pendingCommits) != 0 {
		t.Fatalf("pending commits after resolution = %d, want 0", len(runtime.pendingCommits))
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-072
func TestClusterFailQueuedDriverRequestsReleasesEveryWaiterWithDriverError(t *testing.T) {
	driverErr := errors.New("raft driver stopped")
	runtime := commitBoundaryRuntime(t, nil)
	runtime.requests = make(chan clusterDriverRequest, 8)

	owned := commitBoundaryPending(runtime, membership.CommandDTO{}, context.Background())
	runtime.pendingCommits = []*clusterPendingCommit{owned}
	projection := make(chan clusterReadResponse, 1)
	runtime.projectionRequests = []clusterDriverRequest{{
		kind: clusterDriverProjection, context: context.Background(), readResult: projection,
	}}
	ownedBackup := make(chan clusterBackupImageResponse, 1)
	runtime.backupRequests = []clusterDriverRequest{{
		kind: clusterDriverBackup, context: context.Background(), backupResult: ownedBackup,
	}}

	deferred := commitBoundaryPending(runtime, membership.CommandDTO{}, context.Background())
	runtime.deferredRequest = clusterDriverRequest{kind: clusterDriverCommit, pending: deferred}
	runtime.hasDeferredRequest = true

	read := make(chan clusterReadResponse, 1)
	queuedProjection := make(chan clusterReadResponse, 1)
	queuedCommit := commitBoundaryPending(runtime, membership.CommandDTO{}, context.Background())
	proposal := make(chan clusterApplyResponse, 1)
	proposalWithCommit := commitBoundaryPending(runtime, membership.CommandDTO{}, context.Background())
	backup := make(chan clusterBackupImageResponse, 1)
	for _, request := range []clusterDriverRequest{
		{kind: clusterDriverRead, readResult: read},
		{kind: clusterDriverProjection, readResult: queuedProjection},
		{kind: clusterDriverCommit, pending: queuedCommit},
		{kind: clusterDriverProposal, applyResult: proposal, pending: proposalWithCommit},
		{kind: clusterDriverBackup, backupResult: backup},
		{kind: clusterDriverCancelRead},
	} {
		runtime.requests <- request
	}

	runtime.failQueuedDriverRequests(driverErr)

	for name, got := range map[string]error{
		"queued read":       (<-read).err,
		"queued projection": (<-queuedProjection).err,
		"queued proposal":   (<-proposal).err,
		"queued backup":     (<-backup).err,
		"owned projection":  (<-projection).err,
		"owned backup":      (<-ownedBackup).err,
	} {
		if !errors.Is(got, driverErr) {
			t.Errorf("%s error = %v, want %v", name, got, driverErr)
		}
	}
	for name, pending := range map[string]*clusterPendingCommit{
		"owned read proof": owned, "deferred commit": deferred, "queued commit": queuedCommit,
	} {
		if stage := commitBoundaryStage(t, pending); !errors.Is(stage.err, driverErr) || !stage.barrierFailure {
			t.Errorf("%s stage = %+v, want barrier failure %v", name, stage, driverErr)
		}
	}
	if len(proposalWithCommit.staged) != 0 {
		t.Fatal("a queued proposal must not be staged as a commit")
	}
	if runtime.hasDeferredRequest || len(runtime.requests) != 0 || len(runtime.pendingCommits) != 0 {
		t.Fatalf("driver left work behind: deferred=%v queued=%d pending=%d",
			runtime.hasDeferredRequest, len(runtime.requests), len(runtime.pendingCommits))
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-072
func TestClusterPendingCommitWaitMapsStageFailuresAndLogsOnce(t *testing.T) {
	cancelledRead, cancelRead := context.WithCancel(context.Background())
	cancelRead()
	cancelledProposal, cancelProposal := context.WithCancel(context.Background())
	cancelProposal()
	applied := membership.ApplyResultDTO{Index: 42, Outcome: membership.ApplyOutcomeApplied}
	tests := []struct {
		name       string
		prepare    func(*clusterRuntime, *clusterPendingCommit)
		wantResult membership.ApplyResultDTO
		wantErr    error
		wantLog    []string
	}{
		{
			name: "applied result",
			prepare: func(_ *clusterRuntime, pending *clusterPendingCommit) {
				pending.staged <- clusterCommitStage{context: context.Background()}
				pending.result <- clusterApplyResponse{result: applied}
			},
			wantResult: applied,
		},
		{
			name: "read proof lost leadership",
			prepare: func(_ *clusterRuntime, pending *clusterPendingCommit) {
				pending.staged <- clusterCommitStage{err: raftadapter.ErrNotLeader, barrierFailure: true}
			},
			wantErr: membership.ErrNotLeader, wantLog: []string{"commit_stage=read_barrier"},
		},
		{
			name: "read proof deadline",
			prepare: func(_ *clusterRuntime, pending *clusterPendingCommit) {
				pending.staged <- clusterCommitStage{err: context.DeadlineExceeded, barrierFailure: true}
			},
			wantErr: membership.ErrQuorumUnavailable, wantLog: []string{"commit_stage=read_barrier", "failure_class=deadline"},
		},
		{
			name: "preflight rejection keeps its own error",
			prepare: func(_ *clusterRuntime, pending *clusterPendingCommit) {
				pending.staged <- clusterCommitStage{err: errClusterProfileCompatibility}
			},
			wantErr: errClusterProfileCompatibility,
		},
		{
			name: "driver stopped before staging",
			prepare: func(runtime *clusterRuntime, _ *clusterPendingCommit) {
				stopped := make(chan struct{})
				close(stopped)
				runtime.driverStopped = stopped
			},
			wantErr: membership.ErrQuorumUnavailable, wantLog: []string{"commit_stage=read_barrier", "failure_class=raft_failed", "driver_stopped=true"},
		},
		{
			name: "read context expired before staging",
			prepare: func(_ *clusterRuntime, pending *clusterPendingCommit) {
				pending.readContext = cancelledRead
			},
			wantErr: membership.ErrQuorumUnavailable, wantLog: []string{"commit_stage=read_barrier", "failure_class=canceled", "context_error=\"context canceled\""},
		},
		{
			name: "apply failure",
			prepare: func(_ *clusterRuntime, pending *clusterPendingCommit) {
				pending.staged <- clusterCommitStage{context: context.Background()}
				pending.result <- clusterApplyResponse{err: errors.New("state machine apply failed")}
			},
			wantErr: membership.ErrQuorumUnavailable, wantLog: []string{"commit_stage=proposal_apply", "failure_class=other"},
		},
		{
			name: "proposal context cancelled before apply",
			prepare: func(_ *clusterRuntime, pending *clusterPendingCommit) {
				pending.staged <- clusterCommitStage{context: cancelledProposal}
			},
			wantErr: membership.ErrQuorumUnavailable, wantLog: []string{"commit_stage=proposal", "failure_class=canceled"},
		},
		{
			name: "driver stopped after staging",
			prepare: func(runtime *clusterRuntime, pending *clusterPendingCommit) {
				stopped := make(chan struct{})
				close(stopped)
				runtime.driverStopped = stopped
				pending.staged <- clusterCommitStage{context: context.Background()}
			},
			wantErr: membership.ErrQuorumUnavailable, wantLog: []string{"commit_stage=proposal", "failure_class=quorum_unavailable"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logs := &lockedLogBuffer{}
			runtime := commitBoundaryRuntime(t, logs)
			pending := commitBoundaryPending(runtime, membership.CommandDTO{}, context.Background())
			test.prepare(runtime, pending)
			runtime.localCommits.Store(1)

			result, err := pending.Wait()
			if !errors.Is(err, test.wantErr) || (test.wantErr == nil && result != test.wantResult) {
				t.Fatalf("Wait() = (%+v, %v), want (%+v, %v)", result, err, test.wantResult, test.wantErr)
			}
			if got := runtime.localCommits.Load(); got != 0 {
				t.Fatalf("local commits after Wait = %d, want 0", got)
			}
			output := logs.String()
			if len(test.wantLog) == 0 && output != "" {
				t.Fatalf("unexpected commit failure log: %s", output)
			}
			for _, want := range test.wantLog {
				if !strings.Contains(output, want) {
					t.Fatalf("log does not contain %q: %s", want, output)
				}
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-072
func TestClusterBeginCommitRefusesUnavailableDriverWithoutLeakingLocalCommit(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	var missingContext context.Context
	tests := []struct {
		name    string
		ctx     context.Context
		prepare func(*clusterRuntime)
		wantLog string
	}{
		{name: "missing context", ctx: missingContext, wantLog: "commit_stage=invalid_context"},
		{name: "driver failed", ctx: context.Background(), prepare: func(runtime *clusterRuntime) { runtime.driverFailed.Store(true) }, wantLog: "commit_stage=read_barrier"},
		{
			name: "driver stopped", ctx: context.Background(), wantLog: "commit_stage=read_barrier",
			prepare: func(runtime *clusterRuntime) {
				stopped := make(chan struct{})
				close(stopped)
				runtime.driverStopped = stopped
			},
		},
		{
			name: "storage failed", ctx: context.Background(), wantLog: "commit_stage=storage_failed",
			prepare: func(runtime *clusterRuntime) {
				runtime.storage.fail(errors.Join(raftadapter.ErrFailed, raftadapter.ErrStorage))
			},
		},
		{name: "caller cancelled while the driver queue is full", ctx: cancelled, wantLog: "failure_class=canceled"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logs := &lockedLogBuffer{}
			runtime := commitBoundaryRuntime(t, logs)
			runtime.requests = make(chan clusterDriverRequest)
			if test.prepare != nil {
				test.prepare(runtime)
			}
			pending, err := runtime.beginCommit(test.ctx, commitBoundaryOwnershipCommand(), nil)
			if pending != nil || !errors.Is(err, membership.ErrQuorumUnavailable) {
				t.Fatalf("beginCommit() = (%v, %v), want no commit and %v", pending, err, membership.ErrQuorumUnavailable)
			}
			if got := runtime.localCommits.Load(); got != 0 {
				t.Fatalf("rejected commit leaked %d local commits", got)
			}
			if !strings.Contains(logs.String(), test.wantLog) {
				t.Fatalf("log does not contain %q: %s", test.wantLog, logs.String())
			}
		})
	}

	runtime := commitBoundaryRuntime(t, nil)
	runtime.requests = make(chan clusterDriverRequest, 1)
	pending, err := runtime.beginCommit(context.Background(), commitBoundaryOwnershipCommand(), nil)
	if err != nil || pending.requestID != "" || len(pending.data) == 0 || runtime.localCommits.Load() != 1 {
		t.Fatalf("beginCommit() = (%+v, %v) localCommits=%d, want a queued commit counted until Wait", pending, err, runtime.localCommits.Load())
	}
	queued := <-runtime.requests
	if queued.kind != clusterDriverCommit || queued.pending != pending || queued.queuedAt.IsZero() {
		t.Fatalf("driver request = %+v, want the pending commit with its enqueue time", queued)
	}
}

func TestClassifyClusterPortErrorMapsLeadershipAndQuorumFailures(t *testing.T) {
	if got := classifyClusterPortError(nil); got != nil {
		t.Fatalf("classifyClusterPortError(nil) = %v, want nil", got)
	}
	if got := classifyClusterPortError(errors.Join(errors.New("wrapped"), raftadapter.ErrNotLeader)); !errors.Is(got, membership.ErrNotLeader) {
		t.Fatalf("not-leader failure = %v, want %v", got, membership.ErrNotLeader)
	}
	if got := classifyClusterPortError(raftadapter.ErrFailed); !errors.Is(got, membership.ErrQuorumUnavailable) {
		t.Fatalf("failed node = %v, want %v", got, membership.ErrQuorumUnavailable)
	}
}

// Traceability: API-04; docs/specs/management-api-and-observability.md §3, §8.
// Scenarios: management-api-and-observability.md:MGT-SCN-020
func TestClusterHealthCheckerReadinessRejectionOrderAndRecovery(t *testing.T) {
	startedAt := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	incompatible := "incompatible_state_schema: committed cluster features are not supported by this release"
	tests := []struct {
		name       string
		prepare    func(*clusterRuntime, *appmgmt.Service, *time.Time)
		wantStatus managementports.HealthStatus
		wantReason string
		wantState  managementports.NodeState
		ready      bool
	}{
		{
			name: "failed storage", wantStatus: managementports.HealthStatusNotReady, wantState: managementports.NodeStateReady,
			wantReason: "raft storage failed: I/O error; restart required",
			prepare: func(runtime *clusterRuntime, _ *appmgmt.Service, _ *time.Time) {
				runtime.storage.fail(errors.Join(raftadapter.ErrFailed, raftadapter.ErrStorage))
			},
		},
		{
			name: "active drain record", wantStatus: managementports.HealthStatusNotReady, wantState: managementports.NodeStateReady,
			wantReason: "node drain record is active",
			prepare: func(runtime *clusterRuntime, _ *appmgmt.Service, _ *time.Time) {
				runtime.drain = &clusterDrainController{runtime: runtime}
				runtime.drain.admissionClosed.Store(true)
			},
		},
		{
			name: "connection rebalance shedding", wantStatus: managementports.HealthStatusNotReady, wantState: managementports.NodeStateReady,
			wantReason: "connection rebalance shedding",
			prepare: func(runtime *clusterRuntime, _ *appmgmt.Service, _ *time.Time) {
				runtime.rebalance = &clusterRebalanceController{runtime: runtime}
				runtime.rebalance.admissionClosed.Store(true)
			},
		},
		{
			name: "draining lifecycle", wantStatus: managementports.HealthStatusNotReady, wantState: managementports.NodeStateDraining,
			wantReason: "node is draining",
			prepare: func(_ *clusterRuntime, management *appmgmt.Service, _ *time.Time) {
				management.SetNodeState(managementports.NodeStateDraining, nil)
			},
		},
		{
			name: "safe to stop lifecycle", wantStatus: managementports.HealthStatusNotReady, wantState: managementports.NodeStateSafeToStop,
			wantReason: "node is draining",
			prepare: func(_ *clusterRuntime, management *appmgmt.Service, _ *time.Time) {
				management.SetNodeState(managementports.NodeStateSafeToStop, nil)
			},
		},
		{
			name: "slow storage", wantStatus: managementports.HealthStatusNotReady, wantState: managementports.NodeStateReady,
			wantReason: "raft storage I/O is slow",
			prepare: func(runtime *clusterRuntime, _ *appmgmt.Service, now *time.Time) {
				runtime.storage.begin()
				*now = now.Add(clusterStorageSlowThreshold)
			},
		},
		{
			name: "incompatible release profile", wantStatus: managementports.HealthStatusNotReady, wantState: managementports.NodeStateJoining,
			wantReason: incompatible,
			prepare: func(runtime *clusterRuntime, _ *appmgmt.Service, _ *time.Time) {
				runtime.releaseProfile = &clusterReleaseProfileController{}
				runtime.releaseProfile.incompatible.Store(&incompatible)
			},
		},
		{
			name: "failed fresh read proof", wantStatus: managementports.HealthStatusNotReady, wantState: managementports.NodeStateJoining,
			wantReason: "cluster quorum, committed voter role, route projection, tenant quota profile, or release profile is not ready",
			prepare: func(runtime *clusterRuntime, _ *appmgmt.Service, _ *time.Time) {
				runtime.driverFailed.Store(true)
			},
		},
		{
			name: "ready cluster", wantStatus: managementports.HealthStatusUp, wantState: managementports.NodeStateReady,
			ready: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := startedAt
			runtime := commitBoundaryRuntime(t, nil)
			runtime.storage = newClusterStorageMonitor(func() time.Time { return now }, nil)
			management := appmgmt.NewService(appmgmt.ServiceConfig{StartedAt: startedAt}, nil, nil, nil, nil)
			management.SetNodeState(managementports.NodeStateReady, nil)
			runtime.managementState = management
			if test.ready {
				management.SetNodeState(managementports.NodeStateJoining, nil)
				drainRunInstallQuotaProfile(t, runtime.machine)
				drainRunMarkRuntimeReady(runtime)
				runtime.requests = make(chan clusterDriverRequest, 8)
				startDrainRunDriver(t, runtime)
			} else {
				test.prepare(runtime, management, &now)
			}
			checker := &clusterHealthChecker{HealthChecker: management, runtime: runtime}

			status, reason := checker.CheckReadiness(context.Background())
			if status != test.wantStatus {
				t.Fatalf("CheckReadiness status = %q (reason %v), want %q", status, reason, test.wantStatus)
			}
			if test.wantReason == "" && reason != nil {
				t.Fatalf("CheckReadiness reason = %q, want none", *reason)
			}
			if test.wantReason != "" && (reason == nil || *reason != test.wantReason) {
				t.Fatalf("CheckReadiness reason = %v, want %q", reason, test.wantReason)
			}
			if got := management.NodeState(); got != test.wantState {
				t.Fatalf("NodeState after readiness check = %s, want %s", got, test.wantState)
			}
		})
	}
}

type clusterSummaryStateProvider struct {
	managementports.StateProvider
	summary managementports.ClusterSummaryDTO
}

func (provider *clusterSummaryStateProvider) GetClusterSummary(context.Context) managementports.ClusterSummaryDTO {
	return provider.summary
}

// Traceability: API-04; docs/specs/management-api-and-observability.md §3, §8.
func TestClusterStateProviderSummaryCountsNodesByDrainLifecycle(t *testing.T) {
	tests := []struct {
		name      string
		phase     nodedrain.Phase
		wantNodes managementports.NodesByStateDTO
	}{
		{name: "no drain", wantNodes: managementports.NodesByStateDTO{Ready: 1, Joining: 2}},
		{name: "draining peer", phase: nodedrain.PhaseTransferringSessions, wantNodes: managementports.NodesByStateDTO{Ready: 1, Joining: 1, Draining: 1}},
		{name: "safe to stop peer", phase: nodedrain.PhaseCompleted, wantNodes: managementports.NodesByStateDTO{Ready: 1, Joining: 1, SafeToStop: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtimes := newReadAdmissionTestCluster(t)
			runtime := runtimes[0]
			runtime.machine = readAdmissionTestMachine(t, runtimes)
			runtime.storage = newClusterStorageMonitor(time.Now, nil)
			drainRunInstallQuotaProfile(t, runtime.machine)
			if test.phase != nodedrain.PhaseUnspecified {
				drainRunStart(t, runtime.machine, 2, test.phase, drainRunLiveSpec(0))
			}
			drainRunMarkRuntimeReady(runtime)
			startDrainRunDriver(t, runtime)

			base := &clusterSummaryStateProvider{summary: managementports.ClusterSummaryDTO{ConnectionCount: 12}}
			summary := (&clusterStateProvider{StateProvider: base, runtime: runtime}).GetClusterSummary(context.Background())
			profile := runtime.config.replicationProfile()
			if summary.ConnectionCount != 12 || summary.ClusterID != runtime.config.ClusterID ||
				summary.ReleaseVersion != runtime.config.ReleaseVersion || summary.MemberCount != 3 || summary.QuorumSize != 2 ||
				summary.ReplicationFactor != profile.Factor || summary.WriteQuorum != profile.WriteQuorum ||
				!summary.QuorumAvailable || summary.UnderReplicated != 0 ||
				summary.LeaderNodeID == nil || *summary.LeaderNodeID != "1" {
				t.Fatalf("cluster summary = %+v, want base fields plus runtime identity, quorum and leader 1", summary)
			}
			if summary.NodesByState != test.wantNodes {
				t.Fatalf("NodesByState = %+v, want %+v", summary.NodesByState, test.wantNodes)
			}
		})
	}
}

// Traceability: API-04; docs/specs/management-api-and-observability.md §3, §8.
func TestClusterNodeSummaryProjectsRetiredRemoteDrainAndLocalStorageFailure(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	runtime := runtimes[0]
	runtime.machine = readAdmissionTestMachine(t, runtimes)
	runtime.storage = newClusterStorageMonitor(time.Now, nil)
	base := &clusterRuntimeNodeSummaryProvider{node: &managementports.NodeSummaryDTO{
		State: managementports.NodeStateReady, Ready: true, ConnectionCount: 3, UptimeMillis: 99,
	}}
	provider := &clusterStateProvider{StateProvider: base, runtime: runtime}
	ctx := context.Background()

	retired := provider.nodeSummary(ctx, membership.MemberIdentityDTO{MemberID: 9, ReleaseVersion: "0.9.0", Phase: membership.MemberPhaseRetired})
	if retired.Role != "RETIRED" || retired.State != managementports.NodeStateSafeToStop || retired.Ready || retired.Version != "0.9.0" {
		t.Fatalf("retired summary = %+v, want RETIRED and SAFE_TO_STOP", retired)
	}

	drainRunStart(t, runtime.machine, 2, nodedrain.PhaseTransferringSessions, drainRunLiveSpec(0))
	remote := provider.nodeSummary(ctx, runtime.machine.Members()[1])
	if remote.State != managementports.NodeStateDraining || remote.Ready ||
		remote.ReadinessReason == nil || *remote.ReadinessReason != "node drain record is active" ||
		remote.ReplicationHealth == nil || *remote.ReplicationHealth != clusterReplicationUnreachable || remote.ReplicationQueue == nil {
		t.Fatalf("draining remote summary = %+v, want DRAINING with unreachable replication", remote)
	}

	healthy := provider.nodeSummary(ctx, runtime.machine.Members()[0])
	if healthy.Role != "LEADER" || !healthy.Ready || healthy.ConnectionCount != 3 || healthy.UptimeMillis != 99 ||
		healthy.ReplicationHealth == nil || *healthy.ReplicationHealth != clusterReplicationHealthy ||
		healthy.StoragePressure || healthy.LastErrorCode != nil {
		t.Fatalf("healthy local summary = %+v, want ready leader with healthy replication", healthy)
	}

	runtime.storage.fail(errors.Join(raftadapter.ErrFailed, raftadapter.ErrStorage))
	failed := provider.nodeSummary(ctx, runtime.machine.Members()[0])
	if failed.Ready || failed.ReadinessReason == nil || *failed.ReadinessReason != "raft storage failed: I/O error; restart required" ||
		failed.ReplicationHealth == nil || *failed.ReplicationHealth != clusterReplicationFailed ||
		!failed.StoragePressure || failed.LastErrorCode == nil || *failed.LastErrorCode != clusterErrorStorageIO {
		t.Fatalf("failed-storage local summary = %+v, want not ready with sanitized storage I/O code", failed)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-072
func TestClusterSendMessagesSkipsUnknownPeersStopsAtBackpressureAndSurfacesClosure(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	runtime := runtimes[0]
	logs := &lockedLogBuffer{}
	runtime.logger = slog.New(slog.NewTextHandler(logs, nil))
	ctx := context.Background()

	runtime.readIndexStaged = 7
	if err := runtime.sendMessages(ctx, []raftadapter.MessageDTO{{To: 99, Type: "MsgApp", Data: []byte("unknown")}}); err != nil {
		t.Fatalf("sendMessages to an unregistered peer: %v", err)
	}
	if runtime.readIndexStaged != 0 {
		t.Fatalf("readIndexStaged = %d, want the staging budget reset", runtime.readIndexStaged)
	}

	if err := runtime.sendMessages(ctx, []raftadapter.MessageDTO{{To: 2, Type: "MsgApp", Data: []byte("queued")}}); err != nil {
		t.Fatalf("sendMessages to a registered peer: %v", err)
	}
	if status, err := runtime.transport.Status(2); err != nil || status.OutboundQueuedMessages != 1 {
		t.Fatalf("peer 2 status = %+v err=%v, want the message queued", status, err)
	}

	// Fill the bounded peer queue; the next Raft message is rejected, not buffered.
	queued := 1
	for {
		err := runtime.transport.Send(tcpadapter.PeerMessageDTO{From: runtime.config.NodeID, To: 2, Data: []byte{1}})
		if errors.Is(err, tcpadapter.ErrPeerBackpressure) {
			break
		}
		if err != nil || queued > 4096 {
			t.Fatalf("filling peer queue: err=%v queued=%d", err, queued)
		}
		queued++
	}
	before, _ := runtime.transport.Status(2)
	err := runtime.sendMessages(ctx, []raftadapter.MessageDTO{
		{To: 2, Type: "MsgApp", Data: []byte("overflow")},
		{To: 2, Type: "MsgApp", Data: []byte("same-batch")},
		{To: 2, Type: "MsgSnap", IsSnapshot: true, Data: []byte("snapshot")},
		{To: 2, Type: "MsgProp", Data: []byte("proposal")},
	})
	if err != nil {
		t.Fatalf("sendMessages under peer backpressure: %v", err)
	}
	after, _ := runtime.transport.Status(2)
	if after.RejectedMessages != before.RejectedMessages+1 || after.OutboundQueuedMessages != before.OutboundQueuedMessages {
		t.Fatalf("peer status before=%+v after=%+v, want exactly one rejected send and no growth", before, after)
	}
	if output := logs.String(); strings.Count(output, "dynamic peer backpressure") != 1 || !strings.Contains(output, "peer_id=2") {
		t.Fatalf("backpressure log = %q, want a single warning for peer 2", output)
	}

	if err := runtime.transport.Close(); err != nil {
		t.Fatalf("close transport: %v", err)
	}
	closed := []raftadapter.MessageDTO{{To: 3, Type: "MsgApp", Data: []byte("late")}}
	if err := runtime.sendMessages(ctx, closed); !errors.Is(err, tcpadapter.ErrPeerTransportClosed) ||
		!strings.Contains(err.Error(), "queue raft message for peer 3") {
		t.Fatalf("sendMessages on a closed transport = %v, want a peer 3 queue error", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := runtime.sendMessages(cancelled, closed); err != nil {
		t.Fatalf("sendMessages during shutdown = %v, want closure ignored", err)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-074
func TestClusterCommittedStateQueriesRequireFreshReadProof(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	drainRunAcquire(t, machine, "tenant-a", "client-a", 1)
	runtime, _ := newDrainRunRuntime(t, machine, nil)
	driver := startDrainRunDriver(t, runtime)
	ctx := context.Background()

	queries := map[string]func() error{
		"Members":                      func() error { _, err := runtime.Members(ctx); return err },
		"CredentialRevocations":        func() error { _, err := runtime.CredentialRevocations(ctx); return err },
		"Blocklist":                    func() error { _, err := runtime.Blocklist(ctx); return err },
		"IsCredentialRevoked":          func() error { _, err := runtime.IsCredentialRevoked(ctx, "credential"); return err },
		"ActiveFeatures":               func() error { _, err := runtime.ActiveFeatures(ctx); return err },
		"GetOperation":                 func() error { _, _, err := runtime.GetOperation(ctx, "missing"); return err },
		"GetOperationByIdempotencyKey": func() error { _, _, err := runtime.GetOperationByIdempotencyKey(ctx, "missing"); return err },
		"GetOwnership":                 func() error { _, _, err := runtime.GetOwnership(ctx, "tenant-a", "client-a"); return err },
		"GetNodeDrain":                 func() error { _, _, err := runtime.GetNodeDrain(ctx, 1); return err },
		"GetConnectionRebalance":       func() error { _, _, err := runtime.GetConnectionRebalance(ctx); return err },
		"GetClusterPolicy":             func() error { _, _, err := runtime.GetClusterPolicy(ctx); return err },
	}
	for name, query := range queries {
		if err := query(); err != nil {
			t.Errorf("%s with a confirmed read proof = %v, want nil", name, err)
		}
	}
	members, err := runtime.Members(ctx)
	if err != nil || len(members) != 3 {
		t.Fatalf("Members() = (%d, %v), want the three committed members", len(members), err)
	}
	ownership, found, err := runtime.GetOwnership(ctx, "tenant-a", "client-a")
	if err != nil || !found || ownership.OwnerNodeID != 1 || ownership.Epoch != 1 {
		t.Fatalf("GetOwnership() = (%+v, %v, %v), want node 1 epoch 1", ownership, found, err)
	}
	if _, found, err := runtime.GetOwnership(ctx, "tenant-a", "unknown"); err != nil || found {
		t.Fatalf("GetOwnership(unknown) found=%v err=%v, want a clean miss", found, err)
	}

	driver.setReadError(raftadapter.ErrNotLeader)
	for name, query := range queries {
		if err := query(); !errors.Is(err, membership.ErrNotLeader) {
			t.Errorf("%s after leadership loss = %v, want %v", name, err, membership.ErrNotLeader)
		}
	}
	driver.setReadError(context.DeadlineExceeded)
	for name, query := range queries {
		if err := query(); !errors.Is(err, membership.ErrQuorumUnavailable) {
			t.Errorf("%s after a failed proof = %v, want %v", name, err, membership.ErrQuorumUnavailable)
		}
	}
}

// Traceability: CLU-04, CLU-05; docs/specs/cluster-session-and-delivery.md §2, §4.1.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-074
func TestClusterAcquireOwnershipAdvancesEpochAndRejectsUnconfirmedCommits(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	drainRunAcquire(t, machine, "tenant-a", "remote", 2)
	runtime, _ := newDrainRunRuntime(t, machine, nil)
	driver := startDrainRunDriver(t, runtime)
	ctx := context.Background()

	if epoch, err := runtime.AcquireOwnership(ctx, "tenant-a", "client", 2); epoch != 0 || !errors.Is(err, membership.ErrNotLeader) {
		t.Fatalf("AcquireOwnership for another node = (%d, %v), want (0, %v)", epoch, err, membership.ErrNotLeader)
	}
	if commands := driver.commands(); len(commands) != 0 {
		t.Fatalf("rejected acquire proposed %+v", commands)
	}
	if epoch, err := runtime.AcquireOwnership(ctx, "tenant-a", "client", 1); err != nil || epoch != 1 {
		t.Fatalf("first AcquireOwnership = (%d, %v), want epoch 1", epoch, err)
	}
	if epoch, err := runtime.AcquireOwnership(ctx, "tenant-a", "client", 1); err != nil || epoch != 2 {
		t.Fatalf("reconnect AcquireOwnership = (%d, %v), want epoch 2", epoch, err)
	}
	if epoch, err := runtime.AcquireOwnership(ctx, "tenant-a", "remote", 1); err != nil || epoch != 2 {
		t.Fatalf("takeover AcquireOwnership = (%d, %v), want epoch 2", epoch, err)
	}
	commands := driver.ownershipCommands()
	if len(commands) != 3 || commands[0].Action != membership.OwnershipActionAcquire || commands[0].ExpectedEpoch != 0 ||
		commands[1].Action != membership.OwnershipActionAcquire || commands[1].ExpectedEpoch != 1 ||
		commands[2].Action != membership.OwnershipActionTransfer || commands[2].ExpectedEpoch != 1 || commands[2].OwnerNodeID != 1 {
		t.Fatalf("ownership commands = %+v, want acquire, re-acquire and a transfer from node 2", commands)
	}
	if owner, _ := machine.GetOwnership("tenant-a", "remote"); owner.OwnerNodeID != 1 || owner.Epoch != 2 {
		t.Fatalf("taken-over ownership = %+v, want node 1 epoch 2", owner)
	}

	driver.mu.Lock()
	driver.script = []drainRunReply{
		{err: membership.ErrQuorumUnavailable},
		{result: membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictStaleEpoch}},
		{result: membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeApplied}},
	}
	driver.mu.Unlock()
	for _, name := range []string{"commit error", "stale epoch", "missing ownership result"} {
		if epoch, err := runtime.AcquireOwnership(ctx, "tenant-a", "client", 1); epoch != 0 || !errors.Is(err, membership.ErrQuorumUnavailable) {
			t.Fatalf("%s: AcquireOwnership = (%d, %v), want (0, %v)", name, epoch, err, membership.ErrQuorumUnavailable)
		}
	}
	driver.setReadError(raftadapter.ErrNotLeader)
	if epoch, err := runtime.AcquireOwnership(ctx, "tenant-a", "client", 1); epoch != 0 || !errors.Is(err, membership.ErrNotLeader) {
		t.Fatalf("AcquireOwnership without a read proof = (%d, %v), want (0, %v)", epoch, err, membership.ErrNotLeader)
	}
}

// Traceability: CLU-05; docs/specs/cluster-session-and-delivery.md §2.2, §4.1.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-074
func TestClusterCommitSubscriptionCommitsFilterAndMapsFailures(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	drainRunAcquire(t, machine, "tenant-a", "client-a", 1)
	runtime, _ := newDrainRunRuntime(t, machine, nil)
	driver := startDrainRunDriver(t, runtime)
	ctx := context.Background()
	add, remove := uint32(membership.SubscriptionActionAdd), uint32(membership.SubscriptionActionRemove)

	if err := runtime.CommitSubscription(ctx, "tenant-a", "client-a", 1, 1, "devices/#", 1, true, true, 2, 99); !errors.Is(err, membership.ErrQuorumUnavailable) {
		t.Fatalf("CommitSubscription with an unknown action = %v, want %v", err, membership.ErrQuorumUnavailable)
	}
	if commands := driver.commands(); len(commands) != 0 {
		t.Fatalf("unknown action proposed %+v", commands)
	}
	if err := runtime.CommitSubscription(ctx, "tenant-a", "client-a", 1, 1, "devices/#", 1, true, true, 2, add); err != nil {
		t.Fatalf("CommitSubscription(add): %v", err)
	}
	subscriptions := machine.Subscriptions()
	if len(subscriptions) != 1 || subscriptions[0].Filter != "devices/#" || subscriptions[0].QoS != 1 ||
		!subscriptions[0].NoLocal || !subscriptions[0].RetainAsPublished || subscriptions[0].RetainHandling != 2 {
		t.Fatalf("committed subscriptions = %+v, want the options carried through unchanged", subscriptions)
	}
	if err := runtime.CommitSubscription(ctx, "tenant-a", "client-a", 1, 9, "devices/#", 1, false, false, 0, add); !errors.Is(err, membership.ErrQuorumUnavailable) {
		t.Fatalf("CommitSubscription with a stale epoch = %v, want %v", err, membership.ErrQuorumUnavailable)
	}
	driver.mu.Lock()
	driver.script = []drainRunReply{{err: membership.ErrQuorumUnavailable}}
	driver.mu.Unlock()
	if err := runtime.CommitSubscription(ctx, "tenant-a", "client-a", 1, 1, "devices/#", 1, false, false, 0, remove); !errors.Is(err, membership.ErrQuorumUnavailable) {
		t.Fatalf("CommitSubscription with a failed commit = %v, want %v", err, membership.ErrQuorumUnavailable)
	}
	if len(machine.Subscriptions()) != 1 {
		t.Fatal("unconfirmed removal changed the committed subscriptions")
	}
	if err := runtime.CommitSubscription(ctx, "tenant-a", "client-a", 1, 1, "devices/#", 1, false, false, 0, remove); err != nil {
		t.Fatalf("CommitSubscription(remove): %v", err)
	}
	if len(machine.Subscriptions()) != 0 {
		t.Fatalf("subscriptions after removal = %+v, want none", machine.Subscriptions())
	}
}
