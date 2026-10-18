package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"narumq/internal/adapters/clock"
	memoryqueue "narumq/internal/adapters/memory/deliveryqueue"
	"narumq/internal/adapters/memory/sessionstore"
	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/application/connect"
	appmgmt "narumq/internal/application/management"
	"narumq/internal/application/membership"
	managementports "narumq/internal/application/ports/management"
	"narumq/internal/domain/nodedrain"
	"narumq/internal/domain/routing"
)

const drainRunOperation = "00000000-0000-4000-8000-000000000301"

// drainRunSpec describes the replicated parameters of a drain record built for
// the worker tests. Times are wall-clock milliseconds because the worker
// compares the committed deadline with time.Now.
type drainRunSpec struct {
	startedAtMillis uint64
	timeoutMillis   uint64
	lbDelayMillis   uint64
	rate            uint32
}

func drainRunLiveSpec(lbDelayMillis uint64) drainRunSpec {
	return drainRunSpec{
		startedAtMillis: uint64(time.Now().UnixMilli()), timeoutMillis: 60_000,
		lbDelayMillis: lbDelayMillis, rate: 1000,
	}
}

func drainRunExpiredSpec() drainRunSpec {
	return drainRunSpec{
		startedAtMillis: uint64(time.Now().UnixMilli()) - 10_000, timeoutMillis: 1_000, rate: 1000,
	}
}

func drainRunApply(t *testing.T, machine *membership.StateMachine, command membership.CommandDTO) membership.ApplyResultDTO {
	t.Helper()
	operationID, err := newClusterOperationID()
	if err != nil {
		t.Fatalf("newClusterOperationID: %v", err)
	}
	command.SchemaMajor, command.SchemaMinor, command.OperationID = membership.SchemaMajor, membership.SchemaMinor, operationID
	result, err := machine.Apply(machine.LastAppliedIndex()+1, command)
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("apply %+v: result=%+v err=%v, want Applied", command, result, err)
	}
	return result
}

// drainRunStart commits START and ADVANCE commands until the member's drain
// record reaches phase (PhaseCompleted adds the COMPLETE command).
func drainRunStart(t *testing.T, machine *membership.StateMachine, memberID uint64, phase nodedrain.Phase, spec drainRunSpec) {
	t.Helper()
	drainRunApply(t, machine, membership.CommandDTO{NodeDrain: &membership.NodeDrainChangeDTO{
		Action: membership.NodeDrainActionStart, MemberID: memberID, DrainOperationID: drainRunOperation,
		IdempotencyKey: "drain-run", ConnectionRatePerSecond: spec.rate,
		LoadBalancerPropagationDelayMillis: spec.lbDelayMillis, TimeoutMillis: spec.timeoutMillis,
		ProposedAtUnixMillis: spec.startedAtMillis,
	}})
	for next := nodedrain.PhaseWaitingForLB; next <= phase && next <= nodedrain.PhaseVerifyingBarrier; next++ {
		drainRunAdvance(t, machine, memberID, next, spec.startedAtMillis)
	}
	if phase == nodedrain.PhaseCompleted {
		drainRunApply(t, machine, membership.CommandDTO{NodeDrain: &membership.NodeDrainChangeDTO{
			Action: membership.NodeDrainActionComplete, MemberID: memberID, DrainOperationID: drainRunOperation,
			ProposedAtUnixMillis: spec.startedAtMillis + 1,
		}})
	}
}

func drainRunAdvance(t *testing.T, machine *membership.StateMachine, memberID uint64, phase nodedrain.Phase, proposedAt uint64) {
	t.Helper()
	drainRunApply(t, machine, membership.CommandDTO{NodeDrain: &membership.NodeDrainChangeDTO{
		Action: membership.NodeDrainActionAdvance, MemberID: memberID, DrainOperationID: drainRunOperation,
		Phase: phase, ProposedAtUnixMillis: proposedAt,
	}})
}

func drainRunAcquire(t *testing.T, machine *membership.StateMachine, tenantID, clientID string, ownerID uint64) {
	t.Helper()
	drainRunApply(t, machine, membership.CommandDTO{Ownership: &membership.OwnershipChangeDTO{
		TenantID: tenantID, ClientID: clientID, OwnerNodeID: ownerID, Epoch: 1, ExpectedEpoch: 0,
		Action: membership.OwnershipActionAcquire,
	}})
}

func drainRunInstallQuotaProfile(t *testing.T, machine *membership.StateMachine) {
	t.Helper()
	drainRunApply(t, machine, membership.CommandDTO{
		TenantQuota: &membership.TenantQuotaChangeDTO{Profile: membership.DefaultTenantQuotaProfile()},
	})
}

func drainRunWaitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// drainRunReply replaces the outcome of one proposal that reaches the scripted driver.
type drainRunReply struct {
	// match selects the command this reply applies to; nil matches any command.
	match  func(membership.CommandDTO) bool
	result membership.ApplyResultDTO
	err    error
}

func drainRunMatchTransfer(command membership.CommandDTO) bool {
	return command.Ownership != nil && command.Ownership.Action == membership.OwnershipActionTransfer
}

// drainRunDriver stands in for the Raft driver goroutine. Commands the script
// does not intercept are applied to the committed state machine exactly like a
// quorum commit would apply them.
type drainRunDriver struct {
	mu      sync.Mutex
	script  []drainRunReply
	seen    []membership.CommandDTO
	readErr error
}

func startDrainRunDriver(t *testing.T, runtime *clusterRuntime, script ...drainRunReply) *drainRunDriver {
	t.Helper()
	driver := &drainRunDriver{script: script}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-ctx.Done():
				return
			case request := <-runtime.requests:
				driver.handle(runtime, request)
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	return driver
}

func (driver *drainRunDriver) handle(runtime *clusterRuntime, request clusterDriverRequest) {
	switch request.kind {
	case clusterDriverRead:
		driver.mu.Lock()
		readErr := driver.readErr
		driver.mu.Unlock()
		request.readResult <- clusterReadResponse{index: runtime.machine.LastAppliedIndex(), err: readErr}
	case clusterDriverCommit:
		pending := request.pending
		pending.staged <- clusterCommitStage{context: pending.context}
		command, err := decodeMembershipCommand(pending.data)
		if err != nil {
			pending.result <- clusterApplyResponse{err: err}
			return
		}
		driver.mu.Lock()
		driver.seen = append(driver.seen, command)
		var reply *drainRunReply
		if len(driver.script) != 0 && (driver.script[0].match == nil || driver.script[0].match(command)) {
			reply = &driver.script[0]
			driver.script = driver.script[1:]
		}
		driver.mu.Unlock()
		if reply != nil {
			pending.result <- clusterApplyResponse{result: reply.result, err: reply.err}
			return
		}
		result, err := runtime.machine.Apply(runtime.machine.LastAppliedIndex()+1, command)
		pending.result <- clusterApplyResponse{result: result, err: err}
	}
}

func (driver *drainRunDriver) setReadError(err error) {
	driver.mu.Lock()
	driver.readErr = err
	driver.mu.Unlock()
}

func (driver *drainRunDriver) commands() []membership.CommandDTO {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	return append([]membership.CommandDTO(nil), driver.seen...)
}

func (driver *drainRunDriver) drainChanges(action membership.NodeDrainAction) []membership.NodeDrainChangeDTO {
	var changes []membership.NodeDrainChangeDTO
	for _, command := range driver.commands() {
		if command.NodeDrain != nil && command.NodeDrain.Action == action {
			changes = append(changes, *command.NodeDrain)
		}
	}
	return changes
}

func (driver *drainRunDriver) ownershipCommands() []membership.OwnershipChangeDTO {
	var changes []membership.OwnershipChangeDTO
	for _, command := range driver.commands() {
		if command.Ownership != nil {
			changes = append(changes, *command.Ownership)
		}
	}
	return changes
}

func (driver *drainRunDriver) advancedTo(phase nodedrain.Phase) bool {
	for _, change := range driver.drainChanges(membership.NodeDrainActionAdvance) {
		if change.Phase == phase {
			return true
		}
	}
	return false
}

// newDrainRunRuntime builds a runtime without a Raft node. Voter connectivity is
// therefore unavailable, which is the "no connected successor" cluster shape.
func newDrainRunRuntime(t *testing.T, machine *membership.StateMachine, logSink io.Writer) (*clusterRuntime, *appmgmt.Service) {
	t.Helper()
	if logSink == nil {
		logSink = io.Discard
	}
	management := appmgmt.NewService(appmgmt.ServiceConfig{StartedAt: time.Unix(1, 0)}, nil, nil, nil, nil)
	management.SetNodeState(managementports.NodeStateReady, nil)
	runtime := &clusterRuntime{
		config: clusterConfigFile{NodeID: 1}, machine: machine, managementState: management,
		storage:  newClusterStorageMonitor(time.Now, nil),
		requests: make(chan clusterDriverRequest, 32),
		logger:   slog.New(slog.NewTextHandler(logSink, nil)),
	}
	return runtime, management
}

func drainRunMarkRuntimeReady(runtime *clusterRuntime) {
	runtime.releaseProfile = &clusterReleaseProfileController{}
	runtime.releaseProfile.synced.Store(true)
	runtime.initialized.Store(true)
	runtime.admissionBootstrapped.Store(true)
	runtime.admissionDynamicMembership.Store(true)
	runtime.admissionLocalVoter.Store(true)
	runtime.admissionLocalPhaseVoter.Store(true)
	runtime.admissionAppliedIndex.Store(1 << 20)
	runtime.projectionIndex.Store(1 << 20)
}

// drainRunStartController starts the controller like runClusterMode does and
// guarantees its worker has stopped before the scripted driver goes away.
func drainRunStartController(t *testing.T, runtime *clusterRuntime) (*clusterDrainController, context.CancelFunc) {
	t.Helper()
	controller := &clusterDrainController{runtime: runtime}
	runtime.drain = controller
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		controller.stopAndWait()
	})
	controller.start(ctx)
	return controller, cancel
}

// Traceability: DEP-02, DEP-04; docs/specs/drain-and-rolling-update.md §2.1, §3.
func TestClusterDrainWorkerHoldsTrafficExclusionUntilLoadBalancerPropagation(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	spec := drainRunLiveSpec(150)
	drainRunStart(t, machine, 1, nodedrain.PhaseExcludingTraffic, spec)
	runtime, management := newDrainRunRuntime(t, machine, nil)
	driver := startDrainRunDriver(t, runtime)

	controller, cancel := drainRunStartController(t, runtime)
	if got := management.NodeState(); got != managementports.NodeStateDraining {
		t.Fatalf("NodeState after starting an active drain = %s, want %s", got, managementports.NodeStateDraining)
	}
	if controller.admissionOpen() {
		t.Fatal("admission stayed open for an active drain record")
	}

	drainRunWaitFor(t, "worker to reach the successor check", func() bool {
		return controller.lastBlockingReason() == string(nodedrain.BlockerNoSuccessorVoter)
	})
	state, exists := machine.GetNodeDrain(1)
	if !exists || state.Phase != nodedrain.PhaseVerifyingSessionState {
		t.Fatalf("committed drain = %+v exists=%v, want VERIFYING_SESSION_STATE", state, exists)
	}
	advances := driver.drainChanges(membership.NodeDrainActionAdvance)
	if len(advances) != 2 || advances[0].Phase != nodedrain.PhaseWaitingForLB || advances[1].Phase != nodedrain.PhaseVerifyingSessionState {
		t.Fatalf("committed ADVANCE commands = %+v, want WAITING_FOR_LB then VERIFYING_SESSION_STATE", advances)
	}
	if advances[0].Progress.ConnectionsInitial != 0 || advances[0].Progress.ConnectionsRemaining != 0 {
		t.Fatalf("traffic-exclusion progress = %+v, want no active connections recorded", advances[0].Progress)
	}
	if held := advances[1].ProposedAtUnixMillis - advances[0].ProposedAtUnixMillis; held < spec.lbDelayMillis {
		t.Fatalf("load balancer wait lasted %dms, want at least the %dms propagation delay", held, spec.lbDelayMillis)
	}
	if reference := controller.currentServerReference(); reference != "" {
		t.Fatalf("server reference = %q, want the empty reference committed by START", reference)
	}

	cancel()
	controller.stopAndWait()
	controller.mu.Lock()
	running := controller.runningOperationID
	controller.mu.Unlock()
	if running != "" {
		t.Fatalf("stopped worker retained operation %q", running)
	}
	if state, _ := machine.GetNodeDrain(1); state.Phase != nodedrain.PhaseVerifyingSessionState {
		t.Fatalf("cancelled worker changed committed phase to %s", state.Phase)
	}
}

// Traceability: DEP-02, DEP-10; docs/specs/drain-and-rolling-update.md §2.2, §3.
func TestClusterDrainWorkerFailsAtDeadlineAndRetriesUnconfirmedFailure(t *testing.T) {
	tests := []struct {
		name         string
		script       []drainRunReply
		wantBlocking string
	}{
		{name: "committed immediately", wantBlocking: string(nodedrain.BlockerPendingWAL)},
		{
			name: "quorum unavailable is retried", wantBlocking: string(nodedrain.BlockerQuorumUnavailable),
			script: []drainRunReply{{err: membership.ErrQuorumUnavailable}},
		},
		{
			name: "unapplied outcome is retried", wantBlocking: string(nodedrain.BlockerQuorumUnavailable),
			script: []drainRunReply{{result: membership.ApplyResultDTO{
				Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictStaleEpoch,
			}}},
		},
		{
			name: "phase conflict re-reads state without a blocker change", wantBlocking: string(nodedrain.BlockerPendingWAL),
			script: []drainRunReply{{result: membership.ApplyResultDTO{
				Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictPhase,
			}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := validBootstrapClusterConfig()
			machine := retainedTestMachine(t, config, config.ReleaseVersion)
			drainRunStart(t, machine, 1, nodedrain.PhaseExcludingTraffic, drainRunExpiredSpec())
			runtime, _ := newDrainRunRuntime(t, machine, nil)
			driver := startDrainRunDriver(t, runtime, test.script...)
			controller := &clusterDrainController{runtime: runtime}
			runtime.drain = controller
			controller.setLastBlock(nodedrain.BlockerPendingWAL)

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			startedAt := time.Now()
			controller.run(ctx, drainRunOperation)

			state, exists := machine.GetNodeDrain(1)
			if !exists || state.Phase != nodedrain.PhaseFailed || state.FailureCode != "drain_timeout" ||
				state.BlockingReason != test.wantBlocking {
				t.Fatalf("committed drain = %+v exists=%v, want FAILED drain_timeout blocked by %q",
					state, exists, test.wantBlocking)
			}
			if got, want := len(driver.drainChanges(membership.NodeDrainActionFail)), 1+len(test.script); got != want {
				t.Fatalf("FAIL proposals = %d, want %d", got, want)
			}
			if len(test.script) != 0 && test.wantBlocking == string(nodedrain.BlockerQuorumUnavailable) &&
				time.Since(startedAt) < drainRetryInterval*4/5 {
				t.Fatalf("unconfirmed FAIL was retried after %s, want the %s retry interval", time.Since(startedAt), drainRetryInterval)
			}
			if controller.lastBlockingReason() != test.wantBlocking {
				t.Fatalf("lastBlockingReason = %q, want %q", controller.lastBlockingReason(), test.wantBlocking)
			}
			controller.mu.Lock()
			running := controller.runningOperationID
			controller.mu.Unlock()
			if running != "" {
				t.Fatalf("finished worker retained operation %q", running)
			}
		})
	}
}

// Traceability: DEP-02, DEP-10; docs/specs/drain-and-rolling-update.md §2.2.
func TestClusterDrainWorkerStopsWhenOperationIsReplacedOrCancelled(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	drainRunStart(t, machine, 1, nodedrain.PhaseVerifyingSessionState, drainRunLiveSpec(0))
	runtime, _ := newDrainRunRuntime(t, machine, nil)
	driver := startDrainRunDriver(t, runtime)
	controller := &clusterDrainController{runtime: runtime}
	runtime.drain = controller

	// A worker for an operation that no longer owns the record exits without proposing.
	controller.run(context.Background(), "00000000-0000-4000-8000-0000000003ff")
	// A worker for a terminal record exits without proposing.
	drainRunApply(t, machine, membership.CommandDTO{NodeDrain: &membership.NodeDrainChangeDTO{
		Action: membership.NodeDrainActionCancel, MemberID: 1, DrainOperationID: drainRunOperation,
		ProposedAtUnixMillis: uint64(time.Now().UnixMilli()),
	}})
	controller.run(context.Background(), drainRunOperation)
	if commands := driver.commands(); len(commands) != 0 {
		t.Fatalf("stopped workers proposed %+v, want no proposals", commands)
	}

	// A cancelled context interrupts the retry wait of a blocked worker.
	cancelledMachine := retainedTestMachine(t, config, config.ReleaseVersion)
	drainRunStart(t, cancelledMachine, 1, nodedrain.PhaseVerifyingSessionState, drainRunLiveSpec(0))
	blockedRuntime, _ := newDrainRunRuntime(t, cancelledMachine, nil)
	blocked := &clusterDrainController{runtime: blockedRuntime}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		blocked.run(ctx, drainRunOperation)
	}()
	drainRunWaitFor(t, "blocked worker", func() bool {
		return blocked.lastBlockingReason() == string(nodedrain.BlockerNoSuccessorVoter)
	})
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked drain worker ignored context cancellation")
	}
}

// Traceability: DEP-02, DEP-08; docs/specs/drain-and-rolling-update.md §2.2, §6.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-007
func TestClusterDrainTransferHoldsSessionsWithoutConnectedSuccessor(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	drainRunAcquire(t, machine, "tenant-a", "client-a", 1)
	drainRunStart(t, machine, 1, nodedrain.PhaseTransferringSessions, drainRunLiveSpec(0))
	runtime, management := newDrainRunRuntime(t, machine, nil)
	driver := startDrainRunDriver(t, runtime)

	controller, cancel := drainRunStartController(t, runtime)
	drainRunWaitFor(t, "no-successor blocker", func() bool {
		return controller.lastBlockingReason() == string(nodedrain.BlockerNoSuccessorVoter)
	})
	cancel()
	controller.stopAndWait()

	if commands := driver.commands(); len(commands) != 0 {
		t.Fatalf("worker proposed %+v without any connected successor, want no commands", commands)
	}
	owner, exists := machine.GetOwnership("tenant-a", "client-a")
	if !exists || owner.OwnerNodeID != 1 || owner.Epoch != 1 {
		t.Fatalf("ownership = %+v exists=%v, want the session retained by node 1 at epoch 1", owner, exists)
	}
	if state, _ := machine.GetNodeDrain(1); state.Phase != nodedrain.PhaseTransferringSessions {
		t.Fatalf("drain phase = %s, want TRANSFERRING_SESSIONS", state.Phase)
	}
	if got := management.NodeState(); got != managementports.NodeStateDraining {
		t.Fatalf("NodeState = %s, want %s while sessions cannot move", got, managementports.NodeStateDraining)
	}
}

// Traceability: DEP-01, DEP-10; docs/specs/drain-and-rolling-update.md §8.1.
func TestClusterDrainClearsCompletedRecordAfterRestartOnceReadinessReturns(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	drainRunInstallQuotaProfile(t, machine)
	drainRunStart(t, machine, 1, nodedrain.PhaseCompleted, drainRunLiveSpec(0))
	logs := &lockedLogBuffer{}
	runtime, management := newDrainRunRuntime(t, machine, logs)
	driver := startDrainRunDriver(t, runtime)
	// A restarted process has not reloaded its projection yet.
	drainRunMarkRuntimeReady(runtime)
	runtime.initialized.Store(false)

	controller, _ := drainRunStartController(t, runtime)
	if got := management.NodeState(); got != managementports.NodeStateJoining {
		t.Fatalf("NodeState for a completed record found at startup = %s, want %s", got, managementports.NodeStateJoining)
	}
	if controller.admissionOpen() {
		t.Fatal("admission opened while the completed drain record was still present")
	}
	drainRunWaitFor(t, "readiness warning", func() bool {
		return strings.Contains(logs.String(), "completed node drain record clearance is waiting") &&
			strings.Contains(logs.String(), "reason=runtime_not_ready")
	})
	if _, exists := machine.GetNodeDrain(1); !exists {
		t.Fatal("completed record was cleared before the runtime became ready")
	}
	if cleared := driver.drainChanges(membership.NodeDrainActionClear); len(cleared) != 0 {
		t.Fatalf("CLEAR proposed while not ready: %+v", cleared)
	}

	runtime.initialized.Store(true)
	drainRunWaitFor(t, "completed record clearance", func() bool {
		_, exists := machine.GetNodeDrain(1)
		return !exists
	})
	controller.stopAndWait()
	if cleared := driver.drainChanges(membership.NodeDrainActionClear); len(cleared) != 1 || cleared[0].DrainOperationID != drainRunOperation {
		t.Fatalf("CLEAR commands = %+v, want exactly one for operation %s", cleared, drainRunOperation)
	}
	if !strings.Contains(logs.String(), "cleared completed node drain record") {
		t.Fatalf("clearance was not logged: %s", logs.String())
	}
	controller.mu.Lock()
	clearing := controller.clearing
	controller.mu.Unlock()
	if clearing {
		t.Fatal("finished clearance worker left the clearing flag set")
	}
	controller.sync()
	if !controller.admissionOpen() {
		t.Fatal("admission stayed closed after the completed record was cleared")
	}
}

// Traceability: DEP-01, DEP-10; docs/specs/drain-and-rolling-update.md §8.1.
func TestClusterDrainClearRetriesAfterCommitFailureOrConflict(t *testing.T) {
	tests := []struct {
		name       string
		reply      drainRunReply
		wantReason string
	}{
		{name: "commit failure", reply: drainRunReply{err: membership.ErrQuorumUnavailable}, wantReason: "reason=clear_commit_failed"},
		{
			name: "unapplied outcome",
			reply: drainRunReply{result: membership.ApplyResultDTO{
				Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictPhase,
			}},
			wantReason: "reason=clear_commit_conflict",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := validBootstrapClusterConfig()
			machine := retainedTestMachine(t, config, config.ReleaseVersion)
			drainRunInstallQuotaProfile(t, machine)
			drainRunStart(t, machine, 1, nodedrain.PhaseCompleted, drainRunLiveSpec(0))
			logs := &lockedLogBuffer{}
			runtime, _ := newDrainRunRuntime(t, machine, logs)
			drainRunMarkRuntimeReady(runtime)
			driver := startDrainRunDriver(t, runtime, test.reply)

			startedAt := time.Now()
			controller, _ := drainRunStartController(t, runtime)
			drainRunWaitFor(t, "completed record clearance", func() bool {
				_, exists := machine.GetNodeDrain(1)
				return !exists
			})
			controller.stopAndWait()
			if got := len(driver.drainChanges(membership.NodeDrainActionClear)); got != 2 {
				t.Fatalf("CLEAR proposals = %d, want the failed attempt and one retry", got)
			}
			if !strings.Contains(logs.String(), test.wantReason) {
				t.Fatalf("log does not contain %q: %s", test.wantReason, logs.String())
			}
			if time.Since(startedAt) < drainClearInterval*4/5 {
				t.Fatalf("clearance retried after %s, want the %s interval", time.Since(startedAt), drainClearInterval)
			}
		})
	}
}

// Traceability: DEP-01, DEP-10; docs/specs/drain-and-rolling-update.md §8.1.
func TestClusterDrainClearCompletedLeavesReplacedRecordsAndHonorsCancellation(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	drainRunInstallQuotaProfile(t, machine)
	drainRunStart(t, machine, 1, nodedrain.PhaseVerifyingBarrier, drainRunLiveSpec(0))
	runtime, _ := newDrainRunRuntime(t, machine, nil)
	driver := startDrainRunDriver(t, runtime)
	controller := &clusterDrainController{runtime: runtime}
	runtime.drain = controller

	// The record has not completed, so clearance has nothing to remove.
	controller.clearCompleted(context.Background(), drainRunOperation)
	drainRunApply(t, machine, membership.CommandDTO{NodeDrain: &membership.NodeDrainChangeDTO{
		Action: membership.NodeDrainActionComplete, MemberID: 1, DrainOperationID: drainRunOperation,
		ProposedAtUnixMillis: uint64(time.Now().UnixMilli()),
	}})
	// A different operation owns the record, so clearance must not touch it.
	controller.clearCompleted(context.Background(), "00000000-0000-4000-8000-0000000003fe")
	if commands := driver.commands(); len(commands) != 0 {
		t.Fatalf("clearance proposed %+v for a record it does not own", commands)
	}

	// A runtime that never becomes ready stops waiting when the process is cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		controller.clearCompleted(ctx, drainRunOperation)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("clearance ignored context cancellation while waiting for readiness")
	}
	if _, exists := machine.GetNodeDrain(1); !exists {
		t.Fatal("cancelled clearance removed the completed record")
	}
}

// Traceability: DEP-01, DEP-10; docs/specs/drain-and-rolling-update.md §8.1.
func TestClusterDrainSyncDoesNotStartClearanceWithoutLiveContextOrWhenAlreadyClearing(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	drainRunStart(t, machine, 1, nodedrain.PhaseCompleted, drainRunLiveSpec(0))
	runtime, _ := newDrainRunRuntime(t, machine, nil)
	driver := startDrainRunDriver(t, runtime)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name       string
		controller *clusterDrainController
	}{
		{name: "no context", controller: &clusterDrainController{runtime: runtime}},
		{name: "cancelled context", controller: &clusterDrainController{runtime: runtime, ctx: cancelled}},
		{name: "already clearing", controller: &clusterDrainController{runtime: runtime, ctx: context.Background(), clearing: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.controller.sync()
			test.controller.stopAndWait()
			if !test.controller.hasRecord || test.controller.admissionOpen() {
				t.Fatal("completed record did not close admission")
			}
		})
	}
	if commands := driver.commands(); len(commands) != 0 {
		t.Fatalf("sync proposed %+v without a clearance worker, want none", commands)
	}
	if _, exists := machine.GetNodeDrain(1); !exists {
		t.Fatal("completed record disappeared without a clearance worker")
	}
}

// drainRunCluster is a three-node in-process Raft cluster whose node 3 runs the
// drain controller. Node 3 is a follower with live peer connections, so the
// worker sees the connectivity facts a real rolling update sees.
type drainRunCluster struct {
	runtimes   [3]*clusterRuntime
	runtime    *clusterRuntime
	machine    *membership.StateMachine
	management *appmgmt.Service
	driver     *drainRunDriver
}

func drainRunServe(t *testing.T, runtimes [3]*clusterRuntime, serving []int, observer int, peers ...uint64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan error, len(serving))
	for _, index := range serving {
		transport := runtimes[index].transport
		go func() { results <- transport.Serve(ctx) }()
	}
	t.Cleanup(func() {
		cancel()
		for range serving {
			if err := <-results; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("transport Serve during cleanup: %v", err)
			}
		}
	})
	for _, peer := range peers {
		drainRunWaitFor(t, fmt.Sprintf("node %d to connect to peer %d", runtimes[observer].config.NodeID, peer), func() bool {
			status, err := runtimes[observer].transport.Status(peer)
			return err == nil && status.Connected
		})
	}
}

func newDrainRunCluster(t *testing.T, spec drainRunSpec, phase nodedrain.Phase, sessions int, withConnections bool, script ...drainRunReply) *drainRunCluster {
	t.Helper()
	runtimes := newReadAdmissionTestCluster(t)
	runtime := runtimes[2]
	machine := readAdmissionTestMachine(t, runtimes)
	drainRunInstallQuotaProfile(t, machine)
	for index := range sessions {
		drainRunAcquire(t, machine, "tenant-a", fmt.Sprintf("client-%d", index), 3)
	}
	drainRunStart(t, machine, 3, phase, spec)

	management := appmgmt.NewService(appmgmt.ServiceConfig{StartedAt: time.Unix(1, 0)}, nil, nil, nil, nil)
	management.SetNodeState(managementports.NodeStateReady, nil)
	runtime.machine = machine
	runtime.managementState = management
	runtime.storage = newClusterStorageMonitor(time.Now, nil)
	runtime.releaseProfile = &clusterReleaseProfileController{}
	runtime.releaseProfile.synced.Store(true)
	runtime.initialized.Store(true)
	status := runtime.node.Status()
	runtime.projectionIndex.Store(status.AppliedIndex)
	runtime.publishProjectionAdmissionStatus(status)
	if !runtime.clusterProjectionReady() {
		t.Fatalf("test cluster projection is not ready: %+v", runtime.projectionReadinessSnapshot())
	}
	if withConnections {
		localQueue, err := memoryqueue.New(16)
		if err != nil {
			t.Fatal(err)
		}
		connections, err := connect.New(clock.SystemClock{}, sessionstore.New(), localQueue, routing.NewTable(),
			clock.SystemScheduler{}, &recoveryWillPublisher{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { connections.Close() })
		runtime.connectUseCase = connections
	}
	drainRunServe(t, runtimes, []int{0, 1, 2}, 2, 1, 2)
	driver := startDrainRunDriver(t, runtime, script...)
	return &drainRunCluster{runtimes: runtimes, runtime: runtime, machine: machine, management: management, driver: driver}
}

// Traceability: DEP-06, DEP-07, DEP-08; docs/specs/drain-and-rolling-update.md §2.2, §3, §6.
func TestClusterDrainWorkerHandsOffSessionsAndCompletesAfterBarrier(t *testing.T) {
	spec := drainRunLiveSpec(50)
	isTransfer := drainRunMatchTransfer
	cluster := newDrainRunCluster(t, spec, nodedrain.PhaseExcludingTraffic, 2, true,
		// The first transfer is not confirmed; the session must stay unmoved and be retried.
		drainRunReply{match: isTransfer, err: membership.ErrQuorumUnavailable},
		// The second transfer is rejected by a concurrent phase change: counted as failed, then retried.
		drainRunReply{match: isTransfer, result: membership.ApplyResultDTO{
			Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictPhase,
		}},
	)
	input := (&clusterDrainController{runtime: cluster.runtime}).barrierInput()
	if input.PendingWAL != 0 {
		t.Fatalf("test cluster has pending WAL %d before drain, want a settled Raft node", input.PendingWAL)
	}

	controller, _ := drainRunStartController(t, cluster.runtime)
	drainRunWaitFor(t, "drain completion", func() bool {
		state, exists := cluster.machine.GetNodeDrain(3)
		return exists && state.Phase == nodedrain.PhaseCompleted
	})
	controller.stopAndWait()

	state, _ := cluster.machine.GetNodeDrain(3)
	if state.Progress.SessionsReadyForTakeover != 2 || state.Progress.SessionsTakenOver != 2 || state.Progress.SessionsFailed != 1 ||
		state.Progress.ConnectionsRemaining != 0 || state.Progress.PendingWAL != 0 || state.Progress.UnderReplicated != 0 {
		t.Fatalf("completed progress = %+v, want 2 ready, 2 taken over, 1 failed and a clean barrier", state.Progress)
	}
	for index := range 2 {
		clientID := fmt.Sprintf("client-%d", index)
		owner, exists := cluster.machine.GetOwnership("tenant-a", clientID)
		if !exists || owner.OwnerNodeID != 1 || owner.Epoch != 2 {
			t.Fatalf("ownership of %s = %+v exists=%v, want node 1 at epoch 2", clientID, owner, exists)
		}
	}
	transfers := 0
	for _, change := range cluster.driver.ownershipCommands() {
		if change.Action == membership.OwnershipActionTransfer {
			transfers++
			if change.OwnerNodeID != 1 {
				t.Fatalf("transfer target = node %d, want lowest connected voter 1", change.OwnerNodeID)
			}
		}
	}
	if transfers != 4 {
		t.Fatalf("transfer proposals = %d, want 4 (unconfirmed, rejected, two applied)", transfers)
	}
	completes := cluster.driver.drainChanges(membership.NodeDrainActionComplete)
	if len(completes) != 1 {
		t.Fatalf("COMPLETE proposals = %+v, want exactly one", completes)
	}
	cluster.runtime.drain.sync()
	if got := cluster.management.NodeState(); got != managementports.NodeStateSafeToStop {
		t.Fatalf("NodeState after the barrier committed = %s, want %s", got, managementports.NodeStateSafeToStop)
	}
	if cluster.runtime.drain.admissionOpen() {
		t.Fatal("admission reopened after the drain completed")
	}
}

// Traceability: DEP-02, DEP-08; docs/specs/drain-and-rolling-update.md §2.2, §6.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-007
func TestClusterDrainWorkerHoldsSessionVerificationUntilQuorumReadAndProjectionRecover(t *testing.T) {
	cluster := newDrainRunCluster(t, drainRunLiveSpec(0), nodedrain.PhaseVerifyingSessionState, 0, true)
	cluster.driver.setReadError(membership.ErrQuorumUnavailable)

	controller, _ := drainRunStartController(t, cluster.runtime)
	drainRunWaitFor(t, "quorum read blocker", func() bool {
		return controller.lastBlockingReason() == string(nodedrain.BlockerQuorumUnavailable)
	})
	if cluster.driver.advancedTo(nodedrain.PhaseTransferringSessions) {
		t.Fatal("worker advanced to session transfer without a quorum read")
	}

	cluster.runtime.initialized.Store(false)
	cluster.driver.setReadError(nil)
	drainRunWaitFor(t, "route projection blocker", func() bool {
		return controller.lastBlockingReason() == string(nodedrain.BlockerRouteProjectionNotReady)
	})
	if cluster.driver.advancedTo(nodedrain.PhaseTransferringSessions) {
		t.Fatal("worker advanced to session transfer before the route projection was ready")
	}
	if state, _ := cluster.machine.GetNodeDrain(3); state.Phase != nodedrain.PhaseVerifyingSessionState {
		t.Fatalf("drain phase = %s while blocked, want VERIFYING_SESSION_STATE", state.Phase)
	}

	cluster.runtime.initialized.Store(true)
	drainRunWaitFor(t, "drain completion", func() bool {
		state, exists := cluster.machine.GetNodeDrain(3)
		return exists && state.Phase == nodedrain.PhaseCompleted
	})
	controller.stopAndWait()
	if state, _ := cluster.machine.GetNodeDrain(3); state.Progress.SessionsReadyForTakeover != 0 || state.Progress.SessionsTakenOver != 0 {
		t.Fatalf("progress = %+v, want no sessions for a node that owns none", state.Progress)
	}
	if got := cluster.driver.ownershipCommands(); len(got) != 0 {
		t.Fatalf("worker proposed ownership changes %+v for a node without sessions", got)
	}
}

// Traceability: DEP-08; docs/specs/drain-and-rolling-update.md §6.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-007
func TestClusterDrainBarrierTreatsUnknownWillStateAsBlockingAndPersistsProgress(t *testing.T) {
	cluster := newDrainRunCluster(t, drainRunLiveSpec(0), nodedrain.PhaseVerifyingBarrier, 0, false)
	controller, _ := drainRunStartController(t, cluster.runtime)

	drainRunWaitFor(t, "pending Will blocker", func() bool {
		return controller.lastBlockingReason() == string(nodedrain.BlockerPendingWill)
	})
	if input := controller.barrierInput(); input.PendingWills != -1 {
		t.Fatalf("PendingWills = %d, want -1 (unknown) without a connect use case", input.PendingWills)
	}
	// A blocked barrier periodically commits its measurements in the same phase.
	drainRunWaitFor(t, "progress commit during blocked barrier", func() bool {
		for _, change := range cluster.driver.drainChanges(membership.NodeDrainActionAdvance) {
			if change.Phase == nodedrain.PhaseVerifyingBarrier {
				return true
			}
		}
		return false
	})
	if completes := cluster.driver.drainChanges(membership.NodeDrainActionComplete); len(completes) != 0 {
		t.Fatalf("COMPLETE proposed while the Will state was unknown: %+v", completes)
	}
	state, _ := cluster.machine.GetNodeDrain(3)
	if state.Phase != nodedrain.PhaseVerifyingBarrier || state.Progress.UnderReplicated != 0 || state.Progress.PendingWAL != 0 {
		t.Fatalf("drain = %+v, want VERIFYING_BARRIER with replicated, applied progress", state)
	}
}

// Traceability: DEP-08; docs/specs/drain-and-rolling-update.md §6.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-007
func TestClusterDrainBarrierInputReflectsLocalFacts(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	runtime := runtimes[0] // the elected leader
	runtime.machine = readAdmissionTestMachine(t, runtimes)
	drainRunAcquire(t, runtime.machine, "tenant-a", "local", 1)
	drainRunAcquire(t, runtime.machine, "tenant-a", "remote", 2)
	for _, owner := range []struct {
		client string
		node   uint64
	}{{"local", 1}, {"remote", 2}} {
		drainRunApply(t, runtime.machine, membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant-a", ClientID: owner.client, OwnerNodeID: owner.node, Epoch: 1,
			Filter: "devices/" + owner.client, QoS: 1, Action: membership.SubscriptionActionAdd,
		}})
	}
	controller := &clusterDrainController{runtime: runtime}
	runtime.drain = controller
	management := appmgmt.NewService(appmgmt.ServiceConfig{StartedAt: time.Unix(1, 0)}, nil, nil, nil, nil)
	runtime.managementState = management
	runtime.localCommits.Store(3)

	status := runtime.node.Status()
	wantPendingWAL := uint64(math.MaxUint64)
	if status.CommitIndex >= status.AppliedIndex {
		wantPendingWAL = status.CommitIndex - status.AppliedIndex
	}

	management.SetNodeState(managementports.NodeStateDraining, nil)
	controller.admissionClosed.Store(true)
	input := controller.barrierInput()
	if !input.AdmissionClosed || !input.ReadinessFailing || input.OwnedSessions != 1 || input.OwnedSubscriptions != 1 ||
		input.PendingLocalCommits != 3 || input.PendingWills != -1 || !input.LocalLeader || input.VoterCount != 3 ||
		input.ConnectedOtherVoters != 0 || input.ActiveConnections != 0 || input.PendingWAL != wantPendingWAL {
		t.Fatalf("barrier input = %+v, want one owned session and subscription, 3 local commits, unknown Wills, local leader and no connected voters", input)
	}
	if blocker, ready := nodedrain.EvaluateBarrier(input); ready || blocker != nodedrain.BlockerSessionsOwned {
		t.Fatalf("barrier = (%s, %v), want sessions_owned first", blocker, ready)
	}

	management.SetNodeState(managementports.NodeStateReady, nil)
	if input := controller.barrierInput(); input.ReadinessFailing {
		t.Fatal("ready node reported readiness as failing")
	}
	controller.admissionClosed.Store(false)
	if blocker, ready := nodedrain.EvaluateBarrier(controller.barrierInput()); ready || blocker != nodedrain.BlockerAdmissionOpen {
		t.Fatalf("barrier with open admission = (%s, %v), want admission_open", blocker, ready)
	}
	runtime.managementState = nil
	controller.admissionClosed.Store(true)
	if input := controller.barrierInput(); !input.ReadinessFailing {
		t.Fatal("missing management state did not fail closed on readiness")
	}
}

// Traceability: DEP-08, CLU-07; docs/specs/drain-and-rolling-update.md §2.2.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-007
func TestClusterDrainLeadershipHandoffFollowsDrainPhaseAndConnectivity(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	leader, follower := runtimes[0], runtimes[1]
	machine := readAdmissionTestMachine(t, runtimes)
	leader.machine, follower.machine = machine, machine
	if err := leader.advanceDrainLeadership(); err != nil || leader.leadershipTransferTo != 0 {
		t.Fatalf("advanceDrainLeadership without a drain record = %v transferTo=%d, want nil/0", err, leader.leadershipTransferTo)
	}

	drainRunStart(t, machine, 1, nodedrain.PhaseExcludingTraffic, drainRunLiveSpec(0))
	if err := leader.advanceDrainLeadership(); err != nil || leader.leadershipTransferTo != 0 {
		t.Fatalf("advanceDrainLeadership before session transfer = %v transferTo=%d, want nil/0", err, leader.leadershipTransferTo)
	}
	for phase := nodedrain.PhaseWaitingForLB; phase <= nodedrain.PhaseTransferringSessions; phase++ {
		drainRunAdvance(t, machine, 1, phase, uint64(time.Now().UnixMilli()))
	}
	// Peers are not connected yet, so there is no one to receive leadership.
	if err := leader.advanceDrainLeadership(); err != nil || leader.leadershipTransferTo != 0 {
		t.Fatalf("advanceDrainLeadership without connected voters = %v transferTo=%d, want nil/0", err, leader.leadershipTransferTo)
	}
	// Only the leader of the drained node hands leadership over.
	if err := follower.advanceDrainLeadership(); err != nil || follower.leadershipTransferTo != 0 {
		t.Fatalf("follower advanceDrainLeadership = %v transferTo=%d, want nil/0", err, follower.leadershipTransferTo)
	}

	drainRunServe(t, runtimes, []int{0, 1}, 0, 2)
	status := leader.node.Status()
	if err := leader.advanceDrainLeadership(); err != nil {
		t.Fatalf("advanceDrainLeadership with a connected voter: %v", err)
	}
	if leader.leadershipTransferTo != 2 || leader.leadershipTransferTerm != status.Term || leader.leadershipTransferTicks != 0 {
		t.Fatalf("leadership transfer = to:%d term:%d ticks:%d, want to:2 term:%d ticks:0",
			leader.leadershipTransferTo, leader.leadershipTransferTerm, leader.leadershipTransferTicks, status.Term)
	}
	leader.leadershipTransferTicks = 7
	if err := leader.advanceDrainLeadership(); err != nil || leader.leadershipTransferTicks != 7 {
		t.Fatalf("second advanceDrainLeadership = %v ticks=%d, want a no-op while a transfer is pending", err, leader.leadershipTransferTicks)
	}

	// A node that is no longer leader treats the stale transfer as already done.
	drainRunWaitFor(t, "follower connection to node 1", func() bool {
		peer, err := follower.transport.Status(1)
		return err == nil && peer.Connected
	})
	if err := follower.transferLocalLeadership(follower.node.Status()); err != nil || follower.leadershipTransferTo != 0 {
		t.Fatalf("transferLocalLeadership from a follower = %v transferTo=%d, want nil/0", err, follower.leadershipTransferTo)
	}
	// A failed node surfaces its error instead of silently skipping the transfer.
	staleStatus := follower.node.Status()
	_ = follower.node.Close()
	err := follower.transferLocalLeadership(staleStatus)
	if !errors.Is(err, raftadapter.ErrClosed) {
		t.Fatalf("transferLocalLeadership on a closed node = %v, want %v", err, raftadapter.ErrClosed)
	}
	if follower.leadershipTransferTo != 0 {
		t.Fatalf("failed transfer recorded target %d", follower.leadershipTransferTo)
	}
}
