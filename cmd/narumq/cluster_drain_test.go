package main

import (
	"context"
	"sync"
	"testing"
	"time"

	appmgmt "narumq/internal/application/management"
	"narumq/internal/application/membership"
	managementports "narumq/internal/application/ports/management"
	"narumq/internal/domain/nodedrain"
)

const (
	controllerDrainOperation = "00000000-0000-4000-8000-000000000201"
	controllerCommandID      = "00000000-0000-4000-8000-000000000202"
)

func controllerDrainTestMachine(t *testing.T, phase nodedrain.Phase) *membership.StateMachine {
	t.Helper()
	machine := membership.NewStateMachine()
	members := []membership.MemberIdentityDTO{
		{MemberID: 1, ClusterID: "cluster-a", Address: "node1.example:19001", DNSSAN: "node1.example", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.2.3", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
		{MemberID: 2, ClusterID: "cluster-a", Address: "node2.example:19002", DNSSAN: "node2.example", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.2.3", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
		{MemberID: 3, ClusterID: "cluster-a", Address: "node3.example:19003", DNSSAN: "node3.example", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.2.3", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
	}
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatalf("InitializeBootstrap: %v", err)
	}
	start := membership.NodeDrainChangeDTO{
		Action: membership.NodeDrainActionStart, MemberID: 1, DrainOperationID: controllerDrainOperation,
		IdempotencyKey: "controller-drain", ConnectionRatePerSecond: 2,
		LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 5_000, ProposedAtUnixMillis: 1_000,
	}
	if _, err := machine.Apply(2, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: controllerCommandID, NodeDrain: &start,
	}); err != nil {
		t.Fatalf("apply START: %v", err)
	}
	phases := []nodedrain.Phase{
		nodedrain.PhaseWaitingForLB,
		nodedrain.PhaseVerifyingSessionState,
		nodedrain.PhaseTransferringSessions,
		nodedrain.PhaseVerifyingBarrier,
	}
	if phase == nodedrain.PhaseFailed {
		change := membership.NodeDrainChangeDTO{
			Action: membership.NodeDrainActionFail, MemberID: 1, DrainOperationID: controllerDrainOperation,
			FailureCode: "drain_timeout", BlockingReason: "quorum_unavailable", ProposedAtUnixMillis: 1_001,
		}
		if _, err := machine.Apply(3, membership.CommandDTO{
			SchemaMajor: membership.SchemaMajor, OperationID: controllerCommandID, NodeDrain: &change,
		}); err != nil {
			t.Fatalf("apply FAIL: %v", err)
		}
		return machine
	}
	if phase == nodedrain.PhaseExcludingTraffic {
		return machine
	}
	index := uint64(3)
	for _, next := range phases {
		change := membership.NodeDrainChangeDTO{
			Action: membership.NodeDrainActionAdvance, MemberID: 1, DrainOperationID: controllerDrainOperation,
			Phase: next, ProposedAtUnixMillis: 1_000 + index,
		}
		if _, err := machine.Apply(index, membership.CommandDTO{
			SchemaMajor: membership.SchemaMajor, OperationID: controllerCommandID, NodeDrain: &change,
		}); err != nil {
			t.Fatalf("apply ADVANCE %s: %v", next, err)
		}
		index++
		if phase == next {
			return machine
		}
	}
	if phase == nodedrain.PhaseCompleted {
		change := membership.NodeDrainChangeDTO{
			Action: membership.NodeDrainActionComplete, MemberID: 1, DrainOperationID: controllerDrainOperation,
			ProposedAtUnixMillis: 1_010,
		}
		if _, err := machine.Apply(index, membership.CommandDTO{
			SchemaMajor: membership.SchemaMajor, OperationID: controllerCommandID, NodeDrain: &change,
		}); err != nil {
			t.Fatalf("apply COMPLETE: %v", err)
		}
		return machine
	}
	if phase == nodedrain.PhaseCancelled {
		change := membership.NodeDrainChangeDTO{
			Action: membership.NodeDrainActionCancel, MemberID: 1, DrainOperationID: controllerDrainOperation,
			ProposedAtUnixMillis: 1_010,
		}
		if _, err := machine.Apply(index, membership.CommandDTO{
			SchemaMajor: membership.SchemaMajor, OperationID: controllerCommandID, NodeDrain: &change,
		}); err != nil {
			t.Fatalf("apply CANCEL: %v", err)
		}
		return machine
	}
	t.Fatalf("unsupported controller test phase %s", phase)
	return nil
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §1, §10.
func TestClusterDrainSyncProjectsLifecycleAndAdmission(t *testing.T) {
	tests := []struct {
		name                  string
		phase                 nodedrain.Phase
		completingOperationID string
		priorRecord           bool
		wantState             managementports.NodeState
		wantAdmissionOpen     bool
	}{
		{name: "excluding", phase: nodedrain.PhaseExcludingTraffic, wantState: managementports.NodeStateDraining, wantAdmissionOpen: false},
		{name: "waiting", phase: nodedrain.PhaseWaitingForLB, wantState: managementports.NodeStateDraining, wantAdmissionOpen: false},
		{name: "verifying session", phase: nodedrain.PhaseVerifyingSessionState, wantState: managementports.NodeStateDraining, wantAdmissionOpen: false},
		{name: "transferring", phase: nodedrain.PhaseTransferringSessions, wantState: managementports.NodeStateDraining, wantAdmissionOpen: false},
		{name: "barrier", phase: nodedrain.PhaseVerifyingBarrier, wantState: managementports.NodeStateDraining, wantAdmissionOpen: false},
		{name: "failed", phase: nodedrain.PhaseFailed, wantState: managementports.NodeStateDraining, wantAdmissionOpen: false},
		{name: "completed by this process", phase: nodedrain.PhaseCompleted, completingOperationID: controllerDrainOperation, wantState: managementports.NodeStateSafeToStop, wantAdmissionOpen: false},
		{name: "completed before restart", phase: nodedrain.PhaseCompleted, wantState: managementports.NodeStateJoining, wantAdmissionOpen: false},
		{name: "cancelled", phase: nodedrain.PhaseCancelled, priorRecord: true, wantState: managementports.NodeStateJoining, wantAdmissionOpen: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine := controllerDrainTestMachine(t, test.phase)
			managementState := appmgmt.NewService(appmgmt.ServiceConfig{StartedAt: time.Unix(1, 0)}, nil, nil, nil, nil)
			managementState.SetNodeState(managementports.NodeStateReady, nil)
			runtime := &clusterRuntime{config: clusterConfigFile{NodeID: 1}, machine: machine, managementState: managementState}
			controller := &clusterDrainController{runtime: runtime, completingOperationID: test.completingOperationID}
			if test.priorRecord {
				controller.hasRecord = true
				controller.lastPhase = nodedrain.PhaseExcludingTraffic
			}
			runtime.drain = controller
			controller.sync()
			if got := managementState.NodeState(); got != test.wantState {
				t.Fatalf("NodeState = %s, want %s", got, test.wantState)
			}
			if got := controller.admissionOpen(); got != test.wantAdmissionOpen {
				t.Fatalf("admissionOpen = %v, want %v", got, test.wantAdmissionOpen)
			}
			if controller.runningOperationID != "" || controller.clearing {
				t.Fatalf("sync without start launched worker: running=%q clearing=%v", controller.runningOperationID, controller.clearing)
			}
		})
	}
}

type drainWaitTestContext struct {
	done    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (ctx *drainWaitTestContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (ctx *drainWaitTestContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.done
}

func (ctx *drainWaitTestContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

func (ctx *drainWaitTestContext) Value(any) any {
	return nil
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §3.
func TestClusterDrainNilGuardsAndCancelledRunCleanup(t *testing.T) {
	var missing *clusterDrainController
	missing.start(nil)
	missing.stopAndWait()
	missing.sync()
	if !missing.admissionOpen() || missing.currentServerReference() != "" {
		t.Fatal("nil drain controller exposed closed admission or a server reference")
	}

	controller := &clusterDrainController{}
	controller.sync()
	controller.runtime = &clusterRuntime{}
	controller.sync()
	if !controller.admissionOpen() || controller.activeConnections() != 0 {
		t.Fatal("controller without a machine or MQTT server changed its state")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	controller.runningOperationID = "old-operation"
	controller.run(ctx, "old-operation")
	if controller.runningOperationID != "" {
		t.Fatalf("cancelled worker retained its own operation ID %q", controller.runningOperationID)
	}
	controller.runningOperationID = "replacement"
	controller.run(ctx, "old-operation")
	if controller.runningOperationID != "replacement" {
		t.Fatalf("cancelled worker cleared replacement operation %q", controller.runningOperationID)
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §6.
func TestDrainWaitHandlesImmediateAndSynchronizedCancellation(t *testing.T) {
	if waitDrain(nil, time.Second) {
		t.Fatal("waitDrain(nil) = true, want false")
	}
	if !waitDrain(context.Background(), 0) {
		t.Fatal("waitDrain with zero delay = false, want true")
	}

	ctx := &drainWaitTestContext{done: make(chan struct{}), entered: make(chan struct{})}
	result := make(chan bool, 1)
	go func() { result <- waitDrain(ctx, time.Hour) }()
	select {
	case <-ctx.entered:
	case <-time.After(time.Second):
		t.Fatal("waitDrain did not enter its cancellation select")
	}
	close(ctx.done)
	select {
	case got := <-result:
		if got {
			t.Fatal("waitDrain returned true after context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("waitDrain did not return after context cancellation")
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §2.2.
func TestMergeDrainProgressPreservesLargestTransferCounts(t *testing.T) {
	tests := []struct {
		name      string
		current   membership.NodeDrainProgressDTO
		committed membership.NodeDrainProgressDTO
		wantTaken uint64
		wantFail  uint64
	}{
		{
			name: "keep locally observed counts",
			current: membership.NodeDrainProgressDTO{
				SessionsTakenOver: 5, SessionsFailed: 4,
			},
			committed: membership.NodeDrainProgressDTO{
				ConnectionsInitial: 12, ConnectionsRemaining: 3, SessionsReadyForTakeover: 8,
				SessionsTakenOver: 2, SessionsFailed: 1, PendingWAL: 4, UnderReplicated: 1,
			},
			wantTaken: 5, wantFail: 4,
		},
		{
			name: "accept larger committed counts",
			current: membership.NodeDrainProgressDTO{
				SessionsTakenOver: 2, SessionsFailed: 1,
			},
			committed: membership.NodeDrainProgressDTO{
				ConnectionsInitial: 12, ConnectionsRemaining: 3, SessionsReadyForTakeover: 8,
				SessionsTakenOver: 5, SessionsFailed: 4, PendingWAL: 4, UnderReplicated: 1,
			},
			wantTaken: 5, wantFail: 4,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := mergeDrainProgress(test.current, test.committed)
			if got.SessionsTakenOver != test.wantTaken || got.SessionsFailed != test.wantFail ||
				got.ConnectionsInitial != test.committed.ConnectionsInitial ||
				got.ConnectionsRemaining != test.committed.ConnectionsRemaining ||
				got.SessionsReadyForTakeover != test.committed.SessionsReadyForTakeover ||
				got.PendingWAL != test.committed.PendingWAL || got.UnderReplicated != test.committed.UnderReplicated {
				t.Fatalf("mergeDrainProgress() = %+v, want committed progress with counts %d/%d",
					got, test.wantTaken, test.wantFail)
			}
		})
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §2.2, §3.
func TestClusterDrainCountsOnlyLocallyOwnedSessions(t *testing.T) {
	config := validBootstrapClusterConfig()
	runtime, stop := newTestClusterOfflineRuntime(t, config)
	defer stop()

	_, err := runtime.machine.Apply(5, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: "00000000-0000-4000-8000-000000000203",
		Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "client-b", OwnerNodeID: 2, Epoch: 1,
			ExpectedEpoch: 0, Action: membership.OwnershipActionAcquire,
		},
	})
	if err != nil {
		t.Fatalf("apply remote ownership: %v", err)
	}

	controller := &clusterDrainController{runtime: runtime}
	if got := controller.ownedSessionCount(); got != 1 {
		t.Fatalf("ownedSessionCount() = %d, want only the one session owned by node %d", got, config.NodeID)
	}
	if got := controller.activeConnections(); got != 0 {
		t.Fatalf("activeConnections() without an MQTT server = %d, want 0", got)
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §1, §10.
func TestClusterDrainStartIgnoresNilContextAndMissingRuntime(t *testing.T) {
	controller := &clusterDrainController{}
	controller.start(nil)
	if controller.ctx != nil {
		t.Fatal("start(nil) stored a context")
	}

	ctx := context.Background()
	controller.start(ctx)
	if controller.ctx != ctx {
		t.Fatal("start with a context did not retain it when runtime is unavailable")
	}
	if !controller.admissionOpen() || controller.currentServerReference() != "" {
		t.Fatal("start without a runtime changed drain admission or server reference")
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §2.2.
func TestClusterDrainBarrierBlocksWhenOtherVotersAreDisconnected(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	runtime := runtimes[0]
	runtime.machine = readAdmissionTestMachine(t, runtimes)
	controller := &clusterDrainController{runtime: runtime}
	controller.admissionClosed.Store(true)

	input := controller.barrierInput()
	blocker, ready := nodedrain.EvaluateBarrier(input)
	if ready || blocker != nodedrain.BlockerInsufficientReplicas {
		t.Fatalf("drain barrier = (%s, %v), want insufficient replicas and not ready", blocker, ready)
	}
	if got := nodedrain.UnderReplicated(input.VoterCount, input.ConnectedOtherVoters); got != 2 {
		t.Fatalf("under-replicated voters = %d, want two disconnected voters", got)
	}
}
