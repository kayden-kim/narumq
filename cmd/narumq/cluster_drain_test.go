package main

import (
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
