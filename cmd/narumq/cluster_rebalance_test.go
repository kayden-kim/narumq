package main

import (
	"context"
	"testing"

	"narumq/internal/application/membership"
)

const (
	controllerRebalanceOperation = "00000000-0000-4000-8000-000000000301"
	controllerRebalanceCommand   = "00000000-0000-4000-8000-000000000302"
)

// CLU-13 controller lifecycle and admission behavior.

func controllerRebalanceTestMachine(t *testing.T, terminal bool) *membership.StateMachine {
	t.Helper()
	members := []membership.MemberIdentityDTO{
		{MemberID: 1, ClusterID: "cluster-a", Address: "node1.example:19001", DNSSAN: "node1.example", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.2.3", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter, Features: []string{membership.ConnectionRebalanceFeature}},
		{MemberID: 2, ClusterID: "cluster-a", Address: "node2.example:19002", DNSSAN: "node2.example", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.2.3", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter, Features: []string{membership.ConnectionRebalanceFeature}},
		{MemberID: 3, ClusterID: "cluster-a", Address: "node3.example:19003", DNSSAN: "node3.example", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.2.3", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter, Features: []string{membership.ConnectionRebalanceFeature}},
	}
	machine := membership.NewStateMachine()
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	activated, err := machine.Apply(2, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: controllerRebalanceCommand,
		FeatureActivation: &membership.FeatureActivationDTO{Feature: membership.ConnectionRebalanceFeature},
	})
	if err != nil || activated.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("activate rebalance feature: result=%+v err=%v", activated, err)
	}
	change := membership.ConnectionRebalanceChangeDTO{
		Action: membership.ConnectionRebalanceActionStart, RebalanceOperationID: controllerRebalanceOperation,
		IdempotencyKey: "controller-rebalance", ServerReference: "lb.example:1883",
		MaxDeviationConnections: 1, ConnectionRatePerSecond: 5,
		LoadBalancerPropagationDelayMillis: 100, TimeoutMillis: 10_000, ProposedAtUnixMillis: 100,
	}
	started, err := machine.Apply(3, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: controllerRebalanceOperation,
		ConnectionRebalance: &change,
	})
	if err != nil || started.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("start rebalance: result=%+v err=%v", started, err)
	}
	if terminal {
		cancel := membership.ConnectionRebalanceChangeDTO{
			Action: membership.ConnectionRebalanceActionCancel, RebalanceOperationID: controllerRebalanceOperation,
			ProposedAtUnixMillis: 101,
		}
		result, err := machine.Apply(4, membership.CommandDTO{
			SchemaMajor: membership.SchemaMajor, OperationID: "00000000-0000-4000-8000-000000000304",
			ConnectionRebalance: &cancel,
		})
		if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
			t.Fatalf("cancel rebalance: result=%+v err=%v", result, err)
		}
	}
	return machine
}

func TestClusterRebalanceSyncWaitsForStartAndClearsTerminalAdmission(t *testing.T) {
	machine := controllerRebalanceTestMachine(t, false)
	controller := &clusterRebalanceController{runtime: &clusterRuntime{config: clusterConfigFile{NodeID: 1}, machine: machine}}
	controller.sync()
	if controller.runningID != "" {
		t.Fatalf("sync before start launched operation %q", controller.runningID)
	}
	controller.admissionClosed.Store(true)
	terminalMachine := controllerRebalanceTestMachine(t, true)
	controller.runtime.machine = terminalMachine
	controller.sync()
	if !controller.admissionOpen() || controller.runningID != "" {
		t.Fatalf("terminal sync left admission closed=%v runningID=%q", !controller.admissionOpen(), controller.runningID)
	}
}

func TestClusterRebalanceSyncStartsLocalParticipantAfterStart(t *testing.T) {
	machine := controllerRebalanceTestMachine(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	controller := &clusterRebalanceController{runtime: &clusterRuntime{config: clusterConfigFile{NodeID: 1}, machine: machine}}
	controller.start(ctx)
	if controller.runningID != controllerRebalanceOperation {
		cancel()
		controller.wg.Wait()
		t.Fatalf("started worker operation=%q", controller.runningID)
	}
	cancel()
	controller.wg.Wait()
	if controller.runningID != "" || !controller.admissionOpen() {
		t.Fatalf("cancelled worker state runningID=%q admissionOpen=%v", controller.runningID, controller.admissionOpen())
	}
}

func TestClusterAdmissionOpenRequiresBothControllers(t *testing.T) {
	runtime := &clusterRuntime{
		drain: &clusterDrainController{}, rebalance: &clusterRebalanceController{},
	}
	if !runtime.clusterAdmissionOpen() {
		t.Fatal("admission should be open when both controllers are open")
	}
	runtime.drain.admissionClosed.Store(true)
	if runtime.clusterAdmissionOpen() {
		t.Fatal("admission remained open while drain was closed")
	}
	runtime.drain.admissionClosed.Store(false)
	runtime.rebalance.admissionClosed.Store(true)
	if runtime.clusterAdmissionOpen() {
		t.Fatal("admission remained open while rebalance was closed")
	}
}

func TestClusterRebalanceOldWorkerCannotClearReplacementAdmission(t *testing.T) {
	controller := &clusterRebalanceController{runningID: "replacement"}
	controller.admissionClosed.Store(true)
	controller.finish("cancelled-old")
	controller.setAdmission("cancelled-old", false)
	if controller.runningID != "replacement" || controller.admissionOpen() {
		t.Fatalf("old worker changed replacement state: runningID=%q admissionOpen=%v", controller.runningID, controller.admissionOpen())
	}
	controller.finish("replacement")
	if controller.runningID != "" || !controller.admissionOpen() {
		t.Fatalf("current worker did not clear its state: runningID=%q admissionOpen=%v", controller.runningID, controller.admissionOpen())
	}
}

func TestClusterRebalanceNilAndMissingRuntimeGuards(t *testing.T) {
	var missing *clusterRebalanceController
	missing.start(nil)
	missing.start(context.Background())
	missing.stopAndWait()
	missing.sync()
	if !missing.admissionOpen() || missing.currentServerReference() != "" {
		t.Fatal("nil rebalance controller exposed closed admission or a server reference")
	}

	controller := &clusterRebalanceController{}
	controller.sync()
	controller.runtime = &clusterRuntime{}
	controller.sync()
	if !controller.admissionOpen() {
		t.Fatal("controller without a machine closed admission")
	}
}

func TestClusterRebalanceSyncClearsAdmissionWithoutLocalParticipant(t *testing.T) {
	inactive := &clusterRebalanceController{
		runtime:         &clusterRuntime{machine: membership.NewStateMachine()},
		runningID:       "previous",
		serverReference: "old-lb.example:1883",
	}
	inactive.admissionClosed.Store(true)
	inactive.sync()
	if !inactive.admissionOpen() || inactive.runningID != "" || inactive.currentServerReference() != "" {
		t.Fatalf("inactive sync left admission closed=%v running=%q serverReference=%q",
			!inactive.admissionOpen(), inactive.runningID, inactive.currentServerReference())
	}

	nonparticipant := &clusterRebalanceController{
		runtime:         &clusterRuntime{config: clusterConfigFile{NodeID: 4}, machine: controllerRebalanceTestMachine(t, false)},
		runningID:       "previous",
		serverReference: "old-lb.example:1883",
	}
	nonparticipant.admissionClosed.Store(true)
	nonparticipant.sync()
	if !nonparticipant.admissionOpen() || nonparticipant.runningID != "" ||
		nonparticipant.currentServerReference() != "lb.example:1883" {
		t.Fatalf("nonparticipant sync left admission closed=%v running=%q serverReference=%q",
			!nonparticipant.admissionOpen(), nonparticipant.runningID, nonparticipant.currentServerReference())
	}
}

func TestClusterRebalanceSyncDoesNotLaunchDuplicateWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	controller := &clusterRebalanceController{
		runtime:   &clusterRuntime{config: clusterConfigFile{NodeID: 1}, machine: controllerRebalanceTestMachine(t, false)},
		ctx:       ctx,
		runningID: controllerRebalanceOperation,
	}
	controller.admissionClosed.Store(true)
	controller.sync()
	if controller.runningID != controllerRebalanceOperation || controller.admissionOpen() {
		t.Fatalf("duplicate sync changed worker state: running=%q admissionOpen=%v",
			controller.runningID, controller.admissionOpen())
	}
	controller.stopAndWait()
}

func TestClusterAdmissionAndConnectedVoterNilRuntimeDefaults(t *testing.T) {
	var runtime *clusterRuntime
	if !runtime.clusterAdmissionOpen() {
		t.Fatal("nil runtime should leave admission open")
	}
	if got := runtime.connectedOtherVoters(); len(got) != 0 {
		t.Fatalf("nil runtime connected voters = %v, want none", got)
	}
	if got := (&clusterRuntime{}).connectedOtherVoters(); len(got) != 0 {
		t.Fatalf("runtime without node or peer transport connected voters = %v, want none", got)
	}
}
func TestClusterRebalanceAdmissionChangesOnlyForMatchingOperation(t *testing.T) {
	controller := &clusterRebalanceController{runningID: "active-operation"}
	controller.admissionClosed.Store(true)

	controller.setAdmission("stale-operation", false)
	if controller.admissionOpen() || controller.runningID != "active-operation" {
		t.Fatalf("stale operation changed admission: running=%q admissionOpen=%v",
			controller.runningID, controller.admissionOpen())
	}

	controller.setAdmission("active-operation", false)
	if !controller.admissionOpen() || controller.runningID != "active-operation" {
		t.Fatalf("matching operation did not open admission: running=%q admissionOpen=%v",
			controller.runningID, controller.admissionOpen())
	}

	controller.setAdmission("active-operation", true)
	if controller.admissionOpen() || controller.runningID != "active-operation" {
		t.Fatalf("matching operation did not close admission: running=%q admissionOpen=%v",
			controller.runningID, controller.admissionOpen())
	}
}

func TestClusterAdmissionOpenAllowsAbsentControllers(t *testing.T) {
	for _, runtime := range []*clusterRuntime{
		{},
		{drain: &clusterDrainController{}},
		{rebalance: &clusterRebalanceController{}},
	} {
		if !runtime.clusterAdmissionOpen() {
			t.Fatalf("admission should be open with an absent controller: runtime=%+v", runtime)
		}
	}

	runtime := &clusterRuntime{drain: &clusterDrainController{}, rebalance: &clusterRebalanceController{}}
	runtime.drain.admissionClosed.Store(true)
	runtime.rebalance.admissionClosed.Store(true)
	if runtime.clusterAdmissionOpen() {
		t.Fatal("admission should remain closed while both controllers are closed")
	}
}
