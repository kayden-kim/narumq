package integration

import (
	"net/http"
	"testing"
	"time"
)

// Traceability: API-04/DEP-10; docs/specs/drain-and-rolling-update.md §7, §8, §8.1.
//
// Kubernetes runs the preStop hook before it kills a container for a failed
// startup probe. The hook starts a drain, and the restarted container must then
// reach startup without any readiness request: the kubelet runs readiness only
// after startup succeeds. The test replays that sequence and the plain crash
// restart of the 2026-10-15 power-loss run with real broker processes.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-004
func TestDynamicClusterRestartStartupOnlyAfterProbeKillAndCrashProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	processes := clusterDrainProcessStartCluster(t, fixture)
	ctx := fixture.ctx
	request := clusterDrainProcessRequest{
		ConnectionRatePerSecond: 2, LoadBalancerPropagationDelayMillis: 500, TimeoutMillis: 60000,
	}

	// The kubelet killed the container after the startup budget ran out, and the
	// preStop hook (GET 404, then POST) drained the node before the kill.
	status, body, err := clusterDrainProcessStartOn(ctx, processes[2], 3, request, "prestop-3-startup-timeout")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("preStop drain start status=%d body=%s err=%v", status, body, err)
	}
	completed := clusterDrainProcessWaitForPhase(t, ctx, processes[2], 3, "COMPLETED")
	if !completed.SafeToStop || completed.State != "SAFE_TO_STOP" {
		t.Fatalf("preStop drain=%+v, want SAFE_TO_STOP", completed)
	}
	if err := processes[2].crash(5 * time.Second); err != nil {
		t.Fatalf("kill node 3 after preStop: %v", err)
	}
	restarted, err := fixture.harness.start("startup-only after probe kill", fixture.bootstrapConfig(3), fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart node 3 after probe kill: %v", err)
	}
	processes[2] = restarted
	assertStartupOnlyRecovery(t, fixture, restarted)

	// Power loss without any drain: the replacement node starts from the WAL on
	// the re-attached volume and has no drain record.
	if err := processes[2].crash(5 * time.Second); err != nil {
		t.Fatalf("kill node 3 without drain: %v", err)
	}
	restarted, err = fixture.harness.start("startup-only after power loss", fixture.bootstrapConfig(3), fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart node 3 after power loss: %v", err)
	}
	processes[2] = restarted
	assertStartupOnlyRecovery(t, fixture, restarted)
}

// assertStartupOnlyRecovery polls startup only, as the kubelet does, and then
// checks that readiness is up and no drain record is left for node 3.
func assertStartupOnlyRecovery(t *testing.T, fixture *clusterMembershipProcessFixture, process *clusterMembershipProcess) {
	t.Helper()
	clusterMembershipProcessWaitForStatus(t, fixture.ctx, process, "/health/startup", http.StatusOK)
	clusterMembershipProcessWaitForStatus(t, fixture.ctx, process, "/health/ready", http.StatusOK)
	clusterDrainProcessWaitForNotFound(t, fixture.ctx, process, 3)
	clusterDrainProcessAssertNodeState(t, fixture.ctx, process, 3, "READY")
}
