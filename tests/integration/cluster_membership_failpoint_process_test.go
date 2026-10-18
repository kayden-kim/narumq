package integration

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

type clusterMembershipFailpointSupervisor struct {
	t       *testing.T
	ctx     context.Context
	fixture *clusterMembershipProcessFixture
	nodes   map[uint64]*clusterMembershipProcess
	configs map[uint64]string
	armed   map[uint64]string
	hits    []string
}

func newClusterMembershipFailpointSupervisor(t *testing.T, ctx context.Context, fixture *clusterMembershipProcessFixture) *clusterMembershipFailpointSupervisor {
	return &clusterMembershipFailpointSupervisor{
		t:       t,
		ctx:     ctx,
		fixture: fixture,
		nodes:   make(map[uint64]*clusterMembershipProcess),
		configs: make(map[uint64]string),
		armed:   make(map[uint64]string),
	}
}

func (s *clusterMembershipFailpointSupervisor) start(id uint64, configPath, point string) {
	s.t.Helper()
	s.configs[id] = configPath
	name := fmt.Sprintf("node %d", id)
	mgmtAddr := s.fixture.managementAddresses[id-1]
	var proc *clusterMembershipProcess
	var err error
	if point != "" {
		s.armed[id] = point
		proc, err = s.fixture.harness.startArmed(name, configPath, mgmtAddr, point)
	} else {
		proc, err = s.fixture.harness.start(name, configPath, mgmtAddr)
	}
	if err != nil {
		s.t.Fatalf("start node %d: %v", id, err)
	}
	s.nodes[id] = proc
}

func (s *clusterMembershipFailpointSupervisor) rearm(id uint64, point string) {
	s.t.Helper()
	proc, exists := s.nodes[id]
	if !exists || proc == nil {
		s.t.Fatalf("rearm node %d does not exist", id)
	}
	if err := proc.stop(5 * time.Second); err != nil {
		s.t.Fatalf("stop node %d before rearming: %v", id, err)
	}
	s.armed[id] = point
	name := fmt.Sprintf("node %d", id)
	mgmtAddr := s.fixture.managementAddresses[id-1]
	newProc, err := s.fixture.harness.startArmed(name, s.configs[id], mgmtAddr, point)
	if err != nil {
		s.t.Fatalf("restart armed node %d: %v", id, err)
	}
	s.nodes[id] = newProc
	clusterMembershipProcessWaitForStatus(s.t, s.ctx, newProc, "/health/ready", http.StatusOK)
}

func (s *clusterMembershipFailpointSupervisor) rearmSnapshotFinalization(id uint64, point string) {
	s.t.Helper()
	proc, exists := s.nodes[id]
	if !exists || proc == nil {
		s.t.Fatalf("rearm node %d does not exist", id)
	}
	if err := proc.stop(5 * time.Second); err != nil {
		s.t.Fatalf("stop node %d before rearming: %v", id, err)
	}
	name := fmt.Sprintf("node %d", id)
	mgmtAddr := s.fixture.managementAddresses[id-1]
	deadline := time.Now().Add(time.Minute)
	for attempt := 0; attempt < 2; attempt++ {
		s.armed[id] = point
		armed, err := s.fixture.harness.startArmed(name, s.configs[id], mgmtAddr, point)
		if err != nil {
			s.t.Fatalf("restart armed node %d: %v", id, err)
		}
		s.nodes[id] = armed
		if s.waitForArmedReady(armed, point, deadline) {
			return
		}
		clusterMembershipProcessRequireFailpointExit(s.t, armed, point)
		delete(s.armed, id)
		if attempt == 1 {
			s.t.Fatalf("snapshot finalization failpoint fired during both startup attempts:\n%s", armed.logs.String())
			return
		}

		// One recovery snapshot may finalize before readiness. Verify its restart,
		// stop that recovered process, then arm the learner-transition snapshot.
		recovered, err := s.fixture.harness.start(name, s.configs[id], mgmtAddr)
		if err != nil {
			s.t.Fatalf("restart node %d after recovery snapshot: %v", id, err)
		}
		s.nodes[id] = recovered
		ctx, cancel := context.WithDeadline(s.ctx, deadline)
		clusterMembershipProcessWaitForStatus(s.t, ctx, recovered, "/health/ready", http.StatusOK)
		cancel()
		if err := recovered.stop(5 * time.Second); err != nil {
			s.t.Fatalf("stop node %d after recovery snapshot: %v", id, err)
		}
	}
}

func (s *clusterMembershipFailpointSupervisor) waitForArmedReady(proc *clusterMembershipProcess, point string, deadline time.Time) bool {
	s.t.Helper()
	var lastStatus int
	var lastBody []byte
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-proc.done:
			return false
		default:
		}
		lastStatus, lastBody, lastErr = clusterMembershipProcessRequest(
			s.ctx, http.MethodGet, proc.managementURL()+"/health/ready", nil, "", "",
		)
		if lastErr == nil && lastStatus == http.StatusOK {
			select {
			case <-proc.done:
				return false
			default:
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.t.Fatalf("node %s did not become ready with failpoint %s armed: status=%d body=%s err=%v",
		proc.name, point, lastStatus, lastBody, lastErr)
	return false
}
func (s *clusterMembershipFailpointSupervisor) stop(id uint64) {
	s.t.Helper()
	proc := s.nodes[id]
	if proc != nil {
		delete(s.nodes, id)
		delete(s.armed, id)
		_ = proc.stop(5 * time.Second)
	}
}

func (s *clusterMembershipFailpointSupervisor) poll() {
	s.t.Helper()
	for id, point := range s.armed {
		proc := s.nodes[id]
		if proc == nil {
			continue
		}
		select {
		case <-proc.done:
			clusterMembershipProcessRequireFailpointExit(s.t, proc, point)
			s.hits = append(s.hits, point)
			delete(s.armed, id)
			name := fmt.Sprintf("node %d", id)
			mgmtAddr := s.fixture.managementAddresses[id-1]
			newProc, err := s.fixture.harness.start(name, s.configs[id], mgmtAddr)
			if err != nil {
				s.t.Fatalf("restart node %d disarmed: %v", id, err)
			}
			s.nodes[id] = newProc
		default:
		}
	}
	for id, proc := range s.nodes {
		if _, isArmed := s.armed[id]; !isArmed && proc != nil {
			select {
			case <-proc.done:
				s.t.Fatalf("disarmed node %d exited unexpectedly (exit=%v):\n%s", id, proc.waitErr, proc.logs.String())
			default:
			}
		}
	}
}

func (s *clusterMembershipFailpointSupervisor) wait(description string, probe func() (bool, string, error)) {
	s.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		s.poll()
		complete, detail, err := probe()
		if err != nil {
			s.t.Fatalf("%s probe failed: %v", description, err)
		}
		if complete {
			return
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("%s timed out: %s", description, detail)
		}
		select {
		case <-s.ctx.Done():
			s.t.Fatalf("%s context cancelled: %v", description, s.ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *clusterMembershipFailpointSupervisor) waitForHits(points ...string) {
	s.t.Helper()
	for _, pt := range points {
		point := pt
		s.wait(fmt.Sprintf("wait for failpoint hit %s", point), func() (bool, string, error) {
			for _, h := range s.hits {
				if h == point {
					return true, "hit recorded", nil
				}
			}
			return false, fmt.Sprintf("failpoint %s was not reached", point), nil
		})
	}
}
func (s *clusterMembershipFailpointSupervisor) failpointHitCount(point string) int {
	count := 0
	for _, hit := range s.hits {
		if hit == point {
			count++
		}
	}
	return count
}

func (s *clusterMembershipFailpointSupervisor) waitForHitCount(point string, want int) {
	s.t.Helper()
	s.wait(fmt.Sprintf("wait for failpoint %s hit count %d", point, want), func() (bool, string, error) {
		got := s.failpointHitCount(point)
		return got >= want, fmt.Sprintf("hit count=%d want at least %d", got, want), nil
	})
}

func (s *clusterMembershipFailpointSupervisor) findLeader(voterIDs []uint64) (uint64, string) {
	for _, id := range voterIDs {
		proc := s.nodes[id]
		if proc == nil {
			continue
		}
		summary, status, _, err := clusterMembershipProcessGetSummary(s.ctx, proc.managementURL(), clusterMembershipProcessAdmin)
		if err != nil || status != http.StatusOK || summary.LeaderNodeID == nil {
			continue
		}
		leaderID, parseErr := strconv.ParseUint(*summary.LeaderNodeID, 10, 64)
		if parseErr == nil && slices.Contains(voterIDs, leaderID) {
			if leaderProc, ok := s.nodes[leaderID]; ok && leaderProc != nil {
				return leaderID, leaderProc.managementURL()
			}
		}
	}
	return 0, ""
}

func (s *clusterMembershipFailpointSupervisor) waitForLeader(voterIDs []uint64) (uint64, string) {
	s.t.Helper()
	var leaderID uint64
	var leaderURL string
	s.wait("leader election", func() (bool, string, error) {
		id, url := s.findLeader(voterIDs)
		if id != 0 && url != "" {
			leaderID = id
			leaderURL = url
			return true, fmt.Sprintf("leader is node %d", id), nil
		}
		return false, "no leader found yet", nil
	})
	return leaderID, leaderURL
}

func (s *clusterMembershipFailpointSupervisor) waitForOperation(nodeID uint64, opID, phase string) {
	s.t.Helper()
	var diagnostics string
	lastDiagnostic := time.Time{}
	s.wait(fmt.Sprintf("operation %s phase %s", opID, phase), func() (bool, string, error) {
		proc := s.nodes[nodeID]
		if proc == nil {
			return false, fmt.Sprintf("node %d not present", nodeID), nil
		}
		status, body, err := clusterMembershipProcessRequest(s.ctx, http.MethodGet,
			proc.managementURL()+"/api/v1/cluster/membership-operations/"+opID, nil, clusterMembershipProcessAdmin, "")
		if err != nil {
			return false, err.Error(), nil
		}
		if status != http.StatusOK {
			return false, fmt.Sprintf("status=%d body=%s", status, body), nil
		}
		operation, decodeErr := clusterMembershipProcessDecodeOperation(body)
		if decodeErr != nil {
			return false, fmt.Sprintf("decode error: %v", decodeErr), nil
		}
		if operation.Phase == phase {
			return true, "phase matches", nil
		}
		if time.Since(lastDiagnostic) >= 2*time.Second {
			ids := make([]uint64, 0, len(s.nodes))
			for id := range s.nodes {
				ids = append(ids, id)
			}
			slices.Sort(ids)
			details := make([]string, 0, len(ids))
			for _, id := range ids {
				node := s.nodes[id]
				if node == nil {
					continue
				}
				nodesStatus, nodesBody, nodesErr := clusterMembershipProcessRequest(s.ctx, http.MethodGet,
					node.managementURL()+"/api/v1/nodes", nil, clusterMembershipProcessAdmin, "")
				details = append(details, fmt.Sprintf("node %d nodes-status=%d body=%s err=%v", id, nodesStatus, nodesBody, nodesErr))
			}
			diagnostics = strings.Join(details, "; ")
			lastDiagnostic = time.Now()
		}
		return false, "current phase=" + operation.Phase + "; " + diagnostics, nil
	})
}

func (s *clusterMembershipFailpointSupervisor) waitForAllReady(ids []uint64) {
	s.t.Helper()
	s.wait("all members ready", func() (bool, string, error) {
		for _, id := range ids {
			proc := s.nodes[id]
			if proc == nil {
				return false, fmt.Sprintf("node %d not present", id), nil
			}
			status, _, err := clusterMembershipProcessRequest(s.ctx, http.MethodGet, proc.managementURL()+"/health/ready", nil, "", "")
			if err != nil || status != http.StatusOK {
				return false, fmt.Sprintf("node %d ready status=%d err=%v", id, status, err), nil
			}
		}
		return true, "all ready", nil
	})
}

func (s *clusterMembershipFailpointSupervisor) procs(ids []uint64) []*clusterMembershipProcess {
	result := make([]*clusterMembershipProcess, 0, len(ids))
	for _, id := range ids {
		result = append(result, s.nodes[id])
	}
	return result
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-023, cluster-session-and-delivery.md:CLU-SCN-024, performance-and-verification.md:PRF-SCN-099
func TestDynamicClusterMembershipTransitionCrashRecovery(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 6, "narumq_failpoint")
	ctx := fixture.ctx
	supervisor := newClusterMembershipFailpointSupervisor(t, ctx, fixture)

	// Cycle 1 initial armed states:
	// n1: add-learner-before-persist, n2: promote-voter-before-persist, n3: disarmed
	supervisor.start(3, fixture.bootstrapConfig(3), "")
	supervisor.start(1, fixture.bootstrapConfig(1), "add-learner-before-persist")
	supervisor.start(2, fixture.bootstrapConfig(2), "promote-voter-before-persist")

	supervisor.waitForAllReady([]uint64{1, 2, 3})
	leaderID, leaderURL := supervisor.waitForLeader([]uint64{1, 2, 3})
	if leaderID == 0 {
		t.Fatal("bootstrap did not elect a leader")
	}

	// Open subscribed session on node 3
	mqttConn := clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[2], fixture.accessToken, "handoff-client", "handoff/state",
	)
	defer mqttConn.Close()

	// --- CYCLE 1: Join 4 (seeds 1,2,3), Remove 3 ---
	// Stage 4
	leaderID, leaderURL = supervisor.waitForLeader([]uint64{1, 2, 3})
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodPost,
		leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(4), clusterMembershipProcessAdmin, "cycle-1-stage-4")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("stage joiner 4 status=%d body=%s err=%v", status, body, err)
	}
	stageOp4, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil {
		t.Fatalf("decode stage op: %v", err)
	}

	// Start joiner 4 armed: snapshot-install-before-persist
	supervisor.start(4, fixture.joinerConfig(4, []uint64{1, 2, 3}), "snapshot-install-before-persist")

	// Wait until operation SUCCEEDED on live non-joiner node
	supervisor.waitForOperation(3, stageOp4.OperationID, "SUCCEEDED")
	supervisor.waitForHits("add-learner-before-persist", "promote-voter-before-persist", "snapshot-install-before-persist")
	supervisor.waitForAllReady([]uint64{1, 2, 3, 4})

	// Rearm n1 for removal: remove-member-before-persist
	supervisor.rearm(1, "remove-member-before-persist")

	// DELETE 3 on leader
	leaderID, leaderURL = supervisor.waitForLeader([]uint64{1, 2, 3, 4})
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodDelete,
		leaderURL+"/api/v1/cluster/members/3", nil, clusterMembershipProcessAdmin, "cycle-1-remove-3")
	if err != nil || (status != http.StatusAccepted && status != http.StatusOK) {
		t.Fatalf("delete member 3 status=%d body=%s err=%v", status, body, err)
	}
	removeOp3, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil {
		t.Fatalf("decode remove op: %v", err)
	}

	// Wait for SUCCEEDED on non-removed node
	supervisor.waitForOperation(4, removeOp3.OperationID, "SUCCEEDED")
	supervisor.waitForHits("remove-member-before-persist")
	supervisor.waitForAllReady([]uint64{1, 2, 4})

	// Retired node 3 returns 503, then stop it gracefully
	readyStatus, _, _ := clusterMembershipProcessRequest(ctx, http.MethodGet, supervisor.nodes[3].managementURL()+"/health/ready", nil, "", "")
	if readyStatus != http.StatusServiceUnavailable {
		t.Fatalf("retired node 3 ready status = %d, want 503", readyStatus)
	}
	supervisor.stop(3)

	clusterMembershipProcessAssertVotersAndRetired(t, ctx, supervisor.procs([]uint64{1, 2, 4}), []string{"1", "2", "4"}, "3")
	clusterMembershipProcessWaitForRouteProjection(t, ctx, supervisor.procs([]uint64{1, 2, 4}), "handoff/state")

	// After cycle 1, held MQTT connection must be closed (handed off 3->1)
	clusterMembershipProcessAssertConnectionClosed(t, mqttConn, 5*time.Second)

	// --- CYCLE 2: Join 5 (seeds 1,2,4), Remove 1 ---
	// Rearm one at a time before stage
	supervisor.rearm(2, "add-learner-after-persist")
	supervisor.rearm(4, "promote-voter-after-persist")

	leaderID, leaderURL = supervisor.waitForLeader([]uint64{1, 2, 4})
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodPost,
		leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(5), clusterMembershipProcessAdmin, "cycle-2-stage-5")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("stage joiner 5 status=%d body=%s err=%v", status, body, err)
	}
	stageOp5, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil {
		t.Fatalf("decode stage op 5: %v", err)
	}

	// Start joiner 5 armed: snapshot-install-after-persist
	supervisor.start(5, fixture.joinerConfig(5, []uint64{1, 2, 4}), "snapshot-install-after-persist")

	supervisor.waitForOperation(4, stageOp5.OperationID, "SUCCEEDED")
	supervisor.waitForHits("add-learner-after-persist", "promote-voter-after-persist", "snapshot-install-after-persist")
	supervisor.waitForAllReady([]uint64{1, 2, 4, 5})

	// Rearm n2 for removal: remove-member-after-persist
	supervisor.rearm(2, "remove-member-after-persist")

	leaderID, leaderURL = supervisor.waitForLeader([]uint64{1, 2, 4, 5})
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodDelete,
		leaderURL+"/api/v1/cluster/members/1", nil, clusterMembershipProcessAdmin, "cycle-2-remove-1")
	if err != nil || (status != http.StatusAccepted && status != http.StatusOK) {
		t.Fatalf("delete member 1 status=%d body=%s err=%v", status, body, err)
	}
	removeOp1, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil {
		t.Fatalf("decode remove op 1: %v", err)
	}

	supervisor.waitForOperation(4, removeOp1.OperationID, "SUCCEEDED")
	supervisor.waitForHits("remove-member-after-persist")
	supervisor.waitForAllReady([]uint64{2, 4, 5})

	readyStatus, _, _ = clusterMembershipProcessRequest(ctx, http.MethodGet, supervisor.nodes[1].managementURL()+"/health/ready", nil, "", "")
	if readyStatus != http.StatusServiceUnavailable {
		t.Fatalf("retired node 1 ready status = %d, want 503", readyStatus)
	}
	supervisor.stop(1)

	clusterMembershipProcessAssertVotersAndRetired(t, ctx, supervisor.procs([]uint64{2, 4, 5}), []string{"2", "4", "5"}, "1")
	clusterMembershipProcessWaitForRouteProjection(t, ctx, supervisor.procs([]uint64{2, 4, 5}), "handoff/state")

	// --- CYCLE 3: Join 6 (seeds 2,4,5), Remove 2 ---
	// Rearm before stage
	supervisor.rearm(4, "add-learner-after-apply")
	supervisor.rearm(5, "promote-voter-after-apply")

	leaderID, leaderURL = supervisor.waitForLeader([]uint64{2, 4, 5})
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodPost,
		leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(6), clusterMembershipProcessAdmin, "cycle-3-stage-6")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("stage joiner 6 status=%d body=%s err=%v", status, body, err)
	}
	stageOp6, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil {
		t.Fatalf("decode stage op 6: %v", err)
	}

	// Start joiner 6 disarmed
	supervisor.start(6, fixture.joinerConfig(6, []uint64{2, 4, 5}), "")

	supervisor.waitForOperation(4, stageOp6.OperationID, "SUCCEEDED")
	supervisor.waitForHits("add-learner-after-apply", "promote-voter-after-apply")
	supervisor.waitForAllReady([]uint64{2, 4, 5, 6})

	// Rearm n4 for removal: remove-member-after-apply
	supervisor.rearm(4, "remove-member-after-apply")

	leaderID, leaderURL = supervisor.waitForLeader([]uint64{2, 4, 5, 6})
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodDelete,
		leaderURL+"/api/v1/cluster/members/2", nil, clusterMembershipProcessAdmin, "cycle-3-remove-2")
	if err != nil || (status != http.StatusAccepted && status != http.StatusOK) {
		t.Fatalf("delete member 2 status=%d body=%s err=%v", status, body, err)
	}
	removeOp2, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil {
		t.Fatalf("decode remove op 2: %v", err)
	}

	supervisor.waitForOperation(5, removeOp2.OperationID, "SUCCEEDED")
	supervisor.waitForHits("remove-member-after-apply")
	supervisor.waitForAllReady([]uint64{4, 5, 6})

	readyStatus, _, _ = clusterMembershipProcessRequest(ctx, http.MethodGet, supervisor.nodes[2].managementURL()+"/health/ready", nil, "", "")
	if readyStatus != http.StatusServiceUnavailable {
		t.Fatalf("retired node 2 ready status = %d, want 503", readyStatus)
	}
	supervisor.stop(2)

	clusterMembershipProcessAssertVotersAndRetired(t, ctx, supervisor.procs([]uint64{4, 5, 6}), []string{"4", "5", "6"}, "2")
	clusterMembershipProcessWaitForRouteProjection(t, ctx, supervisor.procs([]uint64{4, 5, 6}), "handoff/state")

	// Final: assert MQTT accepted on the current leader
	finalLeaderID, _ := supervisor.waitForLeader([]uint64{4, 5, 6})
	clusterMembershipProcessAssertMQTTAccepted(t, fixture.mqttAddresses[finalLeaderID-1], fixture.accessToken)
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-024, cluster-session-and-delivery.md:CLU-SCN-025, drain-and-rolling-update.md:DRN-SCN-010
func TestDynamicClusterMembershipRemovalHandoffWaitsForQuorum(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 4, "narumq_failpoint")
	ctx := fixture.ctx
	harness := fixture.harness

	// 1. Start n3 disarmed first, then start n1 and n2 armed begin-remove-after-apply
	n3, err := harness.start("bootstrap node 3", fixture.bootstrapConfig(3), fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("start n3: %v", err)
	}
	n1, err := harness.startArmed("bootstrap node 1", fixture.bootstrapConfig(1), fixture.managementAddresses[0], "begin-remove-after-apply")
	if err != nil {
		t.Fatalf("start n1: %v", err)
	}
	n2, err := harness.startArmed("bootstrap node 2", fixture.bootstrapConfig(2), fixture.managementAddresses[1], "begin-remove-after-apply")
	if err != nil {
		t.Fatalf("start n2: %v", err)
	}

	clusterMembershipProcessWaitForBootstrap(t, ctx, []*clusterMembershipProcess{n1, n2, n3})
	conn := clusterMembershipProcessOpenSubscribedSession(t, fixture.mqttAddresses[2], fixture.accessToken, "handoff-client", "handoff/state")
	defer conn.Close()
	// 2. Stage 4 on current leader
	_, leaderURL := clusterMembershipProcessFindLeader(t, ctx, []*clusterMembershipProcess{n1, n2, n3}, 3)
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodPost,
		leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(4), clusterMembershipProcessAdmin, "handoff-quorum-stage-4")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("stage joiner 4 status=%d body=%s err=%v", status, body, err)
	}
	stageOp4, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil {
		t.Fatalf("decode stage op: %v", err)
	}

	// Start joiner 4 armed route-projection-rebuild-error
	n4, err := harness.startArmed("joiner node 4", fixture.joinerConfig(4, []uint64{1, 2, 3}), fixture.managementAddresses[3], "route-projection-rebuild-error")
	if err != nil {
		t.Fatalf("start joiner 4: %v", err)
	}

	// Wait until operation SUCCEEDED on n3
	clusterMembershipProcessWaitForOperation(t, ctx, n3, stageOp4.OperationID, "SUCCEEDED")

	// 3. Assert on n4
	clusterMembershipProcessWaitForStatus(t, ctx, n4, "/health/live", http.StatusOK)
	for range 3 {
		readyStatus, _, _ := clusterMembershipProcessRequest(ctx, http.MethodGet, n4.managementURL()+"/health/ready", nil, "", "")
		if readyStatus != http.StatusServiceUnavailable {
			t.Fatalf("n4 readiness status = %d, want 503", readyStatus)
		}
		time.Sleep(500 * time.Millisecond)
	}
	clusterMembershipProcessAssertMQTTRejected(t, fixture.mqttAddresses[3])
	if err := n4.isAlive(); err != nil {
		t.Fatalf("n4 should remain alive despite projection failure: %v", err)
	}
	if !strings.Contains(n4.logs.String(), "cluster route projection rebuild failed") {
		t.Fatalf("n4 logs missing expected warning; logs:\n%s", n4.logs.String())
	}
	for _, proc := range []*clusterMembershipProcess{n1, n2, n3} {
		clusterMembershipProcessWaitForStatus(t, ctx, proc, "/health/ready", http.StatusOK)
	}

	// 4. Graceful restart of n4 disarmed -> ready 200
	if err := n4.stop(5 * time.Second); err != nil {
		t.Fatalf("stop n4: %v", err)
	}
	n4, err = harness.start("joiner node 4", fixture.joinerConfig(4, []uint64{1, 2, 3}), fixture.managementAddresses[3])
	if err != nil {
		t.Fatalf("restart n4 disarmed: %v", err)
	}
	clusterMembershipProcessWaitForStatus(t, ctx, n4, "/health/ready", http.StatusOK)
	clusterMembershipProcessWaitForRouteProjection(t, ctx, []*clusterMembershipProcess{n1, n2, n3, n4}, "handoff/state")

	// 5. DELETE member 3 on leader with key handoff-quorum-remove-3
	_, leaderURL = clusterMembershipProcessFindLeader(t, ctx, []*clusterMembershipProcess{n1, n2, n3, n4}, 4)
	_, _, _ = clusterMembershipProcessRequest(ctx, http.MethodDelete,
		leaderURL+"/api/v1/cluster/members/3", nil, clusterMembershipProcessAdmin, "handoff-quorum-remove-3")

	// 6. Wait on process.done for n1 and n2 (30s timeout each) and assert failpoint exit
	clusterMembershipProcessRequireFailpointExit(t, n1, "begin-remove-after-apply")
	clusterMembershipProcessRequireFailpointExit(t, n2, "begin-remove-after-apply")

	// 7. For 3s, assert connection open, n3/n4 ready 503, and MQTT rejected on n4
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		clusterMembershipProcessAssertConnectionOpen(t, conn, 300*time.Millisecond)
		s3, _, _ := clusterMembershipProcessRequest(ctx, http.MethodGet, n3.managementURL()+"/health/ready", nil, "", "")
		if s3 != http.StatusServiceUnavailable {
			t.Fatalf("n3 readiness status = %d during quorum loss, want 503", s3)
		}
		s4, _, _ := clusterMembershipProcessRequest(ctx, http.MethodGet, n4.managementURL()+"/health/ready", nil, "", "")
		if s4 != http.StatusServiceUnavailable {
			t.Fatalf("n4 readiness status = %d during quorum loss, want 503", s4)
		}
		clusterMembershipProcessAssertMQTTRejected(t, fixture.mqttAddresses[3])
	}

	// 8. Restart n1 and n2 disarmed
	n1, err = harness.start("bootstrap node 1", fixture.bootstrapConfig(1), fixture.managementAddresses[0])
	if err != nil {
		t.Fatalf("restart n1 disarmed: %v", err)
	}
	n2, err = harness.start("bootstrap node 2", fixture.bootstrapConfig(2), fixture.managementAddresses[1])
	if err != nil {
		t.Fatalf("restart n2 disarmed: %v", err)
	}

	var removeOpID string
	retryDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(retryDeadline) {
		liveNodes := []*clusterMembershipProcess{n1, n2, n4}
		var currentLeaderURL string
		for _, node := range liveNodes {
			summary, st, _, gErr := clusterMembershipProcessGetSummary(ctx, node.managementURL(), clusterMembershipProcessAdmin)
			if gErr == nil && st == http.StatusOK && summary.LeaderNodeID != nil {
				lID, pErr := strconv.ParseUint(*summary.LeaderNodeID, 10, 64)
				if pErr == nil {
					for _, candidate := range liveNodes {
						if candidate.managementAddress == fixture.managementAddresses[lID-1] {
							currentLeaderURL = candidate.managementURL()
							break
						}
					}
				}
			}
			if currentLeaderURL != "" {
				break
			}
		}
		if currentLeaderURL != "" {
			delStatus, delBody, delErr := clusterMembershipProcessRequest(ctx, http.MethodDelete,
				currentLeaderURL+"/api/v1/cluster/members/3", nil, clusterMembershipProcessAdmin, "handoff-quorum-remove-3")
			if delErr == nil && (delStatus == http.StatusOK || delStatus == http.StatusAccepted) {
				op, decErr := clusterMembershipProcessDecodeOperation(delBody)
				if decErr == nil && op.OperationID != "" {
					removeOpID = op.OperationID
					break
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	if removeOpID == "" {
		t.Fatal("failed to submit or recover remove-3 operation after quorum return")
	}

	// The old owner closes this socket when it applies the committed ownership
	// transfer. Check that handoff effect independently of n4's removal status.
	clusterMembershipProcessAssertConnectionClosed(t, conn, 30*time.Second)
	// 9. Wait for planned membership removal to succeed on the surviving node.
	clusterMembershipProcessWaitForOperation(t, ctx, n4, removeOpID, "SUCCEEDED")
	clusterMembershipProcessAssertVotersAndRetired(t, ctx, []*clusterMembershipProcess{n1, n2, n4}, []string{"1", "2", "4"}, "3")
	clusterMembershipProcessWaitForRouteProjection(t, ctx, []*clusterMembershipProcess{n1, n2, n4}, "handoff/state")

	for _, proc := range []*clusterMembershipProcess{n1, n2, n4} {
		clusterMembershipProcessWaitForStatus(t, ctx, proc, "/health/ready", http.StatusOK)
	}
	nodesRemaining := map[uint64]*clusterMembershipProcess{1: n1, 2: n2, 4: n4}
	finalLeaderID, _ := clusterMembershipFindLeaderByMap(t, ctx, nodesRemaining)
	clusterMembershipProcessAssertMQTTAccepted(t, fixture.mqttAddresses[finalLeaderID-1], fixture.accessToken)
}

func clusterMembershipFindLeaderByMap(t *testing.T, ctx context.Context, nodes map[uint64]*clusterMembershipProcess) (uint64, string) {
	t.Helper()
	for _, proc := range nodes {
		summary, status, _, err := clusterMembershipProcessGetSummary(ctx, proc.managementURL(), clusterMembershipProcessAdmin)
		if err != nil || status != http.StatusOK || summary.LeaderNodeID == nil {
			continue
		}
		id, parseErr := strconv.ParseUint(*summary.LeaderNodeID, 10, 64)
		if parseErr == nil {
			if leaderProc, ok := nodes[id]; ok && leaderProc != nil {
				return id, leaderProc.managementURL()
			}
		}
	}
	t.Fatalf("no node reported an active cluster leader")
	return 0, ""
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-035
func TestSnapshotFinalizationCrashRecoveryAcrossProcessRestart(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 4, "narumq_failpoint")
	supervisor := newClusterMembershipFailpointSupervisor(t, fixture.ctx, fixture)

	// The bootstrap typed snapshot is created during startup. Crashing before
	// rename leaves only a temporary file; restart must finalize a usable snapshot.
	supervisor.start(1, fixture.bootstrapConfig(1), "snapshot-create-before-finalize")
	supervisor.start(2, fixture.bootstrapConfig(2), "")
	supervisor.start(3, fixture.bootstrapConfig(3), "")
	supervisor.waitForHits("snapshot-create-before-finalize")
	supervisor.waitForAllReady([]uint64{1, 2, 3})
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, supervisor.procs([]uint64{1, 2, 3}))

	// The learner transition must create the second snapshot. One recovery
	// snapshot may finalize during startup; the specialized rearm allows that
	// single crash, proves restart, and requires the armed node to become ready.
	const snapshotFinalizationFailpoint = "snapshot-create-after-finalize"
	supervisor.rearmSnapshotFinalization(1, snapshotFinalizationFailpoint)
	_, leaderURL := supervisor.waitForLeader([]uint64{1, 2, 3})
	if point, armed := supervisor.armed[1]; !armed || point != snapshotFinalizationFailpoint {
		t.Fatalf("node 1 failpoint was consumed before staging the learner: armed=%t point=%q", armed, point)
	}
	armedNode := supervisor.nodes[1]
	hitsBeforeStage := supervisor.failpointHitCount(snapshotFinalizationFailpoint)
	status, body, err := clusterMembershipProcessRequest(fixture.ctx, http.MethodPost,
		leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(4), clusterMembershipProcessAdmin, "snapshot-finalize-stage-node-4")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("stage member 4 status=%d body=%s err=%v", status, body, err)
	}
	operation, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil || operation.Phase != "STAGED" {
		t.Fatalf("decode staged operation=%+v err=%v, want STAGED", operation, err)
	}
	supervisor.start(4, fixture.joinerConfig(4, []uint64{1, 2, 3}), "")
	joiner := supervisor.nodes[4]
	supervisor.waitForHitCount(snapshotFinalizationFailpoint, hitsBeforeStage+1)
	armedLogs := armedNode.logs.String()
	if !strings.Contains(armedLogs, fmt.Sprintf(`"operation_id":"%s"`, operation.OperationID)) ||
		!strings.Contains(armedLogs, `"member_id":4`) || !strings.Contains(armedLogs, `"phase":"STAGED"`) {
		t.Fatalf("node 1 reached the snapshot failpoint without applying learner operation %q:\n%s",
			operation.OperationID, armedLogs)
	}
	supervisor.waitForOperation(3, operation.OperationID, "SUCCEEDED")
	supervisor.waitForAllReady([]uint64{1, 2, 3, 4})
	clusterMembershipProcessAssertConvergedSummary(t, fixture.ctx, supervisor.procs([]uint64{1, 2, 3}), joiner, 4)
}

// Traceability: CLU-04, CLU-05, DEP-09; docs/specs/cluster-session-and-delivery.md §2, §2.1, §11.12; docs/specs/drain-and-rolling-update.md §10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-078, cluster-session-and-delivery.md:CLU-SCN-079, drain-and-rolling-update.md:DRN-SCN-010
func TestDynamicClusterMQTTConnectOwnershipTakeoverProcessAcceptance(t *testing.T) {
	t.Parallel()

	const (
		clientID = "ownership-takeover-client"
		filter   = "ownership/#"
		topic    = "ownership/event"
		point    = "ownership-acquire-after-commit"
	)
	fixture := newClusterMembershipProcessFixture(t, 3, "narumq_failpoint")

	p1, err := fixture.harness.start(
		"ownership bootstrap node 1", fixture.bootstrapConfig(1), fixture.managementAddresses[0],
	)
	if err != nil {
		t.Fatalf("start node 1: %v", err)
	}
	p3, err := fixture.harness.start(
		"ownership bootstrap node 3", fixture.bootstrapConfig(3), fixture.managementAddresses[2],
	)
	if err != nil {
		t.Fatalf("start node 3: %v", err)
	}
	p2, err := fixture.harness.startArmed(
		"ownership failpoint node 2", fixture.bootstrapConfig(2), fixture.managementAddresses[1], point,
	)
	if err != nil {
		t.Fatalf("start armed node 2: %v", err)
	}
	processes := []*clusterMembershipProcess{p1, p2, p3}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, processes)

	owner1 := clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[0], fixture.accessToken, clientID, filter,
	)
	defer owner1.Close()
	clusterMembershipProcessWaitForRouteProjectionClients(t, fixture.ctx, processes, filter, clientID)

	owner3, present := clusterMembershipProcessReconnectSession(
		t, fixture.mqttAddresses[2], fixture.accessToken, clientID,
	)
	defer owner3.Close()
	if !present {
		t.Fatal("node 3 takeover returned SessionPresent=false")
	}
	clusterMembershipProcessAssertConnectionClosed(t, owner1, 10*time.Second)
	clusterMembershipProcessWaitForRouteProjectionClients(t, fixture.ctx, processes, filter, clientID)
	clusterMembershipProcessPublish(
		t, fixture.mqttAddresses[0], fixture.accessToken, "ownership-publisher-1", topic, []byte("after-takeover"),
	)
	clusterMembershipProcessRequireLivePublish(t, owner3, topic, "after-takeover")

	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	observedNoConnAck := false
	for time.Now().Before(deadline) {
		conn, _, err := clusterMembershipProcessTryReconnectSession(
			fixture.mqttAddresses[1], fixture.accessToken, clientID,
		)
		switch {
		case err == nil:
			_ = conn.Close()
			t.Fatal("armed node 2 returned a positive CONNACK after ownership commit")
		case strings.HasPrefix(err.Error(), "read CONNACK header: ") && clusterMembershipProcessIsClosed(err):
			observedNoConnAck = true
		default:
			lastErr = err
			if p2.isAlive() != nil {
				t.Fatalf("node 2 exited without an established CONNECT attempt ending before CONNACK: %v", err)
			}
		}
		if observedNoConnAck {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !observedNoConnAck {
		t.Fatalf("node 2 did not close the CONNECT socket without CONNACK within 15s; last error: %v", lastErr)
	}
	clusterMembershipProcessRequireFailpointExit(t, p2, point)
	clusterMembershipProcessAssertConnectionClosed(t, owner3, 30*time.Second)

	survivors := []*clusterMembershipProcess{p1, p3}
	recovered, present := clusterMembershipProcessReconnectSession(
		t, fixture.mqttAddresses[0], fixture.accessToken, clientID,
	)
	defer recovered.Close()
	if !present {
		t.Fatal("node 1 recovery reconnect returned SessionPresent=false")
	}
	clusterMembershipProcessWaitForRouteProjectionClients(t, fixture.ctx, survivors, filter, clientID)
	clusterMembershipProcessPublish(
		t, fixture.mqttAddresses[2], fixture.accessToken, "ownership-publisher-3", topic, []byte("after-recovery"),
	)
	clusterMembershipProcessRequireLivePublish(t, recovered, topic, "after-recovery")
}
