package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Traceability: CLU-02/CLU-07/CLU-14; docs/specs/cluster-session-and-delivery.md §1, §7, §11.20.
func TestDynamicClusterVoterVolumeLossProcessAcceptance(t *testing.T) {
	t.Parallel()
	fixture := newClusterMembershipProcessFixture(t, 4, "")
	ctx := fixture.ctx
	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		process, err := fixture.harness.start(fmt.Sprintf("bootstrap node %d", index+1), fixture.bootstrapConfig(uint64(index+1)), fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, processes)
	clusterMembershipProcessPublishRetained(t, fixture.mqttAddresses[0], fixture.accessToken, "retained-publisher", "retained/volume", []byte("v1"))
	offline := clusterMembershipProcessOpenSubscribedSession(t, fixture.mqttAddresses[2], fixture.accessToken, "offline-subscriber", "offline/#")
	_ = offline.Close()
	_, leaderURL := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 1)
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "offline-publisher", "offline/volume", []byte("kept"))
	clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 2)

	if err := processes[2].crash(5 * time.Second); err != nil {
		t.Fatalf("crash voter 3: %v", err)
	}
	directory := filepath.Join(fixture.tempDir, "node-3-data")
	if err := os.RemoveAll(directory); err != nil {
		t.Fatalf("remove lost voter volume: %v", err)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatalf("create empty voter volume: %v", err)
	}
	lost, err := fixture.harness.start("bootstrap node 3 after volume loss", fixture.bootstrapConfig(3), fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart lost voter: %v", err)
	}
	select {
	case <-lost.done:
	case <-time.After(30 * time.Second):
		t.Fatal("lost voter did not refuse empty-directory bootstrap within 30 seconds")
	}
	if code, exited := lost.exitCode(); !exited || code != 1 {
		t.Fatalf("lost voter exit=(%d,%t), want (1,true)", code, exited)
	}
	if !strings.Contains(lost.logs.String(), "cluster bootstrap refused") {
		t.Fatalf("lost voter refusal log missing: %s", lost.logs.String())
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("refused voter changed empty volume: entries=%v err=%v", entries, err)
	}
	remaining := []*clusterMembershipProcess{processes[0], processes[1]}
	for _, process := range remaining {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}
	_, leaderURL = clusterMembershipProcessFindLeader(t, ctx, remaining, 3)
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodPost, leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(4), clusterMembershipProcessAdmin, "volume-loss-stage-4")
	stage, decodeErr := clusterMembershipProcessDecodeOperation(body)
	if err != nil || decodeErr != nil || status != http.StatusAccepted || stage.Phase != "STAGED" {
		t.Fatalf("stage replacement status=%d body=%s err=%v decode=%v", status, body, err, decodeErr)
	}
	joiner, err := fixture.harness.start("replacement joiner 4", fixture.joinerConfig(4, []uint64{1, 2}), fixture.managementAddresses[3])
	if err != nil {
		t.Fatalf("start replacement: %v", err)
	}
	clusterMembershipProcessWaitForOperation(t, ctx, processes[0], stage.OperationID, "SUCCEEDED")
	clusterMembershipProcessWaitForStatus(t, ctx, joiner, "/health/ready", http.StatusOK)
	_, leaderURL = clusterMembershipProcessFindLeader(t, ctx, remaining, 4)
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodDelete, leaderURL+"/api/v1/cluster/members/3", nil, clusterMembershipProcessAdmin, "volume-loss-remove-3")
	removal, decodeErr := clusterMembershipProcessDecodeOperation(body)
	if err != nil || decodeErr != nil || status != http.StatusAccepted || removal.Phase != "REMOVING" {
		t.Fatalf("remove lost voter status=%d body=%s err=%v decode=%v", status, body, err, decodeErr)
	}
	clusterMembershipProcessWaitForOperation(t, ctx, processes[0], removal.OperationID, "SUCCEEDED")
	survivors := []*clusterMembershipProcess{processes[0], processes[1], joiner}
	clusterMembershipProcessWaitForClusterSize(t, ctx, survivors, 3)
	clusterMembershipProcessAssertVotersAndRetired(t, ctx, survivors, []string{"1", "2", "4"}, "3")

	retained := clusterMembershipProcessOpenCleanSubscribedSession(t, fixture.mqttAddresses[3], fixture.accessToken, "retained-subscriber-joiner", "retained/volume")
	defer retained.Close()
	if got := clusterMembershipProcessReadRetained(t, retained, 1); !reflect.DeepEqual(got, map[string]string{"retained/volume": "v1"}) {
		t.Fatalf("replacement retained=%v", got)
	}
	resumed, present := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[3], fixture.accessToken, "offline-subscriber")
	defer resumed.Close()
	if !present {
		t.Fatal("replacement lost persistent session")
	}
	if got := clusterMembershipProcessReadPublishes(t, resumed, 1); !reflect.DeepEqual(got, [][2]string{{"offline/volume", "kept"}}) {
		t.Fatalf("replacement offline messages=%v", got)
	}
}

// Traceability: CLU-02/CLU-07/CLU-14; docs/specs/cluster-session-and-delivery.md §1, §11.20.
func TestDynamicClusterBootstrapProbeAllowsLateCoBootstrapPeer(t *testing.T) {
	fixture := newClusterMembershipProcessFixture(t, 3, "narumq_failpoint")
	ctx := fixture.ctx
	processes := make([]*clusterMembershipProcess, 3)
	late, err := fixture.harness.startArmed("delayed bootstrap node 3", fixture.bootstrapConfig(3), fixture.managementAddresses[2], "bootstrap-probe-delay")
	if err != nil {
		t.Fatalf("start delayed peer: %v", err)
	}
	processes[2] = late
	for index := range 2 {
		process, err := fixture.harness.start(fmt.Sprintf("bootstrap node %d", index+1), fixture.bootstrapConfig(uint64(index+1)), fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	for _, process := range processes[:2] {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}
	if status, _, err := clusterMembershipProcessRequest(ctx, http.MethodGet, late.managementURL()+"/health/live", nil, "", ""); err == nil {
		t.Fatalf("delayed peer opened management before probe: status=%d", status)
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, processes)
	for _, line := range strings.Split(late.logs.String(), "\n") {
		var record struct {
			Message     string `json:"msg"`
			Status      string `json:"status"`
			Term        uint64 `json:"term"`
			CommitIndex uint64 `json:"commit_index"`
		}
		if json.Unmarshal([]byte(line), &record) == nil && record.Message == "cluster bootstrap peer confirmed" && record.Status == "ready" && (record.Term > 1 || record.CommitIndex > 3) {
			return
		}
	}
	t.Fatalf("late peer never received READY from a progressed co-bootstrap peer: %s", late.logs.String())
}
