package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"narumq/internal/adapters/raft/records"
)

func TestClusterBackupRestoreProcessCrashAndResync(t *testing.T) {
	fixture := newClusterMembershipProcessFixture(t, 4, "")
	ctx := fixture.ctx
	bootstrapConfigPaths := []string{
		fixture.bootstrapConfig(1),
		fixture.bootstrapConfig(2),
		fixture.bootstrapConfig(3),
	}
	bootstrap := make([]*clusterMembershipProcess, 3)
	for index, configPath := range bootstrapConfigPaths {
		process, err := fixture.harness.start(fmt.Sprintf("source bootstrap node %d", index+1), configPath, fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start source node %d: %v", index+1, err)
		}
		bootstrap[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, bootstrap)

	_, leaderURL := clusterMembershipProcessFindLeader(t, ctx, bootstrap, 3)
	stageStatus, stageBody, err := clusterMembershipProcessRequest(ctx, http.MethodPost,
		leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(4), clusterMembershipProcessAdmin, "backup-restore-stage-node-4")
	if err != nil || stageStatus != http.StatusAccepted {
		t.Fatalf("stage member 4 status=%d body=%s err=%v", stageStatus, stageBody, err)
	}
	stagedOperation, err := clusterMembershipProcessDecodeOperation(stageBody)
	if err != nil || stagedOperation.Phase != "STAGED" {
		t.Fatalf("decode staged operation=%+v err=%v, want STAGED", stagedOperation, err)
	}

	backupRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, leaderURL+"/api/v1/cluster/backups", nil)
	if err != nil {
		t.Fatalf("create online backup request: %v", err)
	}
	backupRequest.Header.Set("Authorization", "Bearer "+clusterMembershipProcessAdmin)
	backupResponse, err := (&http.Client{Timeout: 10 * time.Second}).Do(backupRequest)
	if err != nil {
		t.Fatalf("capture online backup: %v", err)
	}
	archiveBytes, readErr := io.ReadAll(backupResponse.Body)
	closeErr := backupResponse.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read online backup body: read=%v close=%v", readErr, closeErr)
	}
	if backupResponse.StatusCode != http.StatusOK || backupResponse.Header.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("online backup status=%d content-type=%q body-size=%d", backupResponse.StatusCode, backupResponse.Header.Get("Content-Type"), len(archiveBytes))
	}
	archive, err := records.DecodeBackupArchive(archiveBytes)
	if err != nil {
		t.Fatalf("decode online backup archive: %v", err)
	}
	if archive.Manifest.GetCommitIndex() < stagedOperation.CommittedIndex {
		t.Fatalf("backup commit index=%d is before staged operation index=%d", archive.Manifest.GetCommitIndex(), stagedOperation.CommittedIndex)
	}
	commitHeader, err := strconv.ParseUint(backupResponse.Header.Get("X-NaruMQ-Backup-Commit-Index"), 10, 64)
	if err != nil || commitHeader != archive.Manifest.GetCommitIndex() {
		t.Fatalf("backup commit header=%q err=%v, archive commit index=%d", backupResponse.Header.Get("X-NaruMQ-Backup-Commit-Index"), err, archive.Manifest.GetCommitIndex())
	}
	if len(archiveBytes) < sha256.Size {
		t.Fatalf("backup archive is too short to contain its SHA-256 trailer: %d", len(archiveBytes))
	}
	archiveDigest := sha256.Sum256(archiveBytes[:len(archiveBytes)-sha256.Size])
	if got, want := backupResponse.Header.Get("X-NaruMQ-Backup-SHA256"), hex.EncodeToString(archiveDigest[:]); got != want {
		t.Fatalf("backup digest header=%q, archive trailer digest=%q", got, want)
	}

	archivePath := filepath.Join(fixture.tempDir, "cluster-backup.nrbak")
	clusterMembershipProcessWriteFile(t, archivePath, archiveBytes, 0o600)
	var restoredConfigPaths [3]string
	for index, originalPath := range bootstrapConfigPaths {
		memberID := uint64(index + 1)
		configBytes, err := os.ReadFile(originalPath)
		if err != nil {
			t.Fatalf("read bootstrap config %d: %v", memberID, err)
		}
		var config clusterMembershipProcessConfig
		if err := json.Unmarshal(configBytes, &config); err != nil {
			t.Fatalf("decode bootstrap config %d: %v", memberID, err)
		}
		config.Directory = filepath.Join(fixture.tempDir, fmt.Sprintf("restored-node-%d-data", memberID))
		restoredConfigPaths[index] = filepath.Join(fixture.tempDir, fmt.Sprintf("restored-node-%d.json", memberID))
		clusterMembershipProcessWriteJSON(t, restoredConfigPaths[index], config)
	}

	for index, process := range bootstrap {
		if err := process.stop(5 * time.Second); err != nil {
			t.Fatalf("stop source node %d before restore: %v", index+1, err)
		}
	}
	for index, configPath := range restoredConfigPaths {
		command := exec.CommandContext(ctx, fixture.binary, "-cluster-config", configPath, "-cluster-restore", archivePath)
		command.Dir = fixture.repoRoot
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("restore offline seed for node %d: %v\n%s", index+1, err, output)
		}
	}

	restored := make([]*clusterMembershipProcess, 3)
	for index, configPath := range restoredConfigPaths {
		process, err := fixture.harness.start(fmt.Sprintf("restored node %d", index+1), configPath, fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start restored node %d: %v", index+1, err)
		}
		restored[index] = process
	}
	clusterBackupRestoreProcessWaitForCluster(t, ctx, restored, 4)
	clusterMembershipProcessWaitForOperation(t, ctx, restored[0], stagedOperation.OperationID, "STAGED")

	if err := restored[0].crash(5 * time.Second); err != nil {
		t.Fatalf("crash restored node 1: %v", err)
	}
	restarted, err := fixture.harness.start("restored node 1 after crash", restoredConfigPaths[0], fixture.managementAddresses[0])
	if err != nil {
		t.Fatalf("restart restored node 1: %v", err)
	}
	restored[0] = restarted
	clusterBackupRestoreProcessWaitForCluster(t, ctx, restored, 4)
	clusterMembershipProcessWaitForOperation(t, ctx, restored[0], stagedOperation.OperationID, "STAGED")

	joiner, err := fixture.harness.start("joiner after restore", fixture.joinerConfig(4, []uint64{1, 2, 3}), fixture.managementAddresses[3])
	if err != nil {
		t.Fatalf("start joiner after restore: %v", err)
	}
	clusterMembershipProcessWaitForOperation(t, ctx, restored[0], stagedOperation.OperationID, "SUCCEEDED")
	clusterMembershipProcessWaitForStatus(t, ctx, joiner, "/health/ready", http.StatusOK)
	clusterMembershipProcessAssertConvergedSummary(t, ctx, restored, joiner, 4)
}

func clusterBackupRestoreProcessWaitForCluster(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, expectedMemberCount int) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, "restored voters ready with a leader", func() (bool, string, error) {
		leaderID := ""
		for _, process := range processes {
			if err := process.isAlive(); err != nil {
				return false, "", err
			}
			readyStatus, readyBody, err := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+"/health/ready", nil, "", "")
			if err != nil || readyStatus != http.StatusOK {
				return false, fmt.Sprintf("%s readiness status=%d body=%s err=%v", process.name, readyStatus, readyBody, err), nil
			}
			summary, status, body, err := clusterMembershipProcessGetSummary(ctx, process.managementURL(), clusterMembershipProcessAdmin)
			if err != nil || status != http.StatusOK {
				return false, fmt.Sprintf("%s summary status=%d body=%s err=%v", process.name, status, body, err), nil
			}
			if summary.MemberCount != expectedMemberCount || !summary.QuorumAvailable || summary.LeaderNodeID == nil || *summary.LeaderNodeID == "" {
				return false, fmt.Sprintf("%s summary=%+v, want %d members, quorum, and leader", process.name, summary, expectedMemberCount), nil
			}
			if leaderID == "" {
				leaderID = *summary.LeaderNodeID
			} else if leaderID != *summary.LeaderNodeID {
				return false, fmt.Sprintf("restored leader views disagree: %s reports %s, %s reports %s", processes[0].name, leaderID, process.name, *summary.LeaderNodeID), nil
			}
		}
		return true, "all restored voters ready", nil
	})
}
