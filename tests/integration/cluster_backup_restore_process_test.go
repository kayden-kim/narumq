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
	"reflect"
	"strconv"
	"testing"
	"time"

	"narumq/internal/adapters/raft/records"
)

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.7, §11.9.
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
	for _, retained := range []struct {
		topic   string
		payload []byte
	}{
		{topic: "retained/a", payload: []byte("alpha")},
		{topic: "retained/b", payload: []byte("beta")},
		{topic: "retained/b"},
		{topic: "retained/c", payload: []byte("gamma")},
	} {
		clusterMembershipProcessPublishRetained(t, fixture.mqttAddresses[0], fixture.accessToken, "retained-publisher", retained.topic, retained.payload)
	}
	expectedRetained := map[string]string{"retained/a": "alpha", "retained/c": "gamma"}
	sourceSubscriber := clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[1], fixture.accessToken, "retained-subscriber-source", "retained/#",
	)
	sourceRetained := clusterMembershipProcessReadRetained(t, sourceSubscriber, len(expectedRetained))
	_ = sourceSubscriber.Close()
	clusterBackupRestoreProcessAssertRetained(t, "cross-node replay", sourceRetained, expectedRetained)
	// 1. Open offline-subscriber on Node 1 subscribing to offline/#, then disconnect (detach).
	offlineSubscriber := clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[0], fixture.accessToken, "offline-subscriber", "offline/#",
	)
	_ = offlineSubscriber.Close()

	// 2. Wait for offline count 2 (retained-subscriber-source and offline-subscriber, 0 messages).
	_, _, _ = clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 2)

	// 3. Publish 3 offline messages on Node 2 (cross-node ENQUEUE).
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "offline-publisher", "offline/1", []byte("one"))
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "offline-publisher", "offline/2", []byte("two"))
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "offline-publisher", "offline/3", []byte("three"))

	// Wait for offline count 5 (2 sessions + 3 messages).
	_, _, _ = clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 5)

	// 4. Crash owner node (Node 1).
	if err := bootstrap[0].crash(5 * time.Second); err != nil {
		t.Fatalf("crash source node 1: %v", err)
	}

	// Reconnect offline-subscriber on Node 3 -> SessionPresent=true, receive 3 messages.
	reconnNode3, presentNode3 := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[2], fixture.accessToken, "offline-subscriber")
	if !presentNode3 {
		t.Fatal("expected session present on reconnect to node 3 after owner crash")
	}
	publishesNode3 := clusterMembershipProcessReadPublishes(t, reconnNode3, 3)
	_ = reconnNode3.Close()
	expectedOfflineFirstBatch := [][2]string{
		{"offline/1", "one"},
		{"offline/2", "two"},
		{"offline/3", "three"},
	}
	if !reflect.DeepEqual(publishesNode3, expectedOfflineFirstBatch) {
		t.Fatalf("reconnect publishes after crash = %+v, want %+v", publishesNode3, expectedOfflineFirstBatch)
	}

	// 5. Restart Node 1 and wait for cluster to recover.
	node1Restarted, err := fixture.harness.start("source bootstrap node 1 after crash", bootstrapConfigPaths[0], fixture.managementAddresses[0])
	if err != nil {
		t.Fatalf("restart source node 1: %v", err)
	}
	bootstrap[0] = node1Restarted
	clusterBackupRestoreProcessWaitForCluster(t, ctx, bootstrap, 4)
	_, leaderURL = clusterMembershipProcessFindLeader(t, ctx, bootstrap, 4)

	// 6. Publish offline/4=four on Node 2.
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "offline-publisher", "offline/4", []byte("four"))

	// Wait for offline count 3 (2 sessions + 1 message). Capture archive for restore.
	archiveBytes, archive, backupHeaders := clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 3)
	if archive.Manifest.GetFormatMajor() != records.BackupFormatMajor {
		t.Fatalf("backup format major=%d, want %d", archive.Manifest.GetFormatMajor(), records.BackupFormatMajor)
	}
	boundaryState := archive.Manifest.GetBoundaryState()
	if boundaryState.GetRetained().GetCount() != uint64(len(expectedRetained)) {
		t.Fatalf("backup retained summary=%+v, want count %d", boundaryState.GetRetained(), len(expectedRetained))
	}
	if boundaryState.GetOffline().GetCount() != 3 {
		t.Fatalf("backup offline summary=%+v, want count 3", boundaryState.GetOffline())
	}
	if archive.Manifest.GetCommitIndex() < stagedOperation.CommittedIndex {
		t.Fatalf("backup commit index=%d is before staged operation index=%d", archive.Manifest.GetCommitIndex(), stagedOperation.CommittedIndex)
	}
	commitHeader, err := strconv.ParseUint(backupHeaders.Get("X-NaruMQ-Backup-Commit-Index"), 10, 64)
	if err != nil || commitHeader != archive.Manifest.GetCommitIndex() {
		t.Fatalf("backup commit header=%q err=%v, archive commit index=%d", backupHeaders.Get("X-NaruMQ-Backup-Commit-Index"), err, archive.Manifest.GetCommitIndex())
	}
	if len(archiveBytes) < sha256.Size {
		t.Fatalf("backup archive is too short to contain its SHA-256 trailer: %d", len(archiveBytes))
	}
	archiveDigest := sha256.Sum256(archiveBytes[:len(archiveBytes)-sha256.Size])
	if got, want := backupHeaders.Get("X-NaruMQ-Backup-SHA256"), hex.EncodeToString(archiveDigest[:]); got != want {
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
	restoredSubscriber := clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[2], fixture.accessToken, "retained-subscriber-restored", "retained/#",
	)
	restoredRetained := clusterMembershipProcessReadRetained(t, restoredSubscriber, len(expectedRetained))
	_ = restoredSubscriber.Close()
	clusterBackupRestoreProcessAssertRetained(t, "restored-node replay", restoredRetained, expectedRetained)
	clusterMembershipProcessWaitForOperation(t, ctx, restored[0], stagedOperation.OperationID, "STAGED")

	// Reconnect offline-subscriber on restored Node 2 -> SessionPresent=true, receive offline/4=four.
	reconnRestored, presentRestored := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[1], fixture.accessToken, "offline-subscriber")
	if !presentRestored {
		t.Fatal("expected session present on restored node 2 reconnect")
	}
	restoredPublishes := clusterMembershipProcessReadPublishes(t, reconnRestored, 1)
	_ = reconnRestored.Close()
	expectedRestoredOffline := [][2]string{{"offline/4", "four"}}
	if !reflect.DeepEqual(restoredPublishes, expectedRestoredOffline) {
		t.Fatalf("restored publishes = %+v, want %+v", restoredPublishes, expectedRestoredOffline)
	}

	// Publish offline/5=five on Node 1
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[0], fixture.accessToken, "offline-publisher", "offline/5", []byte("five"))
	if err := restored[0].crash(5 * time.Second); err != nil {
		t.Fatalf("crash restored node 1: %v", err)
	}
	restarted, err := fixture.harness.start("restored node 1 after crash", restoredConfigPaths[0], fixture.managementAddresses[0])
	if err != nil {
		t.Fatalf("restart restored node 1: %v", err)
	}
	restored[0] = restarted
	clusterBackupRestoreProcessWaitForCluster(t, ctx, restored, 4)
	restartedSubscriber := clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[0], fixture.accessToken, "retained-subscriber-restarted", "retained/#",
	)
	restartedRetained := clusterMembershipProcessReadRetained(t, restartedSubscriber, len(expectedRetained))
	_ = restartedSubscriber.Close()
	clusterBackupRestoreProcessAssertRetained(t, "restarted-node replay", restartedRetained, expectedRetained)
	clusterMembershipProcessWaitForOperation(t, ctx, restored[0], stagedOperation.OperationID, "STAGED")

	joiner, err := fixture.harness.start("joiner after restore", fixture.joinerConfig(4, []uint64{1, 2, 3}), fixture.managementAddresses[3])
	if err != nil {
		t.Fatalf("start joiner after restore: %v", err)
	}
	clusterMembershipProcessWaitForOperation(t, ctx, restored[0], stagedOperation.OperationID, "SUCCEEDED")
	clusterMembershipProcessWaitForStatus(t, ctx, joiner, "/health/ready", http.StatusOK)
	clusterMembershipProcessAssertConvergedSummary(t, ctx, restored, joiner, 4)
	joinerSubscriber := clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[3], fixture.accessToken, "retained-subscriber-joiner", "retained/#",
	)
	joinerRetained := clusterMembershipProcessReadRetained(t, joinerSubscriber, len(expectedRetained))
	_ = joinerSubscriber.Close()
	clusterBackupRestoreProcessAssertRetained(t, "joiner replay", joinerRetained, expectedRetained)

	// Reconnect offline-subscriber on joiner -> SessionPresent=true, receive offline/5=five.
	reconnJoiner, presentJoiner := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[3], fixture.accessToken, "offline-subscriber")
	if !presentJoiner {
		t.Fatal("expected session present on joiner reconnect")
	}
	joinerPublishes := clusterMembershipProcessReadPublishes(t, reconnJoiner, 1)
	_ = reconnJoiner.Close()
	expectedJoinerOffline := [][2]string{{"offline/5", "five"}}
	if !reflect.DeepEqual(joinerPublishes, expectedJoinerOffline) {
		t.Fatalf("joiner offline publishes = %+v, want %+v", joinerPublishes, expectedJoinerOffline)
	}

	// Verify total kind summary convergence between restored[1] and joiner
	clusterMembershipProcessWait(t, ctx, "converged backup summary between restored node 2 and joiner", func() (bool, string, error) {
		_, archiveA, _ := clusterBackupRestoreProcessCaptureBackup(t, ctx, restored[1].managementURL())
		_, archiveB, _ := clusterBackupRestoreProcessCaptureBackup(t, ctx, joiner.managementURL())
		if archiveA.Manifest.GetCommitIndex() != archiveB.Manifest.GetCommitIndex() {
			return false, fmt.Sprintf("commit index mismatch: node 2 has %d, joiner has %d", archiveA.Manifest.GetCommitIndex(), archiveB.Manifest.GetCommitIndex()), nil
		}
		if !records.EqualBackupStateSummary(archiveA.Manifest.GetBoundaryState(), archiveB.Manifest.GetBoundaryState()) {
			return false, fmt.Sprintf("boundary state mismatch at commit index %d: node2=%+v joiner=%+v", archiveA.Manifest.GetCommitIndex(), archiveA.Manifest.GetBoundaryState(), archiveB.Manifest.GetBoundaryState()), nil
		}
		return true, fmt.Sprintf("boundary state summary matched at commit index %d", archiveA.Manifest.GetCommitIndex()), nil
	})
}

func clusterBackupRestoreProcessCaptureBackup(t *testing.T, ctx context.Context, baseURL string) ([]byte, *records.BackupArchive, http.Header) {
	t.Helper()
	backupRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/v1/cluster/backups", nil)
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
	return archiveBytes, archive, backupResponse.Header
}

func clusterBackupRestoreProcessWaitForOfflineCount(t *testing.T, ctx context.Context, baseURL string, want uint64) ([]byte, *records.BackupArchive, http.Header) {
	t.Helper()
	var (
		lastBytes   []byte
		lastArchive *records.BackupArchive
		lastHeaders http.Header
	)
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("offline summary count %d", want), func() (bool, string, error) {
		b, archive, headers := clusterBackupRestoreProcessCaptureBackup(t, ctx, baseURL)
		lastBytes = b
		lastArchive = archive
		lastHeaders = headers
		count := archive.Manifest.GetBoundaryState().GetOffline().GetCount()
		if count == want {
			return true, fmt.Sprintf("offline count reached %d", want), nil
		}
		return false, fmt.Sprintf("current offline count is %d, want %d", count, want), nil
	})
	return lastBytes, lastArchive, lastHeaders
}

func clusterBackupRestoreProcessAssertRetained(t *testing.T, name string, got, want map[string]string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s retained values=%v, want %v", name, got, want)
	}
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
