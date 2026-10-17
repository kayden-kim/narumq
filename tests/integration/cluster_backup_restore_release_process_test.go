package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// Traceability: CLU-11/CLU-14; docs/specs/cluster-session-and-delivery.md §10, §11.19.
// Acceptance: NARUMQ_RELEASE_COMPAT
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-119, cluster-session-and-delivery.md:CLU-SCN-120, cluster-session-and-delivery.md:CLU-SCN-121, cluster-session-and-delivery.md:CLU-SCN-122, cluster-session-and-delivery.md:CLU-SCN-222
func TestClusterBackupRestoreReleaseWindowProcessAcceptance(t *testing.T) {
	if os.Getenv("NARUMQ_RELEASE_COMPAT") != "1" {
		t.Skip("set NARUMQ_RELEASE_COMPAT=1 to run backup archive release-window acceptance")
	}
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	binN2 := fixture.buildBinary(t, "restore-1.0.0", "-X main.buildReleaseVersion=1.0.0")
	binN1 := fixture.buildBinary(t, "restore-1.1.0", "-X main.buildReleaseVersion=1.1.0")
	binN := fixture.buildBinary(t, "restore-1.2.0", "-X main.buildReleaseVersion=1.2.0 -X main.buildFeatures=compatibility-probe")
	ctx := fixture.ctx
	configs := []string{fixture.bootstrapConfig(1), fixture.bootstrapConfig(2), fixture.bootstrapConfig(3)}
	processes := clusterBackupReleaseUpgradeFromN1(t, fixture, configs, binN1, binN)
	archiveBPath := clusterBackupReleaseCaptureArchiveB(t, fixture, processes)

	status, body := clusterReleaseCompatActivate(t, ctx, processes[0])
	if status != http.StatusOK {
		t.Fatalf("activate feature status=%d body=%s", status, body)
	}
	status, features, body, err := clusterReleaseCompatFeatures(ctx, processes[0])
	if err != nil || status != http.StatusOK || !slices.Equal(features.ActiveFeatures, []string{"compatibility-probe"}) {
		t.Fatalf("active features status=%d state=%+v body=%s error=%v", status, features, body, err)
	}
	_, leaderURL := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	archiveCBytes, _, _, err := clusterBackupRestoreProcessCaptureBackup(t, ctx, leaderURL)
	if err != nil {
		t.Fatal(err)
	}
	archiveCPath := filepath.Join(fixture.tempDir, "archive-c.nrbak")
	clusterMembershipProcessWriteFile(t, archiveCPath, archiveCBytes, 0o600)
	for _, process := range processes {
		if err := process.stop(5 * time.Second); err != nil {
			t.Fatal(err)
		}
	}

	for _, rejection := range []struct {
		label   string
		binary  string
		markers []string
	}{
		{"rejected-feature", binN1, []string{"backup active feature", "compatibility-probe", "is not supported by this release"}},
		{"rejected-window", binN2, []string{"outside the supported release window"}},
	} {
		rejectedConfigs := clusterBackupReleaseRestoreConfigs(t, fixture, configs, rejection.label)
		output, err := clusterBackupReleaseRestore(ctx, rejection.binary, fixture.repoRoot, rejectedConfigs[0], archiveCPath)
		if err == nil {
			t.Fatalf("%s restore succeeded", rejection.label)
		}
		for _, marker := range rejection.markers {
			if !strings.Contains(string(output), marker) {
				t.Fatalf("%s restore error=%v output=%s, want %q", rejection.label, err, output, marker)
			}
		}
		target := filepath.Join(fixture.tempDir, rejection.label+"-node-1-data")
		if _, err := os.Stat(target); !os.IsNotExist(err) {
			t.Fatalf("%s target stat = %v, want not-exist", rejection.label, err)
		}
		t.Logf("%s: non-zero exit and target directory absent; %s", rejection.label, output)
	}

	downgradeConfigs := clusterBackupReleaseRestoreConfigs(t, fixture, configs, "downgrade")
	for index, configPath := range downgradeConfigs {
		output, err := clusterBackupReleaseRestore(ctx, binN1, fixture.repoRoot, configPath, archiveBPath)
		if err != nil {
			t.Fatalf("downgrade restore node %d: %v\n%s", index+1, err, output)
		}
		process, err := fixture.harness.startBinary(binN1, fmt.Sprintf("archive downgrade node %d", index+1), configPath, fixture.managementAddresses[index])
		if err != nil {
			t.Fatal(err)
		}
		processes[index] = process
	}
	clusterBackupRestoreProcessWaitForCluster(t, ctx, processes, 3)
	clusterReleaseCompatWaitVersions(t, ctx, processes[0], map[string]string{"1": "1.1.0", "2": "1.1.0", "3": "1.1.0"})
	retained := clusterMembershipProcessOpenCleanSubscribedSession(t, fixture.mqttAddresses[2], fixture.accessToken, "archive-downgrade-retained", "archive/retained")
	gotRetained := clusterMembershipProcessReadRetained(t, retained, 1)
	_ = retained.Close()
	clusterBackupRestoreProcessAssertRetained(t, "downgrade", gotRetained, map[string]string{"archive/retained": "v1"})
	reconnected, present := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[2], fixture.accessToken, "archive-offline-sub")
	if !present {
		t.Fatal("downgrade restore lost SessionPresent")
	}
	gotOffline := clusterMembershipProcessReadPublishes(t, reconnected, 1)
	_ = reconnected.Close()
	if !reflect.DeepEqual(gotOffline, [][2]string{{"archive/offline/2", "two"}}) {
		t.Fatalf("downgrade offline publishes = %v", gotOffline)
	}
	t.Log("N -> N-1: committed profiles, retained v1, SessionPresent and offline two restored")
}

func clusterBackupReleaseUpgradeFromN1(t *testing.T, fixture *clusterMembershipProcessFixture, configs []string, binN1, binN string) []*clusterMembershipProcess {
	t.Helper()
	ctx := fixture.ctx
	processes := make([]*clusterMembershipProcess, 3)
	for index, configPath := range configs {
		process, err := fixture.harness.startBinary(binN1, fmt.Sprintf("archive source node %d", index+1), configPath, fixture.managementAddresses[index])
		if err != nil {
			t.Fatal(err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrapRelease(t, ctx, processes, "1.1.0")
	clusterMembershipProcessPublishRetained(t, fixture.mqttAddresses[0], fixture.accessToken, "archive-retained-pub", "archive/retained", []byte("v1"))
	subscriber := clusterMembershipProcessOpenSubscribedSession(t, fixture.mqttAddresses[0], fixture.accessToken, "archive-offline-sub", "archive/offline/#")
	_ = subscriber.Close()
	_, leaderURL := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	// Ensure DETACH is committed before sending the offline message.
	_, _, _ = clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 1)
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "archive-offline-pub", "archive/offline/1", []byte("one"))
	archiveABytes, archiveA, _ := clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 2)
	if archiveA.Manifest.GetProducerRelease() != "1.1.0" {
		t.Fatalf("archive A producer = %q", archiveA.Manifest.GetProducerRelease())
	}
	archiveAPath := filepath.Join(fixture.tempDir, "archive-a.nrbak")
	clusterMembershipProcessWriteFile(t, archiveAPath, archiveABytes, 0o600)
	for _, process := range processes {
		if err := process.stop(5 * time.Second); err != nil {
			t.Fatal(err)
		}
	}

	upgradeConfigs := clusterBackupReleaseRestoreConfigs(t, fixture, configs, "upgrade")
	for index, configPath := range upgradeConfigs {
		output, err := clusterBackupReleaseRestore(ctx, binN, fixture.repoRoot, configPath, archiveAPath)
		if err != nil {
			t.Fatalf("upgrade restore node %d: %v\n%s", index+1, err, output)
		}
		process, err := fixture.harness.startBinary(binN, fmt.Sprintf("archive upgrade node %d", index+1), configPath, fixture.managementAddresses[index])
		if err != nil {
			t.Fatal(err)
		}
		processes[index] = process
	}
	clusterBackupRestoreProcessWaitForCluster(t, ctx, processes, 3)
	clusterReleaseCompatWaitVersions(t, ctx, processes[0], map[string]string{"1": "1.2.0", "2": "1.2.0", "3": "1.2.0"})
	retained := clusterMembershipProcessOpenCleanSubscribedSession(t, fixture.mqttAddresses[2], fixture.accessToken, "archive-upgrade-retained", "archive/retained")
	gotRetained := clusterMembershipProcessReadRetained(t, retained, 1)
	_ = retained.Close()
	clusterBackupRestoreProcessAssertRetained(t, "upgrade", gotRetained, map[string]string{"archive/retained": "v1"})
	reconnected, present := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[2], fixture.accessToken, "archive-offline-sub")
	if !present {
		t.Fatal("upgrade restore lost SessionPresent")
	}
	gotOffline := clusterMembershipProcessReadPublishes(t, reconnected, 1)
	_ = reconnected.Close()
	if !reflect.DeepEqual(gotOffline, [][2]string{{"archive/offline/1", "one"}}) {
		t.Fatalf("upgrade offline publishes = %v", gotOffline)
	}
	t.Log("N-1 -> N: committed profiles, retained v1, SessionPresent and offline one restored")
	return processes
}

func clusterBackupReleaseCaptureArchiveB(t *testing.T, fixture *clusterMembershipProcessFixture, processes []*clusterMembershipProcess) string {
	t.Helper()
	ctx := fixture.ctx
	_, leaderURL := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	_, _, _ = clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 1)
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "archive-offline-pub", "archive/offline/2", []byte("two"))
	archiveBBytes, archiveB, _ := clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 2)
	if archiveB.Manifest.GetProducerRelease() != "1.2.0" {
		t.Fatalf("archive B producer = %q", archiveB.Manifest.GetProducerRelease())
	}
	archiveBPath := filepath.Join(fixture.tempDir, "archive-b.nrbak")
	clusterMembershipProcessWriteFile(t, archiveBPath, archiveBBytes, 0o600)
	return archiveBPath
}

func clusterBackupReleaseRestoreConfigs(t *testing.T, fixture *clusterMembershipProcessFixture, bootstrapConfigPaths []string, label string) []string {
	t.Helper()
	paths := make([]string, len(bootstrapConfigPaths))
	for index, originalPath := range bootstrapConfigPaths {
		configBytes, err := os.ReadFile(originalPath)
		if err != nil {
			t.Fatal(err)
		}
		var config clusterMembershipProcessConfig
		if err := json.Unmarshal(configBytes, &config); err != nil {
			t.Fatal(err)
		}
		config.Directory = filepath.Join(fixture.tempDir, fmt.Sprintf("%s-node-%d-data", label, index+1))
		config.Management.AuditDirectory = filepath.Join(fixture.tempDir, fmt.Sprintf("%s-node-%d-audit", label, index+1))
		paths[index] = filepath.Join(fixture.tempDir, fmt.Sprintf("%s-node-%d.json", label, index+1))
		clusterMembershipProcessWriteJSON(t, paths[index], config)
	}
	return paths
}

func clusterBackupReleaseRestore(ctx context.Context, binary, repoRoot, configPath, archivePath string) ([]byte, error) {
	configPath, err := clusterProcessConfigForBinary(binary, configPath)
	if err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, binary, "-cluster-config", configPath, "-cluster-restore", archivePath)
	command.Dir = repoRoot
	return command.CombinedOutput()
}
