package integration

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Traceability: CLU-14; ADR-0023; docs/specs/cluster-session-and-delivery.md §11.22.
// Acceptance: NARUMQ_WAL_LIFECYCLE
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-157
func TestDynamicClusterWALCompactionProcessAcceptance(t *testing.T) {
	if os.Getenv("NARUMQ_WAL_LIFECYCLE") != "1" {
		t.Skip("set NARUMQ_WAL_LIFECYCLE=1 to run WAL compaction process acceptance")
	}
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	ctx := fixture.ctx
	processes := make([]*clusterMembershipProcess, 3)
	configs := make([]string, 3)
	for i := range processes {
		configs[i] = fixture.bootstrapConfig(uint64(i + 1))
		process, err := fixture.harness.start(fmt.Sprintf("wal lifecycle node %d", i+1), configs[i], fixture.managementAddresses[i])
		if err != nil {
			t.Fatal(err)
		}
		processes[i] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, processes)
	leaderID, _ := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	_, first := clusterReleaseCompatLoadUntilSnapshot(t, ctx, fixture, processes, leaderID, []uint64{1, 2, 3}, 0)
	before := make([]os.FileInfo, 3)
	for i := range processes {
		before[i] = clusterWALLifecycleFileInfo(t, filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-data", i+1), "wal.bin"))
	}
	leaderID, _ = clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	payload, second := clusterReleaseCompatLoadUntilSnapshot(t, ctx, fixture, processes, leaderID, []uint64{1, 2, 3}, first)
	for i := range processes {
		directory := filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-data", i+1))
		latest := clusterSubscriptionReplayLatestSnapshotIndex(t, directory)
		files, err := filepath.Glob(filepath.Join(directory, "snapshot-*.bin"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 1 {
			t.Fatalf("node %d snapshot files=%v, want exactly one", i+1, files)
		}
		index, err := clusterSubscriptionReplayParseSnapshotIndex(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(files[0]), "snapshot-"), ".bin"))
		if err != nil || index != latest || index < second {
			t.Fatalf("node %d snapshot index=%d latest=%d second=%d error=%v", i+1, index, latest, second, err)
		}
		if _, err := os.Stat(filepath.Join(directory, "wal.bin.tmp")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("node %d WAL temp stat=%v", i+1, err)
		}
		temps, err := filepath.Glob(filepath.Join(directory, "snapshot-*.bin.tmp"))
		if err != nil {
			t.Fatal(err)
		}
		for _, temp := range temps {
			index, err := clusterSubscriptionReplayParseSnapshotIndex(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(temp), "snapshot-"), ".bin.tmp"))
			if err != nil {
				t.Fatal(err)
			}
			if index < latest {
				t.Fatalf("node %d stale snapshot temp=%s latest=%d", i+1, temp, latest)
			}
		}
		after := clusterWALLifecycleFileInfo(t, filepath.Join(directory, "wal.bin"))
		replaced := !os.SameFile(before[i], after)
		t.Logf("node %d snapshot index=%d wal.bin bytes=%d replaced=%t", i+1, latest, after.Size(), replaced)
		if !replaced {
			t.Fatalf("node %d WAL was not replaced", i+1)
		}
	}
	for i, process := range processes {
		if err := process.stop(5 * time.Second); err != nil {
			t.Fatalf("stop node %d: %v", i+1, err)
		}
	}
	for i := range processes {
		process, err := fixture.harness.start(fmt.Sprintf("wal lifecycle node %d restart", i+1), configs[i], fixture.managementAddresses[i])
		if err != nil {
			t.Fatal(err)
		}
		processes[i] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, processes)
	for _, id := range []uint64{1, 2, 3} {
		clusterReleaseCompatAssertRetained(t, fixture, id, payload)
	}
}

// File.Stat captures Windows file identity immediately; os.Stat loads it lazily
// when SameFile runs and would identify the replacement instead of the old file.
func clusterWALLifecycleFileInfo(t *testing.T, path string) os.FileInfo {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	info, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil {
		t.Fatal(statErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	return info
}
