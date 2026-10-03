package main

import (
	"context"
	"testing"
	"time"
)

// Traceability: CLU-11; docs/contracts/cluster-release-compatibility-v1.md §5.
func TestClusterReleaseProfileSnapshotWakeReconfirmsAfterStaleRead(t *testing.T) {
	runtime := clusterReleaseProfileTestRuntime(t, validBootstrapClusterConfig())
	controller := runtime.releaseProfile
	controller.synced.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	controller.start(ctx)
	t.Cleanup(func() { cancel(); controller.stopAndWait() })

	beforeRestore := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
	controller.observeSnapshotRestore()
	if runtime.releaseProfileSynced() {
		t.Fatal("snapshot restore did not close the gate")
	}
	beforeRestore.readResult <- clusterReadResponse{index: 1}
	oldFeatures := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
	oldFeatures.readResult <- clusterReadResponse{index: 1}

	// The initial reconcile was held before its ticker existed. Restoration
	// must wake a new revision immediately, not await the first periodic tick.
	var freshMembers clusterDriverRequest
	select {
	case freshMembers = <-runtime.requests:
	case <-time.After(clusterReleaseProfileRetryInterval / 2):
		t.Fatal("snapshot reconfirmation waited for the periodic reconcile")
	}
	if runtime.releaseProfileSynced() {
		t.Fatal("pre-restore reads reopened the gate")
	}
	freshMembers.readResult <- clusterReadResponse{index: 1}
	freshFeatures := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
	if runtime.releaseProfileSynced() {
		t.Fatal("gate reopened before both new fresh barriers completed")
	}
	freshFeatures.readResult <- clusterReadResponse{index: 1}
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for !runtime.releaseProfileSynced() {
		select {
		case <-ctx.Done():
			t.Fatal("matching profile did not reopen after fresh confirmation")
		case <-poll.C:
		}
	}
}
