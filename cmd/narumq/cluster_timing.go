package main

import (
	"time"

	adaptersmetrics "narumq/internal/adapters/metrics"
)

func (runtime *clusterRuntime) observeRaftStage(stage adaptersmetrics.RaftStage, startedAt time.Time) {
	if runtime.metrics != nil && !startedAt.IsZero() {
		runtime.metrics.ObserveRaftStage(stage, time.Since(startedAt))
	}
}

// finishCommitRead runs only on the driver and records each proof once.
func (runtime *clusterRuntime) finishCommitRead(pending *clusterPendingCommit, err error) {
	if pending.readConfirmed {
		return
	}
	runtime.observeRaftStage(adaptersmetrics.RaftReadIndex, pending.readStartedAt)
	pending.readConfirmed, pending.readErr = true, err
}
