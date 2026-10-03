package main

import (
	"errors"
	"testing"
	"time"

	raftadapter "narumq/internal/adapters/raft"
)

// processReadyForTest consumes opaque node history in deterministic fixtures.
// Actual drivers use ProcessReady, StorageReady and CompleteApply directly.
func processReadyForTest(t testing.TB, node *raftadapter.Node) (raftadapter.BatchDTO, error) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var output raftadapter.BatchDTO
	for {
		batch, err := node.ProcessReady()
		if err != nil {
			return raftadapter.BatchDTO{}, err
		}
		output.Progress = output.Progress || batch.Progress
		output.Messages = append(output.Messages, batch.Messages...)
		output.Commands = append(output.Commands, batch.Commands...)
		output.CommandIndexes = append(output.CommandIndexes, batch.CommandIndexes...)
		output.ConfigurationChanges = append(output.ConfigurationChanges, batch.ConfigurationChanges...)
		output.ReadStates = append(output.ReadStates, batch.ReadStates...)
		if len(batch.SnapshotData) != 0 {
			output.SnapshotData, output.SnapshotIndex = batch.SnapshotData, batch.SnapshotIndex
		}
		if err := node.CompleteApply(batch.ApplySequence); err != nil {
			return raftadapter.BatchDTO{}, err
		}
		if !node.StoragePending() {
			return output, nil
		}
		if !batch.Progress {
			select {
			case <-node.StorageReady():
			case <-timer.C:
				return raftadapter.BatchDTO{}, errors.New("timed out consuming asynchronous Raft storage")
			}
		}
	}
}
