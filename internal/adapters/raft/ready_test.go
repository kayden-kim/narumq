package raft

import (
	"errors"
	"testing"
	"time"

	etcdraft "go.etcd.io/raft/v3"
)

// processReadyForTest consumes opaque adapter history synchronously so protocol
// fixtures retain deterministic logical delivery. Runtime tests poll directly.
func processReadyForTest(t testing.TB, node *Node) (BatchDTO, error) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var output BatchDTO
	for {
		batch, err := node.ProcessReady()
		if err != nil {
			return BatchDTO{}, err
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
			return BatchDTO{}, err
		}
		if !node.StoragePending() {
			return output, nil
		}
		if !batch.Progress {
			select {
			case <-node.StorageReady():
			case <-timer.C:
				return BatchDTO{}, errors.New("timed out consuming asynchronous Raft storage")
			}
		}
	}
}

// appendReadyForTest writes fixture records through the production batch writer
// without starting a worker; callers own this isolated stableStore.
func appendReadyForTest(store *stableStore, ready etcdraft.Ready) error {
	record, err := readyRecordMode(ready, store.dynamic)
	if err != nil {
		return err
	}
	worker := WALAppendWorker{store: store}
	if err := worker.persistBatch([]*walAppendRequest{{record: record}}); err != nil {
		return err
	}
	return store.applyRecord(record)
}
