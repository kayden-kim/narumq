package main

import (
	"time"

	adaptersmetrics "narumq/internal/adapters/metrics"
)

const clusterProposalBatchBytes = 64 << 10

// submitCommitBatch preserves one waiter and log entry per fresh-proof command.
// A rejected batch reports failure to every caller; it is not retried here.
func (runtime *clusterRuntime) submitCommitBatch(pending []*clusterPendingCommit, proposals *[]clusterProposalWaiter) bool {
	var commands [clusterDriverStageLimit][]byte
	for index, commit := range pending {
		commands[index] = commit.data
	}
	startedAt := time.Now()
	err := runtime.node.ProposeBatchForwarded(commands[:len(pending)])
	for _, commit := range pending {
		if err != nil {
			runtime.observeRaftStage(adaptersmetrics.RaftProposal, startedAt)
			commit.result <- clusterApplyResponse{err: err}
		} else {
			*proposals = append(*proposals, clusterProposalWaiter{
				data: commit.data, result: commit.result, startedAt: startedAt,
			})
		}
	}
	return err == nil
}
