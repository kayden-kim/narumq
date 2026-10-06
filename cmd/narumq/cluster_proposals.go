package main

import (
	"slices"
	"time"

	adaptersmetrics "narumq/internal/adapters/metrics"
	raftadapter "narumq/internal/adapters/raft"
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
	if err == nil {
		runtime.observeCommitLeadership()
	}
	for _, commit := range pending {
		if err != nil {
			runtime.observeRaftStage(adaptersmetrics.RaftProposal, startedAt)
			runtime.removeCommitBacklog(commit.backlogEntry)
			commit.result <- clusterApplyResponse{err: err}
		} else {
			runtime.markCommitProposed(commit.backlogEntry)
			*proposals = append(*proposals, clusterProposalWaiter{
				data: commit.data, result: commit.result, startedAt: startedAt, backlogEntry: commit.backlogEntry,
			})
		}
	}
	return err == nil
}

// reclaimUnsentProposals moves forwarded MsgProp frames that no socket write
// started for from disconnected peers into the Raft adapter's deferred FIFO.
func (runtime *clusterRuntime) reclaimUnsentProposals() {
	if runtime.transport == nil || runtime.node == nil {
		return
	}
	status := runtime.node.Status()
	peers := make([]raftadapter.NodeID, 0, len(status.VoterIDs)+len(status.LearnerIDs))
	for _, ids := range [][]raftadapter.NodeID{status.VoterIDs, status.LearnerIDs} {
		for _, id := range ids {
			if id != status.NodeID {
				peers = append(peers, id)
			}
		}
	}
	slices.Sort(peers)
	peers = slices.Compact(peers)
	for _, peerID := range peers {
		messages, bytes := runtime.node.DeferredProposalRoom()
		if messages == 0 {
			return
		}
		frames, err := runtime.transport.ReclaimUnsent(uint64(peerID), messages, bytes)
		if err != nil || len(frames) == 0 {
			continue
		}
		count := 0
		for _, data := range frames {
			if err := runtime.node.DeferForwardedProposal(data); err != nil {
				if runtime.logger != nil {
					runtime.logger.Warn("reclaimed raft proposal discarded", "peer_id", peerID, "error", err)
				}
			} else {
				count++
			}
		}
		if count > 0 && runtime.logger != nil {
			runtime.logger.Info("raft proposals reclaimed from disconnected peer", "peer_id", peerID, "messages", count)
		}
	}
}
