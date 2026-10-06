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
	status := runtime.node.Status()
	leadership := raftLeadership{leader: status.LeaderID, term: status.Term, role: status.Role}
	for _, commit := range pending {
		if err != nil {
			runtime.observeRaftStage(adaptersmetrics.RaftProposal, startedAt)
			runtime.removeCommitBacklog(commit.backlogEntry)
			commit.result <- clusterApplyResponse{err: err}
		} else {
			runtime.markCommitProposed(commit.backlogEntry)
			*proposals = append(*proposals, clusterProposalWaiter{
				context:      commit.context,
				requestID:    commit.requestID,
				proofIndex:   commit.readProofIndex,
				command:      commit.command,
				data:         commit.data,
				result:       commit.result,
				startedAt:    startedAt,
				leadership:   leadership,
				backlogEntry: commit.backlogEntry,
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

func hasUnappliedProposals(proposals []clusterProposalWaiter) bool {
	for _, w := range proposals {
		if !w.applied && w.command.Proposal != nil && (w.context == nil || w.context.Err() == nil) {
			return true
		}
	}
	return false
}

func (runtime *clusterRuntime) safeRetirementFloor(currentProof uint64, proposals []clusterProposalWaiter, staged ...*clusterPendingCommit) uint64 {
	minProof := currentProof
	for _, waiter := range proposals {
		if !waiter.applied && (waiter.context == nil || waiter.context.Err() == nil) && waiter.proofIndex > 0 && waiter.proofIndex < minProof {
			minProof = waiter.proofIndex
		}
	}
	for _, pending := range runtime.pendingCommits {
		if pending != nil && pending.context.Err() == nil && pending.readProofIndex > 0 && pending.readProofIndex < minProof {
			minProof = pending.readProofIndex
		}
	}
	for _, pending := range staged {
		if pending != nil && pending.context.Err() == nil && pending.readProofIndex > 0 && pending.readProofIndex < minProof {
			minProof = pending.readProofIndex
		}
	}
	floor := uint64(0)
	if minProof > 0 {
		floor = minProof - 1
	}
	if currentProof > 0 && floor >= currentProof {
		floor = currentProof - 1
	}
	return floor
}
