package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/application/membership"
)

// recoverProposals is driver-owned. Each leadership generation obtains a new
// nonce-bearing quorum proof before replaying still-live commands once.
func (runtime *clusterRuntime) recoverProposals(ctx context.Context, reads map[string]clusterReadWaiter, proposals *[]clusterProposalWaiter) (bool, error) {
	if runtime.machine == nil {
		return false, nil
	}
	// A restored receipt already proves durable apply. It does not need another
	// election to resolve its local waiter. Expired callers never receive an ACK.
	if !runtime.projectionFailed.Load() && runtime.projectionIndex.Load() >= runtime.machine.LastAppliedIndex() {
		for i := range *proposals {
			waiter := &(*proposals)[i]
			identity := waiter.command.Proposal
			if waiter.applied || identity == nil {
				continue
			}
			if waiter.context != nil && waiter.context.Err() != nil {
				waiter.response = clusterApplyResponse{err: waiter.context.Err()}
				waiter.applied = true
				continue
			}
			if receipt, found := runtime.machine.ProposalReceipt(identity.OriginNodeID, identity.RequestID); found {
				if receipt.ProofIndex != identity.ProofIndex || !bytes.Equal(receipt.CommandDigest, identity.CommandDigest) {
					return false, membership.ErrInvalidCommand
				}
				waiter.response = clusterApplyResponse{result: receipt.Result}
				waiter.applied = true
			}
		}
		runtime.resolveAppliedProposals(proposals)
	}
	if !hasUnappliedProposals(*proposals) {
		if runtime.recoveryProofKey != "" {
			delete(reads, runtime.recoveryProofKey)
		}
		runtime.recoveryProofKey = ""
		runtime.recoveryProofWait, runtime.recoveryProofReady = false, false
		return false, nil
	}
	status := runtime.node.Status()
	leadership := raftLeadership{leader: status.LeaderID, term: status.Term, role: status.Role}
	if leadership != runtime.recoveryLeadership {
		if runtime.recoveryProofKey != "" {
			delete(reads, runtime.recoveryProofKey)
		}
		runtime.recoveryLeadership = leadership
		runtime.recoveryProofKey = ""
		runtime.recoveryProofReady = false
		runtime.recoveryProofWait = false
		for _, waiter := range *proposals {
			if waiter.command.Proposal != nil && !waiter.applied && (waiter.context == nil || waiter.context.Err() == nil) && waiter.leadership != leadership {
				runtime.recoveryProofWait = true
				break
			}
		}
	}
	if status.LeaderID == 0 {
		return false, nil
	}
	if runtime.recoveryProofWait && runtime.recoveryProofKey == "" {
		if runtime.readIndexCapacityForStatus(status, len(reads)) == 0 {
			return false, nil
		}
		key := fmt.Sprintf("recovery-%d", runtime.sequence.Add(1))
		waiter := clusterReadWaiter{ctx: ctx, startedAt: time.Now()}
		if err := runtime.issueReadProof(key, &waiter, status); err != nil {
			return false, err
		}
		reads[key] = waiter
		runtime.recoveryProofKey = key
		runtime.readIndexStaged++
		return true, nil
	}
	if !runtime.recoveryProofReady || runtime.projectionFailed.Load() {
		return false, nil
	}
	var commands [clusterDriverStageLimit][]byte
	var owners [clusterDriverStageLimit]*clusterProposalWaiter
	count, size, replayed := 0, 0, 0
	flush := func() error {
		if count == 0 {
			return nil
		}
		if err := runtime.node.ProposeBatchForwarded(commands[:count]); err != nil {
			return err
		}
		for _, waiter := range owners[:count] {
			waiter.leadership = leadership
		}
		replayed += count
		clear(commands[:count])
		clear(owners[:count])
		count, size = 0, 0
		return nil
	}
	for i := range *proposals {
		waiter := &(*proposals)[i]
		if waiter.applied || waiter.command.Proposal == nil || (waiter.context != nil && waiter.context.Err() != nil) || waiter.leadership == leadership {
			continue
		}
		if count == len(commands) || (count != 0 && size+len(waiter.data) > clusterProposalBatchBytes) {
			if err := flush(); err != nil {
				if errors.Is(err, raftadapter.ErrBackpressure) {
					return replayed != 0, nil
				}
				return false, err
			}
		}
		commands[count], owners[count] = waiter.data, waiter
		count++
		size += len(waiter.data)
	}
	if err := flush(); err != nil {
		if errors.Is(err, raftadapter.ErrBackpressure) {
			return replayed != 0, nil
		}
		return false, err
	}
	runtime.recoveryProofReady = false
	if replayed != 0 && runtime.logger != nil {
		runtime.logger.Info("cluster pending commands re-proposed after fresh proof", "commands", replayed, "leader_id", status.LeaderID, "term", status.Term)
	}
	return replayed != 0, nil
}

// prepareProposal freezes the original payload from the already-owned canonical
// bytes. Retried commands must never observe subsequent caller slice mutations.
func (runtime *clusterRuntime) prepareProposal(pending *clusterPendingCommit, floor uint64) error {
	command, err := decodeMembershipCommand(pending.data)
	if err != nil {
		return err
	}
	if pending.requestID == "" {
		pending.requestID, err = newClusterOperationID()
		if err != nil {
			return err
		}
	}
	digest := sha256.Sum256(pending.data)
	command.Proposal = &membership.ProposalIdentityDTO{OriginNodeID: runtime.config.NodeID,
		ProofIndex: pending.readProofIndex, RetiredThrough: floor, RequestID: pending.requestID, CommandDigest: digest[:]}
	data, err := encodeMembershipCommand(command)
	if err != nil {
		return err
	}
	pending.command, pending.data = command, data
	return nil
}

func (runtime *clusterRuntime) refreshPendingProposalProof(pending *clusterPendingCommit, reads map[string]clusterReadWaiter) {
	pending.readConfirmed, pending.readErr, pending.readProofIndex = false, nil, 0
	waiter := clusterReadWaiter{ctx: pending.readContext, commit: pending}
	if err := runtime.issueReadProof(pending.contextKey, &waiter, runtime.node.Status()); err != nil {
		runtime.finishCommitRead(pending, err)
		return
	}
	reads[pending.contextKey] = waiter
}
