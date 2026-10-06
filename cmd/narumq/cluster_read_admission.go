package main

import (
	"context"
	"encoding/binary"

	adaptersmetrics "narumq/internal/adapters/metrics"
	raftadapter "narumq/internal/adapters/raft"
)

const (
	clusterReadIndexFrameBytes = 256 // Includes the 128-byte context, protobuf metadata and framing.
	clusterReadIndexReserve    = 2
	clusterReadIndexFrames     = 2 // Heartbeat and proof response compete in the same peer FIFO.
	clusterReadIndexLimit      = (32 - clusterReadIndexReserve) / clusterReadIndexFrames
	clusterReadProofHeader     = 16 // Origin node ID and per-emission nonce.
)

// readIndexCapacity accounts for writes already queued and reads not flushed in a Ready yet.
// A leader needs remote capacity for a quorum, not for every potentially slow voter.
func (runtime *clusterRuntime) readIndexCapacity(outstanding int) int {
	if runtime.transport == nil {
		return clusterDriverStageLimit
	}
	return runtime.readIndexCapacityForStatus(runtime.node.Status(), outstanding)
}

func (runtime *clusterRuntime) readIndexCapacityForStatus(status raftadapter.StatusDTO, outstanding int) int {
	if runtime.transport == nil {
		return clusterDriverStageLimit
	}
	// Independent origins fan out into the same leader-side peer queues.
	sources := min(clusterReadIndexLimit, max(1, len(status.VoterIDs)+len(status.LearnerIDs)))
	available := clusterReadIndexLimit/sources - outstanding
	if available <= 0 {
		return 0
	}
	peerCapacity := func(peerID raftadapter.NodeID) int {
		capacity, err := runtime.transport.OutboundCapacity(uint64(peerID))
		if err != nil || runtime.faults.isolates(uint64(peerID)) {
			return 0
		}
		messages := (capacity.Messages-clusterReadIndexReserve)/(clusterReadIndexFrames*sources) - runtime.readIndexStaged
		bytes := (capacity.Bytes/clusterReadIndexFrameBytes-clusterReadIndexReserve)/(clusterReadIndexFrames*sources) - runtime.readIndexStaged
		return max(0, min(available, messages, bytes))
	}
	if status.Role != raftadapter.RoleLeader {
		if status.LeaderID == 0 {
			return 0 // Keep the original FIFO request until Raft knows where to send it.
		}
		return peerCapacity(status.LeaderID)
	}
	if len(status.VoterIDs) <= 1 {
		return available
	}
	var counts [clusterReadIndexLimit + 1]int
	for _, peerID := range status.VoterIDs {
		if peerID != status.NodeID {
			counts[peerCapacity(peerID)]++
		}
	}
	needed := len(status.VoterIDs) / 2
	for capacity := clusterReadIndexLimit; capacity > 0; capacity-- {
		needed -= counts[capacity]
		if needed <= 0 {
			return capacity
		}
	}
	return 0
}

// forwardedProposalCapacity keeps deferred replay inside the actual peer FIFO budget.
// It runs under Node's lock and must not call Node or wait for transport progress.
func (runtime *clusterRuntime) forwardedProposalCapacity(peerID raftadapter.NodeID) (int, int) {
	if runtime == nil || runtime.transport == nil {
		return 0, 0
	}
	for _, isolatedPeerID := range runtime.forwardedProposalIsolations {
		if isolatedPeerID == uint64(peerID) {
			return 0, 0
		}
	}
	if status, err := runtime.transport.Status(uint64(peerID)); err != nil || !status.Connected {
		return 0, 0
	}
	capacity, err := runtime.transport.OutboundCapacity(uint64(peerID))
	if err != nil {
		return 0, 0
	}
	return max(0, capacity.Messages-clusterReadIndexReserve),
		max(0, capacity.Bytes-clusterReadIndexReserve*clusterReadIndexFrameBytes)
}

func driverRequestContext(request clusterDriverRequest) context.Context {
	if request.kind == clusterDriverCommit {
		return request.pending.readContext
	}
	return request.context
}

// tryDriverRequest leaves one unissued FIFO head in driver ownership, without resetting its deadline.
func (runtime *clusterRuntime) tryDriverRequest(request clusterDriverRequest, reads map[string]clusterReadWaiter, proposals *[]clusterProposalWaiter) bool {
	if request.kind == clusterDriverRead || request.kind == clusterDriverCommit {
		ctx := driverRequestContext(request)
		if (ctx == nil || ctx.Err() == nil) && runtime.readIndexCapacity(len(reads)) == 0 {
			runtime.deferredRequest = request
			runtime.hasDeferredRequest = true
			return false
		}
	}
	before := len(reads)
	runtime.handleDriverRequest(request, reads, proposals)
	if len(reads) > before {
		runtime.readIndexStaged++
	}
	return true
}

func (runtime *clusterRuntime) deferredRequestDone() <-chan struct{} {
	if runtime.hasDeferredRequest {
		if ctx := driverRequestContext(runtime.deferredRequest); ctx != nil {
			return ctx.Done()
		}
	}
	return nil
}

func (runtime *clusterRuntime) driverRequestChannel() <-chan clusterDriverRequest {
	if runtime.hasDeferredRequest {
		return nil
	}
	return runtime.requests
}

// The caller key stays stable for cancellation; only the wire proof changes.
func (runtime *clusterRuntime) issueReadProof(key string, waiter *clusterReadWaiter, status raftadapter.StatusDTO) error {
	var wire [128]byte
	if len(key) > len(wire)-clusterReadProofHeader {
		return raftadapter.ErrInvalidMessage
	}
	nonce := runtime.sequence.Add(1)
	binary.BigEndian.PutUint64(wire[:8], uint64(status.NodeID))
	binary.BigEndian.PutUint64(wire[8:clusterReadProofHeader], nonce)
	copy(wire[clusterReadProofHeader:], key)
	if err := runtime.node.ReadIndex(wire[:clusterReadProofHeader+len(key)]); err != nil {
		return err
	}
	waiter.proofSequence = nonce
	waiter.leadership = raftLeadership{leader: status.LeaderID, term: status.Term, role: status.Role}
	return nil
}

// A Raft leadership reset may discard an issued read without reporting an
// error. Re-issue only this read-only proof, with a new nonce and old deadline.
func (runtime *clusterRuntime) refreshReadWaiters(reads map[string]clusterReadWaiter) {
	if len(reads) == 0 {
		return
	}
	status := runtime.node.Status()
	leadership := raftLeadership{leader: status.LeaderID, term: status.Term, role: status.Role}
	outstanding := 0
	for key, waiter := range reads {
		if waiter.leadership != leadership {
			waiter.proofSequence = 0
			waiter.leadership = leadership
			reads[key] = waiter
		}
		if waiter.proofSequence != 0 {
			outstanding++
		}
	}
	if status.LeaderID == 0 {
		return
	}
	for key, waiter := range reads {
		if waiter.proofSequence != 0 || (waiter.ctx != nil && waiter.ctx.Err() != nil) {
			continue
		}
		if runtime.readIndexCapacityForStatus(status, outstanding) == 0 {
			return
		}
		if err := runtime.issueReadProof(key, &waiter, status); err != nil {
			delete(reads, key)
			if waiter.commit != nil {
				runtime.finishCommitRead(waiter.commit, err)
			} else {
				runtime.observeRaftStage(adaptersmetrics.RaftReadIndex, waiter.startedAt)
				if waiter.result != nil {
					waiter.result <- clusterReadResponse{err: err}
				} else if key == runtime.recoveryProofKey {
					runtime.recoveryProofKey = ""
				}
			}
			continue
		}
		reads[key] = waiter
		runtime.readIndexStaged++
		outstanding++
	}
}
