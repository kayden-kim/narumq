package main

import (
	"context"

	raftadapter "narumq/internal/adapters/raft"
)

const (
	clusterReadIndexFrameBytes = 256 // Includes the 128-byte context, protobuf metadata and framing.
	clusterReadIndexReserve    = 2
	clusterReadIndexFrames     = 2 // Heartbeat and proof response compete in the same peer FIFO.
	clusterReadIndexLimit      = (32 - clusterReadIndexReserve) / clusterReadIndexFrames
)

// readIndexCapacity accounts for writes already queued and reads not flushed in a Ready yet.
// A leader needs remote capacity for a quorum, not for every potentially slow voter.
func (runtime *clusterRuntime) readIndexCapacity(outstanding int) int {
	if runtime.transport == nil {
		return clusterDriverStageLimit
	}
	status := runtime.node.Status()
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
			return available // Preserve the existing no-leader ReadIndex deadline behavior.
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
