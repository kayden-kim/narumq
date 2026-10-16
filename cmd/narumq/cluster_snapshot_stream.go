package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/membership"
)

// clusterSnapshotStreamSender sends one serialized MsgSnap through the snapshot stream (ADR-0062).
type clusterSnapshotStreamSender interface {
	SendSnapshot(context.Context, tcpadapter.SnapshotStreamDTO) error
}

// clusterSnapshotStreamReporter tells Raft how a snapshot stream ended.
type clusterSnapshotStreamReporter interface {
	ReportSnapshotStream(peerID raftadapter.NodeID, delivered bool) error
}

// clusterSnapshotStreams runs the leader side of the snapshot stream outside the Raft Ready loop.
// A transfer is one goroutine per peer. Raft keeps a peer in snapshot state until the result is
// reported, so a second MsgSnap for a peer with a transfer in flight is dropped.
type clusterSnapshotStreams struct {
	logger   *slog.Logger
	sender   clusterSnapshotStreamSender
	reporter clusterSnapshotStreamReporter
	isolated func(peerID uint64) bool
	// wake tells the driver that Raft has work: a reported result or a received snapshot.
	wake chan struct{}

	mu       sync.Mutex
	inFlight map[raftadapter.NodeID]struct{}
	workers  sync.WaitGroup
}

func newClusterSnapshotStreams(
	logger *slog.Logger,
	sender clusterSnapshotStreamSender,
	reporter clusterSnapshotStreamReporter,
	isolated func(uint64) bool,
) *clusterSnapshotStreams {
	return &clusterSnapshotStreams{
		logger: logger, sender: sender, reporter: reporter, isolated: isolated,
		wake: make(chan struct{}, 1), inFlight: make(map[raftadapter.NodeID]struct{}),
	}
}

// signal wakes the driver without blocking; notifications coalesce.
func (streams *clusterSnapshotStreams) signal() {
	if streams == nil {
		return
	}
	select {
	case streams.wake <- struct{}{}:
	default:
	}
}

// wait blocks until every transfer goroutine returned. Cancel the context given to start first.
func (streams *clusterSnapshotStreams) wait() {
	if streams != nil {
		streams.workers.Wait()
	}
}

// wakeChannel returns the channel the driver selects on; nil blocks forever when there is no coordinator.
func (streams *clusterSnapshotStreams) wakeChannel() <-chan struct{} {
	if streams == nil {
		return nil
	}
	return streams.wake
}

// start launches one transfer per listed snapshot unless the peer already has one in flight.
func (streams *clusterSnapshotStreams) start(ctx context.Context, requests []raftadapter.SnapshotStreamDTO) {
	if streams == nil {
		return
	}
	for _, request := range requests {
		if streams.isolated != nil && streams.isolated(uint64(request.To)) {
			streams.report(request.To, false)
			continue
		}
		streams.mu.Lock()
		_, busy := streams.inFlight[request.To]
		if !busy {
			streams.inFlight[request.To] = struct{}{}
			streams.workers.Add(1)
		}
		streams.mu.Unlock()
		if busy {
			continue
		}
		go streams.run(ctx, request)
	}
}

func (streams *clusterSnapshotStreams) run(ctx context.Context, request raftadapter.SnapshotStreamDTO) {
	defer streams.workers.Done()
	peer := uint64(request.To)
	streams.logger.Info("raft snapshot stream started",
		"peer_id", peer, "snapshot_index", request.Index, "total_bytes", len(request.Data))
	err := streams.sender.SendSnapshot(ctx, tcpadapter.SnapshotStreamDTO{
		To: peer, Index: request.Index, Term: request.Term, Data: request.Data,
	})
	if err == nil {
		streams.logger.Info("raft snapshot stream completed",
			"peer_id", peer, "snapshot_index", request.Index, "total_bytes", len(request.Data),
			"chunks", tcpadapter.SnapshotStreamChunkCount(len(request.Data)))
	} else {
		streams.logger.Warn("raft snapshot stream failed",
			"peer_id", peer, "snapshot_index", request.Index, "reason", tcpadapter.SnapshotStreamFailureReason(err))
	}
	streams.mu.Lock()
	delete(streams.inFlight, request.To)
	streams.mu.Unlock()
	streams.report(request.To, err == nil)
}

func (streams *clusterSnapshotStreams) report(peer raftadapter.NodeID, delivered bool) {
	err := streams.reporter.ReportSnapshotStream(peer, delivered)
	if err != nil && !errors.Is(err, raftadapter.ErrClosed) && !errors.Is(err, raftadapter.ErrFailed) {
		// The peer left the configuration while the transfer ran; Raft has nothing to retry.
		streams.logger.Warn("raft snapshot stream result not reported", "peer_id", uint64(peer), "delivered", delivered)
	}
	streams.signal()
}

// snapshotStreamActive reports whether the cluster activated the snapshot-stream feature.
// Raft calls it under the Node mutex; it only reads the state machine.
func (runtime *clusterRuntime) snapshotStreamActive() bool {
	if runtime == nil || runtime.machine == nil {
		return false
	}
	for _, feature := range runtime.machine.ActiveFeatures() {
		if feature == membership.SnapshotStreamFeature {
			return true
		}
	}
	return false
}

// receiveSnapshotStream hands a snapshot received over the stream to the local Raft node. The
// transport already verified its digest. A refusal makes the sender report the transfer failed.
func (runtime *clusterRuntime) receiveSnapshotStream(_ context.Context, receipt tcpadapter.SnapshotStreamReceiptDTO) error {
	if runtime.faults.isolates(receipt.From) {
		return errors.New("snapshot stream peer is isolated by fault injection")
	}
	err := runtime.node.StepSnapshotStream(raftadapter.MessageDTO{
		From: raftadapter.NodeID(receipt.From), To: raftadapter.NodeID(runtime.config.NodeID), Data: receipt.Data,
	}, receipt.Index, receipt.Term)
	if err != nil {
		return err
	}
	runtime.logger.Info("raft snapshot stream received",
		"from_peer_id", receipt.From, "snapshot_index", receipt.Index, "total_bytes", len(receipt.Data))
	runtime.snapshotStreams.signal()
	return nil
}
