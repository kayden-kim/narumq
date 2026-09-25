package main

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	raftadapter "narumq/internal/adapters/raft"
)

const (
	clusterStorageSlowThreshold  = time.Second
	clusterStorageRecoveryWindow = 10 * time.Second
)

const (
	clusterReplicationHealthy     = "HEALTHY"
	clusterReplicationDegraded    = "DEGRADED"
	clusterReplicationFailed      = "FAILED"
	clusterReplicationUnreachable = "UNREACHABLE"
)

const (
	clusterErrorStorageNoSpace     = "storage_no_space"
	clusterErrorStorageIO          = "storage_io_error"
	clusterErrorStorageSlow        = "storage_slow"
	clusterErrorRaftNodeFailed     = "raft_node_failed"
	clusterErrorPeerDisconnected   = "peer_disconnected"
	clusterErrorPeerQueueFull      = "peer_queue_full"
	clusterErrorPeerInvalidMessage = "peer_invalid_message"
	clusterErrorPeerTransport      = "peer_transport_error"
)

type clusterStorageMonitor struct {
	now    func() time.Time
	logger *slog.Logger

	mu            sync.Mutex
	inFlightSince time.Time
	lastSlowAt    time.Time
	failureCode   string
	lastErrorCode string
}

type clusterStorageView struct {
	health          string
	lastErrorCode   string
	storagePressure bool
	reason          string
}

func newClusterStorageMonitor(now func() time.Time, logger *slog.Logger) *clusterStorageMonitor {
	return &clusterStorageMonitor{now: now, logger: logger}
}

func (monitor *clusterStorageMonitor) begin() {
	monitor.mu.Lock()
	monitor.inFlightSince = monitor.now()
	monitor.mu.Unlock()
}

func (monitor *clusterStorageMonitor) end() {
	completedAt := monitor.now()
	monitor.mu.Lock()
	if monitor.inFlightSince.IsZero() {
		monitor.mu.Unlock()
		return
	}
	duration := completedAt.Sub(monitor.inFlightSince)
	monitor.inFlightSince = time.Time{}
	if duration < clusterStorageSlowThreshold {
		monitor.mu.Unlock()
		return
	}
	monitor.lastSlowAt = completedAt
	if monitor.failureCode == "" {
		monitor.lastErrorCode = clusterErrorStorageSlow
	}
	logger := monitor.logger
	monitor.mu.Unlock()
	if logger != nil {
		logger.Warn("cluster raft storage operation was slow", "duration", duration)
	}
}

func (monitor *clusterStorageMonitor) fail(err error) {
	monitor.mu.Lock()
	defer monitor.mu.Unlock()
	if monitor.failureCode != "" {
		return
	}
	switch {
	case raftadapter.IsStorageFull(err):
		monitor.failureCode = clusterErrorStorageNoSpace
	case errors.Is(err, raftadapter.ErrStorage):
		monitor.failureCode = clusterErrorStorageIO
	default:
		monitor.failureCode = clusterErrorRaftNodeFailed
	}
	monitor.lastErrorCode = monitor.failureCode
}

func (monitor *clusterStorageMonitor) view() clusterStorageView {
	now := monitor.now()
	monitor.mu.Lock()
	defer monitor.mu.Unlock()
	view := clusterStorageView{health: clusterReplicationHealthy, lastErrorCode: monitor.lastErrorCode}
	if monitor.failureCode != "" {
		view.health = clusterReplicationFailed
		switch monitor.failureCode {
		case clusterErrorStorageNoSpace:
			view.storagePressure = true
			view.reason = "raft storage failed: no space left; restart required"
		case clusterErrorStorageIO:
			view.storagePressure = true
			view.reason = "raft storage failed: I/O error; restart required"
		default:
			view.reason = "raft node failed; restart required"
		}
		return view
	}
	if !monitor.inFlightSince.IsZero() && now.Sub(monitor.inFlightSince) >= clusterStorageSlowThreshold {
		monitor.lastErrorCode = clusterErrorStorageSlow
		view.lastErrorCode = clusterErrorStorageSlow
		view.health = clusterReplicationDegraded
		view.storagePressure = true
		view.reason = "raft storage I/O is slow"
		return view
	}
	if !monitor.lastSlowAt.IsZero() && now.Sub(monitor.lastSlowAt) < clusterStorageRecoveryWindow {
		view.health = clusterReplicationDegraded
		view.storagePressure = true
		view.reason = "raft storage I/O is slow"
	}
	return view
}

// handleDriverFailure records a terminal Raft failure and reports whether serve must stay up fail-closed.
func (runtime *clusterRuntime) handleDriverFailure(err error) bool {
	if !errors.Is(err, raftadapter.ErrFailed) {
		return false
	}
	runtime.storage.fail(err)
	view := runtime.storage.view()
	runtime.logger.Error("cluster raft node failed; node stays unready until restart", "error_code", view.lastErrorCode, "error", err)
	return true
}
