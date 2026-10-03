package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"syscall"
	"testing"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
	appmgmt "narumq/internal/application/management"
	"narumq/internal/application/membership"
	managementports "narumq/internal/application/ports/management"
)

func TestClusterStorageMonitorHealthTransitions(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	monitor := newClusterStorageMonitor(func() time.Time { return now }, slog.New(slog.NewTextHandler(io.Discard, nil)))

	check := func(name, wantHealth, wantError, wantReason string, wantPressure bool) {
		t.Helper()
		view := monitor.view()
		if view.health != wantHealth || view.lastErrorCode != wantError || view.reason != wantReason || view.storagePressure != wantPressure {
			t.Fatalf("%s view = %+v, want health=%q error=%q reason=%q pressure=%t", name, view, wantHealth, wantError, wantReason, wantPressure)
		}
	}

	check("idle", clusterReplicationHealthy, "", "", false)
	monitor.begin()
	now = now.Add(clusterStorageSlowThreshold - time.Millisecond)
	check("in flight below threshold", clusterReplicationHealthy, "", "", false)
	now = now.Add(time.Millisecond)
	check("in flight at threshold", clusterReplicationDegraded, clusterErrorStorageSlow, "raft storage I/O is slow", true)
	now = now.Add(500 * time.Millisecond)
	monitor.end()
	check("slow operation completed", clusterReplicationDegraded, clusterErrorStorageSlow, "raft storage I/O is slow", true)
	now = now.Add(clusterStorageRecoveryWindow - time.Millisecond)
	check("recovery window", clusterReplicationDegraded, clusterErrorStorageSlow, "raft storage I/O is slow", true)
	now = now.Add(time.Millisecond)
	check("recovered", clusterReplicationHealthy, clusterErrorStorageSlow, "", false)
}

func TestClusterStorageMonitorFailureClassification(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantCode   string
		wantReason string
		wantPress  bool
	}{
		{
			name:       "disk full",
			err:        fmt.Errorf("%w: %w", raftadapter.ErrFailed, fmt.Errorf("%w: %w", raftadapter.ErrStorage, syscall.ENOSPC)),
			wantCode:   clusterErrorStorageNoSpace,
			wantReason: "raft storage failed: no space left; restart required",
			wantPress:  true,
		},
		{
			name:       "storage I/O",
			err:        fmt.Errorf("%w: %w", raftadapter.ErrFailed, fmt.Errorf("%w: %w", raftadapter.ErrStorage, syscall.EIO)),
			wantCode:   clusterErrorStorageIO,
			wantReason: "raft storage failed: I/O error; restart required",
			wantPress:  true,
		},
		{
			name:       "non-storage Raft failure",
			err:        raftadapter.ErrFailed,
			wantCode:   clusterErrorRaftNodeFailed,
			wantReason: "raft node failed; restart required",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
			monitor := newClusterStorageMonitor(func() time.Time { return now }, slog.New(slog.NewTextHandler(io.Discard, nil)))
			monitor.fail(testCase.err)
			view := monitor.view()
			if view.health != clusterReplicationFailed || view.lastErrorCode != testCase.wantCode || view.reason != testCase.wantReason || view.storagePressure != testCase.wantPress {
				t.Fatalf("failure view = %+v, want code=%q reason=%q pressure=%t", view, testCase.wantCode, testCase.wantReason, testCase.wantPress)
			}
			monitor.fail(raftadapter.ErrFailed)
			monitor.begin()
			now = now.Add(clusterStorageSlowThreshold)
			monitor.end()
			if got := monitor.view(); got.lastErrorCode != testCase.wantCode || got.health != clusterReplicationFailed {
				t.Fatalf("first failure changed after later failures: %+v", got)
			}
		})
	}
}

func TestHandleDriverFailureKeepsNodeFailClosed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	failedRuntime := &clusterRuntime{
		storage: newClusterStorageMonitor(func() time.Time { return now }, logger),
		logger:  logger,
	}
	failure := fmt.Errorf("process Ready: %w", fmt.Errorf("%w: %w", raftadapter.ErrFailed, raftadapter.ErrStorage))
	if !failedRuntime.handleDriverFailure(failure) {
		t.Fatal("handleDriverFailure() = false for terminal Raft failure")
	}
	if got := failedRuntime.storage.view(); got.health != clusterReplicationFailed || got.lastErrorCode != clusterErrorStorageIO {
		t.Fatalf("storage view after driver failure = %+v", got)
	}

	healthyRuntime := &clusterRuntime{
		storage: newClusterStorageMonitor(func() time.Time { return now }, logger),
		logger:  logger,
	}
	if healthyRuntime.handleDriverFailure(errors.New("serve dynamic peer transport")) {
		t.Fatal("handleDriverFailure() = true for non-Raft failure")
	}
	if got := healthyRuntime.storage.view(); got.health != clusterReplicationHealthy {
		t.Fatalf("storage view after non-Raft failure = %+v", got)
	}
}

func TestFailedStorageClosesReadinessAndCommit(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	runtime := &clusterRuntime{
		storage: newClusterStorageMonitor(func() time.Time { return now }, logger),
		logger:  logger,
	}
	runtime.storage.fail(fmt.Errorf("%w: %w", raftadapter.ErrFailed, raftadapter.ErrStorage))

	if err := runtime.checkReadiness(context.Background(), "health"); !errors.Is(err, errClusterStorageUnhealthy) {
		t.Fatalf("checkReadiness() = %v, want storage unhealthy", err)
	}
	if _, err := runtime.Commit(context.Background(), membership.CommandDTO{}); !errors.Is(err, membership.ErrQuorumUnavailable) {
		t.Fatalf("Commit() = %v, want quorum unavailable", err)
	}
}

func TestStorageHealthReadinessPreservesLifecycleState(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	monitor := newClusterStorageMonitor(func() time.Time { return now }, nil)
	monitor.begin()
	now = now.Add(clusterStorageSlowThreshold)
	state := appmgmt.NewService(appmgmt.ServiceConfig{StartedAt: now}, nil, nil, nil, nil)
	state.SetNodeState(managementports.NodeStateReady, nil)
	checker := &clusterHealthChecker{
		runtime: &clusterRuntime{storage: monitor, managementState: state},
	}

	status, reason := checker.CheckReadiness(context.Background())
	if status != managementports.HealthStatusNotReady || reason == nil || *reason != "raft storage I/O is slow" {
		t.Fatalf("CheckReadiness() = (%q, %v), want storage not ready", status, reason)
	}
	if got := state.NodeState(); got != managementports.NodeStateReady {
		t.Fatalf("NodeState() = %q, want lifecycle state to remain READY", got)
	}
}

// Traceability: API-04; docs/specs/management-api-and-observability.md §3, §8.
func TestClusterStartupWaitsForTypedRecovery(t *testing.T) {
	state := appmgmt.NewService(appmgmt.ServiceConfig{StartedAt: time.Unix(1, 0)}, nil, nil, nil, nil)
	state.SetNodeState(managementports.NodeStateReady, nil)
	runtime := &clusterRuntime{}
	checker := &clusterHealthChecker{HealthChecker: state, runtime: runtime}

	status, reason := checker.CheckStartup(context.Background())
	if status != managementports.HealthStatusStarting || reason == nil || *reason != "typed raft state has not been recovered" {
		t.Fatalf("startup before recovery=(%q,%v), want STARTING", status, reason)
	}
	runtime.initialized.Store(true)
	status, reason = checker.CheckStartup(context.Background())
	if status != managementports.HealthStatusUp || reason != nil {
		t.Fatalf("startup after typed recovery=(%q,%v), want UP", status, reason)
	}
}

func TestFailedStorageLivenessReturnsDown(t *testing.T) {
	monitor := newClusterStorageMonitor(func() time.Time { return time.Unix(1, 0) }, nil)
	monitor.fail(fmt.Errorf("%w: %w", raftadapter.ErrFailed, raftadapter.ErrStorage))
	checker := &clusterHealthChecker{runtime: &clusterRuntime{storage: monitor}}

	status, reason := checker.CheckLiveness(context.Background())
	if status != managementports.HealthStatusDown || reason == nil || *reason != "raft storage failed: I/O error; restart required" {
		t.Fatalf("CheckLiveness() = (%q, %v), want terminal storage failure", status, reason)
	}
}

func TestRemoteReplicationViewUsesSanitizedPeerHealth(t *testing.T) {
	cases := []struct {
		name          string
		status        tcpadapter.PeerTransportStatusDTO
		wantHealth    string
		wantErrorCode string
	}{
		{name: "connected", status: tcpadapter.PeerTransportStatusDTO{Connected: true}, wantHealth: clusterReplicationHealthy},
		{
			name: "queue full", status: tcpadapter.PeerTransportStatusDTO{Connected: true, LastError: "outbound queue full"},
			wantHealth: clusterReplicationHealthy, wantErrorCode: clusterErrorPeerQueueFull,
		},
		{
			name: "invalid message", status: tcpadapter.PeerTransportStatusDTO{Connected: true, LastError: "invalid peer message"},
			wantHealth: clusterReplicationHealthy, wantErrorCode: clusterErrorPeerInvalidMessage,
		},
		{
			name: "disconnected", status: tcpadapter.PeerTransportStatusDTO{LastError: "peer disconnected"},
			wantHealth: clusterReplicationUnreachable, wantErrorCode: clusterErrorPeerDisconnected,
		},
		{
			name: "unknown error", status: tcpadapter.PeerTransportStatusDTO{Connected: true, LastError: "private socket detail"},
			wantHealth: clusterReplicationHealthy, wantErrorCode: clusterErrorPeerTransport,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			health, errorCode, _ := remoteReplicationView(testCase.status)
			if health != testCase.wantHealth {
				t.Fatalf("health = %q, want %q", health, testCase.wantHealth)
			}
			if testCase.wantErrorCode == "" {
				if errorCode != nil {
					t.Fatalf("errorCode = %q, want nil", *errorCode)
				}
				return
			}
			if errorCode == nil || *errorCode != testCase.wantErrorCode || *errorCode == testCase.status.LastError {
				t.Fatalf("errorCode = %v, want sanitized %q", errorCode, testCase.wantErrorCode)
			}
		})
	}
}

func TestClusterReadyRejectsStorageFaults(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	degraded := newClusterStorageMonitor(func() time.Time { return now }, nil)
	degraded.begin()
	now = now.Add(clusterStorageSlowThreshold)
	failed := newClusterStorageMonitor(func() time.Time { return now }, nil)
	failed.fail(fmt.Errorf("%w: %w", raftadapter.ErrFailed, raftadapter.ErrStorage))

	for _, testCase := range []struct {
		name    string
		monitor *clusterStorageMonitor
	}{
		{name: "degraded", monitor: degraded},
		{name: "failed", monitor: failed},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime := &clusterRuntime{storage: testCase.monitor}
			if runtime.clusterReady() {
				t.Fatal("clusterReady() = true, want storage fault to reject admission")
			}
		})
	}
}
