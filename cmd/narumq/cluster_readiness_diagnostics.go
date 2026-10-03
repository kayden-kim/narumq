package main

import (
	"context"
	"log/slog"
	"time"
)

// Only captured decision inputs are logged; diagnostics must not resample or retry readiness.
func (runtime *clusterRuntime) logReadinessFailure(purpose, stage string, err error, storage clusterStorageView, barrier uint64, snapshot *clusterReadinessSnapshot, sequenceBefore, sequenceAfter uint64, freshProofConfirmed bool) {
	if runtime.logger == nil {
		return
	}
	class := classifyCommitFailure(err)
	reason := class
	if stage == "readiness_storage" {
		reason = "storage_unhealthy"
	} else if snapshot != nil {
		reason = readinessSnapshotRejection(snapshot, barrier)
	}
	summary, emit := runtime.commitFailureLogSummary(stage, reason, purpose, time.Now())
	if !emit || !runtime.logger.Enabled(context.Background(), slog.LevelWarn) {
		return
	}
	var buffer [28]slog.Attr
	fields := append(buffer[:0],
		slog.Uint64("node_id", runtime.config.NodeID),
		slog.String("purpose", purpose),
		slog.String("readiness_stage", stage),
		slog.String("failure_class", class),
		slog.String("failure_reason", reason),
		slog.Any("error", err),
		slog.Uint64("failure_count", summary.totalCount),
		slog.Uint64("failures_since_previous_log", summary.sinceLastLog),
		slog.Time("first_failure_at", summary.firstAt),
		slog.Time("last_failure_at", summary.lastAt),
		slog.String("storage_health_before_read", storage.health),
		slog.String("storage_error_code_before_read", storage.lastErrorCode),
		slog.Bool("storage_pressure_before_read", storage.storagePressure),
		slog.Bool("fresh_proof_confirmed", freshProofConfirmed),
	)
	if snapshot != nil {
		fields = append(fields,
			slog.Uint64("proof_index", barrier),
			slog.Uint64("applied_index", snapshot.appliedIndex),
			slog.Uint64("projection_index", snapshot.projectionIndex),
			slog.Bool("initialized", snapshot.initialized),
			slog.Bool("projection_failed", snapshot.projectionFailed),
			slog.Bool("bootstrapped", snapshot.bootstrapped),
			slog.Bool("driver_failed", snapshot.driverFailed),
			slog.Bool("dynamic_membership", snapshot.dynamicMembership),
			slog.Bool("local_voter", snapshot.localVoter),
			slog.Bool("local_phase_voter", snapshot.localPhaseVoter),
			slog.Bool("tenant_quota_profile_committed", snapshot.tenantQuotaProfileCommitted),
			slog.Bool("release_profile_synced", snapshot.releaseProfileSynced),
			slog.Uint64("ready_sequence_before", sequenceBefore),
			slog.Uint64("ready_sequence_after", sequenceAfter),
		)
	}
	runtime.logger.LogAttrs(context.Background(), slog.LevelWarn, "cluster readiness rejected", fields...)
}

// Match satisfiedAt's predicate order; all flags remain in the log when several fail.
func readinessSnapshotRejection(snapshot *clusterReadinessSnapshot, barrier uint64) string {
	switch {
	case !snapshot.initialized:
		return "uninitialized"
	case snapshot.projectionFailed:
		return "projection_failed"
	case !snapshot.bootstrapped:
		return "not_bootstrapped"
	case snapshot.driverFailed:
		return "driver_failed"
	case !snapshot.dynamicMembership:
		return "not_dynamic_membership"
	case !snapshot.localVoter:
		return "local_not_voter"
	case !snapshot.localPhaseVoter:
		return "local_phase_not_voter"
	case !snapshot.tenantQuotaProfileCommitted:
		return "quota_profile_missing"
	case !snapshot.releaseProfileSynced:
		return "release_profile_unsynced"
	case snapshot.projectionIndex < snapshot.appliedIndex:
		return "projection_behind_applied"
	case barrier > snapshot.appliedIndex:
		return "proof_above_applied"
	default:
		return "unspecified"
	}
}
