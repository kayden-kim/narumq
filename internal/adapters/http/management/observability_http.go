package management

import (
	"errors"
	"net/http"

	observationports "narumq/internal/application/ports/observability"
)

func writeObservabilityError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, observationports.ErrNodeNotFound):
		WriteError(w, r, http.StatusNotFound, "resource_not_found", "node not found", false, nil)
	case errors.Is(err, observationports.ErrInvalidCursor):
		WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid pagination cursor", false, nil)
	case errors.Is(err, observationports.ErrInvalidPageSize):
		writeInvalidRequest(w, r, err)
	case errors.Is(err, observationports.ErrMetricsCollectionIncomplete):
		WriteError(w, r, http.StatusServiceUnavailable, "metrics_collection_incomplete", "metrics collection is incomplete", true, nil)
	case errors.Is(err, observationports.ErrMetricsSnapshotTooLarge):
		WriteError(w, r, http.StatusServiceUnavailable, "metrics_snapshot_too_large", "metrics response exceeds the configured size limit", true, nil)
	case errors.Is(err, observationports.ErrMetricsAggregationFailed):
		WriteError(w, r, http.StatusServiceUnavailable, "metrics_aggregation_failed", "metrics snapshot does not match the aggregation contract", true, nil)
	default:
		WriteError(w, r, http.StatusInternalServerError, "internal_error", "an internal error occurred", false, nil)
	}
}

func writeObservabilityUnavailable(w http.ResponseWriter, r *http.Request) {
	WriteError(w, r, http.StatusServiceUnavailable, "observability_unavailable", "observability data is temporarily unavailable", true, nil)
}
