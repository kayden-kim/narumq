package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// Scenarios: performance-and-verification.md:PRF-SCN-017
func TestLogReadinessFailureSamplesRepeatedRejectionsAndOmitsSensitiveFields(t *testing.T) {
	if clusterCommitFailureLogInterval != time.Second {
		t.Fatalf("readiness sampling interval = %v, want 1s", clusterCommitFailureLogInterval)
	}
	var output bytes.Buffer
	runtime := &clusterRuntime{
		logger: slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	storage := clusterStorageView{health: clusterReplicationHealthy}
	snapshot := &clusterReadinessSnapshot{initialized: true, bootstrapped: true, localVoter: true}

	for range 3 {
		runtime.logReadinessFailure("publish", "readiness_local_state", errClusterRuntimeNotReady, storage, 7, snapshot, 1, 2, true)
	}
	records := decodeReadinessLogRecords(t, &output)
	if len(records) != 1 {
		t.Fatalf("records after 3 repeated rejections = %d, want only the first one", len(records))
	}
	first := records[0]
	if got := first["failure_count"]; got != float64(1) {
		t.Fatalf("first record failure_count = %v, want 1", got)
	}
	if got := first["failure_reason"]; got != "not_dynamic_membership" {
		t.Fatalf("first record failure_reason = %v, want not_dynamic_membership", got)
	}

	// Move the last sampled log one interval into the past instead of sleeping.
	runtime.commitFailureMu.Lock()
	for key, state := range runtime.commitFailureLogs {
		state.lastLoggedAt = state.lastLoggedAt.Add(-clusterCommitFailureLogInterval - time.Millisecond)
		runtime.commitFailureLogs[key] = state
	}
	runtime.commitFailureMu.Unlock()

	runtime.logReadinessFailure("publish", "readiness_local_state", errClusterRuntimeNotReady, storage, 7, snapshot, 1, 2, true)
	records = decodeReadinessLogRecords(t, &output)
	if len(records) != 1 {
		t.Fatalf("records after the sampling interval = %d, want 1", len(records))
	}
	sampled := records[0]
	if sampled["failure_count"] != float64(4) || sampled["failures_since_previous_log"] != float64(3) {
		t.Fatalf("sampled record failure_count=%v since_previous=%v, want 4 and 3", sampled["failure_count"], sampled["failures_since_previous_log"])
	}

	// A different stage or rejection class is sampled independently.
	runtime.logReadinessFailure("publish", "readiness_storage", errClusterStorageUnhealthy, clusterStorageView{health: "failed"}, 0, nil, 0, 0, false)
	runtime.logReadinessFailure("session", "readiness_local_state", errClusterRuntimeNotReady, storage, 7, snapshot, 1, 2, true)
	records = decodeReadinessLogRecords(t, &output)
	if len(records) != 2 {
		t.Fatalf("records for a different stage and purpose = %d, want 2", len(records))
	}
	if records[0]["failure_reason"] != "storage_unhealthy" || records[0]["failure_count"] != float64(1) {
		t.Fatalf("storage record = %v, want storage_unhealthy with count 1", records[0])
	}

	for _, record := range append([]map[string]any{first, sampled}, records...) {
		for key := range record {
			lower := strings.ToLower(key)
			for _, forbidden := range []string{"client", "tenant_id", "payload", "topic", "token"} {
				if strings.Contains(lower, forbidden) {
					t.Fatalf("readiness diagnostic field %q is forbidden", key)
				}
			}
		}
	}
}

func decodeReadinessLogRecords(t *testing.T, output *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode readiness log line %q: %v", line, err)
		}
		records = append(records, record)
	}
	output.Reset()
	return records
}
