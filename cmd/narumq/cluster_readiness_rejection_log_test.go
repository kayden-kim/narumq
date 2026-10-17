package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	raftadapter "narumq/internal/adapters/raft"
)

// readinessLogTestRuntime returns a runtime that has no committed tenant quota profile and no
// initialized projection, so every local snapshot check rejects. The driver goroutine answers
// one fresh read request with the given response.
func readinessLogTestRuntime(t *testing.T, output *bytes.Buffer, read *clusterReadResponse) *clusterRuntime {
	t.Helper()
	config := validBootstrapClusterConfig()
	runtime := &clusterRuntime{
		config:   config,
		node:     new(raftadapter.Node),
		machine:  retainedTestMachine(t, config, config.ReleaseVersion),
		storage:  newClusterStorageMonitor(func() time.Time { return time.Unix(1, 0) }, nil),
		requests: make(chan clusterDriverRequest, 1),
		logger:   slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	if read != nil {
		go func() {
			request := <-runtime.requests
			request.readResult <- *read
		}()
	}
	return runtime
}

func singleReadinessRejection(t *testing.T, output *bytes.Buffer, purpose, stage string) map[string]any {
	t.Helper()
	records := decodeReadinessLogRecords(t, output)
	if len(records) != 1 {
		t.Fatalf("readiness rejection records = %d, want 1: %v", len(records), records)
	}
	record := records[0]
	if record["msg"] != "cluster readiness rejected" || record["purpose"] != purpose || record["readiness_stage"] != stage {
		t.Fatalf("readiness rejection record = %v, want msg, purpose=%s, readiness_stage=%s", record, purpose, stage)
	}
	return record
}

func requireReadinessSnapshotFields(t *testing.T, record map[string]any, want bool) {
	t.Helper()
	for _, key := range []string{"proof_index", "applied_index", "projection_index", "initialized", "ready_sequence_before", "ready_sequence_after"} {
		if _, present := record[key]; present != want {
			t.Fatalf("readiness rejection field %q present=%t, want %t: %v", key, present, want, record)
		}
	}
}

// Traceability: API-04; docs/specs/management-api-and-observability.md §4.
// Scenarios: management-api-and-observability.md:MGT-SCN-045
func TestReadinessRejectionLogRecordsFreshProofOnlyAfterFreshReadBarrier(t *testing.T) {
	t.Run("publish local projection admission is never a fresh proof", func(t *testing.T) {
		var output bytes.Buffer
		runtime := readinessLogTestRuntime(t, &output, nil)
		if runtime.clusterProjectionReady() {
			t.Fatal("uninitialized projection was admitted")
		}
		record := singleReadinessRejection(t, &output, "publish", "readiness_local_state")
		if record["fresh_proof_confirmed"] != false {
			t.Fatalf("publish admission fresh_proof_confirmed = %v, want false", record["fresh_proof_confirmed"])
		}
		requireReadinessSnapshotFields(t, record, true)
	})

	t.Run("queued publish admission after the Ready drain", func(t *testing.T) {
		var output bytes.Buffer
		runtime := readinessLogTestRuntime(t, &output, nil)
		result := make(chan clusterReadResponse, 1)
		runtime.projectionRequests = []clusterDriverRequest{{
			kind: clusterDriverProjection, context: context.Background(), readResult: result,
		}}
		runtime.resolveProjectionRequests(nil, true)
		if response := <-result; !errors.Is(response.err, errClusterRuntimeNotReady) {
			t.Fatalf("queued admission error = %v, want %v", response.err, errClusterRuntimeNotReady)
		}
		record := singleReadinessRejection(t, &output, "publish", "readiness_local_state")
		if record["fresh_proof_confirmed"] != false {
			t.Fatalf("queued publish admission fresh_proof_confirmed = %v, want false", record["fresh_proof_confirmed"])
		}
		requireReadinessSnapshotFields(t, record, true)
	})

	t.Run("local snapshot rejection after the fresh read barrier", func(t *testing.T) {
		var output bytes.Buffer
		runtime := readinessLogTestRuntime(t, &output, &clusterReadResponse{index: 7})
		if err := runtime.checkReadiness(context.Background(), "mqtt"); !errors.Is(err, errClusterRuntimeNotReady) {
			t.Fatalf("checkReadiness() = %v, want %v", err, errClusterRuntimeNotReady)
		}
		record := singleReadinessRejection(t, &output, "mqtt", "readiness_local_state")
		if record["fresh_proof_confirmed"] != true || record["proof_index"] != float64(7) {
			t.Fatalf("fresh_proof_confirmed=%v proof_index=%v, want true and 7", record["fresh_proof_confirmed"], record["proof_index"])
		}
		requireReadinessSnapshotFields(t, record, true)
	})

	t.Run("fresh proof stage rejection omits the snapshot", func(t *testing.T) {
		var output bytes.Buffer
		runtime := readinessLogTestRuntime(t, &output, &clusterReadResponse{err: context.DeadlineExceeded})
		if err := runtime.checkReadiness(context.Background(), "mqtt"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("checkReadiness() = %v, want %v", err, context.DeadlineExceeded)
		}
		record := singleReadinessRejection(t, &output, "mqtt", "readiness_fresh_read")
		if record["fresh_proof_confirmed"] != false {
			t.Fatalf("fresh read failure fresh_proof_confirmed = %v, want false", record["fresh_proof_confirmed"])
		}
		requireReadinessSnapshotFields(t, record, false)
	})

	storageFailure := fmt.Errorf("%w: %w", raftadapter.ErrFailed, raftadapter.ErrStorage)
	t.Run("storage stage rejection omits the snapshot", func(t *testing.T) {
		var output bytes.Buffer
		runtime := readinessLogTestRuntime(t, &output, nil)
		runtime.storage.fail(storageFailure)
		if err := runtime.checkReadiness(context.Background(), "mqtt"); !errors.Is(err, errClusterStorageUnhealthy) {
			t.Fatalf("checkReadiness() = %v, want %v", err, errClusterStorageUnhealthy)
		}
		record := singleReadinessRejection(t, &output, "mqtt", "readiness_storage")
		if record["fresh_proof_confirmed"] != false {
			t.Fatalf("storage rejection fresh_proof_confirmed = %v, want false", record["fresh_proof_confirmed"])
		}
		requireReadinessSnapshotFields(t, record, false)
	})

	t.Run("publish storage rejection omits the snapshot", func(t *testing.T) {
		var output bytes.Buffer
		runtime := readinessLogTestRuntime(t, &output, nil)
		runtime.storage.fail(storageFailure)
		if runtime.clusterProjectionReady() {
			t.Fatal("failed storage admitted a publish projection")
		}
		record := singleReadinessRejection(t, &output, "publish", "readiness_storage")
		if record["fresh_proof_confirmed"] != false {
			t.Fatalf("publish storage rejection fresh_proof_confirmed = %v, want false", record["fresh_proof_confirmed"])
		}
		requireReadinessSnapshotFields(t, record, false)
	})
}
