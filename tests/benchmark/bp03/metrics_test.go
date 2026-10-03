package main

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteWALSummaryUsesBucketDeltasAndOverflow(t *testing.T) {
	const start = `# HELP narumq_raft_wal_sync_duration_seconds test
# TYPE narumq_raft_wal_sync_duration_seconds histogram
narumq_raft_wal_sync_duration_seconds_bucket{le="0.001"} 10
narumq_raft_wal_sync_duration_seconds_bucket{le="0.01"} 20
narumq_raft_wal_sync_duration_seconds_bucket{le="+Inf"} 20
narumq_raft_wal_sync_duration_seconds_count 20
narumq_raft_wal_sync_duration_seconds_sum 0.015
# TYPE narumq_raft_wal_sync_entries histogram
narumq_raft_wal_sync_entries_bucket{le="1"} 10
narumq_raft_wal_sync_entries_bucket{le="2"} 20
narumq_raft_wal_sync_entries_bucket{le="+Inf"} 20
narumq_raft_wal_sync_entries_sum 30
narumq_raft_wal_sync_entries_count 20
# TYPE narumq_raft_wal_written_bytes_total counter
narumq_raft_wal_written_bytes_total 1000
`
	const end = `narumq_raft_wal_sync_duration_seconds_bucket{le="0.001"} 60
narumq_raft_wal_sync_duration_seconds_bucket{le="0.01"} 109
narumq_raft_wal_sync_duration_seconds_bucket{le="+Inf"} 110
narumq_raft_wal_sync_duration_seconds_count 110
narumq_raft_wal_sync_duration_seconds_sum 0.065
narumq_raft_wal_sync_entries_bucket{le="1"} 60
narumq_raft_wal_sync_entries_bucket{le="2"} 110
narumq_raft_wal_sync_entries_bucket{le="+Inf"} 110
narumq_raft_wal_sync_entries_sum 120
narumq_raft_wal_sync_entries_count 110
narumq_raft_wal_written_bytes_total 5100
`
	dir := t.TempDir()
	summary, err := writeWALSummary(dir, "narumq", []walWindowInput{{Name: "step-01", Duration: 10 * time.Second, AckedMessages: 200, Nodes: []walNodeInput{{Name: "node1", Start: []byte(start), End: []byte(end)}}}})
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Valid {
		t.Fatalf("summary invalid: %v", summary.FailureReasons)
	}
	cluster := summary.Windows[0].Cluster
	if cluster.SyncCount != 90 || cluster.EntriesSum != 90 || cluster.WALBytes != 4100 || cluster.SyncsPerSecond != 9 || cluster.MeanEntriesPerSync != 1 || cluster.WALBytesPerAckedMessage == nil || *cluster.WALBytesPerAckedMessage != 20.5 {
		t.Fatalf("unexpected cluster deltas: %+v", cluster)
	}
	if cluster.SyncDurationSeconds.P50.UpperBound == nil || *cluster.SyncDurationSeconds.P50.UpperBound != .001 || cluster.SyncDurationSeconds.P99.UpperBound != nil || !cluster.SyncDurationSeconds.P99.Overflow || !cluster.SyncDurationSeconds.P999.Overflow {
		t.Fatalf("unexpected duration quantiles: %+v", cluster.SyncDurationSeconds)
	}
	if cluster.EntriesPerSync.P50.UpperBound == nil || *cluster.EntriesPerSync.P50.UpperBound != 1 || cluster.EntriesPerSync.P99.UpperBound == nil || *cluster.EntriesPerSync.P99.UpperBound != 2 || cluster.EntriesPerSync.P99.Overflow {
		t.Fatalf("unexpected entry quantiles: %+v", cluster.EntriesPerSync)
	}
	var persisted WALSummary
	data, err := os.ReadFile(filepath.Join(dir, "wal-summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if !persisted.Valid || persisted.Target != "narumq" {
		t.Fatalf("unexpected persisted summary: %+v", persisted)
	}
}

func TestWriteWALSummaryMissingFamilyPersistsInvalidSummary(t *testing.T) {
	const scrape = `narumq_raft_wal_sync_duration_seconds_bucket{le="+Inf"} 2
narumq_raft_wal_sync_duration_seconds_count 2
narumq_raft_wal_sync_duration_seconds_sum 0
narumq_raft_wal_sync_entries_bucket{le="+Inf"} 2
narumq_raft_wal_sync_entries_count 2
narumq_raft_wal_sync_entries_sum 0
`
	dir := t.TempDir()
	summary, err := writeWALSummary(dir, "narumq", []walWindowInput{{Name: "offline", Duration: time.Second, Nodes: []walNodeInput{{Name: "node1", Start: []byte(scrape), End: []byte(scrape)}}}})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Valid || len(summary.FailureReasons) == 0 || !strings.Contains(strings.Join(summary.FailureReasons, " "), "written_bytes_total") {
		t.Fatalf("missing family should invalidate summary: %+v", summary)
	}
	if _, err := os.Stat(filepath.Join(dir, "wal-summary.json")); err != nil {
		t.Fatalf("invalid summary was not persisted: %v", err)
	}
}

func TestWriteWALSummaryEMQXNotApplicable(t *testing.T) {
	dir := t.TempDir()
	summary, err := writeWALSummary(dir, "emqx", nil)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Valid || summary.Applicability != "not_applicable" {
		t.Fatalf("unexpected EMQX summary: %+v", summary)
	}
}

func TestHistogramQuantileRequiresPositiveInfinityWhenNoFiniteBucketQualifies(t *testing.T) {
	start := walHistogram{Buckets: map[float64]uint64{1: 0, math.Inf(1): 0}}
	end := walHistogram{Buckets: map[float64]uint64{1: 4, math.Inf(1): 5}}
	got := histogramQuantile(start, end, 5, .999)
	if got.UpperBound != nil || !got.Overflow {
		t.Fatalf("expected overflow quantile, got %+v", got)
	}
}
