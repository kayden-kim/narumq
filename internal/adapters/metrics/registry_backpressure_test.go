package metrics

import (
	"strings"
	"testing"
	"time"
)

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.5, §11.28.
func TestRegistryPublishBackpressure(t *testing.T) {
	r := NewRegistry()
	if strings.Contains(string(r.Render(false)), "narumq_storage_free_bytes") {
		t.Fatal("space gauge published before first measurement")
	}
	r.RecordPublishBackpressureRejection("disk_free")
	r.RecordPublishBackpressureRejection("disk_free")
	r.RecordPublishBackpressureRejection("unknown")
	r.SetPublishBackpressureActive("disk_free", true)
	r.SetPublishBackpressureActive("tenant_quota", true)
	r.SetStorageFreeBytes(1024)
	r.SetCommitBacklogOldest(1250 * time.Millisecond)
	r.AddDurableDeliverySaturatedTargets(3)
	r.AddDurableDeliverySaturatedTargets(0)
	r.AddDurableDeliverySaturatedTargets(-1)
	lags := map[uint64]uint64{3: 7, 2: 9}
	r.SetPeerReplicationLag(lags)
	lags[2] = 100
	text := string(r.Render(false))
	for _, line := range []string{
		"narumq_publish_backpressure_rejections_total{reason=\"commit_lag\"} 0",
		"narumq_publish_backpressure_rejections_total{reason=\"disk_free\"} 2",
		"narumq_publish_backpressure_rejections_total{reason=\"delivery_capacity\"} 0",
		"narumq_publish_backpressure_rejections_total{reason=\"tenant_quota\"} 0",
		"narumq_publish_backpressure_active{reason=\"commit_lag\"} 0",
		"narumq_publish_backpressure_active{reason=\"disk_free\"} 1",
		"narumq_storage_free_bytes 1024",
		"narumq_cluster_commit_backlog_oldest_seconds 1.250000",
		"narumq_durable_delivery_saturated_targets_total 3",
		"narumq_raft_peer_replication_lag_entries{peer=\"2\"} 9",
		"narumq_raft_peer_replication_lag_entries{peer=\"3\"} 7",
	} {
		if !strings.Contains(text, line+"\n") {
			t.Errorf("missing sample %q", line)
		}
	}
	if strings.Index(text, "{peer=\"2\"}") > strings.Index(text, "{peer=\"3\"}") {
		t.Fatal("peer order not canonical")
	}
	r.SetPeerReplicationLag(map[uint64]uint64{4: 1})
	r.SetPublishBackpressureActive("disk_free", false)
	r.SetCommitBacklogOldest(0)
	text = string(r.Render(false))
	if strings.Contains(text, "{peer=\"2\"}") || strings.Contains(text, "{peer=\"3\"}") {
		t.Fatal("old peer series retained")
	}
	for _, line := range []string{"narumq_raft_peer_replication_lag_entries{peer=\"4\"} 1", "narumq_publish_backpressure_active{reason=\"disk_free\"} 0", "narumq_cluster_commit_backlog_oldest_seconds 0.000000"} {
		if !strings.Contains(text, line+"\n") {
			t.Error(line)
		}
	}
	r.SetPeerReplicationLag(nil)
	if strings.Contains(string(r.Render(false)), "narumq_raft_peer_replication_lag_entries{peer=") {
		t.Fatal("follower exposes stale leader series")
	}
}
