package metrics

import (
	"bytes"
	"fmt"
	"sort"
	"time"
)

var publishBackpressureReasons = [...]string{"commit_lag", "disk_free", "delivery_capacity", "tenant_quota"}

type peerReplicationLagEntry struct{ peer, lag uint64 }

// RecordPublishBackpressureRejection counts a known ingress rejection reason.
func (r *Registry) RecordPublishBackpressureRejection(reason string) {
	for i, known := range publishBackpressureReasons {
		if reason == known {
			r.publishBackpressureRejections[i].Add(1)
			return
		}
	}
}

// SetPublishBackpressureActive updates the sampled pressure state for disk or commit lag.
func (r *Registry) SetPublishBackpressureActive(reason string, active bool) {
	for i := range r.publishBackpressureActive {
		if reason == publishBackpressureReasons[i] {
			r.publishBackpressureActive[i].Store(active)
			return
		}
	}
}

// SetStorageFreeBytes records successfully measured filesystem space.
func (r *Registry) SetStorageFreeBytes(bytes uint64) {
	r.storageFreeBytes.Store(bytes)
	r.storageFreeBytesSet.Store(true)
}

// SetCommitBacklogOldest updates the age of the oldest unfinished delivery command.
func (r *Registry) SetCommitBacklogOldest(age time.Duration) { r.commitBacklogOldest.Store(int64(age)) }

// AddDurableDeliverySaturatedTargets counts targets skipped by committed apply.
func (r *Registry) AddDurableDeliverySaturatedTargets(n int) {
	if n > 0 {
		r.durableDeliverySaturated.Add(uint64(n))
	}
}

// SetPeerReplicationLag replaces the leader's observed peer lag set with an owned sorted copy.
func (r *Registry) SetPeerReplicationLag(lags map[uint64]uint64) {
	var entries []peerReplicationLagEntry
	if len(lags) > 0 {
		entries = make([]peerReplicationLagEntry, 0, len(lags))
	}
	for peer, lag := range lags {
		entries = append(entries, peerReplicationLagEntry{peer: peer, lag: lag})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].peer < entries[j].peer })
	r.mu.Lock()
	r.peerReplicationLag = entries
	r.mu.Unlock()
}

func (r *Registry) renderPublishBackpressure(buf *bytes.Buffer) {
	fmt.Fprintln(buf, "# HELP narumq_publish_backpressure_rejections_total New durable publishes rejected by admission reason.")
	fmt.Fprintln(buf, "# TYPE narumq_publish_backpressure_rejections_total counter")
	for i, reason := range publishBackpressureReasons {
		fmt.Fprintf(buf, "narumq_publish_backpressure_rejections_total{reason=\"%s\"} %d\n", reason, r.publishBackpressureRejections[i].Load())
	}
	fmt.Fprintln(buf, "# HELP narumq_publish_backpressure_active Whether a sampled ingress pressure reason is active.")
	fmt.Fprintln(buf, "# TYPE narumq_publish_backpressure_active gauge")
	for i := range r.publishBackpressureActive {
		value := 0
		if r.publishBackpressureActive[i].Load() {
			value = 1
		}
		fmt.Fprintf(buf, "narumq_publish_backpressure_active{reason=\"%s\"} %d\n", publishBackpressureReasons[i], value)
	}
	if r.storageFreeBytesSet.Load() {
		fmt.Fprintln(buf, "# HELP narumq_storage_free_bytes Filesystem bytes available to the local cluster data directory.")
		fmt.Fprintln(buf, "# TYPE narumq_storage_free_bytes gauge")
		fmt.Fprintf(buf, "narumq_storage_free_bytes %d\n", r.storageFreeBytes.Load())
	}
	fmt.Fprintln(buf, "# HELP narumq_cluster_commit_backlog_oldest_seconds Age of the oldest unfinished ingress delivery command.")
	fmt.Fprintln(buf, "# TYPE narumq_cluster_commit_backlog_oldest_seconds gauge")
	fmt.Fprintf(buf, "narumq_cluster_commit_backlog_oldest_seconds %.6f\n", time.Duration(r.commitBacklogOldest.Load()).Seconds())
	fmt.Fprintln(buf, "# HELP narumq_durable_delivery_saturated_targets_total Durable delivery targets skipped because committed capacity was saturated.")
	fmt.Fprintln(buf, "# TYPE narumq_durable_delivery_saturated_targets_total counter")
	fmt.Fprintf(buf, "narumq_durable_delivery_saturated_targets_total %d\n", r.durableDeliverySaturated.Load())
	fmt.Fprintln(buf, "# HELP narumq_raft_peer_replication_lag_entries Leader commit entries not yet matched by each peer.")
	fmt.Fprintln(buf, "# TYPE narumq_raft_peer_replication_lag_entries gauge")
	r.mu.RLock()
	for _, entry := range r.peerReplicationLag {
		fmt.Fprintf(buf, "narumq_raft_peer_replication_lag_entries{peer=\"%d\"} %d\n", entry.peer, entry.lag)
	}
	r.mu.RUnlock()
}
