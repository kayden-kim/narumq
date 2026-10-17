package metrics

import (
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
