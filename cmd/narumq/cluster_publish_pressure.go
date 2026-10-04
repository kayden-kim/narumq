package main

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/application/membership"
	"narumq/internal/application/publish"
)

type clusterCommitBacklog struct {
	mu      sync.Mutex
	entries list.List
}

func (b *clusterCommitBacklog) add(now time.Time) *list.Element {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.entries.PushBack(now)
}
func (b *clusterCommitBacklog) remove(entry *list.Element) {
	if b == nil || entry == nil {
		return
	}
	b.mu.Lock()
	b.entries.Remove(entry)
	b.mu.Unlock()
}
func (b *clusterCommitBacklog) oldest(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if entry := b.entries.Front(); entry != nil {
		age := now.Sub(entry.Value.(time.Time))
		if age > 0 {
			return age
		}
	}
	return 0
}

type clusterPublishPressureController struct {
	runtime      *clusterRuntime
	minFreeBytes uint64
	maxCommitLag time.Duration
	freeBytes    func(string) (uint64, error)
	backlog      clusterCommitBacklog
	diskPressure atomic.Bool
	lagActive    atomic.Bool
	sampleErr    atomic.Bool
	rejectionLog map[string]time.Time
	logMu        sync.Mutex
	wg           sync.WaitGroup
}

func (c *clusterPublishPressureController) start(ctx context.Context) {
	if c == nil {
		return
	}
	c.sample(time.Now())
	c.wg.Go(func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				c.sample(now)
			}
		}
	})
}
func (c *clusterPublishPressureController) stopAndWait() {
	if c != nil {
		c.wg.Wait()
	}
}
func (c *clusterPublishPressureController) diskPressureActive() bool {
	return c != nil && c.minFreeBytes > 0 && c.diskPressure.Load()
}

func (c *clusterPublishPressureController) sample(now time.Time) {
	free, override := c.runtime.faults.freeBytesOverride()
	var err error
	if !override {
		free, err = c.freeBytes(c.runtime.config.Directory)
	}
	if err != nil {
		if !c.sampleErr.Swap(true) && c.runtime.logger != nil {
			c.runtime.logger.Warn("cluster storage free space unavailable", "error", err)
		}
	} else {
		c.sampleErr.Store(false)
		if c.runtime.metrics != nil {
			c.runtime.metrics.SetStorageFreeBytes(free)
		}
		active := c.diskPressure.Load()
		if c.minFreeBytes == 0 {
			active = false
		} else if free < c.minFreeBytes {
			active = true
		} else if free >= c.minFreeBytes+c.minFreeBytes/8 {
			active = false
		}
		if old := c.diskPressure.Swap(active); old != active && c.runtime.logger != nil {
			if active {
				c.runtime.logger.Warn("publish backpressure activated", "reason", "disk_free", "free_bytes", free, "min_free_bytes", c.minFreeBytes)
			} else {
				c.runtime.logger.Info("publish backpressure cleared", "reason", "disk_free", "free_bytes", free)
			}
		}
	}
	age := c.backlog.oldest(now)
	active := age >= c.maxCommitLag
	if old := c.lagActive.Swap(active); old != active && c.runtime.logger != nil {
		if active {
			c.runtime.logger.Warn("publish backpressure activated", "reason", "commit_lag", "oldest_seconds", age.Seconds())
		} else {
			c.runtime.logger.Info("publish backpressure cleared", "reason", "commit_lag", "oldest_seconds", age.Seconds())
		}
	}
	if c.runtime.metrics == nil {
		return
	}
	c.runtime.metrics.SetCommitBacklogOldest(age)
	c.runtime.metrics.SetPublishBackpressureActive("disk_free", c.diskPressureActive())
	c.runtime.metrics.SetPublishBackpressureActive("commit_lag", active)
	var lags map[uint64]uint64
	if c.runtime.node != nil {
		status := c.runtime.node.Status()
		if uint64(status.LeaderID) == c.runtime.config.NodeID {
			lags = make(map[uint64]uint64)
			for _, member := range c.runtime.machine.Members() {
				if member.MemberID == c.runtime.config.NodeID || member.Phase == membership.MemberPhaseRetired {
					continue
				}
				if match, ok := c.runtime.node.PeerMatchIndex(raftadapter.NodeID(member.MemberID)); ok {
					lag := uint64(0)
					if status.CommitIndex > match {
						lag = status.CommitIndex - match
					}
					lags[member.MemberID] = lag
				}
			}
		}
	}
	c.runtime.metrics.SetPeerReplicationLag(lags)
}

func (c *clusterPublishPressureController) shouldLog(reason string, now time.Time) bool {
	c.logMu.Lock()
	defer c.logMu.Unlock()
	if c.rejectionLog == nil {
		c.rejectionLog = make(map[string]time.Time)
	}
	if last, ok := c.rejectionLog[reason]; ok && now.Sub(last) < 10*time.Second {
		return false
	}
	c.rejectionLog[reason] = now
	return true
}

func (c *clusterPublishPressureController) CheckPressure() error {
	reason := ""
	if c.diskPressureActive() {
		reason = "disk_free"
	} else if c.backlog.oldest(time.Now()) >= c.maxCommitLag {
		reason = "commit_lag"
	}
	if reason == "" {
		return nil
	}
	if c.runtime.metrics != nil {
		c.runtime.metrics.RecordPublishBackpressureRejection(reason)
	}
	if c.shouldLog(reason, time.Now()) && c.runtime.logger != nil {
		c.runtime.logger.Warn("publish rejected by backpressure", "reason", reason)
	}
	return fmt.Errorf("%w: %s", publish.ErrPublishBackpressure, reason)
}

func (c *clusterPublishPressureController) CheckDeliveryCapacity(change membership.DeliveryChangeDTO) error {
	result := c.runtime.machine.DeliveryPublishCapacity(change)
	if result.Fits {
		return nil
	}
	if result.Reason == membership.DeliveryCapacityTenantQuota {
		if c.runtime.metrics != nil {
			c.runtime.metrics.RecordPublishBackpressureRejection("tenant_quota")
		}
		c.runtime.reportTenantQuotaRejection(result.TenantQuota)
	} else {
		if c.runtime.metrics != nil {
			c.runtime.metrics.RecordPublishBackpressureRejection("delivery_capacity")
		}
		if c.shouldLog(result.Reason, time.Now()) && c.runtime.logger != nil {
			c.runtime.logger.Warn("publish rejected by durable delivery capacity", "reason", result.Reason, "tenant", change.TenantID)
		}
	}
	return fmt.Errorf("%w: %s", publish.ErrDurableDeliveryCapacity, result.Reason)
}

func (runtime *clusterRuntime) addCommitBacklog(now time.Time) *list.Element {
	if runtime == nil || runtime.publishPressure == nil {
		return nil
	}
	return runtime.publishPressure.backlog.add(now)
}
func (runtime *clusterRuntime) removeCommitBacklog(entry *list.Element) {
	if runtime != nil && runtime.publishPressure != nil {
		runtime.publishPressure.backlog.remove(entry)
	}
}
func (runtime *clusterRuntime) reportDeliverySaturation(result membership.ApplyResultDTO) {
	if runtime != nil && runtime.metrics != nil && result.Delivery != nil {
		runtime.metrics.AddDurableDeliverySaturatedTargets(len(result.Delivery.Saturated))
	}
}
