package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	adaptersmetrics "narumq/internal/adapters/metrics"
	"narumq/internal/application/membership"
	"narumq/internal/application/publish"
)

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.5, §11.28.
func TestClusterCommitBacklogOldestAge(t *testing.T) {
	var b clusterCommitBacklog
	now := time.Unix(100, 0)
	if age := b.oldest(now); age != 0 {
		t.Fatal(age)
	}
	first := b.add(now.Add(-3 * time.Second))
	middle := b.add(now.Add(-2 * time.Second))
	last := b.add(now.Add(-time.Second))
	b.remove(middle)
	if age := b.oldest(now); age != 3*time.Second {
		t.Fatal(age)
	}
	b.remove(first)
	if age := b.oldest(now); age != time.Second {
		t.Fatal(age)
	}
	b.remove(last)
	b.remove(nil)
	if age := b.oldest(now); age != 0 {
		t.Fatal(age)
	}
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §11.28.
func TestClusterPublishPressureDiskHysteresis(t *testing.T) {
	var logs bytes.Buffer
	r := &clusterRuntime{metrics: adaptersmetrics.NewRegistry(), logger: slog.New(slog.NewTextHandler(&logs, nil))}
	free := uint64(2 << 30)
	var sampleErr error
	c := &clusterPublishPressureController{runtime: r, minFreeBytes: 1 << 30, maxCommitLag: time.Second, freeBytes: func(string) (uint64, error) { return free, sampleErr }}
	r.publishPressure = c
	for _, tc := range []struct {
		free   uint64
		active bool
	}{{2 << 30, false}, {1<<30 - 1, true}, {1<<30 + 64<<20, true}, {1<<30 + 128<<20, false}} {
		free = tc.free
		c.sample(time.Now())
		err := c.CheckPressure()
		if c.diskPressureActive() != tc.active || errors.Is(err, publish.ErrPublishBackpressure) != tc.active {
			t.Fatalf("free=%d active=%v error=%v", free, c.diskPressureActive(), err)
		}
	}
	free = 1
	c.sample(time.Now())
	sampleErr = errors.New("measurement unavailable")
	c.sample(time.Now())
	c.sample(time.Now())
	if !c.diskPressureActive() || strings.Count(logs.String(), "cluster storage free space unavailable") != 1 {
		t.Fatalf("error changed pressure or repeated warning: %s", logs.String())
	}
	sampleErr = nil
	free = 2 << 30
	c.sample(time.Now())
	sampleErr = errors.New("new failure")
	c.sample(time.Now())
	if c.diskPressureActive() || strings.Count(logs.String(), "cluster storage free space unavailable") != 2 {
		t.Fatal("measurement recovery did not reset error episode")
	}
	sampleErr = nil
	c.minFreeBytes = 0
	free = 1
	c.sample(time.Now())
	if err := c.CheckPressure(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(r.metrics.Render(false)), "narumq_storage_free_bytes 1\n") {
		t.Fatal("disabled admission stopped gauge")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.start(ctx)
	cancel()
	c.stopAndWait()
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §11.28.
func TestClusterPublishPressureCommitLag(t *testing.T) {
	r := &clusterRuntime{metrics: adaptersmetrics.NewRegistry()}
	c := &clusterPublishPressureController{runtime: r, maxCommitLag: time.Second, freeBytes: func(string) (uint64, error) { return 2 << 30, nil }}
	r.publishPressure = c
	entry := r.addCommitBacklog(time.Now().Add(-2 * time.Second))
	if err := c.CheckPressure(); !errors.Is(err, publish.ErrPublishBackpressure) || !strings.Contains(err.Error(), "commit_lag") {
		t.Fatalf("lag rejection=%v", err)
	}
	c.sample(time.Now())
	text := string(r.metrics.Render(false))
	for _, line := range []string{"narumq_publish_backpressure_rejections_total{reason=\"commit_lag\"} 1\n", "narumq_publish_backpressure_active{reason=\"commit_lag\"} 1\n"} {
		if !strings.Contains(text, line) {
			t.Fatal(line)
		}
	}
	r.removeCommitBacklog(entry)
	if err := c.CheckPressure(); err != nil {
		t.Fatal(err)
	}
	c.sample(time.Now())
	if !strings.Contains(string(r.metrics.Render(false)), "narumq_cluster_commit_backlog_oldest_seconds 0.000000\n") {
		t.Fatal("completed entry remains in metric")
	}
	var nilRuntime *clusterRuntime
	nilRuntime.removeCommitBacklog(nil)
	if nilRuntime.addCommitBacklog(time.Now()) != nil {
		t.Fatal("nil runtime registered backlog")
	}
}

// Traceability: CLU-12, TEN-08; docs/specs/cluster-session-and-delivery.md §11.28.
func TestClusterPublishPressureCapacityReportsTenantQuota(t *testing.T) {
	machine := membership.NewStateMachine()
	apply := func(index uint64, command membership.CommandDTO) {
		t.Helper()
		command.SchemaMajor = membership.SchemaMajor
		command.SchemaMinor = membership.SchemaMinor
		command.OperationID = fmt.Sprintf("00000000-0000-4000-8000-%012d", index)
		result, err := machine.Apply(index, command)
		if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
			t.Fatalf("apply: %+v %v", result, err)
		}
	}
	profile := membership.DefaultTenantQuotaProfile()
	profile.Version = 1
	profile.Default.OfflineQueueMessages = 1
	apply(1, membership.CommandDTO{TenantQuota: &membership.TenantQuotaChangeDTO{Profile: profile}})
	apply(2, membership.CommandDTO{Ownership: &membership.OwnershipChangeDTO{TenantID: "acme", ClientID: "sub", OwnerNodeID: 1, Epoch: 1, Action: membership.OwnershipActionAcquire}})
	apply(3, membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{TenantID: "acme", ClientID: "sub", OwnerNodeID: 1, Epoch: 1, Filter: "bp/#", QoS: 1, Action: membership.SubscriptionActionAdd}})
	change := membership.DeliveryChangeDTO{TenantID: "acme", ClientID: "publisher", Topic: "bp/test", Payload: []byte("value"), PublishQoS: 1, ProposedAtUnixMillis: 1, Action: membership.DeliveryActionPublish, Targets: []membership.DeliveryTargetDTO{{ClientID: "sub"}}}
	apply(4, membership.CommandDTO{Delivery: &change})
	r := &clusterRuntime{machine: machine, metrics: adaptersmetrics.NewRegistry()}
	c := &clusterPublishPressureController{runtime: r}
	if err := c.CheckDeliveryCapacity(change); !errors.Is(err, publish.ErrDurableDeliveryCapacity) {
		t.Fatal(err)
	}
	text := string(r.metrics.Render(false))
	for _, line := range []string{"narumq_tenant_quota_rejections_total{tenant=\"acme\",dimension=\"offline_queue_messages\"} 1\n", "narumq_publish_backpressure_rejections_total{reason=\"tenant_quota\"} 1\n"} {
		if !strings.Contains(text, line) {
			t.Fatal(line)
		}
	}
}
