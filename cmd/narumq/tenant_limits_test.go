package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	adaptersmetrics "narumq/internal/adapters/metrics"
	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/tenancy"
)

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-028
func TestTenantLimitRejectionReporterCountsEveryRejectionAndRateLimitsWarnings(t *testing.T) {
	registry := adaptersmetrics.NewRegistry()
	var logs bytes.Buffer
	reporter := &tenantLimitRejectionReporter{
		metrics: registry,
		logger:  slog.New(slog.NewTextHandler(&logs, nil)),
	}
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		reporter.TenantLimitRejected(tenantID, tenantlimit.DimensionMessageRate)
	}
	output := string(registry.Render(false))
	if !strings.Contains(output, `narumq_tenant_limit_rejections_total{tenant="tenant-a",dimension="message_rate"} 3`) {
		t.Fatalf("rejection counter did not count every rejection:\n%s", output)
	}
	if count := strings.Count(logs.String(), "tenant limit rejected a request"); count != 1 {
		t.Fatalf("warning count = %d, want one within the 10-second window: %s", count, logs.String())
	}
	if !strings.Contains(logs.String(), `tenant=tenant-a`) || !strings.Contains(logs.String(), `dimension=message_rate`) {
		t.Fatalf("warning missing tenant/dimension attributes: %s", logs.String())
	}
	reporter.TenantLimitRejected(tenantID, tenantlimit.DimensionPayloadSize)
	if count := strings.Count(logs.String(), "tenant limit rejected a request"); count != 2 {
		t.Fatalf("another dimension was incorrectly throttled: %s", logs.String())
	}
	key := tenantQuotaRejectionLogKey{tenantID: "tenant-a", dimension: string(tenantlimit.DimensionMessageRate)}
	reporter.log.lastLog[key] = time.Now().Add(-10 * time.Second)
	reporter.TenantLimitRejected(tenantID, tenantlimit.DimensionMessageRate)
	if count := strings.Count(logs.String(), "tenant limit rejected a request"); count != 3 {
		t.Fatalf("warning did not resume after 10s: %s", logs.String())
	}
	if output := string(registry.Render(false)); !strings.Contains(output, `narumq_tenant_limit_rejections_total{tenant="tenant-a",dimension="message_rate"} 4`) {
		t.Fatalf("throttling lost a counter increment: %s", output)
	}
	reporter.TenantLimitRejected(tenantID, tenantlimit.Dimension("invalid"))
	if count := strings.Count(logs.String(), "tenant limit rejected a request"); count != 3 {
		t.Fatalf("unknown dimension was logged: %s", logs.String())
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-028
func TestNewTenantLimitServiceOptionalConfiguration(t *testing.T) {
	if service, err := newTenantLimitService(nil, adaptersmetrics.NewRegistry(), nil); err != nil || service != nil {
		t.Fatalf("nil bundle service = (%v, %v), want (nil, nil)", service, err)
	}
	if service, err := newTenantLimitService(&SecurityBundle{}, adaptersmetrics.NewRegistry(), nil); err != nil || service != nil {
		t.Fatalf("absent limits service = (%v, %v), want (nil, nil)", service, err)
	}
	service, err := newTenantLimitService(&SecurityBundle{TenantLimits: &tenantlimit.Profile{MaxWait: time.Second}}, adaptersmetrics.NewRegistry(), nil)
	if err != nil || service == nil {
		t.Fatalf("configured tenant limit service = (%v, %v), want non-nil service", service, err)
	}
	service, err = newTenantLimitService(&SecurityBundle{TenantLimits: &tenantlimit.Profile{MaxWait: -1}}, adaptersmetrics.NewRegistry(), nil)
	if err == nil || service != nil {
		t.Fatalf("invalid profile accepted: service=%v err=%v", service, err)
	}
}
