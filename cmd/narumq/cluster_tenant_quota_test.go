package main

import (
	"bytes"
	"context"
	"log/slog"
	adaptersmetrics "narumq/internal/adapters/metrics"
	"narumq/internal/application/membership"
	"strings"
	"testing"
	"time"
)

func TestConfiguredTenantQuotaProfileUsesBuiltinOrStrictConfig(t *testing.T) {
	if got, want := configuredTenantQuotaProfile(clusterConfigFile{}), membership.DefaultTenantQuotaProfile(); !membership.EqualTenantQuotaProfile(got, want) {
		t.Fatalf("omitted tenant quota profile = %+v, want built-in %+v", got, want)
	}

	config := clusterConfigFile{TenantQuotas: validTenantQuotaProfileConfig()}
	got := configuredTenantQuotaProfile(config)
	if got.Version != 1 || len(got.Tenants) != 1 || got.Tenants[0].TenantID != "acme" || got.Default.StorageBytes != 4096 {
		t.Fatalf("configured tenant quota profile = %+v", got)
	}
}

func TestClusterTenantQuotaControllerLogsCommittedMismatchOnce(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.TenantQuotas = validTenantQuotaProfileConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	committed := config.TenantQuotas.membershipProfile()
	committed.Version = 2
	if _, err := machine.Apply(3, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: "00000000-0000-4000-8000-000000000002",
		TenantQuota: &membership.TenantQuotaChangeDTO{Profile: committed},
	}); err != nil {
		t.Fatalf("commit newer tenant quota profile: %v", err)
	}

	var logs bytes.Buffer
	metricsRegistry := adaptersmetrics.NewRegistry()
	runtime := &clusterRuntime{
		config: config, machine: machine,
		logger: slog.New(slog.NewTextHandler(&logs, nil)), metrics: metricsRegistry,
	}
	controller := &clusterTenantQuotaController{runtime: runtime}
	controller.reconcile(context.Background())
	controller.reconcile(context.Background())

	if got, want := strings.Count(logs.String(), "committed tenant quota profile differs from local configuration"), 1; got != want {
		t.Fatalf("mismatch warning count = %d, want %d; logs=%s", got, want, logs.String())
	}
	if got, found := machine.TenantQuotaProfile(); !found || !membership.EqualTenantQuotaProfile(got, committed) {
		t.Fatalf("mismatched local profile changed committed state: found=%v profile=%+v", found, got)
	}
	metricsOutput := string(metricsRegistry.Render(false))
	for _, line := range []string{
		"narumq_tenant_quota_profile_committed 1\n",
		"narumq_tenant_quota_profile_version 2\n",
		"narumq_tenant_quota_config_mismatch 1\n",
	} {
		if !strings.Contains(metricsOutput, line) {
			t.Errorf("mismatch profile gauge missing %q:\n%s", line, metricsOutput)
		}
	}
}

func TestTenantQuotaRejectionReporterRateLimitsByTenantAndDimension(t *testing.T) {
	var reporter tenantQuotaRejectionReporter
	now := time.Unix(1_000, 0)
	if !reporter.shouldLog("tenant-a", string(membership.TenantQuotaRetainedBytes), now) {
		t.Fatal("first rejection was suppressed")
	}
	if reporter.shouldLog("tenant-a", string(membership.TenantQuotaRetainedBytes), now.Add(9*time.Second)) {
		t.Fatal("rejection inside the 10-second window was not rate-limited")
	}
	if !reporter.shouldLog("tenant-a", string(membership.TenantQuotaOfflineQueueBytes), now.Add(time.Second)) {
		t.Fatal("different quota dimension was incorrectly rate-limited")
	}
	if !reporter.shouldLog("tenant-a", string(membership.TenantQuotaRetainedBytes), now.Add(10*time.Second)) {
		t.Fatal("rejection at the end of the 10-second window was rate-limited")
	}
}

func TestRuntimeTenantQuotaReporterCountsEveryRejectionAndRateLimitsWarnings(t *testing.T) {
	var logs bytes.Buffer
	runtime := &clusterRuntime{
		logger:  slog.New(slog.NewTextHandler(&logs, nil)),
		metrics: adaptersmetrics.NewRegistry(),
	}
	rejection := &membership.TenantQuotaRejectionDTO{
		TenantID: "acme", Dimension: membership.TenantQuotaRetainedBytes, ProfileVersion: 4,
	}
	runtime.reportTenantQuotaRejection(rejection)
	runtime.reportTenantQuotaRejection(rejection)

	output := string(runtime.metrics.Render(true))
	if !strings.Contains(output, `narumq_tenant_quota_rejections_total{tenant="acme",dimension="retained_bytes"} 2`) {
		t.Fatalf("quota rejection counter did not record both commits:\n%s", output)
	}
	if got, want := strings.Count(logs.String(), "tenant storage quota rejected a committed mutation"), 1; got != want {
		t.Fatalf("rate-limited warning count = %d, want %d; logs=%s", got, want, logs.String())
	}
}
