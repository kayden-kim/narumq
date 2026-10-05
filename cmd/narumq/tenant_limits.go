package main

import (
	"log/slog"
	"time"

	adaptersclock "narumq/internal/adapters/clock"
	adaptersmetrics "narumq/internal/adapters/metrics"
	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/tenancy"
)

type tenantLimitRejectionReporter struct {
	metrics *adaptersmetrics.Registry
	logger  *slog.Logger
	log     tenantQuotaRejectionReporter
}

func (reporter *tenantLimitRejectionReporter) TenantLimitRejected(tenantID tenancy.TenantID, dimension tenantlimit.Dimension) {
	if reporter == nil || reporter.metrics == nil {
		return
	}
	label := reporter.metrics.RecordTenantLimitRejection(tenantID.String(), string(dimension))
	if label == "" || reporter.logger == nil {
		return
	}
	if reporter.log.shouldLog(label, string(dimension), time.Now()) {
		reporter.logger.Warn("tenant limit rejected a request", "tenant", label, "dimension", string(dimension))
	}
}

func newTenantLimitService(bundle *SecurityBundle, registry *adaptersmetrics.Registry, logger *slog.Logger) (*tenantlimit.Service, error) {
	if bundle == nil || bundle.TenantLimits == nil {
		return nil, nil
	}
	reporter := &tenantLimitRejectionReporter{metrics: registry, logger: logger}
	return tenantlimit.New(adaptersclock.SystemClock{}, *bundle.TenantLimits, reporter)
}
