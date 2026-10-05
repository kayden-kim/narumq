package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"narumq/internal/application/membership"
)

const (
	clusterTenantQuotaRetryInterval = 500 * time.Millisecond
	tenantQuotaRejectionLogInterval = 10 * time.Second
)

type clusterTenantQuotaController struct {
	runtime  *clusterRuntime
	mu       sync.Mutex
	wg       sync.WaitGroup
	mismatch string
}

func (controller *clusterTenantQuotaController) start(ctx context.Context) {
	if controller == nil || controller.runtime == nil || ctx == nil {
		return
	}
	controller.wg.Add(1)
	go func() {
		defer controller.wg.Done()
		controller.run(ctx)
	}()
}

func (controller *clusterTenantQuotaController) stopAndWait() {
	if controller != nil {
		controller.wg.Wait()
	}
}

func (controller *clusterTenantQuotaController) run(ctx context.Context) {
	controller.reconcile(ctx)
	ticker := time.NewTicker(clusterTenantQuotaRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			controller.reconcile(ctx)
		}
	}
}

func (controller *clusterTenantQuotaController) reconcile(ctx context.Context) {
	if controller == nil || controller.runtime == nil || controller.runtime.machine == nil || ctx == nil || ctx.Err() != nil {
		return
	}
	desired := configuredTenantQuotaProfile(controller.runtime.config)
	committed, found := controller.runtime.machine.TenantQuotaProfile()
	if !found {
		if controller.runtime.metrics != nil {
			controller.runtime.metrics.SetTenantQuotaProfile(false, 0, false)
		}
		controller.setMismatch(false, desired.Version, 0)
		controller.propose(ctx, desired)
		return
	}
	mismatch := committed.Version > desired.Version ||
		(committed.Version == desired.Version && !membership.EqualTenantQuotaProfile(committed, desired))
	if controller.runtime.metrics != nil {
		controller.runtime.metrics.SetTenantQuotaProfile(true, committed.Version, mismatch)
	}
	controller.setMismatch(mismatch, desired.Version, committed.Version)
	if mismatch {
		return
	}
	if committed.Version < desired.Version {
		controller.propose(ctx, desired)
	}
}

func configuredTenantQuotaProfile(config clusterConfigFile) membership.TenantQuotaProfileDTO {
	if config.TenantQuotas == nil {
		return membership.DefaultTenantQuotaProfile()
	}
	return config.TenantQuotas.membershipProfile()
}

func (controller *clusterTenantQuotaController) propose(ctx context.Context, profile membership.TenantQuotaProfileDTO) {
	operationID, err := newClusterOperationID()
	if err != nil {
		return
	}
	_, _ = controller.runtime.Commit(ctx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: operationID,
		TenantQuota: &membership.TenantQuotaChangeDTO{Profile: profile},
	})
}

func (controller *clusterTenantQuotaController) setMismatch(mismatch bool, configuredVersion, committedVersion uint64) {
	if controller == nil {
		return
	}
	key := ""
	if mismatch {
		key = fmt.Sprintf("configured=%d,committed=%d", configuredVersion, committedVersion)
	}
	controller.mu.Lock()
	if key == controller.mismatch {
		controller.mu.Unlock()
		return
	}
	controller.mismatch = key
	controller.mu.Unlock()
	if key != "" && controller.runtime != nil && controller.runtime.logger != nil {
		controller.runtime.logger.Warn("committed tenant quota profile differs from local configuration",
			"configured_version", configuredVersion, "committed_version", committedVersion)
	}
}

type tenantQuotaRejectionLogKey struct {
	tenantID  string
	dimension string
}

type tenantQuotaRejectionReporter struct {
	mu      sync.Mutex
	lastLog map[tenantQuotaRejectionLogKey]time.Time
}

func (reporter *tenantQuotaRejectionReporter) shouldLog(tenantID string, dimension string, now time.Time) bool {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if reporter.lastLog == nil {
		reporter.lastLog = make(map[tenantQuotaRejectionLogKey]time.Time)
	}
	key := tenantQuotaRejectionLogKey{tenantID: tenantID, dimension: dimension}
	if last, exists := reporter.lastLog[key]; exists && now.Sub(last) < tenantQuotaRejectionLogInterval {
		return false
	}
	reporter.lastLog[key] = now
	return true
}

func (runtime *clusterRuntime) reportTenantQuotaRejection(rejection *membership.TenantQuotaRejectionDTO) {
	if runtime == nil || rejection == nil {
		return
	}
	label := rejection.TenantID
	if runtime.metrics != nil {
		label = runtime.metrics.RecordTenantQuotaRejection(label, string(rejection.Dimension))
		if label == "" {
			return
		}
	}
	if !runtime.tenantQuotaRejectionLog.shouldLog(label, string(rejection.Dimension), time.Now()) || runtime.logger == nil {
		return
	}
	runtime.logger.Warn("tenant storage quota rejected a committed mutation",
		"tenant", label, "dimension", string(rejection.Dimension), "profile_version", rejection.ProfileVersion)
}
