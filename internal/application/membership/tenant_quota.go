package membership

import (
	"slices"

	"narumq/internal/domain/tenancy"
)

type tenantStorageUsage struct {
	retainedBytes int
	queueMessages int
	queueBytes    int
	storageBytes  int
}

func (machine *StateMachine) addRetainedUsageLocked(tenantID string, byteCount int) {
	machine.retainedBytes += byteCount
	usage := machine.tenantUsage[tenantID]
	usage.retainedBytes += byteCount
	usage.storageBytes += byteCount
	machine.setTenantUsageLocked(tenantID, usage)
}

func (machine *StateMachine) addOfflineUsageLocked(tenantID string, messages, byteCount int) {
	machine.offlineMessages += messages
	machine.offlineBytes += byteCount
	usage := machine.tenantUsage[tenantID]
	usage.queueMessages += messages
	usage.queueBytes += byteCount
	usage.storageBytes += byteCount
	machine.setTenantUsageLocked(tenantID, usage)
}

func (machine *StateMachine) addDeliveryMessageUsageLocked(tenantID string, messages, byteCount int) {
	machine.deliveryEntries += messages
	machine.deliveryBytes += byteCount
	usage := machine.tenantUsage[tenantID]
	usage.queueMessages += messages
	usage.queueBytes += byteCount
	usage.storageBytes += byteCount
	machine.setTenantUsageLocked(tenantID, usage)
}

func (machine *StateMachine) addDeliveryInboundUsageLocked(tenantID string, entries, byteCount int) {
	machine.deliveryEntries += entries
	machine.deliveryBytes += byteCount
	usage := machine.tenantUsage[tenantID]
	usage.storageBytes += byteCount
	machine.setTenantUsageLocked(tenantID, usage)
}

func (machine *StateMachine) setTenantUsageLocked(tenantID string, usage tenantStorageUsage) {
	if usage == (tenantStorageUsage{}) {
		delete(machine.tenantUsage, tenantID)
		return
	}
	if machine.tenantUsage == nil {
		machine.tenantUsage = make(map[string]tenantStorageUsage)
	}
	machine.tenantUsage[tenantID] = usage
}

func tenantQuotaObjectCount(profile *TenantQuotaProfileDTO) int {
	if profile == nil {
		return 0
	}
	return 1 + len(profile.Tenants)
}

func cloneTenantQuotaProfile(profile *TenantQuotaProfileDTO) *TenantQuotaProfileDTO {
	if profile == nil {
		return nil
	}
	cloned := *profile
	cloned.Tenants = append([]TenantStorageQuotaDTO(nil), profile.Tenants...)
	return &cloned
}

func tenantQuotaProfileTenantMap(profile *TenantQuotaProfileDTO) map[string]TenantStorageQuotaDTO {
	if profile == nil {
		return nil
	}
	byTenant := make(map[string]TenantStorageQuotaDTO, len(profile.Tenants))
	for _, tenant := range profile.Tenants {
		byTenant[tenant.TenantID] = tenant
	}
	return byTenant
}

// DefaultTenantQuotaProfile returns version zero with every dimension at its cluster-wide maximum.
func DefaultTenantQuotaProfile() TenantQuotaProfileDTO {
	return TenantQuotaProfileDTO{
		Default: TenantStorageQuotaDTO{
			RetainedBytes:        MaxTenantQuotaRetainedBytes,
			OfflineQueueMessages: MaxTenantQuotaOfflineQueueMessages,
			OfflineQueueBytes:    MaxTenantQuotaOfflineQueueBytes,
			StorageBytes:         MaxTenantQuotaStorageBytes,
		},
	}
}

// ValidateTenantQuotaProfile validates bounded limits and canonical tenant ordering.
func ValidateTenantQuotaProfile(profile TenantQuotaProfileDTO) error {
	if profile.Default.TenantID != "" || !validTenantQuotaLimits(profile.Default) || len(profile.Tenants) > MaxTenantQuotaTenants {
		return ErrInvalidCommand
	}
	for index, tenant := range profile.Tenants {
		if !boundedUTF8(tenant.TenantID, 1, maxTenantIDBytes) || !validTenantQuotaLimits(tenant) {
			return ErrInvalidCommand
		}
		if _, err := tenancy.NewTenantID(tenant.TenantID); err != nil {
			return ErrInvalidCommand
		}
		if index > 0 && profile.Tenants[index-1].TenantID >= tenant.TenantID {
			return ErrInvalidCommand
		}
	}
	return nil
}

func validTenantQuotaLimits(quota TenantStorageQuotaDTO) bool {
	return quota.RetainedBytes > 0 && quota.RetainedBytes <= MaxTenantQuotaRetainedBytes &&
		quota.OfflineQueueMessages > 0 && quota.OfflineQueueMessages <= MaxTenantQuotaOfflineQueueMessages &&
		quota.OfflineQueueBytes > 0 && quota.OfflineQueueBytes <= MaxTenantQuotaOfflineQueueBytes &&
		quota.StorageBytes > 0 && quota.StorageBytes <= MaxTenantQuotaStorageBytes
}

// EqualTenantQuotaProfile reports whether profiles have identical versioned contents.
func EqualTenantQuotaProfile(left, right TenantQuotaProfileDTO) bool {
	return left.Version == right.Version && left.Default == right.Default && slices.Equal(left.Tenants, right.Tenants)
}

func (machine *StateMachine) tenantQuotaExceededLocked(tenantID string, retainedDelta, queueMessagesDelta, queueBytesDelta, storageDelta int) *TenantQuotaRejectionDTO {
	quota := TenantStorageQuotaDTO{}
	var version uint64
	if policy := machine.clusterPolicy; policy != nil && policy.Policy.TenantQuotas != nil &&
		slices.Contains(machine.activeFeatures, ClusterPolicyTenantQuotasFeature) {
		quotas := policy.Policy.TenantQuotas
		quota = quotas.Default
		if tenantQuota, exists := machine.clusterPolicyTenantQuotaByTenant[tenantID]; exists {
			quota = tenantQuota
		}
		version = policy.Version
	} else {
		if machine.tenantQuota == nil {
			return nil
		}
		quota = machine.tenantQuota.Default
		if tenantQuota, exists := machine.tenantQuotaByTenant[tenantID]; exists {
			quota = tenantQuota
		}
		version = machine.tenantQuota.Version
	}
	usage := machine.tenantUsage[tenantID]
	if retainedDelta > 0 && usage.retainedBytes+retainedDelta > int(quota.RetainedBytes) {
		return &TenantQuotaRejectionDTO{TenantID: tenantID, Dimension: TenantQuotaRetainedBytes, ProfileVersion: version}
	}
	if queueMessagesDelta > 0 && usage.queueMessages+queueMessagesDelta > int(quota.OfflineQueueMessages) {
		return &TenantQuotaRejectionDTO{TenantID: tenantID, Dimension: TenantQuotaOfflineQueueMessages, ProfileVersion: version}
	}
	if queueBytesDelta > 0 && usage.queueBytes+queueBytesDelta > int(quota.OfflineQueueBytes) {
		return &TenantQuotaRejectionDTO{TenantID: tenantID, Dimension: TenantQuotaOfflineQueueBytes, ProfileVersion: version}
	}
	if storageDelta > 0 && usage.storageBytes+storageDelta > int(quota.StorageBytes) {
		return &TenantQuotaRejectionDTO{TenantID: tenantID, Dimension: TenantQuotaStorageBytes, ProfileVersion: version}
	}
	return nil
}

func (machine *StateMachine) applyTenantQuota(index uint64, change TenantQuotaChangeDTO) ApplyResultDTO {
	if machine.tenantQuota == nil || change.Profile.Version > machine.tenantQuota.Version {
		machine.tenantQuota = cloneTenantQuotaProfile(&change.Profile)
		machine.tenantQuotaByTenant = tenantQuotaProfileTenantMap(machine.tenantQuota)
		return ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	}
	if change.Profile.Version == machine.tenantQuota.Version && EqualTenantQuotaProfile(change.Profile, *machine.tenantQuota) {
		return ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	}
	return conflictResult(index, ConflictTenantQuotaStale)
}

// TenantQuotaProfile returns the committed quota profile as an owned copy.
func (machine *StateMachine) TenantQuotaProfile() (TenantQuotaProfileDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	if machine.tenantQuota == nil {
		return TenantQuotaProfileDTO{}, false
	}
	return *cloneTenantQuotaProfile(machine.tenantQuota), true
}

// TenantQuotaProfileVersion returns the committed quota profile version.
func (machine *StateMachine) TenantQuotaProfileVersion() (uint64, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	if machine.tenantQuota == nil {
		return 0, false
	}
	return machine.tenantQuota.Version, true
}

func sameTenantQuotaProfile(left, right *TenantQuotaProfileDTO) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return EqualTenantQuotaProfile(*left, *right)
}

func validateSnapshotTenantQuotaProfile(profile *TenantQuotaProfileDTO) error {
	if profile != nil && ValidateTenantQuotaProfile(*profile) != nil {
		return ErrInvalidSnapshot
	}
	return nil
}

func installTenantQuotaProfile(profile *TenantQuotaProfileDTO) (*TenantQuotaProfileDTO, map[string]TenantStorageQuotaDTO) {
	cloned := cloneTenantQuotaProfile(profile)
	return cloned, tenantQuotaProfileTenantMap(cloned)
}

func validateTenantQuotaCommand(change TenantQuotaChangeDTO) error {
	return ValidateTenantQuotaProfile(change.Profile)
}
