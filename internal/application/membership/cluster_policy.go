package membership

import (
	"slices"
)

const maxClusterPolicyPacketSize uint32 = 268435455

// ValidateClusterPolicy validates the MQTT limits and optional tenant quota values.
func ValidateClusterPolicy(policy ClusterPolicyDTO) error {
	limits := policy.MQTTLimits
	if limits.MaxPacketSize == 0 || limits.MaxPacketSize > maxClusterPolicyPacketSize ||
		limits.ReceiveMaximum == 0 || limits.ReceiveMaximum > 65535 ||
		limits.ConnectTimeoutMillis == 0 || limits.PacketReadTimeoutMillis == 0 || limits.WriteTimeoutMillis == 0 {
		return ErrInvalidCommand
	}
	if policy.TenantQuotas != nil && ValidateTenantQuotaProfile(TenantQuotaProfileDTO{
		Default: policy.TenantQuotas.Default, Tenants: policy.TenantQuotas.Tenants,
	}) != nil {
		return ErrInvalidCommand
	}
	return nil
}

// EqualClusterPolicy reports whether two policies have identical committed values.
func EqualClusterPolicy(left, right ClusterPolicyDTO) bool {
	if left.MQTTLimits != right.MQTTLimits {
		return false
	}
	if left.TenantQuotas == nil || right.TenantQuotas == nil {
		return left.TenantQuotas == nil && right.TenantQuotas == nil
	}
	return left.TenantQuotas.Default == right.TenantQuotas.Default &&
		slices.Equal(left.TenantQuotas.Tenants, right.TenantQuotas.Tenants)
}

func cloneClusterTenantQuotas(quotas *ClusterTenantQuotasDTO) *ClusterTenantQuotasDTO {
	if quotas == nil {
		return nil
	}
	cloned := *quotas
	cloned.Tenants = slices.Clone(quotas.Tenants)
	return &cloned
}

func clusterTenantQuotaMap(quotas *ClusterTenantQuotasDTO) map[string]TenantStorageQuotaDTO {
	if quotas == nil {
		return nil
	}
	byTenant := make(map[string]TenantStorageQuotaDTO, len(quotas.Tenants))
	for _, tenant := range quotas.Tenants {
		byTenant[tenant.TenantID] = tenant
	}
	return byTenant
}

func clusterPolicyObjectCount(state *ClusterPolicyStateDTO) int {
	if state == nil {
		return 0
	}
	count := 1
	if state.Policy.TenantQuotas != nil {
		count += 1 + len(state.Policy.TenantQuotas.Tenants)
	}
	return count
}

func validateClusterPolicyChange(change ClusterPolicyChangeDTO) error {
	if change.Version == 0 || change.Version <= change.ExpectedVersion ||
		ValidateClusterPolicy(change.Policy) != nil ||
		(change.IdempotencyKey != "" && !validIdempotencyKey(change.IdempotencyKey)) {
		return ErrInvalidCommand
	}
	return nil
}

func validateClusterPolicyState(state ClusterPolicyStateDTO) error {
	if state.Version == 0 || state.PreviousVersion >= state.Version ||
		ValidateClusterPolicy(state.Policy) != nil ||
		(state.IdempotencyKey != "" && !validIdempotencyKey(state.IdempotencyKey)) {
		return ErrInvalidSnapshot
	}
	return nil
}

func cloneClusterPolicyState(state *ClusterPolicyStateDTO) *ClusterPolicyStateDTO {
	if state == nil {
		return nil
	}
	cloned := *state
	cloned.Policy.TenantQuotas = cloneClusterTenantQuotas(state.Policy.TenantQuotas)
	return &cloned
}

func (machine *StateMachine) applyClusterPolicy(index uint64, change ClusterPolicyChangeDTO) ApplyResultDTO {
	if !slices.Contains(machine.activeFeatures, ClusterPolicyFeature) ||
		(change.Policy.TenantQuotas != nil && !slices.Contains(machine.activeFeatures, ClusterPolicyTenantQuotasFeature)) {
		return conflictResult(index, ConflictFeatureInactive)
	}

	currentVersion := uint64(0)
	if current := machine.clusterPolicy; current != nil {
		if change.IdempotencyKey != "" && change.IdempotencyKey == current.IdempotencyKey {
			if change.ExpectedVersion == current.PreviousVersion && change.Version == current.Version &&
				EqualClusterPolicy(change.Policy, current.Policy) {
				return ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
			}
			return conflictResult(index, ConflictIdempotency)
		}
		currentVersion = current.Version
	}
	if change.ExpectedVersion != currentVersion {
		return conflictResult(index, ConflictClusterPolicyVersion)
	}
	machine.clusterPolicy = &ClusterPolicyStateDTO{
		Version: change.Version, Policy: ClusterPolicyDTO{
			MQTTLimits: change.Policy.MQTTLimits, TenantQuotas: cloneClusterTenantQuotas(change.Policy.TenantQuotas),
		},
		IdempotencyKey: change.IdempotencyKey, PreviousVersion: change.ExpectedVersion,
	}
	machine.clusterPolicyTenantQuotaByTenant = clusterTenantQuotaMap(change.Policy.TenantQuotas)
	return ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
}

// ClusterPolicy returns the latest committed policy as an owned value copy.
func (machine *StateMachine) ClusterPolicy() (ClusterPolicyStateDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	if machine.clusterPolicy == nil {
		return ClusterPolicyStateDTO{}, false
	}
	return *cloneClusterPolicyState(machine.clusterPolicy), true
}

// EffectiveTenantQuotaProfile returns the applied profile and its replicated source.
func (machine *StateMachine) EffectiveTenantQuotaProfile() (TenantQuotaProfileDTO, string, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	if policy := machine.clusterPolicy; policy != nil && policy.Policy.TenantQuotas != nil &&
		slices.Contains(machine.activeFeatures, ClusterPolicyTenantQuotasFeature) {
		quotas := policy.Policy.TenantQuotas
		return TenantQuotaProfileDTO{
			Version: policy.Version, Default: quotas.Default, Tenants: slices.Clone(quotas.Tenants),
		}, "clusterPolicy", true
	}
	if machine.tenantQuota == nil {
		return TenantQuotaProfileDTO{}, "none", false
	}
	return *cloneTenantQuotaProfile(machine.tenantQuota), "replicatedProfile", true
}
