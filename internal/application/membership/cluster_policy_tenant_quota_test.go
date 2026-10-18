package membership

import (
	"slices"
	"testing"
)

// Traceability: CFG-SCN-37; docs/specs/configuration-and-runtime-options.md §10; ADR-0052 §1.1.1.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-37
func TestClusterPolicyTenantQuotaActivationRejectsUnsupportedStagedMember(t *testing.T) {
	machine := releaseProfileMachine(t, "1.2.0", []string{ClusterPolicyFeature, ClusterPolicyTenantQuotasFeature})
	if result := activateFeatureForTest(t, machine, 2, ClusterPolicyFeature); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("activate cluster-policy = %+v", result)
	}
	staged := stageRecord(testOperation4, "join-4", 4)
	staged.Member.ReleaseVersion = "1.2.0"
	if result := applyMustSucceed(t, machine, 3, testCommand(testOperation4, &staged)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("stage member without tenant quota support = %+v", result)
	}
	result := activateFeatureForTest(t, machine, 4, ClusterPolicyTenantQuotasFeature)
	if result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictFeatureUnsupported ||
		slices.Contains(machine.ActiveFeatures(), ClusterPolicyTenantQuotasFeature) {
		t.Fatalf("activate feature with unsupported staged member = %+v active=%q; want feature_not_supported_by_all_members", result, machine.ActiveFeatures())
	}
}

// Traceability: CFG-SCN-34~36; docs/contracts/replicated-state-v1.md §3; ADR-0052 §1.1.1.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-34, configuration-and-runtime-options.md:CFG-SCN-36
func TestClusterPolicyTenantQuotaFeatureGateAndEffectiveProfile(t *testing.T) {
	inactive := clusterPolicyTestMachine(t)
	policy := clusterPolicyTestValue(64)
	policy.TenantQuotas = &ClusterTenantQuotasDTO{
		Default: DefaultTenantQuotaProfile().Default,
		Tenants: []TenantStorageQuotaDTO{tenantQuotaForTest("tenant-a", 100, 100, 100, 100)},
	}
	result, err := inactive.Apply(3, clusterPolicyTestCommand(3, 0, 1, policy, "policy-1"))
	if err != nil || result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictFeatureInactive {
		t.Fatalf("apply policy quotas without feature = (%+v, %v), want feature_inactive", result, err)
	}
	if _, found := inactive.ClusterPolicy(); found {
		t.Fatal("feature-inactive policy partially changed committed state")
	}
	if profile, source, found := inactive.EffectiveTenantQuotaProfile(); found || source != "none" || !EqualTenantQuotaProfile(profile, TenantQuotaProfileDTO{}) {
		t.Fatalf("effective profile without committed quota = (%+v, %q, %t), want empty none", profile, source, found)
	}

	machine := releaseProfileMachine(t, "1.2.0", []string{ClusterPolicyFeature, ClusterPolicyTenantQuotasFeature})
	if result := activateFeatureForTest(t, machine, 2, ClusterPolicyFeature); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("activate cluster-policy = %+v", result)
	}
	if result := activateFeatureForTest(t, machine, 3, ClusterPolicyTenantQuotasFeature); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("activate cluster-policy-tenant-quotas = %+v", result)
	}

	legacyQuota := tenantQuotaProfileForTest(7, tenantQuotaForTest("tenant-a", 4096, 4096, 4096, 4096))
	if result := applyTenantQuotaForTest(t, machine, 4, legacyQuota); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("apply legacy profile = %+v", result)
	}
	if result, err := machine.Apply(5, clusterPolicyTestCommand(5, 0, 1, policy, "policy-1")); err != nil || result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("apply policy quota = (%+v, %v)", result, err)
	}

	effective, source, found := machine.EffectiveTenantQuotaProfile()
	wantEffective := TenantQuotaProfileDTO{
		Version: 1, Default: policy.TenantQuotas.Default, Tenants: policy.TenantQuotas.Tenants,
	}
	if !found || source != "clusterPolicy" || !EqualTenantQuotaProfile(effective, wantEffective) {
		t.Fatalf("effective profile = (%+v, %q, %t), want policy profile %+v", effective, source, found, wantEffective)
	}
	stored, found := machine.ClusterPolicy()
	if !found || stored.Policy.TenantQuotas == nil {
		t.Fatalf("committed policy = (%+v, %t), want tenant quotas", stored, found)
	}
	stored.Policy.TenantQuotas.Tenants[0].RetainedBytes = 999
	effective.Tenants[0].RetainedBytes = 999
	storedAgain, _ := machine.ClusterPolicy()
	effectiveAgain, _, _ := machine.EffectiveTenantQuotaProfile()
	if storedAgain.Policy.TenantQuotas.Tenants[0].RetainedBytes != 100 || effectiveAgain.Tenants[0].RetainedBytes != 100 {
		t.Fatal("mutating a returned policy or effective profile changed committed quota state")
	}

	rejected := applyRetainedForTest(t, machine, 6, retainedReplaceForTest("tenant-a", "large", "publisher", make([]byte, 256), 0, 0, 1000))
	if rejected.Outcome != ApplyOutcomeConflict || rejected.Conflict != ConflictRetainedCapacity || rejected.TenantQuota == nil || rejected.TenantQuota.ProfileVersion != 1 {
		t.Fatalf("policy quota rejection = %+v, want policy version 1", rejected)
	}

	snapshot, err := machine.Snapshot(6)
	if err != nil {
		t.Fatalf("snapshot with policy quotas: %v", err)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore policy quota snapshot: %v", err)
	}
	restoredProfile, restoredSource, restoredFound := restored.EffectiveTenantQuotaProfile()
	if !restoredFound || restoredSource != "clusterPolicy" || !EqualTenantQuotaProfile(restoredProfile, wantEffective) {
		t.Fatalf("restored effective profile = (%+v, %q, %t), want policy profile", restoredProfile, restoredSource, restoredFound)
	}
	restoredSnapshot, err := restored.Snapshot(6)
	if err != nil || !sameSnapshotState(snapshot, restoredSnapshot) {
		t.Fatalf("restored snapshot = (%+v, %v), want original replicated state", restoredSnapshot, err)
	}

	withoutPolicyQuota := clusterPolicyTestValue(64)
	if result, err := restored.Apply(7, clusterPolicyTestCommand(7, 1, 2, withoutPolicyQuota, "policy-2")); err != nil || result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("remove policy quota = (%+v, %v)", result, err)
	}
	fallback, fallbackSource, fallbackFound := restored.EffectiveTenantQuotaProfile()
	if !fallbackFound || fallbackSource != "replicatedProfile" || !EqualTenantQuotaProfile(fallback, legacyQuota) {
		t.Fatalf("fallback profile = (%+v, %q, %t), want legacy profile %+v", fallback, fallbackSource, fallbackFound, legacyQuota)
	}
	if result := applyRetainedForTest(t, restored, 8, retainedReplaceForTest("tenant-a", "large", "publisher", make([]byte, 256), 0, 0, 1000)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("legacy profile did not take effect after policy quota removal: %+v", result)
	}
}
