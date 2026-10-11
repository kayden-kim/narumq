package membership

import (
	"context"
	"errors"
	"testing"
)

func clusterPolicyServiceFixture(t *testing.T, active bool) (*ClusterPolicyService, *stateMachinePort) {
	t.Helper()
	features := []string(nil)
	if active {
		features = []string{ClusterPolicyFeature}
	}
	machine := releaseProfileMachine(t, "1.2.0", features)
	if active {
		if result := activateFeatureForTest(t, machine, 2, ClusterPolicyFeature); result.Outcome != ApplyOutcomeApplied {
			t.Fatalf("activate ClusterPolicy feature = %+v", result)
		}
	}
	port := &stateMachinePort{machine: machine, nextIndex: machine.LastAppliedIndex()}
	service, err := NewClusterPolicyService(port)
	if err != nil {
		t.Fatalf("NewClusterPolicyService() error = %v", err)
	}
	return service, port
}

func assertClusterPolicyServiceError(t *testing.T, err error, code ServiceErrorCode, retryable bool) {
	t.Helper()
	var serviceError *ServiceError
	if !errors.As(err, &serviceError) || serviceError.Code != code || serviceError.Retryable != retryable {
		t.Fatalf("error = %v, want %s retryable=%v", err, code, retryable)
	}
}

// Traceability: API-09; docs/contracts/management-api-v1.md §1, §4; ADR-0052 §1.1.1.
func TestClusterPolicyServiceUpdateReplayConflictAndRead(t *testing.T) {
	service, port := clusterPolicyServiceFixture(t, true)
	ctx := context.Background()
	if state, found, err := service.GetClusterPolicy(ctx); err != nil || found || state != (ClusterPolicyStateDTO{}) {
		t.Fatalf("unconfigured policy = (%+v, %t, %v), want empty state", state, found, err)
	}

	request := UpdateClusterPolicyRequestDTO{
		ExpectedVersion: 0, IdempotencyKey: "policy-1", Policy: clusterPolicyTestValue(64),
	}
	first, err := service.UpdateClusterPolicy(ctx, request)
	want := ClusterPolicyStateDTO{
		Version: 1, Policy: request.Policy, IdempotencyKey: request.IdempotencyKey, PreviousVersion: 0,
	}
	if err != nil || first != want {
		t.Fatalf("initial update = (%+v, %v), want %+v", first, err, want)
	}
	replay, err := service.UpdateClusterPolicy(ctx, request)
	if err != nil || replay != want || port.commitCalls != 2 {
		t.Fatalf("same-key replay = (%+v, %v), commits=%d; want same state and a committed replay", replay, err, port.commitCalls)
	}

	changed := request
	changed.Policy = clusterPolicyTestValue(32)
	_, err = service.UpdateClusterPolicy(ctx, changed)
	assertClusterPolicyServiceError(t, err, ServiceErrorIdempotencyConflict, false)

	stale := request
	stale.IdempotencyKey = "policy-old"
	_, err = service.UpdateClusterPolicy(ctx, stale)
	assertClusterPolicyServiceError(t, err, ServiceErrorClusterPolicyVersionConflict, false)

	state, found, err := service.GetClusterPolicy(ctx)
	if err != nil || !found || state != want {
		t.Fatalf("committed policy read = (%+v, %t, %v), want %+v", state, found, err, want)
	}
}

// Traceability: API-09; docs/contracts/management-api-v1.md §1, §4; ADR-0052 §1.1.1.
func TestClusterPolicyServiceRejectsInvalidAndInactiveUpdates(t *testing.T) {
	service, port := clusterPolicyServiceFixture(t, true)
	valid := UpdateClusterPolicyRequestDTO{
		ExpectedVersion: 0, IdempotencyKey: "policy-1", Policy: clusterPolicyTestValue(64),
	}
	invalid := []struct {
		name    string
		request UpdateClusterPolicyRequestDTO
	}{
		{name: "missing key", request: func() UpdateClusterPolicyRequestDTO { r := valid; r.IdempotencyKey = ""; return r }()},
		{name: "invalid policy", request: func() UpdateClusterPolicyRequestDTO { r := valid; r.Policy.MQTTLimits.MaxPacketSize = 0; return r }()},
		{name: "version overflow", request: func() UpdateClusterPolicyRequestDTO { r := valid; r.ExpectedVersion = ^uint64(0); return r }()},
	}
	for _, testCase := range invalid {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := service.UpdateClusterPolicy(context.Background(), testCase.request)
			assertClusterPolicyServiceError(t, err, ServiceErrorInvalidRequest, false)
		})
	}
	if port.commitCalls != 0 {
		t.Fatalf("invalid updates committed %d times, want zero", port.commitCalls)
	}
	quotaRequest := valid
	quotaRequest.Policy.TenantQuotas = &ClusterTenantQuotasDTO{
		Default: DefaultTenantQuotaProfile().Default,
		Tenants: []TenantStorageQuotaDTO{tenantQuotaForTest("tenant-a", 100, 100, 100, 100)},
	}
	_, err := service.UpdateClusterPolicy(context.Background(), quotaRequest)
	assertClusterPolicyServiceError(t, err, ServiceErrorFeatureInactive, false)
	if port.commitCalls != 0 {
		t.Fatalf("update using inactive tenant quota feature committed %d times, want zero", port.commitCalls)
	}

	inactive, inactivePort := clusterPolicyServiceFixture(t, false)
	_, err = inactive.UpdateClusterPolicy(context.Background(), valid)
	assertClusterPolicyServiceError(t, err, ServiceErrorFeatureInactive, false)
	if inactivePort.commitCalls != 0 {
		t.Fatalf("inactive feature update committed %d times, want zero", inactivePort.commitCalls)
	}
}

func TestClusterPolicyServiceMapsUnavailablePort(t *testing.T) {
	service, port := clusterPolicyServiceFixture(t, true)
	port.commitError = ErrNotLeader
	_, err := service.UpdateClusterPolicy(context.Background(), UpdateClusterPolicyRequestDTO{
		ExpectedVersion: 0, IdempotencyKey: "policy-1", Policy: clusterPolicyTestValue(64),
	})
	assertClusterPolicyServiceError(t, err, ServiceErrorNotLeader, true)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := service.GetClusterPolicy(ctx); err == nil {
		t.Fatal("GetClusterPolicy() accepted a cancelled context")
	} else {
		assertClusterPolicyServiceError(t, err, ServiceErrorQuorumUnavailable, true)
	}
}
