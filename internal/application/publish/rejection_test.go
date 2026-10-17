package publish

import (
	"errors"
	"fmt"
	"testing"

	"narumq/internal/application/membership"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/retained"
)

// Traceability: PRD MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.2.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-056
func TestRejectionReasonFormatsClassAndCause(t *testing.T) {
	tenantQuota := NewRejectionError(ErrDurableDeliveryCapacity, RejectionClassTenantLimit, RejectionCauseTenantQuota)
	for _, testCase := range []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"tenant-quota", tenantQuota, "tenant_limit:tenant_quota"},
		{"wrapped", fmt.Errorf("publish: %w", tenantQuota), "tenant_limit:tenant_quota"},
		{"commit-lag", NewRejectionError(ErrPublishBackpressure, RejectionClassServerBackpressure, RejectionCauseCommitLag), "server_backpressure:commit_lag"},
		{"bare-retained-full", retained.ErrFull, "server_backpressure:retained_capacity"},
		{"tenant-retained-full", NewRejectionError(retained.ErrFull, RejectionClassTenantLimit, RejectionCauseTenantQuota), "tenant_limit:tenant_quota"},
		{"unknown-backpressure", ErrPublishBackpressure, ""},
		{"unknown-capacity", ErrDurableDeliveryCapacity, ""},
		{"other", errors.New("boom"), ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := RejectionReason(testCase.err); got != testCase.want {
				t.Fatalf("RejectionReason() = %q, want %q", got, testCase.want)
			}
		})
	}
	if !errors.Is(tenantQuota, ErrDurableDeliveryCapacity) {
		t.Fatal("RejectionError does not unwrap to the wrapped error")
	}
}

// Traceability: PRD MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.2.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-056
func TestDurableDeliveryResultClassifiesCapacityRefusals(t *testing.T) {
	delivery := &membership.DeliveryResultDTO{}
	for _, testCase := range []struct {
		name      string
		committed membership.ApplyResultDTO
		err       error
		wantErr   error
		want      string
	}{
		{name: "queue-full", err: deliveryqueue.ErrFull, wantErr: ErrDurableDeliveryCapacity, want: "server_backpressure:receive_capacity"},
		{name: "global-capacity-conflict", committed: membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictDeliveryCapacity}, wantErr: ErrDurableDeliveryCapacity, want: "server_backpressure:delivery_capacity"},
		{name: "tenant-quota-conflict", committed: membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictDeliveryCapacity, TenantQuota: &membership.TenantQuotaRejectionDTO{}}, wantErr: ErrDurableDeliveryCapacity, want: "tenant_limit:tenant_quota"},
		{name: "other-conflict", committed: membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictRetainedCapacity}, wantErr: ErrDurableDeliveryRejected, want: ""},
		{name: "other-error", err: errors.New("raft unavailable"), want: ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := durableDeliveryResult(testCase.committed, testCase.err)
			if err == nil {
				t.Fatal("durableDeliveryResult() returned no error")
			}
			if testCase.wantErr != nil && !errors.Is(err, testCase.wantErr) {
				t.Fatalf("durableDeliveryResult() error = %v, want %v", err, testCase.wantErr)
			}
			if got := RejectionReason(err); got != testCase.want {
				t.Fatalf("RejectionReason() = %q, want %q", got, testCase.want)
			}
		})
	}
	_, err := durableDeliveryResult(membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeApplied, Delivery: delivery}, nil)
	if err != nil {
		t.Fatalf("durableDeliveryResult(applied) error = %v", err)
	}
}
