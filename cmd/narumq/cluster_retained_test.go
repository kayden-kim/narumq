package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/application/membership"
	applicationretained "narumq/internal/application/ports/retained"
	"narumq/internal/application/publish"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

// Traceability: PRD CLU-08/CLU-14/TEN-05; docs/specs/cluster-session-and-delivery.md §4·§10.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-056
func TestClusterRetainedCommitErrorMapsAppliedCapacityAndConflict(t *testing.T) {
	passthrough := errors.New("commit transport failed")
	for _, test := range []struct {
		name   string
		result membership.ApplyResultDTO
		err    error
		want   error
		reason string
	}{
		{name: "applied", result: membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeApplied}},
		{
			name: "capacity",
			result: membership.ApplyResultDTO{
				Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictRetainedCapacity,
			},
			want:   applicationretained.ErrFull,
			reason: "server_backpressure:retained_capacity",
		},
		{
			name: "tenant quota capacity",
			result: membership.ApplyResultDTO{
				Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictRetainedCapacity,
				TenantQuota: &membership.TenantQuotaRejectionDTO{},
			},
			want:   applicationretained.ErrFull,
			reason: "tenant_limit:tenant_quota",
		},
		{
			name: "other conflict",
			result: membership.ApplyResultDTO{
				Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictStaleEpoch,
			},
			want: membership.ErrQuorumUnavailable,
		},
		{name: "error passthrough", err: passthrough, want: passthrough},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := clusterRetainedCommitError(test.result, test.err)
			if test.want == nil && err != nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("clusterRetainedCommitError() = %v, want %v", err, test.want)
			}
			if got := publish.RejectionReason(err); got != test.reason {
				t.Fatalf("RejectionReason() = %q, want %q", got, test.reason)
			}
		})
	}
}

// Traceability: PRD CLU-08/CLU-14/TEN-05; docs/specs/cluster-session-and-delivery.md §4·§10.
func TestClusterRetainedMessageRestoresTenantPublisherAndExpiry(t *testing.T) {
	expiresAt := time.Date(2026, time.January, 1, 0, 0, 30, 0, time.UTC)
	message, err := clusterRetainedMessage(membership.RetainedStateDTO{
		TenantID: "tenant-a", Topic: "events/state", PublisherClientID: "publisher",
		QoS: 1, Payload: []byte("value"), ExpiresAtUnixMillis: uint64(expiresAt.UnixMilli()),
	})
	if err != nil {
		t.Fatal(err)
	}
	key, err := tenancy.ParseSessionKeyRoutingID(string(message.PublisherSessionID))
	if err != nil {
		t.Fatalf("parse publisher session: %v", err)
	}
	if key.TenantID() != message.TenantID || key.ClientID() != "publisher" || message.Topic != "events/state" ||
		message.QoS != mqtt.QoSAtLeastOnce || string(message.Payload) != "value" || !message.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("restored retained message = %+v, publisher key = %+v", message, key)
	}
	if _, err := clusterRetainedMessage(membership.RetainedStateDTO{TenantID: "", PublisherClientID: "publisher"}); err == nil {
		t.Fatal("clusterRetainedMessage accepted an invalid tenant")
	}
	if _, err := clusterRetainedMessage(membership.RetainedStateDTO{TenantID: "tenant-a", PublisherClientID: ""}); err == nil {
		t.Fatal("clusterRetainedMessage accepted an invalid publisher")
	}
}

// Traceability: PRD CLU-08/CLU-14/TEN-05; docs/specs/cluster-session-and-delivery.md §4·§10.
func TestClusterRetainedCommitRejectsNonClusterPublisherAndOversizePayload(t *testing.T) {
	runtime := &clusterRuntime{}
	change := publish.RetainedChangeDTO{Message: applicationretained.MessageDTO{
		TenantID: mustTenantForClusterTest(t, "tenant-a"), PublisherSessionID: routing.SessionID("plain-client"),
	}}
	if err := runtime.CommitRetained(context.Background(), change); !errors.Is(err, errClusterRetainedPublisher) {
		t.Fatalf("non-cluster publisher error = %v, want %v", err, errClusterRetainedPublisher)
	}
	sessionKey, err := tenancy.NewSessionKey(change.Message.TenantID, "publisher")
	if err != nil {
		t.Fatal(err)
	}
	change.Message.PublisherSessionID = routing.SessionID(sessionKey.RoutingID())
	change.Message.Payload = make([]byte, membership.MaxRetainedPayloadBytes+1)
	if err := runtime.CommitRetained(context.Background(), change); !errors.Is(err, applicationretained.ErrFull) {
		t.Fatalf("oversize payload error = %v, want %v", err, applicationretained.ErrFull)
	}
}

// Traceability: PRD CLU-08/CLU-14/TEN-05; docs/specs/cluster-session-and-delivery.md §4·§10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-038
func TestClusterRetainedProjectionRebuildsIncrementallyAndDeletes(t *testing.T) {
	runtime := newClusterRetainedProjectionTestRuntime(t, membership.MaxRetainedMessages)
	first := clusterRetainedChangeForTest("tenant-a", "events/a", "publisher", "alpha", membership.RetainedActionReplace)
	applyClusterRetainedForTest(t, runtime.machine, 1, first)
	runtime.retainedResyncRequired.Store(true)
	runtime.projectionMu.Lock()
	err := runtime.rebuildRetainedProjectionLocked()
	runtime.projectionMu.Unlock()
	if err != nil {
		t.Fatalf("rebuild retained projection: %v", err)
	}
	if runtime.retainedResyncRequired.Load() {
		t.Fatal("successful retained rebuild left resync required")
	}
	assertClusterRetainedProjection(t, runtime, []string{"tenant-a/events/a=alpha"})

	replacement := clusterRetainedChangeForTest("tenant-a", "events/a", "publisher", "beta", membership.RetainedActionReplace)
	applyClusterRetainedForTest(t, runtime.machine, 2, replacement)
	runtime.syncRetainedProjection(replacement)
	assertClusterRetainedProjection(t, runtime, []string{"tenant-a/events/a=beta"})

	deletion := clusterRetainedChangeForTest("tenant-a", "events/a", "publisher", "", membership.RetainedActionDelete)
	applyClusterRetainedForTest(t, runtime.machine, 3, deletion)
	runtime.syncRetainedProjection(deletion)
	assertClusterRetainedProjection(t, runtime, []string{})
}

// Traceability: PRD CLU-08/CLU-14/TEN-05; docs/specs/cluster-session-and-delivery.md §4·§10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-038
func TestClusterRetainedProjectionRebuildsAfterIncrementalCapacityFailure(t *testing.T) {
	runtime := newClusterRetainedProjectionTestRuntime(t, 1)
	staleTenant := mustTenantForClusterTest(t, "stale-tenant")
	if err := runtime.retained.Replace(applicationretained.MessageDTO{
		TenantID: staleTenant, PublisherSessionID: "stale", Topic: "old", Payload: []byte("old"),
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	change := clusterRetainedChangeForTest("tenant-a", "events/a", "publisher", "current", membership.RetainedActionReplace)
	applyClusterRetainedForTest(t, runtime.machine, 1, change)
	runtime.syncRetainedProjection(change)
	assertClusterRetainedProjection(t, runtime, []string{"tenant-a/events/a=current"})
	if runtime.retainedResyncRequired.Load() || runtime.projectionFailed.Load() {
		t.Fatal("successful full rebuild after incremental capacity failure left projection unhealthy")
	}
}

// Traceability: PRD CLU-08/CLU-14/TEN-05; docs/specs/cluster-session-and-delivery.md §4·§10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-038
func TestClusterRetainedProjectionFailurePreservesCacheAndLogsOnce(t *testing.T) {
	runtime := newClusterRetainedProjectionTestRuntime(t, 1)
	staleTenant := mustTenantForClusterTest(t, "stale-tenant")
	if err := runtime.retained.Replace(applicationretained.MessageDTO{
		TenantID: staleTenant, PublisherSessionID: "stale", Topic: "old", Payload: []byte("old"),
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	first := clusterRetainedChangeForTest("tenant-a", "events/a", "publisher", "first", membership.RetainedActionReplace)
	second := clusterRetainedChangeForTest("tenant-a", "events/b", "publisher", "second", membership.RetainedActionReplace)
	applyClusterRetainedForTest(t, runtime.machine, 1, first)
	applyClusterRetainedForTest(t, runtime.machine, 2, second)

	var logs strings.Builder
	runtime.logger = slog.New(slog.NewTextHandler(&logs, nil))
	runtime.syncRetainedProjection(first)
	runtime.syncRetainedProjection(second)
	if !runtime.retainedResyncRequired.Load() || !runtime.projectionFailed.Load() {
		t.Fatal("failed full retained rebuild did not mark projection unhealthy")
	}
	assertClusterRetainedProjection(t, runtime, []string{"stale-tenant/old=old"})
	if got := strings.Count(logs.String(), "cluster retained projection rebuild failed"); got != 1 {
		t.Fatalf("retained projection failure log count = %d, want 1: %s", got, logs.String())
	}
}

func newClusterRetainedProjectionTestRuntime(t *testing.T, maxMessages int) *clusterRuntime {
	t.Helper()
	repository, err := memoryretained.New(maxMessages, membership.MaxRetainedBytes)
	if err != nil {
		t.Fatal(err)
	}
	return &clusterRuntime{
		machine: membership.NewStateMachine(), retained: repository,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func clusterRetainedChangeForTest(tenantID, topic, publisher, payload string, action membership.RetainedAction) membership.RetainedChangeDTO {
	change := membership.RetainedChangeDTO{
		TenantID: tenantID, Topic: topic, PublisherClientID: publisher,
		ProposedAtUnixMillis: 1, Action: action,
	}
	if action == membership.RetainedActionReplace {
		change.Payload = []byte(payload)
		change.QoS = 1
	}
	return change
}

func applyClusterRetainedForTest(t *testing.T, machine *membership.StateMachine, index uint64, change membership.RetainedChangeDTO) {
	t.Helper()
	operationID := fmt.Sprintf("00000000-0000-4000-8000-%012d", index)
	result, err := machine.Apply(index, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: operationID, Retained: &change,
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("apply retained index %d = %+v, %v", index, result, err)
	}
}

func assertClusterRetainedProjection(t *testing.T, runtime *clusterRuntime, want []string) {
	t.Helper()
	messages, err := runtime.retained.List(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(messages))
	for index, message := range messages {
		got[index] = fmt.Sprintf("%s/%s=%s", message.TenantID, message.Topic, message.Payload)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("retained projection = %v, want %v", got, want)
	}
}

func mustTenantForClusterTest(t *testing.T, raw string) tenancy.TenantID {
	t.Helper()
	tenantID, err := tenancy.NewTenantID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return tenantID
}
