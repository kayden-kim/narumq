package rollout

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

var errStatusWriteLost = errors.New("status write lost before it reached the API server")

// lossyStatusStore loses exactly the failAt-th status write (1-based; 0 never loses one) and
// can run a hook right after the first successful write.
type lossyStatusStore struct {
	inner      StatusStore
	failAt     int
	calls      int
	afterWrite func()
}

func (store *lossyStatusStore) CompareAndSwapStatus(ctx context.Context, rollout RolloutDTO, expected string, status StatusDTO) (string, error) {
	store.calls++
	if store.calls == store.failAt {
		return "", errStatusWriteLost
	}
	resourceVersion, err := store.inner.CompareAndSwapStatus(ctx, rollout, expected, status)
	if err == nil && store.afterWrite != nil {
		hook := store.afterWrite
		store.afterWrite = nil
		hook()
	}
	return resourceVersion, err
}

func useLossyStatusStore(harness *rolloutTestHarness, failAt int) *lossyStatusStore {
	store := &lossyStatusStore{inner: harness.status, failAt: failAt}
	harness.service.status = store
	return store
}

func reconcileWithContext(ctx context.Context, harness *rolloutTestHarness) (ReconcileResultDTO, error) {
	harness.request.Status = harness.status.status
	harness.request.ResourceVersion = harness.status.resourceVersion
	return harness.service.Reconcile(ctx, harness.request)
}

func requireStatusWriteLost(t *testing.T, harness *rolloutTestHarness) {
	t.Helper()
	_, err := harness.reconcile(t)
	if !errors.Is(err, errStatusWriteLost) {
		t.Fatalf("Reconcile() error = %v, want the lost status write to surface so the work is retried", err)
	}
}

func requirePersistedPhase(t *testing.T, harness *rolloutTestHarness, want string) {
	t.Helper()
	if got := harness.status.status.Phase; got != want {
		t.Fatalf("persisted phase = %s, want %s", got, want)
	}
}

func rolloutSessionsReadyDrift(observation *DrainObservationDTO) {
	observation.SessionsReady++
	observation.SessionsTakenOver++
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.5, §3 "Drain failure / timeout".
func TestLostStatusWriteSurfacesAndTheNextReconcileResumesWithoutRepeatingEffects(t *testing.T) {
	cases := []struct {
		name          string
		steps         int
		failAt        int
		arrange       func(*rolloutTestHarness)
		wantPhase     string
		afterLoss     func(*testing.T, *rolloutTestHarness)
		recoverPhase  string
		afterRecovery func(*testing.T, *rolloutTestHarness)
	}{
		{
			name: "drain intent is not persisted so no drain starts", steps: 0, failAt: 2,
			wantPhase: PhasePreflight, recoverPhase: PhaseDraining,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.broker.starts != 0 {
					t.Fatalf("drain started %d times without a persisted intent", h.broker.starts)
				}
			},
		},
		{
			name: "first SAFE_TO_STOP observation", steps: 2, failAt: 1,
			wantPhase: PhaseDraining, recoverPhase: PhaseDrainSafe,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.status.status.SafeToStopObservedAt != nil || h.runtime.deleteCalls != 0 {
					t.Fatalf("lost write left SAFE_TO_STOP evidence or deleted: %+v", h.status.status)
				}
			},
		},
		{
			name: "Pod resourceVersion refresh while DrainSafe", steps: 3, failAt: 1,
			arrange:   func(h *rolloutTestHarness) { h.runtime.workload.Pods[0].ResourceVersion = "pod-rv-refreshed" },
			wantPhase: PhaseDrainSafe, recoverPhase: PhaseDrainSafe,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.status.status.CurrentPodRV == "pod-rv-refreshed" {
					t.Fatal("lost write was recorded as if it had persisted")
				}
			},
			afterRecovery: func(t *testing.T, h *rolloutTestHarness) {
				if h.status.status.CurrentPodRV != "pod-rv-refreshed" || h.runtime.deleteCalls != 0 {
					t.Fatalf("recovery did not adopt the new resourceVersion without deleting: %+v", h.status.status)
				}
			},
		},
		{
			name: "changed SAFE_TO_STOP evidence before approval", steps: 3, failAt: 1,
			arrange:   func(h *rolloutTestHarness) { h.broker.onSafeObservation = rolloutSessionsReadyDrift },
			wantPhase: PhaseDrainSafe, recoverPhase: PhaseDeleteApproved,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.audit.calls != 0 || h.runtime.deleteCalls != 0 || h.status.status.DeleteIntent {
					t.Fatalf("approval proceeded on unpersisted evidence: audit=%d deletes=%d", h.audit.calls, h.runtime.deleteCalls)
				}
			},
		},
		{
			name: "Pod already terminating while DeleteApproved", steps: 4, failAt: 1,
			arrange:   func(h *rolloutTestHarness) { h.runtime.workload.Pods[0].Deleting = true },
			wantPhase: PhaseDeleteApproved, recoverPhase: PhaseAwaitingReplacement,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.deleteCalls != 0 {
					t.Fatalf("deleted an already terminating Pod: %d calls", h.runtime.deleteCalls)
				}
			},
		},
		{
			name: "Pod resourceVersion changed while DeleteApproved", steps: 4, failAt: 1,
			arrange:   func(h *rolloutTestHarness) { h.runtime.workload.Pods[0].ResourceVersion = "pod-rv-refreshed" },
			wantPhase: PhaseDeleteApproved, recoverPhase: PhaseDrainSafe,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.deleteCalls != 0 || !h.status.status.DeleteIntent {
					t.Fatalf("lost write changed approval state: %+v deletes=%d", h.status.status, h.runtime.deleteCalls)
				}
			},
			afterRecovery: func(t *testing.T, h *rolloutTestHarness) {
				if h.status.status.DeleteIntent || h.status.status.CurrentPodRV != "pod-rv-refreshed" {
					t.Fatalf("recovery kept stale approval for a changed Pod: %+v", h.status.status)
				}
			},
		},
		{
			name: "changed SAFE_TO_STOP evidence while DeleteApproved", steps: 4, failAt: 1,
			arrange:   func(h *rolloutTestHarness) { h.broker.onSafeObservation = rolloutSessionsReadyDrift },
			wantPhase: PhaseDeleteApproved, recoverPhase: PhaseAwaitingReplacement,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.deleteCalls != 0 {
					t.Fatalf("deleted before the changed evidence was persisted: %d calls", h.runtime.deleteCalls)
				}
			},
			afterRecovery: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.deleteCalls != 1 {
					t.Fatalf("recovery deleted %d times, want once", h.runtime.deleteCalls)
				}
			},
		},
		{
			name: "DELETE accepted but the replacement phase is not persisted", steps: 4, failAt: 1,
			wantPhase: PhaseDeleteApproved, recoverPhase: PhaseAwaitingReplacement,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.deleteCalls != 1 {
					t.Fatalf("delete calls = %d, want the approved DELETE to have been issued once", h.runtime.deleteCalls)
				}
			},
			afterRecovery: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.deleteCalls != 1 {
					t.Fatalf("recovery repeated the DELETE: %d calls", h.runtime.deleteCalls)
				}
			},
		},
		{
			name: "ready replacement acceptance", steps: 5, failAt: 1,
			arrange:   func(h *rolloutTestHarness) { replaceWithTarget(h, 0) },
			wantPhase: PhaseAwaitingReplacement, recoverPhase: PhasePreflight,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if len(h.status.status.CompletedOrdinals) != 0 || h.status.status.CurrentOrdinal != 0 || h.runtime.released != 0 {
					t.Fatalf("lost write advanced the rollout: %+v released=%d", h.status.status, h.runtime.released)
				}
			},
			afterRecovery: func(t *testing.T, h *rolloutTestHarness) {
				status := h.status.status
				if len(status.CompletedOrdinals) != 1 || status.CompletedOrdinals[0] != 0 || status.CurrentOrdinal != 1 {
					t.Fatalf("recovery advanced ordinal %d with completed %v, want ordinal 0 completed once", status.CurrentOrdinal, status.CompletedOrdinals)
				}
			},
		},
		{
			name: "abort while the approved Pod is terminating", steps: 4, failAt: 1,
			arrange: func(h *rolloutTestHarness) {
				h.runtime.workload.Pods[0].Deleting = true
				h.request.Abort = true
			},
			wantPhase: PhaseDeleteApproved, recoverPhase: PhaseAwaitingReplacement,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.released != 0 {
					t.Fatal("target released although abort was not persisted")
				}
			},
		},
		{
			name: "abort of an already SAFE_TO_STOP drain", steps: 2, failAt: 1,
			arrange:   func(h *rolloutTestHarness) { h.request.Abort = true },
			wantPhase: PhaseDraining, recoverPhase: PhaseAborted,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.released != 0 || h.broker.cancels != 0 {
					t.Fatalf("lost abort released=%d cancels=%d, want neither", h.runtime.released, h.broker.cancels)
				}
			},
			afterRecovery: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.released != 1 || h.broker.cancels != 0 {
					t.Fatalf("recovered abort released=%d cancels=%d, want 1 and 0", h.runtime.released, h.broker.cancels)
				}
			},
		},
		{
			name: "abort commit while DrainSafe", steps: 3, failAt: 1,
			arrange:   func(h *rolloutTestHarness) { h.request.Abort = true },
			wantPhase: PhaseDrainSafe, recoverPhase: PhaseAborted,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.released != 0 {
					t.Fatal("target released although abort was not persisted")
				}
			},
			afterRecovery: func(t *testing.T, h *rolloutTestHarness) {
				if h.runtime.released != 1 {
					t.Fatalf("recovered abort released the target %d times, want once", h.runtime.released)
				}
			},
		},
		{
			name: "blocker status", steps: 1, failAt: 1,
			arrange:   func(h *rolloutTestHarness) { h.runtime.claimed = false },
			wantPhase: PhaseDraining, recoverPhase: PhaseDraining,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.status.status.FailureCode != "" {
					t.Fatalf("lost blocker write was recorded: %q", h.status.status.FailureCode)
				}
			},
			afterRecovery: func(t *testing.T, h *rolloutTestHarness) {
				if h.status.status.FailureCode != "target_owned" {
					t.Fatalf("recovered blocker code = %q, want target_owned", h.status.status.FailureCode)
				}
			},
		},
		{
			name: "failure status", steps: 1, failAt: 1,
			arrange:   func(h *rolloutTestHarness) { h.request.WorkloadName = "other-broker" },
			wantPhase: PhaseDraining, recoverPhase: PhaseFailed,
			afterLoss: func(t *testing.T, h *rolloutTestHarness) {
				if h.status.status.FailureCode != "" {
					t.Fatalf("lost failure write was recorded: %q", h.status.status.FailureCode)
				}
			},
			afterRecovery: func(t *testing.T, h *rolloutTestHarness) {
				if h.status.status.FailureCode != "spec_changed" {
					t.Fatalf("recovered failure code = %q, want spec_changed", h.status.status.FailureCode)
				}
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := faultHarness(t)
			stepN(t, harness, test.steps)
			if test.arrange != nil {
				test.arrange(harness)
			}
			useLossyStatusStore(harness, test.failAt)
			requireStatusWriteLost(t, harness)
			requirePersistedPhase(t, harness, test.wantPhase)
			if test.afterLoss != nil {
				test.afterLoss(t, harness)
			}
			if _, err := harness.reconcile(t); err != nil {
				t.Fatalf("reconcile after the lost write: %v", err)
			}
			requirePersistedPhase(t, harness, test.recoverPhase)
			if test.afterRecovery != nil {
				test.afterRecovery(t, harness)
			}
		})
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.9.
func TestLostStatusWriteDuringRetryKeepsTheOldRetryTokenUntilTheNewOneIsPersisted(t *testing.T) {
	t.Run("failed drain retry", func(t *testing.T) {
		harness, _, _, _ := failedDrainHarness(t)
		failed := harness.status.status
		harness.request.RetryToken = "attempt-2"
		useLossyStatusStore(harness, 1)
		requireStatusWriteLost(t, harness)
		if harness.status.status.RetryTokenHash != failed.RetryTokenHash || harness.status.status.Phase != PhaseFailed {
			t.Fatalf("lost write consumed the retry token: %+v", harness.status.status)
		}
		if harness.broker.cancels != 1 {
			t.Fatalf("cancels = %d, want the failed drain cancelled once before the write was lost", harness.broker.cancels)
		}
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
		status := harness.status.status
		if status.Phase != PhasePreflight || status.RetryTokenHash != retryTokenHash("attempt-2") || status.AttemptKey == failed.AttemptKey {
			t.Fatalf("recovered retry status = %+v, want a fresh Preflight attempt", status)
		}
		if harness.broker.cancels != 1 {
			t.Fatalf("recovery cancelled the already cancelled drain again: %d cancels", harness.broker.cancels)
		}
	})
	t.Run("SAFE_TO_STOP Pod already terminating after approval", func(t *testing.T) {
		harness, _, _, _ := faultHarness(t)
		failAtApprovalDeadline(t, harness, 4)
		failed := harness.status.status
		if !failed.DeleteIntent {
			t.Fatalf("setup lacks a recorded delete approval: %+v", failed)
		}
		harness.runtime.workload.Pods[0].Deleting = true
		harness.request.RetryToken = "attempt-2"
		useLossyStatusStore(harness, 1)
		requireStatusWriteLost(t, harness)
		if harness.status.status.RetryTokenHash != failed.RetryTokenHash || harness.status.status.Phase != PhaseFailed {
			t.Fatalf("lost write consumed the retry token: %+v", harness.status.status)
		}
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
		status := harness.status.status
		if status.Phase != PhaseAwaitingReplacement || status.RetryTokenHash != retryTokenHash("attempt-2") || status.FailureCode != "" {
			t.Fatalf("recovered retry status = %+v, want AwaitingReplacement with the new token", status)
		}
		if harness.runtime.deleteCalls != 0 {
			t.Fatalf("delete calls = %d, want none: the terminating Pod needs no controller DELETE", harness.runtime.deleteCalls)
		}
	})
	t.Run("SAFE_TO_STOP re-approval", func(t *testing.T) {
		harness, _, _, _ := faultHarness(t)
		failAtApprovalDeadline(t, harness, 3)
		failed := harness.status.status
		harness.request.RetryToken = "attempt-2"
		useLossyStatusStore(harness, 1)
		requireStatusWriteLost(t, harness)
		if harness.status.status.RetryTokenHash != failed.RetryTokenHash || harness.status.status.ApprovalDeadlineAt != nil {
			t.Fatalf("lost write granted a re-approval window: %+v", harness.status.status)
		}
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
		status := harness.status.status
		if status.Phase != PhaseDrainSafe || status.ApprovalDeadlineAt == nil || status.RetryTokenHash != retryTokenHash("attempt-2") {
			t.Fatalf("recovered retry status = %+v, want DrainSafe with a new approval deadline", status)
		}
	})
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.9.
func TestSafeToStopRetryBlocksWithoutConsumingTokenWhenPodIdentityCannotBeRead(t *testing.T) {
	harness, runtime, _, _ := faultHarness(t)
	failAtApprovalDeadline(t, harness, 3)
	failed := harness.status.status
	harness.request.RetryToken = "attempt-2"
	runtime.failInspectIn = 1
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	status := harness.status.status
	if status.Phase != PhaseFailed || status.FailureCode != "retry_cancel_unconfirmed" || status.RetryTokenHash != failed.RetryTokenHash {
		t.Fatalf("unreadable Pod identity: phase=%s code=%s tokenConsumed=%t, want Failed retry_cancel_unconfirmed with the token kept",
			status.Phase, status.FailureCode, status.RetryTokenHash != failed.RetryTokenHash)
	}
	if harness.runtime.deleteCalls != 0 {
		t.Fatalf("deleted a Pod whose identity was unknown: %d calls", harness.runtime.deleteCalls)
	}
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhaseDrainSafe || harness.status.status.RetryTokenHash != retryTokenHash("attempt-2") {
		t.Fatalf("retry did not proceed once the Pod identity was readable: %+v", harness.status.status)
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.7, §3 "Drain failure / timeout".
func TestApprovalDeadlineReachedWhileEvidenceIsPersistedFailsWithoutApprovingOrDeleting(t *testing.T) {
	for _, test := range []struct {
		name  string
		steps int
	}{
		{"before approval", 3},
		{"before DELETE", 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := faultHarness(t)
			stepN(t, harness, test.steps)
			deadline := *harness.status.status.DrainDeadlineAt
			approvalsBefore := len(auditActions(harness, "rollout.pod.delete_approved"))
			harness.broker.onSafeObservation = rolloutSessionsReadyDrift
			store := useLossyStatusStore(harness, 0)
			store.afterWrite = func() { harness.clock.now = deadline }
			if _, err := harness.reconcile(t); err != nil {
				t.Fatal(err)
			}
			requireStatus(t, harness, PhaseFailed, "drain_timeout")
			if harness.runtime.deleteCalls != 0 {
				t.Fatalf("deleted after the approval deadline: %d calls", harness.runtime.deleteCalls)
			}
			if approvals := len(auditActions(harness, "rollout.pod.delete_approved")); approvals != approvalsBefore {
				t.Fatalf("delete_approved events = %d, want %d: no approval after the deadline", approvals, approvalsBefore)
			}
		})
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.7, §3 "Drain failure / timeout".
func TestApprovalDeadlineReachedDuringFreshDrainGetFailsWithoutApprovingOrDeleting(t *testing.T) {
	for _, test := range []struct {
		name  string
		steps int
	}{
		{"before approval", 3},
		{"before DELETE", 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := faultHarness(t)
			stepN(t, harness, test.steps)
			deadline := *harness.status.status.DrainDeadlineAt
			approvalsBefore := len(auditActions(harness, "rollout.pod.delete_approved"))
			harness.broker.advanceOnGet = func() { harness.clock.now = deadline }
			if _, err := harness.reconcile(t); err != nil {
				t.Fatal(err)
			}
			requireStatus(t, harness, PhaseFailed, "drain_timeout")
			if harness.runtime.deleteCalls != 0 {
				t.Fatalf("deleted on evidence that arrived at the deadline: %d calls", harness.runtime.deleteCalls)
			}
			if approvals := len(auditActions(harness, "rollout.pod.delete_approved")); approvals != approvalsBefore {
				t.Fatalf("delete_approved events = %d, want %d", approvals, approvalsBefore)
			}
		})
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.7.
func TestSafeToStopTimestampMustPrecedeTheEffectiveApprovalDeadline(t *testing.T) {
	t.Run("a lost SAFE_TO_STOP timestamp is restamped from the fresh GET", func(t *testing.T) {
		harness, _, _, _ := faultHarness(t)
		stepN(t, harness, 3)
		harness.status.status.SafeToStopObservedAt = nil
		stepN(t, harness, 1)
		status := harness.status.status
		if status.Phase != PhaseDeleteApproved || status.SafeToStopObservedAt == nil || !status.SafeToStopObservedAt.Equal(harness.clock.now) {
			t.Fatalf("restamped approval status = %+v, want DeleteApproved stamped with the fresh observation time", status)
		}
	})
	for _, test := range []struct {
		name  string
		clear bool
	}{
		{"a restamp at the tightened broker deadline is refused", true},
		{"a recorded timestamp at the tightened broker deadline is refused", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := faultHarness(t)
			stepN(t, harness, 3)
			if test.clear {
				harness.status.status.SafeToStopObservedAt = nil
			}
			harness.broker.onGet = func(observation *DrainObservationDTO) { observation.DeadlineAt = harness.clock.now }
			stepN(t, harness, 1)
			requireStatus(t, harness, PhaseDrainSafe, "barrier_unknown")
			if harness.runtime.deleteCalls != 0 || len(auditActions(harness, "rollout.pod.delete_approved")) != 0 {
				t.Fatalf("approved on evidence not before the deadline: deletes=%d approvals=%d",
					harness.runtime.deleteCalls, len(auditActions(harness, "rollout.pod.delete_approved")))
			}
		})
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.7.
func TestAuditIdentityThatCannotFormValidEventsFailsClosedWithoutDeleting(t *testing.T) {
	harness, runtime, broker, images := faultHarness(t)
	stepN(t, harness, 3)
	auditCallsBefore := harness.audit.calls
	service, err := NewService(ServiceConfig{ProducerVersion: strings.Repeat("v", 65), ProducerInstanceID: "controller-0"},
		harness.status, runtime, images, broker, harness.readiness, harness.audit, harness.clock)
	if err != nil {
		t.Fatal(err)
	}
	harness.service = service
	stepN(t, harness, 1)
	requireStatus(t, harness, PhaseFailed, "audit_invalid")
	if harness.runtime.deleteCalls != 0 || harness.status.status.DeleteIntent {
		t.Fatalf("approval without a valid audit event: deletes=%d status=%+v", harness.runtime.deleteCalls, harness.status.status)
	}
	if harness.audit.calls != auditCallsBefore {
		t.Fatalf("invalid events reached the audit sink: %d calls, want %d", harness.audit.calls, auditCallsBefore)
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.7.
func TestCancelledContextBlocksApprovalBeforeAnyAuditAppend(t *testing.T) {
	harness, _, _, _ := faultHarness(t)
	stepN(t, harness, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := reconcileWithContext(ctx, harness)
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want a recorded blocker", err)
	}
	if result.Phase != PhaseDrainSafe || result.RequeueAfter != PollInterval {
		t.Fatalf("Reconcile() result = %+v, want DrainSafe requeued", result)
	}
	requireStatus(t, harness, PhaseDrainSafe, "audit_unavailable")
	if harness.audit.calls != 0 || harness.runtime.deleteCalls != 0 || harness.status.status.DeleteIntent {
		t.Fatalf("cancelled approval: audit=%d deletes=%d status=%+v", harness.audit.calls, harness.runtime.deleteCalls, harness.status.status)
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.9.
func TestAbortCommitsWhenTheBrokerNoLongerKnowsTheDrainOperation(t *testing.T) {
	harness, _, _, _ := drainingHarness(t)
	delete(harness.broker.operations, harness.status.status.AttemptKey)
	harness.request.Abort = true
	stepN(t, harness, 1)
	requireStatus(t, harness, PhaseAborted, "")
	if harness.broker.cancels != 0 || harness.runtime.released != 1 || harness.runtime.deleteCalls != 0 {
		t.Fatalf("abort without a broker operation: cancels=%d released=%d deletes=%d",
			harness.broker.cancels, harness.runtime.released, harness.runtime.deleteCalls)
	}
}

func TestDrainObservationWithoutStartTimeKeepsTheRecordedStart(t *testing.T) {
	harness, _, _, _ := drainingHarness(t)
	recorded := harness.status.status.DrainStartedAt
	if recorded == nil || recorded.IsZero() {
		t.Fatalf("setup lacks a recorded drain start: %+v", harness.status.status)
	}
	want := *recorded
	harness.broker.onGet = func(observation *DrainObservationDTO) { observation.StartedAt = time.Time{} }
	stepN(t, harness, 1)
	got := harness.status.status.DrainStartedAt
	if got == nil || !got.Equal(want) {
		t.Fatalf("drain start = %v, want the previously recorded %v", got, want)
	}
}

func TestPreflightIgnoresNonBrokerContainersInTheStatefulSetTemplate(t *testing.T) {
	harness, _, _, _ := faultHarness(t)
	harness.runtime.workload.Containers = append(harness.runtime.workload.Containers,
		ContainerDTO{Name: "metrics-proxy", Image: "registry.test/proxy:1.0"})
	stepN(t, harness, 1)
	requireStatus(t, harness, PhaseDraining, "")
	if harness.status.status.DrainIntentAt == nil {
		t.Fatalf("a sidecar container blocked the rollout: %+v", harness.status.status)
	}
}
