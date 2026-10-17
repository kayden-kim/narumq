package rollout

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// faultRuntime wraps the shared runtime fake with injectable Kubernetes failures.
type faultRuntime struct {
	*rolloutTestRuntime
	claimErr             error
	releaseErr           error
	releaseCalls         int
	failInspectIn        int
	swallowDelete        bool
	deleteErrAfterEffect error
}

func (runtime *faultRuntime) ClaimTarget(ctx context.Context, claim TargetClaimDTO) (bool, error) {
	if runtime.claimErr != nil {
		return false, runtime.claimErr
	}
	return runtime.rolloutTestRuntime.ClaimTarget(ctx, claim)
}

func (runtime *faultRuntime) ReleaseTarget(ctx context.Context, claim TargetClaimDTO) error {
	runtime.releaseCalls++
	if runtime.releaseErr != nil {
		return runtime.releaseErr
	}
	return runtime.rolloutTestRuntime.ReleaseTarget(ctx, claim)
}

// InspectWorkload fails the failInspectIn-th next call (1 fails the very next call).
func (runtime *faultRuntime) InspectWorkload(ctx context.Context, rollout RolloutDTO) (WorkloadDTO, error) {
	if runtime.failInspectIn > 0 {
		runtime.failInspectIn--
		if runtime.failInspectIn == 0 {
			return WorkloadDTO{}, errors.New("Kubernetes API unavailable")
		}
	}
	return runtime.rolloutTestRuntime.InspectWorkload(ctx, rollout)
}

// DeletePod either accepts the request without terminating the Pod, or applies the delete and still reports an error.
func (runtime *faultRuntime) DeletePod(ctx context.Context, request DeletePodDTO) error {
	if runtime.swallowDelete {
		runtime.rolloutTestRuntime.deleteCalls++
		return nil
	}
	if runtime.deleteErrAfterEffect != nil {
		if err := runtime.rolloutTestRuntime.DeletePod(ctx, request); err != nil {
			return err
		}
		return runtime.deleteErrAfterEffect
	}
	return runtime.rolloutTestRuntime.DeletePod(ctx, request)
}

// faultBroker wraps the shared broker fake with injectable management API failures.
type faultBroker struct {
	*rolloutTestBroker
	featuresErr error
	startErr    error
	cancelErr   error
	cancelPhase string
	beforeGet   func()
	beforeStart func()
}

func (broker *faultBroker) ClusterFeatures(ctx context.Context, namespace, endpoint, secret string) (ClusterFeaturesDTO, error) {
	if broker.featuresErr != nil {
		return ClusterFeaturesDTO{}, broker.featuresErr
	}
	return broker.rolloutTestBroker.ClusterFeatures(ctx, namespace, endpoint, secret)
}

func (broker *faultBroker) GetDrain(ctx context.Context, namespace, endpoint, secret string, nodeID uint32, key string) (DrainObservationDTO, bool, error) {
	if broker.beforeGet != nil {
		broker.beforeGet()
	}
	return broker.rolloutTestBroker.GetDrain(ctx, namespace, endpoint, secret, nodeID, key)
}

func (broker *faultBroker) StartDrain(ctx context.Context, namespace, endpoint, secret string, nodeID uint32, key string) (DrainObservationDTO, error) {
	if broker.beforeStart != nil {
		broker.beforeStart()
	}
	if broker.startErr != nil {
		return DrainObservationDTO{}, broker.startErr
	}
	return broker.rolloutTestBroker.StartDrain(ctx, namespace, endpoint, secret, nodeID, key)
}

func (broker *faultBroker) CancelDrain(ctx context.Context, namespace, endpoint, secret string, nodeID uint32, key string) (DrainObservationDTO, error) {
	if broker.cancelErr != nil {
		return DrainObservationDTO{}, broker.cancelErr
	}
	observation, err := broker.rolloutTestBroker.CancelDrain(ctx, namespace, endpoint, secret, nodeID, key)
	if err == nil && broker.cancelPhase != "" {
		observation.Phase = broker.cancelPhase
	}
	return observation, err
}

// faultHarness is the standard three-node harness with fault-injecting runtime, broker and an editable OCI metadata map.
func faultHarness(t *testing.T) (*rolloutTestHarness, *faultRuntime, *faultBroker, rolloutTestImages) {
	t.Helper()
	harness := newRolloutTestHarness(t)
	oldImage := harness.runtime.workload.Pods[0].Image
	images := make(rolloutTestImages)
	for _, image := range []string{oldImage, harness.request.TargetImage} {
		version := "1.1.0"
		if image == harness.request.TargetImage {
			version = harness.request.TargetVersion
		}
		images[image] = ImageMetadataDTO{
			Image: image, ManifestDigest: digestFromImage(image), OS: "linux", Architecture: "arm64",
			Version: version, Features: slices.Clone(harness.request.TargetFeatures),
		}
	}
	runtime := &faultRuntime{rolloutTestRuntime: harness.runtime}
	broker := &faultBroker{rolloutTestBroker: harness.broker}
	service, err := NewService(ServiceConfig{ProducerVersion: "1.2.3", ProducerInstanceID: "controller-0"},
		harness.status, runtime, images, broker, harness.readiness, harness.audit, harness.clock)
	if err != nil {
		t.Fatal(err)
	}
	harness.service = service
	return harness, runtime, broker, images
}

func stepN(t *testing.T, harness *rolloutTestHarness, count int) {
	t.Helper()
	for range count {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatalf("reconcile in phase %s: %v", harness.status.status.Phase, err)
		}
	}
}

func requireStatus(t *testing.T, harness *rolloutTestHarness, phase, code string) {
	t.Helper()
	status := harness.status.status
	if status.Phase != phase || status.FailureCode != code {
		t.Fatalf("status phase=%s code=%s reason=%q, want phase=%s code=%q", status.Phase, status.FailureCode, status.FailureReason, phase, code)
	}
}

// drainingHarness stops after the broker drain operation exists but is still transferring sessions.
func drainingHarness(t *testing.T) (*rolloutTestHarness, *faultRuntime, *faultBroker, rolloutTestImages) {
	t.Helper()
	harness, runtime, broker, images := faultHarness(t)
	harness.broker.safe = false
	stepN(t, harness, 2)
	requireStatus(t, harness, PhaseDraining, "drain_incomplete")
	if harness.status.status.DrainOperationID == "" || harness.broker.starts != 1 {
		t.Fatalf("drain operation was not persisted: %+v starts=%d", harness.status.status, harness.broker.starts)
	}
	return harness, runtime, broker, images
}

// failedDrainHarness leaves the rollout Failed after the broker reported FAILED for the only drain operation.
func failedDrainHarness(t *testing.T) (*rolloutTestHarness, *faultRuntime, *faultBroker, rolloutTestImages) {
	t.Helper()
	harness, runtime, broker, images := drainingHarness(t)
	harness.broker.onGet = func(observation *DrainObservationDTO) {
		observation.Phase, observation.FailureCode, observation.FailurePresent = "FAILED", "drain_timeout", true
	}
	stepN(t, harness, 1)
	harness.broker.onGet = nil
	requireStatus(t, harness, PhaseFailed, "drain_timeout")
	return harness, runtime, broker, images
}

func TestNewServiceRejectsMissingDependenciesAndProducerIdentity(t *testing.T) {
	harness := newRolloutTestHarness(t)
	config := ServiceConfig{ProducerVersion: "1.2.3", ProducerInstanceID: "controller-0"}
	images := rolloutTestImages{}
	cases := []struct {
		name  string
		build func() (*Service, error)
	}{
		{"status store", func() (*Service, error) {
			return NewService(config, nil, harness.runtime, images, harness.broker, harness.readiness, harness.audit, harness.clock)
		}},
		{"runtime", func() (*Service, error) {
			return NewService(config, harness.status, nil, images, harness.broker, harness.readiness, harness.audit, harness.clock)
		}},
		{"image metadata", func() (*Service, error) {
			return NewService(config, harness.status, harness.runtime, nil, harness.broker, harness.readiness, harness.audit, harness.clock)
		}},
		{"broker", func() (*Service, error) {
			return NewService(config, harness.status, harness.runtime, images, nil, harness.readiness, harness.audit, harness.clock)
		}},
		{"readiness probe", func() (*Service, error) {
			return NewService(config, harness.status, harness.runtime, images, harness.broker, nil, harness.audit, harness.clock)
		}},
		{"audit publisher", func() (*Service, error) {
			return NewService(config, harness.status, harness.runtime, images, harness.broker, harness.readiness, nil, harness.clock)
		}},
		{"clock", func() (*Service, error) {
			return NewService(config, harness.status, harness.runtime, images, harness.broker, harness.readiness, harness.audit, nil)
		}},
		{"producer version", func() (*Service, error) {
			return NewService(ServiceConfig{ProducerInstanceID: "controller-0"}, harness.status, harness.runtime, images, harness.broker, harness.readiness, harness.audit, harness.clock)
		}},
		{"producer instance", func() (*Service, error) {
			return NewService(ServiceConfig{ProducerVersion: "1.2.3"}, harness.status, harness.runtime, images, harness.broker, harness.readiness, harness.audit, harness.clock)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if service, err := test.build(); err == nil || service != nil {
				t.Fatalf("NewService accepted a missing %s: service=%v err=%v", test.name, service, err)
			}
		})
	}
}

func TestReconcileRejectsIncompleteRequestsBeforeTouchingTargetOrStatus(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*RolloutDTO)
	}{
		{"namespace", func(request *RolloutDTO) { request.Namespace = "" }},
		{"name", func(request *RolloutDTO) { request.Name = "" }},
		{"uid", func(request *RolloutDTO) { request.UID = "" }},
		{"resource version", func(request *RolloutDTO) { request.ResourceVersion = "" }},
		{"workload", func(request *RolloutDTO) { request.WorkloadName = "" }},
		{"target image", func(request *RolloutDTO) { request.TargetImage = "" }},
		{"target version", func(request *RolloutDTO) { request.TargetVersion = "" }},
		{"target features", func(request *RolloutDTO) { request.TargetFeatures = nil }},
		{"registry secret", func(request *RolloutDTO) { request.RegistrySecretName = "" }},
		{"management endpoint", func(request *RolloutDTO) { request.ManagementEndpoint = "" }},
		{"management secret name", func(request *RolloutDTO) { request.ManagementSecretName = "" }},
		{"management secret", func(request *RolloutDTO) { request.ManagementSecret = "" }},
		{"retry token", func(request *RolloutDTO) { request.RetryToken = "" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness := newRolloutTestHarness(t)
			request := harness.request
			test.mutate(&request)
			result, err := harness.service.Reconcile(context.Background(), request)
			if err == nil || result != (ReconcileResultDTO{}) {
				t.Fatalf("incomplete request accepted: result=%+v err=%v", result, err)
			}
			if harness.runtime.claimCalls != 0 || harness.status.writes != 0 || harness.broker.gets != 0 {
				t.Fatalf("incomplete request had side effects: claims=%d writes=%d gets=%d", harness.runtime.claimCalls, harness.status.writes, harness.broker.gets)
			}
		})
	}
}

func TestInitialStatusWriteFailureStopsBeforeClaimingTarget(t *testing.T) {
	harness := newRolloutTestHarness(t)
	harness.status.failNext = true
	if _, err := harness.reconcile(t); err == nil {
		t.Fatal("lost initial status write was hidden")
	}
	if harness.status.status.Phase != "" || harness.runtime.claimCalls != 0 || harness.broker.gets != 0 {
		t.Fatalf("rollout acted before its status existed: status=%+v claims=%d gets=%d", harness.status.status, harness.runtime.claimCalls, harness.broker.gets)
	}
}

func TestTargetClaimOutcomesNeverMutateWorkload(t *testing.T) {
	t.Run("claim API error keeps status and retries", func(t *testing.T) {
		harness, runtime, _, _ := faultHarness(t)
		stepN(t, harness, 1)
		writes, before := harness.status.writes, harness.status.status
		runtime.claimErr = errors.New("lease API unavailable")
		result, err := harness.reconcile(t)
		if err != nil || result.RequeueAfter != PollInterval || result.Phase != PhaseDraining {
			t.Fatalf("claim error result=%+v err=%v, want requeue in Draining without error", result, err)
		}
		if harness.status.writes != writes || harness.status.status.Phase != before.Phase || harness.broker.gets != 0 || harness.runtime.deleteCalls != 0 {
			t.Fatalf("unclaimed rollout touched state: writes=%d->%d gets=%d deletes=%d", writes, harness.status.writes, harness.broker.gets, harness.runtime.deleteCalls)
		}
	})
	t.Run("another rollout owns the StatefulSet", func(t *testing.T) {
		harness := newRolloutTestHarness(t)
		harness.runtime.claimed = false
		result, err := harness.reconcile(t)
		if err != nil || result.RequeueAfter != PollInterval {
			t.Fatalf("foreign claim result=%+v err=%v", result, err)
		}
		requireStatus(t, harness, PhasePreflight, "target_owned")
		if harness.broker.gets != 0 || harness.broker.starts != 0 || harness.runtime.deleteCalls != 0 {
			t.Fatalf("foreign-owned target was drained or deleted: gets=%d starts=%d deletes=%d", harness.broker.gets, harness.broker.starts, harness.runtime.deleteCalls)
		}
	})
	t.Run("completed rollout releases the claim and retries release", func(t *testing.T) {
		harness, runtime, _, _ := faultHarness(t)
		stepN(t, harness, 1)
		harness.status.status.Phase = PhaseCompleted
		claims := harness.runtime.claimCalls
		runtime.releaseErr = errors.New("lease API unavailable")
		result, err := harness.reconcile(t)
		if err != nil || result.RequeueAfter != PollInterval || result.Phase != PhaseCompleted {
			t.Fatalf("failed release result=%+v err=%v, want retry", result, err)
		}
		runtime.releaseErr = nil
		result, err = harness.reconcile(t)
		if err != nil || result.RequeueAfter != 0 || result.Phase != PhaseCompleted || harness.runtime.released != 1 {
			t.Fatalf("successful release result=%+v err=%v released=%d", result, err, harness.runtime.released)
		}
		if harness.runtime.claimCalls != claims {
			t.Fatalf("completed rollout claimed the target again: %d -> %d", claims, harness.runtime.claimCalls)
		}
	})
	t.Run("aborted rollout is terminal without abort flag", func(t *testing.T) {
		harness, runtime, _, _ := faultHarness(t)
		stepN(t, harness, 1)
		harness.status.status.Phase = PhaseAborted
		gets := harness.broker.gets
		result, err := harness.reconcile(t)
		if err != nil || result.Phase != PhaseAborted || result.RequeueAfter != 0 || runtime.releaseCalls != 0 || harness.broker.gets != gets {
			t.Fatalf("aborted rollout acted: result=%+v err=%v releases=%d", result, err, runtime.releaseCalls)
		}
	})
}

// Traceability: DEP-18; docs/contracts/rollout-controller-v1.md §2.5, §3 "Exclusive controller/target".
func TestSpecDriftDuringActiveRolloutFailsWithOperatorAction(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*RolloutDTO)
	}{
		{"management endpoint", func(request *RolloutDTO) { request.ManagementEndpoint = "http://other.narumq.svc:8080" }},
		{"registry secret", func(request *RolloutDTO) { request.RegistrySecretName = "other-registry" }},
		{"management secret name", func(request *RolloutDTO) { request.ManagementSecretName = "other-secret" }},
		{"workload", func(request *RolloutDTO) { request.WorkloadName = "other-broker" }},
		{"target version", func(request *RolloutDTO) { request.TargetVersion = "1.3.0" }},
		{"target image", func(request *RolloutDTO) {
			request.TargetImage = "registry.test/narumq@sha256:" + strings.Repeat("c", 64)
		}},
		{"target features", func(request *RolloutDTO) { request.TargetFeatures = []string{"compatibility-probe", "extra-feature"} }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := faultHarness(t)
			stepN(t, harness, 1)
			test.mutate(&harness.request)
			if _, err := harness.reconcile(t); err != nil {
				t.Fatal(err)
			}
			requireStatus(t, harness, PhaseFailed, "spec_changed")
			if !strings.HasSuffix(harness.status.status.FailureReason, failedOperatorAction) {
				t.Fatalf("failure reason lacks operator action: %q", harness.status.status.FailureReason)
			}
			if harness.runtime.deleteCalls != 0 || harness.broker.starts != 0 || harness.broker.gets != 0 {
				t.Fatalf("spec drift mutated workload: deletes=%d starts=%d gets=%d", harness.runtime.deleteCalls, harness.broker.starts, harness.broker.gets)
			}
			failed := auditActions(harness, "rollout.failed")
			if len(failed) != 1 || failed[0].Reason != "spec_changed" {
				t.Fatalf("rollout.failed audit events = %+v", failed)
			}
		})
	}
}

func TestFailureIsRecordedEvenWhenAuditPublisherIsUnavailable(t *testing.T) {
	harness, _, _, _ := faultHarness(t)
	stepN(t, harness, 1)
	harness.audit.readyErr = errors.New("audit PVC unavailable")
	harness.request.TargetVersion = "1.3.0"
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, harness, PhaseFailed, "spec_changed")
	if harness.audit.calls != 0 || len(harness.audit.events) != 0 {
		t.Fatalf("failure published audit events while the publisher was not ready: calls=%d events=%d", harness.audit.calls, len(harness.audit.events))
	}
}

func TestCorruptPersistedStatusFailsClosed(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*StatusDTO)
		wantCode string
	}{
		{"ordinal beyond three nodes", func(status *StatusDTO) { status.CurrentOrdinal = 3 }, "invalid_ordinal"},
		{"negative ordinal", func(status *StatusDTO) { status.CurrentOrdinal = -1 }, "invalid_ordinal"},
		{"unknown phase", func(status *StatusDTO) { status.Phase = "Bogus" }, "unknown_phase"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := faultHarness(t)
			stepN(t, harness, 1)
			test.mutate(&harness.status.status)
			if _, err := harness.reconcile(t); err != nil {
				t.Fatal(err)
			}
			requireStatus(t, harness, PhaseFailed, test.wantCode)
			if harness.runtime.deleteCalls != 0 || harness.broker.starts != 0 {
				t.Fatalf("corrupt status mutated workload: deletes=%d starts=%d", harness.runtime.deleteCalls, harness.broker.starts)
			}
		})
	}
}

func TestRetryTokenChangeMidAttemptFailsThenRestartsOrdinalWithNewAttemptKey(t *testing.T) {
	harness, _, _, _ := faultHarness(t)
	stepN(t, harness, 1)
	firstKey, firstHash := harness.status.status.AttemptKey, harness.status.status.RetryTokenHash
	harness.request.RetryToken = "attempt-2"
	stepN(t, harness, 1)
	requireStatus(t, harness, PhaseFailed, "retry_token_changed")
	if harness.status.status.RetryTokenHash != firstHash {
		t.Fatal("failing on a mid-attempt token change consumed the new token")
	}
	stepN(t, harness, 1)
	status := harness.status.status
	if status.Phase != PhasePreflight || status.FailureCode != "" || status.RetryTokenHash != retryTokenHash("attempt-2") ||
		status.AttemptKey == firstKey || status.AttemptKey != attemptKey("rollout-uid", 0, "attempt-2") || status.DrainOperationID != "" || status.DrainDeadlineAt != nil {
		t.Fatalf("retry before any drain started = %+v", status)
	}
	if harness.broker.cancels != 0 || harness.broker.starts != 0 {
		t.Fatalf("retry without a drain operation touched the broker: cancels=%d starts=%d", harness.broker.cancels, harness.broker.starts)
	}
	stepN(t, harness, 2)
	if harness.status.status.Phase != PhaseDraining || harness.status.status.DrainOperationID == "" || harness.broker.starts != 1 {
		t.Fatalf("restarted attempt did not start a fresh drain: %+v starts=%d", harness.status.status, harness.broker.starts)
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.9, §3 "Drain failure / timeout".
func TestRetryAfterFailedDrainCancelsOldOperationAndStartsFreshAttempt(t *testing.T) {
	harness, _, _, _ := failedDrainHarness(t)
	failed := harness.status.status
	harness.request.RetryToken = "attempt-2"
	result, err := harness.reconcile(t)
	if err != nil {
		t.Fatal(err)
	}
	status := harness.status.status
	if result.Phase != PhasePreflight || result.RequeueAfter != 0 || status.Phase != PhasePreflight || status.FailureCode != "" || status.FailureReason != "" {
		t.Fatalf("retry result=%+v status=%+v, want a clean Preflight", result, status)
	}
	if harness.broker.cancels != 1 || harness.broker.operations[failed.AttemptKey].Phase != "CANCELLED" {
		t.Fatalf("failed drain was not cancelled: cancels=%d old=%+v", harness.broker.cancels, harness.broker.operations[failed.AttemptKey])
	}
	if status.AttemptKey == failed.AttemptKey || status.RetryTokenHash != retryTokenHash("attempt-2") || status.DrainOperationID != "" ||
		status.DrainIntentAt != nil || status.DrainDeadlineAt != nil || status.DrainSafeToStop != nil || status.DrainFailureCode != "" ||
		status.DrainPhase != "" || status.DeleteIntent {
		t.Fatalf("retry kept stale drain evidence: %+v", status)
	}
	stepN(t, harness, 2)
	if harness.broker.starts != 2 || harness.status.status.Phase != PhaseDraining || harness.status.status.DrainOperationID == "" || harness.runtime.deleteCalls != 0 {
		t.Fatalf("fresh attempt: starts=%d status=%+v deletes=%d", harness.broker.starts, harness.status.status, harness.runtime.deleteCalls)
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.9, §3 "Drain failure / timeout".
func TestRetryNeverConsumesTokenWithoutConfirmedCancellation(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*rolloutTestHarness, *faultBroker)
		wantCode  string
	}{
		{"drain GET unknown", func(h *rolloutTestHarness, _ *faultBroker) { h.broker.getErr = errors.New("management API timeout") }, "retry_cancel_unconfirmed"},
		{"cancel request fails", func(_ *rolloutTestHarness, b *faultBroker) { b.cancelErr = errors.New("management API timeout") }, "retry_cancel_unconfirmed"},
		{"cancel does not reach CANCELLED", func(_ *rolloutTestHarness, b *faultBroker) { b.cancelPhase = "DRAINING" }, "retry_cancel_unconfirmed"},
		{"persisted operation is foreign", func(h *rolloutTestHarness, _ *faultBroker) {
			key := h.status.status.AttemptKey
			operation := h.broker.operations[key]
			operation.OperationID = "foreign-operation"
			h.broker.operations[key] = operation
		}, "retry_cancel_unconfirmed"},
		{"persisted operation vanished", func(h *rolloutTestHarness, _ *faultBroker) {
			delete(h.broker.operations, h.status.status.AttemptKey)
		}, "retry_cancel_unconfirmed"},
		{"ownership replay fails", func(h *rolloutTestHarness, b *faultBroker) {
			h.status.status.DrainOperationID = ""
			b.startErr = errors.New("management API timeout")
		}, "retry_cancel_unconfirmed"},
		{"failed attempt has no attempt key", func(h *rolloutTestHarness, _ *faultBroker) { h.status.status.AttemptKey = "" }, "retry_without_attempt"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness, _, broker, _ := failedDrainHarness(t)
			failed := harness.status.status
			startsBefore := harness.broker.starts
			test.configure(harness, broker)
			harness.request.RetryToken = "attempt-2"
			if _, err := harness.reconcile(t); err != nil {
				t.Fatal(err)
			}
			requireStatus(t, harness, PhaseFailed, test.wantCode)
			if harness.status.status.RetryTokenHash != failed.RetryTokenHash {
				t.Fatal("unconfirmed cancellation consumed the new retryToken")
			}
			if harness.broker.starts != startsBefore || harness.runtime.deleteCalls != 0 {
				t.Fatalf("blocked retry mutated broker or Pod: starts %d->%d deletes=%d", startsBefore, harness.broker.starts, harness.runtime.deleteCalls)
			}
		})
	}
}

func TestRetryAcceptsAlreadyCancelledDrainWithoutSecondCancel(t *testing.T) {
	harness, _, _, _ := failedDrainHarness(t)
	key := harness.status.status.AttemptKey
	operation := harness.broker.operations[key]
	operation.Phase = "CANCELLED"
	harness.broker.operations[key] = operation
	harness.request.RetryToken = "attempt-2"
	stepN(t, harness, 1)
	requireStatus(t, harness, PhasePreflight, "")
	if harness.broker.cancels != 0 {
		t.Fatalf("already cancelled drain was cancelled again: %d", harness.broker.cancels)
	}
}

func TestRetryReplaysUnrecordedDrainStartBeforeCancelling(t *testing.T) {
	harness, _, _, _ := failedDrainHarness(t)
	harness.status.status.DrainOperationID = ""
	harness.request.RetryToken = "attempt-2"
	stepN(t, harness, 1)
	requireStatus(t, harness, PhasePreflight, "")
	if harness.broker.cancels != 1 || harness.broker.starts != 2 {
		t.Fatalf("replay-then-cancel: cancels=%d starts=%d, want 1 and 2", harness.broker.cancels, harness.broker.starts)
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.9, §3 "Drain failure / timeout".
func TestAbortBeforeSafeToStopCancelsDrainBeforeReleasingTarget(t *testing.T) {
	harness, _, _, _ := drainingHarness(t)
	harness.request.Abort = true
	result, err := harness.reconcile(t)
	if err != nil {
		t.Fatal(err)
	}
	status := harness.status.status
	if result.Phase != PhaseAborted || status.Phase != PhaseAborted || status.DrainPhase != "CANCELLED" || status.DeleteIntent || status.FailureCode != "" {
		t.Fatalf("abort result=%+v status=%+v", result, status)
	}
	if harness.broker.cancels != 1 || harness.runtime.released != 1 || harness.runtime.deleteCalls != 0 {
		t.Fatalf("abort side effects: cancels=%d released=%d deletes=%d", harness.broker.cancels, harness.runtime.released, harness.runtime.deleteCalls)
	}
}

func TestAbortStaysUncommittedUntilBrokerDrainStateIsConfirmed(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*rolloutTestHarness, *faultBroker)
		heal      func(*rolloutTestHarness, *faultBroker)
	}{
		{"drain GET unknown", func(h *rolloutTestHarness, _ *faultBroker) { h.broker.getErr = errors.New("management API timeout") },
			func(h *rolloutTestHarness, _ *faultBroker) { h.broker.getErr = nil }},
		{"cancel request fails", func(_ *rolloutTestHarness, b *faultBroker) { b.cancelErr = errors.New("management API timeout") },
			func(_ *rolloutTestHarness, b *faultBroker) { b.cancelErr = nil }},
		{"cancel does not reach CANCELLED", func(_ *rolloutTestHarness, b *faultBroker) { b.cancelPhase = "DRAINING" },
			func(_ *rolloutTestHarness, b *faultBroker) { b.cancelPhase = "" }},
		{"observed drain is foreign", func(h *rolloutTestHarness, _ *faultBroker) {
			key := h.status.status.AttemptKey
			operation := h.broker.operations[key]
			operation.OperationID = "foreign-operation"
			h.broker.operations[key] = operation
		}, func(h *rolloutTestHarness, _ *faultBroker) {
			key := h.status.status.AttemptKey
			operation := h.broker.operations[key]
			operation.OperationID = h.status.status.DrainOperationID
			h.broker.operations[key] = operation
		}},
		{"ownership replay fails", func(h *rolloutTestHarness, b *faultBroker) {
			h.status.status.DrainOperationID = ""
			b.startErr = errors.New("management API timeout")
		}, func(_ *rolloutTestHarness, b *faultBroker) { b.startErr = nil }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness, _, broker, _ := drainingHarness(t)
			harness.request.Abort = true
			test.configure(harness, broker)
			if _, err := harness.reconcile(t); err != nil {
				t.Fatal(err)
			}
			requireStatus(t, harness, PhaseDraining, "abort_unconfirmed")
			if harness.runtime.released != 0 {
				t.Fatal("target was released before the drain was confirmed cancelled")
			}
			test.heal(harness, broker)
			stepN(t, harness, 1)
			requireStatus(t, harness, PhaseAborted, "")
			if harness.runtime.released != 1 || harness.runtime.deleteCalls != 0 {
				t.Fatalf("healed abort: released=%d deletes=%d", harness.runtime.released, harness.runtime.deleteCalls)
			}
		})
	}
}

func TestAbortReplaysUnrecordedDrainStartBeforeCancelling(t *testing.T) {
	harness, _, _, _ := drainingHarness(t)
	harness.status.status.DrainOperationID = ""
	harness.request.Abort = true
	stepN(t, harness, 1)
	requireStatus(t, harness, PhaseAborted, "")
	if harness.broker.cancels != 1 || harness.broker.starts != 2 {
		t.Fatalf("replay-then-cancel: cancels=%d starts=%d, want 1 and 2", harness.broker.cancels, harness.broker.starts)
	}
}

func TestAbortDoesNotCancelDrainThatAlreadyReachedSafeToStop(t *testing.T) {
	harness, _, _, _ := drainingHarness(t)
	harness.broker.safe = true
	harness.request.Abort = true
	stepN(t, harness, 1)
	status := harness.status.status
	if status.Phase != PhaseAborted || status.DrainState != "SAFE_TO_STOP" || harness.broker.cancels != 0 || harness.runtime.released != 1 {
		t.Fatalf("abort of SAFE_TO_STOP drain: status=%+v cancels=%d released=%d", status, harness.broker.cancels, harness.runtime.released)
	}
}

func TestAbortFromFailedDrainDoesNotCancelTerminalOperation(t *testing.T) {
	harness, _, _, _ := failedDrainHarness(t)
	harness.request.Abort = true
	stepN(t, harness, 1)
	status := harness.status.status
	if status.Phase != PhaseAborted || status.FailureCode != "" || status.FailureReason != "" || harness.broker.cancels != 0 || harness.runtime.released != 1 {
		t.Fatalf("abort of Failed rollout: status=%+v cancels=%d released=%d", status, harness.broker.cancels, harness.runtime.released)
	}
}

func TestAbortWithoutCancellableDrainReleasesTargetImmediately(t *testing.T) {
	for _, test := range []struct {
		name  string
		steps int
	}{
		{"before the first drain intent", 0},
		{"after SAFE_TO_STOP but before approval", 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := faultHarness(t)
			stepN(t, harness, test.steps)
			harness.request.Abort = true
			stepN(t, harness, 1)
			if harness.status.status.Phase != PhaseAborted || harness.broker.cancels != 0 || harness.runtime.deleteCalls != 0 ||
				harness.runtime.released != 1 || len(auditActions(harness, "rollout.pod.delete_approved")) != 0 {
				t.Fatalf("abort: status=%+v cancels=%d deletes=%d released=%d", harness.status.status, harness.broker.cancels, harness.runtime.deleteCalls, harness.runtime.released)
			}
		})
	}
}

func TestAbortAfterApprovalReportsUnknownWorkloadAndObservedPodTermination(t *testing.T) {
	harness, runtime, _, _ := faultHarness(t)
	stepN(t, harness, 4)
	if !harness.status.status.DeleteIntent {
		t.Fatalf("setup lacks persisted delete intent: %+v", harness.status.status)
	}
	harness.request.Abort = true
	runtime.failInspectIn = 1
	result, err := harness.reconcile(t)
	if err != nil || result.RequeueAfter != PollInterval || result.Phase != PhaseDeleteApproved || harness.runtime.released != 0 {
		t.Fatalf("unknown workload: result=%+v err=%v released=%d", result, err, harness.runtime.released)
	}
	// The approved DELETE took effect while the controller was away: abort must wait for the replacement, not release.
	harness.runtime.workload.Pods[0].Deleting = true
	result, err = harness.reconcile(t)
	if err != nil || result.Phase != PhaseAwaitingReplacement || result.RequeueAfter != 0 {
		t.Fatalf("terminating Pod: result=%+v err=%v", result, err)
	}
	if harness.status.status.Phase != PhaseAwaitingReplacement || harness.runtime.released != 0 || harness.runtime.deleteCalls != 0 {
		t.Fatalf("abort released or deleted while the Pod was terminating: status=%+v released=%d deletes=%d", harness.status.status, harness.runtime.released, harness.runtime.deleteCalls)
	}
}

func TestAbortWhileAwaitingReplacementBlocksOnInconsistentEvidence(t *testing.T) {
	harness, _, _, _ := faultHarness(t)
	stepN(t, harness, 5)
	requireStatus(t, harness, PhaseAwaitingReplacement, "")
	harness.request.Abort = true
	harness.runtime.workload.ImagePullSecrets = nil
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, harness, PhaseAwaitingReplacement, "replacement_unconfirmed")
	if harness.runtime.released != 0 {
		t.Fatal("target released without replacement evidence")
	}
}

func TestAbortedRolloutRetriesTargetReleaseAndReportsFailure(t *testing.T) {
	harness, runtime, _, _ := faultHarness(t)
	stepN(t, harness, 1)
	harness.status.status.Phase = PhaseAborted
	harness.request.Abort = true
	runtime.releaseErr = errors.New("lease API unavailable")
	if _, err := harness.reconcile(t); !errors.Is(err, runtime.releaseErr) {
		t.Fatalf("release failure was hidden: %v", err)
	}
	runtime.releaseErr = nil
	result, err := harness.reconcile(t)
	if err != nil || result.Phase != PhaseAborted || harness.runtime.released != 1 {
		t.Fatalf("retried release: result=%+v err=%v released=%d", result, err, harness.runtime.released)
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §3 "Drain failure / timeout".
func TestDrainDeadlineIsEnforcedAtEveryUnknownResult(t *testing.T) {
	t.Run("deadline elapsed before polling never calls the broker", func(t *testing.T) {
		harness, _, _, _ := faultHarness(t)
		stepN(t, harness, 1)
		harness.clock.now = *harness.status.status.DrainDeadlineAt
		stepN(t, harness, 1)
		requireStatus(t, harness, PhaseFailed, "drain_timeout")
		if harness.broker.gets != 0 || harness.broker.starts != 0 || !strings.HasSuffix(harness.status.status.FailureReason, failedOperatorAction) {
			t.Fatalf("late poll: gets=%d starts=%d reason=%q", harness.broker.gets, harness.broker.starts, harness.status.status.FailureReason)
		}
	})
	t.Run("unknown GET result that outlives the deadline", func(t *testing.T) {
		harness, _, broker, _ := drainingHarness(t)
		deadline := *harness.status.status.DrainDeadlineAt
		harness.broker.getErr = errors.New("management API timeout")
		broker.beforeGet = func() { harness.clock.now = deadline }
		stepN(t, harness, 1)
		requireStatus(t, harness, PhaseFailed, "drain_timeout")
		if harness.runtime.deleteCalls != 0 {
			t.Fatal("deleted after an unknown drain result")
		}
	})
	t.Run("unknown start result that outlives the deadline", func(t *testing.T) {
		harness, _, broker, _ := faultHarness(t)
		stepN(t, harness, 1)
		deadline := *harness.status.status.DrainDeadlineAt
		broker.startErr = errors.New("management API timeout")
		broker.beforeStart = func() { harness.clock.now = deadline }
		stepN(t, harness, 1)
		requireStatus(t, harness, PhaseFailed, "drain_timeout")
		if harness.broker.starts != 0 || harness.runtime.deleteCalls != 0 {
			t.Fatalf("starts=%d deletes=%d", harness.broker.starts, harness.runtime.deleteCalls)
		}
	})
	t.Run("a shorter broker deadline tightens the persisted deadline", func(t *testing.T) {
		harness, _, _, _ := drainingHarness(t)
		harness.broker.onGet = func(observation *DrainObservationDTO) { observation.DeadlineAt = harness.clock.now.Add(time.Minute) }
		stepN(t, harness, 1)
		want := harness.clock.now.Add(time.Minute)
		if got := harness.status.status.DrainDeadlineAt; got == nil || !got.Equal(want) {
			t.Fatalf("persisted deadline = %v, want %v", got, want)
		}
		harness.broker.onGet = nil
		harness.clock.now = want
		stepN(t, harness, 1)
		requireStatus(t, harness, PhaseFailed, "drain_timeout")
	})
	t.Run("a later broker deadline never extends the persisted deadline", func(t *testing.T) {
		harness, _, _, _ := drainingHarness(t)
		before := *harness.status.status.DrainDeadlineAt
		harness.broker.onGet = func(observation *DrainObservationDTO) { observation.DeadlineAt = harness.clock.now.Add(time.Hour) }
		stepN(t, harness, 1)
		if got := harness.status.status.DrainDeadlineAt; got == nil || !got.Equal(before) {
			t.Fatalf("persisted deadline moved from %v to %v", before, got)
		}
	})
}

func TestDrainStartFailureKeepsIntentAndRetriesWithoutOperation(t *testing.T) {
	harness, _, broker, _ := faultHarness(t)
	stepN(t, harness, 1)
	broker.startErr = errors.New("management API timeout")
	stepN(t, harness, 1)
	requireStatus(t, harness, PhaseDraining, "drain_start_unconfirmed")
	if harness.status.status.DrainOperationID != "" || harness.broker.starts != 0 {
		t.Fatalf("unconfirmed start recorded an operation: %+v", harness.status.status)
	}
	broker.startErr = nil
	stepN(t, harness, 1)
	if harness.status.status.DrainOperationID == "" || harness.broker.starts != 1 {
		t.Fatalf("retry did not start the drain: %+v starts=%d", harness.status.status, harness.broker.starts)
	}
}

func seedBrokerOperation(harness *rolloutTestHarness) {
	harness.broker.operations[harness.status.status.AttemptKey] = DrainObservationDTO{
		OperationID: "operation-1", NodeID: 1, Phase: "TRANSFERRING_SESSIONS", State: "DRAINING",
		StartedAt: harness.clock.now, DeadlineAt: harness.clock.now.Add(DefaultDrainTimeout), SafeToStop: new(false),
	}
}

func TestDrainAdoptsOperationFromUncertainStartOnlyThroughIdempotentReplay(t *testing.T) {
	t.Run("replay confirms ownership", func(t *testing.T) {
		harness, _, _, _ := faultHarness(t)
		harness.broker.safe = false
		stepN(t, harness, 1)
		seedBrokerOperation(harness)
		stepN(t, harness, 1)
		status := harness.status.status
		if status.Phase != PhaseDraining || status.DrainOperationID != "operation-1" || harness.broker.starts != 1 {
			t.Fatalf("adopted operation: %+v starts=%d", status, harness.broker.starts)
		}
	})
	t.Run("failed replay does not adopt", func(t *testing.T) {
		harness, _, broker, _ := faultHarness(t)
		harness.broker.safe = false
		stepN(t, harness, 1)
		seedBrokerOperation(harness)
		broker.startErr = errors.New("management API timeout")
		stepN(t, harness, 1)
		requireStatus(t, harness, PhaseDraining, "drain_start_unconfirmed")
		if harness.status.status.DrainOperationID != "" {
			t.Fatalf("adopted an operation without proof of ownership: %+v", harness.status.status)
		}
	})
}

func TestDrainObservationInconsistenciesNeverAdvance(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*rolloutTestHarness)
		wantPhase string
		wantCode  string
	}{
		{"operation identity changed", func(h *rolloutTestHarness) {
			key := h.status.status.AttemptKey
			operation := h.broker.operations[key]
			operation.OperationID = "operation-other"
			h.broker.operations[key] = operation
		}, PhaseFailed, "drain_operation_mismatch"},
		{"operation disappeared", func(h *rolloutTestHarness) {
			delete(h.broker.operations, h.status.status.AttemptKey)
		}, PhaseDraining, "drain_operation_missing"},
		{"observation lacks a deadline", func(h *rolloutTestHarness) {
			h.broker.onGet = func(observation *DrainObservationDTO) { observation.DeadlineAt = time.Time{} }
		}, PhaseDraining, "drain_observation_invalid"},
		{"Pod replaced while draining", func(h *rolloutTestHarness) {
			h.runtime.workload.Pods[0].UID = "replacement-pod-uid"
		}, PhaseFailed, "pod_identity_changed"},
		{"Pod terminating while draining", func(h *rolloutTestHarness) {
			h.runtime.workload.Pods[0].Deleting = true
		}, PhaseDraining, "preflight_blocked"},
		{"StatefulSet replaced while draining", func(h *rolloutTestHarness) {
			h.runtime.workload.UID = "recreated-statefulset"
		}, PhaseDraining, "preflight_blocked"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := drainingHarness(t)
			test.configure(harness)
			stepN(t, harness, 1)
			requireStatus(t, harness, test.wantPhase, test.wantCode)
			if harness.broker.starts != 1 || harness.runtime.deleteCalls != 0 || harness.status.status.DrainSafeToStop == nil || *harness.status.status.DrainSafeToStop {
				t.Fatalf("inconsistent drain evidence advanced: starts=%d deletes=%d status=%+v", harness.broker.starts, harness.runtime.deleteCalls, harness.status.status)
			}
		})
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.9, §3 "Drain failure / timeout".
func TestSafeToStopRequiresEveryDrainCounterToBeZero(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*DrainObservationDTO)
		seen   func(StatusDTO) bool
	}{
		{"connections remain", func(o *DrainObservationDTO) { o.ConnectionsRemaining = 2 }, func(s StatusDTO) bool { return s.DrainConnectionsRemaining == 2 }},
		{"sessions failed", func(o *DrainObservationDTO) { o.SessionsFailed = 1 }, func(s StatusDTO) bool { return s.DrainSessionsFailed == 1 }},
		{"WAL pending", func(o *DrainObservationDTO) { o.PendingWAL = 7 }, func(s StatusDTO) bool { return s.DrainPendingWAL == 7 }},
		{"under-replicated", func(o *DrainObservationDTO) { o.UnderReplicated = 3 }, func(s StatusDTO) bool { return s.DrainUnderReplicated == 3 }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := drainingHarness(t)
			harness.broker.safe = true
			harness.broker.onSafeObservation = test.mutate
			stepN(t, harness, 1)
			requireStatus(t, harness, PhaseDraining, "drain_incomplete")
			status := harness.status.status
			if !test.seen(status) || status.SafeToStopObservedAt != nil || harness.runtime.deleteCalls != 0 {
				t.Fatalf("incomplete SAFE_TO_STOP was accepted or not recorded: %+v deletes=%d", status, harness.runtime.deleteCalls)
			}
			// The broker later reports a fully drained node: only then may the rollout advance.
			harness.broker.onSafeObservation = nil
			key := status.AttemptKey
			operation := harness.broker.operations[key]
			operation.ConnectionsRemaining, operation.SessionsFailed, operation.PendingWAL, operation.UnderReplicated = 0, 0, 0, 0
			harness.broker.operations[key] = operation
			stepN(t, harness, 1)
			if harness.status.status.Phase != PhaseDrainSafe || harness.status.status.SafeToStopObservedAt == nil {
				t.Fatalf("complete drain did not advance: %+v", harness.status.status)
			}
		})
	}
}

func TestBrokerFailureCodeIsPersistedAndFailsRollout(t *testing.T) {
	cases := []struct {
		name      string
		mutate    func(*DrainObservationDTO)
		wantCode  string
		wantDrain string
	}{
		{"FAILED without code", func(o *DrainObservationDTO) { o.Phase = "FAILED" }, "drain_failed", ""},
		{"failure flag on a running drain", func(o *DrainObservationDTO) { o.FailurePresent = true }, "drain_failed", ""},
		{"FAILED with broker code", func(o *DrainObservationDTO) { o.Phase, o.FailureCode = "FAILED", "peer_lost" }, "peer_lost", "peer_lost"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := drainingHarness(t)
			harness.broker.onGet = test.mutate
			stepN(t, harness, 1)
			requireStatus(t, harness, PhaseFailed, test.wantCode)
			status := harness.status.status
			if status.DrainFailureCode != test.wantDrain || !strings.HasPrefix(status.FailureReason, "broker drain entered FAILED; ") || harness.runtime.deleteCalls != 0 {
				t.Fatalf("failed drain status = %+v deletes=%d", status, harness.runtime.deleteCalls)
			}
		})
	}
}

func TestUnchangedDrainPollDoesNotRewriteStatus(t *testing.T) {
	harness, _, _, _ := drainingHarness(t)
	writes, blocked := harness.status.writes, harness.status.status
	for step := range 3 {
		result, err := harness.reconcile(t)
		if err != nil {
			t.Fatal(err)
		}
		if harness.status.writes != writes || result.RequeueAfter != PollInterval || result.Phase != PhaseDraining {
			t.Fatalf("poll %d rewrote unchanged status or lost the poll interval: writes=%d want=%d result=%+v", step, harness.status.writes, writes, result)
		}
	}
	if harness.status.status.FailureCode != "drain_incomplete" || !harness.status.status.Conditions[0].LastTransitionTime.Equal(blocked.Conditions[0].LastTransitionTime) {
		t.Fatalf("polling changed the blocker: %+v", harness.status.status)
	}
}

func TestDrainSafePodChangesBlockOrRefreshApprovalPreconditions(t *testing.T) {
	t.Run("Pod UID changed", func(t *testing.T) {
		harness, _, _, _ := faultHarness(t)
		stepN(t, harness, 3)
		harness.runtime.workload.Pods[0].UID = "intruder-uid"
		stepN(t, harness, 1)
		requireStatus(t, harness, PhaseDrainSafe, "pod_identity_changed")
		if harness.audit.calls != 0 || harness.runtime.deleteCalls != 0 {
			t.Fatalf("approved a different Pod: audit=%d deletes=%d", harness.audit.calls, harness.runtime.deleteCalls)
		}
	})
	t.Run("Pod terminating", func(t *testing.T) {
		harness, _, _, _ := faultHarness(t)
		stepN(t, harness, 3)
		harness.runtime.workload.Pods[0].Deleting = true
		stepN(t, harness, 1)
		requireStatus(t, harness, PhaseDrainSafe, "preflight_blocked")
		if harness.audit.calls != 0 || harness.runtime.deleteCalls != 0 {
			t.Fatalf("approved a terminating Pod: audit=%d deletes=%d", harness.audit.calls, harness.runtime.deleteCalls)
		}
	})
	t.Run("resourceVersion changed re-reads before approving", func(t *testing.T) {
		harness, _, _, _ := faultHarness(t)
		stepN(t, harness, 3)
		harness.runtime.workload.Pods[0].ResourceVersion = "rv-bumped"
		result, err := harness.reconcile(t)
		if err != nil || result.RequeueAfter != PollInterval || result.Phase != PhaseDrainSafe {
			t.Fatalf("resourceVersion refresh result=%+v err=%v", result, err)
		}
		if harness.status.status.CurrentPodRV != "rv-bumped" || harness.audit.calls != 0 || harness.runtime.deleteCalls != 0 {
			t.Fatalf("refresh: %+v audit=%d deletes=%d", harness.status.status, harness.audit.calls, harness.runtime.deleteCalls)
		}
		stepN(t, harness, 2)
		// The fake accepts a DELETE only when the request carries the refreshed resourceVersion.
		if harness.status.status.Phase != PhaseAwaitingReplacement || harness.runtime.deleteCalls != 1 {
			t.Fatalf("delete with refreshed precondition: %+v deletes=%d", harness.status.status, harness.runtime.deleteCalls)
		}
	})
}

// Traceability: DEP-18; docs/contracts/rollout-controller-v1.md §2.7, §3 "Kubernetes mutation 결과 불명".
func TestApprovalEvidenceLossBlocksUntilFreshSafeEvidenceReturns(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*rolloutTestHarness)
		heal      func(*rolloutTestHarness)
	}{
		{"drain GET unknown", func(h *rolloutTestHarness) { h.broker.getErr = errors.New("management API timeout") },
			func(h *rolloutTestHarness) { h.broker.getErr = nil }},
		{"drain no longer SAFE_TO_STOP", func(h *rolloutTestHarness) { h.broker.safe = false },
			func(h *rolloutTestHarness) { h.broker.safe = true }},
		{"operation identity changed", func(h *rolloutTestHarness) {
			key := h.status.status.AttemptKey
			operation := h.broker.operations[key]
			operation.OperationID = "foreign-operation"
			h.broker.operations[key] = operation
		}, func(h *rolloutTestHarness) {
			key := h.status.status.AttemptKey
			operation := h.broker.operations[key]
			operation.OperationID = h.status.status.DrainOperationID
			h.broker.operations[key] = operation
		}},
		{"WAL pending again", func(h *rolloutTestHarness) {
			h.broker.onSafeObservation = func(observation *DrainObservationDTO) { observation.PendingWAL = 4 }
		}, func(h *rolloutTestHarness) {
			h.broker.onSafeObservation = nil
			key := h.status.status.AttemptKey
			operation := h.broker.operations[key]
			operation.PendingWAL = 0
			h.broker.operations[key] = operation
		}},
	}
	for _, phase := range []struct {
		name  string
		steps int
		want  string
	}{{"DrainSafe", 3, PhaseDrainSafe}, {"DeleteApproved", 4, PhaseDeleteApproved}} {
		for _, test := range cases {
			t.Run(phase.name+"/"+test.name, func(t *testing.T) {
				harness, _, _, _ := faultHarness(t)
				stepN(t, harness, phase.steps)
				approvals := len(auditActions(harness, "rollout.pod.delete_approved"))
				test.configure(harness)
				result, err := harness.reconcile(t)
				if err != nil {
					t.Fatal(err)
				}
				requireStatus(t, harness, phase.want, "barrier_unknown")
				if result.RequeueAfter != PollInterval || harness.runtime.deleteCalls != 0 || len(auditActions(harness, "rollout.pod.delete_approved")) != approvals {
					t.Fatalf("lost evidence still progressed: result=%+v deletes=%d", result, harness.runtime.deleteCalls)
				}
				test.heal(harness)
				stepN(t, harness, 1)
				wantPhase := PhaseDeleteApproved
				wantDeletes := 0
				if phase.want == PhaseDeleteApproved {
					wantPhase, wantDeletes = PhaseAwaitingReplacement, 1
				}
				if harness.status.status.Phase != wantPhase || harness.runtime.deleteCalls != wantDeletes {
					t.Fatalf("healed evidence: phase=%s deletes=%d, want %s/%d", harness.status.status.Phase, harness.runtime.deleteCalls, wantPhase, wantDeletes)
				}
			})
		}
	}
}

// Traceability: DEP-18; docs/contracts/rollout-controller-v1.md §2.7, §3 "Kubernetes mutation 결과 불명".
func TestDeleteOutcomesAreClassifiedFromObservedPodNotFromDeleteResult(t *testing.T) {
	t.Run("Pod already gone before DELETE", func(t *testing.T) {
		harness, _, _, _ := faultHarness(t)
		stepN(t, harness, 4)
		harness.runtime.workload.Pods = harness.runtime.workload.Pods[1:]
		result, err := harness.reconcile(t)
		if err != nil || result.Phase != PhaseAwaitingReplacement || result.RequeueAfter != PollInterval || harness.runtime.deleteCalls != 0 {
			t.Fatalf("absent Pod: result=%+v err=%v deletes=%d", result, err, harness.runtime.deleteCalls)
		}
	})
	t.Run("workload unreadable after DELETE keeps approval", func(t *testing.T) {
		harness, runtime, _, _ := faultHarness(t)
		stepN(t, harness, 4)
		runtime.failInspectIn = 2
		result, err := harness.reconcile(t)
		if err != nil || result.Phase != PhaseDeleteApproved || result.RequeueAfter != PollInterval || harness.runtime.deleteCalls != 1 {
			t.Fatalf("unreadable workload: result=%+v err=%v deletes=%d", result, err, harness.runtime.deleteCalls)
		}
		if harness.status.status.Phase != PhaseDeleteApproved || harness.status.status.CurrentOrdinal != 0 {
			t.Fatalf("status advanced without observing the Pod: %+v", harness.status.status)
		}
		stepN(t, harness, 1)
		if harness.status.status.Phase != PhaseAwaitingReplacement || harness.runtime.deleteCalls != 1 {
			t.Fatalf("next poll: %+v deletes=%d, want the terminating Pod observed without another DELETE", harness.status.status, harness.runtime.deleteCalls)
		}
	})
	t.Run("DELETE error that took effect", func(t *testing.T) {
		harness, runtime, _, _ := faultHarness(t)
		stepN(t, harness, 4)
		runtime.deleteErrAfterEffect = errors.New("connection reset after DELETE")
		stepN(t, harness, 1)
		requireStatus(t, harness, PhaseAwaitingReplacement, "")
		if harness.runtime.deleteCalls != 1 {
			t.Fatalf("deletes=%d", harness.runtime.deleteCalls)
		}
	})
	t.Run("DELETE accepted but Pod UID remains", func(t *testing.T) {
		harness, runtime, _, _ := faultHarness(t)
		stepN(t, harness, 4)
		runtime.swallowDelete = true
		stepN(t, harness, 1)
		requireStatus(t, harness, PhaseDeleteApproved, "delete_unconfirmed")
		if !strings.Contains(harness.status.status.FailureReason, "accepted") || harness.runtime.deleteCalls != 1 || harness.status.status.CurrentOrdinal != 0 {
			t.Fatalf("accepted-but-present DELETE: %+v deletes=%d", harness.status.status, harness.runtime.deleteCalls)
		}
	})
}

func TestReplacementIsNotAcceptedUntilEveryEvidenceSourceAgrees(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*rolloutTestHarness)
	}{
		{"old Pod still terminating", func(*rolloutTestHarness) {}},
		{"replacement not Ready", func(h *rolloutTestHarness) {
			replaceWithTarget(h, 0)
			h.runtime.workload.Pods[0].Ready = false
		}},
		{"readiness endpoint fails", func(h *rolloutTestHarness) {
			replaceWithTarget(h, 0)
			h.readiness.fail = true
		}},
		{"broker profile still old", func(h *rolloutTestHarness) {
			replaceWithTarget(h, 0)
			h.broker.features.Members[0].ReleaseVersion = "1.1.0"
		}},
		{"StatefulSet recreated Pod on old image", func(h *rolloutTestHarness) {
			pod := &h.runtime.workload.Pods[0]
			pod.UID, pod.ResourceVersion, pod.Deleting, pod.Ready = "recreated-uid", "recreated-rv", false, true
		}},
		{"runtime digest is another image", func(h *rolloutTestHarness) {
			replaceWithTarget(h, 0)
			h.runtime.workload.Pods[0].ImageID = "containerd://sha256:" + strings.Repeat("e", 64)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness, _, _, _ := faultHarness(t)
			stepN(t, harness, 5)
			requireStatus(t, harness, PhaseAwaitingReplacement, "")
			test.configure(harness)
			result, err := harness.reconcile(t)
			if err != nil {
				t.Fatal(err)
			}
			status := harness.status.status
			if status.Phase != PhaseAwaitingReplacement || status.CurrentOrdinal != 0 || len(status.CompletedOrdinals) != 0 ||
				result.RequeueAfter != PollInterval || harness.runtime.released != 0 || harness.runtime.deleteCalls != 1 {
				t.Fatalf("replacement accepted early: result=%+v status=%+v deletes=%d", result, status, harness.runtime.deleteCalls)
			}
		})
	}
}

func TestReplacementOfAlreadyAdvancedOrdinalFailsInsteadOfAdvancingTwice(t *testing.T) {
	harness, _, _, _ := faultHarness(t)
	stepN(t, harness, 5)
	harness.status.status.CompletedOrdinals = []int32{0}
	replaceWithTarget(harness, 0)
	stepN(t, harness, 1)
	requireStatus(t, harness, PhaseFailed, "ordinal_completed_twice")
	if harness.status.status.CurrentOrdinal != 0 {
		t.Fatalf("ordinal advanced: %+v", harness.status.status)
	}
}

func TestLastReplacementCompletesRolloutAndRetriesTargetRelease(t *testing.T) {
	harness, runtime, _, _ := faultHarness(t)
	stepN(t, harness, 1)
	status := harness.status.status
	status.Phase, status.CurrentOrdinal = PhaseAwaitingReplacement, 2
	status.CurrentPodName, status.CurrentPodUID, status.CurrentPodRV = "broker-2", "pod-uid-2", "pod-rv-2"
	status.CompletedOrdinals = []int32{0, 1}
	harness.status.status = status
	replaceWithTarget(harness, 2)
	runtime.releaseErr = errors.New("lease API unavailable")
	result, err := harness.reconcile(t)
	if err != nil || result.Phase != PhaseCompleted || result.RequeueAfter != PollInterval {
		t.Fatalf("completion with failed release: result=%+v err=%v", result, err)
	}
	if !slices.Equal(harness.status.status.CompletedOrdinals, []int32{0, 1, 2}) || harness.status.status.Phase != PhaseCompleted {
		t.Fatalf("completion was not persisted before releasing the target: %+v", harness.status.status)
	}
	runtime.releaseErr = nil
	result, err = harness.reconcile(t)
	if err != nil || result.Phase != PhaseCompleted || result.RequeueAfter != 0 || harness.runtime.released != 1 {
		t.Fatalf("retried release: result=%+v err=%v released=%d", result, err, harness.runtime.released)
	}
}

func upgradeAllPods(harness *rolloutTestHarness) {
	for ordinal := range harness.runtime.workload.Pods {
		pod := &harness.runtime.workload.Pods[ordinal]
		pod.Image = harness.request.TargetImage
		pod.ImageID = "containerd://" + digestFromImage(harness.request.TargetImage)
		harness.broker.features.Members[ordinal].ReleaseVersion = harness.request.TargetVersion
	}
}

func TestAlreadyUpgradedClusterCompletesWithoutDrainAndRetriesTargetRelease(t *testing.T) {
	harness, runtime, _, _ := faultHarness(t)
	upgradeAllPods(harness)
	stepN(t, harness, 2)
	if harness.status.status.CurrentOrdinal != 2 || !slices.Equal(harness.status.status.CompletedOrdinals, []int32{0, 1}) {
		t.Fatalf("already-upgraded ordinals did not advance: %+v", harness.status.status)
	}
	runtime.releaseErr = errors.New("lease API unavailable")
	if _, err := harness.reconcile(t); !errors.Is(err, runtime.releaseErr) {
		t.Fatalf("release failure after completion was hidden: %v", err)
	}
	requireStatus(t, harness, PhaseCompleted, "")
	result, err := harness.reconcile(t)
	if err != nil || result.RequeueAfter != PollInterval || result.Phase != PhaseCompleted {
		t.Fatalf("completed rollout release retry: result=%+v err=%v", result, err)
	}
	runtime.releaseErr = nil
	result, err = harness.reconcile(t)
	if err != nil || result.RequeueAfter != 0 || harness.runtime.released != 1 {
		t.Fatalf("release success: result=%+v err=%v released=%d", result, err, harness.runtime.released)
	}
	if !slices.Equal(harness.status.status.CompletedOrdinals, []int32{0, 1, 2}) || harness.broker.starts != 0 || harness.broker.gets != 0 ||
		harness.runtime.deleteCalls != 0 || harness.audit.calls != 0 {
		t.Fatalf("no-op upgrade touched the cluster: %+v starts=%d gets=%d deletes=%d audit=%d",
			harness.status.status, harness.broker.starts, harness.broker.gets, harness.runtime.deleteCalls, harness.audit.calls)
	}
}

func TestAlreadyUpgradedOrdinalCannotBeCompletedTwice(t *testing.T) {
	harness, _, _, _ := faultHarness(t)
	upgradeAllPods(harness)
	stepN(t, harness, 1)
	harness.status.status.CurrentOrdinal = 0
	stepN(t, harness, 1)
	requireStatus(t, harness, PhaseFailed, "ordinal_completed_twice")
}

func TestPreflightAcceptsKubeletImageIDReferenceForms(t *testing.T) {
	oldDigest := "sha256:" + strings.Repeat("a", 64)
	for name, imageID := range map[string]string{
		"bare digest":      oldDigest,
		"docker-pullable":  "docker-pullable://registry.test/narumq@" + oldDigest,
		"containerd store": "containerd://" + oldDigest,
	} {
		t.Run(name, func(t *testing.T) {
			harness, _, _, _ := faultHarness(t)
			for ordinal := range harness.runtime.workload.Pods {
				harness.runtime.workload.Pods[ordinal].ImageID = imageID
			}
			result, err := harness.reconcile(t)
			if err != nil || result.Phase != PhaseDraining || harness.status.status.FailureCode != "" {
				t.Fatalf("imageID %q rejected: result=%+v status=%+v err=%v", imageID, result, harness.status.status, err)
			}
		})
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.1, §3 "Preflight".
func TestPreflightBlocksOnUnverifiableOrInconsistentClusterEvidence(t *testing.T) {
	oldDigest := "sha256:" + strings.Repeat("a", 64)
	cases := []struct {
		name      string
		configure func(h *rolloutTestHarness, runtime *faultRuntime, broker *faultBroker, images rolloutTestImages)
	}{
		{"workload read fails", func(_ *rolloutTestHarness, r *faultRuntime, _ *faultBroker, _ rolloutTestImages) { r.failInspectIn = 1 }},
		{"update strategy is RollingUpdate", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.UpdateStrategy = "RollingUpdate"
		}},
		{"two replicas", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Replicas = 2
		}},
		{"generation not observed", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.ObservedGeneration = h.runtime.workload.Generation - 1
		}},
		{"workload name differs", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Name = "other-broker"
		}},
		{"workload has no UID", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.UID = ""
		}},
		{"target image is a tag", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.request.TargetImage = "registry.test/narumq:latest"
		}},
		{"target version is invalid", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.request.TargetVersion = "latest"
		}},
		{"target features are unsorted", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.request.TargetFeatures = []string{"zeta", "alpha"}
		}},
		{"template image differs from CR", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Containers[0].Image = h.runtime.workload.Pods[0].Image
		}},
		{"two narumq containers", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Containers = append(h.runtime.workload.Containers, h.runtime.workload.Containers[0])
		}},
		{"registry secret not referenced", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.ImagePullSecrets = nil
		}},
		{"target OCI metadata unavailable", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, images rolloutTestImages) {
			delete(images, h.request.TargetImage)
		}},
		{"target OCI version label differs", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, images rolloutTestImages) {
			metadata := images[h.request.TargetImage]
			metadata.Version = "1.9.0"
			images[h.request.TargetImage] = metadata
		}},
		{"target OCI platform missing", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, images rolloutTestImages) {
			metadata := images[h.request.TargetImage]
			metadata.OS = ""
			images[h.request.TargetImage] = metadata
		}},
		{"target OCI features differ", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, images rolloutTestImages) {
			metadata := images[h.request.TargetImage]
			metadata.Features = nil
			images[h.request.TargetImage] = metadata
		}},
		{"broker profiles unavailable", func(_ *rolloutTestHarness, _ *faultRuntime, b *faultBroker, _ rolloutTestImages) {
			b.featuresErr = errors.New("management API timeout")
		}},
		{"profile node ID out of range", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.broker.features.Members[2].NodeID = 4
		}},
		{"duplicate profile node ID", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.broker.features.Members[2].NodeID = 1
		}},
		{"profile missing for a node", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.broker.features.Members = h.broker.features.Members[:2]
		}},
		{"member outside compatibility window", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.broker.features.Members[1].ReleaseVersion = "2.0.0"
		}},
		{"active feature unsupported by members", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.broker.features.ActiveFeatures = []string{"compatibility-probe", "zzz-new-feature"}
		}},
		{"Pod name does not match ordinal", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[1].Name = "broker-7"
		}},
		{"Pod without UID", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[1].UID = ""
		}},
		{"two Pods for one ordinal", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods = append(h.runtime.workload.Pods, h.runtime.workload.Pods[0])
		}},
		{"ordinal without Pod", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods = h.runtime.workload.Pods[:2]
		}},
		{"unrelated Pod terminating", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[1].Deleting = true
		}},
		{"Pod image is a tag", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[0].Image = "registry.test/narumq:latest"
		}},
		{"Pod image digest is upper case", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[0].Image = "registry.test/narumq@sha256:" + strings.Repeat("A", 64)
		}},
		{"Pod OCI metadata unavailable", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, images rolloutTestImages) {
			delete(images, h.runtime.workload.Pods[0].Image)
		}},
		{"Pod runtime digest is another image", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[0].ImageID = "containerd://sha256:" + strings.Repeat("e", 64)
		}},
		{"Pod runtime digest is truncated", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[0].ImageID = "containerd://sha256:" + oldDigest[7:20]
		}},
		{"Pod runtime digest is upper case", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[0].ImageID = "containerd://sha256:" + strings.ToUpper(oldDigest[7:])
		}},
		{"Pod runtime digest is empty", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[0].ImageID = ""
		}},
		{"Pod platform differs from target", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[1].Architecture = "amd64"
		}},
		{"Pod platform unknown", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.runtime.workload.Pods[1].OS = ""
		}},
		{"Pod OCI labels disagree with broker profile", func(h *rolloutTestHarness, _ *faultRuntime, _ *faultBroker, _ rolloutTestImages) {
			h.broker.features.Members[1].ReleaseVersion = "1.2.0"
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness, runtime, broker, images := faultHarness(t)
			test.configure(harness, runtime, broker, images)
			result, err := harness.reconcile(t)
			if err != nil {
				t.Fatal(err)
			}
			requireStatus(t, harness, PhasePreflight, "preflight_blocked")
			conditions := harness.status.status.Conditions
			if result.RequeueAfter != PollInterval || result.Phase != PhasePreflight || len(conditions) != 1 ||
				conditions[0].Status != "False" || conditions[0].Reason != "preflight_blocked" {
				t.Fatalf("blocked preflight: result=%+v conditions=%+v", result, conditions)
			}
			if harness.broker.gets != 0 || harness.broker.starts != 0 || harness.runtime.deleteCalls != 0 || harness.status.status.DrainOperationID != "" ||
				harness.status.status.DrainIntentAt != nil {
				t.Fatalf("blocked preflight started a drain or deleted: gets=%d starts=%d deletes=%d status=%+v",
					harness.broker.gets, harness.broker.starts, harness.runtime.deleteCalls, harness.status.status)
			}
		})
	}
}
