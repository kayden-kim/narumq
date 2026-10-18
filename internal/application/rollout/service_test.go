package rollout

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	auditapp "narumq/internal/application/audit"
	portsaudit "narumq/internal/application/ports/audit"
)

type rolloutTestClock struct{ now time.Time }

func (clock *rolloutTestClock) Now() time.Time { return clock.now }

type rolloutTestStatus struct {
	resourceVersion string
	status          StatusDTO
	failNext        bool
	writes          int
}

func (store *rolloutTestStatus) CompareAndSwapStatus(_ context.Context, rollout RolloutDTO, expected string, status StatusDTO) (string, error) {
	if store.failNext {
		store.failNext = false
		return "", errors.New("injected status write loss")
	}
	if rollout.ResourceVersion != expected || expected != store.resourceVersion {
		return "", errors.New("resourceVersion conflict")
	}
	store.status = persistedStatus(status)
	store.writes++
	store.resourceVersion = fmt.Sprintf("rv-%d", len(status.CompletedOrdinals)+len(status.Conditions)+storeRevision(store.resourceVersion))
	return store.resourceVersion, nil
}

// persistedStatus mirrors the Kubernetes status round trip: metav1.Time keeps RFC3339 seconds.
func persistedStatus(status StatusDTO) StatusDTO {
	seconds := func(value *time.Time) *time.Time {
		if value == nil {
			return nil
		}
		persisted := value.UTC().Truncate(time.Second)
		return &persisted
	}
	status.DrainIntentAt, status.DrainStartedAt = seconds(status.DrainIntentAt), seconds(status.DrainStartedAt)
	status.DrainDeadlineAt, status.SafeToStopObservedAt = seconds(status.DrainDeadlineAt), seconds(status.SafeToStopObservedAt)
	status.LastTransitionTime = status.LastTransitionTime.UTC().Truncate(time.Second)
	status.Conditions = slices.Clone(status.Conditions)
	for index := range status.Conditions {
		status.Conditions[index].LastTransitionTime = status.Conditions[index].LastTransitionTime.UTC().Truncate(time.Second)
	}
	return status
}

func storeRevision(value string) int {
	var revision int
	_, _ = fmt.Sscanf(value, "rv-%d", &revision)
	if revision == 0 {
		return 1
	}
	return revision + 1
}

type rolloutTestRuntime struct {
	workload       WorkloadDTO
	claimed        bool
	claimCalls     int
	released       int
	deleteCalls    int
	deleteOrdinals []int32
	deleteErr      error
}

func (runtime *rolloutTestRuntime) ClaimTarget(context.Context, TargetClaimDTO) (bool, error) {
	runtime.claimCalls++
	return runtime.claimed, nil
}

func (runtime *rolloutTestRuntime) ReleaseTarget(context.Context, TargetClaimDTO) error {
	runtime.released++
	return nil
}

func (runtime *rolloutTestRuntime) InspectWorkload(context.Context, RolloutDTO) (WorkloadDTO, error) {
	workload := runtime.workload
	workload.Containers = slices.Clone(workload.Containers)
	workload.ImagePullSecrets = slices.Clone(workload.ImagePullSecrets)
	workload.Pods = slices.Clone(workload.Pods)
	return workload, nil
}

func (runtime *rolloutTestRuntime) DeletePod(_ context.Context, request DeletePodDTO) error {
	for index := range runtime.workload.Pods {
		pod := &runtime.workload.Pods[index]
		if pod.Name != request.Name {
			continue
		}
		if pod.UID != request.UID || pod.ResourceVersion != request.ResourceVersion {
			return errors.New("delete UID or resourceVersion precondition failed")
		}
		runtime.deleteCalls++
		runtime.deleteOrdinals = append(runtime.deleteOrdinals, pod.Ordinal)
		if runtime.deleteErr != nil {
			return runtime.deleteErr
		}
		pod.Deleting = true
		pod.ResourceVersion = "terminating-" + pod.Name
		return nil
	}
	return errors.New("requested Pod is absent")
}

type rolloutTestImages map[string]ImageMetadataDTO

func (images rolloutTestImages) ReadImageMetadata(_ context.Context, _, image, _ string) (ImageMetadataDTO, error) {
	metadata, found := images[image]
	if !found {
		return ImageMetadataDTO{}, errors.New("unknown digest")
	}
	return metadata, nil
}

type rolloutTestBroker struct {
	features          ClusterFeaturesDTO
	operations        map[string]DrainObservationDTO
	starts            int
	gets              int
	cancels           int
	safe              bool
	incomplete        bool
	getErr            error
	advanceOnGet      func()
	onSafeObservation func(*DrainObservationDTO)
	onGet             func(*DrainObservationDTO)
}

func (broker *rolloutTestBroker) ClusterFeatures(context.Context, string, string, string) (ClusterFeaturesDTO, error) {
	return broker.features, nil
}

func (broker *rolloutTestBroker) GetDrain(_ context.Context, _, _, _ string, nodeID uint32, key string) (DrainObservationDTO, bool, error) {
	broker.gets++
	if broker.getErr != nil {
		return DrainObservationDTO{}, false, broker.getErr
	}
	observation, found := broker.operations[key]
	if !found {
		return DrainObservationDTO{}, false, nil
	}
	if broker.safe {
		observation.Phase, observation.State = "COMPLETED", "SAFE_TO_STOP"
		observation.SafeToStop = new(true)
		if broker.incomplete {
			observation.ConnectionsRemaining = 1
		}
	}
	if broker.safe && broker.onSafeObservation != nil {
		broker.onSafeObservation(&observation)
		broker.operations[key] = observation
	}
	if broker.advanceOnGet != nil {
		broker.advanceOnGet()
		broker.advanceOnGet = nil
	}
	if broker.onGet != nil {
		broker.onGet(&observation)
		broker.operations[key] = observation
	}
	if observation.NodeID != nodeID {
		return DrainObservationDTO{}, false, errors.New("node mismatch")
	}
	return observation, true, nil
}

func (broker *rolloutTestBroker) StartDrain(ctx context.Context, _, _, _ string, nodeID uint32, key string) (DrainObservationDTO, error) {
	broker.starts++
	started := time.Now().UTC()
	if testClock, ok := ctx.Value(rolloutTestClockKey{}).(*rolloutTestClock); ok {
		started = testClock.Now().UTC()
	}
	observation := DrainObservationDTO{
		OperationID: fmt.Sprintf("operation-%d", nodeID), NodeID: nodeID,
		Phase: "TRANSFERRING_SESSIONS", State: "DRAINING", StartedAt: started,
		DeadlineAt: started.Add(DefaultDrainTimeout), SafeToStop: new(false),
	}
	broker.operations[key] = observation
	return observation, nil
}

func (broker *rolloutTestBroker) CancelDrain(_ context.Context, _, _, _ string, nodeID uint32, key string) (DrainObservationDTO, error) {
	broker.cancels++
	observation, found := broker.operations[key]
	if !found || observation.NodeID != nodeID {
		return DrainObservationDTO{}, errors.New("drain operation not found")
	}
	observation.Phase, observation.State = "CANCELLED", "READY"
	observation.SafeToStop = new(false)
	broker.operations[key] = observation
	return observation, nil
}

type rolloutTestClockKey struct{}

type rolloutTestReadiness struct {
	fail  bool
	calls int
}

func (probe *rolloutTestReadiness) Ready(context.Context, string) error {
	probe.calls++
	if probe.fail {
		return errors.New("not ready")
	}
	return nil
}

type rolloutTestAudit struct {
	readyErr   error
	publishErr error
	calls      int
	events     map[string]portsaudit.AuditEventDTO
}

func (publisher *rolloutTestAudit) Ready() error { return publisher.readyErr }
func (publisher *rolloutTestAudit) PublishOnce(event portsaudit.AuditEventDTO) error {
	publisher.calls++
	if err := auditapp.ValidateEvent(event); err != nil {
		return err
	}
	if publisher.publishErr != nil {
		return publisher.publishErr
	}
	if publisher.events == nil {
		publisher.events = make(map[string]portsaudit.AuditEventDTO)
	}
	publisher.events[event.EventID] = event
	return nil
}

type rolloutTestHarness struct {
	service   *Service
	status    *rolloutTestStatus
	runtime   *rolloutTestRuntime
	broker    *rolloutTestBroker
	readiness *rolloutTestReadiness
	audit     *rolloutTestAudit
	clock     *rolloutTestClock
	request   RolloutDTO
}

func newRolloutTestHarness(t *testing.T) *rolloutTestHarness {
	t.Helper()
	oldDigest := "sha256:" + strings.Repeat("a", 64)
	targetDigest := "sha256:" + strings.Repeat("b", 64)
	oldImage := "registry.test/narumq@" + oldDigest
	targetImage := "registry.test/narumq@" + targetDigest
	features := []string{"compatibility-probe"}
	pods := make([]PodDTO, 3)
	members := make([]MemberProfileDTO, 3)
	for ordinal := range 3 {
		pods[ordinal] = PodDTO{
			Name: fmt.Sprintf("broker-%d", ordinal), Ordinal: int32(ordinal), UID: fmt.Sprintf("pod-uid-%d", ordinal),
			ResourceVersion: fmt.Sprintf("pod-rv-%d", ordinal), Image: oldImage,
			ImageID: "containerd://" + oldDigest, PodIP: fmt.Sprintf("10.0.0.%d", ordinal+1),
			OS: "linux", Architecture: "arm64", Ready: true,
		}
		members[ordinal] = MemberProfileDTO{NodeID: uint32(ordinal + 1), ReleaseVersion: "1.1.0", Features: slices.Clone(features)}
	}
	workload := WorkloadDTO{
		Name: "broker", UID: "statefulset-uid", ResourceVersion: "statefulset-rv", Generation: 3,
		ObservedGeneration: 3, Replicas: 3, UpdateStrategy: "OnDelete",
		Containers: []ContainerDTO{{Name: "narumq", Image: targetImage}}, ImagePullSecrets: []string{"registry"}, Pods: pods,
	}
	clock := &rolloutTestClock{now: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)}
	status := &rolloutTestStatus{resourceVersion: "1"}
	runtime := &rolloutTestRuntime{workload: workload, claimed: true}
	broker := &rolloutTestBroker{
		features:   ClusterFeaturesDTO{ActiveFeatures: slices.Clone(features), Members: members},
		operations: make(map[string]DrainObservationDTO), safe: true,
	}
	readiness := &rolloutTestReadiness{}
	publisher := &rolloutTestAudit{}
	service, err := NewService(ServiceConfig{ProducerVersion: "1.2.3", ProducerInstanceID: "controller-0"}, status, runtime,
		rolloutTestImages{
			oldImage:    {Image: oldImage, ManifestDigest: oldDigest, ConfigDigest: "sha256:" + strings.Repeat("c", 64), OS: "linux", Architecture: "arm64", Version: "1.1.0", Features: slices.Clone(features)},
			targetImage: {Image: targetImage, ManifestDigest: targetDigest, ConfigDigest: "sha256:" + strings.Repeat("d", 64), OS: "linux", Architecture: "arm64", Version: "1.2.0", Features: slices.Clone(features)},
		}, broker, readiness, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}
	request := RolloutDTO{
		Namespace: "narumq", Name: "upgrade", UID: "rollout-uid", Generation: 1, ResourceVersion: "1",
		WorkloadName: "broker", TargetImage: targetImage, TargetVersion: "1.2.0", TargetFeatures: slices.Clone(features),
		RegistrySecretName: "registry", ManagementEndpoint: "http://narumq-management.narumq.svc:8080",
		ManagementSecret: "operator-api-key", ManagementSecretName: "management-operator", RetryToken: "attempt-1",
	}
	return &rolloutTestHarness{service: service, status: status, runtime: runtime, broker: broker, readiness: readiness, audit: publisher, clock: clock, request: request}
}

func (harness *rolloutTestHarness) reconcile(t *testing.T) (ReconcileResultDTO, error) {
	t.Helper()
	harness.request.Status = harness.status.status
	harness.request.ResourceVersion = harness.status.resourceVersion
	ctx := context.WithValue(context.Background(), rolloutTestClockKey{}, harness.clock)
	result, err := harness.service.Reconcile(ctx, harness.request)
	return result, err
}

func (harness *rolloutTestHarness) serviceWithPersistedState(t *testing.T) {
	t.Helper()
	service, err := NewService(ServiceConfig{ProducerVersion: "1.2.3", ProducerInstanceID: "controller-0"}, harness.status, harness.runtime,
		rolloutTestImages{
			"registry.test/narumq@sha256:" + strings.Repeat("a", 64): {Image: "registry.test/narumq@sha256:" + strings.Repeat("a", 64), ManifestDigest: "sha256:" + strings.Repeat("a", 64), OS: "linux", Architecture: "arm64", Version: "1.1.0", Features: []string{"compatibility-probe"}},
			"registry.test/narumq@sha256:" + strings.Repeat("b", 64): {Image: harness.request.TargetImage, ManifestDigest: "sha256:" + strings.Repeat("b", 64), OS: "linux", Architecture: "arm64", Version: "1.2.0", Features: []string{"compatibility-probe"}},
		}, harness.broker, harness.readiness, harness.audit, harness.clock)
	if err != nil {
		t.Fatal(err)
	}
	harness.service = service
}

func replaceWithTarget(harness *rolloutTestHarness, ordinal int32) {
	pod := &harness.runtime.workload.Pods[ordinal]
	pod.UID = fmt.Sprintf("replacement-uid-%d", ordinal)
	pod.ResourceVersion = fmt.Sprintf("replacement-rv-%d", ordinal)
	pod.Image = harness.request.TargetImage
	pod.ImageID = "containerd://" + strings.TrimPrefix(harness.request.TargetImage[strings.LastIndex(harness.request.TargetImage, "@")+1:], "")
	pod.Deleting, pod.Ready = false, true
	harness.broker.features.Members[ordinal].ReleaseVersion = harness.request.TargetVersion
}

func TestPreflightAcceptsRuntimeConfigDigestBoundToOCIManifest(t *testing.T) {
	harness := newRolloutTestHarness(t)
	configDigest := "sha256:" + strings.Repeat("c", 64)
	for ordinal := range harness.runtime.workload.Pods {
		harness.runtime.workload.Pods[ordinal].ImageID = "containerd://" + configDigest
	}
	if _, err := harness.reconcile(t); err != nil {
		t.Fatalf("accept runtime config digest linked to immutable OCI manifest: %v", err)
	}
	if harness.runtime.deleteCalls != 0 {
		t.Fatalf("preflight config digest check deleted Pods before drain approval: %d", harness.runtime.deleteCalls)
	}
}

func TestPreflightAcceptsKubeletImageReferenceDigest(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for ordinal := range harness.runtime.workload.Pods {
		harness.runtime.workload.Pods[ordinal].ImageID = harness.runtime.workload.Pods[ordinal].Image
	}
	if result, err := harness.reconcile(t); err != nil || result.Phase != PhaseDraining {
		t.Fatalf("preflight rejected kubelet imageID manifest reference: result=%+v phase=%s err=%v", result, harness.status.status.Phase, err)
	}
	if harness.runtime.deleteCalls != 0 {
		t.Fatalf("preflight check deleted a Pod: %d", harness.runtime.deleteCalls)
	}
}

func TestRolloutSerializesThreeNodeLifecycleUntilReplacementReadiness(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for ordinal := int32(0); ordinal < 3; ordinal++ {
		for range 8 {
			result, err := harness.reconcile(t)
			if err != nil {
				t.Fatalf("reconcile ordinal %d phase %s: %v", ordinal, harness.status.status.Phase, err)
			}
			if result.Phase == PhaseDrainSafe || result.Phase == PhaseDeleteApproved {
				if result.RequeueAfter != PollInterval {
					t.Fatalf("active phase %s lacks a persisted-state polling interval: %s", result.Phase, result.RequeueAfter)
				}
			}
			if result.Phase == PhaseAwaitingReplacement {
				break
			}
		}
		if harness.status.status.Phase != PhaseAwaitingReplacement || harness.status.status.CurrentOrdinal != ordinal {
			t.Fatalf("ordinal %d did not stop at replacement gate: status=%+v", ordinal, harness.status.status)
		}
		if len(harness.runtime.deleteOrdinals) != int(ordinal+1) || harness.runtime.deleteOrdinals[ordinal] != ordinal {
			t.Fatalf("delete sequence before replacement %d = %v", ordinal, harness.runtime.deleteOrdinals)
		}
		if ordinal < 2 && harness.status.status.CurrentOrdinal != ordinal {
			t.Fatalf("next ordinal advanced before replacement readiness: status=%+v", harness.status.status)
		}
		replaceWithTarget(harness, ordinal)
		if _, err := harness.reconcile(t); err != nil {
			t.Fatalf("accept ready replacement ordinal %d: %v", ordinal, err)
		}
		wantPhase := PhasePreflight
		if ordinal == 2 {
			wantPhase = PhaseCompleted
		}
		if harness.status.status.Phase != wantPhase {
			t.Fatalf("replacement ordinal %d phase = %s, want %s", ordinal, harness.status.status.Phase, wantPhase)
		}
	}
	if harness.runtime.deleteCalls != 3 || !slices.Equal(harness.runtime.deleteOrdinals, []int32{0, 1, 2}) || harness.runtime.released != 1 {
		t.Fatalf("rollout lifecycle deletes=%v calls=%d target releases=%d", harness.runtime.deleteOrdinals, harness.runtime.deleteCalls, harness.runtime.released)
	}
	if !slices.Equal(harness.status.status.CompletedOrdinals, []int32{0, 1, 2}) || len(harness.audit.events) < 3 {
		t.Fatalf("completed ordinals/audit events = %v/%d", harness.status.status.CompletedOrdinals, len(harness.audit.events))
	}
	approved := 0
	for _, event := range harness.audit.events {
		switch event.Action {
		case "rollout.pod.delete_approved":
			approved++
			if event.RequestID != event.EventID || event.Actor.Subject != "narumq-rollout-controller" ||
				event.Target.Type != "broker_pod" || event.Target.ID == "" || event.Reason != "safe_to_stop" {
				t.Fatalf("approval event does not match rollout audit contract: %+v", event)
			}
			if _, exists := event.Details["pendingWAL"]; !exists {
				t.Fatalf("approval event omits persisted drain evidence: %+v", event.Details)
			}
		case "rollout.pod.delete_denied":
			if event.Result != portsaudit.AuditResultDenied || event.Target.Type != "broker_pod" || event.Target.ID == "" {
				t.Fatalf("denial event does not match rollout audit contract: %+v", event)
			}
		default:
			t.Fatalf("unexpected rollout audit action %q", event.Action)
		}
	}
	if approved != 3 {
		t.Fatalf("approved delete events = %d, want one per ordinal", approved)
	}
}

func TestChangingSafeBarrierIsPersistedBeforeUIDRVDelete(t *testing.T) {
	harness := newRolloutTestHarness(t)
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	harness.broker.safe = true
	harness.broker.onSafeObservation = func(observation *DrainObservationDTO) {
		observation.SessionsReady++
		observation.SessionsTakenOver++
	}
	for range 8 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatalf("reconcile changing complete SAFE_TO_STOP evidence: %v", err)
		}
		if harness.runtime.deleteCalls != 0 {
			break
		}
	}
	if harness.runtime.deleteCalls != 1 || harness.status.status.Phase != PhaseAwaitingReplacement {
		t.Fatalf("complete changing barrier did not reach the UID/RV delete gate: deletes=%d status=%+v", harness.runtime.deleteCalls, harness.status.status)
	}
	if harness.status.status.DrainSessionsReady < 2 || harness.status.status.DrainSessionsTakenOver != harness.status.status.DrainSessionsReady ||
		harness.status.status.DrainSafeToStop == nil || !*harness.status.status.DrainSafeToStop {
		t.Fatalf("latest full barrier was not durably persisted before delete: %+v", harness.status.status)
	}
}

func TestUnknownIncompleteAndLateDrainEvidenceNeverDeletes(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*rolloutTestHarness)
		want      string
	}{
		{name: "unknown GET", configure: func(h *rolloutTestHarness) { h.broker.getErr = errors.New("API unavailable") }, want: PhaseDraining},
		{name: "incomplete barrier", configure: func(h *rolloutTestHarness) { h.broker.incomplete = true }, want: PhaseDraining},
		{name: "SAFE_TO_STOP returned at deadline", configure: func(h *rolloutTestHarness) {
			h.broker.advanceOnGet = func() { h.clock.now = h.clock.now.Add(DefaultDrainTimeout) }
		}, want: PhaseFailed},
		{name: "drain FAILED", configure: func(h *rolloutTestHarness) {
			h.broker.onGet = func(obs *DrainObservationDTO) {
				obs.Phase = "FAILED"
				obs.FailureCode = "drain_timeout"
				obs.FailurePresent = true
				obs.SafeToStop = new(false)
			}
		}, want: PhaseFailed},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newRolloutTestHarness(t)
			harness.broker.safe = false
			if _, err := harness.reconcile(t); err != nil || harness.status.status.Phase != PhaseDraining {
				t.Fatalf("start drain intent: status=%+v err=%v", harness.status.status, err)
			}
			if _, err := harness.reconcile(t); err != nil || harness.status.status.DrainOperationID == "" {
				t.Fatalf("issue start drain: status=%+v err=%v", harness.status.status, err)
			}
			harness.broker.safe = true
			testCase.configure(harness)
			if _, err := harness.reconcile(t); err != nil {
				t.Fatalf("poll unknown barrier: %v", err)
			}
			if harness.status.status.Phase != testCase.want || harness.runtime.deleteCalls != 0 || harness.status.status.CurrentOrdinal != 0 {
				t.Fatalf("unsafe drain advanced/deleted: phase=%s want=%s ordinal=%d deletes=%d", harness.status.status.Phase, testCase.want, harness.status.status.CurrentOrdinal, harness.runtime.deleteCalls)
			}
		})
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.9, §3 "Drain failure / timeout", §5 rollout.failed.
func TestDrainFailedShowsSafeOperatorActionInStatusAndAudit(t *testing.T) {
	harness := newRolloutTestHarness(t)
	harness.broker.safe = false
	for range 2 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	harness.broker.onGet = func(observation *DrainObservationDTO) {
		observation.Phase, observation.FailureCode, observation.FailurePresent = "FAILED", "drain_timeout", true
	}
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	status := harness.status.status
	if status.Phase != PhaseFailed || status.FailureCode != "drain_timeout" || harness.runtime.deleteCalls != 0 {
		t.Fatalf("FAILED drain: phase=%s code=%s deletes=%d", status.Phase, status.FailureCode, harness.runtime.deleteCalls)
	}
	want := "broker drain entered FAILED; " + failedOperatorAction
	if status.FailureReason != want || len(status.Conditions) == 0 || status.Conditions[len(status.Conditions)-1].Message != want {
		t.Fatalf("status does not name the operator action: reason=%q conditions=%+v", status.FailureReason, status.Conditions)
	}
	if !strings.Contains(failedOperatorAction, "spec.retryToken") || !strings.Contains(failedOperatorAction, "spec.abort=true") {
		t.Fatalf("operator action omits retry or abort: %q", failedOperatorAction)
	}
	var failed *portsaudit.AuditEventDTO
	for _, event := range harness.audit.events {
		if event.Action == "rollout.failed" {
			failed = &event
		}
	}
	if failed == nil || failed.Details["operatorAction"] != failedOperatorAction || failed.Details["failureReason"] != "broker drain entered FAILED" {
		t.Fatalf("rollout.failed audit operator action = %+v", failed)
	}
	// The Failed attempt stays put until the operator changes retryToken.
	if _, err := harness.reconcile(t); err != nil || harness.status.status.Phase != PhaseFailed || harness.broker.starts != 1 {
		t.Fatalf("Failed attempt progressed without operator action: phase=%s starts=%d err=%v", harness.status.status.Phase, harness.broker.starts, err)
	}
}

func TestRestartResumesPersistedDrainAndDoesNotStartSecondOperation(t *testing.T) {
	harness := newRolloutTestHarness(t)
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhaseDraining || harness.status.status.DrainOperationID == "" || harness.broker.starts != 1 {
		t.Fatalf("initial persisted operation = %+v starts=%d", harness.status.status, harness.broker.starts)
	}
	attemptKey, operationID, deadline := harness.status.status.AttemptKey, harness.status.status.DrainOperationID, *harness.status.status.DrainDeadlineAt
	harness.serviceWithPersistedState(t)
	harness.broker.safe = false
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.broker.starts != 1 || harness.status.status.AttemptKey != attemptKey || harness.status.status.DrainOperationID != operationID ||
		harness.status.status.DrainDeadlineAt == nil || !harness.status.status.DrainDeadlineAt.Equal(deadline) || harness.runtime.deleteCalls != 0 {
		t.Fatalf("restart lost/duplicated drain state: %+v starts=%d deletes=%d", harness.status.status, harness.broker.starts, harness.runtime.deleteCalls)
	}
}

func TestAuditAppendRetryAfterStatusCASFailureDeduplicatesAcrossRestart(t *testing.T) {
	harness := newRolloutTestHarness(t)
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhaseDrainSafe {
		t.Fatalf("safe barrier phase = %s", harness.status.status.Phase)
	}
	harness.status.failNext = true
	if _, err := harness.reconcile(t); err == nil {
		t.Fatal("status CAS failure after audit append was hidden")
	}
	if harness.status.status.Phase != PhaseDrainSafe || len(harness.audit.events) != 1 || harness.runtime.deleteCalls != 0 {
		t.Fatalf("audit/CAS failure changed deletion state: phase=%s events=%d deletes=%d", harness.status.status.Phase, len(harness.audit.events), harness.runtime.deleteCalls)
	}
	harness.serviceWithPersistedState(t)
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhaseDeleteApproved || harness.audit.calls != 2 || len(harness.audit.events) != 1 || harness.runtime.deleteCalls != 0 {
		t.Fatalf("restart retry audit = phase %s calls %d unique %d deletes %d", harness.status.status.Phase, harness.audit.calls, len(harness.audit.events), harness.runtime.deleteCalls)
	}
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.runtime.deleteCalls != 1 || harness.status.status.Phase != PhaseAwaitingReplacement {
		t.Fatalf("approved restart delete = %d phase=%s", harness.runtime.deleteCalls, harness.status.status.Phase)
	}
}

func TestAbortAfterDeleteIntentWaitsForReplacementReady(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for range 4 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	if harness.status.status.Phase != PhaseDeleteApproved {
		t.Fatalf("pre-delete phase = %s", harness.status.status.Phase)
	}
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhaseAwaitingReplacement || harness.runtime.deleteCalls != 1 {
		t.Fatalf("delete transition = %s, calls=%d", harness.status.status.Phase, harness.runtime.deleteCalls)
	}
	harness.request.Abort = true
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhaseAwaitingReplacement || harness.status.status.CurrentOrdinal != 0 || harness.runtime.deleteCalls != 1 {
		t.Fatalf("abort advanced while replacement was absent: %+v deletes=%d", harness.status.status, harness.runtime.deleteCalls)
	}
	replaceWithTarget(harness, 0)
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhaseAborted || harness.status.status.CurrentOrdinal != 0 || harness.runtime.deleteCalls != 1 {
		t.Fatalf("abort after replacement readiness = %+v deletes=%d", harness.status.status, harness.runtime.deleteCalls)
	}
}

func TestAbortAfterApprovalButBeforeDeleteClearsIntentAndReleasesTarget(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for range 4 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	if harness.status.status.Phase != PhaseDeleteApproved || !harness.status.status.DeleteIntent {
		t.Fatalf("expected persisted approval before abort: %+v", harness.status.status)
	}
	harness.request.Abort = true
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhaseAborted || harness.status.status.DeleteIntent ||
		harness.runtime.deleteCalls != 0 || harness.runtime.released != 1 {
		t.Fatalf("abort did not clear approval without deleting: status=%+v deletes=%d released=%d",
			harness.status.status, harness.runtime.deleteCalls, harness.runtime.released)
	}
}

func TestAlreadyTargetPreflightRequiresPodHealthProbe(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for ordinal := range harness.runtime.workload.Pods {
		harness.runtime.workload.Pods[ordinal].Image = harness.request.TargetImage
		harness.runtime.workload.Pods[ordinal].ImageID = "containerd://" + strings.TrimPrefix(harness.request.TargetImage[strings.LastIndex(harness.request.TargetImage, "@")+1:], "")
		harness.broker.features.Members[ordinal].ReleaseVersion = harness.request.TargetVersion
	}
	harness.readiness.fail = true
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhasePreflight || harness.status.status.CurrentOrdinal != 0 || harness.runtime.deleteCalls != 0 || harness.runtime.released != 0 {
		t.Fatalf("health failure advanced an already-target Pod: %+v", harness.status.status)
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.9, §3 "Drain failure / timeout".
func TestApprovalAfterPersistedDeadlineFailsWithoutDelete(t *testing.T) {
	cases := []struct {
		name  string
		steps int
		phase string
		// deleteUnconfirmed leaves the rollout blocked with delete_unconfirmed before the deadline passes.
		deleteUnconfirmed bool
		// podRVChanged re-reads the evidence through the DeleteApproved resourceVersion-change branch.
		podRVChanged bool
	}{
		{name: "drain safe before approval", steps: 3, phase: PhaseDrainSafe},
		{name: "delete approved before DELETE", steps: 4, phase: PhaseDeleteApproved},
		{name: "delete approved, DELETE result unknown", steps: 4, phase: PhaseDeleteApproved, deleteUnconfirmed: true},
		{name: "delete approved, Pod resourceVersion changed", steps: 4, phase: PhaseDeleteApproved, podRVChanged: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness := newRolloutTestHarness(t)
			for range test.steps {
				if _, err := harness.reconcile(t); err != nil {
					t.Fatal(err)
				}
			}
			if harness.status.status.Phase != test.phase || harness.status.status.DrainDeadlineAt == nil {
				t.Fatalf("status before deadline = %+v", harness.status.status)
			}
			if test.deleteUnconfirmed {
				harness.runtime.deleteErr = errors.New("Kubernetes DELETE timeout")
				if _, err := harness.reconcile(t); err != nil {
					t.Fatal(err)
				}
				if harness.status.status.FailureCode != "delete_unconfirmed" {
					t.Fatalf("unknown DELETE result code = %s", harness.status.status.FailureCode)
				}
			}
			deletesBefore, startsBefore := harness.runtime.deleteCalls, harness.broker.starts
			if test.podRVChanged {
				harness.runtime.workload.Pods[0].ResourceVersion = "rv-after-approval"
			}
			harness.clock.now = *harness.status.status.DrainDeadlineAt
			if _, err := harness.reconcile(t); err != nil {
				t.Fatal(err)
			}
			status := harness.status.status
			if status.Phase != PhaseFailed || status.FailureCode != "drain_timeout" || !strings.HasSuffix(status.FailureReason, safeToStopOperatorAction) {
				t.Fatalf("late approval: phase=%s code=%s reason=%q, want Failed drain_timeout with abort-only operator action", status.Phase, status.FailureCode, status.FailureReason)
			}
			if harness.runtime.deleteCalls != deletesBefore || harness.broker.starts != startsBefore || status.CurrentOrdinal != 0 || len(status.CompletedOrdinals) != 0 {
				t.Fatalf("late approval mutated workload: deletes=%d->%d drainStarts=%d->%d ordinal=%d completed=%v",
					deletesBefore, harness.runtime.deleteCalls, startsBefore, harness.broker.starts, status.CurrentOrdinal, status.CompletedOrdinals)
			}
			var failed []portsaudit.AuditEventDTO
			for _, event := range harness.audit.events {
				if event.Action == "rollout.failed" {
					failed = append(failed, event)
				}
			}
			if len(failed) != 1 || failed[0].Reason != "drain_timeout" || failed[0].Details["operatorAction"] != safeToStopOperatorAction {
				t.Fatalf("rollout.failed audit events = %+v, want one drain_timeout event with abort-only action", failed)
			}
		})
	}
}

// failAtApprovalDeadline drives the harness to SAFE_TO_STOP in the given phase and lets the approval deadline pass.
func failAtApprovalDeadline(t *testing.T, harness *rolloutTestHarness, steps int) {
	t.Helper()
	for range steps {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	harness.clock.now = *harness.status.status.DrainDeadlineAt
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if status := harness.status.status; status.Phase != PhaseFailed || status.FailureCode != "drain_timeout" || status.DrainSafeToStop == nil || !*status.DrainSafeToStop {
		t.Fatalf("setup did not fail at SAFE_TO_STOP: %+v", status)
	}
}

func auditActions(harness *rolloutTestHarness, action string) []portsaudit.AuditEventDTO {
	var events []portsaudit.AuditEventDTO
	for _, event := range harness.audit.events {
		if event.Action == action {
			events = append(events, event)
		}
	}
	return events
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.7, §2.9, §3 "Drain failure / timeout".
func TestSafeToStopRetryReapprovesSameDrainAndDeletesOnce(t *testing.T) {
	cases := []struct {
		name         string
		steps        int
		podRVChanged bool
	}{
		{name: "failed in DrainSafe", steps: 3},
		{name: "failed in DeleteApproved", steps: 4},
		{name: "Pod resourceVersion changed after failure", steps: 4, podRVChanged: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness := newRolloutTestHarness(t)
			failAtApprovalDeadline(t, harness, test.steps)
			failed := harness.status.status
			approvalsBefore := len(auditActions(harness, "rollout.pod.delete_approved"))
			starts, cancels, deletes := harness.broker.starts, harness.broker.cancels, harness.runtime.deleteCalls
			if test.podRVChanged {
				harness.runtime.workload.Pods[0].ResourceVersion = "rv-after-failure"
			}
			// Past both the persisted drain deadline and the broker deadline: only the new approval deadline may allow approval.
			harness.clock.now = harness.clock.now.Add(time.Minute)
			harness.request.RetryToken = "attempt-2"
			if _, err := harness.reconcile(t); err != nil {
				t.Fatal(err)
			}
			status := harness.status.status
			if status.Phase != PhaseDrainSafe || status.AttemptKey != failed.AttemptKey || status.DrainOperationID != failed.DrainOperationID ||
				status.ApprovalDeadlineAt == nil || !status.ApprovalDeadlineAt.Equal(harness.clock.now.Add(DefaultDrainTimeout)) ||
				status.CurrentPodRV != harness.runtime.workload.Pods[0].ResourceVersion || status.DeleteIntent || status.RetryTokenHash != retryTokenHash("attempt-2") {
				t.Fatalf("SAFE_TO_STOP retry status = %+v, want DrainSafe on the same drain with a new approval deadline", status)
			}
			if harness.broker.starts != starts || harness.broker.cancels != cancels || harness.runtime.deleteCalls != deletes {
				t.Fatalf("retry mutated broker or Pod: starts %d->%d cancels %d->%d deletes %d->%d",
					starts, harness.broker.starts, cancels, harness.broker.cancels, deletes, harness.runtime.deleteCalls)
			}
			for range 2 {
				if _, err := harness.reconcile(t); err != nil {
					t.Fatal(err)
				}
			}
			if harness.status.status.Phase != PhaseAwaitingReplacement || harness.runtime.deleteCalls != deletes+1 || harness.broker.starts != starts || harness.broker.cancels != cancels {
				t.Fatalf("re-approval: phase=%s deletes=%d (want %d) starts=%d cancels=%d", harness.status.status.Phase, harness.runtime.deleteCalls, deletes+1, harness.broker.starts, harness.broker.cancels)
			}
			if approvals := auditActions(harness, "rollout.pod.delete_approved"); len(approvals) != approvalsBefore+1 {
				t.Fatalf("delete_approved events = %d, want a distinct re-approval event after %d", len(approvals), approvalsBefore)
			}
		})
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.9, §3 "Drain failure / timeout".
func TestSafeToStopRetryFailsAgainAtApprovalDeadlineWithoutDelete(t *testing.T) {
	harness := newRolloutTestHarness(t)
	failAtApprovalDeadline(t, harness, 3)
	harness.request.RetryToken = "attempt-2"
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	approvalDeadline := *harness.status.status.ApprovalDeadlineAt
	harness.broker.getErr = errors.New("drain GET unavailable")
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if status := harness.status.status; status.Phase != PhaseDrainSafe || status.FailureCode != "barrier_unknown" {
		t.Fatalf("unknown GET before the approval deadline: phase=%s code=%s, want DrainSafe barrier_unknown", status.Phase, status.FailureCode)
	}
	harness.clock.now = approvalDeadline
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	status := harness.status.status
	if status.Phase != PhaseFailed || status.FailureCode != "drain_timeout" || !strings.HasSuffix(status.FailureReason, safeToStopOperatorAction) || harness.runtime.deleteCalls != 0 {
		t.Fatalf("approval deadline: phase=%s code=%s reason=%q deletes=%d, want Failed drain_timeout without delete", status.Phase, status.FailureCode, status.FailureReason, harness.runtime.deleteCalls)
	}
	if failed := auditActions(harness, "rollout.failed"); len(failed) != 2 {
		t.Fatalf("rollout.failed events = %d, want the first failure and a distinct re-approval failure", len(failed))
	}
}

// Traceability: DEP-11, DEP-18; docs/contracts/rollout-controller-v1.md §2.7, §2.9.
func TestSafeToStopRetryNeverAdoptsUnauditedPodChangeOrUnconfirmedDrain(t *testing.T) {
	cases := []struct {
		name      string
		steps     int
		configure func(*rolloutTestHarness)
		wantPhase string
		wantCode  string
	}{
		{name: "Pod replaced without delete intent", steps: 3, configure: func(h *rolloutTestHarness) {
			h.runtime.workload.Pods[0].UID = "unaudited-uid"
		}, wantPhase: PhaseFailed, wantCode: "pod_identity_changed"},
		{name: "Pod terminating without delete intent", steps: 3, configure: func(h *rolloutTestHarness) {
			h.runtime.workload.Pods[0].Deleting = true
		}, wantPhase: PhaseFailed, wantCode: "pod_identity_changed"},
		{name: "Pod terminating after audited delete intent", steps: 4, configure: func(h *rolloutTestHarness) {
			h.runtime.workload.Pods[0].Deleting = true
		}, wantPhase: PhaseAwaitingReplacement},
		{name: "drain GET unavailable", steps: 3, configure: func(h *rolloutTestHarness) {
			h.broker.getErr = errors.New("drain GET unavailable")
		}, wantPhase: PhaseFailed, wantCode: "retry_cancel_unconfirmed"},
		{name: "drain no longer SAFE_TO_STOP", steps: 3, configure: func(h *rolloutTestHarness) {
			h.broker.safe = false
		}, wantPhase: PhaseFailed, wantCode: "retry_cancel_unconfirmed"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			harness := newRolloutTestHarness(t)
			failAtApprovalDeadline(t, harness, test.steps)
			failed := harness.status.status
			starts, cancels, deletes := harness.broker.starts, harness.broker.cancels, harness.runtime.deleteCalls
			test.configure(harness)
			harness.request.RetryToken = "attempt-2"
			if _, err := harness.reconcile(t); err != nil {
				t.Fatal(err)
			}
			status := harness.status.status
			if status.Phase != test.wantPhase || status.FailureCode != test.wantCode || status.AttemptKey != failed.AttemptKey {
				t.Fatalf("retry: phase=%s code=%s attemptKeyKept=%t, want phase=%s code=%q", status.Phase, status.FailureCode, status.AttemptKey == failed.AttemptKey, test.wantPhase, test.wantCode)
			}
			if test.wantPhase == PhaseFailed && status.RetryTokenHash != failed.RetryTokenHash {
				t.Fatal("blocked retry consumed the new retryToken")
			}
			if harness.broker.starts != starts || harness.broker.cancels != cancels || harness.runtime.deleteCalls != deletes {
				t.Fatalf("retry mutated broker or Pod: starts %d->%d cancels %d->%d deletes %d->%d",
					starts, harness.broker.starts, cancels, harness.broker.cancels, deletes, harness.runtime.deleteCalls)
			}
		})
	}
}

func TestAuditSinkFailureBlocksDelete(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for range 3 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	harness.audit.readyErr = errors.New("audit PVC unavailable")
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhaseDrainSafe || harness.runtime.deleteCalls != 0 || harness.audit.calls != 0 {
		t.Fatalf("audit outage did not fail closed: phase=%s deletes=%d audit=%d", harness.status.status.Phase, harness.runtime.deleteCalls, harness.audit.calls)
	}
}

func TestAuditAppendFailureBlocksDeleteAndNextNode(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for range 3 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	if harness.status.status.Phase != PhaseDrainSafe {
		t.Fatalf("pre-failure phase = %s", harness.status.status.Phase)
	}

	harness.audit.publishErr = errors.New("audit append/fsync failed")
	for range 2 {
		result, err := harness.reconcile(t)
		if err != nil {
			t.Fatalf("reconcile with failed audit append: %v", err)
		}
		if result.Phase != PhaseDrainSafe || harness.status.status.CurrentOrdinal != 0 ||
			harness.status.status.DeleteIntent || harness.runtime.deleteCalls != 0 || len(harness.runtime.deleteOrdinals) != 0 ||
			len(harness.audit.events) != 0 {
			t.Fatalf("audit append failure did not fail closed: result=%+v status=%+v deletes=%v events=%d",
				result, harness.status.status, harness.runtime.deleteOrdinals, len(harness.audit.events))
		}
	}
	if harness.audit.calls != 2 {
		t.Fatalf("PublishOnce calls = %d, want retry of the same blocked approval", harness.audit.calls)
	}
}

func TestPodResourceVersionConflictDoesNotDeleteReplacement(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for range 4 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	if harness.status.status.Phase != PhaseDeleteApproved {
		t.Fatalf("approval phase = %s", harness.status.status.Phase)
	}
	harness.runtime.workload.Pods[0].ResourceVersion = "replacement-rv-before-delete"
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.runtime.deleteCalls != 0 || harness.status.status.Phase != PhaseDrainSafe {
		t.Fatalf("stale resourceVersion deleted Pod or kept approval: deletes=%d phase=%s", harness.runtime.deleteCalls, harness.status.status.Phase)
	}
}

// Traceability: DEP-18; docs/contracts/rollout-controller-v1.md §2.7, §3 "Kubernetes mutation 결과 불명".
func TestDeletePodUnknownResultRetriesSamePreconditionAndHoldsNextNode(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for range 4 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	if harness.status.status.Phase != PhaseDeleteApproved {
		t.Fatalf("phase = %s, want %s", harness.status.status.Phase, PhaseDeleteApproved)
	}
	approvedUID, approvedRV := harness.status.status.CurrentPodUID, harness.status.status.CurrentPodRV
	startsBefore := harness.broker.starts
	harness.runtime.deleteErr = errors.New("Kubernetes DELETE timeout")
	assertHeld := func(step string, wantDeletes int) {
		t.Helper()
		status := harness.status.status
		if status.CurrentOrdinal != 0 || len(status.CompletedOrdinals) != 0 || harness.broker.starts != startsBefore {
			t.Fatalf("%s advanced to next node: ordinal=%d completed=%v drainStarts=%d", step, status.CurrentOrdinal, status.CompletedOrdinals, harness.broker.starts)
		}
		// The fake counts a DELETE only when the request carries the approved Pod UID and resourceVersion.
		if harness.runtime.deleteCalls != wantDeletes || !slices.Equal(harness.runtime.deleteOrdinals, slices.Repeat([]int32{0}, wantDeletes)) {
			t.Fatalf("%s deletes=%d ordinals=%v, want %d precondition-matched deletes of ordinal 0", step, harness.runtime.deleteCalls, harness.runtime.deleteOrdinals, wantDeletes)
		}
		if status.CurrentPodUID != approvedUID || status.CurrentPodRV != approvedRV {
			t.Fatalf("%s changed delete preconditions: uid=%s rv=%s", step, status.CurrentPodUID, status.CurrentPodRV)
		}
	}
	result, err := harness.reconcile(t)
	if err != nil {
		t.Fatal(err)
	}
	if result.Phase != PhaseDeleteApproved || harness.status.status.FailureCode != "delete_unconfirmed" {
		t.Fatalf("unknown DELETE result: phase=%s code=%s", result.Phase, harness.status.status.FailureCode)
	}
	assertHeld("unknown DELETE", 1)
	// Same Pod UID remains and deletion has not started: retry with the identical precondition.
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	assertHeld("retry while same UID remains", 2)
	// The earlier DELETE took effect late: deletionTimestamp is set, so no duplicate DELETE is sent.
	harness.runtime.workload.Pods[0].Deleting = true
	result, err = harness.reconcile(t)
	if err != nil {
		t.Fatal(err)
	}
	if result.Phase != PhaseAwaitingReplacement {
		t.Fatalf("phase after observed deletionTimestamp = %s, want %s", result.Phase, PhaseAwaitingReplacement)
	}
	assertHeld("deletionTimestamp observed", 2)
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	assertHeld("replacement not ready", 2)
}

// Traceability: DEP-18; docs/contracts/rollout-controller-v1.md §2.1, §2.6, §3 "Kubernetes mutation 결과 불명".
func TestUnknownDeleteRetryKeepsBlockerWithoutRewritingUnchangedStatus(t *testing.T) {
	harness := newRolloutTestHarness(t)
	// The broker reports sub-second times; the persisted status keeps whole seconds.
	harness.broker.onGet = func(observation *DrainObservationDTO) {
		observation.StartedAt = observation.StartedAt.Truncate(time.Second).Add(742 * time.Millisecond)
	}
	for range 4 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	if harness.status.status.Phase != PhaseDeleteApproved {
		t.Fatalf("phase = %s, want %s", harness.status.status.Phase, PhaseDeleteApproved)
	}
	harness.runtime.deleteErr = errors.New("Kubernetes DELETE timeout")
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	blocked, writes := harness.status.status, harness.status.writes
	if blocked.FailureCode != "delete_unconfirmed" || len(blocked.Conditions) != 1 || blocked.Conditions[0].Reason != "delete_unconfirmed" {
		t.Fatalf("unknown DELETE status = %+v", blocked)
	}
	for step := range 3 {
		harness.clock.now = harness.clock.now.Add(PollInterval)
		result, err := harness.reconcile(t)
		if err != nil {
			t.Fatal(err)
		}
		status := harness.status.status
		// Every status write is a watch event that re-runs reconcile at once, so an unchanged poll must not write.
		if harness.status.writes != writes || result.RequeueAfter != PollInterval || result.Phase != PhaseDeleteApproved {
			t.Fatalf("poll %d rewrote unchanged status or skipped the poll interval: writes=%d want=%d result=%+v", step, harness.status.writes, writes, result)
		}
		if status.FailureCode != "delete_unconfirmed" || !status.Conditions[0].LastTransitionTime.Equal(blocked.Conditions[0].LastTransitionTime) {
			t.Fatalf("poll %d lost the blocker or its transition time: %+v", step, status)
		}
		if harness.runtime.deleteCalls != 2+step {
			t.Fatalf("poll %d deletes = %d, want retry with the same precondition", step, harness.runtime.deleteCalls)
		}
	}
}

// Traceability: DEP-18; docs/contracts/rollout-controller-v1.md §2.5, §3 "Exclusive controller/target" (status CAS).
func TestStatusCASConflictRejectsStaleUpdateAndLeavesWorkloadUnmutated(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for range 3 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	if harness.status.status.Phase != PhaseDrainSafe {
		t.Fatalf("phase = %s, want %s before approval", harness.status.status.Phase, PhaseDrainSafe)
	}
	persisted := harness.status.status
	// A concurrent writer (non-leader or second controller) advances the CR resourceVersion.
	stale := harness.request
	stale.Status = persisted
	stale.ResourceVersion = harness.status.resourceVersion
	harness.status.resourceVersion = "concurrent-writer-rv"
	ctx := context.WithValue(context.Background(), rolloutTestClockKey{}, harness.clock)
	if _, err := harness.service.Reconcile(ctx, stale); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("stale status write must stop on CAS conflict, got %v", err)
	}
	if harness.runtime.deleteCalls != 0 {
		t.Fatalf("deleteCalls = %d after CAS conflict, want 0", harness.runtime.deleteCalls)
	}
	if harness.status.status.Phase != persisted.Phase || harness.status.status.DeleteIntent {
		t.Fatalf("CAS conflict changed persisted state: before=%s after=%+v", persisted.Phase, harness.status.status)
	}
}
