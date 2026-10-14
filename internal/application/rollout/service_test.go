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
}

func (store *rolloutTestStatus) CompareAndSwapStatus(_ context.Context, rollout RolloutDTO, expected string, status StatusDTO) (string, error) {
	if store.failNext {
		store.failNext = false
		return "", errors.New("injected status write loss")
	}
	if rollout.ResourceVersion != expected || expected != store.resourceVersion {
		return "", errors.New("resourceVersion conflict")
	}
	store.status = status
	store.resourceVersion = fmt.Sprintf("rv-%d", len(status.CompletedOrdinals)+len(status.Conditions)+storeRevision(store.resourceVersion))
	return store.resourceVersion, nil
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

func TestFreshApprovalAtPersistedDeadlineIsBlocked(t *testing.T) {
	harness := newRolloutTestHarness(t)
	for range 3 {
		if _, err := harness.reconcile(t); err != nil {
			t.Fatal(err)
		}
	}
	if harness.status.status.Phase != PhaseDrainSafe || harness.status.status.DrainDeadlineAt == nil {
		t.Fatalf("safe drain status = %+v", harness.status.status)
	}
	harness.clock.now = *harness.status.status.DrainDeadlineAt
	if _, err := harness.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if harness.status.status.Phase != PhaseDrainSafe || harness.runtime.deleteCalls != 0 || harness.audit.calls != 0 {
		t.Fatalf("late approval was not blocked: phase=%s deletes=%d audit=%d", harness.status.status.Phase, harness.runtime.deleteCalls, harness.audit.calls)
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
