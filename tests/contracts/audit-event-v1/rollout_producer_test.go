package auditeventv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"narumq/internal/adapters/auditlog"
	portsaudit "narumq/internal/application/ports/audit"
	rolloutapp "narumq/internal/application/rollout"
)

// Traceability: SEC-11; docs/contracts/rollout-controller-v1.md §3, §5; docs/contracts/audit-event-v1.md §2, §6.
func TestRolloutControllerAuditEventsUseAuditV1SchemaAndRedactSecrets(t *testing.T) {
	const (
		managementSecret = "known-rollout-management-secret-not-for-audit"
		retryToken       = "known-rollout-retry-token-not-for-audit"
		failedRetryToken = "known-rollout-retry-token-terminal-not-for-audit"
		managementRef    = "management-secret-ref-not-for-audit"
		registryRef      = "registry-secret-ref-not-for-audit"
	)
	const features = "compatibility-probe"
	oldDigest := "sha256:" + strings.Repeat("a", 64)
	targetDigest := "sha256:" + strings.Repeat("b", 64)
	oldImage := "registry.test/narumq@" + oldDigest
	targetImage := "registry.test/narumq@" + targetDigest
	oldMetadata := rolloutapp.ImageMetadataDTO{
		Image: oldImage, ManifestDigest: oldDigest, ConfigDigest: "sha256:" + strings.Repeat("c", 64),
		OS: "linux", Architecture: "arm64", Version: "1.1.0", Features: []string{features},
	}
	targetMetadata := rolloutapp.ImageMetadataDTO{
		Image: targetImage, ManifestDigest: targetDigest, ConfigDigest: "sha256:" + strings.Repeat("d", 64),
		OS: "linux", Architecture: "arm64", Version: "1.2.0", Features: []string{features},
	}
	pods := make([]rolloutapp.PodDTO, 3)
	members := make([]rolloutapp.MemberProfileDTO, 3)
	for ordinal := range 3 {
		pods[ordinal] = rolloutapp.PodDTO{
			Name: fmt.Sprintf("broker-%d", ordinal), Ordinal: int32(ordinal), UID: fmt.Sprintf("pod-uid-%d", ordinal),
			ResourceVersion: fmt.Sprintf("pod-rv-%d", ordinal), Image: oldImage, ImageID: "containerd://" + oldDigest,
			PodIP: fmt.Sprintf("10.0.0.%d", ordinal+1), OS: "linux", Architecture: "arm64", Ready: true,
		}
		members[ordinal] = rolloutapp.MemberProfileDTO{NodeID: uint32(ordinal + 1), ReleaseVersion: "1.1.0", Features: []string{features}}
	}
	workload := rolloutapp.WorkloadDTO{
		Name: "broker", UID: "statefulset-uid", ResourceVersion: "workload-rv", Generation: 1, ObservedGeneration: 1,
		Replicas: 3, UpdateStrategy: "OnDelete", Containers: []rolloutapp.ContainerDTO{{Name: "narumq", Image: targetImage}},
		ImagePullSecrets: []string{registryRef}, Pods: pods,
	}
	clock := rolloutAuditContractClock{now: time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)}
	status := &rolloutAuditContractStatus{resourceVersion: 1}
	runtime := &rolloutAuditContractRuntime{workload: workload}
	broker := &rolloutAuditContractBroker{
		features: rolloutapp.ClusterFeaturesDTO{
			ActiveFeatures: []string{features}, Members: members,
		},
		operations: make(map[string]rolloutapp.DrainObservationDTO), expectedManagementSecret: managementSecret,
	}
	images := rolloutAuditContractImages{oldImage: oldMetadata, targetImage: targetMetadata}
	directory := t.TempDir()

	publisher, err := auditlog.OpenFileAuditPublisher(auditlog.FileAuditPublisherConfig{
		Directory: directory, MaxFileBytes: auditlog.DefaultMaxFileBytes,
	})
	if err != nil {
		t.Fatalf("OpenFileAuditPublisher: %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("Close audit publisher: %v", err)
		}
	}()
	if err := publisher.Ready(); err != nil {
		t.Fatalf("audit publisher not ready: %v", err)
	}
	service, err := rolloutapp.NewService(rolloutapp.ServiceConfig{
		ProducerVersion: "1.2.3", ProducerInstanceID: "controller-pod-uid",
	}, status, runtime, images, broker, rolloutAuditContractReadiness{}, publisher, clock)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	rollout := rolloutapp.RolloutDTO{
		Namespace: "narumq", Name: "upgrade", UID: "rollout-uid", Generation: 1, ResourceVersion: "1",
		WorkloadName: "broker", TargetImage: targetImage, TargetVersion: "1.2.0", TargetFeatures: []string{features},
		RegistrySecretName: registryRef, ManagementEndpoint: "http://narumq-management.narumq.svc:8080",
		ManagementSecret: managementSecret, ManagementSecretName: managementRef, RetryToken: retryToken,
	}
	for range 4 {
		rollout.ResourceVersion = strconv.Itoa(status.resourceVersion)
		rollout.Status = status.status
		if _, err := service.Reconcile(context.Background(), rollout); err != nil {
			t.Fatalf("Reconcile rollout: %v", err)
		}
	}
	if status.status.Phase != rolloutapp.PhaseDeleteApproved || runtime.deleteCalls != 0 {
		t.Fatalf("audit approval did not persist before Pod delete: phase=%s deletes=%d", status.status.Phase, runtime.deleteCalls)
	}

	page, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 10})
	if err != nil {
		t.Fatalf("ReadAuditRecords: %v", err)
	}
	if len(page.Records) != 1 {
		t.Fatalf("durable audit record count = %d, want one approval", len(page.Records))
	}
	result, err := auditlog.VerifyDirectory(directory)
	if err != nil {
		t.Fatalf("VerifyDirectory: %v", err)
	}
	if result.Records != 1 {
		t.Fatalf("verified durable audit record count = %d, want 1", result.Records)
	}
	eventJSON := page.Records[0].EventJSON
	var event map[string]any
	if err := json.Unmarshal(eventJSON, &event); err != nil {
		t.Fatalf("decode durable audit event: %v", err)
	}
	schema := compileAuditEventSchema(t)
	if err := schema.Validate(event); err != nil {
		t.Fatalf("rollout producer event does not match Audit Event v1 schema: %v", err)
	}
	for _, secret := range []string{managementSecret, retryToken} {
		if strings.Contains(string(eventJSON), secret) {
			t.Errorf("audit event contains raw credential %q", secret)
		}
	}
	statusJSON, err := json.Marshal(status.status)
	if err != nil {
		t.Fatalf("marshal rollout status: %v", err)
	}
	for _, secret := range []string{managementSecret, retryToken} {
		if strings.Contains(string(statusJSON), secret) {
			t.Errorf("persisted rollout status contains raw credential %q", secret)
		}
	}
	var decoded struct {
		SchemaVersion string `json:"schemaVersion"`
		EventID       string `json:"eventId"`
		RequestID     string `json:"requestId"`
		Producer      struct {
			Component  string `json:"component"`
			InstanceID string `json:"instanceId"`
		} `json:"producer"`
		Action string `json:"action"`
		Target struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		} `json:"target"`
		Result string `json:"result"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(eventJSON, &decoded); err != nil {
		t.Fatalf("decode audit contract fields: %v", err)
	}
	if decoded.SchemaVersion != "1.0" || decoded.EventID == "" || decoded.RequestID != decoded.EventID ||
		decoded.Producer.Component != "ROLLOUT_CONTROLLER" || decoded.Producer.InstanceID != "controller-pod-uid" ||
		decoded.Action != "rollout.pod.delete_approved" || decoded.Target.Type != "broker_pod" ||
		decoded.Target.ID != "broker-0" || decoded.Result != "ALLOWED" || decoded.Reason != "safe_to_stop" {
		t.Fatalf("durable approval audit contract fields = %+v", decoded)
	}
	failedRollout := rollout
	failedRollout.Name, failedRollout.UID, failedRollout.RetryToken = "failed-upgrade", "failed-rollout-uid", failedRetryToken
	status.status = rolloutapp.StatusDTO{}
	status.resourceVersion = 1
	broker.operations = make(map[string]rolloutapp.DrainObservationDTO)
	broker.failOperation = true
	for range 3 {
		failedRollout.ResourceVersion = strconv.Itoa(status.resourceVersion)
		failedRollout.Status = status.status
		if _, err := service.Reconcile(context.Background(), failedRollout); err != nil {
			t.Fatalf("Reconcile failed rollout: %v", err)
		}
	}
	if status.status.Phase != rolloutapp.PhaseFailed || runtime.deleteCalls != 0 {
		t.Fatalf("failed drain did not stop without Pod deletion: phase=%s deletes=%d", status.status.Phase, runtime.deleteCalls)
	}
	page, err = publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 10})
	if err != nil {
		t.Fatalf("ReadAuditRecords after terminal failure: %v", err)
	}
	if len(page.Records) != 3 {
		t.Fatalf("durable rollout event count = %d, want approval, denial, and failure", len(page.Records))
	}
	result, err = auditlog.VerifyDirectory(directory)
	if err != nil {
		t.Fatalf("VerifyDirectory after terminal failure: %v", err)
	}
	if result.Records != 3 {
		t.Fatalf("verified audit record count = %d, want 3", result.Records)
	}
	seen := make(map[string]int, 3)
	for _, record := range page.Records {
		var event map[string]any
		if err := json.Unmarshal(record.EventJSON, &event); err != nil {
			t.Fatalf("decode durable event: %v", err)
		}
		if err := schema.Validate(event); err != nil {
			t.Fatalf("rollout event does not match Audit Event v1 schema: %v", err)
		}
		for _, secret := range []string{managementSecret, retryToken, failedRetryToken} {
			if strings.Contains(string(record.EventJSON), secret) {
				t.Errorf("durable audit event contains raw credential %q", secret)
			}
		}
		var fields struct {
			EventID   string `json:"eventId"`
			RequestID string `json:"requestId"`
			Producer  struct {
				Component  string `json:"component"`
				InstanceID string `json:"instanceId"`
			} `json:"producer"`
			Action string `json:"action"`
			Target struct {
				Type string `json:"type"`
				ID   string `json:"id"`
			} `json:"target"`
			Result  string         `json:"result"`
			Reason  string         `json:"reason"`
			Details map[string]any `json:"details"`
		}
		if err := json.Unmarshal(record.EventJSON, &fields); err != nil {
			t.Fatalf("decode rollout event fields: %v", err)
		}
		if fields.EventID == "" || fields.RequestID != fields.EventID ||
			fields.Producer.Component != "ROLLOUT_CONTROLLER" || fields.Producer.InstanceID != "controller-pod-uid" {
			t.Fatalf("invalid common rollout audit identity: %+v", fields)
		}
		switch fields.Action {
		case "rollout.pod.delete_approved":
			if fields.Result != "ALLOWED" || fields.Reason != "safe_to_stop" ||
				fields.Target.Type != "broker_pod" || fields.Target.ID != "broker-0" {
				t.Errorf("invalid approval event boundary: %+v", fields)
			}
			if _, ok := fields.Details["pendingWAL"]; !ok {
				t.Errorf("approval event omitted barrier evidence: %+v", fields.Details)
			}
		case "rollout.pod.delete_denied":
			if fields.Result != "DENIED" || fields.Reason != "drain_failed" ||
				fields.Target.Type != "broker_pod" || fields.Target.ID != "broker-0" ||
				fields.Details["observedBlocker"] != "drain_failed" {
				t.Errorf("invalid denial event boundary: %+v", fields)
			}
		case "rollout.failed":
			operatorAction, actionExists := fields.Details["operatorAction"].(string)
			if fields.Result != "FAILED" || fields.Reason != "drain_failed" ||
				fields.Target.Type != "rollout" || fields.Target.ID != "narumq/failed-upgrade" ||
				!actionExists || strings.TrimSpace(operatorAction) == "" {
				t.Errorf("invalid terminal failure event boundary: %+v", fields)
			}
		default:
			t.Errorf("unexpected rollout audit action %q", fields.Action)
		}
		seen[fields.Action]++
	}
	for _, action := range []string{"rollout.pod.delete_approved", "rollout.pod.delete_denied", "rollout.failed"} {
		if seen[action] != 1 {
			t.Errorf("durable action %q count = %d, want one", action, seen[action])
		}
	}
	statusJSON, err = json.Marshal(status.status)
	if err != nil {
		t.Fatalf("marshal terminal rollout status: %v", err)
	}
	for _, secret := range []string{managementSecret, retryToken, failedRetryToken} {
		if strings.Contains(string(statusJSON), secret) {
			t.Errorf("persisted terminal status contains raw credential %q", secret)
		}
	}
}

type rolloutAuditContractClock struct{ now time.Time }

func (clock rolloutAuditContractClock) Now() time.Time { return clock.now }

type rolloutAuditContractStatus struct {
	resourceVersion int
	status          rolloutapp.StatusDTO
}

func (store *rolloutAuditContractStatus) CompareAndSwapStatus(_ context.Context, _ rolloutapp.RolloutDTO, expected string, status rolloutapp.StatusDTO) (string, error) {
	if expected != strconv.Itoa(store.resourceVersion) {
		return "", errors.New("rollout status resourceVersion conflict")
	}
	store.resourceVersion++
	store.status = status
	return strconv.Itoa(store.resourceVersion), nil
}

type rolloutAuditContractRuntime struct {
	workload    rolloutapp.WorkloadDTO
	deleteCalls int
}

func (runtime *rolloutAuditContractRuntime) ClaimTarget(context.Context, rolloutapp.TargetClaimDTO) (bool, error) {
	return true, nil
}
func (runtime *rolloutAuditContractRuntime) ReleaseTarget(context.Context, rolloutapp.TargetClaimDTO) error {
	return nil
}
func (runtime *rolloutAuditContractRuntime) InspectWorkload(context.Context, rolloutapp.RolloutDTO) (rolloutapp.WorkloadDTO, error) {
	workload := runtime.workload
	workload.Containers = slices.Clone(workload.Containers)
	workload.ImagePullSecrets = slices.Clone(workload.ImagePullSecrets)
	workload.Pods = slices.Clone(workload.Pods)
	return workload, nil
}
func (runtime *rolloutAuditContractRuntime) DeletePod(context.Context, rolloutapp.DeletePodDTO) error {
	runtime.deleteCalls++
	return nil
}

type rolloutAuditContractImages map[string]rolloutapp.ImageMetadataDTO

func (images rolloutAuditContractImages) ReadImageMetadata(_ context.Context, _, image, _ string) (rolloutapp.ImageMetadataDTO, error) {
	metadata, ok := images[image]
	if !ok {
		return rolloutapp.ImageMetadataDTO{}, errors.New("unknown immutable image")
	}
	return metadata, nil
}

type rolloutAuditContractBroker struct {
	features                 rolloutapp.ClusterFeaturesDTO
	operations               map[string]rolloutapp.DrainObservationDTO
	expectedManagementSecret string
	failOperation            bool
}

func (broker *rolloutAuditContractBroker) ClusterFeatures(_ context.Context, _, _, token string) (rolloutapp.ClusterFeaturesDTO, error) {
	if err := broker.checkManagementSecret(token); err != nil {
		return rolloutapp.ClusterFeaturesDTO{}, err
	}
	return broker.features, nil
}
func (broker *rolloutAuditContractBroker) GetDrain(_ context.Context, _, _, token string, nodeID uint32, key string) (rolloutapp.DrainObservationDTO, bool, error) {
	if err := broker.checkManagementSecret(token); err != nil {
		return rolloutapp.DrainObservationDTO{}, false, err
	}
	observation, ok := broker.operations[key]
	if !ok {
		return rolloutapp.DrainObservationDTO{}, false, nil
	}
	if observation.NodeID != nodeID {
		return rolloutapp.DrainObservationDTO{}, false, errors.New("node mismatch")
	}
	if broker.failOperation {
		observation.Phase, observation.State, observation.SafeToStop = "FAILED", "FAILED", new(false)
		observation.FailureCode, observation.FailurePresent = "drain_failed", true
	} else {
		observation.Phase, observation.State, observation.SafeToStop = "COMPLETED", "SAFE_TO_STOP", new(true)
	}
	broker.operations[key] = observation
	return observation, true, nil
}
func (broker *rolloutAuditContractBroker) StartDrain(_ context.Context, _, _, token string, nodeID uint32, key string) (rolloutapp.DrainObservationDTO, error) {
	if err := broker.checkManagementSecret(token); err != nil {
		return rolloutapp.DrainObservationDTO{}, err
	}
	observation := rolloutapp.DrainObservationDTO{
		OperationID: "operation-1", NodeID: nodeID, Phase: "TRANSFERRING_SESSIONS", State: "DRAINING",
		StartedAt:  time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
		DeadlineAt: time.Date(2026, 10, 15, 12, 10, 0, 0, time.UTC), SafeToStop: new(false),
	}
	broker.operations[key] = observation
	return observation, nil
}
func (broker *rolloutAuditContractBroker) CancelDrain(_ context.Context, _, _, token string, _ uint32, _ string) (rolloutapp.DrainObservationDTO, error) {
	if err := broker.checkManagementSecret(token); err != nil {
		return rolloutapp.DrainObservationDTO{}, err
	}
	return rolloutapp.DrainObservationDTO{}, errors.New("unexpected drain cancellation")
}
func (broker *rolloutAuditContractBroker) checkManagementSecret(token string) error {
	if token != broker.expectedManagementSecret {
		return errors.New("management credential was not passed to the broker adapter")
	}
	return nil
}

type rolloutAuditContractReadiness struct{}

func (rolloutAuditContractReadiness) Ready(context.Context, string) error { return nil }
