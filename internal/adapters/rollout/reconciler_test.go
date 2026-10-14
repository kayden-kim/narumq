package rollout

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	portsaudit "narumq/internal/application/ports/audit"
	rolloutapp "narumq/internal/application/rollout"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type reconcilerCacheReadGuard struct {
	client.Client
}

func (guard reconcilerCacheReadGuard) Get(ctx context.Context, key types.NamespacedName, object client.Object, options ...client.GetOption) error {
	if _, isRollout := object.(*NaruMQRollout); isRollout {
		return errors.New("rollout ResourceVersion must come from the uncached reader")
	}
	return guard.Client.Get(ctx, key, object, options...)
}

type reconcilerStubServiceRuntime struct {
	workload     rolloutapp.WorkloadDTO
	claimCalls   int
	releaseCalls int
}

func (s *reconcilerStubServiceRuntime) ClaimTarget(context.Context, rolloutapp.TargetClaimDTO) (bool, error) {
	s.claimCalls++
	return true, nil
}
func (s *reconcilerStubServiceRuntime) ReleaseTarget(context.Context, rolloutapp.TargetClaimDTO) error {
	s.releaseCalls++
	return nil
}
func (s *reconcilerStubServiceRuntime) InspectWorkload(context.Context, rolloutapp.RolloutDTO) (rolloutapp.WorkloadDTO, error) {
	return s.workload, nil
}
func (s *reconcilerStubServiceRuntime) DeletePod(context.Context, rolloutapp.DeletePodDTO) error {
	return nil
}

type reconcilerStubImages struct{}

func (reconcilerStubImages) ReadImageMetadata(_ context.Context, _, image, _ string) (rolloutapp.ImageMetadataDTO, error) {
	return rolloutapp.ImageMetadataDTO{
		Image: image, ManifestDigest: "sha256:" + strings.Repeat("b", 64), OS: "linux", Architecture: "arm64",
		Version: "1.2.0", Features: []string{"compatibility-probe"},
	}, nil
}

type reconcilerStubBroker struct{}

func (reconcilerStubBroker) ClusterFeatures(context.Context, string, string, string) (rolloutapp.ClusterFeaturesDTO, error) {
	return rolloutapp.ClusterFeaturesDTO{
		ActiveFeatures: []string{"compatibility-probe"},
		Members: []rolloutapp.MemberProfileDTO{
			{NodeID: 1, ReleaseVersion: "1.2.0", Features: []string{"compatibility-probe"}},
			{NodeID: 2, ReleaseVersion: "1.2.0", Features: []string{"compatibility-probe"}},
			{NodeID: 3, ReleaseVersion: "1.2.0", Features: []string{"compatibility-probe"}},
		},
	}, nil
}
func (reconcilerStubBroker) GetDrain(context.Context, string, string, string, uint32, string) (rolloutapp.DrainObservationDTO, bool, error) {
	return rolloutapp.DrainObservationDTO{}, false, nil
}
func (reconcilerStubBroker) StartDrain(context.Context, string, string, string, uint32, string) (rolloutapp.DrainObservationDTO, error) {
	return rolloutapp.DrainObservationDTO{
		OperationID: "op-1", NodeID: 1, Phase: "TRANSFERRING_SESSIONS", State: "DRAINING",
		StartedAt: time.Now().UTC(), DeadlineAt: time.Now().UTC().Add(rolloutapp.DefaultDrainTimeout),
	}, nil
}
func (reconcilerStubBroker) CancelDrain(context.Context, string, string, string, uint32, string) (rolloutapp.DrainObservationDTO, error) {
	return rolloutapp.DrainObservationDTO{}, nil
}

type reconcilerStubProbe struct{}

func (reconcilerStubProbe) Ready(context.Context, string) error { return nil }

type reconcilerStubAudit struct{}

func (reconcilerStubAudit) Ready() error                               { return nil }
func (reconcilerStubAudit) PublishOnce(portsaudit.AuditEventDTO) error { return nil }

type reconcilerStubClock struct{}

func (reconcilerStubClock) Now() time.Time { return time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC) }

func TestReconcilerMissingSecretsAndNormalReconcileCAS(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = AddToScheme(scheme)

	targetDigest := "sha256:" + strings.Repeat("b", 64)
	_ = appsv1.AddToScheme(scheme)
	_ = coordinationv1.AddToScheme(scheme)
	targetImage := "809446814769.dkr.ecr.ap-northeast-2.amazonaws.com/narumq@" + targetDigest

	rolloutCR := &NaruMQRollout{
		ObjectMeta: metav1.ObjectMeta{Namespace: "narumq", Name: "narumq-rollout", UID: "cr-uid", ResourceVersion: "1", Generation: 1},
		Spec: NaruMQRolloutSpec{
			WorkloadRef:        WorkloadReference{Name: "narumq"},
			Target:             TargetReference{Image: targetImage, ReleaseVersion: "1.2.0", Features: []string{"compatibility-probe"}},
			ImagePullSecretRef: SecretReference{Name: "narumq-ecr"},
			Management:         ManagementReference{Endpoint: "http://narumq-management.narumq.svc:8080", CredentialSecretRef: SecretReference{Name: "narumq-operator"}},
			RetryToken:         "token-1",
		},
	}

	operatorSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "narumq", Name: "narumq-operator"},
		Data:       map[string][]byte{"token": []byte("valid-token")},
	}

	ecrSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "narumq", Name: "narumq-ecr"},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{}}`)},
	}

	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&NaruMQRollout{}).WithObjects(
		rolloutCR, operatorSecret, ecrSecret,
	).Build()

	runtimeAdapter, err := NewKubernetesRuntime(kube, kube)
	if err != nil {
		t.Fatal(err)
	}

	pods := []rolloutapp.PodDTO{
		{Name: "narumq-0", Ordinal: 0, UID: "uid-0", ResourceVersion: "1", Image: targetImage, ImageID: "containerd://" + targetDigest, OS: "linux", Architecture: "arm64", Ready: true},
		{Name: "narumq-1", Ordinal: 1, UID: "uid-1", ResourceVersion: "1", Image: targetImage, ImageID: "containerd://" + targetDigest, OS: "linux", Architecture: "arm64", Ready: true},
		{Name: "narumq-2", Ordinal: 2, UID: "uid-2", ResourceVersion: "1", Image: targetImage, ImageID: "containerd://" + targetDigest, OS: "linux", Architecture: "arm64", Ready: true},
	}
	workload := rolloutapp.WorkloadDTO{
		Name: "narumq", UID: "ss-uid", Generation: 1, ObservedGeneration: 1, Replicas: 3, UpdateStrategy: "OnDelete",
		Containers: []rolloutapp.ContainerDTO{{Name: "narumq", Image: targetImage}}, ImagePullSecrets: []string{"narumq-ecr"}, Pods: pods,
	}

	serviceRuntime := &reconcilerStubServiceRuntime{workload: workload}
	service, err := rolloutapp.NewService(
		rolloutapp.ServiceConfig{ProducerVersion: "1.2.3", ProducerInstanceID: "ctrl-1"},
		runtimeAdapter,
		serviceRuntime,
		reconcilerStubImages{},
		reconcilerStubBroker{},
		reconcilerStubProbe{},
		reconcilerStubAudit{},
		reconcilerStubClock{},
	)
	if err != nil {
		t.Fatal(err)
	}

	reconciler, err := NewReconciler(reconcilerCacheReadGuard{Client: kube}, kube, service)
	if err != nil {
		t.Fatal(err)
	}

	// The first reconcile installs deletion protection and cannot claim a target or mutate status.
	_, err = reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "narumq", Name: "narumq-rollout"}})
	if err != nil {
		t.Fatalf("install deletion finalizer: %v", err)
	}
	protected := &NaruMQRollout{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "narumq", Name: "narumq-rollout"}, protected); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(protected.Finalizers, rolloutFinalizer) || protected.Status.Phase != "" || serviceRuntime.claimCalls != 0 {
		t.Fatalf("first reconcile acted before durable finalizer: finalizers=%v status=%+v claims=%d", protected.Finalizers, protected.Status, serviceRuntime.claimCalls)
	}
	if err := kube.Delete(context.Background(), ecrSecret); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "narumq", Name: "narumq-rollout"}}); err == nil {
		t.Fatal("missing image-pull Secret did not fail closed")
	}
	wrongType := ecrSecret.DeepCopy()
	wrongType.ResourceVersion = ""
	wrongType.Type = corev1.SecretTypeOpaque
	if err := kube.Create(context.Background(), wrongType); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "narumq", Name: "narumq-rollout"}}); err == nil {
		t.Fatal("wrong-type image-pull Secret did not fail closed")
	}
	if err := kube.Delete(context.Background(), wrongType); err != nil {
		t.Fatal(err)
	}
	ecrSecret.ResourceVersion = ""
	if err := kube.Create(context.Background(), ecrSecret); err != nil {
		t.Fatal(err)
	}
	protected = &NaruMQRollout{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "narumq", Name: "narumq-rollout"}, protected); err != nil {
		t.Fatal(err)
	}
	if protected.Status.Phase != "" || serviceRuntime.claimCalls != 0 {
		t.Fatalf("invalid image Secret reached rollout side effects: status=%+v claims=%d", protected.Status, serviceRuntime.claimCalls)
	}
	_, err = reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "narumq", Name: "narumq-rollout"}})
	if err != nil {
		t.Fatalf("reconcile protected rollout: %v", err)
	}
	updated := &NaruMQRollout{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "narumq", Name: "narumq-rollout"}, updated); err != nil {
		t.Fatalf("fetch updated CR: %v", err)
	}
	if updated.Status.Phase != rolloutapp.PhasePreflight && updated.Status.Phase != rolloutapp.PhaseCompleted {
		t.Fatalf("unexpected status phase after reconcile: %s", updated.Status.Phase)
	}
	if !slices.Contains(updated.Status.CompletedOrdinals, 0) {
		t.Fatalf("expected completed ordinals to contain 0, got %v status=%+v", updated.Status.CompletedOrdinals, updated.Status)
	}
	if err := kube.Delete(context.Background(), updated); err != nil {
		t.Fatalf("request CR deletion: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "narumq", Name: "narumq-rollout"}}); err != nil {
		t.Fatalf("abort and release deletion finalizer: %v", err)
	}
	deleted := &NaruMQRollout{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "narumq", Name: "narumq-rollout"}, deleted); err == nil {
		t.Fatal("terminal aborted rollout retained its deletion finalizer")
	}
	if serviceRuntime.releaseCalls != 1 {
		t.Fatalf("aborted rollout released target %d times, want once", serviceRuntime.releaseCalls)
	}
}
