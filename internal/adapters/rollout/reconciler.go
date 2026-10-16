package rollout

import (
	"context"
	"errors"

	rolloutapp "narumq/internal/application/rollout"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const rolloutFinalizer = "narumq.io/rollout-protection"

type Reconciler struct {
	client  client.Client
	reader  client.Reader
	service *rolloutapp.Service
}

func NewReconciler(kube client.Client, reader client.Reader, service *rolloutapp.Service) (*Reconciler, error) {
	if kube == nil || reader == nil || service == nil {
		return nil, errors.New("rollout reconciler dependencies are required")
	}
	return &Reconciler{client: kube, reader: reader, service: service}, nil
}

func (reconciler *Reconciler) SetupWithManager(manager manager.Manager) error {
	if manager == nil {
		return errors.New("controller manager is required")
	}
	return builder.ControllerManagedBy(manager).For(&NaruMQRollout{}).Complete(reconciler)
}

func (reconciler *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	// The status store compares ResourceVersion, so reconcile from an uncached read.
	resource := &NaruMQRollout{}
	if err := reconciler.reader.Get(ctx, request.NamespacedName, resource); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	if resource.DeletionTimestamp.IsZero() && !containsFinalizer(resource.Finalizers, rolloutFinalizer) {
		// Patch only metadata: a full Update re-encodes spec and drops explicit zero values such as
		// abort=false, which leaves a GitOps-applied CR permanently out of sync.
		base := resource.DeepCopy()
		resource.Finalizers = append(resource.Finalizers, rolloutFinalizer)
		if err := reconciler.client.Patch(ctx, resource, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: rolloutapp.PollInterval}, nil
	}
	if !resource.DeletionTimestamp.IsZero() && !containsFinalizer(resource.Finalizers, rolloutFinalizer) {
		return reconcile.Result{}, nil
	}
	if resource.Spec.Management.CredentialSecretRef.Name == "" || resource.Spec.ImagePullSecretRef.Name == "" {
		return reconcile.Result{}, errors.New("same-namespace management and image-pull Secret references are required")
	}
	credential := &corev1.Secret{}
	if err := reconciler.reader.Get(ctx, types.NamespacedName{Namespace: resource.Namespace, Name: resource.Spec.Management.CredentialSecretRef.Name}, credential); err != nil {
		return reconcile.Result{}, errors.New("same-namespace management credential Secret is unavailable")
	}
	token, ok := credential.Data["token"]
	if !ok || len(token) == 0 {
		return reconcile.Result{}, errors.New("management credential Secret must contain a non-empty token key")
	}
	imageSecret := &corev1.Secret{}
	if err := reconciler.reader.Get(ctx, types.NamespacedName{Namespace: resource.Namespace, Name: resource.Spec.ImagePullSecretRef.Name}, imageSecret); err != nil ||
		imageSecret.Type != corev1.SecretTypeDockerConfigJson || len(imageSecret.Data[corev1.DockerConfigJsonKey]) == 0 {
		return reconcile.Result{}, errors.New("same-namespace image-pull Secret is unavailable or invalid")
	}
	status, err := statusFromResource(resource.Status)
	if err != nil {
		return reconcile.Result{}, err
	}
	requestDTO := rolloutapp.RolloutDTO{
		Namespace: resource.Namespace, Name: resource.Name, UID: string(resource.UID), Generation: resource.Generation,
		ResourceVersion: resource.ResourceVersion, WorkloadName: resource.Spec.WorkloadRef.Name,
		TargetImage: resource.Spec.Target.Image, TargetVersion: resource.Spec.Target.ReleaseVersion,
		TargetFeatures:     append([]string(nil), resource.Spec.Target.Features...),
		RegistrySecretName: resource.Spec.ImagePullSecretRef.Name, ManagementEndpoint: resource.Spec.Management.Endpoint,
		ManagementSecret: string(token), ManagementSecretName: resource.Spec.Management.CredentialSecretRef.Name,
		RetryToken: resource.Spec.RetryToken, Abort: resource.Spec.Abort || !resource.DeletionTimestamp.IsZero(), Status: status,
	}
	result, err := reconciler.service.Reconcile(ctx, requestDTO)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !resource.DeletionTimestamp.IsZero() && (result.Phase == rolloutapp.PhaseAborted || result.Phase == rolloutapp.PhaseCompleted) {
		fresh := &NaruMQRollout{}
		if err := reconciler.reader.Get(ctx, types.NamespacedName{Namespace: resource.Namespace, Name: resource.Name}, fresh); err != nil {
			return reconcile.Result{}, err
		}
		if fresh.UID != resource.UID {
			return reconcile.Result{}, errors.New("rollout UID changed while releasing its deletion finalizer")
		}
		base := fresh.DeepCopy()
		fresh.Finalizers = removeFinalizer(fresh.Finalizers, rolloutFinalizer)
		if err := reconciler.client.Patch(ctx, fresh, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}
	return reconcile.Result{RequeueAfter: result.RequeueAfter}, nil
}

func containsFinalizer(finalizers []string, target string) bool {
	for _, finalizer := range finalizers {
		if finalizer == target {
			return true
		}
	}
	return false
}

func removeFinalizer(finalizers []string, target string) []string {
	filtered := finalizers[:0]
	for _, finalizer := range finalizers {
		if finalizer != target {
			filtered = append(filtered, finalizer)
		}
	}
	return filtered
}

var _ reconcile.Reconciler = (*Reconciler)(nil)
