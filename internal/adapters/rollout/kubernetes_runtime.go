package rollout

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	rolloutapp "narumq/internal/application/rollout"
	rolloutdomain "narumq/internal/domain/rollout"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	controllerContainerName = "narumq"
	managementServiceName   = "narumq-management"
	managementServicePort   = int32(8080)
)

type KubernetesRuntime struct {
	client client.Client
	reader client.Reader
}

func NewKubernetesRuntime(kube client.Client, reader client.Reader) (*KubernetesRuntime, error) {
	if kube == nil || reader == nil {
		return nil, errors.New("Kubernetes client and uncached API reader are required")
	}
	return &KubernetesRuntime{client: kube, reader: reader}, nil
}

func (adapter *KubernetesRuntime) ClaimTarget(ctx context.Context, claim rolloutapp.TargetClaimDTO) (bool, error) {
	if err := validateClaim(claim); err != nil {
		return false, err
	}
	name := targetLeaseName(claim.Workload)
	key := types.NamespacedName{Namespace: claim.Namespace, Name: name}
	lease := &coordinationv1.Lease{}
	err := adapter.reader.Get(ctx, key, lease)
	if apierrors.IsNotFound(err) {
		duration := int32(30)
		now := metav1.NewMicroTime(time.Now().UTC())
		lease = &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Namespace: claim.Namespace, Name: name,
				Labels:      map[string]string{"app.kubernetes.io/managed-by": "narumq-rollout-controller"},
				Annotations: map[string]string{"narumq.io/workload": claim.Workload, "narumq.io/rollout": claim.Rollout}},
			Spec: coordinationv1.LeaseSpec{HolderIdentity: new(claim.RolloutUID), LeaseDurationSeconds: &duration, AcquireTime: &now, RenewTime: &now},
		}
		if err := adapter.client.Create(ctx, lease); err != nil {
			if apierrors.IsAlreadyExists(err) || apierrors.IsConflict(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != claim.RolloutUID ||
		lease.Annotations["narumq.io/workload"] != claim.Workload {
		return false, nil
	}
	now := metav1.NewMicroTime(time.Now().UTC())
	lease.Spec.RenewTime = &now
	if err := adapter.client.Update(ctx, lease); err != nil {
		if apierrors.IsConflict(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (adapter *KubernetesRuntime) ReleaseTarget(ctx context.Context, claim rolloutapp.TargetClaimDTO) error {
	if err := validateClaim(claim); err != nil {
		return err
	}
	lease := &coordinationv1.Lease{}
	key := types.NamespacedName{Namespace: claim.Namespace, Name: targetLeaseName(claim.Workload)}
	if err := adapter.reader.Get(ctx, key, lease); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != claim.RolloutUID {
		return errors.New("target Lease ownership changed before release")
	}
	uid, resourceVersion := lease.UID, lease.ResourceVersion
	return adapter.client.Delete(ctx, lease, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}})
}

func (adapter *KubernetesRuntime) InspectWorkload(ctx context.Context, rollout rolloutapp.RolloutDTO) (rolloutapp.WorkloadDTO, error) {
	if rollout.Namespace == "" || rollout.WorkloadName == "" {
		return rolloutapp.WorkloadDTO{}, errors.New("workload namespace and name are required")
	}
	statefulSet := &appsv1.StatefulSet{}
	if err := adapter.reader.Get(ctx, types.NamespacedName{Namespace: rollout.Namespace, Name: rollout.WorkloadName}, statefulSet); err != nil {
		return rolloutapp.WorkloadDTO{}, err
	}
	selector, err := metav1.LabelSelectorAsSelector(statefulSet.Spec.Selector)
	if err != nil || selector.Empty() {
		return rolloutapp.WorkloadDTO{}, errors.New("StatefulSet selector is invalid")
	}
	service := &corev1.Service{}
	if err := adapter.reader.Get(ctx, types.NamespacedName{Namespace: rollout.Namespace, Name: managementServiceName}, service); err != nil {
		return rolloutapp.WorkloadDTO{}, errors.New("same-namespace management Service is unavailable")
	}
	if err := validateManagementService(service, statefulSet); err != nil {
		return rolloutapp.WorkloadDTO{}, err
	}
	pods := &corev1.PodList{}
	if err := adapter.reader.List(ctx, pods, client.InNamespace(rollout.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return rolloutapp.WorkloadDTO{}, err
	}
	replicas := int32(0)
	if statefulSet.Spec.Replicas != nil {
		replicas = *statefulSet.Spec.Replicas
	}
	workload := rolloutapp.WorkloadDTO{
		Name: statefulSet.Name, UID: string(statefulSet.UID), ResourceVersion: statefulSet.ResourceVersion,
		Generation: statefulSet.Generation, ObservedGeneration: statefulSet.Status.ObservedGeneration,
		Replicas: replicas, UpdateStrategy: string(statefulSet.Spec.UpdateStrategy.Type),
		Containers:       make([]rolloutapp.ContainerDTO, 0, len(statefulSet.Spec.Template.Spec.Containers)),
		ImagePullSecrets: make([]string, 0, len(statefulSet.Spec.Template.Spec.ImagePullSecrets)),
		Pods:             make([]rolloutapp.PodDTO, 0, len(pods.Items)),
	}
	for _, container := range statefulSet.Spec.Template.Spec.Containers {
		workload.Containers = append(workload.Containers, rolloutapp.ContainerDTO{Name: container.Name, Image: container.Image})
	}
	for _, secret := range statefulSet.Spec.Template.Spec.ImagePullSecrets {
		workload.ImagePullSecrets = append(workload.ImagePullSecrets, secret.Name)
	}
	if len(pods.Items) > rolloutdomain.NodeCount {
		return rolloutapp.WorkloadDTO{}, errors.New("more Pods match the rollout StatefulSet selector than supported ordinals")
	}
	for index := range pods.Items {
		pod := &pods.Items[index]
		ordinal, err := podOrdinal(statefulSet.Name, pod.Name)
		if err != nil || !hasStatefulSetOwner(pod, statefulSet.UID) {
			return rolloutapp.WorkloadDTO{}, errors.New("Pod is not owned by the requested StatefulSet at a stable ordinal")
		}
		var containerImage, imageID string
		containerMatches := 0
		for _, container := range pod.Spec.Containers {
			if container.Name == controllerContainerName {
				containerImage = container.Image
				containerMatches++
			}
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == controllerContainerName {
				imageID = status.ImageID
			}
		}
		if containerMatches != 1 || containerImage == "" {
			return rolloutapp.WorkloadDTO{}, errors.New("Pod does not contain exactly one NaruMQ container")
		}
		if err := serviceSelectsPod(service, pod); err != nil {
			return rolloutapp.WorkloadDTO{}, err
		}
		node := &corev1.Node{}
		if pod.Spec.NodeName == "" || adapter.reader.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node) != nil {
			return rolloutapp.WorkloadDTO{}, errors.New("Pod node platform is unavailable")
		}
		ready := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				ready = true
				break
			}
		}
		workload.Pods = append(workload.Pods, rolloutapp.PodDTO{
			Name: pod.Name, Ordinal: ordinal, UID: string(pod.UID), ResourceVersion: pod.ResourceVersion,
			Image: containerImage, ImageID: imageID, PodIP: pod.Status.PodIP,
			OS: node.Labels[corev1.LabelOSStable], Architecture: node.Labels[corev1.LabelArchStable],
			Ready: ready, Deleting: pod.DeletionTimestamp != nil,
		})
	}
	return workload, nil
}

func (adapter *KubernetesRuntime) DeletePod(ctx context.Context, request rolloutapp.DeletePodDTO) error {
	if request.Namespace == "" || request.Name == "" || request.UID == "" || request.ResourceVersion == "" {
		return errors.New("Pod delete requires namespace, name, UID and resourceVersion preconditions")
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: request.Namespace, Name: request.Name}}
	uid, resourceVersion := types.UID(request.UID), request.ResourceVersion
	return adapter.client.Delete(ctx, pod, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}})
}

func podOrdinal(statefulSetName, podName string) (int32, error) {
	prefix := statefulSetName + "-"
	if !strings.HasPrefix(podName, prefix) {
		return 0, errors.New("Pod name does not match StatefulSet")
	}
	value := strings.TrimPrefix(podName, prefix)
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || strconv.FormatInt(parsed, 10) != value || parsed < 0 || parsed >= int64(rolloutdomain.NodeCount) {
		return 0, errors.New("Pod ordinal is not canonical or supported")
	}
	return int32(parsed), nil
}

func hasStatefulSetOwner(pod *corev1.Pod, uid types.UID) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.UID == uid && owner.Kind == "StatefulSet" && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}

func validateManagementService(service *corev1.Service, statefulSet *appsv1.StatefulSet) error {
	if service.Spec.ClusterIP == "" || service.Spec.ClusterIP == corev1.ClusterIPNone || len(service.Spec.Selector) == 0 {
		return errors.New("management Service must be a namespaced ClusterIP Service with a selector")
	}
	foundPort := false
	for _, port := range service.Spec.Ports {
		if port.Name != "http-mgmt" || port.Port != managementServicePort || port.Protocol != corev1.ProtocolTCP {
			continue
		}
		if port.TargetPort.Type == intstr.Int && port.TargetPort.IntVal == managementServicePort {
			foundPort = true
			break
		}
		if port.TargetPort.Type == intstr.String {
			for _, container := range statefulSet.Spec.Template.Spec.Containers {
				for _, containerPort := range container.Ports {
					if containerPort.Name == port.TargetPort.StrVal && containerPort.ContainerPort == managementServicePort {
						foundPort = true
					}
				}
			}
		}
	}
	if !foundPort {
		return errors.New("management Service must expose TCP port 8080 to the NaruMQ management container")
	}
	return nil
}

func serviceSelectsPod(service *corev1.Service, pod *corev1.Pod) error {
	for key, expected := range service.Spec.Selector {
		if pod.Labels[key] != expected {
			return errors.New("management Service selector does not include every rollout Pod")
		}
	}
	return nil
}

func validateClaim(claim rolloutapp.TargetClaimDTO) error {
	if claim.Namespace == "" || claim.Workload == "" || claim.RolloutUID == "" || claim.Rollout == "" ||
		len(validation.IsDNS1123Label(claim.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(claim.Workload)) != 0 {
		return errors.New("target claim identity is invalid")
	}
	return nil
}

func targetLeaseName(workload string) string {
	digest := sha256.Sum256([]byte(workload))
	return "narumq-rollout-" + hex.EncodeToString(digest[:8])
}

var _ rolloutapp.Runtime = (*KubernetesRuntime)(nil)
