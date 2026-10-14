package rollout

import (
	"context"
	"errors"

	rolloutapp "narumq/internal/application/rollout"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

func (adapter *KubernetesRuntime) CompareAndSwapStatus(ctx context.Context, request rolloutapp.RolloutDTO, expectedResourceVersion string, status rolloutapp.StatusDTO) (string, error) {
	if expectedResourceVersion == "" || expectedResourceVersion != request.ResourceVersion || request.UID == "" {
		return "", errors.New("status CAS identity is incomplete")
	}
	resource := &NaruMQRollout{}
	key := types.NamespacedName{Namespace: request.Namespace, Name: request.Name}
	if err := adapter.reader.Get(ctx, key, resource); err != nil {
		return "", err
	}
	if string(resource.UID) != request.UID || resource.ResourceVersion != expectedResourceVersion {
		return "", apierrors.NewConflict(schema.GroupResource{Group: GroupVersion.Group, Resource: "narumqrollouts"}, request.Name, errors.New("rollout UID or resourceVersion changed"))
	}
	converted, err := statusToResource(status)
	if err != nil {
		return "", err
	}
	resource.Status = converted
	if err := adapter.client.Status().Update(ctx, resource); err != nil {
		return "", err
	}
	return resource.ResourceVersion, nil
}

var _ rolloutapp.StatusStore = (*KubernetesRuntime)(nil)
