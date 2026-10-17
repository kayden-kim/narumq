package rollout

import (
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "narumq.io", Version: "v1alpha1"}

var SchemeBuilder = runtime.NewSchemeBuilder(func(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &NaruMQRollout{}, &NaruMQRolloutList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
})
var AddToScheme = SchemeBuilder.AddToScheme

type WorkloadReference struct {
	Name string `json:"name"`
}

type TargetReference struct {
	Image          string   `json:"image"`
	ReleaseVersion string   `json:"releaseVersion"`
	Features       []string `json:"features"`
}

type ManagementReference struct {
	Endpoint            string          `json:"endpoint"`
	CredentialSecretRef SecretReference `json:"credentialSecretRef"`
}

type SecretReference struct {
	Name string `json:"name"`
}

type NaruMQRolloutSpec struct {
	WorkloadRef        WorkloadReference   `json:"workloadRef"`
	Target             TargetReference     `json:"target"`
	ImagePullSecretRef SecretReference     `json:"imagePullSecretRef"`
	Management         ManagementReference `json:"management"`
	RetryToken         string              `json:"retryToken"`
	Abort              bool                `json:"abort,omitempty"`
}

type NaruMQRolloutStatus struct {
	ObservedGeneration        int64              `json:"observedGeneration,omitempty"`
	Phase                     string             `json:"phase,omitempty"`
	AttemptKey                string             `json:"attemptKey,omitempty"`
	RetryTokenHash            string             `json:"retryTokenHash,omitempty"`
	CurrentOrdinal            int32              `json:"currentOrdinal,omitempty"`
	CurrentPodName            string             `json:"currentPodName,omitempty"`
	CurrentPodUID             string             `json:"currentPodUID,omitempty"`
	CurrentPodResourceVersion string             `json:"currentPodResourceVersion,omitempty"`
	DrainOperationID          string             `json:"drainOperationID,omitempty"`
	DrainIntentAt             *metav1.Time       `json:"drainIntentAt,omitempty"`
	DrainStartedAt            *metav1.Time       `json:"drainStartedAt,omitempty"`
	DrainDeadlineAt           *metav1.Time       `json:"drainDeadlineAt,omitempty"`
	DrainPhase                string             `json:"drainPhase,omitempty"`
	DrainState                string             `json:"drainState,omitempty"`
	DrainSafeToStop           *bool              `json:"drainSafeToStop,omitempty"`
	SafeToStopObservedAt      *metav1.Time       `json:"safeToStopObservedAt,omitempty"`
	ApprovalDeadlineAt        *metav1.Time       `json:"approvalDeadlineAt,omitempty"`
	DrainConnectionsInitial   int64              `json:"drainConnectionsInitial,omitempty"`
	DrainConnectionsRemaining int64              `json:"drainConnectionsRemaining,omitempty"`
	DrainSessionsReady        int64              `json:"drainSessionsReady,omitempty"`
	DrainSessionsTakenOver    int64              `json:"drainSessionsTakenOver,omitempty"`
	DrainSessionsFailed       int64              `json:"drainSessionsFailed,omitempty"`
	DrainPendingWAL           int64              `json:"drainPendingWAL,omitempty"`
	DrainUnderReplicated      int64              `json:"drainUnderReplicated,omitempty"`
	DrainFailureCode          string             `json:"drainFailureCode,omitempty"`
	DrainBlockingReason       string             `json:"drainBlockingReason,omitempty"`
	DeleteIntent              bool               `json:"deleteIntent,omitempty"`
	FailureCode               string             `json:"failureCode,omitempty"`
	FailureReason             string             `json:"failureReason,omitempty"`
	CompletedOrdinals         []int32            `json:"completedOrdinals,omitempty"`
	TargetImage               string             `json:"targetImage,omitempty"`
	TargetVersion             string             `json:"targetVersion,omitempty"`
	TargetFeatures            []string           `json:"targetFeatures,omitempty"`
	WorkloadUID               string             `json:"workloadUID,omitempty"`
	WorkloadName              string             `json:"workloadName,omitempty"`
	RegistrySecretName        string             `json:"registrySecretName,omitempty"`
	ManagementEndpoint        string             `json:"managementEndpoint,omitempty"`
	ManagementSecretName      string             `json:"managementSecretName,omitempty"`
	WorkloadGeneration        int64              `json:"workloadGeneration,omitempty"`
	LastTransitionTime        *metav1.Time       `json:"lastTransitionTime,omitempty"`
	Conditions                []metav1.Condition `json:"conditions,omitempty"`
}

type NaruMQRollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NaruMQRolloutSpec   `json:"spec"`
	Status            NaruMQRolloutStatus `json:"status,omitempty"`
}

type NaruMQRolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NaruMQRollout `json:"items"`
}

func (resource *NaruMQRollout) DeepCopyObject() runtime.Object { return resource.DeepCopy() }

func (resource *NaruMQRollout) DeepCopy() *NaruMQRollout {
	if resource == nil {
		return nil
	}
	copy := *resource
	copy.ObjectMeta = *resource.ObjectMeta.DeepCopy()
	copy.Spec.Target.Features = slices.Clone(resource.Spec.Target.Features)
	copy.Status.CompletedOrdinals = slices.Clone(resource.Status.CompletedOrdinals)
	copy.Status.TargetFeatures = slices.Clone(resource.Status.TargetFeatures)
	copy.Status.Conditions = slices.Clone(resource.Status.Conditions)
	copy.Status.DrainIntentAt = copyTime(resource.Status.DrainIntentAt)
	copy.Status.DrainStartedAt = copyTime(resource.Status.DrainStartedAt)
	copy.Status.DrainDeadlineAt = copyTime(resource.Status.DrainDeadlineAt)
	copy.Status.SafeToStopObservedAt = copyTime(resource.Status.SafeToStopObservedAt)
	copy.Status.ApprovalDeadlineAt = copyTime(resource.Status.ApprovalDeadlineAt)
	copy.Status.LastTransitionTime = copyTime(resource.Status.LastTransitionTime)
	if resource.Status.DrainSafeToStop != nil {
		value := *resource.Status.DrainSafeToStop
		copy.Status.DrainSafeToStop = &value
	}
	return &copy
}

func (resource *NaruMQRolloutList) DeepCopyObject() runtime.Object {
	if resource == nil {
		return nil
	}
	copy := *resource
	copy.ListMeta = *resource.ListMeta.DeepCopy()
	copy.Items = make([]NaruMQRollout, len(resource.Items))
	for index := range resource.Items {
		copy.Items[index] = *resource.Items[index].DeepCopy()
	}
	return &copy
}

func copyTime(value *metav1.Time) *metav1.Time {
	if value == nil {
		return nil
	}
	copy := metav1.NewTime(time.Time(value.Time))
	return &copy
}
