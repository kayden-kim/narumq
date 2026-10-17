package rollout

import (
	"errors"
	"math"
	"time"

	rolloutapp "narumq/internal/application/rollout"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func statusFromResource(status NaruMQRolloutStatus) (rolloutapp.StatusDTO, error) {
	counts := [7]int64{
		status.DrainConnectionsInitial, status.DrainConnectionsRemaining, status.DrainSessionsReady,
		status.DrainSessionsTakenOver, status.DrainSessionsFailed, status.DrainPendingWAL,
		status.DrainUnderReplicated,
	}
	for _, count := range counts {
		if count < 0 {
			return rolloutapp.StatusDTO{}, errors.New("persisted drain evidence contains a negative counter")
		}
	}
	conditions := make([]rolloutapp.ConditionDTO, 0, len(status.Conditions))
	for _, condition := range status.Conditions {
		conditions = append(conditions, rolloutapp.ConditionDTO{
			Type: condition.Type, Status: string(condition.Status), Reason: condition.Reason,
			Message: condition.Message, ObservedGeneration: condition.ObservedGeneration,
			LastTransitionTime: condition.LastTransitionTime.Time,
		})
	}
	return rolloutapp.StatusDTO{
		Phase: status.Phase, ObservedGeneration: status.ObservedGeneration, AttemptKey: status.AttemptKey,
		RetryTokenHash: status.RetryTokenHash, CurrentOrdinal: status.CurrentOrdinal,
		CurrentPodName: status.CurrentPodName, CurrentPodUID: status.CurrentPodUID,
		CurrentPodRV: status.CurrentPodResourceVersion, DrainOperationID: status.DrainOperationID,
		DrainIntentAt: timeFromMeta(status.DrainIntentAt), DrainStartedAt: timeFromMeta(status.DrainStartedAt),
		DrainDeadlineAt: timeFromMeta(status.DrainDeadlineAt), DrainPhase: status.DrainPhase, DrainState: status.DrainState,
		DrainSafeToStop: cloneBool(status.DrainSafeToStop), SafeToStopObservedAt: timeFromMeta(status.SafeToStopObservedAt),
		ApprovalDeadlineAt:      timeFromMeta(status.ApprovalDeadlineAt),
		DrainConnectionsInitial: uint64(status.DrainConnectionsInitial), DrainConnectionsRemaining: uint64(status.DrainConnectionsRemaining),
		DrainSessionsReady: uint64(status.DrainSessionsReady), DrainSessionsTakenOver: uint64(status.DrainSessionsTakenOver),
		DrainSessionsFailed: uint64(status.DrainSessionsFailed), DrainPendingWAL: uint64(status.DrainPendingWAL),
		DrainUnderReplicated: uint64(status.DrainUnderReplicated), DrainFailureCode: status.DrainFailureCode,
		DrainBlockingReason: status.DrainBlockingReason, DeleteIntent: status.DeleteIntent,
		FailureCode: status.FailureCode, FailureReason: status.FailureReason, Conditions: conditions,
		CompletedOrdinals: append([]int32(nil), status.CompletedOrdinals...), TargetImage: status.TargetImage,
		TargetVersion: status.TargetVersion, TargetFeatures: append([]string(nil), status.TargetFeatures...),
		WorkloadUID: status.WorkloadUID, WorkloadGeneration: status.WorkloadGeneration,
		WorkloadName: status.WorkloadName, RegistrySecretName: status.RegistrySecretName,
		ManagementEndpoint: status.ManagementEndpoint, ManagementSecretName: status.ManagementSecretName,
		LastTransitionTime: timeValue(status.LastTransitionTime),
	}, nil
}

func statusToResource(status rolloutapp.StatusDTO) (NaruMQRolloutStatus, error) {
	counts := [7]uint64{
		status.DrainConnectionsInitial, status.DrainConnectionsRemaining, status.DrainSessionsReady,
		status.DrainSessionsTakenOver, status.DrainSessionsFailed, status.DrainPendingWAL,
		status.DrainUnderReplicated,
	}
	for _, count := range counts {
		if count > math.MaxInt64 {
			return NaruMQRolloutStatus{}, errors.New("drain evidence counter exceeds Kubernetes status integer range")
		}
	}
	conditions := make([]metav1.Condition, 0, len(status.Conditions))
	for _, condition := range status.Conditions {
		conditions = append(conditions, metav1.Condition{
			Type: condition.Type, Status: metav1.ConditionStatus(condition.Status), Reason: condition.Reason,
			Message: condition.Message, ObservedGeneration: condition.ObservedGeneration,
			LastTransitionTime: metav1.NewTime(condition.LastTransitionTime),
		})
	}
	return NaruMQRolloutStatus{
		Phase: status.Phase, ObservedGeneration: status.ObservedGeneration, AttemptKey: status.AttemptKey,
		RetryTokenHash: status.RetryTokenHash, CurrentOrdinal: status.CurrentOrdinal,
		CurrentPodName: status.CurrentPodName, CurrentPodUID: status.CurrentPodUID,
		CurrentPodResourceVersion: status.CurrentPodRV, DrainOperationID: status.DrainOperationID,
		DrainIntentAt: metaTime(status.DrainIntentAt), DrainStartedAt: metaTime(status.DrainStartedAt),
		DrainDeadlineAt: metaTime(status.DrainDeadlineAt), DrainPhase: status.DrainPhase, DrainState: status.DrainState,
		DrainSafeToStop: cloneBool(status.DrainSafeToStop), SafeToStopObservedAt: metaTime(status.SafeToStopObservedAt),
		ApprovalDeadlineAt:      metaTime(status.ApprovalDeadlineAt),
		DrainConnectionsInitial: int64(status.DrainConnectionsInitial), DrainConnectionsRemaining: int64(status.DrainConnectionsRemaining),
		DrainSessionsReady: int64(status.DrainSessionsReady), DrainSessionsTakenOver: int64(status.DrainSessionsTakenOver),
		DrainSessionsFailed: int64(status.DrainSessionsFailed), DrainPendingWAL: int64(status.DrainPendingWAL),
		DrainUnderReplicated: int64(status.DrainUnderReplicated), DrainFailureCode: status.DrainFailureCode,
		DrainBlockingReason: status.DrainBlockingReason, DeleteIntent: status.DeleteIntent,
		FailureCode: status.FailureCode, FailureReason: status.FailureReason, CompletedOrdinals: append([]int32(nil), status.CompletedOrdinals...),
		TargetImage: status.TargetImage, TargetVersion: status.TargetVersion, TargetFeatures: append([]string(nil), status.TargetFeatures...),
		WorkloadUID: status.WorkloadUID, WorkloadGeneration: status.WorkloadGeneration,
		WorkloadName: status.WorkloadName, RegistrySecretName: status.RegistrySecretName,
		ManagementEndpoint: status.ManagementEndpoint, ManagementSecretName: status.ManagementSecretName,
		LastTransitionTime: metaTime(timePointer(status.LastTransitionTime)), Conditions: conditions,
	}, nil
}

func timeFromMeta(value *metav1.Time) *time.Time {
	if value == nil {
		return nil
	}
	converted := value.Time.UTC()
	return &converted
}

func metaTime(value *time.Time) *metav1.Time {
	if value == nil {
		return nil
	}
	converted := metav1.NewTime(value.UTC())
	return &converted
}

func timeValue(value *metav1.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.Time
}

func timePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}
