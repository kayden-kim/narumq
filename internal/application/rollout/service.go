package rollout

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	auditapp "narumq/internal/application/audit"
	portsaudit "narumq/internal/application/ports/audit"
	"narumq/internal/domain/release"
	rolloutdomain "narumq/internal/domain/rollout"
)

type ServiceConfig struct {
	ProducerVersion    string
	ProducerInstanceID string
}

type Service struct {
	status    StatusStore
	runtime   Runtime
	images    ImageMetadataReader
	broker    Broker
	readiness ReadinessProbe
	audit     AuditPublisher
	clock     Clock
	config    ServiceConfig
}

func NewService(config ServiceConfig, status StatusStore, runtime Runtime, images ImageMetadataReader, broker Broker, readiness ReadinessProbe, audit AuditPublisher, clock Clock) (*Service, error) {
	if status == nil || runtime == nil || images == nil || broker == nil || readiness == nil || audit == nil || clock == nil ||
		config.ProducerVersion == "" || config.ProducerInstanceID == "" {
		return nil, errors.New("rollout service dependencies and producer identity are required")
	}
	return &Service{status: status, runtime: runtime, images: images, broker: broker, readiness: readiness, audit: audit, clock: clock, config: config}, nil
}

func (service *Service) Reconcile(ctx context.Context, rollout RolloutDTO) (ReconcileResultDTO, error) {
	if ctx == nil || !validRollout(rollout) {
		return ReconcileResultDTO{}, errors.New("valid rollout request and context are required")
	}
	state := rollout.Status
	if state.Phase == "" {
		state = newStatus(rollout, service.clock.Now())
		if _, err := service.save(ctx, &rollout, state); err != nil {
			return ReconcileResultDTO{}, err
		}
	}
	claim := targetClaim(rollout)
	if state.Phase == PhaseCompleted {
		if err := service.runtime.ReleaseTarget(ctx, claim); err != nil {
			return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
		}
		return ReconcileResultDTO{Phase: state.Phase}, nil
	}
	owned, err := service.runtime.ClaimTarget(ctx, claim)
	if err != nil {
		return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
	}
	if !owned {
		return service.block(ctx, rollout, state, "target_owned", "another rollout owns this StatefulSet")
	}
	if !statusMatchesSpec(rollout, state) {
		return service.fail(ctx, rollout, state, "spec_changed", "target identity cannot change during an active rollout")
	}
	if rollout.Abort {
		return service.abort(ctx, rollout, state)
	}
	tokenHash := retryTokenHash(rollout.RetryToken)
	if state.Phase == PhaseFailed {
		if tokenHash == state.RetryTokenHash {
			return ReconcileResultDTO{Phase: state.Phase}, nil
		}
		return service.retry(ctx, rollout, state, tokenHash)
	}
	if state.Phase == PhaseAborted {
		return ReconcileResultDTO{Phase: state.Phase}, nil
	}
	if tokenHash != state.RetryTokenHash {
		return service.fail(ctx, rollout, state, "retry_token_changed", "retryToken may change only after a failed attempt")
	}

	if state.CurrentOrdinal < 0 || state.CurrentOrdinal >= rolloutdomain.NodeCount {
		return service.fail(ctx, rollout, state, "invalid_ordinal", "rollout status ordinal is outside the three-node workload")
	}
	workload, features, metadata, err := service.preflight(ctx, rollout, state)
	if err != nil {
		return service.block(ctx, rollout, state, "preflight_blocked", "preflight evidence is unavailable or inconsistent")
	}
	ordinal := state.CurrentOrdinal
	pod, found := findPod(workload.Pods, ordinal)
	if state.WorkloadUID == "" {
		state.WorkloadUID, state.WorkloadGeneration = workload.UID, workload.Generation
	}
	state.ObservedGeneration = rollout.Generation
	state.FailureCode, state.FailureReason = "", ""
	setReadyCondition(&state, rollout.Generation, service.clock.Now())
	switch state.Phase {
	case PhasePreflight:
		if !found {
			return service.block(ctx, rollout, state, "pod_missing", "current StatefulSet ordinal has no Pod")
		}
		if pod.Deleting {
			return service.block(ctx, rollout, state, "pod_deleting", "current Pod is already terminating")
		}
		member := findMember(features.Members, uint32(ordinal+1))
		if podMatchesTarget(*pod, rollout, metadata, member) && pod.Ready {
			if err := service.readiness.Ready(ctx, pod.PodIP); err != nil {
				return service.block(ctx, rollout, state, "readiness_unconfirmed", "target Pod health endpoint is not ready")
			}
			return service.completeCurrentOrdinal(ctx, rollout, state, workload)
		}
		state.CurrentPodName, state.CurrentPodUID, state.CurrentPodRV = pod.Name, pod.UID, pod.ResourceVersion
		intendedAt := service.clock.Now().UTC()
		deadline := intendedAt.Add(DefaultDrainTimeout)
		state.DrainIntentAt, state.DrainStartedAt, state.DrainDeadlineAt = &intendedAt, nil, &deadline
		state.AttemptKey = attemptKey(rollout.UID, ordinal, rollout.RetryToken)
		state.DrainOperationID, state.DrainPhase, state.DrainState = "", "", ""
		state.DrainSafeToStop, state.SafeToStopObservedAt, state.DeleteIntent = nil, nil, false
		state.DrainConnectionsInitial, state.DrainConnectionsRemaining = 0, 0
		state.DrainSessionsReady, state.DrainSessionsTakenOver, state.DrainSessionsFailed = 0, 0, 0
		state.DrainPendingWAL, state.DrainUnderReplicated = 0, 0
		state.DrainFailureCode, state.DrainBlockingReason = "", ""
		state.Phase = PhaseDraining
		if _, err := service.save(ctx, &rollout, state); err != nil {
			return ReconcileResultDTO{}, err
		}
		return ReconcileResultDTO{RequeueAfter: 0, Phase: state.Phase}, nil
	case PhaseDraining:
		if !found || pod.UID != state.CurrentPodUID || pod.Deleting {
			return service.fail(ctx, rollout, state, "pod_identity_changed", "draining Pod UID changed or began terminating before approval")
		}
		return service.pollDrain(ctx, rollout, state)
	case PhaseDrainSafe:
		if !found || pod.UID != state.CurrentPodUID || pod.Deleting {
			return service.block(ctx, rollout, state, "pod_identity_changed", "safe-to-stop evidence no longer identifies the current Pod")
		}
		if pod.ResourceVersion != state.CurrentPodRV {
			state.CurrentPodRV = pod.ResourceVersion
			if _, err := service.save(ctx, &rollout, state); err != nil {
				return ReconcileResultDTO{}, err
			}
			return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
		}
		return service.approveDelete(ctx, rollout, state)
	case PhaseDeleteApproved:
		if !found || pod.UID != state.CurrentPodUID || pod.Deleting {
			state.Phase = PhaseAwaitingReplacement
			if _, err := service.save(ctx, &rollout, state); err != nil {
				return ReconcileResultDTO{}, err
			}
			return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
		}
		if pod.ResourceVersion != state.CurrentPodRV {
			if _, err := service.requireSafeDrain(ctx, rollout, &state); err != nil {
				return service.block(ctx, rollout, state, "barrier_unknown", "drain GET did not return complete safe evidence")
			}
			state.CurrentPodRV = pod.ResourceVersion
			state.Phase, state.DeleteIntent = PhaseDrainSafe, false
			if _, err := service.save(ctx, &rollout, state); err != nil {
				return ReconcileResultDTO{}, err
			}
			return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
		}
		evidenceChanged, err := service.requireSafeDrain(ctx, rollout, &state)
		if err != nil {
			return service.block(ctx, rollout, state, "barrier_unknown", "drain GET did not return complete safe evidence")
		}
		if evidenceChanged {
			if _, err := service.save(ctx, &rollout, state); err != nil {
				return ReconcileResultDTO{}, err
			}
		}
		if state.DrainDeadlineAt == nil || !service.clock.Now().UTC().Before(*state.DrainDeadlineAt) {
			return service.fail(ctx, rollout, state, "drain_timeout", "fresh SAFE_TO_STOP evidence was not persisted before the drain deadline")
		}
		deleteErr := service.runtime.DeletePod(ctx, DeletePodDTO{
			Namespace: rollout.Namespace, Name: state.CurrentPodName, UID: state.CurrentPodUID, ResourceVersion: state.CurrentPodRV,
		})
		latest, inspectErr := service.runtime.InspectWorkload(ctx, rollout)
		if inspectErr != nil {
			return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
		}
		current, stillFound := findPod(latest.Pods, ordinal)
		if !stillFound || current.UID != state.CurrentPodUID || current.Deleting {
			state.Phase = PhaseAwaitingReplacement
			if _, err := service.save(ctx, &rollout, state); err != nil {
				return ReconcileResultDTO{}, err
			}
			return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
		}
		if deleteErr != nil {
			return service.block(ctx, rollout, state, "delete_unconfirmed", "Pod delete result is unknown; the same UID and resourceVersion remain")
		}
		return service.block(ctx, rollout, state, "delete_unconfirmed", "Pod delete was accepted but the current UID remains")
	case PhaseAwaitingReplacement:
		return service.awaitReplacement(ctx, rollout, state, workload, features, metadata, pod, found)
	default:
		return service.fail(ctx, rollout, state, "unknown_phase", "rollout status phase is not recognized")
	}
}

func (service *Service) preflight(ctx context.Context, rollout RolloutDTO, state StatusDTO) (WorkloadDTO, ClusterFeaturesDTO, ImageMetadataDTO, error) {
	workload, err := service.runtime.InspectWorkload(ctx, rollout)
	if err != nil {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, err
	}
	if workload.Name != rollout.WorkloadName || workload.UID == "" || workload.Replicas != rolloutdomain.NodeCount ||
		workload.UpdateStrategy != "OnDelete" || workload.ObservedGeneration < workload.Generation {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("StatefulSet must be observed, three-replica, and OnDelete")
	}
	if state.WorkloadUID != "" && workload.UID != state.WorkloadUID {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("StatefulSet UID changed during rollout")
	}
	if !validImageDigest(rollout.TargetImage) {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("target image must use an immutable platform manifest digest")
	}
	if _, err := release.Parse(rollout.TargetVersion); err != nil || !release.ValidFeatureSet(rollout.TargetFeatures) {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("target version or sorted feature set is invalid")
	}
	var templateImage string
	matches := 0
	for _, container := range workload.Containers {
		if container.Name == "narumq" {
			templateImage = container.Image
			matches++
		}
	}
	if matches != 1 || templateImage != rollout.TargetImage {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("StatefulSet narumq container image must equal the CR target digest")
	}
	if !slices.Contains(workload.ImagePullSecrets, rollout.RegistrySecretName) {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("StatefulSet must reference the same-namespace OCI credentials Secret")
	}
	targetMetadata, err := service.images.ReadImageMetadata(ctx, rollout.Namespace, rollout.TargetImage, rollout.RegistrySecretName)
	if err != nil {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, fmt.Errorf("target OCI metadata unavailable: %w", err)
	}
	if targetMetadata.ManifestDigest != digestFromImage(rollout.TargetImage) || targetMetadata.OS == "" || targetMetadata.Architecture == "" ||
		targetMetadata.Version != rollout.TargetVersion || !slices.Equal(targetMetadata.Features, rollout.TargetFeatures) {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("target OCI labels do not match the CR identity or platform")
	}
	features, err := service.broker.ClusterFeatures(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret)
	if err != nil {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, fmt.Errorf("committed broker profiles unavailable: %w", err)
	}
	var members [rolloutdomain.NodeCount]rolloutdomain.ReleaseProfile
	var orderedMembers [rolloutdomain.NodeCount]MemberProfileDTO
	var memberSeen [rolloutdomain.NodeCount]bool
	for _, member := range features.Members {
		if member.NodeID < 1 || member.NodeID > rolloutdomain.NodeCount || memberSeen[member.NodeID-1] {
			return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("committed broker profiles do not map one-to-one to three ordinals")
		}
		memberSeen[member.NodeID-1] = true
		orderedMembers[member.NodeID-1] = member
		members[member.NodeID-1] = rolloutdomain.ReleaseProfile{NodeID: member.NodeID, Version: member.ReleaseVersion, Features: member.Features}
	}
	for _, exists := range memberSeen {
		if !exists {
			return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("committed broker profiles do not cover all ordinals")
		}
	}
	if err := rolloutdomain.ValidateCompatibility(rolloutdomain.ReleaseProfile{Version: targetMetadata.Version, Features: targetMetadata.Features}, members[:], features.ActiveFeatures); err != nil {
		return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, err
	}
	var pods [rolloutdomain.NodeCount]PodDTO
	var podSeen [rolloutdomain.NodeCount]bool
	for _, pod := range workload.Pods {
		if pod.Ordinal < 0 || pod.Ordinal >= rolloutdomain.NodeCount || pod.Name != fmt.Sprintf("%s-%d", workload.Name, pod.Ordinal) ||
			pod.UID == "" || pod.ResourceVersion == "" {
			return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("StatefulSet Pod identity does not match its stable ordinal")
		}
		if podSeen[pod.Ordinal] {
			return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("multiple Pods map to one StatefulSet ordinal")
		}
		podSeen[pod.Ordinal], pods[pod.Ordinal] = true, pod
	}
	pendingReplacement := state.Phase == PhaseAwaitingReplacement || state.Phase == PhaseDeleteApproved
	for ordinal := int32(0); ordinal < rolloutdomain.NodeCount; ordinal++ {
		if !podSeen[ordinal] {
			if pendingReplacement && ordinal == state.CurrentOrdinal {
				continue
			}
			return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("StatefulSet ordinal has no Pod")
		}
		pod := pods[ordinal]
		if pod.Deleting && (!pendingReplacement || ordinal != state.CurrentOrdinal) {
			return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("unexpected StatefulSet Pod deletion is in progress")
		}
		if !validImageDigest(pod.Image) {
			return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("Pod image spec does not use an immutable digest")
		}
		imageMetadata, metadataErr := service.images.ReadImageMetadata(ctx, rollout.Namespace, pod.Image, rollout.RegistrySecretName)
		if metadataErr != nil || imageMetadata.ManifestDigest != digestFromImage(pod.Image) ||
			!podRuntimeImageMatches(pod, imageMetadata) || pod.OS == "" || pod.Architecture == "" ||
			imageMetadata.OS != pod.OS || imageMetadata.Architecture != pod.Architecture ||
			targetMetadata.OS != pod.OS || targetMetadata.Architecture != pod.Architecture {
			return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("Pod OCI metadata is unavailable, not digest-bound, or targets another platform")
		}
		member := &orderedMembers[ordinal]
		if !(pendingReplacement && ordinal == state.CurrentOrdinal && pod.Image == rollout.TargetImage && pod.UID != state.CurrentPodUID) {
			if imageMetadata.Version != member.ReleaseVersion || !slices.Equal(imageMetadata.Features, member.Features) {
				return WorkloadDTO{}, ClusterFeaturesDTO{}, ImageMetadataDTO{}, errors.New("Pod OCI labels disagree with the committed broker profile")
			}
		}
	}
	return workload, features, targetMetadata, nil
}

func (service *Service) pollDrain(ctx context.Context, rollout RolloutDTO, state StatusDTO) (ReconcileResultDTO, error) {
	if state.DrainDeadlineAt == nil || !service.clock.Now().Before(*state.DrainDeadlineAt) {
		return service.fail(ctx, rollout, state, "drain_timeout", "persisted drain deadline elapsed before a safe observation")
	}
	nodeID := uint32(state.CurrentOrdinal + 1)
	observation, found, err := service.broker.GetDrain(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret, nodeID, state.AttemptKey)
	if err != nil {
		return service.waitOrTimeout(ctx, rollout, state, "drain_observation_unknown", "drain API result is unknown")
	}
	if !found {
		if state.DrainOperationID != "" {
			return service.block(ctx, rollout, state, "drain_operation_missing", "persisted drain operation is no longer observable")
		}
		observation, err = service.broker.StartDrain(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret, nodeID, state.AttemptKey)
		if err != nil {
			return service.waitOrTimeout(ctx, rollout, state, "drain_start_unconfirmed", "drain start result is unknown")
		}
	} else if state.DrainOperationID == "" {
		// A matching idempotency-key replay proves a previously uncertain POST belongs to this attempt.
		observation, err = service.broker.StartDrain(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret, nodeID, state.AttemptKey)
		if err != nil {
			return service.waitOrTimeout(ctx, rollout, state, "drain_start_unconfirmed", "existing drain ownership could not be confirmed")
		}
	} else if observation.OperationID != state.DrainOperationID {
		return service.fail(ctx, rollout, state, "drain_operation_mismatch", "observed operation does not match the persisted drain operation")
	}
	if observation.NodeID != nodeID || observation.OperationID == "" || observation.DeadlineAt.IsZero() {
		return service.block(ctx, rollout, state, "drain_observation_invalid", "drain response does not identify the requested node and operation")
	}
	state.DrainOperationID = observation.OperationID
	service.recordDrain(&state, observation)
	if observation.Phase == "FAILED" || observation.FailurePresent {
		return service.fail(ctx, rollout, state, nonempty(observation.FailureCode, "drain_failed"), "broker drain entered FAILED")
	}
	if rolloutdomain.SafeToStop(observation.Phase, observation.State, observation.SafeToStop, observation.FailurePresent) &&
		observation.ConnectionsRemaining == 0 && observation.SessionsFailed == 0 && observation.PendingWAL == 0 && observation.UnderReplicated == 0 {
		now := service.clock.Now().UTC()
		if state.DrainDeadlineAt == nil || !now.Before(*state.DrainDeadlineAt) {
			return service.fail(ctx, rollout, state, "drain_timeout", "SAFE_TO_STOP was first observed at or after the persisted deadline")
		}
		state.SafeToStopObservedAt = &now
		state.Phase = PhaseDrainSafe
		if _, err := service.save(ctx, &rollout, state); err != nil {
			return ReconcileResultDTO{}, err
		}
		return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
	}
	return service.waitOrTimeout(ctx, rollout, state, "drain_incomplete", "drain barrier is not completely and safely satisfied")
}

func (service *Service) approveDelete(ctx context.Context, rollout RolloutDTO, state StatusDTO) (ReconcileResultDTO, error) {
	evidenceChanged, err := service.requireSafeDrain(ctx, rollout, &state)
	if err != nil {
		return service.block(ctx, rollout, state, "barrier_unknown", "fresh drain evidence is not a complete SAFE_TO_STOP result")
	}
	if state.SafeToStopObservedAt == nil || state.DrainDeadlineAt == nil || !state.SafeToStopObservedAt.Before(*state.DrainDeadlineAt) {
		return service.fail(ctx, rollout, state, "drain_timeout", "SAFE_TO_STOP was not observed before the persisted deadline")
	}
	if evidenceChanged {
		if _, err := service.save(ctx, &rollout, state); err != nil {
			return ReconcileResultDTO{}, err
		}
	}
	if state.DrainDeadlineAt == nil || !service.clock.Now().UTC().Before(*state.DrainDeadlineAt) {
		return service.fail(ctx, rollout, state, "drain_timeout", "fresh SAFE_TO_STOP evidence was not persisted before the approval deadline")
	}
	if err := service.audit.Ready(); err != nil {
		return service.block(ctx, rollout, state, "audit_unavailable", "durable audit publisher is unavailable")
	}
	now := service.clock.Now().UTC()
	action := "rollout.pod.delete_approved"
	eventID := eventID(rollout.UID, state.CurrentOrdinal, state.CurrentPodUID, state.CurrentPodRV, state.AttemptKey, action)
	event := portsaudit.AuditEventDTO{
		SchemaVersion: "1.0",
		EventID:       eventID,
		OccurredAt:    now,
		RequestID:     eventID,
		Producer:      portsaudit.AuditProducerDTO{Component: "ROLLOUT_CONTROLLER", InstanceID: service.config.ProducerInstanceID, Version: service.config.ProducerVersion},
		Actor:         portsaudit.AuditActorDTO{Subject: "narumq-rollout-controller"},
		Action:        action,
		Target:        portsaudit.AuditTargetDTO{Type: "broker_pod", ID: state.CurrentPodName},
		Result:        portsaudit.AuditResultAllowed,
		Reason:        "safe_to_stop",
		Details: map[string]any{
			"rollout": rollout.Name, "rolloutUid": rollout.UID, "workload": rollout.WorkloadName, "ordinal": state.CurrentOrdinal,
			"nodeId": state.CurrentOrdinal + 1, "podUid": state.CurrentPodUID, "podResourceVersion": state.CurrentPodRV,
			"attemptKey": state.AttemptKey, "drainOperationId": state.DrainOperationID, "targetImage": rollout.TargetImage,
			"targetReleaseVersion": rollout.TargetVersion, "drainPhase": state.DrainPhase, "drainState": state.DrainState,
			"drainSafeToStop": state.DrainSafeToStop, "connectionsInitial": state.DrainConnectionsInitial,
			"connectionsRemaining": state.DrainConnectionsRemaining, "sessionsReady": state.DrainSessionsReady,
			"sessionsTakenOver": state.DrainSessionsTakenOver, "sessionsFailed": state.DrainSessionsFailed,
			"pendingWAL": state.DrainPendingWAL, "underReplicated": state.DrainUnderReplicated,
			"drainFailureCode": state.DrainFailureCode, "blockingReason": state.DrainBlockingReason,
			"safeToStopObservedAt": state.SafeToStopObservedAt.UTC().Format(time.RFC3339Nano),
		},
	}
	if err := auditapp.ValidateEvent(event); err != nil {
		return service.fail(ctx, rollout, state, "audit_invalid", "controller audit event does not satisfy Audit Event v1")
	}
	if err := service.publishAudit(ctx, event); err != nil {
		return service.block(ctx, rollout, state, "audit_unavailable", "durable audit append or fsync failed")
	}
	state.Phase, state.DeleteIntent = PhaseDeleteApproved, true
	state.FailureCode, state.FailureReason = "", ""
	if _, err := service.save(ctx, &rollout, state); err != nil {
		return ReconcileResultDTO{}, err
	}
	return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
}

func (service *Service) requireSafeDrain(ctx context.Context, rollout RolloutDTO, state *StatusDTO) (bool, error) {
	now := service.clock.Now().UTC()
	if state.DrainDeadlineAt == nil || !now.Before(*state.DrainDeadlineAt) {
		return false, errors.New("persisted drain deadline elapsed before fresh approval evidence")
	}
	before := *state
	observation, found, err := service.broker.GetDrain(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret, uint32(state.CurrentOrdinal+1), state.AttemptKey)
	if err != nil || !found || observation.DeadlineAt.IsZero() {
		return false, errors.New("drain GET did not return a complete operation")
	}
	now = service.clock.Now().UTC()
	if !now.Before(*state.DrainDeadlineAt) {
		return false, errors.New("fresh SAFE_TO_STOP evidence arrived at or after the persisted deadline")
	}
	if state.DrainOperationID == "" || observation.OperationID != state.DrainOperationID || observation.NodeID != uint32(state.CurrentOrdinal+1) ||
		!rolloutdomain.SafeToStop(observation.Phase, observation.State, observation.SafeToStop, observation.FailurePresent) ||
		observation.ConnectionsRemaining != 0 || observation.SessionsFailed != 0 || observation.PendingWAL != 0 || observation.UnderReplicated != 0 {
		return false, errors.New("drain GET is not a complete failure-free SAFE_TO_STOP observation")
	}
	service.recordDrain(state, observation)
	if state.SafeToStopObservedAt == nil {
		observedAt := service.clock.Now().UTC()
		if state.DrainDeadlineAt == nil || !observedAt.Before(*state.DrainDeadlineAt) {
			return false, errors.New("SAFE_TO_STOP was observed at or after the persisted deadline")
		}
		state.SafeToStopObservedAt = &observedAt
	}
	if state.DrainDeadlineAt == nil || !state.SafeToStopObservedAt.Before(*state.DrainDeadlineAt) {
		return false, errors.New("SAFE_TO_STOP evidence is not before the persisted deadline")
	}
	return !sameDrainEvidence(before, *state), nil
}

func (service *Service) awaitReplacement(ctx context.Context, rollout RolloutDTO, state StatusDTO, workload WorkloadDTO, features ClusterFeaturesDTO, metadata ImageMetadataDTO, pod *PodDTO, found bool) (ReconcileResultDTO, error) {
	if !found || pod.UID == state.CurrentPodUID || pod.Deleting || pod.Image != rollout.TargetImage ||
		!podRuntimeImageMatches(*pod, metadata) || !pod.Ready {
		return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
	}
	member := findMember(features.Members, uint32(state.CurrentOrdinal+1))
	if !podMatchesTarget(*pod, rollout, metadata, member) {
		return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
	}
	if err := service.readiness.Ready(ctx, pod.PodIP); err != nil {
		return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
	}
	if slices.Contains(state.CompletedOrdinals, state.CurrentOrdinal) {
		return service.fail(ctx, rollout, state, "ordinal_completed_twice", "StatefulSet ordinal was already advanced")
	}
	state.CompletedOrdinals = append(state.CompletedOrdinals, state.CurrentOrdinal)
	slices.Sort(state.CompletedOrdinals)
	if rollout.Abort {
		state.Phase = PhaseAborted
		state.FailureCode, state.FailureReason = "", ""
		state.DeleteIntent = false
	} else if state.CurrentOrdinal == rolloutdomain.NodeCount-1 {
		state.Phase = PhaseCompleted
		state.FailureCode, state.FailureReason = "", ""
	} else {
		state.CurrentOrdinal++
		state.CurrentPodName, state.CurrentPodUID, state.CurrentPodRV = "", "", ""
		clearDrainEvidence(&state)
		state.AttemptKey = attemptKey(rollout.UID, state.CurrentOrdinal, rollout.RetryToken)
		state.Phase = PhasePreflight
	}
	state.WorkloadUID, state.WorkloadGeneration = workload.UID, workload.Generation
	if _, err := service.save(ctx, &rollout, state); err != nil {
		return ReconcileResultDTO{}, err
	}
	if state.Phase == PhaseCompleted || state.Phase == PhaseAborted {
		if err := service.runtime.ReleaseTarget(ctx, targetClaim(rollout)); err != nil {
			return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
		}
	}
	return ReconcileResultDTO{RequeueAfter: 0, Phase: state.Phase}, nil
}

func (service *Service) completeCurrentOrdinal(ctx context.Context, rollout RolloutDTO, state StatusDTO, workload WorkloadDTO) (ReconcileResultDTO, error) {
	if slices.Contains(state.CompletedOrdinals, state.CurrentOrdinal) {
		return service.fail(ctx, rollout, state, "ordinal_completed_twice", "StatefulSet ordinal was already advanced")
	}
	state.CompletedOrdinals = append(state.CompletedOrdinals, state.CurrentOrdinal)
	slices.Sort(state.CompletedOrdinals)
	if state.CurrentOrdinal == rolloutdomain.NodeCount-1 {
		state.Phase = PhaseCompleted
	} else {
		state.CurrentOrdinal++
		state.CurrentPodName, state.CurrentPodUID, state.CurrentPodRV = "", "", ""
		clearDrainEvidence(&state)
		state.AttemptKey = attemptKey(rollout.UID, state.CurrentOrdinal, rollout.RetryToken)
		state.Phase = PhasePreflight
	}
	state.WorkloadUID, state.WorkloadGeneration = workload.UID, workload.Generation
	_, err := service.save(ctx, &rollout, state)
	if err == nil && state.Phase == PhaseCompleted {
		err = service.runtime.ReleaseTarget(ctx, targetClaim(rollout))
	}
	return ReconcileResultDTO{RequeueAfter: 0, Phase: state.Phase}, err
}

func (service *Service) abort(ctx context.Context, rollout RolloutDTO, state StatusDTO) (ReconcileResultDTO, error) {
	if state.Phase == PhaseCompleted || state.Phase == PhaseAborted {
		err := service.runtime.ReleaseTarget(ctx, targetClaim(rollout))
		return ReconcileResultDTO{Phase: state.Phase}, err
	}
	if state.Phase == PhaseAwaitingReplacement {
		workload, features, metadata, err := service.preflight(ctx, rollout, state)
		if err != nil {
			return service.block(ctx, rollout, state, "replacement_unconfirmed", "replacement evidence is unavailable or inconsistent")
		}
		pod, found := findPod(workload.Pods, state.CurrentOrdinal)
		return service.awaitReplacement(ctx, rollout, state, workload, features, metadata, pod, found)
	}
	if state.DeleteIntent {
		workload, err := service.runtime.InspectWorkload(ctx, rollout)
		if err != nil {
			return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
		}
		pod, found := findPod(workload.Pods, state.CurrentOrdinal)
		if !found || pod.UID != state.CurrentPodUID || pod.Deleting {
			state.Phase = PhaseAwaitingReplacement
			if _, err := service.save(ctx, &rollout, state); err != nil {
				return ReconcileResultDTO{}, err
			}
			return ReconcileResultDTO{RequeueAfter: 0, Phase: state.Phase}, nil
		}
	}
	if state.AttemptKey != "" && (state.Phase == PhaseDraining || state.Phase == PhaseFailed) {
		nodeID := uint32(state.CurrentOrdinal + 1)
		observation, found, err := service.broker.GetDrain(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret, nodeID, state.AttemptKey)
		if err != nil {
			return service.block(ctx, rollout, state, "abort_unconfirmed", "drain state is unknown; abort was not committed")
		}
		if found && (rolloutdomain.SafeToStop(observation.Phase, observation.State, observation.SafeToStop, observation.FailurePresent) ||
			observation.Phase == "FAILED" || observation.Phase == "CANCELLED") {
			service.recordDrain(&state, observation)
			state.Phase, state.FailureCode, state.FailureReason = PhaseAborted, "", ""
			state.DeleteIntent = false
			_, err := service.save(ctx, &rollout, state)
			if err == nil {
				err = service.runtime.ReleaseTarget(ctx, targetClaim(rollout))
			}
			return ReconcileResultDTO{Phase: state.Phase}, err
		}
		if found {
			if state.DrainOperationID != "" && state.DrainOperationID != observation.OperationID {
				return service.block(ctx, rollout, state, "abort_unconfirmed", "observed drain does not match the persisted operation")
			}
			if state.DrainOperationID == "" {
				replay, replayErr := service.broker.StartDrain(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret, nodeID, state.AttemptKey)
				if replayErr != nil || replay.OperationID == "" || replay.OperationID != observation.OperationID {
					return service.block(ctx, rollout, state, "abort_unconfirmed", "drain ownership could not be confirmed")
				}
				observation = replay
			}
			cancelled, cancelErr := service.broker.CancelDrain(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret, nodeID, state.AttemptKey)
			if cancelErr != nil || cancelled.OperationID != observation.OperationID || cancelled.NodeID != nodeID || cancelled.Phase != "CANCELLED" {
				return service.block(ctx, rollout, state, "abort_unconfirmed", "drain cancellation was not confirmed")
			}
			service.recordDrain(&state, cancelled)
		}
	}
	state.Phase, state.FailureCode, state.FailureReason = PhaseAborted, "", ""
	state.DeleteIntent = false
	_, err := service.save(ctx, &rollout, state)
	if err == nil {
		err = service.runtime.ReleaseTarget(ctx, targetClaim(rollout))
	}
	return ReconcileResultDTO{Phase: state.Phase}, err
}

func (service *Service) retry(ctx context.Context, rollout RolloutDTO, state StatusDTO, tokenHash string) (ReconcileResultDTO, error) {
	if state.AttemptKey == "" {
		return service.fail(ctx, rollout, state, "retry_without_attempt", "failed rollout has no persisted drain attempt")
	}
	nodeID := uint32(state.CurrentOrdinal + 1)
	observation, found, err := service.broker.GetDrain(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret, nodeID, state.AttemptKey)
	if err != nil {
		return service.block(ctx, rollout, state, "retry_cancel_unconfirmed", "previous failed drain state is unknown")
	}
	if found {
		if state.DrainOperationID != "" && state.DrainOperationID != observation.OperationID {
			return service.block(ctx, rollout, state, "retry_cancel_unconfirmed", "previous drain operation identity does not match status")
		}
		if state.DrainOperationID == "" {
			replay, replayErr := service.broker.StartDrain(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret, nodeID, state.AttemptKey)
			if replayErr != nil || replay.OperationID == "" || replay.OperationID != observation.OperationID {
				return service.block(ctx, rollout, state, "retry_cancel_unconfirmed", "previous drain ownership could not be confirmed")
			}
			observation = replay
		}
		if observation.Phase != "CANCELLED" {
			cancelled, cancelErr := service.broker.CancelDrain(ctx, rollout.Namespace, rollout.ManagementEndpoint, rollout.ManagementSecret, nodeID, state.AttemptKey)
			if cancelErr != nil || cancelled.OperationID != observation.OperationID || cancelled.NodeID != nodeID || cancelled.Phase != "CANCELLED" {
				return service.block(ctx, rollout, state, "retry_cancel_unconfirmed", "previous failed drain could not be cancelled")
			}
		}
	} else if state.DrainOperationID != "" {
		return service.block(ctx, rollout, state, "retry_cancel_unconfirmed", "persisted failed drain operation is no longer observable")
	}
	state.RetryTokenHash = tokenHash
	state.AttemptKey = attemptKey(rollout.UID, state.CurrentOrdinal, rollout.RetryToken)
	state.DrainOperationID, state.DrainIntentAt, state.DrainStartedAt, state.DrainDeadlineAt = "", nil, nil, nil
	state.DrainPhase, state.DrainState, state.DrainSafeToStop = "", "", nil
	state.SafeToStopObservedAt = nil
	state.DrainConnectionsInitial, state.DrainConnectionsRemaining = 0, 0
	state.DrainSessionsReady, state.DrainSessionsTakenOver, state.DrainSessionsFailed = 0, 0, 0
	state.DrainPendingWAL, state.DrainUnderReplicated = 0, 0
	state.DrainFailureCode, state.DrainBlockingReason = "", ""
	state.DeleteIntent, state.FailureCode, state.FailureReason = false, "", ""
	state.Phase = PhasePreflight
	if _, err := service.save(ctx, &rollout, state); err != nil {
		return ReconcileResultDTO{}, err
	}
	return ReconcileResultDTO{RequeueAfter: 0, Phase: state.Phase}, nil
}

func (service *Service) waitOrTimeout(ctx context.Context, rollout RolloutDTO, state StatusDTO, code, message string) (ReconcileResultDTO, error) {
	now := service.clock.Now().UTC()
	if state.DrainDeadlineAt != nil && !now.Before(*state.DrainDeadlineAt) {
		return service.fail(ctx, rollout, state, "drain_timeout", "drain did not reach SAFE_TO_STOP before its persisted deadline")
	}
	return service.block(ctx, rollout, state, code, message)
}

func (service *Service) recordDrain(state *StatusDTO, observation DrainObservationDTO) {
	state.DrainOperationID = observation.OperationID
	state.DrainPhase, state.DrainState = observation.Phase, observation.State
	safe := observation.SafeToStop != nil && *observation.SafeToStop
	state.DrainSafeToStop = &safe
	state.DrainConnectionsInitial, state.DrainConnectionsRemaining = observation.ConnectionsInitial, observation.ConnectionsRemaining
	state.DrainSessionsReady, state.DrainSessionsTakenOver, state.DrainSessionsFailed = observation.SessionsReady, observation.SessionsTakenOver, observation.SessionsFailed
	state.DrainPendingWAL, state.DrainUnderReplicated = observation.PendingWAL, observation.UnderReplicated
	state.DrainFailureCode, state.DrainBlockingReason = observation.FailureCode, observation.BlockingReason
	started, deadline := observation.StartedAt.UTC(), observation.DeadlineAt.UTC()
	if !started.IsZero() {
		state.DrainStartedAt = &started
	}
	if !deadline.IsZero() && (state.DrainDeadlineAt == nil || deadline.Before(*state.DrainDeadlineAt)) {
		state.DrainDeadlineAt = &deadline
	}
}

func (service *Service) block(ctx context.Context, rollout RolloutDTO, state StatusDTO, code, message string) (ReconcileResultDTO, error) {
	state.FailureCode, state.FailureReason = code, message
	setBlockedCondition(&state, rollout.Generation, code, message, service.clock.Now())
	if _, err := service.save(ctx, &rollout, state); err != nil {
		return ReconcileResultDTO{}, err
	}
	return ReconcileResultDTO{RequeueAfter: PollInterval, Phase: state.Phase}, nil
}

func (service *Service) fail(ctx context.Context, rollout RolloutDTO, state StatusDTO, code, message string) (ReconcileResultDTO, error) {
	now := service.clock.Now().UTC()
	state.Phase, state.FailureCode, state.FailureReason = PhaseFailed, code, message
	if service.audit.Ready() == nil {
		if state.CurrentPodName != "" && state.AttemptKey != "" {
			action := "rollout.pod.delete_denied"
			deniedID := eventID(rollout.UID, state.CurrentOrdinal, state.CurrentPodUID, state.CurrentPodRV, state.AttemptKey+"\x00"+code, action)
			denied := portsaudit.AuditEventDTO{
				SchemaVersion: "1.0", EventID: deniedID, OccurredAt: now, RequestID: deniedID,
				Producer: portsaudit.AuditProducerDTO{Component: "ROLLOUT_CONTROLLER", InstanceID: service.config.ProducerInstanceID, Version: service.config.ProducerVersion},
				Actor:    portsaudit.AuditActorDTO{Subject: "narumq-rollout-controller"}, Action: action,
				Target: portsaudit.AuditTargetDTO{Type: "broker_pod", ID: state.CurrentPodName},
				Result: portsaudit.AuditResultDenied, Reason: code,
				Details: map[string]any{"rolloutUid": rollout.UID, "nodeId": state.CurrentOrdinal + 1, "observedBlocker": code},
			}
			if auditapp.ValidateEvent(denied) == nil {
				_ = service.publishAudit(ctx, denied)
			}
		}
		action := "rollout.failed"
		auditID := eventID(rollout.UID, state.CurrentOrdinal, state.CurrentPodUID, state.CurrentPodRV, state.AttemptKey+"\x00"+code, action)
		event := portsaudit.AuditEventDTO{
			SchemaVersion: "1.0", EventID: auditID, OccurredAt: now, RequestID: auditID,
			Producer: portsaudit.AuditProducerDTO{Component: "ROLLOUT_CONTROLLER", InstanceID: service.config.ProducerInstanceID, Version: service.config.ProducerVersion},
			Actor:    portsaudit.AuditActorDTO{Subject: "narumq-rollout-controller"}, Action: action,
			Target: portsaudit.AuditTargetDTO{Type: "rollout", ID: rollout.Namespace + "/" + rollout.Name},
			Result: portsaudit.AuditResultFailed, Reason: code,
			Details: map[string]any{
				"rolloutUid": rollout.UID, "currentNode": state.CurrentOrdinal + 1, "drainOperationId": state.DrainOperationID,
				"operatorAction": message,
			},
		}
		if auditapp.ValidateEvent(event) == nil {
			_ = service.publishAudit(ctx, event)
		}
	}
	setBlockedCondition(&state, rollout.Generation, code, message, now)
	if _, err := service.save(ctx, &rollout, state); err != nil {
		return ReconcileResultDTO{}, err
	}
	return ReconcileResultDTO{Phase: state.Phase}, nil
}

func (service *Service) publishAudit(ctx context.Context, event portsaudit.AuditEventDTO) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return service.audit.PublishOnce(event)
}

func (service *Service) save(ctx context.Context, rollout *RolloutDTO, state StatusDTO) (string, error) {
	state.LastTransitionTime = service.clock.Now().UTC()
	resourceVersion, err := service.status.CompareAndSwapStatus(ctx, *rollout, rollout.ResourceVersion, state)
	if err != nil {
		return "", err
	}
	rollout.ResourceVersion = resourceVersion
	return resourceVersion, nil
}

func newStatus(rollout RolloutDTO, now time.Time) StatusDTO {
	features := slices.Clone(rollout.TargetFeatures)
	state := StatusDTO{
		Phase: PhasePreflight, AttemptKey: attemptKey(rollout.UID, 0, rollout.RetryToken),
		RetryTokenHash: retryTokenHash(rollout.RetryToken), CurrentOrdinal: 0,
		TargetImage: rollout.TargetImage, TargetVersion: rollout.TargetVersion, TargetFeatures: features,
		WorkloadName: rollout.WorkloadName, RegistrySecretName: rollout.RegistrySecretName,
		ManagementEndpoint: rollout.ManagementEndpoint, ManagementSecretName: rollout.ManagementSecretName,
		ObservedGeneration: rollout.Generation, LastTransitionTime: now.UTC(),
	}
	setReadyCondition(&state, rollout.Generation, now)
	return state
}

func targetClaim(rollout RolloutDTO) TargetClaimDTO {
	return TargetClaimDTO{Namespace: rollout.Namespace, Workload: rollout.WorkloadName, RolloutUID: rollout.UID, Rollout: rollout.Name}
}

func validRollout(rollout RolloutDTO) bool {
	return rollout.Namespace != "" && rollout.Name != "" && rollout.UID != "" && rollout.ResourceVersion != "" &&
		rollout.WorkloadName != "" && rollout.TargetImage != "" && rollout.TargetVersion != "" && len(rollout.TargetFeatures) != 0 &&
		rollout.RegistrySecretName != "" && rollout.ManagementEndpoint != "" && rollout.ManagementSecretName != "" &&
		rollout.ManagementSecret != "" && rollout.RetryToken != ""
}

func statusMatchesSpec(rollout RolloutDTO, state StatusDTO) bool {
	return state.WorkloadName == rollout.WorkloadName && state.RegistrySecretName == rollout.RegistrySecretName &&
		state.ManagementEndpoint == rollout.ManagementEndpoint && state.ManagementSecretName == rollout.ManagementSecretName &&
		state.TargetImage == rollout.TargetImage && state.TargetVersion == rollout.TargetVersion &&
		slices.Equal(state.TargetFeatures, rollout.TargetFeatures)
}

func findPod(pods []PodDTO, ordinal int32) (*PodDTO, bool) {
	for index := range pods {
		if pods[index].Ordinal == ordinal {
			return &pods[index], true
		}
	}
	return nil, false
}

func findMember(members []MemberProfileDTO, nodeID uint32) *MemberProfileDTO {
	for index := range members {
		if members[index].NodeID == nodeID {
			return &members[index]
		}
	}
	return nil
}

func podMatchesTarget(pod PodDTO, rollout RolloutDTO, metadata ImageMetadataDTO, member *MemberProfileDTO) bool {
	return pod.Image == rollout.TargetImage && podRuntimeImageMatches(pod, metadata) && member != nil &&
		member.ReleaseVersion == rollout.TargetVersion && slices.Equal(member.Features, rollout.TargetFeatures)
}

func setReadyCondition(state *StatusDTO, generation int64, now time.Time) {
	state.Conditions = []ConditionDTO{{Type: "Ready", Status: "True", Reason: "PreflightAccepted", Message: "rollout evidence is being reconciled", ObservedGeneration: generation, LastTransitionTime: now.UTC()}}
}

func setBlockedCondition(state *StatusDTO, generation int64, reason, message string, now time.Time) {
	state.Conditions = []ConditionDTO{{Type: "Ready", Status: "False", Reason: reason, Message: message, ObservedGeneration: generation, LastTransitionTime: now.UTC()}}
}

func retryTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func attemptKey(uid string, ordinal int32, retryToken string) string {
	identity := fmt.Sprintf("%s\x00%d\x00%s", uid, ordinal, retryTokenHash(retryToken))
	sum := sha256.Sum256([]byte(identity))
	return "rollout-" + hex.EncodeToString(sum[:])
}

func eventID(uid string, ordinal int32, podUID, podRV, key, action string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{uid, fmt.Sprint(ordinal), podUID, podRV, key, action}, "\x00")))
	return "rollout-" + hex.EncodeToString(sum[:])
}

func validImageDigest(image string) bool {
	return digestFromImage(image) != "" && strings.Contains(image, "@sha256:")
}

func digestFromImage(image string) string {
	index := strings.LastIndex(image, "@sha256:")
	if index <= 0 {
		return ""
	}
	digest := image[index+1:]
	if len(digest) != len("sha256:")+64 || digest[:7] != "sha256:" || !lowerHex(digest[7:]) {
		return ""
	}
	return digest
}

func digestFromImageID(imageID string) string {
	if index := strings.LastIndex(imageID, "@sha256:"); index >= 0 {
		return parseImageDigest(imageID[index+1:])
	}
	if strings.HasPrefix(imageID, "containerd://sha256:") {
		return parseImageDigest(strings.TrimPrefix(imageID, "containerd://"))
	}
	return parseImageDigest(imageID)
}

func parseImageDigest(value string) string {
	if len(value) == len("sha256:")+64 && value[:7] == "sha256:" && lowerHex(value[7:]) {
		return value
	}
	return ""
}
func podRuntimeImageMatches(pod PodDTO, metadata ImageMetadataDTO) bool {
	digest := digestFromImageID(pod.ImageID)
	return digest != "" && (digest == metadata.ManifestDigest || metadata.ConfigDigest != "" && digest == metadata.ConfigDigest)
}

func lowerHex(value string) bool {
	for _, character := range value {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return len(value) == 64
}

func nonempty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func clearDrainEvidence(state *StatusDTO) {
	state.DrainIntentAt, state.DrainStartedAt, state.DrainDeadlineAt, state.SafeToStopObservedAt = nil, nil, nil, nil
	state.DrainOperationID, state.DrainPhase, state.DrainState = "", "", ""
	state.DrainSafeToStop, state.DeleteIntent = nil, false
	state.DrainConnectionsInitial, state.DrainConnectionsRemaining = 0, 0
	state.DrainSessionsReady, state.DrainSessionsTakenOver, state.DrainSessionsFailed = 0, 0, 0
	state.DrainPendingWAL, state.DrainUnderReplicated = 0, 0
	state.DrainFailureCode, state.DrainBlockingReason = "", ""
}

func sameDrainEvidence(left, right StatusDTO) bool {
	return left.DrainOperationID == right.DrainOperationID &&
		left.DrainPhase == right.DrainPhase && left.DrainState == right.DrainState &&
		sameBool(left.DrainSafeToStop, right.DrainSafeToStop) &&
		left.DrainConnectionsInitial == right.DrainConnectionsInitial &&
		left.DrainConnectionsRemaining == right.DrainConnectionsRemaining &&
		left.DrainSessionsReady == right.DrainSessionsReady &&
		left.DrainSessionsTakenOver == right.DrainSessionsTakenOver &&
		left.DrainSessionsFailed == right.DrainSessionsFailed &&
		left.DrainPendingWAL == right.DrainPendingWAL &&
		left.DrainUnderReplicated == right.DrainUnderReplicated &&
		left.DrainFailureCode == right.DrainFailureCode &&
		left.DrainBlockingReason == right.DrainBlockingReason &&
		sameTime(left.DrainStartedAt, right.DrainStartedAt) &&
		sameTime(left.DrainDeadlineAt, right.DrainDeadlineAt) &&
		sameTime(left.SafeToStopObservedAt, right.SafeToStopObservedAt)
}

func sameBool(left, right *bool) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func sameTime(left, right *time.Time) bool {
	return left == nil && right == nil || left != nil && right != nil && left.Equal(*right)
}
