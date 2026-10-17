// Package rollout coordinates drain-gated, immutable-image StatefulSet replacement.
package rollout

import (
	"context"
	portsaudit "narumq/internal/application/ports/audit"
	"time"
)

const (
	PhasePreflight           = "Preflight"
	PhaseDraining            = "Draining"
	PhaseDrainSafe           = "DrainSafe"
	PhaseDeleteApproved      = "DeleteApproved"
	PhaseAwaitingReplacement = "AwaitingReplacement"
	PhaseFailed              = "Failed"
	PhaseAborted             = "Aborted"
	PhaseCompleted           = "Completed"
	PollInterval             = 5 * time.Second
	DefaultDrainTimeout      = 10 * time.Minute
)

type RolloutDTO struct {
	Namespace            string
	Name                 string
	UID                  string
	Generation           int64
	ResourceVersion      string
	WorkloadName         string
	TargetImage          string
	TargetVersion        string
	TargetFeatures       []string
	RegistrySecretName   string
	ManagementEndpoint   string
	ManagementSecret     string
	ManagementSecretName string
	RetryToken           string
	Abort                bool
	Status               StatusDTO
}

type StatusDTO struct {
	Phase                string
	ObservedGeneration   int64
	AttemptKey           string
	RetryTokenHash       string
	CurrentOrdinal       int32
	CurrentPodName       string
	CurrentPodUID        string
	CurrentPodRV         string
	DrainOperationID     string
	DrainIntentAt        *time.Time
	DrainStartedAt       *time.Time
	DrainDeadlineAt      *time.Time
	DrainPhase           string
	DrainState           string
	DrainSafeToStop      *bool
	SafeToStopObservedAt *time.Time
	// ApprovalDeadlineAt bounds approval and DELETE after a SAFE_TO_STOP re-approval; nil means DrainDeadlineAt applies.
	ApprovalDeadlineAt        *time.Time
	DrainConnectionsInitial   uint64
	DrainConnectionsRemaining uint64
	DrainSessionsReady        uint64
	DrainSessionsTakenOver    uint64
	DrainSessionsFailed       uint64
	DrainPendingWAL           uint64
	DrainUnderReplicated      uint64
	DrainFailureCode          string
	DrainBlockingReason       string
	DeleteIntent              bool
	FailureCode               string
	Conditions                []ConditionDTO
	FailureReason             string
	CompletedOrdinals         []int32
	TargetImage               string
	TargetVersion             string
	TargetFeatures            []string
	WorkloadUID               string
	WorkloadName              string
	RegistrySecretName        string
	ManagementEndpoint        string
	ManagementSecretName      string
	WorkloadGeneration        int64
	LastTransitionTime        time.Time
}

type ConditionDTO struct {
	Type               string
	Status             string
	Reason             string
	Message            string
	ObservedGeneration int64
	LastTransitionTime time.Time
}

type ContainerDTO struct {
	Name  string
	Image string
}

type PodDTO struct {
	Name            string
	Ordinal         int32
	UID             string
	ResourceVersion string
	Image           string
	ImageID         string
	PodIP           string
	OS              string
	Architecture    string
	Ready           bool
	Deleting        bool
}

type WorkloadDTO struct {
	Name               string
	UID                string
	ResourceVersion    string
	Generation         int64
	ObservedGeneration int64
	Replicas           int32
	UpdateStrategy     string
	Containers         []ContainerDTO
	ImagePullSecrets   []string
	Pods               []PodDTO
}

type ImageMetadataDTO struct {
	Image          string
	ManifestDigest string
	ConfigDigest   string
	OS             string
	Architecture   string
	Version        string
	Features       []string
}

type MemberProfileDTO struct {
	NodeID         uint32
	ReleaseVersion string
	Features       []string
}

type ClusterFeaturesDTO struct {
	ActiveFeatures []string
	Members        []MemberProfileDTO
}

type DrainObservationDTO struct {
	OperationID          string
	NodeID               uint32
	State                string
	Phase                string
	StartedAt            time.Time
	DeadlineAt           time.Time
	ConnectionsInitial   uint64
	ConnectionsRemaining uint64
	SessionsReady        uint64
	SessionsTakenOver    uint64
	SessionsFailed       uint64
	PendingWAL           uint64
	UnderReplicated      uint64
	SafeToStop           *bool
	FailureCode          string
	BlockingReason       string
	FailurePresent       bool
}

type TargetClaimDTO struct {
	Namespace  string
	Workload   string
	RolloutUID string
	Rollout    string
}

type DeletePodDTO struct {
	Namespace       string
	Name            string
	UID             string
	ResourceVersion string
}

type ReconcileResultDTO struct {
	RequeueAfter time.Duration
	Phase        string
}

type StatusStore interface {
	CompareAndSwapStatus(context.Context, RolloutDTO, string, StatusDTO) (string, error)
}

type Runtime interface {
	ClaimTarget(context.Context, TargetClaimDTO) (bool, error)
	ReleaseTarget(context.Context, TargetClaimDTO) error
	InspectWorkload(context.Context, RolloutDTO) (WorkloadDTO, error)
	DeletePod(context.Context, DeletePodDTO) error
}

type ImageMetadataReader interface {
	ReadImageMetadata(context.Context, string, string, string) (ImageMetadataDTO, error)
}

type Broker interface {
	ClusterFeatures(context.Context, string, string, string) (ClusterFeaturesDTO, error)
	GetDrain(context.Context, string, string, string, uint32, string) (DrainObservationDTO, bool, error)
	StartDrain(context.Context, string, string, string, uint32, string) (DrainObservationDTO, error)
	CancelDrain(context.Context, string, string, string, uint32, string) (DrainObservationDTO, error)
}

type ReadinessProbe interface {
	Ready(context.Context, string) error
}

type AuditPublisher interface {
	Ready() error
	PublishOnce(portsaudit.AuditEventDTO) error
}

type Clock interface {
	Now() time.Time
}
