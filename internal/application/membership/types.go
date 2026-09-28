// Package membership owns committed cluster membership and replicated session state.
package membership

import "narumq/internal/domain/nodedrain"

// SchemaMajor and SchemaMinor identify the supported replicated-state contract.
const (
	SchemaMajor uint32 = 1
	SchemaMinor uint32 = 0
)

// Retained capacity is enforced deterministically at apply time and matches
// the per-node retained projection limits.
const (
	MaxRetainedMessages     = 1024
	MaxRetainedBytes        = 16 << 20
	MaxRetainedPayloadBytes = 1_000_000
)

// Offline capacity is enforced deterministically at apply time.
const (
	MaxOfflineMessagesPerSession = 64
	MaxOfflineMessages           = 4096
	MaxOfflineBytes              = 16 << 20
	MaxOfflinePayloadBytes       = 1_000_000
	MaxOfflineDetachMessageBytes = 768 << 10
)

// Durable delivery capacity is enforced deterministically at apply time.
const (
	MaxDeliveryMessagesPerSession = 64
	MaxDeliveryInboundPerSession  = 64
	MaxDeliveryEntries            = 4096
	MaxDeliveryBytes              = 16 << 20
	MaxDeliveryPayloadBytes       = 1_000_000
	MaxDeliveryTargets            = 1024
)

// OfflineAction identifies a replicated offline session/message mutation.
type OfflineAction uint32

const (
	OfflineActionUnspecified OfflineAction = iota
	OfflineActionDetach
	OfflineActionEnqueue
	OfflineActionClaim
)

// OfflineMessageDTO is one queued offline delivery.
type OfflineMessageDTO struct {
	Sequence            uint64
	Topic               string
	Payload             []byte
	QoS                 uint32
	Retain              bool
	ExpiresAtUnixMillis uint64
}

// OfflineChangeDTO detaches a session, enqueues an offline message, or claims offline messages.
type OfflineChangeDTO struct {
	TenantID                   string
	ClientID                   string
	OwnerNodeID                uint64
	Epoch                      uint64
	Action                     OfflineAction
	SessionExpiresAtUnixMillis uint64
	ProposedAtUnixMillis       uint64
	Messages                   []OfflineMessageDTO
}

// OfflineSessionStateDTO is the committed offline queue of one detached session.
type OfflineSessionStateDTO struct {
	TenantID                   string
	ClientID                   string
	Epoch                      uint64
	SessionExpiresAtUnixMillis uint64
	NextSequence               uint64
	Messages                   []OfflineMessageDTO
}

// OfflineClaimDTO carries messages returned by an authorized CLAIM mutation.
type OfflineClaimDTO struct {
	Found    bool
	Messages []OfflineMessageDTO
}

// MemberPhase is a replicated membership lifecycle phase.
type MemberPhase uint32

const (
	MemberPhaseUnspecified MemberPhase = iota
	MemberPhaseStaged
	MemberPhaseLearner
	MemberPhaseCatchingUp
	MemberPhaseVoter
	MemberPhaseLeaving
	MemberPhaseRetired
)

// MembershipAction identifies a membership record operation.
type MembershipAction uint32

const (
	MembershipActionUnspecified MembershipAction = iota
	MembershipActionStageJoin
	MembershipActionAddLearner
	MembershipActionPromoteVoter
	MembershipActionBeginRemove
	MembershipActionRemoveMember
	MembershipActionAbortStage
)

// OwnershipAction identifies an ownership mutation.
type OwnershipAction uint32

const (
	OwnershipActionUnspecified OwnershipAction = iota
	OwnershipActionAcquire
	OwnershipActionTransfer
	OwnershipActionRelease
)

// SubscriptionAction identifies a persistent subscription mutation.
type SubscriptionAction uint32

const (
	SubscriptionActionUnspecified SubscriptionAction = iota
	SubscriptionActionAdd
	SubscriptionActionRemove
)

// RetainedAction identifies a replicated retained-message mutation.
type RetainedAction uint32

const (
	RetainedActionUnspecified RetainedAction = iota
	RetainedActionReplace
	RetainedActionDelete
)

// DeliveryAction identifies a replicated durable-delivery mutation.
type DeliveryAction uint32

const (
	DeliveryActionUnspecified DeliveryAction = iota
	DeliveryActionPublish
	DeliveryActionReceive
	DeliveryActionRelease
	DeliveryActionSend
	DeliveryActionAdvance
	DeliveryActionComplete
	DeliveryActionAttach
	DeliveryActionDetach
	DeliveryActionDiscard
)

// DeliveryPhase identifies an outbound durable message state.
type DeliveryPhase uint32

const (
	DeliveryPhaseUnspecified DeliveryPhase = iota
	DeliveryPhaseQueued
	DeliveryPhaseAwaitPubAck
	DeliveryPhaseAwaitPubRec
	DeliveryPhaseAwaitPubComp
)

// ConfigurationAction identifies a committed Raft configuration transition.
type ConfigurationAction uint8

const (
	ConfigurationActionUnspecified ConfigurationAction = iota
	ConfigurationActionAddLearner
	ConfigurationActionPromoteVoter
	ConfigurationActionRemoveMember
)

// NodeDrainAction identifies one committed node drain state transition.
type NodeDrainAction uint32

const (
	NodeDrainActionUnspecified NodeDrainAction = iota
	NodeDrainActionStart
	NodeDrainActionAdvance
	NodeDrainActionComplete
	NodeDrainActionFail
	NodeDrainActionCancel
	NodeDrainActionClear
)

// CommandDTO is an application-owned discriminated union for one committed command.
// Exactly one payload must be non-nil. Its fields intentionally mirror the v1
// replicated-state contract without importing adapter or generated wire types.
type CommandDTO struct {
	SchemaMajor  uint32
	SchemaMinor  uint32
	OperationID  string
	Membership   *MembershipRecordDTO
	Ownership    *OwnershipChangeDTO
	Subscription *SubscriptionChangeDTO
	Delivery     *DeliveryChangeDTO
	Retained     *RetainedChangeDTO
	Offline      *OfflineChangeDTO
	NodeDrain    *NodeDrainChangeDTO
}

// MemberIdentityDTO is a committed cluster member identity and lifecycle phase.
type MemberIdentityDTO struct {
	MemberID             uint64
	ClusterID            string
	Address              string
	DNSSAN               string
	CertificateSHA256    []byte
	ReleaseVersion       string
	ClusterProtocolMajor uint32
	ClusterProtocolMinor uint32
	StateSchemaMajor     uint32
	StateSchemaMinor     uint32
	Phase                MemberPhase
	OperationID          string
}

// MembershipRecordDTO is one current membership-operation history record.
type MembershipRecordDTO struct {
	OperationID            string
	Action                 MembershipAction
	IdempotencyKey         string
	Member                 *MemberIdentityDTO
	TargetMemberID         uint64
	ExpectedConfStateIndex uint64
	// CommittedIndex is zero in a proposal command and exact in committed history.
	CommittedIndex uint64
}

// OwnershipChangeDTO carries an epoch-fenced session ownership mutation.
type OwnershipChangeDTO struct {
	TenantID      string
	ClientID      string
	ExpectedEpoch uint64
	OwnerNodeID   uint64
	Epoch         uint64
	Action        OwnershipAction
}

// OwnershipStateDTO is the current committed owner of one tenant-scoped session.
type OwnershipStateDTO struct {
	TenantID       string
	ClientID       string
	OwnerNodeID    uint64
	Epoch          uint64
	CommittedIndex uint64
}

// SubscriptionChangeDTO carries one epoch-fenced persistent subscription mutation.
type SubscriptionChangeDTO struct {
	TenantID          string
	ClientID          string
	OwnerNodeID       uint64
	Epoch             uint64
	Filter            string
	QoS               uint32
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    uint32
	Action            SubscriptionAction
}

// SubscriptionStateDTO is one committed persistent subscription.
type SubscriptionStateDTO struct {
	TenantID          string
	ClientID          string
	OwnerNodeID       uint64
	Epoch             uint64
	Filter            string
	QoS               uint32
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    uint32
}

// RetainedChangeDTO replaces or deletes the retained value of (TenantID, Topic).
// Timestamps are proposer Unix milliseconds; apply never reads a local clock.
type RetainedChangeDTO struct {
	TenantID             string
	Topic                string
	PublisherClientID    string
	QoS                  uint32
	Payload              []byte
	ExpiresAtUnixMillis  uint64
	ProposedAtUnixMillis uint64
	Action               RetainedAction
}

// RetainedStateDTO is one committed retained value.
type RetainedStateDTO struct {
	TenantID            string
	Topic               string
	PublisherClientID   string
	QoS                 uint32
	Payload             []byte
	ExpiresAtUnixMillis uint64
}

// DeliveryTargetDTO is an ingress-projected candidate target; apply revalidates it.
type DeliveryTargetDTO struct {
	ClientID string
	Retain   bool
}

// DeliveryChangeDTO carries one durable delivery action.
type DeliveryChangeDTO struct {
	TenantID                   string
	ClientID                   string
	OwnerNodeID                uint64
	Epoch                      uint64
	Action                     DeliveryAction
	ProposedAtUnixMillis       uint64
	Topic                      string
	Payload                    []byte
	Retain                     bool
	ExpiresAtUnixMillis        uint64
	PacketID                   uint32
	Targets                    []DeliveryTargetDTO
	Sequence                   uint64
	Reset                      bool
	SessionExpiresAtUnixMillis uint64
	PublishQoS                 uint32
}

// DeliveryMessageDTO stores one durable outbound delivery.
type DeliveryMessageDTO struct {
	Sequence            uint64
	Topic               string
	Payload             []byte
	QoS                 uint32
	Retain              bool
	ExpiresAtUnixMillis uint64
	PacketID            uint32
	Phase               DeliveryPhase
}

// DeliveryInboundDTO stores a publisher-side QoS 2 exchange or RELEASE tombstone.
type DeliveryInboundDTO struct {
	PacketID            uint32
	Topic               string
	Payload             []byte
	Retain              bool
	ExpiresAtUnixMillis uint64
	Released            bool
	ReleasedIndex       uint64
}

// DeliverySessionStateDTO is one session's durable queue and inbound QoS 2 state.
type DeliverySessionStateDTO struct {
	TenantID                   string
	ClientID                   string
	SessionExpiresAtUnixMillis uint64
	NextSequence               uint64
	Messages                   []DeliveryMessageDTO
	Inbound                    []DeliveryInboundDTO
}

type DeliveryEnqueuedDTO struct {
	ClientID string
	Sequence uint64
}

type DeliveryResultDTO struct {
	Found     bool
	Released  bool
	Enqueued  []DeliveryEnqueuedDTO
	Saturated []string
	Skipped   []string
	Session   *DeliverySessionStateDTO
}

// ApplyOutcome identifies whether a committed command changed replicated state.
type ApplyOutcome uint8

const (
	ApplyOutcomeApplied ApplyOutcome = iota + 1
	ApplyOutcomeConflict
)

// ApplyConflictCode is a deterministic, adapter-independent domain rejection.
type ApplyConflictCode string

const (
	ConflictNone                     ApplyConflictCode = ""
	ConflictMemberIDReserved         ApplyConflictCode = "member_id_reserved"
	ConflictMemberIdentity           ApplyConflictCode = "member_identity_conflict"
	ConflictIdempotency              ApplyConflictCode = "idempotency_conflict"
	ConflictOperationID              ApplyConflictCode = "operation_id_conflict"
	ConflictMembershipInProgress     ApplyConflictCode = "membership_change_in_progress"
	ConflictOperationNotFound        ApplyConflictCode = "operation_not_found"
	ConflictPhase                    ApplyConflictCode = "phase_conflict"
	ConflictConfigurationMismatch    ApplyConflictCode = "configuration_mismatch"
	ConflictStaleEpoch               ApplyConflictCode = "stale_epoch"
	ConflictOwnershipMissing         ApplyConflictCode = "ownership_missing"
	ConflictMemberNotFound           ApplyConflictCode = "member_not_found"
	ConflictCompatibilityUnsupported ApplyConflictCode = "compatibility_unsupported"
	ConflictRetainedCapacity         ApplyConflictCode = "retained_capacity_exceeded"
	ConflictOfflineCapacity          ApplyConflictCode = "offline_capacity_exceeded"
	ConflictOfflineSessionMissing    ApplyConflictCode = "offline_session_missing"
	ConflictDeliveryCapacity         ApplyConflictCode = "delivery_capacity_exceeded"
	ConflictDeliveryMissing          ApplyConflictCode = "delivery_missing"
	ConflictDeliveryPacketID         ApplyConflictCode = "delivery_packet_id_conflict"
	ConflictNodeDrainActive          ApplyConflictCode = "node_drain_active"
	ConflictNodeDrainInProgress      ApplyConflictCode = "node_drain_in_progress"
	ConflictDrainNotCancellable      ApplyConflictCode = "drain_not_cancellable"
	ConflictDeliverySessionMissing   ApplyConflictCode = "delivery_session_missing"
)

// ApplyResultDTO reports the deterministic result at a committed application index.
type ApplyResultDTO struct {
	Index        uint64
	Outcome      ApplyOutcome
	Conflict     ApplyConflictCode
	Operation    *MembershipOperationDTO
	Ownership    *OwnershipStateDTO
	Subscription *SubscriptionStateDTO
	Delivery     *DeliveryResultDTO
	OfflineClaim *OfflineClaimDTO
	NodeDrain    *NodeDrainStateDTO
}

// MembershipOperationPhase is the management-facing current operation status.
type MembershipOperationPhase string

const (
	OperationPhaseStaged       MembershipOperationPhase = "STAGED"
	OperationPhaseLearner      MembershipOperationPhase = "LEARNER"
	OperationPhaseCatchingUp   MembershipOperationPhase = "CATCHING_UP"
	OperationPhasePromoting    MembershipOperationPhase = "PROMOTING"
	OperationPhaseTransferring MembershipOperationPhase = "TRANSFERRING"
	OperationPhaseRemoving     MembershipOperationPhase = "REMOVING"
	OperationPhaseSucceeded    MembershipOperationPhase = "SUCCEEDED"
	OperationPhaseAborted      MembershipOperationPhase = "ABORTED"
)

// MembershipOperationDTO is a read-only view of the latest committed operation record.
type MembershipOperationDTO struct {
	OperationID    string
	Action         MembershipAction
	MemberID       uint64
	Phase          MembershipOperationPhase
	CommittedIndex uint64
	// Replayed is true only on a mutation result returned for an idempotent retry.
	Replayed bool
	Record   MembershipRecordDTO
}

// StartNodeDrainRequestDTO contains the operator's validated drain parameters.
type StartNodeDrainRequestDTO struct {
	MemberID                           uint64
	IdempotencyKey, ServerReference    string
	ConnectionRatePerSecond            uint32
	LoadBalancerPropagationDelayMillis uint64
	TimeoutMillis                      uint64
}

// NodeDrainProgressDTO reports the committed safe-to-stop progress counters.
type NodeDrainProgressDTO struct {
	ConnectionsInitial, ConnectionsRemaining    uint64
	SessionsReadyForTakeover, SessionsTakenOver uint64
	SessionsFailed, PendingWAL, UnderReplicated uint64
}

// NodeDrainChangeDTO carries one deterministic replicated drain transition.
type NodeDrainChangeDTO struct {
	Action                             NodeDrainAction
	MemberID                           uint64
	DrainOperationID, IdempotencyKey   string
	ServerReference                    string
	ConnectionRatePerSecond            uint32
	LoadBalancerPropagationDelayMillis uint64
	TimeoutMillis                      uint64
	Phase                              nodedrain.Phase
	Progress                           NodeDrainProgressDTO
	FailureCode, BlockingReason        string
	ProposedAtUnixMillis               uint64
}

// NodeDrainStateDTO is the committed state of one node drain operation.
type NodeDrainStateDTO struct {
	DrainOperationID                          string
	MemberID                                  uint64
	IdempotencyKey                            string
	Phase                                     nodedrain.Phase
	ServerReference                           string
	ConnectionRatePerSecond                   uint32
	LoadBalancerPropagationDelayMillis        uint64
	TimeoutMillis                             uint64
	StartedAtUnixMillis, DeadlineAtUnixMillis uint64
	PhaseStartedAtUnixMillis                  uint64
	Progress                                  NodeDrainProgressDTO
	FailureCode, BlockingReason               string
	CommittedIndex                            uint64
}

// NodeDrainOperationDTO combines one drain state with retry classification.
type NodeDrainOperationDTO struct {
	State    NodeDrainStateDTO
	Replayed bool
}

// SnapshotDTO is the canonical application view of a replicated state snapshot.
// Digest is opaque adapter-verified envelope data; an empty Digest asks the adapter
// to compute the contract's deterministic protobuf state digest when encoding.
type SnapshotDTO struct {
	SchemaMajor       uint32
	SchemaMinor       uint32
	LastIncludedIndex uint64
	ObjectCount       uint64
	Digest            []byte
	Members           []MemberIdentityDTO
	Ownerships        []OwnershipStateDTO
	Subscriptions     []SubscriptionStateDTO
	DeliverySessions  []DeliverySessionStateDTO
	MembershipHistory []MembershipRecordDTO
	Retained          []RetainedStateDTO
	OfflineSessions   []OfflineSessionStateDTO
	NodeDrains        []NodeDrainStateDTO
}
