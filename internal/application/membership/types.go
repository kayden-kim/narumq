// Package membership owns committed cluster membership and replicated session state.
package membership

// SchemaMajor and SchemaMinor identify the supported replicated-state contract.
const (
	SchemaMajor uint32 = 1
	SchemaMinor uint32 = 0
)

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

// InboundDeliveryPhase is the exact replicated inbound QoS checkpoint phase.
type InboundDeliveryPhase uint32

const (
	InboundPhaseUnspecified InboundDeliveryPhase = iota
	InboundQoS1AwaitAcceptance
	InboundQoS1Accepted
	InboundQoS2AwaitAcceptance
	InboundQoS2AwaitRelease
	InboundQoS2AwaitFinalAcceptance
	InboundQoS2Complete
)

// OutboundDeliveryPhase is the exact replicated outbound QoS checkpoint phase.
type OutboundDeliveryPhase uint32

const (
	OutboundPhaseUnspecified OutboundDeliveryPhase = iota
	OutboundAwaitPubAck
	OutboundAwaitPubRec
	OutboundAwaitPubComp
	OutboundComplete
)

// ConfigurationAction identifies a committed Raft configuration transition.
type ConfigurationAction uint8

const (
	ConfigurationActionUnspecified ConfigurationAction = iota
	ConfigurationActionAddLearner
	ConfigurationActionPromoteVoter
	ConfigurationActionRemoveMember
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
	Checkpoint   *SessionCheckpointDTO
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

// PublishPacketDTO contains the supported, lossless MQTT PUBLISH checkpoint fields.
type PublishPacketDTO struct {
	ProtocolVersion          uint32
	DUP                      bool
	QoS                      uint32
	Retain                   bool
	PacketID                 uint32
	Topic                    string
	Payload                  []byte
	HasMessageExpiryInterval bool
	MessageExpiryInterval    uint32
}

// InboundCheckpointDTO stores one inbound QoS 1/2 exchange and its phase.
type InboundCheckpointDTO struct {
	Publish PublishPacketDTO
	Phase   InboundDeliveryPhase
}

// OutboundCheckpointDTO stores one outbound QoS 1/2 exchange and its phase.
type OutboundCheckpointDTO struct {
	Publish PublishPacketDTO
	Phase   OutboundDeliveryPhase
}

// SessionCheckpointDTO atomically replaces checkpointed inflight session state.
type SessionCheckpointDTO struct {
	TenantID    string
	ClientID    string
	OwnerNodeID uint64
	Epoch       uint64
	Inbound     []InboundCheckpointDTO
	Outbound    []OutboundCheckpointDTO
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
)

// ApplyResultDTO reports the deterministic result at a committed application index.
type ApplyResultDTO struct {
	Index        uint64
	Outcome      ApplyOutcome
	Conflict     ApplyConflictCode
	Operation    *MembershipOperationDTO
	Ownership    *OwnershipStateDTO
	Subscription *SubscriptionStateDTO
	Checkpoint   *SessionCheckpointDTO
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
	Checkpoints       []SessionCheckpointDTO
	MembershipHistory []MembershipRecordDTO
}
