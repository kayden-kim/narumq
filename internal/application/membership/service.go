package membership

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"

	"narumq/internal/domain/release"
)

var (
	// ErrNotLeader classifies a membership port failure caused by a non-leader node.
	ErrNotLeader = errors.New("not leader")
	// ErrQuorumUnavailable classifies a membership port failure without a committed outcome.
	ErrQuorumUnavailable = errors.New("quorum unavailable")
)

// CommandPort is the application-owned boundary for committed membership mutations
// and management reads. Commit returns only after a command has a committed outcome;
// proposal acceptance alone must never be returned as a successful result. Adapters
// classify leadership failures as ErrNotLeader rather than leaking adapter errors.
type CommandPort interface {
	Commit(ctx context.Context, command CommandDTO) (ApplyResultDTO, error)
	Members(ctx context.Context) ([]MemberIdentityDTO, error)
	ActiveFeatures(ctx context.Context) ([]string, error)
	GetOperation(ctx context.Context, operationID string) (MembershipOperationDTO, bool, error)
	GetOperationByIdempotencyKey(ctx context.Context, idempotencyKey string) (MembershipOperationDTO, bool, error)
	GetOwnership(ctx context.Context, tenantID, clientID string) (OwnershipStateDTO, bool, error)
}

// StageJoinRequestDTO contains candidate identity and the caller's retry key.
type StageJoinRequestDTO struct {
	IdempotencyKey         string
	Member                 MemberIdentityDTO
	ExpectedConfStateIndex uint64
}

// ServiceErrorCode identifies a stable, HTTP-mappable membership use-case failure.
type ServiceErrorCode string

const (
	ServiceErrorInvalidRequest                ServiceErrorCode = "invalid_request"
	ServiceErrorResourceNotFound              ServiceErrorCode = "resource_not_found"
	ServiceErrorMemberIDReserved              ServiceErrorCode = "member_id_reserved"
	ServiceErrorMemberIdentityConflict        ServiceErrorCode = "member_identity_conflict"
	ServiceErrorIdempotencyConflict           ServiceErrorCode = "idempotency_conflict"
	ServiceErrorMembershipInProgress          ServiceErrorCode = "membership_change_in_progress"
	ServiceErrorReplicationFactorUnsatisfied  ServiceErrorCode = "replication_factor_unsatisfied"
	ServiceErrorOperationConflict             ServiceErrorCode = "operation_conflict"
	ServiceErrorNotLeader                     ServiceErrorCode = "not_leader"
	ServiceErrorQuorumUnavailable             ServiceErrorCode = "quorum_unavailable"
	ServiceErrorCompatibilityUnsupported      ServiceErrorCode = "compatibility_unsupported"
	ServiceErrorFeatureUnsupported            ServiceErrorCode = "feature_not_supported_by_all_members"
	ServiceErrorInvalidNodeTransition         ServiceErrorCode = "invalid_node_transition"
	ServiceErrorDrainNotCancellable           ServiceErrorCode = "drain_not_cancellable"
	ServiceErrorNodeDrainInProgress           ServiceErrorCode = "node_drain_in_progress"
	ServiceErrorFeatureInactive               ServiceErrorCode = "feature_inactive"
	ServiceErrorCredentialRevocationCapacity  ServiceErrorCode = "credential_revocation_capacity_exceeded"
	ServiceErrorBlocklistCapacity             ServiceErrorCode = "blocklist_capacity_exceeded"
	ServiceErrorConnectionRebalanceInProgress ServiceErrorCode = "connection_rebalance_in_progress"
	ServiceErrorRebalanceNotCancellable       ServiceErrorCode = "rebalance_not_cancellable"
)

// ServiceError exposes a stable sanitized message and retry classification without
// retaining or exposing the adapter's internal cause.
type ServiceError struct {
	Code      ServiceErrorCode
	Message   string
	Retryable bool
	NodeState string
}

// Is preserves application-owned port classifications without exposing adapter causes.
func (serviceError *ServiceError) Is(target error) bool {
	return serviceError.Code == ServiceErrorNotLeader && target == ErrNotLeader || serviceError.Code == ServiceErrorQuorumUnavailable && target == ErrQuorumUnavailable
}

func (serviceError *ServiceError) Error() string {
	return serviceError.Message
}

// Service stages join and planned-removal operations and reads committed operation status.
type Service struct {
	port        CommandPort
	replication ReplicationProfileDTO
}

// NewService constructs a membership use case service with its application-owned port.
func NewService(port CommandPort, replication ReplicationProfileDTO) (*Service, error) {
	if port == nil {
		return nil, errors.New("membership command port is required")
	}
	if err := ValidateReplicationProfile(replication); err != nil {
		return nil, fmt.Errorf("membership service: %w", err)
	}
	return &Service{port: port, replication: replication}, nil
}

// StageJoin commits a candidate identity before returning its operation status.
func (service *Service) StageJoin(ctx context.Context, request StageJoinRequestDTO) (MembershipOperationDTO, error) {
	if ctx == nil || !validIdempotencyKey(request.IdempotencyKey) {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	member := cloneMember(request.Member)
	member.Phase = MemberPhaseStaged
	member.OperationID = ""
	if member.ClusterProtocolMajor != 2 || member.ClusterProtocolMinor != 0 || member.StateSchemaMajor != SchemaMajor || member.StateSchemaMinor != SchemaMinor {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorCompatibilityUnsupported, false)
	}
	members, err := service.port.Members(ctx)
	if err != nil {
		return MembershipOperationDTO{}, portFailure(err)
	}
	if len(members) == 0 || members[0].ClusterID == "" {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	member.ClusterID = members[0].ClusterID
	if validateMemberIdentityContent(member) != nil {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	existing, exists, err := service.port.GetOperationByIdempotencyKey(ctx, request.IdempotencyKey)
	if err != nil {
		return MembershipOperationDTO{}, portFailure(err)
	}
	if exists {
		if !validJoinAction(existing.Record.Action) || existing.Record.TargetMemberID != 0 || existing.Record.Member == nil || !sameMemberIdentity(*existing.Record.Member, member) {
			return MembershipOperationDTO{}, newServiceError(ServiceErrorIdempotencyConflict, false)
		}
		existing.Replayed = true
		return cloneOperation(existing), nil
	}
	operationID, err := newOperationID()
	if err != nil {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	member.Phase = MemberPhaseStaged
	member.OperationID = operationID
	if validateMemberIdentity(member) != nil {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	command := CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: operationID,
		Membership: &MembershipRecordDTO{
			OperationID: operationID, Action: MembershipActionStageJoin,
			IdempotencyKey: request.IdempotencyKey, Member: &member,
			ExpectedConfStateIndex: request.ExpectedConfStateIndex,
		},
	}
	return service.commitOperation(ctx, command)
}

// StartPlannedRemoval commits the LEAVING phase for an existing voter.
func (service *Service) StartPlannedRemoval(ctx context.Context, memberID uint64, idempotencyKey string, expectedConfStateIndex uint64) (MembershipOperationDTO, error) {
	if ctx == nil || memberID == 0 || !validIdempotencyKey(idempotencyKey) {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	existing, exists, err := service.port.GetOperationByIdempotencyKey(ctx, idempotencyKey)
	if err != nil {
		return MembershipOperationDTO{}, portFailure(err)
	}
	if exists {
		if !validRemoveAction(existing.Record.Action) || existing.Record.TargetMemberID != memberID {
			return MembershipOperationDTO{}, newServiceError(ServiceErrorIdempotencyConflict, false)
		}
		existing.Replayed = true
		return cloneOperation(existing), nil
	}
	members, err := service.port.Members(ctx)
	if err != nil {
		return MembershipOperationDTO{}, portFailure(err)
	}
	var member MemberIdentityDTO
	found := false
	for _, candidate := range members {
		if candidate.MemberID == memberID {
			member = cloneMember(candidate)
			found = true
			break
		}
	}
	if !found {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorResourceNotFound, false)
	}
	if member.Phase != MemberPhaseVoter {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorOperationConflict, false)
	}
	voterIDs := make([]uint64, 0, len(members))
	for _, candidate := range members {
		if candidate.Phase == MemberPhaseVoter {
			voterIDs = append(voterIDs, candidate.MemberID)
		}
	}
	if !RemovalKeepsReplicationFactor(memberID, voterIDs, service.replication) {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorReplicationFactorUnsatisfied, false)
	}
	operationID, err := newOperationID()
	if err != nil {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	member.Phase = MemberPhaseLeaving
	member.OperationID = operationID
	if validateMemberIdentity(member) != nil {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorOperationConflict, false)
	}
	command := CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: operationID,
		Membership: &MembershipRecordDTO{
			OperationID: operationID, Action: MembershipActionBeginRemove,
			IdempotencyKey: idempotencyKey, Member: &member, TargetMemberID: memberID,
			ExpectedConfStateIndex: expectedConfStateIndex,
		},
	}
	return service.commitOperation(ctx, command)
}

// GetOperation returns committed status for one canonical operation ID.
func (service *Service) GetOperation(ctx context.Context, operationID string) (MembershipOperationDTO, error) {
	if ctx == nil || !validUUID(operationID) {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	operation, exists, err := service.port.GetOperation(ctx, operationID)
	if err != nil {
		return MembershipOperationDTO{}, portFailure(err)
	}
	if !exists {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorResourceNotFound, false)
	}
	if operation.OperationID != operationID || operation.CommittedIndex == 0 {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return cloneOperation(operation), nil
}

// ClusterFeatures returns the committed feature set and profiles of non-retired members.
func (service *Service) ClusterFeatures(ctx context.Context) (FeatureStateDTO, error) {
	if ctx == nil {
		return FeatureStateDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return FeatureStateDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	members, err := service.port.Members(ctx)
	if err != nil {
		return FeatureStateDTO{}, portFailure(err)
	}
	activeFeatures, err := service.port.ActiveFeatures(ctx)
	if err != nil {
		return FeatureStateDTO{}, portFailure(err)
	}

	profiles := make([]MemberFeatureProfileDTO, 0, len(members))
	for _, member := range members {
		if member.Phase == MemberPhaseRetired {
			continue
		}
		features := slices.Clone(member.Features)
		if features == nil {
			features = []string{}
		}
		profiles = append(profiles, MemberFeatureProfileDTO{
			MemberID: member.MemberID, ReleaseVersion: member.ReleaseVersion, Features: features,
		})
	}
	sort.Slice(profiles, func(left, right int) bool { return profiles[left].MemberID < profiles[right].MemberID })
	if activeFeatures == nil {
		activeFeatures = []string{}
	} else {
		activeFeatures = slices.Clone(activeFeatures)
	}
	return FeatureStateDTO{ActiveFeatures: activeFeatures, Members: profiles}, nil
}

// ActivateFeature commits cluster-wide activation of one feature advertised by every non-retired member.
func (service *Service) ActivateFeature(ctx context.Context, feature string) (FeatureStateDTO, error) {
	if ctx == nil || !release.ValidFeature(feature) {
		return FeatureStateDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return FeatureStateDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	operationID, err := newOperationID()
	if err != nil {
		return FeatureStateDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	result, err := service.port.Commit(ctx, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: operationID,
		FeatureActivation: &FeatureActivationDTO{Feature: feature},
	})
	if err != nil {
		return FeatureStateDTO{}, portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		return FeatureStateDTO{}, conflictServiceError(result.Conflict)
	}
	if result.Outcome != ApplyOutcomeApplied || result.Index == 0 {
		return FeatureStateDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return service.ClusterFeatures(ctx)
}

func (service *Service) commitOperation(ctx context.Context, command CommandDTO) (MembershipOperationDTO, error) {
	result, err := service.port.Commit(ctx, command)
	if err != nil {
		return MembershipOperationDTO{}, portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		return MembershipOperationDTO{}, conflictServiceError(result.Conflict)
	}
	if result.Outcome != ApplyOutcomeApplied || result.Index == 0 || result.Operation == nil || result.Operation.CommittedIndex == 0 {
		return MembershipOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	operation := cloneOperation(*result.Operation)
	operation.Replayed = operation.CommittedIndex < result.Index
	return operation, nil
}

func conflictServiceError(conflict ApplyConflictCode) *ServiceError {
	switch conflict {
	case ConflictMemberIDReserved:
		return newServiceError(ServiceErrorMemberIDReserved, false)
	case ConflictMemberIdentity:
		return newServiceError(ServiceErrorMemberIdentityConflict, false)
	case ConflictIdempotency:
		return newServiceError(ServiceErrorIdempotencyConflict, false)
	case ConflictMembershipInProgress:
		return newServiceError(ServiceErrorMembershipInProgress, true)
	case ConflictOperationNotFound:
		return newServiceError(ServiceErrorResourceNotFound, false)
	case ConflictMemberNotFound:
		return newServiceError(ServiceErrorResourceNotFound, false)
	case ConflictCompatibilityUnsupported:
		return newServiceError(ServiceErrorCompatibilityUnsupported, false)
	case ConflictFeatureUnsupported:
		return newServiceError(ServiceErrorFeatureUnsupported, false)
	case ConflictFeatureInactive:
		return newServiceError(ServiceErrorFeatureInactive, false)
	case ConflictCredentialRevocationCapacity:
		return newServiceError(ServiceErrorCredentialRevocationCapacity, false)
	case ConflictBlocklistCapacity:
		return newServiceError(ServiceErrorBlocklistCapacity, false)
	default:
		return newServiceError(ServiceErrorOperationConflict, false)
	}
}

func newServiceError(code ServiceErrorCode, retryable bool) *ServiceError {
	message := "membership operation failed"
	switch code {
	case ServiceErrorInvalidRequest:
		message = "membership request is invalid"
	case ServiceErrorResourceNotFound:
		message = "membership resource was not found"
	case ServiceErrorMemberIDReserved:
		message = "member ID is reserved"
	case ServiceErrorMemberIdentityConflict:
		message = "member identity conflicts with committed state"
	case ServiceErrorIdempotencyConflict:
		message = "idempotency key was used with different content"
	case ServiceErrorMembershipInProgress:
		message = "another membership change is in progress"
	case ServiceErrorReplicationFactorUnsatisfied:
		message = "member removal would leave fewer voters than the replication factor"
	case ServiceErrorOperationConflict:
		message = "membership operation conflicts with committed state"
	case ServiceErrorNotLeader:
		message = "node is not the membership leader"
	case ServiceErrorQuorumUnavailable:
		message = "committed membership outcome is unavailable"
	case ServiceErrorCompatibilityUnsupported:
		message = "member compatibility is unsupported"
	case ServiceErrorFeatureUnsupported:
		message = "feature is not supported by every active member"
	case ServiceErrorFeatureInactive:
		message = "required cluster feature is inactive"
	case ServiceErrorCredentialRevocationCapacity:
		message = "credential revocation capacity is exhausted"
	case ServiceErrorBlocklistCapacity:
		message = "client blocklist capacity is exhausted"
	case ServiceErrorInvalidNodeTransition:
		message = "node lifecycle transition is not allowed"
	case ServiceErrorDrainNotCancellable:
		message = "node drain can no longer be cancelled"
	case ServiceErrorNodeDrainInProgress:
		message = "another node drain is in progress"
	}
	return &ServiceError{Code: code, Message: message, Retryable: retryable}
}
func portFailure(err error) *ServiceError {
	if errors.Is(err, ErrNotLeader) {
		return newServiceError(ServiceErrorNotLeader, true)
	}
	return newServiceError(ServiceErrorQuorumUnavailable, true)
}

func newOperationID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	var encoded [36]byte
	hex.Encode(encoded[0:8], value[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], value[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], value[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], value[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], value[10:16])
	return string(encoded[:]), nil
}
