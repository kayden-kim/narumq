package membership

import (
	"context"
	"errors"

	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/domain/nodedrain"
)

// NodeDrainPort commits drain transitions and reads their quorum-visible state.
type NodeDrainPort interface {
	Commit(ctx context.Context, command CommandDTO) (ApplyResultDTO, error)
	Members(ctx context.Context) ([]MemberIdentityDTO, error)
	GetNodeDrain(ctx context.Context, memberID uint64) (NodeDrainStateDTO, bool, error)
}

// DrainService implements management operations over committed node-drain state.
type DrainService struct {
	port  NodeDrainPort
	clock applicationclock.Clock
}

// NewDrainService constructs a drain use case service with committed-state and clock ports.
func NewDrainService(port NodeDrainPort, clock applicationclock.Clock) (*DrainService, error) {
	if port == nil {
		return nil, errors.New("node drain port is required")
	}
	if clock == nil {
		return nil, errors.New("node drain clock is required")
	}
	return &DrainService{port: port, clock: clock}, nil
}

// StartNodeDrain validates, commits, or replays a drain operation for one voter.
func (service *DrainService) StartNodeDrain(ctx context.Context, request StartNodeDrainRequestDTO) (NodeDrainOperationDTO, error) {
	if ctx == nil || request.MemberID == 0 || !validIdempotencyKey(request.IdempotencyKey) ||
		nodedrain.ValidateParameters(request.ServerReference, request.ConnectionRatePerSecond,
			request.LoadBalancerPropagationDelayMillis, request.TimeoutMillis) != nil {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}

	current, exists, err := service.port.GetNodeDrain(ctx, request.MemberID)
	if err != nil {
		return NodeDrainOperationDTO{}, portFailure(err)
	}
	if exists {
		if current.Phase.Active() {
			return service.replayOrConflict(current, request)
		}
		if current.Phase == nodedrain.PhaseCancelled && current.IdempotencyKey == request.IdempotencyKey {
			return NodeDrainOperationDTO{State: current, Replayed: true}, nil
		}
	}

	members, err := service.port.Members(ctx)
	if err != nil {
		return NodeDrainOperationDTO{}, portFailure(err)
	}
	var member *MemberIdentityDTO
	for index := range members {
		if members[index].MemberID == request.MemberID {
			member = &members[index]
			break
		}
	}
	if member == nil {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorResourceNotFound, false)
	}
	if member.Phase != MemberPhaseVoter {
		state := "JOINING"
		if member.Phase == MemberPhaseRetired {
			state = "SAFE_TO_STOP"
		}
		serviceError := newServiceError(ServiceErrorInvalidNodeTransition, false)
		serviceError.NodeState = state
		return NodeDrainOperationDTO{}, serviceError
	}

	proposedAt := service.clock.Now().UnixMilli()
	if proposedAt <= 0 {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	drainOperationID, err := newOperationID()
	if err != nil {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	commandOperationID, err := newOperationID()
	if err != nil {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	command := CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: commandOperationID,
		NodeDrain: &NodeDrainChangeDTO{
			Action: NodeDrainActionStart, MemberID: request.MemberID, DrainOperationID: drainOperationID,
			IdempotencyKey: request.IdempotencyKey, ServerReference: request.ServerReference,
			ConnectionRatePerSecond:            request.ConnectionRatePerSecond,
			LoadBalancerPropagationDelayMillis: request.LoadBalancerPropagationDelayMillis,
			TimeoutMillis:                      request.TimeoutMillis, ProposedAtUnixMillis: uint64(proposedAt),
		},
	}
	result, err := service.port.Commit(ctx, command)
	if err != nil {
		return NodeDrainOperationDTO{}, portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		switch result.Conflict {
		case ConflictNodeDrainActive:
			latest, found, err := service.port.GetNodeDrain(ctx, request.MemberID)
			if err != nil {
				return NodeDrainOperationDTO{}, portFailure(err)
			}
			if found && latest.Phase.Active() {
				return service.replayOrConflict(latest, request)
			}
			return NodeDrainOperationDTO{}, newServiceError(ServiceErrorOperationConflict, false)
		case ConflictConnectionRebalanceInProgress:
			return NodeDrainOperationDTO{}, newServiceError(ServiceErrorConnectionRebalanceInProgress, true)
		case ConflictNodeDrainInProgress:
			return NodeDrainOperationDTO{}, newServiceError(ServiceErrorNodeDrainInProgress, true)
		case ConflictMembershipInProgress:
			return NodeDrainOperationDTO{}, newServiceError(ServiceErrorMembershipInProgress, true)
		case ConflictMemberNotFound:
			return NodeDrainOperationDTO{}, newServiceError(ServiceErrorResourceNotFound, false)
		case ConflictPhase:
			return NodeDrainOperationDTO{}, newServiceError(ServiceErrorInvalidNodeTransition, false)
		default:
			return NodeDrainOperationDTO{}, conflictServiceError(result.Conflict)
		}
	}
	if result.Outcome != ApplyOutcomeApplied || result.NodeDrain == nil {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return NodeDrainOperationDTO{State: *result.NodeDrain}, nil
}

// GetNodeDrain reads one committed operation or reports that it does not exist.
func (service *DrainService) GetNodeDrain(ctx context.Context, memberID uint64) (NodeDrainOperationDTO, error) {
	if ctx == nil || memberID == 0 {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	state, exists, err := service.port.GetNodeDrain(ctx, memberID)
	if err != nil {
		return NodeDrainOperationDTO{}, portFailure(err)
	}
	if !exists {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorResourceNotFound, false)
	}
	return NodeDrainOperationDTO{State: state}, nil
}

// CancelNodeDrain commits cancellation without reverting completed ownership transfers.
func (service *DrainService) CancelNodeDrain(ctx context.Context, memberID uint64) (NodeDrainOperationDTO, error) {
	if ctx == nil || memberID == 0 {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	state, exists, err := service.port.GetNodeDrain(ctx, memberID)
	if err != nil {
		return NodeDrainOperationDTO{}, portFailure(err)
	}
	if !exists {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorResourceNotFound, false)
	}
	if state.Phase == nodedrain.PhaseCompleted {
		serviceError := newServiceError(ServiceErrorDrainNotCancellable, false)
		serviceError.NodeState = "SAFE_TO_STOP"
		return NodeDrainOperationDTO{}, serviceError
	}
	if state.Phase == nodedrain.PhaseCancelled {
		return NodeDrainOperationDTO{State: state, Replayed: true}, nil
	}
	proposedAt := service.clock.Now().UnixMilli()
	if proposedAt <= 0 {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	commandOperationID, err := newOperationID()
	if err != nil {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	result, err := service.port.Commit(ctx, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: commandOperationID,
		NodeDrain: &NodeDrainChangeDTO{
			Action: NodeDrainActionCancel, MemberID: memberID, DrainOperationID: state.DrainOperationID,
			ProposedAtUnixMillis: uint64(proposedAt),
		},
	})
	if err != nil {
		return NodeDrainOperationDTO{}, portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		switch result.Conflict {
		case ConflictDrainNotCancellable:
			serviceError := newServiceError(ServiceErrorDrainNotCancellable, false)
			serviceError.NodeState = "SAFE_TO_STOP"
			return NodeDrainOperationDTO{}, serviceError
		case ConflictPhase:
			return NodeDrainOperationDTO{}, newServiceError(ServiceErrorOperationConflict, false)
		default:
			return NodeDrainOperationDTO{}, conflictServiceError(result.Conflict)
		}
	}
	if result.Outcome != ApplyOutcomeApplied || result.NodeDrain == nil {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return NodeDrainOperationDTO{State: *result.NodeDrain}, nil
}

func (service *DrainService) replayOrConflict(state NodeDrainStateDTO, request StartNodeDrainRequestDTO) (NodeDrainOperationDTO, error) {
	if state.IdempotencyKey == request.IdempotencyKey && !sameDrainParameters(state, request) {
		return NodeDrainOperationDTO{}, newServiceError(ServiceErrorIdempotencyConflict, false)
	}
	return NodeDrainOperationDTO{State: state, Replayed: true}, nil
}

func sameDrainParameters(state NodeDrainStateDTO, request StartNodeDrainRequestDTO) bool {
	return state.ServerReference == request.ServerReference &&
		state.ConnectionRatePerSecond == request.ConnectionRatePerSecond &&
		state.LoadBalancerPropagationDelayMillis == request.LoadBalancerPropagationDelayMillis &&
		state.TimeoutMillis == request.TimeoutMillis
}
