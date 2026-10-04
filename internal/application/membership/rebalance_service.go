package membership

import (
	"context"
	"errors"
	"slices"

	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/domain/connrebalance"
)

// ConnectionRebalancePort commits transitions and reads their quorum-visible state.
type ConnectionRebalancePort interface {
	Commit(ctx context.Context, command CommandDTO) (ApplyResultDTO, error)
	ActiveFeatures(ctx context.Context) ([]string, error)
	GetConnectionRebalance(ctx context.Context) (ConnectionRebalanceStateDTO, bool, error)
}

// RebalanceService manages one committed cluster-wide connection rebalance.
type RebalanceService struct {
	port  ConnectionRebalancePort
	clock applicationclock.Clock
}

func NewRebalanceService(port ConnectionRebalancePort, clock applicationclock.Clock) (*RebalanceService, error) {
	if port == nil {
		return nil, errors.New("connection rebalance port is required")
	}
	if clock == nil {
		return nil, errors.New("connection rebalance clock is required")
	}
	return &RebalanceService{port: port, clock: clock}, nil
}

// StartConnectionRebalance validates, commits, or replays a manual operation.
func (service *RebalanceService) StartConnectionRebalance(ctx context.Context, request StartConnectionRebalanceRequestDTO) (ConnectionRebalanceOperationDTO, error) {
	if ctx == nil || !validIdempotencyKey(request.IdempotencyKey) || connrebalance.ValidateParameters(request.ServerReference, request.MaxDeviationConnections, request.ConnectionRatePerSecond, request.LoadBalancerPropagationDelayMillis, request.TimeoutMillis) != nil {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	features, err := service.port.ActiveFeatures(ctx)
	if err != nil {
		return ConnectionRebalanceOperationDTO{}, portFailure(err)
	}
	if !slices.Contains(features, ConnectionRebalanceFeature) {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorFeatureInactive, false)
	}
	current, exists, err := service.port.GetConnectionRebalance(ctx)
	if err != nil {
		return ConnectionRebalanceOperationDTO{}, portFailure(err)
	}
	if exists && (current.IdempotencyKey == request.IdempotencyKey || current.Phase.Active()) {
		return service.rebalanceReplayOrConflict(current, request)
	}
	proposedAt := service.clock.Now().UnixMilli()
	if proposedAt <= 0 {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	rebalanceID, err := newOperationID()
	if err != nil {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	commandID, err := newOperationID()
	if err != nil {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	result, err := service.port.Commit(ctx, CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: commandID, ConnectionRebalance: &ConnectionRebalanceChangeDTO{
		Action: ConnectionRebalanceActionStart, RebalanceOperationID: rebalanceID, IdempotencyKey: request.IdempotencyKey, ServerReference: request.ServerReference,
		MaxDeviationConnections: request.MaxDeviationConnections, ConnectionRatePerSecond: request.ConnectionRatePerSecond, LoadBalancerPropagationDelayMillis: request.LoadBalancerPropagationDelayMillis, TimeoutMillis: request.TimeoutMillis, ProposedAtUnixMillis: uint64(proposedAt),
	}})
	if err != nil {
		return ConnectionRebalanceOperationDTO{}, portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		switch result.Conflict {
		case ConflictConnectionRebalanceInProgress:
			latest, found, err := service.port.GetConnectionRebalance(ctx)
			if err != nil {
				return ConnectionRebalanceOperationDTO{}, portFailure(err)
			}
			if found && (latest.IdempotencyKey == request.IdempotencyKey || latest.Phase.Active()) {
				return service.rebalanceReplayOrConflict(latest, request)
			}
			return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorOperationConflict, false)
		case ConflictNodeDrainInProgress:
			return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorNodeDrainInProgress, true)
		case ConflictMembershipInProgress:
			return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorMembershipInProgress, true)
		case ConflictFeatureInactive:
			return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorFeatureInactive, false)
		case ConflictPhase:
			return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorOperationConflict, false)
		default:
			return ConnectionRebalanceOperationDTO{}, conflictServiceError(result.Conflict)
		}
	}
	if result.Outcome != ApplyOutcomeApplied || result.ConnectionRebalance == nil {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return ConnectionRebalanceOperationDTO{State: *result.ConnectionRebalance}, nil
}

func (service *RebalanceService) GetConnectionRebalance(ctx context.Context) (ConnectionRebalanceOperationDTO, error) {
	if ctx == nil {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	state, exists, err := service.port.GetConnectionRebalance(ctx)
	if err != nil {
		return ConnectionRebalanceOperationDTO{}, portFailure(err)
	}
	if !exists {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorResourceNotFound, false)
	}
	return ConnectionRebalanceOperationDTO{State: state}, nil
}

// CancelConnectionRebalance does not revert ownership transfers that already committed.
func (service *RebalanceService) CancelConnectionRebalance(ctx context.Context) (ConnectionRebalanceOperationDTO, error) {
	current, err := service.GetConnectionRebalance(ctx)
	if err != nil {
		return ConnectionRebalanceOperationDTO{}, err
	}
	state := current.State
	if state.Phase == connrebalance.PhaseCompleted || state.Phase == connrebalance.PhaseFailed {
		return ConnectionRebalanceOperationDTO{}, rebalanceNotCancellable(state.Phase)
	}
	if state.Phase == connrebalance.PhaseCancelled {
		return ConnectionRebalanceOperationDTO{State: state, Replayed: true}, nil
	}
	proposedAt := service.clock.Now().UnixMilli()
	if proposedAt <= 0 {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	commandID, err := newOperationID()
	if err != nil {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	result, err := service.port.Commit(ctx, CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: commandID, ConnectionRebalance: &ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionCancel, RebalanceOperationID: state.RebalanceOperationID, ProposedAtUnixMillis: uint64(proposedAt)}})
	if err != nil {
		return ConnectionRebalanceOperationDTO{}, portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		switch result.Conflict {
		case ConflictRebalanceNotCancellable:
			latest, found, readErr := service.port.GetConnectionRebalance(ctx)
			if readErr != nil {
				return ConnectionRebalanceOperationDTO{}, portFailure(readErr)
			}
			if found && latest.RebalanceOperationID == state.RebalanceOperationID {
				return ConnectionRebalanceOperationDTO{}, rebalanceNotCancellable(latest.Phase)
			}
			return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorOperationConflict, false)
		case ConflictPhase:
			return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorOperationConflict, false)
		default:
			return ConnectionRebalanceOperationDTO{}, conflictServiceError(result.Conflict)
		}
	}
	if result.Outcome != ApplyOutcomeApplied || result.ConnectionRebalance == nil {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return ConnectionRebalanceOperationDTO{State: *result.ConnectionRebalance}, nil
}

func rebalanceNotCancellable(phase connrebalance.Phase) *ServiceError {
	err := newServiceError(ServiceErrorRebalanceNotCancellable, false)
	err.NodeState = phase.String()
	return err
}

func (service *RebalanceService) rebalanceReplayOrConflict(state ConnectionRebalanceStateDTO, request StartConnectionRebalanceRequestDTO) (ConnectionRebalanceOperationDTO, error) {
	if state.IdempotencyKey != request.IdempotencyKey {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorConnectionRebalanceInProgress, true)
	}
	if state.ServerReference != request.ServerReference || state.MaxDeviationConnections != request.MaxDeviationConnections || state.ConnectionRatePerSecond != request.ConnectionRatePerSecond || state.LoadBalancerPropagationDelayMillis != request.LoadBalancerPropagationDelayMillis || state.TimeoutMillis != request.TimeoutMillis {
		return ConnectionRebalanceOperationDTO{}, newServiceError(ServiceErrorIdempotencyConflict, false)
	}
	return ConnectionRebalanceOperationDTO{State: state, Replayed: true}, nil
}
