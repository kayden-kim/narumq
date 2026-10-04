package membership

import (
	"math"

	"narumq/internal/domain/connrebalance"
)

func validateConnectionRebalanceChange(change ConnectionRebalanceChangeDTO) error {
	if !validUUID(change.RebalanceOperationID) || change.ProposedAtUnixMillis == 0 {
		return ErrInvalidCommand
	}
	parametersZero := change.IdempotencyKey == "" && change.ServerReference == "" &&
		change.MaxDeviationConnections == 0 && change.ConnectionRatePerSecond == 0 &&
		change.LoadBalancerPropagationDelayMillis == 0 && change.TimeoutMillis == 0
	reportZero := change.NodeID == 0 && change.Connections == 0 && change.SessionsTransferred == 0 && !change.Shedding
	switch change.Action {
	case ConnectionRebalanceActionStart:
		if !validIdempotencyKey(change.IdempotencyKey) ||
			connrebalance.ValidateParameters(change.ServerReference, change.MaxDeviationConnections,
				change.ConnectionRatePerSecond, change.LoadBalancerPropagationDelayMillis,
				change.TimeoutMillis) != nil ||
			change.ProposedAtUnixMillis > math.MaxUint64-change.TimeoutMillis ||
			change.FailureCode != "" || !reportZero {
			return ErrInvalidCommand
		}
	case ConnectionRebalanceActionReport:
		if change.NodeID == 0 || change.Connections > connrebalance.MaxReportedConnections ||
			!parametersZero || change.FailureCode != "" {
			return ErrInvalidCommand
		}
	case ConnectionRebalanceActionFail:
		if !parametersZero || !reportZero || !validDrainFailureToken(change.FailureCode, false) {
			return ErrInvalidCommand
		}
	case ConnectionRebalanceActionCancel:
		if !parametersZero || !reportZero || change.FailureCode != "" {
			return ErrInvalidCommand
		}
	default:
		return ErrInvalidCommand
	}
	return nil
}

func validateConnectionRebalanceState(state ConnectionRebalanceStateDTO) error {
	if !state.Phase.Valid() || !validUUID(state.RebalanceOperationID) || !validIdempotencyKey(state.IdempotencyKey) ||
		state.StartedAtUnixMillis == 0 || state.DeadlineAtUnixMillis <= state.StartedAtUnixMillis ||
		state.DeadlineAtUnixMillis-state.StartedAtUnixMillis != state.TimeoutMillis ||
		state.StartedAtUnixMillis > math.MaxUint64-state.TimeoutMillis ||
		connrebalance.ValidateParameters(state.ServerReference, state.MaxDeviationConnections,
			state.ConnectionRatePerSecond, state.LoadBalancerPropagationDelayMillis, state.TimeoutMillis) != nil ||
		state.CommittedIndex == 0 || len(state.Participants) < 2 {
		return ErrInvalidSnapshot
	}
	if state.FinishedAtUnixMillis != 0 && !state.Phase.Terminal() {
		return ErrInvalidSnapshot
	}
	if state.FailureCode != "" && (state.Phase != connrebalance.PhaseFailed || !validDrainFailureToken(state.FailureCode, false)) {
		return ErrInvalidSnapshot
	}
	for index, participant := range state.Participants {
		if participant.NodeID == 0 || participant.Connections > connrebalance.MaxReportedConnections ||
			(index > 0 && state.Participants[index-1].NodeID >= participant.NodeID) {
			return ErrInvalidSnapshot
		}
	}
	return nil
}
