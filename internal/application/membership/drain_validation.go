package membership

import (
	"math"

	"narumq/internal/domain/nodedrain"
)

func validateNodeDrainChange(change NodeDrainChangeDTO) error {
	if change.MemberID == 0 || !validUUID(change.DrainOperationID) || change.ProposedAtUnixMillis == 0 {
		return ErrInvalidCommand
	}
	parametersZero := change.IdempotencyKey == "" && change.ServerReference == "" &&
		change.ConnectionRatePerSecond == 0 && change.LoadBalancerPropagationDelayMillis == 0 && change.TimeoutMillis == 0
	progressZero := change.Progress == (NodeDrainProgressDTO{})
	failureZero := change.FailureCode == "" && change.BlockingReason == ""
	switch change.Action {
	case NodeDrainActionStart:
		if !validIdempotencyKey(change.IdempotencyKey) ||
			nodedrain.ValidateParameters(change.ServerReference, change.ConnectionRatePerSecond,
				change.LoadBalancerPropagationDelayMillis, change.TimeoutMillis) != nil ||
			change.ProposedAtUnixMillis > math.MaxUint64-change.TimeoutMillis ||
			change.Phase != nodedrain.PhaseUnspecified || !progressZero || !failureZero {
			return ErrInvalidCommand
		}
	case NodeDrainActionAdvance:
		if !parametersZero || change.Phase < nodedrain.PhaseExcludingTraffic ||
			change.Phase > nodedrain.PhaseVerifyingBarrier || !failureZero {
			return ErrInvalidCommand
		}
	case NodeDrainActionComplete:
		if !parametersZero || change.Phase != nodedrain.PhaseUnspecified || !failureZero {
			return ErrInvalidCommand
		}
	case NodeDrainActionFail:
		if !parametersZero || change.Phase != nodedrain.PhaseUnspecified ||
			!validDrainFailureToken(change.FailureCode, false) || !validDrainFailureToken(change.BlockingReason, true) {
			return ErrInvalidCommand
		}
	case NodeDrainActionCancel, NodeDrainActionClear:
		if !parametersZero || change.Phase != nodedrain.PhaseUnspecified || !progressZero || !failureZero {
			return ErrInvalidCommand
		}
	default:
		return ErrInvalidCommand
	}
	return nil
}

func validDrainFailureToken(value string, allowEmpty bool) bool {
	if value == "" {
		return allowEmpty
	}
	if len(value) > 64 {
		return false
	}
	for index := range len(value) {
		if value[index] != '_' && (value[index] < 'a' || value[index] > 'z') {
			return false
		}
	}
	return true
}

func validateNodeDrainState(state NodeDrainStateDTO) error {
	if state.MemberID == 0 || !validUUID(state.DrainOperationID) || !validIdempotencyKey(state.IdempotencyKey) ||
		!state.Phase.Valid() || state.StartedAtUnixMillis == 0 || state.DeadlineAtUnixMillis <= state.StartedAtUnixMillis ||
		state.DeadlineAtUnixMillis-state.StartedAtUnixMillis != state.TimeoutMillis ||
		state.PhaseStartedAtUnixMillis < state.StartedAtUnixMillis || state.CommittedIndex == 0 ||
		nodedrain.ValidateParameters(state.ServerReference, state.ConnectionRatePerSecond,
			state.LoadBalancerPropagationDelayMillis, state.TimeoutMillis) != nil {
		return ErrInvalidSnapshot
	}
	if state.Phase == nodedrain.PhaseFailed {
		if !validDrainFailureToken(state.FailureCode, false) || !validDrainFailureToken(state.BlockingReason, true) {
			return ErrInvalidSnapshot
		}
	} else if state.FailureCode != "" || state.BlockingReason != "" {
		return ErrInvalidSnapshot
	}
	return nil
}
