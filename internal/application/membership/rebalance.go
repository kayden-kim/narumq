package membership

import (
	"slices"
	"sort"

	"narumq/internal/domain/connrebalance"
)

func (machine *StateMachine) applyConnectionRebalance(index uint64, change ConnectionRebalanceChangeDTO) ApplyResultDTO {
	if !slices.Contains(machine.activeFeatures, ConnectionRebalanceFeature) {
		return conflictResult(index, ConflictFeatureInactive)
	}
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	switch change.Action {
	case ConnectionRebalanceActionStart:
		if machine.connectionRebalance != nil && machine.connectionRebalance.Phase.Active() {
			return conflictResult(index, ConflictConnectionRebalanceInProgress)
		}
		for _, drain := range machine.nodeDrains {
			if drain.Phase.Active() {
				return conflictResult(index, ConflictNodeDrainInProgress)
			}
		}
		if machine.hasInProgressOperation() {
			return conflictResult(index, ConflictMembershipInProgress)
		}
		participants := make([]ConnectionRebalanceReportDTO, 0, len(machine.members))
		for nodeID, member := range machine.members {
			if member.Phase == MemberPhaseVoter {
				participants = append(participants, ConnectionRebalanceReportDTO{NodeID: nodeID})
			}
		}
		sort.Slice(participants, func(left, right int) bool { return participants[left].NodeID < participants[right].NodeID })
		if len(participants) < 2 {
			return conflictResult(index, ConflictPhase)
		}
		state := ConnectionRebalanceStateDTO{
			RebalanceOperationID: change.RebalanceOperationID, IdempotencyKey: change.IdempotencyKey,
			Phase: connrebalance.PhaseRunning, ServerReference: change.ServerReference,
			MaxDeviationConnections:            change.MaxDeviationConnections,
			ConnectionRatePerSecond:            change.ConnectionRatePerSecond,
			LoadBalancerPropagationDelayMillis: change.LoadBalancerPropagationDelayMillis,
			TimeoutMillis:                      change.TimeoutMillis, StartedAtUnixMillis: change.ProposedAtUnixMillis,
			DeadlineAtUnixMillis: change.ProposedAtUnixMillis + change.TimeoutMillis,
			Participants:         participants, CommittedIndex: index,
		}
		machine.connectionRebalance = &state
		result.ConnectionRebalance = new(cloneConnectionRebalanceState(state))
		return result
	}

	current := machine.connectionRebalance
	if current == nil || current.RebalanceOperationID != change.RebalanceOperationID {
		return conflictResult(index, ConflictPhase)
	}
	if !current.Phase.Active() {
		if change.Action == ConnectionRebalanceActionCancel {
			if current.Phase == connrebalance.PhaseCancelled {
				current.CommittedIndex = index
				result.ConnectionRebalance = new(cloneConnectionRebalanceState(*current))
				return result
			}
			if current.Phase == connrebalance.PhaseCompleted || current.Phase == connrebalance.PhaseFailed {
				return conflictResult(index, ConflictRebalanceNotCancellable)
			}
		}
		return conflictResult(index, ConflictPhase)
	}
	switch change.Action {
	case ConnectionRebalanceActionReport:
		participantIndex := sort.Search(len(current.Participants), func(i int) bool { return current.Participants[i].NodeID >= change.NodeID })
		if participantIndex == len(current.Participants) || current.Participants[participantIndex].NodeID != change.NodeID {
			return conflictResult(index, ConflictPhase)
		}
		if change.ProposedAtUnixMillis < current.Participants[participantIndex].ReportedAtUnixMillis {
			return conflictResult(index, ConflictPhase)
		}
		current.Participants[participantIndex] = ConnectionRebalanceReportDTO{
			NodeID: change.NodeID, Connections: change.Connections, SessionsTransferred: change.SessionsTransferred,
			Shedding: change.Shedding, ReportedAtUnixMillis: change.ProposedAtUnixMillis,
		}
		reports := make([]connrebalance.Report, len(current.Participants))
		for i, participant := range current.Participants {
			reports[i] = connrebalance.Report{NodeID: participant.NodeID, Connections: participant.Connections, Shedding: participant.Shedding, ReportedAtUnixMillis: participant.ReportedAtUnixMillis}
		}
		if connrebalance.Evaluate(reports, current.MaxDeviationConnections, change.ProposedAtUnixMillis).Converged {
			current.Phase = connrebalance.PhaseCompleted
			current.FinishedAtUnixMillis = change.ProposedAtUnixMillis
		}
	case ConnectionRebalanceActionFail:
		current.Phase = connrebalance.PhaseFailed
		current.FailureCode = change.FailureCode
		current.FinishedAtUnixMillis = change.ProposedAtUnixMillis
	case ConnectionRebalanceActionCancel:
		current.Phase = connrebalance.PhaseCancelled
		current.FinishedAtUnixMillis = change.ProposedAtUnixMillis
	default:
		return conflictResult(index, ConflictPhase)
	}
	current.CommittedIndex = index
	machine.connectionRebalance = current
	result.ConnectionRebalance = new(cloneConnectionRebalanceState(*current))
	return result
}

func cloneConnectionRebalanceStatePointer(state *ConnectionRebalanceStateDTO) *ConnectionRebalanceStateDTO {
	if state == nil {
		return nil
	}
	cloned := cloneConnectionRebalanceState(*state)
	return &cloned
}

func cloneConnectionRebalanceState(state ConnectionRebalanceStateDTO) ConnectionRebalanceStateDTO {
	state.Participants = slices.Clone(state.Participants)
	return state
}

// ConnectionRebalance returns the committed rebalance state and an owned participant slice.
func (machine *StateMachine) ConnectionRebalance() (ConnectionRebalanceStateDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	if machine.connectionRebalance == nil {
		return ConnectionRebalanceStateDTO{}, false
	}
	return cloneConnectionRebalanceState(*machine.connectionRebalance), true
}
