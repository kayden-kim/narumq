package membership

import (
	"sort"

	"narumq/internal/domain/nodedrain"
)

func (machine *StateMachine) applyNodeDrain(index uint64, change NodeDrainChangeDTO) ApplyResultDTO {
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	current, exists := machine.nodeDrains[change.MemberID]
	if change.Action == NodeDrainActionStart {
		member, memberExists := machine.members[change.MemberID]
		if !memberExists {
			return conflictResult(index, ConflictMemberNotFound)
		}
		if member.Phase != MemberPhaseVoter {
			return conflictResult(index, ConflictPhase)
		}
		if machine.connectionRebalance != nil && machine.connectionRebalance.Phase.Active() {
			return conflictResult(index, ConflictConnectionRebalanceInProgress)
		}
		if exists && current.Phase.Active() {
			return conflictResult(index, ConflictNodeDrainActive)
		}
		for memberID, drain := range machine.nodeDrains {
			if memberID != change.MemberID && drain.Phase.Active() {
				return conflictResult(index, ConflictNodeDrainInProgress)
			}
		}
		if machine.hasInProgressOperation() {
			return conflictResult(index, ConflictMembershipInProgress)
		}
		state := NodeDrainStateDTO{
			DrainOperationID:                   change.DrainOperationID,
			MemberID:                           change.MemberID,
			IdempotencyKey:                     change.IdempotencyKey,
			Phase:                              nodedrain.PhaseExcludingTraffic,
			ServerReference:                    change.ServerReference,
			ConnectionRatePerSecond:            change.ConnectionRatePerSecond,
			LoadBalancerPropagationDelayMillis: change.LoadBalancerPropagationDelayMillis,
			TimeoutMillis:                      change.TimeoutMillis,
			StartedAtUnixMillis:                change.ProposedAtUnixMillis,
			DeadlineAtUnixMillis:               change.ProposedAtUnixMillis + change.TimeoutMillis,
			PhaseStartedAtUnixMillis:           change.ProposedAtUnixMillis,
			CommittedIndex:                     index,
		}
		machine.nodeDrains[change.MemberID] = state
		result.NodeDrain = new(state)
		return result
	}

	if !exists || current.DrainOperationID != change.DrainOperationID {
		return conflictResult(index, ConflictPhase)
	}
	switch change.Action {
	case NodeDrainActionAdvance:
		if !nodedrain.CanAdvance(current.Phase, change.Phase) {
			return conflictResult(index, ConflictPhase)
		}
		if current.Phase != change.Phase {
			current.PhaseStartedAtUnixMillis = change.ProposedAtUnixMillis
		}
		current.Phase = change.Phase
		current.Progress = change.Progress
	case NodeDrainActionComplete:
		if !nodedrain.CanComplete(current.Phase) {
			return conflictResult(index, ConflictPhase)
		}
		current.Phase = nodedrain.PhaseCompleted
		current.PhaseStartedAtUnixMillis = change.ProposedAtUnixMillis
		current.Progress = change.Progress
	case NodeDrainActionFail:
		if !nodedrain.CanFail(current.Phase) {
			return conflictResult(index, ConflictPhase)
		}
		current.Phase = nodedrain.PhaseFailed
		current.PhaseStartedAtUnixMillis = change.ProposedAtUnixMillis
		current.Progress = change.Progress
		current.FailureCode = change.FailureCode
		current.BlockingReason = change.BlockingReason
	case NodeDrainActionCancel:
		if current.Phase == nodedrain.PhaseCompleted {
			return conflictResult(index, ConflictDrainNotCancellable)
		}
		if current.Phase == nodedrain.PhaseCancelled {
			result.NodeDrain = new(current)
			return result
		}
		if !nodedrain.CanCancel(current.Phase) {
			return conflictResult(index, ConflictPhase)
		}
		current.Phase = nodedrain.PhaseCancelled
		current.PhaseStartedAtUnixMillis = change.ProposedAtUnixMillis
	case NodeDrainActionClear:
		if !nodedrain.CanClear(current.Phase) {
			return conflictResult(index, ConflictPhase)
		}
		delete(machine.nodeDrains, change.MemberID)
		result.NodeDrain = new(current)
		return result
	default:
		return conflictResult(index, ConflictPhase)
	}
	current.CommittedIndex = index
	machine.nodeDrains[change.MemberID] = current
	result.NodeDrain = new(current)
	return result
}

// GetNodeDrain returns the committed drain state of one node.
func (machine *StateMachine) GetNodeDrain(memberID uint64) (NodeDrainStateDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	state, exists := machine.nodeDrains[memberID]
	return state, exists
}

// NodeDrains returns a member-ID-sorted copy of all committed drain records.
func (machine *StateMachine) NodeDrains() []NodeDrainStateDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	states := make([]NodeDrainStateDTO, 0, len(machine.nodeDrains))
	for _, state := range machine.nodeDrains {
		states = append(states, state)
	}
	sort.Slice(states, func(left, right int) bool { return states[left].MemberID < states[right].MemberID })
	return states
}
