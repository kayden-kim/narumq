package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/application/membership"
	managementports "narumq/internal/application/ports/management"
	"narumq/internal/domain/nodedrain"
)

const (
	drainRetryInterval    = 500 * time.Millisecond
	drainProgressInterval = time.Second
	drainClearInterval    = time.Second
)

type clusterDrainController struct {
	runtime               *clusterRuntime
	mu                    sync.Mutex
	admissionClosed       atomic.Bool
	serverReference       string
	lastPhase             nodedrain.Phase
	hasRecord             bool
	completingOperationID string
	runningOperationID    string
	clearing              bool
	ctx                   context.Context
	wg                    sync.WaitGroup
	lastBlock             nodedrain.Blocker
}

func (controller *clusterDrainController) start(ctx context.Context) {
	if controller == nil || ctx == nil {
		return
	}
	controller.mu.Lock()
	controller.ctx = ctx
	controller.mu.Unlock()
	controller.sync()
}

func (controller *clusterDrainController) stopAndWait() {
	if controller == nil {
		return
	}
	controller.mu.Lock()
	controller.mu.Unlock()
	controller.wg.Wait()
}

func (controller *clusterDrainController) sync() {
	if controller == nil || controller.runtime == nil || controller.runtime.machine == nil {
		return
	}
	state, exists := controller.runtime.machine.GetNodeDrain(controller.runtime.config.NodeID)
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if !exists || state.Phase == nodedrain.PhaseCancelled {
		wasDraining := controller.hasRecord && controller.lastPhase.Active()
		controller.admissionClosed.Store(false)
		controller.serverReference = ""
		controller.lastPhase = nodedrain.PhaseUnspecified
		controller.hasRecord = false
		controller.runningOperationID = ""
		controller.completingOperationID = ""
		controller.clearing = false
		controller.lastBlock = ""
		if wasDraining && controller.runtime.managementState != nil {
			reason := "node drain cancelled; readiness recheck pending"
			controller.runtime.managementState.SetNodeState(managementports.NodeStateJoining, &reason)
		}
		return
	}

	controller.serverReference = state.ServerReference
	controller.lastPhase = state.Phase
	controller.hasRecord = true
	switch state.Phase {
	case nodedrain.PhaseCompleted:
		controller.admissionClosed.Store(true)
		if controller.completingOperationID == state.DrainOperationID {
			if controller.runtime.managementState != nil {
				controller.runtime.managementState.SetNodeState(managementports.NodeStateSafeToStop, nil)
			}
			return
		}
		if controller.runtime.managementState != nil {
			reason := "previous drain completed; clearing drain record"
			controller.runtime.managementState.SetNodeState(managementports.NodeStateJoining, &reason)
		}
		controller.startClearLocked(state.DrainOperationID)
	case nodedrain.PhaseFailed:
		controller.admissionClosed.Store(true)
		if controller.runtime.managementState != nil {
			reason := "node drain failed: " + state.FailureCode
			controller.runtime.managementState.SetNodeState(managementports.NodeStateDraining, &reason)
		}
	default:
		controller.admissionClosed.Store(true)
		if controller.runtime.managementState != nil {
			reason := "node is draining"
			controller.runtime.managementState.SetNodeState(managementports.NodeStateDraining, &reason)
		}
		if controller.ctx != nil && controller.ctx.Err() == nil && controller.runningOperationID != state.DrainOperationID {
			controller.runningOperationID = state.DrainOperationID
			controller.wg.Add(1)
			go func(ctx context.Context, operationID string) {
				defer controller.wg.Done()
				controller.run(ctx, operationID)
			}(controller.ctx, state.DrainOperationID)
		}
	}
}

func (controller *clusterDrainController) startClearLocked(operationID string) {
	if controller.clearing || controller.ctx == nil || controller.ctx.Err() != nil {
		return
	}
	controller.clearing = true
	controller.runtime.logger.Info("resuming completed node drain record clearance",
		"drain_operation_id", operationID, "member_id", controller.runtime.config.NodeID)
	controller.wg.Add(1)
	go func(ctx context.Context, operationID string) {
		defer controller.wg.Done()
		defer func() {
			controller.mu.Lock()
			controller.clearing = false
			controller.mu.Unlock()
		}()
		controller.clearCompleted(ctx, operationID)
	}(controller.ctx, operationID)
}

func (controller *clusterDrainController) admissionOpen() bool {
	return controller == nil || !controller.admissionClosed.Load()
}

func (controller *clusterDrainController) currentServerReference() string {
	if controller == nil {
		return ""
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.serverReference
}

func (controller *clusterDrainController) setLastBlock(blocker nodedrain.Blocker) {
	controller.mu.Lock()
	controller.lastBlock = blocker
	controller.mu.Unlock()
}

func (controller *clusterDrainController) lastBlockingReason() string {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return string(controller.lastBlock)
}

func (controller *clusterDrainController) run(ctx context.Context, operationID string) {
	defer func() {
		controller.mu.Lock()
		if controller.runningOperationID == operationID {
			controller.runningOperationID = ""
		}
		controller.mu.Unlock()
	}()

	progress := membership.NodeDrainProgressDTO{}
	progressOperationID := ""
	lastProgressCommitAt := time.Now()
	nextTransferAt := time.Time{}
	for ctx.Err() == nil {
		state, exists := controller.runtime.machine.GetNodeDrain(controller.runtime.config.NodeID)
		if !exists || state.DrainOperationID != operationID || state.Phase.Terminal() {
			return
		}
		if progressOperationID != operationID {
			progress = state.Progress
			progressOperationID = operationID
		} else {
			progress = mergeDrainProgress(progress, state.Progress)
		}
		now := time.Now()
		if now.UnixMilli() > 0 && uint64(now.UnixMilli()) >= state.DeadlineAtUnixMillis {
			result, err := controller.commitDrain(ctx, state, membership.NodeDrainActionFail, nodedrain.PhaseUnspecified,
				progress, "drain_timeout", controller.lastBlockingReason())
			if err != nil {
				controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
				if !waitDrain(ctx, drainRetryInterval) {
					return
				}
				continue
			}
			if result.Outcome == membership.ApplyOutcomeConflict && result.Conflict == membership.ConflictPhase {
				continue
			}
			if result.Outcome == membership.ApplyOutcomeApplied {
				return
			}
			controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
			if !waitDrain(ctx, drainRetryInterval) {
				return
			}
			continue
		}

		switch state.Phase {
		case nodedrain.PhaseExcludingTraffic:
			connections := controller.activeConnections()
			progress.ConnectionsInitial = uint64(connections)
			progress.ConnectionsRemaining = uint64(connections)
			if controller.proposePhase(ctx, state, nodedrain.PhaseWaitingForLB, progress, &lastProgressCommitAt) {
				continue
			}
		case nodedrain.PhaseWaitingForLB:
			readyAt := state.DeadlineAtUnixMillis
			if state.PhaseStartedAtUnixMillis <= state.DeadlineAtUnixMillis {
				remainingToDeadline := state.DeadlineAtUnixMillis - state.PhaseStartedAtUnixMillis
				if state.LoadBalancerPropagationDelayMillis <= remainingToDeadline {
					readyAt = state.PhaseStartedAtUnixMillis + state.LoadBalancerPropagationDelayMillis
				}
			}
			if uint64(now.UnixMilli()) >= readyAt {
				if controller.proposePhase(ctx, state, nodedrain.PhaseVerifyingSessionState, progress, &lastProgressCommitAt) {
					continue
				}
			} else if !waitDrain(ctx, time.Duration(readyAt-uint64(now.UnixMilli()))*time.Millisecond) {
				return
			}
		case nodedrain.PhaseVerifyingSessionState:
			connected := controller.connectedOtherVoters()
			if len(connected) == 0 {
				controller.setLastBlock(nodedrain.BlockerNoSuccessorVoter)
				if !waitDrain(ctx, drainRetryInterval) {
					return
				}
				continue
			}
			if err := controller.runtime.readBarrier(ctx); err != nil {
				controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
				if !waitDrain(ctx, drainRetryInterval) {
					return
				}
				continue
			}
			if !controller.runtime.clusterProjectionReady() {
				controller.setLastBlock(nodedrain.BlockerRouteProjectionNotReady)
				if !waitDrain(ctx, drainRetryInterval) {
					return
				}
				continue
			}
			progress.SessionsReadyForTakeover = uint64(controller.ownedSessionCount())
			controller.setLastBlock("")
			if controller.proposePhase(ctx, state, nodedrain.PhaseTransferringSessions, progress, &lastProgressCommitAt) {
				continue
			}
		case nodedrain.PhaseTransferringSessions:
			connected := controller.connectedOtherVoters()
			voters := make([]uint64, 0, len(connected)+1)
			voters = append(voters, controller.runtime.config.NodeID)
			voters = append(voters, connected...)
			plan := membership.PlanRemovalHandoff(
				controller.runtime.config.NodeID, voters,
				controller.runtime.machine.Ownerships(), controller.runtime.machine.Subscriptions(),
			)
			if plan.Block != membership.RemovalHandoffBlockNone {
				switch plan.Block {
				case membership.RemovalHandoffBlockEpochExhausted:
					controller.setLastBlock(nodedrain.Blocker(plan.Block))
					result, err := controller.commitDrain(ctx, state, membership.NodeDrainActionFail, nodedrain.PhaseUnspecified,
						progress, "ownership_epoch_exhausted", string(plan.Block))
					if err == nil && result.Outcome == membership.ApplyOutcomeApplied {
						return
					}
					if err != nil {
						controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
					}
				default:
					controller.setLastBlock(nodedrain.Blocker(plan.Block))
				}
				if !waitDrain(ctx, drainRetryInterval) {
					return
				}
				continue
			}
			if len(plan.Transfers) != 0 {
				now = time.Now()
				if !nextTransferAt.IsZero() && now.Before(nextTransferAt) {
					if !waitDrain(ctx, time.Until(nextTransferAt)) {
						return
					}
					continue
				}
				transfer := plan.Transfers[0]
				opID, err := newClusterOperationID()
				if err != nil {
					controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
					if !waitDrain(ctx, drainRetryInterval) {
						return
					}
					continue
				}
				result, err := controller.runtime.Commit(ctx, membership.CommandDTO{
					SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
					OperationID: opID, Ownership: &transfer,
				})
				if err != nil {
					controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
					if !waitDrain(ctx, drainRetryInterval) {
						return
					}
					continue
				}
				if result.Outcome == membership.ApplyOutcomeApplied ||
					(result.Outcome == membership.ApplyOutcomeConflict && (result.Conflict == membership.ConflictStaleEpoch || result.Conflict == membership.ConflictOwnershipMissing)) {
					progress.SessionsTakenOver++
				} else {
					progress.SessionsFailed++
				}
				base := time.Now()
				if nextTransferAt.After(base) {
					base = nextTransferAt
				}
				nextTransferAt = base.Add(time.Second / time.Duration(state.ConnectionRatePerSecond))
				controller.setLastBlock("")
				if !controller.persistDrainProgress(ctx, state, progress, &lastProgressCommitAt) {
					continue
				}
				continue
			}
			connections := controller.activeConnections()
			progress.ConnectionsRemaining = uint64(connections)
			if plan.RemoveReady {
				if connections > 0 {
					controller.setLastBlock(nodedrain.BlockerConnectionsRemaining)
					if !controller.persistDrainProgress(ctx, state, progress, &lastProgressCommitAt) {
						continue
					}
					if !waitDrain(ctx, 200*time.Millisecond) {
						return
					}
					continue
				}
				controller.setLastBlock("")
				if controller.proposePhase(ctx, state, nodedrain.PhaseVerifyingBarrier, progress, &lastProgressCommitAt) {
					continue
				}
			}
			if !controller.persistDrainProgress(ctx, state, progress, &lastProgressCommitAt) {
				continue
			}
			if !waitDrain(ctx, drainRetryInterval) {
				return
			}
		case nodedrain.PhaseVerifyingBarrier:
			input := controller.barrierInput()
			progress.PendingWAL = input.PendingWAL
			progress.UnderReplicated = nodedrain.UnderReplicated(input.VoterCount, input.ConnectedOtherVoters)
			blocker, ready := nodedrain.EvaluateBarrier(input)
			if !ready {
				controller.setLastBlock(blocker)
				if !controller.persistDrainProgress(ctx, state, progress, &lastProgressCommitAt) {
					continue
				}
				if !waitDrain(ctx, drainRetryInterval) {
					return
				}
				continue
			}
			controller.setLastBlock("")
			progress.ConnectionsRemaining = 0
			controller.mu.Lock()
			controller.completingOperationID = operationID
			controller.mu.Unlock()
			result, err := controller.commitDrain(ctx, state, membership.NodeDrainActionComplete, nodedrain.PhaseUnspecified, progress, "", "")
			if err != nil {
				controller.mu.Lock()
				if controller.completingOperationID == operationID {
					controller.completingOperationID = ""
				}
				controller.mu.Unlock()
				controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
				if !waitDrain(ctx, drainRetryInterval) {
					return
				}
				continue
			}
			if result.Outcome == membership.ApplyOutcomeApplied {
				return
			}
			controller.mu.Lock()
			if controller.completingOperationID == operationID {
				controller.completingOperationID = ""
			}
			controller.mu.Unlock()
			if result.Outcome == membership.ApplyOutcomeConflict && result.Conflict == membership.ConflictPhase {
				continue
			}
			controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
			if !waitDrain(ctx, drainRetryInterval) {
				return
			}
		}
	}
}

func (controller *clusterDrainController) proposePhase(
	ctx context.Context,
	state membership.NodeDrainStateDTO,
	phase nodedrain.Phase,
	progress membership.NodeDrainProgressDTO,
	lastProgressCommitAt *time.Time,
) bool {
	result, err := controller.commitDrain(ctx, state, membership.NodeDrainActionAdvance, phase, progress, "", "")
	if err != nil {
		controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
		waitDrain(ctx, drainRetryInterval)
		return false
	}
	if result.Outcome == membership.ApplyOutcomeApplied ||
		(result.Outcome == membership.ApplyOutcomeConflict && result.Conflict == membership.ConflictPhase) {
		*lastProgressCommitAt = time.Now()
		return true
	}
	controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
	waitDrain(ctx, drainRetryInterval)
	return false
}

func (controller *clusterDrainController) persistDrainProgress(
	ctx context.Context,
	state membership.NodeDrainStateDTO,
	progress membership.NodeDrainProgressDTO,
	lastProgressCommitAt *time.Time,
) bool {
	if time.Since(*lastProgressCommitAt) < drainProgressInterval {
		return true
	}
	result, err := controller.commitDrain(ctx, state, membership.NodeDrainActionAdvance, state.Phase, progress, "", "")
	if err != nil {
		controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
		waitDrain(ctx, drainRetryInterval)
		return false
	}
	if result.Outcome == membership.ApplyOutcomeApplied ||
		(result.Outcome == membership.ApplyOutcomeConflict && result.Conflict == membership.ConflictPhase) {
		*lastProgressCommitAt = time.Now()
		return true
	}
	controller.setLastBlock(nodedrain.BlockerQuorumUnavailable)
	waitDrain(ctx, drainRetryInterval)
	return false
}

func (controller *clusterDrainController) commitDrain(
	ctx context.Context,
	state membership.NodeDrainStateDTO,
	action membership.NodeDrainAction,
	phase nodedrain.Phase,
	progress membership.NodeDrainProgressDTO,
	failureCode string,
	blockingReason string,
) (membership.ApplyResultDTO, error) {
	opID, err := newClusterOperationID()
	if err != nil {
		return membership.ApplyResultDTO{}, err
	}
	proposedAt := time.Now().UnixMilli()
	if proposedAt <= 0 {
		return membership.ApplyResultDTO{}, errors.New("drain proposal time is invalid")
	}
	return controller.runtime.Commit(ctx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: opID,
		NodeDrain: &membership.NodeDrainChangeDTO{
			Action: action, MemberID: state.MemberID, DrainOperationID: state.DrainOperationID,
			Phase: phase, Progress: progress, FailureCode: failureCode, BlockingReason: blockingReason,
			ProposedAtUnixMillis: uint64(proposedAt),
		},
	})
}

func (controller *clusterDrainController) clearCompleted(ctx context.Context, operationID string) {
	var lastWarningAt time.Time
	warnWait := func(reason string, err error) {
		now := time.Now()
		if !lastWarningAt.IsZero() && now.Sub(lastWarningAt) < 10*time.Second {
			return
		}
		controller.runtime.logger.Warn("completed node drain record clearance is waiting",
			"drain_operation_id", operationID, "member_id", controller.runtime.config.NodeID,
			"reason", reason, "error", err)
		lastWarningAt = now
	}
	for ctx.Err() == nil {
		state, exists := controller.runtime.machine.GetNodeDrain(controller.runtime.config.NodeID)
		if !exists || state.DrainOperationID != operationID || state.Phase != nodedrain.PhaseCompleted {
			return
		}
		checkCtx, cancelCheck := context.WithTimeout(ctx, clusterReadTimeout)
		readinessErr := controller.runtime.checkReadiness(checkCtx, "drain")
		cancelCheck()
		if readinessErr != nil {
			warnWait("runtime_not_ready", readinessErr)
		} else {
			result, err := controller.commitDrain(ctx, state, membership.NodeDrainActionClear, nodedrain.PhaseUnspecified,
				membership.NodeDrainProgressDTO{}, "", "")
			if err != nil {
				warnWait("clear_commit_failed", err)
			} else if result.Outcome == membership.ApplyOutcomeApplied {
				controller.runtime.logger.Info("cleared completed node drain record",
					"drain_operation_id", operationID, "member_id", controller.runtime.config.NodeID,
					"committed_index", result.Index)
				return
			} else {
				warnWait("clear_commit_conflict", fmt.Errorf("outcome=%d conflict=%q", result.Outcome, result.Conflict))
			}
		}
		if !waitDrain(ctx, drainClearInterval) {
			return
		}
	}
}

func (controller *clusterDrainController) connectedOtherVoters() []uint64 {
	if controller.runtime.node == nil || controller.runtime.transport == nil {
		return nil
	}
	status := controller.runtime.node.Status()
	connected := make([]uint64, 0, len(status.VoterIDs))
	for _, voterID := range status.VoterIDs {
		if uint64(voterID) == controller.runtime.config.NodeID {
			continue
		}
		peerStatus, err := controller.runtime.transport.Status(uint64(voterID))
		if err == nil && peerStatus.Connected {
			connected = append(connected, uint64(voterID))
		}
	}
	sort.Slice(connected, func(left, right int) bool { return connected[left] < connected[right] })
	return connected
}

func (controller *clusterDrainController) activeConnections() int {
	if controller.runtime.mqttServer == nil {
		return 0
	}
	return controller.runtime.mqttServer.ActiveConnectionCount()
}

func (controller *clusterDrainController) ownedSessionCount() int {
	count := 0
	for _, ownership := range controller.runtime.machine.Ownerships() {
		if ownership.OwnerNodeID == controller.runtime.config.NodeID {
			count++
		}
	}
	return count
}

func (controller *clusterDrainController) barrierInput() nodedrain.BarrierInput {
	status := controller.runtime.node.Status()
	pendingWAL := uint64(math.MaxUint64)
	if status.CommitIndex >= status.AppliedIndex {
		pendingWAL = status.CommitIndex - status.AppliedIndex
	}
	ownedSessions, ownedSubscriptions := 0, 0
	for _, ownership := range controller.runtime.machine.Ownerships() {
		if ownership.OwnerNodeID == controller.runtime.config.NodeID {
			ownedSessions++
		}
	}
	for _, subscription := range controller.runtime.machine.Subscriptions() {
		if subscription.OwnerNodeID == controller.runtime.config.NodeID {
			ownedSubscriptions++
		}
	}
	readinessFailing := controller.runtime.managementState == nil ||
		controller.runtime.managementState.NodeState() != managementports.NodeStateReady
	pendingWills := -1
	if controller.runtime.connectUseCase != nil {
		pendingWills = controller.runtime.connectUseCase.PendingWillCount()
	}
	return nodedrain.BarrierInput{
		AdmissionClosed: controller.admissionClosed.Load(), ReadinessFailing: readinessFailing,
		ActiveConnections: controller.activeConnections(), OwnedSessions: ownedSessions,
		OwnedSubscriptions: ownedSubscriptions, PendingWAL: pendingWAL,
		VoterCount: len(status.VoterIDs), ConnectedOtherVoters: len(controller.connectedOtherVoters()),
		LocalLeader:         uint64(status.LeaderID) == controller.runtime.config.NodeID,
		PendingLocalCommits: int(controller.runtime.localCommits.Load()), PendingWills: pendingWills,
	}
}

func waitDrain(ctx context.Context, delay time.Duration) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

func mergeDrainProgress(current, committed membership.NodeDrainProgressDTO) membership.NodeDrainProgressDTO {
	current.ConnectionsInitial = committed.ConnectionsInitial
	current.ConnectionsRemaining = committed.ConnectionsRemaining
	current.SessionsReadyForTakeover = committed.SessionsReadyForTakeover
	current.PendingWAL = committed.PendingWAL
	current.UnderReplicated = committed.UnderReplicated
	if committed.SessionsTakenOver > current.SessionsTakenOver {
		current.SessionsTakenOver = committed.SessionsTakenOver
	}
	if committed.SessionsFailed > current.SessionsFailed {
		current.SessionsFailed = committed.SessionsFailed
	}
	return current
}

func (runtime *clusterRuntime) transferLocalLeadership(status raftadapter.StatusDTO) error {
	var transferee raftadapter.NodeID
	for _, voterID := range status.VoterIDs {
		if voterID == raftadapter.NodeID(runtime.config.NodeID) {
			continue
		}
		if peerStatus, err := runtime.transport.Status(uint64(voterID)); err == nil && peerStatus.Connected {
			transferee = voterID
			break
		}
	}
	if transferee == 0 {
		return nil
	}
	if err := runtime.node.TransferLeadership(transferee); err != nil {
		if errors.Is(err, raftadapter.ErrNotLeader) {
			return nil
		}
		return fmt.Errorf("transfer leadership before removing local voter: %w", err)
	}
	runtime.leadershipTransferTo = transferee
	runtime.leadershipTransferTerm = status.Term
	runtime.leadershipTransferTicks = 0
	return nil
}

func (runtime *clusterRuntime) advanceDrainLeadership() error {
	if runtime.leadershipTransferTo != 0 || runtime.node.Status().Role != raftadapter.RoleLeader {
		return nil
	}
	state, exists := runtime.machine.GetNodeDrain(runtime.config.NodeID)
	if !exists || (state.Phase != nodedrain.PhaseTransferringSessions && state.Phase != nodedrain.PhaseVerifyingBarrier) {
		return nil
	}
	return runtime.transferLocalLeadership(runtime.node.Status())
}
