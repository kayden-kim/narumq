package main

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/domain/connrebalance"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

var errClusterConnectionRebalanceInactive = errors.New("cluster connection rebalance feature is not active")

const connectionRebalanceTickInterval = 200 * time.Millisecond

type clusterRebalanceController struct {
	runtime         *clusterRuntime
	mu              sync.Mutex
	admissionClosed atomic.Bool
	serverReference string
	runningID       string
	ctx             context.Context
	wg              sync.WaitGroup
}

func (controller *clusterRebalanceController) start(ctx context.Context) {
	if controller == nil || ctx == nil {
		return
	}
	controller.mu.Lock()
	controller.ctx = ctx
	controller.mu.Unlock()
	controller.sync()
}

func (controller *clusterRebalanceController) stopAndWait() {
	if controller == nil {
		return
	}
	controller.wg.Wait()
}

func (controller *clusterRebalanceController) sync() {
	if controller == nil || controller.runtime == nil || controller.runtime.machine == nil {
		return
	}
	state, exists := controller.runtime.machine.ConnectionRebalance()
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if !exists || !state.Phase.Active() {
		controller.admissionClosed.Store(false)
		controller.serverReference = ""
		controller.runningID = ""
		return
	}
	controller.serverReference = state.ServerReference
	participant := false
	for _, report := range state.Participants {
		if report.NodeID == controller.runtime.config.NodeID {
			participant = true
			break
		}
	}
	if !participant {
		controller.admissionClosed.Store(false)
		controller.runningID = ""
		return
	}
	if controller.ctx == nil || controller.runningID == state.RebalanceOperationID {
		return
	}
	controller.admissionClosed.Store(false)
	controller.runningID = state.RebalanceOperationID
	operationID := state.RebalanceOperationID
	ctx := controller.ctx
	controller.wg.Go(func() { controller.run(ctx, operationID) })
}

func (controller *clusterRebalanceController) admissionOpen() bool {
	return controller == nil || !controller.admissionClosed.Load()
}

func (controller *clusterRebalanceController) currentServerReference() string {
	if controller == nil {
		return ""
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.serverReference
}

func (controller *clusterRebalanceController) finish(operationID string) {
	controller.mu.Lock()
	if controller.runningID == operationID {
		controller.admissionClosed.Store(false)
		controller.runningID = ""
	}
	controller.mu.Unlock()
}
func (controller *clusterRebalanceController) setAdmission(operationID string, closed bool) {
	controller.mu.Lock()
	if controller.runningID == operationID {
		controller.admissionClosed.Store(closed)
	}
	controller.mu.Unlock()
}

func (controller *clusterRebalanceController) run(ctx context.Context, operationID string) {
	ticker := time.NewTicker(connectionRebalanceTickInterval)
	defer ticker.Stop()
	defer controller.finish(operationID)
	var shed connrebalance.ShedState
	var transferred uint64
	transferredRestored := false
	var nextTransferAt time.Time
	var lastReport struct {
		connections uint64
		shedding    bool
		transferred uint64
		atMillis    uint64
	}
	var attempted map[routing.SessionID]struct{}
	barrierPassed := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		state, exists := controller.runtime.machine.ConnectionRebalance()
		if !exists || state.RebalanceOperationID != operationID || !state.Phase.Active() {
			controller.finish(operationID)
			return
		}
		now := time.Now()
		nowMillis := uint64(now.UnixMilli())
		if nowMillis >= state.DeadlineAtUnixMillis {
			controller.commitChange(ctx, state, membership.ConnectionRebalanceActionFail, connrebalance.FailureTimeout, 0, 0, 0, false)
			continue
		}

		var ids []routing.SessionID
		if controller.runtime.mqttServer != nil {
			ids = controller.runtime.mqttServer.ConnectedSessionIDs()
		}
		count := uint64(len(ids))
		if !transferredRestored {
			for _, report := range state.Participants {
				if report.NodeID == controller.runtime.config.NodeID {
					transferred = report.SessionsTransferred
					break
				}
			}
			transferredRestored = true
		}
		reports := make([]connrebalance.Report, len(state.Participants))
		for index, report := range state.Participants {
			reports[index] = connrebalance.Report{
				NodeID: report.NodeID, Connections: report.Connections,
				Shedding: report.Shedding, ReportedAtUnixMillis: report.ReportedAtUnixMillis,
			}
		}
		evaluation := connrebalance.Evaluate(reports, state.MaxDeviationConnections, nowMillis)
		previousStage := shed.Stage
		var wantTransfer bool
		shed, wantTransfer = connrebalance.Step(shed, evaluation, count, nowMillis, state.LoadBalancerPropagationDelayMillis)
		controller.setAdmission(operationID, shed.Shedding())
		if previousStage != connrebalance.ShedIdle && shed.Stage == connrebalance.ShedIdle {
			attempted = nil
			nextTransferAt = time.Time{}
			barrierPassed = false
		}

		if wantTransfer && (nextTransferAt.IsZero() || !now.Before(nextTransferAt)) {
			if !barrierPassed {
				if err := controller.runtime.readBarrier(ctx); err != nil || !controller.runtime.clusterProjectionReady() {
					continue
				}
				barrierPassed = true
			}
			target, found := membership.SelectRebalanceTarget(controller.runtime.config.NodeID, state.Participants, controller.runtime.connectedOtherVoters())
			if found {
				if attempted == nil {
					attempted = make(map[routing.SessionID]struct{})
				}
				candidates := make([]membership.OwnershipStateDTO, 0, len(ids))
				for _, id := range ids {
					key, err := tenancy.ParseSessionKeyRoutingID(string(id))
					if err != nil {
						continue
					}
					ownership, ok := controller.runtime.machine.GetOwnership(key.TenantID().String(), key.ClientID())
					if ok {
						candidates = append(candidates, ownership)
					}
				}
				skip := func(candidate membership.OwnershipStateDTO) bool {
					id, err := clusterSessionID(candidate.TenantID, candidate.ClientID)
					if err != nil {
						return true
					}
					_, exists := attempted[id]
					return exists
				}
				change, plan := membership.PlanRebalanceTransfer(controller.runtime.config.NodeID, target, candidates, skip)
				if plan {
					id, idErr := clusterSessionID(change.TenantID, change.ClientID)
					result, err := membership.ApplyResultDTO{}, idErr
					if err == nil {
						result, err = controller.commitOwnership(ctx, change)
					}
					if err == nil && (result.Outcome == membership.ApplyOutcomeApplied ||
						(result.Outcome == membership.ApplyOutcomeConflict && (result.Conflict == membership.ConflictStaleEpoch || result.Conflict == membership.ConflictOwnershipMissing))) {
						transferred++
					}
					if idErr == nil {
						attempted[id] = struct{}{}
					}
					rate := time.Second / time.Duration(state.ConnectionRatePerSecond)
					base := now
					if nextTransferAt.After(base) {
						base = nextTransferAt
					}
					nextTransferAt = base.Add(rate)
				}
			}
		}

		if lastReport.connections != count || lastReport.shedding != shed.Shedding() || lastReport.transferred != transferred ||
			lastReport.atMillis == 0 || nowMillis >= lastReport.atMillis && nowMillis-lastReport.atMillis >= connrebalance.ReportRefreshMillis {
			result, err := controller.commitChange(ctx, state, membership.ConnectionRebalanceActionReport, "", controller.runtime.config.NodeID, count, transferred, shed.Shedding())
			if err == nil && result.Outcome == membership.ApplyOutcomeApplied {
				lastReport.connections = count
				lastReport.shedding = shed.Shedding()
				lastReport.transferred = transferred
				lastReport.atMillis = nowMillis
			}
		}
	}
}

func (controller *clusterRebalanceController) commitOwnership(ctx context.Context, change membership.OwnershipChangeDTO) (membership.ApplyResultDTO, error) {
	operationID, err := newClusterOperationID()
	if err != nil {
		return membership.ApplyResultDTO{}, err
	}
	return controller.runtime.Commit(ctx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: operationID, Ownership: &change,
	})
}

func (controller *clusterRebalanceController) commitChange(
	ctx context.Context,
	state membership.ConnectionRebalanceStateDTO,
	action membership.ConnectionRebalanceAction,
	failureCode string,
	nodeID, connections, transferred uint64,
	shedding bool,
) (membership.ApplyResultDTO, error) {
	operationID, err := newClusterOperationID()
	if err != nil {
		return membership.ApplyResultDTO{}, err
	}
	proposedAt := time.Now().UnixMilli()
	if proposedAt <= 0 {
		return membership.ApplyResultDTO{}, errors.New("connection rebalance proposal time is invalid")
	}
	return controller.runtime.Commit(ctx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: operationID,
		ConnectionRebalance: &membership.ConnectionRebalanceChangeDTO{
			Action: action, RebalanceOperationID: state.RebalanceOperationID,
			NodeID: nodeID, Connections: connections, SessionsTransferred: transferred,
			Shedding: shedding, FailureCode: failureCode, ProposedAtUnixMillis: uint64(proposedAt),
		},
	})
}

func (runtime *clusterRuntime) clusterAdmissionOpen() bool {
	if runtime == nil {
		return true
	}
	drainOpen := runtime.drain == nil || runtime.drain.admissionOpen()
	rebalanceOpen := runtime.rebalance == nil || runtime.rebalance.admissionOpen()
	return drainOpen && rebalanceOpen
}

func (runtime *clusterRuntime) connectedOtherVoters() []uint64 {
	if runtime == nil || runtime.node == nil || runtime.transport == nil {
		return nil
	}
	status := runtime.node.Status()
	connected := make([]uint64, 0, len(status.VoterIDs))
	for _, voterID := range status.VoterIDs {
		id := uint64(voterID)
		if id == runtime.config.NodeID {
			continue
		}
		peerStatus, err := runtime.transport.Status(id)
		if err == nil && peerStatus.Connected {
			connected = append(connected, id)
		}
	}
	sort.Slice(connected, func(left, right int) bool { return connected[left] < connected[right] })
	return connected
}
