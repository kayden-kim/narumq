package membership

import (
	"context"
	"errors"
	"testing"
	"time"

	"narumq/internal/domain/connrebalance"
)

type rebalanceServiceTestPort struct {
	machine *StateMachine
	err     error
}

func (p *rebalanceServiceTestPort) Commit(_ context.Context, c CommandDTO) (ApplyResultDTO, error) {
	if p.err != nil {
		return ApplyResultDTO{}, p.err
	}
	return p.machine.Apply(p.machine.LastAppliedIndex()+1, c)
}
func (p *rebalanceServiceTestPort) ActiveFeatures(context.Context) ([]string, error) {
	return p.machine.ActiveFeatures(), p.err
}
func (p *rebalanceServiceTestPort) GetConnectionRebalance(context.Context) (ConnectionRebalanceStateDTO, bool, error) {
	s, ok := p.machine.ConnectionRebalance()
	return s, ok, p.err
}
func newRebalanceServiceTest(t *testing.T) (*RebalanceService, *rebalanceServiceTestPort) {
	t.Helper()
	p := &rebalanceServiceTestPort{machine: activeConnectionRebalanceMachine(t)}
	s, err := NewRebalanceService(p, drainServiceTestClock{time.UnixMilli(1000)})
	if err != nil {
		t.Fatal(err)
	}
	return s, p
}
func rebalanceServiceTestRequest() StartConnectionRebalanceRequestDTO {
	return StartConnectionRebalanceRequestDTO{IdempotencyKey: "rb-1", ServerReference: "lb:1883", MaxDeviationConnections: 1, ConnectionRatePerSecond: 5, LoadBalancerPropagationDelayMillis: 500, TimeoutMillis: 120_000}
}
func assertRebalanceServiceError(t *testing.T, err error, code ServiceErrorCode, retryable bool) {
	t.Helper()
	var e *ServiceError
	if !errors.As(err, &e) || e.Code != code || e.Retryable != retryable {
		t.Fatalf("error=%v, want %s retryable=%v", err, code, retryable)
	}
}

// Traceability: CLU-13; docs/specs/cluster-session-and-delivery.md §9, §11.31.
func TestRebalanceServiceReplayAndConflicts(t *testing.T) {
	s, p := newRebalanceServiceTest(t)
	ctx := context.Background()
	request := rebalanceServiceTestRequest()
	initial, err := s.StartConnectionRebalance(ctx, request)
	if err != nil || initial.Replayed || initial.State.Phase != connrebalance.PhaseRunning {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	replay, err := s.StartConnectionRebalance(ctx, request)
	if err != nil || !replay.Replayed || replay.State.RebalanceOperationID != initial.State.RebalanceOperationID {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	for _, mutate := range []func(*StartConnectionRebalanceRequestDTO){func(r *StartConnectionRebalanceRequestDTO) { r.ServerReference = "other:1883" }, func(r *StartConnectionRebalanceRequestDTO) { r.MaxDeviationConnections++ }, func(r *StartConnectionRebalanceRequestDTO) { r.ConnectionRatePerSecond++ }, func(r *StartConnectionRebalanceRequestDTO) { r.LoadBalancerPropagationDelayMillis++ }, func(r *StartConnectionRebalanceRequestDTO) { r.TimeoutMillis++ }} {
		changed := request
		mutate(&changed)
		_, err := s.StartConnectionRebalance(ctx, changed)
		assertRebalanceServiceError(t, err, ServiceErrorIdempotencyConflict, false)
	}
	changed := request
	changed.IdempotencyKey = "other"
	_, err = s.StartConnectionRebalance(ctx, changed)
	assertRebalanceServiceError(t, err, ServiceErrorConnectionRebalanceInProgress, true)
	cancelled, err := s.CancelConnectionRebalance(ctx)
	if err != nil || cancelled.State.Phase != connrebalance.PhaseCancelled || cancelled.Replayed {
		t.Fatalf("cancel=%+v err=%v", cancelled, err)
	}
	replay, err = s.CancelConnectionRebalance(ctx)
	if err != nil || !replay.Replayed {
		t.Fatalf("cancel replay=%+v err=%v", replay, err)
	}
	replay, err = s.StartConnectionRebalance(ctx, request)
	if err != nil || !replay.Replayed {
		t.Fatalf("terminal replay=%+v err=%v", replay, err)
	}
	next, err := s.StartConnectionRebalance(ctx, changed)
	if err != nil || next.Replayed || next.State.RebalanceOperationID == initial.State.RebalanceOperationID {
		t.Fatalf("next=%+v err=%v", next, err)
	}
	drain, _ := NewDrainService(&drainServiceTestPort{machine: p.machine}, drainServiceTestClock{time.UnixMilli(1000)})
	_, err = drain.StartNodeDrain(ctx, drainServiceTestRequest(1, "drain"))
	assertRebalanceServiceError(t, err, ServiceErrorConnectionRebalanceInProgress, true)
}

func TestRebalanceServiceFeatureAndContextErrors(t *testing.T) {
	s, p := newRebalanceServiceTest(t)
	request := rebalanceServiceTestRequest()
	p.machine.activeFeatures = nil
	_, err := s.StartConnectionRebalance(context.Background(), request)
	assertRebalanceServiceError(t, err, ServiceErrorFeatureInactive, false)
	if _, exists := p.machine.ConnectionRebalance(); exists {
		t.Fatal("inactive feature changed state")
	}
	_, err = s.StartConnectionRebalance(nil, request)
	assertRebalanceServiceError(t, err, ServiceErrorInvalidRequest, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.CancelConnectionRebalance(ctx)
	assertRebalanceServiceError(t, err, ServiceErrorQuorumUnavailable, true)
	_, err = s.CancelConnectionRebalance(nil)
	assertRebalanceServiceError(t, err, ServiceErrorInvalidRequest, false)
	_, err = s.CancelConnectionRebalance(context.Background())
	assertRebalanceServiceError(t, err, ServiceErrorResourceNotFound, false)
	p.err = ErrQuorumUnavailable
	_, err = s.GetConnectionRebalance(context.Background())
	assertRebalanceServiceError(t, err, ServiceErrorQuorumUnavailable, true)
}

func TestRebalanceServiceTerminalCancellation(t *testing.T) {
	for _, phase := range []connrebalance.Phase{connrebalance.PhaseCompleted, connrebalance.PhaseFailed} {
		t.Run(phase.String(), func(t *testing.T) {
			s, p := newRebalanceServiceTest(t)
			started, err := s.StartConnectionRebalance(context.Background(), rebalanceServiceTestRequest())
			if err != nil {
				t.Fatal(err)
			}
			if phase == connrebalance.PhaseFailed {
				_, err = p.Commit(context.Background(), connectionRebalanceCommand(10, ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionFail, RebalanceOperationID: started.State.RebalanceOperationID, FailureCode: connrebalance.FailureTimeout, ProposedAtUnixMillis: 2000}))
			} else {
				for _, nodeID := range []uint64{1, 2, 3} {
					_, err = p.Commit(context.Background(), connectionRebalanceCommand(10+nodeID, ConnectionRebalanceChangeDTO{Action: ConnectionRebalanceActionReport, RebalanceOperationID: started.State.RebalanceOperationID, NodeID: nodeID, Connections: 3, ProposedAtUnixMillis: 2000}))
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.CancelConnectionRebalance(context.Background())
			assertRebalanceServiceError(t, err, ServiceErrorRebalanceNotCancellable, false)
			var e *ServiceError
			errors.As(err, &e)
			if e.NodeState != phase.String() {
				t.Fatalf("state=%s", e.NodeState)
			}
			replay, err := s.StartConnectionRebalance(context.Background(), rebalanceServiceTestRequest())
			if err != nil || !replay.Replayed || replay.State.Phase != phase {
				t.Fatalf("terminal replay=%+v err=%v", replay, err)
			}
		})
	}
}
