package membership

import (
	"context"
	"errors"
	"testing"
	"time"

	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/domain/nodedrain"
)

type drainServiceTestClock struct{ now time.Time }

func (clock drainServiceTestClock) Now() time.Time { return clock.now }

type drainServiceTestPort struct {
	machine    *StateMachine
	commitErr  error
	membersErr error
	getErr     error
}

func (port *drainServiceTestPort) Commit(ctx context.Context, command CommandDTO) (ApplyResultDTO, error) {
	if port.commitErr != nil {
		return ApplyResultDTO{}, port.commitErr
	}
	return port.machine.Apply(port.machine.LastAppliedIndex()+1, command)
}

func (port *drainServiceTestPort) Members(context.Context) ([]MemberIdentityDTO, error) {
	if port.membersErr != nil {
		return nil, port.membersErr
	}
	return port.machine.Members(), nil
}

func (port *drainServiceTestPort) GetNodeDrain(_ context.Context, memberID uint64) (NodeDrainStateDTO, bool, error) {
	if port.getErr != nil {
		return NodeDrainStateDTO{}, false, port.getErr
	}
	state, exists := port.machine.GetNodeDrain(memberID)
	return state, exists, nil
}

func newDrainServiceTest(t *testing.T) (*DrainService, *drainServiceTestPort) {
	t.Helper()
	port := &drainServiceTestPort{machine: newDrainTestMachine(t)}
	clock := drainServiceTestClock{now: time.Unix(1_800_000_000, 123_000_000)}
	service, err := NewDrainService(port, clock)
	if err != nil {
		t.Fatalf("NewDrainService: %v", err)
	}
	return service, port
}

func drainServiceTestRequest(memberID uint64, key string) StartNodeDrainRequestDTO {
	return StartNodeDrainRequestDTO{
		MemberID: memberID, IdempotencyKey: key, ServerReference: "mqtt.example:8883",
		ConnectionRatePerSecond: 2, LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 2_000,
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §2.1, §2.2, §10.
func TestDrainServiceStartIdempotencyAndValidation(t *testing.T) {
	service, _ := newDrainServiceTest(t)
	ctx := context.Background()
	request := drainServiceTestRequest(1, "drain-key-1")
	started, err := service.StartNodeDrain(ctx, request)
	if err != nil || started.Replayed || started.State.Phase != nodedrain.PhaseExcludingTraffic {
		t.Fatalf("StartNodeDrain = (%+v, %v), want new EXCLUDING_TRAFFIC operation", started, err)
	}
	if started.State.StartedAtUnixMillis != 1_800_000_000_123 || started.State.DeadlineAtUnixMillis-started.State.StartedAtUnixMillis != request.TimeoutMillis {
		t.Fatalf("committed start/deadline = %d/%d", started.State.StartedAtUnixMillis, started.State.DeadlineAtUnixMillis)
	}

	replayed, err := service.StartNodeDrain(ctx, request)
	if err != nil || !replayed.Replayed || replayed.State.DrainOperationID != started.State.DrainOperationID {
		t.Fatalf("idempotent StartNodeDrain = (%+v, %v)", replayed, err)
	}
	changed := request
	changed.TimeoutMillis++
	if _, err := service.StartNodeDrain(ctx, changed); !isDrainServiceError(err, ServiceErrorIdempotencyConflict) {
		t.Fatalf("same key with changed parameters error = %v, want idempotency conflict", err)
	}
	otherKey := request
	otherKey.IdempotencyKey = "drain-key-2"
	otherKey.ConnectionRatePerSecond++
	replayed, err = service.StartNodeDrain(ctx, otherKey)
	if err != nil || !replayed.Replayed || replayed.State.DrainOperationID != started.State.DrainOperationID {
		t.Fatalf("active operation with another key = (%+v, %v), want replay", replayed, err)
	}

	if _, err := service.GetNodeDrain(ctx, 99); !isDrainServiceError(err, ServiceErrorResourceNotFound) {
		t.Fatalf("missing GET error = %v, want resource_not_found", err)
	}
	if _, err := service.CancelNodeDrain(ctx, 99); !isDrainServiceError(err, ServiceErrorResourceNotFound) {
		t.Fatalf("missing DELETE error = %v, want resource_not_found", err)
	}
	if _, err := service.StartNodeDrain(ctx, drainServiceTestRequest(99, "drain-key-3")); !isDrainServiceError(err, ServiceErrorResourceNotFound) {
		t.Fatalf("missing member start error = %v, want resource_not_found", err)
	}
	if _, err := service.StartNodeDrain(ctx, StartNodeDrainRequestDTO{MemberID: 1, IdempotencyKey: "drain-key-4", ConnectionRatePerSecond: 0, TimeoutMillis: 2_000}); !isDrainServiceError(err, ServiceErrorInvalidRequest) {
		t.Fatalf("invalid start error = %v, want invalid_request", err)
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §1, §2.1, §10.
func TestDrainServiceNonVoterStateAndClusterConflicts(t *testing.T) {
	t.Run("non-voter includes lifecycle state", func(t *testing.T) {
		service, port := newDrainServiceTest(t)
		join := stageRecord(testOperation4, "join-4", 4)
		applyMustSucceed(t, port.machine, 2, testCommand(testOperation4, &join))
		_, err := service.StartNodeDrain(context.Background(), drainServiceTestRequest(4, "drain-key-1"))
		if !isDrainServiceError(err, ServiceErrorInvalidNodeTransition) {
			t.Fatalf("non-voter start error = %v, want invalid_node_transition", err)
		}
		var serviceError *ServiceError
		if !errors.As(err, &serviceError) || serviceError.NodeState != "JOINING" {
			t.Fatalf("non-voter error state = %+v, want JOINING", serviceError)
		}
	})

	t.Run("another active drain maps retryable conflict", func(t *testing.T) {
		service, port := newDrainServiceTest(t)
		if _, err := service.StartNodeDrain(context.Background(), drainServiceTestRequest(1, "drain-key-1")); err != nil {
			t.Fatalf("start first drain: %v", err)
		}
		_, err := service.StartNodeDrain(context.Background(), drainServiceTestRequest(2, "drain-key-2"))
		if !isDrainServiceError(err, ServiceErrorNodeDrainInProgress) {
			t.Fatalf("concurrent drain error = %v, want node_drain_in_progress", err)
		}
		var serviceError *ServiceError
		if !errors.As(err, &serviceError) || !serviceError.Retryable {
			t.Fatalf("concurrent drain error retryable=%v", serviceError)
		}
		if _, exists := port.machine.GetNodeDrain(2); exists {
			t.Fatal("conflicting second start committed a drain")
		}
	})

	t.Run("membership change maps conflict", func(t *testing.T) {
		service, port := newDrainServiceTest(t)
		join := stageRecord(testOperation4, "join-4", 4)
		applyMustSucceed(t, port.machine, 2, testCommand(testOperation4, &join))
		if _, err := service.StartNodeDrain(context.Background(), drainServiceTestRequest(1, "drain-key-1")); !isDrainServiceError(err, ServiceErrorMembershipInProgress) {
			t.Fatalf("membership conflict error = %v", err)
		}
	})
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §1, §2.3, §10.
func TestDrainServiceCancelReplayAndCompletedRejection(t *testing.T) {
	service, port := newDrainServiceTest(t)
	ctx := context.Background()
	request := drainServiceTestRequest(2, "drain-key-1")
	started, err := service.StartNodeDrain(ctx, request)
	if err != nil {
		t.Fatalf("StartNodeDrain: %v", err)
	}
	cancelled, err := service.CancelNodeDrain(ctx, 2)
	if err != nil || cancelled.State.Phase != nodedrain.PhaseCancelled || cancelled.Replayed {
		t.Fatalf("CancelNodeDrain = (%+v, %v), want new CANCELLED state", cancelled, err)
	}
	replayed, err := service.CancelNodeDrain(ctx, 2)
	if err != nil || !replayed.Replayed || replayed.State.Phase != nodedrain.PhaseCancelled {
		t.Fatalf("repeated CancelNodeDrain = (%+v, %v), want replay", replayed, err)
	}
	replayed, err = service.StartNodeDrain(ctx, request)
	if err != nil || !replayed.Replayed || replayed.State.DrainOperationID != started.State.DrainOperationID {
		t.Fatalf("cancelled operation replay = (%+v, %v)", replayed, err)
	}

	request.IdempotencyKey = "drain-key-2"
	restarted, err := service.StartNodeDrain(ctx, request)
	if err != nil || restarted.Replayed || restarted.State.DrainOperationID == started.State.DrainOperationID {
		t.Fatalf("restart after CANCELLED = (%+v, %v), want a new operation", restarted, err)
	}
	advanceDrainServiceToBarrier(t, port, restarted.State)
	complete := NodeDrainChangeDTO{
		Action: NodeDrainActionComplete, MemberID: 2, DrainOperationID: restarted.State.DrainOperationID,
		ProposedAtUnixMillis: restarted.State.StartedAtUnixMillis + 10,
	}
	applyMustSucceed(t, port.machine, port.machine.LastAppliedIndex()+1, CommandDTO{
		SchemaMajor: SchemaMajor, OperationID: testOperation4, NodeDrain: &complete,
	})
	_, err = service.CancelNodeDrain(ctx, 2)
	if !isDrainServiceError(err, ServiceErrorDrainNotCancellable) {
		t.Fatalf("completed cancellation error = %v, want drain_not_cancellable", err)
	}
	var serviceError *ServiceError
	if !errors.As(err, &serviceError) || serviceError.NodeState != "SAFE_TO_STOP" {
		t.Fatalf("completed cancellation details = %+v, want SAFE_TO_STOP", serviceError)
	}
}

func advanceDrainServiceToBarrier(t *testing.T, port *drainServiceTestPort, state NodeDrainStateDTO) {
	t.Helper()
	for offset, phase := range []nodedrain.Phase{
		nodedrain.PhaseWaitingForLB,
		nodedrain.PhaseVerifyingSessionState,
		nodedrain.PhaseTransferringSessions,
		nodedrain.PhaseVerifyingBarrier,
	} {
		change := NodeDrainChangeDTO{
			Action: NodeDrainActionAdvance, MemberID: state.MemberID, DrainOperationID: state.DrainOperationID,
			Phase: phase, ProposedAtUnixMillis: state.StartedAtUnixMillis + uint64(offset) + 1,
		}
		applyMustSucceed(t, port.machine, port.machine.LastAppliedIndex()+1, CommandDTO{
			SchemaMajor: SchemaMajor, OperationID: testOperation4, NodeDrain: &change,
		})
	}
}

func isDrainServiceError(err error, code ServiceErrorCode) bool {
	var serviceError *ServiceError
	return errors.As(err, &serviceError) && serviceError.Code == code
}

var _ applicationclock.Clock = drainServiceTestClock{}
