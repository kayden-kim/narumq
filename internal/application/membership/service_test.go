package membership

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type stateMachinePort struct {
	machine           *StateMachine
	nextIndex         uint64
	commitCalls       int
	returnUncommitted bool
	commitError       error
}

func (port *stateMachinePort) Commit(ctx context.Context, command CommandDTO) (ApplyResultDTO, error) {
	port.commitCalls++
	if err := ctx.Err(); err != nil {
		return ApplyResultDTO{}, err
	}
	if port.commitError != nil {
		return ApplyResultDTO{}, port.commitError
	}
	if port.returnUncommitted {
		return ApplyResultDTO{}, nil
	}
	port.nextIndex++
	return port.machine.Apply(port.nextIndex, command)
}

func (port *stateMachinePort) Members(ctx context.Context) ([]MemberIdentityDTO, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return port.machine.Members(), nil
}

func (port *stateMachinePort) GetOperation(ctx context.Context, operationID string) (MembershipOperationDTO, bool, error) {
	if err := ctx.Err(); err != nil {
		return MembershipOperationDTO{}, false, err
	}
	operation, exists := port.machine.GetOperation(operationID)
	return operation, exists, nil
}
func (port *stateMachinePort) GetOperationByIdempotencyKey(ctx context.Context, idempotencyKey string) (MembershipOperationDTO, bool, error) {
	if err := ctx.Err(); err != nil {
		return MembershipOperationDTO{}, false, err
	}
	operation, exists := port.machine.GetOperationByIdempotencyKey(idempotencyKey)
	return operation, exists, nil
}
func (port *stateMachinePort) GetOwnership(ctx context.Context, tenantID, clientID string) (OwnershipStateDTO, bool, error) {
	if err := ctx.Err(); err != nil {
		return OwnershipStateDTO{}, false, err
	}
	ownership, exists := port.machine.GetOwnership(tenantID, clientID)
	return ownership, exists, nil
}

func newServiceFixture(t *testing.T) (*Service, *stateMachinePort) {
	t.Helper()
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, testVoters()); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	port := &stateMachinePort{machine: machine, nextIndex: 1}
	service, err := NewService(port)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return service, port
}

func TestServiceStageJoinIdempotencyAndOperationStatus(t *testing.T) {
	service, port := newServiceFixture(t)
	request := StageJoinRequestDTO{IdempotencyKey: "join-key", Member: testMember(4, MemberPhaseUnspecified, "")}
	request.Member.ClusterID = ""
	first, err := service.StageJoin(context.Background(), request)
	if err != nil {
		t.Fatalf("stage join: %v", err)
	}
	if first.Phase != OperationPhaseStaged || first.MemberID != 4 || first.CommittedIndex != 2 || first.Record.Member.Phase != MemberPhaseStaged || first.Replayed {
		t.Fatalf("stage result = %+v", first)
	}
	if got, want := first.Record.Member.ClusterID, testVoters()[0].ClusterID; got != want {
		t.Fatalf("staged cluster ID = %q, want committed cluster ID %q", got, want)
	}
	retry, err := service.StageJoin(context.Background(), request)
	if err != nil {
		t.Fatalf("same-content retry: %v", err)
	}
	if retry.OperationID != first.OperationID || retry.CommittedIndex != first.CommittedIndex || !retry.Replayed || port.commitCalls != 1 {
		t.Fatalf("retry = %+v original=%+v commitCalls=%d", retry, first, port.commitCalls)
	}

	changed := request
	changed.Member.Address = "other.example:19004"
	_, err = service.StageJoin(context.Background(), changed)
	var serviceError *ServiceError
	if !errors.As(err, &serviceError) || serviceError.Code != ServiceErrorIdempotencyConflict || strings.Contains(err.Error(), "other.example") {
		t.Fatalf("changed-content service error=%v, want sanitized idempotency conflict", err)
	}
	status, err := service.GetOperation(context.Background(), first.OperationID)
	if err != nil || status.OperationID != first.OperationID || status.Phase != OperationPhaseStaged {
		t.Fatalf("operation status=(%+v, %v)", status, err)
	}
	if _, err := service.GetOperation(context.Background(), "not-a-uuid"); !errors.As(err, &serviceError) || serviceError.Code != ServiceErrorInvalidRequest {
		t.Fatalf("invalid operation ID error=%v", err)
	}
	if _, err := service.GetOperation(context.Background(), "00000000-0000-4000-8000-000000000099"); !errors.As(err, &serviceError) || serviceError.Code != ServiceErrorResourceNotFound {
		t.Fatalf("unknown operation error=%v", err)
	}
}

func TestServiceRejectsUnsupportedIdentityAndUncommittedPortResult(t *testing.T) {
	service, port := newServiceFixture(t)
	request := StageJoinRequestDTO{IdempotencyKey: "join-key", Member: testMember(4, MemberPhaseVoter, "")}
	request.Member.ClusterProtocolMajor = 3
	_, err := service.StageJoin(context.Background(), request)
	var serviceError *ServiceError
	if !errors.As(err, &serviceError) || serviceError.Code != ServiceErrorCompatibilityUnsupported || port.commitCalls != 0 {
		t.Fatalf("unsupported version error=%v commits=%d", err, port.commitCalls)
	}

	request.Member = testMember(4, MemberPhaseStaged, "")
	request.Member.ReleaseVersion = "2.0.0"
	_, err = service.StageJoin(context.Background(), request)
	if !errors.As(err, &serviceError) || serviceError.Code != ServiceErrorCompatibilityUnsupported || port.commitCalls != 1 {
		t.Fatalf("incompatible release error=%v commits=%d", err, port.commitCalls)
	}

	request.Member.ReleaseVersion = "1.2.3"
	port.returnUncommitted = true
	_, err = service.StageJoin(context.Background(), request)
	if !errors.As(err, &serviceError) || serviceError.Code != ServiceErrorQuorumUnavailable || !serviceError.Retryable {
		t.Fatalf("uncommitted port response error=%v, want retryable quorum failure", err)
	}
}

func TestServicePlannedRemovalRetriesCommittedOperation(t *testing.T) {
	service, port := newServiceFixture(t)
	first, err := service.StartPlannedRemoval(context.Background(), 3, "remove-key", 17)
	if err != nil {
		t.Fatalf("start removal: %v", err)
	}
	if first.Phase != OperationPhaseRemoving || first.MemberID != 3 || first.CommittedIndex != 2 || first.Record.ExpectedConfStateIndex != 17 {
		t.Fatalf("removal result = %+v", first)
	}
	retry, err := service.StartPlannedRemoval(context.Background(), 3, "remove-key", 17)
	if err != nil {
		t.Fatalf("retry removal: %v", err)
	}
	if retry.OperationID != first.OperationID || retry.CommittedIndex != first.CommittedIndex || !retry.Replayed || port.commitCalls != 1 {
		t.Fatalf("remove retry=%+v original=%+v commitCalls=%d", retry, first, port.commitCalls)
	}
	if _, err := service.StartPlannedRemoval(context.Background(), 3, "changed-key", 17); err == nil {
		t.Fatal("different key unexpectedly started a second removal")
	} else {
		var serviceError *ServiceError
		if !errors.As(err, &serviceError) || serviceError.Code != ServiceErrorOperationConflict {
			t.Fatalf("removal conflict error=%v, want typed operation conflict", err)
		}
	}

	removed, err := port.machine.ApplyConfiguration(5, first.OperationID, ConfigurationActionRemoveMember, 3)
	if err != nil || removed.Outcome != ApplyOutcomeApplied {
		t.Fatalf("complete removal: result=%+v err=%v", removed, err)
	}
	port.nextIndex = 5
	retryAfterRetirement, err := service.StartPlannedRemoval(context.Background(), 3, "remove-key", 17)
	if err != nil || retryAfterRetirement.OperationID != first.OperationID || !retryAfterRetirement.Replayed {
		t.Fatalf("retry after retirement=(%+v, %v)", retryAfterRetirement, err)
	}
}

func TestServiceRedactsPortCauseAndHonorsCanceledContext(t *testing.T) {
	service, port := newServiceFixture(t)
	port.commitError = errors.New("private adapter cause")
	_, err := service.StartPlannedRemoval(context.Background(), 3, "remove-key", 1)
	var serviceError *ServiceError
	if !errors.As(err, &serviceError) || serviceError.Code != ServiceErrorQuorumUnavailable || strings.Contains(err.Error(), "private adapter cause") {
		t.Fatalf("port failure was not sanitized: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.GetOperation(canceled, testOperation4); !errors.As(err, &serviceError) || serviceError.Code != ServiceErrorQuorumUnavailable {
		t.Fatalf("canceled context error=%v", err)
	}
}

func TestServicePreservesNotLeaderPortClassification(t *testing.T) {
	service, port := newServiceFixture(t)
	port.commitError = ErrNotLeader
	request := StageJoinRequestDTO{IdempotencyKey: "join-key", Member: testMember(4, MemberPhaseStaged, "")}
	_, err := service.StageJoin(context.Background(), request)
	var serviceError *ServiceError
	if !errors.As(err, &serviceError) || !errors.Is(err, ErrNotLeader) || serviceError.Code != ServiceErrorNotLeader || !serviceError.Retryable {
		t.Fatalf("stage non-leader error=%v, want retryable not_leader", err)
	}

	port.commitError = ErrNotLeader
	_, err = service.StartPlannedRemoval(context.Background(), 3, "remove-key", 1)
	if !errors.As(err, &serviceError) || !errors.Is(err, ErrNotLeader) || serviceError.Code != ServiceErrorNotLeader || !serviceError.Retryable {
		t.Fatalf("remove non-leader error=%v, want retryable not_leader", err)
	}
}
