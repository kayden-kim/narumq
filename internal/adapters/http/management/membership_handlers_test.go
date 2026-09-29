package management

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"narumq/internal/application/membership"
)

const managementOperationID = "00000000-0000-4000-8000-000000000004"

type membershipServiceStub struct {
	stageOperation         membership.MembershipOperationDTO
	removeOperation        membership.MembershipOperationDTO
	getOperation           membership.MembershipOperationDTO
	stageError             error
	removeError            error
	getError               error
	stageCalls             int
	removeCalls            int
	getCalls               int
	stageRequest           membership.StageJoinRequestDTO
	removeMemberID         uint64
	removeIdempotencyKey   string
	getOperationID         string
	featureState           membership.FeatureStateDTO
	featureQueryError      error
	featureActivationErr   error
	featureQueryCalls      int
	featureActivationCalls int
	activatedFeature       string
}

func (stub *membershipServiceStub) StageJoin(_ context.Context, request membership.StageJoinRequestDTO) (membership.MembershipOperationDTO, error) {
	stub.stageCalls++
	stub.stageRequest = request
	return stub.stageOperation, stub.stageError
}

func (stub *membershipServiceStub) StartPlannedRemoval(_ context.Context, memberID uint64, idempotencyKey string, _ uint64) (membership.MembershipOperationDTO, error) {
	stub.removeCalls++
	stub.removeMemberID = memberID
	stub.removeIdempotencyKey = idempotencyKey
	return stub.removeOperation, stub.removeError
}

func (stub *membershipServiceStub) GetOperation(_ context.Context, operationID string) (membership.MembershipOperationDTO, error) {
	stub.getCalls++
	stub.getOperationID = operationID
	return stub.getOperation, stub.getError
}
func (stub *membershipServiceStub) ClusterFeatures(context.Context) (membership.FeatureStateDTO, error) {
	stub.featureQueryCalls++
	return stub.featureState, stub.featureQueryError
}

func (stub *membershipServiceStub) ActivateFeature(_ context.Context, feature string) (membership.FeatureStateDTO, error) {
	stub.featureActivationCalls++
	stub.activatedFeature = feature
	return stub.featureState, stub.featureActivationErr
}

func newMembershipHTTPTestHandler(service MembershipUseCase, role Role) http.Handler {
	return NewRouterWithMembership(
		ServerConfig{AuthToken: "cluster-secret", AuthTokenRole: role},
		&mockStateProvider{}, nil, nil, service, nil, nil,
	)
}

func validMemberAdmissionJSON() string {
	return `{"memberId":4,"address":"node4.example:19004","dnsSan":"node4.example","certificateSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","releaseVersion":"1.0.0","clusterProtocolMajor":2,"clusterProtocolMinor":0,"stateSchemaMajor":1,"stateSchemaMinor":0}`
}

func membershipRequest(method, path, body string, role Role) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer cluster-secret")
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if role == RoleClusterAdmin {
		request.Header.Set("Idempotency-Key", "add-member-4")
	}
	return request
}

func TestMembershipHTTPStageResponseAndIdempotentRetry(t *testing.T) {
	stub := &membershipServiceStub{stageOperation: membership.MembershipOperationDTO{
		OperationID: managementOperationID, Action: membership.MembershipActionStageJoin,
		MemberID: 4, Phase: membership.OperationPhaseStaged, CommittedIndex: 41,
		Record: membership.MembershipRecordDTO{Action: membership.MembershipActionStageJoin},
	}}
	handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)

	request := membershipRequest(http.MethodPost, "/api/v1/cluster/members", validMemberAdmissionJSON(), RoleClusterAdmin)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("initial stage status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data membershipOperationResponse `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode operation response: %v", err)
	}
	if envelope.Data.OperationID != managementOperationID || envelope.Data.Action != "ADD" || envelope.Data.MemberID != 4 || envelope.Data.Phase != membership.OperationPhaseStaged || envelope.Data.CommittedIndex != 41 {
		t.Fatalf("membership operation response=%+v", envelope.Data)
	}
	if strings.Contains(response.Body.String(), "certificateSha256") || strings.Contains(response.Body.String(), "node4.example") {
		t.Fatalf("response disclosed peer identity: %s", response.Body.String())
	}
	if stub.stageCalls != 1 || stub.stageRequest.IdempotencyKey != "add-member-4" || stub.stageRequest.Member.CertificateSHA256[0] != 0xaa {
		t.Fatalf("service stage request=%+v calls=%d", stub.stageRequest, stub.stageCalls)
	}

	stub.stageOperation.Replayed = true
	request = membershipRequest(http.MethodPost, "/api/v1/cluster/members", validMemberAdmissionJSON(), RoleClusterAdmin)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || stub.stageCalls != 2 {
		t.Fatalf("idempotent retry status=%d calls=%d body=%s", response.Code, stub.stageCalls, response.Body.String())
	}
}

func TestMembershipHTTPRejectsNonCanonicalAdmissionJSON(t *testing.T) {
	stub := &membershipServiceStub{}
	handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
	valid := validMemberAdmissionJSON()
	cases := []struct {
		name        string
		body        string
		contentType string
	}{
		{name: "unknown field", body: strings.TrimSuffix(valid, "}") + `,"unexpected":true}`, contentType: "application/json"},
		{name: "duplicate field", body: strings.Replace(valid, `"memberId":4`, `"memberId":4,"memberId":5`, 1), contentType: "application/json"},
		{name: "trailing value", body: valid + ` {}`, contentType: "application/json"},
		{name: "missing required field", body: `{"memberId":4}`, contentType: "application/json"},
		{name: "uppercase fingerprint", body: strings.Replace(valid, strings.Repeat("a", 64), strings.Repeat("A", 64), 1), contentType: "application/json"},
		{name: "wrong media type", body: valid, contentType: "text/plain"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/members", strings.NewReader(testCase.body))
			request.Header.Set("Authorization", "Bearer cluster-secret")
			request.Header.Set("Idempotency-Key", "add-member-4")
			request.Header.Set("Content-Type", testCase.contentType)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("invalid admission status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	if stub.stageCalls != 0 {
		t.Fatalf("invalid requests reached service %d times", stub.stageCalls)
	}
}

func TestMembershipHTTPRemovalAndReadOnlyStatusAuthorization(t *testing.T) {
	stub := &membershipServiceStub{
		removeOperation: membership.MembershipOperationDTO{
			OperationID: managementOperationID, Action: membership.MembershipActionBeginRemove,
			MemberID: 4, Phase: membership.OperationPhaseRemoving, CommittedIndex: 42,
			Record: membership.MembershipRecordDTO{TargetMemberID: 4},
		},
		getOperation: membership.MembershipOperationDTO{
			OperationID: managementOperationID, MemberID: 4,
			Phase: membership.OperationPhaseRemoving, CommittedIndex: 42,
			Record: membership.MembershipRecordDTO{TargetMemberID: 4},
		},
	}
	handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
	request := membershipRequest(http.MethodDelete, "/api/v1/cluster/members/4", "", RoleClusterAdmin)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || stub.removeCalls != 1 || stub.removeMemberID != 4 || stub.removeIdempotencyKey != "add-member-4" {
		t.Fatalf("remove status=%d calls=%d member=%d key=%q body=%s", response.Code, stub.removeCalls, stub.removeMemberID, stub.removeIdempotencyKey, response.Body.String())
	}

	request = membershipRequest(http.MethodDelete, "/api/v1/cluster/members/04", "", RoleClusterAdmin)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || stub.removeCalls != 1 {
		t.Fatalf("noncanonical removal status=%d calls=%d body=%s", response.Code, stub.removeCalls, response.Body.String())
	}

	readOnly := newMembershipHTTPTestHandler(stub, RoleReadOnly)
	request = httptest.NewRequest(http.MethodGet, "/api/v1/cluster/membership-operations/"+managementOperationID, nil)
	request.Header.Set("Authorization", "Bearer cluster-secret")
	response = httptest.NewRecorder()
	readOnly.ServeHTTP(response, request)
	if response.Code != http.StatusOK || stub.getCalls != 1 || stub.getOperationID != managementOperationID {
		t.Fatalf("operation read status=%d calls=%d id=%q body=%s", response.Code, stub.getCalls, stub.getOperationID, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/api/v1/cluster/members", strings.NewReader(validMemberAdmissionJSON()))
	request.Header.Set("Authorization", "Bearer cluster-secret")
	request.Header.Set("Idempotency-Key", "add-member-4")
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	readOnly.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || stub.stageCalls != 0 {
		t.Fatalf("read-only stage status=%d stage calls=%d", response.Code, stub.stageCalls)
	}
}

func TestMembershipHTTPMapsNotLeaderSeparatelyFromQuorum(t *testing.T) {
	stub := &membershipServiceStub{stageError: &membership.ServiceError{
		Code: membership.ServiceErrorNotLeader, Message: "node is not the membership leader", Retryable: true,
	}}
	handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
	request := membershipRequest(http.MethodPost, "/api/v1/cluster/members", validMemberAdmissionJSON(), RoleClusterAdmin)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("not-leader status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != "not_leader" || !envelope.Error.Retryable || errors.Is(stub.stageError, membership.ErrQuorumUnavailable) {
		t.Fatalf("not-leader error envelope=%+v", envelope.Error)
	}
}
