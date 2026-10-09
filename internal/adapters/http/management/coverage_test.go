package management

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"narumq/internal/application/membership"
)

type cmmFailingBody struct{}

func (cmmFailingBody) Read([]byte) (int, error) {
	return 0, errors.New("body transport failed")
}

func cmmDecodeErrorEnvelope(t *testing.T, response *httptest.ResponseRecorder) ErrorBody {
	t.Helper()
	var envelope ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v body=%s", err, response.Body.String())
	}
	return envelope.Error
}

func cmmMembershipRequest(method, path string, body io.Reader) *http.Request {
	request := httptest.NewRequest(method, path, body)
	request.Header.Set("X-API-Key", "cluster-secret")
	return request
}

func TestCmmMembershipHTTPRejectsWrongMethodsAndQueryParameters(t *testing.T) {
	stub := &membershipServiceStub{}
	handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
	cases := []struct {
		name   string
		method string
		path   string
		status int
		code   string
	}{
		{name: "stage with GET", method: http.MethodGet, path: "/api/v1/cluster/members", status: http.StatusMethodNotAllowed, code: "method_not_allowed"},
		{name: "stage with query", method: http.MethodPost, path: "/api/v1/cluster/members?dryRun=true", status: http.StatusBadRequest, code: "invalid_request"},
		{name: "remove with GET", method: http.MethodGet, path: "/api/v1/cluster/members/4", status: http.StatusMethodNotAllowed, code: "method_not_allowed"},
		{name: "remove with query", method: http.MethodDelete, path: "/api/v1/cluster/members/4?force=true", status: http.StatusBadRequest, code: "invalid_request"},
		{name: "operation with POST", method: http.MethodPost, path: "/api/v1/cluster/membership-operations/" + managementOperationID, status: http.StatusMethodNotAllowed, code: "method_not_allowed"},
		{name: "operation with query", method: http.MethodGet, path: "/api/v1/cluster/membership-operations/" + managementOperationID + "?verbose=1", status: http.StatusBadRequest, code: "invalid_request"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request := cmmMembershipRequest(testCase.method, testCase.path, strings.NewReader(validMemberAdmissionJSON()))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", "cmm-key")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != testCase.status {
				t.Fatalf("status=%d want %d body=%s", response.Code, testCase.status, response.Body.String())
			}
			if body := cmmDecodeErrorEnvelope(t, response); body.Code != testCase.code {
				t.Fatalf("error code=%q want %q", body.Code, testCase.code)
			}
		})
	}
	if stub.stageCalls != 0 || stub.removeCalls != 0 || stub.getCalls != 0 {
		t.Fatalf("rejected requests reached service stage=%d remove=%d get=%d", stub.stageCalls, stub.removeCalls, stub.getCalls)
	}
}

func TestCmmMembershipHTTPRequiresExactlyOneVisibleIdempotencyKey(t *testing.T) {
	cases := []struct {
		name   string
		values []string
	}{
		{name: "missing", values: nil},
		{name: "repeated", values: []string{"key-a", "key-b"}},
		{name: "empty", values: []string{""}},
		{name: "longer than 128 bytes", values: []string{strings.Repeat("k", 129)}},
		{name: "contains space", values: []string{"key with space"}},
		{name: "contains control byte", values: []string{"key\x7f"}},
	}
	for _, route := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "stage", method: http.MethodPost, path: "/api/v1/cluster/members", body: validMemberAdmissionJSON()},
		{name: "remove", method: http.MethodDelete, path: "/api/v1/cluster/members/4"},
	} {
		for _, testCase := range cases {
			t.Run(route.name+"/"+testCase.name, func(t *testing.T) {
				stub := &membershipServiceStub{}
				handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
				request := cmmMembershipRequest(route.method, route.path, strings.NewReader(route.body))
				if route.body != "" {
					request.Header.Set("Content-Type", "application/json")
				}
				if testCase.values != nil {
					request.Header["Idempotency-Key"] = testCase.values
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusBadRequest {
					t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
				}
				if body := cmmDecodeErrorEnvelope(t, response); body.Code != "invalid_request" || !strings.Contains(body.Message, "Idempotency-Key") {
					t.Fatalf("error=%+v", body)
				}
				if stub.stageCalls != 0 || stub.removeCalls != 0 {
					t.Fatalf("invalid key reached service stage=%d remove=%d", stub.stageCalls, stub.removeCalls)
				}
			})
		}
	}

	stub := &membershipServiceStub{}
	handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
	boundaryKey := strings.Repeat("~", 128)
	request := cmmMembershipRequest(http.MethodDelete, "/api/v1/cluster/members/4", nil)
	request.Header.Set("Idempotency-Key", boundaryKey)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || stub.removeIdempotencyKey != boundaryKey {
		t.Fatalf("128-byte visible key status=%d key=%q body=%s", response.Code, stub.removeIdempotencyKey, response.Body.String())
	}
}

func TestCmmMembershipHTTPRemovalRejectsBodyAndMapsOutcome(t *testing.T) {
	t.Run("body is rejected before service", func(t *testing.T) {
		stub := &membershipServiceStub{}
		handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
		request := cmmMembershipRequest(http.MethodDelete, "/api/v1/cluster/members/4", strings.NewReader("{}"))
		request.Header.Set("Idempotency-Key", "remove-4")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || stub.removeCalls != 0 {
			t.Fatalf("status=%d calls=%d body=%s", response.Code, stub.removeCalls, response.Body.String())
		}
		if body := cmmDecodeErrorEnvelope(t, response); !strings.Contains(body.Message, "must not contain a body") {
			t.Fatalf("error=%+v", body)
		}
	})

	t.Run("unreadable body is rejected before service", func(t *testing.T) {
		stub := &membershipServiceStub{}
		handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
		request := cmmMembershipRequest(http.MethodDelete, "/api/v1/cluster/members/4", cmmFailingBody{})
		request.Header.Set("Idempotency-Key", "remove-4")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || stub.removeCalls != 0 {
			t.Fatalf("status=%d calls=%d body=%s", response.Code, stub.removeCalls, response.Body.String())
		}
	})

	t.Run("service error is mapped", func(t *testing.T) {
		stub := &membershipServiceStub{removeError: &membership.ServiceError{
			Code: membership.ServiceErrorMembershipInProgress, Message: "membership change in progress", Retryable: true,
		}}
		handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
		request := cmmMembershipRequest(http.MethodDelete, "/api/v1/cluster/members/4", nil)
		request.Header.Set("Idempotency-Key", "remove-4")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusConflict || stub.removeCalls != 1 {
			t.Fatalf("status=%d calls=%d body=%s", response.Code, stub.removeCalls, response.Body.String())
		}
		body := cmmDecodeErrorEnvelope(t, response)
		if body.Code != "membership_change_in_progress" || !body.Retryable || body.Message != "membership change in progress" {
			t.Fatalf("error=%+v", body)
		}
	})

	for _, testCase := range []struct {
		name      string
		operation membership.MembershipOperationDTO
	}{
		{name: "replayed", operation: membership.MembershipOperationDTO{
			OperationID: managementOperationID, MemberID: 4, Phase: membership.OperationPhaseRemoving, Replayed: true,
			Record: membership.MembershipRecordDTO{TargetMemberID: 4},
		}},
		{name: "already succeeded", operation: membership.MembershipOperationDTO{
			OperationID: managementOperationID, MemberID: 4, Phase: membership.OperationPhaseSucceeded,
			Record: membership.MembershipRecordDTO{TargetMemberID: 4},
		}},
		{name: "already aborted", operation: membership.MembershipOperationDTO{
			OperationID: managementOperationID, MemberID: 4, Phase: membership.OperationPhaseAborted,
			Record: membership.MembershipRecordDTO{TargetMemberID: 4},
		}},
	} {
		t.Run(testCase.name+" returns 200", func(t *testing.T) {
			stub := &membershipServiceStub{removeOperation: testCase.operation}
			handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
			request := cmmMembershipRequest(http.MethodDelete, "/api/v1/cluster/members/4", nil)
			request.Header.Set("Idempotency-Key", "remove-4")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			var envelope struct {
				Data membershipOperationResponse `json:"data"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Data.Action != "REMOVE" || envelope.Data.Phase != testCase.operation.Phase {
				t.Fatalf("operation=%+v", envelope.Data)
			}
		})
	}
}

func TestCmmMembershipHTTPStrictJSONRejections(t *testing.T) {
	valid := validMemberAdmissionJSON()
	cases := []struct {
		name         string
		body         io.Reader
		contentTypes []string
		message      string
	}{
		{name: "missing content type", body: strings.NewReader(valid), message: "one Content-Type header is required"},
		{name: "repeated content type", body: strings.NewReader(valid), contentTypes: []string{"application/json", "application/json"}, message: "one Content-Type header is required"},
		{name: "malformed content type", body: strings.NewReader(valid), contentTypes: []string{"application/json; charset"}, message: "Content-Type must be application/json"},
		{name: "unreadable body", body: cmmFailingBody{}, contentTypes: []string{"application/json"}, message: "could not be read"},
		{name: "empty body", body: strings.NewReader(""), contentTypes: []string{"application/json"}, message: "empty, oversized, or invalid UTF-8"},
		{name: "oversized body", body: strings.NewReader(strings.Repeat(" ", maxMembershipRequestBytes) + valid), contentTypes: []string{"application/json"}, message: "empty, oversized, or invalid UTF-8"},
		{name: "invalid UTF-8", body: strings.NewReader(strings.Replace(valid, "node4.example:19004", "node4\xff", 1)), contentTypes: []string{"application/json"}, message: "empty, oversized, or invalid UTF-8"},
		{name: "not JSON", body: strings.NewReader("}"), contentTypes: []string{"application/json"}, message: "invalid or contains duplicate fields"},
		{name: "non-string object key", body: strings.NewReader(`{1:2}`), contentTypes: []string{"application/json"}, message: "invalid or contains duplicate fields"},
		{name: "unterminated object", body: strings.NewReader(`{"memberId":4`), contentTypes: []string{"application/json"}, message: "invalid or contains duplicate fields"},
		{name: "trailing comma in object", body: strings.NewReader(`{"memberId":4,}`), contentTypes: []string{"application/json"}, message: "invalid or contains duplicate fields"},
		{name: "nested duplicate key", body: strings.NewReader(`{"address":{"host":"a","host":"b"}}`), contentTypes: []string{"application/json"}, message: "invalid or contains duplicate fields"},
		{name: "duplicate key inside array", body: strings.NewReader(`{"address":[{"k":1,"k":2}]}`), contentTypes: []string{"application/json"}, message: "invalid or contains duplicate fields"},
		{name: "unterminated array", body: strings.NewReader(`{"address":[1,2`), contentTypes: []string{"application/json"}, message: "invalid or contains duplicate fields"},
		{name: "array with trailing comma", body: strings.NewReader(`[1,]`), contentTypes: []string{"application/json"}, message: "invalid or contains duplicate fields"},
		{name: "well-formed array with wrong type", body: strings.NewReader(`{"address":["node4.example"]}`), contentTypes: []string{"application/json"}, message: "unknown field or invalid value"},
		{name: "top-level array", body: strings.NewReader(`[]`), contentTypes: []string{"application/json"}, message: "unknown field or invalid value"},
		{name: "top-level scalar followed by trailing value", body: strings.NewReader(`4 5`), contentTypes: []string{"application/json"}, message: "invalid or contains duplicate fields"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := &membershipServiceStub{}
			handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
			request := cmmMembershipRequest(http.MethodPost, "/api/v1/cluster/members", testCase.body)
			request.Header.Set("Idempotency-Key", "add-member-4")
			request.Header["Content-Type"] = testCase.contentTypes
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			body := cmmDecodeErrorEnvelope(t, response)
			if body.Code != "invalid_request" || !strings.Contains(body.Message, testCase.message) {
				t.Fatalf("error=%+v want message containing %q", body, testCase.message)
			}
			if stub.stageCalls != 0 {
				t.Fatalf("rejected body reached service %d times", stub.stageCalls)
			}
		})
	}
}

func TestCmmMembershipHTTPAcceptsJSONMediaTypeParametersAndTerminalStage(t *testing.T) {
	stub := &membershipServiceStub{stageOperation: membership.MembershipOperationDTO{
		OperationID: managementOperationID, MemberID: 4, Phase: membership.OperationPhaseSucceeded, CommittedIndex: 50,
	}}
	handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
	request := cmmMembershipRequest(http.MethodPost, "/api/v1/cluster/members", strings.NewReader(" \n"+validMemberAdmissionJSON()+"\n "))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set("Idempotency-Key", "add-member-4")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || stub.stageCalls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", response.Code, stub.stageCalls, response.Body.String())
	}
	member := stub.stageRequest.Member
	if member.MemberID != 4 || member.Address != "node4.example:19004" || member.DNSSAN != "node4.example" || member.ReleaseVersion != "1.0.0" ||
		member.ClusterProtocolMajor != 2 || member.ClusterProtocolMinor != 0 || member.StateSchemaMajor != 1 || member.StateSchemaMinor != 0 || len(member.CertificateSHA256) != 32 {
		t.Fatalf("stage member=%+v", member)
	}
}

func TestCmmMembershipHTTPMapsServiceErrors(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		status    int
		code      string
		message   string
		retryable bool
	}{
		{name: "invalid request", err: &membership.ServiceError{Code: membership.ServiceErrorInvalidRequest, Message: "bad member"}, status: http.StatusBadRequest, code: "invalid_request", message: "bad member"},
		{name: "not found", err: &membership.ServiceError{Code: membership.ServiceErrorResourceNotFound, Message: "operation not found"}, status: http.StatusNotFound, code: "resource_not_found", message: "operation not found"},
		{name: "member id reserved", err: &membership.ServiceError{Code: membership.ServiceErrorMemberIDReserved, Message: "reserved"}, status: http.StatusConflict, code: "member_id_reserved", message: "reserved"},
		{name: "identity conflict", err: &membership.ServiceError{Code: membership.ServiceErrorMemberIdentityConflict, Message: "identity"}, status: http.StatusConflict, code: "member_identity_conflict", message: "identity"},
		{name: "idempotency conflict", err: &membership.ServiceError{Code: membership.ServiceErrorIdempotencyConflict, Message: "key reused"}, status: http.StatusConflict, code: "idempotency_conflict", message: "key reused"},
		{name: "operation conflict", err: &membership.ServiceError{Code: membership.ServiceErrorOperationConflict, Message: "conflict"}, status: http.StatusConflict, code: "operation_conflict", message: "conflict"},
		{name: "compatibility", err: &membership.ServiceError{Code: membership.ServiceErrorCompatibilityUnsupported, Message: "unsupported"}, status: http.StatusUnprocessableEntity, code: "compatibility_unsupported", message: "unsupported"},
		{name: "quorum", err: &membership.ServiceError{Code: membership.ServiceErrorQuorumUnavailable, Message: "no quorum", Retryable: true}, status: http.StatusServiceUnavailable, code: "quorum_unavailable", message: "no quorum", retryable: true},
		{name: "unknown service code is sanitized", err: &membership.ServiceError{Code: "disk_on_fire", Message: "secret adapter detail", Retryable: true}, status: http.StatusInternalServerError, code: "internal_error", message: "internal membership error"},
		{name: "foreign error is sanitized", err: errors.New("raft: secret adapter detail"), status: http.StatusInternalServerError, code: "internal_error", message: "internal membership error"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := &membershipServiceStub{getError: testCase.err}
			handler := newMembershipHTTPTestHandler(stub, RoleReadOnly)
			request := cmmMembershipRequest(http.MethodGet, "/api/v1/cluster/membership-operations/"+managementOperationID, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != testCase.status || stub.getCalls != 1 {
				t.Fatalf("status=%d want %d calls=%d body=%s", response.Code, testCase.status, stub.getCalls, response.Body.String())
			}
			body := cmmDecodeErrorEnvelope(t, response)
			if body.Code != testCase.code || body.Message != testCase.message || body.Retryable != testCase.retryable {
				t.Fatalf("error=%+v want code=%q message=%q retryable=%v", body, testCase.code, testCase.message, testCase.retryable)
			}
			if strings.Contains(response.Body.String(), "secret adapter detail") {
				t.Fatalf("response leaked internal detail: %s", response.Body.String())
			}
		})
	}
}
