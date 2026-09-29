package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"narumq/internal/application/membership"
)

func TestClusterFeatureHTTPListReturnsNonNullArraysAndStringNodeIDs(t *testing.T) {
	stub := &membershipServiceStub{featureState: membership.FeatureStateDTO{
		Members: []membership.MemberFeatureProfileDTO{{MemberID: 17, ReleaseVersion: "1.2.0"}},
	}}
	handler := newMembershipHTTPTestHandler(stub, RoleReadOnly)
	request := membershipRequest(http.MethodGet, "/api/v1/cluster/features", "", RoleReadOnly)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data clusterFeatureStateResponse `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode feature response: %v", err)
	}
	if envelope.Data.ActiveFeatures == nil || len(envelope.Data.ActiveFeatures) != 0 {
		t.Fatalf("activeFeatures = %#v, want non-null empty array", envelope.Data.ActiveFeatures)
	}
	if len(envelope.Data.Members) != 1 || envelope.Data.Members[0].NodeID != "17" || envelope.Data.Members[0].ReleaseVersion != "1.2.0" {
		t.Fatalf("members = %+v, want nodeId \"17\" with committed profile", envelope.Data.Members)
	}
	if envelope.Data.Members[0].Features == nil || len(envelope.Data.Members[0].Features) != 0 {
		t.Fatalf("member features = %#v, want non-null empty array", envelope.Data.Members[0].Features)
	}
	if stub.featureQueryCalls != 1 || stub.featureActivationCalls != 0 {
		t.Fatalf("feature calls query=%d activation=%d", stub.featureQueryCalls, stub.featureActivationCalls)
	}
}

func TestClusterFeatureHTTPActivateAndMapUnsupportedConflict(t *testing.T) {
	stub := &membershipServiceStub{featureState: membership.FeatureStateDTO{
		ActiveFeatures: []string{"compatibility-probe"},
		Members:        []membership.MemberFeatureProfileDTO{{MemberID: 3, ReleaseVersion: "1.2.0", Features: []string{"compatibility-probe"}}},
	}}
	handler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
	request := membershipRequest(http.MethodPost, "/api/v1/cluster/features/compatibility-probe", "", RoleClusterAdmin)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || stub.featureActivationCalls != 1 || stub.activatedFeature != "compatibility-probe" {
		t.Fatalf("activate status=%d calls=%d feature=%q body=%s", response.Code, stub.featureActivationCalls, stub.activatedFeature, response.Body.String())
	}
	var envelope struct {
		Data clusterFeatureStateResponse `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode activation response: %v", err)
	}
	if len(envelope.Data.ActiveFeatures) != 1 || envelope.Data.ActiveFeatures[0] != "compatibility-probe" {
		t.Fatalf("activeFeatures = %v, want activated feature", envelope.Data.ActiveFeatures)
	}

	stub.featureActivationErr = &membership.ServiceError{
		Code: membership.ServiceErrorFeatureUnsupported, Message: "feature is not supported by every active member",
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("unsupported activation status=%d body=%s, want 409", response.Code, response.Body.String())
	}
	var failure ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
		t.Fatalf("decode activation error: %v", err)
	}
	if failure.Error.Code != "feature_not_supported_by_all_members" || failure.Error.Retryable {
		t.Fatalf("activation error = %+v, want non-retryable feature conflict", failure.Error)
	}
}

func TestClusterFeatureHTTPRejectsInvalidFeatureAndReadOnlyActivation(t *testing.T) {
	stub := &membershipServiceStub{}
	adminHandler := newMembershipHTTPTestHandler(stub, RoleClusterAdmin)
	request := membershipRequest(http.MethodPost, "/api/v1/cluster/features/Bad", "", RoleClusterAdmin)
	response := httptest.NewRecorder()
	adminHandler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || stub.featureActivationCalls != 0 {
		t.Fatalf("invalid feature status=%d activation calls=%d body=%s", response.Code, stub.featureActivationCalls, response.Body.String())
	}
	var invalid ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &invalid); err != nil {
		t.Fatalf("decode invalid-feature error: %v", err)
	}
	if invalid.Error.Code != "invalid_request" {
		t.Fatalf("invalid-feature error code=%q, want invalid_request", invalid.Error.Code)
	}

	readOnlyHandler := newMembershipHTTPTestHandler(stub, RoleReadOnly)
	request = membershipRequest(http.MethodPost, "/api/v1/cluster/features/compatibility-probe", "", RoleReadOnly)
	response = httptest.NewRecorder()
	readOnlyHandler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || stub.featureActivationCalls != 0 {
		t.Fatalf("read-only activation status=%d calls=%d body=%s", response.Code, stub.featureActivationCalls, response.Body.String())
	}
}
