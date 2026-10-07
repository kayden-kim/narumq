package auditeventv1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"narumq/internal/adapters/auditlog"
	audithttp "narumq/internal/adapters/http/management"
	auditapp "narumq/internal/application/audit"
	appmembership "narumq/internal/application/membership"
	portsclock "narumq/internal/application/ports/clock"
	portsmanagement "narumq/internal/application/ports/management"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/connrebalance"
	"narumq/internal/domain/nodedrain"
)

type contractClock struct{ now time.Time }

func (clock contractClock) Now() time.Time { return clock.now }

type contractMetrics struct{}

func (contractMetrics) AuditOutcome(string)         {}
func (contractMetrics) SetAuditPublisherReady(bool) {}

var _ portsclock.Clock = contractClock{}
var _ portsmetrics.AuditOutcomeRecorder = contractMetrics{}
var _ portsmanagement.ClusterBackupProvider = contractBackupProvider{}
var _ audithttp.MembershipUseCase = contractMembershipService{}
var _ audithttp.NodeDrainUseCase = contractDrainService{}
var _ audithttp.ConnectionRebalanceUseCase = contractRebalanceService{}

// Traceability: SEC-11/API-08/NFR-17; docs/specs/tenancy-and-security.md §9.
func TestAuditEventMatchesSchemaAndFileChain(t *testing.T) {
	schema := compileAuditEventSchema(t)
	directory := t.TempDir()
	publisher, err := auditlog.OpenFileAuditPublisher(auditlog.FileAuditPublisherConfig{
		Directory:    directory,
		MaxFileBytes: auditlog.DefaultMaxFileBytes,
	})
	if err != nil {
		t.Fatalf("OpenFileAuditPublisher() error = %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	service, err := auditapp.NewService(auditapp.ServiceConfig{
		ClusterID:             "contract-cluster",
		NodeID:                1,
		ReleaseVersion:        "1.0.0",
		DenialEventsPerSecond: 20,
	}, publisher, contractClock{now: time.Date(2026, time.October, 8, 12, 30, 0, 123456789, time.FixedZone("test", 2*60*60))}, contractMetrics{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	const (
		adminSecret    = "known-contract-admin-secret-do-not-persist"
		readOnlySecret = "known-contract-readonly-secret-do-not-persist"
		invalidSecret  = "known-contract-invalid-secret-do-not-persist"
	)
	membershipService := contractMembershipService{
		featureError: &appmembership.ServiceError{
			Code: appmembership.ServiceErrorFeatureUnsupported, Message: "feature is unsupported by all members",
		},
	}
	drainService := contractDrainService{}
	rebalanceService := contractRebalanceService{}
	backupProvider := contractBackupProvider{}
	adminRouter := newContractManagementRouter(adminSecret, audithttp.RoleClusterAdmin, membershipService, drainService, rebalanceService, backupProvider, service)
	readOnlyRouter := newContractManagementRouter(readOnlySecret, audithttp.RoleReadOnly, membershipService, drainService, rebalanceService, backupProvider, service)
	conflictDrainRouter := newContractManagementRouter(
		adminSecret, audithttp.RoleClusterAdmin, membershipService,
		contractDrainService{startError: &appmembership.ServiceError{
			Code: appmembership.ServiceErrorInvalidNodeTransition, Message: "node lifecycle transition is not allowed",
		}},
		rebalanceService, backupProvider, service,
	)

	type requestCase struct {
		name, method, path, body, token, idempotencyKey string
		router                                          http.Handler
		status                                          int
		action, result, reason, targetType, targetID    string
		subject, role                                   string
		requestID                                       string
	}
	cases := []requestCase{
		{
			name: "stage member", method: http.MethodPost, path: "/api/v1/cluster/members",
			body: validContractMemberAdmissionJSON, token: adminSecret, idempotencyKey: "stage-member-4",
			router: adminRouter, status: http.StatusAccepted, action: "cluster.member.stage", result: "ALLOWED",
			reason: "succeeded", targetType: "cluster_member", targetID: "4", subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "remove member", method: http.MethodDelete, path: "/api/v1/cluster/members/4",
			token: adminSecret, idempotencyKey: "remove-member-4", router: adminRouter, status: http.StatusAccepted,
			action: "cluster.member.remove", result: "ALLOWED", reason: "succeeded",
			targetType: "cluster_member", targetID: "4", subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "activate feature", method: http.MethodPost, path: "/api/v1/cluster/features/audit-unknown-feature",
			token: adminSecret, router: adminRouter, status: http.StatusConflict,
			action: "cluster.feature.activate", result: "FAILED", reason: "feature_not_supported_by_all_members",
			targetType: "cluster_feature", targetID: "audit-unknown-feature", subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "start drain", method: http.MethodPost, path: "/api/v1/nodes/3/drain",
			body: validContractDrainRequestJSON, token: adminSecret, idempotencyKey: "start-drain-3",
			router: adminRouter, status: http.StatusAccepted, action: "node.drain.start", result: "ALLOWED",
			reason: "succeeded", targetType: "node", targetID: "3", subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "cancel drain", method: http.MethodDelete, path: "/api/v1/nodes/3/drain",
			token: adminSecret, idempotencyKey: "cancel-drain-3", router: adminRouter, status: http.StatusOK,
			action: "node.drain.cancel", result: "ALLOWED", reason: "succeeded",
			targetType: "node", targetID: "3", subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "start rebalance", method: http.MethodPost, path: "/api/v1/cluster/rebalance",
			body: validContractRebalanceRequestJSON, token: adminSecret, idempotencyKey: "start-rebalance",
			router: adminRouter, status: http.StatusAccepted, action: "cluster.rebalance.start", result: "ALLOWED",
			reason: "succeeded", targetType: "cluster", targetID: "contract-cluster", subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "cancel rebalance", method: http.MethodDelete, path: "/api/v1/cluster/rebalance",
			token: adminSecret, idempotencyKey: "cancel-rebalance", router: adminRouter, status: http.StatusOK,
			action: "cluster.rebalance.cancel", result: "ALLOWED", reason: "succeeded",
			targetType: "cluster", targetID: "contract-cluster", subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "create backup", method: http.MethodPost, path: "/api/v1/cluster/backups",
			token: adminSecret, router: adminRouter, status: http.StatusOK, action: "cluster.backup.create",
			result: "ALLOWED", reason: "succeeded", targetType: "cluster", targetID: "contract-cluster",
			subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "invalid credential", method: http.MethodGet, path: "/api/v1/cluster", token: invalidSecret,
			router: adminRouter, status: http.StatusUnauthorized, action: "management.request", result: "DENIED",
			reason: "unauthorized", targetType: "management_endpoint", targetID: "/api/v1/cluster", subject: "unauthenticated",
		},
		{
			name: "read-only mutation", method: http.MethodPost, path: "/api/v1/nodes/3/drain",
			body: validContractDrainRequestJSON, token: readOnlySecret, idempotencyKey: "readonly-drain-3",
			router: readOnlyRouter, status: http.StatusForbidden, action: "management.request", result: "DENIED",
			reason: "forbidden", targetType: "management_endpoint", targetID: "/api/v1/nodes/{nodeId}/drain",
			subject: "management-bearer-token", role: "ReadOnly",
		},
		{
			name: "drain conflict", method: http.MethodPost, path: "/api/v1/nodes/3/drain",
			body: validContractDrainRequestJSON, token: adminSecret, idempotencyKey: "conflicting-drain-3",
			router: conflictDrainRouter, status: http.StatusConflict, action: "node.drain.start", result: "FAILED",
			reason: "invalid_node_transition", targetType: "node", targetID: "3",
			subject: "management-bearer-token", role: "ClusterAdmin",
		},
	}
	for index := range cases {
		test := &cases[index]
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			request.Header.Set("Authorization", "Bearer "+test.token)
			if test.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			if test.idempotencyKey != "" {
				request.Header.Set("Idempotency-Key", test.idempotencyKey)
			}
			response := httptest.NewRecorder()
			test.router.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status=%d body=%q, want %d", response.Code, response.Body.String(), test.status)
			}
			test.requestID = response.Header().Get(audithttp.HeaderRequestID)
			if test.requestID == "" {
				t.Fatal("response has no request ID")
			}
		})
	}

	logPath := filepath.Join(directory, "audit.jsonl")
	storedBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile(audit.jsonl) error = %v", err)
	}
	for _, secret := range []string{adminSecret, readOnlySecret, invalidSecret} {
		if bytes.Contains(storedBytes, []byte(secret)) {
			t.Fatalf("stored audit bytes contain known secret %q", secret)
		}
	}
	lines := bytes.Split(bytes.TrimSuffix(storedBytes, []byte{'\n'}), []byte{'\n'})
	if len(lines) != len(cases) {
		t.Fatalf("audit log has %d records, want %d", len(lines), len(cases))
	}
	type contractEvent struct {
		RequestID string `json:"requestId"`
		Actor     struct {
			Subject string   `json:"subject"`
			Roles   []string `json:"roles"`
		} `json:"actor"`
		Action string `json:"action"`
		Target struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		} `json:"target"`
		Result  string         `json:"result"`
		Reason  string         `json:"reason"`
		Details map[string]any `json:"details"`
	}
	var firstEvent map[string]any
	for index, line := range lines {
		var record struct {
			Event json.RawMessage `json:"event"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode audit record %d: %v", index+1, err)
		}
		var eventValue any
		if err := json.Unmarshal(record.Event, &eventValue); err != nil {
			t.Fatalf("decode event %d: %v", index+1, err)
		}
		if err := schema.Validate(eventValue); err != nil {
			t.Fatalf("schema rejected event %d: %v", index+1, err)
		}
		var event contractEvent
		if err := json.Unmarshal(record.Event, &event); err != nil {
			t.Fatalf("decode event fields %d: %v", index+1, err)
		}
		want := cases[index]
		if event.RequestID != want.requestID || event.Action != want.action || event.Result != want.result || event.Reason != want.reason ||
			event.Target.Type != want.targetType || event.Target.ID != want.targetID ||
			event.Details["httpStatus"] != float64(want.status) || event.Details["method"] != want.method ||
			event.Actor.Subject != want.subject {
			t.Fatalf("event %d = %+v, want request/action/result/reason/target/status/method/subject %+v", index+1, event, want)
		}
		if want.role == "" {
			if len(event.Actor.Roles) != 0 {
				t.Fatalf("event %d roles=%v, want none", index+1, event.Actor.Roles)
			}
		} else if len(event.Actor.Roles) != 1 || event.Actor.Roles[0] != want.role {
			t.Fatalf("event %d roles=%v, want [%s]", index+1, event.Actor.Roles, want.role)
		}
		if index == 0 {
			if err := json.Unmarshal(record.Event, &firstEvent); err != nil {
				t.Fatalf("decode first event object: %v", err)
			}
		}
	}
	firstEvent["unknownOptionalProperty"] = "not-in-contract"
	if err := schema.Validate(firstEvent); err == nil {
		t.Fatal("schema accepted an unknown optional event property")
	}

	verification, err := auditlog.VerifyDirectory(directory)
	if err != nil {
		t.Fatalf("VerifyDirectory() error = %v", err)
	}
	if verification.Records != uint64(len(cases)) || verification.FirstSeq != 1 || verification.LastSeq != uint64(len(cases)) {
		t.Fatalf("unexpected verified chain summary: %+v", verification)
	}
}

const (
	validContractMemberAdmissionJSON  = `{"memberId":4,"address":"node4.example:19004","dnsSan":"node4.example","certificateSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","releaseVersion":"1.0.0","clusterProtocolMajor":2,"clusterProtocolMinor":0,"stateSchemaMajor":1,"stateSchemaMinor":0}`
	validContractDrainRequestJSON     = `{"connectionRatePerSecond":2,"loadBalancerPropagationDelayMs":1500,"timeoutMs":60000}`
	validContractRebalanceRequestJSON = `{"maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMs":500,"timeoutMs":120000}`
)

type contractBackupProvider struct{}

func (contractBackupProvider) CaptureClusterBackup(context.Context) (portsmanagement.ClusterBackupDTO, error) {
	archive := []byte("contract backup")
	checksum := sha256.Sum256(archive)
	return portsmanagement.ClusterBackupDTO{CommitIndex: 1, ArchiveSHA256: checksum, Archive: archive}, nil
}

type contractMembershipService struct {
	featureError error
}

func (service contractMembershipService) StageJoin(_ context.Context, request appmembership.StageJoinRequestDTO) (appmembership.MembershipOperationDTO, error) {
	return appmembership.MembershipOperationDTO{
		OperationID:    "00000000-0000-4000-8000-000000000011",
		Action:         appmembership.MembershipActionStageJoin,
		MemberID:       request.Member.MemberID,
		Phase:          appmembership.OperationPhaseStaged,
		CommittedIndex: 11,
	}, nil
}

func (contractMembershipService) StartPlannedRemoval(_ context.Context, memberID uint64, _ string, _ uint64) (appmembership.MembershipOperationDTO, error) {
	return appmembership.MembershipOperationDTO{
		OperationID:    "00000000-0000-4000-8000-000000000012",
		Action:         appmembership.MembershipActionBeginRemove,
		MemberID:       memberID,
		Phase:          appmembership.OperationPhaseRemoving,
		CommittedIndex: 12,
		Record:         appmembership.MembershipRecordDTO{TargetMemberID: memberID},
	}, nil
}

func (contractMembershipService) GetOperation(context.Context, string) (appmembership.MembershipOperationDTO, error) {
	return appmembership.MembershipOperationDTO{}, nil
}

func (contractMembershipService) ClusterFeatures(context.Context) (appmembership.FeatureStateDTO, error) {
	return appmembership.FeatureStateDTO{}, nil
}

func (service contractMembershipService) ActivateFeature(context.Context, string) (appmembership.FeatureStateDTO, error) {
	return appmembership.FeatureStateDTO{}, service.featureError
}

type contractDrainService struct {
	startError error
}

func (service contractDrainService) StartNodeDrain(_ context.Context, request appmembership.StartNodeDrainRequestDTO) (appmembership.NodeDrainOperationDTO, error) {
	if service.startError != nil {
		return appmembership.NodeDrainOperationDTO{}, service.startError
	}
	return appmembership.NodeDrainOperationDTO{State: appmembership.NodeDrainStateDTO{
		DrainOperationID: "00000000-0000-4000-8000-000000000013",
		MemberID:         request.MemberID,
		Phase:            nodedrain.PhaseExcludingTraffic,
	}}, nil
}

func (contractDrainService) GetNodeDrain(_ context.Context, memberID uint64) (appmembership.NodeDrainOperationDTO, error) {
	return appmembership.NodeDrainOperationDTO{State: appmembership.NodeDrainStateDTO{
		DrainOperationID: "00000000-0000-4000-8000-000000000013",
		MemberID:         memberID,
		Phase:            nodedrain.PhaseExcludingTraffic,
	}}, nil
}

func (contractDrainService) CancelNodeDrain(_ context.Context, memberID uint64) (appmembership.NodeDrainOperationDTO, error) {
	return appmembership.NodeDrainOperationDTO{State: appmembership.NodeDrainStateDTO{
		DrainOperationID: "00000000-0000-4000-8000-000000000013",
		MemberID:         memberID,
		Phase:            nodedrain.PhaseCancelled,
	}}, nil
}

type contractRebalanceService struct{}

func (contractRebalanceService) StartConnectionRebalance(context.Context, appmembership.StartConnectionRebalanceRequestDTO) (appmembership.ConnectionRebalanceOperationDTO, error) {
	return contractRebalanceOperation(), nil
}

func (contractRebalanceService) GetConnectionRebalance(context.Context) (appmembership.ConnectionRebalanceOperationDTO, error) {
	return contractRebalanceOperation(), nil
}

func (contractRebalanceService) CancelConnectionRebalance(context.Context) (appmembership.ConnectionRebalanceOperationDTO, error) {
	return contractRebalanceOperation(), nil
}

func contractRebalanceOperation() appmembership.ConnectionRebalanceOperationDTO {
	return appmembership.ConnectionRebalanceOperationDTO{State: appmembership.ConnectionRebalanceStateDTO{
		RebalanceOperationID: "00000000-0000-4000-8000-000000000014",
		Phase:                connrebalance.PhaseRunning,
	}}
}

func newContractManagementRouter(
	token string,
	role audithttp.Role,
	membershipService audithttp.MembershipUseCase,
	drainService audithttp.NodeDrainUseCase,
	rebalanceService audithttp.ConnectionRebalanceUseCase,
	backupProvider portsmanagement.ClusterBackupProvider,
	auditService audithttp.AuditUseCase,
) http.Handler {
	return audithttp.NewRouterWithMembership(
		audithttp.ServerConfig{ClusterID: "contract-cluster", AuthToken: token, AuthTokenRole: role},
		nil, nil, nil, membershipService, drainService, rebalanceService, backupProvider, auditService,
	)
}

func compileAuditEventSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() could not locate contract test")
	}
	schemaPath := filepath.Join(filepath.Dir(source), "..", "..", "..", "docs", "contracts", "audit-event-v1.schema.json")
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", schemaPath, err)
	}
	var schemaDocument any
	if err := json.Unmarshal(schemaBytes, &schemaDocument); err != nil {
		t.Fatalf("decode audit schema: %v", err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	const schemaLocation = "urn:narumq:contract:audit-event:v1"
	if err := compiler.AddResource(schemaLocation, schemaDocument); err != nil {
		t.Fatalf("AddResource(audit schema) error = %v", err)
	}
	compiled, err := compiler.Compile(schemaLocation)
	if err != nil {
		t.Fatalf("compile audit schema: %v", err)
	}
	return compiled
}
