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
	portsaudit "narumq/internal/application/ports/audit"
	portsclock "narumq/internal/application/ports/clock"
	portsmanagement "narumq/internal/application/ports/management"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/connrebalance"
	"narumq/internal/domain/nodedrain"
	"narumq/internal/domain/security"
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
// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
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
	adminRouter := newContractManagementRouter(
		audithttp.ServerConfig{ClusterID: "contract-cluster", AuthToken: adminSecret},
		membershipService, drainService, rebalanceService, backupProvider, service, nil,
	)
	readOnlyRouter := newContractManagementRouter(
		audithttp.ServerConfig{ClusterID: "contract-cluster", APIKeys: []audithttp.APIKeyCredential{{
			ID: "dashboard", Key: readOnlySecret, Roles: []audithttp.Role{audithttp.RoleReadOnly},
		}}},
		membershipService, drainService, rebalanceService, backupProvider, service, nil,
	)
	conflictDrainRouter := newContractManagementRouter(
		audithttp.ServerConfig{ClusterID: "contract-cluster", AuthToken: adminSecret},
		membershipService,
		contractDrainService{startError: &appmembership.ServiceError{
			Code: appmembership.ServiceErrorInvalidNodeTransition, Message: "node lifecycle transition is not allowed",
		}},
		rebalanceService, backupProvider, service, nil,
	)

	type requestCase struct {
		name, method, path, body, token, idempotencyKey string
		router                                          http.Handler
		status                                          int
		action, result, reason, targetType, targetID    string
		subject, role                                   string
		requestID                                       string
		apiKey                                          bool
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
			name: "revoke credential", method: http.MethodPost, path: "/api/v1/security/credential-revocations/credential-a",
			token: adminSecret, router: adminRouter, status: http.StatusOK,
			action: "security.credential.revoke", result: "ALLOWED", reason: "succeeded",
			targetType: "mqtt_credential", targetID: "credential-a", subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "restore credential", method: http.MethodDelete, path: "/api/v1/security/credential-revocations/credential-a",
			token: adminSecret, idempotencyKey: "restore-credential-a", router: adminRouter, status: http.StatusOK,
			action: "security.credential.restore", result: "ALLOWED", reason: "succeeded",
			targetType: "mqtt_credential", targetID: "credential-a", subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "block client", method: http.MethodPost, path: "/api/v1/security/blocklist/client-ids/t/c",
			token: adminSecret, router: adminRouter, status: http.StatusOK,
			action: "security.blocklist.add", result: "ALLOWED", reason: "succeeded",
			targetType: "mqtt_blocklist_entry", targetID: "clientId:t/c", subject: "management-bearer-token", role: "ClusterAdmin",
		},
		{
			name: "unblock client", method: http.MethodDelete, path: "/api/v1/security/blocklist/client-ids/t/c",
			token: adminSecret, router: adminRouter, status: http.StatusOK,
			action: "security.blocklist.remove", result: "ALLOWED", reason: "succeeded",
			targetType: "mqtt_blocklist_entry", targetID: "clientId:t/c", subject: "management-bearer-token", role: "ClusterAdmin",
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
			subject: "apikey:dashboard", role: "ReadOnly", apiKey: true,
		},
		{
			name: "read-only credential mutation", method: http.MethodPost, path: "/api/v1/security/credential-revocations/credential-a",
			token: readOnlySecret, router: readOnlyRouter, status: http.StatusForbidden, action: "management.request", result: "DENIED",
			reason: "forbidden", targetType: "management_endpoint", targetID: "/api/v1/security/credential-revocations/{credentialId}",
			subject: "apikey:dashboard", role: "ReadOnly", apiKey: true,
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
			if test.apiKey {
				request.Header.Set("X-API-Key", test.token)
			} else {
				request.Header.Set("Authorization", "Bearer "+test.token)
			}
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

// Traceability: SEC-10; docs/specs/tenancy-and-security.md §6; docs/specs/management-api-and-observability.md §1; ADR-0048 §7.
func TestAuditQueryReturnsStoredChainRecords(t *testing.T) {
	directory := t.TempDir()
	publisher, err := auditlog.OpenFileAuditPublisher(auditlog.FileAuditPublisherConfig{
		Directory: directory, MaxFileBytes: auditlog.DefaultMaxFileBytes,
	})
	if err != nil {
		t.Fatalf("OpenFileAuditPublisher() error = %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()
	auditService, err := auditapp.NewService(auditapp.ServiceConfig{
		ClusterID: "contract-cluster", NodeID: 1, ReleaseVersion: "1.0.0", DenialEventsPerSecond: 20,
	}, publisher, contractClock{now: time.Date(2026, time.October, 8, 12, 30, 0, 0, time.UTC)}, contractMetrics{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	queryService, err := auditapp.NewQueryService(publisher)
	if err != nil {
		t.Fatalf("NewQueryService() error = %v", err)
	}

	const (
		readOnlySecret  = "known-audit-query-readonly-secret"
		auditReadSecret = "known-audit-query-security-secret"
	)
	router := newContractManagementRouter(
		audithttp.ServerConfig{
			ClusterID: "contract-cluster",
			APIKeys: []audithttp.APIKeyCredential{
				{ID: "dashboard", Key: readOnlySecret, Roles: []audithttp.Role{audithttp.RoleReadOnly}},
				{ID: "audit-reader", Key: auditReadSecret, Roles: []audithttp.Role{audithttp.RoleSecurityAdmin}},
			},
		},
		contractMembershipService{}, contractDrainService{}, contractRebalanceService{},
		contractBackupProvider{}, auditService, queryService,
	)
	deniedMutation := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/members", strings.NewReader(validContractMemberAdmissionJSON))
	deniedMutation.Header.Set("X-API-Key", readOnlySecret)
	deniedMutation.Header.Set("Content-Type", "application/json")
	deniedResponse := httptest.NewRecorder()
	router.ServeHTTP(deniedResponse, deniedMutation)
	if deniedResponse.Code != http.StatusForbidden {
		t.Fatalf("ReadOnly mutation status=%d body=%q, want %d", deniedResponse.Code, deniedResponse.Body.String(), http.StatusForbidden)
	}

	storedBytes, err := os.ReadFile(filepath.Join(directory, "audit.jsonl"))
	if err != nil {
		t.Fatalf("ReadFile(audit.jsonl) error = %v", err)
	}
	var storedRecord struct {
		Seq      uint64          `json:"seq"`
		PrevHash string          `json:"prevHash"`
		Hash     string          `json:"hash"`
		Event    json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(storedBytes), &storedRecord); err != nil {
		t.Fatalf("decode stored audit record: %v", err)
	}

	queryRequest := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/nodes/1/audit/events?actorSubject=apikey%3Adashboard&action=management.request&result=DENIED&limit=1",
		nil,
	)
	queryRequest.Header.Set("X-API-Key", auditReadSecret)
	queryResponse := httptest.NewRecorder()
	router.ServeHTTP(queryResponse, queryRequest)
	if queryResponse.Code != http.StatusOK {
		t.Fatalf("audit query status=%d body=%q, want %d", queryResponse.Code, queryResponse.Body.String(), http.StatusOK)
	}
	var queryResult struct {
		Data []struct {
			Seq      uint64          `json:"seq"`
			PrevHash string          `json:"prevHash"`
			Hash     string          `json:"hash"`
			Event    json.RawMessage `json:"event"`
		} `json:"data"`
		Page struct {
			NextCursor *string `json:"nextCursor"`
			HasMore    bool    `json:"hasMore"`
		} `json:"page"`
	}
	if err := json.Unmarshal(queryResponse.Body.Bytes(), &queryResult); err != nil {
		t.Fatalf("decode audit query response: %v", err)
	}
	if len(queryResult.Data) != 1 || queryResult.Page.HasMore || queryResult.Page.NextCursor != nil {
		t.Fatalf("audit query result=%+v, want one final stored record", queryResult)
	}
	gotRecord := queryResult.Data[0]
	if gotRecord.Seq != storedRecord.Seq || gotRecord.PrevHash != storedRecord.PrevHash ||
		gotRecord.Hash != storedRecord.Hash || !bytes.Equal(gotRecord.Event, storedRecord.Event) {
		t.Fatalf("audit query record=%+v, want exact stored chain record %+v", gotRecord, storedRecord)
	}
	var storedEvent struct {
		Actor struct {
			Subject string   `json:"subject"`
			Roles   []string `json:"roles"`
		} `json:"actor"`
		Action string `json:"action"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(storedRecord.Event, &storedEvent); err != nil {
		t.Fatalf("decode stored audit event: %v", err)
	}
	if storedEvent.Actor.Subject != "apikey:dashboard" || len(storedEvent.Actor.Roles) != 1 ||
		storedEvent.Actor.Roles[0] != "ReadOnly" || storedEvent.Action != "management.request" || storedEvent.Result != "DENIED" {
		t.Fatalf("stored actor/event=%+v, want the denied ReadOnly API key request", storedEvent)
	}
	verification, err := auditlog.VerifyDirectory(directory)
	if err != nil {
		t.Fatalf("VerifyDirectory() error = %v", err)
	}
	if verification.Records != 1 || verification.FirstSeq != storedRecord.Seq || verification.LastSeq != storedRecord.Seq {
		t.Fatalf("query changed stored audit chain: %+v, record=%+v", verification, storedRecord)
	}
}

// Traceability: SEC-11/NFR-17; docs/contracts/audit-event-v1.md §4; docs/specs/tenancy-and-security.md §8.
func TestAuditConsumersPreserveUnknownActionAndReason(t *testing.T) {
	schema := compileAuditEventSchema(t)
	directory := t.TempDir()
	publisher, err := auditlog.OpenFileAuditPublisher(auditlog.FileAuditPublisherConfig{
		Directory: directory, MaxFileBytes: auditlog.DefaultMaxFileBytes,
	})
	if err != nil {
		t.Fatalf("OpenFileAuditPublisher() error = %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	occurredAt := time.Date(2026, time.October, 11, 9, 0, 0, 0, time.UTC)
	events := []portsaudit.AuditEventDTO{
		{
			SchemaVersion: "1.0",
			EventID:       "evt-known",
			OccurredAt:    occurredAt,
			RequestID:     "req-known",
			Producer:      portsaudit.AuditProducerDTO{Component: "BROKER", InstanceID: "contract-cluster/1", Version: "1.0.0"},
			Actor:         portsaudit.AuditActorDTO{Subject: "contract-actor"},
			Action:        "node.drain.start",
			Target:        portsaudit.AuditTargetDTO{Type: "node", ID: "1"},
			Result:        portsaudit.AuditResultAllowed,
			Reason:        "succeeded",
			Details:       map[string]any{"httpStatus": 202, "method": "POST"},
		},
		{
			SchemaVersion: "1.0",
			EventID:       "evt-unknown-rollout",
			OccurredAt:    occurredAt,
			RequestID:     "req-unknown-rollout",
			Producer:      portsaudit.AuditProducerDTO{Component: "ROLLOUT_CONTROLLER", InstanceID: "contract-rollout/1", Version: "1.0.0"},
			Actor:         portsaudit.AuditActorDTO{Subject: "contract-actor"},
			Action:        "rollout.pod.delete_approved",
			Target:        portsaudit.AuditTargetDTO{Type: "rollout_pod", ID: "narumq-2"},
			Result:        portsaudit.AuditResultDenied,
			Reason:        "unknown_barrier_state",
			Details:       map[string]any{"barrier": "unknown"},
		},
		{
			SchemaVersion: "1.0",
			EventID:       "evt-unknown-future",
			OccurredAt:    occurredAt,
			RequestID:     "req-unknown-future",
			Producer:      portsaudit.AuditProducerDTO{Component: "BROKER", InstanceID: "contract-cluster/1", Version: "1.0.0"},
			Actor:         portsaudit.AuditActorDTO{Subject: "contract-actor"},
			Action:        "future.feature.activate_v2",
			Target:        portsaudit.AuditTargetDTO{Type: "future_resource", ID: "f-1"},
			Result:        portsaudit.AuditResultFailed,
			Reason:        "future_reason_code",
		},
	}
	for _, event := range events {
		if err := publisher.Publish(event); err != nil {
			t.Fatalf("Publish(%s) error = %v", event.EventID, err)
		}
	}

	storedBytes, err := os.ReadFile(filepath.Join(directory, "audit.jsonl"))
	if err != nil {
		t.Fatalf("ReadFile(audit.jsonl) error = %v", err)
	}
	lines := bytes.Split(bytes.TrimSpace(storedBytes), []byte("\n"))
	if len(lines) != len(events) {
		t.Fatalf("stored audit records=%d, want %d", len(lines), len(events))
	}
	storedEvents := make([]json.RawMessage, len(lines))
	for index, line := range lines {
		var record struct {
			Event json.RawMessage `json:"event"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode stored audit record %d: %v", index+1, err)
		}
		var eventValue any
		if err := json.Unmarshal(record.Event, &eventValue); err != nil {
			t.Fatalf("decode stored audit event %d: %v", index+1, err)
		}
		if err := schema.Validate(eventValue); err != nil {
			t.Fatalf("schema rejected stored audit event %d: %v", index+1, err)
		}
		var eventFields struct {
			Action   string `json:"action"`
			Reason   string `json:"reason"`
			Producer struct {
				Component string `json:"component"`
			} `json:"producer"`
		}
		if err := json.Unmarshal(record.Event, &eventFields); err != nil {
			t.Fatalf("decode stored audit event fields %d: %v", index+1, err)
		}
		want := events[index]
		if eventFields.Action != want.Action || eventFields.Reason != want.Reason || eventFields.Producer.Component != want.Producer.Component {
			t.Fatalf("stored audit event %d action/reason/component=%q/%q/%q, want %q/%q/%q",
				index+1, eventFields.Action, eventFields.Reason, eventFields.Producer.Component,
				want.Action, want.Reason, want.Producer.Component)
		}
		storedEvents[index] = append(json.RawMessage(nil), record.Event...)
	}
	verification, err := auditlog.VerifyDirectory(directory)
	if err != nil {
		t.Fatalf("VerifyDirectory() error = %v", err)
	}
	if verification.Records != 3 || verification.FirstSeq != 1 || verification.LastSeq != 3 {
		t.Fatalf("verified chain summary=%+v, want records=3 first=1 last=3", verification)
	}

	auditService, err := auditapp.NewService(auditapp.ServiceConfig{
		ClusterID: "contract-cluster", NodeID: 1, ReleaseVersion: "1.0.0", DenialEventsPerSecond: 20,
	}, publisher, contractClock{now: occurredAt}, contractMetrics{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	queryService, err := auditapp.NewQueryService(publisher)
	if err != nil {
		t.Fatalf("NewQueryService() error = %v", err)
	}
	const auditSecret = "known-audit-unknown-values-secret"
	router := newContractManagementRouter(
		audithttp.ServerConfig{
			ClusterID: "contract-cluster",
			APIKeys: []audithttp.APIKeyCredential{
				{ID: "audit-reader", Key: auditSecret, Roles: []audithttp.Role{audithttp.RoleSecurityAdmin}},
			},
		},
		contractMembershipService{}, contractDrainService{}, contractRebalanceService{},
		contractBackupProvider{}, auditService, queryService,
	)

	queryRequest := httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/audit/events?limit=10", nil)
	queryRequest.Header.Set("X-API-Key", auditSecret)
	queryResponse := httptest.NewRecorder()
	router.ServeHTTP(queryResponse, queryRequest)
	if queryResponse.Code != http.StatusOK {
		t.Fatalf("audit query status=%d body=%q, want %d", queryResponse.Code, queryResponse.Body.String(), http.StatusOK)
	}
	var queryResult struct {
		Data []struct {
			Seq      uint64          `json:"seq"`
			PrevHash string          `json:"prevHash"`
			Hash     string          `json:"hash"`
			Event    json.RawMessage `json:"event"`
		} `json:"data"`
	}
	if err := json.Unmarshal(queryResponse.Body.Bytes(), &queryResult); err != nil {
		t.Fatalf("decode audit query response: %v", err)
	}
	if len(queryResult.Data) != 3 {
		t.Fatalf("audit query records=%d, want 3", len(queryResult.Data))
	}
	for index, record := range queryResult.Data {
		storedIndex := len(storedEvents) - 1 - index
		if record.Seq != uint64(len(storedEvents)-index) || !bytes.Equal(record.Event, storedEvents[storedIndex]) {
			t.Fatalf("audit query record %d seq=%d event=%s, want seq=%d exact stored event %s",
				index, record.Seq, record.Event, len(storedEvents)-index, storedEvents[storedIndex])
		}
	}

	rolloutRequest := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/nodes/1/audit/events?action=rollout.pod.delete_approved",
		nil,
	)
	rolloutRequest.Header.Set("X-API-Key", auditSecret)
	rolloutResponse := httptest.NewRecorder()
	router.ServeHTTP(rolloutResponse, rolloutRequest)
	if rolloutResponse.Code != http.StatusOK {
		t.Fatalf("rollout audit query status=%d body=%q, want %d", rolloutResponse.Code, rolloutResponse.Body.String(), http.StatusOK)
	}
	var rolloutResult struct {
		Data []struct {
			Event json.RawMessage `json:"event"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rolloutResponse.Body.Bytes(), &rolloutResult); err != nil {
		t.Fatalf("decode rollout audit query response: %v", err)
	}
	if len(rolloutResult.Data) != 1 {
		t.Fatalf("rollout audit query records=%d, want 1", len(rolloutResult.Data))
	}
	var rolloutEvent struct {
		Reason   string `json:"reason"`
		Producer struct {
			Component string `json:"component"`
		} `json:"producer"`
	}
	if err := json.Unmarshal(rolloutResult.Data[0].Event, &rolloutEvent); err != nil {
		t.Fatalf("decode rollout audit event: %v", err)
	}
	if rolloutEvent.Reason != "unknown_barrier_state" || rolloutEvent.Producer.Component != "ROLLOUT_CONTROLLER" {
		t.Fatalf("rollout audit event reason/component=%q/%q, want %q/%q",
			rolloutEvent.Reason, rolloutEvent.Producer.Component, "unknown_barrier_state", "ROLLOUT_CONTROLLER")
	}

	futureRequest := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/nodes/1/audit/events?action=future.feature.activate_v2",
		nil,
	)
	futureRequest.Header.Set("X-API-Key", auditSecret)
	futureResponse := httptest.NewRecorder()
	router.ServeHTTP(futureResponse, futureRequest)
	if futureResponse.Code != http.StatusOK {
		t.Fatalf("future audit query status=%d body=%q, want %d", futureResponse.Code, futureResponse.Body.String(), http.StatusOK)
	}
	var futureResult struct {
		Data []struct {
			Event json.RawMessage `json:"event"`
		} `json:"data"`
	}
	if err := json.Unmarshal(futureResponse.Body.Bytes(), &futureResult); err != nil {
		t.Fatalf("decode future audit query response: %v", err)
	}
	if len(futureResult.Data) != 1 {
		t.Fatalf("future audit query records=%d, want 1", len(futureResult.Data))
	}
	var futureEvent struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(futureResult.Data[0].Event, &futureEvent); err != nil {
		t.Fatalf("decode future audit event: %v", err)
	}
	if futureEvent.Reason != "future_reason_code" {
		t.Fatalf("future audit event reason=%q, want %q", futureEvent.Reason, "future_reason_code")
	}

	verification, err = auditlog.VerifyDirectory(directory)
	if err != nil {
		t.Fatalf("VerifyDirectory() after query error = %v", err)
	}
	if verification.Records != 3 {
		t.Fatalf("query changed stored audit chain: records=%d, want 3", verification.Records)
	}
}

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
func TestDataPlaneDenialEventsMatchSchema(t *testing.T) {
	schema := compileAuditEventSchema(t)
	directory := t.TempDir()
	publisher, err := auditlog.OpenFileAuditPublisher(auditlog.FileAuditPublisherConfig{
		Directory: directory, MaxFileBytes: auditlog.DefaultMaxFileBytes,
	})
	if err != nil {
		t.Fatalf("OpenFileAuditPublisher() error = %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()
	clock := contractClock{now: time.Date(2026, time.October, 9, 12, 30, 0, 0, time.UTC)}
	recorder, err := auditapp.NewDataPlaneRecorder(auditapp.DataPlaneRecorderConfig{
		ClusterID: "contract-cluster", NodeID: 2, ReleaseVersion: "1.0.0",
		EventsPerSecond: 20, QueueCapacity: 8,
	}, publisher, clock, contractMetrics{})
	if err != nil {
		t.Fatalf("NewDataPlaneRecorder() error = %v", err)
	}
	defer recorder.Close()
	recorder.Start()
	denials := []portsaudit.DataPlaneDenialDTO{
		{
			Action: portsaudit.DataPlaneActionConnect, Reason: "invalid_credentials",
			ClientID: "rejected-client", SourceIP: "203.0.113.10", AuthMethod: "jwt",
		},
		{
			Action: portsaudit.DataPlaneActionPublish, Reason: portsaudit.DataPlaneReasonNotAuthorized,
			TenantID: "tenant-a", Subject: "device-a", CredentialID: "credential-a",
			Roles: []string{"publisher"}, ClientID: "client-a", SourceIP: "203.0.113.10",
			Topic: strings.Repeat("界", 600),
		},
		{
			Action: portsaudit.DataPlaneActionSubscribe, Reason: portsaudit.DataPlaneReasonNotAuthorized,
			TenantID: "tenant-a", Subject: "device-a", CredentialID: "credential-a",
			Roles: []string{"publisher"}, ClientID: "client-a", SourceIP: "203.0.113.10", Topic: "sensors/#",
		},
		{
			Action: portsaudit.DataPlaneActionUnsubscribe, Reason: portsaudit.DataPlaneReasonNotAuthorized,
			TenantID: "tenant-a", Subject: "device-a", CredentialID: "credential-a",
			Roles: []string{"publisher"}, ClientID: "client-a", SourceIP: "203.0.113.10", Topic: "sensors/#",
		},
		{
			Action: portsaudit.DataPlaneActionWill, Reason: portsaudit.DataPlaneReasonNotAuthorized,
			TenantID: "tenant-a", Subject: "device-a", CredentialID: "credential-a",
			Roles: []string{"publisher"}, ClientID: "client-a", Topic: "sensors/will",
		},
	}
	for _, denial := range denials {
		recorder.RecordDataPlaneDenial(denial)
	}
	recorder.Close()

	rawLog, err := os.ReadFile(filepath.Join(directory, "audit.jsonl"))
	if err != nil {
		t.Fatalf("ReadFile(audit.jsonl) error = %v", err)
	}
	lines := bytes.Split(bytes.TrimSuffix(rawLog, []byte{'\n'}), []byte{'\n'})
	if len(lines) != len(denials) {
		t.Fatalf("audit log has %d events, want %d", len(lines), len(denials))
	}
	wantActions := []string{"mqtt.connect", "mqtt.publish", "mqtt.subscribe", "mqtt.unsubscribe", "mqtt.will"}
	wantTargets := []string{"mqtt_client", "mqtt_topic", "mqtt_topic_filter", "mqtt_topic_filter", "mqtt_topic"}
	for index, line := range lines {
		var record struct {
			Event json.RawMessage `json:"event"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode record %d: %v", index+1, err)
		}
		var eventValue any
		if err := json.Unmarshal(record.Event, &eventValue); err != nil {
			t.Fatalf("decode event %d: %v", index+1, err)
		}
		if err := schema.Validate(eventValue); err != nil {
			t.Fatalf("schema rejected data-plane event %d: %v", index+1, err)
		}
		var event struct {
			Actor struct {
				Subject string `json:"subject"`
			} `json:"actor"`
			Action   string                    `json:"action"`
			Target   struct{ Type, ID string } `json:"target"`
			Result   string                    `json:"result"`
			Reason   string                    `json:"reason"`
			SourceIP string                    `json:"sourceIp"`
			Details  map[string]any            `json:"details"`
		}
		if err := json.Unmarshal(record.Event, &event); err != nil {
			t.Fatalf("decode data-plane event fields %d: %v", index+1, err)
		}
		if event.Action != wantActions[index] || event.Target.Type != wantTargets[index] ||
			event.Result != "DENIED" || event.Reason != denials[index].Reason || event.SourceIP != denials[index].SourceIP {
			t.Fatalf("data-plane event %d = %+v", index+1, event)
		}
		if index == 0 && (event.Actor.Subject != "unauthenticated" || event.Details["authMethod"] != "jwt") {
			t.Fatalf("pre-authentication event = %+v", event)
		}
		if index == 1 && (len([]rune(event.Target.ID)) != 512 || event.Details["truncated"] != true) {
			t.Fatalf("truncated publish event target/details = %d runes/%v", len([]rune(event.Target.ID)), event.Details)
		}
	}
	verification, err := auditlog.VerifyDirectory(directory)
	if err != nil {
		t.Fatalf("VerifyDirectory() error = %v", err)
	}
	if verification.Records != uint64(len(denials)) || verification.FirstSeq != 1 || verification.LastSeq != uint64(len(denials)) {
		t.Fatalf("unexpected verified data-plane chain summary: %+v", verification)
	}
}

const (
	validContractMemberAdmissionJSON  = `{"memberId":4,"address":"node4.example:19004","dnsSan":"node4.example","certificateSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","releaseVersion":"1.0.0","clusterProtocolMajor":2,"clusterProtocolMinor":0,"stateSchemaMajor":1,"stateSchemaMinor":0}`
	validContractDrainRequestJSON     = `{"connectionRatePerSecond":2,"loadBalancerPropagationDelayMillis":1500,"timeoutMillis":60000}`
	validContractRebalanceRequestJSON = `{"maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMillis":500,"timeoutMillis":120000}`
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

type contractCredentialRevocationService struct{}

func (contractCredentialRevocationService) ListCredentialRevocations(context.Context) ([]appmembership.CredentialRevocationStateDTO, error) {
	return []appmembership.CredentialRevocationStateDTO{{
		CredentialID: "credential-a", RevokedAtUnixMillis: 1_760_000_000_000,
	}}, nil
}

func (contractCredentialRevocationService) RevokeCredential(_ context.Context, credentialID string) (appmembership.CredentialRevocationStatusDTO, error) {
	return appmembership.CredentialRevocationStatusDTO{
		CredentialID: credentialID, Revoked: true, RevokedAtUnixMillis: 1_760_000_000_000,
	}, nil
}

func (contractCredentialRevocationService) RestoreCredential(_ context.Context, credentialID string) (appmembership.CredentialRevocationStatusDTO, error) {
	return appmembership.CredentialRevocationStatusDTO{CredentialID: credentialID}, nil
}

type contractBlocklistService struct{}

func (contractBlocklistService) ListBlocklist(context.Context) (appmembership.BlocklistDTO, error) {
	entry := security.BlockEntry{Kind: security.BlockKindUsername, Value: "contract-user"}
	return appmembership.BlocklistDTO{
		PolicyVersion: 1,
		Entries:       []appmembership.BlocklistStateDTO{{Entry: entry, BlockedAtUnixMillis: 1_760_000_000_000}},
	}, nil
}

func (contractBlocklistService) Block(_ context.Context, entry security.BlockEntry) (appmembership.BlocklistStatusDTO, error) {
	return appmembership.BlocklistStatusDTO{
		Entry: entry, Blocked: true, BlockedAtUnixMillis: 1_760_000_000_000, PolicyVersion: 2,
	}, nil
}

func (contractBlocklistService) Unblock(_ context.Context, entry security.BlockEntry) (appmembership.BlocklistStatusDTO, error) {
	return appmembership.BlocklistStatusDTO{Entry: entry, PolicyVersion: 3}, nil
}

func newContractManagementRouter(
	config audithttp.ServerConfig,
	membershipService audithttp.MembershipUseCase,
	drainService audithttp.NodeDrainUseCase,
	rebalanceService audithttp.ConnectionRebalanceUseCase,
	backupProvider portsmanagement.ClusterBackupProvider,
	auditService audithttp.AuditUseCase,
	auditQueryService audithttp.AuditQueryUseCase,
) http.Handler {
	config.LocalNodeID = 1
	return audithttp.NewRouterWithMembership(
		config,
		nil, nil, nil,
		membershipService, drainService, rebalanceService, backupProvider, auditService, auditQueryService,
		contractCredentialRevocationService{},
		contractBlocklistService{},
		nil,
		nil,
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
