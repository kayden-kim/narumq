package managementv1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"

	managementhttp "narumq/internal/adapters/http/management"

	adaptersmetrics "narumq/internal/adapters/metrics"
	"narumq/internal/application/audit"
	appmembership "narumq/internal/application/membership"
	portsaudit "narumq/internal/application/ports/audit"
	"narumq/internal/domain/connrebalance"
	"narumq/internal/domain/nodedrain"
	"narumq/internal/domain/security"

	ports "narumq/internal/application/ports/management"
)

const contractServerURL = "https://narumq.example.invalid"

type operationFixture struct {
	method         string
	path           string
	operationID    string
	target         string
	accept         string
	body           string
	idempotencyKey string
	expectedStatus int
}

func managementOperationFixtures() []operationFixture {
	return []operationFixture{
		{method: http.MethodGet, path: "/health/live", operationID: "getLiveness", target: "/health/live"},
		{method: http.MethodGet, path: "/health/ready", operationID: "getReadiness", target: "/health/ready"},
		{method: http.MethodGet, path: "/health/startup", operationID: "getStartup", target: "/health/startup"},
		{method: http.MethodGet, path: "/health/drain", operationID: "getDrainCompletion", target: "/health/drain"},
		{method: http.MethodGet, path: "/metrics", operationID: "getMetrics", target: "/metrics"},
		{method: http.MethodGet, path: "/api/v1/cluster", operationID: "getCluster", target: "/api/v1/cluster"},
		{method: http.MethodGet, path: "/api/v1/nodes", operationID: "listNodes", target: "/api/v1/nodes?limit=50"},
		{method: http.MethodGet, path: "/api/v1/nodes/{nodeId}", operationID: "getNode", target: "/api/v1/nodes/node-1"},
		{method: http.MethodGet, path: "/api/v1/clients", operationID: "listClients", target: "/api/v1/clients?tenantId=default&clientIdPrefix=device-&nodeId=node-1&protocol=MQTT_5_0&limit=50"},
		{method: http.MethodGet, path: "/api/v1/clients/{tenantId}/{clientId}", operationID: "getClientSession", target: "/api/v1/clients/default/device-1"},
		{method: http.MethodGet, path: "/api/v1/subscriptions", operationID: "listSubscriptions", target: "/api/v1/subscriptions?tenantId=default&clientId=device-1&limit=50"},
		{method: http.MethodGet, path: "/api/v1/retained", operationID: "listRetainedMessages", target: "/api/v1/retained?tenantId=default&topicPrefix=sensors/&limit=50"},
		{method: http.MethodGet, path: "/api/v1/tenants", operationID: "listTenants", target: "/api/v1/tenants?limit=50"},
	}
}
func auditQueryOperationFixture() operationFixture {
	return operationFixture{
		method: http.MethodGet, path: "/api/v1/audit/events", operationID: "listAuditEvents",
		target: "/api/v1/audit/events?limit=2&actorSubject=management-bearer-token&action=cluster.member.stage&targetType=cluster_member&targetId=4&requestId=contract-audit-request&result=ALLOWED",
	}
}

func membershipOperationFixtures() []operationFixture {
	return []operationFixture{
		{
			method: http.MethodPost, path: "/api/v1/cluster/members", operationID: "stageClusterMember",
			target: "/api/v1/cluster/members", body: `{"memberId":4,"address":"node4.example:19004","dnsSan":"node4.example","certificateSha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","releaseVersion":"1.0.0","clusterProtocolMajor":2,"clusterProtocolMinor":0,"stateSchemaMajor":1,"stateSchemaMinor":0}`,
			idempotencyKey: "contract-stage-4", expectedStatus: http.StatusAccepted,
		},
		{
			method: http.MethodDelete, path: "/api/v1/cluster/members/{memberId}", operationID: "removeClusterMember",
			target: "/api/v1/cluster/members/3", idempotencyKey: "contract-remove-3", expectedStatus: http.StatusAccepted,
		},
		{
			method: http.MethodGet, path: "/api/v1/cluster/membership-operations/{operationId}", operationID: "getMembershipOperation",
			target: "/api/v1/cluster/membership-operations/00000000-0000-4000-8000-000000000004", expectedStatus: http.StatusOK,
		},
	}
}
func drainOperationFixtures() []operationFixture {
	return []operationFixture{
		{
			method: http.MethodPost, path: "/api/v1/nodes/{nodeId}/drain", operationID: "startNodeDrain",
			target:         "/api/v1/nodes/3/drain",
			body:           `{"serverReference":"node3.example:1883","connectionRatePerSecond":10,"loadBalancerPropagationDelayMs":1000,"timeoutMs":60000}`,
			idempotencyKey: "contract-drain-start", expectedStatus: http.StatusAccepted,
		},
		{
			method: http.MethodGet, path: "/api/v1/nodes/{nodeId}/drain", operationID: "getNodeDrain",
			target: "/api/v1/nodes/3/drain", expectedStatus: http.StatusOK,
		},
		{
			method: http.MethodDelete, path: "/api/v1/nodes/{nodeId}/drain", operationID: "cancelNodeDrain",
			target: "/api/v1/nodes/3/drain", idempotencyKey: "contract-drain-cancel", expectedStatus: http.StatusOK,
		},
	}
}
func connectionRebalanceOperationFixtures() []operationFixture {
	return []operationFixture{
		{
			method: http.MethodPost, path: "/api/v1/cluster/rebalance", operationID: "startConnectionRebalance",
			target:         "/api/v1/cluster/rebalance",
			body:           `{"serverReference":"lb.example:1883","maxDeviationConnections":1,"connectionRatePerSecond":5,"loadBalancerPropagationDelayMs":500,"timeoutMs":120000}`,
			idempotencyKey: "contract-rebalance-start", expectedStatus: http.StatusAccepted,
		},
		{
			method: http.MethodGet, path: "/api/v1/cluster/rebalance", operationID: "getConnectionRebalance",
			target: "/api/v1/cluster/rebalance", expectedStatus: http.StatusOK,
		},
		{
			method: http.MethodDelete, path: "/api/v1/cluster/rebalance", operationID: "cancelConnectionRebalance",
			target: "/api/v1/cluster/rebalance", idempotencyKey: "contract-rebalance-cancel", expectedStatus: http.StatusOK,
		},
	}
}

func backupOperationFixtures() []operationFixture {
	return []operationFixture{
		{method: http.MethodPost, path: "/api/v1/cluster/backups", operationID: "createClusterBackup", target: "/api/v1/cluster/backups", expectedStatus: http.StatusOK},
	}
}
func featureOperationFixtures() []operationFixture {
	return []operationFixture{
		{method: http.MethodGet, path: "/api/v1/cluster/features", operationID: "listClusterFeatures", target: "/api/v1/cluster/features", expectedStatus: http.StatusOK},
		{method: http.MethodPost, path: "/api/v1/cluster/features/{feature}", operationID: "activateClusterFeature", target: "/api/v1/cluster/features/compatibility-probe", expectedStatus: http.StatusOK},
	}
}

func credentialRevocationOperationFixtures() []operationFixture {
	return []operationFixture{
		{
			method: http.MethodGet, path: "/api/v1/security/credential-revocations",
			operationID: "listCredentialRevocations", target: "/api/v1/security/credential-revocations",
			expectedStatus: http.StatusOK,
		},
		{
			method: http.MethodPost, path: "/api/v1/security/credential-revocations/{credentialId}",
			operationID: "revokeCredential", target: "/api/v1/security/credential-revocations/credential-a",
			expectedStatus: http.StatusOK,
		},
		{
			method: http.MethodDelete, path: "/api/v1/security/credential-revocations/{credentialId}",
			operationID: "restoreCredential", target: "/api/v1/security/credential-revocations/credential-a",
			expectedStatus: http.StatusOK,
		},
	}
}

func blocklistOperationFixtures() []operationFixture {
	return []operationFixture{
		{method: http.MethodGet, path: "/api/v1/security/blocklist", operationID: "listBlocklist", target: "/api/v1/security/blocklist", expectedStatus: http.StatusOK},
		{method: http.MethodPost, path: "/api/v1/security/blocklist/client-ids/{tenantId}/{clientId}", operationID: "blockClientId", target: "/api/v1/security/blocklist/client-ids/tenant-a/client-a", expectedStatus: http.StatusOK},
		{method: http.MethodDelete, path: "/api/v1/security/blocklist/client-ids/{tenantId}/{clientId}", operationID: "unblockClientId", target: "/api/v1/security/blocklist/client-ids/tenant-a/client-a", expectedStatus: http.StatusOK},
		{method: http.MethodPost, path: "/api/v1/security/blocklist/usernames/{username}", operationID: "blockUsername", target: "/api/v1/security/blocklist/usernames/contract-user", expectedStatus: http.StatusOK},
		{method: http.MethodDelete, path: "/api/v1/security/blocklist/usernames/{username}", operationID: "unblockUsername", target: "/api/v1/security/blocklist/usernames/contract-user", expectedStatus: http.StatusOK},
		{method: http.MethodPost, path: "/api/v1/security/blocklist/certificates/{certificateSha256}", operationID: "blockCertificate", target: "/api/v1/security/blocklist/certificates/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", expectedStatus: http.StatusOK},
		{method: http.MethodDelete, path: "/api/v1/security/blocklist/certificates/{certificateSha256}", operationID: "unblockCertificate", target: "/api/v1/security/blocklist/certificates/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", expectedStatus: http.StatusOK},
		{method: http.MethodPost, path: "/api/v1/security/blocklist/ip-prefixes/{address}/{prefixLength}", operationID: "blockIpPrefix", target: "/api/v1/security/blocklist/ip-prefixes/203.0.113.0/24", expectedStatus: http.StatusOK},
		{method: http.MethodDelete, path: "/api/v1/security/blocklist/ip-prefixes/{address}/{prefixLength}", operationID: "unblockIpPrefix", target: "/api/v1/security/blocklist/ip-prefixes/203.0.113.0/24", expectedStatus: http.StatusOK},
	}
}

func contractDocumentPath(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", "..", "..", "docs", "contracts", "management-api-v1.openapi.json"))
}

func loadManagementContract(t *testing.T) (*openapi3.T, routers.Router) {
	t.Helper()
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	doc, err := loader.LoadFromFile(contractDocumentPath(t))
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}
	if !doc.IsOpenAPI31OrLater() {
		t.Fatalf("OpenAPI version = %s, want 3.1 or later", doc.OpenAPI)
	}
	if err := doc.Validate(
		context.Background(),
		openapi3.EnableExamplesValidation(),
		openapi3.EnableMultiError(),
		openapi3.EnableSchemaFormatValidation(),
		openapi3.IsOpenAPI31OrLater(),
	); err != nil {
		t.Fatalf("validate OpenAPI document: %v", err)
	}
	doc.Servers = nil
	openapi3filter.RegisterBodyDecoder("application/openmetrics-text", openapi3filter.PlainBodyDecoder)
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatalf("create OpenAPI router: %v", err)
	}
	return doc, router
}

func contractValidationOptions() *openapi3filter.Options {
	return &openapi3filter.Options{
		MultiError: true,
		SchemaValidationOptions: []openapi3.SchemaValidationOption{
			openapi3.EnableJSONSchema2020(),
		},
		AuthenticationFunc: func(context.Context, *openapi3filter.AuthenticationInput) error {
			return nil
		},
	}
}

func newContractRequest(fixture operationFixture) *http.Request {
	var body io.Reader
	if fixture.body != "" {
		body = strings.NewReader(fixture.body)
	}
	req := httptest.NewRequest(fixture.method, contractServerURL+fixture.target, body)
	if strings.HasPrefix(fixture.path, "/api/") {
		req.Header.Set("Authorization", "Bearer contract-token")
	}
	if fixture.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if fixture.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", fixture.idempotencyKey)
	}
	if fixture.accept != "" {
		req.Header.Set("Accept", fixture.accept)
	}
	if fixture.path == "/metrics" && fixture.accept == "" {
		req.Header.Set("Accept", "text/plain; version=0.0.4")
	}
	return req
}

func findContractRoute(t *testing.T, router routers.Router, req *http.Request) (*openapi3filter.RequestValidationInput, *openapi3filter.Options) {
	t.Helper()
	route, pathParams, err := router.FindRoute(req)
	if err != nil {
		t.Fatalf("find OpenAPI route for %s %s: %v", req.Method, req.URL.RequestURI(), err)
	}
	options := contractValidationOptions()
	return &openapi3filter.RequestValidationInput{
		Request:     req,
		PathParams:  pathParams,
		QueryParams: req.URL.Query(),
		Route:       route,
		Options:     options,
	}, options
}

func validateResponse(t *testing.T, input *openapi3filter.RequestValidationInput, options *openapi3filter.Options, response *httptest.ResponseRecorder) {
	t.Helper()
	validationInput := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: input,
		Status:                 response.Code,
		Header:                 response.Header(),
		Body:                   io.NopCloser(bytes.NewReader(response.Body.Bytes())),
		Options:                options,
	}
	if err := openapi3filter.ValidateResponse(context.Background(), validationInput); err != nil {
		t.Fatalf("validate %s %s response status %d content-type %q: %v\n%s", input.Request.Method, input.Request.URL.RequestURI(), response.Code, response.Header().Get("Content-Type"), err, response.Body.String())
	}
}

type contractStateProvider struct{}

func (contractStateProvider) GetClusterSummary(context.Context) ports.ClusterSummaryDTO {
	leader := "node-1"
	return ports.ClusterSummaryDTO{
		ClusterID:         "cluster-1",
		ReleaseVersion:    "1.0.0",
		MemberCount:       1,
		QuorumSize:        1,
		ReplicationFactor: 1,
		WriteQuorum:       1,
		QuorumAvailable:   true,
		LeaderNodeID:      &leader,
		NodesByState:      ports.NodesByStateDTO{Ready: 1},
		UnderReplicated:   0,
		ConnectionCount:   0,
		SessionCount:      1,
		SubscriptionCount: 1,
	}
}

func (contractStateProvider) GetNodeSummary(context.Context, string) (*ports.NodeSummaryDTO, error) {
	reason := "raft storage I/O is slow"
	health := "DEGRADED"
	errorCode := "storage_slow"
	return &ports.NodeSummaryDTO{
		NodeID:            "node-1",
		Version:           "1.0.0",
		UptimeMs:          0,
		State:             ports.NodeStateReady,
		Ready:             false,
		ReadinessReason:   &reason,
		Role:              "LEADER",
		LastAppliedIndex:  0,
		ConnectionCount:   0,
		StoragePressure:   true,
		ReplicationHealth: &health,
		LastErrorCode:     &errorCode,
	}, nil
}

func (contractStateProvider) ListNodeSummaries(context.Context, ports.NodeFilter) ([]ports.NodeSummaryDTO, *ports.PageDTO, error) {
	local, _ := contractStateProvider{}.GetNodeSummary(context.Background(), "node-1")
	reason := "remote readiness is not available from this node"
	health := "UNREACHABLE"
	errorCode := "peer_disconnected"
	remote := ports.NodeSummaryDTO{
		NodeID: "node-2", Version: "1.0.0", State: ports.NodeStateJoining, Ready: false,
		ReadinessReason: &reason, Role: "FOLLOWER", LastAppliedIndex: 0,
		StoragePressure: false, ReplicationHealth: &health, LastErrorCode: &errorCode,
		ReplicationQueue: &ports.ReplicationQueueDTO{
			OutboundMessages: 1, OutboundBytes: 128, RejectedMessages: 2,
		},
	}
	return []ports.NodeSummaryDTO{*local, remote}, emptyPage(), nil
}

func (contractStateProvider) ListClients(context.Context, ports.ClientFilter) ([]ports.ClientSessionDTO, *ports.PageDTO, error) {
	return []ports.ClientSessionDTO{{
		TenantID:          "default",
		ClientID:          "device-1",
		NodeID:            stringPointer("node-1"),
		Protocol:          "MQTT_5_0",
		Connected:         true,
		SessionPresent:    true,
		ExpiryAt:          nil,
		InflightCount:     0,
		OfflineQueueCount: 0,
		MaskedIdentity:    nil,
	}}, emptyPage(), nil
}

func (contractStateProvider) GetClient(context.Context, string, string) (*ports.ClientSessionDTO, error) {
	return &ports.ClientSessionDTO{
		TenantID:          "default",
		ClientID:          "device-1",
		NodeID:            stringPointer("node-1"),
		Protocol:          "MQTT_5_0",
		Connected:         true,
		SessionPresent:    true,
		ExpiryAt:          nil,
		InflightCount:     0,
		OfflineQueueCount: 0,
		MaskedIdentity:    nil,
	}, nil
}

func (contractStateProvider) ListSubscriptions(context.Context, ports.SubscriptionFilter) ([]ports.SubscriptionDTO, *ports.PageDTO, error) {
	return []ports.SubscriptionDTO{{
		TenantID:    "default",
		ClientID:    "device-1",
		TopicFilter: "sensors/#",
		QoS:         0,
		SharedGroup: nil,
	}}, emptyPage(), nil
}

func (contractStateProvider) ListRetained(context.Context, ports.RetainedFilter) ([]ports.RetainedMetadataDTO, *ports.PageDTO, error) {
	now := time.Date(2026, time.September, 20, 0, 0, 0, 0, time.UTC)
	return []ports.RetainedMetadataDTO{{
		TenantID:     "default",
		Topic:        "sensors/temp",
		QoS:          0,
		PayloadBytes: 4,
		UpdatedAt:    now,
		ExpiresAt:    nil,
	}}, emptyPage(), nil
}

func (contractStateProvider) ListTenants(context.Context, ports.TenantFilter) ([]ports.TenantSummaryDTO, *ports.PageDTO, error) {
	return []ports.TenantSummaryDTO{{
		TenantID:    "default",
		DisplayName: stringPointer("Default Tenant"),
		Status:      "ACTIVE",
		Version:     1,
		CreatedAt:   time.Date(2026, time.September, 20, 0, 0, 0, 0, time.UTC),
	}}, emptyPage(), nil
}

func (contractStateProvider) CheckLiveness(context.Context) (ports.HealthStatus, *string) {
	return ports.HealthStatusUp, nil
}

func (contractStateProvider) CheckReadiness(context.Context) (ports.HealthStatus, *string) {
	return ports.HealthStatusUp, nil
}

func (contractStateProvider) CheckStartup(context.Context) (ports.HealthStatus, *string) {
	return ports.HealthStatusUp, nil
}

func (contractStateProvider) CheckDrain(context.Context) (ports.HealthStatus, *string) {
	return ports.HealthStatusUp, nil
}

func emptyPage() *ports.PageDTO {
	return &ports.PageDTO{NextCursor: nil, HasMore: false}
}

func stringPointer(value string) *string {
	return &value
}

func contractHandler(cfg managementhttp.ServerConfig) http.Handler {
	if cfg.RequestRate == 0 {
		cfg.RequestRate = managementhttp.MaxRequestRate
	}
	if cfg.AuthFailureRate == 0 {
		cfg.AuthFailureRate = managementhttp.MaxRequestRate
	}
	return managementhttp.NewRouter(
		cfg,
		contractStateProvider{},
		contractStateProvider{},
		adaptersmetrics.NewRegistry(),
	)
}
func clusterManagementContractHandler() http.Handler {
	return clusterManagementContractHandlerWithRebalance(contractRebalanceService{})
}

func clusterManagementContractHandlerWithRebalance(rebalanceService managementhttp.ConnectionRebalanceUseCase) http.Handler {
	return clusterManagementContractHandlerWithServices(managementhttp.ServerConfig{
		ClusterID: "contract-cluster", AuthToken: "contract-token",
		RequestRate: managementhttp.MaxRequestRate, AuthFailureRate: managementhttp.MaxRequestRate,
		AllowAnonymousProbes: true, AllowAnonymousMetrics: true,
	}, rebalanceService, contractAuditQueryUseCase{})
}

func clusterManagementContractHandlerWithConfig(config managementhttp.ServerConfig) http.Handler {
	return clusterManagementContractHandlerWithServices(config, contractRebalanceService{}, contractAuditQueryUseCase{})
}

func clusterManagementContractHandlerWithServices(
	config managementhttp.ServerConfig,
	rebalanceService managementhttp.ConnectionRebalanceUseCase,
	auditQueryService managementhttp.AuditQueryUseCase,
) http.Handler {
	if config.RequestRate == 0 {
		config.RequestRate = managementhttp.MaxRequestRate
	}
	if config.AuthFailureRate == 0 {
		config.AuthFailureRate = managementhttp.MaxRequestRate
	}
	return managementhttp.NewRouterWithMembership(
		config,
		contractStateProvider{},
		contractStateProvider{},
		adaptersmetrics.NewRegistry(),
		contractMembershipService{},
		contractDrainService{},
		rebalanceService,
		contractBackupProvider{},
		contractAuditUseCase{},
		auditQueryService,
		contractCredentialRevocationService{},
		contractBlocklistService{},
	)
}

type contractAuditQueryUseCase struct{}

func (contractAuditQueryUseCase) Query(context.Context, audit.QueryCommandDTO) (audit.QueryResultDTO, error) {
	return audit.QueryResultDTO{Records: []portsaudit.AuditRecordDTO{contractAuditRecord()}}, nil
}

type contractAuditRecordReader struct{}

func (contractAuditRecordReader) ReadAuditRecords(
	context.Context,
	portsaudit.AuditRecordQueryDTO,
) (portsaudit.AuditRecordPageDTO, error) {
	return portsaudit.AuditRecordPageDTO{Records: []portsaudit.AuditRecordDTO{contractAuditRecord()}}, nil
}

func contractAuditRecord() portsaudit.AuditRecordDTO {
	return portsaudit.AuditRecordDTO{
		Seq: 1, PrevHash: strings.Repeat("0", 64), Hash: strings.Repeat("1", 64),
		EventJSON: []byte(`{"schemaVersion":"1.0","eventId":"contract-audit-event","occurredAt":"2026-10-10T00:00:00Z","requestId":"contract-audit-request","producer":{"component":"BROKER","instanceId":"contract-cluster/node-1","version":"1.0.0"},"actor":{"subject":"management-bearer-token","roles":["ClusterAdmin"]},"action":"cluster.member.stage","target":{"type":"cluster_member","id":"4"},"result":"ALLOWED","reason":"succeeded"}`),
	}
}

type contractAuditUseCase struct{}

func (contractAuditUseCase) AdmitMutation() error                { return nil }
func (contractAuditUseCase) Record(audit.RecordCommandDTO) error { return nil }

type contractBackupProvider struct{}

func (contractBackupProvider) CaptureClusterBackup(context.Context) (ports.ClusterBackupDTO, error) {
	framed := []byte("NMQBKUP3 contract fixture")
	checksum := sha256.Sum256(framed)
	archive := append(append([]byte(nil), framed...), checksum[:]...)
	return ports.ClusterBackupDTO{CommitIndex: 41, ArchiveSHA256: checksum, Archive: archive}, nil
}

type contractMembershipService struct{}

func (contractMembershipService) StageJoin(_ context.Context, request appmembership.StageJoinRequestDTO) (appmembership.MembershipOperationDTO, error) {
	return appmembership.MembershipOperationDTO{
		OperationID:    "00000000-0000-4000-8000-000000000004",
		Action:         appmembership.MembershipActionStageJoin,
		MemberID:       request.Member.MemberID,
		Phase:          appmembership.OperationPhaseStaged,
		CommittedIndex: 41,
	}, nil
}

func (contractMembershipService) StartPlannedRemoval(_ context.Context, memberID uint64, _ string, _ uint64) (appmembership.MembershipOperationDTO, error) {
	return appmembership.MembershipOperationDTO{
		OperationID:    "00000000-0000-4000-8000-000000000004",
		Action:         appmembership.MembershipActionBeginRemove,
		MemberID:       memberID,
		Phase:          appmembership.OperationPhaseRemoving,
		CommittedIndex: 42,
	}, nil
}

func (contractMembershipService) GetOperation(_ context.Context, operationID string) (appmembership.MembershipOperationDTO, error) {
	return appmembership.MembershipOperationDTO{
		OperationID:    operationID,
		Action:         appmembership.MembershipActionStageJoin,
		MemberID:       4,
		Phase:          appmembership.OperationPhaseCatchingUp,
		CommittedIndex: 43,
	}, nil
}
func (contractMembershipService) ClusterFeatures(context.Context) (appmembership.FeatureStateDTO, error) {
	return appmembership.FeatureStateDTO{
		ActiveFeatures: []string{},
		Members: []appmembership.MemberFeatureProfileDTO{{
			MemberID: 1, ReleaseVersion: "1.2.0", Features: []string{"compatibility-probe"},
		}},
	}, nil
}

func (contractMembershipService) ActivateFeature(_ context.Context, feature string) (appmembership.FeatureStateDTO, error) {
	return appmembership.FeatureStateDTO{
		ActiveFeatures: []string{feature},
		Members: []appmembership.MemberFeatureProfileDTO{{
			MemberID: 1, ReleaseVersion: "1.2.0", Features: []string{"compatibility-probe"},
		}},
	}, nil
}

type contractCredentialRevocationService struct{}

func (contractCredentialRevocationService) ListCredentialRevocations(context.Context) ([]appmembership.CredentialRevocationStateDTO, error) {
	return []appmembership.CredentialRevocationStateDTO{{
		CredentialID: "credential-a", RevokedAtUnixMillis: 1_700_000_000_000,
	}}, nil
}

func (contractCredentialRevocationService) RevokeCredential(_ context.Context, credentialID string) (appmembership.CredentialRevocationStatusDTO, error) {
	return appmembership.CredentialRevocationStatusDTO{
		CredentialID: credentialID, Revoked: true, RevokedAtUnixMillis: 1_700_000_000_000,
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
		Entries:       []appmembership.BlocklistStateDTO{{Entry: entry, BlockedAtUnixMillis: 1_700_000_000_000}},
	}, nil
}

func (contractBlocklistService) Block(_ context.Context, entry security.BlockEntry) (appmembership.BlocklistStatusDTO, error) {
	return appmembership.BlocklistStatusDTO{
		Entry: entry, Blocked: true, BlockedAtUnixMillis: 1_700_000_000_000, PolicyVersion: 2,
	}, nil
}

func (contractBlocklistService) Unblock(_ context.Context, entry security.BlockEntry) (appmembership.BlocklistStatusDTO, error) {
	return appmembership.BlocklistStatusDTO{Entry: entry, PolicyVersion: 3}, nil
}

type contractDrainService struct{}

func (contractDrainService) StartNodeDrain(_ context.Context, request appmembership.StartNodeDrainRequestDTO) (appmembership.NodeDrainOperationDTO, error) {
	return appmembership.NodeDrainOperationDTO{State: appmembership.NodeDrainStateDTO{
		DrainOperationID: "00000000-0000-4000-8000-000000000005",
		MemberID:         request.MemberID, Phase: nodedrain.PhaseExcludingTraffic,
		StartedAtUnixMillis: 1, DeadlineAtUnixMillis: 60001,
	}}, nil
}

func (contractDrainService) GetNodeDrain(_ context.Context, memberID uint64) (appmembership.NodeDrainOperationDTO, error) {
	return appmembership.NodeDrainOperationDTO{State: appmembership.NodeDrainStateDTO{
		DrainOperationID: "00000000-0000-4000-8000-000000000005",
		MemberID:         memberID, Phase: nodedrain.PhaseTransferringSessions,
		StartedAtUnixMillis: 1, DeadlineAtUnixMillis: 60001,
	}}, nil
}

func (contractDrainService) CancelNodeDrain(_ context.Context, memberID uint64) (appmembership.NodeDrainOperationDTO, error) {
	return appmembership.NodeDrainOperationDTO{State: appmembership.NodeDrainStateDTO{
		DrainOperationID: "00000000-0000-4000-8000-000000000005",
		MemberID:         memberID, Phase: nodedrain.PhaseCancelled,
		StartedAtUnixMillis: 1, DeadlineAtUnixMillis: 60001,
	}}, nil
}

type contractRebalanceService struct {
	phase                  connrebalance.Phase
	replayed               bool
	withoutServerReference bool
}

func (service contractRebalanceService) StartConnectionRebalance(_ context.Context, request appmembership.StartConnectionRebalanceRequestDTO) (appmembership.ConnectionRebalanceOperationDTO, error) {
	phase := service.phase
	if !phase.Valid() {
		phase = connrebalance.PhaseRunning
	}
	operation := contractRebalanceOperation(request.IdempotencyKey, phase, service.replayed)
	if service.withoutServerReference {
		operation.State.ServerReference = ""
	}
	return operation, nil
}

func (service contractRebalanceService) GetConnectionRebalance(context.Context) (appmembership.ConnectionRebalanceOperationDTO, error) {
	phase := service.phase
	if !phase.Valid() {
		phase = connrebalance.PhaseCompleted
	}
	operation := contractRebalanceOperation("contract-rebalance-start", phase, false)
	if service.withoutServerReference {
		operation.State.ServerReference = ""
	}
	return operation, nil
}

func (service contractRebalanceService) CancelConnectionRebalance(context.Context) (appmembership.ConnectionRebalanceOperationDTO, error) {
	phase := service.phase
	if !phase.Valid() {
		phase = connrebalance.PhaseCancelled
	}
	operation := contractRebalanceOperation("contract-rebalance-start", phase, false)
	if service.withoutServerReference {
		operation.State.ServerReference = ""
	}
	return operation, nil
}

func contractRebalanceOperation(idempotencyKey string, phase connrebalance.Phase, replayed bool) appmembership.ConnectionRebalanceOperationDTO {
	finishedAt := uint64(0)
	failureCode := ""
	participants := []appmembership.ConnectionRebalanceReportDTO{
		{NodeID: 1, Connections: 12, SessionsTransferred: 5, Shedding: phase == connrebalance.PhaseRunning, ReportedAtUnixMillis: 2000},
		{NodeID: 2, Connections: 7, ReportedAtUnixMillis: 2000},
		{NodeID: 3},
	}
	if phase.Terminal() {
		finishedAt = 3000
	}
	if phase == connrebalance.PhaseFailed {
		failureCode = "rebalance_timeout"
	}
	return appmembership.ConnectionRebalanceOperationDTO{
		State: appmembership.ConnectionRebalanceStateDTO{
			RebalanceOperationID: "00000000-0000-4000-8000-000000000006",
			IdempotencyKey:       idempotencyKey, Phase: phase, ServerReference: "lb.example:1883",
			MaxDeviationConnections: 1, ConnectionRatePerSecond: 5,
			LoadBalancerPropagationDelayMillis: 500, TimeoutMillis: 120000,
			StartedAtUnixMillis: 1000, DeadlineAtUnixMillis: 121000, FinishedAtUnixMillis: finishedAt,
			Participants: participants, FailureCode: failureCode,
		},
		Replayed: replayed,
	}
}

func expectedOperationSet() []string {
	fixtures := append(managementOperationFixtures(), auditQueryOperationFixture())
	fixtures = append(fixtures, membershipOperationFixtures()...)
	fixtures = append(fixtures, backupOperationFixtures()...)
	fixtures = append(fixtures, drainOperationFixtures()...)
	fixtures = append(fixtures, connectionRebalanceOperationFixtures()...)
	fixtures = append(fixtures, credentialRevocationOperationFixtures()...)
	fixtures = append(fixtures, blocklistOperationFixtures()...)
	fixtures = append(fixtures, featureOperationFixtures()...)
	operations := make([]string, 0, len(fixtures))
	for _, fixture := range fixtures {
		operations = append(operations, fixture.method+" "+fixture.path+" "+fixture.operationID)
	}
	sort.Strings(operations)
	return operations
}

func documentOperationSet(doc *openapi3.T) []string {
	operations := make([]string, 0)
	for path, item := range doc.Paths.Map() {
		for method, operation := range item.Operations() {
			operations = append(operations, strings.ToUpper(method)+" "+path+" "+operation.OperationID)
		}
	}
	sort.Strings(operations)
	return operations
}

// Traceability: API-02; docs/specs/management-api-and-observability.md §9.
func TestOpenAPIContractIsValid(t *testing.T) {
	loadManagementContract(t)
}

// Traceability: API-02; docs/specs/management-api-and-observability.md §9.
func TestManagementHandlerResponsesMatchOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	handler := contractHandler(managementhttp.ServerConfig{
		AllowAnonymousProbes:  true,
		AllowAnonymousMetrics: true,
	})

	for _, fixture := range managementOperationFixtures() {
		t.Run(fixture.operationID, func(t *testing.T) {
			req := newContractRequest(fixture)
			input, options := findContractRoute(t, router, req)
			if input.Route.Operation.OperationID != fixture.operationID {
				t.Fatalf("runtime fixture matched operation %q, want %q", input.Route.Operation.OperationID, fixture.operationID)
			}
			if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
				t.Fatalf("validate representative request: %v", err)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
			}
			validateResponse(t, input, options, recorder)

			if fixture.operationID == "getMetrics" {
				openMetrics := operationFixture{
					method:      fixture.method,
					path:        fixture.path,
					operationID: fixture.operationID,
					target:      fixture.target,
					accept:      "application/openmetrics-text; version=1.0.0",
				}
				openMetricsRequest := newContractRequest(openMetrics)
				openMetricsInput, openMetricsOptions := findContractRoute(t, router, openMetricsRequest)
				openMetricsRecorder := httptest.NewRecorder()
				handler.ServeHTTP(openMetricsRecorder, openMetricsRequest)
				if openMetricsRecorder.Code != http.StatusOK {
					t.Fatalf("OpenMetrics status = %d: %s", openMetricsRecorder.Code, openMetricsRecorder.Body.String())
				}
				validateResponse(t, openMetricsInput, openMetricsOptions, openMetricsRecorder)
				if got := openMetricsRecorder.Header().Get("Content-Type"); got != managementhttp.ContentTypeOpenMetrics {
					t.Fatalf("OpenMetrics content type = %q, want %q", got, managementhttp.ContentTypeOpenMetrics)
				}
				if !strings.HasSuffix(openMetricsRecorder.Body.String(), "# EOF\n") {
					t.Fatalf("OpenMetrics body must end with # EOF: %q", openMetricsRecorder.Body.String())
				}
			}
		})
	}
}

// Traceability: SEC-10/API-08; docs/specs/management-api-and-observability.md §1, §9; docs/specs/tenancy-and-security.md §6.
func TestAuditQueryHandlerResponsesMatchOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	queryService, err := audit.NewQueryService(contractAuditRecordReader{})
	if err != nil {
		t.Fatalf("NewQueryService() error = %v", err)
	}
	handler := clusterManagementContractHandlerWithServices(
		managementhttp.ServerConfig{ClusterID: "contract-cluster", AuthToken: "contract-token"},
		contractRebalanceService{}, queryService,
	)
	fixture := auditQueryOperationFixture()
	request := newContractRequest(fixture)
	input, options := findContractRoute(t, router, request)
	if input.Route.Operation.OperationID != fixture.operationID {
		t.Fatalf("runtime fixture matched operation %q, want %q", input.Route.Operation.OperationID, fixture.operationID)
	}
	if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
		t.Fatalf("validate audit query request: %v", err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("audit query status=%d body=%q, want %d", response.Code, response.Body.String(), http.StatusOK)
	}
	validateResponse(t, input, options, response)

	invalidRequest := httptest.NewRequest(http.MethodGet, contractServerURL+"/api/v1/audit/events?result=UNKNOWN", nil)
	invalidRequest.Header.Set("Authorization", "Bearer contract-token")
	invalidInput, invalidOptions := findContractRoute(t, router, invalidRequest)
	invalidResponse := httptest.NewRecorder()
	handler.ServeHTTP(invalidResponse, invalidRequest)
	if invalidResponse.Code != http.StatusBadRequest {
		t.Fatalf("invalid audit result status=%d body=%q, want %d", invalidResponse.Code, invalidResponse.Body.String(), http.StatusBadRequest)
	}
	validateResponse(t, invalidInput, invalidOptions, invalidResponse)
}

func TestMembershipHandlerResponsesMatchOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	handler := clusterManagementContractHandler()

	for _, fixture := range membershipOperationFixtures() {
		t.Run(fixture.operationID, func(t *testing.T) {
			req := newContractRequest(fixture)
			input, options := findContractRoute(t, router, req)
			if input.Route.Operation.OperationID != fixture.operationID {
				t.Fatalf("runtime fixture matched operation %q, want %q", input.Route.Operation.OperationID, fixture.operationID)
			}
			if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
				t.Fatalf("validate representative request: %v", err)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			if recorder.Code != fixture.expectedStatus {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, fixture.expectedStatus, recorder.Body.String())
			}
			validateResponse(t, input, options, recorder)
		})
	}
}
func TestDrainHandlerResponsesMatchOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	handler := clusterManagementContractHandler()

	for _, fixture := range drainOperationFixtures() {
		t.Run(fixture.operationID, func(t *testing.T) {
			req := newContractRequest(fixture)
			input, options := findContractRoute(t, router, req)
			if input.Route.Operation.OperationID != fixture.operationID {
				t.Fatalf("runtime fixture matched operation %q, want %q", input.Route.Operation.OperationID, fixture.operationID)
			}
			if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
				t.Fatalf("validate representative request: %v", err)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			if recorder.Code != fixture.expectedStatus {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, fixture.expectedStatus, recorder.Body.String())
			}
			validateResponse(t, input, options, recorder)
		})
	}
}

func TestRebalanceHandlerResponsesMatchOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	fixtures := connectionRebalanceOperationFixtures()
	cases := []struct {
		name                   string
		index                  int
		phase                  connrebalance.Phase
		replayed               bool
		withoutServerReference bool
		status                 int
	}{
		{name: "start running", index: 0},
		{name: "start replay", index: 0, replayed: true, status: http.StatusOK},
		{name: "get running with no server reference", index: 1, phase: connrebalance.PhaseRunning, withoutServerReference: true},
		{name: "get completed", index: 1, phase: connrebalance.PhaseCompleted},
		{name: "get failed", index: 1, phase: connrebalance.PhaseFailed},
		{name: "get cancelled", index: 1, phase: connrebalance.PhaseCancelled},
		{name: "cancel cancelled", index: 2},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := fixtures[testCase.index]
			req := newContractRequest(fixture)
			input, options := findContractRoute(t, router, req)
			if input.Route.Operation.OperationID != fixture.operationID {
				t.Fatalf("runtime fixture matched operation %q, want %q", input.Route.Operation.OperationID, fixture.operationID)
			}
			if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
				t.Fatalf("validate representative request: %v", err)
			}
			handler := clusterManagementContractHandlerWithRebalance(contractRebalanceService{
				phase: testCase.phase, replayed: testCase.replayed, withoutServerReference: testCase.withoutServerReference,
			})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			status := testCase.status
			if status == 0 {
				status = fixture.expectedStatus
			}
			if recorder.Code != status {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, status, recorder.Body.String())
			}
			validateResponse(t, input, options, recorder)
		})
	}
}

func TestClusterBackupHandlerResponseMatchesOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	handler := clusterManagementContractHandler()

	for _, fixture := range backupOperationFixtures() {
		t.Run(fixture.operationID, func(t *testing.T) {
			req := newContractRequest(fixture)
			input, options := findContractRoute(t, router, req)
			if input.Route.Operation.OperationID != fixture.operationID {
				t.Fatalf("runtime fixture matched operation %q, want %q", input.Route.Operation.OperationID, fixture.operationID)
			}
			if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
				t.Fatalf("validate representative request: %v", err)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			if recorder.Code != fixture.expectedStatus {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, fixture.expectedStatus, recorder.Body.String())
			}
			validateResponse(t, input, options, recorder)
		})
	}
}

func TestClusterFeatureHandlerResponsesMatchOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	handler := clusterManagementContractHandler()

	for _, fixture := range featureOperationFixtures() {
		t.Run(fixture.operationID, func(t *testing.T) {
			req := newContractRequest(fixture)
			input, options := findContractRoute(t, router, req)
			if input.Route.Operation.OperationID != fixture.operationID {
				t.Fatalf("runtime fixture matched operation %q, want %q", input.Route.Operation.OperationID, fixture.operationID)
			}
			if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
				t.Fatalf("validate representative request: %v", err)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			if recorder.Code != fixture.expectedStatus {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, fixture.expectedStatus, recorder.Body.String())
			}
			validateResponse(t, input, options, recorder)
		})
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7; docs/contracts/management-api-v1.md.
func TestCredentialRevocationHandlerResponsesMatchOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	handler := clusterManagementContractHandler()
	for _, fixture := range credentialRevocationOperationFixtures() {
		t.Run(fixture.operationID, func(t *testing.T) {
			request := newContractRequest(fixture)
			input, options := findContractRoute(t, router, request)
			if input.Route.Operation.OperationID != fixture.operationID {
				t.Fatalf("runtime fixture matched operation %q, want %q", input.Route.Operation.OperationID, fixture.operationID)
			}
			if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
				t.Fatalf("validate representative request: %v", err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != fixture.expectedStatus {
				t.Fatalf("status=%d want %d: %s", response.Code, fixture.expectedStatus, response.Body.String())
			}
			validateResponse(t, input, options, response)
		})
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050; docs/contracts/management-api-v1.md.
func TestBlocklistHandlerResponsesMatchOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	handler := clusterManagementContractHandler()
	for _, fixture := range blocklistOperationFixtures() {
		t.Run(fixture.operationID, func(t *testing.T) {
			request := newContractRequest(fixture)
			input, options := findContractRoute(t, router, request)
			if input.Route.Operation.OperationID != fixture.operationID {
				t.Fatalf("runtime fixture matched operation %q, want %q", input.Route.Operation.OperationID, fixture.operationID)
			}
			if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
				t.Fatalf("validate representative request: %v", err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != fixture.expectedStatus {
				t.Fatalf("status=%d want %d: %s", response.Code, fixture.expectedStatus, response.Body.String())
			}
			validateResponse(t, input, options, response)
		})
	}
}

// Traceability: API-02/SEC-10; docs/specs/management-api-and-observability.md §9; docs/specs/tenancy-and-security.md §6.
func TestManagementOperationSetMatchesOpenAPI(t *testing.T) {
	doc, _ := loadManagementContract(t)
	got := documentOperationSet(doc)
	want := expectedOperationSet()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OpenAPI operation set mismatch\n got: %s\nwant: %s", formatOperationSet(got), formatOperationSet(want))
	}
}

// Traceability: PRD API-05; docs/specs/management-api-and-observability.md §9.
func TestManagementQueryValidationMatchesOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	handler := contractHandler(managementhttp.ServerConfig{AllowAnonymousMetrics: true})

	valid := []string{
		"/api/v1/nodes?limit=1",
		"/api/v1/nodes?limit=1000",
		"/api/v1/clients?limit=50",
		"/api/v1/subscriptions?tenantId=default&limit=50",
		"/api/v1/retained?tenantId=default&limit=50",
		"/api/v1/tenants?limit=50",
	}
	for _, target := range valid {
		t.Run("valid "+target, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, contractServerURL+target, nil)
			req.Header.Set("Authorization", "Bearer contract-token")
			input, options := findContractRoute(t, router, req)
			if err := openapi3filter.ValidateRequest(context.Background(), input); err != nil {
				t.Fatalf("validate request: %v", err)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
			}
			validateResponse(t, input, options, recorder)
		})
	}

	invalid := []string{
		"/api/v1/nodes?unknown=value",
		"/api/v1/clients?limit=1&limit=2",
		"/api/v1/clients?limit=0",
		"/api/v1/subscriptions",
		"/api/v1/subscriptions?tenantId=",
		"/api/v1/retained",
		"/api/v1/retained?tenantId=",
	}
	for _, target := range invalid {
		t.Run("invalid "+target, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, contractServerURL+target, nil)
			req.Header.Set("Authorization", "Bearer contract-token")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
			}
			var response managementhttp.ErrorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if response.Error.Code != "invalid_request" {
				t.Fatalf("error code = %q, want invalid_request", response.Error.Code)
			}
		})
	}
	t.Run("over max page size", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, contractServerURL+"/api/v1/clients?limit=1001", nil)
		req.Header.Set("Authorization", "Bearer contract-token")
		input, options := findContractRoute(t, router, req)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
		}
		validateResponse(t, input, options, recorder)
		var response managementhttp.ErrorResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode error response: %v", err)
		}
		if response.Error.Code != "invalid_request" {
			t.Fatalf("error code = %q, want invalid_request", response.Error.Code)
		}
	})
}

type contractRateLimitClock struct {
	now time.Time
}

func (clock *contractRateLimitClock) Now() time.Time {
	return clock.now
}

// TestManagementRateLimitMatchesOpenAPI verifies principal and failed-authentication 429 responses.
// Traceability: PRD API-05; docs/specs/management-api-and-observability.md §1.5.
func TestManagementRateLimitMatchesOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	clock := &contractRateLimitClock{now: time.Unix(1_800_000_000, 0)}
	handler := managementhttp.NewRouter(managementhttp.ServerConfig{
		AuthToken:       "bearer-secret",
		RequestRate:     1,
		AuthFailureRate: 1,
		Clock:           clock,
	}, contractStateProvider{}, contractStateProvider{}, adaptersmetrics.NewRegistry())

	for _, testCase := range []struct {
		name            string
		authorization   string
		wantStatusCodes []int
	}{
		{name: "principal", authorization: "Bearer bearer-secret", wantStatusCodes: []int{http.StatusOK, http.StatusTooManyRequests}},
		{name: "authentication failure", authorization: "Bearer invalid", wantStatusCodes: []int{http.StatusUnauthorized, http.StatusTooManyRequests}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			for index, wantStatus := range testCase.wantStatusCodes {
				request := httptest.NewRequest(http.MethodGet, contractServerURL+"/api/v1/cluster", nil)
				request.RemoteAddr = "192.0.2.10:1234"
				request.Header.Set("Authorization", testCase.authorization)
				input, options := findContractRoute(t, router, request)
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				if recorder.Code != wantStatus {
					t.Fatalf("request %d status = %d, want %d: %s", index+1, recorder.Code, wantStatus, recorder.Body.String())
				}
				validateResponse(t, input, options, recorder)
			}
		})
	}
}

// Traceability: API-05/SEC-10; docs/specs/management-api-and-observability.md §1.5; docs/specs/tenancy-and-security.md §6.
func TestManagementAuthorizationMatchesOpenAPI(t *testing.T) {
	_, router := loadManagementContract(t)
	handler := clusterManagementContractHandlerWithConfig(managementhttp.ServerConfig{
		ClusterID:   "contract-cluster",
		AuthToken:   "bearer-secret",
		APIKeys:     []managementhttp.APIKeyCredential{{ID: "dashboard", Key: "api-secret", Roles: []managementhttp.Role{managementhttp.RoleReadOnly}}},
		RequestRate: managementhttp.MaxRequestRate, AuthFailureRate: managementhttp.MaxRequestRate,
	})

	cases := []struct {
		name       string
		path       string
		headerName string
		header     string
		wantStatus int
	}{
		{name: "anonymous", path: "/api/v1/cluster", wantStatus: http.StatusUnauthorized},
		{name: "wrong bearer", path: "/api/v1/cluster", headerName: "Authorization", header: "Bearer wrong", wantStatus: http.StatusUnauthorized},
		{name: "bearer success", path: "/api/v1/cluster", headerName: "Authorization", header: "Bearer bearer-secret", wantStatus: http.StatusOK},
		{name: "api key success", path: "/api/v1/cluster", headerName: "X-API-Key", header: "api-secret", wantStatus: http.StatusOK},
		{name: "api key insufficient audit role", path: "/api/v1/audit/events?limit=1", headerName: "X-API-Key", header: "api-secret", wantStatus: http.StatusForbidden},
		{name: "break-glass audit access", path: "/api/v1/audit/events?limit=1", headerName: "Authorization", header: "Bearer bearer-secret", wantStatus: http.StatusOK},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, contractServerURL+testCase.path, nil)
			if testCase.headerName != "" {
				req.Header.Set(testCase.headerName, testCase.header)
			}
			input, options := findContractRoute(t, router, req)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			if recorder.Code != testCase.wantStatus {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, testCase.wantStatus, recorder.Body.String())
			}
			validateResponse(t, input, options, recorder)
		})
	}
}

func formatOperationSet(operations []string) string {
	return fmt.Sprintf("%v", operations)
}
