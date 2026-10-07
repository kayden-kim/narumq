package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Traceability: SEC-11/API-08/NFR-17; docs/specs/tenancy-and-security.md §9.
func TestDynamicClusterManagementAuditProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	if err := os.WriteFile(fixture.auditDirectory(1), []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("create node 1 audit path as a regular file: %v", err)
	}
	processes := clusterDrainProcessStartCluster(t, fixture)
	ctx := fixture.ctx

	status, body, _, err := clusterManagementAuditProcessRequest(ctx, http.MethodGet,
		processes[0].managementURL()+"/api/v1/cluster", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("node 1 management GET status=%d body=%s err=%v, want 200", status, body, err)
	}
	drainBody := []byte(`{"connectionRatePerSecond":2,"loadBalancerPropagationDelayMs":1500,"timeoutMs":60000}`)
	status, body, _, err = clusterManagementAuditProcessRequest(ctx, http.MethodPost,
		processes[0].managementURL()+"/api/v1/nodes/3/drain", drainBody, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusServiceUnavailable {
		t.Fatalf("node 1 drain mutation status=%d body=%s err=%v, want 503", status, body, err)
	}
	var unavailable struct {
		Error struct {
			Code      string `json:"code"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &unavailable); err != nil {
		t.Fatalf("decode node 1 audit-unavailable response %q: %v", body, err)
	}
	if unavailable.Error.Code != "audit_unavailable" || !unavailable.Error.Retryable {
		t.Fatalf("node 1 mutation error=%+v, want retryable audit_unavailable", unavailable.Error)
	}
	status, body, _, err = clusterManagementAuditProcessRequest(ctx, http.MethodGet,
		processes[0].managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("node 1 metrics status=%d body=%s err=%v, want 200", status, body, err)
	}
	metrics := string(body)
	if !strings.Contains(metrics, "narumq_audit_publisher_ready 0\n") ||
		!strings.Contains(metrics, `narumq_audit_events_total{outcome="mutation_rejected"} 1`) {
		t.Fatalf("node 1 audit metrics do not reflect unavailable publisher and rejected mutation: %s", metrics)
	}
	clusterMembershipProcessAssertMQTTAccepted(t, fixture.mqttAddresses[0], fixture.accessToken)

	const invalidToken = "invalid-audit-process-token"
	status, body, unauthorizedRequestID, err := clusterManagementAuditProcessRequest(ctx, http.MethodGet,
		processes[1].managementURL()+"/api/v1/cluster", nil, invalidToken, "")
	if err != nil || status != http.StatusUnauthorized {
		t.Fatalf("node 2 invalid-token GET status=%d body=%s err=%v, want 401", status, body, err)
	}
	featurePath := processes[1].managementURL() + "/api/v1/cluster/features/audit-unknown-feature"
	status, body, featureRequestID, err := clusterManagementAuditProcessRequest(ctx, http.MethodPost,
		featurePath, nil, clusterMembershipProcessAdmin, "")
	if err != nil || status < http.StatusBadRequest || status >= http.StatusInternalServerError {
		t.Fatalf("node 2 feature mutation status=%d body=%s err=%v, want 4xx", status, body, err)
	}
	var featureFailure struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &featureFailure); err != nil {
		t.Fatalf("decode node 2 feature error %q: %v", body, err)
	}
	if featureFailure.Error.Code == "" {
		t.Fatalf("node 2 feature error has no code: %s", body)
	}

	logPath := filepath.Join(fixture.auditDirectory(2), "audit.jsonl")
	logFile, err := os.Open(logPath)
	if err != nil {
		t.Fatalf("open node 2 audit log %q: %v", logPath, err)
	}
	scanner := bufio.NewScanner(logFile)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var events []clusterAuditProcessEvent
	for scanner.Scan() {
		var record struct {
			Event clusterAuditProcessEvent `json:"event"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			_ = logFile.Close()
			t.Fatalf("decode node 2 audit record: %v", err)
		}
		events = append(events, record.Event)
	}
	scanErr := scanner.Err()
	closeErr := logFile.Close()
	if scanErr != nil || closeErr != nil {
		t.Fatalf("read node 2 audit log: scan=%v close=%v", scanErr, closeErr)
	}
	if len(events) != 2 {
		t.Fatalf("node 2 audit log has %d events, want exactly 2", len(events))
	}
	denied := events[0]
	if denied.RequestID != unauthorizedRequestID || denied.Action != "management.request" ||
		denied.Result != "DENIED" || denied.Reason != "unauthorized" ||
		denied.Actor.Subject != "unauthenticated" || len(denied.Actor.Roles) != 0 ||
		denied.Target.Type != "management_endpoint" || denied.Target.ID != "/api/v1/cluster" ||
		denied.Details.HTTPStatus != http.StatusUnauthorized || denied.Details.Method != http.MethodGet {
		t.Fatalf("node 2 unauthorized event=%+v requestId=%q", denied, unauthorizedRequestID)
	}
	failed := events[1]
	if failed.RequestID != featureRequestID || failed.Action != "cluster.feature.activate" ||
		failed.Result != "FAILED" || failed.Reason != featureFailure.Error.Code ||
		failed.Actor.Subject != "management-bearer-token" || len(failed.Actor.Roles) != 1 ||
		failed.Actor.Roles[0] != "ClusterAdmin" || failed.Target.Type != "cluster_feature" ||
		failed.Target.ID != "audit-unknown-feature" || failed.Details.HTTPStatus != status ||
		failed.Details.Method != http.MethodPost {
		t.Fatalf("node 2 feature failure event=%+v response=%+v requestId=%q", failed, featureFailure.Error, featureRequestID)
	}
	rawLog, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read node 2 audit log bytes: %v", err)
	}
	for _, secret := range []string{clusterMembershipProcessAdmin, invalidToken} {
		if strings.Contains(string(rawLog), secret) {
			t.Fatalf("node 2 audit log contains secret %q", secret)
		}
	}

	verifyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	verify := exec.CommandContext(verifyCtx, fixture.harness.binary, "-audit-verify", fixture.auditDirectory(2))
	verify.Dir = fixture.repoRoot
	output, err := verify.CombinedOutput()
	if err != nil {
		t.Fatalf("current broker binary audit verification failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "audit verification passed") || !strings.Contains(string(output), "records=2") {
		t.Fatalf("audit verifier output=%q, want successful two-record summary", output)
	}

	tamperedDirectory := t.TempDir()
	tamperedLog := bytes.Replace(rawLog, []byte(`"reason":"unauthorized"`), []byte(`"reason":"forbidden"`), 1)
	if bytes.Equal(tamperedLog, rawLog) {
		t.Fatal("tamper operation did not change an audit event")
	}
	if err := os.WriteFile(filepath.Join(tamperedDirectory, "audit.jsonl"), tamperedLog, 0o600); err != nil {
		t.Fatalf("write tampered audit copy: %v", err)
	}
	verifyTampered := exec.CommandContext(verifyCtx, fixture.harness.binary, "-audit-verify", tamperedDirectory)
	verifyTampered.Dir = fixture.repoRoot
	tamperedOutput, err := verifyTampered.CombinedOutput()
	if err == nil || !strings.Contains(string(tamperedOutput), "audit.jsonl:1") {
		t.Fatalf("tampered audit verification error=%v output=%q, want filename and line", err, tamperedOutput)
	}
}

type clusterAuditProcessEvent struct {
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
	Result  string `json:"result"`
	Reason  string `json:"reason"`
	Details struct {
		HTTPStatus int    `json:"httpStatus"`
		Method     string `json:"method"`
	} `json:"details"`
}

func clusterManagementAuditProcessRequest(ctx context.Context, method, requestURL string, body []byte, token, idempotencyKey string) (int, []byte, string, error) {
	request, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		return 0, nil, "", err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if len(body) != 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, "", err
	}
	responseBody, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return response.StatusCode, nil, response.Header.Get("X-Request-Id"), readErr
	}
	if closeErr != nil {
		return response.StatusCode, responseBody, response.Header.Get("X-Request-Id"), closeErr
	}
	return response.StatusCode, responseBody, response.Header.Get("X-Request-Id"), nil
}
