package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Traceability: SEC-11/API-08/NFR-17; docs/specs/tenancy-and-security.md §9.
// Scenarios: management-api-and-observability.md:MGT-SCN-012, management-api-and-observability.md:MGT-SCN-013, tenancy-and-security.md:SEC-SCN-046, tenancy-and-security.md:SEC-SCN-050
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
	drainBody := []byte(`{"connectionRatePerSecond":2,"loadBalancerPropagationDelayMillis":1500,"timeoutMillis":60000}`)
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

// Traceability: SEC-11/NFR-17; docs/specs/tenancy-and-security.md §8.
// Scenarios: tenancy-and-security.md:SEC-SCN-047, tenancy-and-security.md:SEC-SCN-048
func TestDynamicClusterMQTTDenialAuditProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	processes := clusterDrainProcessStartCluster(t, fixture)
	ctx := fixture.ctx
	node := processes[1]

	const invalidToken = "invalid-mqtt-audit-token"
	invalidConnection, invalidReason, invalidSourceIP := clusterDataPlaneProcessConnect(t, fixture.mqttAddresses[1], "jwt", invalidToken, "audit-invalid-auth-client", 5)
	_ = invalidConnection.Close()
	if invalidReason != 0x87 {
		t.Fatalf("invalid-token MQTT 5 CONNACK reason = 0x%02x, want 0x87 Not authorized", invalidReason)
	}

	unlistedConnection, unlistedReason, unlistedSourceIP := clusterDataPlaneProcessConnect(t, fixture.mqttAddresses[1], "jwt", fixture.accessToken, "audit-unlisted-client", 5)
	_ = unlistedConnection.Close()
	if unlistedReason != 0x87 {
		t.Fatalf("unlisted-client MQTT 5 CONNACK reason = 0x%02x, want 0x87 Not authorized", unlistedReason)
	}

	readyConnection, readyReason, readySourceIP := clusterDataPlaneProcessConnect(t, fixture.mqttAddresses[1], "jwt", fixture.accessToken, "ready-connection-client", 5)
	if readyReason != 0 {
		_ = readyConnection.Close()
		t.Fatalf("ready MQTT 5 CONNECT reason = 0x%02x, want success", readyReason)
	}
	const deniedFilter = "audit/denied/#"
	subscribeBody := []byte{0, 1, 0}
	subscribeBody = append(subscribeBody, byte(len(deniedFilter)>>8), byte(len(deniedFilter)))
	subscribeBody = append(subscribeBody, deniedFilter...)
	subscribeBody = append(subscribeBody, 0)
	clusterDataPlaneProcessWritePacket(t, readyConnection, 0x82, subscribeBody)
	header, suback, err := clusterMembershipProcessReadMQTTPacket(readyConnection)
	if err != nil || header != 0x90 || len(suback) < 4 || suback[len(suback)-1] != 0x87 {
		_ = readyConnection.Close()
		t.Fatalf("SUBACK header=0x%02x body=% X err=%v; want MQTT 5 not-authorized reason 0x87", header, suback, err)
	}
	const deniedTopic = "audit/denied/topic"
	publishBody := []byte{0, byte(len(deniedTopic))}
	publishBody = append(publishBody, deniedTopic...)
	publishBody = append(publishBody, 0)
	publishBody = append(publishBody, 'x')
	clusterDataPlaneProcessWritePacket(t, readyConnection, 0x30, publishBody)
	clusterDataPlaneProcessWritePacket(t, readyConnection, 0xC0, nil)
	header, pingResponse, err := clusterMembershipProcessReadMQTTPacket(readyConnection)
	if err != nil || header != 0xD0 || len(pingResponse) != 0 {
		_ = readyConnection.Close()
		t.Fatalf("PINGRESP header=0x%02x body=% X err=%v; want packet-ordering barrier", header, pingResponse, err)
	}
	_ = readyConnection.Close()

	logPath := filepath.Join(fixture.auditDirectory(2), "audit.jsonl")
	var events []clusterAuditProcessEvent
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rawLog, readErr := os.ReadFile(logPath)
		if readErr == nil {
			lastCompleteLine := bytes.LastIndexByte(rawLog, '\n')
			if lastCompleteLine >= 0 {
				for _, line := range bytes.Split(rawLog[:lastCompleteLine], []byte{'\n'}) {
					if len(line) == 0 {
						continue
					}
					var record struct {
						Event clusterAuditProcessEvent `json:"event"`
					}
					if err := json.Unmarshal(line, &record); err != nil {
						t.Fatalf("decode node 2 audit event: %v", err)
					}
					if strings.HasPrefix(record.Event.Action, "mqtt.") {
						events = append(events, record.Event)
					}
				}
			}
			if len(events) == 4 {
				break
			}
		} else if !os.IsNotExist(readErr) {
			t.Fatalf("read node 2 audit log: %v", readErr)
		}
		events = nil
		time.Sleep(100 * time.Millisecond)
	}
	if len(events) != 4 {
		rawLog, _ := os.ReadFile(logPath)
		_, metricsBody, metricsErr := clusterMembershipProcessRequest(ctx, http.MethodGet, node.managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "")
		t.Fatalf("node 2 data-plane audit events = %d, want 4: %+v; raw log=%s; metrics error=%v body=%s", len(events), events, rawLog, metricsErr, metricsBody)
	}
	want := []struct {
		action, reason, targetType, targetID, tenantID, sourceIP, subject, credentialID, clientID, authMethod string
		roles                                                                                                 []string
	}{
		{
			action: "mqtt.connect", reason: "invalid_credentials", targetType: "mqtt_client",
			targetID: "audit-invalid-auth-client", sourceIP: invalidSourceIP, subject: "unauthenticated", authMethod: "jwt",
		},
		{
			action: "mqtt.connect", reason: "client_id_not_allowed", targetType: "mqtt_client",
			targetID: "audit-unlisted-client", tenantID: "acceptance", sourceIP: unlistedSourceIP,
			subject: "mqtt-process-client", credentialID: "mqtt-process-client-credential", roles: []string{"client"}, authMethod: "jwt",
		},
		{
			action: "mqtt.subscribe", reason: "not_authorized", targetType: "mqtt_topic_filter",
			targetID: deniedFilter, tenantID: "acceptance", sourceIP: readySourceIP,
			subject: "mqtt-process-client", credentialID: "mqtt-process-client-credential", roles: []string{"client"}, clientID: "ready-connection-client",
		},
		{
			action: "mqtt.publish", reason: "not_authorized", targetType: "mqtt_topic",
			targetID: deniedTopic, tenantID: "acceptance", sourceIP: readySourceIP,
			subject: "mqtt-process-client", credentialID: "mqtt-process-client-credential", roles: []string{"client"}, clientID: "ready-connection-client",
		},
	}
	for index, event := range events {
		expected := want[index]
		if event.Action != expected.action || event.Result != "DENIED" || event.Reason != expected.reason ||
			event.Actor.Subject != expected.subject || event.Actor.TenantID != expected.tenantID || event.Actor.CredentialID != expected.credentialID ||
			!reflect.DeepEqual(event.Actor.Roles, expected.roles) || event.Target.Type != expected.targetType ||
			event.Target.ID != expected.targetID || event.Target.TenantID != expected.tenantID || event.SourceIP != expected.sourceIP ||
			event.Details.AuthMethod != expected.authMethod || event.Details.ClientID != expected.clientID {
			t.Fatalf("data-plane event %d = %+v, want %+v", index+1, event, expected)
		}
	}
	rawLog, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read node 2 audit log bytes: %v", err)
	}
	for _, secret := range []string{fixture.accessToken, invalidToken} {
		if strings.Contains(string(rawLog), secret) {
			t.Fatalf("node 2 audit log contains MQTT credential %q", secret)
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
	if !strings.Contains(string(output), "audit verification passed") || !strings.Contains(string(output), "records=4") {
		t.Fatalf("audit verifier output=%q, want successful four-record summary", output)
	}

	status, metricsBody, err := clusterMembershipProcessRequest(ctx, http.MethodGet, node.managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("node 2 metrics status=%d body=%s err=%v, want 200", status, metricsBody, err)
	}
	metrics := string(metricsBody)
	for _, outcome := range []string{"dataplane_suppressed", "dataplane_dropped"} {
		wantMetric := `narumq_audit_events_total{outcome="` + outcome + `"} 0`
		if !strings.Contains(metrics, wantMetric) {
			t.Fatalf("node 2 MQTT audit metric %q is not zero: %s", outcome, metrics)
		}
	}
}

func clusterDataPlaneProcessConnect(t *testing.T, address, username, password, clientID string, protocolVersion byte) (net.Conn, byte, string) {
	t.Helper()
	if protocolVersion != 4 && protocolVersion != 5 {
		t.Fatalf("unsupported MQTT protocol version %d", protocolVersion)
	}
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("dial MQTT node %s: %v", address, err)
	}
	if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = connection.Close()
		t.Fatalf("set MQTT CONNECT deadline: %v", err)
	}
	sourceIP, _, err := net.SplitHostPort(connection.LocalAddr().String())
	if err != nil {
		_ = connection.Close()
		t.Fatalf("parse MQTT client source address %q: %v", connection.LocalAddr(), err)
	}
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', protocolVersion, 0xC2, 0, 60}
	if protocolVersion == 5 {
		body = append(body, 0)
	}
	for _, field := range []string{clientID, username, password} {
		body = append(body, byte(len(field)>>8), byte(len(field)))
		body = append(body, field...)
	}
	clusterDataPlaneProcessWritePacket(t, connection, 0x10, body)
	header, response, err := clusterMembershipProcessReadMQTTPacket(connection)
	minimumResponseLength := 2
	if protocolVersion == 5 {
		minimumResponseLength = 3
	}
	if err != nil || header != 0x20 || len(response) < minimumResponseLength {
		_ = connection.Close()
		t.Fatalf("read MQTT %d CONNACK header=0x%02x body=% X err=%v", protocolVersion, header, response, err)
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		_ = connection.Close()
		t.Fatalf("clear MQTT CONNECT deadline: %v", err)
	}
	return connection, response[1], sourceIP
}

func clusterDataPlaneProcessWritePacket(t *testing.T, connection net.Conn, header byte, body []byte) {
	t.Helper()
	if _, err := io.Copy(connection, bytes.NewReader(clusterMembershipProcessEncodeMQTTPacket(header, body))); err != nil {
		t.Fatalf("write MQTT packet 0x%02x: %v", header, err)
	}
}

type clusterAuditProcessEvent struct {
	RequestID string `json:"requestId"`
	Actor     struct {
		Subject      string   `json:"subject"`
		TenantID     string   `json:"tenantId"`
		CredentialID string   `json:"credentialId"`
		Roles        []string `json:"roles"`
	} `json:"actor"`
	Action string `json:"action"`
	Target struct {
		Type     string `json:"type"`
		ID       string `json:"id"`
		TenantID string `json:"tenantId"`
	} `json:"target"`
	Result   string `json:"result"`
	Reason   string `json:"reason"`
	SourceIP string `json:"sourceIp"`
	Details  struct {
		HTTPStatus int    `json:"httpStatus"`
		Method     string `json:"method"`
		AuthMethod string `json:"authMethod"`
		ClientID   string `json:"clientId"`
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
