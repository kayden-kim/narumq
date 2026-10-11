package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// Traceability: API-09; CFG-SCN-28; docs/specs/configuration-and-runtime-options.md §6; ADR-0052 §1.1.1.
func TestDynamicClusterPolicyProcessAcceptance(t *testing.T) {
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	initialLimits := clusterPolicyProcessMQTTLimits{
		MaxPacketSize: 8192, ReceiveMaximum: 64, ConnectTimeoutMillis: 10000,
		PacketReadTimeoutMillis: 30000, WriteTimeoutMillis: 5000,
	}
	configPaths := make([]string, 3)
	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		configPaths[index] = fixture.bootstrapConfig(uint64(index + 1))
		clusterPolicyProcessSetDesiredConfig(t, configPaths[index], 1, initialLimits)
		process, err := fixture.harness.start(fmt.Sprintf("API-09 policy node %d", index+1), configPaths[index], fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start policy node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, processes)
	for _, process := range processes {
		clusterMembershipProcessWaitForStatus(t, fixture.ctx, process, "/health/ready", http.StatusOK)
	}

	status, body, err := clusterMembershipProcessRequest(fixture.ctx, http.MethodGet,
		processes[0].managementURL()+"/api/v1/cluster/policy", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("pre-feature policy GET status=%d body=%s err=%v", status, body, err)
	}
	state := clusterPolicyProcessDecodeState(t, body)
	if state.Version != 0 || state.Policy != nil {
		t.Fatalf("pre-feature policy state=%+v, want version 0 and null policy", state)
	}
	inactiveHeaders := clusterPolicyProcessHeaders(`"0"`, "api09-before-feature")
	status, body, err = clusterMembershipProcessRequestWithHeader(fixture.ctx, http.MethodPut,
		processes[0].managementURL()+"/api/v1/cluster/policy", clusterPolicyProcessBody(initialLimits), inactiveHeaders)
	if err != nil || status != http.StatusConflict || !strings.Contains(string(body), `"code":"feature_inactive"`) {
		t.Fatalf("pre-feature policy PUT status=%d body=%s err=%v, want feature_inactive", status, body, err)
	}

	oldConnection := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "metrics-gauge-subscriber", true)
	defer oldConnection.Close()
	clusterProcessActivateFeature(t, fixture.ctx, processes, "cluster-policy")
	clusterPolicyProcessWaitForCommittedState(t, fixture.ctx, processes, 1, initialLimits)

	updatedLimits := initialLimits
	updatedLimits.MaxPacketSize = 2048
	firstHeaders := clusterPolicyProcessHeaders(`"1"`, "api09-policy-v2")
	status, body, err = clusterMembershipProcessRequestWithHeader(fixture.ctx, http.MethodPut,
		processes[0].managementURL()+"/api/v1/cluster/policy", clusterPolicyProcessBody(updatedLimits), firstHeaders)
	if err != nil || status != http.StatusOK {
		t.Fatalf("policy PUT status=%d body=%s err=%v", status, body, err)
	}
	firstState := clusterPolicyProcessDecodeState(t, body)
	if firstState.Version != 2 || firstState.Policy == nil || firstState.Policy.MQTTLimits.MaxPacketSize != updatedLimits.MaxPacketSize {
		t.Fatalf("policy PUT response=%+v body=%s, want committed version 2", firstState, body)
	}

	status, retryBody, err := clusterMembershipProcessRequestWithHeader(fixture.ctx, http.MethodPut,
		processes[0].managementURL()+"/api/v1/cluster/policy", clusterPolicyProcessBody(updatedLimits), firstHeaders)
	if err != nil || status != http.StatusOK {
		t.Fatalf("same-key policy retry status=%d body=%s err=%v", status, retryBody, err)
	}
	retryState := clusterPolicyProcessDecodeState(t, retryBody)
	if retryState.Version != firstState.Version || retryState.Policy == nil || retryState.Policy.MQTTLimits != updatedLimits {
		t.Fatalf("same-key retry state=%+v; first response=%+v", retryState, firstState)
	}

	changedLimits := updatedLimits
	changedLimits.ReceiveMaximum = 32
	status, body, err = clusterMembershipProcessRequestWithHeader(fixture.ctx, http.MethodPut,
		processes[0].managementURL()+"/api/v1/cluster/policy", clusterPolicyProcessBody(changedLimits), firstHeaders)
	if err != nil || status != http.StatusConflict || !strings.Contains(string(body), `"code":"idempotency_conflict"`) {
		t.Fatalf("same-key changed policy status=%d body=%s err=%v, want idempotency_conflict", status, body, err)
	}
	staleHeaders := clusterPolicyProcessHeaders(`"1"`, "api09-stale-version")
	status, body, err = clusterMembershipProcessRequestWithHeader(fixture.ctx, http.MethodPut,
		processes[0].managementURL()+"/api/v1/cluster/policy", clusterPolicyProcessBody(updatedLimits), staleHeaders)
	if err != nil || status != http.StatusConflict || !strings.Contains(string(body), `"code":"cluster_policy_version_conflict"`) {
		t.Fatalf("stale policy PUT status=%d body=%s err=%v, want cluster_policy_version_conflict", status, body, err)
	}

	clusterPolicyProcessWaitForCommittedState(t, fixture.ctx, processes, 2, updatedLimits)
	for _, process := range processes {
		status, metrics, err := clusterMembershipProcessRequest(fixture.ctx, http.MethodGet,
			process.managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "")
		if err != nil || status != http.StatusOK || !strings.Contains(string(metrics), "narumq_cluster_policy_config_mismatch 1") {
			t.Fatalf("node %s policy mismatch metrics status=%d err=%v body=%s", process.name, status, err, metrics)
		}
	}

	largePayload := strings.Repeat("p", 3000)
	clusterPolicyProcessPublishAndAssertOpen(t, oldConnection, largePayload)
	newConnection := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "ready-connection-client", true)
	clusterPolicyProcessPublishAndAssertClosed(t, newConnection, largePayload)
	_ = newConnection.Close()

	if err := processes[2].stop(10 * time.Second); err != nil {
		t.Fatalf("stop policy node 3 for restart: %v", err)
	}
	restarted, err := fixture.harness.start("API-09 policy node 3 restart", configPaths[2], fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart policy node 3: %v", err)
	}
	processes[2] = restarted
	clusterMembershipProcessWaitForStatus(t, fixture.ctx, restarted, "/health/ready", http.StatusOK)
	clusterPolicyProcessWaitForCommittedState(t, fixture.ctx, []*clusterMembershipProcess{restarted}, 2, updatedLimits)
	status, metrics, err := clusterMembershipProcessRequest(fixture.ctx, http.MethodGet,
		restarted.managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK || !strings.Contains(string(metrics), "narumq_cluster_policy_config_mismatch 1") {
		t.Fatalf("restarted policy node mismatch metrics status=%d err=%v body=%s", status, err, metrics)
	}
}

type clusterPolicyProcessMQTTLimits struct {
	MaxPacketSize           uint32 `json:"maxPacketSize"`
	ReceiveMaximum          uint32 `json:"receiveMaximum"`
	ConnectTimeoutMillis    uint32 `json:"connectTimeoutMillis"`
	PacketReadTimeoutMillis uint32 `json:"packetReadTimeoutMillis"`
	WriteTimeoutMillis      uint32 `json:"writeTimeoutMillis"`
}

type clusterPolicyProcessState struct {
	Version uint64                     `json:"version"`
	Policy  *clusterPolicyProcessValue `json:"policy"`
}

type clusterPolicyProcessValue struct {
	MQTTLimits   clusterPolicyProcessMQTTLimits    `json:"mqttLimits"`
	TenantQuotas *clusterPolicyProcessTenantQuotas `json:"tenantQuotas"`
}

func clusterPolicyProcessSetDesiredConfig(t *testing.T, path string, version uint64, limits clusterPolicyProcessMQTTLimits) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read policy node config %q: %v", path, err)
	}
	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatalf("decode policy node config %q: %v", path, err)
	}
	config["clusterPolicy"] = map[string]any{"version": version, "mqttLimits": limits}
	clusterMembershipProcessWriteJSON(t, path, config)
}

func clusterPolicyProcessBody(limits clusterPolicyProcessMQTTLimits) []byte {
	body, _ := json.Marshal(struct {
		MQTTLimits clusterPolicyProcessMQTTLimits `json:"mqttLimits"`
	}{MQTTLimits: limits})
	return body
}

func clusterPolicyProcessHeaders(ifMatch, idempotencyKey string) http.Header {
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+clusterMembershipProcessAdmin)
	headers.Set("If-Match", ifMatch)
	headers.Set("Idempotency-Key", idempotencyKey)
	return headers
}

func clusterPolicyProcessDecodeState(t *testing.T, body []byte) clusterPolicyProcessState {
	t.Helper()
	var envelope struct {
		Data clusterPolicyProcessState `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode cluster policy response: %v body=%s", err, body)
	}
	return envelope.Data
}

func clusterPolicyProcessWaitForCommittedState(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, version uint64, limits clusterPolicyProcessMQTTLimits) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("cluster policy version %d", version), func() (bool, string, error) {
		for _, process := range processes {
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
				process.managementURL()+"/api/v1/cluster/policy", nil, clusterMembershipProcessAdmin, "")
			if err != nil || status != http.StatusOK {
				return false, fmt.Sprintf("%s GET status=%d body=%s err=%v", process.name, status, body, err), nil
			}
			state, decodeErr := clusterPolicyProcessDecodeStateValue(body)
			if decodeErr != nil {
				return false, process.name + " policy decode: " + decodeErr.Error(), nil
			}
			if state.Version != version || state.Policy == nil || state.Policy.MQTTLimits != limits {
				return false, fmt.Sprintf("%s policy version=%d value=%+v, want version=%d limits=%+v", process.name, state.Version, state.Policy, version, limits), nil
			}
		}
		return true, fmt.Sprintf("all %d queried nodes report policy version %d", len(processes), version), nil
	})
}

func clusterPolicyProcessDecodeStateValue(body []byte) (clusterPolicyProcessState, error) {
	var envelope struct {
		Data clusterPolicyProcessState `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return clusterPolicyProcessState{}, err
	}
	return envelope.Data, nil
}

func clusterPolicyProcessPublishAndAssertOpen(t *testing.T, connection net.Conn, payload string) {
	t.Helper()
	clusterPolicyProcessWritePublish(t, connection, payload)
	clusterDurableProcessWriteFatal(t, connection, 0xC0, nil)
	_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer connection.SetReadDeadline(time.Time{})
	kind, _, _, err := clusterDurableProcessReadRaw(connection)
	if err != nil || kind != 13 {
		t.Fatalf("existing MQTT connection ping response kind=%d err=%v; prior packet-size limit should remain active", kind, err)
	}
}

func clusterPolicyProcessPublishAndAssertClosed(t *testing.T, connection net.Conn, payload string) {
	t.Helper()
	clusterPolicyProcessWritePublish(t, connection, payload)
	_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer connection.SetReadDeadline(time.Time{})
	_, _, _, err := clusterDurableProcessReadRaw(connection)
	if err == nil {
		t.Fatal("new MQTT connection remained open after a packet exceeded the committed maximum")
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		t.Fatal("new MQTT connection remained open after a packet exceeded the committed maximum")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		var operationError *net.OpError
		if !errors.As(err, &operationError) {
			t.Fatalf("new MQTT connection closed with unexpected read error: %v", err)
		}
	}
}

func clusterPolicyProcessWritePublish(t *testing.T, connection net.Conn, payload string) {
	t.Helper()
	topic := "live/api09/limits"
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	body = append(body, payload...)
	if err := clusterDurableProcessWrite(connection, 0x30, body); err != nil {
		t.Fatalf("write policy acceptance PUBLISH: %v", err)
	}
}
