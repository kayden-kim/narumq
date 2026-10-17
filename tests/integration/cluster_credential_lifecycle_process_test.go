package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	authentication "narumq/internal/adapters/authentication"
)

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; ADR-0049 §7.
// Scenarios: tenancy-and-security.md:SEC-SCN-034, tenancy-and-security.md:SEC-SCN-035
func TestDynamicClusterCredentialRevocationProcessAcceptance(t *testing.T) {
	fixture := newClusterMembershipProcessFixture(t, 3, "")

	oldPassword := []byte("SEC07-revoked-password")
	newPassword := []byte("SEC07-current-password")
	oldHash, err := authentication.HashPassword(oldPassword)
	if err != nil {
		t.Fatalf("hash revoked credential password: %v", err)
	}
	newHash, err := authentication.HashPassword(newPassword)
	if err != nil {
		t.Fatalf("hash current credential password: %v", err)
	}
	clientIDs := []string{
		"sec07-old-mqtt311", "sec07-old-mqtt5", "sec07-watcher",
		"sec07-denied-1-4", "sec07-denied-1-5", "sec07-denied-2-4", "sec07-denied-2-5", "sec07-denied-3-4", "sec07-denied-3-5",
		"sec07-new-1-4", "sec07-new-1-5", "sec07-new-2-4", "sec07-new-2-5", "sec07-new-3-4", "sec07-new-3-5",
		"sec07-restored-1", "sec07-restored-2", "sec07-restored-3",
	}
	passwordIdentities := []map[string]any{
		{
			"username": "sec07-old", "passwordHash": oldHash, "tenantId": "sec07", "subject": "device-1",
			"credentialId": "sec07-credential-old", "allowedClientIds": clientIDs, "roles": []string{"client"},
		},
		{
			"username": "sec07-new", "passwordHash": newHash, "tenantId": "sec07", "subject": "device-1",
			"credentialId": "sec07-credential-new", "allowedClientIds": clientIDs, "roles": []string{"client"},
		},
	}
	securityConfig := map[string]any{
		"passwordIdentities": passwordIdentities,
		"acl": []map[string]string{
			{"tenantId": "sec07", "role": "client", "effect": "allow", "action": "publish", "topicFilter": "sec07/#"},
			{"tenantId": "sec07", "role": "client", "effect": "allow", "action": "subscribe", "topicFilter": "sec07/#"},
			{"tenantId": "sec07", "role": "client", "effect": "allow", "action": "will", "topicFilter": "sec07/#"},
		},
	}
	sec07WriteSecurityConfig(t, fixture, securityConfig)

	processes := clusterDrainProcessStartCluster(t, fixture)
	clusterProcessActivateFeature(t, fixture.ctx, processes, "credential-revocation")

	watcher, reason, _ := clusterDataPlaneProcessConnect(t, fixture.mqttAddresses[2], "sec07-new", string(newPassword), "sec07-watcher", 4)
	if reason != 0 {
		_ = watcher.Close()
		t.Fatalf("new credential watcher CONNACK reason=0x%02x, want success", reason)
	}
	defer watcher.Close()
	clusterDurableProcessSubscribe(t, watcher, 1, "sec07/will/#", 0)
	clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, processes, "sec07", "sec07-watcher|sec07/will/#|0")

	oldMQTT311, reason := sec07ConnectWithWill(t, fixture.mqttAddresses[0], "sec07-old", string(oldPassword), "sec07-old-mqtt311", "sec07/will/old-mqtt311", "must-not-publish", 4)
	if reason != 0 {
		_ = oldMQTT311.Close()
		t.Fatalf("old MQTT 3.1.1 CONNECT reason=0x%02x, want success", reason)
	}
	defer oldMQTT311.Close()
	oldMQTT5, reason := sec07ConnectWithWill(t, fixture.mqttAddresses[1], "sec07-old", string(oldPassword), "sec07-old-mqtt5", "sec07/will/old-mqtt5", "must-not-publish", 5)
	if reason != 0 {
		_ = oldMQTT5.Close()
		t.Fatalf("old MQTT 5 CONNECT reason=0x%02x, want success", reason)
	}
	defer oldMQTT5.Close()

	revokeURL := processes[0].managementURL() + "/api/v1/security/credential-revocations/sec07-credential-old"
	status, body, err := clusterMembershipProcessRequest(fixture.ctx, http.MethodPost, revokeURL, nil, clusterMembershipProcessAdmin, "sec07-revoke-old-credential")
	if err != nil || status != http.StatusOK {
		t.Fatalf("revoke old credential status=%d body=%s error=%v", status, body, err)
	}
	var revokeResponse struct {
		Data struct {
			CredentialID string `json:"credentialId"`
			Revoked      bool   `json:"revoked"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &revokeResponse); err != nil || revokeResponse.Data.CredentialID != "sec07-credential-old" || !revokeResponse.Data.Revoked {
		t.Fatalf("revoke response=%s decode error=%v", body, err)
	}
	for nodeIndex, process := range processes {
		status, body, err := clusterMembershipProcessRequest(fixture.ctx, http.MethodGet,
			process.managementURL()+"/api/v1/security/credential-revocations", nil, clusterMembershipProcessAdmin, "")
		if err != nil || status != http.StatusOK {
			t.Fatalf("node %d revocation list status=%d body=%s error=%v", nodeIndex+1, status, body, err)
		}
		var list struct {
			Data struct {
				Revocations []struct {
					CredentialID string `json:"credentialId"`
				} `json:"revocations"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &list); err != nil || len(list.Data.Revocations) != 1 || list.Data.Revocations[0].CredentialID != "sec07-credential-old" {
			t.Fatalf("node %d committed revocation list=%s decode error=%v", nodeIndex+1, body, err)
		}
	}

	sec07AssertDisconnect(t, oldMQTT311, 4, 0)
	sec07AssertDisconnect(t, oldMQTT5, 5, 0x87)
	for nodeIndex, address := range fixture.mqttAddresses {
		for _, version := range []byte{4, 5} {
			clientID := fmt.Sprintf("sec07-denied-%d-%d", nodeIndex+1, version)
			connection, reason, _ := clusterDataPlaneProcessConnect(t, address, "sec07-old", string(oldPassword), clientID, version)
			_ = connection.Close()
			want := map[byte]byte{4: 0x05, 5: 0x87}[version]
			if reason != want {
				t.Fatalf("node %d MQTT %d revoked credential CONNACK reason=0x%02x, want 0x%02x", nodeIndex+1, version, reason, want)
			}
			connection, reason, _ = clusterDataPlaneProcessConnect(t, address, "sec07-new", string(newPassword), fmt.Sprintf("sec07-new-%d-%d", nodeIndex+1, version), version)
			_ = connection.Close()
			if reason != 0 {
				t.Fatalf("node %d MQTT %d replacement credential CONNACK reason=0x%02x, want success", nodeIndex+1, version, reason)
			}
		}
	}

	clusterMembershipProcessWait(t, fixture.ctx, "credential revocation CONNECT audit", func() (bool, string, error) {
		for nodeIndex := range processes {
			events, err := sec07ReadAuditEvents(fixture.auditDirectory(uint64(nodeIndex + 1)))
			if err != nil {
				return false, err.Error(), nil
			}
			for _, version := range []byte{4, 5} {
				clientID := fmt.Sprintf("sec07-denied-%d-%d", nodeIndex+1, version)
				if !sec07HasAuditEvent(events, "credential_revoked", clientID) {
					return false, fmt.Sprintf("node %d has no credential_revoked CONNECT audit for %s", nodeIndex+1, clientID), nil
				}
			}
		}
		return true, "all nodes recorded revoked CONNECT denials", nil
	})
	sec07AssertNoWill(t, watcher)

	restoreURL := processes[0].managementURL() + "/api/v1/security/credential-revocations/sec07-credential-old"
	status, body, err = clusterMembershipProcessRequest(fixture.ctx, http.MethodDelete, restoreURL, nil, clusterMembershipProcessAdmin, "sec07-restore-old-credential")
	if err != nil || status != http.StatusOK {
		t.Fatalf("restore old credential status=%d body=%s error=%v", status, body, err)
	}
	for nodeIndex, address := range fixture.mqttAddresses {
		connection, reason, _ := clusterDataPlaneProcessConnect(t, address, "sec07-old", string(oldPassword), fmt.Sprintf("sec07-restored-%d", nodeIndex+1), 4)
		_ = connection.Close()
		if reason != 0 {
			t.Fatalf("node %d restored credential CONNACK reason=0x%02x, want success", nodeIndex+1, reason)
		}
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7, §9; ADR-0049 §7.
// Scenarios: tenancy-and-security.md:SEC-SCN-038
func TestDynamicClusterCredentialExpiryProcessAcceptance(t *testing.T) {
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	issuer := newClusterProcessJWTIssuer(t, "sec07-expiry-key")
	securityConfig := map[string]any{
		"jwt": map[string]any{
			"issuer": "https://issuer.example", "audience": "narumq", "algorithms": []string{"RS256"},
			"jwksUrl": issuer.jwksURL, "cacheTtlMillis": 300000, "refreshCooldownMillis": 30000, "clockSkewMillis": 1000, "httpTimeoutMillis": 5000,
		},
		"identities": []map[string]any{
			{
				"issuer": "https://issuer.example", "subject": "expiring-device", "tenantId": "sec07",
				"credentialId": "sec07-jwt-expiring", "allowedClientIds": []string{"sec07-expiring-mqtt311", "sec07-expiring-mqtt5"}, "roles": []string{"client"},
			},
			{
				"issuer": "https://issuer.example", "subject": "will-watcher", "tenantId": "sec07",
				"credentialId": "sec07-jwt-watcher", "allowedClientIds": []string{"sec07-expiry-watcher"}, "roles": []string{"client"},
			},
		},
		"acl": []map[string]string{
			{"tenantId": "sec07", "role": "client", "effect": "allow", "action": "publish", "topicFilter": "sec07/#"},
			{"tenantId": "sec07", "role": "client", "effect": "allow", "action": "subscribe", "topicFilter": "sec07/#"},
			{"tenantId": "sec07", "role": "client", "effect": "allow", "action": "will", "topicFilter": "sec07/#"},
		},
	}
	sec07WriteSecurityConfig(t, fixture, securityConfig)
	processes := clusterDrainProcessStartCluster(t, fixture)

	watcherToken := issuer.sign(t, "will-watcher", time.Now().Add(time.Hour), nil)
	watcher, reason, _ := clusterDataPlaneProcessConnect(t, fixture.mqttAddresses[2], "jwt", watcherToken, "sec07-expiry-watcher", 4)
	if reason != 0 {
		_ = watcher.Close()
		t.Fatalf("expiry watcher CONNACK reason=0x%02x, want success", reason)
	}
	defer watcher.Close()
	clusterDurableProcessSubscribe(t, watcher, 1, "sec07/will/#", 0)
	clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, processes, "sec07", "sec07-expiry-watcher|sec07/will/#|0")

	expiresAt := time.Now().Add(8 * time.Second)
	expiringToken := issuer.sign(t, "expiring-device", expiresAt, nil)
	mqtt311, reason := sec07ConnectWithWill(t, fixture.mqttAddresses[0], "jwt", expiringToken, "sec07-expiring-mqtt311", "sec07/will/expiry-mqtt311", "expired-311", 4)
	if reason != 0 {
		_ = mqtt311.Close()
		t.Fatalf("expiring MQTT 3.1.1 CONNECT reason=0x%02x, want success", reason)
	}
	defer mqtt311.Close()
	mqtt5, reason := sec07ConnectWithWill(t, fixture.mqttAddresses[1], "jwt", expiringToken, "sec07-expiring-mqtt5", "sec07/will/expiry-mqtt5", "expired-5", 5)
	if reason != 0 {
		_ = mqtt5.Close()
		t.Fatalf("expiring MQTT 5 CONNECT reason=0x%02x, want success", reason)
	}
	defer mqtt5.Close()

	sec07AssertDisconnect(t, mqtt311, 4, 0)
	sec07AssertDisconnect(t, mqtt5, 5, 0xA0)
	for _, want := range []struct{ topic, payload string }{
		{"sec07/will/expiry-mqtt311", "expired-311"},
		{"sec07/will/expiry-mqtt5", "expired-5"},
	} {
		packet := clusterDurableProcessRead(t, watcher)
		if packet.kind != 3 || packet.topic != want.topic || packet.payload != want.payload {
			t.Fatalf("expiry Will packet=%+v, want PUBLISH topic=%q payload=%q", packet, want.topic, want.payload)
		}
	}
}

func sec07WriteSecurityConfig(t *testing.T, fixture *clusterMembershipProcessFixture, config any) {
	t.Helper()
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatalf("encode SEC-07 security config: %v", err)
	}
	if err := os.WriteFile(fixture.securityPath, data, 0o600); err != nil {
		t.Fatalf("write SEC-07 security config: %v", err)
	}
}

func sec07ConnectWithWill(t *testing.T, address, username, password, clientID, willTopic, willPayload string, version byte) (net.Conn, byte) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("dial MQTT node %s: %v", address, err)
	}
	if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = connection.Close()
		t.Fatalf("set SEC-07 CONNECT deadline: %v", err)
	}
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', version, 0xC6, 0, 60}
	if version == 5 {
		body = append(body, 0)
	}
	body = sec07AppendMQTTString(body, clientID)
	if version == 5 {
		body = append(body, 0)
	}
	body = sec07AppendMQTTString(body, willTopic)
	body = sec07AppendMQTTString(body, willPayload)
	body = sec07AppendMQTTString(body, username)
	body = sec07AppendMQTTString(body, password)
	clusterDataPlaneProcessWritePacket(t, connection, 0x10, body)
	header, response, err := clusterMembershipProcessReadMQTTPacket(connection)
	minimumResponseLength := 2
	if version == 5 {
		minimumResponseLength = 3
	}
	if err != nil || header != 0x20 || len(response) < minimumResponseLength {
		_ = connection.Close()
		t.Fatalf("read MQTT %d SEC-07 CONNACK header=0x%02x body=% X err=%v", version, header, response, err)
	}
	if err := connection.SetDeadline(time.Time{}); err != nil {
		_ = connection.Close()
		t.Fatalf("clear SEC-07 CONNECT deadline: %v", err)
	}
	return connection, response[1]
}

func sec07AppendMQTTString(body []byte, value string) []byte {
	return append(append(body, byte(len(value)>>8), byte(len(value))), value...)
}

func sec07AssertDisconnect(t *testing.T, connection net.Conn, version, wantReason byte) {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(12 * time.Second)); err != nil {
		t.Fatalf("set MQTT %d lifecycle read deadline: %v", version, err)
	}
	defer connection.SetReadDeadline(time.Time{})
	header, body, err := clusterMembershipProcessReadMQTTPacket(connection)
	if version == 5 {
		if err != nil || header != 0xE0 || len(body) == 0 || body[0] != wantReason {
			t.Fatalf("MQTT 5 DISCONNECT header=0x%02x body=% X err=%v, want reason 0x%02x", header, body, err, wantReason)
		}
		return
	}
	if err == nil || !clusterMembershipProcessIsClosed(err) {
		t.Fatalf("MQTT 3.1.1 connection read header=0x%02x body=% X err=%v, want immediate socket close", header, body, err)
	}
}

func sec07AssertNoWill(t *testing.T, connection net.Conn) {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(2500 * time.Millisecond)); err != nil {
		t.Fatalf("set Will suppression deadline: %v", err)
	}
	defer connection.SetReadDeadline(time.Time{})
	header, body, err := clusterMembershipProcessReadMQTTPacket(connection)
	if err == nil {
		t.Fatalf("revoked credential Will reached watcher as packet 0x%02x body=% X", header, body)
	}
	networkError, ok := err.(net.Error)
	if !ok || !networkError.Timeout() {
		t.Fatalf("Will watcher failed instead of remaining idle: %v", err)
	}
}

func sec07ReadAuditEvents(directory string) ([]clusterAuditProcessEvent, error) {
	data, err := os.ReadFile(filepath.Join(directory, "audit.jsonl"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var events []clusterAuditProcessEvent
	if lastCompleteLine := bytes.LastIndexByte(data, '\n'); lastCompleteLine >= 0 {
		for _, line := range bytes.Split(data[:lastCompleteLine], []byte{'\n'}) {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var record struct {
				Event clusterAuditProcessEvent `json:"event"`
			}
			if err := json.Unmarshal(line, &record); err != nil {
				return nil, fmt.Errorf("decode audit record: %w", err)
			}
			events = append(events, record.Event)
		}
	}
	return events, nil
}

func sec07HasAuditEvent(events []clusterAuditProcessEvent, reason, clientID string) bool {
	for _, event := range events {
		if event.Reason == reason && event.Target.ID == clientID {
			return true
		}
	}
	return false
}
