package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authentication "narumq/internal/adapters/authentication"
)

// Traceability: SEC-05; docs/specs/tenancy-and-security.md §2.3, §2.4, §9.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-29, mqtt-protocol-and-session.md:MQTT-SCN-021, tenancy-and-security.md:SEC-SCN-014, tenancy-and-security.md:SEC-SCN-017
func TestDynamicClusterPasswordAuthenticationProcessAcceptance(t *testing.T) {
	t.Parallel()
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	passwordA := []byte("SEC05-sensitive-password-A")
	passwordB := []byte("SEC05-sensitive-password-B")
	wrongPassword := []byte("SEC05-wrong-password")
	unknownPassword := []byte("SEC05-unknown-user-password")

	hashCommandContext, cancelHashCommand := context.WithTimeout(fixture.ctx, 30*time.Second)
	defer cancelHashCommand()
	hashCommand := exec.CommandContext(hashCommandContext, fixture.binary, "-hash-password")
	hashCommand.Stdin = bytes.NewReader(passwordA)
	hashOutput, err := hashCommand.Output()
	if err != nil {
		t.Fatalf("run -hash-password for tenant A: %v", err)
	}
	if len(hashOutput) < 2 || hashOutput[len(hashOutput)-1] != '\n' {
		t.Fatalf("-hash-password output does not contain one terminated hash line")
	}
	hashA := string(hashOutput[:len(hashOutput)-1])
	hashB, err := authentication.HashPassword(passwordB)
	if err != nil {
		t.Fatalf("hash tenant B password: %v", err)
	}

	sensitiveValues := [][]byte{
		passwordA, passwordB, wrongPassword, unknownPassword, []byte(hashA), []byte(hashB),
	}

	conflictingCommand := exec.CommandContext(fixture.ctx, fixture.binary, "-hash-password", "-cluster-config", "unused.json")
	conflictingOutput, conflictingErr := conflictingCommand.CombinedOutput()
	if conflictingErr == nil || !strings.Contains(string(conflictingOutput), "-hash-password cannot be combined with -cluster-config") {
		t.Fatalf("conflicting hash command error=%v output=%q", conflictingErr, conflictingOutput)
	}

	type passwordIdentity struct {
		Username         string   `json:"username"`
		PasswordHash     string   `json:"passwordHash"`
		TenantID         string   `json:"tenantId"`
		Subject          string   `json:"subject"`
		CredentialID     string   `json:"credentialId"`
		AllowedClientIDs []string `json:"allowedClientIds"`
		Roles            []string `json:"roles"`
	}
	type aclRule struct {
		TenantID    string `json:"tenantId"`
		Role        string `json:"role"`
		Effect      string `json:"effect"`
		Action      string `json:"action"`
		TopicFilter string `json:"topicFilter"`
	}
	securityConfig := struct {
		PasswordIdentities []passwordIdentity `json:"passwordIdentities"`
		ACL                []aclRule          `json:"acl"`
	}{
		PasswordIdentities: []passwordIdentity{
			{
				Username: "sec05-user-a", PasswordHash: hashA, TenantID: "tenant-a", Subject: "device-a", CredentialID: "sec05-credential-a",
				AllowedClientIDs: clusterPasswordProcessAllowedClientIDs("tenant-a"), Roles: []string{"client"},
			},
			{
				Username: "sec05-user-b", PasswordHash: hashB, TenantID: "tenant-b", Subject: "device-b", CredentialID: "sec05-credential-b",
				AllowedClientIDs: clusterPasswordProcessAllowedClientIDs("tenant-b"), Roles: []string{"client"},
			},
		},
		ACL: []aclRule{
			{TenantID: "tenant-a", Role: "client", Effect: "allow", Action: "publish", TopicFilter: "sec05/isolation/#"},
			{TenantID: "tenant-a", Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "sec05/isolation/#"},
			{TenantID: "tenant-b", Role: "client", Effect: "allow", Action: "publish", TopicFilter: "sec05/isolation/#"},
			{TenantID: "tenant-b", Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "sec05/isolation/#"},
		},
	}
	securityBytes, err := json.MarshalIndent(securityConfig, "", "  ")
	if err != nil {
		t.Fatalf("marshal password security config: %v", err)
	}
	if err := os.WriteFile(fixture.securityPath, securityBytes, 0o600); err != nil {
		t.Fatalf("write password security config: %v", err)
	}

	processes := clusterDrainProcessStartCluster(t, fixture)
	versions := []byte{4, 5}
	wantRejectReason := map[byte]byte{4: 0x05, 5: 0x87}
	connectAndClose := func(address, username string, password []byte, clientID string, version byte) byte {
		t.Helper()
		connection, reason, _ := clusterDataPlaneProcessConnect(t, address, username, string(password), clientID, version)
		if err := connection.Close(); err != nil {
			t.Fatalf("close CONNECT probe: %v", err)
		}
		return reason
	}

	for nodeIndex, address := range fixture.mqttAddresses {
		for _, version := range versions {
			clientID := fmt.Sprintf("sec05-a-success-%d-%d", nodeIndex+1, version)
			if reason := connectAndClose(address, "sec05-user-a", passwordA, clientID, version); reason != 0 {
				t.Fatalf("node %d MQTT %d valid password CONNACK reason=0x%02x, want success", nodeIndex+1, version, reason)
			}
		}
	}

	for nodeIndex, address := range fixture.mqttAddresses {
		for _, version := range versions {
			wrongClientID := fmt.Sprintf("sec05-wrong-%d-%d", nodeIndex+1, version)
			wrongReason := connectAndClose(address, "sec05-user-a", wrongPassword, wrongClientID, version)
			unknownReason := connectAndClose(address, "sec05-unknown-user", unknownPassword, fmt.Sprintf("sec05-unknown-%d-%d", nodeIndex+1, version), version)
			if wrongReason != wantRejectReason[version] || unknownReason != wrongReason {
				t.Fatalf("node %d MQTT %d wrong-password reason=0x%02x, unknown-user reason=0x%02x; want same 0x%02x", nodeIndex+1, version, wrongReason, unknownReason, wantRejectReason[version])
			}
			foreignClientID := fmt.Sprintf("sec05-b-only-client-%d", version)
			clientIDReason := connectAndClose(address, "sec05-user-a", passwordA, foreignClientID, version)
			if clientIDReason != wantRejectReason[version] {
				t.Fatalf("node %d MQTT %d foreign Client ID reason=0x%02x, want 0x%02x", nodeIndex+1, version, clientIDReason, wantRejectReason[version])
			}
		}
	}

	for nodeIndex, process := range processes {
		status, metricsBody, err := clusterMembershipProcessRequest(fixture.ctx, http.MethodGet, process.managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "")
		if err != nil || status != http.StatusOK {
			t.Fatalf("node %d metrics status=%d error=%v body=%s", nodeIndex+1, status, err, metricsBody)
		}
		metrics := string(metricsBody)
		clusterPasswordProcessAssertNoSensitiveValues(t, fmt.Sprintf("node %d metrics", nodeIndex+1), metricsBody, sensitiveValues)
		for _, expected := range []string{
			`narumq_auth_attempts_total{method="password",result="success",reason="none"} 2`,
			`narumq_auth_attempts_total{method="password",result="failure",reason="invalid_credentials"} 4`,
			`narumq_auth_attempts_total{method="password",result="failure",reason="client_id_not_allowed"} 2`,
		} {
			if !strings.Contains(metrics, expected) {
				t.Fatalf("node %d missing authentication metric %q: %s", nodeIndex+1, expected, metrics)
			}
		}
	}

	var liveConnections []net.Conn
	closeLiveConnections := func() {
		for _, connection := range liveConnections {
			_ = connection.Close()
		}
		liveConnections = nil
	}
	defer closeLiveConnections()
	connectLive := func(address, username string, password []byte, clientID string) net.Conn {
		t.Helper()
		connection, reason, _ := clusterDataPlaneProcessConnect(t, address, username, string(password), clientID, 4)
		if reason != 0 {
			_ = connection.Close()
			t.Fatalf("MQTT 3.1.1 password CONNECT for %q reason=0x%02x, want success", clientID, reason)
		}
		liveConnections = append(liveConnections, connection)
		return connection
	}
	const isolationFilter = "sec05/isolation/#"
	const isolationTopic = "sec05/isolation/value"
	aSubscriber := connectLive(fixture.mqttAddresses[0], "sec05-user-a", passwordA, "sec05-a-subscriber")
	bSubscriber := connectLive(fixture.mqttAddresses[1], "sec05-user-b", passwordB, "sec05-b-subscriber")
	clusterDurableProcessSubscribe(t, aSubscriber, 1, isolationFilter, 0)
	clusterDurableProcessSubscribe(t, bSubscriber, 1, isolationFilter, 0)
	clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, processes, "tenant-a", "sec05-a-subscriber|"+isolationFilter+"|0")
	clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, processes, "tenant-b", "sec05-b-subscriber|"+isolationFilter+"|0")
	aPublisher := connectLive(fixture.mqttAddresses[2], "sec05-user-a", passwordA, "sec05-a-publisher")
	bPublisher := connectLive(fixture.mqttAddresses[0], "sec05-user-b", passwordB, "sec05-b-publisher")
	clusterDurableProcessPublish(t, aPublisher, isolationTopic, 0, 0, "tenant-a-payload")
	aDelivery := clusterMembershipProcessReadPublishes(t, aSubscriber, 1)
	clusterPasswordProcessPingBarrier(t, bSubscriber)
	if aDelivery[0] != [2]string{isolationTopic, "tenant-a-payload"} {
		t.Fatalf("tenant A delivery = %v", aDelivery[0])
	}
	clusterDurableProcessPublish(t, bPublisher, isolationTopic, 0, 0, "tenant-b-payload")
	bDelivery := clusterMembershipProcessReadPublishes(t, bSubscriber, 1)
	clusterPasswordProcessPingBarrier(t, aSubscriber)
	if bDelivery[0] != [2]string{isolationTopic, "tenant-b-payload"} {
		t.Fatalf("tenant B delivery = %v", bDelivery[0])
	}

	for nodeIndex := range processes {
		wantClients := make(map[string]string)
		for _, version := range versions {
			wantClients[fmt.Sprintf("sec05-wrong-%d-%d", nodeIndex+1, version)] = "invalid_credentials"
			wantClients[fmt.Sprintf("sec05-unknown-%d-%d", nodeIndex+1, version)] = "invalid_credentials"
			wantClients[fmt.Sprintf("sec05-b-only-client-%d", version)] = "client_id_not_allowed"
		}
		auditDirectory := fixture.auditDirectory(uint64(nodeIndex + 1))
		events := clusterPasswordProcessWaitForConnectAudit(t, auditDirectory, 6)
		auditBytes, err := os.ReadFile(filepath.Join(auditDirectory, "audit.jsonl"))
		if err != nil {
			t.Fatalf("read node %d audit log: %v", nodeIndex+1, err)
		}
		clusterPasswordProcessAssertNoSensitiveValues(t, fmt.Sprintf("node %d audit log", nodeIndex+1), auditBytes, sensitiveValues)
		reasons := make(map[string]int)
		for _, event := range events {
			wantReason, exists := wantClients[event.Target.ID]
			if !exists || event.Action != "mqtt.connect" || event.Result != "DENIED" || event.Reason != wantReason || event.Details.AuthMethod != "password" {
				t.Fatalf("node %d password denial audit event = %+v", nodeIndex+1, event)
			}
			delete(wantClients, event.Target.ID)
			reasons[event.Reason]++
		}
		if len(wantClients) != 0 || reasons["invalid_credentials"] != 4 || reasons["client_id_not_allowed"] != 2 {
			t.Fatalf("node %d denial audit clients=%v reasons=%v; want four invalid credentials and two Client ID denials", nodeIndex+1, wantClients, reasons)
		}
	}

	closeLiveConnections()
	for index, process := range processes {
		if err := process.stop(10 * time.Second); err != nil {
			t.Fatalf("stop cluster node %d: %v", index+1, err)
		}
	}
	clusterPasswordProcessAssertNoPlainSecrets(t, fixture.tempDir, [][]byte{passwordA, passwordB, wrongPassword, unknownPassword})
	for index, process := range processes {
		clusterPasswordProcessAssertNoSensitiveValues(t, fmt.Sprintf("node %d process log", index+1), []byte(process.logs.String()), sensitiveValues)
	}
}

func clusterPasswordProcessAllowedClientIDs(tenant string) []string {
	suffix := strings.TrimPrefix(tenant, "tenant-")
	clientIDs := []string{
		"sec05-" + suffix + "-only-client-4",
		"sec05-" + suffix + "-only-client-5",
		"sec05-" + suffix + "-subscriber",
		"sec05-" + suffix + "-publisher",
	}
	for node := 1; node <= 3; node++ {
		for _, version := range []byte{4, 5} {
			clientIDs = append(clientIDs, fmt.Sprintf("sec05-%s-success-%d-%d", suffix, node, version))
		}
	}
	return clientIDs
}

func clusterPasswordProcessPingBarrier(t *testing.T, connection net.Conn) {
	t.Helper()
	clusterDataPlaneProcessWritePacket(t, connection, 0xC0, nil)
	header, body, err := clusterMembershipProcessReadMQTTPacket(connection)
	if err != nil || header != 0xD0 || len(body) != 0 {
		t.Fatalf("MQTT PINGRESP header=0x%02x body=% X err=%v", header, body, err)
	}
}

func clusterPasswordProcessWaitForConnectAudit(t *testing.T, directory string, want int) []clusterAuditProcessEvent {
	t.Helper()
	logPath := filepath.Join(directory, "audit.jsonl")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(logPath)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("read password authentication audit log: %v", err)
		}
		var events []clusterAuditProcessEvent
		if err == nil {
			completeLineEnd := bytes.LastIndexByte(data, '\n')
			if completeLineEnd >= 0 {
				for _, line := range bytes.Split(data[:completeLineEnd], []byte{'\n'}) {
					if len(line) == 0 {
						continue
					}
					var record struct {
						Event clusterAuditProcessEvent `json:"event"`
					}
					if err := json.Unmarshal(line, &record); err != nil {
						t.Fatalf("decode password authentication audit event: %v", err)
					}
					if record.Event.Action == "mqtt.connect" {
						events = append(events, record.Event)
					}
				}
			}
		}
		if len(events) == want {
			return events
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("password authentication audit event count for %s did not reach %d", directory, want)
	return nil
}

func clusterPasswordProcessAssertNoSensitiveValues(t *testing.T, location string, data []byte, values [][]byte) {
	t.Helper()
	for _, value := range values {
		if bytes.Contains(data, value) {
			t.Errorf("%s contains sensitive authentication material", location)
			return
		}
	}
}

func clusterPasswordProcessAssertNoPlainSecrets(t *testing.T, root string, secrets [][]byte) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, secret := range secrets {
			if bytes.Contains(data, secret) {
				t.Errorf("file %s contains a plaintext password", path)
				break
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan process, config, audit, and Raft files for plaintext passwords: %v", err)
	}
}
