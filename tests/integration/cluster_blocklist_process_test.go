package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	authentication "narumq/internal/adapters/authentication"
)

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-041, tenancy-and-security.md:SEC-SCN-042, tenancy-and-security.md:SEC-SCN-043, tenancy-and-security.md:SEC-SCN-044
func TestDynamicClusterClientBlocklistProcessAcceptance(t *testing.T) {
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	password := []byte("SEC12-process-acceptance-password")
	passwordHash, err := authentication.HashPassword(password)
	if err != nil {
		t.Fatalf("hash SEC-12 process password: %v", err)
	}
	clientIDs := []string{"sec12-watcher", "sec12-client-a", "sec12-blocked-live", "sec12-other-1", "sec12-other-2", "sec12-other-3"}
	for node := 1; node <= 3; node++ {
		for _, version := range []byte{4, 5} {
			clientIDs = append(clientIDs, fmt.Sprintf("sec12-denied-user-%d-%d", node, version))
		}
		clientIDs = append(clientIDs, fmt.Sprintf("sec12-ip-denied-%d", node), fmt.Sprintf("sec12-unblocked-%d", node))
	}
	config := map[string]any{
		"passwordIdentities": []map[string]any{
			{"username": "sec12-user", "passwordHash": passwordHash, "tenantId": "sec12", "subject": "sec12-user", "credentialId": "sec12-credential-user", "allowedClientIds": clientIDs, "roles": []string{"client"}},
			{"username": "sec12-blocked-user", "passwordHash": passwordHash, "tenantId": "sec12", "subject": "sec12-blocked-user", "credentialId": "sec12-credential-blocked-user", "allowedClientIds": clientIDs, "roles": []string{"client"}},
			{"username": "sec12-other-user", "passwordHash": passwordHash, "tenantId": "sec12-other", "subject": "sec12-other-user", "credentialId": "sec12-credential-other-user", "allowedClientIds": []string{"sec12-client-a", "sec12-other-1", "sec12-other-2", "sec12-other-3"}, "roles": []string{"client"}},
		},
		"acl": []map[string]string{
			{"tenantId": "sec12", "role": "client", "effect": "allow", "action": "publish", "topicFilter": "sec12/#"},
			{"tenantId": "sec12", "role": "client", "effect": "allow", "action": "subscribe", "topicFilter": "sec12/#"},
			{"tenantId": "sec12", "role": "client", "effect": "allow", "action": "will", "topicFilter": "sec12/#"},
		},
	}
	sec07WriteSecurityConfig(t, fixture, config)
	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		configPath := fixture.bootstrapConfig(uint64(index + 1))
		clusterTenantQuotaProcessSetProfile(t, configPath, clusterTenantQuotaProcessProfile(1))
		process, startErr := fixture.harness.start(
			fmt.Sprintf("SEC-12 bootstrap node %d", index+1), configPath, fixture.managementAddresses[index],
		)
		if startErr != nil {
			t.Fatalf("start SEC-12 bootstrap node %d: %v", index+1, startErr)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, processes)
	for _, process := range processes {
		clusterTenantQuotaProcessWaitForProfile(t, fixture.ctx, process, 1, false)
		clusterMembershipProcessWaitForStatus(t, fixture.ctx, process, "/health/ready", http.StatusOK)
	}
	clusterProcessActivateFeature(t, fixture.ctx, processes, "client-blocklist")

	listURL := "/api/v1/security/blocklist"
	readBlocklist := func(node int) (uint64, []struct {
		Kind      string    `json:"kind"`
		TenantID  string    `json:"tenantId"`
		Value     string    `json:"value"`
		BlockedAt time.Time `json:"blockedAt"`
	}, []byte) {
		t.Helper()
		status, body, requestErr := clusterMembershipProcessRequest(fixture.ctx, http.MethodGet, processes[node].managementURL()+listURL, nil, clusterMembershipProcessAdmin, "")
		if requestErr != nil || status != http.StatusOK {
			t.Fatalf("node %d blocklist GET status=%d body=%s error=%v", node+1, status, body, requestErr)
		}
		var result struct {
			Data struct {
				PolicyVersion uint64 `json:"policyVersion"`
				Entries       []struct {
					Kind      string    `json:"kind"`
					TenantID  string    `json:"tenantId"`
					Value     string    `json:"value"`
					BlockedAt time.Time `json:"blockedAt"`
				} `json:"entries"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("decode node %d blocklist: %v; body=%s", node+1, err, body)
		}
		return result.Data.PolicyVersion, result.Data.Entries, body
	}
	for node := range processes {
		version, entries, body := readBlocklist(node)
		if version != 0 || len(entries) != 0 {
			t.Fatalf("node %d initial blocklist=%s, want policyVersion 0 and no entries", node+1, body)
		}
	}

	watcher, reason, _ := clusterDataPlaneProcessConnect(t, fixture.mqttAddresses[2], "sec12-user", string(password), "sec12-watcher", 4)
	if reason != 0 {
		_ = watcher.Close()
		t.Fatalf("watcher CONNACK reason=0x%02x, want success", reason)
	}
	defer watcher.Close()
	clusterDurableProcessSubscribe(t, watcher, 1, "sec12/will/#", 0)
	clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, processes, "sec12", "sec12-watcher|sec12/will/#|0")

	liveClient, reason := sec07ConnectWithWill(t, fixture.mqttAddresses[1], "sec12-user", string(password), "sec12-client-a", "sec12/will/a", "must-not-publish", 5)
	if reason != 0 {
		_ = liveClient.Close()
		t.Fatalf("live MQTT 5 client CONNACK reason=0x%02x, want success", reason)
	}
	defer liveClient.Close()
	clientPath := "/api/v1/security/blocklist/client-ids/sec12/sec12-client-a"
	status, body, err := clusterMembershipProcessRequest(fixture.ctx, http.MethodPost, processes[0].managementURL()+clientPath, nil, clusterMembershipProcessAdmin, "")
	t0 := time.Now()
	if err != nil || status != http.StatusOK {
		t.Fatalf("block client ID status=%d body=%s error=%v", status, body, err)
	}
	var change struct {
		Data struct {
			Blocked       bool   `json:"blocked"`
			PolicyVersion uint64 `json:"policyVersion"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &change); err != nil || !change.Data.Blocked || change.Data.PolicyVersion == 0 {
		t.Fatalf("client ID block response=%s decode error=%v", body, err)
	}
	v1 := change.Data.PolicyVersion
	deadline := t0.Add(5 * time.Second)
	sec12AssertClosedBy(t, liveClient, 5, 0x98, deadline)
	t.Logf("client ID live disconnect applied in %s", time.Since(t0))
	for node, address := range fixture.mqttAddresses {
		for _, version := range []byte{4, 5} {
			clientID := fmt.Sprintf("sec12-denied-client-%d-%d", node+1, version)
			connection, connack, _ := clusterDataPlaneProcessConnect(t, address, "sec12-user", string(password), "sec12-client-a", version)
			_ = connection.Close()
			want := map[byte]byte{4: 0x05, 5: 0x8A}[version]
			if connack != want {
				t.Fatalf("node %d MQTT %d blocked client ID CONNACK=0x%02x, want 0x%02x (client %s)", node+1, version, connack, want, clientID)
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d MQTT %d client ID denial exceeded five-second propagation deadline", node+1, version)
			}
		}
	}
	otherConnections := make([]net.Conn, 0, len(fixture.mqttAddresses))
	for node, address := range fixture.mqttAddresses {
		other, connack, _ := clusterDataPlaneProcessConnect(t, address, "sec12-other-user", string(password), fmt.Sprintf("sec12-other-%d", node+1), 5)
		if connack != 0 {
			_ = other.Close()
			t.Fatalf("node %d other-tenant same client ID CONNACK=0x%02x, want success", node+1, connack)
		}
		otherConnections = append(otherConnections, other)
	}
	defer func() {
		for _, connection := range otherConnections {
			_ = connection.Close()
		}
	}()
	for node, address := range fixture.mqttAddresses {
		other, connack, _ := clusterDataPlaneProcessConnect(t, address, "sec12-other-user", string(password), "sec12-client-a", 5)
		_ = other.Close()
		if connack != 0 {
			t.Fatalf("node %d other-tenant same client ID CONNACK=0x%02x, want success", node+1, connack)
		}
		version, entries, blockBody := readBlocklist(node)
		if version != v1 || len(entries) != 1 || entries[0].Kind != "clientId" || entries[0].TenantID != "sec12" || entries[0].Value != "sec12-client-a" {
			t.Fatalf("node %d client ID blocklist=%s, want version %d and clientId entry", node+1, blockBody, v1)
		}
	}
	t.Logf("client ID block propagated and denied on all nodes in %s", time.Since(t0))
	sec07AssertNoWill(t, watcher)

	blockedLive, connack, _ := clusterDataPlaneProcessConnect(t, fixture.mqttAddresses[2], "sec12-blocked-user", string(password), "sec12-blocked-live", 4)
	if connack != 0 {
		_ = blockedLive.Close()
		t.Fatalf("blocked-user live MQTT 3.1.1 CONNACK=0x%02x, want success", connack)
	}
	defer blockedLive.Close()
	usernamePath := "/api/v1/security/blocklist/usernames/sec12-blocked-user"
	status, body, err = clusterMembershipProcessRequest(fixture.ctx, http.MethodPost, processes[0].managementURL()+usernamePath, nil, clusterMembershipProcessAdmin, "")
	usernameT0 := time.Now()
	if err != nil || status != http.StatusOK {
		t.Fatalf("block username status=%d body=%s error=%v", status, body, err)
	}
	var usernameChange struct {
		Data struct {
			Blocked       bool   `json:"blocked"`
			PolicyVersion uint64 `json:"policyVersion"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &usernameChange); err != nil || !usernameChange.Data.Blocked || usernameChange.Data.PolicyVersion <= v1 {
		t.Fatalf("username block response=%s decode error=%v", body, err)
	}
	v2 := usernameChange.Data.PolicyVersion
	usernameDeadline := usernameT0.Add(5 * time.Second)
	sec12AssertClosedBy(t, blockedLive, 4, 0, usernameDeadline)
	for node, address := range fixture.mqttAddresses {
		for _, version := range []byte{4, 5} {
			connection, denied, _ := clusterDataPlaneProcessConnect(t, address, "sec12-blocked-user", string(password), fmt.Sprintf("sec12-denied-user-%d-%d", node+1, version), version)
			_ = connection.Close()
			want := map[byte]byte{4: 0x05, 5: 0x8A}[version]
			if denied != want || time.Now().After(usernameDeadline) {
				t.Fatalf("node %d MQTT %d username denial=0x%02x, want 0x%02x before deadline", node+1, version, denied, want)
			}
		}
		version, entries, blockBody := readBlocklist(node)
		if version != v2 || len(entries) != 2 {
			t.Fatalf("node %d username blocklist=%s, want version %d and two entries", node+1, blockBody, v2)
		}
	}
	t.Logf("username block propagated and denied on all nodes in %s", time.Since(usernameT0))

	ipPath := "/api/v1/security/blocklist/ip-prefixes/127.0.0.1/32"
	status, body, err = clusterMembershipProcessRequest(fixture.ctx, http.MethodPost, processes[0].managementURL()+ipPath, nil, clusterMembershipProcessAdmin, "")
	ipT0 := time.Now()
	if err != nil || status != http.StatusOK {
		t.Fatalf("block loopback prefix status=%d body=%s error=%v", status, body, err)
	}
	ipDeadline := ipT0.Add(5 * time.Second)
	sec12AssertClosedBy(t, watcher, 4, 0, ipDeadline)
	for _, connection := range otherConnections {
		sec12AssertClosedBy(t, connection, 5, 0x98, ipDeadline)
	}
	t.Logf("IP block closed watcher and allowed connections in %s", time.Since(ipT0))
	for node, address := range fixture.mqttAddresses {
		connection, denied, _ := clusterDataPlaneProcessConnect(t, address, "sec12-user", string(password), fmt.Sprintf("sec12-ip-denied-%d", node+1), 5)
		_ = connection.Close()
		if denied != 0x8A || time.Now().After(ipDeadline) {
			t.Fatalf("node %d MQTT 5 loopback block CONNACK=0x%02x, want 0x8a before deadline", node+1, denied)
		}
	}
	t.Logf("IP block denied new MQTT 5 CONNECTs on all nodes in %s", time.Since(ipT0))

	status, body, err = clusterMembershipProcessRequest(fixture.ctx, http.MethodDelete, processes[0].managementURL()+ipPath, nil, clusterMembershipProcessAdmin, "")
	unblockT0 := time.Now()
	if err != nil || status != http.StatusOK {
		t.Fatalf("unblock loopback prefix status=%d body=%s error=%v", status, body, err)
	}
	unblockDeadline := time.Now().Add(5 * time.Second)
	for node, address := range fixture.mqttAddresses {
		connectCtx, cancel := context.WithDeadline(fixture.ctx, unblockDeadline)
		clusterMembershipProcessWait(t, connectCtx, fmt.Sprintf("node %d loopback unblock CONNECT", node+1), func() (bool, string, error) {
			connection, connectReason, detail := clusterDataPlaneProcessConnect(t, address, "sec12-user", string(password), fmt.Sprintf("sec12-unblocked-%d", node+1), 5)
			if connection != nil {
				_ = connection.Close()
			}
			if connectReason == 0 {
				return true, "MQTT 5 CONNECT accepted", nil
			}
			return false, fmt.Sprintf("CONNACK reason=0x%02x detail=%s", connectReason, detail), nil
		})
		cancel()
	}
	t.Logf("IP unblock CONNECT freshness reached all nodes in %s", time.Since(unblockT0))

	clusterMembershipProcessWait(t, fixture.ctx, "SEC-12 blocklist audit events", func() (bool, string, error) {
		for node := range processes {
			events, readErr := sec07ReadAuditEvents(fixture.auditDirectory(uint64(node + 1)))
			if readErr != nil {
				return false, readErr.Error(), nil
			}
			found := false
			for _, event := range events {
				if event.Action == "mqtt.connect" && event.Reason == "blocked" &&
					event.Target.Type == "mqtt_client" && event.Target.ID == "sec12-client-a" && event.Target.TenantID == "sec12" &&
					event.Actor.Subject == "sec12-user" && event.Actor.TenantID == "sec12" &&
					event.Actor.CredentialID == "sec12-credential-user" && event.Details.AuthMethod == "password" {
					found = true
					break
				}
			}
			if !found {
				return false, fmt.Sprintf("node %d has no blocked clientId CONNECT audit; events=%+v", node+1, events), nil
			}
		}
		events, readErr := sec07ReadAuditEvents(fixture.auditDirectory(1))
		if readErr != nil {
			return false, readErr.Error(), nil
		}
		added, removed := false, false
		for _, event := range events {
			if event.Target.Type != "mqtt_blocklist_entry" || event.Target.ID != "ipPrefix:127.0.0.1/32" {
				continue
			}
			added = added || event.Action == "security.blocklist.add"
			removed = removed || event.Action == "security.blocklist.remove"
		}
		if !added || !removed {
			return false, fmt.Sprintf("node 1 blocklist mutation audit add=%t remove=%t", added, removed), nil
		}
		return true, "all node denials and node 1 mutations audited", nil
	})

	for _, path := range []string{clientPath, usernamePath} {
		status, body, err = clusterMembershipProcessRequest(fixture.ctx, http.MethodDelete, processes[0].managementURL()+path, nil, clusterMembershipProcessAdmin, "")
		if err != nil || status != http.StatusOK {
			t.Fatalf("delete remaining blocklist entry %s status=%d body=%s error=%v", path, status, body, err)
		}
	}
	var finalVersion uint64
	for node := range processes {
		version, entries, blockBody := readBlocklist(node)
		if len(entries) != 0 {
			t.Fatalf("node %d final blocklist=%s, want no entries", node+1, blockBody)
		}
		if node == 0 {
			finalVersion = version
		} else if version != finalVersion {
			t.Fatalf("node %d final policyVersion=%d, want node 1 version %d", node+1, version, finalVersion)
		}
	}
}

func sec12AssertClosedBy(t *testing.T, connection net.Conn, version, wantReason byte, deadline time.Time) {
	t.Helper()
	if err := connection.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set MQTT %d blocklist read deadline: %v", version, err)
	}
	defer connection.SetReadDeadline(time.Time{})
	header, body, err := clusterMembershipProcessReadMQTTPacket(connection)
	if version == 5 {
		if err != nil || header != 0xE0 || len(body) == 0 || body[0] != wantReason {
			t.Fatalf("MQTT 5 DISCONNECT header=0x%02x body=% X err=%v, want reason 0x%02x before %s", header, body, err, wantReason, deadline.Format(time.RFC3339Nano))
		}
		return
	}
	if err == nil || !clusterMembershipProcessIsClosed(err) {
		t.Fatalf("MQTT 3.1.1 connection read header=0x%02x body=% X err=%v, want socket close before %s", header, body, err, deadline.Format(time.RFC3339Nano))
	}
}
