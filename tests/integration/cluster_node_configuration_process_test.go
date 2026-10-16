package integration

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// Traceability: CFG-SCN-31/32; docs/specs/configuration-and-runtime-options.md §10.
func TestDynamicClusterNodeConfigurationProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	issuer := newClusterProcessJWTIssuer(t, "management-config-key")
	jwksURL, err := url.Parse(issuer.jwksURL)
	if err != nil {
		t.Fatalf("parse management JWKS URL: %v", err)
	}
	jwksURL.User = url.UserPassword("url-user", "url-secret")
	fixture.managementAPIKeys = []clusterMembershipProcessAPIKey{{
		ID: "dashboard", KeyFile: fixture.readOnlyAPIKeyPath, Roles: []string{"ReadOnly"},
	}}
	fixture.managementOIDC = &clusterMembershipProcessOIDC{
		Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"},
		JWKSURL: jwksURL.String(), CacheTTLMillis: 300000, RefreshCooldownMillis: 30000,
		ClockSkewMillis: 30000, HTTPTimeoutMillis: 5000,
		Identities: []clusterMembershipProcessOIDCIdentity{{Subject: "management-secret-subject", Roles: []string{"SecurityAdmin"}}},
	}

	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		process, err := fixture.harness.start(
			fmt.Sprintf("node configuration bootstrap node %d", index+1),
			fixture.bootstrapConfig(uint64(index+1)),
			fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, processes)

	request := func(processIndex int, method, path string) (int, []byte, error) {
		header := make(http.Header)
		header.Set("Authorization", "Bearer "+clusterMembershipProcessAdmin)
		return clusterMembershipProcessRequestWithHeader(
			fixture.ctx, method, processes[processIndex].managementURL()+path, nil, header,
		)
	}

	status, body, err := request(0, http.MethodGet, "/api/v1/nodes/1/config")
	if err != nil || status != http.StatusOK {
		t.Fatalf("get local node configuration status=%d body=%s err=%v, want %d", status, body, err, http.StatusOK)
	}
	var response struct {
		Data struct {
			Node struct {
				ClusterID string `json:"clusterId"`
				NodeID    string `json:"nodeId"`
				Role      string `json:"role"`
			} `json:"node"`
			Peer struct {
				TLS struct {
					CertificateConfigured bool `json:"certificateConfigured"`
					PrivateKeyConfigured  bool `json:"privateKeyConfigured"`
					CAConfigured          bool `json:"caConfigured"`
				} `json:"tls"`
			} `json:"peer"`
			Management struct {
				APIKeyCount int `json:"apiKeyCount"`
				OIDC        *struct {
					IdentityCount int    `json:"identityCount"`
					JWKSURL       string `json:"jwksUrl"`
				} `json:"oidc"`
				BreakGlassTokenConfigured bool `json:"breakGlassTokenConfigured"`
				AuditDirectoryConfigured  bool `json:"auditDirectoryConfigured"`
			} `json:"management"`
			MQTT struct {
				SecurityConfigConfigured bool `json:"securityConfigConfigured"`
			} `json:"mqtt"`
			PublishBackpressure struct {
				MinFreeBytes       uint64 `json:"minFreeBytes"`
				MaxCommitLagMillis uint64 `json:"maxCommitLagMillis"`
			} `json:"publishBackpressure"`
			Security struct {
				JWT *struct {
					JWKSURL string `json:"jwksUrl"`
				} `json:"jwt"`
				IdentityCounts struct {
					JWT      int `json:"jwt"`
					Password int `json:"password"`
					MTLS     int `json:"mtls"`
				} `json:"identityCounts"`
				ACL []struct {
					TenantID    string `json:"tenantId"`
					Role        string `json:"role"`
					Effect      string `json:"effect"`
					Action      string `json:"action"`
					TopicFilter string `json:"topicFilter"`
				} `json:"acl"`
			} `json:"security"`
			EffectivePolicy struct {
				MQTTLimits struct {
					Source string `json:"source"`
				} `json:"mqttLimits"`
			} `json:"effectivePolicy"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode node configuration response: %v; body=%s", err, body)
	}
	if response.Data.Node.ClusterID != clusterMembershipProcessClusterID || response.Data.Node.NodeID != "1" || response.Data.Node.Role != "bootstrap" {
		t.Fatalf("node identity = %+v, want local bootstrap node", response.Data.Node)
	}
	if !response.Data.Peer.TLS.CertificateConfigured || !response.Data.Peer.TLS.PrivateKeyConfigured || !response.Data.Peer.TLS.CAConfigured {
		t.Fatalf("peer TLS configuration = %+v, want configured file-presence flags", response.Data.Peer.TLS)
	}
	management := response.Data.Management
	if management.APIKeyCount != 1 || management.OIDC == nil || management.OIDC.IdentityCount != 1 ||
		!management.BreakGlassTokenConfigured || !management.AuditDirectoryConfigured {
		t.Fatalf("management security metadata = %+v, want configured token, API key, OIDC identity, and audit directory", management)
	}
	if got := management.OIDC.JWKSURL; got != issuer.jwksURL {
		t.Fatalf("management JWKS URL = %q, want userinfo-free %q", got, issuer.jwksURL)
	}
	if !response.Data.MQTT.SecurityConfigConfigured || response.Data.Security.JWT == nil {
		t.Fatalf("MQTT security metadata = %+v, security = %+v, want configured JWT security", response.Data.MQTT, response.Data.Security)
	}
	if response.Data.Security.IdentityCounts.JWT != 1 || response.Data.Security.IdentityCounts.Password != 0 ||
		response.Data.Security.IdentityCounts.MTLS != 0 {
		t.Fatalf("security identity counts = %+v, want JWT=1, password=0, mTLS=0", response.Data.Security.IdentityCounts)
	}
	if got := response.Data.Security.ACL; len(got) != 63 ||
		got[0].TenantID != "acceptance" || got[0].Role != "client" || got[0].Effect != "allow" ||
		got[0].Action != "subscribe" || got[0].TopicFilter != "handoff/#" ||
		got[len(got)-1].TenantID != "acceptance" || got[len(got)-1].Role != "client" ||
		got[len(got)-1].Effect != "allow" || got[len(got)-1].Action != "subscribe" ||
		got[len(got)-1].TopicFilter != "recovery/#" {
		t.Fatalf("ACL configuration = %+v, want 63 rules in configured order", got)
	}

	if response.Data.PublishBackpressure.MinFreeBytes != 16<<20 || response.Data.PublishBackpressure.MaxCommitLagMillis != 1000 {
		t.Fatalf("publish backpressure = %+v, want applied local values", response.Data.PublishBackpressure)
	}
	if response.Data.EffectivePolicy.MQTTLimits.Source != "default" {
		t.Fatalf("effective MQTT limit source = %q, want default", response.Data.EffectivePolicy.MQTTLimits.Source)
	}

	secrets := []string{
		string(fixture.certificates[0].keyPEM),
		fixture.tokenPath, fixture.readOnlyAPIKeyPath, fixture.securityPath, fixture.auditDirectory(1), fixture.caPath,
		fixture.certificates[0].certPath(fixture.tempDir), fixture.certificates[0].keyPath(fixture.tempDir),
		"url-user", "url-secret", "management-secret-subject", "mqtt-process-client",
		clusterMembershipProcessAdmin, clusterMembershipProcessReadOnlyAPIKey,
		hex.EncodeToString(fixture.certificates[0].fingerprint[:]),
	}
	for _, secret := range secrets {
		if strings.Contains(string(body), secret) {
			t.Errorf("node configuration response exposed sensitive value %q: %s", secret, body)
		}
	}

	status, body, err = request(0, http.MethodGet, "/api/v1/nodes/2/config")
	assertNodeScopeResponse(t, "remote config node", status, body, err, http.StatusConflict, "node_not_local", "1")
	status, body, err = request(0, http.MethodGet, "/api/v1/nodes/01/config")
	assertNodeScopeResponse(t, "noncanonical config node", status, body, err, http.StatusNotFound, "", "")
	status, body, err = request(0, http.MethodGet, "/api/v1/nodes/0/config")
	assertNodeScopeResponse(t, "zero config node", status, body, err, http.StatusNotFound, "", "")
	status, body, err = request(0, http.MethodGet, "/api/v1/nodes/2/audit/events")
	assertNodeScopeResponse(t, "remote audit node", status, body, err, http.StatusConflict, "node_not_local", "1")
	status, body, err = request(0, http.MethodGet, "/api/v1/nodes/01/audit/events")
	assertNodeScopeResponse(t, "noncanonical audit node", status, body, err, http.StatusNotFound, "", "")
	status, body, err = request(0, http.MethodGet, "/api/v1/audit/events")
	assertNodeScopeResponse(t, "removed cluster-wide audit route", status, body, err, http.StatusNotFound, "", "")

	status, body, err = request(1, http.MethodGet, "/api/v1/nodes/2/config")
	if err != nil || status != http.StatusOK {
		t.Fatalf("get node 2 configuration from node 2 status=%d body=%s err=%v, want %d", status, body, err, http.StatusOK)
	}
}

func assertNodeScopeResponse(t *testing.T, name string, status int, body []byte, err error, wantStatus int, wantCode, wantServingNodeID string) {
	t.Helper()
	if err != nil || status != wantStatus {
		t.Fatalf("%s status=%d body=%s err=%v, want %d", name, status, body, err, wantStatus)
	}
	if wantCode == "" {
		return
	}
	var response struct {
		Error struct {
			Code    string            `json:"code"`
			Details map[string]string `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode %s error response: %v; body=%s", name, err, body)
	}
	if response.Error.Code != wantCode || response.Error.Details["servingNodeId"] != wantServingNodeID {
		t.Fatalf("%s error = %+v, want code %q and servingNodeId %q", name, response.Error, wantCode, wantServingNodeID)
	}
}
