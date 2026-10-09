package integration

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestDynamicClusterTenantLimitsProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	securityPath := clusterTenantLimitsProcessSecurity(t, fixture)
	configPaths := make([]string, 3)
	for index := range configPaths {
		configPaths[index] = clusterTenantLimitsProcessConfig(t, fixture.bootstrapConfig(uint64(index+1)), securityPath)
	}

	nodes := make([]*clusterMembershipProcess, len(configPaths))
	for index, path := range configPaths {
		process, err := fixture.harness.start(fmt.Sprintf("tenant limits bootstrap node %d", index+1), path, fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", index+1, err)
		}
		nodes[index] = process
	}
	ctx := fixture.ctx
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	for _, process := range nodes {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}

	const filterA = "offline/tl/a"
	const filterB = "offline/tl/b"
	const filterC = "offline/tl/c"
	subA, code := clusterTenantLimitsProcessConnectResult(t, fixture.mqttAddresses[0], fixture.accessToken, "tl-sub-1")
	if code != 0 {
		t.Fatalf("connect tl-sub-1 CONNACK=%#x, want 0x00", code)
	}
	t.Cleanup(func() { _ = subA.Close() })
	if got := clusterTenantLimitsProcessSubscribeResult(t, subA, 1, filterA); len(got) != 1 || got[0] != 0 {
		t.Fatalf("tl-sub-1 SUBACK=%v, want [0x00]", got)
	}

	subB, code := clusterTenantLimitsProcessConnectResult(t, fixture.mqttAddresses[1], fixture.accessToken, "tl-sub-2")
	if code != 0 {
		t.Fatalf("connect tl-sub-2 CONNACK=%#x, want 0x00", code)
	}
	t.Cleanup(func() { _ = subB.Close() })
	if got := clusterTenantLimitsProcessSubscribeResult(t, subB, 1, filterB); len(got) != 1 || got[0] != 0 {
		t.Fatalf("tl-sub-2 SUBACK=%v, want [0x00]", got)
	}
	clusterDurableProcessWaitForSubscriptions(t, ctx, nodes, "acceptance",
		"tl-sub-1|"+filterA+"|0", "tl-sub-2|"+filterB+"|0")

	subC, code := clusterTenantLimitsProcessConnectResult(t, fixture.mqttAddresses[2], fixture.accessToken, "tl-sub-3")
	if code != 0 {
		t.Fatalf("connect tl-sub-3 CONNACK=%#x, want 0x00", code)
	}
	t.Cleanup(func() { _ = subC.Close() })
	if got := clusterTenantLimitsProcessSubscribeResult(t, subC, 1, filterC); len(got) != 1 || got[0] != 0x80 {
		t.Fatalf("tl-sub-3 SUBACK=%v, want [0x80]", got)
	}
	clusterTenantQuotaProcessWaitForMetric(t, ctx, nodes[2], "narumq_tenant_limit_rejections_total{tenant=\"acceptance\",dimension=\"subscriptions\"} 1\n")

	conn1, code := clusterTenantLimitsProcessConnectResult(t, fixture.mqttAddresses[0], fixture.accessToken, "tl-conn-1")
	if code != 0 {
		t.Fatalf("connect tl-conn-1 on node1 CONNACK=%#x, want 0x00", code)
	}
	t.Cleanup(func() { _ = conn1.Close() })
	_, code = clusterTenantLimitsProcessConnectResult(t, fixture.mqttAddresses[0], fixture.accessToken, "tl-conn-2")
	if code != 0x03 {
		t.Fatalf("connect tl-conn-2 on node1 CONNACK=%#x, want 0x03", code)
	}
	clusterTenantQuotaProcessWaitForMetric(t, ctx, nodes[0], "narumq_tenant_limit_rejections_total{tenant=\"acceptance\",dimension=\"connections\"} 1\n")
	conn2, code := clusterTenantLimitsProcessConnectResult(t, fixture.mqttAddresses[1], fixture.accessToken, "tl-conn-2")
	if code != 0 {
		t.Fatalf("connect tl-conn-2 on node2 CONNACK=%#x, want 0x00", code)
	}
	t.Cleanup(func() { _ = conn2.Close() })
}

func clusterTenantLimitsProcessSecurity(t *testing.T, fixture *clusterMembershipProcessFixture) string {
	t.Helper()
	data, err := os.ReadFile(fixture.securityPath)
	if err != nil {
		t.Fatalf("read fixture security config: %v", err)
	}
	var security map[string]any
	if err := json.Unmarshal(data, &security); err != nil {
		t.Fatalf("decode fixture security config: %v", err)
	}
	security["tenantLimits"] = map[string]any{
		"default": map[string]any{"maxConnections": 2, "maxSubscriptions": 2},
	}
	data, err = json.MarshalIndent(security, "", "  ")
	if err != nil {
		t.Fatalf("encode tenant limits security config: %v", err)
	}
	path := filepath.Join(fixture.tempDir, "mqtt-security-tenant-limits.json")
	clusterMembershipProcessWriteFile(t, path, data, 0o600)
	return path
}

func clusterTenantLimitsProcessConfig(t *testing.T, path, securityPath string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read node config %s: %v", path, err)
	}
	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatalf("decode node config %s: %v", path, err)
	}
	mqtt, ok := config["mqtt"].(map[string]any)
	if !ok {
		t.Fatalf("node config %s has no mqtt object", path)
	}
	mqtt["securityConfigFile"] = securityPath
	data, err = json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatalf("encode node config %s: %v", path, err)
	}
	out := strings.TrimSuffix(path, ".json") + "-tenant-limits.json"
	clusterMembershipProcessWriteFile(t, out, data, 0o600)
	return out
}

func clusterTenantLimitsProcessConnectResult(t *testing.T, address, token, clientID string) (net.Conn, byte) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s for %q: %v", address, clientID, err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0xc0, 0, 60}
	for _, field := range []string{clientID, "jwt", token} {
		body = append(body, byte(len(field)>>8), byte(len(field)))
		body = append(body, field...)
	}
	if err := clusterDurableProcessWrite(conn, 0x10, body); err != nil {
		_ = conn.Close()
		t.Fatalf("write CONNECT %q: %v", clientID, err)
	}
	kind, _, response, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != 2 || len(response) != 2 {
		_ = conn.Close()
		t.Fatalf("read CONNACK %q: kind=%d payload=%x err=%v", clientID, kind, response, err)
	}
	_ = conn.SetDeadline(time.Time{})
	if response[1] != 0 {
		_ = conn.Close()
		return nil, response[1]
	}
	return clusterDurableProcessWrap(conn, response[0]&1 != 0), response[1]
}

func clusterTenantLimitsProcessSubscribeResult(t *testing.T, conn net.Conn, id uint16, filter string) []byte {
	t.Helper()
	body := []byte{byte(id >> 8), byte(id), byte(len(filter) >> 8), byte(len(filter))}
	body = append(body, filter...)
	body = append(body, 0)
	if err := clusterDurableProcessWrite(conn, 0x82, body); err != nil {
		t.Fatalf("write SUBSCRIBE %q: %v", filter, err)
	}
	kind, _, response, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != 9 || len(response) < 3 || binary.BigEndian.Uint16(response[:2]) != id {
		t.Fatalf("read SUBACK %q: kind=%d payload=%x err=%v", filter, kind, response, err)
	}
	return response[2:]
}
