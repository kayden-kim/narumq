package integration

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Traceability: API-03; docs/specs/management-api-and-observability.md §7.1.
// Scenarios: management-api-and-observability.md:MGT-SCN-028
func TestDynamicClusterSubscriptionGaugeProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	configPaths := make([]string, 3)
	for index := range configPaths {
		configPaths[index] = fixture.bootstrapConfig(uint64(index + 1))
		clusterTenantQuotaProcessSetProfile(t, configPaths[index], clusterTenantQuotaProcessProfile(1))
	}
	processes := make([]*clusterMembershipProcess, len(configPaths))
	for index, path := range configPaths {
		process, err := fixture.harness.start(
			fmt.Sprintf("metrics bootstrap node %d", index+1), path, fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("start metrics bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	ctx := fixture.ctx
	clusterMembershipProcessWaitForBootstrap(t, ctx, processes)
	clusterMetricsProcessWaitForLine(t, ctx, processes, "narumq_tenant_quota_profile_committed 1")
	for _, process := range processes {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}
	clusterMetricsProcessWaitForLine(t, ctx, processes, "narumq_subscriptions_active 0")

	connection := clusterSessionMetadataConnect(
		t, fixture.mqttAddresses[1], fixture.accessToken, "metrics-gauge-subscriber", 0, nil,
	)
	defer connection.Close()

	clusterDrainProcessSubscribeV5(t, connection, 1, "metrics/a", 1)
	clusterDrainProcessSubscribeV5(t, connection, 2, "metrics/b", 0)
	clusterMetricsProcessWaitForLine(t, ctx, processes, "narumq_subscriptions_active 2")

	body := []byte{0, 3, 0, 0, byte(len("metrics/a"))}
	body = append(body, "metrics/a"...)
	if err := clusterDurableProcessWrite(connection, 0xA2, body); err != nil {
		t.Fatalf("send MQTT 5 UNSUBSCRIBE: %v", err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	kind, _, ack, err := clusterDurableProcessReadRaw(connection)
	_ = connection.SetReadDeadline(time.Time{})
	if err != nil || kind != 11 || !bytes.Equal(ack, []byte{0, 3, 0, 0}) {
		t.Fatalf("MQTT 5 UNSUBACK kind=%d body=%x err=%v, want packet ID 3 and success reason", kind, ack, err)
	}

	clusterMetricsProcessWaitForLine(t, ctx, processes, "narumq_subscriptions_active 1")
	if err := clusterDurableProcessWrite(connection, 0xE0, []byte{0, 0}); err != nil {
		t.Fatalf("send MQTT 5 DISCONNECT: %v", err)
	}
	clusterMembershipProcessAssertConnectionClosed(t, connection, 2*time.Second)
	clusterMetricsProcessWaitForLine(t, ctx, processes, "narumq_subscriptions_active 0")
}

func clusterMetricsProcessWaitForLine(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, line string) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, "all nodes report "+line, func() (bool, string, error) {
		for _, process := range processes {
			if err := process.isAlive(); err != nil {
				return false, "", err
			}
			status, body, err := clusterMembershipProcessRequest(
				ctx, http.MethodGet, process.managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "",
			)
			if err != nil {
				return false, fmt.Sprintf("%s metrics request failed: %v", process.name, err), nil
			}
			if status != http.StatusOK || !strings.Contains(string(body), "\n"+line+"\n") {
				return false, fmt.Sprintf("%s missing %q", process.name, line), nil
			}
		}
		return true, "", nil
	})
}
