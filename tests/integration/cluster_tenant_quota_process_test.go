package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// Traceability: TEN-08; docs/specs/cluster-session-and-delivery.md §11.16.
func TestDynamicClusterTenantStorageQuotaProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	configPaths := make([]string, 3)
	for index := range configPaths {
		configPaths[index] = fixture.bootstrapConfig(uint64(index + 1))
		clusterTenantQuotaProcessSetProfile(t, configPaths[index], clusterTenantQuotaProcessProfile(1))
	}

	nodes := make([]*clusterMembershipProcess, len(configPaths))
	for index, path := range configPaths {
		process, err := fixture.harness.start(fmt.Sprintf("tenant quota bootstrap node %d", index+1), path, fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", index+1, err)
		}
		nodes[index] = process
	}
	ctx := fixture.ctx
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	for _, process := range nodes {
		clusterTenantQuotaProcessWaitForProfile(t, ctx, process, 1, false)
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}

	const retainedTopic = "retained/quota"
	if err := clusterMembershipProcessTryPublishWithRetain(
		fixture.mqttAddresses[0], fixture.accessToken, "retained-publisher", retainedTopic, []byte("kept"), true,
	); err != nil {
		for _, process := range nodes {
			status, body, requestErr := clusterMembershipProcessRequest(ctx, http.MethodGet,
				process.managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "")
			t.Logf("node=%s metrics status=%d err=%v body=%s logs=%q",
				process.name, status, requestErr, body, process.logs.String())
		}
		t.Fatalf("initial retained publish failed: %v", err)
	}
	clusterTenantQuotaProcessExpectRetainedQuotaClose(
		t, fixture.mqttAddresses[0], fixture.accessToken, "retained-publisher", retainedTopic, strings.Repeat("x", 5000),
	)
	retainedReject := `narumq_tenant_quota_rejections_total{tenant="acceptance",dimension="retained_bytes"} 1`
	clusterTenantQuotaProcessWaitForMetric(t, ctx, nodes[0], retainedReject)
	if metrics := clusterTenantQuotaProcessMetrics(t, ctx, nodes[1]); strings.Contains(metrics, `dimension="retained_bytes"`) {
		t.Fatalf("non-proposer node recorded retained quota rejection: %s", metrics)
	}
	retainedSubscriber := clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[1], fixture.accessToken, "retained-subscriber-source", retainedTopic,
	)
	retained := clusterMembershipProcessReadRetained(t, retainedSubscriber, 1)
	_ = retainedSubscriber.Close()
	if got := retained[retainedTopic]; got != "kept" {
		t.Fatalf("retained value after rejected replacement = %q, want kept", got)
	}

	const durableFilter = "durable-takeover/quota/#"
	offlineSubscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "durable-takeover-sub", false)
	clusterDurableProcessSubscribe(t, offlineSubscriber, 1, durableFilter, 1)
	clusterDurableProcessWaitForSubscriptions(t, ctx, nodes,
		"retained-subscriber-source|"+retainedTopic+"|0", "durable-takeover-sub|"+durableFilter+"|1")
	clusterDurableProcessWriteFatal(t, offlineSubscriber, 0xE0, nil)
	clusterMembershipProcessAssertConnectionClosed(t, offlineSubscriber, 2*time.Second)
	_ = offlineSubscriber.Close()

	publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "durable-takeover-pub", true)
	clusterTenantQuotaProcessPublishAndAck(t, publisher, "durable-takeover/quota/v1-one", 10, "v1-one")
	clusterTenantQuotaProcessPublishAndAck(t, publisher, "durable-takeover/quota/v1-two", 11, "v1-two")
	v1Reject := `narumq_tenant_quota_rejections_total{tenant="acceptance",dimension="offline_queue_messages"} 1`
	clusterTenantQuotaProcessWaitForMetric(t, ctx, nodes[1], v1Reject)
	if metrics := clusterTenantQuotaProcessMetrics(t, ctx, nodes[2]); strings.Contains(metrics, `dimension="offline_queue_messages"`) {
		t.Fatalf("non-proposer node recorded durable quota rejection: %s", metrics)
	}

	offlineSubscriber = clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-takeover-sub", false)
	if !clusterDurableProcessSessionPresent(t, offlineSubscriber) {
		t.Fatal("offline subscriber reconnect returned SessionPresent=false")
	}
	first := clusterDurableProcessRead(t, offlineSubscriber)
	if first.kind != 3 || first.qos != 1 || first.topic != "durable-takeover/quota/v1-one" || first.payload != "v1-one" {
		t.Fatalf("v1 durable queue delivery = %+v, want only v1-one", first)
	}
	clusterDurableProcessWriteFatal(t, offlineSubscriber, 0x40, []byte{byte(first.packetID >> 8), byte(first.packetID)})
	clusterTenantQuotaProcessExpectPingResp(t, offlineSubscriber)
	clusterDurableProcessWriteFatal(t, offlineSubscriber, 0xE0, nil)
	clusterMembershipProcessAssertConnectionClosed(t, offlineSubscriber, 2*time.Second)
	_ = offlineSubscriber.Close()
	_ = publisher.Close()

	// One new-config voter commits version 2; old-config peers must keep serving from that committed profile.
	clusterTenantQuotaProcessSetProfile(t, configPaths[0], clusterTenantQuotaProcessProfile(2))
	if err := nodes[0].crash(5 * time.Second); err != nil {
		t.Fatalf("crash node 1 for quota profile update: %v", err)
	}
	upgradedNode, err := fixture.harness.start("tenant quota node 1 version 2", configPaths[0], fixture.managementAddresses[0])
	if err != nil {
		t.Fatalf("restart node 1 with quota version 2: %v", err)
	}
	nodes[0] = upgradedNode
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	clusterTenantQuotaProcessWaitForProfile(t, ctx, nodes[0], 2, false)
	clusterTenantQuotaProcessWaitForProfile(t, ctx, nodes[1], 2, true)
	clusterTenantQuotaProcessWaitForProfile(t, ctx, nodes[2], 2, true)
	clusterMembershipProcessWaitForStatus(t, ctx, nodes[2], "/health/ready", http.StatusOK)

	publisher = clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-takeover-pub", true)
	clusterTenantQuotaProcessPublishAndAck(t, publisher, "durable-takeover/quota/v2-one", 20, "v2-one")
	clusterTenantQuotaProcessPublishAndAck(t, publisher, "durable-takeover/quota/v2-two", 21, "v2-two")
	clusterTenantQuotaProcessPublishAndAck(t, publisher, "durable-takeover/quota/v2-three", 22, "v2-three")
	v2Reject := `narumq_tenant_quota_rejections_total{tenant="acceptance",dimension="offline_queue_messages"} 1`
	clusterTenantQuotaProcessWaitForMetric(t, ctx, nodes[2], v2Reject)
	_ = publisher.Close()

	// Restart the formerly lower-config node with version 2 while its two durable entries remain queued.
	clusterTenantQuotaProcessSetProfile(t, configPaths[2], clusterTenantQuotaProcessProfile(2))
	if err := nodes[2].crash(5 * time.Second); err != nil {
		t.Fatalf("crash node 3 for quota profile convergence: %v", err)
	}
	convergedNode, err := fixture.harness.start("tenant quota node 3 version 2", configPaths[2], fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart node 3 with quota version 2: %v", err)
	}
	nodes[2] = convergedNode
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	clusterTenantQuotaProcessWaitForProfile(t, ctx, nodes[2], 2, false)
	clusterMembershipProcessWaitForStatus(t, ctx, nodes[2], "/health/ready", http.StatusOK)

	retainedSubscriber = clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[1], fixture.accessToken, "retained-subscriber-restored", retainedTopic,
	)
	retained = clusterMembershipProcessReadRetained(t, retainedSubscriber, 1)
	_ = retainedSubscriber.Close()
	if got := retained[retainedTopic]; got != "kept" {
		t.Fatalf("retained value after node restart = %q, want kept", got)
	}

	publisher = clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-takeover-pub", true)
	clusterTenantQuotaProcessPublishAndAck(t, publisher, "durable-takeover/quota/v2-four", 23, "v2-four")
	v2RestartReject := `narumq_tenant_quota_rejections_total{tenant="acceptance",dimension="offline_queue_messages"} 1`
	clusterTenantQuotaProcessWaitForMetric(t, ctx, nodes[2], v2RestartReject)
	_ = publisher.Close()

	offlineSubscriber = clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "durable-takeover-sub", false)
	if !clusterDurableProcessSessionPresent(t, offlineSubscriber) {
		t.Fatal("subscriber reconnect after node restart returned SessionPresent=false")
	}
	for index, want := range []struct {
		topic   string
		payload string
	}{{"durable-takeover/quota/v2-one", "v2-one"}, {"durable-takeover/quota/v2-two", "v2-two"}} {
		message := clusterDurableProcessRead(t, offlineSubscriber)
		if message.kind != 3 || message.qos != 1 || message.topic != want.topic || message.payload != want.payload {
			t.Fatalf("restored durable delivery %d = %+v, want %s/%s", index+1, message, want.topic, want.payload)
		}
		clusterDurableProcessWriteFatal(t, offlineSubscriber, 0x40, []byte{byte(message.packetID >> 8), byte(message.packetID)})
	}
	clusterTenantQuotaProcessExpectPingResp(t, offlineSubscriber)
	clusterDurableProcessWriteFatal(t, offlineSubscriber, 0xE0, nil)
	clusterMembershipProcessAssertConnectionClosed(t, offlineSubscriber, 2*time.Second)
	_ = offlineSubscriber.Close()
}

func clusterTenantQuotaProcessProfile(version uint64) *clusterMembershipProcessTenantQuotaProfile {
	limits := clusterMembershipProcessTenantQuotaTenant{
		TenantID: "acceptance", RetainedBytes: 4096, OfflineQueueMessages: 1,
		OfflineQueueBytes: 1 << 20, StorageBytes: 2 << 20,
	}
	if version >= 2 {
		limits.RetainedBytes = 8192
		limits.OfflineQueueMessages = 2
		limits.StorageBytes = 3 << 20
	}
	return &clusterMembershipProcessTenantQuotaProfile{
		Version: version,
		Default: clusterMembershipProcessTenantQuotaLimits{
			RetainedBytes: 16 << 20, OfflineQueueMessages: 8192,
			OfflineQueueBytes: 32 << 20, StorageBytes: 48 << 20,
		},
		Tenants: []clusterMembershipProcessTenantQuotaTenant{limits},
	}
}

func clusterTenantQuotaProcessSetProfile(t *testing.T, path string, profile *clusterMembershipProcessTenantQuotaProfile) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cluster process config %q: %v", path, err)
	}
	var config clusterMembershipProcessConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatalf("decode cluster process config %q: %v", path, err)
	}
	config.TenantQuotas = profile
	clusterMembershipProcessWriteJSON(t, path, config)
}

func clusterTenantQuotaProcessPublishAndAck(t *testing.T, publisher net.Conn, topic string, packetID uint16, payload string) {
	t.Helper()
	clusterDurableProcessPublish(t, publisher, topic, 1, packetID, payload)
	clusterDurableProcessAck(t, publisher, 4, packetID)
}

func clusterTenantQuotaProcessExpectRetainedQuotaClose(t *testing.T, address, token, clientID, topic, payload string) {
	t.Helper()
	connection := clusterDurableProcessConnect(t, address, token, clientID, true)
	defer connection.Close()
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	body = append(body, payload...)
	clusterDurableProcessWriteFatal(t, connection, 0x31, body)
	clusterDurableProcessWriteFatal(t, connection, 0xC0, nil)
	_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer connection.SetReadDeadline(time.Time{})
	for range 2 {
		_, _, _, err := clusterDurableProcessReadRaw(connection)
		if err == nil {
			continue
		}
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			t.Fatal("MQTT 3.1.1 connection remained open after retained quota rejection")
		}
		return
	}
	t.Fatal("retained quota rejection did not close the MQTT 3.1.1 connection")
}

func clusterTenantQuotaProcessExpectPingResp(t *testing.T, connection net.Conn) {
	t.Helper()
	clusterDurableProcessWriteFatal(t, connection, 0xC0, nil)
	_ = connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer connection.SetReadDeadline(time.Time{})
	kind, _, body, err := clusterDurableProcessReadRaw(connection)
	if err != nil || kind != 13 || len(body) != 0 {
		t.Fatalf("expected PINGRESP after draining durable queue, got kind=%d body=%x err=%v", kind, body, err)
	}
}

func clusterTenantQuotaProcessMetrics(t *testing.T, ctx context.Context, process *clusterMembershipProcess) string {
	t.Helper()
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
		process.managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("read %s metrics status=%d err=%v body=%s", process.name, status, err, body)
	}
	return string(body)
}

func clusterTenantQuotaProcessWaitForProfile(t *testing.T, ctx context.Context, process *clusterMembershipProcess, version uint64, mismatch bool) {
	t.Helper()
	mismatchValue := 0
	if mismatch {
		mismatchValue = 1
	}
	clusterMembershipProcessWait(t, ctx, process.name+" committed tenant quota profile", func() (bool, string, error) {
		if err := process.isAlive(); err != nil {
			return false, "", err
		}
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
			process.managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "")
		if err != nil {
			return false, err.Error(), nil
		}
		if status != http.StatusOK {
			return false, fmt.Sprintf("metrics status=%d body=%s", status, body), nil
		}
		want := []string{
			"narumq_tenant_quota_profile_committed 1\n",
			fmt.Sprintf("narumq_tenant_quota_profile_version %d\n", version),
			fmt.Sprintf("narumq_tenant_quota_config_mismatch %d\n", mismatchValue),
		}
		for _, line := range want {
			if !strings.Contains(string(body), line) {
				return false, fmt.Sprintf("metrics missing %q", line), nil
			}
		}
		return true, "", nil
	})
}

func clusterTenantQuotaProcessWaitForMetric(t *testing.T, ctx context.Context, process *clusterMembershipProcess, line string) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, process.name+" tenant quota metric", func() (bool, string, error) {
		if err := process.isAlive(); err != nil {
			return false, "", err
		}
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
			process.managementURL()+"/metrics", nil, clusterMembershipProcessAdmin, "")
		if err != nil {
			return false, err.Error(), nil
		}
		if status != http.StatusOK {
			return false, fmt.Sprintf("metrics status=%d body=%s", status, body), nil
		}
		if !strings.Contains(string(body), line) {
			return false, "missing " + line, nil
		}
		return true, "", nil
	})
}
