package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

var clusterDrainWillReasons = [...]string{
	"suppressed_normal_disconnect", "suppressed_reconnected_within_delay",
	"expected_mqtt311_server_disconnect", "expected_zero_or_expired_will_delay",
	"unexpected_supported_client", "unplanned_failure",
}

// Traceability: DEP-07/DEP-12; docs/specs/drain-and-rolling-update.md §5, §10.
func TestDynamicClusterDrainWillReasonProcessAcceptance(t *testing.T) {
	t.Parallel()
	fixture, nodes := clusterDurableProcessStart(t)
	ctx := fixture.ctx
	clusterSessionMetadataActivate(t, ctx, nodes)
	addr := fixture.mqttAddresses
	safeWill := &clusterSessionMetadataWill{topic: "drain-will/safe", payload: "safe", delay: 8}
	safe := clusterSessionMetadataConnect(t, addr[2], fixture.accessToken, "drain-will-safe", 300, safeWill)
	defer func() { _ = safe.Close() }()
	total := make(map[string]int64)
	for index, round := range []struct {
		drainID         uint64
		ref             string
		observerAddress string
	}{
		{3, addr[0], addr[0]}, {1, addr[1], addr[2]}, {2, addr[2], addr[0]},
	} {
		number := index + 1
		observerID := fmt.Sprintf("drain-will-observer-%d", number)
		observer := clusterDurableProcessConnect(t, round.observerAddress, fixture.accessToken, observerID, true)
		defer observer.Close()
		clusterDurableProcessSubscribe(t, observer, 1, "drain-will/#", 0)
		clusterDrainWillWaitForObserver(t, ctx, nodes, observerID)
		before := clusterDrainWillOutcomes(t, ctx, nodes)
		var v311, zero, expire net.Conn
		if number == 1 {
			v311 = clusterDurableProcessConnectWill(t, addr[2], fixture.accessToken, "drain-will-v311", false, &clusterDurableProcessWill{topic: "drain-will/v311", payload: "v311"})
			defer v311.Close()
			zero = clusterSessionMetadataConnect(t, addr[2], fixture.accessToken, "drain-will-zero", 60, &clusterSessionMetadataWill{topic: "drain-will/zero", payload: "zero", delay: 0})
			defer zero.Close()
			expire = clusterSessionMetadataConnect(t, addr[2], fixture.accessToken, "drain-will-expire", 60, &clusterSessionMetadataWill{topic: "drain-will/expire", payload: "expire", delay: 3})
			defer expire.Close()
			normal := clusterSessionMetadataConnect(t, addr[2], fixture.accessToken, "drain-will-normal", 60, &clusterSessionMetadataWill{topic: "drain-will/normal", payload: "normal", delay: 5})
			defer normal.Close()
			if err := clusterDurableProcessWrite(normal, 0xE0, []byte{0, 0}); err != nil {
				t.Fatal(err)
			}
			clusterMembershipProcessAssertConnectionClosed(t, normal, 3*time.Second)
			_ = normal.Close()
		}
		_, leaderURL := clusterMembershipProcessFindLeader(t, ctx, nodes, 3)
		status, body, err := clusterDrainProcessStart(ctx, leaderURL, round.drainID, clusterDrainProcessRequest{ServerReference: round.ref, ConnectionRatePerSecond: 10, LoadBalancerPropagationDelayMillis: 500, TimeoutMillis: 60000}, fmt.Sprintf("drain-will-%d", number))
		if err != nil || status != http.StatusAccepted {
			t.Fatalf("round %d drain status=%d body=%s err=%v", number, status, body, err)
		}
		clusterDrainWillReadServerDisconnect(t, safe, round.ref)
		_ = safe.Close()
		safe = clusterSessionMetadataConnect(t, round.ref, fixture.accessToken, "drain-will-safe", 300, safeWill)
		safeAt := time.Now()
		if !clusterDurableProcessSessionPresent(t, safe) {
			t.Fatalf("round %d safe reconnect lost session", number)
		}
		if number == 1 {
			clusterDrainWillReadServerDisconnect(t, zero, round.ref)
			clusterDrainWillReadServerDisconnect(t, expire, round.ref)
			clusterMembershipProcessAssertConnectionClosed(t, v311, 20*time.Second)
			_ = zero.Close()
			_ = expire.Close()
			_ = v311.Close()
		}
		witness := nodes[0]
		if round.drainID == 1 {
			witness = nodes[1]
		}
		clusterDrainProcessWaitForPhase(t, ctx, witness, round.drainID, "COMPLETED")
		if number == 1 {
			got := clusterDrainWillCollect(t, observer, 3, 20*time.Second)
			want := map[string]string{"drain-will/v311": "v311", "drain-will/zero": "zero", "drain-will/expire": "expire"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("observed Wills=%v want=%v", got, want)
			}
		}
		remaining := time.Until(safeAt.Add(10 * time.Second))
		if remaining <= 0 {
			t.Fatalf("round %d missed safe Will observation window", number)
		}
		clusterSessionMetadataAssertNoPublish(t, observer, remaining)
		after := clusterDrainWillOutcomes(t, ctx, nodes)
		want := map[string]int64{"suppressed_reconnected_within_delay": 1}
		if number == 1 {
			want["suppressed_normal_disconnect"] = 1
			want["expected_mqtt311_server_disconnect"] = 1
			want["expected_zero_or_expired_will_delay"] = 2
		}
		clusterDrainWillAssertDelta(t, number, before, after, want)
		for _, reason := range clusterDrainWillReasons {
			total[reason] += after[reason] - before[reason]
		}
		_ = observer.Close()
		id := round.drainID
		if err := nodes[id-1].stop(10 * time.Second); err != nil {
			t.Fatal(err)
		}
		restarted, err := fixture.harness.start(fmt.Sprintf("drain will node %d restart", id), fixture.bootstrapConfig(id), fixture.managementAddresses[id-1])
		if err != nil {
			t.Fatal(err)
		}
		nodes[id-1] = restarted
		clusterMembershipProcessWaitForStatus(t, ctx, restarted, "/health/ready", http.StatusOK)
		clusterDrainProcessWaitForNotFound(t, ctx, restarted, id)
	}
	if total["unexpected_supported_client"] != 0 || total["suppressed_reconnected_within_delay"] != 3 {
		t.Fatalf("rolling outcomes=%v", total)
	}
	t.Logf("rolling total=%v", total)
}

func clusterDrainWillOutcomes(t *testing.T, ctx context.Context, nodes []*clusterMembershipProcess) map[string]int64 {
	t.Helper()
	totals := make(map[string]int64, len(clusterDrainWillReasons))
	for _, node := range nodes {
		text := clusterTenantQuotaProcessMetrics(t, ctx, node)
		for _, reason := range clusterDrainWillReasons {
			prefix := fmt.Sprintf("narumq_will_outcomes_total{reason=\"%s\"} ", reason)
			found := false
			for line := range strings.SplitSeq(text, "\n") {
				if strings.HasPrefix(line, prefix) {
					count, err := strconv.ParseInt(strings.TrimPrefix(line, prefix), 10, 64)
					if err != nil || count < 0 {
						t.Fatalf("node %s invalid Will metric %q: %v", node.name, line, err)
					}
					totals[reason] += count
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("node %s missing Will reason %s", node.name, reason)
			}
		}
	}
	return totals
}

func clusterDrainWillAssertDelta(t *testing.T, round int, before, after, want map[string]int64) {
	t.Helper()
	delta := make(map[string]int64, len(clusterDrainWillReasons))
	for _, reason := range clusterDrainWillReasons {
		delta[reason] = after[reason] - before[reason]
		if delta[reason] != want[reason] {
			t.Errorf("round %d %s delta=%d want=%d", round, reason, delta[reason], want[reason])
		}
	}
	t.Logf("round %d delta=%v", round, delta)
	if t.Failed() {
		t.FailNow()
	}
}

func clusterDrainWillWaitForObserver(t *testing.T, ctx context.Context, nodes []*clusterMembershipProcess, clientID string) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, "drain Will observer applied on every node", func() (bool, string, error) {
		for _, node := range nodes {
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, node.managementURL()+"/api/v1/subscriptions?tenantId=acceptance", nil, clusterMembershipProcessAdmin, "")
			if err != nil || status != http.StatusOK {
				return false, fmt.Sprintf("subscriptions status=%d err=%v", status, err), nil
			}
			var envelope struct {
				Data []struct {
					ClientID    string `json:"clientId"`
					TopicFilter string `json:"topicFilter"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &envelope); err != nil {
				return false, "", err
			}
			found := false
			for _, row := range envelope.Data {
				if row.ClientID == clientID && row.TopicFilter == "drain-will/#" {
					found = true
					break
				}
			}
			if !found {
				return false, node.name + " missing observer", nil
			}
		}
		return true, "observer applied", nil
	})
}

func clusterDrainWillReadServerDisconnect(t *testing.T, conn net.Conn, wantReference string) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	kind, _, body, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != 14 {
		t.Fatalf("server DISCONNECT kind=%d body=%x err=%v", kind, body, err)
	}
	clusterDrainProcessAssertServerReference(t, body, wantReference)
}

func clusterDrainWillCollect(t *testing.T, observer net.Conn, count int, timeout time.Duration) map[string]string {
	t.Helper()
	result := make(map[string]string, count)
	deadline := time.Now().Add(timeout)
	for range count {
		packet := clusterSessionMetadataReadPublish(t, observer, time.Until(deadline), false)
		if _, exists := result[packet.topic]; exists {
			t.Fatalf("duplicate Will topic %s", packet.topic)
		}
		result[packet.topic] = packet.payload
	}
	return result
}
