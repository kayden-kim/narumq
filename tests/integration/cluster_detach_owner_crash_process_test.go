package integration

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §11.26.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-185, cluster-session-and-delivery.md:CLU-SCN-186
func TestDynamicClusterDetachOwnerCrashProcessAcceptance(t *testing.T) {
	t.Parallel()
	for _, boundary := range []struct {
		name  string
		point string
	}{
		{"before-commit", "offline-detach-before-commit"},
		{"after-commit", "offline-detach-after-commit"},
	} {
		t.Run(boundary.name, func(t *testing.T) {
			clusterDetachOwnerCrashScenario(t, boundary.point)
		})
	}
}

func clusterDetachOwnerCrashScenario(t *testing.T, point string) {
	t.Helper()
	const (
		clientID      = "detach-crash"
		filter        = "detach/crash/#"
		liveTopic     = "detach/crash/live"
		outageTopic   = "detach/crash/outage"
		afterTopic    = "detach/crash/after-recovery"
		sentinelTopic = "detach/crash/sentinel"
	)
	fixture := newClusterMembershipProcessFixture(t, 3, "narumq_failpoint")
	nodes := make([]*clusterMembershipProcess, 3)
	for i := range nodes {
		var err error
		if i == 2 {
			nodes[i], err = fixture.harness.startArmed("detach crash node 3", fixture.bootstrapConfig(3), fixture.managementAddresses[2], point)
		} else {
			nodes[i], err = fixture.harness.start(fmt.Sprintf("detach crash node %d", i+1), fixture.bootstrapConfig(uint64(i+1)), fixture.managementAddresses[i])
		}
		if err != nil {
			t.Fatalf("start node %d: %v", i+1, err)
		}
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
	clusterSessionMetadataActivate(t, fixture.ctx, nodes)

	// This is the only persistent session on the armed node.
	subscriber := clusterSessionMetadataConnect(t, fixture.mqttAddresses[2], fixture.accessToken, clientID, 300, nil)
	defer subscriber.Close()
	clusterSessionMetadataSubscribe(t, subscriber, 41, filter)
	clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, nodes, "acceptance", clientID+"|"+filter+"|1")
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[0], fixture.accessToken, "detach-crash-pub-live", liveTopic, []byte("live"))
	live := clusterSessionMetadataReadPublish(t, subscriber, 5*time.Second, true)
	if live.topic != liveTopic || live.payload != "live" || live.qos != 0 {
		t.Fatalf("live delivery = %+v, want QoS 0 live", live)
	}
	if err := subscriber.Close(); err != nil {
		t.Fatal(err)
	}
	clusterMembershipProcessRequireFailpointExit(t, nodes[2], point)
	client := clusterSessionMetadataGetClient(t, fixture.ctx, nodes[0], clientID)
	if !client.Connected || client.NodeID != "3" {
		t.Fatalf("crashed owner metadata = %+v, want connected node 3", client)
	}
	t.Logf("%s: owner exited 86 with failpoint marker; metadata Connected=true NodeID=3", point)
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[0], fixture.accessToken, "detach-crash-pub-outage", outageTopic, []byte("outage"))
	if point == "offline-detach-before-commit" {
		time.Sleep(time.Second)
		client = clusterSessionMetadataGetClient(t, fixture.ctx, nodes[0], clientID)
		if client.OfflineQueueCount != 0 {
			t.Fatalf("before-commit outage queue = %d, want 0", client.OfflineQueueCount)
		}
	} else {
		outageCtx, cancel := context.WithTimeout(fixture.ctx, 10*time.Second)
		defer cancel()
		clusterMembershipProcessWait(t, outageCtx, "outage captured while owner is stopped", func() (bool, string, error) {
			view, status, err := clusterSessionMetadataTryGetClient(outageCtx, nodes[0], clientID)
			if err != nil || status != http.StatusOK {
				return false, fmt.Sprintf("client status=%d err=%v", status, err), nil
			}
			client = view
			return view.OfflineQueueCount == 1, fmt.Sprintf("client=%+v", view), nil
		})
	}
	t.Logf("%s: stopped owner OfflineQueueCount=%d", point, client.OfflineQueueCount)

	// Disarm the restart: recovery itself crosses DetachSession again.
	restarted, err := fixture.harness.start("detach crash node 3 restart", fixture.bootstrapConfig(3), fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart node 3: %v", err)
	}
	nodes[2] = restarted
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
	recoveryCtx, cancelRecovery := context.WithTimeout(fixture.ctx, 30*time.Second)
	defer cancelRecovery()
	base := -1
	clusterMembershipProcessWait(t, recoveryCtx, "recovered metadata detached with expiry", func() (bool, string, error) {
		view, status, err := clusterSessionMetadataTryGetClient(recoveryCtx, nodes[0], clientID)
		if err != nil || status != http.StatusOK {
			return false, fmt.Sprintf("client status=%d err=%v", status, err), nil
		}
		if view.Connected || view.ExpiryAt == nil {
			return false, fmt.Sprintf("client=%+v", view), nil
		}
		base = view.OfflineQueueCount
		return true, fmt.Sprintf("client=%+v", view), nil
	})
	if point == "offline-detach-after-commit" && base != 1 {
		t.Fatalf("after-commit recovered queue = %d, want 1", base)
	}
	// A queued peer frame may reach the registered queue before recovery DETACH.
	if base != 0 && base != 1 {
		t.Fatalf("recovered queue = %d, want 0 or 1", base)
	}
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[0], fixture.accessToken, "detach-crash-pub-after", afterTopic, []byte("after-recovery"))
	captureCtx, cancelCapture := context.WithTimeout(fixture.ctx, 10*time.Second)
	defer cancelCapture()
	clusterMembershipProcessWait(t, captureCtx, "capture resumed after owner recovery", func() (bool, string, error) {
		view, status, err := clusterSessionMetadataTryGetClient(captureCtx, nodes[0], clientID)
		if err != nil || status != http.StatusOK {
			return false, fmt.Sprintf("client status=%d err=%v", status, err), nil
		}
		return view.OfflineQueueCount == base+1, fmt.Sprintf("client=%+v base=%d", view, base), nil
	})

	resumed := clusterSessionMetadataConnect(t, fixture.mqttAddresses[1], fixture.accessToken, clientID, 300, nil)
	defer resumed.Close()
	if !clusterDurableProcessSessionPresent(t, resumed) {
		t.Fatal("reconnect CONNACK returned SessionPresent=false")
	}
	if base == 1 {
		outage := clusterSessionMetadataReadPublish(t, resumed, 5*time.Second, true)
		if outage.topic != outageTopic || outage.payload != "outage" || outage.qos != 0 {
			t.Fatalf("first offline delivery = %+v, want QoS 0 outage", outage)
		}
	}
	after := clusterSessionMetadataReadPublish(t, resumed, 5*time.Second, true)
	if after.topic != afterTopic || after.payload != "after-recovery" || after.qos != 0 {
		t.Fatalf("recovered delivery = %+v, want QoS 0 after-recovery", after)
	}
	clusterSessionMetadataAssertNoPublish(t, resumed, 2*time.Second)
	// Publish on the new owner to avoid a stale remote ownership projection.
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "detach-crash-pub-sentinel", sentinelTopic, []byte("sentinel"))
	sentinel := clusterSessionMetadataReadPublish(t, resumed, 5*time.Second, true)
	if sentinel.topic != sentinelTopic || sentinel.payload != "sentinel" || sentinel.qos != 0 {
		t.Fatalf("sentinel delivery = %+v, want QoS 0 sentinel", sentinel)
	}
	t.Logf("%s: recovery Connected=false with expiry, base=%d; capture=%d; SessionPresent=1; FIFO replay, no live replay or duplicate, sentinel received", point, base, base+1)
}
