package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.5, §11.28.
func TestDynamicClusterPublishBackpressureProcessAcceptance(t *testing.T) {
	fixture, nodes, controls := clusterFencingProcessStart(t)
	defer clusterDurableFaultClearControl(t, controls[0])
	connectV5 := func(node int, id string) net.Conn {
		t.Helper()
		conn, _, code, err := clusterDrainProcessConnectV5(fixture.mqttAddresses[node-1], fixture.accessToken, id, false)
		if err != nil || code != 0 {
			t.Fatalf("connect %s: code=%x err=%v", id, code, err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	subscriber := connectV5(3, "bp-sub")
	clusterDrainProcessSubscribeV5(t, subscriber, 1, "bp/#", 1)
	clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, nodes, "acceptance", "bp-sub|bp/#|1")
	seen := map[string]int{}
	live := 0
	consumeUntil := func(want string) {
		t.Helper()
		for {
			delivery := clusterMatrixReadPublish(t, subscriber, true, 10*time.Second)
			if delivery.qos == 0 {
				if delivery.topic != "bp/disk/live" {
					t.Fatalf("unexpected live delivery %+v", delivery)
				}
				live++
			} else {
				if seen[delivery.topic] == 0 || !delivery.dup {
					seen[delivery.topic]++
				}
				clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(delivery.packetID >> 8), byte(delivery.packetID)})
			}
			if delivery.topic == want {
				return
			}
		}
	}
	t.Run("commit-lag", func(t *testing.T) {
		held := connectV5(1, "bp-held")
		rejected := connectV5(1, "bp-rejected")
		legacy := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "bp-closed", true)
		defer legacy.Close()
		other := connectV5(2, "bp-other")
		clusterFencingWriteHold(t, controls[0], "delivery")
		clusterBackpressurePublishV5(t, held, "bp/lag/held", 1, 1, false)
		clusterFencingWaitForLog(t, nodes[0], "narumq failpoint holding: kind=delivery", 3*time.Second)
		time.Sleep(1200 * time.Millisecond)
		clusterBackpressurePublishV5(t, rejected, "bp/lag/rejected", 1, 1, false)
		if code := clusterBackpressureReadAck(t, rejected, 0x40, 1); code != 0x97 {
			t.Fatalf("lag PUBACK=%x", code)
		}
		clusterDurableProcessPublish(t, legacy, "bp/lag/closed", 1, 1, "value")
		clusterMembershipProcessAssertConnectionClosed(t, legacy, 2*time.Second)
		clusterBackpressurePublishV5(t, other, "bp/lag/other", 1, 1, false)
		if code := clusterBackpressureReadAck(t, other, 0x40, 1); code != 0 {
			t.Fatalf("healthy node PUBACK=%x", code)
		}
		clusterBackpressureWaitMetric(t, fixture.ctx, nodes[0], `narumq_publish_backpressure_rejections_total{reason="commit_lag"} 2`, 5*time.Second)
		clusterDurableFaultClearControl(t, controls[0])
		_ = held.SetReadDeadline(time.Now().Add(10 * time.Second))
		if code := clusterBackpressureReadAckDeadline(t, held, 0x40, 1); code != 0 {
			t.Fatalf("held PUBACK=%x", code)
		}
		clusterBackpressurePublishV5(t, rejected, "bp/lag/retry", 1, 2, false)
		if code := clusterBackpressureReadAck(t, rejected, 0x40, 2); code != 0 {
			t.Fatalf("retry PUBACK=%x", code)
		}
	})
	t.Run("disk-pressure", func(t *testing.T) {
		publisher := connectV5(1, "bp-disk")
		clusterFaultWriteControlJSON(t, controls[0], []byte(`{"freeBytes":1024}`))
		clusterBackpressureWaitMetric(t, fixture.ctx, nodes[0], `narumq_publish_backpressure_active{reason="disk_free"} 1`, 5*time.Second)
		clusterBackpressurePublishV5(t, publisher, "bp/disk/q1", 1, 3, false)
		if code := clusterBackpressureReadAck(t, publisher, 0x40, 3); code != 0x97 {
			t.Fatalf("disk PUBACK=%x", code)
		}
		clusterBackpressurePublishV5(t, publisher, "bp/disk/q2", 2, 4, false)
		if code := clusterBackpressureReadAck(t, publisher, 0x50, 4); code != 0x97 {
			t.Fatalf("disk PUBREC=%x", code)
		}
		clusterBackpressurePublishV5(t, publisher, "bp/disk/retained", 0, 0, true)
		_ = publisher.SetReadDeadline(time.Now().Add(2 * time.Second))
		kind, _, body, err := clusterDurableProcessReadRaw(publisher)
		if err != nil || kind != 14 || len(body) == 0 || body[0] != 0x97 {
			t.Fatalf("retained DISCONNECT=%x err=%v kind=%d", body, err, kind)
		}
		clusterMembershipProcessAssertConnectionClosed(t, publisher, 2*time.Second)
		livePublisher := connectV5(1, "bp-live")
		clusterBackpressurePublishV5(t, livePublisher, "bp/disk/live", 0, 0, false)
		consumeUntil("bp/disk/live")
		status, body, err := clusterMembershipProcessRequest(fixture.ctx, http.MethodGet, nodes[0].managementURL()+"/health/ready", nil, clusterMembershipProcessAdmin, "")
		if err != nil || status != http.StatusOK {
			t.Fatalf("ready=%d %s %v", status, body, err)
		}
		clusterBackpressureAssertStorage(t, fixture.ctx, nodes[0], true)
		if text := clusterTenantQuotaProcessMetrics(t, fixture.ctx, nodes[0]); !strings.Contains(text, "narumq_storage_free_bytes 1024\n") {
			t.Fatal("free bytes gauge missing")
		}
		clusterDurableFaultClearControl(t, controls[0])
		clusterBackpressureWaitMetric(t, fixture.ctx, nodes[0], `narumq_publish_backpressure_active{reason="disk_free"} 0`, 5*time.Second)
		clusterBackpressurePublishV5(t, livePublisher, "bp/disk/after", 1, 5, false)
		if code := clusterBackpressureReadAck(t, livePublisher, 0x40, 5); code != 0 {
			t.Fatalf("recovered PUBACK=%x", code)
		}
		clusterBackpressureAssertStorage(t, fixture.ctx, nodes[0], false)
	})
	if t.Failed() {
		return
	}
	sentinel := connectV5(2, "bp-sentinel")
	clusterBackpressurePublishV5(t, sentinel, "bp/sentinel", 1, 6, false)
	if code := clusterBackpressureReadAck(t, sentinel, 0x40, 6); code != 0 {
		t.Fatalf("sentinel PUBACK=%x", code)
	}
	consumeUntil("bp/sentinel")
	want := map[string]int{"bp/lag/held": 1, "bp/lag/other": 1, "bp/lag/retry": 1, "bp/disk/after": 1, "bp/sentinel": 1}
	if !reflect.DeepEqual(seen, want) || live != 1 {
		t.Fatalf("delivered durable=%v live=%d; want %v live=1", seen, live, want)
	}
	t.Logf("exact durable topic set %v; QoS0 live=%d; rejected topics absent", seen, live)
}

func clusterBackpressurePublishV5(t *testing.T, conn net.Conn, topic string, qos byte, id uint16, retain bool) {
	t.Helper()
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	if qos > 0 {
		body = append(body, byte(id>>8), byte(id))
	}
	body = append(body, 0)
	body = append(body, "value"...)
	header := byte(0x30) | qos<<1
	if retain {
		header |= 1
	}
	clusterDurableProcessWriteFatal(t, conn, header, body)
}
func clusterBackpressureReadAck(t *testing.T, conn net.Conn, wantHeader byte, id uint16) byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	return clusterBackpressureReadAckDeadline(t, conn, wantHeader, id)
}
func clusterBackpressureReadAckDeadline(t *testing.T, conn net.Conn, wantHeader byte, id uint16) byte {
	t.Helper()
	defer conn.SetReadDeadline(time.Time{})
	kind, _, body, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != wantHeader>>4 || len(body) < 2 || binary.BigEndian.Uint16(body[:2]) != id {
		t.Fatalf("ACK kind=%d body=%x err=%v want=%x id=%d", kind, body, err, wantHeader, id)
	}
	if len(body) == 2 {
		return 0
	}
	return body[2]
}
func clusterBackpressureWaitMetric(t *testing.T, parent context.Context, node *clusterMembershipProcess, line string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	clusterTenantQuotaProcessWaitForMetric(t, ctx, node, line)
}
func clusterBackpressureAssertStorage(t *testing.T, ctx context.Context, node *clusterMembershipProcess, want bool) {
	t.Helper()
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, node.managementURL()+"/api/v1/nodes/1", nil, clusterMembershipProcessAdmin, "")
	var response struct {
		Data struct {
			StoragePressure   bool   `json:"storagePressure"`
			ReplicationHealth string `json:"replicationHealth"`
		}
	}
	if err != nil || status != http.StatusOK {
		t.Fatalf("node summary=%d %s %v", status, body, err)
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Data.StoragePressure != want || response.Data.ReplicationHealth != "HEALTHY" {
		t.Fatalf("node pressure summary=%s want pressure=%v healthy", body, want)
	}
}
