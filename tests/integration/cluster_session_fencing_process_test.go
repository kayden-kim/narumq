package integration

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"narumq/internal/application/membership"
)

// Traceability: CLU-04/CLU-05; docs/specs/cluster-session-and-delivery.md §2, §2.1, §11.15.
func TestDynamicClusterConcurrentConnectProcessAcceptance(t *testing.T) {
	fixture, nodes, controlPaths := clusterFencingProcessStart(t)
	endpoints := []clusterFencingEndpoint{
		{nodeID: 2, address: fixture.mqttAddresses[1]},
		{nodeID: 3, address: fixture.mqttAddresses[2]},
	}

	t.Run("held-race", func(t *testing.T) {
		for _, endpoint := range endpoints {
			controlPath := controlPaths[endpoint.nodeID-1]
			clusterFencingWriteHold(t, controlPath, "ownership")
			t.Cleanup(func() { clusterDurableFaultClearControl(t, controlPath) })
		}
		resultChannel := clusterFencingStartConnectSimultaneously(endpoints, fixture.accessToken, "concurrent-held")
		for _, endpoint := range endpoints {
			clusterFencingWaitForLog(t, nodes[endpoint.nodeID-1],
				"narumq failpoint holding: kind=ownership client=concurrent-held expected_epoch=0", 10*time.Second)
		}
		for _, endpoint := range endpoints {
			clusterDurableFaultClearControl(t, controlPaths[endpoint.nodeID-1])
		}
		results := clusterFencingCollectConnectResults(resultChannel, len(endpoints))

		var winner clusterFencingConnectionResult
		var accepted, rejected int
		for _, result := range results {
			if result.err != nil {
				t.Fatalf("node %d held CONNECT: %v", result.nodeID, result.err)
			}
			switch result.code {
			case 0:
				accepted++
				winner = result
			case 3:
				rejected++
			default:
				t.Fatalf("node %d held CONNECT return code=%d, want 0 or 3", result.nodeID, result.code)
			}
		}
		if accepted != 1 || rejected != 1 || winner.conn == nil {
			t.Fatalf("held CONNECT results=%+v, want exactly one accepted and one server-unavailable result", results)
		}
		defer winner.conn.Close()

		clusterFencingWaitForLog(t, nodes[winner.nodeID-1],
			"narumq failpoint held result: kind=ownership client=concurrent-held expected_epoch=0 outcome=applied", 5*time.Second)
		loserID := uint64(2)
		if winner.nodeID == 2 {
			loserID = 3
		}
		clusterFencingWaitForLog(t, nodes[loserID-1],
			"narumq failpoint held result: kind=ownership client=concurrent-held expected_epoch=0 outcome=conflict conflict=stale_epoch", 5*time.Second)

		publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "matrix-pub", true)
		defer publisher.Close()
		clusterDurableProcessSubscribe(t, winner.conn, 1, "concurrent/held", 1)
		clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, nodes, "concurrent-held|concurrent/held|1")
		clusterDurableProcessPublish(t, publisher, "concurrent/held", 1, 610, "after-race")
		clusterDurableProcessAck(t, publisher, 4, 610)
		delivery := clusterDurableProcessRead(t, winner.conn)
		if delivery.kind != 3 || delivery.topic != "concurrent/held" || delivery.payload != "after-race" || delivery.qos != 1 || delivery.dup {
			t.Fatalf("held-race delivery = %+v, want one fresh QoS1 after-race PUBLISH", delivery)
		}
		clusterFencingAck(t, winner.conn, delivery.packetID)
	})

	t.Run("natural-race", func(t *testing.T) {
		for round := 1; round <= 10; round++ {
			clientID := fmt.Sprintf("concurrent-race-%d", round)
			results := clusterFencingConnectSimultaneously(endpoints, fixture.accessToken, clientID)
			var live []clusterFencingConnectionResult
			for _, result := range results {
				if result.err != nil {
					t.Fatalf("round %d node %d CONNECT: %v", round, result.nodeID, result.err)
				}
				if result.code != 0 && result.code != 3 {
					t.Fatalf("round %d node %d CONNECT return code=%d, want 0 or 3", round, result.nodeID, result.code)
				}
				if result.conn != nil && clusterFencingAlive(result.conn) {
					live = append(live, result)
				}
			}
			if len(live) != 1 {
				for _, result := range results {
					if result.conn != nil {
						_ = result.conn.Close()
					}
				}
				t.Fatalf("round %d accepted/live connections=%v, want exactly one", round, clusterFencingLiveNodeIDs(live))
			}
			winner := live[0]
			filter := fmt.Sprintf("concurrent/race/%d", round)
			clusterDurableProcessSubscribe(t, winner.conn, uint16(round), filter, 1)
			_ = winner.conn.Close()
			for _, result := range results {
				if result.conn != nil && result.nodeID != winner.nodeID {
					_ = result.conn.Close()
				}
			}
		}
	})
}

// Traceability: CLU-05/CLU-06; docs/specs/cluster-session-and-delivery.md §2.1, §2.3, §11.15.
func TestDynamicClusterStaleEpochFencingProcessAcceptance(t *testing.T) {
	fixture, nodes, controlPaths := clusterFencingProcessStart(t)
	publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "stale-pub", true)
	defer publisher.Close()
	want := make([]string, 0, 2)

	t.Run("stale-subscription", func(t *testing.T) {
		oldOwner := clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "stale-owner", false)
		defer oldOwner.Close()
		clusterDurableProcessSubscribe(t, oldOwner, 1, "stale/a", 1)
		want = append(want, "stale-owner|stale/a|1")
		clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, nodes, want...)

		clusterFencingWriteHold(t, controlPaths[2], "subscription")
		t.Cleanup(func() { clusterDurableFaultClearControl(t, controlPaths[2]) })
		filter := "stale/b"
		body := []byte{0, 2, 0, byte(len(filter))}
		body = append(body, filter...)
		body = append(body, 1)
		clusterDurableProcessWriteFatal(t, oldOwner, 0x82, body)
		clusterFencingWaitForLog(t, nodes[2],
			"narumq failpoint holding: kind=subscription client=stale-owner filter=stale/b epoch=1", 10*time.Second)

		newOwner := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "stale-owner", false)
		defer newOwner.Close()
		if !clusterDurableProcessSessionPresent(t, newOwner) {
			t.Fatal("stale-owner takeover did not resume its persistent session")
		}
		clusterDurableProcessAssertNoSuccess(t, oldOwner, 9, 2)
		clusterMembershipProcessAssertConnectionClosed(t, oldOwner, 5*time.Second)
		clusterDurableFaultClearControl(t, controlPaths[2])
		clusterFencingWaitForLog(t, nodes[2],
			"narumq failpoint held result: kind=subscription client=stale-owner filter=stale/b epoch=1 outcome=conflict conflict=stale_epoch", 10*time.Second)

		clusterDurableProcessPublish(t, publisher, "stale/b", 1, 701, "must-not-arrive")
		clusterDurableProcessAck(t, publisher, 4, 701)
		clusterDurableProcessPublish(t, publisher, "stale/a", 1, 702, "sentinel-a")
		clusterDurableProcessAck(t, publisher, 4, 702)
		delivery := clusterDurableProcessRead(t, newOwner)
		if delivery.kind != 3 || delivery.topic != "stale/a" || delivery.payload != "sentinel-a" || delivery.qos != 1 {
			t.Fatalf("new owner received %+v, want only stale/a sentinel", delivery)
		}
		clusterFencingAck(t, newOwner, delivery.packetID)
		clusterMatrixAssertQuiet(t, newOwner, time.Second)
		clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, nodes, want...)
	})

	t.Run("stale-inflight-complete", func(t *testing.T) {
		oldOwner := clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "stale-inflight", false)
		defer oldOwner.Close()
		clusterDurableProcessSubscribe(t, oldOwner, 1, "stale/inflight", 1)
		want = append(want, "stale-inflight|stale/inflight|1")
		clusterDurableProcessWaitForSubscriptions(t, fixture.ctx, nodes, want...)

		clusterDurableProcessPublish(t, publisher, "stale/inflight", 1, 703, "m1")
		clusterDurableProcessAck(t, publisher, 4, 703)
		original := clusterDurableProcessRead(t, oldOwner)
		if original.kind != 3 || original.topic != "stale/inflight" || original.payload != "m1" || original.qos != 1 || original.dup || original.packetID == 0 {
			t.Fatalf("initial inflight delivery = %+v, want fresh QoS1 m1", original)
		}

		clusterFencingWriteHold(t, controlPaths[2], "delivery")
		t.Cleanup(func() { clusterDurableFaultClearControl(t, controlPaths[2]) })
		clusterDurableProcessWriteFatal(t, oldOwner, 0x40, []byte{byte(original.packetID >> 8), byte(original.packetID)})
		action := uint32(membership.DeliveryActionComplete)
		clusterFencingWaitForLog(t, nodes[2],
			fmt.Sprintf("narumq failpoint holding: kind=delivery client=stale-inflight action=%d epoch=1", action), 10*time.Second)

		newOwner := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "stale-inflight", false)
		if !clusterDurableProcessSessionPresent(t, newOwner) {
			t.Fatal("stale-inflight takeover did not resume its persistent session")
		}
		replay := clusterDurableProcessRead(t, newOwner)
		if replay.kind != 3 || replay.topic != original.topic || replay.payload != original.payload || replay.qos != 1 || !replay.dup || replay.packetID != original.packetID {
			t.Fatalf("takeover replay = %+v, want duplicate m1 with packet ID %d", replay, original.packetID)
		}
		clusterMembershipProcessAssertConnectionClosed(t, oldOwner, 5*time.Second)
		clusterDurableFaultClearControl(t, controlPaths[2])
		clusterFencingWaitForLog(t, nodes[2], fmt.Sprintf(
			"narumq failpoint held result: kind=delivery client=stale-inflight action=%d epoch=1 outcome=conflict conflict=stale_epoch", action), 10*time.Second)

		clusterDurableProcessWriteFatal(t, newOwner, 0xE0, nil)
		_ = newOwner.Close()
		thirdOwner := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "stale-inflight", false)
		defer thirdOwner.Close()
		if !clusterDurableProcessSessionPresent(t, thirdOwner) {
			t.Fatal("third owner did not resume the inflight persistent session")
		}
		replay = clusterDurableProcessRead(t, thirdOwner)
		if replay.kind != 3 || replay.topic != original.topic || replay.payload != original.payload || replay.qos != 1 || !replay.dup || replay.packetID != original.packetID {
			t.Fatalf("post-stale-complete replay = %+v, want duplicate m1 with packet ID %d", replay, original.packetID)
		}
		clusterFencingAck(t, thirdOwner, replay.packetID)

		clusterDurableProcessPublish(t, publisher, "stale/inflight", 1, 704, "sentinel-i")
		clusterDurableProcessAck(t, publisher, 4, 704)
		sentinel := clusterDurableProcessRead(t, thirdOwner)
		if sentinel.kind != 3 || sentinel.topic != "stale/inflight" || sentinel.payload != "sentinel-i" || sentinel.qos != 1 {
			t.Fatalf("post-completion delivery = %+v, want sentinel-i", sentinel)
		}
		clusterFencingAck(t, thirdOwner, sentinel.packetID)
		clusterMatrixAssertQuiet(t, thirdOwner, time.Second)
	})
}

type clusterFencingEndpoint struct {
	nodeID  uint64
	address string
}

type clusterFencingConnectionResult struct {
	nodeID uint64
	conn   net.Conn
	code   byte
	err    error
}

func clusterFencingProcessStart(t *testing.T) (*clusterMembershipProcessFixture, []*clusterMembershipProcess, []string) {
	t.Helper()
	fixture := newClusterMembershipProcessFixture(t, 3, "narumq_failpoint")
	controlPaths := make([]string, 3)
	nodes := make([]*clusterMembershipProcess, 3)
	for nodeID := uint64(1); nodeID <= 3; nodeID++ {
		controlPath := filepath.Join(fixture.tempDir, fmt.Sprintf("node-%d-fault.json", nodeID))
		controlPaths[nodeID-1] = controlPath
		process, err := fixture.harness.launch(fmt.Sprintf("cluster fencing node %d", nodeID),
			fixture.bootstrapConfig(nodeID), fixture.managementAddresses[nodeID-1],
			[]string{"NARUMQ_FAILPOINT_CONTROL=" + controlPath})
		if err != nil {
			t.Fatalf("start failpoint node %d: %v", nodeID, err)
		}
		nodes[nodeID-1] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
	return fixture, nodes, controlPaths
}

func clusterFencingWriteHold(t *testing.T, path string, kinds ...string) {
	t.Helper()
	data, err := json.Marshal(struct {
		HoldCommands []string `json:"holdCommands"`
	}{HoldCommands: kinds})
	if err != nil {
		t.Fatalf("encode hold control file: %v", err)
	}
	clusterFaultWriteControlJSON(t, path, data)
}

func clusterFencingWaitForLog(t *testing.T, process *clusterMembershipProcess, marker string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		logs := process.logs.String()
		if strings.Contains(logs, marker) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s did not log marker %q within %s; logs:\n%s", process.name, marker, timeout, process.logs.String())
}

func clusterFencingConnectOnce(address, accessToken, clientID string) (net.Conn, byte, error) {
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return nil, 0, fmt.Errorf("dial %s: %w", address, err)
	}
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0xC0, 0, 60}
	for _, field := range []string{clientID, "jwt", accessToken} {
		body = append(body, byte(len(field)>>8), byte(len(field)))
		body = append(body, field...)
	}
	if err := clusterDurableProcessWrite(connection, 0x10, body); err != nil {
		_ = connection.Close()
		return nil, 0, fmt.Errorf("send CONNECT: %w", err)
	}
	kind, _, response, err := clusterDurableProcessReadRaw(connection)
	if err != nil {
		_ = connection.Close()
		return nil, 0, fmt.Errorf("read CONNACK: %w", err)
	}
	if kind != 2 || len(response) != 2 {
		_ = connection.Close()
		return nil, 0, fmt.Errorf("invalid CONNACK kind=%d body=%x", kind, response)
	}
	code := response[1]
	if code != 0 {
		_ = connection.Close()
		return nil, code, nil
	}
	_ = connection.SetDeadline(time.Time{})
	return connection, code, nil
}

func clusterFencingConnectSimultaneously(endpoints []clusterFencingEndpoint, accessToken, clientID string) []clusterFencingConnectionResult {
	return clusterFencingCollectConnectResults(
		clusterFencingStartConnectSimultaneously(endpoints, accessToken, clientID), len(endpoints))
}

func clusterFencingStartConnectSimultaneously(endpoints []clusterFencingEndpoint, accessToken, clientID string) <-chan clusterFencingConnectionResult {
	start := make(chan struct{})
	results := make(chan clusterFencingConnectionResult, len(endpoints))
	var ready sync.WaitGroup
	ready.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint clusterFencingEndpoint) {
			ready.Done()
			<-start
			connection, code, err := clusterFencingConnectOnce(endpoint.address, accessToken, clientID)
			results <- clusterFencingConnectionResult{nodeID: endpoint.nodeID, conn: connection, code: code, err: err}
		}(endpoint)
	}
	ready.Wait()
	close(start)
	return results
}

func clusterFencingCollectConnectResults(results <-chan clusterFencingConnectionResult, count int) []clusterFencingConnectionResult {
	all := make([]clusterFencingConnectionResult, 0, count)
	for range count {
		all = append(all, <-results)
	}
	return all
}

func clusterFencingAck(t *testing.T, connection net.Conn, packetID uint16) {
	t.Helper()
	clusterDurableProcessWriteFatal(t, connection, 0x40, []byte{byte(packetID >> 8), byte(packetID)})
}

func clusterFencingAlive(connection net.Conn) bool {
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	if err := clusterDurableProcessWrite(connection, 0xC0, nil); err != nil {
		_ = connection.SetDeadline(time.Time{})
		return false
	}
	kind, _, body, err := clusterDurableProcessReadRaw(connection)
	_ = connection.SetDeadline(time.Time{})
	if err != nil {
		if clusterMembershipProcessIsClosed(err) {
			return false
		}
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return false
		}
		return false
	}
	return kind == 13 && len(body) == 0
}

func clusterFencingLiveNodeIDs(results []clusterFencingConnectionResult) []uint64 {
	nodeIDs := make([]uint64, len(results))
	for index, result := range results {
		nodeIDs[index] = result.nodeID
	}
	return nodeIDs
}
