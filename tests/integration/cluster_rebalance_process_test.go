package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

type clusterRebalanceProcessOperation struct {
	OperationID                          string `json:"operationId"`
	State                                string `json:"state"`
	TotalConnections, FairShare, Ceiling uint64
	Nodes                                []struct {
		NodeID              string `json:"nodeId"`
		Connections         uint64 `json:"connections"`
		SessionsTransferred uint64 `json:"sessionsTransferred"`
		Shedding            bool   `json:"shedding"`
	} `json:"nodes"`
}

type clusterRebalanceProcessClient struct {
	id   string
	v5   bool
	node int
	conn net.Conn
}

type clusterRebalanceProcessRedirect struct {
	client *clusterRebalanceProcessClient
	kind   byte
	body   []byte
	err    error
}

// Traceability: CLU-13; docs/specs/cluster-session-and-delivery.md §9, §11.31.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-226
func TestDynamicClusterConnectionRebalanceProcessAcceptance(t *testing.T) {
	t.Parallel()
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	nodes := clusterDrainProcessStartCluster(t, fixture)
	ctx := fixture.ctx
	baseURL := nodes[0].managementURL()
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, baseURL+"/api/v1/cluster/rebalance", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusNotFound {
		t.Fatalf("missing rebalance status=%d body=%s err=%v", status, body, err)
	}
	clusterRebalanceProcessAssertError(t, body, "resource_not_found")
	clusterProcessActivateFeature(t, ctx, nodes, "connection-rebalance")

	clients := make([]*clusterRebalanceProcessClient, 0, 18)
	counts := []uint64{0, 0, 0}
	want := make(map[string]clusterDrainProcessSubscription)
	for i := range 16 {
		id := fmt.Sprintf("rb-v5-%02d", i)
		node := 0
		if i >= 10 {
			node = 1
		}
		conn, present, reason, err := clusterDrainProcessConnectV5(fixture.mqttAddresses[node], fixture.accessToken, id, false)
		if err != nil || reason != 0 || present {
			t.Fatalf("initial %s present=%v reason=%x err=%v", id, present, reason, err)
		}
		client := &clusterRebalanceProcessClient{id: id, v5: true, node: node, conn: conn}
		clients = append(clients, client)
		counts[node]++
		t.Cleanup(func() { _ = client.conn.Close() })
		filter := "rebalance/" + id
		clusterDrainProcessSubscribeV5(t, conn, 1, filter, 1)
		want[id] = clusterDrainProcessSubscription{topicFilter: filter, qos: 1}
	}
	for i := range 2 {
		id := fmt.Sprintf("rb-v3-%02d", i)
		filter := "rebalance/" + id
		conn := clusterMembershipProcessOpenSubscribedSession(t, fixture.mqttAddresses[0], fixture.accessToken, id, filter)
		client := &clusterRebalanceProcessClient{id: id, node: 0, conn: conn}
		clients = append(clients, client)
		counts[0]++
		t.Cleanup(func() { _ = client.conn.Close() })
		want[id] = clusterDrainProcessSubscription{topicFilter: filter, qos: 0}
	}
	publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "rb-pub", false)
	defer publisher.Close()
	counts[1]++
	clusterDrainProcessWaitForRouteProjection(t, ctx, nodes, want)
	redirects := make(chan clusterRebalanceProcessRedirect, len(clients))
	var readers sync.WaitGroup
	for _, client := range clients {
		readers.Go(func() {
			kind, _, body, err := clusterDurableProcessReadRaw(client.conn)
			redirects <- clusterRebalanceProcessRedirect{client: client, kind: kind, body: body, err: err}
		})
	}
	// Always unblock and join readers before the test fixture stops its brokers.
	defer func() {
		for _, client := range clients {
			_ = client.conn.Close()
		}
		readers.Wait()
	}()
	request := map[string]any{"serverReference": "lb.rebalance.test:1883", "maxDeviationConnections": 1, "connectionRatePerSecond": 5, "loadBalancerPropagationDelayMillis": 60000, "timeoutMillis": 120000}
	post := func(key string) (int, []byte, error) {
		encoded, err := json.Marshal(request)
		if err != nil {
			return 0, nil, err
		}
		return clusterMembershipProcessRequest(ctx, http.MethodPost, baseURL+"/api/v1/cluster/rebalance", encoded, clusterMembershipProcessAdmin, key)
	}
	status, body, err = post("rb-cancel")
	cancelledStart := clusterRebalanceProcessDecode(t, body)
	if err != nil || status != http.StatusAccepted || cancelledStart.State != "RUNNING" || len(cancelledStart.Nodes) != 3 {
		t.Fatalf("cancel start status=%d operation=%+v body=%s err=%v", status, cancelledStart, body, err)
	}
	readyCtx, cancelReady := context.WithTimeout(ctx, 10*time.Second)
	clusterMembershipProcessWaitForStatus(t, readyCtx, nodes[0], "/health/ready", http.StatusServiceUnavailable)
	cancelReady()
	reason, err := clusterRebalanceProcessConnectReason(fixture.mqttAddresses[0], fixture.accessToken, "rb-rejected")
	if err != nil || reason != 0x88 {
		t.Fatalf("shedding CONNECT reason=%x err=%v", reason, err)
	}
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodDelete, baseURL+"/api/v1/cluster/rebalance", nil, clusterMembershipProcessAdmin, "rb-cancel-del")
	cancelled := clusterRebalanceProcessDecode(t, body)
	if err != nil || status != http.StatusOK || cancelled.State != "CANCELLED" {
		t.Fatalf("cancel status=%d state=%+v err=%v", status, cancelled, err)
	}
	readyCtx, cancelReady = context.WithTimeout(ctx, 10*time.Second)
	clusterMembershipProcessWaitForStatus(t, readyCtx, nodes[0], "/health/ready", http.StatusOK)
	cancelReady()
	probe, _, reason, err := clusterDrainProcessConnectV5(fixture.mqttAddresses[0], fixture.accessToken, "rb-probe", true)
	if err != nil || reason != 0 {
		t.Fatalf("cancelled CONNECT reason=%x err=%v", reason, err)
	}
	// Normal DISCONNECT closes the probe before the next measurement.
	clusterDurableProcessWriteFatal(t, probe, 0xE0, nil)
	clusterMembershipProcessAssertConnectionClosed(t, probe, 2*time.Second)
	_ = probe.Close()
	select {
	case event := <-redirects:
		t.Fatalf("cancel redirected %s kind=%d body=%x err=%v", event.client.id, event.kind, event.body, event.err)
	default:
	}

	request["loadBalancerPropagationDelayMillis"] = 500
	status, body, err = post("rb-main")
	initial := clusterRebalanceProcessDecode(t, body)
	if err != nil || status != http.StatusAccepted || initial.State != "RUNNING" {
		t.Fatalf("start status=%d body=%s err=%v", status, body, err)
	}
	status, body, err = post("rb-main")
	replay := clusterRebalanceProcessDecode(t, body)
	if err != nil || status != http.StatusOK || replay.OperationID != initial.OperationID {
		t.Fatalf("replay status=%d operation=%+v err=%v", status, replay, err)
	}
	request["maxDeviationConnections"] = 2
	status, body, err = post("rb-main")
	request["maxDeviationConnections"] = 1
	if err != nil || status != http.StatusConflict {
		t.Fatalf("key conflict status=%d body=%s err=%v", status, body, err)
	}
	clusterRebalanceProcessAssertError(t, body, "idempotency_conflict")
	status, body, err = clusterDrainProcessStart(ctx, baseURL, 2, clusterDrainProcessRequest{ConnectionRatePerSecond: 5, LoadBalancerPropagationDelayMillis: 500, TimeoutMillis: 120000}, "rb-drain-conflict")
	if err != nil || status != http.StatusConflict {
		t.Fatalf("drain conflict status=%d body=%s err=%v", status, body, err)
	}
	clusterRebalanceProcessAssertError(t, body, "connection_rebalance_in_progress")

	moved := make(map[string]bool)
	var packetID uint16 = 100
	handleRedirect := func(event clusterRebalanceProcessRedirect) {
		client := event.client
		if moved[client.id] {
			t.Fatalf("duplicate redirect for %s", client.id)
		}
		if client.node != 0 {
			t.Fatalf("underloaded node redirected %s", client.id)
		}
		if client.v5 {
			if event.err != nil || event.kind != 14 {
				t.Fatalf("%s redirect kind=%d body=%x err=%v", client.id, event.kind, event.body, event.err)
			}
			clusterDrainProcessAssertServerReference(t, event.body, "lb.rebalance.test:1883")
		} else {
			if event.err == nil || !clusterMembershipProcessIsClosed(event.err) {
				t.Fatalf("MQTT3 redirect %s kind=%d err=%v", client.id, event.kind, event.err)
			}
		}
		_ = client.conn.Close()
		counts[client.node]--
		moved[client.id] = true
		if client.v5 {
			packetID++
			clusterDurableProcessPublish(t, publisher, "rebalance/"+client.id, 1, packetID, "moved-"+client.id)
			clusterDurableProcessAck(t, publisher, 4, packetID)
		}
		target := -1
		for index, node := range nodes {
			status, _, err := clusterMembershipProcessRequest(ctx, http.MethodGet, node.managementURL()+"/health/ready", nil, clusterMembershipProcessAdmin, "")
			if err == nil && status == http.StatusOK && (target < 0 || counts[index] < counts[target]) {
				target = index
			}
		}
		if target < 0 {
			t.Fatal("no ready test LB target")
		}
		if client.v5 {
			resumed, present, reason, err := clusterDrainProcessConnectV5(fixture.mqttAddresses[target], fixture.accessToken, client.id, false)
			if err != nil || reason != 0 || !present {
				t.Fatalf("%s resume present=%v reason=%x err=%v", client.id, present, reason, err)
			}
			client.conn = resumed
			topic, payload, qos, id := clusterDrainProcessReadV5Publish(t, resumed)
			if topic != "rebalance/"+client.id || string(payload) != "moved-"+client.id || qos != 1 || id == 0 {
				t.Fatalf("%s resumed delivery topic=%s payload=%s qos=%d id=%d", client.id, topic, payload, qos, id)
			}
			clusterDurableProcessWriteFatal(t, resumed, 0x40, []byte{byte(id >> 8), byte(id)})
		} else {
			resumed, present := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[target], fixture.accessToken, client.id)
			if !present {
				t.Fatalf("%s MQTT3 session missing", client.id)
			}
			client.conn = resumed
		}
		client.node = target
		counts[target]++
	}
	var completed clusterRebalanceProcessOperation
	acceptanceCtx, cancelAcceptance := context.WithTimeout(ctx, 90*time.Second)
	defer cancelAcceptance()
	clusterMembershipProcessWait(t, acceptanceCtx, "connection rebalance COMPLETED", func() (bool, string, error) {
		for {
			select {
			case event := <-redirects:
				handleRedirect(event)
			default:
				goto readState
			}
		}
	readState:
		status, body, err := clusterMembershipProcessRequest(acceptanceCtx, http.MethodGet, baseURL+"/api/v1/cluster/rebalance", nil, clusterMembershipProcessAdmin, "")
		if err != nil || status != http.StatusOK {
			return false, fmt.Sprintf("status=%d body=%s err=%v", status, body, err), nil
		}
		var envelope struct {
			Data clusterRebalanceProcessOperation `json:"data"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			return false, "", err
		}
		completed = envelope.Data
		if completed.State == "FAILED" {
			return false, "", fmt.Errorf("rebalance failed: %s", body)
		}
		return completed.State == "COMPLETED", string(body), nil
	})
	// Redirects already emitted before completion must also finish their session checks.
	for {
		select {
		case event := <-redirects:
			handleRedirect(event)
		default:
			goto assertFinal
		}
	}
assertFinal:
	if completed.TotalConnections != 19 || completed.FairShare != 7 || completed.Ceiling != 8 {
		t.Fatalf("final totals=%+v, want total19/fair7/ceiling8", completed)
	}
	if len(moved) != 5 {
		t.Fatalf("redirected=%v, want five node1 clients", moved)
	}
	for _, node := range completed.Nodes {
		if node.Connections > completed.Ceiling || node.Shedding {
			t.Fatalf("not converged: %+v ceiling=%d", node, completed.Ceiling)
		}
		switch node.NodeID {
		case "1":
			if node.Connections != 7 || node.SessionsTransferred != uint64(len(moved)) {
				t.Fatalf("node1 report=%+v moved=%v", node, moved)
			}
		case "2":
			if node.Connections != 7 || node.SessionsTransferred != 0 {
				t.Fatalf("node2 changed: %+v", node)
			}
		case "3":
			if node.Connections < 1 {
				t.Fatal("node3 received no connections")
			}
		}
	}
	for _, count := range counts {
		if count > completed.Ceiling {
			t.Fatalf("test-tracked distribution=%v ceiling=%d", counts, completed.Ceiling)
		}
	}
	clusterMembershipProcessWaitForStatus(t, ctx, nodes[0], "/health/ready", http.StatusOK)
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodDelete, baseURL+"/api/v1/cluster/rebalance", nil, clusterMembershipProcessAdmin, "rb-completed-cancel")
	if err != nil || status != http.StatusConflict {
		t.Fatalf("completed cancel status=%d body=%s err=%v", status, body, err)
	}
	clusterDrainProcessAssertError(t, body, "rebalance_not_cancellable", "COMPLETED")
	t.Logf("rebalance completed: distribution=%v moved=%d operation=%s reports=%+v", counts, len(moved), completed.OperationID, completed.Nodes)
}

func clusterRebalanceProcessConnectReason(address, accessToken, clientID string) (byte, error) {
	connection, _, reason, err := clusterDrainProcessConnectV5(address, accessToken, clientID, true)
	if connection != nil {
		_ = connection.Close()
	}
	return reason, err
}
func clusterRebalanceProcessDecode(t *testing.T, body []byte) clusterRebalanceProcessOperation {
	t.Helper()
	var envelope struct {
		Data clusterRebalanceProcessOperation `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode rebalance: %v body=%s", err, body)
	}
	return envelope.Data
}
func clusterRebalanceProcessAssertError(t *testing.T, body []byte, code string) {
	t.Helper()
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Error.Code != code {
		t.Fatalf("error=%s err=%v want=%s", body, err, code)
	}
}
