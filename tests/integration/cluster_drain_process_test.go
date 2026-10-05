package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"testing"
	"time"

	"narumq/internal/domain/mqtt"
)

// Traceability: DEP-06/DEP-12; docs/specs/drain-and-rolling-update.md §2.1, §2.2, §2.3, §3, §4, §6, §10.
func TestDynamicClusterNodeDrainProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	processes := clusterDrainProcessStartCluster(t, fixture)
	ctx := fixture.ctx

	v5Subscriber, present, reason, err := clusterDrainProcessConnectV5(
		fixture.mqttAddresses[2], fixture.accessToken, "drain-v5-a", false,
	)
	if err != nil || reason != 0 || present {
		t.Fatalf("initial MQTT 5 subscriber CONNECT present=%t reason=0x%02x err=%v", present, reason, err)
	}
	defer v5Subscriber.Close()
	clusterDrainProcessSubscribeV5(t, v5Subscriber, 1, "drain/a", 1)

	openSubscribers := make([]net.Conn, 3)
	for index := range openSubscribers {
		clientID := fmt.Sprintf("drain-open-%d", index+1)
		openSubscribers[index] = clusterMembershipProcessOpenSubscribedSession(
			t, fixture.mqttAddresses[2], fixture.accessToken, clientID, "drain/#",
		)
		defer openSubscribers[index].Close()
	}
	detached, err := clusterMembershipProcessTryOpenSubscribedSession(
		fixture.mqttAddresses[2], fixture.accessToken, "drain-detached-c", "drain/#",
	)
	if err != nil {
		t.Fatalf("open detached persistent session: %v", err)
	}
	if err := clusterDurableProcessWrite(detached, 0xE0, nil); err != nil {
		t.Fatalf("send normal MQTT 3.1.1 DISCONNECT: %v", err)
	}
	clusterMembershipProcessAssertConnectionClosed(t, detached, 2*time.Second)
	_ = detached.Close()
	clusterDrainProcessWaitForRouteProjection(t, ctx, processes, map[string]clusterDrainProcessSubscription{
		"drain-detached-c": {topicFilter: "drain/#", qos: 0},
		"drain-open-1":     {topicFilter: "drain/#", qos: 0},
		"drain-open-2":     {topicFilter: "drain/#", qos: 0},
		"drain-open-3":     {topicFilter: "drain/#", qos: 0},
		"drain-v5-a":       {topicFilter: "drain/a", qos: 1},
	})

	request := clusterDrainProcessRequest{
		ServerReference: fixture.mqttAddresses[0], ConnectionRatePerSecond: 2,
		LoadBalancerPropagationDelayMs: 1500, TimeoutMs: 60000,
	}
	leaderID, leaderURL := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	status, body, err := clusterDrainProcessStart(ctx, leaderURL, 3, request, "drain-3")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("start node 3 drain status=%d body=%s err=%v", status, body, err)
	}
	initial, err := clusterDrainProcessDecodeOperation(body)
	if err != nil || initial.State != "DRAINING" {
		t.Fatalf("initial drain operation=%+v err=%v body=%s", initial, err, body)
	}

	clusterMembershipProcessWaitForStatus(t, ctx, processes[2], "/health/ready", http.StatusServiceUnavailable)
	rejected, _, rejectReason, rejectErr := clusterDrainProcessConnectV5(
		fixture.mqttAddresses[2], fixture.accessToken, "drain-new-rejected", false,
	)
	if rejected != nil {
		_ = rejected.Close()
	}
	if rejectErr != nil || rejectReason != 0x88 {
		clusterMatrixLogBrokerDiagnostics(t, fixture, processes)
		t.Fatalf("new MQTT 5 CONNECT reason=0x%02x err=%v, want 0x88", rejectReason, rejectErr)
	}

	time.Sleep(time.Second)
	clusterDrainProcessPing(t, v5Subscriber)
	for _, subscriber := range openSubscribers {
		clusterDrainProcessPing(t, subscriber)
	}

	leaderURL = processes[leaderID-1].managementURL()
	status, body, err = clusterDrainProcessStart(ctx, leaderURL, 3, request, "drain-3")
	if err != nil || status != http.StatusOK {
		t.Fatalf("replay node 3 drain status=%d body=%s err=%v", status, body, err)
	}
	replayed, err := clusterDrainProcessDecodeOperation(body)
	if err != nil || replayed.OperationID != initial.OperationID {
		t.Fatalf("drain replay operation=%+v initial=%+v err=%v", replayed, initial, err)
	}

	type closeObservation struct {
		clientID string
		at       time.Time
		kind     byte
		body     []byte
		err      error
	}
	closed := make(chan closeObservation, 4)
	observe := func(clientID string, connection net.Conn) {
		go func() {
			_ = connection.SetReadDeadline(time.Now().Add(15 * time.Second))
			kind, _, packetBody, readErr := clusterDurableProcessReadRaw(connection)
			closed <- closeObservation{clientID: clientID, at: time.Now(), kind: kind, body: packetBody, err: readErr}
		}()
	}
	observe("drain-v5-a", v5Subscriber)
	for index, subscriber := range openSubscribers {
		observe(fmt.Sprintf("drain-open-%d", index+1), subscriber)
	}
	observations := make([]closeObservation, 0, 4)
	for range 4 {
		select {
		case observation := <-closed:
			observations = append(observations, observation)
		case <-ctx.Done():
			t.Fatalf("wait for drain socket closes: %v", ctx.Err())
		}
	}
	var disconnectBody []byte
	var closeTimes []time.Time
	for _, observation := range observations {
		closeTimes = append(closeTimes, observation.at)
		if observation.clientID == "drain-v5-a" {
			if observation.err != nil {
				t.Fatalf("read MQTT 5 drain DISCONNECT: %v", observation.err)
			}
			if observation.kind != 14 {
				t.Fatalf("MQTT 5 drain packet type=%d body=%x, want DISCONNECT", observation.kind, observation.body)
			}
			disconnectBody = observation.body
			continue
		}
		if observation.err == nil || !clusterMembershipProcessIsClosed(observation.err) {
			t.Fatalf("MQTT 3.1.1 %s did not close without a packet: kind=%d body=%x err=%v",
				observation.clientID, observation.kind, observation.body, observation.err)
		}
	}
	clusterDrainProcessAssertServerReference(t, disconnectBody, fixture.mqttAddresses[0])
	if span := drainProcessTimeSpan(closeTimes); span < time.Second {
		t.Fatalf("drain socket close spread=%s, want at least 1s", span)
	}
	if err := v5Subscriber.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set MQTT 5 close deadline: %v", err)
	}
	var trailing [1]byte
	if _, err := v5Subscriber.Read(trailing[:]); err == nil || !clusterMembershipProcessIsClosed(err) {
		t.Fatalf("MQTT 5 socket remained open after DISCONNECT: %v", err)
	}

	completed := clusterDrainProcessWaitForPhase(t, ctx, processes[0], 3, "COMPLETED")
	if completed.State != "SAFE_TO_STOP" || !completed.SafeToStop || completed.Connections.Remaining != 0 || completed.Sessions.TakenOver < 5 {
		t.Fatalf("completed drain state=%+v, want safe-to-stop, zero connections, and at least five takeovers", completed)
	}
	clusterMembershipProcessWaitForStatus(t, ctx, processes[2], "/health/drain", http.StatusOK)
	clusterMembershipProcessWaitForStatus(t, ctx, processes[2], "/health/ready", http.StatusServiceUnavailable)
	clusterDrainProcessAssertNodeState(t, ctx, processes[0], 3, "SAFE_TO_STOP")

	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodDelete,
		processes[0].managementURL()+"/api/v1/nodes/3/drain", nil, clusterMembershipProcessAdmin, "drain-cancel-after-safe")
	if err != nil || status != http.StatusConflict {
		t.Fatalf("cancel SAFE_TO_STOP drain status=%d body=%s err=%v", status, body, err)
	}
	clusterDrainProcessAssertError(t, body, "drain_not_cancellable", "SAFE_TO_STOP")

	v5Resumed, v5Present, v5Reason, err := clusterDrainProcessConnectV5(
		fixture.mqttAddresses[0], fixture.accessToken, "drain-v5-a", false,
	)
	if err != nil || v5Reason != 0 || !v5Present {
		t.Fatalf("MQTT 5 persistent session reconnect present=%t reason=0x%02x err=%v", v5Present, v5Reason, err)
	}
	defer v5Resumed.Close()
	cResumed, cPresent := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[0], fixture.accessToken, "drain-detached-c")
	defer cResumed.Close()
	if !cPresent {
		t.Fatal("detached MQTT 3.1.1 session reconnect returned SessionPresent=false")
	}
	clusterDrainProcessWaitForRouteProjection(t, ctx, processes, map[string]clusterDrainProcessSubscription{
		"drain-detached-c": {topicFilter: "drain/#", qos: 0},
		"drain-open-1":     {topicFilter: "drain/#", qos: 0},
		"drain-open-2":     {topicFilter: "drain/#", qos: 0},
		"drain-open-3":     {topicFilter: "drain/#", qos: 0},
		"drain-v5-a":       {topicFilter: "drain/a", qos: 1},
	})
	publisher, publisherPresent := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[0], fixture.accessToken, "drain-publisher")
	defer publisher.Close()
	if publisherPresent {
		t.Fatal("new drain publisher unexpectedly resumed a persistent session")
	}
	clusterDurableProcessPublish(t, publisher, "drain/a", 1, 77, "after-drain")
	clusterDurableProcessAck(t, publisher, 4, 77)
	topic, payload, qos, packetID := clusterDrainProcessReadV5Publish(t, v5Resumed)
	if topic != "drain/a" || string(payload) != "after-drain" || qos != 1 || packetID == 0 {
		t.Fatalf("post-drain MQTT 5 delivery topic=%q payload=%q qos=%d packetID=%d", topic, payload, qos, packetID)
	}
	clusterDurableProcessWriteFatal(t, v5Resumed, 0x40, []byte{byte(packetID >> 8), byte(packetID)})

	if err := processes[2].stop(10 * time.Second); err != nil {
		t.Fatalf("stop safe-to-stop node 3: %v", err)
	}
	restarted, err := fixture.harness.start("drain node 3 clean restart", fixture.bootstrapConfig(3), fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart node 3: %v", err)
	}
	processes[2] = restarted
	clusterMembershipProcessWaitForStatus(t, ctx, restarted, "/health/ready", http.StatusOK)
	clusterDrainProcessWaitForNotFound(t, ctx, restarted, 3)
	readyClient, readyPresent := clusterMembershipProcessReconnectSession(t,
		fixture.mqttAddresses[2], fixture.accessToken, "drain-after-restart")
	defer readyClient.Close()
	if readyPresent {
		t.Fatal("new MQTT client unexpectedly resumed a session after drain restart")
	}
}

// Traceability: DEP-06/DEP-12; docs/specs/drain-and-rolling-update.md §2.1, §2.3, §3, §6, §8, §10.
func TestDynamicClusterNodeDrainCancelTimeoutAndResumeProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	processes := clusterDrainProcessStartCluster(t, fixture)
	ctx := fixture.ctx
	leaderID, leaderURL := clusterMembershipProcessFindLeader(t, ctx, processes, 3)

	cancelRequest := clusterDrainProcessRequest{
		ConnectionRatePerSecond: 2, LoadBalancerPropagationDelayMs: 30000, TimeoutMs: 60000,
	}
	status, body, err := clusterDrainProcessStart(ctx, leaderURL, 2, cancelRequest, "drain-cancel-node-2")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("start cancellable drain status=%d body=%s err=%v", status, body, err)
	}
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodDelete,
		processes[leaderID-1].managementURL()+"/api/v1/nodes/2/drain", nil,
		clusterMembershipProcessAdmin, "drain-cancel-node-2-delete")
	if err != nil || status != http.StatusOK {
		t.Fatalf("cancel drain status=%d body=%s err=%v", status, body, err)
	}
	cancelled, err := clusterDrainProcessDecodeOperation(body)
	if err != nil || cancelled.Phase != "CANCELLED" || cancelled.State != "READY" {
		t.Fatalf("cancelled operation=%+v err=%v body=%s", cancelled, err, body)
	}
	clusterMembershipProcessWaitForStatus(t, ctx, processes[1], "/health/ready", http.StatusOK)
	cancelCheck, present := clusterMembershipProcessReconnectSession(t,
		fixture.mqttAddresses[1], fixture.accessToken, "drain-cancel-check")
	defer cancelCheck.Close()
	if present {
		t.Fatal("cancel check CONNECT unexpectedly resumed a session")
	}

	resumeRequest := clusterDrainProcessRequest{
		ConnectionRatePerSecond: 2, LoadBalancerPropagationDelayMs: 4000, TimeoutMs: 60000,
	}
	leaderID, leaderURL = clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	status, body, err = clusterDrainProcessStart(ctx, leaderURL, 3, resumeRequest, "drain-resume-node-3")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("start resumable drain status=%d body=%s err=%v", status, body, err)
	}
	started, err := clusterDrainProcessDecodeOperation(body)
	if err != nil {
		t.Fatalf("decode resumable drain: %v", err)
	}
	clusterDrainProcessWaitForPhase(t, ctx, processes[0], 3, "WAITING_FOR_LB")
	if err := processes[2].crash(5 * time.Second); err != nil {
		t.Fatalf("crash node 3 during LB wait: %v", err)
	}
	restarted, err := fixture.harness.start("drain node 3 crash recovery", fixture.bootstrapConfig(3), fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart node 3 after crash: %v", err)
	}
	processes[2] = restarted
	resumed := clusterDrainProcessWaitForPhase(t, ctx, processes[2], 3, "COMPLETED")
	if resumed.OperationID != started.OperationID || !resumed.SafeToStop || resumed.State != "SAFE_TO_STOP" {
		t.Fatalf("recovered operation=%+v, original=%+v", resumed, started)
	}

	if err := processes[2].stop(10 * time.Second); err != nil {
		t.Fatalf("stop completed node 3 before clearance: %v", err)
	}
	restarted, err = fixture.harness.start("drain node 3 clear recovery", fixture.bootstrapConfig(3), fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart node 3 for completed-record CLEAR: %v", err)
	}
	processes[2] = restarted
	clusterDrainProcessWaitForNotFound(t, ctx, restarted, 3)
	clusterMembershipProcessWaitForStatus(t, ctx, restarted, "/health/ready", http.StatusOK)

	if err := processes[0].stop(10 * time.Second); err != nil {
		t.Fatalf("stop node 1 to block safe-to-stop quorum: %v", err)
	}
	clusterMembershipProcessWaitForStatus(t, ctx, processes[1], "/health/ready", http.StatusOK)
	clusterMembershipProcessWaitForStatus(t, ctx, processes[2], "/health/ready", http.StatusOK)
	leaderID, leaderURL = clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	timeoutRequest := clusterDrainProcessRequest{
		ConnectionRatePerSecond: 2, LoadBalancerPropagationDelayMs: 0, TimeoutMs: 4000,
	}
	status, body, err = clusterDrainProcessStart(ctx, leaderURL, 3, timeoutRequest, "drain-timeout-node-3")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("start timeout drain without all successors status=%d body=%s err=%v", status, body, err)
	}
	failed := clusterDrainProcessWaitForPhase(t, ctx, processes[2], 3, "FAILED")
	if failed.Failure == nil || failed.Failure.Code != "drain_timeout" || failed.Failure.BlockingReason == nil ||
		*failed.Failure.BlockingReason != "insufficient_replicas" {
		t.Fatalf("timeout drain failure=%+v, want drain_timeout/insufficient_replicas", failed.Failure)
	}
	clusterMembershipProcessWaitForStatus(t, ctx, processes[2], "/health/drain", http.StatusServiceUnavailable)
	clusterMembershipProcessWaitForStatus(t, ctx, processes[2], "/health/ready", http.StatusServiceUnavailable)
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodDelete,
		processes[2].managementURL()+"/api/v1/nodes/3/drain", nil,
		clusterMembershipProcessAdmin, "drain-timeout-node-3-cancel")
	if err != nil || status != http.StatusOK {
		t.Fatalf("cancel timed-out drain status=%d body=%s err=%v", status, body, err)
	}
	cancelled, err = clusterDrainProcessDecodeOperation(body)
	if err != nil || cancelled.Phase != "CANCELLED" || cancelled.State != "READY" {
		t.Fatalf("timeout drain cancellation=%+v err=%v body=%s", cancelled, err, body)
	}

	node1, err := fixture.harness.start("drain node 1 quorum recovery", fixture.bootstrapConfig(1), fixture.managementAddresses[0])
	if err != nil {
		t.Fatalf("restart node 1: %v", err)
	}
	processes[0] = node1
	clusterMembershipProcessWaitForBootstrap(t, ctx, processes)
}

type clusterDrainProcessRequest struct {
	ServerReference                string `json:"serverReference"`
	ConnectionRatePerSecond        uint32 `json:"connectionRatePerSecond"`
	LoadBalancerPropagationDelayMs uint64 `json:"loadBalancerPropagationDelayMs"`
	TimeoutMs                      uint64 `json:"timeoutMs"`
}

type clusterDrainProcessOperation struct {
	OperationID string `json:"operationId"`
	NodeID      string `json:"nodeId"`
	State       string `json:"state"`
	Phase       string `json:"phase"`
	SafeToStop  bool   `json:"safeToStop"`
	Connections struct {
		Initial   uint64 `json:"initial"`
		Remaining uint64 `json:"remaining"`
	} `json:"connections"`
	Sessions struct {
		StateReadyForTakeover uint64 `json:"stateReadyForTakeover"`
		TakenOver             uint64 `json:"takenOver"`
		Failed                uint64 `json:"failed"`
	} `json:"sessions"`
	Failure *struct {
		Code           string  `json:"code"`
		BlockingReason *string `json:"blockingReason"`
	} `json:"failure"`
}

func clusterDrainProcessStart(ctx context.Context, baseURL string, nodeID uint64, request clusterDrainProcessRequest, idempotencyKey string) (int, []byte, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return 0, nil, err
	}
	return clusterMembershipProcessRequest(ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/nodes/%d/drain", baseURL, nodeID), body, clusterMembershipProcessAdmin, idempotencyKey)
}

func clusterDrainProcessDecodeOperation(body []byte) (clusterDrainProcessOperation, error) {
	var envelope struct {
		Data clusterDrainProcessOperation `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return clusterDrainProcessOperation{}, err
	}
	if envelope.Data.OperationID == "" {
		return clusterDrainProcessOperation{}, errors.New("drain operationId is empty")
	}
	return envelope.Data, nil
}

func clusterDrainProcessRead(ctx context.Context, process *clusterMembershipProcess, nodeID uint64) (clusterDrainProcessOperation, int, []byte, error) {
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/v1/nodes/%d/drain", process.managementURL(), nodeID), nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		return clusterDrainProcessOperation{}, status, body, err
	}
	operation, err := clusterDrainProcessDecodeOperation(body)
	return operation, status, body, err
}

func clusterDrainProcessWaitForPhase(t *testing.T, ctx context.Context, process *clusterMembershipProcess, nodeID uint64, phase string) clusterDrainProcessOperation {
	t.Helper()
	var operation clusterDrainProcessOperation
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("node %d drain phase %s", nodeID, phase), func() (bool, string, error) {
		if err := process.isAlive(); err != nil {
			return false, "", err
		}
		current, status, body, err := clusterDrainProcessRead(ctx, process, nodeID)
		if err != nil {
			return false, fmt.Sprintf("GET status=%d body=%s err=%v", status, body, err), nil
		}
		operation = current
		if current.Phase == phase {
			return true, "", nil
		}
		return false, "phase=" + current.Phase, nil
	})
	return operation
}

func clusterDrainProcessWaitForNotFound(t *testing.T, ctx context.Context, process *clusterMembershipProcess, nodeID uint64) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("node %d drain record clearance", nodeID), func() (bool, string, error) {
		if err := process.isAlive(); err != nil {
			return false, "", err
		}
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
			fmt.Sprintf("%s/api/v1/nodes/%d/drain", process.managementURL(), nodeID), nil, clusterMembershipProcessAdmin, "")
		if err != nil {
			return false, err.Error(), nil
		}
		if status == http.StatusNotFound {
			return true, "", nil
		}
		return false, fmt.Sprintf("status=%d body=%s", status, body), nil
	})
}

func clusterDrainProcessStartCluster(t *testing.T, fixture *clusterMembershipProcessFixture) []*clusterMembershipProcess {
	t.Helper()
	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		process, err := fixture.harness.start(
			fmt.Sprintf("drain bootstrap node %d", index+1), fixture.bootstrapConfig(uint64(index+1)), fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("start drain bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, processes)
	return processes
}

func clusterDrainProcessAssertNodeState(t *testing.T, ctx context.Context, process *clusterMembershipProcess, nodeID uint64, want string) {
	t.Helper()
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
		process.managementURL()+"/api/v1/nodes", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("list nodes status=%d body=%s err=%v", status, body, err)
	}
	var envelope struct {
		Data []struct {
			NodeID string `json:"nodeId"`
			State  string `json:"state"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode node list: %v", err)
	}
	for _, node := range envelope.Data {
		if node.NodeID == fmt.Sprint(nodeID) {
			if node.State != want {
				t.Fatalf("node %d state=%q, want %q; list=%s", nodeID, node.State, want, body)
			}
			return
		}
	}
	t.Fatalf("node %d missing from node list: %s", nodeID, body)
}

type clusterDrainProcessSubscription struct {
	topicFilter string
	qos         int
}

func clusterDrainProcessWaitForRouteProjection(
	t *testing.T,
	ctx context.Context,
	processes []*clusterMembershipProcess,
	want map[string]clusterDrainProcessSubscription,
) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, "drain route projection across all nodes", func() (bool, string, error) {
		var reference []string
		for _, process := range processes {
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
				process.managementURL()+"/api/v1/subscriptions?tenantId=acceptance", nil,
				clusterMembershipProcessAdmin, "")
			if err != nil {
				return false, fmt.Sprintf("%s query error: %v", process.name, err), nil
			}
			if status != http.StatusOK {
				return false, fmt.Sprintf("%s status=%d body=%s", process.name, status, body), nil
			}
			var list struct {
				Data []struct {
					TenantID    string `json:"tenantId"`
					ClientID    string `json:"clientId"`
					TopicFilter string `json:"topicFilter"`
					QoS         int    `json:"qos"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &list); err != nil {
				return false, fmt.Sprintf("%s decode error: %v", process.name, err), nil
			}
			if len(list.Data) != len(want) {
				return false, fmt.Sprintf("%s has %d subscriptions, want %d", process.name, len(list.Data), len(want)), nil
			}
			seen := make(map[string]bool, len(want))
			observed := make([]string, 0, len(want))
			for _, subscription := range list.Data {
				expected, ok := want[subscription.ClientID]
				if !ok || seen[subscription.ClientID] || subscription.TenantID != "acceptance" ||
					subscription.TopicFilter != expected.topicFilter || subscription.QoS != expected.qos {
					return false, fmt.Sprintf("%s subscription=%+v, want drain projection=%v", process.name, subscription, want), nil
				}
				seen[subscription.ClientID] = true
				observed = append(observed, fmt.Sprintf("%s|%s|%d", subscription.ClientID, subscription.TopicFilter, subscription.QoS))
			}
			if len(seen) != len(want) {
				return false, fmt.Sprintf("%s projection clients=%v, want=%v", process.name, seen, want), nil
			}
			sort.Strings(observed)
			if reference == nil {
				reference = observed
			} else if len(reference) != len(observed) {
				return false, fmt.Sprintf("projection length mismatch: reference=%v, %s=%v", reference, process.name, observed), nil
			} else {
				for index := range reference {
					if reference[index] != observed[index] {
						return false, fmt.Sprintf("projection mismatch: reference=%v, %s=%v", reference, process.name, observed), nil
					}
				}
			}
		}
		return true, "all nodes agree on QoS-aware drain route projection", nil
	})
}

func clusterDrainProcessAssertError(t *testing.T, body []byte, wantCode, wantState string) {
	t.Helper()
	var envelope struct {
		Error struct {
			Code    string                     `json:"code"`
			Details map[string]json.RawMessage `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode error response: %v body=%s", err, body)
	}
	if envelope.Error.Code != wantCode {
		t.Fatalf("error code=%q, want %q; body=%s", envelope.Error.Code, wantCode, body)
	}
	var state string
	if err := json.Unmarshal(envelope.Error.Details["state"], &state); err != nil || state != wantState {
		t.Fatalf("error details.state=%q err=%v, want %q; body=%s", state, err, wantState, body)
	}
}

func clusterDrainProcessConnectV5(address, accessToken, clientID string, cleanStart bool) (net.Conn, bool, byte, error) {
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return nil, false, 0, fmt.Errorf("dial %s: %w", address, err)
	}
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	flags := byte(0xC0)
	if cleanStart {
		flags |= 0x02
	}
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, flags, 0, 60, 5, 0x11, 0, 0, 0x0e, 0x10}
	for _, field := range []string{clientID, "jwt", accessToken} {
		body = append(body, byte(len(field)>>8), byte(len(field)))
		body = append(body, field...)
	}
	if _, err := connection.Write(clusterMembershipProcessEncodeMQTTPacket(0x10, body)); err != nil {
		_ = connection.Close()
		return nil, false, 0, fmt.Errorf("send MQTT 5 CONNECT: %w", err)
	}
	kind, _, response, err := clusterDurableProcessReadRaw(connection)
	if err != nil {
		_ = connection.Close()
		return nil, false, 0, fmt.Errorf("read MQTT 5 CONNACK: %w", err)
	}
	if kind != 2 {
		_ = connection.Close()
		return nil, false, 0, fmt.Errorf("invalid MQTT 5 CONNACK type=%d body=%x", kind, response)
	}
	decoder, err := mqtt.NewDecoder(1 << 20)
	if err != nil {
		_ = connection.Close()
		return nil, false, 0, err
	}
	packet, _, err := decoder.Decode(clusterMembershipProcessEncodeMQTTPacket(0x20, response), mqtt.ProtocolV5)
	if err != nil {
		_ = connection.Close()
		return nil, false, 0, fmt.Errorf("decode MQTT 5 CONNACK: %w", err)
	}
	connAck := packet.(*mqtt.ConnAck)
	_ = connection.SetDeadline(time.Time{})
	return connection, connAck.SessionPresent, connAck.ReasonCode, nil
}

func clusterDrainProcessSubscribeV5(t *testing.T, connection net.Conn, packetID uint16, filter string, qos byte) {
	t.Helper()
	body := []byte{byte(packetID >> 8), byte(packetID), 0, byte(len(filter) >> 8), byte(len(filter))}
	body = append(body, filter...)
	body = append(body, qos)
	if err := clusterDurableProcessWrite(connection, 0x82, body); err != nil {
		t.Fatalf("send MQTT 5 SUBSCRIBE: %v", err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	kind, _, ack, err := clusterDurableProcessReadRaw(connection)
	_ = connection.SetReadDeadline(time.Time{})
	if err != nil || kind != 9 || len(ack) != 4 || binary.BigEndian.Uint16(ack[:2]) != packetID || ack[2] != 0 || ack[3] != qos {
		t.Fatalf("MQTT 5 SUBACK kind=%d body=%x err=%v, want packet=%d QoS=%d", kind, ack, err, packetID, qos)
	}
}

func clusterDrainProcessPing(t *testing.T, connection net.Conn) {
	t.Helper()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	if err := clusterDurableProcessWrite(connection, 0xC0, nil); err != nil {
		t.Fatalf("send MQTT PINGREQ: %v", err)
	}
	kind, _, body, err := clusterDurableProcessReadRaw(connection)
	_ = connection.SetDeadline(time.Time{})
	if err != nil || kind != 13 || len(body) != 0 {
		t.Fatalf("PINGRESP kind=%d body=%x err=%v", kind, body, err)
	}
}

func clusterDrainProcessAssertServerReference(t *testing.T, body []byte, expected string) {
	t.Helper()
	if len(body) < 5 || body[0] != 0x9c {
		t.Fatalf("MQTT 5 DISCONNECT body=%x, want reason 0x9c", body)
	}
	propertyLength, offset, err := clusterDrainProcessReadVariableByteInteger(body, 1)
	if err != nil || propertyLength != len(body)-offset {
		t.Fatalf("DISCONNECT property length=%d offset=%d body=%x err=%v", propertyLength, offset, body, err)
	}
	if offset+3 > len(body) || body[offset] != 0x1c {
		t.Fatalf("DISCONNECT properties=%x, want Server Reference property 0x1c", body[offset:])
	}
	referenceLength := int(binary.BigEndian.Uint16(body[offset+1 : offset+3]))
	if offset+3+referenceLength != len(body) || string(body[offset+3:]) != expected {
		t.Fatalf("Server Reference=%q, want %q; body=%x", string(body[offset+3:]), expected, body)
	}
}

func clusterDrainProcessReadVariableByteInteger(data []byte, offset int) (int, int, error) {
	value, multiplier := 0, 1
	for range 4 {
		if offset >= len(data) {
			return 0, offset, io.ErrUnexpectedEOF
		}
		digit := data[offset]
		offset++
		value += int(digit&127) * multiplier
		if digit&128 == 0 {
			return value, offset, nil
		}
		multiplier *= 128
	}
	return 0, offset, errors.New("malformed MQTT variable byte integer")
}

func clusterDrainProcessReadV5Publish(t *testing.T, connection net.Conn) (string, []byte, byte, uint16) {
	t.Helper()
	delivery := clusterMatrixReadPublish(t, connection, true, 5*time.Second)
	return delivery.topic, []byte(delivery.payload), delivery.qos, delivery.packetID
}

func drainProcessTimeSpan(values []time.Time) time.Duration {
	if len(values) < 2 {
		return 0
	}
	sort.Slice(values, func(left, right int) bool { return values[left].Before(values[right]) })
	return values[len(values)-1].Sub(values[0])
}
