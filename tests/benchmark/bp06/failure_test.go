package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"narumq/tests/benchmark/internal/environment"
)

func TestNaruMQResyncRetainsWatermarkAcrossLeaderChangeAndRetriesPages(t *testing.T) {
	profile, _, _, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	var node1Calls, node2Calls atomic.Int32
	var servers [3]*httptest.Server
	for i := range servers {
		id := i + 1
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health/ready" {
				w.WriteHeader(http.StatusOK)
				return
			}
			if r.URL.Path != "/api/v1/nodes" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if id == 1 {
				switch node1Calls.Add(1) {
				case 1:
					fmt.Fprint(w, `{"data":[],"page":{"hasMore":true}}`)
				case 2:
					fmt.Fprint(w, nodeList(1, 90, 7, "HEALTHY"))
				case 3:
					fmt.Fprint(w, nodeList(1, 100, 7, "HEALTHY"))
				default:
					fmt.Fprint(w, nodeList(2, 200, 100, "HEALTHY"))
				}
				return
			}
			if id == 2 {
				if node2Calls.Add(1) == 1 {
					fmt.Fprint(w, nodeList(2, 200, 100, "DEGRADED"))
					return
				}
				fmt.Fprint(w, nodeList(2, 200, 100, "HEALTHY"))
				return
			}
			fmt.Fprint(w, nodeList(2, 200, 100, "HEALTHY"))
		}))
		defer servers[i].Close()
	}
	nodes := make([]environment.ClusterNodeResult, 3)
	for i, server := range servers {
		nodes[i].Name = fmt.Sprintf("node%d", i+1)
		nodes[i].PublishedPorts = map[string]string{"8080/tcp": strings.TrimPrefix(server.URL, "http://")}
	}
	driver, err := newNaruMQDriver(profile, "secret", nodes)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	detail, err := driver.resync(ctx, roundPlan{Round: 1, Node: "node3", NodeID: 3})
	if err != nil {
		t.Fatal(err)
	}
	if detail.NaruMQ == nil || detail.NaruMQ.LeaderNode != 2 || detail.NaruMQ.WatermarkAppliedIndex != 100 || detail.NaruMQ.TargetMatchIndex != 100 {
		t.Fatalf("resync detail=%+v", detail)
	}
	if node1Calls.Load() < 4 || node2Calls.Load() == 0 {
		t.Fatalf("page/leader retries missing: node1=%d node2=%d", node1Calls.Load(), node2Calls.Load())
	}
}

func nodeList(leader int, leaderApplied, targetMatch uint64, targetHealth string) string {
	rows := make([]narumqNode, 0, 3)
	for id := 1; id <= 3; id++ {
		role, applied, health := "FOLLOWER", uint64(0), "HEALTHY"
		if id == leader {
			role, applied = "LEADER", leaderApplied
		}
		if id == 3 {
			applied, health = targetMatch, targetHealth
		}
		rows = append(rows, narumqNode{NodeID: fmt.Sprint(id), Role: role, LastAppliedIndex: applied, ReplicationHealth: health})
	}
	data, _ := json.Marshal(narumqNodePage{Data: rows})
	return string(data)
}

func TestNaruMQResyncAllowsRestartedLeaderWithoutPeerHealth(t *testing.T) {
	profile, _, _, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		rows := []narumqNode{
			{NodeID: "1", Role: "FOLLOWER", LastAppliedIndex: 7, ReplicationHealth: "HEALTHY"},
			{NodeID: "2", Role: "FOLLOWER", LastAppliedIndex: 7, ReplicationHealth: "HEALTHY"},
			{NodeID: "3", Role: "LEADER", LastAppliedIndex: 8, ReplicationHealth: "DEGRADED"},
		}
		_ = json.NewEncoder(w).Encode(narumqNodePage{Data: rows})
	}))
	defer server.Close()
	nodes := make([]environment.ClusterNodeResult, 3)
	for i := range nodes {
		nodes[i].PublishedPorts = map[string]string{"8080/tcp": strings.TrimPrefix(server.URL, "http://")}
	}
	driver, err := newNaruMQDriver(profile, "token", nodes)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	detail, err := driver.resync(ctx, roundPlan{Node: "node3", NodeID: 3})
	if err != nil {
		t.Fatal(err)
	}
	if detail.NaruMQ == nil || detail.NaruMQ.LeaderNode != 3 || detail.NaruMQ.WatermarkAppliedIndex != 8 || detail.NaruMQ.TargetMatchIndex != 8 {
		t.Fatalf("leader target resync detail=%+v", detail)
	}
}

func TestNaruMQResyncHonorsDeadline(t *testing.T) {
	profile, _, _, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		fmt.Fprint(w, `{"data":[],"page":{"hasMore":false}}`)
	}))
	defer server.Close()
	nodes := make([]environment.ClusterNodeResult, 3)
	for i := range nodes {
		nodes[i].PublishedPorts = map[string]string{"8080/tcp": strings.TrimPrefix(server.URL, "http://")}
	}
	driver, err := newNaruMQDriver(profile, "token", nodes)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := driver.resync(ctx, roundPlan{NodeID: 3}); err == nil {
		t.Fatal("resync succeeded without a leader before deadline")
	}
}

func TestMarkerAckWaitsForMatchingRoundAndState(t *testing.T) {
	root := t.TempDir()
	round := roundPlan{Round: 2, NodeID: 3}
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = writeAtomicJSON(filepath.Join(root, "control", "round-02-ack.json"), map[string]any{"round": 1, "state": "killing"})
		time.Sleep(10 * time.Millisecond)
		_ = writeAtomicJSON(filepath.Join(root, "control", "round-02-ack.json"), map[string]any{"round": 2, "state": "killing"})
	}()
	if err := waitMarkerAck(context.Background(), root, round, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestMarkerAckTimeoutAndFailureCompletion(t *testing.T) {
	root := t.TempDir()
	err := waitMarkerAck(context.Background(), root, roundPlan{Round: 3}, time.Now().Add(5*time.Millisecond))
	if err == nil || !strings.Contains(err.Error(), "did not acknowledge") {
		t.Fatalf("ack timeout=%v", err)
	}
	err = newFailureController("unsupported", Profile{}, "run", root, nil, environment.ClusterTargetOptions{})(context.Background(), make([]environment.ClusterNodeResult, 3))
	if err == nil {
		t.Fatal("unsupported target accepted")
	}
	var completion struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := readControlJSON(filepath.Join(root, "control", "failure-complete.json"), &completion); err != nil {
		t.Fatal(err)
	}
	if completion.Status != "failed" || !strings.Contains(completion.Reason, "unsupported failure target") {
		t.Fatalf("completion=%+v", completion)
	}
	var summary failureSummary
	if err := readControlJSON(filepath.Join(root, "failure-summary.json"), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Valid || len(summary.FailureReasons) == 0 {
		t.Fatalf("failure summary=%+v", summary)
	}
}

func TestFailureControllerKeepsIncompleteRoundDiagnostics(t *testing.T) {
	profile, _, _, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "control"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeAtomicJSON(filepath.Join(root, "control", "workload-ready.json"), map[string]any{"connections": profile.clients()}); err != nil {
		t.Fatal(err)
	}
	err = newFailureController("narumq", profile, "run", root, &bp06Fixture{ManagementToken: "token"}, environment.ClusterTargetOptions{})(context.Background(), nil)
	if err == nil {
		t.Fatal("missing nodes accepted")
	}
	var summary failureSummary
	if err := readControlJSON(filepath.Join(root, "failure-summary.json"), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Valid || len(summary.Rounds) != 0 {
		t.Fatalf("failure summary=%+v", summary)
	}
}

func TestFailureMarkerWritesExpectedControlJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control", "round-01.json")
	round := roundPlan{Round: 1, Node: "node2", NodeID: 2}
	if err := writeFailureMarker(path, round, "killing"); err != nil {
		t.Fatal(err)
	}
	var marker roundMarker
	if err := readControlJSON(path, &marker); err != nil {
		t.Fatal(err)
	}
	if marker.Round != 1 || marker.Node != "node2" || marker.State != "killing" || marker.WrittenAt == "" {
		t.Fatalf("marker=%+v", marker)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("atomic marker temp file remains: %v", err)
	}
}
