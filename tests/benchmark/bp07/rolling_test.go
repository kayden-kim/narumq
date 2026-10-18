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

// TestDrainCompletionRequiresSafeBarrierAndReportsFailure verifies drain completion requires the safe barrier and reports failure details.
// Scenarios: performance-and-verification.md:PRF-SCN-021
func TestDrainCompletionRequiresSafeBarrierAndReportsFailure(t *testing.T) {
	for _, tc := range []struct {
		name, body   string
		done, failed bool
	}{
		{"running", `{"data":{"state":"DRAINING","phase":"DISCONNECTING","safeToStop":false}}`, false, false},
		{"complete", `{"data":{"state":"SAFE_TO_STOP","phase":"COMPLETED","safeToStop":true}}`, true, false},
		{"failed", `{"data":{"phase":"FAILED","failure":{"code":"TRANSFER_FAILED","blockingReason":"peer_not_ready"}}}`, false, true},
		{"cancelled", `{"data":{"phase":"CANCELLED","failure":{"code":"OPERATION_CANCELLED"}}}`, false, true},
		{"incomplete_barrier", `{"data":{"state":"SAFE_TO_STOP","phase":"COMPLETED","safeToStop":false}}`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, done, err := drainCompletion([]byte(tc.body))
			if done != tc.done || (err != nil) != tc.failed {
				t.Fatalf("done=%v err=%v", done, err)
			}
			if tc.name == "failed" && (!strings.Contains(err.Error(), "TRANSFER_FAILED") || !strings.Contains(err.Error(), "peer_not_ready")) {
				t.Fatalf("failure details missing: %v", err)
			}
		})
	}
}

// TestEMQXCompletionRejectsDisabledAndUnknownStates verifies EMQX evacuation completion rejects disabled and unknown states.
// Scenarios: performance-and-verification.md:PRF-SCN-021
func TestEMQXCompletionRejectsDisabledAndUnknownStates(t *testing.T) {
	for _, tc := range []struct {
		state        string
		done, failed bool
	}{
		{"prohibiting\n", true, false}, {"waiting_health_check", false, false}, {"evicting_conns", false, false}, {"waiting_takeover", false, false}, {"evicting_sessions", false, false}, {"disabled", false, true}, {"{enabled, #{}}", false, true},
	} {
		t.Run(strings.TrimSpace(tc.state), func(t *testing.T) {
			done, err := emqxCompletion(tc.state)
			if done != tc.done || (err != nil) != tc.failed {
				t.Fatalf("done=%v err=%v", done, err)
			}
		})
	}
}

// TestWillOutcomesRequireAllReasonsAndMonotonicCounters verifies Will outcome counters require every reason and stay monotonic.
// Scenarios: performance-and-verification.md:PRF-SCN-020
func TestWillOutcomesRequireAllReasonsAndMonotonicCounters(t *testing.T) {
	before, err := parseWillOutcomes([]byte(rollingMetricFixture(4)))
	if err != nil {
		t.Fatal(err)
	}
	after, err := parseWillOutcomes([]byte(rollingMetricFixture(9)))
	if err != nil {
		t.Fatal(err)
	}
	delta, err := willOutcomeDelta(before, after)
	if err != nil {
		t.Fatal(err)
	}
	for _, reason := range willOutcomeReasons {
		if delta[reason] != 5 {
			t.Fatalf("%s delta=%d, want 5", reason, delta[reason])
		}
	}
	missing := strings.Split(strings.TrimSpace(rollingMetricFixture(1)), "\n")[:5]
	if _, err := parseWillOutcomes([]byte(strings.Join(missing, "\n"))); err == nil {
		t.Fatal("missing outcome accepted")
	}
	if _, err := willOutcomeDelta(after, before); err == nil {
		t.Fatal("counter reset during drain accepted")
	}
}

// TestRollingFailurePublishesCompletionAndAtomicMarker verifies a failed round publishes completion and an atomic marker.
// Scenarios: performance-and-verification.md:PRF-SCN-021
func TestRollingFailurePublishesCompletionAndAtomicMarker(t *testing.T) {
	profile, _, _, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	artifact := t.TempDir()
	if err := writeAtomicJSON(filepath.Join(artifact, "control", "workload-ready.json"), map[string]any{"writtenAt": time.Now().UTC().Format(time.RFC3339Nano), "connections": profile.clients()}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer management-token" {
			t.Error("missing management authorization")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case r.URL.Path == "/health/ready":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/metrics":
			fmt.Fprint(w, rollingMetricFixture(0))
		case r.URL.Path == "/api/v1/nodes/3/drain" && r.Method == http.MethodPost:
			if r.Header.Get("Idempotency-Key") != "bp07-test-run-round-01" {
				t.Errorf("unexpected idempotency key: %s", r.Header.Get("Idempotency-Key"))
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["serverReference"] != bp07LBReference {
				t.Errorf("unexpected drain reference: %v", body)
			}
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprint(w, `{"data":{"operationId":"op-1","state":"DRAINING","phase":"DISCONNECTING"}}`)
		case r.URL.Path == "/api/v1/nodes/3/drain" && r.Method == http.MethodGet:
			fmt.Fprint(w, `{"data":{"operationId":"op-1","phase":"FAILED","failure":{"code":"TRANSFER_FAILED","blockingReason":"peer_not_ready"}}}`)
		default:
			t.Errorf("unexpected management request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	nodes := make([]environment.ClusterNodeResult, 3)
	for i := range nodes {
		nodes[i] = environment.ClusterNodeResult{Name: fmt.Sprintf("node%d", i+1), PublishedPorts: map[string]string{"8080/tcp": strings.TrimPrefix(server.URL, "http://")}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = newRollingController("narumq", profile, "test-run", artifact, &bp07Fixture{ManagementToken: "management-token"}, environment.ClusterTargetOptions{})(ctx, nodes)
	if err == nil || !strings.Contains(err.Error(), "TRANSFER_FAILED") {
		t.Fatalf("controller error=%v", err)
	}
	var completion rollingCompletion
	if err := readControlJSON(filepath.Join(artifact, "control", "rolling-complete.json"), &completion); err != nil {
		t.Fatal(err)
	}
	if completion.Status != "failed" || !strings.Contains(completion.Reason, "peer_not_ready") {
		t.Fatalf("completion=%#v", completion)
	}
	var summary rollingSummary
	if err := readControlJSON(filepath.Join(artifact, "rolling-summary.json"), &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Valid || len(summary.Rounds) != 1 || summary.Rounds[0].Node != "node3" {
		t.Fatalf("summary=%#v", summary)
	}
	markerPath := filepath.Join(artifact, "control", "round-01.json")
	var marker roundMarker
	if err := readControlJSON(markerPath, &marker); err != nil {
		t.Fatal(err)
	}
	if marker.State != "draining" || marker.Node != "node3" {
		t.Fatalf("marker=%#v", marker)
	}
	marker.State = "restarting"
	if err := writeAtomicJSON(markerPath, marker); err != nil {
		t.Fatal(err)
	}
	if err := readControlJSON(markerPath, &marker); err != nil {
		t.Fatal(err)
	}
	if marker.State != "restarting" {
		t.Fatalf("replacement marker=%#v", marker)
	}
	if _, err := os.Stat(markerPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary marker remains: %v", err)
	}
}

// TestRollingScrapeSumsThreeNodesAndPreservesRawMetrics verifies Will counters are summed across three nodes with raw metrics preserved.
// Scenarios: performance-and-verification.md:PRF-SCN-020
func TestRollingScrapeSumsThreeNodesAndPreservesRawMetrics(t *testing.T) {
	profile, _, _, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	artifact := t.TempDir()
	var counter atomic.Int64
	counter.Store(2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, rollingMetricFixture(counter.Load())) }))
	defer server.Close()
	nodes := make([]environment.ClusterNodeResult, 3)
	for i := range nodes {
		nodes[i] = environment.ClusterNodeResult{Name: fmt.Sprintf("node%d", i+1), PublishedPorts: map[string]string{"8080/tcp": strings.TrimPrefix(server.URL, "http://")}}
	}
	driver, err := newNaruMQDriver(profile, "run", artifact, "token", nodes)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.scrape(context.Background(), "round-01-before"); err != nil {
		t.Fatal(err)
	}
	counter.Store(5)
	if err := driver.scrape(context.Background(), "round-01-after"); err != nil {
		t.Fatal(err)
	}
	delta, err := willOutcomeDelta(driver.snapshots["round-01-before"], driver.snapshots["round-01-after"])
	if err != nil {
		t.Fatal(err)
	}
	for _, reason := range willOutcomeReasons {
		if delta[reason] != 9 {
			t.Fatalf("summed delta[%s]=%d, want 9", reason, delta[reason])
		}
	}
	raw, err := os.ReadFile(filepath.Join(artifact, "metrics", "round-01-after-node3.prom"))
	if err != nil {
		t.Fatal(err)
	}
	values, err := parseWillOutcomes(raw)
	if err != nil {
		t.Fatal(err)
	}
	if values["suppressed_reconnected_within_delay"] != 5 {
		t.Fatalf("saved metrics=%v", values)
	}
}

// TestEMQXCLIRecognizesPinnedCallbackExitStatus verifies the EMQX CLI result handling for the pinned callback exit status.
// Scenarios: performance-and-verification.md:PRF-SCN-021
func TestEMQXCLIRecognizesPinnedCallbackExitStatus(t *testing.T) {
	for _, tc := range []struct {
		name, output string
		exitCode     int
		failed       bool
	}{
		{"zero_success", "Rebalance(evacuation) started", 0, false},
		{"callback_true", "Rebalance(evacuation) started\n", 1, false},
		{"rpc_failure_after_message", "Rebalance(evacuation) started\nRPC failed", 1, true},
		{"unrelated_nonzero", "Rebalance(evacuation) started", 2, true},
		{"reported_failure", "Rebalance start error: invalid nodes", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := emqxCLIResult(tc.output, tc.exitCode, "Rebalance(evacuation) started")
			if (err != nil) != tc.failed {
				t.Fatalf("exit=%d output=%q error=%v", tc.exitCode, tc.output, err)
			}
		})
	}
}

func rollingMetricFixture(value int64) string {
	var body strings.Builder
	for _, reason := range willOutcomeReasons {
		fmt.Fprintf(&body, "narumq_will_outcomes_total{reason=\"%s\"} %d\n", reason, value)
	}
	return body.String()
}
