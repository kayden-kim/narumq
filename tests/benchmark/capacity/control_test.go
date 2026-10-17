package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestMarkerPathNamesFollowTheContract checks the control marker file names.
// Scenarios: performance-and-verification.md:PRF-SCN-053
func TestMarkerPathNamesFollowTheContract(t *testing.T) {
	if got := markerPath("control", 3, 2, "kill-ack"); got != filepath.Join("control", "step-03-r2-kill-ack.json") {
		t.Fatalf("marker path = %q", got)
	}
}

// TestKillPhaseReportsEachStateOnceAndKeepsLastMarker checks marker state order.
// Scenarios: performance-and-verification.md:PRF-SCN-053
func TestKillPhaseReportsEachStateOnceAndKeepsLastMarker(t *testing.T) {
	dir := t.TempDir()
	phase := newKillPhase(dir, 1, 1)
	if state, first := phase.poll(); state != "" || first {
		t.Fatalf("poll before any marker = %q, %t", state, first)
	}
	write := func(state, failure string) {
		t.Helper()
		marker := killMarker{State: state, Target: killTarget{NodeID: "2", Container: "broker-2"}, At: formatTime(time.Now()), Error: failure}
		if err := writeAtomicJSON(markerPath(dir, 1, 1, "kill"), marker); err != nil {
			t.Fatal(err)
		}
	}
	steps := []struct {
		state string
		first bool
	}{{killStateKilling, true}, {killStateKilling, false}, {killStateDown, true}, {killStateDown, false}, {killStateRestored, true}}
	for _, step := range steps {
		write(step.state, "")
		if state, first := phase.poll(); state != step.state || first != step.first {
			t.Fatalf("poll = (%q,%t), want (%q,%t)", state, first, step.state, step.first)
		}
	}
	write(killStateFailed, "docker unavailable")
	if state, first := phase.poll(); state != killStateFailed || !first || phase.lastMarker.Error != "docker unavailable" || phase.lastMarker.Target.NodeID != "2" {
		t.Fatalf("failed marker = (%q,%t) %+v", state, first, phase.lastMarker)
	}
}

// TestKillPhaseAckWritesKillingAcknowledgement writes the runner's killing acknowledgement.
// Scenarios: performance-and-verification.md:PRF-SCN-053
func TestKillPhaseAckWritesKillingAcknowledgement(t *testing.T) {
	dir := t.TempDir()
	phase := newKillPhase(dir, 2, 3)
	observed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := phase.ack(observed); err != nil {
		t.Fatal(err)
	}
	var ack killAckMarker
	if err := readControlJSON(markerPath(dir, 2, 3, "kill-ack"), &ack); err != nil {
		t.Fatal(err)
	}
	if ack.State != killStateKilling || ack.ObservedAt != "2026-01-02T03:04:05Z" {
		t.Fatalf("ack = %+v", ack)
	}
}

func TestWriteAtomicJSONLeavesNoTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	if err := writeAtomicJSON(filepath.Join(dir, "marker.json"), map[string]int{"a": 1}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "marker.json" {
		t.Fatalf("directory after write: %v, err %v", entries, err)
	}
}

// TestWaitForMarkerObservesLateFileAndTimesOut checks marker wait and timeout.
// Scenarios: performance-and-verification.md:PRF-SCN-053
func TestWaitForMarkerObservesLateFileAndTimesOut(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "late.json")
	go func() {
		time.Sleep(60 * time.Millisecond)
		_ = writeAtomicJSON(path, struct{}{})
	}()
	if !waitForMarker(context.Background(), 2*time.Second, func() bool { return fileExists(path) }) {
		t.Fatal("late marker was not observed")
	}
	if waitForMarker(context.Background(), 50*time.Millisecond, func() bool { return false }) {
		t.Fatal("wait for a missing marker did not time out")
	}
}
