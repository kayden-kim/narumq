package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

// Control marker states written by the controller to step-NN-rR-kill.json.
const (
	killStateKilling   = "killing"
	killStateDown      = "down"
	killStateRestoring = "restoring"
	killStateRestored  = "restored"
	// killStateFailed lets a controller report that it could not perform the injection or
	// restore; the runner marks the step invalid.
	killStateFailed = "failed"

	markerPollInterval = 20 * time.Millisecond
)

type killTarget struct {
	NodeID    string `json:"nodeId"`
	Container string `json:"container,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Worker    string `json:"worker,omitempty"`
}

type readyMarker struct {
	Step         int    `json:"step"`
	Run          int    `json:"run"`
	Cells        int    `json:"cells"`
	Rate         int    `json:"rate"`
	Phase        string `json:"phase"`
	LeaderNodeID string `json:"leaderNodeId"`
	WrittenAt    string `json:"writtenAt"`
}

type killMarker struct {
	State  string     `json:"state"`
	Target killTarget `json:"target"`
	At     string     `json:"at"`
	Error  string     `json:"error,omitempty"`
}

type killAckMarker struct {
	State      string `json:"state"`
	ObservedAt string `json:"observedAt"`
}

type n1DoneMarker struct {
	Step      int    `json:"step"`
	Run       int    `json:"run"`
	Phase     string `json:"phase"`
	WrittenAt string `json:"writtenAt"`
}

type completeMarker struct {
	Step      int      `json:"step"`
	Run       int      `json:"run"`
	Verdict   string   `json:"verdict"`
	Reasons   []string `json:"reasons"`
	WrittenAt string   `json:"writtenAt"`
}

type runCompleteMarker struct {
	ProfileID string `json:"profileId"`
	Output    string `json:"output"`
	Adopted   bool   `json:"adopted"`
	Aborted   string `json:"aborted,omitempty"`
	WrittenAt string `json:"writtenAt"`
}

func markerPath(dir string, step, run int, name string) string {
	return filepath.Join(dir, fmt.Sprintf("step-%02d-r%d-%s.json", step, run, name))
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// writeAtomicJSON writes value through a temporary file and rename so readers never see a
// partial marker. Windows readers can briefly block replacement, so the rename is retried.
func writeAtomicJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp := path + ".tmp"
	if err := os.WriteFile(temp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Second)
	for {
		err := os.Rename(temp, path)
		if err == nil {
			return nil
		}
		sharingViolation := errors.Is(err, syscall.Errno(32))
		if runtime.GOOS != "windows" || (!errors.Is(err, os.ErrPermission) && !sharingViolation) || !time.Now().Before(deadline) {
			_ = os.Remove(temp)
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readControlJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// waitForMarker polls until read reports done, the deadline passes, or the context ends.
func waitForMarker(ctx context.Context, timeout time.Duration, done func() bool) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(markerPollInterval)
	defer ticker.Stop()
	for {
		if done() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return done()
		case <-ticker.C:
		}
	}
}

// killPhase is the runner's view of a step's controller exchange.
type killPhase struct {
	dir        string
	step, run  int
	observed   map[string]bool
	lastMarker killMarker
}

func newKillPhase(dir string, step, run int) *killPhase {
	return &killPhase{dir: dir, step: step, run: run, observed: make(map[string]bool)}
}

// poll reads the kill marker once and reports whether state is first observed now.
func (k *killPhase) poll() (state string, first bool) {
	var marker killMarker
	if readControlJSON(markerPath(k.dir, k.step, k.run, "kill"), &marker) != nil || marker.State == "" {
		return "", false
	}
	k.lastMarker = marker
	if k.observed[marker.State] {
		return marker.State, false
	}
	k.observed[marker.State] = true
	return marker.State, true
}

func (k *killPhase) ack(observedAt time.Time) error {
	return writeAtomicJSON(markerPath(k.dir, k.step, k.run, "kill-ack"), killAckMarker{State: killStateKilling, ObservedAt: formatTime(observedAt)})
}
