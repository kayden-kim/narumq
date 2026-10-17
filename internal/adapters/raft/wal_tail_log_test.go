package raft

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Traceability: CLU-14; ADR-0023; docs/specs/cluster-session-and-delivery.md §11.37.
func TestOpenLogsTruncatedWALTail(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "node-1")
	node, err := Open(dynamicTestConfig(1, directory))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := processReadyForTest(t, node)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.CompleteApply(batch.ApplySequence); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, walName)
	intact, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if intact.Size() == 0 {
		t.Fatal("bootstrapped node left an empty WAL; the test needs a frame to extend")
	}

	logs := &lockedLogBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	open := func() {
		config := dynamicTestConfig(1, directory)
		config.Logger = logger
		reopened, err := Open(config)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}

	open()
	if strings.Contains(logs.String(), "raft wal torn tail truncated") {
		t.Fatalf("an intact WAL logged a truncation:\n%s", logs.String())
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0, 0, 0, 9, 1, 2, 3}); err != nil { // 7 bytes: shorter than a frame header
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	open()
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != intact.Size() {
		t.Fatalf("WAL size after recovery = %d, want the intact %d", after.Size(), intact.Size())
	}
	output := logs.String()
	for _, want := range []string{
		"raft wal torn tail truncated", "level=INFO", "path=", fmt.Sprintf("valid_bytes=%d", intact.Size()),
		"dropped_bytes=7", "reason=partial_frame_header",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("log lacks %q:\n%s", want, output)
		}
	}
}
