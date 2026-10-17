//go:build windows

package raft

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-153
func TestStableStoreCompactionWindowsSharingLock(t *testing.T) {
	for _, transient := range []bool{true, false} {
		name := "persistent"
		if transient {
			name = "transient"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			entries, commands := compactionTestState(10, 0)
			store := openCompactionTestStore(t, directory, entries)
			// os.Open does not grant FILE_SHARE_DELETE. This is a real OS lock,
			// independent of the store's own WAL handle and worker synchronization.
			reader, err := os.Open(filepath.Join(directory, walName))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if transient {
				released := make(chan struct{})
				go func() {
					time.Sleep(100 * time.Millisecond)
					_ = reader.Close()
					close(released)
				}()
				defer func() { <-released }()
			}
			started := time.Now()
			err = store.createSnapshot(5, &pb.ConfState{Voters: []uint64{1, 2, 3}}, commands[:5])
			t.Logf("transient=%t elapsed=%s snapshot error=%v", transient, time.Since(started), err)
			if transient {
				if err != nil {
					t.Fatalf("compaction after temporary reader lock: %v", err)
				}
				assertCompactionTail(t, directory, 6, 10)
				assertCompactionFiles(t, directory, 5)
				// Appends must go to the replacement, not the now unlinked old file.
				nextEntries, nextCommands := compactionTestState(11, 0)
				if _, err := store.writeWALFrame(walRecord{entries: nextEntries[10:], hardState: &pb.HardState{Term: uint64Ptr(1), Commit: uint64Ptr(11)}}, nil); err != nil {
					t.Fatal(err)
				}
				commands = nextCommands
			} else {
				if !errors.Is(err, syscall.ERROR_ACCESS_DENIED) && !errors.Is(err, syscall.Errno(32)) {
					t.Fatalf("persistent reader lock error = %v, want Windows sharing failure", err)
				}
				if time.Since(started) > 2*time.Second {
					t.Fatal("persistent reader lock exceeded the bounded replacement wait")
				}
				assertCompactionTail(t, directory, 1, 10)
			}
			if err := reader.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				t.Fatal(err)
			}
			if err := store.close(); err != nil {
				t.Fatal(err)
			}
			assertCompactionRecovery(t, directory, commands)
		})
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-153
func TestRenameFileWindowsSourceSharingLock(t *testing.T) {
	directory := t.TempDir()
	source, destination := filepath.Join(directory, "new"), filepath.Join(directory, "old")
	if err := os.WriteFile(source, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	released := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = reader.Close()
		close(released)
	}()
	defer func() { <-released }()
	if err := renameFile(source, destination); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(destination)
	if err != nil || string(data) != "replacement" {
		t.Fatalf("destination = %q, %v; want replacement", data, err)
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source after replacement: %v, want absent", err)
	}
}
