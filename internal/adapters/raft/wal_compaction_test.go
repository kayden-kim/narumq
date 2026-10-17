package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func compactionTestState(count, size int) ([]*pb.Entry, [][]byte) {
	entries := make([]*pb.Entry, count)
	commands := make([][]byte, count)
	for i := range entries {
		data := []byte(fmt.Sprintf("command-%d", i+1))
		if size > 0 {
			data = bytes.Repeat([]byte{byte('a' + i%26)}, size)
		}
		commands[i] = data
		entries[i] = &pb.Entry{Index: uint64Ptr(uint64(i + 1)), Term: uint64Ptr(1), Type: pb.EntryNormal.Enum(), Data: data}
	}
	return entries, commands
}

func openCompactionTestStore(t *testing.T, directory string, entries []*pb.Entry) *stableStore {
	t.Helper()
	// The initial large WAL must also respect the existing per-frame limit.
	records := make([]walRecord, 0, len(entries))
	for _, entry := range entries {
		records = append(records, walRecord{entries: []*pb.Entry{entry}})
	}
	records = append(records, walRecord{hardState: &pb.HardState{Term: uint64Ptr(1), Commit: uint64Ptr(uint64(len(entries)))}})
	writeTestWAL(t, directory, records...)
	store, err := openStableStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func assertCompactionFiles(t *testing.T, directory string, index uint64) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(directory, "snapshot-*.bin"))
	if err != nil || len(files) != 1 || filepath.Base(files[0]) != snapshotFilename(index) {
		t.Fatalf("snapshot files = %v, error = %v, want only %s", files, err, snapshotFilename(index))
	}
	if _, err := os.Stat(filepath.Join(directory, "wal.bin.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("WAL temp stat = %v, want absent", err)
	}
}

func assertCompactionTail(t *testing.T, directory string, first, last uint64) []walRecord {
	t.Helper()
	records, err := readWAL(filepath.Join(directory, walName))
	if err != nil {
		t.Fatal(err)
	}
	next := first
	var commit uint64
	for _, record := range records {
		if record.snapshot != nil {
			t.Fatal("compacted WAL contains snapshot record")
		}
		if record.hardState != nil {
			commit = record.hardState.GetCommit()
		}
		for _, entry := range record.entries {
			if entry.GetIndex() != next {
				t.Fatalf("WAL entry index = %d, want %d", entry.GetIndex(), next)
			}
			next++
		}
	}
	if next != last+1 || commit != last {
		t.Fatalf("WAL tail end=%d commit=%d, want end=%d commit=%d", next, commit, last+1, last)
	}
	return records
}

func assertCompactionRecovery(t *testing.T, directory string, commands [][]byte) {
	t.Helper()
	store, err := openStableStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if !equalStringValues(commandStrings(store.commands), commandStrings(commands)) || store.appliedIndex != uint64(len(commands)) || store.hardState.GetCommit() != uint64(len(commands)) {
		t.Fatalf("recovered commands=%d applied=%d commit=%d, want exact %d commands", len(store.commands), store.appliedIndex, store.hardState.GetCommit(), len(commands))
	}
	if _, err := os.Stat(filepath.Join(directory, "wal.bin.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale WAL temp = %v", err)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-150
func TestStableStoreSnapshotCompactsWALToSnapshotTail(t *testing.T) {
	directory := t.TempDir()
	entries, commands := compactionTestState(10, 0)
	store := openCompactionTestStore(t, directory, entries)
	conf := &pb.ConfState{Voters: []uint64{1, 2, 3}}
	if err := store.createSnapshot(5, conf, commands[:5]); err != nil {
		t.Fatal(err)
	}
	assertCompactionTail(t, directory, 6, 10)
	assertCompactionFiles(t, directory, 5)
	if err := store.createSnapshot(8, conf, commands[:8]); err != nil {
		t.Fatal(err)
	}
	assertCompactionTail(t, directory, 9, 10)
	assertCompactionFiles(t, directory, 8)
	nextEntries, nextCommands := compactionTestState(11, 0)
	if _, err := store.writeWALFrame(walRecord{entries: nextEntries[10:], hardState: &pb.HardState{Term: uint64Ptr(1), Commit: uint64Ptr(11)}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := filePersistenceOps.sync(store.wal); err != nil {
		t.Fatal(err)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}
	assertCompactionRecovery(t, directory, nextCommands)
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-151
func TestStableStoreCompactionSplitsLargeTailIntoFrames(t *testing.T) {
	directory := t.TempDir()
	entries, commands := compactionTestState(40, 60*1024)
	store := openCompactionTestStore(t, directory, entries)
	if err := store.createSnapshot(1, &pb.ConfState{Voters: []uint64{1, 2, 3}}, commands[:1]); err != nil {
		t.Fatal(err)
	}
	records := assertCompactionTail(t, directory, 2, 40)
	if len(records) < 2 {
		t.Fatal("large tail was not split into frames")
	}
	data, err := os.ReadFile(filepath.Join(directory, walName))
	if err != nil {
		t.Fatal(err)
	}
	for offset := len(walMagic); offset < len(data); {
		length := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		if length > maxWALFramePayload {
			t.Fatalf("payload size=%d", length)
		}
		offset += 8 + length
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}
	assertCompactionRecovery(t, directory, commands)
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-152
func TestStableStoreRecoversEveryCompactionCrashState(t *testing.T) {
	for _, name := range []string{"stale temp", "snapshot before WAL replace", "replaced WAL before prune"} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			entries, commands := compactionTestState(10, 0)
			store := openCompactionTestStore(t, directory, entries)
			conf := &pb.ConfState{Voters: []uint64{1, 2, 3}}
			if name == "replaced WAL before prune" {
				if err := store.createSnapshot(5, conf, commands[:5]); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.close(); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "stale temp":
				if err := os.WriteFile(filepath.Join(directory, "wal.bin.tmp"), []byte("garbage"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "snapshot before WAL replace", "replaced WAL before prune":
				index := uint64(5)
				if name == "replaced WAL before prune" {
					index = 3
				}
				state, err := encodeApplicationState(commands[:index])
				if err != nil {
					t.Fatal(err)
				}
				if err := persistSnapshotFile(directory, &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: uint64Ptr(index), Term: uint64Ptr(1), ConfState: conf}, Data: state}); err != nil {
					t.Fatal(err)
				}
			}
			assertCompactionRecovery(t, directory, commands)
			if name == "replaced WAL before prune" {
				reopened, err := openStableStore(directory)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.close()
				if err := reopened.createSnapshot(8, conf, commands[:8]); err != nil {
					t.Fatal(err)
				}
				assertCompactionFiles(t, directory, 8)
			}
		})
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-153
func TestStableStoreCompactionFailureKeepsRecoverableState(t *testing.T) {
	for _, name := range []string{"temp sync", "rename", "directory sync"} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			entries, commands := compactionTestState(10, 0)
			store := openCompactionTestStore(t, directory, entries)
			original := filePersistenceOps
			defer func() { filePersistenceOps = original }()
			injected := errors.New("injected compaction failure")
			replaced := false
			filePersistenceOps.sync = func(file *os.File) error {
				if name == "temp sync" && filepath.Base(file.Name()) == "wal.bin.tmp" {
					return injected
				}
				return original.sync(file)
			}
			filePersistenceOps.rename = func(oldPath, newPath string) error {
				if filepath.Base(newPath) == walName {
					if name == "rename" {
						return injected
					}
					replaced = true
				}
				return original.rename(oldPath, newPath)
			}
			filePersistenceOps.syncDirectory = func(path string) error {
				if name == "directory sync" && replaced {
					return injected
				}
				return original.syncDirectory(path)
			}
			if err := store.createSnapshot(5, &pb.ConfState{Voters: []uint64{1, 2, 3}}, commands[:5]); !errors.Is(err, injected) {
				t.Fatalf("snapshot error=%v, want injected failure", err)
			}
			if err := store.close(); err != nil {
				t.Fatal(err)
			}
			filePersistenceOps = original
			assertCompactionRecovery(t, directory, commands)
		})
	}
}

func TestStableStoreSnapshotAtLastIndexCompactsWAL(t *testing.T) {
	directory := t.TempDir()
	entries, commands := compactionTestState(4, 0)
	store := openCompactionTestStore(t, directory, entries)
	if err := store.createSnapshot(uint64(len(entries)), &pb.ConfState{Voters: []uint64{1, 2, 3}}, commands); err != nil {
		t.Fatalf("createSnapshot at last index: %v", err)
	}

	records := assertCompactionTail(t, directory, uint64(len(entries))+1, uint64(len(entries)))
	if len(records) != 1 || records[0].hardState == nil || len(records[0].entries) != 0 {
		t.Fatalf("WAL records after snapshot at last index = %#v, want one hard-state-only record", records)
	}
	assertCompactionFiles(t, directory, uint64(len(entries)))
	assertCompactionRecovery(t, directory, commands)
}
