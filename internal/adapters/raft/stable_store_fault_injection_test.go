package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	netraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestClusterPersistenceErrorBranches(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(filePath, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareClusterDirectory(filepath.Join(filePath, "child"), 1, []NodeID{1, 2, 3}); err == nil {
		t.Fatal("prepareClusterDirectory accepted an unusable parent")
	}
	metadataDirectory := t.TempDir()
	if err := os.Mkdir(filepath.Join(metadataDirectory, clusterMetaName), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareClusterDirectory(metadataDirectory, 1, []NodeID{1, 2, 3}); err == nil {
		t.Fatal("prepareClusterDirectory accepted a metadata directory")
	}
	if hasFinalizedSnapshot(filepath.Join(t.TempDir(), "missing")) {
		t.Fatal("hasFinalizedSnapshot reported a missing directory")
	}

	metadataPath := filepath.Join(t.TempDir(), clusterMetaName)
	if err := writeClusterMeta(metadataPath, 1, []NodeID{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(data[len(clusterMetaMagic)+1+8:len(clusterMetaMagic)+1+8+4], 2)
	binary.BigEndian.PutUint32(data[len(data)-4:], crc32.Checksum(data[:len(data)-4], crcTable))
	if err := os.WriteFile(metadataPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateClusterMeta(metadataPath, 1, []NodeID{1, 2, 3}); !errors.Is(err, ErrConfigMismatch) {
		t.Fatalf("peer count mismatch error = %v", err)
	}

	original := filePersistenceOps
	defer func() { filePersistenceOps = original }()
	filePersistenceOps.write = func(io.Writer, []byte) (int, error) { return 0, errors.New("write failure") }
	if err := writeClusterMeta(filepath.Join(t.TempDir(), clusterMetaName), 1, []NodeID{1, 2, 3}); err == nil {
		t.Fatal("writeClusterMeta accepted write failure")
	}
	filePersistenceOps = original
	filePersistenceOps.sync = func(*os.File) error { return errors.New("sync failure") }
	if err := writeClusterMeta(filepath.Join(t.TempDir(), clusterMetaName), 1, []NodeID{1, 2, 3}); err == nil {
		t.Fatal("writeClusterMeta accepted sync failure")
	}
	filePersistenceOps = original
	filePersistenceOps.rename = func(string, string) error { return errors.New("rename failure") }
	if err := writeClusterMeta(filepath.Join(t.TempDir(), clusterMetaName), 1, []NodeID{1, 2, 3}); err == nil {
		t.Fatal("writeClusterMeta accepted rename failure")
	}
}

func TestSnapshotPersistenceAndLoaderErrorBranches(t *testing.T) {
	original := filePersistenceOps
	defer func() { filePersistenceOps = original }()
	for _, testCase := range []struct {
		name      string
		configure func()
	}{
		{name: "write", configure: func() {
			filePersistenceOps.write = func(io.Writer, []byte) (int, error) { return 0, errors.New("write failure") }
		}},
		{name: "sync", configure: func() { filePersistenceOps.sync = func(*os.File) error { return errors.New("sync failure") } }},
		{name: "rename", configure: func() { filePersistenceOps.rename = func(string, string) error { return errors.New("rename failure") } }},
		{name: "sync directory", configure: func() {
			filePersistenceOps.syncDirectory = func(string) error { return errors.New("directory sync failure") }
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			filePersistenceOps = original
			testCase.configure()
			if err := persistSnapshotFile(t.TempDir(), testSnapshot(4)); err == nil {
				t.Fatal("persistSnapshotFile accepted persistence failure")
			}
		})
	}
	filePersistenceOps = original
	directory := t.TempDir()
	tempPath := filepath.Join(directory, snapshotFilename(4)+".tmp")
	if err := os.Mkdir(tempPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := persistSnapshotFile(directory, testSnapshot(4)); err == nil {
		t.Fatal("persistSnapshotFile accepted a temporary directory")
	}

	invalidDirectory := t.TempDir()
	if err := os.Mkdir(filepath.Join(invalidDirectory, snapshotFilename(5)), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadLatestSnapshot(invalidDirectory); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("snapshot directory read error = %v", err)
	}
	invalidPayloadDirectory := t.TempDir()
	path := filepath.Join(invalidPayloadDirectory, snapshotFilename(6))
	payload := []byte{0xff}
	data := make([]byte, 16+len(payload))
	copy(data[:8], snapshotMagic)
	binary.BigEndian.PutUint32(data[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint32(data[12:16], crc32.Checksum(payload, crcTable))
	copy(data[16:], payload)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadLatestSnapshot(invalidPayloadDirectory); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("invalid snapshot protobuf error = %v", err)
	}
}

func TestWALDecodeBoundaryBranches(t *testing.T) {
	for _, payload := range [][]byte{
		{1, 1, 0, 0},
		{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{1, 2, 0, 0, 0xff, 0xff, 0xff, 0xff},
		{1, 2, 0, 0, 0, 0, 0, 1, 0xff},
		{1, 0, 0, 0, 0x00, 0x10, 0x00, 0x01},
		{1, 0, 0, 0, 0, 0, 0, 1, 0xff},
	} {
		if _, err := decodeWALRecord(payload); err == nil {
			t.Fatalf("decodeWALRecord(% X) unexpectedly succeeded", payload)
		}
	}
	commands := make([][]byte, maxStateCommands+1)
	for index := range commands {
		commands[index] = []byte("x")
	}
	if _, err := encodeApplicationState(commands); err == nil {
		t.Fatal("encodeApplicationState accepted too many commands")
	}
	if _, err := decodeApplicationState(append([]byte(stateMagic), []byte{0, 0, 0, 0xFF, 0}...)); err == nil {
		t.Fatal("decodeApplicationState accepted an oversized command count")
	}
	if _, err := decodeApplicationState(append(append([]byte(stateMagic), []byte{0, 0, 0, 1}...), 0, 0)); err == nil {
		t.Fatal("decodeApplicationState accepted a truncated command")
	}
	if _, err := decodeApplicationState(append(append([]byte(stateMagic), []byte{0, 0, 0, 1}...), 0, 0, 0, 2, 0xff)); err == nil {
		t.Fatal("decodeApplicationState accepted an invalid command length")
	}
	invalid := &pb.Entry{Type: pb.EntryConfChange.Enum(), Data: []byte{0xff}}
	if _, err := decodeConfChange(invalid); err == nil {
		t.Fatal("decodeConfChange accepted malformed v1 data")
	}
	invalid.Type = pb.EntryConfChangeV2.Enum()
	if _, err := decodeConfChange(invalid); err == nil {
		t.Fatal("decodeConfChange accepted malformed v2 data")
	}
	if cloneConfState(nil) == nil || len(cloneCommands(nil)) != 0 || len(cloneEntries(nil)) != 0 {
		t.Fatal("clone helpers returned invalid empty values")
	}
	if got := appendUnique([]uint64{1}, 1); len(got) != 1 {
		t.Fatalf("appendUnique duplicate = %#v", got)
	}
	if _, ok := parseSnapshotFilename("snapshot-0000000000000000000x.bin"); ok {
		t.Fatal("parseSnapshotFilename accepted a non-digit index")
	}
	if _, ok := parseSnapshotFilename("snapshot-99999999999999999999.bin"); ok {
		t.Fatal("parseSnapshotFilename accepted an overflowing index")
	}
}

func TestStableStoreAppendReadyErrorBranches(t *testing.T) {
	store, err := openStableStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	original := filePersistenceOps
	filePersistenceOps.write = func(io.Writer, []byte) (int, error) { return 0, errors.New("append write failure") }
	if err := store.appendReady(netraft.Ready{Entries: []*pb.Entry{{Index: uint64Ptr(1)}}}); err == nil {
		t.Fatal("appendReady accepted write failure")
	}
	filePersistenceOps = original
	filePersistenceOps.sync = func(*os.File) error { return errors.New("append sync failure") }
	if err := store.appendReady(netraft.Ready{Entries: []*pb.Entry{{Index: uint64Ptr(1)}}}); err == nil {
		t.Fatal("appendReady accepted sync failure")
	}
	filePersistenceOps = original
	invalidSnapshot := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: uint64Ptr(1)}}
	if err := store.appendReady(netraft.Ready{Snapshot: invalidSnapshot}); err == nil {
		t.Fatal("appendReady accepted invalid snapshot state")
	}
	filePersistenceOps = original
	if err := store.appendReady(netraft.Ready{Entries: []*pb.Entry{{Index: uint64Ptr(1), Data: bytes.Repeat([]byte{'x'}, maxWALFramePayload)}}}); err == nil {
		t.Fatal("appendReady accepted an oversized entry")
	}
	snapshotStore, err := openStableStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshotStore.appendReady(netraft.Ready{Snapshot: testSnapshot(1)}); err != nil {
		t.Fatal(err)
	}
	if err := snapshotStore.close(); err != nil {
		t.Fatal(err)
	}
	closeStore, err := openStableStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	filePersistenceOps.sync = func(*os.File) error { return errors.New("close sync failure") }
	if err := closeStore.close(); err == nil {
		t.Fatal("close accepted sync failure")
	}
	filePersistenceOps = original
	if err := store.createSnapshot(1, &pb.ConfState{}, [][]byte{make([]byte, maxCommandSize+1)}); err == nil {
		t.Fatal("createSnapshot accepted an oversized command")
	}
}

func TestOpenStableStoreSyncsDirectoryWhenOpeningWAL(t *testing.T) {
	original := filePersistenceOps
	defer func() { filePersistenceOps = original }()

	var syncedDirectories []string
	filePersistenceOps.syncDirectory = func(directory string) error {
		syncedDirectories = append(syncedDirectories, directory)
		return nil
	}

	directory := t.TempDir()
	store, err := openDynamicStableStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(syncedDirectories) != 1 || syncedDirectories[0] != directory {
		store.close()
		t.Fatalf("new WAL directory syncs = %v, want [%s]", syncedDirectories, directory)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}

	syncedDirectories = nil
	store, err = openDynamicStableStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(syncedDirectories) != 1 || syncedDirectories[0] != directory {
		store.close()
		t.Fatalf("reopened WAL directory syncs = %v, want [%s]", syncedDirectories, directory)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}

	retryDirectory := t.TempDir()
	syncAttempts := 0
	filePersistenceOps.syncDirectory = func(string) error {
		syncAttempts++
		if syncAttempts == 1 {
			return errors.New("directory sync failure")
		}
		return nil
	}
	if _, err := openDynamicStableStore(retryDirectory); err == nil || !strings.Contains(err.Error(), "sync WAL directory") {
		t.Fatalf("first open error = %v, want WAL directory sync failure", err)
	}
	store, err = openDynamicStableStore(retryDirectory)
	if err != nil {
		t.Fatalf("retry after directory sync failure: %v", err)
	}
	if syncAttempts != 2 {
		store.close()
		t.Fatalf("directory sync attempts after retry = %d, want 2", syncAttempts)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}
}
