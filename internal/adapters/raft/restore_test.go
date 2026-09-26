package raft

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestCompleteRestoreSeedsOpenableDirectory(t *testing.T) {
	directory := t.TempDir()
	if err := BeginRestore(directory); err != nil {
		t.Fatal(err)
	}
	applicationData := []byte("typed application snapshot")
	seed := RestoreSeedDTO{
		ClusterID: dynamicTestClusterID, NodeID: 1, CertificateSHA256: [32]byte{0x42},
		BootstrapVoterIDs: []NodeID{1, 2, 3}, SnapshotIndex: 7, SnapshotTerm: 3,
		ApplicationData: applicationData,
	}
	if err := CompleteRestore(directory, seed); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(directory, restoreMarkerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restore marker after completion error = %v, want not-exist", err)
	}

	node, err := Open(Config{
		NodeID: 1, Directory: directory, DynamicMembership: true,
		ClusterID: dynamicTestClusterID, CertificateSHA256: [32]byte{0x42}, BootstrapVoterIDs: []NodeID{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("open restored directory: %v", err)
	}
	defer node.Close()
	status := node.Status()
	if status.AppliedIndex != seed.SnapshotIndex || !slices.Equal(status.VoterIDs, seed.BootstrapVoterIDs) || len(status.LearnerIDs) != 0 {
		t.Fatalf("restored status = %+v", status)
	}
	if got := node.ApplicationSnapshot(); !bytes.Equal(got, applicationData) {
		t.Fatalf("restored application snapshot = %q, want %q", got, applicationData)
	}
}

func TestBeginRestoreRejectsForeignEntriesAndCleansIncompleteRestore(t *testing.T) {
	foreignDirectory := t.TempDir()
	if err := BeginRestore(foreignDirectory); err != nil {
		t.Fatal(err)
	}
	foreignPath := filepath.Join(foreignDirectory, "operator-data")
	if err := os.WriteFile(foreignPath, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := BeginRestore(foreignDirectory); !errors.Is(err, ErrRestoreTargetNotEmpty) {
		t.Fatalf("BeginRestore() with foreign entry error = %v", err)
	}
	if data, err := os.ReadFile(foreignPath); err != nil || string(data) != "keep" {
		t.Fatalf("foreign file after rejection = %q, error = %v", data, err)
	}

	metadataDirectory := t.TempDir()
	if err := writeDynamicClusterMeta(filepath.Join(metadataDirectory, clusterMetaName), dynamicTestClusterID, 1, [32]byte{0x42}, []NodeID{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := BeginRestore(metadataDirectory); !errors.Is(err, ErrRestoreTargetNotEmpty) {
		t.Fatalf("BeginRestore() with cluster metadata error = %v", err)
	}

	incompleteDirectory := t.TempDir()
	for name, data := range map[string][]byte{
		restoreMarkerName:            []byte("narumq restore incomplete\n"),
		walName:                      []byte("partial WAL"),
		clusterMetaName + ".tmp":     []byte("partial metadata"),
		snapshotFilename(8):          []byte("snapshot"),
		snapshotFilename(9) + ".tmp": []byte("temporary snapshot"),
	} {
		if err := os.WriteFile(filepath.Join(incompleteDirectory, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := BeginRestore(incompleteDirectory); err != nil {
		t.Fatalf("BeginRestore() cleanup: %v", err)
	}
	entries, err := os.ReadDir(incompleteDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != restoreMarkerName {
		t.Fatalf("directory after cleanup = %v, want only %s", entries, restoreMarkerName)
	}
}

func TestCompleteRestoreCrashBeforeMetaIsNotStartable(t *testing.T) {
	directory := t.TempDir()
	if err := BeginRestore(directory); err != nil {
		t.Fatal(err)
	}
	seed := RestoreSeedDTO{
		ClusterID: dynamicTestClusterID, NodeID: 1, CertificateSHA256: [32]byte{0x42},
		BootstrapVoterIDs: []NodeID{1, 2, 3}, SnapshotIndex: 7, SnapshotTerm: 3,
		ApplicationData: []byte("typed application snapshot"),
	}
	original := filePersistenceOps
	filePersistenceOps.rename = func(oldPath, newPath string) error {
		if filepath.Base(newPath) == clusterMetaName {
			return errors.New("injected metadata rename failure")
		}
		return original.rename(oldPath, newPath)
	}
	restoreErr := CompleteRestore(directory, seed)
	filePersistenceOps = original
	if restoreErr == nil {
		t.Fatal("CompleteRestore() accepted metadata rename failure")
	}
	if _, err := Open(Config{
		NodeID: 1, Directory: directory, DynamicMembership: true,
		ClusterID: dynamicTestClusterID, CertificateSHA256: [32]byte{0x42}, BootstrapVoterIDs: []NodeID{1, 2, 3},
	}); !errors.Is(err, ErrBootstrapIncomplete) {
		t.Fatalf("Open() after incomplete restore = %v, want ErrBootstrapIncomplete", err)
	}
	if err := BeginRestore(directory); err != nil {
		t.Fatalf("BeginRestore() cleanup after crash: %v", err)
	}
}

func TestOpenRejectsRestoreMarkerAfterMetadataPublication(t *testing.T) {
	directory := t.TempDir()
	if err := BeginRestore(directory); err != nil {
		t.Fatal(err)
	}
	seed := RestoreSeedDTO{
		ClusterID: dynamicTestClusterID, NodeID: 1, CertificateSHA256: [32]byte{0x42},
		BootstrapVoterIDs: []NodeID{1, 2, 3}, SnapshotIndex: 7, SnapshotTerm: 3,
		ApplicationData: []byte("typed application snapshot"),
	}
	if err := CompleteRestore(directory, seed); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, restoreMarkerName), []byte("narumq restore incomplete\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{
		NodeID: 1, Directory: directory, DynamicMembership: true,
		ClusterID: dynamicTestClusterID, CertificateSHA256: [32]byte{0x42}, BootstrapVoterIDs: []NodeID{1, 2, 3},
	}); !errors.Is(err, ErrBootstrapIncomplete) {
		t.Fatalf("Open() with restore marker and metadata = %v, want ErrBootstrapIncomplete", err)
	}
}
