package raft

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	pb "go.etcd.io/raft/v3/raftpb"
)

const restoreMarkerName = "restore-incomplete"

var ErrRestoreTargetNotEmpty = errors.New("raft restore target directory is not empty")

// RestoreSeedDTO contains the identity and committed snapshot needed to seed an empty Raft directory.
type RestoreSeedDTO struct {
	ClusterID         string
	NodeID            NodeID
	CertificateSHA256 [32]byte
	BootstrapVoterIDs []NodeID
	SnapshotIndex     uint64
	SnapshotTerm      uint64
	ApplicationData   []byte
}

// BeginRestore creates a durable marker or clears only an earlier marked restore attempt.
func BeginRestore(directory string) error {
	if directory == "" {
		return ErrConfigMismatch
	}
	info, err := os.Stat(directory)
	switch {
	case err == nil && !info.IsDir():
		return fmt.Errorf("%w: directory is not a directory", ErrConfigMismatch)
	case err == nil:
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return fmt.Errorf("create restore directory: %w", err)
		}
	case err != nil:
		return fmt.Errorf("inspect restore directory: %w", err)
	}

	metadataPath := filepath.Join(directory, clusterMetaName)
	if _, err := os.Lstat(metadataPath); err == nil {
		return ErrRestoreTargetNotEmpty
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect restore metadata: %w", err)
	}
	markerPath := filepath.Join(directory, restoreMarkerName)
	markerInfo, markerErr := os.Lstat(markerPath)
	markerExists := markerErr == nil
	if markerErr != nil && !errors.Is(markerErr, os.ErrNotExist) {
		return fmt.Errorf("inspect restore marker: %w", markerErr)
	}
	if markerExists && !markerInfo.Mode().IsRegular() {
		return ErrRestoreTargetNotEmpty
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("list restore directory: %w", err)
	}
	if !markerExists && len(entries) != 0 {
		return ErrRestoreTargetNotEmpty
	}
	if markerExists {
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !restoreOwnedEntry(entry.Name()) {
				return ErrRestoreTargetNotEmpty
			}
		}
		for _, entry := range entries {
			if err := os.Remove(filepath.Join(directory, entry.Name())); err != nil {
				return fmt.Errorf("remove incomplete restore entry %q: %w", entry.Name(), err)
			}
		}
		if err := filePersistenceOps.syncDirectory(directory); err != nil {
			return fmt.Errorf("sync cleared restore directory: %w", err)
		}
	}
	return createRestoreMarker(directory)
}

// CompleteRestore writes a finalized snapshot and WAL before publishing cluster metadata.
func CompleteRestore(directory string, seed RestoreSeedDTO) error {
	if directory == "" {
		return ErrConfigMismatch
	}
	markerPath := filepath.Join(directory, restoreMarkerName)
	markerInfo, err := os.Lstat(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: restore was not begun", ErrConfigMismatch)
	}
	if err != nil {
		return fmt.Errorf("inspect restore marker: %w", err)
	}
	if !markerInfo.Mode().IsRegular() {
		return ErrConfigMismatch
	}
	if !validClusterID(seed.ClusterID) || seed.NodeID == 0 || seed.CertificateSHA256 == ([32]byte{}) {
		return ErrConfigMismatch
	}
	voterIDs, err := normalizeBootstrapVoterIDs(seed.NodeID, seed.BootstrapVoterIDs)
	if err != nil || !slices.Equal(voterIDs, seed.BootstrapVoterIDs) || len(voterIDs) != 3 {
		return ErrConfigMismatch
	}
	if seed.SnapshotIndex == 0 || seed.SnapshotTerm == 0 || len(seed.ApplicationData) == 0 || len(seed.ApplicationData) > maxSnapshotPayload {
		return ErrConfigMismatch
	}
	if _, err := os.Lstat(filepath.Join(directory, clusterMetaName)); err == nil {
		return ErrRestoreTargetNotEmpty
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect restore metadata: %w", err)
	}

	voters := make([]uint64, len(voterIDs))
	for index, voterID := range voterIDs {
		voters[index] = uint64(voterID)
	}
	index := seed.SnapshotIndex
	term := seed.SnapshotTerm
	snapshot := &pb.Snapshot{
		Data: bytes.Clone(seed.ApplicationData),
		Metadata: &pb.SnapshotMetadata{
			Index: &index,
			Term:  &term,
			ConfState: &pb.ConfState{
				Voters: voters,
			},
		},
	}
	if err := persistSnapshotFileMode(directory, snapshot, true, nil); err != nil {
		return err
	}
	if err := createRestoreWAL(directory, term, index); err != nil {
		return err
	}
	if err := writeDynamicClusterMeta(filepath.Join(directory, clusterMetaName), seed.ClusterID, seed.NodeID, seed.CertificateSHA256, voterIDs); err != nil {
		return err
	}
	if err := os.Remove(markerPath); err != nil {
		return fmt.Errorf("remove restore marker: %w", err)
	}
	if err := filePersistenceOps.syncDirectory(directory); err != nil {
		return fmt.Errorf("sync completed restore directory: %w", err)
	}
	return nil
}

func createRestoreMarker(directory string) error {
	path := filepath.Join(directory, restoreMarkerName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create restore marker: %w", err)
	}
	writeErr := writeAll(file, []byte("narumq restore incomplete\n"))
	if writeErr == nil {
		writeErr = filePersistenceOps.sync(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("persist restore marker: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close restore marker: %w", closeErr)
	}
	if err := filePersistenceOps.syncDirectory(directory); err != nil {
		return fmt.Errorf("sync restore marker directory: %w", err)
	}
	return nil
}

func createRestoreWAL(directory string, term, commitIndex uint64) error {
	path := filepath.Join(directory, walName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create restore WAL: %w", err)
	}
	payload, encodeErr := encodeWALRecordMode(walRecord{hardState: &pb.HardState{Term: &term, Commit: &commitIndex}}, true)
	if encodeErr == nil {
		header := walFrameHeader(payload)
		if writeErr := writeAll(file, []byte(walMagic)); writeErr != nil {
			encodeErr = writeErr
		} else if writeErr := writeAll(file, header[:]); writeErr != nil {
			encodeErr = writeErr
		} else if writeErr := writeAll(file, payload); writeErr != nil {
			encodeErr = writeErr
		}
	}
	if encodeErr == nil {
		encodeErr = filePersistenceOps.sync(file)
	}
	closeErr := file.Close()
	if encodeErr != nil {
		return fmt.Errorf("persist restore WAL: %w", encodeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close restore WAL: %w", closeErr)
	}
	if err := filePersistenceOps.syncDirectory(directory); err != nil {
		return fmt.Errorf("sync restore WAL directory: %w", err)
	}
	return nil
}

func restoreOwnedEntry(name string) bool {
	switch name {
	case restoreMarkerName, walName, clusterMetaName + ".tmp":
		return true
	}
	if _, ok := parseSnapshotFilename(name); ok {
		return true
	}
	if strings.HasSuffix(name, ".tmp") {
		_, ok := parseSnapshotFilename(strings.TrimSuffix(name, ".tmp"))
		return ok
	}
	return false
}
