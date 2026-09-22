package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	clusterMetaName = "cluster.meta"
	walName         = "wal.bin"

	clusterMetaMagic = "NMQRAFT1"
	walMagic         = "NMQWAL01"
	snapshotMagic    = "NMQSNAP1"
	stateMagic       = "NMQSTATE2"

	formatVersion = 1

	maxWALFramePayload = 2 * 1024 * 1024
	maxSnapshotPayload = 64 * 1024 * 1024
	maxStateCommands   = 1 << 20
	maxCommandSize     = 64 * 1024
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type persistenceOps struct {
	write  func(io.Writer, []byte) (int, error)
	sync   func(*os.File) error
	rename func(string, string) error
}

var filePersistenceOps = persistenceOps{
	write:  func(w io.Writer, data []byte) (int, error) { return w.Write(data) },
	sync:   (*os.File).Sync,
	rename: os.Rename,
}

type walRecord struct {
	hardState *pb.HardState
	snapshot  *pb.Snapshot
	entries   []*pb.Entry
}

type stableStore struct {
	directory string
	wal       *os.File
	memory    *etcdraft.MemoryStorage

	snapshotIndex uint64
	hardState     *pb.HardState
	confState     *pb.ConfState
	commits       []CommitDTO
	appliedIndex  uint64
}

func prepareClusterDirectory(directory string, nodeID NodeID, peerIDs []NodeID) (bool, error) {
	if directory == "" {
		return false, fmt.Errorf("%w: directory is empty", ErrConfigMismatch)
	}

	info, err := os.Stat(directory)
	switch {
	case err == nil && !info.IsDir():
		return false, fmt.Errorf("%w: directory is not a directory", ErrConfigMismatch)
	case err != nil && !errors.Is(err, os.ErrNotExist):
		return false, fmt.Errorf("open cluster directory: %w", err)
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return false, fmt.Errorf("create cluster directory: %w", err)
		}
	}

	metaPath := filepath.Join(directory, clusterMetaName)
	_, metaErr := os.Stat(metaPath)
	if errors.Is(metaErr, os.ErrNotExist) {
		entries, readErr := os.ReadDir(directory)
		if readErr != nil {
			return false, fmt.Errorf("inspect cluster directory: %w", readErr)
		}
		if len(entries) != 0 {
			return false, ErrBootstrapIncomplete
		}
		if err := writeClusterMeta(metaPath, nodeID, peerIDs); err != nil {
			return false, err
		}
		return true, nil
	}
	if metaErr != nil {
		return false, fmt.Errorf("inspect cluster metadata: %w", metaErr)
	}

	if err := validateClusterMeta(metaPath, nodeID, peerIDs); err != nil {
		return false, err
	}
	if !hasFinalizedSnapshot(directory) {
		return false, ErrBootstrapIncomplete
	}
	return false, nil
}

func writeClusterMeta(path string, nodeID NodeID, peerIDs []NodeID) error {
	payload := make([]byte, 0, len(clusterMetaMagic)+1+8+4+8*len(peerIDs)+4)
	payload = append(payload, clusterMetaMagic...)
	payload = append(payload, formatVersion)
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(nodeID))
	payload = append(payload, number[:]...)
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(peerIDs)))
	payload = append(payload, count[:]...)
	for _, peerID := range peerIDs {
		binary.BigEndian.PutUint64(number[:], uint64(peerID))
		payload = append(payload, number[:]...)
	}
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.Checksum(payload, crcTable))
	payload = append(payload, checksum[:]...)

	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create cluster metadata: %w", err)
	}
	writeErr := writeAll(file, payload)
	if writeErr == nil {
		writeErr = filePersistenceOps.sync(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("persist cluster metadata: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close cluster metadata: %w", closeErr)
	}
	if err := filePersistenceOps.rename(tempPath, path); err != nil {
		return fmt.Errorf("finalize cluster metadata: %w", err)
	}
	return nil
}

func validateClusterMeta(path string, nodeID NodeID, peerIDs []NodeID) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read cluster metadata: %w", err)
	}
	minimum := len(clusterMetaMagic) + 1 + 8 + 4 + 4
	if len(data) < minimum || string(data[:len(clusterMetaMagic)]) != clusterMetaMagic {
		return fmt.Errorf("%w: invalid cluster metadata", ErrConfigMismatch)
	}
	if data[len(clusterMetaMagic)] != formatVersion {
		return fmt.Errorf("%w: unsupported cluster metadata version", ErrConfigMismatch)
	}
	storedChecksum := binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(data[:len(data)-4], crcTable) != storedChecksum {
		return fmt.Errorf("%w: cluster metadata checksum mismatch", ErrConfigMismatch)
	}
	position := len(clusterMetaMagic) + 1
	storedNode := binary.BigEndian.Uint64(data[position : position+8])
	position += 8
	peerCount := binary.BigEndian.Uint32(data[position : position+4])
	position += 4
	if peerCount != uint32(len(peerIDs)) || len(data) != position+int(peerCount)*8+4 {
		return fmt.Errorf("%w: cluster metadata peer set mismatch", ErrConfigMismatch)
	}
	storedPeers := make([]NodeID, peerCount)
	for index := range storedPeers {
		storedPeers[index] = NodeID(binary.BigEndian.Uint64(data[position : position+8]))
		position += 8
	}
	if storedNode != uint64(nodeID) || !slices.Equal(storedPeers, peerIDs) {
		return fmt.Errorf("%w: cluster metadata identity mismatch", ErrConfigMismatch)
	}
	return nil
}

func hasFinalizedSnapshot(directory string) bool {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if _, ok := parseSnapshotFilename(entry.Name()); ok {
			return true
		}
	}
	return false
}

func openStableStore(directory string) (*stableStore, error) {
	latestSnapshot, snapshotIndex, err := loadLatestSnapshot(directory)
	if err != nil {
		return nil, err
	}
	records, err := readWAL(filepath.Join(directory, walName))
	if err != nil {
		return nil, err
	}

	memory := etcdraft.NewMemoryStorage()
	store := &stableStore{
		directory:     directory,
		memory:        memory,
		hardState:     &pb.HardState{},
		confState:     &pb.ConfState{},
		snapshotIndex: snapshotIndex,
	}
	if latestSnapshot != nil {
		if err := memory.ApplySnapshot(proto.Clone(latestSnapshot).(*pb.Snapshot)); err != nil {
			return nil, fmt.Errorf("apply persisted snapshot: %w", err)
		}
		store.confState = cloneConfState(latestSnapshot.GetMetadata().GetConfState())
		store.commits, err = decodeApplicationState(snapshotIndex, latestSnapshot.GetData())
		if err != nil {
			return nil, fmt.Errorf("%w: snapshot state: %v", ErrCorruptSnapshot, err)
		}
		store.appliedIndex = snapshotIndex
	}

	for _, record := range records {
		if record.snapshot != nil && !etcdraft.IsEmptySnap(record.snapshot) {
			index := record.snapshot.GetMetadata().GetIndex()
			switch {
			case index < store.snapshotIndex:
			case index == store.snapshotIndex:
				if latestSnapshot != nil && !proto.Equal(latestSnapshot, record.snapshot) {
					return nil, fmt.Errorf("%w: WAL snapshot differs from finalized snapshot", ErrCorruptWAL)
				}
			case index > store.snapshotIndex:
				if err := memory.ApplySnapshot(proto.Clone(record.snapshot).(*pb.Snapshot)); err != nil {
					return nil, fmt.Errorf("%w: WAL snapshot: %v", ErrCorruptWAL, err)
				}
				store.snapshotIndex = index
				store.confState = cloneConfState(record.snapshot.GetMetadata().GetConfState())
				store.commits, err = decodeApplicationState(index, record.snapshot.GetData())
				if err != nil {
					return nil, fmt.Errorf("%w: WAL snapshot state: %v", ErrCorruptWAL, err)
				}
				store.appliedIndex = index
			}
		}
		if len(record.entries) != 0 {
			entries := trimEntriesAtSnapshot(record.entries, store.snapshotIndex)
			if len(entries) != 0 {
				if err := validateEntrySequence(entries); err != nil {
					return nil, fmt.Errorf("%w: %v", ErrCorruptWAL, err)
				}
				lastIndex, err := memory.LastIndex()
				if err != nil {
					return nil, fmt.Errorf("read WAL last index: %w", err)
				}
				firstIndex := entries[0].GetIndex()
				if firstIndex > lastIndex && firstIndex-lastIndex > 1 {
					return nil, fmt.Errorf("%w: WAL entry gap at index %d", ErrCorruptWAL, firstIndex)
				}
				if err := memory.Append(cloneEntries(entries)); err != nil {
					return nil, fmt.Errorf("%w: append WAL entries: %v", ErrCorruptWAL, err)
				}
			}
		}
		if record.hardState != nil {
			store.hardState = proto.Clone(record.hardState).(*pb.HardState)
		}
	}

	if err := memory.SetHardState(proto.Clone(store.hardState).(*pb.HardState)); err != nil {
		return nil, fmt.Errorf("set recovered hard state: %w", err)
	}
	lastIndex, err := memory.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("read recovered last index: %w", err)
	}
	if store.hardState.GetCommit() < store.snapshotIndex || store.hardState.GetCommit() > lastIndex {
		return nil, fmt.Errorf("%w: commit index %d is outside [%d,%d]", ErrCorruptWAL, store.hardState.GetCommit(), store.snapshotIndex, lastIndex)
	}
	if err := store.rebuildAppliedState(lastIndex); err != nil {
		return nil, err
	}

	walPath := filepath.Join(directory, walName)
	wal, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	if stat, statErr := wal.Stat(); statErr != nil {
		wal.Close()
		return nil, fmt.Errorf("stat WAL: %w", statErr)
	} else if stat.Size() == 0 {
		if err := writeAll(wal, []byte(walMagic)); err != nil {
			wal.Close()
			return nil, fmt.Errorf("initialize WAL: %w", err)
		}
		if err := filePersistenceOps.sync(wal); err != nil {
			wal.Close()
			return nil, fmt.Errorf("sync WAL header: %w", err)
		}
	}
	if _, err := wal.Seek(0, io.SeekEnd); err != nil {
		wal.Close()
		return nil, fmt.Errorf("seek WAL: %w", err)
	}
	store.wal = wal
	return store, nil
}

func (s *stableStore) rebuildAppliedState(lastIndex uint64) error {
	if s.hardState.GetCommit() == s.snapshotIndex {
		s.appliedIndex = s.snapshotIndex
		return nil
	}
	firstIndex, err := s.memory.FirstIndex()
	if err != nil {
		return fmt.Errorf("read recovered first index: %w", err)
	}
	if firstIndex > lastIndex {
		return fmt.Errorf("%w: no entries through commit index", ErrCorruptWAL)
	}
	entries, err := s.memory.Entries(firstIndex, lastIndex+1, math.MaxUint64)
	if err != nil {
		return fmt.Errorf("read recovered entries: %w", err)
	}
	confState := cloneConfState(s.confState)
	commits := cloneCommits(s.commits)
	for _, entry := range entries {
		if entry.GetIndex() <= s.snapshotIndex || entry.GetIndex() > s.hardState.GetCommit() {
			continue
		}
		switch entry.GetType() {
		case pb.EntryNormal:
			if len(entry.GetData()) != 0 {
				if len(entry.GetData()) > maxCommandSize {
					return fmt.Errorf("%w: command at index %d exceeds %d bytes", ErrCorruptWAL, entry.GetIndex(), maxCommandSize)
				}
				commits = append(commits, CommitDTO{
					Index: entry.GetIndex(),
					Data:  bytes.Clone(entry.GetData()),
				})
			}
		case pb.EntryConfChange, pb.EntryConfChangeV2:
			change, err := decodeConfChange(entry)
			if err != nil {
				return fmt.Errorf("%w: configuration entry %d: %v", ErrCorruptWAL, entry.GetIndex(), err)
			}
			confState, err = applyConfState(confState, change)
			if err != nil {
				return fmt.Errorf("%w: configuration entry %d: %v", ErrCorruptWAL, entry.GetIndex(), err)
			}
		default:
			return fmt.Errorf("%w: unknown entry type %d", ErrCorruptWAL, entry.GetType())
		}
		s.appliedIndex = entry.GetIndex()
	}
	s.confState = confState
	s.commits = commits
	if s.appliedIndex < s.snapshotIndex {
		s.appliedIndex = s.snapshotIndex
	}
	return nil
}

func (s *stableStore) appendReady(ready etcdraft.Ready) error {
	record, err := readyRecord(ready)
	if err != nil {
		return err
	}
	payload, err := encodeWALRecord(record)
	if err != nil {
		return err
	}
	if len(payload) > maxWALFramePayload {
		return fmt.Errorf("WAL frame exceeds %d bytes", maxWALFramePayload)
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:], crc32.Checksum(payload, crcTable))
	if err := writeAll(s.wal, header[:]); err != nil {
		return fmt.Errorf("append WAL frame header: %w", err)
	}
	if err := writeAll(s.wal, payload); err != nil {
		return fmt.Errorf("append WAL frame payload: %w", err)
	}
	if err := filePersistenceOps.sync(s.wal); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}

	if ready.Snapshot != nil && !etcdraft.IsEmptySnap(ready.Snapshot) {
		if err := persistSnapshotFile(s.directory, ready.Snapshot); err != nil {
			return err
		}
		if err := s.memory.ApplySnapshot(proto.Clone(ready.Snapshot).(*pb.Snapshot)); err != nil {
			return fmt.Errorf("apply ready snapshot: %w", err)
		}
		s.snapshotIndex = ready.Snapshot.GetMetadata().GetIndex()
		s.confState = cloneConfState(ready.Snapshot.GetMetadata().GetConfState())
		s.commits, err = decodeApplicationState(ready.Snapshot.GetMetadata().GetIndex(), ready.Snapshot.GetData())
		if err != nil {
			return fmt.Errorf("decode ready snapshot state: %w", err)
		}
		s.appliedIndex = s.snapshotIndex
	}
	if len(ready.Entries) != 0 {
		if err := s.memory.Append(cloneEntries(ready.Entries)); err != nil {
			return fmt.Errorf("append ready entries: %w", err)
		}
	}
	if ready.HardState != nil {
		s.hardState = proto.Clone(ready.HardState).(*pb.HardState)
		if err := s.memory.SetHardState(proto.Clone(ready.HardState).(*pb.HardState)); err != nil {
			return fmt.Errorf("set ready hard state: %w", err)
		}
	}
	return nil
}
func (s *stableStore) close() error {
	if s.wal == nil {
		return nil
	}
	if err := filePersistenceOps.sync(s.wal); err != nil {
		_ = s.wal.Close()
		return fmt.Errorf("close WAL: %w", err)
	}
	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}
	s.wal = nil
	return nil
}

func (s *stableStore) createSnapshot(index uint64, confState *pb.ConfState, commits []CommitDTO) error {
	if index == 0 {
		return nil
	}
	current, err := s.memory.Snapshot()
	if err != nil {
		return fmt.Errorf("read current snapshot: %w", err)
	}
	if index <= current.GetMetadata().GetIndex() {
		return nil
	}
	lastIndex, err := s.memory.LastIndex()
	if err != nil {
		return fmt.Errorf("read snapshot last index: %w", err)
	}
	if index > lastIndex {
		return fmt.Errorf("snapshot index %d exceeds last index %d", index, lastIndex)
	}
	state, err := encodeApplicationState(index, commits)
	if err != nil {
		return err
	}
	snapshot, err := s.memory.CreateSnapshot(index, cloneConfState(confState), state)
	if err != nil {
		return fmt.Errorf("create memory snapshot: %w", err)
	}
	if err := persistSnapshotFile(s.directory, snapshot); err != nil {
		return err
	}
	if err := s.memory.Compact(index); err != nil && !errors.Is(err, etcdraft.ErrCompacted) {
		return fmt.Errorf("compact memory storage: %w", err)
	}
	s.snapshotIndex = index
	s.confState = cloneConfState(confState)
	s.commits = cloneCommits(commits)
	s.appliedIndex = index
	return nil
}

func persistSnapshotFile(directory string, snapshot *pb.Snapshot) error {
	if snapshot == nil || etcdraft.IsEmptySnap(snapshot) {
		return nil
	}
	index := snapshot.GetMetadata().GetIndex()
	data, err := proto.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	if len(data) > maxSnapshotPayload {
		return fmt.Errorf("snapshot exceeds %d bytes", maxSnapshotPayload)
	}
	path := filepath.Join(directory, snapshotFilename(index))
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect snapshot file: %w", err)
	}
	var header [16]byte
	copy(header[:8], snapshotMagic)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(data)))
	binary.BigEndian.PutUint32(header[12:16], crc32.Checksum(data, crcTable))
	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create snapshot file: %w", err)
	}
	writeErr := writeAll(file, header[:])
	if writeErr == nil {
		writeErr = writeAll(file, data)
	}
	if writeErr == nil {
		writeErr = filePersistenceOps.sync(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("persist snapshot: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close snapshot: %w", closeErr)
	}
	if err := filePersistenceOps.rename(tempPath, path); err != nil {
		return fmt.Errorf("finalize snapshot: %w", err)
	}
	return nil
}

func loadLatestSnapshot(directory string) (*pb.Snapshot, uint64, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, 0, fmt.Errorf("inspect snapshots: %w", err)
	}
	var indexes []uint64
	for _, entry := range entries {
		if index, ok := parseSnapshotFilename(entry.Name()); ok {
			indexes = append(indexes, index)
		}
	}
	if len(indexes) == 0 {
		return nil, 0, nil
	}
	sort.Slice(indexes, func(left, right int) bool { return indexes[left] < indexes[right] })
	index := indexes[len(indexes)-1]
	path := filepath.Join(directory, snapshotFilename(index))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: read snapshot: %v", ErrCorruptSnapshot, err)
	}
	if len(data) < 16 || string(data[:8]) != snapshotMagic {
		return nil, 0, fmt.Errorf("%w: invalid snapshot header", ErrCorruptSnapshot)
	}
	length := binary.BigEndian.Uint32(data[8:12])
	if length > maxSnapshotPayload || int(length) != len(data)-16 {
		return nil, 0, fmt.Errorf("%w: invalid snapshot length", ErrCorruptSnapshot)
	}
	payload := data[16:]
	if crc32.Checksum(payload, crcTable) != binary.BigEndian.Uint32(data[12:16]) {
		return nil, 0, fmt.Errorf("%w: snapshot checksum mismatch", ErrCorruptSnapshot)
	}
	var snapshot pb.Snapshot
	if err := proto.Unmarshal(payload, &snapshot); err != nil {
		return nil, 0, fmt.Errorf("%w: decode snapshot: %v", ErrCorruptSnapshot, err)
	}
	if snapshot.GetMetadata().GetIndex() != index {
		return nil, 0, fmt.Errorf("%w: snapshot filename index mismatch", ErrCorruptSnapshot)
	}
	if _, err := decodeApplicationState(index, snapshot.GetData()); err != nil {
		return nil, 0, fmt.Errorf("%w: snapshot state: %v", ErrCorruptSnapshot, err)
	}
	return &snapshot, index, nil
}

func readWAL(path string) ([]walRecord, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat WAL: %w", err)
	}
	if stat.Size() == 0 {
		return nil, nil
	}
	if stat.Size() < int64(len(walMagic)) {
		return nil, fmt.Errorf("%w: truncated WAL magic", ErrCorruptWAL)
	}
	magic := make([]byte, len(walMagic))
	if _, err := io.ReadFull(file, magic); err != nil || string(magic) != walMagic {
		return nil, fmt.Errorf("%w: invalid WAL magic", ErrCorruptWAL)
	}
	fileSize := stat.Size()
	offset := int64(len(walMagic))
	validOffset := offset
	var records []walRecord
	for offset < fileSize {
		remaining := fileSize - offset
		if remaining < 8 {
			if err := truncateWAL(file, validOffset); err != nil {
				return nil, err
			}
			break
		}
		var header [8]byte
		if _, err := file.ReadAt(header[:], offset); err != nil {
			return nil, fmt.Errorf("%w: read WAL frame header: %v", ErrCorruptWAL, err)
		}
		length := binary.BigEndian.Uint32(header[:4])
		if length > maxWALFramePayload {
			return nil, fmt.Errorf("%w: WAL frame length %d exceeds %d", ErrCorruptWAL, length, maxWALFramePayload)
		}
		frameEnd := offset + 8 + int64(length)
		if frameEnd > fileSize {
			if err := truncateWAL(file, validOffset); err != nil {
				return nil, err
			}
			break
		}
		payload := make([]byte, length)
		if _, err := file.ReadAt(payload, offset+8); err != nil {
			return nil, fmt.Errorf("%w: read WAL frame: %v", ErrCorruptWAL, err)
		}
		checksum := binary.BigEndian.Uint32(header[4:])
		if crc32.Checksum(payload, crcTable) != checksum {
			if frameEnd == fileSize {
				if err := truncateWAL(file, validOffset); err != nil {
					return nil, err
				}
				break
			}
			return nil, fmt.Errorf("%w: middle WAL frame checksum mismatch", ErrCorruptWAL)
		}
		record, err := decodeWALRecord(payload)
		if err != nil {
			return nil, fmt.Errorf("%w: decode WAL frame: %v", ErrCorruptWAL, err)
		}
		records = append(records, record)
		validOffset = frameEnd
		offset = frameEnd
	}
	return records, nil
}

func truncateWAL(file *os.File, offset int64) error {
	if err := file.Truncate(offset); err != nil {
		return fmt.Errorf("%w: truncate torn WAL tail: %v", ErrCorruptWAL, err)
	}
	if err := filePersistenceOps.sync(file); err != nil {
		return fmt.Errorf("%w: sync truncated WAL: %v", ErrCorruptWAL, err)
	}
	return nil
}

func readyRecord(ready etcdraft.Ready) (walRecord, error) {
	record := walRecord{
		entries: cloneEntries(ready.Entries),
	}
	if ready.HardState != nil && !etcdraft.IsEmptyHardState(ready.HardState) {
		record.hardState = proto.Clone(ready.HardState).(*pb.HardState)
	}
	if ready.Snapshot != nil && !etcdraft.IsEmptySnap(ready.Snapshot) {
		record.snapshot = proto.Clone(ready.Snapshot).(*pb.Snapshot)
		if _, err := decodeApplicationState(ready.Snapshot.GetMetadata().GetIndex(), record.snapshot.GetData()); err != nil {
			return walRecord{}, fmt.Errorf("invalid ready snapshot state: %w", err)
		}
	}
	return record, nil
}

func encodeWALRecord(record walRecord) ([]byte, error) {
	var buffer bytes.Buffer
	buffer.WriteByte(formatVersion)
	flags := byte(0)
	if record.hardState != nil {
		flags |= 1
	}
	if record.snapshot != nil {
		flags |= 2
	}
	buffer.WriteByte(flags)
	buffer.Write([]byte{0, 0})
	var number [8]byte
	if record.hardState != nil {
		binary.BigEndian.PutUint64(number[:], record.hardState.GetTerm())
		buffer.Write(number[:])
		binary.BigEndian.PutUint64(number[:], record.hardState.GetVote())
		buffer.Write(number[:])
		binary.BigEndian.PutUint64(number[:], record.hardState.GetCommit())
		buffer.Write(number[:])
	}
	if record.snapshot != nil {
		data, err := proto.Marshal(record.snapshot)
		if err != nil {
			return nil, err
		}
		if len(data) > maxSnapshotPayload {
			return nil, fmt.Errorf("snapshot exceeds %d bytes", maxSnapshotPayload)
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(data)))
		buffer.Write(length[:])
		buffer.Write(data)
	}
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(record.entries)))
	buffer.Write(count[:])
	for _, entry := range record.entries {
		data, err := proto.Marshal(entry)
		if err != nil {
			return nil, err
		}
		if len(data) > maxWALFramePayload {
			return nil, fmt.Errorf("entry exceeds WAL frame bound")
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(data)))
		buffer.Write(length[:])
		buffer.Write(data)
		if buffer.Len() > maxWALFramePayload {
			return nil, fmt.Errorf("WAL frame exceeds %d bytes", maxWALFramePayload)
		}
	}
	return buffer.Bytes(), nil
}

func decodeWALRecord(payload []byte) (walRecord, error) {
	reader := newBoundedReader(payload)
	version, err := reader.byte()
	if err != nil || version != formatVersion {
		return walRecord{}, fmt.Errorf("unsupported payload version")
	}
	flags, err := reader.byte()
	if err != nil {
		return walRecord{}, err
	}
	if flags&^byte(3) != 0 {
		return walRecord{}, fmt.Errorf("unknown WAL payload flags")
	}
	if _, err := reader.bytes(2); err != nil {
		return walRecord{}, err
	}
	var record walRecord
	if flags&1 != 0 {
		term, err := reader.u64()
		if err != nil {
			return walRecord{}, err
		}
		vote, err := reader.u64()
		if err != nil {
			return walRecord{}, err
		}
		commit, err := reader.u64()
		if err != nil {
			return walRecord{}, err
		}
		record.hardState = &pb.HardState{Term: &term, Vote: &vote, Commit: &commit}
	}
	if flags&2 != 0 {
		length, err := reader.u32()
		if err != nil {
			return walRecord{}, err
		}
		if length > maxSnapshotPayload || uint64(length) > uint64(reader.remaining()) {
			return walRecord{}, fmt.Errorf("invalid snapshot length")
		}
		data, err := reader.bytes(int(length))
		if err != nil {
			return walRecord{}, err
		}
		var snapshot pb.Snapshot
		if err := proto.Unmarshal(data, &snapshot); err != nil {
			return walRecord{}, err
		}
		if snapshot.GetMetadata().GetIndex() == 0 {
			return walRecord{}, fmt.Errorf("empty snapshot")
		}
		if _, err := decodeApplicationState(snapshot.GetMetadata().GetIndex(), snapshot.GetData()); err != nil {
			return walRecord{}, err
		}
		record.snapshot = &snapshot
	}
	count, err := reader.u32()
	if err != nil {
		return walRecord{}, err
	}
	if count > maxStateCommands {
		return walRecord{}, fmt.Errorf("entry count exceeds bound")
	}
	record.entries = make([]*pb.Entry, 0, count)
	for index := uint32(0); index < count; index++ {
		length, err := reader.u32()
		if err != nil {
			return walRecord{}, err
		}
		if length > maxWALFramePayload || uint64(length) > uint64(reader.remaining()) {
			return walRecord{}, fmt.Errorf("invalid entry length")
		}
		data, err := reader.bytes(int(length))
		if err != nil {
			return walRecord{}, err
		}
		var entry pb.Entry
		if err := proto.Unmarshal(data, &entry); err != nil {
			return walRecord{}, err
		}
		record.entries = append(record.entries, &entry)
	}
	if reader.remaining() != 0 {
		return walRecord{}, fmt.Errorf("trailing WAL payload bytes")
	}
	if err := validateEntrySequence(record.entries); err != nil {
		return walRecord{}, err
	}
	return record, nil
}

func encodeApplicationState(snapshotIndex uint64, commits []CommitDTO) ([]byte, error) {
	if len(commits) > maxStateCommands {
		return nil, fmt.Errorf("application commit count exceeds bound")
	}
	var buffer bytes.Buffer
	buffer.WriteString(stateMagic)
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(commits)))
	buffer.Write(countBuf[:])

	var prevIndex uint64
	var header [12]byte
	for _, commit := range commits {
		if commit.Index == 0 || commit.Index <= prevIndex {
			return nil, fmt.Errorf("commit index %d not strictly increasing (previous %d)", commit.Index, prevIndex)
		}
		if commit.Index > snapshotIndex {
			return nil, fmt.Errorf("commit index %d exceeds snapshot index %d", commit.Index, snapshotIndex)
		}
		if len(commit.Data) == 0 {
			return nil, fmt.Errorf("commit at index %d has empty payload", commit.Index)
		}
		if len(commit.Data) > maxCommandSize {
			return nil, fmt.Errorf("application commit exceeds %d bytes", maxCommandSize)
		}
		prevIndex = commit.Index

		binary.BigEndian.PutUint64(header[:8], commit.Index)
		binary.BigEndian.PutUint32(header[8:], uint32(len(commit.Data)))
		buffer.Write(header[:])
		buffer.Write(commit.Data)

		if buffer.Len() > maxSnapshotPayload {
			return nil, fmt.Errorf("application state exceeds %d bytes", maxSnapshotPayload)
		}
	}
	return buffer.Bytes(), nil
}

func decodeApplicationState(snapshotIndex uint64, data []byte) ([]CommitDTO, error) {
	if len(data) < len(stateMagic)+4 || string(data[:len(stateMagic)]) != stateMagic {
		return nil, fmt.Errorf("invalid application state magic")
	}
	position := len(stateMagic)
	count := binary.BigEndian.Uint32(data[position : position+4])
	position += 4
	if count > maxStateCommands {
		return nil, fmt.Errorf("application commit count exceeds bound")
	}

	commits := make([]CommitDTO, 0, count)
	var prevIndex uint64
	for index := uint32(0); index < count; index++ {
		if len(data)-position < 12 {
			return nil, fmt.Errorf("truncated application commit header")
		}
		commitIndex := binary.BigEndian.Uint64(data[position : position+8])
		length := binary.BigEndian.Uint32(data[position+8 : position+12])
		position += 12

		if commitIndex == 0 || commitIndex <= prevIndex {
			return nil, fmt.Errorf("commit index %d not strictly increasing (previous %d)", commitIndex, prevIndex)
		}
		if commitIndex > snapshotIndex {
			return nil, fmt.Errorf("commit index %d exceeds snapshot index %d", commitIndex, snapshotIndex)
		}
		if length == 0 {
			return nil, fmt.Errorf("commit at index %d has empty payload", commitIndex)
		}
		if length > maxCommandSize || uint64(length) > uint64(len(data)-position) {
			return nil, fmt.Errorf("invalid application commit length")
		}
		prevIndex = commitIndex

		commits = append(commits, CommitDTO{
			Index: commitIndex,
			Data:  bytes.Clone(data[position : position+int(length)]),
		})
		position += int(length)
	}
	if position != len(data) {
		return nil, fmt.Errorf("trailing application state bytes")
	}
	return commits, nil
}

func decodeConfChange(entry *pb.Entry) (*pb.ConfChangeV2, error) {
	switch entry.GetType() {
	case pb.EntryConfChange:
		var change pb.ConfChange
		if err := proto.Unmarshal(entry.GetData(), &change); err != nil {
			return nil, err
		}
		return change.AsV2(), nil
	case pb.EntryConfChangeV2:
		var change pb.ConfChangeV2
		if err := proto.Unmarshal(entry.GetData(), &change); err != nil {
			return nil, err
		}
		return &change, nil
	default:
		return nil, fmt.Errorf("entry is not a configuration change")
	}
}

func applyConfState(current *pb.ConfState, change *pb.ConfChangeV2) (*pb.ConfState, error) {
	next := cloneConfState(current)
	for _, single := range change.GetChanges() {
		if single.GetNodeId() == 0 {
			return nil, fmt.Errorf("configuration change contains node id zero")
		}
		switch single.GetType() {
		case pb.ConfChangeAddNode:
			next.Learners = removeID(next.Learners, single.GetNodeId())
			next.Voters = appendUnique(next.Voters, single.GetNodeId())
		case pb.ConfChangeAddLearnerNode:
			next.Voters = removeID(next.Voters, single.GetNodeId())
			next.Learners = appendUnique(next.Learners, single.GetNodeId())
		case pb.ConfChangeRemoveNode:
			next.Voters = removeID(next.Voters, single.GetNodeId())
			next.Learners = removeID(next.Learners, single.GetNodeId())
		case pb.ConfChangeUpdateNode:
		default:
			return nil, fmt.Errorf("unknown configuration change type %d", single.GetType())
		}
	}
	sort.Slice(next.Voters, func(left, right int) bool { return next.Voters[left] < next.Voters[right] })
	sort.Slice(next.Learners, func(left, right int) bool { return next.Learners[left] < next.Learners[right] })
	return next, nil
}

func cloneConfState(state *pb.ConfState) *pb.ConfState {
	if state == nil {
		return &pb.ConfState{}
	}
	return proto.Clone(state).(*pb.ConfState)
}

func cloneCommits(commits []CommitDTO) []CommitDTO {
	if commits == nil {
		return nil
	}
	cloned := make([]CommitDTO, len(commits))
	for index, commit := range commits {
		cloned[index] = CommitDTO{
			Index: commit.Index,
			Data:  bytes.Clone(commit.Data),
		}
	}
	return cloned
}

func cloneEntries(entries []*pb.Entry) []*pb.Entry {
	cloned := make([]*pb.Entry, len(entries))
	for index, entry := range entries {
		cloned[index] = proto.Clone(entry).(*pb.Entry)
	}
	return cloned
}

func trimEntriesAtSnapshot(entries []*pb.Entry, snapshotIndex uint64) []*pb.Entry {
	position := 0
	for position < len(entries) && entries[position].GetIndex() <= snapshotIndex {
		position++
	}
	return entries[position:]
}

func validateEntrySequence(entries []*pb.Entry) error {
	for index, entry := range entries {
		if entry == nil || entry.GetIndex() == 0 {
			return fmt.Errorf("entry has invalid index")
		}
		if index > 0 && entry.GetIndex() != entries[index-1].GetIndex()+1 {
			return fmt.Errorf("entry sequence is not contiguous")
		}
	}
	return nil
}

func appendUnique(values []uint64, value uint64) []uint64 {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func removeID(values []uint64, value uint64) []uint64 {
	result := values[:0]
	for _, existing := range values {
		if existing != value {
			result = append(result, existing)
		}
	}
	return result
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := filePersistenceOps.write(writer, data)
		if written < 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func snapshotFilename(index uint64) string {
	return fmt.Sprintf("snapshot-%020d.bin", index)
}

func parseSnapshotFilename(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".bin") {
		return 0, false
	}
	middle := strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), ".bin")
	if len(middle) != 20 {
		return 0, false
	}
	for _, character := range middle {
		if character < '0' || character > '9' {
			return 0, false
		}
	}
	index, err := strconv.ParseUint(middle, 10, 64)
	return index, err == nil
}

type boundedReader struct {
	data []byte
	pos  int
}

func newBoundedReader(data []byte) *boundedReader {
	return &boundedReader{data: data}
}

func (r *boundedReader) remaining() int {
	return len(r.data) - r.pos
}

func (r *boundedReader) byte() (byte, error) {
	if r.remaining() < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	value := r.data[r.pos]
	r.pos++
	return value, nil
}

func (r *boundedReader) bytes(length int) ([]byte, error) {
	if length < 0 || r.remaining() < length {
		return nil, io.ErrUnexpectedEOF
	}
	value := r.data[r.pos : r.pos+length]
	r.pos += length
	return value, nil
}

func (r *boundedReader) u32() (uint32, error) {
	data, err := r.bytes(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(data), nil
}

func (r *boundedReader) u64() (uint64, error) {
	data, err := r.bytes(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(data), nil
}
