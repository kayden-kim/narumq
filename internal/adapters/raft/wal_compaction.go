package raft

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const walCompactionName = walName + ".tmp"

// compactWAL requires the Node mutex and an idle WAL worker. Memory contains only
// completed durable appends, including persisted entries beyond the commit index.
func (s *stableStore) compactWAL(snapshotIndex uint64) error {
	last, err := s.memory.LastIndex()
	if err != nil {
		return fmt.Errorf("compact WAL: %w", err)
	}
	var tail []*pb.Entry
	if snapshotIndex < last {
		tail, err = s.memory.Entries(snapshotIndex+1, last+1, math.MaxUint64)
		if err != nil {
			return fmt.Errorf("compact WAL: %w", err)
		}
	}
	limit := maxWALFramePayload
	if s.dynamic {
		limit = maxDynamicWALFramePayload
	}
	tmpPath := filepath.Join(s.directory, walCompactionName)
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("compact WAL: %w", err)
	}
	// Before closing the old WAL, all failures leave its recoverable bytes intact.
	persistErr := writeAll(tmp, []byte(walMagic))
	if persistErr == nil {
		record := walRecord{}
		if !etcdraft.IsEmptyHardState(s.hardState) {
			record.hardState = s.hardState
		}
		for record.persistent() || len(tail) != 0 {
			// Four format bytes, optional 24-byte HardState, four-byte entry count.
			size := 8
			if record.hardState != nil {
				size += 24
			}
			count := 0
			for count < len(tail) {
				entrySize := 4 + proto.Size(tail[count])
				if size+entrySize > limit {
					break
				}
				size += entrySize
				count++
			}
			if count == 0 && len(tail) != 0 && record.hardState == nil {
				persistErr = fmt.Errorf("entry exceeds WAL frame bound")
				break
			}
			record.entries = tail[:count]
			payload, encodeErr := encodeWALRecordMode(record, s.dynamic)
			if encodeErr != nil {
				persistErr = encodeErr
				break
			}
			header := walFrameHeader(payload)
			if persistErr = writeAll(tmp, header[:]); persistErr != nil {
				break
			}
			if persistErr = writeAll(tmp, payload); persistErr != nil {
				break
			}
			tail = tail[count:]
			record = walRecord{}
		}
	}
	if persistErr == nil {
		persistErr = filePersistenceOps.sync(tmp)
	}
	closeErr := tmp.Close()
	if persistErr == nil {
		persistErr = closeErr
	}
	if persistErr != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("compact WAL: %w", persistErr)
	}
	// Windows cannot atomically replace an open destination file.
	if err := s.close(); err != nil {
		return fmt.Errorf("compact WAL: close previous WAL: %w", err)
	}
	if err := filePersistenceOps.rename(tmpPath, filepath.Join(s.directory, walName)); err != nil {
		return fmt.Errorf("compact WAL: replace WAL: %w", err)
	}
	if err := filePersistenceOps.syncDirectory(s.directory); err != nil {
		return fmt.Errorf("compact WAL: sync WAL directory: %w", err)
	}
	wal, err := os.OpenFile(filepath.Join(s.directory, walName), os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("compact WAL: reopen WAL: %w", err)
	}
	if _, err := wal.Seek(0, io.SeekEnd); err != nil {
		_ = wal.Close()
		return fmt.Errorf("compact WAL: reopen WAL: %w", err)
	}
	s.wal = wal
	return nil
}

func pruneSnapshotFiles(directory string, latest uint64) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("prune snapshots: %w", err)
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		index, ok := parseSnapshotFilename(strings.TrimSuffix(name, ".tmp"))
		if !ok || index >= latest {
			continue
		}
		if err := os.Remove(filepath.Join(directory, name)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("prune snapshots: %w", err)
		}
		removed = true
	}
	if removed {
		if err := filePersistenceOps.syncDirectory(directory); err != nil {
			return fmt.Errorf("prune snapshots: %w", err)
		}
	}
	return nil
}
