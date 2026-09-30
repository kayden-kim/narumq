package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

type powerLossJournal struct {
	mu        sync.Mutex
	dataRoot  string
	graveRoot string
	files     map[uint64]*fileUndo
	dirs      map[string][]directoryOperation
	graveSeq  uint64
}

type fileUndo struct {
	baseSize int64
	ranges   []savedRange
}

type savedRange struct {
	offset int64
	data   []byte
}

type directoryOperation struct {
	kind    string
	name    string
	newName string
	grave   string
}

type powerCutResult struct {
	Torn           bool              `json:"torn"`
	BrokerExitCode int               `json:"brokerExitCode"`
	ReleasedFsyncs int               `json:"releasedFsyncs"`
	Files          []rolledBackFile  `json:"files"`
	Entries        []rolledBackEntry `json:"entries"`
}

type rolledBackFile struct {
	Path           string `json:"path"`
	DurableBytes   int64  `json:"durableBytes"`
	UnsyncedBytes  int64  `json:"unsyncedBytes"`
	KeptBytes      int64  `json:"keptBytes"`
	RestoredRanges int    `json:"restoredRanges"`
}

type rolledBackEntry struct {
	Directory string `json:"directory"`
	Operation string `json:"operation"`
	Name      string `json:"name"`
	NewName   string `json:"newName,omitempty"`
}

func newPowerLossJournal(dataRoot, graveRoot string) *powerLossJournal {
	return &powerLossJournal{
		dataRoot:  dataRoot,
		graveRoot: graveRoot,
		files:     make(map[uint64]*fileUndo),
		dirs:      make(map[string][]directoryOperation),
	}
}

func (journal *powerLossJournal) recordWrite(ino uint64, reader *os.File, offset int64, length int) error {
	if offset < 0 || length < 0 {
		return fmt.Errorf("record write range: invalid offset %d or length %d", offset, length)
	}
	stat, err := reader.Stat()
	if err != nil {
		return fmt.Errorf("stat dirty file: %w", err)
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	undo, ok := journal.files[ino]
	if !ok {
		undo = &fileUndo{baseSize: stat.Size()}
		journal.files[ino] = undo
	}
	if length == 0 {
		return nil
	}

	end := offset + int64(length)
	if end < offset {
		end = int64(^uint64(0) >> 1)
	}
	limit := undo.baseSize
	if stat.Size() < limit {
		limit = stat.Size()
	}
	if offset >= limit || end <= offset {
		return nil
	}
	if end > limit {
		end = limit
	}
	data, err := readRange(reader, offset, end-offset)
	if err != nil {
		return fmt.Errorf("save overwritten file range: %w", err)
	}
	undo.ranges = append(undo.ranges, savedRange{offset: offset, data: data})
	return nil
}

func (journal *powerLossJournal) recordTruncate(ino uint64, reader *os.File, size int64) error {
	if size < 0 {
		return fmt.Errorf("record truncate: negative size %d", size)
	}
	stat, err := reader.Stat()
	if err != nil {
		return fmt.Errorf("stat truncated file: %w", err)
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	undo, ok := journal.files[ino]
	if !ok {
		undo = &fileUndo{baseSize: stat.Size()}
		journal.files[ino] = undo
	}
	if size >= stat.Size() {
		return nil
	}
	end := stat.Size()
	if undo.baseSize < end {
		end = undo.baseSize
	}
	if size >= end {
		return nil
	}
	data, err := readRange(reader, size, end-size)
	if err != nil {
		return fmt.Errorf("save truncated file range: %w", err)
	}
	undo.ranges = append(undo.ranges, savedRange{offset: size, data: data})
	return nil
}

func (journal *powerLossJournal) fileSynced(ino uint64) {
	journal.mu.Lock()
	delete(journal.files, ino)
	journal.mu.Unlock()
}

func (journal *powerLossJournal) recordCreate(dir, name string) {
	journal.recordDirectoryOperation(dir, directoryOperation{kind: "create", name: name})
}

func (journal *powerLossJournal) recordMkdir(dir, name string) {
	journal.recordDirectoryOperation(dir, directoryOperation{kind: "mkdir", name: name})
}

func (journal *powerLossJournal) recordDirectoryOperation(dir string, operation directoryOperation) {
	journal.mu.Lock()
	journal.dirs[cleanRelativeDirectory(dir)] = append(journal.dirs[cleanRelativeDirectory(dir)], operation)
	journal.mu.Unlock()
}

func (journal *powerLossJournal) unlink(dir, name string) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	dir = cleanRelativeDirectory(dir)
	grave := journal.nextGravePathLocked()
	if err := os.Rename(journal.dataPath(dir, name), grave); err != nil {
		return fmt.Errorf("move unlinked file to grave: %w", err)
	}
	journal.dirs[dir] = append(journal.dirs[dir], directoryOperation{kind: "unlink", name: name, grave: grave})
	return nil
}

func (journal *powerLossJournal) rmdir(dir, name string) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	dir = cleanRelativeDirectory(dir)
	if err := syscall.Rmdir(journal.dataPath(dir, name)); err != nil {
		return fmt.Errorf("remove directory: %w", err)
	}
	journal.dirs[dir] = append(journal.dirs[dir], directoryOperation{kind: "rmdir", name: name})
	return nil
}

func (journal *powerLossJournal) rename(dir, oldName, newName string) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	dir = cleanRelativeDirectory(dir)
	newPath := journal.dataPath(dir, newName)
	grave := ""
	if _, err := os.Lstat(newPath); err == nil {
		grave = journal.nextGravePathLocked()
		if err := os.Rename(newPath, grave); err != nil {
			return fmt.Errorf("move replaced file to grave: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect rename target: %w", err)
	}
	if err := os.Rename(journal.dataPath(dir, oldName), newPath); err != nil {
		if grave != "" {
			if restoreErr := os.Rename(grave, newPath); restoreErr != nil {
				return fmt.Errorf("rename file: %v; restore replaced target: %w", err, restoreErr)
			}
		}
		return fmt.Errorf("rename file: %w", err)
	}
	journal.dirs[dir] = append(journal.dirs[dir], directoryOperation{kind: "rename", name: oldName, newName: newName, grave: grave})
	return nil
}

func (journal *powerLossJournal) directorySynced(dir string) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	dir = cleanRelativeDirectory(dir)
	for _, operation := range journal.dirs[dir] {
		if operation.grave == "" {
			continue
		}
		if err := os.RemoveAll(operation.grave); err != nil {
			return fmt.Errorf("remove durable grave entry: %w", err)
		}
	}
	delete(journal.dirs, dir)
	return nil
}

func (journal *powerLossJournal) rollback(torn bool) (powerCutResult, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()

	result := powerCutResult{
		Torn:    torn,
		Files:   make([]rolledBackFile, 0),
		Entries: make([]rolledBackEntry, 0),
	}
	directories := make([]string, 0, len(journal.dirs))
	for dir := range journal.dirs {
		directories = append(directories, dir)
	}
	sort.Slice(directories, func(left, right int) bool {
		leftDepth := strings.Count(directories[left], "/")
		rightDepth := strings.Count(directories[right], "/")
		if directories[left] == "" {
			leftDepth = -1
		}
		if directories[right] == "" {
			rightDepth = -1
		}
		if leftDepth == rightDepth {
			return directories[left] < directories[right]
		}
		return leftDepth > rightDepth
	})
	for _, dir := range directories {
		operations := journal.dirs[dir]
		for index := len(operations) - 1; index >= 0; index-- {
			operation := operations[index]
			if err := journal.rollbackDirectoryOperation(dir, operation); err != nil {
				return result, err
			}
			result.Entries = append(result.Entries, rolledBackEntry{
				Directory: dir,
				Operation: operation.kind,
				Name:      operation.name,
				NewName:   operation.newName,
			})
		}
	}
	if err := os.RemoveAll(journal.graveRoot); err != nil {
		return result, fmt.Errorf("clear grave directory: %w", err)
	}

	pathsByInode := make(map[uint64]string)
	if err := filepath.WalkDir(journal.dataRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("stat %s has no inode number", path)
		}
		relative, err := filepath.Rel(journal.dataRoot, path)
		if err != nil {
			return err
		}
		pathsByInode[stat.Ino] = filepath.ToSlash(relative)
		return nil
	}); err != nil {
		return result, fmt.Errorf("index files before rollback: %w", err)
	}

	inodes := make([]uint64, 0, len(journal.files))
	for ino := range journal.files {
		inodes = append(inodes, ino)
	}
	sort.Slice(inodes, func(left, right int) bool {
		return pathsByInode[inodes[left]] < pathsByInode[inodes[right]]
	})
	for _, ino := range inodes {
		path, ok := pathsByInode[ino]
		if !ok {
			continue
		}
		undo := journal.files[ino]
		fullPath := filepath.Join(journal.dataRoot, filepath.FromSlash(path))
		file, err := os.OpenFile(fullPath, os.O_RDWR, 0)
		if err != nil {
			return result, fmt.Errorf("open dirty file %s: %w", path, err)
		}
		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return result, fmt.Errorf("stat dirty file %s: %w", path, err)
		}
		unsynced := stat.Size() - undo.baseSize
		if unsynced < 0 {
			unsynced = 0
		}
		kept := int64(0)
		if torn {
			kept = unsynced / 2
		}
		keptData, err := readRange(file, undo.baseSize, kept)
		if err != nil {
			file.Close()
			return result, fmt.Errorf("read torn file tail %s: %w", path, err)
		}
		if err := file.Truncate(undo.baseSize); err != nil {
			file.Close()
			return result, fmt.Errorf("truncate unsynced file tail %s: %w", path, err)
		}
		for index := len(undo.ranges) - 1; index >= 0; index-- {
			if err := writeRange(file, undo.ranges[index].offset, undo.ranges[index].data); err != nil {
				file.Close()
				return result, fmt.Errorf("restore overwritten file range %s: %w", path, err)
			}
		}
		if kept > 0 {
			if err := writeRange(file, undo.baseSize, keptData); err != nil {
				file.Close()
				return result, fmt.Errorf("restore torn file prefix %s: %w", path, err)
			}
		}
		if err := file.Close(); err != nil {
			return result, fmt.Errorf("close rolled back file %s: %w", path, err)
		}
		if unsynced > 0 || len(undo.ranges) > 0 {
			result.Files = append(result.Files, rolledBackFile{
				Path:           path,
				DurableBytes:   undo.baseSize,
				UnsyncedBytes:  unsynced,
				KeptBytes:      kept,
				RestoredRanges: len(undo.ranges),
			})
		}
	}
	clear(journal.files)
	clear(journal.dirs)
	return result, nil
}

func (journal *powerLossJournal) rollbackDirectoryOperation(dir string, operation directoryOperation) error {
	path := journal.dataPath(dir, operation.name)
	switch operation.kind {
	case "create":
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("rollback created entry %s: %w", path, err)
		}
	case "mkdir":
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("rollback created directory %s: %w", path, err)
		}
	case "unlink":
		if err := os.Rename(operation.grave, path); err != nil {
			return fmt.Errorf("restore unlinked entry %s: %w", path, err)
		}
	case "rmdir":
		if err := os.Mkdir(path, 0o755); err != nil {
			return fmt.Errorf("restore removed directory %s: %w", path, err)
		}
	case "rename":
		newPath := journal.dataPath(dir, operation.newName)
		if err := os.Rename(newPath, path); err != nil {
			return fmt.Errorf("restore renamed entry %s: %w", path, err)
		}
		if operation.grave != "" {
			if err := os.Rename(operation.grave, newPath); err != nil {
				return fmt.Errorf("restore replaced entry %s: %w", newPath, err)
			}
		}
	default:
		return fmt.Errorf("unknown directory operation %q", operation.kind)
	}
	return nil
}

func (journal *powerLossJournal) nextGravePathLocked() string {
	journal.graveSeq++
	return filepath.Join(journal.graveRoot, fmt.Sprint(journal.graveSeq))
}

func (journal *powerLossJournal) dataPath(dir, name string) string {
	return filepath.Join(journal.dataRoot, filepath.FromSlash(dir), name)
}

func cleanRelativeDirectory(dir string) string {
	if dir == "" || dir == "." {
		return ""
	}
	return strings.TrimPrefix(filepath.ToSlash(filepath.Clean(filepath.FromSlash(dir))), "./")
}

func readRange(reader io.ReaderAt, offset, length int64) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	if length < 0 || uint64(length) > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("invalid range length %d", length)
	}
	data := make([]byte, int(length))
	read, err := reader.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	if read != len(data) {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

func writeRange(writer io.WriterAt, offset int64, data []byte) error {
	written, err := writer.WriteAt(data, offset)
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	return nil
}
