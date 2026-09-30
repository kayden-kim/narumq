package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestPowerLossJournalRollsBackUnsyncedAppend(t *testing.T) {
	for _, testCase := range []struct {
		name string
		torn bool
		want string
	}{
		{name: "clean", want: "base"},
		{name: "torn", torn: true, want: "basela"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			journal, dataRoot, graveRoot := newTestPowerLossJournal(t)
			path := filepath.Join(dataRoot, "wal.bin")
			file := writeDurableTestFile(t, journal, path, []byte("base"))
			ino := testInode(t, file)
			if err := journal.recordWrite(ino, file, 4, 5); err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt([]byte("later"), 4); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}

			result, err := journal.rollback(testCase.torn)
			if err != nil {
				t.Fatal(err)
			}
			assertFileContents(t, path, testCase.want)
			if len(result.Files) != 1 || result.Files[0].Path != "wal.bin" || result.Files[0].DurableBytes != 4 || result.Files[0].UnsyncedBytes != 5 || result.Files[0].KeptBytes != map[bool]int64{true: 2, false: 0}[testCase.torn] {
				t.Fatalf("rollback files = %+v", result.Files)
			}
			assertGraveEmpty(t, graveRoot)
		})
	}
}

func TestPowerLossJournalRestoresOverwrittenSyncedBytes(t *testing.T) {
	journal, dataRoot, _ := newTestPowerLossJournal(t)
	path := filepath.Join(dataRoot, "state.bin")
	file := writeDurableTestFile(t, journal, path, []byte("durable-content"))
	ino := testInode(t, file)
	if err := journal.recordWrite(ino, file, 2, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("XXXX"), 2); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := journal.rollback(false)
	if err != nil {
		t.Fatal(err)
	}
	assertFileContents(t, path, "durable-content")
	if len(result.Files) != 1 || result.Files[0].RestoredRanges != 1 {
		t.Fatalf("rollback files = %+v, want one restored range", result.Files)
	}
}

func TestPowerLossJournalRestoresTruncateAndRewrite(t *testing.T) {
	journal, dataRoot, _ := newTestPowerLossJournal(t)
	path := filepath.Join(dataRoot, "state.bin")
	file := writeDurableTestFile(t, journal, path, []byte("durable-content"))
	ino := testInode(t, file)
	if err := journal.recordTruncate(ino, file, 5); err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(5); err != nil {
		t.Fatal(err)
	}
	if err := journal.recordWrite(ino, file, 0, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("fresh"), 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := journal.rollback(false)
	if err != nil {
		t.Fatal(err)
	}
	assertFileContents(t, path, "durable-content")
	if len(result.Files) != 1 || result.Files[0].RestoredRanges != 2 {
		t.Fatalf("rollback files = %+v, want two restored ranges", result.Files)
	}
}

func TestPowerLossJournalCreateRequiresDirectorySync(t *testing.T) {
	for _, synced := range []bool{false, true} {
		t.Run(map[bool]string{false: "without directory sync", true: "with directory sync"}[synced], func(t *testing.T) {
			journal, dataRoot, _ := newTestPowerLossJournal(t)
			journal.recordCreate("", "created")
			path := filepath.Join(dataRoot, "created")
			if err := os.WriteFile(path, []byte("new"), 0o600); err != nil {
				t.Fatal(err)
			}
			if synced {
				if err := journal.directorySynced(""); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := journal.rollback(false); err != nil {
				t.Fatal(err)
			}
			_, err := os.Stat(path)
			if synced && err != nil {
				t.Fatalf("directory-synced create was lost: %v", err)
			}
			if !synced && !os.IsNotExist(err) {
				t.Fatalf("unsynced create stat error = %v, want not-exist", err)
			}
		})
	}
}

func TestPowerLossJournalRenameReplacingTarget(t *testing.T) {
	for _, synced := range []bool{false, true} {
		t.Run(map[bool]string{false: "without directory sync", true: "with directory sync"}[synced], func(t *testing.T) {
			journal, dataRoot, graveRoot := newTestPowerLossJournal(t)
			source := filepath.Join(dataRoot, "source")
			target := filepath.Join(dataRoot, "target")
			if err := os.WriteFile(source, []byte("source"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := journal.rename("", "source", "target"); err != nil {
				t.Fatal(err)
			}
			if synced {
				if err := journal.directorySynced(""); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := journal.rollback(false); err != nil {
				t.Fatal(err)
			}
			if synced {
				assertFileContents(t, target, "source")
				if _, err := os.Stat(source); !os.IsNotExist(err) {
					t.Fatalf("source exists after durable rename: %v", err)
				}
			} else {
				assertFileContents(t, source, "source")
				assertFileContents(t, target, "target")
			}
			assertGraveEmpty(t, graveRoot)
		})
	}
}

func TestPowerLossJournalRestoresUnlinkedFile(t *testing.T) {
	journal, dataRoot, _ := newTestPowerLossJournal(t)
	path := filepath.Join(dataRoot, "offline")
	if err := os.WriteFile(path, []byte("durable payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := journal.unlink("", "offline"); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.rollback(false); err != nil {
		t.Fatal(err)
	}
	assertFileContents(t, path, "durable payload")
}

func newTestPowerLossJournal(t *testing.T) (*powerLossJournal, string, string) {
	t.Helper()
	backing := t.TempDir()
	dataRoot := filepath.Join(backing, "data")
	graveRoot := filepath.Join(backing, "grave")
	if err := os.Mkdir(dataRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(graveRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	return newPowerLossJournal(dataRoot, graveRoot), dataRoot, graveRoot
}

func writeDurableTestFile(t *testing.T, journal *powerLossJournal, path string, data []byte) *os.File {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	journal.fileSynced(testInode(t, file))
	return file
}

func testInode(t *testing.T, file *os.File) uint64 {
	t.Helper()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("file stat has no inode")
	}
	return stat.Ino
}

func assertFileContents(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Fatalf("file %s = %q, want %q", path, data, want)
	}
}

func assertGraveEmpty(t *testing.T, graveRoot string) {
	t.Helper()
	entries, err := os.ReadDir(graveRoot)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("grave entries = %s, want empty", strings.Join(names, ", "))
	}
}

func TestChaosControllerRejectsHeldFsyncResumingAfterRemount(t *testing.T) {
	controller := newChaosController(&ioDelay{})
	if err := controller.setMode(string(storageModeHoldWALFsync)); err != nil {
		t.Fatal(err)
	}

	controller.opMu.RLock()
	generation := controller.cutGeneration
	controller.opMu.RUnlock()

	const rel = "node/wal.bin"
	controller.mu.Lock()
	controller.held = append(controller.held, rel)
	release := controller.release
	controller.mu.Unlock()

	controller.beginCut()
	if released := controller.releaseHeld(); released != 1 {
		t.Fatalf("released held fsyncs = %d, want 1", released)
	}
	controller.resetAfterRemount(&powerLossJournal{})

	if errno := controller.waitHeldFsync(context.Background(), rel, release, generation); errno != syscall.EIO {
		t.Fatalf("resumed held fsync errno = %v, want EIO", errno)
	}
}
