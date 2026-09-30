package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestChaosFSMountedPowerLossAndSyncModes(t *testing.T) {
	if os.Getenv("NARUMQ_CHAOSFS_MOUNT_TEST") != "1" {
		t.Skip("set NARUMQ_CHAOSFS_MOUNT_TEST=1 to exercise the mounted FUSE filesystem")
	}
	t.Run("file and directory durability", testChaosFSMountDurability)
	t.Run("held WAL fsync and torn rollback", testChaosFSMountHeldWALFsync)
	t.Run("file and directory sync failures", testChaosFSMountSyncFailures)
}

func TestChaosFSMountedRejectsLateHeldWALFsync(t *testing.T) {
	if os.Getenv("NARUMQ_CHAOSFS_MOUNT_TEST") != "1" {
		t.Skip("set NARUMQ_CHAOSFS_MOUNT_TEST=1 to exercise the mounted FUSE filesystem")
	}

	mount, _, mountPoint, markUnmounted := mountChaosFSTest(t)
	nodePath := filepath.Join(mountPoint, "node")
	if err := os.Mkdir(nodePath, 0o755); err != nil {
		t.Fatal(err)
	}
	syncMountedDirectory(t, mountPoint)
	syncMountedDirectory(t, nodePath)
	wal := createMountedFile(t, filepath.Join(nodePath, "wal.bin"), "durable")
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := mount.controller.setMode(string(storageModeHoldWALFsync)); err != nil {
		t.Fatal(err)
	}

	mount.controller.beginCut()
	syncResult := make(chan error, 1)
	go func() { syncResult <- wal.Sync() }()
	select {
	case err := <-syncResult:
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("late WAL Sync error = %v, want EIO", err)
		}
	case <-time.After(5 * time.Second):
		released := mount.controller.releaseHeld()
		t.Fatalf("late WAL Sync blocked after cut; released %d held fsyncs", released)
	}
	if held := mount.controller.status().HeldWalFsyncs; held != 0 {
		t.Fatalf("late WAL Sync registered %d held fsyncs after cut, want 0", held)
	}
	closeMountedFile(t, wal)
	if _, err := mount.unmountAndRollback(false); err != nil {
		t.Fatal(err)
	}
	markUnmounted()
}

func testChaosFSMountDurability(t *testing.T) {
	mount, _, mountPoint, markUnmounted := mountChaosFSTest(t)
	dataRoot := filepath.Join(mount.backing, "data")

	fileA := createMountedFile(t, filepath.Join(mountPoint, "a"), "durable-a")
	if err := fileA.Sync(); err != nil {
		t.Fatal(err)
	}
	closeMountedFile(t, fileA)
	syncMountedDirectory(t, mountPoint)

	appendA, err := os.OpenFile(filepath.Join(mountPoint, "a"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appendA.WriteString("-unsynced"); err != nil {
		t.Fatal(err)
	}
	closeMountedFile(t, appendA)

	fileB := createMountedFile(t, filepath.Join(mountPoint, "b"), "file-synced-only")
	if err := fileB.Sync(); err != nil {
		t.Fatal(err)
	}
	closeMountedFile(t, fileB)

	tempFile := createMountedFile(t, filepath.Join(mountPoint, "tmp"), "renamed-file")
	if err := tempFile.Sync(); err != nil {
		t.Fatal(err)
	}
	closeMountedFile(t, tempFile)
	if err := os.Rename(filepath.Join(mountPoint, "tmp"), filepath.Join(mountPoint, "c")); err != nil {
		t.Fatal(err)
	}

	mount.controller.beginCut()
	if _, err := mount.unmountAndRollback(false); err != nil {
		t.Fatal(err)
	}
	markUnmounted()

	assertFileContents(t, filepath.Join(dataRoot, "a"), "durable-a")
	for _, name := range []string{"b", "c", "tmp"} {
		if _, err := os.Stat(filepath.Join(dataRoot, name)); !os.IsNotExist(err) {
			t.Fatalf("%s stat error after power cut = %v, want not-exist", name, err)
		}
	}
}

func testChaosFSMountHeldWALFsync(t *testing.T) {
	mount, _, mountPoint, markUnmounted := mountChaosFSTest(t)
	nodePath := filepath.Join(mountPoint, "node")
	if err := os.Mkdir(nodePath, 0o755); err != nil {
		t.Fatal(err)
	}
	syncMountedDirectory(t, mountPoint)
	syncMountedDirectory(t, nodePath)

	walPath := filepath.Join(nodePath, "wal.bin")
	wal := createMountedFile(t, walPath, "base")
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	syncMountedDirectory(t, nodePath)
	if err := mount.controller.setMode(string(storageModeHoldWALFsync)); err != nil {
		t.Fatal(err)
	}
	if _, err := wal.WriteString("abcdef"); err != nil {
		t.Fatal(err)
	}
	syncResult := make(chan error, 1)
	go func() { syncResult <- wal.Sync() }()
	waitForHeldWALFsync(t, mount.controller)

	mount.controller.beginCut()
	if released := mount.controller.releaseHeld(); released != 1 {
		t.Fatalf("released held WAL fsyncs = %d, want 1", released)
	}
	select {
	case err := <-syncResult:
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("held WAL Sync error = %v, want EIO", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("held WAL Sync did not return after power cut")
	}
	closeMountedFile(t, wal)

	result, err := mount.unmountAndRollback(true)
	if err != nil {
		t.Fatal(err)
	}
	markUnmounted()
	rolledBack := false
	for _, file := range result.Files {
		if file.Path == "node/wal.bin" {
			rolledBack = true
			if file.DurableBytes != 4 || file.UnsyncedBytes != 6 || file.KeptBytes != 3 {
				t.Fatalf("torn WAL rollback = %+v", file)
			}
		}
	}
	if !rolledBack {
		t.Fatalf("power cut files = %+v, want node/wal.bin", result.Files)
	}
	assertFileContents(t, filepath.Join(mount.backing, "data", "node", "wal.bin"), "baseabc")
}

func testChaosFSMountSyncFailures(t *testing.T) {
	mount, _, mountPoint, _ := mountChaosFSTest(t)
	file := createMountedFile(t, filepath.Join(mountPoint, "failure.bin"), "sync-error")
	if err := mount.controller.setMode(string(storageModeFailFsync)); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("file Sync error = %v, want EIO", err)
	}
	if err := mount.controller.setMode(string(storageModeFailDirsync)); err != nil {
		t.Fatal(err)
	}
	if err := syncMountedDirectoryResult(mountPoint); !errors.Is(err, syscall.EIO) {
		t.Fatalf("directory Sync error = %v, want EIO", err)
	}
	if err := file.Sync(); err != nil {
		t.Fatalf("file Sync in fail-dirsync mode = %v, want success", err)
	}
	closeMountedFile(t, file)
}

func mountChaosFSTest(t *testing.T) (*chaosMount, *chaosController, string, func()) {
	t.Helper()
	backing := t.TempDir()
	mountPoint := filepath.Join(t.TempDir(), "mount")
	controller := newChaosController(&ioDelay{})
	mount, err := mountChaosFS(backing, mountPoint, controller)
	if err != nil {
		t.Fatal(err)
	}
	mounted := true
	markUnmounted := func() { mounted = false }
	t.Cleanup(func() {
		if mounted {
			if err := mount.unmount(); err != nil {
				t.Errorf("unmount chaos filesystem: %v", err)
			}
		}
	})
	return mount, controller, mountPoint, markUnmounted
}

func createMountedFile(t *testing.T, path, contents string) *os.File {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(contents); err != nil {
		file.Close()
		t.Fatal(err)
	}
	return file
}

func closeMountedFile(t *testing.T, file *os.File) {
	t.Helper()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func syncMountedDirectory(t *testing.T, path string) {
	t.Helper()
	if err := syncMountedDirectoryResult(path); err != nil {
		t.Fatal(err)
	}
}

func syncMountedDirectoryResult(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func waitForHeldWALFsync(t *testing.T, controller *chaosController) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if controller.status().HeldWalFsyncs >= 1 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("WAL fsync did not enter held state")
		case <-ticker.C:
		}
	}
}
