package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const maxDelayMilliseconds = int64((1<<63 - 1) / int64(time.Millisecond))

type fillDiskResult struct {
	Path           string `json:"path"`
	BytesWritten   uint64 `json:"bytesWritten"`
	FreeBytesAfter uint64 `json:"freeBytesAfter"`
	StoppedBy      string `json:"stoppedBy"`
}

func runFillDisk(args []string) error {
	flags := newFlagSet("fill-disk")
	path := flags.String("path", "", "file path to fill until ENOSPC")
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if err := requireFlag("path", *path); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*path), 0o755); err != nil {
		return fmt.Errorf("create ballast directory: %w", err)
	}
	file, err := os.OpenFile(*path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create ballast file: %w", err)
	}

	var bytesWritten uint64
	largeWritten, full, err := writeUntilFull(file, make([]byte, 1<<20))
	bytesWritten += largeWritten
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("fill ballast with 1 MiB blocks: %w", err)
	}
	if !full {
		smallWritten, stopped, writeErr := writeUntilFull(file, make([]byte, 4<<10))
		bytesWritten += smallWritten
		if writeErr != nil {
			_ = file.Close()
			return fmt.Errorf("fill ballast with 4 KiB blocks: %w", writeErr)
		}
		full = stopped
	}
	if !full {
		_ = file.Close()
		return errors.New("ballast write stopped without reaching ENOSPC")
	}

	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil && !errors.Is(syncErr, syscall.ENOSPC) {
		return fmt.Errorf("sync ballast file: %w", syncErr)
	}
	if closeErr != nil && !errors.Is(closeErr, syscall.ENOSPC) {
		return fmt.Errorf("close ballast file: %w", closeErr)
	}
	free, err := freeBytes(filepath.Dir(*path))
	if err != nil {
		return fmt.Errorf("read remaining filesystem capacity: %w", err)
	}
	return writeJSON(fillDiskResult{Path: *path, BytesWritten: bytesWritten, FreeBytesAfter: free, StoppedBy: "ENOSPC"})
}

func writeUntilFull(file *os.File, block []byte) (uint64, bool, error) {
	var total uint64
	for {
		written, err := file.Write(block)
		total += uint64(written)
		if errors.Is(err, syscall.ENOSPC) {
			return total, true, nil
		}
		if err != nil {
			return total, false, err
		}
		if written != len(block) {
			return total, false, io.ErrShortWrite
		}
	}
}

func runFreeDisk(args []string) error {
	flags := newFlagSet("free-disk")
	path := flags.String("path", "", "ballast file to remove")
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if err := requireFlag("path", *path); err != nil {
		return err
	}
	if err := os.Remove(*path); err != nil {
		return fmt.Errorf("remove ballast file: %w", err)
	}
	free, err := freeBytes(filepath.Dir(*path))
	if err != nil {
		return fmt.Errorf("read remaining filesystem capacity: %w", err)
	}
	return writeJSON(struct {
		Path           string `json:"path"`
		FreeBytesAfter uint64 `json:"freeBytesAfter"`
	}{Path: *path, FreeBytesAfter: free})
}

func freeBytes(path string) (uint64, error) {
	var stats syscall.Statfs_t
	if err := syscall.Statfs(path, &stats); err != nil {
		return 0, err
	}
	return uint64(stats.Bavail) * uint64(stats.Bsize), nil
}

func runSetIODelay(args []string) error {
	flags := newFlagSet("set-io-delay")
	path := flags.String("file", "", "atomic I/O delay control file")
	milliseconds := flags.Int64("ms", -1, "delay for each filesystem write and fsync")
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if err := requireFlag("file", *path); err != nil {
		return err
	}
	if *milliseconds < 0 {
		return errors.New("--ms must be non-negative")
	}
	if *milliseconds > maxDelayMilliseconds {
		return errors.New("--ms exceeds the maximum delay")
	}
	if err := replaceDelayFile(*path, *milliseconds); err != nil {
		return err
	}
	return writeJSON(struct {
		DelayMs int64 `json:"delayMs"`
	}{DelayMs: *milliseconds})
}

func replaceDelayFile(path string, milliseconds int64) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create delay file directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".io-delay-*")
	if err != nil {
		return fmt.Errorf("create temporary delay file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := fmt.Fprintf(temporary, "%d\n", milliseconds); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write delay value: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync delay value: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close delay value: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("install delay value: %w", err)
	}
	return nil
}

func runProbeIO(args []string) error {
	flags := newFlagSet("probe-io")
	path := flags.String("path", "", "temporary file path on the storage under test")
	if err := parseFlags(flags, args); err != nil {
		return err
	}
	if err := requireFlag("path", *path); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*path), 0o755); err != nil {
		return fmt.Errorf("create probe directory: %w", err)
	}
	file, err := os.OpenFile(*path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create I/O probe file: %w", err)
	}
	startedAt := time.Now()
	written, writeErr := file.Write(make([]byte, 4096))
	if writeErr == nil && written != 4096 {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if removeErr := os.Remove(*path); removeErr != nil && writeErr == nil {
		writeErr = removeErr
	}
	if writeErr != nil {
		return fmt.Errorf("run I/O probe: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close I/O probe file: %w", closeErr)
	}
	return writeJSON(struct {
		Path       string `json:"path"`
		Bytes      int    `json:"bytes"`
		DurationMs int64  `json:"durationMs"`
	}{Path: *path, Bytes: 4096, DurationMs: time.Since(startedAt).Milliseconds()})
}

func parseDelayValue(data []byte) (int64, bool) {
	value, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	return value, err == nil && value >= 0 && value <= maxDelayMilliseconds
}
