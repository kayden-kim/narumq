//go:build !windows

package raft

import "os"

func renameFile(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}

func syncDirectory(path string) error {
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
