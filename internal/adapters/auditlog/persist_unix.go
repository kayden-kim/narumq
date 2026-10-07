//go:build !windows

package auditlog

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
