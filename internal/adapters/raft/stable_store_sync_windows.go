//go:build windows

package raft

import (
	"syscall"
	"time"
	"unsafe"
)

var moveFileExW = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

func renameFile(oldPath, newPath string) error {
	oldPointer, err := syscall.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}
	newPointer, err := syscall.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	const moveFileReplaceExisting = 0x1
	const moveFileWriteThrough = 0x8
	var deadline time.Time
	for {
		result, _, callErr := moveFileExW.Call(
			uintptr(unsafe.Pointer(oldPointer)),
			uintptr(unsafe.Pointer(newPointer)),
			moveFileReplaceExisting|moveFileWriteThrough,
		)
		if result != 0 {
			return nil
		}
		// A reader without FILE_SHARE_DELETE can briefly prevent replacement.
		// Keep the durable rename atomic; never delete the destination first.
		if callErr != syscall.ERROR_ACCESS_DENIED && callErr != syscall.Errno(32) {
			if callErr != syscall.Errno(0) {
				return callErr
			}
			return syscall.EINVAL
		}
		if deadline.IsZero() {
			deadline = time.Now().Add(250 * time.Millisecond)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return callErr
		}
		time.Sleep(min(10*time.Millisecond, remaining))
	}
}

// MoveFileExW with MOVEFILE_WRITE_THROUGH persists the renamed directory entry
// before returning, providing the Windows equivalent of syncing the directory.
func syncDirectory(string) error {
	return nil
}
