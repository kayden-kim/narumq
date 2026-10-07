//go:build windows

package auditlog

import (
	"syscall"
	"unsafe"
)

var moveFileExAuditW = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

func renameFile(oldPath, newPath string) error {
	oldPointer, err := syscall.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}
	newPointer, err := syscall.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	const moveFileWriteThrough = 0x8
	result, _, callErr := moveFileExAuditW.Call(
		uintptr(unsafe.Pointer(oldPointer)),
		uintptr(unsafe.Pointer(newPointer)),
		moveFileWriteThrough,
	)
	if result != 0 {
		return nil
	}
	if callErr != syscall.Errno(0) {
		return callErr
	}
	return syscall.EINVAL
}

// MoveFileExW with MOVEFILE_WRITE_THROUGH persists a rename before returning.
func syncDirectory(string) error {
	return nil
}
