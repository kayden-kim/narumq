//go:build windows

package raft

import (
	"errors"
	"syscall"
)

// IsStorageFull reports whether err was caused by exhausted filesystem capacity.
func IsStorageFull(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.Errno(112)) || errors.Is(err, syscall.Errno(39))
}
