//go:build !linux

package tcp

import "syscall"

// setPeerUserTimeout leaves non-Linux sockets with keepalive only (ADR-0059 §1).
func setPeerUserTimeout(syscall.RawConn) error {
	return nil
}
