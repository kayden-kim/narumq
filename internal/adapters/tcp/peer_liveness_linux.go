//go:build linux

package tcp

import (
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func setPeerUserTimeout(raw syscall.RawConn) error {
	var optionErr error
	if err := raw.Control(func(fd uintptr) {
		optionErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT,
			int(peerTCPUserTimeout/time.Millisecond))
	}); err != nil {
		return err
	}
	return optionErr
}
