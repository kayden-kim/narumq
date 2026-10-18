package tcp

import (
	"net"
	"syscall"
	"time"
)

// Dynamic peer socket liveness follows ADR-0059. Only the long-lived Raft and forward
// streams use it: the dialing side sets it before connecting and the accepting side
// sets it after the stream magic is read. The kernel then closes a stream whose remote
// host stopped answering, and the dialing side redials the committed address.
const (
	peerKeepAliveIdle     = 5 * time.Second
	peerKeepAliveInterval = 5 * time.Second
	peerKeepAliveCount    = 2
	peerTCPUserTimeout    = 10 * time.Second
)

var peerKeepAlive = net.KeepAliveConfig{
	Enable:   true,
	Idle:     peerKeepAliveIdle,
	Interval: peerKeepAliveInterval,
	Count:    peerKeepAliveCount,
}

// newPeerNetDialer returns the TCP dialer for dynamic peer streams.
func newPeerNetDialer() *net.Dialer {
	return &net.Dialer{
		Timeout:         peerIOTimeout,
		KeepAliveConfig: peerKeepAlive,
		Control: func(_, _ string, raw syscall.RawConn) error {
			return setPeerUserTimeout(raw)
		},
	}
}

// applyPeerLiveness sets the liveness options on an accepted stream socket. Non-TCP
// sockets, such as in-memory test pipes, keep their defaults.
func applyPeerLiveness(socket net.Conn) error {
	connection, ok := socket.(*net.TCPConn)
	if !ok {
		return nil
	}
	if err := connection.SetKeepAliveConfig(peerKeepAlive); err != nil {
		return err
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return err
	}
	return setPeerUserTimeout(raw)
}
