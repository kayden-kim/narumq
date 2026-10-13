package websocket

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	coderws "github.com/coder/websocket"
	portsmetrics "narumq/internal/application/ports/metrics"
)

type conn struct {
	net.Conn
	ws            *coderws.Conn
	tlsState      *tls.ConnectionState
	readDeadline  atomic.Int64
	writeDeadline atomic.Int64
	closeOnce     sync.Once
	closeErr      error
}

var _ net.Conn = (*conn)(nil)

func newConn(websocketConnection *coderws.Conn, tlsState *tls.ConnectionState) *conn {
	return &conn{
		Conn:     coderws.NetConn(context.Background(), websocketConnection, coderws.MessageBinary),
		ws:       websocketConnection,
		tlsState: tlsState,
	}
}

func (connection *conn) MetricsTransport() string {
	if connection.tlsState != nil {
		return portsmetrics.TransportWSS
	}
	return portsmetrics.TransportWS
}

func (connection *conn) ConnectionState() tls.ConnectionState {
	if connection.tlsState == nil {
		return tls.ConnectionState{}
	}
	return *connection.tlsState
}

func (connection *conn) SetDeadline(deadline time.Time) error {
	deadlineValue := unixNanoDeadline(deadline)
	connection.readDeadline.Store(deadlineValue)
	connection.writeDeadline.Store(deadlineValue)
	return connection.Conn.SetDeadline(deadline)
}

func (connection *conn) SetReadDeadline(deadline time.Time) error {
	connection.readDeadline.Store(unixNanoDeadline(deadline))
	return connection.Conn.SetReadDeadline(deadline)
}

func (connection *conn) SetWriteDeadline(deadline time.Time) error {
	connection.writeDeadline.Store(unixNanoDeadline(deadline))
	return connection.Conn.SetWriteDeadline(deadline)
}

func (connection *conn) Read(buffer []byte) (int, error) {
	count, err := connection.Conn.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		deadline := connection.readDeadline.Load()
		if deadline != 0 && time.Now().UnixNano() >= deadline {
			err = os.ErrDeadlineExceeded
		}
	}
	return count, err
}

func (connection *conn) Write(buffer []byte) (int, error) {
	count, err := connection.Conn.Write(buffer)
	if err != nil {
		deadline := connection.writeDeadline.Load()
		if deadline != 0 && time.Now().UnixNano() >= deadline {
			err = os.ErrDeadlineExceeded
		}
	}
	return count, err
}

func (connection *conn) Close() error {
	connection.closeOnce.Do(func() {
		connection.closeErr = connection.ws.CloseNow()
		// CloseNow shuts the socket; NetConn.Close only stops its timers.
		_ = connection.Conn.Close()
	})
	return connection.closeErr
}

func unixNanoDeadline(deadline time.Time) int64 {
	if deadline.IsZero() {
		return 0
	}
	return deadline.UnixNano()
}
