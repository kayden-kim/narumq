package websocket

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

func newWebSocketTestPair(t *testing.T) (net.Conn, *coderws.Conn) {
	t.Helper()
	listener := newTestListener(t)
	accepted := startListenerAccept(listener)
	client, _, err := dialTestWebSocket(t, listener.Addr().String(), Path, []string{Subprotocol}, nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	connection := waitListenerAccept(t, accepted)
	t.Cleanup(func() {
		_ = connection.Close()
		_ = client.CloseNow()
	})
	return connection, client
}

func newRawWebSocketTestPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	listener := newTestListener(t)
	accepted := startListenerAccept(listener)
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, "http://"+listener.Addr().String()+Path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Protocol", Subprotocol)
	if err := request.Write(client); err != nil {
		t.Fatalf("write WebSocket upgrade request: %v", err)
	}
	response, err := http.ReadResponse(bufio.NewReader(client), request)
	if err != nil {
		t.Fatalf("read WebSocket upgrade response: %v", err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want %d", response.StatusCode, http.StatusSwitchingProtocols)
	}
	if err := client.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	return waitListenerAccept(t, accepted), client
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-02.
func TestConnReadDeadlineReportsTimeout(t *testing.T) {
	connection, _ := newWebSocketTestPair(t)
	if err := connection.SetReadDeadline(time.Now().Add(40 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	_, err := connection.Read(make([]byte, 1))
	var timeout net.Error
	if !errors.Is(err, os.ErrDeadlineExceeded) || !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("Read() error = %v, want os.ErrDeadlineExceeded and net.Error timeout", err)
	}
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-02.
func TestConnWriteDeadlineReportsTimeout(t *testing.T) {
	connection, _ := newWebSocketTestPair(t)
	deadline := time.Now().Add(100 * time.Millisecond)
	if err := connection.SetWriteDeadline(deadline); err != nil {
		t.Fatalf("SetWriteDeadline() error = %v", err)
	}
	payload := make([]byte, 1<<20)
	var err error
	for {
		_, err = connection.Write(payload)
		if err != nil {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("Write() kept succeeding after its deadline")
		}
	}
	var timeout net.Error
	if !errors.Is(err, os.ErrDeadlineExceeded) || !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("Write() error = %v, want os.ErrDeadlineExceeded and net.Error timeout", err)
	}
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-02.
func TestConnCloseDoesNotWaitForUnresponsivePeer(t *testing.T) {
	connection, peer := newRawWebSocketTestPair(t)
	started := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- connection.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("Close() took %s, want at most one second", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() waited for the unresponsive peer")
	}

	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var frame [2]byte
	count, err := peer.Read(frame[:])
	if count != 0 || err == nil {
		t.Fatalf("peer read = (%d, %v), want abrupt socket close without a WebSocket close frame", count, err)
	}
}
