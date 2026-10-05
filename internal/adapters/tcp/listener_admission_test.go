package tcp

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	portsmetrics "narumq/internal/application/ports/metrics"
)

type admissionTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *admissionTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *admissionTestClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

type admissionRejection struct {
	transport string
	reason    string
}

type admissionTestRecorder struct {
	rejections chan admissionRejection
}

func newAdmissionTestRecorder() *admissionTestRecorder {
	return &admissionTestRecorder{rejections: make(chan admissionRejection, 32)}
}

func (recorder *admissionTestRecorder) ListenerAdmissionRejected(transport string, reason string) {
	recorder.rejections <- admissionRejection{transport: transport, reason: reason}
}

type admissionTestListener struct {
	net.Listener
	accepted chan net.Conn
	failures chan error
	done     chan struct{}
}

func newAdmissionTestListener(t *testing.T, config ListenerAdmissionConfig) *admissionTestListener {
	t.Helper()
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := NewAdmissionListener(inner, config)
	if err != nil {
		_ = inner.Close()
		t.Fatal(err)
	}
	testListener := &admissionTestListener{
		Listener: listener,
		accepted: make(chan net.Conn, 32),
		failures: make(chan error, 1),
		done:     make(chan struct{}),
	}
	go func() {
		defer close(testListener.done)
		for {
			connection, err := testListener.Listener.Accept()
			if err != nil {
				testListener.failures <- err
				return
			}
			testListener.accepted <- connection
		}
	}()
	t.Cleanup(func() { _ = testListener.Close() })
	return testListener
}

func (listener *admissionTestListener) Accept() (net.Conn, error) {
	select {
	case connection := <-listener.accepted:
		return connection, nil
	case err := <-listener.failures:
		return nil, err
	case <-time.After(2 * time.Second):
		return nil, errors.New("timed out waiting for accepted connection")
	}
}

func (listener *admissionTestListener) Close() error {
	err := listener.Listener.Close()
	<-listener.done
	return err
}

func admissionDial(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func admissionAccept(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func admissionAssertRejected(t *testing.T, connection net.Conn, recorder *admissionTestRecorder, reason string) {
	t.Helper()
	select {
	case got := <-recorder.rejections:
		if got.transport != portsmetrics.TransportTCP || got.reason != reason {
			t.Fatalf("rejection = (%q, %q), want (%q, %q)", got.transport, got.reason, portsmetrics.TransportTCP, reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not record rejected connection")
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var received [1]byte
	count, err := connection.Read(received[:])
	if count != 0 || err == nil {
		t.Fatalf("rejected socket read = (%d, %v), want zero bytes and an error", count, err)
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("rejected socket read timed out instead of closing: %v", err)
	}
}

// Traceability: PRD MQTT-09/MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.1.
func TestNewAdmissionListenerValidatesConfig(t *testing.T) {
	clock := &admissionTestClock{now: time.Now()}
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()

	valid := ListenerAdmissionConfig{Transport: portsmetrics.TransportTCP, MaxConnections: 1, Clock: clock}
	cases := []struct {
		name   string
		inner  net.Listener
		config ListenerAdmissionConfig
		want   error
	}{
		{name: "nil listener", config: valid, want: ErrNilListener},
		{name: "nil clock", inner: inner, config: ListenerAdmissionConfig{Transport: portsmetrics.TransportTCP, MaxConnections: 1}, want: ErrNilClock},
		{name: "invalid transport", inner: inner, config: ListenerAdmissionConfig{Transport: "ws", MaxConnections: 1, Clock: clock}, want: ErrInvalidConfig},
		{name: "both disabled", inner: inner, config: ListenerAdmissionConfig{Transport: portsmetrics.TransportTCP, Clock: clock}, want: ErrInvalidConfig},
		{name: "negative max connections", inner: inner, config: ListenerAdmissionConfig{Transport: portsmetrics.TransportTCP, MaxConnections: -1, Clock: clock}, want: ErrInvalidConfig},
		{name: "max connections above bound", inner: inner, config: ListenerAdmissionConfig{Transport: portsmetrics.TransportTCP, MaxConnections: MaxListenerAdmissionLimit + 1, Clock: clock}, want: ErrInvalidConfig},
		{name: "negative connection rate", inner: inner, config: ListenerAdmissionConfig{Transport: portsmetrics.TransportTCP, ConnectionRate: -1, Clock: clock}, want: ErrInvalidConfig},
		{name: "connection rate above bound", inner: inner, config: ListenerAdmissionConfig{Transport: portsmetrics.TransportTCP, ConnectionRate: MaxListenerAdmissionLimit + 1, Clock: clock}, want: ErrInvalidConfig},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			listener, err := NewAdmissionListener(test.inner, test.config)
			if listener != nil || !errors.Is(err, test.want) {
				t.Fatalf("NewAdmissionListener() = (%v, %v), want (nil, %v)", listener, err, test.want)
			}
		})
	}
}

// Traceability: PRD MQTT-09/MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.1.
func TestAdmissionListenerClosesSocketsAboveMaximumUntilRelease(t *testing.T) {
	recorder := newAdmissionTestRecorder()
	listener := newAdmissionTestListener(t, ListenerAdmissionConfig{
		Transport: portsmetrics.TransportTCP, MaxConnections: 1,
		Clock: &admissionTestClock{now: time.Now()}, Recorder: recorder,
	})
	_ = admissionDial(t, listener)
	firstServer := admissionAccept(t, listener)

	secondClient := admissionDial(t, listener)
	admissionAssertRejected(t, secondClient, recorder, portsmetrics.ListenerAdmissionReasonMaxConnections)
	thirdClient := admissionDial(t, listener)
	admissionAssertRejected(t, thirdClient, recorder, portsmetrics.ListenerAdmissionReasonMaxConnections)

	if err := firstServer.Close(); err != nil {
		t.Fatal(err)
	}
	_ = firstServer.Close()
	_ = admissionDial(t, listener)
	_ = admissionAccept(t, listener)
	fifthClient := admissionDial(t, listener)
	admissionAssertRejected(t, fifthClient, recorder, portsmetrics.ListenerAdmissionReasonMaxConnections)
}

// Traceability: PRD MQTT-09/MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.1.
func TestAdmissionListenerRefillsConnectionRateFromClock(t *testing.T) {
	clock := &admissionTestClock{now: time.Now()}
	recorder := newAdmissionTestRecorder()
	listener := newAdmissionTestListener(t, ListenerAdmissionConfig{
		Transport: portsmetrics.TransportTCP, ConnectionRate: 2, Clock: clock, Recorder: recorder,
	})
	_ = admissionDial(t, listener)
	firstServer := admissionAccept(t, listener)
	_ = admissionDial(t, listener)
	secondServer := admissionAccept(t, listener)
	thirdClient := admissionDial(t, listener)
	admissionAssertRejected(t, thirdClient, recorder, portsmetrics.ListenerAdmissionReasonConnectionRate)

	clock.Advance(500 * time.Millisecond)
	_ = admissionDial(t, listener)
	fourthServer := admissionAccept(t, listener)
	fifthClient := admissionDial(t, listener)
	admissionAssertRejected(t, fifthClient, recorder, portsmetrics.ListenerAdmissionReasonConnectionRate)

	clock.Advance(-time.Second)
	sixthClient := admissionDial(t, listener)
	admissionAssertRejected(t, sixthClient, recorder, portsmetrics.ListenerAdmissionReasonConnectionRate)

	clock.Advance(10 * time.Second)
	_ = admissionDial(t, listener)
	seventhServer := admissionAccept(t, listener)
	_ = admissionDial(t, listener)
	eighthServer := admissionAccept(t, listener)
	ninthClient := admissionDial(t, listener)
	admissionAssertRejected(t, ninthClient, recorder, portsmetrics.ListenerAdmissionReasonConnectionRate)

	for _, connection := range []net.Conn{firstServer, secondServer, fourthServer, seventhServer, eighthServer} {
		_ = connection.Close()
	}
}

// Traceability: PRD MQTT-09/MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.1.
func TestAdmissionListenerChecksMaximumBeforeRate(t *testing.T) {
	recorder := newAdmissionTestRecorder()
	listener := newAdmissionTestListener(t, ListenerAdmissionConfig{
		Transport: portsmetrics.TransportTCP, MaxConnections: 1, ConnectionRate: 2,
		Clock: &admissionTestClock{now: time.Now()}, Recorder: recorder,
	})
	_ = admissionDial(t, listener)
	firstServer := admissionAccept(t, listener)
	for range 3 {
		client := admissionDial(t, listener)
		admissionAssertRejected(t, client, recorder, portsmetrics.ListenerAdmissionReasonMaxConnections)
	}
	if err := firstServer.Close(); err != nil {
		t.Fatal(err)
	}
	_ = admissionDial(t, listener)
	secondServer := admissionAccept(t, listener)
	if err := secondServer.Close(); err != nil {
		t.Fatal(err)
	}
	thirdClient := admissionDial(t, listener)
	admissionAssertRejected(t, thirdClient, recorder, portsmetrics.ListenerAdmissionReasonConnectionRate)
}

// Traceability: PRD MQTT-09/MQTT-11; docs/specs/mqtt-protocol-and-session.md §7.1.
func TestAdmissionListenerReturnsInnerAcceptError(t *testing.T) {
	listener := newAdmissionTestListener(t, ListenerAdmissionConfig{
		Transport: portsmetrics.TransportTCP, MaxConnections: 1,
		Clock: &admissionTestClock{now: time.Now()},
	})
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := listener.Accept()
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept() error = %v, want net.ErrClosed", err)
	}
}
