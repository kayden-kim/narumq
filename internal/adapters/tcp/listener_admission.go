package tcp

import (
	"net"
	"sync"
	"time"

	applicationclock "narumq/internal/application/ports/clock"
	portsmetrics "narumq/internal/application/ports/metrics"
)

// MaxListenerAdmissionLimit bounds both per-listener admission settings.
const MaxListenerAdmissionLimit = 1_000_000

// ListenerAdmissionConfig limits new sockets for one listener. Zero disables a limit.
type ListenerAdmissionConfig struct {
	Transport      string
	MaxConnections int
	ConnectionRate int
	Clock          applicationclock.Clock
	Recorder       portsmetrics.ListenerAdmissionRecorder
}

// NewAdmissionListener closes sockets above the limits immediately after accept.
func NewAdmissionListener(inner net.Listener, config ListenerAdmissionConfig) (net.Listener, error) {
	if inner == nil {
		return nil, ErrNilListener
	}
	if config.Clock == nil {
		return nil, ErrNilClock
	}
	if (config.Transport != portsmetrics.TransportTCP && config.Transport != portsmetrics.TransportTLS &&
		config.Transport != portsmetrics.TransportWS && config.Transport != portsmetrics.TransportWSS) ||
		config.MaxConnections < 0 || config.MaxConnections > MaxListenerAdmissionLimit ||
		config.ConnectionRate < 0 || config.ConnectionRate > MaxListenerAdmissionLimit ||
		(config.MaxConnections == 0 && config.ConnectionRate == 0) {
		return nil, ErrInvalidConfig
	}

	return &admissionListener{
		Listener: inner,
		config:   config,
		tokens:   float64(config.ConnectionRate),
		refilled: config.Clock.Now(),
	}, nil
}

type admissionListener struct {
	net.Listener
	config   ListenerAdmissionConfig
	mu       sync.Mutex
	active   int
	tokens   float64
	refilled time.Time
}

func (listener *admissionListener) Accept() (net.Conn, error) {
	for {
		socket, err := listener.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if reason, ok := listener.admit(); ok {
			return &admittedConn{Conn: socket, listener: listener}, nil
		} else {
			_ = socket.Close()
			if listener.config.Recorder != nil {
				listener.config.Recorder.ListenerAdmissionRejected(listener.config.Transport, reason)
			}
		}
	}
}

func (listener *admissionListener) admit() (string, bool) {
	listener.mu.Lock()
	defer listener.mu.Unlock()

	if listener.config.MaxConnections > 0 && listener.active >= listener.config.MaxConnections {
		return portsmetrics.ListenerAdmissionReasonMaxConnections, false
	}

	if listener.config.ConnectionRate > 0 {
		now := listener.config.Clock.Now()
		if elapsed := now.Sub(listener.refilled); elapsed > 0 {
			listener.tokens += elapsed.Seconds() * float64(listener.config.ConnectionRate)
			if listener.tokens > float64(listener.config.ConnectionRate) {
				listener.tokens = float64(listener.config.ConnectionRate)
			}
			listener.refilled = now
		}
		if listener.tokens < 1 {
			return portsmetrics.ListenerAdmissionReasonConnectionRate, false
		}
		listener.tokens--
	}

	listener.active++
	return "", true
}

type admittedConn struct {
	net.Conn
	listener    *admissionListener
	releaseOnce sync.Once
}

func (connection *admittedConn) Close() error {
	err := connection.Conn.Close()
	connection.releaseOnce.Do(func() {
		connection.listener.mu.Lock()
		connection.listener.active--
		connection.listener.mu.Unlock()
	})
	return err
}
