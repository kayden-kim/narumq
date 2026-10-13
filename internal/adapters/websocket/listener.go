package websocket

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	coderws "github.com/coder/websocket"
)

const (
	// Path is the only HTTP path accepted for MQTT WebSocket connections.
	Path = "/mqtt"
	// Subprotocol is the required WebSocket subprotocol for MQTT connections.
	Subprotocol = "mqtt"
	// HandshakeTimeout bounds TLS and HTTP upgrade handshakes.
	HandshakeTimeout = 10 * time.Second
	maxHeaderBytes   = 8 << 10
)

var (
	// ErrNilListener is returned when the listener has no underlying socket source.
	ErrNilListener = errors.New("websocket listener: nil inner listener")
	// ErrInvalidConfig is returned when the handshake timeout or logger is invalid.
	ErrInvalidConfig = errors.New("websocket listener: invalid config")
)

// Config controls WebSocket handshake limits and HTTP server logging.
type Config struct {
	HandshakeTimeout time.Duration
	Logger           *slog.Logger
}

// NewListener exposes HTTP-upgraded MQTT WebSocket connections as net.Conn values.
// It starts the HTTP server when Accept is called.
func NewListener(inner net.Listener, config Config) (net.Listener, error) {
	if inner == nil {
		return nil, ErrNilListener
	}
	if config.HandshakeTimeout <= 0 || config.Logger == nil {
		return nil, ErrInvalidConfig
	}

	listener := &listener{
		inner:       inner,
		config:      config,
		accepted:    make(chan net.Conn),
		serveFailed: make(chan error, 1),
		done:        make(chan struct{}),
		serveDone:   make(chan struct{}),
	}
	listener.server = &http.Server{
		Handler:           listener,
		ReadHeaderTimeout: config.HandshakeTimeout,
		ReadTimeout:       config.HandshakeTimeout,
		WriteTimeout:      config.HandshakeTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
		TLSNextProto:      map[string]func(*http.Server, *tls.Conn, http.Handler){},
		ErrorLog:          slog.NewLogLogger(config.Logger.Handler(), slog.LevelWarn),
	}
	return listener, nil
}

type listener struct {
	inner       net.Listener
	config      Config
	server      *http.Server
	accepted    chan net.Conn
	serveFailed chan error
	done        chan struct{}
	serveDone   chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
	mu          sync.RWMutex
	started     bool
	closed      bool
	closeErr    error
}

func (listener *listener) Accept() (net.Conn, error) {
	listener.mu.RLock()
	closed := listener.closed
	listener.mu.RUnlock()
	if closed {
		return nil, net.ErrClosed
	}
	listener.start()

	select {
	case connection := <-listener.accepted:
		return connection, nil
	case err := <-listener.serveFailed:
		return nil, err
	case <-listener.done:
		select {
		case err := <-listener.serveFailed:
			return nil, err
		default:
			return nil, net.ErrClosed
		}
	}
}

func (listener *listener) start() {
	listener.startOnce.Do(func() {
		listener.mu.Lock()
		if listener.closed {
			listener.mu.Unlock()
			return
		}
		listener.started = true
		listener.mu.Unlock()
		go listener.serve()
	})
}

func (listener *listener) serve() {
	defer close(listener.serveDone)
	if err := listener.server.Serve(listener.inner); err != nil && !errors.Is(err, http.ErrServerClosed) {
		select {
		case listener.serveFailed <- err:
		case <-listener.done:
		}
	}
}

func (listener *listener) Close() error {
	listener.closeOnce.Do(func() {
		listener.mu.Lock()
		listener.closed = true
		close(listener.done)
		listener.mu.Unlock()
		listener.closeErr = listener.server.Close()
		_ = listener.inner.Close()
	})

	listener.mu.RLock()
	started := listener.started
	listener.mu.RUnlock()
	if started {
		<-listener.serveDone
	}
	return listener.closeErr
}

func (listener *listener) Addr() net.Addr {
	return listener.inner.Addr()
}

func (listener *listener) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != Path {
		http.NotFound(writer, request)
		return
	}

	websocketConnection, err := coderws.Accept(writer, request, &coderws.AcceptOptions{
		Subprotocols: []string{Subprotocol},
		// MQTT credentials authenticate clients; browser Origin checks are not the MQTT trust boundary.
		InsecureSkipVerify: true,
		CompressionMode:    coderws.CompressionDisabled,
	})
	if err != nil {
		return
	}
	if !strings.EqualFold(websocketConnection.Subprotocol(), Subprotocol) {
		_ = websocketConnection.Close(coderws.StatusProtocolError, "mqtt subprotocol required")
		return
	}

	connection := newConn(websocketConnection, request.TLS)
	select {
	case listener.accepted <- connection:
	case <-listener.done:
		_ = connection.Close()
	case <-time.After(listener.config.HandshakeTimeout):
		_ = connection.Close()
	}
}

var _ http.Handler = (*listener)(nil)
var _ net.Listener = (*listener)(nil)
