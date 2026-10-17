package websocket

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	portsmetrics "narumq/internal/application/ports/metrics"
)

type listenerAcceptResult struct {
	connection net.Conn
	err        error
}

func testConfig() Config {
	return Config{
		HandshakeTimeout: HandshakeTimeout,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func newTestListener(t *testing.T) net.Listener {
	t.Helper()
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := NewListener(inner, testConfig())
	if err != nil {
		_ = inner.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-01.
// Scenarios: mqtt-protocol-and-session.md:MQTT-10-SCN-01
func TestNewListenerValidatesConfig(t *testing.T) {
	if listener, err := NewListener(nil, Config{}); !errors.Is(err, ErrNilListener) || listener != nil {
		t.Fatalf("NewListener(nil, Config{}) = (%v, %v), want (nil, ErrNilListener)", listener, err)
	}

	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()

	for _, test := range []struct {
		name   string
		config Config
	}{
		{name: "non-positive handshake timeout", config: Config{Logger: testConfig().Logger}},
		{name: "nil logger", config: Config{HandshakeTimeout: HandshakeTimeout}},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := NewListener(inner, test.config)
			if !errors.Is(err, ErrInvalidConfig) || listener != nil {
				t.Fatalf("NewListener() = (%v, %v), want (nil, ErrInvalidConfig)", listener, err)
			}
		})
	}
}

func startListenerAccept(listener net.Listener) <-chan listenerAcceptResult {
	results := make(chan listenerAcceptResult, 1)
	go func() {
		connection, err := listener.Accept()
		results <- listenerAcceptResult{connection: connection, err: err}
	}()
	return results
}

func waitListenerAccept(t *testing.T, results <-chan listenerAcceptResult) net.Conn {
	t.Helper()
	select {
	case result := <-results:
		if result.err != nil {
			t.Fatalf("Accept() error = %v", result.err)
		}
		return result.connection
	case <-time.After(2 * time.Second):
		t.Fatal("Accept() did not return an upgraded connection")
		return nil
	}
}

func dialTestWebSocket(t *testing.T, address, path string, subprotocols []string, client *http.Client) (*coderws.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return coderws.Dial(ctx, "ws://"+address+path, &coderws.DialOptions{
		HTTPClient:   client,
		Subprotocols: subprotocols,
	})
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-01.
// Scenarios: mqtt-protocol-and-session.md:MQTT-10-SCN-01
func TestListenerStreamsBinaryMessagesAsByteStream(t *testing.T) {
	listener := newTestListener(t)
	accepted := startListenerAccept(listener)
	client, _, err := dialTestWebSocket(t, listener.Addr().String(), Path, []string{Subprotocol}, nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer client.CloseNow()
	if client.Subprotocol() != Subprotocol {
		t.Fatalf("subprotocol = %q, want %q", client.Subprotocol(), Subprotocol)
	}

	connection := waitListenerAccept(t, accepted)
	defer connection.Close()
	for _, payload := range [][]byte{[]byte("MQ"), []byte("TT/"), []byte("one" + "two")} {
		if err := client.Write(context.Background(), coderws.MessageBinary, payload); err != nil {
			t.Fatalf("write binary message %q: %v", payload, err)
		}
	}
	stream := make([]byte, len("MQTT/onetwo"))
	if _, err := io.ReadFull(connection, stream); err != nil {
		t.Fatalf("read binary stream: %v", err)
	}
	if got, want := string(stream), "MQTT/onetwo"; got != want {
		t.Fatalf("stream = %q, want %q", got, want)
	}

	for _, want := range []string{"server-", "stream"} {
		if _, err := connection.Write([]byte(want)); err != nil {
			t.Fatalf("write stream response %q: %v", want, err)
		}
		messageType, response, err := client.Read(context.Background())
		if err != nil {
			t.Fatalf("read response message %q: %v", want, err)
		}
		if messageType != coderws.MessageBinary || string(response) != want {
			t.Fatalf("response = (%v, %q), want binary %q", messageType, response, want)
		}
	}
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-01.
// Scenarios: mqtt-protocol-and-session.md:MQTT-10-SCN-01
func TestListenerRejectsWrongPathAndMissingSubprotocol(t *testing.T) {
	listener := newTestListener(t)
	accepted := startListenerAccept(listener)

	connection, response, err := dialTestWebSocket(t, listener.Addr().String(), "/other", []string{Subprotocol}, nil)
	if connection != nil {
		_ = connection.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusNotFound {
		t.Fatalf("wrong-path Dial() = (%v, %#v, %v), want HTTP 404", connection, response, err)
	}

	client, response, err := dialTestWebSocket(t, listener.Addr().String(), Path, []string{"other"}, nil)
	if err != nil {
		t.Fatalf("missing-subprotocol Dial() error = %v", err)
	}
	defer client.CloseNow()
	if response == nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("missing-subprotocol response = %#v, want HTTP 101", response)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err = client.Read(ctx)
	if status := coderws.CloseStatus(err); status != coderws.StatusProtocolError {
		t.Fatalf("missing-subprotocol close status = %v, want %v (error %v)", status, coderws.StatusProtocolError, err)
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case result := <-accepted:
		if result.connection != nil || !errors.Is(result.err, net.ErrClosed) {
			t.Fatalf("Accept() = (%v, %v), want (nil, net.ErrClosed)", result.connection, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Accept() remained blocked after Close()")
	}
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-01.
// Scenarios: mqtt-protocol-and-session.md:MQTT-10-SCN-01
func TestListenerRejectsTextFrame(t *testing.T) {
	listener := newTestListener(t)
	accepted := startListenerAccept(listener)
	client, _, err := dialTestWebSocket(t, listener.Addr().String(), Path, []string{Subprotocol}, nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer client.CloseNow()
	connection := waitListenerAccept(t, accepted)
	defer connection.Close()

	if err := client.Write(context.Background(), coderws.MessageText, []byte("not MQTT")); err != nil {
		t.Fatalf("write text frame: %v", err)
	}
	if _, err := connection.Read(make([]byte, 16)); err == nil {
		t.Fatal("Read() accepted a text frame")
	}
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-03.
// Scenarios: mqtt-protocol-and-session.md:MQTT-10-SCN-03
func TestListenerExposesRemoteIPAndTransport(t *testing.T) {
	listener := newTestListener(t)
	accepted := startListenerAccept(listener)
	client, _, err := dialTestWebSocket(t, listener.Addr().String(), Path, []string{Subprotocol}, nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer client.CloseNow()
	connection := waitListenerAccept(t, accepted)
	defer connection.Close()

	remote, ok := connection.RemoteAddr().(*net.TCPAddr)
	if !ok || !remote.IP.Equal(net.ParseIP("127.0.0.1")) {
		t.Fatalf("RemoteAddr() = %v, want loopback TCP address", connection.RemoteAddr())
	}
	labeler, ok := connection.(interface{ MetricsTransport() string })
	if !ok || labeler.MetricsTransport() != portsmetrics.TransportWS {
		t.Fatalf("transport label = %v, want %q", labeler, portsmetrics.TransportWS)
	}
	stateProvider, ok := connection.(interface{ ConnectionState() tls.ConnectionState })
	if !ok {
		t.Fatal("WS connection does not expose TLS state")
	}
	state := stateProvider.ConnectionState()
	if state.HandshakeComplete || state.Version != 0 || state.NegotiatedProtocol != "" || len(state.VerifiedChains) != 0 {
		t.Fatalf("ConnectionState() = %+v, want zero value", state)
	}
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-04.
// Scenarios: mqtt-protocol-and-session.md:MQTT-10-SCN-04
func TestListenerWSSExposesVerifiedClientCertificate(t *testing.T) {
	_, root, rootKey := makeWebSocketTestCertificate(t, "test root", nil, nil, true, nil, false)
	serverPair, _, _ := makeWebSocketTestCertificate(t, "websocket server", root, rootKey, false, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, true)
	clientPair, _, _ := makeWebSocketTestCertificate(t, "mqtt-client", root, rootKey, false, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, false)

	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(root)
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsListener := tls.NewListener(inner, &tls.Config{
		Certificates: []tls.Certificate{serverPair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
		MinVersion:   tls.VersionTLS12,
	})
	listener, err := NewListener(tlsListener, testConfig())
	if err != nil {
		_ = tlsListener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := startListenerAccept(listener)

	serverRoots := x509.NewCertPool()
	serverRoots.AddCert(root)
	httpClient := &http.Client{Transport: &http.Transport{
		Proxy: nil,
		TLSClientConfig: &tls.Config{
			RootCAs:      serverRoots,
			Certificates: []tls.Certificate{clientPair},
			MinVersion:   tls.VersionTLS12,
		},
	}}
	url := "wss://" + listener.Addr().String() + Path
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(ctx, url, &coderws.DialOptions{
		HTTPClient:   httpClient,
		Subprotocols: []string{Subprotocol},
	})
	if err != nil {
		t.Fatalf("WSS Dial() error = %v", err)
	}
	defer client.CloseNow()
	connection := waitListenerAccept(t, accepted)
	defer connection.Close()

	stateProvider, ok := connection.(interface{ ConnectionState() tls.ConnectionState })
	if !ok {
		t.Fatal("WSS connection does not expose TLS state")
	}
	state := stateProvider.ConnectionState()
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		t.Fatal("WSS TLS state has no verified client certificate")
	}
	if got := state.VerifiedChains[0][0].Subject.CommonName; got != "mqtt-client" {
		t.Fatalf("verified client CommonName = %q, want mqtt-client", got)
	}
	labeler, ok := connection.(interface{ MetricsTransport() string })
	if !ok || labeler.MetricsTransport() != portsmetrics.TransportWSS {
		t.Fatalf("transport label = %v, want %q", labeler, portsmetrics.TransportWSS)
	}
	_ = connection.Close()
	_ = client.CloseNow()

	acceptedWithoutCertificate := startListenerAccept(listener)
	noCertificateClient := &http.Client{Transport: &http.Transport{
		Proxy: nil,
		TLSClientConfig: &tls.Config{
			RootCAs:    serverRoots,
			MinVersion: tls.VersionTLS12,
		},
	}}
	noCertificateContext, cancelNoCertificate := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelNoCertificate()
	noCertificateConnection, _, err := coderws.Dial(noCertificateContext, url, &coderws.DialOptions{
		HTTPClient:   noCertificateClient,
		Subprotocols: []string{Subprotocol},
	})
	if noCertificateConnection != nil {
		_ = noCertificateConnection.CloseNow()
	}
	if err == nil {
		t.Fatal("WSS Dial() succeeded without a required client certificate")
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case result := <-acceptedWithoutCertificate:
		if result.connection != nil || !errors.Is(result.err, net.ErrClosed) {
			t.Fatalf("Accept() = (%v, %v), want (nil, net.ErrClosed)", result.connection, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Accept() remained blocked after certificate rejection and Close()")
	}
}

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-02.
// Scenarios: mqtt-protocol-and-session.md:MQTT-10-SCN-02
func TestListenerHandshakeTimeout(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "HTTP upgrade"
		if secure {
			name = "WSS TLS handshake"
		}
		t.Run(name, func(t *testing.T) {
			rawListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			var inner net.Listener = rawListener
			if secure {
				inner = tls.NewListener(rawListener, &tls.Config{MinVersion: tls.VersionTLS12})
			}
			config := testConfig()
			config.HandshakeTimeout = 50 * time.Millisecond
			websocketListener, err := NewListener(inner, config)
			if err != nil {
				_ = inner.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = websocketListener.Close() })
			accepted := startListenerAccept(websocketListener)

			client, err := net.Dial("tcp", websocketListener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			_, err = io.Copy(io.Discard, client)
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatalf("handshake did not time out: %v", err)
			}
			if elapsed := time.Since(started); elapsed > 2*time.Second {
				t.Fatalf("handshake timeout took %s, want at most two seconds", elapsed)
			}

			if err := websocketListener.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			select {
			case result := <-accepted:
				if result.connection != nil || !errors.Is(result.err, net.ErrClosed) {
					t.Fatalf("Accept() = (%v, %v), want (nil, net.ErrClosed)", result.connection, result.err)
				}
			case <-time.After(time.Second):
				t.Fatal("Accept() remained blocked after Close()")
			}
		})
	}
}

type closeTestAddr struct{}

func (closeTestAddr) Network() string { return "test" }
func (closeTestAddr) String() string  { return "websocket-test" }

type blockingTestListener struct {
	closed     chan struct{}
	accepting  chan struct{}
	acceptOnce sync.Once
	once       sync.Once
}

func newBlockingTestListener() *blockingTestListener {
	return &blockingTestListener{closed: make(chan struct{}), accepting: make(chan struct{})}
}

func (listener *blockingTestListener) Accept() (net.Conn, error) {
	listener.acceptOnce.Do(func() { close(listener.accepting) })
	<-listener.closed
	return nil, net.ErrClosed
}

func (listener *blockingTestListener) Close() error {
	listener.once.Do(func() { close(listener.closed) })
	return nil
}

func (*blockingTestListener) Addr() net.Addr { return closeTestAddr{} }

// Traceability: docs/specs/mqtt-protocol-and-session.md §1, §8.3 MQTT-10-SCN-02.
// Scenarios: mqtt-protocol-and-session.md:MQTT-10-SCN-02
func TestListenerCloseUnblocksAcceptAndClosesInnerListener(t *testing.T) {
	inner := newBlockingTestListener()
	listener, err := NewListener(inner, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-inner.accepting:
		t.Fatal("NewListener started the HTTP server before Accept()")
	default:
	}
	accepted := startListenerAccept(listener)
	select {
	case <-inner.accepting:
	case <-time.After(time.Second):
		t.Fatal("HTTP server did not start accepting")
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-inner.closed:
	case <-time.After(time.Second):
		t.Fatal("Close() did not close the inner listener")
	}
	select {
	case result := <-accepted:
		if result.connection != nil || !errors.Is(result.err, net.ErrClosed) {
			t.Fatalf("Accept() = (%v, %v), want (nil, net.ErrClosed)", result.connection, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Accept() remained blocked after Close()")
	}
}

func makeWebSocketTestCertificate(
	t *testing.T,
	commonName string,
	parent *x509.Certificate,
	parentKey *ecdsa.PrivateKey,
	isCA bool,
	usages []x509.ExtKeyUsage,
	serverIP bool,
) (tls.Certificate, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate %s key: %v", commonName, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate %s serial: %v", commonName, err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
	}
	if isCA {
		template.IsCA = true
		template.BasicConstraintsValid = true
		template.KeyUsage |= x509.KeyUsageCertSign
	}
	if serverIP {
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	parentCertificate, signingKey := parent, parentKey
	if isCA {
		parentCertificate, signingKey = template, key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parentCertificate, &key.PublicKey, signingKey)
	if err != nil {
		t.Fatalf("create %s certificate: %v", commonName, err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse %s certificate: %v", commonName, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal %s key: %v", commonName, err)
	}
	pair, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatalf("load %s certificate: %v", commonName, err)
	}
	pair.Leaf = parsed
	return pair, parsed, key
}
