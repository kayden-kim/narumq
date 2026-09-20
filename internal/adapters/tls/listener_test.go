package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
)

type mockTLSConn struct {
	net.Conn
	state tls.ConnectionState
}

func (m mockTLSConn) ConnectionState() tls.ConnectionState {
	return m.state
}

func TestListen_NilConfig(t *testing.T) {
	listener, err := Listen("tcp", "127.0.0.1:0", nil)
	if !errors.Is(err, ErrNilConfig) {
		t.Fatalf("expected ErrNilConfig, got %v", err)
	}
	if listener != nil {
		listener.Close()
	}
}

func TestListen_InvalidAddress(t *testing.T) {
	tempDir := t.TempDir()
	ca := generateCA(t, "Root CA")
	server := generateServerCert(t, ca, "localhost", []string{"127.0.0.1"})
	certPath := writeTempFile(t, tempDir, "server.crt", server.certPEM)
	keyPath := writeTempFile(t, tempDir, "server.key", server.keyPEM)

	cfg, err := BuildTLSConfig(Config{CertFile: certPath, KeyFile: keyPath})
	if err != nil {
		t.Fatalf("BuildTLSConfig error: %v", err)
	}

	listener, err := Listen("tcp", "999.999.999.999:99999", cfg)
	if err == nil {
		if listener != nil {
			listener.Close()
		}
		t.Fatal("expected listen error for invalid address, got nil")
	}
}

func TestListen_SuccessAndHandshake(t *testing.T) {
	tempDir := t.TempDir()
	ca := generateCA(t, "Root CA")
	server := generateServerCert(t, ca, "localhost", []string{"127.0.0.1"})
	certPath := writeTempFile(t, tempDir, "server.crt", server.certPEM)
	keyPath := writeTempFile(t, tempDir, "server.key", server.keyPEM)

	tlsConfig, err := BuildTLSConfig(Config{
		CertFile: certPath,
		KeyFile:  keyPath,
	})
	if err != nil {
		t.Fatalf("BuildTLSConfig error: %v", err)
	}

	listener, err := Listen("tcp", "127.0.0.1:0", tlsConfig)
	if err != nil {
		t.Fatalf("Listen error: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		buf := make([]byte, 5)
		if _, err := io.ReadFull(conn, buf); err != nil {
			errCh <- err
			return
		}
		if string(buf) != "hello" {
			errCh <- errors.New("unexpected message")
			return
		}
		if _, err := conn.Write([]byte("world")); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(ca.cert)

	clientConn, err := tls.Dial("tcp", addr, &tls.Config{
		RootCAs:    rootCAs,
		ServerName: "127.0.0.1",
	})
	if err != nil {
		t.Fatalf("tls.Dial error: %v", err)
	}
	defer clientConn.Close()

	if _, err := clientConn.Write([]byte("hello")); err != nil {
		t.Fatalf("client write error: %v", err)
	}

	resp := make([]byte, 5)
	if _, err := io.ReadFull(clientConn, resp); err != nil {
		t.Fatalf("client read error: %v", err)
	}
	if string(resp) != "world" {
		t.Fatalf("expected response 'world', got %q", string(resp))
	}

	if err := <-errCh; err != nil {
		t.Fatalf("server handler error: %v", err)
	}
}

func TestExtractClientIdentity(t *testing.T) {
	t.Run("non-tls socket", func(t *testing.T) {
		s1, s2 := net.Pipe()
		defer s1.Close()
		defer s2.Close()
		identity, ok := ExtractClientIdentity(s1)
		if ok || identity != nil {
			t.Fatalf("expected nil, false for plain socket, got %v, %v", identity, ok)
		}
	})

	t.Run("tls socket with no peer certs", func(t *testing.T) {
		mock := mockTLSConn{
			state: tls.ConnectionState{
				PeerCertificates: nil,
			},
		}
		identity, ok := ExtractClientIdentity(mock)
		if ok || identity != nil {
			t.Fatalf("expected nil, false when no peer certs, got %v, %v", identity, ok)
		}
	})

	t.Run("tls socket with peer cert", func(t *testing.T) {
		cert := &x509.Certificate{
			SerialNumber:   big.NewInt(123456789),
			DNSNames:       []string{"device.narumq.internal"},
			EmailAddresses: []string{"device@narumq.internal"},
		}
		cert.Subject.CommonName = "device-01"
		mock := mockTLSConn{
			state: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{cert},
			},
		}
		identity, ok := ExtractClientIdentity(mock)
		if !ok || identity == nil {
			t.Fatalf("expected valid identity, got %v, %v", identity, ok)
		}
		if identity.CommonName != "device-01" {
			t.Errorf("expected CommonName = device-01, got %q", identity.CommonName)
		}
		if len(identity.DNSNames) != 1 || identity.DNSNames[0] != "device.narumq.internal" {
			t.Errorf("unexpected DNSNames: %v", identity.DNSNames)
		}
		if len(identity.Emails) != 1 || identity.Emails[0] != "device@narumq.internal" {
			t.Errorf("unexpected Emails: %v", identity.Emails)
		}
		if identity.SerialNumber != "123456789" {
			t.Errorf("expected SerialNumber = 123456789, got %q", identity.SerialNumber)
		}
	})
}

func TestVerifyClientIdentity(t *testing.T) {
	identity := &ClientIdentity{
		CommonName: "sensor-primary",
		DNSNames:   []string{"sensor.example.com", "alt.example.com"},
		Emails:     []string{"sensor@example.com"},
	}

	tests := []struct {
		name     string
		identity *ClientIdentity
		clientID string
		want     bool
	}{
		{name: "nil identity", identity: nil, clientID: "sensor-primary", want: false},
		{name: "match CN", identity: identity, clientID: "sensor-primary", want: true},
		{name: "match DNS 1", identity: identity, clientID: "sensor.example.com", want: true},
		{name: "match DNS 2", identity: identity, clientID: "alt.example.com", want: true},
		{name: "match Email", identity: identity, clientID: "sensor@example.com", want: true},
		{name: "mismatch", identity: identity, clientID: "unknown-device", want: false},
		{name: "empty clientID", identity: identity, clientID: "", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := VerifyClientIdentity(tc.identity, tc.clientID)
			if got != tc.want {
				t.Fatalf("VerifyClientIdentity(%v, %q) = %v, want %v", tc.identity, tc.clientID, got, tc.want)
			}
		})
	}
}

func TestVerifyConnClientIdentity(t *testing.T) {
	t.Run("non-tls socket passes", func(t *testing.T) {
		s1, s2 := net.Pipe()
		defer s1.Close()
		defer s2.Close()
		if !VerifyConnClientIdentity(s1, "any-client-id") {
			t.Fatal("expected true for plain socket")
		}
	})

	t.Run("tls socket without client cert passes", func(t *testing.T) {
		mock := mockTLSConn{
			state: tls.ConnectionState{
				PeerCertificates: nil,
			},
		}
		if !VerifyConnClientIdentity(mock, "any-client-id") {
			t.Fatal("expected true when no peer certificates present")
		}
	})

	t.Run("tls socket with client cert checks identity", func(t *testing.T) {
		cert := &x509.Certificate{
			DNSNames:       []string{"gateway.local"},
			EmailAddresses: []string{"admin@gateway.local"},
		}
		cert.Subject.CommonName = "gateway-01"
		mock := mockTLSConn{
			state: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{cert},
			},
		}
		if !VerifyConnClientIdentity(mock, "gateway-01") {
			t.Error("expected true for matching CN")
		}
		if !VerifyConnClientIdentity(mock, "gateway.local") {
			t.Error("expected true for matching DNS")
		}
		if !VerifyConnClientIdentity(mock, "admin@gateway.local") {
			t.Error("expected true for matching Email")
		}
		if VerifyConnClientIdentity(mock, "impostor") {
			t.Error("expected false for mismatched client ID")
		}
	})
}
