package tls

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type mockTLSConn struct {
	net.Conn
	state tls.ConnectionState
}

func (m mockTLSConn) ConnectionState() tls.ConnectionState {
	return m.state
}

func TestNewListenerValidatesDependencies(t *testing.T) {
	listener, err := NewListener(nil, nil)
	if !errors.Is(err, ErrNilConfig) {
		t.Fatalf("expected ErrNilConfig, got %v", err)
	}
	if listener != nil {
		listener.Close()
	}
	listener, err = NewListener(nil, &tls.Config{MinVersion: tls.VersionTLS12})
	if !errors.Is(err, ErrNilListener) || listener != nil {
		t.Fatalf("expected nil listener and ErrNilListener, got %v, %v", listener, err)
	}
}

// Scenarios: tenancy-and-security.md:SEC-SCN-003
func TestNewListenerSuccessAndHandshake(t *testing.T) {
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

	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen error: %v", err)
	}
	defer inner.Close()
	listener, err := NewListener(inner, tlsConfig)
	if err != nil {
		t.Fatalf("NewListener error: %v", err)
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

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-005, tenancy-and-security.md:SEC-SCN-045
func TestExtractVerifiedClientCertificateDTO(t *testing.T) {
	t.Run("non-tls socket", func(t *testing.T) {
		s1, s2 := net.Pipe()
		defer s1.Close()
		defer s2.Close()
		dto, ok := ExtractVerifiedClientCertificateDTO(s1)
		if ok || dto.CommonName != "" || len(dto.DNSNames) != 0 || len(dto.EmailAddresses) != 0 {
			t.Fatalf("expected empty DTO and false for plain socket, got %#v, %v", dto, ok)
		}
	})

	t.Run("peer certificate without verified chain", func(t *testing.T) {
		cert := &x509.Certificate{DNSNames: []string{"unverified.example"}}
		mock := mockTLSConn{
			state: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{cert},
			},
		}
		dto, ok := ExtractVerifiedClientCertificateDTO(mock)
		if ok || dto.CommonName != "" || len(dto.DNSNames) != 0 || len(dto.EmailAddresses) != 0 {
			t.Fatalf("expected empty DTO and false for unverified peer certificate, got %#v, %v", dto, ok)
		}
	})
	t.Run("verified chain copies identity fields", func(t *testing.T) {

		notAfter := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
		cert := &x509.Certificate{
			Raw:            []byte("verified leaf DER"),
			DNSNames:       []string{"device.narumq.internal"},
			EmailAddresses: []string{"device@narumq.internal"},
			NotAfter:       notAfter,
		}
		cert.Subject.CommonName = "device-01"
		mock := mockTLSConn{
			state: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{cert},
				VerifiedChains:   [][]*x509.Certificate{{cert}},
			},
		}
		dto, ok := ExtractVerifiedClientCertificateDTO(mock)
		if !ok {
			t.Fatal("expected verified client certificate DTO")
		}
		sum := sha256.Sum256(cert.Raw)
		if dto.SHA256 != hex.EncodeToString(sum[:]) {
			t.Fatalf("certificate fingerprint = %q; want lowercase SHA-256 %q", dto.SHA256, hex.EncodeToString(sum[:]))
		}
		if dto.CommonName != "device-01" || dto.NotAfter != notAfter {
			t.Fatalf("identity scalar fields = %#v", dto)
		}
		if len(dto.DNSNames) != 1 || dto.DNSNames[0] != "device.narumq.internal" {
			t.Fatalf("DNS SANs = %v", dto.DNSNames)
		}
		if len(dto.EmailAddresses) != 1 || dto.EmailAddresses[0] != "device@narumq.internal" {
			t.Fatalf("email SANs = %v", dto.EmailAddresses)
		}

		dto.DNSNames[0] = "mutated-dto.example"
		dto.EmailAddresses[0] = "mutated-dto@example"
		if cert.DNSNames[0] != "device.narumq.internal" || cert.EmailAddresses[0] != "device@narumq.internal" {
			t.Fatal("certificate slices alias returned DTO")
		}
		cert.DNSNames[0] = "mutated-source.example"
		cert.EmailAddresses[0] = "mutated-source@example"
		if dto.DNSNames[0] != "mutated-dto.example" || dto.EmailAddresses[0] != "mutated-dto@example" {
			t.Fatal("returned DTO slices alias certificate source")
		}
	})
}
