package tls

import (
	"crypto/tls"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildTLSConfig_MissingCertificate(t *testing.T) {
	tempDir := t.TempDir()
	ca := generateCA(t, "Root CA")
	server := generateServerCert(t, ca, "localhost", []string{"localhost", "127.0.0.1"})

	certPath := writeTempFile(t, tempDir, "server.crt", server.certPEM)
	keyPath := writeTempFile(t, tempDir, "server.key", server.keyPEM)

	tests := []struct {
		name     string
		certFile string
		keyFile  string
		wantErr  error
	}{
		{name: "empty cert file", certFile: "", keyFile: keyPath, wantErr: ErrMissingCertificate},
		{name: "empty key file", certFile: certPath, keyFile: "", wantErr: ErrMissingCertificate},
		{name: "both empty", certFile: "", keyFile: "", wantErr: ErrMissingCertificate},
		{name: "nonexistent cert file", certFile: filepath.Join(tempDir, "nonexistent.crt"), keyFile: keyPath, wantErr: os.ErrNotExist},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				CertFile: tc.certFile,
				KeyFile:  tc.keyFile,
			}
			_, err := BuildTLSConfig(cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error wrapping %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestBuildTLSConfig_InsecureTLSVersion(t *testing.T) {
	tempDir := t.TempDir()
	ca := generateCA(t, "Root CA")
	server := generateServerCert(t, ca, "localhost", []string{"localhost"})
	certPath := writeTempFile(t, tempDir, "server.crt", server.certPEM)
	keyPath := writeTempFile(t, tempDir, "server.key", server.keyPEM)

	insecureVersions := []string{"1.0", "1.1", "ssl3.0", "tls1.0", "2.0"}
	for _, ver := range insecureVersions {
		t.Run("reject-"+ver, func(t *testing.T) {
			cfg := Config{
				CertFile:   certPath,
				KeyFile:    keyPath,
				MinVersion: ver,
			}
			_, err := BuildTLSConfig(cfg)
			if !errors.Is(err, ErrInsecureTLSVersion) {
				t.Fatalf("expected ErrInsecureTLSVersion for %q, got %v", ver, err)
			}
		})
	}
}

func TestBuildTLSConfig_ValidTLSVersions(t *testing.T) {
	tempDir := t.TempDir()
	ca := generateCA(t, "Root CA")
	server := generateServerCert(t, ca, "localhost", []string{"localhost"})
	certPath := writeTempFile(t, tempDir, "server.crt", server.certPEM)
	keyPath := writeTempFile(t, tempDir, "server.key", server.keyPEM)

	tests := []struct {
		name        string
		version     string
		wantVersion uint16
	}{
		{name: "default", version: "", wantVersion: tls.VersionTLS12},
		{name: "1.2", version: "1.2", wantVersion: tls.VersionTLS12},
		{name: "1.3", version: "1.3", wantVersion: tls.VersionTLS13},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				CertFile:   certPath,
				KeyFile:    keyPath,
				MinVersion: tc.version,
			}
			tlsConfig, err := BuildTLSConfig(cfg)
			if err != nil {
				t.Fatalf("BuildTLSConfig() error = %v", err)
			}
			if tlsConfig.MinVersion != tc.wantVersion {
				t.Fatalf("expected MinVersion = 0x%04x, got 0x%04x", tc.wantVersion, tlsConfig.MinVersion)
			}
		})
	}
}

func TestBuildTLSConfig_CipherSuites(t *testing.T) {
	tempDir := t.TempDir()
	ca := generateCA(t, "Root CA")
	server := generateServerCert(t, ca, "localhost", []string{"localhost"})
	certPath := writeTempFile(t, tempDir, "server.crt", server.certPEM)
	keyPath := writeTempFile(t, tempDir, "server.key", server.keyPEM)

	t.Run("default cipher suites are AEAD only", func(t *testing.T) {
		cfg := Config{
			CertFile: certPath,
			KeyFile:  keyPath,
		}
		tlsConfig, err := BuildTLSConfig(cfg)
		if err != nil {
			t.Fatalf("BuildTLSConfig() error = %v", err)
		}
		if len(tlsConfig.CipherSuites) != len(defaultCipherSuites) {
			t.Fatalf("expected %d default cipher suites, got %d", len(defaultCipherSuites), len(tlsConfig.CipherSuites))
		}
	})

	t.Run("valid custom cipher suites", func(t *testing.T) {
		cfg := Config{
			CertFile: certPath,
			KeyFile:  keyPath,
			CipherSuites: []string{
				"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
				"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
			},
		}
		tlsConfig, err := BuildTLSConfig(cfg)
		if err != nil {
			t.Fatalf("BuildTLSConfig() error = %v", err)
		}
		if len(tlsConfig.CipherSuites) != 2 {
			t.Fatalf("expected 2 cipher suites, got %d", len(tlsConfig.CipherSuites))
		}
		if tlsConfig.CipherSuites[0] != tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 {
			t.Errorf("unexpected cipher suite 0: 0x%04x", tlsConfig.CipherSuites[0])
		}
	})

	insecureSuites := []string{
		"TLS_RSA_WITH_RC4_128_SHA",
		"TLS_RSA_WITH_3DES_EDE_CBC_SHA",
		"TLS_RSA_WITH_AES_128_CBC_SHA",
		"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
		"UNKNOWN_CIPHER_SUITE",
	}
	for _, suite := range insecureSuites {
		t.Run("reject-"+suite, func(t *testing.T) {
			cfg := Config{
				CertFile:     certPath,
				KeyFile:      keyPath,
				CipherSuites: []string{suite},
			}
			_, err := BuildTLSConfig(cfg)
			if !errors.Is(err, ErrInsecureCipherSuite) {
				t.Fatalf("expected ErrInsecureCipherSuite for %q, got %v", suite, err)
			}
		})
	}
}

func TestBuildTLSConfig_ClientAuth(t *testing.T) {
	tempDir := t.TempDir()
	ca := generateCA(t, "Root CA")
	server := generateServerCert(t, ca, "localhost", []string{"localhost"})
	certPath := writeTempFile(t, tempDir, "server.crt", server.certPEM)
	keyPath := writeTempFile(t, tempDir, "server.key", server.keyPEM)
	caPath := writeTempFile(t, tempDir, "ca.crt", ca.certPEM)

	t.Run("none mode", func(t *testing.T) {
		cfg := Config{
			CertFile:   certPath,
			KeyFile:    keyPath,
			ClientAuth: ClientAuthNone,
		}
		tlsConfig, err := BuildTLSConfig(cfg)
		if err != nil {
			t.Fatalf("BuildTLSConfig() error = %v", err)
		}
		if tlsConfig.ClientAuth != tls.NoClientCert {
			t.Fatalf("expected NoClientCert, got %v", tlsConfig.ClientAuth)
		}
		if tlsConfig.ClientCAs != nil {
			t.Fatalf("expected nil ClientCAs, got %v", tlsConfig.ClientCAs)
		}
	})

	t.Run("request mode with CA", func(t *testing.T) {
		cfg := Config{
			CertFile:   certPath,
			KeyFile:    keyPath,
			CAFile:     caPath,
			ClientAuth: ClientAuthRequest,
		}
		tlsConfig, err := BuildTLSConfig(cfg)
		if err != nil {
			t.Fatalf("BuildTLSConfig() error = %v", err)
		}
		if tlsConfig.ClientAuth != tls.VerifyClientCertIfGiven {
			t.Fatalf("expected VerifyClientCertIfGiven, got %v", tlsConfig.ClientAuth)
		}
		if tlsConfig.ClientCAs == nil {
			t.Fatal("expected non-nil ClientCAs")
		}
	})

	t.Run("require mode with CA", func(t *testing.T) {
		cfg := Config{
			CertFile:   certPath,
			KeyFile:    keyPath,
			CAFile:     caPath,
			ClientAuth: ClientAuthRequire,
		}
		tlsConfig, err := BuildTLSConfig(cfg)
		if err != nil {
			t.Fatalf("BuildTLSConfig() error = %v", err)
		}
		if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
			t.Fatalf("expected RequireAndVerifyClientCert, got %v", tlsConfig.ClientAuth)
		}
		if tlsConfig.ClientCAs == nil {
			t.Fatal("expected non-nil ClientCAs")
		}
	})

	t.Run("invalid client auth mode", func(t *testing.T) {
		cfg := Config{
			CertFile:   certPath,
			KeyFile:    keyPath,
			ClientAuth: "invalid-mode",
		}
		_, err := BuildTLSConfig(cfg)
		if !errors.Is(err, ErrInvalidClientAuth) {
			t.Fatalf("expected ErrInvalidClientAuth, got %v", err)
		}
	})

	t.Run("request mode missing CA file", func(t *testing.T) {
		cfg := Config{
			CertFile:   certPath,
			KeyFile:    keyPath,
			ClientAuth: ClientAuthRequest,
		}
		_, err := BuildTLSConfig(cfg)
		if !errors.Is(err, ErrMissingClientCA) {
			t.Fatalf("expected ErrMissingClientCA, got %v", err)
		}
	})

	t.Run("require mode missing CA file", func(t *testing.T) {
		cfg := Config{
			CertFile:   certPath,
			KeyFile:    keyPath,
			ClientAuth: ClientAuthRequire,
		}
		_, err := BuildTLSConfig(cfg)
		if !errors.Is(err, ErrMissingClientCA) {
			t.Fatalf("expected ErrMissingClientCA, got %v", err)
		}
	})

	t.Run("nonexistent CA file", func(t *testing.T) {
		cfg := Config{
			CertFile:   certPath,
			KeyFile:    keyPath,
			CAFile:     filepath.Join(tempDir, "nonexistent-ca.crt"),
			ClientAuth: ClientAuthRequire,
		}
		_, err := BuildTLSConfig(cfg)
		if err == nil {
			t.Fatal("expected error for nonexistent CA file, got nil")
		}
	})

	t.Run("invalid CA file content", func(t *testing.T) {
		invalidCAPath := writeTempFile(t, tempDir, "bad-ca.crt", []byte("NOT A VALID CERTIFICATE PEM"))
		cfg := Config{
			CertFile:   certPath,
			KeyFile:    keyPath,
			CAFile:     invalidCAPath,
			ClientAuth: ClientAuthRequire,
		}
		_, err := BuildTLSConfig(cfg)
		if !errors.Is(err, ErrInvalidClientCA) {
			t.Fatalf("expected ErrInvalidClientCA, got %v", err)
		}
	})
}
