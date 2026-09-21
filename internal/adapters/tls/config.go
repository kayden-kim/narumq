// Package tls provides TLS and mTLS listener adapters and configuration builders.
package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ClientAuthMode defines the client certificate verification policy.
type ClientAuthMode string

const (
	// ClientAuthNone disables client certificate requests.
	ClientAuthNone ClientAuthMode = "none"
	// ClientAuthRequest requests a client certificate but does not require it.
	ClientAuthRequest ClientAuthMode = "request"
	// ClientAuthRequire requires and verifies a client certificate.
	ClientAuthRequire ClientAuthMode = "require"
)

var (
	// ErrMissingCertificate is returned when the certificate or key file is missing.
	ErrMissingCertificate = errors.New("tls server certificate and private key files are required")
	// ErrMissingClientCA is returned when client authentication is requested without a CA file.
	ErrMissingClientCA = errors.New("tls client CA file is required when client authentication is enabled")
	// ErrInvalidClientCA is returned when the CA file cannot be parsed or contains no valid certificates.
	ErrInvalidClientCA = errors.New("tls client CA file contains no valid certificates")
	// ErrInsecureTLSVersion is returned when a TLS version below 1.2 is configured.
	ErrInsecureTLSVersion = errors.New("insecure TLS version: minimum version must be 1.2 or higher")
	// ErrInsecureCipherSuite is returned when an unapproved or insecure cipher suite is requested.
	ErrInsecureCipherSuite = errors.New("insecure or unapproved TLS cipher suite")
	// ErrInvalidClientAuth is returned when an unrecognized client authentication mode is given.
	ErrInvalidClientAuth = errors.New("invalid tls client auth mode")
)

var approvedCipherSuites = map[string]uint16{
	"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256":       tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":         tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384":       tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":         tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305":        tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	"TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305":          tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256": tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	"TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256":   tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
}

var defaultCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
}

// Config describes the TLS listener configuration options.
type Config struct {
	CertFile     string
	KeyFile      string
	CAFile       string
	ClientAuth   ClientAuthMode
	MinVersion   string
	CipherSuites []string
}

// BuildTLSConfig parses and validates the TLS configuration and returns a standard *tls.Config.
func BuildTLSConfig(config Config) (*tls.Config, error) {
	if config.CertFile == "" || config.KeyFile == "" {
		return nil, ErrMissingCertificate
	}
	cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate key pair: %w", err)
	}

	var minVersion uint16
	switch strings.TrimSpace(config.MinVersion) {
	case "", "1.2":
		minVersion = tls.VersionTLS12
	case "1.3":
		minVersion = tls.VersionTLS13
	default:
		return nil, fmt.Errorf("%w: %q", ErrInsecureTLSVersion, config.MinVersion)
	}

	var clientAuth tls.ClientAuthType
	switch config.ClientAuth {
	case "", ClientAuthNone:
		clientAuth = tls.NoClientCert
	case ClientAuthRequest:
		clientAuth = tls.VerifyClientCertIfGiven
	case ClientAuthRequire:
		clientAuth = tls.RequireAndVerifyClientCert
	default:
		return nil, fmt.Errorf("%w: %q", ErrInvalidClientAuth, config.ClientAuth)
	}

	var clientCAs *x509.CertPool
	if clientAuth == tls.VerifyClientCertIfGiven || clientAuth == tls.RequireAndVerifyClientCert {
		if config.CAFile == "" {
			return nil, ErrMissingClientCA
		}
		caBytes, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read client CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, ErrInvalidClientCA
		}
		clientCAs = pool
	}

	var cipherSuites []uint16
	if len(config.CipherSuites) == 0 {
		cipherSuites = append([]uint16(nil), defaultCipherSuites...)
	} else {
		cipherSuites = make([]uint16, 0, len(config.CipherSuites))
		for _, name := range config.CipherSuites {
			trimmed := strings.TrimSpace(name)
			id, approved := approvedCipherSuites[trimmed]
			if !approved {
				return nil, fmt.Errorf("%w: %q", ErrInsecureCipherSuite, name)
			}
			cipherSuites = append(cipherSuites, id)
		}
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minVersion,
		ClientAuth:   clientAuth,
		ClientCAs:    clientCAs,
		CipherSuites: cipherSuites,
	}, nil
}
