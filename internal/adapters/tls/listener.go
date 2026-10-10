package tls

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"net"

	applicationauthentication "narumq/internal/application/ports/authentication"
)

// ErrNilConfig is returned when attempting to listen with a nil TLS configuration.
var ErrNilConfig = errors.New("tls config is nil")

// ErrNilListener is returned when the inner network listener is nil.
var ErrNilListener = errors.New("tls inner listener is nil")

type tlsConnectionState interface {
	ConnectionState() tls.ConnectionState
}

// NewListener wraps an already bound listener with TLS; handshakes run on first I/O.
func NewListener(inner net.Listener, tlsConfig *tls.Config) (net.Listener, error) {
	if tlsConfig == nil {
		return nil, ErrNilConfig
	}
	if inner == nil {
		return nil, ErrNilListener
	}
	return tls.NewListener(inner, tlsConfig), nil
}

// ExtractVerifiedClientCertificateDTO extracts identity fields from a verified TLS client certificate.
// It returns false for non-TLS sockets and connections without a verified certificate chain.
func ExtractVerifiedClientCertificateDTO(conn net.Conn) (applicationauthentication.ClientCertificateDTO, bool) {
	tlsConn, ok := conn.(tlsConnectionState)
	if !ok {
		return applicationauthentication.ClientCertificateDTO{}, false
	}
	state := tlsConn.ConnectionState()
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 || chain[0] == nil {
			continue
		}
		certificate := chain[0]
		sum := sha256.Sum256(certificate.Raw)
		return applicationauthentication.ClientCertificateDTO{
			CommonName:     certificate.Subject.CommonName,
			SHA256:         hex.EncodeToString(sum[:]),
			DNSNames:       append([]string(nil), certificate.DNSNames...),
			EmailAddresses: append([]string(nil), certificate.EmailAddresses...),
			NotAfter:       certificate.NotAfter,
		}, true
	}
	return applicationauthentication.ClientCertificateDTO{}, false
}
