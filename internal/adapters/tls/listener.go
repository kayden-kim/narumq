package tls

import (
	"crypto/tls"
	"errors"
	"net"

	applicationauthentication "narumq/internal/application/ports/authentication"
)

// ErrNilConfig is returned when attempting to listen with a nil TLS configuration.
var ErrNilConfig = errors.New("tls config is nil")

type tlsConnectionState interface {
	ConnectionState() tls.ConnectionState
}

// Listen creates a TLS listener wrapping a standard network listener.
func Listen(network, address string, tlsConfig *tls.Config) (net.Listener, error) {
	if tlsConfig == nil {
		return nil, ErrNilConfig
	}
	inner, err := net.Listen(network, address)
	if err != nil {
		return nil, err
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
		return applicationauthentication.ClientCertificateDTO{
			CommonName:     certificate.Subject.CommonName,
			DNSNames:       append([]string(nil), certificate.DNSNames...),
			EmailAddresses: append([]string(nil), certificate.EmailAddresses...),
			NotAfter:       certificate.NotAfter,
		}, true
	}
	return applicationauthentication.ClientCertificateDTO{}, false
}
