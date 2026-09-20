package tls

import (
	"crypto/tls"
	"errors"
	"net"
)

// ErrNilConfig is returned when attempting to listen with a nil TLS configuration.
var ErrNilConfig = errors.New("tls config is nil")

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

// ClientIdentity captures authenticated X.509 client identity fields.
type ClientIdentity struct {
	CommonName   string
	DNSNames     []string
	Emails       []string
	SerialNumber string
}

type tlsConnectionState interface {
	ConnectionState() tls.ConnectionState
}

// ExtractClientIdentity extracts client identity fields from an active TLS connection.
// If the connection is not a TLS socket or contains no peer certificates, it returns nil, false.
func ExtractClientIdentity(conn net.Conn) (*ClientIdentity, bool) {
	tlsConn, ok := conn.(tlsConnectionState)
	if !ok {
		return nil, false
	}
	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil, false
	}
	cert := state.PeerCertificates[0]
	return &ClientIdentity{
		CommonName:   cert.Subject.CommonName,
		DNSNames:     cert.DNSNames,
		Emails:       cert.EmailAddresses,
		SerialNumber: cert.SerialNumber.String(),
	}, true
}

// VerifyClientIdentity checks if the client ID matches any of the authenticated identity attributes
// (CommonName, DNS SANs, or Email SANs).
func VerifyClientIdentity(identity *ClientIdentity, clientID string) bool {
	if identity == nil {
		return false
	}
	if identity.CommonName == clientID {
		return true
	}
	for _, dns := range identity.DNSNames {
		if dns == clientID {
			return true
		}
	}
	for _, email := range identity.Emails {
		if email == clientID {
			return true
		}
	}
	return false
}

// VerifyConnClientIdentity verifies that if the connection has an mTLS peer certificate,
// the clientID matches its identity attributes. If no peer certificate was presented,
// it returns true.
func VerifyConnClientIdentity(conn net.Conn, clientID string) bool {
	identity, ok := ExtractClientIdentity(conn)
	if !ok {
		return true
	}
	return VerifyClientIdentity(identity, clientID)
}
