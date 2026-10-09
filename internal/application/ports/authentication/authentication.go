// Package authentication defines the application-owned credential verification port.
package authentication

import (
	"context"
	"time"

	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

// Credential carries the MQTT CONNECT username and copied Password bytes.
type Credential struct {
	Username string
	Password []byte
}

// ClientCertificateDTO carries verified leaf identity fields copied at the adapter boundary.
type ClientCertificateDTO struct {
	CommonName     string
	DNSNames       []string
	EmailAddresses []string
	NotAfter       time.Time
}

// ClientCertificateAuthenticationProvider verifies a client certificate identity binding.
type ClientCertificateAuthenticationProvider interface {
	AuthenticateClientCertificate(ctx context.Context, certificate ClientCertificateDTO) (Result, error)
}

// Result carries verified identity binding data without exposing provider types.
type Result struct {
	TenantID         tenancy.TenantID
	Subject          string
	CredentialID     string
	AllowedClientIDs []string
	Roles            []security.Role
	ExpiresAt        time.Time
}

// AuthenticationProvider verifies one credential and returns trusted binding data.
type AuthenticationProvider interface {
	Authenticate(ctx context.Context, credential Credential) (Result, error)
}
