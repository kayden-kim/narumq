// Package authentication defines the application-owned credential verification port.
package authentication

import (
	"context"
	"time"

	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

// Credential carries a copied MQTT CONNECT Password token.
type Credential struct {
	Token []byte
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
