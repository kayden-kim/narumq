package authentication

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"golang.org/x/crypto/argon2"

	applicationauthentication "narumq/internal/application/ports/authentication"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

const (
	passwordHashTime            uint32 = 2
	passwordHashMemoryKiB       uint32 = 19 * 1024
	passwordHashParallelism     uint8  = 1
	passwordHashSaltLength             = 16
	passwordHashDigestLength           = 32
	maxPasswordLength                  = 65535
	maxConcurrentPasswordHashes        = 4
)

var (
	// ErrInvalidPasswordHash reports a malformed or unsupported Argon2id PHC string.
	ErrInvalidPasswordHash = errors.New("invalid password hash")
	// ErrInvalidPasswordLength reports password input outside the supported byte range.
	ErrInvalidPasswordLength = errors.New("password must contain between 1 and 65535 bytes")
)

// PasswordHash is a validated, opaque Argon2id salt and digest pair.
type PasswordHash struct {
	salt   [passwordHashSaltLength]byte
	digest [passwordHashDigestLength]byte
	valid  bool
}

// ParsePasswordHash validates the sole supported Argon2id PHC profile.
func ParsePasswordHash(encoded string) (PasswordHash, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" || parts[2] != "v=19" || parts[3] != "m=19456,t=2,p=1" {
		return PasswordHash{}, ErrInvalidPasswordHash
	}
	if len(parts[4]) != base64.RawStdEncoding.EncodedLen(passwordHashSaltLength) || len(parts[5]) != base64.RawStdEncoding.EncodedLen(passwordHashDigestLength) {
		return PasswordHash{}, ErrInvalidPasswordHash
	}

	var hash PasswordHash
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) != len(hash.salt) || base64.RawStdEncoding.EncodeToString(salt) != parts[4] {
		return PasswordHash{}, ErrInvalidPasswordHash
	}
	copy(hash.salt[:], salt)
	clear(salt)

	digest, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(digest) != len(hash.digest) || base64.RawStdEncoding.EncodeToString(digest) != parts[5] {
		clear(digest)
		return PasswordHash{}, ErrInvalidPasswordHash
	}
	copy(hash.digest[:], digest)
	clear(digest)
	hash.valid = true
	return hash, nil
}

// HashPassword creates an Argon2id PHC hash with a cryptographically random salt.
func HashPassword(password []byte) (string, error) {
	if len(password) == 0 || len(password) > maxPasswordLength {
		return "", ErrInvalidPasswordLength
	}
	var salt [passwordHashSaltLength]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}
	digest := argon2.IDKey(password, salt[:], passwordHashTime, passwordHashMemoryKiB, passwordHashParallelism, passwordHashDigestLength)
	encoded := "$argon2id$v=19$m=19456,t=2,p=1$" + base64.RawStdEncoding.EncodeToString(salt[:]) + "$" + base64.RawStdEncoding.EncodeToString(digest)
	clear(digest)
	return encoded, nil
}

// PasswordIdentityBinding binds one exact username to a tenant and client scope.
type PasswordIdentityBinding struct {
	Username         string
	PasswordHash     PasswordHash
	TenantID         tenancy.TenantID
	Subject          string
	CredentialID     string
	AllowedClientIDs []string
	Roles            []security.Role
}

// PasswordAuthenticationProvider verifies configured username and Argon2id bindings.
type PasswordAuthenticationProvider struct {
	bindings map[string]PasswordIdentityBinding
	slots    chan struct{}
}

// NewPasswordAuthenticationProvider validates and copies password identity bindings.
func NewPasswordAuthenticationProvider(bindings []PasswordIdentityBinding) (*PasswordAuthenticationProvider, error) {
	if len(bindings) == 0 {
		return nil, errors.New("password authentication bindings are required")
	}

	copiedBindings := make(map[string]PasswordIdentityBinding, len(bindings))
	seenCredentialIDs := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		if binding.Username == "" || !binding.PasswordHash.valid || binding.TenantID == (tenancy.TenantID{}) || binding.Subject == "" || binding.CredentialID == "" {
			return nil, errors.New("password authentication identity binding is incomplete")
		}
		if _, exists := copiedBindings[binding.Username]; exists {
			return nil, errors.New("password authentication username is duplicated")
		}
		if _, exists := seenCredentialIDs[binding.CredentialID]; exists {
			return nil, errors.New("password authentication credential ID is duplicated")
		}
		seenCredentialIDs[binding.CredentialID] = struct{}{}
		if len(binding.AllowedClientIDs) == 0 || len(binding.Roles) == 0 {
			return nil, errors.New("password authentication identity binding is incomplete")
		}

		allowedClientIDs := make([]string, len(binding.AllowedClientIDs))
		seenClientIDs := make(map[string]struct{}, len(binding.AllowedClientIDs))
		for index, clientID := range binding.AllowedClientIDs {
			if clientID == "" {
				return nil, errors.New("password authentication allowed Client ID is empty")
			}
			if _, exists := seenClientIDs[clientID]; exists {
				return nil, errors.New("password authentication allowed Client ID is duplicated")
			}
			seenClientIDs[clientID] = struct{}{}
			allowedClientIDs[index] = clientID
		}

		roles := make([]security.Role, len(binding.Roles))
		seenRoles := make(map[security.Role]struct{}, len(binding.Roles))
		for index, role := range binding.Roles {
			if role == "" {
				return nil, errors.New("password authentication role is empty")
			}
			if _, exists := seenRoles[role]; exists {
				return nil, errors.New("password authentication role is duplicated")
			}
			seenRoles[role] = struct{}{}
			roles[index] = role
		}
		binding.AllowedClientIDs = allowedClientIDs
		binding.Roles = roles
		copiedBindings[binding.Username] = binding
	}

	return &PasswordAuthenticationProvider{
		bindings: copiedBindings,
		slots:    make(chan struct{}, maxConcurrentPasswordHashes),
	}, nil
}

// Authenticate verifies a password and returns its configured identity binding.
func (provider *PasswordAuthenticationProvider) Authenticate(ctx context.Context, credential applicationauthentication.Credential) (applicationauthentication.Result, error) {
	if provider == nil || ctx == nil || len(credential.Password) == 0 || len(credential.Password) > maxPasswordLength || ctx.Err() != nil {
		return applicationauthentication.Result{}, ErrInvalidCredential
	}
	select {
	case provider.slots <- struct{}{}:
		defer func() { <-provider.slots }()
	default:
		return applicationauthentication.Result{}, ErrInvalidCredential
	}
	if ctx.Err() != nil {
		return applicationauthentication.Result{}, ErrInvalidCredential
	}

	binding, known := provider.bindings[credential.Username]
	var salt [passwordHashSaltLength]byte
	var expected [passwordHashDigestLength]byte
	if known {
		salt = binding.PasswordHash.salt
		expected = binding.PasswordHash.digest
	}
	actual := argon2.IDKey(credential.Password, salt[:], passwordHashTime, passwordHashMemoryKiB, passwordHashParallelism, passwordHashDigestLength)
	matches := subtle.ConstantTimeCompare(actual, expected[:]) == 1
	clear(actual)
	if ctx.Err() != nil || !known || !matches {
		return applicationauthentication.Result{}, ErrInvalidCredential
	}

	return applicationauthentication.Result{
		TenantID:         binding.TenantID,
		Subject:          binding.Subject,
		CredentialID:     binding.CredentialID,
		AllowedClientIDs: append([]string(nil), binding.AllowedClientIDs...),
		Roles:            append([]security.Role(nil), binding.Roles...),
		ExpiresAt:        time.UnixMilli(math.MaxInt64),
	}, nil
}

var _ applicationauthentication.AuthenticationProvider = (*PasswordAuthenticationProvider)(nil)
