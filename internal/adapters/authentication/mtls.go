package authentication

import (
	"context"
	"errors"

	applicationauthentication "narumq/internal/application/ports/authentication"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrInvalidClientCertificate means that a certificate DTO is incomplete or expired.
	ErrInvalidClientCertificate = errors.New("invalid client certificate")
	// ErrExpiredClientCertificate means that the certificate NotAfter boundary has passed.
	ErrExpiredClientCertificate = errors.New("expired client certificate")
	// ErrUnmappedClientCertificate means that no configured identity binding matches the certificate.
	ErrUnmappedClientCertificate = errors.New("unmapped client certificate")
	// ErrAmbiguousClientCertificate means that multiple configured identity bindings match the certificate.
	ErrAmbiguousClientCertificate = errors.New("ambiguous client certificate")
)

// ClientCertificateIdentityKind selects the certificate field used for static identity mapping.
type ClientCertificateIdentityKind string

const (
	ClientCertificateIdentityKindCommonName ClientCertificateIdentityKind = "commonName"
	ClientCertificateIdentityKindDNSSAN     ClientCertificateIdentityKind = "dnsSan"
	ClientCertificateIdentityKindEmailSAN   ClientCertificateIdentityKind = "emailSan"
)

// ClientCertificateIdentityBinding binds one certificate selector to a tenant and client scope.
type ClientCertificateIdentityBinding struct {
	Kind             ClientCertificateIdentityKind
	Value            string
	TenantID         tenancy.TenantID
	Subject          string
	CredentialID     string
	AllowedClientIDs []string
	Roles            []security.Role
}

// MTLSAuthenticationProvider maps verified certificate identity fields to static bindings.
type MTLSAuthenticationProvider struct {
	clock    Clock
	bindings []ClientCertificateIdentityBinding
}

// NewMTLSAuthenticationProvider validates and constructs a deterministic certificate mapper.
func NewMTLSAuthenticationProvider(clock Clock, bindings []ClientCertificateIdentityBinding) (*MTLSAuthenticationProvider, error) {
	if clock == nil {
		return nil, errors.New("mTLS authentication clock is nil")
	}
	if len(bindings) == 0 {
		return nil, errors.New("mTLS authentication bindings are required")
	}

	copiedBindings := make([]ClientCertificateIdentityBinding, len(bindings))
	seenSelectors := make(map[struct {
		kind  ClientCertificateIdentityKind
		value string
	}]struct{}, len(bindings))
	seenCredentialIDs := make(map[string]struct{}, len(bindings))
	for index, binding := range bindings {
		if !validClientCertificateIdentityKind(binding.Kind) {
			return nil, errors.New("mTLS authentication identity kind is invalid")
		}
		if binding.Value == "" {
			return nil, errors.New("mTLS authentication identity value is empty")
		}
		if binding.TenantID == (tenancy.TenantID{}) || binding.Subject == "" || binding.CredentialID == "" {
			return nil, errors.New("mTLS authentication identity binding is incomplete")
		}
		selector := struct {
			kind  ClientCertificateIdentityKind
			value string
		}{kind: binding.Kind, value: binding.Value}
		if _, exists := seenSelectors[selector]; exists {
			return nil, errors.New("mTLS authentication identity selector is duplicated")
		}
		seenSelectors[selector] = struct{}{}
		if _, exists := seenCredentialIDs[binding.CredentialID]; exists {
			return nil, errors.New("mTLS authentication credential ID is duplicated")
		}
		seenCredentialIDs[binding.CredentialID] = struct{}{}

		if len(binding.AllowedClientIDs) == 0 || len(binding.Roles) == 0 {
			return nil, errors.New("mTLS authentication identity binding is incomplete")
		}
		allowedClientIDs := make([]string, len(binding.AllowedClientIDs))
		seenClientIDs := make(map[string]struct{}, len(binding.AllowedClientIDs))
		for clientIndex, clientID := range binding.AllowedClientIDs {
			if clientID == "" {
				return nil, errors.New("mTLS authentication allowed Client ID is empty")
			}
			if _, exists := seenClientIDs[clientID]; exists {
				return nil, errors.New("mTLS authentication allowed Client ID is duplicated")
			}
			seenClientIDs[clientID] = struct{}{}
			allowedClientIDs[clientIndex] = clientID
		}
		roles := make([]security.Role, len(binding.Roles))
		seenRoles := make(map[security.Role]struct{}, len(binding.Roles))
		for roleIndex, role := range binding.Roles {
			if role == "" {
				return nil, errors.New("mTLS authentication role is empty")
			}
			if _, exists := seenRoles[role]; exists {
				return nil, errors.New("mTLS authentication role is duplicated")
			}
			seenRoles[role] = struct{}{}
			roles[roleIndex] = role
		}
		copiedBindings[index] = ClientCertificateIdentityBinding{
			Kind:             binding.Kind,
			Value:            binding.Value,
			TenantID:         binding.TenantID,
			Subject:          binding.Subject,
			CredentialID:     binding.CredentialID,
			AllowedClientIDs: allowedClientIDs,
			Roles:            roles,
		}
	}
	return &MTLSAuthenticationProvider{clock: clock, bindings: copiedBindings}, nil
}

// AuthenticateClientCertificate maps a verified certificate DTO to exactly one static binding.
func (provider *MTLSAuthenticationProvider) AuthenticateClientCertificate(ctx context.Context, certificate applicationauthentication.ClientCertificateDTO) (applicationauthentication.Result, error) {
	if provider == nil || ctx == nil || certificate.NotAfter.IsZero() {
		return applicationauthentication.Result{}, ErrInvalidClientCertificate
	}
	if !provider.clock.Now().Before(certificate.NotAfter) {
		return applicationauthentication.Result{}, ErrExpiredClientCertificate
	}

	var matched *ClientCertificateIdentityBinding
	for index := range provider.bindings {
		binding := &provider.bindings[index]
		if !certificateMatchesBinding(certificate, *binding) {
			continue
		}
		if matched != nil {
			return applicationauthentication.Result{}, ErrAmbiguousClientCertificate
		}
		matched = binding
	}
	if matched == nil {
		return applicationauthentication.Result{}, ErrUnmappedClientCertificate
	}
	return applicationauthentication.Result{
		TenantID:         matched.TenantID,
		Subject:          matched.Subject,
		CredentialID:     matched.CredentialID,
		AllowedClientIDs: append([]string(nil), matched.AllowedClientIDs...),
		Roles:            append([]security.Role(nil), matched.Roles...),
		ExpiresAt:        certificate.NotAfter,
	}, nil
}

func validClientCertificateIdentityKind(kind ClientCertificateIdentityKind) bool {
	switch kind {
	case ClientCertificateIdentityKindCommonName, ClientCertificateIdentityKindDNSSAN, ClientCertificateIdentityKindEmailSAN:
		return true
	default:
		return false
	}
}

func certificateMatchesBinding(certificate applicationauthentication.ClientCertificateDTO, binding ClientCertificateIdentityBinding) bool {
	switch binding.Kind {
	case ClientCertificateIdentityKindCommonName:
		return certificate.CommonName == binding.Value
	case ClientCertificateIdentityKindDNSSAN:
		return containsExact(certificate.DNSNames, binding.Value)
	case ClientCertificateIdentityKindEmailSAN:
		return containsExact(certificate.EmailAddresses, binding.Value)
	default:
		return false
	}
}

func containsExact(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

var _ applicationauthentication.ClientCertificateAuthenticationProvider = (*MTLSAuthenticationProvider)(nil)
