package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	authenticationadapter "narumq/internal/adapters/authentication"
	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

const maxCredentialIDBytes = 512

type rawSecurityConfig struct {
	JWT                *rawJWTConfig               `json:"jwt"`
	Identities         []rawIdentityConfig         `json:"identities"`
	PasswordIdentities []rawPasswordIdentityConfig `json:"passwordIdentities"`
	MTLSIdentities     []rawMTLSIdentityConfig     `json:"mtlsIdentities"`
	ACL                []rawACLRuleConfig          `json:"acl"`
	TenantLimits       *rawTenantLimitsConfig      `json:"tenantLimits"`
}

type rawTenantLimitValues struct {
	MaxConnections    *uint64 `json:"maxConnections"`
	MaxSessions       *uint64 `json:"maxSessions"`
	MaxSubscriptions  *uint64 `json:"maxSubscriptions"`
	MessagesPerSecond *uint64 `json:"messagesPerSecond"`
	BytesPerSecond    *uint64 `json:"bytesPerSecond"`
	MaxPayloadBytes   *uint64 `json:"maxPayloadBytes"`
}

type rawTenantLimitsConfig struct {
	MaxWait *string                  `json:"maxWait"`
	Default rawTenantLimitValues     `json:"default"`
	Tenants []rawTenantLimitOverride `json:"tenants"`
}

type rawTenantLimitOverride struct {
	TenantID          string  `json:"tenantId"`
	MaxConnections    *uint64 `json:"maxConnections"`
	MaxSessions       *uint64 `json:"maxSessions"`
	MaxSubscriptions  *uint64 `json:"maxSubscriptions"`
	MessagesPerSecond *uint64 `json:"messagesPerSecond"`
	BytesPerSecond    *uint64 `json:"bytesPerSecond"`
	MaxPayloadBytes   *uint64 `json:"maxPayloadBytes"`
}

type rawJWTConfig struct {
	Issuer          string   `json:"issuer"`
	Audience        string   `json:"audience"`
	Algorithms      []string `json:"algorithms"`
	JWKSURL         string   `json:"jwksUrl"`
	CacheTTL        string   `json:"cacheTtl"`
	RefreshCooldown string   `json:"refreshCooldown"`
	ClockSkew       string   `json:"clockSkew"`
	HTTPTimeout     string   `json:"httpTimeout"`
}

type rawIdentityConfig struct {
	Issuer           string   `json:"issuer"`
	Subject          string   `json:"subject"`
	TenantID         string   `json:"tenantId"`
	CredentialID     string   `json:"credentialId"`
	AllowedClientIDs []string `json:"allowedClientIds"`
	Roles            []string `json:"roles"`
}
type rawPasswordIdentityConfig struct {
	Username         string   `json:"username"`
	PasswordHash     string   `json:"passwordHash"`
	TenantID         string   `json:"tenantId"`
	Subject          string   `json:"subject"`
	CredentialID     string   `json:"credentialId"`
	AllowedClientIDs []string `json:"allowedClientIds"`
	Roles            []string `json:"roles"`
}

type rawMTLSIdentityConfig struct {
	IdentityKind     string   `json:"identityKind"`
	Identity         string   `json:"identity"`
	TenantID         string   `json:"tenantId"`
	Subject          string   `json:"subject"`
	CredentialID     string   `json:"credentialId"`
	AllowedClientIDs []string `json:"allowedClientIds"`
	Roles            []string `json:"roles"`
}

type rawACLRuleConfig struct {
	TenantID    string `json:"tenantId"`
	Role        string `json:"role"`
	Effect      string `json:"effect"`
	Action      string `json:"action"`
	TopicFilter string `json:"topicFilter"`
}

// SecurityBundle encapsulates validated authentication methods, policy and optional tenant limits.
type SecurityBundle struct {
	JWTConfig        *authenticationadapter.Config
	PasswordBindings []authenticationadapter.PasswordIdentityBinding
	MTLSBindings     []authenticationadapter.ClientCertificateIdentityBinding
	Policy           security.Policy
	TenantLimits     *tenantlimit.Profile
}

// loadSecurityConfigFile reads and strictly decodes a JSON security configuration file.
func loadSecurityConfigFile(path string) (*SecurityBundle, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read security config file %q: %w", path, err)
	}
	return parseSecurityConfig(bytes.NewReader(data))
}

// parseSecurityConfig strictly validates a single JSON security configuration value.
func parseSecurityConfig(reader io.Reader) (*SecurityBundle, error) {
	if reader == nil {
		return nil, errors.New("security config reader is nil")
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read security config: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var raw rawSecurityConfig
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode security config: %w", err)
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("security config contains trailing JSON value")
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return nil, fmt.Errorf("check security config JSON keys: %w", err)
	}

	jwtConfig, passwordBindings, mtlsBindings, err := validateAuthenticationConfig(raw.JWT, raw.Identities, raw.PasswordIdentities, raw.MTLSIdentities)
	if err != nil {
		return nil, err
	}

	policy, err := validateACL(raw.ACL)
	if err != nil {
		return nil, err
	}

	tenantLimits, err := validateTenantLimits(raw.TenantLimits)
	if err != nil {
		return nil, err
	}

	return &SecurityBundle{
		JWTConfig:        jwtConfig,
		PasswordBindings: passwordBindings,
		MTLSBindings:     mtlsBindings,
		Policy:           policy,
		TenantLimits:     tenantLimits,
	}, nil
}

func validateTenantLimits(raw *rawTenantLimitsConfig) (*tenantlimit.Profile, error) {
	if raw == nil {
		return nil, nil
	}
	if len(raw.Tenants) > tenantlimit.MaxTenantOverrides {
		return nil, fmt.Errorf("invalid tenantLimits: too many tenant overrides")
	}
	profile := &tenantlimit.Profile{MaxWait: tenantlimit.DefaultMaxWait, Default: resolveTenantLimitValues(raw.Default), Tenants: make(map[tenancy.TenantID]tenantlimit.Limits, len(raw.Tenants))}
	if raw.MaxWait != nil {
		maxWait, err := time.ParseDuration(*raw.MaxWait)
		if err != nil {
			return nil, fmt.Errorf("invalid tenantLimits: maxWait: %w", err)
		}
		profile.MaxWait = maxWait
	}
	for _, override := range raw.Tenants {
		tenantID, err := tenancy.NewTenantID(override.TenantID)
		if err != nil {
			return nil, fmt.Errorf("invalid tenantLimits: tenantId: %w", err)
		}
		if _, exists := profile.Tenants[tenantID]; exists {
			return nil, fmt.Errorf("invalid tenantLimits: duplicate tenantId %q", override.TenantID)
		}
		limits := profile.Default
		applyTenantLimitValues(&limits, rawTenantLimitValues{
			MaxConnections: override.MaxConnections, MaxSessions: override.MaxSessions,
			MaxSubscriptions: override.MaxSubscriptions, MessagesPerSecond: override.MessagesPerSecond,
			BytesPerSecond: override.BytesPerSecond, MaxPayloadBytes: override.MaxPayloadBytes,
		})
		profile.Tenants[tenantID] = limits
	}
	if err := tenantlimit.ValidateProfile(*profile); err != nil {
		return nil, fmt.Errorf("invalid tenantLimits: %w", err)
	}
	return profile, nil
}

func resolveTenantLimitValues(raw rawTenantLimitValues) tenantlimit.Limits {
	var limits tenantlimit.Limits
	applyTenantLimitValues(&limits, raw)
	return limits
}

func applyTenantLimitValues(limits *tenantlimit.Limits, raw rawTenantLimitValues) {
	if raw.MaxConnections != nil {
		limits.MaxConnections = *raw.MaxConnections
	}
	if raw.MaxSessions != nil {
		limits.MaxSessions = *raw.MaxSessions
	}
	if raw.MaxSubscriptions != nil {
		limits.MaxSubscriptions = *raw.MaxSubscriptions
	}
	if raw.MessagesPerSecond != nil {
		limits.MessagesPerSecond = *raw.MessagesPerSecond
	}
	if raw.BytesPerSecond != nil {
		limits.BytesPerSecond = *raw.BytesPerSecond
	}
	if raw.MaxPayloadBytes != nil {
		limits.MaxPayloadBytes = *raw.MaxPayloadBytes
	}
}

func validateAuthenticationConfig(
	jwt *rawJWTConfig,
	identities []rawIdentityConfig,
	passwordIdentities []rawPasswordIdentityConfig,
	mtlsIdentities []rawMTLSIdentityConfig,
) (*authenticationadapter.Config, []authenticationadapter.PasswordIdentityBinding, []authenticationadapter.ClientCertificateIdentityBinding, error) {
	if jwt != nil && len(passwordIdentities) > 0 {
		return nil, nil, nil, errors.New("jwt and passwordIdentities are mutually exclusive")
	}
	seenCredentialIDs := make(map[string]struct{}, len(identities)+len(passwordIdentities)+len(mtlsIdentities))
	var jwtConfig *authenticationadapter.Config
	if jwt == nil {
		if len(identities) > 0 {
			return nil, nil, nil, errors.New("identities require jwt")
		}
	} else {
		var err error
		jwtConfig, err = validateJWTAndIdentities(jwt, identities, seenCredentialIDs)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	passwordBindings, err := validatePasswordIdentities(passwordIdentities, seenCredentialIDs)
	if err != nil {
		return nil, nil, nil, err
	}
	mtlsBindings, err := validateMTLSIdentities(mtlsIdentities, seenCredentialIDs)
	if err != nil {
		return nil, nil, nil, err
	}
	if jwtConfig == nil && len(passwordBindings) == 0 && len(mtlsBindings) == 0 {
		return nil, nil, nil, errors.New("at least one authentication method is required")
	}
	return jwtConfig, passwordBindings, mtlsBindings, nil
}

func validateJWTVerifierConfig(prefix string, jwt *rawJWTConfig) (authenticationadapter.VerifierConfig, error) {
	if jwt.Issuer == "" {
		return authenticationadapter.VerifierConfig{}, fmt.Errorf("%s.issuer is required", prefix)
	}
	if jwt.Audience == "" {
		return authenticationadapter.VerifierConfig{}, fmt.Errorf("%s.audience is required", prefix)
	}
	if len(jwt.Algorithms) == 0 {
		return authenticationadapter.VerifierConfig{}, fmt.Errorf("%s.algorithms is required", prefix)
	}
	seenAlgs := make(map[string]struct{}, len(jwt.Algorithms))
	for _, alg := range jwt.Algorithms {
		if alg == "" {
			return authenticationadapter.VerifierConfig{}, fmt.Errorf("%s algorithm must not be empty", prefix)
		}
		if _, exists := seenAlgs[alg]; exists {
			return authenticationadapter.VerifierConfig{}, fmt.Errorf("duplicate %s algorithm %q", prefix, alg)
		}
		seenAlgs[alg] = struct{}{}
	}

	parsedURL, err := url.Parse(jwt.JWKSURL)
	if err != nil || !parsedURL.IsAbs() || parsedURL.Host == "" {
		return authenticationadapter.VerifierConfig{}, fmt.Errorf("%s.jwksUrl must be an absolute URL", prefix)
	}
	if parsedURL.Scheme != "https" {
		if parsedURL.Scheme != "http" || !isLoopbackHost(parsedURL.Hostname()) {
			return authenticationadapter.VerifierConfig{}, fmt.Errorf("%s.jwksUrl must be HTTPS or loopback HTTP", prefix)
		}
	}

	cacheTTL, err := parsePositiveDuration(prefix+".cacheTtl", jwt.CacheTTL)
	if err != nil {
		return authenticationadapter.VerifierConfig{}, err
	}
	refreshCooldown, err := parsePositiveDuration(prefix+".refreshCooldown", jwt.RefreshCooldown)
	if err != nil {
		return authenticationadapter.VerifierConfig{}, err
	}
	clockSkew, err := parsePositiveDuration(prefix+".clockSkew", jwt.ClockSkew)
	if err != nil {
		return authenticationadapter.VerifierConfig{}, err
	}
	httpTimeout, err := parsePositiveDuration(prefix+".httpTimeout", jwt.HTTPTimeout)
	if err != nil {
		return authenticationadapter.VerifierConfig{}, err
	}
	return authenticationadapter.VerifierConfig{
		Issuer: jwt.Issuer, Audience: jwt.Audience, Algorithms: append([]string(nil), jwt.Algorithms...),
		JWKSURL: jwt.JWKSURL, CacheTTL: cacheTTL, RefreshCooldown: refreshCooldown,
		ClockSkew: clockSkew, HTTPTimeout: httpTimeout,
	}, nil
}

func validateJWTAndIdentities(
	jwt *rawJWTConfig,
	identities []rawIdentityConfig,
	seenCredentialIDs map[string]struct{},
) (*authenticationadapter.Config, error) {
	verifierConfig, err := validateJWTVerifierConfig("jwt", jwt)
	if err != nil {
		return nil, err
	}
	if len(identities) == 0 {
		return nil, errors.New("identities must contain at least one binding")
	}
	if seenCredentialIDs == nil {
		seenCredentialIDs = make(map[string]struct{}, len(identities))
	}
	bindings := make([]authenticationadapter.IdentityBinding, len(identities))
	seenSubjects := make(map[string]struct{}, len(identities))
	for i, id := range identities {
		if id.Issuer != jwt.Issuer {
			return nil, fmt.Errorf("identity issuer %q does not match jwt.issuer %q", id.Issuer, jwt.Issuer)
		}
		if id.Subject == "" {
			return nil, errors.New("identity subject is required")
		}
		if _, exists := seenSubjects[id.Subject]; exists {
			return nil, fmt.Errorf("duplicate identity subject %q", id.Subject)
		}
		seenSubjects[id.Subject] = struct{}{}
		tenantID, err := tenancy.NewTenantID(id.TenantID)
		if err != nil {
			return nil, fmt.Errorf("invalid tenant ID %q: %w", id.TenantID, err)
		}
		if id.CredentialID == "" {
			return nil, errors.New("identity credentialId is required")
		}
		if len(id.CredentialID) > maxCredentialIDBytes || !utf8.ValidString(id.CredentialID) {
			return nil, errors.New("identity credentialId must be 1 to 512 bytes of UTF-8")
		}
		if _, exists := seenCredentialIDs[id.CredentialID]; exists {
			return nil, fmt.Errorf("duplicate credentialId %q", id.CredentialID)
		}
		seenCredentialIDs[id.CredentialID] = struct{}{}
		allowedClientIDs, err := validateAllowedClientIDs("identity", id.Subject, id.AllowedClientIDs)
		if err != nil {
			return nil, err
		}
		roles, err := validateRoles("identity", id.Subject, id.Roles)
		if err != nil {
			return nil, err
		}
		bindings[i] = authenticationadapter.IdentityBinding{
			Issuer: id.Issuer, Subject: id.Subject, TenantID: tenantID,
			CredentialID: id.CredentialID, AllowedClientIDs: allowedClientIDs, Roles: roles,
		}
	}
	return &authenticationadapter.Config{
		Issuer: verifierConfig.Issuer, Audience: verifierConfig.Audience,
		Algorithms: verifierConfig.Algorithms, JWKSURL: verifierConfig.JWKSURL,
		CacheTTL: verifierConfig.CacheTTL, RefreshCooldown: verifierConfig.RefreshCooldown,
		ClockSkew: verifierConfig.ClockSkew, HTTPTimeout: verifierConfig.HTTPTimeout, Bindings: bindings,
	}, nil
}

func validatePasswordIdentities(
	identities []rawPasswordIdentityConfig,
	seenCredentialIDs map[string]struct{},
) ([]authenticationadapter.PasswordIdentityBinding, error) {
	if len(identities) == 0 {
		return nil, nil
	}
	if seenCredentialIDs == nil {
		seenCredentialIDs = make(map[string]struct{}, len(identities))
	}
	bindings := make([]authenticationadapter.PasswordIdentityBinding, len(identities))
	seenUsernames := make(map[string]struct{}, len(identities))
	for index, identity := range identities {
		if identity.Username == "" {
			return nil, errors.New("password identity username is required")
		}
		if _, exists := seenUsernames[identity.Username]; exists {
			return nil, fmt.Errorf("duplicate password identity username %q", identity.Username)
		}
		seenUsernames[identity.Username] = struct{}{}
		if identity.Subject == "" {
			return nil, errors.New("password identity subject is required")
		}
		passwordHash, err := authenticationadapter.ParsePasswordHash(identity.PasswordHash)
		if err != nil {
			return nil, fmt.Errorf("password identity %q has invalid passwordHash: %w", identity.Username, err)
		}
		tenantID, err := tenancy.NewTenantID(identity.TenantID)
		if err != nil {
			return nil, fmt.Errorf("invalid password identity tenant ID %q: %w", identity.TenantID, err)
		}
		if identity.CredentialID == "" {
			return nil, errors.New("password identity credentialId is required")
		}
		if len(identity.CredentialID) > maxCredentialIDBytes || !utf8.ValidString(identity.CredentialID) {
			return nil, errors.New("password identity credentialId must be 1 to 512 bytes of UTF-8")
		}
		if _, exists := seenCredentialIDs[identity.CredentialID]; exists {
			return nil, fmt.Errorf("duplicate credentialId %q", identity.CredentialID)
		}
		seenCredentialIDs[identity.CredentialID] = struct{}{}
		allowedClientIDs, err := validateAllowedClientIDs("password identity", identity.Username, identity.AllowedClientIDs)
		if err != nil {
			return nil, err
		}
		roles, err := validateRoles("password identity", identity.Username, identity.Roles)
		if err != nil {
			return nil, err
		}
		bindings[index] = authenticationadapter.PasswordIdentityBinding{
			Username: identity.Username, PasswordHash: passwordHash, TenantID: tenantID,
			Subject: identity.Subject, CredentialID: identity.CredentialID,
			AllowedClientIDs: allowedClientIDs, Roles: roles,
		}
	}
	return bindings, nil
}

func validateMTLSIdentities(
	identities []rawMTLSIdentityConfig,
	seenCredentialIDs map[string]struct{},
) ([]authenticationadapter.ClientCertificateIdentityBinding, error) {
	if len(identities) == 0 {
		return nil, nil
	}
	if seenCredentialIDs == nil {
		seenCredentialIDs = make(map[string]struct{}, len(identities))
	}
	type selector struct {
		kind  authenticationadapter.ClientCertificateIdentityKind
		value string
	}
	seenSelectors := make(map[selector]struct{}, len(identities))
	bindings := make([]authenticationadapter.ClientCertificateIdentityBinding, len(identities))
	for index, identity := range identities {
		kind, ok := parseMTLSIdentityKind(identity.IdentityKind)
		if !ok {
			return nil, fmt.Errorf("invalid mTLS identityKind %q", identity.IdentityKind)
		}
		if identity.Identity == "" {
			return nil, errors.New("mTLS identity is required")
		}
		key := selector{kind: kind, value: identity.Identity}
		if _, exists := seenSelectors[key]; exists {
			return nil, fmt.Errorf("duplicate mTLS identity selector %q", identity.Identity)
		}
		seenSelectors[key] = struct{}{}

		tenantID, err := tenancy.NewTenantID(identity.TenantID)
		if err != nil {
			return nil, fmt.Errorf("invalid mTLS tenant ID %q: %w", identity.TenantID, err)
		}
		if identity.Subject == "" {
			return nil, errors.New("mTLS identity subject is required")
		}
		if identity.CredentialID == "" {
			return nil, errors.New("mTLS identity credentialId is required")
		}
		if len(identity.CredentialID) > maxCredentialIDBytes || !utf8.ValidString(identity.CredentialID) {
			return nil, errors.New("mTLS identity credentialId must be 1 to 512 bytes of UTF-8")
		}
		if _, exists := seenCredentialIDs[identity.CredentialID]; exists {
			return nil, fmt.Errorf("duplicate credentialId %q", identity.CredentialID)
		}
		seenCredentialIDs[identity.CredentialID] = struct{}{}
		allowedClientIDs, err := validateAllowedClientIDs("mTLS identity", identity.Identity, identity.AllowedClientIDs)
		if err != nil {
			return nil, err
		}
		roles, err := validateRoles("mTLS identity", identity.Identity, identity.Roles)
		if err != nil {
			return nil, err
		}
		bindings[index] = authenticationadapter.ClientCertificateIdentityBinding{
			Kind:             kind,
			Value:            identity.Identity,
			TenantID:         tenantID,
			Subject:          identity.Subject,
			CredentialID:     identity.CredentialID,
			AllowedClientIDs: allowedClientIDs,
			Roles:            roles,
		}
	}
	return bindings, nil
}

func parseMTLSIdentityKind(raw string) (authenticationadapter.ClientCertificateIdentityKind, bool) {
	switch raw {
	case string(authenticationadapter.ClientCertificateIdentityKindCommonName):
		return authenticationadapter.ClientCertificateIdentityKindCommonName, true
	case string(authenticationadapter.ClientCertificateIdentityKindDNSSAN):
		return authenticationadapter.ClientCertificateIdentityKindDNSSAN, true
	case string(authenticationadapter.ClientCertificateIdentityKindEmailSAN):
		return authenticationadapter.ClientCertificateIdentityKindEmailSAN, true
	default:
		return "", false
	}
}

func validateAllowedClientIDs(kind, identity string, clientIDs []string) ([]string, error) {
	if len(clientIDs) == 0 {
		return nil, fmt.Errorf("%s %q must have at least one allowed Client ID", kind, identity)
	}
	copied := make([]string, len(clientIDs))
	seen := make(map[string]struct{}, len(clientIDs))
	for index, clientID := range clientIDs {
		if clientID == "" {
			return nil, fmt.Errorf("%s %q has empty allowed Client ID", kind, identity)
		}
		if _, exists := seen[clientID]; exists {
			return nil, fmt.Errorf("%s %q has duplicate allowed Client ID %q", kind, identity, clientID)
		}
		seen[clientID] = struct{}{}
		copied[index] = clientID
	}
	return copied, nil
}

func validateRoles(kind, identity string, roleStrings []string) ([]security.Role, error) {
	if len(roleStrings) == 0 {
		return nil, fmt.Errorf("%s %q must have at least one role", kind, identity)
	}
	roles := make([]security.Role, len(roleStrings))
	seen := make(map[security.Role]struct{}, len(roleStrings))
	for index, roleString := range roleStrings {
		role := security.Role(roleString)
		if role == "" {
			return nil, fmt.Errorf("%s %q has empty role", kind, identity)
		}
		if _, exists := seen[role]; exists {
			return nil, fmt.Errorf("%s %q has duplicate role %q", kind, identity, role)
		}
		seen[role] = struct{}{}
		roles[index] = role
	}
	return roles, nil
}

func validateACL(aclConfigs []rawACLRuleConfig) (security.Policy, error) {
	rules := make([]security.Rule, len(aclConfigs))
	for i, ruleCfg := range aclConfigs {
		tenantID, err := tenancy.NewTenantID(ruleCfg.TenantID)
		if err != nil {
			return security.Policy{}, fmt.Errorf("invalid ACL rule tenantId %q: %w", ruleCfg.TenantID, err)
		}

		effect := security.Effect(ruleCfg.Effect)
		if effect != security.EffectAllow && effect != security.EffectDeny {
			return security.Policy{}, fmt.Errorf("invalid ACL effect %q, must be allow or deny", ruleCfg.Effect)
		}

		action := security.Action(ruleCfg.Action)
		switch action {
		case security.ActionPublish, security.ActionSubscribe, security.ActionUnsubscribe, security.ActionWill:
		default:
			return security.Policy{}, fmt.Errorf("invalid ACL action %q", ruleCfg.Action)
		}

		topicFilter, err := tenancy.NewTopicFilter(tenantID, ruleCfg.TopicFilter)
		if err != nil {
			return security.Policy{}, fmt.Errorf("invalid ACL topicFilter %q: %w", ruleCfg.TopicFilter, err)
		}

		rule, err := security.NewRule(tenantID, security.Role(ruleCfg.Role), effect, action, topicFilter)
		if err != nil {
			return security.Policy{}, fmt.Errorf("create ACL rule: %w", err)
		}
		rules[i] = rule
	}

	return security.NewPolicy(rules)
}

func parsePositiveDuration(name, raw string) (time.Duration, error) {
	if raw == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s duration %q: %w", name, raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", name)
	}
	return d, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
