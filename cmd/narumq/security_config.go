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

	authenticationadapter "narumq/internal/adapters/authentication"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type rawSecurityConfig struct {
	JWT        rawJWTConfig        `json:"jwt"`
	Identities []rawIdentityConfig `json:"identities"`
	ACL        []rawACLRuleConfig  `json:"acl"`
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

type rawACLRuleConfig struct {
	TenantID    string `json:"tenantId"`
	Role        string `json:"role"`
	Effect      string `json:"effect"`
	Action      string `json:"action"`
	TopicFilter string `json:"topicFilter"`
}

// SecurityBundle encapsulates the validated authentication and authorization policy.
type SecurityBundle struct {
	AuthConfig authenticationadapter.Config
	Policy     security.Policy
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
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	var raw rawSecurityConfig
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode security config: %w", err)
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("security config contains trailing JSON value")
	}

	authConfig, err := validateJWTAndIdentities(raw.JWT, raw.Identities)
	if err != nil {
		return nil, err
	}

	policy, err := validateACL(raw.ACL)
	if err != nil {
		return nil, err
	}

	return &SecurityBundle{
		AuthConfig: authConfig,
		Policy:     policy,
	}, nil
}

func validateJWTAndIdentities(jwt rawJWTConfig, identities []rawIdentityConfig) (authenticationadapter.Config, error) {
	if jwt.Issuer == "" {
		return authenticationadapter.Config{}, errors.New("jwt.issuer is required")
	}
	if jwt.Audience == "" {
		return authenticationadapter.Config{}, errors.New("jwt.audience is required")
	}
	if len(jwt.Algorithms) == 0 {
		return authenticationadapter.Config{}, errors.New("jwt.algorithms is required")
	}
	seenAlgs := make(map[string]struct{}, len(jwt.Algorithms))
	for _, alg := range jwt.Algorithms {
		if alg == "" {
			return authenticationadapter.Config{}, errors.New("jwt algorithm must not be empty")
		}
		if _, exists := seenAlgs[alg]; exists {
			return authenticationadapter.Config{}, fmt.Errorf("duplicate jwt algorithm %q", alg)
		}
		seenAlgs[alg] = struct{}{}
	}

	parsedURL, err := url.Parse(jwt.JWKSURL)
	if err != nil || !parsedURL.IsAbs() || parsedURL.Host == "" {
		return authenticationadapter.Config{}, errors.New("jwt.jwksUrl must be an absolute URL")
	}
	if parsedURL.Scheme != "https" {
		if parsedURL.Scheme != "http" || !isLoopbackHost(parsedURL.Hostname()) {
			return authenticationadapter.Config{}, errors.New("jwt.jwksUrl must be HTTPS or loopback HTTP")
		}
	}

	cacheTTL, err := parsePositiveDuration("jwt.cacheTtl", jwt.CacheTTL)
	if err != nil {
		return authenticationadapter.Config{}, err
	}
	refreshCooldown, err := parsePositiveDuration("jwt.refreshCooldown", jwt.RefreshCooldown)
	if err != nil {
		return authenticationadapter.Config{}, err
	}
	clockSkew, err := parsePositiveDuration("jwt.clockSkew", jwt.ClockSkew)
	if err != nil {
		return authenticationadapter.Config{}, err
	}
	httpTimeout, err := parsePositiveDuration("jwt.httpTimeout", jwt.HTTPTimeout)
	if err != nil {
		return authenticationadapter.Config{}, err
	}

	if len(identities) == 0 {
		return authenticationadapter.Config{}, errors.New("identities must contain at least one binding")
	}

	bindings := make([]authenticationadapter.IdentityBinding, len(identities))
	seenSubjects := make(map[string]struct{}, len(identities))
	seenCreds := make(map[string]struct{}, len(identities))

	for i, id := range identities {
		if id.Issuer != jwt.Issuer {
			return authenticationadapter.Config{}, fmt.Errorf("identity issuer %q does not match jwt.issuer %q", id.Issuer, jwt.Issuer)
		}
		if id.Subject == "" {
			return authenticationadapter.Config{}, errors.New("identity subject is required")
		}
		if _, exists := seenSubjects[id.Subject]; exists {
			return authenticationadapter.Config{}, fmt.Errorf("duplicate identity subject %q", id.Subject)
		}
		seenSubjects[id.Subject] = struct{}{}

		tenantID, err := tenancy.NewTenantID(id.TenantID)
		if err != nil {
			return authenticationadapter.Config{}, fmt.Errorf("invalid tenant ID %q: %w", id.TenantID, err)
		}

		if id.CredentialID == "" {
			return authenticationadapter.Config{}, errors.New("identity credentialId is required")
		}
		if _, exists := seenCreds[id.CredentialID]; exists {
			return authenticationadapter.Config{}, fmt.Errorf("duplicate credentialId %q", id.CredentialID)
		}
		seenCreds[id.CredentialID] = struct{}{}

		if len(id.AllowedClientIDs) == 0 {
			return authenticationadapter.Config{}, fmt.Errorf("identity %q must have at least one allowed Client ID", id.Subject)
		}
		seenClientIDs := make(map[string]struct{}, len(id.AllowedClientIDs))
		for _, clientID := range id.AllowedClientIDs {
			if clientID == "" {
				return authenticationadapter.Config{}, fmt.Errorf("identity %q has empty allowed Client ID", id.Subject)
			}
			if _, exists := seenClientIDs[clientID]; exists {
				return authenticationadapter.Config{}, fmt.Errorf("identity %q has duplicate allowed Client ID %q", id.Subject, clientID)
			}
			seenClientIDs[clientID] = struct{}{}
		}

		if len(id.Roles) == 0 {
			return authenticationadapter.Config{}, fmt.Errorf("identity %q must have at least one role", id.Subject)
		}
		roles := make([]security.Role, len(id.Roles))
		seenRoles := make(map[security.Role]struct{}, len(id.Roles))
		for rIdx, roleStr := range id.Roles {
			role := security.Role(roleStr)
			if role == "" {
				return authenticationadapter.Config{}, fmt.Errorf("identity %q has empty role", id.Subject)
			}
			if _, exists := seenRoles[role]; exists {
				return authenticationadapter.Config{}, fmt.Errorf("identity %q has duplicate role %q", id.Subject, role)
			}
			seenRoles[role] = struct{}{}
			roles[rIdx] = role
		}

		bindings[i] = authenticationadapter.IdentityBinding{
			Issuer:           id.Issuer,
			Subject:          id.Subject,
			TenantID:         tenantID,
			CredentialID:     id.CredentialID,
			AllowedClientIDs: append([]string(nil), id.AllowedClientIDs...),
			Roles:            roles,
		}
	}

	return authenticationadapter.Config{
		Issuer:          jwt.Issuer,
		Audience:        jwt.Audience,
		Algorithms:      append([]string(nil), jwt.Algorithms...),
		JWKSURL:         jwt.JWKSURL,
		CacheTTL:        cacheTTL,
		RefreshCooldown: refreshCooldown,
		ClockSkew:       clockSkew,
		HTTPTimeout:     httpTimeout,
		Bindings:        bindings,
	}, nil
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
