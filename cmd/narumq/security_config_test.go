package main

import (
	"strings"
	"testing"
	"time"

	"narumq/internal/domain/tenancy"
)

const validSecurityJSON = `{
  "jwt": {
    "issuer": "https://issuer.example",
    "audience": "narumq",
    "algorithms": ["RS256"],
    "jwksUrl": "https://issuer.example/.well-known/jwks.json",
    "cacheTtl": "15m",
    "refreshCooldown": "30s",
    "clockSkew": "30s",
    "httpTimeout": "5s"
  },
  "identities": [
    {
      "issuer": "https://issuer.example",
      "subject": "device-1",
      "tenantId": "tenant-a",
      "credentialId": "device-1-key",
      "allowedClientIds": ["device-1"],
      "roles": ["publisher"]
    }
  ],
  "acl": [
    {
      "tenantId": "tenant-a",
      "role": "publisher",
      "effect": "allow",
      "action": "publish",
      "topicFilter": "sensors/#"
    }
  ]
}`

// Traceability: SEC-02, SEC-04, TEN-01
func TestParseSecurityConfig_Valid(t *testing.T) {
	bundle, err := parseSecurityConfig(strings.NewReader(validSecurityJSON))
	if err != nil {
		t.Fatalf("parseSecurityConfig() unexpected error = %v", err)
	}
	if bundle.JWTConfig == nil {
		t.Fatal("JWTConfig = nil, want JWT configuration")
	}
	if bundle.JWTConfig.Issuer != "https://issuer.example" {
		t.Errorf("Issuer = %q, want 'https://issuer.example'", bundle.JWTConfig.Issuer)
	}
	if len(bundle.JWTConfig.Bindings) != 1 {
		t.Errorf("Bindings length = %d, want 1", len(bundle.JWTConfig.Bindings))
	}
}

func TestParseSecurityConfig_LoopbackHTTPAllowed(t *testing.T) {
	loopbackJSON := strings.Replace(validSecurityJSON, "https://issuer.example/.well-known/jwks.json", "http://127.0.0.1:8080/jwks.json", 1)
	if _, err := parseSecurityConfig(strings.NewReader(loopbackJSON)); err != nil {
		t.Fatalf("loopback HTTP rejected: %v", err)
	}
}

func TestParseSecurityConfig_Rejections(t *testing.T) {
	cases := []struct {
		name        string
		input       string
		errContains string
	}{
		{
			name:        "unknown field",
			input:       `{"jwt": {"issuer": "https://issuer.example", "audience": "narumq", "algorithms": ["RS256"], "jwksUrl": "https://issuer.example/jwks", "cacheTtl": "1m", "refreshCooldown": "1m", "clockSkew": "1m", "httpTimeout": "1m"}, "identities": [{"issuer": "https://issuer.example", "subject": "s", "tenantId": "t", "credentialId": "c", "allowedClientIds": ["a"], "roles": ["r"]}], "extra": 123}`,
			errContains: "unknown field",
		},
		{
			name:        "trailing JSON",
			input:       validSecurityJSON + `{"second": true}`,
			errContains: "trailing JSON",
		},
		{
			name:        "non-loopback HTTP",
			input:       strings.Replace(validSecurityJSON, "https://issuer.example/.well-known/jwks.json", "http://example.com/jwks.json", 1),
			errContains: "HTTPS or loopback HTTP",
		},
		{
			name:        "negative duration",
			input:       strings.Replace(validSecurityJSON, `"cacheTtl": "15m"`, `"cacheTtl": "-5s"`, 1),
			errContains: "must be greater than zero",
		},
		{
			name:        "zero duration",
			input:       strings.Replace(validSecurityJSON, `"httpTimeout": "5s"`, `"httpTimeout": "0s"`, 1),
			errContains: "must be greater than zero",
		},
		{
			name:        "unparseable duration",
			input:       strings.Replace(validSecurityJSON, `"clockSkew": "30s"`, `"clockSkew": "not-a-duration"`, 1),
			errContains: "invalid jwt.clockSkew duration",
		},
		{
			name:        "duplicate algorithm",
			input:       strings.Replace(validSecurityJSON, `["RS256"]`, `["RS256", "RS256"]`, 1),
			errContains: "duplicate jwt algorithm",
		},
		{
			name:        "issuer mismatch",
			input:       strings.Replace(validSecurityJSON, `"issuer": "https://issuer.example"`, `"issuer": "https://other.example"`, 1),
			errContains: "does not match jwt.issuer",
		},
		{
			name: "duplicate subject",
			input: strings.Replace(validSecurityJSON, `"roles": ["publisher"]`, `"roles": ["publisher"]}, {
      "issuer": "https://issuer.example",
      "subject": "device-1",
      "tenantId": "tenant-a",
      "credentialId": "device-2-key",
      "allowedClientIds": ["device-1"],
      "roles": ["publisher"]`, 1),
			errContains: "duplicate identity subject",
		},
		{
			name: "duplicate credentialId",
			input: strings.Replace(validSecurityJSON, `"roles": ["publisher"]`, `"roles": ["publisher"]}, {
      "issuer": "https://issuer.example",
      "subject": "device-2",
      "tenantId": "tenant-a",
      "credentialId": "device-1-key",
      "allowedClientIds": ["device-2"],
      "roles": ["publisher"]`, 1),
			errContains: "duplicate credentialId",
		},
		{
			name:        "invalid tenant ID",
			input:       strings.Replace(validSecurityJSON, `"tenantId": "tenant-a"`, `"tenantId": ""`, 1),
			errContains: "invalid tenant ID",
		},
		{
			name:        "invalid ACL effect",
			input:       strings.Replace(validSecurityJSON, `"effect": "allow"`, `"effect": "permit"`, 1),
			errContains: "must be allow or deny",
		},
		{
			name:        "invalid ACL action",
			input:       strings.Replace(validSecurityJSON, `"action": "publish"`, `"action": "forward"`, 1),
			errContains: "invalid ACL action",
		},
		{
			name:        "malformed topic filter",
			input:       strings.Replace(validSecurityJSON, `"topicFilter": "sensors/#"`, `"topicFilter": "sensors/#/extra"`, 1),
			errContains: "invalid ACL topicFilter",
		},
		{
			name: "duplicate ACL rule",
			input: strings.Replace(validSecurityJSON, `"topicFilter": "sensors/#"`, `"topicFilter": "sensors/#"}, {
      "tenantId": "tenant-a",
      "role": "publisher",
      "effect": "allow",
      "action": "publish",
      "topicFilter": "sensors/#"`, 1),
			errContains: "duplicate",
		},
		{
			name:        "missing issuer",
			input:       strings.Replace(validSecurityJSON, `"issuer": "https://issuer.example"`, `"issuer": ""`, 1),
			errContains: "jwt.issuer is required",
		},
		{
			name:        "missing audience",
			input:       strings.Replace(validSecurityJSON, `"audience": "narumq"`, `"audience": ""`, 1),
			errContains: "jwt.audience is required",
		},
		{
			name:        "empty algorithms",
			input:       strings.Replace(validSecurityJSON, `["RS256"]`, `[]`, 1),
			errContains: "jwt.algorithms is required",
		},
		{
			name:        "empty algorithm element",
			input:       strings.Replace(validSecurityJSON, `["RS256"]`, `[""]`, 1),
			errContains: "jwt algorithm must not be empty",
		},
		{
			name:        "relative jwksUrl",
			input:       strings.Replace(validSecurityJSON, "https://issuer.example/.well-known/jwks.json", "/relative/path", 1),
			errContains: "absolute URL",
		},
		{
			name:        "empty identities",
			input:       `{"jwt": {"issuer": "https://issuer.example", "audience": "narumq", "algorithms": ["RS256"], "jwksUrl": "https://issuer.example/jwks", "cacheTtl": "1m", "refreshCooldown": "1m", "clockSkew": "1m", "httpTimeout": "1m"}, "identities": []}`,
			errContains: "identities must contain at least one binding",
		},
		{
			name:        "empty subject",
			input:       strings.Replace(validSecurityJSON, `"subject": "device-1"`, `"subject": ""`, 1),
			errContains: "identity subject is required",
		},
		{
			name:        "empty credentialId",
			input:       strings.Replace(validSecurityJSON, `"credentialId": "device-1-key"`, `"credentialId": ""`, 1),
			errContains: "identity credentialId is required",
		},
		{
			name:        "empty allowedClientIds",
			input:       strings.Replace(validSecurityJSON, `["device-1"]`, `[]`, 1),
			errContains: "must have at least one allowed Client ID",
		},
		{
			name:        "empty client id in list",
			input:       strings.Replace(validSecurityJSON, `["device-1"]`, `[""]`, 1),
			errContains: "empty allowed Client ID",
		},
		{
			name:        "duplicate client id in list",
			input:       strings.Replace(validSecurityJSON, `["device-1"]`, `["d", "d"]`, 1),
			errContains: "duplicate allowed Client ID",
		},
		{
			name:        "empty roles list",
			input:       strings.Replace(validSecurityJSON, `["publisher"]`, `[]`, 1),
			errContains: "must have at least one role",
		},
		{
			name:        "empty role in list",
			input:       strings.Replace(validSecurityJSON, `["publisher"]`, `[""]`, 1),
			errContains: "empty role",
		},
		{
			name:        "duplicate role in list",
			input:       strings.Replace(validSecurityJSON, `["publisher"]`, `["p", "p"]`, 1),
			errContains: "duplicate role",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSecurityConfig(strings.NewReader(tc.input))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("error = %v, want substring %q", err, tc.errContains)
			}
		})
	}
}

func TestParseSecurityConfig_NilReader(t *testing.T) {
	if _, err := parseSecurityConfig(nil); err == nil {
		t.Fatal("expected error for nil reader")
	}
}

const validMTLSSecurityJSON = `{
  "mtlsIdentities": [
    {
      "identityKind": "commonName",
      "identity": "device-1.example",
      "tenantId": "tenant-a",
      "subject": "device-1",
      "credentialId": "device-1-cert",
      "allowedClientIds": ["device-1"],
      "roles": ["publisher"]
    }
  ],
  "acl": [
    {
      "tenantId": "tenant-a",
      "role": "publisher",
      "effect": "allow",
      "action": "publish",
      "topicFilter": "sensors/#"
    }
  ]
}`

func TestParseSecurityConfig_MTLSOnly(t *testing.T) {
	bundle, err := parseSecurityConfig(strings.NewReader(validMTLSSecurityJSON))
	if err != nil {
		t.Fatalf("parseSecurityConfig() unexpected error = %v", err)
	}
	if bundle.JWTConfig != nil {
		t.Fatal("JWTConfig is non-nil for mTLS-only configuration")
	}
	if len(bundle.MTLSBindings) != 1 {
		t.Fatalf("MTLSBindings length = %d, want 1", len(bundle.MTLSBindings))
	}
	binding := bundle.MTLSBindings[0]
	if binding.Kind != "commonName" || binding.Value != "device-1.example" {
		t.Fatalf("mTLS selector = (%q, %q), want (commonName, device-1.example)", binding.Kind, binding.Value)
	}
}

func TestParseSecurityConfig_CombinedAuthentication(t *testing.T) {
	input := strings.Replace(
		validSecurityJSON,
		`  "acl": [`,
		`  "mtlsIdentities": [{"identityKind": "dnsSan", "identity": "device-1.example", "tenantId": "tenant-a", "subject": "device-1-mtls", "credentialId": "device-1-cert", "allowedClientIds": ["device-1-mtls"], "roles": ["publisher"]}],
  "acl": [`,
		1,
	)
	bundle, err := parseSecurityConfig(strings.NewReader(input))
	if err != nil {
		t.Fatalf("parseSecurityConfig() unexpected error = %v", err)
	}
	if bundle.JWTConfig == nil || len(bundle.JWTConfig.Bindings) != 1 {
		t.Fatalf("JWTConfig = %#v, want one JWT binding", bundle.JWTConfig)
	}
	if len(bundle.MTLSBindings) != 1 {
		t.Fatalf("MTLSBindings length = %d, want 1", len(bundle.MTLSBindings))
	}
}

func TestParseSecurityConfig_MTLSValidation(t *testing.T) {
	cases := []struct {
		name        string
		input       string
		errContains string
	}{
		{
			name:        "unknown identity kind",
			input:       strings.Replace(validMTLSSecurityJSON, `"commonName"`, `"subject"`, 1),
			errContains: "invalid mTLS identityKind",
		},
		{
			name: "duplicate selector",
			input: strings.Replace(
				validMTLSSecurityJSON,
				`    }
  ],
  "acl":`,
				`    },
    {
      "identityKind": "commonName",
      "identity": "device-1.example",
      "tenantId": "tenant-a",
      "subject": "device-1-2",
      "credentialId": "device-1-cert-2",
      "allowedClientIds": ["device-1-2"],
      "roles": ["publisher"]
    }
  ],
  "acl":`,
				1,
			),
			errContains: "duplicate mTLS identity selector",
		},
		{
			name:        "duplicate credential across methods",
			input:       strings.Replace(validMTLSSecurityJSON, `"device-1-cert"`, `"device-1-key"`, 1),
			errContains: "duplicate credentialId",
		},
		{
			name:        "no authentication method",
			input:       `{"acl": []}`,
			errContains: "at least one authentication method is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := tc.input
			if tc.name == "duplicate credential across methods" {
				input = strings.Replace(
					validSecurityJSON,
					`  "acl": [`,
					`  "mtlsIdentities": [{"identityKind": "commonName", "identity": "device-1.example", "tenantId": "tenant-a", "subject": "device-1-mtls", "credentialId": "device-1-key", "allowedClientIds": ["device-1-mtls"], "roles": ["publisher"]}],
  "acl": [`,
					1,
				)
			}
			_, err := parseSecurityConfig(strings.NewReader(input))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("error = %v, want substring %q", err, tc.errContains)
			}
		})
	}
}
func tenantLimitsSecurityJSON(value string) string {
	return strings.Replace(validSecurityJSON, `  "acl": [`, `  "tenantLimits": `+value+`,
  "acl": [`, 1)
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestParseSecurityConfig_TenantLimitsInheritance(t *testing.T) {
	input := tenantLimitsSecurityJSON(`{"default":{"maxConnections":10,"maxSessions":20,"maxSubscriptions":30,"messagesPerSecond":40,"bytesPerSecond":50,"maxPayloadBytes":60},"tenants":[{"tenantId":"tenant-a","maxConnections":0,"messagesPerSecond":7}]}`)
	bundle, err := parseSecurityConfig(strings.NewReader(input))
	if err != nil {
		t.Fatalf("parseSecurityConfig() error = %v", err)
	}
	if bundle.TenantLimits == nil {
		t.Fatal("TenantLimits = nil")
	}
	if bundle.TenantLimits.MaxWait != time.Second {
		t.Fatalf("MaxWait = %s, want 1s", bundle.TenantLimits.MaxWait)
	}
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	got := bundle.TenantLimits.LimitsFor(tenantID)
	if got.MaxConnections != 0 || got.MaxSessions != 20 || got.MaxSubscriptions != 30 ||
		got.MessagesPerSecond != 7 || got.BytesPerSecond != 50 || got.MaxPayloadBytes != 60 {
		t.Fatalf("tenant-a limits = %+v, want inherited values with explicit zero and override", got)
	}
	if got := bundle.TenantLimits.LimitsFor(tenancy.TenantID{}); got.MaxConnections != 10 || got.MessagesPerSecond != 40 {
		t.Fatalf("default limits = %+v", got)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestParseSecurityConfig_TenantLimitsRejectInvalidProfiles(t *testing.T) {
	tooMany := `{"tenants":[` + strings.TrimSuffix(strings.Repeat(`{"tenantId":"tenant-a"},`, 4097), ",") + `]}`
	cases := []struct {
		name, value string
	}{
		{"unknown field", `{"unexpected":1}`},
		{"unknown default field", `{"default":{"unexpected":1}}`},
		{"unknown override field", `{"tenants":[{"tenantId":"tenant-a","unexpected":1}]}`},
		{"missing tenant id", `{"tenants":[{}]}`},
		{"session out of range", `{"default":{"maxSessions":1000001}}`},
		{"subscription out of range", `{"default":{"maxSubscriptions":1000001}}`},
		{"message rate out of range", `{"tenants":[{"tenantId":"tenant-a","messagesPerSecond":1000001}]}`},
		{"invalid tenant id", `{"tenants":[{"tenantId":"bad\u0000tenant"}]}`},
		{"duplicate tenant", `{"tenants":[{"tenantId":"tenant-a"},{"tenantId":"tenant-a"}]}`},
		{"count out of range", `{"default":{"maxConnections":1000001}}`},
		{"byte rate out of range", `{"default":{"bytesPerSecond":1099511627777}}`},
		{"payload out of range", `{"default":{"maxPayloadBytes":268435456}}`},
		{"negative value", `{"default":{"messagesPerSecond":-1}}`},
		{"wait too large", `{"maxWait":"11s"}`},
		{"negative wait", `{"maxWait":"-1s"}`},
		{"malformed wait", `{"maxWait":"abc"}`},
		{"too many tenants", tooMany},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseSecurityConfig(strings.NewReader(tenantLimitsSecurityJSON(tc.value))); err == nil {
				t.Fatal("expected tenantLimits validation error")
			}
		})
	}
}
