package security

import (
	"errors"
	"testing"
	"time"

	"narumq/internal/domain/tenancy"
)

func newSecurityFixture(t *testing.T) (tenancy.TenantID, Principal) {
	t.Helper()
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := NewPrincipal(tenant, "device-1", "subject-1", "credential-1", []Role{"publisher", "auditor"}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	return tenant, principal
}

func newSecurityRule(t *testing.T, tenant tenancy.TenantID, role Role, effect Effect, action Action, rawFilter string) Rule {
	t.Helper()
	filter, err := tenancy.NewTopicFilter(tenant, rawFilter)
	if err != nil {
		t.Fatal(err)
	}
	rule, err := NewRule(tenant, role, effect, action, filter)
	if err != nil {
		t.Fatal(err)
	}
	return rule
}

// Requirements: TEN-04, SEC-06
func TestPolicyExplicitDenyWinsAcrossAllPrincipalRoles(t *testing.T) {
	tenant, principal := newSecurityFixture(t)
	policy, err := NewPolicy([]Rule{
		newSecurityRule(t, tenant, "publisher", EffectAllow, ActionPublish, "sensors/#"),
		newSecurityRule(t, tenant, "auditor", EffectDeny, ActionPublish, "sensors/private/#"),
		newSecurityRule(t, tenant, "publisher", EffectAllow, ActionSubscribe, "sensors/#"),
		newSecurityRule(t, tenant, "auditor", EffectDeny, ActionSubscribe, "sensors/private/#"),
	})
	if err != nil {
		t.Fatal(err)
	}
	publicTopic, err := tenancy.NewTopic(tenant, "sensors/public/temperature")
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.AuthorizeTopic(principal, ActionPublish, publicTopic); err != nil {
		t.Fatalf("public publish denied: %v", err)
	}
	privateTopic, err := tenancy.NewTopic(tenant, "sensors/private/temperature")
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.AuthorizeTopic(principal, ActionPublish, privateTopic); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("private publish error = %v, want %v", err, ErrNotAuthorized)
	}
	privateFilter, err := tenancy.NewTopicFilter(tenant, "sensors/private/+")
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.AuthorizeFilter(principal, ActionSubscribe, privateFilter); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("private subscribe error = %v, want %v", err, ErrNotAuthorized)
	}
}

// Requirements: TEN-04, SEC-06
func TestPolicyUsesFilterContainmentAndDefaultDeny(t *testing.T) {
	tenant, principal := newSecurityFixture(t)
	policy, err := NewPolicy([]Rule{
		newSecurityRule(t, tenant, "publisher", EffectAllow, ActionSubscribe, "events/#"),
		newSecurityRule(t, tenant, "publisher", EffectAllow, ActionUnsubscribe, "events/#"),
	})
	if err != nil {
		t.Fatal(err)
	}
	narrowFilter, err := tenancy.NewTopicFilter(tenant, "events/room/+")
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.AuthorizeFilter(principal, ActionSubscribe, narrowFilter); err != nil {
		t.Fatalf("contained filter denied: %v", err)
	}
	broadFilter, err := tenancy.NewTopicFilter(tenant, "#")
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.AuthorizeFilter(principal, ActionSubscribe, broadFilter); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("uncontained filter error = %v, want %v", err, ErrNotAuthorized)
	}
	unlistedTopic, err := tenancy.NewTopic(tenant, "private/secret")
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.AuthorizeTopic(principal, ActionPublish, unlistedTopic); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("default-deny publish error = %v, want %v", err, ErrNotAuthorized)
	}
}

// Requirements: TEN-02, TEN-04
func TestPrincipalMustMatchSessionTenantAndClient(t *testing.T) {
	tenant, principal := newSecurityFixture(t)
	matchingKey, err := tenancy.NewSessionKey(tenant, "device-1")
	if err != nil {
		t.Fatal(err)
	}
	if !principal.Matches(matchingKey) {
		t.Fatal("principal did not match its session key")
	}
	otherTenant, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	otherKey, err := tenancy.NewSessionKey(otherTenant, "device-1")
	if err != nil {
		t.Fatal(err)
	}
	if principal.Matches(otherKey) {
		t.Fatal("principal matched a cross-tenant session key")
	}
	otherClient, err := tenancy.NewSessionKey(tenant, "device-2")
	if err != nil {
		t.Fatal(err)
	}
	if principal.Matches(otherClient) {
		t.Fatal("principal matched a different Client ID")
	}
}

// Requirements: TEN-04, SEC-06
func TestPrincipalRolesAreDeepCopied(t *testing.T) {
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	roles := []Role{"publisher"}
	principal, err := NewPrincipal(tenant, "device-1", "subject-1", "credential-1", roles, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	roles[0] = "administrator"
	stored := principal.Roles()
	stored[0] = "administrator"
	if principal.Roles()[0] != "publisher" {
		t.Fatalf("principal roles were not immutable: %#v", principal.Roles())
	}
}

// Requirements: TEN-04, SEC-06
func TestNewRuleRejectsUnconstructedTenantAndFilter(t *testing.T) {
	if _, err := NewRule(tenancy.TenantID{}, "publisher", EffectAllow, ActionPublish, tenancy.TopicFilter{}); !errors.Is(err, ErrInvalidRule) {
		t.Fatalf("zero-value NewRule() error = %v, want %v", err, ErrInvalidRule)
	}
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRule(tenant, "publisher", EffectAllow, ActionPublish, tenancy.TopicFilter{}); !errors.Is(err, ErrInvalidRule) {
		t.Fatalf("unconstructed filter NewRule() error = %v, want %v", err, ErrInvalidRule)
	}
}

func TestRuleAndPrincipalAccessors(t *testing.T) {
	tenant, principal := newSecurityFixture(t)
	rule := newSecurityRule(t, tenant, "publisher", EffectAllow, ActionPublish, "sensors/#")
	if rule.TenantID() != tenant {
		t.Fatalf("TenantID = %v, want %v", rule.TenantID(), tenant)
	}
	if rule.Role() != "publisher" {
		t.Fatalf("Role = %v, want publisher", rule.Role())
	}
	if rule.Effect() != EffectAllow {
		t.Fatalf("Effect = %v, want allow", rule.Effect())
	}
	if rule.Action() != ActionPublish {
		t.Fatalf("Action = %v, want publish", rule.Action())
	}
	if rule.TopicFilter().Filter() != "sensors/#" {
		t.Fatalf("TopicFilter = %v, want sensors/#", rule.TopicFilter().Filter())
	}
	if principal.CredentialID() != "credential-1" {
		t.Fatalf("CredentialID = %v, want credential-1", principal.CredentialID())
	}
	if principal.ExpiresAt() != time.Unix(100, 0) {
		t.Fatalf("ExpiresAt = %v, want %v", principal.ExpiresAt(), time.Unix(100, 0))
	}
}

func TestSecurityValidationErrors(t *testing.T) {
	tenantA, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	filterA, err := tenancy.NewTopicFilter(tenantA, "sensors/#")
	if err != nil {
		t.Fatal(err)
	}
	filterB, err := tenancy.NewTopicFilter(tenantB, "sensors/#")
	if err != nil {
		t.Fatal(err)
	}

	// NewRule error branches
	if _, err := NewRule(tenantA, "", EffectAllow, ActionPublish, filterA); !errors.Is(err, ErrInvalidRule) {
		t.Fatalf("empty role NewRule error = %v", err)
	}
	if _, err := NewRule(tenantA, "pub", EffectAllow, ActionPublish, filterB); !errors.Is(err, ErrInvalidRule) {
		t.Fatalf("tenant mismatch NewRule error = %v", err)
	}
	if _, err := NewRule(tenantA, "pub", "invalid_effect", ActionPublish, filterA); !errors.Is(err, ErrInvalidEffect) {
		t.Fatalf("invalid effect NewRule error = %v", err)
	}
	if _, err := NewRule(tenantA, "pub", EffectAllow, "invalid_action", filterA); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("invalid action NewRule error = %v", err)
	}

	// NewPrincipal error branches
	validRoles := []Role{"publisher"}
	now := time.Now()
	if _, err := NewPrincipal(tenancy.TenantID{}, "client", "sub", "cred", validRoles, now); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("zero tenant NewPrincipal error = %v", err)
	}
	if _, err := NewPrincipal(tenantA, "", "sub", "cred", validRoles, now); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("empty client NewPrincipal error = %v", err)
	}
	if _, err := NewPrincipal(tenantA, "client", "", "cred", validRoles, now); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("empty subject NewPrincipal error = %v", err)
	}
	if _, err := NewPrincipal(tenantA, "client", "sub", "", validRoles, now); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("empty cred NewPrincipal error = %v", err)
	}
	if _, err := NewPrincipal(tenantA, "client", "sub", "cred", validRoles, time.Time{}); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("zero time NewPrincipal error = %v", err)
	}
	if _, err := NewPrincipal(tenantA, "client", "sub", "cred", []Role{""}, now); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("empty role NewPrincipal error = %v", err)
	}
	if _, err := NewPrincipal(tenantA, "client", "sub", "cred", []Role{"r", "r"}, now); !errors.Is(err, ErrInvalidPrincipal) {
		t.Fatalf("duplicate role NewPrincipal error = %v", err)
	}

	// AuthorizeTopic and AuthorizeFilter invalid actions
	ruleA, _ := NewRule(tenantA, "publisher", EffectAllow, ActionPublish, filterA)
	policy, _ := NewPolicy([]Rule{ruleA})
	principal, _ := NewPrincipal(tenantA, "client", "sub", "cred", validRoles, now)
	topicA, _ := tenancy.NewTopic(tenantA, "sensors/temp")

	if err := policy.AuthorizeTopic(principal, ActionSubscribe, topicA); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("AuthorizeTopic with ActionSubscribe error = %v", err)
	}
	if err := policy.AuthorizeFilter(principal, ActionPublish, filterA); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("AuthorizeFilter with ActionPublish error = %v", err)
	}

	// Cross-tenant authorization
	principalB, _ := NewPrincipal(tenantB, "client", "sub", "cred", validRoles, now)
	if err := policy.AuthorizeTopic(principalB, ActionPublish, topicA); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("cross-tenant AuthorizeTopic error = %v", err)
	}
	if err := policy.AuthorizeFilter(principalB, ActionSubscribe, filterA); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("cross-tenant AuthorizeFilter error = %v", err)
	}
}
