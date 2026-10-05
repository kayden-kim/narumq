package security

import (
	"errors"
	"testing"
	"time"

	"narumq/internal/domain/tenancy"
)

func TestCmmNewPolicyRejectsUnconstructedOrCorruptedRules(t *testing.T) {
	tenant, _ := newSecurityFixture(t)
	valid := newSecurityRule(t, tenant, "publisher", EffectAllow, ActionPublish, "sensors/#")
	otherTenant, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	foreignFilter, err := tenancy.NewTopicFilter(otherTenant, "sensors/#")
	if err != nil {
		t.Fatal(err)
	}
	badEffect := valid
	badEffect.effect = "audit"
	badAction := valid
	badAction.action = "connect"
	foreignTopicFilter := valid
	foreignTopicFilter.topicFilter = foreignFilter
	emptyRole := valid
	emptyRole.role = ""

	cases := []struct {
		name  string
		rules []Rule
	}{
		{name: "zero rule", rules: []Rule{{}}},
		{name: "zero rule after valid rule", rules: []Rule{valid, {}}},
		{name: "unsupported effect", rules: []Rule{badEffect}},
		{name: "unsupported action", rules: []Rule{badAction}},
		{name: "filter from another tenant", rules: []Rule{foreignTopicFilter}},
		{name: "empty role", rules: []Rule{emptyRole}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			policy, err := NewPolicy(testCase.rules)
			if !errors.Is(err, ErrInvalidRule) {
				t.Fatalf("NewPolicy() error = %v, want %v", err, ErrInvalidRule)
			}
			if len(policy.rules) != 0 {
				t.Fatalf("NewPolicy() returned %d rules on error, want 0", len(policy.rules))
			}
		})
	}
}

func TestCmmPolicyIgnoresRulesForRolesThePrincipalLacks(t *testing.T) {
	tenant, publisher := newSecurityFixture(t)
	policy, err := NewPolicy([]Rule{
		newSecurityRule(t, tenant, "publisher", EffectAllow, ActionPublish, "sensors/#"),
		newSecurityRule(t, tenant, "publisher", EffectAllow, ActionSubscribe, "sensors/#"),
	})
	if err != nil {
		t.Fatal(err)
	}
	outsider, err := NewPrincipal(tenant, "device-2", "subject-2", "credential-2", []Role{"subscriber", "operator"}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	topic, err := tenancy.NewTopic(tenant, "sensors/room")
	if err != nil {
		t.Fatal(err)
	}
	filter, err := tenancy.NewTopicFilter(tenant, "sensors/+")
	if err != nil {
		t.Fatal(err)
	}

	if err := policy.AuthorizeTopic(publisher, ActionPublish, topic); err != nil {
		t.Fatalf("publisher AuthorizeTopic() error = %v, want nil", err)
	}
	if err := policy.AuthorizeFilter(publisher, ActionSubscribe, filter); err != nil {
		t.Fatalf("publisher AuthorizeFilter() error = %v, want nil", err)
	}
	if err := policy.AuthorizeTopic(outsider, ActionPublish, topic); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("role-less AuthorizeTopic() error = %v, want %v", err, ErrNotAuthorized)
	}
	if err := policy.AuthorizeFilter(outsider, ActionSubscribe, filter); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("role-less AuthorizeFilter() error = %v, want %v", err, ErrNotAuthorized)
	}
}

func TestCmmPolicyHandlesCorruptedRuleFilters(t *testing.T) {
	tenant, principal := newSecurityFixture(t)
	otherTenant, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	foreignFilter, err := tenancy.NewTopicFilter(otherTenant, "sensors/#")
	if err != nil {
		t.Fatal(err)
	}
	topic, err := tenancy.NewTopic(tenant, "sensors/room")
	if err != nil {
		t.Fatal(err)
	}
	filter, err := tenancy.NewTopicFilter(tenant, "sensors/+")
	if err != nil {
		t.Fatal(err)
	}
	allowPublish := newSecurityRule(t, tenant, "publisher", EffectAllow, ActionPublish, "sensors/#")
	allowSubscribe := newSecurityRule(t, tenant, "publisher", EffectAllow, ActionSubscribe, "sensors/#")
	foreignDenyPublish := Rule{tenantID: tenant, role: "publisher", effect: EffectDeny, action: ActionPublish, topicFilter: foreignFilter}
	foreignDenySubscribe := Rule{tenantID: tenant, role: "publisher", effect: EffectDeny, action: ActionSubscribe, topicFilter: foreignFilter}
	zeroFilterPublish := Rule{tenantID: tenant, role: "publisher", effect: EffectAllow, action: ActionPublish}
	zeroFilterSubscribe := Rule{tenantID: tenant, role: "publisher", effect: EffectAllow, action: ActionSubscribe}

	t.Run("publish skips rule filter from another tenant", func(t *testing.T) {
		policy := Policy{rules: []Rule{foreignDenyPublish, allowPublish}}
		if err := policy.AuthorizeTopic(principal, ActionPublish, topic); err != nil {
			t.Fatalf("AuthorizeTopic() error = %v, want %v", err, nil)
		}
	})
	t.Run("subscribe skips rule filter from another tenant", func(t *testing.T) {
		policy := Policy{rules: []Rule{foreignDenySubscribe, allowSubscribe}}
		if err := policy.AuthorizeFilter(principal, ActionSubscribe, filter); err != nil {
			t.Fatalf("AuthorizeFilter() error = %v, want %v", err, nil)
		}
	})
	t.Run("publish fails closed on invalid rule filter", func(t *testing.T) {
		policy := Policy{rules: []Rule{zeroFilterPublish, allowPublish}}
		if err := policy.AuthorizeTopic(principal, ActionPublish, topic); !errors.Is(err, ErrNotAuthorized) {
			t.Fatalf("AuthorizeTopic() error = %v, want %v", err, ErrNotAuthorized)
		}
	})
	t.Run("subscribe fails closed on invalid rule filter", func(t *testing.T) {
		policy := Policy{rules: []Rule{zeroFilterSubscribe, allowSubscribe}}
		if err := policy.AuthorizeFilter(principal, ActionSubscribe, filter); !errors.Is(err, ErrNotAuthorized) {
			t.Fatalf("AuthorizeFilter() error = %v, want %v", err, ErrNotAuthorized)
		}
	})
}
