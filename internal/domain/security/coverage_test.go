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
