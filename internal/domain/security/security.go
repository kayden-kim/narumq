// Package security owns immutable authenticated principals and tenant-local ACL policy.
package security

import (
	"errors"
	"time"

	"narumq/internal/domain/tenancy"
)

var (
	// ErrInvalidPrincipal means that an authenticated principal is incomplete or invalid.
	ErrInvalidPrincipal = errors.New("invalid security principal")
	// ErrInvalidRule means that an ACL rule is incomplete or invalid.
	ErrInvalidRule = errors.New("invalid ACL rule")
	// ErrDuplicateRule means that a policy contains the same rule more than once.
	ErrDuplicateRule = errors.New("duplicate ACL rule")
	// ErrInvalidAction means that an unsupported ACL action was supplied.
	ErrInvalidAction = errors.New("invalid ACL action")
	// ErrInvalidEffect means that an unsupported ACL effect was supplied.
	ErrInvalidEffect = errors.New("invalid ACL effect")
	// ErrNotAuthorized means that a principal is not allowed to perform an action.
	ErrNotAuthorized = errors.New("not authorized")
)

// Role identifies one configured authorization role.
type Role string

// Action identifies one MQTT authorization operation.
type Action string

const (
	// ActionPublish authorizes publication to a concrete topic.
	ActionPublish Action = "publish"
	// ActionSubscribe authorizes creation of a subscription filter.
	ActionSubscribe Action = "subscribe"
	// ActionUnsubscribe authorizes removal of a subscription filter.
	ActionUnsubscribe Action = "unsubscribe"
	// ActionWill authorizes publication of a Will message.
	ActionWill Action = "will"
)

// Effect identifies whether an ACL rule permits or denies an action.
type Effect string

const (
	// EffectAllow permits an action when no matching deny applies.
	EffectAllow Effect = "allow"
	// EffectDeny explicitly rejects an action.
	EffectDeny Effect = "deny"
)

// Rule is one immutable tenant-local role ACL rule.
type Rule struct {
	tenantID    tenancy.TenantID
	role        Role
	effect      Effect
	action      Action
	topicFilter tenancy.TopicFilter
}

// NewRule validates and constructs an ACL rule.
func NewRule(tenantID tenancy.TenantID, role Role, effect Effect, action Action, topicFilter tenancy.TopicFilter) (Rule, error) {
	if role == "" || tenantID == (tenancy.TenantID{}) || tenantID != topicFilter.TenantID() {
		return Rule{}, ErrInvalidRule
	}
	if _, err := tenancy.NewTopicFilter(tenantID, topicFilter.Filter()); err != nil {
		return Rule{}, ErrInvalidRule
	}
	if effect != EffectAllow && effect != EffectDeny {
		return Rule{}, ErrInvalidEffect
	}
	if !validAction(action) {
		return Rule{}, ErrInvalidAction
	}
	return Rule{
		tenantID:    tenantID,
		role:        role,
		effect:      effect,
		action:      action,
		topicFilter: topicFilter,
	}, nil
}

// TenantID returns the rule tenant.
func (rule Rule) TenantID() tenancy.TenantID {
	return rule.tenantID
}

// Role returns the role to which the rule applies.
func (rule Rule) Role() Role {
	return rule.role
}

// Effect returns the rule effect.
func (rule Rule) Effect() Effect {
	return rule.effect
}

// Action returns the rule action.
func (rule Rule) Action() Action {
	return rule.action
}

// TopicFilter returns the rule's tenant-scoped filter.
func (rule Rule) TopicFilter() tenancy.TopicFilter {
	return rule.topicFilter
}

// Principal is an immutable authenticated MQTT identity snapshot.
type Principal struct {
	tenantID     tenancy.TenantID
	clientID     string
	subject      string
	credentialID string
	roles        []Role
	expiresAt    time.Time
}

// NewPrincipal validates and constructs an immutable principal snapshot.
func NewPrincipal(tenantID tenancy.TenantID, clientID, subject, credentialID string, roles []Role, expiresAt time.Time) (Principal, error) {
	if _, err := tenancy.NewSessionKey(tenantID, clientID); err != nil {
		return Principal{}, ErrInvalidPrincipal
	}
	if subject == "" || credentialID == "" || expiresAt.IsZero() {
		return Principal{}, ErrInvalidPrincipal
	}
	copiedRoles := make([]Role, len(roles))
	seen := make(map[Role]struct{}, len(roles))
	for index, role := range roles {
		if role == "" {
			return Principal{}, ErrInvalidPrincipal
		}
		if _, exists := seen[role]; exists {
			return Principal{}, ErrInvalidPrincipal
		}
		seen[role] = struct{}{}
		copiedRoles[index] = role
	}
	return Principal{
		tenantID:     tenantID,
		clientID:     clientID,
		subject:      subject,
		credentialID: credentialID,
		roles:        copiedRoles,
		expiresAt:    expiresAt,
	}, nil
}

// TenantID returns the principal tenant.
func (principal Principal) TenantID() tenancy.TenantID {
	return principal.tenantID
}

// ClientID returns the principal Client ID.
func (principal Principal) ClientID() string {
	return principal.clientID
}

// Subject returns the authenticated subject.
func (principal Principal) Subject() string {
	return principal.subject
}

// CredentialID returns the non-secret credential identifier.
func (principal Principal) CredentialID() string {
	return principal.credentialID
}

// Roles returns a copy of the principal roles.
func (principal Principal) Roles() []Role {
	return append([]Role(nil), principal.roles...)
}

// ExpiresAt returns the credential expiry time.
func (principal Principal) ExpiresAt() time.Time {
	return principal.expiresAt
}

// Matches reports whether the principal owns the supplied tenant-scoped session key.
func (principal Principal) Matches(sessionKey tenancy.SessionKey) bool {
	return principal.tenantID == sessionKey.TenantID() && principal.clientID == sessionKey.ClientID()
}

// Policy evaluates immutable tenant-local ACL rules.
type Policy struct {
	rules []Rule
}

// NewPolicy validates and constructs an immutable ACL policy.
func NewPolicy(rules []Rule) (Policy, error) {
	copiedRules := append([]Rule(nil), rules...)
	seen := make(map[Rule]struct{}, len(copiedRules))
	for _, rule := range copiedRules {
		if rule.role == "" || !rule.tenantIDValid() || !rule.topicFilterValid() || !validAction(rule.action) || !validEffect(rule.effect) {
			return Policy{}, ErrInvalidRule
		}
		if _, exists := seen[rule]; exists {
			return Policy{}, ErrDuplicateRule
		}
		seen[rule] = struct{}{}
	}
	return Policy{rules: copiedRules}, nil
}

// AuthorizeTopic evaluates publish or Will authorization for a concrete topic.
func (policy Policy) AuthorizeTopic(principal Principal, action Action, topic tenancy.Topic) error {
	if action != ActionPublish && action != ActionWill {
		return ErrInvalidAction
	}
	if !principal.MatchesKeyTenant(topic.TenantID()) {
		return ErrNotAuthorized
	}
	matchedAllow := false
	for _, rule := range policy.rules {
		if !policyRuleApplies(rule, principal, action) {
			continue
		}
		matched, err := tenancy.TopicMatches(topic, rule.topicFilter)
		if err != nil {
			if errors.Is(err, tenancy.ErrTenantMismatch) {
				continue
			}
			return ErrNotAuthorized
		}
		if !matched {
			continue
		}
		if rule.effect == EffectDeny {
			return ErrNotAuthorized
		}
		matchedAllow = true
	}
	if !matchedAllow {
		return ErrNotAuthorized
	}
	return nil
}

// AuthorizeFilter evaluates subscribe or unsubscribe authorization for a topic filter.
func (policy Policy) AuthorizeFilter(principal Principal, action Action, filter tenancy.TopicFilter) error {
	if action != ActionSubscribe && action != ActionUnsubscribe {
		return ErrInvalidAction
	}
	if !principal.MatchesKeyTenant(filter.TenantID()) {
		return ErrNotAuthorized
	}
	matchedAllow := false
	for _, rule := range policy.rules {
		if !policyRuleApplies(rule, principal, action) {
			continue
		}
		intersects, err := tenancy.FiltersIntersect(rule.topicFilter, filter)
		if err != nil {
			if errors.Is(err, tenancy.ErrTenantMismatch) {
				continue
			}
			return ErrNotAuthorized
		}
		if rule.effect == EffectDeny && intersects {
			return ErrNotAuthorized
		}
		if rule.effect == EffectAllow {
			contains, err := tenancy.FilterContains(rule.topicFilter, filter)
			if err != nil {
				if errors.Is(err, tenancy.ErrTenantMismatch) {
					continue
				}
				return ErrNotAuthorized
			}
			if contains {
				matchedAllow = true
			}
		}
	}
	if !matchedAllow {
		return ErrNotAuthorized
	}
	return nil
}

func (rule Rule) tenantIDValid() bool {
	_, err := tenancy.NewSessionKey(rule.tenantID, "security-rule")
	return err == nil
}

func (rule Rule) topicFilterValid() bool {
	if rule.tenantID == (tenancy.TenantID{}) {
		return false
	}
	_, err := tenancy.NewTopicFilter(rule.tenantID, rule.topicFilter.Filter())
	return err == nil && rule.topicFilter.TenantID() == rule.tenantID
}

func (principal Principal) MatchesKeyTenant(tenantID tenancy.TenantID) bool {
	return principal.tenantID == tenantID && principal.clientID != ""
}

func policyRuleApplies(rule Rule, principal Principal, action Action) bool {
	if rule.tenantID != principal.tenantID || rule.action != action {
		return false
	}
	for _, role := range principal.roles {
		if role == rule.role {
			return true
		}
	}
	return false
}

func validAction(action Action) bool {
	switch action {
	case ActionPublish, ActionSubscribe, ActionUnsubscribe, ActionWill:
		return true
	default:
		return false
	}
}

func validEffect(effect Effect) bool {
	return effect == EffectAllow || effect == EffectDeny
}
