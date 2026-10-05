package subscribe

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"narumq/internal/application/tenantlimit"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type tenantLimitClock struct{}

func (tenantLimitClock) Now() time.Time { return time.Unix(100, 0) }

type tenantLimitReporter struct{ dimensions []tenantlimit.Dimension }

func (reporter *tenantLimitReporter) TenantLimitRejected(_ tenancy.TenantID, dimension tenantlimit.Dimension) {
	reporter.dimensions = append(reporter.dimensions, dimension)
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestSubscribeTenantQuota(t *testing.T) {
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV5, mqtt.ProtocolV311} {
		name := "v5"
		if version == mqtt.ProtocolV311 {
			name = "v311"
		}
		t.Run(name, func(t *testing.T) {
			tenant, _ := tenancy.NewTenantID("a")
			other, _ := tenancy.NewTenantID("b")
			now := time.Unix(100, 0)
			makePrincipal := func(id tenancy.TenantID, client string) security.Principal {
				principal, err := security.NewPrincipal(id, client, "subject", "cred", []security.Role{"client"}, now.Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				return principal
			}
			var rules []security.Rule
			for _, id := range []tenancy.TenantID{tenant, other} {
				filter, _ := tenancy.NewTopicFilter(id, "allowed/#")
				rule, err := security.NewRule(id, "client", security.EffectAllow, security.ActionSubscribe, filter)
				if err != nil {
					t.Fatal(err)
				}
				rules = append(rules, rule)
			}
			policy, err := security.NewPolicy(rules)
			if err != nil {
				t.Fatal(err)
			}
			routes := routing.NewTable()
			if err := routes.BindSessionTenant("a/client", tenant); err != nil {
				t.Fatal(err)
			}
			if err := routes.BindSessionTenant("b/client", other); err != nil {
				t.Fatal(err)
			}
			reporter := &tenantLimitReporter{}
			limits, err := tenantlimit.New(tenantLimitClock{}, tenantlimit.Profile{Default: tenantlimit.Limits{MaxSubscriptions: 2}}, reporter)
			if err != nil {
				t.Fatal(err)
			}
			queue := &recordingQueue{}
			committer := &cmmCommitter{}
			useCase, err := New(routes, queue, &recordingRetained{}, WithPolicy(policy), WithTenantLimits(limits), WithCommitter(committer))
			if err != nil {
				t.Fatal(err)
			}
			ownership := SessionOwnership{TenantID: "a", ClientID: "client", OwnerNodeID: 1, Epoch: 1}
			principal := makePrincipal(tenant, "client")
			packet := mqtt.Subscribe{Version: version, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "allowed/one"}, {Filter: "$share/group/allowed/two", QoS: 1}, {Filter: "allowed/three"}, {Filter: "forbidden/four"}}}
			result, err := useCase.SubscribeCommitted(context.Background(), "a/client", packet, now, ownership, principal)
			if err != nil {
				t.Fatal(err)
			}
			quota, acl := byte(0x97), byte(0x87)
			if version == mqtt.ProtocolV311 {
				quota, acl = 0x80, 0x80
			}
			if !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{0, 1, quota, acl}) {
				t.Fatal(result.Ack.ReasonCodes)
			}
			if len(committer.commits) != 2 || committer.commits[0].filter != "allowed/one" || committer.commits[1].filter != "$share/group/allowed/two" {
				t.Fatal(committer.commits)
			}
			if _, exists := routes.Lookup("a/client", "allowed/three"); exists {
				t.Fatal("refused filter entered route table")
			}
			if !reflect.DeepEqual(reporter.dimensions, []tenantlimit.Dimension{tenantlimit.DimensionSubscriptions}) {
				t.Fatal(reporter.dimensions)
			}
			// A rejected-only request must never submit a replicated mutation or replay.
			committer.commits = nil
			result, err = useCase.SubscribeCommitted(context.Background(), "a/client", mqtt.Subscribe{Version: version, PacketID: 2, Subscriptions: []mqtt.Subscription{{Filter: "allowed/rejected"}}}, now, ownership, principal)
			if err != nil || !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{quota}) || len(committer.commits) != 0 || len(queue.deliveries) != 0 {
				t.Fatal(result, err, committer.commits, queue.deliveries)
			}
			// Replacement is allowed at the limit and preserves the subscription count.
			result, err = useCase.SubscribeCommitted(context.Background(), "a/client", mqtt.Subscribe{Version: version, PacketID: 3, Subscriptions: []mqtt.Subscription{{Filter: "allowed/one", QoS: 2}}}, now, ownership, principal)
			if err != nil || !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{2}) || routes.TenantSubscriptionCount(tenant) != 2 {
				t.Fatal(result, err)
			}
			result, err = useCase.SubscribeCommitted(context.Background(), "b/client", mqtt.Subscribe{Version: version, PacketID: 4, Subscriptions: []mqtt.Subscription{{Filter: "allowed/other"}}}, now, SessionOwnership{TenantID: "b", ClientID: "client", OwnerNodeID: 1, Epoch: 1}, makePrincipal(other, "client"))
			if err != nil || !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{0}) {
				t.Fatal(result, err)
			}
		})
	}
}

type tenantQuotaBarrierCommitter struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (committer *tenantQuotaBarrierCommitter) CommitSubscription(
	ctx context.Context, _, _ string, _, _ uint64, _ string, _ uint32, _, _ bool, _, _ uint32,
) error {
	if committer.calls.Add(1) == 1 {
		close(committer.entered)
		select {
		case <-committer.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func tenantQuotaConcurrentFixture(t *testing.T, committer SubscriptionCommitter) (*UseCase, *routing.Table, tenancy.TenantID, security.Principal, security.Principal) {
	t.Helper()
	tenant, err := tenancy.NewTenantID("a")
	if err != nil {
		t.Fatal(err)
	}
	now := tenantLimitClock{}.Now()
	first, err := security.NewPrincipal(tenant, "first", "subject", "cred", []security.Role{"client"}, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	second, err := security.NewPrincipal(tenant, "second", "subject", "cred", []security.Role{"client"}, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	filter, err := tenancy.NewTopicFilter(tenant, "allowed/#")
	if err != nil {
		t.Fatal(err)
	}
	var rules []security.Rule
	for _, action := range []security.Action{security.ActionSubscribe, security.ActionUnsubscribe} {
		rule, err := security.NewRule(tenant, "client", security.EffectAllow, action, filter)
		if err != nil {
			t.Fatal(err)
		}
		rules = append(rules, rule)
	}
	policy, err := security.NewPolicy(rules)
	if err != nil {
		t.Fatal(err)
	}
	routes := routing.NewTable()
	for _, id := range []routing.SessionID{"a/first", "a/second"} {
		if err := routes.BindSessionTenant(id, tenant); err != nil {
			t.Fatal(err)
		}
	}
	limits, err := tenantlimit.New(tenantLimitClock{}, tenantlimit.Profile{Default: tenantlimit.Limits{MaxSubscriptions: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	useCase, err := New(routes, &recordingQueue{}, &recordingRetained{}, WithPolicy(policy), WithTenantLimits(limits), WithCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}
	return useCase, routes, tenant, first, second
}

type tenantQuotaSubscribeOutcome struct {
	result ResultDTO
	err    error
}

func tenantQuotaSubscribeAsync(ctx context.Context, useCase *UseCase, id routing.SessionID, principal security.Principal, filter string) <-chan tenantQuotaSubscribeOutcome {
	done := make(chan tenantQuotaSubscribeOutcome, 1)
	go func() {
		result, err := useCase.SubscribeCommitted(ctx, id, mqtt.Subscribe{
			Version: mqtt.ProtocolV5, PacketID: 1,
			Subscriptions: []mqtt.Subscription{{Filter: filter, RetainHandling: mqtt.DoNotSendRetained}},
		}, tenantLimitClock{}.Now(), SessionOwnership{TenantID: "a", ClientID: principal.ClientID(), OwnerNodeID: 1, Epoch: 1}, principal)
		done <- tenantQuotaSubscribeOutcome{result: result, err: err}
	}()
	return done
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestSubscribeTenantQuotaSerializesLocalAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	committer := &tenantQuotaBarrierCommitter{entered: make(chan struct{}), release: make(chan struct{})}
	useCase, routes, tenant, firstPrincipal, secondPrincipal := tenantQuotaConcurrentFixture(t, committer)
	firstDone := tenantQuotaSubscribeAsync(ctx, useCase, "a/first", firstPrincipal, "allowed/first")
	select {
	case <-committer.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	secondDone := tenantQuotaSubscribeAsync(ctx, useCase, "a/second", secondPrincipal, "allowed/second")
	var second tenantQuotaSubscribeOutcome
	secondFinished := false
	select {
	case second = <-secondDone:
		secondFinished = true
	case <-time.After(time.Second):
		// Release the first commit if local admission correctly holds the second.
	}
	close(committer.release)
	var first tenantQuotaSubscribeOutcome
	select {
	case first = <-firstDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !secondFinished {
		select {
		case second = <-secondDone:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if first.err != nil || second.err != nil || !reflect.DeepEqual(first.result.Ack.ReasonCodes, []byte{0}) || !reflect.DeepEqual(second.result.Ack.ReasonCodes, []byte{0x97}) {
		t.Fatalf("local quota admission: first=%v/%v second=%v/%v", first.result.Ack.ReasonCodes, first.err, second.result.Ack.ReasonCodes, second.err)
	}
	if count := routes.TenantSubscriptionCount(tenant); count != 1 || committer.calls.Load() != 1 {
		t.Fatalf("local subscription count=%d commits=%d, want 1/1", count, committer.calls.Load())
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
func TestSubscribeTenantQuotaSerializesReplacementAndRemoval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	committer := &tenantQuotaBarrierCommitter{entered: make(chan struct{}), release: make(chan struct{})}
	useCase, routes, tenant, firstPrincipal, secondPrincipal := tenantQuotaConcurrentFixture(t, committer)
	if _, err := routes.Subscribe("a/first", mqtt.Subscription{Filter: "allowed/old"}); err != nil {
		t.Fatal(err)
	}
	replacementDone := tenantQuotaSubscribeAsync(ctx, useCase, "a/first", firstPrincipal, "allowed/old")
	select {
	case <-committer.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	removed := make(chan error, 1)
	go func() {
		ack, err := useCase.UnsubscribeCommitted(ctx, "a/first", mqtt.Unsubscribe{Version: mqtt.ProtocolV5, PacketID: 2, Filters: []string{"allowed/old"}},
			SessionOwnership{TenantID: "a", ClientID: "first", OwnerNodeID: 1, Epoch: 1}, firstPrincipal)
		if err == nil && !reflect.DeepEqual(ack.ReasonCodes, []byte{0}) {
			err = fmt.Errorf("unsubscribe reasons=%v, want [0]", ack.ReasonCodes)
		}
		removed <- err
	}()
	removalFinished := false
	var admitted tenantQuotaSubscribeOutcome
	select {
	case err := <-removed:
		if err != nil {
			t.Fatal(err)
		}
		removalFinished = true
		admitted = <-tenantQuotaSubscribeAsync(ctx, useCase, "a/second", secondPrincipal, "allowed/new")
	case <-time.After(time.Second):
		// A replacement must keep its slot until its local mutation finishes.
	}
	close(committer.release)
	select {
	case replacement := <-replacementDone:
		if replacement.err != nil || !reflect.DeepEqual(replacement.result.Ack.ReasonCodes, []byte{0}) {
			t.Fatal(replacement)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !removalFinished {
		select {
		case err := <-removed:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case admitted = <-tenantQuotaSubscribeAsync(ctx, useCase, "a/second", secondPrincipal, "allowed/new"):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if admitted.err != nil || !reflect.DeepEqual(admitted.result.Ack.ReasonCodes, []byte{0}) {
		t.Fatal(admitted)
	}
	if _, exists := routes.Lookup("a/first", "allowed/old"); exists {
		t.Fatal("the completed unsubscribe was overwritten by an older replacement")
	}
	if count := routes.TenantSubscriptionCount(tenant); count != 1 {
		t.Fatalf("subscription count after replacement/removal/new admission=%d, want 1", count)
	}
}
