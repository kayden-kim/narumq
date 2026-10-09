package subscribe

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type cmmCommit struct {
	tenantID string
	clientID string
	owner    uint64
	epoch    uint64
	filter   string
	action   uint32
}

type cmmCommitter struct {
	commits []cmmCommit
	err     error
}

func (committer *cmmCommitter) CommitSubscription(
	_ context.Context, tenantID, clientID string, owner, epoch uint64, filter string, _ uint32, _, _ bool, _, action uint32,
) error {
	committer.commits = append(committer.commits, cmmCommit{
		tenantID: tenantID, clientID: clientID, owner: owner, epoch: epoch, filter: filter, action: action,
	})
	return committer.err
}

// Traceability: PRD MQTT-06/MQTT-07/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §4 항목 6.
func TestCmmUnsubscribeCommitFailureKeepsRouteAndStopsAck(t *testing.T) {
	routes := routing.NewTable()
	if _, err := routes.Subscribe("client-a", mqtt.Subscription{Filter: "sensors/#", QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	commitErr := errors.New("quorum lost")
	committer := &cmmCommitter{err: commitErr}
	useCase, err := New(routes, &recordingQueue{}, &recordingRetained{}, WithCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}
	ownership := SessionOwnership{TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: 7, Epoch: 3}
	packet := mqtt.Unsubscribe{Version: mqtt.ProtocolV5, PacketID: 9, Filters: []string{"broken/#/filter", "sensors/#", "other/#"}}

	result, err := useCase.UnsubscribeCommitted(context.Background(), "client-a", packet, ownership)
	if !errors.Is(err, commitErr) {
		t.Fatalf("UnsubscribeCommitted() error = %v, want %v", err, commitErr)
	}
	if result.Ack.PacketID != 9 || !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{0x8F}) {
		t.Fatalf("partial ack = %#v, want only the invalid-filter reason before the failed commit", result.Ack)
	}
	wantCommits := []cmmCommit{{tenantID: "tenant-a", clientID: "client-a", owner: 7, epoch: 3, filter: "sensors/#", action: subscriptionActionRemove}}
	if !reflect.DeepEqual(committer.commits, wantCommits) {
		t.Fatalf("commits = %#v, want %#v", committer.commits, wantCommits)
	}
	if _, ok := routes.Lookup("client-a", "sensors/#"); !ok {
		t.Fatal("route removed although the fenced remove commit failed")
	}

	fallback := useCase.Unsubscribe("client-a", mqtt.Unsubscribe{Version: mqtt.ProtocolV5, PacketID: 10, Filters: []string{"sensors/#"}})
	want := mqtt.UnsubAck{Version: mqtt.ProtocolV5, PacketID: 10}
	if !reflect.DeepEqual(fallback, want) {
		t.Fatalf("Unsubscribe() after commit failure = %#v, want reasonless %#v", fallback, want)
	}
	if _, ok := routes.Lookup("client-a", "sensors/#"); !ok {
		t.Fatal("standalone Unsubscribe removed route after commit failure")
	}
}

// Traceability: PRD MQTT-06/TEST-03/05; MQTT-3.11.3-1/2.
func TestCmmUnsubscribeInvalidSessionReportsTopicFilterInvalid(t *testing.T) {
	useCase := newSubscribeUseCase(t, routing.NewTable(), &recordingQueue{}, &recordingRetained{})
	ack := useCase.Unsubscribe("", mqtt.Unsubscribe{Version: mqtt.ProtocolV5, PacketID: 5, Filters: []string{"sensors/#", "a/b"}})
	if !reflect.DeepEqual(ack.ReasonCodes, []byte{0x8F, 0x8F}) {
		t.Fatalf("invalid-session reasons = % X, want 8F 8F", ack.ReasonCodes)
	}
	v311 := useCase.Unsubscribe("", mqtt.Unsubscribe{Version: mqtt.ProtocolV311, PacketID: 6, Filters: []string{"sensors/#"}})
	if v311.PacketID != 6 || len(v311.ReasonCodes) != 0 {
		t.Fatalf("MQTT 3.1.1 invalid-session UNSUBACK = %#v, want packet-id-only", v311)
	}
}

// Traceability: PRD MQTT-04/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5.
func TestCmmSubscribeRejectsCorruptRetainedTopic(t *testing.T) {
	routes := routing.NewTable()
	queue := &recordingQueue{}
	repository := &recordingRetained{messages: []retained.MessageDTO{
		{Topic: "events/one", Payload: []byte("ok"), QoS: mqtt.QoSAtMostOnce},
		{Topic: "events/+", Payload: []byte("corrupt"), QoS: mqtt.QoSAtMostOnce},
	}}
	useCase := newSubscribeUseCase(t, routes, queue, repository)
	_, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "events/#"}}}, time.Time{})
	if !errors.Is(err, tenancy.ErrInvalidTopicName) {
		t.Fatalf("Subscribe() error = %v, want %v", err, tenancy.ErrInvalidTopicName)
	}
	if len(queue.batchSizes) != 0 {
		t.Fatalf("queue received batches %v after corrupt retained topic", queue.batchSizes)
	}
	if _, ok := routes.Lookup("subscriber", "events/#"); ok {
		t.Fatal("route committed after retained replay failure")
	}
}

// Traceability: PRD MQTT-04/TEST-04/05; docs/specs/mqtt-protocol-and-session.md §5.
func TestCmmSubscribeReplaysRetainedInTopicOrder(t *testing.T) {
	queue := &recordingQueue{}
	repository := &recordingRetained{messages: []retained.MessageDTO{
		{Topic: "a/z", Payload: []byte("z"), QoS: mqtt.QoSAtMostOnce},
		{Topic: "a/b", Payload: []byte("b"), QoS: mqtt.QoSAtMostOnce},
		{Topic: "a/m", Payload: []byte("m"), QoS: mqtt.QoSAtMostOnce},
	}}
	useCase := newSubscribeUseCase(t, routing.NewTable(), queue, repository)
	result, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "a/+"}}}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if result.RetainedQueued != 3 || !reflect.DeepEqual(queue.batchSizes, []int{3}) {
		t.Fatalf("replay = count %d batches %v, want one batch of 3", result.RetainedQueued, queue.batchSizes)
	}
	topics := make([]string, 0, len(queue.deliveries))
	for _, delivery := range queue.deliveries {
		topics = append(topics, delivery.Topic)
	}
	if want := []string{"a/b", "a/m", "a/z"}; !reflect.DeepEqual(topics, want) {
		t.Fatalf("replay order = %v, want %v", topics, want)
	}
}

// Traceability: PRD MQTT-06/MQTT-07/TEST-03/05; MQTT-3.9.3-2 (MQTT 3.1.1 exposes only 0x80 failure).
func TestCmmSubscribeNonV5FailuresUseGenericFailureCode(t *testing.T) {
	t.Run("replay-queue-full", func(t *testing.T) {
		routes := routing.NewTable()
		queue := &recordingQueue{err: deliveryqueue.ErrFull}
		repository := &recordingRetained{messages: []retained.MessageDTO{{Topic: "events/one", Payload: []byte("v"), QoS: mqtt.QoSAtMostOnce}}}
		useCase := newSubscribeUseCase(t, routes, queue, repository)
		result, err := useCase.Subscribe("subscriber", mqtt.Subscribe{Version: mqtt.ProtocolV311, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "events/#"}}}, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{0x80}) || result.RetainedQueued != 0 {
			t.Fatalf("MQTT 3.1.1 queue-full result = %#v, want 0x80 without replay", result)
		}
		if _, ok := routes.Lookup("subscriber", "events/#"); ok {
			t.Fatal("route committed after MQTT 3.1.1 replay capacity failure")
		}
	})
	t.Run("not-authorized", func(t *testing.T) {
		routes := routing.NewTable()
		policy, principal := cmmPolicyAndPrincipal(t)
		useCase, err := New(routes, &recordingQueue{}, &recordingRetained{}, WithPolicy(policy))
		if err != nil {
			t.Fatal(err)
		}
		result, err := useCase.Subscribe("client-1", mqtt.Subscribe{Version: mqtt.ProtocolV311, PacketID: 2, Subscriptions: []mqtt.Subscription{
			{Filter: "forbidden/#", QoS: mqtt.QoSAtLeastOnce},
			{Filter: "allowed/x", QoS: mqtt.QoSAtLeastOnce},
		}}, time.Time{}, principal)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{0x80, 0x01}) {
			t.Fatalf("MQTT 3.1.1 authorization reasons = % X, want 80 01", result.Ack.ReasonCodes)
		}
		if _, ok := routes.Lookup("client-1", "forbidden/#"); ok {
			t.Fatal("unauthorized MQTT 3.1.1 route committed")
		}
	})
	t.Run("shared-no-local-without-v5", func(t *testing.T) {
		routes := routing.NewTable()
		useCase := newSubscribeUseCase(t, routes, &recordingQueue{}, &recordingRetained{})
		result, err := useCase.Subscribe("client-1", mqtt.Subscribe{Version: mqtt.ProtocolUnknown, PacketID: 3, Subscriptions: []mqtt.Subscription{
			{Filter: "$share/team/events", NoLocal: true},
		}}, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result.Ack.ReasonCodes, []byte{0x80}) {
			t.Fatalf("non-MQTT-5 shared No Local reason = % X, want 80", result.Ack.ReasonCodes)
		}
		if _, ok := routes.Lookup("client-1", "$share/team/events"); ok {
			t.Fatal("shared No Local route committed")
		}
	})
}

func cmmPolicyAndPrincipal(t *testing.T) (security.Policy, security.Principal) {
	t.Helper()
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	allowedFilter, err := tenancy.NewTopicFilter(tenant, "allowed/#")
	if err != nil {
		t.Fatal(err)
	}
	rule, err := security.NewRule(tenant, "client", security.EffectAllow, security.ActionSubscribe, allowedFilter)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := security.NewPolicy([]security.Rule{rule})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := security.NewPrincipal(tenant, "client-1", "device-1", "cred-1", []security.Role{"client"}, time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return policy, principal
}
