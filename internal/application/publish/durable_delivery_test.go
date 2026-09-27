package publish

import (
	"context"
	"errors"
	"testing"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type recordingDeliveryCommitter struct {
	changes     []membership.DeliveryChangeDTO
	result      membership.DeliveryResultDTO
	err         error
	completed   bool
	applyResult *membership.ApplyResultDTO
}

func (c *recordingDeliveryCommitter) CommitDelivery(_ context.Context, change membership.DeliveryChangeDTO) (membership.ApplyResultDTO, error) {
	c.changes = append(c.changes, change)
	c.completed = true
	if c.err != nil {
		return membership.ApplyResultDTO{}, c.err
	}
	if c.applyResult != nil {
		return *c.applyResult, nil
	}
	return membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeApplied, Delivery: &c.result}, nil
}

func mustPublishSessionID(t *testing.T, clientID string) routing.SessionID {
	t.Helper()
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	key, err := tenancy.NewSessionKey(tenant, clientID)
	if err != nil {
		t.Fatal(err)
	}
	return routing.SessionID(key.RoutingID())
}

func durablePublishFixture(t *testing.T, result membership.DeliveryResultDTO, committerErr error) (*UseCase, *recordingQueue, *recordingDeliveryCommitter, time.Time) {
	t.Helper()
	now := testNow()
	routes := routing.NewTable()
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"publisher", "durable-sub", "qos0-sub"} {
		key, err := tenancy.NewSessionKey(tenant, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := routes.BindSessionTenant(routing.SessionID(key.RoutingID()), tenant); err != nil {
			t.Fatal(err)
		}
	}
	durableKey, _ := tenancy.NewSessionKey(tenant, "durable-sub")
	qos0Key, _ := tenancy.NewSessionKey(tenant, "qos0-sub")
	if _, err := routes.Subscribe(routing.SessionID(durableKey.RoutingID()), mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSExactlyOnce}); err != nil {
		t.Fatal(err)
	}
	if _, err := routes.Subscribe(routing.SessionID(qos0Key.RoutingID()), mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	committer := &recordingDeliveryCommitter{result: result, err: committerErr}
	queue.onEnqueue = func() {
		if !committer.completed {
			t.Error("QoS 0 fallback ran before durable commit completed")
		}
	}
	useCase, err := New(&fakeClock{now: now}, routes, queue, &recordingRetainedRepository{}, WithDurableDeliveryCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}
	return useCase, queue, committer, now
}

func TestPublishDurableQoSCommitsBeforeQoS0Fallback(t *testing.T) {
	for _, qos := range []mqtt.QoS{mqtt.QoSAtLeastOnce, mqtt.QoSExactlyOnce} {
		name := "qos1"
		if qos == mqtt.QoSExactlyOnce {
			name = "qos2"
		}
		t.Run(name, func(t *testing.T) {
			useCase, queue, committer, now := durablePublishFixture(t, membership.DeliveryResultDTO{Enqueued: []membership.DeliveryEnqueuedDTO{{ClientID: "durable-sub", Sequence: 1}}}, nil)
			_, err := useCase.Publish(mustPublishSessionID(t, "publisher"), mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("x"), QoS: qos}, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(committer.changes) != 1 {
				t.Fatalf("commit count = %d", len(committer.changes))
			}
			change := committer.changes[0]
			if change.Action != membership.DeliveryActionPublish || change.PublishQoS != uint32(qos) || len(change.Targets) != 1 || change.Targets[0].ClientID != "durable-sub" {
				t.Fatalf("durable command = %+v", change)
			}
			if len(queue.attempts) != 1 || queue.attempts[0].SessionID != mustPublishSessionID(t, "qos0-sub") {
				t.Fatalf("QoS 0 fallback = %+v", queue.attempts)
			}
		})
	}
}

func TestPublishDurableCommitsWithNoDurableTargets(t *testing.T) {
	now := testNow()
	routes := routing.NewTable()
	tenant, _ := tenancy.NewTenantID("tenant-a")
	publisher := mustPublishSessionID(t, "publisher")
	if err := routes.BindSessionTenant(publisher, tenant); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	committer := &recordingDeliveryCommitter{}
	useCase, err := New(&fakeClock{now: now}, routes, queue, &recordingRetainedRepository{}, WithDurableDeliveryCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Publish(publisher, mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/none", QoS: mqtt.QoSAtLeastOnce}, now); err != nil {
		t.Fatal(err)
	}
	if len(committer.changes) != 1 || len(committer.changes[0].Targets) != 0 {
		t.Fatalf("commits = %+v, want one empty-target PUBLISH", committer.changes)
	}
}

func TestPublishDurableCommitErrorStopsQoS0Fanout(t *testing.T) {
	for _, tc := range []struct {
		name      string
		commitErr error
		want      error
	}{
		{name: "command-too-large", commitErr: errors.New("command too large")},
		{name: "capacity", commitErr: deliveryqueue.ErrFull, want: ErrDurableDeliveryCapacity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useCase, queue, _, now := durablePublishFixture(t, membership.DeliveryResultDTO{}, tc.commitErr)
			_, err := useCase.Publish(mustPublishSessionID(t, "publisher"), mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/one", QoS: mqtt.QoSAtLeastOnce}, now)
			if err == nil {
				t.Fatal("Publish error = nil")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("Publish error = %v, want %v", err, tc.want)
			}
			if len(queue.attempts) != 0 {
				t.Fatalf("QoS 0 fanout after failed commit: %+v", queue.attempts)
			}
		})
	}
}

func TestPublishQoS2ReleaseNotFoundSkipsQoS0Fallback(t *testing.T) {
	useCase, queue, committer, now := durablePublishFixture(t, membership.DeliveryResultDTO{Found: false}, nil)
	_, err := useCase.PublishQoS2Release(mustPublishSessionID(t, "publisher"), 4, 9, mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("x"), QoS: mqtt.QoSExactlyOnce, PacketID: 17}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(committer.changes) != 1 || committer.changes[0].Action != membership.DeliveryActionRelease {
		t.Fatalf("release commit = %+v", committer.changes)
	}
	if len(queue.attempts) != 0 {
		t.Fatalf("fallback after not-found RELEASE: %+v", queue.attempts)
	}
}

func TestPublishQoS2WillAndStandaloneBehavior(t *testing.T) {
	useCase, _, committer, now := durablePublishFixture(t, membership.DeliveryResultDTO{Enqueued: []membership.DeliveryEnqueuedDTO{{ClientID: "durable-sub", Sequence: 1}}}, nil)
	if _, err := useCase.Publish(mustPublishSessionID(t, "publisher"), mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/will", Payload: []byte("will"), QoS: mqtt.QoSExactlyOnce}, now); err != nil {
		t.Fatal(err)
	}
	if len(committer.changes) != 1 || committer.changes[0].PublishQoS != 2 {
		t.Fatalf("QoS 2 will command = %+v", committer.changes)
	}

	standalone, localQueue, localCommitter, now := durablePublishFixture(t, membership.DeliveryResultDTO{}, nil)
	standalone.durableDeliveryCommitter = nil
	// The commit-order assertion applies only when the durable path is enabled.
	localQueue.onEnqueue = nil
	if _, err := standalone.Publish(mustPublishSessionID(t, "publisher"), mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/one", QoS: mqtt.QoSAtLeastOnce}, now); err != nil {
		t.Fatal(err)
	}
	if len(localCommitter.changes) != 0 || len(localQueue.attempts) != 2 {
		t.Fatalf("standalone behavior: commits=%d deliveries=%+v", len(localCommitter.changes), localQueue.attempts)
	}
}

func TestPublishQoS2WillUsesAuthenticatedTenantAfterSessionRemoval(t *testing.T) {
	useCase, queue, committer, now := durablePublishFixture(t, membership.DeliveryResultDTO{
		Enqueued: []membership.DeliveryEnqueuedDTO{{ClientID: "durable-sub", Sequence: 1}},
	}, nil)
	publisher := mustPublishSessionID(t, "publisher")
	publisherKey, err := tenancy.ParseSessionKeyRoutingID(string(publisher))
	if err != nil {
		t.Fatal(err)
	}
	useCase.routes.RemoveSession(publisher)

	packet := mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/will", Payload: []byte("will"), QoS: mqtt.QoSExactlyOnce}
	if _, err := useCase.PublishWill(publisher, publisherKey, packet, now); err != nil {
		t.Fatal(err)
	}
	if len(committer.changes) != 1 {
		t.Fatalf("QoS2 Will commit count = %d, want 1", len(committer.changes))
	}
	change := committer.changes[0]
	if change.Action != membership.DeliveryActionPublish || change.TenantID != "tenant-a" || change.ClientID != "publisher" ||
		change.PublishQoS != 2 || len(change.Targets) != 1 || change.Targets[0].ClientID != "durable-sub" {
		t.Fatalf("QoS2 Will commit = %+v", change)
	}
	if len(queue.attempts) != 1 || queue.attempts[0].SessionID != mustPublishSessionID(t, "qos0-sub") {
		t.Fatalf("QoS2 Will QoS0 lane = %+v, want qos0-sub", queue.attempts)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestReceiveQoS2CommitsPublisherStateBeforeAcknowledgement(t *testing.T) {
	useCase, queue, committer, now := durablePublishFixture(t, membership.DeliveryResultDTO{Found: true}, nil)
	packet := mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events/inbound", Payload: []byte("payload"),
		QoS: mqtt.QoSExactlyOnce, PacketID: 29, Retain: true,
		HasMessageExpiryInterval: true, MessageExpiryInterval: 30,
	}
	if err := useCase.ReceiveQoS2(mustPublishSessionID(t, "publisher"), 4, 9, packet, now); err != nil {
		t.Fatal(err)
	}
	if len(committer.changes) != 1 {
		t.Fatalf("durable RECEIVE count=%d, want 1", len(committer.changes))
	}
	change := committer.changes[0]
	if change.Action != membership.DeliveryActionReceive || change.OwnerNodeID != 4 || change.Epoch != 9 ||
		change.PacketID != 29 || change.Topic != packet.Topic || string(change.Payload) != string(packet.Payload) ||
		!change.Retain || change.ExpiresAtUnixMillis != uint64(now.Add(30*time.Second).UnixMilli()) {
		t.Fatalf("durable RECEIVE = %+v", change)
	}
	if len(queue.attempts) != 0 {
		t.Fatalf("inbound QoS 2 was locally fanned out before RELEASE: %+v", queue.attempts)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestReceiveQoS2RejectsInvalidOrUncommittableTransitions(t *testing.T) {
	now := testNow()
	validPacket := mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/inbound", QoS: mqtt.QoSExactlyOnce, PacketID: 29}
	capacityConflict := membership.ApplyResultDTO{
		Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictDeliveryCapacity,
	}
	for _, testCase := range []struct {
		name   string
		setup  func(*UseCase, *recordingDeliveryCommitter)
		packet mqtt.Publish
		owner  uint64
		epoch  uint64
		at     time.Time
		want   error
	}{
		{name: "invalid QoS", packet: mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/inbound", QoS: mqtt.QoSAtMostOnce, PacketID: 29}, owner: 4, epoch: 9, at: now, want: routing.ErrInvalidQoS},
		{name: "zero packet identifier", packet: mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/inbound", QoS: mqtt.QoSExactlyOnce}, owner: 4, epoch: 9, at: now, want: routing.ErrInvalidQoS},
		{name: "missing owner", packet: validPacket, owner: 0, epoch: 9, at: now, want: ErrDurableDeliveryRejected},
		{name: "missing epoch", packet: validPacket, owner: 4, epoch: 0, at: now, want: ErrDurableDeliveryRejected},
		{name: "committer unavailable", setup: func(useCase *UseCase, _ *recordingDeliveryCommitter) { useCase.durableDeliveryCommitter = nil }, packet: validPacket, owner: 4, epoch: 9, at: now, want: ErrDurableDeliveryRejected},
		{name: "capacity error", setup: func(_ *UseCase, committer *recordingDeliveryCommitter) { committer.err = deliveryqueue.ErrFull }, packet: validPacket, owner: 4, epoch: 9, at: now, want: ErrDurableDeliveryCapacity},
		{name: "capacity conflict", setup: func(_ *UseCase, committer *recordingDeliveryCommitter) { committer.applyResult = &capacityConflict }, packet: validPacket, owner: 4, epoch: 9, at: now, want: ErrDurableDeliveryCapacity},
		{name: "invalid commit time", setup: func(useCase *UseCase, _ *recordingDeliveryCommitter) {
			useCase.clock = &fakeClock{now: time.UnixMilli(0)}
		}, packet: validPacket, owner: 4, epoch: 9, at: now, want: errInvalidDurableTimestamp},
		{name: "nonpositive expiry", packet: mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/inbound", QoS: mqtt.QoSExactlyOnce, PacketID: 29, HasMessageExpiryInterval: true, MessageExpiryInterval: 1}, owner: 4, epoch: 9, at: time.UnixMilli(-3000), want: errInvalidDurableTimestamp},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			useCase, _, committer, _ := durablePublishFixture(t, membership.DeliveryResultDTO{}, nil)
			if testCase.setup != nil {
				testCase.setup(useCase, committer)
			}
			err := useCase.ReceiveQoS2(mustPublishSessionID(t, "publisher"), testCase.owner, testCase.epoch, testCase.packet, testCase.at)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("ReceiveQoS2 error=%v, want %v", err, testCase.want)
			}
		})
	}
	if err := (*UseCase)(nil).ReceiveQoS2(mustPublishSessionID(t, "publisher"), 4, 9, validPacket, now); !errors.Is(err, ErrNilClock) {
		t.Fatalf("nil use case error=%v, want %v", err, ErrNilClock)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestPublishQoS2ReleaseFallsBackWhenDurableCommitterIsAbsent(t *testing.T) {
	useCase, queue, committer, now := durablePublishFixture(t, membership.DeliveryResultDTO{}, nil)
	useCase.durableDeliveryCommitter = nil
	queue.onEnqueue = nil
	packet := mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/release", Payload: []byte("released"), QoS: mqtt.QoSExactlyOnce, PacketID: 31}
	if _, err := useCase.PublishQoS2Release(mustPublishSessionID(t, "publisher"), 4, 9, packet, now); err != nil {
		t.Fatal(err)
	}
	if len(committer.changes) != 0 || len(queue.attempts) != 2 {
		t.Fatalf("standalone RELEASE fallback: commits=%d local deliveries=%+v", len(committer.changes), queue.attempts)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestPublishQoS2ReleaseRejectsInvalidExchange(t *testing.T) {
	now := testNow()
	for _, testCase := range []struct {
		name   string
		packet mqtt.Publish
		owner  uint64
		epoch  uint64
		want   error
	}{
		{name: "invalid QoS", packet: mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/release", QoS: mqtt.QoSAtLeastOnce, PacketID: 31}, owner: 4, epoch: 9, want: routing.ErrInvalidQoS},
		{name: "missing owner", packet: mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/release", QoS: mqtt.QoSExactlyOnce, PacketID: 31}, owner: 0, epoch: 9, want: ErrDurableDeliveryRejected},
		{name: "missing epoch", packet: mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/release", QoS: mqtt.QoSExactlyOnce, PacketID: 31}, owner: 4, epoch: 0, want: ErrDurableDeliveryRejected},
		{name: "missing packet identifier", packet: mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/release", QoS: mqtt.QoSExactlyOnce}, owner: 4, epoch: 9, want: ErrDurableDeliveryRejected},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			useCase, _, _, _ := durablePublishFixture(t, membership.DeliveryResultDTO{}, nil)
			_, err := useCase.PublishQoS2Release(mustPublishSessionID(t, "publisher"), testCase.owner, testCase.epoch, testCase.packet, now)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("PublishQoS2Release error=%v, want %v", err, testCase.want)
			}
		})
	}
	if _, err := (*UseCase)(nil).PublishQoS2Release(mustPublishSessionID(t, "publisher"), 4, 9, mqtt.Publish{QoS: mqtt.QoSExactlyOnce, PacketID: 31}, now); !errors.Is(err, ErrNilClock) {
		t.Fatalf("nil use case error=%v, want %v", err, ErrNilClock)
	}
}

// Traceability: SEC-06, TEN-04, CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestReceiveQoS2RejectsUnauthorizedPublisherBeforeCommit(t *testing.T) {
	now := testNow()
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	allowedFilter, err := tenancy.NewTopicFilter(tenant, "allowed/#")
	if err != nil {
		t.Fatal(err)
	}
	rule, err := security.NewRule(tenant, "publisher", security.EffectAllow, security.ActionPublish, allowedFilter)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := security.NewPolicy([]security.Rule{rule})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := security.NewPrincipal(tenant, "publisher", "device-1", "credential-1", []security.Role{"publisher"}, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	routes := routing.NewTable()
	publisher := mustPublishSessionID(t, "publisher")
	if err := routes.BindSessionTenant(publisher, tenant); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	committer := &recordingDeliveryCommitter{}
	useCase, err := New(&fakeClock{now: now}, routes, queue, &recordingRetainedRepository{},
		WithPolicy(policy), WithDurableDeliveryCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}

	err = useCase.ReceiveQoS2(publisher, 4, 9, mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "forbidden/topic", Payload: []byte("secret"),
		QoS: mqtt.QoSExactlyOnce, PacketID: 31,
	}, now, principal)
	if !errors.Is(err, security.ErrNotAuthorized) {
		t.Fatalf("unauthorized RECEIVE error=%v, want ErrNotAuthorized", err)
	}
	if len(committer.changes) != 0 || len(queue.attempts) != 0 {
		t.Fatalf("unauthorized RECEIVE mutated state: commits=%+v deliveries=%+v", committer.changes, queue.attempts)
	}
}
