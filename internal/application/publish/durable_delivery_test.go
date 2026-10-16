package publish

import (
	"context"
	"errors"
	"testing"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/deliverystate"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type recordingDeliveryCommitter struct {
	changes         []membership.DeliveryChangeDTO
	result          membership.DeliveryResultDTO
	err             error
	completed       bool
	applyResult     *membership.ApplyResultDTO
	barriers        int
	barrierBeginErr error
	barrierErr      error
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

type testPendingDelivery struct {
	result membership.ApplyResultDTO
}

func (pending testPendingDelivery) Wait() (membership.ApplyResultDTO, error) {
	return pending.result, nil
}

func (c *recordingDeliveryCommitter) BeginDelivery(ctx context.Context, change membership.DeliveryChangeDTO) (deliverystate.Pending, error) {
	result, err := c.CommitDelivery(ctx, change)
	if err != nil {
		return nil, err
	}
	return testPendingDelivery{result: result}, nil
}

type testQuorumBarrier struct {
	committer *recordingDeliveryCommitter
}

func (barrier testQuorumBarrier) Wait() error {
	barrier.committer.completed = true
	return barrier.committer.barrierErr
}

func (c *recordingDeliveryCommitter) BeginQuorumBarrier(context.Context) (deliverystate.QuorumBarrier, error) {
	c.barriers++
	if c.barrierBeginErr != nil {
		return nil, c.barrierBeginErr
	}
	return testQuorumBarrier{committer: c}, nil
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

func untargetedDurablePublishFixture(t *testing.T, options ...Option) (*UseCase, *recordingQueue, *recordingDeliveryCommitter, time.Time) {
	t.Helper()
	now := testNow()
	routes := routing.NewTable()
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"publisher", "qos0-sub"} {
		key, err := tenancy.NewSessionKey(tenant, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := routes.BindSessionTenant(routing.SessionID(key.RoutingID()), tenant); err != nil {
			t.Fatal(err)
		}
	}
	qos0Key, _ := tenancy.NewSessionKey(tenant, "qos0-sub")
	if _, err := routes.Subscribe(routing.SessionID(qos0Key.RoutingID()), mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	committer := &recordingDeliveryCommitter{}
	queue.onEnqueue = func() {
		if !committer.completed {
			t.Error("QoS 0 fallback ran before quorum barrier completed")
		}
	}
	opts := append([]Option{WithDurableDeliveryCommitter(committer)}, options...)
	useCase, err := New(&fakeClock{now: now}, routes, queue, &recordingRetainedRepository{}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return useCase, queue, committer, now
}

// Traceability: CLU-07; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestPublishDurableWithoutTargetsUsesQuorumBarrier(t *testing.T) {
	t.Run("qos1 publish", func(t *testing.T) {
		useCase, queue, committer, now := untargetedDurablePublishFixture(t)
		if _, err := useCase.Publish(mustPublishSessionID(t, "publisher"), mqtt.Publish{
			Version: mqtt.ProtocolV5, Topic: "events/none", QoS: mqtt.QoSAtLeastOnce,
		}, now); err != nil {
			t.Fatal(err)
		}
		if len(committer.changes) != 0 || committer.barriers != 1 || len(queue.attempts) != 1 {
			t.Fatalf("changes=%+v barriers=%d QoS0 attempts=%+v", committer.changes, committer.barriers, queue.attempts)
		}
		if queue.attempts[0].SessionID != mustPublishSessionID(t, "qos0-sub") {
			t.Fatalf("QoS0 attempts = %+v, want qos0-sub", queue.attempts)
		}
	})

	t.Run("qos1 begin publish", func(t *testing.T) {
		useCase, queue, committer, now := untargetedDurablePublishFixture(t)
		pending, err := useCase.BeginPublish(mustPublishSessionID(t, "publisher"), mqtt.Publish{
			Version: mqtt.ProtocolV5, Topic: "events/none", QoS: mqtt.QoSAtLeastOnce,
		}, now)
		if err != nil {
			t.Fatal(err)
		}
		if committer.completed || len(queue.attempts) != 0 {
			t.Fatalf("BeginPublish completed before Wait: completed=%t attempts=%+v", committer.completed, queue.attempts)
		}
		if _, err := pending.Wait(); err != nil {
			t.Fatal(err)
		}
		if len(committer.changes) != 0 || committer.barriers != 1 || len(queue.attempts) != 1 {
			t.Fatalf("changes=%+v barriers=%d QoS0 attempts=%+v", committer.changes, committer.barriers, queue.attempts)
		}
	})

	t.Run("qos2 will", func(t *testing.T) {
		useCase, queue, committer, now := untargetedDurablePublishFixture(t)
		publisher := mustPublishSessionID(t, "publisher")
		publisherKey, err := tenancy.ParseSessionKeyRoutingID(string(publisher))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := useCase.PublishWill(publisher, publisherKey, mqtt.Publish{
			Version: mqtt.ProtocolV5, Topic: "events/none", QoS: mqtt.QoSExactlyOnce,
		}, now); err != nil {
			t.Fatal(err)
		}
		if len(committer.changes) != 0 || committer.barriers != 1 || len(queue.attempts) != 1 {
			t.Fatalf("changes=%+v barriers=%d QoS0 attempts=%+v", committer.changes, committer.barriers, queue.attempts)
		}
	})

	for _, asynchronous := range []bool{false, true} {
		name := "sync"
		if asynchronous {
			name = "begin"
		}
		for _, failBegin := range []bool{false, true} {
			failure := "wait failure"
			if failBegin {
				failure = "begin failure"
			}
			t.Run(name+" barrier "+failure, func(t *testing.T) {
				useCase, queue, committer, now := untargetedDurablePublishFixture(t)
				barrierErr := errors.New("quorum lost")
				if failBegin {
					committer.barrierBeginErr = barrierErr
				} else {
					committer.barrierErr = barrierErr
				}
				packet := mqtt.Publish{
					Version: mqtt.ProtocolV5, Topic: "events/none", QoS: mqtt.QoSAtLeastOnce,
				}
				var err error
				if asynchronous {
					var pending PendingPublish
					pending, err = useCase.BeginPublish(mustPublishSessionID(t, "publisher"), packet, now)
					if failBegin {
						if !errors.Is(err, barrierErr) {
							t.Fatalf("BeginPublish error = %v, want %v", err, barrierErr)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						if committer.completed || len(queue.attempts) != 0 {
							t.Fatalf("publication completed before Wait: completed=%t attempts=%+v", committer.completed, queue.attempts)
						}
						_, err = pending.Wait()
					}
				} else {
					_, err = useCase.Publish(mustPublishSessionID(t, "publisher"), packet, now)
				}
				if !errors.Is(err, barrierErr) {
					t.Fatalf("publication error = %v, want %v", err, barrierErr)
				}
				if committer.barriers != 1 || len(committer.changes) != 0 || len(queue.attempts) != 0 {
					t.Fatalf("barriers=%d changes=%+v QoS0 attempts=%+v", committer.barriers, committer.changes, queue.attempts)
				}
			})
		}
	}

	t.Run("retained qos1", func(t *testing.T) {
		retainedCommitter := &recordingRetainedCommitter{}
		useCase, _, committer, now := untargetedDurablePublishFixture(t, WithRetainedCommitter(retainedCommitter))
		if _, err := useCase.Publish(mustPublishSessionID(t, "publisher"), mqtt.Publish{
			Version: mqtt.ProtocolV5, Topic: "events/retained", Payload: []byte("value"),
			QoS: mqtt.QoSAtLeastOnce, Retain: true,
		}, now); err != nil {
			t.Fatal(err)
		}
		if len(retainedCommitter.changes) != 1 || len(committer.changes) != 0 || committer.barriers != 1 {
			t.Fatalf("retained=%d delivery changes=%+v barriers=%d", len(retainedCommitter.changes), committer.changes, committer.barriers)
		}
	})

	t.Run("release keeps command", func(t *testing.T) {
		useCase, _, committer, now := untargetedDurablePublishFixture(t)
		_, err := useCase.PublishQoS2Release(mustPublishSessionID(t, "publisher"), 4, 9, mqtt.Publish{
			Version: mqtt.ProtocolV5, Topic: "events/none", Payload: []byte("value"),
			QoS: mqtt.QoSExactlyOnce, PacketID: 17,
		}, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(committer.changes) != 1 || committer.changes[0].Action != membership.DeliveryActionRelease ||
			len(committer.changes[0].Targets) != 0 || committer.barriers != 0 {
			t.Fatalf("changes=%+v barriers=%d", committer.changes, committer.barriers)
		}
	})
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
func TestBeginPublishCommitFailuresPreserveLocalQueue(t *testing.T) {
	commitErr := errors.New("delivery proposal rejected")
	for _, tc := range []struct {
		name      string
		configure func(*recordingDeliveryCommitter)
		want      error
		wait      bool
	}{
		{
			name: "begin error",
			configure: func(committer *recordingDeliveryCommitter) {
				committer.err = commitErr
			},
			want: commitErr,
		},
		{
			name: "rejected apply result",
			configure: func(committer *recordingDeliveryCommitter) {
				committer.applyResult = &membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeConflict}
			},
			want: ErrDurableDeliveryRejected,
			wait: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useCase, queue, committer, now := durablePublishFixture(t, membership.DeliveryResultDTO{}, nil)
			tc.configure(committer)
			pending, err := useCase.BeginPublish(mustPublishSessionID(t, "publisher"), mqtt.Publish{
				Version: mqtt.ProtocolV5, Topic: "events/one", QoS: mqtt.QoSAtLeastOnce,
			}, now)
			if tc.wait {
				if err != nil {
					t.Fatalf("BeginPublish error = %v, want pending publication", err)
				}
				_, err = pending.Wait()
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("durable publication error = %v, want %v", err, tc.want)
			}
			if len(committer.changes) != 1 || len(queue.attempts) != 0 || len(queue.deliveries) != 0 {
				t.Fatalf("failed durable submission changed local delivery state: changes=%+v attempts=%+v deliveries=%+v",
					committer.changes, queue.attempts, queue.deliveries)
			}
		})
	}
}
func TestPublishDurableSaturationPreservesQoS0Fallback(t *testing.T) {
	useCase, queue, _, now := durablePublishFixture(t, membership.DeliveryResultDTO{
		Saturated: []string{"durable-sub"},
	}, nil)
	result, err := useCase.Publish(mustPublishSessionID(t, "publisher"), mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("value"), QoS: mqtt.QoSAtLeastOnce,
	}, now)
	if err != nil {
		t.Fatalf("Publish error = %v", err)
	}
	if len(result.Saturated) != 1 || result.Saturated[0] != mustPublishSessionID(t, "durable-sub") {
		t.Fatalf("saturated = %v, want durable-sub", result.Saturated)
	}
	if len(result.Delivered) != 1 || result.Delivered[0] != mustPublishSessionID(t, "qos0-sub") {
		t.Fatalf("delivered = %v, want qos0-sub", result.Delivered)
	}
	if len(queue.deliveries) != 1 || queue.deliveries[0].SessionID != mustPublishSessionID(t, "qos0-sub") {
		t.Fatalf("local queue = %+v, want only qos0-sub", queue.deliveries)
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

// Traceability: PRD MQTT-04/CLU-08; docs/specs/cluster-session-and-delivery.md §4.2, §11.32.
func TestPublishDurableRetainedCommitFailureStopsDeliveryCommit(t *testing.T) {
	errRetainedCommit := errors.New("retained quorum unavailable")
	for _, tc := range []struct {
		name string
		qos  mqtt.QoS
	}{
		{name: "qos1-publish", qos: mqtt.QoSAtLeastOnce},
		{name: "qos2-release", qos: mqtt.QoSExactlyOnce},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useCase, queue, committer, now := durablePublishFixture(t,
				membership.DeliveryResultDTO{Enqueued: []membership.DeliveryEnqueuedDTO{{ClientID: "durable-sub", Sequence: 1}}}, nil)
			retained := &recordingRetainedCommitter{err: errRetainedCommit}
			useCase.retainedCommitter = retained
			packet := mqtt.Publish{
				Version: mqtt.ProtocolV5, Topic: "events/retained", Payload: []byte("value"),
				QoS: tc.qos, PacketID: 17, Retain: true,
			}
			var err error
			if tc.qos == mqtt.QoSExactlyOnce {
				_, err = useCase.PublishQoS2Release(mustPublishSessionID(t, "publisher"), 4, 9, packet, now)
			} else {
				_, err = useCase.Publish(mustPublishSessionID(t, "publisher"), packet, now)
			}
			if !errors.Is(err, errRetainedCommit) {
				t.Fatalf("retained publish error = %v, want %v", err, errRetainedCommit)
			}
			if len(retained.changes) != 1 {
				t.Fatalf("retained commits = %+v, want one failed replace", retained.changes)
			}
			if change := retained.changes[0]; change.Message.QoS != tc.qos || change.Delete {
				t.Fatalf("retained change = %+v, want replace with QoS %d", change, tc.qos)
			}
			if len(committer.changes) != 0 || committer.barriers != 0 || len(queue.attempts) != 0 {
				t.Fatalf("failed retained commit caused delivery: changes=%+v barriers=%d fanout=%+v",
					committer.changes, committer.barriers, queue.attempts)
			}
		})
	}
}

func TestBeginPublishMapsDeliveryCapacityConflict(t *testing.T) {
	useCase, queue, committer, now := durablePublishFixture(t, membership.DeliveryResultDTO{}, nil)
	committer.applyResult = &membership.ApplyResultDTO{
		Outcome:  membership.ApplyOutcomeConflict,
		Conflict: membership.ConflictDeliveryCapacity,
	}

	pending, err := useCase.BeginPublish(mustPublishSessionID(t, "publisher"), mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("value"), QoS: mqtt.QoSAtLeastOnce,
	}, now)
	if err != nil {
		t.Fatalf("BeginPublish error = %v, want pending publication", err)
	}
	_, err = pending.Wait()
	if !errors.Is(err, ErrDurableDeliveryCapacity) {
		t.Fatalf("pending.Wait error = %v, want ErrDurableDeliveryCapacity", err)
	}
	if len(committer.changes) != 1 || len(queue.attempts) != 0 || len(queue.deliveries) != 0 {
		t.Fatalf("capacity conflict changed local delivery state: changes=%+v attempts=%+v deliveries=%+v",
			committer.changes, queue.attempts, queue.deliveries)
	}
}

func TestPublishWillRejectsMismatchedTrustedSession(t *testing.T) {
	useCase, queue, committer, now := durablePublishFixture(t, membership.DeliveryResultDTO{}, nil)
	publisher := mustPublishSessionID(t, "publisher")
	otherKey, err := tenancy.ParseSessionKeyRoutingID(string(mustPublishSessionID(t, "qos0-sub")))
	if err != nil {
		t.Fatal(err)
	}

	_, err = useCase.PublishWill(publisher, otherKey, mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events/will", Payload: []byte("value"), QoS: mqtt.QoSExactlyOnce,
	}, now)
	if !errors.Is(err, errInvalidDurableSession) {
		t.Fatalf("PublishWill error = %v, want invalid durable session", err)
	}
	if len(committer.changes) != 0 || committer.barriers != 0 || len(queue.attempts) != 0 {
		t.Fatalf("mismatched trusted session caused publication: changes=%+v barriers=%d deliveries=%+v",
			committer.changes, committer.barriers, queue.attempts)
	}
}

// Traceability: MQTT-07, CLU-05; docs/specs/cluster-session-and-delivery.md §4.2, §11.35.
func TestPublishDurableTargetsCarrySharedOriginUnlessNonSharedRouteAlsoMatches(t *testing.T) {
	now := testNow()
	routes := routing.NewTable()
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	subscribe := func(clientID, filter string) {
		key, err := tenancy.NewSessionKey(tenant, clientID)
		if err != nil {
			t.Fatal(err)
		}
		id := routing.SessionID(key.RoutingID())
		if err := routes.BindSessionTenant(id, tenant); err != nil {
			t.Fatal(err)
		}
		if _, err := routes.Subscribe(id, mqtt.Subscription{Filter: filter, QoS: mqtt.QoSAtLeastOnce}); err != nil {
			t.Fatal(err)
		}
	}
	publisher, _ := tenancy.NewSessionKey(tenant, "publisher")
	if err := routes.BindSessionTenant(routing.SessionID(publisher.RoutingID()), tenant); err != nil {
		t.Fatal(err)
	}
	subscribe("worker", "$share/g/events/#")
	subscribe("mixed", "$share/h/events/#")
	subscribe("mixed", "events/#")
	committer := &recordingDeliveryCommitter{result: membership.DeliveryResultDTO{Enqueued: []membership.DeliveryEnqueuedDTO{{ClientID: "worker", Sequence: 1}, {ClientID: "mixed", Sequence: 1}}}}
	useCase, err := New(&fakeClock{now: now}, routes, &recordingQueue{errors: make(map[routing.SessionID]error)}, &recordingRetainedRepository{}, WithDurableDeliveryCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Publish(routing.SessionID(publisher.RoutingID()), mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/one", Payload: []byte("x"), QoS: mqtt.QoSAtLeastOnce}, now); err != nil {
		t.Fatal(err)
	}
	if len(committer.changes) != 1 {
		t.Fatalf("commit count = %d", len(committer.changes))
	}
	origins := make(map[string]string)
	for _, target := range committer.changes[0].Targets {
		origins[target.ClientID] = target.SharedFilter
	}
	want := map[string]string{"worker": "$share/g/events/#", "mixed": ""}
	if len(origins) != len(want) || origins["worker"] != want["worker"] || origins["mixed"] != want["mixed"] {
		t.Fatalf("target origins = %v, want %v", origins, want)
	}
}
