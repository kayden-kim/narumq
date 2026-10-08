package connect

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
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type durableConnectQueue struct {
	*fakeDeliveryQueue
	resetCalls    int
	resetEpoch    uint64
	resetQueued   []deliveryqueue.DeliveryDTO
	resetOutbound []deliveryqueue.OutboundDTO
	resetErr      error
}

func newDurableConnectQueue() *durableConnectQueue {
	return &durableConnectQueue{fakeDeliveryQueue: newFakeDeliveryQueue()}
}

func (q *durableConnectQueue) ResetDurable(id routing.SessionID, epoch uint64, queued []deliveryqueue.DeliveryDTO, outbound []deliveryqueue.OutboundDTO, now time.Time) error {
	q.resetCalls++
	q.resetEpoch = epoch
	q.resetQueued = append([]deliveryqueue.DeliveryDTO(nil), queued...)
	q.resetOutbound = append([]deliveryqueue.OutboundDTO(nil), outbound...)
	if q.resetErr != nil {
		return q.resetErr
	}
	q.queued[id] = append([]deliveryqueue.DeliveryDTO(nil), queued...)
	return nil
}

type recordingDurableConnectCommitter struct {
	changes       []membership.DeliveryChangeDTO
	attachResult  membership.DeliveryResultDTO
	applyResult   *membership.ApplyResultDTO
	err           error
	activations   []activationCall
	deactivations []activationCall
	onActivate    func()
	activationErr error
}

type activationCall struct {
	sessionID      routing.SessionID
	epoch, through uint64
}

func (c *recordingDurableConnectCommitter) CommitDelivery(_ context.Context, change membership.DeliveryChangeDTO) (membership.ApplyResultDTO, error) {
	c.changes = append(c.changes, change)
	if c.err != nil {
		return membership.ApplyResultDTO{}, c.err
	}
	if c.applyResult != nil {
		return *c.applyResult, nil
	}
	result := membership.DeliveryResultDTO{}
	if change.Action == membership.DeliveryActionAttach {
		result = c.attachResult
	}
	return membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeApplied, Delivery: &result}, nil
}

type testPendingDelivery struct {
	result membership.ApplyResultDTO
}

func (pending testPendingDelivery) Wait() (membership.ApplyResultDTO, error) {
	return pending.result, nil
}

func (c *recordingDurableConnectCommitter) BeginDelivery(ctx context.Context, change membership.DeliveryChangeDTO) (deliverystate.Pending, error) {
	result, err := c.CommitDelivery(ctx, change)
	if err != nil {
		return nil, err
	}
	return testPendingDelivery{result: result}, nil
}
func (c *recordingDurableConnectCommitter) ActivateDelivery(id routing.SessionID, epoch, through uint64) error {
	if c.onActivate != nil {
		c.onActivate()
	}
	c.activations = append(c.activations, activationCall{id, epoch, through})
	return c.activationErr
}
func (c *recordingDurableConnectCommitter) DeactivateDelivery(id routing.SessionID, epoch uint64) {
	c.deactivations = append(c.deactivations, activationCall{sessionID: id, epoch: epoch})
}

func durableConnectFixture(t *testing.T, now time.Time, offline *fakeOfflineCoordinator, committer *recordingDurableConnectCommitter) (*UseCase, *fakeSessionRepository, *durableConnectQueue, *fakeOwnershipCoordinator) {
	t.Helper()
	repository := newFakeSessionRepository()
	queue := newDurableConnectQueue()
	ownership := &fakeOwnershipCoordinator{epoch: 7}
	if offline == nil {
		offline = &fakeOfflineCoordinator{}
	}
	useCase, err := New(&fakeClock{now: now}, repository, queue, routing.NewTable(), &fakeScheduler{}, &fakeWillPublisher{},
		WithOwnershipCoordinator(ownership, 3), WithDurableDeliveryCommitter(committer), WithOfflineCoordinator(offline))
	if err != nil {
		t.Fatal(err)
	}
	return useCase, repository, queue, ownership
}

func durableConnectState() *membership.DeliverySessionStateDTO {
	return &membership.DeliverySessionStateDTO{NextSequence: 4, Messages: []membership.DeliveryMessageDTO{
		{Sequence: 2, Topic: "durable/queued", Payload: []byte("q"), QoS: 1, Phase: membership.DeliveryPhaseQueued},
	}}
}

// Traceability: CLU-05/CLU-06; docs/specs/cluster-session-and-delivery.md §4.2.
func TestConnectAttachResetsQueueBeforeActivationAndCombinesSessionPresent(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name                           string
		cleanStart, found, wantPresent bool
	}{
		{"durable-resume", false, true, true},
		{"no-durable-state", false, false, false},
		{"clean-start-resets", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var state *membership.DeliverySessionStateDTO
			if tc.found && !tc.cleanStart {
				state = durableConnectState()
			} else {
				state = &membership.DeliverySessionStateDTO{NextSequence: 4}
			}
			committer := &recordingDurableConnectCommitter{attachResult: membership.DeliveryResultDTO{Found: tc.found, Session: state}}
			useCase, _, queue, _ := durableConnectFixture(t, now, nil, committer)
			committer.onActivate = func() {
				if queue.resetCalls != 1 {
					t.Errorf("ActivateDelivery preceded ResetDurable: resets=%d", queue.resetCalls)
				}
			}
			result, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "durable-client", CleanStart: tc.cleanStart, SessionExpiryInterval: 120})
			if err != nil || !result.Accepted {
				t.Fatalf("Connect result=%+v err=%v", result, err)
			}
			if result.ConnAck.SessionPresent != tc.wantPresent {
				t.Fatalf("SessionPresent=%v want %v", result.ConnAck.SessionPresent, tc.wantPresent)
			}
			wantQueued := 1
			if tc.cleanStart || !tc.found {
				wantQueued = 0
			}
			if queue.resetCalls != 1 || queue.resetEpoch != 7 || len(queue.resetQueued) != wantQueued {
				t.Fatalf("ResetDurable calls=%d epoch=%d queued=%+v", queue.resetCalls, queue.resetEpoch, queue.resetQueued)
			}
			if wantQueued == 1 && (queue.resetQueued[0].Topic != "durable/queued" || queue.resetQueued[0].DurableSequence != 2 || queue.resetQueued[0].DurableEpoch != 7) {
				t.Fatalf("durable queue projection = %+v", queue.resetQueued)
			}
			if len(committer.activations) != 1 || committer.activations[0].epoch != 7 || committer.activations[0].through != 3 {
				t.Fatalf("activation = %+v", committer.activations)
			}
			if len(committer.changes) == 0 || committer.changes[0].Action != membership.DeliveryActionAttach || committer.changes[0].Reset != tc.cleanStart {
				t.Fatalf("ATTACH = %+v", committer.changes)
			}
		})
	}
}

// Traceability: CLU-05/CLU-06; docs/specs/cluster-session-and-delivery.md §4.2.
func TestConnectDurableResetThenOfflineClaimKeepsDurableFirst(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	committer := &recordingDurableConnectCommitter{attachResult: membership.DeliveryResultDTO{Found: true, Session: durableConnectState()}}
	tenant, _ := tenancy.NewTenantID("default")
	key, _ := tenancy.NewSessionKey(tenant, "ordered")
	offline := &fakeOfflineCoordinator{claimResult: OfflineClaimDTO{Found: true, Deliveries: []deliveryqueue.DeliveryDTO{{SessionID: routing.SessionID(key.RoutingID()), Topic: "offline/qos0", QoS: mqtt.QoSAtMostOnce}}}}
	useCase, _, queue, _ := durableConnectFixture(t, now, offline, committer)
	result, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "ordered", SessionExpiryInterval: 120})
	if err != nil || !result.Accepted {
		t.Fatalf("Connect result=%+v err=%v", result, err)
	}
	if !result.ConnAck.SessionPresent {
		t.Fatal("offline CLAIM should contribute SessionPresent")
	}
	queued, err := queue.ListQueued(result.SessionID, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 2 || queued[0].Topic != "durable/queued" || queued[1].Topic != "offline/qos0" {
		t.Fatalf("queue order = %+v", queued)
	}
	if len(offline.claimCalls) != 1 || queue.resetCalls != 1 {
		t.Fatalf("reset=%d claims=%d", queue.resetCalls, len(offline.claimCalls))
	}
}

// Traceability: CLU-06; docs/specs/cluster-session-and-delivery.md §4.2.
func TestDisconnectDurableDetachAndDiscard(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		clean  bool
		action membership.DeliveryAction
	}{
		{"persistent-detach", false, membership.DeliveryActionDetach},
		{"clean-discard", true, membership.DeliveryActionDiscard},
	} {
		t.Run(tc.name, func(t *testing.T) {
			committer := &recordingDurableConnectCommitter{attachResult: membership.DeliveryResultDTO{Session: durableConnectState()}}
			useCase, _, _, _ := durableConnectFixture(t, now, nil, committer)
			request := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "disconnect-client", CleanStart: tc.clean}
			if !tc.clean {
				request.SessionExpiryInterval = 60
			}
			connected, err := useCase.Connect(request)
			if err != nil || !connected.Accepted {
				t.Fatalf("Connect=%+v err=%v", connected, err)
			}
			if err := useCase.Disconnect(connected.SessionID, session.DisconnectNormal); err != nil {
				t.Fatal(err)
			}
			if len(committer.changes) != 2 || committer.changes[1].Action != tc.action {
				t.Fatalf("lifecycle commands = %+v", committer.changes)
			}
			if tc.action == membership.DeliveryActionDetach {
				want := uint64(now.Add(time.Minute).UnixMilli())
				if committer.changes[1].SessionExpiresAtUnixMillis != want {
					t.Fatalf("expiry=%d want %d", committer.changes[1].SessionExpiresAtUnixMillis, want)
				}
			} else if committer.changes[1].SessionExpiresAtUnixMillis != 0 {
				t.Fatalf("DISCARD expiry unexpectedly set: %+v", committer.changes[1])
			}
		})
	}
}

// Traceability: CLU-05/CLU-06; docs/specs/cluster-session-and-delivery.md §4.2.
func TestConnectDurableRollbackDeactivatesAfterOfflineClaimFailure(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	committer := &recordingDurableConnectCommitter{attachResult: membership.DeliveryResultDTO{Found: true, Session: durableConnectState()}}
	offline := &fakeOfflineCoordinator{claimErr: errors.New("claim failed")}
	useCase, repository, queue, _ := durableConnectFixture(t, now, offline, committer)
	result, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "rollback"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted || result.ConnAck.ReasonCode == 0 {
		t.Fatalf("failed activation result = %+v", result)
	}
	if len(committer.activations) != 1 || len(committer.deactivations) != 1 || committer.deactivations[0].epoch != 7 {
		t.Fatalf("activation=%+v deactivation=%+v", committer.activations, committer.deactivations)
	}
	if _, found, _ := repository.Load("rollback"); found {
		t.Fatal("failed new activation left session persisted")
	}
	if len(queue.removes) == 0 {
		t.Fatalf("failed activation did not remove local queue: %+v", queue.removes)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestConnectRestoresDurableOutboundPhases(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	expiresAt := now.Add(75 * time.Second)
	committer := &recordingDurableConnectCommitter{attachResult: membership.DeliveryResultDTO{
		Found: true,
		Session: &membership.DeliverySessionStateDTO{NextSequence: 4, Messages: []membership.DeliveryMessageDTO{
			{Sequence: 1, Topic: "pending/puback", QoS: 1, PacketID: 11, Phase: membership.DeliveryPhaseAwaitPubAck, ExpiresAtUnixMillis: uint64(expiresAt.UnixMilli())},
			{Sequence: 2, Topic: "pending/pubrec", QoS: 2, PacketID: 12, Phase: membership.DeliveryPhaseAwaitPubRec},
			{Sequence: 3, Topic: "pending/pubcomp", QoS: 2, PacketID: 13, Phase: membership.DeliveryPhaseAwaitPubComp},
		}},
	}}
	useCase, _, queue, _ := durableConnectFixture(t, now, nil, committer)

	result, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "durable-client", SessionExpiryInterval: 120})
	if err != nil || !result.Accepted {
		t.Fatalf("Connect result=%+v err=%v", result, err)
	}
	if len(queue.resetOutbound) != 3 {
		t.Fatalf("restored outbound count=%d, want 3: %+v", len(queue.resetOutbound), queue.resetOutbound)
	}
	for index, want := range []struct {
		packetID mqtt.PacketIdentifier
		qos      mqtt.QoS
		packet   string
	}{
		{11, mqtt.QoSAtLeastOnce, "PUBLISH"},
		{12, mqtt.QoSExactlyOnce, "PUBLISH"},
		{13, mqtt.QoSExactlyOnce, "PUBREL"},
	} {
		outbound := queue.resetOutbound[index]
		if outbound.PacketID != want.packetID || outbound.DurableSequence != uint64(index+1) || outbound.DurableEpoch != 7 {
			t.Fatalf("restored outbound[%d]=%+v", index, outbound)
		}
		action, err := outbound.State.Resume()
		if err != nil {
			t.Fatalf("resume outbound packet %d: %v", want.packetID, err)
		}
		switch packet := action.Send.(type) {
		case *mqtt.Publish:
			if want.packet != "PUBLISH" || packet.PacketID != want.packetID || packet.QoS != want.qos || !packet.DUP {
				t.Fatalf("resumed packet for %d = %#v", want.packetID, packet)
			}
		case *mqtt.PubRel:
			if want.packet != "PUBREL" || packet.PacketID != want.packetID {
				t.Fatalf("resumed packet for %d = %#v", want.packetID, packet)
			}
		default:
			t.Fatalf("resumed packet for %d has type %T", want.packetID, action.Send)
		}
	}
	if !queue.resetOutbound[0].ExpiresAt.Equal(expiresAt) {
		t.Fatalf("restored expiry=%v want %v", queue.resetOutbound[0].ExpiresAt, expiresAt)
	}
	if len(committer.activations) != 1 || committer.activations[0].through != 3 {
		t.Fatalf("activation = %+v, want one activation through sequence 3", committer.activations)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestResetDurableQueueRejectsInvalidReplicatedMessages(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	tenant, err := tenancy.NewTenantID("default")
	if err != nil {
		t.Fatal(err)
	}
	key, err := tenancy.NewSessionKey(tenant, "invalid-state")
	if err != nil {
		t.Fatal(err)
	}
	sessionID := routing.SessionID(key.RoutingID())
	for _, testCase := range []struct {
		name    string
		message membership.DeliveryMessageDTO
	}{
		{name: "queued QoS zero", message: membership.DeliveryMessageDTO{Sequence: 1, Topic: "invalid/qos", Phase: membership.DeliveryPhaseQueued}},
		{name: "PUBACK with QoS 2", message: membership.DeliveryMessageDTO{Sequence: 1, Topic: "invalid/puback", QoS: 2, PacketID: 17, Phase: membership.DeliveryPhaseAwaitPubAck}},
		{name: "PUBREC with QoS 1", message: membership.DeliveryMessageDTO{Sequence: 1, Topic: "invalid/pubrec", QoS: 1, PacketID: 18, Phase: membership.DeliveryPhaseAwaitPubRec}},
		{name: "PUBCOMP without packet ID", message: membership.DeliveryMessageDTO{Sequence: 1, Topic: "invalid/pubcomp", QoS: 2, Phase: membership.DeliveryPhaseAwaitPubComp}},
		{name: "unknown phase", message: membership.DeliveryMessageDTO{Sequence: 1, Topic: "invalid/phase", QoS: 1, Phase: membership.DeliveryPhase(99)}},
		{name: "expiry overflow", message: membership.DeliveryMessageDTO{Sequence: 1, Topic: "invalid/expiry", QoS: 1, Phase: membership.DeliveryPhaseQueued, ExpiresAtUnixMillis: uint64(1 << 63)}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			useCase, _, queue, _ := durableConnectFixture(t, now, nil, &recordingDurableConnectCommitter{})
			err := useCase.resetDurableQueue(sessionID, 7, mqtt.ProtocolV5, membership.DeliverySessionStateDTO{NextSequence: 2, Messages: []membership.DeliveryMessageDTO{testCase.message}}, now)
			if !errors.Is(err, ErrDurableDeliveryRejected) {
				t.Fatalf("resetDurableQueue error=%v, want rejected snapshot", err)
			}
			if queue.resetCalls != 0 {
				t.Fatalf("invalid snapshot replaced local queue %d times", queue.resetCalls)
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestResetDurableQueueSurfacesProjectionFailures(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	tenant, _ := tenancy.NewTenantID("default")
	key, _ := tenancy.NewSessionKey(tenant, "projection-failure")
	sessionID := routing.SessionID(key.RoutingID())
	resetErr := errors.New("local durable queue unavailable")
	useCase, _, queue, _ := durableConnectFixture(t, now, nil, &recordingDurableConnectCommitter{})
	queue.resetErr = resetErr
	err := useCase.resetDurableQueue(sessionID, 7, mqtt.ProtocolV5, membership.DeliverySessionStateDTO{NextSequence: 1}, now)
	if !errors.Is(err, resetErr) {
		t.Fatalf("reset queue error=%v, want %v", err, resetErr)
	}

	activationErr := errors.New("cluster attachment unavailable")
	committer := &recordingDurableConnectCommitter{activationErr: activationErr}
	useCase, _, queue, _ = durableConnectFixture(t, now, nil, committer)
	err = useCase.resetDurableQueue(sessionID, 7, mqtt.ProtocolV5, membership.DeliverySessionStateDTO{NextSequence: 1}, now)
	if !errors.Is(err, activationErr) {
		t.Fatalf("activation error=%v, want %v", err, activationErr)
	}
	if queue.resetCalls != 1 || len(committer.activations) != 1 {
		t.Fatalf("projection setup: resets=%d activations=%d", queue.resetCalls, len(committer.activations))
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestConnectRejectsDurableAttachConflictAndRollsBackSession(t *testing.T) {
	now := time.Date(2026, time.January, 1, 10, 0, 0, 0, time.UTC)
	committer := &recordingDurableConnectCommitter{
		applyResult: &membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeConflict},
	}
	useCase, repository, queue, _ := durableConnectFixture(t, now, nil, committer)

	result, err := useCase.Connect(mqtt.Connect{
		Version: mqtt.ProtocolV5, ClientID: "durable-conflict", SessionExpiryInterval: 120,
	})
	if err != nil || result.Accepted || result.ConnAck.ReasonCode != 0x88 {
		t.Fatalf("CONNECT result=%+v err=%v, want ownership-unavailable rejection", result, err)
	}
	if len(committer.changes) != 1 || committer.changes[0].Action != membership.DeliveryActionAttach {
		t.Fatalf("committed delivery changes=%+v, want one ATTACH", committer.changes)
	}
	if len(repository.keyedStates) != 0 || repository.deleteCall != 1 {
		t.Fatalf("session state after rejected ATTACH: keyed=%v deletes=%d", repository.keyedStates, repository.deleteCall)
	}
	if queue.registered[result.SessionID] {
		t.Fatalf("local queue remained registered after rejected ATTACH: %v", queue.registered)
	}
}

func TestDurableDeliveryHelpersRejectInvalidCommitsAndAttachment(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	tenant, err := tenancy.NewTenantID("default")
	if err != nil {
		t.Fatal(err)
	}
	sessionKey, err := tenancy.NewSessionKey(tenant, "helper-client")
	if err != nil {
		t.Fatal(err)
	}
	sessionID := routing.SessionID(sessionKey.RoutingID())

	t.Run("commit unavailable", func(t *testing.T) {
		useCase, _, _, _, _, _, _ := newUseCase(t, now)
		if _, err := useCase.commitDurableDelivery(context.Background(), membership.DeliveryChangeDTO{}); !errors.Is(err, ErrDurableDeliveryRejected) {
			t.Fatalf("commitDurableDelivery() error = %v, want ErrDurableDeliveryRejected", err)
		}
	})
	t.Run("commit error", func(t *testing.T) {
		wantErr := errors.New("replicated commit failed")
		useCase, _, _, _ := durableConnectFixture(t, now, nil, &recordingDurableConnectCommitter{err: wantErr})
		if _, err := useCase.commitDurableDelivery(context.Background(), membership.DeliveryChangeDTO{}); !errors.Is(err, wantErr) {
			t.Fatalf("commitDurableDelivery() error = %v, want %v", err, wantErr)
		}
	})
	t.Run("commit conflict", func(t *testing.T) {
		useCase, _, _, _ := durableConnectFixture(t, now, nil, &recordingDurableConnectCommitter{
			applyResult: &membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeConflict},
		})
		if _, err := useCase.commitDurableDelivery(context.Background(), membership.DeliveryChangeDTO{}); !errors.Is(err, ErrDurableDeliveryRejected) {
			t.Fatalf("commitDurableDelivery() error = %v, want ErrDurableDeliveryRejected", err)
		}
	})
	t.Run("zero attach time", func(t *testing.T) {
		committer := &recordingDurableConnectCommitter{}
		useCase, _, _, _ := durableConnectFixture(t, now, nil, committer)
		if _, _, err := useCase.attachDurableDelivery(context.Background(), sessionKey, sessionID, 7, mqtt.ProtocolV5, false, time.UnixMilli(0)); !errors.Is(err, ErrDurableDeliveryRejected) {
			t.Fatalf("attachDurableDelivery() error = %v, want ErrDurableDeliveryRejected", err)
		}
		if len(committer.changes) != 0 {
			t.Fatalf("zero-time attach committed changes: %+v", committer.changes)
		}
	})
	t.Run("missing delivery result", func(t *testing.T) {
		committer := &recordingDurableConnectCommitter{applyResult: &membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeApplied}}
		useCase, _, _, _ := durableConnectFixture(t, now, nil, committer)
		if _, _, err := useCase.attachDurableDelivery(context.Background(), sessionKey, sessionID, 7, mqtt.ProtocolV5, false, now); !errors.Is(err, ErrDurableDeliveryRejected) {
			t.Fatalf("attachDurableDelivery() error = %v, want ErrDurableDeliveryRejected", err)
		}
	})
	t.Run("missing session projection", func(t *testing.T) {
		useCase, _, _, _ := durableConnectFixture(t, now, nil, &recordingDurableConnectCommitter{})
		if _, _, err := useCase.attachDurableDelivery(context.Background(), sessionKey, sessionID, 7, mqtt.ProtocolV5, false, now); !errors.Is(err, ErrDurableDeliveryRejected) {
			t.Fatalf("attachDurableDelivery() error = %v, want ErrDurableDeliveryRejected", err)
		}
	})
	t.Run("reset failure", func(t *testing.T) {
		resetErr := errors.New("durable queue reset failed")
		committer := &recordingDurableConnectCommitter{attachResult: membership.DeliveryResultDTO{
			Session: &membership.DeliverySessionStateDTO{NextSequence: 1},
		}}
		useCase, _, queue, _ := durableConnectFixture(t, now, nil, committer)
		queue.resetErr = resetErr
		if _, _, err := useCase.attachDurableDelivery(context.Background(), sessionKey, sessionID, 7, mqtt.ProtocolV5, false, now); !errors.Is(err, resetErr) {
			t.Fatalf("attachDurableDelivery() error = %v, want %v", err, resetErr)
		}
		if queue.resetCalls != 1 {
			t.Fatalf("ResetDurable calls = %d, want 1", queue.resetCalls)
		}
	})
}

func TestCommitDeliveryLifecycleValidatesTimesBeforeCommit(t *testing.T) {
	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	tenant, err := tenancy.NewTenantID("default")
	if err != nil {
		t.Fatal(err)
	}
	sessionKey, err := tenancy.NewSessionKey(tenant, "lifecycle-client")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("no committer", func(t *testing.T) {
		useCase, _, _, _, _, _, _ := newUseCase(t, now)
		if err := useCase.commitDeliveryLifecycle(context.Background(), sessionKey, 0, membership.DeliveryActionDetach, time.Time{}, time.Time{}); err != nil {
			t.Fatalf("commitDeliveryLifecycle() without committer = %v, want nil", err)
		}
	})

	tests := []struct {
		name             string
		epoch            uint64
		proposedAt       time.Time
		sessionExpiresAt time.Time
	}{
		{name: "zero epoch", epoch: 0, proposedAt: now},
		{name: "zero proposal time", epoch: 7},
		{name: "negative expiry time", epoch: 7, proposedAt: now, sessionExpiresAt: time.UnixMilli(-1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			committer := &recordingDurableConnectCommitter{}
			useCase, _, _, _ := durableConnectFixture(t, now, nil, committer)
			err := useCase.commitDeliveryLifecycle(context.Background(), sessionKey, tc.epoch, membership.DeliveryActionDetach, tc.proposedAt, tc.sessionExpiresAt)
			if !errors.Is(err, ErrDurableDeliveryRejected) {
				t.Fatalf("commitDeliveryLifecycle() = %v, want ErrDurableDeliveryRejected", err)
			}
			if len(committer.changes) != 0 {
				t.Fatalf("invalid lifecycle times committed changes: %+v", committer.changes)
			}
		})
	}
}
