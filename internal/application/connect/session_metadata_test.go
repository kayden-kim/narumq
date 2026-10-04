package connect

import (
	"context"
	"errors"
	"testing"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type metadataCommitCall struct {
	change membership.SessionMetadataChangeDTO
}

type fakeSessionMetadataCoordinator struct {
	calls    []metadataCommitCall
	outcomes []SessionMetadataOutcomeDTO
	errors   []error
}

func (coordinator *fakeSessionMetadataCoordinator) CommitSessionMetadata(
	_ context.Context, change membership.SessionMetadataChangeDTO,
) (SessionMetadataOutcomeDTO, error) {
	coordinator.calls = append(coordinator.calls, metadataCommitCall{change: change})
	index := len(coordinator.calls) - 1
	var outcome SessionMetadataOutcomeDTO
	if index < len(coordinator.outcomes) {
		outcome = coordinator.outcomes[index]
	}
	if index < len(coordinator.errors) {
		return outcome, coordinator.errors[index]
	}
	return outcome, nil
}

func configureSessionMetadata(useCase *UseCase, coordinator *fakeSessionMetadataCoordinator, epoch uint64) {
	WithOwnershipCoordinator(&fakeOwnershipCoordinator{epoch: epoch}, 7)(useCase)
	WithSessionMetadataCoordinator(coordinator)(useCase)
}

func metadataWill(topic string, delay uint32) *mqtt.Will {
	return &mqtt.Will{Topic: topic, Payload: []byte("payload"), QoS: byte(mqtt.QoSAtLeastOnce), DelayInterval: delay}
}

func metadataWillDTO(will *mqtt.Will) *membership.SessionWillDTO {
	return &membership.SessionWillDTO{
		Topic: will.Topic, Payload: append([]byte(nil), will.Payload...), QoS: uint32(will.QoS),
		Retain: will.Retain, DelayIntervalSeconds: will.DelayInterval,
	}
}

func connectMetadataSession(t *testing.T, useCase *UseCase, clientID string, expiry uint32, will *mqtt.Will) routing.SessionID {
	t.Helper()
	result, err := useCase.Connect(mqtt.Connect{
		Version: mqtt.ProtocolV5, ClientID: clientID, SessionExpiryInterval: expiry, Will: will,
	})
	if err != nil || !result.Accepted {
		t.Fatalf("Connect() = %#v, %v", result, err)
	}
	return result.SessionID
}

func TestSessionMetadataDisconnectUsesCommittedWillAndSuppressesOnCommitFailure(t *testing.T) {
	// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	localWill := metadataWill("local/will", 0)
	committedWill := metadataWill("committed/will", 0)

	t.Run("committed Will replaces local decision", func(t *testing.T) {
		useCase, _, repository, queue, _, _, publisher := newUseCase(t, now)
		coordinator := &fakeSessionMetadataCoordinator{outcomes: []SessionMetadataOutcomeDTO{{Replicated: true, Will: committedWill}}}
		configureSessionMetadata(useCase, coordinator, 4)
		sessionID := connectMetadataSession(t, useCase, "committed", 30, localWill)
		removedBefore := len(queue.removes)
		if err := useCase.Disconnect(sessionID, session.DisconnectAbnormal); err != nil {
			t.Fatal(err)
		}
		if len(publisher.published) != 1 || publisher.published[0].will.Topic != committedWill.Topic {
			t.Fatalf("published Wills = %#v, want only committed Will %q", publisher.published, committedWill.Topic)
		}
		if len(coordinator.calls) != 1 || coordinator.calls[0].change.Action != membership.SessionMetadataActionDetach ||
			coordinator.calls[0].change.WillDisposition != membership.SessionWillDispositionClaim {
			t.Fatalf("metadata commits = %#v, want DETACH/CLAIM", coordinator.calls)
		}
		if _, found := repository.states["committed"]; !found || len(queue.removes) != removedBefore {
			t.Fatalf("disconnect lifecycle lost persistent session or changed queue: saved=%v removes=%v", found, queue.removes)
		}
	})

	t.Run("commit failure suppresses Will", func(t *testing.T) {
		useCase, _, repository, queue, _, _, publisher := newUseCase(t, now)
		commitErr := errors.New("quorum unavailable")
		coordinator := &fakeSessionMetadataCoordinator{errors: []error{commitErr}}
		configureSessionMetadata(useCase, coordinator, 4)
		sessionID := connectMetadataSession(t, useCase, "failed", 30, localWill)
		removedBefore := len(queue.removes)
		if err := useCase.Disconnect(sessionID, session.DisconnectAbnormal); !errors.Is(err, commitErr) {
			t.Fatalf("Disconnect() error = %v, want %v", err, commitErr)
		}
		if len(publisher.published) != 0 {
			t.Fatalf("published Will after failed commit: %#v", publisher.published)
		}
		if _, found := repository.states["failed"]; !found || len(queue.removes) != removedBefore {
			t.Fatalf("failed metadata commit lost detached session or changed queue: saved=%v removes=%v", found, queue.removes)
		}
	})

	t.Run("inactive feature retains local behavior", func(t *testing.T) {
		useCase, _, _, _, _, _, publisher := newUseCase(t, now)
		coordinator := &fakeSessionMetadataCoordinator{outcomes: []SessionMetadataOutcomeDTO{{Replicated: false}}}
		configureSessionMetadata(useCase, coordinator, 4)
		sessionID := connectMetadataSession(t, useCase, "inactive", 30, localWill)
		if err := useCase.Disconnect(sessionID, session.DisconnectAbnormal); err != nil {
			t.Fatal(err)
		}
		if len(publisher.published) != 1 || publisher.published[0].will.Topic != localWill.Topic {
			t.Fatalf("published Wills = %#v, want local Will %q", publisher.published, localWill.Topic)
		}
	})
}

func TestSessionMetadataWillTimerCommitsClaimBeforePublish(t *testing.T) {
	// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	will := metadataWill("timer/will", 1)
	coordinator := &fakeSessionMetadataCoordinator{outcomes: []SessionMetadataOutcomeDTO{
		{Replicated: true}, {Replicated: true, Will: will},
	}}
	useCase, _, repository, _, _, scheduler, publisher := newUseCase(t, now)
	configureSessionMetadata(useCase, coordinator, 8)
	sessionID := connectMetadataSession(t, useCase, "timer", 30, will)
	if err := useCase.Disconnect(sessionID, session.DisconnectAbnormal); err != nil {
		t.Fatal(err)
	}
	if len(scheduler.timers) != 2 || scheduler.timers[0].delay != time.Second {
		t.Fatalf("scheduled timers = %#v, want delayed Will and expiry", scheduler.timers)
	}
	scheduler.timers[0].callback()
	if len(coordinator.calls) != 2 || coordinator.calls[1].change.Action != membership.SessionMetadataActionClaimWill {
		t.Fatalf("metadata calls = %#v, want CLAIM_WILL after DETACH", coordinator.calls)
	}
	if len(publisher.published) != 1 || publisher.published[0].will.Topic != will.Topic {
		t.Fatalf("published Wills = %#v, want claimed Will %q", publisher.published, will.Topic)
	}
	state, found := repository.states["timer"]
	if !found || state.Will() == nil || !state.WillPublished() {
		t.Fatalf("local Will claim was not persisted: found=%v will=%v published=%v", found, found && state.Will() != nil, found && state.WillPublished())
	}
}

func TestSessionMetadataExpiryStaleDeletesLocalStateWithoutRetry(t *testing.T) {
	// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	coordinator := &fakeSessionMetadataCoordinator{outcomes: []SessionMetadataOutcomeDTO{{Replicated: false}}, errors: []error{nil, ErrSessionOwnershipStale}}
	useCase, clock, repository, queue, _, scheduler, publisher := newUseCase(t, now)
	configureSessionMetadata(useCase, coordinator, 3)
	sessionID := connectMetadataSession(t, useCase, "expiry", 1, metadataWill("expiry/will", 0))
	removedBefore := len(queue.removes)
	if err := useCase.Disconnect(sessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	clock.now = now.Add(time.Second)
	var expiryTimer *fakeTimer
	for _, timer := range scheduler.timers {
		if timer.delay == time.Second {
			expiryTimer = timer
		}
	}
	if expiryTimer == nil {
		t.Fatalf("no expiry timer scheduled: %#v", scheduler.timers)
	}
	expiryTimer.callback()
	if _, found := repository.states["expiry"]; found {
		t.Fatal("stale owner retained expired local session")
	}
	if len(queue.removes) != removedBefore+1 || queue.removes[len(queue.removes)-1] != sessionID {
		t.Fatalf("queue removals = %v, want local queue cleanup for %q", queue.removes, sessionID)
	}
	if timers := useCase.activeTimers[string(sessionID)]; timers != nil {
		t.Fatalf("stale owner scheduled retry timer: %#v", timers)
	}
	if len(publisher.published) != 0 {
		t.Fatalf("stale owner published Will: %#v", publisher.published)
	}
}

func recoveredMetadata(clientID string, epoch uint64, connected bool, now time.Time) RecoveredSessionDTO {
	return RecoveredSessionDTO{OwnershipEpoch: epoch, Metadata: membership.SessionMetadataStateDTO{
		TenantID: "tenant", ClientID: clientID, Epoch: epoch, ProtocolVersion: 5,
		Retention: membership.SessionRetentionFinite, ExpiryIntervalSeconds: 30,
		Connected: connected, Will: metadataWillDTO(metadataWill("recovered/"+clientID, 0)),
	}}
}

func TestRecoverOwnedConnectedSessionDisconnectsAndCommitsWill(t *testing.T) {
	// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	will := metadataWill("recovered/current", 0)
	coordinator := &fakeSessionMetadataCoordinator{outcomes: []SessionMetadataOutcomeDTO{{Replicated: true, Will: will}}}
	useCase, _, repository, queue, _, scheduler, publisher := newUseCase(t, now)
	configureSessionMetadata(useCase, coordinator, 11)
	recovered := recoveredMetadata("current", 11, true, now)
	recovered.Metadata.Will = metadataWillDTO(will)
	if err := useCase.RecoverOwnedSession(context.Background(), recovered); err != nil {
		t.Fatal(err)
	}
	state, found := repository.states["current"]
	if !found || state.Connected() || state.Will() == nil || !state.WillPublished() {
		t.Fatalf("recovered lifecycle = %#v found=%v; want detached and committed Will claimed", state, found)
	}
	if len(coordinator.calls) != 1 || coordinator.calls[0].change.Action != membership.SessionMetadataActionDetach ||
		coordinator.calls[0].change.WillDisposition != membership.SessionWillDispositionClaim ||
		coordinator.calls[0].change.SessionExpiresAtUnixMillis != uint64(now.Add(30*time.Second).UnixMilli()) {
		t.Fatalf("recovery metadata commits = %#v, want abnormal DETACH/CLAIM with finite expiry", coordinator.calls)
	}
	if len(publisher.published) != 1 || publisher.published[0].will.Topic != will.Topic {
		t.Fatalf("published Wills = %#v, want recovered Will %q", publisher.published, will.Topic)
	}
	tenant, _ := tenancy.NewTenantID("tenant")
	currentKey, err := tenancy.NewSessionKey(tenant, "current")
	if err != nil {
		t.Fatal(err)
	}
	if len(queue.registers) != 1 || queue.registers[0] != routing.SessionID(currentKey.RoutingID()) {
		t.Fatalf("delivery registrations = %q, want recovered session registered", queue.registers)
	}
	if len(scheduler.timers) != 1 || scheduler.timers[0].delay != 30*time.Second {
		t.Fatalf("recovery expiry timers = %#v, want 30 seconds", scheduler.timers)
	}
}

func TestRecoverOwnedPreviousEpochAppliesPlannedHandoffWillRules(t *testing.T) {
	// Traceability: DEP-07/DEP-12; docs/specs/drain-and-rolling-update.md §5.
	for _, test := range []struct {
		name    string
		version uint32
		delay   uint32
		reason  string
	}{
		{"zero", 5, 0, "expected_zero_or_expired_will_delay"},
		{"delayed", 5, 5, "expected_zero_or_expired_will_delay"},
		{"mqtt311", 4, 0, "expected_mqtt311_server_disconnect"},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			will := metadataWill("recovered/previous", test.delay)
			outcomes := []SessionMetadataOutcomeDTO{{Replicated: true, Will: will}}
			if test.delay != 0 {
				outcomes = []SessionMetadataOutcomeDTO{{Replicated: true}, {Replicated: true, Will: will}}
			}
			coordinator := &fakeSessionMetadataCoordinator{outcomes: outcomes}
			useCase, _, _, _, _, scheduler, publisher := newUseCase(t, now)
			configureSessionMetadata(useCase, coordinator, 12)
			recorder := &fakeWillOutcomes{}
			WithWillOutcomeRecorder(recorder)(useCase)
			recovered := recoveredMetadata("previous", 11, true, now)
			recovered.OwnershipEpoch = 12
			recovered.Metadata.ProtocolVersion = test.version
			recovered.Metadata.Will = metadataWillDTO(will)
			if test.version == 4 {
				recovered.Metadata.Retention = membership.SessionRetentionInfinite
				recovered.Metadata.ExpiryIntervalSeconds = 0
			}
			if err := useCase.RecoverOwnedSession(context.Background(), recovered); err != nil {
				t.Fatal(err)
			}
			wantDisposition := membership.SessionWillDispositionClaim
			if test.delay != 0 {
				wantDisposition = membership.SessionWillDispositionSchedule
				if len(publisher.published) != 0 || scheduler.timers[0].delay != 5*time.Second {
					t.Fatal("delayed recovery published immediately or used wrong delay")
				}
				scheduler.timers[0].callback()
			}
			if coordinator.calls[0].change.WillDisposition != wantDisposition || len(publisher.published) != 1 || len(recorder.reasons) != 1 || recorder.reasons[0] != test.reason {
				t.Fatalf("calls=%+v publishes=%+v reasons=%v", coordinator.calls, publisher.published, recorder.reasons)
			}
			if err := useCase.RecoverOwnedSession(context.Background(), recovered); err != nil {
				t.Fatal(err)
			}
			if len(publisher.published) != 1 || len(recorder.reasons) != 1 {
				t.Fatal("recovery repeated final outcome")
			}
		})
	}
}

func TestRecoverOwnedDetachedSessionUsesAbsoluteTimerDeadlines(t *testing.T) {
	// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name        string
		willDue     time.Time
		expiresAt   time.Time
		willDelay   time.Duration
		expiryDelay time.Duration
	}{
		{name: "future absolute deadlines", willDue: now.Add(4 * time.Second), expiresAt: now.Add(20 * time.Second), willDelay: 4 * time.Second, expiryDelay: 20 * time.Second},
		{name: "overdue Will fires immediately", willDue: now.Add(-time.Second), expiresAt: now.Add(20 * time.Second), willDelay: 0, expiryDelay: 20 * time.Second},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			useCase, _, repository, queue, _, scheduler, publisher := newUseCase(t, now)
			coordinator := &fakeSessionMetadataCoordinator{}
			configureSessionMetadata(useCase, coordinator, 17)
			dto := recoveredMetadata("detached", 17, false, now)
			dto.Metadata.DetachedAtUnixMillis = uint64(now.Add(-time.Second).UnixMilli())
			dto.Metadata.SessionExpiresAtUnixMillis = uint64(testCase.expiresAt.UnixMilli())
			dto.Metadata.Will = metadataWillDTO(metadataWill("recovered/detached", 5))
			dto.Metadata.WillDueAtUnixMillis = uint64(testCase.willDue.UnixMilli())
			if err := useCase.RecoverOwnedSession(context.Background(), dto); err != nil {
				t.Fatal(err)
			}
			state, found := repository.states["detached"]
			if !found || state.Connected() || state.Will() == nil || state.WillPublished() || state.WillDiscarded() {
				t.Fatalf("recovered detached lifecycle = %#v found=%v", state, found)
			}
			if len(scheduler.timers) != 2 || scheduler.timers[0].delay != testCase.willDelay || scheduler.timers[1].delay != testCase.expiryDelay {
				t.Fatalf("absolute timer delays = %#v, want Will=%s expiry=%s", scheduler.timers, testCase.willDelay, testCase.expiryDelay)
			}
			if len(queue.registers) != 0 || len(publisher.published) != 0 {
				t.Fatalf("detached restore registered/published unexpectedly: registers=%v published=%v", queue.registers, publisher.published)
			}
		})
	}
}

func TestRecoverOwnedSessionSkipsConnectedAndAlreadyProcessedEpoch(t *testing.T) {
	// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	useCase, _, repository, queue, _, scheduler, publisher := newUseCase(t, now)
	configureSessionMetadata(useCase, &fakeSessionMetadataCoordinator{}, 21)
	connectMetadataSession(t, useCase, "local", 30, metadataWill("local/will", 0))
	alreadyProcessed := recoveredMetadata("processed", 21, false, now)
	alreadyProcessed.Metadata.DetachedAtUnixMillis = uint64(now.UnixMilli())
	alreadyProcessed.Metadata.SessionExpiresAtUnixMillis = uint64(now.Add(time.Minute).UnixMilli())
	tenant, _ := tenancy.NewTenantID("tenant")
	key, err := tenancy.NewSessionKey(tenant, "processed")
	if err != nil {
		t.Fatal(err)
	}
	localState, err := session.Restore(session.RestoreInput{
		Key: key, Version: mqtt.ProtocolV5, Retention: session.RetentionFinite, ExpiryInterval: 30,
		ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveKey(key, localState); err != nil {
		t.Fatal(err)
	}
	processedID := routing.SessionID(key.RoutingID())
	useCase.ownershipEpochs[processedID] = 21
	localMetadata := recoveredMetadata("local", 21, true, now)
	localMetadata.Metadata.TenantID = "default"
	if err := useCase.RecoverOwnedSession(context.Background(), localMetadata); err != nil {
		t.Fatal(err)
	}
	if err := useCase.RecoverOwnedSession(context.Background(), alreadyProcessed); err != nil {
		t.Fatal(err)
	}
	if repository.saveCalls != 2 || len(queue.registers) != 1 || len(scheduler.timers) != 0 || len(publisher.published) != 0 {
		t.Fatalf("idempotence mutated state: saves=%d registers=%v timers=%#v published=%#v", repository.saveCalls, queue.registers, scheduler.timers, publisher.published)
	}
	if state, found := repository.states["local"]; !found || !state.Connected() {
		t.Fatal("local connected lifecycle was not preserved")
	}
}

func TestRecoverOwnedSessionReturnsCommitErrorForRetry(t *testing.T) {
	// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	commitErr := errors.New("metadata quorum unavailable")
	will := metadataWill("recovered/retry", 0)
	coordinator := &fakeSessionMetadataCoordinator{
		outcomes: []SessionMetadataOutcomeDTO{{Replicated: true, Will: will}, {Replicated: true, Will: will}},
		errors:   []error{commitErr, nil},
	}
	useCase, _, repository, _, _, _, publisher := newUseCase(t, now)
	configureSessionMetadata(useCase, coordinator, 31)
	dto := recoveredMetadata("retry", 31, true, now)
	if err := useCase.RecoverOwnedSession(context.Background(), dto); !errors.Is(err, commitErr) {
		t.Fatalf("first recovery error = %v, want %v", err, commitErr)
	}
	if len(publisher.published) != 0 {
		t.Fatalf("published Will despite failed recovery commit: %#v", publisher.published)
	}
	if err := useCase.RecoverOwnedSession(context.Background(), dto); err != nil {
		t.Fatalf("retry recovery error = %v", err)
	}
	if len(coordinator.calls) != 2 {
		t.Fatalf("metadata commit calls = %d, want retry after error", len(coordinator.calls))
	}
	if len(publisher.published) != 1 {
		t.Fatalf("retry should publish after successful commit: %#v", publisher.published)
	}
	state, found := repository.states["retry"]
	if !found || state.Connected() {
		t.Fatalf("retry recovery state = %#v found=%v, want detached", state, found)
	}
}
