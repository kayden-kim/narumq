package connect

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	applicationauthentication "narumq/internal/application/ports/authentication"
	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type fakeClock struct {
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	return clock.now
}

type fakeSessionRepository struct {
	states     map[string]session.Lifecycle
	loadErr    error
	saveErr    error
	deleteErr  error
	loadCalls  int
	saveCalls  int
	deleteCall int
}

func newFakeSessionRepository() *fakeSessionRepository {
	return &fakeSessionRepository{states: make(map[string]session.Lifecycle)}
}

func (repository *fakeSessionRepository) Load(clientID string) (session.Lifecycle, bool, error) {
	repository.loadCalls++
	if repository.loadErr != nil {
		return session.Lifecycle{}, false, repository.loadErr
	}
	state, found := repository.states[clientID]
	return state, found, nil
}

func (repository *fakeSessionRepository) Save(state session.Lifecycle) error {
	repository.saveCalls++
	if repository.saveErr != nil {
		return repository.saveErr
	}
	repository.states[state.ClientID()] = state
	return nil
}

func (repository *fakeSessionRepository) Delete(clientID string) error {
	repository.deleteCall++
	if repository.deleteErr != nil {
		return repository.deleteErr
	}
	delete(repository.states, clientID)
	return nil
}

type fakeDeliveryQueue struct {
	registered  map[routing.SessionID]bool
	registerErr error
	registers   []routing.SessionID
	removes     []routing.SessionID
}

func newFakeDeliveryQueue() *fakeDeliveryQueue {
	return &fakeDeliveryQueue{registered: make(map[routing.SessionID]bool)}
}

func (queue *fakeDeliveryQueue) Register(sessionID routing.SessionID) error {
	queue.registers = append(queue.registers, sessionID)
	if queue.registerErr != nil {
		return queue.registerErr
	}
	queue.registered[sessionID] = true
	return nil
}

func (queue *fakeDeliveryQueue) Remove(sessionID routing.SessionID) {
	queue.removes = append(queue.removes, sessionID)
	delete(queue.registered, sessionID)
}

func (*fakeDeliveryQueue) TryEnqueue(deliveryqueue.DeliveryDTO, time.Time) error {
	return nil
}

func (*fakeDeliveryQueue) TryEnqueueBatch([]deliveryqueue.DeliveryDTO, time.Time) error {
	return nil
}

func (*fakeDeliveryQueue) Drain(routing.SessionID, time.Time) ([]deliveryqueue.DeliveryDTO, error) {
	return nil, nil
}

type fakeTimer struct {
	stopped  bool
	callback func()
	delay    time.Duration
}

func (t *fakeTimer) Stop() bool {
	if t.stopped {
		return false
	}
	t.stopped = true
	return true
}

type fakeScheduler struct {
	timers []*fakeTimer
}

func (s *fakeScheduler) AfterFunc(d time.Duration, f func()) Timer {
	timer := &fakeTimer{callback: f, delay: d}
	s.timers = append(s.timers, timer)
	return timer
}

type publishedWill struct {
	sessionID routing.SessionID
	will      mqtt.Will
}

type fakeWillPublisher struct {
	published []publishedWill
	err       error
}

func (p *fakeWillPublisher) PublishWill(sessionID routing.SessionID, will mqtt.Will) error {
	p.published = append(p.published, publishedWill{sessionID: sessionID, will: will})
	return p.err
}

func newUseCase(t *testing.T, now time.Time) (*UseCase, *fakeClock, *fakeSessionRepository, *fakeDeliveryQueue, *routing.Table, *fakeScheduler, *fakeWillPublisher) {
	t.Helper()
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	routes := routing.NewTable()
	scheduler := &fakeScheduler{}
	willPublisher := &fakeWillPublisher{}
	useCase, err := New(clock, repository, queue, routes, scheduler, willPublisher)
	if err != nil {
		t.Fatal(err)
	}
	return useCase, clock, repository, queue, routes, scheduler, willPublisher
}

func TestNewRejectsNilDependencies(t *testing.T) {
	clock := &fakeClock{}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	routes := routing.NewTable()
	scheduler := &fakeScheduler{}
	willPublisher := &fakeWillPublisher{}
	cases := []struct {
		name string
		want error
		make func() (applicationclock.Clock, SessionRepository, deliveryqueue.Queue, *routing.Table, Scheduler, WillPublisher)
	}{
		{name: "clock", want: ErrNilClock, make: func() (applicationclock.Clock, SessionRepository, deliveryqueue.Queue, *routing.Table, Scheduler, WillPublisher) {
			return nil, repository, queue, routes, scheduler, willPublisher
		}},
		{name: "repository", want: ErrNilSessionRepository, make: func() (applicationclock.Clock, SessionRepository, deliveryqueue.Queue, *routing.Table, Scheduler, WillPublisher) {
			return clock, nil, queue, routes, scheduler, willPublisher
		}},
		{name: "queue", want: ErrNilDeliveryQueue, make: func() (applicationclock.Clock, SessionRepository, deliveryqueue.Queue, *routing.Table, Scheduler, WillPublisher) {
			return clock, repository, nil, routes, scheduler, willPublisher
		}},
		{name: "routes", want: ErrNilRoutingTable, make: func() (applicationclock.Clock, SessionRepository, deliveryqueue.Queue, *routing.Table, Scheduler, WillPublisher) {
			return clock, repository, queue, nil, scheduler, willPublisher
		}},
		{name: "scheduler", want: ErrNilScheduler, make: func() (applicationclock.Clock, SessionRepository, deliveryqueue.Queue, *routing.Table, Scheduler, WillPublisher) {
			return clock, repository, queue, routes, nil, willPublisher
		}},
		{name: "willPublisher", want: ErrNilWillPublisher, make: func() (applicationclock.Clock, SessionRepository, deliveryqueue.Queue, *routing.Table, Scheduler, WillPublisher) {
			return clock, repository, queue, routes, scheduler, nil
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			c, rep, q, r, s, wp := testCase.make()
			if _, err := New(c, rep, q, r, s, wp); !errors.Is(err, testCase.want) {
				t.Fatalf("New() error = %v, want %v", err, testCase.want)
			}
		})
	}
}

// Traceability: PRD MQTT-03/05/TEST-03; docs/specs/mqtt-protocol-and-session.md §2.2 and §6.
func TestConnectRejectsUnsupportedDevelopmentStateWithoutCreatingSession(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		packet     mqtt.Connect
		wantReason byte
	}{
		{name: "empty-v311", packet: mqtt.Connect{Version: mqtt.ProtocolV311}, wantReason: 0x02},
		{name: "empty-v5", packet: mqtt.Connect{Version: mqtt.ProtocolV5}, wantReason: 0x85},
		{name: "username-v311", packet: mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "client", HasUsername: true, Username: "user"}, wantReason: 0x03},
		{name: "password-v5", packet: mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", HasPassword: true, HasUsername: true, Password: []byte("secret")}, wantReason: 0x83},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			useCase, _, repository, queue, _, _, _ := newUseCase(t, now)
			result, err := useCase.Connect(testCase.packet)
			if err != nil {
				t.Fatal(err)
			}
			if result.Accepted || result.ConnAck.ReasonCode != testCase.wantReason || result.ConnAck.SessionPresent {
				t.Fatalf("Connect() = %#v, want rejected reason %#x", result, testCase.wantReason)
			}
			if repository.loadCalls != 0 || repository.saveCalls != 0 || len(queue.registers) != 0 {
				t.Fatalf("rejected CONNECT touched lifecycle: loads=%d saves=%d registers=%v", repository.loadCalls, repository.saveCalls, queue.registers)
			}
		})
	}
}

// Traceability: PRD MQTT-03/TEST-03/04; docs/specs/mqtt-protocol-and-session.md §2.2.
func TestConnectCleanResumeAndExpiryResetRoutingAndQueue(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, clock, _, queue, routes, _, _ := newUseCase(t, now)
	connect := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", SessionExpiryInterval: 10}
	first, err := useCase.Connect(connect)
	if err != nil || !first.Accepted || first.ConnAck.SessionPresent {
		t.Fatalf("initial Connect() = %#v, %v", first, err)
	}
	if _, err := routes.Subscribe(first.SessionID, mqtt.Subscription{Filter: "sensors/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect(first.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	resumed, err := useCase.Connect(connect)
	if err != nil || !resumed.Accepted || !resumed.ConnAck.SessionPresent {
		t.Fatalf("resumed Connect() = %#v, %v", resumed, err)
	}
	matches, err := routes.Match("publisher", "sensors/room", mqtt.QoSAtMostOnce, false)
	if err != nil || !reflect.DeepEqual(matches, []routing.Match{{SessionID: "client", QoS: mqtt.QoSAtMostOnce}}) {
		t.Fatalf("resumed routes = %#v, %v", matches, err)
	}
	if len(queue.removes) != 2 || len(queue.registers) != 2 {
		t.Fatalf("queue lifecycle = removes=%v registers=%v, want two removes and two registers", queue.removes, queue.registers)
	}

	clock.now = now.Add(1 * time.Second)
	cleanStart := connect
	cleanStart.CleanStart = true
	fresh, err := useCase.Connect(cleanStart)
	if err != nil || fresh.Accepted {
		t.Fatalf("duplicate clean-start Connect() = %#v, %v, want negative CONNACK", fresh, err)
	}

	if err := useCase.Disconnect(resumed.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	fresh, err = useCase.Connect(cleanStart)
	if err != nil || !fresh.Accepted || fresh.ConnAck.SessionPresent {
		t.Fatalf("clean-start replacement = %#v, %v", fresh, err)
	}
	matches, err = routes.Match("publisher", "sensors/room", mqtt.QoSAtMostOnce, false)
	if err != nil || len(matches) != 0 {
		t.Fatalf("routes after clean-start replacement = %#v, %v", matches, err)
	}

	if err := useCase.Disconnect(fresh.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	finite := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "finite", SessionExpiryInterval: 2}
	if result, err := useCase.Connect(finite); err != nil || !result.Accepted {
		t.Fatalf("finite initial Connect() = %#v, %v", result, err)
	}
	if _, err := routes.Subscribe("finite", mqtt.Subscription{Filter: "alerts/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect("finite", session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(3 * time.Second)
	expired, err := useCase.Connect(finite)
	if err != nil || !expired.Accepted || expired.ConnAck.SessionPresent {
		t.Fatalf("expired Connect() = %#v, %v", expired, err)
	}
	matches, err = routes.Match("publisher", "alerts/critical", mqtt.QoSAtMostOnce, false)
	if err != nil || len(matches) != 0 {
		t.Fatalf("routes after expiry reset = %#v, %v", matches, err)
	}
}

// Traceability: PRD MQTT-03/TEST-03; duplicate ownership is rejected without takeover.
func TestConnectRejectsDuplicateAndRepositoryErrors(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, repository, queue, _, _, _ := newUseCase(t, now)
	packet := mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "client"}
	accepted, err := useCase.Connect(packet)
	if err != nil || !accepted.Accepted {
		t.Fatalf("initial Connect() = %#v, %v", accepted, err)
	}
	duplicate, err := useCase.Connect(packet)
	if err != nil || duplicate.Accepted || duplicate.ConnAck.ReasonCode != 0x02 {
		t.Fatalf("duplicate Connect() = %#v, %v", duplicate, err)
	}
	if len(queue.registers) != 1 || repository.saveCalls != 1 {
		t.Fatalf("duplicate changed lifecycle: registers=%v saves=%d", queue.registers, repository.saveCalls)
	}

	loadErr := errors.New("load failed")
	repository.loadErr = loadErr
	if _, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "other"}); !errors.Is(err, loadErr) {
		t.Fatalf("load error = %v, want %v", err, loadErr)
	}
	repository.loadErr = nil
	repository.saveErr = errors.New("save failed")
	if _, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "other"}); !errors.Is(err, repository.saveErr) {
		t.Fatalf("save error = %v, want %v", err, repository.saveErr)
	}
}

func TestConnectRollsBackRepositoryWhenQueueRegistrationFails(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	queue.registerErr = errors.New("register failed")
	useCase, err := New(clock, repository, queue, routing.NewTable(), &fakeScheduler{}, &fakeWillPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client"}); !errors.Is(err, queue.registerErr) {
		t.Fatalf("Connect() error = %v, want %v", err, queue.registerErr)
	}
	if _, found, err := repository.Load("client"); err != nil || found {
		t.Fatalf("repository after registration failure = found %v, err %v", found, err)
	}
}

// Traceability: PRD MQTT-03/TEST-03/04; docs/specs/mqtt-protocol-and-session.md §2.2.
func TestConnectRollsBackExistingSessionWhenQueueRegistrationFails(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	useCase, err := New(clock, repository, queue, routing.NewTable(), &fakeScheduler{}, &fakeWillPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	packet := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "persistent", SessionExpiryInterval: 60}
	initial, err := useCase.Connect(packet)
	if err != nil || !initial.Accepted {
		t.Fatalf("initial Connect() = %#v, %v", initial, err)
	}
	if err := useCase.Disconnect(initial.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	before, found, err := repository.Load("persistent")
	if err != nil || !found || before.Connected() {
		t.Fatalf("stored disconnected state = (%#v, %v, %v)", before, found, err)
	}

	queue.registerErr = errors.New("resume register failed")
	if _, err := useCase.Connect(packet); !errors.Is(err, queue.registerErr) {
		t.Fatalf("resume Connect() error = %v, want %v", err, queue.registerErr)
	}
	after, found, err := repository.Load("persistent")
	if err != nil || !found || after.Connected() || after.ClientID() != before.ClientID() {
		t.Fatalf("repository after resume registration failure = (%#v, %v, %v), want prior disconnected state", after, found, err)
	}
}

// Traceability: PRD MQTT-03/TEST-03/04; docs/specs/mqtt-protocol-and-session.md §2.2.
func TestDisconnectRemovesImmediateSessionsAndPreservesPersistentRoutes(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, repository, queue, routes, _, _ := newUseCase(t, now)
	persistent := mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "persistent"}
	if result, err := useCase.Connect(persistent); err != nil || !result.Accepted {
		t.Fatalf("persistent Connect() = %#v, %v", result, err)
	}
	if _, err := routes.Subscribe("persistent", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect("persistent", session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	state, found, err := repository.Load("persistent")
	if err != nil || !found || state.Connected() {
		t.Fatalf("persistent stored state = (%#v, %v, %v)", state, found, err)
	}
	matches, err := routes.Match("publisher", "events/created", mqtt.QoSAtMostOnce, false)
	if err != nil || len(matches) != 1 {
		t.Fatalf("persistent routes after disconnect = %#v, %v", matches, err)
	}

	immediate := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "immediate", SessionExpiryInterval: 0}
	if result, err := useCase.Connect(immediate); err != nil || !result.Accepted {
		t.Fatalf("immediate Connect() = %#v, %v", result, err)
	}
	if _, err := routes.Subscribe("immediate", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect("immediate", session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	if _, found, err := repository.Load("immediate"); err != nil || found {
		t.Fatalf("immediate repository entry = found %v, err %v", found, err)
	}
	matches, err = routes.Match("publisher", "events/created", mqtt.QoSAtMostOnce, false)
	if err != nil || len(matches) != 1 || matches[0].SessionID != "persistent" {
		t.Fatalf("routes after immediate disconnect = %#v, %v", matches, err)
	}
	if len(queue.removes) != 4 {
		t.Fatalf("queue removes = %v, want four lifecycle removals", queue.removes)
	}
}
func TestConnectNilUnsupportedAndDisconnectErrors(t *testing.T) {
	var nilUseCase *UseCase
	if _, err := nilUseCase.Connect(mqtt.Connect{}); !errors.Is(err, ErrNilSessionRepository) {
		t.Fatalf("nil Connect() error = %v", err)
	}
	if err := nilUseCase.Disconnect("session", session.DisconnectNormal); !errors.Is(err, ErrNilSessionRepository) {
		t.Fatalf("nil Disconnect() error = %v", err)
	}
	useCase, _, repository, _, _, _, _ := newUseCase(t, time.Unix(0, 0))
	if _, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolUnknown, ClientID: "client"}); !errors.Is(err, mqtt.ErrUnsupportedProtocolVersion) {
		t.Fatalf("unsupported CONNECT error = %v", err)
	}
	if err := useCase.Disconnect("missing", session.DisconnectNormal); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing Disconnect() error = %v", err)
	}
	loadErr := errors.New("disconnect load failed")
	repository.loadErr = loadErr
	if err := useCase.Disconnect("client", session.DisconnectNormal); !errors.Is(err, loadErr) {
		t.Fatalf("Disconnect load error = %v", err)
	}
}

func TestConnectLifecycleRollbackAndSessionErrors(t *testing.T) {
	now := time.Unix(0, 0)
	useCase, _, repository, queue, _, _, _ := newUseCase(t, now)
	repository.states["client"] = session.Lifecycle{}
	if _, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client"}); err == nil {
		t.Fatal("Connect unexpectedly accepted identity-mismatched lifecycle")
	}
	delete(repository.states, "client")
	accepted, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "persistent", SessionExpiryInterval: 10})
	if err != nil || !accepted.Accepted {
		t.Fatalf("initial persistent Connect() = %#v, %v", accepted, err)
	}
	queue.registerErr = errors.New("resume queue failure")
	if err := useCase.Disconnect(accepted.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	queue.registerErr = nil
	if _, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "persistent", SessionExpiryInterval: 10}); err != nil {
		t.Fatal(err)
	}
	repository.saveErr = errors.New("disconnect save failure")
	if err := useCase.Disconnect("persistent", session.DisconnectNormal); !errors.Is(err, repository.saveErr) {
		t.Fatalf("persistent Disconnect() save error = %v", err)
	}
	repository.saveErr = nil
}

func TestDisconnectRejectsAlreadyDisconnectedSession(t *testing.T) {
	useCase, _, _, _, _, _, _ := newUseCase(t, time.Unix(0, 0))
	result, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "disconnected"})
	if err != nil || !result.Accepted {
		t.Fatalf("Connect() = %#v, %v", result, err)
	}
	if err := useCase.Disconnect(result.SessionID, session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect(result.SessionID, session.DisconnectNormal); !errors.Is(err, session.ErrNotConnected) {
		t.Fatalf("second Disconnect() error = %v", err)
	}
}

func TestConnectDeleteErrorOnImmediateDisconnect(t *testing.T) {
	useCase, _, repository, _, _, _, _ := newUseCase(t, time.Unix(0, 0))
	result, err := useCase.Connect(mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "immediate", SessionExpiryInterval: 0})
	if err != nil || !result.Accepted {
		t.Fatal(err)
	}
	repository.deleteErr = errors.New("delete failed")
	if err := useCase.Disconnect(result.SessionID, session.DisconnectNormal); !errors.Is(err, repository.deleteErr) {
		t.Fatalf("immediate Disconnect() delete error = %v", err)
	}
}

// Traceability: PRD MQTT-01/TEST-04; MQTT-3.1.2-8; MQTT-3.1.2-10; MQTT-3.1.3-9.
func TestConnect_WillScheduledOnAbnormalDisconnect(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, repository, _, _, scheduler, willPublisher := newUseCase(t, now)
	connectPacket := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "will-client",
		SessionExpiryInterval: 30,
		Will: &mqtt.Will{
			QoS:           1,
			Topic:         "status/will",
			Payload:       []byte("lost"),
			DelayInterval: 10,
		},
	}
	accepted, err := useCase.Connect(connectPacket)
	if err != nil || !accepted.Accepted {
		t.Fatalf("Connect() = %#v, %v", accepted, err)
	}

	// Abnormal disconnect schedules will for 10s and expiry for 30s
	if err := useCase.Disconnect("will-client", session.DisconnectAbnormal); err != nil {
		t.Fatal(err)
	}
	if len(scheduler.timers) != 2 {
		t.Fatalf("expected 2 scheduled timers, got %d", len(scheduler.timers))
	}
	willTimer := scheduler.timers[0]
	if willTimer.delay != 10*time.Second {
		t.Fatalf("expected will delay 10s, got %v", willTimer.delay)
	}
	expiryTimer := scheduler.timers[1]
	if expiryTimer.delay != 30*time.Second {
		t.Fatalf("expected expiry delay 30s, got %v", expiryTimer.delay)
	}
	if len(willPublisher.published) != 0 {
		t.Fatal("will must not be published before timer fires")
	}

	// Fire will timer
	willTimer.callback()
	if len(willPublisher.published) != 1 {
		t.Fatalf("expected 1 published will, got %d", len(willPublisher.published))
	}
	published := willPublisher.published[0]
	if published.sessionID != "will-client" || published.will.Topic != "status/will" || string(published.will.Payload) != "lost" {
		t.Fatalf("published will mismatch: %#v", published)
	}

	// Verify stored state has willPublished == true
	state, found, err := repository.Load("will-client")
	if err != nil || !found || !state.WillPublished() {
		t.Fatalf("stored state = (%#v, %v, %v), want willPublished=true", state, found, err)
	}

	// Firing will timer again does not publish again (MQTT-3.1.2-10)
	willTimer.callback()
	if len(willPublisher.published) != 1 {
		t.Fatalf("duplicate will timer fire published again: %d", len(willPublisher.published))
	}
}

// Traceability: PRD MQTT-01/TEST-04; MQTT-3.1.3-9.
func TestConnect_ReconnectCancelsWillTimer(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, _, _, _, scheduler, willPublisher := newUseCase(t, now)
	connectPacket := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "reconnect-client",
		SessionExpiryInterval: 60,
		Will: &mqtt.Will{
			Topic:         "status/will",
			Payload:       []byte("lost"),
			DelayInterval: 10,
		},
	}
	if res, err := useCase.Connect(connectPacket); err != nil || !res.Accepted {
		t.Fatalf("initial Connect failed: %#v, %v", res, err)
	}
	if err := useCase.Disconnect("reconnect-client", session.DisconnectAbnormal); err != nil {
		t.Fatal(err)
	}
	if len(scheduler.timers) != 2 {
		t.Fatalf("expected 2 timers, got %d", len(scheduler.timers))
	}
	willTimer := scheduler.timers[0]
	if willTimer.stopped {
		t.Fatal("will timer should not be stopped yet")
	}

	// Reconnect before timer fires
	reconnectPacket := connectPacket
	reconnectPacket.CleanStart = false
	res, err := useCase.Connect(reconnectPacket)
	if err != nil || !res.Accepted || !res.ConnAck.SessionPresent {
		t.Fatalf("reconnect failed: %#v, %v", res, err)
	}
	if !willTimer.stopped {
		t.Fatal("will timer must be stopped upon reconnect")
	}

	// If the callback somehow runs, it must not publish
	willTimer.callback()
	if len(willPublisher.published) != 0 {
		t.Fatalf("reconnected session published will: %#v", willPublisher.published)
	}
}

// Traceability: PRD MQTT-01/TEST-04; MQTT-3.1.2-10; MQTT-3.14.4-3.
func TestConnect_NormalDisconnectDiscardsWill(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, repository, _, _, scheduler, willPublisher := newUseCase(t, now)
	connectPacket := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "normal-client",
		SessionExpiryInterval: 30,
		Will: &mqtt.Will{
			Topic:         "status/will",
			Payload:       []byte("bye"),
			DelayInterval: 10,
		},
	}
	if res, err := useCase.Connect(connectPacket); err != nil || !res.Accepted {
		t.Fatalf("Connect failed: %#v, %v", res, err)
	}
	if err := useCase.Disconnect("normal-client", session.DisconnectNormal); err != nil {
		t.Fatal(err)
	}

	// Only expiry timer should be scheduled (1 timer), not will timer
	if len(scheduler.timers) != 1 {
		t.Fatalf("expected 1 timer (expiry only), got %d", len(scheduler.timers))
	}
	if scheduler.timers[0].delay != 30*time.Second {
		t.Fatalf("expected expiry timer with 30s delay, got %v", scheduler.timers[0].delay)
	}
	if len(willPublisher.published) != 0 {
		t.Fatal("normal disconnect must not publish will")
	}

	state, found, err := repository.Load("normal-client")
	if err != nil || !found || !state.WillDiscarded() {
		t.Fatalf("stored state = (%#v, %v, %v), want willDiscarded=true", state, found, err)
	}
}

// Traceability: PRD MQTT-01/TEST-04; MQTT-3.1.2-8; MQTT-3.1.3-9.
func TestConnect_SessionExpiryBeforeWillDelayPublishesWill(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, clock, repository, queue, routes, scheduler, willPublisher := newUseCase(t, now)
	connectPacket := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "expiry-first",
		SessionExpiryInterval: 10,
		Will: &mqtt.Will{
			Topic:         "status/expired-first",
			Payload:       []byte("expired"),
			DelayInterval: 60,
		},
	}
	if res, err := useCase.Connect(connectPacket); err != nil || !res.Accepted {
		t.Fatalf("Connect failed: %#v, %v", res, err)
	}
	if _, err := routes.Subscribe("expiry-first", mqtt.Subscription{Filter: "test/#"}); err != nil {
		t.Fatal(err)
	}

	if err := useCase.Disconnect("expiry-first", session.DisconnectAbnormal); err != nil {
		t.Fatal(err)
	}
	// Will delay was clamped to expiry duration (10s), expiry timer is 10s
	if len(scheduler.timers) != 2 {
		t.Fatalf("expected 2 timers, got %d", len(scheduler.timers))
	}
	if scheduler.timers[0].delay != 10*time.Second || scheduler.timers[1].delay != 10*time.Second {
		t.Fatalf("expected both delays to be 10s: %v, %v", scheduler.timers[0].delay, scheduler.timers[1].delay)
	}

	// Advance clock and fire expiry timer
	clock.now = clock.now.Add(10 * time.Second)
	expiryTimer := scheduler.timers[1]
	expiryTimer.callback()

	// Will must have been published on expiry!
	if len(willPublisher.published) != 1 {
		t.Fatalf("expected 1 published will on expiry, got %d", len(willPublisher.published))
	}
	if willPublisher.published[0].will.Topic != "status/expired-first" {
		t.Fatalf("published topic mismatch: %s", willPublisher.published[0].will.Topic)
	}

	// Session must be purged from repository, queue, routes
	if _, found, err := repository.Load("expiry-first"); err != nil || found {
		t.Fatalf("session still in repository after expiry: found=%v, err=%v", found, err)
	}
	if len(queue.removes) < 2 {
		t.Fatalf("queue removes: %v", queue.removes)
	}
	matches, err := routes.Match("pub", "test/topic", mqtt.QoSAtMostOnce, false)
	if err != nil || len(matches) != 0 {
		t.Fatalf("routes after expiry = %v", matches)
	}
}

func TestConnect_CloseCancelsAllTimers(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, _, _, _, scheduler, _ := newUseCase(t, now)

	for _, id := range []string{"client-1", "client-2"} {
		connectPacket := mqtt.Connect{
			Version:               mqtt.ProtocolV5,
			ClientID:              id,
			SessionExpiryInterval: 60,
			Will: &mqtt.Will{
				Topic:         "status/" + id,
				Payload:       []byte("lost"),
				DelayInterval: 10,
			},
		}
		if res, err := useCase.Connect(connectPacket); err != nil || !res.Accepted {
			t.Fatal(err)
		}
		if err := useCase.Disconnect(routing.SessionID(id), session.DisconnectAbnormal); err != nil {
			t.Fatal(err)
		}
	}

	if len(scheduler.timers) != 4 {
		t.Fatalf("expected 4 timers scheduled, got %d", len(scheduler.timers))
	}
	for _, timer := range scheduler.timers {
		if timer.stopped {
			t.Fatal("timer should not be stopped yet")
		}
	}

	if err := useCase.Close(); err != nil {
		t.Fatal(err)
	}

	for i, timer := range scheduler.timers {
		if !timer.stopped {
			t.Fatalf("timer %d must be stopped after Close()", i)
		}
	}
}

func TestConnect_TimerCallbackEdgeCases(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, repository, _, _, _, willPublisher := newUseCase(t, now)

	// onWillTimer on missing session
	useCase.onWillTimer("missing")
	if len(willPublisher.published) != 0 {
		t.Fatal("unexpected will publication on missing session")
	}

	// onExpiryTimer on missing session
	useCase.onExpiryTimer("missing")

	// Connect client with will
	connectPacket := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "edge-client",
		SessionExpiryInterval: 30,
		Will: &mqtt.Will{
			Topic:         "status/edge",
			Payload:       []byte("lost"),
			DelayInterval: 10,
		},
	}
	if res, err := useCase.Connect(connectPacket); err != nil || !res.Accepted {
		t.Fatal(err)
	}

	// onWillTimer while connected does nothing
	useCase.onWillTimer("edge-client")
	if len(willPublisher.published) != 0 {
		t.Fatal("unexpected will publication while connected")
	}

	// onExpiryTimer while connected does nothing
	useCase.onExpiryTimer("edge-client")

	// Disconnect client
	if err := useCase.Disconnect("edge-client", session.DisconnectAbnormal); err != nil {
		t.Fatal(err)
	}

	// onExpiryTimer before expiry does nothing
	useCase.onExpiryTimer("edge-client")

	// onWillTimer when save fails
	repository.saveErr = errors.New("save failed")
	useCase.onWillTimer("edge-client")
	if len(willPublisher.published) != 0 {
		t.Fatal("published will when save failed")
	}
	repository.saveErr = nil

	// Successful onWillTimer
	useCase.onWillTimer("edge-client")
	if len(willPublisher.published) != 1 {
		t.Fatalf("expected 1 published will, got %d", len(willPublisher.published))
	}

	// Second onWillTimer when will is already claimed
	useCase.onWillTimer("edge-client")
	if len(willPublisher.published) != 1 {
		t.Fatalf("duplicate will publication, got %d", len(willPublisher.published))
	}

	// Close on nil UseCase
	var nilUseCase *UseCase
	if err := nilUseCase.Close(); err != nil {
		t.Fatal(err)
	}

	// Disconnect with immediate will and publisher error
	connectImmediate := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "immediate-err",
		SessionExpiryInterval: 0,
		Will: &mqtt.Will{
			Topic:   "status/immediate",
			Payload: []byte("lost"),
		},
	}
	if res, err := useCase.Connect(connectImmediate); err != nil || !res.Accepted {
		t.Fatal(err)
	}
	willPublisher.err = errors.New("will publisher error")
	if err := useCase.Disconnect("immediate-err", session.DisconnectAbnormal); !errors.Is(err, willPublisher.err) {
		t.Fatalf("expected publisher error, got %v", err)
	}
}

type fakeAuthenticator struct {
	err    error
	result applicationauthentication.Result
}

func (a *fakeAuthenticator) Authenticate(ctx context.Context, credential applicationauthentication.Credential) (applicationauthentication.Result, error) {
	if a.err != nil {
		return applicationauthentication.Result{}, a.err
	}
	return a.result, nil
}

// Requirements: SEC-04, TEN-01, TEN-02
func TestConnectAuthenticatedPipeline(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	filter, err := tenancy.NewTopicFilter(tenant, "sensors/#")
	if err != nil {
		t.Fatal(err)
	}
	rule, err := security.NewRule(tenant, "publisher", security.EffectAllow, security.ActionWill, filter)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := security.NewPolicy([]security.Rule{rule})
	if err != nil {
		t.Fatal(err)
	}

	auth := &fakeAuthenticator{
		result: applicationauthentication.Result{
			TenantID:         tenant,
			Subject:          "device-1",
			CredentialID:     "cred-1",
			AllowedClientIDs: []string{"client-1"},
			Roles:            []security.Role{"publisher"},
			ExpiresAt:        now.Add(time.Hour),
		},
	}

	makeUseCase := func() (*UseCase, *fakeSessionRepository, *fakeDeliveryQueue, *fakeWillPublisher) {
		clock := &fakeClock{now: now}
		repository := newFakeSessionRepository()
		queue := newFakeDeliveryQueue()
		routes := routing.NewTable()
		scheduler := &fakeScheduler{}
		publisher := &fakeWillPublisher{}
		uc, err := New(clock, repository, queue, routes, scheduler, publisher, WithSecurity(auth, policy))
		if err != nil {
			t.Fatal(err)
		}
		return uc, repository, queue, publisher
	}

	validToken := []byte("valid.jwt.token")
	cases := []struct {
		name       string
		packet     mqtt.Connect
		authErr    error
		wantReason byte
	}{
		{
			name:       "missing-username-v311",
			packet:     mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "client-1", HasPassword: true, Password: validToken},
			wantReason: 0x05,
		},
		{
			name:       "missing-username-v5",
			packet:     mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client-1", HasPassword: true, Password: validToken},
			wantReason: 0x87,
		},
		{
			name:       "missing-password-v311",
			packet:     mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "client-1", HasUsername: true, Username: "u"},
			wantReason: 0x05,
		},
		{
			name:       "missing-password-v5",
			packet:     mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client-1", HasUsername: true, Username: "u"},
			wantReason: 0x87,
		},
		{
			name:       "empty-password-v5",
			packet:     mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client-1", HasUsername: true, HasPassword: true, Password: []byte("")},
			wantReason: 0x87,
		},
		{
			name:       "auth-failed-v5",
			packet:     mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client-1", HasUsername: true, HasPassword: true, Password: []byte("bad")},
			authErr:    errors.New("invalid jwt signature"),
			wantReason: 0x87,
		},
		{
			name:       "client-id-mismatch-v5",
			packet:     mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "unauthorized-client", HasUsername: true, HasPassword: true, Password: validToken},
			wantReason: 0x87,
		},
		{
			name: "will-acl-denied-v5",
			packet: mqtt.Connect{
				Version:     mqtt.ProtocolV5,
				ClientID:    "client-1",
				HasUsername: true,
				HasPassword: true,
				Password:    validToken,
				Will: &mqtt.Will{
					Topic:   "unauthorized/topic",
					Payload: []byte("bye"),
				},
			},
			wantReason: 0x87,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			useCase, repository, queue, _ := makeUseCase()
			auth.err = tc.authErr
			defer func() { auth.err = nil }()

			res, err := useCase.Connect(tc.packet)
			if err != nil {
				t.Fatal(err)
			}
			if res.Accepted || res.ConnAck.ReasonCode != tc.wantReason {
				t.Fatalf("rejected connect = (%#v), want reason %#x", res, tc.wantReason)
			}
			if repository.loadCalls != 0 || repository.saveCalls != 0 || len(queue.registers) != 0 {
				t.Fatalf("rejected connect modified state: loads=%d saves=%d registers=%v", repository.loadCalls, repository.saveCalls, queue.registers)
			}
		})
	}

	t.Run("successful-auth-creates-session-and-will-authorizes", func(t *testing.T) {
		useCase, repository, queue, publisher := makeUseCase()
		connect := mqtt.Connect{
			Version:     mqtt.ProtocolV5,
			ClientID:    "client-1",
			HasUsername: true,
			HasPassword: true,
			Password:    validToken,
			Will: &mqtt.Will{
				Topic:   "sensors/status",
				Payload: []byte("lost"),
			},
		}
		res, err := useCase.Connect(connect)
		if err != nil || !res.Accepted || res.ConnAck.ReasonCode != 0 {
			t.Fatalf("Connect failed: %#v, %v", res, err)
		}
		if res.Principal.TenantID() != tenant || res.Principal.ClientID() != "client-1" {
			t.Fatalf("Principal mismatch: %#v", res.Principal)
		}
		if repository.saveCalls != 1 || len(queue.registers) != 1 {
			t.Fatalf("state not saved: saves=%d registers=%v", repository.saveCalls, queue.registers)
		}

		// Disconnect abnormal publishes will
		if err := useCase.Disconnect(res.SessionID, session.DisconnectAbnormal); err != nil {
			t.Fatal(err)
		}
		if len(publisher.published) != 1 || publisher.published[0].will.Topic != "sensors/status" {
			t.Fatalf("will not published: %#v", publisher.published)
		}
	})
}
