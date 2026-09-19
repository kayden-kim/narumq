package connect

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
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

func (*fakeDeliveryQueue) TryEnqueue(deliveryqueue.DeliveryDTO) error {
	return nil
}

func (*fakeDeliveryQueue) Drain(routing.SessionID) ([]deliveryqueue.DeliveryDTO, error) {
	return nil, nil
}

func newUseCase(t *testing.T, now time.Time) (*UseCase, *fakeClock, *fakeSessionRepository, *fakeDeliveryQueue, *routing.Table) {
	t.Helper()
	clock := &fakeClock{now: now}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	routes := routing.NewTable()
	useCase, err := New(clock, repository, queue, routes)
	if err != nil {
		t.Fatal(err)
	}
	return useCase, clock, repository, queue, routes
}

func TestNewRejectsNilDependencies(t *testing.T) {
	clock := &fakeClock{}
	repository := newFakeSessionRepository()
	queue := newFakeDeliveryQueue()
	routes := routing.NewTable()
	cases := []struct {
		name string
		want error
		make func() (Clock, SessionRepository, deliveryqueue.Queue, *routing.Table)
	}{
		{name: "clock", want: ErrNilClock, make: func() (Clock, SessionRepository, deliveryqueue.Queue, *routing.Table) {
			return nil, repository, queue, routes
		}},
		{name: "repository", want: ErrNilSessionRepository, make: func() (Clock, SessionRepository, deliveryqueue.Queue, *routing.Table) {
			return clock, nil, queue, routes
		}},
		{name: "queue", want: ErrNilDeliveryQueue, make: func() (Clock, SessionRepository, deliveryqueue.Queue, *routing.Table) {
			return clock, repository, nil, routes
		}},
		{name: "routes", want: ErrNilRoutingTable, make: func() (Clock, SessionRepository, deliveryqueue.Queue, *routing.Table) {
			return clock, repository, queue, nil
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			clock, repository, queue, routes := testCase.make()
			if _, err := New(clock, repository, queue, routes); !errors.Is(err, testCase.want) {
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
		{name: "will-v5", packet: mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", Will: &mqtt.Will{Topic: "last/will"}}, wantReason: 0x83},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			useCase, _, repository, queue, _ := newUseCase(t, now)
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
	useCase, clock, _, queue, routes := newUseCase(t, now)
	connect := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", SessionExpiryInterval: 10}
	first, err := useCase.Connect(connect)
	if err != nil || !first.Accepted || first.ConnAck.SessionPresent {
		t.Fatalf("initial Connect() = %#v, %v", first, err)
	}
	if err := routes.Subscribe(first.SessionID, "sensors/#", mqtt.QoSAtMostOnce); err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect(first.SessionID); err != nil {
		t.Fatal(err)
	}
	resumed, err := useCase.Connect(connect)
	if err != nil || !resumed.Accepted || !resumed.ConnAck.SessionPresent {
		t.Fatalf("resumed Connect() = %#v, %v", resumed, err)
	}
	matches, err := routes.Match("sensors/room", mqtt.QoSAtMostOnce)
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

	if err := useCase.Disconnect(resumed.SessionID); err != nil {
		t.Fatal(err)
	}
	fresh, err = useCase.Connect(cleanStart)
	if err != nil || !fresh.Accepted || fresh.ConnAck.SessionPresent {
		t.Fatalf("clean-start replacement = %#v, %v", fresh, err)
	}
	matches, err = routes.Match("sensors/room", mqtt.QoSAtMostOnce)
	if err != nil || len(matches) != 0 {
		t.Fatalf("routes after clean-start replacement = %#v, %v", matches, err)
	}

	if err := useCase.Disconnect(fresh.SessionID); err != nil {
		t.Fatal(err)
	}
	finite := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "finite", SessionExpiryInterval: 2}
	if result, err := useCase.Connect(finite); err != nil || !result.Accepted {
		t.Fatalf("finite initial Connect() = %#v, %v", result, err)
	}
	if err := routes.Subscribe("finite", "alerts/#", mqtt.QoSAtMostOnce); err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect("finite"); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(3 * time.Second)
	expired, err := useCase.Connect(finite)
	if err != nil || !expired.Accepted || expired.ConnAck.SessionPresent {
		t.Fatalf("expired Connect() = %#v, %v", expired, err)
	}
	matches, err = routes.Match("alerts/critical", mqtt.QoSAtMostOnce)
	if err != nil || len(matches) != 0 {
		t.Fatalf("routes after expiry reset = %#v, %v", matches, err)
	}
}

// Traceability: PRD MQTT-03/TEST-03; duplicate ownership is rejected without takeover.
func TestConnectRejectsDuplicateAndRepositoryErrors(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	useCase, _, repository, queue, _ := newUseCase(t, now)
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
	useCase, err := New(clock, repository, queue, routing.NewTable())
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
	useCase, err := New(clock, repository, queue, routing.NewTable())
	if err != nil {
		t.Fatal(err)
	}
	packet := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "persistent", SessionExpiryInterval: 60}
	initial, err := useCase.Connect(packet)
	if err != nil || !initial.Accepted {
		t.Fatalf("initial Connect() = %#v, %v", initial, err)
	}
	if err := useCase.Disconnect(initial.SessionID); err != nil {
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
	useCase, _, repository, queue, routes := newUseCase(t, now)
	persistent := mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "persistent"}
	if result, err := useCase.Connect(persistent); err != nil || !result.Accepted {
		t.Fatalf("persistent Connect() = %#v, %v", result, err)
	}
	if err := routes.Subscribe("persistent", "events/#", mqtt.QoSAtMostOnce); err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect("persistent"); err != nil {
		t.Fatal(err)
	}
	state, found, err := repository.Load("persistent")
	if err != nil || !found || state.Connected() {
		t.Fatalf("persistent stored state = (%#v, %v, %v)", state, found, err)
	}
	matches, err := routes.Match("events/created", mqtt.QoSAtMostOnce)
	if err != nil || len(matches) != 1 {
		t.Fatalf("persistent routes after disconnect = %#v, %v", matches, err)
	}

	immediate := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "immediate", SessionExpiryInterval: 0}
	if result, err := useCase.Connect(immediate); err != nil || !result.Accepted {
		t.Fatalf("immediate Connect() = %#v, %v", result, err)
	}
	if err := routes.Subscribe("immediate", "events/#", mqtt.QoSAtMostOnce); err != nil {
		t.Fatal(err)
	}
	if err := useCase.Disconnect("immediate"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := repository.Load("immediate"); err != nil || found {
		t.Fatalf("immediate repository entry = found %v, err %v", found, err)
	}
	matches, err = routes.Match("events/created", mqtt.QoSAtMostOnce)
	if err != nil || len(matches) != 1 || matches[0].SessionID != "persistent" {
		t.Fatalf("routes after immediate disconnect = %#v, %v", matches, err)
	}
	if len(queue.removes) != 4 {
		t.Fatalf("queue removes = %v, want four lifecycle removals", queue.removes)
	}
}
