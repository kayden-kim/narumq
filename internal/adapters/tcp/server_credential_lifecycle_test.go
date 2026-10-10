package tcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"narumq/internal/application/connect"
	portsaudit "narumq/internal/application/ports/audit"
	applicationauthentication "narumq/internal/application/ports/authentication"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

type tcpCredentialRevocationChecker struct {
	mu                    sync.Mutex
	revoked               bool
	err                   error
	checked               []string
	ownershipCommitted    func() bool
	checkedAfterOwnership bool
}

func (checker *tcpCredentialRevocationChecker) IsCredentialRevoked(_ context.Context, credentialID string) (bool, error) {
	checker.mu.Lock()
	defer checker.mu.Unlock()
	checker.checked = append(checker.checked, credentialID)
	if checker.ownershipCommitted != nil && !checker.ownershipCommitted() {
		checker.checkedAfterOwnership = false
		return false, nil
	}
	if checker.ownershipCommitted != nil {
		checker.checkedAfterOwnership = true
	}
	return checker.revoked, checker.err
}

type tcpCredentialTimer struct {
	delay    time.Duration
	callback func()
	mu       sync.Mutex
	stopped  bool
}

func (timer *tcpCredentialTimer) Stop() bool {
	timer.mu.Lock()
	defer timer.mu.Unlock()
	if timer.stopped {
		return false
	}
	timer.stopped = true
	return true
}

func (timer *tcpCredentialTimer) fire() bool {
	timer.mu.Lock()
	if timer.stopped {
		timer.mu.Unlock()
		return false
	}
	timer.stopped = true
	timer.mu.Unlock()
	timer.callback()
	return true
}

type tcpCredentialScheduler struct {
	mu     sync.Mutex
	timers []*tcpCredentialTimer
}

func (scheduler *tcpCredentialScheduler) AfterFunc(delay time.Duration, callback func()) connect.Timer {
	timer := &tcpCredentialTimer{delay: delay, callback: callback}
	scheduler.mu.Lock()
	scheduler.timers = append(scheduler.timers, timer)
	scheduler.mu.Unlock()
	return timer
}

func (scheduler *tcpCredentialScheduler) waitForDelay(t *testing.T, delay time.Duration, occurrence int) *tcpCredentialTimer {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		scheduler.mu.Lock()
		matches := make([]*tcpCredentialTimer, 0, len(scheduler.timers))
		for _, timer := range scheduler.timers {
			if timer.delay == delay {
				matches = append(matches, timer)
			}
		}
		scheduler.mu.Unlock()
		if len(matches) > occurrence {
			return matches[occurrence]
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timer with delay %s occurrence %d was not scheduled", delay, occurrence)
	return nil
}

type tcpCredentialAuditRecorder struct {
	mu      sync.Mutex
	denials []portsaudit.DataPlaneDenialDTO
}

func (recorder *tcpCredentialAuditRecorder) RecordDataPlaneDenial(denial portsaudit.DataPlaneDenialDTO) {
	recorder.mu.Lock()
	recorder.denials = append(recorder.denials, denial)
	recorder.mu.Unlock()
}

func (recorder *tcpCredentialAuditRecorder) snapshot() []portsaudit.DataPlaneDenialDTO {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]portsaudit.DataPlaneDenialDTO(nil), recorder.denials...)
}

func newTCPCredentialRuntime(
	t *testing.T,
	checker *tcpCredentialRevocationChecker,
	scheduler *tcpCredentialScheduler,
	expiresAfter time.Duration,
	allowWill bool,
) (*adapterTestRuntime, tenancy.TenantID) {
	t.Helper()
	config := defaultAdapterConfig()
	config.ClusterMode = true
	config.LocalMemberID = 7
	config.ClusterReady = func() bool { return true }
	config.ClusterAdmissionOpen = func() bool { return true }
	config.ClusterProjectionReady = func() bool { return true }
	config.CredentialRevocationChecker = checker
	config.TimerScheduler = scheduler
	runtime := newAdapterTestRuntime(t, 16, config)
	tenant, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	policy := security.Policy{}
	if allowWill {
		filter, err := tenancy.NewTopicFilter(tenant, "expiry/#")
		if err != nil {
			t.Fatal(err)
		}
		willRule, err := security.NewRule(tenant, "device", security.EffectAllow, security.ActionWill, filter)
		if err != nil {
			t.Fatal(err)
		}
		subscribeRule, err := security.NewRule(tenant, "device", security.EffectAllow, security.ActionSubscribe, filter)
		if err != nil {
			t.Fatal(err)
		}
		policy, err = security.NewPolicy([]security.Rule{willRule, subscribeRule})
		if err != nil {
			t.Fatal(err)
		}
		subscribe.WithPolicy(policy)(runtime.server.subscriptions)
		publish.WithPolicy(policy)(runtime.server.publications)
	}
	authenticator := &testServerAuthenticator{result: applicationauthentication.Result{
		TenantID: tenant, Subject: "device-1", CredentialID: "credential-a",
		AllowedClientIDs: []string{"watcher", "victim", "later"}, Roles: []security.Role{"device"},
		ExpiresAt: runtime.server.config.Clock.Now().Add(expiresAfter),
	}}
	connect.WithSecurity(authenticator, nil, policy, "jwt")(runtime.server.connections)
	connect.WithCredentialRevocationChecker(checker)(runtime.server.connections)
	return runtime, tenant
}

func tcpCredentialConnect(t *testing.T, runtime *adapterTestRuntime, client net.Conn, clientID string, version mqtt.ProtocolVersion, will *mqtt.Will) *mqtt.ConnAck {
	t.Helper()
	packet := &mqtt.Connect{
		Version: version, CleanStart: true, ClientID: clientID,
		HasUsername: true, Username: "device", HasPassword: true, Password: []byte("valid-token"), Will: will,
	}
	if version == mqtt.ProtocolV5 {
		packet.SessionExpiryInterval = 60
	}
	writeTestPackets(t, client, packet)
	response, _, err := runtime.server.limits.Load().decoder.Decode(readTestFrame(t, client), version)
	if err != nil {
		t.Fatalf("decode %v CONNECT response: %v", version, err)
	}
	connAck, ok := response.(*mqtt.ConnAck)
	if !ok {
		t.Fatalf("CONNECT response = %T, want CONNACK", response)
	}
	return connAck
}

func tcpCredentialDial(t *testing.T, runtime *adapterTestRuntime) net.Conn {
	t.Helper()
	client, serverSide := net.Pipe()
	runtime.listener.connections <- serverSide
	select {
	case <-runtime.listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("server did not accept credential test connection")
	}
	return client
}

func tcpCredentialAssertClosed(t *testing.T, client net.Conn) {
	t.Helper()
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
			return
		}
		t.Fatal(err)
	}
	var buffer [1]byte
	_, err := client.Read(buffer[:])
	if err == nil {
		t.Fatal("connection remained open")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("connection remained open until read timeout: %v", err)
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7.
func TestTCPConnectRejectsRevokedCredentialAfterOwnershipCommit(t *testing.T) {
	for _, test := range []struct {
		name       string
		version    mqtt.ProtocolVersion
		revoked    bool
		checkerErr error
		wantReason byte
	}{
		{name: "MQTT 3.1.1 revoked", version: mqtt.ProtocolV311, revoked: true, wantReason: 0x05},
		{name: "MQTT 5 revoked", version: mqtt.ProtocolV5, revoked: true, wantReason: 0x87},
		{name: "MQTT 3.1.1 quorum unavailable", version: mqtt.ProtocolV311, checkerErr: context.DeadlineExceeded, wantReason: 0x03},
		{name: "MQTT 5 quorum unavailable", version: mqtt.ProtocolV5, checkerErr: context.DeadlineExceeded, wantReason: 0x88},
	} {
		t.Run(test.name, func(t *testing.T) {
			checker := &tcpCredentialRevocationChecker{revoked: test.revoked, err: test.checkerErr}
			scheduler := &tcpCredentialScheduler{}
			runtime, _ := newTCPCredentialRuntime(t, checker, scheduler, time.Hour, false)
			defer runtime.stop(t)
			checker.ownershipCommitted = func() bool {
				runtime.cluster.mu.Lock()
				defer runtime.cluster.mu.Unlock()
				return runtime.cluster.ownershipCalls > 0
			}
			auditRecorder := &tcpCredentialAuditRecorder{}
			connect.WithDataPlaneAudit(auditRecorder)(runtime.server.connections)

			connAck := tcpCredentialConnect(t, runtime, runtime.client, "victim", test.version, nil)
			if connAck.ReasonCode != test.wantReason {
				t.Fatalf("CONNACK reason = 0x%02x, want 0x%02x", connAck.ReasonCode, test.wantReason)
			}
			checker.mu.Lock()
			checkedAfterOwnership := checker.checkedAfterOwnership
			checked := append([]string(nil), checker.checked...)
			checker.mu.Unlock()
			if !checkedAfterOwnership || len(checked) != 2 || checked[0] != "credential-a" || checked[1] != "credential-a" {
				t.Fatalf("revocation checks = %v with final check after ownership=%t", checked, checkedAfterOwnership)
			}
			denials := auditRecorder.snapshot()
			if test.revoked {
				if len(denials) != 1 || denials[0].Action != portsaudit.DataPlaneActionConnect ||
					denials[0].Reason != "credential_revoked" || denials[0].CredentialID != "credential-a" {
					t.Fatalf("revoked CONNECT audit = %+v", denials)
				}
			} else if len(denials) != 0 {
				t.Fatalf("unavailable revocation check recorded false denial: %+v", denials)
			}
		})
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7.
func TestTCPCommittedRevocationClosesSessionsAndBlocksLocalReconnection(t *testing.T) {
	for _, test := range []struct {
		name       string
		version    mqtt.ProtocolVersion
		wantReason byte
	}{
		{name: "MQTT 3.1.1", version: mqtt.ProtocolV311, wantReason: 0x05},
		{name: "MQTT 5", version: mqtt.ProtocolV5, wantReason: 0x87},
	} {
		t.Run(test.name, func(t *testing.T) {
			checker := &tcpCredentialRevocationChecker{}
			scheduler := &tcpCredentialScheduler{}
			runtime, _ := newTCPCredentialRuntime(t, checker, scheduler, time.Hour, false)
			defer runtime.stop(t)
			auditRecorder := &tcpCredentialAuditRecorder{}
			connect.WithDataPlaneAudit(auditRecorder)(runtime.server.connections)

			if connAck := tcpCredentialConnect(t, runtime, runtime.client, "victim", test.version, nil); connAck.ReasonCode != 0 {
				t.Fatalf("initial CONNACK reason = 0x%02x, want success", connAck.ReasonCode)
			}
			if !waitForActiveConnections(runtime.server, 1, time.Second) || len(runtime.server.ConnectedSessionIDs()) != 1 {
				t.Fatal("accepted credential session was not bound")
			}
			runtime.server.ApplyCredentialRevocation("credential-a", true)
			if test.version == mqtt.ProtocolV5 {
				packet, _, err := runtime.server.limits.Load().decoder.Decode(readTestFrame(t, runtime.client), mqtt.ProtocolV5)
				disconnect, ok := packet.(*mqtt.Disconnect)
				if err != nil || !ok || disconnect.ReasonCode != 0x87 {
					t.Fatalf("revocation DISCONNECT = (%#v, %v), want reason 0x87", packet, err)
				}
				if !scheduler.waitForDelay(t, drainDisconnectFlushTimeout, 0).fire() {
					t.Fatal("MQTT 5 forced-close timer did not fire")
				}
			}
			tcpCredentialAssertClosed(t, runtime.client)
			if !waitForActiveConnections(runtime.server, 0, time.Second) {
				t.Fatal("revoked connection remained active")
			}

			later := tcpCredentialDial(t, runtime)
			defer later.Close()
			if connAck := tcpCredentialConnect(t, runtime, later, "later", test.version, nil); connAck.ReasonCode != test.wantReason {
				t.Fatalf("reconnection CONNACK reason = 0x%02x, want 0x%02x", connAck.ReasonCode, test.wantReason)
			}
			denials := auditRecorder.snapshot()
			if len(denials) != 1 || denials[0].Action != portsaudit.DataPlaneActionConnect ||
				denials[0].Reason != "credential_revoked" || denials[0].CredentialID != "credential-a" {
				t.Fatalf("local revocation CONNECT audit = %+v", denials)
			}
		})
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7.
func TestTCPCredentialExpiryDisconnectsAndProcessesWill(t *testing.T) {
	for _, test := range []struct {
		name    string
		version mqtt.ProtocolVersion
	}{
		{name: "MQTT 3.1.1", version: mqtt.ProtocolV311},
		{name: "MQTT 5", version: mqtt.ProtocolV5},
	} {
		t.Run(test.name, func(t *testing.T) {
			checker := &tcpCredentialRevocationChecker{}
			scheduler := &tcpCredentialScheduler{}
			runtime, _ := newTCPCredentialRuntime(t, checker, scheduler, time.Second, true)
			defer runtime.stop(t)

			watcher := runtime.client
			if connAck := tcpCredentialConnect(t, runtime, watcher, "watcher", mqtt.ProtocolV5, nil); connAck.ReasonCode != 0 {
				t.Fatalf("watcher CONNACK reason = 0x%02x, want success", connAck.ReasonCode)
			}
			writeTestPackets(t, watcher, &mqtt.Subscribe{
				Version: mqtt.ProtocolV5, PacketID: 1,
				Subscriptions: []mqtt.Subscription{{Filter: "expiry/#", QoS: mqtt.QoSAtMostOnce}},
			})
			if packet, _, err := runtime.server.limits.Load().decoder.Decode(readTestFrame(t, watcher), mqtt.ProtocolV5); err != nil {
				t.Fatalf("decode watcher SUBACK: %v", err)
			} else if _, ok := packet.(*mqtt.SubAck); !ok {
				t.Fatalf("watcher subscription response = %T, want SUBACK", packet)
			}

			victim := tcpCredentialDial(t, runtime)
			defer victim.Close()
			will := &mqtt.Will{Topic: "expiry/will", Payload: []byte("expired")}
			if connAck := tcpCredentialConnect(t, runtime, victim, "victim", test.version, will); connAck.ReasonCode != 0 {
				t.Fatalf("victim CONNACK reason = 0x%02x, want success", connAck.ReasonCode)
			}
			if !scheduler.waitForDelay(t, time.Second, 1).fire() {
				t.Fatal("victim credential-expiry timer did not fire")
			}
			if test.version == mqtt.ProtocolV5 {
				packet, _, err := runtime.server.limits.Load().decoder.Decode(readTestFrame(t, victim), mqtt.ProtocolV5)
				disconnect, ok := packet.(*mqtt.Disconnect)
				if err != nil || !ok || disconnect.ReasonCode != 0xA0 {
					t.Fatalf("expiry DISCONNECT = (%#v, %v), want reason 0xA0", packet, err)
				}
				if !scheduler.waitForDelay(t, drainDisconnectFlushTimeout, 0).fire() {
					t.Fatal("expiry forced-close timer did not fire")
				}
			}
			tcpCredentialAssertClosed(t, victim)

			packet, _, err := runtime.server.limits.Load().decoder.Decode(readTestFrame(t, watcher), mqtt.ProtocolV5)
			willPublish, ok := packet.(*mqtt.Publish)
			if err != nil || !ok || willPublish.Topic != will.Topic || string(willPublish.Payload) != string(will.Payload) {
				t.Fatalf("expiry Will delivery = (%#v, %v), want topic %q and payload %q", packet, err, will.Topic, will.Payload)
			}
		})
	}
}
