package session

import (
	"errors"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/tenancy"
	"reflect"
	"testing"
	"time"
)

// Traceability: PRD MQTT-03/TEST-03/04; MQTT-3.1.2-2/3.
func TestOpenAndDisconnectMQTT311CleanStartLifecycle(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	clean, present, err := Open(nil, mqtt.Connect{Version: mqtt.ProtocolV311, CleanStart: true, ClientID: "client"}, now)
	if err != nil || present || !clean.Connected() || clean.ClientID() != "client" || clean.ExpiryInterval() != 0 {
		t.Fatalf("clean Open() = (%#v, %v, %v)", clean, present, err)
	}
	result, err := clean.Disconnect(now, DisconnectNormal)
	closed := result.Lifecycle
	if err != nil || !result.SessionRemoved || !closed.Expired(now) || closed.Connected() {
		t.Fatalf("clean Disconnect() = (%#v, %v, %v)", closed, result.SessionRemoved, err)
	}
	if _, err := closed.Disconnect(now, DisconnectNormal); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("second Disconnect() error = %v, want ErrNotConnected", err)
	}

	persistent, present, err := Open(nil, mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "persistent"}, now)
	if err != nil || present {
		t.Fatalf("persistent initial Open() = (%#v, %v, %v)", persistent, present, err)
	}
	res, err := persistent.Disconnect(now, DisconnectNormal)
	persistent = res.Lifecycle
	if err != nil || res.SessionRemoved || persistent.Expired(now) {
		t.Fatalf("persistent Disconnect() = (%#v, %v, %v)", persistent, res.SessionRemoved, err)
	}
	resumed, present, err := Open(&persistent, mqtt.Connect{Version: mqtt.ProtocolV311, ClientID: "persistent"}, now.Add(24*time.Hour))
	if err != nil || !present || !resumed.Connected() || resumed.ClientID() != "persistent" {
		t.Fatalf("persistent resume = (%#v, %v, %v)", resumed, present, err)
	}
	if _, finite := persistent.ExpiresAt(); finite {
		t.Fatal("MQTT 3.1.1 persistent session unexpectedly has finite deadline")
	}
}

// Traceability: PRD MQTT-03/TEST-03/04/07; MQTT-3.1.2-2/3.
func TestMQTT5SessionExpiryBoundariesAndReconnect(t *testing.T) {
	start := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	finiteConnect := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "finite", SessionExpiryInterval: 7}
	state, present, err := Open(nil, finiteConnect, start)
	if err != nil || present || state.ExpiryInterval() != 7 || !state.Connected() {
		t.Fatalf("finite Open() = (%#v, %v, %v)", state, present, err)
	}
	res, err := state.Disconnect(start, DisconnectNormal)
	closed := res.Lifecycle
	if err != nil || res.SessionRemoved {
		t.Fatalf("finite Disconnect() = (%#v, %v, %v)", closed, res.SessionRemoved, err)
	}
	deadline, ok := closed.ExpiresAt()
	if !ok || !deadline.Equal(start.Add(7*time.Second)) {
		t.Fatalf("deadline = (%v, %v), want %v", deadline, ok, start.Add(7*time.Second))
	}
	for _, testCase := range []struct {
		name    string
		at      time.Time
		expired bool
	}{
		{name: "before-deadline", at: deadline.Add(-time.Nanosecond), expired: false},
		{name: "at-deadline", at: deadline, expired: true},
		{name: "after-deadline", at: deadline.Add(time.Nanosecond), expired: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := closed.Expired(testCase.at); got != testCase.expired {
				t.Fatalf("Expired(%v) = %v, want %v", testCase.at, got, testCase.expired)
			}
		})
	}
	resumed, present, err := Open(&closed, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "finite", SessionExpiryInterval: 9}, deadline.Add(-time.Nanosecond))
	if err != nil || !present || resumed.ExpiryInterval() != 9 {
		t.Fatalf("finite resume before deadline = (%#v, %v, %v)", resumed, present, err)
	}
	expired, present, err := Open(&closed, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "finite", SessionExpiryInterval: 9}, deadline)
	if err != nil || present || !expired.Connected() {
		t.Fatalf("finite reopen at deadline = (%#v, %v, %v)", expired, present, err)
	}

	immediate, present, err := Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "immediate"}, start)
	if err != nil || present {
		t.Fatalf("immediate Open() = (%#v, %v, %v)", immediate, present, err)
	}
	res, err = immediate.Disconnect(start, DisconnectNormal)
	immediate = res.Lifecycle
	if err != nil || !res.SessionRemoved || !immediate.Expired(start) {
		t.Fatalf("immediate Disconnect() = (%#v, %v, %v)", immediate, res.SessionRemoved, err)
	}
	if _, finite := immediate.ExpiresAt(); finite {
		t.Fatal("immediate expiry unexpectedly exposed a finite deadline")
	}

	never, present, err := Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "never", SessionExpiryInterval: ^uint32(0)}, start)
	if err != nil || present {
		t.Fatalf("never-expire Open() = (%#v, %v, %v)", never, present, err)
	}
	res, err = never.Disconnect(start, DisconnectNormal)
	never = res.Lifecycle
	if err != nil || res.SessionRemoved || never.Expired(start.Add(1000*time.Hour)) {
		t.Fatalf("never-expire Disconnect() = (%#v, %v, %v)", never, res.SessionRemoved, err)
	}
	if _, finite := never.ExpiresAt(); finite {
		t.Fatal("never-expire session unexpectedly has finite deadline")
	}
}

// Traceability: PRD MQTT-03/TEST-03/04; MQTT-3.1.2-2 and MQTT-3.1.2-3.
func TestMQTT5CleanStartFiniteExpiryPersistsAfterDisconnect(t *testing.T) {
	now := time.Date(2026, time.January, 4, 0, 0, 0, 0, time.UTC)
	connect := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		CleanStart:            true,
		ClientID:              "clean-finite",
		SessionExpiryInterval: 7,
	}
	state, present, err := Open(nil, connect, now)
	if err != nil || present || !state.Connected() {
		t.Fatalf("Clean Start finite Open() = (%#v, %v, %v)", state, present, err)
	}
	res, err := state.Disconnect(now, DisconnectNormal)
	closed := res.Lifecycle
	if err != nil || res.SessionRemoved {
		t.Fatalf("Clean Start finite Disconnect() = (%#v, %v, %v)", closed, res.SessionRemoved, err)
	}
	deadline, ok := closed.ExpiresAt()
	if !ok || !deadline.Equal(now.Add(7*time.Second)) {
		t.Fatalf("Clean Start finite deadline = (%v, %v), want %v", deadline, ok, now.Add(7*time.Second))
	}
	if closed.Expired(deadline.Add(-time.Nanosecond)) || !closed.Expired(deadline) {
		t.Fatalf("Clean Start finite expiry boundary is incorrect")
	}
	resumed, present, err := Open(&closed, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "clean-finite"}, deadline.Add(-time.Nanosecond))
	if err != nil || !present || !resumed.Connected() {
		t.Fatalf("Clean Start finite resume = (%#v, %v, %v)", resumed, present, err)
	}
}

// Traceability: PRD MQTT-03/TEST-03/04; ownership transition boundary.
func TestOpenCleanStartAndIllegalLifecycleTransitions(t *testing.T) {
	now := time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC)
	old, _, err := Open(nil, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", SessionExpiryInterval: 100}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(&old, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "other"}, now); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("identity mismatch error = %v, want ErrIdentityMismatch", err)
	}
	if _, _, err := Open(&old, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", CleanStart: true}, now); !errors.Is(err, ErrAlreadyConnected) {
		t.Fatalf("connected Open() error = %v, want ErrAlreadyConnected", err)
	}
	res, err := old.Disconnect(now, DisconnectNormal)
	old = res.Lifecycle
	if err != nil {
		t.Fatal(err)
	}
	fresh, present, err := Open(&old, mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", CleanStart: true}, now.Add(time.Second))
	if err != nil || present || !fresh.Connected() {
		t.Fatalf("Clean Start replacement = (%#v, %v, %v)", fresh, present, err)
	}
	res, err = fresh.Disconnect(now.Add(time.Second), DisconnectNormal)
	fresh = res.Lifecycle
	if err != nil || !res.SessionRemoved || !fresh.Expired(now.Add(time.Second)) {
		t.Fatalf("Clean Start disconnect = (%#v, %v, %v)", fresh, res.SessionRemoved, err)
	}
	if _, err := fresh.Disconnect(now, DisconnectNormal); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("disconnected Disconnect() error = %v, want ErrNotConnected", err)
	}
}

// Traceability: PRD MQTT-01/TEST-04; MQTT-3.1.2-10; MQTT-3.14.4-3.
func TestLifecycle_WillTransitions_NormalDisconnect_SuppressesWill(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	connect := mqtt.Connect{
		Version:  mqtt.ProtocolV311,
		ClientID: "will-client",
		Will: &mqtt.Will{
			Topic:   "status/will",
			Payload: []byte("goodbye"),
		},
	}
	state, _, err := Open(nil, connect, now)
	if err != nil {
		t.Fatal(err)
	}
	if state.Will() == nil || state.Will().Topic != "status/will" {
		t.Fatal("expected will message stored in session")
	}

	res, err := state.Disconnect(now, DisconnectNormal)
	if err != nil {
		t.Fatal(err)
	}
	if res.PublishWillNow || res.ScheduleWill {
		t.Fatalf("normal disconnect must not publish or schedule will: %#v", res)
	}
	if !res.Lifecycle.WillDiscarded() {
		t.Fatal("expected willDiscarded to be true")
	}
	if _, claimed, ok := res.Lifecycle.ClaimWill(); ok || claimed != nil {
		t.Fatalf("expected ClaimWill on discarded will to return false, got %v", ok)
	}
}

// Traceability: PRD MQTT-01/TEST-04; MQTT-3.1.2-8; MQTT-3.1.2-10.
func TestLifecycle_WillTransitions_AbnormalAndWithWill_MQTT311(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	connect := mqtt.Connect{
		Version:  mqtt.ProtocolV311,
		ClientID: "will-311",
		Will: &mqtt.Will{
			Topic:   "status/abnormal",
			Payload: []byte("lost"),
		},
	}
	state, _, err := Open(nil, connect, now)
	if err != nil {
		t.Fatal(err)
	}
	res, err := state.Disconnect(now, DisconnectAbnormal)
	if err != nil {
		t.Fatal(err)
	}
	if !res.PublishWillNow || res.ScheduleWill || res.Will == nil || res.Will.Topic != "status/abnormal" {
		t.Fatalf("expected immediate will publication on abnormal disconnect: %#v", res)
	}
	if !res.Lifecycle.WillPublished() {
		t.Fatal("expected willPublished to be true")
	}
	if _, _, ok := res.Lifecycle.ClaimWill(); ok {
		t.Fatal("second ClaimWill on published will must return false")
	}
}

// Traceability: PRD MQTT-01/TEST-04; MQTT-3.1.2-8; MQTT-3.1.3-9.
func TestLifecycle_WillTransitions_MQTT5_ImmediateSessionExpiry(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	connect := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "will-v5-immediate",
		SessionExpiryInterval: 0,
		Will: &mqtt.Will{
			Topic:         "status/v5",
			Payload:       []byte("offline"),
			DelayInterval: 10,
		},
	}
	state, _, err := Open(nil, connect, now)
	if err != nil {
		t.Fatal(err)
	}
	res, err := state.Disconnect(now, DisconnectWithWill)
	if err != nil {
		t.Fatal(err)
	}
	if !res.PublishWillNow || res.ScheduleWill || !res.SessionRemoved {
		t.Fatalf("expected immediate will and session removal when expiry is 0: %#v", res)
	}
	if res.Will == nil || res.Will.Topic != "status/v5" {
		t.Fatalf("expected will message in DisconnectResult: %#v", res.Will)
	}
}

// Traceability: PRD MQTT-01/TEST-04; MQTT-3.1.2-8; MQTT-3.1.3-9.
func TestLifecycle_WillTransitions_MQTT5_DelayAndClamping(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	// Case A: Will Delay (5s) < Session Expiry (20s)
	connectA := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "delay-shorter",
		SessionExpiryInterval: 20,
		Will: &mqtt.Will{
			Topic:         "status/delay",
			Payload:       []byte("offline"),
			DelayInterval: 5,
		},
	}
	stateA, _, err := Open(nil, connectA, now)
	if err != nil {
		t.Fatal(err)
	}
	resA, err := stateA.Disconnect(now, DisconnectAbnormal)
	if err != nil {
		t.Fatal(err)
	}
	if resA.PublishWillNow || !resA.ScheduleWill || resA.WillDelay != 5*time.Second {
		t.Fatalf("expected scheduled will with 5s delay: %#v", resA)
	}
	if !resA.ScheduleExpiry || resA.ExpiryDuration != 20*time.Second {
		t.Fatalf("expected scheduled expiry with 20s duration: %#v", resA)
	}

	// Claim will from disconnected state
	updatedState, claimedWill, ok := resA.Lifecycle.ClaimWill()
	if !ok || claimedWill == nil || claimedWill.Topic != "status/delay" {
		t.Fatalf("ClaimWill failed: ok=%v, will=%#v", ok, claimedWill)
	}
	if !updatedState.WillPublished() {
		t.Fatal("expected WillPublished to be true after ClaimWill")
	}
	if _, _, ok := updatedState.ClaimWill(); ok {
		t.Fatal("second ClaimWill must return false")
	}

	// Case B: Will Delay (30s) > Session Expiry (10s) -> clamped to 10s
	connectB := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "delay-longer",
		SessionExpiryInterval: 10,
		Will: &mqtt.Will{
			Topic:         "status/clamped",
			Payload:       []byte("offline"),
			DelayInterval: 30,
		},
	}
	stateB, _, err := Open(nil, connectB, now)
	if err != nil {
		t.Fatal(err)
	}
	resB, err := stateB.Disconnect(now, DisconnectWithWill)
	if err != nil {
		t.Fatal(err)
	}
	if resB.PublishWillNow || !resB.ScheduleWill || resB.WillDelay != 10*time.Second {
		t.Fatalf("expected will delay clamped to session expiry (10s): %#v", resB)
	}
	if !resB.ScheduleExpiry || resB.ExpiryDuration != 10*time.Second {
		t.Fatalf("expected expiry duration 10s: %#v", resB)
	}

	// Case C: Reconnect cancels old Will
	connectReconnect := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "delay-shorter",
		CleanStart:            false,
		SessionExpiryInterval: 20,
	}
	resumed, present, err := Open(&resA.Lifecycle, connectReconnect, now.Add(2*time.Second))
	if err != nil || !present || !resumed.Connected() {
		t.Fatalf("resumed Open failed: present=%v, err=%v", present, err)
	}
	if resumed.Will() != nil {
		t.Fatal("reconnected session without will must have nil will")
	}
}

// Traceability: PRD MQTT-01/TEST-04; MQTT-3.1.2-8; MQTT-3.1.3-9.
func TestLifecycle_ExpireClaimsPendingWill(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	connect := mqtt.Connect{
		Version:               mqtt.ProtocolV5,
		ClientID:              "will-expire",
		SessionExpiryInterval: 10,
		Will: &mqtt.Will{
			Topic:         "status/expired",
			Payload:       []byte("bye"),
			DelayInterval: 15,
		},
	}
	state, _, err := Open(nil, connect, now)
	if err != nil {
		t.Fatal(err)
	}
	res, err := state.Disconnect(now, DisconnectAbnormal)
	if err != nil {
		t.Fatal(err)
	}

	// Before expiry
	_, _, ok := res.Lifecycle.Expire(now.Add(5 * time.Second))
	if ok {
		t.Fatal("Expire before deadline should return false")
	}

	// At expiry
	expiredState, will, ok := res.Lifecycle.Expire(now.Add(10 * time.Second))
	if !ok || will == nil || will.Topic != "status/expired" {
		t.Fatalf("Expire at deadline should claim will: ok=%v, will=%#v", ok, will)
	}
	if !expiredState.Expired(now.Add(10 * time.Second)) {
		t.Fatal("session should be marked expired")
	}

	// Subsequent expire
	_, _, ok = expiredState.Expire(now.Add(11 * time.Second))
	if ok {
		t.Fatal("subsequent Expire should return false")
	}
}

// Traceability: TEN-02
func TestLifecycle_SessionKeyIsolation(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tenantA, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := tenancy.NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	keyA, err := tenancy.NewSessionKey(tenantA, "device-1")
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := tenancy.NewSessionKey(tenantB, "device-1")
	if err != nil {
		t.Fatal(err)
	}

	connect := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "device-1", SessionExpiryInterval: 100}
	stateA, present, err := Open(nil, connect, now, keyA)
	if err != nil || present || stateA.SessionKey() != keyA || stateA.TenantID() != tenantA {
		t.Fatalf("Open with keyA failed: %#v, %v", stateA, err)
	}

	disRes, err := stateA.Disconnect(now, DisconnectNormal)
	if err != nil {
		t.Fatal(err)
	}
	closedA := disRes.Lifecycle

	// Cross-tenant resume with same ClientID must fail with ErrIdentityMismatch
	if _, _, err := Open(&closedA, connect, now.Add(time.Second), keyB); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("cross-tenant Open error = %v, want ErrIdentityMismatch", err)
	}

	// Same-tenant resume must succeed
	resumedA, present, err := Open(&closedA, connect, now.Add(time.Second), keyA)
	if err != nil || !present || resumedA.SessionKey() != keyA {
		t.Fatalf("same-tenant resume failed: %#v, %v", resumedA, err)
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestRestoreValidatesCommittedLifecycle(t *testing.T) {
	tenant, _ := tenancy.NewTenantID("restore")
	key, _ := tenancy.NewSessionKey(tenant, "client")
	now := time.Unix(1000, 0)
	base := RestoreInput{Key: key, Version: mqtt.ProtocolV5, Retention: RetentionFinite, ExpiryInterval: 7, Connected: true}
	for _, test := range []struct {
		name   string
		mutate func(*RestoreInput)
	}{
		{"zero key", func(in *RestoreInput) { in.Key = tenancy.SessionKey{} }},
		{"unsupported version", func(in *RestoreInput) { in.Version = 3 }},
		{"unknown retention", func(in *RestoreInput) { in.Retention = 255 }},
		{"connected deadline", func(in *RestoreInput) { in.ExpiresAt = now }},
		{"disconnected immediate", func(in *RestoreInput) { in.Connected = false; in.Retention = RetentionImmediate }},
		{"finite missing deadline", func(in *RestoreInput) { in.Connected = false }},
		{"infinite deadline", func(in *RestoreInput) { in.Connected = false; in.Retention = RetentionInfinite; in.ExpiresAt = now }},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := base
			test.mutate(&input)
			if _, err := Restore(input); !errors.Is(err, ErrInvalidRestore) {
				t.Fatalf("Restore error=%v, want ErrInvalidRestore", err)
			}
		})
	}
	will := &mqtt.Will{Topic: "restore/will", Payload: []byte("payload"), DelayInterval: 2}
	for _, version := range []mqtt.ProtocolVersion{mqtt.ProtocolV311, mqtt.ProtocolV5} {
		for _, interval := range []uint32{0, 7, ^uint32(0)} {
			packet := mqtt.Connect{Version: version, ClientID: key.ClientID(), SessionExpiryInterval: interval, CleanStart: interval == 0, Will: will}
			original, _, err := Open(nil, packet, now, key)
			if err != nil {
				t.Fatal(err)
			}
			restored, err := Restore(RestoreInput{Key: key, Version: version, Retention: original.Retention(), ExpiryInterval: original.ExpiryInterval(), Connected: true, Will: original.Will()})
			if err != nil {
				t.Fatal(err)
			}
			expected, err := original.Disconnect(now, DisconnectAbnormal)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := restored.Disconnect(now, DisconnectAbnormal)
			if err != nil || !reflect.DeepEqual(actual, expected) {
				t.Fatalf("Disconnect version=%d interval=%d: got %#v err=%v, want %#v", version, interval, actual, err, expected)
			}
			if expected.SessionRemoved {
				continue
			}
			deadline, _ := expected.Lifecycle.ExpiresAt()
			closed, err := Restore(RestoreInput{Key: key, Version: version, Retention: original.Retention(), ExpiryInterval: original.ExpiryInterval(), ExpiresAt: deadline, Will: original.Will()})
			if err != nil {
				t.Fatal(err)
			}
			// Recover only the remaining Will, before either lifecycle claims it.
			before := now.Add(time.Second)
			_, _, gotExpired := closed.Expire(before)
			_, _, wantExpired := expected.Lifecycle.Expire(before)
			if gotExpired != wantExpired {
				t.Fatal("restored early expiry differs")
			}
			if !deadline.IsZero() {
				wantState, wantWill, wantExpired := expected.Lifecycle.Expire(deadline)
				gotState, gotWill, gotExpired := closed.Expire(deadline)
				if gotExpired != wantExpired || !reflect.DeepEqual(gotWill, wantWill) || gotState.Expired(deadline) != wantState.Expired(deadline) {
					t.Fatalf("restored expiry differs: got will=%#v expired=%v, want will=%#v expired=%v", gotWill, gotExpired, wantWill, wantExpired)
				}
			}
		}
	}
	restored, err := Restore(RestoreInput{Key: key, Version: mqtt.ProtocolV5, Retention: RetentionInfinite, Connected: true, Will: will})
	if err != nil {
		t.Fatal(err)
	}
	will.Payload[0] = 'X'
	if string(restored.Will().Payload) != "payload" {
		t.Fatal("restored Will aliases caller payload")
	}
}

// Traceability: PRD MQTT-01/TEST-04; DEP-07/DEP-12; MQTT-3.1.2-8; docs/specs/drain-and-rolling-update.md §5.
func TestLifecycle_TakeoverAndPlannedHandoffWillRules(t *testing.T) {
	for _, test := range []struct {
		name                       string
		reason                     DisconnectReason
		version                    mqtt.ProtocolVersion
		delay, expiry              uint32
		publish, schedule, discard bool
		wantDelay                  time.Duration
	}{
		{"takeover", DisconnectTakeover, mqtt.ProtocolV5, 10, 60, false, false, true, 0},
		{"mqtt311", DisconnectPlannedHandoff, mqtt.ProtocolV311, 0, 0, true, false, false, 0},
		{"zero", DisconnectPlannedHandoff, mqtt.ProtocolV5, 0, 60, true, false, false, 0},
		{"delayed", DisconnectPlannedHandoff, mqtt.ProtocolV5, 10, 60, false, true, false, 10 * time.Second},
		{"immediate retention", DisconnectPlannedHandoff, mqtt.ProtocolV5, 10, 0, true, false, false, 0},
		{"clamped", DisconnectPlannedHandoff, mqtt.ProtocolV5, 10, 3, false, true, false, 3 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(1, 0)
			state, _, err := Open(nil, mqtt.Connect{Version: test.version, ClientID: "client", SessionExpiryInterval: test.expiry, Will: &mqtt.Will{Topic: "status/will", DelayInterval: test.delay}}, now)
			if err != nil || !state.HasPendingWill() {
				t.Fatalf("pending Will before disconnect: %v", err)
			}
			result, err := state.Disconnect(now, test.reason)
			if err != nil || result.PublishWillNow != test.publish || result.ScheduleWill != test.schedule || result.WillDelay != test.wantDelay || result.Lifecycle.WillDiscarded() != test.discard {
				t.Fatalf("Disconnect() = %+v, %v", result, err)
			}
			if result.Lifecycle.HasPendingWill() != test.schedule {
				t.Fatal("pending Will state differs from scheduled outcome")
			}
			if test.schedule {
				claimed, _, ok := result.Lifecycle.ClaimWill()
				if !ok || claimed.HasPendingWill() {
					t.Fatal("claim left pending Will")
				}
			}
		})
	}
}
