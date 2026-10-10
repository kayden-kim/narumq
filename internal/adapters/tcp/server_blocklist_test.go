package tcp

import (
	"testing"
	"time"

	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
	"narumq/internal/application/ports/audit"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/security"
	"narumq/internal/domain/tenancy"
)

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
func TestTCPCommittedBlocklistClosesMatchingSessionsAndDiscardsWill(t *testing.T) {
	for _, test := range []struct {
		name           string
		version        mqtt.ProtocolVersion
		wantReason     byte
		wantDisconnect bool
		withWill       bool
	}{
		{name: "MQTT 3.1.1", version: mqtt.ProtocolV311, wantReason: 0x05},
		{name: "MQTT 5", version: mqtt.ProtocolV5, wantReason: 0x8A, wantDisconnect: true, withWill: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheduler := &tcpCredentialScheduler{}
			runtime, tenant := newTCPCredentialRuntime(t, &tcpCredentialRevocationChecker{}, scheduler, time.Hour, test.withWill)
			defer runtime.stop(t)
			auditRecorder := &tcpCredentialAuditRecorder{}
			connect.WithDataPlaneAudit(auditRecorder)(runtime.server.connections)

			var will *mqtt.Will
			if test.withWill {
				will = &mqtt.Will{Topic: "expiry/will", Payload: []byte("blocked")}
			}
			client := runtime.client
			defer client.Close()
			if connAck := tcpCredentialConnect(t, runtime, client, "victim", test.version, will); connAck.ReasonCode != 0 {
				t.Fatalf("initial CONNACK reason = 0x%02x, want success", connAck.ReasonCode)
			}
			if !waitForActiveConnections(runtime.server, 1, time.Second) || len(runtime.server.ConnectedSessionIDs()) != 1 {
				t.Fatal("accepted session was not bound")
			}
			unmatched := tcpCredentialDial(t, runtime)
			defer unmatched.Close()
			if connAck := tcpCredentialConnect(t, runtime, unmatched, "watcher", test.version, nil); connAck.ReasonCode != 0 {
				t.Fatalf("unmatched session CONNACK reason = 0x%02x, want success", connAck.ReasonCode)
			}
			if !waitForActiveConnections(runtime.server, 2, time.Second) || len(runtime.server.ConnectedSessionIDs()) != 2 {
				t.Fatal("both accepted sessions were not bound")
			}

			entry, err := security.NewBlockEntry(security.BlockKindClientID, tenant.String(), "victim")
			if err != nil {
				t.Fatal(err)
			}
			runtime.server.ApplyBlocklist(entry, true)
			if test.wantDisconnect {
				packet, _, err := runtime.server.decoder.Decode(readTestFrame(t, client), mqtt.ProtocolV5)
				disconnect, ok := packet.(*mqtt.Disconnect)
				if err != nil || !ok || disconnect.ReasonCode != 0x98 {
					t.Fatalf("blocklist DISCONNECT = (%#v, %v), want reason 0x98", packet, err)
				}
				if !scheduler.waitForDelay(t, drainDisconnectFlushTimeout, 0).fire() {
					t.Fatal("MQTT 5 forced-close timer did not fire")
				}
			}
			tcpCredentialAssertClosed(t, client)
			if !waitForActiveConnections(runtime.server, 1, time.Second) {
				t.Fatal("blocklist did not preserve the unmatched session")
			}

			if test.withWill {
				key, err := tenancy.NewSessionKey(tenant, "victim")
				if err != nil {
					t.Fatal(err)
				}
				state, found, err := runtime.repo.LoadKey(key)
				if err != nil || !found || state.HasPendingWill() || !state.WillDiscarded() || state.WillPublished() {
					t.Fatalf("blocked session Will state = (%#v, found=%t, err=%v), want discarded and unpublished", state, found, err)
				}
			}

			later := tcpCredentialDial(t, runtime)
			defer later.Close()
			if connAck := tcpCredentialConnect(t, runtime, later, "victim", test.version, nil); connAck.ReasonCode != test.wantReason {
				t.Fatalf("blocked reconnect CONNACK reason = 0x%02x, want 0x%02x", connAck.ReasonCode, test.wantReason)
			}
			if !waitForActiveConnections(runtime.server, 1, time.Second) {
				t.Fatal("blocked reconnect changed the unmatched active session")
			}
			denials := auditRecorder.snapshot()
			if len(denials) != 1 || denials[0].Action != audit.DataPlaneActionConnect || denials[0].Reason != "blocked" || denials[0].ClientID != "victim" {
				t.Fatalf("blocked reconnect audit = %+v", denials)
			}
		})
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
func TestTCPBlocklistSnapshotReplaceClosesAndUnblocksConnections(t *testing.T) {
	scheduler := &tcpCredentialScheduler{}
	runtime, tenant := newTCPCredentialRuntime(t, &tcpCredentialRevocationChecker{}, scheduler, time.Hour, false)
	defer runtime.stop(t)
	client := runtime.client
	defer client.Close()
	if connAck := tcpCredentialConnect(t, runtime, client, "victim", mqtt.ProtocolV5, nil); connAck.ReasonCode != 0 {
		t.Fatalf("initial CONNACK reason = 0x%02x, want success", connAck.ReasonCode)
	}

	entry, err := security.NewBlockEntry(security.BlockKindClientID, tenant.String(), "victim")
	if err != nil {
		t.Fatal(err)
	}
	runtime.server.ReplaceBlocklist([]security.BlockEntry{entry})
	packet, _, err := runtime.server.decoder.Decode(readTestFrame(t, client), mqtt.ProtocolV5)
	disconnect, ok := packet.(*mqtt.Disconnect)
	if err != nil || !ok || disconnect.ReasonCode != 0x98 {
		t.Fatalf("restored blocklist DISCONNECT = (%#v, %v), want reason 0x98", packet, err)
	}
	if !scheduler.waitForDelay(t, drainDisconnectFlushTimeout, 0).fire() {
		t.Fatal("MQTT 5 forced-close timer did not fire")
	}
	tcpCredentialAssertClosed(t, client)
	if !waitForActiveConnections(runtime.server, 0, time.Second) {
		t.Fatal("snapshot-blocked connection remained active")
	}

	runtime.server.ReplaceBlocklist(nil)
	later := tcpCredentialDial(t, runtime)
	defer later.Close()
	if connAck := tcpCredentialConnect(t, runtime, later, "victim", mqtt.ProtocolV5, nil); connAck.ReasonCode != 0 {
		t.Fatalf("reconnect after empty snapshot CONNACK reason = 0x%02x, want success", connAck.ReasonCode)
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
func TestTCPConnectRejectsBlockAppliedAfterOwnershipCommit(t *testing.T) {
	checker := &tcpCredentialRevocationChecker{}
	scheduler := &tcpCredentialScheduler{}
	runtime, tenant := newTCPCredentialRuntime(t, checker, scheduler, time.Hour, true)
	defer runtime.stop(t)
	entry, err := security.NewBlockEntry(security.BlockKindClientID, tenant.String(), "victim")
	if err != nil {
		t.Fatal(err)
	}
	runtime.cluster.mu.Lock()
	runtime.cluster.ownershipCommitted = func(tenantID, clientID string, ownerNodeID, epoch uint64) {
		runtime.server.RecordCommittedOwnership(membership.OwnershipStateDTO{
			TenantID: tenantID, ClientID: clientID, OwnerNodeID: ownerNodeID, Epoch: epoch,
		})
		runtime.server.ApplyBlocklist(entry, true)
	}
	runtime.cluster.mu.Unlock()
	auditRecorder := &tcpCredentialAuditRecorder{}
	connect.WithDataPlaneAudit(auditRecorder)(runtime.server.connections)

	will := &mqtt.Will{Topic: "expiry/will", Payload: []byte("must-not-publish")}
	connAck := tcpCredentialConnect(t, runtime, runtime.client, "victim", mqtt.ProtocolV5, will)
	if connAck.ReasonCode != 0x8A {
		t.Fatalf("post-ownership blocklist CONNACK reason = 0x%02x, want 0x8a", connAck.ReasonCode)
	}
	if !waitForActiveConnections(runtime.server, 0, time.Second) || len(runtime.server.ConnectedSessionIDs()) != 0 {
		t.Fatal("blocked CONNECT created an active session")
	}
	denials := auditRecorder.snapshot()
	if len(denials) != 1 || denials[0].Action != audit.DataPlaneActionConnect ||
		denials[0].Reason != "blocked" || denials[0].ClientID != "victim" {
		t.Fatalf("post-ownership blocklist denial audit = %+v", denials)
	}
	key, err := tenancy.NewSessionKey(tenant, "victim")
	if err != nil {
		t.Fatal(err)
	}
	state, found, err := runtime.repo.LoadKey(key)
	if err != nil || !found || state.HasPendingWill() || !state.WillDiscarded() || state.WillPublished() {
		t.Fatalf("post-ownership blocked session Will = (%#v, found=%t, err=%v), want discarded and unpublished", state, found, err)
	}
}
