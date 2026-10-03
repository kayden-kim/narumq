package records

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

func sessionMetadataRecordForTest(client string, epoch uint64) *SessionMetadataState {
	return &SessionMetadataState{
		TenantId: "tenant", ClientId: client, Epoch: epoch,
		ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_FINITE,
		ExpiryIntervalSeconds: 60, Connected: false,
		DetachedAtUnixMillis: 100, SessionExpiresAtUnixMillis: 200,
		Will:                &SessionWill{Topic: "will/topic", Payload: []byte("payload"), Qos: 1, DelayIntervalSeconds: 2},
		WillDueAtUnixMillis: 150,
		Principal:           &SessionPrincipal{Subject: "subject", CredentialId: "credential", Roles: []string{"writer", "reader"}, ExpiresAtUnixMillis: 300},
	}
}

func sessionMetadataSnapshotForTest() *ReplicatedStateSnapshot {
	return &ReplicatedStateSnapshot{
		ActiveFeatures:  []string{"session-metadata"},
		Ownerships:      []*OwnershipState{{TenantId: "tenant", ClientId: "z", OwnerNodeId: 1, Epoch: 3}, {TenantId: "tenant", ClientId: "a", OwnerNodeId: 1, Epoch: 2}},
		SessionMetadata: []*SessionMetadataState{sessionMetadataRecordForTest("z", 2), sessionMetadataRecordForTest("a", 2)},
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestSessionMetadataCommandCodecRoundTripsAndValidatesBoundaries(t *testing.T) {
	valid := &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_SessionMetadata{SessionMetadata: &SessionMetadataChange{
		TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 2,
		Action:               SessionMetadataAction_SESSION_METADATA_ACTION_DETACH,
		WillDisposition:      SessionWillDisposition_SESSION_WILL_DISPOSITION_SCHEDULE,
		ProposedAtUnixMillis: 100, SessionExpiresAtUnixMillis: 200, WillDueAtUnixMillis: 150,
	}}}
	wire, err := MarshalCommand(valid)
	if err != nil {
		t.Fatalf("MarshalCommand(): %v", err)
	}
	decoded, err := UnmarshalCommand(wire)
	if err != nil || !proto.Equal(decoded, valid) {
		t.Fatalf("UnmarshalCommand() = (%v, %v), want round trip", decoded, err)
	}

	validOwnership := &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_Ownership{Ownership: &OwnershipChange{
		TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1,
		Action: OwnershipAction_OWNERSHIP_ACTION_ACQUIRE,
		Session: &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_FINITE, ExpiryIntervalSeconds: 60,
			Will:      &SessionWill{Topic: "will", Payload: make([]byte, maxSessionWillPayloadBytes), Qos: 2, DelayIntervalSeconds: 4},
			Principal: &SessionPrincipal{Subject: strings.Repeat("s", maxSessionPrincipalFieldBytes), CredentialId: "credential", Roles: []string{"role"}, ExpiresAtUnixMillis: 1}},
	}}}
	if _, err := MarshalCommand(validOwnership); err != nil {
		t.Fatalf("MarshalCommand(valid ownership session): %v", err)
	}

	invalidChanges := []struct {
		name   string
		change *SessionMetadataChange
	}{
		{"identity", &SessionMetadataChange{TenantId: "", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: SessionMetadataAction_SESSION_METADATA_ACTION_CLAIM_WILL, ProposedAtUnixMillis: 1}},
		{"owner", &SessionMetadataChange{TenantId: "tenant", ClientId: "client", Epoch: 1, Action: SessionMetadataAction_SESSION_METADATA_ACTION_CLAIM_WILL, ProposedAtUnixMillis: 1}},
		{"epoch", &SessionMetadataChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Action: SessionMetadataAction_SESSION_METADATA_ACTION_CLAIM_WILL, ProposedAtUnixMillis: 1}},
		{"proposed at", &SessionMetadataChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: SessionMetadataAction_SESSION_METADATA_ACTION_CLAIM_WILL}},
		{"unknown action", &SessionMetadataChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: 99, ProposedAtUnixMillis: 1}},
		{"detach unspecified", &SessionMetadataChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: SessionMetadataAction_SESSION_METADATA_ACTION_DETACH, ProposedAtUnixMillis: 1}},
		{"scheduled due absent", &SessionMetadataChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: SessionMetadataAction_SESSION_METADATA_ACTION_DETACH, WillDisposition: SessionWillDisposition_SESSION_WILL_DISPOSITION_SCHEDULE, ProposedAtUnixMillis: 1}},
		{"scheduled due before proposed", &SessionMetadataChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: SessionMetadataAction_SESSION_METADATA_ACTION_DETACH, WillDisposition: SessionWillDisposition_SESSION_WILL_DISPOSITION_SCHEDULE, ProposedAtUnixMillis: 2, WillDueAtUnixMillis: 1}},
		{"claim timestamps", &SessionMetadataChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: SessionMetadataAction_SESSION_METADATA_ACTION_CLAIM_WILL, ProposedAtUnixMillis: 1, SessionExpiresAtUnixMillis: 2}},
		{"discard schedule", &SessionMetadataChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: SessionMetadataAction_SESSION_METADATA_ACTION_DISCARD, WillDisposition: SessionWillDisposition_SESSION_WILL_DISPOSITION_SCHEDULE, ProposedAtUnixMillis: 1}},
		{"expiry not after proposed", &SessionMetadataChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: SessionMetadataAction_SESSION_METADATA_ACTION_DETACH, WillDisposition: SessionWillDisposition_SESSION_WILL_DISPOSITION_DISCARD, ProposedAtUnixMillis: 2, SessionExpiresAtUnixMillis: 2}},
	}
	for _, tc := range invalidChanges {
		t.Run(tc.name, func(t *testing.T) {
			command := &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_SessionMetadata{SessionMetadata: tc.change}}
			if _, err := MarshalCommand(command); err == nil {
				t.Fatal("MarshalCommand() accepted invalid session metadata change")
			}
		})
	}

	invalidSessions := []struct {
		name    string
		session *SessionConnect
		action  OwnershipAction
	}{
		{"protocol", &SessionConnect{ProtocolVersion: 3, Retention: SessionRetention_SESSION_RETENTION_INFINITE}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"unknown retention", &SessionConnect{ProtocolVersion: 5, Retention: 9, ExpiryIntervalSeconds: 1}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"v4 finite", &SessionConnect{ProtocolVersion: 4, Retention: SessionRetention_SESSION_RETENTION_FINITE}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"v4 interval", &SessionConnect{ProtocolVersion: 4, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: 1}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"v4 delay", &SessionConnect{ProtocolVersion: 4, Retention: SessionRetention_SESSION_RETENTION_INFINITE, Will: &SessionWill{Topic: "will", DelayIntervalSeconds: 1}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"immediate interval", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_IMMEDIATE, ExpiryIntervalSeconds: 1}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"finite zero", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_FINITE}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"finite infinite sentinel", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_FINITE, ExpiryIntervalSeconds: ^uint32(0)}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"infinite interval", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: 1}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"invalid topic", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0), Will: &SessionWill{Topic: "a/#"}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"payload too large", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0), Will: &SessionWill{Topic: "will", Payload: make([]byte, maxSessionWillPayloadBytes+1)}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"qos too large", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0), Will: &SessionWill{Topic: "will", Qos: 3}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"principal without will", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0), Principal: &SessionPrincipal{Subject: "s", CredentialId: "c", ExpiresAtUnixMillis: 1}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"principal missing expiry", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0), Will: &SessionWill{Topic: "will"}, Principal: &SessionPrincipal{Subject: "s", CredentialId: "c"}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"duplicate roles", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0), Will: &SessionWill{Topic: "will"}, Principal: &SessionPrincipal{Subject: "s", CredentialId: "c", Roles: []string{"r", "r"}, ExpiresAtUnixMillis: 1}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"subject too large", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0), Will: &SessionWill{Topic: "will"}, Principal: &SessionPrincipal{Subject: strings.Repeat("s", maxSessionPrincipalFieldBytes+1), CredentialId: "c", ExpiresAtUnixMillis: 1}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"invalid principal UTF-8", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0), Will: &SessionWill{Topic: "will"}, Principal: &SessionPrincipal{Subject: string([]byte{0xff}), CredentialId: "c", ExpiresAtUnixMillis: 1}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"too many roles", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0), Will: &SessionWill{Topic: "will"}, Principal: &SessionPrincipal{Subject: "s", CredentialId: "c", Roles: make([]string, maxSessionPrincipalRoles+1), ExpiresAtUnixMillis: 1}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"role too large", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0), Will: &SessionWill{Topic: "will"}, Principal: &SessionPrincipal{Subject: "s", CredentialId: "c", Roles: []string{strings.Repeat("r", maxSessionPrincipalRoleBytes+1)}, ExpiresAtUnixMillis: 1}}, OwnershipAction_OWNERSHIP_ACTION_ACQUIRE},
		{"release session", &SessionConnect{ProtocolVersion: 5, Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0)}, OwnershipAction_OWNERSHIP_ACTION_RELEASE},
	}
	for _, tc := range invalidSessions {
		t.Run(tc.name, func(t *testing.T) {
			command := &CommandEnvelope{SchemaMajor: 1, OperationId: op1, Payload: &CommandEnvelope_Ownership{Ownership: &OwnershipChange{TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: tc.action, Session: tc.session}}}
			if _, err := MarshalCommand(command); err == nil {
				t.Fatal("MarshalCommand() accepted invalid ownership session")
			}
		})
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestSessionMetadataSnapshotCodecSortsCountsAndValidatesState(t *testing.T) {
	state := sessionMetadataSnapshotForTest()
	before := proto.Clone(state)
	wire, err := MarshalSnapshot(state, 10)
	if err != nil {
		t.Fatalf("MarshalSnapshot(): %v", err)
	}
	if !proto.Equal(state, before) {
		t.Fatal("MarshalSnapshot() mutated source snapshot")
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(): %v", err)
	}
	if envelope.ObjectCount != 5 {
		t.Fatalf("object count = %d, want 5", envelope.ObjectCount)
	}
	if envelope.State.SessionMetadata[0].ClientId != "a" || envelope.State.SessionMetadata[1].ClientId != "z" {
		t.Fatalf("session metadata order = %q, %q", envelope.State.SessionMetadata[0].ClientId, envelope.State.SessionMetadata[1].ClientId)
	}
	reencoded, err := MarshalSnapshot(envelope.State, 10)
	if err != nil || !bytes.Equal(reencoded, wire) {
		t.Fatalf("canonical snapshot round trip error = %v", err)
	}

	changed := proto.Clone(envelope).(*SnapshotEnvelope)
	changed.State.SessionMetadata[0].Will.Payload = []byte("different")
	changedWire, err := MarshalSnapshot(changed.State, 10)
	if err != nil {
		t.Fatalf("MarshalSnapshot(changed state): %v", err)
	}
	changedEnvelope, err := UnmarshalSnapshot(changedWire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(changed state): %v", err)
	}
	if bytes.Equal(envelope.Sha256, changedEnvelope.Sha256) {
		t.Fatal("session metadata change did not change snapshot digest")
	}

	wrongCount := proto.Clone(envelope).(*SnapshotEnvelope)
	wrongCount.ObjectCount--
	wrongCountWire, err := deterministic.Marshal(wrongCount)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnmarshalSnapshot(wrongCountWire); err == nil {
		t.Fatal("UnmarshalSnapshot() accepted a session-metadata object-count mismatch")
	}

	invalidStates := []struct {
		name  string
		state *ReplicatedStateSnapshot
	}{
		{"feature inactive", func() *ReplicatedStateSnapshot {
			s := sessionMetadataSnapshotForTest()
			s.ActiveFeatures = nil
			return s
		}()},
		{"missing ownership", func() *ReplicatedStateSnapshot {
			s := sessionMetadataSnapshotForTest()
			s.Ownerships = s.Ownerships[:1]
			return s
		}()},
		{"epoch above ownership", func() *ReplicatedStateSnapshot {
			s := sessionMetadataSnapshotForTest()
			s.SessionMetadata[1].Epoch = 4
			return s
		}()},
		{"duplicate key", func() *ReplicatedStateSnapshot {
			s := sessionMetadataSnapshotForTest()
			s.SessionMetadata[1].ClientId = s.SessionMetadata[0].ClientId
			return s
		}()},
		{"connected detached fields", func() *ReplicatedStateSnapshot {
			s := sessionMetadataSnapshotForTest()
			s.SessionMetadata[0].Connected = true
			return s
		}()},
		{"immediate detached", func() *ReplicatedStateSnapshot {
			s := sessionMetadataSnapshotForTest()
			s.SessionMetadata[0].Retention = SessionRetention_SESSION_RETENTION_IMMEDIATE
			s.SessionMetadata[0].ExpiryIntervalSeconds = 0
			return s
		}()},
		{"finite missing expiry", func() *ReplicatedStateSnapshot {
			s := sessionMetadataSnapshotForTest()
			s.SessionMetadata[0].SessionExpiresAtUnixMillis = 0
			return s
		}()},
		{"will due mismatch", func() *ReplicatedStateSnapshot {
			s := sessionMetadataSnapshotForTest()
			s.SessionMetadata[0].WillDueAtUnixMillis = 0
			return s
		}()},
	}
	for _, tc := range invalidStates {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := MarshalSnapshot(tc.state, 10); err == nil {
				t.Fatal("MarshalSnapshot() accepted invalid session metadata state")
			}
		})
	}

	boundary := &ReplicatedStateSnapshot{ActiveFeatures: []string{"session-metadata"}}
	for i := range maxSessionWillBytes / (maxSessionWillPayloadBytes + 1) {
		client := "c" + string(rune('a'+i))
		boundary.Ownerships = append(boundary.Ownerships, &OwnershipState{TenantId: "tenant", ClientId: client, OwnerNodeId: 1, Epoch: 1})
		boundary.SessionMetadata = append(boundary.SessionMetadata, &SessionMetadataState{
			TenantId: "tenant", ClientId: client, Epoch: 1, ProtocolVersion: 5,
			Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0),
			Will: &SessionWill{Topic: "w", Payload: make([]byte, maxSessionWillPayloadBytes)}, WillDueAtUnixMillis: 2,
			DetachedAtUnixMillis: 1,
		})
	}
	if err := validateSnapshotState(boundary); err != nil {
		t.Fatalf("validateSnapshotState(exact 8 MiB): %v", err)
	}
	boundary.SessionMetadata = append(boundary.SessionMetadata, &SessionMetadataState{
		TenantId: "tenant", ClientId: "overflow", Epoch: 1, ProtocolVersion: 5,
		Retention: SessionRetention_SESSION_RETENTION_INFINITE, ExpiryIntervalSeconds: ^uint32(0),
		Will: &SessionWill{Topic: "w", Payload: make([]byte, maxSessionWillPayloadBytes)}, WillDueAtUnixMillis: 2,
		DetachedAtUnixMillis: 1,
	})
	boundary.Ownerships = append(boundary.Ownerships, &OwnershipState{TenantId: "tenant", ClientId: "overflow", OwnerNodeId: 1, Epoch: 1})
	if err := validateSnapshotState(boundary); err == nil {
		t.Fatal("validateSnapshotState() accepted Will bytes over 8 MiB")
	}
}
