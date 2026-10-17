package main

import (
	"bytes"
	"reflect"
	"testing"

	"narumq/internal/adapters/raft/records"
	"narumq/internal/application/membership"

	"google.golang.org/protobuf/proto"
)

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-168
func TestClusterSessionMetadataRecordBridgeRoundTrips(t *testing.T) {
	will := &membership.SessionWillDTO{
		Topic: "meta/will/client", Payload: []byte{0, 1, 2}, QoS: 2, Retain: true, DelayIntervalSeconds: 7,
	}
	principal := &membership.SessionPrincipalDTO{
		Subject: "subject", CredentialID: "credential", Roles: []string{"reader", "publisher"}, ExpiresAtUnixMillis: 9000,
	}
	connect := membership.SessionConnectDTO{
		ProtocolVersion: 5, Retention: membership.SessionRetentionFinite, ExpiryIntervalSeconds: 30,
		Will: will, Principal: principal,
	}
	commands := []membership.CommandDTO{
		{SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1, Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant", ClientID: "connected", OwnerNodeID: 3, Epoch: 4,
			Action: membership.OwnershipActionTransfer, Session: &connect,
		}},
		{SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1, Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant", ClientID: "legacy", OwnerNodeID: 3, Epoch: 4,
			Action: membership.OwnershipActionAcquire,
		}},
		{SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1, SessionMetadata: &membership.SessionMetadataChangeDTO{
			TenantID: "tenant", ClientID: "detached", OwnerNodeID: 3, Epoch: 4,
			Action: membership.SessionMetadataActionDetach, WillDisposition: membership.SessionWillDispositionSchedule,
			ProposedAtUnixMillis: 1000, SessionExpiresAtUnixMillis: 31000, WillDueAtUnixMillis: 8000,
		}},
		{SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1, SessionMetadata: &membership.SessionMetadataChangeDTO{
			TenantID: "tenant", ClientID: "claimed", OwnerNodeID: 3, Epoch: 4,
			Action:               membership.SessionMetadataActionClaimWill,
			WillDisposition:      membership.SessionWillDispositionUnspecified,
			ProposedAtUnixMillis: 1000,
		}},
	}
	for i, command := range commands {
		encoded, err := encodeMembershipCommand(command)
		if err != nil {
			t.Fatalf("encode command %d: %v", i, err)
		}
		if command.Ownership != nil && command.Ownership.Session == nil {
			legacyWire, err := records.UnmarshalCommand(encoded)
			if err != nil {
				t.Fatalf("decode legacy ownership wire: %v", err)
			}
			if legacyWire.GetOwnership().GetSession() != nil {
				t.Fatal("legacy ownership encoded an empty session message")
			}
		}
		decoded, err := decodeMembershipCommand(encoded)
		if err != nil {
			t.Fatalf("decode command %d: %v", i, err)
		}
		if !reflect.DeepEqual(decoded, command) {
			t.Fatalf("command %d round trip = %#v, want %#v", i, decoded, command)
		}
		reencoded, err := encodeMembershipCommand(decoded)
		if err != nil {
			t.Fatalf("re-encode command %d: %v", i, err)
		}
		if !bytes.Equal(reencoded, encoded) {
			t.Fatalf("command %d wire bytes changed after round trip", i)
		}
	}

	metadata := membership.SessionMetadataStateDTO{
		TenantID: "tenant", ClientID: "offline", Epoch: 4, ProtocolVersion: 5,
		Retention: membership.SessionRetentionFinite, ExpiryIntervalSeconds: 60,
		Connected: false, DetachedAtUnixMillis: 1000, SessionExpiresAtUnixMillis: 61000,
		Will: will, WillDueAtUnixMillis: 7000, Principal: principal,
	}
	snapshot := membership.SnapshotDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, LastIncludedIndex: 8,
		Ownerships:     []membership.OwnershipStateDTO{{TenantID: "tenant", ClientID: "offline", OwnerNodeID: 3, Epoch: 4}},
		ActiveFeatures: []string{membership.SessionMetadataFeature}, SessionMetadata: []membership.SessionMetadataStateDTO{metadata},
		ObjectCount: 3,
	}
	encoded, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	decoded, err := decodeMembershipSnapshot(encoded)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if !reflect.DeepEqual(decoded.SessionMetadata, snapshot.SessionMetadata) {
		t.Fatalf("metadata snapshot round trip = %#v, want %#v", decoded.SessionMetadata, snapshot.SessionMetadata)
	}
	if decoded.ObjectCount != snapshot.ObjectCount {
		t.Fatalf("snapshot object count = %d, want %d", decoded.ObjectCount, snapshot.ObjectCount)
	}
	reencoded, err := encodeMembershipSnapshot(decoded)
	if err != nil {
		t.Fatalf("re-encode snapshot: %v", err)
	}
	if !bytes.Equal(reencoded, encoded) {
		t.Fatal("snapshot wire bytes changed after round trip")
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestClusterSessionMetadataRecordBridgeRejectsUnknownEnums(t *testing.T) {
	if _, err := sessionRetentionToRecord(membership.SessionRetention(99)); err == nil {
		t.Fatal("accepted unknown DTO retention")
	}
	if _, err := sessionRetentionFromRecord(records.SessionRetention(99)); err == nil {
		t.Fatal("accepted unknown record retention")
	}
	if _, err := sessionMetadataActionToRecord(membership.SessionMetadataAction(99)); err == nil {
		t.Fatal("accepted unknown DTO action")
	}
	if _, err := sessionMetadataActionFromRecord(records.SessionMetadataAction(99)); err == nil {
		t.Fatal("accepted unknown record action")
	}
	if _, err := sessionWillDispositionToRecord(membership.SessionWillDisposition(99)); err == nil {
		t.Fatal("accepted unknown DTO Will disposition")
	}
	if _, err := sessionWillDispositionFromRecord(records.SessionWillDisposition(99)); err == nil {
		t.Fatal("accepted unknown record Will disposition")
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestClusterOwnershipSessionConversionRejectsUnknownRetention(t *testing.T) {
	command := membership.CommandDTO{SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
		Ownership: &membership.OwnershipChangeDTO{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: membership.OwnershipActionAcquire,
			Session: &membership.SessionConnectDTO{ProtocolVersion: 5, ExpiryIntervalSeconds: 60, Retention: membership.SessionRetention(99)}}}
	if _, err := encodeMembershipCommand(command); err == nil {
		t.Fatal("encoded ownership session with unknown retention")
	}
	wire, err := proto.Marshal(&records.CommandEnvelope{
		SchemaMajor: membership.SchemaMajor, OperationId: bridgeOperation1,
		Payload: &records.CommandEnvelope_Ownership{Ownership: &records.OwnershipChange{
			TenantId: "tenant", ClientId: "client", OwnerNodeId: 1, Epoch: 1, Action: records.OwnershipAction_OWNERSHIP_ACTION_ACQUIRE,
			Session: &records.SessionConnect{ProtocolVersion: 5, ExpiryIntervalSeconds: 60, Retention: records.SessionRetention(99)},
		}},
	})
	if err != nil {
		t.Fatalf("marshal invalid command fixture: %v", err)
	}
	if _, err := decodeMembershipCommand(wire); err == nil {
		t.Fatal("decoded ownership session with unknown retention")
	}
}
