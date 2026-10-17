package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"narumq/internal/adapters/raft/records"
	"narumq/internal/application/membership"
	"narumq/internal/domain/connrebalance"
	"narumq/internal/domain/nodedrain"
)

const (
	bridgeOperation1 = "00000000-0000-4000-8000-000000000001"
	bridgeOperation4 = "00000000-0000-4000-8000-000000000004"
)

func bridgeTypedCommandFixtures() []membership.CommandDTO {
	member := membership.MemberIdentityDTO{
		MemberID: 4, ClusterID: "cluster", Address: "node4.example:19004", DNSSAN: "node4.example",
		CertificateSHA256: bytes.Repeat([]byte{0x11}, 32), ReleaseVersion: "1.0.0",
		ClusterProtocolMajor: 2, StateSchemaMajor: 1,
		Phase: membership.MemberPhaseStaged, OperationID: bridgeOperation4,
		Features: []string{"compatibility-probe"},
	}
	return []membership.CommandDTO{
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation4,
			Membership: &membership.MembershipRecordDTO{
				OperationID: bridgeOperation4, Action: membership.MembershipActionStageJoin,
				IdempotencyKey: "key-4", Member: &member, ExpectedConfStateIndex: 12,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			Ownership: &membership.OwnershipChangeDTO{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
				Action: membership.OwnershipActionAcquire,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			Subscription: &membership.SubscriptionChangeDTO{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
				Filter: "sensors/#", QoS: 1, NoLocal: true, RetainAsPublished: true,
				RetainHandling: 2, Action: membership.SubscriptionActionAdd,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			Delivery: &membership.DeliveryChangeDTO{
				TenantID: "tenant", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1,
				Action: membership.DeliveryActionReceive, ProposedAtUnixMillis: 1000,
				Topic: "sensors/temp", Payload: []byte("x"), PacketID: 7, Retain: true,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation4,
			Delivery: &membership.DeliveryChangeDTO{
				TenantID: "tenant", ClientID: "publisher",
				Action: membership.DeliveryActionPublish, ProposedAtUnixMillis: 1000,
				Topic: "commands/reply", Payload: []byte("y"), PublishQoS: 2,
				Targets: []membership.DeliveryTargetDTO{{ClientID: "client", Retain: true}},
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			Retained: &membership.RetainedChangeDTO{
				TenantID: "tenant", Topic: "status", PublisherClientID: "publisher",
				QoS: 1, Payload: []byte("retained"), ExpiresAtUnixMillis: 2000,
				ProposedAtUnixMillis: 1000, Action: membership.RetainedActionReplace,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			Retained: &membership.RetainedChangeDTO{
				TenantID: "tenant", Topic: "status", PublisherClientID: "publisher",
				ProposedAtUnixMillis: 1000, Action: membership.RetainedActionDelete,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			Offline: &membership.OfflineChangeDTO{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
				Action: membership.OfflineActionDetach, SessionExpiresAtUnixMillis: 2000,
				ProposedAtUnixMillis: 1000,
				Messages: []membership.OfflineMessageDTO{{
					Sequence: 0, Topic: "status", Payload: []byte("val"), QoS: 0,
					ExpiresAtUnixMillis: 1500,
				}},
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			TenantQuota: &membership.TenantQuotaChangeDTO{Profile: membership.TenantQuotaProfileDTO{
				Version: 7, Default: membership.DefaultTenantQuotaProfile().Default,
				Tenants: []membership.TenantStorageQuotaDTO{{
					TenantID: "tenant-a", RetainedBytes: 128, OfflineQueueMessages: 64,
					OfflineQueueBytes: 4096, StorageBytes: 8192,
				}},
			}},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation4,
			MemberProfile: &membership.MemberProfileChangeDTO{
				MemberID: 4, ReleaseVersion: "1.0.0", Features: []string{"compatibility-probe"},
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			FeatureActivation: &membership.FeatureActivationDTO{Feature: "compatibility-probe"},
		},
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-099
func TestMembershipRecordBridgeRoundTripsTypedCommands(t *testing.T) {
	commands := append(bridgeTypedCommandFixtures(), bridgeExtendedTypedCommandFixtures()...)

	for index, command := range commands {
		wire, err := encodeMembershipCommand(command)
		if err != nil {
			t.Fatalf("encode command %d: %v", index, err)
		}
		decoded, err := decodeMembershipCommand(wire)
		if err != nil {
			t.Fatalf("decode command %d: %v", index, err)
		}
		if !reflect.DeepEqual(decoded, command) {
			t.Fatalf("command %d round trip = %#v, want %#v", index, decoded, command)
		}
		reencoded, err := encodeMembershipCommand(decoded)
		if err != nil {
			t.Fatalf("re-encode command %d: %v", index, err)
		}
		if !bytes.Equal(reencoded, wire) {
			t.Fatalf("command %d encoding changed after decode", index)
		}
	}

	invalid := membership.CommandDTO{SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1}
	if _, err := encodeMembershipCommand(invalid); err == nil {
		t.Fatal("accepted command with no payload")
	}
}
func TestMembershipRecordBridgeRejectsUnsupportedCommandEnums(t *testing.T) {
	validMember := bridgeTypedCommandFixtures()[0].Membership.Member
	tests := []struct {
		name    string
		command membership.CommandDTO
	}{
		{
			name: "node drain action",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				NodeDrain: &membership.NodeDrainChangeDTO{Action: 0},
			},
		},
		{
			name: "node drain phase",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				NodeDrain: &membership.NodeDrainChangeDTO{Action: membership.NodeDrainActionStart, Phase: nodedrain.Phase(255)},
			},
		},
		{
			name: "retained action",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Retained: &membership.RetainedChangeDTO{Action: 0},
			},
		},
		{
			name: "session metadata action",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				SessionMetadata: &membership.SessionMetadataChangeDTO{Action: membership.SessionMetadataActionUnspecified},
			},
		},
		{
			name: "session will disposition",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				SessionMetadata: &membership.SessionMetadataChangeDTO{
					Action: membership.SessionMetadataActionDetach, WillDisposition: membership.SessionWillDisposition(255),
				},
			},
		},
		{
			name: "connection rebalance action",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				ConnectionRebalance: &membership.ConnectionRebalanceChangeDTO{Action: membership.ConnectionRebalanceActionUnspecified},
			},
		},
		{
			name: "membership action",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Membership: &membership.MembershipRecordDTO{Action: membership.MembershipActionUnspecified, Member: validMember},
			},
		},
		{
			name: "ownership action",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Ownership: &membership.OwnershipChangeDTO{Action: membership.OwnershipActionUnspecified},
			},
		},
		{
			name: "subscription action",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Subscription: &membership.SubscriptionChangeDTO{Action: membership.SubscriptionActionUnspecified},
			},
		},
		{
			name: "delivery action",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Delivery: &membership.DeliveryChangeDTO{Action: membership.DeliveryActionUnspecified},
			},
		},
		{
			name: "offline action",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Offline: &membership.OfflineChangeDTO{Action: membership.OfflineActionUnspecified},
			},
		},
		{
			name: "membership action above supported range",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Membership: &membership.MembershipRecordDTO{Action: membership.MembershipAction(255), Member: validMember},
			},
		},
		{
			name: "ownership action above supported range",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Ownership: &membership.OwnershipChangeDTO{Action: membership.OwnershipAction(255)},
			},
		},
		{
			name: "subscription action above supported range",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Subscription: &membership.SubscriptionChangeDTO{Action: membership.SubscriptionAction(255)},
			},
		},
		{
			name: "delivery action above supported range",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Delivery: &membership.DeliveryChangeDTO{Action: membership.DeliveryAction(255)},
			},
		},
		{
			name: "retained action above supported range",
			command: membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
				Retained: &membership.RetainedChangeDTO{Action: membership.RetainedAction(255)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if wire, err := encodeMembershipCommand(test.command); err == nil || wire != nil {
				t.Fatalf("unsupported enum encoded as %#v with error %v", wire, err)
			}
		})
	}
}

func bridgeSnapshotFixture() membership.SnapshotDTO {
	member := membership.MemberIdentityDTO{
		MemberID: 4, ClusterID: "cluster", Address: "node4.example:19004", DNSSAN: "node4.example",
		CertificateSHA256: bytes.Repeat([]byte{0x11}, 32), ReleaseVersion: "1.0.0",
		ClusterProtocolMajor: 2, StateSchemaMajor: 1,
		Phase: membership.MemberPhaseStaged, OperationID: bridgeOperation4,
		Features: []string{"compatibility-probe"},
	}
	return membership.SnapshotDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, LastIncludedIndex: 10,
		Members: []membership.MemberIdentityDTO{member}, ActiveFeatures: []string{"compatibility-probe"},
		Ownerships:    []membership.OwnershipStateDTO{{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, CommittedIndex: 8}},
		Subscriptions: []membership.SubscriptionStateDTO{{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Filter: "sensors/#", QoS: 1}},
		DeliverySessions: []membership.DeliverySessionStateDTO{{
			TenantID: "tenant", ClientID: "client", NextSequence: 2,
			Messages: []membership.DeliveryMessageDTO{{
				Sequence: 1, Topic: "sensors/temp", Payload: []byte("z"), QoS: 1,
				PacketID: 9, Phase: membership.DeliveryPhaseAwaitPubAck,
			}},
			Inbound: []membership.DeliveryInboundDTO{{
				PacketID: 7, Topic: "sensors/temp", Payload: []byte("inbound"), Retain: true,
			}},
		}},
		MembershipHistory: []membership.MembershipRecordDTO{{
			OperationID: bridgeOperation4, Action: membership.MembershipActionStageJoin,
			IdempotencyKey: "key-4", Member: &member, ExpectedConfStateIndex: 12, CommittedIndex: 9,
		}},
		Retained: []membership.RetainedStateDTO{{
			TenantID: "tenant", Topic: "status", PublisherClientID: "publisher",
			QoS: 1, Payload: []byte("retained"), ExpiresAtUnixMillis: 2000,
		}},
		OfflineSessions: []membership.OfflineSessionStateDTO{{
			TenantID: "tenant", ClientID: "client", Epoch: 1, NextSequence: 2,
			Messages: []membership.OfflineMessageDTO{{
				Sequence: 1, Topic: "status", Payload: []byte("val"), QoS: 0,
			}},
		}},
		NodeDrains: []membership.NodeDrainStateDTO{{
			DrainOperationID: bridgeOperation4, MemberID: 4, IdempotencyKey: "drain-4",
			Phase: nodedrain.PhaseExcludingTraffic, ServerReference: "lb.example:1883",
			ConnectionRatePerSecond: 5, LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 3000,
			StartedAtUnixMillis: 1000, DeadlineAtUnixMillis: 4000, PhaseStartedAtUnixMillis: 1000,
			Progress:       membership.NodeDrainProgressDTO{ConnectionsInitial: 8, ConnectionsRemaining: 3, PendingWAL: 2},
			CommittedIndex: 9,
		}},
		TenantQuotaProfile: &membership.TenantQuotaProfileDTO{
			Version: 7, Default: membership.DefaultTenantQuotaProfile().Default,
			Tenants: []membership.TenantStorageQuotaDTO{{
				TenantID: "tenant-a", RetainedBytes: 128, OfflineQueueMessages: 64,
				OfflineQueueBytes: 4096, StorageBytes: 8192,
			}},
		},
		ObjectCount: 14,
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-099
func TestMembershipRecordBridgePreservesSnapshotIndexesAndDigest(t *testing.T) {
	snapshot := bridgeSnapshotFixture()
	wire, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	decoded, err := decodeMembershipSnapshot(wire)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if got := decoded.MembershipHistory[0].CommittedIndex; got != 9 {
		t.Fatalf("restored membership transition index = %d, want 9", got)
	}
	if decoded.ObjectCount != snapshot.ObjectCount || len(decoded.Digest) != 32 {
		t.Fatalf("restored snapshot metadata = count %d digest bytes %d", decoded.ObjectCount, len(decoded.Digest))
	}
	want := snapshot
	want.Digest = decoded.Digest
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("snapshot round trip = %#v, want %#v", decoded, want)
	}
	reencoded, err := encodeMembershipSnapshot(decoded)
	if err != nil {
		t.Fatalf("re-encode snapshot: %v", err)
	}
	if !bytes.Equal(reencoded, wire) {
		t.Fatal("snapshot encoding changed after decode")
	}

	invalid := snapshot
	invalid.ObjectCount++
	if _, err := encodeMembershipSnapshot(invalid); err == nil {
		t.Fatal("accepted mismatched snapshot object count")
	}
}

// Traceability: CLU-11/CLU-14; docs/specs/cluster-session-and-delivery.md §10, §11.19.
func TestMembershipRecordBridgeRejectsUnsupportedSnapshotPhases(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*membership.SnapshotDTO)
		wantError string
	}{
		{
			name: "member phase",
			mutate: func(snapshot *membership.SnapshotDTO) {
				snapshot.Members = append([]membership.MemberIdentityDTO(nil), snapshot.Members...)
				snapshot.Members[0].Phase = membership.MemberPhase(255)
			},
			wantError: "unsupported member phase",
		},
		{
			name: "delivery phase",
			mutate: func(snapshot *membership.SnapshotDTO) {
				snapshot.DeliverySessions = append([]membership.DeliverySessionStateDTO(nil), snapshot.DeliverySessions...)
				snapshot.DeliverySessions[0].Messages = append([]membership.DeliveryMessageDTO(nil), snapshot.DeliverySessions[0].Messages...)
				snapshot.DeliverySessions[0].Messages[0].Phase = membership.DeliveryPhase(255)
			},
			wantError: "unsupported delivery phase",
		},
		{
			name: "node drain phase",
			mutate: func(snapshot *membership.SnapshotDTO) {
				snapshot.NodeDrains = append([]membership.NodeDrainStateDTO(nil), snapshot.NodeDrains...)
				snapshot.NodeDrains[0].Phase = nodedrain.Phase(255)
			},
			wantError: "unsupported node drain phase",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := bridgeSnapshotFixture()
			test.mutate(&snapshot)
			if _, err := encodeMembershipSnapshot(snapshot); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("encode snapshot error = %v, want %q", err, test.wantError)
			}
		})
	}
}
func TestMembershipRecordBridgeAcceptsUnspecifiedNodeDrainCommandPhase(t *testing.T) {
	command := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
		NodeDrain: &membership.NodeDrainChangeDTO{
			Action: membership.NodeDrainActionStart, MemberID: 1, DrainOperationID: bridgeOperation1,
			IdempotencyKey: "drain-1", ServerReference: "lb.example:1883", ConnectionRatePerSecond: 2,
			LoadBalancerPropagationDelayMillis: 100, TimeoutMillis: 1000, ProposedAtUnixMillis: 100,
		},
	}
	wire, err := encodeMembershipCommand(command)
	if err != nil {
		t.Fatalf("encode unspecified-phase node drain command: %v", err)
	}
	decoded, err := decodeMembershipCommand(wire)
	if err != nil {
		t.Fatalf("decode unspecified-phase node drain command: %v", err)
	}
	if !reflect.DeepEqual(decoded, command) {
		t.Fatalf("node drain command round trip = %#v, want %#v", decoded, command)
	}
}

func bridgeExtendedTypedCommandFixtures() []membership.CommandDTO {
	return []membership.CommandDTO{
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			Offline: &membership.OfflineChangeDTO{
				TenantID: "tenant", ClientID: "client", Epoch: 1, Action: membership.OfflineActionEnqueue,
				ProposedAtUnixMillis: 1000,
				Messages:             []membership.OfflineMessageDTO{{Sequence: 0, Topic: "status", Payload: []byte("val"), QoS: 0}},
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			Offline: &membership.OfflineChangeDTO{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
				Action: membership.OfflineActionClaim, ProposedAtUnixMillis: 1000,
				Messages: []membership.OfflineMessageDTO{},
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			SessionMetadata: &membership.SessionMetadataChangeDTO{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
				Action: membership.SessionMetadataActionDetach, WillDisposition: membership.SessionWillDispositionDiscard,
				ProposedAtUnixMillis: 1000,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			SessionMetadata: &membership.SessionMetadataChangeDTO{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
				Action: membership.SessionMetadataActionClaimWill, WillDisposition: membership.SessionWillDispositionUnspecified,
				ProposedAtUnixMillis: 1000,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			SessionMetadata: &membership.SessionMetadataChangeDTO{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
				Action: membership.SessionMetadataActionDiscard, WillDisposition: membership.SessionWillDispositionClaim,
				ProposedAtUnixMillis: 1000,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			SessionMetadata: &membership.SessionMetadataChangeDTO{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
				Action: membership.SessionMetadataActionDetach, WillDisposition: membership.SessionWillDispositionSchedule,
				ProposedAtUnixMillis: 1000, WillDueAtUnixMillis: 2000,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			ConnectionRebalance: &membership.ConnectionRebalanceChangeDTO{
				Action: membership.ConnectionRebalanceActionStart, RebalanceOperationID: bridgeOperation1,
				IdempotencyKey: "rebalance-1", ServerReference: "lb.example:1883",
				MaxDeviationConnections: 2, ConnectionRatePerSecond: 5,
				LoadBalancerPropagationDelayMillis: 250, TimeoutMillis: 3000,
				ProposedAtUnixMillis: 1000,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			ConnectionRebalance: &membership.ConnectionRebalanceChangeDTO{
				Action: membership.ConnectionRebalanceActionReport, RebalanceOperationID: bridgeOperation1,
				NodeID: 4, Connections: 7, SessionsTransferred: 3,
				ProposedAtUnixMillis: 1000,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			ConnectionRebalance: &membership.ConnectionRebalanceChangeDTO{
				Action: membership.ConnectionRebalanceActionFail, RebalanceOperationID: bridgeOperation1,
				FailureCode:          "capacity",
				ProposedAtUnixMillis: 1000,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation1,
			ConnectionRebalance: &membership.ConnectionRebalanceChangeDTO{
				Action: membership.ConnectionRebalanceActionCancel, RebalanceOperationID: bridgeOperation1,
				ProposedAtUnixMillis: 1000,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation4,
			NodeDrain: &membership.NodeDrainChangeDTO{
				Action: membership.NodeDrainActionStart, MemberID: 4, DrainOperationID: bridgeOperation4,
				IdempotencyKey: "drain-4", ServerReference: "lb.example:1883",
				ConnectionRatePerSecond: 5, LoadBalancerPropagationDelayMillis: 250,
				TimeoutMillis: 3000, ProposedAtUnixMillis: 1000,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation4,
			NodeDrain: &membership.NodeDrainChangeDTO{
				Action: membership.NodeDrainActionAdvance, MemberID: 4, DrainOperationID: bridgeOperation4,
				Phase: nodedrain.PhaseExcludingTraffic, ProposedAtUnixMillis: 1001,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation4,
			NodeDrain: &membership.NodeDrainChangeDTO{
				Action: membership.NodeDrainActionComplete, MemberID: 4, DrainOperationID: bridgeOperation4,
				ProposedAtUnixMillis: 1002,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation4,
			NodeDrain: &membership.NodeDrainChangeDTO{
				Action: membership.NodeDrainActionFail, MemberID: 4, DrainOperationID: bridgeOperation4,
				FailureCode: "drain_timeout", ProposedAtUnixMillis: 1003,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation4,
			NodeDrain: &membership.NodeDrainChangeDTO{
				Action: membership.NodeDrainActionCancel, MemberID: 4, DrainOperationID: bridgeOperation4,
				ProposedAtUnixMillis: 1004,
			},
		},
		{
			SchemaMajor: membership.SchemaMajor, OperationID: bridgeOperation4,
			NodeDrain: &membership.NodeDrainChangeDTO{
				Action: membership.NodeDrainActionClear, MemberID: 4, DrainOperationID: bridgeOperation4,
				ProposedAtUnixMillis: 1005,
			},
		},
	}
}

func TestMembershipRecordBridgeRejectsInvalidRecordConversions(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{
			name: "unsupported retained action from record",
			run: func() error {
				_, err := retainedActionFromRecord(0)
				return err
			},
			want: "unsupported retained action 0",
		},
		{
			name: "unsupported delivery action from record",
			run: func() error {
				_, err := deliveryActionFromRecord(0)
				return err
			},
			want: "unsupported delivery action 0",
		},
		{
			name: "unsupported rebalance action from record",
			run: func() error {
				_, err := rebalanceActionFromRecord(0)
				return err
			},
			want: "unsupported connection rebalance action 0",
		},
		{
			name: "missing rebalance state",
			run: func() error {
				_, err := rebalanceStateFromRecord(nil)
				return err
			},
			want: "connection rebalance state is required",
		},
		{
			name: "invalid rebalance phase",
			run: func() error {
				_, err := rebalanceStateFromRecord(&records.ConnectionRebalanceState{})
				return err
			},
			want: "unsupported connection rebalance phase",
		},
		{
			name: "unsupported node drain action from record",
			run: func() error {
				_, err := nodeDrainActionFromRecord(0)
				return err
			},
			want: "unsupported node drain action 0",
		},
		{
			name: "unspecified node drain phase is rejected in snapshot",
			run: func() error {
				_, err := drainPhaseFromRecord(records.NodeDrainPhase_NODE_DRAIN_PHASE_UNSPECIFIED, false)
				return err
			},
			want: "unsupported node drain phase 0",
		},
		{
			name: "nil delivery change",
			run: func() error {
				_, err := deliveryChangeFromRecord(nil)
				return err
			},
			want: "delivery change record is nil",
		},
		{
			name: "unsupported delivery phase",
			run: func() error {
				_, err := deliverySessionFromRecord(&records.DeliverySessionState{
					Messages: []*records.DeliveryMessage{{}},
				})
				return err
			},
			want: "unsupported delivery phase 0",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("conversion error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestMembershipRecordBridgeAcceptsValidRecordConversions(t *testing.T) {
	phase := records.ConnectionRebalancePhase(1)
	state, err := rebalanceStateFromRecord(&records.ConnectionRebalanceState{
		Phase: phase,
		Participants: []*records.ConnectionRebalanceReport{{
			NodeId: 3, Connections: 8, SessionsTransferred: 2, Shedding: true, ReportedAtUnixMillis: 90,
		}},
	})
	if err != nil {
		t.Fatalf("decode valid rebalance state: %v", err)
	}
	want := membership.ConnectionRebalanceStateDTO{
		Phase: connrebalance.Phase(phase),
		Participants: []membership.ConnectionRebalanceReportDTO{{
			NodeID: 3, Connections: 8, SessionsTransferred: 2, Shedding: true, ReportedAtUnixMillis: 90,
		}},
	}
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("rebalance state conversion = %#v, want %#v", state, want)
	}

	if got, err := drainPhaseFromRecord(records.NodeDrainPhase_NODE_DRAIN_PHASE_UNSPECIFIED, true); err != nil || got != nodedrain.PhaseUnspecified {
		t.Fatalf("unspecified command drain phase = %v, %v; want unspecified, nil", got, err)
	}
	if got := nodeDrainProgressFromRecord(nil); !reflect.DeepEqual(got, membership.NodeDrainProgressDTO{}) {
		t.Fatalf("nil drain progress = %#v, want zero progress", got)
	}
}
