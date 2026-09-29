package main

import (
	"bytes"
	"reflect"
	"testing"

	"narumq/internal/application/membership"
)

const (
	bridgeOperation1 = "00000000-0000-4000-8000-000000000001"
	bridgeOperation4 = "00000000-0000-4000-8000-000000000004"
)

func TestMembershipRecordBridgeRoundTripsTypedCommands(t *testing.T) {
	member := membership.MemberIdentityDTO{
		MemberID: 4, ClusterID: "cluster", Address: "node4.example:19004", DNSSAN: "node4.example",
		CertificateSHA256: bytes.Repeat([]byte{0x11}, 32), ReleaseVersion: "v1",
		ClusterProtocolMajor: 2, StateSchemaMajor: 1,
		Phase: membership.MemberPhaseStaged, OperationID: bridgeOperation4,
	}
	commands := []membership.CommandDTO{
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
	}

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

func TestMembershipRecordBridgePreservesSnapshotIndexesAndDigest(t *testing.T) {
	member := membership.MemberIdentityDTO{
		MemberID: 4, ClusterID: "cluster", Address: "node4.example:19004", DNSSAN: "node4.example",
		CertificateSHA256: bytes.Repeat([]byte{0x11}, 32), ReleaseVersion: "v1",
		ClusterProtocolMajor: 2, StateSchemaMajor: 1,
		Phase: membership.MemberPhaseStaged, OperationID: bridgeOperation4,
	}
	snapshot := membership.SnapshotDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, LastIncludedIndex: 10,
		Members:       []membership.MemberIdentityDTO{member},
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
		TenantQuotaProfile: &membership.TenantQuotaProfileDTO{
			Version: 7, Default: membership.DefaultTenantQuotaProfile().Default,
			Tenants: []membership.TenantStorageQuotaDTO{{
				TenantID: "tenant-a", RetainedBytes: 128, OfflineQueueMessages: 64,
				OfflineQueueBytes: 4096, StorageBytes: 8192,
			}},
		},
		ObjectCount: 12,
	}
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
