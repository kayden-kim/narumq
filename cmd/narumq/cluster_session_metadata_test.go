package main

import (
	"context"
	"errors"
	"testing"

	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
	"narumq/internal/domain/mqtt"
)

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestClusterSessionMetadataCommitRequiresActiveFeature(t *testing.T) {
	runtime, _, stop := newClusterDurableTestRuntime(t)
	defer stop()
	ctx := context.Background()
	request := connect.SessionOwnershipRequestDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: runtime.config.NodeID,
		Session: membership.SessionConnectDTO{
			ProtocolVersion: 5, Retention: membership.SessionRetentionInfinite,
			ExpiryIntervalSeconds: ^uint32(0),
			Will:                  &membership.SessionWillDTO{Topic: "events/will", Payload: []byte("offline"), QoS: 1},
		},
	}

	inactiveOutcome, err := runtime.CommitSessionMetadata(ctx, membership.SessionMetadataChangeDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: runtime.config.NodeID, Epoch: 1,
		Action: membership.SessionMetadataActionDiscard, WillDisposition: membership.SessionWillDispositionDiscard, ProposedAtUnixMillis: 1,
	})
	if err != nil || inactiveOutcome.Replicated {
		t.Fatalf("inactive CommitSessionMetadata() = (%+v, %v), want unreplicated success", inactiveOutcome, err)
	}
	for _, member := range runtime.machine.Members() {
		commitClusterDurableTestCommand(t, runtime, membership.CommandDTO{
			MemberProfile: &membership.MemberProfileChangeDTO{
				MemberID: member.MemberID, ReleaseVersion: member.ReleaseVersion, Features: []string{"compatibility-probe"},
			},
		})
	}
	commitClusterDurableTestCommand(t, runtime, membership.CommandDTO{
		FeatureActivation: &membership.FeatureActivationDTO{Feature: "compatibility-probe"},
	})
	unrelatedOutcome, err := runtime.CommitSessionMetadata(ctx, membership.SessionMetadataChangeDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: runtime.config.NodeID, Epoch: 1,
		Action: membership.SessionMetadataActionDiscard, WillDisposition: membership.SessionWillDispositionDiscard, ProposedAtUnixMillis: 1,
	})
	if err != nil || unrelatedOutcome.Replicated {
		t.Fatalf("unrelated active feature enabled metadata: outcome=%+v err=%v", unrelatedOutcome, err)
	}

	inactiveEpoch, err := runtime.AcquireSessionOwnership(ctx, request)
	if err != nil {
		t.Fatalf("inactive AcquireSessionOwnership() error = %v", err)
	}
	if _, found := runtime.machine.GetSessionMetadata(request.TenantID, request.ClientID); found {
		t.Fatal("inactive ownership acquisition installed session metadata")
	}
	ownership, found, err := runtime.GetOwnership(ctx, request.TenantID, request.ClientID)
	if err != nil || !found || ownership.Epoch != inactiveEpoch.Epoch || ownership.OwnerNodeID != runtime.config.NodeID {
		t.Fatalf("inactive acquisition ownership = (%+v, %v, %v), epoch=%d", ownership, found, err, inactiveEpoch.Epoch)
	}

	for _, member := range runtime.machine.Members() {
		commitClusterDurableTestCommand(t, runtime, membership.CommandDTO{
			MemberProfile: &membership.MemberProfileChangeDTO{
				MemberID: member.MemberID, ReleaseVersion: member.ReleaseVersion, Features: []string{"compatibility-probe", membership.SessionMetadataFeature},
			},
		})
	}
	commitClusterDurableTestCommand(t, runtime, membership.CommandDTO{
		FeatureActivation: &membership.FeatureActivationDTO{Feature: membership.SessionMetadataFeature},
	})
	activeEpoch, err := runtime.AcquireSessionOwnership(ctx, request)
	if err != nil {
		t.Fatalf("active AcquireSessionOwnership() error = %v", err)
	}
	state, found := runtime.machine.GetSessionMetadata(request.TenantID, request.ClientID)
	if !found || state.Epoch != activeEpoch.Epoch || !state.Connected || state.Will == nil {
		t.Fatalf("active ownership acquisition metadata = (%+v, %v), want connected metadata at epoch %d", state, found, activeEpoch.Epoch)
	}

	_, err = runtime.CommitSessionMetadata(ctx, membership.SessionMetadataChangeDTO{
		TenantID: request.TenantID, ClientID: request.ClientID, OwnerNodeID: runtime.config.NodeID,
		Epoch: inactiveEpoch.Epoch, Action: membership.SessionMetadataActionClaimWill, ProposedAtUnixMillis: 2,
	})
	if !errors.Is(err, connect.ErrSessionOwnershipStale) {
		t.Fatalf("stale CommitSessionMetadata() error = %v, want ErrSessionOwnershipStale", err)
	}

	claimed, err := runtime.CommitSessionMetadata(ctx, membership.SessionMetadataChangeDTO{
		TenantID: request.TenantID, ClientID: request.ClientID, OwnerNodeID: runtime.config.NodeID,
		Epoch: activeEpoch.Epoch, Action: membership.SessionMetadataActionDetach,
		WillDisposition: membership.SessionWillDispositionClaim, ProposedAtUnixMillis: 3,
	})
	if err != nil || !claimed.Replicated || claimed.Will == nil {
		t.Fatalf("committed Will claim = (%+v, %v), want one replicated Will", claimed, err)
	}
	wantWill := &mqtt.Will{Topic: "events/will", Payload: []byte("offline"), QoS: 1}
	if claimed.Will.Topic != wantWill.Topic || string(claimed.Will.Payload) != string(wantWill.Payload) || claimed.Will.QoS != wantWill.QoS {
		t.Fatalf("committed Will = %+v, want %+v", claimed.Will, wantWill)
	}

	repeated, err := runtime.CommitSessionMetadata(ctx, membership.SessionMetadataChangeDTO{
		TenantID: request.TenantID, ClientID: request.ClientID, OwnerNodeID: runtime.config.NodeID,
		Epoch: activeEpoch.Epoch, Action: membership.SessionMetadataActionClaimWill, ProposedAtUnixMillis: 4,
	})
	if err != nil || !repeated.Replicated || repeated.Will != nil {
		t.Fatalf("repeated committed Will claim = (%+v, %v), want replicated empty result", repeated, err)
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestClusterSessionViewCountsCommittedQueueAndExchanges(t *testing.T) {
	machine := membership.NewStateMachine()
	runtime := &clusterRuntime{machine: machine}
	if clients, active := runtime.ClusterSessions(); active || clients != nil {
		t.Fatal("inactive metadata replaced the local management view")
	}
	snapshot := membership.SnapshotDTO{
		SchemaMajor: 1, LastIncludedIndex: 1, ObjectCount: 10,
		ActiveFeatures: []string{membership.SessionMetadataFeature},
		Ownerships:     []membership.OwnershipStateDTO{{TenantID: "tenant", ClientID: "client", OwnerNodeID: 3, Epoch: 1, CommittedIndex: 1}},
		SessionMetadata: []membership.SessionMetadataStateDTO{{
			TenantID: "tenant", ClientID: "client", Epoch: 1, ProtocolVersion: 5,
			Retention: membership.SessionRetentionFinite, ExpiryIntervalSeconds: 6,
			DetachedAtUnixMillis: 100, SessionExpiresAtUnixMillis: 5000,
		}},
		OfflineSessions: []membership.OfflineSessionStateDTO{{
			TenantID: "tenant", ClientID: "client", Epoch: 1, NextSequence: 2,
			Messages: []membership.OfflineMessageDTO{{Sequence: 1, Topic: "meta/qos0"}},
		}},
		DeliverySessions: []membership.DeliverySessionStateDTO{{
			TenantID: "tenant", ClientID: "client", NextSequence: 3,
			Messages: []membership.DeliveryMessageDTO{
				{Sequence: 1, Topic: "meta/inflight", QoS: 1, PacketID: 9, Phase: membership.DeliveryPhaseAwaitPubAck},
				{Sequence: 2, Topic: "meta/queued", QoS: 1, Phase: membership.DeliveryPhaseQueued},
			},
			Inbound: []membership.DeliveryInboundDTO{
				{PacketID: 11, Topic: "meta/received"},
				{PacketID: 12, Topic: "meta/released", Released: true, ReleasedIndex: 1},
			},
		}},
	}
	if err := machine.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	clients, active := runtime.ClusterSessions()
	if !active || len(clients) != 1 {
		t.Fatalf("view=%+v active=%v", clients, active)
	}
	client := clients[0]
	if client.NodeID == nil || *client.NodeID != "3" || client.Protocol != "MQTT_5_0" ||
		client.Connected || !client.SessionPresent || client.ExpiryAt == nil || client.ExpiryAt.UnixMilli() != 5000 ||
		client.InflightCount != 2 || client.OfflineQueueCount != 2 {
		t.Fatalf("committed session view=%+v, want owner3/inflight2/queued2/deadline5000", client)
	}
}
