package main

import (
	"context"
	"testing"
	"time"

	"narumq/internal/adapters/clock"
	memoryqueue "narumq/internal/adapters/memory/deliveryqueue"
	"narumq/internal/adapters/memory/sessionstore"
	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

type recoveryWillPublisher struct{ published int }

func (publisher *recoveryWillPublisher) PublishWill(_ routing.SessionID, _ tenancy.SessionKey, _ mqtt.Will) error {
	publisher.published++
	return nil
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestClusterSessionRecoveryReclaimsLocalOwnerOnly(t *testing.T) {
	runtime, _, stop := newClusterDurableTestRuntime(t)
	defer stop()
	for _, member := range runtime.machine.Members() {
		commitClusterDurableTestCommand(t, runtime, membership.CommandDTO{MemberProfile: &membership.MemberProfileChangeDTO{MemberID: member.MemberID, ReleaseVersion: member.ReleaseVersion, Features: []string{membership.SessionMetadataFeature}}})
	}
	commitClusterDurableTestCommand(t, runtime, membership.CommandDTO{FeatureActivation: &membership.FeatureActivationDTO{Feature: membership.SessionMetadataFeature}})
	request := connect.SessionOwnershipRequestDTO{TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: runtime.config.NodeID, Session: membership.SessionConnectDTO{ProtocolVersion: 5, Retention: membership.SessionRetentionFinite, ExpiryIntervalSeconds: 30, Will: &membership.SessionWillDTO{Topic: "meta/will/handoff", Payload: []byte("published")}}}
	if _, err := runtime.AcquireSessionOwnership(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	epoch, err := runtime.AcquireOwnership(context.Background(), request.TenantID, request.ClientID, request.OwnerNodeID)
	if err != nil {
		t.Fatal(err)
	}
	request.ClientID = "remote"
	remoteEpoch, err := runtime.AcquireSessionOwnership(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	commitClusterDurableTestCommand(t, runtime, membership.CommandDTO{Ownership: &membership.OwnershipChangeDTO{TenantID: request.TenantID, ClientID: request.ClientID, OwnerNodeID: 2, ExpectedEpoch: remoteEpoch.Epoch, Epoch: remoteEpoch.Epoch + 1, Action: membership.OwnershipActionTransfer}})
	repository := sessionstore.New()
	runtime.localDeliveries, err = memoryqueue.New(64)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recoveryWillPublisher{}
	useCase, err := connect.New(clock.SystemClock{}, repository, runtime.localDeliveries, routing.NewTable(), clock.SystemScheduler{}, publisher, connect.WithOwnershipCoordinator(runtime, runtime.config.NodeID), connect.WithSessionMetadataCoordinator(runtime))
	if err != nil {
		t.Fatal(err)
	}
	defer useCase.Close()
	runtime.connectUseCase = useCase
	controller := &clusterSessionRecoveryController{runtime: runtime, keys: make(chan clusterSessionRecoveryKey, 1)}
	pending := map[clusterSessionRecoveryKey]struct{}{{tenantID: "tenant-a", clientID: "client-a"}: {}, {tenantID: "tenant-a", clientID: "remote"}: {}, {tenantID: "tenant-a", clientID: "missing"}: {}}
	controller.recoverPending(context.Background(), pending)
	if len(pending) != 0 {
		t.Fatalf("recovery pending=%v", pending)
	}
	local, found := runtime.machine.GetSessionMetadata("tenant-a", "client-a")
	if !found || local.Connected || local.Epoch != epoch || local.Will != nil || local.SessionExpiresAtUnixMillis <= uint64(time.Now().UnixMilli()) {
		t.Fatalf("recovered handoff=%+v found=%v", local, found)
	}
	remote, found := runtime.machine.GetSessionMetadata("tenant-a", "remote")
	if !found || !remote.Connected || remote.Will == nil {
		t.Fatalf("foreign owner metadata mutated=%+v found=%v", remote, found)
	}
	if publisher.published != 1 {
		t.Fatal("planned handoff did not publish Will exactly once")
	}
	before := local.DetachedAtUnixMillis
	pending[clusterSessionRecoveryKey{tenantID: "tenant-a", clientID: "client-a"}] = struct{}{}
	controller.recoverPending(context.Background(), pending)
	local, _ = runtime.machine.GetSessionMetadata("tenant-a", "client-a")
	if local.DetachedAtUnixMillis != before {
		t.Fatal("repeat recovery moved disconnect deadline")
	}
	if publisher.published != 1 {
		t.Fatal("planned handoff did not publish Will exactly once")
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestClusterSessionRecoveryQueueOverflowRequestsFullScan(t *testing.T) {
	controller := &clusterSessionRecoveryController{keys: make(chan clusterSessionRecoveryKey, 1)}
	controller.enqueue("tenant", "first")
	controller.enqueue("tenant", "second")
	if !controller.fullScan.Load() {
		t.Fatal("overflow lost recovery request instead of scanning")
	}
	key := <-controller.keys
	if key.clientID != "first" {
		t.Fatalf("queued key=%+v", key)
	}
}
