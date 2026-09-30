package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	clockadapter "narumq/internal/adapters/clock"
	memorydeliveryqueue "narumq/internal/adapters/memory/deliveryqueue"
	"narumq/internal/application/membership"
	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

type retryingClusterDurableTestSink struct {
	failures   int
	deliveries []applicationdeliveryqueue.DeliveryDTO
}

func (sink *retryingClusterDurableTestSink) DeliverDurable(delivery applicationdeliveryqueue.DeliveryDTO) error {
	if sink.failures > 0 {
		sink.failures--
		return errors.New("local durable queue unavailable")
	}
	sink.deliveries = append(sink.deliveries, delivery)
	return nil
}

type clusterDurableTestSink struct {
	deliveries []applicationdeliveryqueue.DeliveryDTO
	err        error
}

func (sink *clusterDurableTestSink) DeliverDurable(delivery applicationdeliveryqueue.DeliveryDTO) error {
	sink.deliveries = append(sink.deliveries, delivery)
	return sink.err
}

func newClusterDurableTestRuntime(t *testing.T) (*clusterRuntime, routing.SessionID, func()) {
	t.Helper()
	config := validBootstrapClusterConfig()
	runtime, stop := newTestClusterOfflineRuntime(t, config)
	sessionID, err := clusterSessionID("tenant-a", "client-a")
	if err != nil {
		stop()
		t.Fatal(err)
	}
	return runtime, sessionID, stop
}

func commitClusterDurableTestCommand(t *testing.T, runtime *clusterRuntime, command membership.CommandDTO) membership.ApplyResultDTO {
	t.Helper()
	operationID, err := newClusterOperationID()
	if err != nil {
		t.Fatal(err)
	}
	command.SchemaMajor = membership.SchemaMajor
	command.SchemaMinor = membership.SchemaMinor
	command.OperationID = operationID
	result, err := runtime.Commit(context.Background(), command)
	if err != nil {
		t.Fatalf("commit test command: %v", err)
	}
	if result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("test command outcome=%v conflict=%v, want applied", result.Outcome, result.Conflict)
	}
	return result
}

func seedClusterDurableDelivery(t *testing.T, runtime *clusterRuntime) (routing.SessionID, membership.DeliveryEnqueuedDTO) {
	t.Helper()
	sessionID, err := clusterSessionID("tenant-a", "client-a")
	if err != nil {
		t.Fatal(err)
	}
	commitClusterDurableTestCommand(t, runtime, membership.CommandDTO{
		Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: runtime.config.NodeID,
			Epoch: 1, Filter: "events/#", QoS: 2, Action: membership.SubscriptionActionAdd,
		},
	})
	result, err := runtime.CommitDelivery(context.Background(), membership.DeliveryChangeDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: runtime.config.NodeID,
		Epoch: 1, Action: membership.DeliveryActionAttach, ProposedAtUnixMillis: 1_700_000_000_000,
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("commit durable attach: result=%+v err=%v", result, err)
	}
	result, err = runtime.CommitDelivery(context.Background(), membership.DeliveryChangeDTO{
		TenantID: "tenant-a", ClientID: "publisher-a", Action: membership.DeliveryActionPublish,
		ProposedAtUnixMillis: 1_700_000_000_001, Topic: "events/one", Payload: []byte("first"),
		PublishQoS: 2, Targets: []membership.DeliveryTargetDTO{{ClientID: "client-a"}},
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied || result.Delivery == nil || len(result.Delivery.Enqueued) != 1 {
		t.Fatalf("commit durable publish: result=%+v err=%v", result, err)
	}
	return sessionID, result.Delivery.Enqueued[0]
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterDurableActivationProjectsCommittedQueueOncePerSequence(t *testing.T) {
	runtime, sessionID, stop := newClusterDurableTestRuntime(t)
	defer stop()
	sink := &clusterDurableTestSink{}
	runtime.durableSink = sink
	_, first := seedClusterDurableDelivery(t, runtime)

	if err := runtime.ActivateDelivery(sessionID, 1, 0); err != nil {
		t.Fatalf("activate durable session: %v", err)
	}
	if len(sink.deliveries) != 1 {
		t.Fatalf("activation projected %d deliveries, want 1", len(sink.deliveries))
	}
	projected := sink.deliveries[0]
	if projected.DurableSequence != first.Sequence || projected.DurableEpoch != 1 || projected.QoS != mqtt.QoSExactlyOnce || projected.Topic != "events/one" || string(projected.Payload) != "first" {
		t.Fatalf("activation projected unexpected delivery: %+v", projected)
	}

	runtime.notifyDurable("tenant-a", []membership.DeliveryEnqueuedDTO{first})
	if len(sink.deliveries) != 1 {
		t.Fatalf("duplicate notification projected %d deliveries, want 1", len(sink.deliveries))
	}
	secondResult, err := runtime.CommitDelivery(context.Background(), membership.DeliveryChangeDTO{
		TenantID: "tenant-a", ClientID: "publisher-a", Action: membership.DeliveryActionPublish,
		ProposedAtUnixMillis: 1_700_000_000_002, Topic: "events/two", Payload: []byte("second"),
		PublishQoS: 2, Targets: []membership.DeliveryTargetDTO{{ClientID: "client-a"}},
	})
	if err != nil || secondResult.Delivery == nil || len(secondResult.Delivery.Enqueued) != 1 {
		t.Fatalf("commit second durable publish: result=%+v err=%v", secondResult, err)
	}
	runtime.notifyDurable("tenant-a", secondResult.Delivery.Enqueued)
	if len(sink.deliveries) != 2 || sink.deliveries[1].DurableSequence != first.Sequence+1 || string(sink.deliveries[1].Payload) != "second" {
		t.Fatalf("next committed sequence was not projected exactly once: %+v", sink.deliveries)
	}
	if err := runtime.ActivateDelivery(sessionID, 1, first.Sequence+1); err != nil {
		t.Fatalf("reactivate through loaded sequence: %v", err)
	}
	if len(sink.deliveries) != 2 {
		t.Fatalf("activation replayed already loaded delivery: %+v", sink.deliveries)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterDurableProjectionRetriesCommittedQueueAfterReconnect(t *testing.T) {
	runtime, sessionID, stop := newClusterDurableTestRuntime(t)
	defer stop()
	sink := &retryingClusterDurableTestSink{failures: 1}
	runtime.durableSink = sink
	_, enqueued := seedClusterDurableDelivery(t, runtime)

	if err := runtime.ActivateDelivery(sessionID, 1, 0); err != nil {
		t.Fatalf("activate after durable publish: %v", err)
	}
	if len(sink.deliveries) != 0 {
		t.Fatalf("failed local projection was delivered: %+v", sink.deliveries)
	}
	message, exists := runtime.machine.DeliveryMessage("tenant-a", "client-a", enqueued.Sequence)
	if !exists || message.Phase != membership.DeliveryPhaseQueued {
		t.Fatalf("failed projection changed committed message: %+v exists=%t", message, exists)
	}

	runtime.DeactivateDelivery(sessionID, 1)
	if err := runtime.ActivateDelivery(sessionID, 1, 0); err != nil {
		t.Fatalf("reactivate committed queue after reconnect: %v", err)
	}
	if len(sink.deliveries) != 1 || sink.deliveries[0].DurableSequence != enqueued.Sequence ||
		string(sink.deliveries[0].Payload) != "first" {
		t.Fatalf("reconnect did not reproject committed delivery: %+v", sink.deliveries)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterDurableProjectionRejectsRemoteAndStaleAttachments(t *testing.T) {
	runtime, sessionID, stop := newClusterDurableTestRuntime(t)
	defer stop()
	sink := &clusterDurableTestSink{}
	runtime.durableSink = sink
	_, enqueued := seedClusterDurableDelivery(t, runtime)
	runtime.durableAttachments = map[routing.SessionID]clusterDurableAttachment{
		sessionID: {epoch: 1},
	}

	remoteNodeID := runtime.config.NodeID + 1
	runtime.config.NodeID = remoteNodeID
	runtime.notifyDurable("tenant-a", []membership.DeliveryEnqueuedDTO{enqueued})
	if len(sink.deliveries) != 0 {
		t.Fatalf("remote owner projected %d local deliveries", len(sink.deliveries))
	}
	if runtime.durableAttachments[sessionID].loadedThrough != 0 {
		t.Fatal("remote notification advanced the local attachment sequence")
	}
	if err := runtime.ActivateDelivery(sessionID, 1, 0); !errors.Is(err, errClusterDurableStale) {
		t.Fatalf("remote activation error=%v, want stale owner", err)
	}

	runtime.removeStaleDurableAttachment(sessionID, membership.OwnershipStateDTO{OwnerNodeID: remoteNodeID, Epoch: 2})
	if _, exists := runtime.durableAttachments[sessionID]; exists {
		t.Fatal("stale ownership epoch retained a local attachment")
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterDurableQueueCommitsQoS2TransitionsBeforeLocalMutation(t *testing.T) {
	runtime, sessionID, stop := newClusterDurableTestRuntime(t)
	defer stop()
	_, enqueued := seedClusterDurableDelivery(t, runtime)
	queue, err := memorydeliveryqueue.New(8)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1_700_000_000_100)
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "events/one", Payload: []byte("first"), QoS: mqtt.QoSExactlyOnce,
		DurableSequence: enqueued.Sequence, DurableEpoch: 1,
	}, now); err != nil {
		t.Fatal(err)
	}
	decorator := &clusterDurableQueue{Queue: queue, runtime: runtime, clock: clockadapter.SystemClock{}}
	if err := decorator.CompleteOutbound(sessionID, 99); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("unknown outbound completion error=%v, want unavailable", err)
	}
	message, exists := runtime.machine.DeliveryMessage("tenant-a", "client-a", enqueued.Sequence)
	if !exists || message.Phase != membership.DeliveryPhaseQueued {
		t.Fatalf("unknown outbound completion changed durable state: %+v exists=%t", message, exists)
	}
	outbound := applicationdeliveryqueue.OutboundDTO{
		SessionID: sessionID, PacketID: 17, DurableSequence: enqueued.Sequence, DurableEpoch: 1,
	}
	if err := decorator.PromoteHead(outbound, now); err != nil {
		t.Fatalf("promote durable QoS 2 head: %v", err)
	}
	message, exists = runtime.machine.DeliveryMessage("tenant-a", "client-a", enqueued.Sequence)
	if !exists || message.PacketID != 17 || message.Phase != membership.DeliveryPhaseAwaitPubRec {
		t.Fatalf("SEND was not committed before promotion: message=%+v exists=%v", message, exists)
	}

	if err := decorator.ReplaceOutbound(outbound); err != nil {
		t.Fatalf("commit QoS 2 ADVANCE: %v", err)
	}
	message, exists = runtime.machine.DeliveryMessage("tenant-a", "client-a", enqueued.Sequence)
	if !exists || message.Phase != membership.DeliveryPhaseAwaitPubComp {
		t.Fatalf("ADVANCE was not committed: message=%+v exists=%v", message, exists)
	}
	if err := decorator.CompleteOutbound(sessionID, outbound.PacketID); err != nil {
		t.Fatalf("commit QoS 2 COMPLETE: %v", err)
	}
	state, exists := runtime.machine.DeliverySession("tenant-a", "client-a")
	if !exists || len(state.Messages) != 0 {
		t.Fatalf("COMPLETE did not clear the replicated queue item: state=%+v exists=%v", state, exists)
	}
	queuedOutbound, err := queue.ListOutbound(sessionID)
	if err != nil || len(queuedOutbound) != 0 {
		t.Fatalf("local outbound state after COMPLETE=%+v err=%v", queuedOutbound, err)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterDurableQueueDropsMissingCommittedHeadAndFencesStaleEpoch(t *testing.T) {
	runtime, sessionID, stop := newClusterDurableTestRuntime(t)
	defer stop()
	queue, err := memorydeliveryqueue.New(8)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1_700_000_000_100)
	missing := applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "events/missing", Payload: []byte("stale"), QoS: mqtt.QoSAtLeastOnce,
		DurableSequence: 99, DurableEpoch: 1,
	}
	if err := queue.TryEnqueue(missing, now); err != nil {
		t.Fatal(err)
	}
	decorator := &clusterDurableQueue{Queue: queue, runtime: runtime, clock: clockadapter.SystemClock{}}
	if err := decorator.PromoteHead(applicationdeliveryqueue.OutboundDTO{
		SessionID: sessionID, PacketID: 9, DurableSequence: 99, DurableEpoch: 1,
	}, now); !errors.Is(err, applicationdeliveryqueue.ErrExpired) {
		t.Fatalf("missing durable head promotion error=%v, want expired", err)
	}
	remaining, err := queue.ListQueued(sessionID, now)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("missing committed head remained queued: %+v err=%v", remaining, err)
	}

	_, enqueued := seedClusterDurableDelivery(t, runtime)
	stale := applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "events/one", Payload: []byte("first"), QoS: mqtt.QoSExactlyOnce,
		DurableSequence: enqueued.Sequence, DurableEpoch: 2,
	}
	if err := queue.TryEnqueue(stale, now); err != nil {
		t.Fatal(err)
	}
	if err := decorator.PromoteHead(applicationdeliveryqueue.OutboundDTO{
		SessionID: sessionID, PacketID: 18, DurableSequence: enqueued.Sequence, DurableEpoch: 2,
	}, now); !errors.Is(err, errClusterDurableStale) {
		t.Fatalf("stale durable epoch promotion error=%v, want stale owner", err)
	}
	remaining, err = queue.ListQueued(sessionID, now)
	if err != nil || len(remaining) != 1 || remaining[0].DurableEpoch != 2 {
		t.Fatalf("stale transition mutated local queue: %+v err=%v", remaining, err)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterDurableDeactivationFencesStaleAndDisconnectedOwners(t *testing.T) {
	runtime, sessionID, stop := newClusterDurableTestRuntime(t)
	defer stop()
	sink := &clusterDurableTestSink{}
	runtime.durableSink = sink
	_, first := seedClusterDurableDelivery(t, runtime)
	if err := runtime.ActivateDelivery(sessionID, 1, 0); err != nil {
		t.Fatalf("activate durable session: %v", err)
	}
	if len(sink.deliveries) != 1 {
		t.Fatalf("initial projection count=%d, want 1", len(sink.deliveries))
	}

	runtime.DeactivateDelivery(sessionID, 2)
	for _, payload := range []string{"still-attached", "detached"} {
		result, err := runtime.CommitDelivery(context.Background(), membership.DeliveryChangeDTO{
			TenantID: "tenant-a", ClientID: "publisher-a", Action: membership.DeliveryActionPublish,
			ProposedAtUnixMillis: uint64(1_700_000_000_002 + len(sink.deliveries)),
			Topic:                "events/next", Payload: []byte(payload), PublishQoS: 1,
			Targets: []membership.DeliveryTargetDTO{{ClientID: "client-a"}},
		})
		if err != nil || result.Delivery == nil || len(result.Delivery.Enqueued) != 1 {
			t.Fatalf("commit %q: result=%+v err=%v", payload, result, err)
		}
		if payload == "detached" {
			runtime.DeactivateDelivery(sessionID, 1)
		}
		runtime.notifyDurable("tenant-a", result.Delivery.Enqueued)
	}
	if len(sink.deliveries) != 2 || sink.deliveries[1].DurableSequence != first.Sequence+1 ||
		string(sink.deliveries[1].Payload) != "still-attached" {
		t.Fatalf("stale deactivation or disconnect projection result=%+v", sink.deliveries)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterDurableCommitDeliveryValidatesAndMapsCapacity(t *testing.T) {
	var unavailable *clusterRuntime
	if _, err := unavailable.CommitDelivery(context.Background(), membership.DeliveryChangeDTO{}); !errors.Is(err, membership.ErrQuorumUnavailable) {
		t.Fatalf("nil runtime commit error=%v, want quorum unavailable", err)
	}

	runtime, sessionID, stop := newClusterDurableTestRuntime(t)
	defer stop()
	seedClusterDurableDelivery(t, runtime)
	if err := runtime.ActivateDelivery(sessionID, 1, 1); err != nil {
		t.Fatalf("activate durable session: %v", err)
	}
	for _, change := range []membership.DeliveryChangeDTO{
		{TenantID: "tenant-a", ClientID: "", Action: membership.DeliveryActionPublish, ProposedAtUnixMillis: 1_700_000_000_010, Topic: "events/invalid", PublishQoS: 1},
		{TenantID: "tenant-a", ClientID: "publisher-a", Action: membership.DeliveryActionPublish, ProposedAtUnixMillis: 1_700_000_000_010, Topic: "events/invalid", PublishQoS: 1, Targets: []membership.DeliveryTargetDTO{{ClientID: ""}}},
	} {
		if _, err := runtime.CommitDelivery(context.Background(), change); err == nil {
			t.Fatalf("invalid durable identity accepted: %+v", change)
		}
	}
	targets := make([]membership.DeliveryTargetDTO, membership.MaxDeliveryTargets)
	for index := range targets {
		targets[index].ClientID = fmt.Sprintf("target-%04d-%s", index, strings.Repeat("x", 48))
	}
	_, err := runtime.CommitDelivery(context.Background(), membership.DeliveryChangeDTO{
		TenantID: "tenant-a", ClientID: "publisher-a", Action: membership.DeliveryActionPublish,
		ProposedAtUnixMillis: 1_700_000_000_011, Topic: "events/oversized",
		Payload: make([]byte, membership.MaxDeliveryPayloadBytes), PublishQoS: 1, Targets: targets,
	})
	if !errors.Is(err, applicationdeliveryqueue.ErrFull) {
		t.Fatalf("oversized durable publish error=%v, want queue capacity", err)
	}

	result, err := runtime.CommitDelivery(nil, membership.DeliveryChangeDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: runtime.config.NodeID, Epoch: 1,
		Action: membership.DeliveryActionDetach, ProposedAtUnixMillis: 1_700_000_000_012,
		SessionExpiresAtUnixMillis: 1_700_000_060_012,
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("nil-context durable detach: result=%+v err=%v", result, err)
	}
	if _, exists := runtime.durableAttachments[sessionID]; exists {
		t.Fatal("committed detach retained the local delivery attachment")
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterRuntimeCommitsOwnershipAndEpochFencedSubscriptions(t *testing.T) {
	runtime, _, stop := newClusterDurableTestRuntime(t)
	defer stop()
	ctx := context.Background()

	owner, exists, err := runtime.GetOwnership(ctx, "tenant-a", "client-a")
	if err != nil || !exists || owner.Epoch != 1 || owner.OwnerNodeID != runtime.config.NodeID {
		t.Fatalf("initial committed ownership=(%+v,%t,%v)", owner, exists, err)
	}
	if _, err := runtime.AcquireOwnership(ctx, "tenant-a", "client-a", runtime.config.NodeID+1); !errors.Is(err, membership.ErrNotLeader) {
		t.Fatalf("remote ownership acquisition error=%v, want not leader", err)
	}
	epoch, err := runtime.AcquireOwnership(ctx, "tenant-a", "client-a", runtime.config.NodeID)
	if err != nil || epoch != 2 {
		t.Fatalf("local ownership reacquisition=(%d,%v), want epoch 2", epoch, err)
	}
	if err := runtime.CommitSubscription(ctx, "tenant-a", "client-a", runtime.config.NodeID, epoch,
		"alerts/#", 2, true, true, 1, uint32(membership.SubscriptionActionAdd)); err != nil {
		t.Fatalf("commit durable subscription: %v", err)
	}
	foundSubscription := false
	for _, subscription := range runtime.machine.Subscriptions() {
		if subscription.Filter == "alerts/#" && subscription.OwnerNodeID == runtime.config.NodeID && subscription.Epoch == epoch {
			foundSubscription = true
		}
	}
	if !foundSubscription {
		t.Fatalf("committed subscription missing at owner epoch %d: %+v", epoch, runtime.machine.Subscriptions())
	}

	if err := runtime.CommitSubscription(ctx, "tenant-a", "client-a", runtime.config.NodeID, 1,
		"alerts/#", 2, true, true, 1, uint32(membership.SubscriptionActionRemove)); !errors.Is(err, membership.ErrQuorumUnavailable) {
		t.Fatalf("stale subscription removal error=%v, want quorum unavailable", err)
	}
	beforeInvalidAction := runtime.machine.LastAppliedIndex()
	if err := runtime.CommitSubscription(ctx, "tenant-a", "client-a", runtime.config.NodeID, epoch,
		"alerts/#", 2, true, true, 1, 99); !errors.Is(err, membership.ErrQuorumUnavailable) {
		t.Fatalf("unsupported subscription action error=%v, want quorum unavailable", err)
	}
	if runtime.machine.LastAppliedIndex() != beforeInvalidAction {
		t.Fatalf("unsupported subscription action advanced applied index from %d to %d", beforeInvalidAction, runtime.machine.LastAppliedIndex())
	}
	if err := runtime.CommitSubscription(ctx, "tenant-a", "client-a", runtime.config.NodeID, epoch,
		"alerts/#", 2, true, true, 1, uint32(membership.SubscriptionActionRemove)); err != nil {
		t.Fatalf("remove committed subscription: %v", err)
	}
	for _, subscription := range runtime.machine.Subscriptions() {
		if subscription.Filter == "alerts/#" {
			t.Fatalf("committed subscription removal retained filter: %+v", subscription)
		}
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2.
func TestClusterDurableQueueDefersPromotionWhenQuorumUnavailable(t *testing.T) {
	runtime, sessionID, stop := newClusterDurableTestRuntime(t)
	defer stop()
	_, enqueued := seedClusterDurableDelivery(t, runtime)
	queue, err := memorydeliveryqueue.New(8)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1_700_000_000_100)
	if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "events/one", Payload: []byte("first"), QoS: mqtt.QoSExactlyOnce,
		DurableSequence: enqueued.Sequence, DurableEpoch: 1,
	}, now); err != nil {
		t.Fatal(err)
	}
	decorator := &clusterDurableQueue{Queue: queue, runtime: runtime, clock: clockadapter.SystemClock{}}
	runtime.storage.fail(errors.New("simulated storage failure"))
	err = decorator.PromoteHead(applicationdeliveryqueue.OutboundDTO{
		SessionID: sessionID, PacketID: 17, DurableSequence: enqueued.Sequence, DurableEpoch: 1,
	}, now)
	if !errors.Is(err, applicationdeliveryqueue.ErrRetryableUnavailable) {
		t.Fatalf("promotion error=%v, want retryable queue unavailability", err)
	}
	message, exists := runtime.machine.DeliveryMessage("tenant-a", "client-a", enqueued.Sequence)
	if !exists || message.Phase != membership.DeliveryPhaseQueued {
		t.Fatalf("unavailable promotion changed durable state: %+v exists=%t", message, exists)
	}
	queued, err := queue.ListQueued(sessionID, now)
	if err != nil || len(queued) != 1 || queued[0].DurableSequence != enqueued.Sequence {
		t.Fatalf("unavailable promotion changed queued head: %+v err=%v", queued, err)
	}
	outbound, err := queue.ListOutbound(sessionID)
	if err != nil || len(outbound) != 0 {
		t.Fatalf("unavailable promotion created local outbound state: %+v err=%v", outbound, err)
	}
}
func TestClusterDurableQueueLifecycleRemovesMessages(t *testing.T) {
	runtime, sessionID, stop := newClusterDurableTestRuntime(t)
	defer stop()
	_, enqueued := seedClusterDurableDelivery(t, runtime)
	queue, err := memorydeliveryqueue.New(256)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}
	decorator := &clusterDurableQueue{Queue: queue, runtime: runtime, clock: clockadapter.SystemClock{}}
	now := time.UnixMilli(1_700_000_000_100)
	for i := uint64(1); i <= 100; i++ {
		seq := enqueued.Sequence + i
		pubResult, err := runtime.CommitDelivery(context.Background(), membership.DeliveryChangeDTO{
			TenantID: "tenant-a", ClientID: "publisher-a", Action: membership.DeliveryActionPublish,
			ProposedAtUnixMillis: uint64(1_700_000_000_100 + i), Topic: "events/test", Payload: []byte("payload"),
			PublishQoS: 2, Targets: []membership.DeliveryTargetDTO{{ClientID: "client-a"}},
		})
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if len(pubResult.Delivery.Saturated) > 0 {
			state, _ := runtime.machine.DeliverySession("tenant-a", "client-a")
			t.Fatalf("publish %d was SATURATED! saturated=%v messages_len=%d",
				i, pubResult.Delivery.Saturated, len(state.Messages))
		}
		packetID := mqtt.PacketIdentifier(i%100 + 1)
		if err := queue.TryEnqueue(applicationdeliveryqueue.DeliveryDTO{
			SessionID: sessionID, Topic: "events/test", Payload: []byte("payload"), QoS: mqtt.QoSExactlyOnce,
			DurableSequence: seq, DurableEpoch: 1,
		}, now); err != nil {
			t.Fatalf("tryEnqueue %d: %v", i, err)
		}
		if err := decorator.PromoteHead(applicationdeliveryqueue.OutboundDTO{
			SessionID: sessionID, PacketID: packetID, DurableSequence: seq, DurableEpoch: 1,
		}, now); err != nil {
			t.Fatalf("promoteHead %d: %v", i, err)
		}
		if err := decorator.ReplaceOutbound(applicationdeliveryqueue.OutboundDTO{
			SessionID: sessionID, PacketID: packetID, DurableSequence: seq, DurableEpoch: 1,
		}); err != nil {
			t.Fatalf("replaceOutbound %d: %v", i, err)
		}
		if err := decorator.CompleteOutbound(sessionID, packetID); err != nil {
			t.Fatalf("completeOutbound %d: %v", i, err)
		}
		sessionState, _ := runtime.machine.DeliverySession("tenant-a", "client-a")
		if len(sessionState.Messages) != 1 {
			t.Fatalf("after complete %d: len(Messages)=%d, want 1", i, len(sessionState.Messages))
		}
	}
}
