package membership

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func deliveryTestApply(t *testing.T, machine *StateMachine, index uint64, change DeliveryChangeDTO) ApplyResultDTO {
	t.Helper()
	return applyMustSucceed(t, machine, index, CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor,
		OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index), Delivery: &change})
}

func deliveryTestApplyResult(t *testing.T, machine *StateMachine, index uint64, change DeliveryChangeDTO) ApplyResultDTO {
	t.Helper()
	result, err := machine.Apply(index, CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor,
		OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index), Delivery: &change})
	if err != nil {
		t.Fatalf("apply delivery command at index %d: %v", index, err)
	}
	return result
}

func deliveryTestAcquire(t *testing.T, machine *StateMachine, index uint64, clientID string) {
	t.Helper()
	applyMustSucceed(t, machine, index, CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor,
		OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index),
		Ownership:   &OwnershipChangeDTO{TenantID: "t", ClientID: clientID, OwnerNodeID: 1, Epoch: 1, Action: OwnershipActionAcquire}})
}

func deliveryTestSubscribe(t *testing.T, machine *StateMachine, index uint64, clientID, filter string, qos uint32) {
	t.Helper()
	applyMustSucceed(t, machine, index, CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor,
		OperationID:  fmt.Sprintf("00000000-0000-4000-8000-%012d", index),
		Subscription: &SubscriptionChangeDTO{TenantID: "t", ClientID: clientID, OwnerNodeID: 1, Epoch: 1, Filter: filter, QoS: qos, Action: SubscriptionActionAdd}})
}

func TestDeliveryActionTransitionsAndAttachmentSemantics(t *testing.T) {
	machine := NewStateMachine()
	deliveryTestAcquire(t, machine, 1, "subscriber")
	deliveryTestSubscribe(t, machine, 2, "subscriber", "orders/#", 2)
	attached := deliveryTestApply(t, machine, 3, DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 1})
	if attached.Delivery == nil || !attached.Delivery.Found || attached.Delivery.Session == nil || attached.Delivery.Session.NextSequence != 1 {
		t.Fatalf("initial ATTACH = %+v", attached.Delivery)
	}
	published := deliveryTestApply(t, machine, 4, DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 2,
		Topic: "orders/new", Payload: []byte("one"), PublishQoS: 2, Targets: []DeliveryTargetDTO{{ClientID: "subscriber"}}})
	if !reflect.DeepEqual(published.Delivery.Enqueued, []DeliveryEnqueuedDTO{{ClientID: "subscriber", Sequence: 1}}) {
		t.Fatalf("PUBLISH enqueue result = %+v", published.Delivery)
	}
	state, _ := machine.DeliverySession("t", "subscriber")
	if state.Messages[0].QoS != 2 || state.Messages[0].Phase != DeliveryPhaseQueued || state.Messages[0].PacketID != 0 {
		t.Fatalf("queued durable message = %+v", state.Messages[0])
	}
	resumed := deliveryTestApply(t, machine, 5, DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 3})
	if !resumed.Delivery.Found || resumed.Delivery.Session.NextSequence != 2 || len(resumed.Delivery.Session.Messages) != 1 {
		t.Fatalf("ATTACH of nonempty session = %+v", resumed.Delivery)
	}
	reset := deliveryTestApply(t, machine, 6, DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 4, Reset: true})
	if reset.Delivery.Found || reset.Delivery.Session.NextSequence != 2 || len(reset.Delivery.Session.Messages) != 0 {
		t.Fatalf("reset ATTACH = %+v", reset.Delivery)
	}
	reattached := deliveryTestApply(t, machine, 7, DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 5})
	if !reattached.Delivery.Found || reattached.Delivery.Session.NextSequence != 2 {
		t.Fatalf("empty ATTACH after reset = %+v", reattached.Delivery)
	}
}

func TestDeliveryReceiveDuplicateCapacityAndTombstoneReplacement(t *testing.T) {
	machine := NewStateMachine()
	deliveryTestAcquire(t, machine, 1, "publisher")
	deliveryTestApply(t, machine, 2, DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 1})
	receive := DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionReceive, ProposedAtUnixMillis: 2, PacketID: 17, Topic: "events/one", Payload: []byte("original")}
	deliveryTestApply(t, machine, 3, receive)
	receive.Payload = []byte("duplicate")
	deliveryTestApply(t, machine, 4, receive)
	state, _ := machine.DeliverySession("t", "publisher")
	if len(state.Inbound) != 1 || string(state.Inbound[0].Payload) != "original" {
		t.Fatalf("duplicate RECEIVE changed inbound state: %+v", state.Inbound)
	}
	for packetID := uint32(18); packetID < 17+MaxDeliveryInboundPerSession; packetID++ {
		next := receive
		next.PacketID = packetID
		next.ProposedAtUnixMillis++
		deliveryTestApply(t, machine, uint64(packetID-12), next)
	}
	before, _ := machine.DeliverySession("t", "publisher")
	full := receive
	full.PacketID = 200
	if result := deliveryTestApplyResult(t, machine, 100, full); result.Conflict != ConflictDeliveryCapacity {
		t.Fatalf("full RECEIVE conflict = %q, want %q", result.Conflict, ConflictDeliveryCapacity)
	}
	after, _ := machine.DeliverySession("t", "publisher")
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("capacity rejection mutated inbound state: before=%+v after=%+v", before, after)
	}
	release := DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionRelease, ProposedAtUnixMillis: 40, PacketID: 17, PublishQoS: 2}
	first := deliveryTestApply(t, machine, 101, release)
	if first.Delivery == nil || !first.Delivery.Found || !first.Delivery.Released {
		t.Fatalf("first RELEASE = %+v", first.Delivery)
	}
	retry := deliveryTestApply(t, machine, 102, release)
	if retry.Delivery.Found || !retry.Delivery.Released || len(retry.Delivery.Enqueued) != 0 {
		t.Fatalf("duplicate RELEASE = %+v", retry.Delivery)
	}
	replacement := receive
	replacement.Payload = []byte("new exchange")
	replacement.ProposedAtUnixMillis = 41
	deliveryTestApply(t, machine, 103, replacement)
	state, _ = machine.DeliverySession("t", "publisher")
	for _, inbound := range state.Inbound {
		if inbound.PacketID == 17 && (inbound.Released || string(inbound.Payload) != "new exchange") {
			t.Fatalf("packet ID reuse did not replace tombstone: %+v", inbound)
		}
	}
}

func TestDeliverySendAdvanceCompleteAndExpiryPurge(t *testing.T) {
	machine := NewStateMachine()
	deliveryTestAcquire(t, machine, 1, "subscriber")
	deliveryTestSubscribe(t, machine, 2, "subscriber", "events/#", 2)
	deliveryTestApply(t, machine, 3, DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 1})
	deliveryTestApply(t, machine, 4, DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 2, Topic: "events/queued", Payload: []byte("q"), PublishQoS: 1, ExpiresAtUnixMillis: 10, Targets: []DeliveryTargetDTO{{ClientID: "subscriber"}}})
	deliveryTestApply(t, machine, 5, DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 2, Topic: "events/inflight", Payload: []byte("i"), PublishQoS: 2, Targets: []DeliveryTargetDTO{{ClientID: "subscriber"}}})
	send := DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionSend, ProposedAtUnixMillis: 3, Sequence: 2, PacketID: 33}
	deliveryTestApply(t, machine, 6, send)
	deliveryTestApply(t, machine, 7, send)
	conflict := send
	conflict.PacketID = 34
	if result := deliveryTestApplyResult(t, machine, 8, conflict); result.Conflict != ConflictDeliveryPacketID {
		t.Fatalf("second SEND packet ID conflict = %q", result.Conflict)
	}
	advance := DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAdvance, ProposedAtUnixMillis: 4, PacketID: 33}
	deliveryTestApply(t, machine, 9, advance)
	deliveryTestApply(t, machine, 10, advance)
	state, _ := machine.DeliverySession("t", "subscriber")
	if state.Messages[0].Phase != DeliveryPhaseQueued || state.Messages[1].Phase != DeliveryPhaseAwaitPubComp {
		t.Fatalf("SEND/ADVANCE phases = %+v", state.Messages)
	}
	// Expiry purges queued messages but preserves assigned inflight messages.
	deliveryTestApply(t, machine, 11, DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionDetach, ProposedAtUnixMillis: 11, SessionExpiresAtUnixMillis: 100})
	state, _ = machine.DeliverySession("t", "subscriber")
	if len(state.Messages) != 1 || state.Messages[0].PacketID != 33 || state.Messages[0].Phase != DeliveryPhaseAwaitPubComp {
		t.Fatalf("expiry purge removed inflight delivery or kept queued expiry: %+v", state.Messages)
	}
	complete := DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionComplete, ProposedAtUnixMillis: 11, PacketID: 33}
	deliveryTestApply(t, machine, 12, complete)
	state, _ = machine.DeliverySession("t", "subscriber")
	if len(state.Messages) != 0 {
		t.Fatalf("COMPLETE did not remove inflight message: %+v", state.Messages)
	}
	deliveryTestApply(t, machine, 13, DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionDetach, ProposedAtUnixMillis: 11, SessionExpiresAtUnixMillis: 11})
	deliveryTestApply(t, machine, 14, DeliveryChangeDTO{TenantID: "t", ClientID: "subscriber", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionDetach, ProposedAtUnixMillis: 12, SessionExpiresAtUnixMillis: 12})
	state, exists := machine.DeliverySession("t", "subscriber")
	if exists || len(state.Messages) != 0 || len(machine.Subscriptions()) != 0 {
		t.Fatalf("session expiry did not purge delivery and subscriptions: state=%+v exists=%t subs=%+v", state, exists, machine.Subscriptions())
	}
}

func TestDeliveryTargetRevalidationOrderAndEffectiveQoS(t *testing.T) {
	machine := NewStateMachine()
	deliveryTestAcquire(t, machine, 1, "a")
	deliveryTestAcquire(t, machine, 2, "b")
	deliveryTestSubscribe(t, machine, 3, "a", "$share/g/orders/#", 1)
	deliveryTestSubscribe(t, machine, 4, "a", "orders/new", 2)
	deliveryTestSubscribe(t, machine, 5, "b", "orders/#", 0)
	result := deliveryTestApply(t, machine, 6, DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 1, Topic: "orders/new", Payload: []byte("v"), PublishQoS: 2,
		Targets: []DeliveryTargetDTO{{ClientID: "b"}, {ClientID: "missing"}, {ClientID: "a"}}})
	if !reflect.DeepEqual(result.Delivery.Skipped, []string{"b", "missing"}) || !reflect.DeepEqual(result.Delivery.Enqueued, []DeliveryEnqueuedDTO{{ClientID: "a", Sequence: 1}}) {
		t.Fatalf("target result order/validation = %+v", result.Delivery)
	}
	message, exists := machine.DeliveryMessage("t", "a", 1)
	if !exists || message.QoS != 2 {
		t.Fatalf("effective QoS from matching subscriptions = (%+v, %t)", message, exists)
	}
}

func TestDeliverySessionQuotaSaturatesOnlyFullTarget(t *testing.T) {
	machine := NewStateMachine()
	deliveryTestAcquire(t, machine, 1, "full")
	deliveryTestAcquire(t, machine, 2, "available")
	deliveryTestSubscribe(t, machine, 3, "full", "quota/#", 1)
	deliveryTestSubscribe(t, machine, 4, "available", "quota/#", 1)
	for i := 0; i < MaxDeliveryMessagesPerSession; i++ {
		deliveryTestApply(t, machine, uint64(i+5), DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 1,
			Topic: "quota/fill", Payload: []byte{byte(i)}, PublishQoS: 1, Targets: []DeliveryTargetDTO{{ClientID: "full"}}})
	}
	result := deliveryTestApply(t, machine, 100, DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 1,
		Topic: "quota/next", Payload: []byte("last"), PublishQoS: 1, Targets: []DeliveryTargetDTO{{ClientID: "full"}, {ClientID: "available"}}})
	if !reflect.DeepEqual(result.Delivery.Saturated, []string{"full"}) || !reflect.DeepEqual(result.Delivery.Enqueued, []DeliveryEnqueuedDTO{{ClientID: "available", Sequence: 1}}) {
		t.Fatalf("session saturation result = %+v", result.Delivery)
	}
	if got, _ := machine.DeliverySession("t", "full"); len(got.Messages) != MaxDeliveryMessagesPerSession || got.NextSequence != MaxDeliveryMessagesPerSession+1 {
		t.Fatalf("full target state = %+v", got)
	}
}

func TestDeliveryReceiveEvictsOldestTombstoneAndDiscardCleansSubscriptions(t *testing.T) {
	machine := NewStateMachine()
	deliveryTestAcquire(t, machine, 1, "publisher")
	deliveryTestSubscribe(t, machine, 2, "publisher", "events/#", 1)
	deliveryTestApply(t, machine, 3, DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 1})
	index := uint64(4)
	for packetID := uint32(1); packetID <= MaxDeliveryInboundPerSession; packetID++ {
		receive := DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionReceive, ProposedAtUnixMillis: 2, PacketID: packetID, Topic: "events/in", Payload: []byte("x")}
		deliveryTestApply(t, machine, index, receive)
		index++
		release := DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionRelease, ProposedAtUnixMillis: 3, PacketID: packetID, PublishQoS: 2}
		deliveryTestApply(t, machine, index, release)
		index++
	}
	newReceive := DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionReceive, ProposedAtUnixMillis: 4, PacketID: 1000, Topic: "events/in", Payload: []byte("new")}
	deliveryTestApply(t, machine, index, newReceive)
	index++
	state, _ := machine.DeliverySession("t", "publisher")
	if len(state.Inbound) != MaxDeliveryInboundPerSession || state.Inbound[0].PacketID != 2 {
		t.Fatalf("oldest tombstone was not evicted: first=%+v total=%d", state.Inbound[0], len(state.Inbound))
	}
	deliveryTestApply(t, machine, 201, DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionDiscard, ProposedAtUnixMillis: 5})
	if _, exists := machine.DeliverySession("t", "publisher"); exists || len(machine.Subscriptions()) != 0 {
		t.Fatalf("DISCARD retained session state or committed subscriptions: state=%+v subscriptions=%+v", machine.DeliverySessions(), machine.Subscriptions())
	}
}

func TestDeliveryInvalidStaleFencesAndMissingRelease(t *testing.T) {
	machine := NewStateMachine()
	deliveryTestAcquire(t, machine, 1, "publisher")
	stale := DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", OwnerNodeID: 2, Epoch: 1, Action: DeliveryActionAttach, ProposedAtUnixMillis: 1}
	if result := deliveryTestApplyResult(t, machine, 2, stale); result.Conflict != ConflictStaleEpoch {
		t.Fatalf("stale ATTACH conflict = %q", result.Conflict)
	}
	missing := DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionRelease, ProposedAtUnixMillis: 2, PacketID: 88, PublishQoS: 2}
	result := deliveryTestApply(t, machine, 3, missing)
	if result.Delivery == nil || result.Delivery.Found || result.Delivery.Released {
		t.Fatalf("missing RELEASE result = %+v", result.Delivery)
	}
	if _, exists := machine.DeliverySession("t", "publisher"); exists {
		t.Fatal("missing RELEASE unexpectedly created session state")
	}
	invalid := missing
	invalid.PublishQoS = 1
	if _, err := machine.Apply(4, CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: "00000000-0000-4000-8000-000000000004", Delivery: &invalid}); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("invalid RELEASE QoS error = %v", err)
	}
}

func TestDeliveryGlobalEntryQuotaSaturatesWhenAllTargetsWouldExceedLimit(t *testing.T) {
	machine := NewStateMachine()
	const fullSessions = MaxDeliveryEntries / MaxDeliveryMessagesPerSession
	clients := make([]string, fullSessions+1)
	for index := range clients {
		clientID := fmt.Sprintf("client-%03d", index)
		clients[index] = clientID
		baseIndex := uint64(index*2 + 1)
		deliveryTestAcquire(t, machine, baseIndex, clientID)
		deliveryTestSubscribe(t, machine, baseIndex+1, clientID, "global/#", 1)
	}
	for sequence := 0; sequence < MaxDeliveryMessagesPerSession; sequence++ {
		targets := make([]DeliveryTargetDTO, fullSessions)
		for index := range targets {
			targets[index] = DeliveryTargetDTO{ClientID: clients[index]}
		}
		result := deliveryTestApply(t, machine, uint64(1000+sequence), DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 1,
			Topic: "global/fill", Payload: []byte("x"), PublishQoS: 1, Targets: targets})
		if len(result.Delivery.Saturated) != 0 || len(result.Delivery.Enqueued) != fullSessions {
			t.Fatalf("fill batch %d result = %+v", sequence, result.Delivery)
		}
	}
	lastTarget := clients[len(clients)-1]
	result := deliveryTestApply(t, machine, 2000, DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 1,
		Topic: "global/full", Payload: []byte("x"), PublishQoS: 1,
		Targets: []DeliveryTargetDTO{{ClientID: lastTarget}, {ClientID: clients[0]}}})
	if !reflect.DeepEqual(result.Delivery.Saturated, []string{lastTarget, clients[0]}) || len(result.Delivery.Enqueued) != 0 {
		t.Fatalf("global quota saturation result = %+v", result.Delivery)
	}
	if got, _ := machine.DeliverySession("t", lastTarget); len(got.Messages) != 0 || got.NextSequence != 1 {
		t.Fatalf("global capacity rejection consumed target sequence: %+v", got)
	}
}
