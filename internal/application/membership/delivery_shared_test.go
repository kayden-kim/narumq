package membership

import (
	"fmt"
	"reflect"
	"testing"
)

const sharedTestFilter = "$share/g/jobs/#"

// sharedFixture applies commands with increasing indexes to a machine whose three voters advertise
// the shared-subscription-reroute feature.
type sharedFixture struct {
	t       *testing.T
	machine *StateMachine
	index   uint64
}

func newSharedFixture(t *testing.T, active bool) *sharedFixture {
	t.Helper()
	fixture := &sharedFixture{t: t, machine: releaseProfileMachine(t, "1.0.0", []string{SharedSubscriptionRerouteFeature}), index: 1}
	if active {
		fixture.index++
		if result := activateFeatureForTest(t, fixture.machine, fixture.index, SharedSubscriptionRerouteFeature); result.Outcome != ApplyOutcomeApplied {
			t.Fatalf("activate shared-subscription-reroute = %+v", result)
		}
	}
	return fixture
}

func (f *sharedFixture) apply(command CommandDTO) ApplyResultDTO {
	f.t.Helper()
	f.index++
	command.SchemaMajor, command.SchemaMinor = SchemaMajor, SchemaMinor
	command.OperationID = fmt.Sprintf("00000000-0000-4000-8000-%012d", f.index)
	return applyMustSucceed(f.t, f.machine, f.index, command)
}

func (f *sharedFixture) delivery(change DeliveryChangeDTO) ApplyResultDTO {
	f.t.Helper()
	if change.TenantID == "" {
		change.TenantID = "t"
	}
	if change.ProposedAtUnixMillis == 0 {
		change.ProposedAtUnixMillis = 1000
	}
	return f.apply(CommandDTO{Delivery: &change})
}

// member acquires a session on ownerNodeID and subscribes it to filter.
func (f *sharedFixture) member(clientID string, ownerNodeID uint64, filter string, qos uint32) {
	f.t.Helper()
	f.apply(CommandDTO{Ownership: &OwnershipChangeDTO{TenantID: "t", ClientID: clientID, OwnerNodeID: ownerNodeID, Epoch: 1, Action: OwnershipActionAcquire}})
	f.subscribe(clientID, ownerNodeID, 1, filter, qos, false)
}

func (f *sharedFixture) subscribe(clientID string, ownerNodeID, epoch uint64, filter string, qos uint32, retainAsPublished bool) {
	f.t.Helper()
	f.apply(CommandDTO{Subscription: &SubscriptionChangeDTO{TenantID: "t", ClientID: clientID, OwnerNodeID: ownerNodeID, Epoch: epoch, Filter: filter, QoS: qos, RetainAsPublished: retainAsPublished, Action: SubscriptionActionAdd}})
}

// publish queues one message on clientID as if the shared group had selected it.
func (f *sharedFixture) publish(clientID, payload string, qos uint32, expires uint64) ApplyResultDTO {
	f.t.Helper()
	return f.delivery(DeliveryChangeDTO{ClientID: "publisher", Action: DeliveryActionPublish, Topic: "jobs/a", Payload: []byte(payload), PublishQoS: qos, ExpiresAtUnixMillis: expires,
		Targets: []DeliveryTargetDTO{{ClientID: clientID, SharedFilter: sharedTestFilter}}})
}

func (f *sharedFixture) send(clientID string, ownerNodeID uint64, sequence uint64, packetID uint32) {
	f.t.Helper()
	f.delivery(DeliveryChangeDTO{ClientID: clientID, OwnerNodeID: ownerNodeID, Epoch: 1, Action: DeliveryActionSend, Sequence: sequence, PacketID: packetID})
}

func (f *sharedFixture) reset(clientID string, ownerNodeID, epoch uint64) ApplyResultDTO {
	f.t.Helper()
	return f.delivery(DeliveryChangeDTO{ClientID: clientID, OwnerNodeID: ownerNodeID, Epoch: epoch, Action: DeliveryActionAttach, Reset: true})
}

func (f *sharedFixture) payloads(clientID string) []string {
	f.t.Helper()
	state, _ := f.machine.DeliverySession("t", clientID)
	payloads := make([]string, 0, len(state.Messages))
	for _, message := range state.Messages {
		payloads = append(payloads, string(message.Payload))
	}
	return payloads
}

// Traceability: MQTT-07, CLU-05; docs/specs/cluster-session-and-delivery.md §4.2, §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-249
func TestSharedRerouteOnCleanResetMovesQueuedAndUnackedQoS1Only(t *testing.T) {
	f := newSharedFixture(t, true)
	f.member("m0", 1, sharedTestFilter, 2)
	f.member("m1", 2, sharedTestFilter, 1)
	f.member("m2", 3, sharedTestFilter, 2)
	f.publish("m0", "unacked", 1, 0)
	f.publish("m0", "queued-1", 1, 0)
	f.publish("m0", "qos2-sent", 2, 0)
	f.publish("m0", "queued-2", 1, 0)
	f.send("m0", 1, 1, 7)
	f.send("m0", 1, 3, 9)
	f.machine.TakeDeliveryReroutes()

	f.reset("m0", 1, 1)

	if got, _ := f.machine.DeliverySession("t", "m0"); len(got.Messages) != 0 || got.NextSequence != 5 {
		t.Fatalf("reset session = %+v, want empty queue and monotonic sequence", got)
	}
	if got := f.payloads("m1"); !reflect.DeepEqual(got, []string{"unacked", "queued-2"}) {
		t.Fatalf("m1 queue = %v, want the ring successor of m0 to take every other message", got)
	}
	if got := f.payloads("m2"); !reflect.DeepEqual(got, []string{"queued-1"}) {
		t.Fatalf("m2 queue = %v", got)
	}
	moved, _ := f.machine.DeliveryMessage("t", "m1", 1)
	if moved.Phase != DeliveryPhaseQueued || moved.PacketID != 0 || moved.QoS != 1 || moved.SharedFilter != sharedTestFilter {
		t.Fatalf("moved message = %+v, want queued QoS 1 with the shared origin kept", moved)
	}
	wantNotified := []DeliveryRerouteDTO{{TenantID: "t", ClientID: "m1", Sequence: 1}, {TenantID: "t", ClientID: "m2", Sequence: 1}, {TenantID: "t", ClientID: "m1", Sequence: 2}}
	if got := f.machine.TakeDeliveryReroutes(); !reflect.DeepEqual(got, wantNotified) {
		t.Fatalf("reroute notifications = %+v, want %+v", got, wantNotified)
	}
	if got := f.machine.TakeDeliveryReroutes(); len(got) != 0 {
		t.Fatalf("reroute notifications were not cleared: %+v", got)
	}
	if subs := f.machine.Subscriptions(); len(subs) != 2 {
		t.Fatalf("reset must remove only the resetting member's subscriptions: %+v", subs)
	}
}

// Traceability: MQTT-07, CLU-05; docs/specs/cluster-session-and-delivery.md §4.2, §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-250
func TestSharedRerouteCapsQoSAndRetainAtDestination(t *testing.T) {
	f := newSharedFixture(t, true)
	f.member("m0", 1, sharedTestFilter, 2)
	f.member("m1", 2, sharedTestFilter, 1)
	f.delivery(DeliveryChangeDTO{ClientID: "publisher", Action: DeliveryActionPublish, Topic: "jobs/a", Payload: []byte("hot"), PublishQoS: 2, Retain: true,
		Targets: []DeliveryTargetDTO{{ClientID: "m0", Retain: true, SharedFilter: sharedTestFilter}}})

	f.reset("m0", 1, 1)

	moved, exists := f.machine.DeliveryMessage("t", "m1", 1)
	if !exists || moved.QoS != 1 || moved.Retain {
		t.Fatalf("moved message = (%+v, %t), want QoS capped to 1 and RETAIN cleared without Retain As Published", moved, exists)
	}
}

// Traceability: MQTT-07, CLU-05; docs/specs/cluster-session-and-delivery.md §4.2, §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-251
func TestSharedRerouteTriesMembersOwnedByProposerFirst(t *testing.T) {
	f := newSharedFixture(t, true)
	f.member("m0", 1, sharedTestFilter, 1)
	f.member("m1", 2, sharedTestFilter, 1)
	f.member("m2", 3, sharedTestFilter, 1)
	f.publish("m0", "one", 1, 0)
	f.publish("m0", "two", 1, 0)
	// m0 reconnects on node 3 (live) after node 1 died: the reset is proposed by node 3.
	f.apply(CommandDTO{Ownership: &OwnershipChangeDTO{TenantID: "t", ClientID: "m0", OwnerNodeID: 3, Epoch: 2, ExpectedEpoch: 1, Action: OwnershipActionAcquire}})

	f.reset("m0", 3, 2)

	if got := f.payloads("m2"); !reflect.DeepEqual(got, []string{"one", "two"}) {
		t.Fatalf("m2 queue = %v, want the proposer's own member to take both messages", got)
	}
	if got := f.payloads("m1"); len(got) != 0 {
		t.Fatalf("m1 queue = %v, want nothing while a member of the proposer's node can take messages", got)
	}
}

// Traceability: MQTT-07, CLU-05; docs/specs/cluster-session-and-delivery.md §4.2, §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-252
func TestSharedRerouteOnDiscardRelease(t *testing.T) {
	f := newSharedFixture(t, true)
	f.member("m0", 1, sharedTestFilter, 1)
	f.member("m1", 2, sharedTestFilter, 1)
	f.member("m2", 3, sharedTestFilter, 1)
	f.publish("m0", "discarded", 1, 0)
	f.delivery(DeliveryChangeDTO{ClientID: "m0", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionDiscard})
	if got := f.payloads("m1"); !reflect.DeepEqual(got, []string{"discarded"}) {
		t.Fatalf("DISCARD did not move the message to the ring successor: m1=%v m2=%v", got, f.payloads("m2"))
	}
	f.publish("m1", "released", 1, 0)
	f.apply(CommandDTO{Ownership: &OwnershipChangeDTO{TenantID: "t", ClientID: "m1", OwnerNodeID: 2, Epoch: 1, ExpectedEpoch: 1, Action: OwnershipActionRelease}})
	if got := f.payloads("m2"); !reflect.DeepEqual(got, []string{"discarded", "released"}) {
		t.Fatalf("ownership Release did not move queued messages: m2=%v", got)
	}
}

// Traceability: MQTT-07, CLU-05; docs/specs/cluster-session-and-delivery.md §4.2, §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-253
func TestSharedRerouteOnSessionExpiryPurgeIsOrderedAndSkipsExpiredMembers(t *testing.T) {
	f := newSharedFixture(t, true)
	for client, owner := range map[string]uint64{"m0": 1, "m1": 2, "m2": 3} {
		f.member(client, owner, sharedTestFilter, 1)
	}
	f.publish("m0", "from-m0", 1, 0)
	f.publish("m1", "from-m1", 1, 0)
	f.delivery(DeliveryChangeDTO{ClientID: "m0", OwnerNodeID: 1, Epoch: 1, Action: DeliveryActionDetach, SessionExpiresAtUnixMillis: 2000})
	f.delivery(DeliveryChangeDTO{ClientID: "m1", OwnerNodeID: 2, Epoch: 1, Action: DeliveryActionDetach, SessionExpiresAtUnixMillis: 2000})

	// Any delivery command after the expiry purges both sessions; neither expired member may receive the other's message.
	f.delivery(DeliveryChangeDTO{ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 3000, Topic: "other/topic", Payload: []byte("x"), PublishQoS: 1})

	if got := f.payloads("m2"); !reflect.DeepEqual(got, []string{"from-m0", "from-m1"}) {
		t.Fatalf("m2 queue = %v, want both messages of the expired sessions in client ID order", got)
	}
	for _, expired := range []string{"m0", "m1"} {
		if _, exists := f.machine.DeliverySession("t", expired); exists {
			t.Fatalf("expired session %s still has delivery state", expired)
		}
	}
}

// Traceability: MQTT-07, CLU-05; docs/specs/cluster-session-and-delivery.md §4.2, §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-254
func TestSharedRerouteSkipsFullMemberAndDropsWhenNobodyCanTake(t *testing.T) {
	f := newSharedFixture(t, true)
	f.member("m0", 1, sharedTestFilter, 1)
	f.member("m1", 2, sharedTestFilter, 1)
	f.member("m2", 3, sharedTestFilter, 1)
	for _, member := range []string{"m1", "m2"} {
		f.subscribe(member, map[string]uint64{"m1": 2, "m2": 3}[member], 1, "fill/#", 1, false)
	}
	fill := func(client string) {
		for i := range MaxDeliveryMessagesPerSession {
			f.delivery(DeliveryChangeDTO{ClientID: "publisher", Action: DeliveryActionPublish, Topic: "fill/x", Payload: []byte{byte(i)}, PublishQoS: 1, Targets: []DeliveryTargetDTO{{ClientID: client}}})
		}
	}
	f.publish("m0", "first", 1, 0)
	f.publish("m0", "second", 1, 0)
	fill("m1")
	f.reset("m0", 1, 1)
	if got := f.payloads("m2"); !reflect.DeepEqual(got, []string{"first", "second"}) {
		t.Fatalf("m2 queue = %v, want the full member m1 skipped", got)
	}

	f.publish("m2", "third", 1, 0)
	f.reset("m2", 3, 1)
	// m1 is full and m0 has no subscription any more: nobody can take the message.
	m1, _ := f.machine.DeliverySession("t", "m1")
	if len(m1.Messages) != MaxDeliveryMessagesPerSession {
		t.Fatalf("full member m1 received a message beyond its limit: %d", len(m1.Messages))
	}
	for _, payload := range f.payloads("m1") {
		if payload == "first" || payload == "second" || payload == "third" {
			t.Fatalf("dropped message %q reached the full member", payload)
		}
	}
	if got, _ := f.machine.DeliverySession("t", "m2"); len(got.Messages) != 0 {
		t.Fatalf("reset member kept messages: %+v", got.Messages)
	}
	if f.machine.deliveryEntries != MaxDeliveryMessagesPerSession {
		t.Fatalf("delivery entries = %d, want only the full member's %d: dropped messages must release their usage", f.machine.deliveryEntries, MaxDeliveryMessagesPerSession)
	}
}

// Traceability: MQTT-07, CLU-05; docs/specs/cluster-session-and-delivery.md §4.2, §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-254
func TestSharedRerouteDropsWhenGroupHasNoOtherMemberAndLeavesNonSharedAndExpiredMessages(t *testing.T) {
	f := newSharedFixture(t, true)
	f.member("m0", 1, sharedTestFilter, 1)
	f.publish("m0", "alone", 1, 0)
	f.reset("m0", 1, 1)
	if got, _ := f.machine.DeliverySession("t", "m0"); len(got.Messages) != 0 || f.machine.deliveryEntries != 0 {
		t.Fatalf("single-member reset kept state: %+v entries=%d", got, f.machine.deliveryEntries)
	}

	f.member("m1", 2, sharedTestFilter, 1)
	f.member("m2", 3, sharedTestFilter, 1)
	f.publish("m1", "expires", 1, 1500)
	f.delivery(DeliveryChangeDTO{ClientID: "publisher", Action: DeliveryActionPublish, Topic: "jobs/a", Payload: []byte("direct"), PublishQoS: 1, Targets: []DeliveryTargetDTO{{ClientID: "m1"}}})
	f.delivery(DeliveryChangeDTO{ClientID: "m1", OwnerNodeID: 2, Epoch: 1, Action: DeliveryActionAttach, Reset: true, ProposedAtUnixMillis: 2000})
	if got := f.payloads("m2"); len(got) != 0 {
		t.Fatalf("m2 queue = %v, want neither the expired message (proposed after expiry) nor the message without a shared origin", got)
	}
}

// Traceability: MQTT-07; docs/specs/cluster-session-and-delivery.md §4.2, §11.35.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-248
func TestSharedOriginIsRecordedOnlyForExactSharedSubscription(t *testing.T) {
	f := newSharedFixture(t, true)
	f.member("m0", 1, "plain/#", 1)
	result := f.delivery(DeliveryChangeDTO{ClientID: "publisher", Action: DeliveryActionPublish, Topic: "plain/a", Payload: []byte("v"), PublishQoS: 1, Targets: []DeliveryTargetDTO{{ClientID: "m0", SharedFilter: sharedTestFilter}}})
	if len(result.Delivery.Enqueued) != 1 {
		t.Fatalf("publish = %+v", result.Delivery)
	}
	if message, _ := f.machine.DeliveryMessage("t", "m0", 1); message.SharedFilter != "" {
		t.Fatalf("origin %q recorded for a client without that shared subscription", message.SharedFilter)
	}
}

// Traceability: CLU-11, CLU-14; docs/specs/cluster-session-and-delivery.md §11.35; docs/contracts/replicated-state-v1.md.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-248
func TestSharedOriginFeatureGateSnapshotAndByteAccounting(t *testing.T) {
	inactive := newSharedFixture(t, false)
	inactive.member("m0", 1, sharedTestFilter, 1)
	inactive.index++
	result, err := inactive.machine.Apply(inactive.index, CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: fmt.Sprintf("00000000-0000-4000-8000-%012d", inactive.index),
		Delivery: &DeliveryChangeDTO{TenantID: "t", ClientID: "publisher", Action: DeliveryActionPublish, ProposedAtUnixMillis: 1000, Topic: "jobs/a", Payload: []byte("v"), PublishQoS: 1,
			Targets: []DeliveryTargetDTO{{ClientID: "m0", SharedFilter: sharedTestFilter}}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictFeatureInactive {
		t.Fatalf("shared origin before activation = %+v, want %s", result, ConflictFeatureInactive)
	}

	active := newSharedFixture(t, true)
	active.member("m0", 1, sharedTestFilter, 1)
	active.publish("m0", "payload", 1, 0)
	wantBytes := len("t") + len("m0") + len("jobs/a") + len("payload") + len(sharedTestFilter)
	if active.machine.deliveryBytes != wantBytes {
		t.Fatalf("delivery bytes = %d, want %d including the shared origin", active.machine.deliveryBytes, wantBytes)
	}
	snapshot, err := active.machine.Snapshot(active.index)
	if err != nil {
		t.Fatal(err)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore with active feature: %v", err)
	}
	if message, _ := restored.DeliveryMessage("t", "m0", 1); message.SharedFilter != sharedTestFilter || restored.deliveryBytes != wantBytes {
		t.Fatalf("restored message = %+v bytes=%d", message, restored.deliveryBytes)
	}

	withoutFeature := snapshot
	withoutFeature.ActiveFeatures = nil
	withoutFeature.ObjectCount--
	if err := NewStateMachine().Restore(withoutFeature); err == nil {
		t.Fatal("snapshot with a shared origin and no active feature was accepted")
	}
	invalid := snapshot
	invalid.DeliverySessions = cloneDeliverySessions(snapshot.DeliverySessions)
	invalid.DeliverySessions[0].Messages[0].SharedFilter = "jobs/#"
	if err := NewStateMachine().Restore(invalid); err == nil {
		t.Fatal("snapshot with a non-shared origin filter was accepted")
	}
}

func cloneDeliverySessions(sessions []DeliverySessionStateDTO) []DeliverySessionStateDTO {
	cloned := make([]DeliverySessionStateDTO, len(sessions))
	for i, session := range sessions {
		cloned[i] = cloneDeliverySession(session)
	}
	return cloned
}
