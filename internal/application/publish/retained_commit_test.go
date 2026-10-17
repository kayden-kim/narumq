package publish

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	applicationretained "narumq/internal/application/ports/retained"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

type recordingRetainedCommitter struct {
	changes []RetainedChangeDTO
	err     error
}

func (committer *recordingRetainedCommitter) CommitRetained(_ context.Context, change RetainedChangeDTO) error {
	committer.changes = append(committer.changes, change)
	return committer.err
}

// Traceability: PRD CLU-08/CLU-14/TEN-05; docs/specs/cluster-session-and-delivery.md §4·§10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-038
func TestPublishCommitsRetainedReplaceAndDeleteWithoutLocalMutation(t *testing.T) {
	now := testNow()
	clock := &fakeClock{now: now}
	routes := routing.NewTable()
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := routes.BindSessionTenant("publisher", tenantID); err != nil {
		t.Fatal(err)
	}
	if err := routes.BindSessionTenant("subscriber", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := routes.Subscribe("subscriber", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	repository := &recordingRetainedRepository{}
	committer := &recordingRetainedCommitter{}
	useCase, err := New(clock, routes, queue, repository, WithRetainedCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}

	expiresAt := now.Add(30 * time.Second)
	if _, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, Retain: true,
		Topic: "events/state", Payload: []byte("value"),
		HasMessageExpiryInterval: true, MessageExpiryInterval: 30,
	}, now); err != nil {
		t.Fatalf("retained replace: %v", err)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Retain: true, Topic: "events/state",
	}, now); err != nil {
		t.Fatalf("zero-payload delete: %v", err)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Retain: true, Topic: "events/expired", Payload: []byte("expired"),
		HasMessageExpiryInterval: true, MessageExpiryInterval: 1,
	}, now.Add(-time.Second)); err != nil {
		t.Fatalf("expired retained delete: %v", err)
	}

	want := []RetainedChangeDTO{
		{
			Message:    retainedMessageForTest(tenantID, "publisher", "events/state", []byte("value"), mqtt.QoSAtLeastOnce, expiresAt),
			ProposedAt: now,
		},
		{
			Message: retainedMessageForTest(tenantID, "publisher", "events/state", nil, mqtt.QoSAtMostOnce, time.Time{}),
			Delete:  true, ProposedAt: now,
		},
		{
			Message: retainedMessageForTest(tenantID, "publisher", "events/expired", nil, mqtt.QoSAtMostOnce, time.Time{}),
			Delete:  true, ProposedAt: now,
		},
	}
	if !reflect.DeepEqual(committer.changes, want) {
		t.Fatalf("retained commits = %#v, want %#v", committer.changes, want)
	}
	if len(repository.replaces) != 0 || len(repository.deletes) != 0 {
		t.Fatalf("committed retained changes mutated local repository: replaces=%#v deletes=%#v", repository.replaces, repository.deletes)
	}
	if len(queue.deliveries) != 2 {
		t.Fatalf("routed deliveries = %d, want two unexpired publications", len(queue.deliveries))
	}
}

// Traceability: PRD CLU-08/CLU-14/TEN-05; docs/specs/cluster-session-and-delivery.md §4·§10.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-038
func TestPublishRetainedCommitFailureStopsFanout(t *testing.T) {
	routes := routing.NewTable()
	if _, err := routes.Subscribe("subscriber", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	repository := &recordingRetainedRepository{}
	committer := &recordingRetainedCommitter{err: applicationretained.ErrFull}
	useCase, err := New(&fakeClock{now: testNow()}, routes, queue, repository, WithRetainedCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}
	_, err = useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Retain: true, Topic: "events/state", Payload: []byte("value"),
	}, testNow())
	if !errors.Is(err, applicationretained.ErrFull) {
		t.Fatalf("Publish() error = %v, want %v", err, applicationretained.ErrFull)
	}
	if len(queue.calls) != 0 || len(repository.replaces) != 0 || len(repository.deletes) != 0 {
		t.Fatalf("failed commit caused side effects: queue=%v replaces=%#v deletes=%#v", queue.calls, repository.replaces, repository.deletes)
	}
}

// Traceability: PRD CLU-08/CLU-14/TEN-05; docs/specs/cluster-session-and-delivery.md §4·§10.
func TestPublishWithoutRetainDoesNotCommitRetainedState(t *testing.T) {
	routes := routing.NewTable()
	if _, err := routes.Subscribe("subscriber", mqtt.Subscription{Filter: "events/#", QoS: mqtt.QoSAtMostOnce}); err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{errors: make(map[routing.SessionID]error)}
	committer := &recordingRetainedCommitter{}
	useCase, err := New(&fakeClock{now: testNow()}, routes, queue, &recordingRetainedRepository{}, WithRetainedCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := useCase.Publish("publisher", mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "events/state", Payload: []byte("value"),
	}, testNow()); err != nil {
		t.Fatal(err)
	}
	if len(committer.changes) != 0 || len(queue.deliveries) != 1 {
		t.Fatalf("non-retained publication commits=%#v deliveries=%#v", committer.changes, queue.deliveries)
	}
}

func retainedMessageForTest(tenant tenancy.TenantID, publisher routing.SessionID, topic string, payload []byte, qos mqtt.QoS, expiresAt time.Time) applicationretained.MessageDTO {
	return applicationretained.MessageDTO{
		TenantID: tenant, PublisherSessionID: publisher, Topic: topic,
		Payload: payload, QoS: qos, ExpiresAt: expiresAt,
	}
}
