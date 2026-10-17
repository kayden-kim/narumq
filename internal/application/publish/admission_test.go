package publish

import (
	"errors"
	"testing"

	"narumq/internal/application/membership"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/tenancy"
)

type fakePublishAdmission struct{ pressure, capacity error }

func (a *fakePublishAdmission) CheckPressure() error { return a.pressure }
func (a *fakePublishAdmission) CheckDeliveryCapacity(membership.DeliveryChangeDTO) error {
	return a.capacity
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.5, §11.28.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-196
func TestPublishAdmissionGatesNewDurableState(t *testing.T) {
	for _, tc := range []struct {
		name, path                                     string
		qos                                            mqtt.QoS
		retain, delete, capacity, allowed, noAdmission bool
	}{
		{name: "phased-qos1-pressure", path: "begin", qos: 1},
		{name: "phased-qos1-capacity", path: "begin", qos: 1, capacity: true},
		{name: "inline-qos1-pressure", qos: 1},
		{name: "inline-retained-qos1-pressure", qos: 1, retain: true},
		{name: "inline-qos1-capacity", qos: 1, capacity: true},
		{name: "retained-qos0-pressure", retain: true},
		{name: "qos0-live", allowed: true},
		{name: "retained-delete", retain: true, delete: true, allowed: true},
		{name: "receive-qos2-pressure", path: "receive", qos: 2},
		{name: "receive-qos2-capacity", path: "receive", qos: 2, capacity: true},
		{name: "phased-release", path: "release-begin", qos: 2, allowed: true},
		{name: "inline-release", path: "release", qos: 2, allowed: true},
		{name: "will", path: "will", qos: 1, allowed: true},
		{name: "nil-admission", path: "begin", qos: 1, allowed: true, noAdmission: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, queue, committer, now := durablePublishFixture(t, membership.DeliveryResultDTO{Found: true, Enqueued: []membership.DeliveryEnqueuedDTO{{ClientID: "durable-sub", Sequence: 1}}}, nil)
			queue.onEnqueue = nil
			retained := &recordingRetainedCommitter{}
			WithRetainedCommitter(retained)(uc)
			admission := &fakePublishAdmission{pressure: ErrPublishBackpressure}
			wantErr := ErrPublishBackpressure
			if tc.capacity {
				admission.pressure = nil
				admission.capacity = ErrDurableDeliveryCapacity
				wantErr = ErrDurableDeliveryCapacity
			}
			if !tc.noAdmission {
				WithAdmission(admission)(uc)
			}
			packet := mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "events/admission", Payload: []byte("value"), QoS: tc.qos, PacketID: 1, Retain: tc.retain}
			if tc.delete {
				packet.Payload = nil
			}
			id := mustPublishSessionID(t, "publisher")
			var err error
			switch tc.path {
			case "begin":
				pending, e := uc.BeginPublish(id, packet, now)
				err = e
				if err == nil {
					_, err = pending.Wait()
				}
			case "receive":
				pending, e := uc.BeginReceiveQoS2(id, 1, 1, packet, now)
				err = e
				if err == nil {
					err = pending.Wait()
				}
			case "release-begin":
				pending, e := uc.BeginQoS2Release(id, 1, 1, packet, now)
				err = e
				if err == nil {
					_, err = pending.Wait()
				}
			case "release":
				_, err = uc.PublishQoS2Release(id, 1, 1, packet, now)
			case "will":
				tenant, _ := tenancy.NewTenantID("tenant-a")
				key, _ := tenancy.NewSessionKey(tenant, "publisher")
				_, err = uc.PublishWill(id, key, packet, now)
			default:
				_, err = uc.Publish(id, packet, now)
			}
			if tc.allowed {
				if err != nil {
					t.Fatal(err)
				}
				if tc.qos > 0 {
					wantAction := membership.DeliveryActionPublish
					if tc.path == "release" || tc.path == "release-begin" {
						wantAction = membership.DeliveryActionRelease
					}
					if len(committer.changes) != 1 || committer.changes[0].Action != wantAction || len(committer.changes[0].Targets) != 1 {
						t.Fatalf("durable state was not committed: %+v", committer.changes)
					}
				}
				if tc.qos == 0 && len(queue.deliveries) != 2 {
					t.Fatalf("live delivery: %+v", queue.deliveries)
				}
				if tc.delete && (len(retained.changes) != 1 || !retained.changes[0].Delete) {
					t.Fatalf("retained deletion: %+v", retained.changes)
				}
			} else {
				if !errors.Is(err, wantErr) {
					t.Fatalf("error=%v want %v", err, wantErr)
				}
				if len(committer.changes) != 0 || committer.barriers != 0 || len(retained.changes) != 0 || len(queue.deliveries) != 0 {
					t.Fatal("rejected publish mutated storage or delivered")
				}
			}
		})
	}
}
