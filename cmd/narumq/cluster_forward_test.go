package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/membership"
	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

type clusterForwardTestPeer struct {
	calls []tcpadapter.PeerMessageDTO
	err   error
}

func (peer *clusterForwardTestPeer) Forward(message tcpadapter.PeerMessageDTO) error {
	peer.calls = append(peer.calls, message)
	return peer.err
}

type clusterForwardTestSink struct {
	calls []applicationdeliveryqueue.DeliveryDTO
	err   error
}

func (sink *clusterForwardTestSink) DeliverForwarded(delivery applicationdeliveryqueue.DeliveryDTO) error {
	sink.calls = append(sink.calls, delivery)
	return sink.err
}

func newClusterForwardTestRuntime(t *testing.T, ownerNodeID uint64) (*clusterRuntime, routing.SessionID) {
	t.Helper()
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	_, err := machine.Apply(4, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: "00000000-0000-4000-8000-000000000010",
		Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: ownerNodeID,
			Epoch: 1, ExpectedEpoch: 0, Action: membership.OwnershipActionAcquire,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tenantID, err := tenancy.NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	sessionKey, err := tenancy.NewSessionKey(tenantID, "client-a")
	if err != nil {
		t.Fatal(err)
	}
	return &clusterRuntime{
		config: config, machine: machine,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, routing.SessionID(sessionKey.RoutingID())
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
func TestClusterForwardRemoteUsesCommittedOwnerAndQoS0(t *testing.T) {
	runtime, sessionID := newClusterForwardTestRuntime(t, 2)
	peer := &clusterForwardTestPeer{}
	runtime.forwarder = peer
	expiresAt := time.UnixMilli(1_700_000_000_123)

	handled, err := runtime.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "live/one", Payload: []byte("value"),
		QoS: mqtt.QoSAtMostOnce, Retain: true, ExpiresAt: expiresAt,
	})
	if err != nil || !handled {
		t.Fatalf("ForwardRemote() = handled %t, error %v; want handled with nil error", handled, err)
	}
	if len(peer.calls) != 1 || peer.calls[0].From != 1 || peer.calls[0].To != 2 {
		t.Fatalf("forward calls = %+v, want one message from node 1 to committed owner node 2", peer.calls)
	}
	forwarded, err := tcpadapter.DecodeForwardedDelivery(peer.calls[0].Data)
	if err != nil {
		t.Fatalf("DecodeForwardedDelivery() error = %v", err)
	}
	if forwarded.SessionID != string(sessionID) || forwarded.Topic != "live/one" ||
		string(forwarded.Payload) != "value" || !forwarded.Retain || forwarded.ExpiresAtUnixMillis != uint64(expiresAt.UnixMilli()) {
		t.Fatalf("forwarded payload = %+v, want original delivery fields", forwarded)
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
func TestClusterForwardRemoteKeepsLocalAndNonQoS0Paths(t *testing.T) {
	localRuntime, sessionID := newClusterForwardTestRuntime(t, 1)
	localPeer := &clusterForwardTestPeer{}
	localRuntime.forwarder = localPeer
	handled, err := localRuntime.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "live/one", QoS: mqtt.QoSAtMostOnce,
	})
	if err != nil || handled || len(localPeer.calls) != 0 {
		t.Fatalf("local ForwardRemote() = handled %t, error %v, calls %d; want unhandled local path", handled, err, len(localPeer.calls))
	}

	remoteRuntime, remoteSessionID := newClusterForwardTestRuntime(t, 2)
	remotePeer := &clusterForwardTestPeer{}
	remoteRuntime.forwarder = remotePeer
	handled, err = remoteRuntime.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{
		SessionID: remoteSessionID, Topic: "live/one", QoS: mqtt.QoSAtLeastOnce,
	})
	if err != nil || handled || len(remotePeer.calls) != 0 {
		t.Fatalf("non-QoS 0 ForwardRemote() = handled %t, error %v, calls %d; want existing local path", handled, err, len(remotePeer.calls))
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
func TestClusterForwardRemoteMapsPeerErrors(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		peerErr error
		wantErr error
	}{
		{name: "backpressure", peerErr: tcpadapter.ErrPeerBackpressure, wantErr: applicationdeliveryqueue.ErrFull},
		{name: "unavailable", peerErr: errors.New("peer transport failed"), wantErr: applicationdeliveryqueue.ErrUnavailable},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime, sessionID := newClusterForwardTestRuntime(t, 2)
			runtime.forwarder = &clusterForwardTestPeer{err: testCase.peerErr}
			handled, err := runtime.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{
				SessionID: sessionID, Topic: "live/one", QoS: mqtt.QoSAtMostOnce,
			})
			if !handled || !errors.Is(err, testCase.wantErr) {
				t.Fatalf("ForwardRemote() = handled %t, error %v; want handled and error %v", handled, err, testCase.wantErr)
			}
		})
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
func TestClusterForwardReceiverRequiresCommittedLocalOwner(t *testing.T) {
	localRuntime, sessionID := newClusterForwardTestRuntime(t, 1)
	localSink := &clusterForwardTestSink{}
	localRuntime.forwardSink = localSink
	payload, err := tcpadapter.EncodeForwardedDelivery(tcpadapter.ForwardedDeliveryDTO{
		SessionID: string(sessionID), Topic: "live/one", Payload: []byte("value"), Retain: true,
		ExpiresAtUnixMillis: 1_700_000_000_123,
	})
	if err != nil {
		t.Fatal(err)
	}
	localRuntime.acceptForwardedFrame(tcpadapter.PeerInboundDelivery{
		Message: tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: payload},
	})
	if len(localSink.calls) != 1 {
		t.Fatalf("local sink calls = %d, want 1", len(localSink.calls))
	}
	got := localSink.calls[0]
	if got.SessionID != sessionID || got.Topic != "live/one" || string(got.Payload) != "value" ||
		got.QoS != mqtt.QoSAtMostOnce || !got.Retain || got.ExpiresAt.UnixMilli() != 1_700_000_000_123 {
		t.Fatalf("local sink delivery = %+v, want decoded QoS 0 delivery", got)
	}

	remoteRuntime, remoteSessionID := newClusterForwardTestRuntime(t, 2)
	remoteSink := &clusterForwardTestSink{}
	remoteRuntime.forwardSink = remoteSink
	remoteRuntime.acceptForwardedFrame(tcpadapter.PeerInboundDelivery{
		Message: tcpadapter.PeerMessageDTO{From: 1, To: 2, Data: payload},
	})
	if len(remoteSink.calls) != 0 {
		t.Fatalf("non-owner sink calls = %d for session %q, want 0", len(remoteSink.calls), remoteSessionID)
	}

	var logOutput strings.Builder
	localRuntime.logger = slog.New(slog.NewTextHandler(&logOutput, nil))
	localRuntime.acceptForwardedFrame(tcpadapter.PeerInboundDelivery{
		Message: tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: []byte("invalid")},
	})
	if len(localSink.calls) != 1 || !strings.Contains(logOutput.String(), "drop malformed cluster forwarded delivery") {
		t.Fatalf("malformed frame sink calls=%d log=%q, want drop warning and no delivery", len(localSink.calls), logOutput.String())
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
func TestClusterForwardRemoteRejectsUnavailableAndInvalidInput(t *testing.T) {
	var nilRuntime *clusterRuntime
	handled, err := nilRuntime.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{QoS: mqtt.QoSAtMostOnce})
	if !handled || !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("nil runtime ForwardRemote() = handled %t, error %v; want handled with ErrUnavailable", handled, err)
	}

	missingMachine := &clusterRuntime{forwarder: &clusterForwardTestPeer{}}
	handled, err = missingMachine.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{QoS: mqtt.QoSAtMostOnce})
	if !handled || !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("missing machine ForwardRemote() = handled %t, error %v; want handled with ErrUnavailable", handled, err)
	}

	runtime, sessionID := newClusterForwardTestRuntime(t, 2)
	if handled, err := runtime.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "live/one", QoS: mqtt.QoSAtMostOnce,
	}); !handled || !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("missing forwarder ForwardRemote() = handled %t, error %v; want handled with ErrUnavailable", handled, err)
	}

	peer := &clusterForwardTestPeer{}
	runtime.forwarder = peer
	unownedRuntime := &clusterRuntime{
		config: runtime.config, machine: membership.NewStateMachine(), forwarder: peer,
	}
	handled, err = unownedRuntime.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "live/one", QoS: mqtt.QoSAtMostOnce,
	})
	if handled || err != nil || len(peer.calls) != 0 {
		t.Fatalf("unowned-session ForwardRemote() = handled %t, error %v, calls %d; want unhandled without error or forwarding", handled, err, len(peer.calls))
	}

	handled, err = runtime.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{
		SessionID: routing.SessionID("invalid"), Topic: "live/one", QoS: mqtt.QoSAtMostOnce,
	})
	if handled || err != nil || len(peer.calls) != 0 {
		t.Fatalf("invalid-session ForwardRemote() = handled %t, error %v, calls %d; want unhandled without error or forwarding", handled, err, len(peer.calls))
	}

	handled, err = runtime.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, QoS: mqtt.QoSAtMostOnce,
	})
	if !handled || !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) || len(peer.calls) != 0 {
		t.Fatalf("invalid-delivery ForwardRemote() = handled %t, error %v, calls %d; want ErrUnavailable without forwarding", handled, err, len(peer.calls))
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
func TestClusterForwardReceiverLogsDeliveryFailures(t *testing.T) {
	runtime, sessionID := newClusterForwardTestRuntime(t, 1)
	payload, err := tcpadapter.EncodeForwardedDelivery(tcpadapter.ForwardedDeliveryDTO{
		SessionID: string(sessionID), Topic: "live/one", Payload: []byte("value"),
	})
	if err != nil {
		t.Fatal(err)
	}

	var logOutput strings.Builder
	runtime.logger = slog.New(slog.NewTextHandler(&logOutput, nil))
	unavailableSink := &clusterForwardTestSink{err: applicationdeliveryqueue.ErrUnavailable}
	runtime.forwardSink = unavailableSink
	runtime.acceptForwardedFrame(tcpadapter.PeerInboundDelivery{
		Message: tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: payload},
	})
	if len(unavailableSink.calls) != 1 || logOutput.Len() != 0 {
		t.Fatalf("unavailable sink calls=%d log=%q; want one delivery attempt without warning", len(unavailableSink.calls), logOutput.String())
	}

	failedSink := &clusterForwardTestSink{err: errors.New("local delivery failed")}
	runtime.forwardSink = failedSink
	runtime.acceptForwardedFrame(tcpadapter.PeerInboundDelivery{
		Message: tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: payload},
	})
	if len(failedSink.calls) != 1 || !strings.Contains(logOutput.String(), "cluster forwarded delivery was not admitted") {
		t.Fatalf("failed sink calls=%d log=%q; want one delivery attempt and warning", len(failedSink.calls), logOutput.String())
	}
	if !failedSink.calls[0].ExpiresAt.IsZero() {
		t.Fatalf("forwarded delivery expiry=%v, want zero expiry", failedSink.calls[0].ExpiresAt)
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
func TestClusterForwardReceiverRejectsInvalidSessionAndMissingSink(t *testing.T) {
	runtime, sessionID := newClusterForwardTestRuntime(t, 1)
	var logOutput strings.Builder
	runtime.logger = slog.New(slog.NewTextHandler(&logOutput, nil))

	invalidSessionPayload, err := tcpadapter.EncodeForwardedDelivery(tcpadapter.ForwardedDeliveryDTO{
		SessionID: "invalid", Topic: "live/one", Payload: []byte("value"),
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime.acceptForwardedFrame(tcpadapter.PeerInboundDelivery{
		Message: tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: invalidSessionPayload},
	})
	if !strings.Contains(logOutput.String(), "drop cluster forwarded delivery with invalid session") {
		t.Fatalf("invalid-session log = %q, want identity rejection warning", logOutput.String())
	}

	logOutput.Reset()
	validSessionPayload, err := tcpadapter.EncodeForwardedDelivery(tcpadapter.ForwardedDeliveryDTO{
		SessionID: string(sessionID), Topic: "live/one", Payload: []byte("value"),
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime.acceptForwardedFrame(tcpadapter.PeerInboundDelivery{
		Message: tcpadapter.PeerMessageDTO{From: 2, To: 1, Data: validSessionPayload},
	})
	if logOutput.Len() != 0 {
		t.Fatalf("missing-sink log = %q, want silent drop when no sink is configured", logOutput.String())
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
func TestClusterForwardRemoteRejectsInvalidTopicEncoding(t *testing.T) {
	runtime, sessionID := newClusterForwardTestRuntime(t, 2)
	peer := &clusterForwardTestPeer{}
	runtime.forwarder = peer
	handled, err := runtime.ForwardRemote(context.Background(), applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: string([]byte{0xff}), QoS: mqtt.QoSAtMostOnce,
	})
	if !handled || !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) || len(peer.calls) != 0 {
		t.Fatalf("invalid-topic ForwardRemote() = handled %t, error %v, calls %d; want handled ErrUnavailable without forwarding", handled, err, len(peer.calls))
	}
}
