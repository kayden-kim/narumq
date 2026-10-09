package connect

import (
	"context"
	"reflect"
	"testing"
	"time"

	"narumq/internal/application/membership"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type fakeWillOutcomes struct{ reasons []string }

func (recorder *fakeWillOutcomes) WillOutcome(reason string) {
	recorder.reasons = append(recorder.reasons, reason)
}

func TestWillOutcomeClassifiers(t *testing.T) {
	for _, test := range []struct {
		origin    willOrigin
		version   mqtt.ProtocolVersion
		delay     uint32
		immediate bool
		want      string
	}{
		{willOriginUnplanned, mqtt.ProtocolV5, 5, false, portsmetrics.WillOutcomeUnplannedFailure},
		{willOriginClientRequested, mqtt.ProtocolV5, 0, false, ""},
		{willOriginPlannedHandoff, mqtt.ProtocolV311, 0, false, portsmetrics.WillOutcomeExpectedMQTT311ServerDisconnect},
		{willOriginPlannedHandoff, mqtt.ProtocolV5, 0, false, portsmetrics.WillOutcomeExpectedZeroOrExpiredWillDelay},
		{willOriginPlannedHandoff, mqtt.ProtocolV5, 5, true, portsmetrics.WillOutcomeExpectedZeroOrExpiredWillDelay},
		{willOriginPlannedHandoff, mqtt.ProtocolV5, 5, false, portsmetrics.WillOutcomeUnexpectedSupportedClient},
	} {
		if got := immediateWillReason(test.origin, test.version, test.delay, test.immediate); got != test.want {
			t.Fatalf("immediate %+v = %q", test, got)
		}
		if test.origin == willOriginPlannedHandoff {
			if got := plannedWillPublishesImmediately(test.version, test.delay, test.immediate); got != (test.want != portsmetrics.WillOutcomeUnexpectedSupportedClient) {
				t.Fatalf("immediate predicate %+v = %v", test, got)
			}
		}
	}
	for origin, want := range map[willOrigin]string{willOriginUnplanned: portsmetrics.WillOutcomeUnplannedFailure, willOriginPlannedHandoff: portsmetrics.WillOutcomeExpectedZeroOrExpiredWillDelay, willOriginClientRequested: ""} {
		if got := delayedWillReason(origin); got != want {
			t.Fatalf("delayed %d = %q", origin, got)
		}
	}
	for reason, want := range map[session.DisconnectReason]willOrigin{session.DisconnectNormal: willOriginUnplanned, session.DisconnectTakeover: willOriginUnplanned, session.DisconnectAbnormal: willOriginUnplanned, session.DisconnectWithWill: willOriginClientRequested, session.DisconnectPlannedHandoff: willOriginPlannedHandoff} {
		if got := willOriginFor(reason); got != want {
			t.Fatalf("origin %d = %d", reason, got)
		}
	}
}

// Traceability: DEP-07/DEP-12; docs/specs/drain-and-rolling-update.md §5.
func TestDisconnectRecordsWillOutcome(t *testing.T) {
	for _, test := range []struct {
		name             string
		reason           session.DisconnectReason
		version          mqtt.ProtocolVersion
		delay            uint32
		timer, reconnect bool
		want             string
		publications     int
	}{
		{"normal", session.DisconnectNormal, mqtt.ProtocolV5, 0, false, false, portsmetrics.WillOutcomeSuppressedNormalDisconnect, 0},
		{"takeover", session.DisconnectTakeover, mqtt.ProtocolV5, 0, false, false, "", 0},
		{"client requested", session.DisconnectWithWill, mqtt.ProtocolV5, 0, false, false, "", 1},
		{"abnormal311", session.DisconnectAbnormal, mqtt.ProtocolV311, 0, false, false, portsmetrics.WillOutcomeUnplannedFailure, 1},
		{"delayed", session.DisconnectAbnormal, mqtt.ProtocolV5, 5, true, false, portsmetrics.WillOutcomeUnplannedFailure, 1},
		{"reconnected", session.DisconnectAbnormal, mqtt.ProtocolV5, 5, false, true, portsmetrics.WillOutcomeSuppressedReconnectedWithinDelay, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			useCase, _, _, _, _, scheduler, publisher := newUseCase(t, time.Unix(100, 0))
			recorder := &fakeWillOutcomes{}
			WithWillOutcomeRecorder(recorder)(useCase)
			packet := mqtt.Connect{Version: test.version, ClientID: "client", SessionExpiryInterval: 60, Will: metadataWill("will/topic", test.delay)}
			result, err := useCase.Connect(packet)
			if err != nil || !result.Accepted {
				t.Fatalf("Connect = %+v %v", result, err)
			}
			if err := useCase.Disconnect(result.SessionID, test.reason); err != nil {
				t.Fatal(err)
			}
			if test.timer {
				scheduler.timers[0].callback()
			}
			if test.reconnect {
				next, err := useCase.Connect(packet)
				if err != nil || !next.Accepted {
					t.Fatalf("reconnect = %+v %v", next, err)
				}
				scheduler.timers[0].callback()
			}
			var want []string
			if test.want != "" {
				want = []string{test.want}
			}
			if !reflect.DeepEqual(recorder.reasons, want) || len(publisher.published) != test.publications {
				t.Fatalf("outcomes=%v publishes=%v want=%v/%d", recorder.reasons, publisher.published, want, test.publications)
			}
		})
	}
}

type fakeSupersededCoordinator struct {
	fakeOwnershipCoordinator
	superseded *membership.SupersededWillDTO
}

func (coordinator *fakeSupersededCoordinator) AcquireSessionOwnership(context.Context, SessionOwnershipRequestDTO) (SessionOwnershipResultDTO, error) {
	return SessionOwnershipResultDTO{Epoch: coordinator.epoch, SupersededWill: coordinator.superseded}, coordinator.err
}

// Traceability: DEP-07/DEP-12; docs/specs/cluster-session-and-delivery.md §4.3.
func TestConnectSupersededWillOutcome(t *testing.T) {
	for _, test := range []struct {
		name           string
		version        uint32
		delay          uint32
		planned, local bool
		want           string
		publications   int
	}{
		{"planned311", 4, 0, true, false, portsmetrics.WillOutcomeExpectedMQTT311ServerDisconnect, 1},
		{"planned5", 5, 5, true, false, portsmetrics.WillOutcomeSuppressedReconnectedWithinDelay, 0},
		{"scheduled local", 5, 5, false, true, portsmetrics.WillOutcomeSuppressedReconnectedWithinDelay, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			useCase, _, _, _, _, scheduler, publisher := newUseCase(t, time.Unix(100, 0))
			recorder := &fakeWillOutcomes{}
			WithWillOutcomeRecorder(recorder)(useCase)
			coordinator := &fakeSupersededCoordinator{fakeOwnershipCoordinator: fakeOwnershipCoordinator{epoch: 1}}
			WithOwnershipCoordinator(coordinator, 7)(useCase)
			packet := mqtt.Connect{Version: mqtt.ProtocolV5, ClientID: "client", SessionExpiryInterval: 60, Will: metadataWill("will/new", 5)}
			if test.local {
				first, err := useCase.Connect(packet)
				if err != nil || !first.Accepted {
					t.Fatalf("initial Connect=%+v %v", first, err)
				}
				if err := useCase.Disconnect(first.SessionID, session.DisconnectAbnormal); err != nil {
					t.Fatal(err)
				}
			}
			coordinator.epoch = 2
			coordinator.superseded = &membership.SupersededWillDTO{Will: *metadataWillDTO(metadataWill("will/old", test.delay)), ProtocolVersion: test.version, Retention: membership.SessionRetentionInfinite, PlannedHandoff: test.planned}
			result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{Packet: packet})
			if err != nil || !result.Accepted {
				t.Fatalf("Connect=%+v %v", result, err)
			}
			if test.local {
				scheduler.timers[0].callback()
			}
			if !reflect.DeepEqual(recorder.reasons, []string{test.want}) || len(publisher.published) != test.publications {
				t.Fatalf("outcomes=%v publishes=%v", recorder.reasons, publisher.published)
			}
			if test.publications == 1 && publisher.published[0].will.Topic != "will/old" {
				t.Fatal("published new Will rather than superseded Will")
			}
		})
	}
}

// Traceability: DEP-07/DEP-12, SEC-06, MQTT-01/TEST-04; MQTT-3.1.2-8; docs/specs/drain-and-rolling-update.md §5.
func TestConnectSupersededWillAuthorization(t *testing.T) {
	for _, test := range []struct {
		name             string
		topic            string
		missingPrincipal bool
		publish          bool
	}{
		{"allowed previous Will", "sensors/old", false, true},
		{"denied previous Will", "private/old", false, false},
		{"missing previous principal", "sensors/old", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			tenant, err := tenancy.NewTenantID("tenant")
			if err != nil {
				t.Fatal(err)
			}
			useCase, _, _, _, _, _, publisher := newUseCase(t, now)
			WithSecurity(&fakeAuthenticator{result: cmmAuthResult(tenant, now, "client")}, nil, cmmWillPolicy(t, tenant), "jwt")(useCase)
			recorder := &fakeWillOutcomes{}
			WithWillOutcomeRecorder(recorder)(useCase)
			superseded := &membership.SupersededWillDTO{
				Will: *metadataWillDTO(metadataWill(test.topic, 0)),
				Principal: &membership.SessionPrincipalDTO{
					Subject: "previous-device", CredentialID: "previous-credential",
					Roles: []string{"publisher"}, ExpiresAtUnixMillis: uint64(now.Add(time.Hour).UnixMilli()),
				},
				ProtocolVersion: 5, Retention: membership.SessionRetentionInfinite, PlannedHandoff: true,
			}
			if test.missingPrincipal {
				superseded.Principal = nil
			}
			coordinator := &fakeSupersededCoordinator{
				fakeOwnershipCoordinator: fakeOwnershipCoordinator{epoch: 2}, superseded: superseded,
			}
			WithOwnershipCoordinator(coordinator, 7)(useCase)
			result, err := useCase.ConnectContext(context.Background(), ConnectRequestDTO{
				Packet: cmmJWTConnect("client", 60, nil),
			})
			if err != nil || !result.Accepted {
				t.Fatalf("new CONNECT was rejected: result=%+v err=%v", result, err)
			}
			if !test.publish {
				if len(publisher.published) != 0 || len(recorder.reasons) != 0 {
					t.Fatalf("unauthorized previous Will published or counted: publishes=%+v reasons=%v", publisher.published, recorder.reasons)
				}
				return
			}
			if len(publisher.published) != 1 || publisher.published[0].will.Topic != test.topic ||
				!reflect.DeepEqual(recorder.reasons, []string{portsmetrics.WillOutcomeExpectedZeroOrExpiredWillDelay}) {
				t.Fatalf("authorized previous Will outcome: publishes=%+v reasons=%v", publisher.published, recorder.reasons)
			}
		})
	}
}
