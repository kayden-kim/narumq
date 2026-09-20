package subscribe

import (
	"errors"
	"reflect"
	"testing"

	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

// Traceability: PRD MQTT-06/MQTT-07/MQTT-08/TEST-05; docs/specs/mqtt-protocol-and-session.md §4 항목 5.
func TestNewRejectsNilRoutingTable(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, ErrNilRoutingTable) {
		t.Fatalf("New(nil) error = %v, want %v", err, ErrNilRoutingTable)
	}
}

// Traceability: PRD MQTT-06/MQTT-07/MQTT-08/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §4 항목 1, 4, 5; MQTT-3.8.3-1/2.
func TestSubscribeKeepsOrderedReasonsAndIsolatesFailures(t *testing.T) {
	routes := routing.NewTable()
	useCase, err := New(routes)
	if err != nil {
		t.Fatal(err)
	}
	packet := mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 9,
		Subscriptions: []mqtt.Subscription{
			{Filter: "sensors/#", QoS: mqtt.QoSExactlyOnce},
			{Filter: "broken/#/filter", QoS: mqtt.QoSAtMostOnce},
			{Filter: "$share/team/sensors/#", QoS: mqtt.QoSAtLeastOnce},
			{Filter: "sensors/room", QoS: mqtt.QoSAtMostOnce, NoLocal: true},
			{Filter: "alerts/+", QoS: mqtt.QoSAtLeastOnce},
		},
	}
	ack := useCase.Subscribe("client-a", packet)
	if ack.Version != packet.Version || ack.PacketID != packet.PacketID {
		t.Fatalf("ack identity = %#v, want version %d packet id %d", ack, packet.Version, packet.PacketID)
	}
	wantReasons := []byte{0x02, 0x8F, 0x9E, 0x83, 0x01}
	if !reflect.DeepEqual(ack.ReasonCodes, wantReasons) {
		t.Fatalf("reasons = % X, want % X", ack.ReasonCodes, wantReasons)
	}

	matches, err := routes.Match("sensors/room", mqtt.QoSExactlyOnce)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(matches, []routing.Match{{SessionID: "client-a", QoS: mqtt.QoSExactlyOnce}}) {
		t.Fatalf("sensors route = %#v", matches)
	}
	matches, err = routes.Match("alerts/critical", mqtt.QoSExactlyOnce)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(matches, []routing.Match{{SessionID: "client-a", QoS: mqtt.QoSAtLeastOnce}}) {
		t.Fatalf("alerts route = %#v", matches)
	}
}

// Traceability: PRD MQTT-06/MQTT-07/TEST-03/04/05; docs/specs/mqtt-protocol-and-session.md §4 항목 6; MQTT-3.10.3-1/2 and MQTT-3.11.3-1/2.
func TestUnsubscribeReasonsAndIdempotence(t *testing.T) {
	routes := routing.NewTable()
	if err := routes.Subscribe("client-a", "sensors/#", mqtt.QoSAtLeastOnce); err != nil {
		t.Fatal(err)
	}
	useCase, err := New(routes)
	if err != nil {
		t.Fatal(err)
	}

	ack := useCase.Unsubscribe("client-a", mqtt.Unsubscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 10,
		Filters:  []string{"sensors/#", "missing/#", "broken/#/filter", "$share/team/sensors/#"},
	})
	wantReasons := []byte{0x00, 0x11, 0x8F, 0x8F}
	if !reflect.DeepEqual(ack.ReasonCodes, wantReasons) {
		t.Fatalf("reasons = % X, want % X", ack.ReasonCodes, wantReasons)
	}
	matches, err := routes.Match("sensors/room", mqtt.QoSExactlyOnce)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("route remained after unsubscribe: %#v", matches)
	}

	v311Ack := useCase.Unsubscribe("client-a", mqtt.Unsubscribe{
		Version:  mqtt.ProtocolV311,
		PacketID: 11,
		Filters:  []string{"missing/#", "broken/#/filter"},
	})
	if v311Ack.Version != mqtt.ProtocolV311 || v311Ack.PacketID != 11 || len(v311Ack.ReasonCodes) != 0 {
		t.Fatalf("MQTT 3.1.1 UNSUBACK = %#v, want packet-id-only", v311Ack)
	}
}

// Traceability: PRD MQTT-08/TEST-03/04; docs/specs/mqtt-protocol-and-session.md §4 항목 4; MQTT-3.8.3-1/2.
func TestSubscribeMQTT311RejectsUnsupportedRuntimeOptions(t *testing.T) {
	routes := routing.NewTable()
	useCase, err := New(routes)
	if err != nil {
		t.Fatal(err)
	}
	ack := useCase.Subscribe("client-a", mqtt.Subscribe{
		Version:  mqtt.ProtocolV311,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{
			{Filter: "a", QoS: mqtt.QoSAtMostOnce, RetainAsPublished: true},
			{Filter: "b", QoS: mqtt.QoSAtMostOnce},
		},
	})
	if !reflect.DeepEqual(ack.ReasonCodes, []byte{0x80, 0x00}) {
		t.Fatalf("reasons = % X, want 80 00", ack.ReasonCodes)
	}
}
func TestSubscribeMapsRoutingOperationErrorsByProtocol(t *testing.T) {
	useCase, err := New(routing.NewTable())
	if err != nil {
		t.Fatal(err)
	}
	v5 := useCase.Subscribe("", mqtt.Subscribe{Version: mqtt.ProtocolV5, Subscriptions: []mqtt.Subscription{{Filter: "topic"}, {Filter: "$share/group/topic"}}})
	if len(v5.ReasonCodes) != 2 || v5.ReasonCodes[0] != 0x8F || v5.ReasonCodes[1] != 0x9E {
		t.Fatalf("MQTT 5 routing errors = % X", v5.ReasonCodes)
	}
	v311 := useCase.Subscribe("", mqtt.Subscribe{Version: mqtt.ProtocolV311, Subscriptions: []mqtt.Subscription{{Filter: "topic"}, {Filter: "$share/group/topic"}}})
	if len(v311.ReasonCodes) != 2 || v311.ReasonCodes[0] != 0x80 || v311.ReasonCodes[1] != 0x80 {
		t.Fatalf("MQTT 3.1.1 routing errors = % X", v311.ReasonCodes)
	}
	ack := useCase.Unsubscribe("", mqtt.Unsubscribe{Version: mqtt.ProtocolV5, Filters: []string{"topic"}})
	if len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] != 0x8F {
		t.Fatalf("MQTT 5 unsubscribe routing error = % X", ack.ReasonCodes)
	}
}
