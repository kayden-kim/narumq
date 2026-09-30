package delivery

import (
	"bytes"
	"errors"
	"testing"

	"narumq/internal/domain/mqtt"
)

// Traceability: PRD MQTT-02/TEST-03/05; MQTT-4.3.2-1/2.
func TestOutboundQoS1HandshakeResumeAndErrors(t *testing.T) {
	input := mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 7, Topic: "orders", Payload: []byte("payload")}
	state, action, err := NewOutbound(input)
	if err != nil {
		t.Fatal(err)
	}
	sent := requirePublish(t, action.Send)
	if sent.DUP || sent.PacketID != input.PacketID || !bytes.Equal(sent.Payload, input.Payload) {
		t.Fatalf("initial action = %#v, want non-duplicate original publish", sent)
	}
	input.Payload[0] = 'X'
	sent.Payload[0] = 'Y'
	resumed, err := state.Resume()
	if err != nil {
		t.Fatal(err)
	}
	duplicate := requirePublish(t, resumed.Send)
	if !duplicate.DUP || !bytes.Equal(duplicate.Payload, []byte("payload")) {
		t.Fatalf("Resume() = %#v, want independent DUP payload", duplicate)
	}

	if _, _, err := state.Receive(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: input.PacketID}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("wrong ACK error = %v, want ErrInvalidTransition", err)
	}
	if _, _, err := state.Receive(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: 8}); !errors.Is(err, ErrPacketIdentifierMismatch) {
		t.Fatalf("mismatch error = %v, want ErrPacketIdentifierMismatch", err)
	}
	state, action, err = state.Receive(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: 7, ReasonCode: 0x10})
	if err != nil || !action.Complete || action.ReasonCode != 0x10 || action.Send != nil {
		t.Fatalf("PUBACK transition = (%#v, %#v, %v)", state, action, err)
	}
	if _, err := state.Resume(); !errors.Is(err, ErrStateComplete) {
		t.Fatalf("completed Resume() error = %v, want ErrStateComplete", err)
	}
}

// Traceability: PRD MQTT-02/TEST-04/05; MQTT-4.3.3-1/2.
func TestOutboundQoS2SuccessFailureAndResume(t *testing.T) {
	input := mqtt.Publish{Version: mqtt.ProtocolV311, QoS: mqtt.QoSExactlyOnce, PacketID: 11, Topic: "orders", Payload: []byte("payload")}
	state, action, err := NewOutbound(input)
	if err != nil {
		t.Fatal(err)
	}
	if actionSend := requirePublish(t, action.Send); actionSend.DUP {
		t.Fatal("initial QoS 2 PUBLISH unexpectedly marked DUP")
	}
	resume, err := state.Resume()
	if err != nil || !requirePublish(t, resume.Send).DUP {
		t.Fatalf("QoS 2 Resume() = (%#v, %v)", resume, err)
	}
	state, action, err = state.Receive(&mqtt.PubRec{Version: mqtt.ProtocolV311, PacketID: 11})
	if err != nil || action.Send == nil {
		t.Fatalf("PUBREC transition = (%#v, %#v, %v)", state, action, err)
	}
	if rel := requirePubRel(t, action.Send); rel.PacketID != 11 || rel.ReasonCode != 0 {
		t.Fatalf("PUBREL = %#v", rel)
	}
	resume, err = state.Resume()
	if err != nil || requirePubRel(t, resume.Send).PacketID != 11 {
		t.Fatalf("PUBREL Resume() = (%#v, %v)", resume, err)
	}
	state, action, err = state.Receive(&mqtt.PubComp{Version: mqtt.ProtocolV311, PacketID: 11, ReasonCode: 0x92})
	if err != nil || !action.Complete || action.ReasonCode != 0x92 {
		t.Fatalf("PUBCOMP transition = (%#v, %#v, %v)", state, action, err)
	}

	failureState, failureAction, err := NewOutbound(mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSExactlyOnce, PacketID: 12, Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if failureAction.Send == nil {
		t.Fatal("failure scenario missing initial PUBLISH")
	}
	failureState, failureAction, err = failureState.Receive(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: 12, ReasonCode: 0x80})
	if err != nil || !failureAction.Complete || failureAction.Send != nil || failureAction.ReasonCode != 0x80 {
		t.Fatalf("failure PUBREC transition = (%#v, %#v, %v)", failureState, failureAction, err)
	}
}

// Traceability: PRD MQTT-02; invalid QoS and Packet Identifier boundaries.
func TestOutboundAndInboundRejectInvalidConstructors(t *testing.T) {
	for _, publish := range []mqtt.Publish{
		{Version: mqtt.ProtocolV5, Topic: "topic"},
		{Version: mqtt.ProtocolV5, QoS: mqtt.QoS(3), PacketID: 1, Topic: "topic"},
		{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, Topic: "topic"},
	} {
		if _, _, err := NewOutbound(publish); !errors.Is(err, expectedDeliveryError(publish)) {
			t.Fatalf("NewOutbound(%#v) error = %v", publish, err)
		}
		if _, _, err := NewInbound(publish); !errors.Is(err, expectedDeliveryError(publish)) {
			t.Fatalf("NewInbound(%#v) error = %v", publish, err)
		}
	}
}

// Traceability: PRD MQTT-02/TEST-03/04; MQTT-4.3.2-1/2.
func TestInboundQoS1AcceptanceAndDuplicateConvergence(t *testing.T) {
	input := mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 21, Topic: "orders", Payload: []byte("payload")}
	state, action, err := NewInbound(input)
	if err != nil {
		t.Fatal(err)
	}
	if !action.AwaitAcceptance || action.Deliver == nil || action.Send != nil {
		t.Fatalf("initial inbound action = %#v", action)
	}
	action.Deliver.Payload[0] = 'X'
	_, duplicateBefore, err := state.Receive(&mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 21, Topic: "orders", Payload: []byte("payload")})
	if err != nil || !duplicateBefore.AwaitAcceptance || duplicateBefore.Deliver != nil || duplicateBefore.Send != nil {
		t.Fatalf("pre-accept duplicate = %#v, %v", duplicateBefore, err)
	}
	state, action, err = state.Accept()
	if err != nil || !action.Complete || requirePubAck(t, action.Send).PacketID != 21 {
		t.Fatalf("acceptance = (%#v, %#v, %v)", state, action, err)
	}
	_, duplicateAfter, err := state.Receive(&mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 21, Topic: "orders", Payload: []byte("payload")})
	if err != nil || duplicateAfter.Deliver != nil || requirePubAck(t, duplicateAfter.Send).PacketID != 21 {
		t.Fatalf("post-accept duplicate = (%#v, %v)", duplicateAfter, err)
	}
	if _, _, err := state.Accept(); !errors.Is(err, ErrAcceptanceNotPending) {
		t.Fatalf("second QoS 1 Accept() error = %v, want ErrAcceptanceNotPending", err)
	}
	if _, _, err := state.Receive(&mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 22, Topic: "orders"}); !errors.Is(err, ErrPacketIdentifierMismatch) {
		t.Fatalf("QoS 1 mismatch error = %v, want ErrPacketIdentifierMismatch", err)
	}
}

// Traceability: PRD MQTT-02/TEST-04/05; MQTT-4.3.3-1/2.
func TestInboundQoS2DurableHandoffAndDuplicateConvergence(t *testing.T) {
	input := mqtt.Publish{Version: mqtt.ProtocolV311, QoS: mqtt.QoSExactlyOnce, PacketID: 31, Topic: "orders", Payload: []byte("payload")}
	state, action, err := NewInbound(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Payload[0] = 'X'
	if !action.AwaitAcceptance || action.Deliver != nil || action.Send != nil {
		t.Fatalf("initial QoS 2 action = %#v", action)
	}
	_, duplicateBefore, err := state.Receive(&mqtt.Publish{Version: mqtt.ProtocolV311, QoS: mqtt.QoSExactlyOnce, PacketID: 31, Topic: "orders", Payload: []byte("payload")})
	if err != nil || !duplicateBefore.AwaitAcceptance || duplicateBefore.Deliver != nil {
		t.Fatalf("pre-accept QoS 2 duplicate = (%#v, %v)", duplicateBefore, err)
	}
	state, action, err = state.Accept()
	if err != nil || requirePubRec(t, action.Send).PacketID != 31 || action.Complete {
		t.Fatalf("first acceptance = (%#v, %#v, %v)", state, action, err)
	}
	_, duplicatePublish, err := state.Receive(&mqtt.Publish{Version: mqtt.ProtocolV311, QoS: mqtt.QoSExactlyOnce, PacketID: 31, Topic: "orders", Payload: []byte("payload")})
	if err != nil || duplicatePublish.Deliver != nil || requirePubRec(t, duplicatePublish.Send).PacketID != 31 {
		t.Fatalf("post-PUBREC duplicate = (%#v, %v)", duplicatePublish, err)
	}
	state, action, err = state.Receive(&mqtt.PubRel{Version: mqtt.ProtocolV311, PacketID: 31})
	if err != nil || !action.AwaitAcceptance || action.Deliver == nil || action.Send != nil {
		t.Fatalf("PUBREL handoff = (%#v, %#v, %v)", state, action, err)
	}
	if !bytes.Equal(action.Deliver.Payload, []byte("payload")) {
		t.Fatalf("PUBREL payload = %q, want payload", action.Deliver.Payload)
	}
	_, duplicateFinalPublish, err := state.Receive(&mqtt.Publish{Version: mqtt.ProtocolV311, QoS: mqtt.QoSExactlyOnce, PacketID: 31, Topic: "orders", Payload: []byte("payload")})
	if err != nil || duplicateFinalPublish.Deliver != nil || requirePubRec(t, duplicateFinalPublish.Send).PacketID != 31 {
		t.Fatalf("final-acceptance duplicate PUBLISH = (%#v, %v)", duplicateFinalPublish, err)
	}
	_, duplicateRelease, err := state.Receive(&mqtt.PubRel{Version: mqtt.ProtocolV311, PacketID: 31})
	if err != nil || !duplicateRelease.AwaitAcceptance || duplicateRelease.Deliver != nil || duplicateRelease.Send != nil {
		t.Fatalf("pre-final duplicate PUBREL = (%#v, %v)", duplicateRelease, err)
	}
	state, action, err = state.Accept()
	if err != nil || !action.Complete || requirePubComp(t, action.Send).PacketID != 31 {
		t.Fatalf("second acceptance = (%#v, %#v, %v)", state, action, err)
	}
	_, duplicateComplete, err := state.Receive(&mqtt.PubRel{Version: mqtt.ProtocolV311, PacketID: 31})
	if err != nil || !duplicateComplete.Complete || requirePubComp(t, duplicateComplete.Send).PacketID != 31 {
		t.Fatalf("post-complete duplicate PUBREL = (%#v, %v)", duplicateComplete, err)
	}
	if _, _, err := state.Receive(&mqtt.PubAck{Version: mqtt.ProtocolV311, PacketID: 31}); !errors.Is(err, ErrStateComplete) {
		t.Fatalf("completed wrong packet error = %v, want ErrStateComplete", err)
	}
}

func TestInboundIsCompleteTracksFinalAcknowledgement(t *testing.T) {
	qos1, _, err := NewInbound(mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 1, Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if qos1.IsComplete() {
		t.Fatal("new QoS 1 exchange is complete")
	}
	qos1, _, err = qos1.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if !qos1.IsComplete() {
		t.Fatal("accepted QoS 1 exchange is not complete")
	}

	qos2, _, err := NewInbound(mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSExactlyOnce, PacketID: 2, Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if qos2.IsComplete() {
		t.Fatal("new QoS 2 exchange is complete")
	}
	qos2, _, err = qos2.Accept()
	if err != nil {
		t.Fatal(err)
	}
	qos2, _, err = qos2.Receive(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 2})
	if err != nil {
		t.Fatal(err)
	}
	if qos2.IsComplete() {
		t.Fatal("QoS 2 exchange completed before PUBCOMP")
	}
	qos2, _, err = qos2.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if !qos2.IsComplete() {
		t.Fatal("QoS 2 exchange is not complete after PUBCOMP")
	}
}

// Traceability: PRD MQTT-02; packet value/pointer ownership and invalid transition edges.
func TestPacketHelpersValuePointerAndNilBranches(t *testing.T) {
	packetCases := []struct {
		name   string
		packet mqtt.Packet
		id     mqtt.PacketIdentifier
		ok     bool
	}{
		{name: "publish-value", packet: mqtt.Publish{PacketID: 1}, id: 1, ok: true},
		{name: "publish-pointer", packet: &mqtt.Publish{PacketID: 2}, id: 2, ok: true},
		{name: "publish-nil", packet: (*mqtt.Publish)(nil), ok: false},
		{name: "puback-value", packet: mqtt.PubAck{PacketID: 3}, id: 3, ok: true},
		{name: "puback-pointer", packet: &mqtt.PubAck{PacketID: 4}, id: 4, ok: true},
		{name: "puback-nil", packet: (*mqtt.PubAck)(nil), ok: false},
		{name: "pubrec-value", packet: mqtt.PubRec{PacketID: 5}, id: 5, ok: true},
		{name: "pubrec-pointer", packet: &mqtt.PubRec{PacketID: 6}, id: 6, ok: true},
		{name: "pubrec-nil", packet: (*mqtt.PubRec)(nil), ok: false},
		{name: "pubrel-value", packet: mqtt.PubRel{PacketID: 7}, id: 7, ok: true},
		{name: "pubrel-pointer", packet: &mqtt.PubRel{PacketID: 8}, id: 8, ok: true},
		{name: "pubrel-nil", packet: (*mqtt.PubRel)(nil), ok: false},
		{name: "pubcomp-value", packet: mqtt.PubComp{PacketID: 9}, id: 9, ok: true},
		{name: "pubcomp-pointer", packet: &mqtt.PubComp{PacketID: 10}, id: 10, ok: true},
		{name: "pubcomp-nil", packet: (*mqtt.PubComp)(nil), ok: false},
		{name: "unsupported", packet: mqtt.Connect{}, ok: false},
	}
	for _, testCase := range packetCases {
		t.Run("packetIdentifier/"+testCase.name, func(t *testing.T) {
			got, ok := packetIdentifier(testCase.packet)
			if ok != testCase.ok || (ok && got != testCase.id) {
				t.Fatalf("packetIdentifier() = (%d, %v), want (%d, %v)", got, ok, testCase.id, testCase.ok)
			}
		})
	}

	publishCases := []struct {
		name   string
		packet mqtt.Packet
		ok     bool
	}{
		{name: "value", packet: mqtt.Publish{}, ok: true},
		{name: "pointer", packet: &mqtt.Publish{}, ok: true},
		{name: "nil", packet: (*mqtt.Publish)(nil), ok: false},
		{name: "unsupported", packet: mqtt.Connect{}, ok: false},
	}
	for _, testCase := range publishCases {
		t.Run("publishValue/"+testCase.name, func(t *testing.T) {
			if _, ok := publishValue(testCase.packet); ok != testCase.ok {
				t.Fatalf("publishValue() ok = %v, want %v", ok, testCase.ok)
			}
		})
	}

	ackCases := []struct {
		name   string
		packet mqtt.Packet
		ok     bool
	}{
		{name: "puback-value", packet: mqtt.PubAck{}, ok: true},
		{name: "puback-pointer", packet: &mqtt.PubAck{}, ok: true},
		{name: "puback-nil", packet: (*mqtt.PubAck)(nil), ok: false},
		{name: "pubrec-value", packet: mqtt.PubRec{}, ok: true},
		{name: "pubrec-pointer", packet: &mqtt.PubRec{}, ok: true},
		{name: "pubrec-nil", packet: (*mqtt.PubRec)(nil), ok: false},
		{name: "pubrel-value", packet: mqtt.PubRel{}, ok: true},
		{name: "pubrel-pointer", packet: &mqtt.PubRel{}, ok: true},
		{name: "pubrel-nil", packet: (*mqtt.PubRel)(nil), ok: false},
		{name: "pubcomp-value", packet: mqtt.PubComp{}, ok: true},
		{name: "pubcomp-pointer", packet: &mqtt.PubComp{}, ok: true},
		{name: "pubcomp-nil", packet: (*mqtt.PubComp)(nil), ok: false},
		{name: "unsupported", packet: mqtt.Connect{}, ok: false},
	}
	for _, testCase := range ackCases {
		t.Run("pubAckValue/"+testCase.name, func(t *testing.T) {
			_, ok := pubAckValue(testCase.packet)
			want := testCase.ok && testCase.name[:6] == "puback"
			if ok != want {
				t.Fatalf("pubAckValue() ok = %v, want %v", ok, want)
			}
		})
		t.Run("pubRecValue/"+testCase.name, func(t *testing.T) {
			_, ok := pubRecValue(testCase.packet)
			want := testCase.ok && testCase.name[:6] == "pubrec"
			if ok != want {
				t.Fatalf("pubRecValue() ok = %v, want %v", ok, want)
			}
		})
		t.Run("pubRelValue/"+testCase.name, func(t *testing.T) {
			_, ok := pubRelValue(testCase.packet)
			want := testCase.ok && testCase.name[:6] == "pubrel"
			if ok != want {
				t.Fatalf("pubRelValue() ok = %v, want %v", ok, want)
			}
		})
		t.Run("pubCompValue/"+testCase.name, func(t *testing.T) {
			_, ok := pubCompValue(testCase.packet)
			want := testCase.ok && testCase.name[:7] == "pubcomp"
			if ok != want {
				t.Fatalf("pubCompValue() ok = %v, want %v", ok, want)
			}
		})
	}
}

// Traceability: PRD MQTT-02; invalid packet order and state boundaries.
func TestInvalidDeliveryStateTransitions(t *testing.T) {
	publish := mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 41, Topic: "orders"}
	if _, _, err := (Outbound{}).Receive(&mqtt.PubAck{PacketID: 41}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("zero Outbound Receive() error = %v", err)
	}
	if _, err := (Outbound{}).Resume(); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("zero Outbound Resume() error = %v", err)
	}
	if _, _, err := (Outbound{publish: publish, phase: outboundPhase(99)}).Receive(&mqtt.PubAck{PacketID: 41}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unknown Outbound phase error = %v", err)
	}
	if _, _, err := (Outbound{publish: publish, phase: outboundComplete}).Receive(&mqtt.PubAck{PacketID: 41}); !errors.Is(err, ErrStateComplete) {
		t.Fatalf("complete Outbound Receive() error = %v", err)
	}

	state, _, err := NewOutbound(publish)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.Receive(&mqtt.Connect{ClientID: "wrong-packet"}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Outbound unsupported packet error = %v", err)
	}
	qos2, _, err := NewOutbound(mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSExactlyOnce, PacketID: 42, Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := qos2.Receive(&mqtt.PubAck{PacketID: 42}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Outbound PUBREC phase wrong ACK error = %v", err)
	}
	qos2, _, err = qos2.Receive(&mqtt.PubRec{PacketID: 42})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := qos2.Receive(&mqtt.PubAck{PacketID: 42}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Outbound PUBCOMP phase wrong ACK error = %v", err)
	}

	inboundPublish := mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 43, Topic: "orders"}
	if _, _, err := (Inbound{}).Accept(); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("zero Inbound Accept() error = %v", err)
	}
	if _, _, err := (Inbound{}).Receive(&mqtt.PubAck{PacketID: 43}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("zero Inbound Receive() error = %v", err)
	}
	if _, _, err := (Inbound{publish: inboundPublish, phase: inboundQoS1AwaitAcceptance}).Receive(&mqtt.Connect{ClientID: "wrong-packet"}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Inbound unsupported packet error = %v", err)
	}
	invalidInboundPhases := []inboundPhase{
		inboundQoS1AwaitAcceptance,
		inboundQoS1Accepted,
		inboundQoS2AwaitAcceptance,
		inboundQoS2AwaitRelease,
		inboundQoS2AwaitFinalAcceptance,
	}
	for _, phase := range invalidInboundPhases {
		state := Inbound{publish: inboundPublish, phase: phase}
		if _, _, err := state.Receive(&mqtt.PubAck{PacketID: 43}); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("Inbound phase %d wrong packet error = %v", phase, err)
		}
	}
	if _, _, err := (Inbound{publish: inboundPublish, phase: inboundPhase(99)}).Receive(&mqtt.PubAck{PacketID: 43}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unknown Inbound phase error = %v", err)
	}
	if _, _, err := (Inbound{publish: inboundPublish, phase: inboundQoS2Complete}).Receive(&mqtt.PubAck{PacketID: 43}); !errors.Is(err, ErrStateComplete) {
		t.Fatalf("complete Inbound wrong packet error = %v", err)
	}
}

func expectedDeliveryError(publish mqtt.Publish) error {
	if publish.QoS != mqtt.QoSAtLeastOnce && publish.QoS != mqtt.QoSExactlyOnce {
		return ErrInvalidQoS
	}
	return ErrPacketIdentifierMismatch
}

func requirePublish(t *testing.T, packet mqtt.Packet) *mqtt.Publish {
	t.Helper()
	value, ok := packet.(*mqtt.Publish)
	if !ok || value == nil {
		t.Fatalf("packet = %T, want *mqtt.Publish", packet)
	}
	return value
}

func requirePubAck(t *testing.T, packet mqtt.Packet) *mqtt.PubAck {
	t.Helper()
	value, ok := packet.(*mqtt.PubAck)
	if !ok || value == nil {
		t.Fatalf("packet = %T, want *mqtt.PubAck", packet)
	}
	return value
}

func requirePubRec(t *testing.T, packet mqtt.Packet) *mqtt.PubRec {
	t.Helper()
	value, ok := packet.(*mqtt.PubRec)
	if !ok || value == nil {
		t.Fatalf("packet = %T, want *mqtt.PubRec", packet)
	}
	return value
}

func requirePubRel(t *testing.T, packet mqtt.Packet) *mqtt.PubRel {
	t.Helper()
	value, ok := packet.(*mqtt.PubRel)
	if !ok || value == nil {
		t.Fatalf("packet = %T, want *mqtt.PubRel", packet)
	}
	return value
}

func requirePubComp(t *testing.T, packet mqtt.Packet) *mqtt.PubComp {
	t.Helper()
	value, ok := packet.(*mqtt.PubComp)
	if !ok || value == nil {
		t.Fatalf("packet = %T, want *mqtt.PubComp", packet)
	}
	return value
}
