package delivery

import (
	"errors"

	"narumq/internal/domain/mqtt"
)

var (
	// ErrInvalidQoS means that a delivery state requires QoS 1 or QoS 2.
	ErrInvalidQoS = errors.New("invalid QoS")
	// ErrInvalidTransition means that a packet is not valid in the current state.
	ErrInvalidTransition = errors.New("invalid delivery transition")
	// ErrPacketIdentifierMismatch means that a packet belongs to another exchange.
	ErrPacketIdentifierMismatch = errors.New("packet identifier mismatch")
	// ErrAcceptanceNotPending means that Accept was called without pending work.
	ErrAcceptanceNotPending = errors.New("delivery acceptance is not pending")
	// ErrStateComplete means that a completed outbound exchange was reused.
	ErrStateComplete = errors.New("delivery state is complete")
)

// Action describes the protocol packet and application handoff produced by a
// value-state transition.
type Action struct {
	// Send is the packet the caller should transmit, if any.
	Send mqtt.Packet
	// Deliver is the application message to hand off, if any.
	Deliver *mqtt.Publish
	// AwaitAcceptance requests a caller-owned durable acceptance step.
	AwaitAcceptance bool
	// Complete reports that this transition finished the exchange.
	Complete bool
	// ReasonCode is the MQTT acknowledgement reason observed or emitted.
	ReasonCode byte
}

type outboundPhase byte

const (
	outboundInvalid outboundPhase = iota
	outboundAwaitPubAck
	outboundAwaitPubRec
	outboundAwaitPubComp
	outboundComplete
)

// Outbound is a value-owned sender-side QoS 1 or QoS 2 exchange.
type Outbound struct {
	publish mqtt.Publish
	phase   outboundPhase
}

// NewOutbound creates a sender state and its first non-duplicate PUBLISH action.
func NewOutbound(publish mqtt.Publish) (Outbound, Action, error) {
	if err := validateQoSPublish(publish); err != nil {
		return Outbound{}, Action{}, err
	}
	publish.DUP = false
	publish.Payload = cloneBytes(publish.Payload)
	state := Outbound{publish: publish}
	if publish.QoS == mqtt.QoSAtLeastOnce {
		state.phase = outboundAwaitPubAck
	} else {
		state.phase = outboundAwaitPubRec
	}
	return state, Action{Send: outboundPublish(state.publish, false)}, nil
}

// Receive applies the next acknowledgement to an outbound exchange.
func (state Outbound) Receive(packet mqtt.Packet) (Outbound, Action, error) {
	if state.phase == outboundComplete {
		return state, Action{}, ErrStateComplete
	}
	if state.phase == outboundInvalid {
		return state, Action{}, ErrInvalidTransition
	}
	packetID, ok := packetIdentifier(packet)
	if !ok {
		return state, Action{}, ErrInvalidTransition
	}
	if packetID != state.publish.PacketID {
		return state, Action{}, ErrPacketIdentifierMismatch
	}

	switch state.phase {
	case outboundAwaitPubAck:
		ack, ok := pubAckValue(packet)
		if !ok {
			return state, Action{}, ErrInvalidTransition
		}
		state.phase = outboundComplete
		return state, Action{Complete: true, ReasonCode: ack.ReasonCode}, nil
	case outboundAwaitPubRec:
		ack, ok := pubRecValue(packet)
		if !ok {
			return state, Action{}, ErrInvalidTransition
		}
		if ack.ReasonCode >= 0x80 {
			state.phase = outboundComplete
			return state, Action{Complete: true, ReasonCode: ack.ReasonCode}, nil
		}
		state.phase = outboundAwaitPubComp
		return state, Action{
			Send:       &mqtt.PubRel{Version: state.publish.Version, PacketID: state.publish.PacketID},
			ReasonCode: ack.ReasonCode,
		}, nil
	case outboundAwaitPubComp:
		ack, ok := pubCompValue(packet)
		if !ok {
			return state, Action{}, ErrInvalidTransition
		}
		state.phase = outboundComplete
		return state, Action{Complete: true, ReasonCode: ack.ReasonCode}, nil
	default:
		return state, Action{}, ErrInvalidTransition
	}
}

// Resume returns the canonical retransmission for an incomplete exchange.
func (state Outbound) Resume() (Action, error) {
	switch state.phase {
	case outboundAwaitPubAck, outboundAwaitPubRec:
		return Action{Send: outboundPublish(state.publish, true)}, nil
	case outboundAwaitPubComp:
		return Action{Send: &mqtt.PubRel{Version: state.publish.Version, PacketID: state.publish.PacketID}}, nil
	case outboundComplete:
		return Action{}, ErrStateComplete
	default:
		return Action{}, ErrInvalidTransition
	}
}

type inboundPhase byte

const (
	inboundInvalid inboundPhase = iota
	inboundQoS1AwaitAcceptance
	inboundQoS1Accepted
	inboundQoS2AwaitAcceptance
	inboundQoS2AwaitRelease
	inboundQoS2AwaitFinalAcceptance
	inboundQoS2Complete
)

// Inbound is a value-owned receiver-side QoS 1 or QoS 2 exchange.
type Inbound struct {
	publish mqtt.Publish
	phase   inboundPhase
}

// NewInbound creates a receiver state and the initial application handoff.
func NewInbound(publish mqtt.Publish) (Inbound, Action, error) {
	if err := validateQoSPublish(publish); err != nil {
		return Inbound{}, Action{}, err
	}
	publish.Payload = cloneBytes(publish.Payload)
	state := Inbound{publish: publish}
	if publish.QoS == mqtt.QoSAtLeastOnce {
		state.phase = inboundQoS1AwaitAcceptance
		return state, Action{Deliver: inboundPublish(state.publish), AwaitAcceptance: true}, nil
	}
	state.phase = inboundQoS2AwaitAcceptance
	return state, Action{AwaitAcceptance: true}, nil
}

// Accept records a caller-owned successful handoff and emits its next ACK.
func (state Inbound) Accept() (Inbound, Action, error) {
	switch state.phase {
	case inboundQoS1AwaitAcceptance:
		state.phase = inboundQoS1Accepted
		return state, Action{
			Send:       &mqtt.PubAck{Version: state.publish.Version, PacketID: state.publish.PacketID},
			Complete:   true,
			ReasonCode: 0,
		}, nil
	case inboundQoS2AwaitAcceptance:
		state.phase = inboundQoS2AwaitRelease
		return state, Action{
			Send:       &mqtt.PubRec{Version: state.publish.Version, PacketID: state.publish.PacketID},
			ReasonCode: 0,
		}, nil
	case inboundQoS2AwaitFinalAcceptance:
		state.phase = inboundQoS2Complete
		return state, Action{
			Send:       &mqtt.PubComp{Version: state.publish.Version, PacketID: state.publish.PacketID},
			Complete:   true,
			ReasonCode: 0,
		}, nil
	case inboundInvalid:
		return state, Action{}, ErrInvalidTransition
	default:
		return state, Action{}, ErrAcceptanceNotPending
	}
}

// Receive applies a duplicate PUBLISH or the next QoS 2 PUBREL.
func (state Inbound) Receive(packet mqtt.Packet) (Inbound, Action, error) {
	if state.phase == inboundInvalid {
		return state, Action{}, ErrInvalidTransition
	}
	packetID, ok := packetIdentifier(packet)
	if !ok {
		return state, Action{}, ErrInvalidTransition
	}
	if packetID != state.publish.PacketID {
		return state, Action{}, ErrPacketIdentifierMismatch
	}

	switch state.phase {
	case inboundQoS1AwaitAcceptance:
		if _, ok := publishValue(packet); !ok {
			return state, Action{}, ErrInvalidTransition
		}
		return state, Action{AwaitAcceptance: true}, nil
	case inboundQoS1Accepted:
		if _, ok := publishValue(packet); !ok {
			return state, Action{}, ErrInvalidTransition
		}
		return state, Action{
			Send:       &mqtt.PubAck{Version: state.publish.Version, PacketID: state.publish.PacketID},
			Complete:   true,
			ReasonCode: 0,
		}, nil
	case inboundQoS2AwaitAcceptance:
		if _, ok := publishValue(packet); !ok {
			return state, Action{}, ErrInvalidTransition
		}
		return state, Action{AwaitAcceptance: true}, nil
	case inboundQoS2AwaitRelease:
		if _, ok := publishValue(packet); ok {
			return state, Action{Send: &mqtt.PubRec{Version: state.publish.Version, PacketID: state.publish.PacketID}}, nil
		}
		if _, ok := pubRelValue(packet); !ok {
			return state, Action{}, ErrInvalidTransition
		}
		state.phase = inboundQoS2AwaitFinalAcceptance
		return state, Action{Deliver: inboundPublish(state.publish), AwaitAcceptance: true}, nil
	case inboundQoS2AwaitFinalAcceptance:
		if _, ok := publishValue(packet); ok {
			return state, Action{Send: &mqtt.PubRec{Version: state.publish.Version, PacketID: state.publish.PacketID}}, nil
		}
		if _, ok := pubRelValue(packet); !ok {
			return state, Action{}, ErrInvalidTransition
		}
		return state, Action{AwaitAcceptance: true}, nil
	case inboundQoS2Complete:
		if _, ok := pubRelValue(packet); !ok {
			return state, Action{}, ErrStateComplete
		}
		return state, Action{
			Send:       &mqtt.PubComp{Version: state.publish.Version, PacketID: state.publish.PacketID},
			Complete:   true,
			ReasonCode: 0,
		}, nil
	default:
		return state, Action{}, ErrInvalidTransition
	}
}

func validateQoSPublish(publish mqtt.Publish) error {
	if publish.QoS != mqtt.QoSAtLeastOnce && publish.QoS != mqtt.QoSExactlyOnce {
		return ErrInvalidQoS
	}
	if publish.PacketID == 0 {
		return ErrPacketIdentifierMismatch
	}
	return nil
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	copyOfValue := make([]byte, len(value))
	copy(copyOfValue, value)
	return copyOfValue
}

func outboundPublish(publish mqtt.Publish, duplicate bool) *mqtt.Publish {
	value := publish
	value.DUP = duplicate
	value.Payload = cloneBytes(publish.Payload)
	return &value
}

func inboundPublish(publish mqtt.Publish) *mqtt.Publish {
	value := publish
	value.Payload = cloneBytes(publish.Payload)
	return &value
}

func packetIdentifier(packet mqtt.Packet) (mqtt.PacketIdentifier, bool) {
	switch value := packet.(type) {
	case mqtt.Publish:
		return value.PacketID, true
	case *mqtt.Publish:
		if value == nil {
			return 0, false
		}
		return value.PacketID, true
	case mqtt.PubAck:
		return value.PacketID, true
	case *mqtt.PubAck:
		if value == nil {
			return 0, false
		}
		return value.PacketID, true
	case mqtt.PubRec:
		return value.PacketID, true
	case *mqtt.PubRec:
		if value == nil {
			return 0, false
		}
		return value.PacketID, true
	case mqtt.PubRel:
		return value.PacketID, true
	case *mqtt.PubRel:
		if value == nil {
			return 0, false
		}
		return value.PacketID, true
	case mqtt.PubComp:
		return value.PacketID, true
	case *mqtt.PubComp:
		if value == nil {
			return 0, false
		}
		return value.PacketID, true
	default:
		return 0, false
	}
}

func publishValue(packet mqtt.Packet) (mqtt.Publish, bool) {
	switch value := packet.(type) {
	case mqtt.Publish:
		return value, true
	case *mqtt.Publish:
		if value == nil {
			return mqtt.Publish{}, false
		}
		return *value, true
	default:
		return mqtt.Publish{}, false
	}
}

func pubAckValue(packet mqtt.Packet) (mqtt.PubAck, bool) {
	switch value := packet.(type) {
	case mqtt.PubAck:
		return value, true
	case *mqtt.PubAck:
		if value == nil {
			return mqtt.PubAck{}, false
		}
		return *value, true
	default:
		return mqtt.PubAck{}, false
	}
}

func pubRecValue(packet mqtt.Packet) (mqtt.PubRec, bool) {
	switch value := packet.(type) {
	case mqtt.PubRec:
		return value, true
	case *mqtt.PubRec:
		if value == nil {
			return mqtt.PubRec{}, false
		}
		return *value, true
	default:
		return mqtt.PubRec{}, false
	}
}

func pubRelValue(packet mqtt.Packet) (mqtt.PubRel, bool) {
	switch value := packet.(type) {
	case mqtt.PubRel:
		return value, true
	case *mqtt.PubRel:
		if value == nil {
			return mqtt.PubRel{}, false
		}
		return *value, true
	default:
		return mqtt.PubRel{}, false
	}
}

func pubCompValue(packet mqtt.Packet) (mqtt.PubComp, bool) {
	switch value := packet.(type) {
	case mqtt.PubComp:
		return value, true
	case *mqtt.PubComp:
		if value == nil {
			return mqtt.PubComp{}, false
		}
		return *value, true
	default:
		return mqtt.PubComp{}, false
	}
}
