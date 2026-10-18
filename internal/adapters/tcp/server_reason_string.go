package tcp

import "narumq/internal/domain/mqtt"

// withoutReasonString returns a copy of a PUBACK or PUBREC without its Reason String.
// The boolean is false when the packet carries no Reason String.
func withoutReasonString(packet mqtt.Packet) (mqtt.Packet, bool) {
	switch ack := packet.(type) {
	case *mqtt.PubAck:
		if ack.ReasonString != "" {
			bare := *ack
			bare.ReasonString = ""
			return &bare, true
		}
	case *mqtt.PubRec:
		if ack.ReasonString != "" {
			bare := *ack
			bare.ReasonString = ""
			return &bare, true
		}
	}
	return nil, false
}
