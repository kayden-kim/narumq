package mqtt

// ProtocolVersion identifies the MQTT protocol level used by a packet.
type ProtocolVersion byte

const (
	// ProtocolUnknown means that the protocol must be discovered from CONNECT.
	ProtocolUnknown ProtocolVersion = 0
	// ProtocolV311 identifies MQTT 3.1.1, protocol level 4.
	ProtocolV311 ProtocolVersion = 4
	// ProtocolV5 identifies MQTT 5.0, protocol level 5.
	ProtocolV5 ProtocolVersion = 5
)

// PacketType identifies the MQTT control packet family.
type PacketType byte

const (
	// PacketTypeConnect identifies a CONNECT packet.
	PacketTypeConnect PacketType = 1
	// PacketTypeConnAck identifies a CONNACK packet.
	PacketTypeConnAck PacketType = 2
	// PacketTypePublish identifies a PUBLISH packet.
	PacketTypePublish PacketType = 3
	// PacketTypePubAck identifies a PUBACK packet.
	PacketTypePubAck PacketType = 4
	// PacketTypePubRec identifies a PUBREC packet.
	PacketTypePubRec PacketType = 5
	// PacketTypePubRel identifies a PUBREL packet.
	PacketTypePubRel PacketType = 6
	// PacketTypePubComp identifies a PUBCOMP packet.
	PacketTypePubComp PacketType = 7
	// PacketTypeSubscribe identifies a SUBSCRIBE packet.
	PacketTypeSubscribe PacketType = 8
	// PacketTypeSubAck identifies a SUBACK packet.
	PacketTypeSubAck PacketType = 9
	// PacketTypeUnsubscribe identifies an UNSUBSCRIBE packet.
	PacketTypeUnsubscribe PacketType = 10
	// PacketTypeUnsubAck identifies an UNSUBACK packet.
	PacketTypeUnsubAck PacketType = 11
)

// QoS identifies the MQTT delivery guarantee requested by a PUBLISH.
type QoS byte

const (
	// QoSAtMostOnce sends a PUBLISH without an acknowledgement exchange.
	QoSAtMostOnce QoS = 0
	// QoSAtLeastOnce completes a PUBLISH with PUBACK.
	QoSAtLeastOnce QoS = 1
	// QoSExactlyOnce completes a PUBLISH with PUBREC, PUBREL, and PUBCOMP.
	QoSExactlyOnce QoS = 2
)

// PacketIdentifier identifies one in-flight QoS 1 or QoS 2 exchange.
type PacketIdentifier uint16

// Packet is the sealed domain representation of an MQTT control packet.
//
// The unexported methods intentionally prevent adapters from implementing a
// packet that the codec does not know how to validate and encode.
type Packet interface {
	packetType() PacketType
	packetVersion() ProtocolVersion
}
