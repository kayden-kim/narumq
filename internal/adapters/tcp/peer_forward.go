package tcp

import (
	"encoding/binary"
	"unicode/utf8"
)

// ForwardedDeliveryDTO is one QoS 0 delivery forwarded to the subscriber owner node.
type ForwardedDeliveryDTO struct {
	SessionID           string
	Topic               string
	Payload             []byte
	Retain              bool
	ExpiresAtUnixMillis uint64
}

// EncodeForwardedDelivery encodes one bounded version-1 live delivery frame.
func EncodeForwardedDelivery(delivery ForwardedDeliveryDTO) ([]byte, error) {
	sessionLength := len(delivery.SessionID)
	topicLength := len(delivery.Topic)
	if sessionLength == 0 || sessionLength > 65535 ||
		topicLength == 0 || topicLength > 65535 || !utf8.ValidString(delivery.Topic) {
		return nil, ErrInvalidPeerMessage
	}
	totalLength := uint64(18) + uint64(sessionLength) + uint64(topicLength) + uint64(len(delivery.Payload))
	if totalLength > uint64(dynamicForwardFrameLimit) {
		return nil, ErrInvalidPeerMessage
	}

	payload := make([]byte, 0, int(totalLength))
	payload = append(payload, 1)
	payload = appendUint16(payload, uint32(sessionLength))
	payload = append(payload, delivery.SessionID...)
	payload = appendUint16(payload, uint32(topicLength))
	payload = append(payload, delivery.Topic...)
	if delivery.Retain {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	payload = appendUint64(payload, delivery.ExpiresAtUnixMillis)
	var payloadLength [4]byte
	binary.BigEndian.PutUint32(payloadLength[:], uint32(len(delivery.Payload)))
	payload = append(payload, payloadLength[:]...)
	payload = append(payload, delivery.Payload...)
	return payload, nil
}

// DecodeForwardedDelivery decodes a canonical version-1 delivery and owns its payload copy.
func DecodeForwardedDelivery(payload []byte) (ForwardedDeliveryDTO, error) {
	if len(payload) < 18 || len(payload) > dynamicForwardFrameLimit {
		return ForwardedDeliveryDTO{}, ErrInvalidPeerMessage
	}
	reader := dynamicWireReader{data: payload}
	version, ok := reader.byte()
	if !ok || version != 1 {
		return ForwardedDeliveryDTO{}, ErrInvalidPeerMessage
	}
	sessionLength, ok := reader.uint16()
	if !ok || sessionLength == 0 {
		return ForwardedDeliveryDTO{}, ErrInvalidPeerMessage
	}
	sessionBytes, ok := reader.bytes(int(sessionLength))
	if !ok {
		return ForwardedDeliveryDTO{}, ErrInvalidPeerMessage
	}
	sessionID := string(sessionBytes)
	topic, ok := reader.string16(65535, false)
	if !ok {
		return ForwardedDeliveryDTO{}, ErrInvalidPeerMessage
	}
	flags, ok := reader.byte()
	if !ok || flags&^byte(1) != 0 {
		return ForwardedDeliveryDTO{}, ErrInvalidPeerMessage
	}
	expiresAt, ok := reader.uint64()
	if !ok {
		return ForwardedDeliveryDTO{}, ErrInvalidPeerMessage
	}
	lengthBytes, ok := reader.bytes(4)
	if !ok {
		return ForwardedDeliveryDTO{}, ErrInvalidPeerMessage
	}
	payloadLength := binary.BigEndian.Uint32(lengthBytes)
	if uint64(payloadLength) > uint64(len(payload)-reader.position) {
		return ForwardedDeliveryDTO{}, ErrInvalidPeerMessage
	}
	messagePayload, ok := reader.bytes(int(payloadLength))
	if !ok || reader.position != len(payload) {
		return ForwardedDeliveryDTO{}, ErrInvalidPeerMessage
	}
	return ForwardedDeliveryDTO{
		SessionID: sessionID, Topic: topic, Payload: append([]byte(nil), messagePayload...),
		Retain: flags&1 != 0, ExpiresAtUnixMillis: expiresAt,
	}, nil
}
