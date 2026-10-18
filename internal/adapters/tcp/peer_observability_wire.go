package tcp

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"unicode/utf8"
)

const (
	peerObservabilityVersion            uint16 = 1
	peerObservabilityRequestSize               = 11
	peerObservabilityResponseHeaderSize        = 20
	peerObservabilityResponseLimit             = 8 << 20
	peerObservabilityJSONMaxDepth              = 16
	peerObservabilityMagic                     = "NMQOBS01"
)

// PeerObservabilityKind identifies the type of snapshot requested from a peer.
type PeerObservabilityKind uint8

const (
	PeerObservabilityNode    PeerObservabilityKind = 1
	PeerObservabilityMetrics PeerObservabilityKind = 2
)

// PeerObservabilityStatus is the outcome encoded in an observability response.
type PeerObservabilityStatus uint8

const (
	PeerObservabilityStatusOK PeerObservabilityStatus = 1 + iota
	PeerObservabilityStatusUnsupported
	PeerObservabilityStatusBusy
	PeerObservabilityStatusSnapshotTooLarge
	PeerObservabilityStatusInternalError
)

// PeerObservabilityRequestDTO is a one-shot request for a member's own snapshot.
type PeerObservabilityRequestDTO struct {
	Kind           PeerObservabilityKind
	TargetMemberID uint64
}

// PeerObservabilityResponseDTO contains the status and optional JSON snapshot body.
type PeerObservabilityResponseDTO struct {
	Status            PeerObservabilityStatus
	ResponderMemberID uint64
	Kind              PeerObservabilityKind
	Body              []byte
}

func validPeerObservabilityKind(kind PeerObservabilityKind) bool {
	return kind == PeerObservabilityNode || kind == PeerObservabilityMetrics
}

func validPeerObservabilityStatus(status PeerObservabilityStatus) bool {
	return status >= PeerObservabilityStatusOK && status <= PeerObservabilityStatusInternalError
}

func encodePeerObservabilityRequest(request PeerObservabilityRequestDTO) ([]byte, error) {
	if !validPeerObservabilityKind(request.Kind) || request.TargetMemberID == 0 {
		return nil, ErrInvalidPeerMessage
	}
	payload := make([]byte, peerObservabilityRequestSize)
	binary.BigEndian.PutUint16(payload[0:2], peerObservabilityVersion)
	payload[2] = byte(request.Kind)
	binary.BigEndian.PutUint64(payload[3:11], request.TargetMemberID)
	return payload, nil
}

func decodePeerObservabilityRequest(payload []byte) (PeerObservabilityRequestDTO, error) {
	if len(payload) != peerObservabilityRequestSize || binary.BigEndian.Uint16(payload[0:2]) != peerObservabilityVersion {
		return PeerObservabilityRequestDTO{}, ErrInvalidPeerMessage
	}
	kind := PeerObservabilityKind(payload[2])
	targetMemberID := binary.BigEndian.Uint64(payload[3:11])
	if !validPeerObservabilityKind(kind) || targetMemberID == 0 {
		return PeerObservabilityRequestDTO{}, ErrInvalidPeerMessage
	}
	return PeerObservabilityRequestDTO{Kind: kind, TargetMemberID: targetMemberID}, nil
}

func encodePeerObservabilityResponse(response PeerObservabilityResponseDTO) ([]byte, error) {
	if !validPeerObservabilityStatus(response.Status) || response.ResponderMemberID == 0 || !validPeerObservabilityKind(response.Kind) {
		return nil, ErrInvalidPeerMessage
	}
	if response.Status != PeerObservabilityStatusOK {
		if len(response.Body) != 0 {
			return nil, ErrInvalidPeerMessage
		}
	} else if len(response.Body) > peerObservabilityResponseLimit-peerObservabilityResponseHeaderSize || validatePeerObservabilityJSON(response.Body) != nil {
		return nil, ErrInvalidPeerMessage
	}
	payload := make([]byte, peerObservabilityResponseHeaderSize+len(response.Body))
	copy(payload[:8], peerObservabilityMagic)
	binary.BigEndian.PutUint16(payload[8:10], peerObservabilityVersion)
	payload[10] = byte(response.Status)
	binary.BigEndian.PutUint64(payload[11:19], response.ResponderMemberID)
	payload[19] = byte(response.Kind)
	copy(payload[peerObservabilityResponseHeaderSize:], response.Body)
	return payload, nil
}

func decodePeerObservabilityResponse(payload []byte, expectedKind PeerObservabilityKind, expectedResponderID uint64) (PeerObservabilityResponseDTO, error) {
	if len(payload) < peerObservabilityResponseHeaderSize || len(payload) > peerObservabilityResponseLimit ||
		string(payload[:8]) != peerObservabilityMagic || binary.BigEndian.Uint16(payload[8:10]) != peerObservabilityVersion {
		return PeerObservabilityResponseDTO{}, ErrInvalidPeerMessage
	}
	status := PeerObservabilityStatus(payload[10])
	responderID := binary.BigEndian.Uint64(payload[11:19])
	kind := PeerObservabilityKind(payload[19])
	body := payload[peerObservabilityResponseHeaderSize:]
	if !validPeerObservabilityStatus(status) || responderID == 0 || responderID != expectedResponderID ||
		!validPeerObservabilityKind(kind) || kind != expectedKind || !validPeerObservabilityKind(expectedKind) || expectedResponderID == 0 {
		return PeerObservabilityResponseDTO{}, ErrInvalidPeerMessage
	}
	if status != PeerObservabilityStatusOK {
		if len(body) != 0 {
			return PeerObservabilityResponseDTO{}, ErrInvalidPeerMessage
		}
		return PeerObservabilityResponseDTO{Status: status, ResponderMemberID: responderID, Kind: kind}, nil
	}
	if validatePeerObservabilityJSON(body) != nil {
		return PeerObservabilityResponseDTO{}, ErrInvalidPeerMessage
	}
	return PeerObservabilityResponseDTO{
		Status: status, ResponderMemberID: responderID, Kind: kind, Body: body,
	}, nil
}

// validatePeerObservabilityJSON accepts exactly one JSON object, recursively
// rejecting duplicate keys and objects nested deeper than the wire contract.
func validatePeerObservabilityJSON(payload []byte) error {
	if len(payload) == 0 || !utf8.Valid(payload) {
		return ErrInvalidPeerMessage
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return ErrInvalidPeerMessage
	}
	if err := consumePeerObservabilityObject(decoder, 1); err != nil {
		return ErrInvalidPeerMessage
	}
	if _, err := decoder.Token(); err != io.EOF {
		return ErrInvalidPeerMessage
	}
	return nil
}

func consumePeerObservabilityObject(decoder *json.Decoder, depth int) error {
	if depth > peerObservabilityJSONMaxDepth {
		return ErrInvalidPeerMessage
	}
	keys := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return ErrInvalidPeerMessage
		}
		if _, duplicate := keys[key]; duplicate {
			return ErrInvalidPeerMessage
		}
		keys[key] = struct{}{}
		value, err := decoder.Token()
		if err != nil {
			return err
		}
		switch value {
		case json.Delim('{'):
			if err := consumePeerObservabilityObject(decoder, depth+1); err != nil {
				return err
			}
		case json.Delim('['):
			if err := consumePeerObservabilityArray(decoder, depth+1); err != nil {
				return err
			}
		case json.Delim('}'), json.Delim(']'):
			return ErrInvalidPeerMessage
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return ErrInvalidPeerMessage
	}
	return nil
}

func consumePeerObservabilityArray(decoder *json.Decoder, depth int) error {
	if depth > peerObservabilityJSONMaxDepth {
		return ErrInvalidPeerMessage
	}
	for decoder.More() {
		value, err := decoder.Token()
		if err != nil {
			return err
		}
		switch value {
		case json.Delim('{'):
			if err := consumePeerObservabilityObject(decoder, depth+1); err != nil {
				return err
			}
		case json.Delim('['):
			if err := consumePeerObservabilityArray(decoder, depth+1); err != nil {
				return err
			}
		case json.Delim('}'), json.Delim(']'):
			return ErrInvalidPeerMessage
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return ErrInvalidPeerMessage
	}
	return nil
}
