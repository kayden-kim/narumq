package tcp

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
)

func TestPeerObservabilityRequestGoldenAndBoundaries(t *testing.T) {
	encoded, err := encodePeerObservabilityRequest(PeerObservabilityRequestDTO{
		Kind: PeerObservabilityMetrics, TargetMemberID: 0x0102030405060708,
	})
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	const wantHex = "0001020102030405060708"
	if got := hex.EncodeToString(encoded); got != wantHex {
		t.Fatalf("request bytes = %s, want %s", got, wantHex)
	}
	decoded, err := decodePeerObservabilityRequest(encoded)
	if err != nil || decoded.Kind != PeerObservabilityMetrics || decoded.TargetMemberID != 0x0102030405060708 {
		t.Fatalf("decode request = %+v, %v", decoded, err)
	}
	for _, invalid := range []PeerObservabilityRequestDTO{
		{Kind: PeerObservabilityNode},
		{Kind: PeerObservabilityKind(0), TargetMemberID: 1},
		{Kind: PeerObservabilityKind(3), TargetMemberID: 1},
	} {
		if _, err := encodePeerObservabilityRequest(invalid); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Errorf("encode invalid request %+v: %v", invalid, err)
		}
	}
	for _, payload := range [][]byte{
		encoded[:len(encoded)-1], append(bytes.Clone(encoded), 0),
		{0, 2, byte(PeerObservabilityNode), 0, 0, 0, 0, 0, 0, 0, 1},
		{0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 1},
		{0, 1, byte(PeerObservabilityNode), 0, 0, 0, 0, 0, 0, 0, 0},
	} {
		if _, err := decodePeerObservabilityRequest(payload); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Errorf("decode invalid request %x: %v", payload, err)
		}
	}
}

func TestPeerObservabilityResponseGoldenAndRoundTrip(t *testing.T) {
	body := []byte(`{"ok":true}`)
	response := PeerObservabilityResponseDTO{
		Status: PeerObservabilityStatusOK, ResponderMemberID: 0x0102030405060708,
		Kind: PeerObservabilityNode, Body: body,
	}
	encoded, err := encodePeerObservabilityResponse(response)
	if err != nil {
		t.Fatalf("encode response: %v", err)
	}
	const wantHex = "4e4d514f42533031000101010203040506070801" + "7b226f6b223a747275657d"
	if got := hex.EncodeToString(encoded); got != wantHex {
		t.Fatalf("response bytes = %s, want %s", got, wantHex)
	}
	decoded, err := decodePeerObservabilityResponse(encoded, PeerObservabilityNode, response.ResponderMemberID)
	if err != nil || decoded.Status != response.Status || decoded.Kind != response.Kind || decoded.ResponderMemberID != response.ResponderMemberID || !bytes.Equal(decoded.Body, body) {
		t.Fatalf("decode response = %+v, %v", decoded, err)
	}
}

func TestPeerObservabilityResponseStatusesAndSizeLimit(t *testing.T) {
	for _, status := range []PeerObservabilityStatus{
		PeerObservabilityStatusUnsupported, PeerObservabilityStatusBusy,
		PeerObservabilityStatusSnapshotTooLarge, PeerObservabilityStatusInternalError,
	} {
		payload, err := encodePeerObservabilityResponse(PeerObservabilityResponseDTO{
			Status: status, ResponderMemberID: 9, Kind: PeerObservabilityMetrics,
		})
		if err != nil || len(payload) != peerObservabilityResponseHeaderSize {
			t.Fatalf("encode status %d: length=%d err=%v", status, len(payload), err)
		}
		decoded, err := decodePeerObservabilityResponse(payload, PeerObservabilityMetrics, 9)
		if err != nil || decoded.Status != status || len(decoded.Body) != 0 {
			t.Fatalf("decode status %d: %+v err=%v", status, decoded, err)
		}
		invalid := PeerObservabilityResponseDTO{Status: status, ResponderMemberID: 9, Kind: PeerObservabilityMetrics, Body: []byte(`{}`)}
		if _, err := encodePeerObservabilityResponse(invalid); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Errorf("non-OK response %d accepted body: %v", status, err)
		}
	}
	invalidBodies := [][]byte{nil, []byte(`[]`), []byte(`{}`)}
	for _, body := range invalidBodies[:2] {
		if _, err := encodePeerObservabilityResponse(PeerObservabilityResponseDTO{
			Status: PeerObservabilityStatusOK, ResponderMemberID: 9, Kind: PeerObservabilityNode, Body: body,
		}); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Errorf("invalid OK body %q accepted: %v", body, err)
		}
	}
	maxBody := make([]byte, peerObservabilityResponseLimit-peerObservabilityResponseHeaderSize)
	const prefix, suffix = `{"padding":"`, `"}`
	copy(maxBody, prefix)
	for index := len(prefix); index < len(maxBody)-len(suffix); index++ {
		maxBody[index] = 'x'
	}
	copy(maxBody[len(maxBody)-len(suffix):], suffix)
	if _, err := encodePeerObservabilityResponse(PeerObservabilityResponseDTO{
		Status: PeerObservabilityStatusOK, ResponderMemberID: 9, Kind: PeerObservabilityNode, Body: maxBody,
	}); err != nil {
		t.Fatalf("maximum-size body rejected: %v", err)
	}
	oversized := append(bytes.Clone(maxBody), ' ')
	if _, err := encodePeerObservabilityResponse(PeerObservabilityResponseDTO{
		Status: PeerObservabilityStatusOK, ResponderMemberID: 9, Kind: PeerObservabilityNode, Body: oversized,
	}); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("oversized response accepted: %v", err)
	}
	// A decoded frame is rejected by its total length before any body copy.
	if _, err := decodePeerObservabilityResponse(append(bytes.Clone(makeResponseHeader()), make([]byte, peerObservabilityResponseLimit-peerObservabilityResponseHeaderSize+1)...), PeerObservabilityNode, 9); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("oversized decoded response accepted: %v", err)
	}
}

func makeResponseHeader() []byte {
	payload, _ := encodePeerObservabilityResponse(PeerObservabilityResponseDTO{
		Status: PeerObservabilityStatusBusy, ResponderMemberID: 9, Kind: PeerObservabilityNode,
	})
	return payload
}

func TestValidatePeerObservabilityJSONStrictness(t *testing.T) {
	valid := []byte(`{"a":{"b":[{"c":1}]},"empty":{}}`)
	if err := validatePeerObservabilityJSON(valid); err != nil {
		t.Fatalf("valid JSON object rejected: %v", err)
	}
	invalid := [][]byte{
		[]byte(``), []byte(`null`), []byte(`[]`), []byte(`1`),
		[]byte(`{"a":1,"a":2}`),
		[]byte(`{"a":{"x":1,"x":2}}`),
		[]byte(`{} {}`), []byte(`{} trailing`), []byte(`{"a":`),
		{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'},
	}
	for _, body := range invalid {
		if err := validatePeerObservabilityJSON(body); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Errorf("invalid JSON %q accepted: %v", body, err)
		}
	}
	// Root object is depth 1; nesting through arrays counts as container depth.
	for depth := 1; depth <= peerObservabilityJSONMaxDepth; depth++ {
		body := nestedPeerJSON(depth)
		if err := validatePeerObservabilityJSON(body); err != nil {
			t.Errorf("depth %d rejected: %v", depth, err)
		}
	}
	if err := validatePeerObservabilityJSON(nestedPeerJSON(peerObservabilityJSONMaxDepth + 1)); !errors.Is(err, ErrInvalidPeerMessage) {
		t.Fatalf("depth %d accepted: %v", peerObservabilityJSONMaxDepth+1, err)
	}
}

func nestedPeerJSON(depth int) []byte {
	var body bytes.Buffer
	for range depth {
		body.WriteString(`{"x":`)
	}
	body.WriteByte('0')
	for range depth {
		body.WriteByte('}')
	}
	return body.Bytes()
}

func TestDecodePeerObservabilityResponseRejectsMismatchAndMalformedStatus(t *testing.T) {
	good, err := encodePeerObservabilityResponse(PeerObservabilityResponseDTO{
		Status: PeerObservabilityStatusOK, ResponderMemberID: 42, Kind: PeerObservabilityNode, Body: json.RawMessage(`{"nodeId":"42"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		kind   PeerObservabilityKind
		id     uint64
		mutate func([]byte)
	}{
		{name: "kind argument mismatch", kind: PeerObservabilityMetrics, id: 42},
		{name: "responder mismatch", kind: PeerObservabilityNode, id: 41},
		{name: "unknown status", kind: PeerObservabilityNode, id: 42, mutate: func(b []byte) { b[10] = 9 }},
		{name: "unknown kind", kind: PeerObservabilityNode, id: 42, mutate: func(b []byte) { b[19] = 9 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := bytes.Clone(good)
			if test.mutate != nil {
				test.mutate(payload)
			}
			if _, err := decodePeerObservabilityResponse(payload, test.kind, test.id); !errors.Is(err, ErrInvalidPeerMessage) {
				t.Fatalf("invalid response accepted: %v", err)
			}
		})
	}
}
