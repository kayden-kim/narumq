package management

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"unicode/utf8"

	"narumq/internal/application/membership"
)

const maxMembershipRequestBytes = 16 * 1024

// MembershipUseCase is the application boundary used by cluster membership routes.
type MembershipUseCase interface {
	StageJoin(context.Context, membership.StageJoinRequestDTO) (membership.MembershipOperationDTO, error)
	StartPlannedRemoval(context.Context, uint64, string, uint64) (membership.MembershipOperationDTO, error)
	GetOperation(context.Context, string) (membership.MembershipOperationDTO, error)
	// ClusterFeatures reads committed feature state and non-retired member profiles.
	ClusterFeatures(context.Context) (membership.FeatureStateDTO, error)
	// ActivateFeature commits cluster-wide activation of a supported feature.
	ActivateFeature(context.Context, string) (membership.FeatureStateDTO, error)
}

type membershipHandler struct {
	service MembershipUseCase
}

type memberAdmissionRequest struct {
	MemberID             *uint64 `json:"memberId"`
	Address              *string `json:"address"`
	DNSSAN               *string `json:"dnsSan"`
	CertificateSHA256    *string `json:"certificateSha256"`
	ReleaseVersion       *string `json:"releaseVersion"`
	ClusterProtocolMajor *uint32 `json:"clusterProtocolMajor"`
	ClusterProtocolMinor *uint32 `json:"clusterProtocolMinor"`
	StateSchemaMajor     *uint32 `json:"stateSchemaMajor"`
	StateSchemaMinor     *uint32 `json:"stateSchemaMinor"`
}

type membershipOperationResponse struct {
	OperationID    string                              `json:"operationId"`
	Action         string                              `json:"action"`
	MemberID       uint64                              `json:"memberId"`
	Phase          membership.MembershipOperationPhase `json:"phase"`
	CommittedIndex uint64                              `json:"committedIndex"`
}

func (handler *membershipHandler) StageMember(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	idempotencyKey, ok := idempotencyKeyFromRequest(w, r)
	if !ok {
		return
	}
	var request memberAdmissionRequest
	if err := decodeStrictMembershipJSON(r, &request); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if request.MemberID == nil || request.Address == nil || request.DNSSAN == nil || request.CertificateSHA256 == nil || request.ReleaseVersion == nil || request.ClusterProtocolMajor == nil || request.ClusterProtocolMinor == nil || request.StateSchemaMajor == nil || request.StateSchemaMinor == nil {
		writeInvalidRequest(w, r, errors.New("all member identity fields are required"))
		return
	}
	setAuditTarget(r, "cluster_member", strconv.FormatUint(*request.MemberID, 10))
	if len(*request.CertificateSHA256) != 64 || !lowerHex(*request.CertificateSHA256) {
		writeInvalidRequest(w, r, errors.New("certificateSha256 must be 64 lowercase hexadecimal characters"))
		return
	}
	fingerprint, err := hex.DecodeString(*request.CertificateSHA256)
	if err != nil {
		writeInvalidRequest(w, r, errors.New("certificateSha256 is invalid"))
		return
	}
	operation, err := handler.service.StageJoin(r.Context(), membership.StageJoinRequestDTO{
		IdempotencyKey: idempotencyKey,
		Member: membership.MemberIdentityDTO{
			MemberID: *request.MemberID, Address: *request.Address, DNSSAN: *request.DNSSAN,
			CertificateSHA256: fingerprint, ReleaseVersion: *request.ReleaseVersion,
			ClusterProtocolMajor: *request.ClusterProtocolMajor, ClusterProtocolMinor: *request.ClusterProtocolMinor,
			StateSchemaMajor: *request.StateSchemaMajor, StateSchemaMinor: *request.StateSchemaMinor,
		},
	})
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	status := http.StatusAccepted
	if operation.Replayed || terminalMembershipPhase(operation.Phase) {
		status = http.StatusOK
	}
	WriteDataEnvelope(w, r, status, membershipOperationBody(operation))
}

func (handler *membershipHandler) RemoveMember(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if r.Body != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1))
		if err != nil || len(body) != 0 {
			writeInvalidRequest(w, r, errors.New("DELETE membership request must not contain a body"))
			return
		}
	}
	memberID, err := parseCanonicalMemberID(r.PathValue("memberId"))
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	idempotencyKey, ok := idempotencyKeyFromRequest(w, r)
	if !ok {
		return
	}
	operation, err := handler.service.StartPlannedRemoval(r.Context(), memberID, idempotencyKey, 0)
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	status := http.StatusAccepted
	if operation.Replayed || terminalMembershipPhase(operation.Phase) {
		status = http.StatusOK
	}
	WriteDataEnvelope(w, r, status, membershipOperationBody(operation))
}

func (handler *membershipHandler) GetOperation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	operation, err := handler.service.GetOperation(r.Context(), r.PathValue("operationId"))
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, membershipOperationBody(operation))
}

func decodeStrictMembershipJSON(r *http.Request, target any) error {
	contentTypes := r.Header.Values("Content-Type")
	if len(contentTypes) != 1 {
		return errors.New("one Content-Type header is required")
	}
	mediaType, _, err := mime.ParseMediaType(contentTypes[0])
	if err != nil || mediaType != "application/json" {
		return errors.New("Content-Type must be application/json")
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, maxMembershipRequestBytes+1))
	if err != nil {
		return errors.New("membership request body could not be read")
	}
	if len(data) == 0 || len(data) > maxMembershipRequestBytes || !utf8.Valid(data) {
		return errors.New("membership request body is empty, oversized, or invalid UTF-8")
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return errors.New("membership request JSON is invalid or contains duplicate fields")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("membership request JSON has an unknown field or invalid value")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("membership request contains trailing JSON value")
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var visit func() error
	visit = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, isDelimiter := token.(json.Delim)
		if !isDelimiter {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return errors.New("JSON object contains a duplicate key")
				}
				seen[key] = struct{}{}
				if err := visit(); err != nil {
					return err
				}
			}
			closeToken, err := decoder.Token()
			if err != nil || closeToken != json.Delim('}') {
				return errors.New("JSON object is unterminated")
			}
		case '[':
			for decoder.More() {
				if err := visit(); err != nil {
					return err
				}
			}
			closeToken, err := decoder.Token()
			if err != nil || closeToken != json.Delim(']') {
				return errors.New("JSON array is unterminated")
			}
		default:
			return errors.New("unexpected JSON delimiter")
		}
		return nil
	}
	if err := visit(); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains trailing value")
	}
	return nil
}

func idempotencyKeyFromRequest(w http.ResponseWriter, r *http.Request) (string, bool) {
	values := r.Header.Values("Idempotency-Key")
	if len(values) != 1 || !validHTTPIdempotencyKey(values[0]) {
		writeInvalidRequest(w, r, errors.New("one valid Idempotency-Key header is required"))
		return "", false
	}
	return values[0], true
}

func validHTTPIdempotencyKey(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func lowerHex(value string) bool {
	for index := 0; index < len(value); index++ {
		if !(value[index] >= '0' && value[index] <= '9' || value[index] >= 'a' && value[index] <= 'f') {
			return false
		}
	}
	return true
}

func parseCanonicalMemberID(value string) (uint64, error) {
	memberID, err := strconv.ParseUint(value, 10, 64)
	if err != nil || memberID == 0 || strconv.FormatUint(memberID, 10) != value {
		return 0, errors.New("memberId must be a canonical positive decimal uint64")
	}
	return memberID, nil
}

func membershipOperationBody(operation membership.MembershipOperationDTO) membershipOperationResponse {
	action := "ADD"
	if operation.Record.TargetMemberID != 0 {
		action = "REMOVE"
	}
	return membershipOperationResponse{
		OperationID: operation.OperationID, Action: action, MemberID: operation.MemberID,
		Phase: operation.Phase, CommittedIndex: operation.CommittedIndex,
	}
}

func terminalMembershipPhase(phase membership.MembershipOperationPhase) bool {
	return phase == membership.OperationPhaseSucceeded || phase == membership.OperationPhaseAborted
}

func writeMembershipServiceError(w http.ResponseWriter, r *http.Request, err error) {
	var serviceError *membership.ServiceError
	if !errors.As(err, &serviceError) {
		WriteError(w, r, http.StatusInternalServerError, "internal_error", "internal membership error", false, nil)
		return
	}
	status := http.StatusInternalServerError
	switch serviceError.Code {
	case membership.ServiceErrorInvalidRequest:
		status = http.StatusBadRequest
	case membership.ServiceErrorResourceNotFound:
		status = http.StatusNotFound
	case membership.ServiceErrorMemberIDReserved,
		membership.ServiceErrorMemberIdentityConflict,
		membership.ServiceErrorIdempotencyConflict,
		membership.ServiceErrorMembershipInProgress,
		membership.ServiceErrorReplicationFactorUnsatisfied,
		membership.ServiceErrorOperationConflict,
		membership.ServiceErrorNotLeader,
		membership.ServiceErrorInvalidNodeTransition,
		membership.ServiceErrorDrainNotCancellable,
		membership.ServiceErrorNodeDrainInProgress,
		membership.ServiceErrorConnectionRebalanceInProgress,
		membership.ServiceErrorRebalanceNotCancellable,
		membership.ServiceErrorFeatureInactive,
		membership.ServiceErrorFeatureUnsupported,
		membership.ServiceErrorCredentialRevocationCapacity:
		status = http.StatusConflict
	case membership.ServiceErrorCompatibilityUnsupported:
		status = http.StatusUnprocessableEntity
	case membership.ServiceErrorQuorumUnavailable:
		status = http.StatusServiceUnavailable
	default:
		serviceError = &membership.ServiceError{Code: "internal_error", Message: "internal membership error"}
	}
	var details any
	if serviceError.NodeState != "" {
		details = map[string]any{"state": serviceError.NodeState}
	}
	WriteError(w, r, status, string(serviceError.Code), serviceError.Message, serviceError.Retryable, details)
}
