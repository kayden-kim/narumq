package management

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/domain/security"
)

// BlocklistUseCase is the application boundary for committed client blocklist operations.
type BlocklistUseCase interface {
	ListBlocklist(context.Context) (membership.BlocklistDTO, error)
	Block(context.Context, security.BlockEntry) (membership.BlocklistStatusDTO, error)
	Unblock(context.Context, security.BlockEntry) (membership.BlocklistStatusDTO, error)
}

type blocklistHandler struct {
	service BlocklistUseCase
}

type blocklistEntryResponse struct {
	Kind      string    `json:"kind"`
	TenantID  string    `json:"tenantId,omitempty"`
	Value     string    `json:"value"`
	BlockedAt time.Time `json:"blockedAt"`
}

type blocklistListResponse struct {
	PolicyVersion uint64                   `json:"policyVersion"`
	Entries       []blocklistEntryResponse `json:"entries"`
}

type blocklistStatusResponse struct {
	Kind          string     `json:"kind"`
	TenantID      string     `json:"tenantId,omitempty"`
	Value         string     `json:"value"`
	Blocked       bool       `json:"blocked"`
	BlockedAt     *time.Time `json:"blockedAt,omitempty"`
	PolicyVersion uint64     `json:"policyVersion"`
}

func (handler *blocklistHandler) List(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeBlocklistMethodNotAllowed(w, r, http.MethodGet)
		return
	}
	if !validateBlocklistRequest(w, r) {
		return
	}
	state, err := handler.service.ListBlocklist(r.Context())
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	response := blocklistListResponse{
		PolicyVersion: state.PolicyVersion,
		Entries:       make([]blocklistEntryResponse, 0, len(state.Entries)),
	}
	for _, stateEntry := range state.Entries {
		response.Entries = append(response.Entries, blocklistEntryBody(stateEntry))
	}
	WriteDataEnvelope(w, r, http.StatusOK, response)
}

func (handler *blocklistHandler) Block(kind security.BlockKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeBlocklistMethodNotAllowed(w, r, http.MethodPost)
			return
		}
		handler.change(w, r, kind, true)
	}
}

func (handler *blocklistHandler) Unblock(kind security.BlockKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			writeBlocklistMethodNotAllowed(w, r, http.MethodDelete)
			return
		}
		handler.change(w, r, kind, false)
	}
}

func (handler *blocklistHandler) change(w http.ResponseWriter, r *http.Request, kind security.BlockKind, blocked bool) {
	if !validateBlocklistRequest(w, r) {
		return
	}
	entry, ok := blocklistEntryFromRequest(w, r, kind)
	if !ok {
		return
	}
	setAuditTarget(r, "mqtt_blocklist_entry", entry.Key())
	var (
		status membership.BlocklistStatusDTO
		err    error
	)
	if blocked {
		status, err = handler.service.Block(r.Context(), entry)
	} else {
		status, err = handler.service.Unblock(r.Context(), entry)
	}
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, blocklistStatusBody(status))
}

func validateBlocklistRequest(w http.ResponseWriter, r *http.Request) bool {
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return false
	}
	if r.Body != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1))
		if err != nil || len(body) != 0 {
			writeInvalidRequest(w, r, errors.New("blocklist request must not contain a body"))
			return false
		}
	}
	return true
}

func blocklistEntryFromRequest(w http.ResponseWriter, r *http.Request, kind security.BlockKind) (security.BlockEntry, bool) {
	tenantID := ""
	value := ""
	message := ""
	switch kind {
	case security.BlockKindClientID:
		tenantID = r.PathValue("tenantId")
		value = r.PathValue("clientId")
		message = "tenantId must be valid and clientId must contain 1 to 256 UTF-8 bytes"
	case security.BlockKindUsername:
		value = r.PathValue("username")
		message = "username must contain 1 to 256 UTF-8 bytes and must not contain NUL"
	case security.BlockKindCertificateSHA256:
		value = r.PathValue("certificateSha256")
		message = "certificateSha256 must be a lowercase 64-character SHA-256 fingerprint"
	case security.BlockKindIPPrefix:
		value = r.PathValue("address") + "/" + r.PathValue("prefixLength")
		message = "ipPrefix must be a canonical CIDR prefix such as 203.0.113.0/24"
	default:
		writeInvalidRequest(w, r, errors.New("blocklist kind is invalid"))
		return security.BlockEntry{}, false
	}
	entry, err := security.NewBlockEntry(kind, tenantID, value)
	if err != nil {
		writeInvalidRequest(w, r, errors.New(message))
		return security.BlockEntry{}, false
	}
	return entry, true
}

func blocklistEntryBody(state membership.BlocklistStateDTO) blocklistEntryResponse {
	return blocklistEntryResponse{
		Kind: state.Entry.Kind.String(), TenantID: state.Entry.TenantID, Value: state.Entry.Value,
		BlockedAt: time.UnixMilli(int64(state.BlockedAtUnixMillis)).UTC(),
	}
}

func blocklistStatusBody(status membership.BlocklistStatusDTO) blocklistStatusResponse {
	response := blocklistStatusResponse{
		Kind: status.Entry.Kind.String(), TenantID: status.Entry.TenantID, Value: status.Entry.Value,
		Blocked: status.Blocked, PolicyVersion: status.PolicyVersion,
	}
	if status.Blocked {
		blockedAt := time.UnixMilli(int64(status.BlockedAtUnixMillis)).UTC()
		response.BlockedAt = &blockedAt
	}
	return response
}

func writeBlocklistMethodNotAllowed(w http.ResponseWriter, r *http.Request, allow string) {
	w.Header().Set("Allow", allow)
	WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
}
