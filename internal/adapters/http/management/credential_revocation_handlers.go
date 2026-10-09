package management

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"narumq/internal/application/membership"
)

// CredentialRevocationUseCase is the application boundary for committed credential revocation operations.
type CredentialRevocationUseCase interface {
	ListCredentialRevocations(context.Context) ([]membership.CredentialRevocationStateDTO, error)
	RevokeCredential(context.Context, string) (membership.CredentialRevocationStatusDTO, error)
	RestoreCredential(context.Context, string) (membership.CredentialRevocationStatusDTO, error)
}

type credentialRevocationHandler struct {
	service CredentialRevocationUseCase
}

type credentialRevocationResponse struct {
	CredentialID string    `json:"credentialId"`
	RevokedAt    time.Time `json:"revokedAt"`
}

type credentialRevocationListResponse struct {
	Revocations []credentialRevocationResponse `json:"revocations"`
}

type credentialRevocationStatusResponse struct {
	CredentialID string     `json:"credentialId"`
	Revoked      bool       `json:"revoked"`
	RevokedAt    *time.Time `json:"revokedAt,omitempty"`
}

func (handler *credentialRevocationHandler) List(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeCredentialRevocationMethodNotAllowed(w, r, http.MethodGet)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	revocations, err := handler.service.ListCredentialRevocations(r.Context())
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	response := credentialRevocationListResponse{Revocations: make([]credentialRevocationResponse, 0, len(revocations))}
	for _, revocation := range revocations {
		response.Revocations = append(response.Revocations, credentialRevocationResponse{
			CredentialID: revocation.CredentialID,
			RevokedAt:    time.UnixMilli(int64(revocation.RevokedAtUnixMillis)).UTC(),
		})
	}
	WriteDataEnvelope(w, r, http.StatusOK, response)
}

func (handler *credentialRevocationHandler) Revoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeCredentialRevocationMethodNotAllowed(w, r, http.MethodPost)
		return
	}
	credentialID, ok := credentialIDFromRequest(w, r)
	if !ok {
		return
	}
	status, err := handler.service.RevokeCredential(r.Context(), credentialID)
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, credentialRevocationStatusBody(status))
}

func (handler *credentialRevocationHandler) Restore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeCredentialRevocationMethodNotAllowed(w, r, http.MethodDelete)
		return
	}
	credentialID, ok := credentialIDFromRequest(w, r)
	if !ok {
		return
	}
	status, err := handler.service.RestoreCredential(r.Context(), credentialID)
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, credentialRevocationStatusBody(status))
}

func credentialIDFromRequest(w http.ResponseWriter, r *http.Request) (string, bool) {
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return "", false
	}
	if r.Body != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1))
		if err != nil || len(body) != 0 {
			writeInvalidRequest(w, r, errors.New("credential revocation request must not contain a body"))
			return "", false
		}
	}
	credentialID := r.PathValue("credentialId")
	if !utf8.ValidString(credentialID) || len(credentialID) < 1 || len(credentialID) > membership.MaxCredentialIDBytes {
		writeInvalidRequest(w, r, errors.New("credentialId must contain 1 to 512 UTF-8 bytes"))
		return "", false
	}
	return credentialID, true
}

func credentialRevocationStatusBody(status membership.CredentialRevocationStatusDTO) credentialRevocationStatusResponse {
	response := credentialRevocationStatusResponse{CredentialID: status.CredentialID, Revoked: status.Revoked}
	if status.Revoked {
		revokedAt := time.UnixMilli(int64(status.RevokedAtUnixMillis)).UTC()
		response.RevokedAt = &revokedAt
	}
	return response
}

func writeCredentialRevocationMethodNotAllowed(w http.ResponseWriter, r *http.Request, method string) {
	w.Header().Set("Allow", method)
	WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
}
