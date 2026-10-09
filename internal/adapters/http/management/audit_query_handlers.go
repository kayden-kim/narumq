package management

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	auditapp "narumq/internal/application/audit"
	ports "narumq/internal/application/ports/management"
)

// AuditQueryUseCase lists this node's stored audit hash-chain records.
type AuditQueryUseCase interface {
	Query(context.Context, auditapp.QueryCommandDTO) (auditapp.QueryResultDTO, error)
}

type auditRecordResponse struct {
	Seq      uint64          `json:"seq"`
	PrevHash string          `json:"prevHash"`
	Hash     string          `json:"hash"`
	Event    json.RawMessage `json:"event"`
}

type auditQueryHandler struct {
	service AuditQueryUseCase
	logger  *slog.Logger
}

func (handler *auditQueryHandler) List(w http.ResponseWriter, r *http.Request) {
	query, err := newQueryParser(r, "limit", "cursor", "actorSubject", "action", "targetType", "targetId", "requestId", "result", "from", "to")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	limit, err := parseLimit(query)
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	get := func(name string) (string, bool) {
		value, err := query.optional(name)
		if err != nil {
			writeInvalidRequest(w, r, err)
			return "", false
		}
		return value, true
	}
	cursor, ok := get("cursor")
	if !ok {
		return
	}
	actor, ok := get("actorSubject")
	if !ok {
		return
	}
	action, ok := get("action")
	if !ok {
		return
	}
	targetType, ok := get("targetType")
	if !ok {
		return
	}
	targetID, ok := get("targetId")
	if !ok {
		return
	}
	requestID, ok := get("requestId")
	if !ok {
		return
	}
	result, ok := get("result")
	if !ok {
		return
	}
	fromValue, ok := get("from")
	if !ok {
		return
	}
	toValue, ok := get("to")
	if !ok {
		return
	}
	from, err := parseAuditQueryTime(fromValue)
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	to, err := parseAuditQueryTime(toValue)
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	response, err := handler.service.Query(r.Context(), auditapp.QueryCommandDTO{
		Cursor: cursor, Limit: limit, ActorSubject: actor, Action: action, TargetType: targetType, TargetID: targetID,
		RequestID: requestID, Result: result, From: from, To: to,
	})
	if err != nil {
		if errors.Is(err, auditapp.ErrInvalidQuery) {
			writeInvalidRequest(w, r, err)
			return
		}
		logger := handler.logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("management audit query failed", "requestId", RequestIDFromContext(r.Context()), "error", err)
		WriteError(w, r, http.StatusServiceUnavailable, "audit_unavailable", "management audit log is unavailable", true, nil)
		return
	}
	records := make([]auditRecordResponse, 0, len(response.Records))
	for _, record := range response.Records {
		records = append(records, auditRecordResponse{Seq: record.Seq, PrevHash: record.PrevHash, Hash: record.Hash, Event: json.RawMessage(record.EventJSON)})
	}
	WriteListEnvelope(w, r, http.StatusOK, records, &ports.PageDTO{NextCursor: response.NextCursor, HasMore: response.HasMore})
}

func parseAuditQueryTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}
