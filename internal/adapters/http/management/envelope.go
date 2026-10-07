package management

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"time"
	"unicode"

	ports "narumq/internal/application/ports/management"
)

const (
	HeaderRequestID        = "X-Request-Id"
	ContentTypeJSON        = "application/json; charset=utf-8"
	ContentTypeMetrics     = "text/plain; version=0.0.4; charset=utf-8"
	ContentTypeOpenMetrics = "application/openmetrics-text; version=1.0.0; charset=utf-8"
)

type contextKey string

const requestIDKey contextKey = "narumq_request_id"

// DataEnvelope wraps a single resource payload.
type DataEnvelope struct {
	Data any `json:"data"`
}

// ListEnvelope wraps an array resource payload with pagination.
type ListEnvelope struct {
	Data any            `json:"data"`
	Page *ports.PageDTO `json:"page"`
}

// ErrorBody represents the machine-readable error structure.
type ErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	Details   any    `json:"details"`
}

// ErrorResponse represents the top-level error envelope.
type ErrorResponse struct {
	Error     ErrorBody `json:"error"`
	RequestID string    `json:"requestId"`
}

func generateRequestID(reader func([]byte) (int, error)) string {
	var b [12]byte
	if _, err := reader(b[:]); err != nil {
		return "req_" + hex.EncodeToString([]byte(time.Now().Format("150405.000000")))
	}
	return "req_" + hex.EncodeToString(b[:])
}
func validRequestID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// RequestIDFromContext returns the request ID installed by the router middleware.
func RequestIDFromContext(ctx context.Context) string {
	if value, ok := ctx.Value(requestIDKey).(string); ok {
		return value
	}
	return ""
}

// GetOrGenerateRequestID retrieves or creates a valid request ID.
func GetOrGenerateRequestID(r *http.Request) string {
	if reqID := RequestIDFromContext(r.Context()); validRequestID(reqID) {
		return reqID
	}
	reqID := strings.TrimSpace(r.Header.Get(HeaderRequestID))
	if validRequestID(reqID) {
		return reqID
	}
	return generateRequestID(rand.Read)
}

// WriteJSON sends a JSON response with status code and Content-Type header.
func WriteJSON(w http.ResponseWriter, r *http.Request, statusCode int, payload any) {
	reqID := GetOrGenerateRequestID(r)
	w.Header().Set(HeaderRequestID, reqID)
	w.Header().Set("Content-Type", ContentTypeJSON)
	w.WriteHeader(statusCode)

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(payload)
}

// WriteDataEnvelope sends a single-item response in {"data": ...} envelope.
func WriteDataEnvelope(w http.ResponseWriter, r *http.Request, statusCode int, data any) {
	WriteJSON(w, r, statusCode, DataEnvelope{Data: data})
}

// WriteListEnvelope sends a list response in {"data": [...], "page": {...}} envelope.
func WriteListEnvelope(w http.ResponseWriter, r *http.Request, statusCode int, data any, page *ports.PageDTO) {
	if page == nil {
		page = &ports.PageDTO{NextCursor: nil, HasMore: false}
	}
	if data == nil {
		data = []any{}
	} else {
		value := reflect.ValueOf(data)
		if value.Kind() == reflect.Slice && value.IsNil() {
			data = reflect.MakeSlice(value.Type(), 0, 0).Interface()
		}
	}
	WriteJSON(w, r, statusCode, ListEnvelope{Data: data, Page: page})
}

// WriteError sends a standard error response envelope.
func WriteError(w http.ResponseWriter, r *http.Request, statusCode int, code, message string, retryable bool, details any) {
	setAuditErrorCode(r, code)
	reqID := GetOrGenerateRequestID(r)
	if details == nil {
		details = map[string]any{}
	}
	resp := ErrorResponse{
		Error: ErrorBody{
			Code:      code,
			Message:   message,
			Retryable: retryable,
			Details:   details,
		},
		RequestID: reqID,
	}
	WriteJSON(w, r, statusCode, resp)
}
