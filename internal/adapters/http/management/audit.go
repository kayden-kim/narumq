package management

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"unicode/utf8"

	"narumq/internal/application/audit"
	portsaudit "narumq/internal/application/ports/audit"
)

// AuditUseCase admits mutations and records management audit events.
type AuditUseCase interface {
	AdmitMutation() error
	Record(audit.RecordCommandDTO) error
}

type auditMutation struct {
	action      string
	targetType  string
	targetParam string
}

type auditOutcome struct {
	errorCode  string
	targetType string
	targetID   string
	details    map[string]any
}

type auditOutcomeContextKey struct{}

func setAuditErrorCode(r *http.Request, code string) {
	if outcome, ok := r.Context().Value(auditOutcomeContextKey{}).(*auditOutcome); ok {
		outcome.errorCode = code
	}
}

func setAuditTarget(r *http.Request, targetType, targetID string) {
	if outcome, ok := r.Context().Value(auditOutcomeContextKey{}).(*auditOutcome); ok {
		outcome.targetType, outcome.targetID = targetType, targetID
	}
}

func setAuditDetails(r *http.Request, details map[string]any) {
	if outcome, ok := r.Context().Value(auditOutcomeContextKey{}).(*auditOutcome); ok {
		outcome.details = details
	}
}

type auditResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *auditResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *auditResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *auditResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (a *Authenticator) require(permission Permission, mutation *auditMutation, rateLimited bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, ok := a.authenticate(r)
		if !ok {
			if rateLimited && !a.authFailureLimiter.allow(remoteAddressKey(r.RemoteAddr)) {
				a.rejectRateLimited(w, r, "unauthenticated", nil)
				return
			}
			if a.cfg.Audit != nil {
				a.recordOutcome(r, "management.request", "management_endpoint", r.Pattern, "unauthenticated", nil, nil, http.StatusUnauthorized, "unauthorized")
			}
			WriteError(w, r, http.StatusUnauthorized, "unauthorized", "authentication required or credentials invalid", false, nil)
			return
		}
		if rateLimited && !a.principalLimiter.allow(principal.subject) {
			a.rejectRateLimited(w, r, principal.subject, principal.roles.names())
			return
		}
		if !principal.roles.allows(permission) {
			if a.cfg.Audit != nil {
				a.recordOutcome(r, "management.request", "management_endpoint", r.Pattern, principal.subject, principal.roles.names(), nil, http.StatusForbidden, "forbidden")
			}
			WriteError(w, r, http.StatusForbidden, "forbidden", "authenticated principal lacks required permissions", false, nil)
			return
		}

		ctx := r.Context()
		if mutation == nil {
			next(w, r.WithContext(ctx))
			return
		}
		logger := a.logger()
		if a.cfg.Audit == nil {
			err := errors.New("management audit service is not configured")
			logger.Warn("management mutation rejected because audit log is unavailable", "requestId", RequestIDFromContext(r.Context()), "action", mutation.action, "error", err)
			WriteError(w, r, http.StatusServiceUnavailable, "audit_unavailable", "management audit log is unavailable", true, nil)
			return
		}
		if err := a.cfg.Audit.AdmitMutation(); err != nil {
			logger.Warn("management mutation rejected because audit log is unavailable", "requestId", RequestIDFromContext(r.Context()), "action", mutation.action, "error", err)
			WriteError(w, r, http.StatusServiceUnavailable, "audit_unavailable", "management audit log is unavailable", true, nil)
			return
		}

		targetID := a.cfg.ClusterID
		if mutation.targetParam != "" {
			targetID = r.PathValue(mutation.targetParam)
			if targetID == "" || !utf8.ValidString(targetID) || utf8.RuneCountInString(targetID) > 512 {
				targetID = "invalid"
			}
		}
		outcome := &auditOutcome{targetType: mutation.targetType, targetID: targetID}
		ctx = context.WithValue(ctx, auditOutcomeContextKey{}, outcome)
		r = r.WithContext(ctx)
		wrapped := &auditResponseWriter{ResponseWriter: w}
		defer func() {
			if recovered := recover(); recovered != nil {
				status := wrapped.status
				if status == 0 {
					status = http.StatusInternalServerError
				}
				a.recordOutcome(r, mutation.action, outcome.targetType, outcome.targetID, principal.subject, principal.roles.names(), mutation, status, "internal_error")
				panic(recovered)
			}
			status := wrapped.status
			if status == 0 {
				status = http.StatusOK
			}
			reason := outcome.errorCode
			if status >= 200 && status < 300 {
				reason = "succeeded"
			} else if reason == "" {
				reason = "internal_error"
			}
			a.recordOutcome(r, mutation.action, outcome.targetType, outcome.targetID, principal.subject, principal.roles.names(), mutation, status, reason)
		}()
		next(wrapped, r)
	}
}

func (a *Authenticator) rejectRateLimited(w http.ResponseWriter, r *http.Request, subject string, roles []string) {
	a.recordOutcome(r, "management.request", "management_endpoint", r.Pattern, subject, roles, nil, http.StatusTooManyRequests, "rate_limited")
	w.Header().Set("Retry-After", "1")
	WriteError(w, r, http.StatusTooManyRequests, "rate_limited", "management API rate limit exceeded", true, nil)
}

func (a *Authenticator) logger() *slog.Logger {
	if a.cfg.Logger != nil {
		return a.cfg.Logger
	}
	return slog.Default()
}

func (a *Authenticator) recordOutcome(r *http.Request, action, targetType, targetID, subject string, roles []string, mutation *auditMutation, status int, reason string) {
	if a.cfg.Audit == nil {
		return
	}
	if targetID == "" {
		targetID = "invalid"
	}
	if targetType == "" {
		targetType = "management_endpoint"
	}
	result := portsaudit.AuditResultDenied
	if reason == "succeeded" {
		result = portsaudit.AuditResultAllowed
	} else if mutation != nil {
		result = portsaudit.AuditResultFailed
	}
	details := map[string]any{"httpStatus": status, "method": r.Method}
	if mutation != nil {
		values := r.Header.Values("Idempotency-Key")
		if len(values) == 1 && validHTTPIdempotencyKey(values[0]) {
			details["idempotencyKey"] = values[0]
		}
	}
	if outcome, ok := r.Context().Value(auditOutcomeContextKey{}).(*auditOutcome); ok {
		for key, value := range outcome.details {
			details[key] = value
		}
	}
	command := audit.RecordCommandDTO{
		RequestID: RequestIDFromContext(r.Context()),
		Actor:     portsaudit.AuditActorDTO{Subject: subject, Roles: roles},
		Action:    action,
		Target:    portsaudit.AuditTargetDTO{Type: targetType, ID: targetID},
		Result:    result,
		Reason:    reason,
		Details:   details,
	}
	if err := a.cfg.Audit.Record(command); err != nil && !errors.Is(err, audit.ErrDenialSuppressed) {
		a.logger().Error("management audit event was not recorded", "requestId", command.RequestID, "action", action, "result", result, "error", err)
	}
}
