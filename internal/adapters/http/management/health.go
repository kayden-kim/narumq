package management

import (
	"net/http"
	"time"

	ports "narumq/internal/application/ports/management"
)

// HealthHandler serves liveness, readiness, startup, and drain probes.
type HealthHandler struct {
	checker ports.HealthChecker
}

// NewHealthHandler creates a HealthHandler.
func NewHealthHandler(checker ports.HealthChecker) *HealthHandler {
	return &HealthHandler{checker: checker}
}

func (h *HealthHandler) respondHealth(w http.ResponseWriter, r *http.Request, status ports.HealthStatus, reason *string) {
	now := time.Now().UTC()
	if status == ports.HealthStatusUp || status == ports.HealthStatusSafeToStop {
		dto := ports.HealthDataDTO{
			Status:     status,
			Reason:     reason,
			ObservedAt: now,
		}
		WriteDataEnvelope(w, r, http.StatusOK, dto)
		return
	}

	msg := "service is unavailable"
	if reason != nil && *reason != "" {
		msg = *reason
	}
	WriteError(w, r, http.StatusServiceUnavailable, "unavailable", msg, true, map[string]any{
		"status":     status,
		"observedAt": now.Format(time.RFC3339),
	})
}

// Live handles GET /health/live.
func (h *HealthHandler) Live(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	status, reason := h.checker.CheckLiveness(r.Context())
	h.respondHealth(w, r, status, reason)
}

// Ready handles GET /health/ready.
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	status, reason := h.checker.CheckReadiness(r.Context())
	h.respondHealth(w, r, status, reason)
}

// Startup handles GET /health/startup.
func (h *HealthHandler) Startup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	status, reason := h.checker.CheckStartup(r.Context())
	h.respondHealth(w, r, status, reason)
}

// Drain handles GET /health/drain.
func (h *HealthHandler) Drain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	status, reason := h.checker.CheckDrain(r.Context())
	h.respondHealth(w, r, status, reason)
}
