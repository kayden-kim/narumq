package management

import (
	"net/http"
	"strings"

	portsmetrics "narumq/internal/application/ports/metrics"
)

// MetricsHandler serves Prometheus / OpenMetrics scrape requests.
type MetricsHandler struct {
	registry portsmetrics.MetricsRenderer
}

// NewMetricsHandler creates a MetricsHandler.
func NewMetricsHandler(registry portsmetrics.MetricsRenderer) *MetricsHandler {
	return &MetricsHandler{registry: registry}
}

// ServeHTTP handles GET /metrics.
func (h *MetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}

	reqID := GetOrGenerateRequestID(r)
	w.Header().Set(HeaderRequestID, reqID)

	if h.registry == nil {
		w.Header().Set("Content-Type", ContentTypeMetrics)
		w.WriteHeader(http.StatusOK)
		return
	}

	accept := r.Header.Get("Accept")
	isOpenMetrics := strings.Contains(accept, "application/openmetrics-text")

	if isOpenMetrics {
		w.Header().Set("Content-Type", ContentTypeOpenMetrics)
	} else {
		w.Header().Set("Content-Type", ContentTypeMetrics)
	}

	data := h.registry.Render(isOpenMetrics)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
