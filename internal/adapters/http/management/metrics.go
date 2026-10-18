package management

import (
	"bytes"
	"encoding/json"
	"errors"
	portsmetrics "narumq/internal/application/ports/metrics"
	observationports "narumq/internal/application/ports/observability"
	"net/http"
	"strings"
)

const maxMetricsResponseBytes = 64 << 20

var (
	prometheusLabelEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n")
	prometheusHelpEscaper  = strings.NewReplacer("\\", "\\\\", "\n", "\\n")
)

// MetricsHandler serves local and cluster Prometheus/OpenMetrics scrapes.
type MetricsHandler struct {
	registry      portsmetrics.MetricsRenderer
	observability observationports.QueryService
}

// NewMetricsHandler creates a MetricsHandler.
func NewMetricsHandler(registry portsmetrics.MetricsRenderer) *MetricsHandler {
	return &MetricsHandler{registry: registry}
}

// ServeHTTP handles GET /metrics.
func (handler *MetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	query, err := newQueryParser(r, "mode")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if requestHasBody(r) {
		WriteError(w, r, http.StatusBadRequest, "request_body_not_allowed", "request body must be empty", false, nil)
		return
	}
	modeText, err := query.optional("mode")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	mode := observationports.MetricsModeNode
	if modeText != "" {
		mode = observationports.MetricsMode(modeText)
	}
	if mode != observationports.MetricsModeNode && mode != observationports.MetricsModeAllNodesUnaggregated &&
		mode != observationports.MetricsModeAllNodesAggregated {
		writeInvalidRequest(w, r, jsonInvalidMetricsMode())
		return
	}

	isOpenMetrics := strings.Contains(r.Header.Get("Accept"), "application/openmetrics-text")
	var data []byte
	if mode == observationports.MetricsModeNode {
		if handler.registry != nil {
			data = handler.registry.Render(isOpenMetrics)
		}
	} else {
		if handler.observability == nil {
			writeObservabilityUnavailable(w, r)
			return
		}
		collection, err := handler.observability.CollectMetrics(r.Context(), mode)
		if err != nil {
			writeObservabilityError(w, r, err)
			return
		}
		if !collection.Complete {
			writeObservabilityError(w, r, observationports.ErrMetricsCollectionIncomplete)
			return
		}
		data, err = renderMetricFamilies(collection.Families, isOpenMetrics)
		if err != nil {
			writeObservabilityError(w, r, err)
			return
		}
	}
	if len(data) > maxMetricsResponseBytes {
		writeObservabilityError(w, r, observationports.ErrMetricsSnapshotTooLarge)
		return
	}
	if isOpenMetrics {
		w.Header().Set("Content-Type", ContentTypeOpenMetrics)
	} else {
		w.Header().Set("Content-Type", ContentTypeMetrics)
	}
	w.Header().Set(HeaderRequestID, GetOrGenerateRequestID(r))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func writeMetricsDataEnvelope(w http.ResponseWriter, r *http.Request, data any) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(DataEnvelope{Data: data}); err != nil {
		WriteError(w, r, http.StatusInternalServerError, "internal_error", "an internal error occurred", false, nil)
		return
	}
	if body.Len() > maxMetricsResponseBytes {
		writeObservabilityError(w, r, observationports.ErrMetricsSnapshotTooLarge)
		return
	}
	w.Header().Set(HeaderRequestID, GetOrGenerateRequestID(r))
	w.Header().Set("Content-Type", ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body.Bytes())
}

func renderMetricFamilies(families []observationports.MetricFamilyDTO, openMetrics bool) ([]byte, error) {
	var output bytes.Buffer
	for _, family := range families {
		output.WriteString("# HELP ")
		output.WriteString(family.Name)
		output.WriteByte(' ')
		output.WriteString(prometheusHelpEscaper.Replace(family.Help))
		output.WriteByte('\n')
		output.WriteString("# TYPE ")
		output.WriteString(family.Name)
		output.WriteByte(' ')
		output.WriteString(family.Type)
		output.WriteByte('\n')
		for _, sample := range family.Samples {
			output.WriteString(sample.Name)
			if len(sample.Labels) > 0 {
				output.WriteByte('{')
				for index, label := range sample.Labels {
					if index > 0 {
						output.WriteByte(',')
					}
					output.WriteString(label.Name)
					output.WriteString("=\"")
					output.WriteString(prometheusLabelEscaper.Replace(label.Value))
					output.WriteByte('"')
				}
				output.WriteByte('}')
			}
			output.WriteByte(' ')
			output.WriteString(sample.Value)
			output.WriteByte('\n')
			if output.Len() > maxMetricsResponseBytes {
				return nil, observationports.ErrMetricsSnapshotTooLarge
			}
		}
	}
	if openMetrics {
		output.WriteString("# EOF\n")
	}
	if output.Len() > maxMetricsResponseBytes {
		return nil, observationports.ErrMetricsSnapshotTooLarge
	}
	return output.Bytes(), nil
}

func jsonInvalidMetricsMode() error {
	return errors.New("invalid metrics mode")
}
