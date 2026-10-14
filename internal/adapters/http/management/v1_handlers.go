package management

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"

	appmgmt "narumq/internal/application/management"
	ports "narumq/internal/application/ports/management"
	observationports "narumq/internal/application/ports/observability"
)

// V1Handler exposes the Management API v1 query endpoints.
type V1Handler struct {
	provider      ports.StateProvider
	observability observationports.QueryService
}

// NewV1Handler creates a V1Handler.
func NewV1Handler(provider ports.StateProvider) *V1Handler {
	return &V1Handler{provider: provider}
}

type queryParser struct {
	values url.Values
}

func newQueryParser(r *http.Request, allowedNames ...string) (queryParser, error) {
	allowed := make(map[string]struct{}, len(allowedNames))
	for _, name := range allowedNames {
		allowed[name] = struct{}{}
	}
	values := r.URL.Query()
	for name, rawValues := range values {
		if _, ok := allowed[name]; !ok {
			return queryParser{}, errors.New("unknown query parameter")
		}
		if len(rawValues) != 1 {
			return queryParser{}, errors.New("query parameter must appear once")
		}
	}
	return queryParser{values: values}, nil
}

func (query queryParser) optional(name string) (string, error) {
	rawValues, ok := query.values[name]
	if !ok {
		return "", nil
	}
	if rawValues[0] == "" {
		return "", errors.New("query parameter must not be empty")
	}
	return rawValues[0], nil
}

func (query queryParser) required(name string) (string, error) {
	value, err := query.optional(name)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", errors.New("required query parameter is missing")
	}
	return value, nil
}

func parseLimit(query queryParser) (int, error) {
	raw, err := query.optional("limit")
	if err != nil {
		return 0, err
	}
	if raw == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 {
		return 0, errors.New("limit must be a positive integer")
	}
	if limit > appmgmt.MaxPageLimit {
		return 0, errors.New("limit must be at most " + strconv.Itoa(appmgmt.MaxPageLimit))
	}
	return limit, nil
}

func writeInvalidRequest(w http.ResponseWriter, r *http.Request, err error) {
	WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), false, nil)
}

// GetCluster handles GET /api/v1/cluster.
func (h *V1Handler) GetCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}

	if h.observability == nil {
		writeObservabilityUnavailable(w, r)
		return
	}
	summary, err := h.observability.GetCluster(r.Context())
	if err != nil {
		writeObservabilityError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, summary)
}

// ListNodes handles GET /api/v1/nodes.
func (h *V1Handler) ListNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}

	query, err := newQueryParser(r, "limit", "cursor", "state")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	limit, err := parseLimit(query)
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	cursor, err := query.optional("cursor")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	stateStr, err := query.optional("state")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	var state observationports.NodeState
	if stateStr != "" {
		switch observationports.NodeState(stateStr) {
		case observationports.NodeStateJoining, observationports.NodeStateReady, observationports.NodeStateDraining, observationports.NodeStateSafeToStop:
			state = observationports.NodeState(stateStr)
		default:
			writeInvalidRequest(w, r, errors.New("invalid node state filter"))
			return
		}
	}

	filter := observationports.NodeFilter{
		State:  state,
		Limit:  limit,
		Cursor: cursor,
	}

	if h.observability == nil {
		writeObservabilityUnavailable(w, r)
		return
	}
	nodes, page, err := h.observability.ListNodes(r.Context(), filter)
	if err != nil {
		h.handleError(w, r, err)
		return
	}

	WriteListEnvelope(w, r, http.StatusOK, nodes, page)
}

// GetNode handles GET /api/v1/nodes/{nodeId}.
func (h *V1Handler) GetNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}

	nodeID := r.PathValue("nodeId")
	if nodeID == "" {
		WriteError(w, r, http.StatusBadRequest, "invalid_request", "nodeId is required", false, nil)
		return
	}

	if h.observability == nil {
		writeObservabilityUnavailable(w, r)
		return
	}
	node, err := h.observability.GetNode(r.Context(), nodeID)
	if err != nil {
		writeObservabilityError(w, r, err)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, node)
}

// GetNodeMetrics handles GET /api/v1/nodes/{nodeId}/metrics.
func (h *V1Handler) GetNodeMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if requestHasBody(r) {
		WriteError(w, r, http.StatusBadRequest, "request_body_not_allowed", "request body must be empty", false, nil)
		return
	}
	nodeID := r.PathValue("nodeId")
	if nodeID == "" {
		WriteError(w, r, http.StatusBadRequest, "invalid_request", "nodeId is required", false, nil)
		return
	}
	if h.observability == nil {
		writeObservabilityUnavailable(w, r)
		return
	}
	result, err := h.observability.GetNodeMetrics(r.Context(), nodeID)
	if err != nil {
		writeObservabilityError(w, r, err)
		return
	}
	writeMetricsDataEnvelope(w, r, result)
}

// GetMetrics handles GET /api/v1/metrics.
func (h *V1Handler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
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
	mode := observationports.MetricsModeAllNodesUnaggregated
	if modeText != "" {
		mode = observationports.MetricsMode(modeText)
	}
	if mode != observationports.MetricsModeNode && mode != observationports.MetricsModeAllNodesUnaggregated &&
		mode != observationports.MetricsModeAllNodesAggregated {
		writeInvalidRequest(w, r, errors.New("invalid metrics mode"))
		return
	}
	if h.observability == nil {
		writeObservabilityUnavailable(w, r)
		return
	}
	result, err := h.observability.CollectMetrics(r.Context(), mode)
	if err != nil {
		writeObservabilityError(w, r, err)
		return
	}
	writeMetricsDataEnvelope(w, r, result)
}

// ListClients handles GET /api/v1/clients.
func (h *V1Handler) ListClients(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}

	query, err := newQueryParser(r, "limit", "cursor", "tenantId", "clientIdPrefix", "nodeId", "protocol")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	limit, err := parseLimit(query)
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	cursor, err := query.optional("cursor")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	tenantID, err := query.optional("tenantId")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	clientIDPrefix, err := query.optional("clientIdPrefix")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	nodeID, err := query.optional("nodeId")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	proto, err := query.optional("protocol")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if proto != "" && proto != "MQTT_3_1_1" && proto != "MQTT_5_0" {
		writeInvalidRequest(w, r, errors.New("invalid protocol filter"))
		return
	}

	filter := ports.ClientFilter{
		TenantID:       tenantID,
		ClientIDPrefix: clientIDPrefix,
		NodeID:         nodeID,
		Protocol:       proto,
		Limit:          limit,
		Cursor:         cursor,
	}

	clients, page, err := h.provider.ListClients(r.Context(), filter)
	if err != nil {
		h.handleError(w, r, err)
		return
	}

	WriteListEnvelope(w, r, http.StatusOK, clients, page)
}

// GetClient handles GET /api/v1/clients/{tenantId}/{clientId}.
func (h *V1Handler) GetClient(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}

	tenantID := r.PathValue("tenantId")
	clientID := r.PathValue("clientId")
	if tenantID == "" || clientID == "" {
		WriteError(w, r, http.StatusBadRequest, "invalid_request", "tenantId and clientId are required", false, nil)
		return
	}

	client, err := h.provider.GetClient(r.Context(), tenantID, clientID)
	if err != nil {
		h.handleError(w, r, err)
		return
	}

	WriteDataEnvelope(w, r, http.StatusOK, client)
}

// ListSubscriptions handles GET /api/v1/subscriptions.
func (h *V1Handler) ListSubscriptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}

	query, err := newQueryParser(r, "limit", "cursor", "tenantId", "clientId")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	limit, err := parseLimit(query)
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	cursor, err := query.optional("cursor")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	tenantID, err := query.required("tenantId")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	clientID, err := query.optional("clientId")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}

	filter := ports.SubscriptionFilter{
		TenantID: tenantID,
		ClientID: clientID,
		Limit:    limit,
		Cursor:   cursor,
	}

	subs, page, err := h.provider.ListSubscriptions(r.Context(), filter)
	if err != nil {
		h.handleError(w, r, err)
		return
	}

	WriteListEnvelope(w, r, http.StatusOK, subs, page)
}

// ListRetained handles GET /api/v1/retained.
func (h *V1Handler) ListRetained(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}

	query, err := newQueryParser(r, "limit", "cursor", "tenantId", "topicPrefix")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	limit, err := parseLimit(query)
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	cursor, err := query.optional("cursor")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	tenantID, err := query.required("tenantId")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	topicPrefix, err := query.optional("topicPrefix")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}

	filter := ports.RetainedFilter{
		TenantID:    tenantID,
		TopicPrefix: topicPrefix,
		Limit:       limit,
		Cursor:      cursor,
	}

	retainedList, page, err := h.provider.ListRetained(r.Context(), filter)
	if err != nil {
		h.handleError(w, r, err)
		return
	}

	WriteListEnvelope(w, r, http.StatusOK, retainedList, page)
}

// ListTenants handles GET /api/v1/tenants.
func (h *V1Handler) ListTenants(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}

	query, err := newQueryParser(r, "limit", "cursor")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	limit, err := parseLimit(query)
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	cursor, err := query.optional("cursor")
	if err != nil {
		writeInvalidRequest(w, r, err)
		return
	}

	filter := ports.TenantFilter{
		Limit:  limit,
		Cursor: cursor,
	}

	tenants, page, err := h.provider.ListTenants(r.Context(), filter)
	if err != nil {
		h.handleError(w, r, err)
		return
	}

	WriteListEnvelope(w, r, http.StatusOK, tenants, page)
}

func (h *V1Handler) handleError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, appmgmt.ErrNodeNotFound):
		WriteError(w, r, http.StatusNotFound, "resource_not_found", "node not found", false, nil)
	case errors.Is(err, appmgmt.ErrClientNotFound):
		WriteError(w, r, http.StatusNotFound, "resource_not_found", "client session not found", false, nil)
	case errors.Is(err, appmgmt.ErrInvalidCursor):
		WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid pagination cursor", false, nil)
	case errors.Is(err, appmgmt.ErrInvalidPageSize):
		writeInvalidRequest(w, r, err)
	default:
		writeObservabilityError(w, r, err)
	}
}
