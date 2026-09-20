package management

import (
	"errors"
	"net/http"
	"strconv"

	appmgmt "narumq/internal/application/management"
	ports "narumq/internal/application/ports/management"
)

// V1Handler exposes the Management API v1 query endpoints.
type V1Handler struct {
	provider ports.StateProvider
}

// NewV1Handler creates a V1Handler.
func NewV1Handler(provider ports.StateProvider) *V1Handler {
	return &V1Handler{provider: provider}
}

func parseLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 {
		return 0, errors.New("limit must be a positive integer")
	}
	return limit, nil
}

// GetCluster handles GET /api/v1/cluster.
func (h *V1Handler) GetCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}

	summary := h.provider.GetClusterSummary(r.Context())
	WriteDataEnvelope(w, r, http.StatusOK, summary)
}

// ListNodes handles GET /api/v1/nodes.
func (h *V1Handler) ListNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}

	limit, err := parseLimit(r)
	if err != nil {
		WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), false, nil)
		return
	}

	stateStr := r.URL.Query().Get("state")
	var state ports.NodeState
	if stateStr != "" {
		switch ports.NodeState(stateStr) {
		case ports.NodeStateJoining, ports.NodeStateReady, ports.NodeStateDraining, ports.NodeStateSafeToStop:
			state = ports.NodeState(stateStr)
		default:
			WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid node state filter", false, nil)
			return
		}
	}

	filter := ports.NodeFilter{
		State:  state,
		Limit:  limit,
		Cursor: r.URL.Query().Get("cursor"),
	}

	nodes, page, err := h.provider.ListNodeSummaries(r.Context(), filter)
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

	nodeID := r.PathValue("nodeId")
	if nodeID == "" {
		WriteError(w, r, http.StatusBadRequest, "invalid_request", "nodeId is required", false, nil)
		return
	}

	node, err := h.provider.GetNodeSummary(r.Context(), nodeID)
	if err != nil {
		h.handleError(w, r, err)
		return
	}

	WriteDataEnvelope(w, r, http.StatusOK, node)
}

// ListClients handles GET /api/v1/clients.
func (h *V1Handler) ListClients(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", false, nil)
		return
	}

	limit, err := parseLimit(r)
	if err != nil {
		WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), false, nil)
		return
	}

	proto := r.URL.Query().Get("protocol")
	if proto != "" && proto != "MQTT_3_1_1" && proto != "MQTT_5_0" {
		WriteError(w, r, http.StatusBadRequest, "invalid_request", "invalid protocol filter", false, nil)
		return
	}

	filter := ports.ClientFilter{
		TenantID:       r.URL.Query().Get("tenantId"),
		ClientIDPrefix: r.URL.Query().Get("clientIdPrefix"),
		NodeID:         r.URL.Query().Get("nodeId"),
		Protocol:       proto,
		Limit:          limit,
		Cursor:         r.URL.Query().Get("cursor"),
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

	limit, err := parseLimit(r)
	if err != nil {
		WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), false, nil)
		return
	}

	filter := ports.SubscriptionFilter{
		TenantID: r.URL.Query().Get("tenantId"),
		ClientID: r.URL.Query().Get("clientId"),
		Limit:    limit,
		Cursor:   r.URL.Query().Get("cursor"),
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

	limit, err := parseLimit(r)
	if err != nil {
		WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), false, nil)
		return
	}

	filter := ports.RetainedFilter{
		TenantID:    r.URL.Query().Get("tenantId"),
		TopicPrefix: r.URL.Query().Get("topicPrefix"),
		Limit:       limit,
		Cursor:      r.URL.Query().Get("cursor"),
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

	limit, err := parseLimit(r)
	if err != nil {
		WriteError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), false, nil)
		return
	}

	filter := ports.TenantFilter{
		Limit:  limit,
		Cursor: r.URL.Query().Get("cursor"),
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
		WriteError(w, r, http.StatusBadRequest, "invalid_page_size", "page size exceeds allowed bounds", false, nil)
	default:
		WriteError(w, r, http.StatusInternalServerError, "internal_error", "an internal error occurred", false, nil)
	}
}
