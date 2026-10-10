package management

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"narumq/internal/application/membership"
)

// ClusterPolicyUseCase reads and commits the cluster-wide MQTT policy.
type ClusterPolicyUseCase interface {
	GetClusterPolicy(context.Context) (membership.ClusterPolicyStateDTO, bool, error)
	UpdateClusterPolicy(context.Context, membership.UpdateClusterPolicyRequestDTO) (membership.ClusterPolicyStateDTO, error)
}

type clusterPolicyHandler struct {
	service ClusterPolicyUseCase
}

type clusterPolicyRequest struct {
	MQTTLimits *clusterPolicyLimitsRequest `json:"mqttLimits"`
}

type clusterPolicyLimitsRequest struct {
	MaxPacketSize           *uint32 `json:"maxPacketSize"`
	ReceiveMaximum          *uint32 `json:"receiveMaximum"`
	ConnectTimeoutMillis    *uint32 `json:"connectTimeoutMillis"`
	PacketReadTimeoutMillis *uint32 `json:"packetReadTimeoutMillis"`
	WriteTimeoutMillis      *uint32 `json:"writeTimeoutMillis"`
}

type clusterPolicyResponse struct {
	Version uint64                      `json:"version"`
	Policy  *clusterPolicyValueResponse `json:"policy"`
}

type clusterPolicyValueResponse struct {
	MQTTLimits clusterPolicyLimitsResponse `json:"mqttLimits"`
}

type clusterPolicyLimitsResponse struct {
	MaxPacketSize           uint32 `json:"maxPacketSize"`
	ReceiveMaximum          uint32 `json:"receiveMaximum"`
	ConnectTimeoutMillis    uint32 `json:"connectTimeoutMillis"`
	PacketReadTimeoutMillis uint32 `json:"packetReadTimeoutMillis"`
	WriteTimeoutMillis      uint32 `json:"writeTimeoutMillis"`
}

func (handler *clusterPolicyHandler) Get(w http.ResponseWriter, r *http.Request) {
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if requestHasBody(r) {
		writeInvalidRequest(w, r, errors.New("cluster policy GET request must not contain a body"))
		return
	}
	state, found, err := handler.service.GetClusterPolicy(r.Context())
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	if !found {
		state = membership.ClusterPolicyStateDTO{}
	}
	w.Header().Set("ETag", clusterPolicyETag(state.Version))
	WriteDataEnvelope(w, r, http.StatusOK, clusterPolicyResponseBody(state))
}

func (handler *clusterPolicyHandler) Update(w http.ResponseWriter, r *http.Request) {
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	expectedVersion, ok := clusterPolicyVersionFromRequest(w, r)
	if !ok {
		return
	}
	idempotencyKey, ok := idempotencyKeyFromRequest(w, r)
	if !ok {
		return
	}
	var request clusterPolicyRequest
	if err := decodeStrictMembershipJSON(r, &request); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if request.MQTTLimits == nil || request.MQTTLimits.MaxPacketSize == nil || request.MQTTLimits.ReceiveMaximum == nil ||
		request.MQTTLimits.ConnectTimeoutMillis == nil || request.MQTTLimits.PacketReadTimeoutMillis == nil || request.MQTTLimits.WriteTimeoutMillis == nil {
		writeInvalidRequest(w, r, errors.New("mqttLimits and all five MQTT limit fields are required"))
		return
	}
	limits := request.MQTTLimits
	policy := membership.ClusterPolicyDTO{MQTTLimits: membership.MQTTLimitsDTO{
		MaxPacketSize: *limits.MaxPacketSize, ReceiveMaximum: *limits.ReceiveMaximum,
		ConnectTimeoutMillis: *limits.ConnectTimeoutMillis, PacketReadTimeoutMillis: *limits.PacketReadTimeoutMillis,
		WriteTimeoutMillis: *limits.WriteTimeoutMillis,
	}}
	if err := membership.ValidateClusterPolicy(policy); err != nil {
		writeInvalidRequest(w, r, errors.New("mqttLimits contains an out-of-range value"))
		return
	}
	state, err := handler.service.UpdateClusterPolicy(r.Context(), membership.UpdateClusterPolicyRequestDTO{
		ExpectedVersion: expectedVersion, IdempotencyKey: idempotencyKey, Policy: policy,
	})
	if err != nil {
		writeMembershipServiceError(w, r, err)
		return
	}
	setAuditDetails(r, map[string]any{"previousVersion": state.PreviousVersion, "newVersion": state.Version})
	w.Header().Set("ETag", clusterPolicyETag(state.Version))
	WriteDataEnvelope(w, r, http.StatusOK, clusterPolicyResponseBody(state))
}

func clusterPolicyVersionFromRequest(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	values := r.Header.Values("If-Match")
	if len(values) != 1 || len(values[0]) < 3 || len(values[0]) > 22 || values[0][0] != '"' || values[0][len(values[0])-1] != '"' {
		writeInvalidRequest(w, r, errors.New("one valid If-Match header is required"))
		return 0, false
	}
	value := values[0][1 : len(values[0])-1]
	version, err := strconv.ParseUint(value, 10, 64)
	if err != nil || version == ^uint64(0) || strconv.FormatUint(version, 10) != value {
		writeInvalidRequest(w, r, errors.New("one valid If-Match header is required"))
		return 0, false
	}
	return version, true
}

func clusterPolicyETag(version uint64) string {
	return `"` + strconv.FormatUint(version, 10) + `"`
}

func clusterPolicyResponseBody(state membership.ClusterPolicyStateDTO) clusterPolicyResponse {
	response := clusterPolicyResponse{Version: state.Version}
	if state.Version == 0 {
		return response
	}
	limits := state.Policy.MQTTLimits
	response.Policy = &clusterPolicyValueResponse{MQTTLimits: clusterPolicyLimitsResponse{
		MaxPacketSize: limits.MaxPacketSize, ReceiveMaximum: limits.ReceiveMaximum,
		ConnectTimeoutMillis: limits.ConnectTimeoutMillis, PacketReadTimeoutMillis: limits.PacketReadTimeoutMillis,
		WriteTimeoutMillis: limits.WriteTimeoutMillis,
	}}
	return response
}
