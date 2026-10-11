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
	MQTTLimits   *clusterPolicyLimitsRequest `json:"mqttLimits"`
	TenantQuotas *clusterTenantQuotasRequest `json:"tenantQuotas"`
}

type clusterPolicyLimitsRequest struct {
	MaxPacketSize           *uint32 `json:"maxPacketSize"`
	ReceiveMaximum          *uint32 `json:"receiveMaximum"`
	ConnectTimeoutMillis    *uint32 `json:"connectTimeoutMillis"`
	PacketReadTimeoutMillis *uint32 `json:"packetReadTimeoutMillis"`
	WriteTimeoutMillis      *uint32 `json:"writeTimeoutMillis"`
}

type clusterTenantQuotasRequest struct {
	Default *clusterTenantQuotaLimitsRequest  `json:"default"`
	Tenants *[]clusterTenantQuotaEntryRequest `json:"tenants"`
}

type clusterTenantQuotaLimitsRequest struct {
	RetainedBytes        *uint64 `json:"retainedBytes"`
	OfflineQueueMessages *uint64 `json:"offlineQueueMessages"`
	OfflineQueueBytes    *uint64 `json:"offlineQueueBytes"`
	StorageBytes         *uint64 `json:"storageBytes"`
}

type clusterTenantQuotaEntryRequest struct {
	TenantID             *string `json:"tenantId"`
	RetainedBytes        *uint64 `json:"retainedBytes"`
	OfflineQueueMessages *uint64 `json:"offlineQueueMessages"`
	OfflineQueueBytes    *uint64 `json:"offlineQueueBytes"`
	StorageBytes         *uint64 `json:"storageBytes"`
}
type clusterPolicyResponse struct {
	Version uint64                      `json:"version"`
	Policy  *clusterPolicyValueResponse `json:"policy"`
}

type clusterPolicyValueResponse struct {
	MQTTLimits   clusterPolicyLimitsResponse        `json:"mqttLimits"`
	TenantQuotas *clusterPolicyTenantQuotasResponse `json:"tenantQuotas"`
}

type clusterPolicyTenantQuotasResponse struct {
	Default tenantQuotaLimitsResponse  `json:"default"`
	Tenants []tenantQuotaEntryResponse `json:"tenants"`
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
	if err := decodeStrictClusterPolicyJSON(r, &request); err != nil {
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
	if request.TenantQuotas != nil {
		quotas, err := clusterTenantQuotasFromRequest(request.TenantQuotas)
		if err != nil {
			writeInvalidRequest(w, r, err)
			return
		}
		policy.TenantQuotas = quotas
	}
	if err := membership.ValidateClusterPolicy(policy); err != nil {
		writeInvalidRequest(w, r, errors.New("mqttLimits or tenantQuotas contains an out-of-range value"))
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

func clusterTenantQuotasFromRequest(request *clusterTenantQuotasRequest) (*membership.ClusterTenantQuotasDTO, error) {
	if request == nil || request.Default == nil || request.Tenants == nil {
		return nil, errors.New("tenantQuotas requires default and tenants")
	}
	defaultQuota, err := clusterTenantQuotaLimitsFromRequest(request.Default)
	if err != nil {
		return nil, err
	}
	tenants := make([]membership.TenantStorageQuotaDTO, len(*request.Tenants))
	for index, requestTenant := range *request.Tenants {
		tenant, err := clusterTenantQuotaEntryFromRequest(requestTenant)
		if err != nil {
			return nil, err
		}
		tenants[index] = tenant
	}
	return &membership.ClusterTenantQuotasDTO{Default: defaultQuota, Tenants: tenants}, nil
}

func clusterTenantQuotaEntryFromRequest(request clusterTenantQuotaEntryRequest) (membership.TenantStorageQuotaDTO, error) {
	if request.TenantID == nil {
		return membership.TenantStorageQuotaDTO{}, errors.New("tenantQuotas tenantId is required")
	}
	quota, err := clusterTenantQuotaLimitsFromRequest(&clusterTenantQuotaLimitsRequest{
		RetainedBytes: request.RetainedBytes, OfflineQueueMessages: request.OfflineQueueMessages,
		OfflineQueueBytes: request.OfflineQueueBytes, StorageBytes: request.StorageBytes,
	})
	if err != nil {
		return membership.TenantStorageQuotaDTO{}, err
	}
	quota.TenantID = *request.TenantID
	return quota, nil
}

func clusterTenantQuotaLimitsFromRequest(request *clusterTenantQuotaLimitsRequest) (membership.TenantStorageQuotaDTO, error) {
	if request == nil || request.RetainedBytes == nil || request.OfflineQueueMessages == nil ||
		request.OfflineQueueBytes == nil || request.StorageBytes == nil {
		return membership.TenantStorageQuotaDTO{}, errors.New("tenantQuotas requires every quota limit")
	}
	return membership.TenantStorageQuotaDTO{
		RetainedBytes: *request.RetainedBytes, OfflineQueueMessages: *request.OfflineQueueMessages,
		OfflineQueueBytes: *request.OfflineQueueBytes, StorageBytes: *request.StorageBytes,
	}, nil
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
	response.Policy = &clusterPolicyValueResponse{
		MQTTLimits: clusterPolicyLimitsResponse{
			MaxPacketSize: limits.MaxPacketSize, ReceiveMaximum: limits.ReceiveMaximum,
			ConnectTimeoutMillis: limits.ConnectTimeoutMillis, PacketReadTimeoutMillis: limits.PacketReadTimeoutMillis,
			WriteTimeoutMillis: limits.WriteTimeoutMillis,
		},
		TenantQuotas: clusterTenantQuotasResponseFromDTO(state.Policy.TenantQuotas),
	}
	return response
}

func clusterTenantQuotasResponseFromDTO(quotas *membership.ClusterTenantQuotasDTO) *clusterPolicyTenantQuotasResponse {
	if quotas == nil {
		return nil
	}
	tenants := make([]tenantQuotaEntryResponse, len(quotas.Tenants))
	for index, tenant := range quotas.Tenants {
		tenants[index] = tenantQuotaEntryResponse{
			TenantID: tenant.TenantID, RetainedBytes: tenant.RetainedBytes,
			OfflineQueueMessages: tenant.OfflineQueueMessages, OfflineQueueBytes: tenant.OfflineQueueBytes,
			StorageBytes: tenant.StorageBytes,
		}
	}
	return &clusterPolicyTenantQuotasResponse{
		Default: tenantQuotaLimitsResponse{
			RetainedBytes: quotas.Default.RetainedBytes, OfflineQueueMessages: quotas.Default.OfflineQueueMessages,
			OfflineQueueBytes: quotas.Default.OfflineQueueBytes, StorageBytes: quotas.Default.StorageBytes,
		},
		Tenants: tenants,
	}
}
