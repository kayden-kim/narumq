package management

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	ports "narumq/internal/application/ports/management"
)

type nodeConfigurationHandler struct {
	service ports.NodeConfigurationUseCase
	logger  *slog.Logger
}

func (handler *nodeConfigurationHandler) Get(w http.ResponseWriter, r *http.Request) {
	if _, err := newQueryParser(r); err != nil {
		writeInvalidRequest(w, r, err)
		return
	}
	if r.Body != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1))
		if err != nil || len(body) != 0 {
			writeInvalidRequest(w, r, errors.New("GET node configuration request must not contain a body"))
			return
		}
	}
	configuration, err := handler.service.GetNodeConfiguration(r.Context())
	if err != nil {
		logger := handler.logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("management node configuration query failed", "requestId", RequestIDFromContext(r.Context()), "error", err)
		WriteError(w, r, http.StatusInternalServerError, "configuration_unavailable", "node configuration is unavailable", false, nil)
		return
	}
	WriteDataEnvelope(w, r, http.StatusOK, nodeConfigurationResponse(configuration))
}

type nodeConfigurationDataResponse struct {
	Node                nodeIdentityConfigurationResponse        `json:"node"`
	Build               nodeBuildConfigurationResponse           `json:"build"`
	Peer                peerConfigurationResponse                `json:"peer"`
	Management          managementConfigurationResponse          `json:"management"`
	MQTT                mqttListenerConfigurationResponse        `json:"mqtt"`
	Replication         replicationConfigurationResponse         `json:"replication"`
	PublishBackpressure publishBackpressureConfigurationResponse `json:"publishBackpressure"`
	RuntimeMetrics      runtimeMetricsConfigurationResponse      `json:"runtimeMetrics"`
	Security            securityConfigurationResponse            `json:"security"`
	EffectivePolicy     effectivePolicyConfigurationResponse     `json:"effectivePolicy"`
}

type nodeIdentityConfigurationResponse struct {
	ClusterID         string `json:"clusterId"`
	NodeID            string `json:"nodeId"`
	Role              string `json:"role"`
	ListenAddress     string `json:"listenAddress"`
	AdvertisedAddress string `json:"advertisedAddress"`
	LocalDNSName      string `json:"localDNSName"`
}

type nodeBuildConfigurationResponse struct {
	ReleaseVersion       string   `json:"releaseVersion"`
	Features             []string `json:"features"`
	ClusterProtocolMajor uint32   `json:"clusterProtocolMajor"`
	ClusterProtocolMinor uint32   `json:"clusterProtocolMinor"`
	StateSchemaMajor     uint32   `json:"stateSchemaMajor"`
	StateSchemaMinor     uint32   `json:"stateSchemaMinor"`
}

type peerConfigurationResponse struct {
	SeedAddresses    []string                      `json:"seedAddresses"`
	BootstrapMembers []peerBootstrapMemberResponse `json:"bootstrapMembers"`
	TLS              peerTLSConfigurationResponse  `json:"tls"`
}

type peerBootstrapMemberResponse struct {
	MemberID uint64 `json:"memberId"`
	Address  string `json:"address"`
	DNSSAN   string `json:"dnsSan"`
}

type peerTLSConfigurationResponse struct {
	CertificateConfigured bool `json:"certificateConfigured"`
	PrivateKeyConfigured  bool `json:"privateKeyConfigured"`
	CAConfigured          bool `json:"caConfigured"`
}

type jwtVerifierConfigurationResponse struct {
	Issuer                string   `json:"issuer"`
	Audience              string   `json:"audience"`
	Algorithms            []string `json:"algorithms"`
	JWKSURL               string   `json:"jwksUrl"`
	CacheTTLMillis        uint32   `json:"cacheTtlMillis"`
	RefreshCooldownMillis uint32   `json:"refreshCooldownMillis"`
	ClockSkewMillis       uint32   `json:"clockSkewMillis"`
	HTTPTimeoutMillis     uint32   `json:"httpTimeoutMillis"`
	IdentityCount         int      `json:"identityCount"`
}

type mqttJWTConfigurationResponse struct {
	Issuer                string   `json:"issuer"`
	Audience              string   `json:"audience"`
	Algorithms            []string `json:"algorithms"`
	JWKSURL               string   `json:"jwksUrl"`
	CacheTTLMillis        uint32   `json:"cacheTtlMillis"`
	RefreshCooldownMillis uint32   `json:"refreshCooldownMillis"`
	ClockSkewMillis       uint32   `json:"clockSkewMillis"`
	HTTPTimeoutMillis     uint32   `json:"httpTimeoutMillis"`
}

type managementConfigurationResponse struct {
	ListenAddress             string                            `json:"listenAddress"`
	RequestRate               int                               `json:"requestRate"`
	AuthFailureRate           int                               `json:"authFailureRate"`
	APIKeyCount               int                               `json:"apiKeyCount"`
	OIDC                      *jwtVerifierConfigurationResponse `json:"oidc"`
	BreakGlassTokenConfigured bool                              `json:"breakGlassTokenConfigured"`
	AuditDirectoryConfigured  bool                              `json:"auditDirectoryConfigured"`
}

type mqttTLSConfigurationResponse struct {
	ClientAuth            string `json:"clientAuth"`
	MinVersion            string `json:"minVersion"`
	CertificateConfigured bool   `json:"certificateConfigured"`
	PrivateKeyConfigured  bool   `json:"privateKeyConfigured"`
	CAConfigured          bool   `json:"caConfigured"`
}

type mqttListenerConfigurationResponse struct {
	TCPListenAddress         string                        `json:"tcpListenAddress"`
	TLSListenAddress         string                        `json:"tlsListenAddress"`
	SecurityConfigConfigured bool                          `json:"securityConfigConfigured"`
	TCPMaxConnections        int                           `json:"tcpMaxConnections"`
	TCPConnectionRate        int                           `json:"tcpConnectionRate"`
	TLSMaxConnections        int                           `json:"tlsMaxConnections"`
	TLSConnectionRate        int                           `json:"tlsConnectionRate"`
	TLS                      *mqttTLSConfigurationResponse `json:"tls"`
}

type replicationConfigurationResponse struct {
	Factor      int `json:"factor"`
	WriteQuorum int `json:"writeQuorum"`
}

type publishBackpressureConfigurationResponse struct {
	MinFreeBytes       uint64 `json:"minFreeBytes"`
	MaxCommitLagMillis uint64 `json:"maxCommitLagMillis"`
}

type runtimeMetricsConfigurationResponse struct {
	Enabled              bool   `json:"enabled"`
	IntervalMillis       uint64 `json:"intervalMillis"`
	OutputFileConfigured bool   `json:"outputFileConfigured"`
}

type tenantLimitValuesResponse struct {
	MaxConnections    uint64 `json:"maxConnections"`
	MaxSessions       uint64 `json:"maxSessions"`
	MaxSubscriptions  uint64 `json:"maxSubscriptions"`
	MessagesPerSecond uint64 `json:"messagesPerSecond"`
	BytesPerSecond    uint64 `json:"bytesPerSecond"`
	MaxPayloadBytes   uint64 `json:"maxPayloadBytes"`
}

type tenantLimitsConfigurationResponse struct {
	MaxWaitMillis uint64                    `json:"maxWaitMillis"`
	Default       tenantLimitValuesResponse `json:"default"`
	TenantCount   int                       `json:"tenantCount"`
}

type securityConfigurationResponse struct {
	ConfigFileConfigured bool                               `json:"configFileConfigured"`
	JWT                  *mqttJWTConfigurationResponse      `json:"jwt"`
	IdentityCounts       securityIdentityCountsResponse     `json:"identityCounts"`
	ACL                  []aclRuleConfigurationResponse     `json:"acl"`
	TenantLimits         *tenantLimitsConfigurationResponse `json:"tenantLimits"`
}

type securityIdentityCountsResponse struct {
	JWT      int `json:"jwt"`
	Password int `json:"password"`
	MTLS     int `json:"mtls"`
}

type aclRuleConfigurationResponse struct {
	TenantID    string `json:"tenantId"`
	Role        string `json:"role"`
	Effect      string `json:"effect"`
	Action      string `json:"action"`
	TopicFilter string `json:"topicFilter"`
}

type mqttLimitsConfigurationResponse struct {
	MaxPacketSize           uint32 `json:"maxPacketSize"`
	ReceiveMaximum          uint32 `json:"receiveMaximum"`
	ConnectTimeoutMillis    uint32 `json:"connectTimeoutMillis"`
	PacketReadTimeoutMillis uint32 `json:"packetReadTimeoutMillis"`
	WriteTimeoutMillis      uint32 `json:"writeTimeoutMillis"`
}

type effectiveMQTTLimitsResponse struct {
	Source  string                          `json:"source"`
	Version *uint64                         `json:"version"`
	Value   mqttLimitsConfigurationResponse `json:"value"`
}

type tenantQuotaLimitsResponse struct {
	RetainedBytes        uint64 `json:"retainedBytes"`
	OfflineQueueMessages uint64 `json:"offlineQueueMessages"`
	OfflineQueueBytes    uint64 `json:"offlineQueueBytes"`
	StorageBytes         uint64 `json:"storageBytes"`
}

type tenantQuotaEntryResponse struct {
	TenantID             string `json:"tenantId"`
	RetainedBytes        uint64 `json:"retainedBytes"`
	OfflineQueueMessages uint64 `json:"offlineQueueMessages"`
	OfflineQueueBytes    uint64 `json:"offlineQueueBytes"`
	StorageBytes         uint64 `json:"storageBytes"`
}

type tenantQuotaProfileResponse struct {
	Version uint64                     `json:"version"`
	Default tenantQuotaLimitsResponse  `json:"default"`
	Tenants []tenantQuotaEntryResponse `json:"tenants"`
}

type effectiveTenantQuotasResponse struct {
	FeatureActive bool                        `json:"featureActive"`
	Source        string                      `json:"source"`
	Version       *uint64                     `json:"version"`
	Value         *tenantQuotaProfileResponse `json:"value"`
}

type clusterPolicyConfigurationStatusResponse struct {
	FeatureActive            bool    `json:"featureActive"`
	TenantQuotaFeatureActive bool    `json:"tenantQuotaFeatureActive"`
	ConfiguredVersion        *uint64 `json:"configuredVersion"`
	CommittedVersion         uint64  `json:"committedVersion"`
	Mismatch                 bool    `json:"mismatch"`
}

type effectivePolicyConfigurationResponse struct {
	ClusterPolicy clusterPolicyConfigurationStatusResponse `json:"clusterPolicy"`
	MQTTLimits    effectiveMQTTLimitsResponse              `json:"mqttLimits"`
	TenantQuotas  effectiveTenantQuotasResponse            `json:"tenantQuotas"`
}

func nodeConfigurationResponse(configuration ports.NodeConfigurationDTO) nodeConfigurationDataResponse {
	bootstrapMembers := make([]peerBootstrapMemberResponse, len(configuration.Peer.BootstrapMembers))
	for index, member := range configuration.Peer.BootstrapMembers {
		bootstrapMembers[index] = peerBootstrapMemberResponse{MemberID: member.MemberID, Address: member.Address, DNSSAN: member.DNSSAN}
	}
	response := nodeConfigurationDataResponse{
		Node: nodeIdentityConfigurationResponse{
			ClusterID: configuration.Node.ClusterID, NodeID: strconv.FormatUint(configuration.Node.NodeID, 10),
			Role: configuration.Node.Role, ListenAddress: configuration.Node.ListenAddress,
			AdvertisedAddress: configuration.Node.AdvertisedAddress, LocalDNSName: configuration.Node.LocalDNSName,
		},
		Build: nodeBuildConfigurationResponse{
			ReleaseVersion: configuration.Build.ReleaseVersion, Features: nonNilStrings(configuration.Build.Features),
			ClusterProtocolMajor: configuration.Build.ClusterProtocolMajor, ClusterProtocolMinor: configuration.Build.ClusterProtocolMinor,
			StateSchemaMajor: configuration.Build.StateSchemaMajor, StateSchemaMinor: configuration.Build.StateSchemaMinor,
		},
		Peer: peerConfigurationResponse{
			SeedAddresses: nonNilStrings(configuration.Peer.SeedAddresses), BootstrapMembers: bootstrapMembers,
			TLS: peerTLSConfigurationResponse{
				CertificateConfigured: configuration.Peer.TLS.CertificateConfigured,
				PrivateKeyConfigured:  configuration.Peer.TLS.PrivateKeyConfigured,
				CAConfigured:          configuration.Peer.TLS.CAConfigured,
			},
		},
		Management: managementConfigurationResponse{
			ListenAddress: configuration.Management.ListenAddress, RequestRate: configuration.Management.RequestRate,
			AuthFailureRate: configuration.Management.AuthFailureRate, APIKeyCount: configuration.Management.APIKeyCount,
			OIDC:                      jwtVerifierResponse(configuration.Management.OIDC),
			BreakGlassTokenConfigured: configuration.Management.BreakGlassTokenConfigured,
			AuditDirectoryConfigured:  configuration.Management.AuditDirectoryConfigured,
		},
		MQTT: mqttListenerConfigurationResponse{
			TCPListenAddress: configuration.MQTT.TCPListenAddress, TLSListenAddress: configuration.MQTT.TLSListenAddress,
			SecurityConfigConfigured: configuration.MQTT.SecurityConfigConfigured,
			TCPMaxConnections:        configuration.MQTT.TCPMaxConnections, TCPConnectionRate: configuration.MQTT.TCPConnectionRate,
			TLSMaxConnections: configuration.MQTT.TLSMaxConnections, TLSConnectionRate: configuration.MQTT.TLSConnectionRate,
			TLS: mqttTLSResponse(configuration.MQTT.TLS),
		},
		Replication: replicationConfigurationResponse{Factor: configuration.Replication.Factor, WriteQuorum: configuration.Replication.WriteQuorum},
		PublishBackpressure: publishBackpressureConfigurationResponse{
			MinFreeBytes: configuration.PublishBackpressure.MinFreeBytes, MaxCommitLagMillis: configuration.PublishBackpressure.MaxCommitLagMillis,
		},
		RuntimeMetrics: runtimeMetricsConfigurationResponse{
			Enabled: configuration.RuntimeMetrics.Enabled, IntervalMillis: configuration.RuntimeMetrics.IntervalMillis,
			OutputFileConfigured: configuration.RuntimeMetrics.OutputFileConfigured,
		},
		Security: securityConfigurationResponse{
			ConfigFileConfigured: configuration.Security.ConfigFileConfigured,
			JWT:                  mqttJWTResponse(configuration.Security.JWT),
			IdentityCounts: securityIdentityCountsResponse{
				JWT: configuration.Security.IdentityCounts.JWT, Password: configuration.Security.IdentityCounts.Password,
				MTLS: configuration.Security.IdentityCounts.MTLS,
			},
			ACL: aclRulesResponse(configuration.Security.ACL), TenantLimits: tenantLimitsResponse(configuration.Security.TenantLimits),
		},
		EffectivePolicy: effectivePolicyConfigurationResponse{
			ClusterPolicy: clusterPolicyStatusResponse(configuration.EffectivePolicy.ClusterPolicy),
			MQTTLimits: effectiveMQTTLimitsResponse{
				Source: configuration.EffectivePolicy.MQTTLimits.Source, Version: configuration.EffectivePolicy.MQTTLimits.Version,
				Value: mqttLimitsResponse(configuration.EffectivePolicy.MQTTLimits.Value),
			},
			TenantQuotas: effectiveTenantQuotasResponse{
				FeatureActive: configuration.EffectivePolicy.TenantQuotas.FeatureActive,
				Source:        configuration.EffectivePolicy.TenantQuotas.Source, Version: configuration.EffectivePolicy.TenantQuotas.Version,
				Value: tenantQuotaProfileResponseFromDTO(configuration.EffectivePolicy.TenantQuotas.Value),
			},
		},
	}
	return response
}

func jwtVerifierResponse(configuration *ports.JWTVerifierConfigurationDTO) *jwtVerifierConfigurationResponse {
	if configuration == nil {
		return nil
	}
	return &jwtVerifierConfigurationResponse{
		Issuer: configuration.Issuer, Audience: configuration.Audience, Algorithms: nonNilStrings(configuration.Algorithms),
		JWKSURL: configuration.JWKSURL, CacheTTLMillis: configuration.CacheTTLMillis,
		RefreshCooldownMillis: configuration.RefreshCooldownMillis, ClockSkewMillis: configuration.ClockSkewMillis,
		HTTPTimeoutMillis: configuration.HTTPTimeoutMillis, IdentityCount: configuration.IdentityCount,
	}
}

func mqttJWTResponse(configuration *ports.JWTConfigurationDTO) *mqttJWTConfigurationResponse {
	if configuration == nil {
		return nil
	}
	return &mqttJWTConfigurationResponse{
		Issuer: configuration.Issuer, Audience: configuration.Audience, Algorithms: nonNilStrings(configuration.Algorithms),
		JWKSURL: configuration.JWKSURL, CacheTTLMillis: configuration.CacheTTLMillis,
		RefreshCooldownMillis: configuration.RefreshCooldownMillis, ClockSkewMillis: configuration.ClockSkewMillis,
		HTTPTimeoutMillis: configuration.HTTPTimeoutMillis,
	}
}

func aclRulesResponse(rules []ports.ACLRuleConfigurationDTO) []aclRuleConfigurationResponse {
	responses := make([]aclRuleConfigurationResponse, len(rules))
	for index, rule := range rules {
		responses[index] = aclRuleConfigurationResponse{
			TenantID: rule.TenantID, Role: rule.Role, Effect: rule.Effect,
			Action: rule.Action, TopicFilter: rule.TopicFilter,
		}
	}
	return responses
}

func mqttTLSResponse(configuration *ports.MQTTTLSConfigurationDTO) *mqttTLSConfigurationResponse {
	if configuration == nil {
		return nil
	}
	return &mqttTLSConfigurationResponse{
		ClientAuth: configuration.ClientAuth, MinVersion: configuration.MinVersion,
		CertificateConfigured: configuration.CertificateConfigured, PrivateKeyConfigured: configuration.PrivateKeyConfigured,
		CAConfigured: configuration.CAConfigured,
	}
}

func tenantLimitsResponse(configuration *ports.TenantLimitsConfigurationDTO) *tenantLimitsConfigurationResponse {
	if configuration == nil {
		return nil
	}
	return &tenantLimitsConfigurationResponse{
		MaxWaitMillis: configuration.MaxWaitMillis,
		Default: tenantLimitValuesResponse{
			MaxConnections: configuration.Default.MaxConnections, MaxSessions: configuration.Default.MaxSessions,
			MaxSubscriptions: configuration.Default.MaxSubscriptions, MessagesPerSecond: configuration.Default.MessagesPerSecond,
			BytesPerSecond: configuration.Default.BytesPerSecond, MaxPayloadBytes: configuration.Default.MaxPayloadBytes,
		},
		TenantCount: configuration.TenantCount,
	}
}

func mqttLimitsResponse(configuration ports.MQTTLimitsConfigurationDTO) mqttLimitsConfigurationResponse {
	return mqttLimitsConfigurationResponse{
		MaxPacketSize: configuration.MaxPacketSize, ReceiveMaximum: configuration.ReceiveMaximum,
		ConnectTimeoutMillis: configuration.ConnectTimeoutMillis, PacketReadTimeoutMillis: configuration.PacketReadTimeoutMillis,
		WriteTimeoutMillis: configuration.WriteTimeoutMillis,
	}
}

func clusterPolicyStatusResponse(configuration ports.ClusterPolicyConfigurationStatusDTO) clusterPolicyConfigurationStatusResponse {
	return clusterPolicyConfigurationStatusResponse{
		FeatureActive: configuration.FeatureActive, TenantQuotaFeatureActive: configuration.TenantQuotaFeatureActive,
		ConfiguredVersion: configuration.ConfiguredVersion, CommittedVersion: configuration.CommittedVersion, Mismatch: configuration.Mismatch,
	}
}

func tenantQuotaProfileResponseFromDTO(configuration *ports.TenantQuotaProfileConfigurationDTO) *tenantQuotaProfileResponse {
	if configuration == nil {
		return nil
	}
	tenants := make([]tenantQuotaEntryResponse, len(configuration.Tenants))
	for index, tenant := range configuration.Tenants {
		tenants[index] = tenantQuotaEntryResponse{
			TenantID: tenant.TenantID, RetainedBytes: tenant.RetainedBytes,
			OfflineQueueMessages: tenant.OfflineQueueMessages, OfflineQueueBytes: tenant.OfflineQueueBytes,
			StorageBytes: tenant.StorageBytes,
		}
	}
	return &tenantQuotaProfileResponse{
		Version: configuration.Version,
		Default: tenantQuotaLimitsResponse{
			RetainedBytes: configuration.Default.RetainedBytes, OfflineQueueMessages: configuration.Default.OfflineQueueMessages,
			OfflineQueueBytes: configuration.Default.OfflineQueueBytes, StorageBytes: configuration.Default.StorageBytes,
		},
		Tenants: tenants,
	}
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
