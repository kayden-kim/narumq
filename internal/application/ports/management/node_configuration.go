package management

import "context"

// NodeConfigurationDTO describes the serving node's applied configuration without secret values or paths.
type NodeConfigurationDTO struct {
	Node                NodeIdentityConfigurationDTO
	Build               NodeBuildConfigurationDTO
	Peer                PeerConfigurationDTO
	Management          ManagementConfigurationDTO
	MQTT                MQTTListenerConfigurationDTO
	Replication         ReplicationConfigurationDTO
	PublishBackpressure PublishBackpressureConfigurationDTO
	RuntimeMetrics      RuntimeMetricsConfigurationDTO
	Security            SecurityConfigurationDTO
	EffectivePolicy     EffectivePolicyConfigurationDTO
}

// NodeIdentityConfigurationDTO contains non-secret identity and listener addresses.
type NodeIdentityConfigurationDTO struct {
	ClusterID         string
	NodeID            uint64
	Role              string
	ListenAddress     string
	AdvertisedAddress string
	LocalDNSName      string
}

// NodeBuildConfigurationDTO describes the running binary's advertised compatibility identity.
type NodeBuildConfigurationDTO struct {
	ReleaseVersion       string
	Features             []string
	ClusterProtocolMajor uint32
	ClusterProtocolMinor uint32
	StateSchemaMajor     uint32
	StateSchemaMinor     uint32
}

// PeerConfigurationDTO contains seed addresses, bootstrap identities, and secret-file presence only.
type PeerConfigurationDTO struct {
	SeedAddresses    []string
	BootstrapMembers []PeerBootstrapMemberConfigurationDTO
	TLS              PeerTLSConfigurationDTO
}

// PeerBootstrapMemberConfigurationDTO omits certificate fingerprints.
type PeerBootstrapMemberConfigurationDTO struct {
	MemberID uint64
	Address  string
	DNSSAN   string
}

// PeerTLSConfigurationDTO reports whether each peer TLS file is configured.
type PeerTLSConfigurationDTO struct {
	CertificateConfigured bool
	PrivateKeyConfigured  bool
	CAConfigured          bool
}

// JWTVerifierConfigurationDTO excludes credentials and removes URL user information before exposure.
type JWTVerifierConfigurationDTO struct {
	Issuer                string
	Audience              string
	Algorithms            []string
	JWKSURL               string
	CacheTTLMillis        uint32
	RefreshCooldownMillis uint32
	ClockSkewMillis       uint32
	HTTPTimeoutMillis     uint32
	IdentityCount         int
}

// JWTConfigurationDTO describes the MQTT JWT verifier without identity records.
type JWTConfigurationDTO struct {
	Issuer                string
	Audience              string
	Algorithms            []string
	JWKSURL               string
	CacheTTLMillis        uint32
	RefreshCooldownMillis uint32
	ClockSkewMillis       uint32
	HTTPTimeoutMillis     uint32
}

// ManagementConfigurationDTO contains management listener and authentication summaries.
type ManagementConfigurationDTO struct {
	ListenAddress             string
	RequestRate               int
	AuthFailureRate           int
	APIKeyCount               int
	OIDC                      *JWTVerifierConfigurationDTO
	BreakGlassTokenConfigured bool
	AuditDirectoryConfigured  bool
}

// MQTTListenerConfigurationDTO contains listener limits and secret-file presence only.
type MQTTListenerConfigurationDTO struct {
	TCPListenAddress         string
	TLSListenAddress         string
	SecurityConfigConfigured bool
	TCPMaxConnections        int
	TCPConnectionRate        int
	TLSMaxConnections        int
	TLSConnectionRate        int
	TLS                      *MQTTTLSConfigurationDTO
}

// MQTTTLSConfigurationDTO reports TLS policy and certificate-file presence.
type MQTTTLSConfigurationDTO struct {
	ClientAuth            string
	MinVersion            string
	CertificateConfigured bool
	PrivateKeyConfigured  bool
	CAConfigured          bool
}

// ReplicationConfigurationDTO contains the local replication profile.
type ReplicationConfigurationDTO struct {
	Factor      int
	WriteQuorum int
}

// PublishBackpressureConfigurationDTO contains local disk and commit-lag thresholds.
type PublishBackpressureConfigurationDTO struct {
	MinFreeBytes       uint64
	MaxCommitLagMillis uint64
}

// RuntimeMetricsConfigurationDTO reports whether runtime metrics and an output file are configured.
type RuntimeMetricsConfigurationDTO struct {
	Enabled              bool
	IntervalMillis       uint64
	OutputFileConfigured bool
}

// SecurityConfigurationDTO contains configured ACL rules and non-secret authentication metadata.
type SecurityConfigurationDTO struct {
	ConfigFileConfigured bool
	JWT                  *JWTConfigurationDTO
	IdentityCounts       IdentityCountsDTO
	ACL                  []ACLRuleConfigurationDTO
	TenantLimits         *TenantLimitsConfigurationDTO
}

// IdentityCountsDTO reports configured credential counts without exposing identity records.
type IdentityCountsDTO struct {
	JWT      int
	Password int
	MTLS     int
}

// ACLRuleConfigurationDTO describes one configured tenant-local authorization rule.
type ACLRuleConfigurationDTO struct {
	TenantID    string
	Role        string
	Effect      string
	Action      string
	TopicFilter string
}

// TenantLimitsConfigurationDTO contains the default limits and number of tenant overrides.
type TenantLimitsConfigurationDTO struct {
	MaxWaitMillis uint64
	Default       TenantLimitValuesDTO
	TenantCount   int
}

// TenantLimitValuesDTO contains one set of local admission limits.
type TenantLimitValuesDTO struct {
	MaxConnections    uint64
	MaxSessions       uint64
	MaxSubscriptions  uint64
	MessagesPerSecond uint64
	BytesPerSecond    uint64
	MaxPayloadBytes   uint64
}

// EffectivePolicyConfigurationDTO describes the applied policy source and feature state.
type EffectivePolicyConfigurationDTO struct {
	ClusterPolicy ClusterPolicyConfigurationStatusDTO
	MQTTLimits    EffectiveMQTTLimitsDTO
	TenantQuotas  EffectiveTenantQuotasDTO
}

// ClusterPolicyConfigurationStatusDTO reports local and committed policy versions.
type ClusterPolicyConfigurationStatusDTO struct {
	FeatureActive            bool
	TenantQuotaFeatureActive bool
	ConfiguredVersion        *uint64
	CommittedVersion         uint64
	Mismatch                 bool
}

// EffectiveMQTTLimitsDTO contains the effective values and their source.
type EffectiveMQTTLimitsDTO struct {
	Source  string
	Version *uint64
	Value   MQTTLimitsConfigurationDTO
}

// MQTTLimitsConfigurationDTO contains replicated connection limits in milliseconds and bytes.
type MQTTLimitsConfigurationDTO struct {
	MaxPacketSize           uint32
	ReceiveMaximum          uint32
	ConnectTimeoutMillis    uint32
	PacketReadTimeoutMillis uint32
	WriteTimeoutMillis      uint32
}

// EffectiveTenantQuotasDTO contains the effective quota profile and its source.
type EffectiveTenantQuotasDTO struct {
	FeatureActive bool
	Source        string
	Version       *uint64
	Value         *TenantQuotaProfileConfigurationDTO
}

// TenantQuotaProfileConfigurationDTO contains the version and effective tenant storage limits.
type TenantQuotaProfileConfigurationDTO struct {
	Version uint64
	Default TenantQuotaLimitsConfigurationDTO
	Tenants []TenantQuotaEntryConfigurationDTO
}

// TenantQuotaLimitsConfigurationDTO contains storage limits for one tenant or the default.
type TenantQuotaLimitsConfigurationDTO struct {
	RetainedBytes        uint64
	OfflineQueueMessages uint64
	OfflineQueueBytes    uint64
	StorageBytes         uint64
}

// TenantQuotaEntryConfigurationDTO contains one tenant's effective storage limits.
type TenantQuotaEntryConfigurationDTO struct {
	TenantID             string
	RetainedBytes        uint64
	OfflineQueueMessages uint64
	OfflineQueueBytes    uint64
	StorageBytes         uint64
}

// NodeConfigurationUseCase reads the applied configuration of the serving node.
type NodeConfigurationUseCase interface {
	GetNodeConfiguration(ctx context.Context) (NodeConfigurationDTO, error)
}
