package main

import (
	"context"
	"errors"
	"net/url"
	"slices"

	authenticationadapter "narumq/internal/adapters/authentication"
	adaptershttp "narumq/internal/adapters/http/management"
	"narumq/internal/application/membership"
	ports "narumq/internal/application/ports/management"
)

func (runtime *clusterRuntime) GetNodeConfiguration(ctx context.Context) (ports.NodeConfigurationDTO, error) {
	if runtime == nil || runtime.machine == nil {
		return ports.NodeConfigurationDTO{}, errors.New("node configuration state is unavailable")
	}
	if ctx == nil {
		return ports.NodeConfigurationDTO{}, errors.New("node configuration context is nil")
	}
	if err := ctx.Err(); err != nil {
		return ports.NodeConfigurationDTO{}, err
	}

	config := runtime.config
	activeFeatures := runtime.machine.ActiveFeatures()
	clusterPolicyFeatureActive := slices.Contains(activeFeatures, membership.ClusterPolicyFeature)
	tenantQuotaFeatureActive := slices.Contains(activeFeatures, membership.ClusterPolicyTenantQuotasFeature)

	committedPolicy, hasCommittedPolicy := runtime.machine.ClusterPolicy()
	var committedPolicyState *membership.ClusterPolicyStateDTO
	committedPolicyVersion := uint64(0)
	if hasCommittedPolicy {
		committedPolicyState = &committedPolicy
		committedPolicyVersion = committedPolicy.Version
	}
	_, policyMismatch := clusterPolicyReconciliationDecision(
		config.ClusterPolicy, committedPolicyState, clusterPolicyFeatureActive, tenantQuotaFeatureActive,
	)
	var configuredPolicyVersion *uint64
	if config.ClusterPolicy != nil {
		version := config.ClusterPolicy.Version
		configuredPolicyVersion = &version
	}

	mqttLimits, mqttLimitsSource, mqttLimitsVersion := effectiveClusterMQTTLimits(config, committedPolicyState, clusterPolicyFeatureActive)
	quotaProfile, quotaSource, hasQuotaProfile := runtime.machine.EffectiveTenantQuotaProfile()
	effectiveTenantQuotas := ports.EffectiveTenantQuotasDTO{
		FeatureActive: tenantQuotaFeatureActive,
		Source:        quotaSource,
	}
	if hasQuotaProfile {
		version := quotaProfile.Version
		effectiveTenantQuotas.Version = &version
		effectiveTenantQuotas.Value = tenantQuotaProfileConfiguration(quotaProfile)
	}

	minFreeBytes, maxCommitLag := config.publishBackpressure()
	replication := config.replicationProfile()
	securityConfigured := config.MQTT.SecurityConfigFile != ""

	managementOIDC, err := managementJWTConfiguration(config.Management.OIDC)
	if err != nil {
		return ports.NodeConfigurationDTO{}, err
	}
	var securityJWT *ports.JWTConfigurationDTO
	identityCounts := ports.IdentityCountsDTO{}
	aclRules := make([]ports.ACLRuleConfigurationDTO, 0)
	var tenantLimits *ports.TenantLimitsConfigurationDTO
	if runtime.securityBundle != nil {
		securityBundle := runtime.securityBundle
		securityJWT, err = securityJWTConfiguration(securityBundle.JWTConfig)
		if err != nil {
			return ports.NodeConfigurationDTO{}, err
		}
		jwtIdentityCount := 0
		if securityBundle.JWTConfig != nil {
			jwtIdentityCount = len(securityBundle.JWTConfig.Bindings)
		}
		identityCounts = ports.IdentityCountsDTO{
			JWT: jwtIdentityCount, Password: len(securityBundle.PasswordBindings),
			MTLS: len(securityBundle.MTLSBindings),
		}
		rules := securityBundle.Policy.Rules()
		aclRules = make([]ports.ACLRuleConfigurationDTO, 0, len(rules))
		for _, rule := range rules {
			aclRules = append(aclRules, ports.ACLRuleConfigurationDTO{
				TenantID: rule.TenantID().String(), Role: string(rule.Role()), Effect: string(rule.Effect()),
				Action: string(rule.Action()), TopicFilter: rule.TopicFilter().Filter(),
			})
		}
		if securityBundle.TenantLimits != nil {
			profile := securityBundle.TenantLimits
			tenantLimits = &ports.TenantLimitsConfigurationDTO{
				MaxWaitMillis: uint64(profile.MaxWait.Milliseconds()),
				Default: ports.TenantLimitValuesDTO{
					MaxConnections: profile.Default.MaxConnections, MaxSessions: profile.Default.MaxSessions,
					MaxSubscriptions: profile.Default.MaxSubscriptions, MessagesPerSecond: profile.Default.MessagesPerSecond,
					BytesPerSecond: profile.Default.BytesPerSecond, MaxPayloadBytes: profile.Default.MaxPayloadBytes,
				},
				TenantCount: len(profile.Tenants),
			}
		}
	}

	result := ports.NodeConfigurationDTO{
		Node: ports.NodeIdentityConfigurationDTO{
			ClusterID: config.ClusterID, NodeID: config.NodeID, Role: config.Role,
			ListenAddress: config.ListenAddress, AdvertisedAddress: config.AdvertisedAddress, LocalDNSName: config.LocalDNSName,
		},
		Build: ports.NodeBuildConfigurationDTO{
			ReleaseVersion: config.ReleaseVersion, Features: slices.Clone(config.Features),
			ClusterProtocolMajor: config.ClusterProtocolMajor, ClusterProtocolMinor: config.ClusterProtocolMinor,
			StateSchemaMajor: config.StateSchemaMajor, StateSchemaMinor: config.StateSchemaMinor,
		},
		Peer: ports.PeerConfigurationDTO{
			SeedAddresses:    slices.Clone(config.SeedAddresses),
			BootstrapMembers: nodeConfigurationBootstrapMembers(config.BootstrapMembers),
			TLS: ports.PeerTLSConfigurationDTO{
				CertificateConfigured: config.PeerTLS.CertFile != "",
				PrivateKeyConfigured:  config.PeerTLS.KeyFile != "",
				CAConfigured:          config.PeerTLS.CAFile != "",
			},
		},
		Management: ports.ManagementConfigurationDTO{
			ListenAddress:   config.Management.ListenAddress,
			RequestRate:     configuredManagementRate(config.Management.RequestRate, adaptershttp.DefaultRequestRate),
			AuthFailureRate: configuredManagementRate(config.Management.AuthFailureRate, adaptershttp.DefaultAuthFailureRate),
			APIKeyCount:     len(config.Management.APIKeys), OIDC: managementOIDC,
			BreakGlassTokenConfigured: config.Management.ClusterAdminTokenFile != "",
			AuditDirectoryConfigured:  config.Management.AuditDirectory != "",
		},
		MQTT: ports.MQTTListenerConfigurationDTO{
			TCPListenAddress: config.MQTT.TCPListenAddress, TLSListenAddress: config.MQTT.TLSListenAddress,
			SecurityConfigConfigured: securityConfigured,
			TCPMaxConnections:        config.MQTT.TCPMaxConnections, TCPConnectionRate: config.MQTT.TCPConnectionRate,
			TLSMaxConnections: config.MQTT.TLSMaxConnections, TLSConnectionRate: config.MQTT.TLSConnectionRate,
			TLS: nodeConfigurationMQTTTLS(config.MQTT.TLS),
		},
		Replication: ports.ReplicationConfigurationDTO{Factor: replication.Factor, WriteQuorum: replication.WriteQuorum},
		PublishBackpressure: ports.PublishBackpressureConfigurationDTO{
			MinFreeBytes: minFreeBytes, MaxCommitLagMillis: uint64(maxCommitLag.Milliseconds()),
		},
		RuntimeMetrics: nodeConfigurationRuntimeMetrics(config.RuntimeMetrics),
		Security: ports.SecurityConfigurationDTO{
			ConfigFileConfigured: securityConfigured, JWT: securityJWT,
			IdentityCounts: identityCounts, ACL: aclRules, TenantLimits: tenantLimits,
		},
		EffectivePolicy: ports.EffectivePolicyConfigurationDTO{
			ClusterPolicy: ports.ClusterPolicyConfigurationStatusDTO{
				FeatureActive: clusterPolicyFeatureActive, TenantQuotaFeatureActive: tenantQuotaFeatureActive,
				ConfiguredVersion: configuredPolicyVersion, CommittedVersion: committedPolicyVersion, Mismatch: policyMismatch,
			},
			MQTTLimits: ports.EffectiveMQTTLimitsDTO{
				Source: mqttLimitsSource, Version: mqttLimitsVersion,
				Value: ports.MQTTLimitsConfigurationDTO{
					MaxPacketSize: mqttLimits.MaxPacketSize, ReceiveMaximum: mqttLimits.ReceiveMaximum,
					ConnectTimeoutMillis: mqttLimits.ConnectTimeoutMillis, PacketReadTimeoutMillis: mqttLimits.PacketReadTimeoutMillis,
					WriteTimeoutMillis: mqttLimits.WriteTimeoutMillis,
				},
			},
			TenantQuotas: effectiveTenantQuotas,
		},
	}
	return result, nil
}

func configuredManagementRate(value *int, defaultValue int) int {
	if value == nil {
		return defaultValue
	}
	return *value
}

func nodeConfigurationBootstrapMembers(members []clusterBootstrapMember) []ports.PeerBootstrapMemberConfigurationDTO {
	result := make([]ports.PeerBootstrapMemberConfigurationDTO, len(members))
	for index, member := range members {
		result[index] = ports.PeerBootstrapMemberConfigurationDTO{MemberID: member.MemberID, Address: member.Address, DNSSAN: member.DNSSAN}
	}
	return result
}

func nodeConfigurationMQTTTLS(config *clusterMQTTTLS) *ports.MQTTTLSConfigurationDTO {
	if config == nil {
		return nil
	}
	return &ports.MQTTTLSConfigurationDTO{
		ClientAuth: config.ClientAuth, MinVersion: config.MinVersion,
		CertificateConfigured: config.CertFile != "", PrivateKeyConfigured: config.KeyFile != "", CAConfigured: config.CAFile != "",
	}
}

func nodeConfigurationRuntimeMetrics(config *clusterRuntimeMetricsConfig) ports.RuntimeMetricsConfigurationDTO {
	if config == nil {
		return ports.RuntimeMetricsConfigurationDTO{}
	}
	return ports.RuntimeMetricsConfigurationDTO{Enabled: true, IntervalMillis: config.IntervalMillis, OutputFileConfigured: config.OutputFile != ""}
}

func tenantQuotaProfileConfiguration(profile membership.TenantQuotaProfileDTO) *ports.TenantQuotaProfileConfigurationDTO {
	tenants := make([]ports.TenantQuotaEntryConfigurationDTO, len(profile.Tenants))
	for index, tenant := range profile.Tenants {
		tenants[index] = ports.TenantQuotaEntryConfigurationDTO{
			TenantID: tenant.TenantID, RetainedBytes: tenant.RetainedBytes,
			OfflineQueueMessages: tenant.OfflineQueueMessages, OfflineQueueBytes: tenant.OfflineQueueBytes,
			StorageBytes: tenant.StorageBytes,
		}
	}
	return &ports.TenantQuotaProfileConfigurationDTO{
		Version: profile.Version,
		Default: ports.TenantQuotaLimitsConfigurationDTO{
			RetainedBytes: profile.Default.RetainedBytes, OfflineQueueMessages: profile.Default.OfflineQueueMessages,
			OfflineQueueBytes: profile.Default.OfflineQueueBytes, StorageBytes: profile.Default.StorageBytes,
		},
		Tenants: tenants,
	}
}

func managementJWTConfiguration(config *clusterManagementOIDC) (*ports.JWTVerifierConfigurationDTO, error) {
	if config == nil {
		return nil, nil
	}
	jwksURL, err := withoutJWKSURLUserInfo(config.JWKSURL)
	if err != nil {
		return nil, err
	}
	return &ports.JWTVerifierConfigurationDTO{
		Issuer: config.Issuer, Audience: config.Audience, Algorithms: slices.Clone(config.Algorithms), JWKSURL: jwksURL,
		CacheTTLMillis: config.CacheTTLMillis, RefreshCooldownMillis: config.RefreshCooldownMillis,
		ClockSkewMillis: config.ClockSkewMillis, HTTPTimeoutMillis: config.HTTPTimeoutMillis,
		IdentityCount: len(config.Identities),
	}, nil
}

func securityJWTConfiguration(config *authenticationadapter.Config) (*ports.JWTConfigurationDTO, error) {
	if config == nil {
		return nil, nil
	}
	jwksURL, err := withoutJWKSURLUserInfo(config.JWKSURL)
	if err != nil {
		return nil, err
	}
	return &ports.JWTConfigurationDTO{
		Issuer: config.Issuer, Audience: config.Audience, Algorithms: slices.Clone(config.Algorithms), JWKSURL: jwksURL,
		CacheTTLMillis: uint32(config.CacheTTL.Milliseconds()), RefreshCooldownMillis: uint32(config.RefreshCooldown.Milliseconds()),
		ClockSkewMillis: uint32(config.ClockSkew.Milliseconds()), HTTPTimeoutMillis: uint32(config.HTTPTimeout.Milliseconds()),
	}, nil
}

func withoutJWKSURLUserInfo(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("configured JWKS URL is invalid")
	}
	parsed.User = nil
	return parsed.String(), nil
}
