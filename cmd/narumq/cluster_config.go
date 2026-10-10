package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	authenticationadapter "narumq/internal/adapters/authentication"
	adaptershttp "narumq/internal/adapters/http/management"
	"narumq/internal/application/membership"
	"narumq/internal/domain/release"
)

type clusterConfigFile struct {
	Mode              string `json:"mode"`
	ClusterID         string `json:"clusterId"`
	NodeID            uint64 `json:"nodeId"`
	Directory         string `json:"directory"`
	ListenAddress     string `json:"listenAddress"`
	AdvertisedAddress string `json:"advertisedAddress"`
	LocalDNSName      string `json:"localDnsName"`
	// ReleaseVersion and Features are resolved from build identity, never from JSON.
	ReleaseVersion       string                            `json:"-"`
	Features             []string                          `json:"-"`
	ClusterProtocolMajor uint32                            `json:"clusterProtocolMajor"`
	ClusterProtocolMinor uint32                            `json:"clusterProtocolMinor"`
	StateSchemaMajor     uint32                            `json:"stateSchemaMajor"`
	StateSchemaMinor     uint32                            `json:"stateSchemaMinor"`
	Role                 string                            `json:"role"`
	BootstrapMembers     []clusterBootstrapMember          `json:"bootstrapMembers"`
	SeedAddresses        []string                          `json:"seedAddresses"`
	PeerTLS              clusterPeerTLS                    `json:"peerTLS"`
	Management           clusterManagement                 `json:"management"`
	MQTT                 clusterMQTT                       `json:"mqtt"`
	TenantQuotas         *clusterTenantQuotaProfileConfig  `json:"tenantQuotas,omitempty"`
	Replication          *clusterReplicationConfig         `json:"replication,omitempty"`
	PublishBackpressure  *clusterPublishBackpressureConfig `json:"publishBackpressure,omitempty"`
	RuntimeMetrics       *clusterRuntimeMetricsConfig      `json:"runtimeMetrics,omitempty"`
}

const (
	clusterDefaultMinFreeBytes                    = 1 << 30
	clusterDefaultMaxCommitLag                    = time.Second
	clusterMaxRuntimeMetricsIntervalMillis uint64 = 3_600_000
)

type clusterPublishBackpressureConfig struct {
	MinFreeBytes       uint64 `json:"minFreeBytes"`
	MaxCommitLagMillis uint64 `json:"maxCommitLagMillis"`
}

type clusterRuntimeMetricsConfig struct {
	OutputFile     string `json:"outputFile"`
	IntervalMillis uint64 `json:"intervalMillis"`
}

func (config clusterConfigFile) publishBackpressure() (minFreeBytes uint64, maxCommitLag time.Duration) {
	if config.PublishBackpressure == nil {
		return clusterDefaultMinFreeBytes, clusterDefaultMaxCommitLag
	}
	return config.PublishBackpressure.MinFreeBytes, time.Duration(config.PublishBackpressure.MaxCommitLagMillis) * time.Millisecond
}

func (config clusterConfigFile) runtimeMetricsInterval() time.Duration {
	return time.Duration(config.RuntimeMetrics.IntervalMillis) * time.Millisecond
}

type clusterReplicationConfig struct {
	Factor      int `json:"factor"`
	WriteQuorum int `json:"writeQuorum"`
}

func (config clusterConfigFile) replicationProfile() membership.ReplicationProfileDTO {
	if config.Replication == nil {
		return membership.StandardReplicationProfile
	}
	return membership.ReplicationProfileDTO{
		Factor: config.Replication.Factor, WriteQuorum: config.Replication.WriteQuorum,
	}
}

type clusterTenantQuotaProfileConfig struct {
	Version uint64                           `json:"version"`
	Default clusterTenantQuotaLimitsConfig   `json:"default"`
	Tenants []clusterTenantQuotaTenantConfig `json:"tenants"`
}

type clusterTenantQuotaLimitsConfig struct {
	RetainedBytes        uint64 `json:"retainedBytes"`
	OfflineQueueMessages uint64 `json:"offlineQueueMessages"`
	OfflineQueueBytes    uint64 `json:"offlineQueueBytes"`
	StorageBytes         uint64 `json:"storageBytes"`
}

type clusterTenantQuotaTenantConfig struct {
	TenantID             string `json:"tenantId"`
	RetainedBytes        uint64 `json:"retainedBytes"`
	OfflineQueueMessages uint64 `json:"offlineQueueMessages"`
	OfflineQueueBytes    uint64 `json:"offlineQueueBytes"`
	StorageBytes         uint64 `json:"storageBytes"`
}

func (profile clusterTenantQuotaProfileConfig) membershipProfile() membership.TenantQuotaProfileDTO {
	tenants := make([]membership.TenantStorageQuotaDTO, len(profile.Tenants))
	for index, tenant := range profile.Tenants {
		tenants[index] = membership.TenantStorageQuotaDTO{
			TenantID: tenant.TenantID, RetainedBytes: tenant.RetainedBytes,
			OfflineQueueMessages: tenant.OfflineQueueMessages,
			OfflineQueueBytes:    tenant.OfflineQueueBytes, StorageBytes: tenant.StorageBytes,
		}
	}
	return membership.TenantQuotaProfileDTO{
		Version: profile.Version,
		Default: membership.TenantStorageQuotaDTO{
			RetainedBytes:        profile.Default.RetainedBytes,
			OfflineQueueMessages: profile.Default.OfflineQueueMessages,
			OfflineQueueBytes:    profile.Default.OfflineQueueBytes,
			StorageBytes:         profile.Default.StorageBytes,
		},
		Tenants: tenants,
	}
}

type clusterBootstrapMember struct {
	MemberID          uint64 `json:"memberId"`
	Address           string `json:"address"`
	DNSSAN            string `json:"dnsSan"`
	CertificateSHA256 string `json:"certificateSha256"`
}

type clusterPeerTLS struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

type clusterMQTTTLS struct {
	CertFile   string `json:"certFile"`
	KeyFile    string `json:"keyFile"`
	CAFile     string `json:"caFile"`
	ClientAuth string `json:"clientAuth"`
	MinVersion string `json:"minVersion"`
}

type clusterManagement struct {
	ListenAddress         string                    `json:"listenAddress"`
	ClusterAdminTokenFile string                    `json:"clusterAdminTokenFile"`
	APIKeys               []clusterManagementAPIKey `json:"apiKeys,omitempty"`
	OIDC                  *clusterManagementOIDC    `json:"oidc,omitempty"`
	AuditDirectory        string                    `json:"auditDirectory"`
	RequestRate           *int                      `json:"requestRate,omitempty"`
	AuthFailureRate       *int                      `json:"authFailureRate,omitempty"`
}

type clusterManagementAPIKey struct {
	ID      string   `json:"id"`
	KeyFile string   `json:"keyFile"`
	Roles   []string `json:"roles"`
}

type clusterManagementOIDC struct {
	rawJWTConfig
	Identities []clusterManagementOIDCIdentity `json:"identities"`
}

type clusterManagementOIDCIdentity struct {
	Subject string   `json:"subject"`
	Roles   []string `json:"roles"`
}

type clusterMQTT struct {
	TCPListenAddress   string          `json:"tcpListenAddress"`
	TLSListenAddress   string          `json:"tlsListenAddress"`
	SecurityConfigFile string          `json:"securityConfigFile"`
	TCPMaxConnections  int             `json:"tcpMaxConnections"`
	TCPConnectionRate  int             `json:"tcpConnectionRate"`
	TLSMaxConnections  int             `json:"tlsMaxConnections"`
	TLSConnectionRate  int             `json:"tlsConnectionRate"`
	TLS                *clusterMQTTTLS `json:"tls,omitempty"`
}

func loadClusterConfigFile(path string) (clusterConfigFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return clusterConfigFile{}, fmt.Errorf("read cluster config file %q: %w", path, err)
	}
	return parseClusterConfig(bytes.NewReader(data))
}

func parseClusterConfig(reader io.Reader) (clusterConfigFile, error) {
	if reader == nil {
		return clusterConfigFile{}, errors.New("cluster config reader is nil")
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return clusterConfigFile{}, errors.New("read cluster config")
	}
	if !utf8.Valid(data) {
		return clusterConfigFile{}, errors.New("cluster config is not valid UTF-8")
	}
	if err := checkClusterJSON(bytes.NewReader(data)); err != nil {
		return clusterConfigFile{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config clusterConfigFile
	if err := decoder.Decode(&config); err != nil {
		return clusterConfigFile{}, errors.New("cluster config has invalid JSON or field types")
	}
	releaseVersion, features, err := resolveBuildIdentity()
	if err != nil {
		return clusterConfigFile{}, err
	}
	config.ReleaseVersion = releaseVersion
	config.Features = features
	if err := validateClusterConfig(config); err != nil {
		return clusterConfigFile{}, err
	}
	return config, nil
}

// checkClusterJSON detects duplicate keys and missing required object members before struct decoding.
func checkClusterJSON(reader io.Reader) error {
	decoder := json.NewDecoder(reader)
	var visit func(string) error
	visit = func(path string) error {
		token, err := decoder.Token()
		if err != nil {
			return errors.New("cluster config has invalid JSON")
		}
		expected := expectedClusterJSONType(path)
		delim, isDelim := token.(json.Delim)
		if isDelim {
			if (expected == "object" && delim != '{') || (expected == "array" && delim != '[') {
				return fmt.Errorf("cluster config field %q has invalid JSON field type", path)
			}
		} else {
			if token == nil || expected == "object" || expected == "array" {
				return fmt.Errorf("cluster config field %q has invalid JSON field type", path)
			}
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return errors.New("cluster config has invalid JSON")
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("cluster config has invalid JSON object key")
				}
				if _, exists := seen[key]; exists {
					return errors.New("cluster config contains duplicate JSON key")
				}
				seen[key] = struct{}{}
				childPath := key
				if path != "" {
					childPath = path + "." + key
				}
				if err := visit(childPath); err != nil {
					return err
				}
			}
			if err := expectClusterKeys(path, seen); err != nil {
				return err
			}
			if _, err := decoder.Token(); err != nil {
				return errors.New("cluster config has invalid JSON")
			}
		case '[':
			for index := 0; decoder.More(); index++ {
				if err := visit(fmt.Sprintf("%s[%d]", path, index)); err != nil {
					return err
				}
			}
			if _, err := decoder.Token(); err != nil {
				return errors.New("cluster config has invalid JSON")
			}
		default:
			return errors.New("cluster config has invalid JSON")
		}
		return nil
	}
	if err := visit(""); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("cluster config contains trailing JSON value")
	}
	return nil
}

func expectedClusterJSONType(path string) string {
	switch path {
	case "", "peerTLS", "management", "management.oidc", "mqtt", "mqtt.tls", "tenantQuotas", "tenantQuotas.default", "replication", "publishBackpressure", "runtimeMetrics":
		return "object"
	case "bootstrapMembers", "seedAddresses", "tenantQuotas.tenants", "management.apiKeys", "management.oidc.algorithms", "management.oidc.identities":
		return "array"
	default:
		if !strings.Contains(path, "].") && (strings.HasPrefix(path, "bootstrapMembers[") ||
			strings.HasPrefix(path, "tenantQuotas.tenants[") || strings.HasPrefix(path, "management.apiKeys[") ||
			strings.HasPrefix(path, "management.oidc.identities[")) && strings.HasSuffix(path, "]") {
			return "object"
		}
		if (strings.HasPrefix(path, "management.apiKeys[") || strings.HasPrefix(path, "management.oidc.identities[")) &&
			strings.HasSuffix(path, ".roles") {
			return "array"
		}
		return ""
	}
}

func expectClusterKeys(path string, seen map[string]struct{}) error {
	var required []string
	switch path {
	case "":
		required = []string{"mode", "clusterId", "nodeId", "directory", "listenAddress", "advertisedAddress", "localDnsName", "clusterProtocolMajor", "clusterProtocolMinor", "stateSchemaMajor", "stateSchemaMinor", "role", "bootstrapMembers", "seedAddresses", "peerTLS", "management", "mqtt"}
	case "peerTLS":
		required = []string{"certFile", "keyFile", "caFile"}
	case "management":
		required = []string{"listenAddress", "clusterAdminTokenFile", "auditDirectory"}
	case "management.oidc":
		required = []string{"issuer", "audience", "algorithms", "jwksUrl", "cacheTtl", "refreshCooldown", "clockSkew", "httpTimeout", "identities"}
	case "mqtt":
		required = []string{"tcpListenAddress", "tlsListenAddress", "securityConfigFile"}
	case "mqtt.tls":
		required = []string{"certFile", "keyFile", "caFile", "clientAuth", "minVersion"}
	case "replication":
		required = []string{"factor", "writeQuorum"}
	case "publishBackpressure":
		required = []string{"minFreeBytes", "maxCommitLagMillis"}
	case "runtimeMetrics":
		required = []string{"outputFile", "intervalMillis"}
	case "tenantQuotas":
		required = []string{"version", "default", "tenants"}
	case "tenantQuotas.default":
		required = []string{"retainedBytes", "offlineQueueMessages", "offlineQueueBytes", "storageBytes"}
	default:
		if strings.HasPrefix(path, "bootstrapMembers[") && strings.HasSuffix(path, "]") {
			required = []string{"memberId", "address", "dnsSan", "certificateSha256"}
		} else if strings.HasPrefix(path, "tenantQuotas.tenants[") && strings.HasSuffix(path, "]") {
			required = []string{"tenantId", "retainedBytes", "offlineQueueMessages", "offlineQueueBytes", "storageBytes"}
		} else if strings.HasPrefix(path, "management.apiKeys[") && strings.HasSuffix(path, "]") {
			required = []string{"id", "keyFile", "roles"}
		} else if strings.HasPrefix(path, "management.oidc.identities[") && strings.HasSuffix(path, "]") {
			required = []string{"subject", "roles"}
		}
	}
	for _, key := range required {
		if _, exists := seen[key]; !exists {
			return fmt.Errorf("cluster config is missing required field %q", key)
		}
	}
	return nil
}

func validateClusterConfig(config clusterConfigFile) error {
	if config.Mode != "cluster" {
		return errors.New("mode must be cluster")
	}
	if !validClusterString(config.ClusterID, 1, 200) {
		return errors.New("clusterId must contain 1 to 200 UTF-8 bytes")
	}
	if config.NodeID == 0 {
		return errors.New("nodeId must be positive")
	}
	if _, err := release.Parse(config.ReleaseVersion); err != nil {
		return errors.New("build release version must be MAJOR.MINOR.PATCH")
	}
	if !release.ValidFeatureSet(config.Features) {
		return errors.New("build features are invalid")
	}
	if config.ClusterProtocolMajor != 2 || config.ClusterProtocolMinor != 0 || config.StateSchemaMajor != 1 || config.StateSchemaMinor != 0 {
		return errors.New("unsupported cluster protocol or state schema version")
	}
	if config.Role != "bootstrap" && config.Role != "joiner" {
		return errors.New("role must be bootstrap or joiner")
	}
	if !validRequiredPath(config.Directory) {
		return errors.New("directory is required")
	}
	if !validHostPort(config.ListenAddress, true) {
		return errors.New("listenAddress must be a valid host:port")
	}
	if !validHostPort(config.AdvertisedAddress, false) {
		return errors.New("advertisedAddress must be a valid non-wildcard host:port")
	}
	if !validDNSName(config.LocalDNSName) {
		return errors.New("localDnsName must be a valid DNS name")
	}
	if !validRequiredPath(config.PeerTLS.CertFile) || !validRequiredPath(config.PeerTLS.KeyFile) || !validRequiredPath(config.PeerTLS.CAFile) {
		return errors.New("peerTLS certFile, keyFile, and caFile are required")
	}
	if !validHostPort(config.Management.ListenAddress, true) || !validRequiredPath(config.Management.ClusterAdminTokenFile) {
		return errors.New("management listenAddress and clusterAdminTokenFile are required")
	}
	if _, _, err := validateClusterManagementAccess(config.Management); err != nil {
		return err
	}
	if !validRequiredPath(config.Management.AuditDirectory) {
		return errors.New("management auditDirectory is required")
	}
	overlaps, err := clusterPathsOverlap(config.Directory, config.Management.AuditDirectory)
	if err != nil || overlaps {
		return errors.New("management auditDirectory must not overlap directory")
	}
	if (config.Management.RequestRate != nil &&
		(*config.Management.RequestRate < 1 || *config.Management.RequestRate > adaptershttp.MaxRequestRate)) ||
		(config.Management.AuthFailureRate != nil &&
			(*config.Management.AuthFailureRate < 1 || *config.Management.AuthFailureRate > adaptershttp.MaxRequestRate)) {
		return errors.New("management requestRate and authFailureRate must be between 1 and 1000000")
	}
	if (config.MQTT.TCPListenAddress != "" && !validHostPort(config.MQTT.TCPListenAddress, true)) || (config.MQTT.TLSListenAddress != "" && !validHostPort(config.MQTT.TLSListenAddress, true)) {
		return errors.New("mqtt listener addresses must be empty or valid host:port values")
	}
	if !validRequiredPath(config.MQTT.SecurityConfigFile) {
		return errors.New("mqtt securityConfigFile is required")
	}
	if config.MQTT.TCPMaxConnections < 0 || config.MQTT.TCPMaxConnections > 1_000_000 ||
		config.MQTT.TCPConnectionRate < 0 || config.MQTT.TCPConnectionRate > 1_000_000 ||
		config.MQTT.TLSMaxConnections < 0 || config.MQTT.TLSMaxConnections > 1_000_000 ||
		config.MQTT.TLSConnectionRate < 0 || config.MQTT.TLSConnectionRate > 1_000_000 {
		return errors.New("mqtt connection limits must be between 0 and 1000000")
	}
	if config.MQTT.TCPListenAddress == "" && (config.MQTT.TCPMaxConnections != 0 || config.MQTT.TCPConnectionRate != 0) {
		return errors.New("mqtt tcp connection limits require tcpListenAddress")
	}
	if config.MQTT.TLSListenAddress == "" && (config.MQTT.TLSMaxConnections != 0 || config.MQTT.TLSConnectionRate != 0) {
		return errors.New("mqtt tls connection limits require tlsListenAddress")
	}
	if err := validateClusterMQTTTLS(config.MQTT); err != nil {
		return err
	}
	if config.TenantQuotas != nil {
		if config.TenantQuotas.Version == 0 {
			return errors.New("tenantQuotas version must be at least 1")
		}
		if err := membership.ValidateTenantQuotaProfile(config.TenantQuotas.membershipProfile()); err != nil {
			return fmt.Errorf("tenantQuotas is invalid: %w", err)
		}
	}
	if config.Replication != nil {
		if err := membership.ValidateReplicationProfile(config.replicationProfile()); err != nil {
			return fmt.Errorf("replication is invalid: %w", err)
		}
	}
	if pressure := config.PublishBackpressure; pressure != nil {
		if pressure.MinFreeBytes > 1<<50 {
			return errors.New("publishBackpressure minFreeBytes must be at most 1125899906842624")
		}
		if pressure.MaxCommitLagMillis < 100 || pressure.MaxCommitLagMillis > 10000 {
			return errors.New("publishBackpressure maxCommitLagMillis must be between 100 and 10000")
		}
	}
	if metrics := config.RuntimeMetrics; metrics != nil {
		if !validRequiredPath(metrics.OutputFile) {
			return errors.New("runtimeMetrics outputFile is required")
		}
		if metrics.IntervalMillis < 1 || metrics.IntervalMillis > clusterMaxRuntimeMetricsIntervalMillis {
			return errors.New("runtimeMetrics intervalMillis must be between 1 and 3600000")
		}
		overlapsDirectory, err := clusterPathsOverlap(metrics.OutputFile, config.Directory)
		if err != nil || overlapsDirectory {
			return errors.New("runtimeMetrics outputFile must not overlap directory or management auditDirectory")
		}
		overlapsAuditDirectory, err := clusterPathsOverlap(metrics.OutputFile, config.Management.AuditDirectory)
		if err != nil || overlapsAuditDirectory {
			return errors.New("runtimeMetrics outputFile must not overlap directory or management auditDirectory")
		}
	}
	if config.Role == "bootstrap" {
		if len(config.BootstrapMembers) != 3 {
			return errors.New("bootstrapMembers must contain exactly three members")
		}
		if len(config.SeedAddresses) != 0 {
			return errors.New("seedAddresses must be empty for bootstrap")
		}
		previousID := uint64(0)
		localFound := false
		seenIDs := make(map[uint64]struct{}, len(config.BootstrapMembers))
		for _, member := range config.BootstrapMembers {
			if member.MemberID == 0 || member.MemberID <= previousID {
				return errors.New("bootstrapMembers must be uniquely sorted by positive memberId")
			}
			previousID = member.MemberID
			if _, exists := seenIDs[member.MemberID]; exists {
				return errors.New("bootstrapMembers contains duplicate memberId")
			}
			seenIDs[member.MemberID] = struct{}{}
			if !validHostPort(member.Address, false) || !validDNSName(member.DNSSAN) || !validFingerprint(member.CertificateSHA256) {
				return errors.New("bootstrap member identity is invalid")
			}
			if member.MemberID == config.NodeID {
				localFound = member.Address == config.AdvertisedAddress && member.DNSSAN == config.LocalDNSName
			}
		}
		if !localFound {
			return errors.New("bootstrapMembers must include the matching local node identity")
		}
	} else {
		if len(config.BootstrapMembers) != 0 {
			return errors.New("bootstrapMembers must be empty for joiner")
		}
		if len(config.SeedAddresses) < 1 || len(config.SeedAddresses) > 16 {
			return errors.New("seedAddresses must contain 1 to 16 addresses for joiner")
		}
		seen := make(map[string]struct{}, len(config.SeedAddresses))
		for _, address := range config.SeedAddresses {
			host, _, splitErr := net.SplitHostPort(address)
			if !validHostPort(address, false) || splitErr != nil || !validDNSName(host) {
				return errors.New("seedAddresses requires DNS host:port addresses")
			}
			if _, exists := seen[address]; exists {
				return errors.New("seedAddresses contains duplicate address")
			}
			seen[address] = struct{}{}
		}
	}
	return nil
}

func validateClusterMQTTTLS(mqtt clusterMQTT) error {
	if mqtt.TLSListenAddress == "" {
		if mqtt.TLS != nil {
			return errors.New("mqtt tls requires tlsListenAddress")
		}
		return nil
	}
	if mqtt.TLS == nil {
		return errors.New("mqtt tls is required when tlsListenAddress is set")
	}
	if !validRequiredPath(mqtt.TLS.CertFile) || !validRequiredPath(mqtt.TLS.KeyFile) {
		return errors.New("mqtt tls certFile and keyFile are required")
	}
	switch mqtt.TLS.ClientAuth {
	case "none":
		if mqtt.TLS.CAFile != "" {
			return errors.New("mqtt tls caFile must be empty when clientAuth is none")
		}
	case "request", "require":
		if !validRequiredPath(mqtt.TLS.CAFile) {
			return errors.New("mqtt tls caFile is required when clientAuth is request or require")
		}
	default:
		return errors.New("mqtt tls clientAuth must be none, request, or require")
	}
	if mqtt.TLS.MinVersion != "1.2" && mqtt.TLS.MinVersion != "1.3" {
		return errors.New("mqtt tls minVersion must be 1.2 or 1.3")
	}
	return nil
}

func validateClusterManagementAccess(config clusterManagement) (*authenticationadapter.VerifierConfig, []adaptershttp.OIDCIdentity, error) {
	seenIDs := make(map[string]struct{}, len(config.APIKeys))
	for i, key := range config.APIKeys {
		if !validManagementAPIKeyID(key.ID) {
			return nil, nil, fmt.Errorf("management apiKeys[%d].id must match ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$", i)
		}
		if _, exists := seenIDs[key.ID]; exists {
			return nil, nil, fmt.Errorf("duplicate management apiKeys id %q", key.ID)
		}
		seenIDs[key.ID] = struct{}{}
		if !validRequiredPath(key.KeyFile) {
			return nil, nil, fmt.Errorf("management apiKeys[%d].keyFile must be a valid path", i)
		}
		if len(key.Roles) == 0 {
			return nil, nil, fmt.Errorf("management apiKeys[%d].roles must contain at least one role", i)
		}
		seenRoles := make(map[adaptershttp.Role]struct{}, len(key.Roles))
		for _, name := range key.Roles {
			role, ok := adaptershttp.ParseRole(name)
			if !ok {
				return nil, nil, fmt.Errorf("management apiKeys[%d] has unknown role %q", i, name)
			}
			if _, exists := seenRoles[role]; exists {
				return nil, nil, fmt.Errorf("management apiKeys[%d] has duplicate role %q", i, name)
			}
			seenRoles[role] = struct{}{}
		}
	}
	if config.OIDC == nil {
		return nil, nil, nil
	}
	verifier, err := validateJWTVerifierConfig("management.oidc", &config.OIDC.rawJWTConfig)
	if err != nil {
		return nil, nil, err
	}
	if len(config.OIDC.Identities) == 0 {
		return nil, nil, errors.New("management.oidc.identities must contain at least one identity")
	}
	identities := make([]adaptershttp.OIDCIdentity, len(config.OIDC.Identities))
	seenSubjects := make(map[string]struct{}, len(config.OIDC.Identities))
	for i, identity := range config.OIDC.Identities {
		if identity.Subject == "" {
			return nil, nil, fmt.Errorf("management.oidc.identities[%d].subject is required", i)
		}
		if utf8.RuneCountInString(identity.Subject) > 251 {
			return nil, nil, fmt.Errorf("management.oidc.identities[%d].subject must not exceed 251 characters", i)
		}
		if _, exists := seenSubjects[identity.Subject]; exists {
			return nil, nil, fmt.Errorf("duplicate management.oidc subject %q", identity.Subject)
		}
		seenSubjects[identity.Subject] = struct{}{}
		if len(identity.Roles) == 0 {
			return nil, nil, fmt.Errorf("management.oidc.identities[%d].roles must contain at least one role", i)
		}
		roles := make([]adaptershttp.Role, len(identity.Roles))
		seenRoles := make(map[adaptershttp.Role]struct{}, len(identity.Roles))
		for j, name := range identity.Roles {
			role, ok := adaptershttp.ParseRole(name)
			if !ok {
				return nil, nil, fmt.Errorf("management.oidc.identities[%d] has unknown role %q", i, name)
			}
			if _, exists := seenRoles[role]; exists {
				return nil, nil, fmt.Errorf("management.oidc.identities[%d] has duplicate role %q", i, name)
			}
			seenRoles[role] = struct{}{}
			roles[j] = role
		}
		identities[i] = adaptershttp.OIDCIdentity{Subject: identity.Subject, Roles: roles}
	}
	return &verifier, identities, nil
}

func validManagementAPIKeyID(value string) bool {
	if len(value) == 0 || len(value) > 64 || !isASCIIAlphaNumeric(value[0]) {
		return false
	}
	for i := 1; i < len(value); i++ {
		b := value[i]
		if !isASCIIAlphaNumeric(b) && b != '.' && b != '_' && b != '-' {
			return false
		}
	}
	return true
}

func isASCIIAlphaNumeric(value byte) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}

type clusterManagementCredentials struct {
	AdminToken     string
	APIKeys        []adaptershttp.APIKeyCredential
	OIDCVerifier   *authenticationadapter.VerifierConfig
	OIDCIdentities []adaptershttp.OIDCIdentity
}

func loadClusterManagementCredentials(config clusterManagement) (clusterManagementCredentials, error) {
	adminBytes, err := os.ReadFile(config.ClusterAdminTokenFile)
	if err != nil {
		return clusterManagementCredentials{}, fmt.Errorf("read cluster admin token file: %w", err)
	}
	adminToken := strings.TrimSpace(string(adminBytes))
	if adminToken == "" {
		return clusterManagementCredentials{}, errors.New("cluster admin token file is empty")
	}
	verifier, identities, err := validateClusterManagementAccess(config)
	if err != nil {
		return clusterManagementCredentials{}, err
	}
	credentials := clusterManagementCredentials{
		AdminToken: adminToken, OIDCVerifier: verifier, OIDCIdentities: identities,
		APIKeys: make([]adaptershttp.APIKeyCredential, len(config.APIKeys)),
	}
	seenKeys := make(map[string]string, len(config.APIKeys))
	for i, configured := range config.APIKeys {
		keyBytes, readErr := os.ReadFile(configured.KeyFile)
		if readErr != nil {
			return clusterManagementCredentials{}, fmt.Errorf("read management API key file for %q: %w", configured.ID, readErr)
		}
		key := strings.TrimSpace(string(keyBytes))
		if key == "" {
			return clusterManagementCredentials{}, fmt.Errorf("management API key file for %q is empty", configured.ID)
		}
		if key == adminToken {
			return clusterManagementCredentials{}, fmt.Errorf("management API key %q must differ from the cluster admin token", configured.ID)
		}
		if otherID, exists := seenKeys[key]; exists {
			return clusterManagementCredentials{}, fmt.Errorf("management API keys %q and %q must differ", otherID, configured.ID)
		}
		seenKeys[key] = configured.ID
		roles := make([]adaptershttp.Role, len(configured.Roles))
		for j, name := range configured.Roles {
			roles[j], _ = adaptershttp.ParseRole(name)
		}
		credentials.APIKeys[i] = adaptershttp.APIKeyCredential{ID: configured.ID, Key: key, Roles: roles}
	}
	return credentials, nil
}

func validClusterString(value string, min, max int) bool {
	return utf8.ValidString(value) && len(value) >= min && len(value) <= max && strings.TrimSpace(value) == value && strings.TrimSpace(value) != ""
}

func validRequiredPath(value string) bool {
	return strings.TrimSpace(value) != "" && strings.TrimSpace(value) == value
}

func clusterPathsOverlap(a, b string) (bool, error) {
	absA, err := filepath.Abs(a)
	if err != nil {
		return false, err
	}
	absB, err := filepath.Abs(b)
	if err != nil {
		return false, err
	}
	if !strings.EqualFold(filepath.VolumeName(absA), filepath.VolumeName(absB)) {
		return false, nil
	}
	relative, err := filepath.Rel(absA, absB)
	if err != nil {
		return false, err
	}
	if relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return true, nil
	}
	relative, err = filepath.Rel(absB, absA)
	if err != nil {
		return false, err
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))), nil
}

func validFingerprint(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func validDNSName(value string) bool {
	if len(value) == 0 || len(value) > 253 || net.ParseIP(value) != nil || strings.ToLower(value) != value {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if !(char >= 'a' && char <= 'z') && !(char >= '0' && char <= '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func validHostPort(address string, allowWildcard bool) bool {
	if address == "" || len(address) > 255 || strings.TrimSpace(address) != address {
		return false
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" || portText == "" {
		return false
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 || strconv.Itoa(port) != portText {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		if net.JoinHostPort(ip.String(), portText) != address {
			return false
		}
		if !allowWildcard && ip.IsUnspecified() {
			return false
		}
		return true
	}
	if !validDNSName(host) || net.JoinHostPort(host, portText) != address {
		return false
	}
	return true
}
