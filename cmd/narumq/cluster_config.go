package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
}

const (
	clusterDefaultMinFreeBytes = 1 << 30
	clusterDefaultMaxCommitLag = time.Second
)

type clusterPublishBackpressureConfig struct {
	MinFreeBytes       uint64 `json:"minFreeBytes"`
	MaxCommitLagMillis uint64 `json:"maxCommitLagMillis"`
}

func (config clusterConfigFile) publishBackpressure() (minFreeBytes uint64, maxCommitLag time.Duration) {
	if config.PublishBackpressure == nil {
		return clusterDefaultMinFreeBytes, clusterDefaultMaxCommitLag
	}
	return config.PublishBackpressure.MinFreeBytes, time.Duration(config.PublishBackpressure.MaxCommitLagMillis) * time.Millisecond
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

type clusterManagement struct {
	ListenAddress         string `json:"listenAddress"`
	ClusterAdminTokenFile string `json:"clusterAdminTokenFile"`
}

type clusterMQTT struct {
	TCPListenAddress   string `json:"tcpListenAddress"`
	TLSListenAddress   string `json:"tlsListenAddress"`
	SecurityConfigFile string `json:"securityConfigFile"`
	TCPMaxConnections  int    `json:"tcpMaxConnections"`
	TCPConnectionRate  int    `json:"tcpConnectionRate"`
	TLSMaxConnections  int    `json:"tlsMaxConnections"`
	TLSConnectionRate  int    `json:"tlsConnectionRate"`
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
				return errors.New("cluster config has invalid JSON field type")
			}
		} else {
			if token == nil || expected == "object" || expected == "array" {
				return errors.New("cluster config has invalid JSON field type")
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
	case "", "peerTLS", "management", "mqtt", "tenantQuotas", "tenantQuotas.default", "replication", "publishBackpressure":
		return "object"
	case "bootstrapMembers", "seedAddresses", "tenantQuotas.tenants":
		return "array"
	default:
		if (strings.HasPrefix(path, "bootstrapMembers[") || strings.HasPrefix(path, "tenantQuotas.tenants[")) &&
			strings.HasSuffix(path, "]") {
			return "object"
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
		required = []string{"listenAddress", "clusterAdminTokenFile"}
	case "mqtt":
		required = []string{"tcpListenAddress", "tlsListenAddress", "securityConfigFile"}
	case "replication":
		required = []string{"factor", "writeQuorum"}
	case "publishBackpressure":
		required = []string{"minFreeBytes", "maxCommitLagMillis"}
	case "tenantQuotas":
		required = []string{"version", "default", "tenants"}
	case "tenantQuotas.default":
		required = []string{"retainedBytes", "offlineQueueMessages", "offlineQueueBytes", "storageBytes"}
	default:
		if strings.HasPrefix(path, "bootstrapMembers[") && strings.HasSuffix(path, "]") {
			required = []string{"memberId", "address", "dnsSan", "certificateSha256"}
		} else if strings.HasPrefix(path, "tenantQuotas.tenants[") && strings.HasSuffix(path, "]") {
			required = []string{"tenantId", "retainedBytes", "offlineQueueMessages", "offlineQueueBytes", "storageBytes"}
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

func validClusterString(value string, min, max int) bool {
	return utf8.ValidString(value) && len(value) >= min && len(value) <= max && strings.TrimSpace(value) == value && strings.TrimSpace(value) != ""
}

func validRequiredPath(value string) bool {
	return strings.TrimSpace(value) != "" && strings.TrimSpace(value) == value
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
