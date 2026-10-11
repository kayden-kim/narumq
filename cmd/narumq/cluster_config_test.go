package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	adaptershttp "narumq/internal/adapters/http/management"
	"narumq/internal/application/membership"
)

func validBootstrapClusterConfig() clusterConfigFile {
	return clusterConfigFile{
		Mode: "cluster", ClusterID: "sample-cluster", NodeID: 1, Directory: "data/node1",
		ListenAddress: "127.0.0.1:19001", AdvertisedAddress: "127.0.0.1:19001", LocalDNSName: "node1.example",
		ReleaseVersion: buildReleaseVersion, ClusterProtocolMajor: 2, StateSchemaMajor: 1,
		Role: "bootstrap",
		BootstrapMembers: []clusterBootstrapMember{
			{MemberID: 1, Address: "127.0.0.1:19001", DNSSAN: "node1.example", CertificateSHA256: strings.Repeat("a", 64)},
			{MemberID: 2, Address: "127.0.0.1:19002", DNSSAN: "node2.example", CertificateSHA256: strings.Repeat("b", 64)},
			{MemberID: 3, Address: "127.0.0.1:19003", DNSSAN: "node3.example", CertificateSHA256: strings.Repeat("c", 64)},
		},
		SeedAddresses: []string{},
		PeerTLS:       clusterPeerTLS{CertFile: "certs/node1.crt", KeyFile: "certs/node1.key", CAFile: "certs/ca.crt"},
		Management:    clusterManagement{ListenAddress: "127.0.0.1:18080", ClusterAdminTokenFile: "secrets/admin.token", AuditDirectory: "data/node1-audit"},
		MQTT: clusterMQTT{
			TCPListenAddress: "", TLSListenAddress: "127.0.0.1:1883", SecurityConfigFile: "security.json",
			TLS: &clusterMQTTTLS{CertFile: "certs/mqtt.crt", KeyFile: "certs/mqtt.key", ClientAuth: "none", MinVersion: "1.2"},
		},
	}
}

func validJoinerClusterConfig() clusterConfigFile {
	config := validBootstrapClusterConfig()
	config.NodeID = 4
	config.Directory = "data/node4"
	config.ListenAddress = "[::]:19004"
	config.AdvertisedAddress = "node4.example:19004"
	config.LocalDNSName = "node4.example"
	config.Role = "joiner"
	config.BootstrapMembers = []clusterBootstrapMember{}
	config.SeedAddresses = []string{"node1.example:19001", "node2.example:19002"}
	return config
}
func validTenantQuotaProfileConfig() *clusterTenantQuotaProfileConfig {
	return &clusterTenantQuotaProfileConfig{
		Version: 1,
		Default: clusterTenantQuotaLimitsConfig{
			RetainedBytes: 1024, OfflineQueueMessages: 64,
			OfflineQueueBytes: 2048, StorageBytes: 4096,
		},
		Tenants: []clusterTenantQuotaTenantConfig{{
			TenantID: "acme", RetainedBytes: 512, OfflineQueueMessages: 32,
			OfflineQueueBytes: 1024, StorageBytes: 2048,
		}},
	}
}

func validClusterPolicyConfig() *clusterPolicyConfig {
	return &clusterPolicyConfig{
		Version: 1,
		MQTTLimits: &clusterMQTTLimitsConfig{
			MaxPacketSize: 1_048_576, ReceiveMaximum: 64,
			ConnectTimeoutMillis: 10_000, PacketReadTimeoutMillis: 30_000, WriteTimeoutMillis: 5_000,
		},
	}
}

func validClusterPolicyTenantQuotasConfig() *clusterPolicyTenantQuotasConfig {
	return &clusterPolicyTenantQuotasConfig{
		Default: clusterTenantQuotaLimitsConfig{
			RetainedBytes: 1024, OfflineQueueMessages: 64,
			OfflineQueueBytes: 2048, StorageBytes: 4096,
		},
		Tenants: []clusterTenantQuotaTenantConfig{{
			TenantID: "acme", RetainedBytes: 512, OfflineQueueMessages: 32,
			OfflineQueueBytes: 1024, StorageBytes: 2048,
		}},
	}
}

func mutateClusterPolicyTenantQuotasJSON(
	t *testing.T,
	data []byte,
	mutate func(map[string]json.RawMessage),
) []byte {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode cluster config: %v", err)
	}
	var policy map[string]json.RawMessage
	if err := json.Unmarshal(root["clusterPolicy"], &policy); err != nil {
		t.Fatalf("decode cluster policy: %v", err)
	}
	var quotas map[string]json.RawMessage
	if err := json.Unmarshal(policy["tenantQuotas"], &quotas); err != nil {
		t.Fatalf("decode policy tenant quotas: %v", err)
	}
	mutate(quotas)
	encodedQuotas, err := json.Marshal(quotas)
	if err != nil {
		t.Fatalf("encode policy tenant quotas: %v", err)
	}
	policy["tenantQuotas"] = encodedQuotas
	encodedPolicy, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("encode cluster policy: %v", err)
	}
	root["clusterPolicy"] = encodedPolicy
	encoded, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode cluster config: %v", err)
	}
	return encoded
}

func TestParseClusterConfigValidRoles(t *testing.T) {
	wantRelease, wantFeatures, err := resolveBuildIdentity()
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range []clusterConfigFile{validBootstrapClusterConfig(), validJoinerClusterConfig()} {
		data, err := json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := parseClusterConfig(strings.NewReader(string(data)))
		if err != nil {
			t.Fatalf("parse %s config: %v", config.Role, err)
		}
		if parsed.Role != config.Role || parsed.NodeID != config.NodeID {
			t.Fatalf("parsed identity = (%s, %d), want (%s, %d)", parsed.Role, parsed.NodeID, config.Role, config.NodeID)
		}
		if parsed.ReleaseVersion != wantRelease || !slices.Equal(parsed.Features, wantFeatures) {
			t.Fatalf("parsed build identity = (%q, %q), want (%q, %q)", parsed.ReleaseVersion, parsed.Features, wantRelease, wantFeatures)
		}
	}
}

// Traceability: API-09; CFG-SCN-25; docs/contracts/process-configuration-v1.md §1.3; ADR-0052 §1.1.1.
func TestParseClusterConfigClusterPolicy(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.ClusterPolicy = validClusterPolicyConfig()
	data := mustMarshalClusterConfig(t, config)
	parsed, err := parseClusterConfig(strings.NewReader(string(data)))
	if err != nil {
		t.Fatalf("parse valid clusterPolicy: %v", err)
	}
	if parsed.ClusterPolicy == nil || parsed.ClusterPolicy.Version != 1 ||
		!membership.EqualClusterPolicy(parsed.ClusterPolicy.membershipPolicy(), config.ClusterPolicy.membershipPolicy()) {
		t.Fatalf("parsed clusterPolicy = %+v, want %+v", parsed.ClusterPolicy, config.ClusterPolicy)
	}

	maximum := validBootstrapClusterConfig()
	maximum.ClusterPolicy = validClusterPolicyConfig()
	maximum.ClusterPolicy.MQTTLimits.MaxPacketSize = 268_435_455
	maximum.ClusterPolicy.MQTTLimits.ReceiveMaximum = 65_535
	maximum.ClusterPolicy.MQTTLimits.ConnectTimeoutMillis = ^uint32(0)
	maximum.ClusterPolicy.MQTTLimits.PacketReadTimeoutMillis = ^uint32(0)
	maximum.ClusterPolicy.MQTTLimits.WriteTimeoutMillis = ^uint32(0)
	if err := validateClusterConfig(maximum); err != nil {
		t.Fatalf("validate maximum clusterPolicy limits: %v", err)
	}

	jsonCases := []struct {
		name string
		edit func(string) string
	}{
		{name: "missing mqttLimits", edit: func(value string) string { return strings.Replace(value, `"mqttLimits":`, `"unknownLimits":`, 1) }},
		{name: "missing required limit", edit: func(value string) string {
			return strings.Replace(value, `,"writeTimeoutMillis":5000`, "", 1)
		}},
		{name: "unknown limit key", edit: func(value string) string {
			return strings.Replace(value, `"writeTimeoutMillis":5000`, `"writeTimeoutMillis":5000,"unexpected":1`, 1)
		}},
		{name: "duplicate limit key", edit: func(value string) string {
			return strings.Replace(value, `"writeTimeoutMillis":5000`, `"writeTimeoutMillis":5000,"writeTimeoutMillis":5000`, 1)
		}},
	}
	for _, testCase := range jsonCases {
		t.Run(testCase.name, func(t *testing.T) {
			invalidJSON := testCase.edit(string(data))
			if _, err := parseClusterConfig(strings.NewReader(invalidJSON)); err == nil {
				t.Fatal("parseClusterConfig() accepted invalid clusterPolicy JSON")
			}
		})
	}

	invalidConfigs := []struct {
		name string
		edit func(*clusterConfigFile)
	}{
		{name: "zero version", edit: func(config *clusterConfigFile) { config.ClusterPolicy.Version = 0 }},
		{name: "zero packet size", edit: func(config *clusterConfigFile) { config.ClusterPolicy.MQTTLimits.MaxPacketSize = 0 }},
		{name: "packet size over MQTT maximum", edit: func(config *clusterConfigFile) { config.ClusterPolicy.MQTTLimits.MaxPacketSize = 268_435_456 }},
		{name: "receive maximum over protocol maximum", edit: func(config *clusterConfigFile) { config.ClusterPolicy.MQTTLimits.ReceiveMaximum = 65_536 }},
		{name: "zero connect timeout", edit: func(config *clusterConfigFile) { config.ClusterPolicy.MQTTLimits.ConnectTimeoutMillis = 0 }},
		{name: "zero packet read timeout", edit: func(config *clusterConfigFile) { config.ClusterPolicy.MQTTLimits.PacketReadTimeoutMillis = 0 }},
		{name: "zero write timeout", edit: func(config *clusterConfigFile) { config.ClusterPolicy.MQTTLimits.WriteTimeoutMillis = 0 }},
		{name: "legacy quota conflict", edit: func(config *clusterConfigFile) { config.TenantQuotas = validTenantQuotaProfileConfig() }},
	}
	for _, testCase := range invalidConfigs {
		t.Run(testCase.name, func(t *testing.T) {
			invalid := validBootstrapClusterConfig()
			invalid.ClusterPolicy = validClusterPolicyConfig()
			testCase.edit(&invalid)
			if err := validateClusterConfig(invalid); err == nil {
				t.Fatal("validateClusterConfig() accepted invalid clusterPolicy configuration")
			}
		})
	}
}

// Traceability: CFG-SCN-33; TEN-08; docs/contracts/process-configuration-v1.md §1.3.
func TestParseClusterConfigClusterPolicyTenantQuotas(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.ClusterPolicy = validClusterPolicyConfig()
	config.ClusterPolicy.TenantQuotas = validClusterPolicyTenantQuotasConfig()
	data := mustMarshalClusterConfig(t, config)
	parsed, err := parseClusterConfig(strings.NewReader(string(data)))
	if err != nil {
		t.Fatalf("parse valid clusterPolicy.tenantQuotas: %v", err)
	}
	if parsed.ClusterPolicy == nil || parsed.ClusterPolicy.TenantQuotas == nil {
		t.Fatalf("parsed clusterPolicy tenant quotas = %+v, want configured quotas", parsed.ClusterPolicy)
	}
	got := parsed.ClusterPolicy.membershipPolicy()
	want := config.ClusterPolicy.membershipPolicy()
	if !membership.EqualClusterPolicy(got, want) || got.TenantQuotas.Default.StorageBytes != 4096 ||
		len(got.TenantQuotas.Tenants) != 1 || got.TenantQuotas.Tenants[0].TenantID != "acme" {
		t.Fatalf("parsed policy quotas = %+v, want %+v", got, want)
	}

	invalidJSON := []struct {
		name   string
		mutate func(map[string]json.RawMessage)
	}{
		{name: "missing default", mutate: func(quotas map[string]json.RawMessage) { delete(quotas, "default") }},
		{name: "missing tenants", mutate: func(quotas map[string]json.RawMessage) { delete(quotas, "tenants") }},
		{name: "unknown key", mutate: func(quotas map[string]json.RawMessage) { quotas["unexpected"] = json.RawMessage(`1`) }},
		{name: "nested profile version", mutate: func(quotas map[string]json.RawMessage) { quotas["version"] = json.RawMessage(`1`) }},
		{name: "default limit over maximum", mutate: func(quotas map[string]json.RawMessage) {
			var limits map[string]json.RawMessage
			if err := json.Unmarshal(quotas["default"], &limits); err != nil {
				t.Fatalf("decode default quota: %v", err)
			}
			limits["retainedBytes"] = json.RawMessage(`16777217`)
			quotas["default"], err = json.Marshal(limits)
			if err != nil {
				t.Fatalf("encode default quota: %v", err)
			}
		}},
		{name: "tenant ID missing", mutate: func(quotas map[string]json.RawMessage) {
			var tenants []map[string]json.RawMessage
			if err := json.Unmarshal(quotas["tenants"], &tenants); err != nil {
				t.Fatalf("decode tenant quotas: %v", err)
			}
			delete(tenants[0], "tenantId")
			quotas["tenants"], err = json.Marshal(tenants)
			if err != nil {
				t.Fatalf("encode tenant quotas: %v", err)
			}
		}},
		{name: "invalid tenant ID", mutate: func(quotas map[string]json.RawMessage) {
			var tenants []map[string]json.RawMessage
			if err := json.Unmarshal(quotas["tenants"], &tenants); err != nil {
				t.Fatalf("decode tenant quotas: %v", err)
			}
			tenants[0]["tenantId"] = json.RawMessage(`""`)
			quotas["tenants"], err = json.Marshal(tenants)
			if err != nil {
				t.Fatalf("encode tenant quotas: %v", err)
			}
		}},
	}
	for _, testCase := range invalidJSON {
		t.Run(testCase.name, func(t *testing.T) {
			invalid := mutateClusterPolicyTenantQuotasJSON(t, data, testCase.mutate)
			if _, err := parseClusterConfig(strings.NewReader(string(invalid))); err == nil {
				t.Fatal("parseClusterConfig() accepted invalid clusterPolicy.tenantQuotas")
			}
		})
	}

	unsorted := validBootstrapClusterConfig()
	unsorted.ClusterPolicy = validClusterPolicyConfig()
	unsorted.ClusterPolicy.TenantQuotas = validClusterPolicyTenantQuotasConfig()
	unsorted.ClusterPolicy.TenantQuotas.Tenants = []clusterTenantQuotaTenantConfig{
		{TenantID: "zeta", RetainedBytes: 512, OfflineQueueMessages: 32, OfflineQueueBytes: 1024, StorageBytes: 2048},
		{TenantID: "acme", RetainedBytes: 512, OfflineQueueMessages: 32, OfflineQueueBytes: 1024, StorageBytes: 2048},
	}
	if _, err := parseClusterConfig(strings.NewReader(string(mustMarshalClusterConfig(t, unsorted)))); err == nil {
		t.Fatal("parseClusterConfig() accepted descending tenant quota IDs")
	}
}

// Traceability: SEC-03; docs/specs/configuration-and-runtime-options.md §6; ADR-0052.
func TestParseClusterConfigMQTTTLS(t *testing.T) {
	validCases := []struct {
		name string
		tls  clusterMQTTTLS
	}{
		{name: "none baseline", tls: clusterMQTTTLS{CertFile: "certs/mqtt.crt", KeyFile: "certs/mqtt.key", ClientAuth: "none", MinVersion: "1.2"}},
		{name: "request", tls: clusterMQTTTLS{CertFile: "certs/mqtt.crt", KeyFile: "certs/mqtt.key", CAFile: "certs/client-ca.crt", ClientAuth: "request", MinVersion: "1.2"}},
		{name: "require TLS 1.3", tls: clusterMQTTTLS{CertFile: "certs/mqtt.crt", KeyFile: "certs/mqtt.key", CAFile: "certs/client-ca.crt", ClientAuth: "require", MinVersion: "1.3"}},
	}
	for _, test := range validCases {
		t.Run(test.name, func(t *testing.T) {
			config := validBootstrapClusterConfig()
			config.MQTT.TLS = &test.tls
			parsed, err := parseClusterConfig(strings.NewReader(string(mustMarshalClusterConfig(t, config))))
			if err != nil {
				t.Fatalf("parse mqtt.tls: %v", err)
			}
			if parsed.MQTT.TLS == nil || *parsed.MQTT.TLS != test.tls {
				t.Fatalf("parsed mqtt.tls = %+v, want %+v", parsed.MQTT.TLS, test.tls)
			}
		})
	}

	invalidCases := []struct {
		name string
		edit func(*clusterMQTT)
		want string
	}{
		{
			name: "TLS object without listener",
			edit: func(mqtt *clusterMQTT) { mqtt.TLSListenAddress = "" },
			want: "mqtt tls requires tlsListenAddress",
		},
		{
			name: "listener without TLS object",
			edit: func(mqtt *clusterMQTT) { mqtt.TLS = nil },
			want: "mqtt tls is required when tlsListenAddress is set",
		},
		{
			name: "missing certificate and key",
			edit: func(mqtt *clusterMQTT) {
				mqtt.TLS.CertFile = ""
				mqtt.TLS.KeyFile = ""
			},
			want: "mqtt tls certFile and keyFile are required",
		},
		{
			name: "CA with client auth none",
			edit: func(mqtt *clusterMQTT) { mqtt.TLS.CAFile = "certs/client-ca.crt" },
			want: "mqtt tls caFile must be empty when clientAuth is none",
		},
		{
			name: "request without CA",
			edit: func(mqtt *clusterMQTT) { mqtt.TLS.ClientAuth = "request" },
			want: "mqtt tls caFile is required when clientAuth is request or require",
		},
		{
			name: "unknown client auth",
			edit: func(mqtt *clusterMQTT) { mqtt.TLS.ClientAuth = "maybe" },
			want: "mqtt tls clientAuth must be none, request, or require",
		},
		{
			name: "unsupported minimum version",
			edit: func(mqtt *clusterMQTT) { mqtt.TLS.MinVersion = "1.1" },
			want: "mqtt tls minVersion must be 1.2 or 1.3",
		},
	}
	for _, test := range invalidCases {
		t.Run(test.name, func(t *testing.T) {
			config := validBootstrapClusterConfig()
			test.edit(&config.MQTT)
			_, err := parseClusterConfig(strings.NewReader(string(mustMarshalClusterConfig(t, config))))
			ccfWantError(t, err, test.want)
		})
	}

	valid := string(mustMarshalClusterConfig(t, validBootstrapClusterConfig()))
	missingMinVersion := strings.Replace(valid, `,"minVersion":"1.2"`, "", 1)
	ccfWantError(t, func() error {
		_, err := parseClusterConfig(strings.NewReader(missingMinVersion))
		return err
	}(), `missing required field "minVersion"`)
	wrongTLSObjectType := strings.Replace(valid,
		`"tls":{"certFile":"certs/mqtt.crt","keyFile":"certs/mqtt.key","caFile":"","clientAuth":"none","minVersion":"1.2"}`,
		`"tls":[]`, 1)
	ccfWantError(t, func() error {
		_, err := parseClusterConfig(strings.NewReader(wrongTLSObjectType))
		return err
	}(), "invalid JSON field type")
}

// Traceability: SEC-01; CFG-SCN-29; docs/specs/configuration-and-runtime-options.md §6; ADR-0052 §1.5.
func TestParseClusterConfigAcceptsNonLoopbackMQTTTCPListener(t *testing.T) {
	for _, address := range []string{"0.0.0.0:1884", "[::]:1884", "10.0.0.5:1884", "mqtt.example:1884"} {
		t.Run(address, func(t *testing.T) {
			config := validBootstrapClusterConfig()
			config.MQTT.TCPListenAddress = address
			parsed, err := parseClusterConfig(strings.NewReader(string(mustMarshalClusterConfig(t, config))))
			if err != nil {
				t.Fatalf("parse non-loopback tcpListenAddress %q: %v", address, err)
			}
			if parsed.MQTT.TCPListenAddress != address || parsed.MQTT.SecurityConfigFile != config.MQTT.SecurityConfigFile {
				t.Fatalf("parsed mqtt = %+v, want tcpListenAddress %q with securityConfigFile %q", parsed.MQTT, address, config.MQTT.SecurityConfigFile)
			}
		})
	}
}

func TestParseClusterConfigTenantQuotaProfile(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.TenantQuotas = validTenantQuotaProfileConfig()
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseClusterConfig(strings.NewReader(string(data)))
	if err != nil {
		t.Fatalf("parse tenant quota profile: %v", err)
	}
	if parsed.TenantQuotas == nil || parsed.TenantQuotas.Version != 1 ||
		parsed.TenantQuotas.Tenants[0].TenantID != "acme" {
		t.Fatalf("parsed tenant quota profile = %+v", parsed.TenantQuotas)
	}
	if parsedWithoutQuota, err := parseClusterConfig(strings.NewReader(string(mustMarshalClusterConfig(t, validBootstrapClusterConfig())))); err != nil {
		t.Fatalf("parse config without tenantQuotas: %v", err)
	} else if parsedWithoutQuota.TenantQuotas != nil {
		t.Fatalf("omitted tenantQuotas = %+v, want nil", parsedWithoutQuota.TenantQuotas)
	}
}

func mustMarshalClusterConfig(t *testing.T, config clusterConfigFile) []byte {
	t.Helper()
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestParseClusterConfigRejectsInvalidInputs(t *testing.T) {
	base := func() clusterConfigFile { return validBootstrapClusterConfig() }
	cases := []struct {
		name string
		edit func(*clusterConfigFile)
	}{
		{name: "wrong mode", edit: func(c *clusterConfigFile) { c.Mode = "standalone" }},
		{name: "zero node ID", edit: func(c *clusterConfigFile) { c.NodeID = 0 }},
		{name: "unsupported version", edit: func(c *clusterConfigFile) { c.ClusterProtocolMinor = 1 }},
		{name: "wrong schema", edit: func(c *clusterConfigFile) { c.StateSchemaMajor = 2 }},
		{name: "unsorted members", edit: func(c *clusterConfigFile) {
			c.BootstrapMembers[0], c.BootstrapMembers[1] = c.BootstrapMembers[1], c.BootstrapMembers[0]
		}},
		{name: "duplicate member IDs", edit: func(c *clusterConfigFile) { c.BootstrapMembers[1].MemberID = 1 }},
		{name: "missing local member", edit: func(c *clusterConfigFile) { c.NodeID = 9 }},
		{name: "wildcard advertised address", edit: func(c *clusterConfigFile) { c.AdvertisedAddress = "0.0.0.0:19001" }},
		{name: "noncanonical port", edit: func(c *clusterConfigFile) { c.ListenAddress = "127.0.0.1:019001" }},
		{name: "invalid DNS SAN", edit: func(c *clusterConfigFile) { c.LocalDNSName = "Node1.example" }},
		{name: "invalid fingerprint", edit: func(c *clusterConfigFile) { c.BootstrapMembers[0].CertificateSHA256 = strings.Repeat("A", 64) }},
		{name: "bootstrap seeds", edit: func(c *clusterConfigFile) { c.SeedAddresses = []string{"node1.example:19001"} }},
		{name: "joiner bootstrap members", edit: func(c *clusterConfigFile) {
			c.Role = "joiner"
			c.NodeID = 4
			c.BootstrapMembers = []clusterBootstrapMember{c.BootstrapMembers[0]}
			c.SeedAddresses = []string{"node1.example:19001"}
		}},
		{name: "joiner without seeds", edit: func(c *clusterConfigFile) {
			c.Role = "joiner"
			c.NodeID = 4
			c.BootstrapMembers = []clusterBootstrapMember{}
			c.SeedAddresses = []string{}
		}},
		{name: "duplicate seeds", edit: func(c *clusterConfigFile) {
			c.Role = "joiner"
			c.NodeID = 4
			c.BootstrapMembers = []clusterBootstrapMember{}
			c.SeedAddresses = []string{"node1.example:19001", "node1.example:19001"}
		}},
		{name: "joiner IP seed", edit: func(c *clusterConfigFile) {
			c.Role = "joiner"
			c.NodeID = 4
			c.BootstrapMembers = []clusterBootstrapMember{}
			c.SeedAddresses = []string{"127.0.0.1:19001"}
		}},
		{name: "blank audit directory", edit: func(c *clusterConfigFile) { c.Management.AuditDirectory = " " }},
		{name: "audit directory equals data directory", edit: func(c *clusterConfigFile) { c.Management.AuditDirectory = c.Directory }},
		{name: "audit directory nested in data directory", edit: func(c *clusterConfigFile) { c.Management.AuditDirectory = c.Directory + "/audit" }},
		{name: "audit directory contains data directory", edit: func(c *clusterConfigFile) { c.Management.AuditDirectory = "data" }},
		{name: "blank token path", edit: func(c *clusterConfigFile) { c.Management.ClusterAdminTokenFile = " " }},
		{name: "blank API key path", edit: func(c *clusterConfigFile) {
			c.Management.APIKeys = []clusterManagementAPIKey{{ID: "dashboard", KeyFile: " ", Roles: []string{"ReadOnly"}}}
		}},
		{name: "tenant quota version zero", edit: func(c *clusterConfigFile) {
			c.TenantQuotas = validTenantQuotaProfileConfig()
			c.TenantQuotas.Version = 0
		}},
		{name: "tenant quota zero limit", edit: func(c *clusterConfigFile) {
			c.TenantQuotas = validTenantQuotaProfileConfig()
			c.TenantQuotas.Default.StorageBytes = 0
		}},
		{name: "tenant quota exceeds global limit", edit: func(c *clusterConfigFile) {
			c.TenantQuotas = validTenantQuotaProfileConfig()
			c.TenantQuotas.Default.RetainedBytes = 17 << 20
		}},
		{name: "tenant quota invalid tenant ID", edit: func(c *clusterConfigFile) {
			c.TenantQuotas = validTenantQuotaProfileConfig()
			c.TenantQuotas.Tenants[0].TenantID = ""
		}},
		{name: "tenant quota unsorted tenants", edit: func(c *clusterConfigFile) {
			c.TenantQuotas = validTenantQuotaProfileConfig()
			tenant := c.TenantQuotas.Tenants[0]
			c.TenantQuotas.Tenants = []clusterTenantQuotaTenantConfig{tenant, tenant}
			c.TenantQuotas.Tenants[0].TenantID = "zeta"
			c.TenantQuotas.Tenants[1].TenantID = "alpha"
		}},
		{name: "oversize cluster ID", edit: func(c *clusterConfigFile) { c.ClusterID = strings.Repeat("x", 201) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := base()
			test.edit(&config)
			data, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parseClusterConfig(strings.NewReader(string(data))); err == nil {
				t.Fatal("parseClusterConfig accepted invalid configuration")
			}
		})
	}
}

// Traceability: SEC-10; docs/specs/tenancy-and-security.md §6.
func TestClusterManagementAccessConfiguration(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.Management.APIKeys = []clusterManagementAPIKey{{ID: "dashboard", KeyFile: "secrets/dashboard.key", Roles: []string{"ReadOnly"}}}
	config.Management.OIDC = &clusterManagementOIDC{
		rawJWTConfig: rawJWTConfig{
			Issuer: "https://issuer.example", Audience: "narumq-management", Algorithms: []string{"RS256"},
			JWKSURL: "https://issuer.example/jwks.json", CacheTTLMillis: 300000, RefreshCooldownMillis: 30000,
			ClockSkewMillis: 30000, HTTPTimeoutMillis: 5000,
		},
		Identities: []clusterManagementOIDCIdentity{{Subject: "alice", Roles: []string{"SecurityAdmin"}}},
	}
	if err := validateClusterConfig(config); err != nil {
		t.Fatalf("valid management access configuration rejected: %v", err)
	}
	parsed, err := parseClusterConfig(strings.NewReader(string(mustMarshalClusterConfig(t, config))))
	if err != nil {
		t.Fatalf("parse management access config: %v", err)
	}
	if parsed.Management.APIKeys[0].ID != "dashboard" || parsed.Management.OIDC.Identities[0].Subject != "alice" {
		t.Fatalf("management access config did not round trip: %+v", parsed.Management)
	}
	verifier, identities, err := validateClusterManagementAccess(parsed.Management)
	if err != nil || verifier == nil || verifier.Issuer != "https://issuer.example" ||
		len(identities) != 1 || identities[0].Roles[0] != adaptershttp.RoleSecurityAdmin {
		t.Fatalf("validated access config = (%+v, %+v, %v)", verifier, identities, err)
	}
	valid := string(mustMarshalClusterConfig(t, validBootstrapClusterConfig()))
	oldConfig := strings.Replace(valid, `"auditDirectory":"data/node1-audit"`, `"readOnlyApiKeyFile":"secrets/old.key","auditDirectory":"data/node1-audit"`, 1)
	if _, err := parseClusterConfig(strings.NewReader(oldConfig)); err == nil {
		t.Fatal("parseClusterConfig accepted the removed readOnlyApiKeyFile field")
	}
	oidcLegacyConfig := string(mustMarshalClusterConfig(t, config))
	oidcLegacyConfig = strings.Replace(oidcLegacyConfig, `"cacheTtlMillis":300000`, `"cacheTtl":"5m"`, 1)
	if oidcLegacyConfig == string(mustMarshalClusterConfig(t, config)) {
		t.Fatal("could not replace cacheTtlMillis in cluster config")
	}
	if _, err := parseClusterConfig(strings.NewReader(oidcLegacyConfig)); err == nil || !strings.Contains(err.Error(), "cacheTtl") {
		t.Fatalf("parseClusterConfig(legacy cacheTtl) error = %v, want unknown cacheTtl field", err)
	}

	cases := []struct {
		name string
		edit func(*clusterManagement)
		want string
	}{
		{"invalid id", func(c *clusterManagement) { c.APIKeys[0].ID = "-bad" }, "management apiKeys[0].id must match ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"},
		{"duplicate id", func(c *clusterManagement) { c.APIKeys = append(c.APIKeys, c.APIKeys[0]) }, `duplicate management apiKeys id "dashboard"`},
		{"invalid key path", func(c *clusterManagement) { c.APIKeys[0].KeyFile = " " }, "management apiKeys[0].keyFile must be a valid path"},
		{"empty key roles", func(c *clusterManagement) { c.APIKeys[0].Roles = nil }, "management apiKeys[0].roles must contain at least one role"},
		{"unknown key role", func(c *clusterManagement) { c.APIKeys[0].Roles = []string{"Reader"} }, `management apiKeys[0] has unknown role "Reader"`},
		{"duplicate key role", func(c *clusterManagement) { c.APIKeys[0].Roles = []string{"ReadOnly", "ReadOnly"} }, `management apiKeys[0] has duplicate role "ReadOnly"`},
		{"missing issuer", func(c *clusterManagement) { c.OIDC.Issuer = "" }, "management.oidc.issuer is required"},
		{"missing audience", func(c *clusterManagement) { c.OIDC.Audience = "" }, "management.oidc.audience is required"},
		{"missing algorithms", func(c *clusterManagement) { c.OIDC.Algorithms = nil }, "management.oidc.algorithms is required"},
		{"empty algorithm", func(c *clusterManagement) { c.OIDC.Algorithms = []string{""} }, "management.oidc algorithm must not be empty"},
		{"duplicate algorithm", func(c *clusterManagement) { c.OIDC.Algorithms = []string{"RS256", "RS256"} }, `duplicate management.oidc algorithm "RS256"`},
		{"nonabsolute JWKS URL", func(c *clusterManagement) { c.OIDC.JWKSURL = "/jwks" }, "management.oidc.jwksUrl must be an absolute URL"},
		{"zero cache TTL", func(c *clusterManagement) { c.OIDC.CacheTTLMillis = 0 }, "management.oidc.cacheTtlMillis must be between 1 and 4294967295"},
		{"zero refresh cooldown", func(c *clusterManagement) { c.OIDC.RefreshCooldownMillis = 0 }, "management.oidc.refreshCooldownMillis must be between 1 and 4294967295"},
		{"zero clock skew", func(c *clusterManagement) { c.OIDC.ClockSkewMillis = 0 }, "management.oidc.clockSkewMillis must be between 1 and 4294967295"},
		{"zero HTTP timeout", func(c *clusterManagement) { c.OIDC.HTTPTimeoutMillis = 0 }, "management.oidc.httpTimeoutMillis must be between 1 and 4294967295"},
		{"invalid OIDC URL", func(c *clusterManagement) { c.OIDC.JWKSURL = "http://example.com/jwks" }, "management.oidc.jwksUrl must be HTTPS or loopback HTTP"},
		{"empty OIDC identities", func(c *clusterManagement) { c.OIDC.Identities = nil }, "management.oidc.identities must contain at least one identity"},
		{"empty subject", func(c *clusterManagement) { c.OIDC.Identities[0].Subject = "" }, "management.oidc.identities[0].subject is required"},
		{"long subject", func(c *clusterManagement) { c.OIDC.Identities[0].Subject = strings.Repeat("a", 252) }, "management.oidc.identities[0].subject must not exceed 251 characters"},
		{"duplicate subject", func(c *clusterManagement) { c.OIDC.Identities = append(c.OIDC.Identities, c.OIDC.Identities[0]) }, `duplicate management.oidc subject "alice"`},
		{"empty identity roles", func(c *clusterManagement) { c.OIDC.Identities[0].Roles = nil }, "management.oidc.identities[0].roles must contain at least one role"},
		{"unknown identity role", func(c *clusterManagement) { c.OIDC.Identities[0].Roles = []string{"Reader"} }, `management.oidc.identities[0] has unknown role "Reader"`},
		{"duplicate identity role", func(c *clusterManagement) { c.OIDC.Identities[0].Roles = []string{"SecurityAdmin", "SecurityAdmin"} }, `management.oidc.identities[0] has duplicate role "SecurityAdmin"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := config.Management
			c.APIKeys = append([]clusterManagementAPIKey(nil), config.Management.APIKeys...)
			c.APIKeys[0].Roles = append([]string(nil), c.APIKeys[0].Roles...)
			oidc := *config.Management.OIDC
			oidc.Identities = append([]clusterManagementOIDCIdentity(nil), oidc.Identities...)
			c.OIDC = &oidc
			tc.edit(&c)
			_, _, err := validateClusterManagementAccess(c)
			if err == nil || !strings.HasPrefix(err.Error(), tc.want) {
				t.Fatalf("validateClusterManagementAccess error = %v, want prefix %q", err, tc.want)
			}
		})
	}
}

// Traceability: SEC-10; docs/specs/tenancy-and-security.md §6.
func TestLoadClusterManagementCredentials(t *testing.T) {
	writeCredential := func(t *testing.T, directory, name, value string) string {
		t.Helper()
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	directory := t.TempDir()
	adminPath := writeCredential(t, directory, "admin.token", " admin-secret \n")
	keyPath := writeCredential(t, directory, "dashboard.key", " dashboard-secret \n")
	management := clusterManagement{
		ClusterAdminTokenFile: adminPath,
		APIKeys:               []clusterManagementAPIKey{{ID: "dashboard", KeyFile: keyPath, Roles: []string{"ReadOnly"}}},
		OIDC: &clusterManagementOIDC{
			rawJWTConfig: rawJWTConfig{
				Issuer: "https://issuer.example", Audience: "narumq-management", Algorithms: []string{"RS256"},
				JWKSURL: "https://issuer.example/jwks.json", CacheTTLMillis: 300000, RefreshCooldownMillis: 30000,
				ClockSkewMillis: 30000, HTTPTimeoutMillis: 5000,
			},
			Identities: []clusterManagementOIDCIdentity{{Subject: "alice", Roles: []string{"SecurityAdmin"}}},
		},
	}
	credentials, err := loadClusterManagementCredentials(management)
	if err != nil || credentials.AdminToken != "admin-secret" || len(credentials.APIKeys) != 1 ||
		credentials.APIKeys[0].Key != "dashboard-secret" || credentials.APIKeys[0].Roles[0] != adaptershttp.RoleReadOnly ||
		credentials.OIDCVerifier == nil || credentials.OIDCVerifier.Issuer != "https://issuer.example" ||
		credentials.OIDCVerifier.HTTPTimeout != 5*time.Second || len(credentials.OIDCIdentities) != 1 {
		t.Fatalf("loaded management credentials = %+v, %v", credentials, err)
	}
	emptyAdminPath := writeCredential(t, directory, "empty-admin.token", "\n \t")
	emptyKeyPath := writeCredential(t, directory, "empty.key", " \n")
	sameKeyPath := writeCredential(t, directory, "same.key", "admin-secret\n")
	sameKey2Path := writeCredential(t, directory, "same2.key", "dashboard-secret\n")
	oidcFree := clusterManagement{ClusterAdminTokenFile: adminPath}
	cases := []struct {
		name   string
		config clusterManagement
		want   string
		cause  error
	}{
		{"missing admin", clusterManagement{ClusterAdminTokenFile: filepath.Join(directory, "missing-admin")}, "read cluster admin token file:", os.ErrNotExist},
		{"empty admin", clusterManagement{ClusterAdminTokenFile: emptyAdminPath}, "cluster admin token file is empty", nil},
		{"missing API key", clusterManagement{ClusterAdminTokenFile: adminPath, APIKeys: []clusterManagementAPIKey{{ID: "missing", KeyFile: filepath.Join(directory, "missing-key"), Roles: []string{"ReadOnly"}}}}, `read management API key file for "missing":`, os.ErrNotExist},
		{"empty API key", clusterManagement{ClusterAdminTokenFile: adminPath, APIKeys: []clusterManagementAPIKey{{ID: "empty", KeyFile: emptyKeyPath, Roles: []string{"ReadOnly"}}}}, `management API key file for "empty" is empty`, nil},
		{"API key equals admin", clusterManagement{ClusterAdminTokenFile: adminPath, APIKeys: []clusterManagementAPIKey{{ID: "same", KeyFile: sameKeyPath, Roles: []string{"ReadOnly"}}}}, `management API key "same" must differ from the cluster admin token`, nil},
		{"duplicate API key secret", clusterManagement{ClusterAdminTokenFile: adminPath, APIKeys: []clusterManagementAPIKey{{ID: "one", KeyFile: keyPath, Roles: []string{"ReadOnly"}}, {ID: "two", KeyFile: sameKey2Path, Roles: []string{"Operator"}}}}, `management API keys "one" and "two" must differ`, nil},
		{"admin only", oidcFree, "", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := loadClusterManagementCredentials(tc.config)
			if tc.want == "" {
				if err != nil || got.AdminToken != "admin-secret" || len(got.APIKeys) != 0 {
					t.Fatalf("load admin-only credentials = %+v, %v", got, err)
				}
			} else if err == nil || !strings.HasPrefix(err.Error(), tc.want) ||
				(tc.cause != nil && !errors.Is(err, tc.cause)) {
				t.Fatalf("load credentials error = %v, want prefix %q wrapping %v", err, tc.want, tc.cause)
			}
		})
	}
}

func TestParseClusterConfigRejectsDuplicateUnknownMissingAndTrailingJSON(t *testing.T) {
	data, err := json.Marshal(validBootstrapClusterConfig())
	if err != nil {
		t.Fatal(err)
	}
	valid := string(data)
	cases := []struct {
		name string
		json string
	}{
		{name: "duplicate top-level", json: strings.Replace(valid, `"mode":"cluster"`, `"mode":"cluster","mode":"cluster"`, 1)},
		{name: "duplicate nested", json: strings.Replace(valid, `"peerTLS":{"certFile":`, `"peerTLS":{"certFile":"first","certFile":`, 1)},
		{name: "unknown field", json: strings.TrimSuffix(valid, "}") + `,"unknown":true}`},
		{name: "releaseVersion from JSON", json: strings.TrimSuffix(valid, "}") + `,"releaseVersion":"1.0.0"}`},
		{name: "missing nested key", json: strings.Replace(valid, `,"keyFile":"certs/node1.key"`, "", 1)},
		{name: "missing audit directory", json: strings.Replace(valid, `,"auditDirectory":"data/node1-audit"`, "", 1)},
		{name: "null array", json: strings.Replace(valid, `"seedAddresses":[]`, `"seedAddresses":null`, 1)},
		{name: "null object", json: strings.Replace(valid, `"peerTLS":{`, `"peerTLS":null`, 1)},
		{name: "trailing value", json: valid + ` {}`},
		{name: "wrong nested type", json: strings.Replace(valid, `"clusterAdminTokenFile":"secrets/admin.token"`, `"clusterAdminTokenFile":12`, 1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseClusterConfig(strings.NewReader(test.json)); err == nil {
				t.Fatal("parseClusterConfig accepted invalid JSON config")
			}
		})
	}
}

func TestParseClusterConfigRejectsMalformedTenantQuotaJSON(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.TenantQuotas = validTenantQuotaProfileConfig()
	valid := string(mustMarshalClusterConfig(t, config))
	cases := []struct {
		name string
		json string
	}{
		{name: "missing profile version", json: strings.Replace(valid, `"version":1,`, "", 1)},
		{name: "missing default limit", json: strings.Replace(valid, `"retainedBytes":1024,`, "", 1)},
		{name: "null tenant list", json: strings.Replace(valid, `"tenants":[`, `"tenants":null,`, 1)},
		{name: "unknown nested field", json: strings.Replace(valid, `"default":{`, `"default":{"unknown":1,`, 1)},
		{name: "wrong tenant list type", json: strings.Replace(valid, `"tenants":[`, `"tenants":{},`, 1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseClusterConfig(strings.NewReader(test.json)); err == nil {
				t.Fatal("parseClusterConfig accepted malformed tenant quota JSON")
			}
		})
	}
}

func TestParseClusterConfigAllowsSiblingAuditDirectory(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.Management.AuditDirectory = "data/node1-audit"
	data := mustMarshalClusterConfig(t, config)
	if _, err := parseClusterConfig(strings.NewReader(string(data))); err != nil {
		t.Fatalf("parseClusterConfig rejected sibling audit directory: %v", err)
	}
}

func TestClusterPathsOverlapAllowsDifferentWindowsVolumes(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows volume paths are only distinct on Windows")
	}
	overlap, err := clusterPathsOverlap(`C:\narumq\data`, `D:\narumq\audit`)
	if err != nil {
		t.Fatalf("clusterPathsOverlap() error = %v, want different volumes accepted as disjoint", err)
	}
	if overlap {
		t.Fatal("clusterPathsOverlap() reported different Windows volumes as overlapping")
	}
}

// Traceability: CLU-09; ADR-0029; docs/specs/cluster-session-and-delivery.md §4.4, §11.27.
func TestParseClusterConfigReplicationProfile(t *testing.T) {
	for _, base := range []clusterConfigFile{validBootstrapClusterConfig(), validJoinerClusterConfig()} {
		t.Run(base.Role, func(t *testing.T) {
			for _, test := range []struct {
				name    string
				profile *clusterReplicationConfig
				raw     string
				wantErr string
			}{
				{name: "omitted"},
				{name: "standard", profile: &clusterReplicationConfig{Factor: 3, WriteQuorum: 2}},
				{name: "five-three", profile: &clusterReplicationConfig{Factor: 5, WriteQuorum: 3}, wantErr: "replication is invalid: replication profile must be factor 3 and writeQuorum 2"},
				{name: "three-three", profile: &clusterReplicationConfig{Factor: 3, WriteQuorum: 3}, wantErr: "replication is invalid: replication profile must be factor 3 and writeQuorum 2"},
				{name: "three-one", profile: &clusterReplicationConfig{Factor: 3, WriteQuorum: 1}, wantErr: "replication is invalid: replication profile must be factor 3 and writeQuorum 2"},
				{name: "zero", profile: &clusterReplicationConfig{}, wantErr: "replication is invalid: replication profile must be factor 3 and writeQuorum 2"},
				{name: "missing quorum", raw: `{"factor":3}`, wantErr: `cluster config is missing required field "writeQuorum"`},
				{name: "unknown key", raw: `{"factor":3,"writeQuorum":2,"x":1}`, wantErr: "cluster config has invalid JSON or field types"},
			} {
				t.Run(test.name, func(t *testing.T) {
					config := base
					config.Replication = test.profile
					data := string(mustMarshalClusterConfig(t, config))
					if test.raw != "" {
						data = strings.TrimSuffix(data, "}") + `,"replication":` + test.raw + "}"
					}
					parsed, err := parseClusterConfig(strings.NewReader(data))
					if test.wantErr != "" {
						if err == nil || err.Error() != test.wantErr {
							t.Fatalf("parse error=%v, want %q", err, test.wantErr)
						}
						return
					}
					if err != nil || parsed.replicationProfile() != membership.StandardReplicationProfile {
						t.Fatalf("parsed profile=%+v error=%v", parsed.replicationProfile(), err)
					}
					if (parsed.Replication == nil) != (test.profile == nil) {
						t.Fatalf("parsed replication=%+v, input=%+v", parsed.Replication, test.profile)
					}
				})
			}
		})
	}
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.5, §11.28.
func TestParseClusterConfigPublishBackpressure(t *testing.T) {
	base := string(mustMarshalClusterConfig(t, validBootstrapClusterConfig()))
	for _, tc := range []struct {
		name, object, wantError string
		minFree                 uint64
		maxLag                  time.Duration
	}{
		{name: "omitted", minFree: 1 << 30, maxLag: time.Second},
		{name: "minimum", object: `{"minFreeBytes":0,"maxCommitLagMillis":100}`, maxLag: 100 * time.Millisecond},
		{name: "maximum", object: `{"minFreeBytes":1125899906842624,"maxCommitLagMillis":10000}`, minFree: 1 << 50, maxLag: 10 * time.Second},
		{name: "missing free", object: `{"maxCommitLagMillis":1000}`, wantError: `cluster config is missing required field "minFreeBytes"`},
		{name: "missing lag", object: `{"minFreeBytes":0}`, wantError: `cluster config is missing required field "maxCommitLagMillis"`},
		{name: "free too large", object: `{"minFreeBytes":1125899906842625,"maxCommitLagMillis":1000}`, wantError: "publishBackpressure minFreeBytes must be at most 1125899906842624"},
		{name: "lag too small", object: `{"minFreeBytes":0,"maxCommitLagMillis":99}`, wantError: "publishBackpressure maxCommitLagMillis must be between 100 and 10000"},
		{name: "lag too large", object: `{"minFreeBytes":0,"maxCommitLagMillis":10001}`, wantError: "publishBackpressure maxCommitLagMillis must be between 100 and 10000"},
		{name: "unknown", object: `{"minFreeBytes":0,"maxCommitLagMillis":1000,"unknown":1}`, wantError: "cluster config has invalid JSON or field types"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := base
			if tc.object != "" {
				input = strings.TrimSuffix(base, "}") + `,"publishBackpressure":` + tc.object + "}"
			}
			parsed, err := parseClusterConfig(strings.NewReader(input))
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("error=%v; want %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			minFree, maxLag := parsed.publishBackpressure()
			if minFree != tc.minFree || maxLag != tc.maxLag {
				t.Fatalf("pressure=(%d,%s)", minFree, maxLag)
			}
		})
	}
}

// Traceability: PERF-08; docs/specs/configuration-and-runtime-options.md §6.
func TestParseClusterConfigRuntimeMetrics(t *testing.T) {
	base := string(mustMarshalClusterConfig(t, validBootstrapClusterConfig()))
	for _, tc := range []struct {
		name, object, wantOutput string
		wantInterval             uint64
		wantDuration             time.Duration
		wantError                string
	}{
		{name: "omitted"},
		{
			name: "minimum", object: `{"outputFile":"metrics/node1.jsonl","intervalMillis":1}`,
			wantOutput: "metrics/node1.jsonl", wantInterval: 1, wantDuration: time.Millisecond,
		},
		{
			name: "maximum", object: `{"outputFile":"metrics/node1.jsonl","intervalMillis":3600000}`,
			wantOutput: "metrics/node1.jsonl", wantInterval: 3_600_000, wantDuration: time.Hour,
		},
		{name: "missing output", object: `{"intervalMillis":1}`, wantError: `cluster config is missing required field "outputFile"`},
		{name: "missing interval", object: `{"outputFile":"metrics/node1.jsonl"}`, wantError: `cluster config is missing required field "intervalMillis"`},
		{name: "zero interval", object: `{"outputFile":"metrics/node1.jsonl","intervalMillis":0}`, wantError: "runtimeMetrics intervalMillis must be between 1 and 3600000"},
		{name: "interval too large", object: `{"outputFile":"metrics/node1.jsonl","intervalMillis":3600001}`, wantError: "runtimeMetrics intervalMillis must be between 1 and 3600000"},
		{name: "empty output", object: `{"outputFile":"","intervalMillis":1}`, wantError: "runtimeMetrics outputFile is required"},
		{name: "output has leading whitespace", object: `{"outputFile":" metrics.jsonl","intervalMillis":1}`, wantError: "runtimeMetrics outputFile is required"},
		{name: "overlaps directory", object: `{"outputFile":"data/node1/metrics.jsonl","intervalMillis":1}`, wantError: "runtimeMetrics outputFile must not overlap directory or management auditDirectory"},
		{name: "overlaps audit directory", object: `{"outputFile":"data/node1-audit/metrics.jsonl","intervalMillis":1}`, wantError: "runtimeMetrics outputFile must not overlap directory or management auditDirectory"},
		{name: "unknown", object: `{"outputFile":"metrics/node1.jsonl","intervalMillis":1,"unknown":1}`, wantError: "cluster config has invalid JSON or field types"},
		{name: "wrong object type", object: `[]`, wantError: `cluster config field "runtimeMetrics" has invalid JSON field type`},
		{name: "duplicate key", object: `{"outputFile":"a.jsonl","outputFile":"b.jsonl","intervalMillis":1}`, wantError: "cluster config contains duplicate JSON key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := base
			if tc.object != "" {
				input = strings.TrimSuffix(base, "}") + `,"runtimeMetrics":` + tc.object + "}"
			}
			parsed, err := parseClusterConfig(strings.NewReader(input))
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("error=%v; want %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantOutput == "" {
				if parsed.RuntimeMetrics != nil {
					t.Fatalf("RuntimeMetrics=%+v; want nil", parsed.RuntimeMetrics)
				}
				return
			}
			if parsed.RuntimeMetrics == nil {
				t.Fatal("RuntimeMetrics is nil")
			}
			if parsed.RuntimeMetrics.OutputFile != tc.wantOutput || parsed.RuntimeMetrics.IntervalMillis != tc.wantInterval {
				t.Fatalf("RuntimeMetrics=%+v", parsed.RuntimeMetrics)
			}
			if got := parsed.runtimeMetricsInterval(); got != tc.wantDuration {
				t.Fatalf("runtimeMetricsInterval()=%s; want %s", got, tc.wantDuration)
			}
		})
	}
}
