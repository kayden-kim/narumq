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
		MQTT:          clusterMQTT{TCPListenAddress: "", TLSListenAddress: "127.0.0.1:1883", SecurityConfigFile: "security.json"},
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
		{name: "blank read-only API key path", edit: func(c *clusterConfigFile) { c.Management.ReadOnlyAPIKeyFile = " " }},
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

// Traceability: UI-01; docs/specs/management-api-and-observability.md §8.
func TestClusterManagementReadOnlyAPIKeyConfiguration(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.Management.ReadOnlyAPIKeyFile = "secrets/read-only.key"
	if err := validateClusterConfig(config); err != nil {
		t.Fatalf("valid read-only API key path rejected: %v", err)
	}
	parsed, err := parseClusterConfig(strings.NewReader(string(mustMarshalClusterConfig(t, config))))
	if err != nil {
		t.Fatalf("parse read-only API key config: %v", err)
	}
	if parsed.Management.ReadOnlyAPIKeyFile != config.Management.ReadOnlyAPIKeyFile {
		t.Fatalf("read-only API key path = %q, want %q", parsed.Management.ReadOnlyAPIKeyFile, config.Management.ReadOnlyAPIKeyFile)
	}

	for _, path := range []string{" ", " secrets/key", "secrets/key "} {
		invalid := validBootstrapClusterConfig()
		invalid.Management.ReadOnlyAPIKeyFile = path
		if err := validateClusterConfig(invalid); err == nil || err.Error() != "management readOnlyApiKeyFile must be a valid path" {
			t.Fatalf("validate readOnlyApiKeyFile %q error = %v, want invalid path error", path, err)
		}
	}
}

// Traceability: UI-01; docs/specs/management-api-and-observability.md §8.
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
	readOnlyPath := writeCredential(t, directory, "read-only.key", " read-only-secret \n")
	adminToken, readOnlyAPIKey, err := loadClusterManagementCredentials(clusterManagement{
		ClusterAdminTokenFile: adminPath, ReadOnlyAPIKeyFile: readOnlyPath,
	})
	if err != nil || adminToken != "admin-secret" || readOnlyAPIKey != "read-only-secret" {
		t.Fatalf("loaded credentials = (%q, %q, %v), want trimmed distinct values", adminToken, readOnlyAPIKey, err)
	}
	adminOnlyToken, noReadOnlyKey, err := loadClusterManagementCredentials(clusterManagement{ClusterAdminTokenFile: adminPath})
	if err != nil || adminOnlyToken != "admin-secret" || noReadOnlyKey != "" {
		t.Fatalf("admin-only credentials = (%q, %q, %v), want admin token and no API key", adminOnlyToken, noReadOnlyKey, err)
	}

	emptyAdminPath := writeCredential(t, directory, "empty-admin.token", "\n \t")
	emptyReadOnlyPath := writeCredential(t, directory, "empty-read-only.key", " \n")
	sameReadOnlyPath := writeCredential(t, directory, "same-read-only.key", "admin-secret\n")
	for _, testCase := range []struct {
		name      string
		config    clusterManagement
		wantError string
		wantCause error
	}{
		{
			name: "admin file missing",
			config: clusterManagement{
				ClusterAdminTokenFile: filepath.Join(directory, "missing-admin.token"),
			},
			wantError: "read cluster admin token file:",
			wantCause: os.ErrNotExist,
		},
		{
			name: "admin file empty",
			config: clusterManagement{
				ClusterAdminTokenFile: emptyAdminPath,
			},
			wantError: "cluster admin token file is empty",
		},
		{
			name: "read-only key file missing",
			config: clusterManagement{
				ClusterAdminTokenFile: adminPath, ReadOnlyAPIKeyFile: filepath.Join(directory, "missing-read-only.key"),
			},
			wantError: "read management read-only API key file:",
			wantCause: os.ErrNotExist,
		},
		{
			name: "read-only key file empty",
			config: clusterManagement{
				ClusterAdminTokenFile: adminPath, ReadOnlyAPIKeyFile: emptyReadOnlyPath,
			},
			wantError: "management read-only API key file is empty",
		},
		{
			name: "read-only key equals admin token",
			config: clusterManagement{
				ClusterAdminTokenFile: adminPath, ReadOnlyAPIKeyFile: sameReadOnlyPath,
			},
			wantError: "management read-only API key must differ from the cluster admin token",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, _, err := loadClusterManagementCredentials(testCase.config)
			if err == nil {
				t.Fatal("loadClusterManagementCredentials succeeded, want error")
			}
			if testCase.wantCause == nil {
				if err.Error() != testCase.wantError {
					t.Fatalf("error = %q, want %q", err, testCase.wantError)
				}
			} else if !strings.HasPrefix(err.Error(), testCase.wantError) || !errors.Is(err, testCase.wantCause) {
				t.Fatalf("error = %v, want prefix %q wrapping %v", err, testCase.wantError, testCase.wantCause)
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
