package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func validBootstrapClusterConfig() clusterConfigFile {
	return clusterConfigFile{
		Mode: "cluster", ClusterID: "sample-cluster", NodeID: 1, Directory: "data/node1",
		ListenAddress: "127.0.0.1:19001", AdvertisedAddress: "127.0.0.1:19001", LocalDNSName: "node1.example",
		ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1,
		Role: "bootstrap",
		BootstrapMembers: []clusterBootstrapMember{
			{MemberID: 1, Address: "127.0.0.1:19001", DNSSAN: "node1.example", CertificateSHA256: strings.Repeat("a", 64)},
			{MemberID: 2, Address: "127.0.0.1:19002", DNSSAN: "node2.example", CertificateSHA256: strings.Repeat("b", 64)},
			{MemberID: 3, Address: "127.0.0.1:19003", DNSSAN: "node3.example", CertificateSHA256: strings.Repeat("c", 64)},
		},
		SeedAddresses: []string{},
		PeerTLS:       clusterPeerTLS{CertFile: "certs/node1.crt", KeyFile: "certs/node1.key", CAFile: "certs/ca.crt"},
		Management:    clusterManagement{ListenAddress: "127.0.0.1:18080", ClusterAdminTokenFile: "secrets/admin.token"},
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

func TestParseClusterConfigValidRoles(t *testing.T) {
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
	}
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
		{name: "blank token path", edit: func(c *clusterConfigFile) { c.Management.ClusterAdminTokenFile = " " }},
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
		{name: "missing required key", json: strings.Replace(valid, `,"stateSchemaMinor":0`, "", 1)},
		{name: "missing nested key", json: strings.Replace(valid, `,"keyFile":"certs/node1.key"`, "", 1)},
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
