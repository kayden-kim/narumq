package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
)

// DrainProfile defines the fixed test-only drain evidence matrix.
type DrainProfile struct {
	SchemaVersion          int                     `json:"schemaVersion"`
	ConnectTimeoutMillis   int64                   `json:"connectTimeoutMillis"`
	OperationTimeoutMillis int64                   `json:"operationTimeoutMillis"`
	SessionExpirySeconds   int64                   `json:"sessionExpirySeconds"`
	WillDelaySeconds       int64                   `json:"willDelaySeconds"`
	ReconnectWithinMillis  int64                   `json:"reconnectWithinMillis"`
	ReconnectBaseMillis    int64                   `json:"reconnectBaseMillis"`
	ReconnectCapMillis     int64                   `json:"reconnectCapMillis"`
	MaxReconnectAttempts   int                     `json:"maxReconnectAttempts"`
	JitterSeed             int64                   `json:"jitterSeed"`
	Cluster                DrainClusterSettings    `json:"cluster"`
	FakeServer             DrainFakeServerSettings `json:"fakeServer"`
	ClientIDs              DrainClientIDs          `json:"clientIds"`
	ExpectedCases          []DrainExpectedCase     `json:"expectedCases"`
}

// DrainClusterSettings fixes the cluster topology and test-only drain bounds.
type DrainClusterSettings struct {
	CommonEndpoint                     string `json:"commonEndpoint"`
	ServerReference                    string `json:"serverReference"`
	ObserverEndpoint                   string `json:"observerEndpoint"`
	DrainNodeID                        int    `json:"drainNodeId"`
	ConnectionRatePerSecond            int    `json:"connectionRatePerSecond"`
	LoadBalancerPropagationDelayMillis int    `json:"loadBalancerPropagationDelayMillis"`
	DrainTimeoutMillis                 int    `json:"drainTimeoutMillis"`
}

// DrainFakeServerSettings identifies the configured and forbidden endpoints.
type DrainFakeServerSettings struct {
	Endpoint     string `json:"endpoint"`
	TrapEndpoint string `json:"trapEndpoint"`
}

// DrainClientIDs contains the exact authenticated fixture identities.
type DrainClientIDs struct {
	Client    string `json:"client"`
	Observer  string `json:"observer"`
	Publisher string `json:"publisher"`
}

// DrainExpectedCase specifies one SDK drain or malformed DISCONNECT result.
type DrainExpectedCase struct {
	CaseID          string           `json:"caseId"`
	Protocol        string           `json:"protocol"`
	Profile         string           `json:"profile"`
	Kind            string           `json:"kind"`
	Vector          string           `json:"vector"`
	ExpectedResults []ExpectedResult `json:"expectedResults"`
}

func (c DrainExpectedCase) asExpectedCase() ExpectedCase {
	return ExpectedCase{CaseID: c.CaseID, Protocol: c.Protocol, Profile: c.Profile, ExpectedResults: c.ExpectedResults}
}

func canonicalDrainCases() []DrainExpectedCase {
	cases := make([]DrainExpectedCase, 0, 10)
	vectors := []struct{ suffix, kind, vector string }{
		{"drain-redirect", "cluster_drain", ""},
		{"malformed-truncated-property", "malformed_disconnect", "truncated_property"},
		{"malformed-disallowed-property", "malformed_disconnect", "disallowed_property"},
		{"malformed-duplicate-server-reference", "malformed_disconnect", "duplicate_server_reference"},
		{"malformed-invalid-server-reference", "malformed_disconnect", "invalid_server_reference_format"},
	}
	for _, sdk := range []string{"go", "python"} {
		for _, v := range vectors {
			observations := []string{"vector", "disconnectSurface", "serverReferenceAccepted", "reconnectEndpoint", "permanentEndpoint"}
			if v.kind == "cluster_drain" {
				observations = []string{"disconnectReasonCode", "serverReference", "serverReferenceAccepted", "reconnectEndpoint", "reconnectElapsedMillis", "sessionPresent", "postRedirectDelivery", "willReceived", "willClassification", "permanentEndpoint"}
			}
			cases = append(cases, DrainExpectedCase{CaseID: sdk + "-mqtt5-" + v.suffix, Protocol: "mqtt5", Profile: "MQTT5_DRAIN_SAFE", Kind: v.kind, Vector: v.vector, ExpectedResults: []ExpectedResult{{SDK: sdk, Role: "client", RequiredObservations: observations}}})
		}
	}
	return cases
}

func canonicalDrainProfile() DrainProfile {
	return DrainProfile{
		SchemaVersion: 1, ConnectTimeoutMillis: 10000, OperationTimeoutMillis: 45000, SessionExpirySeconds: 60, WillDelaySeconds: 5,
		ReconnectWithinMillis: 2000, ReconnectBaseMillis: 100, ReconnectCapMillis: 2000, MaxReconnectAttempts: 5, JitterSeed: 20260921,
		Cluster:    DrainClusterSettings{CommonEndpoint: "lb.compat.test:1883", ServerReference: "node2.compat.test:1883", ObserverEndpoint: "node1.compat.test:1883", DrainNodeID: 3, ConnectionRatePerSecond: 10, LoadBalancerPropagationDelayMillis: 1000, DrainTimeoutMillis: 60000},
		FakeServer: DrainFakeServerSettings{Endpoint: "localhost:1883", TrapEndpoint: "localhost:1886"},
		ClientIDs:  DrainClientIDs{Client: "compat-drain-client", Observer: "compat-drain-observer", Publisher: "compat-drain-publisher"}, ExpectedCases: canonicalDrainCases(),
	}
}

// Validate rejects any drift from the approved drain profile, including order.
func (p DrainProfile) Validate() error {
	if !reflect.DeepEqual(p, canonicalDrainProfile()) {
		return errors.New("drain profile does not match the canonical test-only matrix")
	}
	return nil
}

// DecodeDrainProfile strictly decodes and validates one drain profile.
func DecodeDrainProfile(data []byte) (DrainProfile, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var p DrainProfile
	if err := decoder.Decode(&p); err != nil {
		return p, fmt.Errorf("decode drain profile: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return p, errors.New("drain profile contains trailing data")
	}
	return p, p.Validate()
}

// LoadDrainProfile reads the immutable test-only drain execution contract.
func LoadDrainProfile(path string) (DrainProfile, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return DrainProfile{}, nil, err
	}
	p, err := DecodeDrainProfile(data)
	return p, data, err
}
