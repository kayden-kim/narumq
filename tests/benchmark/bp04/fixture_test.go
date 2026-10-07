package main

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestBP04FixtureRestrictsClientIdentityAndCleansUp(t *testing.T) {
	profile := smokeProfile(t)
	fixture, err := newBP04Fixture(profile, "fixture-test")
	if err != nil {
		t.Fatal(err)
	}
	fixtureDirectory := fixture.Directory
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Errorf("close BP-04 fixture: %v", err)
		}
	})

	securityBytes, err := os.ReadFile(filepath.Join(fixture.NodeConfigDirectories[0], "mqtt-security.json"))
	if err != nil {
		t.Fatal(err)
	}
	var security bp04SecurityConfig
	if err := json.Unmarshal(securityBytes, &security); err != nil {
		t.Fatal(err)
	}
	if security.JWT.Issuer != "https://issuer.example" || security.JWT.Audience != "narumq" ||
		!slices.Equal(security.JWT.Algorithms, []string{"RS256"}) {
		t.Fatalf("JWT configuration = %+v", security.JWT)
	}
	if len(security.Identities) != 1 {
		t.Fatalf("identity count = %d, want 1", len(security.Identities))
	}
	identity := security.Identities[0]
	if identity.Issuer != "https://issuer.example" || identity.Subject != "bp04-client" ||
		identity.TenantID != bp04TenantID || identity.CredentialID != "bp04-client-credential" ||
		!slices.Equal(identity.Roles, []string{"client"}) {
		t.Fatalf("fixture identity = %+v", identity)
	}
	wantClientIDs := bp04ClientIDs(profile)
	if !slices.Equal(identity.AllowedClientIDs, wantClientIDs) {
		t.Fatalf("allowed client IDs differ from the workload plan: got %d, want %d", len(identity.AllowedClientIDs), len(wantClientIDs))
	}
	seenClientIDs := make(map[string]struct{}, len(identity.AllowedClientIDs))
	for _, clientID := range identity.AllowedClientIDs {
		if _, exists := seenClientIDs[clientID]; exists {
			t.Fatalf("duplicate allowed client ID %q", clientID)
		}
		seenClientIDs[clientID] = struct{}{}
	}
	if len(security.ACL) != 2 || security.ACL[0] != (bp04ACLRule{TenantID: bp04TenantID, Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "bp04/#"}) ||
		security.ACL[1] != (bp04ACLRule{TenantID: bp04TenantID, Role: "client", Effect: "allow", Action: "publish", TopicFilter: "bp04/#"}) {
		t.Fatalf("fixture ACL = %+v", security.ACL)
	}

	credentialsBytes, err := os.ReadFile(filepath.Join(fixture.RunnerConfigDirectory, "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	var credentials bp04RunnerCredentials
	if err := json.Unmarshal(credentialsBytes, &credentials); err != nil {
		t.Fatal(err)
	}
	if credentials.Username != "jwt" || credentials.Password == "" || credentials.ManagementToken != fixture.ManagementToken {
		t.Fatalf("runner credentials do not match fixture identity: username=%q passwordSet=%t managementTokenMatches=%t", credentials.Username, credentials.Password != "", credentials.ManagementToken == fixture.ManagementToken)
	}
	runnerFiles, err := os.ReadDir(fixture.RunnerConfigDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(runnerFiles) != 2 || runnerFiles[0].Name() != "credentials.json" || runnerFiles[1].Name() != "jwks.json" {
		t.Fatalf("runner config files = %v, want credentials.json and jwks.json", runnerFiles)
	}
	networkOwnerFiles, err := os.ReadDir(fixture.NetworkOwnerConfigDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(networkOwnerFiles) != 1 || networkOwnerFiles[0].Name() != "jwks.json" {
		t.Fatalf("network-owner config files = %v, want only jwks.json", networkOwnerFiles)
	}

	caPEM, err := os.ReadFile(filepath.Join(fixture.NodeConfigDirectories[0], "ca.pem"))
	if err != nil {
		t.Fatal(err)
	}
	caBlock, _ := pem.Decode(caPEM)
	if caBlock == nil {
		t.Fatal("fixture CA PEM is missing")
	}
	caCertificate, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	nodePEM, err := os.ReadFile(filepath.Join(fixture.NodeConfigDirectories[0], "node.pem"))
	if err != nil {
		t.Fatal(err)
	}
	nodeBlock, _ := pem.Decode(nodePEM)
	if nodeBlock == nil {
		t.Fatal("node certificate PEM is missing")
	}
	nodeCertificate, err := x509.ParseCertificate(nodeBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(caCertificate)
	if _, err := nodeCertificate.Verify(x509.VerifyOptions{
		Roots: roots, DNSName: "node1.bp04.test", KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		t.Fatalf("node mTLS certificate verification failed: %v", err)
	}

	aclBytes, err := os.ReadFile(filepath.Join(fixture.EMQXConfigDirectory, "acl.conf"))
	if err != nil {
		t.Fatal(err)
	}
	const wantEMQXACL = "{allow, {username, \"jwt\"}, all, [\"bp04/#\", \"$share/bp04g/bp04/#\"]}.\n{deny, all}.\n"
	if string(aclBytes) != wantEMQXACL {
		t.Fatalf("EMQX ACL = %q, want %q", aclBytes, wantEMQXACL)
	}

	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(fixtureDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fixture directory still exists after Close: stat error = %v", err)
	}
}
