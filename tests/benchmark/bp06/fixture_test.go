package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFixtureLimitsClientsAndACLToFailureWorkload verifies fixture credentials, ACL and client allow list.
// Scenarios: performance-and-verification.md:PRF-SCN-029
func TestFixtureLimitsClientsAndACLToFailureWorkload(t *testing.T) {
	profile := loadSmoke(t)
	fixture, err := newBP06Fixture(profile, "fixture-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	}()
	data, err := os.ReadFile(filepath.Join(fixture.NodeConfigDirectories[0], "mqtt-security.json"))
	if err != nil {
		t.Fatal(err)
	}
	var security bp06SecurityConfig
	if err := json.Unmarshal(data, &security); err != nil {
		t.Fatal(err)
	}
	allowed := make(map[string]bool)
	for _, identity := range security.Identities {
		for _, id := range identity.AllowedClientIDs {
			if strings.Contains(id, "will") {
				t.Fatalf("Will identity allowed: %s", id)
			}
			allowed[id] = true
		}
	}
	for _, client := range workloadClients(profile) {
		if !allowed[client.id] {
			t.Errorf("workload client denied by fixture: %s", client.id)
		}
	}
	actions := make(map[string]bool)
	for _, rule := range security.ACL {
		if rule.TopicFilter != "bp06/#" || rule.Effect != "allow" || (rule.Action != "subscribe" && rule.Action != "publish") {
			t.Fatalf("unexpected ACL rule: %#v", rule)
		}
		actions[rule.Action] = true
	}
	if !actions["subscribe"] || !actions["publish"] {
		t.Fatalf("missing required actions: %v", actions)
	}
	acl, err := os.ReadFile(filepath.Join(fixture.EMQXConfigDirectory, "acl.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if string(acl) != "{allow, {username, \"jwt\"}, all, [\"bp06/#\"]}.\n{deny, all}.\n" {
		t.Fatalf("unexpected EMQX access policy: %s", acl)
	}
}
