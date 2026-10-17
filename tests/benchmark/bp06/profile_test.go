package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func loadSmoke(t *testing.T) Profile {
	t.Helper()
	p, _, _, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSmokeProfileIdentityAndDigest(t *testing.T) {
	p, _, digest, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	if p.ProfileID != "bp06-smoke-v1" || p.rounds() != 3 || p.clients() != 15 || digest != "ca5e28b04623e708d9521ab34ce5ed3b9dd617d4ac57c4571243bdd6061f6a15" {
		t.Fatalf("unexpected smoke identity: profile=%#v digest=%s", p, digest)
	}
}

// TestDecodeProfileRejectsUnknownMissingNullAndTrailing verifies strict profile decoding.
// Scenarios: performance-and-verification.md:PRF-SCN-028
func TestDecodeProfileRejectsUnknownMissingNullAndTrailing(t *testing.T) {
	data, err := os.ReadFile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for field := range raw {
		t.Run("missing_"+field, func(t *testing.T) {
			copyFields := make(map[string]json.RawMessage, len(raw))
			for key, value := range raw {
				copyFields[key] = value
			}
			delete(copyFields, field)
			encoded, _ := json.Marshal(copyFields)
			if _, err := DecodeProfile(encoded); err == nil {
				t.Fatalf("accepted missing %s", field)
			}
			copyFields[field] = json.RawMessage("null")
			encoded, _ = json.Marshal(copyFields)
			if _, err := DecodeProfile(encoded); err == nil {
				t.Fatalf("accepted null %s", field)
			}
		})
	}
	for _, invalid := range [][]byte{append(append([]byte(nil), data...), []byte(" {}")...), []byte(strings.Replace(string(data), "\"seed\": 6", "\"seed\": 6, \"unknown\": true", 1))} {
		if _, err := DecodeProfile(invalid); err == nil {
			t.Fatal("accepted unknown field or trailing value")
		}
	}
}

// TestProfileRejectsUnsafeRecoveryCombinations verifies rejection of unsafe timeout, expiry and capacity combinations.
// Scenarios: performance-and-verification.md:PRF-SCN-028
func TestProfileRejectsUnsafeRecoveryCombinations(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Profile)
	}{
		{"session_expiry", func(p *Profile) { p.SessionExpirySeconds = 60; p.DownMillis = 30000 }},
		{"resync_before_ready", func(p *Profile) { p.ResyncTimeoutMillis = p.ReadyTimeoutMillis - 1 }},
		{"recovery_before_reconnect", func(p *Profile) { p.RecoveryTimeoutMillis = p.ReconnectTimeoutMillis - 1 }},
		{"duplicate_node", func(p *Profile) { p.KillOrder = []int{1, 1, 2} }},
		{"license_capacity", func(p *Profile) { p.IdleClients = 12 }},
		{"insufficient_capacity", func(p *Profile) { p.MaxMessages = 399 }},
		{"excessive_cycles", func(p *Profile) { p.Cycles = 11 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := loadSmoke(t)
			tc.change(&p)
			if err := p.Validate(); err == nil {
				t.Fatal("accepted unsafe workload")
			}
		})
	}
}

// TestProfileAcceptsBoundaryClientCapacity verifies the capacity boundary is accepted.
// Scenarios: performance-and-verification.md:PRF-SCN-028
func TestProfileAcceptsBoundaryClientCapacity(t *testing.T) {
	p := loadSmoke(t)
	p.IdleClients = 11
	p.Cycles = 10
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if p.clients() != 19 || p.rounds() != 30 {
		t.Fatalf("incorrect capacity clients=%d rounds=%d", p.clients(), p.rounds())
	}
}
