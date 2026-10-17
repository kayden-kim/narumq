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
	if p.ProfileID != "bp07-smoke-v1" || p.rounds() != 3 || p.clients() != 18 || digest != "e1f305e1e5c38e4b00e161bfce733c46cd393075517241978594470fb508a7fb" {
		t.Fatalf("unexpected smoke identity: profile=%#v digest=%s", p, digest)
	}
}

// TestDecodeProfileRejectsUnknownMissingNullAndTrailing verifies unknown, missing, null and trailing JSON input is rejected.
// Scenarios: performance-and-verification.md:PRF-SCN-018
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
	for _, invalid := range [][]byte{append(append([]byte(nil), data...), []byte(" {}")...), []byte(strings.Replace(string(data), "\"seed\": 7", "\"seed\": 7, \"unknown\": true", 1))} {
		if _, err := DecodeProfile(invalid); err == nil {
			t.Fatal("accepted unknown field or trailing value")
		}
	}
}

// TestProfileRejectsUnsafeRollingCombinations verifies out-of-range and unsafe rolling combinations are rejected.
// Scenarios: performance-and-verification.md:PRF-SCN-018
func TestProfileRejectsUnsafeRollingCombinations(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Profile)
	}{
		{"fractional_LB_seconds", func(p *Profile) { p.LBPropagationMillis = 1500 }},
		{"drain_equals_LB", func(p *Profile) { p.DrainTimeoutMillis = 2000 }},
		{"Will_equals_reconnect", func(p *Profile) { p.WillDelaySeconds = 30 }},
		{"duplicate_node", func(p *Profile) { p.ReplacementOrder = []int{1, 1, 2} }},
		{"license_capacity", func(p *Profile) { p.IdleClients = 9 }},
		{"health_poll_exceeds_propagation", func(p *Profile) { p.HealthPollMillis = 1500 }},
		{"session_expires_before_Will", func(p *Profile) { p.SessionExpirySeconds = 60 }},
		{"insufficient_warmup_capacity", func(p *Profile) { p.MaxMessages = 199 }},
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

// TestProfileAcceptsBoundaryClientCapacityAndZeroSettles verifies the profile bounds accept exact boundary values.
// Scenarios: performance-and-verification.md:PRF-SCN-018
func TestProfileAcceptsBoundaryClientCapacityAndZeroSettles(t *testing.T) {
	p := loadSmoke(t)
	p.IdleClients = 8
	p.Cycles = 10
	p.InterRoundSettleMillis = 0
	p.WillSettleMillis = 0
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	if p.clients() != 19 || p.rounds() != 30 {
		t.Fatalf("incorrect capacity clients=%d rounds=%d", p.clients(), p.rounds())
	}
}
