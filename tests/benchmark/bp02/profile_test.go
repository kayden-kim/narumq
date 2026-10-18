package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func profileSourceForTest(t *testing.T) (Profile, []byte) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "smoke.json"))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := DecodeProfile(data)
	if err != nil {
		t.Fatal(err)
	}
	return profile, data
}

// TestLoadProfilePreservesSourceHash verifies the profile SHA-256 source hash is preserved.
// Scenarios: performance-and-verification.md:PRF-SCN-040
func TestLoadProfilePreservesSourceHash(t *testing.T) {
	path := filepath.Join("testdata", "smoke.json")
	profile, source, hash, err := LoadProfile(path)
	if err != nil {
		t.Fatal(err)
	}
	if profile.ProfileID != "bp02-smoke-v1" || profile.Purpose != purposeSmoke {
		t.Fatalf("loaded profile identity = %q/%q", profile.ProfileID, profile.Purpose)
	}
	digest := sha256.Sum256(source)
	if hash != hex.EncodeToString(digest[:]) {
		t.Fatalf("profile SHA-256 = %q, want %q", hash, hex.EncodeToString(digest[:]))
	}
	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(source) != string(actual) {
		t.Fatal("LoadProfile changed the exact source bytes")
	}
}

// TestDecodeProfileRejectsUnknownFieldAndInvalidBounds verifies rejection of unknown fields and out-of-range values.
// Scenarios: performance-and-verification.md:PRF-SCN-040
func TestDecodeProfileRejectsUnknownFieldAndInvalidBounds(t *testing.T) {
	_, source := profileSourceForTest(t)
	var profile map[string]json.RawMessage
	if err := json.Unmarshal(source, &profile); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		change func(map[string]json.RawMessage)
	}{
		{name: "unknown field", change: func(fields map[string]json.RawMessage) { fields["unexpected"] = json.RawMessage(`true`) }},
		{name: "payload size below bound", change: func(fields map[string]json.RawMessage) { fields["payloadSizeCycle"] = json.RawMessage(`[31]`) }},
		{name: "rate not strictly increasing", change: func(fields map[string]json.RawMessage) { fields["ratesPerStreamPerSecond"] = json.RawMessage(`[5,2]`) }},
		{name: "max messages below computed count", change: func(fields map[string]json.RawMessage) { fields["maxMessages"] = json.RawMessage(`575`) }},
		{name: "offline count above durable limit", change: func(fields map[string]json.RawMessage) { fields["offlineMessagesPerStream"] = json.RawMessage(`65`) }},
		{name: "missing required field", change: func(fields map[string]json.RawMessage) { delete(fields, "sessionExpirySeconds") }},
		{name: "trailing JSON value", change: func(fields map[string]json.RawMessage) {}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fields := make(map[string]json.RawMessage, len(profile)+1)
			for key, value := range profile {
				fields[key] = append(json.RawMessage(nil), value...)
			}
			test.change(fields)
			data, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if test.name == "trailing JSON value" {
				data = append(data, []byte(` {}`)...)
			}
			if _, err := DecodeProfile(data); err == nil {
				t.Fatal("DecodeProfile accepted invalid profile")
			}
		})
	}
}

func TestDecodeProfileAcceptsBoundsAndCalculatesSmokeCount(t *testing.T) {
	p, _ := profileSourceForTest(t)
	count, err := p.expectedMessageCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 576 {
		t.Fatalf("computed message count = %d, want 576", count)
	}

	p.PayloadSizeCycle = []int{32}
	p.KeepAliveSeconds, p.SessionExpirySeconds = 10, 60
	p.TLSEveryNthClient, p.IdleClientsTenantA, p.IdleClientsTenantB = 1, 0, 0
	p.RatesPerStreamPerSecond = []uint64{1}
	p.MaxInflightPerPublisher, p.AckTimeoutMillis = 1, 1000
	p.WarmupMillis, p.MeasureMillis, p.DrainMillis = 1000, 3000, 1000
	p.IdleBaselineMillis, p.SubscribedIdleMillis = 3000, 3000
	p.OfflineSettleMillis, p.OfflineMessagesPerStream, p.OfflinePublishRatePerSecond = 100, 1, 1
	p.ReconnectTimeoutMillis = 1000
	p.RetainedTopicsTenantA, p.RetainedTopicsTenantB = 1, 1
	p.RetainedReplayTimeoutMillis, p.ReadyTimeoutMillis, p.SetupTimeoutMillis = 1000, 10000, 5000
	p.MaxMessages, p.BrokerCPUs, p.GeneratorCPUs = 31, 0.001, 0.001
	p.BrokerMemoryBytes, p.GeneratorMemoryBytes = 1, 1
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate rejected lower inclusive bounds: %v", err)
	}

	p.PayloadSizeCycle = make([]int, 32)
	for i := range p.PayloadSizeCycle {
		p.PayloadSizeCycle[i] = 65536
	}
	p.KeepAliveSeconds, p.SessionExpirySeconds = 300, 3600
	p.TLSEveryNthClient, p.IdleClientsTenantA, p.IdleClientsTenantB = 16, 256, 256
	p.RatesPerStreamPerSecond = []uint64{50}
	p.MaxInflightPerPublisher, p.AckTimeoutMillis = 64, 60000
	p.WarmupMillis, p.MeasureMillis, p.DrainMillis = 60000, 120000, 30000
	p.IdleBaselineMillis, p.SubscribedIdleMillis = 60000, 60000
	p.OfflineSettleMillis, p.OfflineMessagesPerStream, p.OfflinePublishRatePerSecond = 10000, 64, 50
	p.ReconnectTimeoutMillis = 120000
	p.RetainedTopicsTenantA, p.RetainedTopicsTenantB = 64, 64
	p.RetainedReplayTimeoutMillis, p.ReadyTimeoutMillis, p.SetupTimeoutMillis = 60000, 600000, 600000
	p.MaxMessages = 1_000_000
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate rejected upper inclusive bounds: %v", err)
	}
}

// TestDecodeProfileRejectsNullAndMalformedInput verifies rejection of null and missing fields.
// Scenarios: performance-and-verification.md:PRF-SCN-040
func TestDecodeProfileRejectsNullAndMalformedInput(t *testing.T) {
	_, source := profileSourceForTest(t)
	for name, data := range map[string][]byte{
		"null payload cycle": []byte(strings.Replace(string(source), `"payloadSizeCycle": [128, 128, 128, 128, 128, 128, 512, 512, 2048, 16384]`, `"payloadSizeCycle": null`, 1)),
		"malformed JSON":     []byte(`{"profileId":`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeProfile(data); err == nil {
				t.Fatal("DecodeProfile accepted invalid input")
			}
		})
	}
}
