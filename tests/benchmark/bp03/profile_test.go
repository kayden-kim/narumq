package main

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validBP03Profile() Profile {
	return Profile{
		ProfileID:                "bp03-test",
		Purpose:                  purposeSmoke,
		Seed:                     3,
		PairsPerQoS:              2,
		PayloadBytes:             256,
		SessionExpirySeconds:     300,
		MaxInflightPerPublisher:  8,
		AckTimeoutMillis:         10000,
		RatesPerStreamPerSecond:  []uint64{5, 10, 25},
		WarmupMillis:             2000,
		MeasureMillis:            12000,
		DrainMillis:              5000,
		OfflineSettleMillis:      1000,
		OfflineMessagesPerStream: 32,
		ReconnectTimeoutMillis:   20000,
		MaxMessages:              4096,
		BrokerCPUs:               1,
		BrokerMemoryBytes:        1 << 30,
		GeneratorCPUs:            2,
		GeneratorMemoryBytes:     1 << 29,
	}
}

func profileFixtureJSON() string {
	return `{
  "profileId": "bp03-test",
  "purpose": "smoke",
  "seed": 3,
  "pairsPerQoS": 2,
  "payloadBytes": 256,
  "sessionExpirySeconds": 300,
  "maxInflightPerPublisher": 8,
  "ackTimeoutMillis": 10000,
  "ratesPerStreamPerSecond": [5, 10, 25],
  "warmupMillis": 2000,
  "measureMillis": 12000,
  "drainMillis": 5000,
  "offlineSettleMillis": 1000,
  "offlineMessagesPerStream": 32,
  "reconnectTimeoutMillis": 20000,
  "maxMessages": 4096,
  "brokerCPUs": 1,
  "brokerMemoryBytes": 1073741824,
  "generatorCPUs": 2,
  "generatorMemoryBytes": 536870912
}`
}

func TestDecodeProfileStrictAndComputesMessageBound(t *testing.T) {
	profile, err := DecodeProfile([]byte(profileFixtureJSON()))
	if err != nil {
		t.Fatalf("DecodeProfile() error = %v", err)
	}
	if profile.PairsPerQoS != 2 || profile.PayloadBytes != 256 || profile.SessionExpirySeconds != 300 || profile.MaxMessages != 4096 {
		t.Fatalf("DecodeProfile() = %#v, want fixed BP-03 smoke values", profile)
	}
	if got, err := profile.expectedMessageCount(); err != nil || got != 2368 {
		t.Fatalf("expectedMessageCount() = %d, %v; want 2368", got, err)
	}
}

func TestBundledSmokeProfileDecodes(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "smoke.json"))
	if err != nil {
		t.Fatal(err)
	}
	profile, err := DecodeProfile(data)
	if err != nil {
		t.Fatalf("DecodeProfile(smoke.json) error = %v", err)
	}
	if profile.ProfileID != "bp03-smoke-v1" || profile.Purpose != purposeSmoke {
		t.Fatalf("bundled smoke profile identity = %q/%q", profile.ProfileID, profile.Purpose)
	}
	if got, err := profile.expectedMessageCount(); err != nil || got != 2368 {
		t.Fatalf("bundled smoke workload count = %d, %v; want 2368", got, err)
	}
}

func TestDecodeProfileRejectsMissingUnknownTrailingAndOverflow(t *testing.T) {
	missingFields := []string{
		"profileId", "purpose", "seed", "pairsPerQoS", "payloadBytes", "sessionExpirySeconds",
		"maxInflightPerPublisher", "ackTimeoutMillis", "ratesPerStreamPerSecond", "warmupMillis",
		"measureMillis", "drainMillis", "offlineSettleMillis", "offlineMessagesPerStream",
		"reconnectTimeoutMillis", "maxMessages", "brokerCPUs", "brokerMemoryBytes",
		"generatorCPUs", "generatorMemoryBytes",
	}
	for _, field := range missingFields {
		t.Run("missing_"+field, func(t *testing.T) {
			lines := strings.Split(profileFixtureJSON(), "\n")
			filtered := make([]string, 0, len(lines))
			for _, line := range lines {
				if strings.HasPrefix(strings.TrimSpace(line), `"`+field+`":`) {
					continue
				}
				filtered = append(filtered, line)
			}
			data := strings.Replace(strings.Join(filtered, "\n"), ",\n}", "\n}", 1)
			_, err := DecodeProfile([]byte(data))
			if err == nil || !strings.Contains(err.Error(), `field "`+field+`" is required`) {
				t.Fatalf("DecodeProfile() error = %v, want required-field error for %s", err, field)
			}
		})
	}

	cases := []struct {
		name string
		data string
		want string
	}{
		{
			name: "unknown field",
			data: strings.Replace(profileFixtureJSON(), `  "pairsPerQoS": 2,`, `  "pairsPerQoS": 2,`+"\n"+`  "unexpected": true,`, 1),
			want: "unknown field",
		},
		{name: "trailing JSON value", data: profileFixtureJSON() + " {}", want: "multiple JSON values"},
		{
			name: "maxMessages uint64 overflow",
			data: strings.Replace(profileFixtureJSON(), `"maxMessages": 4096`, `"maxMessages": 18446744073709551616`, 1),
			want: "cannot unmarshal",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := DecodeProfile([]byte(testCase.data))
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("DecodeProfile() error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func TestProfileRejectsEveryOutOfRangeValue(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Profile)
	}{
		{"empty profileId", func(p *Profile) { p.ProfileID = " " }},
		{"unsupported purpose", func(p *Profile) { p.Purpose = "capacity" }},
		{"pairs below minimum", func(p *Profile) { p.PairsPerQoS = 0 }},
		{"pairs above maximum", func(p *Profile) { p.PairsPerQoS = 9 }},
		{"payload below minimum", func(p *Profile) { p.PayloadBytes = 15 }},
		{"payload above maximum", func(p *Profile) { p.PayloadBytes = 65537 }},
		{"session expiry below minimum", func(p *Profile) { p.SessionExpirySeconds = 0 }},
		{"session expiry above maximum", func(p *Profile) { p.SessionExpirySeconds = 86401 }},
		{"inflight below minimum", func(p *Profile) { p.MaxInflightPerPublisher = 0 }},
		{"inflight above maximum", func(p *Profile) { p.MaxInflightPerPublisher = 33 }},
		{"ack timeout below minimum", func(p *Profile) { p.AckTimeoutMillis = 999 }},
		{"ack timeout above maximum", func(p *Profile) { p.AckTimeoutMillis = 60001 }},
		{"no rates", func(p *Profile) { p.RatesPerStreamPerSecond = nil }},
		{"too many rates", func(p *Profile) { p.RatesPerStreamPerSecond = []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9} }},
		{"rate below minimum", func(p *Profile) { p.RatesPerStreamPerSecond = []uint64{0} }},
		{"rate above maximum", func(p *Profile) { p.RatesPerStreamPerSecond = []uint64{1001} }},
		{"rates not increasing", func(p *Profile) { p.RatesPerStreamPerSecond = []uint64{5, 5} }},
		{"warmup nonpositive", func(p *Profile) { p.WarmupMillis = 0 }},
		{"measurement too short", func(p *Profile) { p.MeasureMillis = 2999 }},
		{"drain nonpositive", func(p *Profile) { p.DrainMillis = 0 }},
		{"negative offline settle", func(p *Profile) { p.OfflineSettleMillis = -1 }},
		{"offline messages below minimum", func(p *Profile) { p.OfflineMessagesPerStream = 0 }},
		{"offline messages above maximum", func(p *Profile) { p.OfflineMessagesPerStream = 49 }},
		{"reconnect timeout too short", func(p *Profile) { p.ReconnectTimeoutMillis = 999 }},
		{"maxMessages below computed workload", func(p *Profile) { p.MaxMessages = 2367 }},
		{"broker CPUs zero", func(p *Profile) { p.BrokerCPUs = 0 }},
		{"broker CPUs NaN", func(p *Profile) { p.BrokerCPUs = math.NaN() }},
		{"broker CPUs infinite", func(p *Profile) { p.BrokerCPUs = math.Inf(1) }},
		{"broker memory nonpositive", func(p *Profile) { p.BrokerMemoryBytes = 0 }},
		{"generator CPUs nonpositive", func(p *Profile) { p.GeneratorCPUs = -1 }},
		{"generator memory nonpositive", func(p *Profile) { p.GeneratorMemoryBytes = 0 }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			profile := validBP03Profile()
			testCase.modify(&profile)
			if err := profile.Validate(); err == nil {
				t.Fatal("Profile.Validate() accepted out-of-range value")
			}
		})
	}
}

func TestLoadProfileReturnsExactBytesAndSHA256(t *testing.T) {
	data := []byte(profileFixtureJSON())
	path := filepath.Join(t.TempDir(), "profile.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, gotBytes, gotHash, err := LoadProfile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if string(gotBytes) != string(data) || gotHash != hex.EncodeToString(digest[:]) {
		t.Fatalf("LoadProfile() returned bytes/hash %q/%q, want exact bytes and %x", gotBytes, gotHash, digest)
	}
}
