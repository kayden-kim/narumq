package main

import (
	"strings"
	"testing"
)

func TestCheckedInProfileMatchesCanonicalMatrix(t *testing.T) {
	profile, _, err := LoadProfile("profile.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(profile.ExpectedCases) != 26 {
		t.Fatalf("expected case count = %d, want 26", len(profile.ExpectedCases))
	}
	if got := expectedSDKVersion("go", "mqtt311"); got != goMQTT311Version {
		t.Fatalf("MQTT 3.1.1 Go version = %q, want %q", got, goMQTT311Version)
	}
	if got := expectedSDKVersion("go", "mqtt5"); got != goMQTT5Version {
		t.Fatalf("MQTT 5 Go version = %q, want %q", got, goMQTT5Version)
	}
	if got := expectedSDKVersion("python", "mqtt5"); got != pythonMQTTVersion {
		t.Fatalf("Python version = %q, want %q", got, pythonMQTTVersion)
	}
}

func TestDecodeProfileRejectsUnknownAndTrailingData(t *testing.T) {
	_, data, err := LoadProfile("profile.json")
	if err != nil {
		t.Fatal(err)
	}
	unknown := strings.TrimSuffix(string(data), "\n")
	unknown = strings.TrimSuffix(unknown, "}") + `,"unknown":true}`
	if _, err := DecodeProfile([]byte(unknown)); err == nil {
		t.Fatal("DecodeProfile accepted an unknown field")
	}
	if _, err := DecodeProfile(append(data, []byte("{}")...)); err == nil {
		t.Fatal("DecodeProfile accepted a second JSON value")
	}
}
func TestProfileRequiresExactPhase1Bounds(t *testing.T) {
	profile, _, err := LoadProfile("profile.json")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		set  func(*Profile)
	}{
		{"connect timeout", func(p *Profile) { p.ConnectTimeoutMillis++ }},
		{"operation timeout", func(p *Profile) { p.OperationTimeoutMillis++ }},
		{"session expiry", func(p *Profile) { p.SessionExpirySeconds++ }},
		{"will delay", func(p *Profile) { p.WillDelaySeconds++ }},
		{"reconnect within", func(p *Profile) { p.ReconnectWithinMillis++ }},
		{"reconnect base", func(p *Profile) { p.ReconnectBaseMillis++ }},
		{"reconnect cap", func(p *Profile) { p.ReconnectCapMillis++ }},
		{"maximum attempts", func(p *Profile) { p.MaxReconnectAttempts++ }},
		{"jitter seed", func(p *Profile) { p.JitterSeed++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := profile
			test.set(&mutated)
			if err := mutated.Validate(); err == nil {
				t.Fatalf("Validate accepted changed %s", test.name)
			}
		})
	}
}

func TestProfileRejectsUnsafeBounds(t *testing.T) {
	profile, _, err := LoadProfile("profile.json")
	if err != nil {
		t.Fatal(err)
	}
	profile.ReconnectWithinMillis = profile.WillDelaySeconds * 1000
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "reconnectWithinMillis") {
		t.Fatalf("invalid reconnect bound error = %v", err)
	}
	profile, _, err = LoadProfile("profile.json")
	if err != nil {
		t.Fatal(err)
	}
	profile.ReconnectBaseMillis = profile.ReconnectCapMillis + 1
	if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), "reconnectBaseMillis") {
		t.Fatalf("invalid jitter bound error = %v", err)
	}
}
