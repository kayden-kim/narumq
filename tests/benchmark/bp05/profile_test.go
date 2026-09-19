package main

import (
	"strings"
	"testing"
)

func validBP05Profile() Profile {
	return Profile{
		ProfileID:                "bp05-test",
		Purpose:                  purposeSmoke,
		ConnectionCount:          8,
		RampConnectionsPerSecond: 8,
		BaselineMillis:           3000,
		IdleMillis:               2000,
		ReconnectCount:           2,
		ReconnectRatePerSecond:   2,
		KeepAliveSeconds:         2,
		PingIntervalMillis:       500,
		BrokerCPUs:               1,
		BrokerMemoryBytes:        128 << 20,
		GeneratorCPUs:            1,
		GeneratorMemoryBytes:     128 << 20,
	}
}

func profileFixtureJSON() string {
	return `{
  "profileId": "bp05-test",
  "purpose": "smoke",
  "connectionCount": 8,
  "rampConnectionsPerSecond": 8,
  "baselineMillis": 3000,
  "idleMillis": 2000,
  "reconnectCount": 2,
  "reconnectRatePerSecond": 2,
  "keepAliveSeconds": 2,
  "pingIntervalMillis": 500,
  "brokerCPUs": 1,
  "brokerMemoryBytes": 134217728,
  "generatorCPUs": 1,
  "generatorMemoryBytes": 134217728
}`
}

func TestDecodeProfileStrictAndValidate(t *testing.T) {
	profile, err := DecodeProfile([]byte(profileFixtureJSON()))
	if err != nil {
		t.Fatalf("DecodeProfile() error = %v", err)
	}
	if profile.ConnectionCount != 8 || profile.ReconnectCount != 2 || profile.Purpose != purposeSmoke {
		t.Fatalf("DecodeProfile() = %#v, want fixed smoke values", profile)
	}
}

func TestDecodeProfileRejectsMissingUnknownTrailingAndOverflow(t *testing.T) {
	cases := []struct {
		name string
		data string
		want string
	}{
		{name: "missing", data: strings.Replace(profileFixtureJSON(), "  \"idleMillis\": 2000,\n", "", 1), want: `field "idleMillis" is required`},
		{name: "unknown", data: strings.Replace(profileFixtureJSON(), "  \"connectionCount\": 8,\n", "  \"connectionCount\": 8,\n  \"unexpected\": true,\n", 1), want: "unknown field"},
		{name: "trailing", data: profileFixtureJSON() + " {}", want: "multiple JSON values"},
		{name: "overflow", data: strings.Replace(profileFixtureJSON(), "  \"connectionCount\": 8,", "  \"connectionCount\": 9223372036854775808,", 1), want: "cannot unmarshal"},
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

func TestProfileRejectsScaleAndTimingViolations(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Profile)
		want   string
	}{
		{name: "zero rate", modify: func(profile *Profile) { profile.RampConnectionsPerSecond = 0 }, want: "greater than zero"},
		{name: "reconnect too large", modify: func(profile *Profile) { profile.ReconnectCount = profile.ConnectionCount + 1 }, want: "reconnectCount"},
		{name: "ping reaches keepalive", modify: func(profile *Profile) { profile.PingIntervalMillis = 2000 }, want: "less than"},
		{name: "idle has one ping", modify: func(profile *Profile) { profile.IdleMillis = 999 }, want: "twice"},
		{name: "short baseline", modify: func(profile *Profile) { profile.BaselineMillis = 1999 }, want: "at least 2000"},
		{name: "negative duration", modify: func(profile *Profile) { profile.IdleMillis = -1 }, want: "greater than zero"},
		{name: "unsupported purpose", modify: func(profile *Profile) { profile.Purpose = "capacity" }, want: "unsupported"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			profile := validBP05Profile()
			testCase.modify(&profile)
			if err := profile.Validate(); err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("Validate() error = %v, want %q", err, testCase.want)
			}
		})
	}
}
