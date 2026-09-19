package main

import (
	"encoding/json"
	"strings"
	"testing"

	"narumq/internal/domain/mqtt"
)

func validProfile() Profile {
	return Profile{
		ProfileID:            "bp01-test",
		Purpose:              purposeSmoke,
		Seed:                 7,
		Topic:                "bp01/test",
		PayloadBytes:         32,
		PublishRatePerSecond: 20,
		WarmupMillis:         100,
		MeasureMillis:        500,
		DrainMillis:          100,
		MaxSamples:           16,
		BrokerCPUs:           1,
		BrokerMemoryBytes:    128 << 20,
		GeneratorCPUs:        1,
		GeneratorMemoryBytes: 128 << 20,
	}
}

func profileFixtureJSON() string {
	return `{
  "profileId": "bp01-test",
  "purpose": "smoke",
  "seed": 7,
  "topic": "bp01/test",
  "payloadBytes": 32,
  "publishRatePerSecond": 20,
  "warmupMillis": 100,
  "measureMillis": 500,
  "drainMillis": 100,
  "maxSamples": 16,
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
	if profile.OfferedMeasurementSamples() != 10 {
		t.Fatalf("OfferedMeasurementSamples() = %d, want 10", profile.OfferedMeasurementSamples())
	}
	if profile.ProtocolVersion() != mqtt.ProtocolV5 || profile.QoS() != mqtt.QoSAtMostOnce || !profile.CleanStart() || profile.SessionExpiryInterval() != 0 {
		t.Fatal("profile protocol contract changed")
	}
}

func TestDecodeProfileRejectsUnknownAndMissingFields(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string
	}{
		{name: "unknown", json: strings.Replace(profileFixtureJSON(), `"seed": 7,`, "\"seed\": 7,\n  \"unexpected\": true,", 1), want: "unknown field"},
		{name: "missing", json: strings.Replace(profileFixtureJSON(), "  \"topic\": \"bp01/test\",\n", "", 1), want: `field "topic" is required`},
		{name: "trailing", json: profileFixtureJSON() + " {}", want: "multiple JSON values"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := DecodeProfile([]byte(testCase.json))
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("DecodeProfile() error = %v, want substring %q", err, testCase.want)
			}
		})
	}
}

func TestProfileRejectsInvalidWorkloadBounds(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Profile)
		want   string
	}{
		{name: "short payload", modify: func(profile *Profile) { profile.PayloadBytes = 15 }, want: "payloadBytes"},
		{name: "zero duration", modify: func(profile *Profile) { profile.MeasureMillis = 0 }, want: "must be greater than zero"},
		{name: "negative duration", modify: func(profile *Profile) { profile.DrainMillis = -1 }, want: "must be greater than zero"},
		{name: "sample cap", modify: func(profile *Profile) { profile.MaxSamples = 9 }, want: "exceeds maxSamples"},
		{name: "purpose", modify: func(profile *Profile) { profile.Purpose = "baseline" }, want: "unsupported"},
		{name: "wildcard topic", modify: func(profile *Profile) { profile.Topic = "bp01/+" }, want: "exact topic"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			profile := validProfile()
			testCase.modify(&profile)
			err := profile.Validate()
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, testCase.want)
			}
		})
	}
}

func TestDecodeProfileRejectsInvalidContractValues(t *testing.T) {
	cases := []struct {
		name string
		args []any
		want string
	}{
		{name: "mqtt 311", args: []any{mqtt.ProtocolV311, mqtt.QoSAtMostOnce, true, uint32(0)}, want: "MQTT 5"},
		{name: "qos 1", args: []any{mqtt.ProtocolV5, mqtt.QoSAtLeastOnce, true, uint32(0)}, want: "QoS 0"},
		{name: "clean start false", args: []any{mqtt.ProtocolV5, mqtt.QoSAtMostOnce, false, uint32(0)}, want: "Clean Start"},
		{name: "expiry", args: []any{mqtt.ProtocolV5, mqtt.QoSAtMostOnce, true, uint32(1)}, want: "Session Expiry"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateProtocolContract(testCase.args[0].(mqtt.ProtocolVersion), testCase.args[1].(mqtt.QoS), testCase.args[2].(bool), testCase.args[3].(uint32))
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("validateProtocolContract() error = %v, want substring %q", err, testCase.want)
			}
		})
	}
}

func TestDecodeProfileRejectsNegativeJSONValues(t *testing.T) {
	cases := []struct {
		name  string
		field string
	}{
		{name: "payloadBytes", field: "payloadBytes"},
		{name: "brokerMemoryBytes", field: "brokerMemoryBytes"},
		{name: "generatorMemoryBytes", field: "generatorMemoryBytes"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			document := map[string]any{}
			if err := json.Unmarshal([]byte(profileFixtureJSON()), &document); err != nil {
				t.Fatal(err)
			}
			document[testCase.field] = -1
			input, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			_, err = DecodeProfile(input)
			if err == nil || !strings.Contains(err.Error(), "must not be negative") {
				t.Fatalf("DecodeProfile() error = %v, want negative-value error", err)
			}
		})
	}
}

func TestProfileValidateRejectsInvalidCPUResources(t *testing.T) {
	profile := validProfile()
	profile.BrokerCPUs = 0
	if err := profile.Validate(); err == nil {
		t.Fatal("Validate() accepted zero broker CPUs")
	}
	profile = validProfile()
	profile.GeneratorCPUs = -1
	if err := profile.Validate(); err == nil {
		t.Fatal("Validate() accepted negative generator CPUs")
	}
}
