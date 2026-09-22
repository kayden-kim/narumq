package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	profileSchemaVersion         = 1
	goMQTT311Version             = "v1.5.1"
	goMQTT5Version               = "v0.23.0"
	pythonMQTTVersion            = "2.1.0"
	phase1ConnectTimeoutMillis   = int64(10000)
	phase1OperationTimeoutMillis = int64(10000)
	phase1SessionExpirySeconds   = int64(60)
	phase1WillDelaySeconds       = int64(5)
	phase1ReconnectWithinMillis  = int64(2000)
	phase1ReconnectBaseMillis    = int64(100)
	phase1ReconnectCapMillis     = int64(2000)
	phase1MaxReconnectAttempts   = 5
	jitterSeed                   = int64(20260921)
)

var runIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// Profile is the checked-in, test-only compatibility execution contract.
type Profile struct {
	SchemaVersion          int            `json:"schemaVersion"`
	ConnectTimeoutMillis   int64          `json:"connectTimeoutMillis"`
	OperationTimeoutMillis int64          `json:"operationTimeoutMillis"`
	SessionExpirySeconds   int64          `json:"sessionExpirySeconds"`
	WillDelaySeconds       int64          `json:"willDelaySeconds"`
	ReconnectWithinMillis  int64          `json:"reconnectWithinMillis"`
	ReconnectBaseMillis    int64          `json:"reconnectBaseMillis"`
	ReconnectCapMillis     int64          `json:"reconnectCapMillis"`
	MaxReconnectAttempts   int            `json:"maxReconnectAttempts"`
	JitterSeed             int64          `json:"jitterSeed"`
	ExpectedCases          []ExpectedCase `json:"expectedCases"`
}

// ExpectedCase describes every result required for one compatibility case.
type ExpectedCase struct {
	CaseID          string           `json:"caseId"`
	Protocol        string           `json:"protocol"`
	Profile         string           `json:"profile"`
	Proxy           bool             `json:"proxy"`
	ExpectedResults []ExpectedResult `json:"expectedResults"`
}

// ExpectedResult describes one role result required by a case.
type ExpectedResult struct {
	SDK                  string   `json:"sdk"`
	Role                 string   `json:"role"`
	RequiredObservations []string `json:"requiredObservations"`
}

// Validate checks profile bounds, case identity and the exact Phase 1 matrix.
func (p Profile) Validate() error {
	if p.SchemaVersion != profileSchemaVersion {
		return fmt.Errorf("unsupported profile schema version %d", p.SchemaVersion)
	}
	positive := []struct {
		name  string
		value int64
	}{
		{"connectTimeoutMillis", p.ConnectTimeoutMillis},
		{"operationTimeoutMillis", p.OperationTimeoutMillis},
		{"sessionExpirySeconds", p.SessionExpirySeconds},
		{"willDelaySeconds", p.WillDelaySeconds},
		{"reconnectWithinMillis", p.ReconnectWithinMillis},
		{"reconnectBaseMillis", p.ReconnectBaseMillis},
		{"reconnectCapMillis", p.ReconnectCapMillis},
	}
	for _, item := range positive {
		if item.value <= 0 {
			return fmt.Errorf("%s must be positive", item.name)
		}
	}
	if p.MaxReconnectAttempts <= 0 {
		return errors.New("maxReconnectAttempts must be positive")
	}
	if p.ReconnectWithinMillis >= p.WillDelaySeconds*1000 {
		return errors.New("reconnectWithinMillis must be less than willDelaySeconds")
	}
	if p.ReconnectBaseMillis > p.ReconnectCapMillis {
		return errors.New("reconnectBaseMillis must not exceed reconnectCapMillis")
	}
	exact := []struct {
		name string
		got  int64
		want int64
	}{
		{"connectTimeoutMillis", p.ConnectTimeoutMillis, phase1ConnectTimeoutMillis},
		{"operationTimeoutMillis", p.OperationTimeoutMillis, phase1OperationTimeoutMillis},
		{"sessionExpirySeconds", p.SessionExpirySeconds, phase1SessionExpirySeconds},
		{"willDelaySeconds", p.WillDelaySeconds, phase1WillDelaySeconds},
		{"reconnectWithinMillis", p.ReconnectWithinMillis, phase1ReconnectWithinMillis},
		{"reconnectBaseMillis", p.ReconnectBaseMillis, phase1ReconnectBaseMillis},
		{"reconnectCapMillis", p.ReconnectCapMillis, phase1ReconnectCapMillis},
	}
	for _, item := range exact {
		if item.got != item.want {
			return fmt.Errorf("%s must equal %d for the Phase 1 evidence profile", item.name, item.want)
		}
	}
	if p.MaxReconnectAttempts != phase1MaxReconnectAttempts {
		return fmt.Errorf("maxReconnectAttempts must equal %d for the Phase 1 evidence profile", phase1MaxReconnectAttempts)
	}
	if p.JitterSeed != jitterSeed {
		return fmt.Errorf("jitterSeed must be %d", jitterSeed)
	}
	want := canonicalExpectedCases()
	if len(p.ExpectedCases) != len(want) {
		return fmt.Errorf("expectedCases has %d entries, want %d", len(p.ExpectedCases), len(want))
	}
	seen := make(map[string]struct{}, len(p.ExpectedCases))
	for index, got := range p.ExpectedCases {
		if strings.TrimSpace(got.CaseID) == "" {
			return fmt.Errorf("expectedCases[%d] has empty caseId", index)
		}
		if _, ok := seen[got.CaseID]; ok {
			return fmt.Errorf("duplicate expected case %q", got.CaseID)
		}
		seen[got.CaseID] = struct{}{}
		if err := validateExpectedCase(got); err != nil {
			return fmt.Errorf("expectedCases[%d] %w", index, err)
		}
		if !equalExpectedCase(got, want[index]) {
			return fmt.Errorf("expected case %q does not match canonical matrix position %d", got.CaseID, index)
		}
	}
	return nil
}

func validateExpectedCase(c ExpectedCase) error {
	if !validProtocol(c.Protocol) {
		return fmt.Errorf("unsupported protocol %q", c.Protocol)
	}
	if strings.TrimSpace(c.Profile) == "" {
		return errors.New("profile is required")
	}
	if len(c.ExpectedResults) == 0 {
		return errors.New("expectedResults must not be empty")
	}
	seen := make(map[string]struct{}, len(c.ExpectedResults))
	for index, result := range c.ExpectedResults {
		if result.SDK != "go" && result.SDK != "python" {
			return fmt.Errorf("expectedResults[%d] has unsupported sdk %q", index, result.SDK)
		}
		if strings.TrimSpace(result.Role) == "" {
			return fmt.Errorf("expectedResults[%d] has empty role", index)
		}
		if _, ok := seen[result.Role]; ok {
			return fmt.Errorf("duplicate expected role %q", result.Role)
		}
		seen[result.Role] = struct{}{}
		if len(result.RequiredObservations) == 0 {
			return fmt.Errorf("expectedResults[%d] has no required observations", index)
		}
	}
	return nil
}

func validProtocol(protocol string) bool {
	return protocol == "mqtt311" || protocol == "mqtt5" || protocol == "mqtt31"
}

// DecodeProfile strictly decodes one JSON value and rejects trailing data.
func DecodeProfile(data []byte) (Profile, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var profile Profile
	if err := decoder.Decode(&profile); err != nil {
		return Profile{}, fmt.Errorf("decode profile: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Profile{}, errors.New("profile contains a second JSON value")
		}
		return Profile{}, fmt.Errorf("decode trailing profile data: %w", err)
	}
	if err := profile.Validate(); err != nil {
		return Profile{}, err
	}
	return profile, nil
}

// LoadProfile reads, strictly decodes and validates a profile file.
func LoadProfile(path string) (Profile, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, nil, fmt.Errorf("read profile: %w", err)
	}
	profile, err := DecodeProfile(data)
	if err != nil {
		return Profile{}, data, err
	}
	return profile, data, nil
}

func canonicalExpectedCases() []ExpectedCase {
	cases := make([]ExpectedCase, 0, 26)
	for _, sdk := range []string{"go", "python"} {
		cases = append(cases,
			ExpectedCase{CaseID: sdk + "-mqtt311-basic-roundtrip", Protocol: "mqtt311", Profile: "MQTT311_BASIC", ExpectedResults: oneClientResult(sdk, "basic")},
			ExpectedCase{CaseID: sdk + "-mqtt311-persistent-resume", Protocol: "mqtt311", Profile: "MQTT311_PERSISTENT", ExpectedResults: oneClientResult(sdk, "persistent")},
			ExpectedCase{CaseID: sdk + "-mqtt311-qos1-retransmit", Protocol: "mqtt311", Profile: "MQTT311_PERSISTENT", Proxy: true, ExpectedResults: oneClientResult(sdk, "qos1")},
			ExpectedCase{CaseID: sdk + "-mqtt311-qos2-retransmit", Protocol: "mqtt311", Profile: "MQTT311_PERSISTENT", Proxy: true, ExpectedResults: oneClientResult(sdk, "qos2")},
			ExpectedCase{CaseID: sdk + "-mqtt311-abrupt-will", Protocol: "mqtt311", Profile: "MQTT311_BASIC", ExpectedResults: oneClientResult(sdk, "will311")},
			ExpectedCase{CaseID: sdk + "-mqtt5-basic-roundtrip", Protocol: "mqtt5", Profile: "MQTT5_STANDARD", ExpectedResults: oneClientResult(sdk, "basic")},
			ExpectedCase{CaseID: sdk + "-mqtt5-persistent-resume", Protocol: "mqtt5", Profile: "MQTT5_STANDARD", ExpectedResults: oneClientResult(sdk, "persistent")},
			ExpectedCase{CaseID: sdk + "-mqtt5-qos1-retransmit", Protocol: "mqtt5", Profile: "MQTT5_STANDARD", Proxy: true, ExpectedResults: oneClientResult(sdk, "qos1")},
			ExpectedCase{CaseID: sdk + "-mqtt5-qos2-retransmit", Protocol: "mqtt5", Profile: "MQTT5_STANDARD", Proxy: true, ExpectedResults: oneClientResult(sdk, "qos2")},
			ExpectedCase{CaseID: sdk + "-mqtt5-will-delay-resume", Protocol: "mqtt5", Profile: "MQTT5_STANDARD", ExpectedResults: oneClientResult(sdk, "will5")},
		)
	}
	cases = append(cases,
		ExpectedCase{CaseID: "go-to-python-mqtt311-qos1", Protocol: "mqtt311", Profile: "MQTT311_BASIC", ExpectedResults: crossResults("go", "python")},
		ExpectedCase{CaseID: "python-to-go-mqtt311-qos1", Protocol: "mqtt311", Profile: "MQTT311_BASIC", ExpectedResults: crossResults("python", "go")},
		ExpectedCase{CaseID: "go-to-python-mqtt5-qos1", Protocol: "mqtt5", Profile: "MQTT5_STANDARD", ExpectedResults: crossResults("go", "python")},
		ExpectedCase{CaseID: "python-to-go-mqtt5-qos1", Protocol: "mqtt5", Profile: "MQTT5_STANDARD", ExpectedResults: crossResults("python", "go")},
		ExpectedCase{CaseID: "go-mqtt31-rejected", Protocol: "mqtt31", Profile: "MQTT31_REJECTED", ExpectedResults: []ExpectedResult{{SDK: "go", Role: "client", RequiredObservations: []string{"connackAccepted", "connectionClosed"}}}},
		ExpectedCase{CaseID: "python-mqtt31-rejected", Protocol: "mqtt31", Profile: "MQTT31_REJECTED", ExpectedResults: []ExpectedResult{{SDK: "python", Role: "client", RequiredObservations: []string{"connackAccepted", "connectionClosed"}}}},
	)
	return cases
}

func oneClientResult(sdk, kind string) []ExpectedResult {
	observations := map[string][]string{
		"basic":      {"qos0RoundTrip", "qos1RoundTrip", "qos2RoundTrip", "disconnectReason"},
		"persistent": {"sessionPresent", "offlineDelivery"},
		"qos1":       {"samePacketIdentifier", "dupObserved", "completion", "subscriberDeliveries", "reconnectAttempt"},
		"qos2":       {"samePacketIdentifier", "dupObserved", "completion", "subscriberDeliveries", "reconnectAttempt"},
		"will311":    {"willReceived", "willClassification"},
		"will5":      {"willReceived", "sessionPresent", "willDelaySeconds"},
	}
	return []ExpectedResult{{SDK: sdk, Role: "client", RequiredObservations: observations[kind]}}
}

func crossResults(publisher, subscriber string) []ExpectedResult {
	return []ExpectedResult{
		{SDK: publisher, Role: "publisher", RequiredObservations: []string{"topic", "payloadHash", "qos", "ackCompleted"}},
		{SDK: subscriber, Role: "subscriber", RequiredObservations: []string{"readinessSignaled", "topic", "payloadHash", "qos", "deliveryCompleted"}},
	}
}

func equalExpectedCase(left, right ExpectedCase) bool {
	if left.CaseID != right.CaseID || left.Protocol != right.Protocol || left.Profile != right.Profile || left.Proxy != right.Proxy || len(left.ExpectedResults) != len(right.ExpectedResults) {
		return false
	}
	for index := range left.ExpectedResults {
		got, want := left.ExpectedResults[index], right.ExpectedResults[index]
		if got.SDK != want.SDK || got.Role != want.Role || !equalStrings(got.RequiredObservations, want.RequiredObservations) {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func expectedCase(profile Profile, caseID string) (ExpectedCase, bool) {
	for _, item := range profile.ExpectedCases {
		if item.CaseID == caseID {
			return item, true
		}
	}
	return ExpectedCase{}, false
}

func expectedSDKVersion(sdk, protocol string) string {
	if sdk == "python" {
		return pythonMQTTVersion
	}
	if protocol == "mqtt5" {
		return goMQTT5Version
	}
	return goMQTT311Version
}

func caseTimeout(profile Profile) time.Duration {
	return time.Duration(profile.OperationTimeoutMillis+profile.ConnectTimeoutMillis)*time.Millisecond +
		time.Duration(profile.WillDelaySeconds)*time.Second + 5*time.Second
}

func validateRunID(runID string) error {
	if !runIDPattern.MatchString(runID) || runID == "." || runID == ".." || strings.ContainsAny(runID, `/\\`) {
		return errors.New("run ID must be a non-empty path-safe identifier")
	}
	return nil
}

func validateOutputDirectory(path string) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("output directory is required")
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(path, 0o755)
	}
	if err != nil {
		return fmt.Errorf("stat output directory: %w", err)
	}
	if !info.IsDir() {
		return errors.New("output path is not a directory")
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read output directory: %w", err)
	}
	if len(entries) != 0 {
		return errors.New("output directory must be empty")
	}
	return nil
}

func pathWithin(root, candidate string) bool {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(rootAbs, candidateAbs)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func sortedCaseIDs(profile Profile) []string {
	ids := make([]string, 0, len(profile.ExpectedCases))
	for _, item := range profile.ExpectedCases {
		ids = append(ids, item.CaseID)
	}
	sort.Strings(ids)
	return ids
}
