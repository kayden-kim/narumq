package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"
)

const resultSchemaVersion = 1

// Result is the SDK-neutral compatibility result exchanged with the runner.
type Result struct {
	SchemaVersion int            `json:"schemaVersion"`
	CaseID        string         `json:"caseId"`
	SDK           string         `json:"sdk"`
	SDKVersion    string         `json:"sdkVersion"`
	Protocol      string         `json:"protocol"`
	Profile       string         `json:"profile"`
	Role          string         `json:"role"`
	Status        string         `json:"status"`
	StartedAt     string         `json:"startedAt"`
	FinishedAt    string         `json:"finishedAt"`
	Observations  map[string]any `json:"observations"`
	Error         string         `json:"error,omitempty"`
}

// CaseSummary records the result status of one expected case.
type CaseSummary struct {
	CaseID      string `json:"caseId"`
	Status      string `json:"status"`
	ResultCount int    `json:"resultCount"`
	Error       string `json:"error,omitempty"`
}

// Summary is the durable run-level compatibility verdict.
type Summary struct {
	SchemaVersion   int           `json:"schemaVersion"`
	RunID           string        `json:"runId"`
	Status          string        `json:"status"`
	ExpectedCases   int           `json:"expectedCases"`
	PassedCases     int           `json:"passedCases"`
	FailedCases     int           `json:"failedCases"`
	MissingCases    []string      `json:"missingCases"`
	UnexpectedCases []string      `json:"unexpectedCases"`
	CaseSummaries   []CaseSummary `json:"cases"`
	StartedAt       string        `json:"startedAt"`
	FinishedAt      string        `json:"finishedAt"`
	Error           string        `json:"error,omitempty"`
}

// DecodeResult strictly decodes one result value and rejects trailing data.
func DecodeResult(data []byte) (Result, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var result Result
	if err := decoder.Decode(&result); err != nil {
		return Result{}, fmt.Errorf("decode result: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Result{}, errors.New("result contains a second JSON value")
		}
		return Result{}, fmt.Errorf("decode trailing result data: %w", err)
	}
	return result, nil
}

// Validate checks one result against the expected role and case contract.
func (r Result) Validate(expected ExpectedCase, expectedRole ExpectedResult) error {
	if err := r.validateEnvelope(expected, expectedRole); err != nil {
		return err
	}
	if r.Status == "passed" {
		for _, observation := range expectedRole.RequiredObservations {
			value, ok := r.Observations[observation]
			if !ok {
				return fmt.Errorf("result %s is missing observation %q", r.CaseID, observation)
			}
			if err := validateObservation(r.CaseID, observation, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r Result) validateEnvelope(expected ExpectedCase, expectedRole ExpectedResult) error {
	if r.SchemaVersion != resultSchemaVersion {
		return fmt.Errorf("result %s has unsupported schema version %d", r.CaseID, r.SchemaVersion)
	}
	if r.CaseID != expected.CaseID {
		return fmt.Errorf("result caseId %q does not match %q", r.CaseID, expected.CaseID)
	}
	if r.SDK != expectedRole.SDK || r.Role != expectedRole.Role {
		return fmt.Errorf("result identity is sdk=%q role=%q, want sdk=%q role=%q", r.SDK, r.Role, expectedRole.SDK, expectedRole.Role)
	}
	if r.SDKVersion != expectedSDKVersion(r.SDK, expected.Protocol) {
		return fmt.Errorf("result sdkVersion %q does not match expected %q", r.SDKVersion, expectedSDKVersion(r.SDK, expected.Protocol))
	}
	if r.Protocol != expected.Protocol || r.Profile != expected.Profile {
		return fmt.Errorf("result protocol/profile %q/%q does not match %q/%q", r.Protocol, r.Profile, expected.Protocol, expected.Profile)
	}
	if r.Status != "passed" && r.Status != "failed" {
		return fmt.Errorf("result %s has unsupported status %q", r.CaseID, r.Status)
	}
	if strings.TrimSpace(r.StartedAt) == "" || strings.TrimSpace(r.FinishedAt) == "" {
		return fmt.Errorf("result %s is missing timestamps", r.CaseID)
	}
	started, err := time.Parse(time.RFC3339Nano, r.StartedAt)
	if err != nil {
		return fmt.Errorf("result %s has invalid startedAt: %w", r.CaseID, err)
	}
	finished, err := time.Parse(time.RFC3339Nano, r.FinishedAt)
	if err != nil {
		return fmt.Errorf("result %s has invalid finishedAt: %w", r.CaseID, err)
	}
	if finished.Before(started) {
		return fmt.Errorf("result %s finishes before it starts", r.CaseID)
	}
	if r.Observations == nil {
		return fmt.Errorf("result %s has nil observations", r.CaseID)
	}
	if r.Status == "passed" && strings.TrimSpace(r.Error) != "" {
		return fmt.Errorf("passed result %s contains error", r.CaseID)
	}
	if r.Status == "failed" && strings.TrimSpace(r.Error) == "" {
		return fmt.Errorf("failed result %s is missing error", r.CaseID)
	}
	return nil
}

func validateObservation(caseID, name string, value any) error {
	switch name {
	case "sessionPresent", "offlineDelivery", "qos0RoundTrip", "qos1RoundTrip", "qos2RoundTrip", "ackCompleted", "readinessSignaled", "deliveryCompleted", "samePacketIdentifier", "dupObserved", "completion", "connectionClosed":
		if boolean, ok := value.(bool); !ok || !boolean {
			return fmt.Errorf("result %s observation %q is not true", caseID, name)
		}
	case "willReceived":
		boolean, ok := value.(bool)
		if !ok {
			return fmt.Errorf("result %s observation %q is not boolean", caseID, name)
		}
		if strings.Contains(caseID, "will-delay") && boolean {
			return fmt.Errorf("result %s observation willReceived must be false", caseID)
		}
		if strings.Contains(caseID, "abrupt-will") && !boolean {
			return fmt.Errorf("result %s observation willReceived must be true", caseID)
		}
	case "connackAccepted":
		if boolean, ok := value.(bool); !ok || boolean {
			return fmt.Errorf("result %s observation connackAccepted must be false", caseID)
		}
	case "subscriberDeliveries":
		number, ok := numericObservation(value)
		if !ok || number < 1 {
			return fmt.Errorf("result %s observation subscriberDeliveries must be at least one", caseID)
		}
	case "reconnectAttempt":
		number, ok := numericObservation(value)
		if !ok || number < 1 {
			return fmt.Errorf("result %s observation reconnectAttempt must be positive", caseID)
		}
	case "qos":
		number, ok := numericObservation(value)
		if !ok || number != 1 {
			return fmt.Errorf("result %s observation qos must equal one", caseID)
		}
	case "willDelaySeconds":
		number, ok := numericObservation(value)
		if !ok || number != 5 {
			return fmt.Errorf("result %s observation willDelaySeconds must equal five", caseID)
		}
	case "topic", "payloadHash", "disconnectReason", "willClassification":
		text, ok := value.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return fmt.Errorf("result %s observation %q must be non-empty text", caseID, name)
		}
		switch name {
		case "topic":
			if text != expectedTopic(caseID) {
				return fmt.Errorf("result %s observation topic %q does not match expected topic", caseID, text)
			}
		case "payloadHash":
			if text != expectedPayloadHash(caseID) {
				return fmt.Errorf("result %s observation payloadHash does not match expected fixture", caseID)
			}
		case "willClassification":
			if text != "expected_mqtt311_server_disconnect" {
				return fmt.Errorf("result %s observation willClassification is %q", caseID, text)
			}
		}
	default:
		return fmt.Errorf("result %s has unsupported required observation %q", caseID, name)
	}
	return nil
}
func expectedTopic(caseID string) string {
	caseToken := sha256.Sum256([]byte("narumq-compatibility/" + caseID))
	token := hex.EncodeToString(caseToken[:])
	return "compat/" + token[:12] + "/message"
}

func expectedPayloadHash(caseID string) string {
	caseToken := sha256.Sum256([]byte("narumq-compatibility/" + caseID))
	token := hex.EncodeToString(caseToken[:])
	payload := []byte("mqtt-client-compatibility/" + token[:24])
	payloadHash := sha256.Sum256(payload)
	return hex.EncodeToString(payloadHash[:])
}

func numericObservation(value any) (float64, bool) {
	number, ok := value.(float64)
	return number, ok
}

// ValidateCaseResults validates all role results for one expected case.
func ValidateCaseResults(expected ExpectedCase, results []Result) error {
	return validateRoleResults(expected.CaseID, expected.ExpectedResults, results, func(r Result, role ExpectedResult) error {
		return r.Validate(expected, role)
	})
}

func validateRoleResults(caseID string, roles []ExpectedResult, results []Result, validate func(Result, ExpectedResult) error) error {
	if len(results) != len(roles) {
		return fmt.Errorf("case %s has %d results, want %d", caseID, len(results), len(roles))
	}
	expectedByRole := make(map[string]ExpectedResult, len(roles))
	for _, role := range roles {
		expectedByRole[role.Role] = role
	}
	seenRoles := make(map[string]struct{}, len(results))
	for _, result := range results {
		if _, ok := seenRoles[result.Role]; ok {
			return fmt.Errorf("case %s has duplicate role %q", caseID, result.Role)
		}
		role, ok := expectedByRole[result.Role]
		if !ok {
			return fmt.Errorf("case %s has unexpected role %q", caseID, result.Role)
		}
		seenRoles[result.Role] = struct{}{}
		if err := validate(result, role); err != nil {
			return err
		}
		if result.Status != "passed" {
			return fmt.Errorf("case %s role %q failed: %s", caseID, result.Role, result.Error)
		}
	}
	for role := range expectedByRole {
		if _, ok := seenRoles[role]; !ok {
			return fmt.Errorf("case %s is missing role %q", caseID, role)
		}
	}
	return nil
}

// AggregateSummary creates the run verdict while preserving all case failures.
func AggregateSummary(runID string, profile Profile, results map[string][]Result, unexpected []string, startedAt, finishedAt string) Summary {
	ids := make([]string, 0, len(profile.ExpectedCases))
	expected := make(map[string]ExpectedCase, len(profile.ExpectedCases))
	for _, c := range profile.ExpectedCases {
		ids = append(ids, c.CaseID)
		expected[c.CaseID] = c
	}
	return aggregateSummary(runID, ids, results, unexpected, startedAt, finishedAt, func(id string, results []Result) error {
		return ValidateCaseResults(expected[id], results)
	})
}

func aggregateSummary(runID string, caseIDs []string, results map[string][]Result, unexpected []string, startedAt, finishedAt string, validate func(string, []Result) error) Summary {
	summary := Summary{
		SchemaVersion:   resultSchemaVersion,
		RunID:           runID,
		Status:          "failed",
		ExpectedCases:   len(caseIDs),
		MissingCases:    make([]string, 0),
		UnexpectedCases: append([]string(nil), unexpected...),
		CaseSummaries:   make([]CaseSummary, 0, len(caseIDs)),
		StartedAt:       startedAt,
		FinishedAt:      finishedAt,
	}
	for _, caseID := range caseIDs {
		caseResults, ok := results[caseID]
		if !ok {
			summary.MissingCases = append(summary.MissingCases, caseID)
			summary.FailedCases++
			summary.CaseSummaries = append(summary.CaseSummaries, CaseSummary{CaseID: caseID, Status: "missing"})
			continue
		}
		caseSummary := CaseSummary{CaseID: caseID, ResultCount: len(caseResults), Status: "failed"}
		if err := validate(caseID, caseResults); err != nil {
			caseSummary.Error = err.Error()
		} else {
			caseSummary.Status = "passed"
			summary.PassedCases++
		}
		if caseSummary.Status != "passed" {
			summary.FailedCases++
		}
		summary.CaseSummaries = append(summary.CaseSummaries, caseSummary)
	}
	if len(summary.MissingCases) > 0 || len(summary.UnexpectedCases) > 0 || summary.PassedCases != summary.ExpectedCases {
		summary.Error = "one or more compatibility cases failed validation"
	} else {
		summary.Status = "passed"
	}
	sort.Strings(summary.MissingCases)
	sort.Strings(summary.UnexpectedCases)
	sort.Slice(summary.CaseSummaries, func(i, j int) bool { return summary.CaseSummaries[i].CaseID < summary.CaseSummaries[j].CaseID })
	return summary
}

// ValidateDrain checks the SDK envelope and the drain-specific observations.
func (r Result) ValidateDrain(p DrainProfile, c DrainExpectedCase, role ExpectedResult) error {
	if err := r.validateEnvelope(c.asExpectedCase(), role); err != nil {
		return err
	}
	if r.Status != "passed" {
		return nil
	}
	for _, name := range role.RequiredObservations {
		if _, ok := r.Observations[name]; !ok {
			return fmt.Errorf("result %s is missing observation %q", r.CaseID, name)
		}
	}
	exact := map[string]any{"permanentEndpoint": p.FakeServer.Endpoint, "reconnectEndpoint": p.FakeServer.Endpoint, "serverReferenceAccepted": false, "vector": c.Vector}
	switch c.Kind {
	case "cluster_drain":
		exact = map[string]any{"disconnectReasonCode": float64(156), "serverReference": p.Cluster.ServerReference, "serverReferenceAccepted": true, "reconnectEndpoint": p.Cluster.ServerReference, "sessionPresent": true, "postRedirectDelivery": true, "willReceived": false, "willClassification": "suppressed_reconnected_within_delay", "permanentEndpoint": p.Cluster.CommonEndpoint}
		elapsed, ok := numericObservation(r.Observations["reconnectElapsedMillis"])
		if !ok || !(elapsed >= 0 && elapsed <= float64(p.ReconnectWithinMillis)) {
			return fmt.Errorf("result %s reconnectElapsedMillis exceeds drain bound", r.CaseID)
		}
	case "malformed_disconnect":
		switch r.Observations["disconnectSurface"] {
		case "server_disconnect", "client_error", "sdk_exception", "connection_lost":
		default:
			return fmt.Errorf("result %s has invalid disconnectSurface", r.CaseID)
		}
	default:
		return fmt.Errorf("unsupported drain kind %q", c.Kind)
	}
	for name, want := range exact {
		got := r.Observations[name]
		// DecodeResult returns only scalar values for these observations.
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("result %s observation %s = %v, want %v", r.CaseID, name, got, want)
		}
	}
	return nil
}

// ValidateDrainCaseResults requires exactly one passing result per drain role.
func ValidateDrainCaseResults(p DrainProfile, c DrainExpectedCase, results []Result) error {
	return validateRoleResults(c.CaseID, c.ExpectedResults, results, func(r Result, role ExpectedResult) error { return r.ValidateDrain(p, c, role) })
}

// AggregateDrainSummary preserves every drain case verdict.
func AggregateDrainSummary(runID string, p DrainProfile, results map[string][]Result, unexpected []string, startedAt, finishedAt string) Summary {
	ids := make([]string, 0, len(p.ExpectedCases))
	expected := make(map[string]DrainExpectedCase, len(p.ExpectedCases))
	for _, c := range p.ExpectedCases {
		ids = append(ids, c.CaseID)
		expected[c.CaseID] = c
	}
	return aggregateSummary(runID, ids, results, unexpected, startedAt, finishedAt, func(id string, results []Result) error { return ValidateDrainCaseResults(p, expected[id], results) })
}
