package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	if len(results) != len(expected.ExpectedResults) {
		return fmt.Errorf("case %s has %d results, want %d", expected.CaseID, len(results), len(expected.ExpectedResults))
	}
	expectedByRole := make(map[string]ExpectedResult, len(expected.ExpectedResults))
	for _, role := range expected.ExpectedResults {
		expectedByRole[role.Role] = role
	}
	seenRoles := make(map[string]struct{}, len(results))
	for _, result := range results {
		if _, ok := seenRoles[result.Role]; ok {
			return fmt.Errorf("case %s has duplicate role %q", expected.CaseID, result.Role)
		}
		role, ok := expectedByRole[result.Role]
		if !ok {
			return fmt.Errorf("case %s has unexpected role %q", expected.CaseID, result.Role)
		}
		seenRoles[result.Role] = struct{}{}
		if err := result.Validate(expected, role); err != nil {
			return err
		}
		if result.Status != "passed" {
			return fmt.Errorf("case %s role %q failed: %s", expected.CaseID, result.Role, result.Error)
		}
	}
	for role := range expectedByRole {
		if _, ok := seenRoles[role]; !ok {
			return fmt.Errorf("case %s is missing role %q", expected.CaseID, role)
		}
	}
	return nil
}

// AggregateSummary creates the run verdict while preserving all case failures.
func AggregateSummary(runID string, profile Profile, results map[string][]Result, unexpected []string, startedAt, finishedAt string) Summary {
	summary := Summary{
		SchemaVersion:   resultSchemaVersion,
		RunID:           runID,
		Status:          "failed",
		ExpectedCases:   len(profile.ExpectedCases),
		MissingCases:    make([]string, 0),
		UnexpectedCases: append([]string(nil), unexpected...),
		CaseSummaries:   make([]CaseSummary, 0, len(profile.ExpectedCases)),
		StartedAt:       startedAt,
		FinishedAt:      finishedAt,
	}
	for _, expected := range profile.ExpectedCases {
		caseResults, ok := results[expected.CaseID]
		if !ok {
			summary.MissingCases = append(summary.MissingCases, expected.CaseID)
			summary.FailedCases++
			summary.CaseSummaries = append(summary.CaseSummaries, CaseSummary{CaseID: expected.CaseID, Status: "missing"})
			continue
		}
		caseSummary := CaseSummary{CaseID: expected.CaseID, ResultCount: len(caseResults), Status: "failed"}
		if err := ValidateCaseResults(expected, caseResults); err != nil {
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
