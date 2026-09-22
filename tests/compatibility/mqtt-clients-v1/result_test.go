package main

import (
	"strings"
	"testing"
)

func passingResult(expected ExpectedCase, role ExpectedResult) Result {
	observations := make(map[string]any, len(role.RequiredObservations))
	for _, name := range role.RequiredObservations {
		switch name {
		case "subscriberDeliveries", "reconnectAttempt", "qos", "willDelaySeconds":
			observations[name] = float64(1)
		case "topic":
			observations[name] = expectedTopic(expected.CaseID)
		case "payloadHash":
			observations[name] = expectedPayloadHash(expected.CaseID)
		case "disconnectReason":
			observations[name] = "value"
		case "willClassification":
			observations[name] = "expected_mqtt311_server_disconnect"
		case "willReceived":
			observations[name] = !strings.Contains(expected.CaseID, "will-delay")
		default:
			observations[name] = true
		}
	}
	if observations["willDelaySeconds"] != nil {
		observations["willDelaySeconds"] = float64(5)
	}
	return Result{
		SchemaVersion: resultSchemaVersion, CaseID: expected.CaseID, SDK: role.SDK,
		SDKVersion: expectedSDKVersion(role.SDK, expected.Protocol), Protocol: expected.Protocol,
		Profile: expected.Profile, Role: role.Role, Status: "passed", StartedAt: "2026-09-21T00:00:00Z",
		FinishedAt: "2026-09-21T00:00:01Z", Observations: observations,
	}
}

func TestResultValidationRejectsFalseRequiredObservation(t *testing.T) {
	profile, _, err := LoadProfile("profile.json")
	if err != nil {
		t.Fatal(err)
	}
	expected := profile.ExpectedCases[2]
	role := expected.ExpectedResults[0]
	result := passingResult(expected, role)
	result.Observations["dupObserved"] = false
	if err := result.Validate(expected, role); err == nil {
		t.Fatal("result with false DUP observation passed validation")
	}
}

func TestValidateCaseResultsRejectsDuplicateAndMissingRoles(t *testing.T) {
	profile, _, err := LoadProfile("profile.json")
	if err != nil {
		t.Fatal(err)
	}
	expected := profile.ExpectedCases[20]
	publisher := passingResult(expected, expected.ExpectedResults[0])
	if err := ValidateCaseResults(expected, []Result{publisher, publisher}); err == nil {
		t.Fatal("duplicate role passed case validation")
	}
	if err := ValidateCaseResults(expected, []Result{publisher}); err == nil {
		t.Fatal("missing role passed case validation")
	}
}
func TestResultValidationRequiresFailedErrorAndObservations(t *testing.T) {
	profile, _, err := LoadProfile("profile.json")
	if err != nil {
		t.Fatal(err)
	}
	expected := profile.ExpectedCases[0]
	role := expected.ExpectedResults[0]
	result := passingResult(expected, role)
	result.Status = "failed"
	result.Error = ""
	if err := result.Validate(expected, role); err == nil {
		t.Fatal("failed result without error passed validation")
	}
	result.Error = "timeout"
	result.Observations = nil
	if err := result.Validate(expected, role); err == nil {
		t.Fatal("failed result with nil observations passed validation")
	}
}

func TestAggregateSummaryCountsMissingCasesAsFailures(t *testing.T) {
	profile, _, err := LoadProfile("profile.json")
	if err != nil {
		t.Fatal(err)
	}
	first := profile.ExpectedCases[0]
	role := first.ExpectedResults[0]
	summary := AggregateSummary("test-run", profile, map[string][]Result{
		first.CaseID: {passingResult(first, role)},
	}, nil, "2026-09-21T00:00:00Z", "2026-09-21T00:00:01Z")
	if summary.Status != "failed" || summary.PassedCases != 1 || summary.FailedCases != 25 {
		t.Fatalf("summary = %+v", summary)
	}
	if len(summary.MissingCases) != 25 {
		t.Fatalf("missing case count = %d, want 25", len(summary.MissingCases))
	}
}
