package main

import "testing"

func passingDrainResult(c DrainExpectedCase, p DrainProfile) Result {
	observations := map[string]any{"vector": c.Vector, "disconnectSurface": "client_error", "serverReferenceAccepted": false, "reconnectEndpoint": p.FakeServer.Endpoint, "permanentEndpoint": p.FakeServer.Endpoint}
	if c.Kind == "cluster_drain" {
		observations = map[string]any{"disconnectReasonCode": float64(156), "serverReference": p.Cluster.ServerReference, "serverReferenceAccepted": true, "reconnectEndpoint": p.Cluster.ServerReference, "reconnectElapsedMillis": float64(100), "sessionPresent": true, "postRedirectDelivery": true, "willReceived": false, "willClassification": "suppressed_reconnected_within_delay", "permanentEndpoint": p.Cluster.CommonEndpoint}
	}
	return Result{SchemaVersion: 1, CaseID: c.CaseID, SDK: c.ExpectedResults[0].SDK, SDKVersion: expectedSDKVersion(c.ExpectedResults[0].SDK, "mqtt5"), Protocol: "mqtt5", Profile: "MQTT5_DRAIN_SAFE", Role: "client", Status: "passed", StartedAt: "2026-09-21T00:00:00Z", FinishedAt: "2026-09-21T00:00:01Z", Observations: observations}
}

func TestValidateDrainResultRejectsContractViolations(t *testing.T) {
	p := canonicalDrainProfile()
	for _, index := range []int{0, 1} {
		c := p.ExpectedCases[index]
		r := passingDrainResult(c, p)
		if err := ValidateDrainCaseResults(p, c, []Result{r}); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name  string
		index int
		field string
		value any
	}{
		{"Will published", 0, "willReceived", true}, {"LB instead of redirect", 0, "reconnectEndpoint", "lb.compat.test:1883"},
		{"late reconnect", 0, "reconnectElapsedMillis", float64(2001)}, {"negative reconnect", 0, "reconnectElapsedMillis", float64(-1)},
		{"no session", 0, "sessionPresent", false}, {"wrong reason", 0, "disconnectReasonCode", float64(142)},
		{"accepted malformed reference", 1, "serverReferenceAccepted", true}, {"trap endpoint", 1, "reconnectEndpoint", "localhost:1886"},
		{"wrong vector", 1, "vector", "other"}, {"invalid surface", 1, "disconnectSurface", "unknown"},
		{"permanent endpoint changed", 0, "permanentEndpoint", "node2.compat.test:1883"}, {"no delivery", 0, "postRedirectDelivery", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := p.ExpectedCases[test.index]
			r := passingDrainResult(c, p)
			r.Observations[test.field] = test.value
			if err := r.ValidateDrain(p, c, c.ExpectedResults[0]); err == nil {
				t.Fatal("accepted contract violation")
			}
		})
	}
	t.Run("missing required observation", func(t *testing.T) {
		c := p.ExpectedCases[0]
		r := passingDrainResult(c, p)
		delete(r.Observations, "willReceived")
		if err := r.ValidateDrain(p, c, c.ExpectedResults[0]); err == nil {
			t.Fatal("accepted missing Will observation")
		}
	})
}

// Scenarios: drain-and-rolling-update.md:DRN-SCN-016
func TestDrainSuiteVerdictsSeparateSDKDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name         string
		kind         string
		failedCase   string
		missingCase  string
		wantStatus   string
		wantExpected int
		wantFailed   int
	}{
		{
			name: "diagnostic failure does not block normal drain",
			kind: "cluster_drain", failedCase: "go-mqtt5-malformed-duplicate-server-reference",
			wantStatus: "passed", wantExpected: 2,
		},
		{
			name: "diagnostic failure remains visible in its own suite",
			kind: "malformed_disconnect", failedCase: "go-mqtt5-malformed-duplicate-server-reference",
			wantStatus: "failed", wantExpected: 8, wantFailed: 1,
		},
		{
			name: "normal drain failure still blocks approval",
			kind: "cluster_drain", failedCase: "go-mqtt5-drain-redirect",
			wantStatus: "failed", wantExpected: 2, wantFailed: 1,
		},
		{
			name: "missing normal SDK result still blocks approval",
			kind: "cluster_drain", missingCase: "python-mqtt5-drain-redirect",
			wantStatus: "failed", wantExpected: 2, wantFailed: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := selectDrainProfile(canonicalDrainProfile(), test.kind)
			results := make(map[string][]Result)
			for _, c := range p.ExpectedCases {
				if c.CaseID == test.missingCase {
					continue
				}
				r := passingDrainResult(c, p)
				if c.CaseID == test.failedCase {
					if c.Kind == "cluster_drain" {
						r.Observations["willReceived"] = true
					} else {
						r.Observations["serverReferenceAccepted"] = true
					}
				}
				results[c.CaseID] = []Result{r}
			}
			summary := AggregateDrainSummary("suite-boundary", p, results, nil, "", "")
			if summary.Status != test.wantStatus || summary.ExpectedCases != test.wantExpected || summary.FailedCases != test.wantFailed {
				t.Fatalf("unexpected suite verdict: %+v", summary)
			}
			for _, item := range summary.CaseSummaries {
				if item.CaseID == test.failedCase && item.Status != "failed" {
					t.Fatalf("failure was hidden: %+v", item)
				}
			}
		})
	}
}
