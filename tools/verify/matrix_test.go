package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadComplianceMatrixFixtures(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "unknown-field", want: "unknown field"},
		{name: "duplicate-statement", want: "duplicate statement ID"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := loadComplianceMatrix(filepath.Join("testdata", "traceability", testCase.name+".json"))
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("loadComplianceMatrix() error = %v, want substring %q", err, testCase.want)
			}
		})
	}
	if _, err := loadComplianceMatrix(filepath.Join("testdata", "traceability", "valid.json")); err != nil {
		t.Fatalf("valid matrix rejected: %v", err)
	}
}

func TestParseTraceabilitySlashExpansionFixture(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "traceability", "slash-expansion.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Trace        string   `json:"trace"`
		Requirements []string `json:"requirements"`
		Statements   []string `json:"statements"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	requirements, statements, issues := parseTraceability(fixture.Trace)
	if len(issues) != 0 {
		t.Fatalf("parseTraceability() issues = %v", issues)
	}
	if !reflect.DeepEqual(requirements, fixture.Requirements) {
		t.Fatalf("requirements = %v, want %v", requirements, fixture.Requirements)
	}
	if !reflect.DeepEqual(statements, fixture.Statements) {
		t.Fatalf("statements = %v, want %v", statements, fixture.Statements)
	}
}

// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-001
func TestVerifyTraceabilityRejectsMissingAndStaleReverseReferences(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "internal", "domain", "mqtt", "example_test.go")
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatal(err)
	}
	source := "package mqtt\n\n// Traceability: PRD MQTT-11/TEST-04; MQTT-1.2.7-1\nfunc TestExample() {}\n"
	if err := os.WriteFile(sourcePath, []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, fixtureName := range []string{"missing-test", "stale-reverse"} {
		t.Run(fixtureName, func(t *testing.T) {
			matrixPath := filepath.Join(root, fixtureName+".json")
			fixturePath := filepath.Join("testdata", "traceability", fixtureName+".json")
			data, err := os.ReadFile(fixturePath)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(matrixPath, data, 0o644); err != nil {
				t.Fatal(err)
			}
			err = verifyTraceability(root, matrixPath)
			if err == nil {
				t.Fatal("verifyTraceability() unexpectedly succeeded")
			}
			if !strings.Contains(err.Error(), "missing test") {
				t.Fatalf("error = %v, want missing test reference", err)
			}
		})
	}
}
