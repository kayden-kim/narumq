package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Scenarios: performance-and-verification.md:PRF-SCN-065
func TestVerifyP0RequirementMapping(t *testing.T) {
	basePRD := "# Fixture PRD\n\n" +
		"| ID | 우선순위 | 요구사항 |\n" +
		"| --- | --- | --- |\n" +
		"| MQTT-01 | P0 | MQTT behavior |\n" +
		"| UI-01 | P0 | Dashboard behavior |\n" +
		"| UI-06 | P1 | Client search |\n\n" +
		"| ID | 요구사항 |\n" +
		"| --- | --- |\n" +
		"| TEST-10 | Verification gate |\n\n" +
		"| 요구사항 | 설명 |\n" +
		"| --- | --- |\n" +
		"| UI-01 | A non-requirement cross-reference |\n"
	baseSource := "package narumq\n\n// Traceability: MQTT-01; x\nfunc TestA() {}\n"
	tracking := "docs/TODO.md#fixture"
	newException := func(id, track string) p0Exception {
		return p0Exception{RequirementID: id, Reason: "tracked fixture exception", Tracking: track}
	}

	tests := []struct {
		name       string
		prd        string
		source     string
		exceptions []p0Exception
		want       p0MappingSummary
		wantError  string
	}{
		{
			name:       "valid",
			prd:        basePRD,
			source:     baseSource,
			exceptions: []p0Exception{newException("UI-01", tracking)},
			want:       p0MappingSummary{Total: 2, Mapped: 1, Exceptions: 1},
		},
		{
			name:      "unmapped",
			prd:       basePRD,
			source:    baseSource,
			wantError: "P0 requirement UI-01 has no Traceability test and no exception",
		},
		{
			name:       "stale",
			prd:        basePRD,
			source:     strings.Replace(baseSource, "MQTT-01;", "MQTT-01, UI-01;", 1),
			exceptions: []p0Exception{newException("UI-01", tracking)},
			wantError:  "P0 requirement UI-01 is mapped by 1 test(s)",
		},
		{
			name:       "non-P0",
			prd:        basePRD,
			source:     baseSource,
			exceptions: []p0Exception{newException("UI-01", tracking), newException("UI-06", tracking)},
			wantError:  "exception UI-06 is not a P0 requirement",
		},
		{
			name:       "unknown",
			prd:        basePRD,
			source:     strings.Replace(baseSource, "MQTT-01;", "MQTT-01, CLU-99;", 1),
			exceptions: []p0Exception{newException("UI-01", tracking)},
			wantError:  "unknown PRD requirement CLU-99",
		},
		{
			name:       "unsorted",
			prd:        basePRD,
			source:     baseSource,
			exceptions: []p0Exception{newException("UI-01", tracking), newException("MQTT-01", tracking)},
			wantError:  "exceptions are not strictly sorted",
		},
		{
			name:       "tracking",
			prd:        basePRD,
			source:     baseSource,
			exceptions: []p0Exception{newException("UI-01", "missing.md#fixture")},
			wantError:  "exception UI-01 tracking path missing.md does not exist",
		},
		{
			name:      "PRD duplicate",
			prd:       strings.Replace(basePRD, "| UI-01 | P0 | Dashboard behavior |\n", "| UI-01 | P0 | Dashboard behavior |\n| UI-01 | P0 | Duplicate row |\n", 1),
			source:    baseSource,
			wantError: "PRD requirement UI-01 appears in multiple rows",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			root := t.TempDir()
			prdPath, exceptionsPath := writeP0MappingFixture(t, root, testCase.prd, testCase.source, testCase.exceptions)
			summary, err := verifyP0RequirementMapping(root, prdPath, exceptionsPath)
			if testCase.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
					t.Fatalf("verifyP0RequirementMapping() error = %v, want substring %q", err, testCase.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("verifyP0RequirementMapping() error = %v", err)
			}
			if summary != testCase.want {
				t.Fatalf("summary = %+v, want %+v", summary, testCase.want)
			}
		})
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-066
func TestLoadP0ExceptionsStrictlyValidatesJSON(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "docs", "TODO.md"), []byte("# Fixture tracking\n"))
	tracking := "docs/TODO.md#fixture"
	exception := p0Exception{RequirementID: "UI-01", Reason: "tracked fixture exception", Tracking: tracking}
	encode := func(list p0ExceptionList) []byte {
		t.Helper()
		data, err := json.Marshal(list)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	tests := []struct {
		name      string
		data      []byte
		wantError string
	}{
		{name: "empty", data: encode(p0ExceptionList{SchemaVersion: 1, Exceptions: []p0Exception{}})},
		{name: "unknown field", data: []byte(`{"schemaVersion":1,"exceptions":[],"unexpected":true}`), wantError: `unknown field "unexpected"`},
		{name: "trailing value", data: []byte(`{"schemaVersion":1,"exceptions":[]} {}`), wantError: "trailing JSON value"},
		{name: "schema version", data: encode(p0ExceptionList{SchemaVersion: 2}), wantError: "schemaVersion = 2, want 1"},
		{name: "duplicate", data: encode(p0ExceptionList{SchemaVersion: 1, Exceptions: []p0Exception{exception, exception}}), wantError: "duplicate exception UI-01"},
		{name: "blank reason", data: encode(p0ExceptionList{SchemaVersion: 1, Exceptions: []p0Exception{{RequirementID: "UI-01", Reason: "  ", Tracking: tracking}}}), wantError: "reason must not be empty"},
		{name: "blank tracking", data: encode(p0ExceptionList{SchemaVersion: 1, Exceptions: []p0Exception{{RequirementID: "UI-01", Reason: "reason", Tracking: "  "}}}), wantError: "tracking must not be empty"},
		{name: "parent traversal", data: encode(p0ExceptionList{SchemaVersion: 1, Exceptions: []p0Exception{{RequirementID: "UI-01", Reason: "reason", Tracking: "../docs/TODO.md"}}}), wantError: "must not contain .."},
		{name: "absolute tracking", data: encode(p0ExceptionList{SchemaVersion: 1, Exceptions: []p0Exception{{RequirementID: "UI-01", Reason: "reason", Tracking: filepath.ToSlash(filepath.Join(root, "docs", "TODO.md"))}}}), wantError: "must be repository-relative"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			path := filepath.Join(root, "exceptions.json")
			if err := os.WriteFile(path, testCase.data, 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := loadP0Exceptions(root, path, map[string]struct{}{"UI-01": {}})
			if testCase.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), testCase.wantError) {
					t.Fatalf("loadP0Exceptions() error = %v, want substring %q", err, testCase.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadP0Exceptions() error = %v", err)
			}
			if len(got.Exceptions) != 0 {
				t.Fatalf("exceptions = %v, want empty", got.Exceptions)
			}
		})
	}
}

func writeP0MappingFixture(t *testing.T, root, prd, source string, exceptions []p0Exception) (string, string) {
	t.Helper()
	prdPath := filepath.Join(root, "docs", "requirements", "PRD.md")
	writeFixtureFile(t, prdPath, []byte(prd))
	writeFixtureFile(t, filepath.Join(root, "cmd", "narumq", "a_test.go"), []byte(source))
	writeFixtureFile(t, filepath.Join(root, "docs", "TODO.md"), []byte("# Fixture tracking\n"))
	data, err := json.Marshal(p0ExceptionList{SchemaVersion: 1, Exceptions: exceptions})
	if err != nil {
		t.Fatal(err)
	}
	exceptionsPath := filepath.Join(root, "tests", "conformance", "p0-requirement-exceptions-v1.json")
	writeFixtureFile(t, exceptionsPath, data)
	return prdPath, exceptionsPath
}

func writeFixtureFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-067
func TestParseTraceabilityAcceptsPRDPrefixesAndKeepsStatementRule(t *testing.T) {
	requirements, statements, issues := parseTraceability("DEP-06/DEP-12; MQTT-11; MQTT-1.2.7-1")
	if !reflect.DeepEqual(requirements, []string{"DEP-06", "DEP-12", "MQTT-11"}) {
		t.Fatalf("requirements = %v, want [DEP-06 DEP-12 MQTT-11]", requirements)
	}
	if !reflect.DeepEqual(statements, []string{"MQTT-1.2.7-1"}) {
		t.Fatalf("statements = %v, want [MQTT-1.2.7-1]", statements)
	}
	if len(issues) != 0 {
		t.Fatalf("issues = %v, want none", issues)
	}

	_, _, issues = parseTraceability("DEP-06; MQTT-1.2.7-1")
	if len(issues) == 0 || !strings.Contains(issues[0], "must declare at least one MQTT or TEST requirement ID") {
		t.Fatalf("issues = %v, want missing MQTT or TEST requirement error", issues)
	}
}
