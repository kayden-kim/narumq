package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseCoverageProfileRejectsMalformedFixture(t *testing.T) {
	_, err := parseCoverageProfile(".", filepath.Join("testdata", "coverage", "malformed-profile.out"), coveragePolicy{
		ProductionPatterns: []string{"./internal/..."},
	})
	if err == nil || !strings.Contains(err.Error(), "must start with mode: atomic") {
		t.Fatalf("parseCoverageProfile() error = %v, want atomic mode error", err)
	}
}

func TestCoverageRatchetRejectsMeasuredValueBelowBaseline(t *testing.T) {
	policy, err := loadCoveragePolicy(filepath.Join("testdata", "coverage", "lower-ratchet.json"))
	if err != nil {
		t.Fatal(err)
	}
	issues := enforceCoverageFloor("overall", packageCoverage{
		Line:     coverageMetric{Covered: 95, Total: 100, Percent: 95},
		Function: coverageMetric{Covered: 95, Total: 100, Percent: 95},
	}, policy.OverallFloor, policy.Ratchet.Overall)
	if len(issues) != 2 || !strings.Contains(strings.Join(issues, "\n"), "ratchet") {
		t.Fatalf("enforceCoverageFloor() = %v, want two ratchet errors", issues)
	}
	if issues := enforceCoverageFloor("overall", packageCoverage{
		Line:     coverageMetric{Covered: 97, Total: 100, Percent: 97},
		Function: coverageMetric{Covered: 97, Total: 100, Percent: 97},
	}, policy.OverallFloor, policy.Ratchet.Overall); len(issues) != 0 {
		t.Fatalf("enforceCoverageFloor() rejected measured improvement: %v", issues)
	}
}

func TestFunctionCoverageSeparatesSameNamedMethodsFixture(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "internal")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join("testdata", "coverage", "same-methods.go"))
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(sourceDir, "sample.go")
	if err := os.WriteFile(sourcePath, source, 0o644); err != nil {
		t.Fatal(err)
	}
	profile, err := os.ReadFile(filepath.Join("testdata", "coverage", "same-methods.out"))
	if err != nil {
		t.Fatal(err)
	}
	profilePath := filepath.Join(root, "coverage.out")
	if err := os.WriteFile(profilePath, profile, 0o644); err != nil {
		t.Fatal(err)
	}
	data, err := parseCoverageProfile(root, profilePath, coveragePolicy{
		ProductionPatterns: []string{"./internal/..."},
	})
	if err != nil {
		t.Fatal(err)
	}
	overall, _ := calculateCoverage(data)
	if overall.Function.Covered != 1 || overall.Function.Total != 2 || overall.Function.Percent != 50 {
		t.Fatalf("same-method fixture function coverage = %#v, want 1/2", overall.Function)
	}
}

func TestParseChangedLinesTracksOnlyAddedLines(t *testing.T) {
	diff := "diff --git a/internal/domain/mqtt/decoder.go b/internal/domain/mqtt/decoder.go\n" +
		"+++ b/internal/domain/mqtt/decoder.go\n" +
		"@@ -2,2 +2,4 @@\n" +
		"-removed\n" +
		" context\n" +
		"+added one\n" +
		"+added two\n"
	changed := parseChangedLines(diff)
	lines := changed["internal/domain/mqtt/decoder.go"]
	if len(lines) != 2 {
		t.Fatalf("changed lines = %v, want two additions", lines)
	}
	if _, ok := lines[3]; !ok {
		t.Fatalf("changed lines = %v, missing line 3", lines)
	}
	if _, ok := lines[4]; !ok {
		t.Fatalf("changed lines = %v, missing line 4", lines)
	}
}

func TestDiffMetricTreatsNoExecutableChangesAsFullCoverage(t *testing.T) {
	metric := diffMetric(nil)
	if metric.Total != 0 || metric.Covered != 0 || metric.Percent != 100 {
		t.Fatalf("diffMetric(nil) = %#v, want empty 100%% metric", metric)
	}
	metric = diffMetric([]bool{true, false})
	if metric.Covered != 1 || metric.Total != 2 || metric.Percent != 50 {
		t.Fatalf("diffMetric values = %#v, want 1/2", metric)
	}
}

func TestCoverageReportWritesBranchStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "summary.json")
	report := coverageReport{
		SchemaVersion: 1,
		Profile:       "coverage.out",
		BranchCoverage: branchCoverageStatus{
			Status: "not_evaluated",
			Reason: "approved branch instrumentation tool is not configured",
		},
	}
	if err := writeCoverageReport(path, report); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"status": "not_evaluated"`) {
		t.Fatalf("coverage report = %s, missing branch status", data)
	}
}
