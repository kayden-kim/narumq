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

func TestParseCoverageProfileAcceptsCRLF(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "internal")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join("testdata", "coverage", "same-methods.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "sample.go"), source, 0o644); err != nil {
		t.Fatal(err)
	}
	profile, err := os.ReadFile(filepath.Join("testdata", "coverage", "same-methods.out"))
	if err != nil {
		t.Fatal(err)
	}
	normalized := strings.ReplaceAll(string(profile), "\r\n", "\n")
	profilePath := filepath.Join(root, "coverage.out")
	if err := os.WriteFile(profilePath, []byte(strings.ReplaceAll(normalized, "\n", "\r\n")), 0o644); err != nil {
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
		t.Fatalf("CRLF fixture function coverage = %#v, want 1/2 (50%%)", overall.Function)
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

func TestParseCoverageProfileExcludesOnlyGeneratedProtobufSources(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "internal")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join("testdata", "coverage", "same-methods.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "sample.go"), source, 0o644); err != nil {
		t.Fatal(err)
	}
	generated := filepath.Join(sourceDir, "replicated-state.pb.go")
	generatedSource := "// Code generated by protoc-gen-go. DO NOT EDIT.\npackage generated\n"
	if err := os.WriteFile(generated, []byte(generatedSource), 0o644); err != nil {
		t.Fatal(err)
	}
	profile := "mode: atomic\n" +
		"internal/sample.go:6.2,6.7 1 1\n" +
		"internal/sample.go:10.2,10.7 1 0\n" +
		"internal/replicated-state.pb.go:1.1,2.1 1 1\n"
	profilePath := filepath.Join(root, "coverage.out")
	if err := os.WriteFile(profilePath, []byte(profile), 0o644); err != nil {
		t.Fatal(err)
	}
	data, err := parseCoverageProfile(root, profilePath, coveragePolicy{
		ProductionPatterns: []string{"./internal/..."},
	})
	if err != nil {
		t.Fatal(err)
	}
	overall, _ := calculateCoverage(data)
	if overall.Function.Covered != 1 || overall.Function.Total != 2 {
		t.Fatalf("generated source affected function coverage: %#v", overall.Function)
	}

	unknown := filepath.Join(sourceDir, "unknown.pb.go")
	if err := os.WriteFile(unknown, []byte("package unknown\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	profile += "internal/unknown.pb.go:1.1,1.8 1 0\n"
	if err := os.WriteFile(profilePath, []byte(profile), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = parseCoverageProfile(root, profilePath, coveragePolicy{
		ProductionPatterns: []string{"./internal/..."},
	})
	if err == nil || !strings.Contains(err.Error(), "non-production or unknown file internal/unknown.pb.go") {
		t.Fatalf("unknown .pb.go profile entry error = %v", err)
	}
	profile = strings.Replace(profile, "internal/unknown.pb.go:1.1,1.8 1 0\n", "", 1)
	docsDir := filepath.Join(root, "docs")
	if err := os.MkdirAll(docsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(docsDir, "generated.pb.go"), []byte(generatedSource), 0o644); err != nil {
		t.Fatal(err)
	}
	profile += "docs/generated.pb.go:1.1,2.1 1 0\n"
	if err := os.WriteFile(profilePath, []byte(profile), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = parseCoverageProfile(root, profilePath, coveragePolicy{
		ProductionPatterns: []string{"./internal/..."},
	})
	if err == nil || !strings.Contains(err.Error(), "non-production or unknown file docs/generated.pb.go") {
		t.Fatalf("non-production generated profile entry error = %v", err)
	}
}
