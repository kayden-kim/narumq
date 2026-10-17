package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	coverageCLIRoutingPackage = "narumq/internal/domain/routing"
	coverageCLIRoutingSource  = `package routing

// Classify returns 1 for positive values.
func Classify(value int) int {
	if value > 0 {
		return 1
	}
	return 0
}
`
	coverageCLIUncoveredAddition = `
// Escalate is the changed code under test.
func Escalate(value int) int {
	if value > 0 {
		return 2
	}
	return 3
}
`
)

// coverageCLIFixture is a temporary Git repository that holds one production package and runs the real
// branch-instrument and coverage commands against it.
type coverageCLIFixture struct {
	root, policyPath string
	policy           coveragePolicy
}

func newCoverageCLIFixture(t *testing.T, policy coveragePolicy) coverageCLIFixture {
	t.Helper()
	root := t.TempDir()
	policyData, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	// The policy lives outside the repository like the temporary policy copies in the scenarios.
	policyPath := filepath.Join(t.TempDir(), "coverage-policy.json")
	if err := os.WriteFile(policyPath, policyData, 0o644); err != nil {
		t.Fatal(err)
	}
	writeBranchTestFile(t, root, "go.mod", []byte("module narumq\n\ngo 1.26.5\n"))
	writeBranchTestFile(t, root, ".gitignore", []byte(".artifacts/\n"))
	runBranchTestCommand(t, root, "git", "init", "-q")
	t.Chdir(root)
	return coverageCLIFixture{root: root, policyPath: policyPath, policy: policy}
}

func (fixture coverageCLIFixture) commit(t *testing.T, message string) string {
	t.Helper()
	runBranchTestCommand(t, fixture.root, "git", "add", "-A")
	runBranchTestCommand(t, fixture.root, "git", "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid",
		"-c", "core.autocrlf=false", "commit", "-q", "-m", message)
	command := exec.Command("git", "rev-parse", "HEAD")
	command.Dir = fixture.root
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git rev-parse HEAD: %v", err)
	}
	return strings.TrimSpace(string(output))
}

// measure runs branch-instrument through the CLI and the instrumented copy's tests with an atomic profile.
func (fixture coverageCLIFixture) measure(t *testing.T) string {
	t.Helper()
	if code, stdout, stderr := runCapturingOutput(t, "branch-instrument", "--policy", fixture.policyPath); code != 0 {
		t.Fatalf("branch-instrument exit code = %d; stdout=%q stderr=%q", code, stdout, stderr)
	}
	profile := filepath.Join(fixture.root, ".artifacts", "verification", "coverage.out")
	moduleDir := filepath.Join(fixture.root, filepath.FromSlash(defaultBranchOutputDir), "module")
	runBranchTestCommand(t, moduleDir, "go", "test", "./...", "-count=1", "-covermode=atomic", "-coverpkg=./internal/...", "-coverprofile="+profile)
	return profile
}

func (fixture coverageCLIFixture) readReport(t *testing.T, path string) coverageReport {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("coverage report was not written: %v", err)
	}
	var report coverageReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("decode coverage report: %v", err)
	}
	return report
}

func coverageCLIPolicy(t *testing.T) coveragePolicy {
	t.Helper()
	policy, err := loadCoveragePolicy(filepath.Join("..", "..", defaultCoveragePolicyPath))
	if err != nil {
		t.Fatal(err)
	}
	return coveragePolicy{
		SchemaVersion:      2,
		ProductionPatterns: []string{"./internal/..."},
		DiffFloor:          policy.DiffFloor,
		CorePackages:       []string{coverageCLIRoutingPackage},
		Ratchet: coverageRatchet{
			BranchTolerance: policy.Ratchet.BranchTolerance,
			Core:            map[string]coverageFloor{coverageCLIRoutingPackage: {}},
		},
	}
}

// Traceability: PRD TEST-09; coverage CLI must reject a routing branch ratchet above the measurement.
// Scenarios: performance-and-verification.md:PRF-SCN-084
func TestRunCoverageRejectsRoutingBranchRatchetAboveMeasured(t *testing.T) {
	policy := coverageCLIPolicy(t)
	// Only the routing branch ratchet is above the measured value; every other floor and ratchet stays at zero.
	policy.Ratchet.Core[coverageCLIRoutingPackage] = coverageFloor{Branch: 100}
	fixture := newCoverageCLIFixture(t, policy)
	writeBranchTestFile(t, fixture.root, "internal/domain/routing/routing.go", []byte(coverageCLIRoutingSource))
	writeBranchTestFile(t, fixture.root, "internal/domain/routing/routing_test.go", []byte(
		"package routing\n\nimport \"testing\"\n\nfunc TestClassify(t *testing.T) {\n\tif Classify(1) != 1 {\n\t\tt.Fatal(\"Classify(1)\")\n\t}\n}\n"))
	fixture.commit(t, "base")
	profile := fixture.measure(t)

	reportPath := filepath.Join(t.TempDir(), "summary.json")
	code, stdout, stderr := runCapturingOutput(t, "coverage", "--profile", profile, "--policy", fixture.policyPath, "--report", reportPath)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stdout=%q stderr=%q", code, stdout, stderr)
	}
	want := coverageCLIRoutingPackage + " branch ratchet 100.00% (tolerance 0.20%p) is above measured 50.00%"
	if !strings.Contains(stderr, want) {
		t.Fatalf("stderr = %q, want substring %q", stderr, want)
	}
	if strings.Contains(stderr, "overall ") || strings.Contains(stdout, "coverage: PASS") {
		t.Fatalf("only the routing ratchet may fail; stdout=%q stderr=%q", stdout, stderr)
	}
	if report := fixture.readReport(t, reportPath); report.Passed || report.BranchCoverage.Status != "measured" {
		t.Fatalf("report passed=%t branch status=%q, want a measured failing report", report.Passed, report.BranchCoverage.Status)
	}
}

// Traceability: PRD TEST-08; coverage CLI independently rejects changed line and branch shortfalls.
// Scenarios: performance-and-verification.md:PRF-SCN-085, performance-and-verification.md:PRF-SCN-094
func TestRunCoverageRejectsHistoricalDiffBelowFloor(t *testing.T) {
	fixture := newCoverageCLIFixture(t, coverageCLIPolicy(t))
	writeBranchTestFile(t, fixture.root, "internal/domain/routing/routing.go", []byte(coverageCLIRoutingSource))
	writeBranchTestFile(t, fixture.root, "internal/domain/routing/routing_test.go", []byte(
		"package routing\n\nimport \"testing\"\n\nfunc TestClassify(t *testing.T) {\n\tif Classify(1) != 1 || Classify(-1) != 0 {\n\t\tt.Fatal(\"Classify\")\n\t}\n}\n"))
	base := fixture.commit(t, "base")
	// The later change adds a function and no test for it.
	writeBranchTestFile(t, fixture.root, "internal/domain/routing/routing.go", []byte(coverageCLIRoutingSource+coverageCLIUncoveredAddition))
	fixture.commit(t, "uncovered change")
	profile := fixture.measure(t)

	reportPath := filepath.Join(t.TempDir(), "summary.json")
	code, stdout, stderr := runCapturingOutput(t, "coverage", "--profile", profile, "--policy", fixture.policyPath, "--report", reportPath, "--base", base)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stdout=%q stderr=%q", code, stdout, stderr)
	}
	for _, want := range []string{
		fmt.Sprintf("diff branch coverage 0.00%% is below %.2f%%", fixture.policy.DiffFloor.Branch),
		fmt.Sprintf("diff line coverage 0.00%% is below %.2f%%", fixture.policy.DiffFloor.Line),
	} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr = %q, want substring %q", stderr, want)
		}
	}
	report := fixture.readReport(t, reportPath)
	if report.Passed || report.Diff == nil || report.Diff.Branch.Percent != 0 || report.Diff.Line.Percent != 0 {
		t.Fatalf("report = %+v, want a failing report with 0%% diff branch and line coverage", report)
	}
}

// Traceability: PRD TEST-08; coverage CLI reports passing measured diff coverage at the repository's floor.
// Scenarios: performance-and-verification.md:PRF-SCN-090
func TestRunCoverageAcceptsDiffAtOrAboveFloorWithMeasuredBranches(t *testing.T) {
	repoPolicy, err := loadCoveragePolicy(filepath.Join("..", "..", defaultCoveragePolicyPath))
	if err != nil {
		t.Fatal(err)
	}
	policy := coverageCLIPolicy(t)
	policy.OverallFloor, policy.CoreFloor = repoPolicy.OverallFloor, repoPolicy.CoreFloor
	fixture := newCoverageCLIFixture(t, policy)
	writeBranchTestFile(t, fixture.root, "internal/domain/routing/routing.go", []byte(coverageCLIRoutingSource))
	testSource := func(body string) []byte {
		return []byte("package routing\n\nimport \"testing\"\n\nfunc TestClassify(t *testing.T) {\n" + body + "}\n")
	}
	writeBranchTestFile(t, fixture.root, "internal/domain/routing/routing_test.go", testSource(
		"\tif Classify(1) != 1 || Classify(-1) != 0 {\n\t\tt.Fatal(\"Classify\")\n\t}\n"))
	base := fixture.commit(t, "base")
	// The changed code is fully covered by its test.
	writeBranchTestFile(t, fixture.root, "internal/domain/routing/routing.go", []byte(coverageCLIRoutingSource+coverageCLIUncoveredAddition))
	writeBranchTestFile(t, fixture.root, "internal/domain/routing/routing_test.go", testSource(
		"\tif Classify(1) != 1 || Classify(-1) != 0 || Escalate(1) != 2 || Escalate(-1) != 3 {\n\t\tt.Fatal(\"Classify or Escalate\")\n\t}\n"))
	fixture.commit(t, "covered change")
	profile := fixture.measure(t)

	reportPath := filepath.Join(t.TempDir(), "summary.json")
	code, stdout, stderr := runCapturingOutput(t, "coverage", "--profile", profile, "--policy", fixture.policyPath, "--report", reportPath, "--base", base)
	if code != 0 || !strings.Contains(stdout, "coverage: PASS") {
		t.Fatalf("exit code = %d, want 0 with coverage: PASS; stdout=%q stderr=%q", code, stdout, stderr)
	}
	report := fixture.readReport(t, reportPath)
	if !report.Passed || report.BranchCoverage.Status != "measured" || len(report.BranchCoverage.RequirementShortfalls) != 0 {
		t.Fatalf("report passed=%t branch=%+v, want passed with measured branches and no shortfalls", report.Passed, report.BranchCoverage)
	}
	if report.Diff == nil {
		t.Fatal("report has no diff coverage")
	}
	for name, metric := range map[string]coverageMetric{"line": report.Diff.Line, "function": report.Diff.Function, "branch": report.Diff.Branch} {
		floor := map[string]float64{"line": policy.DiffFloor.Line, "function": policy.DiffFloor.Function, "branch": policy.DiffFloor.Branch}[name]
		if metric.Total == 0 || metric.Percent < floor {
			t.Fatalf("diff %s coverage = %+v, want measured changed code at or above the %.2f%% floor", name, metric, floor)
		}
	}
}
