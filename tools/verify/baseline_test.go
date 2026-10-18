package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFloorBranchPercentFloorsExactFractionsToTwoDecimals(t *testing.T) {
	cases := []struct {
		covered, total int
		want           float64
	}{
		{10830, 13073, 82.84},
		{1, 3, 33.33},
		{2, 3, 66.66},
		{5, 5, 100},
		{0, 7, 0},
		{3, 0, 0},
	}
	for _, test := range cases {
		if got := floorBranchPercent(test.covered, test.total); got != test.want {
			t.Errorf("floorBranchPercent(%d, %d) = %v, want %v", test.covered, test.total, got, test.want)
		}
	}
}

func TestDeriveBranchBaselineTakesSmallerOfTwoMeasurementsPerMetric(t *testing.T) {
	first := branchMeasurement{Overall: coverageMetric{Covered: 10830, Total: 13073}, Core: map[string]coverageMetric{"a": {Covered: 9, Total: 10}, "b": {Covered: 2, Total: 3}}}
	second := branchMeasurement{Overall: coverageMetric{Covered: 10835, Total: 13073}, Core: map[string]coverageMetric{"a": {Covered: 8, Total: 10}, "b": {Covered: 3, Total: 3}}}
	got := deriveBranchBaseline(first, second)
	if got.Overall != 82.84 || got.Core["a"] != 80 || got.Core["b"] != 66.66 {
		t.Fatalf("deriveBranchBaseline() = %+v, want overall 82.84, a 80, b 66.66", got)
	}
}

// baselineFixture drives runBranchBaseline with a fake command runner. summary renders the coverage summary
// and log the stdout of run number run (1-based). failRun makes that run exit non-zero; zero disables it.
type baselineFixture struct {
	root    string
	policy  coveragePolicy
	runs    int
	summary func(run int, policy coveragePolicy) coverageReport
	log     func(run int) string
	failRun int
}

func newBaselineFixture(t *testing.T) *baselineFixture {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module "+modulePath+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	policyData, err := os.ReadFile(filepath.Join("..", "..", filepath.FromSlash(defaultCoveragePolicyPath)))
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(root, filepath.FromSlash(defaultCoveragePolicyPath))
	if err := os.MkdirAll(filepath.Dir(policyPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policyPath, policyData, 0o644); err != nil {
		t.Fatal(err)
	}
	policy, err := loadCoveragePolicy(policyPath)
	if err != nil {
		t.Fatal(err)
	}
	return &baselineFixture{
		root:   root,
		policy: policy,
		summary: func(run int, policy coveragePolicy) coverageReport {
			report := coverageReport{SchemaVersion: 2, Passed: true, Overall: packageCoverage{Branch: coverageMetric{Covered: 800 + run, Total: 1000}}, Core: map[string]packageCoverage{}, BranchCoverage: branchCoverageStatus{Status: "measured"}}
			for _, name := range policy.CorePackages {
				report.Core[name] = packageCoverage{Branch: coverageMetric{Covered: 90 + run, Total: 100}}
			}
			return report
		},
		log: func(int) string { return "scenarios: PASS\nlocal: PASS\n" },
	}
}

func (fixture *baselineFixture) runner(t *testing.T) commandRunner {
	return func(_ context.Context, root string, spec commandSpec) error {
		fixture.runs++
		summaryPath := filepath.Join(root, filepath.FromSlash(coverageSummaryPath))
		if _, err := os.Stat(summaryPath); err == nil {
			t.Errorf("run %d started with a stale coverage summary", fixture.runs)
		}
		if err := os.MkdirAll(filepath.Join(root, ".artifacts", "verification"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(spec.StdoutFile)), []byte(fixture.log(fixture.runs)), 0o644); err != nil {
			t.Fatal(err)
		}
		if fixture.runs == fixture.failRun {
			return commandFailure{Spec: spec, Code: 1, Err: errors.New("exit status 1")}
		}
		data, err := json.Marshal(fixture.summary(fixture.runs, fixture.policy))
		if err != nil {
			t.Fatal(err)
		}
		return os.WriteFile(summaryPath, data, 0o644)
	}
}

func TestRunBranchBaselineRunsLocalThreeTimesAndDerivesBaselineFromFirstTwo(t *testing.T) {
	fixture := newBaselineFixture(t)
	var out bytes.Buffer
	report, err := runBranchBaseline(context.Background(), fixture.root, fixture.runner(t), &out)
	if err != nil {
		t.Fatalf("runBranchBaseline() = %v\n%s", err, out.String())
	}
	if fixture.runs != 3 || len(report.Runs) != 3 {
		t.Fatalf("runs = %d, recorded = %d, want 3 and 3", fixture.runs, len(report.Runs))
	}
	// Overall branch 801/1000 and 802/1000: the smaller of the first two is 80.1. The third run (803) is not used.
	if report.Baseline.Overall != 80.1 {
		t.Fatalf("overall baseline = %v, want 80.1", report.Baseline.Overall)
	}
	for _, name := range fixture.policy.CorePackages {
		if report.Baseline.Core[name] != 91 {
			t.Fatalf("core baseline %s = %v, want 91", name, report.Baseline.Core[name])
		}
	}
	written, err := os.ReadFile(filepath.Join(fixture.root, filepath.FromSlash(defaultBranchBaselinePath)))
	if err != nil {
		t.Fatal(err)
	}
	var decoded branchBaselineReport
	if err := json.Unmarshal(written, &decoded); err != nil || decoded.Baseline.Overall != 80.1 || len(decoded.Runs) != 3 {
		t.Fatalf("written baseline = %s (%v), want overall 80.1 and 3 runs", written, err)
	}
}

func TestRunBranchBaselineRejectsFailedOrUnprovenRuns(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*baselineFixture)
		wantErr  string
		wantRuns int
	}{
		{"second local run fails", func(f *baselineFixture) { f.failRun = 2 }, "baseline run 2", 2},
		{"third local run fails", func(f *baselineFixture) { f.failRun = 3 }, "baseline run 3", 3},
		{"log has no PASS line", func(f *baselineFixture) { f.log = func(int) string { return "local: FAIL\n" } }, `has no "local: PASS" line`, 1},
		{"summary schema is not 2", func(f *baselineFixture) {
			base := f.summary
			f.summary = func(run int, policy coveragePolicy) coverageReport {
				report := base(run, policy)
				report.SchemaVersion = 1
				return report
			}
		}, "schemaVersion = 1, want 2", 1},
		{"branch is not measured", func(f *baselineFixture) {
			base := f.summary
			f.summary = func(run int, policy coveragePolicy) coverageReport {
				report := base(run, policy)
				report.BranchCoverage.Status = "unmeasured"
				return report
			}
		}, `branch status = "unmeasured", want measured`, 1},
		{"summary did not pass", func(f *baselineFixture) {
			base := f.summary
			f.summary = func(run int, policy coveragePolicy) coverageReport {
				report := base(run, policy)
				report.Passed = false
				report.Error = "coverage gate failed"
				return report
			}
		}, "did not pass", 1},
		{"a core metric is missing", func(f *baselineFixture) {
			base := f.summary
			f.summary = func(run int, policy coveragePolicy) coverageReport {
				report := base(run, policy)
				delete(report.Core, policy.CorePackages[0])
				return report
			}
		}, "no core branch metric for narumq/internal/domain/connrebalance", 1},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBaselineFixture(t)
			test.mutate(fixture)
			_, err := runBranchBaseline(context.Background(), fixture.root, fixture.runner(t), &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("runBranchBaseline() = %v, want error containing %q", err, test.wantErr)
			}
			if fixture.runs != test.wantRuns {
				t.Fatalf("runs = %d, want %d (stop at the first bad run)", fixture.runs, test.wantRuns)
			}
			if _, statErr := os.Stat(filepath.Join(fixture.root, filepath.FromSlash(defaultBranchBaselinePath))); statErr == nil {
				t.Fatal("a baseline file was written for a rejected run")
			}
		})
	}
}
