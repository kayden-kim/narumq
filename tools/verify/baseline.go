package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	branchBaselineRuns        = 3
	defaultBranchBaselinePath = ".artifacts/verification/branch-baseline.json"
	coverageSummaryPath       = ".artifacts/verification/coverage-summary.json"
)

// branchMeasurement is the measured branch outcome count of one full local run.
type branchMeasurement struct {
	Run     int                       `json:"run"`
	Overall coverageMetric            `json:"overall"`
	Core    map[string]coverageMetric `json:"core"`
}

// branchBaseline is the ratchet candidate derived from the first two measurements.
type branchBaseline struct {
	Overall float64            `json:"overall"`
	Core    map[string]float64 `json:"core"`
}

// branchBaselineReport is written to .artifacts/verification/branch-baseline.json.
type branchBaselineReport struct {
	SchemaVersion int                 `json:"schemaVersion"`
	Runs          []branchMeasurement `json:"runs"`
	Baseline      branchBaseline      `json:"baseline"`
}

// floorBranchPercent returns covered/total in percent floored to two decimals. It uses integer
// arithmetic so the rounded percent in a summary can never push the baseline above the measurement.
func floorBranchPercent(covered, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(covered*10000/total) / 100
}

// deriveBranchBaseline takes the smaller of the two measured values per metric and floors it to two decimals.
func deriveBranchBaseline(first, second branchMeasurement) branchBaseline {
	baseline := branchBaseline{
		Overall: min(floorBranchPercent(first.Overall.Covered, first.Overall.Total), floorBranchPercent(second.Overall.Covered, second.Overall.Total)),
		Core:    make(map[string]float64, len(first.Core)),
	}
	for name, metric := range first.Core {
		other := second.Core[name]
		baseline.Core[name] = min(floorBranchPercent(metric.Covered, metric.Total), floorBranchPercent(other.Covered, other.Total))
	}
	return baseline
}

// readBranchMeasurement validates one coverage summary written by a successful local run.
func readBranchMeasurement(path string, run int, corePackages []string) (branchMeasurement, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return branchMeasurement{}, fmt.Errorf("read coverage summary: %w", err)
	}
	var report coverageReport
	if err := json.Unmarshal(data, &report); err != nil {
		return branchMeasurement{}, fmt.Errorf("decode coverage summary: %w", err)
	}
	switch {
	case report.SchemaVersion != 2:
		return branchMeasurement{}, fmt.Errorf("coverage summary schemaVersion = %d, want 2", report.SchemaVersion)
	case !report.Passed || report.Error != "":
		return branchMeasurement{}, fmt.Errorf("coverage summary did not pass: %s", report.Error)
	case report.BranchCoverage.Status != "measured":
		return branchMeasurement{}, fmt.Errorf("coverage summary branch status = %q, want measured", report.BranchCoverage.Status)
	case report.Overall.Branch.Total <= 0:
		return branchMeasurement{}, errors.New("coverage summary reports no overall branch outcomes")
	}
	measurement := branchMeasurement{Run: run, Overall: report.Overall.Branch, Core: make(map[string]coverageMetric, len(corePackages))}
	var missing []string
	for _, name := range corePackages {
		core, ok := report.Core[name]
		if !ok || core.Branch.Total <= 0 {
			missing = append(missing, name)
			continue
		}
		measurement.Core[name] = core.Branch
	}
	if len(missing) > 0 {
		return branchMeasurement{}, fmt.Errorf("coverage summary has no core branch metric for %s", strings.Join(missing, ", "))
	}
	return measurement, nil
}

// runBranchBaseline runs `go run ./tools/verify local` three times through runner. Every run must
// exit 0 with a `local: PASS` line and leave a fresh schema 2 summary with measured branch outcomes
// for the overall total and every core package. The baseline is the smaller of the first two
// measurements per metric, floored to two decimals. The third run only has to pass.
// Scenarios: performance-and-verification.md:PRF-SCN-082
func runBranchBaseline(ctx context.Context, root string, runner commandRunner, out io.Writer) (branchBaselineReport, error) {
	if err := ensureRepositoryRoot(root); err != nil {
		return branchBaselineReport{}, err
	}
	policy, err := loadCoveragePolicy(filepath.Join(root, filepath.FromSlash(defaultCoveragePolicyPath)))
	if err != nil {
		return branchBaselineReport{}, err
	}
	summary := filepath.Join(root, filepath.FromSlash(coverageSummaryPath))
	var runs []branchMeasurement
	for run := 1; run <= branchBaselineRuns; run++ {
		if err := os.Remove(summary); err != nil && !errors.Is(err, os.ErrNotExist) {
			return branchBaselineReport{}, fmt.Errorf("remove previous coverage summary: %w", err)
		}
		logPath := fmt.Sprintf(".artifacts/verification/baseline-local-%d.log", run)
		command := commandSpec{Name: "go", Args: []string{"run", "./tools/verify", "local"}, StdoutFile: logPath}
		if err := runner(ctx, root, command); err != nil {
			return branchBaselineReport{}, fmt.Errorf("baseline run %d: %w", run, err)
		}
		logData, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(logPath)))
		if err != nil {
			return branchBaselineReport{}, fmt.Errorf("baseline run %d: read %s: %w", run, logPath, err)
		}
		if !hasLine(logData, "local: PASS") {
			return branchBaselineReport{}, fmt.Errorf("baseline run %d: %s has no %q line", run, logPath, "local: PASS")
		}
		measurement, err := readBranchMeasurement(summary, run, policy.CorePackages)
		if err != nil {
			return branchBaselineReport{}, fmt.Errorf("baseline run %d: %w", run, err)
		}
		fmt.Fprintf(out, "baseline run %d: local: PASS, overall branch %d/%d (%.2f%%)\n", run, measurement.Overall.Covered, measurement.Overall.Total, floorBranchPercent(measurement.Overall.Covered, measurement.Overall.Total))
		runs = append(runs, measurement)
	}
	report := branchBaselineReport{SchemaVersion: 1, Runs: runs, Baseline: deriveBranchBaseline(runs[0], runs[1])}
	fmt.Fprintf(out, "baseline: overall branch %.2f%% (smaller of runs 1 and 2, floored to 0.01%%p)\n", report.Baseline.Overall)
	for _, name := range policy.CorePackages {
		fmt.Fprintf(out, "baseline: %s branch %.2f%% (policy ratchet %.2f%%)\n", name, report.Baseline.Core[name], policy.Ratchet.Core[name].Branch)
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return branchBaselineReport{}, fmt.Errorf("encode branch baseline: %w", err)
	}
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(defaultBranchBaselinePath)), append(encoded, '\n'), 0o644); err != nil {
		return branchBaselineReport{}, fmt.Errorf("write branch baseline: %w", err)
	}
	return report, nil
}

func hasLine(data []byte, want string) bool {
	for _, line := range bytes.Split(data, []byte("\n")) {
		if strings.TrimSpace(string(line)) == want {
			return true
		}
	}
	return false
}
