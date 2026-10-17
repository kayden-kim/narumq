package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testGremlinsConfig = "unleash:\n  workers: 1\n  test-cpu: 1\n  threshold:\n    efficacy: 80\n    mutant-coverage: 80\n"

// gremlinsReportBytes renders a report in the schema gremlins v0.6.0 writes. The aggregate scores
// default to the values Gremlins computes: TIMED OUT mutants are in neither denominator.
func gremlinsReportBytes(t *testing.T, tally mutationTally, overrideEfficacy, overrideCoverage *float64) []byte {
	t.Helper()
	var mutations []map[string]any
	add := func(status string, count int) {
		for range count {
			mutations = append(mutations, map[string]any{"type": "CONDITIONALS_NEGATION", "status": status, "line": len(mutations) + 1, "column": 1})
		}
	}
	add("KILLED", tally.Killed)
	add("LIVED", tally.Lived)
	add("NOT COVERED", tally.NotCovered)
	add("TIMED OUT", tally.TimedOut)
	add("NOT VIABLE", tally.NotViable)
	efficacy, _ := tally.efficacy()
	coverage, _ := tally.mutantCoverage()
	if overrideEfficacy != nil {
		efficacy = *overrideEfficacy
	}
	if overrideCoverage != nil {
		coverage = *overrideCoverage
	}
	data, err := json.Marshal(map[string]any{
		"go_module":           "narumq",
		"files":               []map[string]any{{"file_name": "state.go", "mutations": mutations}},
		"test_efficacy":       efficacy,
		"mutations_coverage":  coverage,
		"mutants_total":       tally.Killed + tally.Lived,
		"mutants_killed":      tally.Killed,
		"mutants_lived":       tally.Lived,
		"mutants_not_viable":  tally.NotViable,
		"mutants_not_covered": tally.NotCovered,
		"elapsed_time":        1.5,
	})
	if err != nil {
		t.Fatalf("encode gremlins report: %v", err)
	}
	return data
}

func writeMutationReports(t *testing.T, tallies map[string]mutationTally, forged map[string][]byte) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "tests", "verification"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "tests", "verification", "gremlins.yaml"), []byte(testGremlinsConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, ".artifacts", "verification"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, tally := range tallies {
		data := gremlinsReportBytes(t, tally, nil, nil)
		if override, ok := forged[name]; ok {
			data = override
		}
		if err := os.WriteFile(filepath.Join(root, ".artifacts", "verification", "mutation-"+name+".json"), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func passingMutationTallies() map[string]mutationTally {
	tallies := make(map[string]mutationTally, len(mutationPackageNames))
	for _, name := range mutationPackageNames {
		tallies[name] = mutationTally{Killed: 90, Lived: 0, NotCovered: 6, TimedOut: 2}
	}
	return tallies
}

// TestVerifyMutationReportsJudgesEachPackageAndNeverCountsTimedOutAsKilled judges Gremlins reports in the
// schema `gremlins unleash --output` writes for the five mutation packages.
// Scenarios: performance-and-verification.md:PRF-SCN-101
func TestVerifyMutationReportsJudgesEachPackageAndNeverCountsTimedOutAsKilled(t *testing.T) {
	t.Run("all five packages at or above 80 percent pass and the floors come from gremlins.yaml", func(t *testing.T) {
		tallies := passingMutationTallies()
		// Exactly 80% efficacy (8 of 10 decided) and exactly 80% mutant coverage (40 of 50).
		tallies["session"] = mutationTally{Killed: 8, Lived: 2, NotCovered: 0}
		tallies["mqtt"] = mutationTally{Killed: 40, Lived: 0, NotCovered: 10}
		var out bytes.Buffer
		if err := verifyMutationReports(writeMutationReports(t, tallies, nil), &out); err != nil {
			t.Fatalf("verifyMutationReports() = %v, want nil\n%s", err, out.String())
		}
		for _, name := range mutationPackageNames {
			if !strings.Contains(out.String(), "mutation "+name+":") {
				t.Fatalf("output has no line for %s:\n%s", name, out.String())
			}
		}
	})

	t.Run("TIMED OUT mutants do not raise efficacy", func(t *testing.T) {
		tallies := passingMutationTallies()
		// 7 killed, 3 lived, 10 timed out: 70% efficacy. Counting the timeouts as killed would give 85%.
		tallies["delivery"] = mutationTally{Killed: 7, Lived: 3, TimedOut: 10}
		err := verifyMutationReports(writeMutationReports(t, tallies, nil), &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "delivery: efficacy 70.00% is below 80.00%") {
			t.Fatalf("verifyMutationReports() = %v, want delivery efficacy 70.00%% rejection", err)
		}
	})

	t.Run("TIMED OUT mutants do not raise mutant coverage", func(t *testing.T) {
		tallies := passingMutationTallies()
		// 7 killed, 3 not covered, 10 timed out: 70% coverage. Timeouts counted as covered would give 85%.
		tallies["security"] = mutationTally{Killed: 7, NotCovered: 3, TimedOut: 10}
		err := verifyMutationReports(writeMutationReports(t, tallies, nil), &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "security: mutant coverage 70.00% is below 80.00%") {
			t.Fatalf("verifyMutationReports() = %v, want security mutant coverage 70.00%% rejection", err)
		}
	})

	t.Run("a report that counts TIMED OUT as killed is rejected", func(t *testing.T) {
		tallies := passingMutationTallies()
		tallies["nodedrain"] = mutationTally{Killed: 7, Lived: 3, TimedOut: 10}
		forgedEfficacy := 85.0
		forged := map[string][]byte{"nodedrain": gremlinsReportBytes(t, tallies["nodedrain"], &forgedEfficacy, nil)}
		err := verifyMutationReports(writeMutationReports(t, tallies, forged), &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "nodedrain: report test_efficacy 85.00 disagrees with 70.00") {
			t.Fatalf("verifyMutationReports() = %v, want nodedrain aggregate disagreement", err)
		}
	})

	t.Run("each package is judged independently and every failure is reported", func(t *testing.T) {
		tallies := passingMutationTallies()
		tallies["delivery"] = mutationTally{Killed: 1, Lived: 4}
		tallies["session"] = mutationTally{Killed: 4, NotCovered: 4}
		err := verifyMutationReports(writeMutationReports(t, tallies, nil), &bytes.Buffer{})
		if err == nil {
			t.Fatal("verifyMutationReports() = nil, want failures")
		}
		for _, want := range []string{"delivery: efficacy 20.00% is below 80.00%", "session: mutant coverage 50.00% is below 80.00%"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not contain %q", err, want)
			}
		}
		for _, name := range []string{"mqtt", "nodedrain", "security"} {
			if strings.Contains(err.Error(), name+":") {
				t.Fatalf("passing package %s was reported as failing: %v", name, err)
			}
		}
	})

	t.Run("efficacy exactly below the floor fails", func(t *testing.T) {
		tallies := passingMutationTallies()
		tallies["mqtt"] = mutationTally{Killed: 79, Lived: 21}
		err := verifyMutationReports(writeMutationReports(t, tallies, nil), &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "mqtt: efficacy 79.00% is below 80.00%") {
			t.Fatalf("verifyMutationReports() = %v, want mqtt efficacy 79.00%% rejection", err)
		}
	})

	t.Run("a package with only timed out mutants has no score and fails", func(t *testing.T) {
		tallies := passingMutationTallies()
		tallies["session"] = mutationTally{TimedOut: 5}
		err := verifyMutationReports(writeMutationReports(t, tallies, nil), &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "session: no killed or lived mutant") {
			t.Fatalf("verifyMutationReports() = %v, want undefined-efficacy rejection", err)
		}
	})

	t.Run("missing report and unfinished mutants are rejected", func(t *testing.T) {
		tallies := passingMutationTallies()
		delete(tallies, "security")
		unfinished, err := json.Marshal(map[string]any{
			"go_module": "narumq",
			"files": []map[string]any{{"file_name": "state.go", "mutations": []map[string]any{
				{"type": "CONDITIONALS_NEGATION", "status": "RUNNABLE", "line": 1, "column": 1},
			}}},
			"test_efficacy": 100, "mutations_coverage": 100,
		})
		if err != nil {
			t.Fatal(err)
		}
		err = verifyMutationReports(writeMutationReports(t, tallies, map[string][]byte{"delivery": unfinished}), &bytes.Buffer{})
		if err == nil {
			t.Fatal("verifyMutationReports() = nil, want failures")
		}
		for _, want := range []string{"security: read gremlins report", `delivery: state.go:1:1 CONDITIONALS_NEGATION has non-final status "RUNNABLE"`} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not contain %q", err, want)
			}
		}
	})

	t.Run("a missing threshold in the gremlins config is rejected", func(t *testing.T) {
		root := writeMutationReports(t, passingMutationTallies(), nil)
		if err := os.WriteFile(filepath.Join(root, "tests", "verification", "gremlins.yaml"), []byte("unleash:\n  workers: 1\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		err := verifyMutationReports(root, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "has no threshold efficacy") {
			t.Fatalf("verifyMutationReports() = %v, want missing threshold rejection", err)
		}
	})
}

func TestRepositoryGremlinsThresholdsStayAtEightyPercent(t *testing.T) {
	efficacy, coverage, err := loadGremlinsThresholds(filepath.Join("..", "..", filepath.FromSlash(defaultGremlinsConfigPath)))
	if err != nil {
		t.Fatal(err)
	}
	if efficacy != 80 || coverage != 80 {
		t.Fatalf("gremlins thresholds = efficacy %v, mutant-coverage %v, want 80 and 80", efficacy, coverage)
	}
}
