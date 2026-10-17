package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

const defaultGremlinsConfigPath = "tests/verification/gremlins.yaml"

// mutationPackageNames are the packages `verify mutation` checks, in report order.
var mutationPackageNames = []string{"delivery", "mqtt", "nodedrain", "security", "session"}

var (
	gremlinsEfficacyPattern = regexp.MustCompile(`(?m)^\s*efficacy:\s*([0-9]+(?:\.[0-9]+)?)\s*$`)
	gremlinsCoveragePattern = regexp.MustCompile(`(?m)^\s*mutant-coverage:\s*([0-9]+(?:\.[0-9]+)?)\s*$`)
)

type gremlinsMutation struct {
	Type   string `json:"type"`
	Status string `json:"status"`
	Line   int    `json:"line"`
	Column int    `json:"column"`
}

type gremlinsFile struct {
	FileName  string             `json:"file_name"`
	Mutations []gremlinsMutation `json:"mutations"`
}

// gremlinsReport is the JSON written by `gremlins unleash --output`. Per-mutant statuses are the
// source of truth; the aggregate fields are cross-checked against them.
type gremlinsReport struct {
	GoModule          string         `json:"go_module"`
	TestEfficacy      float64        `json:"test_efficacy"`
	MutationsCoverage float64        `json:"mutations_coverage"`
	MutantsKilled     int            `json:"mutants_killed"`
	MutantsLived      int            `json:"mutants_lived"`
	MutantsNotViable  int            `json:"mutants_not_viable"`
	MutantsNotCovered int            `json:"mutants_not_covered"`
	Files             []gremlinsFile `json:"files"`
}

// mutationTally counts per-mutant statuses. TimedOut mutants are neither killed nor lived.
type mutationTally struct {
	Killed, Lived, NotCovered, TimedOut, NotViable int
}

// efficacy is killed/(killed+lived) in percent. A package without any decided mutant has none.
func (tally mutationTally) efficacy() (float64, bool) {
	decided := tally.Killed + tally.Lived
	if decided == 0 {
		return 0, false
	}
	return 100 * float64(tally.Killed) / float64(decided), true
}

// mutantCoverage is (killed+lived)/(killed+lived+notCovered) in percent.
func (tally mutationTally) mutantCoverage() (float64, bool) {
	covered := tally.Killed + tally.Lived
	total := covered + tally.NotCovered
	if total == 0 {
		return 0, false
	}
	return 100 * float64(covered) / float64(total), true
}

// loadGremlinsThresholds reads the efficacy and mutant-coverage floors from the Gremlins config so the
// verify command and Gremlins itself judge against the same numbers.
func loadGremlinsThresholds(path string) (efficacy, coverage float64, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, fmt.Errorf("read gremlins config %s: %w", path, err)
	}
	read := func(pattern *regexp.Regexp, name string) (float64, error) {
		match := pattern.FindSubmatch(data)
		if match == nil {
			return 0, fmt.Errorf("gremlins config %s has no threshold %s", path, name)
		}
		return strconv.ParseFloat(string(match[1]), 64)
	}
	if efficacy, err = read(gremlinsEfficacyPattern, "efficacy"); err != nil {
		return 0, 0, err
	}
	if coverage, err = read(gremlinsCoveragePattern, "mutant-coverage"); err != nil {
		return 0, 0, err
	}
	return efficacy, coverage, nil
}

// tallyGremlinsReport decodes one report strictly and counts its per-mutant statuses. Statuses that
// are not final outcomes (RUNNABLE, SKIPPED, unknown) mean the run did not complete.
func tallyGremlinsReport(data []byte) (mutationTally, error) {
	var report gremlinsReport
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&report); err != nil {
		return mutationTally{}, fmt.Errorf("decode gremlins report: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return mutationTally{}, errors.New("decode gremlins report: trailing JSON value")
	}
	var tally mutationTally
	for _, file := range report.Files {
		for _, mutation := range file.Mutations {
			switch mutation.Status {
			case "KILLED":
				tally.Killed++
			case "LIVED":
				tally.Lived++
			case "NOT COVERED":
				tally.NotCovered++
			case "TIMED OUT":
				tally.TimedOut++
			case "NOT VIABLE":
				tally.NotViable++
			default:
				return mutationTally{}, fmt.Errorf("%s:%d:%d %s has non-final status %q", file.FileName, mutation.Line, mutation.Column, mutation.Type, mutation.Status)
			}
		}
	}
	if tally.Killed != report.MutantsKilled || tally.Lived != report.MutantsLived || tally.NotCovered != report.MutantsNotCovered || tally.NotViable != report.MutantsNotViable {
		return mutationTally{}, fmt.Errorf("per-mutant statuses (killed %d, lived %d, not covered %d, not viable %d) disagree with report totals (killed %d, lived %d, not covered %d, not viable %d)",
			tally.Killed, tally.Lived, tally.NotCovered, tally.NotViable,
			report.MutantsKilled, report.MutantsLived, report.MutantsNotCovered, report.MutantsNotViable)
	}
	if efficacy, ok := tally.efficacy(); ok && math.Abs(efficacy-report.TestEfficacy) > 0.01 {
		return mutationTally{}, fmt.Errorf("report test_efficacy %.2f disagrees with %.2f computed from mutant statuses; TIMED OUT mutants must not count as killed", report.TestEfficacy, efficacy)
	}
	if coverage, ok := tally.mutantCoverage(); ok && math.Abs(coverage-report.MutationsCoverage) > 0.01 {
		return mutationTally{}, fmt.Errorf("report mutations_coverage %.2f disagrees with %.2f computed from mutant statuses", report.MutationsCoverage, coverage)
	}
	return tally, nil
}

// judgeMutationTally applies the per-package floors. TIMED OUT mutants never raise either score.
func judgeMutationTally(tally mutationTally, efficacyFloor, coverageFloor float64) []string {
	var failures []string
	if efficacy, ok := tally.efficacy(); !ok {
		failures = append(failures, "no killed or lived mutant, efficacy is undefined")
	} else if efficacy < efficacyFloor {
		failures = append(failures, fmt.Sprintf("efficacy %.2f%% is below %.2f%%", efficacy, efficacyFloor))
	}
	if coverage, ok := tally.mutantCoverage(); !ok {
		failures = append(failures, "no killed, lived or not covered mutant, mutant coverage is undefined")
	} else if coverage < coverageFloor {
		failures = append(failures, fmt.Sprintf("mutant coverage %.2f%% is below %.2f%%", coverage, coverageFloor))
	}
	return failures
}

// verifyMutationReports judges every package's Gremlins report under .artifacts/verification and
// prints one line per package. Every package is judged even after an earlier one fails.
func verifyMutationReports(root string, out io.Writer) error {
	efficacyFloor, coverageFloor, err := loadGremlinsThresholds(filepath.Join(root, filepath.FromSlash(defaultGremlinsConfigPath)))
	if err != nil {
		return err
	}
	var failures []error
	for _, name := range mutationPackageNames {
		path := filepath.Join(root, ".artifacts", "verification", "mutation-"+name+".json")
		data, err := os.ReadFile(path)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: read gremlins report: %w", name, err))
			continue
		}
		tally, err := tallyGremlinsReport(data)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", name, err))
			continue
		}
		efficacy, _ := tally.efficacy()
		coverage, _ := tally.mutantCoverage()
		fmt.Fprintf(out, "mutation %s: killed %d, lived %d, not covered %d, timed out %d, efficacy %.2f%%, mutant coverage %.2f%%\n",
			name, tally.Killed, tally.Lived, tally.NotCovered, tally.TimedOut, efficacy, coverage)
		for _, failure := range judgeMutationTally(tally, efficacyFloor, coverageFloor) {
			failures = append(failures, fmt.Errorf("%s: %s", name, failure))
		}
	}
	return errors.Join(failures...)
}
