package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Scenarios: performance-and-verification.md:PRF-SCN-115
func TestRequireExecutedTreatsOnlyUnrunAcceptanceTestsAsOutsideGate(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"go.mod":          "module " + scenarioFixtureModule + "\n",
		"docs/specs/a.md": scenarioTable("AAA-SCN-001", "AAA-SCN-002", "AAA-SCN-003", "AAA-SCN-004", "AAA-SCN-005"),
		"pkg/one_test.go": "package pkg\n\n" +
			"// Acceptance: NARUMQ_A\n// Scenarios: a.md:AAA-SCN-001\nfunc TestOptSkipped(t *testing.T) {}\n\n" +
			"// Acceptance: NARUMQ_A\n// Scenarios: a.md:AAA-SCN-002\nfunc TestOptMissing(t *testing.T) {}\n\n" +
			"// Acceptance: NARUMQ_B\n// Scenarios: a.md:AAA-SCN-003\nfunc TestOptPassed(t *testing.T) {}\n\n" +
			"// Acceptance: NARUMQ_B\n// Scenarios: a.md:AAA-SCN-004\nfunc TestOptFailed(t *testing.T) {}\n\n" +
			"// Scenarios: a.md:AAA-SCN-005\nfunc TestPlainSkipped(t *testing.T) {}\n",
		"results/go-test.json": goTestEvents(scenarioFixtureModule+"/pkg", map[string]string{
			"TestOptSkipped": "skip", "TestOptPassed": "pass", "TestOptFailed": "fail", "TestPlainSkipped": "skip",
		}),
	})
	options := scenarioOptions{GoTestJSON: []string{"results/go-test.json"}, OutputDir: defaultScenarioOutputDir, RequireExecuted: true}

	report, err := runScenarios(root, options, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}

	wantStatuses := map[string]string{
		"AAA-SCN-001": scenarioStatusNotRun,
		"AAA-SCN-002": scenarioStatusNotRun,
		"AAA-SCN-003": scenarioStatusPassed,
		"AAA-SCN-004": scenarioStatusFailed,
		"AAA-SCN-005": scenarioStatusNotRun,
	}
	wantReasons := map[string]string{
		"AAA-SCN-001": scenarioReasonOutsideGate,
		"AAA-SCN-002": scenarioReasonOutsideGate,
		"AAA-SCN-003": "",
		"AAA-SCN-004": "",
		"AAA-SCN-005": scenarioReasonSkipped,
	}
	if got := scenarioStatuses(report, "a.md"); !reflect.DeepEqual(got, wantStatuses) {
		t.Fatalf("statuses = %v, want %v", got, wantStatuses)
	}
	if got := scenarioReasons(report, "a.md"); !reflect.DeepEqual(got, wantReasons) {
		t.Fatalf("reasons = %v, want %v", got, wantReasons)
	}
	if got, want := report.Summary, (scenarioSummary{Specs: 1, Scenarios: 5, Passed: 1, Failed: 1, NotRun: 3, NotExecuted: 1, OutsideGate: 2, RequireExecuted: true}); got != want {
		t.Fatalf("summary = %+v, want %+v", got, want)
	}
	markdown, err := os.ReadFile(filepath.Join(root, ".artifacts", "verification", "scenarios", "report.md"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "`" + scenarioFixtureModule + "/pkg.TestOptSkipped` (pkg/one_test.go:5): 게이트 밖"; !strings.Contains(string(markdown), want) {
		t.Fatalf("report.md missing %q:\n%s", want, markdown)
	}

	// Without the failed acceptance test and the skipped plain test the gate passes even though two
	// acceptance tests did not run.
	if err := os.WriteFile(filepath.Join(root, "results", "go-test.json"), []byte(goTestEvents(scenarioFixtureModule+"/pkg", map[string]string{
		"TestOptSkipped": "skip", "TestOptPassed": "pass", "TestOptFailed": "skip", "TestPlainSkipped": "pass",
	})), 0o644); err != nil {
		t.Fatal(err)
	}
	report, err = runScenarios(root, options, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.hasProblems() {
		t.Fatalf("execution check failed on acceptance tests that did not run: %+v", report.Summary)
	}
	if got, want := report.Summary.OutsideGate, 3; got != want {
		t.Fatalf("outside gate = %d, want %d", got, want)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-115
func TestScanScenariosRejectsMisplacedAndMalformedAcceptanceMarkers(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"go.mod":          "module " + scenarioFixtureModule + "\n",
		"docs/specs/a.md": scenarioTable("AAA-SCN-001"),
		"pkg/one_test.go": "package pkg\n\n" +
			"// Acceptance: NARUMQ_OK\n// Scenarios: a.md:AAA-SCN-001\nfunc TestValid(t *testing.T) {}\n\n" + // line 3
			"// Acceptance: NARUMQ_HELPER\n// Scenarios: a.md:AAA-SCN-001\nfunc helper() {}\n\n" + // line 7
			"// Acceptance: narumq_lower\n// Scenarios: a.md:AAA-SCN-001\nfunc TestLower(t *testing.T) {}\n\n" + // line 11
			"// Acceptance:\n// Scenarios: a.md:AAA-SCN-001\nfunc TestEmpty(t *testing.T) {}\n\n" + // line 15
			"// Acceptance: NARUMQ_ONE\n// Acceptance: NARUMQ_TWO\n// Scenarios: a.md:AAA-SCN-001\nfunc TestTwice(t *testing.T) {}\n\n" + // line 19
			"// Acceptance: NARUMQ_ALONE\nfunc TestNoScenarios(t *testing.T) {}\n\n" + // line 24, func 25
			"// Acceptance: NARUMQ_BENCH\n// Scenarios: a.md:AAA-SCN-001\nfunc BenchmarkMarked(b *testing.B) {}\n", // line 27
		"pkg/source.go": "package pkg\n\n// Acceptance: NARUMQ_SOURCE\n// Scenarios: a.md:AAA-SCN-001\nfunc TestInSource(t *testing.T) {}\n",
	})

	refs, problems, err := scanScenarioTests(root)
	if err != nil {
		t.Fatal(err)
	}

	got := make([]string, 0, len(problems))
	for _, problem := range problems {
		got = append(got, problem.String())
	}
	want := []string{
		"pkg/one_test.go:7: Acceptance: must be a line of the doc comment of a Test function in a _test.go file",
		"pkg/one_test.go:11: malformed Acceptance: \"narumq_lower\" is not an environment variable name",
		"pkg/one_test.go:15: malformed Acceptance: \"\" is not an environment variable name",
		"pkg/one_test.go:20: duplicate Acceptance: line",
		"pkg/one_test.go:25: Acceptance: needs a Scenarios: line in the same doc comment",
		"pkg/one_test.go:27: Acceptance: must be a line of the doc comment of a Test function in a _test.go file",
		"pkg/source.go:3: Acceptance: must be a line of the doc comment of a Test function in a _test.go file",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("problems =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	marked := make(map[string]string)
	for _, ref := range refs {
		if ref.Test.Acceptance != "" {
			marked[ref.Test.Name] = ref.Test.Acceptance
		}
	}
	if wantMarked := map[string]string{"TestValid": "NARUMQ_OK", "TestTwice": "NARUMQ_ONE"}; !reflect.DeepEqual(marked, wantMarked) {
		t.Fatalf("marked tests = %v, want %v", marked, wantMarked)
	}
}

func acceptanceFixture(t *testing.T) string {
	t.Helper()
	return writeScenarioFixture(t, map[string]string{
		"go.mod":          "module " + modulePath + "\n",
		"docs/specs/a.md": scenarioTable("AAA-SCN-001", "AAA-SCN-002", "AAA-SCN-003"),
		"pkg/one_test.go": "package pkg\n\n" +
			"// Acceptance: NARUMQ_A\n// Scenarios: a.md:AAA-SCN-001\nfunc TestOptAlpha(t *testing.T) {}\n\n" +
			"// Acceptance: NARUMQ_B\n// Scenarios: a.md:AAA-SCN-002\nfunc TestOptBeta(t *testing.T) {}\n\n" +
			"// Scenarios: a.md:AAA-SCN-003\nfunc TestPlain(t *testing.T) {}\n",
	})
}

// fakeAcceptanceRunner records each command and writes events to its stdout file like go test -json.
func fakeAcceptanceRunner(events string, runErr error, calls *[]commandSpec) commandRunner {
	return func(_ context.Context, root string, spec commandSpec) error {
		*calls = append(*calls, spec)
		path := filepath.Join(root, filepath.FromSlash(spec.StdoutFile))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(events), 0o644); err != nil {
			return err
		}
		return runErr
	}
}

func acceptanceEnvironment(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

// Scenarios: performance-and-verification.md:PRF-SCN-116
func TestRunAcceptanceRequiresEnvironmentBeforeRunning(t *testing.T) {
	root := acceptanceFixture(t)
	var calls []commandSpec

	err := runAcceptance(context.Background(), root, "", acceptanceEnvironment(map[string]string{"NARUMQ_A": "1", "NARUMQ_B": "true"}), fakeAcceptanceRunner("", nil, &calls), &bytes.Buffer{})

	if err == nil || !strings.Contains(err.Error(), "set NARUMQ_B=1") || strings.Contains(err.Error(), "NARUMQ_A") {
		t.Fatalf("error = %v, want only NARUMQ_B=1 reported missing", err)
	}
	if len(calls) != 0 {
		t.Fatalf("ran %d commands before the environment was ready", len(calls))
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-116
func TestRunAcceptanceRequiresEverySelectedTestToPass(t *testing.T) {
	ready := acceptanceEnvironment(map[string]string{"NARUMQ_A": "1", "NARUMQ_B": "1"})
	pkg := modulePath + "/pkg"
	for _, tc := range []struct {
		name       string
		events     string
		runErr     error
		wantErr    []string
		wantOutput string
	}{
		{name: "all pass", events: goTestEvents(pkg, map[string]string{"TestOptAlpha": "pass", "TestOptBeta": "pass"}), wantOutput: "acceptance: 2 tests passed"},
		{name: "skip", events: goTestEvents(pkg, map[string]string{"TestOptAlpha": "pass", "TestOptBeta": "skip"}), wantErr: []string{"TestOptBeta (건너뜀)"}},
		{name: "missing", events: goTestEvents(pkg, map[string]string{"TestOptAlpha": "pass"}), wantErr: []string{"TestOptBeta (결과 없음)"}},
		{name: "fail", events: goTestEvents(pkg, map[string]string{"TestOptAlpha": "fail", "TestOptBeta": "pass"}), runErr: errors.New("exit status 1"), wantErr: []string{"exit status 1", "TestOptAlpha (실패)"}},
		{name: "command fails after passes", events: goTestEvents(pkg, map[string]string{"TestOptAlpha": "pass", "TestOptBeta": "pass"}), runErr: errors.New("exit status 2"), wantErr: []string{"exit status 2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := acceptanceFixture(t)
			var calls []commandSpec
			var output bytes.Buffer

			err := runAcceptance(context.Background(), root, "", ready, fakeAcceptanceRunner(tc.events, tc.runErr, &calls), &output)

			wantArgs := []string{"test", "-json", "-count=1", "-timeout=3h", "-run", "^(TestOptAlpha|TestOptBeta)$", "./pkg"}
			if len(calls) != 1 || !reflect.DeepEqual(calls[0].Args, wantArgs) || calls[0].StdoutFile != defaultAcceptanceGoTestJSONPath || calls[0].Dir != "" {
				t.Fatalf("commands = %+v, want one go %v in the source root writing %s", calls, wantArgs, defaultAcceptanceGoTestJSONPath)
			}
			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("runAcceptance error = %v", err)
				}
				if !strings.Contains(output.String(), tc.wantOutput) {
					t.Fatalf("output = %q, want %q", output.String(), tc.wantOutput)
				}
				return
			}
			if err == nil {
				t.Fatal("runAcceptance accepted a selected test without a pass result")
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %v, want %q", err, want)
				}
			}
		})
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-116
func TestRunAcceptanceRunFilterSelectsMarkedTestsOnly(t *testing.T) {
	ready := acceptanceEnvironment(map[string]string{"NARUMQ_B": "1"})
	root := acceptanceFixture(t)
	var calls []commandSpec

	err := runAcceptance(context.Background(), root, "Beta|Plain", ready, fakeAcceptanceRunner(goTestEvents(modulePath+"/pkg", map[string]string{"TestOptBeta": "pass"}), nil, &calls), &bytes.Buffer{})
	if err != nil {
		t.Fatalf("runAcceptance error = %v", err)
	}
	if len(calls) != 1 || calls[0].Args[5] != "^(TestOptBeta)$" {
		t.Fatalf("commands = %+v, want only TestOptBeta selected", calls)
	}

	for _, pattern := range []string{"Plain", "("} {
		calls = nil
		if err := runAcceptance(context.Background(), root, pattern, ready, fakeAcceptanceRunner("", nil, &calls), &bytes.Buffer{}); err == nil || len(calls) != 0 {
			t.Fatalf("pattern %q: error = %v, commands = %d; want rejection before running", pattern, err, len(calls))
		}
	}
}
