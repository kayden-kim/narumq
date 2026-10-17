package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

const scenarioFixtureModule = "example.test/fixture"

func writeScenarioFixture(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func scenarioTable(ids ...string) string {
	var table strings.Builder
	table.WriteString("| 시나리오 ID | 요구사항 ID | 조건과 입력 | 기대 결과 |\n| --- | --- | --- | --- |\n")
	for _, id := range ids {
		table.WriteString("| " + id + " | - | 조건 | 결과 |\n")
	}
	return table.String()
}

func scenarioIDs(spec scenarioSpec) []string {
	ids := make([]string, 0, len(spec.Scenarios))
	for _, scenario := range spec.Scenarios {
		ids = append(ids, scenario.ID)
	}
	return ids
}

func scenarioStatuses(report scenarioReport, spec string) map[string]string {
	statuses := make(map[string]string)
	for _, specReport := range report.Specs {
		if specReport.Spec != spec {
			continue
		}
		for _, scenario := range specReport.Scenarios {
			statuses[scenario.ID] = scenario.Status
		}
	}
	return statuses
}

func problemMessages(report scenarioReport) string {
	lines := make([]string, 0, len(report.Problems))
	for _, problem := range report.Problems {
		lines = append(lines, problem.Location+": "+problem.Message)
	}
	return strings.Join(lines, "\n")
}

func TestParseScenarioTablesReadsOnlyScenarioTables(t *testing.T) {
	content := "# 스펙\n\n" +
		"| 항목 | 값 |\n| --- | --- |\n| NOT-SCN-001 | 다른 표 |\n\n" +
		scenarioTable("AAA-SCN-001", "AAA-SCN-002") + "\n" +
		"| ID | 설명 |\n| --- | --- |\n| NOT-SCN-002 | 첫 칸이 다른 표 |\n\n" +
		"```md\n" + scenarioTable("FENCED-SCN-001") + "```\n\n" +
		scenarioTable("AAA-SCN-003")

	spec, problems := parseScenarioTables("a.md", content)

	if len(problems) != 0 {
		t.Fatalf("problems = %v, want none", problems)
	}
	if got, want := scenarioIDs(spec), []string{"AAA-SCN-001", "AAA-SCN-002", "AAA-SCN-003"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-076
func TestParseScenarioTablesRejectsDuplicateAndMalformedIDs(t *testing.T) {
	content := scenarioTable("AAA-SCN-001") + "| AAA-SCN-001 | - | 조건 | 결과 |\n| `AAA-SCN-002` | - | 조건 | 결과 |\n"

	spec, problems := parseScenarioTables("a.md", content)

	if got, want := scenarioIDs(spec), []string{"AAA-SCN-001"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
	if len(problems) != 2 {
		t.Fatalf("problems = %v, want duplicate and malformed", problems)
	}
	if problems[0].Location != "docs/specs/a.md:4" || !strings.Contains(problems[0].Message, "duplicate scenario ID AAA-SCN-001") {
		t.Fatalf("duplicate problem = %v", problems[0])
	}
	if problems[1].Location != "docs/specs/a.md:5" || !strings.Contains(problems[1].Message, "invalid scenario ID") {
		t.Fatalf("malformed problem = %v", problems[1])
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-073
func TestScanScenariosBindsMarkerToNextDeclarationAcrossTraceabilityLines(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"go.mod": "module " + scenarioFixtureModule + "\n\ngo 1.26\n",
		"pkg/one_test.go": "package pkg\n\n" +
			"// Traceability: PRD TEST-10; docs/specs/a.md §1.\n" +
			"// Scenarios: a.md:AAA-SCN-001, a.md:AAA-SCN-002\n" +
			"// Scenarios: b.md:BBB-SCN-001\n" +
			"func TestOne(t *testing.T) {}\n\n" +
			"// Scenarios: a.md:AAA-SCN-003\n" +
			"var detached = 1\n\n" +
			"func TestTwo(t *testing.T) {}\n",
		"tests/chaos/run_test.py": "import unittest\n\n" +
			"# Scenarios: a.md:AAA-SCN-004\n" +
			"# other note\n" +
			"@mock.patch(\n    \"x\",\n)\n" +
			"def scenario_safe_retry(client):\n    pass\n\n" +
			"# Scenarios: a.md:AAA-SCN-005\n\n" +
			"def not_adjacent():\n    pass\n\n" +
			"# Scenarios: a.md:AAA-SCN-007\n" +
			"class StoragePressureRun:\n    pass\n",
		"tools/x/run.go": "package x\n\n" +
			"// Scenarios: a.md:AAA-SCN-006\n" +
			"func (run *compatibilityRun[T]) runDrainClusterCase() {}\n",
		"node_modules/dep_test.go": "package dep\n\n// Scenarios: ignored.md:IGN-SCN-001\nfunc TestIgnored(t *testing.T) {}\n",
		".artifacts/old_test.go":   "package old\n\n// Scenarios: ignored.md:IGN-SCN-001\nfunc TestIgnored(t *testing.T) {}\n",
	})

	refs, problems, err := scanScenarioTests(root)
	if err != nil {
		t.Fatal(err)
	}

	got := make([]string, 0, len(refs))
	for _, ref := range refs {
		got = append(got, ref.Spec+":"+ref.ID+" -> "+ref.Test.Key+" "+ref.Location)
	}
	want := []string{
		"a.md:AAA-SCN-001 -> example.test/fixture/pkg.TestOne pkg/one_test.go:4",
		"a.md:AAA-SCN-002 -> example.test/fixture/pkg.TestOne pkg/one_test.go:4",
		"b.md:BBB-SCN-001 -> example.test/fixture/pkg.TestOne pkg/one_test.go:5",
		"a.md:AAA-SCN-004 -> tests/chaos/run_test.py#scenario_safe_retry tests/chaos/run_test.py:3",
		"a.md:AAA-SCN-007 -> tests/chaos/run_test.py#StoragePressureRun tests/chaos/run_test.py:16",
		"a.md:AAA-SCN-006 -> example.test/fixture/tools/x.compatibilityRun.runDrainClusterCase tools/x/run.go:3",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("refs = %#v, want %#v", got, want)
	}
	var locations []string
	for _, problem := range problems {
		locations = append(locations, problem.Location)
	}
	if want := []string{"pkg/one_test.go:8", "tests/chaos/run_test.py:11"}; !reflect.DeepEqual(locations, want) {
		t.Fatalf("problem locations = %v, want %v (%v)", locations, want, problems)
	}
}

func TestScanScenarioTestsResolvesNestedModuleImportPath(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"go.mod":                   "module " + scenarioFixtureModule + "\n",
		"tests/nested/go.mod":      "module example.test/nested\n",
		"tests/nested/x/x_test.go": "package x\n\n// Scenarios: a.md:AAA-SCN-001\nfunc TestNested(t *testing.T) {}\n",
	})

	refs, problems, err := scanScenarioTests(root)
	if err != nil || len(problems) != 0 {
		t.Fatalf("scan err=%v problems=%v", err, problems)
	}
	if len(refs) != 1 || refs[0].Test.Key != "example.test/nested/x.TestNested" {
		t.Fatalf("refs = %#v, want nested module import path", refs)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-073
func TestRunScenariosReportsMetadataErrorsWithLocation(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"docs/specs/a.md":      scenarioTable("AAA-SCN-001"),
		"docs/specs/README.md": scenarioTable("README-SCN-001"),
		"docs/specs/AGENTS.md": scenarioTable("AGENTS-SCN-001"),
		"pkg/one_test.go": "package pkg\n\n" +
			"// Scenarios: a.md:AAA-SCN-999\n" +
			"func TestUnknownID(t *testing.T) {}\n\n" +
			"// Scenarios: missing.md:AAA-SCN-001\n" +
			"func TestUnknownFile(t *testing.T) {}\n\n" +
			"// Scenarios: a.md AAA-SCN-001\n" +
			"func TestMalformed(t *testing.T) {}\n\n" +
			"// Scenarios: README.md:README-SCN-001\n" +
			"func TestSkippedSpec(t *testing.T) {}\n",
	})

	report, err := runScenarios(root, scenarioOptions{OutputDir: defaultScenarioOutputDir}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}

	messages := problemMessages(report)
	for _, want := range []string{
		"pkg/one_test.go:3: unknown scenario AAA-SCN-999 in a.md",
		"pkg/one_test.go:6: unknown spec file missing.md",
		"pkg/one_test.go:9: malformed scenario reference",
		"pkg/one_test.go:12: unknown spec file README.md",
	} {
		if !strings.Contains(messages, want) {
			t.Fatalf("problems missing %q:\n%s", want, messages)
		}
	}
	if report.Summary.Errors != 4 || report.Summary.Specs != 1 {
		t.Fatalf("summary = %+v, want 4 errors and only a.md parsed", report.Summary)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-074
func TestRunScenariosStatusPrecedenceFailedOverNotRunOverPassed(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"go.mod":          "module " + scenarioFixtureModule + "\n",
		"docs/specs/a.md": scenarioTable("AAA-SCN-001", "AAA-SCN-002", "AAA-SCN-003", "AAA-SCN-004", "AAA-SCN-005", "AAA-SCN-006", "AAA-SCN-007"),
		"pkg/one_test.go": "package pkg\n\n" +
			"// Scenarios: a.md:AAA-SCN-002, a.md:AAA-SCN-005, a.md:AAA-SCN-006\nfunc TestNone(t *testing.T) {}\n\n" +
			"// Scenarios: a.md:AAA-SCN-003, a.md:AAA-SCN-004, a.md:AAA-SCN-005\nfunc TestPass(t *testing.T) {}\n\n" +
			"// Scenarios: a.md:AAA-SCN-004, a.md:AAA-SCN-006\nfunc TestFail(t *testing.T) {}\n\n" +
			"// Scenarios: a.md:AAA-SCN-007\nfunc TestSkip(t *testing.T) {}\n",
		"results/go.json": strings.Join([]string{
			`{"Action":"pass","Package":"` + scenarioFixtureModule + `/pkg","Test":"TestPass"}`,
			`{"Action":"fail","Package":"` + scenarioFixtureModule + `/pkg","Test":"TestFail"}`,
			`{"Action":"skip","Package":"` + scenarioFixtureModule + `/pkg","Test":"TestSkip"}`,
		}, "\n") + "\n",
	})

	report, err := runScenarios(root, scenarioOptions{GoTestJSON: []string{"results/go.json"}, OutputDir: defaultScenarioOutputDir}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		"AAA-SCN-001": scenarioStatusUnlinked,
		"AAA-SCN-002": scenarioStatusNotRun,
		"AAA-SCN-003": scenarioStatusPassed,
		"AAA-SCN-004": scenarioStatusFailed,
		"AAA-SCN-005": scenarioStatusNotRun,
		"AAA-SCN-006": scenarioStatusFailed,
		"AAA-SCN-007": scenarioStatusNotRun,
	}
	if got := scenarioStatuses(report, "a.md"); !reflect.DeepEqual(got, want) {
		t.Fatalf("statuses = %v, want %v", got, want)
	}
	if got, want := report.Summary, (scenarioSummary{Specs: 1, Scenarios: 7, Passed: 1, NotRun: 3, NotExecuted: 3, Unlinked: 1, Failed: 2}); got != want {
		t.Fatalf("summary = %+v, want %+v", got, want)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-074
func TestLoadGoTestJSONFailsTopLevelTestWhenSubtestFails(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"events.json": strings.Join([]string{
			`{"Action":"run","Package":"p","Test":"TestParent"}`,
			`{"Action":"pass","Package":"p","Test":"TestParent/ok"}`,
			`{"Action":"fail","Package":"p","Test":"TestParent/bad"}`,
			`{"Action":"pass","Package":"p","Test":"TestParent"}`,
			`{"Action":"pass","Package":"p","Test":"TestOnlySubtestPass/ok"}`,
			`{"Action":"fail","Package":"p","Test":"TestTruncated/bad"}`,
			`{"Action":"fail","Package":"p"}`,
		}, "\n"),
	})
	results := newScenarioResults()

	if err := loadGoTestJSON(filepath.Join(root, "events.json"), results); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{"p.TestParent": testResultFail, "p.TestTruncated": testResultFail}
	if got := outcomeStatuses(results); !reflect.DeepEqual(got, want) {
		t.Fatalf("results = %v, want %v", got, want)
	}
	if _, executed := results.packages["p"]; !executed || len(results.packages) != 1 {
		t.Fatalf("executed packages = %v, want only p", results.packages)
	}
}

func TestLoadScenarioResultsReadsJSONLinesAndRejectsBadRecords(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"good.jsonl":    "{\"test\":\"tests/x.py#scenario_a\",\"status\":\"pass\"}\n\n{\"test\":\"tests/x.py#scenario_a\",\"status\":\"fail\"}\n{\"test\":\"tests/x.py#scenario_b\",\"status\":\"skip\"}\n",
		"status.jsonl":  "{\"test\":\"tests/x.py#a\",\"status\":\"ok\"}\n",
		"field.jsonl":   "{\"test\":\"tests/x.py#a\",\"status\":\"pass\",\"extra\":1}\n",
		"missing.jsonl": "{\"status\":\"pass\"}\n",
	})
	results := newScenarioResults()

	if err := loadScenarioResults(filepath.Join(root, "good.jsonl"), results); err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"tests/x.py#scenario_a": testResultFail, "tests/x.py#scenario_b": testResultSkip}; !reflect.DeepEqual(outcomeStatuses(results), want) {
		t.Fatalf("results = %v, want %v", outcomeStatuses(results), want)
	}
	for name, wantError := range map[string]string{"status.jsonl": "not pass, fail or skip", "field.jsonl": "unknown field", "missing.jsonl": "test is required"} {
		if err := loadScenarioResults(filepath.Join(root, name), newScenarioResults()); err == nil || !strings.Contains(err.Error(), wantError) {
			t.Fatalf("%s error = %v, want %q", name, err, wantError)
		}
	}
}

// Traceability: PRD TEST-10.
// Scenarios: performance-and-verification.md:PRF-SCN-072, performance-and-verification.md:PRF-SCN-074
func TestScenariosCommandExitCodeAndReportFiles(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"go.mod":          "module " + scenarioFixtureModule + "\n",
		"docs/specs/a.md": scenarioTable("AAA-SCN-001", "AAA-SCN-002"),
		"pkg/one_test.go": "package pkg\n\n// Scenarios: a.md:AAA-SCN-001\nfunc TestOne(t *testing.T) {}\n",
		"results.jsonl":   "{\"test\":\"" + scenarioFixtureModule + "/pkg.TestOne\",\"status\":\"pass\"}\n",
	})
	t.Chdir(root)

	if code := run([]string{"scenarios", "--results", "results.jsonl"}); code != 1 {
		t.Fatalf("exit code with unlinked scenario = %d, want 1", code)
	}
	var report scenarioReport
	data, err := os.ReadFile(filepath.Join(root, ".artifacts", "verification", "scenarios", "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Summary.Unlinked != 1 || report.Summary.Passed != 1 {
		t.Fatalf("report summary = %+v, want 1 unlinked and 1 passed", report.Summary)
	}
	markdown, err := os.ReadFile(filepath.Join(root, ".artifacts", "verification", "scenarios", "report.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"| AAA-SCN-001 | 통과 |", "| AAA-SCN-002 | 미연결 | - |"} {
		if !strings.Contains(string(markdown), want) {
			t.Fatalf("report.md missing %q:\n%s", want, markdown)
		}
	}

	if err := os.WriteFile(filepath.Join(root, "pkg", "one_test.go"), []byte("package pkg\n\n// Scenarios: a.md:AAA-SCN-001, a.md:AAA-SCN-002\nfunc TestOne(t *testing.T) {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"scenarios"}); code != 0 {
		t.Fatalf("exit code without results = %d, want 0 (all linked, 미실행)", code)
	}
	if code := run([]string{"scenarios", "--output", "out", "--results", "missing.jsonl"}); code != 1 {
		t.Fatalf("exit code with unreadable results = %d, want 1", code)
	}
	failure := "{\"test\":\"" + scenarioFixtureModule + "/pkg.TestOne\",\"status\":\"fail\"}\n"
	if err := os.WriteFile(filepath.Join(root, "results.jsonl"), []byte(failure), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"scenarios", "--output", "out", "--results", "results.jsonl"}); code != 1 {
		t.Fatalf("exit code with failed test = %d, want 1", code)
	}
	if _, err := os.Stat(filepath.Join(root, "out", "report.md")); err != nil {
		t.Fatalf("custom output directory report: %v", err)
	}
}

func TestCollectTestMetadataIgnoresScenarioLines(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"cmd/x/x_test.go": "package x\n\n" +
			"// Traceability: PRD MQTT-10; docs/specs/a.md §1.\n" +
			"// Scenarios: a.md:MQTT-10-SCN-05, a.md:CLU-11-SCN-01\n" +
			"func TestA(t *testing.T) {}\n",
	})

	metadata, err := collectTestMetadata(root)
	if err != nil {
		t.Fatal(err)
	}

	if len(metadata) != 1 || !reflect.DeepEqual(metadata[0].Requirements, []string{"MQTT-10"}) {
		t.Fatalf("metadata = %+v, want only requirement MQTT-10", metadata)
	}
}

func outcomeStatuses(results *scenarioResults) map[string]string {
	statuses := make(map[string]string, len(results.outcomes))
	for key, outcome := range results.outcomes {
		statuses[key] = outcome.Status
	}
	return statuses
}

func scenarioReasons(report scenarioReport, spec string) map[string]string {
	reasons := make(map[string]string)
	for _, specReport := range report.Specs {
		if specReport.Spec != spec {
			continue
		}
		for _, scenario := range specReport.Scenarios {
			reasons[scenario.ID] = scenario.Reason
		}
	}
	return reasons
}

func goTestEvents(pkg string, actions map[string]string) string {
	names := make([]string, 0, len(actions))
	for name := range actions {
		names = append(names, name)
	}
	sort.Strings(names)
	lines := make([]string, 0, len(names)+1)
	for _, name := range names {
		lines = append(lines, `{"Action":"`+actions[name]+`","Package":"`+pkg+`","Test":"`+name+`"}`)
	}
	lines = append(lines, `{"Action":"pass","Package":"`+pkg+`"}`)
	return strings.Join(lines, "\n") + "\n"
}

// executionFixture links five scenarios: a passed and a skipped test and a test without an event in
// the executed package, plus a test in a package that is absent from the JSON and a Python function.
func executionFixture(t *testing.T) string {
	t.Helper()
	return writeScenarioFixture(t, map[string]string{
		"go.mod":          "module " + scenarioFixtureModule + "\n",
		"docs/specs/a.md": scenarioTable("AAA-SCN-001", "AAA-SCN-002", "AAA-SCN-003", "AAA-SCN-004", "AAA-SCN-005"),
		"pkg/one_test.go": "package pkg\n\n" +
			"// Scenarios: a.md:AAA-SCN-001\nfunc TestPassed(t *testing.T) {}\n\n" +
			"// Scenarios: a.md:AAA-SCN-002\nfunc TestSkipped(t *testing.T) {}\n\n" +
			"// Scenarios: a.md:AAA-SCN-003\nfunc TestMissing(t *testing.T) {}\n",
		"other/two_test.go":    "package other\n\n// Scenarios: a.md:AAA-SCN-004\nfunc TestOtherPackage(t *testing.T) {}\n",
		"tests/chaos/scene.py": "# Scenarios: a.md:AAA-SCN-005\ndef scenario_py():\n    pass\n",
		"results/go-test.json": goTestEvents(scenarioFixtureModule+"/pkg", map[string]string{"TestPassed": "pass", "TestSkipped": "skip"}),
	})
}

// Scenarios: performance-and-verification.md:PRF-SCN-110, performance-and-verification.md:PRF-SCN-111
func TestRequireExecutedFailsOnlyInsideExecutedScope(t *testing.T) {
	root := executionFixture(t)
	options := scenarioOptions{GoTestJSON: []string{"results/go-test.json"}, OutputDir: defaultScenarioOutputDir, RequireExecuted: true}

	report, err := runScenarios(root, options, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}

	wantStatuses := map[string]string{
		"AAA-SCN-001": scenarioStatusPassed,
		"AAA-SCN-002": scenarioStatusNotRun,
		"AAA-SCN-003": scenarioStatusNotRun,
		"AAA-SCN-004": scenarioStatusNotRun,
		"AAA-SCN-005": scenarioStatusNotRun,
	}
	wantReasons := map[string]string{
		"AAA-SCN-001": "",
		"AAA-SCN-002": scenarioReasonSkipped,
		"AAA-SCN-003": scenarioReasonNoResult,
		"AAA-SCN-004": scenarioReasonOutsideGate,
		"AAA-SCN-005": scenarioReasonOutsideGate,
	}
	if got := scenarioStatuses(report, "a.md"); !reflect.DeepEqual(got, wantStatuses) {
		t.Fatalf("statuses = %v, want %v", got, wantStatuses)
	}
	if got := scenarioReasons(report, "a.md"); !reflect.DeepEqual(got, wantReasons) {
		t.Fatalf("reasons = %v, want %v", got, wantReasons)
	}
	if got, want := report.Summary, (scenarioSummary{Specs: 1, Scenarios: 5, Passed: 1, NotRun: 4, NotExecuted: 2, OutsideGate: 2, RequireExecuted: true}); got != want {
		t.Fatalf("summary = %+v, want %+v", got, want)
	}
	if !report.Summary.hasProblems() {
		t.Fatal("execution check accepted a skipped test and a test without a result inside the executed scope")
	}
	if report.Inputs.ExecutedPackages != 1 {
		t.Fatalf("executed packages = %d, want 1", report.Inputs.ExecutedPackages)
	}
	markdown, err := os.ReadFile(filepath.Join(root, ".artifacts", "verification", "scenarios", "report.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"| AAA-SCN-002 | 미실행 (건너뜀) |", "| AAA-SCN-003 | 미실행 (결과 없음) |", "| AAA-SCN-004 | 미실행 (게이트 밖) |", "게이트 밖"} {
		if !strings.Contains(string(markdown), want) {
			t.Fatalf("report.md missing %q:\n%s", want, markdown)
		}
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-111, performance-and-verification.md:PRF-SCN-112
func TestOutOfScopeTestsAndLinkageCheckDoNotFailOnNotRun(t *testing.T) {
	root := executionFixture(t)
	if err := os.WriteFile(filepath.Join(root, "pkg", "one_test.go"), []byte("package pkg\n\n// Scenarios: a.md:AAA-SCN-001, a.md:AAA-SCN-002, a.md:AAA-SCN-003\nfunc TestPassed(t *testing.T) {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	outside, err := runScenarios(root, scenarioOptions{GoTestJSON: []string{"results/go-test.json"}, OutputDir: defaultScenarioOutputDir, RequireExecuted: true}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	linkage, err := runScenarios(root, scenarioOptions{OutputDir: defaultScenarioOutputDir}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}

	if outside.Summary.hasProblems() || outside.Summary.NotExecuted != 0 || outside.Summary.OutsideGate != 2 || outside.Summary.Passed != 3 {
		t.Fatalf("execution check summary = %+v, want only passed and 게이트 밖 scenarios", outside.Summary)
	}
	if linkage.Summary.hasProblems() || linkage.Summary.NotRun != 5 || linkage.Summary.OutsideGate != 5 {
		t.Fatalf("linkage check summary = %+v, want five 미실행 (게이트 밖) scenarios and no failure", linkage.Summary)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-110
func TestRequireExecutedScopeIncludesResultsFileKeys(t *testing.T) {
	root := writeScenarioFixture(t, map[string]string{
		"go.mod":          "module " + scenarioFixtureModule + "\n",
		"docs/specs/a.md": scenarioTable("AAA-SCN-001", "AAA-SCN-002", "AAA-SCN-003", "AAA-SCN-004", "AAA-SCN-005"),
		"tests/chaos/scene.py": "# Scenarios: a.md:AAA-SCN-001\ndef scenario_passed():\n    pass\n\n" +
			"# Scenarios: a.md:AAA-SCN-002\ndef scenario_skipped():\n    pass\n\n" +
			"# Scenarios: a.md:AAA-SCN-003\ndef scenario_unrecorded():\n    pass\n",
		"tools/x/run.go": "package x\n\n" +
			"// Scenarios: a.md:AAA-SCN-004\nfunc (run *Run) Execute() {}\n\n" +
			"// Scenarios: a.md:AAA-SCN-005\nfunc (run *Run) Observe() {}\n",
		"results/go-test.json": goTestEvents(scenarioFixtureModule+"/tools/x", map[string]string{"TestOther": "pass"}),
		"results/python.jsonl": strings.Join([]string{
			`{"test":"tests/chaos/scene.py#scenario_passed","status":"pass","commit":"abc123","environment":"EKS run-1","command":"python run.py","finishedAt":"2026-10-17T01:02:03Z"}`,
			`{"test":"tests/chaos/scene.py#scenario_skipped","status":"skip"}`,
			"{\"test\":\"" + scenarioFixtureModule + "/tools/x.Run.Execute\",\"status\":\"pass\"}",
		}, "\n") + "\n",
	})

	report, err := runScenarios(root, scenarioOptions{
		GoTestJSON: []string{"results/go-test.json"}, Results: []string{"results/python.jsonl"}, OutputDir: defaultScenarioOutputDir, RequireExecuted: true,
	}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}

	wantReasons := map[string]string{
		"AAA-SCN-001": "",
		"AAA-SCN-002": scenarioReasonSkipped,
		"AAA-SCN-003": scenarioReasonOutsideGate,
		"AAA-SCN-004": "",
		// A non-Test Go function is not reported by go test, so its package being executed is not enough.
		"AAA-SCN-005": scenarioReasonOutsideGate,
	}
	if got := scenarioReasons(report, "a.md"); !reflect.DeepEqual(got, wantReasons) {
		t.Fatalf("reasons = %v, want %v", got, wantReasons)
	}
	if report.Summary.NotExecuted != 1 || !report.Summary.hasProblems() {
		t.Fatalf("summary = %+v, want the recorded skip to fail the execution check", report.Summary)
	}
	if report.Inputs.RecordedResults != 3 {
		t.Fatalf("recorded results = %d, want 3", report.Inputs.RecordedResults)
	}
	markdown, err := os.ReadFile(filepath.Join(root, ".artifacts", "verification", "scenarios", "report.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"커밋 abc123", "환경 EKS run-1", "명령 python run.py", "종료 2026-10-17T01:02:03Z"} {
		if !strings.Contains(string(markdown), want) {
			t.Fatalf("report.md missing run record %q:\n%s", want, markdown)
		}
	}
	var linked scenarioLinkedTest
	for _, entry := range report.Specs[0].Scenarios {
		if entry.ID == "AAA-SCN-001" {
			linked = entry.Tests[0]
		}
	}
	if linked.Commit != "abc123" || linked.Environment != "EKS run-1" || !linked.InScope {
		t.Fatalf("linked test record = %+v, want commit, environment and in-scope", linked)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-110, performance-and-verification.md:PRF-SCN-112
func TestScenariosCommandRequireExecutedExitCodes(t *testing.T) {
	root := executionFixture(t)
	t.Chdir(root)

	if code := run([]string{"scenarios", "--require-executed"}); code != 1 {
		t.Fatalf("--require-executed without any result input: exit code = %d, want 1", code)
	}
	if code := run([]string{"scenarios", "--go-test-json", "results/go-test.json", "--require-executed"}); code != 1 {
		t.Fatalf("--require-executed with a skipped and a missing test in scope: exit code = %d, want 1", code)
	}
	if code := run([]string{"scenarios", "--go-test-json", "results/go-test.json"}); code != 0 {
		t.Fatalf("linkage check with the same input: exit code = %d, want 0", code)
	}
	if code := run([]string{"scenarios"}); code != 0 {
		t.Fatalf("linkage check without results: exit code = %d, want 0", code)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-110
func TestRunScenariosRejectsRequireExecutedWithoutInputs(t *testing.T) {
	root := executionFixture(t)

	_, err := runScenarios(root, scenarioOptions{OutputDir: defaultScenarioOutputDir, RequireExecuted: true}, time.Unix(0, 0))

	if err == nil || !strings.Contains(err.Error(), "--require-executed needs") {
		t.Fatalf("error = %v, want an input requirement", err)
	}
}
