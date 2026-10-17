package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	goparser "go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	defaultScenarioOutputDir = ".artifacts/verification/scenarios"
	scenarioSpecsDir         = "docs/specs"
	scenarioTableHeaderCell  = "시나리오 ID"

	scenarioStatusUnlinked = "미연결"
	scenarioStatusNotRun   = "미실행"
	scenarioStatusPassed   = "통과"
	scenarioStatusFailed   = "실패"

	testResultPass = "pass"
	testResultFail = "fail"
	testResultSkip = "skip"
	testResultNone = "none"
)

var (
	scenarioIDPattern     = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*$`)
	scenarioRefPattern    = regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9._-]*\.md):([A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*)$`)
	pythonDefinitionLine  = regexp.MustCompile(`^(?:(?:async\s+)?def|class)\s+([A-Za-z_][A-Za-z0-9_]*)\b`)
	pythonScenarioComment = regexp.MustCompile(`^#\s*Scenarios:(.*)$`)
	// Test and Fuzz functions are reported by name in go test -json. Benchmarks and examples are not
	// run by default, so they only count when a results file reports them.
	goReportedTestPattern  = regexp.MustCompile(`^(?:Test|Fuzz)(?:[^a-z].*)?$`)
	scenarioSkippedDirs    = map[string]struct{}{".git": {}, ".artifacts": {}, "node_modules": {}, "vendor": {}}
	scenarioSkippedSpecs   = map[string]struct{}{"README.md": {}, "AGENTS.md": {}}
	scenarioResultStatuses = map[string]struct{}{testResultPass: {}, testResultFail: {}, testResultSkip: {}}
)

type scenarioOptions struct {
	GoTestJSON      []string
	Results         []string
	OutputDir       string
	RequireExecuted bool
}

type scenarioDefinition struct {
	ID   string
	Line int
}

type scenarioSpec struct {
	File      string
	Scenarios []scenarioDefinition
}

type scenarioTest struct {
	Key  string
	Name string
	File string
	Line int
	// Package is the Go import path whose `go test -json` events can report this test. It is empty
	// for tests that go test does not report by name, such as Python functions and non-Test Go functions.
	Package string
}

type scenarioRef struct {
	Spec     string
	ID       string
	Test     scenarioTest
	Location string
}

type scenarioProblem struct {
	Location string
	Message  string
}

func (problem scenarioProblem) String() string {
	return problem.Location + ": " + problem.Message
}

type scenarioSummary struct {
	Specs     int `json:"specs"`
	Scenarios int `json:"scenarios"`
	Passed    int `json:"passed"`
	// NotRun counts every 미실행 scenario. NotExecuted is the part whose tests belong to the executed
	// scope but have no pass result. OutsideGate is the rest.
	NotRun          int  `json:"notRun"`
	NotExecuted     int  `json:"notExecuted"`
	OutsideGate     int  `json:"outsideGate"`
	Unlinked        int  `json:"unlinked"`
	Failed          int  `json:"failed"`
	Errors          int  `json:"errors"`
	RequireExecuted bool `json:"requireExecuted"`
}

// hasProblems reports whether the check fails. The linkage check fails on 오류, 미연결 and 실패.
// The execution check also fails on scenarios that belong to the executed scope without a pass result.
func (summary scenarioSummary) hasProblems() bool {
	return summary.Errors > 0 || summary.Unlinked > 0 || summary.Failed > 0 || (summary.RequireExecuted && summary.NotExecuted > 0)
}

type scenarioLinkedTest struct {
	Test        string `json:"test"`
	File        string `json:"file"`
	Line        int    `json:"line"`
	Result      string `json:"result"`
	InScope     bool   `json:"inScope"`
	Commit      string `json:"commit,omitempty"`
	Environment string `json:"environment,omitempty"`
	Command     string `json:"command,omitempty"`
	FinishedAt  string `json:"finishedAt,omitempty"`
}

type scenarioEntry struct {
	ID     string               `json:"id"`
	Status string               `json:"status"`
	Reason string               `json:"reason,omitempty"`
	Tests  []scenarioLinkedTest `json:"tests"`
}

type scenarioSpecReport struct {
	Spec      string          `json:"spec"`
	Scenarios []scenarioEntry `json:"scenarios"`
}

type scenarioProblemReport struct {
	Location string `json:"location"`
	Message  string `json:"message"`
}

type scenarioInputs struct {
	GoTestJSON       []string `json:"goTestJson"`
	Results          []string `json:"results"`
	RequireExecuted  bool     `json:"requireExecuted"`
	ExecutedPackages int      `json:"executedPackages"`
	RecordedResults  int      `json:"recordedResults"`
}

type scenarioReport struct {
	GeneratedAt string                  `json:"generatedAt"`
	Inputs      scenarioInputs          `json:"inputs"`
	Summary     scenarioSummary         `json:"summary"`
	Specs       []scenarioSpecReport    `json:"specs"`
	Problems    []scenarioProblemReport `json:"problems"`
}

// testOutcome is the merged result of one test with the optional run record fields.
type testOutcome struct {
	Status      string
	Commit      string
	Environment string
	Command     string
	FinishedAt  string
}

// scenarioResults holds test outcomes and the executed scope. A test belongs to the executed scope
// when its Go package appears in a go test JSON stream or its key appears in a results file.
type scenarioResults struct {
	outcomes map[string]testOutcome
	packages map[string]struct{}
	recorded map[string]struct{}
}

func newScenarioResults() *scenarioResults {
	return &scenarioResults{
		outcomes: make(map[string]testOutcome),
		packages: make(map[string]struct{}),
		recorded: make(map[string]struct{}),
	}
}

// merge keeps the strongest status for one test: fail over pass over skip. For equal strength the
// later record wins so the run record fields describe the latest run.
func (results *scenarioResults) merge(key string, outcome testOutcome) {
	rank := map[string]int{"": 0, testResultSkip: 1, testResultPass: 2, testResultFail: 3}
	if rank[outcome.Status] >= rank[results.outcomes[key].Status] {
		results.outcomes[key] = outcome
	}
}

func (results *scenarioResults) inScope(test scenarioTest) bool {
	if _, recorded := results.recorded[test.Key]; recorded {
		return true
	}
	if test.Package == "" {
		return false
	}
	_, executed := results.packages[test.Package]
	return executed
}

// runScenarios builds the scenario linkage and execution report and writes report.md and report.json.
// The returned error covers operational failures only. Metadata, linkage and result problems are
// reported through the summary so the caller decides the exit code.
func runScenarios(root string, options scenarioOptions, now time.Time) (scenarioReport, error) {
	if options.RequireExecuted && len(options.GoTestJSON) == 0 && len(options.Results) == 0 {
		return scenarioReport{}, errors.New("--require-executed needs --go-test-json or --results")
	}
	specs, problems, err := parseScenarioSpecs(root)
	if err != nil {
		return scenarioReport{}, err
	}
	refs, testProblems, err := scanScenarioTests(root)
	if err != nil {
		return scenarioReport{}, err
	}
	problems = append(problems, testProblems...)

	results := newScenarioResults()
	for _, path := range options.GoTestJSON {
		if err := loadGoTestJSON(resolveScenarioPath(root, path), results); err != nil {
			return scenarioReport{}, err
		}
	}
	for _, path := range options.Results {
		if err := loadScenarioResults(resolveScenarioPath(root, path), results); err != nil {
			return scenarioReport{}, err
		}
	}

	report := buildScenarioReport(specs, refs, problems, results, options.RequireExecuted, now)
	report.Inputs = scenarioInputs{
		GoTestJSON:       nonNilStrings(options.GoTestJSON),
		Results:          nonNilStrings(options.Results),
		RequireExecuted:  options.RequireExecuted,
		ExecutedPackages: len(results.packages),
		RecordedResults:  len(results.recorded),
	}
	if err := writeScenarioReport(resolveScenarioPath(root, options.OutputDir), report); err != nil {
		return scenarioReport{}, err
	}
	return report, nil
}

func resolveScenarioPath(root, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}

const scenarioListedLimit = 20

// printScenarioFindings lists metadata errors, failed scenarios, scenarios that the execution check
// rejects and the first unlinked scenarios.
func printScenarioFindings(out io.Writer, report scenarioReport) {
	for _, problem := range report.Problems {
		fmt.Fprintf(out, "scenarios: 오류 %s: %s\n", problem.Location, problem.Message)
	}
	unlinked, notExecuted := 0, 0
	for _, spec := range report.Specs {
		for _, scenario := range spec.Scenarios {
			switch {
			case scenario.Status == scenarioStatusFailed:
				fmt.Fprintf(out, "scenarios: 실패 %s:%s (%s)\n", spec.Spec, scenario.ID, joinLinkedTests(scenario.Tests, testResultFail))
			case scenario.Status == scenarioStatusNotRun && report.Summary.RequireExecuted && scenario.Reason != scenarioReasonOutsideGate:
				notExecuted++
				if notExecuted <= scenarioListedLimit {
					fmt.Fprintf(out, "scenarios: 미실행(%s) %s:%s (%s)\n", scenario.Reason, spec.Spec, scenario.ID, joinLinkedTests(scenario.Tests, ""))
				}
			case scenario.Status == scenarioStatusUnlinked:
				unlinked++
				if unlinked <= scenarioListedLimit {
					fmt.Fprintf(out, "scenarios: 미연결 %s:%s\n", spec.Spec, scenario.ID)
				}
			}
		}
	}
	if notExecuted > scenarioListedLimit {
		fmt.Fprintf(out, "scenarios: 미실행 %d개 더 있다. 전체 목록은 report.md를 본다\n", notExecuted-scenarioListedLimit)
	}
	if unlinked > scenarioListedLimit {
		fmt.Fprintf(out, "scenarios: 미연결 %d개 더 있다. 전체 목록은 report.md를 본다\n", unlinked-scenarioListedLimit)
	}
}

// joinLinkedTests names the linked tests with the given result, or the in-scope tests without a pass
// result when result is empty.
func joinLinkedTests(tests []scenarioLinkedTest, result string) string {
	names := make([]string, 0, len(tests))
	for _, test := range tests {
		if (result != "" && test.Result == result) || (result == "" && test.InScope && test.Result != testResultPass) {
			names = append(names, test.Test)
		}
	}
	return strings.Join(names, ", ")
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func parseScenarioSpecs(root string) ([]scenarioSpec, []scenarioProblem, error) {
	directory := filepath.Join(root, filepath.FromSlash(scenarioSpecsDir))
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, nil, fmt.Errorf("read spec directory %s: %w", scenarioSpecsDir, err)
	}
	var specs []scenarioSpec
	var problems []scenarioProblem
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".md") {
			continue
		}
		if _, skipped := scenarioSkippedSpecs[name]; skipped {
			continue
		}
		data, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return nil, nil, fmt.Errorf("read spec %s: %w", name, err)
		}
		spec, specProblems := parseScenarioTables(name, string(data))
		specs = append(specs, spec)
		problems = append(problems, specProblems...)
	}
	sort.Slice(specs, func(left, right int) bool { return specs[left].File < specs[right].File })
	return specs, problems, nil
}

// parseScenarioTables collects the IDs of every table whose header starts with "시나리오 ID".
// Tables inside code fences and tables with another first header cell are ignored.
func parseScenarioTables(file, content string) (scenarioSpec, []scenarioProblem) {
	spec := scenarioSpec{File: file}
	var problems []scenarioProblem
	seen := make(map[string]int)
	inFence := false
	inScenarioTable := false
	for index, raw := range strings.Split(content, "\n") {
		lineNumber := index + 1
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "```") || strings.HasPrefix(line, "~~~") {
			inFence = !inFence
			inScenarioTable = false
			continue
		}
		if inFence {
			continue
		}
		if !strings.HasPrefix(line, "|") {
			inScenarioTable = false
			continue
		}
		cells := splitPRDTableRow(line)
		if !inScenarioTable {
			inScenarioTable = cells[0] == scenarioTableHeaderCell
			continue
		}
		if isTableSeparatorRow(cells) {
			continue
		}
		location := fmt.Sprintf("%s/%s:%d", scenarioSpecsDir, file, lineNumber)
		id := cells[0]
		if !scenarioIDPattern.MatchString(id) {
			problems = append(problems, scenarioProblem{Location: location, Message: fmt.Sprintf("invalid scenario ID %q (use the bare ID without backticks or links)", id)})
			continue
		}
		if first, duplicate := seen[id]; duplicate {
			problems = append(problems, scenarioProblem{Location: location, Message: fmt.Sprintf("duplicate scenario ID %s (first defined at line %d)", id, first)})
			continue
		}
		seen[id] = lineNumber
		spec.Scenarios = append(spec.Scenarios, scenarioDefinition{ID: id, Line: lineNumber})
	}
	return spec, problems
}

func isTableSeparatorRow(cells []string) bool {
	for _, cell := range cells {
		if strings.Trim(cell, "-: ") != "" {
			return false
		}
	}
	return true
}

func scanScenarioTests(root string) ([]scenarioRef, []scenarioProblem, error) {
	var refs []scenarioRef
	var problems []scenarioProblem
	modules := newGoModuleResolver(root)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if _, skipped := scenarioSkippedDirs[entry.Name()]; skipped {
				return filepath.SkipDir
			}
			return nil
		}
		relative := relativePath(root, path)
		switch {
		case strings.HasSuffix(entry.Name(), ".go"):
			data, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("read %s: %w", relative, err)
			}
			if !bytes.Contains(data, []byte("Scenarios:")) {
				return nil
			}
			found, foundProblems, err := scanGoScenarios(path, relative, data, modules)
			if err != nil {
				return err
			}
			refs = append(refs, found...)
			problems = append(problems, foundProblems...)
		case strings.HasSuffix(entry.Name(), ".py") && strings.HasPrefix(relative, "tests/"):
			data, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("read %s: %w", relative, err)
			}
			found, foundProblems := scanPythonScenarioTests(relative, string(data))
			refs = append(refs, found...)
			problems = append(problems, foundProblems...)
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("scan test sources: %w", err)
	}
	return refs, problems, nil
}

// scanGoScenarios binds each "Scenarios:" doc-comment line to the function or method it documents.
// Test, Benchmark, Fuzz and Example functions get results from go test JSON. Other functions get
// results from --results records. A Scenarios comment attached to anything else is a problem.
func scanGoScenarios(path, relative string, source []byte, modules *goModuleResolver) ([]scenarioRef, []scenarioProblem, error) {
	fileSet := token.NewFileSet()
	parsed, err := goparser.ParseFile(fileSet, path, source, goparser.ParseComments)
	if err != nil {
		return nil, []scenarioProblem{{Location: relative, Message: fmt.Sprintf("parse Go source: %v", err)}}, nil
	}
	importPath, err := modules.importPath(filepath.Dir(path))
	if err != nil {
		return nil, nil, err
	}
	attached := make(map[*ast.CommentGroup]scenarioTest)
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Doc == nil {
			continue
		}
		name := function.Name.Name
		goTestPackage := ""
		if function.Recv != nil {
			name = receiverTypeName(function.Recv) + "." + name
		} else if strings.HasSuffix(relative, "_test.go") && goReportedTestPattern.MatchString(name) {
			goTestPackage = importPath
		}
		attached[function.Doc] = scenarioTest{
			Key:     importPath + "." + name,
			Name:    name,
			File:    relative,
			Line:    fileSet.Position(function.Pos()).Line,
			Package: goTestPackage,
		}
	}
	var refs []scenarioRef
	var problems []scenarioProblem
	for _, group := range parsed.Comments {
		test, isTestDoc := attached[group]
		for _, comment := range group.List {
			firstLine := fileSet.Position(comment.Pos()).Line
			for offset, text := range commentLines(comment.Text) {
				arguments, ok := scenarioMarker(text)
				if !ok {
					continue
				}
				location := fmt.Sprintf("%s:%d", relative, firstLine+offset)
				if !isTestDoc {
					problems = append(problems, scenarioProblem{Location: location, Message: "Scenarios: must be a line of the doc comment directly above a func declaration"})
					continue
				}
				found, parseProblems := parseScenarioRefs(arguments, location, test)
				refs = append(refs, found...)
				problems = append(problems, parseProblems...)
			}
		}
	}
	return refs, problems, nil
}

// receiverTypeName returns the receiver type name without pointer or type parameters.
func receiverTypeName(receiver *ast.FieldList) string {
	if receiver == nil || len(receiver.List) == 0 {
		return ""
	}
	expression := receiver.List[0].Type
	for {
		switch typed := expression.(type) {
		case *ast.StarExpr:
			expression = typed.X
		case *ast.ParenExpr:
			expression = typed.X
		case *ast.IndexExpr:
			expression = typed.X
		case *ast.IndexListExpr:
			expression = typed.X
		case *ast.Ident:
			return typed.Name
		default:
			return "?"
		}
	}
}

// commentLines returns the text lines of one Go comment without its comment markers.
func commentLines(text string) []string {
	switch {
	case strings.HasPrefix(text, "//"):
		return []string{strings.TrimSpace(strings.TrimPrefix(text, "//"))}
	case strings.HasPrefix(text, "/*"):
		body := strings.TrimSuffix(strings.TrimPrefix(text, "/*"), "*/")
		lines := strings.Split(body, "\n")
		for index := range lines {
			lines[index] = strings.TrimSpace(lines[index])
		}
		return lines
	}
	return nil
}

func scenarioMarker(text string) (string, bool) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "Scenarios:") {
		return "", false
	}
	return strings.TrimSpace(strings.TrimPrefix(text, "Scenarios:")), true
}

// scanPythonScenarioTests binds each "# Scenarios:" comment to the next def line. Other comment
// lines and decorators may sit between the marker and the def.
func scanPythonScenarioTests(relative, content string) ([]scenarioRef, []scenarioProblem) {
	type pendingMarker struct {
		line      int
		arguments string
	}
	var refs []scenarioRef
	var problems []scenarioProblem
	var pending []pendingMarker
	decoratorDepth := 0
	for index, raw := range strings.Split(content, "\n") {
		lineNumber := index + 1
		line := strings.TrimSpace(raw)
		if decoratorDepth > 0 {
			decoratorDepth += strings.Count(line, "(") + strings.Count(line, "[") + strings.Count(line, "{")
			decoratorDepth -= strings.Count(line, ")") + strings.Count(line, "]") + strings.Count(line, "}")
			continue
		}
		if matches := pythonScenarioComment.FindStringSubmatch(line); matches != nil {
			pending = append(pending, pendingMarker{line: lineNumber, arguments: strings.TrimSpace(matches[1])})
			continue
		}
		if len(pending) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "@") {
			decoratorDepth = strings.Count(line, "(") + strings.Count(line, "[") + strings.Count(line, "{") -
				strings.Count(line, ")") - strings.Count(line, "]") - strings.Count(line, "}")
			if decoratorDepth < 0 {
				decoratorDepth = 0
			}
			continue
		}
		if matches := pythonDefinitionLine.FindStringSubmatch(line); matches != nil {
			test := scenarioTest{Key: relative + "#" + matches[1], Name: matches[1], File: relative, Line: lineNumber}
			for _, marker := range pending {
				found, parseProblems := parseScenarioRefs(marker.arguments, fmt.Sprintf("%s:%d", relative, marker.line), test)
				refs = append(refs, found...)
				problems = append(problems, parseProblems...)
			}
		} else {
			for _, marker := range pending {
				problems = append(problems, scenarioProblem{Location: fmt.Sprintf("%s:%d", relative, marker.line), Message: "Scenarios: must be a comment directly above a def or class"})
			}
		}
		pending = nil
	}
	for _, marker := range pending {
		problems = append(problems, scenarioProblem{Location: fmt.Sprintf("%s:%d", relative, marker.line), Message: "Scenarios: must be a comment directly above a def or class"})
	}
	return refs, problems
}

func parseScenarioRefs(arguments, location string, test scenarioTest) ([]scenarioRef, []scenarioProblem) {
	if arguments == "" {
		return nil, []scenarioProblem{{Location: location, Message: "malformed Scenarios: no <spec>.md:<ID> reference"}}
	}
	var refs []scenarioRef
	var problems []scenarioProblem
	seen := make(map[string]struct{})
	for _, item := range strings.Split(arguments, ",") {
		item = strings.TrimSpace(item)
		matches := scenarioRefPattern.FindStringSubmatch(item)
		if matches == nil {
			problems = append(problems, scenarioProblem{Location: location, Message: fmt.Sprintf("malformed scenario reference %q (want <spec>.md:<ID>)", item)})
			continue
		}
		if _, duplicate := seen[item]; duplicate {
			continue
		}
		seen[item] = struct{}{}
		refs = append(refs, scenarioRef{Spec: matches[1], ID: matches[2], Test: test, Location: location})
	}
	return refs, problems
}

// goModuleResolver maps a test directory to the import path of its package using the nearest go.mod.
type goModuleResolver struct {
	root  string
	cache map[string]goModule
}

type goModule struct {
	Dir  string
	Path string
}

func newGoModuleResolver(root string) *goModuleResolver {
	return &goModuleResolver{root: root, cache: make(map[string]goModule)}
}

func (resolver *goModuleResolver) importPath(directory string) (string, error) {
	module, err := resolver.module(directory)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(module.Dir, directory)
	if err != nil {
		return "", fmt.Errorf("resolve import path of %s: %w", directory, err)
	}
	if relative == "." {
		return module.Path, nil
	}
	return module.Path + "/" + filepath.ToSlash(relative), nil
}

func (resolver *goModuleResolver) module(directory string) (goModule, error) {
	if module, cached := resolver.cache[directory]; cached {
		return module, nil
	}
	data, err := os.ReadFile(filepath.Join(directory, "go.mod"))
	switch {
	case err == nil:
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "module" {
				module := goModule{Dir: directory, Path: strings.Trim(fields[1], `"`)}
				resolver.cache[directory] = module
				return module, nil
			}
		}
		return goModule{}, fmt.Errorf("%s/go.mod has no module line", directory)
	case !errors.Is(err, os.ErrNotExist):
		return goModule{}, fmt.Errorf("read %s/go.mod: %w", directory, err)
	}
	parent := filepath.Dir(directory)
	if directory == resolver.root || parent == directory {
		module := goModule{Dir: resolver.root, Path: modulePath}
		resolver.cache[directory] = module
		return module, nil
	}
	module, err := resolver.module(parent)
	if err != nil {
		return goModule{}, err
	}
	resolver.cache[directory] = module
	return module, nil
}

// loadGoTestJSON merges the top-level test results of one `go test -json` stream into results and
// records every package that appears in the stream as executed.
// A failing subtest fails its top-level test even when the stream lacks the parent's own event.
func loadGoTestJSON(path string, results *scenarioResults) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read go test JSON %s: %w", path, err)
	}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var event struct {
			Action  string `json:"Action"`
			Package string `json:"Package"`
			Test    string `json:"Test"`
		}
		if err := decoder.Decode(&event); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("parse go test JSON %s: %w", path, err)
		}
		if event.Package != "" {
			results.packages[event.Package] = struct{}{}
		}
		if event.Test == "" || event.Package == "" {
			continue
		}
		status := event.Action
		if _, known := scenarioResultStatuses[status]; !known {
			continue
		}
		top, _, isSubtest := strings.Cut(event.Test, "/")
		if isSubtest && status != testResultFail {
			continue
		}
		results.merge(event.Package+"."+top, testOutcome{Status: status})
	}
}

// loadScenarioResults reads JSON Lines of
// {"test":"<key>","status":"pass|fail|skip","commit":"","environment":"","command":"","finishedAt":""}.
// Only test and status are required. Every key in the file belongs to the executed scope.
func loadScenarioResults(path string, results *scenarioResults) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read results %s: %w", path, err)
	}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	for index, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		var record struct {
			Test        string `json:"test"`
			Status      string `json:"status"`
			Commit      string `json:"commit"`
			Environment string `json:"environment"`
			Command     string `json:"command"`
			FinishedAt  string `json:"finishedAt"`
		}
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return fmt.Errorf("parse results %s:%d: %w", path, index+1, err)
		}
		if record.Test == "" {
			return fmt.Errorf("parse results %s:%d: test is required", path, index+1)
		}
		if _, known := scenarioResultStatuses[record.Status]; !known {
			return fmt.Errorf("parse results %s:%d: status %q is not pass, fail or skip", path, index+1, record.Status)
		}
		results.recorded[record.Test] = struct{}{}
		results.merge(record.Test, testOutcome{Status: record.Status, Commit: record.Commit, Environment: record.Environment, Command: record.Command, FinishedAt: record.FinishedAt})
	}
	return nil
}

func buildScenarioReport(specs []scenarioSpec, refs []scenarioRef, problems []scenarioProblem, results *scenarioResults, requireExecuted bool, now time.Time) scenarioReport {
	defined := make(map[string]map[string]struct{}, len(specs))
	for _, spec := range specs {
		ids := make(map[string]struct{}, len(spec.Scenarios))
		for _, scenario := range spec.Scenarios {
			ids[scenario.ID] = struct{}{}
		}
		defined[spec.File] = ids
	}
	linked := make(map[string]map[string]scenarioTest)
	for _, ref := range refs {
		ids, knownSpec := defined[ref.Spec]
		if !knownSpec {
			problems = append(problems, scenarioProblem{Location: ref.Location, Message: fmt.Sprintf("unknown spec file %s (no such file under %s)", ref.Spec, scenarioSpecsDir)})
			continue
		}
		if _, knownID := ids[ref.ID]; !knownID {
			problems = append(problems, scenarioProblem{Location: ref.Location, Message: fmt.Sprintf("unknown scenario %s in %s", ref.ID, ref.Spec)})
			continue
		}
		key := ref.Spec + "\x00" + ref.ID
		if linked[key] == nil {
			linked[key] = make(map[string]scenarioTest)
		}
		linked[key][ref.Test.Key] = ref.Test
	}

	report := scenarioReport{
		GeneratedAt: now.UTC().Format(time.RFC3339),
		Specs:       make([]scenarioSpecReport, 0, len(specs)),
	}
	report.Summary.Specs = len(specs)
	report.Summary.RequireExecuted = requireExecuted
	for _, spec := range specs {
		specReport := scenarioSpecReport{Spec: spec.File, Scenarios: make([]scenarioEntry, 0, len(spec.Scenarios))}
		for _, scenario := range spec.Scenarios {
			tests := linked[spec.File+"\x00"+scenario.ID]
			entry := scenarioEntry{ID: scenario.ID, Tests: make([]scenarioLinkedTest, 0, len(tests))}
			for _, test := range tests {
				outcome := results.outcomes[test.Key]
				status := outcome.Status
				if status == "" {
					status = testResultNone
				}
				entry.Tests = append(entry.Tests, scenarioLinkedTest{
					Test: test.Key, File: test.File, Line: test.Line, Result: status, InScope: results.inScope(test),
					Commit: outcome.Commit, Environment: outcome.Environment, Command: outcome.Command, FinishedAt: outcome.FinishedAt,
				})
			}
			sort.Slice(entry.Tests, func(left, right int) bool { return entry.Tests[left].Test < entry.Tests[right].Test })
			entry.Status, entry.Reason = evaluateScenario(entry.Tests)
			switch entry.Status {
			case scenarioStatusUnlinked:
				report.Summary.Unlinked++
			case scenarioStatusNotRun:
				report.Summary.NotRun++
				if entry.Reason == scenarioReasonOutsideGate {
					report.Summary.OutsideGate++
				} else {
					report.Summary.NotExecuted++
				}
			case scenarioStatusPassed:
				report.Summary.Passed++
			case scenarioStatusFailed:
				report.Summary.Failed++
			}
			report.Summary.Scenarios++
			specReport.Scenarios = append(specReport.Scenarios, entry)
		}
		report.Specs = append(report.Specs, specReport)
	}
	sort.SliceStable(problems, func(left, right int) bool {
		if problems[left].Location != problems[right].Location {
			return problems[left].Location < problems[right].Location
		}
		return problems[left].Message < problems[right].Message
	})
	report.Problems = make([]scenarioProblemReport, 0, len(problems))
	for _, problem := range problems {
		report.Problems = append(report.Problems, scenarioProblemReport{Location: problem.Location, Message: problem.Message})
	}
	report.Summary.Errors = len(problems)
	return report
}

const (
	scenarioReasonNoResult    = "결과 없음"
	scenarioReasonSkipped     = "건너뜀"
	scenarioReasonOutsideGate = "게이트 밖"
)

// evaluateScenario applies the precedence 미연결, 실패, 미실행, 통과. A 미실행 scenario carries the
// reason 결과 없음 or 건너뜀 when a linked test belongs to the executed scope without a pass result,
// otherwise 게이트 밖.
func evaluateScenario(tests []scenarioLinkedTest) (string, string) {
	if len(tests) == 0 {
		return scenarioStatusUnlinked, ""
	}
	allPassed, noResult, skipped := true, false, false
	for _, test := range tests {
		switch test.Result {
		case testResultFail:
			return scenarioStatusFailed, ""
		case testResultPass:
			continue
		case testResultSkip:
			skipped = true
		default:
			noResult = noResult || test.InScope
		}
		allPassed = false
	}
	switch {
	case allPassed:
		return scenarioStatusPassed, ""
	case noResult:
		return scenarioStatusNotRun, scenarioReasonNoResult
	case skipped:
		return scenarioStatusNotRun, scenarioReasonSkipped
	}
	return scenarioStatusNotRun, scenarioReasonOutsideGate
}

func writeScenarioReport(directory string, report scenarioReport) error {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create scenario report directory: %w", err)
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode scenario report: %w", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "report.json"), append(encoded, '\n'), 0o644); err != nil {
		return fmt.Errorf("write scenario report.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "report.md"), []byte(renderScenarioMarkdown(report)), 0o644); err != nil {
		return fmt.Errorf("write scenario report.md: %w", err)
	}
	return nil
}

func renderScenarioMarkdown(report scenarioReport) string {
	var out strings.Builder
	summary := report.Summary
	out.WriteString("# 시나리오 커버리지·결과 보고서\n\n")
	out.WriteString("`go run ./tools/verify scenarios`가 생성했다. 기준은 `docs/conventions/test-conventions.md`다.\n\n")
	fmt.Fprintf(&out, "- 검사: %s\n", describeScenarioCheck(summary.RequireExecuted))
	fmt.Fprintf(&out, "- 생성 시각: %s\n", report.GeneratedAt)
	fmt.Fprintf(&out, "- go test JSON 입력: %s\n", describeScenarioInputs(report.Inputs.GoTestJSON))
	fmt.Fprintf(&out, "- 결과 JSON Lines 입력: %s\n", describeScenarioInputs(report.Inputs.Results))
	fmt.Fprintf(&out, "- 실행 범위: Go 패키지 %d개, 결과 파일 테스트 %d개\n\n", report.Inputs.ExecutedPackages, report.Inputs.RecordedResults)
	out.WriteString("## 요약\n\n| 항목 | 개수 |\n| --- | --- |\n")
	fmt.Fprintf(&out, "| 스펙 | %d |\n| 시나리오 | %d |\n| 통과 | %d |\n| 실패 | %d |\n| 미실행 | %d |\n| 미실행: 실행 범위 안(%s, %s) | %d |\n| 미실행: %s | %d |\n| 미연결 | %d |\n| 오류 | %d |\n\n",
		summary.Specs, summary.Scenarios, summary.Passed, summary.Failed, summary.NotRun,
		scenarioReasonNoResult, scenarioReasonSkipped, summary.NotExecuted, scenarioReasonOutsideGate, summary.OutsideGate,
		summary.Unlinked, summary.Errors)
	if len(report.Problems) > 0 {
		out.WriteString("## 오류\n\n")
		for _, problem := range report.Problems {
			fmt.Fprintf(&out, "- `%s`: %s\n", problem.Location, problem.Message)
		}
		out.WriteString("\n")
	}
	for _, spec := range report.Specs {
		fmt.Fprintf(&out, "## %s\n\n", spec.Spec)
		if len(spec.Scenarios) == 0 {
			out.WriteString("시나리오 표가 없다.\n\n")
			continue
		}
		out.WriteString("| 시나리오 ID | 상태 | 연결 테스트 |\n| --- | --- | --- |\n")
		for _, scenario := range spec.Scenarios {
			tests := make([]string, 0, len(scenario.Tests))
			for _, test := range scenario.Tests {
				tests = append(tests, describeLinkedTest(test))
			}
			linkedText := "-"
			if len(tests) > 0 {
				linkedText = strings.Join(tests, "<br>")
			}
			status := scenario.Status
			if scenario.Reason != "" {
				status += " (" + scenario.Reason + ")"
			}
			fmt.Fprintf(&out, "| %s | %s | %s |\n", scenario.ID, status, linkedText)
		}
		out.WriteString("\n")
	}
	return out.String()
}

func describeScenarioCheck(requireExecuted bool) string {
	if requireExecuted {
		return "실행 검사(`--require-executed`): 오류·미연결·실패, 실행 범위 안의 미실행에서 실패한다"
	}
	return "연결 검사: 오류·미연결·실패에서 실패한다. 미실행은 실패로 세지 않는다"
}

func describeScenarioInputs(paths []string) string {
	if len(paths) == 0 {
		return "없음"
	}
	quoted := make([]string, 0, len(paths))
	for _, path := range paths {
		quoted = append(quoted, "`"+filepath.ToSlash(path)+"`")
	}
	return strings.Join(quoted, ", ")
}

// describeLinkedTest renders one linked test with its status and the optional run record fields.
func describeLinkedTest(test scenarioLinkedTest) string {
	text := fmt.Sprintf("`%s` (%s:%d): %s", test.Test, test.File, test.Line, describeTestResult(test))
	for _, field := range []struct{ label, value string }{
		{"커밋", test.Commit}, {"환경", test.Environment}, {"명령", test.Command}, {"종료", test.FinishedAt},
	} {
		if field.value != "" {
			text += " · " + field.label + " " + strings.ReplaceAll(field.value, "|", "\\|")
		}
	}
	return text
}

func describeTestResult(test scenarioLinkedTest) string {
	switch test.Result {
	case testResultPass:
		return "통과"
	case testResultFail:
		return "실패"
	case testResultSkip:
		return scenarioReasonSkipped
	}
	if test.InScope {
		return scenarioReasonNoResult
	}
	return scenarioReasonOutsideGate
}

// formatScenarioSummary renders the one-line summary printed by the subcommand.
func formatScenarioSummary(summary scenarioSummary) string {
	verdict := "PASS"
	if summary.hasProblems() {
		verdict = "FAIL"
	}
	check := "연결 검사"
	if summary.RequireExecuted {
		check = "실행 검사"
	}
	return fmt.Sprintf("scenarios: %s (%s; specs %d, scenarios %d, 통과 %d, 실패 %d, 미실행 %d [실행 범위 안 %d, 게이트 밖 %d], 미연결 %d, 오류 %d)",
		verdict, check, summary.Specs, summary.Scenarios, summary.Passed, summary.Failed, summary.NotRun, summary.NotExecuted, summary.OutsideGate, summary.Unlinked, summary.Errors)
}
