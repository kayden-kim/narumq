package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// Scenarios: performance-and-verification.md:PRF-SCN-078
func TestInstrumentBranchesPreservesLinesAndAddsOnlyMissingArms(t *testing.T) {
	source := []byte(`package sample
func sample(value bool, input chan int) {
	if value {
	}
	if value {
	} else {
	}
	switch value {
	case true:
	}
	switch value {
	case true:
	default:
	}
	select {
	case <-input:
	}
	select {
	case <-input:
	default:
	}
	for value && value {
		break
	}
	for range []int{} {
	}
	switch {
	}
	select {
	}
}
func _() {
	if true {
	}
}
`)
	instrumented, decisions, err := instrumentBranches("internal/sample.go", source)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(source, []byte{'\n'}) != bytes.Count(instrumented, []byte{'\n'}) {
		t.Fatal("instrumentation changed line count")
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), "sample.go", instrumented, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ifElse []bool
	var switchDefaults, selectDefaults []int
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.IfStmt:
			ifElse = append(ifElse, value.Else != nil)
		case *ast.SwitchStmt:
			count := 0
			for _, clause := range value.Body.List {
				if clause.(*ast.CaseClause).List == nil {
					count++
				}
			}
			switchDefaults = append(switchDefaults, count)
		case *ast.SelectStmt:
			count := 0
			for _, clause := range value.Body.List {
				if clause.(*ast.CommClause).Comm == nil {
					count++
				}
			}
			selectDefaults = append(selectDefaults, count)
		}
		return true
	})
	if !reflect.DeepEqual(ifElse, []bool{true, true, false}) || !reflect.DeepEqual(switchDefaults, []int{1, 1, 0}) || !reflect.DeepEqual(selectDefaults, []int{0, 1, 0}) {
		t.Fatalf("arm structure: if else %v, switch defaults %v, select defaults %v", ifElse, switchDefaults, selectDefaults)
	}
	var kinds []string
	for _, decision := range decisions {
		kinds = append(kinds, decision.Kind)
	}
	if !reflect.DeepEqual(kinds, []string{"if", "if", "switch", "switch", "select", "select"}) {
		t.Fatalf("decisions = %v, want only supported nonempty decisions outside func _", kinds)
	}
	second, repeated, err := instrumentBranches("internal/sample.go", instrumented)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(second, instrumented) || !reflect.DeepEqual(repeated, decisions) {
		t.Fatal("instrumentation of complete arms must be idempotent")
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-078
func TestLoadBranchManifestRejectsInvalidSchemaAndJSON(t *testing.T) {
	for _, value := range []struct {
		name, content, want string
	}{
		{"unknown", `{"schemaVersion":1,"files":[],"unknown":true}`, "unknown field"},
		{"trailing", `{"schemaVersion":1,"files":[]} {}`, "trailing JSON value"},
		{"malformedTrailing", `{"schemaVersion":1,"files":[]} invalid`, "trailing JSON"},
		{"version", `{"schemaVersion":2,"files":[]}`, "want 1"},
	} {
		t.Run(value.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest.json")
			if err := os.WriteFile(path, []byte(value.content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := loadBranchManifest(path); err == nil || !strings.Contains(err.Error(), value.want) {
				t.Fatalf("loadBranchManifest error = %v, want %s", err, value.want)
			}
		})
	}
}

func writeBranchTestFile(t *testing.T, root, path string, content []byte) {
	t.Helper()
	path = filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func runBranchTestCommand(t *testing.T, root, name string, args ...string) {
	t.Helper()
	command := exec.Command(name, args...)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, output)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-077, performance-and-verification.md:PRF-SCN-083
func TestBranchCoverageMatchesGoCoverBlocks(t *testing.T) {
	source := []byte(`package sample
var Literal = func(value bool) {
	if value {
		sink++
	}
}
var sink int
func Sample(value int, dynamic any, input chan int) {
	if value > 0 {
		sink++
	}
	if value == 1 {
		sink++
	} else if value == 2 {
		sink++
	} else {
		sink++
	}
	switch value {
	case 1:
		sink++
	case -2:
	case 3:
		sink++
	}
	switch dynamic.(type) {
	case int:
		sink++
	case string:
		sink++
	}
	select {
	case <-input:
		sink++
	default:
		sink++
	}
}
func _() {
	if true {
		sink++
	}
}
`)
	tests := []byte(`package sample
import "testing"
func TestSample(t *testing.T) {
	Sample(1, 1, make(chan int))
	Literal(true)
	if sink != 6 { t.Fatalf("sink = %d, want 6", sink) }
}
`)
	root, copyRoot := t.TempDir(), t.TempDir()
	path := "internal/sample/sample.go"
	instrumented, decisions, err := instrumentBranches(path, source)
	if err != nil {
		t.Fatal(err)
	}
	for _, decision := range decisions {
		if decision.Line >= 39 {
			t.Fatalf("func _ must not have a decision: %#v", decision)
		}
	}
	for _, dir := range []string{root, copyRoot} {
		writeBranchTestFile(t, dir, "go.mod", []byte("module narumq\n\ngo 1.26.5\n"))
		writeBranchTestFile(t, dir, "internal/sample/sample_test.go", tests)
	}
	writeBranchTestFile(t, root, path, source)
	writeBranchTestFile(t, copyRoot, path, instrumented)
	profile := filepath.Join(copyRoot, "c.out")
	runBranchTestCommand(t, copyRoot, "go", "test", "./...", "-count=1", "-covermode=atomic", "-coverpkg=./internal/...", "-coverprofile="+profile)
	policy := coveragePolicy{ProductionPatterns: []string{"./internal/..."}}
	data, err := parseCoverageProfile(root, profile, policy)
	if err != nil {
		t.Fatal(err)
	}
	manifest := branchManifest{SchemaVersion: 1, Files: []branchFile{{Path: path, Package: "narumq/internal/sample", SHA256: fmt.Sprintf("%x", sha256.Sum256(source)), Decisions: decisions}}}
	evaluation, err := evaluateBranchCoverage(root, manifest, data)
	if err != nil {
		t.Fatal(err)
	}
	var uncovered []string
	for _, outcome := range evaluation.Outcomes {
		if !outcome.Covered {
			uncovered = append(uncovered, outcome.ID)
		}
	}
	want := []string{
		path + ":3:2 if else",
		path + ":9:2 if else",
		path + ":12:2 if else",
		path + ":14:9 if then",
		path + ":14:9 if else",
		path + ":19:2 switch case@22:2",
		path + ":19:2 switch case@23:2",
		path + ":19:2 switch default",
		path + ":26:2 typeSwitch case@29:2",
		path + ":26:2 typeSwitch default",
		path + ":32:2 select case@33:2",
	}
	sort.Strings(uncovered)
	sort.Strings(want)
	if !reflect.DeepEqual(uncovered, want) {
		t.Fatalf("uncovered outcomes = %v, want %v", uncovered, want)
	}
	if evaluation.Overall.Covered != 6 || evaluation.Overall.Total != 17 {
		t.Fatalf("branch metric = %#v, want 6/17", evaluation.Overall)
	}
	originalProfile := filepath.Join(root, "c.out")
	runBranchTestCommand(t, root, "go", "test", "./...", "-count=1", "-covermode=atomic", "-coverpkg=./internal/...", "-coverprofile="+originalProfile)
	original, err := parseCoverageProfile(root, originalProfile, policy)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := calculateCoverage(original)
	after, _ := calculateCoverage(data)
	if before.Line.Total != after.Line.Total || before.Function.Total != after.Function.Total {
		t.Fatalf("coverage totals changed: original %#v, instrumented %#v", before, after)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-080
func TestEvaluateBranchCoverageRejectsStaleOrMismatchedManifest(t *testing.T) {
	root := t.TempDir()
	path := "internal/sample.go"
	source := []byte("package sample\nfunc Sample(value bool) {\n\tif value {\n\t}\n}\n")
	writeBranchTestFile(t, root, path, source)
	_, decisions, err := instrumentBranches(path, source)
	if err != nil {
		t.Fatal(err)
	}
	file := branchFile{Path: path, Package: "narumq/internal", SHA256: fmt.Sprintf("%x", sha256.Sum256(source)), Decisions: decisions}
	data := coverageData{Sources: map[string]sourceFile{path: {Path: path}}}
	for _, test := range []struct {
		name  string
		files []branchFile
		want  string
	}{
		{"stale", []branchFile{{Path: path, SHA256: "wrong"}}, "branch manifest is stale for internal/sample.go"},
		{"missing", nil, "missing [internal/sample.go] unexpected []"},
		{"unexpected", []branchFile{file, {Path: "internal/extra.go"}}, "missing [] unexpected [internal/extra.go]"},
		{"block", []branchFile{file}, "has no coverage block; instrumentation and profile do not match"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := evaluateBranchCoverage(root, branchManifest{SchemaVersion: 1, Files: test.files}, data)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("evaluateBranchCoverage error = %v, want %s", err, test.want)
			}
		})
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-081, performance-and-verification.md:PRF-SCN-087, performance-and-verification.md:PRF-SCN-091, performance-and-verification.md:PRF-SCN-093
func TestBranchRatchetDiffFloorAndRequirementShortfall(t *testing.T) {
	actual := coverageMetric{Covered: 80, Total: 100, Percent: 80}
	if issues := enforceBranchRatchet("overall", actual, 75, 0); len(issues) != 0 {
		t.Fatalf("ratchet rejected 80 >= 75: %v", issues)
	}
	shortfall, exists := branchRequirementShortfall("overall", actual, 85)
	if !exists || shortfall != "overall branch coverage 80.00% is below requirement 85.00%" {
		t.Fatalf("shortfall = %q, %v", shortfall, exists)
	}
	actual.Percent = 74
	if issues := enforceBranchRatchet("overall", actual, 75, 0); len(issues) != 1 || !strings.Contains(issues[0], "branch ratchet") {
		t.Fatalf("ratchet did not reject 74 < 75: %v", issues)
	}
	if issues := enforceBranchRatchet("empty", coverageMetric{}, 0, 0); len(issues) != 0 {
		t.Fatalf("zero-outcome package rejected: %v", issues)
	}

	actual.Percent = 79.61
	if issues := enforceBranchRatchet("overall", actual, 79.81, 0.20); len(issues) != 0 {
		t.Fatalf("ratchet rejected 79.61%% within tolerance: %v", issues)
	}
	actual.Percent = 79.60
	want := "overall branch ratchet 79.81% (tolerance 0.20%p) is above measured 79.60%"
	if issues := enforceBranchRatchet("overall", actual, 79.81, 0.20); len(issues) != 1 || issues[0] != want {
		t.Fatalf("ratchet issue = %v, want [%s]", issues, want)
	}
	changed := map[string]map[int]struct{}{"internal/sample.go": {3: {}}}
	values := diffBranchValues(changed, []branchOutcomeResult{
		{File: "internal/sample.go", DecisionLine: 3, Covered: true},
		{File: "internal/sample.go", DecisionLine: 3, Covered: false},
		{File: "internal/sample.go", DecisionLine: 4, Covered: false},
		{File: "internal/other.go", DecisionLine: 3, Covered: false},
	})
	if !reflect.DeepEqual(values, []bool{true, false}) {
		t.Fatalf("changed decision selection = %v", values)
	}

	// A real Git diff selects all ten decisions in a newly added production file.
	root := t.TempDir()
	writeBranchTestFile(t, root, "go.mod", []byte("module narumq\n\ngo 1.26.5\n"))
	runBranchTestCommand(t, root, "git", "init")
	runBranchTestCommand(t, root, "git", "add", "go.mod")
	runBranchTestCommand(t, root, "git", "-c", "user.name=Coverage", "-c", "user.email=coverage@example.invalid", "commit", "-m", "base")
	source := []byte("package sample\nfunc Sample(value bool) {\n" + strings.Repeat("\tif value {\n\t}\n", 10) + "}\n")
	path := "internal/sample.go"
	writeBranchTestFile(t, root, path, source)
	runBranchTestCommand(t, root, "git", "add", path)
	runBranchTestCommand(t, root, "git", "-c", "user.name=Coverage", "-c", "user.email=coverage@example.invalid", "commit", "-m", "decisions")
	_, decisions, err := instrumentBranches(path, source)
	if err != nil {
		t.Fatal(err)
	}
	manifest := branchManifest{SchemaVersion: 1, Files: []branchFile{{Path: path, Package: "narumq/internal", SHA256: fmt.Sprintf("%x", sha256.Sum256(source)), Decisions: decisions}}}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	writeBranchTestFile(t, root, "manifest.json", manifestJSON)
	var profile strings.Builder
	profile.WriteString("mode: atomic\n")
	for index, decision := range decisions {
		for arm, outcome := range decision.Outcomes {
			count := 1
			if index == 0 {
				count = 0
			}
			statements := 0
			if arm == 0 {
				statements = 1
			}
			fmt.Fprintf(&profile, "narumq/%s:%d.%d,%d.%d %d %d\n", path, outcome.Start.Line, outcome.Start.Column, outcome.End.Line, outcome.End.Column, statements, count)
		}
	}
	writeBranchTestFile(t, root, "coverage.out", []byte(profile.String()))
	policy := coveragePolicy{
		ProductionPatterns: []string{"./internal/..."},
		OverallFloor:       coverageFloor{Branch: 95},
		DiffFloor:          coverageFloor{Branch: 95},
	}
	evaluation, err := evaluateCoverage(context.Background(), root, filepath.Join(root, "coverage.out"), policy, filepath.Join(root, "manifest.json"), "HEAD~1")
	if err == nil || !strings.Contains(err.Error(), "diff branch coverage 90.00% is below 95.00%") {
		t.Fatalf("diff gate error = %v, want diff branch floor failure", err)
	}
	if evaluation == nil || evaluation.Report.Passed || evaluation.Report.Diff.Branch.Percent != 90 || len(evaluation.Report.BranchCoverage.RequirementShortfalls) != 1 {
		t.Fatalf("diff failure report = %#v", evaluation)
	}
	policy.DiffFloor.Branch = 90
	evaluation, err = evaluateCoverage(context.Background(), root, filepath.Join(root, "coverage.out"), policy, filepath.Join(root, "manifest.json"), "HEAD~1")
	if err != nil || !evaluation.Report.Passed || evaluation.Report.Diff.Branch.Percent != 90 {
		t.Fatalf("diff coverage at its floor must pass: %#v, %v", evaluation, err)
	}
	policy.DiffFloor.Branch = 90.01
	policy.Ratchet.BranchTolerance = 0.20
	evaluation, err = evaluateCoverage(context.Background(), root, filepath.Join(root, "coverage.out"), policy, filepath.Join(root, "manifest.json"), "HEAD~1")
	if err == nil || !strings.Contains(err.Error(), "diff branch coverage 90.00% is below 90.01%") || evaluation == nil || evaluation.Report.Passed {
		t.Fatalf("diff coverage below its floor must fail without ratchet tolerance: %#v, %v", evaluation, err)
	}
	evaluation, err = evaluateCoverage(context.Background(), root, filepath.Join(root, "coverage.out"), policy, filepath.Join(root, "manifest.json"), "")
	if err != nil || !evaluation.Report.Passed || len(evaluation.Report.BranchCoverage.RequirementShortfalls) != 1 {
		t.Fatalf("requirement shortfall must not fail baseline gate: %#v, %v", evaluation, err)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-079
func TestBranchInstrumentRejectsUnsafeOutputBeforeDeletingFiles(t *testing.T) {
	root := t.TempDir()
	writeBranchTestFile(t, root, "internal/sample.go", []byte("package sample\nfunc Sample() {}\n"))
	writeBranchTestFile(t, root, "go.mod", []byte("module narumq\n\ngo 1.26.5\n"))
	policy := coveragePolicy{
		SchemaVersion:      2,
		ProductionPatterns: []string{"./internal/..."},
		CorePackages:       []string{"narumq/internal"},
		Ratchet:            coverageRatchet{Core: map[string]coverageFloor{"narumq/internal": {}}},
	}
	content, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	writeBranchTestFile(t, root, "policy.json", content)
	for _, output := range []string{".", "..", "internal", ".artifacts", ".artifacts/verification", ".artifacts/other"} {
		t.Run(output, func(t *testing.T) {
			if err := runBranchInstrument(context.Background(), root, filepath.Join(root, "policy.json"), output); err == nil || !strings.Contains(err.Error(), "unsafe branch output directory") {
				t.Fatalf("unsafe output %s error = %v", output, err)
			}
			for _, path := range []string{"go.mod", "internal/sample.go", "policy.json"} {
				if _, err := os.Stat(filepath.Join(root, path)); err != nil {
					t.Fatalf("protected file %s was removed: %v", path, err)
				}
			}
		})
	}
	for _, output := range []string{filepath.Join(root, ".artifacts", "verification", "branch"), t.TempDir()} {
		if err := validateBranchOutputDir(root, output); err != nil {
			t.Fatalf("dedicated output %s rejected: %v", output, err)
		}
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-079
func TestBranchOutputRejectsSymlinkToRepositoryRoot(t *testing.T) {
	root := t.TempDir()
	alias := filepath.Join(t.TempDir(), "repository-alias")
	if err := os.Symlink(root, alias); err != nil {
		if runtime.GOOS != "windows" {
			t.Fatal(err)
		}
		// Directory junctions exercise the same resolved-path boundary without
		// requiring the Windows symbolic-link privilege.
		runBranchTestCommand(t, filepath.Dir(alias), "cmd", "/c", "mklink", "/J", alias, root)
	}
	t.Cleanup(func() {
		if err := os.Remove(alias); err != nil {
			t.Error(err)
		}
	})
	if err := validateBranchOutputDir(root, alias); err == nil || !strings.Contains(err.Error(), "unsafe branch output directory") {
		t.Fatalf("repository alias output error = %v", err)
	}
	if err := validateBranchOutputDir(root, filepath.Join(alias, "internal", "new-output")); err == nil || !strings.Contains(err.Error(), "unsafe branch output directory") {
		t.Fatalf("source subtree alias output error = %v", err)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-079
func TestBranchOutputRejectsRepositoryLinkToExternalDirectory(t *testing.T) {
	root, external := t.TempDir(), t.TempDir()
	alias := filepath.Join(root, filepath.FromSlash(defaultBranchOutputDir))
	if err := os.MkdirAll(filepath.Dir(alias), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, alias); err != nil {
		if runtime.GOOS != "windows" {
			t.Fatal(err)
		}
		runBranchTestCommand(t, root, "cmd", "/c", "mklink", "/J", alias, external)
	}
	t.Cleanup(func() {
		if err := os.Remove(alias); err != nil {
			t.Error(err)
		}
	})
	for _, path := range []string{alias, filepath.Join(alias, "child-output")} {
		if err := validateBranchOutputDir(root, path); err == nil || !strings.Contains(err.Error(), "repository-local symlink or junction") {
			t.Fatalf("repository output alias %s error = %v", path, err)
		}
	}
}
