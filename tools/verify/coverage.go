package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/build"
	goparser "go/parser"
	"go/token"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type coverageFloor struct {
	Line     float64 `json:"line"`
	Function float64 `json:"function"`
	Branch   float64 `json:"branch"`
}

type coveragePolicy struct {
	SchemaVersion      int             `json:"schemaVersion"`
	ProductionPatterns []string        `json:"productionPatterns"`
	OverallFloor       coverageFloor   `json:"overallFloor"`
	CoreFloor          coverageFloor   `json:"coreFloor"`
	DiffFloor          coverageFloor   `json:"diffFloor"`
	CorePackages       []string        `json:"corePackages"`
	Ratchet            coverageRatchet `json:"ratchet"`
}

type coverageRatchet struct {
	Overall coverageFloor            `json:"overall"`
	Core    map[string]coverageFloor `json:"core"`
}

type coverageBlock struct {
	File        string
	StartLine   int
	StartColumn int
	EndLine     int
	EndColumn   int
	Statements  int
	Count       int
}

type sourceFunction struct {
	Package   string
	File      string
	Name      string
	StartLine int
	EndLine   int
}

type sourceFile struct {
	Path      string
	Package   string
	Lines     []string
	Functions []sourceFunction
}

type coverageData struct {
	Blocks         []coverageBlock
	Sources        map[string]sourceFile
	ExecutableLine map[string]map[int]bool
	CoveredLine    map[string]map[int]bool
	Functions      []sourceFunction
	CoveredFunc    map[string]bool
}

type coverageMetric struct {
	Covered int     `json:"covered"`
	Total   int     `json:"total"`
	Percent float64 `json:"percent"`
}

type packageCoverage struct {
	Line     coverageMetric `json:"line"`
	Function coverageMetric `json:"function"`
	Branch   coverageMetric `json:"branch"`
}

type diffCoverage struct {
	Line     coverageMetric `json:"line"`
	Function coverageMetric `json:"function"`
	Branch   coverageMetric `json:"branch"`
}

type branchCoverageStatus struct {
	Status                string   `json:"status"`
	Reason                string   `json:"reason,omitempty"`
	Manifest              string   `json:"manifest,omitempty"`
	RequirementShortfalls []string `json:"requirementShortfalls"`
	UncoveredOutcomes     []string `json:"uncoveredOutcomes"`
}

type coverageReport struct {
	SchemaVersion  int                        `json:"schemaVersion"`
	Profile        string                     `json:"profile"`
	Passed         bool                       `json:"passed"`
	Error          string                     `json:"error,omitempty"`
	Overall        packageCoverage            `json:"overall"`
	Core           map[string]packageCoverage `json:"core"`
	Diff           *diffCoverage              `json:"diff,omitempty"`
	BranchCoverage branchCoverageStatus       `json:"branchCoverage"`
}

const defaultCoveragePolicyPath = "tests/verification/coverage-policy.json"

var coverageProfileLinePattern = regexp.MustCompile(`^(.*):(\d+)\.(\d+),(\d+)\.(\d+) (\d+) (\d+)$`)
var diffHunkPattern = regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@`)

func loadCoveragePolicy(path string) (coveragePolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return coveragePolicy{}, fmt.Errorf("read coverage policy %s: %w", path, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var policy coveragePolicy
	if err := decoder.Decode(&policy); err != nil {
		return coveragePolicy{}, fmt.Errorf("decode coverage policy %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return coveragePolicy{}, fmt.Errorf("decode coverage policy %s: trailing JSON value", path)
		}
		return coveragePolicy{}, fmt.Errorf("decode coverage policy %s: trailing JSON: %w", path, err)
	}
	if issues := validateCoveragePolicy(policy); len(issues) > 0 {
		return coveragePolicy{}, fmt.Errorf("invalid coverage policy %s:\n%w", path, issues)
	}
	return policy, nil
}

func validateCoveragePolicy(policy coveragePolicy) architectureIssues {
	var issues architectureIssues
	if policy.SchemaVersion != 2 {
		issues = append(issues, architectureIssue{Message: fmt.Sprintf("coverage policy schemaVersion = %d, want 2", policy.SchemaVersion)})
	}
	if len(policy.ProductionPatterns) == 0 {
		issues = append(issues, architectureIssue{Message: "coverage policy productionPatterns must not be empty"})
	}
	if !validCoverageFloor(policy.OverallFloor) {
		issues = append(issues, architectureIssue{Message: "coverage policy overallFloor must be between 0 and 100"})
	}
	if !validCoverageFloor(policy.CoreFloor) {
		issues = append(issues, architectureIssue{Message: "coverage policy coreFloor must be between 0 and 100"})
	}
	if !validCoverageFloor(policy.DiffFloor) {
		issues = append(issues, architectureIssue{Message: "coverage policy diffFloor must be between 0 and 100"})
	}
	if len(policy.CorePackages) == 0 {
		issues = append(issues, architectureIssue{Message: "coverage policy corePackages must not be empty"})
	}
	if !equalSortedUnique(policy.ProductionPatterns) {
		issues = append(issues, architectureIssue{Message: "coverage policy productionPatterns must be sorted and unique"})
	}
	if !equalSortedUnique(policy.CorePackages) {
		issues = append(issues, architectureIssue{Message: "coverage policy corePackages must be sorted and unique"})
	}
	if !validCoverageFloor(policy.Ratchet.Overall) {
		issues = append(issues, architectureIssue{Message: "coverage policy ratchet.overall must be between 0 and 100"})
	}
	if len(policy.Ratchet.Core) != len(policy.CorePackages) {
		issues = append(issues, architectureIssue{Message: "coverage policy ratchet.core must contain every core package exactly once"})
	}
	for _, packagePath := range policy.CorePackages {
		floor, exists := policy.Ratchet.Core[packagePath]
		if !exists {
			issues = append(issues, architectureIssue{Message: "coverage policy ratchet.core missing " + packagePath})
			continue
		}
		if !validCoverageFloor(floor) {
			issues = append(issues, architectureIssue{Message: "coverage policy ratchet.core invalid for " + packagePath})
		}
	}
	return issues
}

func validCoverageFloor(floor coverageFloor) bool {
	return floor.Line >= 0 && floor.Line <= 100 && floor.Function >= 0 && floor.Function <= 100 && floor.Branch >= 0 && floor.Branch <= 100
}

func equalSortedUnique(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] >= values[index] {
			return false
		}
	}
	return true
}

func runCoverage(ctx context.Context, root, profilePath, policyPath, reportPath, manifestPath, base string) error {
	policy, err := loadCoveragePolicy(policyPath)
	if err != nil {
		return err
	}
	report := coverageReport{
		SchemaVersion:  2,
		Profile:        relativePath(root, profilePath),
		Core:           make(map[string]packageCoverage),
		BranchCoverage: branchCoverageStatus{Status: "not_evaluated", Reason: "coverage evaluation failed before branch evaluation", RequirementShortfalls: []string{}, UncoveredOutcomes: []string{}},
	}
	evaluation, evaluateErr := evaluateCoverage(ctx, root, profilePath, policy, manifestPath, base)
	if evaluation != nil {
		report = evaluation.Report
	}
	if evaluateErr != nil {
		report.Passed = false
		report.Error = evaluateErr.Error()
	}
	if err := writeCoverageReport(reportPath, report); err != nil {
		if evaluateErr != nil {
			return fmt.Errorf("%v; write coverage report: %w", evaluateErr, err)
		}
		return err
	}
	return evaluateErr
}

type coverageEvaluation struct {
	Report coverageReport
}

func evaluateCoverage(ctx context.Context, root, profilePath string, policy coveragePolicy, manifestPath, base string) (*coverageEvaluation, error) {
	data, err := parseCoverageProfile(root, profilePath, policy)
	if err != nil {
		return nil, err
	}
	manifest, err := loadBranchManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	branch, err := evaluateBranchCoverage(root, manifest, data)
	if err != nil {
		return nil, err
	}
	overall, packages := calculateCoverage(data)
	overall.Branch = branch.Overall
	report := coverageReport{
		SchemaVersion:  2,
		Profile:        relativePath(root, profilePath),
		Passed:         false,
		Overall:        overall,
		Core:           make(map[string]packageCoverage),
		BranchCoverage: branchCoverageStatus{Status: "measured", Manifest: relativePath(root, manifestPath), RequirementShortfalls: []string{}, UncoveredOutcomes: []string{}},
	}
	for _, outcome := range branch.Outcomes {
		if !outcome.Covered {
			report.BranchCoverage.UncoveredOutcomes = append(report.BranchCoverage.UncoveredOutcomes, outcome.ID)
		}
	}
	sort.Strings(report.BranchCoverage.UncoveredOutcomes)
	for _, packagePath := range policy.CorePackages {
		metric, exists := packages[packagePath]
		if !exists {
			return &coverageEvaluation{Report: report}, fmt.Errorf("core package %s has no production coverage data", packagePath)
		}
		metric.Branch = branch.Packages[packagePath]
		report.Core[packagePath] = metric
	}

	var issues []string
	issues = append(issues, enforceCoverageFloor("overall", overall, policy.OverallFloor, policy.Ratchet.Overall)...)
	issues = append(issues, enforceBranchRatchet("overall", overall.Branch, policy.Ratchet.Overall.Branch)...)
	if shortfall, exists := branchRequirementShortfall("overall", overall.Branch, policy.OverallFloor.Branch); exists {
		report.BranchCoverage.RequirementShortfalls = append(report.BranchCoverage.RequirementShortfalls, shortfall)
	}
	for _, packagePath := range policy.CorePackages {
		metric := report.Core[packagePath]
		ratchet := policy.Ratchet.Core[packagePath]
		issues = append(issues, enforceCoverageFloor(packagePath, metric, policy.CoreFloor, ratchet)...)
		issues = append(issues, enforceBranchRatchet(packagePath, metric.Branch, ratchet.Branch)...)
		if shortfall, exists := branchRequirementShortfall(packagePath, metric.Branch, policy.CoreFloor.Branch); exists {
			report.BranchCoverage.RequirementShortfalls = append(report.BranchCoverage.RequirementShortfalls, shortfall)
		}
	}
	sort.Strings(report.BranchCoverage.RequirementShortfalls)
	if base != "" {
		diff, diffErr := calculateDiffCoverage(ctx, root, base, data, branch)
		if diffErr != nil {
			return &coverageEvaluation{Report: report}, diffErr
		}
		report.Diff = &diff
		issues = append(issues, enforceCoverageFloor("diff", packageCoverage{Line: diff.Line, Function: diff.Function}, policy.DiffFloor, coverageFloor{})...)
		if diff.Branch.Percent+0.000001 < policy.DiffFloor.Branch {
			issues = append(issues, fmt.Sprintf("diff branch coverage %.2f%% is below %.2f%%", diff.Branch.Percent, policy.DiffFloor.Branch))
		}
	}
	if len(issues) > 0 {
		return &coverageEvaluation{Report: report}, fmt.Errorf("coverage gate failed:\n%s", strings.Join(sortedStrings(issues), "\n"))
	}
	report.Passed = true
	return &coverageEvaluation{Report: report}, nil
}

func enforceCoverageFloor(name string, actual packageCoverage, floor, ratchet coverageFloor) []string {
	var issues []string
	if actual.Line.Percent+0.000001 < floor.Line {
		issues = append(issues, fmt.Sprintf("%s line coverage %.2f%% is below %.2f%%", name, actual.Line.Percent, floor.Line))
	}
	if actual.Function.Percent+0.000001 < floor.Function {
		issues = append(issues, fmt.Sprintf("%s function coverage %.2f%% is below %.2f%%", name, actual.Function.Percent, floor.Function))
	}
	if ratchet.Line > floor.Line && actual.Line.Percent+0.005 < ratchet.Line {
		issues = append(issues, fmt.Sprintf("%s line ratchet %.2f%% is above measured %.2f%%", name, ratchet.Line, actual.Line.Percent))
	}
	if ratchet.Function > floor.Function && actual.Function.Percent+0.005 < ratchet.Function {
		issues = append(issues, fmt.Sprintf("%s function ratchet %.2f%% is above measured %.2f%%", name, ratchet.Function, actual.Function.Percent))
	}
	return issues
}

func writeCoverageReport(path string, report coverageReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create coverage report directory: %w", err)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode coverage report: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write coverage report %s: %w", path, err)
	}
	return nil
}

func parseCoverageProfile(root, path string, policy coveragePolicy) (coverageData, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return coverageData{}, fmt.Errorf("read coverage profile %s: %w", path, err)
	}
	lines := strings.Split(strings.TrimSuffix(string(content), "\n"), "\n")
	if len(lines) == 0 || strings.TrimSuffix(lines[0], "\r") != "mode: atomic" {
		return coverageData{}, fmt.Errorf("coverage profile %s must start with mode: atomic", path)
	}
	sources, err := loadProductionSources(root, policy.ProductionPatterns)
	if err != nil {
		return coverageData{}, err
	}
	data := coverageData{
		Sources:        sources,
		ExecutableLine: make(map[string]map[int]bool),
		CoveredLine:    make(map[string]map[int]bool),
		CoveredFunc:    make(map[string]bool),
	}
	blocks := make(map[string]coverageBlock)
	for lineNumber, line := range lines[1:] {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		match := coverageProfileLinePattern.FindStringSubmatch(line)
		if match == nil {
			return coverageData{}, fmt.Errorf("coverage profile %s line %d is malformed", path, lineNumber+2)
		}
		startLine, _ := strconv.Atoi(match[2])
		startColumn, _ := strconv.Atoi(match[3])
		endLine, _ := strconv.Atoi(match[4])
		endColumn, _ := strconv.Atoi(match[5])
		statements, _ := strconv.Atoi(match[6])
		count, _ := strconv.Atoi(match[7])
		if startLine <= 0 || endLine < startLine || startColumn <= 0 || endColumn <= 0 || statements < 0 || count < 0 {
			return coverageData{}, fmt.Errorf("coverage profile %s line %d has invalid block values", path, lineNumber+2)
		}
		file := normalizeProfilePath(root, match[1])
		source, exists := sources[file]
		if !exists {
			generated, generatedErr := isExcludedGeneratedSource(root, file)
			if generatedErr != nil {
				return coverageData{}, generatedErr
			}
			if generated {
				continue
			}
			return coverageData{}, fmt.Errorf("coverage profile references non-production or unknown file %s", file)
		}
		if endLine > len(source.Lines) {
			return coverageData{}, fmt.Errorf("coverage profile block %s:%d exceeds source lines", file, startLine)
		}
		block := coverageBlock{File: file, StartLine: startLine, StartColumn: startColumn, EndLine: endLine, EndColumn: endColumn, Statements: statements, Count: count}
		key := fmt.Sprintf("%s:%d.%d,%d.%d:%d", file, startLine, startColumn, endLine, endColumn, statements)
		if previous, exists := blocks[key]; exists {
			if count > 0 && previous.Count > math.MaxInt-count {
				return coverageData{}, fmt.Errorf("coverage profile count overflow at %s", key)
			}
			previous.Count += count
			blocks[key] = previous
		} else {
			blocks[key] = block
		}
	}
	if len(blocks) == 0 {
		return coverageData{}, fmt.Errorf("coverage profile %s contains no coverage blocks", path)
	}
	data.Blocks = make([]coverageBlock, 0, len(blocks))
	for _, block := range blocks {
		data.Blocks = append(data.Blocks, block)
	}
	sort.Slice(data.Blocks, func(left, right int) bool {
		return coverageBlockKey(data.Blocks[left]) < coverageBlockKey(data.Blocks[right])
	})
	for _, block := range data.Blocks {
		if block.Statements == 0 {
			continue
		}
		if _, exists := data.ExecutableLine[block.File]; !exists {
			data.ExecutableLine[block.File] = make(map[int]bool)
			data.CoveredLine[block.File] = make(map[int]bool)
		}
		for line := block.StartLine; line <= block.EndLine; line++ {
			data.ExecutableLine[block.File][line] = true
			if block.Count > 0 {
				data.CoveredLine[block.File][line] = true
			}
		}
	}
	for _, source := range sources {
		data.Functions = append(data.Functions, source.Functions...)
	}
	sort.Slice(data.Functions, func(left, right int) bool {
		return sourceFunctionKey(data.Functions[left]) < sourceFunctionKey(data.Functions[right])
	})
	for _, function := range data.Functions {
		for _, block := range data.Blocks {
			if block.File != function.File || block.StartLine < function.StartLine || block.EndLine > function.EndLine {
				continue
			}
			if block.Count > 0 {
				data.CoveredFunc[sourceFunctionKey(function)] = true
				break
			}
		}
	}
	for path, source := range sources {
		if len(source.Functions) > 0 && len(data.ExecutableLine[path]) == 0 {
			return coverageData{}, fmt.Errorf("production file %s is missing from coverage profile", path)
		}
	}
	return data, nil
}

func coverageBlockKey(block coverageBlock) string {
	return fmt.Sprintf("%s:%d.%d,%d.%d:%d", block.File, block.StartLine, block.StartColumn, block.EndLine, block.EndColumn, block.Statements)
}

func loadProductionSources(root string, patterns []string) (map[string]sourceFile, error) {
	sources := make(map[string]sourceFile)
	for _, pattern := range patterns {
		if pattern != "./cmd/..." && pattern != "./internal/..." {
			return nil, fmt.Errorf("unsupported production coverage pattern %s", pattern)
		}
		base := strings.TrimSuffix(strings.TrimPrefix(pattern, "./"), "/...")
		basePath := filepath.Join(root, filepath.FromSlash(base))
		if _, err := os.Stat(basePath); err != nil {
			return nil, fmt.Errorf("production coverage root %s: %w", base, err)
		}
		err := filepath.WalkDir(basePath, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") || strings.HasSuffix(entry.Name(), ".pb.go") {
				return nil
			}
			match, matchErr := build.Default.MatchFile(filepath.Dir(path), entry.Name())
			if matchErr != nil || !match {
				return nil
			}
			file, err := parseSourceFile(root, path)
			if err != nil {
				return err
			}
			if _, exists := sources[file.Path]; exists {
				return nil
			}
			sources[file.Path] = file
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk production coverage root %s: %w", base, err)
		}
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("production coverage patterns matched no Go source")
	}
	return sources, nil
}

func isExcludedGeneratedSource(root, file string) (bool, error) {
	if (!strings.HasPrefix(file, "cmd/") && !strings.HasPrefix(file, "internal/")) ||
		!strings.HasSuffix(file, ".pb.go") {
		return false, nil
	}
	clean := filepath.Clean(filepath.FromSlash(file))
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return false, nil
	}
	source, err := os.Open(filepath.Join(root, clean))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("open generated coverage source %s: %w", file, err)
	}
	defer source.Close()
	scanner := bufio.NewScanner(source)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return false, fmt.Errorf("read generated coverage source %s: %w", file, err)
		}
		return false, nil
	}
	header := scanner.Text()
	return strings.HasPrefix(header, "// Code generated ") && strings.HasSuffix(header, " DO NOT EDIT."), nil
}

func parseSourceFile(root, path string) (sourceFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return sourceFile{}, fmt.Errorf("read production source %s: %w", path, err)
	}
	fileSet := token.NewFileSet()
	parsed, err := goparser.ParseFile(fileSet, path, data, 0)
	if err != nil {
		return sourceFile{}, fmt.Errorf("parse production source %s: %w", relativePath(root, path), err)
	}
	relative := relativePath(root, path)
	packagePath := modulePath + "/" + filepath.ToSlash(filepath.Dir(relative))
	source := sourceFile{Path: relative, Package: packagePath, Lines: strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name == nil || function.Body == nil {
			continue
		}
		source.Functions = append(source.Functions, sourceFunction{
			Package:   packagePath,
			File:      relative,
			Name:      function.Name.Name,
			StartLine: fileSet.Position(function.Body.Pos()).Line,
			EndLine:   fileSet.Position(function.Body.End()).Line,
		})
	}
	return source, nil
}

func normalizeProfilePath(root, profilePath string) string {
	value := filepath.ToSlash(strings.TrimSpace(profilePath))
	if filepath.IsAbs(profilePath) {
		return relativePath(root, profilePath)
	}
	value = strings.TrimPrefix(value, "./")
	value = strings.TrimPrefix(value, modulePath+"/")
	return filepath.ToSlash(value)
}

func calculateCoverage(data coverageData) (packageCoverage, map[string]packageCoverage) {
	packages := make(map[string]packageCoverage)
	lineByPackage := make(map[string][]bool)
	functionByPackage := make(map[string][]bool)
	for path, lines := range data.ExecutableLine {
		packagePath := data.Sources[path].Package
		for line, executable := range lines {
			if !executable {
				continue
			}
			lineByPackage[packagePath] = append(lineByPackage[packagePath], data.CoveredLine[path][line])
		}
	}
	for _, function := range data.Functions {
		functionByPackage[function.Package] = append(functionByPackage[function.Package], data.CoveredFunc[sourceFunctionKey(function)])
	}
	for packagePath, values := range lineByPackage {
		packages[packagePath] = packageCoverage{Line: boolMetric(values)}
	}
	for packagePath, values := range functionByPackage {
		metric := packages[packagePath]
		metric.Function = boolMetric(values)
		packages[packagePath] = metric
	}
	var allLines, allFunctions []bool
	for _, values := range lineByPackage {
		allLines = append(allLines, values...)
	}
	for _, values := range functionByPackage {
		allFunctions = append(allFunctions, values...)
	}
	return packageCoverage{Line: boolMetric(allLines), Function: boolMetric(allFunctions)}, packages
}

func boolMetric(values []bool) coverageMetric {
	metric := coverageMetric{Total: len(values)}
	for _, covered := range values {
		if covered {
			metric.Covered++
		}
	}
	if metric.Total > 0 {
		metric.Percent = roundCoverage(float64(metric.Covered) * 100 / float64(metric.Total))
	}
	return metric
}

func diffMetric(values []bool) coverageMetric {
	if len(values) == 0 {
		return coverageMetric{Percent: 100}
	}
	return boolMetric(values)
}

func roundCoverage(value float64) float64 {
	return math.Round(value*100) / 100
}

func sourceFunctionKey(function sourceFunction) string {
	return fmt.Sprintf("%s#%s@%d", function.File, function.Name, function.StartLine)
}

func calculateDiffCoverage(ctx context.Context, root, base string, data coverageData, branch branchEvaluation) (diffCoverage, error) {
	if err := verifyBaseRef(ctx, root, base); err != nil {
		return diffCoverage{}, err
	}
	command := exec.CommandContext(ctx, "git", "diff", "--unified=0", "--no-ext-diff", base+"...HEAD", "--", "cmd", "internal")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return diffCoverage{}, fmt.Errorf("git diff for base %s failed: %w: %s", base, err, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return diffCoverage{}, fmt.Errorf("git diff for base %s: %w", base, err)
	}
	changed := parseChangedLines(string(output))
	var lineValues []bool
	functionValues := make(map[string]bool)
	for file, lines := range changed {
		source, exists := data.Sources[file]
		if !exists || strings.HasSuffix(file, "_test.go") {
			continue
		}
		for line := range lines {
			if !data.ExecutableLine[file][line] || sourceLineIsCommentOrDelimiter(source.Lines, line) {
				continue
			}
			lineValues = append(lineValues, data.CoveredLine[file][line])
			for _, function := range source.Functions {
				if line >= function.StartLine && line <= function.EndLine {
					functionValues[sourceFunctionKey(function)] = data.CoveredFunc[sourceFunctionKey(function)]
				}
			}
		}
	}
	functionResults := make([]bool, 0, len(functionValues))
	for _, covered := range functionValues {
		functionResults = append(functionResults, covered)
	}
	return diffCoverage{Line: diffMetric(lineValues), Function: diffMetric(functionResults), Branch: diffMetric(diffBranchValues(changed, branch.Outcomes))}, nil
}

func verifyBaseRef(ctx context.Context, root, base string) error {
	command := exec.CommandContext(ctx, "git", "rev-parse", "--verify", base+"^{commit}")
	command.Dir = root
	if output, err := command.Output(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("base ref %s is unavailable: %w: %s", base, err, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return fmt.Errorf("base ref %s is unavailable: %w", base, err)
	} else if strings.TrimSpace(string(output)) == "" {
		return fmt.Errorf("base ref %s is unavailable: empty revision", base)
	}
	return nil
}

func parseChangedLines(diff string) map[string]map[int]struct{} {
	changed := make(map[string]map[int]struct{})
	currentFile := ""
	newLine := 0
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, "+++ b/") {
			currentFile = filepath.ToSlash(strings.TrimPrefix(line, "+++ b/"))
			newLine = 0
			continue
		}
		match := diffHunkPattern.FindStringSubmatch(line)
		if match != nil {
			newLine, _ = strconv.Atoi(match[1])
			continue
		}
		if currentFile == "" || newLine == 0 || strings.HasPrefix(line, "diff --git ") {
			continue
		}
		if strings.HasPrefix(line, "+") {
			if !strings.HasPrefix(line, "+++") {
				if _, exists := changed[currentFile]; !exists {
					changed[currentFile] = make(map[int]struct{})
				}
				changed[currentFile][newLine] = struct{}{}
			}
			newLine++
			continue
		}
		if strings.HasPrefix(line, " ") {
			newLine++
		}
	}
	return changed
}

func sourceLineIsCommentOrDelimiter(lines []string, line int) bool {
	if line <= 0 || line > len(lines) {
		return true
	}
	trimmed := strings.TrimSpace(lines[line-1])
	if trimmed == "" || strings.HasPrefix(trimmed, "//") || strings.HasPrefix(trimmed, "/*") || strings.HasPrefix(trimmed, "*") || strings.HasPrefix(trimmed, "*/") {
		return true
	}
	return trimmed == "{" || trimmed == "}" || trimmed == "(" || trimmed == ")"
}

func sortedStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
