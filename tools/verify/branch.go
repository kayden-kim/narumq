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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const defaultBranchOutputDir = ".artifacts/verification/branch"
const defaultBranchManifestPath = ".artifacts/verification/branch/manifest.json"

type branchPosition struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

type branchOutcome struct {
	Name  string         `json:"name"`
	Start branchPosition `json:"start"`
	End   branchPosition `json:"end"`
}

type branchDecision struct {
	Kind     string          `json:"kind"`
	Line     int             `json:"line"`
	Column   int             `json:"column"`
	Outcomes []branchOutcome `json:"outcomes"`
}

type branchFile struct {
	Path      string           `json:"path"`
	Package   string           `json:"package"`
	SHA256    string           `json:"sha256"`
	Decisions []branchDecision `json:"decisions"`
}

type branchManifest struct {
	SchemaVersion int          `json:"schemaVersion"`
	Files         []branchFile `json:"files"`
}

type branchInsertion struct {
	Offset int
	Text   string
}

func instrumentBranches(relativePath string, source []byte) ([]byte, []branchDecision, error) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, relativePath, source, parser.SkipObjectResolution)
	if err != nil {
		return nil, nil, fmt.Errorf("parse production source %s: %w", relativePath, err)
	}
	var insertions []branchInsertion
	ast.Inspect(parsed, func(node ast.Node) bool {
		if function, ok := node.(*ast.FuncDecl); ok && function.Name.Name == "_" {
			return false
		}
		switch statement := node.(type) {
		case *ast.IfStmt:
			if statement.Else == nil {
				insertions = append(insertions, branchInsertion{Offset: fset.Position(statement.Body.Rbrace).Offset + 1, Text: " else {}"})
			}
		case *ast.SwitchStmt:
			insertions = appendMissingBranchDefault(fset, statement.Body, insertions)
		case *ast.TypeSwitchStmt:
			insertions = appendMissingBranchDefault(fset, statement.Body, insertions)
		}
		return true
	})
	sort.Slice(insertions, func(i, j int) bool { return insertions[i].Offset > insertions[j].Offset })
	size := len(source)
	for _, insertion := range insertions {
		size += len(insertion.Text)
	}
	instrumented := make([]byte, size)
	sourceEnd, destinationEnd := len(source), size
	for _, insertion := range insertions {
		segmentSize := sourceEnd - insertion.Offset
		destinationEnd -= segmentSize
		copy(instrumented[destinationEnd:], source[insertion.Offset:sourceEnd])
		destinationEnd -= len(insertion.Text)
		copy(instrumented[destinationEnd:], insertion.Text)
		sourceEnd = insertion.Offset
	}
	copy(instrumented[:destinationEnd], source[:sourceEnd])
	fset = token.NewFileSet()
	parsed, err = parser.ParseFile(fset, relativePath, instrumented, parser.SkipObjectResolution)
	if err != nil {
		return nil, nil, fmt.Errorf("instrumented source %s does not parse: %w", relativePath, err)
	}
	if bytes.Count(source, []byte{'\n'}) != bytes.Count(instrumented, []byte{'\n'}) {
		return nil, nil, fmt.Errorf("instrumented source %s changed line count", relativePath)
	}
	decisions := make([]branchDecision, 0)
	ast.Inspect(parsed, func(node ast.Node) bool {
		if function, ok := node.(*ast.FuncDecl); ok && function.Name.Name == "_" {
			return false
		}
		var kind string
		var outcomes []branchOutcome
		switch statement := node.(type) {
		case *ast.IfStmt:
			kind = "if"
			outcomes = []branchOutcome{
				{Name: "then", Start: branchASTPosition(fset, statement.Body.Pos()), End: branchASTPosition(fset, statement.Body.End())},
				{Name: "else", Start: branchASTPosition(fset, statement.Body.End()), End: branchASTPosition(fset, statement.Else.End())},
			}
		case *ast.SwitchStmt:
			kind, outcomes = "switch", branchClauseOutcomes(fset, statement.Body)
		case *ast.TypeSwitchStmt:
			kind, outcomes = "typeSwitch", branchClauseOutcomes(fset, statement.Body)
		case *ast.SelectStmt:
			kind, outcomes = "select", branchClauseOutcomes(fset, statement.Body)
		}
		if len(outcomes) > 0 {
			position := branchASTPosition(fset, node.Pos())
			decisions = append(decisions, branchDecision{Kind: kind, Line: position.Line, Column: position.Column, Outcomes: outcomes})
		}
		return true
	})
	sort.Slice(decisions, func(i, j int) bool {
		return branchPositionLess(branchPosition{Line: decisions[i].Line, Column: decisions[i].Column}, branchPosition{Line: decisions[j].Line, Column: decisions[j].Column})
	})
	return instrumented, decisions, nil
}

func appendMissingBranchDefault(fset *token.FileSet, body *ast.BlockStmt, insertions []branchInsertion) []branchInsertion {
	if len(body.List) == 0 {
		return insertions
	}
	for _, node := range body.List {
		if node.(*ast.CaseClause).List == nil {
			return insertions
		}
	}
	return append(insertions, branchInsertion{Offset: fset.Position(body.Rbrace).Offset, Text: "default:"})
}

func branchASTPosition(fset *token.FileSet, position token.Pos) branchPosition {
	value := fset.Position(position)
	return branchPosition{Line: value.Line, Column: value.Column}
}

func branchClauseOutcomes(fset *token.FileSet, body *ast.BlockStmt) []branchOutcome {
	outcomes := make([]branchOutcome, 0, len(body.List))
	for _, node := range body.List {
		isDefault := false
		switch clause := node.(type) {
		case *ast.CaseClause:
			isDefault = clause.List == nil
		case *ast.CommClause:
			isDefault = clause.Comm == nil
		}
		start := branchASTPosition(fset, node.Pos())
		name := fmt.Sprintf("case@%d:%d", start.Line, start.Column)
		if isDefault {
			name = "default"
		}
		outcomes = append(outcomes, branchOutcome{Name: name, Start: start, End: branchASTPosition(fset, node.End())})
	}
	return outcomes
}

func branchPositionLess(left, right branchPosition) bool {
	return left.Line < right.Line || left.Line == right.Line && left.Column < right.Column
}

func loadBranchManifest(path string) (branchManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return branchManifest{}, fmt.Errorf("read branch manifest %s: %w", path, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest branchManifest
	if err := decoder.Decode(&manifest); err != nil {
		return branchManifest{}, fmt.Errorf("decode branch manifest %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return branchManifest{}, fmt.Errorf("decode branch manifest %s: trailing JSON value", path)
		}
		return branchManifest{}, fmt.Errorf("decode branch manifest %s: trailing JSON: %w", path, err)
	}
	if manifest.SchemaVersion != 1 {
		return branchManifest{}, fmt.Errorf("branch manifest schemaVersion = %d, want 1", manifest.SchemaVersion)
	}
	return manifest, nil
}

func runBranchInstrument(ctx context.Context, root, policyPath, outputDir string) error {
	policy, err := loadCoveragePolicy(policyPath)
	if err != nil {
		return err
	}
	sources, err := loadProductionSources(root, policy.ProductionPatterns)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(outputDir) {
		outputDir = filepath.Join(root, outputDir)
	}
	if err := validateBranchOutputDir(root, outputDir); err != nil {
		return err
	}
	if err := os.RemoveAll(outputDir); err != nil {
		return fmt.Errorf("remove branch instrumentation directory: %w", err)
	}
	moduleDir := filepath.Join(outputDir, "module")
	if err := os.MkdirAll(moduleDir, 0o755); err != nil {
		return fmt.Errorf("create branch instrumentation module: %w", err)
	}
	command := exec.CommandContext(ctx, "git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	list, err := command.Output()
	if err != nil {
		return fmt.Errorf("list repository files for branch instrumentation: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	manifest := branchManifest{SchemaVersion: 1, Files: make([]branchFile, 0, len(sources))}
	copied := make(map[string]bool, len(sources))
	for _, path := range strings.Split(string(list), "\x00") {
		if path == "" {
			continue
		}
		path = filepath.ToSlash(path)
		original := filepath.Join(root, filepath.FromSlash(path))
		info, err := os.Stat(original)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("stat branch copy source %s: %w", path, err)
		}
		content, err := os.ReadFile(original)
		if err != nil {
			return fmt.Errorf("read branch copy source %s: %w", path, err)
		}
		if source, ok := sources[path]; ok {
			hash := fmt.Sprintf("%x", sha256.Sum256(content))
			instrumented, decisions, err := instrumentBranches(path, content)
			if err != nil {
				return err
			}
			content = instrumented
			manifest.Files = append(manifest.Files, branchFile{Path: path, Package: source.Package, SHA256: hash, Decisions: decisions})
			copied[path] = true
		}
		destination := filepath.Join(moduleDir, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return fmt.Errorf("create branch copy directory for %s: %w", path, err)
		}
		if err := os.WriteFile(destination, content, info.Mode().Perm()); err != nil {
			return fmt.Errorf("write branch copy source %s: %w", path, err)
		}
	}
	var missing []string
	for path := range sources {
		if !copied[path] {
			missing = append(missing, path)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return fmt.Errorf("production source %s is not in the branch instrumentation copy set", missing[0])
	}
	sort.Slice(manifest.Files, func(i, j int) bool { return manifest.Files[i].Path < manifest.Files[j].Path })
	content, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode branch manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "manifest.json"), append(content, '\n'), 0o644); err != nil {
		return fmt.Errorf("write branch manifest: %w", err)
	}
	return nil
}

type branchOutcomeResult struct {
	File         string
	DecisionLine int
	ID           string
	Covered      bool
}

type branchEvaluation struct {
	Overall  coverageMetric
	Packages map[string]coverageMetric
	Outcomes []branchOutcomeResult
}

func evaluateBranchCoverage(root string, manifest branchManifest, data coverageData) (branchEvaluation, error) {
	files := make(map[string]bool, len(manifest.Files))
	var missing, unexpected []string
	for _, file := range manifest.Files {
		files[file.Path] = true
		if _, exists := data.Sources[file.Path]; !exists {
			unexpected = append(unexpected, file.Path)
		}
	}
	for path := range data.Sources {
		if !files[path] {
			missing = append(missing, path)
		}
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	if len(missing) > 0 || len(unexpected) > 0 {
		return branchEvaluation{}, fmt.Errorf("branch manifest does not match production sources: missing %v unexpected %v", missing, unexpected)
	}
	blocksByFile := make(map[string][]coverageBlock)
	for _, block := range data.Blocks {
		blocksByFile[block.File] = append(blocksByFile[block.File], block)
	}
	for _, blocks := range blocksByFile {
		sort.Slice(blocks, func(i, j int) bool {
			return branchPositionLess(branchPosition{Line: blocks[i].StartLine, Column: blocks[i].StartColumn}, branchPosition{Line: blocks[j].StartLine, Column: blocks[j].StartColumn})
		})
	}
	evaluation := branchEvaluation{Packages: make(map[string]coverageMetric), Outcomes: make([]branchOutcomeResult, 0)}
	packageValues := make(map[string][]bool)
	var overallValues []bool
	for _, file := range manifest.Files {
		source, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(file.Path)))
		if err != nil {
			return branchEvaluation{}, fmt.Errorf("read branch production source %s: %w", file.Path, err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(source)) != file.SHA256 {
			return branchEvaluation{}, fmt.Errorf("branch manifest is stale for %s; rerun go run ./tools/verify branch-instrument", file.Path)
		}
		blocks := blocksByFile[file.Path]
		for _, decision := range file.Decisions {
			for _, outcome := range decision.Outcomes {
				id := fmt.Sprintf("%s:%d:%d %s %s", file.Path, decision.Line, decision.Column, decision.Kind, outcome.Name)
				index := sort.Search(len(blocks), func(i int) bool {
					return !branchPositionLess(branchPosition{Line: blocks[i].StartLine, Column: blocks[i].StartColumn}, outcome.Start)
				})
				if index == len(blocks) || branchPositionLess(outcome.End, branchPosition{Line: blocks[index].StartLine, Column: blocks[index].StartColumn}) {
					return branchEvaluation{}, fmt.Errorf("branch outcome %s has no coverage block; instrumentation and profile do not match", id)
				}
				first := blocks[index]
				covered := false
				for ; index < len(blocks) && blocks[index].StartLine == first.StartLine && blocks[index].StartColumn == first.StartColumn; index++ {
					covered = covered || blocks[index].Count > 0
				}
				evaluation.Outcomes = append(evaluation.Outcomes, branchOutcomeResult{File: file.Path, DecisionLine: decision.Line, ID: id, Covered: covered})
				packageValues[file.Package] = append(packageValues[file.Package], covered)
				overallValues = append(overallValues, covered)
			}
		}
	}
	evaluation.Overall = boolMetric(overallValues)
	for pkg, values := range packageValues {
		evaluation.Packages[pkg] = boolMetric(values)
	}
	return evaluation, nil
}

func enforceBranchRatchet(name string, actual coverageMetric, ratchet, tolerance float64) []string {
	if actual.Percent+0.005 < ratchet-tolerance {
		return []string{fmt.Sprintf("%s branch ratchet %.2f%% (tolerance %.2f%%p) is above measured %.2f%%", name, ratchet, tolerance, actual.Percent)}
	}
	return nil
}

func branchRequirementShortfall(name string, actual coverageMetric, floor float64) (string, bool) {
	if actual.Percent+0.000001 < floor {
		return fmt.Sprintf("%s branch coverage %.2f%% is below requirement %.2f%%", name, actual.Percent, floor), true
	}
	return "", false
}

func diffBranchValues(changed map[string]map[int]struct{}, outcomes []branchOutcomeResult) []bool {
	var values []bool
	for _, outcome := range outcomes {
		if _, exists := changed[outcome.File][outcome.DecisionLine]; exists {
			values = append(values, outcome.Covered)
		}
	}
	return values
}

func validateBranchOutputDir(root, outputDir string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("resolve branch repository root: %w", err)
	}
	repositoryPath := root
	isWithin := func(parent, child string) bool {
		relative, err := filepath.Rel(parent, child)
		return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve branch repository root: %w", err)
	}
	output, err := filepath.Abs(outputDir)
	if err != nil {
		return fmt.Errorf("resolve branch output directory: %w", err)
	}
	// Check repository-local links before canonicalization can turn an
	// in-repository output into an apparently permitted external directory.
	for prefix := output; isWithin(repositoryPath, prefix); prefix = filepath.Dir(prefix) {
		relative, _ := filepath.Rel(repositoryPath, prefix)
		if relative == "." {
			break
		}
		if _, err := os.Readlink(prefix); err == nil {
			return fmt.Errorf("unsafe branch output directory %s: repository-local symlink or junction", outputDir)
		}
	}
	var suffix []string
	for {
		resolved, resolveErr := filepath.EvalSymlinks(output)
		if resolveErr == nil {
			output = resolved
			break
		}
		if !os.IsNotExist(resolveErr) {
			return fmt.Errorf("resolve branch output directory: %w", resolveErr)
		}
		parent := filepath.Dir(output)
		if parent == output {
			return fmt.Errorf("resolve branch output directory: %w", resolveErr)
		}
		suffix = append(suffix, filepath.Base(output))
		output = parent
	}
	for index := len(suffix) - 1; index >= 0; index-- {
		output = filepath.Join(output, suffix[index])
	}
	// EvalSymlinks can leave Windows directory junctions unresolved. Do not
	// delete through a remaining link anywhere in the output ancestry.
	for prefix := output; ; prefix = filepath.Dir(prefix) {
		if _, err := os.Readlink(prefix); err == nil {
			return fmt.Errorf("unsafe branch output directory %s: unresolved symlink or junction", outputDir)
		}
		if filepath.Dir(prefix) == prefix {
			break
		}
	}
	if isWithin(output, root) {
		return fmt.Errorf("unsafe branch output directory %s: repository root or ancestor", outputDir)
	}
	if isWithin(root, output) {
		branchRoot := filepath.Join(root, filepath.FromSlash(defaultBranchOutputDir))
		if !isWithin(branchRoot, output) {
			return fmt.Errorf("unsafe branch output directory %s: use %s or a subdirectory, or an output outside the repository", outputDir, defaultBranchOutputDir)
		}
	}
	return nil
}
