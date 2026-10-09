package main

import (
	"bytes"
	"encoding/json"
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
)

const (
	defaultMatrixPath = "tests/conformance/mqtt-compliance-v1.json"
	modulePath        = "narumq"
)

type complianceMatrix struct {
	SchemaVersion    int               `json:"schemaVersion"`
	Profile          string            `json:"profile"`
	ProtocolVersions []string          `json:"protocolVersions"`
	Statements       []matrixStatement `json:"statements"`
}

type matrixStatement struct {
	ID             string          `json:"id"`
	RequirementIDs []string        `json:"requirementIds"`
	Tests          []testReference `json:"tests"`
}

type testReference struct {
	Package string `json:"package"`
	Name    string `json:"name"`
}

type testMetadata struct {
	Reference    testReference
	File         string
	Line         int
	Requirements []string
	Statements   []string
}

type traceabilityIssue struct {
	Message string
}

type traceabilityIssues []traceabilityIssue

func (issues traceabilityIssues) Error() string {
	messages := make([]string, 0, len(issues))
	for _, issue := range issues {
		messages = append(messages, issue.Message)
	}
	sort.Strings(messages)
	return strings.Join(messages, "\n")
}

var (
	statementTokenPattern   = regexp.MustCompile(`\bMQTT-\d+\.\d+\.\d+(?:-\d+(?:/\d+)*)?\b`)
	statementPattern        = regexp.MustCompile(`^MQTT-\d+\.\d+\.\d+-\d+$`)
	requirementTokenPattern = regexp.MustCompile(`\b(?:MQTT|CLU|TEN|SEC|API|UI|DEP|CLIENT|NFR|PERF|TEST)-\d{2}(?:/\d{2})*\b`)
	functionNamePattern     = regexp.MustCompile(`^(?:Test|Fuzz)[A-Za-z0-9_]+$`)
)

func loadComplianceMatrix(path string) (complianceMatrix, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return complianceMatrix{}, fmt.Errorf("read compliance matrix %s: %w", path, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var matrix complianceMatrix
	if err := decoder.Decode(&matrix); err != nil {
		return complianceMatrix{}, fmt.Errorf("decode compliance matrix %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return complianceMatrix{}, fmt.Errorf("decode compliance matrix %s: trailing JSON value", path)
		}
		return complianceMatrix{}, fmt.Errorf("decode compliance matrix %s: trailing JSON: %w", path, err)
	}
	if issues := validateComplianceMatrix(matrix); len(issues) > 0 {
		return complianceMatrix{}, fmt.Errorf("invalid compliance matrix %s:\n%w", path, issues)
	}
	return matrix, nil
}

func validateComplianceMatrix(matrix complianceMatrix) traceabilityIssues {
	var issues traceabilityIssues
	if matrix.SchemaVersion != 1 {
		issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("matrix schemaVersion = %d, want 1", matrix.SchemaVersion)})
	}
	if matrix.Profile != "phase0-single-node-ingress" {
		issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("matrix profile = %q, want %q", matrix.Profile, "phase0-single-node-ingress")})
	}
	if !equalStrings(matrix.ProtocolVersions, []string{"3.1.1", "5.0"}) {
		issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("matrix protocolVersions = %v, want [3.1.1 5.0]", matrix.ProtocolVersions)})
	}
	if len(matrix.Statements) == 0 {
		issues = append(issues, traceabilityIssue{Message: "matrix statements must not be empty"})
	}

	seenStatements := make(map[string]struct{}, len(matrix.Statements))
	previousID := ""
	for index, statement := range matrix.Statements {
		location := fmt.Sprintf("statement[%d]", index)
		if statement.ID == "" {
			issues = append(issues, traceabilityIssue{Message: location + ".id is empty"})
		} else if !statementPattern.MatchString(statement.ID) {
			issues = append(issues, traceabilityIssue{Message: location + ".id = " + statement.ID + ": invalid MQTT statement ID"})
		}
		if _, exists := seenStatements[statement.ID]; exists {
			issues = append(issues, traceabilityIssue{Message: "duplicate statement ID: " + statement.ID})
		}
		seenStatements[statement.ID] = struct{}{}
		if previousID != "" && statement.ID <= previousID {
			issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("statements are not strictly sorted: %s before %s", previousID, statement.ID)})
		}
		previousID = statement.ID

		issues = append(issues, validateSortedRequirements(location+".requirementIds", statement.RequirementIDs)...)
		if len(statement.RequirementIDs) == 0 {
			issues = append(issues, traceabilityIssue{Message: location + ".requirementIds must not be empty"})
		}
		if len(statement.Tests) == 0 {
			issues = append(issues, traceabilityIssue{Message: location + ".tests must not be empty"})
		}
		previousTest := ""
		seenTests := make(map[string]struct{}, len(statement.Tests))
		for testIndex, test := range statement.Tests {
			testLocation := fmt.Sprintf("%s.tests[%d]", location, testIndex)
			if test.Package == "" || test.Name == "" {
				issues = append(issues, traceabilityIssue{Message: testLocation + " must contain package and name"})
			}
			if !strings.HasPrefix(test.Package, modulePath+"/") {
				issues = append(issues, traceabilityIssue{Message: testLocation + ".package = " + test.Package + ": must use module-relative import path"})
			}
			if !functionNamePattern.MatchString(test.Name) {
				issues = append(issues, traceabilityIssue{Message: testLocation + ".name = " + test.Name + ": invalid test function name"})
			}
			key := test.Package + "#" + test.Name
			if _, exists := seenTests[key]; exists {
				issues = append(issues, traceabilityIssue{Message: "duplicate test reference in " + statement.ID + ": " + key})
			}
			seenTests[key] = struct{}{}
			if previousTest != "" && key <= previousTest {
				issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("tests for %s are not strictly sorted: %s before %s", statement.ID, previousTest, key)})
			}
			previousTest = key
		}
	}
	return issues
}

func validateSortedRequirements(location string, values []string) traceabilityIssues {
	var issues traceabilityIssues
	previous := ""
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !validRequirementID(value) {
			issues = append(issues, traceabilityIssue{Message: location + " contains invalid requirement ID: " + value})
		}
		if _, exists := seen[value]; exists {
			issues = append(issues, traceabilityIssue{Message: location + " contains duplicate requirement ID: " + value})
		}
		seen[value] = struct{}{}
		if previous != "" && value <= previous {
			issues = append(issues, traceabilityIssue{Message: location + " is not strictly sorted"})
		}
		previous = value
	}
	return issues
}

func validRequirementID(value string) bool {
	if len(value) != len("MQTT-00") && len(value) != len("TEST-00") {
		return false
	}
	if strings.HasPrefix(value, "MQTT-") {
		return value >= "MQTT-01" && value <= "MQTT-13"
	}
	if strings.HasPrefix(value, "TEST-") {
		return value >= "TEST-01" && value <= "TEST-17"
	}
	return false
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func collectTestMetadata(root string) ([]testMetadata, error) {
	files, err := testSourceFiles(root)
	if err != nil {
		return nil, err
	}
	fileSet := token.NewFileSet()
	metadata := make([]testMetadata, 0)
	var issues traceabilityIssues
	for _, file := range files {
		parsed, err := goparser.ParseFile(fileSet, file, nil, goparser.ParseComments)
		if err != nil {
			issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("%s: parse test source: %v", relativePath(root, file), err)})
			continue
		}
		relativeDir := filepath.ToSlash(filepath.Dir(relativePath(root, file)))
		packagePath := modulePath + "/" + relativeDir
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || function.Name == nil || !functionNamePattern.MatchString(function.Name.Name) || function.Doc == nil {
				continue
			}
			text := function.Doc.Text()
			marker := strings.Index(text, "Traceability:")
			if marker < 0 {
				continue
			}
			trace := text[marker+len("Traceability:"):]
			requirements, statementIDs, parseIssues := parseTraceability(trace)
			for _, issue := range parseIssues {
				issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("%s:%d %s: %s", relativePath(root, file), fileSet.Position(function.Pos()).Line, function.Name.Name, issue)})
			}
			metadata = append(metadata, testMetadata{
				Reference:    testReference{Package: packagePath, Name: function.Name.Name},
				File:         relativePath(root, file),
				Line:         fileSet.Position(function.Pos()).Line,
				Requirements: requirements,
				Statements:   statementIDs,
			})
		}
	}
	if len(issues) > 0 {
		return nil, issues
	}
	sort.Slice(metadata, func(left, right int) bool {
		return testReferenceKey(metadata[left].Reference) < testReferenceKey(metadata[right].Reference)
	})
	return metadata, nil
}

func testSourceFiles(root string) ([]string, error) {
	var files []string
	for _, directory := range []string{"cmd", "internal", "tests"} {
		path := filepath.Join(root, directory)
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("stat test source root %s: %w", directory, err)
		}
		err := filepath.WalkDir(path, func(file string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			if strings.HasSuffix(entry.Name(), "_test.go") {
				files = append(files, file)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk test source root %s: %w", directory, err)
		}
	}
	sort.Strings(files)
	return files, nil
}

func parseTraceability(trace string) ([]string, []string, []string) {
	var issues []string
	requirementMatches := requirementTokenPattern.FindAllString(trace, -1)
	requirements := make([]string, 0)
	seenRequirements := make(map[string]struct{})
	for _, match := range requirementMatches {
		base, suffix := splitTraceabilityID(match)
		for _, number := range strings.Split(suffix, "/") {
			id := base + "-" + number
			if _, exists := seenRequirements[id]; exists {
				issues = append(issues, "duplicate requirement ID "+id)
				continue
			}
			if strings.HasPrefix(id, "MQTT-") || strings.HasPrefix(id, "TEST-") {
				if !validRequirementID(id) {
					issues = append(issues, "invalid requirement ID "+id)
					continue
				}
			}
			seenRequirements[id] = struct{}{}
			requirements = append(requirements, id)
		}
	}
	sort.Strings(requirements)

	statementMatches := statementTokenPattern.FindAllString(trace, -1)
	statements := make([]string, 0)
	seenStatements := make(map[string]struct{})
	for _, match := range statementMatches {
		if !strings.Contains(match, "-") || !strings.Contains(strings.TrimPrefix(match, "MQTT-"), "-") {
			issues = append(issues, "invalid MQTT statement ID "+match)
			continue
		}
		base, suffix := splitTraceabilityID(match)
		for _, number := range strings.Split(suffix, "/") {
			id := base + "-" + number
			if !statementPattern.MatchString(id) {
				issues = append(issues, "invalid MQTT statement ID "+id)
				continue
			}
			if _, exists := seenStatements[id]; exists {
				issues = append(issues, "duplicate MQTT statement ID "+id)
				continue
			}
			seenStatements[id] = struct{}{}
			statements = append(statements, id)
		}
	}
	hasMQTTOrTestRequirement := false
	for requirementID := range seenRequirements {
		if strings.HasPrefix(requirementID, "MQTT-") || strings.HasPrefix(requirementID, "TEST-") {
			hasMQTTOrTestRequirement = true
			break
		}
	}
	if len(statements) > 0 && !hasMQTTOrTestRequirement {
		issues = append(issues, "MQTT statement metadata must declare at least one MQTT or TEST requirement ID")
	}
	sort.Strings(statements)
	return requirements, statements, issues
}

func splitTraceabilityID(value string) (string, string) {
	index := strings.LastIndexByte(value, '-')
	return value[:index], value[index+1:]
}

func verifyTraceability(root, matrixPath string) error {
	matrix, err := loadComplianceMatrix(matrixPath)
	if err != nil {
		return err
	}
	metadata, err := collectTestMetadata(root)
	if err != nil {
		return fmt.Errorf("collect test metadata: %w", err)
	}

	byReference := make(map[string]testMetadata, len(metadata))
	var issues traceabilityIssues
	for _, item := range metadata {
		if len(item.Statements) == 0 {
			continue
		}
		key := testReferenceKey(item.Reference)
		if _, exists := byReference[key]; exists {
			issues = append(issues, traceabilityIssue{Message: "duplicate test function reference: " + key})
			continue
		}
		byReference[key] = item
	}

	matrixByID := make(map[string]matrixStatement, len(matrix.Statements))
	for _, statement := range matrix.Statements {
		matrixByID[statement.ID] = statement
	}
	expectedReferences := make(map[string]map[string]testReference)
	expectedRequirements := make(map[string]map[string]struct{})
	for _, item := range metadata {
		if len(item.Statements) == 0 {
			continue
		}
		for _, statementID := range item.Statements {
			if _, exists := expectedReferences[statementID]; !exists {
				expectedReferences[statementID] = make(map[string]testReference)
				expectedRequirements[statementID] = make(map[string]struct{})
			}
			key := testReferenceKey(item.Reference)
			expectedReferences[statementID][key] = item.Reference
			for _, requirementID := range item.Requirements {
				if strings.HasPrefix(requirementID, "MQTT-") || strings.HasPrefix(requirementID, "TEST-") {
					expectedRequirements[statementID][requirementID] = struct{}{}
				}
			}
		}
	}

	for statementID, references := range expectedReferences {
		statement, exists := matrixByID[statementID]
		if !exists {
			issues = append(issues, traceabilityIssue{Message: "matrix missing statement " + statementID})
			continue
		}
		actualReferences := make(map[string]testReference, len(statement.Tests))
		for _, reference := range statement.Tests {
			key := testReferenceKey(reference)
			actualReferences[key] = reference
			item, exists := byReference[key]
			if !exists {
				issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("%s references missing test %s", statementID, key)})
				continue
			}
			if !containsString(item.Statements, statementID) {
				issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("%s references %s but its Traceability comment omits the statement", statementID, key)})
			}
		}
		for key := range references {
			if _, exists := actualReferences[key]; !exists {
				issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("matrix statement %s missing reverse test reference %s", statementID, key)})
			}
		}
		for key := range actualReferences {
			if _, exists := references[key]; !exists {
				issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("matrix statement %s has stale test reference %s", statementID, key)})
			}
		}
		expected := sortedSet(expectedRequirements[statementID])
		if !equalStrings(statement.RequirementIDs, expected) {
			issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("matrix statement %s requirementIds = %v, want source union %v", statementID, statement.RequirementIDs, expected)})
		}
	}
	for statementID := range matrixByID {
		if _, exists := expectedReferences[statementID]; !exists {
			issues = append(issues, traceabilityIssue{Message: "matrix has stale statement " + statementID})
		}
	}
	if len(issues) > 0 {
		return issues
	}
	return nil
}

func testReferenceKey(reference testReference) string {
	return reference.Package + "#" + reference.Name
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func relativePath(root, path string) string {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(relative)
}
