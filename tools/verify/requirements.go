package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	defaultPRDPath          = "docs/requirements/PRD.md"
	defaultP0ExceptionsPath = "tests/conformance/p0-requirement-exceptions-v1.json"
)

var prdRequirementIDPattern = regexp.MustCompile(`^[A-Z]+-\d{2}$`)

type prdRequirements struct {
	All map[string]struct{}
	P0  []string
}

type p0ExceptionList struct {
	SchemaVersion int           `json:"schemaVersion"`
	Exceptions    []p0Exception `json:"exceptions"`
}

type p0Exception struct {
	RequirementID string `json:"requirementId"`
	Reason        string `json:"reason"`
	Tracking      string `json:"tracking"`
}

type p0MappingSummary struct {
	Total      int
	Mapped     int
	Exceptions int
}

func loadPRDRequirements(path string) (prdRequirements, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return prdRequirements{}, fmt.Errorf("read PRD %s: %w", path, err)
	}

	requirements := prdRequirements{All: make(map[string]struct{})}
	inIDTable := false
	hasPriorityColumn := false
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			inIDTable = false
			continue
		}
		cells := splitPRDTableRow(line)
		if !inIDTable {
			if len(cells) == 0 || cells[0] != "ID" {
				continue
			}
			inIDTable = true
			hasPriorityColumn = len(cells) > 1 && cells[1] == "우선순위"
			continue
		}
		if len(cells) == 0 || !prdRequirementIDPattern.MatchString(cells[0]) {
			continue
		}
		id := cells[0]
		if _, exists := requirements.All[id]; exists {
			return prdRequirements{}, fmt.Errorf("PRD requirement %s appears in multiple rows", id)
		}
		requirements.All[id] = struct{}{}
		if hasPriorityColumn && len(cells) > 1 && cells[1] == "P0" {
			requirements.P0 = append(requirements.P0, id)
		}
	}

	sort.Strings(requirements.P0)
	if len(requirements.P0) == 0 {
		return prdRequirements{}, fmt.Errorf("PRD has no P0 requirement rows")
	}
	return requirements, nil
}

func splitPRDTableRow(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")
	cells := strings.Split(line, "|")
	for index := range cells {
		cells[index] = strings.TrimSpace(cells[index])
	}
	return cells
}

func loadP0Exceptions(root, path string, p0 map[string]struct{}) (p0ExceptionList, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return p0ExceptionList{}, fmt.Errorf("read P0 exception list %s: %w", path, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var exceptions p0ExceptionList
	if err := decoder.Decode(&exceptions); err != nil {
		return p0ExceptionList{}, fmt.Errorf("decode P0 exception list %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return p0ExceptionList{}, fmt.Errorf("decode P0 exception list %s: trailing JSON value", path)
		}
		return p0ExceptionList{}, fmt.Errorf("decode P0 exception list %s: trailing JSON: %w", path, err)
	}
	if exceptions.SchemaVersion != 1 {
		return p0ExceptionList{}, fmt.Errorf("P0 exception list schemaVersion = %d, want 1", exceptions.SchemaVersion)
	}

	seen := make(map[string]struct{}, len(exceptions.Exceptions))
	previousID := ""
	for _, exception := range exceptions.Exceptions {
		if exception.RequirementID == "" {
			return p0ExceptionList{}, fmt.Errorf("exception requirementId must not be empty")
		}
		if _, exists := seen[exception.RequirementID]; exists {
			return p0ExceptionList{}, fmt.Errorf("duplicate exception %s", exception.RequirementID)
		}
		seen[exception.RequirementID] = struct{}{}
		if previousID != "" && exception.RequirementID <= previousID {
			return p0ExceptionList{}, fmt.Errorf("exceptions are not strictly sorted")
		}
		previousID = exception.RequirementID
		if _, exists := p0[exception.RequirementID]; !exists {
			return p0ExceptionList{}, fmt.Errorf("exception %s is not a P0 requirement", exception.RequirementID)
		}
		if strings.TrimSpace(exception.Reason) == "" {
			return p0ExceptionList{}, fmt.Errorf("exception %s reason must not be empty", exception.RequirementID)
		}
		if strings.TrimSpace(exception.Tracking) == "" {
			return p0ExceptionList{}, fmt.Errorf("exception %s tracking must not be empty", exception.RequirementID)
		}
		if err := validateP0ExceptionTracking(root, exception.RequirementID, exception.Tracking); err != nil {
			return p0ExceptionList{}, err
		}
	}
	return exceptions, nil
}

func validateP0ExceptionTracking(root, requirementID, tracking string) error {
	path, _, _ := strings.Cut(tracking, "#")
	if path == "" {
		return fmt.Errorf("exception %s tracking path must not be empty", requirementID)
	}
	if strings.Contains(path, "..") {
		return fmt.Errorf("exception %s tracking path %s must not contain ..", requirementID, path)
	}
	localPath := filepath.FromSlash(path)
	if filepath.IsAbs(localPath) || filepath.VolumeName(localPath) != "" {
		return fmt.Errorf("exception %s tracking path %s must be repository-relative", requirementID, path)
	}
	resolvedPath := filepath.Join(root, localPath)
	if _, err := os.Stat(resolvedPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("exception %s tracking path %s does not exist", requirementID, path)
		}
		return fmt.Errorf("check exception %s tracking path %s: %w", requirementID, path, err)
	}
	return nil
}

func verifyP0RequirementMapping(root, prdPath, exceptionsPath string) (p0MappingSummary, error) {
	prd, err := loadPRDRequirements(prdPath)
	if err != nil {
		return p0MappingSummary{}, err
	}
	p0 := make(map[string]struct{}, len(prd.P0))
	for _, id := range prd.P0 {
		p0[id] = struct{}{}
	}
	exceptions, err := loadP0Exceptions(root, exceptionsPath, p0)
	if err != nil {
		return p0MappingSummary{}, err
	}
	metadata, err := collectTestMetadata(root)
	if err != nil {
		return p0MappingSummary{}, fmt.Errorf("collect test metadata: %w", err)
	}

	mappedTests := make(map[string]map[string]struct{})
	var issues traceabilityIssues
	for _, item := range metadata {
		for _, requirementID := range item.Requirements {
			if _, exists := prd.All[requirementID]; !exists {
				issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("%s:%d %s: unknown PRD requirement %s", item.File, item.Line, item.Reference.Name, requirementID)})
			}
			if _, isP0 := p0[requirementID]; !isP0 {
				continue
			}
			if _, exists := mappedTests[requirementID]; !exists {
				mappedTests[requirementID] = make(map[string]struct{})
			}
			mappedTests[requirementID][testReferenceKey(item.Reference)] = struct{}{}
		}
	}

	exceptionIDs := make(map[string]struct{}, len(exceptions.Exceptions))
	for _, exception := range exceptions.Exceptions {
		exceptionIDs[exception.RequirementID] = struct{}{}
	}
	mappedCount := 0
	for _, requirementID := range prd.P0 {
		mappedCountForID := len(mappedTests[requirementID])
		_, hasException := exceptionIDs[requirementID]
		if mappedCountForID > 0 {
			mappedCount++
		}
		if mappedCountForID == 0 && !hasException {
			issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("P0 requirement %s has no Traceability test and no exception in %s", requirementID, exceptionsPath)})
		}
		if mappedCountForID > 0 && hasException {
			issues = append(issues, traceabilityIssue{Message: fmt.Sprintf("P0 requirement %s is mapped by %d test(s) but still listed in %s", requirementID, mappedCountForID, exceptionsPath)})
		}
	}
	if len(issues) > 0 {
		return p0MappingSummary{}, issues
	}
	return p0MappingSummary{Total: len(prd.P0), Mapped: mappedCount, Exceptions: len(exceptions.Exceptions)}, nil
}
