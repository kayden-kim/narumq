package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strings"
)

type importPackage struct {
	ImportPath string
	Imports    []string
	Error      *goListError
}

type goListError struct {
	Err string `json:"Err"`
}

func verifyArchitecture(ctx context.Context, root string) error {
	packages, err := goListProductionPackages(ctx, root)
	if err != nil {
		return err
	}
	if issues := validateImportGraph(packages); len(issues) > 0 {
		return issues
	}
	return nil
}

func goListProductionPackages(ctx context.Context, root string) ([]importPackage, error) {
	command := exec.CommandContext(ctx, "go", "list", "-json", "./cmd/...", "./internal/...")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("go list production packages failed: %w: %s", err, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return nil, fmt.Errorf("go list production packages: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	packages := make([]importPackage, 0)
	for {
		var item struct {
			ImportPath string       `json:"ImportPath"`
			Imports    []string     `json:"Imports"`
			Error      *goListError `json:"Error"`
		}
		if err := decoder.Decode(&item); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("decode go list output: %w", err)
		}
		packages = append(packages, importPackage{ImportPath: item.ImportPath, Imports: item.Imports, Error: item.Error})
	}
	return packages, nil
}

func validateImportGraph(packages []importPackage) architectureIssues {
	var issues architectureIssues
	for _, item := range packages {
		if item.Error != nil && item.Error.Err != "" {
			issues = append(issues, architectureIssue{Message: fmt.Sprintf("%s: go list package error: %s", item.ImportPath, item.Error.Err)})
			continue
		}
		if !isProductionPackage(item.ImportPath) {
			issues = append(issues, architectureIssue{Message: "unexpected package outside production graph: " + item.ImportPath})
			continue
		}
		for _, imported := range item.Imports {
			if allowedImport(item.ImportPath, imported) {
				continue
			}
			issues = append(issues, architectureIssue{Message: fmt.Sprintf("%s imports forbidden package %s", item.ImportPath, imported)})
		}
	}
	return issues
}

type architectureIssue struct {
	Message string
}

type architectureIssues []architectureIssue

func (issues architectureIssues) Error() string {
	messages := make([]string, 0, len(issues))
	for _, issue := range issues {
		messages = append(messages, issue.Message)
	}
	sort.Strings(messages)
	return strings.Join(messages, "\n")
}

func isProductionPackage(path string) bool {
	return strings.HasPrefix(path, modulePath+"/cmd/") || strings.HasPrefix(path, modulePath+"/internal/")
}

func allowedImport(source, imported string) bool {
	if imported == source {
		return true
	}
	if strings.HasPrefix(imported, modulePath+"/tests/") || strings.HasPrefix(imported, modulePath+"/tools/") || strings.HasPrefix(imported, modulePath+"/cmd/") {
		return false
	}
	if isStandardImport(imported) {
		return true
	}
	if strings.HasPrefix(source, modulePath+"/cmd/narumq") {
		return strings.HasPrefix(imported, modulePath+"/internal/adapters/") ||
			strings.HasPrefix(imported, modulePath+"/internal/application/") ||
			strings.HasPrefix(imported, modulePath+"/internal/domain/")
	}
	if strings.HasPrefix(source, modulePath+"/internal/domain/") {
		return strings.HasPrefix(imported, modulePath+"/internal/domain/")
	}
	if strings.HasPrefix(source, modulePath+"/internal/application/") {
		return strings.HasPrefix(imported, modulePath+"/internal/application/") ||
			strings.HasPrefix(imported, modulePath+"/internal/domain/")
	}
	if strings.HasPrefix(source, modulePath+"/internal/adapters/") {
		if strings.HasPrefix(imported, modulePath+"/internal/adapters/") {
			return false
		}
		return strings.HasPrefix(imported, modulePath+"/internal/application/") ||
			strings.HasPrefix(imported, modulePath+"/internal/domain/") ||
			isThirdPartyImport(imported)
	}
	return false
}

func isStandardImport(path string) bool {
	return !strings.Contains(path, ".") && !strings.HasPrefix(path, modulePath+"/")
}

func isThirdPartyImport(path string) bool {
	return !isStandardImport(path) && !strings.HasPrefix(path, modulePath+"/")
}

func sortedImports(imports []string) []string {
	result := append([]string(nil), imports...)
	sort.Strings(result)
	return result
}
