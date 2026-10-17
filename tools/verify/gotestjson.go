package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const (
	defaultGoTestJSONPath = ".artifacts/verification/go-test.json"

	goTestFailureOutputLines = 30
	goTestFailureTotalLines  = 400
)

// printGoTestFailures prints a compact list of failed tests from a `go test -json` file. The gate
// writes that stream to a file instead of the console, so this is how a failure becomes visible.
// A missing or unreadable file prints nothing.
func printGoTestFailures(out io.Writer, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	type event struct {
		Action     string `json:"Action"`
		Package    string `json:"Package"`
		ImportPath string `json:"ImportPath"`
		Test       string `json:"Test"`
		Output     string `json:"Output"`
	}
	output := make(map[string][]string)
	buildOutput := make(map[string][]string)
	failed := make(map[string]struct{})
	failedPackages := make(map[string]struct{})
	decoder := json.NewDecoder(bytes.NewReader(bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))))
	for {
		var item event
		if err := decoder.Decode(&item); err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Fprintf(out, "go test: stopped reading %s: %v\n", path, err)
			}
			break
		}
		key := item.Package + "\x00" + item.Test
		switch item.Action {
		case "output":
			output[key] = append(output[key], strings.TrimRight(item.Output, "\r\n"))
		case "build-output":
			buildOutput[item.ImportPath] = append(buildOutput[item.ImportPath], strings.TrimRight(item.Output, "\r\n"))
		case "fail":
			if item.Test == "" {
				failedPackages[item.Package] = struct{}{}
			} else {
				failed[key] = struct{}{}
			}
		}
	}

	leaves := make([]string, 0, len(failed))
	for key := range failed {
		leaf := true
		for other := range failed {
			if strings.HasPrefix(other, key+"/") {
				leaf = false
				break
			}
		}
		if leaf {
			leaves = append(leaves, key)
		}
	}
	sort.Strings(leaves)
	packagesWithFailedTests := make(map[string]struct{})
	for _, key := range leaves {
		pkg, _, _ := strings.Cut(key, "\x00")
		packagesWithFailedTests[pkg] = struct{}{}
	}
	var packagesOnly []string
	for pkg := range failedPackages {
		if _, hasTests := packagesWithFailedTests[pkg]; !hasTests {
			packagesOnly = append(packagesOnly, pkg)
		}
	}
	sort.Strings(packagesOnly)
	if len(leaves) == 0 && len(packagesOnly) == 0 {
		return
	}

	budget := goTestFailureTotalLines
	printLines := func(lines []string) {
		start := max(0, len(lines)-goTestFailureOutputLines)
		for _, line := range lines[start:] {
			if budget <= 0 {
				return
			}
			if strings.HasPrefix(line, "=== ") || strings.HasPrefix(strings.TrimSpace(line), "--- PASS") {
				continue
			}
			budget--
			fmt.Fprintf(out, "    %s\n", line)
		}
	}
	fmt.Fprintf(out, "go test: %d failed test(s), %d failed package(s) without a failed test (from %s)\n", len(leaves), len(packagesOnly), path)
	for _, key := range leaves {
		pkg, test, _ := strings.Cut(key, "\x00")
		fmt.Fprintf(out, "  FAIL %s %s\n", pkg, test)
		printLines(output[key])
	}
	for _, pkg := range packagesOnly {
		fmt.Fprintf(out, "  FAIL %s (package failed without a failed test)\n", pkg)
		for importPath, lines := range buildOutput {
			if importPath == pkg || strings.HasPrefix(importPath, pkg+" ") {
				printLines(lines)
			}
		}
		printLines(output[pkg+"\x00"])
	}
	if budget <= 0 {
		fmt.Fprintf(out, "  ... output truncated; see %s\n", path)
	}
}
