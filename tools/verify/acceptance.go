package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const defaultAcceptanceGoTestJSONPath = ".artifacts/verification/acceptance-go-test.json"

// runAcceptance runs the Go tests marked "Acceptance: <ENV>" outside the default gate. Every selected
// test must pass: a skip, a failure or a missing result fails the command, so an unprepared
// environment never counts as acceptance evidence.
//
// Scenarios: performance-and-verification.md:PRF-SCN-116
func runAcceptance(ctx context.Context, root, runPattern string, getenv func(string) string, runner commandRunner, out io.Writer) error {
	if err := ensureRepositoryRoot(root); err != nil {
		return err
	}
	tests, err := selectAcceptanceTests(root, runPattern)
	if err != nil {
		return err
	}
	var missing []string
	seenEnvironment := make(map[string]struct{})
	for _, test := range tests {
		if _, seen := seenEnvironment[test.Acceptance]; seen {
			continue
		}
		seenEnvironment[test.Acceptance] = struct{}{}
		if getenv(test.Acceptance) != "1" {
			missing = append(missing, test.Acceptance+"=1")
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("set %s before running the selected acceptance tests", strings.Join(missing, ", "))
	}

	names := make([]string, 0, len(tests))
	directories := make(map[string]struct{})
	for _, test := range tests {
		names = append(names, test.Name)
		directories["./"+path.Dir(filepath.ToSlash(test.File))] = struct{}{}
	}
	args := []string{"test", "-json", "-count=1", "-timeout=3h", "-run", "^(" + strings.Join(names, "|") + ")$"}
	packages := make([]string, 0, len(directories))
	for directory := range directories {
		packages = append(packages, directory)
	}
	sort.Strings(packages)
	args = append(args, packages...)
	fmt.Fprintf(out, "acceptance: running %d tests in %s\n", len(tests), strings.Join(packages, " "))
	runErr := runner(ctx, root, commandSpec{Name: "go", Args: args, StdoutFile: defaultAcceptanceGoTestJSONPath})

	resultPath := filepath.Join(root, filepath.FromSlash(defaultAcceptanceGoTestJSONPath))
	results := newScenarioResults()
	if err := loadGoTestJSON(resultPath, results); err != nil {
		return errors.Join(runErr, err)
	}
	var notPassed []string
	for _, test := range tests {
		status := results.outcomes[test.Key].Status
		if status == testResultPass {
			continue
		}
		reason := scenarioReasonNoResult
		switch status {
		case testResultFail:
			reason = "실패"
		case testResultSkip:
			reason = scenarioReasonSkipped
		}
		notPassed = append(notPassed, fmt.Sprintf("%s (%s)", test.Name, reason))
	}
	if runErr != nil || len(notPassed) > 0 {
		printGoTestFailures(out, resultPath)
		if len(notPassed) == 0 {
			return runErr
		}
		return errors.Join(runErr, fmt.Errorf("acceptance tests without a pass result: %s", strings.Join(notPassed, ", ")))
	}
	fmt.Fprintf(out, "acceptance: %d tests passed; results in %s\n", len(tests), defaultAcceptanceGoTestJSONPath)
	return nil
}

// selectAcceptanceTests returns the marked Go tests whose name matches runPattern, sorted by key.
// Scan problems stop the command because they can hide or misattribute an acceptance test.
func selectAcceptanceTests(root, runPattern string) ([]scenarioTest, error) {
	var filter *regexp.Regexp
	if runPattern != "" {
		compiled, err := regexp.Compile(runPattern)
		if err != nil {
			return nil, fmt.Errorf("invalid --run pattern: %w", err)
		}
		filter = compiled
	}
	refs, problems, err := scanScenarioTests(root)
	if err != nil {
		return nil, err
	}
	if len(problems) > 0 {
		messages := make([]string, 0, len(problems))
		for _, problem := range problems {
			messages = append(messages, problem.String())
		}
		return nil, fmt.Errorf("fix test metadata first: %s", strings.Join(messages, "; "))
	}
	selected := make(map[string]scenarioTest)
	for _, ref := range refs {
		test := ref.Test
		if test.Acceptance == "" || (filter != nil && !filter.MatchString(test.Name)) {
			continue
		}
		selected[test.Key] = test
	}
	if len(selected) == 0 {
		if runPattern != "" {
			return nil, fmt.Errorf("no Acceptance test matches --run %q", runPattern)
		}
		return nil, errors.New("no Acceptance test found")
	}
	tests := make([]scenarioTest, 0, len(selected))
	for _, test := range selected {
		tests = append(tests, test)
	}
	sort.Slice(tests, func(left, right int) bool { return tests[left].Key < tests[right].Key })
	return tests, nil
}
