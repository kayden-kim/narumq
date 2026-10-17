package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		printUsage()
		return 2
	}
	root, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "verify:", err)
		return 1
	}
	ctx := context.Background()
	switch args[0] {
	case "traceability":
		flags := flag.NewFlagSet("traceability", flag.ContinueOnError)
		matrixPath := flags.String("matrix", defaultMatrixPath, "compliance matrix path")
		prdPath := flags.String("prd", defaultPRDPath, "PRD requirement table path")
		p0ExceptionsPath := flags.String("p0-exceptions", defaultP0ExceptionsPath, "P0 requirement exception list path")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if err := verifyTraceability(root, filepath.Clean(*matrixPath)); err != nil {
			return printCommandError("traceability", err)
		}
		summary, err := verifyP0RequirementMapping(root, filepath.Clean(*prdPath), filepath.Clean(*p0ExceptionsPath))
		if err != nil {
			return printCommandError("traceability", err)
		}
		fmt.Printf("traceability: PASS (P0 mapped %d/%d, exceptions %d)\n", summary.Mapped, summary.Total, summary.Exceptions)
		return 0
	case "architecture":
		flags := flag.NewFlagSet("architecture", flag.ContinueOnError)
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if err := verifyArchitecture(ctx, root); err != nil {
			return printCommandError("architecture", err)
		}
		fmt.Println("architecture: PASS")
		return 0
	case "markdown-links":
		flags := flag.NewFlagSet("markdown-links", flag.ContinueOnError)
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "markdown-links: does not accept positional arguments")
			return 2
		}
		summary, err := verifyMarkdownLinks(root)
		if err != nil {
			return printCommandError("markdown-links", err)
		}
		fmt.Printf("markdown-links: PASS (files %d, local links %d)\n", summary.MarkdownFiles, summary.LocalLinks)
		return 0
	case "scenarios":
		flags := flag.NewFlagSet("scenarios", flag.ContinueOnError)
		var goTestJSON, results stringListFlag
		flags.Var(&goTestJSON, "go-test-json", "go test -json event stream file (repeatable)")
		flags.Var(&results, "results", "JSON Lines result file for non-Go tests (repeatable)")
		outputDir := flags.String("output", defaultScenarioOutputDir, "scenario report output directory")
		requireExecuted := flags.Bool("require-executed", false, "also fail on linked tests in the executed scope that have no pass result")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "scenarios: does not accept positional arguments")
			return 2
		}
		report, err := runScenarios(root, scenarioOptions{GoTestJSON: goTestJSON, Results: results, OutputDir: filepath.Clean(*outputDir), RequireExecuted: *requireExecuted}, time.Now())
		if err != nil {
			return printCommandError("scenarios", err)
		}
		printScenarioFindings(os.Stderr, report)
		fmt.Println(formatScenarioSummary(report.Summary))
		if report.Summary.hasProblems() {
			return 1
		}
		return 0

	case "branch-instrument":
		flags := flag.NewFlagSet("branch-instrument", flag.ContinueOnError)
		policyPath := flags.String("policy", defaultCoveragePolicyPath, "coverage policy path")
		outputDir := flags.String("output", defaultBranchOutputDir, "branch instrumentation output directory")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if err := runBranchInstrument(ctx, root, filepath.Clean(*policyPath), filepath.Clean(*outputDir)); err != nil {
			return printCommandError("branch-instrument", err)
		}
		fmt.Println("branch-instrument: PASS")
		return 0
	case "coverage":
		flags := flag.NewFlagSet("coverage", flag.ContinueOnError)
		profilePath := flags.String("profile", "", "Go coverage profile path")
		policyPath := flags.String("policy", defaultCoveragePolicyPath, "coverage policy path")
		reportPath := flags.String("report", ".artifacts/verification/coverage-summary.json", "coverage report path")
		manifestPath := flags.String("branch-manifest", defaultBranchManifestPath, "branch instrumentation manifest path")
		base := flags.String("base", "", "base git ref for diff coverage")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if *profilePath == "" {
			fmt.Fprintln(os.Stderr, "coverage: --profile is required")
			return 2
		}
		if err := runCoverage(ctx, root, filepath.Clean(*profilePath), filepath.Clean(*policyPath), filepath.Clean(*reportPath), filepath.Clean(*manifestPath), *base); err != nil {
			return printCommandError("coverage", err)
		}
		fmt.Println("coverage: PASS")
		return 0
	case "local":
		flags := flag.NewFlagSet("local", flag.ContinueOnError)
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if err := runLocal(ctx, root, executeCommand); err != nil {
			return printCommandError("local", err)
		}
		fmt.Println("local: PASS")
		return 0
	case "ci":
		flags := flag.NewFlagSet("ci", flag.ContinueOnError)
		base := flags.String("base", "", "base git ref for diff coverage")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if err := runCI(ctx, root, *base, executeCommand); err != nil {
			return printCommandError("ci", err)
		}
		fmt.Println("ci: PASS")
		return 0
	case "acceptance":
		flags := flag.NewFlagSet("acceptance", flag.ContinueOnError)
		runPattern := flags.String("run", "", "Go regexp that selects Acceptance tests by name")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "acceptance: does not accept positional arguments")
			return 2
		}
		if err := runAcceptance(ctx, root, *runPattern, os.Getenv, executeCommand, os.Stdout); err != nil {
			return printCommandError("acceptance", err)
		}
		fmt.Println("acceptance: PASS")
		return 0
	case "mutation":
		flags := flag.NewFlagSet("mutation", flag.ContinueOnError)
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "mutation: does not accept positional arguments")
			return 2
		}
		if err := runMutation(ctx, root, executeCommand); err != nil {
			return printCommandError("mutation", err)
		}
		fmt.Println("mutation: PASS")
		return 0
	case "baseline":
		flags := flag.NewFlagSet("baseline", flag.ContinueOnError)
		resultsPath := flags.String("results", defaultAcceptanceResultsPath, "JSON Lines file that receives the baseline result")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "baseline: does not accept positional arguments")
			return 2
		}
		_, runErr := runBranchBaseline(ctx, root, executeCommand, os.Stdout)
		return finishAcceptance(root, *resultsPath, "runBranchBaseline", "go run ./tools/verify baseline", "baseline", runErr)
	case "package-tests":
		flags := flag.NewFlagSet("package-tests", flag.ContinueOnError)
		resultsPath := flags.String("results", defaultAcceptanceResultsPath, "JSON Lines file that receives the package test result")
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if flags.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "package-tests: does not accept positional arguments")
			return 2
		}
		runErr := runVerifyPackageTests(ctx, root, executeCommand)
		return finishAcceptance(root, *resultsPath, "runVerifyPackageTests", "go run ./tools/verify package-tests", "package-tests", runErr)
	default:
		fmt.Fprintf(os.Stderr, "verify: unknown subcommand %q\n", args[0])
		printUsage()
		return 2
	}
}

func printCommandError(name string, err error) int {
	fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
	var failure commandFailure
	if errors.As(err, &failure) && failure.Code > 0 {
		return failure.Code
	}
	return 1
}
func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: go run ./tools/verify <traceability|architecture|markdown-links|scenarios|branch-instrument|coverage|local|ci|acceptance|mutation|baseline|package-tests>")
	fmt.Fprintln(os.Stderr, "  acceptance [--run REGEXP] runs the Go tests marked Acceptance: <ENV> and requires every selected test to pass")
	fmt.Fprintln(os.Stderr, "  ci requires --base <git-ref>; coverage requires --profile <path>; mutation runs all five packages")
	fmt.Fprintln(os.Stderr, "  scenarios [--go-test-json FILE]... [--results FILE]... [--require-executed] [--output DIR] writes report.md and report.json")
	fmt.Fprintln(os.Stderr, "  baseline runs local three times and derives the branch baseline; package-tests runs go test ./tools/verify; both accept --results FILE")
}

type stringListFlag []string

func (values *stringListFlag) String() string {
	return strings.Join(*values, ",")
}

func (values *stringListFlag) Set(value string) error {
	if value == "" {
		return errors.New("value must not be empty")
	}
	*values = append(*values, filepath.Clean(value))
	return nil
}
