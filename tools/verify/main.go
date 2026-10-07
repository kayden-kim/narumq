package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
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
		if err := flags.Parse(args[1:]); err != nil {
			return 2
		}
		if err := verifyTraceability(root, filepath.Clean(*matrixPath)); err != nil {
			return printCommandError("traceability", err)
		}
		fmt.Println("traceability: PASS")
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
	fmt.Fprintln(os.Stderr, "usage: go run ./tools/verify <traceability|architecture|branch-instrument|coverage|local|ci>")
	fmt.Fprintln(os.Stderr, "  ci requires --base <git-ref>; coverage requires --profile <path>")
}
