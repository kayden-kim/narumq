package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := runCLI(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "bp05: %v\n", err)
		os.Exit(1)
	}
}

func runCLI(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("subcommand is required: compare or run")
	}
	switch args[0] {
	case "compare":
		return compareCommand(context.Background(), args[1:], stdout, stderr)
	case "run":
		return runCommand(args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown subcommand %q; only compare and run are supported", args[0])
	}
}

func runCommand(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	profilePath := flags.String("profile", "", "path to the BP-05 profile JSON")
	target := flags.String("target", "", "broker target: narumq or emqx")
	address := flags.String("address", "127.0.0.1:1883", "MQTT TCP address")
	outputDir := flags.String("output", "", "target artifact directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected run arguments: %v", flags.Args())
	}
	if *profilePath == "" || *target == "" || *outputDir == "" {
		return errors.New("run requires --profile, --target, and --output")
	}
	profile, _, _, err := LoadProfile(*profilePath)
	if err != nil {
		return err
	}
	summary, runErr := Run(context.Background(), RunOptions{Profile: profile, Target: *target, Address: *address, OutputDir: *outputDir})
	if err := json.NewEncoder(stdout).Encode(summary); err != nil {
		return fmt.Errorf("write run result: %w", err)
	}
	return runErr
}
