package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("expected a command: supervise, fill-disk, free-disk, set-io-delay, or probe-io")
	}
	switch args[0] {
	case "supervise":
		return runSupervise(args[1:])
	case "fill-disk":
		return runFillDisk(args[1:])
	case "free-disk":
		return runFreeDisk(args[1:])
	case "set-io-delay":
		return runSetIODelay(args[1:])
	case "probe-io":
		return runProbeIO(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func newFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func parseFlags(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return nil
}

func requireFlag(name, value string) error {
	if value == "" {
		return fmt.Errorf("--%s is required", name)
	}
	return nil
}

func writeJSON(value any) error {
	return json.NewEncoder(os.Stdout).Encode(value)
}
