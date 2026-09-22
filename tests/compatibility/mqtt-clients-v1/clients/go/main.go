package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	var err error
	if filepath.Base(os.Args[0]) == "proxy" {
		err = runProxyCLI(os.Args[1:])
	} else {
		err = runClientCLI(os.Args[1:])
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "go-compatibility-client: %v\n", err)
		os.Exit(1)
	}
}
func runClientCLI(args []string) error {
	flags := flag.NewFlagSet("go-compatibility-client", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	caseID := flags.String("case", "", "compatibility case identifier")
	role := flags.String("role", "", "client, publisher or subscriber")
	address := flags.String("address", "", "MQTT broker address")
	profilePath := flags.String("profile", "", "strict compatibility profile path")
	output := flags.String("output", "", "existing empty role output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *caseID == "" || *role == "" || *address == "" || *profilePath == "" || *output == "" {
		return fmt.Errorf("--case, --role, --address, --profile and --output are required")
	}
	if _, _, err := splitAddress(*address); err != nil {
		return err
	}
	return runDriver(driverOptions{caseID: *caseID, role: *role, address: *address, profilePath: *profilePath, output: *output}, func(ctx context.Context, item profileCase, expected profileRole, f fixture, p profile, address string) (map[string]any, error) {
		if item.Protocol == "mqtt311" || item.Protocol == "mqtt31" {
			return runMQTT311(ctx, item, expected, f, p, address, *output)
		}
		if item.Protocol == "mqtt5" {
			return runMQTT5(ctx, item, expected, f, p, *output, address)
		}
		return nil, fmt.Errorf("unsupported protocol %q", item.Protocol)
	})
}
