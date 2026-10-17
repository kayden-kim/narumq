// Command release-profile validates the release-artifact deployment-profile descriptor
// (deploy/release/deployment-profiles-v1.json) and filters verification evidence by profile.
//
//	go run ./tools/release-profile validate -descriptor deploy/release/deployment-profiles-v1.json
//	go run ./tools/release-profile filter   -descriptor deploy/release/deployment-profiles-v1.json -evidence evidence.json
//
// validate strictly decodes the descriptor, prints each profile's Supported/Experimental/Unsupported
// status, and cross-checks the EKS_EC2_HA pins against the parsed EKS manifests.
// Run it from the repository root, or pass -manifests.
//
// filter reads a JSON array of {"profile","kind","source"} records (kind: ha, rpo, rollingUpdate)
// and prints which records may support EKS_EC2_HA evidence and which are excluded and why.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

const (
	defaultManifestDir = "deploy/kubernetes/eks-ec2-ha"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: release-profile validate|filter -descriptor <file> [options]")
		return 2
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(stderr)
	descriptorPath := flags.String("descriptor", "", "deployment profile descriptor JSON file")
	manifestDir := flags.String("manifests", defaultManifestDir, "EKS_EC2_HA manifest directory (validate)")
	evidencePath := flags.String("evidence", "", "JSON array of evidence records (filter)")

	switch args[0] {
	case "validate", "filter":
	default:
		fmt.Fprintf(stderr, "unknown command %q (want validate or filter)\n", args[0])
		return 2
	}
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}
	if *descriptorPath == "" {
		fmt.Fprintln(stderr, "-descriptor is required")
		return 2
	}
	descriptor, err := loadDescriptor(*descriptorPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	if args[0] == "validate" {
		if err := CrossCheckManifests(descriptor, *manifestDir); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprint(stdout, descriptor.Display())
		return 0
	}

	if *evidencePath == "" {
		fmt.Fprintln(stderr, "-evidence is required")
		return 2
	}
	records, err := loadEvidence(*evidencePath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	eligible, excluded, err := descriptor.FilterEvidence(records)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	type excludedView struct {
		Evidence
		Reason string `json:"reason"`
	}
	view := struct {
		Eligible []Evidence     `json:"eligible"`
		Excluded []excludedView `json:"excluded"`
	}{Eligible: eligible, Excluded: make([]excludedView, 0, len(excluded))}
	if view.Eligible == nil {
		view.Eligible = []Evidence{}
	}
	for _, item := range excluded {
		view.Excluded = append(view.Excluded, excludedView{Evidence: item.Evidence, Reason: item.Reason})
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(view); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func loadDescriptor(path string) (Descriptor, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Descriptor{}, fmt.Errorf("read descriptor: %w", err)
	}
	return DecodeDescriptor(data)
}

func loadEvidence(path string) ([]Evidence, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read evidence: %w", err)
	}
	var records []Evidence
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&records); err != nil {
		return nil, fmt.Errorf("decode evidence: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode evidence: unexpected data after the JSON document")
	}
	return records, nil
}
