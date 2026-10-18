package main

import (
	"flag"
	"fmt"
	"os"

	"narumq/internal/application/membership"
	"narumq/internal/domain/release"
)

func main() {
	version := flag.String("version", "", "canonical release version embedded in the broker")
	features := flag.String("features", "", "canonical full feature set embedded in the broker")
	flag.Parse()

	if _, err := release.Parse(*version); err != nil {
		fail(fmt.Errorf("invalid release version: %w", err))
	}
	if _, err := membership.ParseBuildFeatureLabel(*features); err != nil {
		fail(fmt.Errorf("invalid release feature label: %w", err))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
