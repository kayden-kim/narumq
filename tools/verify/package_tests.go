package main

import (
	"context"
	"fmt"
)

// runVerifyPackageTests runs the whole tools/verify test package, which holds the coverage and
// branch gate tests, through runner. It is invoked by the package-tests subcommand and is not
// called from any test of this package.
// Scenarios: performance-and-verification.md:PRF-SCN-095
func runVerifyPackageTests(ctx context.Context, root string, runner commandRunner) error {
	if err := ensureRepositoryRoot(root); err != nil {
		return err
	}
	command := commandSpec{Name: "go", Args: []string{"test", "./tools/verify", "-count=1"}}
	if err := runner(ctx, root, command); err != nil {
		return fmt.Errorf("tools/verify package tests failed: %w", err)
	}
	return nil
}
