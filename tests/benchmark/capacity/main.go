// Command capacity is the BP-02 capacity measurement harness (docs/specs/performance-and-verification.md §3.9).
//
//	capacity run          target-agnostic runner driven through control markers
//	capacity local        Docker controller that builds, runs and kills a local 3-node cluster
//	capacity serve-jwks   serves a JWKS file for the broker JWT verifier
//	capacity security-config  writes the broker security.json for a profile
//	capacity fixture      writes EKS runner fixtures (JWKS, tokens, CA, server certificate)
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

const usage = "usage: capacity <run|local|serve-jwks|security-config|fixture> [flags]"

func main() {
	if err := dispatch(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "capacity:", err)
		os.Exit(1)
	}
}

func dispatch(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", usage)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "run":
		return runCommand(ctx, args[1:])
	case "local":
		return localCommand(ctx, args[1:])
	case "serve-jwks":
		return serveJWKSCommand(ctx, args[1:])
	case "security-config":
		return securityConfigCommand(args[1:])
	case "fixture":
		return fixtureCommand(args[1:])
	default:
		return fmt.Errorf("unknown command %q; %s", args[0], usage)
	}
}
