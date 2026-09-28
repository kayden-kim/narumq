//go:build narumq_failpoint

package main

import (
	"fmt"
	"os"
)

func loadClusterFailpoint() (clusterFailpoint, error) {
	return parseClusterFailpoint(os.Getenv("NARUMQ_FAILPOINT"))
}

func loadClusterFaultControlPath() string {
	return os.Getenv("NARUMQ_FAILPOINT_CONTROL")
}

func triggerClusterFailpoint(point clusterFailpoint) {
	fmt.Fprintf(os.Stderr, "narumq failpoint reached: %s\n", point)
	os.Exit(clusterFailpointExitCode)
}

func writeClusterFaultMarker(message string) {
	fmt.Fprintln(os.Stderr, message)
}
