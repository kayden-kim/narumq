//go:build !narumq_failpoint

package main

func loadClusterFailpoint() (clusterFailpoint, error) {
	return clusterFailpointNone, nil
}

func loadClusterFaultControlPath() string {
	return ""
}

func triggerClusterFailpoint(point clusterFailpoint) {
}

func writeClusterFaultMarker(message string) {}
