//go:build !narumq_failpoint

package main

func loadClusterFailpoint() (clusterFailpoint, error) {
	return clusterFailpointNone, nil
}

func triggerClusterFailpoint(point clusterFailpoint) {
}
