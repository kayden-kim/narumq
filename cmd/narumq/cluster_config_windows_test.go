package main

import "testing"

// Scenarios: management-api-and-observability.md:MGT-SCN-014
func TestClusterPathsOverlapAllowsDifferentWindowsVolumes(t *testing.T) {
	overlap, err := clusterPathsOverlap(`C:\narumq\data`, `D:\narumq\audit`)
	if err != nil {
		t.Fatalf("clusterPathsOverlap() error = %v, want different volumes accepted as disjoint", err)
	}
	if overlap {
		t.Fatal("clusterPathsOverlap() reported different Windows volumes as overlapping")
	}
}
