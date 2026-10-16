//go:build !linux

package main

import "runtime"

// processCPUSeconds is unavailable off Linux; generator CPU is then recorded as unknown.
func processCPUSeconds() (float64, bool) { return 0, false }

func processRSSBytes() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Sys
}
