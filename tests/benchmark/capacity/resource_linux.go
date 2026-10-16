//go:build linux

package main

import (
	"os"
	"strconv"
	"strings"
)

const clockTicksPerSecond = 100

// processCPUSeconds returns this process's user+system CPU seconds from /proc/self/stat.
func processCPUSeconds() (float64, bool) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, false
	}
	text := string(data)
	// The command name is parenthesised and may contain spaces; fields restart after the last ')'.
	closing := strings.LastIndexByte(text, ')')
	if closing < 0 {
		return 0, false
	}
	fields := strings.Fields(text[closing+1:])
	// After ')' the fields start at state (field 3); utime and stime are fields 14 and 15.
	if len(fields) < 13 {
		return 0, false
	}
	utime, errUser := strconv.ParseUint(fields[11], 10, 64)
	stime, errSystem := strconv.ParseUint(fields[12], 10, 64)
	if errUser != nil || errSystem != nil {
		return 0, false
	}
	return float64(utime+stime) / clockTicksPerSecond, true
}

// processRSSBytes returns the resident set size from /proc/self/statm.
func processRSSBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
