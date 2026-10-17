package main

import (
	"os"
	"strings"
	"testing"
)

// TestShippedProfilesDecodeAndValidate loads the shipped capacity profiles and checks their decisions.
// Scenarios: performance-and-verification.md:PRF-SCN-049
func TestShippedProfilesDecodeAndValidate(t *testing.T) {
	for _, name := range []string{"testdata/local-smoke.json", "testdata/eks-capacity-v1.json"} {
		p, data, sha, err := LoadProfile(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(data) == 0 || len(sha) != 64 {
			t.Fatalf("%s: source bytes %d, sha %q", name, len(data), sha)
		}
		if p.MaxCells() < 1 {
			t.Fatalf("%s: no ladder cells", name)
		}
	}
	eks, _, _, _ := LoadProfile("testdata/eks-capacity-v1.json")
	if eks.Purpose != purposeEKSCapacity || eks.BisectRounds != 2 || eks.ConfirmRuns != 5 || eks.LatencyP99LimitMillis != 100 {
		t.Fatalf("eks profile does not carry the §3.9 decisions: %+v", eks)
	}
}

// TestDecodeProfileRejectsUnknownMissingTrailingAndOutOfBounds rejects malformed and out-of-range profiles.
// Scenarios: performance-and-verification.md:PRF-SCN-049
func TestDecodeProfileRejectsUnknownMissingTrailingAndOutOfBounds(t *testing.T) {
	source, err := os.ReadFile("testdata/local-smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	valid := string(source)
	mutations := map[string]string{
		"unknown field":     strings.Replace(valid, `"seed": 2,`, `"seed": 2, "surprise": 1,`, 1),
		"missing field":     strings.Replace(valid, `"bisectRounds": 0,`, ``, 1),
		"trailing document": valid + `{}`,
		"bad purpose":       strings.Replace(valid, `"local-smoke"`, `"smoke"`, 1),
		"bisect too large":  strings.Replace(valid, `"bisectRounds": 0`, `"bisectRounds": 5`, 1),
		"confirm zero":      strings.Replace(valid, `"confirmRuns": 1`, `"confirmRuns": 0`, 1),
		"measure too short": strings.Replace(valid, `"measureMillis": 5000`, `"measureMillis": 1999`, 1),
		"tiny payload":      strings.Replace(valid, `[128, 128, 128, 128, 128, 128, 512, 512, 2048, 16384]`, `[16]`, 1),
		"rate above 50":     strings.Replace(valid, `{ "cells": 2, "ratePerStreamPerSecond": 2 }`, `{ "cells": 2, "ratePerStreamPerSecond": 51 }`, 1),
		"steps not growing": strings.Replace(valid, `{ "cells": 2, "ratePerStreamPerSecond": 2 }`, `{ "cells": 1, "ratePerStreamPerSecond": 2 }`, 1),
		"cpu percent over":  strings.Replace(valid, `"generatorCpuSaturationPercent": 80`, `"generatorCpuSaturationPercent": 101`, 1),
		"step without rate": strings.Replace(valid, `{ "cells": 1, "ratePerStreamPerSecond": 2 }`, `{ "cells": 1 }`, 1),
		"ready below 10s":   strings.Replace(valid, `"readyTimeoutMillis": 120000`, `"readyTimeoutMillis": 9999`, 1),
	}
	for name, doc := range mutations {
		if doc == valid {
			t.Fatalf("%s: mutation did not change the profile", name)
		}
		if _, err := DecodeProfile([]byte(doc)); err == nil {
			t.Errorf("%s: profile was accepted", name)
		}
	}
	if _, err := DecodeProfile(source); err != nil {
		t.Fatalf("unmodified profile rejected: %v", err)
	}
}
