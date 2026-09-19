package environment

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadStatsParsesDecimalAndBinaryUnits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.jsonl")
	data := `{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"12.5%","MemUsage":"1.5MiB / 2GB"}}` + "\n"
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	samples, err := ReadStats(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 1 || samples[0].CPUPercent != 12.5 || samples[0].MemoryUsageBytes != 1572864 || samples[0].MemoryLimitBytes != 2000000000 {
		t.Fatalf("ReadStats() = %#v", samples)
	}
	if _, ok := samples[0].Docker["MemUsage"]; !ok {
		t.Fatal("ReadStats() did not preserve raw Docker map")
	}
}

func TestReadStatsRejectsUnknownNegativeAndMalformedUnits(t *testing.T) {
	cases := []string{
		`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"1%","MemUsage":"1TiB / 2GiB"}}`,
		`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"1%","MemUsage":"-1B / 2GiB"}}`,
		`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"NaN%","MemUsage":"1B / 2GiB"}}`,
	}
	for _, line := range cases {
		path := filepath.Join(t.TempDir(), "stats.jsonl")
		if err := os.WriteFile(path, []byte(line+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadStats(path); err == nil || strings.TrimSpace(err.Error()) == "" {
			t.Fatalf("ReadStats() accepted invalid line %q", line)
		}
	}
}

func TestParseByteQuantityRejectsOverflow(t *testing.T) {
	if _, err := parseByteQuantity("999999999999999999999GB"); err == nil {
		t.Fatal("parseByteQuantity() accepted int64 overflow")
	}
}
