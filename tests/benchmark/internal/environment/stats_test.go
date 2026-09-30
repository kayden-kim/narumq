package environment

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadStatsParsesDecimalAndBinaryUnits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.jsonl")
	data := `{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"12.5%","MemUsage":"1.5MiB / 2GB","NetIO":"1.5kB / 2MB","BlockIO":"0B / 0B"}}` + "\n"
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	samples, err := ReadStats(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 1 || samples[0].CPUPercent != 12.5 || samples[0].MemoryUsageBytes != 1572864 || samples[0].MemoryLimitBytes != 2000000000 || samples[0].NetworkRxBytes != 1500 || samples[0].NetworkTxBytes != 2000000 || samples[0].BlockReadBytes != 0 || samples[0].BlockWriteBytes != 0 {
		t.Fatalf("ReadStats() = %#v", samples)
	}
	if _, ok := samples[0].Docker["MemUsage"]; !ok {
		t.Fatal("ReadStats() did not preserve raw Docker map")
	}
}

func TestReadStatsRejectsUnknownNegativeAndMalformedUnits(t *testing.T) {
	cases := []string{
		`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"1%","MemUsage":"1XB / 2GiB","NetIO":"0B / 0B","BlockIO":"0B / 0B"}}`,
		`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"1%","MemUsage":"-1B / 2GiB","NetIO":"0B / 0B","BlockIO":"0B / 0B"}}`,
		`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"NaN%","MemUsage":"1B / 2GiB","NetIO":"0B / 0B","BlockIO":"0B / 0B"}}`,
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
