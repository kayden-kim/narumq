package integration

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §11.4; ADR-0009.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-017
func TestRaftModuleMatchesApprovedVersionAndChecksums(t *testing.T) {
	root := clusterMembershipProcessRepoRoot(t)
	const module = "go.etcd.io/raft/v3"
	const version = "v3.7.0"
	const sum = "h1:BGzlwx07bLv8PW6OU5HObuz1y4hlPZUXA07pM1mPUh4="
	const modSum = "h1:6gX6T2X907DjnjsFLODnTxba77stjs84W9gTTI0GUNA="
	manifest, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	pinned := false
	for _, line := range strings.Split(string(manifest), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == module && fields[1] == version {
			pinned = true
		}
	}
	if !pinned {
		t.Fatalf("go.mod does not pin %s %s", module, version)
	}
	checksums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{module + " " + version + " " + sum, module + " " + version + "/go.mod " + modSum} {
		found := false
		for _, line := range strings.Split(string(checksums), "\n") {
			if strings.TrimSpace(line) == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("go.sum missing approved checksum %q", want)
		}
	}
	decision, err := os.ReadFile(filepath.Join(root, "docs", "decisions", "ADR-0009-raft-production-state-profile.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{module + " " + version, sum, modSum} {
		if !strings.Contains(string(decision), "`"+want+"`") {
			t.Fatalf("ADR-0009 does not contain approved value %q", want)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	verify := exec.CommandContext(ctx, "go", "mod", "verify")
	verify.Dir = root
	output, err := verify.CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "all modules verified" {
		t.Fatalf("go mod verify: err=%v output=%s", err, output)
	}
	download := exec.CommandContext(ctx, "go", "mod", "download", "-json", module+"@"+version)
	download.Dir = root
	output, err = download.CombinedOutput()
	if err != nil {
		t.Fatalf("go mod download: %v\n%s", err, output)
	}
	var result struct {
		Path     string
		Version  string
		Sum      string
		GoModSum string
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode module download: %v\n%s", err, output)
	}
	if result.Path != module || result.Version != version || result.Sum != sum || result.GoModSum != modSum {
		t.Fatalf("downloaded module = %+v, want %s@%s with approved checksums", result, module, version)
	}
}
