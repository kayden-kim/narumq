package integration

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const clusterHistoricalReleaseRevision = "58a95f801e1467379db2eb6f3454ae44e954418a"

// Traceability: CLU-11/NFR-15; docs/specs/cluster-session-and-delivery.md §8, §11.21.
func TestDynamicClusterHistoricalReleaseCompatibilityProcessAcceptance(t *testing.T) {
	if os.Getenv("NARUMQ_RELEASE_COMPAT") != "1" {
		t.Skip("set NARUMQ_RELEASE_COMPAT=1 to run historical-source release compatibility acceptance")
	}
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	fixture.omitPublishBackpressure = true
	source := clusterHistoricalReleaseSource(t, fixture)
	binN2 := fixture.buildBinaryFrom(t, source, "historical-1.0.0", "-X main.buildReleaseVersion=1.0.0")
	binN1 := fixture.buildBinaryFrom(t, source, "historical-1.1.0", "-X main.buildReleaseVersion=1.1.0")
	binN := fixture.buildBinary(t, "head-1.2.0", "-X main.buildReleaseVersion=1.2.0 -X main.buildFeatures=compatibility-probe")
	runClusterReleaseCompatibilityAcceptance(t, fixture, binN2, binN1, binN)
}

// Traceability: CLU-11/CLU-14; docs/specs/cluster-session-and-delivery.md §10, §11.21.
func TestClusterBackupRestoreHistoricalReleaseProcessAcceptance(t *testing.T) {
	if os.Getenv("NARUMQ_RELEASE_COMPAT") != "1" {
		t.Skip("set NARUMQ_RELEASE_COMPAT=1 to run historical-source backup archive acceptance")
	}
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	fixture.omitPublishBackpressure = true
	source := clusterHistoricalReleaseSource(t, fixture)
	binN1 := fixture.buildBinaryFrom(t, source, "historical-restore-1.1.0", "-X main.buildReleaseVersion=1.1.0")
	binN := fixture.buildBinary(t, "head-restore-1.2.0", "-X main.buildReleaseVersion=1.2.0 -X main.buildFeatures=compatibility-probe")
	ctx := fixture.ctx
	configs := []string{fixture.bootstrapConfig(1), fixture.bootstrapConfig(2), fixture.bootstrapConfig(3)}
	processes := clusterBackupReleaseUpgradeFromN1(t, fixture, configs, binN1, binN)
	archiveBPath := clusterBackupReleaseCaptureArchiveB(t, fixture, processes)
	for _, process := range processes {
		if err := process.stop(5 * time.Second); err != nil {
			t.Fatal(err)
		}
	}
	rejected := clusterBackupReleaseRestoreConfigs(t, fixture, configs, "historical-downgrade")
	output, err := clusterBackupReleaseRestore(ctx, binN1, fixture.repoRoot, rejected[0], archiveBPath)
	if err == nil {
		t.Fatal("historical N-1 restore of HEAD archive succeeded")
	}
	if !strings.Contains(string(output), "backup release or schema does not match cluster config") {
		t.Fatalf("historical N-1 restore error=%v output=%s, want exact-release rejection", err, output)
	}
	target := filepath.Join(fixture.tempDir, "historical-downgrade-node-1-data")
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("historical N-1 restore target stat = %v, want not-exist", err)
	}
	t.Logf("HEAD -> historical N-1: non-zero exit and target directory absent; %s", output)
}

func clusterHistoricalReleaseSource(t *testing.T, fixture *clusterMembershipProcessFixture) string {
	t.Helper()
	source := filepath.Join(fixture.tempDir, "source-"+clusterHistoricalReleaseRevision[:12])
	clusterMembershipProcessExtractRevision(t, fixture.repoRoot, clusterHistoricalReleaseRevision, source)
	t.Logf("historical N-1 source %s", clusterHistoricalReleaseRevision)
	return source
}

func clusterMembershipProcessExtractRevision(t *testing.T, repoRoot, revision, destination string) {
	t.Helper()
	resolveCtx, resolveCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer resolveCancel()
	resolved, err := exec.CommandContext(resolveCtx, "git", "-C", repoRoot, "rev-parse", "--verify", revision+"^{commit}").CombinedOutput()
	if err != nil {
		t.Fatalf("resolve historical revision %s (full git history required): %v\n%s", revision, err, resolved)
	}
	if got := strings.TrimSpace(string(resolved)); got != revision {
		t.Fatalf("resolved historical revision = %q, want %s", got, revision)
	}

	archiveCtx, archiveCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer archiveCancel()
	command := exec.CommandContext(archiveCtx, "git", "-C", repoRoot, "archive", "--format=tar", revision)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("open historical archive output: %v", err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("start historical archive: %v", err)
	}
	waited := false
	defer func() {
		if !waited {
			archiveCancel()
			_ = command.Wait()
		}
	}()
	reader := tar.NewReader(stdout)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read historical archive: %v", err)
		}
		if header.Typeflag == tar.TypeXGlobalHeader {
			continue
		}
		name := filepath.FromSlash(header.Name)
		if !filepath.IsLocal(name) {
			t.Fatalf("non-local archive entry %s", header.Name)
		}
		path := filepath.Join(destination, name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatalf("create historical directory %s: %v", path, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("create historical parent directory %s: %v", path, err)
			}
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, header.FileInfo().Mode().Perm()|0o600)
			if err != nil {
				t.Fatalf("create historical file %s: %v", path, err)
			}
			_, copyErr := io.Copy(file, reader)
			closeErr := file.Close()
			if copyErr != nil {
				t.Fatalf("extract historical file %s: %v", path, copyErr)
			}
			if closeErr != nil {
				t.Fatalf("close historical file %s: %v", path, closeErr)
			}
		default:
			t.Fatalf("unsupported archive entry %s type %c", header.Name, header.Typeflag)
		}
	}
	err = command.Wait()
	waited = true
	if err != nil {
		t.Fatalf("extract historical revision %s: %v\n%s", revision, err, stderr.String())
	}
}
