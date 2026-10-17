package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setControllerEnvironment(t *testing.T, auditDirectory string) {
	t.Helper()
	t.Setenv("POD_NAMESPACE", "narumq")
	t.Setenv("POD_UID", "pod-uid-1")
	t.Setenv("NARUMQ_AUDIT_DIRECTORY", auditDirectory)
	t.Setenv("NARUMQ_ALLOWED_OCI_REGISTRIES", "registry.test")
}

func setBuildVersion(t *testing.T, version string) {
	t.Helper()
	previous := buildVersion
	buildVersion = version
	t.Cleanup(func() { buildVersion = previous })
}

func TestRequiredEnvironmentTrimsValuesAndRejectsBlankOnes(t *testing.T) {
	t.Setenv("NARUMQ_TEST_VALUE", "  narumq  ")
	got, err := requiredEnvironment("NARUMQ_TEST_VALUE")
	if err != nil || got != "narumq" {
		t.Fatalf("requiredEnvironment() = %q, %v; want the trimmed value", got, err)
	}

	for name, value := range map[string]string{"empty": "", "whitespace only": " \t "} {
		t.Setenv("NARUMQ_TEST_VALUE", value)
		got, err := requiredEnvironment("NARUMQ_TEST_VALUE")
		if err == nil || got != "" {
			t.Fatalf("%s: requiredEnvironment() = %q, %v; want an error and no value", name, got, err)
		}
		if !strings.Contains(err.Error(), "NARUMQ_TEST_VALUE") {
			t.Fatalf("%s: error %q does not name the missing variable", name, err)
		}
	}
}

func TestRunRefusesToStartWhenAnyRequiredEnvironmentVariableIsMissing(t *testing.T) {
	for _, missing := range []string{"POD_NAMESPACE", "POD_UID", "NARUMQ_AUDIT_DIRECTORY", "NARUMQ_ALLOWED_OCI_REGISTRIES"} {
		t.Run(missing, func(t *testing.T) {
			auditDirectory := filepath.Join(t.TempDir(), "audit")
			setControllerEnvironment(t, auditDirectory)
			t.Setenv(missing, "  ")
			err := run()
			if err == nil || !strings.Contains(err.Error(), missing) {
				t.Fatalf("run() error = %v, want a refusal naming %s", err, missing)
			}
			if _, statErr := os.Stat(auditDirectory); !os.IsNotExist(statErr) {
				t.Fatalf("run() touched the audit directory before validating its configuration: %v", statErr)
			}
		})
	}
}

func TestRunRefusesToStartWithoutADurableAuditPublisher(t *testing.T) {
	notADirectory := filepath.Join(t.TempDir(), "audit-file")
	if err := os.WriteFile(notADirectory, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	setControllerEnvironment(t, notADirectory)
	setBuildVersion(t, "1.2.3")
	err := run()
	if err == nil || !strings.Contains(err.Error(), "audit publisher is unavailable") {
		t.Fatalf("run() error = %v, want the unavailable durable audit publisher refusal", err)
	}
}

func TestRunRefusesAControllerBuildWithoutAnEmbeddedVersion(t *testing.T) {
	for _, version := range []string{"", "dev"} {
		t.Run("version "+version, func(t *testing.T) {
			auditDirectory := filepath.Join(t.TempDir(), "audit")
			setControllerEnvironment(t, auditDirectory)
			setBuildVersion(t, version)
			err := run()
			if err == nil || !strings.Contains(err.Error(), "build version") {
				t.Fatalf("run() error = %v, want the missing embedded build version refusal", err)
			}
		})
	}
}

func TestRunRejectsBlankRegistryAllowlistEntryBeforeContactingTheCluster(t *testing.T) {
	setControllerEnvironment(t, filepath.Join(t.TempDir(), "audit"))
	t.Setenv("NARUMQ_ALLOWED_OCI_REGISTRIES", "registry.test, ,other.test")
	setBuildVersion(t, "1.2.3")
	err := run()
	if err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("run() error = %v, want the empty allowlist entry refusal", err)
	}
}
