package diskspace

import (
	"path/filepath"
	"testing"
)

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.5, §11.28.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-203
func TestFreeBytes(t *testing.T) {
	path := t.TempDir()
	available, err := FreeBytes(path)
	if err != nil || available == 0 {
		t.Fatalf("FreeBytes(%q)=%d,%v", path, available, err)
	}
	t.Logf("available filesystem bytes: %d", available)
	if _, err := FreeBytes(filepath.Join(path, "does-not-exist")); err == nil {
		t.Fatal("nonexistent path succeeded")
	}
	if _, err := FreeBytes(path + "\x00"); err == nil {
		t.Fatal("invalid path succeeded")
	}
}
