package membership

import (
	"slices"
	"strings"
	"testing"
)

// Traceability: CLU-14; ADR-0062; docs/contracts/cluster-release-compatibility-v1.md.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-259
func TestSnapshotStreamFeatureIsAdvertisedAndActivatedLikeOtherFeatures(t *testing.T) {
	if SnapshotStreamFeature != "snapshot-stream" {
		t.Fatalf("SnapshotStreamFeature = %q, want snapshot-stream", SnapshotStreamFeature)
	}
	features, err := BuildFeatures("")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(features, SnapshotStreamFeature) {
		t.Fatalf("build features %v lack %q", features, SnapshotStreamFeature)
	}
	if _, err := ParseBuildFeatureLabel(strings.Join(features, ",")); err != nil {
		t.Fatalf("canonical label with the new token rejected: %v", err)
	}

	machine := releaseProfileMachine(t, "1.0.0", []string{SnapshotStreamFeature})
	if slices.Contains(machine.ActiveFeatures(), SnapshotStreamFeature) {
		t.Fatal("snapshot-stream was active before activation")
	}
	if result := activateFeatureForTest(t, machine, 2, SnapshotStreamFeature); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("activation outcome = %+v", result)
	}
	if !slices.Contains(machine.ActiveFeatures(), SnapshotStreamFeature) {
		t.Fatal("snapshot-stream is not active after activation")
	}

	// Activation needs every non-retired member to advertise the token.
	partial := releaseProfileMachine(t, "1.0.0", []string{SnapshotStreamFeature})
	applyMemberProfileForTest(t, partial, 2, 3, "1.0.0", nil)
	if result := activateFeatureForTest(t, partial, 3, SnapshotStreamFeature); result.Outcome == ApplyOutcomeApplied {
		t.Fatalf("activation succeeded although member 3 does not advertise the token: %+v", result)
	}
}
