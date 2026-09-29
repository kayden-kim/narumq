package membership

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

func releaseProfileMachine(t *testing.T, version string, features []string) *StateMachine {
	t.Helper()
	members := testVoters()
	for index := range members {
		members[index].ReleaseVersion = version
		members[index].Features = slices.Clone(features)
	}
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatalf("initialize %s bootstrap: %v", version, err)
	}
	return machine
}

func releaseProfileOperation(index uint64) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", index)
}

func applyMemberProfileForTest(t *testing.T, machine *StateMachine, index, memberID uint64, version string, features []string) ApplyResultDTO {
	t.Helper()
	result, err := machine.Apply(index, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(index),
		MemberProfile: &MemberProfileChangeDTO{MemberID: memberID, ReleaseVersion: version, Features: features},
	})
	if err != nil {
		t.Fatalf("apply profile for member %d: %v", memberID, err)
	}
	return result
}

func activateFeatureForTest(t *testing.T, machine *StateMachine, index uint64, feature string) ApplyResultDTO {
	t.Helper()
	result, err := machine.Apply(index, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(index),
		FeatureActivation: &FeatureActivationDTO{Feature: feature},
	})
	if err != nil {
		t.Fatalf("activate feature %q: %v", feature, err)
	}
	return result
}

func memberByIDForTest(t *testing.T, machine *StateMachine, memberID uint64) MemberIdentityDTO {
	t.Helper()
	for _, member := range machine.Members() {
		if member.MemberID == memberID {
			return member
		}
	}
	t.Fatalf("member %d not found", memberID)
	return MemberIdentityDTO{}
}

func TestMemberProfileChangeCompatibilityAndActiveFeatures(t *testing.T) {
	machine := releaseProfileMachine(t, "1.1.0", nil)
	features := []string{"compatibility-probe"}
	if result := applyMemberProfileForTest(t, machine, 2, 1, "1.2.0", features); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("adjacent release profile result = %+v", result)
	}
	features[0] = "caller-mutation"
	if got := memberByIDForTest(t, machine, 1).Features; !slices.Equal(got, []string{"compatibility-probe"}) {
		t.Fatalf("stored features share input memory: %q", got)
	}
	if result := applyMemberProfileForTest(t, machine, 3, 1, "1.2.0", []string{"compatibility-probe"}); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("same profile no-op result = %+v", result)
	}
	if result := applyMemberProfileForTest(t, machine, 4, 1, "1.3.0", []string{"compatibility-probe"}); result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictCompatibilityUnsupported {
		t.Fatalf("N-2 profile result = %+v, want compatibility conflict", result)
	}
	if result := applyMemberProfileForTest(t, machine, 5, 99, "1.2.0", nil); result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictMemberNotFound {
		t.Fatalf("unknown member profile result = %+v", result)
	}

	active := releaseProfileMachine(t, "1.2.0", []string{"compatibility-probe"})
	if result := activateFeatureForTest(t, active, 2, "compatibility-probe"); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("initial activation result = %+v", result)
	}
	if result := applyMemberProfileForTest(t, active, 3, 2, "1.2.0", nil); result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictActiveFeatureMissing {
		t.Fatalf("profile dropping active feature result = %+v", result)
	}
	if got := memberByIDForTest(t, active, 2).Features; !slices.Equal(got, []string{"compatibility-probe"}) {
		t.Fatalf("rejected profile changed committed features: %q", got)
	}
}

func TestFeatureActivationRequiresAllNonRetiredMembers(t *testing.T) {
	stagedMachine := releaseProfileMachine(t, "1.2.0", []string{"compatibility-probe"})
	stage := stageRecord(testOperation4, "join-4", 4)
	stage.Member.ReleaseVersion = "1.2.0"
	if result := applyMustSucceed(t, stagedMachine, 2, testCommand(testOperation4, &stage)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("stage profile without feature result = %+v", result)
	}
	if result := activateFeatureForTest(t, stagedMachine, 3, "compatibility-probe"); result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictFeatureUnsupported {
		t.Fatalf("activation with unsupported staged member = %+v", result)
	}

	machine := releaseProfileMachine(t, "1.2.0", []string{"alpha", "compatibility-probe", "zeta"})
	snapshot, err := machine.Snapshot(1)
	if err != nil {
		t.Fatal(err)
	}
	retired := testMember(4, MemberPhaseRetired, "")
	retired.ReleaseVersion = "1.0.0"
	snapshot.Members = append(snapshot.Members, retired)
	snapshot.ObjectCount++
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore retired member with old profile: %v", err)
	}
	for index, feature := range []string{"zeta", "alpha", "compatibility-probe"} {
		if result := activateFeatureForTest(t, restored, uint64(index+2), feature); result.Outcome != ApplyOutcomeApplied {
			t.Fatalf("activation of %q with retired unsupported member = %+v", feature, result)
		}
	}
	if result := activateFeatureForTest(t, restored, 5, "zeta"); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("repeated activation result = %+v", result)
	}
	activeFeatures := restored.ActiveFeatures()
	if !slices.Equal(activeFeatures, []string{"alpha", "compatibility-probe", "zeta"}) {
		t.Fatalf("active features = %q, want canonical order", activeFeatures)
	}
	activeFeatures[0] = "caller-mutation"
	if got := restored.ActiveFeatures(); !slices.Equal(got, []string{"alpha", "compatibility-probe", "zeta"}) {
		t.Fatalf("active feature getter shares state memory: %q", got)
	}
}

func TestStageJoinUsesAdjacentReleaseWindowAndIgnoresRetiredMembers(t *testing.T) {
	machine := releaseProfileMachine(t, "1.1.0", nil)
	snapshot, err := machine.Snapshot(1)
	if err != nil {
		t.Fatal(err)
	}
	retired := testMember(4, MemberPhaseRetired, "")
	retired.ReleaseVersion = "1.0.0"
	snapshot.Members = append(snapshot.Members, retired)
	snapshot.ObjectCount++
	machine = NewStateMachine()
	if err := machine.Restore(snapshot); err != nil {
		t.Fatalf("restore retired old release: %v", err)
	}

	candidate := testMember(5, MemberPhaseStaged, testOperation4)
	candidate.ReleaseVersion = "1.2.0"
	stage := MembershipRecordDTO{
		OperationID: testOperation4, Action: MembershipActionStageJoin,
		IdempotencyKey: "join-5", Member: &candidate,
	}
	if result := applyMustSucceed(t, machine, 2, testCommand(testOperation4, &stage)); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("adjacent candidate with retired N-2 member result = %+v", result)
	}

	incompatible := releaseProfileMachine(t, "1.1.0", nil)
	candidate.MemberID = 4
	candidate.Address = "node4.example:19004"
	candidate.DNSSAN = "node4.example"
	candidate.ReleaseVersion = "1.3.0"
	stage.Member = &candidate
	stage.IdempotencyKey = "join-4"
	result, err := incompatible.Apply(2, testCommand(testOperation4, &stage))
	if err != nil {
		t.Fatalf("Apply(2) returned error: %v", err)
	}
	if result.Outcome != ApplyOutcomeConflict || result.Conflict != ConflictCompatibilityUnsupported {
		t.Fatalf("N-2 candidate result = %+v, want compatibility conflict", result)
	}
}

func TestBeginRemoveAcceptsStaleMutableProfileFields(t *testing.T) {
	machine := releaseProfileMachine(t, "1.2.3", nil)
	if result := applyMemberProfileForTest(t, machine, 2, 3, "1.1.3", []string{"compatibility-probe"}); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("profile update before removal = %+v", result)
	}
	staleRequest := testMember(3, MemberPhaseLeaving, testOperation5)
	staleRequest.ReleaseVersion = "1.2.3"
	record := MembershipRecordDTO{
		OperationID: testOperation5, Action: MembershipActionBeginRemove,
		IdempotencyKey: "remove-3", Member: &staleRequest, TargetMemberID: 3,
	}
	result := applyMustSucceed(t, machine, 3, testCommand(testOperation5, &record))
	if result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("removal with stale release/features result = %+v", result)
	}
	member := memberByIDForTest(t, machine, 3)
	if member.Phase != MemberPhaseLeaving || member.ReleaseVersion != "1.1.3" || !slices.Equal(member.Features, []string{"compatibility-probe"}) {
		t.Fatalf("removal did not preserve committed profile: %+v", member)
	}
}

func TestReleaseProfileSnapshotRoundTripAndWindowValidation(t *testing.T) {
	machine := releaseProfileMachine(t, "1.2.0", []string{"compatibility-probe"})
	if result := activateFeatureForTest(t, machine, 2, "compatibility-probe"); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("activate before snapshot = %+v", result)
	}
	snapshot, err := machine.Snapshot(2)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ObjectCount != 4 || !slices.Equal(snapshot.ActiveFeatures, []string{"compatibility-probe"}) {
		t.Fatalf("snapshot profile state = count %d features %q", snapshot.ObjectCount, snapshot.ActiveFeatures)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore release profile snapshot: %v", err)
	}
	restoredSnapshot, err := restored.Snapshot(2)
	if err != nil {
		t.Fatal(err)
	}
	if restoredSnapshot.ObjectCount != snapshot.ObjectCount || !slices.Equal(restoredSnapshot.ActiveFeatures, snapshot.ActiveFeatures) || !slices.Equal(restored.Members()[0].Features, snapshot.Members[0].Features) {
		t.Fatalf("restored release profile snapshot differs: %+v", restoredSnapshot)
	}

	invalidWindow := cloneSnapshot(snapshot)
	invalidWindow.Members[0].ReleaseVersion = "1.0.0"
	invalidWindow.Members[1].ReleaseVersion = "1.1.0"
	invalidWindow.Members[2].ReleaseVersion = "1.2.0"
	if err := NewStateMachine().Restore(invalidWindow); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("N-2 snapshot error = %v, want ErrInvalidSnapshot", err)
	}
}

func TestReleaseProfileCommandValidation(t *testing.T) {
	commands := []CommandDTO{
		{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(101), MemberProfile: &MemberProfileChangeDTO{ReleaseVersion: "1.2.0"}},
		{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(102), MemberProfile: &MemberProfileChangeDTO{MemberID: 1, ReleaseVersion: "v1.2.0"}},
		{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(103), MemberProfile: &MemberProfileChangeDTO{MemberID: 1, ReleaseVersion: "1.2.0", Features: []string{"zeta", "alpha"}}},
		{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(104), FeatureActivation: &FeatureActivationDTO{Feature: "Bad"}},
	}
	for index, command := range commands {
		if err := validateCommand(command); !errors.Is(err, ErrInvalidCommand) {
			t.Errorf("invalid release profile command %d error = %v, want ErrInvalidCommand", index, err)
		}
	}
}
