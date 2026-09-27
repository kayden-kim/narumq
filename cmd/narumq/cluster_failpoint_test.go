package main

import (
	"testing"

	raftadapter "narumq/internal/adapters/raft"
)

func TestClusterFailpointParse(t *testing.T) {
	cases := []struct {
		input   string
		want    clusterFailpoint
		wantErr bool
	}{
		{"", clusterFailpointNone, false},
		{"add-learner-before-persist", failpointAddLearnerBeforePersist, false},
		{"add-learner-after-persist", failpointAddLearnerAfterPersist, false},
		{"add-learner-after-apply", failpointAddLearnerAfterApply, false},
		{"promote-voter-before-persist", failpointPromoteVoterBeforePersist, false},
		{"promote-voter-after-persist", failpointPromoteVoterAfterPersist, false},
		{"promote-voter-after-apply", failpointPromoteVoterAfterApply, false},
		{"remove-member-before-persist", failpointRemoveMemberBeforePersist, false},
		{"remove-member-after-persist", failpointRemoveMemberAfterPersist, false},
		{"remove-member-after-apply", failpointRemoveMemberAfterApply, false},
		{"snapshot-install-before-persist", failpointSnapshotInstallBefore, false},
		{"snapshot-install-after-persist", failpointSnapshotInstallAfter, false},
		{"snapshot-create-before-finalize", failpointSnapshotCreateBeforeFinalize, false},
		{"snapshot-create-after-finalize", failpointSnapshotCreateAfterFinalize, false},
		{"begin-remove-after-apply", failpointBeginRemoveAfterApply, false},
		{"route-projection-rebuild-error", failpointRouteProjectionRebuild, false},
		{"ownership-acquire-after-commit", failpointOwnershipAcquireAfterCommit, false},
		{"bogus", "", true},
		{"add-learner", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseClusterFailpoint(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseClusterFailpoint(%q) err = %v, wantErr = %v", tc.input, err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("parseClusterFailpoint(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestClusterFailpointMatchesOnlyItsBoundary(t *testing.T) {
	stages := []raftadapter.PersistenceStage{
		raftadapter.PersistenceBeforeReady,
		raftadapter.PersistenceAfterReady,
	}
	actions := []raftadapter.MembershipChangeAction{
		raftadapter.MembershipAddLearner,
		raftadapter.MembershipPromoteVoter,
		raftadapter.MembershipRemove,
	}

	for _, pt := range validClusterFailpoints {
		t.Run(string(pt), func(t *testing.T) {
			// Test persistence boundaries
			for _, stage := range stages {
				for _, action := range actions {
					boundary := raftadapter.PersistenceBoundaryDTO{
						Stage:                stage,
						ConfigurationActions: []raftadapter.MembershipChangeAction{action},
					}
					matched := pt.matchesPersistence(boundary)
					wantMatch := (pt == failpointAddLearnerBeforePersist && stage == raftadapter.PersistenceBeforeReady && action == raftadapter.MembershipAddLearner) ||
						(pt == failpointAddLearnerAfterPersist && stage == raftadapter.PersistenceAfterReady && action == raftadapter.MembershipAddLearner) ||
						(pt == failpointPromoteVoterBeforePersist && stage == raftadapter.PersistenceBeforeReady && action == raftadapter.MembershipPromoteVoter) ||
						(pt == failpointPromoteVoterAfterPersist && stage == raftadapter.PersistenceAfterReady && action == raftadapter.MembershipPromoteVoter) ||
						(pt == failpointRemoveMemberBeforePersist && stage == raftadapter.PersistenceBeforeReady && action == raftadapter.MembershipRemove) ||
						(pt == failpointRemoveMemberAfterPersist && stage == raftadapter.PersistenceAfterReady && action == raftadapter.MembershipRemove)
					if matched != wantMatch {
						t.Errorf("matchesPersistence(%v, action %v) = %t, want %t", stage, action, matched, wantMatch)
					}
				}
				snapBoundary := raftadapter.PersistenceBoundaryDTO{
					Stage:            stage,
					InstallsSnapshot: true,
				}
				snapMatched := pt.matchesPersistence(snapBoundary)
				wantSnapMatch := (pt == failpointSnapshotInstallBefore && stage == raftadapter.PersistenceBeforeReady) ||
					(pt == failpointSnapshotInstallAfter && stage == raftadapter.PersistenceAfterReady)
				if snapMatched != wantSnapMatch {
					t.Errorf("matchesPersistence(%v, snapshot) = %t, want %t", stage, snapMatched, wantSnapMatch)
				}
			}
			for _, stage := range []raftadapter.PersistenceStage{
				raftadapter.PersistenceBeforeSnapshotFinalize,
				raftadapter.PersistenceAfterSnapshotFinalize,
			} {
				boundary := raftadapter.PersistenceBoundaryDTO{Stage: stage, CreatesSnapshot: true}
				matched := pt.matchesPersistence(boundary)
				wantMatch := (pt == failpointSnapshotCreateBeforeFinalize && stage == raftadapter.PersistenceBeforeSnapshotFinalize) ||
					(pt == failpointSnapshotCreateAfterFinalize && stage == raftadapter.PersistenceAfterSnapshotFinalize)
				if matched != wantMatch {
					t.Errorf("matchesPersistence(%v, local snapshot) = %t, want %t", stage, matched, wantMatch)
				}
			}

			// Test configuration apply
			for _, action := range actions {
				matched := pt.matchesConfigurationApply(action)
				wantMatch := (pt == failpointAddLearnerAfterApply && action == raftadapter.MembershipAddLearner) ||
					(pt == failpointPromoteVoterAfterApply && action == raftadapter.MembershipPromoteVoter) ||
					(pt == failpointRemoveMemberAfterApply && action == raftadapter.MembershipRemove)
				if matched != wantMatch {
					t.Errorf("matchesConfigurationApply(%v) = %t, want %t", action, matched, wantMatch)
				}
			}

			// Test begin-remove apply
			if got := pt.matchesBeginRemoveApply(); got != (pt == failpointBeginRemoveAfterApply) {
				t.Errorf("matchesBeginRemoveApply() = %t, want %t", got, pt == failpointBeginRemoveAfterApply)
			}

			// Test route projection rebuild error
			if got := pt.failsRouteProjection(); got != (pt == failpointRouteProjectionRebuild) {
				t.Errorf("failsRouteProjection() = %t, want %t", got, pt == failpointRouteProjectionRebuild)
			}
			if got := pt.matchesOwnershipAcquireAfterCommit(); got != (pt == failpointOwnershipAcquireAfterCommit) {
				t.Errorf("matchesOwnershipAcquireAfterCommit() = %t, want %t", got, pt == failpointOwnershipAcquireAfterCommit)
			}
		})
	}
}
