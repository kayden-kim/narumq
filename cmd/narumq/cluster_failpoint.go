package main

import (
	"fmt"
	"slices"

	raftadapter "narumq/internal/adapters/raft"
)

type clusterFailpoint string

const clusterFailpointExitCode = 86

const (
	clusterFailpointNone clusterFailpoint = ""

	failpointAddLearnerBeforePersist      clusterFailpoint = "add-learner-before-persist"
	failpointAddLearnerAfterPersist       clusterFailpoint = "add-learner-after-persist"
	failpointAddLearnerAfterApply         clusterFailpoint = "add-learner-after-apply"
	failpointPromoteVoterBeforePersist    clusterFailpoint = "promote-voter-before-persist"
	failpointPromoteVoterAfterPersist     clusterFailpoint = "promote-voter-after-persist"
	failpointPromoteVoterAfterApply       clusterFailpoint = "promote-voter-after-apply"
	failpointRemoveMemberBeforePersist    clusterFailpoint = "remove-member-before-persist"
	failpointRemoveMemberAfterPersist     clusterFailpoint = "remove-member-after-persist"
	failpointRemoveMemberAfterApply       clusterFailpoint = "remove-member-after-apply"
	failpointSnapshotInstallBefore        clusterFailpoint = "snapshot-install-before-persist"
	failpointSnapshotInstallAfter         clusterFailpoint = "snapshot-install-after-persist"
	failpointSnapshotCreateBeforeFinalize clusterFailpoint = "snapshot-create-before-finalize"
	failpointSnapshotCreateAfterFinalize  clusterFailpoint = "snapshot-create-after-finalize"
	failpointBeginRemoveAfterApply        clusterFailpoint = "begin-remove-after-apply"
	failpointRouteProjectionRebuild       clusterFailpoint = "route-projection-rebuild-error"
)

var validClusterFailpoints = []clusterFailpoint{
	failpointAddLearnerBeforePersist,
	failpointAddLearnerAfterPersist,
	failpointAddLearnerAfterApply,
	failpointPromoteVoterBeforePersist,
	failpointPromoteVoterAfterPersist,
	failpointPromoteVoterAfterApply,
	failpointRemoveMemberBeforePersist,
	failpointRemoveMemberAfterPersist,
	failpointRemoveMemberAfterApply,
	failpointSnapshotInstallBefore,
	failpointSnapshotInstallAfter,
	failpointSnapshotCreateBeforeFinalize,
	failpointSnapshotCreateAfterFinalize,
	failpointBeginRemoveAfterApply,
	failpointRouteProjectionRebuild,
}

func parseClusterFailpoint(value string) (clusterFailpoint, error) {
	if value == "" {
		return clusterFailpointNone, nil
	}
	point := clusterFailpoint(value)
	if !slices.Contains(validClusterFailpoints, point) {
		return "", fmt.Errorf("unknown cluster failpoint %q", value)
	}
	return point, nil
}

func (point clusterFailpoint) matchesPersistence(boundary raftadapter.PersistenceBoundaryDTO) bool {
	if boundary.CreatesSnapshot {
		switch boundary.Stage {
		case raftadapter.PersistenceBeforeSnapshotFinalize:
			return point == failpointSnapshotCreateBeforeFinalize
		case raftadapter.PersistenceAfterSnapshotFinalize:
			return point == failpointSnapshotCreateAfterFinalize
		default:
			return false
		}
	}
	if boundary.InstallsSnapshot {
		switch boundary.Stage {
		case raftadapter.PersistenceBeforeReady:
			return point == failpointSnapshotInstallBefore
		case raftadapter.PersistenceAfterReady:
			return point == failpointSnapshotInstallAfter
		}
	}
	for _, action := range boundary.ConfigurationActions {
		switch action {
		case raftadapter.MembershipAddLearner:
			if (boundary.Stage == raftadapter.PersistenceBeforeReady && point == failpointAddLearnerBeforePersist) ||
				(boundary.Stage == raftadapter.PersistenceAfterReady && point == failpointAddLearnerAfterPersist) {
				return true
			}
		case raftadapter.MembershipPromoteVoter:
			if (boundary.Stage == raftadapter.PersistenceBeforeReady && point == failpointPromoteVoterBeforePersist) ||
				(boundary.Stage == raftadapter.PersistenceAfterReady && point == failpointPromoteVoterAfterPersist) {
				return true
			}
		case raftadapter.MembershipRemove:
			if (boundary.Stage == raftadapter.PersistenceBeforeReady && point == failpointRemoveMemberBeforePersist) ||
				(boundary.Stage == raftadapter.PersistenceAfterReady && point == failpointRemoveMemberAfterPersist) {
				return true
			}
		}
	}
	return false
}

func (point clusterFailpoint) matchesConfigurationApply(action raftadapter.MembershipChangeAction) bool {
	switch action {
	case raftadapter.MembershipAddLearner:
		return point == failpointAddLearnerAfterApply
	case raftadapter.MembershipPromoteVoter:
		return point == failpointPromoteVoterAfterApply
	case raftadapter.MembershipRemove:
		return point == failpointRemoveMemberAfterApply
	default:
		return false
	}
}

func (point clusterFailpoint) matchesBeginRemoveApply() bool {
	return point == failpointBeginRemoveAfterApply
}

func (point clusterFailpoint) failsRouteProjection() bool {
	return point == failpointRouteProjectionRebuild
}
