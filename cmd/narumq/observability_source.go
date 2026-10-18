package main

import (
	"context"
	"errors"
	"strconv"

	adaptersmetrics "narumq/internal/adapters/metrics"
	appmgmt "narumq/internal/application/management"
	"narumq/internal/application/membership"
	clockports "narumq/internal/application/ports/clock"
	observationports "narumq/internal/application/ports/observability"
)

// observabilitySource provides request-local data from one process and its committed membership.
type observabilitySource struct {
	runtime        *clusterRuntime
	state          *appmgmt.Service
	metrics        *adaptersmetrics.Registry
	clock          clockports.Clock
	clusterID      string
	localNodeID    string
	releaseVersion string
}

func (source *observabilitySource) ClusterSnapshot(ctx context.Context) (observationports.ClusterSnapshotDTO, error) {
	if ctx == nil {
		return observationports.ClusterSnapshotDTO{}, errors.New("observability context is required")
	}
	if err := ctx.Err(); err != nil {
		return observationports.ClusterSnapshotDTO{}, err
	}
	summary := source.state.GetClusterSummary(ctx)
	if source.runtime == nil {
		return observationports.ClusterSnapshotDTO{
			ClusterID: summary.ClusterID, ReleaseVersion: summary.ReleaseVersion, LocalNodeID: source.localNodeID,
			Members:    []observationports.MemberDTO{{NodeID: source.localNodeID, Phase: observationports.MembershipPhaseStandalone}},
			QuorumSize: summary.QuorumSize, ReplicationFactor: summary.ReplicationFactor,
			WriteQuorum: summary.WriteQuorum, LeaderNodeID: summary.LeaderNodeID,
			UnderReplicated: summary.UnderReplicated, SubscriptionCount: nonnegativeUint64(summary.SubscriptionCount),
		}, nil
	}
	if source.runtime.machine == nil || source.runtime.node == nil {
		return observationports.ClusterSnapshotDTO{}, errors.New("cluster membership is unavailable")
	}
	members := source.runtime.machine.Members()
	clusterMembers := make([]observationports.MemberDTO, 0, len(members))
	for _, member := range members {
		phase, ok := observabilityMembershipPhase(member.Phase)
		if !ok {
			return observationports.ClusterSnapshotDTO{}, errors.New("cluster membership phase is invalid")
		}
		clusterMembers = append(clusterMembers, observationports.MemberDTO{
			NodeID: strconv.FormatUint(member.MemberID, 10), Phase: phase,
		})
	}
	status := source.runtime.node.Status()
	profile := source.runtime.config.replicationProfile()
	var leaderNodeID *string
	if status.LeaderID != 0 {
		leader := strconv.FormatUint(uint64(status.LeaderID), 10)
		leaderNodeID = &leader
	}
	return observationports.ClusterSnapshotDTO{
		ClusterID: source.runtime.config.ClusterID, ReleaseVersion: source.runtime.config.ReleaseVersion,
		LocalNodeID: source.localNodeID, Members: clusterMembers,
		QuorumSize: max(len(status.VoterIDs)/2+1, 1), ReplicationFactor: profile.Factor,
		WriteQuorum: profile.WriteQuorum, LeaderNodeID: leaderNodeID,
		UnderReplicated: len(status.LearnerIDs), SubscriptionCount: nonnegativeUint64(summary.SubscriptionCount),
	}, nil
}

func (source *observabilitySource) FreshQuorumAvailable(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, errors.New("observability context is required")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if source.runtime == nil {
		return source.state.GetClusterSummary(ctx).QuorumAvailable, nil
	}
	if err := source.runtime.checkReadiness(ctx, "summary"); err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, nil
	}
	return true, nil
}

func (source *observabilitySource) CaptureNodeSnapshot(ctx context.Context) (observationports.NodeSnapshotDTO, error) {
	if ctx == nil {
		return observationports.NodeSnapshotDTO{}, errors.New("observability context is required")
	}
	if err := ctx.Err(); err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	node, err := source.state.GetNodeSummary(ctx, source.localNodeID)
	if err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	if node == nil {
		return observationports.NodeSnapshotDTO{}, errors.New("local node snapshot is unavailable")
	}
	cluster := source.state.GetClusterSummary(ctx)
	state := observationports.NodeState(node.State)
	ready := node.Ready
	role := "LEADER"
	lastAppliedIndex := uint64(max(node.LastAppliedIndex, 0))
	connectionCount := nonnegativeUint64(node.ConnectionCount)
	sessionCount := nonnegativeUint64(cluster.SessionCount)
	storagePressure := node.StoragePressure
	snapshot := observationports.NodeSnapshotDTO{
		ObservedAt: source.clock.Now().UTC(), Version: &node.Version,
		UptimeMillis: &node.UptimeMillis, State: &state, Ready: &ready,
		ReadinessReason: node.ReadinessReason, Role: &role,
		LastAppliedIndex: &lastAppliedIndex, ConnectionCount: &connectionCount,
		SessionCount: &sessionCount, StoragePressure: &storagePressure,
	}
	if source.runtime != nil {
		status := source.runtime.node.Status()
		role = "LEARNER"
		if containsNodeID(status.VoterIDs, source.runtime.config.NodeID) {
			role = "FOLLOWER"
			if uint64(status.LeaderID) == source.runtime.config.NodeID {
				role = "LEADER"
			}
		}
		lastAppliedIndex = status.AppliedIndex
		if source.runtime.storage != nil {
			view := source.runtime.storage.view()
			replicationHealth := view.health
			snapshot.ReplicationHealth = &replicationHealth
			if view.health != clusterReplicationHealthy {
				ready = false
				snapshot.ReadinessReason = &view.reason
			}
			storagePressure = view.storagePressure
			if source.runtime.publishPressure != nil {
				storagePressure = storagePressure || source.runtime.publishPressure.diskPressureActive()
			}
			if view.lastErrorCode != "" {
				lastErrorCode := view.lastErrorCode
				snapshot.LastErrorCode = &lastErrorCode
			}
		}
	}
	snapshot.Role = &role
	snapshot.LastAppliedIndex = &lastAppliedIndex
	snapshot.Ready = &ready
	snapshot.StoragePressure = &storagePressure
	if err := ctx.Err(); err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	return snapshot, nil
}

func (source *observabilitySource) CaptureMetricsSnapshot(ctx context.Context) (observationports.MetricsSnapshotDTO, error) {
	if ctx == nil {
		return observationports.MetricsSnapshotDTO{}, errors.New("observability context is required")
	}
	if err := ctx.Err(); err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	if source.metrics == nil {
		return observationports.MetricsSnapshotDTO{}, errors.New("metrics registry is unavailable")
	}
	snapshot, err := source.metrics.CaptureMetrics()
	if err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	if err := ctx.Err(); err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	return observationports.MetricsSnapshotDTO{
		ObservedAt: source.clock.Now().UTC(), StartedAt: source.metrics.StartedAt(), Snapshot: snapshot,
	}, nil
}

func observabilityMembershipPhase(phase membership.MemberPhase) (observationports.MembershipPhase, bool) {
	switch phase {
	case membership.MemberPhaseStaged:
		return observationports.MembershipPhaseStaged, true
	case membership.MemberPhaseLearner:
		return observationports.MembershipPhaseLearner, true
	case membership.MemberPhaseCatchingUp:
		return observationports.MembershipPhaseCatchingUp, true
	case membership.MemberPhaseVoter:
		return observationports.MembershipPhaseVoter, true
	case membership.MemberPhaseLeaving:
		return observationports.MembershipPhaseLeaving, true
	case membership.MemberPhaseRetired:
		return observationports.MembershipPhaseRetired, true
	default:
		return "", false
	}
}

func nonnegativeUint64(value int) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}

var _ observationports.ClusterSnapshotSource = (*observabilitySource)(nil)
var _ observationports.LocalSnapshotSource = (*observabilitySource)(nil)
