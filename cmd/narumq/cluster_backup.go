package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/adapters/raft/records"
	managementports "narumq/internal/application/ports/management"
)

const clusterBackupTimeout = 5 * time.Second

var _ managementports.ClusterBackupProvider = (*clusterRuntime)(nil)

func (runtime *clusterRuntime) CaptureClusterBackup(ctx context.Context) (managementports.ClusterBackupDTO, error) {
	if ctx == nil || runtime == nil || runtime.node == nil {
		return managementports.ClusterBackupDTO{}, fmt.Errorf("%w: cluster runtime is not available", managementports.ErrClusterBackupUnavailable)
	}
	backupCtx, cancel := context.WithTimeout(ctx, clusterBackupTimeout)
	defer cancel()
	if err := runtime.checkReadiness(backupCtx); err != nil {
		return managementports.ClusterBackupDTO{}, fmt.Errorf("%w: quorum-fresh boundary: %v", managementports.ErrClusterBackupUnavailable, err)
	}

	result := make(chan clusterBackupImageResponse, 1)
	request := clusterDriverRequest{kind: clusterDriverBackup, context: backupCtx, backupResult: result}
	select {
	case runtime.requests <- request:
	case <-backupCtx.Done():
		return managementports.ClusterBackupDTO{}, fmt.Errorf("%w: %v", managementports.ErrClusterBackupUnavailable, backupCtx.Err())
	}

	var response clusterBackupImageResponse
	select {
	case response = <-result:
	case <-backupCtx.Done():
		return managementports.ClusterBackupDTO{}, fmt.Errorf("%w: %v", managementports.ErrClusterBackupUnavailable, backupCtx.Err())
	}
	if response.err != nil {
		return managementports.ClusterBackupDTO{}, fmt.Errorf("%w: capture committed image: %v", managementports.ErrClusterBackupUnavailable, response.err)
	}
	backup, err := buildClusterBackupArchive(runtime.config, response.image, response.boundarySnapshot)
	if err != nil {
		return managementports.ClusterBackupDTO{}, fmt.Errorf("build cluster backup archive: %w", err)
	}
	return backup, nil
}

func (runtime *clusterRuntime) captureBackupImage() clusterBackupImageResponse {
	image, err := runtime.node.BackupImage()
	if errors.Is(err, raftadapter.ErrNoApplicationSnapshot) {
		if snapshotErr := runtime.persistTypedSnapshot(true); snapshotErr != nil {
			return clusterBackupImageResponse{err: fmt.Errorf("persist initial typed snapshot: %w", snapshotErr)}
		}
		image, err = runtime.node.BackupImage()
	}
	if err != nil {
		return clusterBackupImageResponse{err: err}
	}
	if runtime.machine == nil || image.CommitIndex != runtime.machine.LastAppliedIndex() {
		return clusterBackupImageResponse{err: fmt.Errorf("backup commit index %d differs from applied state index", image.CommitIndex)}
	}
	boundary, err := runtime.machine.Snapshot(image.CommitIndex)
	if err != nil {
		return clusterBackupImageResponse{err: fmt.Errorf("snapshot backup boundary state: %w", err)}
	}
	boundaryData, err := encodeMembershipSnapshot(boundary)
	if err != nil {
		return clusterBackupImageResponse{err: fmt.Errorf("encode backup boundary state: %w", err)}
	}
	return clusterBackupImageResponse{image: image, boundarySnapshot: boundaryData}
}

func buildClusterBackupArchive(config clusterConfigFile, image raftadapter.BackupImageDTO, boundarySnapshot []byte) (managementports.ClusterBackupDTO, error) {
	boundary, err := records.SummarizeSnapshot(boundarySnapshot)
	if err != nil {
		return managementports.ClusterBackupDTO{}, fmt.Errorf("summarize committed state: %w", err)
	}
	if boundary.GetLastIncludedIndex() != image.CommitIndex {
		return managementports.ClusterBackupDTO{}, fmt.Errorf("boundary snapshot index %d differs from commit index %d", boundary.GetLastIncludedIndex(), image.CommitIndex)
	}

	entries := make([]*records.BackupEntry, 0, len(image.Commands)+len(image.ConfigurationChanges))
	for _, command := range image.Commands {
		entries = append(entries, &records.BackupEntry{
			Index:   command.Index,
			Payload: &records.BackupEntry_Command{Command: command.Data},
		})
	}
	for _, change := range image.ConfigurationChanges {
		action, err := backupConfigurationAction(change.Action)
		if err != nil {
			return managementports.ClusterBackupDTO{}, err
		}
		entries = append(entries, &records.BackupEntry{
			Index: change.Index,
			Payload: &records.BackupEntry_Configuration{Configuration: &records.BackupConfigurationChange{
				OperationId: change.OperationID, MemberId: uint64(change.MemberID), Action: action,
			}},
		})
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].Index < entries[right].Index })

	voterIDs := make([]uint64, len(image.VoterIDs))
	for index, id := range image.VoterIDs {
		voterIDs[index] = uint64(id)
	}
	learnerIDs := make([]uint64, len(image.LearnerIDs))
	for index, id := range image.LearnerIDs {
		learnerIDs[index] = uint64(id)
	}
	archive := &records.BackupArchive{
		Manifest: &records.BackupManifest{
			FormatMajor: records.BackupFormatMajor, FormatMinor: records.BackupFormatMinor,
			ClusterId: config.ClusterID, SourceMemberId: config.NodeID, ProducerRelease: config.ReleaseVersion,
			ClusterProtocolMajor: config.ClusterProtocolMajor, ClusterProtocolMinor: config.ClusterProtocolMinor,
			StateSchemaMajor: config.StateSchemaMajor, StateSchemaMinor: config.StateSchemaMinor,
			CreatedAtUnixMillis: uint64(time.Now().UTC().UnixMilli()),
			SnapshotIndex:       image.SnapshotIndex, SnapshotTerm: image.SnapshotTerm,
			CommitIndex: image.CommitIndex, CommitTerm: image.CommitTerm,
			VoterIds: voterIDs, LearnerIds: learnerIDs, EntryCount: uint64(len(entries)), BoundaryState: boundary,
		},
		Snapshot: image.SnapshotData,
		Entries:  entries,
	}
	encoded, checksum, err := records.EncodeBackupArchive(archive)
	if err != nil {
		return managementports.ClusterBackupDTO{}, fmt.Errorf("encode archive: %w", err)
	}
	return managementports.ClusterBackupDTO{CommitIndex: image.CommitIndex, ArchiveSHA256: checksum, Archive: encoded}, nil
}

func backupConfigurationAction(action raftadapter.MembershipChangeAction) (records.BackupConfigurationAction, error) {
	switch action {
	case raftadapter.MembershipAddLearner:
		return records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_ADD_LEARNER, nil
	case raftadapter.MembershipPromoteVoter:
		return records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_PROMOTE_VOTER, nil
	case raftadapter.MembershipRemove:
		return records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_REMOVE, nil
	default:
		return records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_UNSPECIFIED, fmt.Errorf("unsupported committed configuration action %d", action)
	}
}
