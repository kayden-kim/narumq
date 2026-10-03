package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/adapters/raft/records"
	"narumq/internal/application/membership"
	"narumq/internal/domain/release"
)

func restoreClusterBackup(config clusterConfigFile, archivePath string) error {
	if err := validateClusterConfig(config); err != nil {
		return fmt.Errorf("validate restore cluster config: %w", err)
	}
	if archivePath == "" {
		return errors.New("cluster backup archive path is required")
	}
	archive, err := readClusterBackupArchive(archivePath)
	if err != nil {
		return err
	}
	seed, err := clusterRestoreSeed(config, archive)
	if err != nil {
		return err
	}
	if err := raftadapter.BeginRestore(config.Directory); err != nil {
		return fmt.Errorf("begin cluster restore: %w", err)
	}
	if err := raftadapter.CompleteRestore(config.Directory, seed); err != nil {
		return fmt.Errorf("complete cluster restore: %w", err)
	}
	return nil
}

func readClusterBackupArchive(path string) (*records.BackupArchive, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open cluster backup archive: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect cluster backup archive: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("cluster backup archive must be a regular file")
	}
	maxBytes := int64(records.MaxBackupArchiveBytes)
	if info.Size() > maxBytes {
		return nil, errors.New("cluster backup archive exceeds the supported size limit")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read cluster backup archive: %w", err)
	}
	if int64(len(encoded)) > maxBytes {
		return nil, errors.New("cluster backup archive exceeds the supported size limit")
	}
	archive, err := records.DecodeBackupArchive(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode cluster backup archive: %w", err)
	}
	return archive, nil
}

func clusterRestoreSeed(config clusterConfigFile, archive *records.BackupArchive) (raftadapter.RestoreSeedDTO, error) {
	if archive == nil || archive.Manifest == nil {
		return raftadapter.RestoreSeedDTO{}, errors.New("cluster backup archive has no manifest")
	}
	manifest := archive.Manifest
	if config.Role != "bootstrap" || len(config.BootstrapMembers) != 3 {
		return raftadapter.RestoreSeedDTO{}, errors.New("cluster restore requires a bootstrap configuration with exactly three members")
	}
	if manifest.GetClusterId() != config.ClusterID {
		return raftadapter.RestoreSeedDTO{}, errors.New("backup cluster ID does not match cluster config")
	}
	if !release.CompatibleStrings(manifest.GetProducerRelease(), config.ReleaseVersion) {
		return raftadapter.RestoreSeedDTO{}, fmt.Errorf("backup producer release %s is outside the supported release window of %s", manifest.GetProducerRelease(), config.ReleaseVersion)
	}
	if manifest.GetClusterProtocolMajor() != config.ClusterProtocolMajor || manifest.GetClusterProtocolMinor() != config.ClusterProtocolMinor ||
		manifest.GetStateSchemaMajor() != config.StateSchemaMajor || manifest.GetStateSchemaMinor() != config.StateSchemaMinor {
		return raftadapter.RestoreSeedDTO{}, errors.New("backup cluster protocol or state schema does not match cluster config")
	}
	if len(manifest.GetVoterIds()) != 3 {
		return raftadapter.RestoreSeedDTO{}, errors.New("cluster restore requires exactly three backup voters")
	}
	if len(manifest.GetLearnerIds()) != 0 {
		return raftadapter.RestoreSeedDTO{}, errors.New("cluster restore does not support backups with learner IDs")
	}

	configuredVoters := make([]uint64, len(config.BootstrapMembers))
	for index, configured := range config.BootstrapMembers {
		configuredVoters[index] = configured.MemberID
	}
	if !slices.Equal(manifest.GetVoterIds(), configuredVoters) {
		return raftadapter.RestoreSeedDTO{}, errors.New("backup voter IDs do not match bootstrap member IDs")
	}
	if !slices.Contains(manifest.GetVoterIds(), manifest.GetSourceMemberId()) {
		return raftadapter.RestoreSeedDTO{}, errors.New("backup source member is not a current voter")
	}

	baseSnapshot, err := decodeMembershipSnapshot(archive.Snapshot)
	if err != nil {
		return raftadapter.RestoreSeedDTO{}, fmt.Errorf("decode backup typed snapshot: %w", err)
	}
	if baseSnapshot.LastIncludedIndex != manifest.GetSnapshotIndex() {
		return raftadapter.RestoreSeedDTO{}, errors.New("backup typed snapshot index does not match manifest")
	}
	machine := membership.NewStateMachine()
	if err := machine.Restore(baseSnapshot); err != nil {
		return raftadapter.RestoreSeedDTO{}, fmt.Errorf("restore backup base state: %w", err)
	}
	commands := make([]raftadapter.AppliedCommandDTO, 0, len(archive.Entries))
	configurations := make([]raftadapter.CommittedConfigurationDTO, 0, len(archive.Entries))
	for _, entry := range archive.Entries {
		if command := entry.GetCommand(); len(command) != 0 {
			commands = append(commands, raftadapter.AppliedCommandDTO{Index: entry.GetIndex(), Data: command})
			continue
		}
		change := entry.GetConfiguration()
		if change == nil {
			return raftadapter.RestoreSeedDTO{}, fmt.Errorf("backup entry at index %d has no supported payload", entry.GetIndex())
		}
		action, err := restoreConfigurationAction(change.GetAction())
		if err != nil {
			return raftadapter.RestoreSeedDTO{}, err
		}
		configurations = append(configurations, raftadapter.CommittedConfigurationDTO{
			Index: entry.GetIndex(), OperationID: change.GetOperationId(),
			MemberID: raftadapter.NodeID(change.GetMemberId()), Action: action,
		})
	}
	if err := replayClusterApplied(machine, commands, configurations); err != nil {
		return raftadapter.RestoreSeedDTO{}, fmt.Errorf("replay committed backup tail: %w", err)
	}
	if err := machine.AdvanceAppliedIndex(manifest.GetCommitIndex()); err != nil {
		return raftadapter.RestoreSeedDTO{}, fmt.Errorf("advance restored state to commit boundary: %w", err)
	}
	boundary, err := machine.Snapshot(manifest.GetCommitIndex())
	if err != nil {
		return raftadapter.RestoreSeedDTO{}, fmt.Errorf("snapshot restored commit boundary: %w", err)
	}
	boundaryData, err := encodeMembershipSnapshot(boundary)
	if err != nil {
		return raftadapter.RestoreSeedDTO{}, fmt.Errorf("encode restored commit boundary: %w", err)
	}
	boundarySummary, err := records.SummarizeSnapshot(boundaryData)
	if err != nil {
		return raftadapter.RestoreSeedDTO{}, fmt.Errorf("summarize restored commit boundary: %w", err)
	}
	if !records.EqualBackupStateSummary(boundarySummary, manifest.GetBoundaryState()) {
		return raftadapter.RestoreSeedDTO{}, errors.New("restored state does not match backup boundary summary")
	}
	for _, feature := range machine.ActiveFeatures() {
		if !release.Contains(config.Features, feature) {
			return raftadapter.RestoreSeedDTO{}, fmt.Errorf("backup active feature %q is not supported by this release", feature)
		}
	}
	if err := validateRestoredMemberIdentities(config, manifest, boundary.Members); err != nil {
		return raftadapter.RestoreSeedDTO{}, err
	}

	var localFingerprint [32]byte
	localFound := false
	for _, configured := range config.BootstrapMembers {
		if configured.MemberID != config.NodeID {
			continue
		}
		fingerprint, err := hex.DecodeString(configured.CertificateSHA256)
		if err != nil || len(fingerprint) != len(localFingerprint) {
			return raftadapter.RestoreSeedDTO{}, errors.New("local bootstrap certificate fingerprint is invalid")
		}
		copy(localFingerprint[:], fingerprint)
		localFound = true
		break
	}
	if !localFound {
		return raftadapter.RestoreSeedDTO{}, errors.New("local node is not a configured bootstrap voter")
	}
	voterIDs := make([]raftadapter.NodeID, len(manifest.GetVoterIds()))
	for index, id := range manifest.GetVoterIds() {
		voterIDs[index] = raftadapter.NodeID(id)
	}
	return raftadapter.RestoreSeedDTO{
		ClusterID: config.ClusterID, NodeID: raftadapter.NodeID(config.NodeID),
		CertificateSHA256: localFingerprint, BootstrapVoterIDs: voterIDs,
		SnapshotIndex: manifest.GetCommitIndex(), SnapshotTerm: manifest.GetCommitTerm(), ApplicationData: boundaryData,
	}, nil
}

func restoreConfigurationAction(action records.BackupConfigurationAction) (raftadapter.MembershipChangeAction, error) {
	switch action {
	case records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_ADD_LEARNER:
		return raftadapter.MembershipAddLearner, nil
	case records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_PROMOTE_VOTER:
		return raftadapter.MembershipPromoteVoter, nil
	case records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_REMOVE:
		return raftadapter.MembershipRemove, nil
	default:
		return 0, fmt.Errorf("unsupported backup configuration action %d", action)
	}
}

func validateRestoredMemberIdentities(config clusterConfigFile, manifest *records.BackupManifest, members []membership.MemberIdentityDTO) error {
	configuredByID := make(map[uint64]clusterBootstrapMember, len(config.BootstrapMembers))
	for _, configured := range config.BootstrapMembers {
		configuredByID[configured.MemberID] = configured
	}
	membersByID := make(map[uint64]membership.MemberIdentityDTO, len(members))
	voterIDs := make([]uint64, 0, len(manifest.GetVoterIds()))
	learnerIDs := make([]uint64, 0, len(manifest.GetLearnerIds()))
	for _, member := range members {
		if _, exists := membersByID[member.MemberID]; exists {
			return errors.New("restored membership state contains duplicate member IDs")
		}
		membersByID[member.MemberID] = member
		switch member.Phase {
		case membership.MemberPhaseVoter, membership.MemberPhaseLeaving:
			voterIDs = append(voterIDs, member.MemberID)
		case membership.MemberPhaseLearner, membership.MemberPhaseCatchingUp:
			learnerIDs = append(learnerIDs, member.MemberID)
		}
	}
	slices.Sort(voterIDs)
	slices.Sort(learnerIDs)
	if !slices.Equal(voterIDs, manifest.GetVoterIds()) || !slices.Equal(learnerIDs, manifest.GetLearnerIds()) {
		return errors.New("backup voter and learner IDs do not match replicated membership state")
	}
	for _, id := range manifest.GetVoterIds() {
		configured, exists := configuredByID[id]
		if !exists {
			return fmt.Errorf("backup voter %d is not a configured bootstrap member", id)
		}
		member, exists := membersByID[id]
		if !exists || (member.Phase != membership.MemberPhaseVoter && member.Phase != membership.MemberPhaseLeaving) {
			return fmt.Errorf("backup voter %d is missing from replicated membership state", id)
		}
		fingerprint, err := hex.DecodeString(configured.CertificateSHA256)
		if err != nil {
			return errors.New("bootstrap certificate fingerprint is invalid")
		}
		if member.ClusterID != config.ClusterID || member.MemberID != configured.MemberID || member.Address != configured.Address ||
			member.DNSSAN != configured.DNSSAN || !bytes.Equal(member.CertificateSHA256, fingerprint) ||
			!release.CompatibleStrings(member.ReleaseVersion, config.ReleaseVersion) || member.ClusterProtocolMajor != config.ClusterProtocolMajor ||
			member.ClusterProtocolMinor != config.ClusterProtocolMinor || member.StateSchemaMajor != config.StateSchemaMajor ||
			member.StateSchemaMinor != config.StateSchemaMinor {
			return fmt.Errorf("backup voter %d identity does not match cluster config", id)
		}
	}
	return nil
}
