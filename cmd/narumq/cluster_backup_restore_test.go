package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/adapters/raft/records"
	"narumq/internal/application/membership"
)

func TestClusterRestoreSeedReconstructsAndVerifiesBoundary(t *testing.T) {
	config := validBootstrapClusterConfig()
	archiveBytes := clusterRestoreTestArchive(t, config, nil)
	archive, err := records.DecodeBackupArchive(archiveBytes)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := clusterRestoreSeed(config, archive)
	if err != nil {
		t.Fatalf("clusterRestoreSeed() error = %v", err)
	}
	if seed.ClusterID != config.ClusterID || seed.NodeID != raftadapter.NodeID(config.NodeID) ||
		seed.SnapshotIndex != archive.Manifest.GetCommitIndex() || seed.SnapshotTerm != archive.Manifest.GetCommitTerm() ||
		!slices.Equal(seed.BootstrapVoterIDs, []raftadapter.NodeID{1, 2, 3}) {
		t.Fatalf("restore seed = %+v", seed)
	}
	boundary, err := decodeMembershipSnapshot(seed.ApplicationData)
	if err != nil {
		t.Fatalf("decode seeded application state: %v", err)
	}
	if boundary.LastIncludedIndex != archive.Manifest.GetCommitIndex() || len(boundary.Members) != 3 {
		t.Fatalf("seeded boundary = %+v", boundary)
	}
}

func TestClusterRestoreSeedRejectsIdentityLearnerAndSummaryMismatch(t *testing.T) {
	config := validBootstrapClusterConfig()
	validArchiveBytes := clusterRestoreTestArchive(t, config, nil)
	validArchive, err := records.DecodeBackupArchive(validArchiveBytes)
	if err != nil {
		t.Fatal(err)
	}

	wrongAddress := config
	wrongAddress.BootstrapMembers = slices.Clone(config.BootstrapMembers)
	wrongAddress.BootstrapMembers[1].Address = "127.0.0.1:19102"
	wrongFingerprint := config
	wrongFingerprint.BootstrapMembers = slices.Clone(config.BootstrapMembers)
	wrongFingerprint.BootstrapMembers[1].CertificateSHA256 = "d" + wrongFingerprint.BootstrapMembers[1].CertificateSHA256[1:]
	wrongRelease := config
	wrongRelease.ReleaseVersion = "2.0.0"
	wrongIDs := config
	wrongIDs.BootstrapMembers = slices.Clone(config.BootstrapMembers)
	wrongIDs.BootstrapMembers[2].MemberID = 4
	wrongCluster := config
	wrongCluster.ClusterID = "different-cluster"
	wrongRole := validJoinerClusterConfig()
	fourVoterArchiveBytes := clusterRestoreTestArchive(t, config, func(image *raftadapter.BackupImageDTO) {
		image.VoterIDs = []raftadapter.NodeID{1, 2, 3, 4}
	})
	fourVoterArchive, err := records.DecodeBackupArchive(fourVoterArchiveBytes)
	if err != nil {
		t.Fatal(err)
	}
	learnerArchiveBytes := clusterRestoreTestArchive(t, config, func(image *raftadapter.BackupImageDTO) {
		image.LearnerIDs = []raftadapter.NodeID{4}
	})
	learnerArchive, err := records.DecodeBackupArchive(learnerArchiveBytes)
	if err != nil {
		t.Fatal(err)
	}
	wrongSourceArchive, err := records.DecodeBackupArchive(validArchiveBytes)
	if err != nil {
		t.Fatal(err)
	}
	wrongSourceArchive.Manifest.SourceMemberId = 4
	wrongSummaryArchive, err := records.DecodeBackupArchive(validArchiveBytes)
	if err != nil {
		t.Fatal(err)
	}
	wrongSummaryArchive.Manifest.BoundaryState.Members.Count++

	for _, test := range []struct {
		name    string
		config  clusterConfigFile
		archive *records.BackupArchive
	}{
		{name: "cluster ID", config: wrongCluster, archive: validArchive},
		{name: "member IDs", config: wrongIDs, archive: validArchive},
		{name: "member address", config: wrongAddress, archive: validArchive},
		{name: "certificate fingerprint", config: wrongFingerprint, archive: validArchive},
		{name: "release", config: wrongRelease, archive: validArchive},
		{name: "joiner configuration", config: wrongRole, archive: validArchive},
		{name: "four voters", config: config, archive: fourVoterArchive},
		{name: "learners", config: config, archive: learnerArchive},
		{name: "source voter", config: config, archive: wrongSourceArchive},
		{name: "boundary summary", config: config, archive: wrongSummaryArchive},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := clusterRestoreSeed(test.config, test.archive); err == nil {
				t.Fatal("clusterRestoreSeed() accepted mismatched backup")
			}
		})
	}
}

// Traceability: CLU-11/CLU-14; docs/specs/cluster-session-and-delivery.md §10, §11.19.
func TestClusterRestoreSeedRejectsProtocolAndSchemaMismatch(t *testing.T) {
	config := validBootstrapClusterConfig()
	encoded := clusterRestoreTestArchive(t, config, nil)
	for _, test := range []struct {
		name   string
		mutate func(*records.BackupManifest)
	}{
		{"protocol major", func(manifest *records.BackupManifest) { manifest.ClusterProtocolMajor++ }},
		{"protocol minor", func(manifest *records.BackupManifest) { manifest.ClusterProtocolMinor++ }},
		{"schema major", func(manifest *records.BackupManifest) { manifest.StateSchemaMajor++ }},
		{"schema minor", func(manifest *records.BackupManifest) { manifest.StateSchemaMinor++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Decode a fresh archive so the case owns its manifest.
			archive, err := records.DecodeBackupArchive(encoded)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(archive.Manifest)
			if _, err := clusterRestoreSeed(config, archive); err == nil ||
				!strings.Contains(err.Error(), "backup cluster protocol or state schema does not match cluster config") {
				t.Fatalf("restore error = %v, want exact protocol/schema rejection", err)
			}
		})
	}
}

func TestClusterRestoreRejectsInvalidArchiveBeforeTouchingTarget(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.Directory = filepath.Join(t.TempDir(), "restore-target")
	archivePath := filepath.Join(t.TempDir(), "invalid.nmqbak")
	if err := os.WriteFile(archivePath, []byte("not a backup"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := restoreClusterBackup(config, archivePath); err == nil {
		t.Fatal("restoreClusterBackup() accepted invalid archive")
	}
	if _, err := os.Stat(config.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restore target after invalid archive: stat error=%v, want not-exist", err)
	}

	v2Path := filepath.Join(t.TempDir(), "v2.nmqbak")
	v2Bytes := append([]byte("NMQBKUP2"), make([]byte, 32)...)
	if err := os.WriteFile(v2Path, v2Bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	err := restoreClusterBackup(config, v2Path)
	if err == nil || !strings.Contains(err.Error(), "unsupported archive format 2") {
		t.Fatalf("restoreClusterBackup(v2) error = %v, want unsupported archive format 2", err)
	}
	if _, err := os.Stat(config.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restore target after v2 archive: stat error=%v, want not-exist", err)
	}
}

func TestClusterRestoreCLISeedsAndExits(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.Directory = filepath.Join(t.TempDir(), "node1")
	archiveBytes := clusterRestoreTestArchive(t, config, nil)
	archivePath := filepath.Join(t.TempDir(), "backup.nmqbak")
	if err := os.WriteFile(archivePath, archiveBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "cluster.json")
	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := runWithContext(context.Background(), []string{
		"-cluster-config", configPath, "-cluster-restore", archivePath,
	}, logger); err != nil {
		t.Fatalf("runWithContext(restore) error = %v", err)
	}

	bootstrapIDs, _, err := clusterBootstrapIdentities(config)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := [32]byte{}
	localFound := false
	for _, member := range config.BootstrapMembers {
		if member.MemberID != config.NodeID {
			continue
		}
		fingerprintBytes, err := hex.DecodeString(member.CertificateSHA256)
		if err != nil || len(fingerprintBytes) != len(fingerprint) {
			t.Fatalf("decode local certificate fingerprint: length=%d error=%v", len(fingerprintBytes), err)
		}
		copy(fingerprint[:], fingerprintBytes)
		localFound = true
		break
	}
	if !localFound {
		t.Fatal("bootstrap config does not contain the local node")
	}
	node, err := raftadapter.Open(raftadapter.Config{
		NodeID: raftadapter.NodeID(config.NodeID), Directory: config.Directory,
		DynamicMembership: true, ClusterID: config.ClusterID,
		CertificateSHA256: fingerprint, BootstrapVoterIDs: bootstrapIDs,
	})
	if err != nil {
		t.Fatalf("open restored node: %v", err)
	}
	defer func() {
		if err := node.Close(); err != nil {
			t.Errorf("close restored node: %v", err)
		}
	}()
	status := node.Status()
	if status.AppliedIndex != 3 || !slices.Equal(status.VoterIDs, bootstrapIDs) || len(status.LearnerIDs) != 0 {
		t.Fatalf("restored status = %+v", status)
	}
	restoredState, err := decodeMembershipSnapshot(node.ApplicationSnapshot())
	if err != nil {
		t.Fatalf("decode restored application snapshot: %v", err)
	}
	if restoredState.LastIncludedIndex != 3 || len(restoredState.Members) != 3 {
		t.Fatalf("restored application state = %+v", restoredState)
	}
}

func TestClusterRestorePreservesForeignTargetEntry(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.Directory = filepath.Join(t.TempDir(), "node1")
	if err := os.MkdirAll(config.Directory, 0o755); err != nil {
		t.Fatal(err)
	}
	foreignPath := filepath.Join(config.Directory, "operator-data")
	if err := os.WriteFile(foreignPath, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(t.TempDir(), "backup.nmqbak")
	if err := os.WriteFile(archivePath, clusterRestoreTestArchive(t, config, nil), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := restoreClusterBackup(config, archivePath); err == nil {
		t.Fatal("restoreClusterBackup() accepted a target containing foreign data")
	}
	if data, err := os.ReadFile(foreignPath); err != nil || string(data) != "keep" {
		t.Fatalf("foreign target entry after rejected restore = %q, error=%v", data, err)
	}
}

func clusterRestoreTestArchive(t *testing.T, config clusterConfigFile, mutate func(*raftadapter.BackupImageDTO)) []byte {
	t.Helper()
	_, members, err := clusterBootstrapIdentities(config)
	if err != nil {
		t.Fatal(err)
	}
	machine := membership.NewStateMachine()
	if err := machine.InitializeBootstrap(3, members); err != nil {
		t.Fatalf("initialize backup state: %v", err)
	}
	snapshot, err := machine.Snapshot(3)
	if err != nil {
		t.Fatalf("snapshot backup state: %v", err)
	}
	encodedSnapshot, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("encode backup state: %v", err)
	}
	image := raftadapter.BackupImageDTO{
		SnapshotIndex: 3, SnapshotTerm: 1, SnapshotData: encodedSnapshot,
		CommitIndex: 3, CommitTerm: 1, VoterIDs: []raftadapter.NodeID{1, 2, 3},
	}
	if mutate != nil {
		mutate(&image)
	}
	backup, err := buildClusterBackupArchive(config, image, encodedSnapshot)
	if err != nil {
		t.Fatalf("build backup fixture: %v", err)
	}
	return backup.Archive
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterBackupArchivePreservesDurableQoSState(t *testing.T) {
	config := validBootstrapClusterConfig()
	runtime, stop := newTestClusterOfflineRuntime(t, config)
	defer stop()
	seedClusterDurableDelivery(t, runtime)

	boundary, err := runtime.machine.Snapshot(runtime.machine.LastAppliedIndex())
	if err != nil {
		t.Fatalf("snapshot committed durable state: %v", err)
	}
	encodedSnapshot, err := encodeMembershipSnapshot(boundary)
	if err != nil {
		t.Fatalf("encode committed durable state: %v", err)
	}
	image := raftadapter.BackupImageDTO{
		SnapshotIndex: boundary.LastIncludedIndex, SnapshotTerm: 1, SnapshotData: encodedSnapshot,
		CommitIndex: boundary.LastIncludedIndex, CommitTerm: 1, VoterIDs: []raftadapter.NodeID{1, 2, 3},
	}
	backup, err := buildClusterBackupArchive(config, image, encodedSnapshot)
	if err != nil {
		t.Fatalf("build committed durable backup: %v", err)
	}
	archive, err := records.DecodeBackupArchive(backup.Archive)
	if err != nil {
		t.Fatalf("decode committed durable backup: %v", err)
	}
	if archive.Manifest.GetBoundaryState().GetInflight().GetCount() != 1 {
		t.Fatalf("backup inflight summary=%+v, want one QoS 2 delivery", archive.Manifest.GetBoundaryState().GetInflight())
	}
	restored, err := decodeMembershipSnapshot(archive.Snapshot)
	if err != nil {
		t.Fatalf("decode backup snapshot: %v", err)
	}
	if len(restored.DeliverySessions) != 1 {
		t.Fatalf("backup delivery sessions=%+v, want one", restored.DeliverySessions)
	}
	session := restored.DeliverySessions[0]
	if session.ClientID != "client-a" || len(session.Messages) != 1 ||
		session.Messages[0].Sequence != 1 || session.Messages[0].QoS != 2 ||
		session.Messages[0].Phase != membership.DeliveryPhaseQueued ||
		string(session.Messages[0].Payload) != "first" {
		t.Fatalf("backup did not preserve queued QoS 2 state: %+v", session)
	}

	inconsistent := image
	inconsistent.CommitIndex++
	if _, err := buildClusterBackupArchive(config, inconsistent, encodedSnapshot); err == nil {
		t.Fatal("backup accepted a boundary snapshot behind the commit index")
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterRestoreSeedReplaysConfigurationTail(t *testing.T) {
	config := validBootstrapClusterConfig()
	_, bootstrapMembers, err := clusterBootstrapIdentities(config)
	if err != nil {
		t.Fatal(err)
	}
	machine := membership.NewStateMachine()
	if err := machine.InitializeBootstrap(1, bootstrapMembers); err != nil {
		t.Fatalf("initialize restore base: %v", err)
	}
	baseSnapshot, err := machine.Snapshot(machine.LastAppliedIndex())
	if err != nil {
		t.Fatalf("snapshot restore base: %v", err)
	}
	baseData, err := encodeMembershipSnapshot(baseSnapshot)
	if err != nil {
		t.Fatalf("encode restore base: %v", err)
	}
	joinID, err := newClusterOperationID()
	if err != nil {
		t.Fatal(err)
	}
	removeID, err := newClusterOperationID()
	if err != nil {
		t.Fatal(err)
	}
	member := bootstrapMembers[0]
	member.MemberID = 4
	member.Address = "127.0.0.1:19004"
	member.DNSSAN = "node-4"
	member.Phase = membership.MemberPhaseStaged
	member.OperationID = joinID
	command := func(index uint64, operationID string, action membership.MembershipAction, phase membership.MemberPhase, target uint64) raftadapter.AppliedCommandDTO {
		t.Helper()
		member.OperationID = operationID
		member.Phase = phase
		record := &membership.MembershipRecordDTO{
			OperationID: operationID, Action: action, Member: &member, TargetMemberID: target,
		}
		if action == membership.MembershipActionStageJoin {
			record.IdempotencyKey = "join-4"
		} else if action == membership.MembershipActionBeginRemove {
			record.IdempotencyKey = "remove-4"
		} else {
			record.IdempotencyKey = "join-4"
		}
		data, err := encodeMembershipCommand(membership.CommandDTO{
			SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
			OperationID: operationID, Membership: record,
		})
		if err != nil {
			t.Fatalf("encode membership command at index %d: %v", index, err)
		}
		return raftadapter.AppliedCommandDTO{Index: index, Data: data}
	}
	commands := []raftadapter.AppliedCommandDTO{
		command(2, joinID, membership.MembershipActionStageJoin, membership.MemberPhaseStaged, 0),
		command(4, joinID, membership.MembershipActionAddLearner, membership.MemberPhaseCatchingUp, 0),
		command(6, removeID, membership.MembershipActionBeginRemove, membership.MemberPhaseLeaving, 4),
	}
	configurations := []raftadapter.CommittedConfigurationDTO{
		{Index: 3, OperationID: joinID, MemberID: 4, Action: raftadapter.MembershipAddLearner},
		{Index: 5, OperationID: joinID, MemberID: 4, Action: raftadapter.MembershipPromoteVoter},
		{Index: 7, OperationID: removeID, MemberID: 4, Action: raftadapter.MembershipRemove},
	}
	if err := replayClusterApplied(machine, commands, configurations); err != nil {
		t.Fatalf("apply membership configuration tail: %v", err)
	}
	boundary, err := machine.Snapshot(machine.LastAppliedIndex())
	if err != nil {
		t.Fatalf("snapshot restore boundary: %v", err)
	}
	boundaryData, err := encodeMembershipSnapshot(boundary)
	if err != nil {
		t.Fatalf("encode restore boundary: %v", err)
	}
	boundarySummary, err := records.SummarizeSnapshot(boundaryData)
	if err != nil {
		t.Fatalf("summarize restore boundary: %v", err)
	}
	entries := []*records.BackupEntry{
		{Index: 2, Payload: &records.BackupEntry_Command{Command: commands[0].Data}},
		{Index: 3, Payload: &records.BackupEntry_Configuration{Configuration: &records.BackupConfigurationChange{
			OperationId: joinID, MemberId: 4, Action: records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_ADD_LEARNER,
		}}},
		{Index: 4, Payload: &records.BackupEntry_Command{Command: commands[1].Data}},
		{Index: 5, Payload: &records.BackupEntry_Configuration{Configuration: &records.BackupConfigurationChange{
			OperationId: joinID, MemberId: 4, Action: records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_PROMOTE_VOTER,
		}}},
		{Index: 6, Payload: &records.BackupEntry_Command{Command: commands[2].Data}},
		{Index: 7, Payload: &records.BackupEntry_Configuration{Configuration: &records.BackupConfigurationChange{
			OperationId: removeID, MemberId: 4, Action: records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_REMOVE,
		}}},
	}
	archive := &records.BackupArchive{
		Manifest: &records.BackupManifest{
			FormatMajor: records.BackupFormatMajor, FormatMinor: records.BackupFormatMinor,
			ClusterId: config.ClusterID, SourceMemberId: 1, ProducerRelease: config.ReleaseVersion,
			ClusterProtocolMajor: config.ClusterProtocolMajor, ClusterProtocolMinor: config.ClusterProtocolMinor,
			StateSchemaMajor: config.StateSchemaMajor, StateSchemaMinor: config.StateSchemaMinor,
			CreatedAtUnixMillis: 1, SnapshotIndex: baseSnapshot.LastIncludedIndex, SnapshotTerm: 1,
			CommitIndex: boundary.LastIncludedIndex, CommitTerm: 1, VoterIds: []uint64{1, 2, 3},
			EntryCount: uint64(len(entries)), BoundaryState: boundarySummary,
		},
		Snapshot: baseData, Entries: entries,
	}
	encoded, _, err := records.EncodeBackupArchive(archive)
	if err != nil {
		t.Fatalf("encode configuration-tail archive: %v", err)
	}
	decoded, err := records.DecodeBackupArchive(encoded)
	if err != nil {
		t.Fatalf("decode configuration-tail archive: %v", err)
	}
	seed, err := clusterRestoreSeed(config, decoded)
	if err != nil {
		t.Fatalf("restore configuration-tail archive: %v", err)
	}
	restored, err := decodeMembershipSnapshot(seed.ApplicationData)
	if err != nil {
		t.Fatalf("decode restored state: %v", err)
	}
	if seed.SnapshotIndex != 7 || restored.LastIncludedIndex != 7 {
		t.Fatalf("restore commit boundary seed=%d state=%d", seed.SnapshotIndex, restored.LastIncludedIndex)
	}
	retired := false
	for _, restoredMember := range restored.Members {
		if restoredMember.MemberID == 4 && restoredMember.Phase == membership.MemberPhaseRetired {
			retired = true
		}
	}
	if !retired {
		t.Fatalf("configuration replay did not retain retired member identity: %+v", restored.Members)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestClusterBackupConfigurationActionsRoundTrip(t *testing.T) {
	for _, testCase := range []struct {
		raftAction   raftadapter.MembershipChangeAction
		backupAction records.BackupConfigurationAction
	}{
		{raftadapter.MembershipAddLearner, records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_ADD_LEARNER},
		{raftadapter.MembershipPromoteVoter, records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_PROMOTE_VOTER},
		{raftadapter.MembershipRemove, records.BackupConfigurationAction_BACKUP_CONFIGURATION_ACTION_REMOVE},
	} {
		backupAction, err := backupConfigurationAction(testCase.raftAction)
		if err != nil || backupAction != testCase.backupAction {
			t.Fatalf("backup configuration action %d=(%d,%v), want %d", testCase.raftAction, backupAction, err, testCase.backupAction)
		}
		raftAction, err := restoreConfigurationAction(testCase.backupAction)
		if err != nil || raftAction != testCase.raftAction {
			t.Fatalf("restore configuration action %d=(%d,%v), want %d", testCase.backupAction, raftAction, err, testCase.raftAction)
		}
	}
	if _, err := backupConfigurationAction(raftadapter.MembershipChangeAction(99)); err == nil {
		t.Fatal("unsupported Raft configuration action was encoded")
	}
	if _, err := restoreConfigurationAction(records.BackupConfigurationAction(99)); err == nil {
		t.Fatal("unsupported backup configuration action was restored")
	}
}

// Traceability: CLU-11/CLU-14; docs/specs/cluster-session-and-delivery.md §10, §11.19.
func TestClusterRestoreSeedAppliesAdjacentReleaseWindow(t *testing.T) {
	for _, test := range []struct {
		archive string
		restore string
		allowed bool
	}{
		{"1.1.0", "1.2.0", true},
		{"1.2.0", "1.1.0", true},
		{"1.2.5", "1.2.0", true},
		{"1.0.0", "1.2.0", false},
		{"1.2.0", "1.0.0", false},
		{"2.2.0", "1.2.0", false},
	} {
		t.Run(test.archive+"->"+test.restore, func(t *testing.T) {
			archiveConfig := validBootstrapClusterConfig()
			archiveConfig.ReleaseVersion = test.archive
			archive, err := records.DecodeBackupArchive(clusterRestoreTestArchive(t, archiveConfig, nil))
			if err != nil {
				t.Fatal(err)
			}
			restoreConfig := validBootstrapClusterConfig()
			restoreConfig.ReleaseVersion = test.restore
			seed, err := clusterRestoreSeed(restoreConfig, archive)
			if !test.allowed {
				if err == nil || !strings.Contains(err.Error(), "outside the supported release window") {
					t.Fatalf("restore error = %v, want release window rejection", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			boundary, err := decodeMembershipSnapshot(seed.ApplicationData)
			if err != nil {
				t.Fatal(err)
			}
			if len(boundary.Members) != 3 {
				t.Fatalf("seeded members = %+v, want three voters", boundary.Members)
			}
			for _, member := range boundary.Members {
				if member.ReleaseVersion != test.archive {
					t.Fatalf("seeded member %d release = %q, want preserved %q", member.MemberID, member.ReleaseVersion, test.archive)
				}
			}
		})
	}
}

// Traceability: CLU-11/CLU-14; docs/specs/cluster-session-and-delivery.md §10, §11.19.
func TestClusterRestoreSeedRejectsUnsupportedActiveFeature(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.ReleaseVersion = "1.2.0"
	config.Features = []string{"compatibility-probe"}
	archive, err := records.DecodeBackupArchive(clusterRestoreTestArchiveWithActiveFeature(t, config, "compatibility-probe"))
	if err != nil {
		t.Fatal(err)
	}
	restoreConfig := validBootstrapClusterConfig()
	restoreConfig.ReleaseVersion = "1.1.0"
	restoreConfig.Features = nil
	if _, err := clusterRestoreSeed(restoreConfig, archive); err == nil ||
		!strings.Contains(err.Error(), `backup active feature "compatibility-probe" is not supported by this release`) {
		t.Fatalf("restore error = %v, want unsupported active feature", err)
	}
	seed, err := clusterRestoreSeed(config, archive)
	if err != nil {
		t.Fatal(err)
	}
	boundary, err := decodeMembershipSnapshot(seed.ApplicationData)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(boundary.ActiveFeatures, []string{"compatibility-probe"}) {
		t.Fatalf("restored active features = %v", boundary.ActiveFeatures)
	}
}

// Traceability: CLU-11/CLU-14; docs/specs/cluster-session-and-delivery.md §10, §11.19.
func TestClusterRestoreMemberReleaseWindow(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.ReleaseVersion = "1.2.0"
	archive, err := records.DecodeBackupArchive(clusterRestoreTestArchive(t, config, nil))
	if err != nil {
		t.Fatal(err)
	}
	_, members, err := clusterBootstrapIdentities(config)
	if err != nil {
		t.Fatal(err)
	}
	members[1].ReleaseVersion = "1.1.0"
	if err := validateRestoredMemberIdentities(config, archive.Manifest, members); err != nil {
		t.Fatal(err)
	}
	members[1].ReleaseVersion = "1.0.0"
	if err := validateRestoredMemberIdentities(config, archive.Manifest, members); err == nil {
		t.Fatal("accepted voter outside release window")
	}
}

func clusterRestoreTestArchiveWithActiveFeature(t *testing.T, config clusterConfigFile, feature string) []byte {
	t.Helper()
	_, members, err := clusterBootstrapIdentities(config)
	if err != nil {
		t.Fatal(err)
	}
	machine := membership.NewStateMachine()
	if err := machine.InitializeBootstrap(3, members); err != nil {
		t.Fatal(err)
	}
	result, err := machine.Apply(4, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID:       "00000000-0000-4000-8000-000000000001",
		FeatureActivation: &membership.FeatureActivationDTO{Feature: feature},
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("activate backup feature = (%+v, %v), want applied", result, err)
	}
	snapshot, err := machine.Snapshot(4)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	backup, err := buildClusterBackupArchive(config, raftadapter.BackupImageDTO{
		SnapshotIndex: 4, SnapshotTerm: 1, SnapshotData: encoded,
		CommitIndex: 4, CommitTerm: 1, VoterIDs: []raftadapter.NodeID{1, 2, 3},
	}, encoded)
	if err != nil {
		t.Fatal(err)
	}
	return backup.Archive
}
