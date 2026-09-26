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
