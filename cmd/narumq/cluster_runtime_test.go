package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/membership"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

func TestMergeSeedAdmissionPeersUsesCommittedIdentityAndActiveMembership(t *testing.T) {
	admitted := []tcpadapter.PeerIdentityDTO{
		{MemberID: 1, Address: "seed-1"},
		{MemberID: 2, Address: "seed-2"},
		{MemberID: 3, Address: "seed-3"},
	}
	committed := []tcpadapter.PeerIdentityDTO{{MemberID: 3, Address: "committed-3"}}
	status := raftadapter.StatusDTO{
		VoterIDs:   []raftadapter.NodeID{1, 2, 3, 4},
		LearnerIDs: []raftadapter.NodeID{5},
	}

	got := mergeSeedAdmissionPeers(committed, admitted, status, 4)
	want := []tcpadapter.PeerIdentityDTO{
		{MemberID: 1, Address: "seed-1"},
		{MemberID: 2, Address: "seed-2"},
		{MemberID: 3, Address: "committed-3"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merged peer identities = %+v, want %+v", got, want)
	}
}

func TestMergeSeedAdmissionPeersKeepsSeedsBeforeConfState(t *testing.T) {
	admitted := []tcpadapter.PeerIdentityDTO{
		{MemberID: 1, Address: "seed-1"},
		{MemberID: 2, Address: "seed-2"},
	}

	got := mergeSeedAdmissionPeers(nil, admitted, raftadapter.StatusDTO{}, 4)
	if !reflect.DeepEqual(got, admitted) {
		t.Fatalf("seed peers before ConfState = %+v, want %+v", got, admitted)
	}
}

func TestUnconfiguredRemotePeerDistinguishesPendingAndInvalidTargets(t *testing.T) {
	status := raftadapter.StatusDTO{
		VoterIDs:   []raftadapter.NodeID{1, 2},
		LearnerIDs: []raftadapter.NodeID{3},
	}
	for _, test := range []struct {
		name   string
		peerID raftadapter.NodeID
		want   bool
	}{
		{name: "unconfigured remote", peerID: 4, want: true},
		{name: "other unconfigured remote", peerID: 5, want: true},
		{name: "voter", peerID: 1},
		{name: "learner", peerID: 3},
		{name: "zero ID", peerID: 0},
		{name: "local ID", peerID: 6},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isUnconfiguredRemotePeer(status, 6, test.peerID); got != test.want {
				t.Fatalf("unconfigured remote peer = %v, want %v", got, test.want)
			}
		})
	}
}

func TestClusterReadinessGate(t *testing.T) {
	base := clusterReadinessSnapshot{
		initialized: true, projectionFailed: false,
		bootstrapped: true, dynamicMembership: true,
		localVoter: true, localPhaseVoter: true, tenantQuotaProfileCommitted: true, releaseProfileSynced: true,
		projectionIndex: 10, appliedIndex: 10,
	}

	cases := []struct {
		name          string
		mutate        func(*clusterReadinessSnapshot)
		barrier       uint64
		wantAdmits    bool
		wantSatisfied bool
		wantReason    string
	}{
		{
			name: "not initialized",
			mutate: func(s *clusterReadinessSnapshot) {
				s.initialized = false
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
			wantReason:    "uninitialized",
		},
		{
			name: "projection failed with equal indexes",
			mutate: func(s *clusterReadinessSnapshot) {
				s.projectionFailed = true
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
			wantReason:    "projection_failed",
		},
		{
			name: "applied-index gap (projection 5 < applied 6)",
			mutate: func(s *clusterReadinessSnapshot) {
				s.projectionIndex = 5
				s.appliedIndex = 6
			},
			barrier:       5,
			wantAdmits:    false,
			wantSatisfied: false,
			wantReason:    "projection_behind_applied",
		},
		{
			name: "not bootstrapped",
			mutate: func(s *clusterReadinessSnapshot) {
				s.bootstrapped = false
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
			wantReason:    "not_bootstrapped",
		},
		{
			name: "learner (localVoter=false)",
			mutate: func(s *clusterReadinessSnapshot) {
				s.localVoter = false
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
			wantReason:    "local_not_voter",
		},
		{
			name: "LEAVING (localPhaseVoter=false)",
			mutate: func(s *clusterReadinessSnapshot) {
				s.localPhaseVoter = false
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
			wantReason:    "local_phase_not_voter",
		},
		{
			name: "barrier > applied",
			mutate: func(s *clusterReadinessSnapshot) {
				s.appliedIndex = 10
				s.projectionIndex = 12
			},
			barrier:       11,
			wantAdmits:    true,
			wantSatisfied: false,
			wantReason:    "proof_above_applied",
		},
		{
			name: "barrier > projection",
			mutate: func(s *clusterReadinessSnapshot) {
				s.appliedIndex = 10
				s.projectionIndex = 10
			},
			barrier:       11,
			wantAdmits:    true,
			wantSatisfied: false,
			wantReason:    "proof_above_applied",
		},
		{
			name: "release profile not synchronized",
			mutate: func(s *clusterReadinessSnapshot) {
				s.releaseProfileSynced = false
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
			wantReason:    "release_profile_unsynced",
		},
		{
			name: "tenant quota profile not committed",
			mutate: func(s *clusterReadinessSnapshot) {
				s.tenantQuotaProfileCommitted = false
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
			wantReason:    "quota_profile_missing",
		},
		{
			name:          "all satisfied",
			mutate:        func(s *clusterReadinessSnapshot) {},
			barrier:       10,
			wantAdmits:    true,
			wantSatisfied: true,
			wantReason:    "unspecified",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := base
			tc.mutate(&s)
			if got := s.admitsBarrier(); got != tc.wantAdmits {
				t.Errorf("admitsBarrier() = %t, want %t", got, tc.wantAdmits)
			}
			if got := s.satisfiedAt(tc.barrier); got != tc.wantSatisfied {
				t.Errorf("satisfiedAt(%d) = %t, want %t", tc.barrier, got, tc.wantSatisfied)
			}
			if got := readinessSnapshotRejection(&s, tc.barrier); got != tc.wantReason {
				t.Errorf("readinessSnapshotRejection(%+v, %d) = %q, want %q", s, tc.barrier, got, tc.wantReason)
			}
		})
	}
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §1, §3.1.
func TestClusterProjectionReadyRequiresInitializedCurrentProjection(t *testing.T) {
	config := validBootstrapClusterConfig()
	directory := t.TempDir()
	if err := raftadapter.BeginRestore(directory); err != nil {
		t.Fatalf("begin restore: %v", err)
	}
	fingerprint := [32]byte{0x42}
	if err := raftadapter.CompleteRestore(directory, raftadapter.RestoreSeedDTO{
		ClusterID: config.ClusterID, NodeID: raftadapter.NodeID(config.NodeID),
		CertificateSHA256: fingerprint, BootstrapVoterIDs: []raftadapter.NodeID{1, 2, 3},
		SnapshotIndex: 10, SnapshotTerm: 1, ApplicationData: []byte("membership snapshot"),
	}); err != nil {
		t.Fatalf("complete restore: %v", err)
	}
	node, err := raftadapter.Open(raftadapter.Config{
		NodeID: raftadapter.NodeID(config.NodeID), Directory: directory, DynamicMembership: true,
		ClusterID: config.ClusterID, CertificateSHA256: fingerprint,
		BootstrapVoterIDs: []raftadapter.NodeID{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("open restored node: %v", err)
	}
	defer node.Close()
	status := node.Status()
	if !status.Bootstrapped || !status.DynamicMembership || status.AppliedIndex != 10 || !containsNodeID(status.VoterIDs, config.NodeID) {
		t.Fatalf("restored node status = %+v", status)
	}

	var storageNow atomic.Int64
	storageNow.Store(time.Now().UnixNano())
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	if _, err := machine.Apply(2, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: "00000000-0000-4000-8000-000000000001",
		TenantQuota: &membership.TenantQuotaChangeDTO{Profile: membership.DefaultTenantQuotaProfile()},
	}); err != nil {
		t.Fatalf("install default tenant quota profile: %v", err)
	}
	runtime := &clusterRuntime{
		config: config, node: node, machine: machine,
		storage:        newClusterStorageMonitor(func() time.Time { return time.Unix(0, storageNow.Load()) }, nil),
		releaseProfile: &clusterReleaseProfileController{},
	}
	runtime.releaseProfile.synced.Store(true)
	runtime.initialized.Store(true)
	runtime.projectionIndex.Store(status.AppliedIndex)
	runtime.publishProjectionAdmissionStatus(status)
	if !runtime.clusterProjectionReady() {
		t.Fatal("caught-up initialized local projection was not admitted")
	}

	runtime.initialized.Store(false)
	if runtime.clusterProjectionReady() {
		t.Fatal("uninitialized runtime admitted local projection")
	}
	runtime.initialized.Store(true)

	runtime.projectionFailed.Store(true)
	if runtime.clusterProjectionReady() {
		t.Fatal("failed projection was admitted")
	}
	runtime.projectionFailed.Store(false)
	runtime.projectionAdmissionSequence.Store(1)
	runtime.finishProjectionAdmissionSequence(errors.New("Ready processing failed"))
	if runtime.projectionAdmissionSequence.Load()&1 != 0 {
		t.Fatal("Ready failure left the admission sequence active")
	}
	if runtime.clusterProjectionReady() {
		t.Fatal("failed Ready driver admitted local projection")
	}
	runtime.driverFailed.Store(false)
	runtime.projectionIndex.Store(status.AppliedIndex - 1)

	startedAt := time.Now()
	admitted := runtime.clusterProjectionReady()
	elapsed := time.Since(startedAt)
	snapshot := runtime.projectionReadinessSnapshot()
	if admitted {
		t.Fatalf("projection behind the applied index was admitted: snapshot=%+v", snapshot)
	}
	if elapsed >= time.Second {
		t.Fatalf("behind-index projection check took %s, want an immediate fail-closed result", elapsed)
	}
	t.Run("admission queued during Ready is served before the next drain", func(t *testing.T) {
		runtime.projectionIndex.Store(status.AppliedIndex)
		runtime.requests = make(chan clusterDriverRequest, 1)
		defer func() { runtime.requests = nil }()
		runtime.projectionAdmissionSequence.Add(1)
		result := make(chan bool, 1)
		go func() { result <- runtime.clusterProjectionReady() }()
		var request clusterDriverRequest
		select {
		case request = <-runtime.requests:
		case <-time.After(250 * time.Millisecond):
			runtime.finishProjectionAdmissionSequence(nil)
			<-result
			t.Fatal("active Ready admission never queued a driver checkpoint")
		}
		runtime.finishProjectionAdmissionSequence(nil)
		runtime.handleDriverRequest(request, nil, nil)
		runtime.resolveProjectionRequests(nil, true)
		// The next drain begins before the blocked ingress goroutine can sample
		// the even sequence. Its checkpoint still linearized in the completed drain.
		runtime.projectionAdmissionSequence.Add(1)
		defer runtime.finishProjectionAdmissionSequence(nil)
		select {
		case admitted := <-result:
			if !admitted {
				t.Fatal("healthy completed projection checkpoint was rejected")
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatal("admission missed the stable checkpoint between Ready drains")
		}
	})

	t.Run("queued admission rejects a projection gap", func(t *testing.T) {
		runtime.requests = make(chan clusterDriverRequest, 1)
		defer func() { runtime.requests = nil }()
		runtime.projectionAdmissionSequence.Add(1)
		result := make(chan bool, 1)
		go func() { result <- runtime.clusterProjectionReady() }()
		var request clusterDriverRequest
		select {
		case request = <-runtime.requests:
		case <-time.After(time.Second):
			runtime.finishProjectionAdmissionSequence(nil)
			<-result
			t.Fatal("admission checkpoint was not queued")
		}
		runtime.projectionIndex.Store(status.AppliedIndex - 1)
		runtime.finishProjectionAdmissionSequence(nil)
		runtime.handleDriverRequest(request, nil, nil)
		runtime.resolveProjectionRequests(nil, true)
		if <-result {
			t.Fatal("queued admission accepted an unapplied projection")
		}
	})

	t.Run("driver exit releases blocked admission", func(t *testing.T) {
		runtime.projectionIndex.Store(status.AppliedIndex)
		runtime.requests = make(chan clusterDriverRequest, 1)
		runtime.driverStopped = make(chan struct{})
		defer func() { runtime.requests, runtime.driverStopped = nil, nil }()
		runtime.projectionAdmissionSequence.Add(1)
		defer runtime.finishProjectionAdmissionSequence(nil)
		result := make(chan bool, 1)
		go func() { result <- runtime.clusterProjectionReady() }()
		select {
		case <-runtime.requests:
		case <-time.After(time.Second):
			close(runtime.driverStopped)
			<-result
			t.Fatal("admission checkpoint was not queued")
		}
		close(runtime.driverStopped)
		select {
		case admitted := <-result:
			if admitted {
				t.Fatal("stopped driver accepted pending admission")
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatal("driver exit left admission blocked")
		}
	})

	runtime.projectionIndex.Store(status.AppliedIndex)
	runtime.storage.begin()
	runtime.projectionAdmissionSequence.Add(1)
	defer runtime.projectionAdmissionSequence.Add(1)
	defer runtime.storage.end()
	readyResult := make(chan bool, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		readyResult <- runtime.clusterProjectionReady()
	}()
	<-started
	time.Sleep(clusterProjectionAdmissionPollInterval)
	storageNow.Add(int64(clusterStorageSlowThreshold))
	select {
	case ready := <-readyResult:
		if ready {
			t.Fatal("admission passed while storage was degraded during a Ready drain")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("admission waited behind the active Ready drain after storage degraded")
	}
}

func TestClusterSessionID(t *testing.T) {
	sessionID, err := clusterSessionID("acceptance", "client-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tenant, _ := tenancy.NewTenantID("acceptance")
	key, _ := tenancy.NewSessionKey(tenant, "client-1")
	want := routing.SessionID(key.RoutingID())
	if sessionID != want {
		t.Fatalf("sessionID = %s, want %s", sessionID, want)
	}
	if _, err := clusterSessionID("", "client-1"); err == nil {
		t.Fatal("expected error for empty tenant")
	}
	if _, err := clusterSessionID("acceptance", ""); err == nil {
		t.Fatal("expected error for empty client")
	}
}

func TestContainsNodeID(t *testing.T) {
	nodes := []raftadapter.NodeID{1, 2, 3}
	if !containsNodeID(nodes, 2) {
		t.Fatal("expected contains 2")
	}
	if containsNodeID(nodes, 4) {
		t.Fatal("expected not contains 4")
	}
	if containsNodeID(nil, 1) {
		t.Fatal("expected not contains in nil")
	}
}

func TestConfigurationAction(t *testing.T) {
	cases := []struct {
		input   raftadapter.MembershipChangeAction
		want    membership.ConfigurationAction
		wantErr bool
	}{
		{raftadapter.MembershipAddLearner, membership.ConfigurationActionAddLearner, false},
		{raftadapter.MembershipPromoteVoter, membership.ConfigurationActionPromoteVoter, false},
		{raftadapter.MembershipRemove, membership.ConfigurationActionRemoveMember, false},
		{raftadapter.MembershipChangeAction(99), 0, true},
	}
	for _, tc := range cases {
		got, err := configurationAction(tc.input)
		if (err != nil) != tc.wantErr {
			t.Fatalf("action %d err=%v, wantErr=%t", tc.input, err, tc.wantErr)
		}
		if got != tc.want {
			t.Fatalf("action %d got=%d, want=%d", tc.input, got, tc.want)
		}
	}
}

func TestPeerPhaseFromMembership(t *testing.T) {
	cases := []struct {
		input   membership.MemberPhase
		want    tcpadapter.PeerPhaseDTO
		wantErr bool
	}{
		{membership.MemberPhaseStaged, tcpadapter.PeerPhaseStaged, false},
		{membership.MemberPhaseLearner, tcpadapter.PeerPhaseLearner, false},
		{membership.MemberPhaseCatchingUp, tcpadapter.PeerPhaseCatchingUp, false},
		{membership.MemberPhaseVoter, tcpadapter.PeerPhaseVoter, false},
		{membership.MemberPhaseLeaving, tcpadapter.PeerPhaseLeaving, false},
		{membership.MemberPhaseRetired, tcpadapter.PeerPhaseRetired, false},
		{membership.MemberPhase(99), 0, true},
	}
	for _, tc := range cases {
		got, err := peerPhaseFromMembership(tc.input)
		if (err != nil) != tc.wantErr {
			t.Fatalf("phase %d err=%v, wantErr=%t", tc.input, err, tc.wantErr)
		}
		if got != tc.want {
			t.Fatalf("phase %d got=%d, want=%d", tc.input, got, tc.want)
		}
	}
}

func TestPeerIdentityFromMember(t *testing.T) {
	if _, err := peerIdentityFromMember(nil); err == nil {
		t.Fatal("expected error for nil member")
	}
	invalidFingerprint := &membership.MemberIdentityDTO{
		MemberID:          1,
		CertificateSHA256: []byte("short"),
	}
	if _, err := peerIdentityFromMember(invalidFingerprint); err == nil {
		t.Fatal("expected error for invalid fingerprint length")
	}
	valid := &membership.MemberIdentityDTO{
		MemberID:          1,
		ClusterID:         "c",
		Address:           "a:1",
		DNSSAN:            "a",
		CertificateSHA256: bytes.Repeat([]byte{0x42}, 32),
		ReleaseVersion:    "1.0.0",
		Phase:             membership.MemberPhaseVoter,
	}
	got, err := peerIdentityFromMember(valid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.MemberID != 1 || got.Phase != tcpadapter.PeerPhaseVoter {
		t.Fatalf("unexpected identity: %+v", got)
	}
}

func TestConfiguredLocalPeerIdentity(t *testing.T) {
	var fp [32]byte
	fp[0] = 0xAA
	bootstrapCfg := clusterConfigFile{Role: "bootstrap", NodeID: 1, ClusterID: "c"}
	gotB := configuredLocalPeerIdentity(bootstrapCfg, fp)
	if gotB.Phase != tcpadapter.PeerPhaseVoter {
		t.Fatalf("bootstrap phase = %d, want Voter", gotB.Phase)
	}

	joinerCfg := clusterConfigFile{Role: "joiner", NodeID: 4, ClusterID: "c"}
	gotJ := configuredLocalPeerIdentity(joinerCfg, fp)
	if gotJ.Phase != tcpadapter.PeerPhaseStaged {
		t.Fatalf("joiner phase = %d, want Staged", gotJ.Phase)
	}
}

func TestCommittedLocalAndPeerIdentities(t *testing.T) {
	machine := membership.NewStateMachine()
	voters := []membership.MemberIdentityDTO{
		{MemberID: 1, ClusterID: "c", Address: "a:1", DNSSAN: "a", CertificateSHA256: bytes.Repeat([]byte{1}, 32), ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
		{MemberID: 2, ClusterID: "c", Address: "a:2", DNSSAN: "a", CertificateSHA256: bytes.Repeat([]byte{2}, 32), ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
		{MemberID: 3, ClusterID: "c", Address: "a:3", DNSSAN: "a", CertificateSHA256: bytes.Repeat([]byte{3}, 32), ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
	}
	if err := machine.InitializeBootstrap(3, voters); err != nil {
		t.Fatal(err)
	}

	local, found := committedLocalIdentity(machine, 1, "1.1.0")
	if !found || local.MemberID != 1 || local.ReleaseVersion != "1.1.0" {
		t.Fatalf("committedLocalIdentity(1, 1.1.0) found=%t local=%+v", found, local)
	}
	_, notFound := committedLocalIdentity(machine, 99, "1.1.0")
	if notFound {
		t.Fatal("expected 99 not found")
	}

	status := raftadapter.StatusDTO{
		VoterIDs:   []raftadapter.NodeID{1, 2, 3},
		LearnerIDs: []raftadapter.NodeID{4},
	}
	peers := committedPeerIdentities(machine, status, 1)
	if len(peers) != 2 || peers[0].MemberID != 2 || peers[1].MemberID != 3 {
		t.Fatalf("committedPeerIdentities = %+v, want peers 2 and 3", peers)
	}
}
func TestClusterPeerSetForReleaseWindow(t *testing.T) {
	tests := []struct {
		name             string
		localRelease     string
		peerReleases     []string
		wantPeerCount    int
		wantIncompatible bool
	}{
		{name: "adjacent release", localRelease: "1.1.0", peerReleases: []string{"1.2.0"}, wantPeerCount: 1},
		{name: "local is outside window", localRelease: "1.0.0", peerReleases: []string{"1.2.0"}, wantIncompatible: true},
		{name: "peer releases are outside each other window", localRelease: "1.1.0", peerReleases: []string{"1.0.0", "1.2.0"}, wantPeerCount: 2, wantIncompatible: true},
		{name: "reject incompatible peer but preserve compatible quorum peer", localRelease: "1.2.0", peerReleases: []string{"1.0.0", "1.2.0"}, wantPeerCount: 1},
		{name: "reject all incompatible peers and quarantine local node", localRelease: "1.2.0", peerReleases: []string{"1.0.0"}, wantIncompatible: true},
		{name: "empty peer set", localRelease: "1.0.0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			peers := make([]tcpadapter.PeerIdentityDTO, len(test.peerReleases))
			for index, version := range test.peerReleases {
				peers[index].ReleaseVersion = version
			}
			filtered, reason := clusterPeerSetForReleaseWindow(test.localRelease, peers)
			if (reason != "") != test.wantIncompatible {
				t.Fatalf("incompatibility reason = %q, want incompatible=%t", reason, test.wantIncompatible)
			}
			if len(filtered) != test.wantPeerCount {
				t.Fatalf("peer count = %d, want %d", len(filtered), test.wantPeerCount)
			}
			if test.wantIncompatible && !strings.HasPrefix(reason, "incompatible_state_schema: ") {
				t.Fatalf("incompatibility reason = %q, want readiness prefix", reason)
			}
		})
	}
}

func TestClusterBootstrapIdentitiesIncludesBuildFeatures(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.Features = []string{"compatibility-probe"}
	_, members, err := clusterBootstrapIdentities(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 {
		t.Fatalf("bootstrap members = %d, want 3", len(members))
	}
	for _, member := range members {
		if !reflect.DeepEqual(member.Features, config.Features) {
			t.Fatalf("member %d features = %v, want %v", member.MemberID, member.Features, config.Features)
		}
	}
	config.Features[0] = "mutated"
	if members[0].Features[0] != "compatibility-probe" {
		t.Fatalf("bootstrap member features alias config: %v", members[0].Features)
	}
}

func TestSamePeerRequest(t *testing.T) {
	fp := [32]byte{1, 2, 3}
	member := membership.MemberIdentityDTO{
		MemberID: 1, ClusterID: "c", Address: "a:1", DNSSAN: "a",
		CertificateSHA256: fp[:], ReleaseVersion: "1.0.0",
		ClusterProtocolMajor: 2, ClusterProtocolMinor: 0,
		StateSchemaMajor: 1, StateSchemaMinor: 0,
	}
	candidate := tcpadapter.PeerIdentityDTO{
		MemberID: 1, ClusterID: "c", Address: "a:1", DNSSAN: "a",
		CertificateSHA256: fp, ReleaseVersion: "1.1.0",
		ClusterProtocolMajor: 2, ClusterProtocolMinor: 0,
		StateSchemaMajor: 1, StateSchemaMinor: 0,
	}
	if !samePeerRequest(member, candidate) {
		t.Fatal("expected adjacent release match")
	}
	candidate.ReleaseVersion = "1.2.0"
	if samePeerRequest(member, candidate) {
		t.Fatal("expected incompatible release mismatch")
	}
	candidate.ReleaseVersion = "1.1.0"
	diffID := candidate
	diffID.MemberID = 2
	if samePeerRequest(member, diffID) {
		t.Fatal("expected mismatch on memberID")
	}
	diffAddr := candidate
	diffAddr.Address = "other:1"
	if samePeerRequest(member, diffAddr) {
		t.Fatal("expected mismatch on address")
	}
}

func TestClusterBatchEmpty(t *testing.T) {
	if !clusterBatchEmpty(raftadapter.BatchDTO{}) {
		t.Fatal("expected empty")
	}
	if clusterBatchEmpty(raftadapter.BatchDTO{Messages: []raftadapter.MessageDTO{{}}}) {
		t.Fatal("expected not empty with messages")
	}
	if clusterBatchEmpty(raftadapter.BatchDTO{Commands: [][]byte{[]byte("cmd")}}) {
		t.Fatal("expected not empty with commands")
	}
	if clusterBatchEmpty(raftadapter.BatchDTO{ConfigurationChanges: []raftadapter.CommittedConfigurationDTO{{}}}) {
		t.Fatal("expected not empty with config")
	}
	if clusterBatchEmpty(raftadapter.BatchDTO{SnapshotData: []byte("snap")}) {
		t.Fatal("expected not empty with snapshot")
	}
	if clusterBatchEmpty(raftadapter.BatchDTO{ReadStates: []raftadapter.ReadStateDTO{{}}}) {
		t.Fatal("expected not empty with read states")
	}
}

func TestNewClusterOperationID(t *testing.T) {
	id1, err := newClusterOperationID()
	if err != nil {
		t.Fatal(err)
	}
	id2, err := newClusterOperationID()
	if err != nil {
		t.Fatal(err)
	}
	if id1 == "" || id2 == "" || id1 == id2 {
		t.Fatalf("ids: %q, %q", id1, id2)
	}
}

func TestProposalResultWaitsForProjectionCompletion(t *testing.T) {
	node, err := raftadapter.Open(raftadapter.Config{
		NodeID: 1, PeerIDs: []raftadapter.NodeID{1, 2, 3}, Directory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	runtime := &clusterRuntime{
		node:   node,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	runtime.projectionIndex.Store(100)
	cmd1 := []byte("cmd1")
	cmd2 := []byte("cmd2")
	ch1 := make(chan clusterApplyResponse, 1)
	ch2 := make(chan clusterApplyResponse, 1)
	waiters := []clusterProposalWaiter{
		{data: cmd1, result: ch1},
		{data: cmd2, result: ch2},
	}
	res := membership.ApplyResultDTO{Index: 42, Outcome: membership.ApplyOutcomeApplied}
	runtime.stageProposalResult(cmd1, res, &waiters)
	if len(waiters) != 2 || !bytes.Equal(waiters[0].data, cmd1) {
		t.Fatalf("staged waiters = %+v", waiters)
	}
	select {
	case <-ch1:
		t.Fatal("proposal result was sent before projection refresh")
	default:
	}
	runtime.resolveAppliedProposals(&waiters)
	if len(waiters) != 1 || !bytes.Equal(waiters[0].data, cmd2) {
		t.Fatalf("resolved waiters = %+v", waiters)
	}
	gotRes := <-ch1
	if gotRes.result.Index != 42 {
		t.Fatalf("got result index = %d", gotRes.result.Index)
	}

}

func TestFailPendingProposalsOnDriverExit(t *testing.T) {
	driverErr := errors.New("raft driver failed")
	stagedResult := make(chan clusterApplyResponse, 1)
	unappliedResult := make(chan clusterApplyResponse, 1)
	waiters := []clusterProposalWaiter{
		{
			data:    []byte("staged"),
			result:  stagedResult,
			applied: true,
			response: clusterApplyResponse{
				result: membership.ApplyResultDTO{
					Index:   42,
					Outcome: membership.ApplyOutcomeApplied,
				},
			},
		},
		{data: []byte("unapplied"), result: unappliedResult},
	}

	runtime := &clusterRuntime{}
	runtime.failPendingProposals(&waiters, driverErr)
	if len(waiters) != 0 {
		t.Fatalf("remaining waiters = %d, want 0", len(waiters))
	}
	for _, testCase := range []struct {
		name   string
		result <-chan clusterApplyResponse
	}{
		{name: "staged", result: stagedResult},
		{name: "unapplied", result: unappliedResult},
	} {
		select {
		case response := <-testCase.result:
			if !errors.Is(response.err, driverErr) {
				t.Errorf("%s waiter error = %v, want %v", testCase.name, response.err, driverErr)
			}
		default:
			t.Errorf("%s waiter was not released", testCase.name)
		}
	}

	stoppedResult := make(chan clusterApplyResponse, 1)
	stoppedWaiters := []clusterProposalWaiter{{result: stoppedResult}}
	runtime.failPendingProposals(&stoppedWaiters, nil)
	if response := <-stoppedResult; response.err == nil {
		t.Fatal("driver exit without an error completed a pending proposal successfully")
	}
	queuedResult := make(chan clusterApplyResponse, 1)
	runtime.requests = make(chan clusterDriverRequest, 1)
	runtime.requests <- clusterDriverRequest{
		kind:        clusterDriverProposal,
		applyResult: queuedResult,
	}
	runtime.failQueuedDriverRequests(driverErr)
	if response := <-queuedResult; !errors.Is(response.err, driverErr) {
		t.Fatalf("queued proposal error = %v, want %v", response.err, driverErr)
	}
	if len(runtime.requests) != 0 {
		t.Fatalf("queued requests remain = %d, want 0", len(runtime.requests))
	}
}
func TestDriverRequestsRejectAfterExit(t *testing.T) {
	stopped := make(chan struct{})
	close(stopped)
	runtime := &clusterRuntime{
		requests:      make(chan clusterDriverRequest, 1),
		driverStopped: stopped,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := runtime.beginCommit(ctx, membership.CommandDTO{}, nil); !errors.Is(err, membership.ErrQuorumUnavailable) {
		t.Fatalf("beginCommit error = %v, want %v", err, membership.ErrQuorumUnavailable)
	}
	if _, err := runtime.requestReadBarrier(ctx); !errors.Is(err, raftadapter.ErrFailed) {
		t.Fatalf("requestReadBarrier error = %v, want %v", err, raftadapter.ErrFailed)
	}
	if len(runtime.requests) != 0 {
		t.Fatalf("requests after driver exit = %d, want 0", len(runtime.requests))
	}
}

func TestCommitFailureLogSummaryIsRateLimitedByStageAndClass(t *testing.T) {
	runtime := &clusterRuntime{}
	start := time.Unix(100, 0)
	first, emit := runtime.commitFailureLogSummary("read_barrier", "deadline", "durable_delivery", start)
	if !emit || first.totalCount != 1 || first.sinceLastLog != 1 {
		t.Fatalf("first failure summary = %+v emit=%t, want immediate count 1", first, emit)
	}
	if _, emit := runtime.commitFailureLogSummary("read_barrier", "deadline", "durable_delivery", start.Add(100*time.Millisecond)); emit {
		t.Fatal("same-stage/class failure was not rate limited")
	}
	next, emit := runtime.commitFailureLogSummary("read_barrier", "deadline", "durable_delivery", start.Add(time.Second))
	if !emit || next.totalCount != 3 || next.sinceLastLog != 2 ||
		!next.firstAt.Equal(start) || !next.lastAt.Equal(start.Add(time.Second)) {
		t.Fatalf("aggregated failure summary = %+v emit=%t, want count 3 and 2 since last log", next, emit)
	}
	if _, emit := runtime.commitFailureLogSummary("proposal", "deadline", "durable_delivery", start.Add(time.Second)); !emit {
		t.Fatal("different stage was suppressed")
	}
	if _, emit := runtime.commitFailureLogSummary("read_barrier", "other", "durable_delivery", start.Add(time.Second)); !emit {
		t.Fatal("different error class was suppressed")
	}
}

func TestRebuildAndRefreshRouteProjection(t *testing.T) {
	node, err := raftadapter.Open(raftadapter.Config{
		NodeID: 1, PeerIDs: []raftadapter.NodeID{1, 2, 3}, Directory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	machine := membership.NewStateMachine()
	routes := routing.NewTable()
	runtime := &clusterRuntime{
		node:    node,
		machine: machine,
		routes:  routes,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	// Apply ownership and subscription to machine
	opAcquire := "00000000-0000-4000-8000-000000000001"
	opSub := "00000000-0000-4000-8000-000000000002"
	_, err = machine.Apply(1, membership.CommandDTO{
		SchemaMajor: 1, OperationID: opAcquire,
		Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "client-1", OwnerNodeID: 1, Epoch: 1,
			Action: membership.OwnershipActionAcquire,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = machine.Apply(2, membership.CommandDTO{
		SchemaMajor: 1, OperationID: opSub,
		Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant-a", ClientID: "client-1", OwnerNodeID: 1, Epoch: 1,
			Filter: "sensor/#", QoS: 0, Action: membership.SubscriptionActionAdd,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := runtime.rebuildRouteProjection(); err != nil {
		t.Fatalf("rebuildRouteProjection: %v", err)
	}
	if runtime.projectionFailed.Load() {
		t.Fatal("expected projectionFailed=false")
	}

	// Injected failpoint
	runtime.failpoint = failpointRouteProjectionRebuild
	runtime.refreshRouteProjection()
	if !runtime.projectionFailed.Load() {
		t.Fatal("expected projectionFailed=true after failpoint")
	}

	// Clear failpoint and refresh again
	runtime.failpoint = clusterFailpointNone
	runtime.refreshRouteProjection()
	if runtime.projectionFailed.Load() {
		t.Fatal("expected projectionFailed=false after clear")
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestReplayClusterAppliedRestoresDurableDeliveryWithoutDuplicateTail(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	applied := func(index uint64, command membership.CommandDTO) raftadapter.AppliedCommandDTO {
		t.Helper()
		operationID, err := newClusterOperationID()
		if err != nil {
			t.Fatal(err)
		}
		command.SchemaMajor = membership.SchemaMajor
		command.SchemaMinor = membership.SchemaMinor
		command.OperationID = operationID
		data, err := encodeMembershipCommand(command)
		if err != nil {
			t.Fatalf("encode durable replay command at index %d: %v", index, err)
		}
		return raftadapter.AppliedCommandDTO{Index: index, Data: data}
	}
	commands := []raftadapter.AppliedCommandDTO{
		applied(5, membership.CommandDTO{Delivery: &membership.DeliveryChangeDTO{
			TenantID: "tenant-a", ClientID: "publisher", Action: membership.DeliveryActionPublish,
			ProposedAtUnixMillis: 1_700_000_000_003, Topic: "events/replay", Payload: []byte("durable"),
			PublishQoS: 2, Targets: []membership.DeliveryTargetDTO{{ClientID: "durable-client"}},
		}}),
		applied(4, membership.CommandDTO{Delivery: &membership.DeliveryChangeDTO{
			TenantID: "tenant-a", ClientID: "durable-client", OwnerNodeID: config.NodeID, Epoch: 1,
			Action: membership.DeliveryActionAttach, ProposedAtUnixMillis: 1_700_000_000_002,
		}}),
		applied(3, membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant-a", ClientID: "durable-client", OwnerNodeID: config.NodeID, Epoch: 1,
			Filter: "events/#", QoS: 2, Action: membership.SubscriptionActionAdd,
		}}),
		applied(2, membership.CommandDTO{Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "durable-client", OwnerNodeID: config.NodeID, Epoch: 1,
			Action: membership.OwnershipActionAcquire,
		}}),
	}
	configurationOperationID, err := newClusterOperationID()
	if err != nil {
		t.Fatal(err)
	}
	_, bootstrapMembers, err := clusterBootstrapIdentities(config)
	if err != nil {
		t.Fatal(err)
	}
	learner := bootstrapMembers[0]
	learner.MemberID = 4
	learner.Address = "127.0.0.1:19004"
	learner.DNSSAN = "node-4"
	learner.Phase = membership.MemberPhaseStaged
	learner.OperationID = configurationOperationID
	stage := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: configurationOperationID,
		Membership: &membership.MembershipRecordDTO{
			OperationID: configurationOperationID, Action: membership.MembershipActionStageJoin,
			IdempotencyKey: "join-4", Member: &learner,
		},
	}
	stageData, err := encodeMembershipCommand(stage)
	if err != nil {
		t.Fatalf("encode staged learner command: %v", err)
	}
	commands = append(commands, raftadapter.AppliedCommandDTO{Index: 6, Data: stageData})
	configurations := []raftadapter.CommittedConfigurationDTO{{
		Index: 7, OperationID: configurationOperationID, MemberID: 4, Action: raftadapter.MembershipAddLearner,
	}}

	if err := replayClusterApplied(machine, commands, configurations); err != nil {
		t.Fatalf("replay committed delivery tail: %v", err)
	}
	state, exists := machine.DeliverySession("tenant-a", "durable-client")
	if !exists || len(state.Messages) != 1 || state.Messages[0].Sequence != 1 ||
		state.Messages[0].Phase != membership.DeliveryPhaseQueued || state.Messages[0].QoS != 2 ||
		string(state.Messages[0].Payload) != "durable" {
		t.Fatalf("replayed delivery state=%+v exists=%t", state, exists)
	}
	var replayedLearner bool
	for _, member := range machine.Members() {
		if member.MemberID == 4 && member.Phase == membership.MemberPhaseLearner {
			replayedLearner = true
		}
	}
	if !replayedLearner || machine.LastAppliedIndex() != 7 {
		t.Fatalf("replayed membership learner=%t appliedIndex=%d", replayedLearner, machine.LastAppliedIndex())
	}
	if err := replayClusterApplied(machine, commands, configurations); err != nil {
		t.Fatalf("replay already-applied delivery tail: %v", err)
	}
	state, exists = machine.DeliverySession("tenant-a", "durable-client")
	if !exists || len(state.Messages) != 1 || state.Messages[0].Sequence != 1 {
		t.Fatalf("replayed tail duplicated durable delivery: %+v exists=%t", state, exists)
	}
	if err := replayClusterApplied(machine, append(commands, commands[0]), nil); err == nil {
		t.Fatal("duplicate committed delivery index was accepted")
	}
	if err := replayClusterApplied(machine, nil, []raftadapter.CommittedConfigurationDTO{{
		Index: 8, OperationID: configurationOperationID, MemberID: 5, Action: raftadapter.MembershipChangeAction(99),
	}}); err == nil {
		t.Fatal("unknown committed configuration action was accepted")
	}
	if err := replayClusterApplied(machine, []raftadapter.AppliedCommandDTO{{Index: 8, Data: []byte{0xff}}}, nil); err == nil {
		t.Fatal("malformed committed command was accepted")
	}
	if machine.LastAppliedIndex() != 7 {
		t.Fatalf("rejected replay advanced applied index to %d", machine.LastAppliedIndex())
	}
}

// Traceability: CLU-05/CLU-06; docs/specs/cluster-session-and-delivery.md §2.1, §2.2, §11.14.
func TestClusterSnapshotAndWALTailReplayRestoresSessionAggregateAndRouteProjection(t *testing.T) {
	config := validBootstrapClusterConfig()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	applied := func(index uint64, command membership.CommandDTO) raftadapter.AppliedCommandDTO {
		t.Helper()
		operationID, err := newClusterOperationID()
		if err != nil {
			t.Fatal(err)
		}
		command.SchemaMajor = membership.SchemaMajor
		command.SchemaMinor = membership.SchemaMinor
		command.OperationID = operationID
		data, err := encodeMembershipCommand(command)
		if err != nil {
			t.Fatalf("encode replay command at index %d: %v", index, err)
		}
		return raftadapter.AppliedCommandDTO{Index: index, Data: data}
	}
	commands := []raftadapter.AppliedCommandDTO{
		applied(2, membership.CommandDTO{Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 1, Epoch: 1,
			Action: membership.OwnershipActionAcquire,
		}}),
		applied(3, membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 1, Epoch: 1,
			Filter: "opts/#", QoS: 2, NoLocal: true, RetainAsPublished: true, RetainHandling: 2,
			Action: membership.SubscriptionActionAdd,
		}}),
		applied(4, membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 1, Epoch: 1,
			Filter: "plain/+/x", QoS: 1, Action: membership.SubscriptionActionAdd,
		}}),
		applied(5, membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 1, Epoch: 1,
			Filter: "gone/#", QoS: 1, Action: membership.SubscriptionActionAdd,
		}}),
		applied(6, membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 1, Epoch: 1,
			Filter: "gone/#", Action: membership.SubscriptionActionRemove,
		}}),
		applied(7, membership.CommandDTO{Delivery: &membership.DeliveryChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 1, Epoch: 1,
			Action: membership.DeliveryActionAttach, ProposedAtUnixMillis: 1_700_000_000_001,
		}}),
		applied(8, membership.CommandDTO{Delivery: &membership.DeliveryChangeDTO{
			TenantID: "tenant-a", ClientID: "publisher", Action: membership.DeliveryActionPublish,
			ProposedAtUnixMillis: 1_700_000_000_002, Topic: "opts/a", Payload: []byte("one"), PublishQoS: 1,
			Targets: []membership.DeliveryTargetDTO{{ClientID: "sub-a"}},
		}}),
		applied(9, membership.CommandDTO{Delivery: &membership.DeliveryChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 1, Epoch: 1,
			Action: membership.DeliveryActionSend, ProposedAtUnixMillis: 1_700_000_000_003,
			Sequence: 1, PacketID: 7,
		}}),
		applied(10, membership.CommandDTO{Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", ExpectedEpoch: 1, OwnerNodeID: 2, Epoch: 2,
			Action: membership.OwnershipActionAcquire,
		}}),
		applied(11, membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 1, Epoch: 1,
			Filter: "stale/#", QoS: 1, Action: membership.SubscriptionActionAdd,
		}}),
		applied(12, membership.CommandDTO{Delivery: &membership.DeliveryChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 1, Epoch: 1,
			Action: membership.DeliveryActionSend, ProposedAtUnixMillis: 1_700_000_000_004,
			Sequence: 1, PacketID: 9,
		}}),
		applied(13, membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 2, Epoch: 2,
			Filter: "late/+", QoS: 1, Action: membership.SubscriptionActionAdd,
		}}),
	}

	reference := retainedTestMachine(t, config, config.ReleaseVersion)
	if err := replayClusterApplied(reference, commands, nil); err != nil {
		t.Fatalf("replay reference aggregate: %v", err)
	}
	if err := replayClusterApplied(machine, commands[:8], nil); err != nil {
		t.Fatalf("replay through snapshot boundary: %v", err)
	}
	snapshot, err := machine.Snapshot(9)
	if err != nil {
		t.Fatalf("snapshot at index 9: %v", err)
	}
	snapshotData, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("encode membership snapshot: %v", err)
	}
	decoded, err := decodeMembershipSnapshot(snapshotData)
	if err != nil {
		t.Fatalf("decode membership snapshot: %v", err)
	}
	restored := membership.NewStateMachine()
	if err := restored.Restore(decoded); err != nil {
		t.Fatalf("restore membership snapshot: %v", err)
	}
	if err := replayClusterApplied(restored, commands, nil); err != nil {
		t.Fatalf("replay WAL tail after snapshot: %v", err)
	}

	subscriptions := restored.Subscriptions()
	if !reflect.DeepEqual(subscriptions, reference.Subscriptions()) {
		t.Fatalf("replayed subscriptions = %+v, want reference %+v", subscriptions, reference.Subscriptions())
	}
	wantSubscriptions := []membership.SubscriptionStateDTO{
		{TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 2, Epoch: 2, Filter: "late/+", QoS: 1},
		{TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 2, Epoch: 2, Filter: "opts/#", QoS: 2, NoLocal: true, RetainAsPublished: true, RetainHandling: 2},
		{TenantID: "tenant-a", ClientID: "sub-a", OwnerNodeID: 2, Epoch: 2, Filter: "plain/+/x", QoS: 1},
	}
	if !reflect.DeepEqual(subscriptions, wantSubscriptions) {
		t.Fatalf("replayed subscriptions = %+v, want %+v", subscriptions, wantSubscriptions)
	}
	delivery, exists := restored.DeliverySession("tenant-a", "sub-a")
	referenceDelivery, referenceExists := reference.DeliverySession("tenant-a", "sub-a")
	if !exists || !referenceExists || !reflect.DeepEqual(delivery, referenceDelivery) ||
		len(delivery.Messages) != 1 || delivery.Messages[0].PacketID != 7 || delivery.Messages[0].PacketID == 9 {
		t.Fatalf("replayed delivery = %+v exists=%t, want reference %+v exists=%t with committed packet ID 7",
			delivery, exists, referenceDelivery, referenceExists)
	}
	var foundOwnership bool
	for _, ownership := range restored.Ownerships() {
		if ownership.TenantID == "tenant-a" && ownership.ClientID == "sub-a" {
			foundOwnership = true
			if ownership.OwnerNodeID != 2 || ownership.Epoch != 2 {
				t.Fatalf("replayed ownership = %+v, want owner 2 epoch 2", ownership)
			}
		}
	}
	if !foundOwnership {
		t.Fatal("replayed ownership for tenant-a/sub-a is missing")
	}

	node, err := raftadapter.Open(raftadapter.Config{
		NodeID: 1, PeerIDs: []raftadapter.NodeID{1, 2, 3}, Directory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	routes := routing.NewTable()
	runtime := &clusterRuntime{
		node: node, machine: restored, routes: routes,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := runtime.rebuildRouteProjection(); err != nil {
		t.Fatalf("rebuildRouteProjection: %v", err)
	}
	sessionID, err := clusterSessionID("tenant-a", "sub-a")
	if err != nil {
		t.Fatalf("clusterSessionID: %v", err)
	}
	got, found := routes.Lookup(sessionID, "opts/#")
	want := mqtt.Subscription{
		Filter: "opts/#", QoS: mqtt.QoSExactlyOnce, NoLocal: true,
		RetainAsPublished: true, RetainHandling: mqtt.DoNotSendRetained,
	}
	if !found || got != want {
		t.Fatalf("projected opts/# = %+v found=%t, want %+v", got, found, want)
	}
	if _, found := routes.Lookup(sessionID, "late/+"); !found {
		t.Fatal("projected late/+ subscription is missing")
	}
	for _, filter := range []string{"gone/#", "stale/#"} {
		if _, found := routes.Lookup(sessionID, filter); found {
			t.Fatalf("projected stale/removed subscription %q is present", filter)
		}
	}
}

// Traceability: MQTT-01, PRD MQTT-10; docs/specs/mqtt-protocol-and-session.md §1.
func TestClusterMQTTListenersBindConfiguredEndpointsAndClosePartialOpen(t *testing.T) {
	listeners, err := clusterMQTTListeners(clusterConfigFile{}, nil, slog.Default())
	if err != nil || len(listeners) != 0 {
		t.Fatalf("empty cluster MQTT listeners=%v err=%v", listeners, err)
	}

	tcpAddress := clusterRuntimeTestFreeAddress(t)
	invalidTLS := clusterConfigFile{
		MQTT: clusterMQTT{
			TCPListenAddress: tcpAddress, TLSListenAddress: clusterRuntimeTestFreeAddress(t),
			TLS: &clusterMQTTTLS{CertFile: filepath.Join(t.TempDir(), "missing.crt"), KeyFile: filepath.Join(t.TempDir(), "missing.key"), ClientAuth: "none", MinVersion: "1.2"},
		},
	}
	if listeners, err := clusterMQTTListeners(invalidTLS, nil, slog.Default()); err == nil || listeners != nil {
		t.Fatalf("invalid TLS listener result=%v err=%v, want closed partial TCP listener and error", listeners, err)
	}
	rebound, err := net.Listen("tcp", tcpAddress)
	if err != nil {
		t.Fatalf("TCP listener remained open after TLS setup failure: %v", err)
	}
	if err := rebound.Close(); err != nil {
		t.Fatal(err)
	}

	missingTLSAddress := clusterRuntimeTestFreeAddress(t)
	missingTLSConfig := clusterConfigFile{
		MQTT: clusterMQTT{
			TCPListenAddress: clusterRuntimeTestFreeAddress(t),
			TLSListenAddress: missingTLSAddress,
		},
	}
	missingTCPAddress := missingTLSConfig.MQTT.TCPListenAddress
	if listeners, err := clusterMQTTListeners(missingTLSConfig, nil, slog.Default()); err == nil ||
		err.Error() != "cluster MQTT TLS listener requires mqtt.tls" || listeners != nil {
		t.Fatalf("missing mqtt.tls listener result=%v err=%v", listeners, err)
	}
	rebound, err = net.Listen("tcp", missingTCPAddress)
	if err != nil {
		t.Fatalf("TCP listener remained open after missing mqtt.tls: %v", err)
	}
	if err := rebound.Close(); err != nil {
		t.Fatal(err)
	}
	ca, caKey, _ := newClusterRuntimeTestCA(t)
	certPEM, keyPEM, _ := issueClusterRuntimeTestCertificate(t, ca, caKey, "mqtt.example.test", 11)
	certPath := filepath.Join(t.TempDir(), "mqtt.crt")
	keyPath := filepath.Join(t.TempDir(), "mqtt.key")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	config := clusterConfigFile{
		MQTT: clusterMQTT{
			TCPListenAddress: clusterRuntimeTestFreeAddress(t),
			TLSListenAddress: clusterRuntimeTestFreeAddress(t),
			TLS:              &clusterMQTTTLS{CertFile: certPath, KeyFile: keyPath, ClientAuth: "none", MinVersion: "1.2"},
			WebSocket: &clusterMQTTWebSocket{
				ListenAddress: clusterRuntimeTestFreeAddress(t), TLSListenAddress: clusterRuntimeTestFreeAddress(t),
			},
		},
	}
	listeners, err = clusterMQTTListeners(config, nil, slog.Default())
	if err != nil || len(listeners) != 4 {
		t.Fatalf("configured TCP/TLS/WS/WSS listeners=%v err=%v, want four bound listeners", listeners, err)
	}
	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}

	wssConflict, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conflictAddress := wssConflict.Addr().String()
	partialConfig := clusterConfigFile{
		MQTT: clusterMQTT{
			TCPListenAddress: clusterRuntimeTestFreeAddress(t),
			WebSocket: &clusterMQTTWebSocket{
				ListenAddress: clusterRuntimeTestFreeAddress(t), TLSListenAddress: conflictAddress,
			},
			TLS: &clusterMQTTTLS{CertFile: certPath, KeyFile: keyPath, ClientAuth: "none", MinVersion: "1.2"},
		},
	}
	tcpAddress = partialConfig.MQTT.TCPListenAddress
	wsAddress := partialConfig.MQTT.WebSocket.ListenAddress
	if listeners, err := clusterMQTTListeners(partialConfig, nil, slog.Default()); err == nil || listeners != nil {
		_ = wssConflict.Close()
		t.Fatalf("WSS conflict result=%v err=%v, want closed partial TCP and WS listeners", listeners, err)
	}
	if err := wssConflict.Close(); err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{tcpAddress, wsAddress} {
		rebound, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("listener on %s remained open after WSS setup failure: %v", address, err)
		}
		if err := rebound.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Traceability: SEC-03, PRD MQTT-10; docs/specs/configuration-and-runtime-options.md §6; ADR-0052.
func TestClusterMQTTSecurityValidatesTLSClientAuthBindings(t *testing.T) {
	securityPath := filepath.Join(t.TempDir(), "security.json")
	if err := os.WriteFile(securityPath, []byte(validMTLSSecurityJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		clientAuth string
		wantError  string
		wss        bool
	}{
		{clientAuth: "none", wantError: "mqtt.tls.clientAuth none cannot be used with mTLS identity bindings"},
		{clientAuth: "require"},
		{clientAuth: "request", wantError: "mqtt.tls.clientAuth request requires JWT or password authentication configuration when mTLS identities are enabled"},
		{clientAuth: "require", wss: true},
		{clientAuth: "none", wantError: "mqtt.tls.clientAuth none cannot be used with mTLS identity bindings", wss: true},
	} {
		name := test.clientAuth
		if test.wss {
			name += " WSS"
		}
		t.Run(name, func(t *testing.T) {
			mqttConfig := clusterMQTT{SecurityConfigFile: securityPath, TLS: &clusterMQTTTLS{ClientAuth: test.clientAuth}}
			if test.wss {
				mqttConfig.WebSocket = &clusterMQTTWebSocket{TLSListenAddress: "127.0.0.1:0"}
			} else {
				mqttConfig.TLSListenAddress = "127.0.0.1:0"
			}
			config := clusterConfigFile{MQTT: mqttConfig}
			bundle, _, mtlsProvider, _, err := clusterMQTTSecurity(config)
			if test.wantError != "" {
				if err == nil || err.Error() != test.wantError {
					t.Fatalf("clusterMQTTSecurity error = %v, want %q", err, test.wantError)
				}
				return
			}
			if err != nil || bundle == nil || mtlsProvider == nil {
				t.Fatalf("clusterMQTTSecurity result = bundle:%v provider:%v err:%v, want configured mTLS provider", bundle, mtlsProvider, err)
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/mqtt-protocol-and-session.md §3.1.
func TestClusterCommitReadProofPreservesSubmissionOrderAndCancellation(t *testing.T) {
	var nodes [3]*raftadapter.Node
	for index := range nodes {
		node, err := raftadapter.Open(raftadapter.Config{
			NodeID: raftadapter.NodeID(index + 1), PeerIDs: []raftadapter.NodeID{1, 2, 3}, Directory: t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[index] = node
		t.Cleanup(func() { node.Close() })
	}
	var proofStates []raftadapter.ReadStateDTO
	pump := func() [][]byte {
		var commands [][]byte
		for range 64 {
			var messages []raftadapter.MessageDTO
			active := false
			for index, node := range nodes {
				batch, err := processReadyForTest(t, node)
				if err != nil {
					t.Fatal(err)
				}
				active = active || !clusterBatchEmpty(batch)
				messages = append(messages, batch.Messages...)
				if index == 0 {
					proofStates = append(proofStates, batch.ReadStates...)
					commands = append(commands, batch.Commands...)
				}
			}
			if !active {
				return commands
			}
			for _, message := range messages {
				if _, err := nodes[message.To-1].Step(message); err != nil {
					t.Fatal(err)
				}
			}
		}
		t.Fatal("Raft did not settle in 64 rounds")
		return nil
	}
	pump()
	node := nodes[0]
	if err := node.Campaign(); err != nil {
		t.Fatal(err)
	}
	pump()
	runtime := &clusterRuntime{node: node, storage: newClusterStorageMonitor(time.Now, nil)}
	makePending := func(key, payload string, ctx context.Context) *clusterPendingCommit {
		return &clusterPendingCommit{
			runtime: runtime, context: ctx, readContext: ctx, contextKey: key, data: []byte(payload),
			staged: make(chan clusterCommitStage, 1), result: make(chan clusterApplyResponse, 1),
		}
	}
	first := makePending("first", "first-change", context.Background())
	second := makePending("second", "second-change", context.Background())
	second.readConfirmed = true
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancelled := makePending("cancelled", "must-not-commit", cancelledCtx)
	cancel()
	runtime.pendingCommits = []*clusterPendingCommit{first, second, cancelled}
	firstWaiter := clusterReadWaiter{ctx: context.Background(), commit: first}
	if err := runtime.issueReadProof("first", &firstWaiter, node.Status()); err != nil {
		t.Fatal(err)
	}
	reads := map[string]clusterReadWaiter{"first": firstWaiter}
	var proposals []clusterProposalWaiter
	if runtime.resolveCommitReads(reads, &proposals) || len(proposals) != 0 {
		t.Fatal("later fresh proof caused a proposal before the first proof")
	}
	select {
	case stage := <-second.staged:
		t.Fatalf("second change overtook the missing first proof: %+v", stage)
	default:
	}
	applied := node.Status().AppliedIndex
	pump()
	runtime.projectionIndex.Store(applied - 1)
	deferred := runtime.completeReadStates(proofStates, reads)
	if runtime.resolveCommitReads(reads, &proposals) || len(deferred) != 1 {
		t.Fatal("read proof beyond local apply/projection permitted submission")
	}
	runtime.projectionIndex.Store(applied)
	if remaining := runtime.completeReadStates(deferred, reads); len(remaining) != 0 {
		t.Fatal("applied fresh proof remained pending")
	}
	if !runtime.resolveCommitReads(reads, &proposals) {
		t.Fatal("completed fresh proofs did not submit changes")
	}
	if stage := <-cancelled.staged; !errors.Is(stage.err, context.Canceled) {
		t.Fatalf("cancelled fresh proof result = %+v", stage)
	}
	commands := pump()
	if len(commands) != 2 || string(commands[0]) != "first-change" || string(commands[1]) != "second-change" {
		t.Fatalf("durably applied command order = %q, want first then second without cancelled command", commands)
	}
}

// Traceability: CLU-09; docs/specs/cluster-session-and-delivery.md §4.4, §11.27.
func TestClusterMembershipRemovalWaitsForReplicationFactor(t *testing.T) {
	for _, ownsSession := range []bool{false, true} {
		name := "without-session"
		if ownsSession {
			name = "with-session"
		}
		t.Run(name, func(t *testing.T) {
			runtimes := newReadAdmissionTestCluster(t)
			runtime := runtimes[0]
			if status := runtime.node.Status(); status.Role != raftadapter.RoleLeader {
				t.Fatalf("fixture has no leader: %+v", status)
			}
			runtime.machine = retainedTestMachine(t, runtime.config, runtime.config.ReleaseVersion)
			runtime.initialized.Store(true)
			runtime.handoffProposed = make(map[string]uint64)
			index := runtime.machine.LastAppliedIndex() + 1
			if ownsSession {
				result, err := runtime.machine.Apply(index, membership.CommandDTO{
					SchemaMajor: membership.SchemaMajor,
					OperationID: "00000000-0000-4000-8000-000000000010",
					Ownership: &membership.OwnershipChangeDTO{
						TenantID: "tenant-a", ClientID: "client-a",
						OwnerNodeID: 3, Epoch: 1, Action: membership.OwnershipActionAcquire,
					},
				})
				if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
					t.Fatalf("acquire leaving member session: result=%+v err=%v", result, err)
				}
				index++
			}
			// Model a committed BeginRemove from an older proposer or a race after the service check.
			const operationID = "00000000-0000-4000-8000-000000000006"
			member := runtime.machine.Members()[2]
			member.Phase = membership.MemberPhaseLeaving
			member.OperationID = operationID
			result, err := runtime.machine.Apply(index, membership.CommandDTO{
				SchemaMajor: membership.SchemaMajor, OperationID: operationID,
				Membership: &membership.MembershipRecordDTO{
					OperationID: operationID, Action: membership.MembershipActionBeginRemove,
					IdempotencyKey: "remove-3", Member: &member, TargetMemberID: 3,
				},
			})
			if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
				t.Fatalf("commit removal before leader recheck: result=%+v err=%v", result, err)
			}
			commitIndex := runtime.node.Status().CommitIndex
			for range 2 {
				if err := runtime.advanceMembership(); err != nil {
					t.Fatalf("advance blocked removal: %v", err)
				}
			}
			// Deliver real Raft output so a wrongly proposed transfer or ConfChange becomes observable.
			for round := 0; ; round++ {
				if round == 64 {
					t.Fatal("Raft did not settle after removal recheck")
				}
				var messages []raftadapter.MessageDTO
				for _, peer := range runtimes {
					batch, err := processReadyForTest(t, peer.node)
					if err != nil {
						t.Fatal(err)
					}
					if len(batch.Commands) != 0 || len(batch.ConfigurationChanges) != 0 {
						t.Fatalf("blocked removal committed transfer or configuration: %+v", batch)
					}
					messages = append(messages, batch.Messages...)
				}
				if len(messages) == 0 {
					break
				}
				for _, message := range messages {
					if _, err := runtimes[message.To-1].node.Step(message); err != nil {
						t.Fatal(err)
					}
				}
			}
			operation, exists := runtime.machine.GetOperation(operationID)
			if !exists || operation.Phase != membership.OperationPhaseRemoving {
				t.Fatalf("blocked removal operation=%+v exists=%t", operation, exists)
			}
			status := runtime.node.Status()
			if status.CommitIndex != commitIndex || !reflect.DeepEqual(status.VoterIDs, []raftadapter.NodeID{1, 2, 3}) {
				t.Fatalf("blocked removal changed Raft state: %+v, prior commit=%d", status, commitIndex)
			}
			if ownsSession {
				owner, exists := runtime.machine.GetOwnership("tenant-a", "client-a")
				if !exists || owner.OwnerNodeID != 3 || owner.Epoch != 1 {
					t.Fatalf("blocked removal changed session ownership: %+v exists=%t", owner, exists)
				}
			}
			t.Log("leader recheck: REMOVING retained, three voters unchanged, no transfer or ConfChange committed")
		})
	}
}
func TestClusterMembershipRemovalOfLocalLeaderTransfersLeadership(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	leader := runtimes[0]
	leader.machine = readAdmissionTestMachine(t, runtimes)
	leader.initialized.Store(true)
	leader.handoffProposed = map[string]uint64{}
	const operationID = "00000000-0000-4000-8000-000000000016"
	member := leader.machine.Members()[0]
	member.Phase = membership.MemberPhaseLeaving
	member.OperationID = operationID
	result, err := leader.machine.Apply(leader.machine.LastAppliedIndex()+1, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: operationID,
		Membership: &membership.MembershipRecordDTO{
			OperationID: operationID, Action: membership.MembershipActionBeginRemove,
			IdempotencyKey: "remove-1", Member: &member, TargetMemberID: 1,
		},
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("apply BeginRemove for local leader: result=%+v err=%v, want Applied", result, err)
	}
	if err := leader.advanceMembership(); err != nil || leader.leadershipTransferTo != 0 {
		t.Fatalf("advanceMembership before Serve: err=%v transferTo=%d, want nil/0", err, leader.leadershipTransferTo)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveResults := make(chan error, 2)
	for _, runtime := range runtimes[:2] {
		runtime := runtime
		go func() { serveResults <- runtime.transport.Serve(ctx) }()
	}
	t.Cleanup(func() {
		cancel()
		for range 2 {
			if err := <-serveResults; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("transport Serve during cleanup: %v", err)
			}
		}
	})
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var peerStatus tcpadapter.PeerTransportStatusDTO
	var peerStatusErr error
	for {
		peerStatus, peerStatusErr = leader.transport.Status(2)
		if peerStatusErr == nil && peerStatus.Connected {
			break
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("leader transport status for peer 2 = %+v, err=%v; want Connected before timeout", peerStatus, peerStatusErr)
		case <-ticker.C:
		}
	}
	status := leader.node.Status()
	if err := leader.advanceMembership(); err != nil {
		t.Fatalf("advanceMembership after Serve: %v", err)
	}
	if leader.leadershipTransferTo != 2 || leader.leadershipTransferTerm != status.Term || leader.leadershipTransferTicks != 0 {
		t.Fatalf("leadership transfer state = to:%d term:%d ticks:%d, want to:2 term:%d ticks:0",
			leader.leadershipTransferTo, leader.leadershipTransferTerm, leader.leadershipTransferTicks, status.Term)
	}
	for round := 0; ; round++ {
		if round == 64 {
			t.Fatal("Raft did not settle after local leader removal transfer")
		}
		var messages []raftadapter.MessageDTO
		for _, runtime := range runtimes {
			batch, err := processReadyForTest(t, runtime.node)
			if err != nil {
				t.Fatalf("process Ready for node %d: %v", runtime.config.NodeID, err)
			}
			messages = append(messages, batch.Messages...)
		}
		if len(messages) == 0 {
			break
		}
		for _, message := range messages {
			if _, err := runtimes[message.To-1].node.Step(message); err != nil {
				t.Fatalf("step Raft message %+v: %v", message, err)
			}
		}
	}
	if got := runtimes[1].node.Status().Role; got != raftadapter.RoleLeader {
		t.Fatalf("node 2 role after transfer = %q, want %q", got, raftadapter.RoleLeader)
	}
	if err := leader.reconcileCommittedPeers(); err != nil {
		t.Fatalf("reconcile committed peers after transfer: %v", err)
	}
	identity := leader.transport.LocalIdentity()
	if identity.Phase != tcpadapter.PeerPhaseLeaving || identity.OperationID != operationID {
		t.Fatalf("local identity after reconciliation = %+v, want LEAVING operation %s", identity, operationID)
	}
}

func TestClusterProposeMembershipChangeIgnoresPendingChangeAndFollower(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	change := raftadapter.MembershipChangeDTO{
		OperationID: "00000000-0000-4000-8000-000000000021", MemberID: 4, Action: raftadapter.MembershipAddLearner,
	}
	if err := runtimes[1].node.ProposeMembershipChange(change); !errors.Is(err, raftadapter.ErrNotLeader) {
		t.Fatalf("follower ProposeMembershipChange(%+v) error = %v, want %v", change, err, raftadapter.ErrNotLeader)
	}
	if err := runtimes[1].proposeMembershipChange(change); err != nil {
		t.Fatalf("follower proposeMembershipChange(%+v) = %v, want nil", change, err)
	}
	if err := runtimes[0].proposeMembershipChange(change); err != nil {
		t.Fatalf("leader proposeMembershipChange(%+v) = %v, want nil", change, err)
	}
	second := change
	second.OperationID = "00000000-0000-4000-8000-000000000022"
	if err := runtimes[0].node.ProposeMembershipChange(second); !errors.Is(err, raftadapter.ErrMembershipChangeInProgress) {
		t.Fatalf("second ProposeMembershipChange(%+v) error = %v, want %v", second, err, raftadapter.ErrMembershipChangeInProgress)
	}
	if err := runtimes[0].proposeMembershipChange(second); err != nil {
		t.Fatalf("leader proposeMembershipChange(%+v) = %v, want nil", second, err)
	}
}

func TestReconcileCommittedPeersSkipsRetiredLocalIdentityOutsideRaft(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	machine := readAdmissionTestMachine(t, runtimes)
	const operationID = "00000000-0000-4000-8000-000000000017"
	member := machine.Members()[2]
	member.Phase = membership.MemberPhaseLeaving
	member.OperationID = operationID
	result, err := machine.Apply(machine.LastAppliedIndex()+1, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: operationID,
		Membership: &membership.MembershipRecordDTO{
			OperationID: operationID, Action: membership.MembershipActionBeginRemove,
			IdempotencyKey: "remove-3", Member: &member, TargetMemberID: 3,
		},
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("apply BeginRemove for member 3: result=%+v err=%v, want Applied", result, err)
	}
	result, err = machine.ApplyConfiguration(machine.LastAppliedIndex()+1, operationID, membership.ConfigurationActionRemoveMember, 3)
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("apply RemoveMember configuration: result=%+v err=%v, want Applied", result, err)
	}
	retired := machine.Members()[2]
	if retired.Phase != membership.MemberPhaseRetired {
		t.Fatalf("member 3 after removal configuration = %+v, want Retired", retired)
	}
	identity := runtimes[2].transport.LocalIdentity()
	fresh, err := raftadapter.Open(raftadapter.Config{
		NodeID: 3, DynamicMembership: true, ClusterID: "read-admission",
		CertificateSHA256: identity.CertificateSHA256, Directory: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open fresh unbootstrapped Raft node: %v", err)
	}
	t.Cleanup(func() {
		if err := fresh.Close(); err != nil {
			t.Errorf("close fresh Raft node: %v", err)
		}
	})
	runtime := &clusterRuntime{
		config: runtimes[2].config, node: fresh, transport: runtimes[2].transport, machine: machine,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	before := runtimes[2].transport.LocalIdentity()
	if err := runtime.reconcileCommittedPeers(); err != nil {
		t.Fatalf("reconcile retired local identity: %v", err)
	}
	if got := runtime.transport.LocalIdentity(); got != before {
		t.Fatalf("transport identity after reconciliation = %+v, want unchanged %+v", got, before)
	}
	if runtime.localIdentity != (tcpadapter.PeerIdentityDTO{}) {
		t.Fatalf("runtime local identity after reconciliation = %+v, want zero value", runtime.localIdentity)
	}
}

func TestClusterAdmitPeerRejectsWhenNotLeader(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	follower := runtimes[1]
	follower.machine = readAdmissionTestMachine(t, runtimes)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := follower.admitPeer(ctx, tcpadapter.PeerIdentityDTO{MemberID: 4})
		result <- err
	}()
	select {
	case request := <-follower.requests:
		if request.kind != clusterDriverRead {
			t.Fatalf("admitPeer driver request = %+v, want clusterDriverRead", request)
		}
		request.readResult <- clusterReadResponse{index: 1}
	case <-ctx.Done():
		t.Fatalf("admitPeer did not request read barrier before timeout: %v", ctx.Err())
	}
	select {
	case err := <-result:
		if !errors.Is(err, raftadapter.ErrNotLeader) {
			t.Fatalf("admitPeer error = %v, want %v", err, raftadapter.ErrNotLeader)
		}
	case <-ctx.Done():
		t.Fatalf("admitPeer result not returned before timeout: %v", ctx.Err())
	}
}

func TestClusterGetOperationReportsReadBarrierFailure(t *testing.T) {
	runtime := clusterReleaseProfileTestRuntime(t, validBootstrapClusterConfig())
	runtime.driverFailed.Store(true)
	operation, found, err := runtime.GetOperation(context.Background(), "00000000-0000-4000-8000-000000000001")
	if !errors.Is(err, membership.ErrQuorumUnavailable) {
		t.Fatalf("GetOperation error = %v, want %v", err, membership.ErrQuorumUnavailable)
	}
	if found || operation != (membership.MembershipOperationDTO{}) {
		t.Fatalf("GetOperation result = operation:%+v found:%t, want zero/false", operation, found)
	}
	if got := len(runtime.requests); got != 0 {
		t.Fatalf("queued driver requests = %d, want 0", got)
	}
}
