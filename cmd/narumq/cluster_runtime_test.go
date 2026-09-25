package main

import (
	"bytes"
	"io"
	"log/slog"
	"reflect"
	"testing"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/membership"
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
		initialized:       true,
		projectionFailed:  false,
		bootstrapped:      true,
		dynamicMembership: true,
		localVoter:        true,
		localPhaseVoter:   true,
		projectionIndex:   10,
		appliedIndex:      10,
	}

	cases := []struct {
		name          string
		mutate        func(*clusterReadinessSnapshot)
		barrier       uint64
		wantAdmits    bool
		wantSatisfied bool
	}{
		{
			name: "not initialized",
			mutate: func(s *clusterReadinessSnapshot) {
				s.initialized = false
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
		},
		{
			name: "projection failed with equal indexes",
			mutate: func(s *clusterReadinessSnapshot) {
				s.projectionFailed = true
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
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
		},
		{
			name: "not bootstrapped",
			mutate: func(s *clusterReadinessSnapshot) {
				s.bootstrapped = false
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
		},
		{
			name: "learner (localVoter=false)",
			mutate: func(s *clusterReadinessSnapshot) {
				s.localVoter = false
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
		},
		{
			name: "LEAVING (localPhaseVoter=false)",
			mutate: func(s *clusterReadinessSnapshot) {
				s.localPhaseVoter = false
			},
			barrier:       10,
			wantAdmits:    false,
			wantSatisfied: false,
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
		},
		{
			name:          "all satisfied",
			mutate:        func(s *clusterReadinessSnapshot) {},
			barrier:       10,
			wantAdmits:    true,
			wantSatisfied: true,
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
		})
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
		ReleaseVersion:    "1.0",
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
		{MemberID: 1, ClusterID: "c", Address: "a:1", DNSSAN: "a", CertificateSHA256: bytes.Repeat([]byte{1}, 32), ReleaseVersion: "1.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
		{MemberID: 2, ClusterID: "c", Address: "a:2", DNSSAN: "a", CertificateSHA256: bytes.Repeat([]byte{2}, 32), ReleaseVersion: "1.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
		{MemberID: 3, ClusterID: "c", Address: "a:3", DNSSAN: "a", CertificateSHA256: bytes.Repeat([]byte{3}, 32), ReleaseVersion: "1.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
	}
	if err := machine.InitializeBootstrap(3, voters); err != nil {
		t.Fatal(err)
	}

	local, found := committedLocalIdentity(machine, 1)
	if !found || local.MemberID != 1 {
		t.Fatalf("committedLocalIdentity(1) found=%t local=%+v", found, local)
	}
	_, notFound := committedLocalIdentity(machine, 99)
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

func TestSamePeerRequest(t *testing.T) {
	fp := [32]byte{1, 2, 3}
	member := membership.MemberIdentityDTO{
		MemberID: 1, ClusterID: "c", Address: "a:1", DNSSAN: "a",
		CertificateSHA256: fp[:], ReleaseVersion: "1.0",
		ClusterProtocolMajor: 2, ClusterProtocolMinor: 0,
		StateSchemaMajor: 1, StateSchemaMinor: 0,
	}
	candidate := tcpadapter.PeerIdentityDTO{
		MemberID: 1, ClusterID: "c", Address: "a:1", DNSSAN: "a",
		CertificateSHA256: fp, ReleaseVersion: "1.0",
		ClusterProtocolMajor: 2, ClusterProtocolMinor: 0,
		StateSchemaMajor: 1, StateSchemaMinor: 0,
	}
	if !samePeerRequest(member, candidate) {
		t.Fatal("expected match")
	}
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

func TestResolveProposalAndCompleteReadStates(t *testing.T) {
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
	runtime.resolveProposal(cmd1, res, &waiters)
	if len(waiters) != 1 || !bytes.Equal(waiters[0].data, cmd2) {
		t.Fatalf("waiters = %+v", waiters)
	}
	gotRes := <-ch1
	if gotRes.result.Index != 42 {
		t.Fatalf("gotRes index = %d", gotRes.result.Index)
	}

	readResult := make(chan clusterReadResponse, 1)
	reads := map[string]clusterReadWaiter{
		"k1": {result: readResult},
	}
	readStates := []raftadapter.ReadStateDTO{
		{RequestContext: []byte("k1"), Index: 0},
	}
	deferred := runtime.completeReadStates(readStates, reads)
	if len(deferred) != 0 || len(reads) != 0 {
		t.Fatalf("deferred=%+v reads=%+v", deferred, reads)
	}
	resp := <-readResult
	if resp.index != 0 {
		t.Fatalf("resp index = %d", resp.index)
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
