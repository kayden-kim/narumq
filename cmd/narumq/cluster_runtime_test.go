package main

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
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

	runtime := &clusterRuntime{
		config: config, node: node, machine: retainedTestMachine(t, config, config.ReleaseVersion),
		storage: newClusterStorageMonitor(time.Now, nil),
	}
	runtime.initialized.Store(true)
	runtime.projectionIndex.Store(status.AppliedIndex)
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
	runtime.projectionIndex.Store(status.AppliedIndex - 1)

	startedAt := time.Now()
	admitted := runtime.clusterProjectionReady()
	elapsed := time.Since(startedAt)
	snapshot := runtime.readinessSnapshot()
	if admitted {
		t.Fatalf("projection behind the applied index was admitted: snapshot=%+v", snapshot)
	}
	if elapsed >= time.Second {
		t.Fatalf("behind-index projection check took %s, want an immediate fail-closed result", elapsed)
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

// Traceability: MQTT-01; docs/specs/mqtt-protocol-and-session.md §1.
func TestClusterMQTTListenersBindConfiguredEndpointsAndClosePartialOpen(t *testing.T) {
	listeners, err := clusterMQTTListeners(clusterConfigFile{})
	if err != nil || len(listeners) != 0 {
		t.Fatalf("empty cluster MQTT listeners=%v err=%v", listeners, err)
	}

	tcpAddress := clusterRuntimeTestFreeAddress(t)
	invalidTLS := clusterConfigFile{
		MQTT:    clusterMQTT{TCPListenAddress: tcpAddress, TLSListenAddress: clusterRuntimeTestFreeAddress(t)},
		PeerTLS: clusterPeerTLS{CertFile: filepath.Join(t.TempDir(), "missing.crt"), KeyFile: filepath.Join(t.TempDir(), "missing.key")},
	}
	if listeners, err := clusterMQTTListeners(invalidTLS); err == nil || listeners != nil {
		t.Fatalf("invalid TLS listener result=%v err=%v, want closed partial TCP listener and error", listeners, err)
	}
	rebound, err := net.Listen("tcp", tcpAddress)
	if err != nil {
		t.Fatalf("TCP listener remained open after TLS setup failure: %v", err)
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
		},
		PeerTLS: clusterPeerTLS{CertFile: certPath, KeyFile: keyPath},
	}
	listeners, err = clusterMQTTListeners(config)
	if err != nil || len(listeners) != 2 {
		t.Fatalf("configured TCP/TLS listeners=%v err=%v, want two bound listeners", listeners, err)
	}
	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
