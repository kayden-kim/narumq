package main

import (
	"reflect"
	"testing"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
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
