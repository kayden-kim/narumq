package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"narumq/internal/application/membership"
)

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §8, §11.8.
func TestClusterRetainedProfileRequiresEveryActiveMemberToMatch(t *testing.T) {
	config := validBootstrapClusterConfig()
	matching := membership.MemberIdentityDTO{
		ClusterID: config.ClusterID, ReleaseVersion: config.ReleaseVersion,
		ClusterProtocolMajor: config.ClusterProtocolMajor, ClusterProtocolMinor: config.ClusterProtocolMinor,
		StateSchemaMajor: config.StateSchemaMajor, StateSchemaMinor: config.StateSchemaMinor,
		Phase: membership.MemberPhaseVoter,
	}
	cases := []struct {
		name    string
		members []membership.MemberIdentityDTO
		want    bool
	}{
		{name: "all active members match", members: []membership.MemberIdentityDTO{matching, matching}, want: true},
		{name: "empty membership", members: nil, want: false},
		{name: "release mismatch voter", members: []membership.MemberIdentityDTO{matching, mutateMemberProfileForTest(matching, func(member *membership.MemberIdentityDTO) { member.ReleaseVersion = "0.9.0" })}},
		{name: "release mismatch learner", members: []membership.MemberIdentityDTO{matching, mutateMemberProfileForTest(matching, func(member *membership.MemberIdentityDTO) {
			member.ReleaseVersion = "0.9.0"
			member.Phase = membership.MemberPhaseLearner
		})}},
		{name: "protocol mismatch", members: []membership.MemberIdentityDTO{mutateMemberProfileForTest(matching, func(member *membership.MemberIdentityDTO) { member.ClusterProtocolMinor++ })}},
		{name: "schema mismatch", members: []membership.MemberIdentityDTO{mutateMemberProfileForTest(matching, func(member *membership.MemberIdentityDTO) { member.StateSchemaMinor++ })}},
		{name: "cluster mismatch", members: []membership.MemberIdentityDTO{mutateMemberProfileForTest(matching, func(member *membership.MemberIdentityDTO) { member.ClusterID = "other-cluster" })}},
		{name: "retired old profile is inactive", members: []membership.MemberIdentityDTO{matching, mutateMemberProfileForTest(matching, func(member *membership.MemberIdentityDTO) {
			member.ReleaseVersion = "0.9.0"
			member.Phase = membership.MemberPhaseRetired
		})}, want: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := clusterExactProfileCompatible(test.members, config); got != test.want {
				t.Fatalf("clusterExactProfileCompatible() = %v, want %v", got, test.want)
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §8, §11.8.
func TestClusterRetainedCommitChecksMembershipAfterFreshReadBarrier(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.ReleaseVersion = "1.2.3"
	machineAtBarrierStart := retainedTestMachine(t, config, config.ReleaseVersion)
	machineAfterBarrier := retainedTestMachine(t, config, "1.0.0")
	runtime := &clusterRuntime{
		config: config, machine: machineAtBarrierStart,
		storage:  newClusterStorageMonitor(func() time.Time { return time.Unix(1, 0) }, nil),
		requests: make(chan clusterDriverRequest),
	}
	change := membership.RetainedChangeDTO{
		TenantID: "tenant", Topic: "status", PublisherClientID: "publisher",
		Payload: []byte("value"), QoS: 1, ProposedAtUnixMillis: 1000,
		Action: membership.RetainedActionReplace,
	}
	command := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: "00000000-0000-4000-8000-000000000001", Retained: &change,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := runtime.Commit(ctx, command)
		result <- err
	}()

	var request clusterDriverRequest
	select {
	case request = <-runtime.requests:
	case <-ctx.Done():
		t.Fatal("Commit() did not request a fresh read barrier")
	}
	if request.kind != clusterDriverRead || request.readResult == nil {
		t.Fatalf("first Commit request = %+v, want read barrier", request)
	}
	runtime.machine = machineAfterBarrier
	request.readResult <- clusterReadResponse{index: 1}
	select {
	case err := <-result:
		if !errors.Is(err, errClusterExactProfileCompatibility) {
			t.Fatalf("Commit(mixed-release retained after barrier) = %v, want compatibility refusal", err)
		}
	case <-ctx.Done():
		t.Fatal("Commit() did not check the membership profile after the read barrier")
	}
}

func retainedTestMachine(t *testing.T, config clusterConfigFile, releaseVersion string) *membership.StateMachine {
	t.Helper()
	members := make([]membership.MemberIdentityDTO, 3)
	for index := range members {
		memberID := uint64(index + 1)
		members[index] = membership.MemberIdentityDTO{
			MemberID: memberID, ClusterID: config.ClusterID,
			Address: fmt.Sprintf("node%d.example:19%03d", memberID, memberID),
			DNSSAN:  fmt.Sprintf("node%d.example", memberID), CertificateSHA256: bytes.Repeat([]byte{byte(memberID)}, 32),
			ReleaseVersion: releaseVersion, ClusterProtocolMajor: config.ClusterProtocolMajor,
			ClusterProtocolMinor: config.ClusterProtocolMinor, StateSchemaMajor: config.StateSchemaMajor,
			StateSchemaMinor: config.StateSchemaMinor, Phase: membership.MemberPhaseVoter,
		}
	}
	machine := membership.NewStateMachine()
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatalf("InitializeBootstrap(): %v", err)
	}
	return machine
}

func mutateMemberProfileForTest(member membership.MemberIdentityDTO, mutate func(*membership.MemberIdentityDTO)) membership.MemberIdentityDTO {
	mutate(&member)
	return member
}
