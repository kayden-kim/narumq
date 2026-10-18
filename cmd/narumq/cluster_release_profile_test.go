package main

import (
	"context"
	"errors"
	"testing"
	"time"

	appmgmt "narumq/internal/application/management"
	"narumq/internal/application/membership"
	managementports "narumq/internal/application/ports/management"
)

func TestEvaluateReleaseProfile(t *testing.T) {
	const nodeID = 7
	const releaseVersion = "1.1.0"
	features := []string{"compatibility-probe"}
	member := membership.MemberIdentityDTO{
		MemberID: nodeID, ReleaseVersion: releaseVersion,
		Features: []string{"compatibility-probe"}, Phase: membership.MemberPhaseVoter,
	}
	cases := []struct {
		name           string
		members        []membership.MemberIdentityDTO
		activeFeatures []string
		features       []string
		wantAction     releaseProfileAction
		wantReason     string
	}{
		{
			name:       "missing local member waits",
			wantAction: releaseProfileWait,
		},
		{
			name:           "staged local member waits before feature compatibility",
			members:        []membership.MemberIdentityDTO{{MemberID: nodeID, Phase: membership.MemberPhaseStaged}},
			activeFeatures: []string{"unsupported-feature"},
			wantAction:     releaseProfileWait,
		},
		{
			name:           "retired local member waits",
			members:        []membership.MemberIdentityDTO{{MemberID: nodeID, Phase: membership.MemberPhaseRetired}},
			activeFeatures: []string{"unsupported-feature"},
			wantAction:     releaseProfileWait,
		},
		{
			name:           "unsupported active feature blocks matching profile",
			members:        []membership.MemberIdentityDTO{member},
			activeFeatures: []string{"unsupported-feature"},
			wantAction:     releaseProfileIncompatible,
			wantReason:     "incompatible_state_schema: committed cluster features are not supported by this release",
		},
		{
			name:           "supported active feature with local identity after peer is synchronized",
			members:        []membership.MemberIdentityDTO{{MemberID: nodeID + 1, Phase: membership.MemberPhaseVoter}, member},
			activeFeatures: []string{"compatibility-probe"},
			features:       features,
			wantAction:     releaseProfileSynced,
		},
		{
			name: "voter release mismatch proposes profile update",
			members: []membership.MemberIdentityDTO{{
				MemberID: nodeID, ReleaseVersion: "1.0.0", Phase: membership.MemberPhaseVoter,
			}},
			features:   features,
			wantAction: releaseProfilePropose,
		},
		{
			name: "learner feature mismatch proposes profile update",
			members: []membership.MemberIdentityDTO{{
				MemberID: nodeID, ReleaseVersion: releaseVersion, Phase: membership.MemberPhaseLearner,
			}},
			features:   features,
			wantAction: releaseProfilePropose,
		},
		{
			name: "catching-up feature mismatch proposes profile update",
			members: []membership.MemberIdentityDTO{{
				MemberID: nodeID, ReleaseVersion: releaseVersion, Phase: membership.MemberPhaseCatchingUp,
			}},
			features:   features,
			wantAction: releaseProfilePropose,
		},
		{
			name: "leaving member waits",
			members: []membership.MemberIdentityDTO{{
				MemberID: nodeID, ReleaseVersion: "1.0.0", Phase: membership.MemberPhaseLeaving,
			}},
			features:   features,
			wantAction: releaseProfileWait,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decision := evaluateReleaseProfile(tc.members, tc.activeFeatures, nodeID, releaseVersion, tc.features)
			if decision.action != tc.wantAction {
				t.Fatalf("action = %d, want %d", decision.action, tc.wantAction)
			}
			if decision.reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", decision.reason, tc.wantReason)
			}
		})
	}
}

// Traceability: CLU-11/CLU-14; docs/specs/cluster-session-and-delivery.md §10, §11.19.
func TestClusterReleaseProfileReconcileCanceledContextPreservesSyncedState(t *testing.T) {
	runtime := clusterReleaseProfileTestRuntime(t, validBootstrapClusterConfig())
	controller := runtime.releaseProfile
	controller.synced.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	controller.reconcile(ctx)

	if !controller.synced.Load() {
		t.Fatal("canceled reconcile changed the previously confirmed release profile")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-111
func TestClusterReleaseProfileReconcilePreservesSyncedStateDuringRead(t *testing.T) {
	runtime := clusterReleaseProfileTestRuntime(t, validBootstrapClusterConfig())
	controller := runtime.releaseProfile
	controller.synced.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reconcileDone := make(chan struct{})
	go func() {
		controller.reconcile(ctx)
		close(reconcileDone)
	}()
	clusterReleaseProfileTestJoinReconcile(t, reconcileDone)

	for index := range 2 {
		var request clusterDriverRequest
		select {
		case request = <-runtime.requests:
		case <-ctx.Done():
			t.Fatalf("receive read request %d: %v", index+1, ctx.Err())
		}
		if request.kind != clusterDriverRead {
			t.Fatalf("request %d kind=%d, want cluster read", index+1, request.kind)
		}
		if !controller.synced.Load() {
			t.Errorf("reconcile cleared synced state while read barrier %d was pending", index+1)
		}
		request.readResult <- clusterReadResponse{index: 1}
	}
	select {
	case <-reconcileDone:
	case <-ctx.Done():
		t.Fatalf("reconcile did not finish: %v", ctx.Err())
	}
	if !controller.synced.Load() {
		t.Fatal("matching committed profile did not remain synchronized")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-111
func TestClusterReleaseProfileReconcileClearsSyncedOnReadErrors(t *testing.T) {
	for _, test := range []struct {
		name            string
		failedReadIndex int
	}{
		{name: "members", failedReadIndex: 0},
		{name: "active features", failedReadIndex: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := clusterReleaseProfileTestRuntime(t, validBootstrapClusterConfig())
			controller := runtime.releaseProfile
			controller.synced.Store(true)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			reconcileDone := make(chan struct{})
			go func() {
				controller.reconcile(ctx)
				close(reconcileDone)
			}()
			clusterReleaseProfileTestJoinReconcile(t, reconcileDone)

			for readIndex := 0; readIndex <= test.failedReadIndex; readIndex++ {
				var request clusterDriverRequest
				select {
				case request = <-runtime.requests:
				case <-ctx.Done():
					t.Fatalf("receive read request %d: %v", readIndex+1, ctx.Err())
				}
				if request.kind != clusterDriverRead {
					t.Fatalf("request %d kind=%d, want cluster read", readIndex+1, request.kind)
				}
				if readIndex == test.failedReadIndex {
					request.readResult <- clusterReadResponse{err: errors.New("read barrier failed")}
					break
				}
				request.readResult <- clusterReadResponse{index: 1}
			}
			select {
			case <-reconcileDone:
			case <-ctx.Done():
				t.Fatalf("reconcile did not finish: %v", ctx.Err())
			}
			if controller.synced.Load() {
				t.Fatal("read error left release profile synchronized")
			}
		})
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-111
func TestClusterReleaseProfileReconcileWaitClearsSyncedState(t *testing.T) {
	runtime := clusterReleaseProfileTestRuntime(t, validBootstrapClusterConfig())
	runtime.config.NodeID = 99
	controller := runtime.releaseProfile
	controller.synced.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reconcileDone := make(chan struct{})
	go func() {
		controller.reconcile(ctx)
		close(reconcileDone)
	}()
	clusterReleaseProfileTestJoinReconcile(t, reconcileDone)

	for readIndex := range 2 {
		request := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
		if request.kind != clusterDriverRead {
			t.Fatalf("request %d kind=%d, want cluster read", readIndex+1, request.kind)
		}
		request.readResult <- clusterReadResponse{index: 1}
	}
	select {
	case <-reconcileDone:
	case <-ctx.Done():
		t.Fatalf("reconcile did not finish: %v", ctx.Err())
	}
	if controller.synced.Load() {
		t.Fatal("missing local member left release profile synchronized")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-111
func TestClusterReleaseProfileReconcileProposalClearsSyncedState(t *testing.T) {
	config := validBootstrapClusterConfig()
	runtime := clusterReleaseProfileTestRuntime(t, config)
	runtime.config.ReleaseVersion = "1.2.0"
	runtime.storage = newClusterStorageMonitor(time.Now, nil)
	controller := runtime.releaseProfile
	controller.synced.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reconcileDone := make(chan struct{})
	go func() {
		controller.reconcile(ctx)
		close(reconcileDone)
	}()
	clusterReleaseProfileTestJoinReconcile(t, reconcileDone)

	for readIndex := range 2 {
		request := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
		if request.kind != clusterDriverRead {
			t.Fatalf("request %d kind=%d, want cluster read", readIndex+1, request.kind)
		}
		request.readResult <- clusterReadResponse{index: 1}
	}
	commit := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
	if controller.synced.Load() {
		t.Fatal("profile mismatch kept readiness synchronized while proposal barrier was pending")
	}
	commit.pending.staged <- clusterCommitStage{
		err: errors.New("stop before proposal completion"), barrierFailure: true,
	}
	select {
	case <-reconcileDone:
	case <-ctx.Done():
		t.Fatalf("reconcile did not finish: %v", ctx.Err())
	}
	if controller.synced.Load() {
		t.Fatal("profile proposal left release profile synchronized")
	}
}

func TestClusterReleaseProfileReconcileMapsProposalConflictsToReadiness(t *testing.T) {
	for _, test := range []struct {
		name       string
		result     membership.ApplyResultDTO
		wantReason string
	}{
		{
			name: "unsupported compatibility",
			result: membership.ApplyResultDTO{
				Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictCompatibilityUnsupported,
			},
			wantReason: "incompatible_state_schema: committed membership rejected this release profile (compatibility_unsupported)",
		},
		{
			name: "missing active feature",
			result: membership.ApplyResultDTO{
				Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictActiveFeatureMissing,
			},
			wantReason: "incompatible_state_schema: committed membership rejected this release profile (active_feature_missing)",
		},
		{
			name: "unrelated conflict",
			result: membership.ApplyResultDTO{
				Outcome: membership.ApplyOutcomeConflict, Conflict: membership.ConflictPhase,
			},
		},
		{
			name:   "applied proposal",
			result: membership.ApplyResultDTO{Outcome: membership.ApplyOutcomeApplied},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := clusterReleaseProfileTestRuntime(t, validBootstrapClusterConfig())
			runtime.storage = newClusterStorageMonitor(time.Now, nil)
			runtime.config.ReleaseVersion = "1.2.0"
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			reconcileDone := make(chan struct{})
			go func() {
				runtime.releaseProfile.reconcile(ctx)
				close(reconcileDone)
			}()
			clusterReleaseProfileTestJoinReconcile(t, reconcileDone)

			for readIndex := range 2 {
				request := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
				if request.kind != clusterDriverRead {
					t.Fatalf("request %d kind=%d, want cluster read", readIndex+1, request.kind)
				}
				request.readResult <- clusterReadResponse{index: 1}
			}
			commit := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
			if commit.kind != clusterDriverCommit || commit.pending == nil {
				t.Fatalf("proposal request = (%d, %v), want commit with pending proposal", commit.kind, commit.pending != nil)
			}
			commit.pending.staged <- clusterCommitStage{context: ctx}
			commit.pending.result <- clusterApplyResponse{result: test.result}
			select {
			case <-reconcileDone:
			case <-ctx.Done():
				t.Fatalf("reconcile did not finish: %v", ctx.Err())
			}

			reason := runtime.releaseProfileIncompatibleReason()
			if test.wantReason == "" {
				if reason != nil {
					t.Fatalf("incompatibility reason = %q, want none", *reason)
				}
			} else if reason == nil || *reason != test.wantReason {
				t.Fatalf("incompatibility reason = %v, want %q", reason, test.wantReason)
			}
			if runtime.releaseProfileSynced() {
				t.Fatal("profile proposal left release profile synchronized")
			}
		})
	}
}

// Traceability: CLU-11; docs/specs/cluster-session-and-delivery.md §11.17.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-113, performance-and-verification.md:PRF-SCN-098
func TestClusterReleaseProfileReconcileBoundsUnansweredProposal(t *testing.T) {
	runtime := clusterReleaseProfileTestRuntime(t, validBootstrapClusterConfig())
	runtime.storage = newClusterStorageMonitor(time.Now, nil)
	runtime.config.ReleaseVersion = "1.2.0"
	ctx, cancel := context.WithTimeout(context.Background(), 3*clusterReleaseProfileCommitTimeout)
	defer cancel()
	reconcileDone := make(chan struct{})
	go func() {
		runtime.releaseProfile.reconcile(ctx)
		close(reconcileDone)
	}()
	clusterReleaseProfileTestJoinReconcile(t, reconcileDone)

	for readIndex := range 2 {
		request := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
		if request.kind != clusterDriverRead {
			t.Fatalf("request %d kind=%d, want cluster read", readIndex+1, request.kind)
		}
		request.readResult <- clusterReadResponse{index: 1}
	}
	commit := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
	if commit.kind != clusterDriverCommit || commit.pending == nil {
		t.Fatalf("proposal request = (%d, %v), want commit with pending proposal", commit.kind, commit.pending != nil)
	}
	outerDeadline, _ := ctx.Deadline()
	proposalDeadline, bounded := commit.pending.context.Deadline()
	if !bounded || !proposalDeadline.Before(outerDeadline) {
		t.Fatalf("profile proposal deadline = (%v, %v), want earlier than controller context %v", proposalDeadline, bounded, outerDeadline)
	}
	// The driver stages the proposal but never reports its commit, as after a lost forward.
	commit.pending.staged <- clusterCommitStage{context: commit.pending.context}
	select {
	case <-reconcileDone:
	case <-time.After(clusterReleaseProfileCommitTimeout + 2*time.Second):
		t.Fatal("reconcile stayed blocked on an unanswered profile proposal")
	}
	if runtime.releaseProfileSynced() {
		t.Fatal("unanswered profile proposal left release profile synchronized")
	}
	if reason := runtime.releaseProfileIncompatibleReason(); reason != nil {
		t.Fatalf("unanswered profile proposal latched incompatibility %q", *reason)
	}
}

func clusterReleaseProfileTestReceiveRequest(
	t *testing.T,
	ctx context.Context,
	runtime *clusterRuntime,
) clusterDriverRequest {
	t.Helper()
	select {
	case request := <-runtime.requests:
		return request
	case <-ctx.Done():
		t.Fatalf("receive driver request: %v", ctx.Err())
		return clusterDriverRequest{}
	}
}

func clusterReleaseProfileTestJoinReconcile(t *testing.T, reconcileDone <-chan struct{}) {
	t.Helper()
	t.Cleanup(func() {
		select {
		case <-reconcileDone:
		case <-time.After(2 * time.Second):
			t.Error("reconcile goroutine did not exit after test cleanup")
		}
	})
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-111
func TestClusterReleaseProfileAppliedLocalMemberChangeInvalidatesStaleRead(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.ReleaseVersion = "1.2.0"
	runtime := clusterReleaseProfileTestRuntime(t, config)
	controller := runtime.releaseProfile
	controller.synced.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reconcileDone := make(chan struct{})
	go func() {
		controller.reconcile(ctx)
		close(reconcileDone)
	}()
	clusterReleaseProfileTestJoinReconcile(t, reconcileDone)
	membersRequest := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
	if membersRequest.kind != clusterDriverRead {
		t.Fatalf("members request kind=%d, want cluster read", membersRequest.kind)
	}
	membersRequest.readResult <- clusterReadResponse{index: 1}
	activeFeaturesRequest := clusterReleaseProfileTestReceiveRequest(t, ctx, runtime)
	if activeFeaturesRequest.kind != clusterDriverRead {
		t.Fatalf("active features request kind=%d, want cluster read", activeFeaturesRequest.kind)
	}
	command := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: "00000000-0000-4000-8000-000000000001",
		MemberProfile: &membership.MemberProfileChangeDTO{
			MemberID: 1, ReleaseVersion: "1.2.1",
		},
	}
	result, err := runtime.machine.Apply(2, command)
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("apply local profile change = (%+v, %v), want applied", result, err)
	}
	controller.observeAppliedCommand(command, result)
	activeFeaturesRequest.readResult <- clusterReadResponse{index: 2}
	select {
	case <-reconcileDone:
	case <-ctx.Done():
		t.Fatalf("reconcile did not finish: %v", ctx.Err())
	}
	if controller.synced.Load() {
		t.Fatal("stale pre-change profile read restored synchronized state")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-111
func TestClusterReleaseProfileAppliedUnsupportedFeatureBlocksReadiness(t *testing.T) {
	config := validBootstrapClusterConfig()
	config.ReleaseVersion = "1.2.0"
	config.Features = []string{"compatibility-probe"}
	runtime := clusterReleaseProfileTestRuntime(t, config)
	runtime.config.ReleaseVersion = "1.1.0"
	runtime.config.Features = nil
	controller := runtime.releaseProfile
	controller.synced.Store(true)

	command := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID:       "00000000-0000-4000-8000-000000000001",
		FeatureActivation: &membership.FeatureActivationDTO{Feature: "compatibility-probe"},
	}
	result, err := runtime.machine.Apply(2, command)
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("activate committed feature = (%+v, %v), want applied", result, err)
	}
	controller.observeAppliedCommand(command, result)

	reason := runtime.releaseProfileIncompatibleReason()
	if reason == nil || *reason != "incompatible_state_schema: committed cluster features are not supported by this release" {
		t.Fatalf("release profile reason = %v, want incompatible active feature", reason)
	}
	if controller.synced.Load() {
		t.Fatal("unsupported active feature left release profile synchronized")
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-111
func TestClusterReleaseProfileCompatibleSnapshotRestoreInvalidatesReadiness(t *testing.T) {
	runtime := clusterReleaseProfileTestRuntime(t, validBootstrapClusterConfig())
	controller := runtime.releaseProfile
	controller.synced.Store(true)

	controller.observeSnapshotRestore()

	if controller.synced.Load() {
		t.Fatal("compatible snapshot restore left release profile synchronized")
	}
	if reason := runtime.releaseProfileIncompatibleReason(); reason != nil {
		t.Fatalf("compatible snapshot restore latched incompatibility: %q", *reason)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-111
func TestClusterReleaseProfileSnapshotRestoreLatchesUnsupportedFeature(t *testing.T) {
	runtime := clusterReleaseProfileRuntimeWithUnsupportedActiveFeature(t)
	controller := runtime.releaseProfile
	controller.synced.Store(true)

	controller.observeSnapshotRestore()

	if controller.synced.Load() {
		t.Fatal("snapshot restore left release profile synchronized")
	}
	if reason := runtime.releaseProfileIncompatibleReason(); reason == nil {
		t.Fatal("snapshot restore did not latch unsupported active feature")
	}
}

func TestClusterReleaseProfileStartRejectsCommittedUnsupportedFeature(t *testing.T) {
	runtime := clusterReleaseProfileRuntimeWithUnsupportedActiveFeature(t)
	controller := runtime.releaseProfile
	ctx, cancel := context.WithCancel(context.Background())
	controller.start(ctx)
	cancel()
	controller.stopAndWait()

	reason := runtime.releaseProfileIncompatibleReason()
	if reason == nil || *reason != "incompatible_state_schema: committed cluster features are not supported by this release" {
		t.Fatalf("release profile reason = %v, want unsupported committed feature", reason)
	}
}

func TestClusterReleaseProfileReconcileRejectsCommittedUnsupportedFeatureBeforeReadBarrier(t *testing.T) {
	runtime := clusterReleaseProfileRuntimeWithUnsupportedActiveFeature(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	runtime.releaseProfile.reconcile(ctx)

	reason := runtime.releaseProfileIncompatibleReason()
	if reason == nil || *reason != "incompatible_state_schema: committed cluster features are not supported by this release" {
		t.Fatalf("release profile reason = %v, want unsupported committed feature", reason)
	}
}

func clusterReleaseProfileRuntimeWithUnsupportedActiveFeature(t *testing.T) *clusterRuntime {
	t.Helper()
	config := validBootstrapClusterConfig()
	config.ReleaseVersion = "1.2.0"
	config.Features = []string{"compatibility-probe"}
	_, members, err := clusterBootstrapIdentities(config)
	if err != nil {
		t.Fatalf("bootstrap identities: %v", err)
	}
	machine := membership.NewStateMachine()
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatalf("initialize bootstrap state: %v", err)
	}
	result, err := machine.Apply(2, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID:       "00000000-0000-4000-8000-000000000001",
		FeatureActivation: &membership.FeatureActivationDTO{Feature: "compatibility-probe"},
	})
	if err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("activate committed feature = (%+v, %v), want applied", result, err)
	}
	runtime := &clusterRuntime{
		config:  clusterConfigFile{NodeID: 1, ReleaseVersion: "1.1.0"},
		machine: machine,
	}
	runtime.releaseProfile = &clusterReleaseProfileController{runtime: runtime}
	return runtime
}

func TestClusterReleaseProfileIncompatibilityBlocksReadiness(t *testing.T) {
	state := appmgmt.NewService(appmgmt.ServiceConfig{StartedAt: time.Unix(1, 0)}, nil, nil, nil, nil)
	state.SetNodeState(managementports.NodeStateReady, nil)
	controller := &clusterReleaseProfileController{}
	const reason = "incompatible_state_schema: committed cluster features are not supported by this release"
	controller.setIncompatible(reason)
	controller.setIncompatible("later incompatibility")
	runtime := &clusterRuntime{
		storage:         newClusterStorageMonitor(func() time.Time { return time.Unix(1, 0) }, nil),
		managementState: state,
		releaseProfile:  controller,
	}
	checker := &clusterHealthChecker{runtime: runtime}

	status, gotReason := checker.CheckReadiness(context.Background())
	if status != managementports.HealthStatusNotReady || gotReason == nil || *gotReason != reason {
		t.Fatalf("CheckReadiness() = (%q, %v), want sticky release incompatibility", status, gotReason)
	}
	if got := state.NodeState(); got != managementports.NodeStateJoining {
		t.Fatalf("NodeState() = %q, want JOINING", got)
	}
}

func clusterReleaseProfileTestRuntime(t *testing.T, config clusterConfigFile) *clusterRuntime {
	t.Helper()
	_, members, err := clusterBootstrapIdentities(config)
	if err != nil {
		t.Fatalf("bootstrap identities: %v", err)
	}
	machine := membership.NewStateMachine()
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatalf("initialize bootstrap state: %v", err)
	}
	runtime := &clusterRuntime{
		config:        config,
		machine:       machine,
		requests:      make(chan clusterDriverRequest, 2),
		driverStopped: make(chan struct{}),
	}
	runtime.releaseProfile = &clusterReleaseProfileController{runtime: runtime}
	return runtime
}
