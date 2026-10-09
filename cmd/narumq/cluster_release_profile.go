package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/domain/release"
)

const clusterReleaseProfileRetryInterval = 500 * time.Millisecond
const clusterReleaseProfileCommitTimeout = 5 * time.Second

type releaseProfileAction uint8

const (
	releaseProfileWait releaseProfileAction = iota
	releaseProfileSynced
	releaseProfilePropose
	releaseProfileIncompatible
)

type releaseProfileDecision struct {
	action releaseProfileAction
	reason string
}

type clusterReleaseProfileController struct {
	runtime       *clusterRuntime
	wg            sync.WaitGroup
	stateMu       sync.Mutex
	revision      uint64
	reconcileWake chan struct{}
	synced        atomic.Bool
	incompatible  atomic.Pointer[string]
}

func (controller *clusterReleaseProfileController) start(ctx context.Context) {
	if controller == nil || controller.runtime == nil || ctx == nil {
		return
	}
	if reason := controller.unsupportedActiveFeatureReason(); reason != "" {
		controller.setIncompatible(reason)
		return
	}
	controller.stateMu.Lock()
	controller.reconcileWake = make(chan struct{}, 1)
	controller.stateMu.Unlock()
	controller.wg.Add(1)
	go func() {
		defer controller.wg.Done()
		controller.run(ctx)
	}()
}

func (controller *clusterReleaseProfileController) unsupportedActiveFeatureReason() string {
	if controller == nil || controller.runtime == nil || controller.runtime.machine == nil {
		return ""
	}
	config := controller.runtime.config
	unsupported := false
	for _, active := range controller.runtime.machine.ActiveFeatures() {
		if !release.Contains(config.Features, active) {
			unsupported = true
			break
		}
	}
	if !unsupported {
		return ""
	}
	for _, member := range controller.runtime.machine.Members() {
		if member.MemberID != config.NodeID {
			continue
		}
		if member.Phase == membership.MemberPhaseStaged || member.Phase == membership.MemberPhaseRetired {
			return ""
		}
		return "incompatible_state_schema: committed cluster features are not supported by this release"
	}
	return ""
}

func (controller *clusterReleaseProfileController) stopAndWait() {
	if controller != nil {
		controller.wg.Wait()
	}
}

func (controller *clusterReleaseProfileController) run(ctx context.Context) {
	controller.reconcile(ctx)
	ticker := time.NewTicker(clusterReleaseProfileRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			controller.reconcile(ctx)
		case <-controller.reconcileWake:
			controller.reconcile(ctx)
		}
	}
}

func (controller *clusterReleaseProfileController) reconcile(ctx context.Context) {
	if controller == nil || controller.runtime == nil || controller.runtime.machine == nil || ctx == nil || ctx.Err() != nil {
		return
	}
	if controller.incompatible.Load() != nil {
		return
	}
	if reason := controller.unsupportedActiveFeatureReason(); reason != "" {
		controller.setIncompatible(reason)
		return
	}
	revision := controller.beginReconcile()
	members, err := controller.runtime.Members(ctx)
	if err != nil {
		if controller.synced.Load() && controller.runtime.logger != nil {
			controller.runtime.logger.Warn("cluster release profile confirmation failed", "stage", "members", "error", err.Error())
		}
		controller.finishReconcile(revision, false)
		return
	}
	activeFeatures, err := controller.runtime.ActiveFeatures(ctx)
	if err != nil {
		if controller.synced.Load() && controller.runtime.logger != nil {
			controller.runtime.logger.Warn("cluster release profile confirmation failed", "stage", "active_features", "error", err.Error())
		}
		controller.finishReconcile(revision, false)
		return
	}
	config := controller.runtime.config
	decision := evaluateReleaseProfile(members, activeFeatures, config.NodeID, config.ReleaseVersion, config.Features)
	switch decision.action {
	case releaseProfileWait:
		if controller.synced.Load() && controller.runtime.logger != nil {
			controller.runtime.logger.Warn("cluster release profile confirmation failed", "stage", "local_member_unavailable")
		}
		controller.finishReconcile(revision, false)
	case releaseProfileSynced:
		controller.finishReconcile(revision, true)
	case releaseProfileIncompatible:
		controller.setIncompatible(decision.reason)
	case releaseProfilePropose:
		if controller.synced.Load() && controller.runtime.logger != nil {
			controller.runtime.logger.Warn("cluster release profile confirmation failed", "stage", "profile_mismatch")
		}
		controller.finishReconcile(revision, false)
		operationID, err := newClusterOperationID()
		if err != nil {
			return
		}
		commitCtx, cancel := context.WithTimeout(ctx, clusterReleaseProfileCommitTimeout)
		result, err := controller.runtime.Commit(commitCtx, membership.CommandDTO{
			SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
			OperationID: operationID,
			MemberProfile: &membership.MemberProfileChangeDTO{
				MemberID: config.NodeID, ReleaseVersion: config.ReleaseVersion,
				Features: slices.Clone(config.Features),
			},
		})
		cancel()
		if err != nil || result.Outcome != membership.ApplyOutcomeConflict {
			return
		}
		switch result.Conflict {
		case membership.ConflictCompatibilityUnsupported, membership.ConflictActiveFeatureMissing:
			controller.setIncompatible(fmt.Sprintf("incompatible_state_schema: committed membership rejected this release profile (%s)", result.Conflict))
		}
	}
}

func (controller *clusterReleaseProfileController) beginReconcile() uint64 {
	controller.stateMu.Lock()
	defer controller.stateMu.Unlock()
	return controller.revision
}

func (controller *clusterReleaseProfileController) finishReconcile(revision uint64, synced bool) {
	controller.stateMu.Lock()
	defer controller.stateMu.Unlock()
	if controller.revision != revision || controller.incompatible.Load() != nil {
		controller.synced.Store(false)
		return
	}
	controller.synced.Store(synced)
}

func (controller *clusterReleaseProfileController) invalidate() {
	controller.stateMu.Lock()
	controller.revision++
	controller.synced.Store(false)
	// Coalesce invalidations while a fresh confirmation is already pending.
	// The wake changes scheduling only; invalidate closes the gate first.
	select {
	case controller.reconcileWake <- struct{}{}:
	default:
	}
	controller.stateMu.Unlock()
}

func (controller *clusterReleaseProfileController) observeAppliedCommand(command membership.CommandDTO, result membership.ApplyResultDTO) {
	if controller == nil || controller.runtime == nil || result.Outcome != membership.ApplyOutcomeApplied {
		return
	}
	if command.MemberProfile != nil && command.MemberProfile.MemberID == controller.runtime.config.NodeID {
		if controller.synced.Load() && controller.runtime.logger != nil {
			controller.runtime.logger.Warn("cluster release profile invalidated", "stage", "local_profile_applied")
		}
		controller.invalidate()
	}
	if command.FeatureActivation != nil {
		if reason := controller.unsupportedActiveFeatureReason(); reason != "" {
			controller.setIncompatible(reason)
		}
	}
}

func (controller *clusterReleaseProfileController) observeSnapshotRestore() {
	if controller == nil || controller.runtime == nil {
		return
	}
	if reason := controller.unsupportedActiveFeatureReason(); reason != "" {
		controller.setIncompatible(reason)
		return
	}
	if controller.synced.Load() && controller.runtime.logger != nil {
		controller.runtime.logger.Warn("cluster release profile invalidated", "stage", "snapshot_restored")
	}
	controller.invalidate()
}

func (controller *clusterReleaseProfileController) setIncompatible(reason string) {
	if controller == nil || reason == "" || controller.incompatible.Load() != nil {
		return
	}
	storedReason := reason
	if !controller.incompatible.CompareAndSwap(nil, &storedReason) {
		return
	}
	controller.invalidate()
	if controller.runtime != nil && controller.runtime.logger != nil {
		controller.runtime.logger.Warn("cluster release profile is incompatible", "reason", reason)
	}
}

func (runtime *clusterRuntime) releaseProfileSynced() bool {
	return runtime != nil && runtime.releaseProfile != nil && runtime.releaseProfile.synced.Load()
}

func (runtime *clusterRuntime) releaseProfileIncompatibleReason() *string {
	if runtime == nil || runtime.releaseProfile == nil {
		return nil
	}
	return runtime.releaseProfile.incompatible.Load()
}

func evaluateReleaseProfile(
	members []membership.MemberIdentityDTO,
	activeFeatures []string,
	nodeID uint64,
	releaseVersion string,
	features []string,
) releaseProfileDecision {
	var local *membership.MemberIdentityDTO
	for index := range members {
		if members[index].MemberID == nodeID {
			local = &members[index]
			break
		}
	}
	if local == nil || local.Phase == membership.MemberPhaseStaged || local.Phase == membership.MemberPhaseRetired {
		return releaseProfileDecision{action: releaseProfileWait}
	}
	for _, active := range activeFeatures {
		if !release.Contains(features, active) {
			return releaseProfileDecision{
				action: releaseProfileIncompatible,
				reason: "incompatible_state_schema: committed cluster features are not supported by this release",
			}
		}
	}
	if local.ReleaseVersion == releaseVersion && slices.Equal(local.Features, features) {
		return releaseProfileDecision{action: releaseProfileSynced}
	}
	switch local.Phase {
	case membership.MemberPhaseLearner, membership.MemberPhaseCatchingUp, membership.MemberPhaseVoter:
		return releaseProfileDecision{action: releaseProfilePropose}
	default:
		return releaseProfileDecision{action: releaseProfileWait}
	}
}
