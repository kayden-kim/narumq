package main

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
)

const (
	clusterSessionRecoveryRetryInterval = time.Second
	clusterSessionRecoveryReadyPoll     = 500 * time.Millisecond
	clusterSessionRecoveryQueueCapacity = 1024
)

type clusterSessionRecoveryKey struct{ tenantID, clientID string }

type clusterSessionRecoveryController struct {
	runtime  *clusterRuntime
	wg       sync.WaitGroup
	keys     chan clusterSessionRecoveryKey
	fullScan atomic.Bool
}

func (controller *clusterSessionRecoveryController) start(ctx context.Context) {
	if controller == nil || controller.runtime == nil || ctx == nil {
		return
	}
	controller.wg.Add(1)
	go func() { defer controller.wg.Done(); controller.run(ctx) }()
}

func (controller *clusterSessionRecoveryController) stopAndWait() {
	if controller != nil {
		controller.wg.Wait()
	}
}

func (controller *clusterSessionRecoveryController) requestFullScan() {
	if controller != nil {
		controller.fullScan.Store(true)
	}
}

func (controller *clusterSessionRecoveryController) enqueue(tenantID, clientID string) {
	if controller == nil {
		return
	}
	select {
	case controller.keys <- clusterSessionRecoveryKey{tenantID: tenantID, clientID: clientID}:
	default:
		controller.requestFullScan()
	}
}

func (controller *clusterSessionRecoveryController) run(ctx context.Context) {
	if controller == nil || controller.runtime == nil || controller.runtime.machine == nil || ctx == nil {
		return
	}
	readyPoll := time.NewTicker(clusterSessionRecoveryReadyPoll)
	defer readyPoll.Stop()
	for {
		readyCtx, cancel := context.WithTimeout(ctx, clusterReadTimeout)
		err := controller.runtime.checkReadiness(readyCtx, "session-recovery")
		cancel()
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-readyPoll.C:
		}
	}
	controller.requestFullScan()
	pending := make(map[clusterSessionRecoveryKey]struct{})
	retry := time.NewTicker(clusterSessionRecoveryRetryInterval)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-controller.keys:
			pending[key] = struct{}{}
		case <-retry.C:
			if controller.fullScan.Swap(false) {
				for _, record := range controller.runtime.machine.SessionMetadata() {
					pending[clusterSessionRecoveryKey{tenantID: record.TenantID, clientID: record.ClientID}] = struct{}{}
				}
			}
			controller.recoverPending(ctx, pending)
		}
	}
}

func (controller *clusterSessionRecoveryController) recoverPending(ctx context.Context, pending map[clusterSessionRecoveryKey]struct{}) {
	if controller == nil || controller.runtime == nil || controller.runtime.machine == nil || ctx == nil {
		return
	}
	runtime := controller.runtime
	if runtime.connectUseCase == nil || !slices.Contains(runtime.machine.ActiveFeatures(), membership.SessionMetadataFeature) {
		clear(pending)
		return
	}
	for key := range pending {
		if ctx.Err() != nil {
			return
		}
		owner, owned := runtime.machine.GetOwnership(key.tenantID, key.clientID)
		record, recorded := runtime.machine.GetSessionMetadata(key.tenantID, key.clientID)
		if !owned || !recorded || owner.OwnerNodeID != runtime.config.NodeID {
			delete(pending, key)
			continue
		}
		err := runtime.connectUseCase.RecoverOwnedSession(ctx, connect.RecoveredSessionDTO{Metadata: record, OwnershipEpoch: owner.Epoch})
		if err != nil {
			if runtime.logger != nil {
				runtime.logger.Warn("session metadata recovery failed", "tenant_id", key.tenantID, "client_id", key.clientID, "error", err)
			}
			continue
		}
		delete(pending, key)
	}
}
