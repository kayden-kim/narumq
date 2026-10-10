package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/membership"
)

const clusterPolicyRetryInterval = 500 * time.Millisecond

type clusterPolicyController struct {
	runtime  *clusterRuntime
	wg       sync.WaitGroup
	mu       sync.Mutex
	mismatch string
}

func (controller *clusterPolicyController) start(ctx context.Context) {
	if controller == nil || controller.runtime == nil || ctx == nil {
		return
	}
	controller.wg.Add(1)
	go func() {
		defer controller.wg.Done()
		controller.run(ctx)
	}()
}

func (controller *clusterPolicyController) stopAndWait() {
	if controller != nil {
		controller.wg.Wait()
	}
}

func (controller *clusterPolicyController) run(ctx context.Context) {
	controller.reconcile(ctx)
	ticker := time.NewTicker(clusterPolicyRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			controller.reconcile(ctx)
		}
	}
}

func (controller *clusterPolicyController) reconcile(ctx context.Context) {
	if controller == nil || controller.runtime == nil || controller.runtime.machine == nil || ctx == nil || ctx.Err() != nil {
		return
	}
	runtime := controller.runtime
	committed, found := runtime.machine.ClusterPolicy()
	var committedState *membership.ClusterPolicyStateDTO
	if found {
		committedState = &committed
	}
	featureActive := slices.Contains(runtime.machine.ActiveFeatures(), membership.ClusterPolicyFeature)
	change, mismatch := clusterPolicyReconciliationDecision(runtime.config.ClusterPolicy, committedState, featureActive)
	controller.observe(committedState, featureActive, mismatch)
	if change != nil {
		controller.propose(ctx, *change)
	}
}

func clusterPolicyReconciliationDecision(
	desired *clusterPolicyConfig,
	committed *membership.ClusterPolicyStateDTO,
	featureActive bool,
) (*membership.ClusterPolicyChangeDTO, bool) {
	if !featureActive {
		return nil, false
	}
	if committed == nil || committed.Version == 0 {
		if desired == nil {
			return nil, false
		}
		return &membership.ClusterPolicyChangeDTO{
			ExpectedVersion: 0, Version: desired.Version, Policy: desired.membershipPolicy(),
			IdempotencyKey: clusterPolicyConfigIdempotencyKey(desired.Version),
		}, false
	}
	if desired == nil {
		return nil, true
	}
	if desired.Version < committed.Version {
		return nil, true
	}
	if desired.Version == committed.Version {
		return nil, desired.membershipPolicy() != committed.Policy
	}
	return &membership.ClusterPolicyChangeDTO{
		ExpectedVersion: committed.Version, Version: desired.Version, Policy: desired.membershipPolicy(),
		IdempotencyKey: clusterPolicyConfigIdempotencyKey(desired.Version),
	}, false
}

func clusterPolicyConfigIdempotencyKey(version uint64) string {
	return fmt.Sprintf("config-v%d", version)
}

func (controller *clusterPolicyController) propose(ctx context.Context, change membership.ClusterPolicyChangeDTO) {
	operationID, err := newClusterOperationID()
	if err != nil || controller == nil || controller.runtime == nil {
		return
	}
	_, _ = controller.runtime.Commit(ctx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: operationID,
		ClusterPolicy: &change,
	})
}

func (controller *clusterPolicyController) observe(committed *membership.ClusterPolicyStateDTO, featureActive, mismatch bool) {
	if controller == nil || controller.runtime == nil {
		return
	}
	runtime := controller.runtime
	committedValue, committedVersion := committed != nil, uint64(0)
	if committed != nil {
		committedVersion = committed.Version
	}
	if runtime.metrics != nil {
		runtime.metrics.SetClusterPolicy(committedValue, committedVersion, mismatch)
	}
	configuredVersion := uint64(0)
	if runtime.config.ClusterPolicy != nil {
		configuredVersion = runtime.config.ClusterPolicy.Version
	}
	controller.setMismatch(featureActive && mismatch, configuredVersion, committedVersion)
}

func (controller *clusterPolicyController) refreshCommittedState() {
	if controller == nil || controller.runtime == nil || controller.runtime.machine == nil {
		return
	}
	runtime := controller.runtime
	committed, found := runtime.machine.ClusterPolicy()
	var committedState *membership.ClusterPolicyStateDTO
	if found {
		committedState = &committed
	}
	featureActive := slices.Contains(runtime.machine.ActiveFeatures(), membership.ClusterPolicyFeature)
	_, mismatch := clusterPolicyReconciliationDecision(runtime.config.ClusterPolicy, committedState, featureActive)
	controller.observe(committedState, featureActive, mismatch)
}

func (controller *clusterPolicyController) setMismatch(mismatch bool, configuredVersion, committedVersion uint64) {
	if controller == nil {
		return
	}
	key := ""
	if mismatch {
		key = fmt.Sprintf("configured=%d,committed=%d", configuredVersion, committedVersion)
	}
	controller.mu.Lock()
	if key == controller.mismatch {
		controller.mu.Unlock()
		return
	}
	controller.mismatch = key
	controller.mu.Unlock()
	if key != "" && controller.runtime != nil && controller.runtime.logger != nil {
		controller.runtime.logger.Warn("committed cluster policy differs from local configuration",
			"configured_version", configuredVersion, "committed_version", committedVersion)
	}
}

func effectiveClusterMQTTLimits(
	config clusterConfigFile,
	committed *membership.ClusterPolicyStateDTO,
	featureActive bool,
) membership.MQTTLimitsDTO {
	if featureActive && committed != nil && committed.Version > 0 {
		return committed.Policy.MQTTLimits
	}
	if config.ClusterPolicy != nil {
		return config.ClusterPolicy.membershipPolicy().MQTTLimits
	}
	return defaultClusterMQTTLimits()
}

func tcpConnectionLimits(limits membership.MQTTLimitsDTO) tcpadapter.ConnectionLimits {
	return tcpadapter.ConnectionLimits{
		MaxPacketSize: int(limits.MaxPacketSize), ReceiveMaximum: uint16(limits.ReceiveMaximum),
		ConnectTimeout:    time.Duration(limits.ConnectTimeoutMillis) * time.Millisecond,
		PacketReadTimeout: time.Duration(limits.PacketReadTimeoutMillis) * time.Millisecond,
		WriteTimeout:      time.Duration(limits.WriteTimeoutMillis) * time.Millisecond,
	}
}

func (runtime *clusterRuntime) applyCommittedClusterMQTTLimits() error {
	if runtime == nil || runtime.mqttServer == nil || runtime.machine == nil {
		return nil
	}
	committed, found := runtime.machine.ClusterPolicy()
	var committedState *membership.ClusterPolicyStateDTO
	if found {
		committedState = &committed
	}
	featureActive := slices.Contains(runtime.machine.ActiveFeatures(), membership.ClusterPolicyFeature)
	limits := effectiveClusterMQTTLimits(runtime.config, committedState, featureActive)
	if err := runtime.mqttServer.SetConnectionLimits(tcpConnectionLimits(limits)); err != nil {
		return fmt.Errorf("apply committed cluster MQTT limits: %w", err)
	}
	return nil
}
