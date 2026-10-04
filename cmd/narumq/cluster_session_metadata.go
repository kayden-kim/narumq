package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
	managementports "narumq/internal/application/ports/management"
	"narumq/internal/domain/mqtt"
)

const clusterSessionMetadataCommitTimeout = 5 * time.Second

var (
	errClusterSessionMetadataInactive = errors.New("cluster session metadata feature is not active")
	errClusterSessionMetadataConflict = errors.New("cluster session metadata command conflict")
)

func clusterCommandRequiresSessionMetadata(command membership.CommandDTO) bool {
	return command.SessionMetadata != nil || (command.Ownership != nil && command.Ownership.Session != nil)
}

// AcquireSessionOwnership attaches CONNECT metadata only after a fresh read
// confirms that the cluster has activated session metadata.
func (runtime *clusterRuntime) AcquireSessionOwnership(ctx context.Context, request connect.SessionOwnershipRequestDTO) (connect.SessionOwnershipResultDTO, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if request.OwnerNodeID != runtime.config.NodeID {
		return connect.SessionOwnershipResultDTO{}, membership.ErrNotLeader
	}
	if _, _, err := runtime.GetOwnership(ctx, request.TenantID, request.ClientID); err != nil {
		return connect.SessionOwnershipResultDTO{}, err
	}

	var session *membership.SessionConnectDTO
	if slices.Contains(runtime.machine.ActiveFeatures(), membership.SessionMetadataFeature) {
		value := request.Session
		session = &value
	}
	epoch, superseded, err := runtime.acquireOwnership(ctx, request.TenantID, request.ClientID, request.OwnerNodeID, session)
	return connect.SessionOwnershipResultDTO{Epoch: epoch, SupersededWill: superseded}, err
}

// CommitSessionMetadata commits a fenced metadata transition and reports only
// the Will returned by the committed claim/discard decision.
func (runtime *clusterRuntime) CommitSessionMetadata(ctx context.Context, change membership.SessionMetadataChangeDTO) (connect.SessionMetadataOutcomeDTO, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	commitCtx, cancel := context.WithTimeout(ctx, clusterSessionMetadataCommitTimeout)
	defer cancel()

	operationID, err := newClusterOperationID()
	if err != nil {
		return connect.SessionMetadataOutcomeDTO{}, membership.ErrQuorumUnavailable
	}
	result, err := runtime.Commit(commitCtx, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: operationID,
		SessionMetadata: &change,
	})
	if errors.Is(err, errClusterSessionMetadataInactive) {
		return connect.SessionMetadataOutcomeDTO{Replicated: false}, nil
	}
	if err != nil {
		return connect.SessionMetadataOutcomeDTO{}, err
	}
	if result.Outcome == membership.ApplyOutcomeConflict {
		if result.Conflict == membership.ConflictStaleEpoch {
			return connect.SessionMetadataOutcomeDTO{}, connect.ErrSessionOwnershipStale
		}
		return connect.SessionMetadataOutcomeDTO{}, fmt.Errorf("%w: %s", errClusterSessionMetadataConflict, result.Conflict)
	}
	outcome := connect.SessionMetadataOutcomeDTO{Replicated: true}
	if result.SessionMetadata != nil && result.SessionMetadata.Will != nil {
		will := result.SessionMetadata.Will
		outcome.Will = &mqtt.Will{
			Topic: will.Topic, Payload: append([]byte(nil), will.Payload...),
			QoS: byte(will.QoS), Retain: will.Retain,
			DelayInterval: will.DelayIntervalSeconds,
		}
	}
	return outcome, nil
}

// ClusterSessions exposes this node's committed lifecycle view after activation.
func (runtime *clusterRuntime) ClusterSessions() ([]managementports.ClientSessionDTO, bool) {
	if runtime == nil || runtime.machine == nil ||
		!slices.Contains(runtime.machine.ActiveFeatures(), membership.SessionMetadataFeature) {
		return nil, false
	}
	offlineCounts := make(map[clusterSessionRecoveryKey]int)
	for _, record := range runtime.machine.OfflineSessions() {
		offlineCounts[clusterSessionRecoveryKey{tenantID: record.TenantID, clientID: record.ClientID}] = len(record.Messages)
	}
	records := runtime.machine.SessionMetadata()
	clients := make([]managementports.ClientSessionDTO, 0, len(records))
	for _, record := range records {
		client := managementports.ClientSessionDTO{
			TenantID: record.TenantID, ClientID: record.ClientID, Connected: record.Connected,
			SessionPresent: true, Protocol: "MQTT_5_0",
			OfflineQueueCount: offlineCounts[clusterSessionRecoveryKey{tenantID: record.TenantID, clientID: record.ClientID}],
		}
		if record.ProtocolVersion == uint32(mqtt.ProtocolV311) {
			client.Protocol = "MQTT_3_1_1"
		}
		if owner, found := runtime.machine.GetOwnership(record.TenantID, record.ClientID); found {
			nodeID := strconv.FormatUint(owner.OwnerNodeID, 10)
			client.NodeID = &nodeID
		}
		if record.SessionExpiresAtUnixMillis > 0 {
			expiresAt := time.UnixMilli(int64(record.SessionExpiresAtUnixMillis))
			client.ExpiryAt = &expiresAt
		}
		if delivery, found := runtime.machine.DeliverySession(record.TenantID, record.ClientID); found {
			for _, message := range delivery.Messages {
				if message.PacketID != 0 {
					client.InflightCount++
				} else {
					client.OfflineQueueCount++
				}
			}
			for _, inbound := range delivery.Inbound {
				if !inbound.Released {
					client.InflightCount++
				}
			}
		}
		clients = append(clients, client)
	}
	return clients, true
}
