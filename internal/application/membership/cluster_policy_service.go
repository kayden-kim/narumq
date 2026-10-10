package membership

import (
	"context"
	"errors"
	"slices"
)

// ClusterPolicyPort commits policy changes and reads quorum-visible state.
type ClusterPolicyPort interface {
	Commit(ctx context.Context, command CommandDTO) (ApplyResultDTO, error)
	ActiveFeatures(ctx context.Context) ([]string, error)
	GetClusterPolicy(ctx context.Context) (ClusterPolicyStateDTO, bool, error)
}

// UpdateClusterPolicyRequestDTO contains the caller's version, retry key and desired policy.
type UpdateClusterPolicyRequestDTO struct {
	ExpectedVersion uint64
	IdempotencyKey  string
	Policy          ClusterPolicyDTO
}

// ClusterPolicyService manages the committed cluster policy.
type ClusterPolicyService struct {
	port ClusterPolicyPort
}

// NewClusterPolicyService constructs the policy use case with its application-owned port.
func NewClusterPolicyService(port ClusterPolicyPort) (*ClusterPolicyService, error) {
	if port == nil {
		return nil, errors.New("cluster policy port is required")
	}
	return &ClusterPolicyService{port: port}, nil
}

// GetClusterPolicy returns quorum-visible policy state; no committed policy has version zero.
func (service *ClusterPolicyService) GetClusterPolicy(ctx context.Context) (ClusterPolicyStateDTO, bool, error) {
	if ctx == nil {
		return ClusterPolicyStateDTO{}, false, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return ClusterPolicyStateDTO{}, false, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	state, found, err := service.port.GetClusterPolicy(ctx)
	if err != nil {
		return ClusterPolicyStateDTO{}, false, portFailure(err)
	}
	if found && validateClusterPolicyState(state) != nil {
		return ClusterPolicyStateDTO{}, false, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return state, found, nil
}

// UpdateClusterPolicy commits the next policy version or replays the latest matching key.
func (service *ClusterPolicyService) UpdateClusterPolicy(ctx context.Context, request UpdateClusterPolicyRequestDTO) (ClusterPolicyStateDTO, error) {
	if ctx == nil || !validIdempotencyKey(request.IdempotencyKey) || ValidateClusterPolicy(request.Policy) != nil || request.ExpectedVersion == ^uint64(0) {
		return ClusterPolicyStateDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return ClusterPolicyStateDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	features, err := service.port.ActiveFeatures(ctx)
	if err != nil {
		return ClusterPolicyStateDTO{}, portFailure(err)
	}
	if !slices.Contains(features, ClusterPolicyFeature) {
		return ClusterPolicyStateDTO{}, newServiceError(ServiceErrorFeatureInactive, false)
	}
	operationID, err := newOperationID()
	if err != nil {
		return ClusterPolicyStateDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	version := request.ExpectedVersion + 1
	result, err := service.port.Commit(ctx, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: operationID,
		ClusterPolicy: &ClusterPolicyChangeDTO{
			ExpectedVersion: request.ExpectedVersion, Version: version,
			Policy: request.Policy, IdempotencyKey: request.IdempotencyKey,
		},
	})
	if err != nil {
		return ClusterPolicyStateDTO{}, portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		return ClusterPolicyStateDTO{}, conflictServiceError(result.Conflict)
	}
	if result.Outcome != ApplyOutcomeApplied || result.Index == 0 {
		return ClusterPolicyStateDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return ClusterPolicyStateDTO{
		Version: version, Policy: request.Policy,
		IdempotencyKey: request.IdempotencyKey, PreviousVersion: request.ExpectedVersion,
	}, nil
}
