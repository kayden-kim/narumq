package membership

import (
	"context"
	"errors"
	"sort"

	applicationclock "narumq/internal/application/ports/clock"
)

// CredentialRevocationPort commits revocation changes and reads committed revocation state.
type CredentialRevocationPort interface {
	Commit(ctx context.Context, command CommandDTO) (ApplyResultDTO, error)
	CredentialRevocations(ctx context.Context) ([]CredentialRevocationStateDTO, error)
}

// CredentialRevocationService implements management operations over committed credential revocations.
type CredentialRevocationService struct {
	port  CredentialRevocationPort
	clock applicationclock.Clock
}

// NewCredentialRevocationService constructs a use case service with committed-state and clock ports.
func NewCredentialRevocationService(port CredentialRevocationPort, clock applicationclock.Clock) (*CredentialRevocationService, error) {
	if port == nil {
		return nil, errors.New("credential revocation port is required")
	}
	if clock == nil {
		return nil, errors.New("credential revocation clock is required")
	}
	return &CredentialRevocationService{port: port, clock: clock}, nil
}

// ListCredentialRevocations returns the committed revocation list in credential-ID order.
func (service *CredentialRevocationService) ListCredentialRevocations(ctx context.Context) ([]CredentialRevocationStateDTO, error) {
	if ctx == nil {
		return nil, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return nil, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	states, err := service.port.CredentialRevocations(ctx)
	if err != nil {
		return nil, portFailure(err)
	}
	if states == nil {
		return []CredentialRevocationStateDTO{}, nil
	}
	states = append([]CredentialRevocationStateDTO(nil), states...)
	sort.Slice(states, func(left, right int) bool { return states[left].CredentialID < states[right].CredentialID })
	for index, state := range states {
		if !validCredentialRevocationState(state) || (index > 0 && states[index-1].CredentialID == state.CredentialID) {
			return nil, newServiceError(ServiceErrorQuorumUnavailable, true)
		}
	}
	return states, nil
}

// RevokeCredential commits revocation of the complete credential ID.
func (service *CredentialRevocationService) RevokeCredential(ctx context.Context, credentialID string) (CredentialRevocationStatusDTO, error) {
	return service.changeCredentialRevocation(ctx, credentialID, CredentialRevocationActionRevoke)
}

// RestoreCredential commits restoration of the credential ID; restoring an active ID is a no-op.
func (service *CredentialRevocationService) RestoreCredential(ctx context.Context, credentialID string) (CredentialRevocationStatusDTO, error) {
	return service.changeCredentialRevocation(ctx, credentialID, CredentialRevocationActionRestore)
}

func (service *CredentialRevocationService) changeCredentialRevocation(
	ctx context.Context,
	credentialID string,
	action CredentialRevocationAction,
) (CredentialRevocationStatusDTO, error) {
	if ctx == nil || !boundedUTF8(credentialID, 1, MaxCredentialIDBytes) {
		return CredentialRevocationStatusDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return CredentialRevocationStatusDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	proposedAt := service.clock.Now().UnixMilli()
	if proposedAt <= 0 {
		return CredentialRevocationStatusDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	operationID, err := newOperationID()
	if err != nil {
		return CredentialRevocationStatusDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	result, err := service.port.Commit(ctx, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: operationID,
		CredentialRevocation: &CredentialRevocationChangeDTO{
			Action: action, CredentialID: credentialID, ProposedAtUnixMillis: uint64(proposedAt),
		},
	})
	if err != nil {
		return CredentialRevocationStatusDTO{}, portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		return CredentialRevocationStatusDTO{}, conflictServiceError(result.Conflict)
	}
	status := result.CredentialRevocation
	if result.Outcome != ApplyOutcomeApplied || result.Index == 0 || status == nil || status.CredentialID != credentialID {
		return CredentialRevocationStatusDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	if (action == CredentialRevocationActionRevoke && (!status.Revoked || status.RevokedAtUnixMillis == 0)) ||
		(action == CredentialRevocationActionRestore && (status.Revoked || status.RevokedAtUnixMillis != 0)) {
		return CredentialRevocationStatusDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return *status, nil
}
