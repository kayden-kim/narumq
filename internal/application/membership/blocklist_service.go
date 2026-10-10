package membership

import (
	"context"
	"errors"
	"sort"

	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/domain/security"
)

// BlocklistPort commits policy changes and reads committed blocklist state.
type BlocklistPort interface {
	Commit(ctx context.Context, command CommandDTO) (ApplyResultDTO, error)
	Blocklist(ctx context.Context) (BlocklistDTO, error)
}

// BlocklistService implements management operations over committed blocklist state.
type BlocklistService struct {
	port  BlocklistPort
	clock applicationclock.Clock
}

// NewBlocklistService constructs a use case service with committed-state and clock ports.
func NewBlocklistService(port BlocklistPort, clock applicationclock.Clock) (*BlocklistService, error) {
	if port == nil {
		return nil, errors.New("blocklist port is required")
	}
	if clock == nil {
		return nil, errors.New("blocklist clock is required")
	}
	return &BlocklistService{port: port, clock: clock}, nil
}

// ListBlocklist returns committed entries in canonical order.
func (service *BlocklistService) ListBlocklist(ctx context.Context) (BlocklistDTO, error) {
	if ctx == nil {
		return BlocklistDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return BlocklistDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	state, err := service.port.Blocklist(ctx)
	if err != nil {
		return BlocklistDTO{}, portFailure(err)
	}
	state.Entries = append([]BlocklistStateDTO(nil), state.Entries...)
	sort.Slice(state.Entries, func(left, right int) bool {
		return security.CompareBlockEntries(state.Entries[left].Entry, state.Entries[right].Entry) < 0
	})
	if len(state.Entries) > MaxBlocklistEntries || (len(state.Entries) > 0 && state.PolicyVersion == 0) {
		return BlocklistDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	for index, entry := range state.Entries {
		if !validBlocklistState(entry) ||
			(index > 0 && security.CompareBlockEntries(state.Entries[index-1].Entry, entry.Entry) == 0) {
			return BlocklistDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
		}
	}
	if state.Entries == nil {
		state.Entries = []BlocklistStateDTO{}
	}
	return state, nil
}

// Block commits a block for one canonical MQTT connection subject.
func (service *BlocklistService) Block(ctx context.Context, entry security.BlockEntry) (BlocklistStatusDTO, error) {
	return service.changeBlocklist(ctx, entry, BlocklistActionBlock)
}

// Unblock commits removal of one canonical MQTT connection subject.
func (service *BlocklistService) Unblock(ctx context.Context, entry security.BlockEntry) (BlocklistStatusDTO, error) {
	return service.changeBlocklist(ctx, entry, BlocklistActionUnblock)
}

func (service *BlocklistService) changeBlocklist(
	ctx context.Context,
	entry security.BlockEntry,
	action BlocklistAction,
) (BlocklistStatusDTO, error) {
	if ctx == nil {
		return BlocklistStatusDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	canonical, err := security.NewBlockEntry(entry.Kind, entry.TenantID, entry.Value)
	if err != nil || canonical != entry {
		return BlocklistStatusDTO{}, newServiceError(ServiceErrorInvalidRequest, false)
	}
	if ctx.Err() != nil {
		return BlocklistStatusDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	proposedAt := service.clock.Now().UnixMilli()
	if proposedAt <= 0 {
		return BlocklistStatusDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	operationID, err := newOperationID()
	if err != nil {
		return BlocklistStatusDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	result, err := service.port.Commit(ctx, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: operationID,
		Blocklist: &BlocklistChangeDTO{
			Action: action, Entry: entry, ProposedAtUnixMillis: uint64(proposedAt),
		},
	})
	if err != nil {
		return BlocklistStatusDTO{}, portFailure(err)
	}
	if result.Outcome == ApplyOutcomeConflict {
		return BlocklistStatusDTO{}, conflictServiceError(result.Conflict)
	}
	status := result.Blocklist
	if result.Outcome != ApplyOutcomeApplied || result.Index == 0 || status == nil || status.Entry != entry {
		return BlocklistStatusDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	if (action == BlocklistActionBlock && (!status.Blocked || status.BlockedAtUnixMillis == 0 || status.PolicyVersion == 0)) ||
		(action == BlocklistActionUnblock && (status.Blocked || status.BlockedAtUnixMillis != 0)) {
		return BlocklistStatusDTO{}, newServiceError(ServiceErrorQuorumUnavailable, true)
	}
	return *status, nil
}
