package membership

import (
	"sort"

	"narumq/internal/domain/release"
	"narumq/internal/domain/security"
)

func validateBlocklistChange(change BlocklistChangeDTO) error {
	if change.ProposedAtUnixMillis == 0 ||
		(change.Action != BlocklistActionBlock && change.Action != BlocklistActionUnblock) {
		return ErrInvalidCommand
	}
	entry, err := security.NewBlockEntry(change.Entry.Kind, change.Entry.TenantID, change.Entry.Value)
	if err != nil || entry != change.Entry {
		return ErrInvalidCommand
	}
	return nil
}

func validBlocklistState(state BlocklistStateDTO) bool {
	entry, err := security.NewBlockEntry(state.Entry.Kind, state.Entry.TenantID, state.Entry.Value)
	return err == nil && entry == state.Entry && state.BlockedAtUnixMillis > 0
}

func (machine *StateMachine) applyBlocklist(index uint64, change BlocklistChangeDTO) ApplyResultDTO {
	if !release.Contains(machine.activeFeatures, ClientBlocklistFeature) {
		return conflictResult(index, ConflictFeatureInactive)
	}
	status := &BlocklistStatusDTO{Entry: change.Entry}
	switch change.Action {
	case BlocklistActionBlock:
		state, exists := machine.blocklist[change.Entry]
		if !exists {
			if len(machine.blocklist) >= MaxBlocklistEntries {
				return conflictResult(index, ConflictBlocklistCapacity)
			}
			state = BlocklistStateDTO{Entry: change.Entry, BlockedAtUnixMillis: change.ProposedAtUnixMillis}
			machine.blocklist[change.Entry] = state
			machine.blocklistIndex.Set(change.Entry, true)
			machine.blocklistVersion = index
		}
		status.Blocked = true
		status.BlockedAtUnixMillis = state.BlockedAtUnixMillis
	case BlocklistActionUnblock:
		if _, exists := machine.blocklist[change.Entry]; exists {
			delete(machine.blocklist, change.Entry)
			machine.blocklistIndex.Set(change.Entry, false)
			machine.blocklistVersion = index
		}
	}
	status.PolicyVersion = machine.blocklistVersion
	return ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied, Blocklist: status}
}

// Blocklist returns the committed blocklist in canonical entry order.
func (machine *StateMachine) Blocklist() BlocklistDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	entries := make([]BlocklistStateDTO, 0, len(machine.blocklist))
	for _, state := range machine.blocklist {
		entries = append(entries, state)
	}
	sort.Slice(entries, func(left, right int) bool {
		return security.CompareBlockEntries(entries[left].Entry, entries[right].Entry) < 0
	})
	return BlocklistDTO{PolicyVersion: machine.blocklistVersion, Entries: entries}
}

// BlocklistEntries returns the blocked entries in canonical order.
func (machine *StateMachine) BlocklistEntries() []security.BlockEntry {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	entries := make([]security.BlockEntry, 0, len(machine.blocklist))
	for entry := range machine.blocklist {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(left, right int) bool {
		return security.CompareBlockEntries(entries[left], entries[right]) < 0
	})
	return entries
}

// IsBlocked reports whether one canonical entry is committed as blocked.
func (machine *StateMachine) IsBlocked(entry security.BlockEntry) bool {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	_, blocked := machine.blocklist[entry]
	return blocked
}

// MatchBlocklist returns the highest-precedence blocked subject kind, if any.
func (machine *StateMachine) MatchBlocklist(subject security.BlockSubject) (security.BlockKind, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	return machine.blocklistIndex.Match(subject)
}
