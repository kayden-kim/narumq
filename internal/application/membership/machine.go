package membership

import (
	"bytes"
	"sort"
	"sync"
)

type sessionKey struct {
	tenantID string
	clientID string
}

type subscriptionKey struct {
	sessionKey
	filter string
}

type operationState struct {
	view MembershipOperationDTO
}

// StateMachine applies committed replicated state in strictly increasing order.
type StateMachine struct {
	mu               sync.RWMutex
	lastAppliedIndex uint64
	members          map[uint64]MemberIdentityDTO
	operations       map[string]operationState
	idempotency      map[string]string
	ownerships       map[sessionKey]OwnershipStateDTO
	subscriptions    map[subscriptionKey]SubscriptionStateDTO
	delivery         map[sessionKey]DeliverySessionStateDTO
	deliveryEntries  int
	deliveryBytes    int
	retained         map[retainedKey]RetainedStateDTO
	retainedBytes    int
	offline          map[sessionKey]OfflineSessionStateDTO
	offlineMessages  int
	offlineBytes     int
}

// NewStateMachine constructs an empty deterministic replicated-state machine.
func NewStateMachine() *StateMachine {
	return &StateMachine{
		members:       make(map[uint64]MemberIdentityDTO),
		operations:    make(map[string]operationState),
		idempotency:   make(map[string]string),
		ownerships:    make(map[sessionKey]OwnershipStateDTO),
		subscriptions: make(map[subscriptionKey]SubscriptionStateDTO),
		delivery:      make(map[sessionKey]DeliverySessionStateDTO),
		retained:      make(map[retainedKey]RetainedStateDTO),
		offline:       make(map[sessionKey]OfflineSessionStateDTO),
	}
}

// LastAppliedIndex returns the highest committed application index consumed.
func (machine *StateMachine) LastAppliedIndex() uint64 {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	return machine.lastAppliedIndex
}

// AdvanceAppliedIndex records committed Raft entries with no application event
// (for example, an election's empty entry). The driver calls this only after
// applying every command and configuration change through index.
func (machine *StateMachine) AdvanceAppliedIndex(index uint64) error {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if index == 0 || index < machine.lastAppliedIndex {
		return ErrInvalidIndex
	}
	machine.lastAppliedIndex = index
	return nil
}

// InitializeBootstrap installs the three configured initial voters after their
// bootstrap ConfChange entries have committed. It does not create fake operations.
func (machine *StateMachine) InitializeBootstrap(index uint64, members []MemberIdentityDTO) error {
	if index == 0 || len(members) != 3 {
		return ErrInvalidCommand
	}
	copied := make([]MemberIdentityDTO, len(members))
	for memberIndex, member := range members {
		if validateMemberIdentity(member) != nil || member.Phase != MemberPhaseVoter || member.OperationID != "" || (memberIndex > 0 && (members[memberIndex-1].MemberID >= member.MemberID || !compatibleMemberProfile(members[0], member))) {
			return ErrInvalidCommand
		}
		copied[memberIndex] = cloneMember(member)
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if machine.lastAppliedIndex != 0 || len(machine.members) != 0 || len(machine.operations) != 0 || len(machine.ownerships) != 0 || len(machine.subscriptions) != 0 || len(machine.delivery) != 0 || len(machine.retained) != 0 || len(machine.offline) != 0 {
		return ErrBootstrapAlreadyInitialized
	}
	for _, member := range copied {
		machine.members[member.MemberID] = member
	}
	machine.lastAppliedIndex = index
	return nil
}

// Apply applies one committed command. Structural errors leave all state untouched;
// deterministic domain conflicts consume the committed index and are returned as outcomes.
func (machine *StateMachine) Apply(index uint64, command CommandDTO) (ApplyResultDTO, error) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if index == 0 || index <= machine.lastAppliedIndex {
		return ApplyResultDTO{}, ErrInvalidIndex
	}
	if validateCommand(command) != nil {
		return ApplyResultDTO{}, ErrInvalidCommand
	}
	machine.lastAppliedIndex = index
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	switch {
	case command.Membership != nil:
		return machine.applyMembership(index, *command.Membership)
	case command.Ownership != nil:
		return machine.applyOwnership(index, *command.Ownership), nil
	case command.Subscription != nil:
		return machine.applySubscription(index, *command.Subscription), nil
	case command.Delivery != nil:
		return machine.applyDelivery(index, *command.Delivery), nil
	case command.Retained != nil:
		return machine.applyRetained(index, *command.Retained), nil
	case command.Offline != nil:
		return machine.applyOffline(index, *command.Offline), nil
	default:
		// validateCommand rejects an empty discriminated union before advancing.
		return result, nil
	}
}

// ApplyConfiguration applies a ConfChangeV2 only after its Raft configuration entry commits.
func (machine *StateMachine) ApplyConfiguration(index uint64, operationID string, action ConfigurationAction, memberID uint64) (ApplyResultDTO, error) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if index == 0 || index <= machine.lastAppliedIndex {
		return ApplyResultDTO{}, ErrInvalidIndex
	}
	if !validUUID(operationID) || memberID == 0 || action < ConfigurationActionAddLearner || action > ConfigurationActionRemoveMember {
		return ApplyResultDTO{}, ErrInvalidCommand
	}
	machine.lastAppliedIndex = index
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	operation, exists := machine.operations[operationID]
	if !exists {
		return conflictResult(index, ConflictOperationNotFound), nil
	}
	record := operation.view.Record
	if record.Member == nil || record.Member.MemberID != memberID {
		return conflictResult(index, ConflictConfigurationMismatch), nil
	}
	member, memberExists := machine.members[memberID]
	if !memberExists || member.OperationID != operationID {
		return conflictResult(index, ConflictConfigurationMismatch), nil
	}
	next := cloneMember(member)
	nextRecord := cloneMembershipRecord(record)
	switch action {
	case ConfigurationActionAddLearner:
		if operation.view.Action != MembershipActionStageJoin || member.Phase != MemberPhaseStaged || record.Action != MembershipActionStageJoin {
			return conflictResult(index, ConflictConfigurationMismatch), nil
		}
		next.Phase = MemberPhaseLearner
		nextRecord.Action = MembershipActionAddLearner
		nextRecord.Member = new(cloneMember(next))
		operation.view.Phase = OperationPhaseLearner
	case ConfigurationActionPromoteVoter:
		if operation.view.Action != MembershipActionAddLearner || member.Phase != MemberPhaseCatchingUp || record.Action != MembershipActionAddLearner {
			return conflictResult(index, ConflictConfigurationMismatch), nil
		}
		next.Phase = MemberPhaseVoter
		next.OperationID = ""
		nextRecord.Action = MembershipActionPromoteVoter
		nextRecord.Member = new(cloneMember(next))
		operation.view.Phase = OperationPhaseSucceeded
	case ConfigurationActionRemoveMember:
		if operation.view.Action != MembershipActionBeginRemove || member.Phase != MemberPhaseLeaving || record.Action != MembershipActionBeginRemove || record.TargetMemberID != memberID {
			return conflictResult(index, ConflictConfigurationMismatch), nil
		}
		next.Phase = MemberPhaseRetired
		next.OperationID = ""
		nextRecord.Action = MembershipActionRemoveMember
		nextRecord.Member = new(cloneMember(next))
		operation.view.Phase = OperationPhaseSucceeded
	}
	nextRecord.CommittedIndex = index
	machine.members[memberID] = next
	operation.view.Action = nextRecord.Action
	operation.view.MemberID = memberID
	operation.view.CommittedIndex = index
	operation.view.Record = nextRecord
	machine.operations[operationID] = operation
	result.Operation = new(cloneOperation(operation.view))
	return result, nil
}

func (machine *StateMachine) applyMembership(index uint64, record MembershipRecordDTO) (ApplyResultDTO, error) {
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	switch record.Action {
	case MembershipActionStageJoin:
		return machine.stageJoin(index, record), nil
	case MembershipActionBeginRemove:
		return machine.beginRemove(index, record), nil
	case MembershipActionAddLearner:
		return machine.markCatchingUp(index, record), nil
	case MembershipActionAbortStage:
		return machine.abortStage(index, record), nil
	case MembershipActionPromoteVoter, MembershipActionRemoveMember:
		return conflictResult(index, ConflictConfigurationMismatch), nil
	default:
		return result, nil
	}
}

func (machine *StateMachine) stageJoin(index uint64, record MembershipRecordDTO) ApplyResultDTO {
	member := *record.Member
	if operationID, exists := machine.idempotency[record.IdempotencyKey]; exists {
		if existing, ok := machine.operations[operationID]; ok && sameJoinRequest(existing.view, record) {
			return operationResult(index, existing.view)
		}
		return conflictResult(index, ConflictIdempotency)
	}
	if existing, exists := machine.operations[record.OperationID]; exists {
		if sameJoinRequest(existing.view, record) {
			return operationResult(index, existing.view)
		}
		return conflictResult(index, ConflictOperationID)
	}
	if _, exists := machine.members[member.MemberID]; exists {
		return conflictResult(index, ConflictMemberIDReserved)
	}
	for _, committed := range machine.members {
		if !compatibleMemberProfile(member, committed) {
			return conflictResult(index, ConflictCompatibilityUnsupported)
		}
	}
	if machine.hasInProgressOperation() {
		return conflictResult(index, ConflictMembershipInProgress)
	}
	member = cloneMember(member)
	recordCopy := cloneMembershipRecord(record)
	recordCopy.CommittedIndex = index
	machine.members[member.MemberID] = member
	operation := MembershipOperationDTO{
		OperationID:    record.OperationID,
		Action:         MembershipActionStageJoin,
		MemberID:       member.MemberID,
		Phase:          OperationPhaseStaged,
		CommittedIndex: index,
		Record:         recordCopy,
	}
	machine.operations[record.OperationID] = operationState{view: operation}
	machine.idempotency[record.IdempotencyKey] = record.OperationID
	return operationResult(index, operation)
}

func (machine *StateMachine) beginRemove(index uint64, record MembershipRecordDTO) ApplyResultDTO {
	requested := *record.Member
	if operationID, exists := machine.idempotency[record.IdempotencyKey]; exists {
		if existing, ok := machine.operations[operationID]; ok && sameRemoveRequest(existing.view, record) {
			return operationResult(index, existing.view)
		}
		return conflictResult(index, ConflictIdempotency)
	}
	if existing, exists := machine.operations[record.OperationID]; exists {
		if sameRemoveRequest(existing.view, record) {
			return operationResult(index, existing.view)
		}
		return conflictResult(index, ConflictOperationID)
	}
	member, exists := machine.members[record.TargetMemberID]
	if !exists {
		return conflictResult(index, ConflictMemberNotFound)
	}
	if !sameMemberIdentity(member, requested) {
		return conflictResult(index, ConflictMemberIdentity)
	}
	if member.Phase != MemberPhaseVoter || member.OperationID != "" {
		return conflictResult(index, ConflictPhase)
	}
	if machine.hasInProgressOperation() {
		return conflictResult(index, ConflictMembershipInProgress)
	}
	member.Phase = MemberPhaseLeaving
	member.OperationID = record.OperationID
	recordCopy := cloneMembershipRecord(record)
	recordCopy.CommittedIndex = index
	recordCopy.Member = new(cloneMember(member))
	machine.members[member.MemberID] = cloneMember(member)
	operation := MembershipOperationDTO{
		OperationID: record.OperationID, Action: MembershipActionBeginRemove,
		MemberID: member.MemberID, Phase: OperationPhaseRemoving,
		CommittedIndex: index, Record: recordCopy,
	}
	machine.operations[record.OperationID] = operationState{view: operation}
	machine.idempotency[record.IdempotencyKey] = record.OperationID
	return operationResult(index, operation)
}

func (machine *StateMachine) markCatchingUp(index uint64, record MembershipRecordDTO) ApplyResultDTO {
	operation, exists := machine.operations[record.OperationID]
	if !exists || operation.view.Action != MembershipActionAddLearner || operation.view.MemberID != record.Member.MemberID || machine.idempotency[record.IdempotencyKey] != record.OperationID {
		return conflictResult(index, ConflictConfigurationMismatch)
	}
	member := machine.members[record.Member.MemberID]
	if !sameMemberIdentity(member, *record.Member) {
		return conflictResult(index, ConflictMemberIdentity)
	}
	if member.Phase == MemberPhaseCatchingUp && record.Action == MembershipActionAddLearner {
		return operationResult(index, operation.view)
	}
	if member.Phase != MemberPhaseLearner || member.OperationID != record.OperationID || record.Member.Phase != MemberPhaseCatchingUp || operation.view.Record.Action != MembershipActionAddLearner {
		return conflictResult(index, ConflictPhase)
	}
	member.Phase = MemberPhaseCatchingUp
	machine.members[member.MemberID] = cloneMember(member)
	recordCopy := cloneMembershipRecord(record)
	recordCopy.CommittedIndex = index
	recordCopy.Member = new(cloneMember(member))
	operation.view.Action = MembershipActionAddLearner
	operation.view.Phase = OperationPhaseCatchingUp
	operation.view.CommittedIndex = index
	operation.view.Record = recordCopy
	machine.operations[record.OperationID] = operation
	return operationResult(index, operation.view)
}

func (machine *StateMachine) abortStage(index uint64, record MembershipRecordDTO) ApplyResultDTO {
	operation, exists := machine.operations[record.OperationID]
	if !exists || operation.view.Action != MembershipActionStageJoin || operation.view.MemberID != record.Member.MemberID || machine.idempotency[record.IdempotencyKey] != record.OperationID {
		return conflictResult(index, ConflictConfigurationMismatch)
	}
	member, memberExists := machine.members[record.Member.MemberID]
	if !memberExists || !sameMemberIdentity(member, *record.Member) {
		return conflictResult(index, ConflictMemberIdentity)
	}
	if operation.view.Phase == OperationPhaseAborted && member.Phase == MemberPhaseRetired {
		return operationResult(index, operation.view)
	}
	if member.Phase != MemberPhaseStaged || member.OperationID != record.OperationID || record.Member.Phase != MemberPhaseRetired {
		return conflictResult(index, ConflictPhase)
	}
	member.Phase = MemberPhaseRetired
	member.OperationID = ""
	machine.members[member.MemberID] = cloneMember(member)
	recordCopy := cloneMembershipRecord(record)
	recordCopy.CommittedIndex = index
	recordCopy.Member = new(cloneMember(member))
	operation.view.Action = MembershipActionAbortStage
	operation.view.Phase = OperationPhaseAborted
	operation.view.CommittedIndex = index
	operation.view.Record = recordCopy
	machine.operations[record.OperationID] = operation
	return operationResult(index, operation.view)
}

func (machine *StateMachine) applyOwnership(index uint64, change OwnershipChangeDTO) ApplyResultDTO {
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	key := sessionKey{tenantID: change.TenantID, clientID: change.ClientID}
	current, exists := machine.ownerships[key]
	switch change.Action {
	case OwnershipActionAcquire:
		if !exists {
			if change.ExpectedEpoch != 0 || change.Epoch != 1 {
				return conflictResult(index, ConflictStaleEpoch)
			}
		} else if current.Epoch != change.ExpectedEpoch || current.Epoch == ^uint64(0) || change.Epoch != current.Epoch+1 {
			return conflictResult(index, ConflictStaleEpoch)
		}
	case OwnershipActionTransfer:
		if !exists {
			return conflictResult(index, ConflictOwnershipMissing)
		}
		if current.Epoch != change.ExpectedEpoch || current.Epoch == ^uint64(0) || change.Epoch != current.Epoch+1 || current.OwnerNodeID == change.OwnerNodeID {
			return conflictResult(index, ConflictStaleEpoch)
		}
	case OwnershipActionRelease:
		if !exists {
			return conflictResult(index, ConflictOwnershipMissing)
		}
		if current.Epoch != change.ExpectedEpoch || change.Epoch != current.Epoch || current.OwnerNodeID != change.OwnerNodeID {
			return conflictResult(index, ConflictStaleEpoch)
		}
		delete(machine.ownerships, key)
		machine.removeDeliveryLocked(key)
		machine.removeSubscriptionsLocked(key)
		machine.removeOfflineLocked(key)
		return result
	}
	state := OwnershipStateDTO{TenantID: change.TenantID, ClientID: change.ClientID, OwnerNodeID: change.OwnerNodeID, Epoch: change.Epoch, CommittedIndex: index}
	machine.ownerships[key] = state
	// Persistent subscriptions follow the committed owner epoch so their route
	// projection cannot continue to target a fenced owner.
	for subscriptionID, subscription := range machine.subscriptions {
		if subscriptionID.sessionKey != key {
			continue
		}
		subscription.OwnerNodeID = change.OwnerNodeID
		subscription.Epoch = change.Epoch
		machine.subscriptions[subscriptionID] = subscription
	}
	result.Ownership = new(state)
	return result
}

func (machine *StateMachine) applySubscription(index uint64, change SubscriptionChangeDTO) ApplyResultDTO {
	key := sessionKey{tenantID: change.TenantID, clientID: change.ClientID}
	if !machine.authorized(key, change.OwnerNodeID, change.Epoch) {
		return conflictResult(index, ConflictStaleEpoch)
	}
	subscriptionID := subscriptionKey{sessionKey: key, filter: change.Filter}
	result := ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied}
	if change.Action == SubscriptionActionRemove {
		delete(machine.subscriptions, subscriptionID)
		return result
	}
	state := SubscriptionStateDTO{
		TenantID: change.TenantID, ClientID: change.ClientID, OwnerNodeID: change.OwnerNodeID,
		Epoch: change.Epoch, Filter: change.Filter, QoS: change.QoS, NoLocal: change.NoLocal,
		RetainAsPublished: change.RetainAsPublished, RetainHandling: change.RetainHandling,
	}
	machine.subscriptions[subscriptionID] = state
	machine.ensureDeliveryRecordLocked(key)
	result.Subscription = new(state)
	return result
}

func (machine *StateMachine) authorized(key sessionKey, ownerNodeID, epoch uint64) bool {
	current, exists := machine.ownerships[key]
	return exists && current.OwnerNodeID == ownerNodeID && current.Epoch == epoch
}

func (machine *StateMachine) hasInProgressOperation() bool {
	for _, operation := range machine.operations {
		if operation.view.Phase != OperationPhaseSucceeded && operation.view.Phase != OperationPhaseAborted {
			return true
		}
	}
	return false
}

// Members returns a member-ID-sorted deep copy of the committed identities.
func (machine *StateMachine) Members() []MemberIdentityDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	members := make([]MemberIdentityDTO, 0, len(machine.members))
	for _, member := range machine.members {
		members = append(members, cloneMember(member))
	}
	sort.Slice(members, func(left, right int) bool { return members[left].MemberID < members[right].MemberID })
	return members
}

// GetOperation returns the current committed history record for an operation ID.
func (machine *StateMachine) GetOperation(operationID string) (MembershipOperationDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	operation, exists := machine.operations[operationID]
	if !exists {
		return MembershipOperationDTO{}, false
	}
	return cloneOperation(operation.view), true
}

// GetOperationByIdempotencyKey returns the committed operation bound to one retry key.
func (machine *StateMachine) GetOperationByIdempotencyKey(idempotencyKey string) (MembershipOperationDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	operationID, exists := machine.idempotency[idempotencyKey]
	if !exists {
		return MembershipOperationDTO{}, false
	}
	operation, exists := machine.operations[operationID]
	if !exists {
		return MembershipOperationDTO{}, false
	}
	return cloneOperation(operation.view), true
}

// Operations returns an operation-ID-sorted deep copy of current history records.
func (machine *StateMachine) Operations() []MembershipOperationDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	operations := make([]MembershipOperationDTO, 0, len(machine.operations))
	for _, operation := range machine.operations {
		operations = append(operations, cloneOperation(operation.view))
	}
	sort.Slice(operations, func(left, right int) bool { return operations[left].OperationID < operations[right].OperationID })
	return operations
}

// Ownerships returns a tenant/client-sorted deep copy of ownership records.
func (machine *StateMachine) Ownerships() []OwnershipStateDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	ownerships := make([]OwnershipStateDTO, 0, len(machine.ownerships))
	for _, ownership := range machine.ownerships {
		ownerships = append(ownerships, ownership)
	}
	sort.Slice(ownerships, func(left, right int) bool {
		return compareSession(ownerships[left].TenantID, ownerships[left].ClientID, ownerships[right].TenantID, ownerships[right].ClientID) < 0
	})
	return ownerships
}

// GetOwnership returns the current ownership record for a tenant and client.
func (machine *StateMachine) GetOwnership(tenantID, clientID string) (OwnershipStateDTO, bool) {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	ownership, exists := machine.ownerships[sessionKey{tenantID: tenantID, clientID: clientID}]
	return ownership, exists
}

// Subscriptions returns a tenant/client/filter-sorted deep copy of persistent routes.
func (machine *StateMachine) Subscriptions() []SubscriptionStateDTO {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	subscriptions := make([]SubscriptionStateDTO, 0, len(machine.subscriptions))
	for _, subscription := range machine.subscriptions {
		subscriptions = append(subscriptions, subscription)
	}
	sort.Slice(subscriptions, func(left, right int) bool { return compareSubscription(subscriptions[left], subscriptions[right]) < 0 })
	return subscriptions
}

func operationResult(index uint64, operation MembershipOperationDTO) ApplyResultDTO {
	return ApplyResultDTO{Index: index, Outcome: ApplyOutcomeApplied, Operation: new(cloneOperation(operation))}
}

func conflictResult(index uint64, conflict ApplyConflictCode) ApplyResultDTO {
	return ApplyResultDTO{Index: index, Outcome: ApplyOutcomeConflict, Conflict: conflict}
}

func sameJoinRequest(operation MembershipOperationDTO, request MembershipRecordDTO) bool {
	if operation.Action == MembershipActionBeginRemove || operation.Record.TargetMemberID != 0 || operation.Record.Member == nil || request.Member == nil {
		return false
	}
	return validJoinAction(operation.Record.Action) && sameMemberIdentity(*operation.Record.Member, *request.Member)
}

func sameRemoveRequest(operation MembershipOperationDTO, request MembershipRecordDTO) bool {
	if operation.Record.TargetMemberID == 0 || operation.Record.TargetMemberID != request.TargetMemberID || operation.Record.Member == nil || request.Member == nil {
		return false
	}
	return validRemoveAction(operation.Record.Action) && sameMemberIdentity(*operation.Record.Member, *request.Member)
}

func validJoinAction(action MembershipAction) bool {
	return action == MembershipActionStageJoin || action == MembershipActionAddLearner || action == MembershipActionPromoteVoter || action == MembershipActionAbortStage
}

func validRemoveAction(action MembershipAction) bool {
	return action == MembershipActionBeginRemove || action == MembershipActionRemoveMember
}

func sameMemberIdentity(left, right MemberIdentityDTO) bool {
	return left.MemberID == right.MemberID && left.ClusterID == right.ClusterID && left.Address == right.Address && left.DNSSAN == right.DNSSAN && bytes.Equal(left.CertificateSHA256, right.CertificateSHA256) && left.ReleaseVersion == right.ReleaseVersion && left.ClusterProtocolMajor == right.ClusterProtocolMajor && left.ClusterProtocolMinor == right.ClusterProtocolMinor && left.StateSchemaMajor == right.StateSchemaMajor && left.StateSchemaMinor == right.StateSchemaMinor
}

func cloneMember(member MemberIdentityDTO) MemberIdentityDTO {
	member.CertificateSHA256 = bytes.Clone(member.CertificateSHA256)
	return member
}

func cloneMembershipRecord(record MembershipRecordDTO) MembershipRecordDTO {
	if record.Member != nil {
		member := cloneMember(*record.Member)
		record.Member = &member
	}
	return record
}

func cloneOperation(operation MembershipOperationDTO) MembershipOperationDTO {
	operation.Record = cloneMembershipRecord(operation.Record)
	return operation
}
