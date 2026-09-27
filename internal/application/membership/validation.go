package membership

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"unicode/utf8"

	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

var (
	// ErrInvalidIndex means a committed application index is zero or not increasing.
	ErrInvalidIndex = errors.New("invalid committed index")
	// ErrInvalidCommand means a command is malformed or violates a structural invariant.
	ErrInvalidCommand = errors.New("invalid replicated command")
	// ErrInvalidSnapshot means a snapshot is malformed, noncanonical, or internally inconsistent.
	ErrInvalidSnapshot = errors.New("invalid replicated snapshot")
	// ErrSnapshotIndexMismatch means the snapshot index cannot safely replace current machine state.
	ErrSnapshotIndexMismatch = errors.New("snapshot index does not match machine state")
	// ErrBootstrapAlreadyInitialized means bootstrap membership can only initialize an empty machine.
	ErrBootstrapAlreadyInitialized = errors.New("bootstrap membership is already initialized")
)

const (
	maxClusterIDBytes      = 200
	maxAddressBytes        = 255
	maxDNSNameBytes        = 253
	maxReleaseVersionBytes = 64
	maxTenantIDBytes       = 128
	maxClientIDBytes       = 256
	maxTopicFilterBytes    = 65535
	maxPublishPayloadBytes = 1_000_000
	maxSnapshotEntries     = 1_000_000
)

func validateCommand(command CommandDTO) error {
	if command.SchemaMajor != SchemaMajor || command.SchemaMinor != SchemaMinor || !validUUID(command.OperationID) {
		return ErrInvalidCommand
	}
	payloadCount := 0
	if command.Membership != nil {
		payloadCount++
	}
	if command.Ownership != nil {
		payloadCount++
	}
	if command.Subscription != nil {
		payloadCount++
	}
	if command.Delivery != nil {
		payloadCount++
	}
	if command.Retained != nil {
		payloadCount++
	}
	if command.Offline != nil {
		payloadCount++
	}
	if payloadCount != 1 {
		return ErrInvalidCommand
	}
	switch {
	case command.Membership != nil:
		if command.Membership.OperationID != command.OperationID || command.Membership.CommittedIndex != 0 || validateMembershipRecord(*command.Membership) != nil {
			return ErrInvalidCommand
		}
	case command.Ownership != nil:
		if validateOwnershipChange(*command.Ownership) != nil {
			return ErrInvalidCommand
		}
	case command.Subscription != nil:
		if validateSubscriptionChange(*command.Subscription) != nil {
			return ErrInvalidCommand
		}
	case command.Delivery != nil:
		if validateDelivery(*command.Delivery) != nil {
			return ErrInvalidCommand
		}
	case command.Retained != nil:
		if validateRetainedChange(*command.Retained) != nil {
			return ErrInvalidCommand
		}
	case command.Offline != nil:
		if validateOfflineChange(*command.Offline) != nil {
			return ErrInvalidCommand
		}
	}
	return nil
}
func validateOfflineChange(change OfflineChangeDTO) error {
	if !validSessionIdentity(change.TenantID, change.ClientID) || change.Epoch == 0 || change.ProposedAtUnixMillis == 0 {
		return ErrInvalidCommand
	}
	for _, msg := range change.Messages {
		if msg.Sequence != 0 || !validTopic(msg.Topic) || msg.QoS != 0 || len(msg.Payload) > MaxOfflinePayloadBytes {
			return ErrInvalidCommand
		}
	}
	switch change.Action {
	case OfflineActionDetach:
		if change.OwnerNodeID == 0 || len(change.Messages) > MaxOfflineMessagesPerSession {
			return ErrInvalidCommand
		}
		totalBytes := 0
		for _, msg := range change.Messages {
			totalBytes += len(msg.Topic) + len(msg.Payload)
		}
		if totalBytes > MaxOfflineDetachMessageBytes {
			return ErrInvalidCommand
		}
	case OfflineActionEnqueue:
		if change.OwnerNodeID != 0 || len(change.Messages) != 1 || change.SessionExpiresAtUnixMillis != 0 {
			return ErrInvalidCommand
		}
	case OfflineActionClaim:
		if change.OwnerNodeID == 0 || len(change.Messages) != 0 || change.SessionExpiresAtUnixMillis != 0 {
			return ErrInvalidCommand
		}
	default:
		return ErrInvalidCommand
	}
	return nil
}

func validateMembershipRecord(record MembershipRecordDTO) error {
	if !validUUID(record.OperationID) || record.Action < MembershipActionStageJoin || record.Action > MembershipActionAbortStage || !validIdempotencyKey(record.IdempotencyKey) {
		return ErrInvalidCommand
	}
	if record.Member != nil && validateMemberIdentity(*record.Member) != nil {
		return ErrInvalidCommand
	}
	if record.TargetMemberID != 0 && record.Action != MembershipActionBeginRemove && record.Action != MembershipActionRemoveMember {
		return ErrInvalidCommand
	}
	switch record.Action {
	case MembershipActionStageJoin:
		if record.Member == nil || record.Member.Phase != MemberPhaseStaged || record.Member.OperationID != record.OperationID || record.TargetMemberID != 0 {
			return ErrInvalidCommand
		}
	case MembershipActionBeginRemove:
		if record.TargetMemberID == 0 || record.Member == nil || record.Member.MemberID != record.TargetMemberID || record.Member.Phase != MemberPhaseLeaving || record.Member.OperationID != record.OperationID {
			return ErrInvalidCommand
		}
	case MembershipActionRemoveMember:
		if record.TargetMemberID == 0 || record.Member == nil || record.Member.MemberID != record.TargetMemberID || record.Member.Phase != MemberPhaseRetired || record.Member.OperationID != "" {
			return ErrInvalidCommand
		}
	case MembershipActionAddLearner:
		if record.TargetMemberID != 0 || record.Member == nil || (record.Member.Phase != MemberPhaseLearner && record.Member.Phase != MemberPhaseCatchingUp) || record.Member.OperationID != record.OperationID {
			return ErrInvalidCommand
		}
	case MembershipActionPromoteVoter:
		if record.TargetMemberID != 0 || record.Member == nil || record.Member.Phase != MemberPhaseVoter || record.Member.OperationID != "" {
			return ErrInvalidCommand
		}
	case MembershipActionAbortStage:
		if record.TargetMemberID != 0 || record.Member == nil || record.Member.Phase != MemberPhaseRetired || record.Member.OperationID != "" {
			return ErrInvalidCommand
		}
	}
	return nil
}

func validateMemberIdentity(member MemberIdentityDTO) error {
	if member.MemberID == 0 || !boundedUTF8(member.ClusterID, 1, maxClusterIDBytes) || !boundedUTF8(member.Address, 1, maxAddressBytes) || !validAddress(member.Address) || !validDNSName(member.DNSSAN) || len(member.CertificateSHA256) != 32 || !boundedUTF8(member.ReleaseVersion, 1, maxReleaseVersionBytes) {
		return ErrInvalidCommand
	}
	if member.ClusterProtocolMajor != 2 || member.ClusterProtocolMinor != 0 || member.StateSchemaMajor != SchemaMajor || member.StateSchemaMinor != SchemaMinor || member.Phase < MemberPhaseStaged || member.Phase > MemberPhaseRetired {
		return ErrInvalidCommand
	}
	if member.Phase == MemberPhaseStaged || member.Phase == MemberPhaseLearner || member.Phase == MemberPhaseCatchingUp || member.Phase == MemberPhaseLeaving {
		if !validUUID(member.OperationID) {
			return ErrInvalidCommand
		}
	} else if member.OperationID != "" {
		return ErrInvalidCommand
	}
	return nil
}

func validateOwnershipChange(change OwnershipChangeDTO) error {
	if !validSessionIdentity(change.TenantID, change.ClientID) || change.OwnerNodeID == 0 || change.Epoch == 0 || change.Action < OwnershipActionAcquire || change.Action > OwnershipActionRelease {
		return ErrInvalidCommand
	}
	return nil
}

func validateSubscriptionChange(change SubscriptionChangeDTO) error {
	if !validSessionIdentity(change.TenantID, change.ClientID) || change.OwnerNodeID == 0 || change.Epoch == 0 || !validTopicFilter(change.Filter) || change.QoS > 2 || change.RetainHandling > 2 || change.Action < SubscriptionActionAdd || change.Action > SubscriptionActionRemove {
		return ErrInvalidCommand
	}
	return nil
}

func validateDelivery(change DeliveryChangeDTO) error {
	if !validSessionIdentity(change.TenantID, change.ClientID) || change.Action < DeliveryActionPublish || change.Action > DeliveryActionDiscard || change.ProposedAtUnixMillis == 0 {
		return ErrInvalidCommand
	}
	validTargets := func() bool {
		if len(change.Targets) > MaxDeliveryTargets {
			return false
		}
		seen := make(map[string]struct{}, len(change.Targets))
		for _, target := range change.Targets {
			if !validSessionIdentity(change.TenantID, target.ClientID) {
				return false
			}
			if _, exists := seen[target.ClientID]; exists {
				return false
			}
			seen[target.ClientID] = struct{}{}
		}
		return true
	}
	noPublisherFence := change.OwnerNodeID == 0 && change.Epoch == 0
	fenced := change.OwnerNodeID > 0 && change.Epoch > 0
	switch change.Action {
	case DeliveryActionPublish:
		if !noPublisherFence || !validTopic(change.Topic) || len(change.Payload) > MaxDeliveryPayloadBytes || !validTargets() || change.Sequence != 0 || change.PacketID != 0 || change.Reset || change.SessionExpiresAtUnixMillis != 0 || change.PublishQoS < 1 || change.PublishQoS > 2 {
			return ErrInvalidCommand
		}
	case DeliveryActionReceive:
		if !fenced || !validTopic(change.Topic) || len(change.Payload) > MaxDeliveryPayloadBytes || change.PacketID == 0 || change.PacketID > 65535 || change.Sequence != 0 || len(change.Targets) != 0 || change.Reset || change.SessionExpiresAtUnixMillis != 0 || change.PublishQoS != 0 {
			return ErrInvalidCommand
		}
	case DeliveryActionRelease:
		if !fenced || change.PacketID == 0 || change.PacketID > 65535 || !validTargets() || change.Topic != "" || len(change.Payload) != 0 || change.Retain || change.ExpiresAtUnixMillis != 0 || change.Sequence != 0 || change.Reset || change.SessionExpiresAtUnixMillis != 0 || change.PublishQoS != 2 {
			return ErrInvalidCommand
		}
	case DeliveryActionSend:
		if !fenced || change.Sequence == 0 || change.PacketID == 0 || change.PacketID > 65535 || !deliveryZeroContent(change) {
			return ErrInvalidCommand
		}
	case DeliveryActionAdvance, DeliveryActionComplete:
		if !fenced || change.PacketID == 0 || change.PacketID > 65535 || change.Sequence != 0 || !deliveryZeroContent(change) {
			return ErrInvalidCommand
		}
	case DeliveryActionAttach:
		if !fenced || change.Reset && (change.Topic != "" || len(change.Payload) != 0) || !deliveryZeroContentExceptReset(change) {
			return ErrInvalidCommand
		}
	case DeliveryActionDetach:
		if !fenced || !deliveryZeroContentExceptSessionExpiry(change) {
			return ErrInvalidCommand
		}
	case DeliveryActionDiscard:
		if !fenced || change.PacketID != 0 || change.Sequence != 0 || !deliveryZeroContent(change) {
			return ErrInvalidCommand
		}
	}
	return nil
}

func deliveryZeroContent(change DeliveryChangeDTO) bool {
	return change.Topic == "" && len(change.Payload) == 0 && !change.Retain && change.ExpiresAtUnixMillis == 0 && len(change.Targets) == 0 && !change.Reset && change.SessionExpiresAtUnixMillis == 0 && change.PublishQoS == 0
}

func deliveryZeroContentExceptReset(change DeliveryChangeDTO) bool {
	return change.Topic == "" && len(change.Payload) == 0 && !change.Retain && change.ExpiresAtUnixMillis == 0 && len(change.Targets) == 0 && change.PacketID == 0 && change.Sequence == 0 && change.SessionExpiresAtUnixMillis == 0 && change.PublishQoS == 0
}

func deliveryZeroContentExceptSessionExpiry(change DeliveryChangeDTO) bool {
	return change.Topic == "" && len(change.Payload) == 0 && !change.Retain && change.ExpiresAtUnixMillis == 0 && len(change.Targets) == 0 && change.PacketID == 0 && change.Sequence == 0 && !change.Reset && change.PublishQoS == 0
}

func validateRetainedChange(change RetainedChangeDTO) error {
	if !validSessionIdentity(change.TenantID, change.PublisherClientID) || !validTopic(change.Topic) || change.ProposedAtUnixMillis == 0 {
		return ErrInvalidCommand
	}
	switch change.Action {
	case RetainedActionReplace:
		if len(change.Payload) == 0 || len(change.Payload) > MaxRetainedPayloadBytes || change.QoS > 2 {
			return ErrInvalidCommand
		}
	case RetainedActionDelete:
		if len(change.Payload) != 0 || change.QoS != 0 || change.ExpiresAtUnixMillis != 0 {
			return ErrInvalidCommand
		}
	default:
		return ErrInvalidCommand
	}
	return nil
}

func validRetainedState(state RetainedStateDTO) bool {
	return validSessionIdentity(state.TenantID, state.PublisherClientID) &&
		validTopic(state.Topic) &&
		len(state.Payload) > 0 && len(state.Payload) <= MaxRetainedPayloadBytes &&
		state.QoS <= 2
}

func validSnapshot(snapshot SnapshotDTO) error {
	if snapshot.SchemaMajor != SchemaMajor || snapshot.SchemaMinor != SchemaMinor || (len(snapshot.Digest) != 0 && len(snapshot.Digest) != 32) {
		return ErrInvalidSnapshot
	}
	offlineMessagesCount := 0
	for _, session := range snapshot.OfflineSessions {
		offlineMessagesCount += len(session.Messages)
	}
	deliveryObjects := len(snapshot.DeliverySessions)
	for _, session := range snapshot.DeliverySessions {
		deliveryObjects += len(session.Messages) + len(session.Inbound)
	}
	count := len(snapshot.Members) + len(snapshot.Ownerships) + len(snapshot.Subscriptions) + deliveryObjects + len(snapshot.MembershipHistory) + len(snapshot.Retained) + len(snapshot.OfflineSessions) + offlineMessagesCount
	if count > maxSnapshotEntries || snapshot.ObjectCount != uint64(count) || (count > 0 && snapshot.LastIncludedIndex == 0) || len(snapshot.Retained) > MaxRetainedMessages {
		return ErrInvalidSnapshot
	}
	for index, member := range snapshot.Members {
		if validateMemberIdentity(member) != nil || (index > 0 && (snapshot.Members[index-1].MemberID >= member.MemberID || !compatibleMemberProfile(snapshot.Members[0], member))) {
			return ErrInvalidSnapshot
		}
	}
	ownershipEpochs := make(map[sessionKey]uint64, len(snapshot.Ownerships))
	for index, ownership := range snapshot.Ownerships {
		if !validSessionIdentity(ownership.TenantID, ownership.ClientID) || ownership.OwnerNodeID == 0 || ownership.Epoch == 0 || ownership.CommittedIndex == 0 || ownership.CommittedIndex > snapshot.LastIncludedIndex || (index > 0 && compareSession(snapshot.Ownerships[index-1].TenantID, snapshot.Ownerships[index-1].ClientID, ownership.TenantID, ownership.ClientID) >= 0) {
			return ErrInvalidSnapshot
		}
		ownershipEpochs[sessionKey{tenantID: ownership.TenantID, clientID: ownership.ClientID}] = ownership.Epoch
	}
	for index, subscription := range snapshot.Subscriptions {
		if !validSessionIdentity(subscription.TenantID, subscription.ClientID) || subscription.OwnerNodeID == 0 || subscription.Epoch == 0 || !validTopicFilter(subscription.Filter) || subscription.QoS > 2 || subscription.RetainHandling > 2 || (index > 0 && compareSubscription(snapshot.Subscriptions[index-1], subscription) >= 0) {
			return ErrInvalidSnapshot
		}
	}
	for index, record := range snapshot.DeliverySessions {
		key := sessionKey{tenantID: record.TenantID, clientID: record.ClientID}
		if !validSessionIdentity(record.TenantID, record.ClientID) || record.NextSequence < 1 || (index > 0 && compareSession(snapshot.DeliverySessions[index-1].TenantID, snapshot.DeliverySessions[index-1].ClientID, record.TenantID, record.ClientID) >= 0) {
			return ErrInvalidSnapshot
		}
		if _, exists := ownershipEpochs[key]; !exists || len(record.Messages) > MaxDeliveryMessagesPerSession || len(record.Inbound) > MaxDeliveryInboundPerSession {
			return ErrInvalidSnapshot
		}
		packetIDs := make(map[uint32]struct{}, len(record.Messages))
		for mi, message := range record.Messages {
			if message.Sequence == 0 || message.Sequence >= record.NextSequence || !validTopic(message.Topic) || message.QoS < 1 || message.QoS > 2 || len(message.Payload) > MaxDeliveryPayloadBytes || (mi > 0 && record.Messages[mi-1].Sequence >= message.Sequence) {
				return ErrInvalidSnapshot
			}
			if message.PacketID == 0 {
				if message.Phase != DeliveryPhaseQueued {
					return ErrInvalidSnapshot
				}
			} else {
				if message.PacketID > 65535 || (message.QoS == 1 && message.Phase != DeliveryPhaseAwaitPubAck) || (message.QoS == 2 && message.Phase != DeliveryPhaseAwaitPubRec && message.Phase != DeliveryPhaseAwaitPubComp) {
					return ErrInvalidSnapshot
				}
				if _, exists := packetIDs[message.PacketID]; exists {
					return ErrInvalidSnapshot
				}
				packetIDs[message.PacketID] = struct{}{}
			}
		}
		for ii, inbound := range record.Inbound {
			if inbound.PacketID == 0 || inbound.PacketID > 65535 || (ii > 0 && record.Inbound[ii-1].PacketID >= inbound.PacketID) || (!inbound.Released && inbound.ReleasedIndex != 0) || (inbound.Released && (inbound.ReleasedIndex == 0 || inbound.ReleasedIndex > snapshot.LastIncludedIndex || len(inbound.Payload) != 0)) || len(inbound.Payload) > MaxDeliveryPayloadBytes || !validTopic(inbound.Topic) {
				return ErrInvalidSnapshot
			}
		}
	}
	for index, record := range snapshot.MembershipHistory {
		if validateMembershipRecord(record) != nil || record.CommittedIndex == 0 || record.CommittedIndex > snapshot.LastIncludedIndex || (index > 0 && snapshot.MembershipHistory[index-1].OperationID >= record.OperationID) {
			return ErrInvalidSnapshot
		}
	}
	retainedBytes := 0
	for index, retained := range snapshot.Retained {
		if !validRetainedState(retained) || (index > 0 && compareRetained(snapshot.Retained[index-1], retained) >= 0) {
			return ErrInvalidSnapshot
		}
		size := retainedStateBytes(retained)
		if size > MaxRetainedBytes-retainedBytes {
			return ErrInvalidSnapshot
		}
		retainedBytes += size
	}
	totalOfflineMessages := 0
	totalOfflineBytes := 0
	for index, session := range snapshot.OfflineSessions {
		if !validSessionIdentity(session.TenantID, session.ClientID) || session.Epoch == 0 || session.NextSequence < 1 || (index > 0 && compareSession(snapshot.OfflineSessions[index-1].TenantID, snapshot.OfflineSessions[index-1].ClientID, session.TenantID, session.ClientID) >= 0) {
			return ErrInvalidSnapshot
		}
		ownerEpoch, hasOwner := ownershipEpochs[sessionKey{tenantID: session.TenantID, clientID: session.ClientID}]
		if !hasOwner || ownerEpoch < session.Epoch {
			return ErrInvalidSnapshot
		}
		if len(session.Messages) > MaxOfflineMessagesPerSession {
			return ErrInvalidSnapshot
		}
		for mIdx, msg := range session.Messages {
			if msg.Sequence == 0 || msg.Sequence >= session.NextSequence || !validTopic(msg.Topic) || msg.QoS != 0 || len(msg.Payload) > MaxOfflinePayloadBytes || (mIdx > 0 && session.Messages[mIdx-1].Sequence >= msg.Sequence) {
				return ErrInvalidSnapshot
			}
			msgBytes := offlineMessageBytes(session.TenantID, session.ClientID, msg)
			if msgBytes > MaxOfflineBytes-totalOfflineBytes {
				return ErrInvalidSnapshot
			}
			totalOfflineBytes += msgBytes
		}
		totalOfflineMessages += len(session.Messages)
		if totalOfflineMessages > MaxOfflineMessages {
			return ErrInvalidSnapshot
		}
	}
	totalDeliveryEntries, totalDeliveryBytes := 0, 0
	for _, record := range snapshot.DeliverySessions {
		totalDeliveryEntries += len(record.Messages) + len(record.Inbound)
		for _, message := range record.Messages {
			totalDeliveryBytes += len(record.TenantID) + len(record.ClientID) + len(message.Topic) + len(message.Payload)
		}
		for _, inbound := range record.Inbound {
			totalDeliveryBytes += len(record.TenantID) + len(record.ClientID) + len(inbound.Topic) + len(inbound.Payload)
		}
		if totalDeliveryEntries > MaxDeliveryEntries || totalDeliveryBytes > MaxDeliveryBytes {
			return ErrInvalidSnapshot
		}
	}
	return validateMembershipState(snapshot.Members, snapshot.MembershipHistory)
}

func validSessionIdentity(tenantID, clientID string) bool {
	if !boundedUTF8(tenantID, 1, maxTenantIDBytes) || !boundedUTF8(clientID, 1, maxClientIDBytes) {
		return false
	}
	tenant, err := tenancy.NewTenantID(tenantID)
	if err != nil {
		return false
	}
	_, err = tenancy.NewSessionKey(tenant, clientID)
	return err == nil
}
func validateMemberIdentityContent(member MemberIdentityDTO) error {
	if member.MemberID == 0 || !boundedUTF8(member.ClusterID, 1, maxClusterIDBytes) || !boundedUTF8(member.Address, 1, maxAddressBytes) || !validAddress(member.Address) || !validDNSName(member.DNSSAN) || len(member.CertificateSHA256) != 32 || !boundedUTF8(member.ReleaseVersion, 1, maxReleaseVersionBytes) {
		return ErrInvalidCommand
	}
	return nil
}

func validTopicFilter(filter string) bool {
	return boundedUTF8(filter, 1, maxTopicFilterBytes) && routing.ValidateTopicFilter(filter) == nil
}

func validTopic(topic string) bool {
	return boundedUTF8(topic, 1, maxTopicFilterBytes) && tenancy.ValidateTopicName(topic) == nil
}

func validAddress(address string) bool {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" || portText == "" || host == "*" {
		return false
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return !ip.IsUnspecified()
	}
	return validDNSName(host)
}
func compatibleMemberProfile(left, right MemberIdentityDTO) bool {
	return left.ClusterID == right.ClusterID && left.ReleaseVersion == right.ReleaseVersion && left.ClusterProtocolMajor == right.ClusterProtocolMajor && left.ClusterProtocolMinor == right.ClusterProtocolMinor && left.StateSchemaMajor == right.StateSchemaMajor && left.StateSchemaMinor == right.StateSchemaMinor
}

func validDNSName(value string) bool {
	if len(value) == 0 || len(value) > maxDNSNameBytes || net.ParseIP(value) != nil {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range []byte(label) {
			if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-') {
				return false
			}
		}
	}
	return true
}

func validIdempotencyKey(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, character := range []byte(value) {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for index := 0; index < len(value); index++ {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		character := value[index]
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

func boundedUTF8(value string, minimum, maximum int) bool {
	return len(value) >= minimum && len(value) <= maximum && utf8.ValidString(value)
}

func compareSession(leftTenant, leftClient, rightTenant, rightClient string) int {
	if leftTenant < rightTenant {
		return -1
	}
	if leftTenant > rightTenant {
		return 1
	}
	if leftClient < rightClient {
		return -1
	}
	if leftClient > rightClient {
		return 1
	}
	return 0
}

func compareRetained(left, right RetainedStateDTO) int {
	if left.TenantID < right.TenantID {
		return -1
	}
	if left.TenantID > right.TenantID {
		return 1
	}
	if left.Topic < right.Topic {
		return -1
	}
	if left.Topic > right.Topic {
		return 1
	}
	return 0
}
func compareSubscription(left, right SubscriptionStateDTO) int {
	if result := compareSession(left.TenantID, left.ClientID, right.TenantID, right.ClientID); result != 0 {
		return result
	}
	if left.Filter < right.Filter {
		return -1
	}
	if left.Filter > right.Filter {
		return 1
	}
	return 0

}
