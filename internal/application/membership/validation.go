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
	if command.Checkpoint != nil {
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
	case command.Checkpoint != nil:
		if validateCheckpoint(*command.Checkpoint) != nil {
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

func validateCheckpoint(checkpoint SessionCheckpointDTO) error {
	if !validSessionIdentity(checkpoint.TenantID, checkpoint.ClientID) || checkpoint.OwnerNodeID == 0 || checkpoint.Epoch == 0 {
		return ErrInvalidCommand
	}
	var previous uint32
	for index, entry := range checkpoint.Inbound {
		if validatePublish(entry.Publish, true) != nil || !validInboundPhase(entry.Publish.QoS, entry.Phase) || (index > 0 && entry.Publish.PacketID <= previous) {
			return ErrInvalidCommand
		}
		previous = entry.Publish.PacketID
	}
	previous = 0
	for index, entry := range checkpoint.Outbound {
		if validatePublish(entry.Publish, true) != nil || !validOutboundPhase(entry.Publish.QoS, entry.Phase) || (index > 0 && entry.Publish.PacketID <= previous) {
			return ErrInvalidCommand
		}
		previous = entry.Publish.PacketID
	}
	return nil
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

func validatePublish(publish PublishPacketDTO, checkpoint bool) error {
	if publish.ProtocolVersion != 4 && publish.ProtocolVersion != 5 || publish.QoS > 2 || publish.PacketID > 65535 || ((publish.QoS == 0) != (publish.PacketID == 0)) {
		return ErrInvalidCommand
	}
	if checkpoint && publish.QoS == 0 || publish.ProtocolVersion == 4 && publish.HasMessageExpiryInterval || !publish.HasMessageExpiryInterval && publish.MessageExpiryInterval != 0 || len(publish.Payload) > maxPublishPayloadBytes || !validTopic(publish.Topic) {
		return ErrInvalidCommand
	}
	return nil
}

func validInboundPhase(qos uint32, phase InboundDeliveryPhase) bool {
	if qos == 1 {
		return phase == InboundQoS1AwaitAcceptance || phase == InboundQoS1Accepted
	}
	if qos == 2 {
		return phase >= InboundQoS2AwaitAcceptance && phase <= InboundQoS2Complete
	}
	return false
}

func validOutboundPhase(qos uint32, phase OutboundDeliveryPhase) bool {
	if qos == 1 {
		return phase == OutboundAwaitPubAck || phase == OutboundComplete
	}
	if qos == 2 {
		return phase == OutboundAwaitPubRec || phase == OutboundAwaitPubComp || phase == OutboundComplete
	}
	return false
}
func validSnapshot(snapshot SnapshotDTO) error {
	if snapshot.SchemaMajor != SchemaMajor || snapshot.SchemaMinor != SchemaMinor || (len(snapshot.Digest) != 0 && len(snapshot.Digest) != 32) {
		return ErrInvalidSnapshot
	}
	offlineMessagesCount := 0
	for _, session := range snapshot.OfflineSessions {
		offlineMessagesCount += len(session.Messages)
	}
	count := len(snapshot.Members) + len(snapshot.Ownerships) + len(snapshot.Subscriptions) + len(snapshot.Checkpoints) + len(snapshot.MembershipHistory) + len(snapshot.Retained) + len(snapshot.OfflineSessions) + offlineMessagesCount
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
	for index, checkpoint := range snapshot.Checkpoints {
		if validateCheckpoint(checkpoint) != nil || (index > 0 && compareSession(snapshot.Checkpoints[index-1].TenantID, snapshot.Checkpoints[index-1].ClientID, checkpoint.TenantID, checkpoint.ClientID) >= 0) {
			return ErrInvalidSnapshot
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
