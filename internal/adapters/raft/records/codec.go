package records

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	maxCommandBytes               = 1 << 20
	maxSnapshotBytes              = 64 << 20
	maxEntries                    = 1_000_000
	maxPublishPayload             = 1_000_000
	maxDeliveryMessagesPerSession = 64
	maxDeliveryInboundPerSession  = 64
	maxDeliveryEntries            = 4096
	maxDeliveryBytes              = 16 << 20
	maxDeliveryTargets            = 1024
	maxRetainedMessages           = 1024
	maxOfflineMessages            = 4096
	maxOfflineMessagesPerSession  = 64
	maxOfflineBytes               = 16 << 20
	maxOfflineDetachMessageBytes  = 768 << 10
)

// ErrCommandTooLarge means a command exceeds the replicated command size limit.
var ErrCommandTooLarge = errors.New("command exceeds 1 MiB limit")

var deterministic = proto.MarshalOptions{Deterministic: true}

// MarshalCommand validates and deterministically encodes a v1 command without modifying it.
func MarshalCommand(command *CommandEnvelope) ([]byte, error) {
	if command == nil {
		return nil, errors.New("command is nil")
	}
	if err := rejectUnknown(command.ProtoReflect()); err != nil {
		return nil, err
	}
	if err := validateCommand(command); err != nil {
		return nil, err
	}
	encoded, err := deterministic.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	if len(encoded) > maxCommandBytes {
		return nil, ErrCommandTooLarge
	}
	return encoded, nil
}

// UnmarshalCommand decodes a canonical v1 command, rejecting unknown and noncanonical wire data.
func UnmarshalCommand(encoded []byte) (*CommandEnvelope, error) {
	if len(encoded) > maxCommandBytes {
		return nil, ErrCommandTooLarge
	}
	command := new(CommandEnvelope)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, command); err != nil {
		return nil, fmt.Errorf("unmarshal command: %w", err)
	}
	if err := rejectUnknown(command.ProtoReflect()); err != nil {
		return nil, err
	}
	if err := validateCommand(command); err != nil {
		return nil, err
	}
	canonical, err := deterministic.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}
	if !bytes.Equal(encoded, canonical) {
		return nil, errors.New("command wire encoding is not canonical")
	}
	return command, nil
}

// MarshalSnapshot sorts a copy of state, computes its digest and encodes a canonical v1 envelope.
func MarshalSnapshot(state *ReplicatedStateSnapshot, lastIncludedIndex uint64) ([]byte, error) {
	if state == nil {
		return nil, errors.New("snapshot state is nil")
	}
	ordered := proto.Clone(state).(*ReplicatedStateSnapshot)
	if err := rejectUnknown(ordered.ProtoReflect()); err != nil {
		return nil, err
	}
	sortSnapshot(ordered)
	if err := validateSnapshotState(ordered); err != nil {
		return nil, err
	}
	if err := validateSnapshotMembershipIndexes(ordered, lastIncludedIndex); err != nil {
		return nil, err
	}
	stateBytes, err := deterministic.Marshal(ordered)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot state: %w", err)
	}
	offlineMessagesCount := 0
	for _, session := range ordered.OfflineSessions {
		if session != nil {
			offlineMessagesCount += len(session.Messages)
		}
	}
	count := uint64(len(ordered.Members) + len(ordered.Ownerships) + len(ordered.Subscriptions) + len(ordered.DeliverySessions) + deliveryObjectCount(ordered.DeliverySessions) + len(ordered.MembershipHistory) + len(ordered.Retained) + len(ordered.OfflineSessions) + offlineMessagesCount)
	digest := sha256.Sum256(stateBytes)
	envelope := &SnapshotEnvelope{SchemaMajor: 1, LastIncludedIndex: lastIncludedIndex, ObjectCount: count, Sha256: digest[:], State: ordered}
	encoded, err := deterministic.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	if len(encoded) > maxSnapshotBytes {
		return nil, errors.New("snapshot exceeds 64 MiB limit")
	}
	return encoded, nil
}

// UnmarshalSnapshot verifies limits, schema, digest, count, state invariants and canonical wire bytes.
func UnmarshalSnapshot(encoded []byte) (*SnapshotEnvelope, error) {
	if len(encoded) > maxSnapshotBytes {
		return nil, errors.New("snapshot exceeds 64 MiB limit")
	}
	envelope := new(SnapshotEnvelope)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, envelope); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	if err := rejectUnknown(envelope.ProtoReflect()); err != nil {
		return nil, err
	}
	if envelope.SchemaMajor != 1 || envelope.SchemaMinor != 0 {
		return nil, errors.New("unsupported snapshot schema")
	}
	if envelope.State == nil {
		return nil, errors.New("snapshot state is required")
	}
	if len(envelope.Sha256) != sha256.Size {
		return nil, errors.New("snapshot SHA-256 must be 32 bytes")
	}
	if err := validateSnapshotState(envelope.State); err != nil {
		return nil, err
	}
	if err := validateSnapshotMembershipIndexes(envelope.State, envelope.LastIncludedIndex); err != nil {
		return nil, err
	}
	offlineMessagesCount := 0
	for _, session := range envelope.State.OfflineSessions {
		if session != nil {
			offlineMessagesCount += len(session.Messages)
		}
	}
	count := uint64(len(envelope.State.Members) + len(envelope.State.Ownerships) + len(envelope.State.Subscriptions) + len(envelope.State.DeliverySessions) + deliveryObjectCount(envelope.State.DeliverySessions) + len(envelope.State.MembershipHistory) + len(envelope.State.Retained) + len(envelope.State.OfflineSessions) + offlineMessagesCount)
	if envelope.ObjectCount != count {
		return nil, errors.New("snapshot object count mismatch")
	}
	stateBytes, err := deterministic.Marshal(envelope.State)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot state: %w", err)
	}
	digest := sha256.Sum256(stateBytes)
	if !bytes.Equal(envelope.Sha256, digest[:]) {
		return nil, errors.New("snapshot checksum mismatch")
	}
	canonical, err := deterministic.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	if !bytes.Equal(encoded, canonical) {
		return nil, errors.New("snapshot wire encoding is not canonical")
	}
	return envelope, nil
}

func validateCommand(c *CommandEnvelope) error {
	if c.SchemaMajor != 1 || c.SchemaMinor != 0 {
		return errors.New("unsupported command schema")
	}
	if !validUUID(c.OperationId) {
		return errors.New("command operation_id must be a canonical lowercase UUID")
	}
	switch payload := c.Payload.(type) {
	case *CommandEnvelope_Membership:
		if payload.Membership == nil {
			return errors.New("membership payload is required")
		}
		if payload.Membership.CommittedIndex != 0 {
			return errors.New("command membership committed_index must be derived from its Raft entry")
		}
		if payload.Membership.OperationId != c.OperationId {
			return errors.New("membership operation_id does not match command")
		}
		return validateMembership(payload.Membership)
	case *CommandEnvelope_Ownership:
		if payload.Ownership == nil {
			return errors.New("ownership payload is required")
		}
		return validateOwnership(payload.Ownership)
	case *CommandEnvelope_Subscription:
		if payload.Subscription == nil {
			return errors.New("subscription payload is required")
		}
		return validateSubscription(payload.Subscription)
	case *CommandEnvelope_Delivery:
		if payload.Delivery == nil {
			return errors.New("delivery payload is required")
		}
		return validateDeliveryChange(payload.Delivery)
	case *CommandEnvelope_Retained:
		if payload.Retained == nil {
			return errors.New("retained payload is required")
		}
		return validateRetained(payload.Retained)
	case *CommandEnvelope_Offline:
		if payload.Offline == nil {
			return errors.New("offline payload is required")
		}
		return validateOffline(payload.Offline)
	default:
		return errors.New("command must contain exactly one known payload")
	}
}

func validateMembership(m *MembershipRecord) error {
	if !validUUID(m.OperationId) {
		return errors.New("membership operation_id must be a canonical lowercase UUID")
	}
	if m.Action < MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN || m.Action > MembershipAction_MEMBERSHIP_ACTION_ABORT_STAGE {
		return errors.New("invalid membership action")
	}
	if !printableKey(m.IdempotencyKey) {
		return errors.New("idempotency_key must be 1..128 printable ASCII bytes without spaces")
	}
	if m.Member != nil {
		if err := validateIdentity(m.Member); err != nil {
			return err
		}
	}
	if m.Action == MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN {
		if m.Member == nil || m.Member.Phase != MemberPhase_MEMBER_PHASE_STAGED || m.Member.OperationId != m.OperationId {
			return errors.New("stage join requires matching staged member identity")
		}
	}
	if m.TargetMemberId != 0 && m.Action != MembershipAction_MEMBERSHIP_ACTION_BEGIN_REMOVE && m.Action != MembershipAction_MEMBERSHIP_ACTION_REMOVE_MEMBER {
		return errors.New("target_member_id is only valid for remove actions")
	}
	return nil
}
func validateIdentity(m *MemberIdentity) error {
	if m.MemberId == 0 {
		return errors.New("member_id must be positive")
	}
	if !boundedUTF8(m.ClusterId, 1, 200) {
		return errors.New("cluster_id must be 1..200 UTF-8 bytes")
	}
	if !boundedUTF8(m.Address, 1, 255) {
		return errors.New("address must be 1..255 UTF-8 bytes")
	}
	host, portText, err := net.SplitHostPort(m.Address)
	if err != nil || host == "" || portText == "" {
		return errors.New("address must be a valid host:port")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return errors.New("address port must be in 1..65535")
	}
	ip := net.ParseIP(host)
	if host == "*" || (ip != nil && ip.IsUnspecified()) {
		return errors.New("address must not be a wildcard listener")
	}
	if ip == nil && !validDNSName(host) {
		return errors.New("address host must be a DNS name or IP literal")
	}
	if !validDNSName(m.DnsSan) {
		return errors.New("dns_san must be a valid 1..253 byte ASCII DNS name")
	}
	if len(m.CertificateSha256) != 32 {
		return errors.New("certificate_sha256 must be 32 bytes")
	}
	if !boundedUTF8(m.ReleaseVersion, 1, 64) {
		return errors.New("release_version must be 1..64 UTF-8 bytes")
	}
	if m.ClusterProtocolMajor != 2 || m.ClusterProtocolMinor != 0 || m.StateSchemaMajor != 1 || m.StateSchemaMinor != 0 {
		return errors.New("unsupported cluster protocol or state schema version")
	}
	if m.Phase < MemberPhase_MEMBER_PHASE_STAGED || m.Phase > MemberPhase_MEMBER_PHASE_RETIRED {
		return errors.New("invalid member phase")
	}
	if m.Phase == MemberPhase_MEMBER_PHASE_STAGED || m.Phase == MemberPhase_MEMBER_PHASE_LEARNER || m.Phase == MemberPhase_MEMBER_PHASE_CATCHING_UP || m.Phase == MemberPhase_MEMBER_PHASE_LEAVING {
		if !validUUID(m.OperationId) {
			return errors.New("in-progress member phase requires a canonical lowercase operation_id")
		}
	} else if m.OperationId != "" {
		return errors.New("stable member phase must not carry operation_id")
	}
	return nil
}
func validateOwnership(o *OwnershipChange) error {
	if !boundedUTF8(o.TenantId, 1, 128) || !boundedUTF8(o.ClientId, 1, 256) {
		return errors.New("ownership tenant_id or client_id is out of bounds")
	}
	if o.OwnerNodeId == 0 || o.Epoch == 0 {
		return errors.New("ownership owner_node_id and epoch must be positive")
	}
	if o.Action < OwnershipAction_OWNERSHIP_ACTION_ACQUIRE || o.Action > OwnershipAction_OWNERSHIP_ACTION_RELEASE {
		return errors.New("invalid ownership action")
	}
	return nil
}
func validateSubscription(s *SubscriptionChange) error {
	if !boundedUTF8(s.TenantId, 1, 128) || !boundedUTF8(s.ClientId, 1, 256) {
		return errors.New("subscription tenant_id or client_id is out of bounds")
	}
	if s.OwnerNodeId == 0 || s.Epoch == 0 {
		return errors.New("subscription owner_node_id and epoch must be positive")
	}
	if !validFilter(s.Filter) {
		return errors.New("invalid MQTT subscription filter")
	}
	if s.Qos > 2 || s.RetainHandling > 2 {
		return errors.New("subscription QoS or retain handling is out of range")
	}
	if s.Action < SubscriptionAction_SUBSCRIPTION_ACTION_ADD || s.Action > SubscriptionAction_SUBSCRIPTION_ACTION_REMOVE {
		return errors.New("invalid subscription action")
	}
	return nil
}
func validateDeliveryChange(change *DeliveryChange) error {
	if !boundedUTF8(change.TenantId, 1, 128) || !boundedUTF8(change.ClientId, 1, 256) {
		return errors.New("delivery tenant_id or client_id is out of bounds")
	}
	if change.Action < DeliveryAction_DELIVERY_ACTION_PUBLISH || change.Action > DeliveryAction_DELIVERY_ACTION_DISCARD {
		return errors.New("invalid delivery action")
	}
	if change.ProposedAtUnixMillis == 0 {
		return errors.New("delivery proposed_at_unix_millis must be positive")
	}
	if change.PublishQos > 2 {
		return errors.New("delivery publish_qos is out of range")
	}
	validateTargets := func() error {
		if len(change.Targets) > maxDeliveryTargets {
			return errors.New("delivery targets exceed limit")
		}
		seen := make(map[string]struct{}, len(change.Targets))
		for _, target := range change.Targets {
			if target == nil || !boundedUTF8(target.ClientId, 1, 256) {
				return errors.New("invalid delivery target client_id")
			}
			if _, exists := seen[target.ClientId]; exists {
				return errors.New("duplicate delivery target")
			}
			seen[target.ClientId] = struct{}{}
		}
		return nil
	}
	switch change.Action {
	case DeliveryAction_DELIVERY_ACTION_PUBLISH:
		if change.OwnerNodeId != 0 || change.Epoch != 0 || change.PacketId != 0 || change.Sequence != 0 || change.Reset_ || change.SessionExpiresAtUnixMillis != 0 {
			return errors.New("invalid delivery publish command fields")
		}
		if change.PublishQos == 0 || !validTopic(change.Topic) || len(change.Payload) > maxPublishPayload {
			return errors.New("invalid delivery publish message")
		}
		return validateTargets()
	case DeliveryAction_DELIVERY_ACTION_RECEIVE:
		if change.OwnerNodeId == 0 || change.Epoch == 0 || change.PacketId == 0 || change.PacketId > 65535 || change.PublishQos != 0 || change.Sequence != 0 || change.Reset_ || change.SessionExpiresAtUnixMillis != 0 || len(change.Targets) != 0 {
			return errors.New("invalid delivery receive command fields")
		}
		if !validTopic(change.Topic) || len(change.Payload) > maxPublishPayload {
			return errors.New("invalid delivery receive message")
		}
	case DeliveryAction_DELIVERY_ACTION_RELEASE:
		if change.OwnerNodeId == 0 || change.Epoch == 0 || change.PacketId == 0 || change.PacketId > 65535 || change.PublishQos != 2 || change.Topic != "" || len(change.Payload) != 0 || change.Retain || change.ExpiresAtUnixMillis != 0 || change.Sequence != 0 || change.Reset_ || change.SessionExpiresAtUnixMillis != 0 {
			return errors.New("invalid delivery release command fields")
		}
		return validateTargets()
	case DeliveryAction_DELIVERY_ACTION_SEND:
		if change.OwnerNodeId == 0 || change.Epoch == 0 || change.Sequence == 0 || change.PacketId == 0 || change.PacketId > 65535 || change.PublishQos != 0 || change.Topic != "" || len(change.Payload) != 0 || change.Retain || change.ExpiresAtUnixMillis != 0 || len(change.Targets) != 0 || change.Reset_ || change.SessionExpiresAtUnixMillis != 0 {
			return errors.New("invalid delivery send command fields")
		}
	case DeliveryAction_DELIVERY_ACTION_ADVANCE, DeliveryAction_DELIVERY_ACTION_COMPLETE:
		if change.OwnerNodeId == 0 || change.Epoch == 0 || change.PacketId == 0 || change.PacketId > 65535 || change.PublishQos != 0 || change.Topic != "" || len(change.Payload) != 0 || change.Retain || change.ExpiresAtUnixMillis != 0 || len(change.Targets) != 0 || change.Sequence != 0 || change.Reset_ || change.SessionExpiresAtUnixMillis != 0 {
			return errors.New("invalid delivery acknowledgement command fields")
		}
	case DeliveryAction_DELIVERY_ACTION_ATTACH:
		if change.OwnerNodeId == 0 || change.Epoch == 0 || change.PublishQos != 0 || change.Topic != "" || len(change.Payload) != 0 || change.Retain || change.ExpiresAtUnixMillis != 0 || change.PacketId != 0 || len(change.Targets) != 0 || change.Sequence != 0 || change.SessionExpiresAtUnixMillis != 0 {
			return errors.New("invalid delivery attach command fields")
		}
	case DeliveryAction_DELIVERY_ACTION_DETACH:
		if change.OwnerNodeId == 0 || change.Epoch == 0 || change.PublishQos != 0 || change.Topic != "" || len(change.Payload) != 0 || change.Retain || change.ExpiresAtUnixMillis != 0 || change.PacketId != 0 || len(change.Targets) != 0 || change.Sequence != 0 || change.Reset_ {
			return errors.New("invalid delivery detach command fields")
		}
	case DeliveryAction_DELIVERY_ACTION_DISCARD:
		if change.OwnerNodeId == 0 || change.Epoch == 0 || change.PublishQos != 0 || change.Topic != "" || len(change.Payload) != 0 || change.Retain || change.ExpiresAtUnixMillis != 0 || change.PacketId != 0 || len(change.Targets) != 0 || change.Sequence != 0 || change.Reset_ || change.SessionExpiresAtUnixMillis != 0 {
			return errors.New("invalid delivery discard command fields")
		}
	}
	return nil
}

func validateRetained(change *RetainedChange) error {
	if !boundedUTF8(change.TenantId, 1, 128) || !boundedUTF8(change.PublisherClientId, 1, 256) || !validTopic(change.Topic) {
		return errors.New("retained tenant_id, publisher_client_id, or topic is invalid")
	}
	if change.ProposedAtUnixMillis == 0 {
		return errors.New("retained proposed_at_unix_millis must be positive")
	}
	switch change.Action {
	case RetainedAction_RETAINED_ACTION_REPLACE:
		if len(change.Payload) == 0 || len(change.Payload) > maxPublishPayload || change.Qos > 2 {
			return errors.New("invalid retained replacement")
		}
	case RetainedAction_RETAINED_ACTION_DELETE:
		if len(change.Payload) != 0 || change.Qos != 0 || change.ExpiresAtUnixMillis != 0 {
			return errors.New("invalid retained delete")
		}
	default:
		return errors.New("invalid retained action")
	}
	return nil
}

func validateRetainedState(state *RetainedState) error {
	if state == nil {
		return errors.New("nil snapshot retained state")
	}
	if !boundedUTF8(state.TenantId, 1, 128) || !boundedUTF8(state.PublisherClientId, 1, 256) || !validTopic(state.Topic) || len(state.Payload) == 0 || len(state.Payload) > maxPublishPayload || state.Qos > 2 {
		return errors.New("invalid snapshot retained state")
	}
	return nil
}

func validateOffline(change *OfflineChange) error {
	if !boundedUTF8(change.TenantId, 1, 128) || !boundedUTF8(change.ClientId, 1, 256) {
		return errors.New("invalid offline tenant_id or client_id")
	}
	if change.Epoch == 0 || change.ProposedAtUnixMillis == 0 {
		return errors.New("offline epoch and proposed_at_unix_millis must be positive")
	}
	for _, msg := range change.Messages {
		if msg == nil {
			return errors.New("nil offline message")
		}
		if msg.Sequence != 0 {
			return errors.New("offline message sequence must be zero in command")
		}
		if !validTopic(msg.Topic) {
			return errors.New("invalid offline message topic")
		}
		if msg.Qos != 0 {
			return errors.New("offline message QoS must be 0")
		}
		if len(msg.Payload) > maxPublishPayload {
			return errors.New("offline message payload exceeds limit")
		}
	}
	switch change.Action {
	case OfflineAction_OFFLINE_ACTION_DETACH:
		if change.OwnerNodeId == 0 || len(change.Messages) > maxOfflineMessagesPerSession {
			return errors.New("invalid offline detach command")
		}
		totalBytes := 0
		for _, msg := range change.Messages {
			totalBytes += len(msg.Topic) + len(msg.Payload)
		}
		if totalBytes > maxOfflineDetachMessageBytes {
			return errors.New("offline detach message bytes exceed limit")
		}
	case OfflineAction_OFFLINE_ACTION_ENQUEUE:
		if change.OwnerNodeId != 0 || len(change.Messages) != 1 || change.SessionExpiresAtUnixMillis != 0 {
			return errors.New("invalid offline enqueue command")
		}
	case OfflineAction_OFFLINE_ACTION_CLAIM:
		if change.OwnerNodeId == 0 || len(change.Messages) != 0 || change.SessionExpiresAtUnixMillis != 0 {
			return errors.New("invalid offline claim command")
		}
	default:
		return errors.New("invalid offline action")
	}
	return nil
}

func validateOfflineSessionState(state *OfflineSessionState) error {
	if state == nil {
		return errors.New("nil snapshot offline session")
	}
	if !boundedUTF8(state.TenantId, 1, 128) || !boundedUTF8(state.ClientId, 1, 256) || state.Epoch == 0 || state.NextSequence < 1 {
		return errors.New("invalid snapshot offline session identity or sequence")
	}
	if len(state.Messages) > maxOfflineMessagesPerSession {
		return errors.New("snapshot offline session exceeds message limit")
	}
	for i, msg := range state.Messages {
		if msg == nil {
			return errors.New("nil snapshot offline message")
		}
		if msg.Sequence == 0 || msg.Sequence >= state.NextSequence || !validTopic(msg.Topic) || msg.Qos != 0 || len(msg.Payload) > maxPublishPayload {
			return errors.New("invalid snapshot offline message")
		}
		if i > 0 && state.Messages[i-1].Sequence >= msg.Sequence {
			return errors.New("snapshot offline messages are not strictly ordered by sequence")
		}
	}
	return nil
}

func validateDeliverySessionState(state *DeliverySessionState) error {
	if state == nil {
		return errors.New("nil snapshot delivery session")
	}
	if !boundedUTF8(state.TenantId, 1, 128) || !boundedUTF8(state.ClientId, 1, 256) || state.NextSequence < 1 {
		return errors.New("invalid snapshot delivery session identity or sequence")
	}
	if len(state.Messages) > maxDeliveryMessagesPerSession || len(state.Inbound) > maxDeliveryInboundPerSession {
		return errors.New("snapshot delivery session exceeds entry limit")
	}
	seenPacketIDs := make(map[uint32]struct{}, len(state.Messages))
	for i, message := range state.Messages {
		if message == nil {
			return errors.New("nil snapshot delivery message")
		}
		if message.Sequence == 0 || message.Sequence >= state.NextSequence || !validTopic(message.Topic) || message.Qos < 1 || message.Qos > 2 || len(message.Payload) > maxPublishPayload {
			return errors.New("invalid snapshot delivery message")
		}
		if i > 0 && state.Messages[i-1].Sequence >= message.Sequence {
			return errors.New("snapshot delivery messages are not strictly ordered by sequence")
		}
		if message.PacketId == 0 {
			if message.Phase != DeliveryPhase_DELIVERY_PHASE_QUEUED {
				return errors.New("queued delivery message has an invalid phase")
			}
			continue
		}
		if message.PacketId > 65535 {
			return errors.New("delivery packet ID is out of range")
		}
		if _, exists := seenPacketIDs[message.PacketId]; exists {
			return errors.New("duplicate delivery packet ID")
		}
		seenPacketIDs[message.PacketId] = struct{}{}
		if (message.Qos == 1 && message.Phase != DeliveryPhase_DELIVERY_PHASE_AWAIT_PUBACK) ||
			(message.Qos == 2 && message.Phase != DeliveryPhase_DELIVERY_PHASE_AWAIT_PUBREC && message.Phase != DeliveryPhase_DELIVERY_PHASE_AWAIT_PUBCOMP) {
			return errors.New("delivery phase conflicts with QoS")
		}
	}
	for i, inbound := range state.Inbound {
		if inbound == nil {
			return errors.New("nil snapshot inbound message")
		}
		if inbound.PacketId == 0 || inbound.PacketId > 65535 || !validTopic(inbound.Topic) || len(inbound.Payload) > maxPublishPayload {
			return errors.New("invalid snapshot inbound message")
		}
		if i > 0 && state.Inbound[i-1].PacketId >= inbound.PacketId {
			return errors.New("snapshot inbound messages are not strictly ordered by packet ID")
		}
		if (!inbound.Released && inbound.ReleasedIndex != 0) ||
			(inbound.Released && (inbound.ReleasedIndex == 0 || len(inbound.Payload) != 0)) {
			return errors.New("invalid snapshot inbound release state")
		}
	}
	return nil
}

func deliveryEntryBytes(tenantID, clientID, topic string, payload []byte) int {
	return len(tenantID) + len(clientID) + len(topic) + len(payload)
}

func deliveryObjectCount(sessions []*DeliverySessionState) int {
	count := 0
	for _, session := range sessions {
		if session != nil {
			count += len(session.Messages) + len(session.Inbound)
		}
	}
	return count
}

func validateSnapshotState(s *ReplicatedStateSnapshot) error {
	offlineMessagesCount := 0
	for _, session := range s.OfflineSessions {
		if session != nil {
			offlineMessagesCount += len(session.Messages)
		}
	}
	count := len(s.Members) + len(s.Ownerships) + len(s.Subscriptions) + len(s.DeliverySessions) + deliveryObjectCount(s.DeliverySessions) + len(s.MembershipHistory) + len(s.Retained) + len(s.OfflineSessions) + offlineMessagesCount
	if count > maxEntries {
		return errors.New("snapshot exceeds 1,000,000 entries")
	}
	if len(s.Retained) > maxRetainedMessages {
		return errors.New("snapshot exceeds retained message limit")
	}
	if offlineMessagesCount > maxOfflineMessages {
		return errors.New("snapshot exceeds offline message limit")
	}
	for _, m := range s.Members {
		if m == nil {
			return errors.New("nil snapshot member")
		}
		if err := validateIdentity(m); err != nil {
			return err
		}
	}
	for i := 1; i < len(s.Members); i++ {
		if s.Members[i-1].MemberId >= s.Members[i].MemberId {
			return errors.New("snapshot members are not uniquely sorted")
		}
	}
	for _, o := range s.Ownerships {
		if o == nil || !boundedUTF8(o.TenantId, 1, 128) || !boundedUTF8(o.ClientId, 1, 256) || o.OwnerNodeId == 0 || o.Epoch == 0 {
			return errors.New("invalid snapshot ownership")
		}
	}
	for i := 1; i < len(s.Ownerships); i++ {
		a, b := s.Ownerships[i-1], s.Ownerships[i]
		if compare2(a.TenantId, a.ClientId, b.TenantId, b.ClientId) >= 0 {
			return errors.New("snapshot ownerships are not uniquely sorted")
		}
	}
	for _, sub := range s.Subscriptions {
		if sub == nil || !boundedUTF8(sub.TenantId, 1, 128) || !boundedUTF8(sub.ClientId, 1, 256) || sub.OwnerNodeId == 0 || sub.Epoch == 0 || !validFilter(sub.Filter) || sub.Qos > 2 || sub.RetainHandling > 2 {
			return errors.New("invalid snapshot subscription")
		}
	}
	for i := 1; i < len(s.Subscriptions); i++ {
		a, b := s.Subscriptions[i-1], s.Subscriptions[i]
		if compare3(a.TenantId, a.ClientId, a.Filter, b.TenantId, b.ClientId, b.Filter) >= 0 {
			return errors.New("snapshot subscriptions are not uniquely sorted")
		}
	}
	deliveryEntries, deliveryBytes := 0, 0
	for _, session := range s.DeliverySessions {
		if err := validateDeliverySessionState(session); err != nil {
			return err
		}
		deliveryEntries += len(session.Messages) + len(session.Inbound)
		for _, message := range session.Messages {
			deliveryBytes += deliveryEntryBytes(session.TenantId, session.ClientId, message.Topic, message.Payload)
		}
		for _, inbound := range session.Inbound {
			deliveryBytes += deliveryEntryBytes(session.TenantId, session.ClientId, inbound.Topic, inbound.Payload)
		}
	}
	for i := 1; i < len(s.DeliverySessions); i++ {
		a, b := s.DeliverySessions[i-1], s.DeliverySessions[i]
		if compare2(a.TenantId, a.ClientId, b.TenantId, b.ClientId) >= 0 {
			return errors.New("snapshot delivery sessions are not uniquely sorted")
		}
	}
	if deliveryEntries > maxDeliveryEntries {
		return errors.New("snapshot exceeds delivery entry limit")
	}
	if deliveryBytes > maxDeliveryBytes {
		return errors.New("snapshot exceeds delivery byte limit")
	}
	for _, m := range s.MembershipHistory {
		if m == nil {
			return errors.New("nil membership history record")
		}
		if err := validateMembership(m); err != nil {
			return err
		}
		if m.CommittedIndex == 0 {
			return errors.New("membership history committed_index must be positive")
		}
	}
	for i := 1; i < len(s.MembershipHistory); i++ {
		if s.MembershipHistory[i-1].OperationId >= s.MembershipHistory[i].OperationId {
			return errors.New("snapshot membership history is not uniquely sorted")
		}
	}
	for _, retained := range s.Retained {
		if err := validateRetainedState(retained); err != nil {
			return err
		}
	}
	for i := 1; i < len(s.Retained); i++ {
		a, b := s.Retained[i-1], s.Retained[i]
		if compare2(a.TenantId, a.Topic, b.TenantId, b.Topic) >= 0 {
			return errors.New("snapshot retained values are not uniquely sorted")
		}
	}
	offlineBytes := 0
	for _, session := range s.OfflineSessions {
		if err := validateOfflineSessionState(session); err != nil {
			return err
		}
		for _, msg := range session.Messages {
			messageBytes := len(session.TenantId) + len(session.ClientId) + len(msg.Topic) + len(msg.Payload)
			if messageBytes > maxOfflineBytes-offlineBytes {
				return errors.New("snapshot exceeds offline byte limit")
			}
			offlineBytes += messageBytes
		}
	}
	for i := 1; i < len(s.OfflineSessions); i++ {
		a, b := s.OfflineSessions[i-1], s.OfflineSessions[i]
		if compare2(a.TenantId, a.ClientId, b.TenantId, b.ClientId) >= 0 {
			return errors.New("snapshot offline sessions are not uniquely sorted")
		}
	}
	return nil
}

func validateSnapshotMembershipIndexes(s *ReplicatedStateSnapshot, lastIncludedIndex uint64) error {
	for _, record := range s.MembershipHistory {
		if record.CommittedIndex > lastIncludedIndex {
			return errors.New("membership history committed_index exceeds last_included_index")
		}
	}
	for _, session := range s.DeliverySessions {
		for _, inbound := range session.Inbound {
			if inbound.ReleasedIndex > lastIncludedIndex {
				return errors.New("delivery released_index exceeds last_included_index")
			}
		}
	}
	return nil
}
func sortSnapshot(s *ReplicatedStateSnapshot) {
	sort.Slice(s.Members, func(i, j int) bool {
		if s.Members[i] == nil {
			return s.Members[j] != nil
		}
		if s.Members[j] == nil {
			return false
		}
		return s.Members[i].MemberId < s.Members[j].MemberId
	})
	sort.Slice(s.Ownerships, func(i, j int) bool {
		if s.Ownerships[i] == nil {
			return s.Ownerships[j] != nil
		}
		if s.Ownerships[j] == nil {
			return false
		}
		a, b := s.Ownerships[i], s.Ownerships[j]
		return compare2(a.TenantId, a.ClientId, b.TenantId, b.ClientId) < 0
	})
	sort.Slice(s.Subscriptions, func(i, j int) bool {
		if s.Subscriptions[i] == nil {
			return s.Subscriptions[j] != nil
		}
		if s.Subscriptions[j] == nil {
			return false
		}
		a, b := s.Subscriptions[i], s.Subscriptions[j]
		return compare3(a.TenantId, a.ClientId, a.Filter, b.TenantId, b.ClientId, b.Filter) < 0
	})
	sort.Slice(s.DeliverySessions, func(i, j int) bool {
		if s.DeliverySessions[i] == nil {
			return s.DeliverySessions[j] != nil
		}
		if s.DeliverySessions[j] == nil {
			return false
		}
		a, b := s.DeliverySessions[i], s.DeliverySessions[j]
		return compare2(a.TenantId, a.ClientId, b.TenantId, b.ClientId) < 0
	})
	sort.Slice(s.MembershipHistory, func(i, j int) bool {
		if s.MembershipHistory[i] == nil {
			return s.MembershipHistory[j] != nil
		}
		if s.MembershipHistory[j] == nil {
			return false
		}
		return s.MembershipHistory[i].OperationId < s.MembershipHistory[j].OperationId
	})
	sort.Slice(s.Retained, func(i, j int) bool {
		if s.Retained[i] == nil {
			return s.Retained[j] != nil
		}
		if s.Retained[j] == nil {
			return false
		}
		return compare2(s.Retained[i].TenantId, s.Retained[i].Topic, s.Retained[j].TenantId, s.Retained[j].Topic) < 0
	})
	sort.Slice(s.OfflineSessions, func(i, j int) bool {
		if s.OfflineSessions[i] == nil {
			return s.OfflineSessions[j] != nil
		}
		if s.OfflineSessions[j] == nil {
			return false
		}
		return compare2(s.OfflineSessions[i].TenantId, s.OfflineSessions[i].ClientId, s.OfflineSessions[j].TenantId, s.OfflineSessions[j].ClientId) < 0
	})
	for _, session := range s.DeliverySessions {
		if session == nil {
			continue
		}
		sort.Slice(session.Messages, func(i, j int) bool {
			if session.Messages[i] == nil {
				return session.Messages[j] != nil
			}
			if session.Messages[j] == nil {
				return false
			}
			return session.Messages[i].Sequence < session.Messages[j].Sequence
		})
		sort.Slice(session.Inbound, func(i, j int) bool {
			if session.Inbound[i] == nil {
				return session.Inbound[j] != nil
			}
			if session.Inbound[j] == nil {
				return false
			}
			return session.Inbound[i].PacketId < session.Inbound[j].PacketId
		})
	}
}
func rejectUnknown(m protoreflect.Message) error {
	if len(m.GetUnknown()) != 0 {
		return errors.New("protobuf contains unknown fields")
	}
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
			continue
		}
		v := m.Get(fd)
		if fd.IsList() {
			list := v.List()
			for j := 0; j < list.Len(); j++ {
				if err := rejectUnknown(list.Get(j).Message()); err != nil {
					return err
				}
			}
		} else if m.Has(fd) {
			if err := rejectUnknown(v.Message()); err != nil {
				return err
			}
		}
	}
	return nil
}
func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for index := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		character := value[index]
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}
func boundedUTF8(s string, min, max int) bool {
	return len(s) >= min && len(s) <= max && utf8.ValidString(s)
}
func printableKey(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}
func asciiDNS(s string) bool {
	if len(s) < 1 || len(s) > 253 {
		return false
	}
	for _, b := range []byte(s) {
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '.' || b == '-') {
			return false
		}
	}
	return true
}
func validDNSName(s string) bool {
	if !asciiDNS(s) || net.ParseIP(s) != nil {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	return true
}
func compare2(a, b, c, d string) int {
	if result := strings.Compare(a, c); result != 0 {
		return result
	}
	return strings.Compare(b, d)
}
func compare3(a, b, c, d, e, f string) int {
	if result := strings.Compare(a, d); result != 0 {
		return result
	}
	if result := strings.Compare(b, e); result != 0 {
		return result
	}
	return strings.Compare(c, f)
}
func validTopic(s string) bool {
	return validMQTTUTF8(s) && !strings.ContainsAny(s, "+#")
}

func validMQTTUTF8(s string) bool {
	if !boundedUTF8(s, 1, 65535) {
		return false
	}
	for _, r := range s {
		if r == 0 || r >= 0xfdd0 && r <= 0xfdef || r&0xffff == 0xfffe || r&0xffff == 0xffff {
			return false
		}
	}
	return true
}

func validFilter(s string) bool {
	if !validMQTTUTF8(s) {
		return false
	}
	if strings.HasPrefix(s, "$share/") {
		parts := strings.SplitN(strings.TrimPrefix(s, "$share/"), "/", 2)
		if len(parts) != 2 || parts[0] == "" || strings.ContainsAny(parts[0], "+#") || parts[1] == "" {
			return false
		}
		s = parts[1]
	}
	levels := strings.Split(s, "/")
	for i, level := range levels {
		if strings.ContainsAny(level, "+#") && level != "+" && level != "#" {
			return false
		}
		if level == "#" && i != len(levels)-1 {
			return false
		}
	}
	return true
}
