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
	maxCommandBytes   = 1 << 20
	maxSnapshotBytes  = 64 << 20
	maxEntries        = 1_000_000
	maxPublishPayload = 1_000_000
)

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
		return nil, errors.New("command exceeds 1 MiB limit")
	}
	return encoded, nil
}

// UnmarshalCommand decodes a canonical v1 command, rejecting unknown and noncanonical wire data.
func UnmarshalCommand(encoded []byte) (*CommandEnvelope, error) {
	if len(encoded) > maxCommandBytes {
		return nil, errors.New("command exceeds 1 MiB limit")
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
	count := uint64(len(ordered.Members) + len(ordered.Ownerships) + len(ordered.Subscriptions) + len(ordered.Checkpoints) + len(ordered.MembershipHistory))
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
	count := uint64(len(envelope.State.Members) + len(envelope.State.Ownerships) + len(envelope.State.Subscriptions) + len(envelope.State.Checkpoints) + len(envelope.State.MembershipHistory))
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
	case *CommandEnvelope_Checkpoint:
		if payload.Checkpoint == nil {
			return errors.New("checkpoint payload is required")
		}
		return validateCheckpoint(payload.Checkpoint)
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
func validateCheckpoint(c *SessionCheckpoint) error {
	if !boundedUTF8(c.TenantId, 1, 128) || !boundedUTF8(c.ClientId, 1, 256) {
		return errors.New("checkpoint tenant_id or client_id is out of bounds")
	}
	if c.OwnerNodeId == 0 || c.Epoch == 0 {
		return errors.New("checkpoint owner_node_id and epoch must be positive")
	}
	seenIn, seenOut := map[uint32]bool{}, map[uint32]bool{}
	for _, item := range c.Inbound {
		if item == nil || item.Publish == nil {
			return errors.New("inbound checkpoint requires publish packet")
		}
		if err := validatePublish(item.Publish, true); err != nil {
			return err
		}
		if item.Phase < InboundDeliveryPhase_INBOUND_QOS1_AWAIT_ACCEPTANCE || item.Phase > InboundDeliveryPhase_INBOUND_QOS2_COMPLETE {
			return errors.New("invalid inbound delivery phase")
		}
		if (item.Publish.Qos == 1 && item.Phase > InboundDeliveryPhase_INBOUND_QOS1_ACCEPTED) || (item.Publish.Qos == 2 && item.Phase < InboundDeliveryPhase_INBOUND_QOS2_AWAIT_ACCEPTANCE) {
			return errors.New("inbound delivery phase conflicts with QoS")
		}
		if seenIn[item.Publish.PacketId] {
			return errors.New("duplicate inbound packet ID")
		}
		seenIn[item.Publish.PacketId] = true
	}
	for _, item := range c.Outbound {
		if item == nil || item.Publish == nil {
			return errors.New("outbound checkpoint requires publish packet")
		}
		if err := validatePublish(item.Publish, true); err != nil {
			return err
		}
		if item.Phase < OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBACK || item.Phase > OutboundDeliveryPhase_OUTBOUND_COMPLETE {
			return errors.New("invalid outbound delivery phase")
		}
		if (item.Publish.Qos == 1 && item.Phase != OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBACK && item.Phase != OutboundDeliveryPhase_OUTBOUND_COMPLETE) || (item.Publish.Qos == 2 && item.Phase != OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBREC && item.Phase != OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBCOMP && item.Phase != OutboundDeliveryPhase_OUTBOUND_COMPLETE) {
			return errors.New("outbound delivery phase conflicts with QoS")
		}
		if seenOut[item.Publish.PacketId] {
			return errors.New("duplicate outbound packet ID")
		}
		seenOut[item.Publish.PacketId] = true
	}
	for i := 1; i < len(c.Inbound); i++ {
		if c.Inbound[i-1].Publish.PacketId >= c.Inbound[i].Publish.PacketId {
			return errors.New("inbound checkpoints are not uniquely sorted by packet ID")
		}
	}
	for i := 1; i < len(c.Outbound); i++ {
		if c.Outbound[i-1].Publish.PacketId >= c.Outbound[i].Publish.PacketId {
			return errors.New("outbound checkpoints are not uniquely sorted by packet ID")
		}
	}
	return nil
}
func validatePublish(p *PublishPacket, checkpoint bool) error {
	if p.ProtocolVersion != 4 && p.ProtocolVersion != 5 {
		return errors.New("publish protocol_version must be 4 or 5")
	}
	if p.Qos > 2 || ((p.Qos == 0) != (p.PacketId == 0)) {
		return errors.New("publish QoS and packet_id are inconsistent")
	}
	if checkpoint && p.Qos == 0 {
		return errors.New("checkpoint publish QoS must be 1 or 2")
	}
	if p.ProtocolVersion == 4 && p.HasMessageExpiryInterval {
		return errors.New("MQTT 3.1.1 publish cannot carry message expiry")
	}
	if !validTopic(p.Topic) {
		return errors.New("invalid MQTT topic")
	}
	if len(p.Payload) > maxPublishPayload {
		return errors.New("publish payload exceeds 1,000,000 bytes")
	}
	if !p.HasMessageExpiryInterval && p.MessageExpiryInterval != 0 {
		return errors.New("message expiry interval must be zero when absent")
	}
	return nil
}
func validateSnapshotState(s *ReplicatedStateSnapshot) error {
	count := len(s.Members) + len(s.Ownerships) + len(s.Subscriptions) + len(s.Checkpoints) + len(s.MembershipHistory)
	if count > maxEntries {
		return errors.New("snapshot exceeds 1,000,000 top-level entries")
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
	for _, c := range s.Checkpoints {
		if c == nil {
			return errors.New("nil snapshot checkpoint")
		}
		if err := validateCheckpoint(c); err != nil {
			return err
		}
	}
	for i := 1; i < len(s.Checkpoints); i++ {
		a, b := s.Checkpoints[i-1], s.Checkpoints[i]
		if compare2(a.TenantId, a.ClientId, b.TenantId, b.ClientId) >= 0 {
			return errors.New("snapshot checkpoints are not uniquely sorted")
		}
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
	return nil
}

func validateSnapshotMembershipIndexes(s *ReplicatedStateSnapshot, lastIncludedIndex uint64) error {
	for _, record := range s.MembershipHistory {
		if record.CommittedIndex > lastIncludedIndex {
			return errors.New("membership history committed_index exceeds last_included_index")
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
	sort.Slice(s.Checkpoints, func(i, j int) bool {
		if s.Checkpoints[i] == nil {
			return s.Checkpoints[j] != nil
		}
		if s.Checkpoints[j] == nil {
			return false
		}
		a, b := s.Checkpoints[i], s.Checkpoints[j]
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
	for _, c := range s.Checkpoints {
		if c == nil {
			continue
		}
		sort.Slice(c.Inbound, func(i, j int) bool {
			if c.Inbound[i] == nil || c.Inbound[i].Publish == nil {
				return false
			}
			if c.Inbound[j] == nil || c.Inbound[j].Publish == nil {
				return true
			}
			return c.Inbound[i].Publish.PacketId < c.Inbound[j].Publish.PacketId
		})
		sort.Slice(c.Outbound, func(i, j int) bool {
			if c.Outbound[i] == nil || c.Outbound[i].Publish == nil {
				return false
			}
			if c.Outbound[j] == nil || c.Outbound[j].Publish == nil {
				return true
			}
			return c.Outbound[i].Publish.PacketId < c.Outbound[j].Publish.PacketId
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
