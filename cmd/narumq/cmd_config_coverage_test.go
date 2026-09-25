package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	authenticationadapter "narumq/internal/adapters/authentication"
	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/adapters/raft/records"
	tcpadapter "narumq/internal/adapters/tcp"
	tlsadapter "narumq/internal/adapters/tls"
	appmgmt "narumq/internal/application/management"
	"narumq/internal/application/membership"
	portsmgmt "narumq/internal/application/ports/management"
)

const ccfOperation = "00000000-0000-4000-8000-0000000000cc"

func ccfDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func ccfStagedMember() membership.MemberIdentityDTO {
	return membership.MemberIdentityDTO{
		MemberID: 4, ClusterID: "cluster", Address: "node4.example:19004", DNSSAN: "node4.example",
		CertificateSHA256: bytes.Repeat([]byte{0x22}, 32), ReleaseVersion: "v1",
		ClusterProtocolMajor: 2, StateSchemaMajor: 1,
		Phase: membership.MemberPhaseStaged, OperationID: ccfOperation,
	}
}

func ccfWantError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want substring %q", err, want)
	}
}

// ccfCheckEnumMapping proves that an application enum maps 1:1 onto its wire enum,
// including the named endpoints, and that the zero value and the first value past
// the supported range are rejected in both directions.
func ccfCheckEnumMapping[A ~uint32, W ~int32](
	t *testing.T,
	first, last A,
	wireFirst, wireLast W,
	toWire func(A) (W, error),
	fromWire func(W) (A, error),
) {
	t.Helper()
	if got, err := toWire(first); err != nil || got != wireFirst {
		t.Fatalf("toWire(first=%d) = (%d, %v), want %d", first, got, err, wireFirst)
	}
	if got, err := toWire(last); err != nil || got != wireLast {
		t.Fatalf("toWire(last=%d) = (%d, %v), want %d", last, got, err, wireLast)
	}
	for value := first; value <= last; value++ {
		wire, err := toWire(value)
		if err != nil {
			t.Fatalf("toWire(%d): %v", value, err)
		}
		back, err := fromWire(wire)
		if err != nil || back != value {
			t.Fatalf("fromWire(toWire(%d)) = (%d, %v)", value, back, err)
		}
	}
	for _, invalid := range []A{first - 1, last + 1} {
		if _, err := toWire(invalid); err == nil {
			t.Fatalf("toWire accepted unsupported value %d", invalid)
		}
	}
	for _, invalid := range []W{wireFirst - 1, wireLast + 1} {
		if _, err := fromWire(invalid); err == nil {
			t.Fatalf("fromWire accepted unsupported value %d", invalid)
		}
	}
}

func TestMembershipRecordEnumMappingsAreBijectiveAndBounded(t *testing.T) {
	t.Run("member phase", func(t *testing.T) {
		ccfCheckEnumMapping(t, membership.MemberPhaseStaged, membership.MemberPhaseRetired,
			records.MemberPhase_MEMBER_PHASE_STAGED, records.MemberPhase_MEMBER_PHASE_RETIRED,
			memberPhaseToRecord, memberPhaseFromRecord)
	})
	t.Run("membership action", func(t *testing.T) {
		ccfCheckEnumMapping(t, membership.MembershipActionStageJoin, membership.MembershipActionAbortStage,
			records.MembershipAction_MEMBERSHIP_ACTION_STAGE_JOIN, records.MembershipAction_MEMBERSHIP_ACTION_ABORT_STAGE,
			membershipActionToRecord, membershipActionFromRecord)
	})
	t.Run("ownership action", func(t *testing.T) {
		ccfCheckEnumMapping(t, membership.OwnershipActionAcquire, membership.OwnershipActionRelease,
			records.OwnershipAction_OWNERSHIP_ACTION_ACQUIRE, records.OwnershipAction_OWNERSHIP_ACTION_RELEASE,
			ownershipActionToRecord, ownershipActionFromRecord)
	})
	t.Run("subscription action", func(t *testing.T) {
		ccfCheckEnumMapping(t, membership.SubscriptionActionAdd, membership.SubscriptionActionRemove,
			records.SubscriptionAction_SUBSCRIPTION_ACTION_ADD, records.SubscriptionAction_SUBSCRIPTION_ACTION_REMOVE,
			subscriptionActionToRecord, subscriptionActionFromRecord)
	})
	t.Run("inbound phase", func(t *testing.T) {
		ccfCheckEnumMapping(t, membership.InboundQoS1AwaitAcceptance, membership.InboundQoS2Complete,
			records.InboundDeliveryPhase_INBOUND_QOS1_AWAIT_ACCEPTANCE, records.InboundDeliveryPhase_INBOUND_QOS2_COMPLETE,
			inboundPhaseToRecord, inboundPhaseFromRecord)
	})
	t.Run("outbound phase", func(t *testing.T) {
		ccfCheckEnumMapping(t, membership.OutboundAwaitPubAck, membership.OutboundComplete,
			records.OutboundDeliveryPhase_OUTBOUND_AWAIT_PUBACK, records.OutboundDeliveryPhase_OUTBOUND_COMPLETE,
			outboundPhaseToRecord, outboundPhaseFromRecord)
	})
}

func TestMembershipRecordBridgeRejectsUnsupportedCommandValues(t *testing.T) {
	member := ccfStagedMember()
	invalidPhaseMember := member
	invalidPhaseMember.Phase = membership.MemberPhaseUnspecified
	publish := membership.PublishPacketDTO{ProtocolVersion: 5, QoS: 1, PacketID: 1, Topic: "a/b", Payload: []byte("p")}
	cases := []struct {
		name    string
		command membership.CommandDTO
		want    string
	}{
		{name: "member phase", command: membership.CommandDTO{Membership: &membership.MembershipRecordDTO{
			OperationID: ccfOperation, Action: membership.MembershipActionStageJoin, IdempotencyKey: "k", Member: &invalidPhaseMember,
		}}, want: "unsupported member phase"},
		{name: "membership action", command: membership.CommandDTO{Membership: &membership.MembershipRecordDTO{
			OperationID: ccfOperation, Action: membership.MembershipActionAbortStage + 1, IdempotencyKey: "k", Member: &member,
		}}, want: "unsupported membership action"},
		{name: "ownership action", command: membership.CommandDTO{Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: membership.OwnershipActionUnspecified,
		}}, want: "unsupported ownership action"},
		{name: "subscription action", command: membership.CommandDTO{Subscription: &membership.SubscriptionChangeDTO{
			TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Filter: "a/#", Action: membership.SubscriptionActionRemove + 1,
		}}, want: "unsupported subscription action"},
		{name: "inbound phase", command: membership.CommandDTO{Checkpoint: &membership.SessionCheckpointDTO{
			TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
			Inbound: []membership.InboundCheckpointDTO{{Publish: publish, Phase: membership.InboundQoS2Complete + 1}},
		}}, want: "unsupported inbound delivery phase"},
		{name: "outbound phase", command: membership.CommandDTO{Checkpoint: &membership.SessionCheckpointDTO{
			TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
			Outbound: []membership.OutboundCheckpointDTO{{Publish: publish, Phase: membership.OutboundPhaseUnspecified}},
		}}, want: "unsupported outbound delivery phase"},
		{name: "two payloads", command: membership.CommandDTO{
			Ownership: &membership.OwnershipChangeDTO{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: membership.OwnershipActionAcquire,
			},
			Subscription: &membership.SubscriptionChangeDTO{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Filter: "a/#", Action: membership.SubscriptionActionAdd,
			},
		}, want: "exactly one payload"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			command := test.command
			command.SchemaMajor = membership.SchemaMajor
			command.OperationID = ccfOperation
			wire, err := encodeMembershipCommand(command)
			if wire != nil {
				t.Fatalf("encode returned %d wire bytes with error", len(wire))
			}
			ccfWantError(t, err, test.want)
		})
	}
}

func TestMembershipRecordBridgeRoundTripsMemberlessRemoval(t *testing.T) {
	command := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, OperationID: ccfOperation,
		Membership: &membership.MembershipRecordDTO{
			OperationID: ccfOperation, Action: membership.MembershipActionRemoveMember,
			IdempotencyKey: "remove-2", TargetMemberID: 2, ExpectedConfStateIndex: 7,
		},
	}
	wire, err := encodeMembershipCommand(command)
	if err != nil {
		t.Fatalf("encode memberless removal: %v", err)
	}
	decoded, err := decodeMembershipCommand(wire)
	if err != nil {
		t.Fatalf("decode memberless removal: %v", err)
	}
	if decoded.Membership == nil || decoded.Membership.Member != nil {
		t.Fatalf("decoded removal member = %#v, want nil member", decoded.Membership)
	}
	if !reflect.DeepEqual(decoded, command) {
		t.Fatalf("memberless removal round trip = %#v, want %#v", decoded, command)
	}
}

func TestMembershipRecordBridgeRejectsMalformedWire(t *testing.T) {
	malformed := []byte{0xff, 0xff, 0xff}
	if command, err := decodeMembershipCommand(malformed); err == nil || !reflect.DeepEqual(command, membership.CommandDTO{}) {
		t.Fatalf("decodeMembershipCommand(malformed) = (%#v, %v), want zero command and error", command, err)
	}
	if snapshot, err := decodeMembershipSnapshot(malformed); err == nil || !reflect.DeepEqual(snapshot, membership.SnapshotDTO{}) {
		t.Fatalf("decodeMembershipSnapshot(malformed) = (%#v, %v), want zero snapshot and error", snapshot, err)
	}
	if _, err := checkpointFromRecord(nil); err == nil {
		t.Fatal("checkpointFromRecord accepted nil record")
	}
	if got := publishFromRecord(nil); !reflect.DeepEqual(got, membership.PublishPacketDTO{}) {
		t.Fatalf("publishFromRecord(nil) = %#v, want zero packet", got)
	}
}

func TestMembershipRecordBridgeRejectsInvalidSnapshots(t *testing.T) {
	member := ccfStagedMember()
	invalidPhaseMember := member
	invalidPhaseMember.Phase = membership.MemberPhaseRetired + 1
	base := func() membership.SnapshotDTO {
		return membership.SnapshotDTO{SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, LastIncludedIndex: 10}
	}
	cases := []struct {
		name string
		edit func(*membership.SnapshotDTO)
		want string
	}{
		{name: "schema", edit: func(s *membership.SnapshotDTO) { s.SchemaMinor = membership.SchemaMinor + 1 }, want: "schema is unsupported"},
		{name: "member phase", edit: func(s *membership.SnapshotDTO) {
			s.Members = []membership.MemberIdentityDTO{invalidPhaseMember}
			s.ObjectCount = 1
		}, want: "unsupported member phase"},
		{name: "checkpoint phase", edit: func(s *membership.SnapshotDTO) {
			s.Checkpoints = []membership.SessionCheckpointDTO{{
				TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
				Inbound: []membership.InboundCheckpointDTO{{Phase: membership.InboundPhaseUnspecified}},
			}}
			s.ObjectCount = 1
		}, want: "unsupported inbound delivery phase"},
		{name: "history member phase", edit: func(s *membership.SnapshotDTO) {
			s.MembershipHistory = []membership.MembershipRecordDTO{{
				OperationID: ccfOperation, Action: membership.MembershipActionStageJoin, IdempotencyKey: "k",
				Member: &invalidPhaseMember, CommittedIndex: 9,
			}}
			s.ObjectCount = 1
		}, want: "unsupported member phase"},
		{name: "history action", edit: func(s *membership.SnapshotDTO) {
			s.MembershipHistory = []membership.MembershipRecordDTO{{
				OperationID: ccfOperation, Action: membership.MembershipActionUnspecified, IdempotencyKey: "k",
				Member: &member, CommittedIndex: 9,
			}}
			s.ObjectCount = 1
		}, want: "unsupported membership action"},
		{name: "wire validation", edit: func(s *membership.SnapshotDTO) {
			s.Ownerships = []membership.OwnershipStateDTO{{TenantID: "tenant", ClientID: "client", OwnerNodeID: 0, Epoch: 1}}
			s.ObjectCount = 1
		}, want: "invalid snapshot ownership"},
		{name: "digest", edit: func(s *membership.SnapshotDTO) {
			s.Ownerships = []membership.OwnershipStateDTO{{TenantID: "tenant", ClientID: "client", OwnerNodeID: 1, Epoch: 1}}
			s.ObjectCount = 1
			s.Digest = make([]byte, 32)
		}, want: "digest does not match"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			snapshot := base()
			test.edit(&snapshot)
			wire, err := encodeMembershipSnapshot(snapshot)
			if wire != nil {
				t.Fatalf("encode returned %d wire bytes with error", len(wire))
			}
			ccfWantError(t, err, test.want)
		})
	}
}

func TestLoadClusterConfigFile(t *testing.T) {
	directory := t.TempDir()
	if _, err := loadClusterConfigFile(filepath.Join(directory, "missing.json")); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("load missing config error = %v, want os.ErrNotExist", err)
	}

	malformed := filepath.Join(directory, "malformed.json")
	if err := os.WriteFile(malformed, []byte(`{"mode":`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadClusterConfigFile(malformed)
	ccfWantError(t, err, "invalid JSON")

	want := validJoinerClusterConfig()
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	valid := filepath.Join(directory, "valid.json")
	if err := os.WriteFile(valid, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadClusterConfigFile(valid)
	if err != nil {
		t.Fatalf("load valid config: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded config = %#v, want %#v", got, want)
	}
}

func TestParseClusterConfigRejectsUnreadableAndStructurallyInvalidJSON(t *testing.T) {
	if _, err := parseClusterConfig(nil); err == nil || !strings.Contains(err.Error(), "reader is nil") {
		t.Fatalf("nil reader error = %v", err)
	}
	if _, err := parseClusterConfig(iotest.ErrReader(errors.New("disk failure"))); err == nil || err.Error() != "read cluster config" {
		t.Fatalf("reader failure error = %v, want read cluster config", err)
	}

	data, err := json.Marshal(validBootstrapClusterConfig())
	if err != nil {
		t.Fatal(err)
	}
	valid := string(data)
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "invalid UTF-8", input: strings.Replace(valid, `"sample-cluster"`, "\"sample-\xff\"", 1), want: "not valid UTF-8"},
		{name: "empty document", input: "", want: "cluster config has invalid JSON"},
		{name: "top-level array", input: `[]`, want: "invalid JSON field type"},
		{name: "top-level scalar", input: `"cluster"`, want: "invalid JSON field type"},
		{name: "object field as array", input: strings.Replace(valid, `"peerTLS":{"certFile":"certs/node1.crt","keyFile":"certs/node1.key","caFile":"certs/ca.crt"}`, `"peerTLS":[]`, 1), want: "invalid JSON field type"},
		{name: "array field as object", input: strings.Replace(valid, `"seedAddresses":[]`, `"seedAddresses":{}`, 1), want: "invalid JSON field type"},
		{name: "null array element", input: strings.Replace(valid, `"seedAddresses":[]`, `"seedAddresses":[null]`, 1), want: "invalid JSON field type"},
		{name: "missing comma between keys", input: `{"x":1 "y":2}`, want: "cluster config has invalid JSON"},
		{name: "unterminated nested object", input: `{"x":{`, want: "cluster config has invalid JSON"},
		{name: "unterminated nested array", input: `{"x":[`, want: "cluster config has invalid JSON"},
		{name: "missing bootstrap member key", input: strings.Replace(valid, `,"dnsSan":"node2.example"`, "", 1), want: `missing required field "dnsSan"`},
		{name: "missing management key", input: strings.Replace(valid, `"listenAddress":"127.0.0.1:18080",`, "", 1), want: `missing required field "listenAddress"`},
		{name: "missing mqtt key", input: strings.Replace(valid, `"tcpListenAddress":"",`, "", 1), want: `missing required field "tcpListenAddress"`},
		{name: "wrong scalar type", input: strings.Replace(valid, `"nodeId":1`, `"nodeId":"1"`, 1), want: "invalid JSON or field types"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseClusterConfig(strings.NewReader(test.input))
			ccfWantError(t, err, test.want)
		})
	}
}

func TestValidateClusterConfigReportsFirstInvalidField(t *testing.T) {
	cases := []struct {
		name string
		edit func(*clusterConfigFile)
		want string
	}{
		{name: "blank cluster ID", edit: func(c *clusterConfigFile) { c.ClusterID = " x" }, want: "clusterId must contain"},
		{name: "empty release", edit: func(c *clusterConfigFile) { c.ReleaseVersion = "" }, want: "releaseVersion must contain"},
		{name: "oversize release", edit: func(c *clusterConfigFile) { c.ReleaseVersion = strings.Repeat("v", 65) }, want: "releaseVersion must contain"},
		{name: "unknown role", edit: func(c *clusterConfigFile) { c.Role = "observer" }, want: "role must be bootstrap or joiner"},
		{name: "blank directory", edit: func(c *clusterConfigFile) { c.Directory = " data" }, want: "directory is required"},
		{name: "listen without port", edit: func(c *clusterConfigFile) { c.ListenAddress = "127.0.0.1" }, want: "listenAddress must be a valid host:port"},
		{name: "advertised zero port", edit: func(c *clusterConfigFile) { c.AdvertisedAddress = "127.0.0.1:0" }, want: "advertisedAddress must be"},
		{name: "local DNS IP", edit: func(c *clusterConfigFile) { c.LocalDNSName = "127.0.0.1" }, want: "localDnsName must be"},
		{name: "blank CA file", edit: func(c *clusterConfigFile) { c.PeerTLS.CAFile = "" }, want: "peerTLS certFile, keyFile, and caFile are required"},
		{name: "management wildcard DNS", edit: func(c *clusterConfigFile) { c.Management.ListenAddress = "*:8080" }, want: "management listenAddress"},
		{name: "mqtt TCP invalid", edit: func(c *clusterConfigFile) { c.MQTT.TCPListenAddress = "127.0.0.1:70000" }, want: "mqtt listener addresses"},
		{name: "mqtt TLS invalid", edit: func(c *clusterConfigFile) { c.MQTT.TLSListenAddress = "tls.example" }, want: "mqtt listener addresses"},
		{name: "missing security file", edit: func(c *clusterConfigFile) { c.MQTT.SecurityConfigFile = "" }, want: "mqtt securityConfigFile is required"},
		{name: "two bootstrap members", edit: func(c *clusterConfigFile) { c.BootstrapMembers = c.BootstrapMembers[:2] }, want: "exactly three members"},
		{name: "zero member ID", edit: func(c *clusterConfigFile) { c.BootstrapMembers[0].MemberID = 0 }, want: "uniquely sorted by positive memberId"},
		{name: "member wildcard address", edit: func(c *clusterConfigFile) { c.BootstrapMembers[1].Address = "0.0.0.0:19002" }, want: "bootstrap member identity is invalid"},
		{name: "member short fingerprint", edit: func(c *clusterConfigFile) {
			c.BootstrapMembers[2].CertificateSHA256 = strings.Repeat("c", 63)
		}, want: "bootstrap member identity is invalid"},
		{name: "local member address mismatch", edit: func(c *clusterConfigFile) { c.BootstrapMembers[0].Address = "node1.example:19001" }, want: "matching local node identity"},
		{name: "too many joiner seeds", edit: func(c *clusterConfigFile) {
			*c = validJoinerClusterConfig()
			c.SeedAddresses = nil
			for index := 0; index < 17; index++ {
				c.SeedAddresses = append(c.SeedAddresses, "seed"+strings.Repeat("x", index)+".example:19001")
			}
		}, want: "1 to 16 addresses"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := validBootstrapClusterConfig()
			test.edit(&config)
			ccfWantError(t, validateClusterConfig(config), test.want)
		})
	}
	if err := validateClusterConfig(validBootstrapClusterConfig()); err != nil {
		t.Fatalf("baseline bootstrap config rejected: %v", err)
	}
}

func TestClusterAddressAndNameValidators(t *testing.T) {
	longLabel := strings.Repeat("a", 64) + ".example"
	longName := strings.Repeat("abcdefghi.", 26) + "example"
	dnsCases := []struct {
		value string
		want  bool
	}{
		{value: "node-1.example", want: true},
		{value: "", want: false},
		{value: longName, want: false},
		{value: longLabel, want: false},
		{value: "node..example", want: false},
		{value: "-node.example", want: false},
		{value: "node-.example", want: false},
		{value: "node_1.example", want: false},
		{value: "Node.example", want: false},
		{value: "10.0.0.1", want: false},
	}
	for _, test := range dnsCases {
		if got := validDNSName(test.value); got != test.want {
			t.Errorf("validDNSName(%q) = %t, want %t", test.value, got, test.want)
		}
	}

	hostPortCases := []struct {
		address  string
		wildcard bool
		want     bool
	}{
		{address: "node.example:1883", want: true},
		{address: "[::1]:1883", want: true},
		{address: "[::]:1883", wildcard: true, want: true},
		{address: "[::]:1883", want: false},
		{address: "", want: false},
		{address: " node.example:1883", want: false},
		{address: strings.Repeat("a", 250) + ".ex:1", want: false},
		{address: "node.example", want: false},
		{address: ":1883", want: false},
		{address: "node.example:", want: false},
		{address: "node.example:0", want: false},
		{address: "node.example:65536", want: false},
		{address: "node.example:+1", want: false},
		{address: "[::0]:1883", wildcard: true, want: false},
		{address: "[0:0::1]:1883", want: false},
		{address: "bad_host:1883", want: false},
	}
	for _, test := range hostPortCases {
		if got := validHostPort(test.address, test.wildcard); got != test.want {
			t.Errorf("validHostPort(%q, %t) = %t, want %t", test.address, test.wildcard, got, test.want)
		}
	}

	fingerprintCases := []struct {
		value string
		want  bool
	}{
		{value: strings.Repeat("0123456789abcdef", 4), want: true},
		{value: strings.Repeat("a", 63), want: false},
		{value: strings.Repeat("a", 65), want: false},
		{value: strings.Repeat("g", 64), want: false},
	}
	for _, test := range fingerprintCases {
		if got := validFingerprint(test.value); got != test.want {
			t.Errorf("validFingerprint(%q) = %t, want %t", test.value, got, test.want)
		}
	}
}

func TestParseRaftPeerConfigRejectsMissingFields(t *testing.T) {
	if _, err := parseRaftPeerConfig(nil); err == nil || !strings.Contains(err.Error(), "reader is nil") {
		t.Fatalf("nil reader error = %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*raftPeerConfigFile)
		want   string
	}{
		{name: "blank cluster ID", mutate: func(c *raftPeerConfigFile) { c.ClusterID = "  " }, want: "clusterId is required"},
		{name: "blank listen address", mutate: func(c *raftPeerConfigFile) { c.ListenAddress = "" }, want: "listenAddress is required"},
		{name: "blank local DNS", mutate: func(c *raftPeerConfigFile) { c.LocalDNSName = " " }, want: "localDnsName is required"},
		{name: "blank cert", mutate: func(c *raftPeerConfigFile) { c.CertFile = "" }, want: "certFile, keyFile, and caFile are required"},
		{name: "blank key", mutate: func(c *raftPeerConfigFile) { c.KeyFile = "" }, want: "certFile, keyFile, and caFile are required"},
		{name: "blank CA", mutate: func(c *raftPeerConfigFile) { c.CAFile = "" }, want: "certFile, keyFile, and caFile are required"},
		{name: "peer outside voters", mutate: func(c *raftPeerConfigFile) { c.Peers[1].ID = 4 }, want: "peer id 4 must be one of the fixed voters"},
		{name: "peer zero", mutate: func(c *raftPeerConfigFile) { c.Peers[0].ID = 0 }, want: "peer id 0 must be one of the fixed voters"},
		{name: "peer blank address", mutate: func(c *raftPeerConfigFile) { c.Peers[0].Address = " " }, want: "peer 2 address and dnsName are required"},
		{name: "peer blank DNS", mutate: func(c *raftPeerConfigFile) { c.Peers[1].DNSName = "" }, want: "peer 3 address and dnsName are required"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseRaftPeerConfig(strings.NewReader(mustMarshalPeerConfig(t, test.mutate)))
			ccfWantError(t, err, test.want)
		})
	}
}

type ccfPeerFixture struct {
	directory string
	config    raftPeerConfigFile
}

func newCCFPeerFixture(t *testing.T) ccfPeerFixture {
	t.Helper()
	directory := t.TempDir()
	caPEM, files := writePeerOnlyCertificates(t, directory)
	caPath := filepath.Join(directory, "ca.crt")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return ccfPeerFixture{directory: directory, config: raftPeerConfigFile{
		ClusterID:     "ccf-cluster",
		NodeID:        1,
		Directory:     filepath.Join(directory, "node-1"),
		ListenAddress: clusterRuntimeTestFreeAddress(t),
		LocalDNSName:  "node1.example",
		CertFile:      files[1].certPath,
		KeyFile:       files[1].keyPath,
		CAFile:        caPath,
		Peers: []raftPeerConfigEndpoint{
			{ID: 2, Address: "127.0.0.1:19002", DNSName: "node2.example"},
			{ID: 3, Address: "127.0.0.1:19003", DNSName: "node3.example"},
		},
	}}
}

func (fixture ccfPeerFixture) write(t *testing.T, config raftPeerConfigFile) string {
	t.Helper()
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(fixture.directory, "peer.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func ccfAssertAddressReleased(t *testing.T, address string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listen address %s still held after failed startup: %v", address, err)
	}
	_ = listener.Close()
}

func TestRunRaftPeerOnlyStartupFailuresReleaseResources(t *testing.T) {
	t.Run("missing config file", func(t *testing.T) {
		err := runWithContext(context.Background(), []string{"-raft-peer-config", filepath.Join(t.TempDir(), "missing.json")}, ccfDiscardLogger())
		ccfWantError(t, err, "load raft peer config: read raft peer config file")
	})
	t.Run("invalid config file", func(t *testing.T) {
		fixture := newCCFPeerFixture(t)
		config := fixture.config
		config.NodeID = 4
		err := runRaftPeerOnly(context.Background(), fixture.write(t, config), ccfDiscardLogger())
		ccfWantError(t, err, "load raft peer config: nodeId must be one of the fixed voters")
	})
	t.Run("missing TLS material", func(t *testing.T) {
		fixture := newCCFPeerFixture(t)
		config := fixture.config
		config.KeyFile = filepath.Join(fixture.directory, "missing.key")
		err := runRaftPeerOnly(context.Background(), fixture.write(t, config), ccfDiscardLogger())
		ccfWantError(t, err, "configure raft peer TLS")
	})
	t.Run("listen address in use", func(t *testing.T) {
		fixture := newCCFPeerFixture(t)
		occupied, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer occupied.Close()
		config := fixture.config
		config.ListenAddress = occupied.Addr().String()
		err = runRaftPeerOnly(context.Background(), fixture.write(t, config), ccfDiscardLogger())
		ccfWantError(t, err, "listen for raft peers")
	})
	t.Run("raft directory is a file", func(t *testing.T) {
		fixture := newCCFPeerFixture(t)
		config := fixture.config
		config.Directory = filepath.Join(fixture.directory, "not-a-directory")
		if err := os.WriteFile(config.Directory, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := runRaftPeerOnly(context.Background(), fixture.write(t, config), ccfDiscardLogger())
		ccfWantError(t, err, "open raft node")
		ccfAssertAddressReleased(t, config.ListenAddress)
	})
	t.Run("transport rejects local DNS name", func(t *testing.T) {
		fixture := newCCFPeerFixture(t)
		config := fixture.config
		config.LocalDNSName = "Invalid_Name"
		err := runRaftPeerOnly(context.Background(), fixture.write(t, config), ccfDiscardLogger())
		ccfWantError(t, err, "create raft peer transport")
		ccfAssertAddressReleased(t, config.ListenAddress)
		ccfAssertDirectoryReleased(t, config.Directory)
	})
}

// ccfAssertDirectoryReleased removes a Raft directory; on Windows this fails
// while the node still holds its WAL or metadata files open.
func ccfAssertDirectoryReleased(t *testing.T, directory string) {
	t.Helper()
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("raft directory %s was not created: %v", directory, err)
	}
	if err := os.RemoveAll(directory); err != nil {
		t.Fatalf("raft directory %s still in use after runtime returned: %v", directory, err)
	}
}

func TestRunRaftPeerOnlyCanceledContextStopsAndReleasesNode(t *testing.T) {
	fixture := newCCFPeerFixture(t)
	path := fixture.write(t, fixture.config)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runRaftPeerOnly(ctx, path, nil); err != nil {
		t.Fatalf("canceled peer-only run error = %v", err)
	}
	ccfAssertAddressReleased(t, fixture.config.ListenAddress)
	ccfAssertDirectoryReleased(t, fixture.config.Directory)
}

func ccfOpenDriverNode(t *testing.T) *raftadapter.Node {
	t.Helper()
	node, err := raftadapter.Open(raftadapter.Config{
		NodeID:    1,
		PeerIDs:   []raftadapter.NodeID{1, 2, 3},
		Directory: filepath.Join(t.TempDir(), "node"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	return node
}

func ccfRunDriver(t *testing.T, ctx context.Context, node *raftadapter.Node, transport *tcpadapter.TCPPeerTransport, logger *slog.Logger) error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- runRaftPeerDriver(ctx, node, transport, logger) }()
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("raft peer driver did not return")
		return nil
	}
}

func TestRunRaftPeerDriverRejectsMissingDependencies(t *testing.T) {
	node := ccfOpenDriverNode(t)
	transport := newDriverQueueTransport(t)
	cases := []struct {
		name      string
		ctx       context.Context
		node      *raftadapter.Node
		transport *tcpadapter.TCPPeerTransport
	}{
		{name: "context", node: node, transport: transport},
		{name: "node", ctx: context.Background(), transport: transport},
		{name: "transport", ctx: context.Background(), node: node},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := runRaftPeerDriver(test.ctx, test.node, test.transport, ccfDiscardLogger())
			ccfWantError(t, err, "requires context, node, and transport")
		})
	}
}

func TestRunRaftPeerDriverStopsOnCanceledContextAndClosesTransport(t *testing.T) {
	node := ccfOpenDriverNode(t)
	transport := newDriverQueueTransport(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ccfRunDriver(t, ctx, node, transport, nil); err != nil {
		t.Fatalf("driver with canceled context error = %v", err)
	}
	if _, ok := <-transport.Receive(); ok {
		t.Fatal("transport Receive stayed open after driver returned")
	}
}

func TestRunRaftPeerDriverFailsWhenNodeIsClosed(t *testing.T) {
	node := ccfOpenDriverNode(t)
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	transport := newDriverQueueTransport(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := ccfRunDriver(t, ctx, node, transport, ccfDiscardLogger())
	ccfWantError(t, err, "tick raft node")
	if !errors.Is(err, raftadapter.ErrClosed) {
		t.Fatalf("driver error = %v, want raft ErrClosed", err)
	}
}

func TestRunRaftPeerDriverFailsWhenTransportStopsUnexpectedly(t *testing.T) {
	node := ccfOpenDriverNode(t)
	transport := newDriverQueueTransport(t)
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := ccfRunDriver(t, ctx, node, transport, ccfDiscardLogger())
	ccfWantError(t, err, "raft peer transport")
}

func TestProcessRaftReadyAndMessagesReportFailures(t *testing.T) {
	logger := ccfDiscardLogger()
	t.Run("closed node Ready", func(t *testing.T) {
		node := ccfOpenDriverNode(t)
		if err := node.Close(); err != nil {
			t.Fatal(err)
		}
		err := processRaftReady(context.Background(), node, newDriverQueueTransport(t), logger)
		ccfWantError(t, err, "process raft Ready")
	})

	t.Run("closed transport", func(t *testing.T) {
		node := ccfOpenDriverNode(t)
		transport := newDriverQueueTransport(t)
		if err := transport.Close(); err != nil {
			t.Fatal(err)
		}
		messages := []raftadapter.MessageDTO{{From: 1, To: 2, Data: []byte{1}}}
		err := processRaftMessages(context.Background(), node, transport, logger, messages)
		ccfWantError(t, err, "queue raft message for peer 2")
		if !errors.Is(err, tcpadapter.ErrPeerTransportClosed) {
			t.Fatalf("error = %v, want ErrPeerTransportClosed", err)
		}
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := processRaftMessages(canceled, node, transport, logger, messages); err != nil {
			t.Fatalf("closed transport during shutdown error = %v, want nil", err)
		}
	})

	t.Run("backpressure report on closed node", func(t *testing.T) {
		node := ccfOpenDriverNode(t)
		transport := newDriverQueueTransport(t)
		for range 32 {
			if err := transport.Send(tcpadapter.PeerMessageDTO{From: 1, To: 3, Data: []byte{0xff}}); err != nil {
				t.Fatalf("fill peer 3 queue: %v", err)
			}
		}
		if err := node.Close(); err != nil {
			t.Fatal(err)
		}
		messages := []raftadapter.MessageDTO{{From: 1, To: 3, Data: []byte{1}}, {From: 1, To: 2, Data: []byte{2}}}
		err := processRaftMessages(context.Background(), node, transport, logger, messages)
		ccfWantError(t, err, "report peer 3 unreachable after backpressure")

		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		before, err := transport.Status(2)
		if err != nil {
			t.Fatal(err)
		}
		if err := processRaftMessages(canceled, node, transport, logger, messages); err != nil {
			t.Fatalf("report failure during shutdown error = %v, want nil", err)
		}
		after, err := transport.Status(2)
		if err != nil {
			t.Fatal(err)
		}
		if after.OutboundQueuedMessages != before.OutboundQueuedMessages+1 {
			t.Fatalf("healthy peer queue = %d, want %d after suppressed report failure", after.OutboundQueuedMessages, before.OutboundQueuedMessages+1)
		}
	})
}

func TestLogRaftPeerBackpressureUnknownPeerOmitsQueueStatus(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	logRaftPeerBackpressure(newDriverQueueTransport(t), logger, 9)
	logged := output.String()
	if !strings.Contains(logged, "raft peer transport backpressure") || !strings.Contains(logged, "peer_id=9") {
		t.Fatalf("backpressure log = %q, want message with peer_id=9", logged)
	}
	if strings.Contains(logged, "queued_messages") {
		t.Fatalf("backpressure log for unknown peer included queue status: %q", logged)
	}
}

func TestRaftRoleName(t *testing.T) {
	cases := map[raftadapter.Role]string{
		raftadapter.RoleFollower:  "follower",
		raftadapter.RoleCandidate: "candidate",
		raftadapter.RoleLeader:    "leader",
		raftadapter.Role(99):      "unknown",
	}
	for role, want := range cases {
		if got := raftRoleName(role); got != want {
			t.Errorf("raftRoleName(%d) = %q, want %q", role, got, want)
		}
	}
}

func TestClusterFailpointIgnoresUnknownConfigurationAction(t *testing.T) {
	for _, point := range validClusterFailpoints {
		for _, action := range []raftadapter.MembershipChangeAction{0, raftadapter.MembershipRemove + 1} {
			if point.matchesConfigurationApply(action) {
				t.Errorf("%s matched unknown configuration action %d", point, action)
			}
		}
	}
}

func TestParseSecurityConfigRejectsIncompleteAuthenticationAndACL(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "identities without jwt",
			input: `{"identities":[{"issuer":"https://issuer.example","subject":"s","tenantId":"t","credentialId":"c","allowedClientIds":["c"],"roles":["r"]}],"acl":[]}`,
			want:  "identities require jwt",
		},
		{name: "missing cache TTL", input: strings.Replace(validSecurityJSON, `"cacheTtl": "15m"`, `"cacheTtl": ""`, 1), want: "jwt.cacheTtl is required"},
		{name: "missing refresh cooldown", input: strings.Replace(validSecurityJSON, `"refreshCooldown": "30s"`, `"refreshCooldown": ""`, 1), want: "jwt.refreshCooldown is required"},
		{name: "mTLS empty identity", input: strings.Replace(validMTLSSecurityJSON, `"identity": "device-1.example"`, `"identity": ""`, 1), want: "mTLS identity is required"},
		{name: "mTLS invalid tenant", input: strings.Replace(validMTLSSecurityJSON, `"tenantId": "tenant-a"`, `"tenantId": ""`, 1), want: "invalid mTLS tenant ID"},
		{name: "mTLS empty subject", input: strings.Replace(validMTLSSecurityJSON, `"subject": "device-1"`, `"subject": ""`, 1), want: "mTLS identity subject is required"},
		{name: "mTLS empty credential", input: strings.Replace(validMTLSSecurityJSON, `"credentialId": "device-1-cert"`, `"credentialId": ""`, 1), want: "mTLS identity credentialId is required"},
		{name: "mTLS no client IDs", input: strings.Replace(validMTLSSecurityJSON, `"allowedClientIds": ["device-1"]`, `"allowedClientIds": []`, 1), want: "must have at least one allowed Client ID"},
		{name: "mTLS no roles", input: strings.Replace(validMTLSSecurityJSON, `"roles": ["publisher"]`, `"roles": []`, 1), want: "must have at least one role"},
		{name: "ACL invalid tenant", input: strings.Replace(validSecurityJSON, "\"tenantId\": \"tenant-a\",\n      \"role\"", "\"tenantId\": \"\",\n      \"role\"", 1), want: "invalid ACL rule tenantId"},
		{name: "ACL empty role", input: strings.Replace(validSecurityJSON, `"role": "publisher"`, `"role": ""`, 1), want: "create ACL rule"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if test.input == validSecurityJSON || test.input == validMTLSSecurityJSON {
				t.Fatal("test mutation did not change the fixture")
			}
			bundle, err := parseSecurityConfig(strings.NewReader(test.input))
			if bundle != nil {
				t.Fatalf("parseSecurityConfig returned bundle %#v with error", bundle)
			}
			ccfWantError(t, err, test.want)
		})
	}
}

func TestParseSecurityConfigAcceptsLocalhostJWKSAndEmailSAN(t *testing.T) {
	for _, jwksURL := range []string{"http://localhost:8080/jwks.json", "http://LOCALHOST/jwks.json"} {
		input := strings.Replace(validSecurityJSON, `"https://issuer.example/.well-known/jwks.json"`, `"`+jwksURL+`"`, 1)
		bundle, err := parseSecurityConfig(strings.NewReader(input))
		if err != nil {
			t.Fatalf("loopback JWKS URL %q rejected: %v", jwksURL, err)
		}
		if bundle.JWTConfig.JWKSURL != jwksURL {
			t.Fatalf("JWKS URL = %q, want %q", bundle.JWTConfig.JWKSURL, jwksURL)
		}
	}
	if _, err := parseSecurityConfig(strings.NewReader(strings.Replace(validSecurityJSON, `"https://issuer.example/.well-known/jwks.json"`, `"http://localhost.example/jwks.json"`, 1))); err == nil {
		t.Fatal("non-loopback HTTP JWKS host accepted")
	}

	bundle, err := parseSecurityConfig(strings.NewReader(strings.Replace(validMTLSSecurityJSON, `"commonName"`, `"emailSan"`, 1)))
	if err != nil {
		t.Fatalf("emailSan identity rejected: %v", err)
	}
	if got := bundle.MTLSBindings[0].Kind; got != authenticationadapter.ClientCertificateIdentityKindEmailSAN {
		t.Fatalf("mTLS identity kind = %q, want emailSan", got)
	}
}

func TestSecurityValidatorsTrackCredentialsWithoutSharedSet(t *testing.T) {
	jwt := &rawJWTConfig{
		Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"},
		JWKSURL: "https://issuer.example/jwks.json", CacheTTL: "1m", RefreshCooldown: "1s", ClockSkew: "1s", HTTPTimeout: "1s",
	}
	identity := rawIdentityConfig{
		Issuer: jwt.Issuer, Subject: "device-1", TenantID: "tenant-a", CredentialID: "shared",
		AllowedClientIDs: []string{"device-1"}, Roles: []string{"publisher"},
	}
	config, err := validateJWTAndIdentities(jwt, []rawIdentityConfig{identity}, nil)
	if err != nil {
		t.Fatalf("validateJWTAndIdentities(nil set): %v", err)
	}
	if len(config.Bindings) != 1 || config.Bindings[0].CredentialID != "shared" {
		t.Fatalf("JWT bindings = %#v, want one shared credential binding", config.Bindings)
	}
	second := identity
	second.Subject = "device-2"
	if _, err := validateJWTAndIdentities(jwt, []rawIdentityConfig{identity, second}, nil); err == nil || !strings.Contains(err.Error(), `duplicate credentialId "shared"`) {
		t.Fatalf("duplicate JWT credential error = %v", err)
	}

	mtls := rawMTLSIdentityConfig{
		IdentityKind: "dnsSan", Identity: "a.example", TenantID: "tenant-a", Subject: "a", CredentialID: "cert",
		AllowedClientIDs: []string{"a"}, Roles: []string{"publisher"},
	}
	duplicate := mtls
	duplicate.Identity = "b.example"
	if _, err := validateMTLSIdentities([]rawMTLSIdentityConfig{mtls, duplicate}, nil); err == nil || !strings.Contains(err.Error(), `duplicate credentialId "cert"`) {
		t.Fatalf("duplicate mTLS credential error = %v", err)
	}
	bindings, err := validateMTLSIdentities([]rawMTLSIdentityConfig{mtls}, nil)
	if err != nil || len(bindings) != 1 || bindings[0].Value != "a.example" {
		t.Fatalf("validateMTLSIdentities(nil set) = (%#v, %v)", bindings, err)
	}
}

func TestParsePositiveDuration(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{raw: "", want: "timeout is required"},
		{raw: "soon", want: "invalid timeout duration"},
		{raw: "0s", want: "timeout must be greater than zero"},
		{raw: "-1s", want: "timeout must be greater than zero"},
	}
	for _, test := range cases {
		_, err := parsePositiveDuration("timeout", test.raw)
		ccfWantError(t, err, test.want)
	}
	if got, err := parsePositiveDuration("timeout", "1500ms"); err != nil || got != 1500*time.Millisecond {
		t.Fatalf("parsePositiveDuration(1500ms) = (%v, %v)", got, err)
	}
}

func TestRunWithContextClusterConfigLoading(t *testing.T) {
	directory := t.TempDir()
	err := runWithContext(context.Background(), []string{"-cluster-config", filepath.Join(directory, "missing.json")}, ccfDiscardLogger())
	ccfWantError(t, err, "load cluster config: read cluster config file")

	config := validBootstrapClusterConfig()
	config.Directory = filepath.Join(directory, "data")
	config.PeerTLS = clusterPeerTLS{
		CertFile: filepath.Join(directory, "missing.crt"),
		KeyFile:  filepath.Join(directory, "missing.key"),
		CAFile:   filepath.Join(directory, "missing-ca.crt"),
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "cluster.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	err = runWithContext(context.Background(), []string{"-cluster-config", path}, ccfDiscardLogger())
	ccfWantError(t, err, "configure cluster peer TLS")
	if strings.Contains(err.Error(), "load cluster config") {
		t.Fatalf("valid cluster config was rejected while loading: %v", err)
	}
	if _, statErr := os.Stat(config.Directory); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("cluster data directory created before TLS validation: %v", statErr)
	}
}

func TestRunWithContextServesTCPAndTLSListenersTogether(t *testing.T) {
	directory := t.TempDir()
	_, files := writePeerOnlyCertificates(t, directory)
	securityPath := filepath.Join(directory, "security.json")
	if err := os.WriteFile(securityPath, []byte(validSecurityJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	logs := &lockedLogBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runWithContext(ctx, []string{
		"-security-config", securityPath,
		"-tcp-listen", "127.0.0.1:0",
		"-tls-listen", "127.0.0.1:0",
		"-tls-cert", files[1].certPath,
		"-tls-key", files[1].keyPath,
	}, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatalf("TCP+TLS run error = %v", err)
	}
	logged := logs.String()
	for _, want := range []string{"MQTT TCP listener started", "MQTT TLS listener started", "MQTT TCP listener stopped", "MQTT TLS listener stopped"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("logs missing %q:\n%s", want, logged)
		}
	}
}

func TestRunWithContextReportsManagementListenFailure(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	address := occupied.Addr().String()
	err = runWithContext(context.Background(), []string{"-http-listen", address, "-http-auth-token", "token"}, ccfDiscardLogger())
	ccfWantError(t, err, "listen management HTTP on")
}

func TestValidateTLSAuthenticationBindingsRequireMode(t *testing.T) {
	err := validateTLSAuthenticationBindings(tlsadapter.ClientAuthRequire, nil)
	ccfWantError(t, err, "-tls-client-auth require requires at least one mTLS identity binding")
	bundle := &SecurityBundle{MTLSBindings: make([]authenticationadapter.ClientCertificateIdentityBinding, 1)}
	if err := validateTLSAuthenticationBindings(tlsadapter.ClientAuthRequire, bundle); err != nil {
		t.Fatalf("require mode with mTLS binding error = %v", err)
	}
}

func TestServeServicesPropagatesServiceFailureAndDrains(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	state := appmgmt.NewService(appmgmt.ServiceConfig{}, nil, nil, nil, nil)
	state.SetNodeState(portsmgmt.NodeStateReady, nil)
	sampler := newRuntimeMetricsTestSampler(t)
	httpServer := &http.Server{Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}

	result := make(chan error, 1)
	go func() { result <- serveServices(nil, nil, httpServer, listener, sampler, state) }()
	select {
	case err = <-result:
	case <-time.After(10 * time.Second):
		t.Fatal("serveServices did not stop after management listener failure")
	}
	ccfWantError(t, err, "management HTTP listener")
	if strings.Contains(err.Error(), "runtime metrics sampler") {
		t.Fatalf("healthy sampler reported an error during failure shutdown: %v", err)
	}
	if got := state.NodeState(); got != portsmgmt.NodeStateSafeToStop {
		t.Fatalf("node state after service failure = %s, want %s", got, portsmgmt.NodeStateSafeToStop)
	}
}

type ccfFailingListener struct {
	err error
}

func (listener ccfFailingListener) Accept() (net.Conn, error) { return nil, listener.err }
func (listener ccfFailingListener) Close() error              { return nil }
func (listener ccfFailingListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
}

func TestServeWithRuntimeMetricsReportsServerFailure(t *testing.T) {
	_, server := newRuntimeMetricsTestServer(t)
	acceptErr := errors.New("ccf accept failure")
	if err := server.AddListener(ccfFailingListener{err: acceptErr}); err != nil {
		t.Fatal(err)
	}
	sampler := newRuntimeMetricsTestSampler(t)
	err := serveWithRuntimeMetrics(context.Background(), server, sampler)
	if !errors.Is(err, acceptErr) {
		t.Fatalf("serveWithRuntimeMetrics error = %v, want accept failure", err)
	}
	ccfWantError(t, err, "serve MQTT TCP listener")
	if strings.Contains(err.Error(), "runtime metrics sampler") {
		t.Fatalf("healthy sampler reported an error after server failure: %v", err)
	}
}
