package main

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"narumq/internal/adapters/raft/records"
	"narumq/internal/application/membership"
)

func TestEncodeMembershipCommandProposalDigestVerification(t *testing.T) {
	cmd := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor,
		SchemaMinor: membership.SchemaMinor,
		OperationID: "00000000-0000-4000-8000-000000000001",
		Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "client-a", ExpectedEpoch: 0,
			OwnerNodeID: 1, Epoch: 1, Action: membership.OwnershipActionAcquire,
		},
		Proposal: &membership.ProposalIdentityDTO{
			OriginNodeID:   1,
			ProofIndex:     10,
			RetiredThrough: 5,
			RequestID:      "00000000-0000-4000-8000-000000000001",
		},
	}
	encoded, err := encodeMembershipCommand(cmd)
	if err != nil {
		t.Fatalf("encodeMembershipCommand: %v", err)
	}

	decoded, err := decodeMembershipCommand(encoded)
	if err != nil {
		t.Fatalf("decodeMembershipCommand: %v", err)
	}
	if decoded.Proposal == nil || decoded.Proposal.OriginNodeID != 1 || decoded.Proposal.ProofIndex != 10 {
		t.Fatalf("decoded proposal mismatch: %+v", decoded.Proposal)
	}
	if len(decoded.Proposal.CommandDigest) != sha256.Size {
		t.Fatalf("digest length = %d, want %d", len(decoded.Proposal.CommandDigest), sha256.Size)
	}

	// Unmarshal wire and tamper with digest: must be rejected
	wire, err := records.UnmarshalCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	wire.Proposal.CommandDigest[0] ^= 0xff
	if _, err := records.MarshalCommand(wire); err == nil {
		t.Fatal("expected MarshalCommand to reject tampered command digest")
	}
}

func TestProposalOriginsSnapshotBridgeRoundTrip(t *testing.T) {
	receiptResult := membership.ApplyResultDTO{
		Index:   5,
		Outcome: membership.ApplyOutcomeApplied,
		Ownership: &membership.OwnershipStateDTO{
			TenantID: "acme", ClientID: "c1", OwnerNodeID: 1, Epoch: 1, CommittedIndex: 5,
		},
	}
	snapshot := membership.SnapshotDTO{
		SchemaMajor:       membership.SchemaMajor,
		SchemaMinor:       membership.SchemaMinor,
		LastIncludedIndex: 10,
		Members: []membership.MemberIdentityDTO{
			{MemberID: 1, ClusterID: "c", Address: "n:1", DNSSAN: "n", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
		},
		ActiveFeatures: []string{membership.ProposalRecoveryFeature},
		ProposalOrigins: []membership.ProposalOriginStateDTO{
			{
				OriginNodeID:   1,
				RetiredThrough: 2,
				Receipts: []membership.ProposalReceiptDTO{
					{
						RequestID:     "00000000-0000-4000-8000-000000000005",
						ProofIndex:    3,
						CommandDigest: make([]byte, 32),
						Result:        receiptResult,
					},
				},
			},
		},
	}
	snapshot.ObjectCount = uint64(len(snapshot.Members) + len(snapshot.ActiveFeatures) + 2) // 1 origin + 1 receipt = 2

	encoded, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("encodeMembershipSnapshot: %v", err)
	}

	decoded, err := decodeMembershipSnapshot(encoded)
	if err != nil {
		t.Fatalf("decodeMembershipSnapshot: %v", err)
	}
	restored := membership.NewStateMachine()
	if err := restored.Restore(decoded); err != nil {
		t.Fatal(err)
	}
	replay, err := restored.Apply(11, membership.CommandDTO{SchemaMajor: membership.SchemaMajor, OperationID: "00000000-0000-4000-8000-000000000005",
		Ownership: &membership.OwnershipChangeDTO{TenantID: "acme", ClientID: "c1", OwnerNodeID: 1, Epoch: 1, Action: membership.OwnershipActionAcquire},
		Proposal:  &membership.ProposalIdentityDTO{OriginNodeID: 1, ProofIndex: 3, RequestID: "00000000-0000-4000-8000-000000000005", CommandDigest: make([]byte, 32)}})
	if err != nil || !replay.Replayed || replay.Ownership == nil || replay.Ownership.Epoch != 1 || replay.Index != 5 {
		t.Fatalf("restored ownership result: %+v %v", replay, err)
	}
	if _, found := restored.GetOwnership("acme", "c1"); found {
		t.Fatal("restored receipt replay acquired ownership again")
	}
}

func TestProposalOriginsDiverseResultsBridgeRoundTrip(t *testing.T) {
	testResults := []membership.ApplyResultDTO{
		{
			Index: 1, Outcome: membership.ApplyOutcomeApplied,
			Operation: &membership.MembershipOperationDTO{
				OperationID:    "00000000-0000-4000-8000-000000000011",
				Action:         membership.MembershipActionStageJoin,
				MemberID:       4,
				Phase:          membership.OperationPhaseStaged,
				CommittedIndex: 1,
				Record: membership.MembershipRecordDTO{
					OperationID:    "00000000-0000-4000-8000-000000000011",
					Action:         membership.MembershipActionStageJoin,
					IdempotencyKey: "k1",
					Member: &membership.MemberIdentityDTO{
						MemberID: 4, ClusterID: "c", Address: "n:4", DNSSAN: "n",
						CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.0.0",
						ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseStaged,
						OperationID: "00000000-0000-4000-8000-000000000011",
					},
					CommittedIndex: 1,
				},
			},
		},
		{
			Index: 2, Outcome: membership.ApplyOutcomeApplied,
			Subscription: &membership.SubscriptionStateDTO{
				TenantID: "acme", ClientID: "c2", OwnerNodeID: 1, Epoch: 1, Filter: "t/#", QoS: 1,
			},
		},
		{
			Index: 3, Outcome: membership.ApplyOutcomeApplied,
			Delivery: &membership.DeliveryResultDTO{
				Found: true, Released: true,
				Enqueued:  []membership.DeliveryEnqueuedDTO{{ClientID: "c1", Sequence: 1}},
				Saturated: []string{"c2"},
				Skipped:   []string{"c3"},
				Session: &membership.DeliverySessionStateDTO{
					TenantID: "acme", ClientID: "c1", NextSequence: 2,
					Messages: []membership.DeliveryMessageDTO{
						{Sequence: 1, Topic: "t/1", Payload: []byte("val"), QoS: 1, PacketID: 10, Phase: membership.DeliveryPhaseAwaitPubAck},
					},
					Inbound: []membership.DeliveryInboundDTO{
						{PacketID: 20, Topic: "t/in", Payload: nil, Released: true, ReleasedIndex: 3},
					},
				},
			},
		},
		{
			Index: 4, Outcome: membership.ApplyOutcomeApplied,
			OfflineClaim: &membership.OfflineClaimDTO{
				Found: true,
				Messages: []membership.OfflineMessageDTO{
					{Sequence: 1, Topic: "t/off", Payload: []byte("off"), QoS: 0},
				},
			},
		},
		{
			Index: 5, Outcome: membership.ApplyOutcomeApplied,
			NodeDrain: &membership.NodeDrainStateDTO{
				DrainOperationID:                   "00000000-0000-4000-8000-000000000055",
				MemberID:                           1,
				IdempotencyKey:                     "k-drain-1",
				Phase:                              6,
				ServerReference:                    "127.0.0.1:1883",
				ConnectionRatePerSecond:            10,
				LoadBalancerPropagationDelayMillis: 100,
				StartedAtUnixMillis:                100,
				PhaseStartedAtUnixMillis:           100,
				DeadlineAtUnixMillis:               1100,
				TimeoutMillis:                      1000,
				CommittedIndex:                     5,
			},
		},
		{
			Index: 6, Outcome: membership.ApplyOutcomeApplied,
			TenantQuota: &membership.TenantQuotaRejectionDTO{
				TenantID: "acme", Dimension: membership.TenantQuotaStorageBytes, ProfileVersion: 1,
			},
		},
		{
			Index: 7, Outcome: membership.ApplyOutcomeApplied,
			SessionMetadata: &membership.SessionMetadataResultDTO{
				Found: true,
				Will: &membership.SessionWillDTO{
					Topic: "will/t", Payload: []byte("bye"), QoS: 1, Retain: true, DelayIntervalSeconds: 10,
				},
			},
		},
		{
			Index: 8, Outcome: membership.ApplyOutcomeApplied,
			ConnectionRebalance: &membership.ConnectionRebalanceStateDTO{
				RebalanceOperationID:               "00000000-0000-4000-8000-000000000088",
				IdempotencyKey:                     "k-reb-1",
				Phase:                              2,
				ServerReference:                    "127.0.0.1:1883",
				MaxDeviationConnections:            1,
				ConnectionRatePerSecond:            10,
				LoadBalancerPropagationDelayMillis: 100,
				StartedAtUnixMillis:                100,
				DeadlineAtUnixMillis:               1100,
				TimeoutMillis:                      1000,
				CommittedIndex:                     8,
				Participants: []membership.ConnectionRebalanceReportDTO{
					{NodeID: 1, Connections: 10},
					{NodeID: 2, Connections: 10},
				},
			},
		},
		{
			Index: 9, Outcome: membership.ApplyOutcomeApplied,
			SupersededWill: &membership.SupersededWillDTO{
				Will:            membership.SessionWillDTO{Topic: "old/w", Payload: []byte("old"), QoS: 1},
				ProtocolVersion: 5, Retention: membership.SessionRetentionFinite, PlannedHandoff: true,
			},
		},
		{
			Index: 10, Outcome: membership.ApplyOutcomeConflict,
			Conflict: membership.ConflictProposalCapacity,
		},
	}

	var receipts []membership.ProposalReceiptDTO
	for i, res := range testResults {
		idx := uint64(10 + i + 1)
		res.Index = idx
		if res.Operation != nil {
			res.Operation.CommittedIndex = idx
			res.Operation.Record.CommittedIndex = idx
		}
		if res.Ownership != nil {
			res.Ownership.CommittedIndex = idx
		}
		if res.Delivery != nil && res.Delivery.Session != nil {
			for k := range res.Delivery.Session.Inbound {
				res.Delivery.Session.Inbound[k].ReleasedIndex = idx
			}
		}
		if res.NodeDrain != nil {
			res.NodeDrain.CommittedIndex = idx
		}
		if res.ConnectionRebalance != nil {
			res.ConnectionRebalance.CommittedIndex = idx
		}
		if !membership.ValidProposalResult(res) {
			t.Fatalf("testResults[%d] invalid: %+v", i, res)
		}
		receipts = append(receipts, membership.ProposalReceiptDTO{
			RequestID:     fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1),
			ProofIndex:    uint64(i + 1),
			CommandDigest: make([]byte, 32),
			Result:        res,
		})
	}

	snapshot := membership.SnapshotDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, LastIncludedIndex: 50,
		Members: []membership.MemberIdentityDTO{
			{MemberID: 1, ClusterID: "c", Address: "n:1", DNSSAN: "n", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter},
		},
		ActiveFeatures: []string{membership.ProposalRecoveryFeature},
		ProposalOrigins: []membership.ProposalOriginStateDTO{
			{OriginNodeID: 1, RetiredThrough: 0, Receipts: receipts},
		},
	}
	snapshot.ObjectCount = uint64(len(snapshot.Members) + len(snapshot.ActiveFeatures) + 1 + len(receipts))

	encoded, err := encodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("encode diverse snapshot: %v", err)
	}
	decoded, err := decodeMembershipSnapshot(encoded)
	if err != nil {
		t.Fatalf("decode diverse snapshot: %v", err)
	}
	restored := membership.NewStateMachine()
	if err := restored.Restore(decoded); err != nil {
		t.Fatalf("restore durable results: %v", err)
	}
	for _, expected := range receipts {
		command := membership.CommandDTO{SchemaMajor: membership.SchemaMajor, OperationID: "00000000-0000-4000-8000-000000009001",
			Delivery: &membership.DeliveryChangeDTO{TenantID: "acme", ClientID: "pub", Action: membership.DeliveryActionPublish, Topic: "t/late", PublishQoS: 1, ProposedAtUnixMillis: 1},
			Proposal: &membership.ProposalIdentityDTO{OriginNodeID: 1, ProofIndex: expected.ProofIndex, RequestID: expected.RequestID, CommandDigest: expected.CommandDigest}}
		actual, err := restored.Apply(restored.LastAppliedIndex()+1, command)
		if err != nil || !actual.Replayed {
			t.Fatalf("restored receipt did not prevent mutation: %+v %v", actual, err)
		}
		actual.Replayed = false
		if !reflect.DeepEqual(actual, expected.Result) {
			t.Fatalf("historical result changed: got %+v want %+v", actual, expected.Result)
		}
	}
	state, err := restored.Snapshot(restored.LastAppliedIndex())
	if err != nil {
		t.Fatal(err)
	}
	if len(state.DeliverySessions) != 0 {
		t.Fatal("replaying a restored result created delivery state")
	}
}

// Traceability: CLU-11/CLU-14; docs/specs/cluster-session-and-delivery.md §10, §11.19.
func TestMembershipSnapshotEncodeRejectsInvalidProposalReceipt(t *testing.T) {
	snapshot := bridgeSnapshotFixture()
	snapshot.ProposalOrigins = []membership.ProposalOriginStateDTO{{
		OriginNodeID: 1,
		Receipts: []membership.ProposalReceiptDTO{{
			RequestID: "00000000-0000-4000-8000-000000000001", ProofIndex: 1,
			CommandDigest: make([]byte, 32), Result: membership.ApplyResultDTO{},
		}},
	}}
	snapshot.ObjectCount += 2
	if _, err := encodeMembershipSnapshot(snapshot); err == nil || err.Error() != "invalid proposal receipt result" {
		t.Fatalf("encode snapshot with invalid proposal receipt error = %v, want invalid proposal receipt result", err)
	}
}

func TestProposalResultFromRecordRejectsMalformedRecords(t *testing.T) {
	tests := []struct {
		name string
		wire *records.ProposalResult
		want string
	}{
		{
			name: "missing result",
			want: "missing proposal receipt result",
		},
		{
			name: "unsupported operation action",
			wire: &records.ProposalResult{Operation: &records.MembershipRecord{}},
			want: "unsupported membership action",
		},
		{
			name: "unsupported delivery message phase",
			wire: &records.ProposalResult{Delivery: &records.ProposalDeliveryResult{
				Session: &records.DeliverySessionState{Messages: []*records.DeliveryMessage{{}}},
			}},
			want: "unsupported delivery phase",
		},
		{
			name: "unspecified drain phase",
			wire: &records.ProposalResult{NodeDrain: &records.NodeDrainState{}},
			want: "unsupported node drain phase",
		},
		{
			name: "invalid rebalance phase",
			wire: &records.ProposalResult{ConnectionRebalance: &records.ConnectionRebalanceState{}},
			want: "unsupported connection rebalance phase",
		},
		{
			name: "superseded Will missing",
			wire: &records.ProposalResult{SupersededWill: &records.ProposalSupersededWill{
				Retention: records.SessionRetention_SESSION_RETENTION_FINITE,
			}},
			want: "invalid proposal superseded Will",
		},
		{
			name: "unknown superseded Will retention",
			wire: &records.ProposalResult{SupersededWill: &records.ProposalSupersededWill{
				Will:      &records.SessionWill{Topic: "will/topic"},
				Retention: records.SessionRetention(255),
			}},
			want: "invalid proposal superseded Will",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := proposalResultFromRecord(test.wire)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("proposal result conversion error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestProposalIdentityRecordConversionsPreserveAndCopyDigest(t *testing.T) {
	digest := []byte{1, 2, 3}
	dto := &membership.ProposalIdentityDTO{
		OriginNodeID: 7, ProofIndex: 12, RetiredThrough: 5,
		RequestID: "00000000-0000-4000-8000-000000000012", CommandDigest: digest,
	}
	wire := proposalIdentityToRecord(dto)
	if wire == nil || !reflect.DeepEqual(proposalIdentityFromRecord(wire), dto) {
		t.Fatalf("proposal identity round trip = %#v, want %#v", proposalIdentityFromRecord(wire), dto)
	}
	digest[0] = 9
	if wire.CommandDigest[0] != 1 {
		t.Fatalf("record digest aliased input: %v", wire.CommandDigest)
	}
	if proposalIdentityToRecord(nil) != nil || proposalIdentityFromRecord(nil) != nil {
		t.Fatal("nil proposal identity did not remain nil")
	}
}
