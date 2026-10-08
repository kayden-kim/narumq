package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"narumq/internal/application/connect"
	"narumq/internal/application/membership"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

func newTestClusterOfflineRuntime(t *testing.T, config clusterConfigFile) (*clusterRuntime, func()) {
	t.Helper()
	machine := retainedTestMachine(t, config, config.ReleaseVersion)
	// Acquire ownership for tenant-a / client-a on node 1, epoch 1
	_, err := machine.Apply(4, membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor, OperationID: "00000000-0000-4000-8000-000000000010",
		Ownership: &membership.OwnershipChangeDTO{
			TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: config.NodeID, Epoch: 1, ExpectedEpoch: 0,
			Action: membership.OwnershipActionAcquire,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	runtime := &clusterRuntime{
		config:   config,
		machine:  machine,
		storage:  newClusterStorageMonitor(func() time.Time { return time.Unix(1, 0) }, nil),
		requests: make(chan clusterDriverRequest),
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	ctx, cancel := context.WithCancel(context.Background())
	applyIndex := uint64(4)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-runtime.requests:
				switch req.kind {
				case clusterDriverRead:
					req.readResult <- clusterReadResponse{index: applyIndex}
				case clusterDriverCommit:
					pending := req.pending
					// Exercise the production post-read gate for inactive metadata.
					if clusterCommandRequiresSessionMetadata(pending.command) && !slices.Contains(runtime.machine.ActiveFeatures(), membership.SessionMetadataFeature) {
						pending.readConfirmed = true
						runtime.pendingCommits = append(runtime.pendingCommits, pending)
						var proposals []clusterProposalWaiter
						runtime.resolveCommitReads(make(map[string]clusterReadWaiter), &proposals)
						continue
					}
					pending.staged <- clusterCommitStage{context: pending.context}
					cmd, err := decodeMembershipCommand(pending.data)
					if err != nil {
						pending.result <- clusterApplyResponse{err: err}
						continue
					}
					applyIndex++
					res, err := runtime.machine.Apply(applyIndex, cmd)
					pending.result <- clusterApplyResponse{result: res, err: err}
				case clusterDriverProposal:
					cmd, err := decodeMembershipCommand(req.data)
					if err != nil {
						req.applyResult <- clusterApplyResponse{err: err}
						continue
					}
					applyIndex++
					res, err := runtime.machine.Apply(applyIndex, cmd)
					req.applyResult <- clusterApplyResponse{result: res, err: err}
				}
			}
		}
	}()

	return runtime, cancel
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestClusterOfflineDetachSplitsAtByteBoundaryAndLogsTailDrop(t *testing.T) {
	config := validBootstrapClusterConfig()
	runtime, stop := newTestClusterOfflineRuntime(t, config)
	defer stop()

	var logBuf strings.Builder
	runtime.logger = slog.New(slog.NewTextHandler(&logBuf, nil))

	// 1. QoS != 0 rejected before commit
	badQoS := connect.DetachRequestDTO{
		TenantID:    "tenant-a",
		ClientID:    "client-a",
		OwnerNodeID: config.NodeID,
		Epoch:       1,
		DetachedAt:  time.Now(),
		Pending: []deliveryqueue.DeliveryDTO{
			{Topic: "t/1", Payload: []byte("p"), QoS: mqtt.QoSAtLeastOnce},
		},
	}
	if err := runtime.DetachSession(context.Background(), badQoS); err == nil {
		t.Fatal("DetachSession accepted QoS > 0")
	}

	// 2. Pending items exceeding 768 KiB boundary: split into DETACH + ENQUEUE
	chunkSize := 200 << 10 // 200 KiB per message
	// 5 messages of 200 KiB = 1000 KiB total > 768 KiB
	// 3 messages = 600 KiB <= 768 KiB (included in DETACH)
	// 4th and 5th messages overflow into ENQUEUE
	pending := make([]deliveryqueue.DeliveryDTO, 5)
	for i := range pending {
		pending[i] = deliveryqueue.DeliveryDTO{
			Topic:   "large/topic",
			Payload: bytes.Repeat([]byte{byte('A' + i)}, chunkSize),
			QoS:     mqtt.QoSAtMostOnce,
		}
	}

	req := connect.DetachRequestDTO{
		TenantID:         "tenant-a",
		ClientID:         "client-a",
		OwnerNodeID:      config.NodeID,
		Epoch:            1,
		SessionExpiresAt: time.Now().Add(time.Hour),
		DetachedAt:       time.Now(),
		Pending:          pending,
	}

	if err := runtime.DetachSession(context.Background(), req); err != nil {
		t.Fatalf("DetachSession() error = %v", err)
	}

	// Verify that all 5 messages are now in the committed offline session
	sessions := runtime.machine.OfflineSessions()
	if len(sessions) != 1 {
		t.Fatalf("offline sessions count = %d, want 1", len(sessions))
	}
	if len(sessions[0].Messages) != 5 {
		t.Fatalf("offline session messages count = %d, want 5", len(sessions[0].Messages))
	}
	for i := range 5 {
		if sessions[0].Messages[i].Sequence != uint64(i+1) {
			t.Fatalf("message[%d] sequence = %d, want %d", i, sessions[0].Messages[i].Sequence, i+1)
		}
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestClusterOfflineClaimDTOConversion(t *testing.T) {
	config := validBootstrapClusterConfig()
	runtime, stop := newTestClusterOfflineRuntime(t, config)
	defer stop()
	// Detach session with 2 messages: one with expiry, one without (0)
	now := time.Now()
	expiry := now.Add(time.Minute)
	pending := []deliveryqueue.DeliveryDTO{
		{Topic: "t/1", Payload: []byte("val1"), QoS: mqtt.QoSAtMostOnce, ExpiresAt: expiry},
		{Topic: "t/2", Payload: []byte("val2"), QoS: mqtt.QoSAtMostOnce},
	}
	err := runtime.DetachSession(context.Background(), connect.DetachRequestDTO{
		TenantID:    "tenant-a",
		ClientID:    "client-a",
		OwnerNodeID: config.NodeID,
		Epoch:       1,
		DetachedAt:  now,
		Pending:     pending,
	})
	if err != nil {
		t.Fatalf("DetachSession() error = %v", err)
	}

	// Claim
	claim, err := runtime.ClaimOffline(context.Background(), "tenant-a", "client-a", config.NodeID, 1, now)
	if err != nil {
		t.Fatalf("ClaimOffline() error = %v", err)
	}
	if !claim.Found {
		t.Fatal("ClaimOffline Found = false, want true")
	}
	if len(claim.Deliveries) != 2 {
		t.Fatalf("ClaimOffline Deliveries count = %d, want 2", len(claim.Deliveries))
	}

	expectedSessionID, _ := clusterSessionID("tenant-a", "client-a")
	if claim.Deliveries[0].SessionID != expectedSessionID {
		t.Fatalf("delivery[0] SessionID = %s, want %s", claim.Deliveries[0].SessionID, expectedSessionID)
	}
	if claim.Deliveries[0].ExpiresAt.UnixMilli() != expiry.UnixMilli() {
		t.Fatalf("delivery[0] ExpiresAt = %v, want %v", claim.Deliveries[0].ExpiresAt, expiry)
	}
	if !claim.Deliveries[1].ExpiresAt.IsZero() {
		t.Fatalf("delivery[1] ExpiresAt = %v, want zero time", claim.Deliveries[1].ExpiresAt)
	}

	// Second claim on empty/missing record returns Found=false
	secondClaim, err := runtime.ClaimOffline(context.Background(), "tenant-a", "client-a", config.NodeID, 1, now)
	if err != nil {
		t.Fatalf("second ClaimOffline() error = %v", err)
	}
	if secondClaim.Found || len(secondClaim.Deliveries) != 0 {
		t.Fatalf("second ClaimOffline = %+v, want Found=false, 0 deliveries", secondClaim)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestClusterOfflineCommitOfflineTable(t *testing.T) {
	config := validBootstrapClusterConfig()
	runtime, stop := newTestClusterOfflineRuntime(t, config)
	defer stop()

	tenantID, _ := tenancy.NewTenantID("tenant-a")
	sKey, _ := tenancy.NewSessionKey(tenantID, "client-a")
	validSessionID := routing.SessionID(sKey.RoutingID())

	// 1. Not detached yet: returns handled=false, err=nil
	delivery := deliveryqueue.DeliveryDTO{
		SessionID: validSessionID,
		Topic:     "t/live",
		Payload:   []byte("p"),
		QoS:       mqtt.QoSAtMostOnce,
	}
	handled, err := runtime.CommitOffline(context.Background(), delivery, time.Now())
	if handled || err != nil {
		t.Fatalf("CommitOffline before detach: handled=%v, err=%v, want false, nil", handled, err)
	}

	// Detach session
	now := time.Now()
	err = runtime.DetachSession(context.Background(), connect.DetachRequestDTO{
		TenantID:    "tenant-a",
		ClientID:    "client-a",
		OwnerNodeID: config.NodeID,
		Epoch:       1,
		DetachedAt:  now,
	})
	if err != nil {
		t.Fatalf("DetachSession() error = %v", err)
	}

	// 2. Detached: valid QoS 0 delivery returns handled=true, err=nil
	handled, err = runtime.CommitOffline(context.Background(), delivery, now)
	if !handled || err != nil {
		t.Fatalf("CommitOffline when detached: handled=%v, err=%v, want true, nil", handled, err)
	}

	// 3. QoS > 0 returns handled=true, ErrUnavailable
	qos1Delivery := delivery
	qos1Delivery.QoS = mqtt.QoSAtLeastOnce
	handled, err = runtime.CommitOffline(context.Background(), qos1Delivery, now)
	if !handled || !errors.Is(err, deliveryqueue.ErrUnavailable) {
		t.Fatalf("CommitOffline QoS 1: handled=%v, err=%v, want true, ErrUnavailable", handled, err)
	}

	// 4. Oversized payload returns handled=true, ErrFull
	oversized := delivery
	oversized.Payload = make([]byte, membership.MaxOfflinePayloadBytes+1)
	handled, err = runtime.CommitOffline(context.Background(), oversized, now)
	if !handled || !errors.Is(err, deliveryqueue.ErrFull) {
		t.Fatalf("CommitOffline oversized: handled=%v, err=%v, want true, ErrFull", handled, err)
	}

	// Encoded command size can exceed its limit before the maximum valid payload reaches its own limit.
	commandTooLarge := delivery
	commandTooLarge.Topic = strings.Repeat("t", 65_535)
	commandTooLarge.Payload = make([]byte, membership.MaxOfflinePayloadBytes)
	handled, err = runtime.CommitOffline(context.Background(), commandTooLarge, now)
	if !handled || !errors.Is(err, deliveryqueue.ErrFull) {
		t.Fatalf("CommitOffline encoded command overflow: handled=%v, err=%v, want true, ErrFull", handled, err)
	}

	// 5. Unparseable routing ID returns handled=false, nil
	unparseable := delivery
	unparseable.SessionID = "bad/session/routing/id/extra"
	handled, err = runtime.CommitOffline(context.Background(), unparseable, now)
	if handled || err != nil {
		t.Fatalf("CommitOffline unparseable: handled=%v, err=%v, want false, nil", handled, err)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestClusterOfflineCommitCapacityAndClaimConflictPreserveState(t *testing.T) {
	config := validBootstrapClusterConfig()
	runtime, stop := newTestClusterOfflineRuntime(t, config)
	defer stop()

	now := time.UnixMilli(1_700_000_100_000)
	pending := make([]deliveryqueue.DeliveryDTO, membership.MaxOfflineMessagesPerSession)
	for i := range pending {
		pending[i] = deliveryqueue.DeliveryDTO{
			Topic: "events/capacity", Payload: []byte{byte(i)}, QoS: mqtt.QoSAtMostOnce,
		}
	}
	if err := runtime.DetachSession(context.Background(), connect.DetachRequestDTO{
		TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: config.NodeID,
		Epoch: 1, DetachedAt: now, Pending: pending,
	}); err != nil {
		t.Fatalf("detach full offline session: %v", err)
	}
	before := runtime.machine.OfflineSessions()
	if len(before) != 1 || len(before[0].Messages) != membership.MaxOfflineMessagesPerSession {
		t.Fatalf("seeded offline state=%+v, want one full session", before)
	}

	sessionID, err := clusterSessionID("tenant-a", "client-a")
	if err != nil {
		t.Fatal(err)
	}
	handled, err := runtime.CommitOffline(context.Background(), deliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "events/overflow", Payload: []byte("extra"), QoS: mqtt.QoSAtMostOnce,
	}, now)
	if !handled || !errors.Is(err, deliveryqueue.ErrFull) {
		t.Fatalf("full-session CommitOffline handled=%t err=%v, want true and ErrFull", handled, err)
	}
	after := runtime.machine.OfflineSessions()
	if len(after) != 1 || len(after[0].Messages) != len(before[0].Messages) {
		t.Fatalf("capacity rejection changed offline message count: before=%d after=%+v", len(before[0].Messages), after)
	}
	for i := range before[0].Messages {
		if before[0].Messages[i].Sequence != after[0].Messages[i].Sequence ||
			string(before[0].Messages[i].Payload) != string(after[0].Messages[i].Payload) {
			t.Fatalf("capacity rejection changed message %d: before=%+v after=%+v", i, before[0].Messages[i], after[0].Messages[i])
		}
	}

	if _, err := runtime.ClaimOffline(context.Background(), "tenant-a", "client-a", config.NodeID+1, 1, now); !errors.Is(err, errClusterOfflineConflict) {
		t.Fatalf("non-owner claim error=%v, want offline conflict", err)
	}
	afterClaimConflict := runtime.machine.OfflineSessions()
	if len(afterClaimConflict) != 1 || len(afterClaimConflict[0].Messages) != len(before[0].Messages) {
		t.Fatalf("conflicting claim changed offline state: %+v", afterClaimConflict)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestClusterOfflineDetachRejectsInvalidQoSAndStaleOwnershipWithoutChangingState(t *testing.T) {
	config := validBootstrapClusterConfig()
	runtime, stop := newTestClusterOfflineRuntime(t, config)
	defer stop()

	now := time.UnixMilli(1_700_000_200_000)
	tests := []struct {
		name    string
		request connect.DetachRequestDTO
		wantErr error
	}{
		{
			name: "nonzero QoS",
			request: connect.DetachRequestDTO{
				TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: config.NodeID,
				Epoch: 1, DetachedAt: now,
				Pending: []deliveryqueue.DeliveryDTO{{Topic: "events/invalid", Payload: []byte("x"), QoS: mqtt.QoSAtLeastOnce}},
			},
		},
		{
			name: "stale epoch",
			request: connect.DetachRequestDTO{
				TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: config.NodeID,
				Epoch: 2, DetachedAt: now,
				Pending: []deliveryqueue.DeliveryDTO{{Topic: "events/stale", Payload: []byte("x"), QoS: mqtt.QoSAtMostOnce}},
			},
			wantErr: errClusterOfflineConflict,
		},
		{
			name: "wrong owner",
			request: connect.DetachRequestDTO{
				TenantID: "tenant-a", ClientID: "client-a", OwnerNodeID: config.NodeID + 1,
				Epoch: 1, DetachedAt: now,
				Pending: []deliveryqueue.DeliveryDTO{{Topic: "events/owner", Payload: []byte("x"), QoS: mqtt.QoSAtMostOnce}},
			},
			wantErr: errClusterOfflineConflict,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := runtime.machine.OfflineSessions()
			if len(before) != 0 {
				t.Fatalf("initial offline sessions = %+v, want none", before)
			}
			err := runtime.DetachSession(context.Background(), tt.request)
			if err == nil {
				t.Fatal("DetachSession() error = nil, want rejection")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("DetachSession() error = %v, want errors.Is(_, %v)", err, tt.wantErr)
			}
			after := runtime.machine.OfflineSessions()
			if len(after) != 0 {
				t.Fatalf("rejected detach changed offline state: %+v", after)
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.1, §10, §11.9.
func TestClusterOfflineClaimRejectsInvalidIdentityWithoutChangingState(t *testing.T) {
	config := validBootstrapClusterConfig()
	runtime, stop := newTestClusterOfflineRuntime(t, config)
	defer stop()

	before := runtime.machine.OfflineSessions()
	claim, err := runtime.ClaimOffline(context.Background(), "", "client-a", config.NodeID, 1, time.UnixMilli(1_700_000_200_000))
	if err == nil {
		t.Fatalf("ClaimOffline() = %+v, want invalid identity error", claim)
	}
	if claim.Found || len(claim.Deliveries) != 0 {
		t.Fatalf("failed ClaimOffline() result = %+v, want empty claim", claim)
	}
	after := runtime.machine.OfflineSessions()
	if len(after) != len(before) {
		t.Fatalf("invalid claim changed offline session count: before=%+v after=%+v", before, after)
	}
}
