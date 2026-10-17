package membership

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-165
func TestSessionMetadataRequiresActiveFeature(t *testing.T) {
	machine := NewStateMachine()
	acquire := sessionOwnershipCommand(1, "alpha", "client", 1, 1, sessionConnect())
	result := mustApplyResult(t, machine, 1, acquire)
	if result.Conflict != ConflictFeatureInactive {
		t.Fatalf("inactive session acquire conflict = %q, want %q", result.Conflict, ConflictFeatureInactive)
	}
	if _, found := machine.GetOwnership("alpha", "client"); found {
		t.Fatal("inactive session acquire changed ownership")
	}

	change := SessionMetadataChangeDTO{
		TenantID: "alpha", ClientID: "client", OwnerNodeID: 1, Epoch: 1,
		Action: SessionMetadataActionDetach, WillDisposition: SessionWillDispositionDiscard, ProposedAtUnixMillis: 10,
	}
	result = mustApplyResult(t, machine, 2, CommandDTO{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(2), SessionMetadata: &change,
	})
	if result.Conflict != ConflictFeatureInactive {
		t.Fatalf("inactive metadata conflict = %q, want %q", result.Conflict, ConflictFeatureInactive)
	}
	if _, found := machine.GetOwnership("alpha", "client"); found {
		t.Fatal("inactive metadata command changed ownership")
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-165
func TestSessionMetadataConnectReplacesPendingWill(t *testing.T) {
	machine := activeSessionMetadataMachine(t)
	applyMustSucceed(t, machine, 3, sessionOwnershipCommand(3, "alpha", "client", 1, 1, sessionConnect()))
	detach := metadataCommand(4, SessionMetadataChangeDTO{
		TenantID: "alpha", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: SessionMetadataActionDetach,
		WillDisposition: SessionWillDispositionSchedule, ProposedAtUnixMillis: 100,
		SessionExpiresAtUnixMillis: 0, WillDueAtUnixMillis: 200,
	})
	applyMustSucceed(t, machine, 4, detach)
	pending, ok := machine.GetSessionMetadata("alpha", "client")
	if !ok || pending.Connected || pending.Will == nil || pending.WillDueAtUnixMillis != 200 {
		t.Fatalf("detached pending-Will metadata = %+v, found %t", pending, ok)
	}

	newConnect := sessionConnect()
	newConnect.Will.Topic = "events/replacement"
	replacement := sessionOwnershipCommand(5, "alpha", "client", 2, 2, newConnect)
	replacement.Ownership.Action = OwnershipActionTransfer
	replacement.Ownership.ExpectedEpoch = 1
	applyMustSucceed(t, machine, 5, replacement)
	connected, ok := machine.GetSessionMetadata("alpha", "client")
	if !ok || !connected.Connected || connected.Epoch != 2 || connected.Will == nil || connected.Will.Topic != "events/replacement" || connected.WillDueAtUnixMillis != 0 || connected.DetachedAtUnixMillis != 0 {
		t.Fatalf("replacement CONNECT metadata = %+v, found %t", connected, ok)
	}

	// A metadata-free handoff changes ownership only; it cannot rewrite session state.
	before := cloneSessionMetadataStateDTO(connected)
	transfer := sessionOwnershipCommand(6, "alpha", "client", 3, 3, nil)
	transfer.Ownership.Action = OwnershipActionTransfer
	transfer.Ownership.ExpectedEpoch = 2
	applyMustSucceed(t, machine, 6, transfer)
	after, ok := machine.GetSessionMetadata("alpha", "client")
	if !ok || !reflect.DeepEqual(after, before) {
		t.Fatalf("session-less transfer changed metadata: before=%+v after=%+v", before, after)
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-165
func TestSessionMetadataDetachDispositions(t *testing.T) {
	t.Run("claim", func(t *testing.T) {
		machine := seededSessionMetadataMachine(t, "claim")
		result := applyMustSucceed(t, machine, 4, metadataCommand(4, SessionMetadataChangeDTO{
			TenantID: "alpha", ClientID: "claim", OwnerNodeID: 1, Epoch: 1, Action: SessionMetadataActionDetach,
			WillDisposition: SessionWillDispositionClaim, ProposedAtUnixMillis: 100, SessionExpiresAtUnixMillis: 0,
		}))
		if result.SessionMetadata == nil || result.SessionMetadata.Will == nil || result.SessionMetadata.Will.Topic != "events/will" {
			t.Fatalf("claimed Will = %+v", result.SessionMetadata)
		}
		state, ok := machine.GetSessionMetadata("alpha", "claim")
		if !ok || state.Will != nil || state.Principal != nil || state.WillDueAtUnixMillis != 0 || state.Connected {
			t.Fatalf("claimed record retained Will state: %+v", state)
		}
		reclaimed := applyMustSucceed(t, machine, 5, metadataCommand(5, SessionMetadataChangeDTO{
			TenantID: "alpha", ClientID: "claim", OwnerNodeID: 1, Epoch: 1, Action: SessionMetadataActionClaimWill,
			ProposedAtUnixMillis: 500,
		}))
		if reclaimed.SessionMetadata == nil || reclaimed.SessionMetadata.Will != nil {
			t.Fatalf("second claim result = %+v, want no Will", reclaimed.SessionMetadata)
		}
	})

	t.Run("schedule and discard", func(t *testing.T) {
		machine := seededSessionMetadataMachine(t, "schedule")
		applyMustSucceed(t, machine, 4, metadataCommand(4, SessionMetadataChangeDTO{
			TenantID: "alpha", ClientID: "schedule", OwnerNodeID: 1, Epoch: 1, Action: SessionMetadataActionDetach,
			WillDisposition: SessionWillDispositionSchedule, ProposedAtUnixMillis: 100, WillDueAtUnixMillis: 300,
		}))
		state, ok := machine.GetSessionMetadata("alpha", "schedule")
		if !ok || state.Will == nil || state.WillDueAtUnixMillis != 300 || state.Principal == nil {
			t.Fatalf("scheduled metadata = %+v, found %t", state, ok)
		}
		applyMustSucceed(t, machine, 5, metadataCommand(5, SessionMetadataChangeDTO{
			TenantID: "alpha", ClientID: "schedule", OwnerNodeID: 1, Epoch: 1, Action: SessionMetadataActionDetach,
			WillDisposition: SessionWillDispositionDiscard, ProposedAtUnixMillis: 400,
		}))
		state, ok = machine.GetSessionMetadata("alpha", "schedule")
		if !ok || state.Will != nil || state.Principal != nil || state.WillDueAtUnixMillis != 0 {
			t.Fatalf("discarded Will remained: %+v", state)
		}
	})

	t.Run("invalid stale and missing", func(t *testing.T) {
		machine := seededSessionMetadataMachine(t, "checks")
		before, _ := machine.GetSessionMetadata("alpha", "checks")
		invalid := SessionMetadataChangeDTO{
			TenantID: "alpha", ClientID: "checks", OwnerNodeID: 1, Epoch: 1, Action: SessionMetadataActionDetach,
			WillDisposition: SessionWillDispositionSchedule, ProposedAtUnixMillis: 100,
			// Infinite retention cannot have an expiry time.
			SessionExpiresAtUnixMillis: 900, WillDueAtUnixMillis: 200,
		}
		result := mustApplyResult(t, machine, 4, metadataCommand(4, invalid))
		if result.Conflict != ConflictSessionMetadataInvalid {
			t.Fatalf("inconsistent retention conflict = %q", result.Conflict)
		}
		stale := invalid
		stale.SessionExpiresAtUnixMillis = 0
		stale.Epoch = 2
		result = mustApplyResult(t, machine, 5, metadataCommand(5, stale))
		if result.Conflict != ConflictStaleEpoch {
			t.Fatalf("stale metadata conflict = %q", result.Conflict)
		}
		after, _ := machine.GetSessionMetadata("alpha", "checks")
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("rejected changes mutated record: before=%+v after=%+v", before, after)
		}
		applyMustSucceed(t, machine, 6, sessionOwnershipCommand(6, "alpha", "absent", 1, 1, nil))
		missing := metadataCommand(7, SessionMetadataChangeDTO{
			TenantID: "alpha", ClientID: "absent", OwnerNodeID: 1, Epoch: 1,
			Action: SessionMetadataActionClaimWill, ProposedAtUnixMillis: 500,
		})
		result = applyMustSucceed(t, machine, 7, missing)
		if result.SessionMetadata == nil || result.SessionMetadata.Found || result.SessionMetadata.Will != nil {
			t.Fatalf("missing-record claim result = %+v", result.SessionMetadata)
		}
	})
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-165
func TestSessionMetadataDiscardAndReleaseFreeCapacity(t *testing.T) {
	machine := activeSessionMetadataMachine(t)
	large := sessionConnect()
	large.Will.Payload = make([]byte, MaxSessionWillPayloadBytes)
	large.Principal = sessionPrincipal()
	perRecord := sessionWillBytes(sessionMetadataFromConnect(sessionKey{}, 1, *large))
	index := uint64(3)
	fullCount := MaxSessionWillBytes / perRecord
	for record := 0; record < fullCount; record++ {
		applyMustSucceed(t, machine, index, sessionOwnershipCommand(index, "alpha", fmt.Sprintf("full-%03d", record), 1, 1, large))
		index++
	}
	tail := sessionConnect()
	tail.Principal = sessionPrincipal()
	remainingBytes := MaxSessionWillBytes - machine.sessionWillBytes
	tail.Will.Payload = make([]byte, remainingBytes-sessionWillBytes(sessionMetadataFromConnect(sessionKey{}, 1, *tail))+len(tail.Will.Payload))
	applyMustSucceed(t, machine, index, sessionOwnershipCommand(index, "alpha", "tail", 1, 1, tail))
	index++
	before := machine.SessionMetadata()
	failed := mustApplyResult(t, machine, index, sessionOwnershipCommand(index, "alpha", "overflow", 1, 1, large))
	index++
	if failed.Conflict != ConflictSessionMetadataCapacity || machine.sessionWillBytes != MaxSessionWillBytes {
		t.Fatalf("over-capacity conflict=%q bytes=%d", failed.Conflict, machine.sessionWillBytes)
	}
	if _, found := machine.GetOwnership("alpha", "overflow"); found {
		t.Fatal("capacity-rejected acquire changed ownership")
	}
	if !reflect.DeepEqual(before, machine.SessionMetadata()) {
		t.Fatal("capacity rejection changed existing records")
	}
	result := applyMustSucceed(t, machine, index, metadataCommand(index, SessionMetadataChangeDTO{
		TenantID: "alpha", ClientID: "full-000", OwnerNodeID: 1, Epoch: 1,
		Action: SessionMetadataActionDiscard, WillDisposition: SessionWillDispositionClaim, ProposedAtUnixMillis: 100,
	}))
	index++
	if result.SessionMetadata == nil || result.SessionMetadata.Will == nil || machine.sessionWillBytes != MaxSessionWillBytes-perRecord {
		t.Fatalf("discard claim result=%+v bytes=%d", result.SessionMetadata, machine.sessionWillBytes)
	}
	if _, found := machine.GetSessionMetadata("alpha", "full-000"); found {
		t.Fatal("discard retained metadata")
	}
	applyMustSucceed(t, machine, index, sessionOwnershipCommand(index, "alpha", "overflow", 1, 1, large))
	index++
	if machine.sessionWillBytes != MaxSessionWillBytes {
		t.Fatal("discard did not free capacity for replacement")
	}
	applyMustSucceed(t, machine, index, sessionOwnershipReleaseCommand(index, "alpha", "overflow", 1, 1))
	index++
	if _, found := machine.GetSessionMetadata("alpha", "overflow"); found {
		t.Fatal("release retained metadata")
	}
	if _, found := machine.GetOwnership("alpha", "overflow"); found {
		t.Fatal("release retained ownership")
	}
	if machine.sessionWillBytes != MaxSessionWillBytes-perRecord {
		t.Fatal("release byte accounting mismatch")
	}
	applyMustSucceed(t, machine, index, sessionOwnershipCommand(index, "alpha", "after-release", 1, 1, large))
	if machine.sessionWillBytes != MaxSessionWillBytes {
		t.Fatal("release did not free capacity for replacement")
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-166
func TestSessionMetadataSnapshotRoundTripAndValidation(t *testing.T) {
	machine := activeSessionMetadataMachine(t)
	zetaConnect := sessionConnect()
	applyMustSucceed(t, machine, 3, sessionOwnershipCommand(3, "zeta", "client", 1, 1, zetaConnect))
	zetaConnect.Will.Payload[0] = 'x'
	zetaConnect.Principal.Roles[0] = "mutated"
	alphaConnect := sessionConnect()
	applyMustSucceed(t, machine, 4, sessionOwnershipCommand(4, "alpha", "client", 1, 1, alphaConnect))
	alphaConnect.Will.Payload[0] = 'y'
	alphaConnect.Principal.Roles[0] = "mutated"
	for _, state := range machine.SessionMetadata() {
		if string(state.Will.Payload) != "payload" || state.Principal.Roles[0] != "role" {
			t.Fatalf("Apply retained caller-owned session buffers: %+v", state)
		}
	}
	snapshot, err := machine.Snapshot(4)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.ObjectCount != 8 || len(snapshot.SessionMetadata) != 2 || snapshot.SessionMetadata[0].TenantID != "alpha" || snapshot.SessionMetadata[1].TenantID != "zeta" {
		t.Fatalf("metadata snapshot ordering/count = count %d records %+v, want object count 8", snapshot.ObjectCount, snapshot.SessionMetadata)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !reflect.DeepEqual(restored.SessionMetadata(), machine.SessionMetadata()) {
		t.Fatalf("restored metadata differs: %+v != %+v", restored.SessionMetadata(), machine.SessionMetadata())
	}

	// State, getter, and snapshot all own their mutable Will/principal data.
	got := machine.SessionMetadata()
	got[0].Will.Payload[0] = 'x'
	got[0].Principal.Roles[0] = "mutated"
	if machine.SessionMetadata()[0].Will.Payload[0] == 'x' || machine.SessionMetadata()[0].Principal.Roles[0] == "mutated" {
		t.Fatal("SessionMetadata getter exposes mutable state")
	}
	snapshot.SessionMetadata[0].Will.Payload[0] = 'y'
	if machine.SessionMetadata()[0].Will.Payload[0] == 'y' {
		t.Fatal("snapshot shares mutable state")
	}

	base, err := machine.Snapshot(4)
	if err != nil {
		t.Fatal(err)
	}
	invalidSnapshots := map[string]func(*SnapshotDTO){
		"missing ownership": func(s *SnapshotDTO) {
			s.ObjectCount -= uint64(len(s.Ownerships))
			s.Ownerships = nil
		},
		"epoch exceeds ownership": func(s *SnapshotDTO) { s.SessionMetadata[0].Epoch = 2 },
		"inactive feature": func(s *SnapshotDTO) {
			s.ObjectCount -= uint64(len(s.ActiveFeatures))
			s.ActiveFeatures = nil
		},
		"connected timestamps": func(s *SnapshotDTO) { s.SessionMetadata[0].DetachedAtUnixMillis = 5 },
		"duplicate key":        func(s *SnapshotDTO) { s.SessionMetadata[1] = cloneSessionMetadataStateDTO(s.SessionMetadata[0]) },
		"unsorted key": func(s *SnapshotDTO) {
			s.SessionMetadata[0], s.SessionMetadata[1] = s.SessionMetadata[1], s.SessionMetadata[0]
		},
		"will without deadline": func(s *SnapshotDTO) {
			s.SessionMetadata[0].Connected = false
			s.SessionMetadata[0].DetachedAtUnixMillis = 1
			s.SessionMetadata[0].WillDueAtUnixMillis = 0
		},
		"metadata epoch zero": func(s *SnapshotDTO) { s.SessionMetadata[0].Epoch = 0 },
		"disconnected without timestamp": func(s *SnapshotDTO) {
			s.SessionMetadata[0].Connected = false
			s.SessionMetadata[0].Will = nil
			s.SessionMetadata[0].Principal = nil
		},
		"immediate detached session": func(s *SnapshotDTO) {
			s.SessionMetadata[0].Connected = false
			s.SessionMetadata[0].Retention = SessionRetentionImmediate
			s.SessionMetadata[0].ExpiryIntervalSeconds = 0
			s.SessionMetadata[0].DetachedAtUnixMillis = 5
			s.SessionMetadata[0].Will = nil
			s.SessionMetadata[0].Principal = nil
		},
		"finite expiry boundary": func(s *SnapshotDTO) {
			s.SessionMetadata[0].Connected = false
			s.SessionMetadata[0].Retention = SessionRetentionFinite
			s.SessionMetadata[0].ExpiryIntervalSeconds = 1
			s.SessionMetadata[0].DetachedAtUnixMillis = 5
			s.SessionMetadata[0].SessionExpiresAtUnixMillis = 5
			s.SessionMetadata[0].WillDueAtUnixMillis = 5
		},
		"infinite expiry": func(s *SnapshotDTO) {
			s.SessionMetadata[0].Connected = false
			s.SessionMetadata[0].DetachedAtUnixMillis = 5
			s.SessionMetadata[0].SessionExpiresAtUnixMillis = 6
			s.SessionMetadata[0].WillDueAtUnixMillis = 5
		},
		"principal without Will": func(s *SnapshotDTO) { s.SessionMetadata[0].Will = nil },
		"unknown retention":      func(s *SnapshotDTO) { s.SessionMetadata[0].Retention = SessionRetention(99) },
		"excess Will bytes": func(s *SnapshotDTO) {
			s.Ownerships = nil
			s.SessionMetadata = nil
			template := SessionMetadataStateDTO{
				TenantID: "alpha", Epoch: 1, ProtocolVersion: 5,
				Retention: SessionRetentionInfinite, ExpiryIntervalSeconds: ^uint32(0), Connected: true,
				Will: &SessionWillDTO{Topic: "events/will", Payload: make([]byte, MaxSessionWillPayloadBytes)},
				Principal: &SessionPrincipalDTO{
					Subject:      strings.Repeat("s", MaxSessionPrincipalFieldBytes),
					CredentialID: strings.Repeat("c", MaxSessionPrincipalFieldBytes),
					Roles:        make([]string, MaxSessionPrincipalRoles), ExpiresAtUnixMillis: 1,
				},
			}
			for i := range template.Principal.Roles {
				template.Principal.Roles[i] = fmt.Sprintf("%04d%s", i, strings.Repeat("r", MaxSessionPrincipalRoleBytes-4))
			}
			for i := range 101 {
				clientID := fmt.Sprintf("client-%03d", i)
				record := cloneSessionMetadataStateDTO(template)
				record.ClientID = clientID
				s.SessionMetadata = append(s.SessionMetadata, record)
				s.Ownerships = append(s.Ownerships, OwnershipStateDTO{
					TenantID: "alpha", ClientID: clientID, OwnerNodeID: 1, Epoch: 1, CommittedIndex: 1,
				})
			}
			s.ObjectCount = uint64(len(s.Members) + len(s.ActiveFeatures) + len(s.Ownerships) + len(s.SessionMetadata))
		},
	}
	for name, mutate := range invalidSnapshots {
		t.Run(name, func(t *testing.T) {
			candidate := cloneSnapshot(base)
			mutate(&candidate)
			if err := NewStateMachine().Restore(candidate); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("Restore(invalid %s) error = %v, want ErrInvalidSnapshot", name, err)
			}
		})
	}
}

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestSessionMetadataValidationBoundaries(t *testing.T) {
	valid := sessionConnect()
	valid.Principal = sessionPrincipal()
	validCases := []struct {
		name string
		edit func(*SessionConnectDTO)
	}{
		{"version zero", func(c *SessionConnectDTO) { c.ProtocolVersion = 0 }},
		{"unsupported version", func(c *SessionConnectDTO) { c.ProtocolVersion = 6 }},
		{"unknown retention", func(c *SessionConnectDTO) { c.Retention = SessionRetention(99) }},
		{"v5 immediate interval", func(c *SessionConnectDTO) { c.Retention = SessionRetentionImmediate; c.ExpiryIntervalSeconds = 1 }},
		{"v5 finite zero", func(c *SessionConnectDTO) { c.Retention = SessionRetentionFinite; c.ExpiryIntervalSeconds = 0 }},
		{"v5 finite max", func(c *SessionConnectDTO) { c.Retention = SessionRetentionFinite; c.ExpiryIntervalSeconds = ^uint32(0) }},
		{"v5 infinite interval", func(c *SessionConnectDTO) {
			c.Retention = SessionRetentionInfinite
			c.ExpiryIntervalSeconds = ^uint32(0) - 1
		}},
		{"v4 finite", func(c *SessionConnectDTO) {
			c.ProtocolVersion = 4
			c.Retention = SessionRetentionFinite
			c.ExpiryIntervalSeconds = 1
		}},
		{"v4 delayed Will", func(c *SessionConnectDTO) {
			c.ProtocolVersion = 4
			c.Retention = SessionRetentionInfinite
			c.ExpiryIntervalSeconds = 0
			c.Will.DelayIntervalSeconds = 1
		}},
		{"invalid Will topic", func(c *SessionConnectDTO) { c.Will.Topic = "bad/#" }},
		{"Will qos", func(c *SessionConnectDTO) { c.Will.QoS = 3 }},
		{"Will payload limit", func(c *SessionConnectDTO) { c.Will.Payload = make([]byte, MaxSessionWillPayloadBytes+1) }},
		{"principal without Will", func(c *SessionConnectDTO) { c.Will = nil }},
		{"empty subject", func(c *SessionConnectDTO) { c.Principal.Subject = "" }},
		{"invalid utf8 subject", func(c *SessionConnectDTO) { c.Principal.Subject = string([]byte{0xff}) }},
		{"principal field limit", func(c *SessionConnectDTO) {
			c.Principal.CredentialID = string(make([]byte, MaxSessionPrincipalFieldBytes+1))
		}},
		{"principal expiry zero", func(c *SessionConnectDTO) { c.Principal.ExpiresAtUnixMillis = 0 }},
		{"principal roles limit", func(c *SessionConnectDTO) { c.Principal.Roles = make([]string, MaxSessionPrincipalRoles+1) }},
		{"principal role length", func(c *SessionConnectDTO) {
			c.Principal.Roles = []string{string(make([]byte, MaxSessionPrincipalRoleBytes+1))}
		}},
		{"duplicate principal role", func(c *SessionConnectDTO) { c.Principal.Roles = []string{"reader", "reader"} }},
	}
	for _, tc := range validCases {
		t.Run(tc.name, func(t *testing.T) {
			connect := cloneSessionConnectForTest(*valid)
			tc.edit(&connect)
			command := sessionOwnershipCommand(1, "alpha", "client", 1, 1, &connect)
			if err := validateCommand(command); !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("validateCommand error = %v, want ErrInvalidCommand", err)
			}
		})
	}

	// Valid protocol-specific retention forms are accepted at the edge values.
	validConnections := []SessionConnectDTO{
		{ProtocolVersion: 4, Retention: SessionRetentionImmediate},
		{ProtocolVersion: 4, Retention: SessionRetentionInfinite},
		{ProtocolVersion: 5, Retention: SessionRetentionImmediate},
		{ProtocolVersion: 5, Retention: SessionRetentionFinite, ExpiryIntervalSeconds: 1},
		{ProtocolVersion: 5, Retention: SessionRetentionFinite, ExpiryIntervalSeconds: ^uint32(0) - 1},
		{ProtocolVersion: 5, Retention: SessionRetentionInfinite, ExpiryIntervalSeconds: ^uint32(0)},
	}
	for i := range validConnections {
		if err := validateCommand(sessionOwnershipCommand(uint64(i+1), "alpha", "valid", 1, 1, &validConnections[i])); err != nil {
			t.Fatalf("valid connection %d rejected: %v", i, err)
		}
	}

	maxPrincipal := sessionPrincipal()
	maxPrincipal.Subject = strings.Repeat("s", MaxSessionPrincipalFieldBytes)
	maxPrincipal.CredentialID = strings.Repeat("c", MaxSessionPrincipalFieldBytes)
	maxPrincipal.Roles = make([]string, MaxSessionPrincipalRoles)
	for i := range maxPrincipal.Roles {
		maxPrincipal.Roles[i] = fmt.Sprintf("%04d%s", i, strings.Repeat("r", MaxSessionPrincipalRoleBytes-4))
	}
	maxWillConnect := sessionConnect()
	maxWillConnect.Will.Payload = make([]byte, MaxSessionWillPayloadBytes)
	maxWillConnect.Will.QoS = 2
	maxWillConnect.Principal = maxPrincipal
	if err := validateCommand(sessionOwnershipCommand(20, "alpha", "limits", 1, 1, maxWillConnect)); err != nil {
		t.Fatalf("inclusive Will/principal limits rejected: %v", err)
	}

	principalCases := []struct {
		name string
		edit func(*SessionPrincipalDTO)
	}{
		{"subject empty", func(p *SessionPrincipalDTO) { p.Subject = "" }},
		{"credential empty", func(p *SessionPrincipalDTO) { p.CredentialID = "" }},
		{"subject too long", func(p *SessionPrincipalDTO) { p.Subject = string(make([]byte, MaxSessionPrincipalFieldBytes+1)) }},
		{"role empty", func(p *SessionPrincipalDTO) { p.Roles = []string{""} }},
		{"role invalid utf8", func(p *SessionPrincipalDTO) { p.Roles = []string{string([]byte{0xff})} }},
	}
	for _, tc := range principalCases {
		t.Run(tc.name, func(t *testing.T) {
			connect := sessionConnect()
			principal := *sessionPrincipal()
			principal.Roles = append([]string(nil), principal.Roles...)
			tc.edit(&principal)
			connect.Principal = &principal
			if err := validateCommand(sessionOwnershipCommand(1, "alpha", "client", 1, 1, connect)); !errors.Is(err, ErrInvalidCommand) {
				t.Fatalf("principal validation error = %v, want ErrInvalidCommand", err)
			}
		})
	}

	// Metadata change shape validation covers action-specific timestamps and dispositions.
	baseChange := SessionMetadataChangeDTO{TenantID: "alpha", ClientID: "client", OwnerNodeID: 1, Epoch: 1, ProposedAtUnixMillis: 10, Action: SessionMetadataActionDetach, WillDisposition: SessionWillDispositionDiscard}
	badChanges := []SessionMetadataChangeDTO{
		{ClientID: "client", OwnerNodeID: 1, Epoch: 1, ProposedAtUnixMillis: 1, Action: SessionMetadataActionDetach, WillDisposition: SessionWillDispositionDiscard},
		{TenantID: "alpha", ClientID: "client", Epoch: 1, ProposedAtUnixMillis: 1, Action: SessionMetadataActionDetach, WillDisposition: SessionWillDispositionUnspecified},
		{TenantID: "alpha", ClientID: "client", OwnerNodeID: 1, Epoch: 0, ProposedAtUnixMillis: 1, Action: SessionMetadataActionDetach, WillDisposition: SessionWillDispositionDiscard},
		func() SessionMetadataChangeDTO { c := baseChange; c.ProposedAtUnixMillis = 0; return c }(),
		func() SessionMetadataChangeDTO {
			c := baseChange
			c.WillDisposition = SessionWillDispositionSchedule
			c.WillDueAtUnixMillis = 9
			return c
		}(),
		func() SessionMetadataChangeDTO { c := baseChange; c.SessionExpiresAtUnixMillis = 10; return c }(),
		func() SessionMetadataChangeDTO {
			c := baseChange
			c.WillDisposition = SessionWillDispositionClaim
			c.WillDueAtUnixMillis = 11
			return c
		}(),
		func() SessionMetadataChangeDTO { c := baseChange; c.Action = SessionMetadataActionClaimWill; return c }(),
		func() SessionMetadataChangeDTO {
			c := baseChange
			c.Action = SessionMetadataActionDiscard
			c.WillDisposition = SessionWillDispositionSchedule
			return c
		}(),
		func() SessionMetadataChangeDTO {
			c := baseChange
			c.Action = SessionMetadataActionUnspecified
			return c
		}(),
	}
	for i, change := range badChanges {
		if err := validateCommand(metadataCommand(uint64(i+1), change)); !errors.Is(err, ErrInvalidCommand) {
			t.Errorf("invalid metadata change %d error = %v, want ErrInvalidCommand", i, err)
		}
	}
	validChanges := []SessionMetadataChangeDTO{
		{TenantID: "alpha", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: SessionMetadataActionDetach, WillDisposition: SessionWillDispositionSchedule, ProposedAtUnixMillis: 10, WillDueAtUnixMillis: 10},
		{TenantID: "alpha", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: SessionMetadataActionClaimWill, ProposedAtUnixMillis: 10},
		{TenantID: "alpha", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: SessionMetadataActionDiscard, WillDisposition: SessionWillDispositionClaim, ProposedAtUnixMillis: 10},
	}
	for i, change := range validChanges {
		if err := validateCommand(metadataCommand(uint64(i+20), change)); err != nil {
			t.Errorf("valid metadata change %d rejected: %v", i, err)
		}
	}
}

func activeSessionMetadataMachine(t *testing.T) *StateMachine {
	t.Helper()
	members := testVoters()
	for i := range members {
		members[i].Features = []string{SessionMetadataFeature}
	}
	machine := NewStateMachine()
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatalf("initialize bootstrap: %v", err)
	}
	result := activateFeatureForTest(t, machine, 2, SessionMetadataFeature)
	if result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("activate session metadata outcome=%+v", result)
	}
	return machine
}

func seededSessionMetadataMachine(t *testing.T, clientID string) *StateMachine {
	t.Helper()
	machine := activeSessionMetadataMachine(t)
	applyMustSucceed(t, machine, 3, sessionOwnershipCommand(3, "alpha", clientID, 1, 1, sessionConnect()))
	return machine
}

func sessionConnect() *SessionConnectDTO {
	return &SessionConnectDTO{
		ProtocolVersion: 5, Retention: SessionRetentionInfinite, ExpiryIntervalSeconds: ^uint32(0),
		Will:      &SessionWillDTO{Topic: "events/will", Payload: []byte("payload"), QoS: 1, DelayIntervalSeconds: 2},
		Principal: sessionPrincipal(),
	}
}

func sessionPrincipal() *SessionPrincipalDTO {
	return &SessionPrincipalDTO{Subject: "subject", CredentialID: "credential", Roles: []string{"role"}, ExpiresAtUnixMillis: 1000}
}

func sessionOwnershipCommand(index uint64, tenantID, clientID string, owner, epoch uint64, connect *SessionConnectDTO) CommandDTO {
	ownership := &OwnershipChangeDTO{TenantID: tenantID, ClientID: clientID, OwnerNodeID: owner, Epoch: epoch, Action: OwnershipActionAcquire, Session: connect}
	return CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(index), Ownership: ownership}
}

func sessionOwnershipReleaseCommand(index uint64, tenantID, clientID string, owner, epoch uint64) CommandDTO {
	change := &OwnershipChangeDTO{
		TenantID: tenantID, ClientID: clientID, OwnerNodeID: owner, Epoch: epoch, ExpectedEpoch: epoch,
		Action: OwnershipActionRelease,
	}
	return CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(index), Ownership: change}
}

func metadataCommand(index uint64, change SessionMetadataChangeDTO) CommandDTO {
	return CommandDTO{SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, OperationID: releaseProfileOperation(index), SessionMetadata: &change}
}

func cloneSessionConnectForTest(input SessionConnectDTO) SessionConnectDTO {
	copy := input
	if input.Will != nil {
		will := *input.Will
		will.Payload = append([]byte(nil), input.Will.Payload...)
		copy.Will = &will
	}
	if input.Principal != nil {
		principal := *input.Principal
		principal.Roles = append([]string(nil), input.Principal.Roles...)
		copy.Principal = &principal
	}
	return copy
}

// Traceability: DEP-07/DEP-12; docs/specs/cluster-session-and-delivery.md §4.3.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-011
func TestApplyOwnershipReportsSupersededWill(t *testing.T) {
	for _, kind := range []string{"planned", "scheduled", "live", "no will", "claimed"} {
		t.Run(kind, func(t *testing.T) {
			machine := activeSessionMetadataMachine(t)
			initial := sessionConnect()
			if kind == "no will" {
				initial.Will, initial.Principal = nil, nil
			}
			applyMustSucceed(t, machine, 3, sessionOwnershipCommand(3, "alpha", "client", 1, 1, initial))
			epoch := uint64(1)
			if kind == "planned" {
				change := sessionOwnershipCommand(4, "alpha", "client", 2, 2, nil)
				change.Ownership.Action, change.Ownership.ExpectedEpoch = OwnershipActionTransfer, 1
				applyMustSucceed(t, machine, 4, change)
				epoch = 2
			} else if kind == "scheduled" || kind == "claimed" {
				disposition := SessionWillDispositionClaim
				due := uint64(0)
				if kind == "scheduled" {
					disposition, due = SessionWillDispositionSchedule, 200
				}
				applyMustSucceed(t, machine, 4, metadataCommand(4, SessionMetadataChangeDTO{
					TenantID: "alpha", ClientID: "client", OwnerNodeID: 1, Epoch: 1, Action: SessionMetadataActionDetach,
					WillDisposition: disposition, ProposedAtUnixMillis: 100, WillDueAtUnixMillis: due,
				}))
			}
			old, _ := machine.GetSessionMetadata("alpha", "client")
			change := sessionOwnershipCommand(5, "alpha", "client", epoch+1, epoch, sessionConnect())
			change.Ownership.OwnerNodeID = oldOwnerForSupersededTest(kind)
			change.Ownership.ExpectedEpoch = epoch
			change.Ownership.Epoch = epoch + 1
			result := applyMustSucceed(t, machine, 5, change)
			want := kind == "planned" || kind == "scheduled"
			if (result.SupersededWill != nil) != want {
				t.Fatalf("superseded=%+v", result.SupersededWill)
			}
			if !want {
				return
			}
			got := result.SupersededWill
			if got.PlannedHandoff != (kind == "planned") || !reflect.DeepEqual(&got.Will, old.Will) || !reflect.DeepEqual(got.Principal, old.Principal) || got.ProtocolVersion != old.ProtocolVersion || got.Retention != old.Retention {
				t.Fatalf("superseded=%+v old=%+v", got, old)
			}
			got.Will.Payload[0] ^= 1
			got.Principal.Roles[0] = "changed"
			if reflect.DeepEqual(got.Will.Payload, old.Will.Payload) || reflect.DeepEqual(got.Principal.Roles, old.Principal.Roles) {
				t.Fatal("result shares replaced record slices")
			}
			current, _ := machine.GetSessionMetadata("alpha", "client")
			if !reflect.DeepEqual(current.Will, initial.Will) || !reflect.DeepEqual(current.Principal, initial.Principal) {
				t.Fatal("result mutation changed committed replacement")
			}
		})
	}
}

func oldOwnerForSupersededTest(kind string) uint64 {
	if kind == "planned" {
		return 2
	}
	return 1
}
