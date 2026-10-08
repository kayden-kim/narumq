package records

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestSessionMetadataSnapshotAcceptsConnectedSessionWithoutDetachTimes(t *testing.T) {
	state := sessionMetadataSnapshotForTest()
	connected := state.SessionMetadata[0]
	connected.Connected = true
	connected.DetachedAtUnixMillis = 0
	connected.SessionExpiresAtUnixMillis = 0
	connected.WillDueAtUnixMillis = 0

	wire, err := MarshalSnapshot(state, 10)
	if err != nil {
		t.Fatalf("MarshalSnapshot(connected session): %v", err)
	}
	envelope, err := UnmarshalSnapshot(wire)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot(connected session): %v", err)
	}
	var restored *SessionMetadataState
	for _, candidate := range envelope.State.SessionMetadata {
		if candidate.ClientId == connected.ClientId {
			restored = candidate
			break
		}
	}
	if !proto.Equal(restored, connected) {
		t.Fatalf("restored connected session = %v, want %v", restored, connected)
	}
}

func TestSessionMetadataSnapshotRejectsInvalidDisconnectedExpiry(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*SessionMetadataState)
		message string
	}{
		{
			name: "finite expiry must follow detach",
			mutate: func(session *SessionMetadataState) {
				session.SessionExpiresAtUnixMillis = session.DetachedAtUnixMillis
			},
			message: "finite disconnected session expiry must follow detach",
		},
		{
			name: "infinite retention has no expiry",
			mutate: func(session *SessionMetadataState) {
				session.Retention = SessionRetention_SESSION_RETENTION_INFINITE
				session.ExpiryIntervalSeconds = ^uint32(0)
				session.SessionExpiresAtUnixMillis = session.DetachedAtUnixMillis + 1
			},
			message: "infinite disconnected session must not expire",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := sessionMetadataSnapshotForTest()
			tc.mutate(state.SessionMetadata[0])
			before := proto.Clone(state)
			if _, err := MarshalSnapshot(state, 10); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("MarshalSnapshot() error = %v, want message %q", err, tc.message)
			}
			if !proto.Equal(state, before) {
				t.Fatal("MarshalSnapshot mutated caller-owned state after rejection")
			}
		})
	}
}
