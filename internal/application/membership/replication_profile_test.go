package membership

import (
	"errors"
	"testing"
)

// Traceability: CLU-09; docs/specs/cluster-session-and-delivery.md §4.4, §11.27.
func TestRemovalKeepsReplicationFactorBoundaries(t *testing.T) {
	for _, test := range []struct {
		name   string
		voters []uint64
		want   bool
	}{
		{name: "four to three", voters: []uint64{1, 2, 3, 4}, want: true},
		{name: "three to two", voters: []uint64{1, 2, 3}, want: false},
		{name: "already removed", voters: []uint64{1, 2, 4}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := RemovalKeepsReplicationFactor(3, test.voters, StandardReplicationProfile); got != test.want {
				t.Fatalf("removal keeps factor=%t, want %t", got, test.want)
			}
		})
	}
	for _, profile := range []ReplicationProfileDTO{
		StandardReplicationProfile, {Factor: 5, WriteQuorum: 3},
		{Factor: 3, WriteQuorum: 3}, {Factor: 3, WriteQuorum: 1}, {},
	} {
		err := ValidateReplicationProfile(profile)
		if profile == StandardReplicationProfile {
			if err != nil {
				t.Fatalf("standard profile: %v", err)
			}
		} else if !errors.Is(err, ErrUnsupportedReplicationProfile) {
			t.Fatalf("profile=%+v error=%v, want unsupported profile", profile, err)
		}
	}
}
