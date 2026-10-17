package nodedrain

import (
	"strings"
	"testing"
)

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §1, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-005
func TestPhaseTransitions(t *testing.T) {
	if got := PhaseUnspecified.String(); got != "UNSPECIFIED" {
		t.Fatalf("unspecified phase = %q", got)
	}
	for phase, want := range map[Phase]string{
		PhaseExcludingTraffic:      "EXCLUDING_TRAFFIC",
		PhaseWaitingForLB:          "WAITING_FOR_LB",
		PhaseVerifyingSessionState: "VERIFYING_SESSION_STATE",
		PhaseTransferringSessions:  "TRANSFERRING_SESSIONS",
		PhaseVerifyingBarrier:      "VERIFYING_BARRIER",
		PhaseCompleted:             "COMPLETED",
		PhaseFailed:                "FAILED",
		PhaseCancelled:             "CANCELLED",
	} {
		if got := phase.String(); got != want {
			t.Errorf("Phase(%d).String() = %q, want %q", phase, got, want)
		}
		if !phase.Valid() {
			t.Errorf("Phase(%d) is not valid", phase)
		}
	}
	if PhaseUnspecified.Valid() || Phase(9).Valid() || Phase(255).Valid() {
		t.Fatal("unspecified or out-of-range phase considered valid")
	}
	for phase := PhaseUnspecified; phase <= PhaseCancelled; phase++ {
		wantTerminal := phase == PhaseCompleted || phase == PhaseFailed || phase == PhaseCancelled
		if got := phase.Terminal(); got != wantTerminal {
			t.Errorf("Phase(%d).Terminal() = %v, want %v", phase, got, wantTerminal)
		}
		wantActive := phase.Valid() && phase != PhaseCancelled
		if got := phase.Active(); got != wantActive {
			t.Errorf("Phase(%d).Active() = %v, want %v", phase, got, wantActive)
		}
	}
	if Phase(9).Active() {
		t.Fatal("unknown phase is active")
	}
	if LifecycleOf(PhaseCompleted) != LifecycleSafeToStop || LifecycleOf(PhaseCancelled) != LifecycleReady ||
		LifecycleOf(PhaseFailed) != LifecycleDraining {
		t.Fatal("terminal phase lifecycle mapping is incorrect")
	}

	if !CanAdvance(PhaseUnspecified, PhaseUnspecified) || !CanAdvance(PhaseUnspecified, PhaseExcludingTraffic) ||
		!CanAdvance(PhaseExcludingTraffic, PhaseExcludingTraffic) ||
		!CanAdvance(PhaseVerifyingSessionState, PhaseTransferringSessions) ||
		!CanAdvance(PhaseVerifyingBarrier, PhaseVerifyingBarrier) {
		t.Fatal("allowed phase transition rejected")
	}
	for _, pair := range [][2]Phase{
		{PhaseExcludingTraffic, PhaseVerifyingSessionState},
		{PhaseWaitingForLB, PhaseExcludingTraffic},
		{PhaseVerifyingBarrier, PhaseCompleted},
		{PhaseCompleted, PhaseCompleted},
		{PhaseFailed, PhaseFailed},
		{PhaseCancelled, PhaseCancelled},
		{Phase(9), Phase(9)},
		{PhaseVerifyingBarrier, Phase(6)},
	} {
		if CanAdvance(pair[0], pair[1]) {
			t.Errorf("CanAdvance(%d, %d) = true", pair[0], pair[1])
		}
	}
	if !CanComplete(PhaseVerifyingBarrier) || CanComplete(PhaseTransferringSessions) ||
		CanFail(PhaseFailed) || CanFail(PhaseCompleted) || !CanCancel(PhaseCancelled) || CanCancel(PhaseCompleted) ||
		!CanClear(PhaseCompleted) || CanClear(PhaseFailed) {
		t.Fatal("terminal transition predicate mismatch")
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §2, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-006
func TestValidateParameters(t *testing.T) {
	valid := func(rate uint32, lb, timeout uint64) error {
		return ValidateParameters("mqtt.example:8883", rate, lb, timeout)
	}
	for _, test := range []struct {
		name        string
		rate        uint32
		lb, timeout uint64
		wantErr     bool
	}{
		{name: "minimum rate", rate: 1, lb: 0, timeout: MinTimeoutMillis},
		{name: "maximum rate", rate: 10000, lb: 0, timeout: MaxTimeoutMillis},
		{name: "zero rate", rate: 0, lb: 0, timeout: MinTimeoutMillis, wantErr: true},
		{name: "rate too high", rate: 10001, lb: 0, timeout: MinTimeoutMillis, wantErr: true},
		{name: "maximum propagation", rate: 1, lb: MaxLoadBalancerPropagationDelayMillis, timeout: MaxLoadBalancerPropagationDelayMillis + 1},
		{name: "propagation too high", rate: 1, lb: MaxLoadBalancerPropagationDelayMillis + 1, timeout: MaxLoadBalancerPropagationDelayMillis + 2, wantErr: true},
		{name: "timeout too short", rate: 1, lb: 0, timeout: MinTimeoutMillis - 1, wantErr: true},
		{name: "timeout too high", rate: 1, lb: 0, timeout: MaxTimeoutMillis + 1, wantErr: true},
		{name: "timeout equal propagation", rate: 1, lb: 1000, timeout: 1000, wantErr: true},
		{name: "timeout below propagation", rate: 1, lb: 1000, timeout: 999, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := valid(test.rate, test.lb, test.timeout)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateParameters() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
	if err := ValidateParameters("", 1, 0, MinTimeoutMillis); err != nil {
		t.Fatalf("empty server reference rejected: %v", err)
	}
	if err := ValidateParameters(strings.Repeat("a", MaxServerReferenceBytes), 1, 0, MinTimeoutMillis); err != nil {
		t.Fatalf("maximum server reference rejected: %v", err)
	}
	for name, reference := range map[string]string{
		"too long":      strings.Repeat("a", MaxServerReferenceBytes+1),
		"invalid utf-8": string([]byte{0xff}),
		"nul":           "host\x00name",
		"newline":       "host\nname",
		"delete":        "host\x7fname",
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateParameters(reference, 1, 0, MinTimeoutMillis); err == nil {
				t.Fatal("invalid server reference accepted")
			}
		})
	}
}

// Traceability: DEP-04/DEP-05; docs/specs/drain-and-rolling-update.md §2.1, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-006
func TestDefaultParametersAreValid(t *testing.T) {
	if err := ValidateParameters("", DefaultConnectionRatePerSecond, DefaultLoadBalancerPropagationDelayMillis, DefaultTimeoutMillis); err != nil {
		t.Fatalf("ValidateParameters(defaults) error = %v", err)
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §6, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-007
func TestEvaluateBarrierReportsFirstFailedCondition(t *testing.T) {
	baseline := BarrierInput{
		AdmissionClosed: true, ReadinessFailing: true, ActiveConnections: 0,
		OwnedSessions: 0, OwnedSubscriptions: 0, PendingWAL: 0,
		VoterCount: 3, ConnectedOtherVoters: 2, LocalLeader: false,
		PendingLocalCommits: 0, PendingWills: 0,
	}
	for _, test := range []struct {
		name   string
		mutate func(*BarrierInput)
		want   Blocker
	}{
		{"admission", func(in *BarrierInput) { in.AdmissionClosed = false }, BlockerAdmissionOpen},
		{"readiness", func(in *BarrierInput) { in.ReadinessFailing = false }, BlockerReadinessUp},
		{"connections", func(in *BarrierInput) { in.ActiveConnections = 1 }, BlockerConnectionsRemaining},
		{"unknown connections", func(in *BarrierInput) { in.ActiveConnections = -1 }, BlockerConnectionsRemaining},
		{"owned sessions", func(in *BarrierInput) { in.OwnedSessions = 1 }, BlockerSessionsOwned},
		{"subscriptions", func(in *BarrierInput) { in.OwnedSubscriptions = 1 }, BlockerOrphanSubscription},
		{"pending wal", func(in *BarrierInput) { in.PendingWAL = 1 }, BlockerPendingWAL},
		{"too few voters", func(in *BarrierInput) { in.VoterCount, in.ConnectedOtherVoters = 1, 0 }, BlockerInsufficientReplicas},
		{"two voters have no remaining quorum", func(in *BarrierInput) { in.VoterCount, in.ConnectedOtherVoters = 2, 1 }, BlockerInsufficientReplicas},
		{"replicas below majority", func(in *BarrierInput) { in.VoterCount, in.ConnectedOtherVoters = 3, 1 }, BlockerInsufficientReplicas},
		{"leader", func(in *BarrierInput) { in.LocalLeader = true }, BlockerLocalLeader},
		{"local commit", func(in *BarrierInput) { in.PendingLocalCommits = 1 }, BlockerPendingLocalCommit},
		{"will", func(in *BarrierInput) { in.PendingWills = 1 }, BlockerPendingWill},
	} {
		t.Run(test.name, func(t *testing.T) {
			in := baseline
			test.mutate(&in)
			blocker, safe := EvaluateBarrier(in)
			if safe || blocker != test.want {
				t.Fatalf("EvaluateBarrier() = (%q, %v), want (%q, false)", blocker, safe, test.want)
			}
		})
	}
	if blocker, safe := EvaluateBarrier(baseline); blocker != "" || !safe {
		t.Fatalf("safe barrier = (%q, %v), want empty blocker and true", blocker, safe)
	}
}

// Traceability: DEP-01/DEP-02; docs/specs/drain-and-rolling-update.md §6, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-007
func TestUnderReplicated(t *testing.T) {
	for _, test := range []struct {
		voters, connected int
		want              uint64
	}{
		{3, 2, 0}, {3, 1, 1}, {3, 0, 2}, {1, 0, 0}, {0, 0, 0}, {3, -1, 3},
	} {
		if got := UnderReplicated(test.voters, test.connected); got != test.want {
			t.Errorf("UnderReplicated(%d, %d) = %d, want %d", test.voters, test.connected, got, test.want)
		}
	}
}
