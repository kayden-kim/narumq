package connrebalance

import "testing"

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-223
func TestEvaluate(t *testing.T) {
	tests := []struct {
		name           string
		reports        []Report
		deviation, now uint64
		want           Evaluation
	}{
		{"empty", nil, 5, 20_000, Evaluation{}},
		{"imbalanced", []Report{{Connections: 60, ReportedAtUnixMillis: 5000}, {Connections: 60, ReportedAtUnixMillis: 5000}, {ReportedAtUnixMillis: 5000}}, 5, 20_000, Evaluation{Fresh: true, Total: 120, FairShare: 40, Ceiling: 45}},
		{"balanced", []Report{{Connections: 40, ReportedAtUnixMillis: 5000}, {Connections: 40, ReportedAtUnixMillis: 5000}, {Connections: 40, ReportedAtUnixMillis: 5000}}, 5, 20_000, Evaluation{Fresh: true, Converged: true, Total: 120, FairShare: 40, Ceiling: 45}},
		{"shedding", []Report{{Connections: 40, Shedding: true, ReportedAtUnixMillis: 5000}}, 5, 20_000, Evaluation{Fresh: true, Total: 40, FairShare: 40, Ceiling: 45}},
		{"stale", []Report{{Connections: 7, ReportedAtUnixMillis: 4999}}, 1, 20_000, Evaluation{Total: 7, FairShare: 7, Ceiling: 8}},
		{"missing", []Report{{Connections: 7}}, 1, 20_000, Evaluation{Total: 7, FairShare: 7, Ceiling: 8}},
		{"rounding and future skew", []Report{{Connections: 7, ReportedAtUnixMillis: 30_000}, {ReportedAtUnixMillis: 30_000}, {ReportedAtUnixMillis: 30_000}}, 1, 20_000, Evaluation{Fresh: true, Total: 7, FairShare: 3, Ceiling: 4}},
		{"at ceiling", []Report{{Connections: 4, ReportedAtUnixMillis: 1}, {Connections: 2, ReportedAtUnixMillis: 1}}, 1, 1, Evaluation{Fresh: true, Converged: true, Total: 6, FairShare: 3, Ceiling: 4}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Evaluate(test.reports, test.deviation, test.now); got != test.want {
				t.Fatalf("got %+v, want %+v", got, test.want)
			}
		})
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-223
func TestStep(t *testing.T) {
	eval := Evaluation{Fresh: true, FairShare: 40, Ceiling: 45}
	tests := []struct {
		name              string
		state             ShedState
		eval              Evaluation
		count, now, delay uint64
		want              ShedState
		transfer          bool
	}{
		{"stale idle", ShedState{}, Evaluation{}, 60, 100, 10, ShedState{}, false},
		{"ceiling idle", ShedState{}, eval, 45, 100, 10, ShedState{}, false},
		{"start", ShedState{}, eval, 60, 100, 10, ShedState{ShedWaitingForLB, 40, 110}, false},
		{"wait", ShedState{ShedWaitingForLB, 40, 110}, eval, 60, 109, 10, ShedState{ShedWaitingForLB, 40, 110}, false},
		{"lb ready", ShedState{ShedWaitingForLB, 40, 110}, eval, 60, 110, 10, ShedState{ShedTransferring, 40, 110}, false},
		{"transfer", ShedState{ShedTransferring, 40, 110}, eval, 41, 111, 10, ShedState{ShedTransferring, 40, 110}, true},
		{"target reached", ShedState{ShedTransferring, 40, 110}, eval, 40, 111, 10, ShedState{ShedSettling, 40, 5111}, false},
		{"below target", ShedState{ShedTransferring, 40, 110}, eval, 39, 111, 10, ShedState{ShedSettling, 40, 5111}, false},
		{"settling", ShedState{ShedSettling, 40, 5111}, eval, 40, 5110, 10, ShedState{ShedSettling, 40, 5111}, false},
		{"settled", ShedState{ShedSettling, 40, 5111}, eval, 40, 5111, 10, ShedState{}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, transfer := Step(test.state, test.eval, test.count, test.now, test.delay)
			if got != test.want || transfer != test.transfer {
				t.Fatalf("got %+v %v, want %+v %v", got, transfer, test.want, test.transfer)
			}
			if got.Shedding() != (got.Stage != ShedIdle) {
				t.Fatal("shedding stage mismatch")
			}
		})
	}
}

func TestValidateParameters(t *testing.T) {
	for _, d := range []uint64{0, 1, 1_000_000, 1_000_001} {
		if got := ValidateParameters("lb:1883", d, 5, 500, 120_000); (got == nil) != (d >= 1 && d <= 1_000_000) {
			t.Fatalf("deviation %d: %v", d, got)
		}
	}
	if ValidateParameters("", 1, 0, 0, 1000) == nil {
		t.Fatal("accepted invalid drain parameters")
	}
}

func TestPhase(t *testing.T) {
	for _, test := range []struct {
		phase                   Phase
		name                    string
		valid, active, terminal bool
	}{{PhaseUnspecified, "UNSPECIFIED", false, false, false}, {PhaseRunning, "RUNNING", true, true, false}, {PhaseCompleted, "COMPLETED", true, false, true}, {PhaseFailed, "FAILED", true, false, true}, {PhaseCancelled, "CANCELLED", true, false, true}, {Phase(99), "UNSPECIFIED", false, false, false}} {
		if test.phase.String() != test.name || test.phase.Valid() != test.valid || test.phase.Active() != test.active || test.phase.Terminal() != test.terminal {
			t.Fatalf("incorrect phase %d", test.phase)
		}
	}
}
