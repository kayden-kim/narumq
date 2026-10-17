package main

import (
	"fmt"
	"slices"
	"testing"
)

func ladderProfile() Profile {
	return Profile{
		Steps: []StepSpec{
			{Cells: 8, RatePerStreamPerSecond: 5}, {Cells: 16, RatePerStreamPerSecond: 5},
			{Cells: 32, RatePerStreamPerSecond: 5}, {Cells: 64, RatePerStreamPerSecond: 5},
		},
		BisectRounds: 2, ConfirmRuns: 2,
	}
}

type recordedRun struct {
	cfg stepConfig
}

// scripted passes a run when pass(cfg) is true, reports invalid when invalid(cfg) is true, and records the order.
func scripted(pass func(stepConfig) bool, invalid func(stepConfig) bool, order *[]recordedRun) stepExecutor {
	return func(cfg stepConfig) (stepOutcome, error) {
		*order = append(*order, recordedRun{cfg})
		switch {
		case invalid != nil && invalid(cfg):
			return stepOutcome{Verdict: verdictInvalid, Reasons: []string{"scripted invalid"}}, nil
		case pass(cfg):
			return stepOutcome{Verdict: verdictPass}, nil
		default:
			return stepOutcome{Verdict: verdictFail, Reasons: []string{"scripted fail"}}, nil
		}
	}
}

func sequence(order []recordedRun) []string {
	out := make([]string, len(order))
	for i, r := range order {
		out[i] = fmt.Sprintf("s%d-r%d-c%d", r.cfg.Step, r.cfg.Run, r.cfg.Cells)
	}
	return out
}

// TestDriveCapacityLadderBisectThenConfirmHighestPass bisects around the first failure and confirms the candidate.
// Scenarios: performance-and-verification.md:PRF-SCN-051
func TestDriveCapacityLadderBisectThenConfirmHighestPass(t *testing.T) {
	var order []recordedRun
	outcome, err := driveCapacity(ladderProfile(), scripted(func(c stepConfig) bool { return c.Cells <= 40 }, nil, &order))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"s1-r1-c8", "s2-r1-c16", "s3-r1-c32", "s4-r1-c64", // ladder stops at the first failure
		"s5-r1-c48", "s6-r1-c40", // bisect between 32 (pass) and 64 (fail)
		"s6-r2-c40", "s6-r3-c40", // two confirmation runs of the highest passing configuration
	}
	if got := sequence(order); !slices.Equal(got, want) {
		t.Fatalf("run order\n got %v\nwant %v", got, want)
	}
	if outcome.Adopted == nil || outcome.Adopted.Cells != 40 || outcome.Adopted.Step != 6 || !slices.Equal(outcome.Adopted.ConfirmRuns, []int{2, 3}) {
		t.Fatalf("adopted = %+v", outcome.Adopted)
	}
	if outcome.LadderExhausted {
		t.Fatal("ladder reported exhausted although step 4 failed")
	}
}

// TestDriveCapacityFallsBackWhenConfirmationFails confirms the next lower candidate after a failed confirmation.
// Scenarios: performance-and-verification.md:PRF-SCN-051
func TestDriveCapacityFallsBackWhenConfirmationFails(t *testing.T) {
	var order []recordedRun
	pass := func(c stepConfig) bool { return c.Cells <= 40 && !(c.Cells == 40 && c.Run == 3) }
	outcome, err := driveCapacity(ladderProfile(), scripted(pass, nil, &order))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"s1-r1-c8", "s2-r1-c16", "s3-r1-c32", "s4-r1-c64", "s5-r1-c48", "s6-r1-c40",
		"s6-r2-c40", "s6-r3-c40", // second confirmation fails, so 40 is dropped
		"s3-r2-c32", "s3-r3-c32", // next lower passing configuration is confirmed
	}
	if got := sequence(order); !slices.Equal(got, want) {
		t.Fatalf("run order\n got %v\nwant %v", got, want)
	}
	if outcome.Adopted == nil || outcome.Adopted.Cells != 32 {
		t.Fatalf("adopted = %+v, want 32 cells", outcome.Adopted)
	}
}

// TestDriveCapacityInvalidStopsTheSearch stops the search at an invalid step.
// Scenarios: performance-and-verification.md:PRF-SCN-051
func TestDriveCapacityInvalidStopsTheSearch(t *testing.T) {
	var order []recordedRun
	invalid := func(c stepConfig) bool { return c.Cells == 32 }
	outcome, err := driveCapacity(ladderProfile(), scripted(func(stepConfig) bool { return true }, invalid, &order))
	if err != nil {
		t.Fatal(err)
	}
	if got := sequence(order); !slices.Equal(got, []string{"s1-r1-c8", "s2-r1-c16", "s3-r1-c32"}) {
		t.Fatalf("run order %v continued after an invalid step", got)
	}
	if outcome.Adopted != nil || outcome.Aborted == "" || outcome.NullReason == "" {
		t.Fatalf("outcome after invalid = %+v", outcome)
	}
}

// TestDriveCapacityFirstStepFailureAdoptsNothing adopts nothing when the first step fails.
// Scenarios: performance-and-verification.md:PRF-SCN-051
func TestDriveCapacityFirstStepFailureAdoptsNothing(t *testing.T) {
	var order []recordedRun
	outcome, err := driveCapacity(ladderProfile(), scripted(func(stepConfig) bool { return false }, nil, &order))
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != 1 || outcome.Adopted != nil || outcome.NullReason == "" {
		t.Fatalf("order %v outcome %+v", sequence(order), outcome)
	}
}

// TestDriveCapacityExhaustedLadderConfirmsTopStep confirms the top step when no step fails.
// Scenarios: performance-and-verification.md:PRF-SCN-051
func TestDriveCapacityExhaustedLadderConfirmsTopStep(t *testing.T) {
	var order []recordedRun
	outcome, err := driveCapacity(ladderProfile(), scripted(func(stepConfig) bool { return true }, nil, &order))
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.LadderExhausted || outcome.Adopted == nil || outcome.Adopted.Cells != 64 || len(order) != 4+2 {
		t.Fatalf("outcome %+v after %v", outcome, sequence(order))
	}
}
