package main

import (
	"errors"
	"testing"
	"time"
)

func TestScheduleHelpersKeepOpenLoopOrderAndPingSpread(t *testing.T) {
	start := time.Unix(0, 0)
	first := scheduledTime(start, 0, 32)
	second := scheduledTime(start, 1, 32)
	if !second.After(first) || second.Sub(first) != 31250*time.Microsecond {
		t.Fatalf("scheduledTime() second offset = %s, want 31.25ms", second.Sub(first))
	}
	if scheduleInterval(32) != 31250*time.Microsecond {
		t.Fatalf("scheduleInterval() = %s, want 31.25ms", scheduleInterval(32))
	}
	if pingOffset(0, 8, 500*time.Millisecond) != 0 || pingOffset(7, 8, 500*time.Millisecond) != 437500*time.Microsecond {
		t.Fatal("pingOffset() did not spread first pings across the interval")
	}
}

func TestCollectAttemptResultsCountsOverloadAndConnectionErrors(t *testing.T) {
	profile := validBP05Profile()
	counters := workloadCounters{}
	alive := map[string]*connectionRecord{}
	results := []attemptResult{
		{clientID: "bp05-000000", index: 0, success: false, overloaded: true, scheduleLag: time.Second, err: errors.New("transport failure")},
	}
	if err := collectAttemptResults(results, false, alive, &counters); err != nil {
		t.Fatal(err)
	}
	if counters.initialAttempted != 1 || counters.initialFailed != 1 || counters.generatorOverload != 1 || counters.transportErrors != 1 || counters.maxScheduleLag != time.Second {
		t.Fatalf("counters = %#v", counters)
	}
	_ = profile
}
