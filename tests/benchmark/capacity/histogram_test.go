package main

import (
	"testing"
	"time"
)

func TestHistogramQuantilesAreNearestRankAndExactAtTheLimit(t *testing.T) {
	h := newHistogram()
	for range 99 {
		h.Record(10 * time.Millisecond)
	}
	h.Record(100 * time.Millisecond)
	if got := h.Quantile(0.99); got != 10*time.Millisecond {
		t.Fatalf("p99 of 99x10ms + 1x100ms = %s, want 10ms (nearest rank 99)", got)
	}
	if got := h.Quantile(1); got != 100*time.Millisecond {
		t.Fatalf("p100 = %s, want exactly 100ms so a p99 at the limit stays a pass", got)
	}
	if got := h.Max(); got != 100*time.Millisecond || h.Count() != 100 {
		t.Fatalf("max %s count %d", got, h.Count())
	}
}

func TestHistogramKeepsLargeValuesWithinOnePercent(t *testing.T) {
	h := newHistogram()
	h.Record(1700 * time.Millisecond)
	got := h.Quantile(0.5)
	if got < 1680*time.Millisecond || got > 1700*time.Millisecond {
		t.Fatalf("1.7s recorded as %s", got)
	}
}

func TestNearestRankAndBoundsMessageSize(t *testing.T) {
	p := testProfile(t)
	bounds, reason := computeBounds(p, capacityOutcome{})
	if bounds != nil || reason == "" {
		t.Fatalf("local-smoke bounds = %+v (%q), want none with a reason", bounds, reason)
	}
	p.Purpose = purposeEKSCapacity
	adopted := &adoptedStep{Step: 2, Cells: 4, Rate: 2, Offered: 64, ConfirmRuns: []int{2, 3}}
	runs := []runRecord{
		{Phase: phaseConfirm, Step: 2, Run: 2, summary: &stepSummary{DeliveryRatesN1: [3]float64{10, 20, 30}}},
		{Phase: phaseConfirm, Step: 2, Run: 3, summary: &stepSummary{DeliveryRatesN1: [3]float64{30, 20, 10}}},
	}
	bounds, reason = computeBounds(p, capacityOutcome{Adopted: adopted, Runs: runs})
	if bounds == nil {
		t.Fatalf("no bounds: %s", reason)
	}
	if size := bounds.BMsgSize; size.P50 != 128 || size.P95 != 16384 || size.P99 != 16384 || size.Max != 16384 {
		t.Fatalf("B_msg_size = %+v, want p50 128 and p95/p99/max 16384", size)
	}
	if bounds.BConn.Connections != 4*cellClients || bounds.BConn.TLS+bounds.BConn.TCP != bounds.BConn.Connections {
		t.Fatalf("B_conn = %+v", bounds.BConn)
	}
	if bounds.BSub.DeliveriesPerSecond != 60 {
		t.Fatalf("B_sub median = %v, want 60", bounds.BSub.DeliveriesPerSecond)
	}
}
