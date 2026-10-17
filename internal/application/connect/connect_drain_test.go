package connect

import (
	"testing"
	"time"
)

// Traceability: DEP-08; docs/specs/drain-and-rolling-update.md §6, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-007
func TestPendingWillCountCountsOnlyScheduledWillTimers(t *testing.T) {
	useCase, _, _, _, _, _, _ := newUseCase(t, time.Now())
	useCase.mu.Lock()
	useCase.activeTimers["will"] = &clientTimers{willTimer: &fakeTimer{}}
	useCase.activeTimers["expiry"] = &clientTimers{expiryTimer: &fakeTimer{}}
	useCase.activeTimers["both"] = &clientTimers{willTimer: &fakeTimer{}, expiryTimer: &fakeTimer{}}
	useCase.activeTimers["nil"] = nil
	useCase.mu.Unlock()

	if got := useCase.PendingWillCount(); got != 2 {
		t.Fatalf("PendingWillCount() = %d, want 2", got)
	}
}
