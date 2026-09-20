package clock

import (
	"sync"
	"testing"
	"time"
)

func TestSystemClock_Now(t *testing.T) {
	clock := SystemClock{}
	before := time.Now()
	now := clock.Now()
	after := time.Now()
	if now.Before(before) || now.After(after) {
		t.Fatalf("clock.Now() %v out of expected range [%v, %v]", now, before, after)
	}
}

func TestSystemScheduler_AfterFunc(t *testing.T) {
	scheduler := SystemScheduler{}
	var wg sync.WaitGroup
	wg.Add(1)
	fired := false
	timer := scheduler.AfterFunc(10*time.Millisecond, func() {
		fired = true
		wg.Done()
	})
	if timer == nil {
		t.Fatal("timer is nil")
	}
	wg.Wait()
	if !fired {
		t.Fatal("callback was not called")
	}

	cancelled := false
	timer2 := scheduler.AfterFunc(100*time.Millisecond, func() {
		cancelled = true
	})
	if !timer2.Stop() {
		t.Fatal("expected Stop to return true")
	}
	time.Sleep(120 * time.Millisecond)
	if cancelled {
		t.Fatal("stopped timer callback was called")
	}
}
