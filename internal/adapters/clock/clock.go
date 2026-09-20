// Package clock provides process-boundary time providers.
package clock

import (
	"time"

	"narumq/internal/application/connect"
)

// SystemClock reads the host wall clock.
type SystemClock struct{}

// Now returns the current wall-clock instant.
func (SystemClock) Now() time.Time {
	return time.Now()
}

// SystemScheduler schedules callbacks using runtime timers.
type SystemScheduler struct{}

// AfterFunc waits for duration d to elapse and then calls f in its own goroutine.
func (SystemScheduler) AfterFunc(d time.Duration, f func()) connect.Timer {
	return time.AfterFunc(d, f)
}
