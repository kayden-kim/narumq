// Package clock provides process-boundary time providers.
package clock

import "time"

// SystemClock reads the host wall clock.
type SystemClock struct{}

// Now returns the current wall-clock instant.
func (SystemClock) Now() time.Time {
	return time.Now()
}
