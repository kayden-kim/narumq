// Package clock defines the application-owned time boundary.
package clock

import "time"

// Clock supplies the instant used by time-sensitive application policies.
type Clock interface {
	Now() time.Time
}
