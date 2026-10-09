package management

import (
	"math"
	"net"
	"sync"
	"time"

	applicationclock "narumq/internal/application/ports/clock"
)

// DefaultRequestRate is the default per-second request rate and burst per authenticated principal.
const DefaultRequestRate = 50

// DefaultAuthFailureRate is the default per-second failed-authentication rate and burst per remote IP.
const DefaultAuthFailureRate = 5

// MaxRequestRate is the maximum configured management API request rate and burst.
const MaxRequestRate = 1_000_000

const maxRateLimitBuckets = 4096

type requestRateLimiter struct {
	rate    float64
	clock   applicationclock.Clock
	mu      sync.Mutex
	buckets map[string]*rateBucket
}

type rateBucket struct {
	tokens   float64
	refilled time.Time
}

func newRequestRateLimiter(rate int, clock applicationclock.Clock) *requestRateLimiter {
	return &requestRateLimiter{
		rate:    float64(rate),
		clock:   clock,
		buckets: make(map[string]*rateBucket),
	}
}

func (limiter *requestRateLimiter) allow(key string) bool {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	now := limiter.clock.Now()
	bucket, exists := limiter.buckets[key]
	if !exists {
		if len(limiter.buckets) >= maxRateLimitBuckets {
			for existingKey, existingBucket := range limiter.buckets {
				limiter.refill(existingBucket, now)
				if existingBucket.tokens >= limiter.rate {
					delete(limiter.buckets, existingKey)
				}
			}
			if len(limiter.buckets) >= maxRateLimitBuckets {
				return false
			}
		}
		bucket = &rateBucket{tokens: limiter.rate, refilled: now}
		limiter.buckets[key] = bucket
	} else {
		limiter.refill(bucket, now)
	}

	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

func (limiter *requestRateLimiter) refill(bucket *rateBucket, now time.Time) {
	if elapsed := now.Sub(bucket.refilled); elapsed > 0 {
		bucket.tokens = math.Min(limiter.rate, bucket.tokens+elapsed.Seconds()*limiter.rate)
		bucket.refilled = now
	}
}

func remoteAddressKey(remoteAddr string) string {
	if remoteAddr == "" {
		return "unknown"
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}
