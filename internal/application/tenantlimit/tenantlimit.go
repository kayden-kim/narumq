// Package tenantlimit applies node-local tenant connection, session, subscription
// and publish traffic limits (PRD TEN-06, TEN-07).
package tenantlimit

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	applicationclock "narumq/internal/application/ports/clock"
	"narumq/internal/domain/tenancy"
)

// Dimension identifies one supported tenant limit.
type Dimension string

const (
	DimensionConnections   Dimension = "connections"
	DimensionSessions      Dimension = "sessions"
	DimensionSubscriptions Dimension = "subscriptions"
	DimensionMessageRate   Dimension = "message_rate"
	DimensionByteRate      Dimension = "byte_rate"
	DimensionPayloadSize   Dimension = "payload_size"
)

const (
	MaxCount             = 1_000_000
	MaxMessagesPerSecond = 1_000_000
	MaxBytesPerSecond    = 1 << 40
	MaxPayloadBytes      = 268_435_455
	MaxWaitLimit         = 10 * time.Second
	DefaultMaxWait       = time.Second
	MaxTenantOverrides   = 4096
)

var (
	ErrInvalidProfile = errors.New("tenant limit profile is invalid")
	ErrNilClock       = errors.New("tenant limit clock is nil")
)

// Limits sets each independent bound. Zero disables that dimension.
type Limits struct {
	MaxConnections, MaxSessions, MaxSubscriptions      uint64
	MessagesPerSecond, BytesPerSecond, MaxPayloadBytes uint64
}

// Profile contains fully resolved tenant overrides; MaxWait zero means no wait.
type Profile struct {
	MaxWait time.Duration
	Default Limits
	Tenants map[tenancy.TenantID]Limits
}

// Reporter records every request rejected by a tenant limit.
type Reporter interface {
	TenantLimitRejected(tenantID tenancy.TenantID, dimension Dimension)
}

// ValidateProfile checks bounds and tenant identifiers before admission begins.
func ValidateProfile(profile Profile) error {
	if profile.MaxWait < 0 || profile.MaxWait > MaxWaitLimit {
		return fmt.Errorf("%w: maxWait must be between 0s and 10s", ErrInvalidProfile)
	}
	if len(profile.Tenants) > MaxTenantOverrides {
		return fmt.Errorf("%w: too many tenant overrides", ErrInvalidProfile)
	}
	if err := validateLimits(profile.Default); err != nil {
		return fmt.Errorf("%w: default: %s", ErrInvalidProfile, err)
	}
	for tenantID, limits := range profile.Tenants {
		if _, err := tenancy.NewTenantID(tenantID.String()); err != nil {
			return fmt.Errorf("%w: tenant ID: %s", ErrInvalidProfile, err)
		}
		if err := validateLimits(limits); err != nil {
			return fmt.Errorf("%w: tenant %q: %s", ErrInvalidProfile, tenantID.String(), err)
		}
	}
	return nil
}

func validateLimits(limits Limits) error {
	switch {
	case limits.MaxConnections > MaxCount:
		return errors.New("maxConnections exceeds 1000000")
	case limits.MaxSessions > MaxCount:
		return errors.New("maxSessions exceeds 1000000")
	case limits.MaxSubscriptions > MaxCount:
		return errors.New("maxSubscriptions exceeds 1000000")
	case limits.MessagesPerSecond > MaxMessagesPerSecond:
		return errors.New("messagesPerSecond exceeds 1000000")
	case limits.BytesPerSecond > MaxBytesPerSecond:
		return errors.New("bytesPerSecond exceeds 1099511627776")
	case limits.MaxPayloadBytes > MaxPayloadBytes:
		return errors.New("maxPayloadBytes exceeds 268435455")
	default:
		return nil
	}
}

// LimitsFor returns the resolved override, or the default for other tenants.
func (profile Profile) LimitsFor(tenantID tenancy.TenantID) Limits {
	if limits, found := profile.Tenants[tenantID]; found {
		return limits
	}
	return profile.Default
}

// Service owns node-local traffic reservations. Adapters own the actual wait.
type Service struct {
	clock    applicationclock.Clock
	profile  Profile
	reporter Reporter
	mu       sync.Mutex
	buckets  map[tenancy.TenantID]*trafficBucket
}

type trafficBucket struct {
	messages, bytes float64
	refilled        time.Time
}

// New validates the profile and captures an immutable copy of its overrides.
func New(clock applicationclock.Clock, profile Profile, reporter Reporter) (*Service, error) {
	if clock == nil {
		return nil, ErrNilClock
	}
	if err := ValidateProfile(profile); err != nil {
		return nil, err
	}
	tenants := make(map[tenancy.TenantID]Limits, len(profile.Tenants))
	for tenantID, limits := range profile.Tenants {
		tenants[tenantID] = limits
	}
	profile.Tenants = tenants
	return &Service{clock: clock, profile: profile, reporter: reporter, buckets: make(map[tenancy.TenantID]*trafficBucket)}, nil
}

// Limits returns no bounds when tenant limits are not configured.
func (service *Service) Limits(tenantID tenancy.TenantID) Limits {
	if service == nil {
		return Limits{}
	}
	return service.profile.LimitsFor(tenantID)
}

// Reject reports one rejection without holding the traffic mutex.
func (service *Service) Reject(tenantID tenancy.TenantID, dimension Dimension) {
	if service != nil && service.reporter != nil {
		service.reporter.TenantLimitRejected(tenantID, dimension)
	}
}

// ReservePublish reserves one fresh PUBLISH and its payload bytes, or rejects it
// without consuming tokens. Negative tokens represent previously reserved waits.
func (service *Service) ReservePublish(tenantID tenancy.TenantID, payloadBytes int) (time.Duration, Dimension) {
	limits := service.Limits(tenantID)
	if limits.MaxPayloadBytes > 0 && uint64(payloadBytes) > limits.MaxPayloadBytes {
		service.Reject(tenantID, DimensionPayloadSize)
		return 0, DimensionPayloadSize
	}
	if limits.MessagesPerSecond == 0 && limits.BytesPerSecond == 0 {
		return 0, ""
	}
	service.mu.Lock()
	wait, rejected := service.reserveLocked(tenantID, limits, payloadBytes)
	service.mu.Unlock()
	if rejected != "" {
		service.Reject(tenantID, rejected)
	}
	return wait, rejected
}

func (service *Service) reserveLocked(tenantID tenancy.TenantID, limits Limits, payloadBytes int) (time.Duration, Dimension) {
	now := service.clock.Now()
	bucket := service.buckets[tenantID]
	if bucket == nil {
		bucket = &trafficBucket{messages: float64(limits.MessagesPerSecond), bytes: float64(limits.BytesPerSecond), refilled: now}
		service.buckets[tenantID] = bucket
	} else if elapsed := now.Sub(bucket.refilled); elapsed > 0 {
		bucket.messages = math.Min(float64(limits.MessagesPerSecond), bucket.messages+elapsed.Seconds()*float64(limits.MessagesPerSecond))
		bucket.bytes = math.Min(float64(limits.BytesPerSecond), bucket.bytes+elapsed.Seconds()*float64(limits.BytesPerSecond))
		bucket.refilled = now
	}
	var messageWait, byteWait float64
	if limits.MessagesPerSecond > 0 {
		messageWait = math.Max(0, (1-bucket.messages)/float64(limits.MessagesPerSecond))
	}
	if limits.BytesPerSecond > 0 {
		byteWait = math.Max(0, (float64(payloadBytes)-bucket.bytes)/float64(limits.BytesPerSecond))
	}
	maxWait := service.profile.MaxWait.Seconds()
	if messageWait > maxWait {
		return 0, DimensionMessageRate
	}
	if byteWait > maxWait {
		return 0, DimensionByteRate
	}
	if limits.MessagesPerSecond > 0 {
		bucket.messages--
	}
	if limits.BytesPerSecond > 0 {
		bucket.bytes -= float64(payloadBytes)
	}
	return time.Duration(math.Ceil(math.Max(messageWait, byteWait) * float64(time.Second))), ""
}
