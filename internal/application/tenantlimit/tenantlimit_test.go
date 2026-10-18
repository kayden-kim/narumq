package tenantlimit

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"narumq/internal/domain/tenancy"
)

type fakeClock struct{ now time.Time }

func (clock *fakeClock) Now() time.Time { return clock.now }

type rejection struct {
	tenant    tenancy.TenantID
	dimension Dimension
}
type rejectionReporter struct{ calls []rejection }

func (reporter *rejectionReporter) TenantLimitRejected(tenant tenancy.TenantID, dimension Dimension) {
	reporter.calls = append(reporter.calls, rejection{tenant, dimension})
}

func tenant(t *testing.T, name string) tenancy.TenantID {
	t.Helper()
	id, err := tenancy.NewTenantID(name)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func newService(t *testing.T, limits Limits, maxWait time.Duration) (*Service, *fakeClock, *rejectionReporter) {
	t.Helper()
	clock := &fakeClock{now: time.Unix(100, 0)}
	reporter := &rejectionReporter{}
	service, err := New(clock, Profile{MaxWait: maxWait, Default: limits}, reporter)
	if err != nil {
		t.Fatal(err)
	}
	return service, clock, reporter
}
func requireReservation(t *testing.T, service *Service, id tenancy.TenantID, bytes int, expectedWait time.Duration, expected Dimension) {
	t.Helper()
	wait, rejected := service.ReservePublish(id, bytes)
	if wait != expectedWait || rejected != expected {
		t.Fatalf("reservation=(%s,%q), want=(%s,%q)", wait, rejected, expectedWait, expected)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-028
func TestValidateProfile(t *testing.T) {
	max := Limits{MaxConnections: MaxCount, MaxSessions: MaxCount, MaxSubscriptions: MaxCount,
		MessagesPerSecond: MaxMessagesPerSecond, BytesPerSecond: MaxBytesPerSecond, MaxPayloadBytes: MaxPayloadBytes}
	for _, profile := range []Profile{{}, {MaxWait: MaxWaitLimit, Default: max}, {Tenants: map[tenancy.TenantID]Limits{tenant(t, "a"): max}}} {
		if err := ValidateProfile(profile); err != nil {
			t.Fatal(err)
		}
	}
	for _, limits := range []Limits{{MaxConnections: MaxCount + 1}, {MaxSessions: MaxCount + 1}, {MaxSubscriptions: MaxCount + 1},
		{MessagesPerSecond: MaxMessagesPerSecond + 1}, {BytesPerSecond: MaxBytesPerSecond + 1}, {MaxPayloadBytes: MaxPayloadBytes + 1}} {
		for _, profile := range []Profile{{Default: limits}, {Tenants: map[tenancy.TenantID]Limits{tenant(t, "a"): limits}}} {
			if err := ValidateProfile(profile); !errors.Is(err, ErrInvalidProfile) {
				t.Fatalf("profile=%+v error=%v", profile, err)
			}
		}
	}
	for _, wait := range []time.Duration{-1, MaxWaitLimit + 1} {
		if err := ValidateProfile(Profile{MaxWait: wait}); !errors.Is(err, ErrInvalidProfile) {
			t.Fatalf("wait=%v error=%v", wait, err)
		}
	}
	if err := ValidateProfile(Profile{Tenants: map[tenancy.TenantID]Limits{{}: {}}}); !errors.Is(err, ErrInvalidProfile) {
		t.Fatal(err)
	}
	many := make(map[tenancy.TenantID]Limits, MaxTenantOverrides+1)
	for i := 0; i < MaxTenantOverrides; i++ {
		many[tenant(t, fmt.Sprintf("t-%d", i))] = Limits{}
	}
	if err := ValidateProfile(Profile{Tenants: many}); err != nil {
		t.Fatal(err)
	}
	many[tenant(t, "extra")] = Limits{}
	if err := ValidateProfile(Profile{Tenants: many}); !errors.Is(err, ErrInvalidProfile) {
		t.Fatal(err)
	}
	if _, err := New(nil, Profile{}, nil); !errors.Is(err, ErrNilClock) {
		t.Fatal(err)
	}
	if _, err := New(&fakeClock{}, Profile{MaxWait: -1}, nil); !errors.Is(err, ErrInvalidProfile) {
		t.Fatal(err)
	}
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-028
func TestLimitsFor(t *testing.T) {
	a, b := tenant(t, "a"), tenant(t, "b")
	profile := Profile{Default: Limits{MaxConnections: 3}, Tenants: map[tenancy.TenantID]Limits{a: {MaxSessions: 2}}}
	if got := profile.LimitsFor(a); got != (Limits{MaxSessions: 2}) {
		t.Fatal(got)
	}
	if got := profile.LimitsFor(b); got != profile.Default {
		t.Fatal(got)
	}
	service, err := New(&fakeClock{}, profile, nil)
	if err != nil {
		t.Fatal(err)
	}
	profile.Tenants[a] = Limits{MaxSessions: 99}
	if got := service.Limits(a); got != (Limits{MaxSessions: 2}) {
		t.Fatalf("profile mutation changed admission: %+v", got)
	}
	var disabled *Service
	if got := disabled.Limits(a); got != (Limits{}) {
		t.Fatal(got)
	}
	disabled.Reject(a, DimensionSessions)
	requireReservation(t, disabled, a, 100, 0, "")
}

// Traceability: TEN-06, TEN-07; docs/specs/tenancy-and-security.md §5.2.
// Scenarios: tenancy-and-security.md:SEC-SCN-026, tenancy-and-security.md:SEC-SCN-027
func TestReservePublish(t *testing.T) {
	a, b := tenant(t, "a"), tenant(t, "b")
	t.Run("PayloadBeforeRate", func(t *testing.T) {
		service, _, reporter := newService(t, Limits{MaxPayloadBytes: 16, MessagesPerSecond: 1}, 0)
		requireReservation(t, service, a, 17, 0, DimensionPayloadSize)
		requireReservation(t, service, a, 16, 0, "")
		requireReservation(t, service, a, 1, 0, DimensionMessageRate)
		if !reflect.DeepEqual(reporter.calls, []rejection{{a, DimensionPayloadSize}, {a, DimensionMessageRate}}) {
			t.Fatal(reporter.calls)
		}
	})
	t.Run("BurstWaitRejectWithoutCharge", func(t *testing.T) {
		service, clock, reporter := newService(t, Limits{MessagesPerSecond: 2}, time.Second)
		requireReservation(t, service, a, 0, 0, "")
		requireReservation(t, service, a, 0, 0, "")
		requireReservation(t, service, a, 0, 500*time.Millisecond, "")
		requireReservation(t, service, a, 0, time.Second, "")
		requireReservation(t, service, a, 0, 0, DimensionMessageRate)
		requireReservation(t, service, b, 0, 0, "")
		clock.now = clock.now.Add(500 * time.Millisecond)
		requireReservation(t, service, a, 0, time.Second, "")
		if !reflect.DeepEqual(reporter.calls, []rejection{{a, DimensionMessageRate}}) {
			t.Fatal(reporter.calls)
		}
		clock.now = clock.now.Add(10 * time.Second)
		requireReservation(t, service, a, 0, 0, "")
		requireReservation(t, service, a, 0, 0, "")
		requireReservation(t, service, a, 0, 500*time.Millisecond, "")
	})
	t.Run("ByteDeficitAndNanosecondCeiling", func(t *testing.T) {
		service, _, _ := newService(t, Limits{BytesPerSecond: 3}, time.Second)
		requireReservation(t, service, a, 3, 0, "")
		requireReservation(t, service, a, 1, 333333334*time.Nanosecond, "")
		requireReservation(t, service, a, 3, 0, DimensionByteRate)
		requireReservation(t, service, a, 2, time.Second, "")
	})
	t.Run("BothDimensionsMessagePrecedence", func(t *testing.T) {
		service, _, _ := newService(t, Limits{MessagesPerSecond: 1, BytesPerSecond: 1}, 0)
		requireReservation(t, service, a, 1, 0, "")
		requireReservation(t, service, a, 1, 0, DimensionMessageRate)
	})
	t.Run("MaximumOfDimensions", func(t *testing.T) {
		service, _, _ := newService(t, Limits{MessagesPerSecond: 2, BytesPerSecond: 4}, time.Second)
		requireReservation(t, service, a, 4, 0, "")
		requireReservation(t, service, a, 3, 750*time.Millisecond, "")
	})
	t.Run("ClockReversalAndRefill", func(t *testing.T) {
		service, clock, _ := newService(t, Limits{MessagesPerSecond: 1, BytesPerSecond: 2}, 0)
		requireReservation(t, service, a, 2, 0, "")
		clock.now = clock.now.Add(-time.Second)
		requireReservation(t, service, a, 0, 0, DimensionMessageRate)
		clock.now = clock.now.Add(time.Second)
		requireReservation(t, service, a, 0, 0, DimensionMessageRate)
		clock.now = clock.now.Add(time.Second)
		requireReservation(t, service, a, 2, 0, "")
	})
	t.Run("DisabledRatesAllocateNoBucket", func(t *testing.T) {
		service, _, _ := newService(t, Limits{}, 0)
		requireReservation(t, service, a, 100, 0, "")
		if len(service.buckets) != 0 {
			t.Fatal(service.buckets)
		}
	})
	t.Run("NilReporter", func(t *testing.T) {
		service, err := New(&fakeClock{}, Profile{Default: Limits{MaxPayloadBytes: 1}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		requireReservation(t, service, a, 2, 0, DimensionPayloadSize)
	})
}
