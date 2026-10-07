package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"strings"
	"unicode"
)

const purposeSmoke = "smoke"

// Profile defines the fixed BP-04 subscription-stress smoke workload.
type Profile struct {
	ProfileID                string   `json:"profileId"`
	Purpose                  string   `json:"purpose"`
	Seed                     uint64   `json:"seed"`
	PayloadBytes             int      `json:"payloadBytes"`
	KeepAliveSeconds         uint16   `json:"keepAliveSeconds"`
	FanoutSubscribers        int      `json:"fanoutSubscribers"`
	WildcardSubscribers      int      `json:"wildcardSubscribers"`
	OverlapSubscribers       int      `json:"overlapSubscribers"`
	SharedSubscribers        int      `json:"sharedSubscribers"`
	RatesPerStreamPerSecond  []uint64 `json:"ratesPerStreamPerSecond"`
	WarmupMillis             int64    `json:"warmupMillis"`
	MeasureMillis            int64    `json:"measureMillis"`
	DrainMillis              int64    `json:"drainMillis"`
	IdleBaselineMillis       int64    `json:"idleBaselineMillis"`
	SubscribedIdleMillis     int64    `json:"subscribedIdleMillis"`
	RouteProbes              int      `json:"routeProbes"`
	RouteProbeIntervalMillis int64    `json:"routeProbeIntervalMillis"`
	RouteTimeoutMillis       int64    `json:"routeTimeoutMillis"`
	ReadyTimeoutMillis       int64    `json:"readyTimeoutMillis"`
	SetupTimeoutMillis       int64    `json:"setupTimeoutMillis"`
	MaxMessages              uint64   `json:"maxMessages"`
	BrokerCPUs               float64  `json:"brokerCPUs"`
	BrokerMemoryBytes        int64    `json:"brokerMemoryBytes"`
	GeneratorCPUs            float64  `json:"generatorCPUs"`
	GeneratorMemoryBytes     int64    `json:"generatorMemoryBytes"`
}

// Validate checks the BP-04 smoke bounds before Docker work or message allocation.
func (p Profile) Validate() error {
	if strings.TrimSpace(p.ProfileID) == "" {
		return errors.New("profileId is required")
	}
	if strings.IndexFunc(p.ProfileID, unicode.IsSpace) >= 0 {
		return errors.New("profileId must not contain whitespace")
	}
	if p.Purpose != purposeSmoke {
		return fmt.Errorf("purpose must be %q", purposeSmoke)
	}
	if p.PayloadBytes < 16 || p.PayloadBytes > 65536 {
		return errors.New("payloadBytes must be between 16 and 65536")
	}
	if p.KeepAliveSeconds < 10 || p.KeepAliveSeconds > 300 {
		return errors.New("keepAliveSeconds must be between 10 and 300")
	}
	if p.FanoutSubscribers < 1 || p.FanoutSubscribers > 256 {
		return errors.New("fanoutSubscribers must be between 1 and 256")
	}
	if p.WildcardSubscribers < 6 || p.WildcardSubscribers > 96 || p.WildcardSubscribers%6 != 0 {
		return errors.New("wildcardSubscribers must be between 6 and 96 and divisible by 6")
	}
	if p.OverlapSubscribers < 1 || p.OverlapSubscribers > 64 {
		return errors.New("overlapSubscribers must be between 1 and 64")
	}
	if p.SharedSubscribers < 2 || p.SharedSubscribers > 64 {
		return errors.New("sharedSubscribers must be between 2 and 64")
	}
	if len(p.RatesPerStreamPerSecond) < 1 || len(p.RatesPerStreamPerSecond) > 4 {
		return errors.New("ratesPerStreamPerSecond must contain between 1 and 4 rates")
	}
	for index, rate := range p.RatesPerStreamPerSecond {
		if rate < 1 || rate > 200 {
			return fmt.Errorf("ratesPerStreamPerSecond[%d] must be between 1 and 200", index)
		}
		if index > 0 && rate <= p.RatesPerStreamPerSecond[index-1] {
			return errors.New("ratesPerStreamPerSecond must be strictly increasing")
		}
	}
	if p.WarmupMillis < 1000 || p.WarmupMillis > 60000 {
		return errors.New("warmupMillis must be between 1000 and 60000")
	}
	if p.MeasureMillis < 3000 || p.MeasureMillis > 120000 {
		return errors.New("measureMillis must be between 3000 and 120000")
	}
	if p.DrainMillis < 1000 || p.DrainMillis > 30000 {
		return errors.New("drainMillis must be between 1000 and 30000")
	}
	if p.IdleBaselineMillis < 3000 || p.IdleBaselineMillis > 60000 {
		return errors.New("idleBaselineMillis must be between 3000 and 60000")
	}
	if p.SubscribedIdleMillis < 3000 || p.SubscribedIdleMillis > 60000 {
		return errors.New("subscribedIdleMillis must be between 3000 and 60000")
	}
	if p.RouteProbes < 1 || p.RouteProbes > 64 {
		return errors.New("routeProbes must be between 1 and 64")
	}
	if p.RouteProbeIntervalMillis < 1 || p.RouteProbeIntervalMillis > 100 {
		return errors.New("routeProbeIntervalMillis must be between 1 and 100")
	}
	if p.RouteTimeoutMillis < 1000 || p.RouteTimeoutMillis > 30000 {
		return errors.New("routeTimeoutMillis must be between 1000 and 30000")
	}
	if p.ReadyTimeoutMillis < 10000 || p.ReadyTimeoutMillis > 600000 {
		return errors.New("readyTimeoutMillis must be between 10000 and 600000")
	}
	if p.SetupTimeoutMillis < 5000 || p.SetupTimeoutMillis > 600000 {
		return errors.New("setupTimeoutMillis must be between 5000 and 600000")
	}
	if p.MaxMessages < 1 || p.MaxMessages > 1_000_000 {
		return errors.New("maxMessages must be between 1 and 1000000")
	}
	if !positiveFinite(p.BrokerCPUs) || p.BrokerMemoryBytes <= 0 || !positiveFinite(p.GeneratorCPUs) || p.GeneratorMemoryBytes <= 0 {
		return errors.New("broker and generator resource limits must be positive finite values")
	}
	messageCount, err := p.expectedMessageCount()
	if err != nil {
		return err
	}
	if p.MaxMessages < messageCount {
		return fmt.Errorf("maxMessages %d is below the computed workload total %d", p.MaxMessages, messageCount)
	}
	return nil
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func (p Profile) expectedMessageCount() (uint64, error) {
	durationMillis := uint64(p.WarmupMillis + p.MeasureMillis)
	var totalNumerator uint64
	for _, rate := range p.RatesPerStreamPerSecond {
		hi, lo := bits.Mul64(rate, 4)
		if hi != 0 {
			return 0, errors.New("computed message count overflows uint64")
		}
		hi, lo = bits.Mul64(lo, durationMillis)
		if hi != 0 || math.MaxUint64-totalNumerator < lo {
			return 0, errors.New("computed message count overflows uint64")
		}
		totalNumerator += lo
	}
	count := totalNumerator / 1000
	if totalNumerator%1000 != 0 {
		count++
	}
	return count, nil
}

type profileJSON struct {
	ProfileID                *string   `json:"profileId"`
	Purpose                  *string   `json:"purpose"`
	Seed                     *uint64   `json:"seed"`
	PayloadBytes             *int64    `json:"payloadBytes"`
	KeepAliveSeconds         *uint64   `json:"keepAliveSeconds"`
	FanoutSubscribers        *int64    `json:"fanoutSubscribers"`
	WildcardSubscribers      *int64    `json:"wildcardSubscribers"`
	OverlapSubscribers       *int64    `json:"overlapSubscribers"`
	SharedSubscribers        *int64    `json:"sharedSubscribers"`
	RatesPerStreamPerSecond  *[]uint64 `json:"ratesPerStreamPerSecond"`
	WarmupMillis             *int64    `json:"warmupMillis"`
	MeasureMillis            *int64    `json:"measureMillis"`
	DrainMillis              *int64    `json:"drainMillis"`
	IdleBaselineMillis       *int64    `json:"idleBaselineMillis"`
	SubscribedIdleMillis     *int64    `json:"subscribedIdleMillis"`
	RouteProbes              *int64    `json:"routeProbes"`
	RouteProbeIntervalMillis *int64    `json:"routeProbeIntervalMillis"`
	RouteTimeoutMillis       *int64    `json:"routeTimeoutMillis"`
	ReadyTimeoutMillis       *int64    `json:"readyTimeoutMillis"`
	SetupTimeoutMillis       *int64    `json:"setupTimeoutMillis"`
	MaxMessages              *uint64   `json:"maxMessages"`
	BrokerCPUs               *float64  `json:"brokerCPUs"`
	BrokerMemoryBytes        *int64    `json:"brokerMemoryBytes"`
	GeneratorCPUs            *float64  `json:"generatorCPUs"`
	GeneratorMemoryBytes     *int64    `json:"generatorMemoryBytes"`
}

// DecodeProfile strictly decodes and validates one BP-04 profile.
func DecodeProfile(data []byte) (Profile, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var raw profileJSON
	if err := decoder.Decode(&raw); err != nil {
		return Profile{}, fmt.Errorf("decode profile: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Profile{}, errors.New("decode profile: multiple JSON values")
		}
		return Profile{}, fmt.Errorf("decode profile trailer: %w", err)
	}
	if missing := missingProfileField(raw); missing != "" {
		return Profile{}, fmt.Errorf("profile field %q is required", missing)
	}
	integerFields := []struct {
		name  string
		value int64
	}{
		{"payloadBytes", *raw.PayloadBytes},
		{"fanoutSubscribers", *raw.FanoutSubscribers},
		{"wildcardSubscribers", *raw.WildcardSubscribers},
		{"overlapSubscribers", *raw.OverlapSubscribers},
		{"sharedSubscribers", *raw.SharedSubscribers},
		{"routeProbes", *raw.RouteProbes},
	}
	for _, field := range integerFields {
		if field.value < 0 || uint64(field.value) > uint64(maxInt()) {
			return Profile{}, fmt.Errorf("%s exceeds supported integer range", field.name)
		}
	}
	if *raw.KeepAliveSeconds > math.MaxUint16 {
		return Profile{}, errors.New("keepAliveSeconds exceeds uint16 range")
	}
	profile := Profile{
		ProfileID:                *raw.ProfileID,
		Purpose:                  *raw.Purpose,
		Seed:                     *raw.Seed,
		PayloadBytes:             int(*raw.PayloadBytes),
		KeepAliveSeconds:         uint16(*raw.KeepAliveSeconds),
		FanoutSubscribers:        int(*raw.FanoutSubscribers),
		WildcardSubscribers:      int(*raw.WildcardSubscribers),
		OverlapSubscribers:       int(*raw.OverlapSubscribers),
		SharedSubscribers:        int(*raw.SharedSubscribers),
		RatesPerStreamPerSecond:  append([]uint64(nil), (*raw.RatesPerStreamPerSecond)...),
		WarmupMillis:             *raw.WarmupMillis,
		MeasureMillis:            *raw.MeasureMillis,
		DrainMillis:              *raw.DrainMillis,
		IdleBaselineMillis:       *raw.IdleBaselineMillis,
		SubscribedIdleMillis:     *raw.SubscribedIdleMillis,
		RouteProbes:              int(*raw.RouteProbes),
		RouteProbeIntervalMillis: *raw.RouteProbeIntervalMillis,
		RouteTimeoutMillis:       *raw.RouteTimeoutMillis,
		ReadyTimeoutMillis:       *raw.ReadyTimeoutMillis,
		SetupTimeoutMillis:       *raw.SetupTimeoutMillis,
		MaxMessages:              *raw.MaxMessages,
		BrokerCPUs:               *raw.BrokerCPUs,
		BrokerMemoryBytes:        *raw.BrokerMemoryBytes,
		GeneratorCPUs:            *raw.GeneratorCPUs,
		GeneratorMemoryBytes:     *raw.GeneratorMemoryBytes,
	}
	if err := profile.Validate(); err != nil {
		return Profile{}, fmt.Errorf("validate profile: %w", err)
	}
	return profile, nil
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func missingProfileField(raw profileJSON) string {
	switch {
	case raw.ProfileID == nil:
		return "profileId"
	case raw.Purpose == nil:
		return "purpose"
	case raw.Seed == nil:
		return "seed"
	case raw.PayloadBytes == nil:
		return "payloadBytes"
	case raw.KeepAliveSeconds == nil:
		return "keepAliveSeconds"
	case raw.FanoutSubscribers == nil:
		return "fanoutSubscribers"
	case raw.WildcardSubscribers == nil:
		return "wildcardSubscribers"
	case raw.OverlapSubscribers == nil:
		return "overlapSubscribers"
	case raw.SharedSubscribers == nil:
		return "sharedSubscribers"
	case raw.RatesPerStreamPerSecond == nil:
		return "ratesPerStreamPerSecond"
	case raw.WarmupMillis == nil:
		return "warmupMillis"
	case raw.MeasureMillis == nil:
		return "measureMillis"
	case raw.DrainMillis == nil:
		return "drainMillis"
	case raw.IdleBaselineMillis == nil:
		return "idleBaselineMillis"
	case raw.SubscribedIdleMillis == nil:
		return "subscribedIdleMillis"
	case raw.RouteProbes == nil:
		return "routeProbes"
	case raw.RouteProbeIntervalMillis == nil:
		return "routeProbeIntervalMillis"
	case raw.RouteTimeoutMillis == nil:
		return "routeTimeoutMillis"
	case raw.ReadyTimeoutMillis == nil:
		return "readyTimeoutMillis"
	case raw.SetupTimeoutMillis == nil:
		return "setupTimeoutMillis"
	case raw.MaxMessages == nil:
		return "maxMessages"
	case raw.BrokerCPUs == nil:
		return "brokerCPUs"
	case raw.BrokerMemoryBytes == nil:
		return "brokerMemoryBytes"
	case raw.GeneratorCPUs == nil:
		return "generatorCPUs"
	case raw.GeneratorMemoryBytes == nil:
		return "generatorMemoryBytes"
	default:
		return ""
	}
}

// LoadProfile returns the validated profile, exact source bytes and their SHA-256.
func LoadProfile(path string) (Profile, []byte, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, nil, "", fmt.Errorf("read profile %q: %w", path, err)
	}
	profile, err := DecodeProfile(data)
	if err != nil {
		return Profile{}, nil, "", err
	}
	digest := sha256.Sum256(data)
	return profile, data, hex.EncodeToString(digest[:]), nil
}
