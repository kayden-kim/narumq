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
	"os"
	"strings"
	"unicode/utf8"

	"narumq/internal/domain/mqtt"
)

const (
	purposeSmoke    = "smoke"
	minPayloadBytes = 16
)

// Profile is the fixed BP-01 workload configuration.
//
// JSON durations are non-negative millisecond integers in the repository-wide
// profile convention. The protocol contract is intentionally not configurable:
// BP-01 always uses MQTT 5, QoS 0, clean start, and zero session expiry.
type Profile struct {
	ProfileID            string  `json:"profileId"`
	Purpose              string  `json:"purpose"`
	Seed                 uint64  `json:"seed"`
	Topic                string  `json:"topic"`
	PayloadBytes         int     `json:"payloadBytes"`
	PublishRatePerSecond uint64  `json:"publishRatePerSecond"`
	WarmupMillis         int64   `json:"warmupMillis"`
	MeasureMillis        int64   `json:"measureMillis"`
	DrainMillis          int64   `json:"drainMillis"`
	MaxSamples           uint64  `json:"maxSamples"`
	BrokerCPUs           float64 `json:"brokerCPUs"`
	BrokerMemoryBytes    int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs        float64 `json:"generatorCPUs"`
	GeneratorMemoryBytes int64   `json:"generatorMemoryBytes"`
}

// ProtocolVersion returns the protocol version fixed by BP-01.
func (p Profile) ProtocolVersion() mqtt.ProtocolVersion {
	return mqtt.ProtocolV5
}

// QoS returns the delivery guarantee fixed by BP-01.
func (p Profile) QoS() mqtt.QoS {
	return mqtt.QoSAtMostOnce
}

// CleanStart returns the session flag fixed by BP-01.
func (p Profile) CleanStart() bool {
	return true
}

// SessionExpiryInterval returns the session expiry fixed by BP-01.
func (p Profile) SessionExpiryInterval() uint32 {
	return 0
}

// OfferedMeasurementSamples returns the deterministic number of measurement
// PUBLISH packets scheduled by this profile.
func (p Profile) OfferedMeasurementSamples() uint64 {
	return samplesForDuration(p.PublishRatePerSecond, p.MeasureMillis)
}

// Validate checks the profile before any network or container work starts.
func (p Profile) Validate() error {
	if strings.TrimSpace(p.ProfileID) == "" {
		return errors.New("profileId is required")
	}
	if strings.TrimSpace(p.Purpose) == "" {
		return errors.New("purpose is required")
	}
	if p.Purpose != purposeSmoke {
		return fmt.Errorf("purpose %q is unsupported; only %q is allowed", p.Purpose, purposeSmoke)
	}
	if p.Topic == "" || !utf8.ValidString(p.Topic) || strings.ContainsAny(p.Topic, "+#") || strings.ContainsRune(p.Topic, 0) {
		return errors.New("topic must be a non-empty UTF-8 exact topic without wildcards")
	}
	if p.PayloadBytes < minPayloadBytes {
		return fmt.Errorf("payloadBytes must be at least %d", minPayloadBytes)
	}
	if p.PublishRatePerSecond == 0 {
		return errors.New("publishRatePerSecond must be greater than zero")
	}
	if p.WarmupMillis <= 0 || p.MeasureMillis <= 0 || p.DrainMillis <= 0 {
		return errors.New("warmupMillis, measureMillis, and drainMillis must be greater than zero")
	}
	if p.MaxSamples == 0 {
		return errors.New("maxSamples must be greater than zero")
	}
	if !isPositiveFinite(p.BrokerCPUs) || p.BrokerMemoryBytes <= 0 || !isPositiveFinite(p.GeneratorCPUs) || p.GeneratorMemoryBytes <= 0 {
		return errors.New("broker and generator resource limits must be positive")
	}
	if err := validateProtocolContract(mqtt.ProtocolV5, mqtt.QoSAtMostOnce, true, 0); err != nil {
		return err
	}
	offered := p.OfferedMeasurementSamples()
	if offered == 0 {
		return errors.New("measureMillis is shorter than one publish interval")
	}
	if offered > p.MaxSamples {
		return fmt.Errorf("measurement sample count %d exceeds maxSamples %d", offered, p.MaxSamples)
	}
	return nil
}

func isPositiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validateProtocolContract(version mqtt.ProtocolVersion, qos mqtt.QoS, cleanStart bool, sessionExpiry uint32) error {
	if version != mqtt.ProtocolV5 {
		return fmt.Errorf("BP-01 requires MQTT 5, got protocol version %d", version)
	}
	if qos != mqtt.QoSAtMostOnce {
		return fmt.Errorf("BP-01 requires QoS 0, got QoS %d", qos)
	}
	if !cleanStart {
		return errors.New("BP-01 requires Clean Start=true")
	}
	if sessionExpiry != 0 {
		return fmt.Errorf("BP-01 requires Session Expiry Interval=0, got %d", sessionExpiry)
	}
	return nil
}

func samplesForDuration(rate uint64, millis int64) uint64 {
	if rate == 0 || millis <= 0 {
		return 0
	}
	if uint64(millis) > math.MaxUint64/rate {
		return math.MaxUint64
	}
	return rate * uint64(millis) / 1000
}

type profileJSON struct {
	ProfileID            *string  `json:"profileId"`
	Purpose              *string  `json:"purpose"`
	Seed                 *uint64  `json:"seed"`
	Topic                *string  `json:"topic"`
	PayloadBytes         *int64   `json:"payloadBytes"`
	PublishRatePerSecond *uint64  `json:"publishRatePerSecond"`
	WarmupMillis         *int64   `json:"warmupMillis"`
	MeasureMillis        *int64   `json:"measureMillis"`
	DrainMillis          *int64   `json:"drainMillis"`
	MaxSamples           *uint64  `json:"maxSamples"`
	BrokerCPUs           *float64 `json:"brokerCPUs"`
	BrokerMemoryBytes    *int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs        *float64 `json:"generatorCPUs"`
	GeneratorMemoryBytes *int64   `json:"generatorMemoryBytes"`
}

// DecodeProfile decodes one strict profile JSON document and validates it.
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
	if *raw.PayloadBytes < 0 {
		return Profile{}, errors.New("payloadBytes must not be negative")
	}
	if *raw.BrokerMemoryBytes < 0 || *raw.GeneratorMemoryBytes < 0 {
		return Profile{}, errors.New("memory limits must not be negative")
	}
	profile := Profile{
		ProfileID:            *raw.ProfileID,
		Purpose:              *raw.Purpose,
		Seed:                 *raw.Seed,
		Topic:                *raw.Topic,
		PayloadBytes:         int(*raw.PayloadBytes),
		PublishRatePerSecond: *raw.PublishRatePerSecond,
		WarmupMillis:         *raw.WarmupMillis,
		MeasureMillis:        *raw.MeasureMillis,
		DrainMillis:          *raw.DrainMillis,
		MaxSamples:           *raw.MaxSamples,
		BrokerCPUs:           *raw.BrokerCPUs,
		BrokerMemoryBytes:    *raw.BrokerMemoryBytes,
		GeneratorCPUs:        *raw.GeneratorCPUs,
		GeneratorMemoryBytes: *raw.GeneratorMemoryBytes,
	}
	if err := profile.Validate(); err != nil {
		return Profile{}, fmt.Errorf("validate profile: %w", err)
	}
	return profile, nil
}

func missingProfileField(raw profileJSON) string {
	switch {
	case raw.ProfileID == nil:
		return "profileId"
	case raw.Purpose == nil:
		return "purpose"
	case raw.Seed == nil:
		return "seed"
	case raw.Topic == nil:
		return "topic"
	case raw.PayloadBytes == nil:
		return "payloadBytes"
	case raw.PublishRatePerSecond == nil:
		return "publishRatePerSecond"
	case raw.WarmupMillis == nil:
		return "warmupMillis"
	case raw.MeasureMillis == nil:
		return "measureMillis"
	case raw.DrainMillis == nil:
		return "drainMillis"
	case raw.MaxSamples == nil:
		return "maxSamples"
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

// LoadProfile reads and validates a profile while preserving its original
// bytes for the artifact SHA-256 and manifest.
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
