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

// Profile defines the fixed BP-02 production-mix smoke workload.
type Profile struct {
	ProfileID                   string   `json:"profileId"`
	Purpose                     string   `json:"purpose"`
	Seed                        uint64   `json:"seed"`
	PayloadSizeCycle            []int    `json:"payloadSizeCycle"`
	KeepAliveSeconds            uint16   `json:"keepAliveSeconds"`
	SessionExpirySeconds        uint32   `json:"sessionExpirySeconds"`
	TLSEveryNthClient           int      `json:"tlsEveryNthClient"`
	IdleClientsTenantA          int      `json:"idleClientsTenantA"`
	IdleClientsTenantB          int      `json:"idleClientsTenantB"`
	RatesPerStreamPerSecond     []uint64 `json:"ratesPerStreamPerSecond"`
	MaxInflightPerPublisher     int      `json:"maxInflightPerPublisher"`
	AckTimeoutMillis            int64    `json:"ackTimeoutMillis"`
	WarmupMillis                int64    `json:"warmupMillis"`
	MeasureMillis               int64    `json:"measureMillis"`
	DrainMillis                 int64    `json:"drainMillis"`
	IdleBaselineMillis          int64    `json:"idleBaselineMillis"`
	SubscribedIdleMillis        int64    `json:"subscribedIdleMillis"`
	OfflineSettleMillis         int64    `json:"offlineSettleMillis"`
	OfflineMessagesPerStream    int      `json:"offlineMessagesPerStream"`
	OfflinePublishRatePerSecond uint64   `json:"offlinePublishRatePerSecond"`
	ReconnectTimeoutMillis      int64    `json:"reconnectTimeoutMillis"`
	RetainedTopicsTenantA       int      `json:"retainedTopicsTenantA"`
	RetainedTopicsTenantB       int      `json:"retainedTopicsTenantB"`
	RetainedReplayTimeoutMillis int64    `json:"retainedReplayTimeoutMillis"`
	ReadyTimeoutMillis          int64    `json:"readyTimeoutMillis"`
	SetupTimeoutMillis          int64    `json:"setupTimeoutMillis"`
	MaxMessages                 uint64   `json:"maxMessages"`
	BrokerCPUs                  float64  `json:"brokerCPUs"`
	BrokerMemoryBytes           int64    `json:"brokerMemoryBytes"`
	GeneratorCPUs               float64  `json:"generatorCPUs"`
	GeneratorMemoryBytes        int64    `json:"generatorMemoryBytes"`
}

// Validate checks BP-02 smoke bounds before Docker work or message allocation.
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
	if len(p.PayloadSizeCycle) < 1 || len(p.PayloadSizeCycle) > 32 {
		return errors.New("payloadSizeCycle must contain between 1 and 32 sizes")
	}
	for index, size := range p.PayloadSizeCycle {
		if size < 32 || size > 65536 {
			return fmt.Errorf("payloadSizeCycle[%d] must be between 32 and 65536", index)
		}
	}
	if p.KeepAliveSeconds < 10 || p.KeepAliveSeconds > 300 {
		return errors.New("keepAliveSeconds must be between 10 and 300")
	}
	if p.SessionExpirySeconds < 60 || p.SessionExpirySeconds > 3600 {
		return errors.New("sessionExpirySeconds must be between 60 and 3600")
	}
	if p.TLSEveryNthClient < 1 || p.TLSEveryNthClient > 16 {
		return errors.New("tlsEveryNthClient must be between 1 and 16")
	}
	if p.IdleClientsTenantA < 0 || p.IdleClientsTenantA > 256 || p.IdleClientsTenantB < 0 || p.IdleClientsTenantB > 256 {
		return errors.New("idle client counts must be between 0 and 256")
	}
	if len(p.RatesPerStreamPerSecond) < 1 || len(p.RatesPerStreamPerSecond) > 4 {
		return errors.New("ratesPerStreamPerSecond must contain between 1 and 4 rates")
	}
	for index, rate := range p.RatesPerStreamPerSecond {
		if rate < 1 || rate > 50 {
			return fmt.Errorf("ratesPerStreamPerSecond[%d] must be between 1 and 50", index)
		}
		if index > 0 && rate <= p.RatesPerStreamPerSecond[index-1] {
			return errors.New("ratesPerStreamPerSecond must be strictly increasing")
		}
	}
	if p.MaxInflightPerPublisher < 1 || p.MaxInflightPerPublisher > 64 {
		return errors.New("maxInflightPerPublisher must be between 1 and 64")
	}
	if p.AckTimeoutMillis < 1000 || p.AckTimeoutMillis > 60000 {
		return errors.New("ackTimeoutMillis must be between 1000 and 60000")
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
	if p.IdleBaselineMillis < 3000 || p.IdleBaselineMillis > 60000 || p.SubscribedIdleMillis < 3000 || p.SubscribedIdleMillis > 60000 {
		return errors.New("idle durations must be between 3000 and 60000")
	}
	if p.OfflineSettleMillis < 100 || p.OfflineSettleMillis > 10000 {
		return errors.New("offlineSettleMillis must be between 100 and 10000")
	}
	if p.OfflineMessagesPerStream < 1 || p.OfflineMessagesPerStream > 64 {
		return errors.New("offlineMessagesPerStream must be between 1 and 64")
	}
	if p.OfflinePublishRatePerSecond < 1 || p.OfflinePublishRatePerSecond > 50 {
		return errors.New("offlinePublishRatePerSecond must be between 1 and 50")
	}
	if p.ReconnectTimeoutMillis < 1000 || p.ReconnectTimeoutMillis > 120000 {
		return errors.New("reconnectTimeoutMillis must be between 1000 and 120000")
	}
	if p.RetainedTopicsTenantA < 1 || p.RetainedTopicsTenantA > 64 || p.RetainedTopicsTenantB < 1 || p.RetainedTopicsTenantB > 64 {
		return errors.New("retained topic counts must be between 1 and 64")
	}
	if p.RetainedReplayTimeoutMillis < 1000 || p.RetainedReplayTimeoutMillis > 60000 {
		return errors.New("retainedReplayTimeoutMillis must be between 1000 and 60000")
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
	var total uint64
	for _, rate := range p.RatesPerStreamPerSecond {
		hi, lo := bits.Mul64(rate, 6)
		if hi != 0 {
			return 0, errors.New("computed message count overflows uint64")
		}
		hi, lo = bits.Mul64(lo, durationMillis)
		if hi != 0 {
			return 0, errors.New("computed message count overflows uint64")
		}
		count := lo / 1000
		if lo%1000 != 0 {
			count++
		}
		if math.MaxUint64-total < count {
			return 0, errors.New("computed message count overflows uint64")
		}
		total += count
	}
	extra := uint64(3*p.OfflineMessagesPerStream + 2*(p.RetainedTopicsTenantA+p.RetainedTopicsTenantB))
	if math.MaxUint64-total < extra {
		return 0, errors.New("computed message count overflows uint64")
	}
	return total + extra, nil
}

type profileJSON struct {
	ProfileID                   *string   `json:"profileId"`
	Purpose                     *string   `json:"purpose"`
	Seed                        *uint64   `json:"seed"`
	PayloadSizeCycle            *[]int64  `json:"payloadSizeCycle"`
	KeepAliveSeconds            *uint64   `json:"keepAliveSeconds"`
	SessionExpirySeconds        *uint64   `json:"sessionExpirySeconds"`
	TLSEveryNthClient           *int64    `json:"tlsEveryNthClient"`
	IdleClientsTenantA          *int64    `json:"idleClientsTenantA"`
	IdleClientsTenantB          *int64    `json:"idleClientsTenantB"`
	RatesPerStreamPerSecond     *[]uint64 `json:"ratesPerStreamPerSecond"`
	MaxInflightPerPublisher     *int64    `json:"maxInflightPerPublisher"`
	AckTimeoutMillis            *int64    `json:"ackTimeoutMillis"`
	WarmupMillis                *int64    `json:"warmupMillis"`
	MeasureMillis               *int64    `json:"measureMillis"`
	DrainMillis                 *int64    `json:"drainMillis"`
	IdleBaselineMillis          *int64    `json:"idleBaselineMillis"`
	SubscribedIdleMillis        *int64    `json:"subscribedIdleMillis"`
	OfflineSettleMillis         *int64    `json:"offlineSettleMillis"`
	OfflineMessagesPerStream    *int64    `json:"offlineMessagesPerStream"`
	OfflinePublishRatePerSecond *uint64   `json:"offlinePublishRatePerSecond"`
	ReconnectTimeoutMillis      *int64    `json:"reconnectTimeoutMillis"`
	RetainedTopicsTenantA       *int64    `json:"retainedTopicsTenantA"`
	RetainedTopicsTenantB       *int64    `json:"retainedTopicsTenantB"`
	RetainedReplayTimeoutMillis *int64    `json:"retainedReplayTimeoutMillis"`
	ReadyTimeoutMillis          *int64    `json:"readyTimeoutMillis"`
	SetupTimeoutMillis          *int64    `json:"setupTimeoutMillis"`
	MaxMessages                 *uint64   `json:"maxMessages"`
	BrokerCPUs                  *float64  `json:"brokerCPUs"`
	BrokerMemoryBytes           *int64    `json:"brokerMemoryBytes"`
	GeneratorCPUs               *float64  `json:"generatorCPUs"`
	GeneratorMemoryBytes        *int64    `json:"generatorMemoryBytes"`
}

// DecodeProfile strictly decodes and validates one BP-02 profile.
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
		{"tlsEveryNthClient", *raw.TLSEveryNthClient}, {"idleClientsTenantA", *raw.IdleClientsTenantA},
		{"idleClientsTenantB", *raw.IdleClientsTenantB}, {"maxInflightPerPublisher", *raw.MaxInflightPerPublisher},
		{"offlineMessagesPerStream", *raw.OfflineMessagesPerStream}, {"retainedTopicsTenantA", *raw.RetainedTopicsTenantA},
		{"retainedTopicsTenantB", *raw.RetainedTopicsTenantB},
	}
	for _, field := range integerFields {
		if field.value < 0 || uint64(field.value) > uint64(maxInt()) {
			return Profile{}, fmt.Errorf("%s exceeds supported integer range", field.name)
		}
	}
	for index, size := range *raw.PayloadSizeCycle {
		if size < 0 || uint64(size) > uint64(maxInt()) {
			return Profile{}, fmt.Errorf("payloadSizeCycle[%d] exceeds supported integer range", index)
		}
	}
	if *raw.KeepAliveSeconds > math.MaxUint16 {
		return Profile{}, errors.New("keepAliveSeconds exceeds uint16 range")
	}
	if *raw.SessionExpirySeconds > math.MaxUint32 {
		return Profile{}, errors.New("sessionExpirySeconds exceeds uint32 range")
	}
	profile := Profile{
		ProfileID: *raw.ProfileID, Purpose: *raw.Purpose, Seed: *raw.Seed,
		PayloadSizeCycle: toInts(*raw.PayloadSizeCycle), KeepAliveSeconds: uint16(*raw.KeepAliveSeconds),
		SessionExpirySeconds: uint32(*raw.SessionExpirySeconds), TLSEveryNthClient: int(*raw.TLSEveryNthClient),
		IdleClientsTenantA: int(*raw.IdleClientsTenantA), IdleClientsTenantB: int(*raw.IdleClientsTenantB),
		RatesPerStreamPerSecond: append([]uint64(nil), (*raw.RatesPerStreamPerSecond)...),
		MaxInflightPerPublisher: int(*raw.MaxInflightPerPublisher), AckTimeoutMillis: *raw.AckTimeoutMillis,
		WarmupMillis: *raw.WarmupMillis, MeasureMillis: *raw.MeasureMillis, DrainMillis: *raw.DrainMillis,
		IdleBaselineMillis: *raw.IdleBaselineMillis, SubscribedIdleMillis: *raw.SubscribedIdleMillis,
		OfflineSettleMillis: *raw.OfflineSettleMillis, OfflineMessagesPerStream: int(*raw.OfflineMessagesPerStream),
		OfflinePublishRatePerSecond: *raw.OfflinePublishRatePerSecond, ReconnectTimeoutMillis: *raw.ReconnectTimeoutMillis,
		RetainedTopicsTenantA: int(*raw.RetainedTopicsTenantA), RetainedTopicsTenantB: int(*raw.RetainedTopicsTenantB),
		RetainedReplayTimeoutMillis: *raw.RetainedReplayTimeoutMillis, ReadyTimeoutMillis: *raw.ReadyTimeoutMillis,
		SetupTimeoutMillis: *raw.SetupTimeoutMillis, MaxMessages: *raw.MaxMessages,
		BrokerCPUs: *raw.BrokerCPUs, BrokerMemoryBytes: *raw.BrokerMemoryBytes,
		GeneratorCPUs: *raw.GeneratorCPUs, GeneratorMemoryBytes: *raw.GeneratorMemoryBytes,
	}
	if err := profile.Validate(); err != nil {
		return Profile{}, fmt.Errorf("validate profile: %w", err)
	}
	return profile, nil
}

func toInts(values []int64) []int {
	out := make([]int, len(values))
	for index, value := range values {
		out[index] = int(value)
	}
	return out
}

func maxInt() int { return int(^uint(0) >> 1) }

func missingProfileField(raw profileJSON) string {
	switch {
	case raw.ProfileID == nil:
		return "profileId"
	case raw.Purpose == nil:
		return "purpose"
	case raw.Seed == nil:
		return "seed"
	case raw.PayloadSizeCycle == nil:
		return "payloadSizeCycle"
	case raw.KeepAliveSeconds == nil:
		return "keepAliveSeconds"
	case raw.SessionExpirySeconds == nil:
		return "sessionExpirySeconds"
	case raw.TLSEveryNthClient == nil:
		return "tlsEveryNthClient"
	case raw.IdleClientsTenantA == nil:
		return "idleClientsTenantA"
	case raw.IdleClientsTenantB == nil:
		return "idleClientsTenantB"
	case raw.RatesPerStreamPerSecond == nil:
		return "ratesPerStreamPerSecond"
	case raw.MaxInflightPerPublisher == nil:
		return "maxInflightPerPublisher"
	case raw.AckTimeoutMillis == nil:
		return "ackTimeoutMillis"
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
	case raw.OfflineSettleMillis == nil:
		return "offlineSettleMillis"
	case raw.OfflineMessagesPerStream == nil:
		return "offlineMessagesPerStream"
	case raw.OfflinePublishRatePerSecond == nil:
		return "offlinePublishRatePerSecond"
	case raw.ReconnectTimeoutMillis == nil:
		return "reconnectTimeoutMillis"
	case raw.RetainedTopicsTenantA == nil:
		return "retainedTopicsTenantA"
	case raw.RetainedTopicsTenantB == nil:
		return "retainedTopicsTenantB"
	case raw.RetainedReplayTimeoutMillis == nil:
		return "retainedReplayTimeoutMillis"
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
