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
)

const purposeSmoke = "smoke"

// Profile defines the fixed BP-07 rolling-drain smoke workload.
type Profile struct {
	ProfileID                     string  `json:"profileId"`
	Purpose                       string  `json:"purpose"`
	Seed                          uint64  `json:"seed"`
	PayloadBytes                  int     `json:"payloadBytes"`
	PublishRatePerStreamPerSecond uint64  `json:"publishRatePerStreamPerSecond"`
	MaxInflightPerPublisher       int     `json:"maxInflightPerPublisher"`
	AckTimeoutMillis              int64   `json:"ackTimeoutMillis"`
	SessionExpirySeconds          uint32  `json:"sessionExpirySeconds"`
	KeepAliveSeconds              uint16  `json:"keepAliveSeconds"`
	IdleClients                   int     `json:"idleClients"`
	WillDelaySeconds              uint32  `json:"willDelaySeconds"`
	WarmupMillis                  int64   `json:"warmupMillis"`
	ReplacementOrder              []int   `json:"replacementOrder"`
	Cycles                        int     `json:"cycles"`
	ConnectionRatePerSecond       int     `json:"connectionRatePerSecond"`
	LBPropagationMillis           int64   `json:"lbPropagationMillis"`
	DrainTimeoutMillis            int64   `json:"drainTimeoutMillis"`
	EMQXWaitTakeoverSeconds       int     `json:"emqxWaitTakeoverSeconds"`
	BrokerStopTimeoutSeconds      int     `json:"brokerStopTimeoutSeconds"`
	ReadyTimeoutMillis            int64   `json:"readyTimeoutMillis"`
	InterRoundSettleMillis        int64   `json:"interRoundSettleMillis"`
	WillSettleMillis              int64   `json:"willSettleMillis"`
	ReconnectTimeoutMillis        int64   `json:"reconnectTimeoutMillis"`
	HealthPollMillis              int64   `json:"healthPollMillis"`
	SettleMillis                  int64   `json:"settleMillis"`
	MaxMessages                   uint64  `json:"maxMessages"`
	BrokerCPUs                    float64 `json:"brokerCPUs"`
	BrokerMemoryBytes             int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs                 float64 `json:"generatorCPUs"`
	GeneratorMemoryBytes          int64   `json:"generatorMemoryBytes"`
}

// Validate checks workload bounds before Docker work or message allocation.
func (p Profile) Validate() error {
	if strings.TrimSpace(p.ProfileID) == "" {
		return errors.New("profileId is required")
	}
	if p.Purpose != purposeSmoke {
		return errors.New("purpose must be smoke")
	}
	if p.PayloadBytes < 16 || p.PayloadBytes > 65536 {
		return errors.New("payloadBytes must be between 16 and 65536")
	}
	if p.PublishRatePerStreamPerSecond < 1 || p.PublishRatePerStreamPerSecond > 100 {
		return errors.New("publishRatePerStreamPerSecond must be between 1 and 100")
	}
	if p.MaxInflightPerPublisher < 1 || p.MaxInflightPerPublisher > 32 {
		return errors.New("maxInflightPerPublisher must be between 1 and 32")
	}
	if p.AckTimeoutMillis < 1000 || p.AckTimeoutMillis > 60000 {
		return errors.New("ackTimeoutMillis must be between 1000 and 60000")
	}
	if p.SessionExpirySeconds < 60 || p.SessionExpirySeconds > 86400 {
		return errors.New("sessionExpirySeconds must be between 60 and 86400")
	}
	if p.KeepAliveSeconds < 5 || p.KeepAliveSeconds > 300 {
		return errors.New("keepAliveSeconds must be between 5 and 300")
	}
	if p.IdleClients < 0 || p.IdleClients > 8 {
		return errors.New("idleClients must be between 0 and 8")
	}
	if p.WillDelaySeconds < 1 || p.WillDelaySeconds > 3600 {
		return errors.New("willDelaySeconds must be between 1 and 3600")
	}
	if p.WarmupMillis < 1000 || p.WarmupMillis > 600000 {
		return errors.New("warmupMillis must be between 1000 and 600000")
	}
	if p.Cycles < 1 || p.Cycles > 10 {
		return errors.New("cycles must be between 1 and 10")
	}
	if p.ConnectionRatePerSecond < 1 || p.ConnectionRatePerSecond > 10000 {
		return errors.New("connectionRatePerSecond must be between 1 and 10000")
	}
	if p.LBPropagationMillis < 1000 || p.LBPropagationMillis > 3600000 {
		return errors.New("lbPropagationMillis must be between 1000 and 3600000")
	}
	if p.DrainTimeoutMillis < 1000 || p.DrainTimeoutMillis > 86400000 {
		return errors.New("drainTimeoutMillis must be between 1000 and 86400000")
	}
	if p.EMQXWaitTakeoverSeconds < 1 || p.EMQXWaitTakeoverSeconds > 600 {
		return errors.New("emqxWaitTakeoverSeconds must be between 1 and 600")
	}
	if p.BrokerStopTimeoutSeconds < 1 || p.BrokerStopTimeoutSeconds > 60 {
		return errors.New("brokerStopTimeoutSeconds must be between 1 and 60")
	}
	if p.ReadyTimeoutMillis < 1000 || p.ReadyTimeoutMillis > 600000 {
		return errors.New("readyTimeoutMillis must be between 1000 and 600000")
	}
	if p.InterRoundSettleMillis < 0 || p.InterRoundSettleMillis > 600000 {
		return errors.New("interRoundSettleMillis must be between 0 and 600000")
	}
	if p.WillSettleMillis < 0 || p.WillSettleMillis > 60000 {
		return errors.New("willSettleMillis must be between 0 and 60000")
	}
	if p.ReconnectTimeoutMillis < 1000 || p.ReconnectTimeoutMillis > 600000 {
		return errors.New("reconnectTimeoutMillis must be between 1000 and 600000")
	}
	if p.HealthPollMillis < 50 || p.HealthPollMillis > 5000 {
		return errors.New("healthPollMillis must be between 50 and 5000")
	}
	if p.SettleMillis < 1000 || p.SettleMillis > 600000 {
		return errors.New("settleMillis must be between 1000 and 600000")
	}
	if p.MaxMessages < 1 || p.MaxMessages > 1000000 {
		return errors.New("maxMessages must be between 1 and 1000000")
	}
	if p.LBPropagationMillis%1000 != 0 {
		return errors.New("lbPropagationMillis must be a multiple of 1000")
	}
	if p.DrainTimeoutMillis <= p.LBPropagationMillis {
		return errors.New("drainTimeoutMillis must exceed lbPropagationMillis")
	}
	if int64(p.WillDelaySeconds)*1000 <= p.ReconnectTimeoutMillis {
		return errors.New("will delay must exceed reconnect timeout")
	}
	if p.SessionExpirySeconds <= p.WillDelaySeconds {
		return errors.New("session expiry must exceed Will delay")
	}
	if p.HealthPollMillis*2 > p.LBPropagationMillis {
		return errors.New("health poll must be at most half the LB propagation delay")
	}
	if len(p.ReplacementOrder) != 3 {
		return errors.New("replacementOrder must be a permutation of 1,2,3")
	}
	var seen [4]bool
	for _, n := range p.ReplacementOrder {
		if n < 1 || n > 3 || seen[n] {
			return errors.New("replacementOrder must be a permutation of 1,2,3")
		}
		seen[n] = true
	}
	if p.MaxMessages < 4*p.PublishRatePerStreamPerSecond*uint64(p.WarmupMillis)/1000 {
		return errors.New("maxMessages is below the warmup capacity")
	}
	if !positiveFinite(p.BrokerCPUs) || !positiveFinite(p.GeneratorCPUs) || p.BrokerMemoryBytes <= 0 || p.GeneratorMemoryBytes <= 0 {
		return errors.New("resource limits must be positive finite values")
	}
	return nil
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
func (p Profile) rounds() int  { return len(p.ReplacementOrder) * p.Cycles }
func (p Profile) clients() int { return 11 + p.IdleClients }

type profileJSON struct {
	ProfileID                     *string  `json:"profileId"`
	Purpose                       *string  `json:"purpose"`
	Seed                          *uint64  `json:"seed"`
	PayloadBytes                  *int     `json:"payloadBytes"`
	PublishRatePerStreamPerSecond *uint64  `json:"publishRatePerStreamPerSecond"`
	MaxInflightPerPublisher       *int     `json:"maxInflightPerPublisher"`
	AckTimeoutMillis              *int64   `json:"ackTimeoutMillis"`
	SessionExpirySeconds          *uint32  `json:"sessionExpirySeconds"`
	KeepAliveSeconds              *uint16  `json:"keepAliveSeconds"`
	IdleClients                   *int     `json:"idleClients"`
	WillDelaySeconds              *uint32  `json:"willDelaySeconds"`
	WarmupMillis                  *int64   `json:"warmupMillis"`
	ReplacementOrder              *[]int   `json:"replacementOrder"`
	Cycles                        *int     `json:"cycles"`
	ConnectionRatePerSecond       *int     `json:"connectionRatePerSecond"`
	LBPropagationMillis           *int64   `json:"lbPropagationMillis"`
	DrainTimeoutMillis            *int64   `json:"drainTimeoutMillis"`
	EMQXWaitTakeoverSeconds       *int     `json:"emqxWaitTakeoverSeconds"`
	BrokerStopTimeoutSeconds      *int     `json:"brokerStopTimeoutSeconds"`
	ReadyTimeoutMillis            *int64   `json:"readyTimeoutMillis"`
	InterRoundSettleMillis        *int64   `json:"interRoundSettleMillis"`
	WillSettleMillis              *int64   `json:"willSettleMillis"`
	ReconnectTimeoutMillis        *int64   `json:"reconnectTimeoutMillis"`
	HealthPollMillis              *int64   `json:"healthPollMillis"`
	SettleMillis                  *int64   `json:"settleMillis"`
	MaxMessages                   *uint64  `json:"maxMessages"`
	BrokerCPUs                    *float64 `json:"brokerCPUs"`
	BrokerMemoryBytes             *int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs                 *float64 `json:"generatorCPUs"`
	GeneratorMemoryBytes          *int64   `json:"generatorMemoryBytes"`
}

// DecodeProfile requires every field and rejects unknown fields and trailing JSON.
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
			return Profile{}, errors.New("multiple JSON values")
		}
		return Profile{}, fmt.Errorf("decode profile trailer: %w", err)
	}
	if raw.ProfileID == nil {
		return Profile{}, errors.New("profile field profileId is required")
	}
	if raw.Purpose == nil {
		return Profile{}, errors.New("profile field purpose is required")
	}
	if raw.Seed == nil {
		return Profile{}, errors.New("profile field seed is required")
	}
	if raw.PayloadBytes == nil {
		return Profile{}, errors.New("profile field payloadBytes is required")
	}
	if raw.PublishRatePerStreamPerSecond == nil {
		return Profile{}, errors.New("profile field publishRatePerStreamPerSecond is required")
	}
	if raw.MaxInflightPerPublisher == nil {
		return Profile{}, errors.New("profile field maxInflightPerPublisher is required")
	}
	if raw.AckTimeoutMillis == nil {
		return Profile{}, errors.New("profile field ackTimeoutMillis is required")
	}
	if raw.SessionExpirySeconds == nil {
		return Profile{}, errors.New("profile field sessionExpirySeconds is required")
	}
	if raw.KeepAliveSeconds == nil {
		return Profile{}, errors.New("profile field keepAliveSeconds is required")
	}
	if raw.IdleClients == nil {
		return Profile{}, errors.New("profile field idleClients is required")
	}
	if raw.WillDelaySeconds == nil {
		return Profile{}, errors.New("profile field willDelaySeconds is required")
	}
	if raw.WarmupMillis == nil {
		return Profile{}, errors.New("profile field warmupMillis is required")
	}
	if raw.ReplacementOrder == nil {
		return Profile{}, errors.New("profile field replacementOrder is required")
	}
	if raw.Cycles == nil {
		return Profile{}, errors.New("profile field cycles is required")
	}
	if raw.ConnectionRatePerSecond == nil {
		return Profile{}, errors.New("profile field connectionRatePerSecond is required")
	}
	if raw.LBPropagationMillis == nil {
		return Profile{}, errors.New("profile field lbPropagationMillis is required")
	}
	if raw.DrainTimeoutMillis == nil {
		return Profile{}, errors.New("profile field drainTimeoutMillis is required")
	}
	if raw.EMQXWaitTakeoverSeconds == nil {
		return Profile{}, errors.New("profile field emqxWaitTakeoverSeconds is required")
	}
	if raw.BrokerStopTimeoutSeconds == nil {
		return Profile{}, errors.New("profile field brokerStopTimeoutSeconds is required")
	}
	if raw.ReadyTimeoutMillis == nil {
		return Profile{}, errors.New("profile field readyTimeoutMillis is required")
	}
	if raw.InterRoundSettleMillis == nil {
		return Profile{}, errors.New("profile field interRoundSettleMillis is required")
	}
	if raw.WillSettleMillis == nil {
		return Profile{}, errors.New("profile field willSettleMillis is required")
	}
	if raw.ReconnectTimeoutMillis == nil {
		return Profile{}, errors.New("profile field reconnectTimeoutMillis is required")
	}
	if raw.HealthPollMillis == nil {
		return Profile{}, errors.New("profile field healthPollMillis is required")
	}
	if raw.SettleMillis == nil {
		return Profile{}, errors.New("profile field settleMillis is required")
	}
	if raw.MaxMessages == nil {
		return Profile{}, errors.New("profile field maxMessages is required")
	}
	if raw.BrokerCPUs == nil {
		return Profile{}, errors.New("profile field brokerCPUs is required")
	}
	if raw.BrokerMemoryBytes == nil {
		return Profile{}, errors.New("profile field brokerMemoryBytes is required")
	}
	if raw.GeneratorCPUs == nil {
		return Profile{}, errors.New("profile field generatorCPUs is required")
	}
	if raw.GeneratorMemoryBytes == nil {
		return Profile{}, errors.New("profile field generatorMemoryBytes is required")
	}
	p := Profile{
		ProfileID:                     *raw.ProfileID,
		Purpose:                       *raw.Purpose,
		Seed:                          *raw.Seed,
		PayloadBytes:                  *raw.PayloadBytes,
		PublishRatePerStreamPerSecond: *raw.PublishRatePerStreamPerSecond,
		MaxInflightPerPublisher:       *raw.MaxInflightPerPublisher,
		AckTimeoutMillis:              *raw.AckTimeoutMillis,
		SessionExpirySeconds:          *raw.SessionExpirySeconds,
		KeepAliveSeconds:              *raw.KeepAliveSeconds,
		IdleClients:                   *raw.IdleClients,
		WillDelaySeconds:              *raw.WillDelaySeconds,
		WarmupMillis:                  *raw.WarmupMillis,
		ReplacementOrder:              *raw.ReplacementOrder,
		Cycles:                        *raw.Cycles,
		ConnectionRatePerSecond:       *raw.ConnectionRatePerSecond,
		LBPropagationMillis:           *raw.LBPropagationMillis,
		DrainTimeoutMillis:            *raw.DrainTimeoutMillis,
		EMQXWaitTakeoverSeconds:       *raw.EMQXWaitTakeoverSeconds,
		BrokerStopTimeoutSeconds:      *raw.BrokerStopTimeoutSeconds,
		ReadyTimeoutMillis:            *raw.ReadyTimeoutMillis,
		InterRoundSettleMillis:        *raw.InterRoundSettleMillis,
		WillSettleMillis:              *raw.WillSettleMillis,
		ReconnectTimeoutMillis:        *raw.ReconnectTimeoutMillis,
		HealthPollMillis:              *raw.HealthPollMillis,
		SettleMillis:                  *raw.SettleMillis,
		MaxMessages:                   *raw.MaxMessages,
		BrokerCPUs:                    *raw.BrokerCPUs,
		BrokerMemoryBytes:             *raw.BrokerMemoryBytes,
		GeneratorCPUs:                 *raw.GeneratorCPUs,
		GeneratorMemoryBytes:          *raw.GeneratorMemoryBytes,
	}
	if err := p.Validate(); err != nil {
		return Profile{}, fmt.Errorf("validate profile: %w", err)
	}
	return p, nil
}

// LoadProfile returns the validated profile, exact source bytes and their SHA-256.
func LoadProfile(path string) (Profile, []byte, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, nil, "", fmt.Errorf("read profile: %w", err)
	}
	p, err := DecodeProfile(data)
	if err != nil {
		return Profile{}, nil, "", err
	}
	digest := sha256.Sum256(data)
	return p, data, hex.EncodeToString(digest[:]), nil
}
