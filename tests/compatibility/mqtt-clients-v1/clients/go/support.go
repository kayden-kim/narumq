package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	go311Version    = "v1.5.1"
	go5Version      = "v0.23.0"
	resultSchema    = 1
	readinessSchema = 1
)

type profile struct {
	SchemaVersion          int           `json:"schemaVersion"`
	ConnectTimeoutMillis   int64         `json:"connectTimeoutMillis"`
	OperationTimeoutMillis int64         `json:"operationTimeoutMillis"`
	SessionExpirySeconds   int64         `json:"sessionExpirySeconds"`
	WillDelaySeconds       int64         `json:"willDelaySeconds"`
	ReconnectWithinMillis  int64         `json:"reconnectWithinMillis"`
	ReconnectBaseMillis    int64         `json:"reconnectBaseMillis"`
	ReconnectCapMillis     int64         `json:"reconnectCapMillis"`
	MaxReconnectAttempts   int           `json:"maxReconnectAttempts"`
	JitterSeed             int64         `json:"jitterSeed"`
	ExpectedCases          []profileCase `json:"expectedCases"`
}

type profileCase struct {
	CaseID          string        `json:"caseId"`
	Protocol        string        `json:"protocol"`
	Profile         string        `json:"profile"`
	Proxy           bool          `json:"proxy"`
	ExpectedResults []profileRole `json:"expectedResults"`
}

type profileRole struct {
	SDK                  string   `json:"sdk"`
	Role                 string   `json:"role"`
	RequiredObservations []string `json:"requiredObservations"`
}

type result struct {
	SchemaVersion int            `json:"schemaVersion"`
	CaseID        string         `json:"caseId"`
	SDK           string         `json:"sdk"`
	SDKVersion    string         `json:"sdkVersion"`
	Protocol      string         `json:"protocol"`
	Profile       string         `json:"profile"`
	Role          string         `json:"role"`
	Status        string         `json:"status"`
	StartedAt     string         `json:"startedAt"`
	FinishedAt    string         `json:"finishedAt"`
	Observations  map[string]any `json:"observations"`
	Error         string         `json:"error,omitempty"`
}

type fixture struct {
	CaseID    string
	ClientID  string
	Topic     string
	WillTopic string
	Payload   []byte
	Hash      string
}

type jitterRecord struct {
	Attempt           int
	UpperBoundMillis  int64
	ChosenDelayMillis int64
	DisconnectReason  string
	SessionPresent    bool
}

type traceEvent struct {
	Direction string `json:"direction"`
	Action    string `json:"action"`
	Metadata  struct {
		PacketType byte   `json:"packetType"`
		DUP        bool   `json:"dup"`
		QoS        byte   `json:"qos"`
		PacketID   uint16 `json:"packetId"`
	} `json:"metadata"`
}

func loadProfile(path string) (profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return profile{}, fmt.Errorf("read profile: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var value profile
	if err := decoder.Decode(&value); err != nil {
		return profile{}, fmt.Errorf("decode profile: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return profile{}, errors.New("profile contains a second JSON value")
		}
		return profile{}, fmt.Errorf("decode trailing profile data: %w", err)
	}
	if value.SchemaVersion != 1 || value.ConnectTimeoutMillis <= 0 || value.OperationTimeoutMillis <= 0 || value.MaxReconnectAttempts <= 0 {
		return profile{}, errors.New("profile has invalid timeout or attempt bounds")
	}
	return value, nil
}

func findCase(p profile, caseID, role string) (profileCase, profileRole, error) {
	for _, item := range p.ExpectedCases {
		if item.CaseID != caseID {
			continue
		}
		for _, expected := range item.ExpectedResults {
			if expected.SDK == "go" && expected.Role == role {
				return item, expected, nil
			}
		}
		return profileCase{}, profileRole{}, fmt.Errorf("role %q is not expected for case %q", role, caseID)
	}
	return profileCase{}, profileRole{}, fmt.Errorf("unknown case %q", caseID)
}

func validateOutput(path string) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("output path is required")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat output path: %w", err)
	}
	if !info.IsDir() {
		return errors.New("output path is not a directory")
	}
	if _, err := os.Stat(filepath.Join(path, "result.json")); err == nil {
		return errors.New("duplicate client invocation: result.json already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func makeFixture(caseID, role string) fixture {
	caseSeed := sha256.Sum256([]byte("narumq-compatibility/" + caseID))
	roleSeed := sha256.Sum256([]byte("narumq-compatibility/" + caseID + "/" + role))
	caseToken := hex.EncodeToString(caseSeed[:])
	roleToken := hex.EncodeToString(roleSeed[:])
	payload := []byte("mqtt-client-compatibility/" + caseToken[:24])
	hash := sha256.Sum256(payload)
	return fixture{
		CaseID: caseID, ClientID: "nq-" + roleToken[:12], Topic: "compat/" + caseToken[:12] + "/message", WillTopic: "compat/" + caseToken[:12] + "/will",
		Payload: payload, Hash: hex.EncodeToString(hash[:]),
	}
}

func (p profile) operationTimeout() time.Duration {
	return time.Duration(p.OperationTimeoutMillis) * time.Millisecond
}
func (p profile) connectTimeout() time.Duration {
	return time.Duration(p.ConnectTimeoutMillis) * time.Millisecond
}

func newJitter(p profile) func(int) jitterRecord {
	random := rand.New(rand.NewPCG(uint64(p.JitterSeed), uint64(p.JitterSeed)^0x9e3779b97f4a7c15))
	var mu sync.Mutex
	return func(attempt int) jitterRecord {
		if attempt < 0 {
			attempt = 0
		}
		upper := p.ReconnectBaseMillis
		for step := 0; step < attempt && upper < p.ReconnectCapMillis; step++ {
			if upper > p.ReconnectCapMillis/2 {
				upper = p.ReconnectCapMillis
				break
			}
			upper *= 2
		}
		if upper > p.ReconnectCapMillis {
			upper = p.ReconnectCapMillis
		}
		mu.Lock()
		chosen := random.Int64N(upper + 1)
		mu.Unlock()
		return jitterRecord{Attempt: attempt, UpperBoundMillis: upper, ChosenDelayMillis: chosen}
	}
}

type readinessSignal struct {
	SchemaVersion int    `json:"schemaVersion"`
	CaseID        string `json:"caseId"`
	SDK           string `json:"sdk"`
	Role          string `json:"role"`
	Protocol      string `json:"protocol"`
	Ready         bool   `json:"ready"`
}

func writeReady(output, caseID, sdk, protocol string) error {
	return writeJSONAtomic(filepath.Join(output, "ready.json"), readinessSignal{
		SchemaVersion: readinessSchema,
		CaseID:        caseID,
		SDK:           sdk,
		Role:          "subscriber",
		Protocol:      protocol,
		Ready:         true,
	})
}

func requireReadiness(item profileCase) error {
	data, err := os.ReadFile("/input/readiness.json")
	if err != nil {
		return fmt.Errorf("read publisher readiness input: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var readiness readinessSignal
	if err := decoder.Decode(&readiness); err != nil {
		return fmt.Errorf("decode publisher readiness input: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("publisher readiness input contains a second JSON value")
		}
		return fmt.Errorf("decode trailing publisher readiness input: %w", err)
	}
	subscriberSDK := ""
	for _, expected := range item.ExpectedResults {
		if expected.Role == "subscriber" {
			subscriberSDK = expected.SDK
			break
		}
	}
	if readiness.SchemaVersion != readinessSchema ||
		readiness.CaseID != item.CaseID ||
		readiness.SDK != subscriberSDK ||
		readiness.Role != "subscriber" ||
		readiness.Protocol != item.Protocol ||
		!readiness.Ready {
		return fmt.Errorf("publisher readiness input does not match case %q", item.CaseID)
	}
	return nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	file, err := os.CreateTemp(filepath.Dir(path), ".narumq-compat-readiness-*.tmp")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}

func writeResult(output string, value result) error {
	if _, err := os.Stat(filepath.Join(output, "result.json")); err == nil {
		return errors.New("result already exists")
	}
	return writeJSON(filepath.Join(output, "result.json"), value)
}

func newResult(caseID, protocol, profileID, role, started string) result {
	version := go311Version
	if protocol == "mqtt5" {
		version = go5Version
	}
	return result{SchemaVersion: resultSchema, CaseID: caseID, SDK: "go", SDKVersion: version, Protocol: protocol, Profile: profileID, Role: role, Status: "failed", StartedAt: started, Observations: map[string]any{}}
}

func expectedSDKVersion(protocol string) string {
	if protocol == "mqtt5" {
		return go5Version
	}
	return go311Version
}

func runDriver(options driverOptions, execute func(context.Context, profileCase, profileRole, fixture, profile, string) (map[string]any, error)) error {
	started := time.Now().UTC()
	value := newResult(options.caseID, options.protocol, options.profileID, options.role, started.Format(time.RFC3339Nano))
	if err := validateOutput(options.output); err != nil {
		return err
	}
	p, profileErr := loadProfile(options.profilePath)
	if profileErr != nil {
		value.Error = profileErr.Error()
		value.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		_ = writeResult(options.output, value)
		return profileErr
	}
	item, expected, err := findCase(p, options.caseID, options.role)
	if err != nil {
		value.Error = err.Error()
		value.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		_ = writeResult(options.output, value)
		return err
	}
	options.protocol = item.Protocol
	options.profileID = item.Profile
	value.Protocol = item.Protocol
	value.Profile = item.Profile
	value.SDKVersion = expectedSDKVersion(item.Protocol)
	if options.role != expected.Role {
		err = fmt.Errorf("unexpected role %q", options.role)
		value.Error = err.Error()
		value.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		_ = writeResult(options.output, value)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.operationTimeout())
	defer cancel()
	observations, runErr := execute(ctx, item, expected, makeFixture(options.caseID, options.role), p, options.address)
	value.Observations = observations
	value.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if runErr == nil {
		value.Status = "passed"
	} else {
		value.Error = runErr.Error()
	}
	writeErr := writeResult(options.output, value)
	if runErr != nil {
		if writeErr != nil {
			return errors.Join(runErr, writeErr)
		}
		return runErr
	}
	return writeErr
}

type driverOptions struct {
	caseID      string
	role        string
	address     string
	protocol    string
	profileID   string
	profilePath string
	output      string
}

func splitAddress(address string) (string, string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || port == "" {
		return "", "", fmt.Errorf("invalid broker address %q", address)
	}
	return host, port, nil
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func readProxyTrace(ctx context.Context, path string, expectedID uint16, qos byte) (bool, bool, error) {
	deadline := time.Now().Add(2 * time.Second)
	for {
		file, err := os.Open(path)
		if err == nil {
			defer file.Close()
			var firstID uint16
			var duplicate bool
			var dropped bool
			var completed bool
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				var event traceEvent
				if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
					return false, false, err
				}
				if event.Metadata.PacketType == 3 && event.Metadata.QoS == qos && event.Direction == "client_to_broker" {
					if firstID == 0 {
						firstID = event.Metadata.PacketID
					} else if event.Metadata.PacketID == firstID && event.Metadata.DUP {
						duplicate = true
					}
				}
				if event.Action == "drop_and_close" {
					dropped = true
				}
				if qos == 1 && event.Metadata.PacketType == 4 && event.Direction == "broker_to_client" && event.Action == "forward" && event.Metadata.PacketID == firstID {
					completed = true
				}
				if qos == 2 && event.Metadata.PacketType == 7 && event.Direction == "broker_to_client" && event.Metadata.PacketID == firstID {
					completed = true
				}
			}
			if err := scanner.Err(); err != nil {
				return false, false, err
			}
			if expectedID != 0 && firstID != expectedID {
				return false, false, fmt.Errorf("proxy packet identifier %d does not match SDK identifier %d", firstID, expectedID)
			}
			if dropped && duplicate && completed {
				return true, true, nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, false, err
		}
		if time.Now().After(deadline) {
			return false, false, errors.New("proxy trace did not record retransmission")
		}
		if err := waitContext(ctx, 50*time.Millisecond); err != nil {
			return false, false, err
		}
	}
}

func requirePayloadHash(payload []byte, expected string) bool {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]) == expected
}
