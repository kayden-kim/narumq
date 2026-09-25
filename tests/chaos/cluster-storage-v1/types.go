package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

const (
	chaosImageName      = "narumq-chaos-node"
	chaosImageTag       = "storage-faults"
	chaosNodeCount      = 3
	maxOutboundMessages = 32
	maxOutboundBytes    = 4_194_304
)

var storageFaultScenarios = []string{
	"follower-disk-full",
	"quorum-disk-full",
	"follower-slow-io",
}

type runOptions struct {
	RunID     string
	OutputDir string
	RepoRoot  string
	Runner    commandRunner
}

type Summary struct {
	RunID      string            `json:"runId"`
	Status     string            `json:"status"`
	StartedAt  time.Time         `json:"startedAt"`
	FinishedAt time.Time         `json:"finishedAt"`
	Scenarios  []scenarioSummary `json:"scenarios"`
	Error      string            `json:"error,omitempty"`
}

type scenarioSummary struct {
	ID       string           `json:"id"`
	Status   string           `json:"status"`
	Error    string           `json:"error,omitempty"`
	Evidence scenarioEvidence `json:"evidence"`
}

type scenarioEvidence struct {
	LeaderNodeID                      string                      `json:"leaderNodeId,omitempty"`
	TargetNodeIDs                     []string                    `json:"targetNodeIds,omitempty"`
	DiskFill                          *diskFillEvidence           `json:"diskFill,omitempty"`
	DiskFills                         map[string]diskFillEvidence `json:"diskFills,omitempty"`
	DetectionDurationMs               int64                       `json:"detectionDurationMs,omitempty"`
	TargetReadiness                   *healthObservation          `json:"targetReadiness,omitempty"`
	TargetLiveness                    *healthObservation          `json:"targetLiveness,omitempty"`
	LeaderReadiness                   *healthObservation          `json:"leaderReadiness,omitempty"`
	TargetReplicationHealth           string                      `json:"targetReplicationHealth,omitempty"`
	TargetLastErrorCode               string                      `json:"targetLastErrorCode,omitempty"`
	TargetStoragePressure             *bool                       `json:"targetStoragePressure,omitempty"`
	ProgressAccepted                  int                         `json:"progressAccepted,omitempty"`
	ProgressAttempts                  int                         `json:"progressAttempts,omitempty"`
	MaxLeaderProbeLatencyMs           int64                       `json:"maxLeaderProbeLatencyMs,omitempty"`
	QueueSamples                      int                         `json:"queueSamples,omitempty"`
	MaxOutboundMessages               int                         `json:"maxOutboundMessages,omitempty"`
	MaxOutboundBytes                  int                         `json:"maxOutboundBytes,omitempty"`
	MaxRejectedMessages               uint64                      `json:"maxRejectedMessages,omitempty"`
	QueueCapReached                   bool                        `json:"queueCapReached,omitempty"`
	LeaderUnreachableTarget           bool                        `json:"leaderUnreachableTarget,omitempty"`
	LeaderAppliedIndexStart           int64                       `json:"leaderAppliedIndexStart,omitempty"`
	LeaderAppliedIndexEnd             int64                       `json:"leaderAppliedIndexEnd,omitempty"`
	FollowerAppliedIndexEnd           int64                       `json:"followerAppliedIndexEnd,omitempty"`
	HaltProbeAttempts                 int                         `json:"haltProbeAttempts,omitempty"`
	HaltAccepted                      int                         `json:"haltAccepted,omitempty"`
	QuorumAvailableDuring             *bool                       `json:"quorumAvailableDuring,omitempty"`
	HaltDurationMs                    int64                       `json:"haltDurationMs,omitempty"`
	AppliedIndexEnd                   int64                       `json:"appliedIndexEnd,omitempty"`
	RecoveryDurationMs                int64                       `json:"recoveryDurationMs,omitempty"`
	ConvergedAppliedIndex             int64                       `json:"convergedAppliedIndex,omitempty"`
	UptimeBeforeRecoveryMs            int64                       `json:"uptimeBeforeRecoveryMs,omitempty"`
	UptimeAfterRecoveryMs             int64                       `json:"uptimeAfterRecoveryMs,omitempty"`
	IOProbeDurationMs                 int64                       `json:"ioProbeDurationMs,omitempty"`
	OriginalBaselineReconnectAccepted bool                        `json:"originalBaselineReconnectAccepted"`
}

type diskFillEvidence struct {
	BytesWritten   uint64 `json:"bytesWritten"`
	FreeBytesAfter uint64 `json:"freeBytesAfter"`
	StoppedBy      string `json:"stoppedBy"`
}

type manifest struct {
	SchemaVersion int                      `json:"schemaVersion"`
	RunID         string                   `json:"runId"`
	Status        string                   `json:"status"`
	StartedAt     time.Time                `json:"startedAt"`
	FinishedAt    time.Time                `json:"finishedAt,omitempty"`
	Platform      platformMetadata         `json:"platform"`
	Images        map[string]imageMetadata `json:"images,omitempty"`
	Command       []string                 `json:"command"`
	Error         string                   `json:"error,omitempty"`
}

type platformMetadata struct {
	HostOS     string `json:"hostOs"`
	HostArch   string `json:"hostArch"`
	DockerOS   string `json:"dockerOs,omitempty"`
	DockerArch string `json:"dockerArch,omitempty"`
	ImageOS    string `json:"imageOs,omitempty"`
	ImageArch  string `json:"imageArch,omitempty"`
}

type imageMetadata struct {
	ID           string   `json:"id"`
	OS           string   `json:"os"`
	Architecture string   `json:"architecture"`
	RepoDigests  []string `json:"repoDigests,omitempty"`
}

type scenarioEvent struct {
	At     time.Time `json:"at"`
	Kind   string    `json:"kind"`
	NodeID string    `json:"nodeId,omitempty"`
	Data   any       `json:"data"`
}

type eventRecorder struct {
	file    *os.File
	encoder *json.Encoder
}

func newEventRecorder(directory string) (*eventRecorder, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(directory, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &eventRecorder{file: file, encoder: json.NewEncoder(file)}, nil
}

func (recorder *eventRecorder) record(kind, nodeID string, data any) error {
	return recorder.encoder.Encode(scenarioEvent{At: time.Now().UTC(), Kind: kind, NodeID: nodeID, Data: data})
}

func (recorder *eventRecorder) close() error {
	if recorder == nil || recorder.file == nil {
		return nil
	}
	syncErr := recorder.file.Sync()
	closeErr := recorder.file.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
