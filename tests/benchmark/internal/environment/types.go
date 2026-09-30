package environment

import (
	"context"
	"time"
)

const (
	EMQXImageReference       = "emqx/emqx:5.8.8@sha256:4593b6e30196754779fa14483332cb0be56c233f1c8a387b1308bec4337c0144"
	EMQXVersion              = "5.8.8"
	EMQXEdition              = "Open Source Edition"
	EMQXLicense              = "Apache-2.0"
	EMQXEOLEnd               = "2026-02-28"
	BuildBaseImageReference  = "golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd"
	PlannedBuildBaseImage    = "golang:1.26.5-bookworm@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251"
	DefaultStartupTimeout    = 30 * time.Second
	DefaultHandshakeTimeout  = 30 * time.Second
	DefaultStatsSamplePeriod = time.Second
)

// BuildOptions selects the benchmark-specific runner target and image tags.
type BuildOptions struct {
	BenchmarkID  string
	RunID        string
	RepoRoot     string
	ArtifactDir  string
	RunnerTarget string
}

// ImageSet contains the immutable image references and IDs produced by BuildImages.
type ImageSet struct {
	NaruMQReference string
	NaruMQID        string
	RunnerReference string
	RunnerID        string
	RunnerTarget    string
}

// TargetOptions describes one broker and runner execution under equal limits.
type TargetOptions struct {
	BenchmarkID            string
	RunID                  string
	Target                 string
	ArtifactDirectory      string
	ProfilePath            string
	ImageReference         string
	RunnerReference        string
	RunnerImageID          string
	RunnerTarget           string
	BrokerCPUs             float64
	BrokerMemoryBytes      int64
	GeneratorCPUs          float64
	GeneratorMemoryBytes   int64
	RunnerArgs             []string
	PreRunSamplingDuration time.Duration
	Timeout                time.Duration
	StartupTimeout         time.Duration
	CollectRuntimeMetrics  bool
	RuntimeMetricsInterval time.Duration
}

// TargetResult is the environment-owned part of one target artifact manifest.
type TargetResult struct {
	Target             string
	Status             string
	Success            bool
	StartedAt          string
	FinishedAt         string
	ArtifactDirectory  string
	SummaryPath        string
	SamplesPath        string
	RuntimeMetricsPath string
	BrokerStatsPath    string
	BrokerLogPath      string
	RunnerLogPath      string
	ContainerInspect   string
	ImageReference     string
	ImageID            string
	ImageRepoDigests   []string
	RunnerImageID      string
	RunnerTarget       string
	FailureReason      string
}

// StatsSample preserves the raw Docker stats object and adds parsed numbers.
type StatsSample struct {
	CollectedAt      string         `json:"collectedAt"`
	Container        string         `json:"container"`
	Docker           map[string]any `json:"docker"`
	CPUPercent       float64        `json:"cpuPercent"`
	MemoryUsageBytes int64          `json:"memoryUsageBytes"`
	MemoryLimitBytes int64          `json:"memoryLimitBytes"`
	NetworkRxBytes   int64          `json:"networkRxBytes"`
	NetworkTxBytes   int64          `json:"networkTxBytes"`
	BlockReadBytes   int64          `json:"blockReadBytes"`
	BlockWriteBytes  int64          `json:"blockWriteBytes"`
}

// BuildImages builds the broker image and the requested benchmark runner image.
func BuildImages(ctx context.Context, options BuildOptions) (ImageSet, error) {
	return buildImages(ctx, options)
}
