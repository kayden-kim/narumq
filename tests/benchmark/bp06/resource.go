package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"narumq/tests/benchmark/internal/environment"
)

type resourceSummary struct {
	SchemaVersion  int                    `json:"schemaVersion"`
	Target         string                 `json:"target"`
	Valid          bool                   `json:"valid"`
	FailureReasons []string               `json:"failureReasons"`
	Steady         resourceWindow         `json:"steady"`
	Rounds         []resourceRoundSummary `json:"rounds"`
	Network        networkSummary         `json:"network"`
}

type resourceWindow struct {
	Nodes []nodeResourceSummary `json:"nodes"`
}

type nodeResourceSummary struct {
	Name             string  `json:"name"`
	Samples          int     `json:"samples"`
	CPUPercentMedian float64 `json:"cpuPercentMedian"`
	CPUPercentMax    float64 `json:"cpuPercentMax"`
	MemoryBytesMax   int64   `json:"memoryBytesMax"`
	BlockWriteBytes  int64   `json:"blockWriteBytes"`
}

type resourceRoundSummary struct {
	Round                int                   `json:"round"`
	Node                 string                `json:"node"`
	Nodes                []nodeResourceSummary `json:"nodes"`
	CPUPeakPercent       float64               `json:"cpuPeakPercent"`
	MemoryPeakBytes      int64                 `json:"memoryPeakBytes"`
	CPUPeakDeltaPercent  float64               `json:"cpuPeakDeltaPercent"`
	MemoryPeakDeltaBytes int64                 `json:"memoryPeakDeltaBytes"`
}

type networkSummary struct {
	RxBytes               int64 `json:"rxBytes"`
	TxBytes               int64 `json:"txBytes"`
	NetworkStatsAvailable bool  `json:"networkStatsAvailable"`
}

type failureEventWindow struct {
	Round      int    `json:"round"`
	Node       string `json:"node"`
	Step       string `json:"step"`
	StartedAt  string `json:"startedAt"`
	FinishedAt string `json:"finishedAt"`
}

type resourceBounds struct{ start, end time.Time }
type timestampedResourceStat struct {
	sample environment.StatsSample
	at     time.Time
}
type parsedFailureRound struct {
	round           int
	node            string
	bounds          resourceBounds
	restartFinished time.Time
}

type failureWindows struct {
	steady *resourceBounds
	rounds []parsedFailureRound
}

func buildResourceSummary(target string, nodes []environment.ClusterNodeResult, failurePath string) (resourceSummary, error) {
	summary := resourceSummary{
		SchemaVersion:  1,
		Target:         target,
		Valid:          true,
		FailureReasons: []string{},
		Steady:         resourceWindow{Nodes: []nodeResourceSummary{}},
		Rounds:         []resourceRoundSummary{},
	}
	windows, err := readFailureWindows(failurePath)
	if err != nil {
		addResourceFailure(&summary, err.Error())
		return summary, nil
	}
	if len(nodes) != 3 {
		addResourceFailure(&summary, fmt.Sprintf("failure resource summary requires three nodes, got %d", len(nodes)))
	}
	if windows.steady == nil {
		addResourceFailure(&summary, "missing or empty steady failure window")
	}
	if len(windows.rounds) == 0 {
		addResourceFailure(&summary, "failure events contain no complete round windows")
	}

	brokerStats := make(map[string][]environment.StatsSample, len(nodes))
	networkStats := make(map[string][]environment.StatsSample, len(nodes))
	for _, node := range nodes {
		broker, readErr := environment.ReadStats(node.BrokerStatsPath)
		if readErr != nil {
			addResourceFailure(&summary, fmt.Sprintf("%s broker stats: %v", node.Name, readErr))
		}
		brokerStats[node.Name] = broker
		network, readErr := environment.ReadStats(node.NetworkStatsPath)
		if readErr != nil {
			addResourceFailure(&summary, fmt.Sprintf("%s network stats: %v", node.Name, readErr))
		}
		networkStats[node.Name] = network
	}

	if windows.steady != nil {
		for _, node := range nodes {
			samples := selectResourceStats(brokerStats[node.Name], windows.steady.start, windows.steady.end, true)
			entry := summarizeResourceNode(node.Name, samples)
			summary.Steady.Nodes = append(summary.Steady.Nodes, entry)
			if entry.Samples < 2 {
				addResourceFailure(&summary, fmt.Sprintf("steady/%s has fewer than two running broker stats samples", node.Name))
			}
		}
	}
	steadyByNode := make(map[string]nodeResourceSummary, len(summary.Steady.Nodes))
	for _, node := range summary.Steady.Nodes {
		steadyByNode[node.Name] = node
	}
	for _, window := range windows.rounds {
		round := resourceRoundSummary{Round: window.round, Node: window.node, Nodes: []nodeResourceSummary{}}
		targetSamples := 0
		cpuPeakNode := ""
		memoryPeakNode := ""
		for _, node := range nodes {
			samples := selectResourceStats(brokerStats[node.Name], window.bounds.start, window.bounds.end, true)
			entry := summarizeResourceNode(node.Name, samples)
			round.Nodes = append(round.Nodes, entry)
			if node.Name != window.node && entry.Samples < 2 {
				addResourceFailure(&summary, fmt.Sprintf("round %02d/%s has fewer than two running broker stats samples", window.round, node.Name))
			}
			if node.Name == window.node && !window.restartFinished.IsZero() {
				targetSamples = len(selectResourceStats(brokerStats[node.Name], window.restartFinished, window.bounds.end, true))
			}
			if entry.CPUPercentMax > round.CPUPeakPercent || cpuPeakNode == "" {
				round.CPUPeakPercent, cpuPeakNode = entry.CPUPercentMax, node.Name
			}
			if entry.MemoryBytesMax > round.MemoryPeakBytes || memoryPeakNode == "" {
				round.MemoryPeakBytes, memoryPeakNode = entry.MemoryBytesMax, node.Name
			}
		}
		if targetSamples < 1 {
			addResourceFailure(&summary, fmt.Sprintf("round %02d has no running target sample after restart", window.round))
		}
		cpuBaseline := steadyByNode[cpuPeakNode]
		memoryBaseline := steadyByNode[memoryPeakNode]
		round.CPUPeakDeltaPercent = round.CPUPeakPercent - cpuBaseline.CPUPercentMedian
		round.MemoryPeakDeltaBytes = round.MemoryPeakBytes - memoryBaseline.MemoryBytesMax
		summary.Rounds = append(summary.Rounds, round)
	}

	if len(windows.rounds) > 0 {
		start := windows.rounds[0].bounds.start
		end := windows.rounds[len(windows.rounds)-1].bounds.end
		for _, node := range nodes {
			samples := selectResourceStats(networkStats[node.Name], start, end, false)
			if len(samples) < 2 {
				continue
			}
			first, last := samples[0].sample, samples[len(samples)-1].sample
			if last.NetworkRxBytes >= first.NetworkRxBytes {
				summary.Network.RxBytes += last.NetworkRxBytes - first.NetworkRxBytes
			}
			if last.NetworkTxBytes >= first.NetworkTxBytes {
				summary.Network.TxBytes += last.NetworkTxBytes - first.NetworkTxBytes
			}
		}
	}
	summary.Network.NetworkStatsAvailable = summary.Network.RxBytes != 0 || summary.Network.TxBytes != 0
	return summary, nil
}

func writeResourceSummary(artifactDir string, summary resourceSummary) error {
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(artifactDir, "resource-summary.json"), append(data, '\n'), 0o644)
}

func readFailureWindows(path string) (failureWindows, error) {
	file, err := os.Open(path)
	if err != nil {
		return failureWindows{}, fmt.Errorf("open failure events %q: %w", path, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1<<20)
	var windows failureWindows
	starts := make(map[int]time.Time)
	rounds := make(map[int]*parsedFailureRound)
	for line := 1; scanner.Scan(); line++ {
		var event failureEventWindow
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return failureWindows{}, fmt.Errorf("decode failure event line %d: %w", line, err)
		}
		start, err := time.Parse(time.RFC3339Nano, event.StartedAt)
		if err != nil {
			return failureWindows{}, fmt.Errorf("parse failure event start line %d: %w", line, err)
		}
		end, err := time.Parse(time.RFC3339Nano, event.FinishedAt)
		if err != nil {
			return failureWindows{}, fmt.Errorf("parse failure event finish line %d: %w", line, err)
		}
		if end.Before(start) {
			return failureWindows{}, fmt.Errorf("failure event line %d ends before it starts", line)
		}
		if event.Step == "steady" {
			if !end.After(start) {
				return failureWindows{}, fmt.Errorf("failure event line %d has empty steady window", line)
			}
			windows.steady = &resourceBounds{start: start, end: end}
			continue
		}
		if event.Round < 1 {
			continue
		}
		round := rounds[event.Round]
		if round == nil {
			round = &parsedFailureRound{round: event.Round, node: event.Node}
			rounds[event.Round] = round
		} else if round.node != event.Node {
			return failureWindows{}, fmt.Errorf("round %02d has inconsistent node", event.Round)
		}
		switch event.Step {
		case "kill":
			starts[event.Round] = start
		case "restart":
			round.restartFinished = end
		case "settle":
			if killStart, ok := starts[event.Round]; ok {
				round.bounds = resourceBounds{start: killStart, end: end}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return failureWindows{}, fmt.Errorf("read failure events %q: %w", path, err)
	}
	for round, event := range rounds {
		if event.bounds.start.IsZero() || event.bounds.end.IsZero() || !event.bounds.end.After(event.bounds.start) {
			return failureWindows{}, fmt.Errorf("round %02d has an incomplete or empty failure resource window", round)
		}
		if event.restartFinished.IsZero() || event.restartFinished.Before(event.bounds.start) || event.restartFinished.After(event.bounds.end) {
			return failureWindows{}, fmt.Errorf("round %02d has no valid restart completion timestamp", round)
		}
		windows.rounds = append(windows.rounds, *event)
	}
	sort.Slice(windows.rounds, func(i, j int) bool { return windows.rounds[i].round < windows.rounds[j].round })
	return windows, nil
}

func selectResourceStats(samples []environment.StatsSample, start, end time.Time, runningOnly bool) []timestampedResourceStat {
	selected := make([]timestampedResourceStat, 0, len(samples))
	for _, sample := range samples {
		at, err := time.Parse(time.RFC3339Nano, sample.CollectedAt)
		if err != nil || at.Before(start) || at.After(end) || (runningOnly && sample.MemoryUsageBytes <= 0) {
			continue
		}
		selected = append(selected, timestampedResourceStat{sample: sample, at: at})
	}
	sort.SliceStable(selected, func(i, j int) bool { return selected[i].at.Before(selected[j].at) })
	return selected
}

func summarizeResourceNode(name string, samples []timestampedResourceStat) nodeResourceSummary {
	entry := nodeResourceSummary{Name: name, Samples: len(samples)}
	if len(samples) == 0 {
		return entry
	}
	cpu := make([]float64, len(samples))
	for i, sample := range samples {
		cpu[i] = sample.sample.CPUPercent
		if sample.sample.MemoryUsageBytes > entry.MemoryBytesMax {
			entry.MemoryBytesMax = sample.sample.MemoryUsageBytes
		}
		if sample.sample.CPUPercent > entry.CPUPercentMax {
			entry.CPUPercentMax = sample.sample.CPUPercent
		}
	}
	sort.Float64s(cpu)
	middle := len(cpu) / 2
	if len(cpu)%2 == 1 {
		entry.CPUPercentMedian = cpu[middle]
	} else {
		entry.CPUPercentMedian = (cpu[middle-1] + cpu[middle]) / 2
	}
	entry.BlockWriteBytes = resetAwareBlockWriteBytes(samples)
	return entry
}

func resetAwareBlockWriteBytes(samples []timestampedResourceStat) int64 {
	var total int64
	for i := 1; i < len(samples); i++ {
		previous, current := samples[i-1].sample.BlockWriteBytes, samples[i].sample.BlockWriteBytes
		if current >= previous {
			total += current - previous
		} else {
			total += current
		}
	}
	return total
}

func addResourceFailure(summary *resourceSummary, reason string) {
	summary.Valid = false
	summary.FailureReasons = append(summary.FailureReasons, reason)
}
