package metrics

import (
	"time"
)

// RaftStage identifies a fixed-cardinality driver timing boundary.
type RaftStage uint8

const (
	RaftInboundQueue RaftStage = iota
	RaftCommitQueue
	RaftReady
	RaftApply
	RaftSend
	RaftReadIndex
	RaftProposal
	raftStageCount
)

var raftStageNames = [raftStageCount]string{
	"inbound_queue", "commit_queue", "ready", "apply", "send", "read_index", "proposal",
}

func newRaftStageHistograms() [raftStageCount]*Histogram {
	var stages [raftStageCount]*Histogram
	buckets := []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10}
	for index := range stages {
		stages[index] = NewHistogram(buckets)
	}
	return stages
}

// ObserveRaftStage records a completed stage, including failed operations.
func (r *Registry) ObserveRaftStage(stage RaftStage, duration time.Duration) {
	if r == nil || stage >= raftStageCount {
		return
	}
	r.raftStages[stage].Observe(duration.Seconds())
}
