// Package connrebalance owns connection balancing and local shedding rules.
package connrebalance

import (
	"errors"

	"narumq/internal/domain/nodedrain"
)

// Phase identifies a replicated rebalance operation's phase.
type Phase uint32

const (
	PhaseUnspecified Phase = iota
	PhaseRunning
	PhaseCompleted
	PhaseFailed
	PhaseCancelled
)

// String returns the stable wire-facing name.
func (p Phase) String() string {
	switch p {
	case PhaseRunning:
		return "RUNNING"
	case PhaseCompleted:
		return "COMPLETED"
	case PhaseFailed:
		return "FAILED"
	case PhaseCancelled:
		return "CANCELLED"
	default:
		return "UNSPECIFIED"
	}
}

func (p Phase) Valid() bool    { return p >= PhaseRunning && p <= PhaseCancelled }
func (p Phase) Active() bool   { return p == PhaseRunning }
func (p Phase) Terminal() bool { return p >= PhaseCompleted && p <= PhaseCancelled }

const (
	MinMaxDeviationConnections uint64 = 1
	MaxMaxDeviationConnections uint64 = 1_000_000
	MaxReportedConnections     uint64 = 4_294_967_295
	ReportRefreshMillis        uint64 = 5_000
	ReportMaxAgeMillis         uint64 = 15_000
	SettleMillis               uint64 = 5_000
	FailureTimeout                    = "rebalance_timeout"
)

var errInvalidParameters = errors.New("invalid connection rebalance parameters")

// ValidateParameters validates the shared management and replicated parameters.
func ValidateParameters(serverReference string, maxDeviation uint64, rate uint32, lbDelayMillis, timeoutMillis uint64) error {
	if nodedrain.ValidateParameters(serverReference, rate, lbDelayMillis, timeoutMillis) != nil ||
		maxDeviation < MinMaxDeviationConnections || maxDeviation > MaxMaxDeviationConnections {
		return errInvalidParameters
	}
	return nil
}

// Report is a participant's latest committed connection measurement.
type Report struct {
	NodeID, Connections  uint64
	Shedding             bool
	ReportedAtUnixMillis uint64
}

// Evaluation describes the fair share, allowed ceiling, and convergence.
type Evaluation struct {
	Fresh, Converged          bool
	Total, FairShare, Ceiling uint64
}

// Evaluate uses proposer time, not a replica's local clock.
func Evaluate(reports []Report, maxDeviation, nowUnixMillis uint64) Evaluation {
	if len(reports) == 0 {
		return Evaluation{}
	}
	eval := Evaluation{Fresh: true}
	for _, report := range reports {
		eval.Total += report.Connections
		if report.ReportedAtUnixMillis == 0 ||
			(nowUnixMillis > report.ReportedAtUnixMillis && nowUnixMillis-report.ReportedAtUnixMillis > ReportMaxAgeMillis) {
			eval.Fresh = false
		}
	}
	n := uint64(len(reports))
	eval.FairShare = (eval.Total + n - 1) / n
	eval.Ceiling = eval.FairShare + maxDeviation
	eval.Converged = eval.Fresh
	for _, report := range reports {
		if report.Connections > eval.Ceiling || report.Shedding {
			eval.Converged = false
		}
	}
	return eval
}

// ShedStage is local execution progress, not replicated authority.
type ShedStage uint8

const (
	ShedIdle ShedStage = iota
	ShedWaitingForLB
	ShedTransferring
	ShedSettling
)

type ShedState struct {
	Stage                   ShedStage
	Target, UntilUnixMillis uint64
}

func (s ShedState) Shedding() bool { return s.Stage != ShedIdle }

// Step returns the next local state and whether to attempt one transfer this tick.
func Step(state ShedState, eval Evaluation, localConnections, nowUnixMillis, lbDelayMillis uint64) (ShedState, bool) {
	switch state.Stage {
	case ShedIdle:
		if eval.Fresh && localConnections > eval.Ceiling {
			return ShedState{Stage: ShedWaitingForLB, Target: eval.FairShare, UntilUnixMillis: nowUnixMillis + lbDelayMillis}, false
		}
	case ShedWaitingForLB:
		if nowUnixMillis >= state.UntilUnixMillis {
			state.Stage = ShedTransferring
		}
	case ShedTransferring:
		if localConnections <= state.Target {
			return ShedState{Stage: ShedSettling, Target: state.Target, UntilUnixMillis: nowUnixMillis + SettleMillis}, false
		}
		return state, true
	case ShedSettling:
		if nowUnixMillis >= state.UntilUnixMillis {
			return ShedState{}, false
		}
	}
	return state, false
}
