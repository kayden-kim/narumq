// Package nodedrain owns node drain lifecycle rules and safe-to-stop barrier decisions.
package nodedrain

import (
	"errors"
	"math"
	"unicode/utf8"
)

// Phase identifies the committed phase of a node drain operation.
type Phase uint8

const (
	// PhaseUnspecified is the zero value used outside an active drain transition.
	PhaseUnspecified Phase = iota
	PhaseExcludingTraffic
	PhaseWaitingForLB
	PhaseVerifyingSessionState
	PhaseTransferringSessions
	PhaseVerifyingBarrier
	PhaseCompleted
	PhaseFailed
	PhaseCancelled
)

// String returns the stable wire-facing name of p.
func (p Phase) String() string {
	switch p {
	case PhaseExcludingTraffic:
		return "EXCLUDING_TRAFFIC"
	case PhaseWaitingForLB:
		return "WAITING_FOR_LB"
	case PhaseVerifyingSessionState:
		return "VERIFYING_SESSION_STATE"
	case PhaseTransferringSessions:
		return "TRANSFERRING_SESSIONS"
	case PhaseVerifyingBarrier:
		return "VERIFYING_BARRIER"
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

// Valid reports whether p is a declared nonzero phase.
func (p Phase) Valid() bool { return p >= PhaseExcludingTraffic && p <= PhaseCancelled }

// Terminal reports whether p ends active execution.
func (p Phase) Terminal() bool {
	return p == PhaseCompleted || p == PhaseFailed || p == PhaseCancelled
}

// Active reports whether a drain record closes new connection admission.
func (p Phase) Active() bool { return p.Valid() && p != PhaseCancelled }

// Lifecycle is the node lifecycle state projected from a drain phase.
type Lifecycle string

const (
	// LifecycleDraining means the node is not ready and drain work is incomplete or failed.
	LifecycleDraining Lifecycle = "DRAINING"
	// LifecycleSafeToStop means the drain barrier committed successfully.
	LifecycleSafeToStop Lifecycle = "SAFE_TO_STOP"
	// LifecycleReady means the drain was cancelled.
	LifecycleReady Lifecycle = "READY"
)

// LifecycleOf maps a drain phase to the node lifecycle it exposes.
func LifecycleOf(p Phase) Lifecycle {
	switch p {
	case PhaseCompleted:
		return LifecycleSafeToStop
	case PhaseCancelled:
		return LifecycleReady
	default:
		return LifecycleDraining
	}
}

// CanAdvance reports whether to is the same or immediately following active phase.
func CanAdvance(from, to Phase) bool {
	return !from.Terminal() && (to == from || (from < PhaseVerifyingBarrier && to == from+1)) &&
		to <= PhaseVerifyingBarrier
}

// CanComplete reports whether from is the phase immediately before completion.
func CanComplete(from Phase) bool { return from == PhaseVerifyingBarrier }

// CanFail reports whether an operation in from can transition to failed.
func CanFail(from Phase) bool { return !from.Terminal() }

// CanCancel reports whether an operation in from can be cancelled.
func CanCancel(from Phase) bool { return from != PhaseCompleted }

// CanClear reports whether a completed drain record can be removed.
func CanClear(from Phase) bool { return from == PhaseCompleted }

const (
	// DefaultConnectionRatePerSecond is the product transfer rate applied when a drain request omits it (ADR-0055).
	DefaultConnectionRatePerSecond uint32 = 100
	// DefaultLoadBalancerPropagationDelayMillis is the product LB wait applied when a drain request omits it (ADR-0055).
	DefaultLoadBalancerPropagationDelayMillis uint64 = 30_000
	// DefaultTimeoutMillis is the product drain timeout applied when a drain request omits it (ADR-0055).
	DefaultTimeoutMillis uint64 = 600_000
	// MinConnectionRatePerSecond is the lowest accepted session transfer rate.
	MinConnectionRatePerSecond uint32 = 1
	// MaxConnectionRatePerSecond is the highest accepted session transfer rate.
	MaxConnectionRatePerSecond uint32 = 10000
	// MaxLoadBalancerPropagationDelayMillis bounds propagation wait configuration.
	MaxLoadBalancerPropagationDelayMillis uint64 = 3_600_000
	// MinTimeoutMillis is the minimum drain operation timeout.
	MinTimeoutMillis uint64 = 1000
	// MaxTimeoutMillis bounds the drain operation timeout.
	MaxTimeoutMillis uint64 = 86_400_000
	// MaxServerReferenceBytes bounds MQTT Server Reference UTF-8 bytes.
	MaxServerReferenceBytes = 256
)

var errInvalidParameters = errors.New("invalid node drain parameters")

// ValidateParameters validates the request values shared by replicated drain commands.
func ValidateParameters(serverReference string, rate uint32, lbDelayMillis, timeoutMillis uint64) error {
	if rate < MinConnectionRatePerSecond || rate > MaxConnectionRatePerSecond ||
		lbDelayMillis > MaxLoadBalancerPropagationDelayMillis ||
		timeoutMillis < MinTimeoutMillis || timeoutMillis > MaxTimeoutMillis ||
		timeoutMillis <= lbDelayMillis || len(serverReference) > MaxServerReferenceBytes ||
		!utf8.ValidString(serverReference) {
		return errInvalidParameters
	}
	for _, r := range serverReference {
		if r <= 0x1f || r == 0x7f {
			return errInvalidParameters
		}
	}
	return nil
}

// Blocker identifies the first failed safe-to-stop barrier condition.
type Blocker string

const (
	BlockerAdmissionOpen           Blocker = "admission_open"
	BlockerReadinessUp             Blocker = "readiness_up"
	BlockerConnectionsRemaining    Blocker = "connections_remaining"
	BlockerSessionsOwned           Blocker = "sessions_owned"
	BlockerOrphanSubscription      Blocker = "orphan_subscription"
	BlockerPendingWAL              Blocker = "pending_wal"
	BlockerInsufficientReplicas    Blocker = "insufficient_replicas"
	BlockerLocalLeader             Blocker = "local_leader"
	BlockerPendingLocalCommit      Blocker = "pending_local_commit"
	BlockerPendingWill             Blocker = "pending_will"
	BlockerNoSuccessorVoter        Blocker = "no_successor_voter"
	BlockerQuorumUnavailable       Blocker = "quorum_unavailable"
	BlockerRouteProjectionNotReady Blocker = "route_projection_not_ready"
)

// BarrierInput contains the local and replicated facts needed to evaluate shutdown safety.
type BarrierInput struct {
	AdmissionClosed, ReadinessFailing                    bool
	ActiveConnections, OwnedSessions, OwnedSubscriptions int
	PendingWAL                                           uint64
	VoterCount, ConnectedOtherVoters                     int
	LocalLeader                                          bool
	PendingLocalCommits, PendingWills                    int
}

// EvaluateBarrier returns the first blocker in the stable evaluation order.
func EvaluateBarrier(in BarrierInput) (Blocker, bool) {
	if !in.AdmissionClosed {
		return BlockerAdmissionOpen, false
	}
	if !in.ReadinessFailing {
		return BlockerReadinessUp, false
	}
	if in.ActiveConnections != 0 {
		return BlockerConnectionsRemaining, false
	}
	if in.OwnedSessions != 0 {
		return BlockerSessionsOwned, false
	}
	if in.OwnedSubscriptions != 0 {
		return BlockerOrphanSubscription, false
	}
	if in.PendingWAL != 0 {
		return BlockerPendingWAL, false
	}
	if in.ConnectedOtherVoters < in.VoterCount/2+1 {
		return BlockerInsufficientReplicas, false
	}
	if in.LocalLeader {
		return BlockerLocalLeader, false
	}
	if in.PendingLocalCommits != 0 {
		return BlockerPendingLocalCommit, false
	}
	if in.PendingWills != 0 {
		return BlockerPendingWill, false
	}
	return "", true
}

// UnderReplicated returns the number of voters other than local that are disconnected.
func UnderReplicated(voterCount, connectedOtherVoters int) uint64 {
	if voterCount <= 1 {
		return 0
	}
	remaining := uint64(voterCount - 1)
	if connectedOtherVoters >= 0 {
		connected := uint64(connectedOtherVoters)
		if connected >= remaining {
			return 0
		}
		return remaining - connected
	}
	missing := uint64(-(int64(connectedOtherVoters) + 1)) + 1
	if remaining > math.MaxUint64-missing {
		return math.MaxUint64
	}
	return remaining + missing
}
