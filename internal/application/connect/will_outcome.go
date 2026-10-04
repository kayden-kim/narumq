package connect

import (
	"narumq/internal/application/membership"
	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/security"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type willOrigin uint8

const (
	willOriginUnplanned willOrigin = iota
	willOriginPlannedHandoff
	willOriginClientRequested
)

func willOriginFor(reason session.DisconnectReason) willOrigin {
	switch reason {
	case session.DisconnectPlannedHandoff:
		return willOriginPlannedHandoff
	case session.DisconnectWithWill:
		return willOriginClientRequested
	default:
		return willOriginUnplanned
	}
}

func plannedWillPublishesImmediately(version mqtt.ProtocolVersion, delaySeconds uint32, immediateRetention bool) bool {
	return version == mqtt.ProtocolV311 || delaySeconds == 0 || immediateRetention
}

func plannedImmediateWillReason(version mqtt.ProtocolVersion, delaySeconds uint32, immediateRetention bool) string {
	if version == mqtt.ProtocolV311 {
		return portsmetrics.WillOutcomeExpectedMQTT311ServerDisconnect
	}
	if delaySeconds == 0 || immediateRetention {
		return portsmetrics.WillOutcomeExpectedZeroOrExpiredWillDelay
	}
	return portsmetrics.WillOutcomeUnexpectedSupportedClient
}

func immediateWillReason(origin willOrigin, version mqtt.ProtocolVersion, delaySeconds uint32, immediateRetention bool) string {
	switch origin {
	case willOriginPlannedHandoff:
		return plannedImmediateWillReason(version, delaySeconds, immediateRetention)
	case willOriginClientRequested:
		return ""
	default:
		return portsmetrics.WillOutcomeUnplannedFailure
	}
}

func delayedWillReason(origin willOrigin) string {
	switch origin {
	case willOriginPlannedHandoff:
		return portsmetrics.WillOutcomeExpectedZeroOrExpiredWillDelay
	case willOriginClientRequested:
		return ""
	default:
		return portsmetrics.WillOutcomeUnplannedFailure
	}
}

// WithWillOutcomeRecorder records Will outcomes without enabling other connect metrics.
func WithWillOutcomeRecorder(recorder portsmetrics.WillOutcomeRecorder) Option {
	return func(useCase *UseCase) { useCase.willOutcomes = recorder }
}

func (useCase *UseCase) recordWillOutcome(reason string) {
	if useCase != nil && useCase.willOutcomes != nil && reason != "" {
		useCase.willOutcomes.WillOutcome(reason)
	}
}

func (useCase *UseCase) resolveSupersededWillLocked(key tenancy.SessionKey, superseded *membership.SupersededWillDTO) *mqtt.Will {
	version := mqtt.ProtocolVersion(superseded.ProtocolVersion)
	delay := superseded.Will.DelayIntervalSeconds
	immediateRetention := superseded.Retention == membership.SessionRetentionImmediate
	immediate := plannedWillPublishesImmediately(version, delay, immediateRetention)
	if !superseded.PlannedHandoff || !immediate {
		useCase.recordWillOutcome(portsmetrics.WillOutcomeSuppressedReconnectedWithinDelay)
		return nil
	}
	will := sessionWillFromDTO(&superseded.Will)
	if useCase.authenticator != nil {
		principal, ok := restorePrincipal(key.TenantID(), key.ClientID(), superseded.Principal)
		if !ok {
			return nil
		}
		topic, err := tenancy.NewTopic(principal.TenantID(), will.Topic)
		if err != nil || useCase.policy.AuthorizeTopic(principal, security.ActionWill, topic) != nil {
			return nil
		}
	}
	useCase.recordWillOutcome(plannedImmediateWillReason(version, delay, immediateRetention))
	return will
}
