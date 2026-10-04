package connect

import (
	"context"
	"errors"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/security"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

// SessionOwnershipCoordinator installs CONNECT metadata with committed ownership.
type SessionOwnershipCoordinator interface {
	AcquireSessionOwnership(ctx context.Context, request SessionOwnershipRequestDTO) (SessionOwnershipResultDTO, error)
}

// SessionOwnershipRequestDTO carries the lifecycle for an ownership acquisition.
type SessionOwnershipRequestDTO struct {
	TenantID, ClientID string
	OwnerNodeID        uint64
	Session            membership.SessionConnectDTO
}

// SessionOwnershipResultDTO carries committed ownership and any replaced pending Will.
type SessionOwnershipResultDTO struct {
	Epoch          uint64
	SupersededWill *membership.SupersededWillDTO
}

// SessionMetadataCoordinator fences and commits lifecycle and Will decisions.
type SessionMetadataCoordinator interface {
	CommitSessionMetadata(ctx context.Context, change membership.SessionMetadataChangeDTO) (SessionMetadataOutcomeDTO, error)
}

// SessionMetadataOutcomeDTO distinguishes committed Will decisions from local fallback.
type SessionMetadataOutcomeDTO struct {
	Replicated bool
	Will       *mqtt.Will
}

// ErrSessionOwnershipStale prevents cleanup by a fenced local owner.
var ErrSessionOwnershipStale = errors.New("connect session ownership is stale")

const sessionMetadataCommitTimeout = 5 * time.Second

// WithSessionMetadataCoordinator configures replicated lifecycle cleanup.
func WithSessionMetadataCoordinator(coordinator SessionMetadataCoordinator) Option {
	return func(useCase *UseCase) { useCase.sessionMetadataCoordinator = coordinator }
}

// RecoveredSessionDTO supplies committed metadata and its current ownership epoch.
type RecoveredSessionDTO struct {
	Metadata       membership.SessionMetadataStateDTO
	OwnershipEpoch uint64
}

func sessionConnectDTO(lifecycle session.Lifecycle, principal security.Principal) membership.SessionConnectDTO {
	dto := membership.SessionConnectDTO{ProtocolVersion: uint32(lifecycle.Version())}
	switch lifecycle.Retention() {
	case session.RetentionImmediate:
		dto.Retention = membership.SessionRetentionImmediate
	case session.RetentionFinite:
		dto.Retention = membership.SessionRetentionFinite
	case session.RetentionInfinite:
		dto.Retention = membership.SessionRetentionInfinite
	}
	if lifecycle.Version() == mqtt.ProtocolV5 {
		dto.ExpiryIntervalSeconds = lifecycle.ExpiryInterval()
	}
	if will := lifecycle.Will(); will != nil {
		delay := will.DelayInterval
		if lifecycle.Version() != mqtt.ProtocolV5 {
			delay = 0
		}
		dto.Will = &membership.SessionWillDTO{Topic: will.Topic, Payload: append([]byte(nil), will.Payload...), QoS: uint32(will.QoS), Retain: will.Retain, DelayIntervalSeconds: delay}
		if principal.Subject() != "" {
			roles := principal.Roles()
			withinBounds := len(principal.Subject()) <= membership.MaxSessionPrincipalFieldBytes &&
				len(principal.CredentialID()) > 0 && len(principal.CredentialID()) <= membership.MaxSessionPrincipalFieldBytes &&
				len(roles) <= membership.MaxSessionPrincipalRoles
			roleNames := make([]string, len(roles))
			for index, role := range roles {
				roleNames[index] = string(role)
				if len(roleNames[index]) == 0 || len(roleNames[index]) > membership.MaxSessionPrincipalRoleBytes {
					withinBounds = false
				}
			}
			expires := principal.ExpiresAt().UnixMilli()
			if expires <= 0 {
				withinBounds = false
			}
			if withinBounds {
				dto.Principal = &membership.SessionPrincipalDTO{Subject: principal.Subject(), CredentialID: principal.CredentialID(), Roles: roleNames, ExpiresAtUnixMillis: uint64(expires)}
			}
		}
	}
	return dto
}

func sessionWillFromDTO(dto *membership.SessionWillDTO) *mqtt.Will {
	if dto == nil {
		return nil
	}
	return &mqtt.Will{Topic: dto.Topic, Payload: append([]byte(nil), dto.Payload...), QoS: byte(dto.QoS), Retain: dto.Retain, DelayInterval: dto.DelayIntervalSeconds}
}

func millisTime(value uint64) (time.Time, error) {
	if value == 0 {
		return time.Time{}, nil
	}
	if value > uint64(1<<63-1) {
		return time.Time{}, session.ErrInvalidRestore
	}
	return time.UnixMilli(int64(value)), nil
}

func restorePrincipal(tenant tenancy.TenantID, clientID string, dto *membership.SessionPrincipalDTO) (security.Principal, bool) {
	if dto == nil || dto.ExpiresAtUnixMillis == 0 || dto.ExpiresAtUnixMillis > uint64(1<<63-1) {
		return security.Principal{}, false
	}
	roles := make([]security.Role, len(dto.Roles))
	for index, role := range dto.Roles {
		roles[index] = security.Role(role)
	}
	principal, err := security.NewPrincipal(tenant, clientID, dto.Subject, dto.CredentialID, roles, time.UnixMilli(int64(dto.ExpiresAtUnixMillis)))
	return principal, err == nil
}

// RecoverOwnedSession installs a committed lifecycle locally after ownership recovery.
func (useCase *UseCase) RecoverOwnedSession(ctx context.Context, recovered RecoveredSessionDTO) error {
	if useCase == nil {
		return ErrNilSessionRepository
	}
	metadata := recovered.Metadata
	tenant, err := tenancy.NewTenantID(metadata.TenantID)
	if err != nil {
		return err
	}
	key, err := tenancy.NewSessionKey(tenant, metadata.ClientID)
	if err != nil {
		return err
	}
	sessionID := routing.SessionID(key.RoutingID())
	useCase.mu.Lock()
	timerKey := string(sessionID)
	current, found, err := useCase.loadSession(key, timerKey)
	if err != nil {
		useCase.mu.Unlock()
		return err
	}
	if found && current.Connected() {
		useCase.mu.Unlock()
		return nil
	}
	if useCase.ownershipEpochs[sessionID] >= recovered.OwnershipEpoch {
		useCase.mu.Unlock()
		return nil
	}
	useCase.stopSessionTimers(timerKey)
	expiresAt, err := millisTime(metadata.SessionExpiresAtUnixMillis)
	if err != nil {
		useCase.mu.Unlock()
		return err
	}
	principal, hasPrincipal := restorePrincipal(tenant, metadata.ClientID, metadata.Principal)
	if hasPrincipal {
		useCase.principals[sessionID] = principal
	} else {
		delete(useCase.principals, sessionID)
	}
	useCase.sessionKeys[sessionID] = key
	previousEpoch, hadPreviousEpoch := useCase.ownershipEpochs[sessionID]
	useCase.ownershipEpochs[sessionID] = recovered.OwnershipEpoch
	version := mqtt.ProtocolVersion(metadata.ProtocolVersion)
	retention := session.RetentionImmediate
	switch metadata.Retention {
	case membership.SessionRetentionImmediate:
		retention = session.RetentionImmediate
	case membership.SessionRetentionFinite:
		retention = session.RetentionFinite
	case membership.SessionRetentionInfinite:
		retention = session.RetentionInfinite
	default:
		useCase.restoreRecoveryMarker(sessionID, previousEpoch, hadPreviousEpoch)
		useCase.mu.Unlock()
		return session.ErrInvalidRestore
	}
	if metadata.Connected {
		lifecycle, restoreErr := session.Restore(session.RestoreInput{Key: key, Version: version, Retention: retention, ExpiryInterval: metadata.ExpiryIntervalSeconds, Connected: true, Will: sessionWillFromDTO(metadata.Will)})
		if restoreErr != nil {
			useCase.restoreRecoveryMarker(sessionID, previousEpoch, hadPreviousEpoch)
			useCase.mu.Unlock()
			return restoreErr
		}
		if err := useCase.saveSession(key, timerKey, lifecycle); err != nil {
			useCase.restoreRecoveryMarker(sessionID, previousEpoch, hadPreviousEpoch)
			useCase.mu.Unlock()
			return err
		}
		if err := useCase.deliveries.Register(sessionID); err != nil {
			_ = useCase.deleteSession(key, timerKey)
			useCase.restoreRecoveryMarker(sessionID, previousEpoch, hadPreviousEpoch)
			useCase.mu.Unlock()
			return err
		}
		_ = useCase.routes.BindSessionTenant(sessionID, tenant)
		reason := session.DisconnectAbnormal
		if metadata.Epoch < recovered.OwnershipEpoch {
			reason = session.DisconnectPlannedHandoff
		}
		outcome, transitionErr := useCase.disconnectLocked(sessionID, reason)
		if transitionErr != nil || outcome.transitionErr != nil {
			useCase.stopSessionTimers(timerKey)
			_ = useCase.deleteSession(key, timerKey)
			useCase.deliveries.Remove(sessionID)
			delete(useCase.sessionKeys, sessionID)
			delete(useCase.principals, sessionID)
			useCase.restoreRecoveryMarker(sessionID, previousEpoch, hadPreviousEpoch)
			useCase.mu.Unlock()
			return errors.Join(transitionErr, outcome.transitionErr)
		}
		useCase.mu.Unlock()
		if outcome.will != nil {
			if publishErr := useCase.willPublisher.PublishWill(sessionID, key, *outcome.will); publishErr != nil {
				return errors.Join(outcome.transitionErr, publishErr)
			}
		}
		return outcome.transitionErr
	}
	will := (*mqtt.Will)(nil)
	willDue, err := millisTime(metadata.WillDueAtUnixMillis)
	if err != nil {
		useCase.restoreRecoveryMarker(sessionID, previousEpoch, hadPreviousEpoch)
		useCase.mu.Unlock()
		return err
	}
	if !willDue.IsZero() {
		will = sessionWillFromDTO(metadata.Will)
	}
	lifecycle, err := session.Restore(session.RestoreInput{Key: key, Version: version, Retention: retention, ExpiryInterval: metadata.ExpiryIntervalSeconds, Connected: false, ExpiresAt: expiresAt, Will: will})
	if err != nil {
		useCase.restoreRecoveryMarker(sessionID, previousEpoch, hadPreviousEpoch)
		useCase.mu.Unlock()
		return err
	}
	if err := useCase.saveSession(key, timerKey, lifecycle); err != nil {
		useCase.restoreRecoveryMarker(sessionID, previousEpoch, hadPreviousEpoch)
		useCase.mu.Unlock()
		return err
	}
	timers := &clientTimers{}
	now := useCase.clock.Now()
	hasTimer := false
	if !willDue.IsZero() {
		delay := willDue.Sub(now)
		if delay < 0 {
			delay = 0
		}
		timers.willTimer = useCase.scheduler.AfterFunc(delay, func() { useCase.onWillTimerForTimer(sessionID, timers) })
		hasTimer = true
	}
	if !expiresAt.IsZero() {
		delay := expiresAt.Sub(now)
		if delay < 0 {
			delay = 0
		}
		timers.expiryTimer = useCase.scheduleExpiryTimerLocked(sessionID, delay, timers)
		hasTimer = true
	}
	if hasTimer {
		useCase.activeTimers[timerKey] = timers
	}
	useCase.mu.Unlock()
	return nil
}

func (useCase *UseCase) restoreRecoveryMarker(sessionID routing.SessionID, epoch uint64, existed bool) {
	if existed {
		useCase.ownershipEpochs[sessionID] = epoch
	} else {
		delete(useCase.ownershipEpochs, sessionID)
	}
}
