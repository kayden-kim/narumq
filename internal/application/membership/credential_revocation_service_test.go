package membership

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type credentialRevocationServicePort struct {
	*stateMachinePort
}

func (port *credentialRevocationServicePort) CredentialRevocations(ctx context.Context) ([]CredentialRevocationStateDTO, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return port.machine.CredentialRevocations(), nil
}

type credentialRevocationServiceClock struct {
	now time.Time
}

func (clock *credentialRevocationServiceClock) Now() time.Time {
	return clock.now
}

func newCredentialRevocationServiceFixture(t *testing.T, active bool) (*CredentialRevocationService, *credentialRevocationServicePort, *credentialRevocationServiceClock) {
	t.Helper()
	machine := releaseProfileMachine(t, "1.2.0", []string{CredentialRevocationFeature})
	if active {
		if result := activateFeatureForTest(t, machine, 2, CredentialRevocationFeature); result.Outcome != ApplyOutcomeApplied {
			t.Fatalf("activate credential revocation feature = %+v", result)
		}
	}
	port := &credentialRevocationServicePort{stateMachinePort: &stateMachinePort{
		machine: machine, nextIndex: machine.LastAppliedIndex(),
	}}
	clock := &credentialRevocationServiceClock{now: time.UnixMilli(1_700_000_000_000)}
	service, err := NewCredentialRevocationService(port, clock)
	if err != nil {
		t.Fatalf("new credential revocation service: %v", err)
	}
	return service, port, clock
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7; docs/contracts/management-api-v1.md.
// Scenarios: tenancy-and-security.md:SEC-SCN-036
func TestCredentialRevocationServiceCommitsAndReturnsCommittedStatus(t *testing.T) {
	service, port, clock := newCredentialRevocationServiceFixture(t, true)
	first, err := service.RevokeCredential(context.Background(), "credential-z")
	if err != nil || !first.Revoked || first.CredentialID != "credential-z" || first.RevokedAtUnixMillis != uint64(clock.now.UnixMilli()) {
		t.Fatalf("first revoke = %+v, %v", first, err)
	}

	clock.now = clock.now.Add(time.Minute)
	repeated, err := service.RevokeCredential(context.Background(), "credential-z")
	if err != nil || repeated != first {
		t.Fatalf("repeated revoke = %+v, %v; want original status %+v", repeated, err, first)
	}
	if _, err := service.RevokeCredential(context.Background(), "credential-a"); err != nil {
		t.Fatalf("revoke second credential: %v", err)
	}

	states, err := service.ListCredentialRevocations(context.Background())
	if err != nil || len(states) != 2 || states[0].CredentialID != "credential-a" || states[1].CredentialID != "credential-z" {
		t.Fatalf("revocation list = %+v, %v", states, err)
	}

	restored, err := service.RestoreCredential(context.Background(), "credential-z")
	if err != nil || restored.CredentialID != "credential-z" || restored.Revoked || restored.RevokedAtUnixMillis != 0 {
		t.Fatalf("restore status = %+v, %v", restored, err)
	}
	noOpRestore, err := service.RestoreCredential(context.Background(), "credential-z")
	if err != nil || noOpRestore != restored {
		t.Fatalf("restore active credential = %+v, %v", noOpRestore, err)
	}
	states, err = service.ListCredentialRevocations(context.Background())
	if err != nil || len(states) != 1 || states[0].CredentialID != "credential-a" {
		t.Fatalf("revocation list after restore = %+v, %v", states, err)
	}
	if port.commitCalls != 5 {
		t.Fatalf("committed commands = %d, want 5", port.commitCalls)
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7; docs/contracts/management-api-v1.md.
// Scenarios: tenancy-and-security.md:SEC-SCN-036
func TestCredentialRevocationServiceRejectsInactiveFeatureAndCapacity(t *testing.T) {
	inactive, inactivePort, _ := newCredentialRevocationServiceFixture(t, false)
	if _, err := inactive.RevokeCredential(context.Background(), "credential-a"); !credentialRevocationServiceErrorIs(t, err, ServiceErrorFeatureInactive) {
		t.Fatalf("inactive feature error = %v", err)
	}
	if inactivePort.commitCalls != 1 || len(inactivePort.machine.CredentialRevocations()) != 0 {
		t.Fatalf("inactive feature committed state: commits=%d states=%+v", inactivePort.commitCalls, inactivePort.machine.CredentialRevocations())
	}

	machine := releaseProfileMachine(t, "1.2.0", []string{CredentialRevocationFeature})
	if result := activateFeatureForTest(t, machine, 2, CredentialRevocationFeature); result.Outcome != ApplyOutcomeApplied {
		t.Fatalf("activate credential revocation feature = %+v", result)
	}
	for index := 0; index < MaxCredentialRevocations; index++ {
		operationID := fmt.Sprintf("00000000-0000-4000-8000-%012d", index+3)
		result, err := machine.Apply(uint64(index+3), CommandDTO{
			SchemaMajor: SchemaMajor, OperationID: operationID,
			CredentialRevocation: &CredentialRevocationChangeDTO{
				Action: CredentialRevocationActionRevoke, CredentialID: fmt.Sprintf("credential-%04d", index), ProposedAtUnixMillis: 1,
			},
		})
		if err != nil || result.Outcome != ApplyOutcomeApplied {
			t.Fatalf("fill revocation capacity at %d: result=%+v err=%v", index, result, err)
		}
	}
	port := &credentialRevocationServicePort{stateMachinePort: &stateMachinePort{
		machine: machine, nextIndex: machine.LastAppliedIndex(),
	}}
	service, err := NewCredentialRevocationService(port, &credentialRevocationServiceClock{now: time.UnixMilli(1_700_000_000_000)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RevokeCredential(context.Background(), "credential-overflow"); !credentialRevocationServiceErrorIs(t, err, ServiceErrorCredentialRevocationCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
}

func credentialRevocationServiceErrorIs(t *testing.T, err error, want ServiceErrorCode) bool {
	t.Helper()
	var serviceError *ServiceError
	return errors.As(err, &serviceError) && serviceError.Code == want
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7.
// Scenarios: tenancy-and-security.md:SEC-SCN-036
func TestCredentialRevocationServiceValidatesInputsAndPortFailures(t *testing.T) {
	service, port, _ := newCredentialRevocationServiceFixture(t, true)
	for _, credentialID := range []string{"", string([]byte{0xff}), strings.Repeat("x", MaxCredentialIDBytes+1)} {
		if _, err := service.RevokeCredential(context.Background(), credentialID); !credentialRevocationServiceErrorIs(t, err, ServiceErrorInvalidRequest) {
			t.Errorf("invalid credential ID %q error = %v", credentialID, err)
		}
	}
	if port.commitCalls != 0 {
		t.Fatalf("invalid credential IDs committed %d commands", port.commitCalls)
	}

	port.commitError = ErrNotLeader
	if _, err := service.RevokeCredential(context.Background(), "credential-a"); !credentialRevocationServiceErrorIs(t, err, ServiceErrorNotLeader) {
		t.Fatalf("not-leader port error = %v", err)
	}
}

// Traceability: SEC-07; docs/specs/tenancy-and-security.md §7.
// Scenarios: tenancy-and-security.md:SEC-SCN-036
func TestNewCredentialRevocationServiceRequiresPorts(t *testing.T) {
	clock := &credentialRevocationServiceClock{now: time.UnixMilli(1)}
	if service, err := NewCredentialRevocationService(nil, clock); service != nil || err == nil {
		t.Fatalf("nil port service=%v error=%v", service, err)
	}
	machine := releaseProfileMachine(t, "1.2.0", []string{CredentialRevocationFeature})
	port := &credentialRevocationServicePort{stateMachinePort: &stateMachinePort{machine: machine}}
	if service, err := NewCredentialRevocationService(port, nil); service != nil || err == nil {
		t.Fatalf("nil clock service=%v error=%v", service, err)
	}
}
