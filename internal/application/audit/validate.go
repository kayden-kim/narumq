package audit

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"time"
	"unicode/utf8"

	portsaudit "narumq/internal/application/ports/audit"
)

var (
	auditActionPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(\.[a-z][a-z0-9_]*)+$`)
	auditTargetPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	auditReasonPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

func validateEvent(event portsaudit.AuditEventDTO) error {
	if event.SchemaVersion != "1.0" {
		return fmt.Errorf("schemaVersion must be 1.0")
	}
	if !validRequiredString(event.EventID, 128) {
		return fmt.Errorf("eventId must contain 1 to 128 valid UTF-8 characters")
	}
	if event.OccurredAt.IsZero() {
		return fmt.Errorf("occurredAt must not be zero")
	}
	occurredAt := event.OccurredAt.UTC().Format(time.RFC3339Nano)
	if _, err := time.Parse(time.RFC3339Nano, occurredAt); err != nil {
		return fmt.Errorf("occurredAt is not RFC3339: %w", err)
	}
	if !validRequiredString(event.RequestID, 128) {
		return fmt.Errorf("requestId must contain 1 to 128 valid UTF-8 characters")
	}
	if event.Producer.Component != "BROKER" && event.Producer.Component != "ROLLOUT_CONTROLLER" {
		return fmt.Errorf("producer.component is not supported")
	}
	if !validRequiredString(event.Producer.InstanceID, 256) {
		return fmt.Errorf("producer.instanceId must contain 1 to 256 valid UTF-8 characters")
	}
	if !validRequiredString(event.Producer.Version, 64) {
		return fmt.Errorf("producer.version must contain 1 to 64 valid UTF-8 characters")
	}
	if !validRequiredString(event.Actor.Subject, 256) {
		return fmt.Errorf("actor.subject must contain 1 to 256 valid UTF-8 characters")
	}
	if !validOptionalString(event.Actor.TenantID, 128) {
		return fmt.Errorf("actor.tenantId must be empty or contain at most 128 valid UTF-8 characters")
	}
	if !validOptionalString(event.Actor.CredentialID, 256) {
		return fmt.Errorf("actor.credentialId must be empty or contain at most 256 valid UTF-8 characters")
	}
	seenRoles := make(map[string]struct{}, len(event.Actor.Roles))
	for _, role := range event.Actor.Roles {
		if !validRequiredString(role, 64) {
			return fmt.Errorf("actor.roles values must contain 1 to 64 valid UTF-8 characters")
		}
		if _, exists := seenRoles[role]; exists {
			return fmt.Errorf("actor.roles must not contain duplicates")
		}
		seenRoles[role] = struct{}{}
	}
	if !validRequiredString(event.Action, 128) || !auditActionPattern.MatchString(event.Action) {
		return fmt.Errorf("action does not match the audit action format")
	}
	if !validRequiredString(event.Target.Type, 64) || !auditTargetPattern.MatchString(event.Target.Type) {
		return fmt.Errorf("target.type does not match the audit target format")
	}
	if !validRequiredString(event.Target.ID, 512) {
		return fmt.Errorf("target.id must contain 1 to 512 valid UTF-8 characters")
	}
	if !validOptionalString(event.Target.TenantID, 128) {
		return fmt.Errorf("target.tenantId must be empty or contain at most 128 valid UTF-8 characters")
	}
	switch event.Result {
	case portsaudit.AuditResultAllowed, portsaudit.AuditResultDenied, portsaudit.AuditResultFailed:
	default:
		return fmt.Errorf("result is not supported")
	}
	if !validRequiredString(event.Reason, 128) || !auditReasonPattern.MatchString(event.Reason) {
		return fmt.Errorf("reason does not match the audit reason format")
	}
	if !validOptionalString(event.PolicyVersion, 128) {
		return fmt.Errorf("policyVersion must be empty or contain at most 128 valid UTF-8 characters")
	}
	if !validOptionalString(event.SourceIP, 128) {
		return fmt.Errorf("sourceIp must be empty or contain at most 128 valid UTF-8 characters")
	}
	if event.Details != nil {
		if _, err := json.Marshal(event.Details); err != nil {
			return fmt.Errorf("details must be a JSON object: %w", err)
		}
		if !validJSONStrings(event.Details) {
			return fmt.Errorf("details contains invalid UTF-8")
		}
	}
	return nil
}

func validRequiredString(value string, maxRunes int) bool {
	if !utf8.ValidString(value) {
		return false
	}
	runes := utf8.RuneCountInString(value)
	return runes > 0 && runes <= maxRunes
}

func validOptionalString(value string, maxRunes int) bool {
	if value == "" {
		return true
	}
	return validRequiredString(value, maxRunes)
}

func validJSONStrings(value any) bool {
	return validJSONStringsValue(reflect.ValueOf(value))
}

func validJSONStringsValue(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Ptr:
		return value.IsNil() || validJSONStringsValue(value.Elem())
	case reflect.String:
		return utf8.ValidString(value.String())
	case reflect.Slice, reflect.Array:
		for index := 0; index < value.Len(); index++ {
			if !validJSONStringsValue(value.Index(index)) {
				return false
			}
		}
	case reflect.Map:
		if value.IsNil() {
			return true
		}
		if value.Type().Key().Kind() != reflect.String {
			return true
		}
		iterator := value.MapRange()
		for iterator.Next() {
			if !utf8.ValidString(iterator.Key().String()) || !validJSONStringsValue(iterator.Value()) {
				return false
			}
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			field := value.Type().Field(index)
			if field.Tag.Get("json") == "-" {
				continue
			}
			fieldValue := value.Field(index)
			if field.PkgPath != "" {
				if !field.Anonymous {
					continue
				}
				if fieldValue.Kind() == reflect.Ptr {
					if fieldValue.IsNil() {
						continue
					}
					fieldValue = fieldValue.Elem()
				}
				if fieldValue.Kind() != reflect.Struct {
					continue
				}
			}
			if !validJSONStringsValue(fieldValue) {
				return false
			}
		}
	}
	return true
}
