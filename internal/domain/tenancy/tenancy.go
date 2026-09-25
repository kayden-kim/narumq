// Package tenancy owns tenant-scoped MQTT identity, topic and filter values.
package tenancy

import (
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"
)

var (
	// ErrInvalidTenantID means that a tenant identifier is empty or not valid UTF-8 MQTT text.
	ErrInvalidTenantID = errors.New("invalid tenant ID")
	// ErrInvalidClientID means that a session Client ID is empty or not valid MQTT text.
	ErrInvalidClientID = errors.New("invalid Client ID")
	// ErrInvalidSessionKey means that a session key contains an invalid tenant or Client ID.
	ErrInvalidSessionKey = errors.New("invalid session key")
	// ErrInvalidTopicFilter means that an MQTT topic filter is malformed.
	ErrInvalidTopicFilter = errors.New("invalid MQTT topic filter")
	// ErrInvalidTopicName means that an MQTT concrete topic is malformed.
	ErrInvalidTopicName = errors.New("invalid MQTT topic name")
	// ErrTenantMismatch means that values from different tenant namespaces were compared.
	ErrTenantMismatch = errors.New("tenant mismatch")
)

// TenantID identifies one isolated tenant namespace.
type TenantID struct {
	value string
}

// NewTenantID validates and constructs a tenant identifier.
func NewTenantID(raw string) (TenantID, error) {
	if raw == "" || !validMQTTString(raw) {
		return TenantID{}, ErrInvalidTenantID
	}
	return TenantID{value: raw}, nil
}

// String returns the external string representation of the tenant identifier.
func (tenantID TenantID) String() string {
	return tenantID.value
}

// SessionKey identifies one client session within a tenant.
type SessionKey struct {
	tenantID TenantID
	clientID string
}

// NewSessionKey validates and constructs a tenant-scoped session key.
func NewSessionKey(tenantID TenantID, clientID string) (SessionKey, error) {
	if !tenantID.valid() {
		return SessionKey{}, ErrInvalidSessionKey
	}
	if clientID == "" || !validMQTTString(clientID) {
		return SessionKey{}, ErrInvalidClientID
	}
	return SessionKey{tenantID: tenantID, clientID: clientID}, nil
}

// TenantID returns the tenant namespace of the session key.
func (key SessionKey) TenantID() TenantID {
	return key.tenantID
}

// ClientID returns the MQTT Client ID of the session key.
func (key SessionKey) ClientID() string {
	return key.clientID
}

// RoutingID returns an injective internal identifier for this tenant-scoped session.
// Its invalid-UTF-8 prefix keeps it disjoint from raw MQTT Client IDs.
func (key SessionKey) RoutingID() string {
	if !key.tenantID.valid() || key.clientID == "" || !validMQTTString(key.clientID) {
		return ""
	}
	tenantID := key.tenantID.String()
	encoded := make([]byte, 1, 1+20+1+len(tenantID)+20+1+len(key.clientID))
	encoded[0] = 0xff
	encoded = strconv.AppendInt(encoded, int64(len(tenantID)), 10)
	encoded = append(encoded, ':')
	encoded = append(encoded, tenantID...)
	encoded = strconv.AppendInt(encoded, int64(len(key.clientID)), 10)
	encoded = append(encoded, ':')
	encoded = append(encoded, key.clientID...)
	return string(encoded)
}

// ParseSessionKeyRoutingID decodes the canonical internal identifier for a tenant-scoped session.
func ParseSessionKeyRoutingID(routingID string) (SessionKey, error) {
	if len(routingID) == 0 || routingID[0] != 0xff {
		return SessionKey{}, ErrInvalidSessionKey
	}

	tenantLength, offset, err := parseSessionKeyRoutingIDLength(routingID, 1)
	if err != nil || tenantLength > len(routingID)-offset {
		return SessionKey{}, ErrInvalidSessionKey
	}
	tenantID, err := NewTenantID(routingID[offset : offset+tenantLength])
	if err != nil {
		return SessionKey{}, ErrInvalidSessionKey
	}
	offset += tenantLength

	clientLength, offset, err := parseSessionKeyRoutingIDLength(routingID, offset)
	if err != nil || clientLength != len(routingID)-offset {
		return SessionKey{}, ErrInvalidSessionKey
	}
	key, err := NewSessionKey(tenantID, routingID[offset:])
	if err != nil {
		return SessionKey{}, ErrInvalidSessionKey
	}
	return key, nil
}

func parseSessionKeyRoutingIDLength(routingID string, offset int) (int, int, error) {
	if offset >= len(routingID) {
		return 0, 0, ErrInvalidSessionKey
	}
	separator := strings.IndexByte(routingID[offset:], ':')
	if separator <= 0 {
		return 0, 0, ErrInvalidSessionKey
	}
	lengthText := routingID[offset : offset+separator]
	if len(lengthText) > 1 && lengthText[0] == '0' {
		return 0, 0, ErrInvalidSessionKey
	}
	for index := range lengthText {
		if lengthText[index] < '0' || lengthText[index] > '9' {
			return 0, 0, ErrInvalidSessionKey
		}
	}
	length, err := strconv.Atoi(lengthText)
	if err != nil {
		return 0, 0, ErrInvalidSessionKey
	}
	return length, offset + separator + 1, nil
}

// Topic identifies one concrete MQTT topic within a tenant.
type Topic struct {
	tenantID TenantID
	name     string
}

// NewTopic validates and constructs a tenant-scoped concrete topic.
func NewTopic(tenantID TenantID, name string) (Topic, error) {
	if !tenantID.valid() {
		return Topic{}, ErrInvalidTenantID
	}
	if err := ValidateTopicName(name); err != nil {
		return Topic{}, err
	}
	return Topic{tenantID: tenantID, name: name}, nil
}

// TenantID returns the tenant namespace of the topic.
func (topic Topic) TenantID() TenantID {
	return topic.tenantID
}

// Name returns the external MQTT topic name.
func (topic Topic) Name() string {
	return topic.name
}

// TopicFilter identifies one MQTT topic filter within a tenant.
type TopicFilter struct {
	tenantID TenantID
	filter   string
}

// NewTopicFilter validates and constructs a tenant-scoped topic filter.
func NewTopicFilter(tenantID TenantID, filter string) (TopicFilter, error) {
	if !tenantID.valid() {
		return TopicFilter{}, ErrInvalidTenantID
	}
	if err := ValidateTopicFilter(filter); err != nil {
		return TopicFilter{}, err
	}
	return TopicFilter{tenantID: tenantID, filter: filter}, nil
}

// TenantID returns the tenant namespace of the topic filter.
func (filter TopicFilter) TenantID() TenantID {
	return filter.tenantID
}

// Filter returns the external MQTT topic filter.
func (filter TopicFilter) Filter() string {
	return filter.filter
}

func (tenantID TenantID) valid() bool {
	return tenantID.value != "" && validMQTTString(tenantID.value)
}

func (topic Topic) valid() bool {
	return topic.tenantID.valid() && ValidateTopicName(topic.name) == nil
}

func (filter TopicFilter) valid() bool {
	return filter.tenantID.valid() && ValidateTopicFilter(filter.filter) == nil
}

// ValidateTopicFilter validates MQTT wildcard placement, shared syntax and UTF-8 rules.
func ValidateTopicFilter(filter string) error {
	if filter == "" || !validMQTTString(filter) {
		return ErrInvalidTopicFilter
	}
	if IsSharedFilter(filter) {
		_, topicFilter, _, err := ParseSharedFilter(filter)
		if err != nil {
			return err
		}
		return validateFilterLevels(topicFilter)
	}
	return validateFilterLevels(filter)
}

func validateFilterLevels(filter string) error {
	levels := strings.Split(filter, "/")
	for index, level := range levels {
		if level == "+" {
			continue
		}
		if level == "#" {
			if index != len(levels)-1 {
				return ErrInvalidTopicFilter
			}
			continue
		}
		if strings.ContainsAny(level, "+#") {
			return ErrInvalidTopicFilter
		}
	}
	return nil
}

// ParseSharedFilter parses MQTT shared-subscription syntax.
// Non-shared filters return isShared=false and the original filter.
func ParseSharedFilter(filter string) (shareName string, topicFilter string, isShared bool, err error) {
	if !strings.HasPrefix(filter, "$share/") {
		return "", filter, false, nil
	}
	if !validMQTTString(filter) {
		return "", "", true, ErrInvalidTopicFilter
	}
	rest := filter[len("$share/"):]
	slashIndex := strings.IndexByte(rest, '/')
	if slashIndex < 0 {
		return "", "", true, ErrInvalidTopicFilter
	}
	shareName = rest[:slashIndex]
	if shareName == "" || strings.ContainsAny(shareName, "/+#") {
		return "", "", true, ErrInvalidTopicFilter
	}
	topicFilter = rest[slashIndex+1:]
	if topicFilter == "" || strings.HasPrefix(topicFilter, "$share/") {
		return "", "", true, ErrInvalidTopicFilter
	}
	if err := validateFilterLevels(topicFilter); err != nil {
		return "", "", true, err
	}
	return shareName, topicFilter, true, nil
}

// ValidateTopicName validates a concrete publication topic without wildcards.
func ValidateTopicName(topic string) error {
	if topic == "" || !validMQTTString(topic) || strings.ContainsAny(topic, "+#") {
		return ErrInvalidTopicName
	}
	return nil
}

// IsSharedFilter reports whether a filter uses MQTT shared-subscription syntax.
func IsSharedFilter(filter string) bool {
	return strings.HasPrefix(filter, "$share/")
}

// TopicMatchesFilter validates a concrete topic and filter, then applies MQTT matching.
func TopicMatchesFilter(topic, filter string) (bool, error) {
	if err := ValidateTopicName(topic); err != nil {
		return false, err
	}
	if err := ValidateTopicFilter(filter); err != nil {
		return false, err
	}
	_, targetFilter, _, err := ParseSharedFilter(filter)
	if err != nil {
		return false, err
	}
	return topicMatchesFilter(topic, targetFilter), nil
}

// TopicMatches reports whether a tenant-scoped topic matches a tenant-scoped filter.
func TopicMatches(topic Topic, filter TopicFilter) (bool, error) {
	if !topic.valid() {
		return false, ErrInvalidTopicName
	}
	if !filter.valid() {
		return false, ErrInvalidTopicFilter
	}
	if topic.tenantID != filter.tenantID {
		return false, ErrTenantMismatch
	}
	return TopicMatchesFilter(topic.name, filter.filter)
}

// FilterContains reports whether every topic admitted by contained is admitted by container.
func FilterContains(container, contained TopicFilter) (bool, error) {
	if !container.valid() || !contained.valid() {
		return false, ErrInvalidTopicFilter
	}
	if container.tenantID != contained.tenantID {
		return false, ErrTenantMismatch
	}
	containerParts, containerHash := filterParts(container.filter)
	containedParts, containedHash := filterParts(contained.filter)
	if !containerHash && containedHash {
		return false, nil
	}
	if !containerHash {
		if len(containerParts) != len(containedParts) {
			return false, nil
		}
		for index := range containerParts {
			if !levelSubset(containerParts[index], containedParts[index], index) {
				return false, nil
			}
		}
		return true, nil
	}
	if containedHash {
		if len(containerParts) > len(containedParts) &&
			!(len(containerParts) == 1 && containerParts[0] == "+" && len(containedParts) == 0) {
			return false, nil
		}
	} else if len(containedParts) < len(containerParts) {
		return false, nil
	}
	if len(containerParts) == 0 && len(containedParts) > 0 && isSystemLiteral(containedParts[0]) {
		return false, nil
	}
	if containedHash && len(containerParts) == 1 && containerParts[0] == "+" && len(containedParts) == 0 {
		return true, nil
	}
	for index := range containerParts {
		if !levelSubset(containerParts[index], containedParts[index], index) {
			return false, nil
		}
	}
	return true, nil
}

// FiltersIntersect reports whether two tenant-scoped filters admit at least one topic in common.
func FiltersIntersect(left, right TopicFilter) (bool, error) {
	if !left.valid() || !right.valid() {
		return false, ErrInvalidTopicFilter
	}
	if left.tenantID != right.tenantID {
		return false, ErrTenantMismatch
	}
	leftParts, leftHash := filterParts(left.filter)
	rightParts, rightHash := filterParts(right.filter)
	if len(leftParts) == 0 && len(rightParts) > 0 && isSystemLiteral(rightParts[0]) {
		return false, nil
	}
	if len(rightParts) == 0 && len(leftParts) > 0 && isSystemLiteral(leftParts[0]) {
		return false, nil
	}
	for index := 0; index < min(len(leftParts), len(rightParts)); index++ {
		if !levelsIntersect(leftParts[index], rightParts[index], index) {
			return false, nil
		}
	}
	if !leftHash && !rightHash {
		return len(leftParts) == len(rightParts), nil
	}
	if !leftHash {
		return len(leftParts) >= len(rightParts), nil
	}
	if !rightHash {
		return len(rightParts) >= len(leftParts), nil
	}
	return true, nil
}

func filterParts(filter string) ([]string, bool) {
	_, topicFilter, _, _ := ParseSharedFilter(filter)
	parts := strings.Split(topicFilter, "/")
	if parts[len(parts)-1] == "#" {
		return parts[:len(parts)-1], true
	}
	return parts, false
}

func levelSubset(container, contained string, index int) bool {
	if container == "+" {
		if contained == "+" {
			return true
		}
		return index != 0 || !isSystemLiteral(contained)
	}
	return contained != "+" && container == contained
}

func levelsIntersect(left, right string, index int) bool {
	if left == "+" && right == "+" {
		return true
	}
	if left == "+" {
		return index != 0 || !isSystemLiteral(right)
	}
	if right == "+" {
		return index != 0 || !isSystemLiteral(left)
	}
	return left == right
}

func isSystemLiteral(level string) bool {
	return level != "" && level != "+" && level != "#" && strings.HasPrefix(level, "$")
}

func topicMatchesFilter(topic, filter string) bool {
	if strings.HasPrefix(topic, "$") {
		firstLevelEnd := strings.IndexByte(filter, '/')
		firstLevel := filter
		if firstLevelEnd >= 0 {
			firstLevel = filter[:firstLevelEnd]
		}
		if firstLevel == "+" || firstLevel == "#" {
			return false
		}
	}

	topicLevels := strings.Split(topic, "/")
	filterLevels := strings.Split(filter, "/")
	for index, filterLevel := range filterLevels {
		if filterLevel == "#" {
			return true
		}
		if index >= len(topicLevels) {
			return false
		}
		if filterLevel != "+" && filterLevel != topicLevels[index] {
			return false
		}
	}
	return len(filterLevels) == len(topicLevels)
}

func validMQTTString(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, codePoint := range value {
		if codePoint == 0 || (codePoint >= 0xD800 && codePoint <= 0xDFFF) {
			return false
		}
		if codePoint >= 0xFDD0 && codePoint <= 0xFDEF {
			return false
		}
		if codePoint&0xFFFF == 0xFFFE || codePoint&0xFFFF == 0xFFFF {
			return false
		}
	}
	return true
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
