// Package release defines canonical broker release versions and feature tokens.
package release

import (
	"errors"
	"strconv"
	"strings"
)

// ErrInvalidVersion is returned when a release is not canonical MAJOR.MINOR.PATCH.
var ErrInvalidVersion = errors.New("release version must be MAJOR.MINOR.PATCH")

// MaxFeatures is the maximum number of feature tokens supported by one build.
const MaxFeatures = 16

// Version is a canonical semantic release version with unsigned 32-bit components.
type Version struct {
	Major uint32
	Minor uint32
	Patch uint32
}

// Parse parses a canonical MAJOR.MINOR.PATCH release version.
func Parse(value string) (Version, error) {
	if len(value) == 0 || len(value) > 64 {
		return Version{}, ErrInvalidVersion
	}

	firstDot := strings.IndexByte(value, '.')
	if firstDot <= 0 {
		return Version{}, ErrInvalidVersion
	}
	remaining := value[firstDot+1:]
	secondDot := strings.IndexByte(remaining, '.')
	if secondDot <= 0 || strings.IndexByte(remaining[secondDot+1:], '.') >= 0 {
		return Version{}, ErrInvalidVersion
	}

	major, ok := parseComponent(value[:firstDot])
	if !ok {
		return Version{}, ErrInvalidVersion
	}
	minor, ok := parseComponent(remaining[:secondDot])
	if !ok {
		return Version{}, ErrInvalidVersion
	}
	patch, ok := parseComponent(remaining[secondDot+1:])
	if !ok {
		return Version{}, ErrInvalidVersion
	}
	return Version{Major: major, Minor: minor, Patch: patch}, nil
}

func parseComponent(value string) (uint32, bool) {
	if len(value) == 0 || (len(value) > 1 && value[0] == '0') {
		return 0, false
	}

	const maxUint32 = ^uint32(0)
	var number uint32
	for index := 0; index < len(value); index++ {
		digit := value[index] - '0'
		if digit > 9 || number > (maxUint32-uint32(digit))/10 {
			return 0, false
		}
		number = number*10 + uint32(digit)
	}
	return number, true
}

// String returns the canonical MAJOR.MINOR.PATCH representation of version.
func (version Version) String() string {
	var buffer [30]byte
	value := strconv.AppendUint(buffer[:0], uint64(version.Major), 10)
	value = append(value, '.')
	value = strconv.AppendUint(value, uint64(version.Minor), 10)
	value = append(value, '.')
	value = strconv.AppendUint(value, uint64(version.Patch), 10)
	return string(value)
}

// Compatible reports whether two releases share a major and differ by at most one minor.
func Compatible(left, right Version) bool {
	if left.Major != right.Major {
		return false
	}
	if left.Minor > right.Minor {
		return left.Minor-right.Minor <= 1
	}
	return right.Minor-left.Minor <= 1
}

// CompatibleStrings parses and compares two canonical release strings.
func CompatibleStrings(left, right string) bool {
	leftVersion, err := Parse(left)
	if err != nil {
		return false
	}
	rightVersion, err := Parse(right)
	if err != nil {
		return false
	}
	return Compatible(leftVersion, rightVersion)
}

// ValidFeature reports whether name is a lower-case opaque feature token.
func ValidFeature(name string) bool {
	if len(name) == 0 || len(name) > 63 || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for index := 1; index < len(name); index++ {
		character := name[index]
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return false
		}
	}
	return true
}

// ValidFeatureSet reports whether features is a sorted, unique set of valid tokens.
func ValidFeatureSet(features []string) bool {
	if len(features) > MaxFeatures {
		return false
	}
	for index, feature := range features {
		if !ValidFeature(feature) || (index > 0 && features[index-1] >= feature) {
			return false
		}
	}
	return true
}

// Contains reports whether the sorted feature set contains name.
func Contains(features []string, name string) bool {
	low, high := 0, len(features)
	for low < high {
		middle := low + (high-low)/2
		if features[middle] < name {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low < len(features) && features[low] == name
}
