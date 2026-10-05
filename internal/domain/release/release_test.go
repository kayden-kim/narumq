package release

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseCanonicalVersions(t *testing.T) {
	tests := []struct {
		value string
		want  Version
	}{
		{value: "0.0.0", want: Version{}},
		{value: "1.2.3", want: Version{Major: 1, Minor: 2, Patch: 3}},
		{value: "4294967295.0.0", want: Version{Major: ^uint32(0)}},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			got, err := Parse(test.value)
			if err != nil {
				t.Fatalf("Parse(%q) error = %v", test.value, err)
			}
			if got != test.want {
				t.Fatalf("Parse(%q) = %#v, want %#v", test.value, got, test.want)
			}
			if got.String() != test.value {
				t.Fatalf("Parse(%q).String() = %q", test.value, got.String())
			}
		})
	}
}

func TestParseRejectsNonCanonicalVersions(t *testing.T) {
	invalid := []string{
		"01.2.3",
		"1.2",
		"1.2.3.4",
		"v1.2.3",
		"1.2.3-rc1",
		"1.2.3+build1",
		"4294967296.0.0",
		"",
		strings.Repeat("1", 65),
		"1..3",
		"1.x.3",
		"1.2.",
		"-1.2.3",
		"1.2.٣",
	}
	for _, value := range invalid {
		t.Run(value, func(t *testing.T) {
			if got, err := Parse(value); !errors.Is(err, ErrInvalidVersion) || got != (Version{}) {
				t.Fatalf("Parse(%q) = %#v, %v; want zero Version and ErrInvalidVersion", value, got, err)
			}
		})
	}
}

func TestCompatibleReleaseWindow(t *testing.T) {
	compatible := [][2]Version{
		{{Major: 1, Minor: 1, Patch: 0}, {Major: 1, Minor: 2, Patch: 9}},
		{{Major: 1, Minor: 2, Patch: 0}, {Major: 1, Minor: 2, Patch: 7}},
		{{Major: 1, Minor: 2, Patch: 0}, {Major: 1, Minor: 1, Patch: 0}},
	}
	for _, pair := range compatible {
		if !Compatible(pair[0], pair[1]) {
			t.Errorf("Compatible(%s, %s) = false, want true", pair[0], pair[1])
		}
		if !Compatible(pair[1], pair[0]) {
			t.Errorf("Compatible(%s, %s) is not symmetric", pair[1], pair[0])
		}
	}

	incompatible := [][2]Version{
		{{Major: 1, Minor: 0}, {Major: 1, Minor: 2}},
		{{Major: 1, Minor: 9}, {Major: 2}},
	}
	for _, pair := range incompatible {
		if Compatible(pair[0], pair[1]) {
			t.Errorf("Compatible(%s, %s) = true, want false", pair[0], pair[1])
		}
	}

	if !CompatibleStrings("1.1.0", "1.2.9") || CompatibleStrings("1.0.0", "1.2.0") ||
		CompatibleStrings("v1.1.0", "1.2.0") || CompatibleStrings("1.2.0", "1.2") {
		t.Fatal("CompatibleStrings returned an incorrect release-window result")
	}
}

func TestValidFeature(t *testing.T) {
	for _, feature := range []string{"a", "compatibility-probe", "feature123", "a" + strings.Repeat("z", 62)} {
		if !ValidFeature(feature) {
			t.Errorf("ValidFeature(%q) = false, want true", feature)
		}
	}
	for _, feature := range []string{"", "Bad", "1feature", "feature_name", "feature/one", "a" + strings.Repeat("z", 63)} {
		if ValidFeature(feature) {
			t.Errorf("ValidFeature(%q) = true, want false", feature)
		}
	}
}

func TestValidFeatureSetAndContains(t *testing.T) {
	if !ValidFeatureSet(nil) || !ValidFeatureSet([]string{}) || !ValidFeatureSet([]string{"a", "compatibility-probe"}) {
		t.Fatal("empty or sorted valid feature set was rejected")
	}
	for _, features := range [][]string{
		{"a", "a"},
		{"b", "a"},
		{"Bad"},
		makeFeatureSet(MaxFeatures + 1),
	} {
		if ValidFeatureSet(features) {
			t.Errorf("ValidFeatureSet(%q) = true, want false", features)
		}
	}

	features := []string{"alpha", "compatibility-probe", "zeta"}
	for _, feature := range []string{"alpha", "compatibility-probe", "zeta"} {
		if !Contains(features, feature) {
			t.Errorf("Contains(%q, %q) = false, want true", features, feature)
		}
	}
	for _, feature := range []string{"", "beta", "zzzz"} {
		if Contains(features, feature) {
			t.Errorf("Contains(%q, %q) = true, want false", features, feature)
		}
	}
}

func makeFeatureSet(count int) []string {
	features := make([]string, count)
	for index := range features {
		features[index] = fmt.Sprintf("feature-%02d", index)
	}
	return features
}
