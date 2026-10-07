package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func smokeProfile(t *testing.T) Profile {
	t.Helper()
	profile, _, _, err := LoadProfile(filepath.Join("testdata", "smoke.json"))
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestLoadProfilePreservesSourceHash(t *testing.T) {
	path := filepath.Join("testdata", "smoke.json")
	profile, source, hash, err := LoadProfile(path)
	if err != nil {
		t.Fatal(err)
	}
	if profile.ProfileID != "bp04-smoke-v1" || profile.Purpose != purposeSmoke {
		t.Fatalf("loaded profile identity = %q/%q", profile.ProfileID, profile.Purpose)
	}
	digest := sha256.Sum256(source)
	if hash != hex.EncodeToString(digest[:]) {
		t.Fatalf("profile SHA-256 = %q, want %q", hash, hex.EncodeToString(digest[:]))
	}
	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(source) != string(actual) {
		t.Fatal("LoadProfile changed the exact source bytes")
	}
}

func TestDecodeProfileRejectsUnknownFieldAndInvalidSmokeBound(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "smoke.json"))
	if err != nil {
		t.Fatal(err)
	}
	withUnknown := strings.Replace(string(data), "{", `{"unknown":true,`, 1)
	if _, err := DecodeProfile([]byte(withUnknown)); err == nil {
		t.Fatal("DecodeProfile accepted an unknown field")
	}

	profile := smokeProfile(t)
	profile.RouteProbeIntervalMillis = 0
	if err := profile.Validate(); err == nil {
		t.Fatal("Validate accepted a zero route probe interval")
	}
}

func TestWorkloadPlanIDsAndTopicOracle(t *testing.T) {
	profile := smokeProfile(t)
	subscribers := workloadSubscribers(profile)
	if len(subscribers) != 96 {
		t.Fatalf("workload subscriber count = %d, want 96", len(subscribers))
	}
	for index, subscriber := range subscribers {
		if subscriber.ordinal != index || subscriber.nodeIndex != index%bp04NodeCount {
			t.Fatalf("subscriber %q ordinal/node = %d/%d, want %d/%d", subscriber.id, subscriber.ordinal, subscriber.nodeIndex, index, index%bp04NodeCount)
		}
	}
	if got := len(expectedRecipients(subscribers, classWildcard, publishTopic(classWildcard, 1))); got != 9 {
		t.Fatalf("wildcard sequence 1 recipients = %d, want 9", got)
	}
	if got := len(expectedRecipients(subscribers, classWildcard, publishTopic(classWildcard, 2))); got != 7 {
		t.Fatalf("wildcard sequence 2 recipients = %d, want 7", got)
	}
	if got := len(expectedRecipients(subscribers, classOverlap, publishTopic(classOverlap, 1))); got != profile.OverlapSubscribers {
		t.Fatalf("overlap recipients = %d, want %d", got, profile.OverlapSubscribers)
	}
	if got := len(expectedRecipients(subscribers, classShared, publishTopic(classShared, 1))); got != profile.SharedSubscribers {
		t.Fatalf("shared group members = %d, want %d", got, profile.SharedSubscribers)
	}

	ids := bp04ClientIDs(profile)
	if len(ids) != 127 {
		t.Fatalf("allowlisted client ID count = %d, want 127", len(ids))
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("client ID %q is duplicated", id)
		}
		seen[id] = struct{}{}
	}
}

func TestTopicMatchesMQTTWildcardBoundaries(t *testing.T) {
	cases := []struct {
		filter string
		topic  string
		want   bool
	}{
		{filter: "bp04/wild/+/d0", topic: "bp04/wild/r1/d0", want: true},
		{filter: "bp04/wild/+/d0", topic: "bp04/wild/r1/d0/extra", want: false},
		{filter: "bp04/wild/#", topic: "bp04/wild", want: true},
		{filter: "bp04/wild/#", topic: "bp04/wild/r1/d0", want: true},
		{filter: "#", topic: "$SYS/broker/uptime", want: false},
		{filter: "$share/bp04g/bp04/shared/#", topic: "bp04/shared/j1", want: true},
		{filter: "$share/bp04g/bp04/shared/#", topic: "bp04/other/j1", want: false},
	}
	for _, test := range cases {
		t.Run(test.filter+"_"+test.topic, func(t *testing.T) {
			if got := topicMatches(test.filter, test.topic); got != test.want {
				t.Fatalf("topicMatches(%q, %q) = %v, want %v", test.filter, test.topic, got, test.want)
			}
		})
	}
}

func TestValidRouteTopicRequiresThreeDigits(t *testing.T) {
	for _, topic := range []string{"bp04/route/000", "bp04/route/012", "bp04/route/999"} {
		if !validRouteTopic(topic) {
			t.Errorf("validRouteTopic(%q) = false", topic)
		}
	}
	for _, topic := range []string{"bp04/route/00", "bp04/route/0000", "bp04/route/01a", "bp04/route/-01", "bp04/route/000/extra", "bp04/route"} {
		if validRouteTopic(topic) {
			t.Errorf("validRouteTopic(%q) = true", topic)
		}
	}
}
