package main

import (
	"path/filepath"
	"testing"
	"time"
)

func bp02SmokeProfile(t *testing.T) Profile {
	t.Helper()
	profile, _, _, err := LoadProfile(filepath.Join("testdata", "smoke.json"))
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestClientPlanAssignsTenantNodeAndTransport(t *testing.T) {
	profile := bp02SmokeProfile(t)
	plan := bp02ClientPlan(profile)
	seen := make(map[string]struct{}, len(plan))
	for _, client := range plan {
		if _, ok := seen[client.id]; ok {
			t.Fatalf("duplicate client ID %q", client.id)
		}
		seen[client.id] = struct{}{}
		if client.nodeIndex != client.ordinal%bp02NodeCount {
			t.Errorf("%s node = %d, ordinal %d", client.id, client.nodeIndex, client.ordinal)
		}
		if client.tls != (client.ordinal%profile.TLSEveryNthClient == 0) {
			t.Errorf("%s TLS assignment mismatch", client.id)
		}
	}
	setup := bp02SetupClients(profile)
	if len(setup) != 49 {
		t.Fatalf("setup clients = %d, want 49", len(setup))
	}
	tlsCount := 0
	for _, client := range setup {
		if client.tls {
			tlsCount++
		}
	}
	if tlsCount != 25 {
		t.Fatalf("TLS setup clients = %d, want 25", tlsCount)
	}
	if plan[0].ordinal != 0 || plan[0].nodeIndex != 0 || !plan[0].tls || plan[1].ordinal != 1 || plan[1].nodeIndex != 1 || plan[1].tls {
		t.Fatalf("initial client assignment = %#v, %#v", plan[0], plan[1])
	}
	a, b := bp02AllowedClientIDs(profile, "bp02-a"), bp02AllowedClientIDs(profile, "bp02-b")
	allowed := make(map[string]struct{}, len(a))
	for _, id := range a {
		allowed[id] = struct{}{}
	}
	for _, id := range b {
		if _, ok := allowed[id]; ok {
			t.Errorf("client ID %q allowed for both tenants", id)
		}
	}
}

func TestExpectedRecipientsRespectTenantAndShareGroup(t *testing.T) {
	subscribers := bp02Subscribers(bp02SmokeProfile(t))
	commandTopic := publishTopic(2, 3)
	recipients := expectedRecipients(subscribers, 2, commandTopic)
	if len(recipients) != 1 || subscribers[recipients[0]].id != "bp02-a-sub-command-02" {
		t.Fatalf("command recipients = %v, want bp02-a-sub-command-02", recipientIDs(subscribers, recipients))
	}
	jobTopic := publishTopic(3, 1)
	collector := newCollector(bp02SmokeProfile(t), "narumq")
	if err := collector.addMessage(3, 1, 1, "step-01-measure", jobTopic, time.Now()); err != nil {
		t.Fatal(err)
	}
	if expected := collector.summary().Steps[0].Streams["a_job"].ExpectedDeliveries; expected != 1 {
		t.Fatalf("shared group expected deliveries = %d, want 1", expected)
	}
	for stream, topic := range []string{"bp02/a/telemetry/dev00", "bp02/a/alert/zone0", "bp02/a/command/dev00", "bp02/a/job/0"} {
		for _, index := range expectedRecipients(subscribers, stream, topic) {
			if subscribers[index].tenant != "bp02-a" {
				t.Errorf("tenant B received tenant A topic: %s", subscribers[index].id)
			}
		}
	}
}

func recipientIDs(subscribers []clientSpec, indexes []int) []string {
	ids := make([]string, 0, len(indexes))
	for _, index := range indexes {
		ids = append(ids, subscribers[index].id)
	}
	return ids
}

func TestPayloadSizeCycleAndValidation(t *testing.T) {
	profile := bp02SmokeProfile(t)
	for _, test := range []struct {
		sequence uint64
		size     int
	}{{1, 128}, {7, 512}, {10, 16384}} {
		payload := makePayload(profile, 0, test.sequence)
		if len(payload) != test.size {
			t.Errorf("sequence %d payload size = %d, want %d", test.sequence, len(payload), test.size)
		}
		sequence, stream, err := validateStreamPayload(profile, payload)
		if err != nil || sequence != test.sequence || stream != 0 {
			t.Errorf("validate sequence %d = %d, %d, %v", test.sequence, stream, sequence, err)
		}
		payload[len(payload)-1] ^= 1
		if _, _, err := validateStreamPayload(profile, payload); err == nil {
			t.Errorf("corrupt sequence %d payload was accepted", test.sequence)
		}
	}
}
