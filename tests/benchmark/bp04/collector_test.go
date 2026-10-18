package main

import (
	"testing"
	"time"
)

// TestSharedDeliveryCountsDuplicatesAcrossDifferentMembers verifies shared group delivery accounting.
// Scenarios: performance-and-verification.md:PRF-SCN-036
func TestSharedDeliveryCountsDuplicatesAcrossDifferentMembers(t *testing.T) {
	profile := smokeProfile(t)
	col := newCollector(profile, "narumq")
	topic := publishTopic(classShared, 1)
	sentAt := time.Now()
	if err := col.addMessage(classShared, 1, 1, "step-01-measure", topic, sentAt); err != nil {
		t.Fatal(err)
	}
	col.markSent(classShared, 1, sentAt)
	members := expectedRecipients(col.subscribers, classShared, topic)
	if len(members) < 2 {
		t.Fatalf("shared group has %d members, want at least two", len(members))
	}
	col.recordDelivery(members[0], 3, 1, topic, sentAt.Add(time.Millisecond))
	col.recordDelivery(members[1], 3, 1, topic, sentAt.Add(2*time.Millisecond))
	col.recordDelivery(members[0], 3, 1, topic, sentAt.Add(3*time.Millisecond))
	col.finishStep(1, sentAt.Add(time.Second))

	shared := col.summary().Steps[0].Classes[classShared]
	if shared.ExpectedDeliveries != 1 || shared.Delivered != 1 || shared.Missing != 0 {
		t.Fatalf("shared expected/delivered/missing = %d/%d/%d, want 1/1/0", shared.ExpectedDeliveries, shared.Delivered, shared.Missing)
	}
	if shared.Duplicates != 2 {
		t.Fatalf("shared duplicates = %d, want 2 for three group deliveries", shared.Duplicates)
	}
	first := col.subscribers[members[0]].id
	second := col.subscribers[members[1]].id
	if shared.MemberDeliveries[first] != 2 || shared.MemberDeliveries[second] != 1 {
		t.Fatalf("shared member delivery counts = %v, want %q=2 and %q=1", shared.MemberDeliveries, first, second)
	}
}

// TestOverlapExtraCopiesStopAtAllowedThree verifies overlapping subscription copy accounting.
// Scenarios: performance-and-verification.md:PRF-SCN-036
func TestOverlapExtraCopiesStopAtAllowedThree(t *testing.T) {
	profile := smokeProfile(t)
	col := newCollector(profile, "narumq")
	topic := publishTopic(classOverlap, 1)
	sentAt := time.Now()
	if err := col.addMessage(classOverlap, 1, 1, "step-01-measure", topic, sentAt); err != nil {
		t.Fatal(err)
	}
	col.markSent(classOverlap, 1, sentAt)
	recipients := expectedRecipients(col.subscribers, classOverlap, topic)
	for _, recipient := range recipients {
		col.recordDelivery(recipient, 2, 1, topic, sentAt.Add(time.Millisecond))
	}
	col.recordDelivery(recipients[0], 2, 1, topic, sentAt.Add(2*time.Millisecond))
	col.recordDelivery(recipients[0], 2, 1, topic, sentAt.Add(3*time.Millisecond))
	col.finishStep(1, sentAt.Add(time.Second))
	withinLimit := col.summary().Steps[0].Classes[classOverlap]
	if withinLimit.ExtraCopies != 2 || withinLimit.Duplicates != 0 {
		t.Fatalf("three overlap copies counted extra/duplicate = %d/%d, want 2/0", withinLimit.ExtraCopies, withinLimit.Duplicates)
	}

	col.recordDelivery(recipients[0], 2, 1, topic, sentAt.Add(4*time.Millisecond))
	col.finishStep(1, sentAt.Add(2*time.Second))
	overLimit := col.summary().Steps[0].Classes[classOverlap]
	if overLimit.ExtraCopies != 2 || overLimit.Duplicates != 1 {
		t.Fatalf("four overlap copies counted extra/duplicate = %d/%d, want 2/1", overLimit.ExtraCopies, overLimit.Duplicates)
	}
}
