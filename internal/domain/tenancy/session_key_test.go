package tenancy

import (
	"testing"
	"unicode/utf8"
)

func TestSessionKeyRoutingIDIsInjectiveAcrossSlashComponents(t *testing.T) {
	firstTenant, err := NewTenantID("a")
	if err != nil {
		t.Fatal(err)
	}
	secondTenant, err := NewTenantID("a/b")
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewSessionKey(firstTenant, "b/c")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSessionKey(secondTenant, "c")
	if err != nil {
		t.Fatal(err)
	}

	firstID := first.RoutingID()
	secondID := second.RoutingID()
	if firstID == secondID {
		t.Fatalf("distinct tenant/client pairs share routing ID %q", firstID)
	}
	if utf8.ValidString(firstID) || utf8.ValidString(secondID) {
		t.Fatal("tenant-scoped routing IDs must be disjoint from valid raw MQTT Client IDs")
	}
}
