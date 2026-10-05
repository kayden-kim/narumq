package tenancy

import (
	"errors"
	"strings"
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

func TestParseSessionKeyRoutingIDRoundTripsMultibyteComponents(t *testing.T) {
	tenantID, err := NewTenantID("テナント")
	if err != nil {
		t.Fatal(err)
	}
	key, err := NewSessionKey(tenantID, "client-雪/with:colon")
	if err != nil {
		t.Fatal(err)
	}

	got, err := ParseSessionKeyRoutingID(key.RoutingID())
	if err != nil {
		t.Fatalf("ParseSessionKeyRoutingID() error = %v", err)
	}
	if got != key {
		t.Fatalf("parsed session key = %+v, want %+v", got, key)
	}
}

func TestParseSessionKeyRoutingIDRejectsMalformedIDs(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		routingID string
	}{
		{name: "empty", routingID: ""},
		{name: "raw client ID", routingID: "client"},
		{name: "missing fields", routingID: "\xff"},
		{name: "empty tenant length", routingID: "\xff:t1:c"},
		{name: "missing tenant length separator", routingID: "\xff1t"},
		{name: "missing client length", routingID: "\xff1:t"},
		{name: "empty client ID", routingID: "\xff1:t1:"},
		{name: "trailing bytes", routingID: "\xff1:t1:c!"},
		{name: "leading zero tenant length", routingID: "\xff01:t1:c"},
		{name: "leading zero client length", routingID: "\xff1:t01:c"},
		{name: "negative tenant length", routingID: "\xff-1:t1:c"},
		{name: "overflowing length", routingID: "\xff999999999999999999999999:t1:c"},
		{name: "invalid tenant UTF-8", routingID: "\xff1:\xff1:c"},
		{name: "invalid client UTF-8", routingID: "\xff1:t1:\xff"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := ParseSessionKeyRoutingID(testCase.routingID); !errors.Is(err, ErrInvalidSessionKey) {
				t.Fatalf("ParseSessionKeyRoutingID(%q) error = %v, want ErrInvalidSessionKey", testCase.routingID, err)
			}
		})
	}
}

func TestParseSessionKeyRoutingIDAcceptsMaximumLengthClientID(t *testing.T) {
	tenantID, err := NewTenantID("tenant")
	if err != nil {
		t.Fatal(err)
	}
	clientID := strings.Repeat("c", 65535)
	key, err := NewSessionKey(tenantID, clientID)
	if err != nil {
		t.Fatal(err)
	}

	got, err := ParseSessionKeyRoutingID(key.RoutingID())
	if err != nil {
		t.Fatalf("ParseSessionKeyRoutingID() error = %v", err)
	}
	if got != key {
		t.Fatal("parsed session key differs at the maximum client ID length")
	}
}
