package main

import (
	"slices"
	"strings"
	"testing"
)

func testProfile(t *testing.T) Profile {
	t.Helper()
	p, _, _, err := LoadProfile("testdata/local-smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestBuildCellHasFiftyThreeClientsWithExactIDsAndTLSOrdinals plans 53 clients per cell with exact IDs.
// Scenarios: performance-and-verification.md:PRF-SCN-049
func TestBuildCellHasFiftyThreeClientsWithExactIDsAndTLSOrdinals(t *testing.T) {
	p := testProfile(t)
	plan := buildCell(p, 7)
	if len(plan.clients) != cellClients {
		t.Fatalf("clients = %d, want %d", len(plan.clients), cellClients)
	}
	seen := map[string]bool{}
	tlsCount := 0
	for ordinal, spec := range plan.clients {
		if seen[spec.id] {
			t.Fatalf("duplicate client ID %q", spec.id)
		}
		seen[spec.id] = true
		if want := ordinal%p.TLSEveryNthClient == 0; spec.tls != want {
			t.Fatalf("client %s ordinal %d tls = %t, want %t", spec.id, ordinal, spec.tls, want)
		}
		if spec.tls {
			tlsCount++
		}
		if !strings.HasPrefix(spec.id, spec.tenant+"-") || !strings.HasPrefix(spec.tenant, "bp02-") || !strings.HasSuffix(strings.Split(spec.tenant, "-")[2], "007") {
			t.Fatalf("client %q does not embed tenant %q for cell 7", spec.id, spec.tenant)
		}
	}
	if got := countTLSClients(p); got != tlsCount {
		t.Fatalf("countTLSClients = %d, plan has %d", got, tlsCount)
	}
	for _, id := range []string{"bp02-a-c007-pub-telemetry", "bp02-a-c007-sub-telemetry-00", "bp02-b-c007-sub-state-01", "bp02-b-c007-idle-005"} {
		if !seen[id] {
			t.Errorf("client %q missing from the plan", id)
		}
	}
	if plan.clients[0].id != "bp02-a-c007-pub-telemetry" {
		t.Fatalf("first planned client = %q, want the telemetry publisher", plan.clients[0].id)
	}
}

// TestTenantClientIDsAreExactPerTenant lists the exact client IDs of each tenant.
// Scenarios: performance-and-verification.md:PRF-SCN-049
func TestTenantClientIDsAreExactPerTenant(t *testing.T) {
	p := testProfile(t)
	a, b := tenantClientIDs(p, 'a', 3), tenantClientIDs(p, 'b', 3)
	if len(a)+len(b) != cellClients {
		t.Fatalf("tenant ID lists cover %d clients, want %d", len(a)+len(b), cellClients)
	}
	for _, id := range a {
		if !strings.HasPrefix(id, "bp02-a-c003-") || slices.Contains(b, id) {
			t.Fatalf("tenant a lists %q", id)
		}
	}
	for _, id := range b {
		if !strings.HasPrefix(id, "bp02-b-c003-") {
			t.Fatalf("tenant b lists %q", id)
		}
	}
}

// TestCellFiltersAndOracleStayInsideTheCell keeps filters and expected recipients inside the cell.
// Scenarios: performance-and-verification.md:PRF-SCN-049
func TestCellFiltersAndOracleStayInsideTheCell(t *testing.T) {
	p := testProfile(t)
	plan := buildCell(p, 7)
	for _, spec := range plan.clients {
		if spec.filter == "" {
			continue
		}
		filter := strings.TrimPrefix(spec.filter, "$share/bp02a-c007/")
		if !strings.HasPrefix(filter, "bp02/"+string(spec.tenant[5])+"/c007/") {
			t.Fatalf("filter %q of %s escapes cell 7", spec.filter, spec.id)
		}
	}
	// QoS 2 command topics go to exactly one exact-filter subscriber each.
	for seq := uint64(1); seq <= 8; seq++ {
		if got := plan.recipientFor(2, seq).expected(); got != 1 {
			t.Fatalf("command seq %d expects %d recipients, want 1", seq, got)
		}
	}
	// Shared jobs deliver one copy across the whole group; broadcast alerts reach every subscriber.
	if got := plan.recipientFor(3, 1).expected(); got != 1 {
		t.Fatalf("shared job expects %d copies, want 1", got)
	}
	if got := plan.recipientFor(0, 1).expected(); got != 6 {
		t.Fatalf("telemetry expects %d copies, want 6", got)
	}
	if got := plan.publishTopic(0, 9); got != plan.publishTopic(0, 1) {
		t.Fatalf("telemetry topic cycle is not 8 distinct topics: %q vs %q", got, plan.publishTopic(0, 1))
	}
}

func TestTopicMatchesHandlesWildcardsAndSharedPrefix(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"bp02/a/c000/telemetry/+", "bp02/a/c000/telemetry/dev01", true},
		{"bp02/a/c000/telemetry/+", "bp02/a/c001/telemetry/dev01", false},
		{"bp02/a/c000/alert/#", "bp02/a/c000/alert/zone2", true},
		{"$share/g/bp02/a/c000/job/#", "bp02/a/c000/job/3", true},
		{"bp02/a/c000/command/dev01", "bp02/a/c000/command/dev02", false},
	}
	for _, tc := range cases {
		if got := topicMatches(tc.filter, tc.topic); got != tc.want {
			t.Errorf("topicMatches(%q, %q) = %t, want %t", tc.filter, tc.topic, got, tc.want)
		}
	}
}

func TestPayloadRoundTripAndTamperDetection(t *testing.T) {
	p := testProfile(t)
	header := payloadHeader{seq: 7, token: 0xABCD0102, cell: 3, stream: 2}
	payload := makePayload(p, header)
	if len(payload) != payloadSize(p, 7) {
		t.Fatalf("payload size %d, want %d", len(payload), payloadSize(p, 7))
	}
	parsed, err := parsePayloadHeader(payload)
	if err != nil || parsed != header {
		t.Fatalf("parsed header %+v, err %v; want %+v", parsed, err, header)
	}
	if err := verifyPayload(p, parsed, payload); err != nil {
		t.Fatal(err)
	}
	payload[len(payload)-1] ^= 0xFF
	if err := verifyPayload(p, parsed, payload); err == nil {
		t.Fatal("tampered payload verified")
	}
}
