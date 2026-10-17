package security

import (
	"errors"
	"strings"
	"testing"
)

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-040
func TestNewBlockEntryAcceptsOnlyCanonicalBoundedValues(t *testing.T) {
	invalid := func(kind BlockKind, tenant, value string) {
		t.Helper()
		if _, err := NewBlockEntry(kind, tenant, value); !errors.Is(err, ErrInvalidBlockEntry) {
			t.Fatalf("NewBlockEntry(%v, %q, %q) error = %v, want ErrInvalidBlockEntry", kind, tenant, value, err)
		}
	}
	valid := func(kind BlockKind, tenant, value string) BlockEntry {
		t.Helper()
		entry, err := NewBlockEntry(kind, tenant, value)
		if err != nil {
			t.Fatalf("NewBlockEntry(%v, %q, %q): %v", kind, tenant, value, err)
		}
		return entry
	}

	valid(BlockKindClientID, strings.Repeat("t", 128), strings.Repeat("c", 256))
	invalid(BlockKindClientID, string([]byte{0xff}), "client")
	invalid(BlockKind(255), "", "")
	invalid(BlockKindClientID, strings.Repeat("t", 129), "client")
	invalid(BlockKindClientID, "tenant", strings.Repeat("c", 257))
	invalid(BlockKindClientID, "tenant", string([]byte{0xff}))
	valid(BlockKindUsername, "", strings.Repeat("u", MaxBlockUsernameBytes))
	invalid(BlockKindUsername, "", strings.Repeat("u", MaxBlockUsernameBytes+1))
	invalid(BlockKindUsername, "", string([]byte{0xff}))
	invalid(BlockKindUsername, "", "user\x00name")

	fingerprint := strings.Repeat("a", 64)
	valid(BlockKindCertificateSHA256, "", fingerprint)
	invalid(BlockKindCertificateSHA256, "", strings.Repeat("a", 63))
	invalid(BlockKindCertificateSHA256, "", strings.Repeat("a", 65))
	invalid(BlockKindCertificateSHA256, "", strings.Repeat("A", 64))

	valid(BlockKindIPPrefix, "", "203.0.113.0/24")
	valid(BlockKindIPPrefix, "", "2001:db8::/32")
	invalid(BlockKindIPPrefix, "", "203.0.113.5/24")
	invalid(BlockKindIPPrefix, "", "2001:DB8::/32")
	invalid(BlockKindIPPrefix, "", "::ffff:1.2.3.4/128")
	invalid(BlockKindUsername, "tenant", "user")
	invalid(BlockKindCertificateSHA256, "tenant", fingerprint)
	invalid(BlockKindIPPrefix, "tenant", "203.0.113.0/24")

	keys := []struct {
		entry BlockEntry
		want  string
	}{
		{valid(BlockKindClientID, "tenant-a", "client/1"), "clientId:tenant-a/client/1"},
		{valid(BlockKindUsername, "", "alice"), "username:alice"},
		{valid(BlockKindCertificateSHA256, "", fingerprint), "certificateSha256:" + fingerprint},
		{valid(BlockKindIPPrefix, "", "203.0.113.0/24"), "ipPrefix:203.0.113.0/24"},
	}
	for _, test := range keys {
		if got := test.entry.Key(); got != test.want {
			t.Errorf("Key() = %q, want %q", got, test.want)
		}
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-040
func TestUnknownBlockKindHasNoWireName(t *testing.T) {
	for _, kind := range []BlockKind{BlockKindUnspecified, BlockKind(255)} {
		if got := kind.String(); got != "" {
			t.Errorf("BlockKind(%d).String() = %q, want empty string", kind, got)
		}
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-040
func TestCompareBlockEntriesOrdersKindTenantAndValue(t *testing.T) {
	tests := []struct {
		name  string
		left  BlockEntry
		right BlockEntry
		want  int
	}{
		{name: "kind before", left: BlockEntry{Kind: BlockKindClientID}, right: BlockEntry{Kind: BlockKindUsername}, want: -1},
		{name: "kind after", left: BlockEntry{Kind: BlockKindUsername}, right: BlockEntry{Kind: BlockKindClientID}, want: 1},
		{name: "tenant before", left: BlockEntry{Kind: BlockKindClientID, TenantID: "tenant-a"}, right: BlockEntry{Kind: BlockKindClientID, TenantID: "tenant-b"}, want: -1},
		{name: "tenant after", left: BlockEntry{Kind: BlockKindClientID, TenantID: "tenant-b"}, right: BlockEntry{Kind: BlockKindClientID, TenantID: "tenant-a"}, want: 1},
		{name: "value before", left: BlockEntry{Kind: BlockKindUsername, Value: "alice"}, right: BlockEntry{Kind: BlockKindUsername, Value: "bob"}, want: -1},
		{name: "value after", left: BlockEntry{Kind: BlockKindUsername, Value: "bob"}, right: BlockEntry{Kind: BlockKindUsername, Value: "alice"}, want: 1},
		{name: "equal", left: BlockEntry{Kind: BlockKindUsername, Value: "alice"}, right: BlockEntry{Kind: BlockKindUsername, Value: "alice"}, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := CompareBlockEntries(test.left, test.right); got != test.want {
				t.Fatalf("CompareBlockEntries(%+v, %+v) = %d, want %d", test.left, test.right, got, test.want)
			}
		})
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-040, tenancy-and-security.md:SEC-SCN-041
func TestBlocklistMatchSemantics(t *testing.T) {
	entry := func(kind BlockKind, tenant, value string) BlockEntry {
		t.Helper()
		result, err := NewBlockEntry(kind, tenant, value)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	ipv4 := entry(BlockKindIPPrefix, "", "203.0.113.0/24")
	ipv6 := entry(BlockKindIPPrefix, "", "2001:db8::/32")
	username := entry(BlockKindUsername, "", "alice")
	certificate := entry(BlockKindCertificateSHA256, "", strings.Repeat("b", 64))
	client := entry(BlockKindClientID, "tenant-a", "device")
	list := NewBlocklist()
	list.Set(ipv4, true)
	list.Set(ipv6, true)

	for _, address := range []string{"203.0.113.0", "203.0.113.255", "2001:db8::", "2001:db8:ffff::1"} {
		if _, matched := list.Match(BlockSubject{SourceIP: address}); !matched {
			t.Errorf("address %q did not match its prefix", address)
		}
	}
	for _, address := range []string{"203.0.112.255", "203.0.114.0", "2001:db7::1", "2001:db9::1"} {
		if _, matched := list.Match(BlockSubject{SourceIP: address}); matched {
			t.Errorf("outside address %q matched a prefix", address)
		}
	}
	if kind, matched := list.Match(BlockSubject{SourceIP: "::ffff:203.0.113.9"}); !matched || kind != BlockKindIPPrefix {
		t.Fatalf("IPv4-mapped source match = %v, %v", kind, matched)
	}
	list.Set(username, true)
	if _, matched := list.Match(BlockSubject{Username: "alice", HasUsername: false}); matched {
		t.Fatal("username matched when HasUsername was false")
	}
	list.Set(client, true)
	if _, matched := list.Match(BlockSubject{TenantID: "tenant-b", ClientID: "device"}); matched {
		t.Fatal("client ID matched in another tenant")
	}
	if _, matched := list.Match(BlockSubject{TenantID: "", ClientID: "device"}); matched {
		t.Fatal("client ID matched with an empty tenant")
	}
	list.Set(ipv4, false)
	if _, matched := list.Match(BlockSubject{SourceIP: "203.0.113.7"}); matched {
		t.Fatal("Set(false) did not remove the prefix")
	}
	list.Replace([]BlockEntry{ipv4})
	if _, matched := list.Match(BlockSubject{Username: "alice", HasUsername: true}); matched {
		t.Fatal("Replace retained an old username")
	}

	precedence := NewBlocklist()
	precedence.Set(client, true)
	precedence.Set(username, true)
	precedence.Set(certificate, true)
	precedence.Set(ipv4, true)
	subject := BlockSubject{TenantID: "tenant-a", ClientID: "device", HasUsername: true, Username: "alice", CertificateSHA256: strings.Repeat("b", 64), SourceIP: "203.0.113.9"}
	for _, want := range []BlockKind{BlockKindIPPrefix, BlockKindCertificateSHA256, BlockKindUsername, BlockKindClientID} {
		got, matched := precedence.Match(subject)
		if !matched || got != want {
			t.Fatalf("precedence match = %v, %v; want %v", got, matched, want)
		}
		precedence.Set(BlockEntry{Kind: want, TenantID: map[BlockKind]string{BlockKindClientID: "tenant-a"}[want], Value: map[BlockKind]string{BlockKindIPPrefix: ipv4.Value, BlockKindCertificateSHA256: certificate.Value, BlockKindUsername: username.Value, BlockKindClientID: client.Value}[want]}, false)
	}
	if _, matched := precedence.Match(subject); matched {
		t.Fatal("all precedence entries were removed but a match remained")
	}
}

// Traceability: SEC-12; docs/specs/tenancy-and-security.md §7.1, §9; ADR-0050.
// Scenarios: tenancy-and-security.md:SEC-SCN-040
func TestBlocklistSetIgnoresMalformedAndUnknownEntries(t *testing.T) {
	list := NewBlocklist()
	list.Set(BlockEntry{Kind: BlockKindIPPrefix, Value: "not-a-prefix"}, true)
	list.Set(BlockEntry{Kind: BlockKind(255), Value: "203.0.113.0/24"}, true)
	if kind, matched := list.Match(BlockSubject{SourceIP: "203.0.113.8"}); matched {
		t.Fatalf("invalid entries produced match kind %v", kind)
	}
}
