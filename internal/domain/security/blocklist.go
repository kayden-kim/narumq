// Package security owns immutable authenticated principals and tenant-local ACL policy.
package security

import (
	"errors"
	"net/netip"
	"strings"
	"unicode/utf8"

	"narumq/internal/domain/tenancy"
)

// ErrInvalidBlockEntry means that a blocklist entry is incomplete or not canonical.
var ErrInvalidBlockEntry = errors.New("invalid blocklist entry")

// MaxBlockUsernameBytes is the maximum UTF-8 size of a blocklist username.
const MaxBlockUsernameBytes = 256

// BlockKind identifies the subject field used by a blocklist entry.
type BlockKind uint8

const (
	// BlockKindUnspecified is not a valid blocklist entry kind.
	BlockKindUnspecified BlockKind = iota
	// BlockKindClientID blocks one Client ID within one tenant.
	BlockKindClientID
	// BlockKindUsername blocks an exact MQTT CONNECT User Name.
	BlockKindUsername
	// BlockKindCertificateSHA256 blocks a verified leaf certificate fingerprint.
	BlockKindCertificateSHA256
	// BlockKindIPPrefix blocks source addresses within one canonical IP prefix.
	BlockKindIPPrefix
)

// String returns the wire name of a blocklist kind.
func (kind BlockKind) String() string {
	switch kind {
	case BlockKindClientID:
		return "clientId"
	case BlockKindUsername:
		return "username"
	case BlockKindCertificateSHA256:
		return "certificateSha256"
	case BlockKindIPPrefix:
		return "ipPrefix"
	default:
		return ""
	}
}

// BlockEntry identifies one exact client, identity, certificate, or IP range to block.
type BlockEntry struct {
	Kind     BlockKind
	TenantID string
	Value    string
}

// NewBlockEntry validates and constructs a canonical blocklist entry.
func NewBlockEntry(kind BlockKind, tenantID, value string) (BlockEntry, error) {
	entry := BlockEntry{Kind: kind, TenantID: tenantID, Value: value}
	switch kind {
	case BlockKindClientID:
		if len(tenantID) == 0 || len(tenantID) > 128 || len(value) == 0 || len(value) > 256 || !utf8.ValidString(value) {
			return BlockEntry{}, ErrInvalidBlockEntry
		}
		if _, err := tenancy.NewTenantID(tenantID); err != nil {
			return BlockEntry{}, ErrInvalidBlockEntry
		}
	case BlockKindUsername:
		if tenantID != "" || len(value) == 0 || len(value) > MaxBlockUsernameBytes || !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
			return BlockEntry{}, ErrInvalidBlockEntry
		}
	case BlockKindCertificateSHA256:
		if tenantID != "" || !isLowercaseSHA256(value) {
			return BlockEntry{}, ErrInvalidBlockEntry
		}
	case BlockKindIPPrefix:
		if tenantID != "" {
			return BlockEntry{}, ErrInvalidBlockEntry
		}
		prefix, err := netip.ParsePrefix(value)
		if err != nil || prefix != prefix.Masked() || prefix.Addr().Is4In6() || prefix.String() != value {
			return BlockEntry{}, ErrInvalidBlockEntry
		}
	default:
		return BlockEntry{}, ErrInvalidBlockEntry
	}
	return entry, nil
}

// Key returns the stable management-audit target ID for the entry.
func (entry BlockEntry) Key() string {
	kind := entry.Kind.String()
	if entry.Kind == BlockKindClientID {
		return kind + ":" + entry.TenantID + "/" + entry.Value
	}
	return kind + ":" + entry.Value
}

// CompareBlockEntries orders entries by kind, tenant ID, then value.
func CompareBlockEntries(left, right BlockEntry) int {
	if left.Kind < right.Kind {
		return -1
	}
	if left.Kind > right.Kind {
		return 1
	}
	if left.TenantID < right.TenantID {
		return -1
	}
	if left.TenantID > right.TenantID {
		return 1
	}
	if left.Value < right.Value {
		return -1
	}
	if left.Value > right.Value {
		return 1
	}
	return 0
}

// BlockSubject contains the connection fields considered by the blocklist matcher.
type BlockSubject struct {
	TenantID          string
	ClientID          string
	HasUsername       bool
	Username          string
	CertificateSHA256 string
	SourceIP          string
}

// Blocklist is a caller-synchronized index of blocked MQTT connection subjects.
type Blocklist struct {
	clientIDs    map[[2]string]struct{}
	usernames    map[string]struct{}
	certificates map[string]struct{}
	prefixes     map[netip.Prefix]struct{}
}

// NewBlocklist creates an empty blocklist index.
func NewBlocklist() *Blocklist {
	return &Blocklist{
		clientIDs:    make(map[[2]string]struct{}),
		usernames:    make(map[string]struct{}),
		certificates: make(map[string]struct{}),
		prefixes:     make(map[netip.Prefix]struct{}),
	}
}

// Set adds or removes one valid entry from the index.
func (list *Blocklist) Set(entry BlockEntry, blocked bool) {
	switch entry.Kind {
	case BlockKindClientID:
		setMembership(list.clientIDs, [2]string{entry.TenantID, entry.Value}, blocked)
	case BlockKindUsername:
		setMembership(list.usernames, entry.Value, blocked)
	case BlockKindCertificateSHA256:
		setMembership(list.certificates, entry.Value, blocked)
	case BlockKindIPPrefix:
		prefix, err := netip.ParsePrefix(entry.Value)
		if err != nil {
			return
		}
		setMembership(list.prefixes, prefix, blocked)
	}
}

// Replace replaces the index with the supplied valid entries.
func (list *Blocklist) Replace(entries []BlockEntry) {
	clear(list.clientIDs)
	clear(list.usernames)
	clear(list.certificates)
	clear(list.prefixes)
	for _, entry := range entries {
		list.Set(entry, true)
	}
}

// Match returns the first matching kind in IP, certificate, username, Client ID order.
func (list *Blocklist) Match(subject BlockSubject) (BlockKind, bool) {
	if address, err := netip.ParseAddr(subject.SourceIP); err == nil {
		address = address.Unmap()
		for prefix := range list.prefixes {
			if prefix.Contains(address) {
				return BlockKindIPPrefix, true
			}
		}
	}
	if subject.CertificateSHA256 != "" {
		if _, blocked := list.certificates[subject.CertificateSHA256]; blocked {
			return BlockKindCertificateSHA256, true
		}
	}
	if subject.HasUsername {
		if _, blocked := list.usernames[subject.Username]; blocked {
			return BlockKindUsername, true
		}
	}
	if subject.TenantID != "" && subject.ClientID != "" {
		if _, blocked := list.clientIDs[[2]string{subject.TenantID, subject.ClientID}]; blocked {
			return BlockKindClientID, true
		}
	}
	return BlockKindUnspecified, false
}

func isLowercaseSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func setMembership[K comparable](entries map[K]struct{}, key K, present bool) {
	if present {
		entries[key] = struct{}{}
		return
	}
	delete(entries, key)
}
