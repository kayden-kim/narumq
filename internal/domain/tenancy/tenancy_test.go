package tenancy

import (
	"errors"
	"testing"
)

// Requirements: TEN-02, TEN-03
func TestTenantValuesPreventPrefixCollisions(t *testing.T) {
	tenantA, err := NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantAB, err := NewTenantID("tenant-ab")
	if err != nil {
		t.Fatal(err)
	}
	keyA, err := NewSessionKey(tenantA, "device")
	if err != nil {
		t.Fatal(err)
	}
	keyAB, err := NewSessionKey(tenantAB, "device")
	if err != nil {
		t.Fatal(err)
	}
	if keyA == keyAB {
		t.Fatal("tenant-scoped session keys collided")
	}
	topicA, err := NewTopic(tenantA, "sensors/temperature")
	if err != nil {
		t.Fatal(err)
	}
	topicAB, err := NewTopic(tenantAB, "sensors/temperature")
	if err != nil {
		t.Fatal(err)
	}
	if topicA == topicAB {
		t.Fatal("tenant-scoped topics collided")
	}
	if topicA.Name() != topicAB.Name() || topicA.TenantID() == topicAB.TenantID() {
		t.Fatal("tenant and external topic accessors lost their separation")
	}
}

// Requirements: TEN-03, TEN-04
func TestFilterContainmentAndIntersectionPreserveMQTTWildcardSemantics(t *testing.T) {
	tenant, err := NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	newFilter := func(raw string) TopicFilter {
		t.Helper()
		filter, err := NewTopicFilter(tenant, raw)
		if err != nil {
			t.Fatalf("NewTopicFilter(%q): %v", raw, err)
		}
		return filter
	}
	containsCases := []struct {
		name      string
		container string
		contained string
		want      bool
	}{
		{name: "hash contains concrete", container: "sensors/#", contained: "sensors/room/temperature", want: true},
		{name: "hash contains wildcard", container: "sensors/#", contained: "sensors/+/temperature", want: true},
		{name: "narrower does not contain broader", container: "sensors/+/temperature", contained: "sensors/#", want: false},
		{name: "plus matches empty level", container: "+/x", contained: "/x", want: true},
		{name: "plus hash covers root hash", container: "+/#", contained: "#", want: true},
		{name: "system explicit", container: "$SYS/#", contained: "$SYS/+", want: true},
		{name: "system wildcard boundary", container: "#", contained: "$SYS/#", want: false},
		{name: "shared underlying filter", container: "$share/group/sensors/#", contained: "$share/other/sensors/room", want: true},
	}
	for _, testCase := range containsCases {
		t.Run("contains/"+testCase.name, func(t *testing.T) {
			contained, err := FilterContains(newFilter(testCase.container), newFilter(testCase.contained))
			if err != nil {
				t.Fatal(err)
			}
			if contained != testCase.want {
				t.Fatalf("FilterContains() = %v, want %v", contained, testCase.want)
			}
		})
	}
	intersectionCases := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{name: "overlap", left: "sensors/+/#", right: "sensors/room/+", want: true},
		{name: "disjoint literals", left: "sensors/a", right: "sensors/b", want: false},
		{name: "deny intersects broad request", left: "sensors/private/#", right: "sensors/#", want: true},
		{name: "system wildcard does not overlap", left: "+/#", right: "$SYS/#", want: false},
		{name: "root wildcard excludes system", left: "#", right: "$SYS/#", want: false},
		{name: "shared groups are ignored", left: "$share/a/events/#", right: "$share/b/events/+", want: true},
	}
	for _, testCase := range intersectionCases {
		t.Run("intersects/"+testCase.name, func(t *testing.T) {
			intersects, err := FiltersIntersect(newFilter(testCase.left), newFilter(testCase.right))
			if err != nil {
				t.Fatal(err)
			}
			if intersects != testCase.want {
				t.Fatalf("FiltersIntersect() = %v, want %v", intersects, testCase.want)
			}
		})
	}
}

// Requirements: TEN-03, TEN-04
func TestTenantRelationsRejectCrossTenantValues(t *testing.T) {
	firstTenant, err := NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	secondTenant, err := NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	firstFilter, err := NewTopicFilter(firstTenant, "events/#")
	if err != nil {
		t.Fatal(err)
	}
	secondFilter, err := NewTopicFilter(secondTenant, "events/#")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FilterContains(firstFilter, secondFilter); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("FilterContains cross-tenant error = %v, want %v", err, ErrTenantMismatch)
	}
	if _, err := FiltersIntersect(firstFilter, secondFilter); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("FiltersIntersect cross-tenant error = %v, want %v", err, ErrTenantMismatch)
	}
}

func TestTenancyValidationAndAccessors(t *testing.T) {
	tenantA, err := NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, err := NewTenantID("tenant-b")
	if err != nil {
		t.Fatal(err)
	}

	// NewSessionKey error paths
	if _, err := NewSessionKey(TenantID{}, "client"); !errors.Is(err, ErrInvalidSessionKey) {
		t.Fatalf("zero tenant NewSessionKey error = %v", err)
	}
	if _, err := NewSessionKey(tenantA, ""); !errors.Is(err, ErrInvalidClientID) {
		t.Fatalf("empty client NewSessionKey error = %v", err)
	}
	if _, err := NewSessionKey(tenantA, "\x00"); !errors.Is(err, ErrInvalidClientID) {
		t.Fatalf("null client NewSessionKey error = %v", err)
	}

	// NewTopic error paths
	if _, err := NewTopic(TenantID{}, "topic"); !errors.Is(err, ErrInvalidTenantID) {
		t.Fatalf("zero tenant NewTopic error = %v", err)
	}
	if _, err := NewTopic(tenantA, ""); !errors.Is(err, ErrInvalidTopicName) {
		t.Fatalf("empty name NewTopic error = %v", err)
	}
	if _, err := NewTopic(tenantA, "topic/+"); !errors.Is(err, ErrInvalidTopicName) {
		t.Fatalf("wildcard NewTopic error = %v", err)
	}

	// NewTopicFilter error paths
	if _, err := NewTopicFilter(TenantID{}, "filter/#"); !errors.Is(err, ErrInvalidTenantID) {
		t.Fatalf("zero tenant NewTopicFilter error = %v", err)
	}
	if _, err := NewTopicFilter(tenantA, ""); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("empty filter NewTopicFilter error = %v", err)
	}
	if _, err := NewTopicFilter(tenantA, "filter/#/extra"); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("invalid filter NewTopicFilter error = %v", err)
	}

	// TopicMatches cross-tenant and unconstructed checks
	validTopic, _ := NewTopic(tenantA, "events/a")
	validFilterA, _ := NewTopicFilter(tenantA, "events/#")
	validFilterB, _ := NewTopicFilter(tenantB, "events/#")

	if _, err := TopicMatches(Topic{}, validFilterA); !errors.Is(err, ErrInvalidTopicName) {
		t.Fatalf("unconstructed topic TopicMatches error = %v", err)
	}
	if _, err := TopicMatches(validTopic, TopicFilter{}); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("unconstructed filter TopicMatches error = %v", err)
	}
	if _, err := TopicMatches(validTopic, validFilterB); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("cross-tenant TopicMatches error = %v", err)
	}
	matched, err := TopicMatches(validTopic, validFilterA)
	if err != nil || !matched {
		t.Fatalf("TopicMatches = (%v, %v), want true, nil", matched, err)
	}

	// FilterContains unconstructed checks
	if _, err := FilterContains(TopicFilter{}, validFilterA); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("unconstructed container FilterContains error = %v", err)
	}
	if _, err := FilterContains(validFilterA, TopicFilter{}); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("unconstructed contained FilterContains error = %v", err)
	}

	// FiltersIntersect unconstructed checks
	if _, err := FiltersIntersect(TopicFilter{}, validFilterA); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("unconstructed left FiltersIntersect error = %v", err)
	}
	if _, err := FiltersIntersect(validFilterA, TopicFilter{}); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("unconstructed right FiltersIntersect error = %v", err)
	}

	// TopicMatchesFilter invalid checks
	if _, err := TopicMatchesFilter("", "filter/#"); !errors.Is(err, ErrInvalidTopicName) {
		t.Fatalf("TopicMatchesFilter empty topic error = %v", err)
	}
	if _, err := TopicMatchesFilter("topic", ""); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("TopicMatchesFilter empty filter error = %v", err)
	}
	if _, err := TopicMatchesFilter("topic", "$share//invalid"); !errors.Is(err, ErrInvalidTopicFilter) {
		t.Fatalf("TopicMatchesFilter invalid shared filter error = %v", err)
	}

	// Accessors
	key, _ := NewSessionKey(tenantA, "device-1")
	if key.TenantID() != tenantA || key.ClientID() != "device-1" {
		t.Fatalf("SessionKey accessors = (%v, %v)", key.TenantID(), key.ClientID())
	}
	if validFilterA.TenantID() != tenantA || validFilterA.Filter() != "events/#" {
		t.Fatalf("TopicFilter accessors = (%v, %v)", validFilterA.TenantID(), validFilterA.Filter())
	}
}
