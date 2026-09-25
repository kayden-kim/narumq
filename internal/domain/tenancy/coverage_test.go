package tenancy

import "testing"

func cmmFilter(t *testing.T, tenant TenantID, raw string) TopicFilter {
	t.Helper()
	filter, err := NewTopicFilter(tenant, raw)
	if err != nil {
		t.Fatalf("NewTopicFilter(%q): %v", raw, err)
	}
	return filter
}

func TestCmmRoutingIDRejectsUnconstructedSessionKeys(t *testing.T) {
	tenant, err := NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		key  SessionKey
	}{
		{name: "zero value", key: SessionKey{}},
		{name: "empty client", key: SessionKey{tenantID: tenant}},
		{name: "invalid tenant", key: SessionKey{tenantID: TenantID{value: "bad\x00tenant"}, clientID: "device-1"}},
		{name: "invalid client", key: SessionKey{tenantID: tenant, clientID: "bad\x00client"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.key.RoutingID(); got != "" {
				t.Fatalf("RoutingID() = %q, want empty", got)
			}
		})
	}
}

func TestCmmFilterContainsLevelAndLengthBoundaries(t *testing.T) {
	tenant, err := NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		container string
		contained string
		want      bool
	}{
		{name: "literal does not contain shorter literal", container: "a/b", contained: "a", want: false},
		{name: "literal does not contain longer literal", container: "a", contained: "a/b", want: false},
		{name: "literal level mismatch", container: "a/b", contained: "a/c", want: false},
		{name: "literal does not contain plus", container: "a/b", contained: "a/+", want: false},
		{name: "plus contains plus", container: "+/a", contained: "+/a", want: true},
		{name: "root plus excludes system literal", container: "+/a", contained: "$SYS/a", want: false},
		{name: "hash prefix longer than contained literal", container: "a/b/#", contained: "a", want: false},
		{name: "hash prefix equals contained literal parent", container: "a/#", contained: "a", want: true},
		{name: "hash does not contain wider hash", container: "a/b/#", contained: "a/#", want: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := FilterContains(cmmFilter(t, tenant, testCase.container), cmmFilter(t, tenant, testCase.contained))
			if err != nil {
				t.Fatalf("FilterContains() error = %v", err)
			}
			if got != testCase.want {
				t.Fatalf("FilterContains(%q, %q) = %v, want %v", testCase.container, testCase.contained, got, testCase.want)
			}
		})
	}
}

func TestCmmFiltersIntersectIsSymmetricAcrossWildcardEdges(t *testing.T) {
	tenant, err := NewTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{name: "system literal against root hash", left: "$SYS/x", right: "#", want: false},
		{name: "plus against plus", left: "+/a", right: "+/a", want: true},
		{name: "literal against plus", left: "a/b", right: "a/+", want: true},
		{name: "root system literal against root plus", left: "$SYS/x", right: "+/x", want: false},
		{name: "nested system-like literal against plus", left: "a/$x", right: "a/+", want: true},
		{name: "literal against shorter hash", left: "a/b/c", right: "a/#", want: true},
		{name: "literal shorter than hash prefix", left: "a", right: "a/b/#", want: false},
		{name: "different literal lengths", left: "a/b", right: "a/b/c", want: false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			left := cmmFilter(t, tenant, testCase.left)
			right := cmmFilter(t, tenant, testCase.right)
			forward, err := FiltersIntersect(left, right)
			if err != nil {
				t.Fatalf("FiltersIntersect(%q, %q) error = %v", testCase.left, testCase.right, err)
			}
			backward, err := FiltersIntersect(right, left)
			if err != nil {
				t.Fatalf("FiltersIntersect(%q, %q) error = %v", testCase.right, testCase.left, err)
			}
			if forward != testCase.want || backward != testCase.want {
				t.Fatalf("FiltersIntersect(%q, %q) = %v/%v (forward/backward), want %v", testCase.left, testCase.right, forward, backward, testCase.want)
			}
		})
	}
}
