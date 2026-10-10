package main

import (
	"errors"
	"testing"
)

func TestRejectDuplicateJSONKeys(t *testing.T) {
	cases := []struct {
		name          string
		input         string
		wantError     bool
		wantDuplicate bool
	}{
		{name: "sibling objects are independent", input: `[{"a":1},{"a":2}]`},
		{name: "different object depths are independent", input: `{"a":{"a":1}}`},
		{name: "case-sensitive names differ", input: `{"a":1,"A":2}`},
		{name: "duplicate top-level key", input: `{"a":1,"a":2}`, wantError: true, wantDuplicate: true},
		{name: "duplicate nested key", input: `{"a":{"b":1,"b":2}}`, wantError: true, wantDuplicate: true},
		{name: "duplicate key in array object", input: `{"a":[{"b":1,"b":2}]}`, wantError: true, wantDuplicate: true},
		{name: "empty input", input: ``, wantError: true},
		{name: "non-string object key", input: `{1:2}`, wantError: true},
		{name: "invalid nested value", input: `{"a":}`, wantError: true},
		{name: "unterminated object", input: `{"a":1`, wantError: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := rejectDuplicateJSONKeys([]byte(test.input))
			if (err != nil) != test.wantError {
				t.Fatalf("rejectDuplicateJSONKeys() error = %v, want error %t", err, test.wantError)
			}
			if errors.Is(err, errDuplicateJSONKey) != test.wantDuplicate {
				t.Fatalf("duplicate JSON key classification = %t, want %t", errors.Is(err, errDuplicateJSONKey), test.wantDuplicate)
			}
		})
	}
}
