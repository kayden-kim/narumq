package main

import (
	"encoding/json"
	"testing"
)

func TestLoadDrainProfileAcceptsCheckedInProfile(t *testing.T) {
	if _, _, err := LoadDrainProfile("drain-profile.json"); err != nil {
		t.Fatal(err)
	}
}

func TestDrainProfileValidateRejectsDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*DrainProfile)
	}{
		{"URI reference", func(p *DrainProfile) { p.Cluster.ServerReference = "mqtt://node2.compat.test:1883" }},
		{"case order", func(p *DrainProfile) { p.ExpectedCases[0], p.ExpectedCases[1] = p.ExpectedCases[1], p.ExpectedCases[0] }},
		{"cluster vector", func(p *DrainProfile) { p.ExpectedCases[0].Vector = "truncated_property" }},
		{"missing malformed vector", func(p *DrainProfile) { p.ExpectedCases[1].Vector = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := canonicalDrainProfile()
			test.mutate(&p)
			data, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeDrainProfile(data); err == nil {
				t.Fatal("accepted profile drift")
			}
		})
	}
	t.Run("unknown field", func(t *testing.T) {
		data, err := json.Marshal(canonicalDrainProfile())
		if err != nil {
			t.Fatal(err)
		}
		data = append([]byte(`{"unknown":true,`), data[1:]...)
		if _, err := DecodeDrainProfile(data); err == nil {
			t.Fatal("accepted unknown field")
		}
	})
	t.Run("trailing JSON", func(t *testing.T) {
		data, _ := json.Marshal(canonicalDrainProfile())
		data = append(data, []byte(` {}`)...)
		if _, err := DecodeDrainProfile(data); err == nil {
			t.Fatal("accepted trailing JSON")
		}
	})
}
