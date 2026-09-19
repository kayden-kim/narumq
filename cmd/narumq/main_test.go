package main

import "testing"

func TestValidateLoopbackListenAddress(t *testing.T) {
	valid := []string{"127.0.0.1:0", "127.255.255.254:1883", "[::1]:0"}
	for _, address := range valid {
		t.Run("valid/"+address, func(t *testing.T) {
			if err := validateLoopbackListenAddress(address); err != nil {
				t.Fatalf("validateLoopbackListenAddress(%q) error = %v", address, err)
			}
		})
	}
	invalid := []string{
		"localhost:1883",
		":1883",
		"0.0.0.0:1883",
		"192.0.2.1:1883",
		"[::]:1883",
		"[::ffff:127.0.0.1]:1883",
		"127.0.0.1",
	}
	for _, address := range invalid {
		t.Run("invalid/"+address, func(t *testing.T) {
			if err := validateLoopbackListenAddress(address); err == nil {
				t.Fatalf("validateLoopbackListenAddress(%q) unexpectedly succeeded", address)
			}
		})
	}
}
